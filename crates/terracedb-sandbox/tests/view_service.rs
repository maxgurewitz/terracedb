use std::sync::Arc;

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    AuthenticatedReadonlyViewRemoteEndpoint, DefaultSandboxStore, LocalReadonlyViewBridge,
    ReadonlyViewCut, ReadonlyViewProtocolRequest, ReadonlyViewProtocolResponse,
    ReadonlyViewProtocolTransport, ReadonlyViewReconnectRequest, ReadonlyViewRemoteEndpoint,
    ReadonlyViewRequest, RemoteReadonlyViewBridge, SandboxConfig, SandboxServices, SandboxStore,
    StaticReadonlyViewRegistry,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

fn sandbox_store(now: u64, seed: u64) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(
        Arc::new(vfs.clone()),
        dependencies.clock,
        SandboxServices::deterministic(),
    );
    (vfs, sandbox)
}

async fn create_empty_base(store: &InMemoryVfsStore, volume_id: VolumeId) {
    store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
}

#[tokio::test]
async fn readonly_view_service_browses_sessions_visible_and_durable_cuts() {
    let (vfs, sandbox) = sandbox_store(200, 601);
    let base_volume_id = VolumeId::new(0x9300);
    let session_volume_id = VolumeId::new(0x9301);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .filesystem()
        .write_file(
            "/workspace/visible.txt",
            b"visible only\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write visible file");

    let registry = Arc::new(StaticReadonlyViewRegistry::new([session.clone()]));
    let service = Arc::new(terracedb_sandbox::ReadonlyViewService::new(registry));
    let bridge = LocalReadonlyViewBridge::new(service.clone());

    let sessions = bridge
        .send(ReadonlyViewProtocolRequest::ListSessions)
        .await
        .expect("list sessions");
    let ReadonlyViewProtocolResponse::Sessions { sessions } = sessions else {
        panic!("expected sessions response");
    };
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_volume_id, session_volume_id);

    let visible_handle = bridge
        .send(ReadonlyViewProtocolRequest::OpenView {
            session_volume_id,
            request: ReadonlyViewRequest {
                cut: ReadonlyViewCut::Visible,
                path: "/workspace".to_string(),
                label: Some("visible".to_string()),
            },
        })
        .await
        .expect("open visible view");
    let ReadonlyViewProtocolResponse::View { handle } = visible_handle else {
        panic!("expected view response");
    };
    assert_eq!(handle.location.cut, ReadonlyViewCut::Visible);

    let listed = bridge
        .send(ReadonlyViewProtocolRequest::ReadDir {
            location: handle.location.clone(),
        })
        .await
        .expect("read visible directory");
    let ReadonlyViewProtocolResponse::Directory { entries } = listed else {
        panic!("expected directory response");
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "visible.txt");

    let visible_stat = bridge
        .send(ReadonlyViewProtocolRequest::Stat {
            location: entries[0].location.clone(),
        })
        .await
        .expect("stat visible file");
    let ReadonlyViewProtocolResponse::Stat { stat } = visible_stat else {
        panic!("expected stat response");
    };
    assert_eq!(
        stat.expect("visible stat").size,
        b"visible only\n".len() as u64
    );

    let visible_bytes = bridge
        .send(ReadonlyViewProtocolRequest::ReadFile {
            location: entries[0].location.clone(),
        })
        .await
        .expect("read visible file");
    let ReadonlyViewProtocolResponse::File { bytes } = visible_bytes else {
        panic!("expected file response");
    };
    assert_eq!(bytes, Some(b"visible only\n".to_vec()));

    let durable_dir = bridge
        .send(ReadonlyViewProtocolRequest::ReadDir {
            location: terracedb_sandbox::ReadonlyViewLocation {
                session_volume_id,
                cut: ReadonlyViewCut::Durable,
                path: "/workspace".to_string(),
            },
        })
        .await
        .expect("read durable directory");
    let ReadonlyViewProtocolResponse::Directory { entries } = durable_dir else {
        panic!("expected directory response");
    };
    assert!(
        entries.is_empty(),
        "durable cut should exclude unflushed writes"
    );

    session.flush().await.expect("flush session");
    bridge.refresh().await.expect("refresh sessions");

    let reopened = bridge
        .send(ReadonlyViewProtocolRequest::ReconnectView {
            request: ReadonlyViewReconnectRequest {
                session_volume_id,
                cut: ReadonlyViewCut::Durable,
                path: "/workspace".to_string(),
                label: Some("durable".to_string()),
            },
        })
        .await
        .expect("reconnect durable view");
    let ReadonlyViewProtocolResponse::View { handle } = reopened else {
        panic!("expected view response");
    };
    assert_eq!(handle.location.cut, ReadonlyViewCut::Durable);

    let durable_dir = bridge
        .send(ReadonlyViewProtocolRequest::ReadDir {
            location: handle.location.clone(),
        })
        .await
        .expect("read durable directory after flush");
    let ReadonlyViewProtocolResponse::Directory { entries } = durable_dir else {
        panic!("expected directory response");
    };
    assert_eq!(entries.len(), 1);

    let handles = bridge
        .send(ReadonlyViewProtocolRequest::ListHandles {
            session_volume_id: Some(session_volume_id),
        })
        .await
        .expect("list handles");
    let ReadonlyViewProtocolResponse::Handles { handles } = handles else {
        panic!("expected handles response");
    };
    assert_eq!(handles.len(), 2);
}

#[tokio::test]
async fn authenticated_remote_readonly_view_bridge_enforces_token_and_reconnects() {
    let (vfs, sandbox) = sandbox_store(210, 602);
    let base_volume_id = VolumeId::new(0x9310);
    let session_volume_id = VolumeId::new(0x9311);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let registry = Arc::new(StaticReadonlyViewRegistry::new([session]));
    let service = Arc::new(terracedb_sandbox::ReadonlyViewService::new(registry));
    let endpoint = Arc::new(AuthenticatedReadonlyViewRemoteEndpoint::new(
        service,
        "secret-token",
    ));
    let remote = RemoteReadonlyViewBridge::new(endpoint.clone(), "secret-token");

    let unauthorized = endpoint
        .send_authenticated("wrong-token", ReadonlyViewProtocolRequest::ListSessions)
        .await
        .expect_err("reject wrong token");
    assert!(matches!(
        unauthorized,
        terracedb_sandbox::SandboxError::ReadonlyViewUnauthorized
    ));

    remote.reconnect().await.expect("reconnect remote bridge");
    let sessions = remote
        .send(ReadonlyViewProtocolRequest::RefreshSessions)
        .await
        .expect("refresh sessions remotely");
    let ReadonlyViewProtocolResponse::Sessions { sessions } = sessions else {
        panic!("expected sessions response");
    };
    assert_eq!(sessions[0].session_volume_id, session_volume_id);
}
