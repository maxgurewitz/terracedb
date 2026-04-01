use std::sync::Arc;

use futures::TryStreamExt;
use terracedb::{
    DbDependencies, LogCursor, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_sandbox::{
    CapabilityManifest, ConflictPolicy, DefaultSandboxStore, GitProvenance, HoistMode,
    HoistedSource, PackageCompatibilityMode, ReadonlyViewCut, ReadonlyViewRequest,
    ReopenSessionOptions, SandboxCapability, SandboxConfig, SandboxServices, SandboxSessionState,
    SandboxStore, TERRACE_SESSION_INFO_KV_KEY,
};
use terracedb_vfs::{
    ActivityKind, ActivityOptions, CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId,
    VolumeStore,
};

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

async fn base_volume(store: &InMemoryVfsStore, volume_id: VolumeId) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
    base.fs()
        .write_file(
            "/workspace/base.txt",
            b"base".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed base");
}

#[tokio::test]
async fn reopen_preserves_session_metadata_and_provenance() {
    let (vfs, sandbox) = sandbox_store(20, 91);
    let base_volume_id = VolumeId::new(0x6100);
    let session_volume_id = VolumeId::new(0x6101);
    base_volume(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: CapabilityManifest {
                capabilities: vec![SandboxCapability::host_module("tickets")],
            },
            execution_policy: None,
            hoisted_source: Some(HoistedSource {
                source_path: "/repo".to_string(),
                mode: HoistMode::DirectorySnapshot,
            }),
            git_provenance: Some(GitProvenance {
                repo_root: "/repo".to_string(),
                head_commit: Some("abc".to_string()),
                branch: Some("main".to_string()),
                remote_url: None,
                pathspec: vec![".".to_string()],
                dirty: false,
            }),
        })
        .await
        .expect("open session");

    let expected = session.info().await;
    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen session");
    assert_eq!(reopened.info().await, expected);
    assert_eq!(
        reopened
            .filesystem()
            .read_file("/workspace/base.txt")
            .await
            .expect("read overlay file"),
        Some(b"base".to_vec())
    );
}

#[tokio::test]
async fn lifecycle_operations_append_tool_runs_and_track_overlay_state() {
    let (vfs, sandbox) = sandbox_store(30, 92);
    let base_volume_id = VolumeId::new(0x6200);
    let session_volume_id = VolumeId::new(0x6201);
    base_volume(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    assert!(session.volume().info().overlay_base.is_some());

    let handle = session
        .open_readonly_view(ReadonlyViewRequest {
            cut: ReadonlyViewCut::Visible,
            path: "/workspace".to_string(),
            label: Some("workspace".to_string()),
        })
        .await
        .expect("open view");
    session
        .close_readonly_view(&handle.handle_id)
        .await
        .expect("close view");
    session
        .set_conflict_policy(ConflictPolicy::CreatePatchBundle)
        .await
        .expect("set policy");
    session
        .close(terracedb_sandbox::CloseSessionOptions::default())
        .await
        .expect("close session");

    let info = session.info().await;
    assert_eq!(info.state, SandboxSessionState::Closed);
    assert!(info.provenance.active_view_handles.is_empty());
    assert_eq!(
        session
            .kv()
            .get_json(TERRACE_SESSION_INFO_KV_KEY)
            .await
            .expect("session kv mirror"),
        Some(serde_json::to_value(&info).expect("encode info"))
    );

    let tool_names = session
        .volume()
        .tools()
        .recent(None)
        .await
        .expect("recent tool runs")
        .into_iter()
        .map(|run| run.name)
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"sandbox.session.open".to_string()));
    assert!(tool_names.contains(&"sandbox.view.open".to_string()));
    assert!(tool_names.contains(&"sandbox.view.close".to_string()));
    assert!(tool_names.contains(&"sandbox.session.set_conflict_policy".to_string()));
    assert!(tool_names.contains(&"sandbox.session.close".to_string()));

    let activities = session
        .volume()
        .activity_since(LogCursor::beginning(), ActivityOptions::default())
        .await
        .expect("activity stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect activity");
    assert!(
        activities
            .iter()
            .filter(|entry| entry.kind == ActivityKind::ToolSucceeded)
            .count()
            >= 5
    );
}
