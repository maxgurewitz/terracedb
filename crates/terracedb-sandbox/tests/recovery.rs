use std::sync::Arc;

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    DefaultSandboxStore, GitProvenance, ReopenSessionOptions, SandboxConfig, SandboxServices,
    SandboxStore, TERRACE_SESSION_INFO_KV_KEY, TERRACE_SESSION_METADATA_PATH,
};
use terracedb_vfs::{
    CloneVolumeSource, CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore,
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

async fn seed_base(store: &InMemoryVfsStore, volume_id: VolumeId) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base");
    base.fs()
        .write_file(
            "/workspace/repo.txt",
            b"repo".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed base");
}

#[tokio::test]
async fn reopen_prefers_file_metadata_and_repairs_missing_kv_mirror() {
    let (vfs, sandbox) = sandbox_store(40, 201);
    let base_volume_id = VolumeId::new(0x8000);
    let session_volume_id = VolumeId::new(0x8001);
    seed_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let mut updated = session.info().await;
    updated.revision += 10;
    updated.provenance.git = Some(GitProvenance {
        repo_root: "/repo".to_string(),
        head_commit: Some("cafebabe".to_string()),
        branch: Some("file-first".to_string()),
        remote_url: None,
        pathspec: vec![".".to_string()],
        dirty: false,
    });
    session
        .volume()
        .fs()
        .write_file(
            TERRACE_SESSION_METADATA_PATH,
            serde_json::to_vec_pretty(&updated).expect("encode updated info"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write session file only");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen session");
    let reopened_info = reopened.info().await;
    assert_eq!(reopened_info.revision, updated.revision);
    assert_eq!(
        reopened_info
            .provenance
            .git
            .clone()
            .and_then(|git| git.branch)
            .expect("branch"),
        "file-first"
    );
    assert_eq!(
        reopened
            .kv()
            .get_json(TERRACE_SESSION_INFO_KV_KEY)
            .await
            .expect("session kv"),
        Some(serde_json::to_value(&reopened_info).expect("encode reopened info"))
    );
}

#[tokio::test]
async fn durable_recovery_only_sees_flushed_provenance_updates() {
    let (source_vfs, sandbox) = sandbox_store(50, 202);
    let base_volume_id = VolumeId::new(0x8100);
    let session_volume_id = VolumeId::new(0x8101);
    seed_base(&source_vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session.flush().await.expect("flush initial session");
    session
        .update_provenance(|provenance| {
            provenance.git = Some(GitProvenance {
                repo_root: "/repo".to_string(),
                head_commit: Some("1".to_string()),
                branch: Some("pending".to_string()),
                remote_url: None,
                pathspec: vec![".".to_string()],
                dirty: false,
            });
        })
        .await
        .expect("update provenance");

    let unflushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut before flush");
    let (unflushed_vfs, unflushed_sandbox) = sandbox_store(60, 203);
    unflushed_vfs
        .import_volume(
            unflushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut before flush");
    let unflushed_session = unflushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen durable cut before flush");
    assert!(unflushed_session.info().await.provenance.git.is_none());

    session.flush().await.expect("flush updated provenance");

    let flushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut after flush");
    let (flushed_vfs, flushed_sandbox) = sandbox_store(70, 204);
    flushed_vfs
        .import_volume(
            flushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut after flush");
    let flushed_session = flushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen durable cut after flush");
    assert_eq!(
        flushed_session
            .info()
            .await
            .provenance
            .git
            .and_then(|git| git.branch)
            .expect("flushed branch"),
        "pending"
    );
}
