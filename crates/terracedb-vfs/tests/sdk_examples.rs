use std::sync::Arc;

use futures::TryStreamExt;
use serde_json::json;

use terracedb::{DbDependencies, LogCursor, StubClock, StubFileSystem, StubObjectStore, StubRng};
use terracedb_vfs::{
    ActivityKind, CreateOptions, InMemoryVfsStore, MkdirOptions, ToolRunStore, VfsFileSystem,
    VfsKvStore, VfsStoreExt, VfsVolumeExt, VolumeConfig, VolumeId, VolumeStore,
};

fn test_store() -> InMemoryVfsStore {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(0x3939)),
    );
    InMemoryVfsStore::with_dependencies(dependencies)
}

async fn run_vfs_session(
    fs: Arc<dyn VfsFileSystem>,
    kv: Arc<dyn VfsKvStore>,
    tools: Arc<dyn ToolRunStore>,
) -> Result<(), terracedb_vfs::VfsError> {
    fs.write_file(
        "/workspace/result.txt",
        b"generated in overlay".to_vec(),
        CreateOptions::default(),
    )
    .await?;
    kv.set_json("last_tool", json!("draft")).await?;
    let run = tools
        .start("draft", Some(json!({ "path": "/workspace/result.txt" })))
        .await?;
    tools.success(run, Some(json!({ "ok": true }))).await?;
    Ok(())
}

#[tokio::test]
async fn sdk_helpers_support_base_volume_overlay_and_capability_scoping() {
    let store = test_store();
    let base = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x401))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");

    base.fs()
        .mkdir(
            "/workspace",
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("mkdir workspace");
    base.fs()
        .write_file(
            "/workspace/prompt.txt",
            b"base prompt".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write base prompt");

    let overlay = store
        .create_overlay_from_volume(
            base.clone(),
            terracedb_vfs::SnapshotOptions::default(),
            VolumeConfig::new(VolumeId::new(0x402))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("create overlay from base volume");

    run_vfs_session(overlay.fs(), overlay.kv(), overlay.tools())
        .await
        .expect("run vfs session");

    assert_eq!(
        overlay
            .fs()
            .read_file("/workspace/prompt.txt")
            .await
            .expect("read inherited prompt"),
        Some(b"base prompt".to_vec())
    );
    assert_eq!(
        overlay
            .fs()
            .read_file("/workspace/result.txt")
            .await
            .expect("read overlay result"),
        Some(b"generated in overlay".to_vec())
    );
    assert_eq!(
        base.fs()
            .read_file("/workspace/result.txt")
            .await
            .expect("read base result"),
        None
    );
    assert_eq!(
        overlay
            .kv()
            .get_json("last_tool")
            .await
            .expect("overlay kv"),
        Some(json!("draft"))
    );

    let activities = overlay
        .visible_activity_since(LogCursor::beginning())
        .await
        .expect("overlay activity stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect overlay activities");
    assert!(
        activities
            .iter()
            .any(|entry| entry.kind == ActivityKind::OverlayCreated)
    );
    assert!(
        activities
            .iter()
            .any(|entry| entry.kind == ActivityKind::ToolSucceeded)
    );
}

#[tokio::test]
async fn volume_extensions_expose_visible_and_durable_views() {
    let store = test_store();
    let volume = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x403))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");

    volume
        .fs()
        .mkdir(
            "/workspace",
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("mkdir workspace");
    volume
        .fs()
        .write_file(
            "/workspace/visible.txt",
            b"visible".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write file");

    let visible = volume.visible_snapshot().await.expect("visible snapshot");
    let durable = volume.durable_snapshot().await.expect("durable snapshot");
    assert!(
        visible
            .fs()
            .stat("/workspace")
            .await
            .expect("visible stat")
            .is_some()
    );
    assert!(
        durable
            .fs()
            .stat("/workspace")
            .await
            .expect("durable stat")
            .is_none()
    );

    let visible_activities = volume
        .visible_activity_since(LogCursor::beginning())
        .await
        .expect("visible activities")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect visible activities");
    let durable_activities = volume
        .durable_activity_since(LogCursor::beginning())
        .await
        .expect("durable activities")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect durable activities");
    assert!(!visible_activities.is_empty());
    assert!(durable_activities.is_empty());

    volume.flush().await.expect("flush volume");
    let durable_after_flush = volume
        .durable_activity_since(LogCursor::beginning())
        .await
        .expect("durable activities after flush")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect durable activities after flush");
    assert_eq!(durable_after_flush, visible_activities);
}
