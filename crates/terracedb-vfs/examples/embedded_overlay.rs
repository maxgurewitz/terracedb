use std::sync::Arc;

use futures::{TryStreamExt, executor::block_on};
use serde_json::json;

use terracedb::{DbDependencies, LogCursor, StubClock, StubFileSystem, StubObjectStore, StubRng};
use terracedb_vfs::{
    ActivityKind, CreateOptions, InMemoryVfsStore, MkdirOptions, SnapshotOptions, ToolRunStore,
    VfsFileSystem, VfsKvStore, VfsStoreExt, VfsVolumeExt, VolumeConfig, VolumeId, VolumeStore,
};

async fn vfs_session(
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let dependencies = DbDependencies::new(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
            Arc::new(StubClock::default()),
            Arc::new(StubRng::seeded(7)),
        );
        let store = InMemoryVfsStore::with_dependencies(dependencies);

        let base = store
            .open_volume(
                VolumeConfig::new(VolumeId::new(1))
                    .with_chunk_size(4096)
                    .with_create_if_missing(true),
            )
            .await?;
        base.fs()
            .mkdir(
                "/workspace",
                MkdirOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await?;
        base.fs()
            .write_file(
                "/workspace/prompt.txt",
                b"base prompt".to_vec(),
                CreateOptions::default(),
            )
            .await?;

        let overlay = store
            .create_overlay_from_volume(
                base.clone(),
                SnapshotOptions::default(),
                VolumeConfig::new(VolumeId::new(2))
                    .with_chunk_size(4096)
                    .with_create_if_missing(true),
            )
            .await?;

        vfs_session(overlay.fs(), overlay.kv(), overlay.tools()).await?;

        let activities = overlay
            .visible_activity_since(LogCursor::beginning())
            .await?
            .try_collect::<Vec<_>>()
            .await?;
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

        println!(
            "overlay volume {} now has {} activity entries",
            overlay.info().volume_id.get(),
            activities.len()
        );

        Ok::<(), terracedb_vfs::VfsError>(())
    })?;
    Ok(())
}
