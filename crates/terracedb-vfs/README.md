# terracedb-vfs

`terracedb-vfs` is the embedded, path-based virtual filesystem library that sits on top of Terracedb. The public surface is intentionally small and runtime-neutral:

- `VolumeStore` opens volumes, clones, and overlays.
- `Volume` exposes filesystem, KV, tool-run, snapshot, activity-tail, and flush operations.
- `VfsFileSystem`, `VfsKvStore`, and `ToolRunStore` are the capability-oriented surfaces you hand to embedded runtimes.
- `VfsStoreExt` and `VfsVolumeExt` provide convenience helpers for the common "base volume + overlay session" and visible-vs-durable cut patterns.

The goal for version 1 is an embedded Rust SDK for in-process embedded sandboxes. It is explicitly not a mount or service API.

## Version 1 Non-Goals

- No FUSE, NFS, MCP, HTTP, CLI, or daemon/service boundary.
- No host filesystem mount surface.
- No mount-oriented inode or file-handle API.
- No direct writes to reserved `vfs_*` tables from application helpers or examples.

## Attribution

`terracedb-vfs` is a Terracedb-native implementation, but the problem framing and some of the motivating surface area were informed by [AgentFS](https://github.com/tursodatabase/agentfs) from Turso.

AgentFS is distributed under the [MIT license](https://github.com/tursodatabase/agentfs/blob/main/LICENSE.md). This README credits and links to AgentFS as related prior art; if any future work copies code or other substantial portions from AgentFS, the corresponding MIT copyright and license notice should be preserved alongside that copied material.

## Example

```rust
use std::sync::Arc;

use futures::{TryStreamExt, executor::block_on};
use serde_json::json;
use terracedb::{DbDependencies, LogCursor, StubClock, StubFileSystem, StubObjectStore, StubRng};
use terracedb_vfs::{
    ActivityKind, VfsFileSystem, VolumeConfig, VolumeStore, VfsStoreExt,
    VfsVolumeExt, VfsKvStore, ToolRunStore, CreateOptions, InMemoryVfsStore,
    MkdirOptions, SnapshotOptions, VolumeId,
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
    let run = tools.start("draft", Some(json!({ "path": "/workspace/result.txt" }))).await?;
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
        assert!(activities.iter().any(|entry| entry.kind == ActivityKind::OverlayCreated));
        assert!(activities.iter().any(|entry| entry.kind == ActivityKind::ToolSucceeded));
        assert_eq!(
            overlay.fs().read_file("/workspace/prompt.txt").await?,
            Some(b"base prompt".to_vec())
        );
        assert_eq!(
            overlay.fs().read_file("/workspace/result.txt").await?,
            Some(b"generated in overlay".to_vec())
        );

        Ok::<(), terracedb_vfs::VfsError>(())
    })?;
    Ok(())
}
```

See [`examples/embedded_overlay.rs`](examples/embedded_overlay.rs) for the same flow as a standalone example binary.
