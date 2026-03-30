use std::sync::Arc;

use futures::{TryStreamExt, future::join_all};

use terracedb::{DbDependencies, LogCursor, StubClock, StubFileSystem, StubObjectStore, StubRng};
use terracedb_vfs::{
    ActivityOptions, AgentFsConfig, AgentFsError, AgentFsStore, CompletedToolRun,
    CompletedToolRunOutcome, CreateOptions, FileKind, InMemoryAgentFsStore, MkdirOptions,
    SnapshotOptions, VolumeId,
};

fn test_store() -> InMemoryAgentFsStore {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(0x5151)),
    );
    InMemoryAgentFsStore::with_dependencies(dependencies)
}

fn sorted_names(entries: Vec<terracedb_vfs::DirEntry>) -> Vec<String> {
    let mut names = entries
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[tokio::test]
async fn reopen_preserves_volume_metadata_root_and_config() {
    let store = test_store();
    let config = AgentFsConfig::new(VolumeId::new(0x35))
        .with_chunk_size(8192)
        .with_create_if_missing(true);

    let volume = store
        .open_volume(config.clone())
        .await
        .expect("create volume");
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
        .expect("mkdir");

    let root = volume
        .fs()
        .stat("/")
        .await
        .expect("stat root")
        .expect("root exists");
    assert_eq!(root.inode.get(), 1);
    assert_eq!(root.kind, FileKind::Directory);

    let reopened = store
        .open_volume(
            AgentFsConfig::new(config.volume_id)
                .with_chunk_size(8192)
                .with_create_if_missing(false),
        )
        .await
        .expect("reopen volume");
    assert_eq!(reopened.info(), volume.info());
    assert!(
        reopened
            .fs()
            .stat("/workspace")
            .await
            .expect("stat workspace")
            .is_some()
    );

    let mismatch = store
        .open_volume(
            AgentFsConfig::new(config.volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(false),
        )
        .await
        .err()
        .expect("mismatched chunk size should fail");
    assert!(matches!(
        mismatch,
        AgentFsError::VolumeConfigMismatch {
            requested_chunk_size: 4096,
            ..
        }
    ));
}

#[tokio::test]
async fn allocators_remain_monotonic_across_reopen_and_concurrent_refresh() {
    let store = test_store();
    let volume = store
        .open_volume(
            AgentFsConfig::new(VolumeId::new(0x36))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");

    volume
        .fs()
        .mkdir(
            "/files",
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("mkdir");

    let fs = volume.fs();
    let write_futures = (0..48).map(|index| {
        let fs = fs.clone();
        async move {
            let path = format!("/files/{index}.txt");
            fs.write_file(
                &path,
                format!("file-{index}").into_bytes(),
                CreateOptions::default(),
            )
            .await
            .expect("write file");
            fs.stat(&path)
                .await
                .expect("stat file")
                .expect("file exists")
                .inode
                .get()
        }
    });
    let mut inode_ids = join_all(write_futures).await;
    inode_ids.sort();
    inode_ids.dedup();
    assert_eq!(inode_ids.len(), 48);
    assert!(inode_ids.iter().all(|inode| *inode > 1));

    let tools = volume.tools();
    let tool_futures = (0..48).map(|index| {
        let tools = tools.clone();
        async move {
            tools
                .record_completed(CompletedToolRun {
                    name: format!("tool-{index}"),
                    params: None,
                    outcome: CompletedToolRunOutcome::Success { result: None },
                })
                .await
                .expect("record tool run")
                .get()
        }
    });
    let mut tool_ids = join_all(tool_futures).await;
    tool_ids.sort();
    tool_ids.dedup();
    assert_eq!(tool_ids.len(), 48);
    assert_eq!(tool_ids.first().copied(), Some(1));

    let max_inode = inode_ids.last().copied().expect("inode ids");
    let max_tool = tool_ids.last().copied().expect("tool ids");
    let activity_before = volume
        .activity_since(LogCursor::beginning(), ActivityOptions::default())
        .await
        .expect("activity stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect activity");
    let max_activity = activity_before
        .last()
        .expect("activity ids")
        .activity_id
        .get();

    let reopened = store
        .open_volume(
            AgentFsConfig::new(VolumeId::new(0x36))
                .with_chunk_size(4096)
                .with_create_if_missing(false),
        )
        .await
        .expect("reopen volume");

    reopened
        .fs()
        .write_file(
            "/files/reopened.txt",
            b"after-reopen".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write reopened file");
    let reopened_inode = reopened
        .fs()
        .stat("/files/reopened.txt")
        .await
        .expect("stat reopened file")
        .expect("reopened file exists")
        .inode
        .get();
    assert!(reopened_inode > max_inode);

    let reopened_tool = reopened
        .tools()
        .record_completed(CompletedToolRun {
            name: "post-reopen".to_string(),
            params: None,
            outcome: CompletedToolRunOutcome::Success { result: None },
        })
        .await
        .expect("record reopened tool")
        .get();
    assert!(reopened_tool > max_tool);

    let activity_after = reopened
        .activity_since(LogCursor::beginning(), ActivityOptions::default())
        .await
        .expect("activity stream after reopen")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect activity after reopen");
    let reopened_activity = activity_after
        .last()
        .expect("activity after reopen")
        .activity_id
        .get();
    assert!(reopened_activity > max_activity);
}

#[tokio::test]
async fn durable_snapshot_and_activity_fence_visible_state_until_fsync() {
    let store = test_store();
    let volume = store
        .open_volume(
            AgentFsConfig::new(VolumeId::new(0x37))
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
        .expect("mkdir");
    volume
        .fs()
        .write_file(
            "/workspace/readme.txt",
            b"durability-cut".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write file");

    let durable_before = volume
        .snapshot(SnapshotOptions { durable: true })
        .await
        .expect("durable snapshot before fsync");
    assert_eq!(
        durable_before
            .fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("read durable snapshot before fsync"),
        None
    );

    let visible_activities = volume
        .activity_since(LogCursor::beginning(), ActivityOptions::default())
        .await
        .expect("visible activity stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect visible activities");
    let durable_activities_before = volume
        .activity_since(
            LogCursor::beginning(),
            ActivityOptions {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("durable activity stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect durable activities");
    assert!(!visible_activities.is_empty());
    assert!(durable_activities_before.is_empty());

    volume.fs().fsync(None).await.expect("fsync");

    let durable_after = volume
        .snapshot(SnapshotOptions { durable: true })
        .await
        .expect("durable snapshot after fsync");
    assert_eq!(
        durable_after
            .fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("read durable snapshot after fsync"),
        Some(b"durability-cut".to_vec())
    );

    let durable_activities_after = volume
        .activity_since(
            LogCursor::beginning(),
            ActivityOptions {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("durable activity stream after fsync")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect durable activities after fsync");
    assert_eq!(durable_activities_after, visible_activities);
}

#[tokio::test]
async fn snapshot_path_resolution_and_chunk_reads_do_not_mix_versions() {
    let store = test_store();
    let volume = store
        .open_volume(
            AgentFsConfig::new(VolumeId::new(0x38))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");

    volume
        .fs()
        .mkdir(
            "/dir",
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("mkdir");
    volume
        .fs()
        .write_file(
            "/dir/file.txt",
            b"abcdefgh".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write file");

    let snapshot = volume
        .snapshot(SnapshotOptions::default())
        .await
        .expect("create snapshot");

    volume
        .fs()
        .rename("/dir/file.txt", "/dir/renamed.txt")
        .await
        .expect("rename file");
    volume
        .fs()
        .pwrite("/dir/renamed.txt", 2, b"ZZZZ".to_vec())
        .await
        .expect("patch file");

    assert_eq!(
        snapshot
            .fs()
            .read_file("/dir/file.txt")
            .await
            .expect("snapshot read old path"),
        Some(b"abcdefgh".to_vec())
    );
    assert_eq!(
        snapshot
            .fs()
            .pread("/dir/file.txt", 1, 6)
            .await
            .expect("snapshot pread old path"),
        Some(b"bcdefg".to_vec())
    );
    assert_eq!(
        snapshot
            .fs()
            .read_file("/dir/renamed.txt")
            .await
            .expect("snapshot read renamed path"),
        None
    );
    assert_eq!(
        sorted_names(
            snapshot
                .fs()
                .readdir("/dir")
                .await
                .expect("snapshot readdir"),
        ),
        vec!["file.txt".to_string()]
    );

    assert_eq!(
        volume
            .fs()
            .read_file("/dir/file.txt")
            .await
            .expect("live read old path"),
        None
    );
    assert_eq!(
        volume
            .fs()
            .pread("/dir/renamed.txt", 1, 6)
            .await
            .expect("live pread renamed path"),
        Some(b"bZZZZg".to_vec())
    );
    assert_eq!(
        sorted_names(volume.fs().readdir("/dir").await.expect("live readdir")),
        vec!["renamed.txt".to_string()]
    );
}
