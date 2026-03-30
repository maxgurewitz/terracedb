use std::sync::Arc;

use futures::TryStreamExt;
use serde_json::json;

use terracedb::{DbDependencies, LogCursor, StubClock, StubFileSystem, StubObjectStore, StubRng};
use terracedb_vfs::{
    ActivityKey, ActivityKind, ActivityOptions, AgentFsConfig, AgentFsStore, ChunkKey,
    CloneVolumeSource, CreateOptions, DentryKey, InMemoryAgentFsStore, InodeId, SnapshotOptions,
    VolumeId,
};

#[tokio::test]
async fn public_vfs_surface_compiles_and_is_instantiable() {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(42)),
    );
    let store = InMemoryAgentFsStore::with_dependencies(dependencies);

    let volume = store
        .open_volume(
            AgentFsConfig::new(VolumeId::new(1))
                .with_chunk_size(8192)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");

    assert_eq!(volume.info().root_inode, InodeId::new(1));

    let fs = volume.fs();
    fs.mkdir(
        "/workspace",
        terracedb_vfs::MkdirOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .expect("mkdir");
    fs.write_file(
        "/workspace/readme.txt",
        b"hello terracedb".to_vec(),
        CreateOptions::default(),
    )
    .await
    .expect("write file");
    assert_eq!(
        fs.read_file("/workspace/readme.txt")
            .await
            .expect("read file"),
        Some(b"hello terracedb".to_vec())
    );

    let kv = volume.kv();
    kv.set_json("mode", json!("demo")).await.expect("set kv");
    assert_eq!(
        kv.get_json("mode").await.expect("get kv"),
        Some(json!("demo"))
    );

    let tools = volume.tools();
    let tool_run_id = tools
        .start("echo", Some(json!({ "cmd": "pwd" })))
        .await
        .expect("start tool");
    tools
        .success(tool_run_id, Some(json!({ "ok": true })))
        .await
        .expect("complete tool");

    let snapshot = volume
        .snapshot(SnapshotOptions::default())
        .await
        .expect("snapshot");
    assert_eq!(
        snapshot
            .fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("read snapshot"),
        Some(b"hello terracedb".to_vec())
    );

    let cloned = store
        .clone_volume(
            CloneVolumeSource::new(volume.info().volume_id),
            AgentFsConfig::new(VolumeId::new(2))
                .with_create_if_missing(true)
                .with_chunk_size(8192),
        )
        .await
        .expect("clone volume");
    assert_eq!(
        cloned
            .fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("read cloned file"),
        Some(b"hello terracedb".to_vec())
    );

    let overlay = store
        .create_overlay(
            snapshot,
            AgentFsConfig::new(VolumeId::new(3))
                .with_create_if_missing(true)
                .with_chunk_size(8192),
        )
        .await
        .expect("create overlay");
    assert_eq!(overlay.base().info().volume_id, volume.info().volume_id);
    assert_eq!(
        overlay
            .fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("read overlay file"),
        Some(b"hello terracedb".to_vec())
    );

    let activities = volume
        .activity_since(LogCursor::beginning(), ActivityOptions::default())
        .await
        .expect("activity stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect activity");
    assert!(
        activities
            .iter()
            .any(|entry| entry.kind == ActivityKind::DirectoryCreated)
    );
    assert!(
        activities
            .iter()
            .any(|entry| entry.kind == ActivityKind::FileWritten)
    );
    assert!(
        activities
            .iter()
            .any(|entry| entry.kind == ActivityKind::KvSet)
    );
    assert!(
        activities
            .iter()
            .any(|entry| entry.kind == ActivityKind::ToolSucceeded)
    );
}

#[test]
fn reserved_key_encodings_round_trip_and_preserve_scan_order() {
    let volume_id = VolumeId::new(0x10);

    let dentries = [
        DentryKey {
            volume_id,
            parent: InodeId::new(1),
            name: "alpha".to_string(),
        },
        DentryKey {
            volume_id,
            parent: InodeId::new(1),
            name: "beta".to_string(),
        },
        DentryKey {
            volume_id,
            parent: InodeId::new(2),
            name: "aardvark".to_string(),
        },
    ];
    let chunks = [
        ChunkKey {
            volume_id,
            inode: InodeId::new(7),
            chunk_index: 0,
        },
        ChunkKey {
            volume_id,
            inode: InodeId::new(7),
            chunk_index: 1,
        },
        ChunkKey {
            volume_id,
            inode: InodeId::new(8),
            chunk_index: 0,
        },
    ];
    let activities = [
        ActivityKey {
            volume_id,
            activity_id: terracedb_vfs::ActivityId::new(1),
        },
        ActivityKey {
            volume_id,
            activity_id: terracedb_vfs::ActivityId::new(2),
        },
        ActivityKey {
            volume_id: VolumeId::new(0x11),
            activity_id: terracedb_vfs::ActivityId::new(1),
        },
    ];

    for key in &dentries {
        let decoded = DentryKey::decode(&key.encode()).expect("decode dentry key");
        assert_eq!(decoded, *key);
    }
    for key in &chunks {
        let decoded = ChunkKey::decode(&key.encode()).expect("decode chunk key");
        assert_eq!(decoded, *key);
    }
    for key in &activities {
        let decoded = ActivityKey::decode(&key.encode()).expect("decode activity key");
        assert_eq!(decoded, *key);
    }

    let mut encoded_dentries = dentries.iter().map(DentryKey::encode).collect::<Vec<_>>();
    encoded_dentries.sort();
    assert_eq!(
        encoded_dentries,
        dentries.iter().map(DentryKey::encode).collect::<Vec<_>>()
    );

    let mut encoded_chunks = chunks.iter().map(ChunkKey::encode).collect::<Vec<_>>();
    encoded_chunks.sort();
    assert_eq!(
        encoded_chunks,
        chunks.iter().map(ChunkKey::encode).collect::<Vec<_>>()
    );

    let mut encoded_activities = activities
        .iter()
        .map(ActivityKey::encode)
        .collect::<Vec<_>>();
    encoded_activities.sort();
    assert_eq!(
        encoded_activities,
        activities
            .iter()
            .map(ActivityKey::encode)
            .collect::<Vec<_>>()
    );
}
