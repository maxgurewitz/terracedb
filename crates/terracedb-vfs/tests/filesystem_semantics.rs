use std::sync::Arc;

use terracedb::{StubClock, StubRng, Timestamp};
use terracedb_vfs::{
    CreateOptions, FileKind, InMemoryVfsStore, MkdirOptions, SnapshotOptions, VfsError,
    VolumeConfig, VolumeId, VolumeStore,
};

fn test_store(clock: Arc<StubClock>) -> InMemoryVfsStore {
    InMemoryVfsStore::new(clock, Arc::new(StubRng::seeded(7)))
}

async fn open_volume(
    store: &InMemoryVfsStore,
    volume_id: u128,
    chunk_size: u32,
) -> Arc<dyn terracedb_vfs::Volume> {
    store
        .open_volume(
            VolumeConfig::new(VolumeId::new(volume_id))
                .with_chunk_size(chunk_size)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume")
}

#[tokio::test]
async fn chunked_file_io_preserves_snapshot_reads_and_holes() {
    let clock = Arc::new(StubClock::new(Timestamp::new(10)));
    let store = test_store(clock.clone());
    let volume = open_volume(&store, 1, 4).await;
    let fs = volume.fs();

    fs.write_file(
        "/chunks.bin",
        b"abcdefghij".to_vec(),
        CreateOptions::default(),
    )
    .await
    .expect("write base file");
    let snapshot = volume
        .snapshot(SnapshotOptions::default())
        .await
        .expect("snapshot");

    clock.set(Timestamp::new(20));
    fs.pwrite("/chunks.bin", 12, b"!".to_vec())
        .await
        .expect("write past eof");
    assert_eq!(
        fs.pread("/chunks.bin", 8, 5).await.expect("pread"),
        Some(vec![b'i', b'j', 0, 0, b'!'])
    );
    assert_eq!(
        fs.read_file("/chunks.bin").await.expect("read file"),
        Some(b"abcdefghij\0\0!".to_vec())
    );

    clock.set(Timestamp::new(30));
    fs.pwrite("/chunks.bin", 3, b"XYZ12".to_vec())
        .await
        .expect("overwrite across chunks");
    assert_eq!(
        fs.pread("/chunks.bin", 2, 8)
            .await
            .expect("pread after patch"),
        Some(b"cXYZ12ij".to_vec())
    );
    let stats = fs
        .stat("/chunks.bin")
        .await
        .expect("stat file")
        .expect("file stats");
    assert_eq!(stats.size, 13);
    assert_eq!(stats.modified_at, Timestamp::new(30));

    clock.set(Timestamp::new(40));
    fs.truncate("/chunks.bin", 5).await.expect("truncate down");
    assert_eq!(
        fs.read_file("/chunks.bin")
            .await
            .expect("read truncated file"),
        Some(b"abcXY".to_vec())
    );

    clock.set(Timestamp::new(50));
    fs.truncate("/chunks.bin", 8).await.expect("truncate up");
    assert_eq!(
        fs.read_file("/chunks.bin")
            .await
            .expect("read expanded file"),
        Some(b"abcXY\0\0\0".to_vec())
    );

    assert_eq!(
        snapshot
            .fs()
            .read_file("/chunks.bin")
            .await
            .expect("read snapshot"),
        Some(b"abcdefghij".to_vec())
    );
}

#[tokio::test]
async fn symlinks_and_hard_links_track_their_own_semantics() {
    let clock = Arc::new(StubClock::new(Timestamp::new(10)));
    let store = test_store(clock.clone());
    let volume = open_volume(&store, 2, 8).await;
    let fs = volume.fs();

    fs.write_file("/target.txt", b"hello".to_vec(), CreateOptions::default())
        .await
        .expect("write target");
    fs.link("/target.txt", "/alias.txt")
        .await
        .expect("create hard link");
    fs.mkdir("/dir", MkdirOptions::default())
        .await
        .expect("mkdir dir");
    fs.symlink("../alias.txt", "/dir/link.txt")
        .await
        .expect("create symlink");

    assert_eq!(
        fs.stat("/target.txt")
            .await
            .expect("stat target")
            .expect("target stats")
            .nlink,
        2
    );
    assert_eq!(
        fs.lstat("/dir/link.txt")
            .await
            .expect("lstat symlink")
            .expect("symlink stats")
            .kind,
        FileKind::Symlink
    );
    assert_eq!(
        fs.stat("/dir/link.txt")
            .await
            .expect("stat symlink")
            .expect("resolved stats")
            .kind,
        FileKind::File
    );
    assert_eq!(
        fs.readlink("/dir/link.txt").await.expect("readlink"),
        "../alias.txt"
    );
    assert_eq!(
        fs.read_file("/dir/link.txt")
            .await
            .expect("read through symlink"),
        Some(b"hello".to_vec())
    );

    clock.set(Timestamp::new(20));
    fs.write_file(
        "/dir/link.txt",
        b"updated".to_vec(),
        CreateOptions::default(),
    )
    .await
    .expect("write through symlink");
    assert_eq!(
        fs.read_file("/target.txt").await.expect("read target"),
        Some(b"updated".to_vec())
    );

    fs.unlink("/target.txt")
        .await
        .expect("unlink one hard link");
    assert_eq!(
        fs.stat("/alias.txt")
            .await
            .expect("stat alias")
            .expect("alias stats")
            .nlink,
        1
    );
    assert_eq!(
        fs.read_file("/alias.txt").await.expect("read alias"),
        Some(b"updated".to_vec())
    );

    fs.unlink("/dir/link.txt").await.expect("unlink symlink");
    assert!(
        fs.lstat("/dir/link.txt")
            .await
            .expect("lstat removed symlink")
            .is_none()
    );
    assert_eq!(
        fs.read_file("/alias.txt")
            .await
            .expect("read remaining link"),
        Some(b"updated".to_vec())
    );
}

#[tokio::test]
async fn rename_readdir_and_directory_link_counts_are_maintained() {
    let clock = Arc::new(StubClock::new(Timestamp::new(10)));
    let store = test_store(clock);
    let volume = open_volume(&store, 3, 4).await;
    let fs = volume.fs();

    fs.mkdir("/workspace", MkdirOptions::default())
        .await
        .expect("mkdir workspace");
    fs.mkdir("/workspace/src", MkdirOptions::default())
        .await
        .expect("mkdir source dir");
    fs.mkdir("/tmp", MkdirOptions::default())
        .await
        .expect("mkdir tmp");
    fs.write_file(
        "/workspace/file.txt",
        b"old".to_vec(),
        CreateOptions::default(),
    )
    .await
    .expect("write workspace file");
    fs.write_file("/tmp/file.txt", b"new".to_vec(), CreateOptions::default())
        .await
        .expect("write tmp file");

    let mut workspace_entries = fs
        .readdir("/workspace")
        .await
        .expect("readdir workspace")
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    workspace_entries.sort();
    assert_eq!(
        workspace_entries,
        vec!["file.txt".to_string(), "src".to_string()]
    );

    assert_eq!(
        fs.lstat("/")
            .await
            .expect("stat root")
            .expect("root stats")
            .nlink,
        4
    );
    assert_eq!(
        fs.lstat("/workspace")
            .await
            .expect("stat workspace")
            .expect("workspace stats")
            .nlink,
        3
    );

    fs.rename("/tmp/file.txt", "/workspace/file.txt")
        .await
        .expect("rename over file");
    assert!(
        fs.read_file("/tmp/file.txt")
            .await
            .expect("read old path")
            .is_none()
    );
    assert_eq!(
        fs.read_file("/workspace/file.txt")
            .await
            .expect("read new path"),
        Some(b"new".to_vec())
    );

    assert!(matches!(
        fs.rmdir("/workspace").await,
        Err(VfsError::DirectoryNotEmpty { .. })
    ));

    fs.rename("/workspace/src", "/src")
        .await
        .expect("move directory");
    assert_eq!(
        fs.lstat("/workspace")
            .await
            .expect("stat workspace after move")
            .expect("workspace stats after move")
            .nlink,
        2
    );
    assert_eq!(
        fs.lstat("/")
            .await
            .expect("stat root after move")
            .expect("root stats after move")
            .nlink,
        5
    );

    fs.unlink("/workspace/file.txt")
        .await
        .expect("remove workspace file");
    fs.rmdir("/workspace")
        .await
        .expect("remove empty workspace");

    let mut root_entries = fs
        .readdir_plus("/")
        .await
        .expect("readdir root")
        .into_iter()
        .map(|entry| (entry.entry.name, entry.stats.kind))
        .collect::<Vec<_>>();
    root_entries.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        root_entries,
        vec![
            ("src".to_string(), FileKind::Directory),
            ("tmp".to_string(), FileKind::Directory),
        ]
    );
}
