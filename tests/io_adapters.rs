use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::TryStreamExt;
use terracedb::{
    Clock, DeterministicRng, FileSystem, FileSystemFailure, FileSystemOperation,
    LocalDirObjectStore, ObjectStore, ObjectStoreFailure, ObjectStoreOperation, OpenOptions, Rng,
    SimulatedClock, SimulatedFileSystem, SimulatedObjectStore, StandardObjectPath,
    StorageErrorKind, Timestamp, TokioFileSystem,
};

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_test_dir(name: &str) -> PathBuf {
    let id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("terracedb-{name}-{}-{id}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

async fn exercise_file_system(fs: &dyn FileSystem, root: &Path) {
    let dir = root.join("files");
    let path = dir.join("segment.log");
    let renamed = dir.join("segment-1.log");
    let handle = fs
        .open(
            path.to_string_lossy().as_ref(),
            OpenOptions {
                create: true,
                read: true,
                write: true,
                truncate: true,
                append: false,
            },
        )
        .await
        .expect("open file");

    fs.write_at(&handle, 0, b"hello world")
        .await
        .expect("write file");
    fs.sync(&handle).await.expect("sync file");

    let full = fs.read_at(&handle, 0, 11).await.expect("read file");
    assert_eq!(full, b"hello world");

    let range = fs.read_at(&handle, 6, 5).await.expect("read range");
    assert_eq!(range, b"world");

    fs.rename(
        path.to_string_lossy().as_ref(),
        renamed.to_string_lossy().as_ref(),
    )
    .await
    .expect("rename file");

    let listed = fs
        .list(dir.to_string_lossy().as_ref())
        .await
        .expect("list files");
    assert_eq!(listed, vec![renamed.to_string_lossy().into_owned()]);

    fs.delete(renamed.to_string_lossy().as_ref())
        .await
        .expect("delete file");
    let listed = fs
        .list(dir.to_string_lossy().as_ref())
        .await
        .expect("list after delete");
    assert!(listed.is_empty());
}

async fn exercise_object_store(store: &dyn ObjectStore) {
    store
        .put("tables/events/0001", b"first-object")
        .await
        .expect("put first object");
    store
        .put("tables/events/0002", b"second-object")
        .await
        .expect("put second object");

    let full = store.get("tables/events/0001").await.expect("get object");
    assert_eq!(full, b"first-object");

    let range = store
        .get_range("tables/events/0002", 7, 13)
        .await
        .expect("get object range");
    assert_eq!(range, b"object");

    store
        .copy("tables/events/0001", "archive/events/0001")
        .await
        .expect("copy object");

    let listed = store.list("tables/events/").await.expect("list objects");
    assert_eq!(
        listed,
        vec![
            "tables/events/0001".to_string(),
            "tables/events/0002".to_string()
        ]
    );

    let copied = store
        .get("archive/events/0001")
        .await
        .expect("get copied object");
    assert_eq!(copied, b"first-object");

    store
        .delete("tables/events/0002")
        .await
        .expect("delete object");
    let listed = store
        .list("tables/events/")
        .await
        .expect("list objects after delete");
    assert_eq!(listed, vec!["tables/events/0001".to_string()]);
}

async fn exercise_standard_object_store(store: &(dyn terracedb::StandardObjectStore + 'static)) {
    let first = StandardObjectPath::from("tables/events/0001");
    let second = StandardObjectPath::from("tables/events/0002");
    let archive = StandardObjectPath::from("archive/events/0001");
    let prefix = StandardObjectPath::from("tables/events");

    terracedb::StandardObjectStoreExt::put(store, &first, b"first-object".as_ref().into())
        .await
        .expect("put first object");
    terracedb::StandardObjectStoreExt::put(store, &second, b"second-object".as_ref().into())
        .await
        .expect("put second object");

    let full = terracedb::StandardObjectStoreExt::get(store, &first)
        .await
        .expect("get object")
        .bytes()
        .await
        .expect("read object bytes");
    assert_eq!(full.as_ref(), b"first-object");

    let range = terracedb::StandardObjectStoreExt::get_range(store, &second, 7..13)
        .await
        .expect("get object range");
    assert_eq!(range.as_ref(), b"object");

    terracedb::StandardObjectStoreExt::copy(store, &first, &archive)
        .await
        .expect("copy object");

    let mut listed = terracedb::StandardObjectStore::list(store, Some(&prefix))
        .map_ok(|meta| meta.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .expect("list objects");
    listed.sort();
    assert_eq!(
        listed,
        vec![
            "tables/events/0001".to_string(),
            "tables/events/0002".to_string()
        ]
    );

    let copied = terracedb::StandardObjectStoreExt::get(store, &archive)
        .await
        .expect("get copied object")
        .bytes()
        .await
        .expect("read copied bytes");
    assert_eq!(copied.as_ref(), b"first-object");

    terracedb::StandardObjectStoreExt::delete(store, &second)
        .await
        .expect("delete object");
    let mut listed = terracedb::StandardObjectStore::list(store, Some(&prefix))
        .map_ok(|meta| meta.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .expect("list objects after delete");
    listed.sort();
    assert_eq!(listed, vec!["tables/events/0001".to_string()]);
}

#[tokio::test]
async fn tokio_filesystem_supports_core_operations() {
    let root = unique_test_dir("tokio-fs");
    let fs = TokioFileSystem::new();

    exercise_file_system(&fs, &root).await;

    cleanup(&root);
}

#[tokio::test]
async fn simulated_filesystem_supports_core_operations() {
    let root = unique_test_dir("sim-fs");
    let fs = SimulatedFileSystem::default();

    exercise_file_system(&fs, &root).await;
}

#[tokio::test]
async fn local_object_store_supports_core_operations() {
    let root = unique_test_dir("local-obj");
    let store = LocalDirObjectStore::new(&root);

    exercise_object_store(&store).await;
    exercise_standard_object_store(&store).await;

    cleanup(&root);
}

#[tokio::test]
async fn simulated_object_store_supports_core_operations() {
    let store = SimulatedObjectStore::default();
    exercise_object_store(&store).await;
    exercise_standard_object_store(&store).await;
}

#[tokio::test]
async fn simulated_filesystem_injects_structured_errors() {
    let root = unique_test_dir("fs-failures");
    let path = root.join("faulty.log");
    let fs = SimulatedFileSystem::default();
    let handle = fs
        .open(
            path.to_string_lossy().as_ref(),
            OpenOptions {
                create: true,
                read: true,
                write: true,
                truncate: true,
                append: false,
            },
        )
        .await
        .expect("open file");

    fs.inject_failure(FileSystemFailure::disk_full(
        path.to_string_lossy().into_owned(),
    ));
    let error = fs
        .write_at(&handle, 0, b"abc")
        .await
        .expect_err("disk full error");
    assert_eq!(error.kind(), StorageErrorKind::Io);

    fs.inject_failure(FileSystemFailure::timeout(
        FileSystemOperation::Sync,
        path.to_string_lossy().into_owned(),
    ));
    let error = fs.sync(&handle).await.expect_err("timeout error");
    assert_eq!(error.kind(), StorageErrorKind::Timeout);

    fs.inject_failure(FileSystemFailure::partial_read(
        path.to_string_lossy().into_owned(),
    ));
    let error = fs
        .read_at(&handle, 0, 3)
        .await
        .expect_err("partial read error");
    assert_eq!(error.kind(), StorageErrorKind::Corruption);
}

#[tokio::test]
async fn simulated_object_store_injects_structured_errors() {
    let store = SimulatedObjectStore::default();
    store
        .put("segments/0001", b"segment-data")
        .await
        .expect("put object");

    store.inject_failure(ObjectStoreFailure::timeout(
        ObjectStoreOperation::Get,
        "segments/0001",
    ));
    let error = store.get("segments/0001").await.expect_err("timeout error");
    assert_eq!(error.kind(), StorageErrorKind::Timeout);

    store.inject_failure(ObjectStoreFailure::partial_read(
        ObjectStoreOperation::GetRange,
        "segments/0001",
    ));
    let error = store
        .get_range("segments/0001", 0, 4)
        .await
        .expect_err("partial read error");
    assert_eq!(error.kind(), StorageErrorKind::Corruption);

    store.inject_failure(ObjectStoreFailure::stale_list("segments/"));
    let error = store.list("segments/").await.expect_err("stale list error");
    assert_eq!(error.kind(), StorageErrorKind::DurabilityBoundary);
}

#[test]
fn deterministic_rng_replays_seeded_output() {
    let left = DeterministicRng::seeded(1234);
    let right = DeterministicRng::seeded(1234);

    assert_eq!(left.next_u64(), right.next_u64());
    assert_eq!(left.next_u64(), right.next_u64());
    assert_eq!(left.uuid(), right.uuid());
    assert_eq!(left.uuid(), right.uuid());
}

#[tokio::test]
async fn simulated_clock_advances_reproducibly() {
    let clock = Arc::new(SimulatedClock::default());
    let waiter = {
        let clock = clock.clone();
        tokio::spawn(async move {
            clock.sleep(Duration::from_millis(10)).await;
            clock.now()
        })
    };

    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    clock.advance(Duration::from_millis(9));
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    assert_eq!(clock.now(), Timestamp::new(9));

    clock.advance(Duration::from_millis(1));
    let woke_at = waiter.await.expect("waiter join");
    assert_eq!(woke_at, Timestamp::new(10));
    assert_eq!(clock.now(), Timestamp::new(10));
}
