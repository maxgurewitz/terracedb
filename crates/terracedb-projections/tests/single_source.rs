use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use terracedb::{
    ChangeFeedError, CommitOptions, Db, FileSystemFailure, FileSystemOperation, KvStream,
    LogCursor, ScanOptions, SequenceNumber, StorageError, StorageErrorKind, StubFileSystem,
    StubObjectStore, Table, TieredDurabilityMode, Value,
    test_support::{
        FailpointMode, bytes as test_bytes, db_failpoint_registry, row_table_config,
        test_dependencies, tiered_test_config,
    },
};
use terracedb_projections::{
    PROJECTION_CURSOR_TABLE_NAME, ProjectionContext, ProjectionError, ProjectionHandler,
    ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun, ProjectionTransaction,
    SingleSourceProjection, failpoints::names as projection_failpoint_names,
};

struct SequenceRecorder {
    output: Table,
    observed_runs: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

#[async_trait]
impl ProjectionHandler for SequenceRecorder {
    async fn apply_with_context(
        &self,
        run: &ProjectionSequenceRun,
        _ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        let keys = run
            .entries()
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>();
        self.observed_runs
            .lock()
            .expect("observed-runs lock poisoned")
            .push(keys);
        tx.put(
            &self.output,
            format!("seq:{}", run.sequence().get()).into_bytes(),
            Value::bytes(run.len().to_string()),
        );
        Ok(())
    }
}

struct MirrorProjection {
    output: Table,
}

#[async_trait]
impl ProjectionHandler for MirrorProjection {
    async fn apply_with_context(
        &self,
        run: &ProjectionSequenceRun,
        _ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        for entry in run.entries() {
            match &entry.value {
                Some(value) => tx.put(&self.output, entry.key.clone(), value.clone()),
                None => tx.delete(&self.output, entry.key.clone()),
            }
        }
        Ok(())
    }
}

struct FrontierSnapshotProjection {
    source: Table,
    output: Table,
}

#[async_trait]
impl ProjectionHandler for FrontierSnapshotProjection {
    async fn apply_with_context(
        &self,
        run: &ProjectionSequenceRun,
        ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        let shared = ctx
            .read(&self.source, b"shared".to_vec())
            .await?
            .map(|value| match value {
                Value::Bytes(bytes) => Ok(String::from_utf8_lossy(&bytes).into_owned()),
                Value::Record(_) => Err(std::io::Error::other(
                    "frontier snapshot projection only expects byte values",
                )),
            })
            .transpose()
            .map_err(ProjectionHandlerError::new)?
            .unwrap_or_else(|| "-".to_string());
        let scanned_keys = collect_rows(
            ctx.scan(&self.source, Vec::new(), vec![0xff], ScanOptions::default())
                .await?,
        )
        .await
        .into_iter()
        .map(|(key, _value)| String::from_utf8_lossy(&key).into_owned())
        .collect::<Vec<_>>()
        .join(",");

        tx.put(
            &self.output,
            format!("snapshot:{}", run.sequence().get()).into_bytes(),
            Value::bytes(format!("{shared}|{scanned_keys}")),
        );
        Ok(())
    }
}

async fn wait_for_watermark(
    handle: &mut terracedb_projections::ProjectionHandle,
    sequence: SequenceNumber,
) {
    tokio::time::timeout(Duration::from_secs(1), handle.wait_for_watermark(sequence))
        .await
        .expect("projection should reach requested watermark")
        .expect("projection should not fail while waiting");
}

async fn collect_rows(stream: KvStream) -> Vec<(Vec<u8>, Vec<u8>)> {
    stream
        .map(|(key, value)| match value {
            Value::Bytes(bytes) => (key, bytes),
            Value::Record(_) => panic!("projection tests only expect byte values"),
        })
        .collect()
        .await
}

#[tokio::test]
async fn projection_processes_whole_sequence_batches_before_advancing_watermark() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-whole-sequence"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("source"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let observed_runs = Arc::new(Mutex::new(Vec::new()));
    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "seq-recorder",
                source.clone(),
                SequenceRecorder {
                    output: output.clone(),
                    observed_runs: observed_runs.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start projection");

    let mut batch = db.write_batch();
    batch.put(&source, b"alpha".to_vec(), test_bytes("v1"));
    batch.put(&source, b"beta".to_vec(), test_bytes("v2"));
    let first_sequence = db
        .commit(batch, CommitOptions::default())
        .await
        .expect("commit multi-entry source batch");
    db.flush().await.expect("flush durable source batch");

    let first_run = runtime
        .scan_whole_sequence_run(&source, LogCursor::beginning())
        .await
        .expect("scan whole sequence run")
        .expect("first sequence should be visible");
    assert_eq!(first_run.sequence(), first_sequence);
    assert_eq!(first_run.len(), 2);
    assert_eq!(first_run.first_cursor(), LogCursor::new(first_sequence, 0));
    assert_eq!(first_run.last_cursor(), LogCursor::new(first_sequence, 1));

    wait_for_watermark(&mut handle, first_sequence).await;

    assert_eq!(handle.current_watermark(), first_sequence);
    assert_eq!(
        output
            .read(format!("seq:{}", first_sequence.get()).into_bytes())
            .await
            .expect("read sequence summary"),
        Some(Value::bytes("2"))
    );
    assert_eq!(
        observed_runs
            .lock()
            .expect("observed-runs lock poisoned")
            .as_slice(),
        &[vec![b"alpha".to_vec(), b"beta".to_vec()]]
    );

    handle.shutdown().await.expect("stop projection");
}

#[tokio::test]
async fn projection_catches_up_startup_backlog_without_new_notification() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-startup-backlog"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("source"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let backlog_sequence = source
        .write(b"user:1".to_vec(), test_bytes("queued"))
        .await
        .expect("write startup backlog");
    db.flush().await.expect("make backlog durable");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "mirror",
                source.clone(),
                MirrorProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start projection");

    wait_for_watermark(&mut handle, backlog_sequence).await;

    assert_eq!(
        output
            .read(b"user:1".to_vec())
            .await
            .expect("read mirrored row"),
        Some(test_bytes("queued"))
    );

    handle.shutdown().await.expect("stop projection");
}

#[tokio::test]
async fn projection_failpoint_before_commit_preserves_cursor_until_retry() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-failpoint-before-commit"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("source"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "mirror-with-failpoint",
                source.clone(),
                MirrorProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start projection");

    db_failpoint_registry(&db).arm_error(
        projection_failpoint_names::PROJECTION_APPLY_BEFORE_COMMIT,
        StorageError::io("simulated projection failpoint"),
        FailpointMode::Once,
    );

    let sequence = source
        .write(b"alpha".to_vec(), Value::bytes("v1"))
        .await
        .expect("write source value");
    db.flush().await.expect("flush source value");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("projection failure should be observed")
        .expect("projection should terminate after the injected failpoint");

    let error = handle
        .shutdown()
        .await
        .expect_err("projection should fail at the named cut point");
    match error {
        ProjectionError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Io);
            assert!(
                storage
                    .to_string()
                    .contains("simulated projection failpoint"),
                "expected injected projection failpoint context, got {storage}"
            );
        }
        other => panic!("expected projection storage error, got {other:?}"),
    }

    assert_eq!(
        output
            .read(b"alpha".to_vec())
            .await
            .expect("read output after failed projection"),
        None
    );

    let mut retry_handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "mirror-with-failpoint",
                source.clone(),
                MirrorProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("restart projection after one-shot failpoint");
    wait_for_watermark(&mut retry_handle, sequence).await;

    assert_eq!(
        output
            .read(b"alpha".to_vec())
            .await
            .expect("read retried projection output"),
        Some(Value::bytes("v1"))
    );

    retry_handle
        .shutdown()
        .await
        .expect("stop restarted projection");
}

#[tokio::test]
async fn single_source_projection_can_scan_and_read_through_projection_context() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-single-source-context"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("source"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "frontier-snapshot",
                source.clone(),
                FrontierSnapshotProjection {
                    source: source.clone(),
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start projection");

    let mut batch = db.write_batch();
    batch.put(&source, b"alpha".to_vec(), test_bytes("a"));
    batch.put(&source, b"shared".to_vec(), test_bytes("current"));
    batch.put(&source, b"zeta".to_vec(), test_bytes("z"));
    let sequence = db
        .commit(batch, CommitOptions::default())
        .await
        .expect("commit source rows");
    db.flush().await.expect("flush source rows");

    wait_for_watermark(&mut handle, sequence).await;

    assert_eq!(
        output
            .read(format!("snapshot:{}", sequence.get()).into_bytes())
            .await
            .expect("read frontier snapshot"),
        Some(Value::bytes("current|alpha,shared,zeta"))
    );

    handle.shutdown().await.expect("stop projection");
}

#[tokio::test]
async fn projection_cursor_and_output_recover_together_after_crash() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let dependencies = test_dependencies(file_system.clone(), object_store.clone());
    let config = terracedb::test_support::tiered_test_config_with_durability(
        "/projection-crash-recovery",
        TieredDurabilityMode::Deferred,
    );
    let db = Db::open(config.clone(), dependencies.clone())
        .await
        .expect("open db");
    let source = db
        .create_table(row_table_config("source"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "mirror",
                source.clone(),
                MirrorProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start projection");

    let sequence = source
        .write(b"user:1".to_vec(), test_bytes("before-crash"))
        .await
        .expect("write source row");
    db.flush().await.expect("flush durable source row");
    wait_for_watermark(&mut handle, sequence).await;

    assert_eq!(
        runtime
            .load_projection_cursor("mirror")
            .await
            .expect("load visible projection cursor"),
        LogCursor::new(sequence, 0)
    );
    assert_eq!(
        output
            .read(b"user:1".to_vec())
            .await
            .expect("read visible mirrored row"),
        Some(test_bytes("before-crash"))
    );

    file_system.crash();
    drop(handle);

    let reopened = Db::open(config, dependencies)
        .await
        .expect("reopen db after crash");
    let reopened_source = reopened.table("source");
    let reopened_output = reopened.table("output");
    let reopened_runtime = ProjectionRuntime::open(reopened.clone())
        .await
        .expect("open projection runtime after crash");

    assert_eq!(
        reopened_runtime
            .load_projection_cursor("mirror")
            .await
            .expect("cursor should rewind after crash"),
        LogCursor::beginning()
    );
    assert_eq!(
        reopened
            .table(PROJECTION_CURSOR_TABLE_NAME)
            .read(b"mirror".to_vec())
            .await
            .expect("read cursor table directly"),
        None
    );
    assert_eq!(
        reopened_output
            .read(b"user:1".to_vec())
            .await
            .expect("read reopened mirrored row"),
        None
    );

    let mut replay_handle = reopened_runtime
        .start_single_source(
            SingleSourceProjection::new(
                "mirror",
                reopened_source.clone(),
                MirrorProjection {
                    output: reopened_output.clone(),
                },
            )
            .with_outputs([reopened_output.clone()]),
        )
        .await
        .expect("restart projection after crash");
    wait_for_watermark(&mut replay_handle, sequence).await;

    assert_eq!(
        reopened_runtime
            .load_projection_cursor("mirror")
            .await
            .expect("cursor should replay from durable source"),
        LogCursor::new(sequence, 0)
    );
    assert_eq!(
        reopened_output
            .read(b"user:1".to_vec())
            .await
            .expect("read replayed mirrored row"),
        Some(test_bytes("before-crash"))
    );
    assert_eq!(
        collect_rows(
            reopened_output
                .scan(Vec::new(), vec![0xff], ScanOptions::default())
                .await
                .expect("scan replayed output")
        )
        .await,
        vec![(b"user:1".to_vec(), b"before-crash".to_vec())]
    );

    replay_handle
        .shutdown()
        .await
        .expect("stop replay projection");
}

#[tokio::test]
async fn projection_runtime_surfaces_change_feed_scan_failures_without_panicking() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-change-feed-failure"),
        test_dependencies(file_system.clone(), object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("source"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let sequence = source
        .write(b"user:1".to_vec(), test_bytes("v1"))
        .await
        .expect("write source row");

    file_system.inject_failure(
        FileSystemFailure::timeout(
            FileSystemOperation::ReadAt,
            "/projection-change-feed-failure/commitlog/SEG-000001",
        )
        .persistent(),
    );

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "mirror",
                source.clone(),
                MirrorProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start projection");

    let wait_error =
        tokio::time::timeout(Duration::from_secs(1), handle.wait_for_watermark(sequence))
            .await
            .expect("projection failure should be observed")
            .expect_err("projection should fail on change-feed scan error");
    match wait_error {
        ProjectionError::Runtime { reason, .. } => {
            assert!(
                reason.contains("simulated timeout"),
                "unexpected runtime reason: {reason}"
            );
        }
        other => panic!("unexpected wait error: {other}"),
    }

    match tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("shutdown should return promptly")
        .expect_err("projection task should return the underlying change-feed error")
    {
        ProjectionError::ChangeFeed(ChangeFeedError::Storage(error)) => {
            assert_eq!(error.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("unexpected shutdown error: {other}"),
    }

    assert_eq!(
        output
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("scan output after failed projection")
            .count()
            .await,
        0
    );
}

#[tokio::test]
async fn projection_runtime_fails_closed_on_legacy_single_source_cursor_rows() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-legacy-cursor"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("source"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");
    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");

    let mut legacy_cursor = vec![1];
    legacy_cursor.extend_from_slice(&LogCursor::new(SequenceNumber::new(7), 0).encode());
    db.table(PROJECTION_CURSOR_TABLE_NAME)
        .write(b"mirror".to_vec(), Value::bytes(legacy_cursor))
        .await
        .expect("write legacy cursor row");

    match runtime
        .start_single_source(
            SingleSourceProjection::new(
                "mirror",
                source,
                MirrorProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output]),
        )
        .await
    {
        Err(ProjectionError::CursorCorruption { reason, .. }) => {
            assert!(reason.contains("legacy single-source cursor row"));
        }
        Err(other) => panic!("expected cursor corruption, got {other:?}"),
        Ok(_) => panic!("expected legacy cursor row to fail closed"),
    }
}

#[tokio::test]
async fn projection_runtime_surfaces_typed_change_feed_storage_errors() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-change-feed-timeout"),
        test_dependencies(file_system.clone(), object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("source"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    source
        .write(b"user:1".to_vec(), test_bytes("queued"))
        .await
        .expect("write source backlog");
    file_system.inject_failure(
        FileSystemFailure::timeout(
            FileSystemOperation::ReadAt,
            "/projection-change-feed-timeout/commitlog/SEG-000001",
        )
        .persistent(),
    );

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "mirror",
                source.clone(),
                MirrorProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start projection");

    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("projection failure should be observed")
        .expect("projection should terminate after the injected scan failure");

    let error = handle
        .shutdown()
        .await
        .expect_err("projection should fail on the injected change-feed error");
    match error {
        ProjectionError::Storage(storage)
        | ProjectionError::ChangeFeed(ChangeFeedError::Storage(storage)) => {
            assert_eq!(storage.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("expected typed projection change-feed failure, got {other:?}"),
    }
}

#[tokio::test]
async fn projection_scan_helper_keeps_snapshot_too_old_distinct() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-snapshot-too-old"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let mut source_config = row_table_config("source");
    source_config.history_retention_sequences = Some(1);
    let source = db
        .create_table(source_config)
        .await
        .expect("create source table");

    let first = source
        .write(b"user:1".to_vec(), test_bytes("v1"))
        .await
        .expect("write first source event");
    let second = source
        .write(b"user:2".to_vec(), test_bytes("v2"))
        .await
        .expect("write second source event");

    let runtime = ProjectionRuntime::open(db)
        .await
        .expect("open projection runtime");
    let error = runtime
        .scan_whole_sequence_run(&source, LogCursor::new(first, 0))
        .await
        .expect_err("stale projection cursor should surface SnapshotTooOld");

    match error {
        ProjectionError::ChangeFeed(error) => {
            let snapshot_too_old = error
                .snapshot_too_old()
                .expect("projection helper should preserve SnapshotTooOld");
            assert_eq!(snapshot_too_old.requested, first);
            assert_eq!(snapshot_too_old.oldest_available, second);
        }
        other => panic!("expected change-feed SnapshotTooOld, got {other:?}"),
    }
}
