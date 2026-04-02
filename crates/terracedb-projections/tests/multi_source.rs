use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use terracedb::{
    Db, KvStream, ScanOptions, SequenceNumber, StubFileSystem, StubObjectStore, Table, TableConfig,
    TieredDurabilityMode, Value,
    test_support::{
        bytes as test_bytes, row_table_config, test_dependencies, tiered_test_config,
        tiered_test_config_with_durability,
    },
};
use terracedb_projections::{
    MultiSourceProjection, MultiSourceProjectionHandler, ProjectionContext, ProjectionError,
    ProjectionHandler, ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun,
    ProjectionTransaction, RecomputeStrategy, SingleSourceProjection,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct ObservedBatch {
    source: String,
    sequence: SequenceNumber,
    seen_left: Option<Vec<u8>>,
    seen_right: Option<Vec<u8>>,
}

struct FrontierObserver {
    left: Table,
    right: Table,
    output: Table,
    observed: Arc<Mutex<Vec<ObservedBatch>>>,
}

#[async_trait]
impl MultiSourceProjectionHandler for FrontierObserver {
    async fn apply(
        &self,
        run: &ProjectionSequenceRun,
        ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        let seen_left =
            ctx.read(&self.left, b"shared".to_vec())
                .await?
                .and_then(|value| match value {
                    Value::Bytes(bytes) => Some(bytes),
                    Value::Record(_) => None,
                });
        let seen_right = ctx
            .read(&self.right, b"shared".to_vec())
            .await?
            .and_then(|value| match value {
                Value::Bytes(bytes) => Some(bytes),
                Value::Record(_) => None,
            });

        self.observed
            .lock()
            .expect("observed batches lock poisoned")
            .push(ObservedBatch {
                source: run.source().name().to_string(),
                sequence: run.sequence(),
                seen_left: seen_left.clone(),
                seen_right: seen_right.clone(),
            });

        tx.put(
            &self.output,
            format!("{}:{}", run.source().name(), run.sequence().get()).into_bytes(),
            Value::bytes(format!(
                "{}|{}",
                seen_left
                    .as_deref()
                    .map(String::from_utf8_lossy)
                    .as_deref()
                    .unwrap_or("-"),
                seen_right
                    .as_deref()
                    .map(String::from_utf8_lossy)
                    .as_deref()
                    .unwrap_or("-")
            )),
        );
        Ok(())
    }
}

struct RawCountsProjection {
    output: Table,
}

#[async_trait]
impl ProjectionHandler for RawCountsProjection {
    async fn apply_with_context(
        &self,
        run: &ProjectionSequenceRun,
        _ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        tx.put(
            &self.output,
            b"count".to_vec(),
            Value::bytes(run.len().to_string()),
        );
        Ok(())
    }
}

struct MirrorCountsProjection {
    output: Table,
}

#[async_trait]
impl ProjectionHandler for MirrorCountsProjection {
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

struct MirrorCurrentStateProjection {
    output: Table,
}

#[async_trait]
impl MultiSourceProjectionHandler for MirrorCurrentStateProjection {
    async fn apply(
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

async fn wait_for_targets<'a, I>(
    handle: &mut terracedb_projections::ProjectionHandle,
    targets: I,
) -> Result<(), ProjectionError>
where
    I: IntoIterator<Item = (&'a Table, SequenceNumber)>,
{
    tokio::time::timeout(Duration::from_secs(1), handle.wait_for_sources(targets))
        .await
        .expect("projection should settle before timeout")
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

fn retained_row_table_config(name: &str, retained: u64) -> TableConfig {
    let mut config = row_table_config(name);
    config.history_retention_sequences = Some(retained);
    config
}

#[tokio::test]
async fn multi_source_frontiers_use_declaration_order_for_equal_sequences() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-multi-source-frontiers"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let left = db
        .create_table(row_table_config("left"))
        .await
        .expect("create left source");
    let right = db
        .create_table(row_table_config("right"))
        .await
        .expect("create right source");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output");

    let observed = Arc::new(Mutex::new(Vec::new()));
    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "frontier-observer",
                [left.clone(), right.clone()],
                FrontierObserver {
                    left: left.clone(),
                    right: right.clone(),
                    output: output.clone(),
                    observed: observed.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start multi-source projection");

    let mut batch = db.write_batch();
    batch.put(&left, b"shared".to_vec(), test_bytes("L1"));
    batch.put(&right, b"shared".to_vec(), test_bytes("R1"));
    let sequence = db
        .commit(batch, terracedb::CommitOptions::default())
        .await
        .expect("commit both sources in one sequence");
    db.flush().await.expect("flush durable source rows");

    wait_for_targets(&mut handle, [(&left, sequence), (&right, sequence)])
        .await
        .expect("wait for both source frontiers");

    assert_eq!(handle.current_frontier().get("left"), Some(&sequence));
    assert_eq!(handle.current_frontier().get("right"), Some(&sequence));
    assert_eq!(
        observed
            .lock()
            .expect("observed batches lock poisoned")
            .as_slice(),
        &[
            ObservedBatch {
                source: "left".to_string(),
                sequence,
                seen_left: Some(b"L1".to_vec()),
                seen_right: None,
            },
            ObservedBatch {
                source: "right".to_string(),
                sequence,
                seen_left: Some(b"L1".to_vec()),
                seen_right: Some(b"R1".to_vec()),
            },
        ]
    );
    assert_eq!(
        output
            .read(format!("left:{}", sequence.get()).into_bytes())
            .await
            .expect("read left frontier snapshot"),
        Some(Value::bytes("L1|-"))
    );
    assert_eq!(
        output
            .read(format!("right:{}", sequence.get()).into_bytes())
            .await
            .expect("read right frontier snapshot"),
        Some(Value::bytes("L1|R1"))
    );

    handle.shutdown().await.expect("stop projection");
}

#[tokio::test]
async fn dependency_waits_propagate_through_single_source_chain() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-dependency-waits"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let events = db
        .create_table(row_table_config("events"))
        .await
        .expect("create events table");
    let counts = db
        .create_table(row_table_config("counts"))
        .await
        .expect("create counts table");
    let final_counts = db
        .create_table(row_table_config("final_counts"))
        .await
        .expect("create final counts table");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let raw_handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "raw-counts",
                events.clone(),
                RawCountsProjection {
                    output: counts.clone(),
                },
            )
            .with_outputs([counts.clone()]),
        )
        .await
        .expect("start raw-counts projection");
    let mut downstream = runtime
        .start_single_source(
            SingleSourceProjection::new(
                "copy-counts",
                counts.clone(),
                MirrorCountsProjection {
                    output: final_counts.clone(),
                },
            )
            .with_outputs([final_counts.clone()])
            .with_dependencies(["raw-counts"]),
        )
        .await
        .expect("start copy-counts projection");

    let sequence = events
        .write(b"event:1".to_vec(), test_bytes("signup"))
        .await
        .expect("write event");
    db.flush().await.expect("flush source event");

    wait_for_targets(&mut downstream, [(&events, sequence)])
        .await
        .expect("wait transitively on upstream source");

    assert_eq!(
        final_counts
            .read(b"count".to_vec())
            .await
            .expect("read downstream output"),
        Some(Value::bytes("1"))
    );

    raw_handle.shutdown().await.expect("stop raw-counts");
    downstream.shutdown().await.expect("stop copy-counts");
}

#[tokio::test]
async fn snapshot_too_old_rebuilds_from_current_state_when_enabled() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-rebuild-from-current-state"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let events = db
        .create_table(retained_row_table_config("events", 1))
        .await
        .expect("create retained events table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let _first = events
        .write(b"event:1".to_vec(), test_bytes("a"))
        .await
        .expect("write first event");
    let second = events
        .write(b"event:2".to_vec(), test_bytes("b"))
        .await
        .expect("write second event");
    db.flush().await.expect("flush retained source");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "rebuildable",
                [events.clone()],
                MirrorCurrentStateProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await
        .expect("start rebuildable projection");

    wait_for_targets(&mut handle, [(&events, second)])
        .await
        .expect("rebuild should catch up to current durable frontier");

    assert_eq!(
        collect_rows(
            output
                .scan(Vec::new(), vec![0xff], ScanOptions::default())
                .await
                .expect("scan rebuilt output"),
        )
        .await,
        vec![
            (b"event:1".to_vec(), b"a".to_vec()),
            (b"event:2".to_vec(), b"b".to_vec()),
        ]
    );

    handle
        .shutdown()
        .await
        .expect("stop rebuildable projection");
}

#[tokio::test]
async fn snapshot_too_old_rebuild_stays_pinned_to_durable_frontier() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/projection-rebuild-pinned-to-durable-frontier",
            TieredDurabilityMode::Deferred,
        ),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let events = db
        .create_table(retained_row_table_config("events", 1))
        .await
        .expect("create retained events table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let first = events
        .write(b"event:1".to_vec(), test_bytes("a"))
        .await
        .expect("write first durable event");
    db.flush().await.expect("flush first durable event");

    let second = events
        .write(b"event:2".to_vec(), test_bytes("b"))
        .await
        .expect("write second durable event");
    db.flush().await.expect("flush second durable event");

    let visible_only = events
        .write(b"event:3".to_vec(), test_bytes("c"))
        .await
        .expect("write visible-only event");
    assert!(
        visible_only > second,
        "expected visible-only write to advance past durable frontier"
    );

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "durable-pinned-rebuild",
                [events.clone()],
                MirrorCurrentStateProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await
        .expect("start rebuildable projection");

    wait_for_targets(&mut handle, [(&events, second)])
        .await
        .expect("rebuild should stop at the durable frontier");

    assert_eq!(handle.current_frontier().get("events"), Some(&second));
    assert_eq!(
        collect_rows(
            output
                .scan(Vec::new(), vec![0xff], ScanOptions::default())
                .await
                .expect("scan rebuilt output"),
        )
        .await,
        vec![
            (b"event:1".to_vec(), b"a".to_vec()),
            (b"event:2".to_vec(), b"b".to_vec()),
        ]
    );

    db.flush().await.expect("flush visible-only source write");
    wait_for_targets(&mut handle, [(&events, visible_only)])
        .await
        .expect("tailing should pick up the formerly visible-only write after durability");

    assert_eq!(
        collect_rows(
            output
                .scan(Vec::new(), vec![0xff], ScanOptions::default())
                .await
                .expect("scan output after durable catch-up"),
        )
        .await,
        vec![
            (b"event:1".to_vec(), b"a".to_vec()),
            (b"event:2".to_vec(), b"b".to_vec()),
            (b"event:3".to_vec(), b"c".to_vec()),
        ]
    );

    assert!(first < second, "durable writes should advance sequences");
    handle
        .shutdown()
        .await
        .expect("stop rebuildable projection");
}

#[tokio::test]
async fn snapshot_too_old_fails_closed_without_rebuild_strategy() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/projection-rebuild-fails-closed"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let events = db
        .create_table(retained_row_table_config("events", 1))
        .await
        .expect("create retained events table");
    let output = db
        .create_table(row_table_config("output"))
        .await
        .expect("create output table");

    let _first = events
        .write(b"event:1".to_vec(), test_bytes("a"))
        .await
        .expect("write first event");
    let second = events
        .write(b"event:2".to_vec(), test_bytes("b"))
        .await
        .expect("write second event");
    db.flush().await.expect("flush retained source");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "conservative",
                [events.clone()],
                MirrorCurrentStateProjection {
                    output: output.clone(),
                },
            )
            .with_outputs([output.clone()]),
        )
        .await
        .expect("start conservative projection");

    let error = wait_for_targets(&mut handle, [(&events, second)])
        .await
        .expect_err("projection should fail closed when rebuild is disabled");
    match error {
        ProjectionError::Runtime { reason, .. } => {
            assert!(
                reason.contains("requires recomputation"),
                "unexpected runtime error: {reason}"
            );
        }
        other => panic!("unexpected projection error: {other}"),
    }

    assert_eq!(
        output
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("scan conservative output")
            .count()
            .await,
        0
    );
}
