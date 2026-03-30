use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use terracedb::{
    Clock, CommitOptions, DeterministicRng, KvStream, Rng, ScanOptions, SequenceNumber, Table,
    Value,
    test_support::{row_table_config, tiered_test_config},
};
use terracedb_projections::{
    MultiSourceProjection, MultiSourceProjectionHandler, ProjectionContext, ProjectionHandle,
    ProjectionHandler, ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun,
    ProjectionTransaction, SingleSourceProjection,
};
use terracedb_simulation::{
    SeededSimulationRunner, SimulationCheckpoint, SimulationStackBuilder,
    TerracedbSimulationHarness, TraceEvent,
};

const PROJECTION_SIMULATION_DURATION: std::time::Duration = std::time::Duration::from_millis(250);
const SIMULATION_MIN_MESSAGE_LATENCY: std::time::Duration = std::time::Duration::from_millis(1);
const SIMULATION_MAX_MESSAGE_LATENCY: std::time::Duration = std::time::Duration::from_millis(1);

struct MirrorProjection {
    output: Table,
}

#[async_trait]
impl ProjectionHandler for MirrorProjection {
    async fn apply(
        &self,
        run: &ProjectionSequenceRun,
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

struct ProjectionStack {
    source: Table,
    output: Table,
    _runtime: ProjectionRuntime,
    handle: ProjectionHandle,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectionSnapshot {
    watermark: SequenceNumber,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

struct FrontierMirrorProjection {
    left: Table,
    right: Table,
    output: Table,
}

#[async_trait]
impl MultiSourceProjectionHandler for FrontierMirrorProjection {
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct MultiSourceProjectionSnapshot {
    frontier: BTreeMap<String, SequenceNumber>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug)]
struct MultiSourceStep {
    left: Option<Vec<u8>>,
    right: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectionSimulationCapture {
    snapshots: Vec<ProjectionSnapshot>,
    trace: Vec<TraceEvent>,
    checkpoints: Vec<SimulationCheckpoint>,
}

async fn collect_rows(stream: KvStream) -> Vec<(Vec<u8>, Vec<u8>)> {
    stream
        .map(|(key, value)| match value {
            Value::Bytes(bytes) => (key, bytes),
            Value::Record(_) => panic!("projection simulation only expects byte values"),
        })
        .collect()
        .await
}

fn planned_batches(seed: u64) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let rng = Arc::new(DeterministicRng::seeded(seed ^ 0x5eed_f00d));
    let mut batches = Vec::new();

    for step in 0..5 {
        let writes = (rng.next_u64() % 3 + 1) as usize;
        let mut batch = Vec::new();
        for _ in 0..writes {
            let key = format!("user:{}", rng.next_u64() % 4).into_bytes();
            let value = format!("s{step}-{}", rng.next_u64() % 100).into_bytes();
            batch.push((key, value));
        }
        batches.push(batch);
    }

    batches
}

fn planned_multi_source_batches(seed: u64) -> Vec<MultiSourceStep> {
    let rng = Arc::new(DeterministicRng::seeded(seed ^ 0xfeed_face));
    let mut batches = Vec::new();

    for step in 0..6 {
        let mode = if step % 3 == 0 {
            2
        } else {
            (rng.next_u64() % 3) as u8
        };
        let left = if mode != 1 {
            Some(format!("L{step}-{}", rng.next_u64() % 100).into_bytes())
        } else {
            None
        };
        let right = if mode != 0 {
            Some(format!("R{step}-{}", rng.next_u64() % 100).into_bytes())
        } else {
            None
        };
        batches.push(MultiSourceStep { left, right });
    }

    batches
}

fn projection_stack_builder() -> SimulationStackBuilder<ProjectionStack> {
    SimulationStackBuilder::new(
        |_context, db| async move {
            let source = db.create_table(row_table_config("source")).await?;
            let output = db.create_table(row_table_config("output")).await?;
            let runtime = ProjectionRuntime::open(db.clone()).await?;
            let handle = runtime
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
                .await?;

            Ok(ProjectionStack {
                source,
                output,
                _runtime: runtime,
                handle,
            })
        },
        |stack| async move {
            stack.handle.shutdown().await?;
            Ok(())
        },
    )
}

fn run_projection_simulation(seed: u64) -> turmoil::Result<ProjectionSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(PROJECTION_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(move |context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config(&format!("/projection-sim-{seed}")),
                projection_stack_builder(),
            )
            .await?;

            harness
                .checkpoint_with("projection-open", |_db, stack| {
                    Box::pin(async move {
                        Ok(BTreeMap::from([(
                            "projection.watermark".to_string(),
                            stack.handle.current_watermark().get().to_string(),
                        )]))
                    })
                })
                .await?;

            let mut snapshots = Vec::new();
            for (step, writes) in planned_batches(seed).into_iter().enumerate() {
                let source = harness.stack().source.clone();
                let output = harness.stack().output.clone();

                let mut batch = harness.db().write_batch();
                for (key, value) in writes {
                    batch.put(&source, key, Value::bytes(value));
                }

                let sequence = harness
                    .db()
                    .commit(batch, CommitOptions::default())
                    .await
                    .expect("commit simulated source batch");
                harness
                    .stack_mut()
                    .handle
                    .wait_for_watermark(sequence)
                    .await
                    .expect("projection should reach simulated watermark");

                let rows = collect_rows(
                    output
                        .scan(Vec::new(), vec![0xff], ScanOptions::default())
                        .await
                        .expect("scan projected output"),
                )
                .await;
                let watermark = harness.stack().handle.current_watermark();
                snapshots.push(ProjectionSnapshot {
                    watermark,
                    rows: rows.clone(),
                });

                harness
                    .checkpoint_with(format!("projection-batch-{step}"), move |_db, stack| {
                        Box::pin(async move {
                            Ok(BTreeMap::from([
                                (
                                    "projection.watermark".to_string(),
                                    stack.handle.current_watermark().get().to_string(),
                                ),
                                ("projection.output_rows".to_string(), rows.len().to_string()),
                            ]))
                        })
                    })
                    .await?;
            }

            let capture = ProjectionSimulationCapture {
                snapshots,
                trace: harness.trace(),
                checkpoints: harness.checkpoints().to_vec(),
            };
            harness.shutdown().await?;
            Ok(capture)
        })
}

#[test]
fn projection_simulation_catches_up_each_batch_within_bounded_simulated_time() -> turmoil::Result {
    let seed = 0x5151_u64;
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(PROJECTION_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(move |context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config("/projection-sim-bounded-catchup"),
                projection_stack_builder(),
            )
            .await?;
            let clock = harness.context().clock();
            let source = harness.stack().source.clone();

            for (step, writes) in planned_batches(seed).into_iter().enumerate() {
                let mut batch = harness.db().write_batch();
                for (key, value) in writes {
                    batch.put(&source, key, Value::bytes(value));
                }

                let sequence = harness
                    .db()
                    .commit(batch, CommitOptions::default())
                    .await
                    .expect("commit simulated source batch");
                let start = clock.now().get();
                harness
                    .stack_mut()
                    .handle
                    .wait_for_watermark(sequence)
                    .await
                    .expect("projection should reach simulated watermark");
                let elapsed = clock.now().get().saturating_sub(start);
                assert!(
                    elapsed <= 25,
                    "projection step {step} should catch up within a bounded simulated time budget: elapsed={elapsed}ms"
                );
            }

            harness.shutdown().await?;
            Ok(())
        })
}

fn run_multi_source_projection_simulation(
    seed: u64,
) -> turmoil::Result<Vec<MultiSourceProjectionSnapshot>> {
    SeededSimulationRunner::new(seed).run_with(move |context| async move {
        let db = context
            .open_db(tiered_test_config(&format!("/projection-multi-sim-{seed}")))
            .await?;
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
            .expect("create output table");
        let runtime = ProjectionRuntime::open(db.clone())
            .await
            .expect("open projection runtime");
        let mut handle = runtime
            .start_multi_source(
                MultiSourceProjection::new(
                    "frontier-mirror",
                    [left.clone(), right.clone()],
                    FrontierMirrorProjection {
                        left: left.clone(),
                        right: right.clone(),
                        output: output.clone(),
                    },
                )
                .with_outputs([output.clone()]),
            )
            .await
            .expect("start multi-source projection");

        let mut snapshots = Vec::new();
        for step in planned_multi_source_batches(seed) {
            let mut batch = db.write_batch();
            let mut targets = Vec::new();

            if let Some(value) = step.left {
                batch.put(&left, b"shared".to_vec(), Value::bytes(value));
                targets.push(&left);
            }
            if let Some(value) = step.right {
                batch.put(&right, b"shared".to_vec(), Value::bytes(value));
                targets.push(&right);
            }

            let sequence = db
                .commit(batch, CommitOptions::default())
                .await
                .expect("commit simulated multi-source batch");
            let targets = targets
                .into_iter()
                .map(|table| (table, sequence))
                .collect::<Vec<_>>();
            handle
                .wait_for_sources(targets)
                .await
                .expect("multi-source projection should reach requested frontier");

            snapshots.push(MultiSourceProjectionSnapshot {
                frontier: handle.current_frontier(),
                rows: collect_rows(
                    output
                        .scan(Vec::new(), vec![0xff], ScanOptions::default())
                        .await
                        .expect("scan projected multi-source output"),
                )
                .await,
            });
        }

        handle.shutdown().await.expect("stop projection");
        Ok(snapshots)
    })
}

#[test]
fn projection_simulation_replays_same_seed() -> turmoil::Result {
    let first = run_projection_simulation(0x1234_5678)?;
    let second = run_projection_simulation(0x1234_5678)?;

    assert_eq!(first, second);
    Ok(())
}

#[test]
fn projection_simulation_changes_shape_for_different_seeds() -> turmoil::Result {
    let left = run_projection_simulation(7)?;
    let right = run_projection_simulation(8)?;

    assert_ne!(left, right);
    Ok(())
}

#[test]
fn multi_source_projection_simulation_replays_same_seed() -> turmoil::Result {
    let first = run_multi_source_projection_simulation(0x0bad_5eed)?;
    let second = run_multi_source_projection_simulation(0x0bad_5eed)?;

    assert_eq!(first, second);
    Ok(())
}

#[test]
fn multi_source_projection_simulation_changes_shape_for_different_seeds() -> turmoil::Result {
    let left = run_multi_source_projection_simulation(17)?;
    let right = run_multi_source_projection_simulation(23)?;

    assert_ne!(left, right);
    Ok(())
}
