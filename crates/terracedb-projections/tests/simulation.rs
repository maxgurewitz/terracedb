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
const RECENT_TODO_LIMIT: usize = 10;

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
struct RecentTodoSnapshot {
    todo_id: Vec<u8>,
    updated_at_ms: u64,
}

struct RecentTodosProjection {
    todos: Table,
    recent: Table,
}

#[async_trait]
impl ProjectionHandler for RecentTodosProjection {
    async fn apply_with_context(
        &self,
        _run: &ProjectionSequenceRun,
        ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        let mut recent = collect_recent_todo_snapshots(
            ctx.scan(&self.todos, Vec::new(), vec![0xff], ScanOptions::default())
                .await?,
        )
        .await;
        recent.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| left.todo_id.cmp(&right.todo_id))
        });

        for slot in 0..RECENT_TODO_LIMIT {
            tx.delete(&self.recent, format!("recent:{slot:02}").into_bytes());
        }
        for (slot, todo) in recent.into_iter().take(RECENT_TODO_LIMIT).enumerate() {
            tx.put(
                &self.recent,
                format!("recent:{slot:02}").into_bytes(),
                Value::bytes(todo.todo_id),
            );
        }
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

async fn collect_recent_todo_snapshots(stream: KvStream) -> Vec<RecentTodoSnapshot> {
    stream
        .map(|(todo_id, value)| RecentTodoSnapshot {
            todo_id,
            updated_at_ms: decode_todo_updated_at(&value),
        })
        .collect()
        .await
}

fn todo_value(updated_at_ms: u64, title: &str) -> Value {
    Value::bytes(format!("{updated_at_ms:020}|{title}"))
}

fn decode_todo_updated_at(value: &Value) -> u64 {
    let Value::Bytes(bytes) = value else {
        panic!("todo simulation only expects byte values");
    };
    let updated_at = std::str::from_utf8(bytes)
        .expect("todo bytes should be utf-8")
        .split_once('|')
        .expect("todo value should include an updated_at prefix")
        .0;
    updated_at
        .parse()
        .expect("todo updated_at prefix should be numeric")
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

#[test]
fn recent_todos_projection_simulation_tracks_last_ten_created_or_modified_rows() -> turmoil::Result
{
    SeededSimulationRunner::new(0x0dd5_eed5)
        .with_simulation_duration(PROJECTION_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let db = context
                .open_db(tiered_test_config("/projection-recent-todos"))
                .await?;
            let todos = db
                .create_table(row_table_config("todos"))
                .await
                .expect("create todos source");
            let recent = db
                .create_table(row_table_config("recent_todos"))
                .await
                .expect("create recent projection output");
            let runtime = ProjectionRuntime::open(db.clone())
                .await
                .expect("open projection runtime");
            let mut handle = runtime
                .start_single_source(
                    SingleSourceProjection::new(
                        "recent-todos",
                        todos.clone(),
                        RecentTodosProjection {
                            todos: todos.clone(),
                            recent: recent.clone(),
                        },
                    )
                    .with_outputs([recent.clone()]),
                )
                .await
                .expect("start recent todos projection");

            let mut create_batch = db.write_batch();
            for todo_id in 1..=12_u64 {
                create_batch.put(
                    &todos,
                    format!("todo:{todo_id:02}").into_bytes(),
                    todo_value(todo_id, &format!("todo {todo_id}")),
                );
            }
            let created_sequence = db
                .commit(create_batch, CommitOptions::default())
                .await
                .expect("commit initial todos");
            handle
                .wait_for_sources([(&todos, created_sequence)])
                .await
                .expect("recent projection should catch up to created todos");

            let mut update_batch = db.write_batch();
            update_batch.put(
                &todos,
                b"todo:02".to_vec(),
                todo_value(20, "todo 2 updated"),
            );
            update_batch.put(
                &todos,
                b"todo:05".to_vec(),
                todo_value(21, "todo 5 updated"),
            );
            update_batch.put(
                &todos,
                b"todo:11".to_vec(),
                todo_value(22, "todo 11 updated"),
            );
            update_batch.put(
                &todos,
                b"todo:01".to_vec(),
                todo_value(23, "todo 1 updated"),
            );
            let updated_sequence = db
                .commit(update_batch, CommitOptions::default())
                .await
                .expect("commit todo updates");
            handle
                .wait_for_sources([(&todos, updated_sequence)])
                .await
                .expect("recent projection should catch up to updated todos");

            let recent_rows = collect_rows(
                recent
                    .scan(Vec::new(), vec![0xff], ScanOptions::default())
                    .await
                    .expect("scan recent todo projection"),
            )
            .await;
            assert_eq!(
                recent_rows
                    .into_iter()
                    .map(|(_slot, todo_id)| String::from_utf8_lossy(&todo_id).into_owned())
                    .collect::<Vec<_>>(),
                vec![
                    "todo:01".to_string(),
                    "todo:11".to_string(),
                    "todo:05".to_string(),
                    "todo:02".to_string(),
                    "todo:12".to_string(),
                    "todo:10".to_string(),
                    "todo:09".to_string(),
                    "todo:08".to_string(),
                    "todo:07".to_string(),
                    "todo:06".to_string(),
                ]
            );
            assert_eq!(
                handle.current_frontier().get("todos"),
                Some(&updated_sequence)
            );

            handle.shutdown().await.expect("stop recent projection");
            Ok(())
        })
}
