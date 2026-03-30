use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use terracedb::{
    CommitOptions, DeterministicRng, KvStream, Rng, ScanOptions, SeededSimulationRunner,
    SequenceNumber, Table, Value,
    test_support::{row_table_config, tiered_test_config},
};
use terracedb_projections::{
    MultiSourceProjection, MultiSourceProjectionHandler, ProjectionContext, ProjectionHandler,
    ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun, ProjectionTransaction,
    SingleSourceProjection,
};

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

fn run_projection_simulation(seed: u64) -> turmoil::Result<Vec<ProjectionSnapshot>> {
    SeededSimulationRunner::new(seed).run_with(move |context| async move {
        let db = context
            .open_db(tiered_test_config(&format!("/projection-sim-{seed}")))
            .await?;
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

        let mut snapshots = Vec::new();
        for writes in planned_batches(seed) {
            let mut batch = db.write_batch();
            for (key, value) in writes {
                batch.put(&source, key, Value::bytes(value));
            }

            let sequence = db
                .commit(batch, CommitOptions::default())
                .await
                .expect("commit simulated source batch");
            handle
                .wait_for_watermark(sequence)
                .await
                .expect("projection should reach simulated watermark");

            snapshots.push(ProjectionSnapshot {
                watermark: handle.current_watermark(),
                rows: collect_rows(
                    output
                        .scan(Vec::new(), vec![0xff], ScanOptions::default())
                        .await
                        .expect("scan projected output"),
                )
                .await,
            });
        }

        handle.shutdown().await.expect("stop projection");
        Ok(snapshots)
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
