use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use terracedb::{
    CommitOptions, DeterministicRng, KvStream, Rng, ScanOptions, SeededSimulationRunner,
    SequenceNumber, Table, Value,
    test_support::{row_table_config, tiered_test_config},
};
use terracedb_projections::{
    ProjectionHandler, ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun,
    ProjectionTransaction, SingleSourceProjection,
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
