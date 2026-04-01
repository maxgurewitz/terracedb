use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::StreamExt;
use terracedb::{
    Clock, CommitOptions, CompactionStrategy, Db, DbConfig, DeterministicRng, OutboxEntry, Rng,
    S3Location, ScanOptions, SsdConfig, StorageConfig, Table, TableConfig, TableFormat,
    TieredDurabilityMode, TieredStorageConfig, Transaction, Value,
};
use terracedb_fuzz::{GeneratedScenarioHarness, assert_seed_replays, assert_seed_variation};
use terracedb_relays::{OutboxRelay, OutboxRelayHandler, RelayEntry};
use terracedb_simulation::SeededSimulationRunner;
use thiserror::Error;

const MIN_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const MAX_MESSAGE_LATENCY: Duration = Duration::from_millis(1);

fn simulation_config(root_path: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: root_path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-relays-sim".to_string(),
                prefix: "relay".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability: TieredDurabilityMode::Deferred,
            local_retention: terracedb::TieredLocalRetentionMode::Offload,
        }),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

fn row_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Row,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(10),
        history_retention_sequences: None,
        compaction_strategy: CompactionStrategy::Leveled,
        schema: None,
        sharding: Default::default(),
        metadata: Default::default(),
    }
}

#[derive(Debug, Error)]
enum MirrorRelayError {
    #[error(transparent)]
    Read(#[from] terracedb::ReadError),
    #[error("relay payload must be utf-8")]
    Utf8,
}

struct MirrorRelayHandler {
    todos: Table,
}

#[async_trait]
impl OutboxRelayHandler for MirrorRelayHandler {
    type Message = Vec<u8>;
    type Error = MirrorRelayError;

    fn decode(&self, entry: OutboxEntry) -> Result<RelayEntry<Self::Message>, Self::Error> {
        Ok(RelayEntry {
            outbox_id: entry.outbox_id,
            idempotency_key: entry.idempotency_key,
            message: entry.payload,
        })
    }

    async fn apply_batch(
        &self,
        tx: &mut Transaction,
        entries: &[RelayEntry<Self::Message>],
    ) -> Result<(), Self::Error> {
        for entry in entries {
            let todo_id =
                String::from_utf8(entry.message.clone()).map_err(|_| MirrorRelayError::Utf8)?;
            if tx
                .read(&self.todos, todo_id.as_bytes().to_vec())
                .await?
                .is_none()
            {
                tx.write(
                    &self.todos,
                    todo_id.clone().into_bytes(),
                    Value::bytes(todo_id),
                );
            }
        }
        Ok(())
    }
}

async fn count_rows(table: &Table) -> Result<usize, terracedb::ReadError> {
    let mut rows = table
        .scan(Vec::new(), vec![0xff], ScanOptions::default())
        .await?;
    let mut count = 0;
    while rows.next().await.is_some() {
        count += 1;
    }
    Ok(count)
}

async fn observe_row_counts_until(
    db: &Db,
    table: &Table,
    expected: usize,
) -> Result<Vec<usize>, terracedb::ReadError> {
    let mut visible = db.subscribe(table);
    let mut last_seen = visible.current();
    let mut observed = vec![count_rows(table).await?];
    while observed.last().copied().unwrap_or_default() != expected {
        let target = last_seen;
        last_seen = visible
            .wait_for(|sequence| sequence > target)
            .await
            .expect("relay output subscription should remain open");
        observed.push(count_rows(table).await?);
    }
    Ok(observed)
}

async fn collect_rows(table: &Table) -> Result<Vec<String>, terracedb::ReadError> {
    let mut rows = table
        .scan(Vec::new(), vec![0xff], ScanOptions::default())
        .await?;
    let mut values = Vec::new();
    while let Some((_key, value)) = rows.next().await {
        let Value::Bytes(bytes) = value else {
            panic!("relay simulation only expects byte rows");
        };
        values.push(String::from_utf8(bytes).expect("mirror rows should be utf-8"));
    }
    Ok(values)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RelayGeneratedScenario {
    seed: u64,
    batches: Vec<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RelayGeneratedOutcome {
    rows: Vec<String>,
    observed_counts: Vec<Vec<usize>>,
}

struct RelayGeneratedHarness;

impl GeneratedScenarioHarness for RelayGeneratedHarness {
    type Scenario = RelayGeneratedScenario;
    type Outcome = RelayGeneratedOutcome;
    type Error = Box<dyn std::error::Error>;

    fn generate(&self, seed: u64) -> Self::Scenario {
        let rng = DeterministicRng::seeded(seed ^ 0x7713);
        let batch_count = ((rng.next_u64() as usize) % 3) + 2;
        let mut batches = Vec::with_capacity(batch_count);
        for batch in 0..batch_count {
            let entry_count = ((rng.next_u64() as usize) % 4) + 1;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let todo_idx = (rng.next_u64() as usize) % 6;
                entries.push(format!("todo-{batch}-{todo_idx}"));
            }
            batches.push(entries);
        }
        RelayGeneratedScenario { seed, batches }
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        Ok(run_generated_relay_scenario(scenario)?)
    }
}

fn run_generated_relay_scenario(
    scenario: RelayGeneratedScenario,
) -> turmoil::Result<RelayGeneratedOutcome> {
    SeededSimulationRunner::new(scenario.seed)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .run_with(move |context| {
            let scenario = scenario.clone();
            async move {
                let db = context
                    .open_db(simulation_config("/relays/generated-campaign"))
                    .await?;
                let outbox_table = db.create_table(row_table_config("outbox")).await?;
                let todos = db.create_table(row_table_config("todos")).await?;
                let outbox = terracedb::TransactionalOutbox::new(outbox_table.clone());
                let clock: Arc<dyn Clock> = context.clock();
                let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
                let relay = OutboxRelay::new(
                    db.clone(),
                    clock,
                    outbox_table.clone(),
                    MirrorRelayHandler {
                        todos: todos.clone(),
                    },
                )
                .with_batch_limit(8)
                .with_idle_poll_interval(Duration::from_millis(1));
                let relay_task = tokio::spawn(async move { relay.run(shutdown_rx).await });

                let mut expected = std::collections::BTreeSet::new();
                let mut observed_counts = Vec::new();
                for (batch_index, batch) in scenario.batches.iter().enumerate() {
                    let mut write_batch = db.write_batch();
                    for (entry_index, todo_id) in batch.iter().enumerate() {
                        outbox.stage_entry(
                            &mut write_batch,
                            OutboxEntry {
                                outbox_id: format!("generated:{batch_index}:{entry_index}")
                                    .into_bytes(),
                                idempotency_key: format!("generated:{batch_index}:{entry_index}"),
                                payload: todo_id.clone().into_bytes(),
                            },
                        )?;
                        expected.insert(todo_id.clone());
                    }
                    db.commit(write_batch, CommitOptions::default()).await?;
                    observed_counts
                        .push(observe_row_counts_until(&db, &todos, expected.len()).await?);
                }

                let rows = collect_rows(&todos).await?;
                let expected_rows = expected.into_iter().collect::<Vec<_>>();
                assert_eq!(rows, expected_rows);
                assert_eq!(count_rows(&outbox_table).await?, 0);

                shutdown_tx.send_replace(true);
                relay_task
                    .await
                    .expect("generated relay task should join")?;
                Ok(RelayGeneratedOutcome {
                    rows,
                    observed_counts,
                })
            }
        })
}

#[test]
fn relay_simulation_applies_placeholder_batch_atomically() -> turmoil::Result {
    SeededSimulationRunner::new(0x7711)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .run_with(|context| async move {
            let db = context
                .open_db(simulation_config("/relays/atomic-batch"))
                .await?;
            let outbox_table = db.create_table(row_table_config("outbox")).await?;
            let todos = db.create_table(row_table_config("todos")).await?;
            let outbox = terracedb::TransactionalOutbox::new(outbox_table.clone());
            let clock: Arc<dyn Clock> = context.clock();
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let relay = OutboxRelay::new(
                db.clone(),
                clock.clone(),
                outbox_table.clone(),
                MirrorRelayHandler {
                    todos: todos.clone(),
                },
            )
            .with_batch_limit(16)
            .with_idle_poll_interval(Duration::from_millis(1));

            let relay_task = tokio::spawn(async move { relay.run(shutdown_rx).await });

            let mut batch = db.write_batch();
            for day in 0..7 {
                outbox.stage_entry(
                    &mut batch,
                    OutboxEntry {
                        outbox_id: format!("planner:{day}").into_bytes(),
                        idempotency_key: format!("planner:{day}"),
                        payload: format!("placeholder:{day}").into_bytes(),
                    },
                )?;
            }
            db.commit(batch, CommitOptions::default()).await?;

            let observed = observe_row_counts_until(&db, &todos, 7).await?;

            assert_eq!(count_rows(&todos).await?, 7);
            assert_eq!(count_rows(&outbox_table).await?, 0);
            assert!(observed.iter().all(|count| *count == 0 || *count == 7));

            shutdown_tx.send_replace(true);
            relay_task.await.expect("relay task should join")?;
            Ok(())
        })
}

#[test]
fn relay_simulation_processes_later_batches_after_idle() -> turmoil::Result {
    SeededSimulationRunner::new(0x7712)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .run_with(|context| async move {
            let db = context
                .open_db(simulation_config("/relays/later-batches"))
                .await?;
            let outbox_table = db.create_table(row_table_config("outbox")).await?;
            let todos = db.create_table(row_table_config("todos")).await?;
            let outbox = terracedb::TransactionalOutbox::new(outbox_table.clone());
            let clock: Arc<dyn Clock> = context.clock();
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let relay = OutboxRelay::new(
                db.clone(),
                clock.clone(),
                outbox_table.clone(),
                MirrorRelayHandler {
                    todos: todos.clone(),
                },
            )
            .with_batch_limit(16)
            .with_idle_poll_interval(Duration::from_millis(1));

            let relay_task = tokio::spawn(async move { relay.run(shutdown_rx).await });

            let mut first = db.write_batch();
            for day in 0..2 {
                outbox.stage_entry(
                    &mut first,
                    OutboxEntry {
                        outbox_id: format!("planner:first:{day}").into_bytes(),
                        idempotency_key: format!("planner:first:{day}"),
                        payload: format!("first:{day}").into_bytes(),
                    },
                )?;
            }
            db.commit(first, CommitOptions::default()).await?;

            observe_row_counts_until(&db, &todos, 2).await?;
            assert_eq!(count_rows(&todos).await?, 2);

            let mut second = db.write_batch();
            for day in 0..3 {
                outbox.stage_entry(
                    &mut second,
                    OutboxEntry {
                        outbox_id: format!("planner:second:{day}").into_bytes(),
                        idempotency_key: format!("planner:second:{day}"),
                        payload: format!("second:{day}").into_bytes(),
                    },
                )?;
            }
            db.commit(second, CommitOptions::default()).await?;

            observe_row_counts_until(&db, &todos, 5).await?;

            assert_eq!(count_rows(&todos).await?, 5);
            assert_eq!(
                collect_rows(&todos).await?,
                vec![
                    "first:0".to_string(),
                    "first:1".to_string(),
                    "second:0".to_string(),
                    "second:1".to_string(),
                    "second:2".to_string(),
                ]
            );
            assert_eq!(count_rows(&outbox_table).await?, 0);

            shutdown_tx.send_replace(true);
            relay_task.await.expect("relay task should join")?;
            Ok(())
        })
}

#[test]
fn relay_generated_campaign_replays_same_seed() -> turmoil::Result {
    let replay = assert_seed_replays(&RelayGeneratedHarness, 0x7713)?;
    assert!(!replay.outcome.rows.is_empty());
    assert!(
        replay
            .outcome
            .observed_counts
            .iter()
            .all(|counts| !counts.is_empty())
    );
    Ok(())
}

#[test]
fn relay_generated_campaign_varies_across_seeds() -> turmoil::Result {
    let _ = assert_seed_variation(&RelayGeneratedHarness, 0x7713, 0x7714, |left, right| {
        left.scenario != right.scenario || left.outcome != right.outcome
    })?;
    Ok(())
}
