use std::{collections::BTreeMap, io, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use terracedb::{
    ChangeEntry, Db, LogCursor, ScanOptions, SequenceNumber, StubClock, StubFileSystem,
    StubObjectStore, Table, Value,
    test_support::{row_table_config, test_dependencies_with_clock, tiered_test_config},
};
use terracedb_debezium::{
    DebeziumColumnProjection, DebeziumEvent, DebeziumEventLogTables, DebeziumIngressHandler,
    DebeziumMaterializer, DebeziumMirrorChange, DebeziumMirrorRow, DebeziumMirrorTables,
    DebeziumPartitionedTableLayout, DebeziumPrimaryKey, DebeziumRowPredicate, DebeziumSourceTable,
    DebeziumTableFilter, DebeziumWorkflowEventPolicy, PostgresDebeziumDecoder,
    mirror_workflow_source_config,
};
use terracedb_fuzz::{
    GeneratedScenarioHarness, assert_seed_replays, assert_seed_variation, decode_json_artifact,
    encode_json_artifact,
};
use terracedb_kafka::{
    DeterministicKafkaBroker, DeterministicKafkaFetchResponse, DeterministicKafkaPartitionScript,
    KafkaBootstrapPolicy, KafkaFetchedBatch, KafkaOffset, KafkaPartitionClaim,
    KafkaPartitionSource, KafkaProgressStore, KafkaSourceProgress, KeepAllKafkaRecords,
    NoopKafkaRuntimeObserver, NoopKafkaTelemetrySink, TableKafkaProgressStore,
    drive_partition_once,
};
use terracedb_projections::{
    MultiSourceProjection, MultiSourceProjectionHandler, ProjectionContext, ProjectionError,
    ProjectionHandle, ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun,
    ProjectionTransaction, RecomputeStrategy,
};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_workflows::{
    WorkflowContext, WorkflowDefinition, WorkflowError, WorkflowHandler, WorkflowHandlerError,
    WorkflowOutput, WorkflowRuntime, WorkflowSource, WorkflowSourceConfig, WorkflowSourceProgress,
    WorkflowSourceRecoveryPolicy, WorkflowStateMutation, WorkflowTrigger,
};

const FULL_SCAN_START: &[u8] = b"";
const FULL_SCAN_END: &[u8] = &[0xff];

#[derive(Clone, Debug, PartialEq, Eq)]
struct DebeziumCampaignCapture {
    cdc_rows: BTreeMap<String, Vec<Vec<u8>>>,
    current_rows: Vec<(Vec<u8>, Vec<u8>)>,
    projection_rows: Vec<(Vec<u8>, Vec<u8>)>,
    workflow_states: BTreeMap<String, Option<Vec<u8>>>,
    progress: BTreeMap<u32, KafkaSourceProgress>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DebeziumCampaignScenario {
    seed: u64,
    snapshot_delay_polls: u8,
    live_delay_polls: u8,
    second_partition_delay_polls: u8,
}

struct DebeziumCampaignHarness;

impl GeneratedScenarioHarness for DebeziumCampaignHarness {
    type Scenario = DebeziumCampaignScenario;
    type Outcome = DebeziumCampaignCapture;
    type Error = Box<dyn std::error::Error>;

    fn generate(&self, seed: u64) -> Self::Scenario {
        generate_debezium_campaign(seed)
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        Ok(run_generated_campaign(scenario)?)
    }
}

fn generate_debezium_campaign(seed: u64) -> DebeziumCampaignScenario {
    DebeziumCampaignScenario {
        seed,
        snapshot_delay_polls: (seed & 1) as u8,
        live_delay_polls: ((seed >> 1) & 1) as u8,
        second_partition_delay_polls: ((seed >> 2) & 1) as u8,
    }
}

struct WestOrderProjection {
    output: Table,
}

#[async_trait]
impl MultiSourceProjectionHandler for WestOrderProjection {
    async fn apply(
        &self,
        run: &ProjectionSequenceRun,
        _ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        for entry in run.entries() {
            let Some(value) = &entry.value else {
                continue;
            };
            let event = DebeziumEvent::from_value(value).map_err(ProjectionHandlerError::new)?;
            if !DebeziumWorkflowEventPolicy::SkipSnapshotsAndTombstones.accepts(&event) {
                continue;
            }

            let order_id = order_id_from_primary_key(&event.primary_key)
                .map_err(ProjectionHandlerError::new)?;
            let key = format!("order-{order_id}").into_bytes();
            let after_is_west =
                event.after.as_ref().and_then(|row| row.get("region")) == Some(&json!("west"));
            if after_is_west {
                let encoded =
                    serde_json::to_vec(event.after.as_ref().expect("west rows must exist"))
                        .map_err(ProjectionHandlerError::new)?;
                tx.put(&self.output, key, Value::bytes(encoded));
            } else {
                tx.delete(&self.output, key);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct MirrorWorkflowHandler;

#[async_trait]
impl WorkflowHandler for MirrorWorkflowHandler {
    async fn route_event(&self, entry: &ChangeEntry) -> Result<String, WorkflowHandlerError> {
        let change = DebeziumMirrorChange::decode(entry).map_err(WorkflowHandlerError::new)?;
        order_id_from_primary_key(change.primary_key()).map_err(WorkflowHandlerError::new)
    }

    async fn handle(
        &self,
        _instance_id: &str,
        state: Option<Value>,
        _trigger: &WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        let next = decode_count(state) + 1;
        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(Value::bytes(next.to_string())),
            outbox_entries: Vec::new(),
            timers: Vec::new(),
        })
    }
}

fn decode_count(value: Option<Value>) -> usize {
    match value {
        None => 0,
        Some(Value::Bytes(bytes)) => std::str::from_utf8(&bytes)
            .expect("workflow state should be utf-8")
            .parse()
            .expect("workflow state should encode a count"),
        Some(Value::Record(_)) => panic!("workflow tests only expect byte state"),
    }
}

fn order_id_from_primary_key(primary_key: &DebeziumPrimaryKey) -> Result<String, io::Error> {
    let Some(value) = primary_key.fields().get("id") else {
        return Err(io::Error::other("primary key is missing `id`"));
    };
    match value {
        serde_json::Value::String(text) => Ok(text.clone()),
        serde_json::Value::Number(number) => Ok(number.to_string()),
        other => Err(io::Error::other(format!(
            "primary key `id` must be a string or number, found {other}"
        ))),
    }
}

fn debezium_key(id: u64) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "payload": {
            "id": id
        }
    }))
    .expect("encode key fixture")
}

fn change_record(
    topic: &str,
    partition: u32,
    offset: u64,
    id: u64,
    operation: &str,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    snapshot: Option<&str>,
    tx_id: &str,
) -> terracedb_kafka::KafkaRecord {
    let mut record = terracedb_kafka::KafkaRecord::new(topic, partition, offset);
    record.key = Some(debezium_key(id));
    record.timestamp_millis = Some(1_700_000_000_000 + offset);
    record.value = Some(
        serde_json::to_vec(&json!({
            "payload": {
                "before": before,
                "after": after,
                "op": operation,
                "source": {
                    "db": "app",
                    "schema": "public",
                    "table": "orders",
                    "snapshot": snapshot.unwrap_or("false"),
                    "lsn": format!("{}", 100 + offset),
                    "txId": format!("{}", 200 + offset),
                    "ts_ms": format!("{}", 1_700_000_000_100_u64 + offset),
                },
                "ts_ms": format!("{}", 1_700_000_000_200_u64 + offset),
                "transaction": {
                    "id": tx_id,
                    "total_order": format!("{}", offset + 1),
                    "data_collection_order": format!("{}", offset + 1),
                }
            }
        }))
        .expect("encode change fixture"),
    );
    record
}

fn tombstone_record(
    topic: &str,
    partition: u32,
    offset: u64,
    id: u64,
) -> terracedb_kafka::KafkaRecord {
    let mut record = terracedb_kafka::KafkaRecord::new(topic, partition, offset);
    record.key = Some(debezium_key(id));
    record.timestamp_millis = Some(1_700_000_000_000 + offset);
    record
}

fn delayed_single(
    empty_polls: usize,
    record: terracedb_kafka::KafkaRecord,
    high_watermark: u64,
) -> Vec<DeterministicKafkaFetchResponse> {
    let mut responses =
        vec![
            DeterministicKafkaFetchResponse::empty(Some(KafkaOffset::new(high_watermark)));
            empty_polls
        ];
    responses.push(DeterministicKafkaFetchResponse::batch(
        KafkaFetchedBatch::new(vec![record], Some(KafkaOffset::new(high_watermark))),
    ));
    responses
}

fn build_campaign_broker(
    scenario: &DebeziumCampaignScenario,
    topic: &str,
) -> DeterministicKafkaBroker {
    let partition0 =
        DeterministicKafkaPartitionScript::new(KafkaOffset::new(0), KafkaOffset::new(3))
            .with_fetch_responses(
                KafkaOffset::new(0),
                delayed_single(
                    scenario.snapshot_delay_polls as usize,
                    change_record(
                        topic,
                        0,
                        0,
                        1,
                        "r",
                        None,
                        Some(json!({
                            "id": 1,
                            "region": "west",
                            "status": "open"
                        })),
                        Some("true"),
                        "tx-p0-0",
                    ),
                    3,
                ),
            )
            .with_fetch_responses(
                KafkaOffset::new(1),
                [DeterministicKafkaFetchResponse::batch(
                    KafkaFetchedBatch::new(
                        vec![change_record(
                            topic,
                            0,
                            1,
                            1,
                            "u",
                            Some(json!({
                                "id": 1,
                                "region": "west",
                                "status": "open"
                            })),
                            Some(json!({
                                "id": 1,
                                "region": "east",
                                "status": "closed"
                            })),
                            None,
                            "tx-p0-1",
                        )],
                        Some(KafkaOffset::new(3)),
                    ),
                )],
            )
            .with_fetch_responses(
                KafkaOffset::new(2),
                delayed_single(
                    scenario.live_delay_polls as usize,
                    change_record(
                        topic,
                        0,
                        2,
                        3,
                        "c",
                        None,
                        Some(json!({
                            "id": 3,
                            "region": "west",
                            "status": "open"
                        })),
                        None,
                        "tx-p0-2",
                    ),
                    3,
                ),
            );

    let partition1 =
        DeterministicKafkaPartitionScript::new(KafkaOffset::new(0), KafkaOffset::new(3))
            .with_fetch_responses(
                KafkaOffset::new(0),
                delayed_single(
                    scenario.second_partition_delay_polls as usize,
                    change_record(
                        topic,
                        1,
                        0,
                        2,
                        "r",
                        None,
                        Some(json!({
                            "id": 2,
                            "region": "west",
                            "status": "open"
                        })),
                        Some("last"),
                        "tx-p1-0",
                    ),
                    3,
                ),
            )
            .with_fetch_responses(
                KafkaOffset::new(1),
                [DeterministicKafkaFetchResponse::batch(
                    KafkaFetchedBatch::new(
                        vec![change_record(
                            topic,
                            1,
                            1,
                            2,
                            "d",
                            Some(json!({
                                "id": 2,
                                "region": "west",
                                "status": "open"
                            })),
                            None,
                            None,
                            "tx-p1-1",
                        )],
                        Some(KafkaOffset::new(3)),
                    ),
                )],
            )
            .with_fetch_responses(
                KafkaOffset::new(2),
                [DeterministicKafkaFetchResponse::batch(
                    KafkaFetchedBatch::new(
                        vec![tombstone_record(topic, 1, 2, 2)],
                        Some(KafkaOffset::new(3)),
                    ),
                )],
            );

    DeterministicKafkaBroker::new([
        (
            terracedb_kafka::KafkaTopicPartition::new(topic, 0),
            partition0,
        ),
        (
            terracedb_kafka::KafkaTopicPartition::new(topic, 1),
            partition1,
        ),
    ])
}

async fn collect_bytes_rows(
    table: &Table,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, terracedb::ReadError> {
    let mut rows = table
        .scan(
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            ScanOptions::default(),
        )
        .await?;
    let mut out = Vec::new();
    while let Some((key, value)) = rows.next().await {
        let Value::Bytes(bytes) = value else {
            panic!("test tables should only store byte values");
        };
        out.push((key, bytes));
    }
    Ok(out)
}

async fn collect_event_rows(table: &Table) -> Result<Vec<Vec<u8>>, terracedb::ReadError> {
    let mut rows = table
        .scan(
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            ScanOptions::default(),
        )
        .await?;
    let mut out = Vec::new();
    while let Some((_key, value)) = rows.next().await {
        let Value::Bytes(bytes) = value else {
            panic!("cdc fixtures should store byte values");
        };
        out.push(bytes);
    }
    Ok(out)
}

async fn collect_current_rows(
    table: &Table,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, terracedb::ReadError> {
    let mut rows = table
        .scan(
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            ScanOptions::default(),
        )
        .await?;
    let mut out = Vec::new();
    while let Some((key, value)) = rows.next().await {
        let Value::Bytes(bytes) = value else {
            panic!("mirror fixtures should store byte values");
        };
        out.push((key, bytes));
    }
    Ok(out)
}

async fn wait_for_projection<'a, I>(
    handle: &mut ProjectionHandle,
    targets: I,
) -> Result<(), ProjectionError>
where
    I: IntoIterator<Item = (&'a Table, SequenceNumber)>,
{
    tokio::time::timeout(Duration::from_secs(1), handle.wait_for_sources(targets))
        .await
        .expect("projection should settle")
}

async fn wait_for_state(
    runtime: &WorkflowRuntime<MirrorWorkflowHandler>,
    instance_id: &str,
    expected: &str,
) -> Result<(), WorkflowError> {
    runtime
        .wait_for_state(instance_id, Value::bytes(expected))
        .await
}

fn run_generated_campaign(
    scenario: DebeziumCampaignScenario,
) -> turmoil::Result<DebeziumCampaignCapture> {
    SeededSimulationRunner::new(scenario.seed).run_with(move |context| {
        let scenario = scenario.clone();
        async move {
            let db = context
                .open_db(tiered_test_config(&format!(
                    "/debezium/campaign-{}",
                    scenario.seed
                )))
                .await?;
            let layout = DebeziumPartitionedTableLayout::new(
                "debezium",
                "dbserver1.public.orders",
                DebeziumSourceTable::new("app", "public", "orders"),
                [0_u32, 1_u32],
            );

            for table_name in layout.cdc_table_names() {
                db.create_table(row_table_config(&table_name)).await?;
            }
            let current = db
                .create_table(row_table_config(layout.current_table_name()))
                .await?;
            let progress_table = db.create_table(row_table_config("kafka_progress")).await?;
            let projection_output = db.create_table(row_table_config("attention")).await?;

            let progress_store = TableKafkaProgressStore::new(progress_table);
            let event_log = DebeziumEventLogTables::from_layouts(&db, [&layout])?;
            let mirror = DebeziumMirrorTables::from_layouts(&db, [&layout])?;
            let materializer = DebeziumMaterializer::hybrid(event_log, mirror)
                .with_table_filter(DebeziumTableFilter::allow_only([layout
                    .source_table()
                    .clone()]))
                .with_row_predicate(DebeziumRowPredicate::ColumnEquals {
                    column: "region".to_string(),
                    value: json!("west"),
                })
                .with_column_projection(
                    DebeziumColumnProjection::default()
                        .include_only(["id", "region", "status"])
                        .with_redaction("status", json!("redacted")),
                );
            let handler = DebeziumIngressHandler::new(
                PostgresDebeziumDecoder::new().with_layouts([&layout]),
                materializer,
            );
            let broker = build_campaign_broker(&scenario, layout.topic());
            let claim0 = KafkaPartitionClaim::new(
                KafkaPartitionSource::new(layout.topic(), 0, KafkaBootstrapPolicy::Earliest)
                    .source_id("orders-cg"),
                KafkaBootstrapPolicy::Earliest,
                1,
            );
            let claim1 = KafkaPartitionClaim::new(
                KafkaPartitionSource::new(layout.topic(), 1, KafkaBootstrapPolicy::Earliest)
                    .source_id("orders-cg"),
                KafkaBootstrapPolicy::Earliest,
                1,
            );

            let observer = NoopKafkaRuntimeObserver;
            let telemetry = NoopKafkaTelemetrySink;
            let worker = terracedb_kafka::KafkaWorkerOptions {
                batch_limit: 1,
                ..terracedb_kafka::KafkaWorkerOptions::default()
            };

            let mut last_sequence_by_partition = BTreeMap::<u32, SequenceNumber>::new();

            while progress_store
                .load(&claim0.source)
                .await?
                .map(|progress| progress.next_offset.get())
                .unwrap_or(0)
                < 2
            {
                let outcome = drive_partition_once(
                    &db,
                    &broker,
                    &progress_store,
                    &claim0,
                    worker,
                    &KeepAllKafkaRecords,
                    &handler,
                    &observer,
                    &telemetry,
                )
                .await?;
                if let Some(sequence) = outcome.telemetry.committed_sequence {
                    last_sequence_by_partition.insert(0, sequence);
                }
            }

            while progress_store
                .load(&claim1.source)
                .await?
                .map(|progress| progress.next_offset.get())
                .unwrap_or(0)
                < 3
            {
                let outcome = drive_partition_once(
                    &db,
                    &broker,
                    &progress_store,
                    &claim1,
                    worker,
                    &KeepAllKafkaRecords,
                    &handler,
                    &observer,
                    &telemetry,
                )
                .await?;
                if let Some(sequence) = outcome.telemetry.committed_sequence {
                    last_sequence_by_partition.insert(1, sequence);
                }
            }

            let workflow_runtime = WorkflowRuntime::open(
                db.clone(),
                context.clock(),
                WorkflowDefinition::new(
                    "mirror-orders",
                    [layout.mirror_workflow_source(&db)],
                    MirrorWorkflowHandler,
                ),
            )
            .await?;
            let workflow_handle = workflow_runtime.start().await?;

            while progress_store
                .load(&claim0.source)
                .await?
                .map(|progress| progress.next_offset.get())
                .unwrap_or(0)
                < 3
            {
                let outcome = drive_partition_once(
                    &db,
                    &broker,
                    &progress_store,
                    &claim0,
                    worker,
                    &KeepAllKafkaRecords,
                    &handler,
                    &observer,
                    &telemetry,
                )
                .await?;
                if let Some(sequence) = outcome.telemetry.committed_sequence {
                    last_sequence_by_partition.insert(0, sequence);
                }
            }

            wait_for_state(&workflow_runtime, "3", "1").await?;

            let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
            let sources = layout.projection_sources(&db);
            let mut projection_handle = projection_runtime
                .start_multi_source(
                    MultiSourceProjection::new(
                        "west-orders",
                        sources.clone(),
                        WestOrderProjection {
                            output: projection_output.clone(),
                        },
                    )
                    .with_outputs([projection_output.clone()])
                    .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
                )
                .await?;
            wait_for_projection(
                &mut projection_handle,
                [
                    (
                        &sources[0],
                        *last_sequence_by_partition.get(&0).expect("p0 sequence"),
                    ),
                    (
                        &sources[1],
                        *last_sequence_by_partition.get(&1).expect("p1 sequence"),
                    ),
                ],
            )
            .await?;

            let mut cdc_rows = BTreeMap::new();
            for table_name in layout.cdc_table_names() {
                cdc_rows.insert(
                    table_name.clone(),
                    collect_event_rows(&db.table(table_name)).await?,
                );
            }
            let current_rows = collect_current_rows(&current).await?;
            let projection_rows = collect_bytes_rows(&projection_output).await?;
            let workflow_states = BTreeMap::from([
                (
                    "1".to_string(),
                    workflow_runtime
                        .load_state("1")
                        .await?
                        .map(expect_bytes_value),
                ),
                (
                    "2".to_string(),
                    workflow_runtime
                        .load_state("2")
                        .await?
                        .map(expect_bytes_value),
                ),
                (
                    "3".to_string(),
                    workflow_runtime
                        .load_state("3")
                        .await?
                        .map(expect_bytes_value),
                ),
            ]);
            let progress = BTreeMap::from([
                (
                    0_u32,
                    progress_store
                        .load(&claim0.source)
                        .await?
                        .expect("partition 0 progress should exist"),
                ),
                (
                    1_u32,
                    progress_store
                        .load(&claim1.source)
                        .await?
                        .expect("partition 1 progress should exist"),
                ),
            ]);

            projection_handle.shutdown().await?;
            workflow_handle.shutdown().await?;

            Ok(DebeziumCampaignCapture {
                cdc_rows,
                current_rows,
                projection_rows,
                workflow_states,
                progress,
            })
        }
    })
}

fn expect_bytes_value(value: Value) -> Vec<u8> {
    let Value::Bytes(bytes) = value else {
        panic!("workflow states should be bytes");
    };
    bytes
}

#[test]
fn hybrid_ingress_projection_and_workflow_campaign_is_seed_stable()
-> Result<(), Box<dyn std::error::Error>> {
    let replay = assert_seed_replays(&DebeziumCampaignHarness, 0xd9_25)?;
    let encoded = encode_json_artifact(&replay.scenario)?;
    let decoded: DebeziumCampaignScenario = decode_json_artifact(&encoded)?;
    let current_key =
        DebeziumPrimaryKey::decode(&replay.outcome.current_rows[0].0).expect("decode mirror key");

    assert_eq!(decoded, replay.scenario);

    assert_eq!(
        replay
            .outcome
            .progress
            .get(&0)
            .map(|progress| progress.next_offset.get()),
        Some(3)
    );
    assert_eq!(
        replay
            .outcome
            .progress
            .get(&1)
            .map(|progress| progress.next_offset.get()),
        Some(3)
    );
    assert_eq!(replay.outcome.current_rows.len(), 1);
    assert_eq!(current_key.fields.get("id"), Some(&json!(3)));
    assert_eq!(replay.outcome.workflow_states.get("1"), Some(&None));
    assert_eq!(replay.outcome.workflow_states.get("2"), Some(&None));
    assert_eq!(
        replay.outcome.workflow_states.get("3"),
        Some(&Some(b"1".to_vec()))
    );
    assert_eq!(replay.outcome.projection_rows.len(), 1);
    assert_eq!(replay.outcome.projection_rows[0].0, b"order-3".to_vec());
    Ok(())
}

#[test]
fn hybrid_ingress_projection_and_workflow_campaign_keeps_outputs_across_delay_variants()
-> Result<(), Box<dyn std::error::Error>> {
    let (left, right) =
        assert_seed_variation(&DebeziumCampaignHarness, 0xd9_25, 0xd9_26, |left, right| {
            left.scenario != right.scenario
        })?;

    assert_ne!(left.scenario, right.scenario);
    assert_eq!(left.outcome, right.outcome);
    Ok(())
}

#[tokio::test]
async fn mirror_sources_fail_closed_when_replay_from_history_is_requested() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config("/debezium-mirror-replay-fail"),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let layout = DebeziumPartitionedTableLayout::new(
        "debezium",
        "dbserver1.public.orders",
        DebeziumSourceTable::new("app", "public", "orders"),
        [0_u32],
    );
    let mut mirror_config = row_table_config(layout.current_table_name());
    mirror_config.history_retention_sequences = Some(1);
    let current = db
        .create_table(mirror_config)
        .await
        .expect("create mirror source");

    let key = DebeziumPrimaryKey::new([("id".to_string(), json!(9))].into_iter().collect());
    let first = current
        .write(
            key.encode().expect("encode key"),
            DebeziumMirrorRow {
                source_table: layout.source_table().clone(),
                primary_key: key.clone(),
                values: [
                    ("id".to_string(), json!(9)),
                    ("region".to_string(), json!("west")),
                ]
                .into_iter()
                .collect(),
                snapshot: None,
                kafka: terracedb_debezium::DebeziumKafkaCoordinates {
                    topic: layout.topic().to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_millis: Some(1),
                },
                transaction: None,
            }
            .to_value()
            .expect("encode mirror row"),
        )
        .await
        .expect("write first mirror row");
    current
        .write(
            key.encode().expect("encode key"),
            DebeziumMirrorRow {
                source_table: layout.source_table().clone(),
                primary_key: key.clone(),
                values: [
                    ("id".to_string(), json!(9)),
                    ("region".to_string(), json!("west")),
                ]
                .into_iter()
                .collect(),
                snapshot: None,
                kafka: terracedb_debezium::DebeziumKafkaCoordinates {
                    topic: layout.topic().to_string(),
                    partition: 0,
                    offset: 1,
                    timestamp_millis: Some(2),
                },
                transaction: None,
            }
            .to_value()
            .expect("encode mirror row"),
        )
        .await
        .expect("write second mirror row");

    let source = WorkflowSource::new(current.clone()).with_config(
        WorkflowSourceConfig::default()
            .with_bootstrap_policy(mirror_workflow_source_config().bootstrap)
            .with_capabilities(mirror_workflow_source_config().capabilities)
            .with_recovery_policy(WorkflowSourceRecoveryPolicy::ReplayFromHistory),
    );
    let runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new("mirror-replay", [source], MirrorWorkflowHandler),
    )
    .await
    .expect("open workflow runtime");

    let stale = WorkflowSourceProgress::from_cursor(LogCursor::new(first, 0));
    runtime
        .tables()
        .source_progress_table()
        .write(
            current.name().as_bytes().to_vec(),
            Value::bytes(stale.encode().expect("encode stale progress")),
        )
        .await
        .expect("persist stale workflow progress");

    let mut handle = runtime.start().await.expect("start workflow");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("workflow failure should be observed")
        .expect("mirror workflow should terminate after replay-from-history failure");
    let error = tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("workflow shutdown should finish")
        .expect_err("mirror source should not satisfy replay-from-history");

    assert!(
        error.to_string().contains("cannot replay from history"),
        "unexpected error: {error}"
    );
}
