use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use serde_json::{Value as JsonValue, json};
use terracedb::{
    ChangeEntry, Db, LogCursor, ObjectStore, ScanOptions, SequenceNumber, StorageErrorKind,
    StubClock, StubFileSystem, StubObjectStore, TieredDurabilityMode, Transaction, Value,
    decode_outbox_entry,
    test_support::{
        FailpointMode, db_failpoint_registry, row_table_config, test_dependencies_with_clock,
        tiered_test_config, tiered_test_config_with_durability,
    },
};
use terracedb_debezium::{
    DebeziumChangeEntry, DebeziumDerivedTransition, DebeziumDerivedTransitionProjection,
    DebeziumMaterializationError, DebeziumMaterializationMode, ensure_layout_tables,
};
use terracedb_example_order_watch::{
    ATTENTION_ORDERS_TABLE_NAME, ATTENTION_TRANSITIONS_TABLE_NAME, BACKLOG_ALERT_ORDER_ID,
    FILTERED_OUT_ORDER_ID, IGNORED_CUSTOMER_ID, LIVE_TRANSITION_ORDER_ID,
    OrderAttentionOrdersTable, OrderAttentionTransition, OrderAttentionTransitionsTable,
    OrderAttentionView, OrderWatchAlert, OrderWatchBoundary, OrderWatchOracleSnapshot,
    OrderWatchOrder, OrderWatchSourceProgress, OrderWatchWorkflowMode, SNAPSHOT_WEST_ORDER_ID,
};
use terracedb_kafka::{
    DeterministicKafkaBroker, DeterministicKafkaBrokerTraceEvent, DeterministicKafkaFetchResponse,
    DeterministicKafkaPartitionScript, KafkaAdmissionBatch, KafkaBatchHandler,
    KafkaBootstrapPolicy, KafkaBroker, KafkaFetchedBatch, KafkaOffset, KafkaPartitionClaim,
    KafkaPartitionSource, KafkaProgressStore, KafkaRecord, KafkaRuntimeError, KafkaRuntimeEvent,
    KafkaSourceId, KafkaSourceProgress, KafkaTopicPartition, KeepAllKafkaRecords,
    NoopKafkaRuntimeObserver, NoopKafkaTelemetrySink, TableKafkaProgressStore,
    drive_partition_once, drive_partition_once_with_retry,
};
use terracedb_projections::{
    MultiSourceProjection, MultiSourceProjectionHandler, ProjectionContext, ProjectionError,
    ProjectionHandle, ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun,
    ProjectionTransaction, RecomputeStrategy,
};
use terracedb_records::{JsonValueCodec, RecordTable, Utf8StringCodec};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_workflows::{
    WorkflowCheckpointId, WorkflowContext, WorkflowDefinition, WorkflowError, WorkflowHandler,
    WorkflowHandlerError, WorkflowObjectStoreCheckpointStore, WorkflowOutput, WorkflowRuntime,
    WorkflowSource, WorkflowSourceAttachMode, WorkflowSourceConfig, WorkflowSourceProgress,
    WorkflowSourceRecoveryPolicy, WorkflowStateMutation, WorkflowTrigger,
    contracts::{WorkflowPayload, WorkflowStateRecord},
    failpoints as workflow_failpoints,
};

const FULL_SCAN_START: &[u8] = b"";
const FULL_SCAN_END: &[u8] = &[0xff];
const WORKFLOW_NAME: &str = "order-watch-alerts";

struct AttentionOrdersProjection {
    boundary: OrderWatchBoundary,
    attention_orders: terracedb::Table,
}

#[async_trait]
impl MultiSourceProjectionHandler for AttentionOrdersProjection {
    async fn apply(
        &self,
        run: &ProjectionSequenceRun,
        _ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        for entry in run.entries() {
            let decoded =
                DebeziumChangeEntry::decode(entry).map_err(ProjectionHandlerError::new)?;
            let Some(event) = decoded.as_event() else {
                continue;
            };
            let order_id = event
                .primary_key
                .require_string_or_number("id")
                .map_err(ProjectionHandlerError::new)?;
            let membership = event.membership(&self.boundary.attention_row_predicate());

            if membership.after {
                let view = OrderAttentionView {
                    order_id: order_id.clone(),
                    reason: self.boundary.attention_reason().to_string(),
                    source_partition: event.kafka.partition,
                    source_offset: event.kafka.offset,
                };
                tx.put(
                    &self.attention_orders,
                    order_id.into_bytes(),
                    serde_json::to_vec(&view)
                        .map(Value::bytes)
                        .map_err(ProjectionHandlerError::new)?,
                );
            } else {
                tx.delete(&self.attention_orders, order_id.into_bytes());
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct AlertWorkflowHandler {
    transitions: terracedb_example_order_watch::OrderAttentionTransitionsTable,
}

#[async_trait]
impl WorkflowHandler for AlertWorkflowHandler {
    async fn route_event(&self, entry: &ChangeEntry) -> Result<String, WorkflowHandlerError> {
        let change = self
            .transitions
            .decode_change_entry(entry)
            .map_err(WorkflowHandlerError::new)?;
        let transition = change.value.ok_or_else(|| {
            WorkflowHandlerError::new(io::Error::other(
                "order-watch transitions must remain append-only",
            ))
        })?;
        Ok(transition.order_id)
    }

    async fn handle(
        &self,
        instance_id: &str,
        state: Option<Value>,
        trigger: &WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        let WorkflowTrigger::Event(entry) = trigger else {
            return Err(WorkflowHandlerError::new(io::Error::other(
                "order-watch only routes source events",
            )));
        };
        let current = decode_state_count(state);
        let transition = self
            .transitions
            .decode_change_entry(entry)
            .map_err(WorkflowHandlerError::new)?
            .value
            .ok_or_else(|| {
                WorkflowHandlerError::new(io::Error::other(
                    "order-watch transitions must remain append-only",
                ))
            })?;
        let alert: OrderWatchAlert = transition.clone().into();
        let next = current + 1;

        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(Value::bytes(next.to_string())),
            outbox_entries: vec![terracedb::OutboxEntry {
                outbox_id: format!("alert:{instance_id}:{next}").into_bytes(),
                idempotency_key: format!("alert:{instance_id}:{next}"),
                payload: serde_json::to_vec(&alert).map_err(WorkflowHandlerError::new)?,
            }],
            timers: Vec::new(),
        })
    }
}

#[derive(Clone, Default)]
struct RecordingKafkaObserver {
    events: Arc<Mutex<Vec<KafkaRuntimeEvent>>>,
}

impl RecordingKafkaObserver {
    fn events(&self) -> Vec<KafkaRuntimeEvent> {
        self.events
            .lock()
            .expect("recording kafka observer lock should not be poisoned")
            .clone()
    }
}

impl terracedb_kafka::KafkaRuntimeObserver for RecordingKafkaObserver {
    fn on_event(&self, event: KafkaRuntimeEvent) {
        self.events
            .lock()
            .expect("recording kafka observer lock should not be poisoned")
            .push(event);
    }
}

#[derive(Clone)]
struct ConflictOnceThen<H> {
    inner: H,
    conflict_table: terracedb::Table,
    triggered: Arc<AtomicBool>,
}

#[async_trait]
impl<H> KafkaBatchHandler for ConflictOnceThen<H>
where
    H: KafkaBatchHandler<Error = DebeziumMaterializationError> + Send + Sync,
{
    type Error = io::Error;

    async fn apply_batch(
        &self,
        tx: &mut Transaction,
        batch: &KafkaAdmissionBatch,
    ) -> Result<(), Self::Error> {
        let guard_key = b"guard".to_vec();
        let _ = tx
            .read(&self.conflict_table, guard_key.clone())
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        if !self.triggered.swap(true, Ordering::SeqCst) {
            self.conflict_table
                .write(guard_key, Value::bytes(b"conflict".to_vec()))
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
        }

        self.inner
            .apply_batch(tx, batch)
            .await
            .map_err(|error| io::Error::other(error.to_string()))
    }
}

#[derive(Clone)]
struct RestartOnceBroker<B> {
    inner: B,
    topic_partition: KafkaTopicPartition,
    requested_offset: KafkaOffset,
    message: String,
    triggered: Arc<AtomicBool>,
}

impl<B> RestartOnceBroker<B> {
    fn new(
        inner: B,
        topic: &str,
        partition: u32,
        requested_offset: KafkaOffset,
        message: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            topic_partition: KafkaTopicPartition::new(topic, partition),
            requested_offset,
            message: message.into(),
            triggered: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl<B> KafkaBroker for RestartOnceBroker<B>
where
    B: KafkaBroker,
{
    type Error = io::Error;

    async fn resolve_offset(
        &self,
        claim: &KafkaPartitionClaim,
        policy: KafkaBootstrapPolicy,
    ) -> Result<KafkaOffset, Self::Error> {
        self.inner
            .resolve_offset(claim, policy)
            .await
            .map_err(|error| io::Error::other(error.to_string()))
    }

    async fn fetch_batch(
        &self,
        claim: &KafkaPartitionClaim,
        requested_offset: KafkaOffset,
        max_records: usize,
    ) -> Result<KafkaFetchedBatch, Self::Error> {
        if claim.source.topic_partition == self.topic_partition
            && requested_offset == self.requested_offset
            && !self.triggered.swap(true, Ordering::SeqCst)
        {
            return Err(io::Error::other(self.message.clone()));
        }

        self.inner
            .fetch_batch(claim, requested_offset, max_records)
            .await
            .map_err(|error| io::Error::other(error.to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OrderWatchTraceCapture {
    snapshot: OrderWatchOracleSnapshot,
    runtime_events: Vec<KafkaRuntimeEvent>,
    broker_trace: Vec<DeterministicKafkaBrokerTraceEvent>,
}

#[derive(Clone, Debug)]
struct PhasedLatestPartition {
    earliest_offset: KafkaOffset,
    bootstrap_latest: KafkaOffset,
    current_high_watermark: KafkaOffset,
    records: BTreeMap<u64, KafkaRecord>,
}

impl PhasedLatestPartition {
    fn new(
        earliest_offset: KafkaOffset,
        bootstrap_latest: KafkaOffset,
        current_high_watermark: KafkaOffset,
        records: impl IntoIterator<Item = KafkaRecord>,
    ) -> Self {
        Self {
            earliest_offset,
            bootstrap_latest,
            current_high_watermark,
            records: records
                .into_iter()
                .map(|record| (record.offset.get(), record))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct PhasedLatestBroker {
    partitions: Arc<Mutex<BTreeMap<KafkaTopicPartition, PhasedLatestPartition>>>,
    latest_bootstrap_cache: Arc<Mutex<BTreeMap<KafkaSourceId, KafkaOffset>>>,
}

impl PhasedLatestBroker {
    fn new(
        bindings: impl IntoIterator<Item = (KafkaTopicPartition, PhasedLatestPartition)>,
    ) -> Self {
        Self {
            partitions: Arc::new(Mutex::new(bindings.into_iter().collect())),
            latest_bootstrap_cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn publish(
        &self,
        topic: &str,
        partition: u32,
        high_watermark: KafkaOffset,
        records: impl IntoIterator<Item = KafkaRecord>,
    ) {
        let mut partitions = self
            .partitions
            .lock()
            .expect("phased latest broker lock should not be poisoned");
        let partition_state = partitions
            .get_mut(&KafkaTopicPartition::new(topic, partition))
            .expect("phased latest broker partition should exist");
        partition_state.current_high_watermark = high_watermark;
        for record in records {
            partition_state.records.insert(record.offset.get(), record);
        }
    }
}

#[async_trait]
impl KafkaBroker for PhasedLatestBroker {
    type Error = std::convert::Infallible;

    async fn resolve_offset(
        &self,
        claim: &KafkaPartitionClaim,
        policy: KafkaBootstrapPolicy,
    ) -> Result<KafkaOffset, Self::Error> {
        let partitions = self
            .partitions
            .lock()
            .expect("phased latest broker lock should not be poisoned");
        let partition = partitions
            .get(&claim.source.topic_partition)
            .expect("phased latest broker partition should exist");

        Ok(match policy {
            KafkaBootstrapPolicy::Earliest => partition.earliest_offset,
            KafkaBootstrapPolicy::Latest => {
                let mut cache = self
                    .latest_bootstrap_cache
                    .lock()
                    .expect("phased latest bootstrap cache should not be poisoned");
                *cache
                    .entry(claim.source.clone())
                    .or_insert(partition.bootstrap_latest)
            }
        })
    }

    async fn fetch_batch(
        &self,
        claim: &KafkaPartitionClaim,
        next_offset: KafkaOffset,
        max_records: usize,
    ) -> Result<KafkaFetchedBatch, Self::Error> {
        let partitions = self
            .partitions
            .lock()
            .expect("phased latest broker lock should not be poisoned");
        let partition = partitions
            .get(&claim.source.topic_partition)
            .expect("phased latest broker partition should exist");

        let records = partition
            .records
            .range(next_offset.get()..partition.current_high_watermark.get())
            .map(|(_, record)| record.clone())
            .take(max_records.max(1))
            .collect::<Vec<_>>();
        Ok(KafkaFetchedBatch::new(
            records,
            Some(partition.current_high_watermark),
        ))
    }
}

fn decode_state_count(state: Option<Value>) -> usize {
    match state {
        None => 0,
        Some(Value::Bytes(bytes)) => std::str::from_utf8(&bytes)
            .expect("workflow state should be utf-8")
            .parse()
            .expect("workflow state should encode a count"),
        Some(Value::Record(_)) => panic!("order-watch only stores byte workflow state"),
    }
}

fn encode_versioned_workflow_contract<T: serde::Serialize>(value: &T) -> Value {
    let mut bytes = vec![1];
    bytes.extend(
        serde_json::to_vec(value)
            .expect("workflow contract value should serialize deterministically"),
    );
    Value::bytes(bytes)
}

fn debezium_key(id: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "payload": {
            "id": id
        }
    }))
    .expect("encode key fixture")
}

fn change_record(
    topic: &str,
    table: &str,
    partition: u32,
    offset: u64,
    id: &str,
    operation: &str,
    before: Option<JsonValue>,
    after: Option<JsonValue>,
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
                    "db": "commerce",
                    "schema": "public",
                    "table": table,
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

fn build_broker(seed: u64, boundary: &OrderWatchBoundary) -> DeterministicKafkaBroker {
    let orders_topic = boundary.orders_layout().topic();
    let customers_topic = boundary.ignored_layout().topic();
    let filtered_delay = (seed & 1) as usize;
    let backlog_delay = ((seed >> 1) & 1) as usize;
    let snapshot_delay = ((seed >> 2) & 1) as usize;
    let live_enter_delay = ((seed >> 3) & 1) as usize;
    let live_exit_delay = ((seed >> 4) & 1) as usize;
    let ignored_delay = ((seed >> 5) & 1) as usize;

    let orders_p0 =
        DeterministicKafkaPartitionScript::new(KafkaOffset::new(0), KafkaOffset::new(4))
            .with_fetch_responses(
                KafkaOffset::new(0),
                delayed_single(
                    filtered_delay,
                    change_record(
                        orders_topic,
                        "orders",
                        0,
                        0,
                        FILTERED_OUT_ORDER_ID,
                        "r",
                        None,
                        Some(json!({
                            "id": FILTERED_OUT_ORDER_ID,
                            "region": "east",
                            "status": "open"
                        })),
                        Some("true"),
                        "orders-p0-0",
                    ),
                    4,
                ),
            )
            .with_fetch_responses(
                KafkaOffset::new(1),
                delayed_single(
                    backlog_delay,
                    change_record(
                        orders_topic,
                        "orders",
                        0,
                        1,
                        BACKLOG_ALERT_ORDER_ID,
                        "c",
                        None,
                        Some(json!({
                            "id": BACKLOG_ALERT_ORDER_ID,
                            "region": "west",
                            "status": "open"
                        })),
                        None,
                        "orders-p0-1",
                    ),
                    4,
                ),
            )
            .with_fetch_responses(
                KafkaOffset::new(2),
                delayed_single(
                    live_enter_delay,
                    change_record(
                        orders_topic,
                        "orders",
                        0,
                        2,
                        LIVE_TRANSITION_ORDER_ID,
                        "c",
                        None,
                        Some(json!({
                            "id": LIVE_TRANSITION_ORDER_ID,
                            "region": "west",
                            "status": "open"
                        })),
                        None,
                        "orders-p0-2",
                    ),
                    4,
                ),
            )
            .with_fetch_responses(
                KafkaOffset::new(3),
                delayed_single(
                    live_exit_delay,
                    change_record(
                        orders_topic,
                        "orders",
                        0,
                        3,
                        LIVE_TRANSITION_ORDER_ID,
                        "u",
                        Some(json!({
                            "id": LIVE_TRANSITION_ORDER_ID,
                            "region": "west",
                            "status": "open"
                        })),
                        Some(json!({
                            "id": LIVE_TRANSITION_ORDER_ID,
                            "region": "east",
                            "status": "closed"
                        })),
                        None,
                        "orders-p0-3",
                    ),
                    4,
                ),
            );

    let orders_p1 =
        DeterministicKafkaPartitionScript::new(KafkaOffset::new(0), KafkaOffset::new(1))
            .with_fetch_responses(
                KafkaOffset::new(0),
                delayed_single(
                    snapshot_delay,
                    change_record(
                        orders_topic,
                        "orders",
                        1,
                        0,
                        SNAPSHOT_WEST_ORDER_ID,
                        "r",
                        None,
                        Some(json!({
                            "id": SNAPSHOT_WEST_ORDER_ID,
                            "region": "west",
                            "status": "open"
                        })),
                        Some("last"),
                        "orders-p1-0",
                    ),
                    1,
                ),
            );

    let customers_p0 =
        DeterministicKafkaPartitionScript::new(KafkaOffset::new(0), KafkaOffset::new(1))
            .with_fetch_responses(
                KafkaOffset::new(0),
                delayed_single(
                    ignored_delay,
                    change_record(
                        customers_topic,
                        "customers",
                        0,
                        0,
                        IGNORED_CUSTOMER_ID,
                        "r",
                        None,
                        Some(json!({
                            "id": IGNORED_CUSTOMER_ID,
                            "segment": "vip"
                        })),
                        Some("true"),
                        "customers-p0-0",
                    ),
                    1,
                ),
            );

    DeterministicKafkaBroker::new([
        (
            terracedb_kafka::KafkaTopicPartition::new(orders_topic, 0),
            orders_p0,
        ),
        (
            terracedb_kafka::KafkaTopicPartition::new(orders_topic, 1),
            orders_p1,
        ),
        (
            terracedb_kafka::KafkaTopicPartition::new(customers_topic, 0),
            customers_p0,
        ),
    ])
}

fn build_claims(
    boundary: &OrderWatchBoundary,
    consumer_group: &str,
) -> (
    KafkaPartitionClaim,
    KafkaPartitionClaim,
    KafkaPartitionClaim,
) {
    let orders_p0 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.orders_layout().topic(),
            0,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id(consumer_group),
        KafkaBootstrapPolicy::Earliest,
        1,
    );
    let orders_p1 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.orders_layout().topic(),
            1,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id(consumer_group),
        KafkaBootstrapPolicy::Earliest,
        1,
    );
    let customers_p0 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.ignored_layout().topic(),
            0,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id(consumer_group),
        KafkaBootstrapPolicy::Earliest,
        1,
    );
    (orders_p0, orders_p1, customers_p0)
}

async fn drive_until_offset<B>(
    db: &Db,
    broker: &B,
    progress_store: &TableKafkaProgressStore,
    claim: &KafkaPartitionClaim,
    target_offset: u64,
    handler: &terracedb_debezium::DebeziumIngressHandler<
        terracedb_debezium::PostgresDebeziumDecoder,
    >,
) -> turmoil::Result<Option<SequenceNumber>>
where
    B: KafkaBroker,
{
    let observer = NoopKafkaRuntimeObserver;
    let telemetry = NoopKafkaTelemetrySink;
    let mut last_sequence = None;
    let worker = terracedb_kafka::KafkaWorkerOptions {
        batch_limit: 1,
        ..terracedb_kafka::KafkaWorkerOptions::default()
    };

    while progress_store
        .load(&claim.source)
        .await?
        .map(|progress| progress.next_offset.get())
        .unwrap_or(0)
        < target_offset
    {
        let outcome = drive_partition_once(
            db,
            broker,
            progress_store,
            claim,
            worker,
            &KeepAllKafkaRecords,
            handler,
            &observer,
            &telemetry,
        )
        .await?;
        if let Some(sequence) = outcome.telemetry.committed_sequence {
            last_sequence = Some(sequence);
        }
    }

    Ok(last_sequence)
}

async fn wait_for_projection<'a, I>(
    handle: &mut ProjectionHandle,
    targets: I,
) -> Result<(), ProjectionError>
where
    I: IntoIterator<Item = (&'a terracedb::Table, SequenceNumber)>,
{
    tokio::time::timeout(Duration::from_secs(1), handle.wait_for_sources(targets))
        .await
        .expect("projection should settle")
}

async fn wait_for_state<P>(
    runtime: &WorkflowRuntime<AlertWorkflowHandler>,
    instance_id: &str,
    predicate: P,
    description: &str,
) -> Result<(), io::Error>
where
    P: Fn(Option<&Value>) -> bool,
{
    tokio::time::timeout(
        Duration::from_secs(5),
        runtime.wait_for_state_where(instance_id, predicate),
    )
    .await
    .map_err(|_| {
        io::Error::other(format!(
            "workflow state for {instance_id} did not satisfy {description} before timeout"
        ))
    })?
    .map_err(io::Error::other)?;
    Ok(())
}

fn decode_state_count_ref(state: Option<&Value>) -> usize {
    match state {
        None => 0,
        Some(Value::Bytes(bytes)) => std::str::from_utf8(bytes)
            .expect("workflow state should be utf-8")
            .parse()
            .expect("workflow state should encode a count"),
        Some(Value::Record(_)) => panic!("order-watch only stores byte workflow state"),
    }
}

async fn wait_for_attach_mode(
    runtime: &WorkflowRuntime<AlertWorkflowHandler>,
    expected: WorkflowSourceAttachMode,
) -> Result<(), WorkflowError> {
    tokio::time::timeout(
        Duration::from_secs(5),
        runtime.wait_for_telemetry(|telemetry| {
            telemetry
                .source_lags
                .iter()
                .any(|lag| lag.attach_mode == Some(expected))
        }),
    )
    .await
    .expect("workflow did not report attach mode before timeout")?;
    Ok(())
}

async fn collect_attention_orders(
    table: &terracedb_example_order_watch::OrderAttentionOrdersTable,
) -> turmoil::Result<BTreeMap<String, OrderAttentionView>> {
    let mut rows = table.scan_all(ScanOptions::default()).await?;
    let mut out = BTreeMap::new();
    while let Some((order_id, row)) = rows.next().await {
        out.insert(order_id, row);
    }
    Ok(out)
}

fn attention_orders_table_named(db: &Db, name: &str) -> OrderAttentionOrdersTable {
    RecordTable::with_codecs(
        db.table(name.to_string()),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
}

fn attention_transitions_table_named(db: &Db, name: &str) -> OrderAttentionTransitionsTable {
    RecordTable::with_codecs(
        db.table(name.to_string()),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
}

async fn collect_attention_transitions(
    table: &terracedb_example_order_watch::OrderAttentionTransitionsTable,
) -> turmoil::Result<Vec<OrderAttentionTransition>> {
    let mut rows = table.scan_all(ScanOptions::default()).await?;
    let mut out = Vec::new();
    while let Some((_key, row)) = rows.next().await {
        out.push(row);
    }
    Ok(out)
}

async fn collect_current_orders(
    table: &terracedb::Table,
) -> turmoil::Result<BTreeMap<String, OrderWatchOrder>> {
    let mut rows = table
        .scan(
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            ScanOptions::default(),
        )
        .await?;
    let mut out = BTreeMap::new();
    while let Some((_key, value)) = rows.next().await {
        let order = OrderWatchOrder::from_current_value(&value)?;
        out.insert(order.order_id.clone(), order);
    }
    Ok(out)
}

async fn collect_outbox_alerts(table: &terracedb::Table) -> turmoil::Result<Vec<OrderWatchAlert>> {
    let mut rows = table
        .scan(
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            ScanOptions::default(),
        )
        .await?;
    let mut out = Vec::new();
    while let Some((outbox_id, value)) = rows.next().await {
        let entry = decode_outbox_entry(outbox_id, &value)?;
        out.push(serde_json::from_slice(&entry.payload)?);
    }
    Ok(out)
}

async fn collect_durable_changes(
    db: &Db,
    table: &terracedb::Table,
) -> turmoil::Result<Vec<ChangeEntry>> {
    let mut stream = db
        .scan_durable_since(table, LogCursor::beginning(), ScanOptions::default())
        .await?;
    let mut out = Vec::new();
    while let Some(entry) = stream.try_next().await? {
        out.push(entry);
    }
    Ok(out)
}

fn state_count_map(
    backlog_count: Option<Value>,
    live_count: Option<Value>,
) -> BTreeMap<String, usize> {
    BTreeMap::from([
        (
            BACKLOG_ALERT_ORDER_ID.to_string(),
            decode_state_count(backlog_count),
        ),
        (
            LIVE_TRANSITION_ORDER_ID.to_string(),
            decode_state_count(live_count),
        ),
    ])
}

fn encode_transition_value(transition: &DebeziumDerivedTransition) -> Result<Value, io::Error> {
    let domain =
        OrderAttentionTransition::from_debezium_transition(transition).map_err(io::Error::other)?;
    serde_json::to_vec(&domain)
        .map(Value::bytes)
        .map_err(io::Error::other)
}

fn run_duplicate_delivery_campaign(seed: u64) -> turmoil::Result<OrderWatchTraceCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_secs(20))
        .run_with(move |context| async move {
            let boundary = OrderWatchBoundary::new();
            let db = context
                .open_db(tiered_test_config(&format!(
                    "/order-watch/duplicate-{seed}"
                )))
                .await?;

            let row_template = row_table_config("template");
            let transitions_config = WorkflowSourceConfig::historical_replayable_source()
                .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
            ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template)
                .await?;
            db.ensure_table(row_table_config(ATTENTION_ORDERS_TABLE_NAME))
                .await?;
            db.ensure_table(transitions_config).await?;
            let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
            let conflict_table = db
                .ensure_table(row_table_config("kafka_conflict_guard"))
                .await?;

            let progress_store = TableKafkaProgressStore::new(progress_table);
            let attention_orders = boundary.attention_orders_table(&db);
            let attention_transitions = boundary.attention_transitions_table(&db);
            let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
            let broker = build_broker(seed, &boundary);
            let observer = RecordingKafkaObserver::default();
            let (orders_p0, orders_p1, customers_p0) =
                build_claims(&boundary, "order-watch-duplicate");

            let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
            let sources = boundary.orders_layout().projection_sources(&db);
            let mut attention_orders_handle = projection_runtime
                .start_multi_source(
                    MultiSourceProjection::new(
                        "order-watch-attention-orders-duplicate",
                        sources.clone(),
                        AttentionOrdersProjection {
                            boundary: boundary.clone(),
                            attention_orders: attention_orders.table().clone(),
                        },
                    )
                    .with_outputs([attention_orders.table().clone()])
                    .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
                )
                .await?;
            let mut attention_transitions_handle = projection_runtime
                .start_multi_source(
                    DebeziumDerivedTransitionProjection::new(
                        attention_transitions.table().clone(),
                        boundary.attention_row_predicate(),
                        encode_transition_value,
                    )
                    .into_multi_source(
                        "order-watch-attention-transitions-duplicate",
                        sources.clone(),
                    ),
                )
                .await?;

            drive_until_offset(&db, &broker, &progress_store, &orders_p0, 1, &handler)
                .await?
                .expect("orders partition 0 should commit the filtered snapshot");

            let retry_handler =
                boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
            let retry_outcome = drive_partition_once_with_retry(
                &db,
                &broker,
                &progress_store,
                &orders_p0,
                terracedb_kafka::KafkaWorkerOptions {
                    batch_limit: 1,
                    ..terracedb_kafka::KafkaWorkerOptions::default()
                },
                &KeepAllKafkaRecords,
                &ConflictOnceThen {
                    inner: retry_handler,
                    conflict_table,
                    triggered: Arc::new(AtomicBool::new(false)),
                },
                &observer,
                &NoopKafkaTelemetrySink,
                2,
            )
            .await?;
            assert_eq!(
                retry_outcome.retained_offsets,
                vec![KafkaOffset::new(1)],
                "the retried batch should cover the backlog west-order create",
            );

            let final_p0 =
                drive_until_offset(&db, &broker, &progress_store, &orders_p0, 4, &handler)
                    .await?
                    .expect("orders partition 0 should commit the remaining live history");
            let final_p1 =
                drive_until_offset(&db, &broker, &progress_store, &orders_p1, 1, &handler)
                    .await?
                    .expect("orders partition 1 should commit the retained snapshot");
            drive_until_offset(&db, &broker, &progress_store, &customers_p0, 1, &handler)
                .await?
                .expect("ignored customers partition should still commit progress");

            wait_for_projection(
                &mut attention_orders_handle,
                [(&sources[0], final_p0), (&sources[1], final_p1)],
            )
            .await?;
            wait_for_projection(
                &mut attention_transitions_handle,
                [(&sources[0], final_p0), (&sources[1], final_p1)],
            )
            .await?;

            let workflow_runtime = WorkflowRuntime::open(
                db.clone(),
                context.clock(),
                WorkflowDefinition::new(
                    WORKFLOW_NAME,
                    [boundary
                        .transition_workflow_source(&db, OrderWatchWorkflowMode::HistoricalReplay)],
                    AlertWorkflowHandler {
                        transitions: attention_transitions.clone(),
                    },
                ),
            )
            .await?;
            let workflow_handle = workflow_runtime.start().await?;
            wait_for_attach_mode(&workflow_runtime, WorkflowSourceAttachMode::Historical).await?;
            wait_for_state(
                &workflow_runtime,
                BACKLOG_ALERT_ORDER_ID,
                |state| decode_state_count_ref(state) == 1,
                "state count == 1",
            )
            .await?;
            wait_for_state(
                &workflow_runtime,
                LIVE_TRANSITION_ORDER_ID,
                |state| decode_state_count_ref(state) == 2,
                "state count == 2",
            )
            .await?;

            let telemetry = workflow_runtime.telemetry_snapshot().await?;
            let snapshot = OrderWatchOracleSnapshot {
                source_offsets: vec![
                    OrderWatchSourceProgress {
                        topic: boundary.orders_layout().topic().to_string(),
                        partition: 0,
                        next_offset: progress_store
                            .load(&orders_p0.source)
                            .await?
                            .expect("orders partition 0 progress should exist")
                            .next_offset
                            .get(),
                    },
                    OrderWatchSourceProgress {
                        topic: boundary.orders_layout().topic().to_string(),
                        partition: 1,
                        next_offset: progress_store
                            .load(&orders_p1.source)
                            .await?
                            .expect("orders partition 1 progress should exist")
                            .next_offset
                            .get(),
                    },
                    OrderWatchSourceProgress {
                        topic: boundary.ignored_layout().topic().to_string(),
                        partition: 0,
                        next_offset: progress_store
                            .load(&customers_p0.source)
                            .await?
                            .expect("customers partition progress should exist")
                            .next_offset
                            .get(),
                    },
                ],
                projection_frontier: attention_transitions_handle.current_frontier(),
                workflow_attach_mode: telemetry
                    .source_lags
                    .first()
                    .and_then(|lag| lag.attach_mode),
                orders_current: collect_current_orders(
                    &db.table(boundary.orders_layout().current_table_name().to_string()),
                )
                .await?,
                attention_orders: collect_attention_orders(&attention_orders).await?,
                attention_transitions: collect_attention_transitions(&attention_transitions)
                    .await?,
                workflow_states: state_count_map(
                    workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
                    workflow_runtime
                        .load_state(LIVE_TRANSITION_ORDER_ID)
                        .await?,
                ),
                outbox_alerts: collect_outbox_alerts(workflow_runtime.tables().outbox_table())
                    .await?,
            };

            attention_orders_handle.shutdown().await?;
            attention_transitions_handle.shutdown().await?;
            workflow_handle.shutdown().await?;

            Ok(OrderWatchTraceCapture {
                snapshot,
                runtime_events: observer.events(),
                broker_trace: broker.trace(),
            })
        })
}

fn run_campaign(
    seed: u64,
    mode: OrderWatchWorkflowMode,
) -> turmoil::Result<OrderWatchOracleSnapshot> {
    run_campaign_with_materialization(seed, mode, DebeziumMaterializationMode::Hybrid)
}

fn run_campaign_with_materialization(
    seed: u64,
    mode: OrderWatchWorkflowMode,
    materialization_mode: DebeziumMaterializationMode,
) -> turmoil::Result<OrderWatchOracleSnapshot> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_secs(20))
        .run_with(move |context| async move {
            let boundary = OrderWatchBoundary::new();
            let db = context
                .open_db(tiered_test_config(&format!(
                    "/order-watch/{seed}-{mode:?}-{materialization_mode:?}"
                )))
                .await?;

            let row_template = row_table_config("template");
            let transitions_config = WorkflowSourceConfig::historical_replayable_source()
                .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
            ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template)
                .await?;
            db.ensure_table(row_table_config(ATTENTION_ORDERS_TABLE_NAME))
                .await?;
            db.ensure_table(transitions_config).await?;
            let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;

            let progress_store = TableKafkaProgressStore::new(progress_table);
            let attention_orders = boundary.attention_orders_table(&db);
            let attention_transitions = boundary.attention_transitions_table(&db);
            let handler = boundary.ingress_handler(&db, materialization_mode)?;
            let broker = build_broker(seed, &boundary);

            let orders_p0 = KafkaPartitionClaim::new(
                KafkaPartitionSource::new(
                    boundary.orders_layout().topic(),
                    0,
                    KafkaBootstrapPolicy::Earliest,
                )
                .source_id("order-watch"),
                KafkaBootstrapPolicy::Earliest,
                1,
            );
            let orders_p1 = KafkaPartitionClaim::new(
                KafkaPartitionSource::new(
                    boundary.orders_layout().topic(),
                    1,
                    KafkaBootstrapPolicy::Earliest,
                )
                .source_id("order-watch"),
                KafkaBootstrapPolicy::Earliest,
                1,
            );
            let customers_p0 = KafkaPartitionClaim::new(
                KafkaPartitionSource::new(
                    boundary.ignored_layout().topic(),
                    0,
                    KafkaBootstrapPolicy::Earliest,
                )
                .source_id("order-watch"),
                KafkaBootstrapPolicy::Earliest,
                1,
            );

            let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
            let sources = boundary.orders_layout().projection_sources(&db);
            let mut attention_orders_handle = projection_runtime
                .start_multi_source(
                    MultiSourceProjection::new(
                        "order-watch-attention-orders",
                        sources.clone(),
                        AttentionOrdersProjection {
                            boundary: boundary.clone(),
                            attention_orders: attention_orders.table().clone(),
                        },
                    )
                    .with_outputs([attention_orders.table().clone()])
                    .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
                )
                .await?;
            let mut attention_transitions_handle = projection_runtime
                .start_multi_source(
                    DebeziumDerivedTransitionProjection::new(
                        attention_transitions.table().clone(),
                        boundary.attention_row_predicate(),
                        encode_transition_value,
                    )
                    .into_multi_source("order-watch-attention-transitions", sources.clone()),
                )
                .await?;

            let (
                workflow_attach_mode,
                workflow_states,
                outbox_alerts,
                workflow_handle,
                workflow_runtime,
            ) = match mode {
                OrderWatchWorkflowMode::HistoricalReplay => {
                    let final_p0 =
                        drive_until_offset(&db, &broker, &progress_store, &orders_p0, 4, &handler)
                            .await?
                            .expect("orders partition 0 should commit full history");
                    let final_p1 =
                        drive_until_offset(&db, &broker, &progress_store, &orders_p1, 1, &handler)
                            .await?
                            .expect("orders partition 1 should commit snapshot");
                    drive_until_offset(&db, &broker, &progress_store, &customers_p0, 1, &handler)
                        .await?
                        .expect("ignored customers partition should still commit progress");

                    wait_for_projection(
                        &mut attention_orders_handle,
                        [(&sources[0], final_p0), (&sources[1], final_p1)],
                    )
                    .await?;
                    wait_for_projection(
                        &mut attention_transitions_handle,
                        [(&sources[0], final_p0), (&sources[1], final_p1)],
                    )
                    .await?;

                    let workflow_runtime = WorkflowRuntime::open(
                        db.clone(),
                        context.clock(),
                        WorkflowDefinition::new(
                            WORKFLOW_NAME,
                            vec![boundary.transition_workflow_source(&db, mode)],
                            AlertWorkflowHandler {
                                transitions: attention_transitions.clone(),
                            },
                        ),
                    )
                    .await?;
                    let workflow_handle = workflow_runtime.start().await?;
                    wait_for_attach_mode(&workflow_runtime, WorkflowSourceAttachMode::Historical)
                        .await?;
                    wait_for_state(
                        &workflow_runtime,
                        BACKLOG_ALERT_ORDER_ID,
                        |state| decode_state_count_ref(state) == 1,
                        "state count == 1",
                    )
                    .await?;
                    wait_for_state(
                        &workflow_runtime,
                        LIVE_TRANSITION_ORDER_ID,
                        |state| decode_state_count_ref(state) == 2,
                        "state count == 2",
                    )
                    .await?;

                    let telemetry = workflow_runtime.telemetry_snapshot().await?;
                    let states = state_count_map(
                        workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
                        workflow_runtime
                            .load_state(LIVE_TRANSITION_ORDER_ID)
                            .await?,
                    );
                    let alerts =
                        collect_outbox_alerts(workflow_runtime.tables().outbox_table()).await?;

                    (
                        telemetry
                            .source_lags
                            .first()
                            .and_then(|lag| lag.attach_mode),
                        states,
                        alerts,
                        Some(workflow_handle),
                        Some(workflow_runtime),
                    )
                }
                OrderWatchWorkflowMode::LiveOnlyAttach => {
                    let backlog_p0 =
                        drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
                            .await?
                            .expect("orders partition 0 should commit backlog");
                    let backlog_p1 =
                        drive_until_offset(&db, &broker, &progress_store, &orders_p1, 1, &handler)
                            .await?
                            .expect("orders partition 1 should commit snapshot");
                    drive_until_offset(&db, &broker, &progress_store, &customers_p0, 1, &handler)
                        .await?
                        .expect("ignored customers partition should still commit progress");

                    wait_for_projection(
                        &mut attention_orders_handle,
                        [(&sources[0], backlog_p0), (&sources[1], backlog_p1)],
                    )
                    .await?;
                    wait_for_projection(
                        &mut attention_transitions_handle,
                        [(&sources[0], backlog_p0), (&sources[1], backlog_p1)],
                    )
                    .await?;

                    let workflow_runtime = WorkflowRuntime::open(
                        db.clone(),
                        context.clock(),
                        WorkflowDefinition::new(
                            WORKFLOW_NAME,
                            vec![boundary.transition_workflow_source(&db, mode)],
                            AlertWorkflowHandler {
                                transitions: attention_transitions.clone(),
                            },
                        ),
                    )
                    .await?;
                    let workflow_handle = workflow_runtime.start().await?;
                    wait_for_attach_mode(&workflow_runtime, WorkflowSourceAttachMode::LiveOnly)
                        .await?;

                    let final_p0 =
                        drive_until_offset(&db, &broker, &progress_store, &orders_p0, 4, &handler)
                            .await?
                            .expect("orders partition 0 should commit live events");
                    wait_for_projection(
                        &mut attention_orders_handle,
                        [(&sources[0], final_p0), (&sources[1], backlog_p1)],
                    )
                    .await?;
                    wait_for_projection(
                        &mut attention_transitions_handle,
                        [(&sources[0], final_p0), (&sources[1], backlog_p1)],
                    )
                    .await?;

                    wait_for_state(
                        &workflow_runtime,
                        LIVE_TRANSITION_ORDER_ID,
                        |state| decode_state_count_ref(state) == 2,
                        "state count == 2",
                    )
                    .await?;

                    let telemetry = workflow_runtime.telemetry_snapshot().await?;
                    let states = state_count_map(
                        workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
                        workflow_runtime
                            .load_state(LIVE_TRANSITION_ORDER_ID)
                            .await?,
                    );
                    let alerts =
                        collect_outbox_alerts(workflow_runtime.tables().outbox_table()).await?;

                    (
                        telemetry
                            .source_lags
                            .first()
                            .and_then(|lag| lag.attach_mode),
                        states,
                        alerts,
                        Some(workflow_handle),
                        Some(workflow_runtime),
                    )
                }
            };

            let snapshot = OrderWatchOracleSnapshot {
                source_offsets: vec![
                    OrderWatchSourceProgress {
                        topic: boundary.orders_layout().topic().to_string(),
                        partition: 0,
                        next_offset: progress_store
                            .load(&orders_p0.source)
                            .await?
                            .expect("orders partition 0 progress should exist")
                            .next_offset
                            .get(),
                    },
                    OrderWatchSourceProgress {
                        topic: boundary.orders_layout().topic().to_string(),
                        partition: 1,
                        next_offset: progress_store
                            .load(&orders_p1.source)
                            .await?
                            .expect("orders partition 1 progress should exist")
                            .next_offset
                            .get(),
                    },
                    OrderWatchSourceProgress {
                        topic: boundary.ignored_layout().topic().to_string(),
                        partition: 0,
                        next_offset: progress_store
                            .load(&customers_p0.source)
                            .await?
                            .expect("customers partition progress should exist")
                            .next_offset
                            .get(),
                    },
                ],
                projection_frontier: attention_transitions_handle.current_frontier(),
                workflow_attach_mode,
                orders_current: if materialization_mode.writes_mirror() {
                    collect_current_orders(
                        &db.table(boundary.orders_layout().current_table_name().to_string()),
                    )
                    .await?
                } else {
                    BTreeMap::new()
                },
                attention_orders: collect_attention_orders(&attention_orders).await?,
                attention_transitions: collect_attention_transitions(&attention_transitions)
                    .await?,
                workflow_states,
                outbox_alerts,
            };

            attention_orders_handle.shutdown().await?;
            attention_transitions_handle.shutdown().await?;
            if let Some(workflow_handle) = workflow_handle {
                workflow_handle.shutdown().await?;
            }
            drop(workflow_runtime);
            Ok(snapshot)
        })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OrderWatchLogicalOutputs {
    workflow_attach_mode: Option<WorkflowSourceAttachMode>,
    attention_orders: BTreeMap<String, OrderAttentionView>,
    attention_transitions: Vec<OrderAttentionTransition>,
    workflow_states: BTreeMap<String, usize>,
    outbox_alerts: Vec<OrderWatchAlert>,
}

impl From<&OrderWatchOracleSnapshot> for OrderWatchLogicalOutputs {
    fn from(snapshot: &OrderWatchOracleSnapshot) -> Self {
        Self {
            workflow_attach_mode: snapshot.workflow_attach_mode,
            attention_orders: snapshot.attention_orders.clone(),
            attention_transitions: snapshot.attention_transitions.clone(),
            workflow_states: snapshot.workflow_states.clone(),
            outbox_alerts: snapshot.outbox_alerts.clone(),
        }
    }
}

type BoxError = Box<dyn Error + Send + Sync>;

fn boxed_error_to_io(error: Box<dyn Error>) -> io::Error {
    io::Error::other(error.to_string())
}

async fn run_restart_resume_campaign(
    seed: u64,
    mode: OrderWatchWorkflowMode,
) -> Result<OrderWatchOracleSnapshot, BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let config = tiered_test_config_with_durability(
        &format!("/order-watch/restart-{mode:?}"),
        TieredDurabilityMode::GroupCommit,
    );
    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    let broker = build_broker(seed, &boundary);

    let open_db = || {
        Db::open(
            config.clone(),
            test_dependencies_with_clock(file_system.clone(), object_store.clone(), clock.clone()),
        )
    };

    let db = open_db().await?;
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(row_table_config(ATTENTION_ORDERS_TABLE_NAME))
        .await?;
    db.ensure_table(transitions_config.clone()).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_orders = boundary.attention_orders_table(&db);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;

    let orders_p0 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.orders_layout().topic(),
            0,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id("order-watch-restart"),
        KafkaBootstrapPolicy::Earliest,
        1,
    );
    let orders_p1 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.orders_layout().topic(),
            1,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id("order-watch-restart"),
        KafkaBootstrapPolicy::Earliest,
        1,
    );
    let customers_p0 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.ignored_layout().topic(),
            0,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id("order-watch-restart"),
        KafkaBootstrapPolicy::Earliest,
        1,
    );

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_orders_handle = projection_runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "order-watch-attention-orders",
                sources.clone(),
                AttentionOrdersProjection {
                    boundary: boundary.clone(),
                    attention_orders: attention_orders.table().clone(),
                },
            )
            .with_outputs([attention_orders.table().clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source("order-watch-attention-transitions", sources.clone()),
        )
        .await?;

    let backlog_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit backlog");
    let backlog_p1 = drive_until_offset(&db, &broker, &progress_store, &orders_p1, 1, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 1 should commit snapshot");
    drive_until_offset(&db, &broker, &progress_store, &customers_p0, 1, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("customers partition should commit snapshot");

    wait_for_projection(
        &mut attention_orders_handle,
        [(&sources[0], backlog_p0), (&sources[1], backlog_p1)],
    )
    .await?;
    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], backlog_p0), (&sources[1], backlog_p1)],
    )
    .await?;

    let workflow_runtime = WorkflowRuntime::open(
        db.clone(),
        clock.clone(),
        WorkflowDefinition::new(
            WORKFLOW_NAME,
            vec![boundary.transition_workflow_source(&db, mode)],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        ),
    )
    .await?;
    let workflow_handle = workflow_runtime.start().await?;
    let expected_attach_mode = match mode {
        OrderWatchWorkflowMode::HistoricalReplay => WorkflowSourceAttachMode::Historical,
        OrderWatchWorkflowMode::LiveOnlyAttach => WorkflowSourceAttachMode::LiveOnly,
    };
    let expected_backlog_count = match mode {
        OrderWatchWorkflowMode::HistoricalReplay => 1,
        OrderWatchWorkflowMode::LiveOnlyAttach => 0,
    };
    wait_for_attach_mode(&workflow_runtime, expected_attach_mode).await?;
    wait_for_state(
        &workflow_runtime,
        BACKLOG_ALERT_ORDER_ID,
        |state| decode_state_count_ref(state) == expected_backlog_count,
        if expected_backlog_count == 0 {
            "state absent or count == 0"
        } else {
            "state count == 1"
        },
    )
    .await?;

    let saved_ingress_offsets = vec![
        OrderWatchSourceProgress {
            topic: boundary.orders_layout().topic().to_string(),
            partition: 0,
            next_offset: progress_store
                .load(&orders_p0.source)
                .await?
                .expect("orders partition 0 progress should exist")
                .next_offset
                .get(),
        },
        OrderWatchSourceProgress {
            topic: boundary.orders_layout().topic().to_string(),
            partition: 1,
            next_offset: progress_store
                .load(&orders_p1.source)
                .await?
                .expect("orders partition 1 progress should exist")
                .next_offset
                .get(),
        },
        OrderWatchSourceProgress {
            topic: boundary.ignored_layout().topic().to_string(),
            partition: 0,
            next_offset: progress_store
                .load(&customers_p0.source)
                .await?
                .expect("customers partition progress should exist")
                .next_offset
                .get(),
        },
    ];
    let saved_attention_orders_frontier = attention_orders_handle.current_frontier();
    let saved_attention_transitions_frontier = attention_transitions_handle.current_frontier();
    let saved_attention_orders = collect_attention_orders(&attention_orders)
        .await
        .map_err(boxed_error_to_io)?;
    let saved_workflow_progress = workflow_runtime
        .load_source_progress(attention_transitions.table())
        .await?;
    let saved_workflow_states = state_count_map(
        workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
        workflow_runtime
            .load_state(LIVE_TRANSITION_ORDER_ID)
            .await?,
    );

    attention_orders_handle.shutdown().await?;
    attention_transitions_handle.shutdown().await?;
    workflow_handle.shutdown().await?;
    db.flush().await?;
    drop(workflow_runtime);
    drop(projection_runtime);
    drop(db);

    let reopened = open_db().await?;
    ensure_layout_tables(
        &reopened,
        boundary.source_layouts(),
        &row_template,
        &row_template,
    )
    .await?;
    reopened
        .ensure_table(row_table_config(ATTENTION_ORDERS_TABLE_NAME))
        .await?;
    reopened.ensure_table(transitions_config).await?;
    let progress_table = reopened
        .ensure_table(row_table_config("kafka_progress"))
        .await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let reopened_attention_orders = boundary.attention_orders_table(&reopened);
    let reopened_attention_transitions = boundary.attention_transitions_table(&reopened);

    let reopened_ingress_offsets = vec![
        OrderWatchSourceProgress {
            topic: boundary.orders_layout().topic().to_string(),
            partition: 0,
            next_offset: progress_store
                .load(&orders_p0.source)
                .await?
                .expect("reopened orders partition 0 progress should exist")
                .next_offset
                .get(),
        },
        OrderWatchSourceProgress {
            topic: boundary.orders_layout().topic().to_string(),
            partition: 1,
            next_offset: progress_store
                .load(&orders_p1.source)
                .await?
                .expect("reopened orders partition 1 progress should exist")
                .next_offset
                .get(),
        },
        OrderWatchSourceProgress {
            topic: boundary.ignored_layout().topic().to_string(),
            partition: 0,
            next_offset: progress_store
                .load(&customers_p0.source)
                .await?
                .expect("reopened customers partition progress should exist")
                .next_offset
                .get(),
        },
    ];
    assert_eq!(
        reopened_ingress_offsets, saved_ingress_offsets,
        "ingress progress should survive restart"
    );

    let reopened_projection_runtime = ProjectionRuntime::open(reopened.clone()).await?;
    let reopened_sources = boundary.orders_layout().projection_sources(&reopened);
    let reopened_attention_orders_frontier = reopened_projection_runtime
        .load_projection_frontier("order-watch-attention-orders", reopened_sources.iter())
        .await?
        .into_iter()
        .map(|(name, state)| (name, state.sequence()))
        .collect::<BTreeMap<_, _>>();
    let reopened_attention_transitions_frontier = reopened_projection_runtime
        .load_projection_frontier("order-watch-attention-transitions", reopened_sources.iter())
        .await?
        .into_iter()
        .map(|(name, state)| (name, state.sequence()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        reopened_attention_orders_frontier, saved_attention_orders_frontier,
        "attention read-model projection frontier should survive restart"
    );
    assert_eq!(
        reopened_attention_transitions_frontier, saved_attention_transitions_frontier,
        "attention transition projection frontier should survive restart"
    );
    assert_eq!(
        collect_attention_orders(&reopened_attention_orders)
            .await
            .map_err(boxed_error_to_io)?,
        saved_attention_orders,
        "projection outputs should be readable immediately after restart"
    );

    let reopened_workflow_runtime = WorkflowRuntime::open(
        reopened.clone(),
        clock.clone(),
        WorkflowDefinition::new(
            WORKFLOW_NAME,
            vec![boundary.transition_workflow_source(&reopened, mode)],
            AlertWorkflowHandler {
                transitions: reopened_attention_transitions.clone(),
            },
        ),
    )
    .await?;
    assert_eq!(
        reopened_workflow_runtime
            .load_source_progress(reopened_attention_transitions.table())
            .await?,
        saved_workflow_progress,
        "workflow source progress should survive restart"
    );
    assert_eq!(
        state_count_map(
            reopened_workflow_runtime
                .load_state(BACKLOG_ALERT_ORDER_ID)
                .await?,
            reopened_workflow_runtime
                .load_state(LIVE_TRANSITION_ORDER_ID)
                .await?,
        ),
        saved_workflow_states,
        "workflow instance state should survive restart"
    );

    let mut reopened_attention_orders_handle = reopened_projection_runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "order-watch-attention-orders",
                reopened_sources.clone(),
                AttentionOrdersProjection {
                    boundary: boundary.clone(),
                    attention_orders: reopened_attention_orders.table().clone(),
                },
            )
            .with_outputs([reopened_attention_orders.table().clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await?;
    let mut reopened_attention_transitions_handle = reopened_projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                reopened_attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions",
                reopened_sources.clone(),
            ),
        )
        .await?;
    let reopened_workflow_handle = reopened_workflow_runtime.start().await?;
    wait_for_attach_mode(&reopened_workflow_runtime, expected_attach_mode).await?;
    wait_for_state(
        &reopened_workflow_runtime,
        BACKLOG_ALERT_ORDER_ID,
        |state| decode_state_count_ref(state) == expected_backlog_count,
        if expected_backlog_count == 0 {
            "state absent or count == 0"
        } else {
            "state count == 1"
        },
    )
    .await?;

    let final_p0 = drive_until_offset(&reopened, &broker, &progress_store, &orders_p0, 4, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit live events after restart");
    wait_for_projection(
        &mut reopened_attention_orders_handle,
        [
            (&reopened_sources[0], final_p0),
            (&reopened_sources[1], backlog_p1),
        ],
    )
    .await?;
    wait_for_projection(
        &mut reopened_attention_transitions_handle,
        [
            (&reopened_sources[0], final_p0),
            (&reopened_sources[1], backlog_p1),
        ],
    )
    .await?;
    wait_for_state(
        &reopened_workflow_runtime,
        LIVE_TRANSITION_ORDER_ID,
        |state| decode_state_count_ref(state) == 2,
        "state count == 2",
    )
    .await?;

    let telemetry = reopened_workflow_runtime.telemetry_snapshot().await?;
    let snapshot = OrderWatchOracleSnapshot {
        source_offsets: vec![
            OrderWatchSourceProgress {
                topic: boundary.orders_layout().topic().to_string(),
                partition: 0,
                next_offset: progress_store
                    .load(&orders_p0.source)
                    .await?
                    .expect("orders partition 0 progress should exist after restart")
                    .next_offset
                    .get(),
            },
            OrderWatchSourceProgress {
                topic: boundary.orders_layout().topic().to_string(),
                partition: 1,
                next_offset: progress_store
                    .load(&orders_p1.source)
                    .await?
                    .expect("orders partition 1 progress should exist after restart")
                    .next_offset
                    .get(),
            },
            OrderWatchSourceProgress {
                topic: boundary.ignored_layout().topic().to_string(),
                partition: 0,
                next_offset: progress_store
                    .load(&customers_p0.source)
                    .await?
                    .expect("customers partition progress should exist after restart")
                    .next_offset
                    .get(),
            },
        ],
        projection_frontier: reopened_attention_transitions_handle.current_frontier(),
        workflow_attach_mode: telemetry
            .source_lags
            .first()
            .and_then(|lag| lag.attach_mode),
        orders_current: collect_current_orders(
            &reopened.table(boundary.orders_layout().current_table_name().to_string()),
        )
        .await
        .map_err(boxed_error_to_io)?,
        attention_orders: collect_attention_orders(&reopened_attention_orders)
            .await
            .map_err(boxed_error_to_io)?,
        attention_transitions: collect_attention_transitions(&reopened_attention_transitions)
            .await
            .map_err(boxed_error_to_io)?,
        workflow_states: state_count_map(
            reopened_workflow_runtime
                .load_state(BACKLOG_ALERT_ORDER_ID)
                .await?,
            reopened_workflow_runtime
                .load_state(LIVE_TRANSITION_ORDER_ID)
                .await?,
        ),
        outbox_alerts: collect_outbox_alerts(reopened_workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?,
    };

    reopened_attention_orders_handle.shutdown().await?;
    reopened_attention_transitions_handle.shutdown().await?;
    reopened_workflow_handle.shutdown().await?;
    drop(reopened_workflow_runtime);
    drop(reopened_projection_runtime);
    drop(reopened);

    Ok(snapshot)
}

#[test]
fn order_watch_harness_smoke_is_deterministic_for_historical_replay() -> turmoil::Result {
    let first = run_campaign(0x96_01, OrderWatchWorkflowMode::HistoricalReplay)?;
    let second = run_campaign(0x96_01, OrderWatchWorkflowMode::HistoricalReplay)?;
    assert_eq!(first, second);
    Ok(())
}

#[test]
fn order_watch_oracle_checks_the_frozen_historical_profile() -> turmoil::Result {
    let snapshot = run_campaign(0x96_02, OrderWatchWorkflowMode::HistoricalReplay)?;
    snapshot
        .validate(OrderWatchWorkflowMode::HistoricalReplay)
        .map_err(io::Error::other)?;
    Ok(())
}

#[test]
fn order_watch_oracle_checks_the_frozen_live_only_profile() -> turmoil::Result {
    let snapshot = run_campaign(0x96_03, OrderWatchWorkflowMode::LiveOnlyAttach)?;
    snapshot
        .validate(OrderWatchWorkflowMode::LiveOnlyAttach)
        .map_err(io::Error::other)?;
    Ok(())
}

#[test]
fn order_watch_event_log_and_hybrid_match_for_historical_replay_outputs() -> turmoil::Result {
    assert_event_log_and_hybrid_match(0x97_01, OrderWatchWorkflowMode::HistoricalReplay)
}

#[test]
fn order_watch_event_log_and_hybrid_match_for_live_only_outputs() -> turmoil::Result {
    assert_event_log_and_hybrid_match(0x97_02, OrderWatchWorkflowMode::LiveOnlyAttach)
}

#[tokio::test]
async fn order_watch_restart_resumes_historical_replay_state() -> Result<(), BoxError> {
    let snapshot =
        run_restart_resume_campaign(0x97_03, OrderWatchWorkflowMode::HistoricalReplay).await?;
    snapshot
        .validate(OrderWatchWorkflowMode::HistoricalReplay)
        .map_err(io::Error::other)?;
    Ok(())
}

#[tokio::test]
async fn order_watch_restart_resumes_live_only_state() -> Result<(), BoxError> {
    let snapshot =
        run_restart_resume_campaign(0x97_04, OrderWatchWorkflowMode::LiveOnlyAttach).await?;
    snapshot
        .validate(OrderWatchWorkflowMode::LiveOnlyAttach)
        .map_err(io::Error::other)?;
    Ok(())
}

#[tokio::test]
async fn mirror_only_keeps_current_rows_but_cannot_replay_attention_history() -> Result<(), BoxError>
{
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/mirror-only-limitations",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(row_table_config(ATTENTION_ORDERS_TABLE_NAME))
        .await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_orders = boundary.attention_orders_table(&db);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Mirror)?;
    let broker = build_broker(0x97_05, &boundary);

    let orders_p0 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.orders_layout().topic(),
            0,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id("order-watch-mirror-only"),
        KafkaBootstrapPolicy::Earliest,
        1,
    );
    let orders_p1 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.orders_layout().topic(),
            1,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id("order-watch-mirror-only"),
        KafkaBootstrapPolicy::Earliest,
        1,
    );
    let customers_p0 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.ignored_layout().topic(),
            0,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id("order-watch-mirror-only"),
        KafkaBootstrapPolicy::Earliest,
        1,
    );

    drive_until_offset(&db, &broker, &progress_store, &orders_p0, 4, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit mirror-only history");
    drive_until_offset(&db, &broker, &progress_store, &orders_p1, 1, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 1 should commit mirror-only snapshot");
    drive_until_offset(&db, &broker, &progress_store, &customers_p0, 1, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("customers partition should commit mirror-only snapshot");

    let current_orders = collect_current_orders(
        &db.table(boundary.orders_layout().current_table_name().to_string()),
    )
    .await
    .map_err(|error| io::Error::other(error.to_string()))?;
    assert!(
        !current_orders.contains_key(FILTERED_OUT_ORDER_ID),
        "row filters should still apply to Mirror current-state",
    );
    assert!(
        !current_orders.contains_key(IGNORED_CUSTOMER_ID),
        "table filters should still exclude non-orders rows from Mirror current-state",
    );
    assert!(
        current_orders.contains_key(SNAPSHOT_WEST_ORDER_ID)
            && current_orders.contains_key(BACKLOG_ALERT_ORDER_ID),
        "Mirror current-state should still retain watched west-region orders",
    );
    assert!(
        !current_orders.contains_key(LIVE_TRANSITION_ORDER_ID),
        "rows leaving the watched subset should still be removed from Mirror current-state",
    );

    let sources = boundary.orders_layout().projection_sources(&db);
    for source in &sources {
        assert_eq!(
            source
                .scan(Vec::new(), vec![0xff], ScanOptions::default())
                .await?
                .count()
                .await,
            0,
            "Mirror-only ingress should leave replayable CDC sources empty",
        );
    }

    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let attention_orders_handle = projection_runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "order-watch-attention-orders",
                sources.clone(),
                AttentionOrdersProjection {
                    boundary: boundary.clone(),
                    attention_orders: attention_orders.table().clone(),
                },
            )
            .with_outputs([attention_orders.table().clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await?;
    let attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source("order-watch-attention-transitions", sources.clone()),
        )
        .await?;

    let workflow_runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            WORKFLOW_NAME,
            vec![
                boundary.transition_workflow_source(&db, OrderWatchWorkflowMode::HistoricalReplay),
            ],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        ),
    )
    .await?;
    let workflow_handle = workflow_runtime.start().await?;
    wait_for_attach_mode(&workflow_runtime, WorkflowSourceAttachMode::Historical).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        collect_attention_orders(&attention_orders)
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "without replayable CDC history the projection-derived attention read model cannot backfill",
    );
    assert!(
        collect_attention_transitions(&attention_transitions)
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "without replayable CDC history the example cannot derive historical attention transitions",
    );
    assert!(
        collect_outbox_alerts(workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "historical replay cannot emit backlog alerts when Mirror-only omits the transition history",
    );

    attention_orders_handle.shutdown().await?;
    attention_transitions_handle.shutdown().await?;
    workflow_handle.shutdown().await?;
    Ok(())
}

fn assert_event_log_and_hybrid_match(seed: u64, mode: OrderWatchWorkflowMode) -> turmoil::Result {
    let hybrid =
        run_campaign_with_materialization(seed, mode, DebeziumMaterializationMode::Hybrid)?;
    let event_log =
        run_campaign_with_materialization(seed, mode, DebeziumMaterializationMode::EventLog)?;

    assert_eq!(
        OrderWatchLogicalOutputs::from(&event_log),
        OrderWatchLogicalOutputs::from(&hybrid),
        "EventLog and Hybrid should produce the same logical attention and alert outputs",
    );
    assert!(
        hybrid.orders_current.contains_key(SNAPSHOT_WEST_ORDER_ID),
        "Hybrid should still expose the current-state mirror for application reads",
    );
    assert!(
        event_log.orders_current.is_empty(),
        "EventLog-only materialization should not expose the current-state mirror",
    );
    Ok(())
}

fn assert_seed_mode_is_deterministic(seed: u64, mode: OrderWatchWorkflowMode) -> turmoil::Result {
    let hybrid =
        run_campaign_with_materialization(seed, mode, DebeziumMaterializationMode::Hybrid)?;
    let hybrid_again =
        run_campaign_with_materialization(seed, mode, DebeziumMaterializationMode::Hybrid)?;
    let event_log =
        run_campaign_with_materialization(seed, mode, DebeziumMaterializationMode::EventLog)?;

    hybrid.validate(mode).map_err(io::Error::other)?;
    assert_eq!(
        hybrid, hybrid_again,
        "seed {seed:#x} mode {mode:?} should be reproducible across repeated runs",
    );
    assert_eq!(
        OrderWatchLogicalOutputs::from(&event_log),
        OrderWatchLogicalOutputs::from(&hybrid),
        "seed {seed:#x} mode {mode:?} should keep the same logical outputs across EventLog and Hybrid",
    );
    Ok(())
}

macro_rules! define_seed_determinism_test {
    ($name:ident, $seed:expr, $mode:expr) => {
        #[test]
        fn $name() -> turmoil::Result {
            assert_seed_mode_is_deterministic($seed, $mode)
        }
    };
}

define_seed_determinism_test!(
    order_watch_seed_9801_historical_replay_stays_deterministic_across_modes,
    0x98_01,
    OrderWatchWorkflowMode::HistoricalReplay
);
define_seed_determinism_test!(
    order_watch_seed_9801_live_only_attach_stays_deterministic_across_modes,
    0x98_01,
    OrderWatchWorkflowMode::LiveOnlyAttach
);
define_seed_determinism_test!(
    order_watch_seed_9802_historical_replay_stays_deterministic_across_modes,
    0x98_02,
    OrderWatchWorkflowMode::HistoricalReplay
);
define_seed_determinism_test!(
    order_watch_seed_9802_live_only_attach_stays_deterministic_across_modes,
    0x98_02,
    OrderWatchWorkflowMode::LiveOnlyAttach
);
define_seed_determinism_test!(
    order_watch_seed_9803_historical_replay_stays_deterministic_across_modes,
    0x98_03,
    OrderWatchWorkflowMode::HistoricalReplay
);
define_seed_determinism_test!(
    order_watch_seed_9803_live_only_attach_stays_deterministic_across_modes,
    0x98_03,
    OrderWatchWorkflowMode::LiveOnlyAttach
);
define_seed_determinism_test!(
    order_watch_seed_9804_historical_replay_stays_deterministic_across_modes,
    0x98_04,
    OrderWatchWorkflowMode::HistoricalReplay
);
define_seed_determinism_test!(
    order_watch_seed_9804_live_only_attach_stays_deterministic_across_modes,
    0x98_04,
    OrderWatchWorkflowMode::LiveOnlyAttach
);

#[test]
fn order_watch_duplicate_delivery_campaign_is_reproducible_and_idempotent() -> turmoil::Result {
    let seed = 0x98_11;
    let baseline = run_campaign_with_materialization(
        seed,
        OrderWatchWorkflowMode::HistoricalReplay,
        DebeziumMaterializationMode::Hybrid,
    )?;
    let first = run_duplicate_delivery_campaign(seed)?;
    let second = run_duplicate_delivery_campaign(seed)?;

    first
        .snapshot
        .validate(OrderWatchWorkflowMode::HistoricalReplay)
        .map_err(io::Error::other)?;
    assert_eq!(
        first, second,
        "duplicate-delivery traces should be reproducible for the same seed",
    );
    assert_eq!(
        first.snapshot.source_offsets, baseline.source_offsets,
        "duplicate Debezium delivery should not perturb persisted source progress",
    );
    assert_eq!(
        first.snapshot.orders_current, baseline.orders_current,
        "duplicate Debezium delivery should keep the mirrored current-state unchanged",
    );
    assert_eq!(
        OrderWatchLogicalOutputs::from(&first.snapshot),
        OrderWatchLogicalOutputs::from(&baseline),
        "duplicate Debezium delivery should converge to the same logical outputs even if commit sequences differ",
    );
    assert!(
        first.runtime_events.iter().any(|event| matches!(
            event,
            KafkaRuntimeEvent::SimulationSeam(
                terracedb_kafka::KafkaSimulationSeam::DuplicateDelivery { offset, .. }
            ) if *offset == KafkaOffset::new(1)
        )),
        "the campaign should exercise the duplicate-delivery seam on the backlog transition batch",
    );
    assert!(
        first
            .broker_trace
            .iter()
            .filter(|event| matches!(
                event,
                DeterministicKafkaBrokerTraceEvent::FetchBatch { requested_offset, .. }
                    if *requested_offset == KafkaOffset::new(1)
            ))
            .count()
            >= 2,
        "the broker trace should preserve enough detail to replay the duplicated fetch locally",
    );
    Ok(())
}

#[tokio::test]
async fn order_watch_broker_restart_reconnect_retries_without_partial_transition_or_alerts()
-> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/broker-restart-reconnect",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(row_table_config(ATTENTION_ORDERS_TABLE_NAME))
        .await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_orders = boundary.attention_orders_table(&db);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = RestartOnceBroker::new(
        build_broker(0x98_17, &boundary),
        boundary.orders_layout().topic(),
        0,
        KafkaOffset::new(1),
        "simulated broker restart during backlog fetch",
    );
    let (orders_p0, _, _) = build_claims(&boundary, "order-watch-broker-reconnect");

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_orders_handle = projection_runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "order-watch-attention-orders-broker-reconnect",
                sources.clone(),
                AttentionOrdersProjection {
                    boundary: boundary.clone(),
                    attention_orders: attention_orders.table().clone(),
                },
            )
            .with_outputs([attention_orders.table().clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-broker-reconnect",
                sources.clone(),
            ),
        )
        .await?;

    let workflow_runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            "order-watch-broker-reconnect",
            [
                WorkflowSource::new(attention_transitions.table().clone()).with_config(
                    boundary.workflow_source_config(OrderWatchWorkflowMode::HistoricalReplay),
                ),
            ],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        ),
    )
    .await?;
    let workflow_handle = workflow_runtime.start().await?;
    wait_for_attach_mode(&workflow_runtime, WorkflowSourceAttachMode::Historical).await?;

    let filtered_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 1, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the filtered snapshot before reconnect");
    let _ = filtered_p0;

    let error = drive_partition_once(
        &db,
        &broker,
        &progress_store,
        &orders_p0,
        terracedb_kafka::KafkaWorkerOptions {
            batch_limit: 1,
            ..terracedb_kafka::KafkaWorkerOptions::default()
        },
        &KeepAllKafkaRecords,
        &handler,
        &NoopKafkaRuntimeObserver,
        &NoopKafkaTelemetrySink,
    )
    .await
    .expect_err("simulated broker restart should bubble up as a broker error");
    match error {
        KafkaRuntimeError::Broker { message, .. } => {
            assert!(
                message.contains("simulated broker restart"),
                "expected reconnect context in broker error, got {message}",
            );
        }
        other => panic!("expected broker error during reconnect test, got {other:?}"),
    }

    assert_eq!(
        progress_store.load(&orders_p0.source).await?,
        Some(KafkaSourceProgress::new(KafkaOffset::new(1))),
        "broker failure should not advance ingress progress past the failed backlog fetch",
    );
    assert!(
        collect_attention_orders(&attention_orders)
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "broker failure should not leak a partial attention read-model update",
    );
    assert!(
        collect_attention_transitions(&attention_transitions)
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "broker failure should not leak a partial transition row",
    );
    assert!(
        collect_outbox_alerts(workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "broker failure should not leak a partial alert before reconnect succeeds",
    );

    let backlog_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("reconnected broker should commit the backlog transition");
    wait_for_projection(&mut attention_orders_handle, [(&sources[0], backlog_p0)]).await?;
    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], backlog_p0)],
    )
    .await?;
    wait_for_state(
        &workflow_runtime,
        BACKLOG_ALERT_ORDER_ID,
        |state| decode_state_count_ref(state) == 1,
        "state count == 1",
    )
    .await?;
    assert_eq!(
        collect_attention_orders(&attention_orders)
            .await
            .map_err(boxed_error_to_io)?
            .len(),
        1,
        "the recovered backlog fetch should publish the attention read-model exactly once",
    );
    assert_eq!(
        collect_attention_transitions(&attention_transitions)
            .await
            .map_err(boxed_error_to_io)?
            .len(),
        1,
        "the recovered backlog fetch should append exactly one transition row",
    );
    assert_eq!(
        collect_outbox_alerts(workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?
            .len(),
        1,
        "the recovered backlog fetch should emit exactly one backlog alert",
    );

    attention_orders_handle.shutdown().await?;
    attention_transitions_handle.shutdown().await?;
    workflow_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn order_watch_historical_and_live_only_workflows_share_one_transition_stream()
-> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/mixed-workflow-modes",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = build_broker(0x98_12, &boundary);
    let (orders_p0, orders_p1, customers_p0) =
        build_claims(&boundary, "order-watch-mixed-workflow-modes");

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-mixed-workflow-modes",
                sources.clone(),
            ),
        )
        .await?;

    let backlog_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the backlog transition");
    let backlog_p1 = drive_until_offset(&db, &broker, &progress_store, &orders_p1, 1, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 1 should commit the retained snapshot");
    drive_until_offset(&db, &broker, &progress_store, &customers_p0, 1, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("ignored customers partition should still commit progress");

    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], backlog_p0), (&sources[1], backlog_p1)],
    )
    .await?;

    let historical_runtime = WorkflowRuntime::open(
        db.clone(),
        clock.clone(),
        WorkflowDefinition::new(
            "order-watch-historical-mixed",
            [boundary.transition_workflow_source(&db, OrderWatchWorkflowMode::HistoricalReplay)],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        ),
    )
    .await?;
    let historical_handle = historical_runtime.start().await?;

    let live_only_runtime = WorkflowRuntime::open(
        db.clone(),
        clock.clone(),
        WorkflowDefinition::new(
            "order-watch-live-only-mixed",
            [boundary.transition_workflow_source(&db, OrderWatchWorkflowMode::LiveOnlyAttach)],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        ),
    )
    .await?;
    let live_only_handle = live_only_runtime.start().await?;

    wait_for_attach_mode(&historical_runtime, WorkflowSourceAttachMode::Historical).await?;
    wait_for_attach_mode(&live_only_runtime, WorkflowSourceAttachMode::LiveOnly).await?;
    wait_for_state(
        &historical_runtime,
        BACKLOG_ALERT_ORDER_ID,
        |state| decode_state_count_ref(state) == 1,
        "state count == 1",
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        live_only_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
        None,
        "live-only attach should intentionally skip the backlog transition",
    );

    let final_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 4, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the live enter/exit transitions");
    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], final_p0), (&sources[1], backlog_p1)],
    )
    .await?;

    wait_for_state(
        &historical_runtime,
        LIVE_TRANSITION_ORDER_ID,
        |state| decode_state_count_ref(state) == 2,
        "state count == 2",
    )
    .await?;
    wait_for_state(
        &live_only_runtime,
        LIVE_TRANSITION_ORDER_ID,
        |state| decode_state_count_ref(state) == 2,
        "state count == 2",
    )
    .await?;

    let historical_alerts = collect_outbox_alerts(historical_runtime.tables().outbox_table())
        .await
        .map_err(boxed_error_to_io)?;
    let live_only_alerts = collect_outbox_alerts(live_only_runtime.tables().outbox_table())
        .await
        .map_err(boxed_error_to_io)?;
    assert_eq!(
        historical_alerts.len(),
        3,
        "historical replay should emit backlog plus both live alerts",
    );
    assert_eq!(
        live_only_alerts.len(),
        2,
        "live-only attach should emit only the live enter/exit alerts",
    );
    assert!(
        historical_alerts
            .iter()
            .any(|alert| alert.order_id == BACKLOG_ALERT_ORDER_ID),
        "historical replay should preserve the backlog alert",
    );
    assert!(
        !live_only_alerts
            .iter()
            .any(|alert| alert.order_id == BACKLOG_ALERT_ORDER_ID),
        "live-only attach should not synthesize backlog alerts after the fact",
    );

    attention_transitions_handle.shutdown().await?;
    historical_handle.shutdown().await?;
    live_only_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn order_watch_projection_rebuild_after_runtime_reopen_matches_primary_outputs()
-> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/projection-rebuild",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(row_table_config(ATTENTION_ORDERS_TABLE_NAME))
        .await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_orders = boundary.attention_orders_table(&db);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = build_broker(0x98_18, &boundary);
    let (orders_p0, orders_p1, _) = build_claims(&boundary, "order-watch-projection-rebuild");

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut primary_attention_orders_handle = projection_runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "order-watch-attention-orders-projection-rebuild-primary",
                sources.clone(),
                AttentionOrdersProjection {
                    boundary: boundary.clone(),
                    attention_orders: attention_orders.table().clone(),
                },
            )
            .with_outputs([attention_orders.table().clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await?;
    let mut primary_attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-projection-rebuild-primary",
                sources.clone(),
            ),
        )
        .await?;

    let final_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 4, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the full retained history");
    let final_p1 = drive_until_offset(&db, &broker, &progress_store, &orders_p1, 1, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 1 should commit the retained snapshot");

    wait_for_projection(
        &mut primary_attention_orders_handle,
        [(&sources[0], final_p0), (&sources[1], final_p1)],
    )
    .await?;
    wait_for_projection(
        &mut primary_attention_transitions_handle,
        [(&sources[0], final_p0), (&sources[1], final_p1)],
    )
    .await?;

    let expected_attention_orders = collect_attention_orders(&attention_orders)
        .await
        .map_err(boxed_error_to_io)?;
    let expected_attention_transitions = collect_attention_transitions(&attention_transitions)
        .await
        .map_err(boxed_error_to_io)?;
    let expected_orders_frontier = primary_attention_orders_handle.current_frontier();
    let expected_transitions_frontier = primary_attention_transitions_handle.current_frontier();

    primary_attention_orders_handle.shutdown().await?;
    primary_attention_transitions_handle.shutdown().await?;
    drop(projection_runtime);

    let rebuild_orders_name = "attention_orders_rebuild";
    let rebuild_transitions_name = "attention_transitions_rebuild";
    let rebuild_transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(rebuild_transitions_name));
    db.ensure_table(row_table_config(rebuild_orders_name))
        .await?;
    db.ensure_table(rebuild_transitions_config).await?;
    let rebuild_attention_orders = attention_orders_table_named(&db, rebuild_orders_name);
    let rebuild_attention_transitions =
        attention_transitions_table_named(&db, rebuild_transitions_name);

    let reopened_projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut rebuilt_attention_orders_handle = reopened_projection_runtime
        .start_multi_source(
            MultiSourceProjection::new(
                "order-watch-attention-orders-projection-rebuild-reopened",
                sources.clone(),
                AttentionOrdersProjection {
                    boundary: boundary.clone(),
                    attention_orders: rebuild_attention_orders.table().clone(),
                },
            )
            .with_outputs([rebuild_attention_orders.table().clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await?;
    let mut rebuilt_attention_transitions_handle = reopened_projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                rebuild_attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-projection-rebuild-reopened",
                sources.clone(),
            ),
        )
        .await?;

    wait_for_projection(
        &mut rebuilt_attention_orders_handle,
        [(&sources[0], final_p0), (&sources[1], final_p1)],
    )
    .await?;
    wait_for_projection(
        &mut rebuilt_attention_transitions_handle,
        [(&sources[0], final_p0), (&sources[1], final_p1)],
    )
    .await?;

    assert_eq!(
        collect_attention_orders(&rebuild_attention_orders)
            .await
            .map_err(boxed_error_to_io)?,
        expected_attention_orders,
        "reopened projection runtime should rebuild the attention read-model from retained CDC history",
    );
    assert_eq!(
        collect_attention_transitions(&rebuild_attention_transitions)
            .await
            .map_err(boxed_error_to_io)?,
        expected_attention_transitions,
        "reopened projection runtime should rebuild the transition stream from retained CDC history",
    );
    assert_eq!(
        rebuilt_attention_orders_handle.current_frontier(),
        expected_orders_frontier,
        "rebuilt attention read-model should converge to the same source frontier",
    );
    assert_eq!(
        rebuilt_attention_transitions_handle.current_frontier(),
        expected_transitions_frontier,
        "rebuilt transition stream should converge to the same source frontier",
    );

    rebuilt_attention_orders_handle.shutdown().await?;
    rebuilt_attention_transitions_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn order_watch_projection_failpoint_retries_without_partial_alerts() -> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/projection-failpoint",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = build_broker(0x98_13, &boundary);
    let (orders_p0, _, _) = build_claims(&boundary, "order-watch-projection-failpoint");

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-projection-failpoint",
                sources.clone(),
            ),
        )
        .await?;

    let workflow_runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            "order-watch-projection-failpoint",
            [
                WorkflowSource::new(attention_transitions.table().clone()).with_config(
                    boundary.workflow_source_config(OrderWatchWorkflowMode::HistoricalReplay),
                ),
            ],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        ),
    )
    .await?;
    let workflow_handle = workflow_runtime.start().await?;
    wait_for_attach_mode(&workflow_runtime, WorkflowSourceAttachMode::Historical).await?;

    db_failpoint_registry(&db).arm_error(
        terracedb_projections::failpoints::names::PROJECTION_APPLY_BEFORE_COMMIT,
        terracedb::StorageError::io("simulated projection failpoint"),
        FailpointMode::Once,
    );

    let backlog_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the backlog transition");

    tokio::time::timeout(
        Duration::from_secs(1),
        attention_transitions_handle.wait_until_terminal(),
    )
    .await
    .expect("projection failure should be observed")
    .expect("projection should terminate after the injected failpoint");

    let error = attention_transitions_handle
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
                "expected injected projection failpoint context, got {storage}",
            );
        }
        other => panic!("expected projection storage error, got {other:?}"),
    }

    assert!(
        collect_attention_transitions(&attention_transitions)
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "projection failure should not leak a partial transition row",
    );
    assert!(
        collect_outbox_alerts(workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "workflow outputs should remain empty while the transition projection is rolled back",
    );

    let retry_sources = boundary.orders_layout().projection_sources(&db);
    let mut retry_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-projection-failpoint",
                retry_sources.clone(),
            ),
        )
        .await?;
    wait_for_projection(&mut retry_handle, [(&retry_sources[0], backlog_p0)]).await?;
    wait_for_state(
        &workflow_runtime,
        BACKLOG_ALERT_ORDER_ID,
        |state| decode_state_count_ref(state) == 1,
        "state count == 1",
    )
    .await?;
    assert_eq!(
        collect_attention_transitions(&attention_transitions)
            .await
            .map_err(boxed_error_to_io)?
            .len(),
        1,
        "retrying the projection should produce exactly one backlog transition",
    );
    assert_eq!(
        collect_outbox_alerts(workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?
            .len(),
        1,
        "retrying the projection should emit exactly one backlog alert",
    );

    retry_handle.shutdown().await?;
    workflow_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn order_watch_workflow_failpoint_retries_without_duplicate_alerts() -> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/workflow-failpoint",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = build_broker(0x98_14, &boundary);
    let (orders_p0, _, _) = build_claims(&boundary, "order-watch-workflow-failpoint");

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-workflow-failpoint",
                sources.clone(),
            ),
        )
        .await?;

    let workflow_name = "order-watch-workflow-failpoint";
    let workflow_definition = || {
        WorkflowDefinition::new(
            workflow_name,
            [
                WorkflowSource::new(attention_transitions.table().clone()).with_config(
                    boundary.workflow_source_config(OrderWatchWorkflowMode::HistoricalReplay),
                ),
            ],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        )
    };

    let workflow_runtime =
        WorkflowRuntime::open(db.clone(), clock.clone(), workflow_definition()).await?;
    let mut workflow_handle = workflow_runtime.start().await?;
    wait_for_attach_mode(&workflow_runtime, WorkflowSourceAttachMode::Historical).await?;

    db_failpoint_registry(&db).arm_error(
        workflow_failpoints::names::WORKFLOW_EXECUTION_BEFORE_COMMIT,
        terracedb::StorageError::io("simulated workflow failpoint"),
        FailpointMode::Once,
    );

    let backlog_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the backlog transition");
    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], backlog_p0)],
    )
    .await?;

    tokio::time::timeout(
        Duration::from_secs(1),
        workflow_handle.wait_until_terminal(),
    )
    .await
    .expect("workflow failure should be observed")
    .expect("workflow should terminate after the injected failpoint");
    let error = workflow_handle
        .shutdown()
        .await
        .expect_err("workflow should fail at the named cut point");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Io);
            assert!(
                storage.to_string().contains("simulated workflow failpoint"),
                "expected injected workflow failpoint context, got {storage}",
            );
        }
        other => panic!("expected workflow storage error, got {other:?}"),
    }

    assert_eq!(
        workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
        None,
        "workflow failure before commit should not persist partial state",
    );
    assert!(
        collect_outbox_alerts(workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?
            .is_empty(),
        "workflow failure before commit should not leak partial outbox entries",
    );
    drop(workflow_runtime);

    let retry_runtime = WorkflowRuntime::open(db.clone(), clock, workflow_definition()).await?;
    let retry_handle = retry_runtime.start().await?;
    wait_for_attach_mode(&retry_runtime, WorkflowSourceAttachMode::Historical).await?;
    wait_for_state(
        &retry_runtime,
        BACKLOG_ALERT_ORDER_ID,
        |state| decode_state_count_ref(state) == 1,
        "state count == 1",
    )
    .await?;
    assert_eq!(
        collect_outbox_alerts(retry_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?
            .len(),
        1,
        "retrying the workflow should emit exactly one backlog alert",
    );

    attention_transitions_handle.shutdown().await?;
    retry_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn order_watch_history_loss_fails_closed_without_fast_forwarding() -> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/snapshot-too-old",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let mut transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    transitions_config.history_retention_sequences = Some(1);
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = build_broker(0x98_15, &boundary);
    let (orders_p0, _, _) = build_claims(&boundary, "order-watch-snapshot-too-old");

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-snapshot-too-old",
                sources.clone(),
            ),
        )
        .await?;

    let backlog_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the backlog transition");
    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], backlog_p0)],
    )
    .await?;
    let first_transition = collect_durable_changes(&db, attention_transitions.table())
        .await
        .map_err(boxed_error_to_io)?
        .into_iter()
        .next()
        .expect("first transition should be readable before history loss");

    let final_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 4, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit enough history to evict the first transition");
    wait_for_projection(&mut attention_transitions_handle, [(&sources[0], final_p0)]).await?;
    db.flush().await?;

    let runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            "order-watch-snapshot-too-old",
            [
                WorkflowSource::new(attention_transitions.table().clone()).with_config(
                    boundary
                        .workflow_source_config(OrderWatchWorkflowMode::HistoricalReplay)
                        .with_recovery_policy(WorkflowSourceRecoveryPolicy::FailClosed),
                ),
            ],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        ),
    )
    .await?;

    let stale_progress = WorkflowSourceProgress::from_cursor(first_transition.cursor);
    runtime
        .tables()
        .source_progress_table()
        .write(
            attention_transitions.table().name().as_bytes().to_vec(),
            Value::bytes(stale_progress.encode()?),
        )
        .await?;
    db.flush().await?;

    let mut handle = runtime.start().await?;
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("workflow failure should be observed")
        .expect("workflow should terminate after fail-closed recovery");
    let error = tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("workflow shutdown should return promptly")
        .expect_err("workflow runtime should fail closed on stale source progress");
    let snapshot_too_old = error
        .snapshot_too_old()
        .expect("workflow error should preserve SnapshotTooOld");
    assert_eq!(
        snapshot_too_old.requested,
        first_transition.cursor.sequence()
    );
    assert!(
        snapshot_too_old.oldest_available > first_transition.cursor.sequence(),
        "history loss should advance the oldest available transition beyond the stale cursor",
    );
    assert_eq!(
        runtime
            .load_source_progress(attention_transitions.table())
            .await?,
        stale_progress,
        "fail-closed recovery must not silently fast-forward the workflow source progress",
    );

    attention_transitions_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn order_watch_checkpoint_restore_recovers_saved_state_and_outbox() -> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let checkpoint_store = Arc::new(WorkflowObjectStoreCheckpointStore::new(
        object_store.clone(),
        "order-watch-checkpoints",
    ));
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/checkpoint-restore",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = build_broker(0x98_19, &boundary);
    let (orders_p0, _, _) = build_claims(&boundary, "order-watch-checkpoint-restore");

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-checkpoint-restore",
                sources.clone(),
            ),
        )
        .await?;

    let backlog_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the backlog transition");
    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], backlog_p0)],
    )
    .await?;

    let workflow_name = "order-watch-checkpoint-restore";
    let definition = || {
        WorkflowDefinition::new(
            workflow_name,
            [
                WorkflowSource::new(attention_transitions.table().clone()).with_config(
                    boundary.workflow_source_config(OrderWatchWorkflowMode::HistoricalReplay),
                ),
            ],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        )
        .with_checkpoint_store(checkpoint_store.clone())
    };

    let workflow_runtime = WorkflowRuntime::open(db.clone(), clock.clone(), definition()).await?;
    let workflow_handle = workflow_runtime.start().await?;
    wait_for_state(
        &workflow_runtime,
        BACKLOG_ALERT_ORDER_ID,
        |state| decode_state_count_ref(state) == 1,
        "state count == 1",
    )
    .await?;
    workflow_handle.shutdown().await?;

    let saved_state = workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?;
    let saved_state_record = workflow_runtime
        .load_state_record(BACKLOG_ALERT_ORDER_ID)
        .await?
        .expect("saved workflow state record should exist");
    let saved_progress = workflow_runtime
        .load_source_progress(attention_transitions.table())
        .await?;
    let saved_alerts = collect_outbox_alerts(workflow_runtime.tables().outbox_table())
        .await
        .map_err(boxed_error_to_io)?;
    workflow_runtime
        .capture_checkpoint(WorkflowCheckpointId::new(8))
        .await?;

    workflow_runtime
        .tables()
        .state_table()
        .write(
            BACKLOG_ALERT_ORDER_ID.as_bytes().to_vec(),
            encode_versioned_workflow_contract(&WorkflowStateRecord {
                state: Some(WorkflowPayload::bytes("mutated")),
                ..saved_state_record
            }),
        )
        .await?;
    workflow_runtime
        .tables()
        .outbox_table()
        .delete(format!("alert:{BACKLOG_ALERT_ORDER_ID}:1").into_bytes())
        .await?;

    assert_ne!(
        workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
        saved_state,
        "the local workflow state should be mutated before restore",
    );
    assert_ne!(
        collect_outbox_alerts(workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?,
        saved_alerts,
        "the local outbox should be mutated before restore",
    );
    drop(workflow_runtime);

    let restored_runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        definition().with_restore_latest_checkpoint_on_open(),
    )
    .await?;
    assert_eq!(
        restored_runtime
            .load_source_progress(attention_transitions.table())
            .await?,
        saved_progress,
        "checkpoint restore should recover the saved workflow source progress",
    );
    assert_eq!(
        restored_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
        saved_state,
        "checkpoint restore should recover the saved workflow state",
    );
    assert_eq!(
        collect_outbox_alerts(restored_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?,
        saved_alerts,
        "checkpoint restore should recover the saved workflow outbox entries",
    );

    attention_transitions_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn order_watch_corrupt_checkpoint_pointer_fails_closed_without_mutating_state()
-> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::new();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let checkpoint_store = Arc::new(WorkflowObjectStoreCheckpointStore::new(
        object_store.clone(),
        "order-watch-checkpoints",
    ));
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/checkpoint-corruption",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store.clone(), clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = build_broker(0x98_16, &boundary);
    let (orders_p0, _, _) = build_claims(&boundary, "order-watch-checkpoint-corruption");

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-checkpoint-corruption",
                sources.clone(),
            ),
        )
        .await?;

    let backlog_p0 = drive_until_offset(&db, &broker, &progress_store, &orders_p0, 2, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("orders partition 0 should commit the backlog transition");
    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], backlog_p0)],
    )
    .await?;

    let workflow_name = "order-watch-checkpoint-corruption";
    let definition = || {
        WorkflowDefinition::new(
            workflow_name,
            [
                WorkflowSource::new(attention_transitions.table().clone()).with_config(
                    boundary.workflow_source_config(OrderWatchWorkflowMode::HistoricalReplay),
                ),
            ],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        )
        .with_checkpoint_store(checkpoint_store.clone())
    };

    let workflow_runtime = WorkflowRuntime::open(db.clone(), clock.clone(), definition()).await?;
    let workflow_handle = workflow_runtime.start().await?;
    wait_for_state(
        &workflow_runtime,
        BACKLOG_ALERT_ORDER_ID,
        |state| decode_state_count_ref(state) == 1,
        "state count == 1",
    )
    .await?;
    workflow_handle.shutdown().await?;

    let saved_state = workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?;
    let saved_alerts = collect_outbox_alerts(workflow_runtime.tables().outbox_table())
        .await
        .map_err(boxed_error_to_io)?;
    workflow_runtime
        .capture_checkpoint(WorkflowCheckpointId::new(7))
        .await?;

    object_store
        .put(
            &checkpoint_store.latest_manifest_key(workflow_name),
            b"not-json",
        )
        .await?;

    let error = match WorkflowRuntime::open(
        db.clone(),
        clock,
        definition().with_restore_latest_checkpoint_on_open(),
    )
    .await
    {
        Ok(_) => panic!("corrupt checkpoint pointer should abort runtime open"),
        Err(error) => error,
    };
    match error {
        WorkflowError::CheckpointStore { source, .. } => {
            let message = source.to_string();
            assert!(
                message.contains("workflow checkpoint latest pointer") || message.contains("json"),
                "expected checkpoint pointer corruption context, got {message}",
            );
        }
        other => panic!("unexpected checkpoint restore failure: {other:?}"),
    }

    assert_eq!(
        workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
        saved_state,
        "failed checkpoint restore should leave existing workflow state untouched",
    );
    assert_eq!(
        collect_outbox_alerts(workflow_runtime.tables().outbox_table())
            .await
            .map_err(boxed_error_to_io)?,
        saved_alerts,
        "failed checkpoint restore should leave existing outbox alerts untouched",
    );

    attention_transitions_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn order_watch_latest_bootstrap_skips_backlog_but_ingests_later_live_records()
-> Result<(), BoxError> {
    let boundary = OrderWatchBoundary::with_partitions([0_u32], Vec::<u32>::new());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/order-watch/latest-bootstrap-phased-broker",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await?;

    let row_template = row_table_config("template");
    let transitions_config = WorkflowSourceConfig::historical_replayable_source()
        .prepare_source_table_config(row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME));
    ensure_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    db.ensure_table(transitions_config).await?;
    let progress_table = db.ensure_table(row_table_config("kafka_progress")).await?;
    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;

    let broker = PhasedLatestBroker::new([(
        KafkaTopicPartition::new(boundary.orders_layout().topic(), 0),
        PhasedLatestPartition::new(
            KafkaOffset::new(0),
            KafkaOffset::new(1),
            KafkaOffset::new(1),
            [change_record(
                boundary.orders_layout().topic(),
                "orders",
                0,
                0,
                BACKLOG_ALERT_ORDER_ID,
                "c",
                None,
                Some(json!({
                    "id": BACKLOG_ALERT_ORDER_ID,
                    "region": "west",
                    "status": "open"
                })),
                None,
                "latest-backlog",
            )],
        ),
    )]);
    let observer = RecordingKafkaObserver::default();
    let claim = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.orders_layout().topic(),
            0,
            KafkaBootstrapPolicy::Latest,
        )
        .source_id("order-watch-latest-bootstrap"),
        KafkaBootstrapPolicy::Latest,
        1,
    );

    let sources = boundary.orders_layout().projection_sources(&db);
    let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
    let mut attention_transitions_handle = projection_runtime
        .start_multi_source(
            DebeziumDerivedTransitionProjection::new(
                attention_transitions.table().clone(),
                boundary.attention_row_predicate(),
                encode_transition_value,
            )
            .into_multi_source(
                "order-watch-attention-transitions-latest-bootstrap",
                sources.clone(),
            ),
        )
        .await?;

    let workflow_runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            "order-watch-latest-bootstrap",
            [
                WorkflowSource::new(attention_transitions.table().clone()).with_config(
                    boundary.workflow_source_config(OrderWatchWorkflowMode::HistoricalReplay),
                ),
            ],
            AlertWorkflowHandler {
                transitions: attention_transitions.clone(),
            },
        ),
    )
    .await?;
    let workflow_handle = workflow_runtime.start().await?;
    wait_for_attach_mode(&workflow_runtime, WorkflowSourceAttachMode::Historical).await?;

    let bootstrap_step = drive_partition_once(
        &db,
        &broker,
        &progress_store,
        &claim,
        terracedb_kafka::KafkaWorkerOptions {
            batch_limit: 1,
            ..terracedb_kafka::KafkaWorkerOptions::default()
        },
        &KeepAllKafkaRecords,
        &handler,
        &observer,
        &NoopKafkaTelemetrySink,
    )
    .await
    .map_err(|error| io::Error::other(error.to_string()))?;
    assert_eq!(
        bootstrap_step.telemetry.start_position.next_offset(),
        KafkaOffset::new(1),
        "latest bootstrap should snapshot the backlog frontier before later live publishes",
    );
    assert_eq!(
        progress_store.load(&claim.source).await?,
        None,
        "an idle latest bootstrap should not persist progress until live data actually arrives",
    );

    broker.publish(
        boundary.orders_layout().topic(),
        0,
        KafkaOffset::new(3),
        [
            change_record(
                boundary.orders_layout().topic(),
                "orders",
                0,
                1,
                LIVE_TRANSITION_ORDER_ID,
                "c",
                None,
                Some(json!({
                    "id": LIVE_TRANSITION_ORDER_ID,
                    "region": "west",
                    "status": "open"
                })),
                None,
                "latest-live-enter",
            ),
            change_record(
                boundary.orders_layout().topic(),
                "orders",
                0,
                2,
                LIVE_TRANSITION_ORDER_ID,
                "u",
                Some(json!({
                    "id": LIVE_TRANSITION_ORDER_ID,
                    "region": "west",
                    "status": "open"
                })),
                Some(json!({
                    "id": LIVE_TRANSITION_ORDER_ID,
                    "region": "east",
                    "status": "closed"
                })),
                None,
                "latest-live-exit",
            ),
        ],
    );

    let final_sequence = drive_until_offset(&db, &broker, &progress_store, &claim, 3, &handler)
        .await
        .map_err(boxed_error_to_io)?
        .expect("latest-bootstrap claim should ingest the later live records");
    wait_for_projection(
        &mut attention_transitions_handle,
        [(&sources[0], final_sequence)],
    )
    .await?;
    wait_for_state(
        &workflow_runtime,
        LIVE_TRANSITION_ORDER_ID,
        |state| decode_state_count_ref(state) == 2,
        "state count == 2",
    )
    .await?;

    assert_eq!(
        workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
        None,
        "historical workflow replay cannot recover backlog that Kafka latest bootstrap never materialized",
    );
    let alerts = collect_outbox_alerts(workflow_runtime.tables().outbox_table())
        .await
        .map_err(boxed_error_to_io)?;
    assert_eq!(alerts.len(), 2);
    assert!(
        alerts
            .iter()
            .all(|alert| alert.order_id == LIVE_TRANSITION_ORDER_ID),
        "only the later live order should appear after latest bootstrap skipped the backlog",
    );
    assert_eq!(
        progress_store.load(&claim.source).await?,
        Some(terracedb_kafka::KafkaSourceProgress::new(KafkaOffset::new(
            3
        ))),
        "source progress should advance from the snapshotted latest offset through the later live records",
    );
    assert!(
        observer.events().iter().any(|event| matches!(
            event,
            KafkaRuntimeEvent::BootstrapResolved { start_position, .. }
                if start_position.next_offset() == KafkaOffset::new(1)
        )),
        "the runtime trace should preserve the snapshotted latest bootstrap offset",
    );

    attention_transitions_handle.shutdown().await?;
    workflow_handle.shutdown().await?;
    Ok(())
}
