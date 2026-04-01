use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Value as JsonValue, json};
use terracedb::{
    ChangeEntry, Db, ScanOptions, SequenceNumber, StubClock, StubFileSystem, StubObjectStore,
    TieredDurabilityMode, Value, decode_outbox_entry,
    test_support::{
        row_table_config, test_dependencies_with_clock, tiered_test_config,
        tiered_test_config_with_durability,
    },
};
use terracedb_debezium::{
    DebeziumChangeEntry, DebeziumDerivedTransition, DebeziumDerivedTransitionProjection,
    DebeziumMaterializationMode, ensure_layout_tables,
};
use terracedb_example_order_watch::{
    ATTENTION_ORDERS_TABLE_NAME, ATTENTION_TRANSITIONS_TABLE_NAME, BACKLOG_ALERT_ORDER_ID,
    FILTERED_OUT_ORDER_ID, IGNORED_CUSTOMER_ID, LIVE_TRANSITION_ORDER_ID, OrderAttentionTransition,
    OrderAttentionView, OrderWatchAlert, OrderWatchBoundary, OrderWatchOracleSnapshot,
    OrderWatchOrder, OrderWatchSourceProgress, OrderWatchWorkflowMode, SNAPSHOT_WEST_ORDER_ID,
};
use terracedb_kafka::{
    DeterministicKafkaBroker, DeterministicKafkaFetchResponse, DeterministicKafkaPartitionScript,
    KafkaBootstrapPolicy, KafkaFetchedBatch, KafkaOffset, KafkaPartitionClaim,
    KafkaPartitionSource, KafkaProgressStore, KeepAllKafkaRecords, NoopKafkaRuntimeObserver,
    NoopKafkaTelemetrySink, TableKafkaProgressStore, drive_partition_once,
};
use terracedb_projections::{
    MultiSourceProjection, MultiSourceProjectionHandler, ProjectionContext, ProjectionError,
    ProjectionHandle, ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun,
    ProjectionTransaction, RecomputeStrategy,
};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_workflows::{
    WorkflowContext, WorkflowDefinition, WorkflowError, WorkflowHandler, WorkflowHandlerError,
    WorkflowOutput, WorkflowRuntime, WorkflowSourceAttachMode, WorkflowSourceConfig,
    WorkflowStateMutation, WorkflowTrigger,
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

async fn drive_until_offset(
    db: &Db,
    broker: &DeterministicKafkaBroker,
    progress_store: &TableKafkaProgressStore,
    claim: &KafkaPartitionClaim,
    target_offset: u64,
    handler: &terracedb_debezium::DebeziumIngressHandler<
        terracedb_debezium::PostgresDebeziumDecoder,
    >,
) -> turmoil::Result<Option<SequenceNumber>> {
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
fn order_watch_event_log_and_hybrid_match_for_logical_outputs() -> turmoil::Result {
    for (seed, mode) in [
        (0x97_01, OrderWatchWorkflowMode::HistoricalReplay),
        (0x97_02, OrderWatchWorkflowMode::LiveOnlyAttach),
    ] {
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
    }
    Ok(())
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
    .map_err(boxed_error_to_io)?;
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
