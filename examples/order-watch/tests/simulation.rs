use std::collections::BTreeMap;
use std::io;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Value as JsonValue, json};
use terracedb::{
    ChangeEntry, Db, ScanOptions, SequenceNumber, Value, decode_outbox_entry,
    test_support::{row_table_config, tiered_test_config},
};
use terracedb_debezium::{
    DebeziumChangeEntry, DebeziumDerivedTransition, DebeziumDerivedTransitionProjection,
    DebeziumMaterializationMode, DebeziumMirrorRow, DebeziumRowExt, ensure_layout_tables,
};
use terracedb_example_order_watch::{
    ATTENTION_ORDERS_TABLE_NAME, ATTENTION_TRANSITIONS_TABLE_NAME, BACKLOG_ALERT_ORDER_ID,
    FILTERED_OUT_ORDER_ID, IGNORED_CUSTOMER_ID, LIVE_TRANSITION_ORDER_ID, OrderAttentionTransition,
    OrderAttentionView, OrderStatus, OrderWatchAlert, OrderWatchBoundary, OrderWatchOracleSnapshot,
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

async fn wait_for_state(
    runtime: &WorkflowRuntime<AlertWorkflowHandler>,
    instance_id: &str,
    expected: usize,
) -> Result<(), WorkflowError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let current = runtime
            .load_state(instance_id)
            .await?
            .map(|value| decode_state_count(Some(value)))
            .unwrap_or(0);
        if current == expected {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("workflow state did not reach {expected} before timeout");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_attach_mode(
    runtime: &WorkflowRuntime<AlertWorkflowHandler>,
    expected: WorkflowSourceAttachMode,
) -> Result<(), WorkflowError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let telemetry = runtime.telemetry_snapshot().await?;
        if telemetry
            .source_lags
            .iter()
            .any(|lag| lag.attach_mode == Some(expected))
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("workflow did not report attach mode {expected:?} before timeout");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
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
        let row = DebeziumMirrorRow::from_value(&value)?;
        let order = order_from_mirror_row(&row)?;
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

fn order_from_mirror_row(row: &DebeziumMirrorRow) -> Result<OrderWatchOrder, io::Error> {
    Ok(OrderWatchOrder {
        order_id: row
            .primary_key
            .require_string_or_number("id")
            .map_err(io::Error::other)?,
        region: row
            .values
            .require_string("region")
            .map(str::to_string)
            .map_err(io::Error::other)?,
        status: status_field(&row.values)?,
        source_partition: row.kafka.partition,
        source_offset: row.kafka.offset,
    })
}

fn status_field(row: &BTreeMap<String, JsonValue>) -> Result<OrderStatus, io::Error> {
    match row.require_string("status").map_err(io::Error::other)? {
        "open" => Ok(OrderStatus::Open),
        "closed" => Ok(OrderStatus::Closed),
        other => Err(io::Error::other(format!("unknown order status `{other}`"))),
    }
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
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_secs(20))
        .run_with(move |context| async move {
            let boundary = OrderWatchBoundary::new();
            let db = context
                .open_db(tiered_test_config(&format!("/order-watch/{seed}-{mode:?}")))
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
            let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
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
                    wait_for_state(&workflow_runtime, BACKLOG_ALERT_ORDER_ID, 1).await?;
                    wait_for_state(&workflow_runtime, LIVE_TRANSITION_ORDER_ID, 2).await?;

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

                    wait_for_state(&workflow_runtime, LIVE_TRANSITION_ORDER_ID, 2).await?;

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
                orders_current: collect_current_orders(
                    &db.table(boundary.orders_layout().current_table_name().to_string()),
                )
                .await?,
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
