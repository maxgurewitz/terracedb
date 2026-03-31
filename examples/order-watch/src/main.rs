use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use rskafka::client::{
    Client, ClientBuilder,
    partition::{OffsetAt, UnknownTopicHandling},
};
use terracedb::{
    ChangeEntry, CompactionStrategy, Db, DbBuilder, DbSettings, S3Location, ScanOptions,
    SequenceNumber, SystemClock, Table, TableConfig, TableFormat, TieredDurabilityMode,
    TieredStorageConfig, Value, decode_outbox_entry,
};
use terracedb_debezium::{
    DebeziumChangeEntry, DebeziumDerivedTransition, DebeziumDerivedTransitionProjection,
    DebeziumMaterializationMode, DebeziumMirrorRow, DebeziumRowExt, create_layout_tables,
};
use terracedb_example_order_watch::{
    ATTENTION_ORDERS_TABLE_NAME, ATTENTION_TRANSITIONS_TABLE_NAME, BACKLOG_ALERT_ORDER_ID,
    FILTERED_OUT_ORDER_ID, IGNORED_CUSTOMER_ID, LIVE_TRANSITION_ORDER_ID, OrderAttentionTransition,
    OrderAttentionView, OrderStatus, OrderWatchAlert, OrderWatchBoundary, OrderWatchOracleSnapshot,
    OrderWatchOrder, OrderWatchSourceProgress, OrderWatchWorkflowMode, SNAPSHOT_WEST_ORDER_ID,
};
use terracedb_kafka::{
    KafkaBootstrapPolicy, KafkaBroker, KafkaFetchedBatch, KafkaOffset, KafkaPartitionClaim,
    KafkaPartitionSource, KafkaProgressStore, KafkaRecord, KafkaRecordHeader, KeepAllKafkaRecords,
    NoopKafkaRuntimeObserver, NoopKafkaTelemetrySink, TableKafkaProgressStore,
    drive_partition_once,
};
use terracedb_projections::{
    MultiSourceProjection, MultiSourceProjectionHandler, ProjectionContext, ProjectionError,
    ProjectionHandle, ProjectionHandlerError, ProjectionRuntime, ProjectionSequenceRun,
    ProjectionTransaction, RecomputeStrategy,
};
use terracedb_workflows::{
    WorkflowContext, WorkflowDefinition, WorkflowHandler, WorkflowHandlerError, WorkflowOutput,
    WorkflowRuntime, WorkflowSourceAttachMode, WorkflowStateMutation, WorkflowTrigger,
};
use thiserror::Error;

const FULL_SCAN_START: &[u8] = b"";
const FULL_SCAN_END: &[u8] = &[0xff];
const WORKFLOW_NAME: &str = "order-watch-alerts";
const DEFAULT_BOOTSTRAP_SERVERS: &str = "127.0.0.1:19092";
const DEFAULT_DATA_DIR: &str = ".tmp/order-watch-data";
const DEFAULT_TIMEOUT_SECS: u64 = 60;

#[derive(Clone, Debug)]
struct CliOptions {
    bootstrap_servers: String,
    data_dir: PathBuf,
    timeout: Duration,
}

impl CliOptions {
    fn parse() -> Result<Self, Box<dyn Error>> {
        let mut args = env::args().skip(1).collect::<Vec<_>>();
        let mut bootstrap_servers = env::var("ORDER_WATCH_KAFKA_BOOTSTRAP_SERVERS")
            .unwrap_or_else(|_| DEFAULT_BOOTSTRAP_SERVERS.to_string());
        let mut data_dir = PathBuf::from(
            env::var("ORDER_WATCH_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string()),
        );
        let mut timeout = Duration::from_secs(
            env::var("ORDER_WATCH_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_TIMEOUT_SECS),
        );

        while !args.is_empty() {
            let flag = args.remove(0);
            match flag.as_str() {
                "--bootstrap-servers" => {
                    bootstrap_servers = take_arg(&mut args, "bootstrap servers")?;
                }
                "--data-dir" => {
                    data_dir = PathBuf::from(take_arg(&mut args, "data dir")?);
                }
                "--timeout-secs" => {
                    timeout = Duration::from_secs(
                        take_arg(&mut args, "timeout")?
                            .parse()
                            .map_err(|_| io::Error::other("timeout must be an integer"))?,
                    );
                }
                "--help" | "-h" | "help" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    return Err(Box::new(io::Error::other(format!(
                        "unknown argument `{other}`"
                    ))));
                }
            }
        }

        Ok(Self {
            bootstrap_servers,
            data_dir,
            timeout,
        })
    }
}

#[derive(Clone)]
struct AttentionOrdersProjection {
    boundary: OrderWatchBoundary,
    attention_orders: Table,
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

#[derive(Debug, Error)]
enum RskafkaBrokerError {
    #[error("bootstrap server list cannot be empty")]
    EmptyBootstrapServers,
    #[error("kafka client request failed: {source}")]
    Client {
        #[source]
        source: rskafka::client::error::Error,
    },
    #[error("kafka partition {topic}[{partition}] is not representable as i32")]
    PartitionOutOfRange { topic: String, partition: u32 },
    #[error("kafka request offset {offset} for {topic}[{partition}] exceeds i64")]
    RequestOffsetOutOfRange {
        topic: String,
        partition: u32,
        offset: u64,
    },
    #[error("kafka broker returned negative offset {offset} for {topic}[{partition}]")]
    NegativeOffset {
        topic: String,
        partition: u32,
        offset: i64,
    },
}

#[derive(Clone, Debug)]
struct RskafkaBroker {
    client: Arc<Client>,
    fetch_max_bytes: i32,
    fetch_max_wait_ms: i32,
}

impl RskafkaBroker {
    async fn connect(bootstrap_servers: &str) -> Result<Self, RskafkaBrokerError> {
        let brokers = bootstrap_servers
            .split(',')
            .map(str::trim)
            .filter(|server| !server.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if brokers.is_empty() {
            return Err(RskafkaBrokerError::EmptyBootstrapServers);
        }

        let client = ClientBuilder::new(brokers)
            .build()
            .await
            .map_err(|source| RskafkaBrokerError::Client { source })?;
        Ok(Self {
            client: Arc::new(client),
            fetch_max_bytes: 1_048_576,
            fetch_max_wait_ms: 250,
        })
    }

    async fn partition_client(
        &self,
        claim: &KafkaPartitionClaim,
    ) -> Result<rskafka::client::partition::PartitionClient, RskafkaBrokerError> {
        let topic = claim.source.topic_partition.topic.clone();
        let partition = claim.source.topic_partition.partition.get();
        let partition =
            i32::try_from(partition).map_err(|_| RskafkaBrokerError::PartitionOutOfRange {
                topic: topic.clone(),
                partition,
            })?;

        self.client
            .partition_client(topic, partition, UnknownTopicHandling::Retry)
            .await
            .map_err(|source| RskafkaBrokerError::Client { source })
    }

    fn encode_offset(
        topic: &str,
        partition: u32,
        offset: i64,
    ) -> Result<KafkaOffset, RskafkaBrokerError> {
        let offset = u64::try_from(offset).map_err(|_| RskafkaBrokerError::NegativeOffset {
            topic: topic.to_string(),
            partition,
            offset,
        })?;
        Ok(KafkaOffset::new(offset))
    }

    fn request_offset(
        topic: &str,
        partition: u32,
        offset: KafkaOffset,
    ) -> Result<i64, RskafkaBrokerError> {
        i64::try_from(offset.get()).map_err(|_| RskafkaBrokerError::RequestOffsetOutOfRange {
            topic: topic.to_string(),
            partition,
            offset: offset.get(),
        })
    }
}

#[async_trait]
impl KafkaBroker for RskafkaBroker {
    type Error = RskafkaBrokerError;

    async fn resolve_offset(
        &self,
        claim: &KafkaPartitionClaim,
        policy: KafkaBootstrapPolicy,
    ) -> Result<KafkaOffset, Self::Error> {
        let topic = claim.source.topic_partition.topic.clone();
        let partition = claim.source.topic_partition.partition.get();
        let at = match policy {
            KafkaBootstrapPolicy::Earliest => OffsetAt::Earliest,
            KafkaBootstrapPolicy::Latest => OffsetAt::Latest,
        };
        let offset = self
            .partition_client(claim)
            .await?
            .get_offset(at)
            .await
            .map_err(|source| RskafkaBrokerError::Client { source })?;
        Self::encode_offset(&topic, partition, offset)
    }

    async fn fetch_batch(
        &self,
        claim: &KafkaPartitionClaim,
        next_offset: KafkaOffset,
        max_records: usize,
    ) -> Result<KafkaFetchedBatch, Self::Error> {
        let topic = claim.source.topic_partition.topic.clone();
        let partition = claim.source.topic_partition.partition.get();
        let client = self.partition_client(claim).await?;
        let high = client
            .get_offset(OffsetAt::Latest)
            .await
            .map_err(|source| RskafkaBrokerError::Client { source })?;
        let high = Self::encode_offset(&topic, partition, high)?;
        if next_offset >= high {
            return Ok(KafkaFetchedBatch::empty(Some(high)));
        }

        let (records, observed_high) = client
            .fetch_records(
                Self::request_offset(&topic, partition, next_offset)?,
                1..self.fetch_max_bytes,
                self.fetch_max_wait_ms,
            )
            .await
            .map_err(|source| RskafkaBrokerError::Client { source })?;
        let high = Self::encode_offset(&topic, partition, observed_high)?;
        let records = records
            .into_iter()
            .take(max_records.max(1))
            .map(|record| {
                Ok(KafkaRecord {
                    topic_partition: terracedb_kafka::KafkaTopicPartition::new(
                        topic.clone(),
                        partition,
                    ),
                    offset: Self::encode_offset(&topic, partition, record.offset)?,
                    timestamp_millis: u64::try_from(record.record.timestamp.timestamp_millis())
                        .ok(),
                    key: record.record.key,
                    value: record.record.value,
                    headers: record
                        .record
                        .headers
                        .into_iter()
                        .map(|(key, value)| KafkaRecordHeader::new(key, value))
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, RskafkaBrokerError>>()?;

        Ok(KafkaFetchedBatch::new(records, Some(high)))
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
        metadata: Default::default(),
    }
}

fn order_watch_db_settings(path: &str, prefix: &str) -> DbSettings {
    DbSettings::tiered_storage(TieredStorageConfig {
        ssd: terracedb::SsdConfig {
            path: path.to_string(),
        },
        s3: S3Location {
            bucket: "terracedb-example-order-watch".to_string(),
            prefix: prefix.to_string(),
        },
        max_local_bytes: 128 * 1024,
        durability: TieredDurabilityMode::GroupCommit,
        local_retention: terracedb::TieredLocalRetentionMode::Offload,
    })
}

fn order_watch_db_builder(path: &str, prefix: &str) -> DbBuilder {
    Db::builder().settings(order_watch_db_settings(path, prefix))
}

async fn ensure_table(db: &Db, config: TableConfig) -> Result<Table, Box<dyn Error>> {
    match db.create_table(config.clone()).await {
        Ok(table) => Ok(table),
        Err(terracedb::CreateTableError::AlreadyExists(_)) => Ok(db.table(config.name)),
        Err(error) => Err(Box::new(error)),
    }
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
    timeout: Duration,
) -> Result<Option<SequenceNumber>, Box<dyn Error>>
where
    B: KafkaBroker,
    B::Error: Error + Send + Sync + 'static,
{
    let observer = NoopKafkaRuntimeObserver;
    let telemetry = NoopKafkaTelemetrySink;
    let worker = terracedb_kafka::KafkaWorkerOptions {
        batch_limit: 1,
        ..terracedb_kafka::KafkaWorkerOptions::default()
    };
    let deadline = Instant::now() + timeout;
    let mut last_sequence = None;

    while progress_store
        .load(&claim.source)
        .await?
        .map(|progress| progress.next_offset.get())
        .unwrap_or(0)
        < target_offset
    {
        if Instant::now() >= deadline {
            return Err(Box::new(io::Error::other(format!(
                "timed out waiting for {} to reach offset {target_offset}",
                claim.source.topic_partition.topic
            ))));
        }
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
    I: IntoIterator<Item = (&'a Table, SequenceNumber)>,
{
    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_sources(targets))
        .await
        .expect("projection should settle")
}

async fn wait_for_state(
    runtime: &WorkflowRuntime<AlertWorkflowHandler>,
    instance_id: &str,
    expected: usize,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + timeout;
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
            return Err(Box::new(io::Error::other(format!(
                "workflow state for {instance_id} did not reach {expected} before timeout"
            ))));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_attach_mode(
    runtime: &WorkflowRuntime<AlertWorkflowHandler>,
    expected: WorkflowSourceAttachMode,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + timeout;
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
            return Err(Box::new(io::Error::other(format!(
                "workflow did not report attach mode {expected:?} before timeout"
            ))));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn collect_attention_orders(
    table: &terracedb_example_order_watch::OrderAttentionOrdersTable,
) -> Result<BTreeMap<String, OrderAttentionView>, Box<dyn Error>> {
    let mut rows = table.scan_all(ScanOptions::default()).await?;
    let mut out = BTreeMap::new();
    while let Some((order_id, row)) = rows.next().await {
        out.insert(order_id, row);
    }
    Ok(out)
}

async fn collect_attention_transitions(
    table: &terracedb_example_order_watch::OrderAttentionTransitionsTable,
) -> Result<Vec<OrderAttentionTransition>, Box<dyn Error>> {
    let mut rows = table.scan_all(ScanOptions::default()).await?;
    let mut out = Vec::new();
    while let Some((_key, row)) = rows.next().await {
        out.push(row);
    }
    Ok(out)
}

async fn collect_current_orders(
    table: &Table,
) -> Result<BTreeMap<String, OrderWatchOrder>, Box<dyn Error>> {
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
        out.insert(
            row.primary_key.require_string_or_number("id")?,
            OrderWatchOrder {
                order_id: row.primary_key.require_string_or_number("id")?,
                region: row.values.require_string("region")?.to_string(),
                status: match row.values.require_string("status")? {
                    "open" => OrderStatus::Open,
                    "closed" => OrderStatus::Closed,
                    other => {
                        return Err(Box::new(io::Error::other(format!(
                            "unknown order status `{other}`"
                        ))));
                    }
                },
                source_partition: row.kafka.partition,
                source_offset: row.kafka.offset,
            },
        );
    }
    Ok(out)
}

async fn collect_outbox_alerts(table: &Table) -> Result<Vec<OrderWatchAlert>, Box<dyn Error>> {
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

async fn run_mode(
    options: &CliOptions,
    mode: OrderWatchWorkflowMode,
) -> Result<OrderWatchOracleSnapshot, Box<dyn Error>> {
    let boundary = OrderWatchBoundary::with_partitions([0_u32], [0_u32]);
    let mode_name = match mode {
        OrderWatchWorkflowMode::HistoricalReplay => "historical",
        OrderWatchWorkflowMode::LiveOnlyAttach => "live-only",
    };
    let mode_dir = options.data_dir.join(mode_name);
    if mode_dir.exists() {
        fs::remove_dir_all(&mode_dir)?;
    }
    fs::create_dir_all(&mode_dir)?;

    let ssd_path = mode_dir.join("ssd");
    let object_store_root = mode_dir.join("object-store");
    let db = order_watch_db_builder(
        ssd_path
            .to_str()
            .ok_or_else(|| io::Error::other("ssd path must be valid utf-8"))?,
        &format!("order-watch-{mode_name}"),
    )
    .local_object_store(&object_store_root)
    .clock(std::sync::Arc::new(SystemClock))
    .open()
    .await?;

    let row_template = row_table_config("template");
    create_layout_tables(&db, boundary.source_layouts(), &row_template, &row_template).await?;
    ensure_table(&db, row_table_config(ATTENTION_ORDERS_TABLE_NAME)).await?;
    ensure_table(&db, row_table_config(ATTENTION_TRANSITIONS_TABLE_NAME)).await?;
    let progress_table = ensure_table(&db, row_table_config("kafka_progress")).await?;

    let progress_store = TableKafkaProgressStore::new(progress_table);
    let attention_orders = boundary.attention_orders_table(&db);
    let attention_transitions = boundary.attention_transitions_table(&db);
    let handler = boundary.ingress_handler(&db, DebeziumMaterializationMode::Hybrid)?;
    let broker = RskafkaBroker::connect(&options.bootstrap_servers).await?;

    let orders_p0 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.orders_layout().topic(),
            0,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id(format!("order-watch-{mode_name}")),
        KafkaBootstrapPolicy::Earliest,
        1,
    );
    let customers_p0 = KafkaPartitionClaim::new(
        KafkaPartitionSource::new(
            boundary.ignored_layout().topic(),
            0,
            KafkaBootstrapPolicy::Earliest,
        )
        .source_id(format!("order-watch-{mode_name}")),
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
            MultiSourceProjection::new(
                "order-watch-attention-transitions",
                sources.clone(),
                DebeziumDerivedTransitionProjection::new(
                    attention_transitions.table().clone(),
                    boundary.attention_row_predicate(),
                    encode_transition_value,
                ),
            )
            .with_outputs([attention_transitions.table().clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await?;

    let (workflow_attach_mode, workflow_states, outbox_alerts, workflow_handle) = match mode {
        OrderWatchWorkflowMode::HistoricalReplay => {
            let final_p0 = drive_until_offset(
                &db,
                &broker,
                &progress_store,
                &orders_p0,
                5,
                &handler,
                options.timeout,
            )
            .await?
            .expect("orders partition 0 should commit the full scenario");
            drive_until_offset(
                &db,
                &broker,
                &progress_store,
                &customers_p0,
                1,
                &handler,
                options.timeout,
            )
            .await?
            .expect("customers partition should commit snapshot progress");

            wait_for_projection(&mut attention_orders_handle, [(&sources[0], final_p0)]).await?;
            wait_for_projection(&mut attention_transitions_handle, [(&sources[0], final_p0)])
                .await?;

            let workflow_runtime = WorkflowRuntime::open(
                db.clone(),
                std::sync::Arc::new(SystemClock),
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
            wait_for_attach_mode(
                &workflow_runtime,
                WorkflowSourceAttachMode::Historical,
                options.timeout,
            )
            .await?;
            wait_for_state(
                &workflow_runtime,
                BACKLOG_ALERT_ORDER_ID,
                1,
                options.timeout,
            )
            .await?;
            wait_for_state(
                &workflow_runtime,
                LIVE_TRANSITION_ORDER_ID,
                2,
                options.timeout,
            )
            .await?;

            let telemetry = workflow_runtime.telemetry_snapshot().await?;
            let states = state_count_map(
                workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
                workflow_runtime
                    .load_state(LIVE_TRANSITION_ORDER_ID)
                    .await?,
            );
            let alerts = collect_outbox_alerts(workflow_runtime.tables().outbox_table()).await?;
            drop(workflow_runtime);

            (
                telemetry
                    .source_lags
                    .first()
                    .and_then(|lag| lag.attach_mode),
                states,
                alerts,
                Some(workflow_handle),
            )
        }
        OrderWatchWorkflowMode::LiveOnlyAttach => {
            let backlog_p0 = drive_until_offset(
                &db,
                &broker,
                &progress_store,
                &orders_p0,
                3,
                &handler,
                options.timeout,
            )
            .await?
            .expect("orders partition 0 should commit snapshot plus backlog");
            drive_until_offset(
                &db,
                &broker,
                &progress_store,
                &customers_p0,
                1,
                &handler,
                options.timeout,
            )
            .await?
            .expect("customers partition should commit snapshot progress");

            wait_for_projection(&mut attention_orders_handle, [(&sources[0], backlog_p0)]).await?;
            wait_for_projection(
                &mut attention_transitions_handle,
                [(&sources[0], backlog_p0)],
            )
            .await?;

            let workflow_runtime = WorkflowRuntime::open(
                db.clone(),
                std::sync::Arc::new(SystemClock),
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
            wait_for_attach_mode(
                &workflow_runtime,
                WorkflowSourceAttachMode::LiveOnly,
                options.timeout,
            )
            .await?;

            let final_p0 = drive_until_offset(
                &db,
                &broker,
                &progress_store,
                &orders_p0,
                5,
                &handler,
                options.timeout,
            )
            .await?
            .expect("orders partition 0 should commit live transitions");
            wait_for_projection(&mut attention_orders_handle, [(&sources[0], final_p0)]).await?;
            wait_for_projection(&mut attention_transitions_handle, [(&sources[0], final_p0)])
                .await?;
            wait_for_state(
                &workflow_runtime,
                LIVE_TRANSITION_ORDER_ID,
                2,
                options.timeout,
            )
            .await?;

            let telemetry = workflow_runtime.telemetry_snapshot().await?;
            let states = state_count_map(
                workflow_runtime.load_state(BACKLOG_ALERT_ORDER_ID).await?,
                workflow_runtime
                    .load_state(LIVE_TRANSITION_ORDER_ID)
                    .await?,
            );
            let alerts = collect_outbox_alerts(workflow_runtime.tables().outbox_table()).await?;
            drop(workflow_runtime);

            (
                telemetry
                    .source_lags
                    .first()
                    .and_then(|lag| lag.attach_mode),
                states,
                alerts,
                Some(workflow_handle),
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
        attention_transitions: collect_attention_transitions(&attention_transitions).await?,
        workflow_states,
        outbox_alerts,
    };

    validate_single_partition_snapshot(&snapshot, mode)?;

    attention_orders_handle.shutdown().await?;
    attention_transitions_handle.shutdown().await?;
    if let Some(workflow_handle) = workflow_handle {
        workflow_handle.shutdown().await?;
    }

    Ok(snapshot)
}

fn validate_single_partition_snapshot(
    snapshot: &OrderWatchOracleSnapshot,
    mode: OrderWatchWorkflowMode,
) -> Result<(), Box<dyn Error>> {
    let boundary = OrderWatchBoundary::with_partitions([0_u32], [0_u32]);
    if snapshot.source_offset(boundary.orders_layout().topic(), 0) != Some(5) {
        return Err(Box::new(io::Error::other(
            "orders partition 0 should advance to offset 5",
        )));
    }
    if snapshot.source_offset(boundary.ignored_layout().topic(), 0) != Some(1) {
        return Err(Box::new(io::Error::other(
            "customers partition 0 should advance to offset 1",
        )));
    }
    if snapshot.orders_current.contains_key(FILTERED_OUT_ORDER_ID) {
        return Err(Box::new(io::Error::other(
            "filtered east orders must never appear in current-state",
        )));
    }
    if snapshot.orders_current.contains_key(IGNORED_CUSTOMER_ID) {
        return Err(Box::new(io::Error::other(
            "customer rows must never appear in orders current-state",
        )));
    }
    if !snapshot.orders_current.contains_key(SNAPSHOT_WEST_ORDER_ID)
        || !snapshot.orders_current.contains_key(BACKLOG_ALERT_ORDER_ID)
    {
        return Err(Box::new(io::Error::other(
            "snapshot west and backlog rows should remain visible in current-state",
        )));
    }
    if snapshot
        .orders_current
        .contains_key(LIVE_TRANSITION_ORDER_ID)
    {
        return Err(Box::new(io::Error::other(
            "live transition row should be removed after leaving the watched subset",
        )));
    }
    if snapshot.attention_transitions.len() != 3 {
        return Err(Box::new(io::Error::other(
            "attention transitions should contain backlog enter plus live enter/exit",
        )));
    }
    if !snapshot
        .attention_orders
        .contains_key(SNAPSHOT_WEST_ORDER_ID)
        || !snapshot
            .attention_orders
            .contains_key(BACKLOG_ALERT_ORDER_ID)
    {
        return Err(Box::new(io::Error::other(
            "attention orders should contain retained west snapshot and backlog rows",
        )));
    }
    let backlog_seen = snapshot
        .outbox_alerts
        .iter()
        .any(|alert| alert.order_id == BACKLOG_ALERT_ORDER_ID);
    if backlog_seen != matches!(mode, OrderWatchWorkflowMode::HistoricalReplay) {
        return Err(Box::new(io::Error::other(
            "historical mode should replay backlog alerts and live-only mode should skip them",
        )));
    }
    if snapshot.workflow_states.get(LIVE_TRANSITION_ORDER_ID) != Some(&2) {
        return Err(Box::new(io::Error::other(
            "live transition order should produce one enter and one exit alert",
        )));
    }
    if snapshot.workflow_states.get(BACKLOG_ALERT_ORDER_ID)
        != Some(&match mode {
            OrderWatchWorkflowMode::HistoricalReplay => 1,
            OrderWatchWorkflowMode::LiveOnlyAttach => 0,
        })
    {
        return Err(Box::new(io::Error::other(
            "backlog alert count should depend on workflow attach mode",
        )));
    }
    Ok(())
}

fn take_arg(args: &mut Vec<String>, name: &str) -> Result<String, Box<dyn Error>> {
    if args.is_empty() {
        return Err(Box::new(io::Error::other(format!(
            "missing required argument: {name}"
        ))));
    }
    Ok(args.remove(0))
}

fn print_usage() {
    println!(
        "\
Usage:
  cargo run -p terracedb-example-order-watch -- [--bootstrap-servers HOST:PORT] [--data-dir DIR] [--timeout-secs SECONDS]

Environment:
  ORDER_WATCH_KAFKA_BOOTSTRAP_SERVERS
  ORDER_WATCH_DATA_DIR
  ORDER_WATCH_TIMEOUT_SECS
"
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let options = CliOptions::parse()?;
    if !options.data_dir.exists() {
        fs::create_dir_all(&options.data_dir)?;
    }

    let historical = run_mode(&options, OrderWatchWorkflowMode::HistoricalReplay).await?;
    println!(
        "historical-replay verified: {} alerts, attach={:?}",
        historical.outbox_alerts.len(),
        historical.workflow_attach_mode
    );

    let live_only = run_mode(&options, OrderWatchWorkflowMode::LiveOnlyAttach).await?;
    println!(
        "live-only-attach verified: {} alerts, attach={:?}",
        live_only.outbox_alerts.len(),
        live_only.workflow_attach_mode
    );

    Ok(())
}
