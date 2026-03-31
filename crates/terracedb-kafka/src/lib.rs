use std::error::Error as StdError;

use async_trait::async_trait;
use terracedb::{
    Db, Key, ReadError, SequenceNumber, StorageError, Table, Transaction, TransactionCommitError,
    Value,
};
use thiserror::Error;

const SOURCE_ID_FORMAT_VERSION: u8 = 1;
const SOURCE_PROGRESS_FORMAT_VERSION: u8 = 1;

pub const DEFAULT_KAFKA_BATCH_LIMIT: usize = 128;

type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaOffset(u64);

impl KafkaOffset {
    pub const ENCODED_LEN: usize = std::mem::size_of::<u64>();

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub const fn encode(self) -> [u8; Self::ENCODED_LEN] {
        self.0.to_be_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(StorageError::corruption(format!(
                "kafka offset must encode exactly {} bytes",
                Self::ENCODED_LEN
            )));
        }

        let mut raw = [0_u8; Self::ENCODED_LEN];
        raw.copy_from_slice(bytes);
        Ok(Self::new(u64::from_be_bytes(raw)))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaPartition(u32);

impl KafkaPartition {
    pub const ENCODED_LEN: usize = std::mem::size_of::<u32>();

    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub const fn encode(self) -> [u8; Self::ENCODED_LEN] {
        self.0.to_be_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(StorageError::corruption(format!(
                "kafka partition must encode exactly {} bytes",
                Self::ENCODED_LEN
            )));
        }

        let mut raw = [0_u8; Self::ENCODED_LEN];
        raw.copy_from_slice(bytes);
        Ok(Self::new(u32::from_be_bytes(raw)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaTopicPartition {
    pub topic: String,
    pub partition: KafkaPartition,
}

impl KafkaTopicPartition {
    pub fn new(topic: impl Into<String>, partition: u32) -> Self {
        Self {
            topic: topic.into(),
            partition: KafkaPartition::new(partition),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KafkaBootstrapPolicy {
    Earliest,
    Latest,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaSourceId {
    pub consumer_group: String,
    pub topic_partition: KafkaTopicPartition,
}

impl KafkaSourceId {
    pub fn new(
        consumer_group: impl Into<String>,
        topic: impl Into<String>,
        partition: u32,
    ) -> Self {
        Self {
            consumer_group: consumer_group.into(),
            topic_partition: KafkaTopicPartition::new(topic, partition),
        }
    }

    pub fn encode(&self) -> Key {
        let consumer_group = self.consumer_group.as_bytes();
        let topic = self.topic_partition.topic.as_bytes();
        let mut bytes = Vec::with_capacity(
            1 + 4 + consumer_group.len() + 4 + topic.len() + KafkaPartition::ENCODED_LEN,
        );
        bytes.push(SOURCE_ID_FORMAT_VERSION);
        bytes.extend_from_slice(&(consumer_group.len() as u32).to_be_bytes());
        bytes.extend_from_slice(consumer_group);
        bytes.extend_from_slice(&(topic.len() as u32).to_be_bytes());
        bytes.extend_from_slice(topic);
        bytes.extend_from_slice(&self.topic_partition.partition.encode());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let Some(version) = bytes.first().copied() else {
            return Err(StorageError::corruption(
                "kafka source id is missing its format version",
            ));
        };
        if version != SOURCE_ID_FORMAT_VERSION {
            return Err(StorageError::corruption(
                "unknown kafka source id format version",
            ));
        }

        let mut cursor = 1;
        let consumer_group_len = decode_len(bytes, &mut cursor, "kafka source consumer group")?;
        let consumer_group = decode_utf8_field(
            bytes,
            &mut cursor,
            consumer_group_len,
            "kafka source consumer group",
        )?;
        let topic_len = decode_len(bytes, &mut cursor, "kafka source topic")?;
        let topic = decode_utf8_field(bytes, &mut cursor, topic_len, "kafka source topic")?;
        let partition = decode_partition_field(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(StorageError::corruption(
                "kafka source id has trailing bytes after the partition",
            ));
        }

        Ok(Self {
            consumer_group,
            topic_partition: KafkaTopicPartition { topic, partition },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaPartitionSource {
    pub topic_partition: KafkaTopicPartition,
    pub bootstrap: KafkaBootstrapPolicy,
}

impl KafkaPartitionSource {
    pub fn new(topic: impl Into<String>, partition: u32, bootstrap: KafkaBootstrapPolicy) -> Self {
        Self {
            topic_partition: KafkaTopicPartition::new(topic, partition),
            bootstrap,
        }
    }

    pub fn source_id(&self, consumer_group: impl Into<String>) -> KafkaSourceId {
        KafkaSourceId {
            consumer_group: consumer_group.into(),
            topic_partition: self.topic_partition.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaPartitionClaim {
    pub source: KafkaSourceId,
    pub bootstrap: KafkaBootstrapPolicy,
    pub generation: u64,
}

impl KafkaPartitionClaim {
    pub fn new(source: KafkaSourceId, bootstrap: KafkaBootstrapPolicy, generation: u64) -> Self {
        Self {
            source,
            bootstrap,
            generation,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaFilterDecision {
    Retain,
    Skip,
}

pub trait KafkaRecordFilter: Send + Sync {
    fn classify(&self, record: &KafkaRecord) -> KafkaFilterDecision;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KeepAllKafkaRecords;

impl KafkaRecordFilter for KeepAllKafkaRecords {
    fn classify(&self, _record: &KafkaRecord) -> KafkaFilterDecision {
        KafkaFilterDecision::Retain
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaRecordHeader {
    pub key: String,
    pub value: Vec<u8>,
}

impl KafkaRecordHeader {
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaRecord {
    pub topic_partition: KafkaTopicPartition,
    pub offset: KafkaOffset,
    pub timestamp_millis: Option<u64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<KafkaRecordHeader>,
}

impl KafkaRecord {
    pub fn new(topic: impl Into<String>, partition: u32, offset: u64) -> Self {
        Self {
            topic_partition: KafkaTopicPartition::new(topic, partition),
            offset: KafkaOffset::new(offset),
            timestamp_millis: None,
            key: None,
            value: None,
            headers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaFetchedBatch {
    pub records: Vec<KafkaRecord>,
    pub high_watermark: Option<KafkaOffset>,
}

impl KafkaFetchedBatch {
    pub fn new(records: Vec<KafkaRecord>, high_watermark: Option<KafkaOffset>) -> Self {
        Self {
            records,
            high_watermark,
        }
    }

    pub fn empty(high_watermark: Option<KafkaOffset>) -> Self {
        Self::new(Vec::new(), high_watermark)
    }

    pub fn offset_bounds(&self) -> Option<(KafkaOffset, KafkaOffset)> {
        Some((self.records.first()?.offset, self.records.last()?.offset))
    }

    pub fn next_offset(&self, fallback: KafkaOffset) -> KafkaOffset {
        self.records
            .last()
            .map(|record| record.offset.next())
            .unwrap_or(fallback)
    }

    pub fn lag_after(&self, next_offset: KafkaOffset) -> Option<u64> {
        self.high_watermark
            .map(|watermark| watermark.get().saturating_sub(next_offset.get()))
    }

    pub fn validate_for(
        &self,
        claim: &KafkaPartitionClaim,
        requested_offset: KafkaOffset,
    ) -> Result<(), KafkaContractError> {
        let mut previous_offset = None;
        for record in &self.records {
            if record.topic_partition != claim.source.topic_partition {
                return Err(KafkaContractError::SourceMismatch {
                    expected_topic: claim.source.topic_partition.topic.clone(),
                    expected_partition: claim.source.topic_partition.partition.get(),
                    observed_topic: record.topic_partition.topic.clone(),
                    observed_partition: record.topic_partition.partition.get(),
                });
            }
            if record.offset < requested_offset {
                return Err(KafkaContractError::OffsetBeforeRequested {
                    requested: requested_offset.get(),
                    observed: record.offset.get(),
                });
            }
            if let Some(previous) = previous_offset
                && record.offset <= previous
            {
                return Err(KafkaContractError::OutOfOrderOffsets {
                    previous: previous.get(),
                    observed: record.offset.get(),
                });
            }
            previous_offset = Some(record.offset);
        }
        Ok(())
    }
}

#[async_trait]
pub trait KafkaBroker: Send + Sync {
    type Error: StdError + Send + Sync + 'static;

    async fn resolve_offset(
        &self,
        claim: &KafkaPartitionClaim,
        policy: KafkaBootstrapPolicy,
    ) -> Result<KafkaOffset, Self::Error>;

    async fn fetch_batch(
        &self,
        claim: &KafkaPartitionClaim,
        next_offset: KafkaOffset,
        max_records: usize,
    ) -> Result<KafkaFetchedBatch, Self::Error>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KafkaSourceProgress {
    pub next_offset: KafkaOffset,
}

impl KafkaSourceProgress {
    pub const ENCODED_LEN: usize = 1 + KafkaOffset::ENCODED_LEN;

    pub const fn new(next_offset: KafkaOffset) -> Self {
        Self { next_offset }
    }

    pub fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::ENCODED_LEN);
        bytes.push(SOURCE_PROGRESS_FORMAT_VERSION);
        bytes.extend_from_slice(&self.next_offset.encode());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != Self::ENCODED_LEN || bytes[0] != SOURCE_PROGRESS_FORMAT_VERSION {
            return Err(StorageError::corruption(
                "kafka source progress value encoding is invalid",
            ));
        }

        Ok(Self::new(KafkaOffset::decode(&bytes[1..])?))
    }

    pub fn last_applied_offset(self) -> Option<KafkaOffset> {
        self.next_offset.get().checked_sub(1).map(KafkaOffset::new)
    }
}

#[async_trait]
pub trait KafkaProgressStore: Send + Sync {
    async fn load(&self, source: &KafkaSourceId) -> Result<Option<KafkaSourceProgress>, ReadError>;

    fn stage_persist(
        &self,
        tx: &mut Transaction,
        source: &KafkaSourceId,
        progress: KafkaSourceProgress,
    );
}

#[derive(Clone, Debug)]
pub struct TableKafkaProgressStore {
    progress_table: Table,
}

impl TableKafkaProgressStore {
    pub fn new(progress_table: Table) -> Self {
        Self { progress_table }
    }

    pub fn progress_table(&self) -> &Table {
        &self.progress_table
    }

    pub fn source_key(source: &KafkaSourceId) -> Key {
        source.encode()
    }

    pub fn encode_progress(progress: KafkaSourceProgress) -> Value {
        Value::bytes(progress.encode())
    }

    pub fn decode_progress(value: &Value) -> Result<KafkaSourceProgress, StorageError> {
        let bytes = expect_bytes_value(value, "kafka source progress")?;
        KafkaSourceProgress::decode(bytes)
    }
}

#[async_trait]
impl KafkaProgressStore for TableKafkaProgressStore {
    async fn load(&self, source: &KafkaSourceId) -> Result<Option<KafkaSourceProgress>, ReadError> {
        let Some(value) = self.progress_table.read(Self::source_key(source)).await? else {
            return Ok(None);
        };
        Ok(Some(
            Self::decode_progress(&value).map_err(ReadError::from)?,
        ))
    }

    fn stage_persist(
        &self,
        tx: &mut Transaction,
        source: &KafkaSourceId,
        progress: KafkaSourceProgress,
    ) {
        tx.write(
            &self.progress_table,
            Self::source_key(source),
            Self::encode_progress(progress),
        );
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KafkaMaterializationLayout {
    #[default]
    OneTablePerPartition,
    SharedPartitionOffsetTable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaPartitionOffset {
    pub partition: KafkaPartition,
    pub offset: KafkaOffset,
}

impl KafkaPartitionOffset {
    pub const ENCODED_LEN: usize = KafkaPartition::ENCODED_LEN + KafkaOffset::ENCODED_LEN;

    pub const fn new(partition: KafkaPartition, offset: KafkaOffset) -> Self {
        Self { partition, offset }
    }

    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0_u8; Self::ENCODED_LEN];
        bytes[..KafkaPartition::ENCODED_LEN].copy_from_slice(&self.partition.encode());
        bytes[KafkaPartition::ENCODED_LEN..].copy_from_slice(&self.offset.encode());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(StorageError::corruption(format!(
                "kafka partition-offset key must encode exactly {} bytes",
                Self::ENCODED_LEN
            )));
        }

        Ok(Self {
            partition: KafkaPartition::decode(&bytes[..KafkaPartition::ENCODED_LEN])?,
            offset: KafkaOffset::decode(&bytes[KafkaPartition::ENCODED_LEN..])?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaMaterializedOrderKey {
    Offset(KafkaOffset),
    PartitionOffset(KafkaPartitionOffset),
}

impl KafkaMaterializedOrderKey {
    pub fn encode(self) -> Key {
        match self {
            Self::Offset(offset) => offset.encode().to_vec(),
            Self::PartitionOffset(partition_offset) => partition_offset.encode().to_vec(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaMaterializationRoute {
    pub layout: KafkaMaterializationLayout,
    pub topic_partition: KafkaTopicPartition,
    pub key: KafkaMaterializedOrderKey,
}

impl KafkaMaterializationLayout {
    pub fn route(
        self,
        topic_partition: &KafkaTopicPartition,
        offset: KafkaOffset,
    ) -> KafkaMaterializationRoute {
        let key = match self {
            Self::OneTablePerPartition => KafkaMaterializedOrderKey::Offset(offset),
            Self::SharedPartitionOffsetTable => KafkaMaterializedOrderKey::PartitionOffset(
                KafkaPartitionOffset::new(topic_partition.partition, offset),
            ),
        };

        KafkaMaterializationRoute {
            layout: self,
            topic_partition: topic_partition.clone(),
            key,
        }
    }

    pub fn route_record(self, record: &KafkaRecord) -> KafkaMaterializationRoute {
        self.route(&record.topic_partition, record.offset)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KafkaShutdownMode {
    #[default]
    DrainCurrentBatch,
    Immediate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaWorkerControl {
    Continue,
    ReleasePartition,
    Shutdown(KafkaShutdownMode),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaWorkerExitReason {
    ClaimReleased,
    ShutdownRequested,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaWorkerStatus {
    Running,
    Idle,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KafkaWorkerOptions {
    pub batch_limit: usize,
    pub shutdown: KafkaShutdownMode,
}

impl Default for KafkaWorkerOptions {
    fn default() -> Self {
        Self {
            batch_limit: DEFAULT_KAFKA_BATCH_LIMIT,
            shutdown: KafkaShutdownMode::DrainCurrentBatch,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KafkaStartPosition {
    Persisted {
        next_offset: KafkaOffset,
    },
    Bootstrapped {
        policy: KafkaBootstrapPolicy,
        next_offset: KafkaOffset,
    },
}

impl KafkaStartPosition {
    pub const fn next_offset(&self) -> KafkaOffset {
        match self {
            Self::Persisted { next_offset } | Self::Bootstrapped { next_offset, .. } => {
                *next_offset
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaAdmissionBatch {
    pub claim: KafkaPartitionClaim,
    pub start_offset: KafkaOffset,
    pub retained_records: Vec<KafkaRecord>,
    pub skipped_offsets: Vec<KafkaOffset>,
    pub next_offset: KafkaOffset,
    pub high_watermark: Option<KafkaOffset>,
}

#[async_trait]
pub trait KafkaRecordHandler: Send + Sync {
    type Error: StdError + Send + Sync + 'static;

    async fn apply_record(
        &self,
        tx: &mut Transaction,
        record: &KafkaRecord,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait KafkaBatchHandler: Send + Sync {
    type Error: StdError + Send + Sync + 'static;

    async fn apply_batch(
        &self,
        tx: &mut Transaction,
        batch: &KafkaAdmissionBatch,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T> KafkaBatchHandler for T
where
    T: KafkaRecordHandler,
{
    type Error = T::Error;

    async fn apply_batch(
        &self,
        tx: &mut Transaction,
        batch: &KafkaAdmissionBatch,
    ) -> Result<(), Self::Error> {
        for record in &batch.retained_records {
            self.apply_record(tx, record).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct KafkaIngressDefinition<F, H> {
    pub consumer_group: String,
    pub sources: Vec<KafkaPartitionSource>,
    pub filter: F,
    pub handler: H,
    pub materialization: KafkaMaterializationLayout,
    pub worker: KafkaWorkerOptions,
}

impl<H> KafkaIngressDefinition<KeepAllKafkaRecords, H> {
    pub fn new(
        consumer_group: impl Into<String>,
        sources: Vec<KafkaPartitionSource>,
        handler: H,
    ) -> Self {
        Self {
            consumer_group: consumer_group.into(),
            sources,
            filter: KeepAllKafkaRecords,
            handler,
            materialization: KafkaMaterializationLayout::default(),
            worker: KafkaWorkerOptions::default(),
        }
    }
}

impl<F, H> KafkaIngressDefinition<F, H> {
    pub fn with_filter<NewFilter>(self, filter: NewFilter) -> KafkaIngressDefinition<NewFilter, H> {
        KafkaIngressDefinition {
            consumer_group: self.consumer_group,
            sources: self.sources,
            filter,
            handler: self.handler,
            materialization: self.materialization,
            worker: self.worker,
        }
    }

    pub fn with_materialization(mut self, materialization: KafkaMaterializationLayout) -> Self {
        self.materialization = materialization;
        self
    }

    pub fn with_worker_options(mut self, worker: KafkaWorkerOptions) -> Self {
        self.worker = worker;
        self
    }

    pub fn source_ids(&self) -> Vec<KafkaSourceId> {
        self.sources
            .iter()
            .map(|source| source.source_id(self.consumer_group.clone()))
            .collect()
    }

    pub fn claims(&self, generation: u64) -> Vec<KafkaPartitionClaim> {
        self.sources
            .iter()
            .map(|source| {
                KafkaPartitionClaim::new(
                    source.source_id(self.consumer_group.clone()),
                    source.bootstrap,
                    generation,
                )
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaPartitionTelemetrySnapshot {
    pub source: KafkaSourceId,
    pub worker_status: KafkaWorkerStatus,
    pub start_position: KafkaStartPosition,
    pub committed_sequence: Option<SequenceNumber>,
    pub next_offset: KafkaOffset,
    pub last_fetched_offset: Option<KafkaOffset>,
    pub last_retained_offset: Option<KafkaOffset>,
    pub retained_records: usize,
    pub skipped_records: usize,
    pub lag_records: Option<u64>,
}

pub trait KafkaTelemetrySink: Send + Sync {
    fn record_snapshot(&self, snapshot: KafkaPartitionTelemetrySnapshot);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopKafkaTelemetrySink;

impl KafkaTelemetrySink for NoopKafkaTelemetrySink {
    fn record_snapshot(&self, _snapshot: KafkaPartitionTelemetrySnapshot) {}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KafkaSimulationSeam {
    RestartFromOffset {
        source: KafkaSourceId,
        next_offset: KafkaOffset,
    },
    DuplicateDelivery {
        source: KafkaSourceId,
        offset: KafkaOffset,
    },
    FilteredButAcknowledged {
        source: KafkaSourceId,
        offset: KafkaOffset,
    },
    PartitionLocalOrdering {
        source: KafkaSourceId,
        first_offset: KafkaOffset,
        last_offset: KafkaOffset,
    },
    ClaimChanged {
        claim: KafkaPartitionClaim,
    },
    CrashBetweenWriteAndProgressPersist {
        source: KafkaSourceId,
        next_offset: KafkaOffset,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KafkaRuntimeEvent {
    BootstrapResolved {
        claim: KafkaPartitionClaim,
        start_position: KafkaStartPosition,
    },
    BatchFetched {
        claim: KafkaPartitionClaim,
        requested_offset: KafkaOffset,
        record_count: usize,
        high_watermark: Option<KafkaOffset>,
    },
    BatchFiltered {
        source: KafkaSourceId,
        retained_records: usize,
        skipped_offsets: Vec<KafkaOffset>,
    },
    BatchCommitted {
        source: KafkaSourceId,
        next_offset: KafkaOffset,
        committed_sequence: SequenceNumber,
    },
    WorkerStopped {
        claim: KafkaPartitionClaim,
        reason: KafkaWorkerExitReason,
    },
    SimulationSeam(KafkaSimulationSeam),
}

pub trait KafkaRuntimeObserver: Send + Sync {
    fn on_event(&self, event: KafkaRuntimeEvent);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopKafkaRuntimeObserver;

impl KafkaRuntimeObserver for NoopKafkaRuntimeObserver {
    fn on_event(&self, _event: KafkaRuntimeEvent) {}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaWorkerStepOutcome {
    pub retained_offsets: Vec<KafkaOffset>,
    pub skipped_offsets: Vec<KafkaOffset>,
    pub telemetry: KafkaPartitionTelemetrySnapshot,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KafkaContractError {
    #[error(
        "broker returned records for {observed_topic}[{observed_partition}] while {expected_topic}[{expected_partition}] was claimed"
    )]
    SourceMismatch {
        expected_topic: String,
        expected_partition: u32,
        observed_topic: String,
        observed_partition: u32,
    },
    #[error("broker returned offset {observed} before requested offset {requested}")]
    OffsetBeforeRequested { requested: u64, observed: u64 },
    #[error(
        "broker returned out-of-order offsets within a partition batch: previous {previous}, observed {observed}"
    )]
    OutOfOrderOffsets { previous: u64, observed: u64 },
}

#[derive(Debug, Error)]
pub enum KafkaRuntimeError {
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Commit(#[from] TransactionCommitError),
    #[error(transparent)]
    Contract(#[from] KafkaContractError),
    #[error("kafka broker operation failed: {message}")]
    Broker {
        message: String,
        #[source]
        source: BoxedError,
    },
    #[error("kafka handler failed: {message}")]
    Handler {
        message: String,
        #[source]
        source: BoxedError,
    },
}

impl KafkaRuntimeError {
    fn broker<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Broker {
            message: error.to_string(),
            source: Box::new(error),
        }
    }

    fn handler<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Handler {
            message: error.to_string(),
            source: Box::new(error),
        }
    }
}

pub async fn resolve_start_position<B, S>(
    broker: &B,
    progress_store: &S,
    claim: &KafkaPartitionClaim,
) -> Result<KafkaStartPosition, KafkaRuntimeError>
where
    B: KafkaBroker,
    S: KafkaProgressStore,
{
    if let Some(progress) = progress_store.load(&claim.source).await? {
        return Ok(KafkaStartPosition::Persisted {
            next_offset: progress.next_offset,
        });
    }

    let next_offset = broker
        .resolve_offset(claim, claim.bootstrap)
        .await
        .map_err(KafkaRuntimeError::broker)?;
    Ok(KafkaStartPosition::Bootstrapped {
        policy: claim.bootstrap,
        next_offset,
    })
}

pub async fn drive_partition_once<B, S, F, H, O, T>(
    db: &Db,
    broker: &B,
    progress_store: &S,
    claim: &KafkaPartitionClaim,
    worker: KafkaWorkerOptions,
    filter: &F,
    handler: &H,
    observer: &O,
    telemetry: &T,
) -> Result<KafkaWorkerStepOutcome, KafkaRuntimeError>
where
    B: KafkaBroker,
    S: KafkaProgressStore,
    F: KafkaRecordFilter,
    H: KafkaBatchHandler,
    O: KafkaRuntimeObserver,
    T: KafkaTelemetrySink,
{
    let start_position = resolve_start_position(broker, progress_store, claim).await?;
    observer.on_event(KafkaRuntimeEvent::BootstrapResolved {
        claim: claim.clone(),
        start_position: start_position.clone(),
    });
    if let KafkaStartPosition::Persisted { next_offset } = start_position {
        observer.on_event(KafkaRuntimeEvent::SimulationSeam(
            KafkaSimulationSeam::RestartFromOffset {
                source: claim.source.clone(),
                next_offset,
            },
        ));
    }

    let requested_offset = start_position.next_offset();
    let fetched = broker
        .fetch_batch(claim, requested_offset, worker.batch_limit.max(1))
        .await
        .map_err(KafkaRuntimeError::broker)?;
    fetched.validate_for(claim, requested_offset)?;
    observer.on_event(KafkaRuntimeEvent::BatchFetched {
        claim: claim.clone(),
        requested_offset,
        record_count: fetched.records.len(),
        high_watermark: fetched.high_watermark,
    });
    if let Some((first_offset, last_offset)) = fetched.offset_bounds() {
        observer.on_event(KafkaRuntimeEvent::SimulationSeam(
            KafkaSimulationSeam::PartitionLocalOrdering {
                source: claim.source.clone(),
                first_offset,
                last_offset,
            },
        ));
    }

    let next_offset = fetched.next_offset(requested_offset);
    let last_fetched_offset = fetched.offset_bounds().map(|(_, last_offset)| last_offset);
    let high_watermark = fetched.high_watermark;
    let lag_records = fetched.lag_after(next_offset);

    let mut retained_records = Vec::new();
    let mut retained_offsets = Vec::new();
    let mut skipped_offsets = Vec::new();
    for record in fetched.records {
        match filter.classify(&record) {
            KafkaFilterDecision::Retain => {
                retained_offsets.push(record.offset);
                retained_records.push(record);
            }
            KafkaFilterDecision::Skip => {
                skipped_offsets.push(record.offset);
                observer.on_event(KafkaRuntimeEvent::SimulationSeam(
                    KafkaSimulationSeam::FilteredButAcknowledged {
                        source: claim.source.clone(),
                        offset: record.offset,
                    },
                ));
            }
        }
    }

    observer.on_event(KafkaRuntimeEvent::BatchFiltered {
        source: claim.source.clone(),
        retained_records: retained_records.len(),
        skipped_offsets: skipped_offsets.clone(),
    });

    let committed_sequence = if last_fetched_offset.is_some() {
        let admission = KafkaAdmissionBatch {
            claim: claim.clone(),
            start_offset: requested_offset,
            retained_records,
            skipped_offsets: skipped_offsets.clone(),
            next_offset,
            high_watermark,
        };
        let mut tx = Transaction::begin(db).await;
        if !admission.retained_records.is_empty() {
            handler
                .apply_batch(&mut tx, &admission)
                .await
                .map_err(KafkaRuntimeError::handler)?;
        }
        progress_store.stage_persist(
            &mut tx,
            &claim.source,
            KafkaSourceProgress::new(next_offset),
        );
        let sequence = tx.commit_no_flush().await?;
        observer.on_event(KafkaRuntimeEvent::BatchCommitted {
            source: claim.source.clone(),
            next_offset,
            committed_sequence: sequence,
        });
        Some(sequence)
    } else {
        None
    };

    let telemetry_snapshot = KafkaPartitionTelemetrySnapshot {
        source: claim.source.clone(),
        worker_status: if committed_sequence.is_some() {
            KafkaWorkerStatus::Running
        } else {
            KafkaWorkerStatus::Idle
        },
        start_position,
        committed_sequence,
        next_offset,
        last_fetched_offset,
        last_retained_offset: retained_offsets.last().copied(),
        retained_records: retained_offsets.len(),
        skipped_records: skipped_offsets.len(),
        lag_records,
    };
    telemetry.record_snapshot(telemetry_snapshot.clone());

    Ok(KafkaWorkerStepOutcome {
        retained_offsets,
        skipped_offsets,
        telemetry: telemetry_snapshot,
    })
}

fn expect_bytes_value<'a>(value: &'a Value, context: &str) -> Result<&'a [u8], StorageError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        Value::Record(_) => Err(StorageError::corruption(format!(
            "{context} must be stored as bytes",
        ))),
    }
}

fn decode_len(bytes: &[u8], cursor: &mut usize, context: &str) -> Result<usize, StorageError> {
    let end = cursor.saturating_add(4);
    if end > bytes.len() {
        return Err(StorageError::corruption(format!(
            "{context} is truncated before its length prefix",
        )));
    }

    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(u32::from_be_bytes(raw) as usize)
}

fn decode_utf8_field(
    bytes: &[u8],
    cursor: &mut usize,
    field_len: usize,
    context: &str,
) -> Result<String, StorageError> {
    let end = cursor.saturating_add(field_len);
    if end > bytes.len() {
        return Err(StorageError::corruption(format!(
            "{context} exceeds the encoded kafka source id length",
        )));
    }

    let field = std::str::from_utf8(&bytes[*cursor..end]).map_err(|error| {
        StorageError::corruption(format!("{context} is not valid utf-8: {error}"))
    })?;
    *cursor = end;
    Ok(field.to_string())
}

fn decode_partition_field(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<KafkaPartition, StorageError> {
    let end = cursor.saturating_add(KafkaPartition::ENCODED_LEN);
    if end > bytes.len() {
        return Err(StorageError::corruption(
            "kafka source partition is truncated",
        ));
    }

    let partition = KafkaPartition::decode(&bytes[*cursor..end])?;
    *cursor = end;
    Ok(partition)
}
