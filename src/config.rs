use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    api::{SchemaDefinition, Value},
    error::StorageError,
    ids::{SequenceNumber, Timestamp},
    scheduler::Scheduler,
};

pub type TableMetadata = BTreeMap<String, JsonValue>;

pub trait MergeOperator: Send + Sync {
    fn merge(
        &self,
        key: &[u8],
        existing: Option<&Value>,
        delta: &Value,
    ) -> Result<Value, StorageError>;
}

pub type MergeOperatorRef = Arc<dyn MergeOperator>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionDecision {
    Keep,
    Remove,
}

#[derive(Clone, Copy, Debug)]
pub struct CompactionDecisionContext<'a> {
    pub key: &'a [u8],
    pub value: &'a Value,
    pub sequence: SequenceNumber,
    pub now: Timestamp,
}

pub trait CompactionFilter: Send + Sync {
    fn decide(&self, ctx: CompactionDecisionContext<'_>) -> CompactionDecision;
}

pub type CompactionFilterRef = Arc<dyn CompactionFilter>;

#[derive(Clone)]
pub struct DbConfig {
    pub storage: StorageConfig,
    pub scheduler: Option<Arc<dyn Scheduler>>,
}

impl fmt::Debug for DbConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbConfig")
            .field("storage", &self.storage)
            .field(
                "scheduler",
                &self.scheduler.as_ref().map(|_| "<dyn Scheduler>"),
            )
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageConfig {
    Tiered(TieredStorageConfig),
    S3Primary(S3PrimaryStorageConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SsdConfig {
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Location {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TieredDurabilityMode {
    #[default]
    GroupCommit,
    Deferred,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredStorageConfig {
    pub ssd: SsdConfig,
    pub s3: S3Location,
    pub max_local_bytes: u64,
    pub durability: TieredDurabilityMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3PrimaryStorageConfig {
    pub s3: S3Location,
    pub mem_cache_size_bytes: u64,
    pub auto_flush_interval: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableFormat {
    Row,
    Columnar,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompactionStrategy {
    Leveled,
    Tiered,
    Fifo,
}

#[derive(Clone)]
pub struct TableConfig {
    pub name: String,
    pub format: TableFormat,
    pub merge_operator: Option<MergeOperatorRef>,
    pub compaction_filter: Option<CompactionFilterRef>,
    pub bloom_filter_bits_per_key: Option<u32>,
    pub history_retention_sequences: Option<u64>,
    pub compaction_strategy: CompactionStrategy,
    pub schema: Option<SchemaDefinition>,
    pub metadata: TableMetadata,
}

impl fmt::Debug for TableConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableConfig")
            .field("name", &self.name)
            .field("format", &self.format)
            .field(
                "merge_operator",
                &self.merge_operator.as_ref().map(|_| "<dyn MergeOperator>"),
            )
            .field(
                "compaction_filter",
                &self
                    .compaction_filter
                    .as_ref()
                    .map(|_| "<dyn CompactionFilter>"),
            )
            .field("bloom_filter_bits_per_key", &self.bloom_filter_bits_per_key)
            .field(
                "history_retention_sequences",
                &self.history_retention_sequences,
            )
            .field("compaction_strategy", &self.compaction_strategy)
            .field("schema", &self.schema)
            .field("metadata", &self.metadata)
            .finish()
    }
}
