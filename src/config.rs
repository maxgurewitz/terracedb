use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    api::{ChangeKind, SchemaDefinition, Value},
    error::StorageError,
    hybrid::{
        HYBRID_COMPACT_TO_WIDE_PROMOTION_METADATA_KEY, HYBRID_TABLE_FEATURES_METADATA_KEY,
        HybridCompactToWidePromotionConfig, HybridProfile, HybridReadConfig, HybridTableFeatures,
    },
    ids::{SequenceNumber, Timestamp},
    scheduler::Scheduler,
    sharding::ShardingConfig,
};

pub type TableMetadata = BTreeMap<String, JsonValue>;

pub trait MergeOperator: Send + Sync {
    fn full_merge(
        &self,
        key: &[u8],
        existing: Option<&Value>,
        operands: &[Value],
    ) -> Result<Value, StorageError>;

    fn partial_merge(
        &self,
        key: &[u8],
        left: &Value,
        right: &Value,
    ) -> Result<Option<Value>, StorageError>;
}

pub type MergeOperatorRef = Arc<dyn MergeOperator>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionDecision {
    Keep,
    Remove,
}

#[derive(Clone, Copy, Debug)]
pub struct CompactionDecisionContext<'a> {
    pub level: u32,
    pub key: &'a [u8],
    pub value: Option<&'a Value>,
    pub sequence: SequenceNumber,
    pub kind: ChangeKind,
    pub now: Timestamp,
}

pub trait CompactionFilter: Send + Sync {
    fn decide(&self, ctx: CompactionDecisionContext<'_>) -> CompactionDecision;
}

pub type CompactionFilterRef = Arc<dyn CompactionFilter>;

pub type TtlExpiryExtractor = fn(&Value) -> Option<Timestamp>;

#[derive(Clone, Copy)]
pub struct TtlCompactionFilter {
    expiry_from_value: TtlExpiryExtractor,
}

impl TtlCompactionFilter {
    pub const fn new(expiry_from_value: TtlExpiryExtractor) -> Self {
        Self { expiry_from_value }
    }
}

impl fmt::Debug for TtlCompactionFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TtlCompactionFilter")
    }
}

impl CompactionFilter for TtlCompactionFilter {
    fn decide(&self, ctx: CompactionDecisionContext<'_>) -> CompactionDecision {
        if ctx.kind == ChangeKind::Delete {
            return CompactionDecision::Keep;
        }

        match ctx.value.and_then(|value| (self.expiry_from_value)(value)) {
            Some(expires_at) if expires_at <= ctx.now => CompactionDecision::Remove,
            _ => CompactionDecision::Keep,
        }
    }
}

#[derive(Clone)]
pub struct DbConfig {
    pub storage: StorageConfig,
    pub hybrid_read: HybridReadConfig,
    pub scheduler: Option<Arc<dyn Scheduler>>,
}

impl fmt::Debug for DbConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbConfig")
            .field("storage", &self.storage)
            .field("hybrid_read", &self.hybrid_read)
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TieredLocalRetentionMode {
    #[default]
    Offload,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredStorageConfig {
    pub ssd: SsdConfig,
    pub s3: S3Location,
    pub max_local_bytes: u64,
    pub durability: TieredDurabilityMode,
    pub local_retention: TieredLocalRetentionMode,
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
    pub max_merge_operand_chain_length: Option<u32>,
    pub compaction_filter: Option<CompactionFilterRef>,
    pub bloom_filter_bits_per_key: Option<u32>,
    pub history_retention_sequences: Option<u64>,
    pub compaction_strategy: CompactionStrategy,
    pub schema: Option<SchemaDefinition>,
    pub sharding: ShardingConfig,
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
                "max_merge_operand_chain_length",
                &self.max_merge_operand_chain_length,
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
            .field("sharding", &self.sharding)
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[derive(Clone)]
pub struct ColumnarTableConfigBuilder {
    name: String,
    merge_operator: Option<MergeOperatorRef>,
    max_merge_operand_chain_length: Option<u32>,
    compaction_filter: Option<CompactionFilterRef>,
    bloom_filter_bits_per_key: Option<u32>,
    history_retention_sequences: Option<u64>,
    compaction_strategy: CompactionStrategy,
    schema: SchemaDefinition,
    sharding: ShardingConfig,
    hybrid_features: HybridTableFeatures,
    compact_to_wide_promotion: Option<HybridCompactToWidePromotionConfig>,
}

impl fmt::Debug for ColumnarTableConfigBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ColumnarTableConfigBuilder")
            .field("name", &self.name)
            .field(
                "merge_operator",
                &self.merge_operator.as_ref().map(|_| "<dyn MergeOperator>"),
            )
            .field(
                "max_merge_operand_chain_length",
                &self.max_merge_operand_chain_length,
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
            .field("sharding", &self.sharding)
            .field("hybrid_features", &self.hybrid_features)
            .field("compact_to_wide_promotion", &self.compact_to_wide_promotion)
            .finish()
    }
}

impl ColumnarTableConfigBuilder {
    pub fn new(name: impl Into<String>, schema: SchemaDefinition) -> Self {
        Self {
            name: name.into(),
            merge_operator: None,
            max_merge_operand_chain_length: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: None,
            history_retention_sequences: None,
            compaction_strategy: CompactionStrategy::Leveled,
            schema,
            sharding: ShardingConfig::default(),
            hybrid_features: HybridTableFeatures::default(),
            compact_to_wide_promotion: None,
        }
    }

    pub fn merge_operator(mut self, merge_operator: Option<MergeOperatorRef>) -> Self {
        self.merge_operator = merge_operator;
        self
    }

    pub fn max_merge_operand_chain_length(mut self, value: Option<u32>) -> Self {
        self.max_merge_operand_chain_length = value;
        self
    }

    pub fn compaction_filter(mut self, compaction_filter: Option<CompactionFilterRef>) -> Self {
        self.compaction_filter = compaction_filter;
        self
    }

    pub fn bloom_filter_bits_per_key(mut self, value: Option<u32>) -> Self {
        self.bloom_filter_bits_per_key = value;
        self
    }

    pub fn history_retention_sequences(mut self, value: Option<u64>) -> Self {
        self.history_retention_sequences = value;
        self
    }

    pub fn compaction_strategy(mut self, strategy: CompactionStrategy) -> Self {
        self.compaction_strategy = strategy;
        self
    }

    pub fn sharding(mut self, sharding: ShardingConfig) -> Self {
        self.sharding = sharding;
        self
    }

    pub fn hybrid_features(mut self, features: HybridTableFeatures) -> Self {
        self.hybrid_features = features;
        self
    }

    pub fn compact_to_wide_promotion(
        mut self,
        promotion: Option<HybridCompactToWidePromotionConfig>,
    ) -> Self {
        self.compact_to_wide_promotion = promotion;
        self
    }

    pub fn build(self) -> TableConfig {
        self.build_for_profile(HybridProfile::Base)
    }

    pub fn build_for_profile(self, profile: HybridProfile) -> TableConfig {
        let mut metadata = TableMetadata::default();
        if matches!(profile, HybridProfile::Accelerated) {
            if !self.hybrid_features.skip_indexes.is_empty()
                || !self.hybrid_features.projection_sidecars.is_empty()
            {
                metadata.insert(
                    HYBRID_TABLE_FEATURES_METADATA_KEY.to_string(),
                    serde_json::to_value(self.hybrid_features)
                        .expect("serialize hybrid table features"),
                );
            }
            if let Some(promotion) = self.compact_to_wide_promotion {
                metadata.insert(
                    HYBRID_COMPACT_TO_WIDE_PROMOTION_METADATA_KEY.to_string(),
                    serde_json::to_value(promotion)
                        .expect("serialize compact-to-wide promotion config"),
                );
            }
        }

        TableConfig {
            name: self.name,
            format: TableFormat::Columnar,
            merge_operator: self.merge_operator,
            max_merge_operand_chain_length: self.max_merge_operand_chain_length,
            compaction_filter: self.compaction_filter,
            bloom_filter_bits_per_key: self.bloom_filter_bits_per_key,
            history_retention_sequences: self.history_retention_sequences,
            compaction_strategy: self.compaction_strategy,
            schema: Some(self.schema),
            sharding: self.sharding,
            metadata,
        }
    }
}
