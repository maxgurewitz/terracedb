use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use crc32fast::hash as crc32_hash;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CommitId, ExecutionDomainOwner, ExecutionDomainPath, ExecutionLane, LogCursor,
    ShardReadyPlacementLayout, StorageError, TableConfig, TableId, TableMetadata,
};

pub(crate) const SHARDING_METADATA_KEY: &str = "terracedb.sharding";

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct VirtualPartitionId(u32);

impl VirtualPartitionId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for VirtualPartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PhysicalShardId(u32);

impl PhysicalShardId {
    pub const UNSHARDED: Self = Self(0);

    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn as_dir_name(self) -> String {
        self.to_string()
    }

    pub fn execution_placement(
        self,
        layout: &ShardReadyPlacementLayout,
    ) -> ShardExecutionPlacement {
        ShardExecutionPlacement {
            physical_shard: self,
            owner: layout.shard_owner(self.to_string()),
            foreground: layout
                .future_shard_lane_path(self.to_string(), ExecutionLane::UserForeground),
            background: layout
                .future_shard_lane_path(self.to_string(), ExecutionLane::UserBackground),
            control_plane: layout
                .future_shard_lane_path(self.to_string(), ExecutionLane::ControlPlane),
        }
    }
}

impl fmt::Display for PhysicalShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}", self.0)
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ShardMapRevision(u64);

impl ShardMapRevision {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ShardMapRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardHashAlgorithm {
    #[default]
    Crc32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashShardingConfig {
    pub virtual_partition_count: u32,
    pub hash_algorithm: ShardHashAlgorithm,
    pub shard_map_revision: ShardMapRevision,
    pub physical_shards_by_virtual_partition: Vec<PhysicalShardId>,
}

impl HashShardingConfig {
    pub fn new(
        virtual_partition_count: u32,
        hash_algorithm: ShardHashAlgorithm,
        shard_map_revision: ShardMapRevision,
        physical_shards_by_virtual_partition: Vec<PhysicalShardId>,
    ) -> Result<Self, ShardingError> {
        let config = Self {
            virtual_partition_count,
            hash_algorithm,
            shard_map_revision,
            physical_shards_by_virtual_partition,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ShardingError> {
        if self.virtual_partition_count == 0 {
            return Err(ShardingError::ZeroVirtualPartitions);
        }
        if self.physical_shards_by_virtual_partition.len() != self.virtual_partition_count as usize
        {
            return Err(ShardingError::AssignmentCountMismatch {
                virtual_partition_count: self.virtual_partition_count,
                actual: self.physical_shards_by_virtual_partition.len(),
            });
        }
        Ok(())
    }

    pub fn shard_assignments(&self) -> Vec<ShardAssignment> {
        self.physical_shards_by_virtual_partition
            .iter()
            .copied()
            .enumerate()
            .map(|(index, physical_shard)| ShardAssignment {
                virtual_partition: VirtualPartitionId::new(index as u32),
                physical_shard,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ShardingConfig {
    #[default]
    Unsharded,
    Hash(HashShardingConfig),
}

impl ShardingConfig {
    pub const fn unsharded() -> Self {
        Self::Unsharded
    }

    pub fn hash(
        virtual_partition_count: u32,
        hash_algorithm: ShardHashAlgorithm,
        shard_map_revision: ShardMapRevision,
        physical_shards_by_virtual_partition: Vec<PhysicalShardId>,
    ) -> Result<Self, ShardingError> {
        Ok(Self::Hash(HashShardingConfig::new(
            virtual_partition_count,
            hash_algorithm,
            shard_map_revision,
            physical_shards_by_virtual_partition,
        )?))
    }

    pub fn validate(&self) -> Result<(), ShardingError> {
        match self {
            Self::Unsharded => Ok(()),
            Self::Hash(config) => config.validate(),
        }
    }

    pub fn is_sharded(&self) -> bool {
        matches!(self, Self::Hash(_))
    }

    pub fn virtual_partition_count(&self) -> u32 {
        match self {
            Self::Unsharded => 1,
            Self::Hash(config) => config.virtual_partition_count,
        }
    }

    pub fn hash_algorithm(&self) -> Option<ShardHashAlgorithm> {
        match self {
            Self::Unsharded => None,
            Self::Hash(config) => Some(config.hash_algorithm),
        }
    }

    pub fn current_revision(&self) -> ShardMapRevision {
        match self {
            Self::Unsharded => ShardMapRevision::default(),
            Self::Hash(config) => config.shard_map_revision,
        }
    }

    pub fn shard_assignments(&self) -> Result<Vec<ShardAssignment>, ShardingError> {
        self.validate()?;
        Ok(match self {
            Self::Unsharded => vec![ShardAssignment {
                virtual_partition: VirtualPartitionId::new(0),
                physical_shard: PhysicalShardId::UNSHARDED,
            }],
            Self::Hash(config) => config.shard_assignments(),
        })
    }

    pub fn physical_shards(&self) -> Result<BTreeSet<PhysicalShardId>, ShardingError> {
        Ok(self
            .shard_assignments()?
            .into_iter()
            .map(|assignment| assignment.physical_shard)
            .collect())
    }

    pub fn partition_counts_per_shard(
        &self,
    ) -> Result<BTreeMap<PhysicalShardId, usize>, ShardingError> {
        let mut counts = BTreeMap::new();
        for assignment in self.shard_assignments()? {
            *counts.entry(assignment.physical_shard).or_default() += 1;
        }
        Ok(counts)
    }

    pub fn physical_shard_for_partition(
        &self,
        partition: VirtualPartitionId,
    ) -> Result<PhysicalShardId, ShardingError> {
        self.validate()?;
        match self {
            Self::Unsharded => Ok(PhysicalShardId::UNSHARDED),
            Self::Hash(config) => config
                .physical_shards_by_virtual_partition
                .get(partition.get() as usize)
                .copied()
                .ok_or(ShardingError::PartitionOutOfRange {
                    partition,
                    virtual_partition_count: config.virtual_partition_count,
                }),
        }
    }

    pub fn route_key(&self, key: &[u8]) -> Result<KeyShardRoute, ShardingError> {
        self.validate()?;
        match self {
            Self::Unsharded => Ok(KeyShardRoute {
                virtual_partition: VirtualPartitionId::new(0),
                physical_shard: PhysicalShardId::UNSHARDED,
                shard_map_revision: ShardMapRevision::default(),
            }),
            Self::Hash(config) => {
                let hash = match config.hash_algorithm {
                    ShardHashAlgorithm::Crc32 => crc32_hash(key),
                };
                let partition = VirtualPartitionId::new(hash % config.virtual_partition_count);
                Ok(KeyShardRoute {
                    virtual_partition: partition,
                    physical_shard: config.physical_shards_by_virtual_partition
                        [partition.get() as usize],
                    shard_map_revision: config.shard_map_revision,
                })
            }
        }
    }

    pub fn compatible_reshard_identity(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Unsharded, Self::Unsharded) => true,
            (Self::Hash(left), Self::Hash(right)) => {
                left.virtual_partition_count == right.virtual_partition_count
                    && left.hash_algorithm == right.hash_algorithm
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyShardRoute {
    pub virtual_partition: VirtualPartitionId,
    pub physical_shard: PhysicalShardId,
    pub shard_map_revision: ShardMapRevision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardAssignment {
    pub virtual_partition: VirtualPartitionId,
    pub physical_shard: PhysicalShardId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableShardingState {
    pub table_id: TableId,
    pub table_name: String,
    pub config: ShardingConfig,
}

impl TableShardingState {
    pub fn new(table_id: TableId, config: &TableConfig) -> Result<Self, ShardingError> {
        validate_user_table_metadata(&config.metadata)?;
        config.sharding.validate()?;
        Ok(Self {
            table_id,
            table_name: config.name.clone(),
            config: config.sharding.clone(),
        })
    }

    pub fn current_revision(&self) -> ShardMapRevision {
        self.config.current_revision()
    }

    pub fn partition_counts_per_shard(&self) -> BTreeMap<PhysicalShardId, usize> {
        self.config
            .partition_counts_per_shard()
            .expect("table sharding state should only exist for validated configs")
    }

    pub fn route_key(&self, key: &[u8]) -> KeyShardRoute {
        self.config
            .route_key(key)
            .expect("table sharding state should only exist for validated configs")
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableBatchShardingPlan {
    pub table_id: TableId,
    pub table_name: String,
    pub shard_map_revision: ShardMapRevision,
    pub physical_shards: BTreeSet<PhysicalShardId>,
    pub virtual_partitions: BTreeSet<VirtualPartitionId>,
}

impl TableBatchShardingPlan {
    pub fn single_shard(&self) -> Option<PhysicalShardId> {
        (self.physical_shards.len() == 1)
            .then(|| self.physical_shards.iter().copied().next())
            .flatten()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatchShardingPlan {
    pub tables: Vec<TableBatchShardingPlan>,
}

impl WriteBatchShardingPlan {
    pub fn commit_shard_hint(&self) -> Option<PhysicalShardId> {
        let mut shards = self
            .tables
            .iter()
            .flat_map(|table| table.physical_shards.iter().copied())
            .collect::<BTreeSet<_>>();
        (shards.len() == 1).then(|| shards.pop_first()).flatten()
    }

    pub fn table(&self, table_name: &str) -> Option<&TableBatchShardingPlan> {
        self.tables
            .iter()
            .find(|table| table.table_name == table_name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum WriteBatchShardingError {
    #[error("table does not exist: {table_name}")]
    MissingTable { table_name: String },
    #[error("table {table_name} has invalid sharding metadata: {message}")]
    InvalidTableConfig { table_name: String, message: String },
    #[error(transparent)]
    Locality(#[from] BatchShardLocalityError),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error(
    "write batch for table {table_name} spans physical shards {first_shard} and {conflicting_shard} \
     via virtual partitions {first_virtual_partition} and {conflicting_virtual_partition}"
)]
pub struct BatchShardLocalityError {
    pub table_name: String,
    pub first_shard: PhysicalShardId,
    pub conflicting_shard: PhysicalShardId,
    pub first_virtual_partition: VirtualPartitionId,
    pub conflicting_virtual_partition: VirtualPartitionId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VirtualPartitionRange {
    pub start: VirtualPartitionId,
    pub end_inclusive: VirtualPartitionId,
}

impl VirtualPartitionRange {
    pub const fn new(start: VirtualPartitionId, end_inclusive: VirtualPartitionId) -> Self {
        Self {
            start,
            end_inclusive,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualPartitionCoverage {
    pub ranges: Vec<VirtualPartitionRange>,
}

impl VirtualPartitionCoverage {
    pub fn single(partition: VirtualPartitionId) -> Self {
        Self {
            ranges: vec![VirtualPartitionRange::new(partition, partition)],
        }
    }

    pub fn full_table(config: &ShardingConfig) -> Self {
        let last = config.virtual_partition_count().saturating_sub(1);
        Self {
            ranges: vec![VirtualPartitionRange::new(
                VirtualPartitionId::new(0),
                VirtualPartitionId::new(last),
            )],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCommitLaneId {
    pub table_id: TableId,
    pub physical_shard: PhysicalShardId,
}

impl ShardCommitLaneId {
    pub const fn new(table_id: TableId, physical_shard: PhysicalShardId) -> Self {
        Self {
            table_id,
            physical_shard,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMemtableOwner {
    pub table_id: TableId,
    pub physical_shard: PhysicalShardId,
    pub shard_map_revision: ShardMapRevision,
}

impl ShardMemtableOwner {
    pub const fn new(
        table_id: TableId,
        physical_shard: PhysicalShardId,
        shard_map_revision: ShardMapRevision,
    ) -> Self {
        Self {
            table_id,
            physical_shard,
            shard_map_revision,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardSstableOwnership {
    pub table_id: TableId,
    pub physical_shard: PhysicalShardId,
    pub shard_map_revision: ShardMapRevision,
    pub virtual_partitions: VirtualPartitionCoverage,
}

impl ShardSstableOwnership {
    pub fn new(
        table_id: TableId,
        physical_shard: PhysicalShardId,
        shard_map_revision: ShardMapRevision,
        virtual_partitions: VirtualPartitionCoverage,
    ) -> Self {
        Self {
            table_id,
            physical_shard,
            shard_map_revision,
            virtual_partitions,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardOpenRequest {
    pub table_id: TableId,
    pub physical_shard: PhysicalShardId,
    pub shard_map_revision: ShardMapRevision,
}

impl ShardOpenRequest {
    pub const fn new(
        table_id: TableId,
        physical_shard: PhysicalShardId,
        shard_map_revision: ShardMapRevision,
    ) -> Self {
        Self {
            table_id,
            physical_shard,
            shard_map_revision,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardChangeCursor {
    pub table_id: TableId,
    pub physical_shard: PhysicalShardId,
    pub commit_id: CommitId,
    pub cursor: LogCursor,
}

impl ShardChangeCursor {
    pub const fn new(
        table_id: TableId,
        physical_shard: PhysicalShardId,
        commit_id: CommitId,
        cursor: LogCursor,
    ) -> Self {
        Self {
            table_id,
            physical_shard,
            commit_id,
            cursor,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardExecutionPlacement {
    pub physical_shard: PhysicalShardId,
    pub owner: ExecutionDomainOwner,
    pub foreground: ExecutionDomainPath,
    pub background: ExecutionDomainPath,
    pub control_plane: ExecutionDomainPath,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReshardPartitionMove {
    pub virtual_partition: VirtualPartitionId,
    pub from_physical_shard: PhysicalShardId,
    pub to_physical_shard: PhysicalShardId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReshardPlanSkeleton {
    pub table_id: TableId,
    pub table_name: String,
    pub source_revision: ShardMapRevision,
    pub target_revision: ShardMapRevision,
    pub moves: Vec<ReshardPartitionMove>,
}

impl ReshardPlanSkeleton {
    pub fn build(
        table_id: TableId,
        table_name: impl Into<String>,
        source: &ShardingConfig,
        target: &ShardingConfig,
    ) -> Result<Self, ShardingError> {
        source.validate()?;
        target.validate()?;
        if !source.compatible_reshard_identity(target) {
            return Err(ShardingError::IncompatibleReshardIdentity {
                source_virtual_partition_count: source.virtual_partition_count(),
                target_virtual_partition_count: target.virtual_partition_count(),
                source_hash_algorithm: source.hash_algorithm(),
                target_hash_algorithm: target.hash_algorithm(),
            });
        }

        let moves = source
            .shard_assignments()?
            .into_iter()
            .zip(target.shard_assignments()?)
            .filter_map(|(left, right)| {
                (left.physical_shard != right.physical_shard).then_some(ReshardPartitionMove {
                    virtual_partition: left.virtual_partition,
                    from_physical_shard: left.physical_shard,
                    to_physical_shard: right.physical_shard,
                })
            })
            .collect();

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            source_revision: source.current_revision(),
            target_revision: target.current_revision(),
            moves,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ShardingError {
    #[error("table metadata key {key} is reserved for terracedb sharding")]
    ReservedMetadataKey { key: String },
    #[error("sharded tables require at least one virtual partition")]
    ZeroVirtualPartitions,
    #[error(
        "sharded table declares {virtual_partition_count} virtual partitions but supplies {actual} shard assignments"
    )]
    AssignmentCountMismatch {
        virtual_partition_count: u32,
        actual: usize,
    },
    #[error(
        "virtual partition {partition} is out of range for table with {virtual_partition_count} partitions"
    )]
    PartitionOutOfRange {
        partition: VirtualPartitionId,
        virtual_partition_count: u32,
    },
    #[error(
        "reshard skeleton requires matching virtual partition counts and hash identities; \
         source has {source_virtual_partition_count} partitions and {source_hash_algorithm:?}, \
         target has {target_virtual_partition_count} partitions and {target_hash_algorithm:?}"
    )]
    IncompatibleReshardIdentity {
        source_virtual_partition_count: u32,
        target_virtual_partition_count: u32,
        source_hash_algorithm: Option<ShardHashAlgorithm>,
        target_hash_algorithm: Option<ShardHashAlgorithm>,
    },
    #[error("decode terracedb sharding metadata failed: {message}")]
    InvalidPersistedMetadata { message: String },
}

pub(crate) fn validate_user_table_metadata(metadata: &TableMetadata) -> Result<(), ShardingError> {
    if metadata.contains_key(SHARDING_METADATA_KEY) {
        return Err(ShardingError::ReservedMetadataKey {
            key: SHARDING_METADATA_KEY.to_string(),
        });
    }
    Ok(())
}

pub(crate) fn encode_persisted_table_metadata(
    metadata: &TableMetadata,
    sharding: &ShardingConfig,
) -> Result<TableMetadata, StorageError> {
    validate_user_table_metadata(metadata)
        .map_err(|error| StorageError::unsupported(error.to_string()))?;
    sharding
        .validate()
        .map_err(|error| StorageError::unsupported(error.to_string()))?;

    let mut persisted = metadata.clone();
    if !matches!(sharding, ShardingConfig::Unsharded) {
        let encoded = serde_json::to_value(sharding).map_err(|error| {
            StorageError::unsupported(format!(
                "encode terracedb sharding metadata failed: {error}"
            ))
        })?;
        persisted.insert(SHARDING_METADATA_KEY.to_string(), encoded);
    }
    Ok(persisted)
}

pub(crate) fn decode_persisted_table_metadata(
    mut metadata: TableMetadata,
) -> Result<(TableMetadata, ShardingConfig), StorageError> {
    let Some(encoded) = metadata.remove(SHARDING_METADATA_KEY) else {
        return Ok((metadata, ShardingConfig::default()));
    };

    let sharding: ShardingConfig = serde_json::from_value(encoded).map_err(|error| {
        StorageError::corruption(format!(
            "{}",
            ShardingError::InvalidPersistedMetadata {
                message: error.to_string(),
            }
        ))
    })?;
    sharding.validate().map_err(|error: ShardingError| {
        StorageError::corruption(format!(
            "{}",
            ShardingError::InvalidPersistedMetadata {
                message: error.to_string(),
            }
        ))
    })?;
    Ok((metadata, sharding))
}
