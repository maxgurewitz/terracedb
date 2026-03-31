extern crate self as terracedb;

pub mod adapters;
pub mod api;
pub mod composition;
pub mod config;
pub mod current_state;
pub mod engine;
pub mod error;
pub mod execution;
pub mod failpoints;
pub mod hybrid;
pub mod ids;
pub mod io;
mod metadata_flatbuffers;
pub mod remote;
pub mod scheduler;
#[cfg(test)]
#[allow(dead_code)]
mod simulation;
pub mod stubs;
pub mod telemetry;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod transaction;

pub use adapters::{
    DeterministicRng, FileSystemFailure, FileSystemOperation, LocalDirObjectStore,
    ObjectStoreFailure, ObjectStoreOperation, SimulatedClock, SimulatedFileSystem,
    SimulatedObjectStore, SystemClock, SystemRng, TokioFileSystem,
};
pub use api::{
    BatchOperation, ChangeEntry, ChangeKind, ChangeStream, ColumnarCacheUsageSnapshot,
    ColumnarRecord, CommitOptions, Db, DbBuilder, DbComponents, DbSettings, FieldDefinition,
    FieldType, FieldValue, Key, KeyPrefix, KvStream, NamedColumnarRecord, ReadSet, ReadSetEntry,
    ScanOptions, SchemaDefinition, Snapshot, Table, Value, WatermarkReceiver,
    WatermarkSubscriptionSet, WatermarkUpdate, WriteBatch,
};
pub use composition::{
    DueTimer, DueTimerBatch, DurableCursorStore, DurableOutboxConsumer, DurableTimerSet,
    OutboxBatch, OutboxEntry, OutboxMessage, ScheduledTimer, TransactionalOutbox,
    decode_outbox_entry,
};
pub use config::{
    CompactionDecision, CompactionDecisionContext, CompactionFilter, CompactionFilterRef,
    CompactionStrategy, DbConfig, MergeOperator, MergeOperatorRef, S3Location,
    S3PrimaryStorageConfig, SsdConfig, StorageConfig, TableConfig, TableFormat, TableMetadata,
    TieredDurabilityMode, TieredLocalRetentionMode, TieredStorageConfig, TtlCompactionFilter,
    TtlExpiryExtractor,
};
pub use current_state::{
    CurrentStateCompactionRowRemovalMode, CurrentStateComputedOrderingKey,
    CurrentStateComputedOrderingKeys, CurrentStateCutoffSource, CurrentStateDerivedOnlyReason,
    CurrentStateEffectiveMode, CurrentStateExactnessRequirement, CurrentStateLocalCleanupResult,
    CurrentStateLogicalFloor, CurrentStateLogicalReclaimAction,
    CurrentStateManifestPublicationResult, CurrentStateMissingValuePolicy,
    CurrentStateOperationalSemantics, CurrentStateOperationalSummary, CurrentStateOracleMutation,
    CurrentStateOracleRow, CurrentStateOrderingContract, CurrentStatePhysicalCleanupAction,
    CurrentStatePhysicalReclaimSemantics, CurrentStatePhysicalRetentionMode,
    CurrentStatePhysicalRetentionSeam, CurrentStatePlanner, CurrentStateProjectionOwnedRange,
    CurrentStateRankBoundary, CurrentStateRankRetentionPolicy, CurrentStateRankSource,
    CurrentStateRankSourceRange, CurrentStateRankSourceScope,
    CurrentStateRankedMaterializationSeam, CurrentStateRebuildMode, CurrentStateRebuildSeam,
    CurrentStateRetainedSetSummary, CurrentStateRetentionBackpressure,
    CurrentStateRetentionBackpressureSignal, CurrentStateRetentionConfiguration,
    CurrentStateRetentionContract, CurrentStateRetentionCoordinationContext,
    CurrentStateRetentionCoordinationPlan, CurrentStateRetentionCoordinationStats,
    CurrentStateRetentionCoordinator, CurrentStateRetentionCoordinatorSnapshot,
    CurrentStateRetentionCoordinatorState, CurrentStateRetentionDeferredReason,
    CurrentStateRetentionError, CurrentStateRetentionEvaluation,
    CurrentStateRetentionEvaluationCost, CurrentStateRetentionMembershipChanges,
    CurrentStateRetentionOracle, CurrentStateRetentionOracleSnapshot,
    CurrentStateRetentionPlanPhase, CurrentStateRetentionPolicy, CurrentStateRetentionReason,
    CurrentStateRetentionSemantics, CurrentStateRetentionSkipReason, CurrentStateRetentionStats,
    CurrentStateRetentionStatus, CurrentStateRetentionTarget, CurrentStateSortDirection,
    CurrentStateThresholdCutoff, CurrentStateThresholdRetentionPolicy,
};
pub use error::{
    ChangeFeedError, CommitError, CreateTableError, FlushError, OpenError, ReadError,
    SnapshotTooOld, StorageError, StorageErrorKind, SubscriptionClosed, WriteError,
};
pub use execution::{
    ColocatedDbWorkloadGenerator, ColocatedDbWorkloadSpec, ContentionClass, DbExecutionProfile,
    DomainBackgroundBudget, DomainBudgetCharge, DomainBudgetOracle, DomainBudgetSnapshot,
    DomainCpuBudget, DomainIoBudget, DomainMemoryBudget, DomainTaggedWork, DurabilityClass,
    ExecutionDomainBudget, ExecutionDomainInvariant, ExecutionDomainInvariantSet,
    ExecutionDomainLifecycleEvent, ExecutionDomainLifecycleHook, ExecutionDomainOwner,
    ExecutionDomainPath, ExecutionDomainPlacement, ExecutionDomainSnapshot, ExecutionDomainSpec,
    ExecutionDomainState, ExecutionDomainUsageSnapshot, ExecutionLane, ExecutionLaneBinding,
    InMemoryDomainBudgetOracle, InMemoryResourceManager, PlacementAssignment, PlacementPolicy,
    PlacementRequest, PreferRequestedDomainPolicy, ResourceManager, ResourceManagerSnapshot,
    WorkPlacementRequest, WorkRuntimeTag,
};
#[doc(hidden)]
pub use failpoints::{
    FailpointAction, FailpointHandle, FailpointHit, FailpointMode, FailpointOutcome,
    FailpointRegistry,
};
pub use hybrid::{
    BaseZoneMapPruner, ByteRange, COLUMNAR_BASE_PART_FORMAT_VERSION,
    COLUMNAR_COMPACT_DIGEST_FORMAT_VERSION, COLUMNAR_PROJECTION_SIDECAR_FORMAT_VERSION,
    COLUMNAR_SKIP_INDEX_SIDECAR_FORMAT_VERSION, COLUMNAR_SYNOPSIS_SIDECAR_FORMAT_VERSION,
    COLUMNAR_V2_BASE_PART_FORMAT_VERSION, COLUMNAR_V2_COMPACT_DIGEST_FORMAT_VERSION,
    COLUMNAR_V2_PROJECTION_SIDECAR_FORMAT_VERSION, COLUMNAR_V2_SKIP_INDEX_SIDECAR_FORMAT_VERSION,
    COLUMNAR_V2_SYNOPSIS_SIDECAR_FORMAT_VERSION, ColumnarArtifactKind, ColumnarCompression,
    ColumnarDecodeField, ColumnarDecodeMetadata, ColumnarEncoding, ColumnarFooter,
    ColumnarFooterPageDirectoryLoader, ColumnarFormatTag, ColumnarGranuleRef,
    ColumnarGranuleSelection, ColumnarGranuleSynopsis, ColumnarHeader, ColumnarMark,
    ColumnarMarkOffset, ColumnarOptionalSidecar, ColumnarPageDirectory, ColumnarPageRef,
    ColumnarPruningOutcome, ColumnarPruningStats, ColumnarSequenceBounds, ColumnarSubstreamKind,
    ColumnarSubstreamRef, ColumnarSynopsisSidecar, ColumnarV2ArtifactKind, ColumnarV2Compression,
    ColumnarV2Encoding, ColumnarV2Footer, ColumnarV2FormatTag, ColumnarV2Header, ColumnarV2Mark,
    ColumnarV2MarkOffset, ColumnarV2PageDirectory, ColumnarV2PageRef, ColumnarV2SubstreamKind,
    ColumnarV2SubstreamRef, CompactPartDigest, CompactToWidePromotionCandidate,
    CompactToWidePromotionDecision, CompactToWidePromotionPolicy, ConservativeCompactToWidePolicy,
    HYBRID_COMPACT_TO_WIDE_PROMOTION_METADATA_KEY, HYBRID_TABLE_FEATURES_METADATA_KEY,
    HybridCompactToWidePromotionConfig, HybridKeyRange, HybridPartDescriptor,
    HybridProjectionSidecarConfig, HybridReadConfig, HybridRowRef, HybridRuntimeContext,
    HybridSkipIndexConfig, HybridSkipIndexFamily, HybridSynopsisPruner, HybridTableFeatures,
    InMemoryRawByteSegmentCache, LateMaterializationPlan, PartDigestAlgorithm,
    PartRepairController, ProjectionSidecarDescriptor, RawByteSegmentCache, RepairState,
    RowProjection, RowRefBatch, RowRefBatchIterator, SelectionMask, SkipIndexProbe,
    SkipIndexProbeResult, SkipIndexSidecarDescriptor, StubColumnarFooterPageDirectoryLoader,
    StubPartRepairController, SurvivorSet, ZoneMapPredicate, ZoneMapSynopsis,
};
pub use ids::{
    CommitId, FieldId, IdEncodingError, LogCursor, ManifestId, SegmentId, SequenceNumber, TableId,
    Timestamp,
};
pub use io::{
    Clock, DbDependencies, FileHandle, FileSystem, ObjectStore, OpenOptions, Rng,
    StandardObjectPath, StandardObjectStore, StandardObjectStoreExt,
};
pub use remote::{
    CacheSpan, ObjectKeyLayout, RemoteCache, RemoteOperation, RemoteRecoveryHint,
    RemoteStorageError, StorageSource, UnifiedStorage, UnifiedStorageError,
};
pub use scheduler::{
    DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT, DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT, NoopScheduler,
    PendingWork, PendingWorkBudget, PendingWorkBudgetBlockReason, PendingWorkType,
    RoundRobinScheduler, ScheduleAction, ScheduleDecision, Scheduler,
    SchedulerObservabilitySnapshot, TableStats, ThrottleDecision,
};
pub use stubs::{StubClock, StubFileSystem, StubObjectStore, StubRng};
pub use telemetry::{
    OperationContext, SpanRelation, add_span_event, db_instance_from_storage, db_name_from_storage,
    set_span_attribute, set_span_attributes, storage_mode_name, telemetry_attrs,
};
#[cfg(any(test, feature = "test-support"))]
pub use test_support::{
    bytes as test_bytes, row_table_config, test_dependencies, test_dependencies_with_clock,
    tiered_test_config, tiered_test_config_with_durability,
};
pub use transaction::{Transaction, TransactionCommitError, TransactionCommitOptions};
