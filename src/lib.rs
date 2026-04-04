extern crate self as terracedb;

pub mod adapters;
pub mod api;
pub mod composition;
pub mod config;
pub mod current_state;
mod durable_formats;
pub mod engine;
pub mod error;
pub mod execution;
pub mod failpoints;
pub mod hybrid;
pub mod ids;
pub mod io;
pub mod pressure;
pub mod remote;
pub mod scheduler;
pub mod sharding;
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
    ColumnarCacheUsageSubscription, ColumnarRecord, ColumnarScanExecution,
    ColumnarScanPartExecution, CommitOptions, Db, DbBuilder, DbComponents, DbProgressSnapshot,
    DbProgressSubscription, DbSettings, DomainColumnarCacheUsageSnapshot, FieldDefinition,
    FieldType, FieldValue, FlushStatus, Key, KeyPrefix, KvStream, NamedColumnarRecord, ReadSet,
    ReadSetEntry, RowScanExecution, ScanExecution, ScanMaterializationSource, ScanOptions,
    ScanPredicate, SchemaDefinition, Snapshot, Table, Value, WatermarkReceiver,
    WatermarkSubscriptionSet, WatermarkUpdate, WriteBatch,
};
pub use composition::{
    DueTimer, DueTimerBatch, DurableCursorStore, DurableOutboxConsumer, DurableTimerSet,
    OutboxBatch, OutboxEntry, OutboxMessage, ScheduledTimer, TransactionalOutbox,
    decode_outbox_entry,
};
pub use config::{
    ColumnarTableConfigBuilder, CompactionDecision, CompactionDecisionContext, CompactionFilter,
    CompactionFilterRef, CompactionStrategy, DbConfig, MergeOperator, MergeOperatorRef,
    RowTableConfigBuilder, S3Location, S3PrimaryStorageConfig, SsdConfig, StorageConfig,
    TableConfig, TableFormat, TableMetadata, TieredDurabilityMode, TieredLocalRetentionMode,
    TieredStorageConfig, TtlCompactionFilter, TtlExpiryExtractor,
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
    AdmissionObservationRecvError, ChangeFeedError, CommitError, CreateTableError, FlushError,
    OpenError, ReadError, SnapshotTooOld, StorageError, StorageErrorKind, SubscriptionClosed,
    WriteError,
};
pub use execution::{
    ColocatedDatabasePlacement, ColocatedDbWorkloadGenerator, ColocatedDbWorkloadSpec,
    ColocatedDeployment, ColocatedDeploymentBuilder, ColocatedDeploymentError,
    ColocatedDeploymentPlacementPolicy, ColocatedDeploymentReport, ColocatedSubsystemPlacement,
    ContentionClass, DbExecutionPlacementReport, DbExecutionProfile, DomainBackgroundBudget,
    DomainBudgetCharge, DomainBudgetOracle, DomainBudgetSnapshot, DomainCpuBudget, DomainIoBudget,
    DomainMemoryBudget, DomainTaggedWork, DurabilityClass, ExecutionBacklogGuard,
    ExecutionBacklogPublication, ExecutionBacklogPublisher, ExecutionDomainBacklogSnapshot,
    ExecutionDomainBudget, ExecutionDomainContentionSnapshot, ExecutionDomainInvariant,
    ExecutionDomainInvariantSet, ExecutionDomainLifecycleEvent, ExecutionDomainLifecycleHook,
    ExecutionDomainOwner, ExecutionDomainPath, ExecutionDomainPlacement, ExecutionDomainSnapshot,
    ExecutionDomainSpec, ExecutionDomainState, ExecutionDomainUsageSnapshot, ExecutionLane,
    ExecutionLaneBinding, ExecutionLanePlacementConfig, ExecutionPlacementDecision,
    ExecutionResourceKind, ExecutionResourceUsage, ExecutionUsageLease, ExecutionUsageReleaseError,
    InMemoryDomainBudgetOracle, InMemoryResourceManager, PlacementAssignment, PlacementPolicy,
    PlacementRequest, PlacementTarget, PreferRequestedDomainPolicy, ResourceAdmissionDecision,
    ResourceManager, ResourceManagerSnapshot, ResourceManagerSubscription,
    ShardExecutionDomainProfile, ShardReadyPlacementLayout, WorkPlacementRequest, WorkRuntimeTag,
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
    HybridCompactToWidePromotionConfig, HybridKeyRange, HybridPartDescriptor, HybridProfile,
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
pub use pressure::{
    AdmissionCorrectnessContext, AdmissionDiagnostics, AdmissionPolicyProfile,
    AdmissionPressureLevel, AdmissionPressureSignal, AdmissionSignals, FlushPressureCandidate,
    PressureBudget, PressureBytes, PressureScope, PressureStats,
    carry_write_delay_across_maintenance, derive_pressure_budget, multi_signal_write_admission,
};
pub use remote::{
    CacheSpan, ObjectKeyLayout, RemoteCache, RemoteCacheEntrySnapshot, RemoteCacheFetchKind,
    RemoteCacheInFlightSnapshot, RemoteCacheProgressSnapshot, RemoteCacheProgressSubscription,
    RemoteOperation, RemoteRecoveryHint, RemoteStorageError, StorageSource, UnifiedStorage,
    UnifiedStorageError,
};
pub use scheduler::{
    AdmissionObservation, AdmissionObservationReceiver, DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT,
    DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT, DomainPriorityOverride, NoopScheduler, PendingWork,
    PendingWorkBudget, PendingWorkBudgetBlockReason, PendingWorkType, RecordedAdmissionDiagnostics,
    RoundRobinScheduler, ScheduleAction, ScheduleDecision, Scheduler, SchedulerIdleWaitResult,
    SchedulerObservabilitySnapshot, SchedulerObservabilitySubscription, SchedulerProgressSnapshot,
    SchedulerProgressStepResult, TableStats, ThrottleDecision,
};
pub use sharding::{
    BatchShardLocalityError, HashShardingConfig, KeyShardRoute, PhysicalShardId,
    PublishShardMapError, ReshardPartitionMove, ReshardPlanError, ReshardPlanPhase,
    ReshardPlanSkeleton, ShardAssignment, ShardChangeCursor, ShardCommitLaneId,
    ShardExecutionPlacement, ShardHashAlgorithm, ShardMapRevision, ShardMemtableOwner,
    ShardOpenRequest, ShardSstableOwnership, ShardingConfig, ShardingError, TableBatchShardingPlan,
    TableReshardingState, TableShardingState, VirtualPartitionCoverage, VirtualPartitionId,
    VirtualPartitionRange, WriteBatchShardingError, WriteBatchShardingPlan,
};
pub use stubs::{StubClock, StubFileSystem, StubObjectStore, StubRng};
pub use telemetry::{
    OperationContext, SpanRelation, add_span_event, db_instance_from_storage, db_name_from_storage,
    set_span_attribute, set_span_attributes, storage_mode_name, telemetry_attrs,
};
#[cfg(any(test, feature = "test-support"))]
pub use test_support::{
    ClockProgressProbe, bytes as test_bytes, row_table_config, test_dependencies,
    test_dependencies_with_clock, tiered_test_config, tiered_test_config_with_durability,
};
pub use transaction::{Transaction, TransactionCommitError, TransactionCommitOptions};
