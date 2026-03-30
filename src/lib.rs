extern crate self as terracedb;

pub mod adapters;
pub mod api;
pub mod composition;
pub mod config;
pub mod engine;
pub mod error;
pub mod ids;
pub mod io;
pub mod remote;
pub mod scheduler;
#[cfg(test)]
#[allow(dead_code)]
mod simulation;
pub mod stubs;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod transaction;

pub use adapters::{
    DeterministicRng, FileSystemFailure, FileSystemOperation, LocalDirObjectStore,
    ObjectStoreFailure, ObjectStoreOperation, SimulatedClock, SimulatedFileSystem,
    SimulatedObjectStore, SystemClock, SystemRng, TokioFileSystem,
};
pub use api::{
    BatchOperation, ChangeEntry, ChangeKind, ChangeStream, ColumnarRecord, CommitOptions, Db,
    FieldDefinition, FieldType, FieldValue, Key, KeyPrefix, KvStream, NamedColumnarRecord, ReadSet,
    ReadSetEntry, ScanOptions, SchemaDefinition, Snapshot, Table, Value, WatermarkReceiver,
    WatermarkSubscriptionSet, WatermarkUpdate, WriteBatch,
};
pub use composition::{
    DueTimer, DueTimerBatch, DurableCursorStore, DurableOutboxConsumer, DurableTimerSet,
    OutboxBatch, OutboxEntry, OutboxMessage, ScheduledTimer, TransactionalOutbox,
};
pub use config::{
    CompactionDecision, CompactionDecisionContext, CompactionFilter, CompactionFilterRef,
    CompactionStrategy, DbConfig, MergeOperator, MergeOperatorRef, S3Location,
    S3PrimaryStorageConfig, SsdConfig, StorageConfig, TableConfig, TableFormat, TableMetadata,
    TieredDurabilityMode, TieredStorageConfig, TtlCompactionFilter, TtlExpiryExtractor,
};
pub use error::{
    CommitError, CreateTableError, FlushError, OpenError, ReadError, SnapshotTooOld, StorageError,
    StorageErrorKind, SubscriptionClosed, WriteError,
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
    PendingWork, PendingWorkType, RoundRobinScheduler, ScheduleAction, ScheduleDecision, Scheduler,
    TableStats, ThrottleDecision,
};
pub use stubs::{StubClock, StubFileSystem, StubObjectStore, StubRng};
#[cfg(any(test, feature = "test-support"))]
pub use test_support::{
    bytes as test_bytes, row_table_config, test_dependencies, test_dependencies_with_clock,
    tiered_test_config, tiered_test_config_with_durability,
};
pub use transaction::{Transaction, TransactionCommitError, TransactionCommitOptions};
