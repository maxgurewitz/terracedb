use async_trait::async_trait;

use crate::{
    api::{BatchOperation, ChangeStream, ScanOptions, Table, WriteBatch},
    config::TableConfig,
    error::{CommitError, CreateTableError, OpenError, SnapshotTooOld, StorageError},
    ids::{LogCursor, ManifestId, SegmentId, SequenceNumber, TableId},
    scheduler::{PendingWork, ScheduleDecision, TableStats},
};

pub mod catalog {
    use super::*;

    #[derive(Clone, Debug)]
    pub struct CatalogEntry {
        pub id: TableId,
        pub config: TableConfig,
    }

    #[async_trait]
    pub trait CatalogStore: Send + Sync {
        async fn load(&self) -> Result<Vec<CatalogEntry>, OpenError>;
        async fn create_table(&self, table: CatalogEntry) -> Result<(), CreateTableError>;
    }

    #[derive(Debug, Default)]
    pub struct StubCatalogStore;

    #[async_trait]
    impl CatalogStore for StubCatalogStore {
        async fn load(&self) -> Result<Vec<CatalogEntry>, OpenError> {
            Ok(Vec::new())
        }

        async fn create_table(&self, _table: CatalogEntry) -> Result<(), CreateTableError> {
            Ok(())
        }
    }
}

pub mod memtable {
    use super::*;

    #[derive(Clone, Debug, Default)]
    pub struct MemtableSnapshot {
        pub visible_sequence: SequenceNumber,
    }

    #[async_trait]
    pub trait MemtableStore: Send + Sync {
        async fn apply_batch(
            &self,
            sequence: SequenceNumber,
            operations: &[BatchOperation],
        ) -> Result<(), CommitError>;
        async fn snapshot(&self, at: SequenceNumber) -> Result<MemtableSnapshot, StorageError>;
    }

    #[derive(Debug, Default)]
    pub struct StubMemtableStore;

    #[async_trait]
    impl MemtableStore for StubMemtableStore {
        async fn apply_batch(
            &self,
            _sequence: SequenceNumber,
            _operations: &[BatchOperation],
        ) -> Result<(), CommitError> {
            Ok(())
        }

        async fn snapshot(&self, at: SequenceNumber) -> Result<MemtableSnapshot, StorageError> {
            Ok(MemtableSnapshot {
                visible_sequence: at,
            })
        }
    }
}

pub mod commit_log;

pub mod sstables {
    use super::*;

    #[derive(Clone, Debug)]
    pub struct SstableRef {
        pub table_id: TableId,
        pub level: u32,
        pub min_sequence: SequenceNumber,
        pub max_sequence: SequenceNumber,
        pub local_id: String,
    }

    #[async_trait]
    pub trait SstableStore: Send + Sync {
        async fn install(
            &self,
            table: TableId,
            batch: WriteBatch,
        ) -> Result<SstableRef, StorageError>;
        async fn stats(&self, table: &Table) -> Result<TableStats, StorageError>;
    }

    #[derive(Debug, Default)]
    pub struct StubSstableStore;

    #[async_trait]
    impl SstableStore for StubSstableStore {
        async fn install(
            &self,
            table: TableId,
            _batch: WriteBatch,
        ) -> Result<SstableRef, StorageError> {
            Ok(SstableRef {
                table_id: table,
                level: 0,
                min_sequence: SequenceNumber::new(0),
                max_sequence: SequenceNumber::new(0),
                local_id: "stub-sstable".to_string(),
            })
        }

        async fn stats(&self, _table: &Table) -> Result<TableStats, StorageError> {
            Ok(TableStats::default())
        }
    }
}

pub mod manifest {
    use super::*;

    #[derive(Clone, Debug, Default)]
    pub struct ManifestRecord {
        pub id: Option<ManifestId>,
        pub last_flushed_sequence: SequenceNumber,
        pub live_segments: Vec<SegmentId>,
    }

    #[async_trait]
    pub trait ManifestStore: Send + Sync {
        async fn load(&self) -> Result<ManifestRecord, OpenError>;
        async fn save(&self, manifest: ManifestRecord) -> Result<ManifestId, StorageError>;
    }

    #[derive(Debug, Default)]
    pub struct StubManifestStore;

    #[async_trait]
    impl ManifestStore for StubManifestStore {
        async fn load(&self) -> Result<ManifestRecord, OpenError> {
            Ok(ManifestRecord::default())
        }

        async fn save(&self, _manifest: ManifestRecord) -> Result<ManifestId, StorageError> {
            Ok(ManifestId::new(1))
        }
    }
}

pub mod compaction {
    use super::*;

    #[derive(Clone, Debug)]
    pub struct CompactionJob {
        pub table_id: TableId,
        pub source_level: u32,
        pub estimated_bytes: u64,
    }

    #[async_trait]
    pub trait CompactionPlanner: Send + Sync {
        async fn plan(&self, stats: &TableStats) -> Result<Vec<CompactionJob>, StorageError>;
        async fn execute(&self, job: CompactionJob) -> Result<(), StorageError>;
    }

    #[derive(Debug, Default)]
    pub struct StubCompactionPlanner;

    #[async_trait]
    impl CompactionPlanner for StubCompactionPlanner {
        async fn plan(&self, _stats: &TableStats) -> Result<Vec<CompactionJob>, StorageError> {
            Ok(Vec::new())
        }

        async fn execute(&self, _job: CompactionJob) -> Result<(), StorageError> {
            Ok(())
        }
    }
}

pub mod scheduler_integration {
    use super::*;

    pub trait SchedulerCoordinator: Send + Sync {
        fn pending_work(&self) -> Vec<PendingWork>;
        fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision>;
    }

    #[derive(Debug, Default)]
    pub struct StubSchedulerCoordinator;

    impl SchedulerCoordinator for StubSchedulerCoordinator {
        fn pending_work(&self) -> Vec<PendingWork> {
            Vec::new()
        }

        fn on_work_available(&self, _work: &[PendingWork]) -> Vec<ScheduleDecision> {
            Vec::new()
        }
    }
}

pub mod change_capture {
    use super::*;

    #[async_trait]
    pub trait ChangeCapture: Send + Sync {
        async fn visible_scan(
            &self,
            table: &Table,
            cursor: LogCursor,
            opts: ScanOptions,
        ) -> Result<ChangeStream, SnapshotTooOld>;
        async fn durable_scan(
            &self,
            table: &Table,
            cursor: LogCursor,
            opts: ScanOptions,
        ) -> Result<ChangeStream, SnapshotTooOld>;
    }

    #[derive(Debug, Default)]
    pub struct StubChangeCapture;

    #[async_trait]
    impl ChangeCapture for StubChangeCapture {
        async fn visible_scan(
            &self,
            _table: &Table,
            _cursor: LogCursor,
            _opts: ScanOptions,
        ) -> Result<ChangeStream, SnapshotTooOld> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn durable_scan(
            &self,
            _table: &Table,
            _cursor: LogCursor,
            _opts: ScanOptions,
        ) -> Result<ChangeStream, SnapshotTooOld> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }
}

pub mod object_store {
    use super::*;

    #[async_trait]
    pub trait ObjectStoreBridge: Send + Sync {
        async fn upload_segment(&self, segment: SegmentId) -> Result<(), StorageError>;
        async fn upload_manifest(&self, manifest: ManifestId) -> Result<(), StorageError>;
    }

    #[derive(Debug, Default)]
    pub struct StubObjectStoreBridge;

    #[async_trait]
    impl ObjectStoreBridge for StubObjectStoreBridge {
        async fn upload_segment(&self, _segment: SegmentId) -> Result<(), StorageError> {
            Ok(())
        }

        async fn upload_manifest(&self, _manifest: ManifestId) -> Result<(), StorageError> {
            Ok(())
        }
    }
}

pub mod projection_runtime {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct ProjectionRegistration {
        pub name: String,
        pub sources: Vec<String>,
        pub outputs: Vec<String>,
    }

    #[async_trait]
    pub trait ProjectionRuntime: Send + Sync {
        async fn register(&self, projection: ProjectionRegistration) -> Result<(), StorageError>;
        async fn start(&self) -> Result<(), StorageError>;
    }

    #[derive(Debug, Default)]
    pub struct StubProjectionRuntime;

    #[async_trait]
    impl ProjectionRuntime for StubProjectionRuntime {
        async fn register(&self, _projection: ProjectionRegistration) -> Result<(), StorageError> {
            Ok(())
        }

        async fn start(&self) -> Result<(), StorageError> {
            Ok(())
        }
    }
}

pub mod workflow_runtime {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct WorkflowRegistration {
        pub name: String,
        pub state_table: String,
        pub inbox_table: String,
    }

    #[async_trait]
    pub trait WorkflowRuntime: Send + Sync {
        async fn register(&self, workflow: WorkflowRegistration) -> Result<(), StorageError>;
        async fn start(&self) -> Result<(), StorageError>;
    }

    #[derive(Debug, Default)]
    pub struct StubWorkflowRuntime;

    #[async_trait]
    impl WorkflowRuntime for StubWorkflowRuntime {
        async fn register(&self, _workflow: WorkflowRegistration) -> Result<(), StorageError> {
            Ok(())
        }

        async fn start(&self) -> Result<(), StorageError> {
            Ok(())
        }
    }
}
