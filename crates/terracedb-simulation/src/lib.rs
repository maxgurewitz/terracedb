use std::{
    collections::{BTreeMap, VecDeque},
    error::Error as StdError,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

use terracedb::{
    ChangeKind, Clock, CommitError, CompactionFilterRef, CompactionStrategy,
    CurrentStateOracleMutation, CurrentStateOracleRow, CurrentStateRetentionContract,
    CurrentStateRetentionCoordinationContext, CurrentStateRetentionCoordinator,
    CurrentStateRetentionError, CurrentStateRetentionEvaluation, Db, DbConfig, DbDependencies,
    DeterministicRng, FileHandle, FileSystem, FileSystemFailure, FileSystemOperation, FlushError,
    Key, KvStream, LogCursor, MergeOperator, MergeOperatorRef, ObjectStore, ObjectStoreOperation,
    OpenError, OpenOptions, ReadError, Rng, S3Location, ScanOptions, SequenceNumber,
    SimulatedFileSystem, SsdConfig, StorageConfig, StorageError, StorageErrorKind, Table,
    TableConfig, TableFormat, TieredDurabilityMode, TieredStorageConfig, Timestamp,
    TtlCompactionFilter, Value, WriteError, test_support::FailpointRegistry,
};

mod hybrid;
mod pressure;

#[allow(unused_imports)]
pub use self::hybrid::*;
#[allow(unused_imports)]
pub use self::pressure::*;
#[allow(unused_imports)]
pub use terracedb::{
    ColocatedDatabasePlacement, ColocatedDbWorkloadGenerator, ColocatedDbWorkloadSpec,
    ColocatedDeployment, ColocatedDeploymentBuilder, ColocatedDeploymentError,
    ColocatedDeploymentReport, ColocatedSubsystemPlacement, ContentionClass,
    DbExecutionPlacementReport, DbExecutionProfile, DomainBudgetCharge, DomainBudgetOracle,
    DomainBudgetSnapshot, DomainTaggedWork, DurabilityClass, ExecutionDomainBacklogSnapshot,
    ExecutionDomainBudget, ExecutionDomainContentionSnapshot, ExecutionDomainInvariant,
    ExecutionDomainInvariantSet, ExecutionDomainOwner, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionDomainSnapshot, ExecutionDomainSpec, ExecutionLane,
    ExecutionLaneBinding, ExecutionLanePlacementConfig, ExecutionPlacementDecision,
    ExecutionResourceKind, ExecutionResourceUsage, InMemoryDomainBudgetOracle,
    InMemoryResourceManager, PlacementAssignment, PlacementRequest, PlacementTarget,
    ResourceAdmissionDecision, ResourceManager, ResourceManagerSnapshot, ShardReadyPlacementLayout,
    WorkPlacementRequest, WorkRuntimeTag,
};

const OBJECT_STORE_HOST: &str = "object-store";
const OBJECT_STORE_PORT: u16 = 9400;
const STUB_DB_LOG_PATH: &str = "/terracedb/sim/stub-db.log";
const IO_CHUNK_LEN: usize = 4096;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TurmoilClock;

#[async_trait]
impl terracedb::Clock for TurmoilClock {
    fn now(&self) -> Timestamp {
        let elapsed = if turmoil::in_simulation() {
            turmoil::elapsed()
        } else {
            Duration::ZERO
        };
        Timestamp::new(elapsed.as_millis() as u64)
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileSystemFaultSpec {
    DiskFull {
        target_prefix: String,
    },
    Timeout {
        operation: FileSystemOperation,
        target_prefix: String,
    },
    PartialRead {
        target_prefix: String,
    },
}

impl FileSystemFaultSpec {
    fn into_failure(self) -> FileSystemFailure {
        match self {
            Self::DiskFull { target_prefix } => FileSystemFailure::disk_full(target_prefix),
            Self::Timeout {
                operation,
                target_prefix,
            } => FileSystemFailure::timeout(operation, target_prefix),
            Self::PartialRead { target_prefix } => FileSystemFailure::partial_read(target_prefix),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectStoreFaultSpec {
    Timeout {
        operation: ObjectStoreOperation,
        target_prefix: String,
    },
    StaleList {
        prefix: String,
    },
    PartialRead {
        operation: ObjectStoreOperation,
        target_prefix: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CutPoint {
    BeforeStep,
    AfterStep,
    AfterDurabilityBoundary,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScheduledFaultKind {
    FileSystem(FileSystemFaultSpec),
    ObjectStore(ObjectStoreFaultSpec),
    Crash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledFault {
    pub step: usize,
    pub cut_point: CutPoint,
    pub kind: ScheduledFaultKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkloadOperation {
    FileWrite {
        path: String,
        data: Vec<u8>,
        sync: bool,
    },
    FileRead {
        path: String,
        offset: u64,
        len: usize,
    },
    ObjectPut {
        key: String,
        data: Vec<u8>,
    },
    ObjectGet {
        key: String,
    },
    ObjectList {
        prefix: String,
    },
    AdvanceClock {
        millis: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedScenario {
    pub seed: u64,
    pub workload: Vec<WorkloadOperation>,
    pub faults: Vec<ScheduledFault>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationResult {
    Unit,
    Sequence(SequenceNumber),
    Bytes(usize),
    Value(Option<Vec<u8>>),
    Rows(Vec<(Vec<u8>, Vec<u8>)>),
    Bool(bool),
    Keys(Vec<String>),
    Error(StorageErrorKind),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceEvent {
    ScenarioStarted {
        seed: u64,
    },
    StepStarted {
        index: usize,
        operation: WorkloadOperation,
    },
    StepResult {
        index: usize,
        result: OperationResult,
    },
    DbStepStarted {
        index: usize,
        operation: DbWorkloadOperation,
    },
    DbStepResult {
        index: usize,
        result: OperationResult,
    },
    FaultInjected {
        step: usize,
        cut_point: CutPoint,
        kind: ScheduledFaultKind,
    },
    FilesystemCheckpoint,
    Crash {
        cut_point: CutPoint,
    },
    Restart,
    Checkpoint {
        label: String,
        metadata: BTreeMap<String, String>,
    },
    DbCommit {
        sequence: SequenceNumber,
        durable_sequence: SequenceNumber,
        mutation: DbMutation,
    },
    DbRecovered {
        current_sequence: SequenceNumber,
        durable_sequence: SequenceNumber,
        table_count: usize,
    },
    StubCommit {
        sequence: SequenceNumber,
        durable_sequence: SequenceNumber,
        mutation: PointMutation,
    },
    StubRecovered {
        current_sequence: SequenceNumber,
        durable_sequence: SequenceNumber,
        key_count: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationOutcome {
    pub seed: u64,
    pub scenario: GeneratedScenario,
    pub trace: Vec<TraceEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationScenarioConfig {
    pub steps: usize,
    pub path_count: usize,
    pub key_count: usize,
    pub max_payload_len: usize,
    pub max_clock_advance_millis: u64,
}

impl Default for SimulationScenarioConfig {
    fn default() -> Self {
        Self {
            steps: 12,
            path_count: 4,
            key_count: 4,
            max_payload_len: 16,
            max_clock_advance_millis: 5,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DbWorkloadOperation {
    Put {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Merge {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
    ReadLatest {
        table: String,
        key: Vec<u8>,
    },
    AdvanceClock {
        millis: u64,
    },
    Flush,
    RunCompaction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbGeneratedScenario {
    pub seed: u64,
    pub root_path: String,
    pub tables: Vec<SimulationTableSpec>,
    pub workload: Vec<DbWorkloadOperation>,
    pub faults: Vec<ScheduledFault>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbSimulationOutcome {
    pub seed: u64,
    pub scenario: DbGeneratedScenario,
    pub trace: Vec<TraceEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbSimulationScenarioConfig {
    pub root_path: String,
    pub tables: Vec<SimulationTableSpec>,
    pub steps: usize,
    pub key_count: usize,
    pub max_payload_len: usize,
}

impl Default for DbSimulationScenarioConfig {
    fn default() -> Self {
        Self {
            root_path: "/terracedb/sim/db".to_string(),
            tables: vec![SimulationTableSpec::merge_row(
                "events",
                SimulationMergeOperatorId::AppendBytes,
                Some(2),
            )],
            steps: 12,
            key_count: 4,
            max_payload_len: 16,
        }
    }
}

#[derive(Clone)]
pub struct SimulationContext {
    seed: u64,
    trace: Arc<Mutex<Vec<TraceEvent>>>,
    file_system: Arc<SimulatedFileSystem>,
    object_store: Arc<NetworkObjectStore>,
    clock: Arc<TurmoilClock>,
    rng: Arc<DeterministicRng>,
    failpoints: Arc<FailpointRegistry>,
}

impl SimulationContext {
    fn new(
        seed: u64,
        file_system: Arc<SimulatedFileSystem>,
        object_store: Arc<NetworkObjectStore>,
        clock: Arc<TurmoilClock>,
        rng: Arc<DeterministicRng>,
    ) -> Self {
        Self {
            seed,
            trace: Arc::new(Mutex::new(Vec::new())),
            file_system,
            object_store,
            clock,
            rng,
            failpoints: Arc::new(FailpointRegistry::default()),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn dependencies(&self) -> DbDependencies {
        let dependencies = DbDependencies::new(
            self.file_system.clone(),
            self.object_store.clone(),
            self.clock.clone(),
            self.rng.clone(),
        );
        dependencies.__attach_failpoint_registry(self.failpoints.clone());
        dependencies
    }

    pub fn file_system(&self) -> Arc<SimulatedFileSystem> {
        self.file_system.clone()
    }

    pub fn object_store(&self) -> Arc<NetworkObjectStore> {
        self.object_store.clone()
    }

    pub fn clock(&self) -> Arc<TurmoilClock> {
        self.clock.clone()
    }

    pub fn rng(&self) -> Arc<DeterministicRng> {
        self.rng.clone()
    }

    /// Returns the shared failpoint registry for this seeded simulation run.
    pub fn failpoints(&self) -> Arc<FailpointRegistry> {
        self.failpoints.clone()
    }

    pub fn record(&self, event: TraceEvent) {
        lock(&self.trace).push(event);
    }

    pub fn trace(&self) -> Vec<TraceEvent> {
        lock(&self.trace).clone()
    }

    pub fn checkpoint_filesystem(&self) {
        self.file_system.checkpoint();
        self.record(TraceEvent::FilesystemCheckpoint);
    }

    pub fn checkpoint(&self, label: impl Into<String>, metadata: BTreeMap<String, String>) {
        self.record(TraceEvent::Checkpoint {
            label: label.into(),
            metadata,
        });
    }

    pub fn crash_filesystem(&self, cut_point: CutPoint) {
        self.file_system.crash();
        self.record(TraceEvent::Crash { cut_point });
    }

    pub async fn open_db(&self, config: DbConfig) -> Result<Db, OpenError> {
        Db::open(config, self.dependencies()).await
    }

    pub async fn restart_db(&self, config: DbConfig, cut_point: CutPoint) -> Result<Db, OpenError> {
        self.crash_filesystem(cut_point);
        self.record(TraceEvent::Restart);
        self.open_db(config).await
    }
}

type SimulationHostFuture = Pin<Box<dyn Future<Output = turmoil::Result<()>> + 'static>>;

#[derive(Clone)]
pub struct SimulationHost {
    name: Arc<str>,
    run: Arc<dyn Fn() -> SimulationHostFuture + Send + Sync>,
}

impl std::fmt::Debug for SimulationHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulationHost")
            .field("name", &self.name)
            .finish()
    }
}

impl SimulationHost {
    pub fn new<F, Fut>(name: impl Into<Arc<str>>, run: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = turmoil::Result<()>> + 'static,
    {
        Self {
            name: name.into(),
            run: Arc::new(move || Box::pin(run())),
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

pub type SimulationStackResult<T> = Result<T, Box<dyn StdError + Send + Sync + 'static>>;

type SimulationStackFuture<T> = Pin<Box<dyn Future<Output = SimulationStackResult<T>> + 'static>>;
type SimulationWaitFuture<'a> = Pin<Box<dyn Future<Output = SimulationStackResult<bool>> + 'a>>;

#[derive(Clone)]
pub struct SimulationStackBuilder<S> {
    open: Arc<dyn Fn(SimulationContext, Db) -> SimulationStackFuture<S> + Send + Sync>,
    shutdown: Arc<dyn Fn(S) -> SimulationStackFuture<()> + Send + Sync>,
}

impl<S> SimulationStackBuilder<S> {
    pub fn new<Open, OpenFut, Shutdown, ShutdownFut>(open: Open, shutdown: Shutdown) -> Self
    where
        Open: Fn(SimulationContext, Db) -> OpenFut + Send + Sync + 'static,
        OpenFut: Future<Output = SimulationStackResult<S>> + 'static,
        Shutdown: Fn(S) -> ShutdownFut + Send + Sync + 'static,
        ShutdownFut: Future<Output = SimulationStackResult<()>> + 'static,
    {
        Self {
            open: Arc::new(move |context, db| Box::pin(open(context, db))),
            shutdown: Arc::new(move |stack| Box::pin(shutdown(stack))),
        }
    }

    async fn open(&self, context: SimulationContext, db: Db) -> SimulationStackResult<S> {
        (self.open)(context, db).await
    }

    async fn shutdown(&self, stack: S) -> SimulationStackResult<()> {
        (self.shutdown)(stack).await
    }
}

impl SimulationStackBuilder<()> {
    pub fn db_only() -> Self {
        Self::new(
            |_context, _db| async move { Ok(()) },
            |_stack| async move { Ok(()) },
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationCheckpoint {
    pub label: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct SimulationFailureReport {
    pub seed: u64,
    pub message: String,
    pub trace: Vec<TraceEvent>,
    pub checkpoints: Vec<SimulationCheckpoint>,
}

impl std::fmt::Display for SimulationFailureReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.message)?;
        writeln!(f, "seed: {}", self.seed)?;

        let faults = self
            .trace
            .iter()
            .filter_map(|event| match event {
                TraceEvent::FaultInjected {
                    step,
                    cut_point,
                    kind,
                } => Some(format!("step={step} cut_point={cut_point:?} kind={kind:?}")),
                _ => None,
            })
            .collect::<Vec<_>>();

        if faults.is_empty() {
            writeln!(f, "faults: none")?;
        } else {
            writeln!(f, "faults:")?;
            for fault in faults {
                writeln!(f, "  - {fault}")?;
            }
        }

        if self.checkpoints.is_empty() {
            writeln!(f, "checkpoints: none")?;
        } else {
            writeln!(f, "checkpoints:")?;
            for checkpoint in &self.checkpoints {
                writeln!(f, "  - {}", checkpoint.label)?;
                for (key, value) in &checkpoint.metadata {
                    writeln!(f, "    {key}={value}")?;
                }
            }
        }

        writeln!(f, "trace:")?;
        for event in &self.trace {
            writeln!(f, "  - {event:?}")?;
        }

        Ok(())
    }
}

impl StdError for SimulationFailureReport {}

#[derive(Debug, Error)]
pub enum SimulationHarnessError {
    #[error(transparent)]
    Open(#[from] OpenError),
    #[error("simulation stack open failed: {message}")]
    StackOpen { message: String },
    #[error("simulation stack shutdown failed: {message}")]
    StackShutdown { message: String },
    #[error("simulation stack is not running")]
    NotRunning,
    #[error("simulation checkpoint {label} failed: {message}")]
    Checkpoint { label: String, message: String },
    #[error("simulation wait {label} failed: {message}")]
    Wait { label: String, message: String },
}

struct ActiveSimulationStack<S> {
    db: Db,
    stack: S,
}

pub struct TerracedbSimulationHarness<S> {
    context: SimulationContext,
    db_config: DbConfig,
    stack_builder: SimulationStackBuilder<S>,
    state: Option<ActiveSimulationStack<S>>,
    checkpoints: Vec<SimulationCheckpoint>,
}

impl<S> TerracedbSimulationHarness<S> {
    pub async fn open(
        context: SimulationContext,
        db_config: DbConfig,
        stack_builder: SimulationStackBuilder<S>,
    ) -> Result<Self, SimulationHarnessError> {
        let db = context.open_db(db_config.clone()).await?;
        let stack = stack_builder
            .open(context.clone(), db.clone())
            .await
            .map_err(|error| SimulationHarnessError::StackOpen {
                message: error.to_string(),
            })?;

        let mut harness = Self {
            context,
            db_config,
            stack_builder,
            state: Some(ActiveSimulationStack { db, stack }),
            checkpoints: Vec::new(),
        };
        harness.checkpoint("opened").await?;
        Ok(harness)
    }

    pub fn seed(&self) -> u64 {
        self.context.seed()
    }

    pub fn context(&self) -> &SimulationContext {
        &self.context
    }

    pub fn trace(&self) -> Vec<TraceEvent> {
        self.context.trace()
    }

    pub fn checkpoints(&self) -> &[SimulationCheckpoint] {
        &self.checkpoints
    }

    pub fn db(&self) -> &Db {
        &self
            .state
            .as_ref()
            .expect("simulation stack should be running")
            .db
    }

    pub fn stack(&self) -> &S {
        &self
            .state
            .as_ref()
            .expect("simulation stack should be running")
            .stack
    }

    pub fn stack_mut(&mut self) -> &mut S {
        &mut self
            .state
            .as_mut()
            .expect("simulation stack should be running")
            .stack
    }

    pub async fn checkpoint(
        &mut self,
        label: impl Into<String>,
    ) -> Result<(), SimulationHarnessError> {
        self.checkpoint_with(label, |_db, _stack| {
            Box::pin(async move { Ok(BTreeMap::new()) })
        })
        .await
    }

    pub async fn checkpoint_with<F>(
        &mut self,
        label: impl Into<String>,
        enrich: F,
    ) -> Result<(), SimulationHarnessError>
    where
        F: for<'a> FnOnce(
            &'a Db,
            &'a S,
        ) -> Pin<
            Box<dyn Future<Output = SimulationStackResult<BTreeMap<String, String>>> + 'a>,
        >,
    {
        let label = label.into();
        let state = self
            .state
            .as_ref()
            .ok_or(SimulationHarnessError::NotRunning)?;
        let mut metadata = base_checkpoint_metadata(&state.db);
        let extra = enrich(&state.db, &state.stack).await.map_err(|error| {
            SimulationHarnessError::Checkpoint {
                label: label.clone(),
                message: error.to_string(),
            }
        })?;
        metadata.extend(extra);
        self.context.checkpoint(label.clone(), metadata.clone());
        self.checkpoints
            .push(SimulationCheckpoint { label, metadata });
        Ok(())
    }

    pub async fn wait_for_change<'a, IV, ID, F>(
        &self,
        label: impl Into<String>,
        visible_tables: IV,
        durable_tables: ID,
        predicate: F,
    ) -> Result<(), SimulationHarnessError>
    where
        IV: IntoIterator<Item = &'a Table>,
        ID: IntoIterator<Item = &'a Table>,
        F: for<'b> FnMut(&'b Db, &'b S) -> SimulationWaitFuture<'b>,
    {
        self.wait_for_tables(label, visible_tables, durable_tables, predicate)
            .await
    }

    pub async fn wait_for_visible<'a, I, F>(
        &self,
        label: impl Into<String>,
        tables: I,
        predicate: F,
    ) -> Result<(), SimulationHarnessError>
    where
        I: IntoIterator<Item = &'a Table>,
        F: for<'b> FnMut(&'b Db, &'b S) -> SimulationWaitFuture<'b>,
    {
        self.wait_for_change(label, tables, std::iter::empty::<&Table>(), predicate)
            .await
    }

    pub async fn wait_for_durable<'a, I, F>(
        &self,
        label: impl Into<String>,
        tables: I,
        predicate: F,
    ) -> Result<(), SimulationHarnessError>
    where
        I: IntoIterator<Item = &'a Table>,
        F: for<'b> FnMut(&'b Db, &'b S) -> SimulationWaitFuture<'b>,
    {
        self.wait_for_change(label, std::iter::empty::<&Table>(), tables, predicate)
            .await
    }

    async fn wait_for_tables<'a, IV, ID, F>(
        &self,
        label: impl Into<String>,
        visible_tables: IV,
        durable_tables: ID,
        mut predicate: F,
    ) -> Result<(), SimulationHarnessError>
    where
        IV: IntoIterator<Item = &'a Table>,
        ID: IntoIterator<Item = &'a Table>,
        F: for<'b> FnMut(&'b Db, &'b S) -> SimulationWaitFuture<'b>,
    {
        let label = label.into();
        let visible_table_names = visible_tables
            .into_iter()
            .map(|table| table.name().to_string())
            .collect::<Vec<_>>();
        let durable_table_names = durable_tables
            .into_iter()
            .map(|table| table.name().to_string())
            .collect::<Vec<_>>();
        let db = self
            .state
            .as_ref()
            .ok_or(SimulationHarnessError::NotRunning)?
            .db
            .clone();

        if visible_table_names.is_empty() && durable_table_names.is_empty() {
            let state = self
                .state
                .as_ref()
                .ok_or(SimulationHarnessError::NotRunning)?;
            if evaluate_wait_predicate(&label, &mut predicate, state).await? {
                return Ok(());
            }
            return Err(SimulationHarnessError::Wait {
                label,
                message: "no tables were provided for the wait condition".to_string(),
            });
        }

        let visible_handles = visible_table_names
            .iter()
            .map(|name| db.table(name))
            .collect::<Vec<_>>();
        let durable_handles = durable_table_names
            .iter()
            .map(|name| db.table(name))
            .collect::<Vec<_>>();
        let mut visible_subscription =
            (!visible_handles.is_empty()).then(|| db.subscribe_visible_set(visible_handles.iter()));
        let mut durable_subscription =
            (!durable_handles.is_empty()).then(|| db.subscribe_durable_set(durable_handles.iter()));

        loop {
            let state = self
                .state
                .as_ref()
                .ok_or(SimulationHarnessError::NotRunning)?;
            if evaluate_wait_predicate(&label, &mut predicate, state).await? {
                return Ok(());
            }

            match (visible_subscription.as_mut(), durable_subscription.as_mut()) {
                (Some(visible), Some(durable)) => {
                    tokio::select! {
                        result = visible.changed() => {
                            result.map_err(|error| SimulationHarnessError::Wait {
                                label: label.clone(),
                                message: format!(
                                    "{error} while waiting on visible table(s): {}",
                                    visible_table_names.join(", ")
                                ),
                            })?;
                        }
                        result = durable.changed() => {
                            result.map_err(|error| SimulationHarnessError::Wait {
                                label: label.clone(),
                                message: format!(
                                    "{error} while waiting on durable table(s): {}",
                                    durable_table_names.join(", ")
                                ),
                            })?;
                        }
                    }
                }
                (Some(visible), None) => {
                    visible
                        .changed()
                        .await
                        .map_err(|error| SimulationHarnessError::Wait {
                            label: label.clone(),
                            message: format!(
                                "{error} while waiting on visible table(s): {}",
                                visible_table_names.join(", ")
                            ),
                        })?;
                }
                (None, Some(durable)) => {
                    durable
                        .changed()
                        .await
                        .map_err(|error| SimulationHarnessError::Wait {
                            label: label.clone(),
                            message: format!(
                                "{error} while waiting on durable table(s): {}",
                                durable_table_names.join(", ")
                            ),
                        })?;
                }
                (None, None) => unreachable!("checked that at least one subscription exists"),
            }
        }
    }

    pub async fn restart(&mut self, cut_point: CutPoint) -> Result<(), SimulationHarnessError> {
        let cut_point_label = format!("{cut_point:?}");
        self.checkpoint_with(
            format!("before-restart-{cut_point_label}"),
            move |_db, _stack| {
                Box::pin(async move {
                    Ok(BTreeMap::from([(
                        "restart.cut_point".to_string(),
                        cut_point_label.clone(),
                    )]))
                })
            },
        )
        .await?;

        let state = self
            .state
            .take()
            .ok_or(SimulationHarnessError::NotRunning)?;
        self.stack_builder
            .shutdown(state.stack)
            .await
            .map_err(|error| SimulationHarnessError::StackShutdown {
                message: error.to_string(),
            })?;

        let db = self
            .context
            .restart_db(self.db_config.clone(), cut_point)
            .await?;
        let stack = self
            .stack_builder
            .open(self.context.clone(), db.clone())
            .await
            .map_err(|error| SimulationHarnessError::StackOpen {
                message: error.to_string(),
            })?;
        self.state = Some(ActiveSimulationStack { db, stack });

        self.checkpoint_with(
            format!("after-restart-{cut_point:?}"),
            move |_db, _stack| {
                Box::pin(async move {
                    Ok(BTreeMap::from([(
                        "restart.cut_point".to_string(),
                        format!("{cut_point:?}"),
                    )]))
                })
            },
        )
        .await?;

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), SimulationHarnessError> {
        if self.state.is_none() {
            return Ok(());
        }

        self.checkpoint("shutdown").await?;
        let state = self.state.take().expect("checked is_some");
        self.stack_builder
            .shutdown(state.stack)
            .await
            .map_err(|error| SimulationHarnessError::StackShutdown {
                message: error.to_string(),
            })
    }

    pub fn failure(&self, message: impl Into<String>) -> SimulationFailureReport {
        SimulationFailureReport {
            seed: self.seed(),
            message: message.into(),
            trace: self.trace(),
            checkpoints: self.checkpoints.clone(),
        }
    }

    pub fn require(
        &self,
        condition: bool,
        message: impl Into<String>,
    ) -> Result<(), SimulationFailureReport> {
        if condition {
            Ok(())
        } else {
            Err(self.failure(message))
        }
    }

    pub fn require_eq<T>(
        &self,
        label: &str,
        actual: &T,
        expected: &T,
    ) -> Result<(), SimulationFailureReport>
    where
        T: PartialEq + std::fmt::Debug,
    {
        if actual == expected {
            Ok(())
        } else {
            Err(self.failure(format!(
                "{label} mismatch: expected {expected:?}, got {actual:?}"
            )))
        }
    }

    pub fn require_ne<T>(
        &self,
        label: &str,
        actual: &T,
        expected: &T,
    ) -> Result<(), SimulationFailureReport>
    where
        T: PartialEq + std::fmt::Debug,
    {
        if actual != expected {
            Ok(())
        } else {
            Err(self.failure(format!("{label} unexpectedly matched {expected:?}")))
        }
    }
}

async fn evaluate_wait_predicate<S, F>(
    label: &str,
    predicate: &mut F,
    state: &ActiveSimulationStack<S>,
) -> Result<bool, SimulationHarnessError>
where
    F: for<'b> FnMut(&'b Db, &'b S) -> SimulationWaitFuture<'b>,
{
    predicate(&state.db, &state.stack)
        .await
        .map_err(|error| SimulationHarnessError::Wait {
            label: label.to_string(),
            message: error.to_string(),
        })
}

fn base_checkpoint_metadata(db: &Db) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "db.visible_sequence".to_string(),
            db.current_sequence().get().to_string(),
        ),
        (
            "db.durable_sequence".to_string(),
            db.current_durable_sequence().get().to_string(),
        ),
    ])
}

const SIMULATION_DRIVER_HOST: &str = "driver";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulatedServiceAction {
    Reply(Vec<u8>),
    DelayReply { delay: Duration, response: Vec<u8> },
    Timeout { delay: Duration },
    Disconnect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulatedServiceRequest {
    pub ordinal: usize,
    pub payload: Vec<u8>,
}

#[derive(Clone)]
pub struct SimulatedExternalService {
    host: Arc<str>,
    port: u16,
    state: Arc<Mutex<SimulatedExternalServiceState>>,
}

impl std::fmt::Debug for SimulatedExternalService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulatedExternalService")
            .field("host", &self.host)
            .field("port", &self.port)
            .finish()
    }
}

#[derive(Default)]
struct SimulatedExternalServiceState {
    requests: Vec<SimulatedServiceRequest>,
    scripted_actions: VecDeque<SimulatedServiceAction>,
}

impl SimulatedExternalService {
    pub fn new(host: impl Into<Arc<str>>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            state: Arc::new(Mutex::new(SimulatedExternalServiceState::default())),
        }
    }

    pub fn with_actions<I>(self, actions: I) -> Self
    where
        I: IntoIterator<Item = SimulatedServiceAction>,
    {
        let mut state = lock(&self.state);
        state.scripted_actions.extend(actions);
        drop(state);
        self
    }

    pub fn enqueue_action(&self, action: SimulatedServiceAction) {
        lock(&self.state).scripted_actions.push_back(action);
    }

    pub fn host(&self) -> &str {
        self.host.as_ref()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn requests(&self) -> Vec<SimulatedServiceRequest> {
        lock(&self.state).requests.clone()
    }

    pub fn checkpoint_metadata(&self) -> BTreeMap<String, String> {
        let state = lock(&self.state);
        BTreeMap::from([
            (
                "service.host".to_string(),
                format!("{}:{}", self.host.as_ref(), self.port),
            ),
            (
                "service.request_count".to_string(),
                state.requests.len().to_string(),
            ),
            (
                "service.pending_actions".to_string(),
                state.scripted_actions.len().to_string(),
            ),
        ])
    }

    pub fn hold_from_driver(&self) {
        turmoil::hold(SIMULATION_DRIVER_HOST, self.host.as_ref());
    }

    pub fn release_from_driver(&self) {
        turmoil::release(SIMULATION_DRIVER_HOST, self.host.as_ref());
    }

    pub fn partition_from_driver(&self) {
        turmoil::partition(SIMULATION_DRIVER_HOST, self.host.as_ref());
    }

    pub fn simulation_host(&self) -> SimulationHost {
        let host = self.host.clone();
        let service = self.clone();
        SimulationHost::new(host, move || {
            let service = service.clone();
            async move { service.run_host().await }
        })
    }

    pub async fn call(&self, payload: Vec<u8>, timeout: Duration) -> std::io::Result<Vec<u8>> {
        let host = self.host.clone();
        let port = self.port;
        let payload = serde_json::to_vec(&SimulatedExternalServiceEnvelope::Call { payload })
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

        tokio::time::timeout(timeout, async move {
            let mut stream = TcpStream::connect((host.as_ref(), port)).await?;
            write_frame(&mut stream, &payload).await?;
            let payload = read_frame(&mut stream).await?;
            let response: SimulatedExternalServiceResponse = serde_json::from_slice(&payload)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            match response {
                SimulatedExternalServiceResponse::Reply { payload } => Ok(payload),
                SimulatedExternalServiceResponse::Ack => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected shutdown acknowledgement for service call",
                )),
            }
        })
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for simulated service {}:{}",
                    self.host.as_ref(),
                    self.port
                ),
            )
        })?
    }

    pub async fn shutdown(&self) -> std::io::Result<()> {
        let mut stream = TcpStream::connect((self.host.as_ref(), self.port)).await?;
        let payload = serde_json::to_vec(&SimulatedExternalServiceEnvelope::Shutdown)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        write_frame(&mut stream, &payload).await?;
        let payload = read_frame(&mut stream).await?;
        let response: SimulatedExternalServiceResponse = serde_json::from_slice(&payload)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        match response {
            SimulatedExternalServiceResponse::Ack => Ok(()),
            SimulatedExternalServiceResponse::Reply { .. } => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected service reply during shutdown",
            )),
        }
    }

    async fn run_host(self) -> turmoil::Result<()> {
        let listener = TcpListener::bind(("0.0.0.0", self.port)).await?;

        loop {
            let (mut stream, _peer) = listener.accept().await?;
            let payload = read_frame(&mut stream).await?;
            let request: SimulatedExternalServiceEnvelope = serde_json::from_slice(&payload)?;
            match request {
                SimulatedExternalServiceEnvelope::Shutdown => {
                    let payload = serde_json::to_vec(&SimulatedExternalServiceResponse::Ack)?;
                    write_frame(&mut stream, &payload).await?;
                    return Ok(());
                }
                SimulatedExternalServiceEnvelope::Call { payload } => {
                    let action = {
                        let mut state = lock(&self.state);
                        let ordinal = state.requests.len() + 1;
                        state.requests.push(SimulatedServiceRequest {
                            ordinal,
                            payload: payload.clone(),
                        });
                        state
                            .scripted_actions
                            .pop_front()
                            .unwrap_or_else(|| SimulatedServiceAction::Reply(b"ok".to_vec()))
                    };

                    match action {
                        SimulatedServiceAction::Reply(response) => {
                            let payload =
                                serde_json::to_vec(&SimulatedExternalServiceResponse::Reply {
                                    payload: response,
                                })?;
                            write_frame(&mut stream, &payload).await?;
                        }
                        SimulatedServiceAction::DelayReply { delay, response } => {
                            tokio::time::sleep(delay).await;
                            let payload =
                                serde_json::to_vec(&SimulatedExternalServiceResponse::Reply {
                                    payload: response,
                                })?;
                            write_frame(&mut stream, &payload).await?;
                        }
                        SimulatedServiceAction::Timeout { delay } => {
                            tokio::time::sleep(delay).await;
                        }
                        SimulatedServiceAction::Disconnect => {}
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum SimulatedExternalServiceEnvelope {
    Call { payload: Vec<u8> },
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum SimulatedExternalServiceResponse {
    Reply { payload: Vec<u8> },
    Ack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimulationMergeOperatorId {
    AppendBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimulationCompactionFilterId {
    ExpiryPrefixTtl,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationTableSpec {
    pub name: String,
    pub format: TableFormat,
    pub merge_operator: Option<SimulationMergeOperatorId>,
    pub compaction_filter: Option<SimulationCompactionFilterId>,
    pub max_merge_operand_chain_length: Option<u32>,
    pub history_retention_sequences: Option<u64>,
    pub compaction_strategy: CompactionStrategy,
}

impl SimulationTableSpec {
    pub fn row(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            format: TableFormat::Row,
            merge_operator: None,
            compaction_filter: None,
            max_merge_operand_chain_length: None,
            history_retention_sequences: None,
            compaction_strategy: CompactionStrategy::Leveled,
        }
    }

    pub fn merge_row(
        name: impl Into<String>,
        merge_operator: SimulationMergeOperatorId,
        max_merge_operand_chain_length: Option<u32>,
    ) -> Self {
        let mut spec = Self::row(name);
        spec.merge_operator = Some(merge_operator);
        spec.max_merge_operand_chain_length = max_merge_operand_chain_length;
        spec
    }

    pub fn ttl_row(name: impl Into<String>) -> Self {
        let mut spec = Self::row(name);
        spec.compaction_filter = Some(SimulationCompactionFilterId::ExpiryPrefixTtl);
        spec
    }

    pub fn table_config(&self) -> TableConfig {
        TableConfig {
            name: self.name.clone(),
            format: self.format,
            merge_operator: self.merge_operator.map(resolve_simulation_merge_operator),
            max_merge_operand_chain_length: self.max_merge_operand_chain_length,
            compaction_filter: self
                .compaction_filter
                .map(resolve_simulation_compaction_filter),
            bloom_filter_bits_per_key: Some(10),
            history_retention_sequences: self.history_retention_sequences,
            compaction_strategy: self.compaction_strategy,
            schema: None,
            metadata: Default::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DbMutation {
    Put {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Merge {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DbVersion {
    sequence: SequenceNumber,
    mutation: TableMutation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TableMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Merge { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbRecoveryMatch {
    pub matched_sequence: SequenceNumber,
    pub durable_sequence: SequenceNumber,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbOracleChange {
    pub table: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub cursor: LogCursor,
    pub sequence: SequenceNumber,
    pub kind: ChangeKind,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DbShadowOracle {
    table_specs: BTreeMap<String, SimulationTableSpec>,
    versions: BTreeMap<String, BTreeMap<Vec<u8>, Vec<DbVersion>>>,
    ordered_changes: Vec<DbOracleChange>,
    durable_sequence: SequenceNumber,
    max_sequence: SequenceNumber,
}

impl DbShadowOracle {
    pub fn new(table_specs: &[SimulationTableSpec]) -> Self {
        Self {
            table_specs: table_specs
                .iter()
                .cloned()
                .map(|spec| (spec.name.clone(), spec))
                .collect(),
            versions: BTreeMap::new(),
            ordered_changes: Vec::new(),
            durable_sequence: SequenceNumber::new(0),
            max_sequence: SequenceNumber::new(0),
        }
    }

    pub fn apply(
        &mut self,
        sequence: SequenceNumber,
        mutation: DbMutation,
        durable: bool,
    ) -> Result<(), DbOracleError> {
        self.apply_with_cursor(LogCursor::new(sequence, 0), mutation, durable)
    }

    pub fn apply_with_cursor(
        &mut self,
        cursor: LogCursor,
        mutation: DbMutation,
        durable: bool,
    ) -> Result<(), DbOracleError> {
        let sequence = cursor.sequence();
        if let Some(previous) = self.ordered_changes.last()
            && previous.cursor >= cursor
        {
            return Err(DbOracleError::ChangeOrdering {
                previous: previous.cursor,
                next: cursor,
            });
        }

        let (table, mutation) = match mutation.clone() {
            DbMutation::Put { table, key, value } => (table, TableMutation::Put { key, value }),
            DbMutation::Merge { table, key, value } => (table, TableMutation::Merge { key, value }),
            DbMutation::Delete { table, key } => (table, TableMutation::Delete { key }),
        };
        let change_table = table.clone();
        let key = mutation.key().to_vec();
        let change_mutation = mutation.clone();

        self.ensure_table_known(&table)?;
        self.versions
            .entry(table)
            .or_default()
            .entry(key)
            .or_default()
            .push(DbVersion { sequence, mutation });

        self.ordered_changes.push(DbOracleChange::from_mutation(
            cursor,
            change_mutation,
            &change_table,
        ));

        self.max_sequence = sequence.max(self.max_sequence);
        if durable {
            self.durable_sequence = sequence.max(self.durable_sequence);
        }
        Ok(())
    }

    pub fn mark_durable_through(&mut self, sequence: SequenceNumber) {
        self.durable_sequence = self.durable_sequence.max(sequence.min(self.max_sequence));
    }

    pub fn durable_sequence(&self) -> SequenceNumber {
        self.durable_sequence
    }

    pub fn max_sequence(&self) -> SequenceNumber {
        self.max_sequence
    }

    pub fn value_at(
        &self,
        table: &str,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<Vec<u8>>, DbOracleError> {
        self.ensure_table_known(table)?;
        let Some(versions) = self
            .versions
            .get(table)
            .and_then(|table_versions| table_versions.get(key))
        else {
            return Ok(None);
        };
        let spec = self
            .table_specs
            .get(table)
            .expect("known table spec should exist");
        let mut operands = Vec::new();
        let mut existing = None;

        for version in versions.iter().rev() {
            if version.sequence > sequence {
                continue;
            }
            match &version.mutation {
                TableMutation::Put { value, .. } => {
                    existing = Some(Value::bytes(value.clone()));
                    break;
                }
                TableMutation::Merge { value, .. } => operands.push(Value::bytes(value.clone())),
                TableMutation::Delete { .. } => break,
            }
        }

        match (existing, operands.is_empty()) {
            (Some(value), true) => decode_oracle_value(table, key, value).map(Some),
            (None, true) => Ok(None),
            (existing, false) => {
                let operator_id =
                    spec.merge_operator
                        .ok_or_else(|| DbOracleError::UnknownTable {
                            table: table.to_string(),
                        })?;
                let operator = resolve_simulation_merge_operator(operator_id);
                operands.reverse();
                let collapsed =
                    collapse_merge_operands_for_oracle(operator.as_ref(), key, &operands).map_err(
                        |error| DbOracleError::MergeResolution {
                            table: table.to_string(),
                            key: key.to_vec(),
                            message: error.to_string(),
                        },
                    )?;
                let merged = operator
                    .full_merge(key, existing.as_ref(), &collapsed)
                    .map_err(|error| DbOracleError::MergeResolution {
                        table: table.to_string(),
                        key: key.to_vec(),
                        message: error.to_string(),
                    })?;
                decode_oracle_value(table, key, merged).map(Some)
            }
        }
    }

    pub fn table_state_at(
        &self,
        table: &str,
        sequence: SequenceNumber,
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, DbOracleError> {
        self.ensure_table_known(table)?;
        let mut state = BTreeMap::new();
        if let Some(table_versions) = self.versions.get(table) {
            for key in table_versions.keys() {
                if let Some(value) = self.value_at(table, key, sequence)? {
                    state.insert(key.clone(), value);
                }
            }
        }
        Ok(state)
    }

    pub fn validate_point_state(
        &self,
        table: &str,
        key: &[u8],
        sequence: SequenceNumber,
        actual: Option<&[u8]>,
    ) -> Result<(), DbOracleError> {
        let expected = self.value_at(table, key, sequence)?;
        if expected.as_deref() == actual {
            Ok(())
        } else {
            Err(DbOracleError::PointState {
                table: table.to_string(),
                key: key.to_vec(),
                sequence,
                expected,
                actual: actual.map(ToOwned::to_owned),
            })
        }
    }

    pub fn validate_recovery_prefix(
        &self,
        table: &str,
        recovered: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<DbRecoveryMatch, DbOracleError> {
        self.ensure_table_known(table)?;
        for seq in self.durable_sequence.get()..=self.max_sequence.get() {
            let sequence = SequenceNumber::new(seq);
            if self.table_state_at(table, sequence)? == *recovered {
                return Ok(DbRecoveryMatch {
                    matched_sequence: sequence,
                    durable_sequence: self.durable_sequence,
                });
            }
        }

        Err(DbOracleError::RecoveryPrefix {
            table: table.to_string(),
            durable_sequence: self.durable_sequence,
            max_sequence: self.max_sequence,
            recovered_key_count: recovered.len(),
        })
    }

    fn ensure_table_known(&self, table: &str) -> Result<(), DbOracleError> {
        if self.table_specs.contains_key(table) {
            Ok(())
        } else {
            Err(DbOracleError::UnknownTable {
                table: table.to_string(),
            })
        }
    }

    fn supports_full_history_validation(&self, table: &str) -> bool {
        self.table_specs
            .get(table)
            .map(|spec| spec.compaction_filter.is_none())
            .unwrap_or(false)
    }

    pub fn visible_changes_since(
        &self,
        table: &str,
        cursor: LogCursor,
    ) -> Result<Vec<DbOracleChange>, DbOracleError> {
        self.changes_since(table, cursor, self.max_sequence)
    }

    pub fn durable_changes_since(
        &self,
        table: &str,
        cursor: LogCursor,
    ) -> Result<Vec<DbOracleChange>, DbOracleError> {
        self.changes_since(table, cursor, self.durable_sequence)
    }

    fn changes_since(
        &self,
        table: &str,
        cursor: LogCursor,
        upper_bound: SequenceNumber,
    ) -> Result<Vec<DbOracleChange>, DbOracleError> {
        self.ensure_table_known(table)?;
        Ok(self
            .ordered_changes
            .iter()
            .filter(|change| change.table == table)
            .filter(|change| change.sequence <= upper_bound)
            .filter(|change| change.cursor > cursor)
            .cloned()
            .collect())
    }
}

impl TableMutation {
    fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Merge { key, .. } | Self::Delete { key } => key,
        }
    }
}

impl DbOracleChange {
    fn from_mutation(cursor: LogCursor, mutation: TableMutation, table: &str) -> Self {
        match mutation {
            TableMutation::Put { key, value } => Self {
                table: table.to_string(),
                key,
                value: Some(value),
                cursor,
                sequence: cursor.sequence(),
                kind: ChangeKind::Put,
            },
            TableMutation::Merge { key, value } => Self {
                table: table.to_string(),
                key,
                value: Some(value),
                cursor,
                sequence: cursor.sequence(),
                kind: ChangeKind::Merge,
            },
            TableMutation::Delete { key } => Self {
                table: table.to_string(),
                key,
                value: None,
                cursor,
                sequence: cursor.sequence(),
                kind: ChangeKind::Delete,
            },
        }
    }
}

#[derive(Debug)]
struct AppendBytesMergeOperator;

impl MergeOperator for AppendBytesMergeOperator {
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&Value>,
        operands: &[Value],
    ) -> Result<Value, StorageError> {
        let mut merged = match existing {
            Some(Value::Bytes(bytes)) => bytes.clone(),
            Some(_) => {
                return Err(StorageError::unsupported(
                    "append merge operator only supports byte values",
                ));
            }
            None => Vec::new(),
        };

        for operand in operands {
            let Value::Bytes(bytes) = operand else {
                return Err(StorageError::unsupported(
                    "append merge operator only supports byte operands",
                ));
            };
            if !merged.is_empty() && !bytes.is_empty() {
                merged.push(b'|');
            }
            merged.extend_from_slice(bytes);
        }

        Ok(Value::Bytes(merged))
    }

    fn partial_merge(
        &self,
        _key: &[u8],
        left: &Value,
        right: &Value,
    ) -> Result<Option<Value>, StorageError> {
        let Value::Bytes(left_bytes) = left else {
            return Err(StorageError::unsupported(
                "append merge operator only supports byte operands",
            ));
        };
        let Value::Bytes(right_bytes) = right else {
            return Err(StorageError::unsupported(
                "append merge operator only supports byte operands",
            ));
        };

        let mut merged = left_bytes.clone();
        if !merged.is_empty() && !right_bytes.is_empty() {
            merged.push(b'|');
        }
        merged.extend_from_slice(right_bytes);
        Ok(Some(Value::Bytes(merged)))
    }
}

fn resolve_simulation_merge_operator(id: SimulationMergeOperatorId) -> MergeOperatorRef {
    match id {
        SimulationMergeOperatorId::AppendBytes => Arc::new(AppendBytesMergeOperator),
    }
}

fn resolve_simulation_compaction_filter(id: SimulationCompactionFilterId) -> CompactionFilterRef {
    match id {
        SimulationCompactionFilterId::ExpiryPrefixTtl => {
            Arc::new(TtlCompactionFilter::new(expiry_from_prefixed_bytes))
        }
    }
}

fn expiry_from_prefixed_bytes(value: &Value) -> Option<Timestamp> {
    let Value::Bytes(bytes) = value else {
        return None;
    };
    let prefix = bytes.get(..8)?;
    Some(Timestamp::new(u64::from_be_bytes(prefix.try_into().ok()?)))
}
fn collapse_merge_operands_for_oracle(
    operator: &dyn MergeOperator,
    key: &[u8],
    operands: &[Value],
) -> Result<Vec<Value>, StorageError> {
    let mut collapsed = Vec::with_capacity(operands.len());
    for operand in operands.iter().cloned() {
        collapsed.push(operand);
        while collapsed.len() >= 2 {
            let right = collapsed.pop().expect("right operand should exist");
            let left = collapsed.pop().expect("left operand should exist");
            match operator.partial_merge(key, &left, &right)? {
                Some(merged) => collapsed.push(merged),
                None => {
                    collapsed.push(left);
                    collapsed.push(right);
                    break;
                }
            }
        }
    }

    Ok(collapsed)
}

fn decode_oracle_value(table: &str, key: &[u8], value: Value) -> Result<Vec<u8>, DbOracleError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        Value::Record(_) => Err(DbOracleError::UnsupportedValue {
            table: table.to_string(),
            key: key.to_vec(),
        }),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateSimulationScenario {
    pub initial_contract: CurrentStateRetentionContract,
    pub coordination: CurrentStateRetentionCoordinationContext,
    pub operations: Vec<CurrentStateSimulationOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CurrentStateSimulationOperation {
    Upsert(CurrentStateOracleRow),
    Delete {
        row_key: Vec<u8>,
    },
    ReviseContract {
        contract: CurrentStateRetentionContract,
    },
    PinSnapshotRows {
        row_keys: Vec<Vec<u8>>,
    },
    ClearSnapshotPins,
    PublishRetainedSet,
    PublishManifest,
    CompleteLocalCleanup,
    Restart,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentStateSimulationStep {
    pub operation: CurrentStateSimulationOperation,
    pub evaluation: CurrentStateRetentionEvaluation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentStateSimulationOutcome {
    pub steps: Vec<CurrentStateSimulationStep>,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CurrentStateSimulationError {
    #[error(transparent)]
    Retention(#[from] CurrentStateRetentionError),
}

pub fn run_current_state_simulation(
    scenario: &CurrentStateSimulationScenario,
) -> Result<CurrentStateSimulationOutcome, CurrentStateSimulationError> {
    let mut coordinator = CurrentStateRetentionCoordinator::new(
        scenario.initial_contract.clone(),
        scenario.coordination.clone(),
    );
    let mut steps = Vec::with_capacity(scenario.operations.len());

    for operation in &scenario.operations {
        match operation {
            CurrentStateSimulationOperation::Upsert(row) => {
                coordinator.apply(CurrentStateOracleMutation::Upsert(row.clone()));
            }
            CurrentStateSimulationOperation::Delete { row_key } => {
                coordinator.apply(CurrentStateOracleMutation::Delete {
                    row_key: row_key.clone(),
                });
            }
            CurrentStateSimulationOperation::ReviseContract { contract } => {
                coordinator.set_contract(contract.clone());
            }
            CurrentStateSimulationOperation::PinSnapshotRows { row_keys } => {
                coordinator.set_snapshot_pins(row_keys.clone());
            }
            CurrentStateSimulationOperation::ClearSnapshotPins => {
                coordinator.clear_snapshot_pins();
            }
            CurrentStateSimulationOperation::PublishManifest => {
                coordinator.publish_manifest();
            }
            CurrentStateSimulationOperation::CompleteLocalCleanup => {
                coordinator.complete_local_cleanup();
            }
            CurrentStateSimulationOperation::PublishRetainedSet => {
                coordinator.publish_retained_set()?;
            }
            CurrentStateSimulationOperation::Restart => {
                coordinator =
                    CurrentStateRetentionCoordinator::from_snapshot(coordinator.snapshot());
            }
        }

        steps.push(CurrentStateSimulationStep {
            operation: operation.clone(),
            evaluation: coordinator.plan()?,
        });
    }

    Ok(CurrentStateSimulationOutcome { steps })
}

pub struct SeededSimulationRunner {
    seed: u64,
    scenario_config: SimulationScenarioConfig,
    simulation_duration: Duration,
    min_message_latency: Duration,
    max_message_latency: Duration,
    hosts: Vec<SimulationHost>,
}

impl SeededSimulationRunner {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            scenario_config: SimulationScenarioConfig::default(),
            simulation_duration: Duration::from_secs(1),
            min_message_latency: Duration::from_millis(1),
            max_message_latency: Duration::from_millis(3),
            hosts: Vec::new(),
        }
    }

    pub fn with_simulation_duration(mut self, duration: Duration) -> Self {
        self.simulation_duration = duration;
        self
    }

    pub fn with_message_latency(mut self, min: Duration, max: Duration) -> Self {
        self.min_message_latency = min;
        self.max_message_latency = max.max(min);
        self
    }

    pub fn with_scenario_config(mut self, config: SimulationScenarioConfig) -> Self {
        self.scenario_config = config;
        self
    }

    pub fn with_host(mut self, host: SimulationHost) -> Self {
        self.hosts.push(host);
        self
    }

    pub fn with_hosts<I>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = SimulationHost>,
    {
        self.hosts.extend(hosts);
        self
    }

    pub fn generate_scenario(&self) -> GeneratedScenario {
        generate_scenario(self.seed, &self.scenario_config)
    }

    pub fn generate_db_scenario(&self, config: &DbSimulationScenarioConfig) -> DbGeneratedScenario {
        generate_db_scenario(self.seed, config)
    }

    pub fn run_generated(&self) -> turmoil::Result<SimulationOutcome> {
        let scenario = self.generate_scenario();
        let seed = self.seed;
        self.run_with(move |ctx| {
            let scenario = scenario.clone();
            async move {
                execute_generated_scenario(&ctx, &scenario).await;
                Ok(SimulationOutcome {
                    seed,
                    scenario,
                    trace: ctx.trace(),
                })
            }
        })
    }

    pub fn run_db_generated(
        &self,
        config: DbSimulationScenarioConfig,
    ) -> turmoil::Result<DbSimulationOutcome> {
        let scenario = self.generate_db_scenario(&config);
        self.run_db_scenario(scenario)
    }

    pub fn run_db_scenario(
        &self,
        scenario: DbGeneratedScenario,
    ) -> turmoil::Result<DbSimulationOutcome> {
        let seed = self.seed;
        self.run_with(move |ctx| {
            let scenario = scenario.clone();
            async move {
                execute_db_scenario(&ctx, &scenario).await?;
                Ok(DbSimulationOutcome {
                    seed,
                    scenario,
                    trace: ctx.trace(),
                })
            }
        })
    }

    pub fn run_with<T, F, Fut>(&self, run: F) -> turmoil::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(SimulationContext) -> Fut + 'static,
        Fut: Future<Output = turmoil::Result<T>> + 'static,
    {
        let mut builder = turmoil_determinism::Builder::new(self.seed);
        builder
            .simulation_duration(self.simulation_duration)
            .tick_duration(Duration::from_millis(1))
            .min_message_latency(self.min_message_latency)
            .max_message_latency(self.max_message_latency)
            .enable_random_order();

        let mut sim = builder.build();
        sim.host(OBJECT_STORE_HOST, || async move {
            run_object_store_host().await
        });
        for host in self.hosts.clone() {
            let name = host.name.clone();
            let run = host.run.clone();
            sim.host(name.as_ref(), move || (run)());
        }

        let result = Arc::new(Mutex::new(None));
        let result_cell = result.clone();
        let seed = self.seed;
        sim.client("driver", async move {
            let context = SimulationContext::new(
                seed,
                Arc::new(SimulatedFileSystem::default()),
                Arc::new(NetworkObjectStore::new(
                    OBJECT_STORE_HOST,
                    OBJECT_STORE_PORT,
                )),
                Arc::new(TurmoilClock),
                Arc::new(DeterministicRng::seeded(seed)),
            );

            context.record(TraceEvent::ScenarioStarted { seed });
            let user_result = run(context.clone()).await;
            let shutdown_result = context.object_store().shutdown().await;
            *lock(&result_cell) = Some(match user_result {
                Ok(value) => shutdown_result.map(|()| value).map_err(Into::into),
                Err(error) => Err(error),
            });
            Ok(())
        });

        sim.run()?;
        lock(&result)
            .take()
            .unwrap_or_else(|| Err("simulation client did not produce a result".into()))
    }
}

fn generate_scenario(seed: u64, config: &SimulationScenarioConfig) -> GeneratedScenario {
    let rng = DeterministicRng::seeded(seed);
    let mut workload = Vec::with_capacity(config.steps);
    let mut faults = Vec::new();

    for step in 0..config.steps {
        let path = format!(
            "/terracedb/sim/file-{}.log",
            choose_index(&rng, config.path_count)
        );
        let key = format!("segments/{}", choose_index(&rng, config.key_count));
        let prefix = format!("segments/{}/", choose_index(&rng, config.key_count));
        let payload = make_payload(&rng, config.max_payload_len, step);
        let op = match rng.next_u64() % 6 {
            0 => WorkloadOperation::FileWrite {
                path: path.clone(),
                data: payload,
                sync: rng.next_u64().is_multiple_of(2),
            },
            1 => WorkloadOperation::FileRead {
                path,
                offset: rng.next_u64() % 4,
                len: ((rng.next_u64() as usize) % config.max_payload_len.max(1)) + 1,
            },
            2 => WorkloadOperation::ObjectPut { key, data: payload },
            3 => WorkloadOperation::ObjectGet { key },
            4 => WorkloadOperation::ObjectList { prefix },
            _ => WorkloadOperation::AdvanceClock {
                millis: (rng.next_u64() % config.max_clock_advance_millis.max(1)) + 1,
            },
        };
        workload.push(op);

        if rng.next_u64().is_multiple_of(5) {
            faults.push(ScheduledFault {
                step,
                cut_point: CutPoint::BeforeStep,
                kind: ScheduledFaultKind::FileSystem(FileSystemFaultSpec::DiskFull {
                    target_prefix: format!(
                        "/terracedb/sim/file-{}",
                        choose_index(&rng, config.path_count)
                    ),
                }),
            });
        }

        if rng.next_u64().is_multiple_of(7) {
            faults.push(ScheduledFault {
                step,
                cut_point: CutPoint::AfterStep,
                kind: ScheduledFaultKind::ObjectStore(ObjectStoreFaultSpec::Timeout {
                    operation: ObjectStoreOperation::Get,
                    target_prefix: format!("segments/{}", choose_index(&rng, config.key_count)),
                }),
            });
        }

        if rng.next_u64().is_multiple_of(11) {
            faults.push(ScheduledFault {
                step,
                cut_point: CutPoint::AfterStep,
                kind: ScheduledFaultKind::Crash,
            });
        }
    }

    GeneratedScenario {
        seed,
        workload,
        faults,
    }
}

fn generate_db_scenario(seed: u64, config: &DbSimulationScenarioConfig) -> DbGeneratedScenario {
    assert!(
        !config.tables.is_empty(),
        "db simulation scenarios require at least one table spec"
    );
    let rng = DeterministicRng::seeded(seed);
    let mut workload = Vec::with_capacity(config.steps);
    let mut faults = Vec::new();

    for step in 0..config.steps {
        let table = &config.tables[choose_index(&rng, config.tables.len().max(1))];
        let key = format!("key-{}", choose_index(&rng, config.key_count)).into_bytes();
        let value = make_payload(&rng, config.max_payload_len, step);
        let operation = match rng.next_u64() % 7 {
            0 => DbWorkloadOperation::Delete {
                table: table.name.clone(),
                key,
            },
            1 | 2 if table.merge_operator.is_some() => DbWorkloadOperation::Merge {
                table: table.name.clone(),
                key,
                value,
            },
            3 => DbWorkloadOperation::ReadLatest {
                table: table.name.clone(),
                key,
            },
            4 => DbWorkloadOperation::Flush,
            5 => DbWorkloadOperation::RunCompaction,
            _ => DbWorkloadOperation::Put {
                table: table.name.clone(),
                key,
                value,
            },
        };
        workload.push(operation);

        if rng.next_u64().is_multiple_of(5) {
            faults.push(ScheduledFault {
                step,
                cut_point: CutPoint::AfterStep,
                kind: ScheduledFaultKind::Crash,
            });
        }
    }

    DbGeneratedScenario {
        seed,
        root_path: config.root_path.clone(),
        tables: config.tables.clone(),
        workload,
        faults,
    }
}

async fn execute_generated_scenario(context: &SimulationContext, scenario: &GeneratedScenario) {
    for (index, operation) in scenario.workload.iter().cloned().enumerate() {
        apply_faults_for_step(context, scenario, index, CutPoint::BeforeStep).await;
        context.record(TraceEvent::StepStarted {
            index,
            operation: operation.clone(),
        });

        let result = match operation {
            WorkloadOperation::FileWrite { path, data, sync } => {
                execute_file_write(context.file_system(), &path, &data, sync).await
            }
            WorkloadOperation::FileRead { path, offset, len } => {
                execute_file_read(context.file_system(), &path, offset, len).await
            }
            WorkloadOperation::ObjectPut { key, data } => {
                execute_object_put(context.object_store(), &key, &data).await
            }
            WorkloadOperation::ObjectGet { key } => {
                execute_object_get(context.object_store(), &key).await
            }
            WorkloadOperation::ObjectList { prefix } => {
                execute_object_list(context.object_store(), &prefix).await
            }
            WorkloadOperation::AdvanceClock { millis } => {
                context.clock().sleep(Duration::from_millis(millis)).await;
                Ok(OperationResult::Bytes(context.clock().now().get() as usize))
            }
        };

        let result = result.unwrap_or_else(|error| OperationResult::Error(error.kind()));
        context.record(TraceEvent::StepResult { index, result });
        apply_faults_for_step(context, scenario, index, CutPoint::AfterStep).await;
    }
}

async fn execute_db_scenario(
    context: &SimulationContext,
    scenario: &DbGeneratedScenario,
) -> turmoil::Result<()> {
    let mut runtime = SimulatedDbRuntime::open(context, scenario).await?;
    let mut oracle = DbShadowOracle::new(&scenario.tables);
    let known_keys = collect_known_db_keys(scenario);

    for (index, operation) in scenario.workload.iter().cloned().enumerate() {
        apply_db_faults_for_step(
            context,
            scenario,
            index,
            CutPoint::BeforeStep,
            &mut runtime,
            &oracle,
            &known_keys,
        )
        .await?;
        context.record(TraceEvent::DbStepStarted {
            index,
            operation: operation.clone(),
        });

        let result =
            execute_db_workload_operation(context, &mut runtime, &mut oracle, operation.clone())
                .await;
        let result = result.unwrap_or_else(|error| OperationResult::Error(error.kind()));
        context.record(TraceEvent::DbStepResult { index, result });

        validate_db_runtime(&runtime, &oracle, &known_keys, context.seed(), index).await?;
        apply_db_faults_for_step(
            context,
            scenario,
            index,
            CutPoint::AfterStep,
            &mut runtime,
            &oracle,
            &known_keys,
        )
        .await?;
    }

    Ok(())
}

struct SimulatedDbRuntime {
    config: DbConfig,
    table_specs: Vec<SimulationTableSpec>,
    db: Db,
    tables: BTreeMap<String, Table>,
}

impl SimulatedDbRuntime {
    async fn open(
        context: &SimulationContext,
        scenario: &DbGeneratedScenario,
    ) -> turmoil::Result<Self> {
        let config = simulation_db_config(&scenario.root_path);
        let db = context.open_db(config.clone()).await?;
        let tables = attach_simulation_tables(&db, &scenario.tables).await?;
        Ok(Self {
            config,
            table_specs: scenario.tables.clone(),
            db,
            tables,
        })
    }

    async fn restart(
        &mut self,
        context: &SimulationContext,
        cut_point: CutPoint,
    ) -> turmoil::Result<()> {
        self.db = context.restart_db(self.config.clone(), cut_point).await?;
        self.tables = attach_simulation_tables(&self.db, &self.table_specs).await?;
        context.record(TraceEvent::DbRecovered {
            current_sequence: self.db.current_sequence(),
            durable_sequence: self.db.current_durable_sequence(),
            table_count: self.tables.len(),
        });
        Ok(())
    }

    fn table(&self, name: &str) -> &Table {
        self.tables
            .get(name)
            .unwrap_or_else(|| panic!("simulation table should be attached: {name}"))
    }
}

fn simulation_db_config(root_path: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: root_path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-sim".to_string(),
                prefix: "merge".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability: TieredDurabilityMode::GroupCommit,
            local_retention: terracedb::TieredLocalRetentionMode::Offload,
        }),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

async fn attach_simulation_tables(
    db: &Db,
    table_specs: &[SimulationTableSpec],
) -> turmoil::Result<BTreeMap<String, Table>> {
    let mut tables = BTreeMap::new();
    for spec in table_specs {
        let table = db.create_table(spec.table_config()).await?;
        tables.insert(spec.name.clone(), table);
    }
    Ok(tables)
}

fn collect_known_db_keys(scenario: &DbGeneratedScenario) -> BTreeMap<String, Vec<Key>> {
    let mut keys = BTreeMap::<String, Vec<Key>>::new();
    for operation in &scenario.workload {
        match operation {
            DbWorkloadOperation::Put { table, key, .. }
            | DbWorkloadOperation::Merge { table, key, .. }
            | DbWorkloadOperation::Delete { table, key }
            | DbWorkloadOperation::ReadLatest { table, key } => {
                let entry = keys.entry(table.clone()).or_default();
                if !entry.contains(key) {
                    entry.push(key.clone());
                }
            }
            DbWorkloadOperation::AdvanceClock { .. }
            | DbWorkloadOperation::Flush
            | DbWorkloadOperation::RunCompaction => {}
        }
    }
    keys
}

async fn apply_db_faults_for_step(
    context: &SimulationContext,
    scenario: &DbGeneratedScenario,
    step: usize,
    cut_point: CutPoint,
    runtime: &mut SimulatedDbRuntime,
    oracle: &DbShadowOracle,
    known_keys: &BTreeMap<String, Vec<Key>>,
) -> turmoil::Result<()> {
    for fault in scenario
        .faults
        .iter()
        .filter(|fault| fault.step == step && fault.cut_point == cut_point)
    {
        match &fault.kind {
            ScheduledFaultKind::FileSystem(spec) => {
                context
                    .file_system()
                    .inject_failure(spec.clone().into_failure());
            }
            ScheduledFaultKind::ObjectStore(spec) => {
                let _ = context.object_store().inject_failure(spec.clone()).await;
            }
            ScheduledFaultKind::Crash => {
                runtime.restart(context, cut_point).await?;
                validate_db_recovery(runtime, oracle, known_keys).await?;
            }
        }

        context.record(TraceEvent::FaultInjected {
            step,
            cut_point,
            kind: fault.kind.clone(),
        });
    }

    Ok(())
}

async fn execute_db_workload_operation(
    context: &SimulationContext,
    runtime: &mut SimulatedDbRuntime,
    oracle: &mut DbShadowOracle,
    operation: DbWorkloadOperation,
) -> Result<OperationResult, StorageError> {
    match operation {
        DbWorkloadOperation::Put { table, key, value } => {
            let sequence = runtime
                .table(&table)
                .write(key.clone(), Value::bytes(value.clone()))
                .await
                .map_err(write_error_to_storage)?;
            let mutation = DbMutation::Put { table, key, value };
            oracle
                .apply(
                    sequence,
                    mutation.clone(),
                    runtime.db.current_durable_sequence() >= sequence,
                )
                .map_err(|error| StorageError::corruption(error.to_string()))?;
            context.record(TraceEvent::DbCommit {
                sequence,
                durable_sequence: runtime.db.current_durable_sequence(),
                mutation,
            });
            Ok(OperationResult::Sequence(sequence))
        }
        DbWorkloadOperation::Merge { table, key, value } => {
            let sequence = runtime
                .table(&table)
                .merge(key.clone(), Value::bytes(value.clone()))
                .await
                .map_err(write_error_to_storage)?;
            let mutation = DbMutation::Merge { table, key, value };
            oracle
                .apply(
                    sequence,
                    mutation.clone(),
                    runtime.db.current_durable_sequence() >= sequence,
                )
                .map_err(|error| StorageError::corruption(error.to_string()))?;
            context.record(TraceEvent::DbCommit {
                sequence,
                durable_sequence: runtime.db.current_durable_sequence(),
                mutation,
            });
            Ok(OperationResult::Sequence(sequence))
        }
        DbWorkloadOperation::Delete { table, key } => {
            let sequence = runtime
                .table(&table)
                .delete(key.clone())
                .await
                .map_err(write_error_to_storage)?;
            let mutation = DbMutation::Delete { table, key };
            oracle
                .apply(
                    sequence,
                    mutation.clone(),
                    runtime.db.current_durable_sequence() >= sequence,
                )
                .map_err(|error| StorageError::corruption(error.to_string()))?;
            context.record(TraceEvent::DbCommit {
                sequence,
                durable_sequence: runtime.db.current_durable_sequence(),
                mutation,
            });
            Ok(OperationResult::Sequence(sequence))
        }
        DbWorkloadOperation::ReadLatest { table, key } => {
            let value = read_table_bytes(runtime.table(&table), key.clone()).await?;
            Ok(OperationResult::Value(value))
        }
        DbWorkloadOperation::AdvanceClock { millis } => {
            context.clock().sleep(Duration::from_millis(millis)).await;
            Ok(OperationResult::Bytes(context.clock().now().get() as usize))
        }
        DbWorkloadOperation::Flush => {
            runtime.db.flush().await.map_err(flush_error_to_storage)?;
            Ok(OperationResult::Unit)
        }
        DbWorkloadOperation::RunCompaction => {
            let compacted = runtime.db.run_next_compaction().await?;
            Ok(OperationResult::Bool(compacted))
        }
    }
}

async fn validate_db_runtime(
    runtime: &SimulatedDbRuntime,
    oracle: &DbShadowOracle,
    known_keys: &BTreeMap<String, Vec<Key>>,
    seed: u64,
    step: usize,
) -> turmoil::Result<()> {
    let visible_sequence = runtime.db.current_sequence();
    let durable_sequence = runtime.db.current_durable_sequence();
    let sampled_sequence = select_validation_sequence(seed, step, visible_sequence);
    let mut sequences = vec![visible_sequence, durable_sequence, sampled_sequence];
    sequences.sort();
    sequences.dedup();

    for (table_name, keys) in known_keys {
        if !oracle.supports_full_history_validation(table_name) {
            continue;
        }
        let table = runtime.table(table_name);
        for sequence in &sequences {
            for key in keys {
                let actual = read_table_bytes_at(table, key.clone(), *sequence).await?;
                oracle.validate_point_state(table_name, key, *sequence, actual.as_deref())?;
            }

            let actual_rows = collect_bytes_rows(
                table
                    .scan_at(Vec::new(), vec![0xff], *sequence, ScanOptions::default())
                    .await
                    .map_err(read_error_to_storage)?,
            )
            .await?;
            let expected_rows = oracle
                .table_state_at(table_name, *sequence)?
                .into_iter()
                .collect::<Vec<_>>();
            if actual_rows != expected_rows {
                return Err(DbOracleError::ScanState {
                    table: table_name.clone(),
                    sequence: *sequence,
                    expected: expected_rows,
                    actual: actual_rows,
                }
                .into());
            }
        }
    }

    Ok(())
}

async fn validate_db_recovery(
    runtime: &SimulatedDbRuntime,
    oracle: &DbShadowOracle,
    known_keys: &BTreeMap<String, Vec<Key>>,
) -> turmoil::Result<()> {
    for (table_name, keys) in known_keys {
        if !oracle.supports_full_history_validation(table_name) {
            continue;
        }
        let table = runtime.table(table_name);
        let mut recovered = BTreeMap::new();
        for key in keys {
            if let Some(value) = read_table_bytes(table, key.clone()).await? {
                recovered.insert(key.clone(), value);
            }
        }
        let _ = oracle.validate_recovery_prefix(table_name, &recovered)?;
    }
    Ok(())
}

fn select_validation_sequence(
    seed: u64,
    step: usize,
    visible_sequence: SequenceNumber,
) -> SequenceNumber {
    if visible_sequence.get() == 0 {
        SequenceNumber::new(0)
    } else {
        let index = seed
            .wrapping_add(step as u64)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15);
        SequenceNumber::new(index % (visible_sequence.get() + 1))
    }
}

async fn read_table_bytes(table: &Table, key: Vec<u8>) -> Result<Option<Vec<u8>>, StorageError> {
    decode_row_value(table.read(key).await.map_err(read_error_to_storage)?)
}

async fn read_table_bytes_at(
    table: &Table,
    key: Vec<u8>,
    sequence: SequenceNumber,
) -> Result<Option<Vec<u8>>, StorageError> {
    decode_row_value(
        table
            .read_at(key, sequence)
            .await
            .map_err(read_error_to_storage)?,
    )
}

async fn collect_bytes_rows(stream: KvStream) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
    let rows = stream.collect::<Vec<_>>().await;
    rows.into_iter()
        .map(|(key, value)| {
            decode_row_value(Some(value))
                .map(|value| (key, value.expect("scan row should contain a value")))
        })
        .collect()
}

fn decode_row_value(value: Option<Value>) -> Result<Option<Vec<u8>>, StorageError> {
    match value {
        Some(Value::Bytes(bytes)) => Ok(Some(bytes)),
        Some(Value::Record(_)) => Err(StorageError::unsupported(
            "row-based simulation harness only supports byte values",
        )),
        None => Ok(None),
    }
}

fn write_error_to_storage(error: WriteError) -> StorageError {
    match error {
        WriteError::Commit(commit) => commit_error_to_storage(commit),
    }
}

fn commit_error_to_storage(error: CommitError) -> StorageError {
    match error {
        CommitError::Storage(error) => error,
        other => {
            StorageError::corruption(format!("unexpected commit error in simulation: {other}"))
        }
    }
}

fn flush_error_to_storage(error: FlushError) -> StorageError {
    match error {
        FlushError::Storage(error) => error,
        FlushError::Unimplemented(message) => StorageError::unsupported(message),
    }
}

fn read_error_to_storage(error: ReadError) -> StorageError {
    match error {
        ReadError::Storage(error) => error,
        other => StorageError::corruption(format!("unexpected read error in simulation: {other}")),
    }
}

async fn apply_faults_for_step(
    context: &SimulationContext,
    scenario: &GeneratedScenario,
    step: usize,
    cut_point: CutPoint,
) {
    for fault in scenario
        .faults
        .iter()
        .filter(|fault| fault.step == step && fault.cut_point == cut_point)
    {
        match &fault.kind {
            ScheduledFaultKind::FileSystem(spec) => {
                context
                    .file_system()
                    .inject_failure(spec.clone().into_failure());
            }
            ScheduledFaultKind::ObjectStore(spec) => {
                let _ = context.object_store().inject_failure(spec.clone()).await;
            }
            ScheduledFaultKind::Crash => {
                context.crash_filesystem(cut_point);
            }
        }

        context.record(TraceEvent::FaultInjected {
            step,
            cut_point,
            kind: fault.kind.clone(),
        });
    }
}

async fn execute_file_write(
    file_system: Arc<SimulatedFileSystem>,
    path: &str,
    data: &[u8],
    sync: bool,
) -> Result<OperationResult, StorageError> {
    let handle = file_system
        .open(
            path,
            OpenOptions {
                create: true,
                read: true,
                write: true,
                truncate: true,
                append: false,
            },
        )
        .await?;
    file_system.write_at(&handle, 0, data).await?;
    if sync {
        file_system.sync(&handle).await?;
    }
    Ok(OperationResult::Bytes(data.len()))
}

async fn execute_file_read(
    file_system: Arc<SimulatedFileSystem>,
    path: &str,
    offset: u64,
    len: usize,
) -> Result<OperationResult, StorageError> {
    let handle = file_system
        .open(
            path,
            OpenOptions {
                create: false,
                read: true,
                write: false,
                truncate: false,
                append: false,
            },
        )
        .await?;
    let bytes = file_system.read_at(&handle, offset, len).await?;
    Ok(OperationResult::Bytes(bytes.len()))
}

async fn execute_object_put(
    object_store: Arc<NetworkObjectStore>,
    key: &str,
    data: &[u8],
) -> Result<OperationResult, StorageError> {
    object_store.put(key, data).await?;
    Ok(OperationResult::Bytes(data.len()))
}

async fn execute_object_get(
    object_store: Arc<NetworkObjectStore>,
    key: &str,
) -> Result<OperationResult, StorageError> {
    let bytes = object_store.get(key).await?;
    Ok(OperationResult::Bytes(bytes.len()))
}

async fn execute_object_list(
    object_store: Arc<NetworkObjectStore>,
    prefix: &str,
) -> Result<OperationResult, StorageError> {
    let keys = object_store.list(prefix).await?;
    Ok(OperationResult::Keys(keys))
}

fn choose_index(rng: &DeterministicRng, bound: usize) -> usize {
    if bound == 0 {
        0
    } else {
        (rng.next_u64() as usize) % bound
    }
}

fn make_payload(rng: &DeterministicRng, max_payload_len: usize, step: usize) -> Vec<u8> {
    let len = ((rng.next_u64() as usize) % max_payload_len.max(1)) + 1;
    (0..len)
        .map(|offset| b'a' + ((step + offset + choose_index(rng, 26)) % 26) as u8)
        .collect()
}

#[derive(Clone, Debug)]
pub struct NetworkObjectStore {
    host: Arc<str>,
    port: u16,
}

impl NetworkObjectStore {
    pub fn new(host: impl Into<Arc<str>>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub async fn inject_failure(&self, failure: ObjectStoreFaultSpec) -> Result<(), StorageError> {
        match self
            .send_request(RemoteObjectStoreRequest::InjectFailure { failure })
            .await?
        {
            RemoteObjectStoreResponse::Unit => Ok(()),
            RemoteObjectStoreResponse::Error(error) => Err(error.into_storage_error()),
            response => Err(StorageError::corruption(format!(
                "unexpected object-store response for inject_failure: {response:?}"
            ))),
        }
    }

    pub async fn shutdown(&self) -> Result<(), StorageError> {
        match self
            .send_request(RemoteObjectStoreRequest::Shutdown)
            .await?
        {
            RemoteObjectStoreResponse::Unit => Ok(()),
            RemoteObjectStoreResponse::Error(error) => Err(error.into_storage_error()),
            response => Err(StorageError::corruption(format!(
                "unexpected object-store response for shutdown: {response:?}"
            ))),
        }
    }

    async fn send_request(
        &self,
        request: RemoteObjectStoreRequest,
    ) -> Result<RemoteObjectStoreResponse, StorageError> {
        let mut stream = TcpStream::connect((self.host.as_ref(), self.port))
            .await
            .map_err(map_network_error)?;
        let payload = serde_json::to_vec(&request)
            .map_err(|error| StorageError::corruption(format!("encode request failed: {error}")))?;
        write_frame(&mut stream, &payload)
            .await
            .map_err(map_network_error)?;
        let payload = read_frame(&mut stream).await.map_err(map_network_error)?;
        serde_json::from_slice(&payload)
            .map_err(|error| StorageError::corruption(format!("decode response failed: {error}")))
    }
}

#[async_trait]
impl ObjectStore for NetworkObjectStore {
    async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        match self
            .send_request(RemoteObjectStoreRequest::Put {
                key: key.to_string(),
                data: data.to_vec(),
            })
            .await?
        {
            RemoteObjectStoreResponse::Unit => Ok(()),
            RemoteObjectStoreResponse::Error(error) => Err(error.into_storage_error()),
            response => Err(StorageError::corruption(format!(
                "unexpected object-store response for put: {response:?}"
            ))),
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        match self
            .send_request(RemoteObjectStoreRequest::Get {
                key: key.to_string(),
            })
            .await?
        {
            RemoteObjectStoreResponse::Bytes(bytes) => Ok(bytes),
            RemoteObjectStoreResponse::Error(error) => Err(error.into_storage_error()),
            response => Err(StorageError::corruption(format!(
                "unexpected object-store response for get: {response:?}"
            ))),
        }
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        match self
            .send_request(RemoteObjectStoreRequest::GetRange {
                key: key.to_string(),
                start,
                end,
            })
            .await?
        {
            RemoteObjectStoreResponse::Bytes(bytes) => Ok(bytes),
            RemoteObjectStoreResponse::Error(error) => Err(error.into_storage_error()),
            response => Err(StorageError::corruption(format!(
                "unexpected object-store response for get_range: {response:?}"
            ))),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        match self
            .send_request(RemoteObjectStoreRequest::Delete {
                key: key.to_string(),
            })
            .await?
        {
            RemoteObjectStoreResponse::Unit => Ok(()),
            RemoteObjectStoreResponse::Error(error) => Err(error.into_storage_error()),
            response => Err(StorageError::corruption(format!(
                "unexpected object-store response for delete: {response:?}"
            ))),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        match self
            .send_request(RemoteObjectStoreRequest::List {
                prefix: prefix.to_string(),
            })
            .await?
        {
            RemoteObjectStoreResponse::Keys(keys) => Ok(keys),
            RemoteObjectStoreResponse::Error(error) => Err(error.into_storage_error()),
            response => Err(StorageError::corruption(format!(
                "unexpected object-store response for list: {response:?}"
            ))),
        }
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
        match self
            .send_request(RemoteObjectStoreRequest::Copy {
                from: from.to_string(),
                to: to.to_string(),
            })
            .await?
        {
            RemoteObjectStoreResponse::Unit => Ok(()),
            RemoteObjectStoreResponse::Error(error) => Err(error.into_storage_error()),
            response => Err(StorageError::corruption(format!(
                "unexpected object-store response for copy: {response:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum RemoteObjectStoreRequest {
    Put { key: String, data: Vec<u8> },
    Get { key: String },
    GetRange { key: String, start: u64, end: u64 },
    Delete { key: String },
    List { prefix: String },
    Copy { from: String, to: String },
    InjectFailure { failure: ObjectStoreFaultSpec },
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum RemoteObjectStoreResponse {
    Unit,
    Bytes(Vec<u8>),
    Keys(Vec<String>),
    Error(RemoteStorageError),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RemoteStorageError {
    kind: StorageErrorKind,
    message: String,
}

impl RemoteStorageError {
    fn from_storage_error(error: StorageError) -> Self {
        Self {
            kind: error.kind(),
            message: error.message().to_string(),
        }
    }

    fn into_storage_error(self) -> StorageError {
        StorageError::new(self.kind, self.message)
    }
}

async fn run_object_store_host() -> turmoil::Result {
    let listener = TcpListener::bind(("0.0.0.0", OBJECT_STORE_PORT)).await?;
    let store = Arc::new(Mutex::new(RemoteObjectStoreState::default()));

    loop {
        let (mut stream, _peer) = listener.accept().await?;
        let payload = read_frame(&mut stream).await?;
        let request: RemoteObjectStoreRequest = serde_json::from_slice(&payload)?;
        let (response, should_stop) = handle_object_store_request(&store, request);
        let payload = serde_json::to_vec(&response)?;
        write_frame(&mut stream, &payload).await?;
        if should_stop {
            return Ok(());
        }
    }
}

#[derive(Debug, Default)]
struct RemoteObjectStoreState {
    objects: BTreeMap<String, Vec<u8>>,
    failures: Vec<ObjectStoreFaultSpec>,
}

fn handle_object_store_request(
    store: &Arc<Mutex<RemoteObjectStoreState>>,
    request: RemoteObjectStoreRequest,
) -> (RemoteObjectStoreResponse, bool) {
    use RemoteObjectStoreRequest as Request;

    let mut state = lock(store);
    let response = match request {
        Request::Put { key, data } => map_remote_unit(
            apply_remote_failure(&mut state, ObjectStoreOperation::Put, &key).map(|()| {
                state.objects.insert(key, data);
            }),
        ),
        Request::Get { key } => map_remote_bytes(
            apply_remote_failure(&mut state, ObjectStoreOperation::Get, &key).and_then(|()| {
                state
                    .objects
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| StorageError::not_found(format!("missing key: {key}")))
            }),
        ),
        Request::GetRange { key, start, end } => map_remote_bytes(
            apply_remote_failure(&mut state, ObjectStoreOperation::GetRange, &key).and_then(|()| {
                let object = state
                    .objects
                    .get(&key)
                    .ok_or_else(|| StorageError::not_found(format!("missing key: {key}")))?;
                let start = start as usize;
                let end = (end as usize).min(object.len());
                if start >= end {
                    Ok(Vec::new())
                } else {
                    Ok(object[start..end].to_vec())
                }
            }),
        ),
        Request::Delete { key } => map_remote_unit(
            apply_remote_failure(&mut state, ObjectStoreOperation::Delete, &key).map(|()| {
                state.objects.remove(&key);
            }),
        ),
        Request::List { prefix } => map_remote_keys(
            apply_remote_failure(&mut state, ObjectStoreOperation::List, &prefix).map(|()| {
                let mut keys = state
                    .objects
                    .keys()
                    .filter(|key| key.starts_with(&prefix))
                    .cloned()
                    .collect::<Vec<_>>();
                keys.sort();
                keys
            }),
        ),
        Request::Copy { from, to } => map_remote_unit(
            apply_remote_failure(&mut state, ObjectStoreOperation::Copy, &from).and_then(|()| {
                let value = state
                    .objects
                    .get(&from)
                    .cloned()
                    .ok_or_else(|| StorageError::not_found(format!("missing key: {from}")))?;
                state.objects.insert(to, value);
                Ok(())
            }),
        ),
        Request::InjectFailure { failure } => {
            state.failures.push(failure);
            RemoteObjectStoreResponse::Unit
        }
        Request::Shutdown => return (RemoteObjectStoreResponse::Unit, true),
    };

    (response, false)
}

fn apply_remote_failure(
    state: &mut RemoteObjectStoreState,
    operation: ObjectStoreOperation,
    target: &str,
) -> Result<(), StorageError> {
    if let Some(index) = state
        .failures
        .iter()
        .position(|failure| remote_failure_matches(failure, operation, target))
    {
        return Err(remote_failure_error(state.failures.remove(index)));
    }

    Ok(())
}

fn remote_failure_matches(
    failure: &ObjectStoreFaultSpec,
    operation: ObjectStoreOperation,
    target: &str,
) -> bool {
    match failure {
        ObjectStoreFaultSpec::Timeout {
            operation: expected,
            target_prefix,
        }
        | ObjectStoreFaultSpec::PartialRead {
            operation: expected,
            target_prefix,
        } => *expected == operation && target.starts_with(target_prefix),
        ObjectStoreFaultSpec::StaleList { prefix } => {
            operation == ObjectStoreOperation::List && target.starts_with(prefix)
        }
    }
}

fn remote_failure_error(failure: ObjectStoreFaultSpec) -> StorageError {
    match failure {
        ObjectStoreFaultSpec::Timeout { .. } => StorageError::timeout("simulated timeout"),
        ObjectStoreFaultSpec::StaleList { .. } => {
            StorageError::durability_boundary("simulated stale list")
        }
        ObjectStoreFaultSpec::PartialRead { .. } => {
            StorageError::corruption("simulated partial read")
        }
    }
}

fn map_remote_unit(result: Result<(), StorageError>) -> RemoteObjectStoreResponse {
    match result {
        Ok(()) => RemoteObjectStoreResponse::Unit,
        Err(error) => {
            RemoteObjectStoreResponse::Error(RemoteStorageError::from_storage_error(error))
        }
    }
}

fn map_remote_bytes(result: Result<Vec<u8>, StorageError>) -> RemoteObjectStoreResponse {
    match result {
        Ok(bytes) => RemoteObjectStoreResponse::Bytes(bytes),
        Err(error) => {
            RemoteObjectStoreResponse::Error(RemoteStorageError::from_storage_error(error))
        }
    }
}

fn map_remote_keys(result: Result<Vec<String>, StorageError>) -> RemoteObjectStoreResponse {
    match result {
        Ok(keys) => RemoteObjectStoreResponse::Keys(keys),
        Err(error) => {
            RemoteObjectStoreResponse::Error(RemoteStorageError::from_storage_error(error))
        }
    }
}

fn map_network_error(error: std::io::Error) -> StorageError {
    match error.kind() {
        std::io::ErrorKind::TimedOut
        | std::io::ErrorKind::WouldBlock
        | std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::BrokenPipe => {
            StorageError::timeout(format!("simulated object-store network error: {error}"))
        }
        _ => StorageError::io(format!("simulated object-store network error: {error}")),
    }
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, payload: &[u8]) -> std::io::Result<()> {
    writer.write_u32_le(payload.len() as u32).await?;
    writer.write_all(payload).await?;
    writer.flush().await
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let len = reader.read_u32_le().await? as usize;
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PointMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PointVersion {
    sequence: SequenceNumber,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShadowOracle {
    versions: BTreeMap<Vec<u8>, Vec<PointVersion>>,
    durable_sequence: SequenceNumber,
    max_sequence: SequenceNumber,
}

impl ShadowOracle {
    pub fn apply(&mut self, sequence: SequenceNumber, mutation: PointMutation, durable: bool) {
        match mutation {
            PointMutation::Put { key, value } => {
                self.versions.entry(key).or_default().push(PointVersion {
                    sequence,
                    value: Some(value),
                })
            }
            PointMutation::Delete { key } => {
                self.versions.entry(key).or_default().push(PointVersion {
                    sequence,
                    value: None,
                })
            }
        }

        self.max_sequence = sequence;
        if durable {
            self.durable_sequence = sequence;
        }
    }

    pub fn durable_sequence(&self) -> SequenceNumber {
        self.durable_sequence
    }

    pub fn max_sequence(&self) -> SequenceNumber {
        self.max_sequence
    }

    pub fn value_at(&self, key: &[u8], sequence: SequenceNumber) -> Option<Vec<u8>> {
        self.versions
            .get(key)
            .and_then(|versions| {
                versions
                    .iter()
                    .rev()
                    .find(|version| version.sequence <= sequence)
            })
            .and_then(|version| version.value.clone())
    }

    pub fn point_state_at(&self, sequence: SequenceNumber) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut state = BTreeMap::new();
        for key in self.versions.keys() {
            if let Some(value) = self.value_at(key, sequence) {
                state.insert(key.clone(), value);
            }
        }
        state
    }

    pub fn validate_sequence_ordering(&self) -> Result<(), OracleError> {
        for versions in self.versions.values() {
            for pair in versions.windows(2) {
                if pair[0].sequence >= pair[1].sequence {
                    return Err(OracleError::SequenceOrdering {
                        previous: pair[0].sequence,
                        next: pair[1].sequence,
                    });
                }
            }
        }
        Ok(())
    }

    pub fn validate_point_state(
        &self,
        key: &[u8],
        sequence: SequenceNumber,
        actual: Option<&[u8]>,
    ) -> Result<(), OracleError> {
        let expected = self.value_at(key, sequence);
        if expected.as_deref() == actual {
            Ok(())
        } else {
            Err(OracleError::PointState {
                key: key.to_vec(),
                sequence,
                expected,
                actual: actual.map(ToOwned::to_owned),
            })
        }
    }

    pub fn validate_recovery_prefix(
        &self,
        recovered: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<RecoveryMatch, OracleError> {
        for seq in self.durable_sequence.get()..=self.max_sequence.get() {
            let sequence = SequenceNumber::new(seq);
            if self.point_state_at(sequence) == *recovered {
                return Ok(RecoveryMatch {
                    matched_sequence: sequence,
                    durable_sequence: self.durable_sequence,
                });
            }
        }

        Err(OracleError::RecoveryPrefix {
            durable_sequence: self.durable_sequence,
            max_sequence: self.max_sequence,
            recovered_key_count: recovered.len(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryMatch {
    pub matched_sequence: SequenceNumber,
    pub durable_sequence: SequenceNumber,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OracleError {
    #[error("sequence ordering violation: {previous} followed by {next}")]
    SequenceOrdering {
        previous: SequenceNumber,
        next: SequenceNumber,
    },
    #[error("point-state mismatch at sequence {sequence}")]
    PointState {
        key: Vec<u8>,
        sequence: SequenceNumber,
        expected: Option<Vec<u8>>,
        actual: Option<Vec<u8>>,
    },
    #[error(
        "recovered state did not match any valid prefix between durable {durable_sequence} and max {max_sequence}"
    )]
    RecoveryPrefix {
        durable_sequence: SequenceNumber,
        max_sequence: SequenceNumber,
        recovered_key_count: usize,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DbOracleError {
    #[error("simulation table is not registered: {table}")]
    UnknownTable { table: String },
    #[error("change ordering violation: {previous:?} followed by {next:?}")]
    ChangeOrdering {
        previous: LogCursor,
        next: LogCursor,
    },
    #[error("point-state mismatch for table {table} at sequence {sequence}")]
    PointState {
        table: String,
        key: Vec<u8>,
        sequence: SequenceNumber,
        expected: Option<Vec<u8>>,
        actual: Option<Vec<u8>>,
    },
    #[error(
        "recovered state for table {table} did not match any valid prefix between durable {durable_sequence} and max {max_sequence}"
    )]
    RecoveryPrefix {
        table: String,
        durable_sequence: SequenceNumber,
        max_sequence: SequenceNumber,
        recovered_key_count: usize,
    },
    #[error("scan-state mismatch for table {table} at sequence {sequence}")]
    ScanState {
        table: String,
        sequence: SequenceNumber,
        expected: Vec<(Vec<u8>, Vec<u8>)>,
        actual: Vec<(Vec<u8>, Vec<u8>)>,
    },
    #[error("merge resolution failed for table {table} key {key:?}: {message}")]
    MergeResolution {
        table: String,
        key: Vec<u8>,
        message: String,
    },
    #[error(
        "table {table} key {key:?} produced a non-byte value unsupported by the simulation oracle"
    )]
    UnsupportedValue { table: String, key: Vec<u8> },
}

pub struct StubDbProcess {
    file_system: Arc<dyn FileSystem>,
    handle: FileHandle,
    log_len: u64,
    state: BTreeMap<Vec<u8>, Vec<u8>>,
    current_sequence: SequenceNumber,
    durable_sequence: SequenceNumber,
}

impl StubDbProcess {
    pub async fn open(dependencies: DbDependencies) -> Result<Self, StorageError> {
        let handle = dependencies
            .file_system
            .open(
                STUB_DB_LOG_PATH,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: false,
                    append: false,
                },
            )
            .await?;
        let bytes = read_all_file(dependencies.file_system.clone(), &handle).await?;
        let mut state = BTreeMap::new();
        let mut current_sequence = SequenceNumber::new(0);
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let record: StubCommitRecord = serde_json::from_slice(line).map_err(|error| {
                StorageError::corruption(format!("decode stub commit failed: {error}"))
            })?;
            apply_point_mutation(&mut state, record.mutation.clone());
            current_sequence = record.sequence;
        }

        Ok(Self {
            file_system: dependencies.file_system,
            handle,
            log_len: bytes.len() as u64,
            state,
            current_sequence,
            durable_sequence: current_sequence,
        })
    }

    pub async fn apply(
        &mut self,
        mutation: PointMutation,
        durable_on_commit: bool,
    ) -> Result<SequenceNumber, StorageError> {
        let sequence = SequenceNumber::new(self.current_sequence.get() + 1);
        let record = StubCommitRecord {
            sequence,
            mutation: mutation.clone(),
        };
        let payload = serde_json::to_vec(&record).map_err(|error| {
            StorageError::corruption(format!("encode stub commit failed: {error}"))
        })?;
        self.file_system
            .write_at(&self.handle, self.log_len, &payload)
            .await?;
        self.file_system
            .write_at(&self.handle, self.log_len + payload.len() as u64, b"\n")
            .await?;
        self.log_len += payload.len() as u64 + 1;
        apply_point_mutation(&mut self.state, mutation);
        self.current_sequence = sequence;

        if durable_on_commit {
            self.flush().await?;
        }

        Ok(sequence)
    }

    pub async fn flush(&mut self) -> Result<(), StorageError> {
        self.file_system.sync(&self.handle).await?;
        self.durable_sequence = self.current_sequence;
        Ok(())
    }

    pub fn current_sequence(&self) -> SequenceNumber {
        self.current_sequence
    }

    pub fn durable_sequence(&self) -> SequenceNumber {
        self.durable_sequence
    }

    pub fn read(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.state.get(key).cloned()
    }

    pub fn state(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        &self.state
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StubCommitRecord {
    sequence: SequenceNumber,
    mutation: PointMutation,
}

fn apply_point_mutation(state: &mut BTreeMap<Vec<u8>, Vec<u8>>, mutation: PointMutation) {
    match mutation {
        PointMutation::Put { key, value } => {
            state.insert(key, value);
        }
        PointMutation::Delete { key } => {
            state.remove(&key);
        }
    }
}

async fn read_all_file(
    file_system: Arc<dyn FileSystem>,
    handle: &FileHandle,
) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    let mut offset = 0;

    loop {
        let chunk = file_system.read_at(handle, offset, IO_CHUNK_LEN).await?;
        if chunk.is_empty() {
            break;
        }
        offset += chunk.len() as u64;
        bytes.extend_from_slice(&chunk);
        if chunk.len() < IO_CHUNK_LEN {
            break;
        }
    }

    Ok(bytes)
}
