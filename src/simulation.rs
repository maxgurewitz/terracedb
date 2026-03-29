use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use async_trait::async_trait;
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

use crate::{
    Clock, Db, DbConfig, DbDependencies, DeterministicRng, FileSystem, FileSystemFailure,
    FileSystemOperation, ObjectStore, ObjectStoreOperation, OpenError, OpenOptions, Rng,
    SequenceNumber, SimulatedFileSystem, StorageError, StorageErrorKind, Timestamp,
};

const OBJECT_STORE_HOST: &str = "object-store";
const OBJECT_STORE_PORT: u16 = 9400;
const STUB_DB_LOG_PATH: &str = "/terracedb/sim/stub-db.log";
const IO_CHUNK_LEN: usize = 4096;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("simulation mutex poisoned")
}

pub fn seed_mad_turmoil(seed: u64) {
    if let Some(mut rng) = mad_turmoil::rand::try_rng() {
        *rng = StdRng::seed_from_u64(seed);
    } else {
        mad_turmoil::rand::set_rng(StdRng::seed_from_u64(seed));
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TurmoilClock;

#[async_trait]
impl crate::Clock for TurmoilClock {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedScenario {
    pub seed: u64,
    pub workload: Vec<WorkloadOperation>,
    pub faults: Vec<ScheduledFault>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationResult {
    Unit,
    Bytes(usize),
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

#[derive(Clone)]
pub struct SimulationContext {
    seed: u64,
    trace: Arc<Mutex<Vec<TraceEvent>>>,
    file_system: Arc<SimulatedFileSystem>,
    object_store: Arc<NetworkObjectStore>,
    clock: Arc<TurmoilClock>,
    rng: Arc<DeterministicRng>,
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
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn dependencies(&self) -> DbDependencies {
        DbDependencies::new(
            self.file_system.clone(),
            self.object_store.clone(),
            self.clock.clone(),
            self.rng.clone(),
        )
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

pub struct SeededSimulationRunner {
    seed: u64,
    scenario_config: SimulationScenarioConfig,
    simulation_duration: Duration,
    min_message_latency: Duration,
    max_message_latency: Duration,
}

impl SeededSimulationRunner {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            scenario_config: SimulationScenarioConfig::default(),
            simulation_duration: Duration::from_secs(1),
            min_message_latency: Duration::from_millis(1),
            max_message_latency: Duration::from_millis(3),
        }
    }

    pub fn with_scenario_config(mut self, config: SimulationScenarioConfig) -> Self {
        self.scenario_config = config;
        self
    }

    pub fn generate_scenario(&self) -> GeneratedScenario {
        generate_scenario(self.seed, &self.scenario_config)
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

    pub fn run_with<T, F, Fut>(&self, run: F) -> turmoil::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(SimulationContext) -> Fut + 'static,
        Fut: Future<Output = turmoil::Result<T>> + 'static,
    {
        seed_mad_turmoil(self.seed);
        let _clock_guard = mad_turmoil::time::SimClocksGuard::init();

        let mut builder = turmoil::Builder::new();
        builder
            .rng_seed(self.seed)
            .simulation_duration(self.simulation_duration)
            .tick_duration(Duration::from_millis(1))
            .min_message_latency(self.min_message_latency)
            .max_message_latency(self.max_message_latency)
            .enable_random_order();

        let mut sim = builder.build();
        sim.host(OBJECT_STORE_HOST, || async move {
            run_object_store_host().await
        });

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

pub struct StubDbProcess {
    file_system: Arc<dyn FileSystem>,
    handle: crate::FileHandle,
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
    handle: &crate::FileHandle,
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
