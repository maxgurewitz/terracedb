use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use serde::Serialize;
use terracedb::{
    ChangeFeedError, Clock, Db, DbDependencies, DeterministicRng, FileSystemFailure,
    FileSystemOperation, LogCursor, ObjectStoreFailure, ObjectStoreOperation, OutboxEntry,
    Rng as TerraceRng, ScanOptions, SequenceNumber, StorageError, StorageErrorKind, StubClock,
    StubFileSystem, StubObjectStore, StubRng, Table, TieredDurabilityMode, Timestamp, Transaction,
    Value,
    test_support::{
        FailpointMode, db_failpoint_registry, row_table_config, test_dependencies,
        test_dependencies_with_clock, tiered_test_config_with_durability,
    },
};
use terracedb_fuzz::{assert_seed_replays, assert_seed_variation};
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, PackageCompatibilityMode, SandboxConfig, SandboxServices,
    SandboxStore,
};
use terracedb_simulation::{
    CutPoint, SeededSimulationRunner, SimulationStackBuilder, TerracedbSimulationHarness,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};
use terracedb_workflows::{
    ContractWorkflowHandler, DEFAULT_TIMER_POLL_INTERVAL, RecurringSchedule, RecurringTickOutput,
    RecurringWorkflowDefinition, RecurringWorkflowHandle, RecurringWorkflowHandler,
    RecurringWorkflowRuntime, RecurringWorkflowState, WorkflowCheckpointArtifactKind,
    WorkflowCheckpointId, WorkflowCheckpointStore, WorkflowContext, WorkflowDefinition,
    WorkflowError, WorkflowHandle, WorkflowHandler, WorkflowHandlerError,
    WorkflowHistoricalArtifactSupport, WorkflowHistoricalEvent, WorkflowHistoricalSourceResolution,
    WorkflowHistoricalSourceScenario, WorkflowObjectStoreCheckpointStore, WorkflowOutput,
    WorkflowProgressMode, WorkflowReplayableSourceKind, WorkflowRuntime, WorkflowSource,
    WorkflowSourceAttachMode, WorkflowSourceBootstrapPolicy, WorkflowSourceConfig,
    WorkflowSourceProgress, WorkflowSourceProgressOrigin, WorkflowSourceRecoveryPolicy,
    WorkflowSourceResumePoint, WorkflowStateMutation, WorkflowTables, WorkflowTimerCommand,
    contracts::{
        self, WorkflowHistoryEvent, WorkflowLifecycleRecord, WorkflowLifecycleState,
        WorkflowPayload, WorkflowRunId, WorkflowStateRecord, WorkflowVisibilityRecord,
    },
    failpoints::names as workflow_failpoint_names,
    sandbox_contracts::SandboxModuleWorkflowTaskV1Handler,
};

struct GeneratedSeedHarness<T> {
    generate: fn(u64) -> T,
}

impl<T> terracedb_fuzz::GeneratedScenarioHarness for GeneratedSeedHarness<T>
where
    T: Clone + std::fmt::Debug + PartialEq + Eq,
{
    type Scenario = u64;
    type Outcome = T;
    type Error = std::convert::Infallible;

    fn generate(&self, seed: u64) -> Self::Scenario {
        seed
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        Ok((self.generate)(scenario))
    }
}
use tokio::sync::Notify;

const WORKFLOW_SIMULATION_DURATION: Duration = Duration::from_millis(600);
const WORKFLOW_TIMER_SIMULATION_DURATION: Duration = Duration::from_millis(1_200);
const SIMULATION_MIN_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const SIMULATION_MAX_MESSAGE_LATENCY: Duration = Duration::from_millis(1);

fn decode_count(value: Option<Value>) -> usize {
    match value {
        None => 0,
        Some(Value::Bytes(bytes)) => std::str::from_utf8(&bytes)
            .expect("state bytes should be utf-8")
            .parse()
            .expect("state should encode a count"),
        Some(Value::Record(_)) => panic!("tests only expect byte state"),
    }
}

#[derive(Clone, Default)]
struct ExecutionStats {
    order: Arc<Mutex<Vec<String>>>,
    active: Arc<Mutex<BTreeMap<String, usize>>>,
    max_active: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl ExecutionStats {
    fn enter(&self, instance_id: &str) {
        self.order
            .lock()
            .expect("order lock poisoned")
            .push(instance_id.to_string());

        let mut active = self.active.lock().expect("active lock poisoned");
        let next = active.get(instance_id).copied().unwrap_or_default() + 1;
        active.insert(instance_id.to_string(), next);

        let mut max_active = self.max_active.lock().expect("max-active lock poisoned");
        let current_max = max_active.get(instance_id).copied().unwrap_or_default();
        if next > current_max {
            max_active.insert(instance_id.to_string(), next);
        }
    }

    fn exit(&self, instance_id: &str) {
        let mut active = self.active.lock().expect("active lock poisoned");
        let next = active
            .get(instance_id)
            .copied()
            .expect("instance must be active")
            - 1;
        active.insert(instance_id.to_string(), next);
    }
}

struct RecordingHandler {
    stats: ExecutionStats,
}

#[async_trait]
impl WorkflowHandler for RecordingHandler {
    async fn route_event(
        &self,
        entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        let key = std::str::from_utf8(&entry.key).expect("source key should be utf-8");
        Ok(key
            .split_once(':')
            .expect("source key should contain an instance prefix")
            .0
            .to_string())
    }

    async fn handle(
        &self,
        instance_id: &str,
        state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        self.stats.enter(instance_id);
        tokio::task::yield_now().await;
        self.stats.exit(instance_id);

        let next = decode_count(state) + 1;
        let payload = match trigger {
            terracedb_workflows::WorkflowTrigger::Event(entry) => entry.key.clone(),
            terracedb_workflows::WorkflowTrigger::Timer { payload, .. } => payload.clone(),
            terracedb_workflows::WorkflowTrigger::Callback { response, .. } => response.clone(),
        };

        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(Value::bytes(next.to_string())),
            outbox_entries: vec![OutboxEntry {
                outbox_id: format!("{instance_id}:{next}").into_bytes(),
                idempotency_key: format!("{instance_id}:{next}"),
                payload,
            }],
            timers: Vec::new(),
        })
    }
}

struct CallbackReplayHandler;

#[async_trait]
impl WorkflowHandler for CallbackReplayHandler {
    async fn route_event(
        &self,
        _entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        Err(WorkflowHandlerError::new(std::io::Error::other(
            "callback workflow does not route source events",
        )))
    }

    async fn handle(
        &self,
        instance_id: &str,
        _state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        let terracedb_workflows::WorkflowTrigger::Callback {
            callback_id,
            response,
        } = trigger
        else {
            panic!("replay test only admits callbacks");
        };

        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(Value::bytes(format!("processed:{callback_id}"))),
            outbox_entries: vec![OutboxEntry {
                outbox_id: format!("{instance_id}:{callback_id}").into_bytes(),
                idempotency_key: format!("{instance_id}:{callback_id}"),
                payload: response.clone(),
            }],
            timers: Vec::new(),
        })
    }
}

struct TimerHandler;

#[async_trait]
impl WorkflowHandler for TimerHandler {
    async fn route_event(
        &self,
        _entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        Err(WorkflowHandlerError::new(std::io::Error::other(
            "timer workflow does not route source events",
        )))
    }

    async fn handle(
        &self,
        _instance_id: &str,
        _state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        match trigger {
            terracedb_workflows::WorkflowTrigger::Callback { .. } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("scheduled")),
                outbox_entries: Vec::new(),
                timers: vec![WorkflowTimerCommand::Schedule {
                    timer_id: b"payment-timeout".to_vec(),
                    fire_at: terracedb::Timestamp::new(5),
                    payload: b"timeout".to_vec(),
                }],
            }),
            terracedb_workflows::WorkflowTrigger::Timer { .. } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("fired")),
                outbox_entries: Vec::new(),
                timers: Vec::new(),
            }),
            terracedb_workflows::WorkflowTrigger::Event(_) => {
                panic!("timer workflow does not process source events")
            }
        }
    }
}

struct CheckpointCaptureHandler;

#[async_trait]
impl WorkflowHandler for CheckpointCaptureHandler {
    async fn route_event(
        &self,
        entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        let key = std::str::from_utf8(&entry.key).expect("source key should be utf-8");
        Ok(key
            .split_once(':')
            .expect("source key should contain an instance prefix")
            .0
            .to_string())
    }

    async fn handle(
        &self,
        instance_id: &str,
        _state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        match trigger {
            terracedb_workflows::WorkflowTrigger::Event(entry) => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes(format!(
                    "event:{}",
                    String::from_utf8_lossy(&entry.key)
                ))),
                outbox_entries: vec![OutboxEntry {
                    outbox_id: format!("{instance_id}:event").into_bytes(),
                    idempotency_key: format!("{instance_id}:event"),
                    payload: entry.key.clone(),
                }],
                timers: vec![WorkflowTimerCommand::Schedule {
                    timer_id: b"follow-up".to_vec(),
                    fire_at: Timestamp::new(50),
                    payload: b"follow-up".to_vec(),
                }],
            }),
            terracedb_workflows::WorkflowTrigger::Callback {
                callback_id,
                response,
            } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes(format!("callback:{callback_id}"))),
                outbox_entries: vec![OutboxEntry {
                    outbox_id: format!("{instance_id}:{callback_id}").into_bytes(),
                    idempotency_key: format!("{instance_id}:{callback_id}"),
                    payload: response.clone(),
                }],
                timers: Vec::new(),
            }),
            terracedb_workflows::WorkflowTrigger::Timer { payload, .. } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("timer-fired")),
                outbox_entries: vec![OutboxEntry {
                    outbox_id: format!("{instance_id}:timer").into_bytes(),
                    idempotency_key: format!("{instance_id}:timer"),
                    payload: payload.clone(),
                }],
                timers: Vec::new(),
            }),
        }
    }
}

struct RecurringTimerHandler;

#[async_trait]
impl RecurringWorkflowHandler for RecurringTimerHandler {
    async fn tick(
        &self,
        instance_id: &str,
        state: &RecurringWorkflowState,
        fire_at: Timestamp,
        _ctx: &WorkflowContext,
    ) -> Result<RecurringTickOutput, WorkflowHandlerError> {
        let tick = state.tick_count + 1;
        Ok(RecurringTickOutput {
            outbox_entries: vec![OutboxEntry {
                outbox_id: format!("{instance_id}:{tick}").into_bytes(),
                idempotency_key: format!("{instance_id}:{tick}"),
                payload: fire_at.get().to_be_bytes().to_vec(),
            }],
            timers: Vec::new(),
        })
    }
}

#[derive(Clone, Default)]
struct BlockingResumeControl {
    callback_entered: Arc<Notify>,
    release_callback: Arc<Notify>,
    order: Arc<Mutex<Vec<String>>>,
}

impl BlockingResumeControl {
    fn record(&self, entry: impl Into<String>) {
        self.order
            .lock()
            .expect("blocking resume order lock poisoned")
            .push(entry.into());
    }

    fn snapshot(&self) -> Vec<String> {
        self.order
            .lock()
            .expect("blocking resume order lock poisoned")
            .clone()
    }
}

struct BlockingResumeHandler {
    control: BlockingResumeControl,
}

#[async_trait]
impl WorkflowHandler for BlockingResumeHandler {
    async fn route_event(
        &self,
        entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        let key = std::str::from_utf8(&entry.key).expect("source key should be utf-8");
        Ok(key
            .split_once(':')
            .expect("source key should contain an instance prefix")
            .0
            .to_string())
    }

    async fn handle(
        &self,
        instance_id: &str,
        state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        match trigger {
            terracedb_workflows::WorkflowTrigger::Callback { callback_id, .. } => {
                self.control
                    .record(format!("{instance_id}:callback:{callback_id}:entered"));
                self.control.callback_entered.notify_one();
                self.control.release_callback.notified().await;
                self.control
                    .record(format!("{instance_id}:callback:{callback_id}:completed"));
            }
            terracedb_workflows::WorkflowTrigger::Event(entry) => {
                self.control.record(format!(
                    "{instance_id}:event:{}",
                    std::str::from_utf8(&entry.key).expect("source key should be utf-8")
                ));
            }
            terracedb_workflows::WorkflowTrigger::Timer { .. } => {
                panic!("blocking resume test only uses callbacks and source events")
            }
        }

        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(Value::bytes((decode_count(state) + 1).to_string())),
            outbox_entries: Vec::new(),
            timers: Vec::new(),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct ContractParityLogic;

impl ContractParityLogic {
    fn route(&self, event: &contracts::WorkflowSourceEvent) -> String {
        String::from_utf8_lossy(&event.key)
            .split_once(':')
            .expect("event key should contain an instance prefix")
            .0
            .to_string()
    }

    fn handle(
        &self,
        input: &contracts::WorkflowTransitionInput,
    ) -> contracts::WorkflowTransitionOutput {
        let sequence = input.history_len.saturating_add(1);
        let id = format!("{}:{sequence}", input.instance_id);
        let payload = match &input.trigger {
            contracts::WorkflowTrigger::Event { event } => event.key.clone(),
            contracts::WorkflowTrigger::Timer { payload, .. } => payload.clone(),
            contracts::WorkflowTrigger::Callback { response, .. } => response.clone(),
        };
        contracts::WorkflowTransitionOutput {
            state: contracts::WorkflowStateMutation::Put {
                state: contracts::WorkflowPayload::bytes(format!(
                    "contract:{}:{sequence}",
                    input.workflow_name
                )),
            },
            lifecycle: None,
            visibility: None,
            continue_as_new: None,
            commands: vec![contracts::WorkflowCommand::Outbox {
                entry: contracts::WorkflowOutboxCommand {
                    outbox_id: id.clone().into_bytes(),
                    idempotency_key: id,
                    payload,
                },
            }],
        }
    }
}

#[derive(Debug)]
struct NativeContractHandler {
    logic: ContractParityLogic,
}

#[async_trait]
impl contracts::WorkflowHandlerContract for NativeContractHandler {
    async fn route_event(
        &self,
        event: &contracts::WorkflowSourceEvent,
    ) -> Result<String, contracts::WorkflowTaskError> {
        Ok(self.logic.route(event))
    }

    async fn handle_task(
        &self,
        input: contracts::WorkflowTransitionInput,
        _ctx: contracts::WorkflowDeterministicContext,
    ) -> Result<contracts::WorkflowTransitionOutput, contracts::WorkflowTaskError> {
        Ok(self.logic.handle(&input))
    }
}

fn contract_bundle(module: &str) -> contracts::WorkflowBundleMetadata {
    contracts::WorkflowBundleMetadata {
        bundle_id: contracts::WorkflowBundleId::new(format!("bundle:{module}")).expect("bundle id"),
        workflow_name: "contract-billing".to_string(),
        kind: contracts::WorkflowBundleKind::Sandbox {
            abi: terracedb_workflows::sandbox_contracts::WORKFLOW_TASK_V1_ABI.to_string(),
            module: module.to_string(),
            entrypoint: "default".to_string(),
        },
        created_at_millis: 1,
        labels: BTreeMap::from([(
            terracedb_workflows::WORKFLOW_RUNTIME_SURFACE_LABEL.to_string(),
            terracedb_workflows::WORKFLOW_RUNTIME_SURFACE_STATE_OUTBOX_TIMERS_V1.to_string(),
        )]),
    }
}

fn native_contract_bundle() -> contracts::WorkflowBundleMetadata {
    contracts::WorkflowBundleMetadata {
        bundle_id: contracts::WorkflowBundleId::new("bundle:native-contract").expect("bundle id"),
        workflow_name: "contract-billing".to_string(),
        kind: contracts::WorkflowBundleKind::NativeRust {
            registration: "tests/native-contract".to_string(),
        },
        created_at_millis: 1,
        labels: BTreeMap::from([(
            terracedb_workflows::WORKFLOW_RUNTIME_SURFACE_LABEL.to_string(),
            terracedb_workflows::WORKFLOW_RUNTIME_SURFACE_STATE_OUTBOX_TIMERS_V1.to_string(),
        )]),
    }
}

fn sandbox_store(now: u64, seed: u64) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(
        Arc::new(vfs.clone()),
        dependencies.clock,
        SandboxServices::deterministic(),
    );
    (vfs, sandbox)
}

async fn seed_sandbox_workflow_module(store: &InMemoryVfsStore, volume_id: VolumeId, path: &str) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open sandbox base volume");
    base.fs()
        .write_file(
            path,
            CONTRACT_PARITY_SANDBOX_MODULE.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write sandbox workflow module");
}

async fn open_sandbox_contract_handler(
    now: u64,
    seed: u64,
    base_volume_id: VolumeId,
    session_volume_id: VolumeId,
    module_path: &str,
    bundle: contracts::WorkflowBundleMetadata,
) -> ContractWorkflowHandler<
    terracedb_workflows::sandbox_contracts::SandboxWorkflowHandlerAdapter<
        SandboxModuleWorkflowTaskV1Handler,
    >,
> {
    let (vfs, sandbox) = sandbox_store(now, seed);
    seed_sandbox_workflow_module(&vfs, base_volume_id, module_path).await;
    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open sandbox session");
    ContractWorkflowHandler::new(
        bundle.clone(),
        terracedb_workflows::sandbox_contracts::SandboxWorkflowHandlerAdapter::new(
            SandboxModuleWorkflowTaskV1Handler::new(session, bundle).expect("sandbox handler"),
        ),
    )
    .expect("contract handler")
}

async fn drive_contract_runtime<H>(
    path: &str,
    clock: Arc<StubClock>,
    handler: H,
    workflow_name: &str,
) -> Result<WorkflowTablesSnapshot, WorkflowError>
where
    H: terracedb_workflows::WorkflowRuntimeHandler + 'static,
{
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(path, TieredDurabilityMode::GroupCommit),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("contract_source"))
        .await
        .expect("create source table");
    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(workflow_name, [source.clone()], handler),
    )
    .await?;
    let handle = runtime.start().await?;

    source
        .write(b"acct-7:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write source event");
    runtime
        .wait_for_state(
            "acct-7",
            Value::bytes(format!("contract:{workflow_name}:1")),
        )
        .await?;
    let snapshot = snapshot_workflow_tables(runtime.tables())
        .await
        .expect("snapshot workflow tables");
    handle.abort().await?;
    Ok(snapshot)
}

const CONTRACT_PARITY_SANDBOX_MODULE: &str = r#"
const bytes = (value) => Array.from(value).map((char) => char.charCodeAt(0));
const keyToInstance = (key) => String.fromCharCode(...key).split(":")[0];

function payloadFromTrigger(trigger) {
  switch (trigger.kind) {
    case "event":
      return trigger.event.key;
    case "timer":
      return trigger.payload;
    case "callback":
      return trigger.response;
    default:
      throw { code: "invalid-contract", message: `unknown trigger kind ${trigger.kind}` };
  }
}

export default {
  routeEventV1(request) {
    return {
      abi: request.abi,
      instance_id: keyToInstance(request.event.key),
    };
  },

  async handleTaskV1(request) {
    const sequence = request.input.history_len + 1;
    const id = `${request.input.instance_id}:${sequence}`;
    return {
      abi: request.abi,
      output: {
        state: {
          kind: "put",
          state: {
            encoding: "application/octet-stream",
            bytes: bytes(`contract:${request.input.workflow_name}:${sequence}`),
          },
        },
        lifecycle: null,
        visibility: null,
        continue_as_new: null,
        commands: [
          {
            kind: "outbox",
            entry: {
              outbox_id: bytes(id),
              idempotency_key: id,
              payload: payloadFromTrigger(request.input.trigger),
            },
          },
        ],
      },
    };
  },
};
"#;

#[tokio::test]
async fn workflow_runtime_surfaces_typed_change_feed_storage_errors() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-change-feed-timeout",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system.clone(), object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create workflow source");

    source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write workflow backlog");
    file_system.inject_failure(
        FileSystemFailure::timeout(
            FileSystemOperation::ReadAt,
            "/workflow-change-feed-timeout/commitlog/SEG-000001",
        )
        .persistent(),
    );

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "orders",
            [source],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");
    let mut handle = runtime.start().await.expect("start workflow runtime");

    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("workflow failure should be observed")
        .expect("workflow should terminate after the injected failpoint");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail on change-feed scan");
    match error {
        WorkflowError::Storage(storage)
        | WorkflowError::ChangeFeed(ChangeFeedError::Storage(storage)) => {
            assert_eq!(storage.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("expected typed workflow change-feed failure, got {other:?}"),
    }
}

#[tokio::test]
async fn callback_admission_failpoint_surfaces_storage_error_before_commit() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-callback-failpoint",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open workflow runtime");

    db_failpoint_registry(&db).arm_error(
        workflow_failpoint_names::WORKFLOW_CALLBACK_ADMISSION_BEFORE_COMMIT,
        StorageError::io("simulated workflow failpoint"),
        FailpointMode::Once,
    );

    let error = runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect_err("failpoint should fail callback admission");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Io);
            assert!(
                storage.to_string().contains("simulated workflow failpoint"),
                "expected injected workflow failpoint context, got {storage}"
            );
        }
        other => panic!("expected workflow storage error from failpoint, got {other:?}"),
    }

    let sequence = runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("retry callback after one-shot failpoint");
    assert_eq!(sequence, terracedb::SequenceNumber::new(1));

    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("workflow should process the retried callback")
    .expect("workflow state wait should not fail");

    handle.shutdown().await.expect("shutdown workflow runtime");
}

#[tokio::test]
async fn workflow_processing_fails_closed_when_active_run_metadata_is_missing() {
    for (case, expected_error) in [
        ("run", "workflow run record missing"),
        ("lifecycle", "workflow lifecycle record missing"),
        ("visibility", "workflow visibility record missing"),
        ("history", "workflow history event"),
    ] {
        let clock = Arc::new(StubClock::default());
        let file_system = Arc::new(StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_test_config_with_durability(
                &format!("/workflow-active-run-metadata-missing-{case}"),
                TieredDurabilityMode::GroupCommit,
            ),
            test_dependencies_with_clock(file_system, object_store, clock.clone()),
        )
        .await
        .expect("open db");

        let runtime = WorkflowRuntime::open(
            db,
            clock,
            WorkflowDefinition::new(
                "callbacks",
                std::iter::empty::<Table>(),
                CallbackReplayHandler,
            ),
        )
        .await
        .expect("open callback workflow runtime");

        runtime
            .admit_callback("order-1", "cb-1", b"approved".to_vec())
            .await
            .expect("admit initial callback");
        let handle = runtime.start().await.expect("start workflow runtime");
        tokio::time::timeout(
            Duration::from_secs(1),
            runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
        )
        .await
        .expect("initial callback should be processed")
        .expect("initial workflow state wait should not fail");
        handle
            .shutdown()
            .await
            .expect("shutdown healthy workflow runtime");

        let state = runtime
            .load_state_record("order-1")
            .await
            .expect("load active state record")
            .expect("active state record should exist");

        match case {
            "run" => {
                let (key, _) = scan_table_rows(runtime.tables().run_table())
                    .await
                    .expect("scan run table")
                    .into_iter()
                    .next()
                    .expect("run record should exist");
                runtime
                    .tables()
                    .run_table()
                    .delete(key)
                    .await
                    .expect("delete active run record");
            }
            "lifecycle" => {
                let (key, _) = scan_table_rows(runtime.tables().lifecycle_table())
                    .await
                    .expect("scan lifecycle table")
                    .into_iter()
                    .next()
                    .expect("lifecycle record should exist");
                runtime
                    .tables()
                    .lifecycle_table()
                    .delete(key)
                    .await
                    .expect("delete active lifecycle record");
            }
            "visibility" => {
                let (key, _) = scan_table_rows(runtime.tables().visibility_table())
                    .await
                    .expect("scan visibility table")
                    .into_iter()
                    .next()
                    .expect("visibility record should exist");
                runtime
                    .tables()
                    .visibility_table()
                    .delete(key)
                    .await
                    .expect("delete active visibility record");
            }
            "history" => {
                let (key, _) = scan_table_rows(runtime.tables().history_table())
                    .await
                    .expect("scan history table")
                    .into_iter()
                    .last()
                    .expect("history record should exist");
                runtime
                    .tables()
                    .history_table()
                    .delete(key)
                    .await
                    .expect("delete latest history record");
            }
            other => panic!("unexpected metadata case {other}"),
        }

        let mut handle = runtime.start().await.expect("restart workflow runtime");
        runtime
            .admit_callback("order-1", "cb-2", b"approved".to_vec())
            .await
            .expect("admit callback with missing metadata");
        tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
            .await
            .expect("runtime should fail closed on missing metadata")
            .expect("wait for workflow termination");

        let error = handle
            .shutdown()
            .await
            .expect_err("workflow runtime should fail closed on missing metadata");
        match error {
            WorkflowError::Storage(storage) => {
                assert_eq!(storage.kind(), StorageErrorKind::Corruption);
                assert!(
                    storage.to_string().contains(expected_error),
                    "expected corruption containing {expected_error}, got {storage}",
                );
                assert!(
                    storage.to_string().contains(state.run_id.as_str()),
                    "expected corruption to name the active run id, got {storage}",
                );
            }
            other => panic!("expected workflow corruption from missing metadata, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn workflow_state_load_and_processing_fail_closed_on_unknown_payload_encoding() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-unknown-payload-encoding",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    let state = runtime
        .load_state_record("order-1")
        .await
        .expect("load active state record")
        .expect("active state record should exist");
    let mutated = WorkflowStateRecord {
        state: Some(WorkflowPayload::with_encoding(
            "application/x.terracedb-workflow-state",
            b"corrupt".to_vec(),
        )),
        ..state
    };
    runtime
        .tables()
        .state_table()
        .write(
            mutated.instance_id.as_bytes().to_vec(),
            encode_versioned_workflow_contract(&mutated),
        )
        .await
        .expect("persist unsupported workflow payload encoding");

    let load_error = runtime
        .load_state("order-1")
        .await
        .expect_err("load_state should fail closed on unsupported payload encoding");
    match load_error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage.to_string().contains(
                    "workflow state encoding application/x.terracedb-workflow-state is unsupported"
                ),
                "expected unsupported-encoding corruption, got {storage}",
            );
        }
        other => panic!("expected workflow corruption from unsupported encoding, got {other:?}"),
    }

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback with unsupported payload encoding");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed on unsupported payload encoding")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed on unsupported payload encoding");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage.to_string().contains(
                    "workflow state encoding application/x.terracedb-workflow-state is unsupported"
                ),
                "expected unsupported-encoding corruption, got {storage}",
            );
        }
        other => panic!("expected workflow corruption from unsupported encoding, got {other:?}"),
    }
}

#[tokio::test]
async fn workflow_new_run_admission_fails_closed_when_terminal_run_metadata_is_missing() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-terminal-run-metadata-missing",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    let state = runtime
        .load_state_record("order-1")
        .await
        .expect("load active state record")
        .expect("active state record should exist");
    let lifecycle = runtime
        .load_lifecycle_record(&state.run_id)
        .await
        .expect("load lifecycle record")
        .expect("lifecycle record should exist");
    let visibility = runtime
        .load_visibility_record(&state.run_id)
        .await
        .expect("load visibility record")
        .expect("visibility record should exist");

    let terminal_updated_at = state.updated_at_millis.saturating_add(1);
    let terminal_state = WorkflowStateRecord {
        lifecycle: WorkflowLifecycleState::Completed,
        updated_at_millis: terminal_updated_at,
        ..state.clone()
    };
    let terminal_lifecycle = WorkflowLifecycleRecord {
        lifecycle: WorkflowLifecycleState::Completed,
        updated_at_millis: terminal_updated_at,
        ..lifecycle
    };
    let mut terminal_summary = visibility.summary.clone();
    terminal_summary.insert("lifecycle".to_string(), "completed".to_string());
    let terminal_visibility = WorkflowVisibilityRecord {
        lifecycle: WorkflowLifecycleState::Completed,
        updated_at_millis: terminal_updated_at,
        summary: terminal_summary,
        ..visibility
    };

    runtime
        .tables()
        .state_table()
        .write(
            terminal_state.instance_id.as_bytes().to_vec(),
            encode_versioned_workflow_contract(&terminal_state),
        )
        .await
        .expect("persist terminal workflow state");

    let (lifecycle_key, _) = scan_table_rows(runtime.tables().lifecycle_table())
        .await
        .expect("scan lifecycle table")
        .into_iter()
        .next()
        .expect("lifecycle row should exist");
    runtime
        .tables()
        .lifecycle_table()
        .write(
            lifecycle_key,
            encode_versioned_workflow_contract(&terminal_lifecycle),
        )
        .await
        .expect("persist terminal lifecycle record");

    let (visibility_key, _) = scan_table_rows(runtime.tables().visibility_table())
        .await
        .expect("scan visibility table")
        .into_iter()
        .next()
        .expect("visibility row should exist");
    runtime
        .tables()
        .visibility_table()
        .write(
            visibility_key,
            encode_versioned_workflow_contract(&terminal_visibility),
        )
        .await
        .expect("persist terminal visibility record");

    let (history_key, _) = scan_table_rows(runtime.tables().history_table())
        .await
        .expect("scan history table")
        .into_iter()
        .last()
        .expect("history row should exist");
    runtime
        .tables()
        .history_table()
        .write(
            history_key,
            encode_versioned_workflow_contract(&WorkflowHistoryEvent::VisibilityUpdated {
                record: terminal_visibility.clone(),
            }),
        )
        .await
        .expect("persist terminal history tail");

    let (run_key, _) = scan_table_rows(runtime.tables().run_table())
        .await
        .expect("scan run table")
        .into_iter()
        .next()
        .expect("run row should exist");
    runtime
        .tables()
        .run_table()
        .delete(run_key)
        .await
        .expect("delete terminal run record");

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback after terminal-state corruption");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed on missing terminal-run metadata")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed on missing terminal-run metadata");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage.to_string().contains("workflow run record missing"),
                "expected missing-run corruption, got {storage}",
            );
            assert!(
                storage.to_string().contains(state.run_id.as_str()),
                "expected corruption to name the terminal run id, got {storage}",
            );
        }
        other => {
            panic!("expected workflow corruption from missing terminal-run metadata, got {other:?}")
        }
    }
}

#[tokio::test]
async fn workflow_processing_fails_closed_when_state_row_is_missing_but_run_metadata_remains() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-missing-state-row",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    runtime
        .tables()
        .state_table()
        .delete(b"order-1".to_vec())
        .await
        .expect("delete workflow state row");

    assert!(
        runtime
            .load_state_record("order-1")
            .await
            .expect("load state after deletion")
            .is_none(),
        "state row should be absent after deletion",
    );

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback after deleting state row");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed when the state row is missing")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed when the state row is missing");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage
                    .to_string()
                    .contains("workflow state record is missing while run"),
                "expected missing-state corruption, got {storage}",
            );
            assert!(
                storage.to_string().contains("workflow instance order-1"),
                "expected corruption to name the instance, got {storage}",
            );
        }
        other => panic!("expected workflow corruption from missing state row, got {other:?}"),
    }
}

#[tokio::test]
async fn workflow_processing_fails_closed_on_gapped_extra_history_events() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-gapped-extra-history",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    let state = runtime
        .load_state_record("order-1")
        .await
        .expect("load active state record")
        .expect("active state record should exist");
    let visibility = runtime
        .load_visibility_record(&state.run_id)
        .await
        .expect("load visibility record")
        .expect("visibility record should exist");
    let (last_history_key, _) = scan_table_rows(runtime.tables().history_table())
        .await
        .expect("scan history table")
        .into_iter()
        .last()
        .expect("history row should exist");
    let mut extra_history_key = last_history_key;
    let extra_sequence = state.history_len.saturating_add(2);
    let key_len = extra_history_key.len();
    extra_history_key[key_len - 8..].copy_from_slice(&extra_sequence.to_be_bytes());
    runtime
        .tables()
        .history_table()
        .write(
            extra_history_key,
            encode_versioned_workflow_contract(&WorkflowHistoryEvent::VisibilityUpdated {
                record: visibility.clone(),
            }),
        )
        .await
        .expect("persist gapped extra history event");

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback with extra history event");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed on gapped extra history")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed on gapped extra history");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage
                    .to_string()
                    .contains(&format!("workflow history sequence {extra_sequence}")),
                "expected gapped-history corruption, got {storage}",
            );
        }
        other => panic!("expected workflow corruption from gapped extra history, got {other:?}"),
    }
}

#[tokio::test]
async fn workflow_processing_fails_closed_when_state_row_is_missing_but_history_and_lifecycle_remain()
 {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-missing-state-row-history-lifecycle",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    runtime
        .tables()
        .state_table()
        .delete(b"order-1".to_vec())
        .await
        .expect("delete workflow state row");
    let (run_key, _) = scan_table_rows(runtime.tables().run_table())
        .await
        .expect("scan run table")
        .into_iter()
        .next()
        .expect("run row should exist");
    runtime
        .tables()
        .run_table()
        .delete(run_key)
        .await
        .expect("delete run row");
    let (visibility_key, _) = scan_table_rows(runtime.tables().visibility_table())
        .await
        .expect("scan visibility table")
        .into_iter()
        .next()
        .expect("visibility row should exist");
    runtime
        .tables()
        .visibility_table()
        .delete(visibility_key)
        .await
        .expect("delete visibility row");

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback after deleting state, run, and visibility");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed when history and lifecycle remain")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed when history and lifecycle remain");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage
                    .to_string()
                    .contains("workflow state record is missing while history for run")
                    || storage
                        .to_string()
                        .contains("workflow state record is missing while lifecycle for run"),
                "expected missing-state history or lifecycle corruption, got {storage}",
            );
            assert!(
                storage.to_string().contains("workflow instance order-1"),
                "expected corruption to name the instance, got {storage}",
            );
        }
        other => panic!(
            "expected workflow corruption from missing state row with history remaining, got {other:?}"
        ),
    }
}

#[tokio::test]
async fn workflow_processing_fails_closed_when_state_row_is_missing_and_only_lifecycle_remains() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-missing-state-row-lifecycle-only",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    runtime
        .tables()
        .state_table()
        .delete(b"order-1".to_vec())
        .await
        .expect("delete workflow state row");
    let (run_key, _) = scan_table_rows(runtime.tables().run_table())
        .await
        .expect("scan run table")
        .into_iter()
        .next()
        .expect("run row should exist");
    runtime
        .tables()
        .run_table()
        .delete(run_key)
        .await
        .expect("delete run row");
    let (visibility_key, _) = scan_table_rows(runtime.tables().visibility_table())
        .await
        .expect("scan visibility table")
        .into_iter()
        .next()
        .expect("visibility row should exist");
    runtime
        .tables()
        .visibility_table()
        .delete(visibility_key)
        .await
        .expect("delete visibility row");
    for (key, _) in scan_table_rows(runtime.tables().history_table())
        .await
        .expect("scan history table")
    {
        runtime
            .tables()
            .history_table()
            .delete(key)
            .await
            .expect("delete history row");
    }

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback after deleting state, run, visibility, and history");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed when only lifecycle remains")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed when only lifecycle remains");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage
                    .to_string()
                    .contains("workflow state record is missing while lifecycle for run"),
                "expected missing-state lifecycle corruption, got {storage}",
            );
        }
        other => panic!(
            "expected workflow corruption from missing state row with only lifecycle remaining, got {other:?}"
        ),
    }
}

#[tokio::test]
async fn workflow_processing_fails_closed_when_state_row_is_missing_and_only_history_remains() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-missing-state-row-history-only",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    runtime
        .tables()
        .state_table()
        .delete(b"order-1".to_vec())
        .await
        .expect("delete workflow state row");
    let (run_key, _) = scan_table_rows(runtime.tables().run_table())
        .await
        .expect("scan run table")
        .into_iter()
        .next()
        .expect("run row should exist");
    runtime
        .tables()
        .run_table()
        .delete(run_key)
        .await
        .expect("delete run row");
    let (visibility_key, _) = scan_table_rows(runtime.tables().visibility_table())
        .await
        .expect("scan visibility table")
        .into_iter()
        .next()
        .expect("visibility row should exist");
    runtime
        .tables()
        .visibility_table()
        .delete(visibility_key)
        .await
        .expect("delete visibility row");
    let (lifecycle_key, _) = scan_table_rows(runtime.tables().lifecycle_table())
        .await
        .expect("scan lifecycle table")
        .into_iter()
        .next()
        .expect("lifecycle row should exist");
    runtime
        .tables()
        .lifecycle_table()
        .delete(lifecycle_key)
        .await
        .expect("delete lifecycle row");
    let history_rows = scan_table_rows(runtime.tables().history_table())
        .await
        .expect("scan history table");
    let (run_created_key, _) = history_rows
        .first()
        .cloned()
        .expect("run-created history row should exist");
    runtime
        .tables()
        .history_table()
        .delete(run_created_key)
        .await
        .expect("delete run-created history row");

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback after leaving history-only leftovers");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed when only history remains")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed when only history remains");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage
                    .to_string()
                    .contains("workflow state record is missing while history for run"),
                "expected missing-state history corruption, got {storage}",
            );
        }
        other => panic!(
            "expected workflow corruption from missing state row with only history remaining, got {other:?}"
        ),
    }
}

#[tokio::test]
async fn workflow_processing_fails_closed_on_custom_run_id_lifecycle_orphans_when_state_is_missing()
{
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-custom-run-id-lifecycle-orphan",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    clear_workflow_tables(runtime.tables())
        .await
        .expect("clear workflow-owned tables before injecting orphan lifecycle");

    let custom_run_id = WorkflowRunId::new("custom-run-id-lifecycle").expect("custom run id");
    let lifecycle = WorkflowLifecycleRecord {
        run_id: custom_run_id.clone(),
        lifecycle: WorkflowLifecycleState::Running,
        updated_at_millis: 1,
        reason: Some("custom-orphan".to_string()),
        task_id: None,
        attempt: 0,
    };
    runtime
        .tables()
        .lifecycle_table()
        .write(
            custom_run_id.as_str().as_bytes().to_vec(),
            encode_versioned_workflow_contract(&lifecycle),
        )
        .await
        .expect("persist orphan lifecycle record");

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback with orphan custom lifecycle");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed on orphan custom lifecycle")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed on orphan custom lifecycle");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage
                    .to_string()
                    .contains("workflow state record is missing while orphan lifecycle for run"),
                "expected orphan-lifecycle corruption, got {storage}",
            );
            assert!(
                storage.to_string().contains(custom_run_id.as_str()),
                "expected corruption to name the custom run id, got {storage}",
            );
        }
        other => panic!("expected workflow corruption from orphan custom lifecycle, got {other:?}"),
    }
}

#[tokio::test]
async fn workflow_processing_fails_closed_on_custom_run_id_history_orphans_when_state_is_missing() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-custom-run-id-history-orphan",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit initial callback");
    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(
        Duration::from_secs(1),
        runtime.wait_for_state("order-1", Value::bytes("processed:cb-1")),
    )
    .await
    .expect("initial callback should be processed")
    .expect("initial workflow state wait should not fail");
    handle
        .shutdown()
        .await
        .expect("shutdown healthy workflow runtime");

    clear_workflow_tables(runtime.tables())
        .await
        .expect("clear workflow-owned tables before injecting orphan history");

    let custom_run_id = WorkflowRunId::new("custom-run-id-history").expect("custom run id");
    let lifecycle = WorkflowLifecycleRecord {
        run_id: custom_run_id.clone(),
        lifecycle: WorkflowLifecycleState::Running,
        updated_at_millis: 1,
        reason: Some("custom-orphan".to_string()),
        task_id: None,
        attempt: 0,
    };
    let mut history_key = custom_run_id.as_str().as_bytes().to_vec();
    history_key.push(0);
    history_key.extend_from_slice(&1_u64.to_be_bytes());
    runtime
        .tables()
        .history_table()
        .write(
            history_key,
            encode_versioned_workflow_contract(&WorkflowHistoryEvent::LifecycleChanged {
                record: lifecycle,
            }),
        )
        .await
        .expect("persist orphan history record");

    let mut handle = runtime.start().await.expect("restart workflow runtime");
    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit callback with orphan custom history");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("runtime should fail closed on orphan custom history")
        .expect("wait for workflow termination");

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail closed on orphan custom history");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage
                    .to_string()
                    .contains("workflow state record is missing while orphan history for run"),
                "expected orphan-history corruption, got {storage}",
            );
            assert!(
                storage.to_string().contains(custom_run_id.as_str()),
                "expected corruption to name the custom run id, got {storage}",
            );
        }
        other => panic!("expected workflow corruption from orphan custom history, got {other:?}"),
    }
}

struct WorkflowStack<H> {
    runtime: WorkflowRuntime<H>,
    handle: Option<WorkflowHandle>,
    source: Option<Table>,
}

impl<H> WorkflowStack<H>
where
    H: WorkflowHandler + 'static,
{
    async fn start(&mut self) -> Result<(), WorkflowError> {
        if self.handle.is_none() {
            self.handle = Some(self.runtime.start().await?);
        }
        Ok(())
    }

    async fn shutdown(self) -> Result<(), WorkflowError> {
        if let Some(handle) = self.handle {
            handle.abort().await?;
        }
        Ok(())
    }
}

fn backlog_stack_builder(
    stats: ExecutionStats,
) -> SimulationStackBuilder<WorkflowStack<RecordingHandler>> {
    configured_backlog_stack_builder(stats, WorkflowSourceConfig::default())
}

fn configured_backlog_stack_builder(
    stats: ExecutionStats,
    source_config: WorkflowSourceConfig,
) -> SimulationStackBuilder<WorkflowStack<RecordingHandler>> {
    SimulationStackBuilder::new(
        move |context, db| {
            let stats = stats.clone();
            let source_config = source_config;
            async move {
                let source = db.create_table(row_table_config("workflow_source")).await?;
                let runtime = WorkflowRuntime::open(
                    db.clone(),
                    context.clock(),
                    WorkflowDefinition::new(
                        "orders",
                        [WorkflowSource::new(source.clone()).with_config(source_config)],
                        RecordingHandler { stats },
                    )
                    .with_progress_mode(WorkflowProgressMode::Buffered),
                )
                .await?;

                Ok(WorkflowStack {
                    runtime,
                    handle: None,
                    source: Some(source),
                })
            }
        },
        |stack| async move {
            stack.shutdown().await?;
            Ok(())
        },
    )
}

fn callback_stack_builder() -> SimulationStackBuilder<WorkflowStack<CallbackReplayHandler>> {
    SimulationStackBuilder::new(
        |context, db| async move {
            let runtime = WorkflowRuntime::open(
                db,
                context.clock(),
                WorkflowDefinition::new(
                    "callbacks",
                    std::iter::empty::<Table>(),
                    CallbackReplayHandler,
                )
                .with_timer_poll_interval(DEFAULT_TIMER_POLL_INTERVAL),
            )
            .await?;

            Ok(WorkflowStack {
                runtime,
                handle: None,
                source: None,
            })
        },
        |stack| async move {
            stack.shutdown().await?;
            Ok(())
        },
    )
}

fn timer_stack_builder(
    progress_mode: Option<WorkflowProgressMode>,
) -> SimulationStackBuilder<WorkflowStack<TimerHandler>> {
    SimulationStackBuilder::new(
        move |context, db| async move {
            let mut definition =
                WorkflowDefinition::new("timers", std::iter::empty::<Table>(), TimerHandler)
                    .with_timer_poll_interval(Duration::from_millis(1));
            if let Some(progress_mode) = progress_mode {
                definition = definition.with_progress_mode(progress_mode);
            }
            let runtime = WorkflowRuntime::open(db, context.clock(), definition).await?;

            Ok(WorkflowStack {
                runtime,
                handle: None,
                source: None,
            })
        },
        |stack| async move {
            stack.shutdown().await?;
            Ok(())
        },
    )
}

struct RecurringWorkflowStack<H> {
    runtime: RecurringWorkflowRuntime<H>,
    handle: Option<RecurringWorkflowHandle>,
}

impl<H> RecurringWorkflowStack<H>
where
    H: RecurringWorkflowHandler + 'static,
{
    async fn start(&mut self) -> Result<(), WorkflowError> {
        if self.handle.is_none() {
            self.handle = Some(self.runtime.start().await?);
        }
        Ok(())
    }

    async fn shutdown(self) -> Result<(), WorkflowError> {
        if let Some(handle) = self.handle {
            handle.abort().await?;
        }
        Ok(())
    }
}

fn recurring_stack_builder() -> SimulationStackBuilder<RecurringWorkflowStack<RecurringTimerHandler>>
{
    SimulationStackBuilder::new(
        |context, db| async move {
            let runtime = RecurringWorkflowRuntime::open(
                db,
                context.clock(),
                RecurringWorkflowDefinition::new(
                    "recurring",
                    "job-1",
                    RecurringSchedule::custom(
                        |now| Some(Timestamp::new(now.get().saturating_add(10))),
                        |_state, fire_at| Some(Timestamp::new(fire_at.get().saturating_add(10))),
                    ),
                    RecurringTimerHandler,
                )
                .with_timer_poll_interval(Duration::from_millis(1)),
            )
            .await?;

            Ok(RecurringWorkflowStack {
                runtime,
                handle: None,
            })
        },
        |stack| async move {
            stack.shutdown().await?;
            Ok(())
        },
    )
}

async fn checkpoint_workflow<H>(
    harness: &mut TerracedbSimulationHarness<WorkflowStack<H>>,
    label: impl Into<String>,
    instance_ids: Vec<&str>,
) -> Result<(), terracedb_simulation::SimulationHarnessError>
where
    H: WorkflowHandler + 'static,
{
    let instance_ids = instance_ids
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    harness
        .checkpoint_with(label, move |_db, stack| {
            Box::pin(async move {
                let mut metadata = BTreeMap::from([
                    (
                        "workflow.name".to_string(),
                        stack.runtime.name().to_string(),
                    ),
                    (
                        "workflow.running".to_string(),
                        stack.handle.is_some().to_string(),
                    ),
                ]);
                if let Some(source) = &stack.source {
                    metadata.insert(
                        "workflow.source_cursor".to_string(),
                        format!("{:?}", stack.runtime.load_source_cursor(source).await?),
                    );
                }
                for instance_id in &instance_ids {
                    metadata.insert(
                        format!("workflow.state.{instance_id}"),
                        format!("{:?}", stack.runtime.load_state(instance_id).await?),
                    );
                }
                Ok(metadata)
            })
        })
        .await
}

async fn wait_for_workflow_state<H>(
    harness: &TerracedbSimulationHarness<WorkflowStack<H>>,
    instance_id: &str,
    expected: &str,
) -> Result<(), terracedb_simulation::SimulationHarnessError>
where
    H: WorkflowHandler + 'static,
{
    let state_table = harness.stack().runtime.tables().state_table().clone();
    let inbox_table = harness.stack().runtime.tables().inbox_table().clone();
    let instance_id = instance_id.to_string();
    let expected = Value::bytes(expected);
    harness
        .wait_for_change(
            format!("workflow state {instance_id} -> {:?}", expected),
            [&state_table],
            [&inbox_table],
            move |_db, stack| {
                let instance_id = instance_id.clone();
                let expected = expected.clone();
                Box::pin(async move {
                    Ok(stack.runtime.load_state(&instance_id).await? == Some(expected))
                })
            },
        )
        .await
}

async fn wait_for_visible_inbox_row<H>(
    harness: &TerracedbSimulationHarness<WorkflowStack<H>>,
    inbox_key: Vec<u8>,
) -> Result<(), terracedb_simulation::SimulationHarnessError>
where
    H: WorkflowHandler + 'static,
{
    let inbox_table = harness.stack().runtime.tables().inbox_table().clone();
    harness
        .wait_for_visible(
            "workflow visible inbox row",
            [&inbox_table],
            move |_db, stack| {
                let inbox_key = inbox_key.clone();
                Box::pin(async move {
                    Ok(stack
                        .runtime
                        .tables()
                        .inbox_table()
                        .read(inbox_key)
                        .await?
                        .is_some())
                })
            },
        )
        .await
}

#[derive(Clone, Debug, PartialEq)]
struct WorkflowTablesSnapshot {
    rows: BTreeMap<String, Vec<(Vec<u8>, Value)>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkflowHistoricalCampaignCase {
    scenario: WorkflowHistoricalSourceScenario,
    has_timers: bool,
    has_callbacks: bool,
    has_pending_outbox: bool,
}

fn generated_historical_campaign(seed: u64) -> Vec<WorkflowHistoricalCampaignCase> {
    let rng = DeterministicRng::seeded(seed);
    let bootstraps = [
        WorkflowSourceBootstrapPolicy::Beginning,
        WorkflowSourceBootstrapPolicy::CurrentDurable,
    ];
    let recoveries = [
        WorkflowSourceRecoveryPolicy::FailClosed,
        WorkflowSourceRecoveryPolicy::RestoreCheckpoint,
        WorkflowSourceRecoveryPolicy::ReplayFromHistory,
        WorkflowSourceRecoveryPolicy::FastForwardToCurrentDurable,
    ];

    (0..8)
        .map(|index| {
            let (bootstrap, recovery, replay, checkpoint_support, event, checkpoint_available) =
                match index {
                    0 => (
                        WorkflowSourceBootstrapPolicy::Beginning,
                        WorkflowSourceRecoveryPolicy::FailClosed,
                        WorkflowReplayableSourceKind::NonReplayable,
                        WorkflowHistoricalArtifactSupport::Unsupported,
                        WorkflowHistoricalEvent::FirstAttach,
                        false,
                    ),
                    1 => (
                        WorkflowSourceBootstrapPolicy::CurrentDurable,
                        WorkflowSourceRecoveryPolicy::FailClosed,
                        WorkflowReplayableSourceKind::NonReplayable,
                        WorkflowHistoricalArtifactSupport::Unsupported,
                        WorkflowHistoricalEvent::FirstAttach,
                        false,
                    ),
                    2 => (
                        WorkflowSourceBootstrapPolicy::Beginning,
                        WorkflowSourceRecoveryPolicy::RestoreCheckpoint,
                        WorkflowReplayableSourceKind::NonReplayable,
                        WorkflowHistoricalArtifactSupport::Optional,
                        WorkflowHistoricalEvent::SnapshotTooOld,
                        true,
                    ),
                    3 => (
                        WorkflowSourceBootstrapPolicy::Beginning,
                        WorkflowSourceRecoveryPolicy::ReplayFromHistory,
                        WorkflowReplayableSourceKind::AppendOnlyOrdered,
                        WorkflowHistoricalArtifactSupport::Unsupported,
                        WorkflowHistoricalEvent::SnapshotTooOld,
                        false,
                    ),
                    4 => (
                        WorkflowSourceBootstrapPolicy::Beginning,
                        WorkflowSourceRecoveryPolicy::FastForwardToCurrentDurable,
                        WorkflowReplayableSourceKind::NonReplayable,
                        WorkflowHistoricalArtifactSupport::Unsupported,
                        WorkflowHistoricalEvent::SnapshotTooOld,
                        false,
                    ),
                    _ => {
                        let bootstrap = bootstraps[(rng.next_u64() as usize) % bootstraps.len()];
                        let recovery = recoveries[(rng.next_u64() as usize) % recoveries.len()];
                        let replay =
                            if matches!(recovery, WorkflowSourceRecoveryPolicy::ReplayFromHistory)
                                && rng.next_u64().is_multiple_of(2)
                            {
                                WorkflowReplayableSourceKind::AppendOnlyOrdered
                            } else {
                                WorkflowReplayableSourceKind::NonReplayable
                            };
                        let checkpoint_support =
                            if matches!(recovery, WorkflowSourceRecoveryPolicy::RestoreCheckpoint)
                                || rng.next_u64().is_multiple_of(3)
                            {
                                WorkflowHistoricalArtifactSupport::Optional
                            } else {
                                WorkflowHistoricalArtifactSupport::Unsupported
                            };
                        let event = if rng.next_u64().is_multiple_of(2) {
                            WorkflowHistoricalEvent::FirstAttach
                        } else {
                            WorkflowHistoricalEvent::SnapshotTooOld
                        };
                        let checkpoint_available =
                            matches!(recovery, WorkflowSourceRecoveryPolicy::RestoreCheckpoint)
                                || rng.next_u64().is_multiple_of(2);
                        (
                            bootstrap,
                            recovery,
                            replay,
                            checkpoint_support,
                            event,
                            checkpoint_available,
                        )
                    }
                };
            let scenario = WorkflowHistoricalSourceScenario::new(
                format!("campaign_source_{index}"),
                WorkflowSourceConfig::default()
                    .with_bootstrap_policy(bootstrap)
                    .with_recovery_policy(recovery)
                    .with_replay_kind(replay)
                    .with_checkpoint_support(checkpoint_support)
                    .with_trigger_journal_support(WorkflowHistoricalArtifactSupport::Optional),
                event,
                SequenceNumber::new(1 + (rng.next_u64() % 32)),
            )
            .with_checkpoint_available(checkpoint_available);

            WorkflowHistoricalCampaignCase {
                scenario,
                has_timers: index == 0 || rng.next_u64().is_multiple_of(2),
                has_callbacks: index == 1 || rng.next_u64().is_multiple_of(2),
                has_pending_outbox: index == 2 || rng.next_u64().is_multiple_of(2),
            }
        })
        .collect()
}

async fn wait_for_runtime_state<H>(
    runtime: &WorkflowRuntime<H>,
    instance_id: &str,
    expected: &str,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    runtime
        .wait_for_state(instance_id, Value::bytes(expected))
        .await
}

async fn scan_table_rows(table: &Table) -> Result<Vec<(Vec<u8>, Value)>, terracedb::ReadError> {
    let mut rows = table
        .scan(Vec::new(), vec![0xff], ScanOptions::default())
        .await?;
    let mut captured = Vec::new();
    while let Some((key, value)) = rows.next().await {
        captured.push((key, value));
    }
    Ok(captured)
}

fn encode_versioned_workflow_contract<T: serde::Serialize>(value: &T) -> Value {
    let mut bytes = vec![1];
    bytes.extend(
        serde_json::to_vec(value)
            .expect("workflow contract value should serialize deterministically"),
    );
    Value::bytes(bytes)
}

async fn write_mutated_state_record(
    table: &Table,
    record: &WorkflowStateRecord,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mutated = WorkflowStateRecord {
        state: Some(WorkflowPayload::bytes("mutated")),
        updated_at_millis: record.updated_at_millis.saturating_add(1),
        ..record.clone()
    };
    table
        .write(
            mutated.instance_id.as_bytes().to_vec(),
            encode_versioned_workflow_contract(&mutated),
        )
        .await?;
    Ok(())
}

async fn snapshot_workflow_tables(
    tables: &WorkflowTables,
) -> Result<WorkflowTablesSnapshot, terracedb::ReadError> {
    let mut rows = BTreeMap::new();
    rows.insert(
        tables.run_table().name().to_string(),
        scan_table_rows(tables.run_table()).await?,
    );
    rows.insert(
        tables.state_table().name().to_string(),
        scan_table_rows(tables.state_table()).await?,
    );
    rows.insert(
        tables.history_table().name().to_string(),
        scan_table_rows(tables.history_table()).await?,
    );
    rows.insert(
        tables.lifecycle_table().name().to_string(),
        scan_table_rows(tables.lifecycle_table()).await?,
    );
    rows.insert(
        tables.visibility_table().name().to_string(),
        scan_table_rows(tables.visibility_table()).await?,
    );
    rows.insert(
        tables.inbox_table().name().to_string(),
        scan_table_rows(tables.inbox_table()).await?,
    );
    rows.insert(
        tables.trigger_order_table().name().to_string(),
        scan_table_rows(tables.trigger_order_table()).await?,
    );
    rows.insert(
        tables.source_progress_table().name().to_string(),
        scan_table_rows(tables.source_progress_table()).await?,
    );
    rows.insert(
        tables.timer_schedule_table().name().to_string(),
        scan_table_rows(tables.timer_schedule_table()).await?,
    );
    rows.insert(
        tables.timer_lookup_table().name().to_string(),
        scan_table_rows(tables.timer_lookup_table()).await?,
    );
    rows.insert(
        tables.outbox_table().name().to_string(),
        scan_table_rows(tables.outbox_table()).await?,
    );
    rows.insert(
        tables.trigger_journal_table().name().to_string(),
        scan_table_rows(tables.trigger_journal_table()).await?,
    );
    Ok(WorkflowTablesSnapshot { rows })
}

async fn clear_table(table: &Table) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rows = scan_table_rows(table).await?;
    for (key, _) in rows {
        table.delete(key).await?;
    }
    Ok(())
}

async fn clear_workflow_tables(
    tables: &WorkflowTables,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    clear_table(tables.run_table()).await?;
    clear_table(tables.state_table()).await?;
    clear_table(tables.history_table()).await?;
    clear_table(tables.lifecycle_table()).await?;
    clear_table(tables.visibility_table()).await?;
    clear_table(tables.inbox_table()).await?;
    clear_table(tables.trigger_order_table()).await?;
    clear_table(tables.source_progress_table()).await?;
    clear_table(tables.timer_schedule_table()).await?;
    clear_table(tables.timer_lookup_table()).await?;
    clear_table(tables.outbox_table()).await?;
    clear_table(tables.trigger_journal_table()).await?;
    Ok(())
}

async fn open_checkpointed_runtime(
    db: Db,
    source: Table,
    clock: Arc<dyn Clock>,
    checkpoint_store: Arc<WorkflowObjectStoreCheckpointStore>,
    restore_on_open: bool,
) -> Result<WorkflowRuntime<CheckpointCaptureHandler>, WorkflowError> {
    let mut definition =
        WorkflowDefinition::new("checkpointed", [source], CheckpointCaptureHandler)
            .with_checkpoint_store(checkpoint_store)
            .with_timer_poll_interval(Duration::from_millis(1));
    if restore_on_open {
        definition = definition.with_restore_latest_checkpoint_on_open();
    }
    WorkflowRuntime::open(db, clock, definition).await
}

async fn open_historical_checkpoint_runtime(
    db: Db,
    source: Table,
    clock: Arc<dyn Clock>,
    checkpoint_store: Arc<WorkflowObjectStoreCheckpointStore>,
    source_config: WorkflowSourceConfig,
) -> Result<WorkflowRuntime<CheckpointCaptureHandler>, WorkflowError> {
    WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "checkpointed",
            [WorkflowSource::new(source).with_config(source_config)],
            CheckpointCaptureHandler,
        )
        .with_checkpoint_store(checkpoint_store)
        .with_timer_poll_interval(Duration::from_millis(1)),
    )
    .await
}

async fn wait_for_recurring_state<H, P>(
    runtime: &RecurringWorkflowRuntime<H>,
    predicate: P,
) -> Result<RecurringWorkflowState, WorkflowError>
where
    H: RecurringWorkflowHandler + 'static,
    P: Fn(&RecurringWorkflowState) -> bool,
{
    runtime.wait_for_state(predicate).await
}

async fn wait_for_simulation_recurring_tick_count<H>(
    harness: &TerracedbSimulationHarness<RecurringWorkflowStack<H>>,
    expected: u64,
) -> Result<(), terracedb_simulation::SimulationHarnessError>
where
    H: RecurringWorkflowHandler + 'static,
{
    let state_table = harness.stack().runtime.tables().state_table().clone();
    harness
        .wait_for_visible(
            format!("recurring tick count -> {expected}"),
            [&state_table],
            move |_db, stack| {
                Box::pin(async move {
                    Ok(stack
                        .runtime
                        .load_state()
                        .await?
                        .map(|state| state.tick_count == expected)
                        .unwrap_or(false))
                })
            },
        )
        .await
}

#[test]
fn workflow_source_config_round_trips_and_defaults_fail_closed() {
    let config = WorkflowSourceConfig::default()
        .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CheckpointOrCurrentDurable)
        .with_recovery_policy(WorkflowSourceRecoveryPolicy::RestoreCheckpointOrFastForward)
        .with_replay_kind(WorkflowReplayableSourceKind::AppendOnlyOrdered)
        .with_checkpoint_support(WorkflowHistoricalArtifactSupport::Optional)
        .with_trigger_journal_support(WorkflowHistoricalArtifactSupport::Required);

    let encoded = serde_json::to_string(&config).expect("encode workflow source config");
    let decoded: WorkflowSourceConfig =
        serde_json::from_str(&encoded).expect("decode workflow source config");

    assert_eq!(decoded, config);
    assert_eq!(
        WorkflowSourceConfig::default().recovery,
        WorkflowSourceRecoveryPolicy::FailClosed,
        "workflow sources should fail closed unless a weaker recovery mode is selected",
    );
}

#[test]
fn workflow_source_progress_round_trips_and_preserves_ordering_semantics() {
    let earlier = WorkflowSourceProgress::from_cursor(LogCursor::new(SequenceNumber::new(7), 3));
    let durable_fence = WorkflowSourceProgress::from_durable_sequence(SequenceNumber::new(7))
        .with_origin(WorkflowSourceProgressOrigin::CurrentDurableBootstrap);
    let later = WorkflowSourceProgress::from_cursor(LogCursor::new(SequenceNumber::new(8), 0))
        .with_origin(WorkflowSourceProgressOrigin::ReplayFromHistory);

    for progress in [earlier, durable_fence, later] {
        let encoded = progress.encode().expect("encode source progress");
        let decoded = WorkflowSourceProgress::decode(&encoded).expect("decode source progress");
        assert_eq!(decoded, progress);
    }

    let mut ordered = vec![later, durable_fence, earlier];
    ordered.sort();
    assert_eq!(ordered, vec![earlier, durable_fence, later]);
}

#[tokio::test]
async fn workflow_source_progress_legacy_cursor_encoding_fails_closed() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-source-progress-legacy",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create workflow source");
    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(source.clone())],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");

    let mut legacy_progress = vec![1];
    legacy_progress.extend_from_slice(&LogCursor::new(SequenceNumber::new(7), 0).encode());
    runtime
        .tables()
        .source_progress_table()
        .write(
            source.name().as_bytes().to_vec(),
            Value::bytes(legacy_progress),
        )
        .await
        .expect("persist legacy source progress");

    match runtime.load_source_progress(&source).await {
        Err(WorkflowError::Storage(storage)) => {
            assert_eq!(storage.kind(), StorageErrorKind::Corruption);
            assert!(
                storage
                    .to_string()
                    .contains("workflow source progress encoding is invalid")
            );
        }
        Err(other) => panic!("expected source progress corruption, got {other:?}"),
        Ok(progress) => panic!("expected legacy source progress to fail closed, got {progress:?}"),
    }
}

#[test]
fn workflow_bootstrap_contract_smoke_cases_are_deterministic() -> turmoil::Result {
    SeededSimulationRunner::new(0x84_01)
        .with_simulation_duration(Duration::from_millis(150))
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-bootstrap-contracts",
                    TieredDurabilityMode::GroupCommit,
                ),
                SimulationStackBuilder::db_only(),
            )
            .await?;
            let source = harness
                .db()
                .create_table(row_table_config("workflow_source"))
                .await?;

            let empty_resolution =
                WorkflowSourceConfig::default().initial_resolution(false, SequenceNumber::new(0));
            harness.require_eq(
                "empty-state bootstrap",
                &empty_resolution,
                &WorkflowHistoricalSourceResolution::AttachFromBeginning,
            )?;

            source
                .write(b"order-1".to_vec(), Value::bytes("created"))
                .await?;
            source
                .write(b"order-2".to_vec(), Value::bytes("confirmed"))
                .await?;
            let durable_sequence = harness.db().current_durable_sequence();

            let beginning_resolution =
                WorkflowSourceConfig::default().initial_resolution(false, durable_sequence);
            harness.require_eq(
                "beginning bootstrap with backlog",
                &beginning_resolution,
                &WorkflowHistoricalSourceResolution::AttachFromBeginning,
            )?;

            let current_durable_resolution = WorkflowSourceConfig::default()
                .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CurrentDurable)
                .initial_resolution(false, durable_sequence);
            harness.require_eq(
                "current-durable bootstrap",
                &current_durable_resolution,
                &WorkflowHistoricalSourceResolution::AttachFromCurrentDurable { durable_sequence },
            )?;

            harness
                .checkpoint_with("bootstrap-contracts", move |_db, _stack| {
                    let durable_sequence = durable_sequence.get();
                    Box::pin(async move {
                        Ok(BTreeMap::from([(
                            "workflow.bootstrap.decisions".to_string(),
                            format!(
                                "beginning={:?};current_durable={durable_sequence}",
                                current_durable_resolution
                            ),
                        )]))
                    })
                })
                .await?;

            Ok(())
        })
}

#[test]
fn workflow_historical_scenarios_round_trip_through_simulation_checkpoints() -> turmoil::Result {
    SeededSimulationRunner::new(0x84_02)
        .with_simulation_duration(Duration::from_millis(150))
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-historical-oracle",
                    TieredDurabilityMode::GroupCommit,
                ),
                SimulationStackBuilder::db_only(),
            )
            .await?;

            let scenarios = vec![
                WorkflowHistoricalSourceScenario::new(
                    "orders_cdc",
                    WorkflowSourceConfig::default()
                        .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::Beginning)
                        .with_recovery_policy(WorkflowSourceRecoveryPolicy::ReplayFromHistory)
                        .with_replay_kind(WorkflowReplayableSourceKind::AppendOnlyOrdered),
                    WorkflowHistoricalEvent::FirstAttach,
                    SequenceNumber::new(5),
                ),
                WorkflowHistoricalSourceScenario::new(
                    "orders_cdc",
                    WorkflowSourceConfig::default()
                        .with_recovery_policy(
                            WorkflowSourceRecoveryPolicy::RestoreCheckpointOrFastForward,
                        )
                        .with_checkpoint_support(WorkflowHistoricalArtifactSupport::Optional)
                        .with_trigger_journal_support(WorkflowHistoricalArtifactSupport::Optional),
                    WorkflowHistoricalEvent::SnapshotTooOld,
                    SequenceNumber::new(12),
                ),
            ]
            .into_iter()
            .map(|scenario| scenario.with_checkpoint_available(true))
            .collect::<Vec<_>>();

            let encoded = serde_json::to_string(&scenarios).expect("encode historical scenarios");
            harness
                .checkpoint_with("workflow-historical-scenarios", move |_db, _stack| {
                    let encoded = encoded.clone();
                    Box::pin(async move {
                        Ok(BTreeMap::from([(
                            "workflow.historical.scenarios".to_string(),
                            encoded,
                        )]))
                    })
                })
                .await?;

            let stored = harness
                .checkpoints()
                .iter()
                .find(|checkpoint| checkpoint.label == "workflow-historical-scenarios")
                .and_then(|checkpoint| {
                    checkpoint
                        .metadata
                        .get("workflow.historical.scenarios")
                        .cloned()
                })
                .expect("checkpoint should retain historical scenarios");
            let decoded: Vec<WorkflowHistoricalSourceScenario> =
                serde_json::from_str(&stored).expect("decode stored historical scenarios");
            harness.require_eq("historical scenario roundtrip", &decoded, &scenarios)?;

            let resolutions = decoded
                .iter()
                .map(WorkflowHistoricalSourceScenario::resolve)
                .collect::<Vec<_>>();
            harness.require_eq(
                "historical scenario resolutions",
                &resolutions,
                &vec![
                    WorkflowHistoricalSourceResolution::AttachFromBeginning,
                    WorkflowHistoricalSourceResolution::RestoreCheckpoint,
                ],
            )?;

            Ok(())
        })
}

#[test]
fn workflow_historical_campaign_generator_is_reproducible_and_varies_historical_choices() {
    let harness = GeneratedSeedHarness {
        generate: generated_historical_campaign,
    };
    let first = assert_seed_replays(&harness, 0x8701)
        .expect("historical generator replay should not fail")
        .outcome;
    let _ = assert_seed_variation(&harness, 0x8701, 0x8702, |left, right| {
        left.outcome != right.outcome
    })
    .expect("historical generator variance should not fail");

    assert!(first.iter().any(|case| case.has_timers));
    assert!(first.iter().any(|case| case.has_callbacks));
    assert!(first.iter().any(|case| case.has_pending_outbox));
    assert!(first.iter().any(|case| {
        matches!(
            case.scenario.resolve(),
            WorkflowHistoricalSourceResolution::AttachFromBeginning
        )
    }));
    assert!(first.iter().any(|case| {
        matches!(
            case.scenario.resolve(),
            WorkflowHistoricalSourceResolution::AttachFromCurrentDurable { .. }
        )
    }));
    assert!(first.iter().any(|case| {
        matches!(
            case.scenario.resolve(),
            WorkflowHistoricalSourceResolution::RestoreCheckpoint
        )
    }));
    assert!(first.iter().any(|case| {
        matches!(
            case.scenario.resolve(),
            WorkflowHistoricalSourceResolution::ReplayFromHistory
        )
    }));
    assert!(first.iter().any(|case| {
        matches!(
            case.scenario.resolve(),
            WorkflowHistoricalSourceResolution::FastForwardToCurrentDurable { .. }
        )
    }));
    assert!(first.iter().any(|case| {
        case.scenario.resolve().attach_mode() == Some(WorkflowSourceAttachMode::LiveOnly)
    }));
    assert!(first.iter().any(|case| {
        case.scenario.resolve().attach_mode() == Some(WorkflowSourceAttachMode::Historical)
    }));
}

#[test]
fn workflow_replays_startup_backlog_round_robin_and_outbox_order() -> turmoil::Result {
    SeededSimulationRunner::new(0x4101)
        .with_simulation_duration(WORKFLOW_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let stats = ExecutionStats::default();
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-backlog-round-robin",
                    TieredDurabilityMode::GroupCommit,
                ),
                backlog_stack_builder(stats.clone()),
            )
            .await?;
            let source = harness
                .stack()
                .source
                .clone()
                .expect("backlog workflow source should exist");

            let mut batch = harness.db().write_batch();
            batch.put(&source, b"alpha:1".to_vec(), Value::bytes("a1"));
            batch.put(&source, b"alpha:2".to_vec(), Value::bytes("a2"));
            batch.put(&source, b"beta:1".to_vec(), Value::bytes("b1"));
            let sequence = harness.db().commit(batch, Default::default()).await?;

            harness.stack_mut().start().await?;
            checkpoint_workflow(&mut harness, "workflow-started", vec!["alpha", "beta"]).await?;
            wait_for_workflow_state(&harness, "alpha", "2").await?;
            wait_for_workflow_state(&harness, "beta", "1").await?;

            checkpoint_workflow(
                &mut harness,
                "workflow-backlog-drained",
                vec!["alpha", "beta"],
            )
            .await?;

            let order = stats.order.lock().expect("order lock poisoned").clone();
            harness.require_eq(
                "workflow execution order",
                &order,
                &vec!["alpha".to_string(), "beta".to_string(), "alpha".to_string()],
            )?;
            let runtime = &harness.stack().runtime;
            harness.require_eq(
                "alpha workflow state",
                &runtime.load_state("alpha").await?,
                &Some(Value::bytes("2")),
            )?;
            harness.require_eq(
                "beta workflow state",
                &runtime.load_state("beta").await?,
                &Some(Value::bytes("1")),
            )?;
            harness.require_eq(
                "workflow source cursor",
                &runtime.load_source_cursor(&source).await?,
                &LogCursor::new(sequence, 2),
            )?;

            let (alpha_max_active, beta_max_active) = {
                let max_active = stats.max_active.lock().expect("max-active lock poisoned");
                (
                    max_active.get("alpha").copied(),
                    max_active.get("beta").copied(),
                )
            };
            harness.require_eq("alpha max concurrency", &alpha_max_active, &Some(1))?;
            harness.require_eq("beta max concurrency", &beta_max_active, &Some(1))?;

            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"alpha:1".to_vec())
                    .await?
                    .is_some(),
                "alpha first outbox entry should be present",
            )?;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"alpha:2".to_vec())
                    .await?
                    .is_some(),
                "alpha second outbox entry should be present",
            )?;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"beta:1".to_vec())
                    .await?
                    .is_some(),
                "beta outbox entry should be present",
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[test]
fn workflow_current_durable_bootstrap_skips_history_and_processes_new_events() -> turmoil::Result {
    SeededSimulationRunner::new(0x4106)
        .with_simulation_duration(WORKFLOW_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let stats = ExecutionStats::default();
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-current-durable-bootstrap",
                    TieredDurabilityMode::GroupCommit,
                ),
                configured_backlog_stack_builder(
                    stats,
                    WorkflowSourceConfig::default()
                        .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CurrentDurable),
                ),
            )
            .await?;
            let source = harness
                .stack()
                .source
                .clone()
                .expect("current-durable workflow source should exist");

            source
                .write(b"skipped-1:created".to_vec(), Value::bytes("created"))
                .await?;
            let durable_before_start = harness.db().current_durable_sequence();

            harness.stack_mut().start().await?;

            let source_progress_table = harness
                .stack()
                .runtime
                .tables()
                .source_progress_table()
                .clone();
            let source_for_wait = source.clone();
            harness
                .wait_for_change(
                    "workflow current-durable bootstrap progress",
                    [&source_progress_table],
                    [&source_progress_table],
                    move |_db, stack| {
                        let source = source_for_wait.clone();
                        Box::pin(async move {
                            let progress = stack.runtime.load_source_progress(&source).await?;
                            Ok(progress.origin()
                                == WorkflowSourceProgressOrigin::CurrentDurableBootstrap
                                && progress.resume_point()
                                    == WorkflowSourceResumePoint::DurableSequenceFence {
                                        sequence: durable_before_start,
                                    })
                        })
                    },
                )
                .await?;

            harness.require_eq(
                "skipped backlog state",
                &harness.stack().runtime.load_state("skipped-1").await?,
                &None,
            )?;

            source
                .write(b"live-1:created".to_vec(), Value::bytes("created"))
                .await?;
            wait_for_workflow_state(&harness, "live-1", "1").await?;

            harness.require_eq(
                "live-only backlog skip",
                &harness.stack().runtime.load_state("skipped-1").await?,
                &None,
            )?;

            let telemetry = harness.stack().runtime.telemetry_snapshot().await?;
            harness.require_eq(
                "telemetry progress origin",
                &telemetry.source_lags[0].progress_origin,
                &WorkflowSourceProgressOrigin::CurrentDurableBootstrap,
            )?;
            harness.require_eq(
                "telemetry attach mode",
                &telemetry.source_lags[0].attach_mode,
                &Some(WorkflowSourceAttachMode::LiveOnly),
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[tokio::test]
async fn workflow_fail_closed_recovery_surfaces_snapshot_too_old_without_fast_forwarding() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-fail-closed-recovery",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let mut source_config = row_table_config("workflow_source");
    source_config.history_retention_sequences = Some(1);
    let source = db
        .create_table(source_config)
        .await
        .expect("create source table");

    let first = source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write first retained event");
    let second = source
        .write(b"order-2:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write second retained event");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(source.clone())
                .with_recovery_policy(WorkflowSourceRecoveryPolicy::FailClosed)],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");

    let stale_progress = WorkflowSourceProgress::from_cursor(LogCursor::new(first, 0));
    runtime
        .tables()
        .source_progress_table()
        .write(
            source.name().as_bytes().to_vec(),
            Value::bytes(
                stale_progress
                    .encode()
                    .expect("encode stale source progress"),
            ),
        )
        .await
        .expect("persist stale workflow source progress");

    let mut handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("workflow failure should be observed")
        .expect("workflow should terminate after the fail-closed recovery");
    let error = tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("workflow shutdown should return promptly")
        .expect_err("workflow runtime should fail closed on stale source progress");

    let snapshot_too_old = error
        .snapshot_too_old()
        .expect("workflow error should preserve SnapshotTooOld");
    assert_eq!(snapshot_too_old.requested, first);
    assert!(
        snapshot_too_old.oldest_available >= second,
        "history loss should advance the oldest available source sequence past the stale cursor",
    );
    assert_eq!(
        runtime
            .load_source_progress(&source)
            .await
            .expect("load source progress after fail-closed recovery"),
        stale_progress,
        "fail-closed recovery must not silently fast-forward the source progress",
    );
}

#[tokio::test]
async fn workflow_current_durable_bootstrap_processes_multiple_live_events_for_one_instance() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-current-durable-multi-event",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create workflow source");

    source
        .write(b"skipped-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write skipped backlog");
    let durable_before_start = db.current_durable_sequence();

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(source.clone()).with_config(
                WorkflowSourceConfig::default()
                    .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CurrentDurable),
            )],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");
    let handle = runtime.start().await.expect("start workflow runtime");

    runtime
        .wait_for_source_progress(
            &source,
            WorkflowSourceProgress::from_durable_sequence(durable_before_start)
                .with_origin(WorkflowSourceProgressOrigin::CurrentDurableBootstrap),
        )
        .await
        .expect("bootstrap source progress should publish the current-durable fence");
    runtime
        .wait_for_state_where("skipped-1", |state| state.is_none())
        .await
        .expect("live-only bootstrap should treat skipped backlog state as absent");

    source
        .write(b"live-1:entered".to_vec(), Value::bytes("entered"))
        .await
        .expect("write first live event");
    source
        .write(b"live-1:exited".to_vec(), Value::bytes("exited"))
        .await
        .expect("write second live event");

    runtime
        .wait_for_state("live-1", Value::bytes("2"))
        .await
        .expect("live workflow state should publish the final value");

    assert_eq!(
        runtime
            .load_state("skipped-1")
            .await
            .expect("load skipped backlog state"),
        None,
    );
    assert_eq!(
        runtime
            .load_state("live-1")
            .await
            .expect("load final live workflow state")
            .map(|state| decode_count(Some(state))),
        Some(2),
    );

    handle.shutdown().await.expect("shutdown workflow runtime");
}

#[tokio::test]
async fn workflow_current_durable_bootstrap_fails_closed_without_retained_history_for_batched_events()
 {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-current-durable-batched-events-fail-closed",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create workflow source");

    source
        .write(b"live-1:event".to_vec(), Value::bytes("backlog"))
        .await
        .expect("write skipped backlog");
    let durable_before_start = db.current_durable_sequence();

    let runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(source.clone()).with_config(
                WorkflowSourceConfig::default()
                    .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CurrentDurable),
            )],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");
    let mut handle = runtime.start().await.expect("start workflow runtime");

    runtime
        .wait_for_source_progress(
            &source,
            WorkflowSourceProgress::from_durable_sequence(durable_before_start)
                .with_origin(WorkflowSourceProgressOrigin::CurrentDurableBootstrap),
        )
        .await
        .expect("bootstrap source progress should publish the current-durable fence");

    let mut tx = Transaction::begin(&db).await;
    tx.write(&source, b"live-1:entered".to_vec(), Value::bytes("entered"));
    tx.write(&source, b"live-1:exited".to_vec(), Value::bytes("exited"));
    tx.commit().await.expect("commit live event batch");

    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("workflow failure should be observed")
        .expect("workflow should terminate after the durable fence falls behind");

    let shutdown_error = handle
        .shutdown()
        .await
        .expect_err("runtime should fail closed once the durable fence falls behind");
    let snapshot_too_old = shutdown_error
        .snapshot_too_old()
        .expect("fail-closed runtime should surface snapshot-too-old details");
    assert_eq!(snapshot_too_old.requested, durable_before_start);
    assert!(
        snapshot_too_old.oldest_available > durable_before_start,
        "expected the source history floor to move past the current-durable fence",
    );
}

#[tokio::test]
async fn workflow_current_durable_bootstrap_processes_multiple_live_events_in_one_commit_when_source_retains_history()
 {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-current-durable-batched-events-retained",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let source_config = WorkflowSourceConfig::live_only_replayable_append_only_source()
        .prepare_source_table_config(row_table_config("workflow_source"));
    let source = db
        .create_table(source_config)
        .await
        .expect("create workflow source");

    source
        .write(b"skipped-1:backlog".to_vec(), Value::bytes("backlog"))
        .await
        .expect("write skipped backlog");
    let durable_before_start = db.current_durable_sequence();

    let runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(source.clone()).with_config(
                WorkflowSourceConfig::default()
                    .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CurrentDurable),
            )],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");
    let handle = runtime.start().await.expect("start workflow runtime");

    runtime
        .wait_for_source_progress(
            &source,
            WorkflowSourceProgress::from_durable_sequence(durable_before_start)
                .with_origin(WorkflowSourceProgressOrigin::CurrentDurableBootstrap),
        )
        .await
        .expect("bootstrap source progress should publish the current-durable fence");

    let mut tx = Transaction::begin(&db).await;
    tx.write(&source, b"live-1:entered".to_vec(), Value::bytes("entered"));
    tx.write(&source, b"live-1:exited".to_vec(), Value::bytes("exited"));
    tx.commit().await.expect("commit live event batch");

    runtime
        .wait_for_state("live-1", Value::bytes("2"))
        .await
        .expect("live workflow state should publish the final value");

    assert_eq!(
        runtime
            .load_state("live-1")
            .await
            .expect("load final batched live workflow state")
            .map(|state| decode_count(Some(state))),
        Some(2),
    );

    handle.shutdown().await.expect("shutdown workflow runtime");
}

#[tokio::test]
async fn workflow_fast_forward_recovery_skips_stale_backlog_and_processes_new_events() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-fast-forward-recovery",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let mut source_config = row_table_config("workflow_source");
    source_config.history_retention_sequences = Some(1);
    let source = db
        .create_table(source_config)
        .await
        .expect("create source table");

    let first = source
        .write(b"skipped-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write first stale source event");
    source
        .write(b"skipped-2:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write retained stale source event");
    let durable_before_start = db.current_durable_sequence();

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(source.clone())
                .with_recovery_policy(WorkflowSourceRecoveryPolicy::FastForwardToCurrentDurable)],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");

    let stale_progress = WorkflowSourceProgress::from_cursor(LogCursor::new(first, 0));
    runtime
        .tables()
        .source_progress_table()
        .write(
            source.name().as_bytes().to_vec(),
            Value::bytes(
                stale_progress
                    .encode()
                    .expect("encode stale source progress"),
            ),
        )
        .await
        .expect("persist stale workflow source progress");

    let handle = runtime.start().await.expect("start workflow runtime");
    runtime
        .wait_for_source_progress(
            &source,
            WorkflowSourceProgress::from_durable_sequence(durable_before_start)
                .with_origin(WorkflowSourceProgressOrigin::FastForwardToCurrentDurable),
        )
        .await
        .expect("fast-forward source progress should publish the durable fence");

    assert_eq!(
        runtime
            .load_state("skipped-1")
            .await
            .expect("load skipped stale state"),
        None,
    );
    assert_eq!(
        runtime
            .load_state("skipped-2")
            .await
            .expect("load retained-but-fast-forwarded state"),
        None,
    );

    let telemetry = runtime
        .telemetry_snapshot()
        .await
        .expect("capture fast-forward telemetry");
    assert_eq!(
        telemetry.source_lags[0].progress_origin,
        WorkflowSourceProgressOrigin::FastForwardToCurrentDurable,
    );
    assert_eq!(
        telemetry.source_lags[0].attach_mode,
        Some(WorkflowSourceAttachMode::LiveOnly),
    );

    handle
        .abort()
        .await
        .expect("abort fast-forward workflow runtime");
}

#[tokio::test]
async fn workflow_non_replayable_sources_cannot_opt_into_replay_from_history_recovery() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-non-replayable-recovery",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let mut source_config = row_table_config("workflow_source");
    source_config.history_retention_sequences = Some(1);
    let source = db
        .create_table(source_config)
        .await
        .expect("create source table");

    let first = source
        .write(b"dropped-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write first stale source event");
    source
        .write(b"retained-2:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write retained source event");

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(source.clone())
                .with_recovery_policy(WorkflowSourceRecoveryPolicy::ReplayFromHistory)],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");

    let stale_progress = WorkflowSourceProgress::from_cursor(LogCursor::new(first, 0));
    runtime
        .tables()
        .source_progress_table()
        .write(
            source.name().as_bytes().to_vec(),
            Value::bytes(
                stale_progress
                    .encode()
                    .expect("encode stale source progress"),
            ),
        )
        .await
        .expect("persist stale workflow source progress");

    let mut handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("workflow failure should be observed")
        .expect("workflow should terminate after the injected change-feed error");
    let error = tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("workflow shutdown should return promptly")
        .expect_err(
            "non-replayable source should fail closed when replay-from-history is requested",
        );

    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Unsupported);
            assert!(
                storage.to_string().contains(
                    "cannot replay from history unless it is configured as append-only ordered"
                ),
                "expected explicit non-replayable recovery error, got {storage}",
            );
        }
        other => panic!("unexpected non-replayable recovery error: {other:?}"),
    }

    assert_eq!(
        runtime
            .load_source_progress(&source)
            .await
            .expect("load source progress after failed replay recovery"),
        stale_progress,
        "non-replayable sources must not silently rewrite stale progress during recovery",
    );
}

#[tokio::test]
async fn workflow_restart_resumes_local_inbox_before_source_bootstrap() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let dependencies =
        test_dependencies_with_clock(file_system.clone(), object_store.clone(), clock.clone());
    let path = "/workflow-local-resume-before-bootstrap";
    let db = Db::open(
        tiered_test_config_with_durability(path, TieredDurabilityMode::GroupCommit),
        dependencies,
    )
    .await
    .expect("open first db");
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create source table");
    source
        .write(b"source-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write source backlog");

    let initial_runtime = WorkflowRuntime::open(
        db.clone(),
        clock.clone(),
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(source.clone())],
            BlockingResumeHandler {
                control: BlockingResumeControl::default(),
            },
        ),
    )
    .await
    .expect("open initial workflow runtime");
    initial_runtime
        .admit_callback("local-1", "cb-1", b"approved".to_vec())
        .await
        .expect("persist local callback before restart");
    drop(initial_runtime);
    drop(db);

    let reopened = Db::open(
        tiered_test_config_with_durability(path, TieredDurabilityMode::GroupCommit),
        test_dependencies_with_clock(file_system, object_store, clock),
    )
    .await
    .expect("reopen db");
    let reopened_source = reopened.table("workflow_source");
    let control = BlockingResumeControl::default();
    let runtime = WorkflowRuntime::open(
        reopened,
        Arc::new(StubClock::default()),
        WorkflowDefinition::new(
            "orders",
            [WorkflowSource::new(reopened_source.clone())],
            BlockingResumeHandler {
                control: control.clone(),
            },
        ),
    )
    .await
    .expect("open restarted workflow runtime");

    let handle = runtime
        .start()
        .await
        .expect("start restarted workflow runtime");
    tokio::time::timeout(Duration::from_secs(1), control.callback_entered.notified())
        .await
        .expect("callback should begin during local durable resume");

    assert!(
        runtime
            .tables()
            .source_progress_table()
            .read(reopened_source.name().as_bytes().to_vec())
            .await
            .expect("read source progress during blocked callback")
            .is_none(),
        "source bootstrap must wait until local durable inbox work has resumed",
    );
    assert_eq!(
        runtime
            .load_state("source-1")
            .await
            .expect("load source-backed workflow state while callback is blocked"),
        None,
        "source events should not execute while restart is still replaying local inbox work",
    );

    assert_eq!(
        control.snapshot(),
        vec!["local-1:callback:cb-1:entered".to_string()],
        "restart should still be replaying local durable inbox work before source bootstrap begins",
    );

    handle
        .abort()
        .await
        .expect("abort restarted workflow runtime while callback replay is blocked");
}

#[test]
fn callback_admission_is_durable_before_return_and_replays_after_restart() -> turmoil::Result {
    SeededSimulationRunner::new(0x4102)
        .with_simulation_duration(WORKFLOW_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-callback-replay",
                    TieredDurabilityMode::Deferred,
                ),
                callback_stack_builder(),
            )
            .await?;

            let durable_sequence = harness
                .stack()
                .runtime
                .admit_callback("order-1", "cb-1", b"approved".to_vec())
                .await?;
            checkpoint_workflow(&mut harness, "callback-admitted", vec!["order-1"]).await?;
            harness.require_eq(
                "durable callback admission sequence",
                &harness.db().current_durable_sequence(),
                &durable_sequence,
            )?;

            harness.restart(CutPoint::AfterStep).await?;
            harness.stack_mut().start().await?;
            wait_for_workflow_state(&harness, "order-1", "processed:cb-1").await?;

            checkpoint_workflow(&mut harness, "callback-replayed", vec!["order-1"]).await?;
            harness.db().flush().await?;
            let runtime = &harness.stack().runtime;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"order-1:cb-1".to_vec())
                    .await?
                    .is_some(),
                "replayed outbox entry should be present after restart",
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[test]
fn timer_loop_waits_for_durable_timer_rows_before_firing() -> turmoil::Result {
    SeededSimulationRunner::new(0x4103)
        .with_simulation_duration(WORKFLOW_TIMER_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-timer-fence",
                    TieredDurabilityMode::Deferred,
                ),
                timer_stack_builder(Some(WorkflowProgressMode::Buffered)),
            )
            .await?;
            harness.stack_mut().start().await?;

            harness
                .stack()
                .runtime
                .admit_callback("order-7", "start", b"begin".to_vec())
                .await?;
            checkpoint_workflow(&mut harness, "timer-scheduled", vec!["order-7"]).await?;
            wait_for_workflow_state(&harness, "order-7", "scheduled").await?;

            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(10))
                .await;
            harness.require_eq(
                "timer state before durable flush",
                &harness.stack().runtime.load_state("order-7").await?,
                &Some(Value::bytes("scheduled")),
            )?;

            harness.db().flush().await?;
            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(1))
                .await;
            wait_for_visible_inbox_row(&harness, b"order-7\0\0\0\0\0\0\0\0\x02".to_vec()).await?;

            harness.db().flush().await?;
            wait_for_workflow_state(&harness, "order-7", "fired").await?;
            checkpoint_workflow(&mut harness, "timer-fired", vec!["order-7"]).await?;
            harness.require_eq(
                "timer final state",
                &harness.stack().runtime.load_state("order-7").await?,
                &Some(Value::bytes("fired")),
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[test]
fn workflow_auto_progress_allows_timer_chains_without_manual_flush() -> turmoil::Result {
    SeededSimulationRunner::new(0x4104)
        .with_simulation_duration(WORKFLOW_TIMER_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-durable-progress",
                    TieredDurabilityMode::Deferred,
                ),
                timer_stack_builder(None),
            )
            .await?;
            harness.stack_mut().start().await?;

            harness
                .stack()
                .runtime
                .admit_callback("order-9", "start", b"begin".to_vec())
                .await?;
            wait_for_workflow_state(&harness, "order-9", "scheduled").await?;

            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(10))
                .await;
            wait_for_workflow_state(&harness, "order-9", "fired").await?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[tokio::test]
async fn workflow_checkpoints_restore_workflow_owned_tables_exactly() {
    let root = "/workflow-checkpoint-restore";
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::new(Timestamp::new(7)));
    let checkpoint_store = Arc::new(WorkflowObjectStoreCheckpointStore::new(
        object_store.clone(),
        "workflow-checkpoints",
    ));

    let db = Db::open(
        tiered_test_config_with_durability(root, TieredDurabilityMode::GroupCommit),
        test_dependencies_with_clock(file_system.clone(), object_store.clone(), clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("checkpoint_source"))
        .await
        .expect("create source table");
    let runtime = open_checkpointed_runtime(
        db.clone(),
        source.clone(),
        clock.clone(),
        checkpoint_store.clone(),
        false,
    )
    .await
    .expect("open checkpointed runtime");

    source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write source event");
    let handle = runtime.start().await.expect("start checkpointed runtime");
    wait_for_runtime_state(&runtime, "order-1", "event:order-1:created")
        .await
        .expect("wait for event state");
    handle
        .shutdown()
        .await
        .expect("shutdown checkpointed runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit callback after shutdown");
    let checkpointed_state = runtime
        .load_state_record("order-1")
        .await
        .expect("load checkpointed state record")
        .expect("checkpointed state record should exist");
    let checkpointed_run = runtime
        .load_run_record(&checkpointed_state.run_id)
        .await
        .expect("load checkpointed run record")
        .expect("checkpointed run record should exist");
    let checkpointed_history = runtime
        .load_run_history(&checkpointed_state.run_id)
        .await
        .expect("load checkpointed run history");
    let checkpointed_lifecycle = runtime
        .load_lifecycle_record(&checkpointed_state.run_id)
        .await
        .expect("load checkpointed lifecycle")
        .expect("checkpointed lifecycle record should exist");
    let checkpointed_visibility = runtime
        .load_visibility_record(&checkpointed_state.run_id)
        .await
        .expect("load checkpointed visibility")
        .expect("checkpointed visibility record should exist");
    assert_eq!(
        checkpointed_state.lifecycle,
        WorkflowLifecycleState::Running
    );
    assert_eq!(
        checkpointed_lifecycle.lifecycle,
        checkpointed_state.lifecycle
    );
    assert_eq!(
        checkpointed_visibility.lifecycle,
        checkpointed_state.lifecycle
    );
    assert_eq!(
        checkpointed_history.last().map(|record| record.sequence),
        Some(checkpointed_state.history_len)
    );

    let expected_snapshot = snapshot_workflow_tables(runtime.tables())
        .await
        .expect("capture expected workflow tables");
    let manifest = runtime
        .capture_checkpoint(WorkflowCheckpointId::new(7))
        .await
        .expect("capture checkpoint");
    assert_eq!(manifest.trigger_journal_high_watermark, Some(2));
    assert!(
        manifest
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == WorkflowCheckpointArtifactKind::TriggerJournal)
    );

    clear_workflow_tables(runtime.tables())
        .await
        .expect("clear local workflow tables");
    write_mutated_state_record(runtime.tables().state_table(), &checkpointed_state)
        .await
        .expect("write mutated local state");
    let mutated_snapshot = snapshot_workflow_tables(runtime.tables())
        .await
        .expect("capture mutated workflow tables");
    assert_ne!(mutated_snapshot, expected_snapshot);

    drop(runtime);
    drop(db);

    let reopened_db = Db::open(
        tiered_test_config_with_durability(root, TieredDurabilityMode::GroupCommit),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("reopen db");
    let reopened_source = reopened_db.table("checkpoint_source");
    let restored_runtime =
        open_checkpointed_runtime(reopened_db, reopened_source, clock, checkpoint_store, true)
            .await
            .expect("restore checkpoint on open");

    let restored_snapshot = snapshot_workflow_tables(restored_runtime.tables())
        .await
        .expect("capture restored workflow tables");
    assert_eq!(restored_snapshot, expected_snapshot);
    assert_eq!(
        restored_runtime
            .load_state_record("order-1")
            .await
            .expect("load restored state record")
            .expect("restored state record should exist"),
        checkpointed_state
    );
    assert_eq!(
        restored_runtime
            .load_run_record(&checkpointed_run.run_id)
            .await
            .expect("load restored run record")
            .expect("restored run record should exist"),
        checkpointed_run
    );
    assert_eq!(
        restored_runtime
            .load_lifecycle_record(&checkpointed_run.run_id)
            .await
            .expect("load restored lifecycle")
            .expect("restored lifecycle should exist"),
        checkpointed_lifecycle
    );
    assert_eq!(
        restored_runtime
            .load_visibility_record(&checkpointed_run.run_id)
            .await
            .expect("load restored visibility")
            .expect("restored visibility should exist"),
        checkpointed_visibility
    );
    assert_eq!(
        restored_runtime
            .load_run_history(&checkpointed_run.run_id)
            .await
            .expect("load restored run history"),
        checkpointed_history
    );
}

#[tokio::test]
async fn workflow_checkpoint_manifest_publication_fails_closed_until_latest_pointer_updates() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let checkpoint_store = Arc::new(WorkflowObjectStoreCheckpointStore::new(
        object_store.clone(),
        "workflow-checkpoints",
    ));
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-checkpoint-manifest-publication",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store.clone(), clock.clone()),
    )
    .await
    .expect("open db");
    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        )
        .with_checkpoint_store(checkpoint_store.clone()),
    )
    .await
    .expect("open callback workflow runtime");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit first callback");
    let first_manifest = runtime
        .capture_checkpoint(WorkflowCheckpointId::new(1))
        .await
        .expect("capture first checkpoint");

    runtime
        .admit_callback("order-1", "cb-2", b"approved".to_vec())
        .await
        .expect("admit second callback");
    object_store.inject_failure(ObjectStoreFailure::timeout(
        ObjectStoreOperation::Put,
        checkpoint_store.latest_manifest_key("callbacks"),
    ));

    let error = runtime
        .capture_checkpoint(WorkflowCheckpointId::new(2))
        .await
        .expect_err("latest pointer upload should fail");
    match error {
        WorkflowError::CheckpointStore { source, .. } => {
            assert!(
                source.to_string().contains("Timeout"),
                "expected timeout from object-store publish failure, got {source}",
            );
        }
        other => panic!("unexpected checkpoint publish error: {other:?}"),
    }

    let latest_manifest = checkpoint_store
        .load_latest_manifest("callbacks")
        .await
        .expect("load latest manifest")
        .expect("first checkpoint should remain latest");
    assert_eq!(latest_manifest.checkpoint_id, first_manifest.checkpoint_id);
    assert!(
        checkpoint_store
            .load_manifest("callbacks", WorkflowCheckpointId::new(2))
            .await
            .expect("load second manifest")
            .is_some()
    );
}

#[tokio::test]
async fn workflow_checkpoint_restore_failpoint_leaves_existing_local_state_untouched() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::new(Timestamp::new(9)));
    let checkpoint_store = Arc::new(WorkflowObjectStoreCheckpointStore::new(
        object_store.clone(),
        "workflow-checkpoints",
    ));
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-checkpoint-restore-failpoint",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("checkpoint_source"))
        .await
        .expect("create source table");
    let runtime = open_checkpointed_runtime(
        db.clone(),
        source.clone(),
        clock.clone(),
        checkpoint_store.clone(),
        false,
    )
    .await
    .expect("open checkpointed runtime");

    source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write source event");
    let handle = runtime.start().await.expect("start checkpointed runtime");
    wait_for_runtime_state(&runtime, "order-1", "event:order-1:created")
        .await
        .expect("wait for event state");
    handle
        .shutdown()
        .await
        .expect("shutdown checkpointed runtime");
    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit callback after shutdown");

    let expected_snapshot = snapshot_workflow_tables(runtime.tables())
        .await
        .expect("capture expected workflow tables");
    let checkpointed_state = runtime
        .load_state_record("order-1")
        .await
        .expect("load checkpointed state record before restore failpoint")
        .expect("checkpointed state record should exist before restore failpoint");
    runtime
        .capture_checkpoint(WorkflowCheckpointId::new(3))
        .await
        .expect("capture restore checkpoint");

    clear_workflow_tables(runtime.tables())
        .await
        .expect("clear workflow tables before restore");
    write_mutated_state_record(runtime.tables().state_table(), &checkpointed_state)
        .await
        .expect("write mutated state");
    let mutated_snapshot = snapshot_workflow_tables(runtime.tables())
        .await
        .expect("capture mutated workflow tables");

    db_failpoint_registry(&db).arm_error(
        workflow_failpoint_names::WORKFLOW_CHECKPOINT_RESTORE_BEFORE_COMMIT,
        StorageError::io("simulated checkpoint restore failpoint"),
        FailpointMode::Once,
    );

    let error = match open_checkpointed_runtime(
        db.clone(),
        source.clone(),
        clock.clone(),
        checkpoint_store.clone(),
        true,
    )
    .await
    {
        Ok(_) => panic!("restore failpoint should abort runtime open"),
        Err(error) => error,
    };
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Io);
            assert!(
                storage
                    .to_string()
                    .contains("simulated checkpoint restore failpoint"),
                "expected injected restore failpoint context, got {storage}",
            );
        }
        other => panic!("unexpected restore failure: {other:?}"),
    }

    let after_failed_restore = snapshot_workflow_tables(runtime.tables())
        .await
        .expect("capture post-failure workflow tables");
    assert_eq!(after_failed_restore, mutated_snapshot);

    let restored_runtime = open_checkpointed_runtime(db, source, clock, checkpoint_store, true)
        .await
        .expect("restore checkpoint after one-shot failpoint");
    let restored_snapshot = snapshot_workflow_tables(restored_runtime.tables())
        .await
        .expect("capture restored workflow tables");
    assert_eq!(restored_snapshot, expected_snapshot);
}

#[tokio::test]
async fn workflow_checkpoint_bootstrap_restores_checkpointed_state_and_retags_progress() {
    let root = "/workflow-auto-checkpoint-bootstrap";
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::new(Timestamp::new(7)));
    let checkpoint_store = Arc::new(WorkflowObjectStoreCheckpointStore::new(
        object_store.clone(),
        "workflow-checkpoints",
    ));

    let db = Db::open(
        tiered_test_config_with_durability(root, TieredDurabilityMode::GroupCommit),
        test_dependencies_with_clock(file_system.clone(), object_store.clone(), clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("checkpoint_source"))
        .await
        .expect("create source table");
    let runtime = open_checkpointed_runtime(
        db.clone(),
        source.clone(),
        clock.clone(),
        checkpoint_store.clone(),
        false,
    )
    .await
    .expect("open initial checkpoint runtime");

    source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write source event");
    let handle = runtime.start().await.expect("start workflow runtime");
    wait_for_runtime_state(&runtime, "order-1", "event:order-1:created")
        .await
        .expect("wait for event state");
    handle
        .shutdown()
        .await
        .expect("shutdown checkpoint runtime");

    runtime
        .capture_checkpoint(WorkflowCheckpointId::new(21))
        .await
        .expect("capture checkpoint");

    clear_workflow_tables(runtime.tables())
        .await
        .expect("clear workflow tables before bootstrap restore");
    drop(runtime);
    drop(db);

    let reopened_db = Db::open(
        tiered_test_config_with_durability(root, TieredDurabilityMode::GroupCommit),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("reopen db");
    let reopened_source = reopened_db.table("checkpoint_source");
    let restored_runtime = open_historical_checkpoint_runtime(
        reopened_db,
        reopened_source.clone(),
        clock,
        checkpoint_store,
        WorkflowSourceConfig::default()
            .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CheckpointOrBeginning)
            .with_checkpoint_support(WorkflowHistoricalArtifactSupport::Optional)
            .with_trigger_journal_support(WorkflowHistoricalArtifactSupport::Optional),
    )
    .await
    .expect("open bootstrap-restore runtime");

    let handle = restored_runtime
        .start()
        .await
        .expect("start bootstrap-restore runtime");

    restored_runtime
        .wait_for_state("order-1", Value::bytes("event:order-1:created"))
        .await
        .expect("bootstrap restore should restore checkpointed workflow state");
    let telemetry = restored_runtime
        .telemetry_snapshot()
        .await
        .expect("capture restored telemetry");

    assert_eq!(
        restored_runtime
            .load_state("order-1")
            .await
            .expect("load restored workflow state"),
        Some(Value::bytes("event:order-1:created")),
    );
    assert!(
        restored_runtime
            .tables()
            .outbox_table()
            .read(b"order-1:event".to_vec())
            .await
            .expect("read restored outbox entry")
            .is_some(),
        "checkpoint bootstrap should restore durable outbox work",
    );
    assert_eq!(
        scan_table_rows(restored_runtime.tables().timer_schedule_table())
            .await
            .expect("scan restored timer schedule")
            .len(),
        1,
        "checkpoint bootstrap should restore the durable timer schedule",
    );

    assert_eq!(
        telemetry.source_lags[0].progress_origin,
        WorkflowSourceProgressOrigin::CheckpointRestore,
    );
    assert_eq!(
        telemetry.source_lags[0].attach_mode,
        Some(WorkflowSourceAttachMode::Historical),
    );

    drop(handle);
}

#[tokio::test]
async fn workflow_restore_checkpoint_recovery_replays_restored_callback_and_new_history() {
    let root = "/workflow-auto-checkpoint-recovery";
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::new(Timestamp::new(7)));
    let checkpoint_store = Arc::new(WorkflowObjectStoreCheckpointStore::new(
        object_store.clone(),
        "workflow-checkpoints",
    ));

    let db = Db::open(
        tiered_test_config_with_durability(root, TieredDurabilityMode::GroupCommit),
        test_dependencies_with_clock(file_system.clone(), object_store.clone(), clock.clone()),
    )
    .await
    .expect("open db");
    let mut source_config = row_table_config("checkpoint_source");
    source_config.history_retention_sequences = Some(3);
    let source = db
        .create_table(source_config)
        .await
        .expect("create checkpoint source");
    let runtime = open_checkpointed_runtime(
        db.clone(),
        source.clone(),
        clock.clone(),
        checkpoint_store.clone(),
        false,
    )
    .await
    .expect("open initial checkpoint runtime");

    let first = source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write first source event");
    let handle = runtime.start().await.expect("start workflow runtime");
    wait_for_runtime_state(&runtime, "order-1", "event:order-1:created")
        .await
        .expect("wait for first source event");
    handle.shutdown().await.expect("shutdown workflow runtime");

    let second = source
        .write(b"order-2:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write second source event");
    runtime
        .tables()
        .source_progress_table()
        .write(
            source.name().as_bytes().to_vec(),
            Value::bytes(
                WorkflowSourceProgress::from_cursor(LogCursor::new(second, 0))
                    .encode()
                    .expect("encode checkpoint frontier"),
            ),
        )
        .await
        .expect("advance checkpoint source frontier");

    runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit callback before checkpoint");
    let checkpointed_state = runtime
        .load_state_record("order-1")
        .await
        .expect("load checkpointed state record")
        .expect("checkpointed state record should exist");
    let checkpointed_run = runtime
        .load_run_record(&checkpointed_state.run_id)
        .await
        .expect("load checkpointed run record")
        .expect("checkpointed run record should exist");
    let checkpointed_history = runtime
        .load_run_history(&checkpointed_state.run_id)
        .await
        .expect("load checkpointed history");
    let checkpointed_lifecycle = runtime
        .load_lifecycle_record(&checkpointed_state.run_id)
        .await
        .expect("load checkpointed lifecycle")
        .expect("checkpointed lifecycle should exist");
    let checkpointed_visibility = runtime
        .load_visibility_record(&checkpointed_state.run_id)
        .await
        .expect("load checkpointed visibility")
        .expect("checkpointed visibility should exist");
    runtime
        .capture_checkpoint(WorkflowCheckpointId::new(22))
        .await
        .expect("capture checkpoint");

    let stale_progress = WorkflowSourceProgress::from_cursor(LogCursor::new(first, 0));
    source
        .write(b"order-3:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write retained post-checkpoint source event");

    clear_workflow_tables(runtime.tables())
        .await
        .expect("clear workflow tables before stale-progress recovery");
    write_mutated_state_record(runtime.tables().state_table(), &checkpointed_state)
        .await
        .expect("write mutated local state");
    runtime
        .tables()
        .source_progress_table()
        .write(
            source.name().as_bytes().to_vec(),
            Value::bytes(
                stale_progress
                    .encode()
                    .expect("encode stale source progress"),
            ),
        )
        .await
        .expect("persist stale local source progress");

    drop(runtime);
    drop(db);

    let reopened_db = Db::open(
        tiered_test_config_with_durability(root, TieredDurabilityMode::GroupCommit),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("reopen db");
    let reopened_source = reopened_db.table("checkpoint_source");
    let recovery_runtime = open_historical_checkpoint_runtime(
        reopened_db,
        reopened_source.clone(),
        clock,
        checkpoint_store,
        WorkflowSourceConfig::default()
            .with_recovery_policy(WorkflowSourceRecoveryPolicy::RestoreCheckpoint)
            .with_checkpoint_support(WorkflowHistoricalArtifactSupport::Optional)
            .with_trigger_journal_support(WorkflowHistoricalArtifactSupport::Optional),
    )
    .await
    .expect("open checkpoint-recovery runtime");

    let handle = recovery_runtime
        .start()
        .await
        .expect("start checkpoint-recovery runtime");
    wait_for_runtime_state(&recovery_runtime, "order-1", "callback:cb-1")
        .await
        .expect("replay restored callback from checkpoint");

    assert!(
        recovery_runtime
            .tables()
            .outbox_table()
            .read(b"order-1:cb-1".to_vec())
            .await
            .expect("read replayed callback outbox entry")
            .is_some(),
        "checkpoint recovery should replay restored callback work",
    );
    let recovered_state = recovery_runtime
        .load_state_record("order-1")
        .await
        .expect("load recovered state record")
        .expect("recovered state record should exist");
    let recovered_run = recovery_runtime
        .load_run_record(&checkpointed_run.run_id)
        .await
        .expect("load recovered run record")
        .expect("recovered run record should exist");
    let recovered_history = recovery_runtime
        .load_run_history(&checkpointed_run.run_id)
        .await
        .expect("load recovered run history");
    let recovered_lifecycle = recovery_runtime
        .load_lifecycle_record(&checkpointed_run.run_id)
        .await
        .expect("load recovered lifecycle")
        .expect("recovered lifecycle should exist");
    let recovered_visibility = recovery_runtime
        .load_visibility_record(&checkpointed_run.run_id)
        .await
        .expect("load recovered visibility")
        .expect("recovered visibility should exist");

    assert_eq!(recovered_state.run_id, checkpointed_state.run_id);
    assert_eq!(recovered_run, checkpointed_run);
    assert_eq!(recovered_lifecycle.run_id, recovered_state.run_id);
    assert_eq!(recovered_lifecycle.lifecycle, recovered_state.lifecycle);
    assert_eq!(recovered_visibility.run_id, recovered_state.run_id);
    assert_eq!(recovered_visibility.lifecycle, recovered_state.lifecycle);
    assert_eq!(
        recovered_visibility.history_len,
        recovered_state.history_len
    );
    assert_eq!(
        recovered_visibility.last_task_id,
        recovered_state.current_task_id
    );
    assert!(recovered_state.history_len > checkpointed_state.history_len);
    assert!(
        recovered_history
            .as_slice()
            .starts_with(checkpointed_history.as_slice())
    );
    assert_eq!(
        recovered_history
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        (1..=recovered_history.len() as u64).collect::<Vec<_>>()
    );
    assert!(matches!(
        recovered_history[checkpointed_history.len()].event,
        WorkflowHistoryEvent::TaskAdmitted { .. }
    ));
    assert!(matches!(
        recovered_history[checkpointed_history.len() + 1].event,
        WorkflowHistoryEvent::TaskApplied { .. }
    ));
    assert_eq!(
        checkpointed_lifecycle.lifecycle,
        checkpointed_state.lifecycle
    );
    assert_eq!(
        checkpointed_visibility.lifecycle,
        checkpointed_state.lifecycle
    );
    assert_eq!(
        recovery_runtime
            .load_source_progress(&reopened_source)
            .await
            .expect("load recovered source progress")
            .origin(),
        WorkflowSourceProgressOrigin::CheckpointRestore,
    );

    handle
        .abort()
        .await
        .expect("abort checkpoint-recovery runtime");
}

#[tokio::test]
async fn recurring_workflow_skips_duplicate_bootstrap_callbacks_and_updates_state() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::new(Timestamp::new(3)));
    let db = Db::open(
        tiered_test_config_with_durability(
            "/recurring-bootstrap-dedupe",
            TieredDurabilityMode::Deferred,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = RecurringWorkflowRuntime::open(
        db,
        clock.clone(),
        RecurringWorkflowDefinition::new(
            "planner",
            "planner-1",
            RecurringSchedule::fixed_interval(10),
            RecurringTimerHandler,
        )
        .with_timer_poll_interval(Duration::from_millis(1)),
    )
    .await
    .expect("open recurring workflow runtime");

    assert!(
        runtime
            .ensure_bootstrapped()
            .await
            .expect("admit first bootstrap")
            .is_some()
    );
    assert!(
        runtime
            .ensure_bootstrapped()
            .await
            .expect("admit duplicate bootstrap")
            .is_some()
    );

    let handle = runtime
        .start()
        .await
        .expect("start recurring workflow runtime");
    let bootstrapped = wait_for_recurring_state(&runtime, |state| state.next_fire_at.is_some())
        .await
        .expect("wait for recurring bootstrap state");
    assert_eq!(bootstrapped.bootstrapped_at, Timestamp::new(3));
    assert_eq!(bootstrapped.last_tick_at, None);
    assert_eq!(bootstrapped.next_fire_at, Some(Timestamp::new(13)));
    assert_eq!(bootstrapped.tick_count, 0);

    clock.set(Timestamp::new(15));
    let fired = wait_for_recurring_state(&runtime, |state| state.tick_count == 1)
        .await
        .expect("wait for recurring timer tick");
    assert_eq!(fired.last_tick_at, Some(Timestamp::new(13)));
    assert_eq!(fired.next_fire_at, Some(Timestamp::new(23)));
    assert_eq!(fired.tick_count, 1);

    assert!(
        runtime
            .tables()
            .outbox_table()
            .read(b"planner-1:1".to_vec())
            .await
            .expect("read first recurring outbox entry")
            .is_some()
    );
    assert!(
        runtime
            .tables()
            .outbox_table()
            .read(b"planner-1:2".to_vec())
            .await
            .expect("read duplicate recurring outbox entry")
            .is_none()
    );

    handle
        .shutdown()
        .await
        .expect("shutdown recurring workflow runtime");
}

#[test]
fn recurring_workflow_recovers_after_restart_with_custom_schedule() -> turmoil::Result {
    SeededSimulationRunner::new(0x4105)
        .with_simulation_duration(Duration::from_millis(2_500))
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/recurring-restart",
                    TieredDurabilityMode::Deferred,
                ),
                recurring_stack_builder(),
            )
            .await?;
            harness.stack_mut().start().await?;

            wait_for_simulation_recurring_tick_count(&harness, 0).await?;
            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(10))
                .await;
            wait_for_simulation_recurring_tick_count(&harness, 1).await?;

            harness.restart(CutPoint::AfterStep).await?;
            harness.stack_mut().start().await?;
            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(10))
                .await;
            wait_for_simulation_recurring_tick_count(&harness, 2).await?;

            let runtime = &harness.stack().runtime;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"job-1:1".to_vec())
                    .await?
                    .is_some(),
                "first recurring outbox entry should survive restart",
            )?;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"job-1:2".to_vec())
                    .await?
                    .is_some(),
                "second recurring outbox entry should be emitted after restart",
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[tokio::test]
async fn workflow_runtime_surfaces_change_feed_scan_failures_without_panicking() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-change-feed-failure",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies(file_system.clone(), object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create source table");

    source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write source event");

    file_system.inject_failure(
        FileSystemFailure::timeout(
            FileSystemOperation::ReadAt,
            "/workflow-change-feed-failure/commitlog/SEG-000001",
        )
        .persistent(),
    );

    let runtime = WorkflowRuntime::open(
        db.clone(),
        Arc::new(StubClock::default()),
        WorkflowDefinition::new(
            "orders",
            [source.clone()],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");
    let mut handle = runtime.start().await.expect("start workflow runtime");

    tokio::time::timeout(Duration::from_secs(1), handle.wait_until_terminal())
        .await
        .expect("workflow failure should be observed")
        .expect("workflow should terminate after the injected change-feed error");

    match tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("workflow shutdown should return promptly")
        .expect_err("workflow runtime should surface the underlying change-feed error")
    {
        WorkflowError::ChangeFeed(ChangeFeedError::Storage(error))
        | WorkflowError::Storage(error) => {
            assert_eq!(error.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("unexpected workflow error: {other}"),
    }

    assert_eq!(
        runtime
            .load_source_cursor(&source)
            .await
            .expect("load source cursor after failed workflow runtime"),
        LogCursor::beginning()
    );
}

#[tokio::test]
async fn contract_runtime_keeps_native_and_sandbox_handlers_in_parity() {
    let native_snapshot = drive_contract_runtime(
        "/workflow-contract-native",
        Arc::new(StubClock::new(Timestamp::new(100))),
        ContractWorkflowHandler::new(
            native_contract_bundle(),
            NativeContractHandler {
                logic: ContractParityLogic,
            },
        )
        .expect("native contract handler"),
        "contract-billing",
    )
    .await
    .expect("drive native contract runtime");

    let sandbox_bundle = contract_bundle("/workspace/billing.js");
    let sandbox_snapshot = drive_contract_runtime(
        "/workflow-contract-sandbox",
        Arc::new(StubClock::new(Timestamp::new(100))),
        open_sandbox_contract_handler(
            100,
            501,
            VolumeId::new(0x9400),
            VolumeId::new(0x9401),
            "/workspace/billing.js",
            sandbox_bundle,
        )
        .await,
        "contract-billing",
    )
    .await
    .expect("drive sandbox contract runtime");

    assert_eq!(native_snapshot, sandbox_snapshot);
}

#[tokio::test]
async fn contract_runtime_requires_explicit_runtime_surface_compatibility() {
    let mut bundle = native_contract_bundle();
    bundle.labels.clear();
    let error = ContractWorkflowHandler::new(
        bundle,
        NativeContractHandler {
            logic: ContractParityLogic,
        },
    )
    .expect_err("missing runtime-surface label should fail fast");
    assert_eq!(error.code, "invalid-contract");
    assert!(
        error
            .message
            .contains(terracedb_workflows::WORKFLOW_RUNTIME_SURFACE_LABEL)
    );
}

#[tokio::test]
async fn contract_runtime_rejects_legacy_pending_inbox_rows_without_admitted_at() {
    #[derive(Serialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    enum LegacyStoredWorkflowTrigger {
        Callback {
            callback_id: String,
            response: Vec<u8>,
        },
    }

    #[derive(Serialize)]
    struct LegacyStoredAdmittedWorkflowTrigger {
        workflow_instance: String,
        trigger_seq: u64,
        trigger: LegacyStoredWorkflowTrigger,
        operation_context: Option<terracedb::OperationContext>,
    }

    fn encode_legacy_payload(value: &impl Serialize) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(
            &serde_json::to_vec(value).expect("encode legacy workflow inbox payload"),
        );
        bytes
    }

    let clock = Arc::new(StubClock::new(Timestamp::new(300)));
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-contract-legacy-inbox",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let bootstrap_runtime = WorkflowRuntime::open(
        db.clone(),
        clock.clone(),
        WorkflowDefinition::new(
            "contract-billing",
            std::iter::empty::<Table>(),
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open bootstrap runtime");
    bootstrap_runtime
        .tables()
        .inbox_table()
        .write(
            {
                let mut key = b"acct-7".to_vec();
                key.push(0);
                key.extend_from_slice(&1_u64.to_be_bytes());
                key
            },
            Value::bytes(encode_legacy_payload(
                &LegacyStoredAdmittedWorkflowTrigger {
                    workflow_instance: "acct-7".to_string(),
                    trigger_seq: 1,
                    trigger: LegacyStoredWorkflowTrigger::Callback {
                        callback_id: "cb-legacy".to_string(),
                        response: b"legacy".to_vec(),
                    },
                    operation_context: None,
                },
            )),
        )
        .await
        .expect("write legacy inbox row");

    let error = match WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "contract-billing",
            std::iter::empty::<Table>(),
            ContractWorkflowHandler::new(
                native_contract_bundle(),
                NativeContractHandler {
                    logic: ContractParityLogic,
                },
            )
            .expect("native contract handler"),
        ),
    )
    .await
    {
        Ok(_) => panic!("legacy inbox rows should fail fast for contract runtime"),
        Err(error) => error,
    };

    match error {
        WorkflowError::Handler { source, .. } => {
            assert!(
                source
                    .to_string()
                    .contains("rejects unsupported pending inbox row")
            );
        }
        other => panic!("expected handler validation error, got {other:?}"),
    }
}

#[tokio::test]
async fn sandbox_contract_runtime_reopen_preserves_replay_semantics() {
    let clock = Arc::new(StubClock::new(Timestamp::new(200)));
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-contract-sandbox-restart",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let bundle = contract_bundle("/workspace/billing.js");

    let runtime = tokio::time::timeout(
        Duration::from_secs(5),
        WorkflowRuntime::open(
            db.clone(),
            clock.clone(),
            WorkflowDefinition::new(
                "contract-billing",
                std::iter::empty::<Table>(),
                open_sandbox_contract_handler(
                    200,
                    601,
                    VolumeId::new(0x9410),
                    VolumeId::new(0x9411),
                    "/workspace/billing.js",
                    bundle.clone(),
                )
                .await,
            ),
        ),
    )
    .await
    .expect("timed out opening initial workflow runtime")
    .expect("open initial workflow runtime");
    let handle = runtime
        .start()
        .await
        .expect("start initial workflow runtime");

    runtime
        .admit_callback("acct-7", "cb-1", b"first".to_vec())
        .await
        .expect("admit first callback");
    tokio::time::timeout(
        Duration::from_secs(5),
        runtime.wait_for_state("acct-7", Value::bytes("contract:contract-billing:1")),
    )
    .await
    .expect("timed out waiting for first state")
    .expect("wait for first state");
    tokio::time::timeout(Duration::from_secs(5), handle.abort())
        .await
        .expect("timed out aborting initial runtime")
        .expect("abort initial runtime");

    let reopened_runtime = tokio::time::timeout(
        Duration::from_secs(5),
        WorkflowRuntime::open(
            db.clone(),
            clock.clone(),
            WorkflowDefinition::new(
                "contract-billing",
                std::iter::empty::<Table>(),
                open_sandbox_contract_handler(
                    200,
                    602,
                    VolumeId::new(0x9420),
                    VolumeId::new(0x9421),
                    "/workspace/billing.js",
                    bundle,
                )
                .await,
            ),
        ),
    )
    .await
    .expect("timed out reopening workflow runtime")
    .expect("reopen workflow runtime");
    let reopened_handle = reopened_runtime
        .start()
        .await
        .expect("start reopened workflow runtime");

    reopened_runtime
        .admit_callback("acct-7", "cb-2", b"second".to_vec())
        .await
        .expect("admit second callback");
    tokio::time::timeout(
        Duration::from_secs(5),
        reopened_runtime.wait_for_state("acct-7", Value::bytes("contract:contract-billing:2")),
    )
    .await
    .expect("timed out waiting for second state")
    .expect("wait for second state");

    assert_eq!(
        reopened_runtime
            .load_state("acct-7")
            .await
            .expect("load reopened state"),
        Some(Value::bytes("contract:contract-billing:2"))
    );
    assert!(
        reopened_runtime
            .tables()
            .outbox_table()
            .read(b"acct-7:1".to_vec())
            .await
            .expect("read first outbox entry")
            .is_some()
    );
    assert!(
        reopened_runtime
            .tables()
            .outbox_table()
            .read(b"acct-7:2".to_vec())
            .await
            .expect("read second outbox entry")
            .is_some()
    );

    tokio::time::timeout(Duration::from_secs(5), reopened_handle.abort())
        .await
        .expect("timed out aborting reopened runtime")
        .expect("abort reopened runtime");
}
