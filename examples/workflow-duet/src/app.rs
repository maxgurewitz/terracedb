use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use terracedb::{
    Clock, Db, DbBuilder, DbConfig, DbDependencies, DbSettings, ReadError, Rng, S3Location,
    ScanOptions, SequenceNumber, StorageError, StubClock, StubFileSystem, StubObjectStore, StubRng,
    SubscriptionClosed, Table, TieredDurabilityMode, TieredStorageConfig, Timestamp, Transaction,
    TransactionCommitError, Value, decode_outbox_entry,
};
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, PackageCompatibilityMode, PackageInstallRequest,
    SandboxConfig, SandboxError, SandboxServices, SandboxSession, SandboxStore, TypeCheckRequest,
};
use terracedb_vfs::{
    CreateOptions, InMemoryVfsStore, VfsError, VolumeConfig, VolumeId, VolumeStore,
};
use terracedb_workflows::{
    ContractWorkflowHandler, WorkflowDefinition, WorkflowError, WorkflowHandle, WorkflowRuntime,
    contracts::{self, NativeWorkflowHandlerAdapter, WorkflowHandlerContract, WorkflowTaskError},
};
use terracedb_workflows_sandbox::{
    SandboxModuleWorkflowTaskV1Handler, SandboxWorkflowHandlerAdapter,
};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};

use crate::model::{
    APPROVAL_TIMEOUT_MILLIS, APPROVE_CALLBACK_ID, NATIVE_WORKFLOW_NAME, RETRY_DELAY_MILLIS,
    ReviewOutboxMessage, ReviewStage, ReviewState, SANDBOX_PACKAGE_JSON_PATH,
    SANDBOX_SOURCE_MODULE_PATH, SANDBOX_TSCONFIG_PATH, SANDBOX_WORKFLOW_NAME, START_CALLBACK_ID,
    decode_review_state, native_registration, review_transition_output, sandbox_bundle,
};

const BASE_VOLUME_ID: VolumeId = VolumeId::new(0x7700);
const SESSION_VOLUME_ID: VolumeId = VolumeId::new(0x7701);
const WORKFLOW_TIMER_POLL_INTERVAL: Duration = Duration::from_millis(2);
const RELAY_BATCH_LIMIT: usize = 128;
const RELAY_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(2);
const PAIR_START_TIMEOUT: Duration = Duration::from_secs(30);

type NativeRuntimeHandler =
    ContractWorkflowHandler<NativeWorkflowHandlerAdapter<NativeReviewWorkflow>>;
type SandboxRuntimeHandler =
    ContractWorkflowHandler<SandboxWorkflowHandlerAdapter<SandboxModuleWorkflowTaskV1Handler>>;

pub type NativeReviewRuntime = WorkflowRuntime<NativeRuntimeHandler>;
pub type SandboxReviewRuntime = WorkflowRuntime<SandboxRuntimeHandler>;

#[derive(Debug, Error)]
pub enum WorkflowDuetError {
    #[error(transparent)]
    Open(#[from] terracedb::OpenError),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    TransactionCommit(#[from] TransactionCommitError),
    #[error(transparent)]
    SubscriptionClosed(#[from] SubscriptionClosed),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Workflow(#[from] WorkflowError),
    #[error(transparent)]
    WorkflowTask(#[from] WorkflowTaskError),
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    #[error(transparent)]
    Vfs(#[from] VfsError),
    #[error("workflow-duet relay task failed: {0}")]
    RelayTask(String),
    #[error("timed out waiting for {flavor:?}/{instance_id} to reach stage {expected}")]
    WaitTimeout {
        flavor: WorkflowDuetFlavor,
        instance_id: String,
        expected: &'static str,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowDuetFlavor {
    Native,
    Sandbox,
}

impl WorkflowDuetFlavor {
    pub fn workflow_name(self) -> &'static str {
        match self {
            Self::Native => NATIVE_WORKFLOW_NAME,
            Self::Sandbox => SANDBOX_WORKFLOW_NAME,
        }
    }

    pub fn counterpart(self) -> Self {
        match self {
            Self::Native => Self::Sandbox,
            Self::Sandbox => Self::Native,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDuetInspection {
    pub flavor: WorkflowDuetFlavor,
    pub workflow_name: String,
    pub instance_id: String,
    pub run_id: String,
    pub target: String,
    pub lifecycle: String,
    pub status: String,
    pub attempt: u32,
    pub waiting_for_callback: Option<String>,
    pub deadline_millis: Option<u64>,
    pub latest_savepoint_id: Option<String>,
    pub visibility_summary: BTreeMap<String, String>,
    pub visible_statuses: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDuetDemoReport {
    pub native: WorkflowDuetInspection,
    pub sandbox: WorkflowDuetInspection,
}

#[derive(Default)]
pub struct NativeReviewWorkflow;

#[async_trait]
impl WorkflowHandlerContract for NativeReviewWorkflow {
    async fn route_event(
        &self,
        _event: &contracts::WorkflowSourceEvent,
    ) -> Result<String, WorkflowTaskError> {
        Err(WorkflowTaskError::invalid_contract(
            "workflow-duet uses explicit callbacks rather than table-routed source events",
        ))
    }

    async fn handle_task(
        &self,
        input: contracts::WorkflowTransitionInput,
        _ctx: contracts::WorkflowDeterministicContext,
    ) -> Result<contracts::WorkflowTransitionOutput, WorkflowTaskError> {
        review_transition_output(NATIVE_WORKFLOW_NAME, &input)
    }
}

#[derive(Clone)]
pub struct WorkflowDuetApp {
    db: Db,
    clock: Arc<dyn Clock>,
    native: NativeReviewRuntime,
    sandbox: SandboxReviewRuntime,
    _sandbox_vfs: Arc<InMemoryVfsStore>,
}

impl WorkflowDuetApp {
    pub async fn open(
        db: Db,
        clock: Arc<dyn Clock>,
        rng: Arc<dyn Rng>,
    ) -> Result<Self, WorkflowDuetError> {
        let native = WorkflowRuntime::open(
            db.clone(),
            clock.clone(),
            WorkflowDefinition::new(
                NATIVE_WORKFLOW_NAME,
                std::iter::empty::<terracedb::Table>(),
                ContractWorkflowHandler::new(
                    native_registration(),
                    NativeWorkflowHandlerAdapter::new(NativeReviewWorkflow),
                )?,
            )
            .with_timer_poll_interval(WORKFLOW_TIMER_POLL_INTERVAL),
        )
        .await?;

        let sandbox_vfs = Arc::new(InMemoryVfsStore::new(clock.clone(), rng));
        let sandbox_session = open_sandbox_session(sandbox_vfs.clone(), clock.clone()).await?;
        let bundle = sandbox_bundle();
        let sandbox = WorkflowRuntime::open(
            db.clone(),
            clock.clone(),
            WorkflowDefinition::new(
                SANDBOX_WORKFLOW_NAME,
                std::iter::empty::<terracedb::Table>(),
                ContractWorkflowHandler::new(
                    bundle.clone(),
                    SandboxWorkflowHandlerAdapter::new(SandboxModuleWorkflowTaskV1Handler::new(
                        sandbox_session,
                        bundle,
                    )?),
                )?,
            )
            .with_timer_poll_interval(WORKFLOW_TIMER_POLL_INTERVAL),
        )
        .await?;

        Ok(Self {
            db,
            clock,
            native,
            sandbox,
            _sandbox_vfs: sandbox_vfs,
        })
    }

    pub fn native_runtime(&self) -> &NativeReviewRuntime {
        &self.native
    }

    pub fn sandbox_runtime(&self) -> &SandboxReviewRuntime {
        &self.sandbox
    }

    pub async fn start(&self) -> Result<WorkflowDuetHandles, WorkflowError> {
        let native = self.native.start().await?;
        match self.sandbox.start().await {
            Ok(sandbox) => Ok(WorkflowDuetHandles {
                native,
                sandbox,
                native_relay: spawn_relay(self.clone(), WorkflowDuetFlavor::Native),
                sandbox_relay: spawn_relay(self.clone(), WorkflowDuetFlavor::Sandbox),
            }),
            Err(error) => {
                let _ = native.abort().await;
                Err(error)
            }
        }
    }

    pub async fn kick_off(
        &self,
        flavor: WorkflowDuetFlavor,
        instance_id: &str,
    ) -> Result<SequenceNumber, WorkflowError> {
        self.admit_callback(flavor, instance_id, START_CALLBACK_ID, b"begin")
            .await
    }

    pub async fn kick_off_pair(&self, instance_id: &str) -> Result<(), WorkflowDuetError> {
        self.kick_off(WorkflowDuetFlavor::Native, instance_id)
            .await?;
        self.kick_off(WorkflowDuetFlavor::Sandbox, instance_id)
            .await?;
        let native = self.wait_for_stage(
            WorkflowDuetFlavor::Native,
            instance_id,
            ReviewStage::RetryBackoff,
            PAIR_START_TIMEOUT,
        );
        let sandbox = self.wait_for_stage(
            WorkflowDuetFlavor::Sandbox,
            instance_id,
            ReviewStage::RetryBackoff,
            PAIR_START_TIMEOUT,
        );
        let _ = tokio::try_join!(native, sandbox)?;
        Ok(())
    }

    pub async fn approve(
        &self,
        flavor: WorkflowDuetFlavor,
        instance_id: &str,
    ) -> Result<SequenceNumber, WorkflowError> {
        self.admit_callback(flavor, instance_id, APPROVE_CALLBACK_ID, b"approved")
            .await
    }

    pub async fn admit_callback(
        &self,
        flavor: WorkflowDuetFlavor,
        instance_id: &str,
        callback_id: &str,
        response: &[u8],
    ) -> Result<SequenceNumber, WorkflowError> {
        match flavor {
            WorkflowDuetFlavor::Native => {
                self.native
                    .admit_callback(instance_id, callback_id, response.to_vec())
                    .await
            }
            WorkflowDuetFlavor::Sandbox => {
                self.sandbox
                    .admit_callback(instance_id, callback_id, response.to_vec())
                    .await
            }
        }
    }

    pub async fn load_review_state(
        &self,
        flavor: WorkflowDuetFlavor,
        instance_id: &str,
    ) -> Result<Option<ReviewState>, WorkflowDuetError> {
        let state_record = match flavor {
            WorkflowDuetFlavor::Native => self.native.load_state_record(instance_id).await?,
            WorkflowDuetFlavor::Sandbox => self.sandbox.load_state_record(instance_id).await?,
        };
        decode_review_state(
            state_record
                .as_ref()
                .and_then(|record| record.state.as_ref()),
        )
        .map_err(Into::into)
    }

    pub async fn wait_for_stage(
        &self,
        flavor: WorkflowDuetFlavor,
        instance_id: &str,
        expected: ReviewStage,
        timeout: Duration,
    ) -> Result<ReviewState, WorkflowDuetError> {
        let expected_name = expected.as_str();
        tokio::time::timeout(timeout, async {
            let value = match flavor {
                WorkflowDuetFlavor::Native => {
                    self.native
                        .wait_for_state_where(instance_id, |value| {
                            runtime_value_to_review_state(value)
                                .is_some_and(|state| state.stage == expected)
                        })
                        .await?
                }
                WorkflowDuetFlavor::Sandbox => {
                    self.sandbox
                        .wait_for_state_where(instance_id, |value| {
                            runtime_value_to_review_state(value)
                                .is_some_and(|state| state.stage == expected)
                        })
                        .await?
                }
            };
            runtime_value_to_review_state(value.as_ref()).ok_or_else(|| {
                WorkflowDuetError::WorkflowTask(WorkflowTaskError::invalid_contract(
                    "workflow-duet state disappeared while waiting for a stage transition",
                ))
            })
        })
        .await
        .map_err(|_| WorkflowDuetError::WaitTimeout {
            flavor,
            instance_id: instance_id.to_string(),
            expected: expected_name,
        })?
    }

    pub async fn inspect_instance(
        &self,
        flavor: WorkflowDuetFlavor,
        instance_id: &str,
    ) -> Result<Option<WorkflowDuetInspection>, WorkflowDuetError> {
        let (state_record, visibility_record, savepoint, visible_history) = match flavor {
            WorkflowDuetFlavor::Native => {
                let state_record = self.native.load_state_record(instance_id).await?;
                let Some(state_record) = state_record else {
                    return Ok(None);
                };
                let visibility_record = self
                    .native
                    .load_visibility_record(&state_record.run_id)
                    .await?
                    .ok_or_else(|| {
                        WorkflowDuetError::WorkflowTask(WorkflowTaskError::invalid_contract(
                            "workflow visibility record should exist while state is active",
                        ))
                    })?;
                let savepoint = self
                    .native
                    .load_savepoint_record(&state_record.run_id)
                    .await?;
                let visible_history = self
                    .native
                    .load_visible_history(&state_record.run_id)
                    .await?;
                (state_record, visibility_record, savepoint, visible_history)
            }
            WorkflowDuetFlavor::Sandbox => {
                let state_record = self.sandbox.load_state_record(instance_id).await?;
                let Some(state_record) = state_record else {
                    return Ok(None);
                };
                let visibility_record = self
                    .sandbox
                    .load_visibility_record(&state_record.run_id)
                    .await?
                    .ok_or_else(|| {
                        WorkflowDuetError::WorkflowTask(WorkflowTaskError::invalid_contract(
                            "workflow visibility record should exist while state is active",
                        ))
                    })?;
                let savepoint = self
                    .sandbox
                    .load_savepoint_record(&state_record.run_id)
                    .await?;
                let visible_history = self
                    .sandbox
                    .load_visible_history(&state_record.run_id)
                    .await?;
                (state_record, visibility_record, savepoint, visible_history)
            }
        };

        let state = decode_review_state(state_record.state.as_ref())?
            .expect("workflow state should remain present while inspection succeeds");
        Ok(Some(WorkflowDuetInspection {
            flavor,
            workflow_name: state_record.workflow_name,
            instance_id: state_record.instance_id,
            run_id: state_record.run_id.to_string(),
            target: render_target(&state_record.target),
            lifecycle: visibility_record.lifecycle.as_str().to_string(),
            status: state.stage.as_str().to_string(),
            attempt: state.attempt,
            waiting_for_callback: state.waiting_for_callback,
            deadline_millis: state.deadline_millis,
            latest_savepoint_id: savepoint.map(|record| record.savepoint_id.to_string()),
            visibility_summary: visibility_record.summary,
            visible_statuses: visible_statuses(&visible_history),
        }))
    }

    async fn relay_approval_if_pair_waiting(
        &self,
        target_flavor: WorkflowDuetFlavor,
        instance_id: &str,
    ) -> Result<bool, WorkflowDuetError> {
        let Some(state) = self.load_review_state(target_flavor, instance_id).await? else {
            return Ok(false);
        };
        if matches!(state.stage, ReviewStage::Approved | ReviewStage::TimedOut) {
            return Ok(false);
        }
        if state.stage == ReviewStage::WaitingApproval {
            self.approve(target_flavor, instance_id).await?;
            return Ok(true);
        }

        let state = self
            .wait_for_stage(
                target_flavor,
                instance_id,
                ReviewStage::WaitingApproval,
                demo_window_hint(),
            )
            .await?;
        if state.stage == ReviewStage::WaitingApproval {
            self.approve(target_flavor, instance_id).await?;
            return Ok(true);
        }
        Ok(false)
    }
}

pub struct WorkflowDuetHandles {
    native: WorkflowHandle,
    sandbox: WorkflowHandle,
    native_relay: WorkflowDuetRelayHandle,
    sandbox_relay: WorkflowDuetRelayHandle,
}

impl WorkflowDuetHandles {
    pub async fn shutdown(self) -> Result<(), WorkflowDuetError> {
        let (native_relay, sandbox_relay) =
            tokio::join!(self.native_relay.shutdown(), self.sandbox_relay.shutdown());
        native_relay?;
        sandbox_relay?;
        let (native_runtime, sandbox_runtime) =
            tokio::join!(self.native.shutdown(), self.sandbox.shutdown());
        native_runtime?;
        sandbox_runtime?;
        Ok(())
    }

    pub async fn abort(self) -> Result<(), WorkflowDuetError> {
        let (native_relay, sandbox_relay) =
            tokio::join!(self.native_relay.abort(), self.sandbox_relay.abort());
        native_relay?;
        sandbox_relay?;
        let (native_runtime, sandbox_runtime) =
            tokio::join!(self.native.abort(), self.sandbox.abort());
        native_runtime?;
        sandbox_runtime?;
        Ok(())
    }
}

struct WorkflowDuetRelayHandle {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), WorkflowDuetError>>,
}

impl WorkflowDuetRelayHandle {
    async fn shutdown(self) -> Result<(), WorkflowDuetError> {
        self.shutdown.send_replace(true);
        match self.task.await {
            Ok(result) => result,
            Err(error) => Err(WorkflowDuetError::RelayTask(error.to_string())),
        }
    }

    async fn abort(self) -> Result<(), WorkflowDuetError> {
        self.shutdown.send_replace(true);
        self.task.abort();
        match self.task.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(WorkflowDuetError::RelayTask(error.to_string())),
        }
    }
}

fn spawn_relay(app: WorkflowDuetApp, source_flavor: WorkflowDuetFlavor) -> WorkflowDuetRelayHandle {
    let (shutdown, shutdown_rx) = watch::channel(false);
    let outbox_table = source_outbox_table(&app, source_flavor);
    let db = app.db.clone();
    let clock = app.clock.clone();
    let task = tokio::spawn(async move {
        relay_cross_runtime_outbox(
            db,
            clock,
            outbox_table,
            app,
            source_flavor,
            source_flavor.counterpart(),
            shutdown_rx,
        )
        .await
    });
    WorkflowDuetRelayHandle { shutdown, task }
}

pub async fn run_local_demo(data_root: &str) -> Result<WorkflowDuetDemoReport, WorkflowDuetError> {
    let demo_timeout = Duration::from_secs(10);
    let clock = Arc::new(StubClock::new(Timestamp::new(0)));
    let rng = Arc::new(StubRng::seeded(0x7711));
    let db = Db::open(
        workflow_duet_db_config(
            &format!("/{data_root}/ssd"),
            &format!("workflow-duet/demo/{data_root}"),
        ),
        DbDependencies::new(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
            clock.clone(),
            rng.clone(),
        ),
    )
    .await?;
    let instance_id = format!("duet-demo-{}", rng.uuid());

    let app = WorkflowDuetApp::open(db, clock.clone(), rng).await?;
    let handles = app.start().await?;

    app.kick_off_pair(&instance_id).await?;
    clock.advance(Duration::from_millis(RETRY_DELAY_MILLIS + 2));

    let native = app.wait_for_stage(
        WorkflowDuetFlavor::Native,
        &instance_id,
        ReviewStage::Approved,
        demo_timeout,
    );
    let sandbox = app.wait_for_stage(
        WorkflowDuetFlavor::Sandbox,
        &instance_id,
        ReviewStage::Approved,
        demo_timeout,
    );
    let _ = tokio::try_join!(native, sandbox)?;

    let native = app
        .inspect_instance(WorkflowDuetFlavor::Native, &instance_id)
        .await?
        .expect("native demo run");
    let sandbox = app
        .inspect_instance(WorkflowDuetFlavor::Sandbox, &instance_id)
        .await?
        .expect("sandbox demo run");

    handles.shutdown().await?;
    Ok(WorkflowDuetDemoReport { native, sandbox })
}

async fn relay_cross_runtime_outbox(
    db: Db,
    clock: Arc<dyn Clock>,
    outbox_table: Table,
    app: WorkflowDuetApp,
    source_flavor: WorkflowDuetFlavor,
    target_flavor: WorkflowDuetFlavor,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), WorkflowDuetError> {
    let mut subscription = db.subscribe(&outbox_table);
    loop {
        let processed =
            relay_cross_runtime_batch(&db, &outbox_table, &app, source_flavor, target_flavor)
                .await?;
        if processed == 0 {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                changed = subscription.changed() => {
                    changed?;
                }
                _ = clock.sleep(RELAY_IDLE_POLL_INTERVAL) => {}
            }
        }
    }
}

async fn relay_cross_runtime_batch(
    db: &Db,
    outbox_table: &Table,
    app: &WorkflowDuetApp,
    source_flavor: WorkflowDuetFlavor,
    target_flavor: WorkflowDuetFlavor,
) -> Result<usize, WorkflowDuetError> {
    let mut rows = outbox_table
        .scan(
            Vec::new(),
            vec![0xff],
            ScanOptions {
                limit: Some(RELAY_BATCH_LIMIT),
                ..ScanOptions::default()
            },
        )
        .await?;

    let mut entries = Vec::new();
    while let Some((outbox_id, value)) = rows.next().await {
        let entry = decode_outbox_entry(outbox_id, &value)?;
        let message: ReviewOutboxMessage = serde_json::from_slice(&entry.payload)?;
        entries.push((entry.outbox_id, message));
    }

    if entries.is_empty() {
        return Ok(0);
    }

    for (_, message) in &entries {
        relay_outbox_message(app, source_flavor, target_flavor, message).await?;
    }

    let mut tx = Transaction::begin(db).await;
    for (outbox_id, _) in &entries {
        tx.delete(outbox_table, outbox_id.clone());
    }
    tx.commit().await?;
    Ok(entries.len())
}

async fn relay_outbox_message(
    app: &WorkflowDuetApp,
    source_flavor: WorkflowDuetFlavor,
    target_flavor: WorkflowDuetFlavor,
    message: &ReviewOutboxMessage,
) -> Result<(), WorkflowDuetError> {
    if message.workflow != source_flavor.workflow_name() {
        return Ok(());
    }
    if message.action != "requested-approval" {
        return Ok(());
    }

    let _ = app
        .relay_approval_if_pair_waiting(target_flavor, &message.instance_id)
        .await?;
    Ok(())
}

pub fn workflow_duet_db_settings(path: &str, prefix: &str) -> DbSettings {
    DbSettings::tiered_storage(TieredStorageConfig {
        ssd: terracedb::SsdConfig {
            path: path.to_string(),
        },
        s3: S3Location {
            bucket: "terracedb-example-workflow-duet".to_string(),
            prefix: prefix.to_string(),
        },
        max_local_bytes: 128 * 1024,
        durability: TieredDurabilityMode::GroupCommit,
        local_retention: terracedb::TieredLocalRetentionMode::Offload,
    })
}

pub fn workflow_duet_db_builder(path: &str, prefix: &str) -> DbBuilder {
    Db::builder().settings(workflow_duet_db_settings(path, prefix))
}

pub fn workflow_duet_db_config(path: &str, prefix: &str) -> DbConfig {
    DbConfig {
        storage: workflow_duet_db_settings(path, prefix).into_storage(),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

async fn open_sandbox_session(
    vfs: Arc<InMemoryVfsStore>,
    clock: Arc<dyn Clock>,
) -> Result<SandboxSession, WorkflowDuetError> {
    seed_sandbox_project(vfs.as_ref()).await?;
    let sandbox = DefaultSandboxStore::new(vfs, clock, SandboxServices::deterministic());
    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id: SESSION_VOLUME_ID,
            session_chunk_size: Some(4096),
            base_volume_id: BASE_VOLUME_ID,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmPureJs,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await?;
    install_sandbox_packages(&session).await?;
    emit_sandbox_workflow(&session).await?;
    Ok(session)
}

async fn seed_sandbox_project(vfs: &InMemoryVfsStore) -> Result<(), WorkflowDuetError> {
    let base = vfs
        .open_volume(
            VolumeConfig::new(BASE_VOLUME_ID)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await?;
    for (path, contents) in [
        (
            SANDBOX_PACKAGE_JSON_PATH,
            include_str!("../sandbox/package.json"),
        ),
        (
            SANDBOX_TSCONFIG_PATH,
            include_str!("../sandbox/tsconfig.json"),
        ),
        (
            SANDBOX_SOURCE_MODULE_PATH,
            include_str!("../sandbox/review_workflow.ts"),
        ),
    ] {
        base.fs()
            .write_file(
                path,
                contents.as_bytes().to_vec(),
                CreateOptions {
                    create_parents: true,
                    ..Default::default()
                },
            )
            .await?;
    }
    Ok(())
}

async fn install_sandbox_packages(session: &SandboxSession) -> Result<(), WorkflowDuetError> {
    let package_json =
        serde_json::from_str::<serde_json::Value>(include_str!("../sandbox/package.json"))?;
    let dependencies = package_json
        .get("dependencies")
        .and_then(|deps| deps.as_object())
        .map(|deps| deps.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    if dependencies.is_empty() {
        return Ok(());
    }
    session
        .install_packages(PackageInstallRequest {
            packages: dependencies,
            materialize_compatibility_view: true,
        })
        .await?;
    Ok(())
}

async fn emit_sandbox_workflow(session: &SandboxSession) -> Result<(), WorkflowDuetError> {
    session
        .emit_typescript(TypeCheckRequest {
            roots: vec![SANDBOX_SOURCE_MODULE_PATH.to_string()],
            ..Default::default()
        })
        .await?;
    Ok(())
}

fn render_target(target: &contracts::WorkflowExecutionTarget) -> String {
    match target {
        contracts::WorkflowExecutionTarget::Bundle { bundle_id } => bundle_id.to_string(),
        contracts::WorkflowExecutionTarget::NativeRegistration { registration_id } => {
            registration_id.to_string()
        }
    }
}

fn source_outbox_table(app: &WorkflowDuetApp, flavor: WorkflowDuetFlavor) -> Table {
    match flavor {
        WorkflowDuetFlavor::Native => app.native.tables().outbox_table().clone(),
        WorkflowDuetFlavor::Sandbox => app.sandbox.tables().outbox_table().clone(),
    }
}

fn visible_statuses(history: &[terracedb_workflows::WorkflowHistoryRecord]) -> Vec<String> {
    history
        .iter()
        .filter_map(|entry| match &entry.event {
            contracts::WorkflowHistoryEvent::VisibilityUpdated { record } => {
                record.summary.get("status").cloned()
            }
            _ => None,
        })
        .collect()
}

fn runtime_value_to_review_state(value: Option<&Value>) -> Option<ReviewState> {
    match value {
        Some(Value::Bytes(bytes)) => serde_json::from_slice(bytes).ok(),
        Some(Value::Record(_)) | None => None,
    }
}

#[allow(dead_code)]
fn demo_window_hint() -> Duration {
    Duration::from_millis(RETRY_DELAY_MILLIS + APPROVAL_TIMEOUT_MILLIS)
}
