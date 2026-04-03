use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::{
    Clock, Db, DbBuilder, DbConfig, DbDependencies, DbSettings, ReadError, Rng, S3Location,
    SequenceNumber, StorageError, StubClock, StubFileSystem, StubObjectStore, StubRng,
    SubscriptionClosed, Table, TieredDurabilityMode, TieredStorageConfig, Timestamp,
    TransactionCommitError,
};
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, PackageCompatibilityMode, SandboxConfig, SandboxError,
    SandboxServices, SandboxSession, SandboxStore,
};
use terracedb_vfs::{
    CreateOptions, InMemoryVfsStore, VfsError, VolumeConfig, VolumeId, VolumeStore,
};
use terracedb_workflows::{
    ContractWorkflowHandler, WorkflowCallbackBridge, WorkflowDefinition, WorkflowError,
    WorkflowHandle, WorkflowRuntime,
    contracts::{self, NativeWorkflowHandlerAdapter, WorkflowHandlerContract, WorkflowTaskError},
};
use terracedb_workflows_sandbox::{
    SandboxModuleWorkflowTaskV1Handler, SandboxWorkflowHandlerAdapter,
};
use thiserror::Error;
use tokio::task::JoinHandle;

use crate::model::{
    NATIVE_WORKFLOW_NAME, RETRY_DELAY_MILLIS, ReviewStage, ReviewState, SANDBOX_MODULE_PATH,
    SANDBOX_PACKAGE_JSON_PATH, SANDBOX_TSCONFIG_PATH, SANDBOX_WORKFLOW_NAME, START_CALLBACK_ID,
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
            Ok(sandbox) => {
                let bridge = WorkflowCallbackBridge::new();
                bridge.register_target(NATIVE_WORKFLOW_NAME, Arc::new(self.native.clone()));
                bridge.register_target(SANDBOX_WORKFLOW_NAME, Arc::new(self.sandbox.clone()));
                Ok(WorkflowDuetHandles {
                    native,
                    sandbox,
                    native_bridge: bridge.spawn_drain_task(
                        self.db.clone(),
                        source_outbox_table(self, WorkflowDuetFlavor::Native),
                        RELAY_IDLE_POLL_INTERVAL,
                        RELAY_BATCH_LIMIT,
                    ),
                    sandbox_bridge: bridge.spawn_drain_task(
                        self.db.clone(),
                        source_outbox_table(self, WorkflowDuetFlavor::Sandbox),
                        RELAY_IDLE_POLL_INTERVAL,
                        RELAY_BATCH_LIMIT,
                    ),
                })
            }
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
        tokio::time::timeout(PAIR_START_TIMEOUT, async {
            tokio::try_join!(
                self.native
                    .wait_for_visible_status(instance_id, ReviewStage::RetryBackoff.as_str()),
                self.sandbox
                    .wait_for_visible_status(instance_id, ReviewStage::RetryBackoff.as_str()),
            )
        })
        .await
        .map_err(|_| WorkflowDuetError::WaitTimeout {
            flavor: WorkflowDuetFlavor::Sandbox,
            instance_id: instance_id.to_string(),
            expected: ReviewStage::RetryBackoff.as_str(),
        })??;
        Ok(())
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
            match flavor {
                WorkflowDuetFlavor::Native => {
                    self.native
                        .wait_for_visible_status(instance_id, expected_name)
                        .await?;
                }
                WorkflowDuetFlavor::Sandbox => {
                    self.sandbox
                        .wait_for_visible_status(instance_id, expected_name)
                        .await?;
                }
            }
            self.load_review_state(flavor, instance_id)
                .await?
                .ok_or_else(|| {
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
}

pub struct WorkflowDuetHandles {
    native: WorkflowHandle,
    sandbox: WorkflowHandle,
    native_bridge: JoinHandle<Result<(), WorkflowError>>,
    sandbox_bridge: JoinHandle<Result<(), WorkflowError>>,
}

impl WorkflowDuetHandles {
    pub async fn shutdown(self) -> Result<(), WorkflowDuetError> {
        self.native_bridge.abort();
        self.sandbox_bridge.abort();
        let (native_runtime, sandbox_runtime) =
            tokio::join!(self.native.shutdown(), self.sandbox.shutdown());
        native_runtime?;
        sandbox_runtime?;
        Ok(())
    }

    pub async fn abort(self) -> Result<(), WorkflowDuetError> {
        self.native_bridge.abort();
        self.sandbox_bridge.abort();
        let (native_runtime, sandbox_runtime) =
            tokio::join!(self.native.abort(), self.sandbox.abort());
        native_runtime?;
        sandbox_runtime?;
        Ok(())
    }
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
    let bundle = sandbox_bundle();
    let package_compat = match &bundle.kind {
        contracts::WorkflowBundleKind::Sandbox { preparation, .. } => {
            match preparation.package_compat() {
                contracts::WorkflowSandboxPackageCompatibility::TerraceOnly => {
                    PackageCompatibilityMode::TerraceOnly
                }
                contracts::WorkflowSandboxPackageCompatibility::NpmPureJs => {
                    PackageCompatibilityMode::NpmPureJs
                }
                contracts::WorkflowSandboxPackageCompatibility::NpmWithNodeBuiltins => {
                    PackageCompatibilityMode::NpmWithNodeBuiltins
                }
            }
        }
        _ => PackageCompatibilityMode::TerraceOnly,
    };
    let sandbox = DefaultSandboxStore::new(vfs, clock, SandboxServices::deterministic());
    sandbox
        .open_session(SandboxConfig {
            session_volume_id: SESSION_VOLUME_ID,
            session_chunk_size: Some(4096),
            base_volume_id: BASE_VOLUME_ID,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .map_err(Into::into)
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
            SANDBOX_MODULE_PATH,
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
