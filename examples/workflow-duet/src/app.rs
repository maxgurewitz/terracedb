use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::{
    Clock, Db, DbBuilder, DbConfig, DbSettings, Rng, S3Location, SequenceNumber, StubClock,
    StubRng, TieredDurabilityMode, TieredStorageConfig, Timestamp, Value,
};
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, PackageCompatibilityMode, SandboxConfig, SandboxError,
    SandboxServices, SandboxSession, SandboxStore,
};
use terracedb_vfs::{
    CreateOptions, InMemoryVfsStore, VfsError, VolumeConfig, VolumeId, VolumeStore,
};
use terracedb_workflows::{
    ContractWorkflowHandler, WorkflowDefinition, WorkflowError, WorkflowHandle, WorkflowRuntime,
    contracts::{
        self, NativeWorkflowHandlerAdapter, WorkflowDescribeRequest, WorkflowHandlerContract,
        WorkflowHistoryEvent, WorkflowHistoryPageRequest, WorkflowTaskError,
        WorkflowVisibilityListRequest,
    },
};
use terracedb_workflows_sandbox::{
    SandboxModuleWorkflowTaskV1Handler, SandboxWorkflowHandlerAdapter,
};
use thiserror::Error;

use crate::model::{
    APPROVAL_TIMEOUT_MILLIS, APPROVE_CALLBACK_ID, NATIVE_WORKFLOW_NAME, RETRY_DELAY_MILLIS,
    ReviewStage, ReviewState, SANDBOX_MODULE_PATH, SANDBOX_WORKFLOW_NAME, START_CALLBACK_ID,
    decode_review_state, native_bundle, review_transition_output, sandbox_bundle,
};

const BASE_VOLUME_ID: VolumeId = VolumeId::new(0x7700);
const SESSION_VOLUME_ID: VolumeId = VolumeId::new(0x7701);
const WORKFLOW_TIMER_POLL_INTERVAL: Duration = Duration::from_millis(2);

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
    pub bundle_id: String,
    pub lifecycle: String,
    pub status: String,
    pub attempt: u32,
    pub waiting_for_callback: Option<String>,
    pub deadline_millis: Option<u64>,
    pub history_event_kinds: Vec<String>,
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

pub struct WorkflowDuetApp {
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
                    native_bundle(),
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
            db,
            clock,
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
            Ok(sandbox) => Ok(WorkflowDuetHandles { native, sandbox }),
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
        let state_record = match flavor {
            WorkflowDuetFlavor::Native => self.native.load_state_record(instance_id).await?,
            WorkflowDuetFlavor::Sandbox => self.sandbox.load_state_record(instance_id).await?,
        };
        let Some(state_record) = state_record else {
            return Ok(None);
        };

        let (describe, history) = match flavor {
            WorkflowDuetFlavor::Native => (
                self.native
                    .describe_run(WorkflowDescribeRequest {
                        run_id: state_record.run_id.clone(),
                    })
                    .await?,
                self.native
                    .load_run_history_page(WorkflowHistoryPageRequest {
                        run_id: state_record.run_id.clone(),
                        after_sequence: None,
                        limit: 128,
                    })
                    .await?,
            ),
            WorkflowDuetFlavor::Sandbox => (
                self.sandbox
                    .describe_run(WorkflowDescribeRequest {
                        run_id: state_record.run_id.clone(),
                    })
                    .await?,
                self.sandbox
                    .load_run_history_page(WorkflowHistoryPageRequest {
                        run_id: state_record.run_id.clone(),
                        after_sequence: None,
                        limit: 128,
                    })
                    .await?,
            ),
        };

        let state = decode_review_state(describe.state.state.as_ref())?
            .expect("workflow state should remain present while describe succeeds");
        Ok(Some(WorkflowDuetInspection {
            flavor,
            workflow_name: describe.state.workflow_name,
            instance_id: describe.state.instance_id,
            run_id: describe.state.run_id.to_string(),
            bundle_id: describe.state.bundle_id.to_string(),
            lifecycle: describe.state.lifecycle.as_str().to_string(),
            status: state.stage.as_str().to_string(),
            attempt: state.attempt,
            waiting_for_callback: state.waiting_for_callback,
            deadline_millis: state.deadline_millis,
            history_event_kinds: history
                .entries
                .into_iter()
                .map(|entry| history_event_kind(&entry.event))
                .collect(),
        }))
    }

    pub async fn list_runs(
        &self,
        flavor: WorkflowDuetFlavor,
    ) -> Result<contracts::WorkflowVisibilityListResponse, WorkflowDuetError> {
        let request = WorkflowVisibilityListRequest {
            workflow_name: Some(flavor.workflow_name().to_string()),
            lifecycle: None,
            deployment_id: None,
            target: None,
            page_size: 64,
            cursor: None,
        };
        match flavor {
            WorkflowDuetFlavor::Native => self.native.list_visibility(request).await,
            WorkflowDuetFlavor::Sandbox => self.sandbox.list_visibility(request).await,
        }
        .map_err(Into::into)
    }
}

pub struct WorkflowDuetHandles {
    native: WorkflowHandle,
    sandbox: WorkflowHandle,
}

impl WorkflowDuetHandles {
    pub async fn shutdown(self) -> Result<(), WorkflowError> {
        let native_result = self.native.shutdown().await;
        let sandbox_result = self.sandbox.shutdown().await;
        native_result?;
        sandbox_result
    }

    pub async fn abort(self) -> Result<(), WorkflowError> {
        let native_result = self.native.abort().await;
        let sandbox_result = self.sandbox.abort().await;
        native_result?;
        sandbox_result
    }
}

pub async fn run_local_demo(data_root: &str) -> Result<WorkflowDuetDemoReport, WorkflowDuetError> {
    let demo_timeout = Duration::from_secs(10);
    let clock = Arc::new(StubClock::new(Timestamp::new(0)));
    let rng = Arc::new(StubRng::seeded(0x7711));
    let ssd_path = format!("{data_root}/ssd");
    let object_store_root = format!("{data_root}/object-store");
    let db = workflow_duet_db_builder(&ssd_path, "workflow-duet/demo")
        .local_object_store(object_store_root)
        .clock(clock.clone())
        .rng(rng.clone())
        .open()
        .await?;

    let app = WorkflowDuetApp::open(db, clock.clone(), rng).await?;
    let handles = app.start().await?;

    app.kick_off(WorkflowDuetFlavor::Native, "native-demo")
        .await?;
    app.kick_off(WorkflowDuetFlavor::Sandbox, "sandbox-demo")
        .await?;
    clock.advance(Duration::from_millis(RETRY_DELAY_MILLIS + 2));

    let native = async {
        app.wait_for_stage(
            WorkflowDuetFlavor::Native,
            "native-demo",
            ReviewStage::WaitingApproval,
            demo_timeout,
        )
        .await?;
        app.approve(WorkflowDuetFlavor::Native, "native-demo")
            .await?;
        app.wait_for_stage(
            WorkflowDuetFlavor::Native,
            "native-demo",
            ReviewStage::Approved,
            demo_timeout,
        )
        .await
    };
    let sandbox = async {
        app.wait_for_stage(
            WorkflowDuetFlavor::Sandbox,
            "sandbox-demo",
            ReviewStage::WaitingApproval,
            demo_timeout,
        )
        .await?;
        app.approve(WorkflowDuetFlavor::Sandbox, "sandbox-demo")
            .await?;
        app.wait_for_stage(
            WorkflowDuetFlavor::Sandbox,
            "sandbox-demo",
            ReviewStage::Approved,
            demo_timeout,
        )
        .await
    };
    let _ = tokio::try_join!(native, sandbox)?;

    let native = app
        .inspect_instance(WorkflowDuetFlavor::Native, "native-demo")
        .await?
        .expect("native demo run");
    let sandbox = app
        .inspect_instance(WorkflowDuetFlavor::Sandbox, "sandbox-demo")
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
    seed_sandbox_module(vfs.as_ref()).await?;
    let sandbox = DefaultSandboxStore::new(vfs, clock, SandboxServices::deterministic());
    sandbox
        .open_session(SandboxConfig {
            session_volume_id: SESSION_VOLUME_ID,
            session_chunk_size: Some(4096),
            base_volume_id: BASE_VOLUME_ID,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .map_err(Into::into)
}

async fn seed_sandbox_module(vfs: &InMemoryVfsStore) -> Result<(), WorkflowDuetError> {
    let base = vfs
        .open_volume(
            VolumeConfig::new(BASE_VOLUME_ID)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await?;
    base.fs()
        .write_file(
            SANDBOX_MODULE_PATH,
            include_str!("../sandbox/review_workflow.js")
                .as_bytes()
                .to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

fn history_event_kind(event: &WorkflowHistoryEvent) -> String {
    match event {
        WorkflowHistoryEvent::RunCreated { .. } => "run-created",
        WorkflowHistoryEvent::TaskAdmitted { .. } => "task-admitted",
        WorkflowHistoryEvent::TaskApplied { .. } => "task-applied",
        WorkflowHistoryEvent::LifecycleChanged { .. } => "lifecycle-changed",
        WorkflowHistoryEvent::VisibilityUpdated { .. } => "visibility-updated",
        WorkflowHistoryEvent::ContinuedAsNew { .. } => "continued-as-new",
        WorkflowHistoryEvent::RunCompleted { .. } => "run-completed",
        WorkflowHistoryEvent::RunFailed { .. } => "run-failed",
    }
    .to_string()
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
