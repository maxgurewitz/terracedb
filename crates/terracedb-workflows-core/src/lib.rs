use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::{OperationContext, Timestamp};
use thiserror::Error;
use tokio::sync::RwLock;

pub const WORKFLOW_PUBLIC_CONTRACT_VERSION: u32 = 1;

macro_rules! workflow_id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, WorkflowTaskError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(WorkflowTaskError::invalid_contract(concat!(
                        stringify!($name),
                        " cannot be empty"
                    )));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

workflow_id_type!(WorkflowBundleId);
workflow_id_type!(WorkflowRegistrationId);
workflow_id_type!(WorkflowDeploymentId);
workflow_id_type!(WorkflowRunId);
workflow_id_type!(WorkflowTaskId);
workflow_id_type!(WorkflowQueryId);
workflow_id_type!(WorkflowUpdateId);
workflow_id_type!(WorkflowRecoverySegmentId);
workflow_id_type!(WorkflowSavepointId);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowPayload {
    pub encoding: String,
    pub bytes: Vec<u8>,
}

impl WorkflowPayload {
    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self::with_encoding("application/octet-stream", bytes)
    }

    pub fn with_encoding(encoding: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            encoding: encoding.into(),
            bytes: bytes.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowChangeKind {
    Put,
    Delete,
    Merge,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSourceEvent {
    pub source_table: String,
    pub key: Vec<u8>,
    pub value: Option<WorkflowPayload>,
    pub cursor: [u8; 16],
    pub sequence: u64,
    pub kind: WorkflowChangeKind,
    pub operation_context: Option<OperationContext>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowTrigger {
    Event {
        event: WorkflowSourceEvent,
    },
    Timer {
        timer_id: Vec<u8>,
        fire_at_millis: u64,
        payload: Vec<u8>,
    },
    Callback {
        callback_id: String,
        response: Vec<u8>,
    },
    Update {
        update_id: WorkflowUpdateId,
        lane: WorkflowUpdateLane,
        name: String,
        payload: Option<WorkflowPayload>,
        requested_at_millis: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowLifecycleState {
    Scheduled,
    Running,
    Suspended,
    Paused,
    RetryWaiting,
    Completed,
    Failed,
}

impl WorkflowLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::Paused => "paused",
            Self::RetryWaiting => "retry-waiting",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowSandboxSourceKind {
    JavaScript,
    TypeScript,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowSandboxPackageCompatibility {
    TerraceOnly,
    NpmPureJs,
    NpmWithNodeBuiltins,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkflowSandboxPreparation {
    #[serde(default)]
    pub source_kind: Option<WorkflowSandboxSourceKind>,
    #[serde(default)]
    pub package_compat: Option<WorkflowSandboxPackageCompatibility>,
    #[serde(default)]
    pub package_manifest_path: Option<String>,
    #[serde(default)]
    pub tsconfig_path: Option<String>,
}

impl WorkflowSandboxPreparation {
    pub fn source_kind(&self) -> WorkflowSandboxSourceKind {
        self.source_kind
            .clone()
            .unwrap_or(WorkflowSandboxSourceKind::JavaScript)
    }

    pub fn package_compat(&self) -> WorkflowSandboxPackageCompatibility {
        self.package_compat
            .unwrap_or(WorkflowSandboxPackageCompatibility::TerraceOnly)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowBundleKind {
    NativeRust {
        registration: String,
    },
    Sandbox {
        abi: String,
        module: String,
        entrypoint: String,
        #[serde(default)]
        preparation: WorkflowSandboxPreparation,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowBundleMetadata {
    pub bundle_id: WorkflowBundleId,
    pub workflow_name: String,
    pub kind: WorkflowBundleKind,
    pub created_at_millis: u64,
    pub labels: BTreeMap<String, String>,
}

impl WorkflowBundleMetadata {
    pub fn target(&self) -> WorkflowExecutionTarget {
        WorkflowExecutionTarget::Bundle {
            bundle_id: self.bundle_id.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowExecutionTarget {
    Bundle {
        bundle_id: WorkflowBundleId,
    },
    NativeRegistration {
        registration_id: WorkflowRegistrationId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowNativeRegistrationMetadata {
    pub registration_id: WorkflowRegistrationId,
    pub workflow_name: String,
    pub registration: String,
    pub registered_at_millis: u64,
    pub labels: BTreeMap<String, String>,
}

impl WorkflowNativeRegistrationMetadata {
    pub fn target(&self) -> WorkflowExecutionTarget {
        WorkflowExecutionTarget::NativeRegistration {
            registration_id: self.registration_id.clone(),
        }
    }
}

impl From<WorkflowBundleMetadata> for WorkflowExecutionDescriptor {
    fn from(bundle: WorkflowBundleMetadata) -> Self {
        Self::Bundle { bundle }
    }
}

impl From<WorkflowNativeRegistrationMetadata> for WorkflowExecutionDescriptor {
    fn from(registration: WorkflowNativeRegistrationMetadata) -> Self {
        Self::NativeRegistration { registration }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowExecutionDescriptor {
    Bundle {
        bundle: WorkflowBundleMetadata,
    },
    NativeRegistration {
        registration: WorkflowNativeRegistrationMetadata,
    },
}

impl WorkflowExecutionDescriptor {
    pub fn target(&self) -> WorkflowExecutionTarget {
        match self {
            Self::Bundle { bundle } => bundle.target(),
            Self::NativeRegistration { registration } => registration.target(),
        }
    }

    pub fn workflow_name(&self) -> &str {
        match self {
            Self::Bundle { bundle } => &bundle.workflow_name,
            Self::NativeRegistration { registration } => &registration.workflow_name,
        }
    }

    pub fn labels(&self) -> &BTreeMap<String, String> {
        match self {
            Self::Bundle { bundle } => &bundle.labels,
            Self::NativeRegistration { registration } => &registration.labels,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowDeploymentEnvironment {
    Preview,
    Production,
}

impl WorkflowDeploymentEnvironment {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preview => "preview",
            Self::Production => "production",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowRolloutPolicy {
    #[default]
    Immediate,
    Manual,
    ByInstanceHash {
        numerator: u32,
        denominator: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDeploymentRecord {
    pub deployment_id: WorkflowDeploymentId,
    pub workflow_name: String,
    pub environment: WorkflowDeploymentEnvironment,
    pub target: WorkflowExecutionTarget,
    pub rollout: WorkflowRolloutPolicy,
    pub created_at_millis: u64,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDeploymentActivation {
    pub workflow_name: String,
    pub environment: WorkflowDeploymentEnvironment,
    pub active_deployment_id: Option<WorkflowDeploymentId>,
    pub active_target: Option<WorkflowExecutionTarget>,
    pub rollout: Option<WorkflowRolloutPolicy>,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRunAssignment {
    pub workflow_name: String,
    pub environment: WorkflowDeploymentEnvironment,
    pub deployment_id: Option<WorkflowDeploymentId>,
    pub target: WorkflowExecutionTarget,
    pub pinned_epoch: u32,
    pub assigned_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionEpoch {
    pub run_id: WorkflowRunId,
    pub workflow_name: String,
    pub environment: WorkflowDeploymentEnvironment,
    pub deployment_id: Option<WorkflowDeploymentId>,
    pub target: WorkflowExecutionTarget,
    pub pinned_epoch: u32,
    pub pinned_at_millis: u64,
}

impl WorkflowExecutionEpoch {
    pub fn from_assignment(
        run_id: WorkflowRunId,
        assignment: WorkflowRunAssignment,
        pinned_at_millis: u64,
    ) -> Self {
        Self {
            run_id,
            workflow_name: assignment.workflow_name,
            environment: assignment.environment,
            deployment_id: assignment.deployment_id,
            target: assignment.target,
            pinned_epoch: assignment.pinned_epoch,
            pinned_at_millis,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDeploymentPreviewRequest {
    pub deployment: WorkflowDeploymentRecord,
    pub instance_id: String,
    pub labels: BTreeMap<String, String>,
    pub previewed_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDeploymentResolutionRequest {
    pub workflow_name: String,
    pub environment: WorkflowDeploymentEnvironment,
    pub instance_id: String,
    pub labels: BTreeMap<String, String>,
    pub requested_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDeploymentActivationRequest {
    pub deployment_id: WorkflowDeploymentId,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDeploymentDeactivationRequest {
    pub deployment_id: WorkflowDeploymentId,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDeploymentRollbackRequest {
    pub workflow_name: String,
    pub environment: WorkflowDeploymentEnvironment,
    pub deployment_id: WorkflowDeploymentId,
    pub updated_at_millis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowUpgradeTransition {
    ContinueAsNew,
    RestartAsNew,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowUpgradeRequest {
    pub current: WorkflowExecutionEpoch,
    pub candidate: WorkflowRunAssignment,
    pub requested_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowUpgradePlan {
    pub transition: WorkflowUpgradeTransition,
    pub current: WorkflowExecutionEpoch,
    pub next: WorkflowRunAssignment,
    pub planned_at_millis: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowUpgradePolicy {
    pub allow_continue_as_new: bool,
    pub allow_restart_as_new: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCompatibilityManifest {
    pub target: WorkflowExecutionTarget,
    pub workflow_name: String,
    pub runtime_surface: Option<String>,
    pub compatible_predecessors: Vec<WorkflowExecutionTarget>,
    pub upgrade_policy: WorkflowUpgradePolicy,
    pub created_at_millis: u64,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCompatibilityCheck {
    pub workflow_name: String,
    pub current: Option<WorkflowExecutionTarget>,
    pub candidate: WorkflowExecutionTarget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowCompatibilityDisposition {
    Compatible,
    RequiresContinueAsNew,
    RequiresRestartAsNew,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCompatibilityDecision {
    pub disposition: WorkflowCompatibilityDisposition,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowLifecycleRecord {
    pub run_id: WorkflowRunId,
    pub lifecycle: WorkflowLifecycleState,
    pub updated_at_millis: u64,
    pub reason: Option<String>,
    pub task_id: Option<WorkflowTaskId>,
    pub attempt: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowCheckpointKind {
    InternalSavepoint,
    Export,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRunRecord {
    pub run_id: WorkflowRunId,
    pub workflow_name: String,
    pub instance_id: String,
    pub lifecycle: WorkflowLifecycleState,
    pub execution: WorkflowExecutionEpoch,
    pub current_task_id: Option<WorkflowTaskId>,
    pub active_recovery_segment_id: Option<WorkflowRecoverySegmentId>,
    pub latest_savepoint_id: Option<WorkflowSavepointId>,
    pub visible_history_len: u64,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStateRecord {
    pub run_id: WorkflowRunId,
    pub target: WorkflowExecutionTarget,
    pub workflow_name: String,
    pub instance_id: String,
    pub lifecycle: WorkflowLifecycleState,
    pub current_task_id: Option<WorkflowTaskId>,
    pub history_len: u64,
    pub state: Option<WorkflowPayload>,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowRecoveryJournalEntry {
    TriggerAdmitted {
        task_id: WorkflowTaskId,
        trigger: WorkflowTrigger,
        admitted_at_millis: u64,
    },
    TransitionApplied {
        task_id: WorkflowTaskId,
        output: WorkflowTransitionOutput,
        committed_at_millis: u64,
    },
    LifecycleChanged {
        record: WorkflowLifecycleRecord,
    },
    SavepointCreated {
        savepoint_id: WorkflowSavepointId,
        covered_through_sequence: u64,
        created_at_millis: u64,
    },
    SegmentSealed {
        segment_id: WorkflowRecoverySegmentId,
        sealed_at_millis: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRecoveryJournalRecord {
    pub run_id: WorkflowRunId,
    pub segment_id: WorkflowRecoverySegmentId,
    pub sequence: u64,
    pub entry: WorkflowRecoveryJournalEntry,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSavepointRecord {
    pub run_id: WorkflowRunId,
    pub savepoint_id: WorkflowSavepointId,
    pub checkpoint_kind: WorkflowCheckpointKind,
    pub covering_segment_id: WorkflowRecoverySegmentId,
    pub covered_through_sequence: u64,
    pub state: Option<WorkflowPayload>,
    pub created_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkflowVisibilityUpdate {
    pub summary: BTreeMap<String, String>,
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVisibilityRecord {
    pub run_id: WorkflowRunId,
    pub target: WorkflowExecutionTarget,
    pub workflow_name: String,
    pub instance_id: String,
    pub lifecycle: WorkflowLifecycleState,
    pub last_task_id: Option<WorkflowTaskId>,
    pub visible_history_len: u64,
    pub summary: BTreeMap<String, String>,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVisibilityEntry {
    pub record: WorkflowVisibilityRecord,
    pub execution: Option<WorkflowExecutionEpoch>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVisibilityListRequest {
    pub workflow_name: Option<String>,
    pub lifecycle: Option<WorkflowLifecycleState>,
    pub deployment_id: Option<WorkflowDeploymentId>,
    pub target: Option<WorkflowExecutionTarget>,
    pub page_size: u32,
    pub cursor: Option<WorkflowRunId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVisibilityListResponse {
    pub entries: Vec<WorkflowVisibilityEntry>,
    pub next_cursor: Option<WorkflowRunId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDescribeRequest {
    pub run_id: WorkflowRunId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDescribeResponse {
    pub state: WorkflowStateRecord,
    pub lifecycle: Option<WorkflowLifecycleRecord>,
    pub visibility: WorkflowVisibilityEntry,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVisibleHistoryPageRequest {
    pub run_id: WorkflowRunId,
    pub after_sequence: Option<u64>,
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVisibleHistoryEntry {
    pub sequence: u64,
    pub lifecycle: WorkflowLifecycleState,
    pub task_id: Option<WorkflowTaskId>,
    pub summary: BTreeMap<String, String>,
    pub note: Option<String>,
    pub recorded_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVisibleHistoryPageResponse {
    pub entries: Vec<WorkflowVisibleHistoryEntry>,
    pub next_after_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowQueryTarget {
    State,
    Visibility,
    Execution,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowQueryRequest {
    pub query_id: WorkflowQueryId,
    pub run_id: WorkflowRunId,
    pub target: WorkflowQueryTarget,
    pub name: String,
    pub payload: Option<WorkflowPayload>,
    pub requested_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowQueryResponse {
    Accepted {
        query_id: WorkflowQueryId,
        payload: Option<WorkflowPayload>,
        answered_at_millis: u64,
    },
    Rejected {
        query_id: WorkflowQueryId,
        error: WorkflowTaskError,
        answered_at_millis: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowUpdateLane {
    Update,
    Control,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowUpdateRequest {
    pub update_id: WorkflowUpdateId,
    pub run_id: WorkflowRunId,
    pub lane: WorkflowUpdateLane,
    pub name: String,
    pub payload: Option<WorkflowPayload>,
    pub requested_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowUpdateAdmission {
    Accepted {
        update_id: WorkflowUpdateId,
        task_id: WorkflowTaskId,
    },
    Rejected {
        update_id: WorkflowUpdateId,
        error: WorkflowTaskError,
    },
    Duplicate {
        update_id: WorkflowUpdateId,
        original_task_id: WorkflowTaskId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowStateMutation {
    Unchanged,
    Put { state: WorkflowPayload },
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowOutboxCommand {
    pub outbox_id: Vec<u8>,
    pub idempotency_key: String,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCallbackDeliveryCommand {
    pub target_workflow: String,
    pub instance_id: String,
    pub callback_id: String,
    pub response: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowTimerCommand {
    Schedule {
        timer_id: Vec<u8>,
        fire_at_millis: u64,
        payload: Vec<u8>,
    },
    Cancel {
        timer_id: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowCommand {
    Outbox {
        entry: WorkflowOutboxCommand,
    },
    Timer {
        command: WorkflowTimerCommand,
    },
    DeliverCallback {
        delivery: WorkflowCallbackDeliveryCommand,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowContinueAsNew {
    pub next_run_id: WorkflowRunId,
    pub next_target: WorkflowExecutionTarget,
    pub state: Option<WorkflowPayload>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTransitionInput {
    pub run_id: WorkflowRunId,
    pub target: WorkflowExecutionTarget,
    pub task_id: WorkflowTaskId,
    pub workflow_name: String,
    pub instance_id: String,
    pub lifecycle: WorkflowLifecycleState,
    pub history_len: u64,
    pub attempt: u32,
    pub admitted_at_millis: u64,
    pub state: Option<WorkflowPayload>,
    pub trigger: WorkflowTrigger,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTransitionOutput {
    pub state: WorkflowStateMutation,
    pub lifecycle: Option<WorkflowLifecycleState>,
    pub visibility: Option<WorkflowVisibilityUpdate>,
    pub continue_as_new: Option<WorkflowContinueAsNew>,
    pub commands: Vec<WorkflowCommand>,
}

impl Default for WorkflowTransitionOutput {
    fn default() -> Self {
        Self {
            state: WorkflowStateMutation::Unchanged,
            lifecycle: None,
            visibility: None,
            continue_as_new: None,
            commands: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowHistoryEvent {
    RunCreated {
        run_id: WorkflowRunId,
        target: WorkflowExecutionTarget,
        instance_id: String,
        scheduled_at_millis: u64,
    },
    TaskAdmitted {
        task_id: WorkflowTaskId,
        trigger: WorkflowTrigger,
        admitted_at_millis: u64,
    },
    TaskApplied {
        task_id: WorkflowTaskId,
        output: WorkflowTransitionOutput,
        committed_at_millis: u64,
    },
    LifecycleChanged {
        record: WorkflowLifecycleRecord,
    },
    VisibilityUpdated {
        record: WorkflowVisibilityRecord,
    },
    ContinuedAsNew {
        from_run_id: WorkflowRunId,
        next_run_id: WorkflowRunId,
        next_target: WorkflowExecutionTarget,
        continued_at_millis: u64,
    },
    RunCompleted {
        run_id: WorkflowRunId,
        completed_at_millis: u64,
    },
    RunFailed {
        run_id: WorkflowRunId,
        failed_at_millis: u64,
        error_code: String,
        error_message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDeterministicSeed {
    pub workflow_name: String,
    pub instance_id: String,
    pub run_id: WorkflowRunId,
    pub task_id: WorkflowTaskId,
    pub trigger_hash: u64,
    pub state_hash: u64,
}

impl WorkflowDeterministicSeed {
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn run_id(&self) -> &WorkflowRunId {
        &self.run_id
    }

    pub fn task_id(&self) -> &WorkflowTaskId {
        &self.task_id
    }

    pub fn stable_id(&self, scope: &str) -> String {
        format!("{scope}:{:016x}", self.scope_hash(scope))
    }

    pub fn stable_time(&self, scope: &str) -> Timestamp {
        Timestamp::new(self.scope_hash(scope))
    }

    fn scope_hash(&self, scope: &str) -> u64 {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(self.workflow_name.as_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(self.instance_id.as_bytes());
        bytes.push(0xfe);
        bytes.extend_from_slice(self.run_id.as_str().as_bytes());
        bytes.push(0xfd);
        bytes.extend_from_slice(self.task_id.as_str().as_bytes());
        bytes.push(0xfc);
        bytes.extend_from_slice(scope.as_bytes());
        bytes.extend_from_slice(&self.trigger_hash.to_be_bytes());
        bytes.extend_from_slice(&self.state_hash.to_be_bytes());
        stable_hash(&bytes)
    }
}

#[derive(Clone)]
pub struct WorkflowDeterministicContext {
    seed: WorkflowDeterministicSeed,
    observability: Arc<dyn WorkflowObservability>,
}

impl WorkflowDeterministicContext {
    pub fn new(
        input: &WorkflowTransitionInput,
        observability: Arc<dyn WorkflowObservability>,
    ) -> Result<Self, WorkflowTaskError> {
        let trigger_hash = stable_hash(
            &serde_json::to_vec(&input.trigger)
                .map_err(|error| WorkflowTaskError::serialization("workflow trigger", error))?,
        );
        let state_hash = stable_hash(
            &serde_json::to_vec(&input.state)
                .map_err(|error| WorkflowTaskError::serialization("workflow state", error))?,
        );
        Ok(Self {
            seed: WorkflowDeterministicSeed {
                workflow_name: input.workflow_name.clone(),
                instance_id: input.instance_id.clone(),
                run_id: input.run_id.clone(),
                task_id: input.task_id.clone(),
                trigger_hash,
                state_hash,
            },
            observability,
        })
    }

    pub fn from_seed(
        seed: WorkflowDeterministicSeed,
        observability: Arc<dyn WorkflowObservability>,
    ) -> Self {
        Self {
            seed,
            observability,
        }
    }

    pub fn seed(&self) -> WorkflowDeterministicSeed {
        self.seed.clone()
    }

    pub fn workflow_name(&self) -> &str {
        self.seed.workflow_name()
    }

    pub fn instance_id(&self) -> &str {
        self.seed.instance_id()
    }

    pub fn run_id(&self) -> &WorkflowRunId {
        self.seed.run_id()
    }

    pub fn task_id(&self) -> &WorkflowTaskId {
        self.seed.task_id()
    }

    pub fn stable_id(&self, scope: &str) -> String {
        self.seed.stable_id(scope)
    }

    pub fn stable_time(&self, scope: &str) -> Timestamp {
        self.seed.stable_time(scope)
    }

    pub fn observability(&self) -> &dyn WorkflowObservability {
        self.observability.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkflowObservationValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowObservationEvent {
    pub name: String,
    pub fields: BTreeMap<String, WorkflowObservationValue>,
}

pub trait WorkflowObservability: Send + Sync {
    fn record_attribute(&self, key: &str, value: WorkflowObservationValue);
    fn record_event(&self, event: WorkflowObservationEvent);
}

#[derive(Debug, Default)]
pub struct NoopWorkflowObservability;

impl WorkflowObservability for NoopWorkflowObservability {
    fn record_attribute(&self, _key: &str, _value: WorkflowObservationValue) {}

    fn record_event(&self, _event: WorkflowObservationEvent) {}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{code}: {message}")]
pub struct WorkflowTaskError {
    pub code: String,
    pub message: String,
}

impl WorkflowTaskError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn invalid_contract(message: impl Into<String>) -> Self {
        Self::new("invalid-contract", message)
    }

    pub fn serialization(what: &str, error: serde_json::Error) -> Self {
        Self::new(
            "serialization",
            format!("failed to serialize {what}: {error}"),
        )
    }
}

#[async_trait]
pub trait WorkflowHandlerContract: Send + Sync {
    async fn route_event(&self, event: &WorkflowSourceEvent) -> Result<String, WorkflowTaskError>;

    async fn handle_task(
        &self,
        input: WorkflowTransitionInput,
        ctx: WorkflowDeterministicContext,
    ) -> Result<WorkflowTransitionOutput, WorkflowTaskError>;
}

pub type SharedWorkflowHandler = Arc<dyn WorkflowHandlerContract>;

#[derive(Clone, Debug)]
pub struct NativeWorkflowHandlerAdapter<H> {
    inner: H,
}

impl<H> NativeWorkflowHandlerAdapter<H> {
    pub fn new(inner: H) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> H {
        self.inner
    }
}

#[async_trait]
impl<H> WorkflowHandlerContract for NativeWorkflowHandlerAdapter<H>
where
    H: WorkflowHandlerContract,
{
    async fn route_event(&self, event: &WorkflowSourceEvent) -> Result<String, WorkflowTaskError> {
        self.inner.route_event(event).await
    }

    async fn handle_task(
        &self,
        input: WorkflowTransitionInput,
        ctx: WorkflowDeterministicContext,
    ) -> Result<WorkflowTransitionOutput, WorkflowTaskError> {
        self.inner.handle_task(input, ctx).await
    }
}

pub trait WorkflowBundleResolver: Send + Sync {
    fn resolve_bundle(
        &self,
        bundle_id: &WorkflowBundleId,
    ) -> Result<WorkflowBundleMetadata, WorkflowTaskError>;
}

#[derive(Clone, Debug, Default)]
pub struct StaticWorkflowBundleResolver {
    bundles: BTreeMap<WorkflowBundleId, WorkflowBundleMetadata>,
}

impl StaticWorkflowBundleResolver {
    pub fn new(bundles: impl IntoIterator<Item = WorkflowBundleMetadata>) -> Self {
        let bundles = bundles
            .into_iter()
            .map(|bundle| (bundle.bundle_id.clone(), bundle))
            .collect();
        Self { bundles }
    }
}

impl WorkflowBundleResolver for StaticWorkflowBundleResolver {
    fn resolve_bundle(
        &self,
        bundle_id: &WorkflowBundleId,
    ) -> Result<WorkflowBundleMetadata, WorkflowTaskError> {
        self.bundles.get(bundle_id).cloned().ok_or_else(|| {
            WorkflowTaskError::new("missing-bundle", format!("unknown bundle {bundle_id}"))
        })
    }
}

pub trait WorkflowExecutionResolver: Send + Sync {
    fn resolve_target(
        &self,
        target: &WorkflowExecutionTarget,
    ) -> Result<WorkflowExecutionDescriptor, WorkflowTaskError>;
}

#[derive(Clone, Debug, Default)]
pub struct StaticWorkflowExecutionResolver {
    bundles: BTreeMap<WorkflowBundleId, WorkflowBundleMetadata>,
    registrations: BTreeMap<WorkflowRegistrationId, WorkflowNativeRegistrationMetadata>,
}

impl StaticWorkflowExecutionResolver {
    pub fn new(
        bundles: impl IntoIterator<Item = WorkflowBundleMetadata>,
        registrations: impl IntoIterator<Item = WorkflowNativeRegistrationMetadata>,
    ) -> Self {
        let bundles = bundles
            .into_iter()
            .map(|bundle| (bundle.bundle_id.clone(), bundle))
            .collect();
        let registrations = registrations
            .into_iter()
            .map(|registration| (registration.registration_id.clone(), registration))
            .collect();
        Self {
            bundles,
            registrations,
        }
    }
}

impl WorkflowBundleResolver for StaticWorkflowExecutionResolver {
    fn resolve_bundle(
        &self,
        bundle_id: &WorkflowBundleId,
    ) -> Result<WorkflowBundleMetadata, WorkflowTaskError> {
        self.bundles.get(bundle_id).cloned().ok_or_else(|| {
            WorkflowTaskError::new("missing-bundle", format!("unknown bundle {bundle_id}"))
        })
    }
}

impl WorkflowExecutionResolver for StaticWorkflowExecutionResolver {
    fn resolve_target(
        &self,
        target: &WorkflowExecutionTarget,
    ) -> Result<WorkflowExecutionDescriptor, WorkflowTaskError> {
        match target {
            WorkflowExecutionTarget::Bundle { bundle_id } => {
                let bundle = self.resolve_bundle(bundle_id)?;
                Ok(WorkflowExecutionDescriptor::Bundle { bundle })
            }
            WorkflowExecutionTarget::NativeRegistration { registration_id } => self
                .registrations
                .get(registration_id)
                .cloned()
                .map(
                    |registration| WorkflowExecutionDescriptor::NativeRegistration { registration },
                )
                .ok_or_else(|| {
                    WorkflowTaskError::new(
                        "missing-registration",
                        format!("unknown registration {registration_id}"),
                    )
                }),
        }
    }
}

#[async_trait]
pub trait WorkflowVisibilityApi: Send + Sync {
    async fn list(
        &self,
        request: WorkflowVisibilityListRequest,
    ) -> Result<WorkflowVisibilityListResponse, WorkflowTaskError>;

    async fn describe(
        &self,
        request: WorkflowDescribeRequest,
    ) -> Result<WorkflowDescribeResponse, WorkflowTaskError>;

    async fn visible_history(
        &self,
        request: WorkflowVisibleHistoryPageRequest,
    ) -> Result<WorkflowVisibleHistoryPageResponse, WorkflowTaskError>;
}

#[derive(Clone, Debug, Default)]
pub struct StaticWorkflowVisibilityApi {
    descriptions: BTreeMap<WorkflowRunId, WorkflowDescribeResponse>,
    visible_history_by_run: BTreeMap<WorkflowRunId, Vec<WorkflowVisibleHistoryEntry>>,
}

impl StaticWorkflowVisibilityApi {
    pub fn new(
        descriptions: impl IntoIterator<Item = WorkflowDescribeResponse>,
        visible_history_by_run: impl IntoIterator<
            Item = (WorkflowRunId, Vec<WorkflowVisibleHistoryEntry>),
        >,
    ) -> Self {
        let descriptions = descriptions
            .into_iter()
            .map(|description| (description.state.run_id.clone(), description))
            .collect();
        let visible_history_by_run = visible_history_by_run.into_iter().collect();
        Self {
            descriptions,
            visible_history_by_run,
        }
    }
}

#[async_trait]
impl WorkflowVisibilityApi for StaticWorkflowVisibilityApi {
    async fn list(
        &self,
        request: WorkflowVisibilityListRequest,
    ) -> Result<WorkflowVisibilityListResponse, WorkflowTaskError> {
        let mut entries = Vec::<WorkflowVisibilityEntry>::new();
        let mut next_cursor = None;
        let limit = if request.page_size == 0 {
            usize::MAX
        } else {
            request.page_size as usize
        };

        for (run_id, description) in &self.descriptions {
            if request
                .cursor
                .as_ref()
                .is_some_and(|cursor| run_id <= cursor)
            {
                continue;
            }
            if request
                .workflow_name
                .as_ref()
                .is_some_and(|name| description.state.workflow_name != *name)
            {
                continue;
            }
            if request
                .lifecycle
                .is_some_and(|lifecycle| description.visibility.record.lifecycle != lifecycle)
            {
                continue;
            }
            if request.deployment_id.as_ref().is_some_and(|deployment_id| {
                description
                    .visibility
                    .execution
                    .as_ref()
                    .and_then(|execution| execution.deployment_id.as_ref())
                    != Some(deployment_id)
            }) {
                continue;
            }
            if request.target.as_ref().is_some_and(|target| {
                description
                    .visibility
                    .execution
                    .as_ref()
                    .map(|execution| &execution.target)
                    != Some(target)
            }) {
                continue;
            }

            if entries.len() == limit {
                next_cursor = entries.last().map(|entry| entry.record.run_id.clone());
                break;
            }
            entries.push(description.visibility.clone());
        }

        Ok(WorkflowVisibilityListResponse {
            entries,
            next_cursor,
        })
    }

    async fn describe(
        &self,
        request: WorkflowDescribeRequest,
    ) -> Result<WorkflowDescribeResponse, WorkflowTaskError> {
        self.descriptions
            .get(&request.run_id)
            .cloned()
            .ok_or_else(|| {
                WorkflowTaskError::new(
                    "missing-run",
                    format!("unknown workflow run {}", request.run_id),
                )
            })
    }

    async fn visible_history(
        &self,
        request: WorkflowVisibleHistoryPageRequest,
    ) -> Result<WorkflowVisibleHistoryPageResponse, WorkflowTaskError> {
        let entries = self
            .visible_history_by_run
            .get(&request.run_id)
            .cloned()
            .unwrap_or_default();
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit as usize
        };

        let mut page = Vec::<WorkflowVisibleHistoryEntry>::new();
        let mut next_after_sequence = None;
        for entry in entries {
            if request
                .after_sequence
                .is_some_and(|after_sequence| entry.sequence <= after_sequence)
            {
                continue;
            }
            if page.len() == limit {
                next_after_sequence = page.last().map(|entry| entry.sequence);
                break;
            }
            page.push(entry);
        }

        Ok(WorkflowVisibleHistoryPageResponse {
            entries: page,
            next_after_sequence,
        })
    }
}

#[async_trait]
pub trait WorkflowInteractionApi: Send + Sync {
    async fn query(
        &self,
        request: WorkflowQueryRequest,
    ) -> Result<WorkflowQueryResponse, WorkflowTaskError>;

    async fn admit_update(
        &self,
        request: WorkflowUpdateRequest,
    ) -> Result<WorkflowUpdateAdmission, WorkflowTaskError>;
}

#[derive(Clone, Debug, Default)]
pub struct StaticWorkflowInteractionApi {
    queries: BTreeMap<WorkflowQueryId, WorkflowQueryResponse>,
    updates: BTreeMap<WorkflowUpdateId, WorkflowUpdateAdmission>,
}

impl StaticWorkflowInteractionApi {
    pub fn new(
        queries: impl IntoIterator<Item = (WorkflowQueryId, WorkflowQueryResponse)>,
        updates: impl IntoIterator<Item = (WorkflowUpdateId, WorkflowUpdateAdmission)>,
    ) -> Self {
        Self {
            queries: queries.into_iter().collect(),
            updates: updates.into_iter().collect(),
        }
    }
}

#[async_trait]
impl WorkflowInteractionApi for StaticWorkflowInteractionApi {
    async fn query(
        &self,
        request: WorkflowQueryRequest,
    ) -> Result<WorkflowQueryResponse, WorkflowTaskError> {
        self.queries.get(&request.query_id).cloned().ok_or_else(|| {
            WorkflowTaskError::new(
                "missing-query",
                format!("unknown workflow query {}", request.query_id),
            )
        })
    }

    async fn admit_update(
        &self,
        request: WorkflowUpdateRequest,
    ) -> Result<WorkflowUpdateAdmission, WorkflowTaskError> {
        self.updates
            .get(&request.update_id)
            .cloned()
            .ok_or_else(|| {
                WorkflowTaskError::new(
                    "missing-update",
                    format!("unknown workflow update {}", request.update_id),
                )
            })
    }
}

#[async_trait]
pub trait WorkflowCompatibilityChecker: Send + Sync {
    async fn evaluate(
        &self,
        check: WorkflowCompatibilityCheck,
    ) -> Result<WorkflowCompatibilityDecision, WorkflowTaskError>;
}

#[derive(Clone, Debug, Default)]
pub struct StaticWorkflowCompatibilityChecker {
    manifests: BTreeMap<WorkflowExecutionTarget, WorkflowCompatibilityManifest>,
}

impl StaticWorkflowCompatibilityChecker {
    pub fn new(manifests: impl IntoIterator<Item = WorkflowCompatibilityManifest>) -> Self {
        let manifests = manifests
            .into_iter()
            .map(|manifest| (manifest.target.clone(), manifest))
            .collect();
        Self { manifests }
    }
}

#[async_trait]
impl WorkflowCompatibilityChecker for StaticWorkflowCompatibilityChecker {
    async fn evaluate(
        &self,
        check: WorkflowCompatibilityCheck,
    ) -> Result<WorkflowCompatibilityDecision, WorkflowTaskError> {
        let manifest = self.manifests.get(&check.candidate).ok_or_else(|| {
            WorkflowTaskError::new(
                "missing-compatibility-manifest",
                format!("unknown compatibility target {:?}", check.candidate),
            )
        })?;
        if manifest.workflow_name != check.workflow_name {
            return Err(WorkflowTaskError::invalid_contract(
                "compatibility target workflow name does not match request",
            ));
        }

        let disposition = match check.current {
            None => WorkflowCompatibilityDisposition::Compatible,
            Some(current) if current == check.candidate => {
                WorkflowCompatibilityDisposition::Compatible
            }
            Some(current) if manifest.compatible_predecessors.contains(&current) => {
                if manifest.upgrade_policy.allow_continue_as_new {
                    WorkflowCompatibilityDisposition::RequiresContinueAsNew
                } else if manifest.upgrade_policy.allow_restart_as_new {
                    WorkflowCompatibilityDisposition::RequiresRestartAsNew
                } else {
                    WorkflowCompatibilityDisposition::Rejected
                }
            }
            Some(_) => WorkflowCompatibilityDisposition::Rejected,
        };
        let reason = match disposition {
            WorkflowCompatibilityDisposition::Compatible => None,
            WorkflowCompatibilityDisposition::RequiresContinueAsNew => {
                Some("candidate requires continue-as-new".to_string())
            }
            WorkflowCompatibilityDisposition::RequiresRestartAsNew => {
                Some("candidate requires restart-as-new".to_string())
            }
            WorkflowCompatibilityDisposition::Rejected => {
                Some("candidate is incompatible with the current execution target".to_string())
            }
        };
        Ok(WorkflowCompatibilityDecision {
            disposition,
            reason,
        })
    }
}

#[async_trait]
pub trait WorkflowDeploymentManager: Send + Sync {
    async fn preview(
        &self,
        request: WorkflowDeploymentPreviewRequest,
    ) -> Result<WorkflowRunAssignment, WorkflowTaskError>;

    async fn activate(
        &self,
        request: WorkflowDeploymentActivationRequest,
    ) -> Result<WorkflowDeploymentActivation, WorkflowTaskError>;

    async fn deactivate(
        &self,
        request: WorkflowDeploymentDeactivationRequest,
    ) -> Result<WorkflowDeploymentActivation, WorkflowTaskError>;

    async fn resolve_for_new_run(
        &self,
        request: WorkflowDeploymentResolutionRequest,
    ) -> Result<WorkflowRunAssignment, WorkflowTaskError>;

    async fn continue_as_new(
        &self,
        request: WorkflowUpgradeRequest,
    ) -> Result<WorkflowUpgradePlan, WorkflowTaskError>;

    async fn restart_as_new(
        &self,
        request: WorkflowUpgradeRequest,
    ) -> Result<WorkflowUpgradePlan, WorkflowTaskError>;

    async fn rollback(
        &self,
        request: WorkflowDeploymentRollbackRequest,
    ) -> Result<WorkflowDeploymentActivation, WorkflowTaskError>;
}

#[derive(Clone, Debug, Default)]
pub struct StaticWorkflowDeploymentManager {
    deployments: BTreeMap<WorkflowDeploymentId, WorkflowDeploymentRecord>,
    active: Arc<RwLock<BTreeMap<(String, WorkflowDeploymentEnvironment), WorkflowDeploymentId>>>,
}

impl StaticWorkflowDeploymentManager {
    pub fn new(deployments: impl IntoIterator<Item = WorkflowDeploymentRecord>) -> Self {
        let deployments = deployments
            .into_iter()
            .map(|deployment| (deployment.deployment_id.clone(), deployment))
            .collect();
        Self {
            deployments,
            active: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn deployment(
        &self,
        deployment_id: &WorkflowDeploymentId,
    ) -> Result<WorkflowDeploymentRecord, WorkflowTaskError> {
        self.deployments.get(deployment_id).cloned().ok_or_else(|| {
            WorkflowTaskError::new(
                "missing-deployment",
                format!("unknown workflow deployment {deployment_id}"),
            )
        })
    }

    fn activation_view(
        &self,
        workflow_name: String,
        environment: WorkflowDeploymentEnvironment,
        deployment_id: Option<WorkflowDeploymentId>,
        updated_at_millis: u64,
    ) -> Result<WorkflowDeploymentActivation, WorkflowTaskError> {
        let (active_target, rollout) = match deployment_id.as_ref() {
            Some(deployment_id) => {
                let deployment = self.deployment(deployment_id)?;
                (Some(deployment.target), Some(deployment.rollout))
            }
            None => (None, None),
        };
        Ok(WorkflowDeploymentActivation {
            workflow_name,
            environment,
            active_deployment_id: deployment_id,
            active_target,
            rollout,
            updated_at_millis,
        })
    }

    fn validate_instance_id(instance_id: &str) -> Result<(), WorkflowTaskError> {
        if instance_id.trim().is_empty() {
            return Err(WorkflowTaskError::invalid_contract(
                "workflow instance id cannot be empty",
            ));
        }
        Ok(())
    }

    fn validate_workflow_name(workflow_name: &str) -> Result<(), WorkflowTaskError> {
        if workflow_name.trim().is_empty() {
            return Err(WorkflowTaskError::invalid_contract(
                "workflow name cannot be empty",
            ));
        }
        Ok(())
    }

    fn assignment_from_deployment(
        deployment: &WorkflowDeploymentRecord,
        assigned_at_millis: u64,
    ) -> WorkflowRunAssignment {
        WorkflowRunAssignment {
            workflow_name: deployment.workflow_name.clone(),
            environment: deployment.environment,
            deployment_id: Some(deployment.deployment_id.clone()),
            target: deployment.target.clone(),
            pinned_epoch: 1,
            assigned_at_millis,
        }
    }

    fn rollout_bucket(
        deployment: &WorkflowDeploymentRecord,
        instance_id: &str,
        labels: &BTreeMap<String, String>,
    ) -> u32 {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(deployment.workflow_name.as_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(deployment.environment.as_str().as_bytes());
        bytes.push(0xfe);
        bytes.extend_from_slice(deployment.deployment_id.as_str().as_bytes());
        bytes.push(0xfd);
        bytes.extend_from_slice(instance_id.as_bytes());
        for (key, value) in labels {
            bytes.push(0xfc);
            bytes.extend_from_slice(key.as_bytes());
            bytes.push(0xfb);
            bytes.extend_from_slice(value.as_bytes());
        }
        stable_hash(&bytes) as u32
    }

    fn ensure_rollout_allows_resolution(
        deployment: &WorkflowDeploymentRecord,
        request: &WorkflowDeploymentResolutionRequest,
    ) -> Result<(), WorkflowTaskError> {
        match deployment.rollout {
            WorkflowRolloutPolicy::Immediate => Ok(()),
            WorkflowRolloutPolicy::Manual => Err(WorkflowTaskError::new(
                "rollout-paused",
                format!(
                    "workflow deployment {} is active but requires an explicit rollout override",
                    deployment.deployment_id
                ),
            )),
            WorkflowRolloutPolicy::ByInstanceHash {
                numerator,
                denominator,
            } => {
                if denominator == 0 {
                    return Err(WorkflowTaskError::invalid_contract(
                        "rollout denominator must be greater than zero",
                    ));
                }
                if numerator > denominator {
                    return Err(WorkflowTaskError::invalid_contract(
                        "rollout numerator cannot exceed denominator",
                    ));
                }
                let bucket =
                    Self::rollout_bucket(deployment, &request.instance_id, &request.labels)
                        % denominator;
                if bucket < numerator {
                    Ok(())
                } else {
                    Err(WorkflowTaskError::new(
                        "rollout-not-selected",
                        format!(
                            "workflow deployment {} did not select instance {} for rollout",
                            deployment.deployment_id, request.instance_id
                        ),
                    ))
                }
            }
        }
    }

    fn plan_upgrade(
        transition: WorkflowUpgradeTransition,
        request: WorkflowUpgradeRequest,
    ) -> Result<WorkflowUpgradePlan, WorkflowTaskError> {
        if request.current.workflow_name != request.candidate.workflow_name {
            return Err(WorkflowTaskError::invalid_contract(
                "upgrade candidate workflow name does not match current epoch",
            ));
        }
        if request.current.environment != request.candidate.environment {
            return Err(WorkflowTaskError::invalid_contract(
                "upgrade candidate environment does not match current epoch",
            ));
        }

        let mut next = request.candidate.clone();
        next.pinned_epoch = request.current.pinned_epoch.saturating_add(1);
        next.assigned_at_millis = request.requested_at_millis;
        Ok(WorkflowUpgradePlan {
            transition,
            current: request.current,
            next,
            planned_at_millis: request.requested_at_millis,
        })
    }
}

#[async_trait]
impl WorkflowDeploymentManager for StaticWorkflowDeploymentManager {
    async fn preview(
        &self,
        request: WorkflowDeploymentPreviewRequest,
    ) -> Result<WorkflowRunAssignment, WorkflowTaskError> {
        Self::validate_workflow_name(&request.deployment.workflow_name)?;
        Self::validate_instance_id(&request.instance_id)?;
        Ok(Self::assignment_from_deployment(
            &request.deployment,
            request.previewed_at_millis,
        ))
    }

    async fn activate(
        &self,
        request: WorkflowDeploymentActivationRequest,
    ) -> Result<WorkflowDeploymentActivation, WorkflowTaskError> {
        let deployment = self.deployment(&request.deployment_id)?;
        let mut active = self.active.write().await;
        active.insert(
            (deployment.workflow_name.clone(), deployment.environment),
            deployment.deployment_id.clone(),
        );
        drop(active);
        self.activation_view(
            deployment.workflow_name,
            deployment.environment,
            Some(deployment.deployment_id),
            request.updated_at_millis,
        )
    }

    async fn deactivate(
        &self,
        request: WorkflowDeploymentDeactivationRequest,
    ) -> Result<WorkflowDeploymentActivation, WorkflowTaskError> {
        let deployment = self.deployment(&request.deployment_id)?;
        let mut active = self.active.write().await;
        let key = (deployment.workflow_name.clone(), deployment.environment);
        if active.get(&key) == Some(&deployment.deployment_id) {
            active.remove(&key);
        }
        let active_deployment_id = active.get(&key).cloned();
        drop(active);
        self.activation_view(
            deployment.workflow_name,
            deployment.environment,
            active_deployment_id,
            request.updated_at_millis,
        )
    }

    async fn resolve_for_new_run(
        &self,
        request: WorkflowDeploymentResolutionRequest,
    ) -> Result<WorkflowRunAssignment, WorkflowTaskError> {
        Self::validate_workflow_name(&request.workflow_name)?;
        Self::validate_instance_id(&request.instance_id)?;

        let deployment_id = {
            let active = self.active.read().await;
            active
                .get(&(request.workflow_name.clone(), request.environment))
                .cloned()
        }
        .ok_or_else(|| {
            WorkflowTaskError::new(
                "missing-deployment",
                format!(
                    "workflow {} has no active {:?} deployment",
                    request.workflow_name, request.environment
                ),
            )
        })?;
        let deployment = self.deployment(&deployment_id)?;
        Self::ensure_rollout_allows_resolution(&deployment, &request)?;
        Ok(Self::assignment_from_deployment(
            &deployment,
            request.requested_at_millis,
        ))
    }

    async fn continue_as_new(
        &self,
        request: WorkflowUpgradeRequest,
    ) -> Result<WorkflowUpgradePlan, WorkflowTaskError> {
        Self::plan_upgrade(WorkflowUpgradeTransition::ContinueAsNew, request)
    }

    async fn restart_as_new(
        &self,
        request: WorkflowUpgradeRequest,
    ) -> Result<WorkflowUpgradePlan, WorkflowTaskError> {
        Self::plan_upgrade(WorkflowUpgradeTransition::RestartAsNew, request)
    }

    async fn rollback(
        &self,
        request: WorkflowDeploymentRollbackRequest,
    ) -> Result<WorkflowDeploymentActivation, WorkflowTaskError> {
        let deployment = self.deployment(&request.deployment_id)?;
        if deployment.workflow_name != request.workflow_name {
            return Err(WorkflowTaskError::invalid_contract(
                "rollback deployment workflow name does not match request",
            ));
        }
        if deployment.environment != request.environment {
            return Err(WorkflowTaskError::invalid_contract(
                "rollback deployment environment does not match request",
            ));
        }

        let mut active = self.active.write().await;
        active.insert(
            (deployment.workflow_name.clone(), deployment.environment),
            deployment.deployment_id.clone(),
        );
        drop(active);
        self.activation_view(
            deployment.workflow_name,
            deployment.environment,
            Some(deployment.deployment_id),
            request.updated_at_millis,
        )
    }
}

pub trait WorkflowHistoryOrderer: Send + Sync {
    fn next_history_sequence(&self, persisted_history_len: u64) -> u64;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AppendOnlyWorkflowHistoryOrderer;

impl WorkflowHistoryOrderer for AppendOnlyWorkflowHistoryOrderer {
    fn next_history_sequence(&self, persisted_history_len: u64) -> u64 {
        persisted_history_len.saturating_add(1)
    }
}

pub trait WorkflowWakeupPlanner: Send + Sync {
    fn timer_task_id(
        &self,
        run_id: &WorkflowRunId,
        timer_id: &[u8],
        fire_at_millis: u64,
    ) -> WorkflowTaskId;

    fn retry_task_id(
        &self,
        run_id: &WorkflowRunId,
        attempt: u32,
        wake_at_millis: u64,
    ) -> WorkflowTaskId;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DeterministicWorkflowWakeupPlanner;

impl WorkflowWakeupPlanner for DeterministicWorkflowWakeupPlanner {
    fn timer_task_id(
        &self,
        run_id: &WorkflowRunId,
        timer_id: &[u8],
        fire_at_millis: u64,
    ) -> WorkflowTaskId {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(run_id.as_str().as_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(timer_id);
        bytes.extend_from_slice(&fire_at_millis.to_be_bytes());
        WorkflowTaskId::new(format!("timer:{:016x}", stable_hash(&bytes)))
            .expect("planner should always produce a non-empty timer task id")
    }

    fn retry_task_id(
        &self,
        run_id: &WorkflowRunId,
        attempt: u32,
        wake_at_millis: u64,
    ) -> WorkflowTaskId {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(run_id.as_str().as_bytes());
        bytes.push(0xfe);
        bytes.extend_from_slice(&attempt.to_be_bytes());
        bytes.extend_from_slice(&wake_at_millis.to_be_bytes());
        WorkflowTaskId::new(format!("retry:{:016x}", stable_hash(&bytes)))
            .expect("planner should always produce a non-empty retry task id")
    }
}

pub trait WorkflowVisibilityProjector: Send + Sync {
    fn project_visibility(
        &self,
        state: &WorkflowStateRecord,
        output: &WorkflowTransitionOutput,
    ) -> WorkflowVisibilityRecord;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PassthroughWorkflowVisibilityProjector;

impl WorkflowVisibilityProjector for PassthroughWorkflowVisibilityProjector {
    fn project_visibility(
        &self,
        state: &WorkflowStateRecord,
        output: &WorkflowTransitionOutput,
    ) -> WorkflowVisibilityRecord {
        let lifecycle = output.lifecycle.unwrap_or(state.lifecycle);
        let mut summary = output.visibility.clone().unwrap_or_default().summary;
        summary
            .entry("lifecycle".to_string())
            .or_insert_with(|| lifecycle.as_str().to_string());
        WorkflowVisibilityRecord {
            run_id: state.run_id.clone(),
            target: state.target.clone(),
            workflow_name: state.workflow_name.clone(),
            instance_id: state.instance_id.clone(),
            lifecycle,
            last_task_id: state.current_task_id.clone(),
            visible_history_len: state.history_len,
            summary,
            updated_at_millis: state.updated_at_millis,
        }
    }
}

pub trait WorkflowParityComparator: Send + Sync {
    fn equivalent(&self, left: &WorkflowTransitionOutput, right: &WorkflowTransitionOutput)
    -> bool;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StrictWorkflowParityComparator;

impl WorkflowParityComparator for StrictWorkflowParityComparator {
    fn equivalent(
        &self,
        left: &WorkflowTransitionOutput,
        right: &WorkflowTransitionOutput,
    ) -> bool {
        left == right
    }
}

fn stable_hash(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod durable_format_tests {
    use std::collections::BTreeMap;

    use terracedb::test_support::assert_durable_format_fixture;

    use super::*;

    #[test]
    fn durable_format_fixtures_match_golden_files() {
        let bundle_id = WorkflowBundleId::new("bundle:orders:v2").expect("bundle id");
        let registration_id =
            WorkflowRegistrationId::new("native:orders:v1").expect("registration id");
        let deployment_id =
            WorkflowDeploymentId::new("deploy:orders:prod:v2").expect("deployment id");
        let run_id = WorkflowRunId::new("run:orders:order-1:2").expect("run id");
        let task_id = WorkflowTaskId::new("task:order-1:update-1").expect("task id");

        let registration = WorkflowNativeRegistrationMetadata {
            registration_id: registration_id.clone(),
            workflow_name: "orders".to_string(),
            registration: "orders/native".to_string(),
            registered_at_millis: 10,
            labels: BTreeMap::from([("channel".to_string(), "stable".to_string())]),
        };
        assert_durable_format_fixture(
            "workflow-native-registration-metadata-v1.bin",
            &encode_fixture(&registration),
        );

        let execution_descriptor = WorkflowExecutionDescriptor::NativeRegistration {
            registration: registration.clone(),
        };
        assert_durable_format_fixture(
            "workflow-execution-descriptor-v1.bin",
            &encode_fixture(&execution_descriptor),
        );
        assert_durable_format_fixture(
            "workflow-execution-descriptor-bundle-v1.bin",
            &encode_fixture(&WorkflowExecutionDescriptor::Bundle {
                bundle: WorkflowBundleMetadata {
                    bundle_id: bundle_id.clone(),
                    workflow_name: "orders".to_string(),
                    kind: WorkflowBundleKind::Sandbox {
                        abi: "workflow-task/v1".to_string(),
                        module: "orders.js".to_string(),
                        entrypoint: "default".to_string(),
                        preparation: Default::default(),
                    },
                    created_at_millis: 10,
                    labels: BTreeMap::from([("channel".to_string(), "bundle".to_string())]),
                },
            }),
        );

        let deployment = WorkflowDeploymentRecord {
            deployment_id: deployment_id.clone(),
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            target: WorkflowExecutionTarget::Bundle {
                bundle_id: bundle_id.clone(),
            },
            rollout: WorkflowRolloutPolicy::ByInstanceHash {
                numerator: 1,
                denominator: 10,
            },
            created_at_millis: 11,
            labels: BTreeMap::from([("channel".to_string(), "canary".to_string())]),
        };
        assert_durable_format_fixture(
            "workflow-deployment-record-v1.bin",
            &encode_fixture(&deployment),
        );
        assert_durable_format_fixture(
            "workflow-rollout-policy-immediate-v1.bin",
            &encode_fixture(&WorkflowRolloutPolicy::Immediate),
        );
        assert_durable_format_fixture(
            "workflow-rollout-policy-manual-v1.bin",
            &encode_fixture(&WorkflowRolloutPolicy::Manual),
        );

        let preview_request = WorkflowDeploymentPreviewRequest {
            deployment: WorkflowDeploymentRecord {
                environment: WorkflowDeploymentEnvironment::Preview,
                ..deployment.clone()
            },
            instance_id: "order-1-preview".to_string(),
            labels: BTreeMap::from([("tenant".to_string(), "preview".to_string())]),
            previewed_at_millis: 11,
        };
        assert_durable_format_fixture(
            "workflow-deployment-preview-request-v1.bin",
            &encode_fixture(&preview_request),
        );

        let resolution_request = WorkflowDeploymentResolutionRequest {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            instance_id: "order-1".to_string(),
            labels: BTreeMap::from([("tenant".to_string(), "west".to_string())]),
            requested_at_millis: 12,
        };
        assert_durable_format_fixture(
            "workflow-deployment-resolution-request-v1.bin",
            &encode_fixture(&resolution_request),
        );

        let activation_request = WorkflowDeploymentActivationRequest {
            deployment_id: deployment_id.clone(),
            updated_at_millis: 12,
        };
        assert_durable_format_fixture(
            "workflow-deployment-activation-request-v1.bin",
            &encode_fixture(&activation_request),
        );

        let deactivation_request = WorkflowDeploymentDeactivationRequest {
            deployment_id: deployment_id.clone(),
            updated_at_millis: 13,
        };
        assert_durable_format_fixture(
            "workflow-deployment-deactivation-request-v1.bin",
            &encode_fixture(&deactivation_request),
        );

        let rollback_request = WorkflowDeploymentRollbackRequest {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            deployment_id: deployment_id.clone(),
            updated_at_millis: 14,
        };
        assert_durable_format_fixture(
            "workflow-deployment-rollback-request-v1.bin",
            &encode_fixture(&rollback_request),
        );

        let activation = WorkflowDeploymentActivation {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            active_deployment_id: Some(deployment_id.clone()),
            active_target: Some(deployment.target.clone()),
            rollout: Some(deployment.rollout.clone()),
            updated_at_millis: 12,
        };
        assert_durable_format_fixture(
            "workflow-deployment-activation-v1.bin",
            &encode_fixture(&activation),
        );
        assert_durable_format_fixture(
            "workflow-deployment-activation-cleared-v1.bin",
            &encode_fixture(&WorkflowDeploymentActivation {
                workflow_name: "orders".to_string(),
                environment: WorkflowDeploymentEnvironment::Production,
                active_deployment_id: None,
                active_target: None,
                rollout: None,
                updated_at_millis: 13,
            }),
        );

        let target = WorkflowExecutionTarget::Bundle {
            bundle_id: bundle_id.clone(),
        };

        let state = WorkflowStateRecord {
            run_id: run_id.clone(),
            target: target.clone(),
            workflow_name: "orders".to_string(),
            instance_id: "order-1".to_string(),
            lifecycle: WorkflowLifecycleState::Running,
            current_task_id: Some(task_id.clone()),
            history_len: 7,
            state: Some(WorkflowPayload::bytes("state")),
            updated_at_millis: 13,
        };
        let visibility = WorkflowVisibilityEntry {
            record: WorkflowVisibilityRecord {
                run_id: run_id.clone(),
                target: target.clone(),
                workflow_name: "orders".to_string(),
                instance_id: "order-1".to_string(),
                lifecycle: WorkflowLifecycleState::Running,
                last_task_id: Some(task_id.clone()),
                visible_history_len: 7,
                summary: BTreeMap::from([
                    ("lifecycle".to_string(), "running".to_string()),
                    ("deployment".to_string(), deployment_id.to_string()),
                ]),
                updated_at_millis: 13,
            },
            execution: Some(WorkflowExecutionEpoch::from_assignment(
                run_id.clone(),
                WorkflowRunAssignment {
                    workflow_name: "orders".to_string(),
                    environment: WorkflowDeploymentEnvironment::Production,
                    deployment_id: Some(deployment_id.clone()),
                    target: deployment.target.clone(),
                    pinned_epoch: 1,
                    assigned_at_millis: 13,
                },
                13,
            )),
        };
        assert_durable_format_fixture(
            "workflow-visibility-entry-v1.bin",
            &encode_fixture(&visibility),
        );

        let visibility_list_request = WorkflowVisibilityListRequest {
            workflow_name: Some("orders".to_string()),
            lifecycle: Some(WorkflowLifecycleState::Running),
            deployment_id: Some(deployment_id.clone()),
            target: Some(deployment.target.clone()),
            page_size: 20,
            cursor: Some(run_id.clone()),
        };
        assert_durable_format_fixture(
            "workflow-visibility-list-request-v1.bin",
            &encode_fixture(&visibility_list_request),
        );

        let visibility_list_response = WorkflowVisibilityListResponse {
            entries: vec![visibility.clone()],
            next_cursor: Some(run_id.clone()),
        };
        assert_durable_format_fixture(
            "workflow-visibility-list-response-v1.bin",
            &encode_fixture(&visibility_list_response),
        );

        let describe_request = WorkflowDescribeRequest {
            run_id: run_id.clone(),
        };
        assert_durable_format_fixture(
            "workflow-describe-request-v1.bin",
            &encode_fixture(&describe_request),
        );

        let describe_response = WorkflowDescribeResponse {
            state: state.clone(),
            lifecycle: Some(WorkflowLifecycleRecord {
                run_id: run_id.clone(),
                lifecycle: WorkflowLifecycleState::Running,
                updated_at_millis: 13,
                reason: Some("task-applied".to_string()),
                task_id: Some(task_id.clone()),
                attempt: 1,
            }),
            visibility: visibility.clone(),
        };
        assert_durable_format_fixture(
            "workflow-describe-response-v1.bin",
            &encode_fixture(&describe_response),
        );

        let history_page_request = WorkflowVisibleHistoryPageRequest {
            run_id: run_id.clone(),
            after_sequence: Some(6),
            limit: 25,
        };
        assert_durable_format_fixture(
            "workflow-visible-history-page-request-v1.bin",
            &encode_fixture(&history_page_request),
        );

        let history_page_response = WorkflowVisibleHistoryPageResponse {
            entries: vec![WorkflowVisibleHistoryEntry {
                sequence: 7,
                lifecycle: WorkflowLifecycleState::Running,
                task_id: Some(task_id.clone()),
                summary: BTreeMap::from([("lifecycle".to_string(), "running".to_string())]),
                note: Some("visible".to_string()),
                recorded_at_millis: 13,
            }],
            next_after_sequence: Some(7),
        };
        assert_durable_format_fixture(
            "workflow-visible-history-page-response-v1.bin",
            &encode_fixture(&history_page_response),
        );

        let query_request = WorkflowQueryRequest {
            query_id: WorkflowQueryId::new("query:orders:inspect-1").expect("query id"),
            run_id: run_id.clone(),
            target: WorkflowQueryTarget::State,
            name: "inspect".to_string(),
            payload: Some(WorkflowPayload::bytes("what-is-current-state")),
            requested_at_millis: 14,
        };
        assert_durable_format_fixture(
            "workflow-query-request-v1.bin",
            &encode_fixture(&query_request),
        );
        assert_durable_format_fixture(
            "workflow-query-target-visibility-v1.bin",
            &encode_fixture(&WorkflowQueryTarget::Visibility),
        );
        assert_durable_format_fixture(
            "workflow-query-target-execution-v1.bin",
            &encode_fixture(&WorkflowQueryTarget::Execution),
        );

        let query_response = WorkflowQueryResponse::Accepted {
            query_id: query_request.query_id.clone(),
            payload: state.state.clone(),
            answered_at_millis: 15,
        };
        assert_durable_format_fixture(
            "workflow-query-response-v1.bin",
            &encode_fixture(&query_response),
        );

        let query_rejected = WorkflowQueryResponse::Rejected {
            query_id: query_request.query_id.clone(),
            error: WorkflowTaskError::new("query-denied", "inspection denied"),
            answered_at_millis: 15,
        };
        assert_durable_format_fixture(
            "workflow-query-response-rejected-v1.bin",
            &encode_fixture(&query_rejected),
        );

        let update_request = WorkflowUpdateRequest {
            update_id: WorkflowUpdateId::new("update:orders:pause-1").expect("update id"),
            run_id: run_id.clone(),
            lane: WorkflowUpdateLane::Control,
            name: "pause".to_string(),
            payload: Some(WorkflowPayload::bytes("operator-request")),
            requested_at_millis: 16,
        };
        assert_durable_format_fixture(
            "workflow-update-request-v1.bin",
            &encode_fixture(&update_request),
        );
        assert_durable_format_fixture(
            "workflow-update-lane-update-v1.bin",
            &encode_fixture(&WorkflowUpdateLane::Update),
        );

        let update_admission = WorkflowUpdateAdmission::Accepted {
            update_id: update_request.update_id.clone(),
            task_id: task_id.clone(),
        };
        assert_durable_format_fixture(
            "workflow-update-admission-v1.bin",
            &encode_fixture(&update_admission),
        );

        let update_rejected = WorkflowUpdateAdmission::Rejected {
            update_id: update_request.update_id.clone(),
            error: WorkflowTaskError::new("update-denied", "control lane denied"),
        };
        assert_durable_format_fixture(
            "workflow-update-admission-rejected-v1.bin",
            &encode_fixture(&update_rejected),
        );

        let update_duplicate = WorkflowUpdateAdmission::Duplicate {
            update_id: update_request.update_id.clone(),
            original_task_id: task_id.clone(),
        };
        assert_durable_format_fixture(
            "workflow-update-admission-duplicate-v1.bin",
            &encode_fixture(&update_duplicate),
        );

        let compatibility = WorkflowCompatibilityManifest {
            target: deployment.target.clone(),
            workflow_name: "orders".to_string(),
            runtime_surface: Some("state-outbox-timers/v1".to_string()),
            compatible_predecessors: vec![registration.target()],
            upgrade_policy: WorkflowUpgradePolicy {
                allow_continue_as_new: true,
                allow_restart_as_new: false,
            },
            created_at_millis: 17,
            labels: BTreeMap::from([("reason".to_string(), "upgrade".to_string())]),
        };
        assert_durable_format_fixture(
            "workflow-compatibility-manifest-v1.bin",
            &encode_fixture(&compatibility),
        );

        let compatibility_check = WorkflowCompatibilityCheck {
            workflow_name: "orders".to_string(),
            current: Some(registration.target()),
            candidate: deployment.target.clone(),
        };
        assert_durable_format_fixture(
            "workflow-compatibility-check-v1.bin",
            &encode_fixture(&compatibility_check),
        );

        let compatibility_decision = WorkflowCompatibilityDecision {
            disposition: WorkflowCompatibilityDisposition::RequiresContinueAsNew,
            reason: Some("candidate requires continue-as-new".to_string()),
        };
        assert_durable_format_fixture(
            "workflow-compatibility-decision-v1.bin",
            &encode_fixture(&compatibility_decision),
        );
        assert_durable_format_fixture(
            "workflow-compatibility-decision-compatible-v1.bin",
            &encode_fixture(&WorkflowCompatibilityDecision {
                disposition: WorkflowCompatibilityDisposition::Compatible,
                reason: None,
            }),
        );
        assert_durable_format_fixture(
            "workflow-compatibility-decision-requires-restart-as-new-v1.bin",
            &encode_fixture(&WorkflowCompatibilityDecision {
                disposition: WorkflowCompatibilityDisposition::RequiresRestartAsNew,
                reason: Some("candidate requires restart-as-new".to_string()),
            }),
        );
        assert_durable_format_fixture(
            "workflow-compatibility-decision-rejected-v1.bin",
            &encode_fixture(&WorkflowCompatibilityDecision {
                disposition: WorkflowCompatibilityDisposition::Rejected,
                reason: Some(
                    "candidate is incompatible with the current execution target".to_string(),
                ),
            }),
        );

        let upgrade_request = WorkflowUpgradeRequest {
            current: visibility
                .execution
                .clone()
                .expect("visibility execution should exist"),
            candidate: WorkflowRunAssignment {
                workflow_name: "orders".to_string(),
                environment: WorkflowDeploymentEnvironment::Production,
                deployment_id: Some(deployment_id.clone()),
                target: deployment.target.clone(),
                pinned_epoch: 1,
                assigned_at_millis: 17,
            },
            requested_at_millis: 18,
        };
        assert_durable_format_fixture(
            "workflow-upgrade-request-v1.bin",
            &encode_fixture(&upgrade_request),
        );

        let upgrade_plan = WorkflowUpgradePlan {
            transition: WorkflowUpgradeTransition::ContinueAsNew,
            current: upgrade_request.current.clone(),
            next: WorkflowRunAssignment {
                workflow_name: "orders".to_string(),
                environment: WorkflowDeploymentEnvironment::Production,
                deployment_id: Some(deployment_id),
                target: WorkflowExecutionTarget::Bundle { bundle_id },
                pinned_epoch: 2,
                assigned_at_millis: 18,
            },
            planned_at_millis: 18,
        };
        assert_durable_format_fixture(
            "workflow-upgrade-plan-v1.bin",
            &encode_fixture(&upgrade_plan),
        );
        assert_durable_format_fixture(
            "workflow-upgrade-plan-restart-as-new-v1.bin",
            &encode_fixture(&WorkflowUpgradePlan {
                transition: WorkflowUpgradeTransition::RestartAsNew,
                current: upgrade_plan.current.clone(),
                next: upgrade_plan.next.clone(),
                planned_at_millis: 19,
            }),
        );
    }

    fn encode_fixture<T: serde::Serialize>(value: &T) -> Vec<u8> {
        serde_json::to_vec(value).expect("encode durable fixture")
    }

    #[test]
    fn revised_recovery_contract_types_round_trip() {
        let bundle_id = WorkflowBundleId::new("bundle:orders:v2").expect("bundle id");
        let deployment_id =
            WorkflowDeploymentId::new("deploy:orders:prod:v2").expect("deployment id");
        let run_id = WorkflowRunId::new("run:orders:order-1:2").expect("run id");
        let task_id = WorkflowTaskId::new("task:order-1:update-1").expect("task id");
        let segment_id =
            WorkflowRecoverySegmentId::new("segment:orders:order-1:2:0").expect("segment id");
        let savepoint_id =
            WorkflowSavepointId::new("savepoint:orders:order-1:2:0").expect("savepoint id");

        let assignment = WorkflowRunAssignment {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            deployment_id: Some(deployment_id.clone()),
            target: WorkflowExecutionTarget::Bundle {
                bundle_id: bundle_id.clone(),
            },
            pinned_epoch: 2,
            assigned_at_millis: 13,
        };
        let execution = WorkflowExecutionEpoch::from_assignment(run_id.clone(), assignment, 14);

        let run = WorkflowRunRecord {
            run_id: run_id.clone(),
            workflow_name: "orders".to_string(),
            instance_id: "order-1".to_string(),
            lifecycle: WorkflowLifecycleState::Running,
            execution: execution.clone(),
            current_task_id: Some(task_id.clone()),
            active_recovery_segment_id: Some(segment_id.clone()),
            latest_savepoint_id: Some(savepoint_id.clone()),
            visible_history_len: 3,
            updated_at_millis: 15,
        };
        assert_json_round_trip(&run);

        let transition_output = WorkflowTransitionOutput {
            state: WorkflowStateMutation::Put {
                state: WorkflowPayload::bytes("state"),
            },
            lifecycle: Some(WorkflowLifecycleState::Running),
            visibility: Some(WorkflowVisibilityUpdate {
                summary: BTreeMap::from([("stage".to_string(), "authorized".to_string())]),
                note: Some("payment authorized".to_string()),
            }),
            continue_as_new: None,
            commands: vec![WorkflowCommand::Outbox {
                entry: WorkflowOutboxCommand {
                    outbox_id: b"order-1:notify".to_vec(),
                    idempotency_key: "order-1:notify".to_string(),
                    payload: b"notify".to_vec(),
                },
            }],
        };

        let journal = WorkflowRecoveryJournalRecord {
            run_id: run_id.clone(),
            segment_id: segment_id.clone(),
            sequence: 9,
            entry: WorkflowRecoveryJournalEntry::TransitionApplied {
                task_id: task_id.clone(),
                output: transition_output.clone(),
                committed_at_millis: 16,
            },
        };
        assert_json_round_trip(&journal);

        let savepoint = WorkflowSavepointRecord {
            run_id: run_id.clone(),
            savepoint_id: savepoint_id.clone(),
            checkpoint_kind: WorkflowCheckpointKind::InternalSavepoint,
            covering_segment_id: segment_id.clone(),
            covered_through_sequence: 9,
            state: Some(WorkflowPayload::bytes("opaque-savepoint-state")),
            created_at_millis: 17,
        };
        assert_json_round_trip(&savepoint);

        let visible_history = WorkflowVisibleHistoryPageResponse {
            entries: vec![WorkflowVisibleHistoryEntry {
                sequence: 3,
                lifecycle: WorkflowLifecycleState::Running,
                task_id: Some(task_id.clone()),
                summary: BTreeMap::from([
                    ("step".to_string(), "authorize-payment".to_string()),
                    ("status".to_string(), "ok".to_string()),
                ]),
                note: Some("collapsed from multiple primitive effects".to_string()),
                recorded_at_millis: 16,
            }],
            next_after_sequence: Some(3),
        };
        assert_json_round_trip(&visible_history);
    }

    fn assert_json_round_trip<T>(value: &T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_vec(value).expect("encode json round trip");
        let decoded = serde_json::from_slice::<T>(&encoded).expect("decode json round trip");
        assert_eq!(&decoded, value);
    }
}
