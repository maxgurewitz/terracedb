use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::{OperationContext, Timestamp};
use thiserror::Error;

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
workflow_id_type!(WorkflowRunId);
workflow_id_type!(WorkflowTaskId);

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
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowBundleKind {
    NativeRust {
        registration: String,
    },
    Sandbox {
        abi: String,
        module: String,
        entrypoint: String,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowLifecycleRecord {
    pub run_id: WorkflowRunId,
    pub lifecycle: WorkflowLifecycleState,
    pub updated_at_millis: u64,
    pub reason: Option<String>,
    pub task_id: Option<WorkflowTaskId>,
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStateRecord {
    pub run_id: WorkflowRunId,
    pub bundle_id: WorkflowBundleId,
    pub workflow_name: String,
    pub instance_id: String,
    pub lifecycle: WorkflowLifecycleState,
    pub current_task_id: Option<WorkflowTaskId>,
    pub history_len: u64,
    pub state: Option<WorkflowPayload>,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkflowVisibilityUpdate {
    pub summary: BTreeMap<String, String>,
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVisibilityRecord {
    pub run_id: WorkflowRunId,
    pub bundle_id: WorkflowBundleId,
    pub workflow_name: String,
    pub instance_id: String,
    pub lifecycle: WorkflowLifecycleState,
    pub last_task_id: Option<WorkflowTaskId>,
    pub history_len: u64,
    pub summary: BTreeMap<String, String>,
    pub updated_at_millis: u64,
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
    Outbox { entry: WorkflowOutboxCommand },
    Timer { command: WorkflowTimerCommand },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowContinueAsNew {
    pub next_run_id: WorkflowRunId,
    pub next_bundle_id: WorkflowBundleId,
    pub state: Option<WorkflowPayload>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTransitionInput {
    pub run_id: WorkflowRunId,
    pub bundle_id: WorkflowBundleId,
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
        bundle_id: WorkflowBundleId,
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
        next_bundle_id: WorkflowBundleId,
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
            bundle_id: state.bundle_id.clone(),
            workflow_name: state.workflow_name.clone(),
            instance_id: state.instance_id.clone(),
            lifecycle,
            last_task_id: state.current_task_id.clone(),
            history_len: state.history_len,
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
