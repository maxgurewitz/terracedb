use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb_workflows_core::{
    WorkflowDeterministicContext, WorkflowDeterministicSeed, WorkflowHandlerContract,
    WorkflowObservationValue, WorkflowSourceEvent, WorkflowTaskError, WorkflowTransitionInput,
    WorkflowTransitionOutput,
};

pub const WORKFLOW_TASK_V1_ABI: &str = "workflow-task/v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTaskV1RouteRequest {
    pub abi: String,
    pub event: WorkflowSourceEvent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTaskV1RouteResponse {
    pub abi: String,
    pub instance_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTaskV1Request {
    pub abi: String,
    pub input: WorkflowTransitionInput,
    pub deterministic: WorkflowDeterministicSeed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTaskV1Response {
    pub abi: String,
    pub output: WorkflowTransitionOutput,
}

#[async_trait]
pub trait WorkflowTaskV1Handler: Send + Sync {
    async fn route_event_v1(
        &self,
        request: WorkflowTaskV1RouteRequest,
    ) -> Result<WorkflowTaskV1RouteResponse, WorkflowTaskError>;

    async fn handle_task_v1(
        &self,
        request: WorkflowTaskV1Request,
    ) -> Result<WorkflowTaskV1Response, WorkflowTaskError>;
}

#[derive(Clone, Debug)]
pub struct SandboxWorkflowHandlerAdapter<H> {
    inner: H,
}

impl<H> SandboxWorkflowHandlerAdapter<H> {
    pub fn new(inner: H) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> H {
        self.inner
    }
}

#[async_trait]
impl<H> WorkflowHandlerContract for SandboxWorkflowHandlerAdapter<H>
where
    H: WorkflowTaskV1Handler,
{
    async fn route_event(&self, event: &WorkflowSourceEvent) -> Result<String, WorkflowTaskError> {
        let response = self
            .inner
            .route_event_v1(WorkflowTaskV1RouteRequest {
                abi: WORKFLOW_TASK_V1_ABI.to_string(),
                event: event.clone(),
            })
            .await?;
        ensure_abi(&response.abi)?;
        Ok(response.instance_id)
    }

    async fn handle_task(
        &self,
        input: WorkflowTransitionInput,
        ctx: WorkflowDeterministicContext,
    ) -> Result<WorkflowTransitionOutput, WorkflowTaskError> {
        ctx.observability().record_attribute(
            "terracedb.workflow.abi",
            WorkflowObservationValue::String(WORKFLOW_TASK_V1_ABI.to_string()),
        );
        let response = self
            .inner
            .handle_task_v1(WorkflowTaskV1Request {
                abi: WORKFLOW_TASK_V1_ABI.to_string(),
                input,
                deterministic: ctx.seed(),
            })
            .await?;
        ensure_abi(&response.abi)?;
        Ok(response.output)
    }
}

fn ensure_abi(abi: &str) -> Result<(), WorkflowTaskError> {
    if abi == WORKFLOW_TASK_V1_ABI {
        return Ok(());
    }
    Err(WorkflowTaskError::new(
        "abi-mismatch",
        format!("expected {WORKFLOW_TASK_V1_ABI}, got {abi}"),
    ))
}
