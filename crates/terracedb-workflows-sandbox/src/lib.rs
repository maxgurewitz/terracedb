use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use terracedb_sandbox::{SandboxError, SandboxSession};
use terracedb_workflows_core::{
    WorkflowBundleKind, WorkflowBundleMetadata, WorkflowDeterministicContext,
    WorkflowDeterministicSeed, WorkflowHandlerContract, WorkflowObservationValue,
    WorkflowSourceEvent, WorkflowTaskError, WorkflowTransitionInput, WorkflowTransitionOutput,
};

pub const WORKFLOW_TASK_V1_ABI: &str = "workflow-task/v1";
pub const WORKFLOW_SANDBOX_SDK_SOURCE: &str = include_str!("../sdk/workflow.js");

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

#[derive(Clone)]
pub struct SandboxModuleWorkflowTaskV1Handler {
    session: SandboxSession,
    bundle_id: String,
    module: String,
    entrypoint: String,
}

impl SandboxModuleWorkflowTaskV1Handler {
    pub fn new(
        session: SandboxSession,
        bundle: WorkflowBundleMetadata,
    ) -> Result<Self, WorkflowTaskError> {
        let bundle_id = bundle.bundle_id.to_string();
        let WorkflowBundleKind::Sandbox {
            abi,
            module,
            entrypoint,
        } = bundle.kind
        else {
            return Err(WorkflowTaskError::invalid_contract(format!(
                "bundle {bundle_id} is not a sandbox workflow bundle"
            )));
        };
        ensure_abi(&abi)?;
        if module.trim().is_empty() {
            return Err(WorkflowTaskError::invalid_contract(format!(
                "sandbox bundle {bundle_id} is missing a module specifier"
            )));
        }
        if entrypoint.trim().is_empty() {
            return Err(WorkflowTaskError::invalid_contract(format!(
                "sandbox bundle {bundle_id} is missing an entrypoint export"
            )));
        }
        Ok(Self {
            session,
            bundle_id,
            module,
            entrypoint,
        })
    }

    async fn invoke<Request, Response>(
        &self,
        method: &str,
        request: Request,
    ) -> Result<Response, WorkflowTaskError>
    where
        Request: Serialize + Send + 'static,
        Response: DeserializeOwned + Send + 'static,
    {
        let session = self.session.clone();
        let bundle_id = self.bundle_id.clone();
        let module = self.module.clone();
        let entrypoint = self.entrypoint.clone();
        let method = method.to_string();
        let request = serde_json::to_value(request)
            .map_err(|error| WorkflowTaskError::serialization("workflow-task/v1 request", error))?;

        tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    WorkflowTaskError::new(
                        "sandbox-runtime",
                        format!("failed to build workflow sandbox runtime: {error}"),
                    )
                })?;

            runtime.block_on(async move {
                let loader = session.module_loader().await;
                let module = loader
                    .resolve(&module, None)
                    .await
                    .map_err(sandbox_error_to_task_error)?;
                let source = build_invocation_source(&module, &entrypoint, &method, request)
                    .map_err(|error| {
                        WorkflowTaskError::serialization("workflow-task/v1 source", error)
                    })?;
                let result = session
                    .eval(source)
                    .await
                    .map_err(sandbox_error_to_task_error)?;
                let envelope: SandboxWorkflowTaskV1Envelope<Response> =
                    serde_json::from_value(result.result.ok_or_else(|| {
                        WorkflowTaskError::new(
                            "sandbox-invalid-response",
                            format!(
                                "workflow-task/v1 module {} did not export a response envelope",
                                bundle_id
                            ),
                        )
                    })?)
                    .map_err(|error| {
                        WorkflowTaskError::serialization(
                            "workflow-task/v1 response envelope",
                            error,
                        )
                    })?;
                match (envelope.ok, envelope.value, envelope.error) {
                    (true, Some(value), _) => Ok(value),
                    (false, _, Some(error)) => Err(error.into_task_error()),
                    _ => Err(WorkflowTaskError::new(
                        "sandbox-invalid-response",
                        format!(
                            "workflow-task/v1 module {} returned an incomplete response envelope",
                            bundle_id
                        ),
                    )),
                }
            })
        })
        .await
        .map_err(|error| {
            WorkflowTaskError::new(
                "sandbox-runtime",
                format!("workflow sandbox task join failed: {error}"),
            )
        })?
    }
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

#[async_trait]
impl WorkflowTaskV1Handler for SandboxModuleWorkflowTaskV1Handler {
    async fn route_event_v1(
        &self,
        request: WorkflowTaskV1RouteRequest,
    ) -> Result<WorkflowTaskV1RouteResponse, WorkflowTaskError> {
        ensure_abi(&request.abi)?;
        self.invoke("routeEventV1", request).await
    }

    async fn handle_task_v1(
        &self,
        request: WorkflowTaskV1Request,
    ) -> Result<WorkflowTaskV1Response, WorkflowTaskError> {
        ensure_abi(&request.abi)?;
        self.invoke("handleTaskV1", request).await
    }
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

#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct SandboxWorkflowTaskV1Envelope<T> {
    ok: bool,
    #[serde(default)]
    value: Option<T>,
    #[serde(default)]
    error: Option<SandboxWorkflowTaskV1Error>,
}

#[derive(Debug, Deserialize)]
struct SandboxWorkflowTaskV1Error {
    code: String,
    message: String,
    #[serde(default)]
    metadata: Option<JsonValue>,
}

impl SandboxWorkflowTaskV1Error {
    fn into_task_error(self) -> WorkflowTaskError {
        let message = match self.metadata {
            Some(metadata) => format!("{} ({metadata})", self.message),
            None => self.message,
        };
        WorkflowTaskError::new(self.code, message)
    }
}

fn build_invocation_source(
    module: &str,
    entrypoint: &str,
    method: &str,
    request: JsonValue,
) -> Result<String, serde_json::Error> {
    let module = serde_json::to_string(module)?;
    let entrypoint = serde_json::to_string(entrypoint)?;
    let method = serde_json::to_string(method)?;
    let request = serde_json::to_string(&request)?;
    Ok(format!(
        r#"
import * as __terrace_workflow_module from {module};

const __terrace_entrypoint_name = {entrypoint};
const __terrace_method_name = {method};
const __terrace_request = {request};
const __terrace_entrypoint = __terrace_entrypoint_name === "default"
  ? __terrace_workflow_module.default
  : __terrace_workflow_module[__terrace_entrypoint_name];

function __terrace_error(code, message, metadata) {{
  return {{
    ok: false,
    error: {{
      code,
      message,
      metadata: metadata ?? null,
    }},
  }};
}}

export default await (async () => {{
  try {{
    if ((__terrace_entrypoint === null) || (__terrace_entrypoint === undefined)) {{
      return __terrace_error(
        "invalid-contract",
        `workflow entrypoint ${{__terrace_entrypoint_name}} was not found`,
      );
    }}
    const __terrace_handler = __terrace_entrypoint[__terrace_method_name];
    if (typeof __terrace_handler !== "function") {{
      return __terrace_error(
        "invalid-contract",
        `workflow entrypoint ${{__terrace_entrypoint_name}} is missing ${{__terrace_method_name}}`,
      );
    }}
    return {{
      ok: true,
      value: await __terrace_handler(__terrace_request),
    }};
  }} catch (error) {{
    const code =
      error && typeof error === "object" && typeof error.code === "string"
        ? error.code
        : "sandbox-rejected";
    const message =
      error && typeof error === "object" && typeof error.message === "string"
        ? error.message
        : String(error);
    const metadata =
      error && typeof error === "object" && "metadata" in error
        ? error.metadata
        : null;
    return __terrace_error(code, message, metadata);
  }}
}})();
"#
    ))
}

fn sandbox_error_to_task_error(error: SandboxError) -> WorkflowTaskError {
    match error {
        SandboxError::CapabilityDenied { specifier } => WorkflowTaskError::new(
            "capability-denied",
            format!("workflow sandbox capability denied: {specifier}"),
        ),
        SandboxError::Execution {
            entrypoint,
            message,
        } => WorkflowTaskError::new("sandbox-execution", format!("{entrypoint}: {message}")),
        other => WorkflowTaskError::new("sandbox-runtime", other.to_string()),
    }
}
