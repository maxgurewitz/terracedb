use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use terracedb_sandbox::{
    PackageCompatibilityMode, PackageInstallRequest, SandboxError, SandboxSession, TypeCheckRequest,
};
use terracedb_workflows_core::{
    WorkflowBundleKind, WorkflowBundleMetadata, WorkflowDeterministicContext,
    WorkflowDeterministicSeed, WorkflowHandlerContract, WorkflowObservationValue,
    WorkflowSandboxPackageCompatibility, WorkflowSandboxPreparation, WorkflowSandboxSourceKind,
    WorkflowSourceEvent, WorkflowTaskError, WorkflowTransitionInput, WorkflowTransitionOutput,
};
use tokio::sync::Mutex;

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

#[derive(Clone)]
pub struct SandboxModuleWorkflowTaskV1Handler {
    session: SandboxSession,
    bundle_id: String,
    module: String,
    entrypoint: String,
    preparation: WorkflowSandboxPreparation,
    prepared_module: Arc<Mutex<Option<PreparedSandboxModule>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreparedSandboxModule {
    session_revision: u64,
    runtime_module: String,
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
            preparation,
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
            preparation,
            prepared_module: Arc::new(Mutex::new(None)),
        })
    }

    async fn resolve_runtime_module(&self) -> Result<String, WorkflowTaskError> {
        let info = self.session.info().await;
        let package_compat = bundle_package_compat(&self.preparation);
        let source_kind = self.preparation.source_kind();
        if info.provenance.package_compat != package_compat {
            return Err(WorkflowTaskError::invalid_contract(format!(
                "sandbox bundle {} expects package compatibility {:?}, but session is {:?}",
                self.bundle_id, package_compat, info.provenance.package_compat
            )));
        }
        let session_revision = info.revision;
        {
            let prepared = self.prepared_module.lock().await;
            if let Some(prepared) = prepared.as_ref()
                && prepared.session_revision == session_revision
            {
                return Ok(prepared.runtime_module.clone());
            }
        }

        let runtime_module = match source_kind {
            WorkflowSandboxSourceKind::JavaScript => self.module.clone(),
            WorkflowSandboxSourceKind::TypeScript => {
                if let Some(package_manifest_path) = self.preparation.package_manifest_path.as_ref()
                {
                    install_manifest_packages(&self.session, package_manifest_path).await?;
                }
                let request = TypeCheckRequest {
                    roots: vec![self.module.clone()],
                    tsconfig_path: self.preparation.tsconfig_path.clone(),
                    ..Default::default()
                };
                let report = self
                    .session
                    .typecheck(request.clone())
                    .await
                    .map_err(sandbox_error_to_task_error)?;
                if !report.diagnostics.is_empty() {
                    let details = report
                        .diagnostics
                        .iter()
                        .map(|diagnostic| match diagnostic.code.as_deref() {
                            Some(code) => {
                                format!("{} {}: {}", diagnostic.path, code, diagnostic.message)
                            }
                            None => format!("{}: {}", diagnostic.path, diagnostic.message),
                        })
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(WorkflowTaskError::invalid_contract(format!(
                        "sandbox bundle {} TypeScript entrypoint failed validation: {}",
                        self.bundle_id, details
                    )));
                }
                self.session
                    .emit_typescript(request)
                    .await
                    .map_err(sandbox_error_to_task_error)?;
                emitted_output_path(&self.module)
            }
        };

        let prepared_revision = self.session.info().await.revision;
        let mut prepared = self.prepared_module.lock().await;
        *prepared = Some(PreparedSandboxModule {
            session_revision: prepared_revision,
            runtime_module: runtime_module.clone(),
        });
        Ok(runtime_module)
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
        let module = self.resolve_runtime_module().await?;
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

fn bundle_package_compat(preparation: &WorkflowSandboxPreparation) -> PackageCompatibilityMode {
    match preparation.package_compat() {
        WorkflowSandboxPackageCompatibility::TerraceOnly => PackageCompatibilityMode::TerraceOnly,
        WorkflowSandboxPackageCompatibility::NpmPureJs => PackageCompatibilityMode::NpmPureJs,
        WorkflowSandboxPackageCompatibility::NpmWithNodeBuiltins => {
            PackageCompatibilityMode::NpmWithNodeBuiltins
        }
    }
}

async fn install_manifest_packages(
    session: &SandboxSession,
    manifest_path: &str,
) -> Result<(), WorkflowTaskError> {
    let bytes = session
        .filesystem()
        .read_file(manifest_path)
        .await
        .map_err(sandbox_error_to_task_error)?
        .ok_or_else(|| {
            WorkflowTaskError::invalid_contract(format!(
                "sandbox workflow package manifest {manifest_path} does not exist"
            ))
        })?;
    let manifest: JsonValue = serde_json::from_slice(&bytes).map_err(|error| {
        WorkflowTaskError::serialization("sandbox workflow package manifest", error)
    })?;
    let mut packages = Vec::new();
    for section in ["dependencies", "devDependencies"] {
        if let Some(deps) = manifest.get(section).and_then(|value| value.as_object()) {
            for (name, version) in deps {
                match version.as_str().map(str::trim) {
                    Some(version) if !version.is_empty() => {
                        packages.push(format!("{name}@{version}"));
                    }
                    _ => packages.push(name.clone()),
                }
            }
        }
    }
    packages.sort();
    packages.dedup();
    if packages.is_empty() {
        return Ok(());
    }
    session
        .install_packages(PackageInstallRequest {
            packages,
            materialize_compatibility_view: true,
        })
        .await
        .map_err(sandbox_error_to_task_error)?;
    Ok(())
}

fn emitted_output_path(path: &str) -> String {
    if path.ends_with(".tsx") {
        return format!("{}{}", path.trim_end_matches(".tsx"), ".jsx");
    }
    if path.ends_with(".mts") {
        return format!("{}{}", path.trim_end_matches(".mts"), ".mjs");
    }
    if path.ends_with(".cts") {
        return format!("{}{}", path.trim_end_matches(".cts"), ".cjs");
    }
    if path.ends_with(".ts") {
        return format!("{}{}", path.trim_end_matches(".ts"), ".js");
    }
    if path.ends_with(".d.ts") {
        return format!("{}{}", path.trim_end_matches(".d.ts"), ".js");
    }
    path.to_string()
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
