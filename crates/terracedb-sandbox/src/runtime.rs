use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    rc::Rc,
    sync::Arc,
};

use async_trait::async_trait;
use boa_engine::{
    Context, JsNativeError, JsResult, JsString, JsValue, NativeFunction, Source, js_string,
    object::FunctionObjectBuilder, property::Attribute,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use terracedb_git::{GitCheckoutRequest, GitDiffRequest, GitRefUpdate, GitStatusOptions};
use terracedb_js::{
    BoaJsRuntimeHost, DeterministicJsEntropySource, FixedJsClock, JsCompatibilityProfile,
    JsExecutionKind, JsExecutionRequest, JsForkPolicy, JsHostServiceAdapter, JsHostServiceRequest,
    JsHostServiceResponse, JsHostServices, JsRuntime, JsRuntimeHost, JsRuntimeOpenRequest,
    JsRuntimePolicy, JsRuntimeProvenance, JsSubstrateError, NeverCancel, RoutedJsHostServices,
    VfsJsHostServiceAdapter,
};
use terracedb_vfs::{CreateOptions, FileKind, JsonValue, MkdirOptions};
use tokio::{sync::Mutex, task_local};
use tracing::{info_span, trace};

use crate::{
    CapabilityCallRequest, CapabilityCallResult, GIT_REMOTE_IMPORT_CAPABILITY_SPECIFIER,
    HOST_CAPABILITY_PREFIX, LoadedSandboxModule, PackageCompatibilityMode, SandboxError,
    SandboxModuleCacheEntry, SandboxModuleKind, SandboxModuleLoadTrace, SandboxSession,
    SandboxSessionInfo, TERRACE_RUNTIME_MODULE_CACHE_PATH,
    loader::{
        FS_HOST_EXPORTS, GIT_REMOTE_IMPORT_HOST_EXPORT, GIT_REPO_HOST_EXPORTS,
        SANDBOX_FS_LIBRARY_SPECIFIER, SANDBOX_GIT_LIBRARY_SPECIFIER, SandboxModuleLoader,
    },
    types::{has_sensitive_remote_bridge_metadata, sensitive_remote_bridge_metadata_keys},
};

task_local! {
    static ACTIVE_NODE_HOST: Rc<NodeRuntimeHost>;
}

const NODE_COMPAT_BOOTSTRAP_SOURCE: &str = include_str!("node_compat_bootstrap.js");
const NODE_RUNTIME_RESOLVE_BUDGET: u64 = 512;
const NODE_RUNTIME_LOAD_BUDGET: u64 = 512;
const NODE_RUNTIME_FS_BUDGET: u64 = 4096;
const NODE_RUNTIME_DEBUG_EVENT_LIMIT: usize = 128;
const KNOWN_NODE_BUILTIN_MODULES: &[&str] = &[
    "assert",
    "assert/strict",
    "async_hooks",
    "buffer",
    "child_process",
    "cluster",
    "console",
    "constants",
    "crypto",
    "dgram",
    "diagnostics_channel",
    "dns",
    "dns/promises",
    "domain",
    "events",
    "fs",
    "fs/promises",
    "http",
    "http2",
    "https",
    "inspector",
    "inspector/promises",
    "module",
    "net",
    "os",
    "path",
    "perf_hooks",
    "process",
    "punycode",
    "querystring",
    "readline",
    "readline/promises",
    "repl",
    "stream",
    "stream/consumers",
    "stream/promises",
    "stream/web",
    "string_decoder",
    "sys",
    "test",
    "timers",
    "timers/promises",
    "tls",
    "trace_events",
    "tty",
    "url",
    "util",
    "util/types",
    "v8",
    "vm",
    "wasi",
    "worker_threads",
    "zlib",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeResolvedKind {
    Builtin,
    CommonJs,
    Json,
}

#[derive(Clone, Debug)]
struct NodeLoadedModule {
    runtime_path: String,
    media_type: String,
    source: String,
    kind: NodeResolvedKind,
}

#[derive(Clone)]
struct NodeRuntimeHost {
    session: SandboxSession,
    workspace_root: String,
    process: Rc<RefCell<NodeProcessState>>,
    loaded_modules: Rc<RefCell<BTreeMap<String, NodeLoadedModule>>>,
    debug_trace: Rc<RefCell<NodeRuntimeDebugTrace>>,
}

#[derive(Clone, Debug, Default)]
struct NodeRuntimeDebugTrace {
    resolve_calls: u64,
    load_calls: u64,
    fs_calls: u64,
    recent_events: Vec<String>,
}

#[derive(Clone, Debug)]
struct NodeProcessState {
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    cwd: String,
    stdout: String,
    stderr: String,
    exit_code: i32,
    version: String,
    exec_path: String,
    platform: String,
    arch: String,
    title: String,
}

impl NodeProcessState {
    fn new(argv: Vec<String>, env: BTreeMap<String, String>, cwd: String) -> Self {
        Self {
            argv,
            env,
            cwd,
            stdout: String::new(),
            stderr: String::new(),
            exit_code: 0,
            version: "v24.12.0".to_string(),
            exec_path: "/usr/bin/node".to_string(),
            platform: "linux".to_string(),
            arch: "x64".to_string(),
            title: "node".to_string(),
        }
    }

    fn to_process_info_json(&self) -> serde_json::Value {
        serde_json::json!({
            "argv": self.argv,
            "env": self.env,
            "cwd": self.cwd,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exitCode": self.exit_code,
            "execPath": self.exec_path,
            "version": self.version,
            "platform": self.platform,
            "arch": self.arch,
            "title": self.title,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxRuntimeHandle {
    pub backend: String,
    pub actor_id: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SandboxExecutionKind {
    Module {
        specifier: String,
    },
    Eval {
        source: String,
        virtual_specifier: Option<String>,
    },
    NodeCommand {
        entrypoint: String,
        argv: Vec<String>,
        cwd: String,
        env: BTreeMap<String, String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxExecutionRequest {
    pub kind: SandboxExecutionKind,
    pub metadata: BTreeMap<String, JsonValue>,
}

impl SandboxExecutionRequest {
    pub fn module(specifier: impl Into<String>) -> Self {
        Self {
            kind: SandboxExecutionKind::Module {
                specifier: specifier.into(),
            },
            metadata: BTreeMap::new(),
        }
    }

    pub fn eval(source: impl Into<String>) -> Self {
        Self {
            kind: SandboxExecutionKind::Eval {
                source: source.into(),
                virtual_specifier: None,
            },
            metadata: BTreeMap::new(),
        }
    }

    pub fn node_command(
        entrypoint: impl Into<String>,
        argv: Vec<String>,
        cwd: impl Into<String>,
        env: BTreeMap<String, String>,
    ) -> Self {
        Self {
            kind: SandboxExecutionKind::NodeCommand {
                entrypoint: entrypoint.into(),
                argv,
                cwd: cwd.into(),
                env,
            },
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SandboxExecutionResult {
    pub backend: String,
    pub actor_id: String,
    pub entrypoint: String,
    pub result: Option<JsonValue>,
    pub module_graph: Vec<String>,
    pub package_imports: Vec<String>,
    pub cache_hits: Vec<String>,
    pub cache_misses: Vec<String>,
    pub cache_entries: Vec<SandboxModuleCacheEntry>,
    pub capability_calls: Vec<crate::CapabilityCallResult>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxRuntimeState {
    pub next_eval_id: u64,
    pub module_cache: BTreeMap<String, SandboxModuleCacheEntry>,
}

#[derive(Clone, Default)]
pub struct SandboxRuntimeStateHandle {
    inner: Arc<Mutex<SandboxRuntimeState>>,
}

impl SandboxRuntimeStateHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn snapshot(&self) -> SandboxRuntimeState {
        self.inner.lock().await.clone()
    }

    pub async fn next_eval_specifier(&self, workspace_root: &str) -> String {
        let mut state = self.inner.lock().await;
        state.next_eval_id = state.next_eval_id.saturating_add(1);
        format!(
            "terrace:{workspace_root}/.terrace/runtime/eval-{}.mjs",
            state.next_eval_id
        )
    }

    pub async fn is_module_cache_hit(&self, candidate: &SandboxModuleCacheEntry) -> bool {
        self.inner
            .lock()
            .await
            .module_cache
            .get(&candidate.specifier)
            .map(|existing| existing.cache_key == candidate.cache_key)
            .unwrap_or(false)
    }

    pub async fn upsert_module_cache(&self, entry: SandboxModuleCacheEntry) {
        self.inner
            .lock()
            .await
            .module_cache
            .insert(entry.specifier.clone(), entry);
    }

    pub async fn hydrate_module_cache(
        &self,
        entries: Vec<SandboxModuleCacheEntry>,
        next_eval_id: u64,
    ) {
        let mut state = self.inner.lock().await;
        for entry in entries {
            state.module_cache.insert(entry.specifier.clone(), entry);
        }
        state.next_eval_id = state.next_eval_id.max(next_eval_id);
    }
}

#[derive(Clone)]
pub struct SandboxRuntimeActor {
    backend: Arc<dyn SandboxRuntimeBackend>,
    handle: SandboxRuntimeHandle,
    execution_lock: Arc<Mutex<()>>,
    state: SandboxRuntimeStateHandle,
}

impl SandboxRuntimeActor {
    pub fn new(backend: Arc<dyn SandboxRuntimeBackend>, handle: SandboxRuntimeHandle) -> Self {
        Self {
            backend,
            handle,
            execution_lock: Arc::new(Mutex::new(())),
            state: SandboxRuntimeStateHandle::new(),
        }
    }

    pub fn handle(&self) -> SandboxRuntimeHandle {
        self.handle.clone()
    }

    pub fn state(&self) -> SandboxRuntimeStateHandle {
        self.state.clone()
    }

    pub async fn execute(
        &self,
        session: &SandboxSession,
        request: SandboxExecutionRequest,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        let _guard = self.execution_lock.lock().await;
        self.backend
            .execute(session, &self.handle, request, self.state.clone())
            .await
    }
}

#[async_trait(?Send)]
pub trait SandboxRuntimeBackend: Send + Sync {
    fn name(&self) -> &str;
    async fn start_session(
        &self,
        session: &SandboxSessionInfo,
    ) -> Result<SandboxRuntimeHandle, SandboxError>;
    async fn resume_session(
        &self,
        session: &SandboxSessionInfo,
    ) -> Result<SandboxRuntimeHandle, SandboxError>;
    async fn execute(
        &self,
        session: &SandboxSession,
        handle: &SandboxRuntimeHandle,
        request: SandboxExecutionRequest,
        state: SandboxRuntimeStateHandle,
    ) -> Result<SandboxExecutionResult, SandboxError>;
    async fn close_session(
        &self,
        session: &SandboxSessionInfo,
        handle: &SandboxRuntimeHandle,
    ) -> Result<(), SandboxError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicRuntimeBackend {
    name: String,
}

impl DeterministicRuntimeBackend {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for DeterministicRuntimeBackend {
    fn default() -> Self {
        Self::new("deterministic-runtime")
    }
}

#[async_trait(?Send)]
impl SandboxRuntimeBackend for DeterministicRuntimeBackend {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start_session(
        &self,
        session: &SandboxSessionInfo,
    ) -> Result<SandboxRuntimeHandle, SandboxError> {
        Ok(SandboxRuntimeHandle {
            backend: self.name.clone(),
            actor_id: format!("{}:{}:open", self.name, session.session_volume_id),
            metadata: BTreeMap::from([(
                "session_revision".to_string(),
                JsonValue::from(session.revision),
            )]),
        })
    }

    async fn resume_session(
        &self,
        session: &SandboxSessionInfo,
    ) -> Result<SandboxRuntimeHandle, SandboxError> {
        Ok(SandboxRuntimeHandle {
            backend: self.name.clone(),
            actor_id: format!("{}:{}:resume", self.name, session.session_volume_id),
            metadata: BTreeMap::from([(
                "session_revision".to_string(),
                JsonValue::from(session.revision),
            )]),
        })
    }

    async fn execute(
        &self,
        session: &SandboxSession,
        handle: &SandboxRuntimeHandle,
        request: SandboxExecutionRequest,
        state: SandboxRuntimeStateHandle,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        let session_info = session.info().await;
        hydrate_runtime_cache_state(session, &session_info, &state).await?;
        if let SandboxExecutionKind::NodeCommand {
            entrypoint,
            argv,
            cwd,
            env,
        } = request.kind.clone()
        {
            return execute_node_command(
                &self.name,
                session,
                handle,
                &session_info,
                &state,
                entrypoint,
                argv,
                cwd,
                env,
            )
            .await;
        }

        let loader = session
            .module_loader_with_state(session_info.clone(), state.clone())
            .await;
        let capability_services =
            Arc::new(SandboxCapabilityHostServiceAdapter::new(session.clone()));
        let host_services: Arc<dyn JsHostServices> = Arc::new(
            RoutedJsHostServices::new()
                .with_adapter(Arc::new(VfsJsHostServiceAdapter::new(
                    session.volume().fs(),
                    [SANDBOX_FS_LIBRARY_SPECIFIER, "node:fs", "node:fs/promises"],
                )))
                .with_adapter(Arc::new(SandboxGitHostServiceAdapter::new(session.clone())))
                .with_adapter(capability_services.clone()),
        );
        let (entrypoint, js_request, inline_eval_cache) =
            prepare_js_execution_request(&request, &session_info, &state, &loader).await?;
        let runtime = open_js_runtime(
            handle,
            &session_info,
            session,
            loader.clone(),
            host_services,
        )
        .await?;
        let report = runtime
            .execute(js_request, Arc::new(NeverCancel))
            .await
            .map_err(|error| js_substrate_error_to_sandbox(error, &entrypoint))?;
        runtime
            .close()
            .await
            .map_err(|error| js_substrate_error_to_sandbox(error, &entrypoint))?;

        let mut trace = loader.trace_snapshot().await;
        if let Some((cache_entry, was_hit)) = inline_eval_cache {
            merge_inline_eval_trace(&mut trace, cache_entry, was_hit);
        }
        persist_runtime_cache(session, &state).await?;

        let module_graph = if report.module_graph.is_empty() {
            trace.module_graph.clone()
        } else {
            report.module_graph
        };
        let package_import_count = trace.package_imports.len();

        Ok(SandboxExecutionResult {
            backend: self.name.clone(),
            actor_id: handle.actor_id.clone(),
            entrypoint,
            result: report.result,
            module_graph: module_graph.clone(),
            package_imports: trace.package_imports,
            cache_hits: trace.cache_hits,
            cache_misses: trace.cache_misses,
            cache_entries: trace.cache_entries,
            capability_calls: capability_services.capability_calls().await,
            metadata: BTreeMap::from([
                (
                    "session_revision".to_string(),
                    JsonValue::from(session_info.revision),
                ),
                (
                    "workspace_root".to_string(),
                    JsonValue::from(session_info.workspace_root),
                ),
                (
                    "module_count".to_string(),
                    JsonValue::from(module_graph.len()),
                ),
                (
                    "package_import_count".to_string(),
                    JsonValue::from(package_import_count),
                ),
                ("js_backend".to_string(), JsonValue::from(report.backend)),
            ]),
        })
    }

    async fn close_session(
        &self,
        _session: &SandboxSessionInfo,
        _handle: &SandboxRuntimeHandle,
    ) -> Result<(), SandboxError> {
        Ok(())
    }
}

#[derive(Clone)]
struct SandboxCapabilityHostServiceAdapter {
    session: SandboxSession,
    capability_calls: Arc<Mutex<Vec<CapabilityCallResult>>>,
}

impl SandboxCapabilityHostServiceAdapter {
    fn new(session: SandboxSession) -> Self {
        Self {
            session,
            capability_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn capability_calls(&self) -> Vec<CapabilityCallResult> {
        self.capability_calls.lock().await.clone()
    }

    async fn call_async(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        if !request.service.starts_with(HOST_CAPABILITY_PREFIX) {
            return Err(JsSubstrateError::HostServiceUnavailable {
                service: request.service,
                operation: request.operation,
            });
        }
        self.call_capability(request).await
    }

    async fn call_capability(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        let result = self
            .session
            .invoke_capability(CapabilityCallRequest {
                specifier: request.service.clone(),
                method: request.operation.clone(),
                args: host_service_arguments(&request.arguments),
            })
            .await
            .map_err(|error| match error {
                SandboxError::CapabilityDenied { .. } => JsSubstrateError::HostServiceDenied {
                    service: request.service.clone(),
                    operation: request.operation.clone(),
                    message: error.to_string(),
                },
                SandboxError::CapabilityUnavailable { .. } => {
                    JsSubstrateError::HostServiceUnavailable {
                        service: request.service.clone(),
                        operation: request.operation.clone(),
                    }
                }
                other => JsSubstrateError::EvaluationFailed {
                    entrypoint: format!("{}::{}", request.service, request.operation),
                    message: other.to_string(),
                },
            })?;
        self.capability_calls.lock().await.push(result.clone());
        Ok(JsHostServiceResponse {
            result: Some(result.value),
            metadata: result.metadata,
        })
    }
}

#[async_trait(?Send)]
impl JsHostServiceAdapter for SandboxCapabilityHostServiceAdapter {
    fn handles_service(&self, service: &str) -> bool {
        service.starts_with(HOST_CAPABILITY_PREFIX)
    }

    async fn call(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        self.call_async(request).await
    }
}

#[derive(Clone)]
struct SandboxGitHostServiceAdapter {
    session: SandboxSession,
}

impl SandboxGitHostServiceAdapter {
    fn new(session: SandboxSession) -> Self {
        Self { session }
    }

    async fn call_async(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        if request.service != SANDBOX_GIT_LIBRARY_SPECIFIER {
            return Err(JsSubstrateError::HostServiceUnavailable {
                service: request.service,
                operation: request.operation,
            });
        }
        match request.operation.as_str() {
            "importRepository" => {
                let args = host_service_argument::<GitHostImportRepositoryRequest>(&request)?;
                if !self
                    .session
                    .info()
                    .await
                    .provenance
                    .capabilities
                    .contains(GIT_REMOTE_IMPORT_CAPABILITY_SPECIFIER)
                {
                    return Err(JsSubstrateError::HostServiceDenied {
                        service: request.service.clone(),
                        operation: request.operation.clone(),
                        message:
                            "remote repository import requires the git-remote-import capability"
                                .to_string(),
                    });
                }
                if has_sensitive_remote_bridge_metadata(&args.metadata) {
                    return Err(JsSubstrateError::HostServiceDenied {
                        service: request.service.clone(),
                        operation: request.operation.clone(),
                        message: format!(
                            "remote bridge credentials must be configured by the host app, not sandbox code (blocked keys: {})",
                            sensitive_remote_bridge_metadata_keys(&args.metadata).join(", ")
                        ),
                    });
                }
                let report = self
                    .session
                    .import_remote_repository(crate::GitRemoteImportRequest {
                        remote_url: args.remote_url,
                        reference: args.reference,
                        metadata: args.metadata,
                        target_root: args.target_root,
                        delete_missing: args.delete_missing,
                    })
                    .await
                    .map_err(|error| sandbox_error_to_js_host_service(&request, error))?;
                Ok(JsHostServiceResponse {
                    result: Some(serde_json::to_value(report)?),
                    metadata: BTreeMap::new(),
                })
            }
            "head" => Ok(JsHostServiceResponse {
                result: Some(serde_json::to_value(
                    self.session
                        .git_head()
                        .await
                        .map_err(|error| sandbox_error_to_js_host_service(&request, error))?,
                )?),
                metadata: BTreeMap::new(),
            }),
            "listRefs" => Ok(JsHostServiceResponse {
                result: Some(serde_json::to_value(
                    self.session
                        .git_list_refs()
                        .await
                        .map_err(|error| sandbox_error_to_js_host_service(&request, error))?,
                )?),
                metadata: BTreeMap::new(),
            }),
            "status" => Ok(JsHostServiceResponse {
                result: Some(serde_json::to_value(
                    self.session
                        .git_status(
                            host_service_optional_argument::<GitHostStatusRequest>(&request)?
                                .into(),
                        )
                        .await
                        .map_err(|error| sandbox_error_to_js_host_service(&request, error))?,
                )?),
                metadata: BTreeMap::new(),
            }),
            "diff" => Ok(JsHostServiceResponse {
                result: Some(serde_json::to_value(
                    self.session
                        .git_diff(
                            host_service_optional_argument::<GitHostDiffRequest>(&request)?.into(),
                        )
                        .await
                        .map_err(|error| sandbox_error_to_js_host_service(&request, error))?,
                )?),
                metadata: BTreeMap::new(),
            }),
            "checkout" => Ok(JsHostServiceResponse {
                result: Some(serde_json::to_value(
                    self.session
                        .git_checkout(
                            host_service_argument::<GitHostCheckoutRequest>(&request)?.into(),
                        )
                        .await
                        .map_err(|error| sandbox_error_to_js_host_service(&request, error))?,
                )?),
                metadata: BTreeMap::new(),
            }),
            "updateRef" => Ok(JsHostServiceResponse {
                result: Some(serde_json::to_value(
                    self.session
                        .git_update_ref(
                            host_service_argument::<GitHostUpdateRefRequest>(&request)?.into(),
                        )
                        .await
                        .map_err(|error| sandbox_error_to_js_host_service(&request, error))?,
                )?),
                metadata: BTreeMap::new(),
            }),
            "createPullRequest" => {
                let args = host_service_argument::<GitHostCreatePullRequestRequest>(&request)?;
                let report = self
                    .session
                    .create_pull_request(crate::PullRequestRequest {
                        title: args.title,
                        body: args.body,
                        head_branch: args.head_branch,
                        base_branch: args.base_branch,
                    })
                    .await
                    .map_err(|error| sandbox_error_to_js_host_service(&request, error))?;
                Ok(JsHostServiceResponse {
                    result: Some(serde_json::to_value(report)?),
                    metadata: BTreeMap::new(),
                })
            }
            _ => Err(JsSubstrateError::HostServiceUnavailable {
                service: request.service,
                operation: request.operation,
            }),
        }
    }
}

#[async_trait(?Send)]
impl JsHostServiceAdapter for SandboxGitHostServiceAdapter {
    fn handles_service(&self, service: &str) -> bool {
        service == SANDBOX_GIT_LIBRARY_SPECIFIER
    }

    async fn call(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        self.call_async(request).await
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GitHostImportRepositoryRequest {
    remote_url: String,
    reference: Option<String>,
    #[serde(default)]
    metadata: BTreeMap<String, JsonValue>,
    target_root: String,
    #[serde(default)]
    delete_missing: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GitHostCreatePullRequestRequest {
    title: String,
    body: String,
    head_branch: String,
    base_branch: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct GitHostStatusRequest {
    pathspec: Vec<String>,
    include_untracked: Option<bool>,
    include_ignored: Option<bool>,
}

impl From<GitHostStatusRequest> for GitStatusOptions {
    fn from(value: GitHostStatusRequest) -> Self {
        let defaults = GitStatusOptions::default();
        Self {
            pathspec: value.pathspec,
            include_untracked: value
                .include_untracked
                .unwrap_or(defaults.include_untracked),
            include_ignored: value.include_ignored.unwrap_or(defaults.include_ignored),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct GitHostDiffRequest {
    pathspec: Vec<String>,
    include_untracked: Option<bool>,
    include_ignored: Option<bool>,
}

impl From<GitHostDiffRequest> for GitDiffRequest {
    fn from(value: GitHostDiffRequest) -> Self {
        let defaults = GitDiffRequest::default();
        Self {
            pathspec: value.pathspec,
            include_untracked: value
                .include_untracked
                .unwrap_or(defaults.include_untracked),
            include_ignored: value.include_ignored.unwrap_or(defaults.include_ignored),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GitHostCheckoutRequest {
    target_ref: String,
    materialize_path: String,
    #[serde(default)]
    pathspec: Vec<String>,
    #[serde(default)]
    update_head: bool,
}

impl From<GitHostCheckoutRequest> for GitCheckoutRequest {
    fn from(value: GitHostCheckoutRequest) -> Self {
        Self {
            target_ref: value.target_ref,
            materialize_path: value.materialize_path,
            pathspec: value.pathspec,
            update_head: value.update_head,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GitHostUpdateRefRequest {
    name: String,
    target: String,
    #[serde(default)]
    previous_target: Option<String>,
    #[serde(default)]
    symbolic: bool,
}

impl From<GitHostUpdateRefRequest> for GitRefUpdate {
    fn from(value: GitHostUpdateRefRequest) -> Self {
        Self {
            name: value.name,
            target: value.target,
            previous_target: value.previous_target,
            symbolic: value.symbolic,
        }
    }
}

async fn execute_node_command(
    runtime_name: &str,
    session: &SandboxSession,
    handle: &SandboxRuntimeHandle,
    session_info: &SandboxSessionInfo,
    state: &SandboxRuntimeStateHandle,
    entrypoint: String,
    argv: Vec<String>,
    cwd: String,
    env: BTreeMap<String, String>,
) -> Result<SandboxExecutionResult, SandboxError> {
    let normalized_entrypoint = resolve_node_path(&cwd, &entrypoint);
    let node_host = Rc::new(NodeRuntimeHost {
        session: session.clone(),
        workspace_root: session_info.workspace_root.clone(),
        process: Rc::new(RefCell::new(NodeProcessState::new(argv, env, cwd))),
        loaded_modules: Rc::new(RefCell::new(BTreeMap::new())),
        debug_trace: Rc::new(RefCell::new(NodeRuntimeDebugTrace::default())),
    });
    ACTIVE_NODE_HOST
        .scope(node_host.clone(), async move {
            let span = info_span!(
                "terracedb.sandbox.node_command",
                entrypoint = %normalized_entrypoint,
                cwd = %node_host.process.borrow().cwd
            );
            let _guard = span.enter();

            let mut context = Context::builder()
                .build()
                .map_err(|error| SandboxError::Execution {
                    entrypoint: normalized_entrypoint.clone(),
                    message: error.to_string(),
                })?;
            install_node_host_bindings(&mut context)?;
            context
                .eval(
                    Source::from_bytes(NODE_COMPAT_BOOTSTRAP_SOURCE.as_bytes())
                        .with_path(&PathBuf::from("/__terrace__/node/bootstrap.js")),
                )
                .map_err(|error| node_execution_error(&normalized_entrypoint, &node_host, error))?;

            let request = JsValue::from_json(
                &serde_json::json!({ "entrypoint": normalized_entrypoint }),
                &mut context,
            )
            .map_err(|error| node_execution_error(&entrypoint, &node_host, error))?;
            let runner = context
                .global_object()
                .get(js_string!("__terraceRunNodeCommand"), &mut context)
                .map_err(|error| node_execution_error(&entrypoint, &node_host, error))?;
            let runner = runner
                .as_callable()
                .ok_or_else(|| SandboxError::Execution {
                    entrypoint: entrypoint.clone(),
                    message: "__terraceRunNodeCommand is not callable".to_string(),
                })?;
            let report = runner
                .call(&JsValue::undefined(), &[request], &mut context)
                .map_err(|error| node_execution_error(&entrypoint, &node_host, error))?;
            context
                .run_jobs()
                .map_err(|error| node_execution_error(&entrypoint, &node_host, error))?;
            let result = report
                .to_json(&mut context)
                .map_err(|error| node_execution_error(&entrypoint, &node_host, error))?;

            persist_runtime_cache(session, state).await?;

            let module_graph = node_host
                .loaded_modules
                .borrow()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            let package_import_count = module_graph
                .iter()
                .filter(|path| path.contains("/node_modules/"))
                .count();

            Ok(SandboxExecutionResult {
                backend: runtime_name.to_string(),
                actor_id: handle.actor_id.clone(),
                entrypoint: entrypoint.clone(),
                result,
                module_graph: module_graph.clone(),
                package_imports: Vec::new(),
                cache_hits: Vec::new(),
                cache_misses: Vec::new(),
                cache_entries: state.snapshot().await.module_cache.into_values().collect(),
                capability_calls: Vec::new(),
                metadata: BTreeMap::from([
                    (
                        "session_revision".to_string(),
                        JsonValue::from(session_info.revision),
                    ),
                    (
                        "workspace_root".to_string(),
                        JsonValue::from(session_info.workspace_root.clone()),
                    ),
                    (
                        "module_count".to_string(),
                        JsonValue::from(module_graph.len()),
                    ),
                    (
                        "package_import_count".to_string(),
                        JsonValue::from(package_import_count),
                    ),
                    (
                        "node_runtime_trace".to_string(),
                        JsonValue::Array(
                            node_host
                                .debug_trace
                                .borrow()
                                .recent_events
                                .iter()
                                .cloned()
                                .map(JsonValue::from)
                                .collect(),
                        ),
                    ),
                ]),
            })
        })
        .await
}

fn install_node_host_bindings(context: &mut Context) -> Result<(), SandboxError> {
    register_global_native(context, "__terraceGetProcessInfo", 0, node_get_process_info)?;
    register_global_native(context, "__terraceGetCwd", 0, node_get_cwd)?;
    register_global_native(context, "__terraceChdir", 1, node_chdir)?;
    register_global_native(context, "__terraceSetExitCode", 1, node_set_exit_code)?;
    register_global_native(context, "__terraceWriteStdout", 1, node_write_stdout)?;
    register_global_native(context, "__terraceWriteStderr", 1, node_write_stderr)?;
    register_global_native(context, "__terraceResolveModule", 2, node_resolve_module)?;
    register_global_native(
        context,
        "__terraceReadModuleSource",
        1,
        node_read_module_source,
    )?;
    register_global_native(
        context,
        "__terraceFsReadTextFile",
        1,
        node_fs_read_text_file,
    )?;
    register_global_native(
        context,
        "__terraceFsWriteTextFile",
        2,
        node_fs_write_text_file,
    )?;
    register_global_native(context, "__terraceFsMkdir", 1, node_fs_mkdir)?;
    register_global_native(context, "__terraceFsReaddir", 1, node_fs_readdir)?;
    register_global_native(context, "__terraceFsStat", 1, node_fs_stat)?;
    register_global_native(context, "__terraceFsRealpath", 1, node_fs_realpath)?;
    register_global_native(context, "__terraceFsUnlink", 1, node_fs_unlink)?;
    register_global_native(context, "__terraceFsRename", 2, node_fs_rename)?;
    Ok(())
}

fn register_global_native(
    context: &mut Context,
    name: &str,
    length: usize,
    function: fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>,
) -> Result<(), SandboxError> {
    let function =
        FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
    context
        .register_global_property(JsString::from(name), function, Attribute::all())
        .map_err(|error| SandboxError::Service {
            service: "runtime",
            message: format!("failed to register node host binding `{name}`: {error}"),
        })
}

fn node_with_host<T>(
    f: impl FnOnce(&NodeRuntimeHost) -> Result<T, SandboxError>,
) -> Result<T, SandboxError> {
    ACTIVE_NODE_HOST
        .try_with(|host| f(host))
        .map_err(|_| SandboxError::Service {
            service: "runtime",
            message: "node host bindings are unavailable outside node command execution"
                .to_string(),
        })?
}

fn node_with_host_js(
    context: &mut Context,
    f: impl FnOnce(&NodeRuntimeHost, &mut Context) -> Result<JsValue, SandboxError>,
) -> JsResult<JsValue> {
    node_with_host(|host| f(host, context)).map_err(js_error)
}

fn node_debug_event(
    host: &NodeRuntimeHost,
    bucket: &str,
    detail: impl Into<String>,
) -> Result<(), SandboxError> {
    let detail = detail.into();
    trace!(target: "terracedb.sandbox.node_runtime", bucket, detail = %detail);
    let mut state = host.debug_trace.borrow_mut();
    match bucket {
        "resolve" => {
            state.resolve_calls = state.resolve_calls.saturating_add(1);
            if state.resolve_calls > NODE_RUNTIME_RESOLVE_BUDGET {
                return Err(SandboxError::Service {
                    service: "node_runtime",
                    message: format!(
                        "ERR_TERRACE_NODE_RESOLVE_BUDGET: exceeded runtime resolve budget ({NODE_RUNTIME_RESOLVE_BUDGET}) while {detail}; recent trace: {}",
                        state.recent_events.join(" | ")
                    ),
                });
            }
        }
        "load" => {
            state.load_calls = state.load_calls.saturating_add(1);
            if state.load_calls > NODE_RUNTIME_LOAD_BUDGET {
                return Err(SandboxError::Service {
                    service: "node_runtime",
                    message: format!(
                        "ERR_TERRACE_NODE_LOAD_BUDGET: exceeded runtime load budget ({NODE_RUNTIME_LOAD_BUDGET}) while {detail}; recent trace: {}",
                        state.recent_events.join(" | ")
                    ),
                });
            }
        }
        "fs" => {
            state.fs_calls = state.fs_calls.saturating_add(1);
            if state.fs_calls > NODE_RUNTIME_FS_BUDGET {
                return Err(SandboxError::Service {
                    service: "node_runtime",
                    message: format!(
                        "ERR_TERRACE_NODE_FS_BUDGET: exceeded runtime fs budget ({NODE_RUNTIME_FS_BUDGET}) while {detail}; recent trace: {}",
                        state.recent_events.join(" | ")
                    ),
                });
            }
        }
        _ => {}
    }
    if state.recent_events.len() == NODE_RUNTIME_DEBUG_EVENT_LIMIT {
        state.recent_events.remove(0);
    }
    state.recent_events.push(format!("{bucket}:{detail}"));
    Ok(())
}

fn node_arg_string(args: &[JsValue], index: usize, context: &mut Context) -> JsResult<String> {
    args.get(index)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)
        .map(|value| value.to_std_string_escaped())
}

fn node_get_process_info(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        JsValue::from_json(&host.process.borrow().to_process_info_json(), context)
            .map_err(sandbox_execution_error)
    })
}

fn node_get_cwd(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(JsString::from(host.process.borrow().cwd.clone()))))
        .map_err(js_error)
}

fn node_chdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        host.process.borrow_mut().cwd = resolve_node_path(&host.process.borrow().cwd, &path);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_set_exit_code(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let code = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .map_err(|error| js_error(SandboxError::Service {
            service: "runtime",
            message: format!("invalid exit code: {error}"),
        }))?;
    node_with_host(|host| {
        host.process.borrow_mut().exit_code = code;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_write_stdout(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let chunk = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        host.process.borrow_mut().stdout.push_str(&chunk);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_write_stderr(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let chunk = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        host.process.borrow_mut().stderr.push_str(&chunk);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_resolve_module(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let specifier = node_arg_string(args, 0, context)?;
    let referrer = args
        .get(1)
        .filter(|value| !value.is_null() && !value.is_undefined())
        .map(|value| value.to_string(context))
        .transpose()?
        .map(|value| value.to_std_string_escaped());
    node_with_host_js(context, |host, context| {
        node_debug_event(
            host,
            "resolve",
            format!(
                "resolve_module specifier={specifier} referrer={}",
                referrer.as_deref().unwrap_or("<root>")
            ),
        )?;
        if let Some(builtin) = node_builtin_name(&specifier) {
            return JsValue::from_json(
                &serde_json::json!({
                    "id": builtin,
                    "kind": "builtin",
                    "specifier": format!("node:{builtin}"),
                }),
                context,
            )
            .map_err(sandbox_execution_error);
        }

        let loaded = resolve_node_loaded_module(host, &specifier, referrer.as_deref())?;
        let id = loaded.runtime_path.clone();
        let kind = match loaded.kind {
            NodeResolvedKind::Builtin => "builtin",
            NodeResolvedKind::CommonJs => "commonjs",
            NodeResolvedKind::Json => "json",
        };
        host.loaded_modules.borrow_mut().insert(id.clone(), loaded);
        JsValue::from_json(
            &serde_json::json!({
                "id": id,
                "specifier": id,
                "kind": kind,
            }),
            context,
        )
        .map_err(sandbox_execution_error)
    })
}

fn node_read_module_source(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let module_id = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "load", format!("read_module_source {module_id}"))?;
        let loaded_modules = host.loaded_modules.borrow();
        let loaded =
            loaded_modules
                .get(&module_id)
                .ok_or_else(|| SandboxError::ModuleNotFound {
                    specifier: module_id.clone(),
                })?;
        Ok(JsValue::from(JsString::from(strip_shebang(
            &loaded.source,
        ))))
    })
    .map_err(js_error)
}

fn node_fs_read_text_file(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("read_text_file {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let bytes = futures::executor::block_on(host.session.filesystem().read_file(&path))?
            .ok_or_else(|| SandboxError::ModuleNotFound {
                specifier: path.clone(),
            })?;
        let text = String::from_utf8(bytes).map_err(|error| SandboxError::Execution {
            entrypoint: path.clone(),
            message: error.to_string(),
        })?;
        Ok(JsValue::from(JsString::from(text)))
    })
    .map_err(js_error)
}

fn node_fs_write_text_file(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let data = node_arg_string(args, 1, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("write_text_file {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        futures::executor::block_on(host.session.filesystem().write_file(
            &path,
            data.into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        ))?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_fs_mkdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("mkdir {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        futures::executor::block_on(host.session.filesystem().mkdir(
            &path,
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        ))?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_fs_readdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "fs", format!("readdir {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let entries = futures::executor::block_on(host.session.filesystem().readdir(&path))?;
        let json = serde_json::Value::Array(
            entries
                .into_iter()
                .map(|entry| serde_json::json!({ "name": entry.name }))
                .collect(),
        );
        JsValue::from_json(&json, context).map_err(sandbox_execution_error)
    })
}

fn node_fs_stat(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "fs", format!("stat {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let stats = futures::executor::block_on(host.session.filesystem().stat(&path))?;
        match stats {
            Some(stats) => JsValue::from_json(&node_stats_to_json(stats), context)
                .map_err(sandbox_execution_error),
            None => Ok(JsValue::null()),
        }
    })
}

fn node_fs_realpath(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("realpath {path}"))?;
        Ok(JsValue::from(JsString::from(resolve_node_path(
            &host.process.borrow().cwd,
            &path,
        ))))
    })
    .map_err(js_error)
}

fn node_fs_unlink(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("unlink {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let stats = futures::executor::block_on(host.session.filesystem().stat(&path))?;
        match stats.map(|stats| stats.kind) {
            Some(FileKind::Directory) => {
                futures::executor::block_on(host.session.filesystem().rmdir(&path))?;
            }
            Some(_) => {
                futures::executor::block_on(host.session.filesystem().unlink(&path))?;
            }
            None => {}
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_fs_rename(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let from = node_arg_string(args, 0, context)?;
    let to = node_arg_string(args, 1, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("rename {from} -> {to}"))?;
        futures::executor::block_on(host.session.filesystem().rename(
            &resolve_node_path(&host.process.borrow().cwd, &from),
            &resolve_node_path(&host.process.borrow().cwd, &to),
        ))?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_builtin_name(specifier: &str) -> Option<String> {
    let normalized = specifier.strip_prefix("node:").unwrap_or(specifier);
    KNOWN_NODE_BUILTIN_MODULES
        .contains(&normalized)
        .then(|| normalized.to_string())
}

fn resolve_node_loaded_module(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
) -> Result<NodeLoadedModule, SandboxError> {
    let resolved_path = resolve_node_module_path(host, specifier, referrer)?;
    load_node_module_from_path(host, &resolved_path)
}

fn resolve_node_module_path(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
) -> Result<String, SandboxError> {
    if specifier.starts_with('/') {
        return resolve_as_file_or_directory(host, &normalize_node_path(specifier))?.ok_or_else(
            || SandboxError::ModuleNotFound {
                specifier: specifier.to_string(),
            },
        );
    }

    if specifier.starts_with("./") || specifier.starts_with("../") {
        let base = referrer
            .map(directory_for_path)
            .unwrap_or_else(|| host.process.borrow().cwd.clone());
        return resolve_as_file_or_directory(host, &resolve_node_path(&base, specifier))?
            .ok_or_else(|| SandboxError::ModuleNotFound {
                specifier: specifier.to_string(),
            });
    }

    if specifier.starts_with('#') {
        return Err(SandboxError::Service {
            service: "node_runtime",
            message: format!(
                "ERR_TERRACE_NODE_UNIMPLEMENTED: package imports are not implemented for {specifier}"
            ),
        });
    }

    let base = referrer
        .map(directory_for_path)
        .unwrap_or_else(|| host.process.borrow().cwd.clone());
    let (package_name, subpath) = split_package_request(specifier)?;
    for directory in ancestor_directories(&base, &host.workspace_root) {
        let package_root = normalize_node_path(&format!("{directory}/node_modules/{package_name}"));
        if let Some(resolved) =
            resolve_package_entry_from_directory(host, &package_root, subpath.as_deref())?
        {
            return Ok(resolved);
        }
    }
    Err(SandboxError::ModuleNotFound {
        specifier: specifier.to_string(),
    })
}

fn resolve_package_entry_from_directory(
    host: &NodeRuntimeHost,
    package_root: &str,
    subpath: Option<&str>,
) -> Result<Option<String>, SandboxError> {
    node_debug_event(
        host,
        "resolve",
        format!(
            "package_entry package_root={package_root} subpath={}",
            subpath.unwrap_or("<root>")
        ),
    )?;
    node_debug_event(host, "fs", format!("stat {package_root}"))?;
    let Some(stats) = futures::executor::block_on(host.session.filesystem().stat(package_root))?
    else {
        return Ok(None);
    };
    if stats.kind != FileKind::Directory {
        return Ok(None);
    }

    if let Some(subpath) = subpath {
        return resolve_as_file_or_directory(host, &normalize_node_path(&format!(
            "{package_root}/{subpath}"
        )));
    }

    let package_json_path = normalize_node_path(&format!("{package_root}/package.json"));
    if let Some(package_json) = read_package_json(host, &package_json_path)? {
        if let Some(target) = package_json
            .get("exports")
            .and_then(select_package_exports_target)
        {
            let candidate = normalize_node_path(&format!("{package_root}/{target}"));
            if let Some(resolved) = resolve_as_file_or_directory(host, &candidate)? {
                return Ok(Some(resolved));
            }
        }

        if let Some(target) = package_json.get("main").and_then(|value| value.as_str()) {
            let candidate = normalize_node_path(&format!("{package_root}/{target}"));
            if let Some(resolved) = resolve_as_file_or_directory(host, &candidate)? {
                return Ok(Some(resolved));
            }
        }
    }

    resolve_as_file_or_directory(host, &normalize_node_path(&format!("{package_root}/index")))
}

fn resolve_as_file_or_directory(
    host: &NodeRuntimeHost,
    base: &str,
) -> Result<Option<String>, SandboxError> {
    node_debug_event(host, "resolve", format!("file_or_directory base={base}"))?;
    for candidate in candidate_module_paths(base) {
        node_debug_event(host, "fs", format!("stat {candidate}"))?;
        let Some(stats) = futures::executor::block_on(host.session.filesystem().stat(&candidate))?
        else {
            continue;
        };
        match stats.kind {
            FileKind::File => return Ok(Some(candidate)),
            FileKind::Directory => {
                if let Some(resolved) = resolve_package_entry_from_directory(host, &candidate, None)?
                {
                    return Ok(Some(resolved));
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

fn load_node_module_from_path(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<NodeLoadedModule, SandboxError> {
    node_debug_event(host, "load", format!("load_module {path}"))?;
    let bytes = futures::executor::block_on(host.session.filesystem().read_file(path))?
        .ok_or_else(|| SandboxError::ModuleNotFound {
            specifier: path.to_string(),
        })?;
    let source = String::from_utf8(bytes).map_err(|error| SandboxError::Execution {
        entrypoint: path.to_string(),
        message: error.to_string(),
    })?;
    let media_type = match PathBuf::from(path).extension().and_then(|ext| ext.to_str()) {
        Some("json") => "application/json",
        _ => "text/javascript",
    }
    .to_string();
    Ok(NodeLoadedModule {
        runtime_path: path.to_string(),
        media_type: media_type.clone(),
        kind: if media_type == "application/json" {
            NodeResolvedKind::Json
        } else {
            NodeResolvedKind::CommonJs
        },
        source,
    })
}

fn read_package_json(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<Option<serde_json::Value>, SandboxError> {
    node_debug_event(host, "fs", format!("read_file {path}"))?;
    let Some(bytes) = futures::executor::block_on(host.session.filesystem().read_file(path))?
    else {
        return Ok(None);
    };
    let text = String::from_utf8(bytes).map_err(|error| SandboxError::Service {
        service: "node_runtime",
        message: format!("package manifest {path} is not valid UTF-8: {error}"),
    })?;
    let parsed = serde_json::from_str(&text).map_err(|error| SandboxError::Service {
        service: "node_runtime",
        message: format!("package manifest {path} is invalid JSON: {error}"),
    })?;
    Ok(Some(parsed))
}

fn split_package_request(specifier: &str) -> Result<(String, Option<String>), SandboxError> {
    if specifier.is_empty() {
        return Err(SandboxError::InvalidModuleSpecifier {
            specifier: specifier.to_string(),
        });
    }
    if let Some(stripped) = specifier.strip_prefix('@') {
        let mut parts = stripped.split('/');
        let scope = parts
            .next()
            .ok_or_else(|| SandboxError::InvalidModuleSpecifier {
                specifier: specifier.to_string(),
            })?;
        let name = parts
            .next()
            .ok_or_else(|| SandboxError::InvalidModuleSpecifier {
                specifier: specifier.to_string(),
            })?;
        let package_name = format!("@{scope}/{name}");
        let rest = parts.collect::<Vec<_>>();
        let subpath = (!rest.is_empty()).then(|| rest.join("/"));
        return Ok((package_name, subpath));
    }
    let mut parts = specifier.split('/');
    let package_name = parts
        .next()
        .ok_or_else(|| SandboxError::InvalidModuleSpecifier {
            specifier: specifier.to_string(),
        })?
        .to_string();
    let rest = parts.collect::<Vec<_>>();
    let subpath = (!rest.is_empty()).then(|| rest.join("/"));
    Ok((package_name, subpath))
}

fn select_package_exports_target(value: &serde_json::Value) -> Option<&str> {
    match value {
        serde_json::Value::String(value) => Some(value.as_str()),
        serde_json::Value::Object(object) => object
            .get(".")
            .and_then(select_package_exports_target)
            .or_else(|| object.get("default").and_then(select_package_exports_target))
            .or_else(|| object.get("require").and_then(select_package_exports_target)),
        _ => None,
    }
}

fn candidate_module_paths(base: &str) -> Vec<String> {
    let base = normalize_node_path(base);
    if PathBuf::from(&base).extension().is_some() {
        return vec![base];
    }
    vec![
        base.clone(),
        format!("{base}.js"),
        format!("{base}.cjs"),
        format!("{base}.mjs"),
        format!("{base}.json"),
    ]
}

fn ancestor_directories(start: &str, workspace_root: &str) -> Vec<String> {
    let mut current = normalize_node_path(start);
    let mut directories = Vec::new();
    loop {
        directories.push(current.clone());
        if current == workspace_root || current == "/" {
            break;
        }
        current = directory_for_path(&current);
    }
    directories
}

fn resolve_node_path(cwd: &str, path: &str) -> String {
    if path.starts_with('/') {
        normalize_node_path(path)
    } else {
        normalize_node_path(&format!("{cwd}/{path}"))
    }
}

fn normalize_node_path(path: &str) -> String {
    let mut parts = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    format!("/{}", parts.join("/"))
}

fn directory_for_path(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| {
            if parent.is_empty() {
                "/".to_string()
            } else {
                parent.to_string()
            }
        })
        .unwrap_or_else(|| "/".to_string())
}

fn strip_shebang(source: &str) -> String {
    if let Some(rest) = source.strip_prefix("#!") {
        match rest.find('\n') {
            Some(index) => format!("//{}\n{}", &rest[..index], &rest[index + 1..]),
            None => String::from("//"),
        }
    } else {
        source.to_string()
    }
}

fn node_stats_to_json(stats: terracedb_vfs::Stats) -> JsonValue {
    serde_json::json!({
        "inode": stats.inode.to_string(),
        "kind": format!("{:?}", stats.kind).to_lowercase(),
        "mode": stats.mode,
        "nlink": stats.nlink,
        "uid": stats.uid,
        "gid": stats.gid,
        "size": stats.size,
        "created_at": stats.created_at.get(),
        "modified_at": stats.modified_at.get(),
        "changed_at": stats.changed_at.get(),
        "accessed_at": stats.accessed_at.get(),
        "rdev": stats.rdev,
    })
}

fn sandbox_execution_error(error: impl ToString) -> SandboxError {
    SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: error.to_string(),
    }
}

fn node_execution_error(
    entrypoint: &str,
    host: &NodeRuntimeHost,
    error: impl ToString,
) -> SandboxError {
    SandboxError::Execution {
        entrypoint: entrypoint.to_string(),
        message: format!(
            "{}\nnode trace: {}",
            error.to_string(),
            host.debug_trace.borrow().recent_events.join(" | ")
        ),
    }
}

fn js_error(error: SandboxError) -> boa_engine::JsError {
    JsNativeError::typ().with_message(error.to_string()).into()
}

async fn open_js_runtime(
    handle: &SandboxRuntimeHandle,
    session_info: &SandboxSessionInfo,
    session: &SandboxSession,
    loader: SandboxModuleLoader,
    host_services: Arc<dyn JsHostServices>,
) -> Result<Arc<dyn JsRuntime>, SandboxError> {
    let runtime_host = BoaJsRuntimeHost::new(Arc::new(loader), host_services)
        .with_clock(Arc::new(FixedJsClock::new(session_info.updated_at.get())))
        .with_entropy(Arc::new(DeterministicJsEntropySource::new(
            js_entropy_seed(session_info),
        )));
    runtime_host
        .open_runtime(JsRuntimeOpenRequest {
            runtime_id: handle.actor_id.clone(),
            policy: runtime_policy_for_session(session, session_info),
            provenance: JsRuntimeProvenance {
                backend: handle.backend.clone(),
                host_model: "sandbox-session".to_string(),
                module_root: session_info.workspace_root.clone(),
                volume_id: Some(session_info.session_volume_id),
                snapshot_sequence: Some(session_info.provenance.base_snapshot.sequence.get()),
                durable_snapshot: session_info.provenance.base_snapshot.durable,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            metadata: BTreeMap::from([
                (
                    "session_revision".to_string(),
                    JsonValue::from(session_info.revision),
                ),
                (
                    "workspace_root".to_string(),
                    JsonValue::from(session_info.workspace_root.clone()),
                ),
            ]),
        })
        .await
        .map_err(|error| js_substrate_error_to_sandbox(error, &handle.actor_id))
}

async fn prepare_js_execution_request(
    request: &SandboxExecutionRequest,
    session_info: &SandboxSessionInfo,
    state: &SandboxRuntimeStateHandle,
    loader: &SandboxModuleLoader,
) -> Result<
    (
        String,
        JsExecutionRequest,
        Option<(SandboxModuleCacheEntry, bool)>,
    ),
    SandboxError,
> {
    match request.kind.clone() {
        SandboxExecutionKind::Module { specifier } => {
            let entrypoint = loader.resolve(&specifier, None).await?;
            Ok((
                entrypoint.clone(),
                JsExecutionRequest {
                    kind: JsExecutionKind::Module {
                        specifier: entrypoint,
                    },
                    metadata: request.metadata.clone(),
                },
                None,
            ))
        }
        SandboxExecutionKind::Eval {
            source,
            virtual_specifier,
        } => {
            let entrypoint = match virtual_specifier {
                Some(specifier) => specifier,
                None => {
                    state
                        .next_eval_specifier(&session_info.workspace_root)
                        .await
                }
            };
            let inline = inline_eval_module(&entrypoint, source.clone());
            let was_hit = state.is_module_cache_hit(&inline.cache_entry).await;
            state.upsert_module_cache(inline.cache_entry.clone()).await;
            Ok((
                entrypoint.clone(),
                JsExecutionRequest {
                    kind: JsExecutionKind::Eval {
                        source,
                        virtual_specifier: Some(entrypoint),
                    },
                    metadata: request.metadata.clone(),
                },
                Some((inline.cache_entry, was_hit)),
            ))
        }
        SandboxExecutionKind::NodeCommand { .. } => Err(SandboxError::Service {
            service: "runtime",
            message: "node command requests should bypass JS module preparation".to_string(),
        }),
    }
}

fn runtime_policy_for_session(
    session: &SandboxSession,
    session_info: &SandboxSessionInfo,
) -> JsRuntimePolicy {
    let mut visible_host_services = BTreeSet::new();

    for operation in FS_HOST_EXPORTS {
        visible_host_services.insert(format!("{SANDBOX_FS_LIBRARY_SPECIFIER}::{operation}"));
    }

    if session_info.provenance.package_compat == PackageCompatibilityMode::NpmWithNodeBuiltins {
        for service in ["node:fs", "node:fs/promises"] {
            for operation in FS_HOST_EXPORTS {
                visible_host_services.insert(format!("{service}::{operation}"));
            }
        }
    }

    if session_info.provenance.git.is_some()
        || session_info
            .provenance
            .capabilities
            .contains(GIT_REMOTE_IMPORT_CAPABILITY_SPECIFIER)
    {
        for operation in GIT_REPO_HOST_EXPORTS {
            visible_host_services.insert(format!("{SANDBOX_GIT_LIBRARY_SPECIFIER}::{operation}"));
        }
    }
    if session
        .git_host_bridge()
        .supports_remote_repository_bridge()
        && session_info
            .provenance
            .capabilities
            .contains(GIT_REMOTE_IMPORT_CAPABILITY_SPECIFIER)
    {
        visible_host_services.insert(format!(
            "{SANDBOX_GIT_LIBRARY_SPECIFIER}::{GIT_REMOTE_IMPORT_HOST_EXPORT}"
        ));
    }

    let capabilities = session.capability_registry();
    for capability in &session_info.provenance.capabilities.capabilities {
        if let Some(module) = capabilities.module(&capability.specifier) {
            for method in module.methods {
                visible_host_services.insert(format!("{}::{}", capability.specifier, method.name));
            }
        }
    }

    JsRuntimePolicy {
        execution_domain: None,
        compatibility_profile: match session_info.provenance.package_compat {
            PackageCompatibilityMode::TerraceOnly => JsCompatibilityProfile::TerraceOnly,
            PackageCompatibilityMode::NpmPureJs | PackageCompatibilityMode::NpmWithNodeBuiltins => {
                JsCompatibilityProfile::SandboxCompat
            }
        },
        allow_workspace_modules: true,
        allow_host_modules: true,
        allow_package_modules: session_info.provenance.package_compat
            != PackageCompatibilityMode::TerraceOnly,
        visible_host_services: visible_host_services.into_iter().collect(),
        forbidden_ambient_defaults: JsForkPolicy::simulation_native_baseline()
            .forbidden_ambient_defaults,
    }
}

fn inline_eval_module(specifier: &str, source: String) -> LoadedSandboxModule {
    let runtime_path = specifier
        .strip_prefix("terrace:")
        .unwrap_or(specifier)
        .to_string();
    let cache_entry = SandboxModuleCacheEntry {
        specifier: specifier.to_string(),
        kind: SandboxModuleKind::Workspace,
        runtime_path: runtime_path.clone(),
        media_type: "text/javascript".to_string(),
        cache_key: {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(specifier.as_bytes());
            hasher.update(source.as_bytes());
            format!("{:08x}", hasher.finalize())
        },
    };
    LoadedSandboxModule {
        specifier: specifier.to_string(),
        kind: SandboxModuleKind::Workspace,
        runtime_path,
        media_type: "text/javascript".to_string(),
        source_map: Some(
            serde_json::json!({
                "version": 3,
                "file": specifier,
                "sources": [specifier],
                "names": [],
                "mappings": "",
            })
            .to_string(),
        ),
        source,
        cache_entry,
    }
}

fn merge_inline_eval_trace(
    trace: &mut SandboxModuleLoadTrace,
    cache_entry: SandboxModuleCacheEntry,
    was_hit: bool,
) {
    if !trace.cache_entries.contains(&cache_entry) {
        trace.cache_entries.push(cache_entry.clone());
    }
    let bucket = if was_hit {
        &mut trace.cache_hits
    } else {
        &mut trace.cache_misses
    };
    if !bucket.contains(&cache_entry.specifier) {
        bucket.push(cache_entry.specifier);
    }
}

async fn persist_runtime_cache(
    session: &SandboxSession,
    state: &SandboxRuntimeStateHandle,
) -> Result<(), SandboxError> {
    let mut cache_entries = state
        .snapshot()
        .await
        .module_cache
        .into_values()
        .collect::<Vec<_>>();
    cache_entries.sort_by(|left, right| left.specifier.cmp(&right.specifier));
    session
        .volume()
        .fs()
        .write_file(
            TERRACE_RUNTIME_MODULE_CACHE_PATH,
            serde_json::to_vec_pretty(&cache_entries)?,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

async fn hydrate_runtime_cache_state(
    session: &SandboxSession,
    session_info: &SandboxSessionInfo,
    state: &SandboxRuntimeStateHandle,
) -> Result<(), SandboxError> {
    if !state.snapshot().await.module_cache.is_empty() {
        return Ok(());
    }
    let Some(bytes) = session
        .filesystem()
        .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
        .await?
    else {
        return Ok(());
    };
    let entries = serde_json::from_slice::<Vec<SandboxModuleCacheEntry>>(&bytes)?;
    let next_eval_id = recovered_next_eval_id(&session_info.workspace_root, &entries);
    state.hydrate_module_cache(entries, next_eval_id).await;
    Ok(())
}

fn recovered_next_eval_id(workspace_root: &str, entries: &[SandboxModuleCacheEntry]) -> u64 {
    let prefix = format!("terrace:{workspace_root}/.terrace/runtime/eval-");
    entries
        .iter()
        .filter_map(|entry| {
            entry
                .specifier
                .strip_prefix(&prefix)?
                .strip_suffix(".mjs")?
                .parse::<u64>()
                .ok()
        })
        .max()
        .unwrap_or(0)
}

fn js_entropy_seed(session_info: &SandboxSessionInfo) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(session_info.session_volume_id.to_string().as_bytes());
    hasher.update(session_info.workspace_root.as_bytes());
    hasher.update(&session_info.revision.to_le_bytes());
    u64::from(hasher.finalize())
}

fn host_service_arguments(arguments: &JsonValue) -> Vec<JsonValue> {
    match arguments {
        JsonValue::Null => Vec::new(),
        JsonValue::Array(values) => values.clone(),
        other => vec![other.clone()],
    }
}

fn host_service_argument<T>(request: &JsHostServiceRequest) -> Result<T, JsSubstrateError>
where
    T: DeserializeOwned,
{
    let value = match &request.arguments {
        JsonValue::Array(values) => values.first().cloned().unwrap_or(JsonValue::Null),
        other => other.clone(),
    };
    serde_json::from_value(value).map_err(|error| JsSubstrateError::EvaluationFailed {
        entrypoint: format!("{}::{}", request.service, request.operation),
        message: format!("invalid host service arguments: {error}"),
    })
}

fn host_service_optional_argument<T>(request: &JsHostServiceRequest) -> Result<T, JsSubstrateError>
where
    T: DeserializeOwned + Default,
{
    let value = match &request.arguments {
        JsonValue::Array(values) => values.first().cloned().unwrap_or(JsonValue::Null),
        other => other.clone(),
    };
    if value.is_null() {
        return Ok(T::default());
    }
    serde_json::from_value(value).map_err(|error| JsSubstrateError::EvaluationFailed {
        entrypoint: format!("{}::{}", request.service, request.operation),
        message: format!("invalid host service arguments: {error}"),
    })
}

fn js_substrate_error_to_sandbox(
    error: JsSubstrateError,
    fallback_entrypoint: &str,
) -> SandboxError {
    match error {
        JsSubstrateError::EvaluationFailed {
            entrypoint,
            message,
        } => SandboxError::Execution {
            entrypoint,
            message,
        },
        JsSubstrateError::UnsupportedSpecifier { specifier } => SandboxError::Execution {
            entrypoint: fallback_entrypoint.to_string(),
            message: format!("unsupported js module specifier: {specifier}"),
        },
        JsSubstrateError::ModuleNotFound { specifier } => SandboxError::Execution {
            entrypoint: fallback_entrypoint.to_string(),
            message: format!("js module not found: {specifier}"),
        },
        JsSubstrateError::ModulePolicyDenied {
            specifier, message, ..
        } => SandboxError::Execution {
            entrypoint: fallback_entrypoint.to_string(),
            message: format!("{specifier}: {message}"),
        },
        JsSubstrateError::HostServiceUnavailable { service, operation } => {
            SandboxError::Execution {
                entrypoint: fallback_entrypoint.to_string(),
                message: format!("js host service is unavailable: {service}::{operation}"),
            }
        }
        JsSubstrateError::HostServiceDenied {
            service,
            operation,
            message,
        } => SandboxError::Execution {
            entrypoint: fallback_entrypoint.to_string(),
            message: format!("js host service denied {service}::{operation}: {message}"),
        },
        JsSubstrateError::Cancelled { runtime_id } => SandboxError::Execution {
            entrypoint: fallback_entrypoint.to_string(),
            message: format!("js runtime {runtime_id} was cancelled"),
        },
        JsSubstrateError::InvalidDirective { module, message } => SandboxError::Execution {
            entrypoint: module,
            message,
        },
        JsSubstrateError::SerdeJson(error) => SandboxError::SerdeJson(error),
        JsSubstrateError::Vfs(error) => SandboxError::Vfs(error),
    }
}

fn sandbox_error_to_js_host_service(
    request: &JsHostServiceRequest,
    error: SandboxError,
) -> JsSubstrateError {
    match error {
        SandboxError::MissingGitProvenance | SandboxError::MissingGitObjectFormat => {
            JsSubstrateError::HostServiceUnavailable {
                service: request.service.clone(),
                operation: request.operation.clone(),
            }
        }
        other => JsSubstrateError::EvaluationFailed {
            entrypoint: format!("{}::{}", request.service, request.operation),
            message: other.to_string(),
        },
    }
}
