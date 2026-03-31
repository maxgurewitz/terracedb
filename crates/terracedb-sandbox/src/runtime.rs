use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use async_trait::async_trait;
use boa_engine::{Context, Module, builtins::promise::PromiseState, js_string};
use serde::{Deserialize, Serialize};
use terracedb_vfs::JsonValue;
use tokio::sync::Mutex;

use crate::{
    SandboxError, SandboxSession, SandboxSessionInfo, TERRACE_RUNTIME_MODULE_CACHE_PATH,
    loader::{
        ActiveGuestHost, LoadedSandboxModule, SandboxModuleCacheEntry, SandboxModuleKind,
        SandboxModuleLoadTrace, with_active_guest_host,
    },
};

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
        let loader = session
            .module_loader_with_state(session_info.clone(), state.clone())
            .await;
        let (entrypoint, root) = match request.kind.clone() {
            SandboxExecutionKind::Module { specifier } => {
                let resolved = loader.resolve(&specifier, None).await?;
                let loaded = loader.load(&resolved, None).await?;
                (resolved, loaded)
            }
            SandboxExecutionKind::Eval {
                source,
                virtual_specifier,
            } => {
                let specifier = match virtual_specifier {
                    Some(specifier) => specifier,
                    None => {
                        state
                            .next_eval_specifier(&session_info.workspace_root)
                            .await
                    }
                };
                let loaded = inline_eval_module(&specifier, source);
                if !state.is_module_cache_hit(&loaded.cache_entry).await {
                    state.upsert_module_cache(loaded.cache_entry.clone()).await;
                }
                (specifier, loaded)
            }
        };

        let host = Arc::new(ActiveGuestHost::new(session.clone()));
        with_active_guest_host(host.clone(), async move {
            let runtime = Rc::new(loader.clone());
            let mut context = Context::builder()
                .module_loader(runtime.clone())
                .build()
                .map_err(|error| SandboxError::Execution {
                    entrypoint: entrypoint.clone(),
                    message: error.to_string(),
                })?;
            let root_module = runtime.to_boa_module(root, &mut context).map_err(|error| {
                SandboxError::Execution {
                    entrypoint: entrypoint.clone(),
                    message: error.to_string(),
                }
            })?;
            let promise = root_module.load_link_evaluate(&mut context);
            context
                .run_jobs()
                .map_err(|error| SandboxError::Execution {
                    entrypoint: entrypoint.clone(),
                    message: error.to_string(),
                })?;
            let result = match promise.state() {
                PromiseState::Pending => {
                    return Err(SandboxError::Execution {
                        entrypoint: entrypoint.clone(),
                        message: "module evaluation is still pending after the job queue drained"
                            .to_string(),
                    });
                }
                PromiseState::Fulfilled(_) => export_default_value(&root_module, &mut context)?,
                PromiseState::Rejected(reason) => {
                    return Err(SandboxError::Execution {
                        entrypoint: entrypoint.clone(),
                        message: format!("{}", reason.display()),
                    });
                }
            };
            let trace = loader.trace_snapshot().await;
            persist_runtime_cache(session, &trace).await?;
            let module_count = trace.module_graph.len();
            let package_import_count = trace.package_imports.len();
            let module_graph = trace.module_graph;
            let package_imports = trace.package_imports;
            let cache_hits = trace.cache_hits;
            let cache_misses = trace.cache_misses;
            let cache_entries = trace.cache_entries;
            Ok(SandboxExecutionResult {
                backend: self.name.clone(),
                actor_id: handle.actor_id.clone(),
                entrypoint,
                result,
                module_graph,
                package_imports,
                cache_hits,
                cache_misses,
                cache_entries,
                capability_calls: host.capability_calls(),
                metadata: BTreeMap::from([
                    (
                        "session_revision".to_string(),
                        JsonValue::from(session_info.revision),
                    ),
                    (
                        "workspace_root".to_string(),
                        JsonValue::from(session_info.workspace_root),
                    ),
                    ("module_count".to_string(), JsonValue::from(module_count)),
                    (
                        "package_import_count".to_string(),
                        JsonValue::from(package_import_count),
                    ),
                ]),
            })
        })
        .await
    }

    async fn close_session(
        &self,
        _session: &SandboxSessionInfo,
        _handle: &SandboxRuntimeHandle,
    ) -> Result<(), SandboxError> {
        Ok(())
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

fn export_default_value(
    module: &Module,
    context: &mut Context,
) -> Result<Option<JsonValue>, SandboxError> {
    let default_value = module
        .namespace(context)
        .get(js_string!("default"), context);
    match default_value {
        Ok(value) if value.is_undefined() => Ok(None),
        Ok(value) => value
            .to_json(context)
            .map_err(|error| SandboxError::Execution {
                entrypoint: "<namespace>".to_string(),
                message: error.to_string(),
            }),
        Err(_) => Ok(None),
    }
}

async fn persist_runtime_cache(
    session: &SandboxSession,
    trace: &SandboxModuleLoadTrace,
) -> Result<(), SandboxError> {
    session
        .volume()
        .fs()
        .write_file(
            TERRACE_RUNTIME_MODULE_CACHE_PATH,
            serde_json::to_vec_pretty(&trace.cache_entries)?,
            terracedb_vfs::CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}
