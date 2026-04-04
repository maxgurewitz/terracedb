use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    path::PathBuf,
    rc::Rc,
    sync::Arc,
    thread_local,
};

use async_trait::async_trait;
use boa_engine::{
    Context, JsNativeError, JsResult, JsString, JsSymbol, JsValue, NativeFunction, Source,
    builtins::promise::PromiseState,
    js_string,
    module::{
        Module, ModuleLoader as BoaModuleLoader, ModuleRequest, Referrer,
        SyntheticModuleInitializer,
    },
    object::{FunctionObjectBuilder, JsObject, ObjectInitializer},
    object::builtins::{JsArray, JsArrayBuffer, JsPromise, JsUint8Array},
    property::Attribute,
};
use brotli::{CompressorReader as BrotliCompressorReader, Decompressor as BrotliDecompressor};
use flate2::{
    Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status,
    read::{DeflateDecoder, GzDecoder, ZlibDecoder},
    write::{DeflateEncoder, GzEncoder, ZlibEncoder},
};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use md5::Md5;
use pbkdf2::pbkdf2_hmac;
use ripemd::Ripemd160;
use scrypt::{Params as ScryptParams, scrypt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest as _, Sha224, Sha256, Sha384, Sha512, Sha512_224, Sha512_256};
use sha3::{
    Sha3_224, Sha3_256, Sha3_384, Sha3_512, Shake128, Shake256,
    digest::{ExtendableOutput, Update as XofUpdate, XofReader},
};
use terracedb_git::{GitCheckoutRequest, GitDiffRequest, GitRefUpdate, GitStatusOptions};
use terracedb_js::{
    BoaJsRuntimeHost, DeterministicJsEntropySource, FixedJsClock, JsCompatibilityProfile,
    JsEntropySource, JsExecutionKind, JsExecutionRequest, JsForkPolicy, JsHostServiceAdapter,
    JsHostServiceRequest, JsHostServiceResponse, JsHostServices, JsRuntime, JsRuntimeHost,
    JsRuntimeOpenRequest, JsRuntimePolicy, JsRuntimeProvenance, JsSubstrateError, NeverCancel,
    RoutedJsHostServices, VfsJsHostServiceAdapter,
};
use terracedb_vfs::{
    CreateOptions, FileKind, JsonValue, MkdirOptions, ReadOnlyVfsFileSystem, Stats, VfsVolumeExt,
};
use tokio::sync::Mutex;
use tracing::{Level, info_span, trace};
use url::Url;

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

thread_local! {
    static ACTIVE_NODE_HOST_STACK: RefCell<Vec<Rc<NodeRuntimeHost>>> = const { RefCell::new(Vec::new()) };
}

const NODE_COMPAT_TARGET_VERSION: &str = "v24.14.1";
const NODE_COMPAT_BOOTSTRAP_SOURCE: &str = include_str!("node_compat_bootstrap.js");
const NODE_UPSTREAM_VFS_ROOT: &str = "/node";
const NODE_UPSTREAM_BOOTSTRAP_REALM_PATH: &str = "/node/lib/internal/bootstrap/realm.js";
const NODE_RUNTIME_RESOLVE_BUDGET: u64 = 16_384;
const NODE_RUNTIME_LOAD_BUDGET: u64 = 8_192;
const NODE_RUNTIME_FS_BUDGET: u64 = 131_072;
const NODE_RUNTIME_DEBUG_EVENT_LIMIT: usize = 1024;
const NODE_RUNTIME_JOB_DRAIN_BUDGET: usize = 64;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum NodeResolvedKind {
    #[serde(rename = "builtin")]
    Builtin,
    #[serde(rename = "commonjs")]
    CommonJs,
    #[serde(rename = "json")]
    Json,
    #[serde(rename = "esm")]
    EsModule,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum NodeResolveMode {
    Require,
    Import,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NodeResolvedModule {
    id: String,
    specifier: String,
    kind: NodeResolvedKind,
}

#[derive(Clone, Debug)]
struct NodeLoadedModule {
    runtime_path: String,
    media_type: String,
    source: String,
    kind: NodeResolvedKind,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct NodeRequireResolveOptions {
    #[serde(default)]
    paths: Option<Vec<String>>,
    #[serde(default)]
    extensions: Option<Vec<String>>,
}

#[derive(Clone)]
struct NodeRuntimeHost {
    runtime_name: String,
    runtime_handle: SandboxRuntimeHandle,
    runtime_state: SandboxRuntimeStateHandle,
    session_info: SandboxSessionInfo,
    session: SandboxSession,
    workspace_root: String,
    capture_debug_trace: bool,
    debug_options: NodeDebugExecutionOptions,
    entropy: DeterministicJsEntropySource,
    process: Rc<RefCell<NodeProcessState>>,
    open_files: Rc<RefCell<NodeOpenFileTable>>,
    loaded_modules: Rc<RefCell<BTreeMap<String, NodeLoadedModule>>>,
    materialized_modules: Rc<RefCell<BTreeMap<String, Module>>>,
    module_graph: Rc<RefCell<NodeModuleGraph>>,
    read_snapshot_fs: Rc<RefCell<Option<Arc<dyn ReadOnlyVfsFileSystem>>>>,
    builtin_ids: Rc<RefCell<Option<Vec<String>>>>,
    debug_trace: Rc<RefCell<NodeRuntimeDebugTrace>>,
    next_child_pid: Rc<RefCell<u32>>,
    zlib_streams: Rc<RefCell<NodeZlibStreamTable>>,
}

struct NodeCommandModuleLoader {
    host: Rc<NodeRuntimeHost>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct NodeRuntimeTraceEvent {
    seq: u64,
    bucket: String,
    detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<JsonValue>,
}

impl NodeRuntimeTraceEvent {
    fn render(&self) -> String {
        if self.bucket == "js" {
            if let Some(label) = self.label.as_deref() {
                if let Some(data) = self.data.as_ref() {
                    return format!("js:{label} {data}");
                }
                if let Some(message) = self.message.as_deref() {
                    return format!("js:{label} {message}");
                }
                return format!("js:{label}");
            }
        }
        format!("{}:{}", self.bucket, self.detail)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct NodeRuntimeExceptionSnapshot {
    seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entrypoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stack: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    require_stack: Vec<String>,
    raw: JsonValue,
}

#[derive(Clone, Debug, Default)]
struct NodeRuntimeDebugTrace {
    resolve_calls: u64,
    load_calls: u64,
    fs_calls: u64,
    next_seq: u64,
    structured_events: Vec<NodeRuntimeTraceEvent>,
    recent_events: Vec<String>,
    last_exception: Option<NodeRuntimeExceptionSnapshot>,
}

#[derive(Clone, Debug, Default)]
struct NodeModuleGraph {
    mutation_epoch: u64,
    stat_cache: BTreeMap<String, Option<Stats>>,
    file_cache: BTreeMap<String, Option<Vec<u8>>>,
    package_json_cache: BTreeMap<String, Option<serde_json::Value>>,
    nearest_package_root_cache: BTreeMap<String, Option<String>>,
    nearest_package_type_cache: BTreeMap<String, Option<String>>,
    module_kind_cache: BTreeMap<String, NodeResolvedKind>,
    node_modules_lookup_cache: BTreeMap<String, Vec<String>>,
    file_or_directory_cache: BTreeMap<NodeFileOrDirectoryCacheKey, Option<String>>,
    package_entry_cache: BTreeMap<NodePackageEntryCacheKey, Option<String>>,
    resolve_path_cache: BTreeMap<NodeResolveCacheKey, Option<String>>,
}

impl NodeModuleGraph {
    fn invalidate(&mut self) {
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
        self.stat_cache.clear();
        self.file_cache.clear();
        self.package_json_cache.clear();
        self.nearest_package_root_cache.clear();
        self.nearest_package_type_cache.clear();
        self.module_kind_cache.clear();
        self.node_modules_lookup_cache.clear();
        self.file_or_directory_cache.clear();
        self.package_entry_cache.clear();
        self.resolve_path_cache.clear();
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodeFileOrDirectoryCacheKey {
    base: String,
    mode: NodeResolveMode,
    extensions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodePackageEntryCacheKey {
    package_root: String,
    subpath: Option<String>,
    mode: NodeResolveMode,
    extensions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodeResolveCacheKey {
    specifier: String,
    referrer: Option<String>,
    mode: NodeResolveMode,
    paths: Vec<String>,
    extensions: Vec<String>,
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

#[derive(Clone, Debug)]
struct NodeOpenFile {
    path: String,
    readable: bool,
    writable: bool,
    append: bool,
    cursor: usize,
    contents: Vec<u8>,
}

#[derive(Clone, Debug)]
struct NodeOpenFileTable {
    next_fd: i32,
    entries: BTreeMap<i32, NodeOpenFile>,
}

#[derive(Default)]
struct NodeZlibStreamTable {
    next_id: u32,
    entries: BTreeMap<u32, NodeZlibStream>,
}

enum NodeZlibStream {
    Compress(NodeZlibCompressStream),
    Decompress(NodeZlibDecompressStream),
}

struct NodeZlibCompressStream {
    mode: String,
    inner: Compress,
}

struct NodeZlibDecompressStream {
    mode: String,
    inner: Decompress,
}

#[derive(Clone, Debug, Deserialize)]
struct NodeChildProcessRequest {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    shell: bool,
    input: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct NodeChildProcessError {
    code: JsonValue,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    errno: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    syscall: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cmd: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct NodeChildProcessResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    file: String,
    spawnfile: String,
    spawnargs: Vec<String>,
    stdout: String,
    stderr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<NodeChildProcessError>,
}

#[derive(Clone, Debug, Deserialize)]
struct NodeCryptoDigestRequest {
    algorithm: String,
    data: Vec<u8>,
    #[serde(default)]
    output_length: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
struct NodeCryptoHmacRequest {
    algorithm: String,
    key: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
struct NodeCryptoPbkdf2Request {
    password: Vec<u8>,
    salt: Vec<u8>,
    iterations: f64,
    keylen: f64,
    digest: String,
}

#[derive(Clone, Debug, Deserialize)]
struct NodeCryptoHkdfRequest {
    digest: String,
    ikm: Vec<u8>,
    salt: Vec<u8>,
    info: Vec<u8>,
    keylen: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct NodeCryptoScryptRequest {
    password: Vec<u8>,
    salt: Vec<u8>,
    keylen: f64,
    #[serde(default)]
    cost: Option<f64>,
    #[serde(default)]
    block_size: Option<f64>,
    #[serde(default)]
    parallelization: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct NodeZlibOptions {
    #[serde(default)]
    level: Option<f64>,
    #[serde(default)]
    quality: Option<f64>,
}

impl Default for NodeOpenFileTable {
    fn default() -> Self {
        Self {
            next_fd: 100,
            entries: BTreeMap::new(),
        }
    }
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
            version: NODE_COMPAT_TARGET_VERSION.to_string(),
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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct NodeDebugExecutionOptions {
    #[serde(default)]
    pub autoinstrument_modules: Vec<String>,
    #[serde(default)]
    pub capture_exceptions: bool,
    #[serde(default)]
    pub trace_intrinsics: bool,
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

    pub fn with_node_debug(mut self, options: NodeDebugExecutionOptions) -> Self {
        self.metadata.insert(
            "node_debug".to_string(),
            serde_json::to_value(options).unwrap_or(JsonValue::Null),
        );
        self
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
                &request.metadata,
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
    metadata: &BTreeMap<String, JsonValue>,
) -> Result<SandboxExecutionResult, SandboxError> {
    ensure_node_upstream_support_tree_present(session).await?;
    let normalized_entrypoint = resolve_node_path(&cwd, &entrypoint);
    let entropy = DeterministicJsEntropySource::new(js_entropy_seed(&session_info));
    let debug_options = metadata
        .get("node_debug")
        .cloned()
        .map(serde_json::from_value::<NodeDebugExecutionOptions>)
        .transpose()
        .map_err(sandbox_execution_error)?
        .unwrap_or_default();
    let node_host = Rc::new(NodeRuntimeHost {
        runtime_name: runtime_name.to_string(),
        runtime_handle: handle.clone(),
        runtime_state: state.clone(),
        session_info: session_info.clone(),
        session: session.clone(),
        workspace_root: session_info.workspace_root.clone(),
        capture_debug_trace: tracing::enabled!(target: "terracedb.sandbox.node_runtime", Level::TRACE)
            || std::env::var_os("TERRACE_CAPTURE_NODE_TRACE").is_some(),
        debug_options,
        entropy: entropy.clone(),
        process: Rc::new(RefCell::new(NodeProcessState::new(argv, env, cwd))),
        open_files: Rc::new(RefCell::new(NodeOpenFileTable::default())),
        loaded_modules: Rc::new(RefCell::new(BTreeMap::new())),
        materialized_modules: Rc::new(RefCell::new(BTreeMap::new())),
        module_graph: Rc::new(RefCell::new(NodeModuleGraph::default())),
        read_snapshot_fs: Rc::new(RefCell::new(None)),
        builtin_ids: Rc::new(RefCell::new(None)),
        debug_trace: Rc::new(RefCell::new(NodeRuntimeDebugTrace::default())),
        next_child_pid: Rc::new(RefCell::new(1000)),
        zlib_streams: Rc::new(RefCell::new(NodeZlibStreamTable::default())),
    });
    let result = execute_node_command_inner(node_host, entrypoint, normalized_entrypoint)?;
    persist_runtime_cache(session, state).await?;
    Ok(result)
}

async fn ensure_node_upstream_support_tree_present(
    session: &SandboxSession,
) -> Result<(), SandboxError> {
    if session
        .volume()
        .fs()
        .stat(NODE_UPSTREAM_BOOTSTRAP_REALM_PATH)
        .await?
        .is_some()
    {
        return Ok(());
    }

    Err(SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: format!(
            "missing upstream Node bootstrap tree at `{NODE_UPSTREAM_VFS_ROOT}`; \
open this session from a Node-capable base layer"
        ),
    })
}

fn execute_node_command_inner(
    node_host: Rc<NodeRuntimeHost>,
    entrypoint: String,
    normalized_entrypoint: String,
) -> Result<SandboxExecutionResult, SandboxError> {
    let span = info_span!(
        "terracedb.sandbox.node_command",
        entrypoint = %normalized_entrypoint,
        cwd = %node_host.process.borrow().cwd
    );
    let _guard = span.enter();

    with_active_node_host(node_host.clone(), |node_host| {
        let module_loader = Rc::new(NodeCommandModuleLoader::new(node_host.clone()));
        let mut context = Context::builder()
            .module_loader(module_loader)
            .build()
            .map_err(|error| SandboxError::Execution {
                entrypoint: normalized_entrypoint.clone(),
                message: error.to_string(),
            })?;
        node_debug_event(node_host, "stage", "context-built".to_string())?;
        install_node_host_bindings(&mut context)?;
        node_debug_event(node_host, "stage", "host-bindings-installed".to_string())?;
        node_debug_event(node_host, "stage", "bootstrap-eval-start".to_string())?;
        context
            .eval(
                Source::from_bytes(NODE_COMPAT_BOOTSTRAP_SOURCE.as_bytes())
                    .with_path(&PathBuf::from("/__terrace__/node/bootstrap.js")),
            )
            .map_err(|error| node_execution_error(&normalized_entrypoint, node_host, error))?;
        node_debug_event(node_host, "stage", "bootstrap-eval-done".to_string())?;
        let debug_config = JsValue::from_json(
            &serde_json::to_value(&node_host.debug_options).map_err(sandbox_execution_error)?,
            &mut context,
        )
        .map_err(|error| node_execution_error(&normalized_entrypoint, node_host, error))?;
        context
            .global_object()
            .set(
                js_string!("__terraceNodeDebugConfig"),
                debug_config,
                false,
                &mut context,
            )
            .map_err(|error| node_execution_error(&normalized_entrypoint, node_host, error))?;

        let request = JsValue::from_json(
            &serde_json::json!({ "entrypoint": normalized_entrypoint }),
            &mut context,
        )
        .map_err(|error| node_execution_error(&entrypoint, node_host, error))?;
        node_debug_event(node_host, "stage", "runner-lookup-start".to_string())?;
        let runner = context
            .global_object()
            .get(js_string!("__terraceRunNodeCommand"), &mut context)
            .map_err(|error| node_execution_error(&entrypoint, node_host, error))?;
        let runner = runner
            .as_callable()
            .ok_or_else(|| SandboxError::Execution {
                entrypoint: entrypoint.clone(),
                message: "__terraceRunNodeCommand is not callable".to_string(),
            })?;
        node_debug_event(node_host, "stage", "runner-call-start".to_string())?;
        let runner_result = runner
            .call(&JsValue::undefined(), &[request], &mut context)
            .map_err(|error| node_execution_error(&entrypoint, node_host, error))?;
        node_debug_event(node_host, "stage", "runner-call-done".to_string())?;
        if let Some(object) = runner_result.as_object() {
            if let Ok(promise) = JsPromise::from_object(object.clone()) {
                match promise.await_blocking(&mut context) {
                    Ok(_) | Err(_) => {}
                }
                drain_node_jobs_until_quiescent(&mut context, node_host, &entrypoint)?;
                if let PromiseState::Rejected(reason) = promise.state() {
                    return Err(node_execution_error(
                        &entrypoint,
                        node_host,
                        boa_engine::JsError::from_opaque(reason),
                    ));
                }
            } else {
                drain_node_jobs_until_quiescent(&mut context, node_host, &entrypoint)?;
            }
        } else {
            drain_node_jobs_until_quiescent(&mut context, node_host, &entrypoint)?;
        }
        let finisher = context
            .global_object()
            .get(js_string!("__terraceFinishNodeCommand"), &mut context)
            .map_err(|error| node_execution_error(&entrypoint, node_host, error))?;
        let finisher = finisher
            .as_callable()
            .ok_or_else(|| SandboxError::Execution {
                entrypoint: entrypoint.clone(),
                message: "__terraceFinishNodeCommand is not callable".to_string(),
            })?;
        let report = finisher
            .call(&JsValue::undefined(), &[], &mut context)
            .map_err(|error| node_execution_error(&entrypoint, node_host, error))?;
        let result = report
            .to_json(&mut context)
            .map_err(|error| node_execution_error(&entrypoint, node_host, error))?;

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
        let debug_trace = node_host.debug_trace.borrow().clone();

        Ok(SandboxExecutionResult {
            backend: node_host.runtime_name.clone(),
            actor_id: node_host.runtime_handle.actor_id.clone(),
            entrypoint: entrypoint.clone(),
            result,
            module_graph: module_graph.clone(),
            package_imports: Vec::new(),
            cache_hits: Vec::new(),
            cache_misses: Vec::new(),
            cache_entries: futures::executor::block_on(node_host.runtime_state.snapshot())
                .module_cache
                .into_values()
                .collect(),
            capability_calls: Vec::new(),
            metadata: BTreeMap::from([
                (
                    "session_revision".to_string(),
                    JsonValue::from(node_host.session_info.revision),
                ),
                (
                    "workspace_root".to_string(),
                    JsonValue::from(node_host.session_info.workspace_root.clone()),
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
                    "node_resolve_calls".to_string(),
                    JsonValue::from(debug_trace.resolve_calls),
                ),
                (
                    "node_load_calls".to_string(),
                    JsonValue::from(debug_trace.load_calls),
                ),
                (
                    "node_fs_calls".to_string(),
                    JsonValue::from(debug_trace.fs_calls),
                ),
                (
                    "node_runtime_trace".to_string(),
                    JsonValue::Array(
                        debug_trace
                            .recent_events
                            .iter()
                            .cloned()
                            .map(JsonValue::from)
                            .collect(),
                    ),
                ),
                (
                    "node_runtime_events".to_string(),
                    serde_json::to_value(&debug_trace.structured_events)
                        .map_err(sandbox_execution_error)?,
                ),
                (
                    "node_runtime_last_exception".to_string(),
                    serde_json::to_value(&debug_trace.last_exception)
                        .map_err(sandbox_execution_error)?,
                ),
            ]),
        })
    })
}

fn install_node_host_bindings(context: &mut Context) -> Result<(), SandboxError> {
    register_global_native(context, "escape", 1, node_global_escape)?;
    register_global_native(context, "unescape", 1, node_global_unescape)?;
    register_global_native(context, "__terraceNodeDebugEvent", 2, node_trace_event)?;
    register_global_native(context, "__terraceGetProcessInfo", 0, node_get_process_info)?;
    register_global_native(context, "__terraceGetCwd", 0, node_get_cwd)?;
    register_global_native(context, "__terraceChdir", 1, node_chdir)?;
    register_global_native(context, "__terraceSetExitCode", 1, node_set_exit_code)?;
    register_global_native(context, "__terraceWriteStdout", 1, node_write_stdout)?;
    register_global_native(context, "__terraceWriteStderr", 1, node_write_stderr)?;
    register_global_native(
        context,
        "__terraceReadBuiltinSource",
        1,
        node_read_builtin_source,
    )?;
    register_global_native(
        context,
        "__terraceInitBootstrapRealm",
        0,
        node_init_bootstrap_realm,
    )?;
    register_global_native(context, "__terraceResolveModule", 3, node_resolve_module)?;
    register_global_native(
        context,
        "__terraceRequireEsmNamespace",
        1,
        node_require_esm_namespace,
    )?;
    register_global_native(
        context,
        "__terraceRequireResolveImpl",
        3,
        node_require_resolve,
    )?;
    register_global_native(
        context,
        "__terraceRequireResolvePathsImpl",
        2,
        node_require_resolve_paths,
    )?;
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
    register_global_native(context, "__terraceFsOpen", 3, node_fs_open)?;
    register_global_native(context, "__terraceFsClose", 1, node_fs_close)?;
    register_global_native(context, "__terraceFsReadFd", 3, node_fs_read_fd)?;
    register_global_native(context, "__terraceFsWriteFd", 3, node_fs_write_fd)?;
    register_global_native(context, "__terraceFsTruncateFd", 2, node_fs_truncate_fd)?;
    register_global_native(context, "__terraceFsMkdir", 1, node_fs_mkdir)?;
    register_global_native(context, "__terraceFsReaddir", 1, node_fs_readdir)?;
    register_global_native(context, "__terraceFsStat", 1, node_fs_stat)?;
    register_global_native(context, "__terraceFsLstat", 1, node_fs_lstat)?;
    register_global_native(context, "__terraceFsReadlink", 1, node_fs_readlink)?;
    register_global_native(context, "__terraceFsRealpath", 1, node_fs_realpath)?;
    register_global_native(context, "__terraceFsLink", 2, node_fs_link)?;
    register_global_native(context, "__terraceFsSymlink", 2, node_fs_symlink)?;
    register_global_native(context, "__terraceFsUnlink", 1, node_fs_unlink)?;
    register_global_native(context, "__terraceFsRename", 2, node_fs_rename)?;
    register_global_native(
        context,
        "__terraceCryptoGetHashes",
        0,
        node_crypto_get_hashes,
    )?;
    register_global_native(context, "__terraceCryptoDigest", 1, node_crypto_digest)?;
    register_global_native(context, "__terraceCryptoHmac", 1, node_crypto_hmac)?;
    register_global_native(
        context,
        "__terraceCryptoRandomBytes",
        1,
        node_crypto_random_bytes,
    )?;
    register_global_native(
        context,
        "__terraceCryptoRandomInt",
        2,
        node_crypto_random_int,
    )?;
    register_global_native(
        context,
        "__terraceCryptoTimingSafeEqual",
        2,
        node_crypto_timing_safe_equal,
    )?;
    register_global_native(context, "__terraceCryptoPbkdf2", 1, node_crypto_pbkdf2)?;
    register_global_native(context, "__terraceCryptoHkdf", 1, node_crypto_hkdf)?;
    register_global_native(context, "__terraceCryptoScrypt", 1, node_crypto_scrypt)?;
    register_global_native(context, "__terraceZlibProcess", 4, node_zlib_process)?;
    register_global_native(context, "__terraceZlibCrc32", 2, node_zlib_crc32)?;
    register_global_native(
        context,
        "__terraceZlibStreamCreate",
        2,
        node_zlib_stream_create,
    )?;
    register_global_native(
        context,
        "__terraceZlibStreamProcess",
        3,
        node_zlib_stream_process,
    )?;
    register_global_native(
        context,
        "__terraceZlibStreamReset",
        1,
        node_zlib_stream_reset,
    )?;
    register_global_native(
        context,
        "__terraceZlibStreamSetParams",
        3,
        node_zlib_stream_set_params,
    )?;
    register_global_native(
        context,
        "__terraceZlibStreamClose",
        1,
        node_zlib_stream_close,
    )?;
    register_global_native(
        context,
        "__terraceChildProcessRun",
        1,
        node_child_process_run,
    )?;
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
    let host = active_node_host()?;
    f(&host)
}

fn active_node_host() -> Result<Rc<NodeRuntimeHost>, SandboxError> {
    ACTIVE_NODE_HOST_STACK
        .try_with(|stack| {
            let stack = stack.borrow();
            stack.last().cloned().ok_or_else(|| SandboxError::Service {
                service: "runtime",
                message: "node host bindings are unavailable outside node command execution"
                    .to_string(),
            })
        })
        .map_err(|_| SandboxError::Service {
            service: "runtime",
            message: "node host bindings are unavailable outside node command execution"
                .to_string(),
        })?
        .map_err(|error| error)
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
    if std::env::var_os("TERRACE_NODE_DEBUG_STDERR").is_some() {
        eprintln!("[terrace-node][{bucket}] {detail}");
    }
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
    let seq = state.next_seq;
    state.next_seq = state.next_seq.saturating_add(1);
    let (label, message, data) = parse_node_runtime_event_detail(bucket, &detail);
    let event = NodeRuntimeTraceEvent {
        seq,
        bucket: bucket.to_string(),
        detail: detail.clone(),
        label,
        message,
        data,
    };
    if let Some(snapshot) = node_exception_snapshot_from_event(&event) {
        state.last_exception = Some(snapshot);
    }
    if state.structured_events.len() == NODE_RUNTIME_DEBUG_EVENT_LIMIT {
        state.structured_events.remove(0);
    }
    state.structured_events.push(event.clone());
    if state.recent_events.len() == NODE_RUNTIME_DEBUG_EVENT_LIMIT {
        state.recent_events.remove(0);
    }
    state.recent_events.push(event.render());
    Ok(())
}

fn parse_node_runtime_event_detail(
    bucket: &str,
    detail: &str,
) -> (Option<String>, Option<String>, Option<JsonValue>) {
    if bucket != "js" {
        return (None, None, None);
    }
    let Some((label, rest)) = detail.split_once(' ') else {
        return (Some(detail.to_string()), None, None);
    };
    let label = Some(label.to_string());
    if let Ok(data) = serde_json::from_str::<JsonValue>(rest) {
        return (label, None, Some(data));
    }
    (label, Some(rest.to_string()), None)
}

fn node_exception_snapshot_from_event(
    event: &NodeRuntimeTraceEvent,
) -> Option<NodeRuntimeExceptionSnapshot> {
    if event.bucket != "js" || event.label.as_deref() != Some("exception") {
        return None;
    }
    let data = event.data.clone().unwrap_or(JsonValue::Null);
    Some(NodeRuntimeExceptionSnapshot {
        seq: event.seq,
        source: data
            .get("source")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        module: data
            .get("module")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        entrypoint: data
            .get("entrypoint")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        name: data
            .get("name")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        message: data
            .get("message")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        stack: data
            .get("stack")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        require_stack: data
            .get("requireStack")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        raw: data,
    })
}

fn node_graph_invalidate(host: &NodeRuntimeHost) {
    host.module_graph.borrow_mut().invalidate();
    host.read_snapshot_fs.borrow_mut().take();
}

fn node_readonly_fs(
    host: &NodeRuntimeHost,
) -> Result<Arc<dyn ReadOnlyVfsFileSystem>, SandboxError> {
    if let Some(fs) = host.read_snapshot_fs.borrow().clone() {
        return Ok(fs);
    }
    node_debug_event(host, "fs", "capture_visible_snapshot".to_string())?;
    let snapshot = futures::executor::block_on(host.session.volume().visible_snapshot())?;
    let fs = snapshot.fs();
    *host.read_snapshot_fs.borrow_mut() = Some(fs.clone());
    Ok(fs)
}

fn node_read_stat(host: &NodeRuntimeHost, path: &str) -> Result<Option<Stats>, SandboxError> {
    let fs = node_readonly_fs(host)?;
    futures::executor::block_on(fs.stat(path)).map_err(Into::into)
}

fn node_read_lstat(host: &NodeRuntimeHost, path: &str) -> Result<Option<Stats>, SandboxError> {
    let fs = node_readonly_fs(host)?;
    futures::executor::block_on(fs.lstat(path)).map_err(Into::into)
}

fn node_read_file(host: &NodeRuntimeHost, path: &str) -> Result<Option<Vec<u8>>, SandboxError> {
    let fs = node_readonly_fs(host)?;
    futures::executor::block_on(fs.read_file(path)).map_err(Into::into)
}

fn node_read_readdir(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<Vec<terracedb_vfs::DirEntry>, SandboxError> {
    let fs = node_readonly_fs(host)?;
    futures::executor::block_on(fs.readdir(path)).map_err(Into::into)
}

fn node_read_readlink(host: &NodeRuntimeHost, path: &str) -> Result<String, SandboxError> {
    let fs = node_readonly_fs(host)?;
    futures::executor::block_on(fs.readlink(path)).map_err(Into::into)
}

fn node_graph_stat(host: &NodeRuntimeHost, path: &str) -> Result<Option<Stats>, SandboxError> {
    if let Some(cached) = host.module_graph.borrow().stat_cache.get(path).cloned() {
        return Ok(cached);
    }
    node_debug_event(host, "fs", format!("stat {path}"))?;
    let stats = node_read_stat(host, path)?;
    host.module_graph
        .borrow_mut()
        .stat_cache
        .insert(path.to_string(), stats.clone());
    Ok(stats)
}

fn node_graph_read_file(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<Option<Vec<u8>>, SandboxError> {
    if let Some(cached) = host.module_graph.borrow().file_cache.get(path).cloned() {
        return Ok(cached);
    }
    node_debug_event(host, "fs", format!("read_file {path}"))?;
    let bytes = node_read_file(host, path)?;
    host.module_graph
        .borrow_mut()
        .file_cache
        .insert(path.to_string(), bytes.clone());
    Ok(bytes)
}

fn with_active_node_host<T>(
    host: Rc<NodeRuntimeHost>,
    f: impl FnOnce(&Rc<NodeRuntimeHost>) -> Result<T, SandboxError>,
) -> Result<T, SandboxError> {
    ACTIVE_NODE_HOST_STACK.with(|stack| {
        stack.borrow_mut().push(host.clone());
    });
    let result = f(&host);
    ACTIVE_NODE_HOST_STACK.with(|stack| {
        let _ = stack.borrow_mut().pop();
    });
    result
}

impl NodeCommandModuleLoader {
    fn new(host: Rc<NodeRuntimeHost>) -> Self {
        Self { host }
    }

    fn materialize(
        &self,
        resolved: &NodeResolvedModule,
        context: &mut Context,
    ) -> Result<Module, SandboxError> {
        if let Some(module) = self
            .host
            .materialized_modules
            .borrow()
            .get(&resolved.specifier)
            .cloned()
        {
            return Ok(module);
        }

        let module = match resolved.kind {
            NodeResolvedKind::Builtin | NodeResolvedKind::CommonJs | NodeResolvedKind::Json => {
                synthetic_node_namespace_module(resolved, context)?
            }
            NodeResolvedKind::EsModule => {
                let loaded = self
                    .host
                    .loaded_modules
                    .borrow()
                    .get(&resolved.id)
                    .cloned()
                    .ok_or_else(|| SandboxError::ModuleNotFound {
                        specifier: resolved.id.clone(),
                    })?;
                Module::parse(
                    Source::from_bytes(strip_shebang(&loaded.source).as_bytes())
                        .with_path(&PathBuf::from(loaded.runtime_path.clone())),
                    None,
                    context,
                )
                .map_err(sandbox_execution_error)?
            }
        };
        self.host
            .materialized_modules
            .borrow_mut()
            .insert(resolved.specifier.clone(), module.clone());
        Ok(module)
    }
}

impl BoaModuleLoader for NodeCommandModuleLoader {
    async fn load_imported_module(
        self: Rc<Self>,
        referrer: Referrer,
        request: ModuleRequest,
        context: &RefCell<&mut Context>,
    ) -> JsResult<Module> {
        let requested = request.specifier().to_std_string_escaped();
        let referrer = referrer
            .path()
            .map(|path| path.to_string_lossy().into_owned());
        let resolved = resolve_node_module(
            &self.host,
            &requested,
            referrer.as_deref(),
            NodeResolveMode::Import,
            None,
        )
        .map_err(js_error)?;
        let mut context = context.borrow_mut();
        self.materialize(&resolved, &mut context).map_err(js_error)
    }

    fn init_import_meta(
        self: Rc<Self>,
        import_meta: &boa_engine::object::JsObject,
        module: &Module,
        context: &mut Context,
    ) {
        let Some(path) = module.path() else {
            return;
        };
        let path = path.to_string_lossy().into_owned();
        if let Ok(url) = node_file_url_from_path(&path) {
            let _ = import_meta.set(js_string!("url"), JsValue::from(JsString::from(url)), true, context);
        }
        let resolver = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_copy_closure_with_captures(
                node_import_meta_resolve_for_module,
                JsString::from(path),
            ),
        )
        .name(js_string!("resolve"))
        .length(1)
        .constructor(false)
        .build();
        let _ = import_meta.set(js_string!("resolve"), resolver, true, context);
    }
}

fn synthetic_node_namespace_module(
    resolved: &NodeResolvedModule,
    context: &mut Context,
) -> Result<Module, SandboxError> {
    let namespace = node_namespace_for_resolved(resolved, context)?;
    let exports = node_namespace_export_names(&namespace, context)?;
    let capture = serde_json::to_string(resolved).map_err(sandbox_execution_error)?;
    let path = if matches!(resolved.kind, NodeResolvedKind::Builtin) {
        None
    } else {
        Some(PathBuf::from(resolved.id.clone()))
    };
    Ok(Module::synthetic(
        &exports,
        SyntheticModuleInitializer::from_copy_closure_with_captures(
            |module, encoded, context| {
                let encoded = encoded.to_std_string_escaped();
                let resolved: NodeResolvedModule = serde_json::from_str(&encoded)
                    .map_err(|error| js_error(sandbox_execution_error(error)))?;
                let namespace =
                    node_namespace_for_resolved(&resolved, context).map_err(js_error)?;
                let namespace = namespace.as_object().ok_or_else(|| {
                    js_error(sandbox_execution_error("namespace export is not an object"))
                })?;
                for export_name in node_namespace_export_names(&namespace.clone().into(), context)
                    .map_err(js_error)?
                {
                    let value = namespace
                        .get(export_name.clone(), context)
                        .map_err(|error| js_error(sandbox_execution_error(error)))?;
                    module.set_export(&export_name, value)?;
                }
                Ok(())
            },
            JsString::from(capture),
        ),
        path,
        None,
        context,
    ))
}

fn node_namespace_for_resolved(
    resolved: &NodeResolvedModule,
    context: &mut Context,
) -> Result<JsValue, SandboxError> {
    let helper = context
        .global_object()
        .get(js_string!("__terraceNamespaceForResolved"), context)
        .map_err(sandbox_execution_error)?;
    let helper = helper
        .as_callable()
        .ok_or_else(|| sandbox_execution_error("__terraceNamespaceForResolved is not callable"))?;
    let resolved = JsValue::from_json(
        &serde_json::to_value(resolved).map_err(sandbox_execution_error)?,
        context,
    )
    .map_err(sandbox_execution_error)?;
    helper
        .call(&JsValue::undefined(), &[resolved], context)
        .map_err(sandbox_execution_error)
}

fn node_namespace_export_names(
    namespace: &JsValue,
    context: &mut Context,
) -> Result<Vec<JsString>, SandboxError> {
    let object = namespace
        .as_object()
        .ok_or_else(|| sandbox_execution_error("namespace export is not an object"))?;
    let mut exports = Vec::new();
    for key in object
        .own_property_keys(context)
        .map_err(sandbox_execution_error)?
    {
        match key {
            boa_engine::property::PropertyKey::String(name) => exports.push(name),
            boa_engine::property::PropertyKey::Index(index) => {
                exports.push(JsString::from(index.get().to_string()));
            }
            boa_engine::property::PropertyKey::Symbol(_) => {}
        }
    }
    Ok(exports)
}

fn node_arg_string(args: &[JsValue], index: usize, context: &mut Context) -> JsResult<String> {
    args.get(index)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)
        .map(|value| value.to_std_string_escaped())
}

fn node_arg_i32(args: &[JsValue], index: usize, context: &mut Context) -> JsResult<i32> {
    args.get(index)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
}

fn node_arg_optional_usize(
    args: &[JsValue],
    index: usize,
    context: &mut Context,
) -> JsResult<Option<usize>> {
    let value = args.get(index).cloned().unwrap_or_else(JsValue::undefined);
    if value.is_null() || value.is_undefined() {
        Ok(None)
    } else {
        value.to_length(context).map(|value| Some(value as usize))
    }
}

fn node_arg_json<T>(args: &[JsValue], index: usize, context: &mut Context) -> JsResult<T>
where
    T: DeserializeOwned,
{
    let value = args
        .get(index)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_json(context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .unwrap_or(JsonValue::Null);
    serde_json::from_value(value).map_err(|error| js_error(sandbox_execution_error(error)))
}

fn node_error_with_code(
    context: &mut Context,
    kind: JsNativeError,
    message: impl Into<String>,
    code: &str,
) -> boa_engine::JsError {
    let object = kind.with_message(message.into()).into_opaque(context);
    let _ = object.set(
        js_string!("code"),
        JsValue::from(JsString::from(code)),
        true,
        context,
    );
    boa_engine::JsError::from_opaque(object.into())
}

fn node_file_url_from_path(path: &str) -> Result<String, SandboxError> {
    Url::from_file_path(path)
        .map(|url| url.to_string())
        .map_err(|_| SandboxError::Execution {
            entrypoint: path.to_string(),
            message: format!("failed to convert path `{path}` to file URL"),
        })
}

fn resolve_import_meta_target(
    host: &NodeRuntimeHost,
    module_path: &str,
    specifier: &str,
) -> Result<String, SandboxError> {
    if let Some(builtin) = node_builtin_name(specifier) {
        return Ok(format!("node:{builtin}"));
    }

    if Url::parse(specifier).is_ok() {
        return Ok(specifier.to_string());
    }

    let referrer_path = module_path.to_string();

    if specifier.starts_with('/')
        || matches!(specifier, "." | "..")
        || specifier.starts_with("./")
        || specifier.starts_with("../")
    {
        let base = directory_for_path(&referrer_path);
        let resolved = if specifier.starts_with('/') {
            normalize_node_path(specifier)
        } else {
            resolve_node_path(&base, specifier)
        };
        return node_file_url_from_path(&resolved);
    }

    let resolved =
        resolve_node_module_path(host, specifier, Some(&referrer_path), NodeResolveMode::Import, None)?;
    if node_builtin_name(&resolved).is_some() {
        return Ok(resolved);
    }
    node_file_url_from_path(&resolved)
}

fn node_import_meta_resolve_for_module(
    _this: &JsValue,
    args: &[JsValue],
    module_path: &JsString,
    context: &mut Context,
) -> JsResult<JsValue> {
    let request = node_arg_string(args, 0, context)?;

    let result = node_with_host(|host| {
        resolve_import_meta_target(
            host,
            &module_path.to_std_string_escaped(),
            &request,
        )
    })
    .map_err(|error| match error {
        SandboxError::ModuleNotFound { .. } => node_error_with_code(
            context,
            JsNativeError::error(),
            format!("Cannot find package '{request}' imported from {}", module_path.to_std_string_escaped()),
            "ERR_MODULE_NOT_FOUND",
        ),
        other => node_error_with_code(
            context,
            JsNativeError::error(),
            other.to_string(),
            "ERR_TERRACE_NODE_IMPORT_META_RESOLVE",
        ),
    })?;

    Ok(JsValue::from(JsString::from(result)))
}

fn node_bytes_from_js(value: &JsValue, context: &mut Context) -> JsResult<Vec<u8>> {
    if let Some(object) = value.as_object() {
        if let Ok(array) = JsUint8Array::from_object(object.clone()) {
            return array.to_vec(context);
        }
    }
    value
        .to_string(context)
        .map(|value| value.to_std_string_escaped().into_bytes())
}

fn node_trace_event(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let bucket = args
            .get(0)
            .cloned()
            .unwrap_or_else(JsValue::undefined)
            .to_string(context)
            .map(|value| value.to_std_string_escaped())
            .map_err(sandbox_execution_error)?;
        let detail = args
            .get(1)
            .cloned()
            .unwrap_or_else(JsValue::undefined)
            .to_string(context)
            .map(|value| value.to_std_string_escaped())
            .map_err(sandbox_execution_error)?;
        node_debug_event(host, &bucket, detail)?;
        Ok(JsValue::undefined())
    })
}

fn node_runtime_progress_snapshot(
    host: &NodeRuntimeHost,
) -> (usize, u64, u64, u64, usize, i32, usize, usize) {
    let modules = host.loaded_modules.borrow().len();
    let trace = host.debug_trace.borrow();
    let process = host.process.borrow();
    (
        modules,
        trace.resolve_calls,
        trace.load_calls,
        trace.fs_calls,
        trace.recent_events.len(),
        process.exit_code,
        process.stdout.len(),
        process.stderr.len(),
    )
}

fn drain_node_next_ticks(
    context: &mut Context,
    host: &NodeRuntimeHost,
    entrypoint: &str,
) -> Result<usize, SandboxError> {
    let drainer = context
        .global_object()
        .get(js_string!("__terraceDrainNextTicks"), context)
        .map_err(|error| node_execution_error(entrypoint, host, error))?;
    let drainer = drainer
        .as_callable()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: entrypoint.to_string(),
            message: "__terraceDrainNextTicks is not callable".to_string(),
        })?;
    let drained = drainer
        .call(&JsValue::undefined(), &[], context)
        .map_err(|error| node_execution_error(entrypoint, host, error))?;
    drained
        .to_length(context)
        .map(|count| count as usize)
        .map_err(|error| node_execution_error(entrypoint, host, error))
}

fn drain_node_timers(
    context: &mut Context,
    host: &NodeRuntimeHost,
    entrypoint: &str,
) -> Result<usize, SandboxError> {
    let drainer = context
        .global_object()
        .get(js_string!("__terraceDrainTimers"), context)
        .map_err(|error| node_execution_error(entrypoint, host, error))?;
    let drainer = drainer
        .as_callable()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: entrypoint.to_string(),
            message: "__terraceDrainTimers is not callable".to_string(),
        })?;
    let drained = drainer
        .call(&JsValue::undefined(), &[], context)
        .map_err(|error| node_execution_error(entrypoint, host, error))?;
    drained
        .to_length(context)
        .map(|count| count as usize)
        .map_err(|error| node_execution_error(entrypoint, host, error))
}

fn drain_node_jobs_until_quiescent(
    context: &mut Context,
    host: &NodeRuntimeHost,
    entrypoint: &str,
) -> Result<(), SandboxError> {
    let mut stable_rounds = 0usize;
    let mut previous = node_runtime_progress_snapshot(host);
    for _ in 0..NODE_RUNTIME_JOB_DRAIN_BUDGET {
        let drained_before = drain_node_next_ticks(context, host, entrypoint)?;
        context
            .run_jobs()
            .map_err(|error| node_execution_error(entrypoint, host, error))?;
        let drained_after = drain_node_next_ticks(context, host, entrypoint)?;
        let drained_timers = drain_node_timers(context, host, entrypoint)?;
        let current = node_runtime_progress_snapshot(host);
        if drained_before == 0 && drained_after == 0 && drained_timers == 0 && current == previous {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= 2 {
                return Ok(());
            }
        } else {
            stable_rounds = 0;
            previous = current;
        }
    }
    node_debug_event(
        host,
        "js",
        format!("job-drain-budget-exhausted entrypoint={entrypoint}"),
    )?;
    Ok(())
}

fn node_get_process_info(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "process", "get_process_info".to_string())?;
        JsValue::from_json(&host.process.borrow().to_process_info_json(), context)
            .map_err(sandbox_execution_error)
    })
}

fn node_get_cwd(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        node_debug_event(host, "process", "get_cwd".to_string())?;
        Ok(JsValue::from(JsString::from(
            host.process.borrow().cwd.clone(),
        )))
    })
    .map_err(js_error)
}

fn node_chdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "process", format!("chdir path={path}"))?;
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
        .map_err(|error| {
            js_error(SandboxError::Service {
                service: "runtime",
                message: format!("invalid exit code: {error}"),
            })
        })?;
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

fn node_read_builtin_source(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let name = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "builtin", format!("read_source start specifier={name}"))?;
        let Some(path) = node_upstream_builtin_vfs_path(&name) else {
            node_debug_event(host, "builtin", format!("read_source empty specifier={name}"))?;
            return Ok(JsValue::from(JsString::from(String::new())));
        };
        let bytes = node_read_file(host, &path)?.ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("upstream Node builtin source `{name}` is missing from sandbox VFS"),
        })?;
        let source = String::from_utf8(bytes).map_err(|error| SandboxError::Service {
            service: "node_runtime",
            message: format!("builtin `{name}` is not valid UTF-8: {error}"),
        })?;
        node_debug_event(
            host,
            "builtin",
            format!("read_source done specifier={name} bytes={}", source.len()),
        )?;
        Ok(JsValue::from(JsString::from(source)))
    })
    .map_err(js_error)
}

fn node_init_bootstrap_realm(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    if args
        .first()
        .is_some_and(|value| !value.is_null() && !value.is_undefined())
    {
        return Err(js_error(SandboxError::Service {
            service: "node_runtime",
            message:
                "bootstrap realm initialization only supports real upstream builtins".to_string(),
        }));
    }
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "bootstrap", "init_bootstrap_realm:start".to_string())?;
        let exports = ObjectInitializer::new(context).build();
        let primordials = JsObject::with_null_proto();
        let private_symbols = node_private_symbols_object(context)?;
        let per_isolate_symbols = node_per_isolate_symbols_object(context)?;
        node_store_global(
            context,
            "__terraceNodePerContextExports",
            JsValue::from(exports.clone()),
        )?;
        node_store_global(
            context,
            "__terraceNodePrimordials",
            JsValue::from(primordials.clone()),
        )?;

        for specifier in [
            "internal/per_context/primordials",
            "internal/per_context/domexception",
            "internal/per_context/messageport",
        ] {
            let function = node_compile_builtin_wrapper(
                context,
                host,
                specifier,
                &["exports", "primordials", "privateSymbols", "perIsolateSymbols"],
                None,
            )?;
            let callable = function.as_callable().ok_or_else(|| SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: format!("builtin `{specifier}` did not compile to a callable wrapper"),
            })?;
            callable
                .call(
                    &JsValue::undefined(),
                    &[
                        JsValue::from(exports.clone()),
                        JsValue::from(primordials.clone()),
                        JsValue::from(private_symbols.clone()),
                        JsValue::from(per_isolate_symbols.clone()),
                    ],
                    context,
                )
                .map_err(sandbox_execution_error)?;
        }

        let process = context
            .global_object()
            .get(js_string!("process"), context)
            .map_err(sandbox_execution_error)?;
        let get_linked_binding = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(node_get_linked_binding),
        )
        .name(js_string!("getLinkedBinding"))
        .length(1)
        .constructor(false)
        .build();
        let get_internal_binding = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(node_get_internal_binding),
        )
        .name(js_string!("getInternalBinding"))
        .length(1)
        .constructor(false)
        .build();
        let realm_factory = node_compile_builtin_wrapper(
            context,
            host,
            "internal/bootstrap/realm",
            &["process", "getLinkedBinding", "getInternalBinding", "primordials"],
            Some("; return { internalBinding, BuiltinModule, require: requireBuiltin, requireBuiltin };"),
        )?;
        let callable = realm_factory
            .as_callable()
            .ok_or_else(|| SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: "bootstrap realm did not compile to a callable wrapper".to_string(),
            })?;
        let realm = callable
            .call(
                &JsValue::undefined(),
                &[
                    process,
                    JsValue::from(get_linked_binding),
                    JsValue::from(get_internal_binding),
                    JsValue::from(primordials),
                ],
                context,
            )
            .map_err(sandbox_execution_error)?;
        node_debug_event(host, "bootstrap", "init_bootstrap_realm:done".to_string())?;
        Ok(realm)
    })
}

fn node_store_global(
    context: &mut Context,
    name: &str,
    value: JsValue,
) -> Result<(), SandboxError> {
    context
        .global_object()
        .set(JsString::from(name), value, true, context)
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_symbol(description: &str) -> Result<JsValue, SandboxError> {
    let symbol = JsSymbol::new(Some(JsString::from(description))).ok_or_else(|| {
        SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("failed to create symbol `{description}`"),
        }
    })?;
    Ok(JsValue::from(symbol))
}

fn node_private_symbols_object(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = JsObject::with_null_proto();
    object
        .set(
            js_string!("transfer_mode_private_symbol"),
            node_symbol("node.transfer_mode_private_symbol")?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_per_isolate_symbols_object(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = JsObject::with_null_proto();
    object
        .set(
            js_string!("messaging_clone_symbol"),
            node_symbol("node.messaging_clone_symbol")?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("messaging_deserialize_symbol"),
            node_symbol("node.messaging_deserialize_symbol")?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_builtin_ids(host: &NodeRuntimeHost) -> Result<Vec<String>, SandboxError> {
    if let Some(ids) = host.builtin_ids.borrow().clone() {
        return Ok(ids);
    }
    let mut ids = Vec::new();
    node_collect_builtin_ids(host, &format!("{NODE_UPSTREAM_VFS_ROOT}/lib"), "", &mut ids)?;
    if node_read_stat(host, &format!("{NODE_UPSTREAM_VFS_ROOT}/deps"))?.is_some() {
        node_collect_builtin_ids(
            host,
            &format!("{NODE_UPSTREAM_VFS_ROOT}/deps"),
            "internal/deps",
            &mut ids,
        )?;
    }
    ids.sort();
    ids.dedup();
    *host.builtin_ids.borrow_mut() = Some(ids.clone());
    Ok(ids)
}

fn node_collect_builtin_ids(
    host: &NodeRuntimeHost,
    directory: &str,
    prefix: &str,
    output: &mut Vec<String>,
) -> Result<(), SandboxError> {
    for entry in node_read_readdir(host, directory)? {
        let child_path = format!("{directory}/{}", entry.name);
        match entry.kind {
            FileKind::Directory => {
                let next_prefix = if prefix.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                node_collect_builtin_ids(host, &child_path, &next_prefix, output)?;
            }
            FileKind::File => {
                if let Some(stem) = entry.name.strip_suffix(".js") {
                    let id = if prefix.is_empty() {
                        stem.to_string()
                    } else {
                        format!("{prefix}/{}", stem)
                    };
                    output.push(id);
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn node_read_builtin_source_text(
    host: &NodeRuntimeHost,
    specifier: &str,
) -> Result<String, SandboxError> {
    let path = node_upstream_builtin_vfs_path(specifier).ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: format!("upstream Node builtin source `{specifier}` is missing from sandbox VFS"),
    })?;
    let bytes = node_read_file(host, &path)?.ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: format!("upstream Node builtin source `{specifier}` is missing from sandbox VFS"),
    })?;
    String::from_utf8(bytes).map_err(|error| SandboxError::Service {
        service: "node_runtime",
        message: format!("builtin `{specifier}` is not valid UTF-8: {error}"),
    })
}

fn node_compile_builtin_wrapper(
    context: &mut Context,
    host: &NodeRuntimeHost,
    specifier: &str,
    parameters: &[&str],
    suffix: Option<&str>,
) -> Result<JsValue, SandboxError> {
    let source = node_read_builtin_source_text(host, specifier)?;
    let mut wrapped = format!("(function({}) {{\n{}", parameters.join(", "), source);
    if !wrapped.ends_with('\n') {
        wrapped.push('\n');
    }
    if let Some(suffix) = suffix {
        wrapped.push_str(suffix);
        if !suffix.ends_with('\n') {
            wrapped.push('\n');
        }
    }
    wrapped.push_str(&format!(
        "//# sourceURL=node:{}\n}})",
        specifier.replace('\\', "\\\\")
    ));
    context
        .eval(
            Source::from_bytes(wrapped.as_bytes())
                .with_path(&PathBuf::from(
                    node_upstream_builtin_vfs_path(specifier).unwrap(),
                )),
        )
        .map_err(sandbox_execution_error)
}

fn node_get_linked_binding(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(JsObject::with_null_proto()))
}

fn node_get_internal_binding(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let name = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "bootstrap", format!("getInternalBinding {name}"))?;
        Ok(JsValue::from(node_internal_binding_object(host, context, &name)?))
    })
}

fn node_internal_binding_object(
    host: &NodeRuntimeHost,
    context: &mut Context,
    name: &str,
) -> Result<JsObject, SandboxError> {
    match name {
        "builtins" => node_internal_binding_builtins(host, context),
        "constants" => node_internal_binding_constants(context),
        "options" => node_internal_binding_options(context),
        "types" => node_internal_binding_types(context),
        "module_wrap" => node_internal_binding_module_wrap(context),
        "errors" => node_internal_binding_errors(context),
        "util" => node_internal_binding_util(context),
        "fs" => node_internal_binding_fs(context),
        "blob" => node_internal_binding_blob(context),
        "buffer" => node_internal_binding_buffer(context),
        "url" => node_internal_binding_url(context),
        "url_pattern" => node_internal_binding_url_pattern(context),
        "modules" => node_internal_binding_modules(context),
        "config" => node_internal_binding_config(context),
        "encoding_binding" => node_internal_binding_encoding(context),
        "string_decoder" => node_internal_binding_string_decoder(context),
        "uv" => node_internal_binding_uv(context),
        "os" => node_internal_binding_os(context),
        "credentials" => node_internal_binding_credentials(context),
        "messaging" => node_internal_binding_messaging(context),
        "profiler" => node_internal_binding_profiler(context),
        other => Err(SandboxError::Service {
            service: "node_runtime",
            message: format!("unsupported internal binding: {other}"),
        }),
    }
}

fn node_internal_binding_fs(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let stat_values = node_fs_binding_stat_values(context)?;
    let fs_req_callback = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_fs_req_callback_construct),
    )
    .name(js_string!("FSReqCallback"))
    .length(1)
    .constructor(true)
    .build();
    object
        .set(
            js_string!("FSReqCallback"),
            JsValue::from(fs_req_callback),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("statValues"),
            JsValue::from(stat_values),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("kUsePromises"), JsValue::from(true), true, context)
        .map_err(sandbox_execution_error)?;
    for (name, function) in [
        ("internalModuleStat", node_internal_fs_internal_module_stat as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("stat", node_internal_fs_stat),
        ("lstat", node_internal_fs_lstat),
        ("readlink", node_internal_fs_readlink),
        ("realpath", node_internal_fs_realpath),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(4)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_fs_req_callback_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let object = this.as_object().unwrap_or_else(JsObject::with_null_proto);
    object
        .set(
            js_string!("useBigInt"),
            args.first().cloned().unwrap_or_else(JsValue::undefined),
            true,
            context,
        )?;
    Ok(JsValue::from(object))
}

fn node_fs_binding_stat_values(context: &mut Context) -> Result<JsObject, SandboxError> {
    let existing = context
        .global_object()
        .get(js_string!("__terraceNodeFsStatValues"), context)
        .map_err(sandbox_execution_error)?;
    if let Some(object) = existing.as_object() {
        return Ok(object);
    }
    let stat_values = JsValue::from_json(&serde_json::json!(vec![0.0; 18]), context)
        .map_err(sandbox_execution_error)?;
    let object = stat_values.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "failed to allocate fs statValues scratch array".to_string(),
    })?;
    node_store_global(context, "__terraceNodeFsStatValues", stat_values)?;
    Ok(object)
}

fn node_fs_mode_with_kind(stats: &terracedb_vfs::Stats) -> u32 {
    let kind_bits = match stats.kind {
        FileKind::File => 0o100000,
        FileKind::Directory => 0o040000,
        FileKind::Symlink => 0o120000,
    };
    if stats.mode & 0o170000 == 0 {
        stats.mode | kind_bits
    } else {
        stats.mode
    }
}

fn node_timestamp_parts(timestamp_ms: u64) -> (f64, f64) {
    let seconds = timestamp_ms / 1000;
    let nanos = (timestamp_ms % 1000) * 1_000_000;
    (seconds as f64, nanos as f64)
}

fn node_fill_fs_stat_values(
    context: &mut Context,
    target: &JsObject,
    stats: &terracedb_vfs::Stats,
) -> Result<(), SandboxError> {
    let (atime_sec, atime_nsec) = node_timestamp_parts(stats.accessed_at.get());
    let (mtime_sec, mtime_nsec) = node_timestamp_parts(stats.modified_at.get());
    let (ctime_sec, ctime_nsec) = node_timestamp_parts(stats.changed_at.get());
    let (birth_sec, birth_nsec) = node_timestamp_parts(stats.created_at.get());
    let values = [
        0.0,
        node_fs_mode_with_kind(stats) as f64,
        stats.nlink as f64,
        stats.uid as f64,
        stats.gid as f64,
        stats.rdev as f64,
        4096.0,
        stats.inode.get() as f64,
        stats.size as f64,
        stats.size.div_ceil(512) as f64,
        atime_sec,
        atime_nsec,
        mtime_sec,
        mtime_nsec,
        ctime_sec,
        ctime_nsec,
        birth_sec,
        birth_nsec,
    ];
    for (index, value) in values.into_iter().enumerate() {
        target
            .set(index as u32, JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(())
}

fn node_fs_req_complete(
    req: Option<&JsValue>,
    context: &mut Context,
    result: JsValue,
) -> Result<JsValue, SandboxError> {
    let Some(req) = req else {
        return Ok(result);
    };
    let Some(req_object) = req.as_object() else {
        return Ok(result);
    };
    let callback = req_object
        .get(js_string!("oncomplete"), context)
        .map_err(sandbox_execution_error)?;
    if let Some(callable) = callback.as_callable() {
        callable
            .call(req, &[JsValue::null(), result], context)
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::undefined())
    } else {
        Ok(result)
    }
}

fn node_internal_fs_internal_module_stat(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let stats = node_read_stat(host, &path)?;
        let code = match stats.map(|value| value.kind) {
            Some(FileKind::File) | Some(FileKind::Symlink) => 0,
            Some(FileKind::Directory) => 1,
            None => -1,
        };
        Ok(JsValue::from(code))
    })
    .map_err(js_error)
}

fn node_internal_fs_stat_like(
    context: &mut Context,
    args: &[JsValue],
    lstat: bool,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let req = args.get(2);
    node_with_host_js(context, |host, context| {
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let stats = if lstat {
            node_read_lstat(host, &path)?
        } else {
            node_read_stat(host, &path)?
        };
        let Some(stats) = stats else {
            return node_fs_req_complete(req, context, JsValue::undefined());
        };
        let stat_values = node_fs_binding_stat_values(context)?;
        node_fill_fs_stat_values(context, &stat_values, &stats)?;
        node_fs_req_complete(req, context, JsValue::from(stat_values))
    })
}

fn node_internal_fs_stat(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let _ = this;
    node_internal_fs_stat_like(context, args, false)
}

fn node_internal_fs_lstat(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let _ = this;
    node_internal_fs_stat_like(context, args, true)
}

fn node_internal_fs_readlink(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let req = args.get(2);
    node_with_host(|host| {
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let target = node_read_readlink(host, &path)?;
        Ok(JsValue::from(JsString::from(target)))
    })
    .and_then(|result| node_fs_req_complete(req, context, result))
    .map_err(js_error)
}

fn node_internal_fs_realpath(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let req = args.get(2);
    node_with_host(|host| {
        let resolved = resolve_node_path(&host.process.borrow().cwd, &path);
        Ok(JsValue::from(JsString::from(resolved)))
    })
    .and_then(|result| node_fs_req_complete(req, context, result))
    .map_err(js_error)
}

fn node_internal_binding_builtins(
    host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let ids = node_builtin_ids(host)?;
    let ids = JsValue::from_json(&serde_json::json!(ids), context).map_err(sandbox_execution_error)?;
    let object = ObjectInitializer::new(context).build();
    let compile = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_builtins_compile_function),
    )
    .name(js_string!("compileFunction"))
    .length(1)
    .constructor(false)
    .build();
    let set_loaders = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_builtins_set_internal_loaders),
    )
    .name(js_string!("setInternalLoaders"))
    .length(2)
    .constructor(false)
    .build();
    object
        .set(js_string!("builtinIds"), ids, true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("config"),
            JsValue::from(JsString::from(
                r#"{"target_defaults":{"default_configuration":"Release"},"variables":{"v8_enable_i18n_support":0}}"#,
            )),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("compileFunction"), JsValue::from(compile), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("setInternalLoaders"),
            JsValue::from(set_loaders),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_blob(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for name in [
        "createBlob",
        "createBlobFromFilePath",
        "concat",
        "getDataObject",
    ] {
        let value = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(node_blob_unimplemented),
        )
        .name(JsString::from(name))
        .length(1)
        .constructor(false)
        .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_buffer(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    object
        .set(js_string!("kMaxLength"), JsValue::from(i32::MAX), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("kStringMaxLength"),
            JsValue::from(i32::MAX),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        (
            "createUnsafeArrayBuffer",
            1usize,
            node_buffer_create_unsafe_array_buffer
                as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>,
        ),
        ("byteLengthUtf8", 1usize, node_buffer_byte_length_utf8),
        ("compare", 2usize, node_buffer_unimplemented),
        ("compareOffset", 6usize, node_buffer_unimplemented),
        ("copy", 5usize, node_buffer_unimplemented),
        ("fill", 5usize, node_buffer_unimplemented),
        ("isAscii", 1usize, node_buffer_unimplemented),
        ("isUtf8", 1usize, node_buffer_unimplemented),
        ("indexOfBuffer", 5usize, node_buffer_unimplemented),
        ("indexOfNumber", 4usize, node_buffer_unimplemented),
        ("indexOfString", 5usize, node_buffer_unimplemented),
        ("swap16", 1usize, node_buffer_unimplemented),
        ("swap32", 1usize, node_buffer_unimplemented),
        ("swap64", 1usize, node_buffer_unimplemented),
        ("atob", 1usize, node_buffer_unimplemented),
        ("btoa", 1usize, node_buffer_unimplemented),
        ("asciiSlice", 3usize, node_buffer_unimplemented),
        ("base64Slice", 3usize, node_buffer_unimplemented),
        ("base64urlSlice", 3usize, node_buffer_unimplemented),
        ("latin1Slice", 3usize, node_buffer_unimplemented),
        ("hexSlice", 3usize, node_buffer_unimplemented),
        ("ucs2Slice", 3usize, node_buffer_unimplemented),
        ("utf8Slice", 3usize, node_buffer_unimplemented),
        ("asciiWriteStatic", 4usize, node_buffer_unimplemented),
        ("base64Write", 4usize, node_buffer_unimplemented),
        ("base64urlWrite", 4usize, node_buffer_unimplemented),
        ("latin1WriteStatic", 4usize, node_buffer_unimplemented),
        ("hexWrite", 4usize, node_buffer_unimplemented),
        ("ucs2Write", 4usize, node_buffer_unimplemented),
        ("utf8WriteStatic", 4usize, node_buffer_unimplemented),
    ] {
        let value = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(function),
        )
        .name(JsString::from(name))
        .length(length)
        .constructor(false)
        .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_url(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let can_parse = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_url_can_parse),
    )
    .name(js_string!("canParse"))
    .length(2)
    .constructor(false)
    .build();
    object
        .set(js_string!("canParse"), JsValue::from(can_parse), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_url_pattern(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let prototype = JsObject::with_null_proto();
    for (name, function) in [
        ("test", node_url_pattern_test as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("exec", node_url_pattern_exec),
    ] {
        let method = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(1)
            .constructor(false)
            .build();
        prototype
            .set(JsString::from(name), JsValue::from(method), true, context)
            .map_err(sandbox_execution_error)?;
    }
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_url_pattern_construct),
    )
    .name(js_string!("URLPattern"))
    .length(2)
    .constructor(true)
    .build();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("URLPattern"), JsValue::from(constructor), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_modules(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, length, function) in [
        ("enableCompileCache", 0usize, node_modules_enable_compile_cache as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("getCompileCacheDir", 0usize, node_modules_get_compile_cache_dir),
        ("compileCacheStatus", 0usize, node_modules_compile_cache_status),
        ("flushCompileCache", 0usize, node_noop),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_config(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, value) in [
        ("bits", JsValue::from(64)),
        ("hasIntl", JsValue::from(false)),
        ("hasInspector", JsValue::from(false)),
        ("hasNodeOptions", JsValue::from(false)),
        ("hasOpenSSL", JsValue::from(false)),
        ("hasSmallICU", JsValue::from(false)),
        ("hasTracing", JsValue::from(false)),
        ("noBrowserGlobals", JsValue::from(true)),
    ] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_encoding(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let encode_into_results = node_encoding_encode_into_results(context)?;
    object
        .set(
            js_string!("encodeIntoResults"),
            JsValue::from(encode_into_results),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("encodeInto", 2usize, node_encoding_encode_into as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("encodeUtf8String", 1usize, node_encoding_encode_utf8_string),
        ("decodeUTF8", 3usize, node_encoding_decode_utf8),
        ("toASCII", 1usize, node_encoding_to_ascii),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_constants(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let constants = JsValue::from_json(
        &serde_json::json!({
            "os": {
                "UV_UDP_REUSEADDR": 4,
                "dlopen": {},
                "errno": {
                    "EACCES": 13,
                    "EEXIST": 17,
                    "EINVAL": 22,
                    "EISDIR": 21,
                    "ENOENT": 2,
                    "ENOTDIR": 20,
                    "EPERM": 1
                },
                "priority": {
                    "PRIORITY_LOW": 19,
                    "PRIORITY_BELOW_NORMAL": 10,
                    "PRIORITY_NORMAL": 0,
                    "PRIORITY_ABOVE_NORMAL": -7,
                    "PRIORITY_HIGH": -14,
                    "PRIORITY_HIGHEST": -20
                },
                "signals": {
                    "SIGABRT": 6,
                    "SIGBREAK": 21,
                    "SIGFPE": 8,
                    "SIGHUP": 1,
                    "SIGILL": 4,
                    "SIGINT": 2,
                    "SIGKILL": 9,
                    "SIGSEGV": 11,
                    "SIGTERM": 15,
                    "SIGUSR1": 10,
                    "SIGUSR2": 12
                }
            },
            "fs": {
                "O_RDONLY": 0,
                "O_WRONLY": 1,
            "O_RDWR": 2,
            "O_CREAT": 64,
            "O_EXCL": 128,
            "O_TRUNC": 512,
            "O_APPEND": 1024,
            "O_SYNC": 1052672,
            "F_OK": 0,
            "R_OK": 4,
            "W_OK": 2,
            "X_OK": 1,
            "COPYFILE_EXCL": 1,
            "COPYFILE_FICLONE": 2,
            "COPYFILE_FICLONE_FORCE": 4,
            "S_IFMT": 61440,
            "S_IFREG": 32768,
            "S_IFDIR": 16384,
            "S_IFCHR": 8192,
            "S_IFBLK": 24576,
            "S_IFIFO": 4096,
            "S_IFLNK": 40960,
            "S_IFSOCK": 49152,
            "UV_FS_SYMLINK_DIR": 1,
            "UV_FS_SYMLINK_JUNCTION": 2,
            "UV_DIRENT_UNKNOWN": 0,
            "UV_DIRENT_FILE": 1,
            "UV_DIRENT_DIR": 2,
            "UV_DIRENT_LINK": 3,
            "UV_DIRENT_FIFO": 4,
            "UV_DIRENT_SOCKET": 5,
            "UV_DIRENT_CHAR": 6,
            "UV_DIRENT_BLOCK": 7
        },
        "crypto": {},
        "zlib": {},
        "tls": {},
        "internal": {
            "EXTENSIONLESS_FORMAT_JAVASCRIPT": 0,
            "EXTENSIONLESS_FORMAT_WASM": 1
        }}),
        context,
    )
    .map_err(sandbox_execution_error)?;
    let constants = constants.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "internalBinding('constants') payload must be an object".to_string(),
    })?;
    object
        .set(
            js_string!("os"),
            constants
                .get(js_string!("os"), context)
                .map_err(sandbox_execution_error)?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for name in ["fs", "crypto", "zlib", "tls", "internal"] {
        object
            .set(
                JsString::from(name),
                constants
                    .get(JsString::from(name), context)
                    .map_err(sandbox_execution_error)?,
                true,
                context,
            )
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_options(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, length, function) in [
        ("getCLIOptionsValues", 0usize, node_options_get_cli_options_values as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("getCLIOptionsInfo", 0usize, node_options_get_cli_options_info),
        ("getOptionsAsFlags", 0usize, node_options_get_options_as_flags),
        ("getEmbedderOptions", 0usize, node_options_get_embedder_options),
        ("getEnvOptionsInputType", 0usize, node_options_get_env_options_input_type),
        ("getNamespaceOptionsInputType", 0usize, node_options_get_namespace_options_input_type),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_types(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, function) in [
        ("isArrayBufferView", node_type_is_array_buffer_view as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("isAsyncFunction", node_type_is_async_function),
        ("isNativeError", node_type_is_native_error),
        ("isPromise", node_type_is_promise),
        ("isProxy", node_type_is_proxy),
        ("isRegExp", node_type_is_regexp),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(1)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_module_wrap(context: &mut Context) -> Result<JsObject, SandboxError> {
    let binding = ObjectInitializer::new(context).build();
    let prototype = JsObject::with_null_proto();
    for (name, function) in [
        ("instantiate", node_module_wrap_instantiate as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("evaluate", node_module_wrap_evaluate),
        ("setExport", node_module_wrap_set_export),
    ] {
        let method = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(1)
            .constructor(false)
            .build();
        prototype
            .set(JsString::from(name), JsValue::from(method), true, context)
            .map_err(sandbox_execution_error)?;
    }
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_module_wrap_construct),
    )
    .name(js_string!("ModuleWrap"))
    .length(4)
    .constructor(true)
    .build();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype), true, context)
        .map_err(sandbox_execution_error)?;
    binding
        .set(js_string!("ModuleWrap"), JsValue::from(constructor), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(binding)
}

fn node_internal_binding_errors(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, function) in [
        ("setEnhanceStackForFatalException", node_noop as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("setPrepareStackTraceCallback", node_noop),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(1)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_util(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let private_symbols = JsObject::with_null_proto();
    let constants = JsValue::from_json(
        &serde_json::json!({
            "ALL_PROPERTIES": 0,
            "ONLY_ENUMERABLE": 2,
            "SKIP_SYMBOLS": 16,
            "kPending": 0,
            "kFulfilled": 1,
            "kRejected": 2
        }),
        context,
    )
    .map_err(sandbox_execution_error)?;
    for (name, description) in [
        ("arrow_message_private_symbol", "node.arrow_message_private_symbol"),
        ("decorated_private_symbol", "node.decorated_private_symbol"),
        ("exit_info_private_symbol", "node.exit_info_private_symbol"),
        ("host_defined_option_symbol", "node.host_defined_option_symbol"),
    ] {
        private_symbols
            .set(JsString::from(name), node_symbol(description)?, true, context)
            .map_err(sandbox_execution_error)?;
    }
    object
        .set(
            js_string!("privateSymbols"),
            JsValue::from(private_symbols),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("constants"), constants, true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("constructSharedArrayBuffer", 1usize, node_util_construct_shared_array_buffer as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("guessHandleType", 1usize, node_util_guess_handle_type),
        ("defineLazyProperties", 4usize, node_util_define_lazy_properties),
        ("getOwnNonIndexProperties", 2usize, node_util_get_own_non_index_properties),
        ("isInsideNodeModules", 1usize, node_util_is_inside_node_modules),
        ("arrayBufferViewHasBuffer", 1usize, node_util_array_buffer_view_has_buffer),
        ("previewEntries", 2usize, node_util_preview_entries),
        ("getCallerLocation", 0usize, node_util_get_caller_location),
        (
            "shouldAbortOnUncaughtToggle",
            0usize,
            node_util_should_abort_on_uncaught_toggle,
        ),
        ("getPromiseDetails", 1usize, node_util_get_promise_details),
        ("getProxyDetails", 2usize, node_util_get_proxy_details),
        ("getConstructorName", 1usize, node_util_get_constructor_name),
        ("getExternalValue", 1usize, node_util_get_external_value),
        ("sleep", 1usize, node_noop),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_string_decoder(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let encodings = JsValue::from_json(
        &serde_json::json!([
            "ascii",
            "base64",
            "base64url",
            "hex",
            "latin1",
            "ucs2",
            "ucs-2",
            "utf16le",
            "utf-16le",
            "utf8",
            "utf-8"
        ]),
        context,
    )
    .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("encodings"), encodings, true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_uv(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let get_error_map = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_uv_get_error_map),
    )
    .name(js_string!("getErrorMap"))
    .length(0)
    .constructor(false)
    .build();
    object
        .set(js_string!("getErrorMap"), JsValue::from(get_error_map), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_os(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, length, function) in [
        ("getAvailableParallelism", 0usize, node_os_get_available_parallelism as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("getCPUs", 0usize, node_os_get_cpus),
        ("getFreeMem", 0usize, node_os_get_free_mem),
        ("getHomeDirectory", 0usize, node_os_get_home_directory),
        ("getHostname", 0usize, node_os_get_hostname),
        ("getInterfaceAddresses", 0usize, node_os_get_interface_addresses),
        ("getLoadAvg", 0usize, node_os_get_load_avg),
        ("getPriority", 0usize, node_os_get_priority),
        ("getOSInformation", 0usize, node_os_get_os_information),
        ("getTotalMem", 0usize, node_os_get_total_mem),
        ("getUserInfo", 0usize, node_os_get_user_info),
        ("getUptime", 0usize, node_os_get_uptime),
        ("setPriority", 2usize, node_os_set_priority),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    object
        .set(js_string!("isBigEndian"), JsValue::from(false), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_credentials(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let get_temp_dir = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_credentials_get_temp_dir),
    )
    .name(js_string!("getTempDir"))
    .length(0)
    .constructor(false)
    .build();
    object
        .set(js_string!("getTempDir"), JsValue::from(get_temp_dir), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_messaging(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let exports = context
        .global_object()
        .get(js_string!("__terraceNodePerContextExports"), context)
        .map_err(sandbox_execution_error)?;
    let exports = exports.as_object().unwrap_or_else(JsObject::with_null_proto);
    let dom_exception = exports
        .get(js_string!("DOMException"), context)
        .map_err(sandbox_execution_error)?;
    let emit_message = exports
        .get(js_string!("emitMessage"), context)
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("DOMException"), dom_exception, true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("emitMessage"), emit_message, true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_profiler(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for name in ["setCoverageDirectory", "setSourceMapCacheGetter"] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(node_noop))
            .name(JsString::from(name))
            .length(1)
            .constructor(false)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_builtins_compile_function(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let id = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_compile_builtin_wrapper(
            context,
            host,
            &id,
            &["exports", "require", "module", "process", "internalBinding", "primordials"],
            None,
        )
    })
}

fn node_builtins_set_internal_loaders(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let internal_binding_loader = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let builtin_require = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    node_store_global(
        context,
        "__terraceInternalBindingLoader",
        internal_binding_loader,
    )
    .map_err(js_error)?;
    node_store_global(context, "__terraceBuiltinRequire", builtin_require).map_err(js_error)?;
    Ok(JsValue::undefined())
}

fn node_upstream_builtin_vfs_path(specifier: &str) -> Option<String> {
    if specifier.is_empty() {
        return None;
    }
    let mut relative = if let Some(remainder) = specifier.strip_prefix("internal/deps/") {
        let mut path = PathBuf::from("deps");
        path.push(remainder);
        path
    } else {
        let mut path = PathBuf::from("lib");
        path.push(specifier);
        path
    };
    relative.set_extension("js");
    let relative = relative.to_string_lossy().replace('\\', "/");
    Some(format!("{NODE_UPSTREAM_VFS_ROOT}/{relative}"))
}

fn node_noop(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    Ok(JsValue::undefined())
}

fn node_buffer_create_unsafe_array_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let size = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    Ok(JsValue::from(JsArrayBuffer::new(size, context)?))
}

fn node_buffer_byte_length_utf8(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    Ok(JsValue::from(input.len() as i32))
}

fn node_buffer_unimplemented(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Err(js_error(SandboxError::Service {
        service: "node_runtime",
        message: "internal buffer binding operation is not implemented yet".to_string(),
    }))
}

fn node_blob_unimplemented(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Err(js_error(SandboxError::Service {
        service: "node_runtime",
        message: "internal blob binding is not implemented yet".to_string(),
    }))
}

fn node_url_can_parse(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = node_arg_string(args, 0, context)?;
    let base = if args
        .get(1)
        .is_some_and(|value| !value.is_null() && !value.is_undefined())
    {
        Some(node_arg_string(args, 1, context)?)
    } else {
        None
    };
    let parsed = base
        .as_deref()
        .map_or_else(|| Url::parse(&input), |base| Url::parse(base).and_then(|base| base.join(&input)));
    Ok(JsValue::from(parsed.is_ok()))
}

fn node_url_pattern_construct(
    this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(this.clone())
}

fn node_url_pattern_test(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(false))
}

fn node_url_pattern_exec(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::null())
}

fn node_modules_enable_compile_cache(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_modules_get_compile_cache_dir(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(JsString::from("")))
}

fn node_modules_compile_cache_status(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_global_escape(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    fn is_unescaped(cp: u16) -> bool {
        let Ok(cp) = u8::try_from(cp) else {
            return false;
        };
        cp.is_ascii_alphanumeric() || [b'_', b'@', b'*', b'+', b'-', b'.', b'/'].contains(&cp)
    }

    let string = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?;
    let mut utf16 = Vec::with_capacity(string.len());
    for cp in &string {
        if is_unescaped(cp) {
            utf16.push(cp);
            continue;
        }
        let escaped = if cp < 256 {
            format!("%{cp:02X}")
        } else {
            format!("%u{cp:04X}")
        };
        utf16.extend(escaped.encode_utf16());
    }
    Ok(js_string!(&utf16[..]).into())
}

fn node_global_unescape(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    fn to_hex_digit(cp: u16) -> Option<u16> {
        char::from_u32(u32::from(cp))
            .and_then(|c| c.to_digit(16))
            .and_then(|digit| u16::try_from(digit).ok())
    }

    #[derive(Clone)]
    struct PeekableN<I, const N: usize>
    where
        I: Iterator,
    {
        iterator: I,
        buffer: [I::Item; N],
        buffered_end: usize,
    }

    impl<I, const N: usize> PeekableN<I, N>
    where
        I: Iterator,
        I::Item: Default + Copy,
    {
        fn new(iterator: I) -> Self {
            Self {
                iterator,
                buffer: [I::Item::default(); N],
                buffered_end: 0,
            }
        }

        fn peek_n(&mut self, count: usize) -> &[I::Item] {
            if count <= self.buffered_end {
                return &self.buffer[..count];
            }
            for _ in 0..(count - self.buffered_end) {
                let Some(next) = self.iterator.next() else {
                    return &self.buffer[..self.buffered_end];
                };
                self.buffer[self.buffered_end] = next;
                self.buffered_end += 1;
            }
            &self.buffer[..count]
        }
    }

    impl<I, const N: usize> Iterator for PeekableN<I, N>
    where
        I: Iterator,
        I::Item: Copy,
    {
        type Item = I::Item;

        fn next(&mut self) -> Option<Self::Item> {
            if self.buffered_end > 0 {
                let item = self.buffer[0];
                self.buffer.rotate_left(1);
                self.buffered_end -= 1;
                return Some(item);
            }
            self.iterator.next()
        }
    }

    let string = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?;
    let mut utf16 = Vec::with_capacity(string.len());
    let mut codepoints = PeekableN::<_, 6>::new(string.iter());

    loop {
        let Some(cp) = codepoints.next() else {
            break;
        };
        if cp != u16::from(b'%') {
            utf16.push(cp);
            continue;
        }

        let Some(unescaped_cp) = (|| match *codepoints.peek_n(5) {
            [u, n1, n2, n3, n4] if u == u16::from(b'u') => {
                let n1 = to_hex_digit(n1)?;
                let n2 = to_hex_digit(n2)?;
                let n3 = to_hex_digit(n3)?;
                let n4 = to_hex_digit(n4)?;
                for _ in 0..5 {
                    codepoints.next();
                }
                Some((n1 << 12) + (n2 << 8) + (n3 << 4) + n4)
            }
            [n1, n2, ..] => {
                let n1 = to_hex_digit(n1)?;
                let n2 = to_hex_digit(n2)?;
                for _ in 0..2 {
                    codepoints.next();
                }
                Some((n1 << 4) + n2)
            }
            _ => None,
        })() else {
            utf16.push(u16::from(b'%'));
            continue;
        };

        utf16.push(unescaped_cp);
    }

    Ok(js_string!(&utf16[..]).into())
}

fn node_options_get_cli_options_values(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(
        &serde_json::json!({
            "--experimental-quic": false,
            "--preserve-symlinks": false,
            "--preserve-symlinks-main": false,
            "--pending-deprecation": false,
            "--no-deprecation": false,
            "--require-module": false
        }),
        context,
    )
}

fn node_options_get_cli_options_info(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(&serde_json::json!({}), context)
}

fn node_options_get_options_as_flags(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(&serde_json::json!([]), context)
}

fn node_options_get_embedder_options(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(
        &serde_json::json!({ "noGlobalSearchPaths": true }),
        context,
    )
}

fn node_options_get_env_options_input_type(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(&serde_json::json!([]), context)
}

fn node_options_get_namespace_options_input_type(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(&serde_json::json!([]), context)
}

fn node_type_is_array_buffer_view(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first().is_some_and(|value| value.as_object().is_some_and(|object| {
            JsUint8Array::from_object(object.clone()).is_ok()
        })),
    ))
}

fn node_type_is_async_function(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(value) = args.first() else {
        return Ok(JsValue::from(false));
    };
    let Some(object) = value.as_object() else {
        return Ok(JsValue::from(false));
    };
    let constructor = object
        .get(js_string!("constructor"), context)
        ?;
    let name = constructor
        .as_object()
        .and_then(|object| object.get(js_string!("name"), context).ok())
        .and_then(|value| value.as_string().map(|value| value.to_std_string_escaped()));
    Ok(JsValue::from(name.as_deref() == Some("AsyncFunction")))
}

fn node_type_is_native_error(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(args.first().is_some_and(JsValue::is_object)))
}

fn node_type_is_promise(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .and_then(JsValue::as_object)
            .is_some_and(|object| JsPromise::from_object(object.clone()).is_ok()),
    ))
}

fn node_type_is_proxy(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(false))
}

fn node_type_is_regexp(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(value) = args.first() else {
        return Ok(JsValue::from(false));
    };
    let Some(object) = value.as_object() else {
        return Ok(JsValue::from(false));
    };
    let tag = object
        .get(js_string!("constructor"), context)
        ?
        .as_object()
        .and_then(|object| object.get(js_string!("name"), context).ok())
        .and_then(|value| value.as_string().map(|value| value.to_std_string_escaped()));
    Ok(JsValue::from(tag.as_deref() == Some("RegExp")))
}

fn node_module_wrap_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let object = this.as_object().unwrap_or_else(JsObject::with_null_proto);
    object
        .set(
            js_string!("url"),
            args.first().cloned().unwrap_or_else(JsValue::undefined),
            true,
            context,
        )
        ?;
    object
        .set(
            js_string!("exportKeys"),
            args.get(2).cloned().unwrap_or_else(JsValue::undefined),
            true,
            context,
        )
        ?;
    object
        .set(
            js_string!("evaluateCallback"),
            args.get(3).cloned().unwrap_or_else(JsValue::undefined),
            true,
            context,
        )
        ?;
    object
        .set(
            js_string!("exports"),
            JsValue::from(JsObject::with_null_proto()),
            true,
            context,
        )
        ?;
    Ok(JsValue::from(object))
}

fn node_module_wrap_instantiate(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::undefined())
}

fn node_module_wrap_evaluate(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let callback = object
        .get(js_string!("evaluateCallback"), context)
        ?;
    if let Some(callable) = callback.as_callable() {
        callable.call(this, &[], context)?;
    }
    Ok(JsValue::undefined())
}

fn node_module_wrap_set_export(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let exports = object
        .get(js_string!("exports"), context)
        ?;
    let Some(exports) = exports.as_object() else {
        return Ok(JsValue::undefined());
    };
    let name = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?;
    let value = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    exports
        .set(name, value, true, context)
        ?;
    Ok(JsValue::undefined())
}

fn node_util_construct_shared_array_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let size = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    let script = format!("new ArrayBuffer({size})");
    context
        .eval(Source::from_bytes(script.as_bytes()))
        
}

fn node_util_guess_handle_type(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(JsString::from("UNKNOWN")))
}

fn node_util_define_lazy_properties(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::undefined())
}

fn node_util_get_own_non_index_properties(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(target) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::from(JsArray::new(context)?));
    };
    let filter = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)?;
    let skip_symbols = filter & 16 != 0;
    let keys = target.own_property_keys(context)?;
    let mut result = Vec::new();
    for key in keys {
        match key {
            boa_engine::property::PropertyKey::Index(_) => {}
            boa_engine::property::PropertyKey::String(name) => result.push(JsValue::from(name)),
            boa_engine::property::PropertyKey::Symbol(symbol) if !skip_symbols => {
                result.push(JsValue::from(symbol));
            }
            boa_engine::property::PropertyKey::Symbol(_) => {}
        }
    }
    Ok(JsValue::from(JsArray::from_iter(result, context)))
}

fn node_util_is_inside_node_modules(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(false))
}

fn node_util_array_buffer_view_has_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::from(false));
    };
    if JsUint8Array::from_object(object.clone()).is_ok() {
        return Ok(JsValue::from(true));
    }
    let buffer = object.get(js_string!("buffer"), context)?;
    Ok(JsValue::from(!buffer.is_undefined() && !buffer.is_null()))
}

fn node_util_preview_entries(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(JsArray::new(context)?))
}

fn node_util_get_caller_location(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::undefined())
}

fn node_util_should_abort_on_uncaught_toggle(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(false))
}

fn node_util_get_promise_details(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    let Ok(promise) = JsPromise::from_object(object.clone()) else {
        return Ok(JsValue::undefined());
    };
    let values = match promise.state() {
        PromiseState::Pending => vec![JsValue::from(0)],
        PromiseState::Fulfilled(value) => vec![JsValue::from(1), value.clone()],
        PromiseState::Rejected(value) => vec![JsValue::from(2), value.clone()],
    };
    Ok(JsValue::from(JsArray::from_iter(values, context)))
}

fn node_util_get_proxy_details(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::undefined())
}

fn node_util_get_constructor_name(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    let constructor = object.get(js_string!("constructor"), context)?;
    let Some(constructor) = constructor.as_object() else {
        return Ok(JsValue::undefined());
    };
    constructor.get(js_string!("name"), context)
}

fn node_util_get_external_value(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::undefined())
}

fn node_encoding_encode_into_results(context: &mut Context) -> Result<JsObject, SandboxError> {
    let existing = context
        .global_object()
        .get(js_string!("__terraceNodeEncodingEncodeIntoResults"), context)
        .map_err(sandbox_execution_error)?;
    if let Some(object) = existing.as_object() {
        return Ok(object);
    }
    let values = JsValue::from_json(&serde_json::json!([0, 0]), context)
        .map_err(sandbox_execution_error)?;
    let object = values.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "failed to allocate encoding encodeIntoResults scratch array".to_string(),
    })?;
    node_store_global(context, "__terraceNodeEncodingEncodeIntoResults", values)?;
    Ok(object)
}

fn node_encoding_encode_into(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let src = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    let Some(dest_object) = args.get(1).and_then(JsValue::as_object) else {
        return Err(js_error(sandbox_execution_error(
            "encoding_binding.encodeInto requires Uint8Array destination",
        )));
    };
    let dest = JsUint8Array::from_object(dest_object.clone())
        .map_err(|_| {
            js_error(sandbox_execution_error(
                "encoding_binding.encodeInto requires Uint8Array destination",
            ))
        })?;
    let capacity = dest.length(context)?;
    let mut read = 0usize;
    let mut written = 0usize;
    for ch in src.chars() {
        let mut encoded = [0u8; 4];
        let chunk = ch.encode_utf8(&mut encoded).as_bytes();
        if written + chunk.len() > capacity {
            break;
        }
        for byte in chunk {
            dest_object.set(written as u32, JsValue::from(*byte), true, context)?;
            written += 1;
        }
        read += ch.len_utf16();
    }
    let results = node_encoding_encode_into_results(context).map_err(js_error)?;
    results.set(0u32, JsValue::from(read as i32), true, context)?;
    results.set(1u32, JsValue::from(written as i32), true, context)?;
    Ok(JsValue::undefined())
}

fn node_encoding_encode_utf8_string(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    Ok(JsValue::from(JsUint8Array::from_iter(
        input.into_bytes(),
        context,
    )?))
}

fn node_encoding_decode_utf8(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let ignore_bom = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_boolean();
    let fatal = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_boolean();
    let mut bytes = node_bytes_from_js(&input, context)?;
    if ignore_bom && bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        bytes.drain(..3);
    }
    let text = if fatal {
        String::from_utf8(bytes).map_err(|error| js_error(sandbox_execution_error(error)))?
    } else {
        String::from_utf8_lossy(&bytes).into_owned()
    };
    Ok(JsValue::from(JsString::from(text)))
}

fn node_encoding_to_ascii(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    Ok(JsValue::from(JsString::from(input)))
}

fn node_uv_get_error_map(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    context
        .eval(Source::from_bytes(b"new Map()"))
        
}

fn node_os_get_available_parallelism(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(1))
}

fn node_os_get_cpus(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(
        &serde_json::json!([{
            "model": "Terrace Deterministic CPU",
            "speed": 2400,
            "times": {
                "user": 1,
                "nice": 0,
                "sys": 1,
                "idle": 1,
                "irq": 0
            }
        }]),
        context,
    )
}

fn node_os_get_free_mem(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_os_get_home_directory(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, _context| {
        Ok(JsValue::from(JsString::from(
            host.process
                .borrow()
                .env
                .get("HOME")
                .cloned()
                .unwrap_or_else(|| "/workspace/home".to_string()),
        )))
    })
}

fn node_os_get_hostname(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(JsString::from("terrace")))
}

fn node_os_get_interface_addresses(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(
        &serde_json::json!({
            "lo": [
                {
                    "address": "127.0.0.1",
                    "netmask": "255.0.0.0",
                    "family": "IPv4",
                    "mac": "00:00:00:00:00:00",
                    "internal": true,
                    "cidr": "127.0.0.1/8"
                }
            ]
        }),
        context,
    )
}

fn node_os_get_load_avg(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(&serde_json::json!([0, 0, 0]), context)
}

fn node_os_get_priority(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_os_get_os_information(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    JsValue::from_json(
        &serde_json::json!(["Linux", "terrace", "1.0.0", "x86_64"]),
        context,
    )
}

fn node_os_get_total_mem(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_os_get_user_info(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        JsValue::from_json(
            &serde_json::json!({
                "username": "sandbox",
                "uid": 1000,
                "gid": 1000,
                "shell": "/bin/sh",
                "homedir": host.process.borrow().env.get("HOME").cloned().unwrap_or_else(|| "/workspace/home".to_string())
            }),
            context,
        )
        .map_err(sandbox_execution_error)
    })
}

fn node_os_get_uptime(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_os_set_priority(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::undefined())
}

fn node_credentials_get_temp_dir(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, _context| {
        let temp_dir = host
            .process
            .borrow()
            .env
            .get("TMPDIR")
            .cloned()
            .unwrap_or_else(|| "/tmp".to_string());
        Ok(JsValue::from(JsString::from(temp_dir)))
    })
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
    let mode = match args
        .get(2)
        .filter(|value| !value.is_null() && !value.is_undefined())
        .map(|value| value.to_string(context))
        .transpose()?
        .map(|value| value.to_std_string_escaped())
        .as_deref()
    {
        Some("import") => NodeResolveMode::Import,
        _ => NodeResolveMode::Require,
    };
    let options = match args.get(3) {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            let value = value
                .to_json(context)
                .map_err(sandbox_execution_error)
                .map_err(js_error)?
                .unwrap_or(JsonValue::Null);
            serde_json::from_value::<NodeRequireResolveOptions>(value)
                .map_err(|error| js_error(sandbox_execution_error(error)))?
        }
        _ => NodeRequireResolveOptions::default(),
    };
    node_with_host_js(context, |host, context| {
        node_debug_event(
            host,
            "resolve",
            format!(
                "resolve_module specifier={specifier} referrer={} mode={mode:?}",
                referrer.as_deref().unwrap_or("<root>")
            ),
        )?;
        let resolved = resolve_node_module(
            host,
            &specifier,
            referrer.as_deref(),
            mode,
            options.extensions.as_deref(),
        )?;
        JsValue::from_json(
            &serde_json::to_value(resolved).map_err(sandbox_execution_error)?,
            context,
        )
        .map_err(sandbox_execution_error)
    })
}

fn node_require_esm_namespace(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let resolved: NodeResolvedModule = node_arg_json(args, 0, context)?;
    if resolved.kind != NodeResolvedKind::EsModule {
        return Err(node_error_with_code(
            context,
            JsNativeError::typ(),
            format!(
                "__terraceRequireEsmNamespace expected an esm module, got {:?}",
                resolved.kind
            ),
            "ERR_INVALID_ARG_VALUE",
        ));
    }
    let host = active_node_host().map_err(js_error)?;
    node_debug_event(&host, "load", format!("require_esm {}", resolved.id)).map_err(js_error)?;
    let loader = NodeCommandModuleLoader::new(host.clone());
    let module = loader
        .materialize(&resolved, context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    let promise = module.load_link_evaluate(context);
    match promise.await_blocking(context) {
        Ok(_) | Err(_) => {}
    }
    drain_node_jobs_until_quiescent(context, &host, &resolved.id).map_err(js_error)?;
    if let PromiseState::Rejected(reason) = promise.state() {
        return Err(boa_engine::JsError::from_opaque(reason));
    }
    Ok(module.namespace(context).into())
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
        Ok(JsValue::from(JsString::from(strip_shebang(&loaded.source))))
    })
    .map_err(js_error)
}

fn node_require_resolve(
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
    let options = match args.get(2) {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            let value = value
                .to_json(context)
                .map_err(sandbox_execution_error)
                .map_err(js_error)?
                .unwrap_or(JsonValue::Null);
            serde_json::from_value::<NodeRequireResolveOptions>(value)
                .map_err(|error| js_error(sandbox_execution_error(error)))?
        }
        _ => NodeRequireResolveOptions::default(),
    };

    node_with_host_js(context, |host, context| {
        let report = resolve_require_report(
            host,
            &specifier,
            referrer.as_deref(),
            options.paths.as_deref(),
            options.extensions.as_deref(),
        );
        JsValue::from_json(
            &serde_json::to_value(report).map_err(sandbox_execution_error)?,
            context,
        )
        .map_err(sandbox_execution_error)
    })
}

fn node_require_resolve_paths(
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
        let paths = require_resolve_lookup_paths(host, &specifier, referrer.as_deref());
        JsValue::from_json(
            &serde_json::json!({
                "paths": paths,
            }),
            context,
        )
        .map_err(sandbox_execution_error)
    })
}

fn resolve_require_report(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
    paths: Option<&[String]>,
    extensions: Option<&[String]>,
) -> JsonValue {
    match resolve_require_target(host, specifier, referrer, paths, extensions) {
        Ok(resolved) => serde_json::json!({
            "ok": true,
            "resolved": resolved,
        }),
        Err((code, message)) => serde_json::json!({
            "ok": false,
            "code": code,
            "message": message,
        }),
    }
}

fn resolve_require_target(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
    paths: Option<&[String]>,
    extensions: Option<&[String]>,
) -> Result<String, (String, String)> {
    if let Some(builtin) = node_builtin_name(specifier) {
        return Ok(if specifier.starts_with("node:") {
            format!("node:{builtin}")
        } else {
            builtin
        });
    }

    resolve_node_module_path_with_paths(
        host,
        specifier,
        referrer,
        NodeResolveMode::Require,
        paths,
        extensions,
    )
        .map_err(|error| {
            let message = match &error {
                SandboxError::ModuleNotFound { .. } => {
                    format!("Cannot find module '{specifier}'")
                }
                other => other.to_string(),
            };
            let code = match error {
                SandboxError::ModuleNotFound { .. } => "MODULE_NOT_FOUND",
                _ => "ERR_TERRACE_NODE_RESOLVE",
            };
            (code.to_string(), message)
        })
}

fn require_resolve_lookup_paths(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
) -> Option<Vec<String>> {
    if node_builtin_name(specifier).is_some() || specifier.starts_with("node:") {
        return None;
    }

    if matches!(specifier, "." | "..")
        || specifier.starts_with("./")
        || specifier.starts_with("../")
    {
        let base = referrer
            .map(directory_for_path)
            .unwrap_or_else(|| host.process.borrow().cwd.clone());
        return Some(vec![base]);
    }

    let base = referrer
        .map(directory_for_path)
        .unwrap_or_else(|| host.process.borrow().cwd.clone());
    Some(node_module_lookup_paths_from(host, &base))
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
        let bytes = node_read_file(host, &path)?.ok_or_else(|| SandboxError::ModuleNotFound {
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
        node_graph_invalidate(host);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

#[derive(Clone, Copy, Debug)]
struct NodeFsOpenFlags {
    readable: bool,
    writable: bool,
    append: bool,
    create: bool,
    truncate: bool,
    exclusive: bool,
}

fn node_fs_open_flags_from_js(
    args: &[JsValue],
    index: usize,
    context: &mut Context,
) -> JsResult<NodeFsOpenFlags> {
    let value = args.get(index).cloned().unwrap_or_else(JsValue::undefined);
    if value.is_undefined() {
        return node_fs_open_flags_from_string("r");
    }
    if value.is_number() {
        let numeric = value.to_i32(context)?;
        return Ok(node_fs_open_flags_from_numeric(numeric));
    }
    node_fs_open_flags_from_string(&value.to_string(context)?.to_std_string_escaped())
}

fn node_fs_open_flags_from_string(flags: &str) -> JsResult<NodeFsOpenFlags> {
    match flags {
        "r" | "rs" | "sr" => Ok(NodeFsOpenFlags {
            readable: true,
            writable: false,
            append: false,
            create: false,
            truncate: false,
            exclusive: false,
        }),
        "r+" | "rs+" | "sr+" => Ok(NodeFsOpenFlags {
            readable: true,
            writable: true,
            append: false,
            create: false,
            truncate: false,
            exclusive: false,
        }),
        "w" => Ok(NodeFsOpenFlags {
            readable: false,
            writable: true,
            append: false,
            create: true,
            truncate: true,
            exclusive: false,
        }),
        "w+" => Ok(NodeFsOpenFlags {
            readable: true,
            writable: true,
            append: false,
            create: true,
            truncate: true,
            exclusive: false,
        }),
        "wx" | "xw" => Ok(NodeFsOpenFlags {
            readable: false,
            writable: true,
            append: false,
            create: true,
            truncate: true,
            exclusive: true,
        }),
        "wx+" | "xw+" => Ok(NodeFsOpenFlags {
            readable: true,
            writable: true,
            append: false,
            create: true,
            truncate: true,
            exclusive: true,
        }),
        "a" => Ok(NodeFsOpenFlags {
            readable: false,
            writable: true,
            append: true,
            create: true,
            truncate: false,
            exclusive: false,
        }),
        "as" | "sa" => Ok(NodeFsOpenFlags {
            readable: false,
            writable: true,
            append: true,
            create: true,
            truncate: false,
            exclusive: false,
        }),
        "a+" => Ok(NodeFsOpenFlags {
            readable: true,
            writable: true,
            append: true,
            create: true,
            truncate: false,
            exclusive: false,
        }),
        "as+" | "sa+" => Ok(NodeFsOpenFlags {
            readable: true,
            writable: true,
            append: true,
            create: true,
            truncate: false,
            exclusive: false,
        }),
        "ax" | "xa" => Ok(NodeFsOpenFlags {
            readable: false,
            writable: true,
            append: true,
            create: true,
            truncate: false,
            exclusive: true,
        }),
        "ax+" | "xa+" => Ok(NodeFsOpenFlags {
            readable: true,
            writable: true,
            append: true,
            create: true,
            truncate: false,
            exclusive: true,
        }),
        _ => Err(JsNativeError::typ()
            .with_message(format!(
                "The argument 'flags' is invalid. Received {flags:?}"
            ))
            .into()),
    }
}

fn node_fs_open_flags_from_numeric(flags: i32) -> NodeFsOpenFlags {
    const O_WRONLY: i32 = 1;
    const O_RDWR: i32 = 2;
    const O_CREAT: i32 = 64;
    const O_EXCL: i32 = 128;
    const O_TRUNC: i32 = 512;
    const O_APPEND: i32 = 1024;

    let writable = flags & (O_WRONLY | O_RDWR) != 0;
    let readable = !writable || flags & O_RDWR != 0;
    NodeFsOpenFlags {
        readable,
        writable,
        append: flags & O_APPEND != 0,
        create: flags & O_CREAT != 0,
        truncate: flags & O_TRUNC != 0,
        exclusive: flags & O_EXCL != 0,
    }
}

fn node_fs_open(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let flags = node_fs_open_flags_from_js(args, 1, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("open {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let existing = futures::executor::block_on(host.session.filesystem().stat(&path))?;
        let existed = existing.is_some();
        let mut contents = futures::executor::block_on(host.session.filesystem().read_file(&path))?
            .unwrap_or_default();
        match existing {
            Some(stats) => {
                if stats.kind == FileKind::Directory {
                    return Err(SandboxError::Service {
                        service: "node_runtime",
                        message: format!("EISDIR: illegal operation on a directory, open '{path}'"),
                    });
                }
            }
            None if !flags.create => {
                return Err(SandboxError::Service {
                    service: "node_runtime",
                    message: format!("ENOENT: no such file or directory, open '{path}'"),
                });
            }
            None => {}
        }

        if flags.exclusive && existed {
            return Err(SandboxError::Service {
                service: "node_runtime",
                message: format!("EEXIST: file already exists, open '{path}'"),
            });
        }

        if flags.truncate && flags.writable && !flags.append {
            contents.clear();
        }

        if flags.create && !existed {
            futures::executor::block_on(host.session.filesystem().write_file(
                &path,
                contents.clone(),
                CreateOptions {
                    create_parents: false,
                    overwrite: true,
                    ..Default::default()
                },
            ))?;
            node_graph_invalidate(host);
        }

        if flags.truncate && flags.writable && !flags.append {
            node_graph_invalidate(host);
        }

        let mut open_files = host.open_files.borrow_mut();
        let fd = open_files.next_fd;
        open_files.next_fd = open_files.next_fd.saturating_add(1);
        open_files.entries.insert(
            fd,
            NodeOpenFile {
                path,
                readable: flags.readable,
                writable: flags.writable,
                append: flags.append,
                cursor: if flags.append { contents.len() } else { 0 },
                contents,
            },
        );
        Ok(JsValue::from(fd))
    })
    .map_err(js_error)
}

fn node_fs_close(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("close {fd}"))?;
        if let Some(entry) = host.open_files.borrow_mut().entries.remove(&fd) {
            if entry.writable {
                futures::executor::block_on(host.session.filesystem().write_file(
                    &entry.path,
                    entry.contents,
                    CreateOptions {
                        create_parents: false,
                        overwrite: true,
                        ..Default::default()
                    },
                ))?;
                node_graph_invalidate(host);
            }
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_fs_read_fd(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    let length = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    let position = node_arg_optional_usize(args, 2, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "fs", format!("read-fd {fd} len={length}"))?;
        let bytes = {
            let mut open_files = host.open_files.borrow_mut();
            let entry = open_files
                .entries
                .get_mut(&fd)
                .ok_or_else(|| SandboxError::Service {
                    service: "node_runtime",
                    message: format!("EBADF: bad file descriptor, read"),
                })?;
            if !entry.readable {
                return Err(SandboxError::Service {
                    service: "node_runtime",
                    message: "EBADF: bad file descriptor, read".to_string(),
                });
            }
            let start = position.unwrap_or(entry.cursor).min(entry.contents.len());
            let end = start.saturating_add(length).min(entry.contents.len());
            if position.is_none() {
                entry.cursor = end;
            }
            entry.contents[start..end].to_vec()
        };
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn node_fs_write_fd(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    let data = node_bytes_from_js(
        &args.get(1).cloned().unwrap_or_else(JsValue::undefined),
        context,
    )?;
    let position = node_arg_optional_usize(args, 2, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("write-fd {fd} len={}", data.len()))?;
        let (path, contents) = {
            let mut open_files = host.open_files.borrow_mut();
            let entry = open_files
                .entries
                .get_mut(&fd)
                .ok_or_else(|| SandboxError::Service {
                    service: "node_runtime",
                    message: "EBADF: bad file descriptor, write".to_string(),
                })?;
            if !entry.writable {
                return Err(SandboxError::Service {
                    service: "node_runtime",
                    message: "EBADF: bad file descriptor, write".to_string(),
                });
            }
            let start = if entry.append {
                entry.contents.len()
            } else {
                position.unwrap_or(entry.cursor)
            };
            if start > entry.contents.len() {
                entry.contents.resize(start, 0);
            }
            let end = start.saturating_add(data.len());
            if end > entry.contents.len() {
                entry.contents.resize(end, 0);
            }
            entry.contents[start..end].copy_from_slice(&data);
            if position.is_none() || entry.append {
                entry.cursor = end;
            }
            (entry.path.clone(), entry.contents.clone())
        };
        futures::executor::block_on(host.session.filesystem().write_file(
            &path,
            contents,
            CreateOptions {
                create_parents: false,
                overwrite: true,
                ..Default::default()
            },
        ))?;
        node_graph_invalidate(host);
        Ok(JsValue::from(data.len() as u32))
    })
    .map_err(js_error)
}

fn node_fs_truncate_fd(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    let length = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("truncate-fd {fd} len={length}"))?;
        let (path, contents) = {
            let mut open_files = host.open_files.borrow_mut();
            let entry = open_files
                .entries
                .get_mut(&fd)
                .ok_or_else(|| SandboxError::Service {
                    service: "node_runtime",
                    message: "EBADF: bad file descriptor, ftruncate".to_string(),
                })?;
            if !entry.writable {
                return Err(SandboxError::Service {
                    service: "node_runtime",
                    message: "EBADF: bad file descriptor, ftruncate".to_string(),
                });
            }
            entry.contents.resize(length, 0);
            if entry.cursor > length {
                entry.cursor = length;
            }
            (entry.path.clone(), entry.contents.clone())
        };
        futures::executor::block_on(host.session.filesystem().write_file(
            &path,
            contents,
            CreateOptions {
                create_parents: false,
                overwrite: true,
                ..Default::default()
            },
        ))?;
        node_graph_invalidate(host);
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
        node_graph_invalidate(host);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_fs_readdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "fs", format!("readdir {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let entries = node_read_readdir(host, &path)?;
        let json = serde_json::Value::Array(
            entries
                .into_iter()
                .map(|entry| {
                    serde_json::json!({
                        "name": entry.name,
                        "kind": format!("{:?}", entry.kind).to_lowercase(),
                    })
                })
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
        let stats = node_read_stat(host, &path)?;
        match stats {
            Some(stats) => JsValue::from_json(&node_stats_to_json(stats), context)
                .map_err(sandbox_execution_error),
            None => Ok(JsValue::null()),
        }
    })
}

fn node_fs_lstat(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "fs", format!("lstat {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let stats = node_read_lstat(host, &path)?;
        match stats {
            Some(stats) => JsValue::from_json(&node_stats_to_json(stats), context)
                .map_err(sandbox_execution_error),
            None => Ok(JsValue::null()),
        }
    })
}

fn node_fs_readlink(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("readlink {path}"))?;
        let path = resolve_node_path(&host.process.borrow().cwd, &path);
        let target = node_read_readlink(host, &path)?;
        Ok(JsValue::from(JsString::from(target)))
    })
    .map_err(js_error)
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

fn node_fs_link(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let from = node_arg_string(args, 0, context)?;
    let to = node_arg_string(args, 1, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("link {from} -> {to}"))?;
        let cwd = host.process.borrow().cwd.clone();
        let from = resolve_node_path(&cwd, &from);
        let to = resolve_node_path(&cwd, &to);
        futures::executor::block_on(host.session.filesystem().link(&from, &to))?;
        node_graph_invalidate(host);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_fs_symlink(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let target = node_arg_string(args, 0, context)?;
    let linkpath = node_arg_string(args, 1, context)?;
    node_with_host(|host| {
        node_debug_event(host, "fs", format!("symlink {target} -> {linkpath}"))?;
        let linkpath = resolve_node_path(&host.process.borrow().cwd, &linkpath);
        futures::executor::block_on(host.session.filesystem().symlink(&target, &linkpath))?;
        node_graph_invalidate(host);
        Ok(JsValue::undefined())
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
        node_graph_invalidate(host);
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
        node_graph_invalidate(host);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

const NODE_CRYPTO_HASHES: &[&str] = &[
    "md5",
    "ripemd",
    "ripemd160",
    "rmd160",
    "sha1",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "sha512-224",
    "sha512-256",
    "sha3-224",
    "sha3-256",
    "sha3-384",
    "sha3-512",
    "shake128",
    "shake256",
];

fn normalize_crypto_hash_name(name: &str) -> Option<&'static str> {
    match name.trim().to_ascii_lowercase().as_str() {
        "md5" => Some("md5"),
        "ripemd" | "ripemd160" | "rmd160" => Some("ripemd160"),
        "sha1" | "sha-1" => Some("sha1"),
        "sha224" | "sha-224" => Some("sha224"),
        "sha256" | "sha-256" => Some("sha256"),
        "sha384" | "sha-384" => Some("sha384"),
        "sha512" | "sha-512" => Some("sha512"),
        "sha512-224" | "sha-512/224" => Some("sha512-224"),
        "sha512-256" | "sha-512/256" => Some("sha512-256"),
        "sha3-224" => Some("sha3-224"),
        "sha3-256" => Some("sha3-256"),
        "sha3-384" => Some("sha3-384"),
        "sha3-512" => Some("sha3-512"),
        "shake128" => Some("shake128"),
        "shake256" => Some("shake256"),
        _ => None,
    }
}

fn crypto_invalid_digest(name: &str) -> SandboxError {
    SandboxError::Service {
        service: "node_runtime",
        message: format!("ERR_CRYPTO_INVALID_DIGEST: Invalid digest: {name}"),
    }
}

fn crypto_default_output_length(algorithm: &str, requested: Option<usize>) -> usize {
    requested.unwrap_or_else(|| match algorithm {
        "shake128" => 16,
        "shake256" => 32,
        _ => 0,
    })
}

fn crypto_usize(value: f64, field: &str) -> Result<usize, SandboxError> {
    if !value.is_finite() || value < 0.0 {
        return Err(SandboxError::Service {
            service: "node_runtime",
            message: format!("ERR_INVALID_ARG_TYPE: {field} must be a non-negative finite integer"),
        });
    }
    Ok(value.trunc() as usize)
}

fn crypto_u32(value: f64, field: &str) -> Result<u32, SandboxError> {
    if !value.is_finite() || value < 0.0 {
        return Err(SandboxError::Service {
            service: "node_runtime",
            message: format!("ERR_INVALID_ARG_TYPE: {field} must be a non-negative finite integer"),
        });
    }
    Ok(value.trunc() as u32)
}

fn crypto_u8(value: f64, field: &str) -> Result<u8, SandboxError> {
    if !value.is_finite() || value < 0.0 {
        return Err(SandboxError::Service {
            service: "node_runtime",
            message: format!("ERR_INVALID_ARG_TYPE: {field} must be a non-negative finite integer"),
        });
    }
    Ok(value.trunc() as u8)
}

fn crypto_hash_bytes(
    algorithm: &str,
    input: &[u8],
    output_length: Option<usize>,
) -> Result<Vec<u8>, SandboxError> {
    let normalized =
        normalize_crypto_hash_name(algorithm).ok_or_else(|| crypto_invalid_digest(algorithm))?;
    Ok(match normalized {
        "md5" => Md5::digest(input).to_vec(),
        "ripemd160" => Ripemd160::digest(input).to_vec(),
        "sha1" => Sha1::digest(input).to_vec(),
        "sha224" => Sha224::digest(input).to_vec(),
        "sha256" => Sha256::digest(input).to_vec(),
        "sha384" => Sha384::digest(input).to_vec(),
        "sha512" => Sha512::digest(input).to_vec(),
        "sha512-224" => Sha512_224::digest(input).to_vec(),
        "sha512-256" => Sha512_256::digest(input).to_vec(),
        "sha3-224" => Sha3_224::digest(input).to_vec(),
        "sha3-256" => Sha3_256::digest(input).to_vec(),
        "sha3-384" => Sha3_384::digest(input).to_vec(),
        "sha3-512" => Sha3_512::digest(input).to_vec(),
        "shake128" => {
            let mut hasher = Shake128::default();
            hasher.update(input);
            let mut reader = hasher.finalize_xof();
            let mut output = vec![0; crypto_default_output_length(normalized, output_length)];
            XofReader::read(&mut reader, &mut output);
            output
        }
        "shake256" => {
            let mut hasher = Shake256::default();
            hasher.update(input);
            let mut reader = hasher.finalize_xof();
            let mut output = vec![0; crypto_default_output_length(normalized, output_length)];
            XofReader::read(&mut reader, &mut output);
            output
        }
        _ => return Err(crypto_invalid_digest(algorithm)),
    })
}

fn crypto_hmac_bytes(algorithm: &str, key: &[u8], input: &[u8]) -> Result<Vec<u8>, SandboxError> {
    let normalized =
        normalize_crypto_hash_name(algorithm).ok_or_else(|| crypto_invalid_digest(algorithm))?;
    macro_rules! compute_hmac {
        ($digest:ty) => {{
            let mut mac = Hmac::<$digest>::new_from_slice(key).map_err(sandbox_execution_error)?;
            Mac::update(&mut mac, input);
            mac.finalize().into_bytes().to_vec()
        }};
    }
    Ok(match normalized {
        "md5" => compute_hmac!(Md5),
        "ripemd160" => compute_hmac!(Ripemd160),
        "sha1" => compute_hmac!(Sha1),
        "sha224" => compute_hmac!(Sha224),
        "sha256" => compute_hmac!(Sha256),
        "sha384" => compute_hmac!(Sha384),
        "sha512" => compute_hmac!(Sha512),
        "sha512-224" => compute_hmac!(Sha512_224),
        "sha512-256" => compute_hmac!(Sha512_256),
        "sha3-224" => compute_hmac!(Sha3_224),
        "sha3-256" => compute_hmac!(Sha3_256),
        "sha3-384" => compute_hmac!(Sha3_384),
        "sha3-512" => compute_hmac!(Sha3_512),
        _ => return Err(crypto_invalid_digest(algorithm)),
    })
}

fn crypto_pbkdf2_bytes(request: &NodeCryptoPbkdf2Request) -> Result<Vec<u8>, SandboxError> {
    let normalized = normalize_crypto_hash_name(&request.digest)
        .ok_or_else(|| crypto_invalid_digest(&request.digest))?;
    let keylen = crypto_usize(request.keylen, "keylen")?;
    let iterations = crypto_u32(request.iterations, "iterations")?;
    let mut output = vec![0; keylen];
    macro_rules! derive {
        ($digest:ty) => {{
            pbkdf2_hmac::<$digest>(&request.password, &request.salt, iterations, &mut output);
        }};
    }
    match normalized {
        "md5" => derive!(Md5),
        "ripemd160" => derive!(Ripemd160),
        "sha1" => derive!(Sha1),
        "sha224" => derive!(Sha224),
        "sha256" => derive!(Sha256),
        "sha384" => derive!(Sha384),
        "sha512" => derive!(Sha512),
        "sha512-224" => derive!(Sha512_224),
        "sha512-256" => derive!(Sha512_256),
        "sha3-224" => derive!(Sha3_224),
        "sha3-256" => derive!(Sha3_256),
        "sha3-384" => derive!(Sha3_384),
        "sha3-512" => derive!(Sha3_512),
        _ => return Err(crypto_invalid_digest(&request.digest)),
    }
    Ok(output)
}

fn crypto_hkdf_bytes(request: &NodeCryptoHkdfRequest) -> Result<Vec<u8>, SandboxError> {
    let normalized = normalize_crypto_hash_name(&request.digest)
        .ok_or_else(|| crypto_invalid_digest(&request.digest))?;
    let keylen = crypto_usize(request.keylen, "keylen")?;
    let mut output = vec![0; keylen];
    macro_rules! expand {
        ($digest:ty) => {{
            let hkdf = Hkdf::<$digest>::new(Some(&request.salt), &request.ikm);
            hkdf.expand(&request.info, &mut output)
                .map_err(sandbox_execution_error)?;
        }};
    }
    match normalized {
        "md5" => expand!(Md5),
        "ripemd160" => expand!(Ripemd160),
        "sha1" => expand!(Sha1),
        "sha224" => expand!(Sha224),
        "sha256" => expand!(Sha256),
        "sha384" => expand!(Sha384),
        "sha512" => expand!(Sha512),
        "sha512-224" => expand!(Sha512_224),
        "sha512-256" => expand!(Sha512_256),
        "sha3-224" => expand!(Sha3_224),
        "sha3-256" => expand!(Sha3_256),
        "sha3-384" => expand!(Sha3_384),
        "sha3-512" => expand!(Sha3_512),
        _ => return Err(crypto_invalid_digest(&request.digest)),
    }
    Ok(output)
}

fn crypto_scrypt_bytes(request: &NodeCryptoScryptRequest) -> Result<Vec<u8>, SandboxError> {
    let keylen = crypto_usize(request.keylen, "keylen")?;
    let log_n = crypto_u8(request.cost.unwrap_or(14.0), "cost")?;
    let r = crypto_u32(request.block_size.unwrap_or(8.0), "block_size")?;
    let p = crypto_u32(request.parallelization.unwrap_or(1.0), "parallelization")?;
    let params = ScryptParams::new(log_n, r, p, keylen).map_err(sandbox_execution_error)?;
    let mut output = vec![0; keylen];
    scrypt(&request.password, &request.salt, &params, &mut output)
        .map_err(sandbox_execution_error)?;
    Ok(output)
}

fn node_crypto_random_u64(host: &NodeRuntimeHost) -> u64 {
    let bytes = host.entropy.fill_bytes(8);
    let mut array = [0u8; 8];
    array.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(array)
}

fn node_crypto_get_hashes(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "crypto", "getHashes")?;
        JsValue::from_json(&serde_json::json!(NODE_CRYPTO_HASHES), context)
            .map_err(sandbox_execution_error)
    })
}

fn node_crypto_digest(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let request = node_arg_json::<NodeCryptoDigestRequest>(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "crypto", format!("digest {}", request.algorithm))?;
        let output_length = match request.output_length {
            Some(value) => Some(crypto_usize(value, "output_length")?),
            None => None,
        };
        let bytes = crypto_hash_bytes(&request.algorithm, &request.data, output_length)?;
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn node_crypto_hmac(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let request = node_arg_json::<NodeCryptoHmacRequest>(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "crypto", format!("hmac {}", request.algorithm))?;
        let bytes = crypto_hmac_bytes(&request.algorithm, &request.key, &request.data)?;
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn node_crypto_random_bytes(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let size = args
        .get(0)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "crypto", format!("randomBytes {size}"))?;
        let bytes = host.entropy.fill_bytes(size);
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn node_crypto_random_int(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let min = node_arg_i32(args, 0, context)? as i64;
    let max = node_arg_i32(args, 1, context)? as i64;
    node_with_host_js(context, |host, _context| {
        node_debug_event(host, "crypto", format!("randomInt {min}..{max}"))?;
        let range = max.saturating_sub(min);
        if range <= 0 {
            return Err(SandboxError::Service {
                service: "node_runtime",
                message: "ERR_OUT_OF_RANGE: randomInt max must be greater than min".to_string(),
            });
        }
        let range = range as u64;
        let threshold = u64::MAX - (u64::MAX % range);
        let sampled = loop {
            let candidate = node_crypto_random_u64(host);
            if candidate < threshold {
                break candidate % range;
            }
        };
        Ok(JsValue::from(min + sampled as i64))
    })
}

fn node_crypto_timing_safe_equal(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let left = node_bytes_from_js(
        &args.get(0).cloned().unwrap_or_else(JsValue::undefined),
        context,
    )?;
    let right = node_bytes_from_js(
        &args.get(1).cloned().unwrap_or_else(JsValue::undefined),
        context,
    )?;
    node_with_host_js(context, |host, _context| {
        node_debug_event(
            host,
            "crypto",
            format!("timingSafeEqual {} {}", left.len(), right.len()),
        )?;
        if left.len() != right.len() {
            return Err(SandboxError::Service {
                service: "node_runtime",
                message:
                    "ERR_CRYPTO_TIMING_SAFE_EQUAL_LENGTH: Input buffers must have the same byte length"
                        .to_string(),
            });
        }
        let mut diff = 0u8;
        for (lhs, rhs) in left.iter().zip(right.iter()) {
            diff |= lhs ^ rhs;
        }
        Ok(JsValue::from(diff == 0))
    })
}

fn node_crypto_pbkdf2(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let request = node_arg_json::<NodeCryptoPbkdf2Request>(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(
            host,
            "crypto",
            format!("pbkdf2 {} {}", request.digest, request.keylen),
        )?;
        let bytes = crypto_pbkdf2_bytes(&request)?;
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn node_crypto_hkdf(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let request = node_arg_json::<NodeCryptoHkdfRequest>(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(
            host,
            "crypto",
            format!("hkdf {} {}", request.digest, request.keylen),
        )?;
        let bytes = crypto_hkdf_bytes(&request)?;
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn node_crypto_scrypt(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let request = node_arg_json::<NodeCryptoScryptRequest>(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "crypto", format!("scrypt {}", request.keylen))?;
        let bytes = crypto_scrypt_bytes(&request)?;
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn zlib_level_from(value: Option<f64>) -> Compression {
    match value.unwrap_or(-1.0).round() as i32 {
        level if level <= 0 => Compression::none(),
        level @ 1..=9 => Compression::new(level as u32),
        _ => Compression::default(),
    }
}

fn brotli_quality_from(value: Option<f64>) -> u32 {
    value
        .map(|entry| entry.round() as i32)
        .map(|entry| entry.clamp(0, 11) as u32)
        .unwrap_or(11)
}

fn zstd_level_from(value: Option<f64>) -> i32 {
    value.map(|entry| entry.round() as i32).unwrap_or(0)
}

fn zlib_incomplete_input(error: &std::io::Error) -> bool {
    matches!(error.kind(), std::io::ErrorKind::UnexpectedEof)
        || error
            .to_string()
            .to_ascii_lowercase()
            .contains("incomplete")
}

fn zlib_decode_all<R: Read>(mut reader: R, finalize: bool) -> Result<Vec<u8>, SandboxError> {
    let mut output = Vec::new();
    match reader.read_to_end(&mut output) {
        Ok(_) => Ok(output),
        Err(_error) if !finalize => Ok(Vec::new()),
        Err(error) => Err(SandboxError::Service {
            service: "node_runtime",
            message: format!("zlib decode failed: {error}"),
        }),
    }
}

fn zlib_encode_all<W: Write>(
    mut writer: W,
    data: &[u8],
    finish: impl FnOnce(W) -> Result<Vec<u8>, std::io::Error>,
) -> Result<Vec<u8>, SandboxError> {
    writer
        .write_all(data)
        .map_err(|error| SandboxError::Service {
            service: "node_runtime",
            message: format!("zlib encode failed: {error}"),
        })?;
    finish(writer).map_err(|error| SandboxError::Service {
        service: "node_runtime",
        message: format!("zlib encode failed: {error}"),
    })
}

fn zlib_process_bytes(
    mode: &str,
    data: &[u8],
    finalize: bool,
    options: &NodeZlibOptions,
) -> Result<Vec<u8>, SandboxError> {
    match mode {
        "gzip" => zlib_encode_all(
            GzEncoder::new(Vec::new(), zlib_level_from(options.level)),
            data,
            |writer| writer.finish(),
        ),
        "deflate" => zlib_encode_all(
            ZlibEncoder::new(Vec::new(), zlib_level_from(options.level)),
            data,
            |writer| writer.finish(),
        ),
        "deflateRaw" => zlib_encode_all(
            DeflateEncoder::new(Vec::new(), zlib_level_from(options.level)),
            data,
            |writer| writer.finish(),
        ),
        "gunzip" => zlib_decode_all(GzDecoder::new(data), finalize),
        "inflate" => zlib_decode_all(ZlibDecoder::new(data), finalize),
        "inflateRaw" => zlib_decode_all(DeflateDecoder::new(data), finalize),
        "unzip" => {
            if data.starts_with(&[0x1f, 0x8b]) {
                return zlib_decode_all(GzDecoder::new(data), finalize);
            }
            zlib_decode_all(ZlibDecoder::new(data), finalize)
                .or_else(|_| zlib_decode_all(DeflateDecoder::new(data), finalize))
        }
        "brotliCompress" => {
            let mut reader = BrotliCompressorReader::new(
                data,
                4096,
                brotli_quality_from(options.quality.or(options.level)),
                22,
            );
            let mut output = Vec::new();
            reader
                .read_to_end(&mut output)
                .map_err(|error| SandboxError::Service {
                    service: "node_runtime",
                    message: format!("zlib encode failed: {error}"),
                })?;
            Ok(output)
        }
        "brotliDecompress" => zlib_decode_all(BrotliDecompressor::new(data, 4096), finalize),
        "zstdCompress" => {
            zstd::stream::encode_all(data, zstd_level_from(options.level)).map_err(|error| {
                SandboxError::Service {
                    service: "node_runtime",
                    message: format!("zlib encode failed: {error}"),
                }
            })
        }
        "zstdDecompress" => zstd::stream::decode_all(data).map_err(|error| SandboxError::Service {
            service: "node_runtime",
            message: format!("zlib decode failed: {error}"),
        }),
        other => Err(SandboxError::Service {
            service: "node_runtime",
            message: format!("unsupported zlib mode: {other}"),
        }),
    }
}

fn zlib_stream_compress(mode: &str, options: &NodeZlibOptions) -> Option<NodeZlibCompressStream> {
    let level = zlib_level_from(options.level);
    let inner = match mode {
        "gzip" => Compress::new_gzip(level, 15),
        "deflate" => Compress::new(level, true),
        "deflateRaw" => Compress::new(level, false),
        _ => return None,
    };
    Some(NodeZlibCompressStream {
        mode: mode.to_string(),
        inner,
    })
}

fn zlib_stream_decompress(mode: &str) -> Option<NodeZlibDecompressStream> {
    let inner = match mode {
        "gunzip" => Decompress::new_gzip(15),
        "inflate" => Decompress::new(true),
        "inflateRaw" => Decompress::new(false),
        _ => return None,
    };
    Some(NodeZlibDecompressStream {
        mode: mode.to_string(),
        inner,
    })
}

fn zlib_flush_compress(flag: u32) -> FlushCompress {
    match flag {
        1 => FlushCompress::Partial,
        2 => FlushCompress::Sync,
        3 => FlushCompress::Full,
        4 => FlushCompress::Finish,
        _ => FlushCompress::None,
    }
}

fn zlib_flush_decompress(flag: u32) -> FlushDecompress {
    match flag {
        2 => FlushDecompress::Sync,
        4 => FlushDecompress::Finish,
        _ => FlushDecompress::None,
    }
}

fn zlib_compress_stream_bytes(
    stream: &mut NodeZlibCompressStream,
    data: &[u8],
    flush_flag: u32,
) -> Result<Vec<u8>, SandboxError> {
    let flush = zlib_flush_compress(flush_flag);
    let mut remaining = data;
    let mut output = Vec::new();
    loop {
        output.reserve(8192);
        let before_in = stream.inner.total_in();
        let before_out = stream.inner.total_out();
        let status = stream
            .inner
            .compress_vec(remaining, &mut output, flush)
            .map_err(|error| SandboxError::Service {
                service: "node_runtime",
                message: format!("zlib encode failed: {error}"),
            })?;
        let consumed = (stream.inner.total_in() - before_in) as usize;
        let produced = (stream.inner.total_out() - before_out) as usize;
        remaining = &remaining[consumed.min(remaining.len())..];
        match status {
            Status::StreamEnd => break,
            Status::Ok | Status::BufError => {
                if consumed == 0 && produced == 0 {
                    break;
                }
                if !remaining.is_empty() {
                    continue;
                }
                match flush {
                    FlushCompress::None => break,
                    FlushCompress::Finish => {
                        if produced == 0 {
                            break;
                        }
                    }
                    _ => {
                        if produced == 0 {
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(output)
}

fn zlib_decompress_stream_bytes(
    stream: &mut NodeZlibDecompressStream,
    data: &[u8],
    flush_flag: u32,
) -> Result<Vec<u8>, SandboxError> {
    let flush = zlib_flush_decompress(flush_flag);
    let mut remaining = data;
    let mut output = Vec::new();
    loop {
        output.reserve(8192);
        let before_in = stream.inner.total_in();
        let before_out = stream.inner.total_out();
        let status = stream
            .inner
            .decompress_vec(remaining, &mut output, flush)
            .map_err(|error| SandboxError::Service {
                service: "node_runtime",
                message: format!("zlib decode failed: {error}"),
            })?;
        let consumed = (stream.inner.total_in() - before_in) as usize;
        let produced = (stream.inner.total_out() - before_out) as usize;
        remaining = &remaining[consumed.min(remaining.len())..];
        match status {
            Status::StreamEnd => break,
            Status::Ok | Status::BufError => {
                if consumed == 0 && produced == 0 {
                    break;
                }
                if !remaining.is_empty() {
                    continue;
                }
                match flush {
                    FlushDecompress::None => break,
                    FlushDecompress::Finish => {
                        if produced == 0 {
                            break;
                        }
                    }
                    _ => {
                        if produced == 0 {
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(output)
}

fn node_zlib_stream_create(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let mode = node_arg_string(args, 0, context)?;
    let options = if args.get(1).is_none()
        || args
            .get(1)
            .is_some_and(|value| value.is_null() || value.is_undefined())
    {
        NodeZlibOptions::default()
    } else {
        node_arg_json::<NodeZlibOptions>(args, 1, context)?
    };
    node_with_host_js(context, |host, _context| {
        let mut streams = host.zlib_streams.borrow_mut();
        let stream = if let Some(stream) = zlib_stream_compress(&mode, &options) {
            NodeZlibStream::Compress(stream)
        } else if let Some(stream) = zlib_stream_decompress(&mode) {
            NodeZlibStream::Decompress(stream)
        } else {
            return Ok(JsValue::null());
        };
        let id = streams.next_id;
        streams.next_id = streams.next_id.saturating_add(1).max(1);
        streams.entries.insert(id, stream);
        Ok(JsValue::new(id))
    })
}

fn node_zlib_stream_process(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let id = args
        .get(0)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)?;
    let data = node_bytes_from_js(
        &args.get(1).cloned().unwrap_or_else(JsValue::undefined),
        context,
    )?;
    let flush_flag = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)?;
    node_with_host_js(context, |host, context| {
        let mut streams = host.zlib_streams.borrow_mut();
        let stream = streams
            .entries
            .get_mut(&id)
            .ok_or_else(|| SandboxError::Service {
                service: "node_runtime",
                message: format!("unknown zlib stream id: {id}"),
            })?;
        let bytes = match stream {
            NodeZlibStream::Compress(stream) => {
                zlib_compress_stream_bytes(stream, &data, flush_flag)?
            }
            NodeZlibStream::Decompress(stream) => {
                zlib_decompress_stream_bytes(stream, &data, flush_flag)?
            }
        };
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn node_zlib_stream_reset(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let id = args
        .get(0)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)?;
    node_with_host_js(context, |host, _context| {
        let mut streams = host.zlib_streams.borrow_mut();
        let stream = streams
            .entries
            .get_mut(&id)
            .ok_or_else(|| SandboxError::Service {
                service: "node_runtime",
                message: format!("unknown zlib stream id: {id}"),
            })?;
        match stream {
            NodeZlibStream::Compress(stream) => stream.inner.reset(),
            NodeZlibStream::Decompress(stream) => stream.inner.reset(true),
        }
        Ok(JsValue::undefined())
    })
}

fn node_zlib_stream_set_params(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let id = args
        .get(0)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)?;
    let level = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)?;
    let _strategy = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)?;
    node_with_host_js(context, |host, _context| {
        let mut streams = host.zlib_streams.borrow_mut();
        let stream = streams
            .entries
            .get_mut(&id)
            .ok_or_else(|| SandboxError::Service {
                service: "node_runtime",
                message: format!("unknown zlib stream id: {id}"),
            })?;
        if let NodeZlibStream::Compress(stream) = stream {
            stream
                .inner
                .set_level(zlib_level_from(Some(level)))
                .map_err(|error| SandboxError::Service {
                    service: "node_runtime",
                    message: format!("zlib params failed: {error}"),
                })?;
        }
        Ok(JsValue::undefined())
    })
}

fn node_zlib_stream_close(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let id = args
        .get(0)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)?;
    node_with_host_js(context, |host, _context| {
        host.zlib_streams.borrow_mut().entries.remove(&id);
        Ok(JsValue::undefined())
    })
}

fn node_zlib_process(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let mode = node_arg_string(args, 0, context)?;
    let data = node_bytes_from_js(
        &args.get(1).cloned().unwrap_or_else(JsValue::undefined),
        context,
    )?;
    let finalize = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_boolean();
    let options = if args.get(3).is_none()
        || args
            .get(3)
            .is_some_and(|value| value.is_null() || value.is_undefined())
    {
        NodeZlibOptions::default()
    } else {
        node_arg_json::<NodeZlibOptions>(args, 3, context)?
    };
    node_with_host_js(context, |host, context| {
        node_debug_event(
            host,
            "zlib",
            format!(
                "process mode={mode} size={} finalize={finalize}",
                data.len()
            ),
        )?;
        let bytes = zlib_process_bytes(&mode, &data, finalize, &options)?;
        JsUint8Array::from_iter(bytes, context)
            .map(Into::into)
            .map_err(sandbox_execution_error)
    })
}

fn node_zlib_crc32(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let data = node_bytes_from_js(
        &args.get(0).cloned().unwrap_or_else(JsValue::undefined),
        context,
    )?;
    let seed = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)
        .unwrap_or(0);
    node_with_host_js(context, |host, _context| {
        node_debug_event(host, "zlib", format!("crc32 size={}", data.len()))?;
        let mut hasher = crc32fast::Hasher::new_with_initial(seed);
        hasher.update(&data);
        Ok(JsValue::from(hasher.finalize() as i64))
    })
}

fn node_child_process_run(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let request = node_arg_json::<NodeChildProcessRequest>(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        node_debug_event(
            host,
            "child_process",
            format!(
                "run command={} shell={} cwd={}",
                request.command,
                request.shell,
                request.cwd.as_deref().unwrap_or("<inherit>")
            ),
        )?;
        let result = execute_child_process_request(host, request)?;
        JsValue::from_json(
            &serde_json::to_value(result).map_err(sandbox_execution_error)?,
            context,
        )
        .map_err(sandbox_execution_error)
    })
}

fn execute_child_process_request(
    host: &NodeRuntimeHost,
    request: NodeChildProcessRequest,
) -> Result<NodeChildProcessResult, SandboxError> {
    let cwd = request
        .cwd
        .as_deref()
        .filter(|cwd| !cwd.is_empty())
        .map(|cwd| resolve_node_path(&host.process.borrow().cwd, cwd))
        .unwrap_or_else(|| host.process.borrow().cwd.clone());
    match node_read_stat(host, &cwd)? {
        Some(stats) if stats.kind == FileKind::Directory => {}
        _ => {
            return Ok(child_process_spawn_error(
                request.command,
                request.args,
                "ENOENT",
                format!("spawn ENOENT: cwd '{cwd}' does not exist"),
                Some(cwd),
                Some("spawn".to_string()),
            ));
        }
    }

    let env = if request.env.is_empty() {
        host.process.borrow().env.clone()
    } else {
        request.env
    };

    if request.shell {
        execute_shell_child_process(host, request.command, cwd, env, request.input)
    } else {
        execute_direct_child_process(host, request.command, request.args, cwd, env, request.input)
    }
}

fn child_process_spawn_error(
    command: String,
    args: Vec<String>,
    code: &str,
    message: String,
    path: Option<String>,
    syscall: Option<String>,
) -> NodeChildProcessResult {
    NodeChildProcessResult {
        pid: None,
        file: command.clone(),
        spawnfile: command.clone(),
        spawnargs: std::iter::once(command.clone()).chain(args).collect(),
        stdout: String::new(),
        stderr: String::new(),
        status: None,
        signal: None,
        error: Some(NodeChildProcessError {
            code: JsonValue::from(code),
            message,
            errno: Some(code.to_string()),
            syscall,
            path,
            cmd: Some(command),
        }),
    }
}

fn next_child_pid(host: &NodeRuntimeHost) -> u32 {
    let mut next = host.next_child_pid.borrow_mut();
    let pid = *next;
    *next = next.saturating_add(1);
    pid
}

fn execute_direct_child_process(
    host: &NodeRuntimeHost,
    command: String,
    args: Vec<String>,
    cwd: String,
    env: BTreeMap<String, String>,
    stdin: Option<String>,
) -> Result<NodeChildProcessResult, SandboxError> {
    let resolved = match resolve_child_command(host, &command, &cwd, &env)? {
        Some(value) => value,
        None => {
            return Ok(child_process_spawn_error(
                command,
                args,
                "ENOENT",
                "spawn ENOENT".to_string(),
                None,
                Some("spawn".to_string()),
            ));
        }
    };
    let pid = next_child_pid(host);
    match resolved.as_str() {
        "pwd" => Ok(NodeChildProcessResult {
            pid: Some(pid),
            file: command.clone(),
            spawnfile: command.clone(),
            spawnargs: std::iter::once(command).chain(args).collect(),
            stdout: format!("{cwd}\n"),
            stderr: String::new(),
            status: Some(0),
            signal: None,
            error: None,
        }),
        "echo" => Ok(NodeChildProcessResult {
            pid: Some(pid),
            file: command.clone(),
            spawnfile: command.clone(),
            spawnargs: std::iter::once(command).chain(args.clone()).collect(),
            stdout: format!("{}\n", args.join(" ")),
            stderr: String::new(),
            status: Some(0),
            signal: None,
            error: None,
        }),
        "env" | "/usr/bin/env" => {
            let stdout = env
                .iter()
                .map(|(key, value)| format!("{key}={value}\n"))
                .collect::<String>();
            Ok(NodeChildProcessResult {
                pid: Some(pid),
                file: command.clone(),
                spawnfile: command.clone(),
                spawnargs: std::iter::once(command).chain(args).collect(),
                stdout,
                stderr: String::new(),
                status: Some(0),
                signal: None,
                error: None,
            })
        }
        "true" => Ok(NodeChildProcessResult {
            pid: Some(pid),
            file: command.clone(),
            spawnfile: command.clone(),
            spawnargs: std::iter::once(command).chain(args).collect(),
            stdout: String::new(),
            stderr: String::new(),
            status: Some(0),
            signal: None,
            error: None,
        }),
        "false" => Ok(NodeChildProcessResult {
            pid: Some(pid),
            file: command.clone(),
            spawnfile: command.clone(),
            spawnargs: std::iter::once(command).chain(args).collect(),
            stdout: String::new(),
            stderr: String::new(),
            status: Some(1),
            signal: None,
            error: None,
        }),
        "cat" => {
            let stdout = if args.is_empty() {
                stdin.unwrap_or_default()
            } else {
                let mut output = String::new();
                for arg in &args {
                    let path = resolve_node_path(&cwd, arg);
                    let bytes =
                        node_read_file(host, &path)?.ok_or_else(|| SandboxError::Service {
                            service: "node_runtime",
                            message: format!("ENOENT: no such file or directory, open '{path}'"),
                        })?;
                    output.push_str(&String::from_utf8_lossy(&bytes));
                }
                output
            };
            Ok(NodeChildProcessResult {
                pid: Some(pid),
                file: command.clone(),
                spawnfile: command.clone(),
                spawnargs: std::iter::once(command).chain(args).collect(),
                stdout,
                stderr: String::new(),
                status: Some(0),
                signal: None,
                error: None,
            })
        }
        resolved_path if child_command_is_node(host, &command, resolved_path) => {
            execute_node_child_process(
                host,
                pid,
                command,
                resolved_path.to_string(),
                args,
                cwd,
                env,
            )
        }
        resolved_path => Ok(child_process_spawn_error(
            resolved_path.to_string(),
            args,
            "ERR_TERRACE_NODE_UNSUPPORTED_EXECUTABLE",
            format!("unsupported executable '{resolved_path}'"),
            Some(resolved_path.to_string()),
            Some("spawn".to_string()),
        )),
    }
}

fn execute_shell_child_process(
    host: &NodeRuntimeHost,
    command: String,
    cwd: String,
    env: BTreeMap<String, String>,
    stdin: Option<String>,
) -> Result<NodeChildProcessResult, SandboxError> {
    let shell_path = "/bin/sh".to_string();
    let pipeline = split_shell_pipeline(&command);
    let mut stdin = stdin;
    let mut last = NodeChildProcessResult {
        pid: Some(next_child_pid(host)),
        file: shell_path.clone(),
        spawnfile: shell_path.clone(),
        spawnargs: vec![shell_path.clone(), "-c".to_string(), command.clone()],
        stdout: String::new(),
        stderr: String::new(),
        status: Some(0),
        signal: None,
        error: None,
    };
    for segment in pipeline {
        let argv = tokenize_shell_segment(&segment, &env);
        if argv.is_empty() {
            continue;
        }
        last = execute_direct_child_process(
            host,
            argv[0].clone(),
            argv[1..].to_vec(),
            cwd.clone(),
            env.clone(),
            stdin.take(),
        )?;
        stdin = Some(last.stdout.clone());
    }
    last.file = shell_path.clone();
    last.spawnfile = shell_path.clone();
    last.spawnargs = vec![shell_path, "-c".to_string(), command];
    if last.error.is_none() {
        last.pid = Some(next_child_pid(host));
    }
    Ok(last)
}

fn execute_node_child_process(
    host: &NodeRuntimeHost,
    pid: u32,
    command: String,
    resolved_command: String,
    args: Vec<String>,
    cwd: String,
    env: BTreeMap<String, String>,
) -> Result<NodeChildProcessResult, SandboxError> {
    let exec_path = host.process.borrow().exec_path.clone();
    let (entrypoint, child_argv) = if child_command_is_exec_path(host, &command) {
        match args.first().cloned() {
            Some(flag) if flag == "-e" || flag == "--eval" => {
                let source = args.get(1).cloned().unwrap_or_default();
                let temp_path = format!(
                    "{}/.terrace/runtime/child-eval-{}.cjs",
                    host.workspace_root, pid
                );
                futures::executor::block_on(host.session.filesystem().write_file(
                    &temp_path,
                    source.into_bytes(),
                    CreateOptions {
                        create_parents: true,
                        overwrite: true,
                        ..Default::default()
                    },
                ))?;
                let argv = vec![exec_path.clone(), temp_path.clone()];
                (temp_path, argv)
            }
            Some(flag) if flag == "-p" || flag == "-pe" => {
                let expression = args.get(1).cloned().unwrap_or_default();
                let temp_path = format!(
                    "{}/.terrace/runtime/child-print-{}.cjs",
                    host.workspace_root, pid
                );
                let source = format!("console.log({expression});\n");
                futures::executor::block_on(host.session.filesystem().write_file(
                    &temp_path,
                    source.into_bytes(),
                    CreateOptions {
                        create_parents: true,
                        overwrite: true,
                        ..Default::default()
                    },
                ))?;
                let argv = vec![exec_path.clone(), temp_path.clone()];
                (temp_path, argv)
            }
            Some(script) => {
                let entrypoint = resolve_node_path(&cwd, &script);
                let argv = std::iter::once(exec_path.clone())
                    .chain(std::iter::once(entrypoint.clone()))
                    .chain(args.into_iter().skip(1))
                    .collect();
                (entrypoint, argv)
            }
            None => {
                return Ok(NodeChildProcessResult {
                    pid: Some(pid),
                    file: command.clone(),
                    spawnfile: command.clone(),
                    spawnargs: vec![command],
                    stdout: String::new(),
                    stderr: String::new(),
                    status: Some(0),
                    signal: None,
                    error: None,
                });
            }
        }
    } else {
        let argv = std::iter::once(exec_path.clone())
            .chain(std::iter::once(resolved_command.clone()))
            .chain(args)
            .collect();
        (resolved_command, argv)
    };

    let child_host = Rc::new(NodeRuntimeHost {
        runtime_name: host.runtime_name.clone(),
        runtime_handle: host.runtime_handle.clone(),
        runtime_state: host.runtime_state.clone(),
        session_info: host.session_info.clone(),
        session: host.session.clone(),
        workspace_root: host.workspace_root.clone(),
        capture_debug_trace: host.capture_debug_trace,
        debug_options: host.debug_options.clone(),
        entropy: host.entropy.clone(),
        process: Rc::new(RefCell::new(NodeProcessState::new(
            child_argv.clone(),
            env,
            cwd,
        ))),
        open_files: Rc::new(RefCell::new(NodeOpenFileTable::default())),
        loaded_modules: Rc::new(RefCell::new(BTreeMap::new())),
        materialized_modules: Rc::new(RefCell::new(BTreeMap::new())),
        module_graph: Rc::new(RefCell::new(NodeModuleGraph::default())),
        read_snapshot_fs: Rc::new(RefCell::new(None)),
        builtin_ids: Rc::new(RefCell::new(None)),
        debug_trace: Rc::new(RefCell::new(NodeRuntimeDebugTrace::default())),
        next_child_pid: host.next_child_pid.clone(),
        zlib_streams: Rc::new(RefCell::new(NodeZlibStreamTable::default())),
    });
    let result = execute_node_command_inner(
        child_host,
        entrypoint.clone(),
        resolve_node_path(&host.process.borrow().cwd, &entrypoint),
    )?;
    let report = result.result.unwrap_or(JsonValue::Null);
    let stdout = json_object_string(&report, "stdout");
    let stderr = json_object_string(&report, "stderr");
    let status = report
        .as_object()
        .and_then(|object| object.get("exitCode"))
        .and_then(|value| value.as_i64())
        .map(|value| value as i32)
        .unwrap_or_default();
    Ok(NodeChildProcessResult {
        pid: Some(pid),
        file: command.clone(),
        spawnfile: command.clone(),
        spawnargs: child_argv,
        stdout,
        stderr,
        status: Some(status),
        signal: None,
        error: None,
    })
}

fn child_command_is_exec_path(host: &NodeRuntimeHost, command: &str) -> bool {
    let exec_path = host.process.borrow().exec_path.clone();
    command == exec_path || command == "node" || command.ends_with("/node")
}

fn child_command_is_node(host: &NodeRuntimeHost, command: &str, resolved: &str) -> bool {
    if child_command_is_exec_path(host, command) {
        return true;
    }
    resolved.ends_with(".js")
        || resolved.ends_with(".cjs")
        || resolved.ends_with(".mjs")
        || node_read_file(host, resolved)
            .ok()
            .flatten()
            .map(|bytes| String::from_utf8_lossy(&bytes).starts_with("#!"))
            .unwrap_or(false)
}

fn resolve_child_command(
    host: &NodeRuntimeHost,
    command: &str,
    cwd: &str,
    env: &BTreeMap<String, String>,
) -> Result<Option<String>, SandboxError> {
    if matches!(
        command,
        "pwd" | "echo" | "env" | "/usr/bin/env" | "cat" | "true" | "false"
    ) {
        return Ok(Some(command.to_string()));
    }
    if child_command_is_exec_path(host, command) {
        return Ok(Some(command.to_string()));
    }
    if command.contains('/') {
        let resolved = resolve_node_path(cwd, command);
        let exists = node_read_stat(host, &resolved)?;
        return Ok(exists.map(|_| resolved));
    }
    let path = env
        .get("PATH")
        .cloned()
        .unwrap_or_else(|| "/usr/bin:/bin:/workspace/project/node_modules/.bin".to_string());
    for dir in path.split(':') {
        if dir.is_empty() {
            continue;
        }
        let candidate = resolve_node_path(cwd, &format!("{dir}/{command}"));
        if node_read_stat(host, &candidate)?.is_some() {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn split_shell_pipeline(command: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    for ch in command.chars() {
        match quote {
            Some(active) if ch == active => quote = None,
            Some(_) => current.push(ch),
            None if ch == '\'' || ch == '"' => quote = Some(ch),
            None if ch == '|' => {
                result.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        result.push(current.trim().to_string());
    }
    result
}

fn tokenize_shell_segment(segment: &str, env: &BTreeMap<String, String>) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut chars = segment.chars().peekable();
    while let Some(ch) = chars.next() {
        match quote {
            Some(active) if ch == active => quote = None,
            Some('\'') => current.push(ch),
            Some('"') if ch == '$' => current.push_str(&read_shell_variable(&mut chars, env)),
            Some(_) => current.push(ch),
            None if ch == '\'' || ch == '"' => quote = Some(ch),
            None if ch.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            None if ch == '$' => current.push_str(&read_shell_variable(&mut chars, env)),
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn read_shell_variable<I>(
    chars: &mut std::iter::Peekable<I>,
    env: &BTreeMap<String, String>,
) -> String
where
    I: Iterator<Item = char>,
{
    let mut name = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            name.push(ch);
            let _ = chars.next();
        } else {
            break;
        }
    }
    env.get(&name).cloned().unwrap_or_default()
}

fn json_object_string(value: &JsonValue, key: &str) -> String {
    value
        .as_object()
        .and_then(|object| object.get(key))
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string()
}

fn node_builtin_name(specifier: &str) -> Option<String> {
    let normalized = specifier.strip_prefix("node:").unwrap_or(specifier);
    if normalized == "test" && !specifier.starts_with("node:") {
        return None;
    }
    KNOWN_NODE_BUILTIN_MODULES
        .contains(&normalized)
        .then(|| normalized.to_string())
}

fn resolve_node_module(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
    mode: NodeResolveMode,
    extensions: Option<&[String]>,
) -> Result<NodeResolvedModule, SandboxError> {
    if let Some(builtin) = node_builtin_name(specifier) {
        return Ok(NodeResolvedModule {
            id: builtin.clone(),
            specifier: format!("node:{builtin}"),
            kind: NodeResolvedKind::Builtin,
        });
    }

    let resolved_path = resolve_node_module_path(host, specifier, referrer, mode, extensions)?;
    if let Some(loaded) = host.loaded_modules.borrow().get(&resolved_path).cloned() {
        return Ok(NodeResolvedModule {
            id: loaded.runtime_path.clone(),
            specifier: loaded.runtime_path,
            kind: loaded.kind,
        });
    }
    let loaded = load_node_module_from_path(host, &resolved_path)?;
    let resolved = NodeResolvedModule {
        id: loaded.runtime_path.clone(),
        specifier: loaded.runtime_path.clone(),
        kind: loaded.kind,
    };
    host.loaded_modules
        .borrow_mut()
        .insert(resolved.id.clone(), loaded);
    Ok(resolved)
}

fn resolve_node_module_path(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
    mode: NodeResolveMode,
    extensions: Option<&[String]>,
) -> Result<String, SandboxError> {
    resolve_node_module_path_with_paths(host, specifier, referrer, mode, None, extensions)
}

fn resolve_node_module_path_with_paths(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
    mode: NodeResolveMode,
    paths: Option<&[String]>,
    extensions: Option<&[String]>,
) -> Result<String, SandboxError> {
    let cache_key = NodeResolveCacheKey {
        specifier: specifier.to_string(),
        referrer: referrer.map(str::to_string),
        mode,
        paths: paths.unwrap_or_default().to_vec(),
        extensions: normalize_require_extensions(extensions),
    };
    if let Some(cached) = host
        .module_graph
        .borrow()
        .resolve_path_cache
        .get(&cache_key)
        .cloned()
    {
        return cached.ok_or_else(|| SandboxError::ModuleNotFound {
            specifier: specifier.to_string(),
        });
    }

    let result =
        resolve_node_module_path_with_paths_uncached(
            host,
            specifier,
            referrer,
            mode,
            paths,
            extensions,
        );
    match &result {
        Ok(resolved) => {
            host.module_graph
                .borrow_mut()
                .resolve_path_cache
                .insert(cache_key, Some(resolved.clone()));
        }
        Err(SandboxError::ModuleNotFound { .. }) => {
            host.module_graph
                .borrow_mut()
                .resolve_path_cache
                .insert(cache_key, None);
        }
        Err(_) => {}
    }
    result
}

fn resolve_node_module_path_with_paths_uncached(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
    mode: NodeResolveMode,
    paths: Option<&[String]>,
    extensions: Option<&[String]>,
) -> Result<String, SandboxError> {
    if specifier.starts_with('/') {
        return resolve_as_file_or_directory(
            host,
            &normalize_node_path(specifier),
            mode,
            extensions,
        )?
            .ok_or_else(|| SandboxError::ModuleNotFound {
                specifier: specifier.to_string(),
            });
    }

    if matches!(specifier, "." | "..")
        || specifier.starts_with("./")
        || specifier.starts_with("../")
    {
        if let Some(paths) = paths {
            let cwd = host.process.borrow().cwd.clone();
            for lookup_base in paths {
                let normalized = if lookup_base.starts_with('/') {
                    normalize_node_path(lookup_base)
                } else {
                    normalize_node_path(&format!("{cwd}/{lookup_base}"))
                };
                if let Some(resolved) = resolve_as_file_or_directory(
                    host,
                    &resolve_node_path(&normalized, specifier),
                    mode,
                    extensions,
                )? {
                    return Ok(resolved);
                }
            }
            return Err(SandboxError::ModuleNotFound {
                specifier: specifier.to_string(),
            });
        }
        let base = referrer
            .map(directory_for_path)
            .unwrap_or_else(|| host.process.borrow().cwd.clone());
        return resolve_as_file_or_directory(
            host,
            &resolve_node_path(&base, specifier),
            mode,
            extensions,
        )?
            .ok_or_else(|| SandboxError::ModuleNotFound {
                specifier: specifier.to_string(),
            });
    }

    if specifier.starts_with('#') {
        return resolve_package_import_from_referrer(host, specifier, referrer, mode, extensions);
    }

    if let Some(paths) = paths {
        return resolve_bare_node_module_with_lookup_paths(host, specifier, mode, paths, extensions);
    }

    let base = referrer
        .map(directory_for_path)
        .unwrap_or_else(|| host.process.borrow().cwd.clone());
    let (package_name, subpath) = split_package_request(specifier)?;
    for directory in node_module_lookup_paths_from(host, &base) {
        let package_root = normalize_node_path(&format!("{directory}/{package_name}"));
        if let Some(resolved) =
            resolve_package_entry_from_directory(
                host,
                &package_root,
                subpath.as_deref(),
                mode,
                extensions,
            )?
        {
            return Ok(resolved);
        }
    }

    Err(SandboxError::ModuleNotFound {
        specifier: specifier.to_string(),
    })
}

fn resolve_bare_node_module_with_lookup_paths(
    host: &NodeRuntimeHost,
    specifier: &str,
    mode: NodeResolveMode,
    paths: &[String],
    extensions: Option<&[String]>,
) -> Result<String, SandboxError> {
    let (package_name, subpath) = split_package_request(specifier)?;
    let cwd = host.process.borrow().cwd.clone();
    for lookup_base in paths {
        let normalized = if lookup_base.starts_with('/') {
            normalize_node_path(lookup_base)
        } else {
            normalize_node_path(&format!("{cwd}/{lookup_base}"))
        };
        for directory in node_module_lookup_paths_from(host, &normalized) {
            let package_root = normalize_node_path(&format!("{directory}/{package_name}"));
            if let Some(resolved) =
                resolve_package_entry_from_directory(
                    host,
                    &package_root,
                    subpath.as_deref(),
                    mode,
                    extensions,
                )?
            {
                return Ok(resolved);
            }
        }
    }

    Err(SandboxError::ModuleNotFound {
        specifier: specifier.to_string(),
    })
}

fn resolve_package_import_from_referrer(
    host: &NodeRuntimeHost,
    specifier: &str,
    referrer: Option<&str>,
    mode: NodeResolveMode,
    extensions: Option<&[String]>,
) -> Result<String, SandboxError> {
    let referrer = referrer.ok_or_else(|| SandboxError::InvalidModuleSpecifier {
        specifier: specifier.to_string(),
    })?;
    let package_root =
        find_nearest_package_root(host, referrer)?.ok_or_else(|| SandboxError::ModuleNotFound {
            specifier: specifier.to_string(),
        })?;
    let package_json_path = normalize_node_path(&format!("{package_root}/package.json"));
    let package_json = read_package_json(host, &package_json_path)?.ok_or_else(|| {
        SandboxError::ModuleNotFound {
            specifier: specifier.to_string(),
        }
    })?;
    let imports = package_json
        .get("imports")
        .ok_or_else(|| SandboxError::ModuleNotFound {
            specifier: specifier.to_string(),
        })?;
    let target = select_package_map_target(imports, specifier, mode).ok_or_else(|| {
        SandboxError::ModuleNotFound {
            specifier: specifier.to_string(),
        }
    })?;
    resolve_package_target(host, &package_root, &target, mode, extensions)?.ok_or_else(|| {
        SandboxError::ModuleNotFound {
            specifier: specifier.to_string(),
        }
    })
}

fn find_nearest_package_root(
    host: &NodeRuntimeHost,
    referrer: &str,
) -> Result<Option<String>, SandboxError> {
    let base = directory_for_path(referrer);
    if let Some(cached) = host
        .module_graph
        .borrow()
        .nearest_package_root_cache
        .get(&base)
        .cloned()
    {
        return Ok(cached);
    }
    for directory in ancestor_directories_to_root(&base) {
        let package_json_path = normalize_node_path(&format!("{directory}/package.json"));
        if read_package_json(host, &package_json_path)?.is_some() {
            host.module_graph
                .borrow_mut()
                .nearest_package_root_cache
                .insert(base, Some(directory.clone()));
            return Ok(Some(directory));
        }
    }
    host.module_graph
        .borrow_mut()
        .nearest_package_root_cache
        .insert(base, None);
    Ok(None)
}

fn resolve_package_entry_from_directory(
    host: &NodeRuntimeHost,
    package_root: &str,
    subpath: Option<&str>,
    mode: NodeResolveMode,
    extensions: Option<&[String]>,
) -> Result<Option<String>, SandboxError> {
    let cache_key = NodePackageEntryCacheKey {
        package_root: package_root.to_string(),
        subpath: subpath.map(str::to_string),
        mode,
        extensions: normalize_require_extensions(extensions),
    };
    if let Some(cached) = host
        .module_graph
        .borrow()
        .package_entry_cache
        .get(&cache_key)
        .cloned()
    {
        return Ok(cached);
    }

    node_debug_event(
        host,
        "resolve",
        format!(
            "package_entry package_root={package_root} subpath={}",
            subpath.unwrap_or("<root>")
        ),
    )?;
    let Some(stats) = node_graph_stat(host, package_root)? else {
        host.module_graph
            .borrow_mut()
            .package_entry_cache
            .insert(cache_key, None);
        return Ok(None);
    };
    if stats.kind != FileKind::Directory {
        host.module_graph
            .borrow_mut()
            .package_entry_cache
            .insert(cache_key, None);
        return Ok(None);
    }

    let package_json_path = normalize_node_path(&format!("{package_root}/package.json"));
    let package_json = read_package_json(host, &package_json_path)?;

    let resolved = if let Some(subpath) = subpath {
        if let Some(package_json) = package_json.as_ref() {
            let export_key = format!("./{subpath}");
            if let Some(exports) = package_json.get("exports") {
                if let Some(target) = select_package_map_target(exports, &export_key, mode) {
                    if let Some(resolved) =
                        resolve_package_target(host, package_root, &target, mode, extensions)?
                    {
                        host.module_graph
                            .borrow_mut()
                            .package_entry_cache
                            .insert(cache_key, Some(resolved.clone()));
                        return Ok(Some(resolved));
                    }
                }
            }
        }
        resolve_as_file_or_directory(
            host,
            &normalize_node_path(&format!("{package_root}/{subpath}")),
            mode,
            extensions,
        )?
    } else {
        let mut resolved = None;

        if let Some(package_json) = package_json.as_ref() {
            if let Some(target) = package_json
                .get("exports")
                .and_then(|value| select_package_map_target(value, ".", mode))
            {
                if let Some(found) =
                    resolve_package_target(host, package_root, &target, mode, extensions)?
                {
                    resolved = Some(found);
                }
            }

            if resolved.is_none() {
                if let Some(target) = package_json.get("main").and_then(|value| value.as_str()) {
                    let candidate = normalize_node_path(&format!("{package_root}/{target}"));
                    resolved = resolve_as_file_or_directory(host, &candidate, mode, extensions)?;
                }
            }
        }

        if resolved.is_none() {
            resolved = resolve_as_file_or_directory(
                host,
                &normalize_node_path(&format!("{package_root}/index")),
                mode,
                extensions,
            )?;
        }

        resolved
    };
    host.module_graph
        .borrow_mut()
        .package_entry_cache
        .insert(cache_key, resolved.clone());
    Ok(resolved)
}

fn resolve_package_target(
    host: &NodeRuntimeHost,
    package_root: &str,
    target: &str,
    mode: NodeResolveMode,
    extensions: Option<&[String]>,
) -> Result<Option<String>, SandboxError> {
    if target.starts_with("./") || target.starts_with("../") {
        return resolve_as_file_or_directory(
            host,
            &normalize_node_path(&format!("{package_root}/{target}")),
            mode,
            extensions,
        );
    }
    if target.starts_with('/') {
        return resolve_as_file_or_directory(host, &normalize_node_path(target), mode, extensions);
    }
    let package_referrer = format!("{package_root}/package.json");
    resolve_node_module_path(host, target, Some(&package_referrer), mode, extensions).map(Some)
}

fn resolve_as_file_or_directory(
    host: &NodeRuntimeHost,
    base: &str,
    mode: NodeResolveMode,
    extensions: Option<&[String]>,
) -> Result<Option<String>, SandboxError> {
    let cache_key = NodeFileOrDirectoryCacheKey {
        base: base.to_string(),
        mode,
        extensions: normalize_require_extensions(extensions),
    };
    if let Some(cached) = host
        .module_graph
        .borrow()
        .file_or_directory_cache
        .get(&cache_key)
        .cloned()
    {
        return Ok(cached);
    }
    node_debug_event(host, "resolve", format!("file_or_directory base={base}"))?;
    let resolved_extensions = normalize_require_extensions(extensions);
    for candidate in candidate_module_paths(base, &resolved_extensions) {
        let Some(stats) = node_graph_stat(host, &candidate)? else {
            continue;
        };
        match stats.kind {
            FileKind::File => {
                host.module_graph
                    .borrow_mut()
                    .file_or_directory_cache
                    .insert(cache_key, Some(candidate.clone()));
                return Ok(Some(candidate));
            }
            FileKind::Directory => {
                if let Some(resolved) =
                    resolve_package_entry_from_directory(host, &candidate, None, mode, extensions)?
                {
                    host.module_graph
                        .borrow_mut()
                        .file_or_directory_cache
                        .insert(cache_key, Some(resolved.clone()));
                    return Ok(Some(resolved));
                }
            }
            _ => {}
        }
    }
    host.module_graph
        .borrow_mut()
        .file_or_directory_cache
        .insert(cache_key, None);
    Ok(None)
}

fn load_node_module_from_path(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<NodeLoadedModule, SandboxError> {
    node_debug_event(host, "load", format!("load_module {path}"))?;
    let bytes = node_graph_read_file(host, path)?.ok_or_else(|| SandboxError::ModuleNotFound {
        specifier: path.to_string(),
    })?;
    let source = String::from_utf8(bytes).map_err(|error| SandboxError::Execution {
        entrypoint: path.to_string(),
        message: error.to_string(),
    })?;
    let kind = node_module_kind_for_path(host, path)?;
    let media_type = match kind {
        NodeResolvedKind::Json => "application/json",
        _ => "text/javascript",
    }
    .to_string();
    Ok(NodeLoadedModule {
        runtime_path: path.to_string(),
        media_type: media_type.clone(),
        kind,
        source,
    })
}

fn read_package_json(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<Option<serde_json::Value>, SandboxError> {
    if let Some(cached) = host
        .module_graph
        .borrow()
        .package_json_cache
        .get(path)
        .cloned()
    {
        return Ok(cached);
    }
    let Some(bytes) = node_graph_read_file(host, path)? else {
        host.module_graph
            .borrow_mut()
            .package_json_cache
            .insert(path.to_string(), None);
        return Ok(None);
    };
    let text = String::from_utf8(bytes).map_err(|error| SandboxError::Service {
        service: "node_runtime",
        message: format!("package manifest {path} is not valid UTF-8: {error}"),
    })?;
    let parsed: serde_json::Value =
        serde_json::from_str(&text).map_err(|error| SandboxError::Service {
            service: "node_runtime",
            message: format!("package manifest {path} is invalid JSON: {error}"),
        })?;
    host.module_graph
        .borrow_mut()
        .package_json_cache
        .insert(path.to_string(), Some(parsed.clone()));
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

fn select_package_map_target(
    value: &serde_json::Value,
    request: &str,
    mode: NodeResolveMode,
) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|candidate| select_package_map_target(candidate, request, mode)),
        serde_json::Value::Object(object) => {
            let is_subpath_map = object
                .keys()
                .any(|key| key == "." || key.starts_with("./") || key.starts_with('#'));
            if is_subpath_map {
                if let Some(exact) = object.get(request) {
                    return select_package_map_target(exact, request, mode);
                }
                for (pattern, target) in object {
                    if !pattern.contains('*') {
                        continue;
                    }
                    if let Some(capture) = match_pattern_key(pattern, request) {
                        if let Some(resolved) = select_package_map_target(target, request, mode) {
                            return Some(resolved.replace('*', &capture));
                        }
                    }
                }
                return None;
            }

            let ordered = match mode {
                NodeResolveMode::Import => ["node", "import", "default", "require"],
                NodeResolveMode::Require => ["node", "require", "default", "import"],
            };
            for condition in ordered {
                if let Some(candidate) = object.get(condition) {
                    if let Some(resolved) = select_package_map_target(candidate, request, mode) {
                        return Some(resolved);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn match_pattern_key(pattern: &str, request: &str) -> Option<String> {
    let (prefix, suffix) = pattern.split_once('*')?;
    request
        .strip_prefix(prefix)?
        .strip_suffix(suffix)
        .map(|capture| capture.to_string())
}

fn node_module_kind_for_path(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<NodeResolvedKind, SandboxError> {
    if let Some(cached) = host
        .module_graph
        .borrow()
        .module_kind_cache
        .get(path)
        .copied()
    {
        return Ok(cached);
    }
    let kind: NodeResolvedKind = match PathBuf::from(path).extension().and_then(|ext| ext.to_str())
    {
        Some("json") => NodeResolvedKind::Json,
        Some("mjs") => NodeResolvedKind::EsModule,
        Some("cjs") => NodeResolvedKind::CommonJs,
        Some("js") => {
            let package_type = nearest_package_type(host, path)?;
            if package_type.as_deref() == Some("module") {
                NodeResolvedKind::EsModule
            } else {
                NodeResolvedKind::CommonJs
            }
        }
        _ => NodeResolvedKind::CommonJs,
    };
    host.module_graph
        .borrow_mut()
        .module_kind_cache
        .insert(path.to_string(), kind);
    Ok(kind)
}

fn nearest_package_type(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<Option<String>, SandboxError> {
    let base = directory_for_path(path);
    if let Some(cached) = host
        .module_graph
        .borrow()
        .nearest_package_type_cache
        .get(&base)
        .cloned()
    {
        return Ok(cached);
    }
    for directory in ancestor_directories_to_root(&base) {
        let package_json_path = normalize_node_path(&format!("{directory}/package.json"));
        if let Some(package_json) = read_package_json(host, &package_json_path)? {
            let resolved = package_json
                .get("type")
                .and_then(|value| value.as_str())
                .map(str::to_string);
            host.module_graph
                .borrow_mut()
                .nearest_package_type_cache
                .insert(base, resolved.clone());
            return Ok(resolved);
        }
    }
    host.module_graph
        .borrow_mut()
        .nearest_package_type_cache
        .insert(base, None);
    Ok(None)
}

fn normalize_require_extensions(extensions: Option<&[String]>) -> Vec<String> {
    let mut resolved = vec![
        ".js".to_string(),
        ".json".to_string(),
        ".node".to_string(),
    ];
    if let Some(extensions) = extensions {
        for extension in extensions {
            if !resolved.contains(extension) {
                resolved.push(extension.clone());
            }
        }
    }
    resolved
}

fn candidate_module_paths(base: &str, extensions: &[String]) -> Vec<String> {
    let base = normalize_node_path(base);
    let mut candidates = vec![base.clone()];
    for extension in extensions {
        candidates.push(format!("{base}{extension}"));
    }
    candidates
}

fn ancestor_directories_to_root(start: &str) -> Vec<String> {
    let mut current = normalize_node_path(start);
    let mut directories = Vec::new();
    loop {
        directories.push(current.clone());
        if current == "/" {
            break;
        }
        current = directory_for_path(&current);
    }
    directories
}

fn node_module_lookup_paths_from(host: &NodeRuntimeHost, base: &str) -> Vec<String> {
    let normalized = normalize_node_path(base);
    if let Some(cached) = host
        .module_graph
        .borrow()
        .node_modules_lookup_cache
        .get(&normalized)
        .cloned()
    {
        return cached;
    }
    let mut paths = Vec::new();
    for directory in ancestor_directories_to_root(&normalized) {
        if directory == "/" {
            paths.push("/node_modules".to_string());
            break;
        }
        if PathBuf::from(&directory)
            .file_name()
            .and_then(|name| name.to_str())
            == Some("node_modules")
        {
            continue;
        }
        paths.push(format!("{directory}/node_modules"));
    }
    host.module_graph
        .borrow_mut()
        .node_modules_lookup_cache
        .insert(normalized, paths.clone());
    paths
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
    let trace = host.debug_trace.borrow();
    let exception_suffix = trace
        .last_exception
        .as_ref()
        .and_then(|value| serde_json::to_string(value).ok())
        .map(|value| format!("\nnode exception: {value}"))
        .unwrap_or_default();
    SandboxError::Execution {
        entrypoint: entrypoint.to_string(),
        message: format!(
            "{}\nnode trace: {}{}",
            error.to_string(),
            trace.recent_events.join(" | "),
            exception_suffix,
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
