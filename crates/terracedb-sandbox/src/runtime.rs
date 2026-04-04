use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    net::ToSocketAddrs,
    path::PathBuf,
    rc::Rc,
    sync::Arc,
    thread_local,
};

use async_trait::async_trait;
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD as BASE64_URL_SAFE_NO_PAD},
};
use boa_ast::scope::Scope;
use boa_ast::visitor::Visitor;
use boa_engine::{
    Context, JsNativeError, JsResult, JsString, JsSymbol, JsValue, NativeFunction, Script, Source,
    builtins::promise::{OperationType, Promise, PromiseState},
    context::HostHooks,
    Finalize, JsData, Trace,
    job::JobCallback,
    job::NativeAsyncJob,
    js_string,
    module::{
        Module, ModuleLoader as BoaModuleLoader, ModuleRequest, Referrer,
        SyntheticModuleInitializer,
    },
    object::{FunctionObjectBuilder, JsObject, ObjectInitializer},
    object::builtins::{
        JsArray, JsArrayBuffer, JsFunction, JsPromise, JsProxy, JsSharedArrayBuffer,
        JsUint8Array,
    },
    property::{Attribute, PropertyDescriptor, PropertyKey},
};
use boa_interner::Interner;
use boa_parser::{Parser as BoaParser, Source as BoaParserSource};
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
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
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
use urlpattern::{UrlPattern as RustUrlPattern, UrlPatternOptions as RustUrlPatternOptions, quirks};

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
const NODE_UPSTREAM_VFS_ROOT: &str = "/node";
const NODE_UPSTREAM_BOOTSTRAP_REALM_PATH: &str = "/node/lib/internal/bootstrap/realm.js";
const NODE_RUNTIME_RESOLVE_BUDGET: u64 = 16_384;
const NODE_RUNTIME_LOAD_BUDGET: u64 = 8_192;
const NODE_RUNTIME_FS_BUDGET: u64 = 131_072;
const NODE_RUNTIME_DEBUG_EVENT_LIMIT: usize = 1024;
const NODE_RUNTIME_JOB_DRAIN_BUDGET: usize = 64;
const NODE_PERFORMANCE_MILESTONE_NODE_START: usize = 0;
const NODE_PERFORMANCE_MILESTONE_V8_START: usize = 1;
const NODE_PERFORMANCE_MILESTONE_ENVIRONMENT: usize = 2;
const NODE_PERFORMANCE_MILESTONE_LOOP_START: usize = 3;
const NODE_PERFORMANCE_MILESTONE_LOOP_EXIT: usize = 4;
const NODE_PERFORMANCE_MILESTONE_BOOTSTRAP_COMPLETE: usize = 5;
const NODE_PERFORMANCE_MILESTONE_TIME_ORIGIN: usize = 6;
const NODE_PERFORMANCE_MILESTONE_TIME_ORIGIN_TIMESTAMP: usize = 7;
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
    bootstrap: Rc<RefCell<NodeBootstrapState>>,
}

#[derive(Clone, Debug, Default)]
struct NodeBlobTable {
    next_id: u64,
    blobs: BTreeMap<u64, Vec<u8>>,
    data_objects: BTreeMap<String, NodeBlobDataObject>,
}

#[derive(Clone, Debug)]
struct NodeBlobDataObject {
    blob_id: u64,
    length: u32,
    mime_type: String,
}

#[derive(Clone, Debug, Default)]
struct NodeSeaState {
    enabled: bool,
    experimental_warning_needed: bool,
    assets: BTreeMap<String, Vec<u8>>,
}

#[derive(Clone, Debug)]
struct NodeInspectorState {
    console: Option<JsObject>,
    console_extension_installer: Option<JsObject>,
    async_hook_enable: Option<JsObject>,
    async_hook_disable: Option<JsObject>,
    network_tracking_enable: Option<JsObject>,
    network_tracking_disable: Option<JsObject>,
    listening: bool,
    waiting_for_debugger: bool,
    host: String,
    port: u16,
    async_tasks: BTreeMap<u64, (String, bool)>,
    network_resources: BTreeMap<String, String>,
    protocol_events: Vec<(String, JsonValue)>,
}

#[derive(Clone)]
struct NodeModuleWrapState {
    url: String,
    wrapper: JsObject,
    status: i32,
    synthetic: bool,
    source_text: Option<String>,
    line_offset: i32,
    column_offset: i32,
    host_defined_option_id: Option<JsValue>,
    has_top_level_await: bool,
    source_url: Option<String>,
    source_map_url: Option<String>,
    synthetic_export_names: Vec<String>,
    synthetic_evaluation_steps: Option<JsObject>,
    imported_cjs: Option<JsObject>,
    synthetic_exports: BTreeMap<String, JsValue>,
    module: Option<Module>,
    module_source_object: Option<JsObject>,
    linked_request_ids: Vec<u64>,
    linked: bool,
    instantiated: bool,
    has_async_graph: Option<bool>,
    error: Option<JsValue>,
}

#[derive(Clone)]
struct NodeContextifyScriptState {
    script: Script,
    filename: String,
    cached_data: Option<JsValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum NodeStreamHandleKind {
    TcpSocket,
    TcpServer,
    PipeSocket,
    PipeServer,
    PipeIpc,
    Tty,
}

impl NodeStreamHandleKind {
    fn is_tcp(&self) -> bool {
        matches!(self, Self::TcpSocket | Self::TcpServer)
    }

    fn is_pipe(&self) -> bool {
        matches!(self, Self::PipeSocket | Self::PipeServer | Self::PipeIpc)
    }

    fn is_tty(&self) -> bool {
        matches!(self, Self::Tty)
    }
}

#[derive(Clone, Debug)]
struct NodeStreamHandleState {
    kind: NodeStreamHandleKind,
    fd: Option<i32>,
    reading: bool,
    closed: bool,
    bytes_written: u64,
    listening: bool,
    bound_address: Option<String>,
    bound_family: Option<String>,
    bound_port: Option<u16>,
    peer_address: Option<String>,
    peer_family: Option<String>,
    peer_port: Option<u16>,
    no_delay: bool,
    keep_alive: bool,
    keep_alive_delay: u32,
    blocking: bool,
    raw_mode: bool,
    pending_instances: i32,
    pipe_mode: i32,
}

#[derive(Clone, Debug)]
struct NodeUdpHandleState {
    object: Option<JsObject>,
    fd: Option<i32>,
    closed: bool,
    refed: bool,
    recv_started: bool,
    bound_address: Option<String>,
    bound_family: Option<String>,
    bound_port: Option<u16>,
    peer_address: Option<String>,
    peer_family: Option<String>,
    peer_port: Option<u16>,
    recv_buffer_size: u32,
    send_buffer_size: u32,
    multicast_interface: Option<String>,
    multicast_ttl: i32,
    multicast_loopback: i32,
    broadcast: i32,
    ttl: i32,
    send_queue_size: usize,
    send_queue_count: usize,
}

#[derive(Clone, Debug)]
struct NodeProcessHandleState {
    object: Option<JsObject>,
    pid: Option<u32>,
    closed: bool,
    refed: bool,
    exit_status: Option<i32>,
    term_signal: Option<String>,
    result: Option<NodeChildProcessResult>,
}

#[derive(Clone, Debug)]
struct NodeMessagePortState {
    object: Option<JsObject>,
    entangled: Option<u64>,
    refed: bool,
    started: bool,
    closed: bool,
    broadcast_name: Option<String>,
    queue: Vec<JsValue>,
}

#[derive(Clone, Debug)]
struct NodeHeldLockState {
    request_id: u64,
    name: String,
    client_id: String,
    mode: String,
    resolve: JsObject,
    reject: JsObject,
}

#[derive(Clone, Debug)]
struct NodePendingLockState {
    request_id: u64,
    name: String,
    client_id: String,
    mode: String,
    callback: JsObject,
    resolve: JsObject,
    reject: JsObject,
}

#[derive(Clone, Debug, Default)]
struct NodeCompileCacheEntryState {
    key: String,
    transpiled: Option<String>,
}

#[derive(Clone, Debug)]
struct NodeUrlPatternState {
    compiled: Rc<RustUrlPattern>,
    protocol: String,
    username: String,
    password: String,
    hostname: String,
    port: String,
    pathname: String,
    search: String,
    hash: String,
    has_regexp_groups: bool,
}

impl Default for NodeInspectorState {
    fn default() -> Self {
        Self {
            console: None,
            console_extension_installer: None,
            async_hook_enable: None,
            async_hook_disable: None,
            network_tracking_enable: None,
            network_tracking_disable: None,
            listening: false,
            waiting_for_debugger: false,
            host: "127.0.0.1".to_string(),
            port: 9229,
            async_tasks: BTreeMap::new(),
            network_resources: BTreeMap::new(),
            protocol_events: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct NodeReportState {
    directory: String,
    filename: String,
    compact: bool,
    exclude_network: bool,
    signal: String,
    report_on_fatal_error: bool,
    report_on_signal: bool,
    report_on_uncaught_exception: bool,
    exclude_env: bool,
}

impl Default for NodeReportState {
    fn default() -> Self {
        Self {
            directory: "/tmp".to_string(),
            filename: "report.json".to_string(),
            compact: false,
            exclude_network: false,
            signal: "SIGUSR2".to_string(),
            report_on_fatal_error: false,
            report_on_signal: false,
            report_on_uncaught_exception: false,
            exclude_env: false,
        }
    }
}

struct NodeCommandModuleLoader {
    host: Rc<NodeRuntimeHost>,
}

#[derive(Clone, Debug, Trace, Finalize, JsData)]
struct NodeJobCallbackState {
    async_id: u64,
    resource: JsValue,
}

#[derive(Debug, Default)]
struct NodeRuntimeHostHooks;

impl HostHooks for NodeRuntimeHostHooks {
    fn make_job_callback(
        &self,
        callback: boa_engine::object::builtins::JsFunction,
        _context: &mut Context,
    ) -> JobCallback {
        let state = active_node_host()
            .ok()
            .map(|host| {
                let bootstrap = host.bootstrap.borrow();
                NodeJobCallbackState {
                    async_id: bootstrap.execution_async_id,
                    resource: bootstrap
                        .execution_async_resources
                        .last()
                        .cloned()
                        .unwrap_or_else(JsValue::undefined),
                }
            })
            .unwrap_or(NodeJobCallbackState {
                async_id: 0,
                resource: JsValue::undefined(),
            });
        JobCallback::new(callback, state)
    }

    fn call_job_callback(
        &self,
        job: &JobCallback,
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<JsValue> {
        let Ok(host) = active_node_host() else {
            return job.callback().call(this, args, context);
        };
        let trampoline = host.bootstrap.borrow().async_callback_trampoline.clone();
        let Some(trampoline) = trampoline else {
            return job.callback().call(this, args, context);
        };
        let Some(trampoline) = JsValue::from(trampoline).as_callable() else {
            return job.callback().call(this, args, context);
        };
        let state = job
            .host_defined()
            .downcast_ref::<NodeJobCallbackState>()
            .cloned()
            .unwrap_or(NodeJobCallbackState {
                async_id: 0,
                resource: JsValue::undefined(),
            });
        let mut trampoline_args = Vec::with_capacity(args.len() + 3);
        trampoline_args.push(JsValue::from(state.async_id as f64));
        trampoline_args.push(state.resource.clone());
        trampoline_args.push(JsValue::from(job.callback().clone()));
        trampoline_args.extend_from_slice(args);
        trampoline.call(this, &trampoline_args, context)
    }

    fn promise_rejection_tracker(
        &self,
        promise: &JsObject<Promise>,
        operation: OperationType,
        context: &mut Context,
    ) {
        let Ok(host) = active_node_host() else {
            return;
        };
        let callback = host.bootstrap.borrow().promise_reject_callback.clone();
        let Some(callback) = callback else {
            return;
        };
        let Some(callable) = JsValue::from(callback).as_callable() else {
            return;
        };
        let event_type = match operation {
            OperationType::Reject => 0.0,
            OperationType::Handle => 1.0,
        };
        let reason = match operation {
            OperationType::Reject => match JsPromise::from(promise.clone()).state() {
                PromiseState::Rejected(reason) => reason,
                _ => JsValue::undefined(),
            },
            OperationType::Handle => JsValue::undefined(),
        };
        let _ = callable.call(
            &JsValue::undefined(),
            &[
                JsValue::from(event_type),
                JsValue::from(JsObject::from(promise.clone())),
                reason,
            ],
            context,
        );
    }
}

#[derive(Clone, Default)]
struct NodeBootstrapState {
    per_context_exports: Option<JsObject>,
    primordials: Option<JsObject>,
    tick_info: Option<JsObject>,
    immediate_info: Option<JsObject>,
    timeout_info: Option<JsObject>,
    hrtime_buffer: Option<JsObject>,
    stream_base_state: Option<JsObject>,
    microtask_queue: Vec<JsObject>,
    async_context_frame: Option<JsValue>,
    promise_reject_callback: Option<JsObject>,
    tick_callback: Option<JsObject>,
    process_immediate_callback: Option<JsObject>,
    process_timers_callback: Option<JsObject>,
    emit_warning_sync: Option<JsObject>,
    next_timer_due_ms: Option<f64>,
    timer_is_refed: bool,
    immediate_is_refed: bool,
    trace_categories: BTreeSet<String>,
    trace_category_buffers: BTreeMap<String, JsObject>,
    async_callback_trampoline: Option<JsObject>,
    async_hooks_object: Option<JsObject>,
    promise_hook_init: Option<JsObject>,
    promise_hook_before: Option<JsObject>,
    promise_hook_after: Option<JsObject>,
    promise_hook_settled: Option<JsObject>,
    trace_category_state_update_handler: Option<JsObject>,
    buffer_prototype: Option<JsObject>,
    prepare_stack_trace_callback: Option<JsObject>,
    source_maps_enabled: bool,
    maybe_cache_generated_source_map_callback: Option<JsObject>,
    enhance_stack_before_inspector: Option<JsObject>,
    enhance_stack_after_inspector: Option<JsObject>,
    profiler_source_map_cache_getter: Option<JsObject>,
    profiler_coverage_directory: Option<String>,
    performance_observer_callback: Option<JsObject>,
    performance_milestones: Option<JsObject>,
    performance_observer_counts: Option<JsObject>,
    messaging_deserialize_create_object_callback: Option<JsObject>,
    should_abort_on_uncaught_toggle: Option<JsObject>,
    module_wrap_import_module_dynamically_callback: Option<JsObject>,
    module_wrap_initialize_import_meta_object_callback: Option<JsObject>,
    snapshot_serialize_callback: Option<JsObject>,
    snapshot_deserialize_callback: Option<JsObject>,
    snapshot_deserialize_main_function: Option<JsObject>,
    compile_cache_enabled: bool,
    compile_cache_dir: Option<String>,
    gc_tracking_installed: bool,
    performance_loop_idle_time_ms: f64,
    performance_uv_loop_count: u64,
    performance_uv_events: u64,
    performance_uv_events_waiting: u64,
    next_compile_cache_entry_id: u64,
    compile_cache_entries: BTreeMap<u64, NodeCompileCacheEntryState>,
    next_host_handle_id: u64,
    host_private_symbols: BTreeMap<String, JsSymbol>,
    execution_async_id: u64,
    trigger_async_id: u64,
    execution_async_resources: Vec<JsValue>,
    wasm_web_api_callback: Option<JsObject>,
    inspector: NodeInspectorState,
    report: NodeReportState,
    sea: NodeSeaState,
    permission_enabled: bool,
    bootstrap_completed: bool,
    blobs: NodeBlobTable,
    contextify_scripts: BTreeMap<u64, NodeContextifyScriptState>,
    module_wraps: BTreeMap<u64, NodeModuleWrapState>,
    url_patterns: BTreeMap<u64, NodeUrlPatternState>,
    stream_handles: BTreeMap<u64, NodeStreamHandleState>,
    udp_handles: BTreeMap<u64, NodeUdpHandleState>,
    process_handles: BTreeMap<u64, NodeProcessHandleState>,
    message_ports: BTreeMap<u64, NodeMessagePortState>,
    env_message_port: Option<u64>,
    broadcast_channels: BTreeMap<String, BTreeSet<u64>>,
    next_lock_request_id: u64,
    held_locks: BTreeMap<String, Vec<NodeHeldLockState>>,
    pending_locks: Vec<NodePendingLockState>,
    registered_destroy_async_ids: BTreeSet<u64>,
    sigint_watchdog_active: bool,
    monotonic_now_ms: f64,
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
    pid: u32,
    ppid: u32,
    umask: u32,
}

fn node_process_versions_json(process: &NodeProcessState) -> serde_json::Value {
    serde_json::json!({
        "node": process.version.trim_start_matches('v'),
        "uv": "1.48.0",
        "modules": "137",
        "napi": "9",
        "v8": "12.4.254.21-node.27",
        "zlib": "1.3.0.1-motley",
        "openssl": "",
        "sqlite": "",
    })
}

fn node_process_features_json() -> serde_json::Value {
    serde_json::json!({
        "inspector": false,
        "debug": false,
        "uv": true,
        "ipv6": true,
        "tls_alpn": false,
        "tls_sni": false,
        "tls_ocsp": false,
        "tls": false,
        "cached_builtins": false,
        "require_module": true,
    })
}

fn node_process_config_json() -> serde_json::Value {
    serde_json::json!({
        "target_defaults": {
            "default_configuration": "Release",
        },
        "variables": {
            "v8_enable_i18n_support": 0,
            "node_quic": 0,
            "asan": 0,
            "icu_small": false,
            "single_executable_application": 0,
            "node_shared": 0,
            "node_shared_openssl": 0,
            "node_use_openssl": 0,
            "is_debug": 0,
            "ubsan": 0,
            "want_separate_host_toolset": 0,
        },
    })
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
            pid: 1,
            ppid: 0,
            umask: 0o022,
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
            "pid": self.pid,
            "ppid": self.ppid,
            "umask": self.umask,
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
        bootstrap: Rc::new(RefCell::new(NodeBootstrapState::default())),
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
            .host_hooks(Rc::new(NodeRuntimeHostHooks))
            .build()
            .map_err(|error| SandboxError::Execution {
                entrypoint: normalized_entrypoint.clone(),
                message: error.to_string(),
            })?;
        node_debug_event(node_host, "stage", "context-built".to_string())?;
        install_node_host_bindings(&mut context)?;
        node_debug_event(node_host, "stage", "host-bindings-installed".to_string())?;
        node_install_base_process_global(&mut context, node_host)?;
        node_debug_event(node_host, "stage", "base-process-installed".to_string())?;
        node_execute_upstream_bootstrap_and_main(
            &mut context,
            node_host,
            &entrypoint,
            &normalized_entrypoint,
        )?;
        drain_node_jobs_until_quiescent(&mut context, node_host, &entrypoint)?;
        let result = node_command_report_json(node_host);

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

fn node_install_base_process_global(
    context: &mut Context,
    host: &NodeRuntimeHost,
) -> Result<(), SandboxError> {
    let process = host.process.borrow();
    let process_object = context
        .eval(Source::from_bytes(b"new (function Process(){})()"))
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "failed to create process object".to_string(),
        })?;

    for (name, value) in [
        ("version", JsValue::from(JsString::from(process.version.clone()))),
        ("platform", JsValue::from(JsString::from(process.platform.clone()))),
        ("arch", JsValue::from(JsString::from(process.arch.clone()))),
        ("title", JsValue::from(JsString::from(process.title.clone()))),
        ("execPath", JsValue::from(JsString::from(process.exec_path.clone()))),
        ("pid", JsValue::from(process.pid)),
        ("ppid", JsValue::from(process.ppid)),
    ] {
        process_object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }

    let argv = JsValue::from_json(&serde_json::json!(process.argv), context)
        .map_err(sandbox_execution_error)?;
    process_object
        .set(js_string!("argv"), argv, true, context)
        .map_err(sandbox_execution_error)?;
    let exec_argv = JsValue::from_json(&serde_json::json!([]), context)
        .map_err(sandbox_execution_error)?;
    process_object
        .set(js_string!("execArgv"), exec_argv, true, context)
        .map_err(sandbox_execution_error)?;
    let env = JsValue::from_json(&serde_json::json!(process.env), context)
        .map_err(sandbox_execution_error)?;
    process_object
        .set(js_string!("env"), env, true, context)
        .map_err(sandbox_execution_error)?;
    let versions = JsValue::from_json(&node_process_versions_json(&process), context)
        .map_err(sandbox_execution_error)?;
    process_object
        .set(js_string!("versions"), versions, true, context)
        .map_err(sandbox_execution_error)?;
    let features = JsValue::from_json(&node_process_features_json(), context)
        .map_err(sandbox_execution_error)?;
    process_object
        .set(js_string!("features"), features, true, context)
        .map_err(sandbox_execution_error)?;
    let config = JsValue::from_json(&node_process_config_json(), context)
        .map_err(sandbox_execution_error)?;
    process_object
        .set(js_string!("config"), config, true, context)
        .map_err(sandbox_execution_error)?;
    let release = JsValue::from_json(
        &serde_json::json!({
            "name": "node",
            "lts": "Krypton",
        }),
        context,
    )
    .map_err(sandbox_execution_error)?;
    process_object
        .set(js_string!("release"), release, true, context)
        .map_err(sandbox_execution_error)?;
    process_object
        .set(
            js_string!("argv0"),
            JsValue::from(JsString::from(process.exec_path.clone())),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;

    drop(process);
    node_store_global(context, "process", JsValue::from(process_object))
}

fn node_execute_upstream_bootstrap_and_main(
    context: &mut Context,
    host: &NodeRuntimeHost,
    entrypoint: &str,
    normalized_entrypoint: &str,
) -> Result<(), SandboxError> {
    node_debug_event(host, "stage", "bootstrap-realm-start".to_string())?;
    let realm = node_initialize_bootstrap_realm(context, host)?;
    let realm_object = realm.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: normalized_entrypoint.to_string(),
        message: "bootstrap realm did not return an object".to_string(),
    })?;
    let process = context
        .global_object()
        .get(js_string!("process"), context)
        .map_err(sandbox_execution_error)?;
    let internal_binding = realm_object
        .get(js_string!("internalBinding"), context)
        .map_err(sandbox_execution_error)?;
    let require_builtin = realm_object
        .get(js_string!("requireBuiltin"), context)
        .map_err(sandbox_execution_error)?;
    let primordials = host
        .bootstrap
        .borrow()
        .primordials
        .clone()
        .map(JsValue::from)
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: normalized_entrypoint.to_string(),
            message: "primordials not initialized".to_string(),
        })?;

    node_prepare_process_for_bootstrap(context, host, &process, &internal_binding)?;
    node_debug_event(host, "stage", "bootstrap-realm-done".to_string())?;

    for specifier in [
        "internal/bootstrap/node",
        "internal/bootstrap/web/exposed-wildcard",
        "internal/bootstrap/web/exposed-window-or-worker",
        "internal/bootstrap/switches/is_main_thread",
        "internal/bootstrap/switches/does_own_process_state",
        "internal/main/run_main_module",
    ] {
        node_debug_event(host, "stage", format!("builtin-start {specifier}"))?;
        let result = node_call_builtin_with_node_runtime(
            context,
            host,
            specifier,
            &[
                process.clone(),
                require_builtin.clone(),
                internal_binding.clone(),
                primordials.clone(),
            ],
            entrypoint,
        )?;
        node_await_if_promise(context, host, entrypoint, result)?;
        node_debug_event(host, "stage", format!("builtin-done {specifier}"))?;
    }

    Ok(())
}

fn node_prepare_process_for_bootstrap(
    context: &mut Context,
    host: &NodeRuntimeHost,
    process: &JsValue,
    internal_binding: &JsValue,
) -> Result<(), SandboxError> {
    let internal_binding = internal_binding
        .as_callable()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "internalBinding is not callable".to_string(),
        })?;
    let util_binding = internal_binding
        .call(
            &JsValue::undefined(),
            &[JsValue::from(JsString::from("util"))],
            context,
        )
        .map_err(sandbox_execution_error)?;
    let util_binding = util_binding.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "internalBinding('util') did not return an object".to_string(),
    })?;
    let private_symbols = util_binding
        .get(js_string!("privateSymbols"), context)
        .map_err(sandbox_execution_error)?;
    let private_symbols = private_symbols
        .as_object()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "internalBinding('util').privateSymbols is not an object".to_string(),
        })?;
    let exit_info_symbol = private_symbols
        .get(js_string!("exit_info_private_symbol"), context)
        .map_err(sandbox_execution_error)?;
    let exit_info_symbol = exit_info_symbol
        .as_symbol()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "exit_info_private_symbol is not a symbol".to_string(),
        })?;
    let fields = JsValue::from_json(&serde_json::json!([0, 0, 0]), context)
        .map_err(sandbox_execution_error)?;
    let process_object = process.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "global process is not an object".to_string(),
    })?;
    process_object
        .set(exit_info_symbol, fields, true, context)
        .map_err(sandbox_execution_error)?;
    node_debug_event(host, "bootstrap", "process-prepared".to_string())?;
    Ok(())
}

fn node_call_builtin_with_node_runtime(
    context: &mut Context,
    host: &NodeRuntimeHost,
    specifier: &str,
    args: &[JsValue],
    entrypoint: &str,
) -> Result<JsValue, SandboxError> {
    let function = node_compile_builtin_wrapper(
        context,
        host,
        specifier,
        &["process", "require", "internalBinding", "primordials"],
        None,
    )?;
    let callable = function.as_callable().ok_or_else(|| SandboxError::Execution {
        entrypoint: entrypoint.to_string(),
        message: format!("builtin `{specifier}` did not compile to a callable wrapper"),
    })?;
    callable
        .call(&JsValue::undefined(), args, context)
        .map_err(sandbox_execution_error)
}

fn node_await_if_promise(
    context: &mut Context,
    host: &NodeRuntimeHost,
    entrypoint: &str,
    value: JsValue,
) -> Result<JsValue, SandboxError> {
    let Some(object) = value.as_object() else {
        return Ok(value);
    };
    let Ok(promise) = JsPromise::from_object(object.clone()) else {
        return Ok(value);
    };
    match promise.await_blocking(context) {
        Ok(_) | Err(_) => {}
    }
    if let PromiseState::Rejected(reason) = promise.state() {
        return Err(node_execution_error(
            entrypoint,
            host,
            boa_engine::JsError::from_opaque(reason),
        ));
    }
    Ok(JsValue::from(object))
}

fn node_command_report_json(host: &NodeRuntimeHost) -> Option<JsonValue> {
    let process = host.process.borrow();
    Some(serde_json::json!({
        "cwd": process.cwd,
        "stdout": process.stdout,
        "stderr": process.stderr,
        "exitCode": process.exit_code,
        "argv": process.argv,
        "builtinAccesses": [],
        "nodeCommandDebug": {
            "topLevelResultKind": "undefined",
            "topLevelThenable": false,
            "topLevelAwaited": false,
            "caughtExit": false,
            "caughtExitCode": null,
        },
    }))
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
        if let Some(callback) = self
            .host
            .bootstrap
            .borrow()
            .module_wrap_initialize_import_meta_object_callback
            .clone()
            && let Some(state) = self
                .host
                .bootstrap
                .borrow()
                .module_wraps
                .values()
                .find(|state| state.url == path)
                .cloned()
        {
            if let Some(callable) = JsValue::from(callback).as_callable() {
                let _ = callable.call(
                    &JsValue::undefined(),
                    &[
                        state
                            .host_defined_option_id
                            .clone()
                            .unwrap_or_else(JsValue::undefined),
                        JsValue::from(import_meta.clone()),
                        JsValue::from(state.wrapper),
                    ],
                    context,
                );
            }
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

fn node_run_microtask_checkpoint(context: &mut Context, host: &NodeRuntimeHost) -> Result<usize, SandboxError> {
    context
        .run_jobs()
        .map_err(sandbox_execution_error)?;
    let queue = std::mem::take(&mut host.bootstrap.borrow_mut().microtask_queue);
    let count = queue.len();
    for callback in queue {
        let callable = JsValue::from(callback.clone()).as_callable().ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "queued microtask is not callable".to_string(),
        })?;
        callable
            .call(&JsValue::undefined(), &[], context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(count)
}

fn node_bootstrap_array_entry(
    context: &mut Context,
    host: &NodeRuntimeHost,
    name: &str,
    index: u32,
) -> Result<i32, SandboxError> {
    let object = match name {
        "tickInfo" => host.bootstrap.borrow().tick_info.clone(),
        "immediateInfo" => host.bootstrap.borrow().immediate_info.clone(),
        "timeoutInfo" => host.bootstrap.borrow().timeout_info.clone(),
        other => {
            return Err(SandboxError::Service {
                service: "node_runtime",
                message: format!("unknown bootstrap array `{other}`"),
            });
        }
    };
    let Some(object) = object else {
        return Ok(0);
    };
    object
        .get(index, context)
        .map_err(sandbox_execution_error)?
        .to_i32(context)
        .map_err(sandbox_execution_error)
}

fn drain_node_next_ticks(
    context: &mut Context,
    host: &NodeRuntimeHost,
    entrypoint: &str,
) -> Result<usize, SandboxError> {
    let tick_callback = host.bootstrap.borrow().tick_callback.clone();
    let Some(tick_callback) = tick_callback else {
        return Ok(0);
    };
    let scheduled_before = node_bootstrap_array_entry(context, host, "tickInfo", 0)?;
    let rejection_before = node_bootstrap_array_entry(context, host, "tickInfo", 1)?;
    let mut drained = 0usize;
    if scheduled_before == 0 {
        drained = drained.saturating_add(node_run_microtask_checkpoint(context, host)?);
    }
    let scheduled_after = node_bootstrap_array_entry(context, host, "tickInfo", 0)?;
    let rejection_after = node_bootstrap_array_entry(context, host, "tickInfo", 1)?;
    if scheduled_after == 0 && rejection_after == 0 {
        return Ok(drained);
    }
    tick_callback
        .call(&JsValue::undefined(), &[], context)
        .map_err(|error| node_execution_error(entrypoint, host, error))?;
    let drained_microtasks =
        node_run_microtask_checkpoint(context, host).map_err(|error| match error {
            SandboxError::Execution { .. } => error,
            other => SandboxError::Execution {
                entrypoint: entrypoint.to_string(),
                message: other.to_string(),
            },
        })?;
    Ok(drained + usize::from(scheduled_after > 0 || rejection_before > 0 || rejection_after > 0) + drained_microtasks)
}

fn drain_node_timers(
    context: &mut Context,
    host: &NodeRuntimeHost,
    entrypoint: &str,
) -> Result<usize, SandboxError> {
    let mut drained = 0usize;
    let bootstrap = host.bootstrap.borrow().clone();
    let now_ms = node_monotonic_now_ms(host);
    let immediate_count = node_bootstrap_array_entry(context, host, "immediateInfo", 0)?;
    let timeout_count = node_bootstrap_array_entry(context, host, "timeoutInfo", 0)?;
    let timer_is_due = bootstrap
        .next_timer_due_ms
        .is_some_and(|due| due <= now_ms);
    let should_run_immediates = immediate_count > 0
        && (bootstrap.immediate_is_refed
            || timeout_count > 0
            || timer_is_due);
    if should_run_immediates {
        if let Some(callback) = bootstrap.process_immediate_callback {
            let callable = JsValue::from(callback.clone()).as_callable().ok_or_else(|| SandboxError::Execution {
                entrypoint: entrypoint.to_string(),
                message: "processImmediate callback is not callable".to_string(),
            })?;
            callable
                .call(&JsValue::undefined(), &[], context)
                .map_err(|error| node_execution_error(entrypoint, host, error))?;
            node_performance_note_event(host, immediate_count as u64);
            drained = drained.saturating_add(1);
        }
    }
    if timeout_count > 0 && timer_is_due {
        if let Some(callback) = bootstrap.process_timers_callback {
            let callable = JsValue::from(callback.clone()).as_callable().ok_or_else(|| SandboxError::Execution {
                entrypoint: entrypoint.to_string(),
                message: "processTimers callback is not callable".to_string(),
            })?;
            let now = JsValue::from(now_ms);
            let next_expiry = callable
                .call(&JsValue::undefined(), &[now], context)
                .map_err(|error| node_execution_error(entrypoint, host, error))?;
            let next_expiry = next_expiry
                .to_number(context)
                .map_err(sandbox_execution_error)?;
            let mut state = host.bootstrap.borrow_mut();
            if next_expiry == 0.0 {
                state.next_timer_due_ms = None;
            } else {
                state.timer_is_refed = next_expiry > 0.0;
                state.next_timer_due_ms = Some(next_expiry.abs());
            }
            node_performance_note_event(host, timeout_count as u64);
            drained = drained.saturating_add(1);
        }
    }
    Ok(drained)
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
        let drained_timers = drain_node_timers(context, host, entrypoint)?;
        let drained_after = drain_node_next_ticks(context, host, entrypoint)?;
        if drained_before == 0 && drained_after == 0 && drained_timers == 0 {
            let now_ms = node_monotonic_now_ms(host);
            let bootstrap = host.bootstrap.borrow();
            let next_due = bootstrap
                .timer_is_refed
                .then_some(bootstrap.next_timer_due_ms)
                .flatten();
            if let Some(next_due) = next_due.filter(|next_due| *next_due > now_ms) {
                node_advance_monotonic_now_ms(host, next_due - now_ms);
                previous = node_runtime_progress_snapshot(host);
                stable_rounds = 0;
                continue;
            }
        }
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
    }).map_err(js_error)
}

fn node_chdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        node_debug_event(host, "process", format!("chdir path={path}"))?;
        host.process.borrow_mut().cwd = resolve_node_path(&host.process.borrow().cwd, &path);
        Ok(JsValue::undefined())
    }).map_err(js_error)
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
    }).map_err(js_error)
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
    }).map_err(js_error)
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

fn node_initialize_bootstrap_realm(
    context: &mut Context,
    host: &NodeRuntimeHost,
) -> Result<JsValue, SandboxError> {
    node_debug_event(host, "bootstrap", "init_bootstrap_realm:start".to_string())?;
    let exports = ObjectInitializer::new(context).build();
    let primordials = JsObject::with_null_proto();
    let private_symbols = node_private_symbols_object(context)?;
    let per_isolate_symbols = node_per_isolate_symbols_object(context)?;
    {
        let mut bootstrap = host.bootstrap.borrow_mut();
        bootstrap.per_context_exports = Some(exports.clone());
        bootstrap.primordials = Some(primordials.clone());
    }

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

fn node_monotonic_now_ms(host: &NodeRuntimeHost) -> f64 {
    let state = host.bootstrap.borrow();
    state.monotonic_now_ms.max(0.0)
}

fn node_advance_monotonic_now_ms(host: &NodeRuntimeHost, delta_ms: f64) -> f64 {
    let mut state = host.bootstrap.borrow_mut();
    let delta_ms = delta_ms.max(0.0);
    state.monotonic_now_ms = (state.monotonic_now_ms + delta_ms).max(0.0);
    state.performance_loop_idle_time_ms =
        (state.performance_loop_idle_time_ms + delta_ms).max(0.0);
    if delta_ms > 0.0 {
        state.performance_uv_loop_count = state.performance_uv_loop_count.saturating_add(1);
    }
    state.monotonic_now_ms
}

fn node_performance_set_milestone(
    host: &NodeRuntimeHost,
    context: &mut Context,
    index: usize,
    value: f64,
) -> Result<(), SandboxError> {
    let milestones = host.bootstrap.borrow().performance_milestones.clone();
    let Some(milestones) = milestones else {
        return Ok(());
    };
    milestones
        .set(index, JsValue::from(value), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_performance_init_milestones(
    host: &NodeRuntimeHost,
    context: &mut Context,
    milestones: &JsObject,
) -> Result<(), SandboxError> {
    let initial = [
        (NODE_PERFORMANCE_MILESTONE_NODE_START, 0.0),
        (NODE_PERFORMANCE_MILESTONE_V8_START, 0.0),
        (NODE_PERFORMANCE_MILESTONE_ENVIRONMENT, 0.0),
        (NODE_PERFORMANCE_MILESTONE_LOOP_START, 0.0),
        (NODE_PERFORMANCE_MILESTONE_LOOP_EXIT, -1.0),
        (NODE_PERFORMANCE_MILESTONE_BOOTSTRAP_COMPLETE, -1.0),
        (NODE_PERFORMANCE_MILESTONE_TIME_ORIGIN, 0.0),
        (NODE_PERFORMANCE_MILESTONE_TIME_ORIGIN_TIMESTAMP, 0.0),
    ];
    for (index, value) in initial {
        milestones
            .set(index, JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    host.bootstrap.borrow_mut().performance_milestones = Some(milestones.clone());
    Ok(())
}

fn node_performance_note_event(host: &NodeRuntimeHost, waiting_events: u64) {
    let mut state = host.bootstrap.borrow_mut();
    state.performance_uv_events = state.performance_uv_events.saturating_add(1);
    state.performance_uv_events_waiting = waiting_events;
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

fn node_host_private_symbol(
    host: &NodeRuntimeHost,
    name: &str,
) -> Result<JsSymbol, SandboxError> {
    if let Some(symbol) = host.bootstrap.borrow().host_private_symbols.get(name).cloned() {
        return Ok(symbol);
    }
    let symbol = JsSymbol::new(Some(JsString::from(format!("node.internal.{name}")))).ok_or_else(
        || SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("failed to create host private symbol `{name}`"),
        },
    )?;
    host.bootstrap
        .borrow_mut()
        .host_private_symbols
        .insert(name.to_string(), symbol.clone());
    Ok(symbol)
}

fn node_next_host_handle_id(host: &NodeRuntimeHost) -> u64 {
    let mut state = host.bootstrap.borrow_mut();
    let id = state.next_host_handle_id;
    state.next_host_handle_id = state.next_host_handle_id.saturating_add(1).max(1);
    id.max(1)
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
        "fs_dir" => node_internal_binding_fs_dir(context),
        "blob" => node_internal_binding_blob(context),
        "buffer" => node_internal_binding_buffer(context),
        "url" => node_internal_binding_url(context),
        "url_pattern" => node_internal_binding_url_pattern(context),
        "modules" => node_internal_binding_modules(context),
        "config" => node_internal_binding_config(context),
        "encoding_binding" => node_internal_binding_encoding(context),
        "string_decoder" => node_internal_binding_string_decoder(context),
        "uv" => node_internal_binding_uv(context),
        "cares_wrap" => node_internal_binding_cares_wrap(context),
        "stream_wrap" => node_internal_binding_stream_wrap(host, context),
        "tcp_wrap" => node_internal_binding_tcp_wrap(context),
        "pipe_wrap" => node_internal_binding_pipe_wrap(context),
        "tty_wrap" => node_internal_binding_tty_wrap(context),
        "udp_wrap" => node_internal_binding_udp_wrap(context),
        "process_wrap" => node_internal_binding_process_wrap(context),
        "spawn_sync" => node_internal_binding_spawn_sync(context),
        "os" => node_internal_binding_os(context),
        "credentials" => node_internal_binding_credentials(context),
        "process_methods" => node_internal_binding_process_methods(host, context),
        "trace_events" => node_internal_binding_trace_events(context),
        "task_queue" => node_internal_binding_task_queue(host, context),
        "timers" => node_internal_binding_timers(host, context),
        "locks" => node_internal_binding_locks(context),
        "worker" => node_internal_binding_worker(context),
        "async_wrap" => node_internal_binding_async_wrap(host, context),
        "async_context_frame" => node_internal_binding_async_context_frame(context),
        "symbols" => node_internal_binding_symbols(context),
        "contextify" => node_internal_binding_contextify(context),
        "messaging" => node_internal_binding_messaging(context),
        "profiler" => node_internal_binding_profiler(context),
        "inspector" => node_internal_binding_inspector(context),
        "wasm_web_api" => node_internal_binding_wasm_web_api(context),
        "cjs_lexer" => node_internal_binding_cjs_lexer(context),
        "permission" => node_internal_binding_permission(context),
        "mksnapshot" => node_internal_binding_mksnapshot(context),
        "performance" => node_internal_binding_performance(context),
        "report" => node_internal_binding_report(context),
        "sea" => node_internal_binding_sea(context),
        "signal_wrap" => node_internal_binding_signal_wrap(context),
        "watchdog" => node_internal_binding_watchdog(context),
        other => Err(SandboxError::Service {
            service: "node_runtime",
            message: format!("unsupported internal binding: {other}"),
        }),
    }
}

const NODE_STREAM_WRAP_K_READ_BYTES_OR_ERROR: i32 = 0;
const NODE_STREAM_WRAP_K_ARRAY_BUFFER_OFFSET: i32 = 1;
const NODE_STREAM_WRAP_K_BYTES_WRITTEN: i32 = 2;
const NODE_STREAM_WRAP_K_LAST_WRITE_WAS_ASYNC: i32 = 3;
const NODE_UV_EBADF: i32 = -9;
const NODE_UV_EINVAL: i32 = -22;
const NODE_UV_EDESTADDRREQ: i32 = -89;
const NODE_UV_EADDRINUSE: i32 = -98;
const NODE_UV_ENOTCONN: i32 = -107;
const NODE_UV_EISCONN: i32 = -106;

fn node_stream_wrap_state_array(
    host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    if let Some(existing) = host.bootstrap.borrow().stream_base_state.clone() {
        return Ok(existing);
    }
    let value = context
        .eval(Source::from_bytes(b"new Int32Array(4)"))
        .map_err(sandbox_execution_error)?;
    let object = value.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "failed to create stream base state array".to_string(),
    })?;
    host.bootstrap.borrow_mut().stream_base_state = Some(object.clone());
    Ok(object)
}

fn node_stream_wrap_req_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let _ = args;
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ()
            .with_message("stream request constructor requires receiver")
            .into());
    };
    for (name, value) in [
        ("oncomplete", JsValue::null()),
        ("callback", JsValue::null()),
        ("handle", JsValue::null()),
    ] {
        this.set(JsString::from(name), value, true, context)?;
    }
    Ok(JsValue::from(this.clone()))
}

fn node_internal_binding_stream_wrap(
    host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let shutdown_wrap = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_stream_wrap_req_construct),
    )
    .name(js_string!("ShutdownWrap"))
    .length(0)
    .constructor(true)
    .build();
    let write_wrap = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_stream_wrap_req_construct),
    )
    .name(js_string!("WriteWrap"))
    .length(0)
    .constructor(true)
    .build();
    let stream_base_state = node_stream_wrap_state_array(host, context)?;
    for (name, value) in [
        ("ShutdownWrap", JsValue::from(shutdown_wrap)),
        ("WriteWrap", JsValue::from(write_wrap)),
        (
            "kReadBytesOrError",
            JsValue::from(NODE_STREAM_WRAP_K_READ_BYTES_OR_ERROR),
        ),
        (
            "kArrayBufferOffset",
            JsValue::from(NODE_STREAM_WRAP_K_ARRAY_BUFFER_OFFSET),
        ),
        ("kBytesWritten", JsValue::from(NODE_STREAM_WRAP_K_BYTES_WRITTEN)),
        (
            "kLastWriteWasAsync",
            JsValue::from(NODE_STREAM_WRAP_K_LAST_WRITE_WAS_ASYNC),
        ),
        ("streamBaseState", JsValue::from(stream_base_state)),
    ] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_stream_handle_id(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<u64, SandboxError> {
    let symbol = node_host_private_symbol(host, "stream_handle.id")?;
    object
        .get(symbol, context)
        .map_err(sandbox_execution_error)?
        .as_number()
        .map(|number| number as u64)
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "stream handle is missing host id".to_string(),
        })
}

fn node_stream_handle_state<F, T>(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
    map: F,
) -> Result<T, SandboxError>
where
    F: FnOnce(&NodeStreamHandleState) -> T,
{
    let id = node_stream_handle_id(host, object, context)?;
    let state = host
        .bootstrap
        .borrow()
        .stream_handles
        .get(&id)
        .cloned()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("unknown stream handle {id}"),
        })?;
    Ok(map(&state))
}

fn node_stream_handle_state_mut<F, T>(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
    map: F,
) -> Result<T, SandboxError>
where
    F: FnOnce(&mut NodeStreamHandleState) -> T,
{
    let id = node_stream_handle_id(host, object, context)?;
    let mut bootstrap = host.bootstrap.borrow_mut();
    let state = bootstrap
        .stream_handles
        .get_mut(&id)
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("unknown stream handle {id}"),
        })?;
    Ok(map(state))
}

fn node_stream_handle_kind_or_code(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<NodeStreamHandleKind, i32> {
    node_stream_handle_state(host, object, context, |state| state.kind.clone())
        .map_err(|_| NODE_UV_EBADF)
}

fn node_stream_handle_set_base_state(
    host: &NodeRuntimeHost,
    context: &mut Context,
    read_bytes_or_error: i32,
    array_buffer_offset: i32,
    bytes_written: i32,
    last_write_was_async: bool,
) -> Result<(), SandboxError> {
    let state = node_stream_wrap_state_array(host, context)?;
    state
        .set(
            NODE_STREAM_WRAP_K_READ_BYTES_OR_ERROR as u32,
            JsValue::from(read_bytes_or_error),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    state
        .set(
            NODE_STREAM_WRAP_K_ARRAY_BUFFER_OFFSET as u32,
            JsValue::from(array_buffer_offset),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    state
        .set(
            NODE_STREAM_WRAP_K_BYTES_WRITTEN as u32,
            JsValue::from(bytes_written),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    state
        .set(
            NODE_STREAM_WRAP_K_LAST_WRITE_WAS_ASYNC as u32,
            JsValue::from(if last_write_was_async { 1 } else { 0 }),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_stream_handle_write_bytes(
    host: &NodeRuntimeHost,
    object: &JsObject,
    bytes: &[u8],
    context: &mut Context,
) -> Result<i32, SandboxError> {
    let (fd, total_bytes) = node_stream_handle_state_mut(host, object, context, |state| {
        state.bytes_written = state.bytes_written.saturating_add(bytes.len() as u64);
        (state.fd, state.bytes_written)
    })?;
    object
        .set(js_string!("bytesWritten"), JsValue::from(total_bytes as f64), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("writeQueueSize"), JsValue::from(0), true, context)
        .map_err(sandbox_execution_error)?;
    node_stream_handle_set_base_state(host, context, 0, 0, bytes.len() as i32, false)?;
    match fd {
        Some(1) => {
            host.process
                .borrow_mut()
                .stdout
                .push_str(&String::from_utf8_lossy(bytes));
        }
        Some(2) => {
            host.process
                .borrow_mut()
                .stderr
                .push_str(&String::from_utf8_lossy(bytes));
        }
        _ => {}
    }
    Ok(0)
}

fn node_stream_handle_construct_with_kind(
    this: &JsValue,
    expected_kind: NodeStreamHandleKind,
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ()
            .with_message("stream handle constructor requires receiver")
            .into());
    };
    node_with_host_js(context, |host, context| {
        let id = node_next_host_handle_id(host);
        let id_symbol = node_host_private_symbol(host, "stream_handle.id")?;
        node_stream_handle_install_base_methods(&this, context)?;
        if expected_kind.is_tcp() {
            node_stream_handle_install_tcp_methods(&this, context)?;
        }
        if expected_kind.is_pipe() {
            node_stream_handle_install_pipe_methods(&this, context)?;
        }
        if expected_kind.is_tty() {
            node_stream_handle_install_tty_methods(&this, context)?;
        }
        this.set(id_symbol, JsValue::from(id as f64), true, context)
            .map_err(sandbox_execution_error)?;
        for (name, value) in [
            ("reading", JsValue::from(false)),
            ("onconnection", JsValue::null()),
            ("onread", JsValue::undefined()),
        ] {
            this.set(JsString::from(name), value, true, context)
                .map_err(sandbox_execution_error)?;
        }
        for (name, value) in [
            (js_string!("bytesWritten"), JsValue::from(0)),
            (js_string!("writeQueueSize"), JsValue::from(0)),
        ] {
            this.define_property_or_throw(
                name,
                PropertyDescriptor::builder()
                    .value(value)
                    .writable(true)
                    .enumerable(true)
                    .configurable(true),
                context,
            )
            .map_err(sandbox_execution_error)?;
        }
        host.bootstrap.borrow_mut().stream_handles.insert(
            id,
            NodeStreamHandleState {
                kind: expected_kind,
                fd: None,
                reading: false,
                closed: false,
                bytes_written: 0,
                listening: false,
                bound_address: None,
                bound_family: None,
                bound_port: None,
                peer_address: None,
                peer_family: None,
                peer_port: None,
                no_delay: false,
                keep_alive: false,
                keep_alive_delay: 0,
                blocking: false,
                raw_mode: false,
                pending_instances: 0,
                pipe_mode: 0,
            },
        );
        Ok(JsValue::from(this.clone()))
    })
}

fn node_tcp_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let kind = match args
        .first()
        .cloned()
        .unwrap_or_else(|| JsValue::from(0))
        .to_i32(context)?
    {
        1 => NodeStreamHandleKind::TcpServer,
        _ => NodeStreamHandleKind::TcpSocket,
    };
    node_stream_handle_construct_with_kind(this, kind, context)
}

fn node_pipe_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let kind = match args
        .first()
        .cloned()
        .unwrap_or_else(|| JsValue::from(0))
        .to_i32(context)?
    {
        1 => NodeStreamHandleKind::PipeServer,
        2 => NodeStreamHandleKind::PipeIpc,
        _ => NodeStreamHandleKind::PipeSocket,
    };
    node_stream_handle_construct_with_kind(this, kind, context)
}

fn node_tty_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let fd = args
        .first()
        .cloned()
        .unwrap_or_else(|| JsValue::from(0))
        .to_i32(context)?;
    let result = node_stream_handle_construct_with_kind(this, NodeStreamHandleKind::Tty, context)?;
    if let Some(object) = this.as_object() {
        node_with_host_js(context, |host, context| {
            node_stream_handle_state_mut(host, &object, context, |state| {
                state.fd = Some(fd);
                state.closed = false;
            })?;
            Ok(JsValue::undefined())
        })?;
    }
    Ok(result)
}

fn node_stream_handle_close(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(-1));
    };
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host_js(context, |host, context| {
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.closed = true;
            state.reading = false;
        })?;
        this.set(js_string!("reading"), JsValue::from(false), true, context)
            .map_err(sandbox_execution_error)?;
        if let Some(callback) = callback.and_then(|value| JsValue::from(value).as_callable()) {
            let _ = callback.call(&JsValue::undefined(), &[], context);
        }
        Ok(JsValue::undefined())
    })
}

fn node_stream_handle_open(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let fd = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)?;
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if fd < 0 {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        if kind.is_tcp() && (0..=2).contains(&fd) {
            return Ok(JsValue::from(NODE_UV_EINVAL));
        }
        if !kind.is_tcp() && !kind.is_pipe() {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.fd = Some(fd);
            state.closed = false;
        })?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_read_start(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(-1));
    };
    node_with_host_js(context, |host, context| {
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.reading = true;
        })?;
        this.set(js_string!("reading"), JsValue::from(true), true, context)
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_read_stop(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(-1));
    };
    node_with_host_js(context, |host, context| {
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.reading = false;
        })?;
        this.set(js_string!("reading"), JsValue::from(false), true, context)
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_use_user_buffer(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_stream_handle_write_buffer(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(-1));
    };
    let data = node_bytes_from_js(args.get(1).unwrap_or(&JsValue::undefined()), context)?;
    node_with_host_js(context, |host, context| {
        Ok(JsValue::from(node_stream_handle_write_bytes(host, &this, &data, context)?))
    })
}

fn node_stream_handle_write_string_with_encoding(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
    encoding: &'static str,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(-1));
    };
    let input = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    let bytes = node_buffer_encode_string(&input, encoding).unwrap_or_default();
    node_with_host_js(context, |host, context| {
        Ok(JsValue::from(node_stream_handle_write_bytes(host, &this, &bytes, context)?))
    })
}

fn node_stream_handle_write_latin1_string(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_stream_handle_write_string_with_encoding(this, args, context, "latin1")
}

fn node_stream_handle_write_utf8_string(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_stream_handle_write_string_with_encoding(this, args, context, "utf8")
}

fn node_stream_handle_write_ascii_string(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_stream_handle_write_string_with_encoding(this, args, context, "ascii")
}

fn node_stream_handle_write_ucs2_string(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_stream_handle_write_string_with_encoding(this, args, context, "utf16le")
}

fn node_stream_handle_writev(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(-1));
    };
    let chunks = args.get(1).and_then(JsValue::as_object);
    let all_buffers = args.get(2).is_some_and(JsValue::to_boolean);
    let mut bytes = Vec::new();
    if let Some(chunks) = chunks {
        let length = chunks.get(js_string!("length"), context)?.to_length(context)?;
        if all_buffers {
            for index in 0..length {
                bytes.extend(node_bytes_from_js(&chunks.get(index, context)?, context)?);
            }
        } else {
            let mut index = 0;
            while index + 1 < length {
                let chunk = chunks.get(index, context)?;
                let encoding = chunks
                    .get(index + 1, context)?
                    .to_string(context)?
                    .to_std_string_escaped();
                let chunk_string = chunk.to_string(context)?.to_std_string_escaped();
                bytes.extend(node_buffer_encode_string(&chunk_string, &encoding).unwrap_or_default());
                index += 2;
            }
        }
    }
    node_with_host_js(context, |host, context| {
        Ok(JsValue::from(node_stream_handle_write_bytes(host, &this, &bytes, context)?))
    })
}

fn node_stream_handle_shutdown(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(-1));
    };
    let req = args.first().and_then(JsValue::as_object);
    node_with_host_js(context, |host, context| {
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.reading = false;
        })?;
        this.set(js_string!("reading"), JsValue::from(false), true, context)
            .map_err(sandbox_execution_error)?;
        if let Some(req) = req {
            req.set(js_string!("handle"), JsValue::from(this.clone()), true, context)
                .map_err(sandbox_execution_error)?;
            if let Some(oncomplete) = req
                .get(js_string!("oncomplete"), context)
                .map_err(sandbox_execution_error)?
                .as_callable()
            {
                let _ = oncomplete.call(&JsValue::from(req), &[], context);
            }
        }
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_getsockname(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let Some(out) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::from(NODE_UV_EINVAL));
    };
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !matches!(kind, NodeStreamHandleKind::TcpSocket | NodeStreamHandleKind::TcpServer) {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        let (address, family, port) = node_stream_handle_state(host, &this, context, |state| {
            (
                state.bound_address.clone().unwrap_or_else(|| {
                    if state.fd.is_some() { "127.0.0.1".to_string() } else { String::new() }
                }),
                state.bound_family.clone().unwrap_or_else(|| {
                    if state.fd.is_some() { "IPv4".to_string() } else { String::new() }
                }),
                state.bound_port.unwrap_or(0),
            )
        })?;
        out.set(js_string!("address"), JsValue::from(JsString::from(address)), true, context)
            .map_err(sandbox_execution_error)?;
        out.set(js_string!("family"), JsValue::from(JsString::from(family)), true, context)
            .map_err(sandbox_execution_error)?;
        out.set(js_string!("port"), JsValue::from(port), true, context)
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_getpeername(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let Some(out) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::from(NODE_UV_EINVAL));
    };
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !kind.is_tcp() {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        let (address, family, port) = node_stream_handle_state(host, &this, context, |state| {
            (
                state.peer_address.clone().unwrap_or_default(),
                state.peer_family.clone().unwrap_or_default(),
                state.peer_port.unwrap_or(0),
            )
        })?;
        out.set(js_string!("address"), JsValue::from(JsString::from(address)), true, context)
            .map_err(sandbox_execution_error)?;
        out.set(js_string!("family"), JsValue::from(JsString::from(family)), true, context)
            .map_err(sandbox_execution_error)?;
        out.set(js_string!("port"), JsValue::from(port), true, context)
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_set_no_delay(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let enable = args.first().is_some_and(JsValue::to_boolean);
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !matches!(kind, NodeStreamHandleKind::TcpSocket) {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.no_delay = enable;
        })?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_set_keep_alive(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let enable = args.first().is_some_and(JsValue::to_boolean);
    let delay = args.get(1).cloned().unwrap_or_else(|| JsValue::from(0)).to_u32(context)?;
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !matches!(kind, NodeStreamHandleKind::TcpSocket) {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.keep_alive = enable;
            state.keep_alive_delay = delay;
        })?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_set_blocking(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let enable = args.first().is_some_and(JsValue::to_boolean);
    node_with_host_js(context, |host, context| {
        if let Err(code) = node_stream_handle_kind_or_code(host, &this, context) {
            return Ok(JsValue::from(code));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.blocking = enable;
        })?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_bind(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let address = args.first().cloned().unwrap_or_else(JsValue::undefined).to_string(context)?.to_std_string_escaped();
    let port = args.get(1).cloned().unwrap_or_else(|| JsValue::from(0)).to_u32(context)? as u16;
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if kind.is_tcp() {
            if address.parse::<std::net::Ipv4Addr>().is_err() {
                return Ok(JsValue::from(NODE_UV_EINVAL));
            }
            let already_bound = node_stream_handle_state(host, &this, context, |state| {
                state.bound_address.is_some() || state.listening
            })?;
            if already_bound {
                return Ok(JsValue::from(NODE_UV_EADDRINUSE));
            }
            node_stream_handle_state_mut(host, &this, context, |state| {
                state.bound_address = Some(address.clone());
                state.bound_family = Some("IPv4".to_string());
                state.bound_port = Some(port);
            })?;
            return Ok(JsValue::from(0));
        }
        if kind.is_pipe() {
            if address.is_empty() {
                return Ok(JsValue::from(NODE_UV_EINVAL));
            }
            let already_bound = node_stream_handle_state(host, &this, context, |state| {
                state.bound_address.is_some() || state.listening
            })?;
            if already_bound {
                return Ok(JsValue::from(NODE_UV_EADDRINUSE));
            }
            node_stream_handle_state_mut(host, &this, context, |state| {
                state.bound_address = Some(address.clone());
                state.bound_family = None;
                state.bound_port = None;
            })?;
            return Ok(JsValue::from(0));
        }
        Ok(JsValue::from(NODE_UV_EBADF))
    })
}

fn node_stream_handle_bind6(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let address = args.first().cloned().unwrap_or_else(JsValue::undefined).to_string(context)?.to_std_string_escaped();
    let port = args.get(1).cloned().unwrap_or_else(|| JsValue::from(0)).to_u32(context)? as u16;
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !matches!(kind, NodeStreamHandleKind::TcpSocket | NodeStreamHandleKind::TcpServer) ||
            address.parse::<std::net::Ipv6Addr>().is_err()
        {
            return Ok(JsValue::from(NODE_UV_EINVAL));
        }
        let already_bound = node_stream_handle_state(host, &this, context, |state| {
            state.bound_address.is_some() || state.listening
        })?;
        if already_bound {
            return Ok(JsValue::from(NODE_UV_EADDRINUSE));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.bound_address = Some(address.clone());
            state.bound_family = Some("IPv6".to_string());
            state.bound_port = Some(port);
        })?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_listen(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let backlog = _args
        .first()
        .cloned()
        .unwrap_or_else(|| JsValue::from(0))
        .to_i32(context)?;
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        let is_server = matches!(kind, NodeStreamHandleKind::TcpServer | NodeStreamHandleKind::PipeServer);
        if !is_server || backlog < 0 {
            return Ok(JsValue::from(NODE_UV_EINVAL));
        }
        let ready = node_stream_handle_state(host, &this, context, |state| {
            !state.closed && !state.listening && (state.fd.is_some() || state.bound_address.is_some())
        })?;
        if !ready {
            return Ok(JsValue::from(NODE_UV_EINVAL));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.listening = true;
        })?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_connect(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let req = args.first().and_then(JsValue::as_object);
    let address = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_string(context)?.to_std_string_escaped();
    let port_value = args.get(2).cloned().unwrap_or_else(|| JsValue::from(0));
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if matches!(kind, NodeStreamHandleKind::TcpSocket) {
            let Some(req) = req else {
                return Ok(JsValue::from(NODE_UV_EINVAL));
            };
            let port = match port_value.to_u32(context) {
                Ok(value) => value as u16,
                Err(_) => return Ok(JsValue::from(NODE_UV_EINVAL)),
            };
            if address.parse::<std::net::Ipv4Addr>().is_err() {
                return Ok(JsValue::from(NODE_UV_EINVAL));
            }
            let already_connected =
                node_stream_handle_state(host, &this, context, |state| state.peer_address.is_some())?;
            if already_connected {
                return Ok(JsValue::from(NODE_UV_EISCONN));
            }
            node_stream_handle_state_mut(host, &this, context, |state| {
                state.peer_address = Some(address.clone());
                state.peer_family = Some("IPv4".to_string());
                state.peer_port = Some(port);
                if state.bound_address.is_none() {
                    state.bound_address = Some("0.0.0.0".to_string());
                    state.bound_family = Some("IPv4".to_string());
                    state.bound_port = Some(0);
                }
            })?;
            req.set(js_string!("handle"), JsValue::from(this.clone()), true, context)
                .map_err(sandbox_execution_error)?;
            if let Some(oncomplete) = req.get(js_string!("oncomplete"), context).map_err(sandbox_execution_error)?.as_callable() {
                let _ = oncomplete.call(
                    &JsValue::from(req.clone()),
                    &[
                        JsValue::from(0),
                        JsValue::from(this.clone()),
                        JsValue::from(req.clone()),
                        JsValue::from(true),
                        JsValue::from(true),
                    ],
                    context,
                );
            }
            return Ok(JsValue::from(0));
        } else if matches!(kind, NodeStreamHandleKind::PipeSocket | NodeStreamHandleKind::PipeIpc) {
            let Some(req) = req else {
                return Ok(JsValue::from(NODE_UV_EINVAL));
            };
            if address.is_empty() {
                return Ok(JsValue::from(NODE_UV_EINVAL));
            }
            let already_connected =
                node_stream_handle_state(host, &this, context, |state| state.peer_address.is_some())?;
            if already_connected {
                return Ok(JsValue::from(NODE_UV_EISCONN));
            }
            node_stream_handle_state_mut(host, &this, context, |state| {
                state.peer_address = Some(address.clone());
                state.peer_family = None;
                state.peer_port = None;
            })?;
            req.set(js_string!("handle"), JsValue::from(this.clone()), true, context)
                .map_err(sandbox_execution_error)?;
            if let Some(oncomplete) = req.get(js_string!("oncomplete"), context).map_err(sandbox_execution_error)?.as_callable() {
                let _ = oncomplete.call(
                    &JsValue::from(req.clone()),
                    &[
                        JsValue::from(0),
                        JsValue::from(this.clone()),
                        JsValue::from(req.clone()),
                        JsValue::from(true),
                        JsValue::from(true),
                    ],
                    context,
                );
            }
            return Ok(JsValue::from(0));
        }
        Ok(JsValue::from(NODE_UV_EBADF))
    })
}

fn node_stream_handle_connect6(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let req = args.first().and_then(JsValue::as_object);
    let address = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_string(context)?.to_std_string_escaped();
    let port = args.get(2).cloned().unwrap_or_else(|| JsValue::from(0)).to_u32(context)? as u16;
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !matches!(kind, NodeStreamHandleKind::TcpSocket) ||
            address.parse::<std::net::Ipv6Addr>().is_err()
        {
            return Ok(JsValue::from(NODE_UV_EINVAL));
        }
        let Some(req) = req else {
            return Ok(JsValue::from(NODE_UV_EINVAL));
        };
        let already_connected =
            node_stream_handle_state(host, &this, context, |state| state.peer_address.is_some())?;
        if already_connected {
            return Ok(JsValue::from(NODE_UV_EISCONN));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.peer_address = Some(address.clone());
            state.peer_family = Some("IPv6".to_string());
            state.peer_port = Some(port);
            if state.bound_address.is_none() {
                state.bound_address = Some("::".to_string());
                state.bound_family = Some("IPv6".to_string());
                state.bound_port = Some(0);
            }
        })?;
        req.set(js_string!("handle"), JsValue::from(this.clone()), true, context)
            .map_err(sandbox_execution_error)?;
        if let Some(oncomplete) = req.get(js_string!("oncomplete"), context).map_err(sandbox_execution_error)?.as_callable() {
            let _ = oncomplete.call(
                &JsValue::from(req.clone()),
                &[
                    JsValue::from(0),
                    JsValue::from(this.clone()),
                    JsValue::from(req.clone()),
                    JsValue::from(true),
                    JsValue::from(true),
                ],
                context,
            );
        }
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_reset(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let close_callback = _args.first().cloned();
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !matches!(kind, NodeStreamHandleKind::TcpSocket) {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.closed = true;
            state.reading = false;
        })?;
        this.set(js_string!("reading"), JsValue::from(false), true, context)
            .map_err(sandbox_execution_error)?;
        if let Some(callback) = close_callback.and_then(|value| value.as_callable()) {
            let symbols = node_internal_binding_symbols(context)?;
            let onclose = symbols
                .get(js_string!("handle_onclose_symbol"), context)
                .map_err(sandbox_execution_error)?
                .as_symbol()
                .ok_or_else(|| sandbox_execution_error("handle_onclose_symbol must be a symbol"))?;
            this.set(onclose, JsValue::from(callback.clone()), true, context)
                .map_err(sandbox_execution_error)?;
        }
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_get_window_size(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let is_tty = node_with_host(|host| {
        let kind = node_stream_handle_kind_or_code(host, &this, context)
            .map_err(|_| SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: "invalid tty handle".to_string(),
            })?;
        let has_fd =
            node_stream_handle_state(host, &this, context, |state| state.fd.is_some() && !state.closed)?;
        Ok(kind.is_tty() && has_fd)
    })
    .map_err(js_error)?;
    if !is_tty {
        return Ok(JsValue::from(NODE_UV_EBADF));
    }
    let Some(out) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::from(NODE_UV_EINVAL));
    };
    node_with_host_js(context, |host, context| {
        let columns = host
            .process
            .borrow()
            .env
            .get("COLUMNS")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(80);
        let lines = host
            .process
            .borrow()
            .env
            .get("LINES")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(24);
        out.set(0u32, JsValue::from(columns), true, context)
            .map_err(sandbox_execution_error)?;
        out.set(1u32, JsValue::from(lines), true, context)
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_set_raw_mode(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let enable = args.first().is_some_and(JsValue::to_boolean);
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !kind.is_tty() {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        let has_fd =
            node_stream_handle_state(host, &this, context, |state| state.fd.is_some() && !state.closed)?;
        if !has_fd {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.raw_mode = enable;
        })?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_fchmod(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let mode = args.first().cloned().unwrap_or_else(|| JsValue::from(0)).to_i32(context)?;
    node_with_host_js(context, |host, context| {
        let kind = match node_stream_handle_kind_or_code(host, &this, context) {
            Ok(kind) => kind,
            Err(code) => return Ok(JsValue::from(code)),
        };
        if !kind.is_pipe() {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        let is_alive =
            node_stream_handle_state(host, &this, context, |state| !state.closed)?;
        if !is_alive {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        node_stream_handle_state_mut(host, &this, context, |state| {
            state.pipe_mode = mode;
        })?;
        Ok(JsValue::from(0))
    })
}

fn node_stream_handle_bytes_written_getter(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(0));
    };
    node_with_host(|host| {
        let bytes = node_stream_handle_state(host, &this, context, |state| state.bytes_written)?;
        Ok(JsValue::from(bytes as f64))
    })
    .map_err(js_error)
}

fn node_stream_handle_get_async_id(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(0));
    };
    node_with_host(|host| {
        let id = node_stream_handle_id(host, &this, context)?;
        Ok(JsValue::from(id as f64))
    })
    .map_err(js_error)
}

fn node_stream_handle_write_queue_size_getter(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_stream_handle_define_method(
    target: &JsObject,
    method_name: &'static str,
    length: usize,
    function: fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>,
    context: &mut Context,
) -> Result<(), SandboxError> {
    target
        .set(
            JsString::from(method_name),
            JsValue::from(
                FunctionObjectBuilder::new(
                    context.realm(),
                    NativeFunction::from_fn_ptr(function),
                )
                .name(JsString::from(method_name))
                .length(length)
                .constructor(false)
                .build(),
            ),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_stream_handle_install_base_methods(
    target: &JsObject,
    context: &mut Context,
) -> Result<(), SandboxError> {
    for (method_name, length, function) in [
        ("close", 1usize, node_stream_handle_close as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("readStart", 0usize, node_stream_handle_read_start),
        ("readStop", 0usize, node_stream_handle_read_stop),
        ("useUserBuffer", 1usize, node_stream_handle_use_user_buffer),
        ("writeBuffer", 2usize, node_stream_handle_write_buffer),
        ("writeLatin1String", 2usize, node_stream_handle_write_latin1_string),
        ("writeUtf8String", 2usize, node_stream_handle_write_utf8_string),
        ("writeAsciiString", 2usize, node_stream_handle_write_ascii_string),
        ("writeUcs2String", 2usize, node_stream_handle_write_ucs2_string),
        ("writev", 3usize, node_stream_handle_writev),
        ("shutdown", 1usize, node_stream_handle_shutdown),
        ("getAsyncId", 0usize, node_stream_handle_get_async_id),
        ("setBlocking", 1usize, node_stream_handle_set_blocking),
    ] {
        node_stream_handle_define_method(target, method_name, length, function, context)?;
    }
    target
        .define_property_or_throw(
            js_string!("bytesWritten"),
            PropertyDescriptor::builder()
                .get(
                    FunctionObjectBuilder::new(
                        context.realm(),
                        NativeFunction::from_fn_ptr(node_stream_handle_bytes_written_getter),
                    )
                    .name(js_string!("get bytesWritten"))
                    .length(0)
                    .constructor(false)
                    .build(),
                )
                .enumerable(true)
                .configurable(true),
            context,
        )
        .map_err(sandbox_execution_error)?;
    target
        .define_property_or_throw(
            js_string!("writeQueueSize"),
            PropertyDescriptor::builder()
                .get(
                    FunctionObjectBuilder::new(
                        context.realm(),
                        NativeFunction::from_fn_ptr(node_stream_handle_write_queue_size_getter),
                    )
                    .name(js_string!("get writeQueueSize"))
                    .length(0)
                    .constructor(false)
                    .build(),
                )
                .enumerable(true)
                .configurable(true),
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_stream_handle_install_tcp_methods(
    target: &JsObject,
    context: &mut Context,
) -> Result<(), SandboxError> {
    for (method_name, length, function) in [
        ("open", 1usize, node_stream_handle_open as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("bind", 3usize, node_stream_handle_bind),
        ("bind6", 3usize, node_stream_handle_bind6),
        ("listen", 1usize, node_stream_handle_listen),
        ("connect", 3usize, node_stream_handle_connect),
        ("connect6", 3usize, node_stream_handle_connect6),
        ("getsockname", 1usize, node_stream_handle_getsockname),
        ("getpeername", 1usize, node_stream_handle_getpeername),
        ("setNoDelay", 1usize, node_stream_handle_set_no_delay),
        ("setKeepAlive", 2usize, node_stream_handle_set_keep_alive),
        ("reset", 0usize, node_stream_handle_reset),
    ] {
        node_stream_handle_define_method(target, method_name, length, function, context)?;
    }
    Ok(())
}

fn node_stream_handle_install_pipe_methods(
    target: &JsObject,
    context: &mut Context,
) -> Result<(), SandboxError> {
    for (method_name, length, function) in [
        ("open", 1usize, node_stream_handle_open as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("bind", 3usize, node_stream_handle_bind),
        ("listen", 1usize, node_stream_handle_listen),
        ("connect", 3usize, node_stream_handle_connect),
        ("fchmod", 1usize, node_stream_handle_fchmod),
    ] {
        node_stream_handle_define_method(target, method_name, length, function, context)?;
    }
    Ok(())
}

fn node_stream_handle_install_tty_methods(
    target: &JsObject,
    context: &mut Context,
) -> Result<(), SandboxError> {
    for (method_name, length, function) in [
        ("getWindowSize", 1usize, node_stream_handle_get_window_size as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("setRawMode", 1usize, node_stream_handle_set_raw_mode),
    ] {
        node_stream_handle_define_method(target, method_name, length, function, context)?;
    }
    Ok(())
}

fn node_stream_handle_constructor(
    constructor_fn: fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>,
    name: &'static str,
    install: fn(&JsObject, &mut Context) -> Result<(), SandboxError>,
    context: &mut Context,
) -> Result<JsFunction, SandboxError> {
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(constructor_fn),
    )
    .name(JsString::from(name))
    .length(1)
    .constructor(true)
    .build();
    let prototype = JsObject::with_null_proto();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype.clone()), false, context)
        .map_err(sandbox_execution_error)?;
    prototype
        .set(js_string!("constructor"), JsValue::from(constructor.clone()), true, context)
        .map_err(sandbox_execution_error)?;
    node_stream_handle_install_base_methods(&prototype, context)?;
    install(&prototype, context)?;
    Ok(constructor)
}

fn node_stream_connect_wrap_construct(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ()
            .with_message("connect request constructor requires receiver")
            .into());
    };
    for (name, value) in [
        ("oncomplete", JsValue::null()),
        ("callback", JsValue::null()),
        ("handle", JsValue::null()),
    ] {
        this.set(JsString::from(name), value, true, context)?;
    }
    Ok(JsValue::from(this.clone()))
}

fn node_stream_connect_wrap_constructor(
    name: &'static str,
    context: &mut Context,
) -> Result<JsFunction, SandboxError> {
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_stream_connect_wrap_construct),
    )
    .name(JsString::from(name))
    .length(0)
    .constructor(true)
    .build();
    let prototype = JsObject::with_null_proto();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype.clone()), false, context)
        .map_err(sandbox_execution_error)?;
    prototype
        .set(js_string!("constructor"), JsValue::from(constructor.clone()), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(constructor)
}

fn node_internal_binding_tcp_wrap(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let tcp = node_stream_handle_constructor(
        node_tcp_construct,
        "TCP",
        node_stream_handle_install_tcp_methods,
        context,
    )?;
    let tcp_connect_wrap = node_stream_connect_wrap_constructor("TCPConnectWrap", context)?;
    let constants = ObjectInitializer::new(context)
        .property(js_string!("SOCKET"), JsValue::from(0), Attribute::all())
        .property(js_string!("SERVER"), JsValue::from(1), Attribute::all())
        .property(js_string!("UV_TCP_IPV6ONLY"), JsValue::from(1), Attribute::all())
        .property(js_string!("UV_TCP_REUSEPORT"), JsValue::from(2), Attribute::all())
        .build();
    for (name, value) in [
        ("TCP", JsValue::from(tcp)),
        ("TCPConnectWrap", JsValue::from(tcp_connect_wrap)),
        ("constants", JsValue::from(constants)),
    ] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_pipe_wrap(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let pipe = node_stream_handle_constructor(
        node_pipe_construct,
        "Pipe",
        node_stream_handle_install_pipe_methods,
        context,
    )?;
    let pipe_connect_wrap = node_stream_connect_wrap_constructor("PipeConnectWrap", context)?;
    let constants = ObjectInitializer::new(context)
        .property(js_string!("SOCKET"), JsValue::from(0), Attribute::all())
        .property(js_string!("SERVER"), JsValue::from(1), Attribute::all())
        .property(js_string!("IPC"), JsValue::from(2), Attribute::all())
        .property(js_string!("UV_READABLE"), JsValue::from(1), Attribute::all())
        .property(js_string!("UV_WRITABLE"), JsValue::from(2), Attribute::all())
        .build();
    for (name, value) in [
        ("Pipe", JsValue::from(pipe)),
        ("PipeConnectWrap", JsValue::from(pipe_connect_wrap)),
        ("constants", JsValue::from(constants)),
    ] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_tty_wrap_is_tty(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let fd = args
        .first()
        .cloned()
        .unwrap_or_else(|| JsValue::from(-1))
        .to_i32(context)?;
    Ok(JsValue::from(false))
}

fn node_internal_binding_tty_wrap(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let tty = node_stream_handle_constructor(
        node_tty_construct,
        "TTY",
        node_stream_handle_install_tty_methods,
        context,
    )?;
    let is_tty = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_tty_wrap_is_tty),
    )
    .name(js_string!("isTTY"))
    .length(1)
    .constructor(false)
    .build();
    for (name, value) in [("TTY", JsValue::from(tty)), ("isTTY", JsValue::from(is_tty))] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_udp_handle_id(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<u64, SandboxError> {
    let symbol = node_host_private_symbol(host, "udp_handle.id")?;
    object
        .get(symbol, context)
        .map_err(sandbox_execution_error)?
        .as_number()
        .map(|value| value as u64)
        .ok_or_else(|| sandbox_execution_error("udp handle is missing host id"))
}

fn node_udp_handle_state<F, T>(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
    f: F,
) -> Result<T, SandboxError>
where
    F: FnOnce(&NodeUdpHandleState) -> T,
{
    let id = node_udp_handle_id(host, object, context)?;
    let bootstrap = host.bootstrap.borrow();
    let state = bootstrap
        .udp_handles
        .get(&id)
        .ok_or_else(|| sandbox_execution_error("udp handle state is missing"))?;
    Ok(f(state))
}

fn node_udp_handle_state_mut<F, T>(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
    f: F,
) -> Result<T, SandboxError>
where
    F: FnOnce(&mut NodeUdpHandleState) -> T,
{
    let id = node_udp_handle_id(host, object, context)?;
    let mut bootstrap = host.bootstrap.borrow_mut();
    let state = bootstrap
        .udp_handles
        .get_mut(&id)
        .ok_or_else(|| sandbox_execution_error("udp handle state is missing"))?;
    Ok(f(state))
}

fn node_udp_handle_req_construct(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ()
            .with_message("UDP request constructor requires receiver")
            .into());
    };
    for (name, value) in [
        ("oncomplete", JsValue::null()),
        ("callback", JsValue::null()),
        ("handle", JsValue::null()),
    ] {
        this.set(JsString::from(name), value, true, context)?;
    }
    Ok(JsValue::from(this.clone()))
}

fn node_udp_handle_constructor(context: &mut Context) -> Result<JsFunction, SandboxError> {
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_udp_construct),
    )
    .name(js_string!("UDP"))
    .length(0)
    .constructor(true)
    .build();
    let prototype = JsObject::with_null_proto();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype.clone()), false, context)
        .map_err(sandbox_execution_error)?;
    prototype
        .set(js_string!("constructor"), JsValue::from(constructor.clone()), true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("close", 0usize, node_udp_close as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("ref", 0usize, node_udp_ref),
        ("unref", 0usize, node_udp_unref),
        ("hasRef", 0usize, node_udp_has_ref),
        ("getAsyncId", 0usize, node_udp_get_async_id),
        ("open", 1usize, node_udp_open),
        ("bind", 3usize, node_udp_bind),
        ("bind6", 3usize, node_udp_bind6),
        ("connect", 2usize, node_udp_connect),
        ("connect6", 2usize, node_udp_connect6),
        ("disconnect", 0usize, node_udp_disconnect),
        ("send", 6usize, node_udp_send),
        ("send6", 6usize, node_udp_send6),
        ("recvStart", 0usize, node_udp_recv_start),
        ("recvStop", 0usize, node_udp_recv_stop),
        ("getsockname", 1usize, node_udp_getsockname),
        ("getpeername", 1usize, node_udp_getpeername),
        ("setMulticastInterface", 1usize, node_udp_set_multicast_interface),
        ("addMembership", 2usize, node_udp_add_membership),
        ("dropMembership", 2usize, node_udp_drop_membership),
        ("addSourceSpecificMembership", 3usize, node_udp_add_source_specific_membership),
        ("dropSourceSpecificMembership", 3usize, node_udp_drop_source_specific_membership),
        ("setMulticastTTL", 1usize, node_udp_set_multicast_ttl),
        ("setMulticastLoopback", 1usize, node_udp_set_multicast_loopback),
        ("setBroadcast", 1usize, node_udp_set_broadcast),
        ("setTTL", 1usize, node_udp_set_ttl),
        ("bufferSize", 3usize, node_udp_buffer_size),
        ("getSendQueueSize", 0usize, node_udp_get_send_queue_size),
        ("getSendQueueCount", 0usize, node_udp_get_send_queue_count),
    ] {
        prototype
            .set(
                JsString::from(name),
                JsValue::from(
                    FunctionObjectBuilder::new(
                        context.realm(),
                        NativeFunction::from_fn_ptr(function),
                    )
                    .name(JsString::from(name))
                    .length(length)
                    .constructor(false)
                    .build(),
                ),
                true,
                context,
            )
            .map_err(sandbox_execution_error)?;
    }
    let fd_getter = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_udp_get_fd),
    )
    .name(js_string!("get fd"))
    .length(0)
    .constructor(false)
    .build();
    prototype
        .define_property_or_throw(
            js_string!("fd"),
            PropertyDescriptor::builder()
                .get(fd_getter)
                .enumerable(false)
                .configurable(true),
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(constructor)
}

fn node_udp_construct(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ()
            .with_message("UDP constructor requires receiver")
            .into());
    };
    node_with_host_js(context, |host, context| {
        let id = node_next_host_handle_id(host);
        let id_symbol = node_host_private_symbol(host, "udp_handle.id")?;
        this.set(id_symbol, JsValue::from(id as f64), true, context)
            .map_err(sandbox_execution_error)?;
        host.bootstrap.borrow_mut().udp_handles.insert(
            id,
            NodeUdpHandleState {
                object: Some(this.clone()),
                fd: None,
                closed: false,
                refed: true,
                recv_started: false,
                bound_address: None,
                bound_family: None,
                bound_port: None,
                peer_address: None,
                peer_family: None,
                peer_port: None,
                recv_buffer_size: 0,
                send_buffer_size: 0,
                multicast_interface: None,
                multicast_ttl: 0,
                multicast_loopback: 0,
                broadcast: 0,
                ttl: 0,
                send_queue_size: 0,
                send_queue_count: 0,
            },
        );
        Ok(JsValue::from(this.clone()))
    })
}

fn node_udp_close(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("UDP receiver must be an object").into());
    };
    node_with_host(|host| {
        node_udp_handle_state_mut(host, &this, context, |state| {
            state.closed = true;
            state.recv_started = false;
            state.fd = None;
            state.send_queue_count = 0;
            state.send_queue_size = 0;
        })?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_udp_ref(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("UDP receiver must be an object").into());
    };
    node_with_host(|host| {
        node_udp_handle_state_mut(host, &this, context, |state| state.refed = true)?;
        Ok(JsValue::from(this.clone()))
    })
    .map_err(js_error)
}

fn node_udp_unref(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("UDP receiver must be an object").into());
    };
    node_with_host(|host| {
        node_udp_handle_state_mut(host, &this, context, |state| state.refed = false)?;
        Ok(JsValue::from(this.clone()))
    })
    .map_err(js_error)
}

fn node_udp_has_ref(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(false));
    };
    node_with_host(|host| {
        let refed = node_udp_handle_state(host, &this, context, |state| state.refed && !state.closed)?;
        Ok(JsValue::from(refed))
    })
    .map_err(js_error)
}

fn node_udp_get_async_id(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(0));
    };
    node_with_host(|host| Ok(JsValue::from(node_udp_handle_id(host, &this, context)? as i32))).map_err(js_error)
}

fn node_udp_get_fd(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    node_with_host(|host| {
        let fd = node_udp_handle_state(host, &this, context, |state| {
            if state.closed { NODE_UV_EBADF } else { state.fd.unwrap_or(NODE_UV_EBADF) }
        })?;
        Ok(JsValue::from(fd))
    })
    .map_err(js_error)
}

fn node_udp_open(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let fd = args.first().cloned().unwrap_or_else(|| JsValue::from(-1)).to_i32(context)?;
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed || fd < 0 {
                NODE_UV_EBADF
            } else {
                state.fd = Some(fd);
                0
            }
        })?;
        Ok(JsValue::from(code))
    })
    .map_err(js_error)
}

fn node_udp_bind_impl(this: &JsObject, address: String, port: u16, family: &str, _flags: u32, context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, this, context, |state| {
            if state.closed {
                NODE_UV_EBADF
            } else if state.bound_address.is_some() {
                NODE_UV_EADDRINUSE
            } else {
                state.bound_address = Some(address);
                state.bound_family = Some(family.to_string());
                state.bound_port = Some(port);
                0
            }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_bind(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let address = node_arg_string(args, 0, context)?;
    let port = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)? as u16;
    let flags = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)?;
    node_udp_bind_impl(&this, address, port, "IPv4", flags, context)
}

fn node_udp_bind6(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let address = node_arg_string(args, 0, context)?;
    let port = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)? as u16;
    let flags = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)?;
    node_udp_bind_impl(&this, address, port, "IPv6", flags, context)
}

fn node_udp_connect_impl(this: &JsObject, address: String, port: u16, family: &str, context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, this, context, |state| {
            if state.closed {
                NODE_UV_EBADF
            } else {
                state.peer_address = Some(address);
                state.peer_family = Some(family.to_string());
                state.peer_port = Some(port);
                0
            }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_connect(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let address = node_arg_string(args, 0, context)?;
    let port = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)? as u16;
    node_udp_connect_impl(&this, address, port, "IPv4", context)
}

fn node_udp_connect6(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let address = node_arg_string(args, 0, context)?;
    let port = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)? as u16;
    node_udp_connect_impl(&this, address, port, "IPv6", context)
}

fn node_udp_disconnect(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed {
                NODE_UV_EBADF
            } else {
                state.peer_address = None;
                state.peer_family = None;
                state.peer_port = None;
                0
            }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_send_impl(this: &JsObject, args: &[JsValue], family: &str, context: &mut Context) -> JsResult<JsValue> {
    let sendto = args.len() == 6;
    let list = args.get(1).and_then(JsValue::as_object).ok_or_else(|| JsNativeError::typ().with_message("UDP send list must be an array"))?;
    let count = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)? as u64;
    let port_arg = if sendto { Some(args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)? as u16) } else { None };
    let address_arg = if sendto { Some(node_arg_string(args, 4, context)?) } else { None };
    let _has_callback = if sendto {
        args.get(5).cloned().unwrap_or_else(JsValue::undefined).to_boolean()
    } else {
        args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_boolean()
    };
    let mut msg_size = 0usize;
    let mut payload = Vec::new();
    for index in 0..count {
        let chunk = list.get(index, context)?;
        let bytes = node_buffer_bytes(&chunk, context)?;
        msg_size = msg_size.saturating_add(bytes.len());
        payload.extend_from_slice(&bytes);
    }
    node_with_host(|host| {
        let (code, target) = {
            let mut bootstrap = host.bootstrap.borrow_mut();
            let id = node_udp_handle_id(host, this, context)?;
            let Some(state) = bootstrap.udp_handles.get_mut(&id) else {
                return Ok(JsValue::from(NODE_UV_EBADF));
            };
            if state.closed {
                return Ok(JsValue::from(NODE_UV_EBADF));
            }
            let (target_family, target_address, target_port) = if sendto {
                (
                    family.to_string(),
                    address_arg.clone().unwrap_or_default(),
                    port_arg.unwrap_or(0),
                )
            } else if let (Some(peer_family), Some(peer_address), Some(peer_port)) =
                (state.peer_family.clone(), state.peer_address.clone(), state.peer_port)
            {
                (peer_family, peer_address, peer_port)
            } else {
                return Ok(JsValue::from(NODE_UV_EDESTADDRREQ));
            };
            if state.bound_family.is_none() {
                state.bound_family = Some(family.to_string());
            }
            if state.bound_address.is_none() {
                state.bound_address = Some(if family == "IPv6" { "::1".to_string() } else { "127.0.0.1".to_string() });
            }
            if state.bound_port.is_none() {
                state.bound_port = Some(0);
            }
            let source_family = state.bound_family.clone().unwrap_or_else(|| family.to_string());
            let source_address = state.bound_address.clone().unwrap_or_else(|| {
                if family == "IPv6" { "::1".to_string() } else { "127.0.0.1".to_string() }
            });
            let source_port = state.bound_port.unwrap_or(0);
            state.send_queue_count = 0;
            state.send_queue_size = 0;
            let target = bootstrap
                .udp_handles
                .values()
                .find(|candidate| {
                    !candidate.closed
                        && candidate.recv_started
                        && candidate.bound_family.as_deref() == Some(target_family.as_str())
                        && candidate.bound_address.as_deref() == Some(target_address.as_str())
                        && candidate.bound_port == Some(target_port)
                })
                .and_then(|candidate| candidate.object.clone())
                .map(|object| (object, source_family, source_address, source_port));
            ((msg_size as i32).saturating_add(1), target)
        };
        if let Some((target, source_family, source_address, source_port)) = target {
            node_udp_schedule_message_delivery(
                target,
                payload,
                source_family,
                source_address,
                source_port,
                context,
            );
        }
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_send(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    node_udp_send_impl(&this, args, "IPv4", context)
}

fn node_udp_send6(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    node_udp_send_impl(&this, args, "IPv6", context)
}

fn node_udp_recv_start(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed { NODE_UV_EBADF } else { state.recv_started = true; 0 }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_recv_stop(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed { NODE_UV_EBADF } else { state.recv_started = false; 0 }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_fill_name(out: &JsObject, address: Option<String>, family: Option<String>, port: Option<u16>, context: &mut Context) -> Result<(), SandboxError> {
    out.set(js_string!("address"), JsValue::from(JsString::from(address.unwrap_or_default())), true, context)
        .map_err(sandbox_execution_error)?;
    out.set(js_string!("family"), JsValue::from(JsString::from(family.unwrap_or_default())), true, context)
        .map_err(sandbox_execution_error)?;
    out.set(js_string!("port"), JsValue::from(port.unwrap_or(0)), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_udp_find_bound_target(
    host: &NodeRuntimeHost,
    family: &str,
    address: &str,
    port: u16,
) -> Option<JsObject> {
    host.bootstrap
        .borrow()
        .udp_handles
        .values()
        .find(|state| {
            !state.closed
                && state.recv_started
                && state.bound_family.as_deref() == Some(family)
                && state.bound_address.as_deref() == Some(address)
                && state.bound_port == Some(port)
        })
        .and_then(|state| state.object.clone())
}

fn node_udp_schedule_message_delivery(
    target: JsObject,
    bytes: Vec<u8>,
    family: String,
    address: String,
    port: u16,
    context: &mut Context,
) {
    context.enqueue_job(
        NativeAsyncJob::new(async move |context_cell| {
            let context = &mut context_cell.borrow_mut();
            let onmessage = target.get(js_string!("onmessage"), context)?;
            let Some(onmessage) = onmessage.as_callable() else {
                return Ok(JsValue::undefined());
            };
            let nread = bytes.len() as i32;
            let buffer = node_js_buffer_from_bytes(bytes, context)?;
            let rinfo = JsValue::from_json(
                &serde_json::json!({
                    "address": address,
                    "family": family,
                    "port": port,
                }),
                context,
            )
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
            onmessage.call(
                &JsValue::from(target.clone()),
                &[
                    JsValue::from(nread),
                    JsValue::from(target.clone()),
                    buffer,
                    rinfo,
                ],
                context,
            )?;
            Ok(JsValue::undefined())
        })
        .into(),
    );
}

fn node_udp_getsockname(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let Some(out) = args.first().and_then(JsValue::as_object) else { return Ok(JsValue::from(NODE_UV_EINVAL)); };
    node_with_host(|host| {
        let (closed, address, family, port) = node_udp_handle_state(host, &this, context, |state| {
            (state.closed, state.bound_address.clone(), state.bound_family.clone(), state.bound_port)
        })?;
        if closed {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        node_udp_fill_name(&out, address, family, port, context)?;
        Ok(JsValue::from(0))
    }).map_err(js_error)
}

fn node_udp_getpeername(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let Some(out) = args.first().and_then(JsValue::as_object) else { return Ok(JsValue::from(NODE_UV_EINVAL)); };
    node_with_host(|host| {
        let (closed, address, family, port) = node_udp_handle_state(host, &this, context, |state| {
            (state.closed, state.peer_address.clone(), state.peer_family.clone(), state.peer_port)
        })?;
        if closed {
            return Ok(JsValue::from(NODE_UV_EBADF));
        }
        if address.is_none() {
            return Ok(JsValue::from(NODE_UV_EINVAL));
        }
        node_udp_fill_name(&out, address, family, port, context)?;
        Ok(JsValue::from(0))
    }).map_err(js_error)
}

fn node_udp_set_multicast_interface(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let iface = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed { NODE_UV_EBADF } else { state.multicast_interface = Some(iface); 0 }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_add_membership(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let _address = node_arg_string(args, 0, context)?;
    let _iface = args.get(1).filter(|v| !v.is_null() && !v.is_undefined()).map(|_| node_arg_string(args, 1, context)).transpose()?;
    node_with_host(|host| {
        let code = node_udp_handle_state(host, &this, context, |state| if state.closed { NODE_UV_EBADF } else { 0 })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_drop_membership(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_udp_add_membership(this, args, context)
}

fn node_udp_add_source_specific_membership(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let _source = node_arg_string(args, 0, context)?;
    let _group = node_arg_string(args, 1, context)?;
    let _iface = args.get(2).filter(|v| !v.is_null() && !v.is_undefined()).map(|_| node_arg_string(args, 2, context)).transpose()?;
    node_with_host(|host| {
        let code = node_udp_handle_state(host, &this, context, |state| if state.closed { NODE_UV_EBADF } else { 0 })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_drop_source_specific_membership(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_udp_add_source_specific_membership(this, args, context)
}

fn node_udp_set_multicast_ttl(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let ttl = args.first().cloned().unwrap_or_else(JsValue::undefined).to_i32(context)?;
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed { NODE_UV_EBADF } else { state.multicast_ttl = ttl; 0 }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_set_multicast_loopback(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let enabled = args.first().cloned().unwrap_or_else(JsValue::undefined).to_i32(context)?;
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed { NODE_UV_EBADF } else { state.multicast_loopback = enabled; 0 }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_set_broadcast(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let enabled = args.first().cloned().unwrap_or_else(JsValue::undefined).to_i32(context)?;
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed { NODE_UV_EBADF } else { state.broadcast = enabled; 0 }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_set_ttl(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(NODE_UV_EBADF)); };
    let ttl = args.first().cloned().unwrap_or_else(JsValue::undefined).to_i32(context)?;
    node_with_host(|host| {
        let code = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed { NODE_UV_EBADF } else { state.ttl = ttl; 0 }
        })?;
        Ok(JsValue::from(code))
    }).map_err(js_error)
}

fn node_udp_buffer_size(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::undefined()); };
    let size = args.first().cloned().unwrap_or_else(JsValue::undefined).to_u32(context)?;
    let is_recv = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_boolean();
    let Some(ctx_obj) = args.get(2).and_then(JsValue::as_object) else { return Ok(JsValue::undefined()); };
    node_with_host(|host| {
        let result = node_udp_handle_state_mut(host, &this, context, |state| {
            if state.closed {
                None
            } else {
                if is_recv {
                    state.recv_buffer_size = size;
                } else {
                    state.send_buffer_size = size;
                }
                Some(size)
            }
        })?;
        if let Some(value) = result {
            Ok(JsValue::from(value))
        } else {
            ctx_obj.set(js_string!("errno"), JsValue::from(NODE_UV_EBADF), true, context)
                .map_err(sandbox_execution_error)?;
            Ok(JsValue::undefined())
        }
    }).map_err(js_error)
}

fn node_udp_get_send_queue_size(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(0)); };
    node_with_host(|host| {
        let size = node_udp_handle_state(host, &this, context, |state| state.send_queue_size as u32)?;
        Ok(JsValue::from(size))
    }).map_err(js_error)
}

fn node_udp_get_send_queue_count(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else { return Ok(JsValue::from(0)); };
    node_with_host(|host| {
        let count = node_udp_handle_state(host, &this, context, |state| state.send_queue_count as u32)?;
        Ok(JsValue::from(count))
    }).map_err(js_error)
}

fn node_internal_binding_udp_wrap(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let udp = node_udp_handle_constructor(context)?;
    let send_wrap = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_udp_handle_req_construct),
    )
    .name(js_string!("SendWrap"))
    .length(0)
    .constructor(true)
    .build();
    let constants = ObjectInitializer::new(context)
        .property(js_string!("UV_UDP_IPV6ONLY"), JsValue::from(1), Attribute::all())
        .property(js_string!("UV_UDP_REUSEADDR"), JsValue::from(4), Attribute::all())
        .property(js_string!("UV_UDP_REUSEPORT"), JsValue::from(2), Attribute::all())
        .build();
    for (name, value) in [
        ("UDP", JsValue::from(udp)),
        ("SendWrap", JsValue::from(send_wrap)),
        ("constants", JsValue::from(constants)),
    ] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_process_handle_id(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<u64, SandboxError> {
    let symbol = node_host_private_symbol(host, "process_handle.id")?;
    object
        .get(symbol, context)
        .map_err(sandbox_execution_error)?
        .as_number()
        .map(|value| value as u64)
        .ok_or_else(|| sandbox_execution_error("process handle is missing host id"))
}

fn node_process_handle_state<F, T>(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
    f: F,
) -> Result<T, SandboxError>
where
    F: FnOnce(&NodeProcessHandleState) -> T,
{
    let id = node_process_handle_id(host, object, context)?;
    let bootstrap = host.bootstrap.borrow();
    let state = bootstrap
        .process_handles
        .get(&id)
        .ok_or_else(|| sandbox_execution_error("process handle state is missing"))?;
    Ok(f(state))
}

fn node_process_handle_state_mut<F, T>(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
    f: F,
) -> Result<T, SandboxError>
where
    F: FnOnce(&mut NodeProcessHandleState) -> T,
{
    let id = node_process_handle_id(host, object, context)?;
    let mut bootstrap = host.bootstrap.borrow_mut();
    let state = bootstrap
        .process_handles
        .get_mut(&id)
        .ok_or_else(|| sandbox_execution_error("process handle state is missing"))?;
    Ok(f(state))
}

fn node_process_schedule_onexit(
    handle: JsObject,
    exit_status: i32,
    term_signal: Option<String>,
    context: &mut Context,
) {
    context.enqueue_job(
        NativeAsyncJob::new(async move |context_cell| {
            let context = &mut context_cell.borrow_mut();
            let onexit = handle.get(js_string!("onexit"), context)?;
            let Some(onexit) = onexit.as_callable() else {
                return Ok(JsValue::undefined());
            };
            let signal = term_signal.unwrap_or_default();
            onexit.call(
                &JsValue::from(handle),
                &[JsValue::from(exit_status), JsValue::from(JsString::from(signal))],
                context,
            )?;
            Ok(JsValue::undefined())
        })
        .into(),
    );
}

fn node_process_wrap_constructor(context: &mut Context) -> Result<JsFunction, SandboxError> {
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_process_wrap_construct),
    )
    .name(js_string!("Process"))
    .length(0)
    .constructor(true)
    .build();
    let prototype = JsObject::with_null_proto();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype.clone()), false, context)
        .map_err(sandbox_execution_error)?;
    prototype
        .set(js_string!("constructor"), JsValue::from(constructor.clone()), true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("spawn", 1usize, node_process_wrap_spawn as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("kill", 1usize, node_process_wrap_kill),
        ("close", 0usize, node_process_wrap_close),
        ("ref", 0usize, node_process_wrap_ref),
        ("unref", 0usize, node_process_wrap_unref),
    ] {
        prototype
            .set(
                JsString::from(name),
                JsValue::from(
                    FunctionObjectBuilder::new(
                        context.realm(),
                        NativeFunction::from_fn_ptr(function),
                    )
                    .name(JsString::from(name))
                    .length(length)
                    .constructor(false)
                    .build(),
                ),
                true,
                context,
            )
            .map_err(sandbox_execution_error)?;
    }
    Ok(constructor)
}

fn node_process_wrap_construct(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("Process constructor requires receiver").into());
    };
    node_with_host_js(context, |host, context| {
        let id = node_next_host_handle_id(host);
        let symbol = node_host_private_symbol(host, "process_handle.id")?;
        this.set(symbol, JsValue::from(id as f64), true, context)
            .map_err(sandbox_execution_error)?;
        this.set(js_string!("pid"), JsValue::from(0), true, context)
            .map_err(sandbox_execution_error)?;
        this.set(js_string!("onexit"), JsValue::null(), true, context)
            .map_err(sandbox_execution_error)?;
        host.bootstrap.borrow_mut().process_handles.insert(
            id,
            NodeProcessHandleState {
                object: Some(this.clone()),
                pid: None,
                closed: false,
                refed: true,
                exit_status: None,
                term_signal: None,
                result: None,
            },
        );
        Ok(JsValue::from(this.clone()))
    })
}

fn node_process_wrap_spawn(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let options = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("options must be an object"))?;
    node_with_host_js(context, |host, context| {
        let file = options
            .get(js_string!("file"), context)
            .map_err(sandbox_execution_error)?
            .to_string(context)
            .map_err(sandbox_execution_error)?
            .to_std_string_escaped();
        let args_value = options
            .get(js_string!("args"), context)
            .map_err(sandbox_execution_error)?;
        let mut argv = Vec::new();
        if let Some(args_array) = args_value.as_object() {
            let len = args_array
                .get(js_string!("length"), context)
                .map_err(sandbox_execution_error)?
                .to_length(context)
                .map_err(sandbox_execution_error)?;
            for index in 0..len {
                argv.push(
                    args_array
                        .get(index, context)
                        .map_err(sandbox_execution_error)?
                        .to_string(context)
                        .map_err(sandbox_execution_error)?
                        .to_std_string_escaped(),
                );
            }
        }
        let cwd = options
            .get(js_string!("cwd"), context)
            .map_err(sandbox_execution_error)
            .ok()
            .filter(|value| !value.is_null() && !value.is_undefined())
            .map(|value| {
                value
                    .to_string(context)
                    .map_err(sandbox_execution_error)
                    .map(|value| value.to_std_string_escaped())
            })
            .transpose()?;
        let mut env = BTreeMap::new();
        if let Some(env_pairs) = options
            .get(js_string!("envPairs"), context)
            .map_err(sandbox_execution_error)?
            .as_object()
        {
            let len = env_pairs
                .get(js_string!("length"), context)
                .map_err(sandbox_execution_error)?
                .to_length(context)
                .map_err(sandbox_execution_error)?;
            for index in 0..len {
                let pair = env_pairs
                    .get(index, context)
                    .map_err(sandbox_execution_error)?
                    .to_string(context)
                    .map_err(sandbox_execution_error)?
                    .to_std_string_escaped();
                if let Some((key, value)) = pair.split_once('=') {
                    env.insert(key.to_string(), value.to_string());
                }
            }
        }
        let request = NodeChildProcessRequest {
            command: file.clone(),
            args: argv,
            cwd,
            env,
            shell: false,
            input: None,
        };
        let result = execute_child_process_request(host, request)?;
        let pid = result.pid.unwrap_or(0);
        let exit_status = result.status.unwrap_or(0);
        let term_signal = result.signal.clone();
        let errno = if let Some(error) = result.error.as_ref() {
            match error.code.as_str().unwrap_or_default() {
                "ENOENT" => -2,
                "EACCES" => -13,
                _ => NODE_UV_EINVAL,
            }
        } else {
            0
        };
        node_process_handle_state_mut(host, &this, context, |state| {
            state.pid = result.pid;
            state.exit_status = result.status;
            state.term_signal = result.signal.clone();
            state.result = Some(result.clone());
        })?;
        this.set(js_string!("pid"), JsValue::from(pid), true, context)
            .map_err(sandbox_execution_error)?;
        if errno == 0 {
            node_process_schedule_onexit(this.clone(), exit_status, term_signal, context);
        }
        Ok(JsValue::from(errno))
    })
}

fn node_process_wrap_kill(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::from(NODE_UV_EBADF));
    };
    let signal = args.first().cloned().unwrap_or_else(JsValue::undefined).to_i32(context)?;
    node_with_host(|host| {
        let (closed, pid) = node_process_handle_state(host, &this, context, |state| (state.closed, state.pid))?;
        if closed || pid.is_none() {
            return Ok(JsValue::from(-3));
        }
        node_process_handle_state_mut(host, &this, context, |state| {
            state.exit_status = None;
            state.term_signal = Some(signal.to_string());
        })?;
        node_process_schedule_onexit(this.clone(), 0, Some(signal.to_string()), context);
        Ok(JsValue::from(0))
    })
    .map_err(js_error)
}

fn node_process_wrap_close(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    node_with_host(|host| {
        node_process_handle_state_mut(host, &this, context, |state| state.closed = true)?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_wrap_ref(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    node_with_host(|host| {
        node_process_handle_state_mut(host, &this, context, |state| state.refed = true)?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_wrap_unref(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    node_with_host(|host| {
        node_process_handle_state_mut(host, &this, context, |state| state.refed = false)?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_internal_binding_process_wrap(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let process = node_process_wrap_constructor(context)?;
    object
        .set(js_string!("Process"), JsValue::from(process), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_spawn_sync(context: &mut Context) -> Result<JsObject, SandboxError> {
    let spawn = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_spawn_sync_spawn),
    )
    .name(js_string!("spawn"))
    .length(1)
    .constructor(false)
    .build();
    Ok(ObjectInitializer::new(context)
        .property(js_string!("spawn"), JsValue::from(spawn), Attribute::all())
        .build())
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
    for (name, length, function) in [
        ("open", 4usize, node_internal_fs_open as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("close", 2usize, node_internal_fs_close),
        ("read", 6usize, node_internal_fs_read),
        ("readFileUtf8", 2usize, node_internal_fs_read_file_utf8),
        ("writeBuffer", 7usize, node_internal_fs_write_buffer),
        ("writeString", 6usize, node_internal_fs_write_string),
        ("getFormatOfExtensionlessFile", 1usize, node_internal_fs_get_format_of_extensionless_file),
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

fn node_internal_binding_fs_dir(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let opendir = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_fs_dir_opendir),
    )
    .name(js_string!("opendir"))
    .length(3)
    .constructor(false)
    .build();
    object
        .set(js_string!("opendir"), JsValue::from(opendir), true, context)
        .map_err(sandbox_execution_error)?;
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

fn node_fs_dir_kind_constant(kind: FileKind) -> u32 {
    match kind {
        FileKind::File => 1,
        FileKind::Directory => 2,
        FileKind::Symlink => 3,
    }
}

fn node_fs_dir_make_handle(
    host: &NodeRuntimeHost,
    context: &mut Context,
    path: &str,
    entries: &[terracedb_vfs::DirEntry],
) -> Result<JsObject, SandboxError> {
    let handle = JsObject::with_null_proto();
    let path_symbol = node_host_private_symbol(host, "fs_dir.path")?;
    let names_symbol = node_host_private_symbol(host, "fs_dir.names")?;
    let types_symbol = node_host_private_symbol(host, "fs_dir.types")?;
    let index_symbol = node_host_private_symbol(host, "fs_dir.index")?;
    let closed_symbol = node_host_private_symbol(host, "fs_dir.closed")?;
    let names = entries
        .iter()
        .map(|entry| JsValue::from(JsString::from(entry.name.clone())));
    let types = entries
        .iter()
        .map(|entry| JsValue::from(node_fs_dir_kind_constant(entry.kind)));
    handle
        .set(
            path_symbol,
            JsValue::from(JsString::from(path)),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    handle
        .set(
            names_symbol,
            JsValue::from(JsArray::from_iter(names, context)),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    handle
        .set(
            types_symbol,
            JsValue::from(JsArray::from_iter(types, context)),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    handle
        .set(index_symbol, JsValue::from(0), true, context)
        .map_err(sandbox_execution_error)?;
    handle
        .set(closed_symbol, JsValue::from(false), true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("read", 3usize, node_fs_dir_handle_read as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("close", 1usize, node_fs_dir_handle_close),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
        handle
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(handle)
}

fn node_fs_dir_opendir(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let req = args.get(2).cloned();
    node_with_host_js(context, |host, context| {
        let resolved = resolve_node_path(&host.process.borrow().cwd, &path);
        let entries = node_read_readdir(host, &resolved)?;
        let handle = node_fs_dir_make_handle(host, context, &resolved, &entries)?;
        node_fs_req_complete(req.as_ref(), context, JsValue::from(handle))
    })
}

fn node_fs_dir_handle_read(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let closed_symbol = node_host_private_symbol(&host, "fs_dir.closed").map_err(js_error)?;
    let index_symbol = node_host_private_symbol(&host, "fs_dir.index").map_err(js_error)?;
    let names_symbol = node_host_private_symbol(&host, "fs_dir.names").map_err(js_error)?;
    let types_symbol = node_host_private_symbol(&host, "fs_dir.types").map_err(js_error)?;
    let handle = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("DirHandle.read receiver must be an object"))?;
    if handle.get(closed_symbol, context)?.to_boolean() {
        return Ok(JsValue::null());
    }
    let index = handle
        .get(index_symbol.clone(), context)?
        .to_u32(context)? as usize;
    let names = handle
        .get(names_symbol, context)?
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("DirHandle names missing"))?;
    let types = handle
        .get(types_symbol, context)?
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("DirHandle types missing"))?;
    let names_array = JsArray::from_object(names.clone())
        .map_err(|_| JsNativeError::typ().with_message("DirHandle names must be an array"))?;
    let len = names_array.length(context)? as usize;
    if index >= len {
        let req = args.get(2).cloned();
        return node_fs_req_complete(req.as_ref(), context, JsValue::null()).map_err(js_error);
    }
    let buffer_size = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)
        .unwrap_or(32) as usize;
    let end = (index + buffer_size.max(1)).min(len);
    let mut values = Vec::with_capacity((end - index) * 2);
    for i in index..end {
        values.push(names.get(i as u32, context)?);
        values.push(types.get(i as u32, context)?);
    }
    let result = JsValue::from(JsArray::from_iter(values, context));
    handle.set(index_symbol, JsValue::from(end as u32), true, context)?;
    let req = args.get(2).cloned();
    node_fs_req_complete(req.as_ref(), context, result).map_err(js_error)
}

fn node_fs_dir_handle_close(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let closed_symbol = node_host_private_symbol(&host, "fs_dir.closed").map_err(js_error)?;
    let handle = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("DirHandle.close receiver must be an object"))?;
    handle.set(closed_symbol, JsValue::from(true), true, context)?;
    let req = args.first().cloned();
    node_fs_req_complete(req.as_ref(), context, JsValue::undefined()).map_err(js_error)
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

fn node_internal_fs_open(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let flags = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .map(node_fs_open_flags_from_numeric)?;
    let req = args.get(3);
    node_with_host(|host| node_fs_open_impl(host, &path, flags).map(JsValue::from))
        .and_then(|result| node_fs_req_complete(req, context, result))
        .map_err(js_error)
}

fn node_internal_fs_close(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    let req = args.get(1);
    node_with_host(|host| node_fs_close_impl(host, fd).map(|_| JsValue::undefined()))
        .and_then(|result| node_fs_req_complete(req, context, result))
        .map_err(js_error)
}

fn node_internal_fs_read(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    let buffer = args
        .get(1)
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("read buffer must be an object"))?;
    let dest = JsUint8Array::from_object(buffer.clone()).map_err(|_| {
        JsNativeError::typ().with_message("read buffer must be a Uint8Array-compatible object")
    })?;
    let offset = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    let length = args
        .get(3)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    let position = if args
        .get(4)
        .is_some_and(|value| value.is_null() || value.is_undefined())
    {
        None
    } else {
        Some(
            args.get(4)
                .cloned()
                .unwrap_or_else(JsValue::undefined)
                .to_length(context)? as usize,
        )
    };
    let req = args.get(5);
    node_with_host_js(context, |host, context| {
        let bytes = node_fs_read_impl(host, fd, length, position)?;
        let mut written = 0usize;
        for (index, byte) in bytes.into_iter().enumerate() {
            dest.set((offset + index) as u32, byte, true, context)
                .map_err(sandbox_execution_error)?;
            written = written.saturating_add(1);
        }
        node_fs_req_complete(req, context, JsValue::from(written as u32))
    })
}

fn node_internal_fs_write_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    let buffer = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    let bytes = node_bytes_from_js(&buffer, context)?;
    let offset = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    let length = args
        .get(3)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    let position = if args
        .get(4)
        .is_some_and(|value| value.is_null() || value.is_undefined())
    {
        None
    } else {
        Some(
            args.get(4)
                .cloned()
                .unwrap_or_else(JsValue::undefined)
                .to_length(context)? as usize,
        )
    };
    let req = args.get(5);
    let ctx = args.get(6);
    let end = offset.saturating_add(length).min(bytes.len());
    let slice = if offset >= bytes.len() {
        Vec::new()
    } else {
        bytes[offset..end].to_vec()
    };
    let result = node_with_host(|host| {
        node_fs_write_impl(host, fd, &slice, position).map(|written| JsValue::from(written as u32))
    })
    .map_err(js_error)?;
    if let Some(ctx_object) = ctx.and_then(JsValue::as_object) {
        ctx_object
            .set(js_string!("errno"), JsValue::undefined(), true, context)
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
    }
    node_fs_req_complete(req, context, result).map_err(js_error)

}

fn node_internal_fs_write_string(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    let data = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    let position = if args
        .get(2)
        .is_some_and(|value| value.is_null() || value.is_undefined())
    {
        None
    } else {
        Some(
            args.get(2)
                .cloned()
                .unwrap_or_else(JsValue::undefined)
                .to_length(context)? as usize,
        )
    };
    let req = args.get(4);
    let ctx = args.get(5);
    let result = node_with_host(|host| {
        node_fs_write_impl(host, fd, data.as_bytes(), position).map(|written| JsValue::from(written as u32))
    })
    .map_err(js_error)?;
    if let Some(ctx_object) = ctx.and_then(JsValue::as_object) {
        ctx_object
            .set(js_string!("errno"), JsValue::undefined(), true, context)
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
    }
    node_fs_req_complete(req, context, result).map_err(js_error)
}

fn node_internal_fs_get_format_of_extensionless_file(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(0))
}

fn node_internal_fs_read_file_utf8(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let _flags = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    node_with_host(|host| {
        let resolved = resolve_node_path(&host.process.borrow().cwd, &path);
        let bytes = node_read_file(host, &resolved)?.ok_or_else(|| SandboxError::ModuleNotFound {
            specifier: resolved.clone(),
        })?;
        let text = String::from_utf8(bytes).map_err(|error| SandboxError::Execution {
            entrypoint: resolved,
            message: error.to_string(),
        })?;
        Ok(JsValue::from(JsString::from(text)))
    })
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
    for (name, length, function) in [
        ("createBlob", 2usize, node_blob_create_blob as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("createBlobFromFilePath", 1usize, node_blob_create_blob_from_file_path),
        ("concat", 1usize, node_blob_concat),
        ("getDataObject", 1usize, node_blob_get_data_object),
        ("storeDataObject", 4usize, node_blob_store_data_object),
        ("revokeObjectURL", 1usize, node_blob_revoke_object_url),
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
        ("setBufferPrototype", 1usize, node_buffer_set_buffer_prototype),
        ("byteLengthUtf8", 1usize, node_buffer_byte_length_utf8),
        ("compare", 2usize, node_buffer_compare),
        ("compareOffset", 6usize, node_buffer_compare_offset),
        ("copy", 5usize, node_buffer_copy),
        ("copyArrayBuffer", 5usize, node_buffer_copy_array_buffer),
        ("fill", 5usize, node_buffer_fill),
        ("isAscii", 1usize, node_buffer_is_ascii),
        ("isUtf8", 1usize, node_buffer_is_utf8),
        ("indexOfBuffer", 5usize, node_buffer_index_of_buffer),
        ("indexOfNumber", 4usize, node_buffer_index_of_number),
        ("indexOfString", 5usize, node_buffer_index_of_string),
        ("swap16", 1usize, node_buffer_swap16),
        ("swap32", 1usize, node_buffer_swap32),
        ("swap64", 1usize, node_buffer_swap64),
        ("atob", 1usize, node_buffer_atob),
        ("btoa", 1usize, node_buffer_btoa),
        ("asciiSlice", 3usize, node_buffer_ascii_slice),
        ("base64Slice", 3usize, node_buffer_base64_slice),
        ("base64urlSlice", 3usize, node_buffer_base64url_slice),
        ("latin1Slice", 3usize, node_buffer_latin1_slice),
        ("hexSlice", 3usize, node_buffer_hex_slice),
        ("ucs2Slice", 3usize, node_buffer_ucs2_slice),
        ("utf8Slice", 3usize, node_buffer_utf8_slice),
        ("asciiWriteStatic", 4usize, node_buffer_ascii_write_static),
        ("base64Write", 4usize, node_buffer_base64_write),
        ("base64urlWrite", 4usize, node_buffer_base64url_write),
        ("latin1WriteStatic", 4usize, node_buffer_latin1_write_static),
        ("hexWrite", 4usize, node_buffer_hex_write),
        ("ucs2Write", 4usize, node_buffer_ucs2_write),
        ("utf8WriteStatic", 4usize, node_buffer_utf8_write_static),
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
    let url_components = JsValue::from_json(&serde_json::json!([0, 0, 0, 0, 0, 0, 0, 0, 1]), context)
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("urlComponents"), url_components, true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("canParse", 2usize, node_url_can_parse as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("parse", 3usize, node_url_parse),
        ("pathToFileURL", 3usize, node_url_path_to_file_url),
        ("update", 3usize, node_url_update),
        ("domainToASCII", 1usize, node_url_domain_to_ascii),
        ("domainToUnicode", 1usize, node_url_domain_to_unicode),
        ("getOrigin", 1usize, node_url_get_origin),
        ("format", 5usize, node_url_format),
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
    for (name, getter) in [
        ("protocol", node_url_pattern_get_protocol as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("username", node_url_pattern_get_username),
        ("password", node_url_pattern_get_password),
        ("hostname", node_url_pattern_get_hostname),
        ("port", node_url_pattern_get_port),
        ("pathname", node_url_pattern_get_pathname),
        ("search", node_url_pattern_get_search),
        ("hash", node_url_pattern_get_hash),
        ("hasRegExpGroups", node_url_pattern_get_has_regexp_groups),
    ] {
        let function = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(getter),
        )
        .name(JsString::from(name))
        .length(0)
        .constructor(false)
        .build();
        prototype
            .set(JsString::from(name), JsValue::from(function), true, context)
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
    object
        .set(
            js_string!("compileCacheStatus"),
            JsValue::from(JsArray::from_iter(
                [
                    JsValue::from(JsString::from("FAILED")),
                    JsValue::from(JsString::from("ENABLED")),
                    JsValue::from(JsString::from("ALREADY_ENABLED")),
                    JsValue::from(JsString::from("DISABLED")),
                ],
                context,
            )),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("cachedCodeTypes"),
            JsValue::from_json(
                &serde_json::json!({
                    "kStrippedTypeScript": 0,
                    "kTransformedTypeScript": 1,
                    "kTransformedTypeScriptWithSourceMaps": 2,
                }),
                context,
            )
            .map_err(sandbox_execution_error)?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("enableCompileCache", 0usize, node_modules_enable_compile_cache as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("getCompileCacheDir", 0usize, node_modules_get_compile_cache_dir),
        ("getNearestParentPackageJSONType", 1usize, node_modules_get_nearest_parent_package_json_type),
        ("getNearestParentPackageJSON", 1usize, node_modules_get_nearest_parent_package_json),
        ("getPackageScopeConfig", 1usize, node_modules_get_package_scope_config),
        ("getPackageType", 1usize, node_modules_get_package_type),
        ("readPackageJSON", 4usize, node_modules_read_package_json),
        ("flushCompileCache", 0usize, node_modules_flush_compile_cache),
        ("getCompileCacheEntry", 3usize, node_modules_get_compile_cache_entry),
        ("saveCompileCacheEntry", 2usize, node_modules_save_compile_cache_entry),
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
    let has_intl = context
        .global_object()
        .has_property(js_string!("Intl"), context)
        .map_err(sandbox_execution_error)?;
    for (name, value) in [
        ("bits", JsValue::from(64)),
        ("hasIntl", JsValue::from(has_intl)),
        ("hasInspector", JsValue::from(false)),
        ("hasNodeOptions", JsValue::from(true)),
        ("hasOpenSSL", JsValue::from(true)),
        ("hasSmallICU", JsValue::from(false)),
        ("hasTracing", JsValue::from(true)),
        ("noBrowserGlobals", JsValue::from(false)),
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
        ("isArgumentsObject", node_type_is_arguments_object),
        ("isArrayBuffer", node_type_is_array_buffer),
        ("isAsyncFunction", node_type_is_async_function),
        ("isBigIntObject", node_type_is_bigint_object),
        ("isBooleanObject", node_type_is_boolean_object),
        ("isBoxedPrimitive", node_type_is_boxed_primitive),
        ("isDataView", node_type_is_data_view),
        ("isDate", node_type_is_date),
        ("isExternal", node_type_is_external),
        ("isGeneratorFunction", node_type_is_generator_function),
        ("isGeneratorObject", node_type_is_generator_object),
        ("isMap", node_type_is_map),
        ("isMapIterator", node_type_is_map_iterator),
        ("isModuleNamespaceObject", node_type_is_module_namespace_object),
        ("isNativeError", node_type_is_native_error),
        ("isNumberObject", node_type_is_number_object),
        ("isPromise", node_type_is_promise),
        ("isProxy", node_type_is_proxy),
        ("isRegExp", node_type_is_regexp),
        ("isSet", node_type_is_set),
        ("isSetIterator", node_type_is_set_iterator),
        ("isSharedArrayBuffer", node_type_is_shared_array_buffer),
        ("isStringObject", node_type_is_string_object),
        ("isSymbolObject", node_type_is_symbol_object),
        ("isWeakMap", node_type_is_weak_map),
        ("isWeakSet", node_type_is_weak_set),
        ("isAnyArrayBuffer", node_type_is_any_array_buffer),
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
        ("link", node_module_wrap_link as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("getModuleRequests", node_module_wrap_get_module_requests),
        ("instantiate", node_module_wrap_instantiate as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("evaluateSync", node_module_wrap_evaluate_sync),
        ("evaluate", node_module_wrap_evaluate),
        ("setExport", node_module_wrap_set_export),
        ("setModuleSourceObject", node_module_wrap_set_module_source_object),
        ("getModuleSourceObject", node_module_wrap_get_module_source_object),
        ("createCachedData", node_module_wrap_create_cached_data),
        ("getNamespace", node_module_wrap_get_namespace),
        ("getStatus", node_module_wrap_get_status),
        ("getError", node_module_wrap_get_error),
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
    let has_async_graph_getter = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_module_wrap_has_async_graph),
    )
    .name(js_string!("hasAsyncGraph"))
    .length(0)
    .constructor(false)
    .build();
    prototype
        .define_property_or_throw(
            js_string!("hasAsyncGraph"),
            PropertyDescriptor::builder()
                .get(has_async_graph_getter)
                .enumerable(false)
                .configurable(true),
            context,
        )
        .map_err(sandbox_execution_error)?;
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
    for (name, value) in [
        ("kUninstantiated", 0i32),
        ("kInstantiating", 1),
        ("kInstantiated", 2),
        ("kEvaluating", 3),
        ("kEvaluated", 4),
        ("kErrored", 5),
        ("kSourcePhase", 0),
        ("kEvaluationPhase", 1),
    ] {
        binding
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    for (name, function) in [
        ("setImportModuleDynamicallyCallback", node_module_wrap_set_import_module_dynamically_callback as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("setInitializeImportMetaObjectCallback", node_module_wrap_set_initialize_import_meta_object_callback),
        ("createRequiredModuleFacade", node_module_wrap_create_required_module_facade),
        ("throwIfPromiseRejected", node_module_wrap_throw_if_promise_rejected),
    ] {
        let value = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(function),
        )
        .name(JsString::from(name))
        .length(1)
        .constructor(false)
        .build();
        binding
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(binding)
}

fn node_internal_binding_errors(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let exit_codes = JsValue::from_json(
        &serde_json::json!({
            "kNoFailure": 0,
            "kGenericUserError": 1,
        }),
        context,
    )
    .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("exitCodes"), exit_codes, true, context)
        .map_err(sandbox_execution_error)?;
    for (name, function) in [
        ("setEnhanceStackForFatalException", node_errors_set_enhance_stack_for_fatal_exception as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("setPrepareStackTraceCallback", node_errors_set_prepare_stack_trace_callback),
        ("setSourceMapsEnabled", node_errors_set_source_maps_enabled),
        ("setMaybeCacheGeneratedSourceMap", node_errors_set_maybe_cache_generated_source_map),
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
            "ONLY_WRITABLE": 1,
            "ONLY_ENUMERABLE": 2,
            "ONLY_CONFIGURABLE": 4,
            "SKIP_STRINGS": 8,
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
        ("contextify_context_private_symbol", "node:contextify:context"),
        ("decorated_private_symbol", "node.decorated_private_symbol"),
        ("exit_info_private_symbol", "node.exit_info_private_symbol"),
        ("host_defined_option_symbol", "node.host_defined_option_symbol"),
        ("transfer_mode_private_symbol", "node:transfer_mode"),
        ("entry_point_module_private_symbol", "node:entry_point_module"),
        ("entry_point_promise_private_symbol", "node:entry_point_promise"),
        ("module_source_private_symbol", "node:module_source"),
        ("module_export_names_private_symbol", "node:module_export_names"),
        ("module_circular_visited_private_symbol", "node:module_circular_visited"),
        ("module_export_private_symbol", "node:module_export"),
        ("module_first_parent_private_symbol", "node:module_first_parent"),
        ("module_last_parent_private_symbol", "node:module_last_parent"),
        ("untransferable_object_private_symbol", "node:untransferableObject"),
        ("source_map_data_private_symbol", "node:source_map_data_private_symbol"),
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
    let should_abort_toggle = {
        let host = active_node_host()?;
        if let Some(existing) = host
            .bootstrap
            .borrow()
            .should_abort_on_uncaught_toggle
            .clone()
        {
            existing
        } else {
            let created = context
                .eval(Source::from_bytes(b"new Uint8Array([0])"))
                .map_err(sandbox_execution_error)?
                .as_object()
                .ok_or_else(|| SandboxError::Execution {
                    entrypoint: "<node-runtime>".to_string(),
                    message: "failed to create shouldAbortOnUncaughtToggle".to_string(),
                })?;
            host.bootstrap.borrow_mut().should_abort_on_uncaught_toggle = Some(created.clone());
            created
        }
    };
    object
        .set(
            js_string!("shouldAbortOnUncaughtToggle"),
            JsValue::from(should_abort_toggle),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("constructSharedArrayBuffer", 1usize, node_util_construct_shared_array_buffer as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("guessHandleType", 1usize, node_util_guess_handle_type),
        ("defineLazyProperties", 4usize, node_util_define_lazy_properties),
        ("getOwnNonIndexProperties", 2usize, node_util_get_own_non_index_properties),
        ("isInsideNodeModules", 1usize, node_util_is_inside_node_modules),
        ("arrayBufferViewHasBuffer", 1usize, node_util_array_buffer_view_has_buffer),
        ("previewEntries", 2usize, node_util_preview_entries),
        ("getCallSites", 1usize, node_util_get_call_sites),
        ("getCallerLocation", 0usize, node_util_get_caller_location),
        ("getPromiseDetails", 1usize, node_util_get_promise_details),
        ("getProxyDetails", 2usize, node_util_get_proxy_details),
        ("getConstructorName", 1usize, node_util_get_constructor_name),
        ("getExternalValue", 1usize, node_util_get_external_value),
        ("sleep", 1usize, node_util_sleep),
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

fn node_internal_binding_cares_wrap(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, length, function) in [
        ("getaddrinfo", 5usize, node_cares_getaddrinfo as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("getnameinfo", 3usize, node_cares_getnameinfo),
        ("canonicalizeIP", 1usize, node_cares_canonicalize_ip),
        ("convertIpv6StringToBuffer", 1usize, node_cares_convert_ipv6_string_to_buffer),
        ("strerror", 1usize, node_cares_strerror),
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
    for (name, constructor) in [
        ("GetAddrInfoReqWrap", node_cares_req_wrap_construct as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("GetNameInfoReqWrap", node_cares_req_wrap_construct),
        ("QueryReqWrap", node_cares_req_wrap_construct),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(constructor))
            .name(JsString::from(name))
            .length(0)
            .constructor(true)
            .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    let af_inet6 = if cfg!(target_os = "macos") || cfg!(target_os = "ios") { 30 } else { 10 };
    for (name, value) in [
        ("AF_INET", 2),
        ("AF_INET6", af_inet6),
        ("AF_UNSPEC", 0),
        ("AI_ADDRCONFIG", 0x0400),
        ("AI_ALL", 0x0100),
        ("AI_V4MAPPED", 0x0800),
        ("DNS_ORDER_VERBATIM", 0),
        ("DNS_ORDER_IPV4_FIRST", 1),
        ("DNS_ORDER_IPV6_FIRST", 2),
    ] {
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_cares_req_wrap_construct(
    this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(this.clone())
}

fn node_cares_canonicalize_ip(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = node_arg_string(args, 0, context)?;
    Ok(match input.parse::<std::net::IpAddr>() {
        Ok(address) => JsValue::from(JsString::from(address.to_string())),
        Err(_) => JsValue::undefined(),
    })
}

fn node_cares_convert_ipv6_string_to_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = node_arg_string(args, 0, context)?;
    let address = input
        .parse::<std::net::Ipv6Addr>()
        .map_err(|_| JsNativeError::typ().with_message("invalid IPv6 address"))?;
    Ok(JsValue::from(JsUint8Array::from_iter(
        address.octets(),
        context,
    )?))
}

fn node_cares_strerror(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let code = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    Ok(JsValue::from(JsString::from(format!("DNS error {code}"))))
}

fn node_cares_getaddrinfo(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let req = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("request must be an object"))?;
    let hostname = node_arg_string(args, 1, context)?;
    let family = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    let addresses = if let Ok(ip) = hostname.parse::<std::net::IpAddr>() {
        vec![ip.to_string()]
    } else {
        let iter = (hostname.as_str(), 0)
            .to_socket_addrs()
            .map_err(|error| JsNativeError::typ().with_message(error.to_string()))?;
        let mut resolved = Vec::new();
        for address in iter {
            let ip = address.ip();
            if family == 4 && !ip.is_ipv4() {
                continue;
            }
            if family == 6 && !ip.is_ipv6() {
                continue;
            }
            let ip = ip.to_string();
            if !resolved.contains(&ip) {
                resolved.push(ip);
            }
        }
        resolved
    };
    if let Some(oncomplete) = req.get(js_string!("oncomplete"), context)?.as_callable() {
        let values = addresses
            .into_iter()
            .map(|value| JsValue::from(JsString::from(value)));
        let result = JsArray::from_iter(values, context);
        let _ = oncomplete.call(&JsValue::from(req.clone()), &[JsValue::from(0), JsValue::from(result)], context);
    }
    Ok(JsValue::from(0))
}

fn node_cares_getnameinfo(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let req = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("request must be an object"))?;
    let hostname = node_arg_string(args, 1, context)?;
    let port = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)
        .unwrap_or_default();
    if let Some(oncomplete) = req.get(js_string!("oncomplete"), context)?.as_callable() {
        let _ = oncomplete.call(
            &JsValue::from(req.clone()),
            &[
                JsValue::from(0),
                JsValue::from(JsString::from(hostname)),
                JsValue::from(JsString::from(port.to_string())),
            ],
            context,
        );
    }
    Ok(JsValue::from(0))
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
        .set(
            js_string!("isBigEndian"),
            JsValue::from(cfg!(target_endian = "big")),
            true,
            context,
        )
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

fn node_internal_binding_process_methods(
    host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    object
        .set(
            js_string!("hrtimeBuffer"),
            JsValue::from(node_process_methods_hrtime_buffer(host, context)?),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("patchProcessObject", 1usize, node_process_methods_patch_process_object as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("dlopen", 0usize, node_process_methods_dlopen),
        ("uptime", 0usize, node_process_methods_uptime),
        ("causeSegfault", 0usize, node_process_methods_cause_segfault),
        ("_getActiveRequests", 0usize, node_process_methods_get_active_requests),
        ("_getActiveHandles", 0usize, node_process_methods_get_active_handles),
        ("getActiveResourcesInfo", 0usize, node_process_methods_get_active_resources_info),
        ("reallyExit", 1usize, node_process_methods_really_exit),
        ("_kill", 2usize, node_process_methods_kill),
        ("cpuUsage", 1usize, node_process_methods_cpu_usage),
        ("threadCpuUsage", 1usize, node_process_methods_cpu_usage),
        ("memoryUsage", 1usize, node_process_methods_memory_usage),
        ("rss", 0usize, node_process_methods_rss),
        ("resourceUsage", 1usize, node_process_methods_resource_usage),
        ("loadEnvFile", 1usize, node_process_methods_load_env_file),
        ("execve", 3usize, node_process_methods_execve),
        ("constrainedMemory", 0usize, node_process_methods_constrained_memory),
        ("availableMemory", 0usize, node_process_methods_available_memory),
        ("abort", 0usize, node_process_methods_abort),
        ("cwd", 0usize, node_process_methods_cwd),
        ("chdir", 1usize, node_process_methods_chdir),
        ("umask", 1usize, node_process_methods_umask),
        ("setEmitWarningSync", 1usize, node_process_methods_set_emit_warning_sync),
        ("_rawDebug", 1usize, node_process_methods_raw_debug),
        ("_debugProcess", 1usize, node_process_methods_debug_process),
        ("_debugEnd", 0usize, node_process_methods_debug_end),
        ("hrtime", 0usize, node_process_methods_hrtime),
        ("hrtimeBigInt", 0usize, node_process_methods_hrtime_bigint),
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

fn node_internal_binding_trace_events(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let category_set = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_trace_events_category_set_construct),
    )
    .name(js_string!("CategorySet"))
    .length(1)
    .constructor(true)
    .build();
    object
        .set(js_string!("CategorySet"), JsValue::from(category_set), true, context)
        .map_err(sandbox_execution_error)?;
    let getter = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_trace_events_get_enabled_categories),
    )
    .name(js_string!("getEnabledCategories"))
    .length(0)
    .constructor(false)
    .build();
    object
        .set(
            js_string!("getEnabledCategories"),
            JsValue::from(getter),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    let setter = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_trace_events_set_category_state_update_handler),
    )
    .name(js_string!("setTraceCategoryStateUpdateHandler"))
    .length(1)
    .constructor(false)
    .build();
    object
        .set(
            js_string!("setTraceCategoryStateUpdateHandler"),
            JsValue::from(setter),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    let getter = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_trace_events_get_category_enabled_buffer),
    )
    .name(js_string!("getCategoryEnabledBuffer"))
    .length(1)
    .constructor(false)
    .build();
    object
        .set(
            js_string!("getCategoryEnabledBuffer"),
            JsValue::from(getter),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, function) in [
        ("trace", node_trace_events_trace as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("isTraceCategoryEnabled", node_trace_events_is_category_enabled),
    ] {
        let value = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(function),
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

fn node_internal_binding_task_queue(
    host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    object
        .set(
            js_string!("tickInfo"),
            JsValue::from(node_bootstrap_cached_array(host, context, "tickInfo", &serde_json::json!([0, 0]))?),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, function) in [
        ("runMicrotasks", node_task_queue_run_microtasks as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("setTickCallback", node_task_queue_set_tick_callback),
        ("enqueueMicrotask", node_task_queue_enqueue_microtask),
        ("setPromiseRejectCallback", node_task_queue_set_promise_reject_callback),
    ] {
        let value = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(function),
        )
        .name(JsString::from(name))
        .length(1)
        .constructor(false)
        .build();
        object
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    let promise_reject_events = JsValue::from_json(
        &serde_json::json!({
            "kPromiseRejectWithNoHandler": 0,
            "kPromiseHandlerAddedAfterReject": 1,
            "kPromiseResolveAfterResolved": 2,
            "kPromiseRejectAfterResolved": 3
        }),
        context,
    )
    .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("promiseRejectEvents"),
            promise_reject_events,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_timers(
    host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    object
        .set(
            js_string!("immediateInfo"),
            JsValue::from(node_bootstrap_cached_array(
                host,
                context,
                "immediateInfo",
                &serde_json::json!([0, 0, 0]),
            )?),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("timeoutInfo"),
            JsValue::from(node_bootstrap_cached_array(
                host,
                context,
                "timeoutInfo",
                &serde_json::json!([0]),
            )?),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("setupTimers", 2usize, node_timers_setup_timers as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("scheduleTimer", 1usize, node_timers_schedule_timer),
        ("toggleTimerRef", 1usize, node_timers_toggle_timer_ref),
        ("toggleImmediateRef", 1usize, node_timers_toggle_immediate_ref),
        ("getLibuvNow", 0usize, node_timers_get_libuv_now),
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

fn node_internal_binding_locks(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, value) in [
        ("LOCK_MODE_SHARED", JsValue::from(js_string!("shared"))),
        ("LOCK_MODE_EXCLUSIVE", JsValue::from(js_string!("exclusive"))),
        ("LOCK_STOLEN_ERROR", JsValue::from(js_string!("LOCK_STOLEN"))),
    ] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    for (name, length, function) in [
        ("request", 6usize, node_locks_request as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("query", 0usize, node_locks_query),
    ] {
        object
            .set(
                JsString::from(name),
                JsValue::from(
                    FunctionObjectBuilder::new(
                        context.realm(),
                        NativeFunction::from_fn_ptr(function),
                    )
                    .name(JsString::from(name))
                    .length(length)
                    .constructor(false)
                    .build(),
                ),
                true,
                context,
            )
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_locks_make_internal_lock(name: &str, mode: &str, context: &mut Context) -> Result<JsValue, SandboxError> {
    Ok(JsValue::from_json(
        &serde_json::json!({
            "name": name,
            "mode": mode,
        }),
        context,
    )
    .map_err(sandbox_execution_error)?)
}

fn node_locks_is_compatible(existing: &[NodeHeldLockState], mode: &str) -> bool {
    if mode == "exclusive" {
        return existing.is_empty();
    }
    existing.iter().all(|lock| lock.mode == "shared")
}

fn node_locks_release_request(
    host: &NodeRuntimeHost,
    request_id: u64,
    context: &mut Context,
) -> Result<(), SandboxError> {
    let resource_names = host
        .bootstrap
        .borrow()
        .held_locks
        .iter()
        .filter_map(|(name, locks)| locks.iter().any(|lock| lock.request_id == request_id).then_some(name.clone()))
        .collect::<Vec<_>>();

    {
        let mut bootstrap = host.bootstrap.borrow_mut();
        for name in resource_names {
            if let Some(locks) = bootstrap.held_locks.get_mut(&name) {
                locks.retain(|lock| lock.request_id != request_id);
                if locks.is_empty() {
                    bootstrap.held_locks.remove(&name);
                }
            }
        }
    }

    node_locks_process_pending(host, context)
}

fn node_locks_attach_release_handlers(
    host: &NodeRuntimeHost,
    request_id: u64,
    result: JsValue,
    resolve: JsObject,
    reject: JsObject,
    context: &mut Context,
) -> Result<(), SandboxError> {
    let promise = JsPromise::resolve(result, context).map_err(sandbox_execution_error)?;
    let on_fulfilled = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_copy_closure_with_captures(
            |_this, args, captures, context| {
                let (request_id, resolve, reject) = captures;
                node_with_host_js(context, |host, context| {
                    node_locks_release_request(host, *request_id, context)?;
                    resolve
                        .call(
                            &JsValue::undefined(),
                            &[args.first().cloned().unwrap_or_else(JsValue::undefined)],
                            context,
                        )
                        .map_err(sandbox_execution_error)?;
                    let _ = reject;
                    Ok(JsValue::undefined())
                })
            },
            (request_id, resolve.clone(), reject.clone()),
        ),
    )
    .name(js_string!("lockRequestFulfilled"))
    .length(1)
    .constructor(false)
    .build();
    let on_rejected = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_copy_closure_with_captures(
            |_this, args, captures, context| {
                let (request_id, _resolve, reject) = captures;
                let reason = args.first().cloned().unwrap_or_else(JsValue::undefined);
                node_with_host_js(context, |host, context| {
                    node_locks_release_request(host, *request_id, context)?;
                    reject
                        .call(&JsValue::undefined(), &[reason.clone()], context)
                        .map_err(sandbox_execution_error)?;
                    Ok(JsValue::undefined())
                })?;
                Err(boa_engine::JsError::from_opaque(reason))
            },
            (request_id, resolve, reject),
        ),
    )
    .name(js_string!("lockRequestRejected"))
    .length(1)
    .constructor(false)
    .build();
    let _ = promise
        .then(Some(on_fulfilled), Some(on_rejected), context)
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_locks_grant_request(
    host: &NodeRuntimeHost,
    request_id: u64,
    name: String,
    client_id: String,
    mode: String,
    callback: JsObject,
    resolve: JsObject,
    reject: JsObject,
    context: &mut Context,
) -> Result<(), SandboxError> {
    {
        let mut bootstrap = host.bootstrap.borrow_mut();
        bootstrap
            .held_locks
            .entry(name.clone())
            .or_default()
            .push(NodeHeldLockState {
                request_id,
                name: name.clone(),
                client_id,
                mode: mode.clone(),
                resolve: resolve.clone(),
                reject: reject.clone(),
            });
    }
    let callback = JsValue::from(callback)
        .as_callable()
        .ok_or_else(|| sandbox_execution_error("locks.request callback must be callable"))?;
    let result = callback
        .call(
            &JsValue::undefined(),
            &[node_locks_make_internal_lock(&name, &mode, context)?],
            context,
        )
        .map_err(sandbox_execution_error)?;
    node_locks_attach_release_handlers(host, request_id, result, resolve, reject, context)
}

fn node_locks_process_pending(host: &NodeRuntimeHost, context: &mut Context) -> Result<(), SandboxError> {
    loop {
        let grantable_index = {
            let bootstrap = host.bootstrap.borrow();
            bootstrap.pending_locks.iter().position(|pending| {
                let held = bootstrap.held_locks.get(&pending.name).map(Vec::as_slice).unwrap_or(&[]);
                node_locks_is_compatible(held, &pending.mode)
            })
        };

        let Some(index) = grantable_index else {
            return Ok(());
        };

        let pending = host.bootstrap.borrow_mut().pending_locks.remove(index);
        node_locks_grant_request(
            host,
            pending.request_id,
            pending.name,
            pending.client_id,
            pending.mode,
            pending.callback,
            pending.resolve,
            pending.reject,
            context,
        )?;
    }
}

fn node_locks_request(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let name = node_arg_string(args, 0, context)?;
    let client_id = node_arg_string(args, 1, context)?;
    let mode = node_arg_string(args, 2, context)?;
    let steal = args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_boolean();
    let if_available = args.get(4).cloned().unwrap_or_else(JsValue::undefined).to_boolean();
    let callback = args
        .get(5)
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("locks.request callback must be callable"))?;
    let _ = JsValue::from(callback.clone())
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("locks.request callback must be callable"))?;

    node_with_host_js(context, |host, context| {
        let (promise, resolvers) = JsPromise::new_pending(context);
        let request_id = {
            let mut bootstrap = host.bootstrap.borrow_mut();
            let request_id = bootstrap.next_lock_request_id;
            bootstrap.next_lock_request_id = bootstrap.next_lock_request_id.saturating_add(1);
            request_id
        };

        if steal {
            let stolen = {
                let mut bootstrap = host.bootstrap.borrow_mut();
                bootstrap.held_locks.remove(&name).unwrap_or_default()
            };
            for held in stolen {
                held.reject
                    .call(
                        &JsValue::undefined(),
                        &[JsValue::from(js_string!("LOCK_STOLEN"))],
                        context,
                    )
                    .map_err(sandbox_execution_error)?;
            }
        }

        let compatible = {
            let bootstrap = host.bootstrap.borrow();
            let held = bootstrap.held_locks.get(&name).map(Vec::as_slice).unwrap_or(&[]);
            node_locks_is_compatible(held, &mode)
        };

        if compatible {
            node_locks_grant_request(
                host,
                request_id,
                name,
                client_id,
                mode,
                callback.clone(),
                resolvers.resolve.clone().into(),
                resolvers.reject.clone().into(),
                context,
            )?;
        } else if if_available {
            let result = callback
                .call(&JsValue::undefined(), &[JsValue::null()], context)
                .map_err(sandbox_execution_error)?;
            let resolved = JsPromise::resolve(result, context).map_err(sandbox_execution_error)?;
            let on_fulfilled = FunctionObjectBuilder::new(
                context.realm(),
                NativeFunction::from_copy_closure_with_captures(
                    |_this, args, resolve, context| {
                        resolve
                            .call(
                                &JsValue::undefined(),
                                &[args.first().cloned().unwrap_or_else(JsValue::undefined)],
                                context,
                            )?;
                        Ok(JsValue::undefined())
                    },
                    resolvers.resolve.clone(),
                ),
            )
            .name(js_string!("lockIfAvailableFulfilled"))
            .length(1)
            .constructor(false)
            .build();
            let on_rejected = FunctionObjectBuilder::new(
                context.realm(),
                NativeFunction::from_copy_closure_with_captures(
                    |_this, args, reject, context| {
                        let reason = args.first().cloned().unwrap_or_else(JsValue::undefined);
                        reject.call(&JsValue::undefined(), &[reason.clone()], context)?;
                        Err(boa_engine::JsError::from_opaque(reason))
                    },
                    resolvers.reject.clone(),
                ),
            )
            .name(js_string!("lockIfAvailableRejected"))
            .length(1)
            .constructor(false)
            .build();
            let _ = resolved
                .then(Some(on_fulfilled), Some(on_rejected), context)
                .map_err(sandbox_execution_error)?;
        } else {
            host.bootstrap.borrow_mut().pending_locks.push(NodePendingLockState {
                request_id,
                name,
                client_id,
                mode,
                callback: callback.clone(),
                resolve: resolvers.resolve.clone().into(),
                reject: resolvers.reject.clone().into(),
            });
        }

        Ok(JsValue::from(promise))
    })
}

fn node_locks_query(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let bootstrap = host.bootstrap.borrow();
        let held = bootstrap
            .held_locks
            .values()
            .flat_map(|locks| locks.iter())
            .map(|lock| {
                serde_json::json!({
                    "name": lock.name,
                    "mode": lock.mode,
                    "clientId": lock.client_id,
                })
            })
            .collect::<Vec<_>>();
        let pending = bootstrap
            .pending_locks
            .iter()
            .map(|lock| {
                serde_json::json!({
                    "name": lock.name,
                    "mode": lock.mode,
                    "clientId": lock.client_id,
                })
            })
            .collect::<Vec<_>>();
        Ok(JsValue::from(
            JsPromise::resolve(
                JsValue::from_json(
                    &serde_json::json!({
                        "held": held,
                        "pending": pending,
                    }),
                    context,
                )
                .map_err(sandbox_execution_error)?,
                context,
            )
            .map_err(sandbox_execution_error)?,
        ))
    })
}

fn node_internal_binding_worker(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, value) in [
        ("isMainThread", JsValue::from(true)),
        ("ownsProcessState", JsValue::from(true)),
        ("threadId", JsValue::from(0)),
    ] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    let get_env_message_port = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_worker_get_env_message_port),
    )
    .name(js_string!("getEnvMessagePort"))
    .length(0)
    .constructor(false)
    .build();
    object
        .set(
            js_string!("getEnvMessagePort"),
            JsValue::from(get_env_message_port),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_worker_get_env_message_port(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        if let Some(id) = host.bootstrap.borrow().env_message_port {
            return Ok(JsValue::from(node_messaging_materialize_port(host, id, context)?));
        }
        Ok(JsValue::undefined())
    })
}

fn node_internal_binding_async_wrap(
    _host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let async_hook_fields = context
        .eval(Source::from_bytes(b"new Uint32Array(8)"))
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "failed to create async_hook_fields".to_string(),
        })?;
    let async_id_fields = context
        .eval(Source::from_bytes(b"new Float64Array([0, 0, 1, -1, 0, 0])"))
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "failed to create async_id_fields".to_string(),
        })?;
    let async_ids_stack = context
        .eval(Source::from_bytes(b"new Float64Array(256)"))
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "failed to create async_ids_stack".to_string(),
        })?;
    let execution_async_resources = context
        .eval(Source::from_bytes(b"[]"))
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "failed to create execution_async_resources".to_string(),
        })?;
    for (name, value) in [
        ("async_hook_fields", JsValue::from(async_hook_fields)),
        ("async_id_fields", JsValue::from(async_id_fields)),
        ("execution_async_resources", JsValue::from(execution_async_resources)),
        ("async_ids_stack", JsValue::from(async_ids_stack)),
        ("Providers", JsValue::from_json(&serde_json::json!({}), context).map_err(sandbox_execution_error)?),
        (
            "constants",
            JsValue::from_json(
                &serde_json::json!({
                    "kInit": 0,
                    "kBefore": 1,
                    "kAfter": 2,
                    "kDestroy": 3,
                    "kTotals": 4,
                    "kPromiseResolve": 5,
                    "kCheck": 6,
                    "kExecutionAsyncId": 0,
                    "kAsyncIdCounter": 2,
                    "kTriggerAsyncId": 1,
                    "kDefaultTriggerAsyncId": 3,
                    "kStackLength": 4,
                    "kUsesExecutionAsyncResource": 5
                }),
                context,
            )
            .map_err(sandbox_execution_error)?,
        ),
    ] {
        object
            .set(JsString::from(name), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    for (name, length, function) in [
        ("setCallbackTrampoline", 1usize, node_async_wrap_set_callback_trampoline as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("registerDestroyHook", 2usize, node_async_wrap_register_destroy_hook),
        ("queueDestroyAsyncId", 1usize, node_async_wrap_queue_destroy_async_id),
        ("setPromiseHooks", 4usize, node_async_wrap_set_promise_hooks),
        ("getPromiseHooks", 0usize, node_async_wrap_get_promise_hooks),
        ("setupHooks", 1usize, node_async_wrap_setup_hooks),
        ("pushAsyncContext", 3usize, node_async_wrap_push_async_context),
        ("popAsyncContext", 1usize, node_async_wrap_pop_async_context),
        ("executionAsyncResource", 1usize, node_async_wrap_execution_async_resource),
        ("clearAsyncIdStack", 0usize, node_async_wrap_clear_async_id_stack),
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

fn node_internal_binding_async_context_frame(
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, length, function) in [
        (
            "getContinuationPreservedEmbedderData",
            0usize,
            node_async_context_frame_get as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>,
        ),
        (
            "setContinuationPreservedEmbedderData",
            1usize,
            node_async_context_frame_set,
        ),
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

fn node_internal_binding_symbols(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = JsObject::with_null_proto();
    for (name, description) in [
        ("fs_use_promises_symbol", "fs_use_promises_symbol"),
        ("resource_symbol", "node.resource_symbol"),
        ("owner_symbol", "node.owner_symbol"),
        ("async_id_symbol", "node.async_id_symbol"),
        ("trigger_async_id_symbol", "node.trigger_async_id_symbol"),
        ("constructor_key_symbol", "constructor_key_symbol"),
        ("handle_onclose_symbol", "handle_onclose"),
        ("no_message_symbol", "no_message_symbol"),
        ("messaging_deserialize_symbol", "messaging_deserialize_symbol"),
        ("messaging_transfer_symbol", "messaging_transfer_symbol"),
        ("messaging_clone_symbol", "messaging_clone_symbol"),
        ("messaging_transfer_list_symbol", "messaging_transfer_list_symbol"),
        ("imported_cjs_symbol", "imported_cjs_symbol"),
        ("oninit", "oninit"),
        ("onpskexchange", "onpskexchange"),
        ("source_text_module_default_hdo", "source_text_module_default_hdo"),
        ("vm_context_no_contextify", "vm_context_no_contextify"),
        ("vm_dynamic_import_default_internal", "node.vm_dynamic_import_default_internal"),
        ("vm_dynamic_import_main_context_default", "vm_dynamic_import_main_context_default"),
        ("vm_dynamic_import_missing_flag", "vm_dynamic_import_missing_flag"),
        ("vm_dynamic_import_no_callback", "vm_dynamic_import_no_callback"),
        ("contextify_context_private_symbol", "node:contextify:context"),
    ] {
        object
            .set(JsString::from(name), node_symbol(description)?, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(object)
}

fn node_internal_binding_contextify(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let script_prototype = JsObject::with_null_proto();
    for (name, length, function) in [
        (
            "runInContext",
            5usize,
            node_contextify_script_run_in_context as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>,
        ),
        ("createCachedData", 0usize, node_contextify_script_create_cached_data),
    ] {
        let value = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(function),
        )
        .name(JsString::from(name))
        .length(length)
        .constructor(false)
        .build();
        script_prototype
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    let script_constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_contextify_script_construct),
    )
    .name(js_string!("ContextifyScript"))
    .length(8)
    .constructor(true)
    .build();
    script_constructor
        .set(js_string!("prototype"), JsValue::from(script_prototype), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("ContextifyScript"),
            JsValue::from(script_constructor),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    let constants = JsValue::from_json(
        &serde_json::json!({
            "measureMemory": {
                "mode": {
                    "SUMMARY": 0,
                    "DETAILED": 1
                },
                "execution": {
                    "DEFAULT": 0,
                    "EAGER": 1
                }
            }
        }),
        context,
    )
    .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("constants"), constants, true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("makeContext", 7usize, node_contextify_make_context as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("containsModuleSyntax", 2usize, node_contextify_contains_module_syntax as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("compileFunctionForCJSLoader", 4usize, node_contextify_compile_function_for_cjs_loader),
        ("compileFunction", 10usize, node_contextify_compile_function),
        ("startSigintWatchdog", 0usize, node_contextify_start_sigint_watchdog),
        ("stopSigintWatchdog", 0usize, node_contextify_stop_sigint_watchdog),
        ("watchdogHasPendingSigint", 0usize, node_contextify_watchdog_has_pending_sigint),
        ("measureMemory", 2usize, node_contextify_measure_memory),
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

fn node_internal_binding_messaging(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let exports = active_node_host()?
        .bootstrap
        .borrow()
        .per_context_exports
        .clone()
        .ok_or_else(|| sandbox_execution_error("per-context exports are not initialized"))?;
    let dom_exception = exports
        .get(js_string!("DOMException"), context)
        .map_err(sandbox_execution_error)?;
    let emit_message = exports
        .get(js_string!("emitMessage"), context)
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("DOMException"), dom_exception, true, context)
        .map_err(sandbox_execution_error)?;
    let expose_lazy_dom_exception_property = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_messaging_expose_lazy_dom_exception_property),
    )
    .name(js_string!("exposeLazyDOMExceptionProperty"))
    .length(1)
    .constructor(false)
    .build();
    object
        .set(
            js_string!("exposeLazyDOMExceptionProperty"),
            JsValue::from(expose_lazy_dom_exception_property),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("emitMessage"), emit_message, true, context)
        .map_err(sandbox_execution_error)?;
    let message_port = node_messaging_message_port_constructor(context)?;
    let message_channel = node_messaging_message_channel_constructor(context)?;
    object
        .set(js_string!("MessagePort"), JsValue::from(message_port), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("MessageChannel"),
            JsValue::from(message_channel),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        (
            "setDeserializerCreateObjectFunction",
            1usize,
            node_messaging_set_deserializer_create_object_function
                as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>,
        ),
        ("structuredClone", 2usize, node_messaging_structured_clone),
        ("receiveMessageOnPort", 1usize, node_messaging_receive_message_on_port),
        ("drainMessagePort", 1usize, node_messaging_drain_message_port),
        ("moveMessagePortToContext", 2usize, node_messaging_move_message_port_to_context),
        ("stopMessagePort", 1usize, node_messaging_stop_message_port),
        ("broadcastChannel", 1usize, node_messaging_broadcast_channel),
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

fn node_messaging_expose_lazy_dom_exception_property(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let target = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("target must be an object"))?;
    let getter = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_copy_closure(|_this, _args, context| {
            let exports = active_node_host()
                .map_err(js_error)?
                .bootstrap
                .borrow()
                .per_context_exports
                .clone()
                .ok_or_else(|| {
                    boa_engine::JsError::from_native(
                        JsNativeError::error()
                            .with_message("per-context exports are not initialized"),
                    )
                })?;
            exports.get(js_string!("DOMException"), context)
        }),
    )
    .name(js_string!("get DOMException"))
    .length(0)
    .constructor(false)
    .build();
    target.define_property_or_throw(
        js_string!("DOMException"),
        PropertyDescriptor::builder()
            .get(getter)
            .enumerable(false)
            .configurable(true),
        context,
    )?;
    Ok(JsValue::undefined())
}

fn node_messaging_port_id(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<u64, SandboxError> {
    let symbol = node_host_private_symbol(host, "message_port.id")?;
    object
        .get(symbol, context)
        .map_err(sandbox_execution_error)?
        .as_number()
        .map(|value| value as u64)
        .ok_or_else(|| sandbox_execution_error("message port is missing host id"))
}

fn node_messaging_new_port(
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let constructor = node_internal_binding_messaging(context)?
        .get(js_string!("MessagePort"), context)
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| sandbox_execution_error("messaging.MessagePort must be a constructor"))?;
    constructor
        .construct(&[], None, context)
        .map_err(sandbox_execution_error)
}

fn node_messaging_materialize_port(
    host: &NodeRuntimeHost,
    id: u64,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    if let Some(port) = host
        .bootstrap
        .borrow()
        .message_ports
        .get(&id)
        .and_then(|state| state.object.clone())
    {
        return Ok(port);
    }
    let constructor = node_internal_binding_messaging(context)?
        .get(js_string!("MessagePort"), context)
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| sandbox_execution_error("messaging.MessagePort must be a constructor"))?;
    let port = JsObject::with_null_proto();
    let prototype = constructor
        .get(js_string!("prototype"), context)
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| sandbox_execution_error("MessagePort.prototype must be an object"))?;
    port.set_prototype(Some(prototype));
    let symbol = node_host_private_symbol(host, "message_port.id")?;
    port.set(symbol, JsValue::from(id as f64), true, context)
        .map_err(sandbox_execution_error)?;
    if let Some(state) = host.bootstrap.borrow_mut().message_ports.get_mut(&id) {
        state.object = Some(port.clone());
    }
    Ok(port)
}

fn node_messaging_message_port_constructor(
    context: &mut Context,
) -> Result<JsFunction, SandboxError> {
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_messaging_message_port_construct),
    )
    .name(js_string!("MessagePort"))
    .length(0)
    .constructor(true)
    .build();
    let prototype = JsObject::with_null_proto();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype.clone()), false, context)
        .map_err(sandbox_execution_error)?;
    prototype
        .set(js_string!("constructor"), JsValue::from(constructor.clone()), true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("postMessage", 1usize, node_messaging_message_port_post_message as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("start", 0usize, node_messaging_message_port_start),
        ("close", 0usize, node_messaging_message_port_close),
        ("ref", 0usize, node_messaging_message_port_ref),
        ("unref", 0usize, node_messaging_message_port_unref),
        ("hasRef", 0usize, node_messaging_message_port_has_ref),
    ] {
        prototype
            .set(
                JsString::from(name),
                JsValue::from(
                    FunctionObjectBuilder::new(
                        context.realm(),
                        NativeFunction::from_fn_ptr(function),
                    )
                    .name(JsString::from(name))
                    .length(length)
                    .constructor(false)
                    .build(),
                ),
                true,
                context,
            )
            .map_err(sandbox_execution_error)?;
    }
    Ok(constructor)
}

fn node_messaging_message_port_construct(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(this) = this.as_object() else {
        return Err(JsNativeError::typ()
            .with_message("MessagePort constructor requires receiver")
            .into());
    };
    node_with_host_js(context, |host, context| {
        let id = node_next_host_handle_id(host);
        let symbol = node_host_private_symbol(host, "message_port.id")?;
        this.set(symbol, JsValue::from(id as f64), true, context)
            .map_err(sandbox_execution_error)?;
        host.bootstrap.borrow_mut().message_ports.insert(
            id,
            NodeMessagePortState {
                object: Some(this.clone()),
                entangled: None,
                refed: true,
                started: false,
                closed: false,
                broadcast_name: None,
                queue: Vec::new(),
            },
        );
        let symbols = node_internal_binding_symbols(context)?;
        let oninit = symbols
            .get(js_string!("oninit"), context)
            .map_err(sandbox_execution_error)?
            .as_symbol()
            .ok_or_else(|| sandbox_execution_error("symbols.oninit must be a symbol"))?;
        if let Some(callback) = this
            .get(oninit, context)
            .map_err(sandbox_execution_error)?
            .as_callable()
        {
            let _ = callback
                .call(&JsValue::from(this.clone()), &[], context)
                .map_err(sandbox_execution_error)?;
        }
        Ok(JsValue::from(this.clone()))
    })
}

fn node_messaging_message_channel_constructor(
    context: &mut Context,
) -> Result<JsFunction, SandboxError> {
    Ok(
        FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_fn_ptr(node_messaging_message_channel_construct),
        )
        .name(js_string!("MessageChannel"))
        .length(0)
        .constructor(true)
        .build(),
    )
}

fn node_messaging_message_channel_construct(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let channel = this.as_object().unwrap_or_else(JsObject::with_null_proto);
    let port1 = node_messaging_new_port(context).map_err(js_error)?;
    let port2 = node_messaging_new_port(context).map_err(js_error)?;
    node_with_host(|host| {
        let port1_id = node_messaging_port_id(host, &port1, context)?;
        let port2_id = node_messaging_port_id(host, &port2, context)?;
        if let Some(state) = host.bootstrap.borrow_mut().message_ports.get_mut(&port1_id) {
            state.entangled = Some(port2_id);
        }
        if let Some(state) = host.bootstrap.borrow_mut().message_ports.get_mut(&port2_id) {
            state.entangled = Some(port1_id);
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    channel.set(js_string!("port1"), JsValue::from(port1), true, context)?;
    channel.set(js_string!("port2"), JsValue::from(port2), true, context)?;
    Ok(JsValue::from(channel))
}

fn node_messaging_no_message_symbol(
    context: &mut Context,
) -> Result<JsValue, SandboxError> {
    let symbols = node_internal_binding_symbols(context)?;
    let symbol = symbols
        .get(js_string!("no_message_symbol"), context)
        .map_err(sandbox_execution_error)?;
    Ok(symbol)
}

fn node_messaging_emit_pending_messages(
    host: &NodeRuntimeHost,
    port_id: u64,
    force: bool,
    context: &mut Context,
) -> Result<(), SandboxError> {
    loop {
        let (port, message) = {
            let mut bootstrap = host.bootstrap.borrow_mut();
            let Some(state) = bootstrap.message_ports.get_mut(&port_id) else {
                return Ok(());
            };
            if state.closed || (!force && !state.started) {
                return Ok(());
            }
            let Some(port) = state.object.clone() else {
                return Ok(());
            };
            let Some(message) = state.queue.first().cloned() else {
                return Ok(());
            };
            state.queue.remove(0);
            (port, message)
        };

        let emit_message = node_internal_binding_messaging(context)?
            .get(js_string!("emitMessage"), context)
            .map_err(sandbox_execution_error)?
            .as_callable()
            .ok_or_else(|| sandbox_execution_error("messaging.emitMessage must be callable"))?;
        emit_message
            .call(
                &JsValue::from(port),
                &[message, JsValue::from(JsArray::new(context).map_err(sandbox_execution_error)?), JsValue::from(js_string!("message"))],
                context,
            )
            .map_err(sandbox_execution_error)?;
    }
}

fn node_messaging_receive_message_on_port(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = args.first().and_then(JsValue::as_object) else {
        return node_messaging_no_message_symbol(context).map_err(js_error);
    };
    node_with_host_js(context, |host, context| {
        let id = node_messaging_port_id(host, &port, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let Some(state) = bootstrap.message_ports.get_mut(&id) else {
            return node_messaging_no_message_symbol(context);
        };
        if state.closed {
            return node_messaging_no_message_symbol(context);
        }
        Ok(if state.queue.is_empty() {
            node_messaging_no_message_symbol(context).unwrap_or_else(|_| JsValue::undefined())
        } else {
            state.queue.remove(0)
        })
    })
}

fn node_messaging_drain_message_port(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    node_with_host(|host| {
        let id = node_messaging_port_id(host, &port, context)?;
        node_messaging_emit_pending_messages(host, id, true, context)?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_messaging_move_message_port_to_context(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let port = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| {
            JsNativeError::typ().with_message("The \"port\" argument must be a MessagePort instance")
        })?;
    let sandbox = args
        .get(1)
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("Invalid context argument"))?;

    let marker = node_contextify_private_symbol(context, "contextify_context_private_symbol")
        .map_err(js_error)?;
    if sandbox
        .get(marker, context)?
        .is_undefined()
    {
        return Err(JsNativeError::typ()
            .with_message("Invalid context argument")
            .into());
    }

    node_with_host_js(context, |host, context| {
        let old_id = node_messaging_port_id(host, &port, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let Some(mut state) = bootstrap.message_ports.remove(&old_id) else {
            return Err(sandbox_execution_error("message port is missing host id"));
        };
        if state.closed {
            return Err(sandbox_execution_error("Cannot send data on closed MessagePort"));
        }
        let new_id = node_next_host_handle_id(host);
        state.object = None;
        bootstrap.message_ports.insert(new_id, state);

        for peer in bootstrap.message_ports.values_mut() {
            if peer.entangled == Some(old_id) {
                peer.entangled = Some(new_id);
            }
        }

        let symbol = node_host_private_symbol(host, "message_port.id")?;
        port.set(symbol, JsValue::from(old_id as f64), true, context)
            .map_err(sandbox_execution_error)?;
        if let Some(old_state) = bootstrap.message_ports.get_mut(&old_id) {
            old_state.closed = true;
            old_state.started = false;
            old_state.entangled = None;
            old_state.queue.clear();
            old_state.object = Some(port.clone());
        } else {
            bootstrap.message_ports.insert(
                old_id,
                NodeMessagePortState {
                    object: Some(port.clone()),
                    entangled: None,
                    refed: false,
                    started: false,
                    closed: true,
                    broadcast_name: None,
                    queue: Vec::new(),
                },
            );
        }

        Ok(JsValue::from(node_messaging_materialize_port(host, new_id, context)?))
    })
}

fn node_messaging_stop_message_port(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    node_with_host(|host| {
        let id = node_messaging_port_id(host, &port, context)?;
        if let Some(state) = host.bootstrap.borrow_mut().message_ports.get_mut(&id) {
            state.started = false;
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_messaging_broadcast_channel(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let name = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    node_with_host_js(context, |host, context| {
        let port = node_messaging_new_port(context)?;
        let id = node_messaging_port_id(host, &port, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        if let Some(state) = bootstrap.message_ports.get_mut(&id) {
            state.broadcast_name = Some(name.clone());
            state.started = true;
        }
        bootstrap
            .broadcast_channels
            .entry(name)
            .or_default()
            .insert(id);
        Ok(JsValue::from(port))
    })
}

fn node_messaging_message_port_post_message(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("MessagePort receiver must be an object").into());
    };
    let message = args.first().cloned().unwrap_or_else(JsValue::undefined);
    node_with_host(|host| {
        let port_id = node_messaging_port_id(host, &port, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let Some(port_state) = bootstrap.message_ports.get(&port_id).cloned() else {
            return Ok(JsValue::undefined());
        };
        if port_state.closed {
            return Ok(JsValue::undefined());
        }
        if let Some(name) = port_state.broadcast_name {
            if let Some(targets) = bootstrap.broadcast_channels.get(&name).cloned() {
                for target_id in targets {
                    if target_id == port_id {
                        continue;
                    }
                    if let Some(target) = bootstrap.message_ports.get_mut(&target_id) {
                        if !target.closed {
                            target.queue.push(message.clone());
                        }
                    }
                }
            }
            drop(bootstrap);
            if let Some(targets) = host.bootstrap.borrow().broadcast_channels.get(&name).cloned() {
                for target_id in targets {
                    if target_id != port_id {
                        node_messaging_emit_pending_messages(host, target_id, false, context)?;
                    }
                }
            }
            return Ok(JsValue::undefined());
        }
        if let Some(target_id) = port_state.entangled {
            if let Some(target) = bootstrap.message_ports.get_mut(&target_id) {
                if !target.closed {
                    target.queue.push(message);
                }
            }
            drop(bootstrap);
            node_messaging_emit_pending_messages(host, target_id, false, context)?;
            return Ok(JsValue::undefined());
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_messaging_message_port_start(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("MessagePort receiver must be an object").into());
    };
    node_with_host(|host| {
        let id = node_messaging_port_id(host, &port, context)?;
        if let Some(state) = host.bootstrap.borrow_mut().message_ports.get_mut(&id) {
            state.started = true;
        }
        node_messaging_emit_pending_messages(host, id, false, context)?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_messaging_message_port_close(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("MessagePort receiver must be an object").into());
    };
    node_with_host(|host| {
        let id = node_messaging_port_id(host, &port, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let entangled = bootstrap
            .message_ports
            .get(&id)
            .and_then(|state| state.entangled);
        let broadcast_name = bootstrap
            .message_ports
            .get(&id)
            .and_then(|state| state.broadcast_name.clone());
        if let Some(state) = bootstrap.message_ports.get_mut(&id) {
            state.closed = true;
            state.started = false;
            state.queue.clear();
            state.entangled = None;
        }
        if let Some(peer_id) = entangled {
            if let Some(peer) = bootstrap.message_ports.get_mut(&peer_id) {
                if peer.entangled == Some(id) {
                    peer.entangled = None;
                }
            }
        }
        if let Some(name) = broadcast_name {
            if let Some(channels) = bootstrap.broadcast_channels.get_mut(&name) {
                channels.remove(&id);
                if channels.is_empty() {
                    bootstrap.broadcast_channels.remove(&name);
                }
            }
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    let symbols = node_internal_binding_symbols(context).map_err(js_error)?;
    let onclose = symbols
        .get(js_string!("handle_onclose_symbol"), context)?
        .as_symbol()
        .ok_or_else(|| JsNativeError::typ().with_message("handle_onclose_symbol must be a symbol"))?;
    if let Some(callback) = port.get(onclose, context)?.as_callable() {
        let _ = callback.call(&JsValue::from(port.clone()), &[], context)?;
    }
    Ok(JsValue::undefined())
}

fn node_messaging_message_port_ref(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("MessagePort receiver must be an object").into());
    };
    node_with_host(|host| {
        let id = node_messaging_port_id(host, &port, context)?;
        if let Some(state) = host.bootstrap.borrow_mut().message_ports.get_mut(&id) {
            state.refed = true;
        }
        Ok(JsValue::from(port.clone()))
    })
    .map_err(js_error)
}

fn node_messaging_message_port_unref(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = this.as_object() else {
        return Err(JsNativeError::typ().with_message("MessagePort receiver must be an object").into());
    };
    node_with_host(|host| {
        let id = node_messaging_port_id(host, &port, context)?;
        if let Some(state) = host.bootstrap.borrow_mut().message_ports.get_mut(&id) {
            state.refed = false;
        }
        Ok(JsValue::from(port.clone()))
    })
    .map_err(js_error)
}

fn node_messaging_message_port_has_ref(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(port) = this.as_object() else {
        return Ok(JsValue::from(false));
    };
    node_with_host(|host| {
        let id = node_messaging_port_id(host, &port, context)?;
        let refed = host
            .bootstrap
            .borrow()
            .message_ports
            .get(&id)
            .map(|state| state.refed && !state.closed)
            .unwrap_or(false);
        Ok(JsValue::from(refed))
    })
    .map_err(js_error)
}

fn node_messaging_set_deserializer_create_object_function(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args
        .first()
        .and_then(JsValue::as_object)
        .filter(|object| object.is_callable())
        .ok_or_else(|| {
            JsNativeError::typ()
                .with_message("deserializer create-object callback must be callable")
        })?;
    node_with_host(|host| {
        host.bootstrap
            .borrow_mut()
            .messaging_deserialize_create_object_callback = Some(callback);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_messaging_structured_clone(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let value = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let options = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    let global = context.global_object();
    let structured_clone = global.get(js_string!("structuredClone"), context)?;
    let callable = structured_clone.as_callable().ok_or_else(|| {
        JsNativeError::typ().with_message("global structuredClone is not callable")
    })?;
    callable.call(
        &JsValue::undefined(),
        &[value, options],
        context,
    )
}

fn node_internal_binding_profiler(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, function) in [
        ("setCoverageDirectory", node_profiler_set_coverage_directory as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("setSourceMapCacheGetter", node_profiler_set_source_map_cache_getter),
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

fn node_internal_binding_inspector(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let console = context
        .global_object()
        .get(js_string!("console"), context)
        .map_err(sandbox_execution_error)?;
    if let Some(console_object) = console.as_object() {
        active_node_host()
            .map_err(sandbox_execution_error)?
            .bootstrap
            .borrow_mut()
            .inspector
            .console = Some(console_object.clone());
    }
    object
        .set(js_string!("console"), console, true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("callAndPauseOnStart", 6usize, node_inspector_call_and_pause_on_start as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("consoleCall", 3usize, node_inspector_console_call),
        ("open", 2usize, node_inspector_open),
        ("url", 0usize, node_inspector_url),
        ("waitForDebugger", 0usize, node_inspector_wait_for_debugger),
        ("asyncTaskScheduled", 3usize, node_inspector_async_task_scheduled),
        ("asyncTaskCanceled", 1usize, node_inspector_async_task_canceled),
        ("asyncTaskStarted", 1usize, node_inspector_async_task_started),
        ("asyncTaskFinished", 1usize, node_inspector_async_task_finished),
        ("registerAsyncHook", 2usize, node_inspector_register_async_hook),
        ("isEnabled", 0usize, node_inspector_is_enabled),
        ("emitProtocolEvent", 2usize, node_inspector_emit_protocol_event),
        ("setupNetworkTracking", 2usize, node_inspector_setup_network_tracking),
        ("setConsoleExtensionInstaller", 1usize, node_inspector_set_console_extension_installer),
        ("putNetworkResource", 2usize, node_inspector_put_network_resource),
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

fn node_internal_binding_wasm_web_api(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let set_implementation = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_wasm_web_api_set_implementation),
    )
    .name(js_string!("setImplementation"))
    .length(1)
    .constructor(false)
    .build();
    object
        .set(
            js_string!("setImplementation"),
            JsValue::from(set_implementation),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_cjs_lexer(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let parse = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_cjs_lexer_parse),
    )
    .name(js_string!("parse"))
    .length(1)
    .constructor(false)
    .build();
    object
        .set(js_string!("parse"), JsValue::from(parse), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_permission(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let has = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_permission_has),
    )
    .name(js_string!("has"))
    .length(2)
    .constructor(false)
    .build();
    object
        .set(js_string!("has"), JsValue::from(has), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_mksnapshot(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let is_building_snapshot_buffer = context
        .eval(Source::from_bytes(b"new Uint8Array([0])"))
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("isBuildingSnapshotBuffer"),
            is_building_snapshot_buffer.clone(),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("runEmbedderPreload", 2usize, node_mksnapshot_run_embedder_preload as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("setSerializeCallback", 1usize, node_mksnapshot_set_serialize_callback),
        ("setDeserializeCallback", 1usize, node_mksnapshot_set_deserialize_callback),
        ("setDeserializeMainFunction", 1usize, node_mksnapshot_set_deserialize_main_function),
        ("compileSerializeMain", 2usize, node_mksnapshot_compile_serialize_main),
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
    object
        .set(
            js_string!("anonymousMainPath"),
            JsValue::from(JsString::from("__node_anonymous_main")),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_performance(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    object
        .set(
            js_string!("constants"),
            JsValue::from_json(
                &serde_json::json!({
                    "NODE_PERFORMANCE_MILESTONE_NODE_START": 0,
                    "NODE_PERFORMANCE_MILESTONE_V8_START": 1,
                    "NODE_PERFORMANCE_MILESTONE_ENVIRONMENT": 2,
                    "NODE_PERFORMANCE_MILESTONE_LOOP_START": 3,
                    "NODE_PERFORMANCE_MILESTONE_LOOP_EXIT": 4,
                    "NODE_PERFORMANCE_MILESTONE_BOOTSTRAP_COMPLETE": 5,
                    "NODE_PERFORMANCE_MILESTONE_TIME_ORIGIN": 6,
                    "NODE_PERFORMANCE_MILESTONE_TIME_ORIGIN_TIMESTAMP": 7,
                    "NODE_PERFORMANCE_ENTRY_TYPE_GC": 0,
                    "NODE_PERFORMANCE_ENTRY_TYPE_HTTP2": 1,
                    "NODE_PERFORMANCE_ENTRY_TYPE_HTTP": 2,
                    "NODE_PERFORMANCE_ENTRY_TYPE_NET": 3,
                    "NODE_PERFORMANCE_ENTRY_TYPE_DNS": 4
                }),
                context,
            )
            .map_err(sandbox_execution_error)?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("milestones"),
            context
                .eval(Source::from_bytes(b"new Float64Array(8)"))
                .map_err(sandbox_execution_error)?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    let milestones = object
        .get(js_string!("milestones"), context)
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| sandbox_execution_error("performance.milestones must be an object"))?;
    let host = active_node_host().map_err(sandbox_execution_error)?;
    node_performance_init_milestones(host.as_ref(), context, &milestones)?;
    object
        .set(
            js_string!("observerCounts"),
            context
                .eval(Source::from_bytes(b"new Uint32Array(5)"))
                .map_err(sandbox_execution_error)?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    if let Some(observer_counts) = object
        .get(js_string!("observerCounts"), context)
        .map_err(sandbox_execution_error)?
        .as_object()
    {
        host.bootstrap.borrow_mut().performance_observer_counts = Some(observer_counts.clone());
    }
    for (name, length, function) in [
        ("markBootstrapComplete", 0usize, node_performance_mark_bootstrap_complete as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("now", 0usize, node_performance_now),
        ("loopIdleTime", 0usize, node_performance_loop_idle_time),
        ("uvMetricsInfo", 0usize, node_performance_uv_metrics_info),
        ("installGarbageCollectionTracking", 0usize, node_performance_install_garbage_collection_tracking),
        ("removeGarbageCollectionTracking", 0usize, node_performance_remove_garbage_collection_tracking),
        ("notify", 2usize, node_performance_notify),
        ("setupObservers", 2usize, node_performance_setup_observers),
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

fn node_internal_binding_sea(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, length, function) in [
        ("isSea", 0usize, node_sea_is_sea as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("isExperimentalSeaWarningNeeded", 0usize, node_sea_is_experimental_warning_needed),
        ("getAsset", 1usize, node_sea_get_asset),
        ("getAssetKeys", 0usize, node_sea_get_asset_keys),
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

fn node_internal_binding_report(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    for (name, length, function) in [
        ("writeReport", 4usize, node_report_write_report as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("getReport", 1usize, node_report_get_report),
        ("getDirectory", 0usize, node_report_get_directory),
        ("setDirectory", 1usize, node_report_set_directory),
        ("getFilename", 0usize, node_report_get_filename),
        ("setFilename", 1usize, node_report_set_filename),
        ("getCompact", 0usize, node_report_get_compact),
        ("setCompact", 1usize, node_report_set_compact),
        ("getExcludeNetwork", 0usize, node_report_get_exclude_network),
        ("setExcludeNetwork", 1usize, node_report_set_exclude_network),
        ("getSignal", 0usize, node_report_get_signal),
        ("setSignal", 1usize, node_report_set_signal),
        ("shouldReportOnFatalError", 0usize, node_report_get_report_on_fatal_error),
        ("setReportOnFatalError", 1usize, node_report_set_report_on_fatal_error),
        ("shouldReportOnSignal", 0usize, node_report_get_report_on_signal),
        ("setReportOnSignal", 1usize, node_report_set_report_on_signal),
        ("shouldReportOnUncaughtException", 0usize, node_report_get_report_on_uncaught_exception),
        ("setReportOnUncaughtException", 1usize, node_report_set_report_on_uncaught_exception),
        ("getExcludeEnv", 0usize, node_report_get_exclude_env),
        ("setExcludeEnv", 1usize, node_report_set_exclude_env),
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

fn node_internal_binding_signal_wrap(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let prototype = JsObject::with_null_proto();
    for (name, length, function) in [
        ("unref", 0usize, node_signal_wrap_unref as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("start", 1usize, node_signal_wrap_start),
        ("close", 0usize, node_signal_wrap_close),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
        prototype
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_signal_wrap_construct),
    )
    .name(js_string!("Signal"))
    .length(0)
    .constructor(true)
    .build();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(js_string!("Signal"), JsValue::from(constructor), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_internal_binding_watchdog(context: &mut Context) -> Result<JsObject, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let prototype = JsObject::with_null_proto();
    for (name, length, function) in [
        ("start", 0usize, node_watchdog_start as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("stop", 0usize, node_watchdog_stop),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(length)
            .constructor(false)
            .build();
        prototype
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    let constructor = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_fn_ptr(node_watchdog_construct),
    )
    .name(js_string!("TraceSigintWatchdog"))
    .length(0)
    .constructor(true)
    .build();
    constructor
        .set(js_string!("prototype"), JsValue::from(prototype), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("TraceSigintWatchdog"),
            JsValue::from(constructor),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_inspector_call_and_pause_on_start(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let function = args
        .first()
        .and_then(JsValue::as_callable)
        .ok_or_else(|| JsNativeError::typ().with_message("inspector.callAndPauseOnStart requires callable"))?;
    let this_value = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    let call_args = args.get(2..).unwrap_or(&[]);
    function.call(&this_value, call_args, context)
}

fn node_inspector_console_call(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let inspector_method = args
        .first()
        .and_then(JsValue::as_callable)
        .ok_or_else(|| JsNativeError::typ().with_message("inspector consoleCall requires inspector callable"))?;
    let node_method = args
        .get(1)
        .and_then(JsValue::as_callable)
        .ok_or_else(|| JsNativeError::typ().with_message("inspector consoleCall requires node callable"))?;
    let call_args = args.get(2..).unwrap_or(&[]);
    let _ = inspector_method.call(this, call_args, context)?;
    node_method.call(this, call_args, context)
}

fn node_inspector_open(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let port = if args.first().is_some_and(|value| !value.is_null() && !value.is_undefined()) {
        let value = args
            .first()
            .cloned()
            .unwrap_or_else(JsValue::undefined)
            .to_u32(context)?;
        u16::try_from(value)
            .map_err(|_| JsNativeError::range().with_message("inspector port must fit in uint16"))?
    } else {
        9229
    };
    let host_name = if args.get(1).is_some_and(|value| !value.is_null() && !value.is_undefined()) {
        node_arg_string(args, 1, context)?
    } else {
        "127.0.0.1".to_string()
    };
    node_with_host(|host| {
        let mut state = host.bootstrap.borrow_mut();
        state.inspector.listening = true;
        state.inspector.waiting_for_debugger = false;
        state.inspector.port = port;
        state.inspector.host = host_name;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_inspector_url(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let state = host.bootstrap.borrow();
        if !state.inspector.listening {
            return Ok(JsValue::undefined());
        }
        Ok(JsValue::from(JsString::from(format!(
            "ws://{}:{}/terrace-inspector",
            state.inspector.host, state.inspector.port
        ))))
    })
    .map_err(js_error)
}

fn node_inspector_wait_for_debugger(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let mut state = host.bootstrap.borrow_mut();
        let active = state.inspector.listening;
        if active {
            state.inspector.waiting_for_debugger = true;
        }
        Ok(JsValue::from(active))
    })
    .map_err(js_error)
}

fn node_inspector_async_task_scheduled(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let task_name = node_arg_string(args, 0, context)?;
    let task_id = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)? as u64;
    let recurring = args.get(2).is_some_and(JsValue::to_boolean);
    node_with_host(|host| {
        host.bootstrap
            .borrow_mut()
            .inspector
            .async_tasks
            .insert(task_id, (task_name, recurring));
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_inspector_async_task_canceled(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let task_id = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)? as u64;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().inspector.async_tasks.remove(&task_id);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_inspector_async_task_started(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let task_id = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)? as u64;
    node_with_host(|host| {
        if let Some(enable) = host.bootstrap.borrow().inspector.async_hook_enable.clone() {
            let _ = enable.call(
                &JsValue::undefined(),
                &[JsValue::from(task_id as f64)],
                context,
            );
        }
        Ok(JsValue::from(
            host.bootstrap.borrow().inspector.async_tasks.contains_key(&task_id),
        ))
    })
    .map_err(js_error)
}

fn node_inspector_async_task_finished(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let task_id = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)? as u64;
    node_with_host(|host| {
        if let Some(disable) = host.bootstrap.borrow().inspector.async_hook_disable.clone() {
            let _ = disable.call(
                &JsValue::undefined(),
                &[JsValue::from(task_id as f64)],
                context,
            );
        }
        if let Some((_, recurring)) = host.bootstrap.borrow().inspector.async_tasks.get(&task_id) {
            if !*recurring {
                host.bootstrap.borrow_mut().inspector.async_tasks.remove(&task_id);
            }
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_inspector_register_async_hook(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let mut bootstrap = host.bootstrap.borrow_mut();
        let mut state = bootstrap.inspector.clone();
        state.async_hook_enable = args.first().and_then(JsValue::as_object);
        state.async_hook_disable = args.get(1).and_then(JsValue::as_object);
        bootstrap.inspector = state;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_inspector_is_enabled(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().inspector.listening)))
        .map_err(js_error)
}

fn node_inspector_emit_protocol_event(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let event_name = node_arg_string(args, 0, context)?;
    let params = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    let params_json = params
        .to_json(context)?
        .unwrap_or(JsonValue::Null);
    node_with_host(|host| {
        host.bootstrap
            .borrow_mut()
            .inspector
            .protocol_events
            .push((event_name.clone(), params_json));
        node_debug_event(host, "inspector", format!("emitProtocolEvent {event_name}"))?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_inspector_setup_network_tracking(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let mut bootstrap = host.bootstrap.borrow_mut();
        let mut state = bootstrap.inspector.clone();
        state.network_tracking_enable = args.first().and_then(JsValue::as_object);
        state.network_tracking_disable = args.get(1).and_then(JsValue::as_object);
        bootstrap.inspector = state;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_inspector_set_console_extension_installer(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().inspector.console_extension_installer =
            args.first().and_then(JsValue::as_object);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_inspector_put_network_resource(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let url = node_arg_string(args, 0, context)?;
    let data = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    node_with_host(|host| {
        host.bootstrap
            .borrow_mut()
            .inspector
            .network_resources
            .insert(url.clone(), data.clone());
        node_debug_event(
            host,
            "inspector",
            format!("putNetworkResource url={url} bytes={}", data.len()),
        )?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_wasm_web_api_set_implementation(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().wasm_web_api_callback = args.first().and_then(JsValue::as_object);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_cjs_lexer_parse(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let source = if args.first().is_some_and(|value| !value.is_null() && !value.is_undefined()) {
        args.first()
            .cloned()
            .unwrap_or_else(JsValue::undefined)
            .to_string(context)?
            .to_std_string_escaped()
    } else {
        String::new()
    };
    let exports_set = context
        .eval(Source::from_bytes(b"new Set()"))
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .as_object()
        .ok_or_else(|| js_error(sandbox_execution_error("failed to create Set for cjs lexer")))?;
    let add = exports_set
        .get(js_string!("add"), context)?
        .as_callable()
        .ok_or_else(|| js_error(sandbox_execution_error("cjs lexer Set.add is not callable")))?;
    let mut export_names = BTreeSet::new();
    let mut reexports = Vec::new();
    for name in node_cjs_lexer_collect_named_exports(&source) {
        export_names.insert(name);
    }
    for specifier in node_cjs_lexer_collect_reexports(&source) {
        reexports.push(JsValue::from(JsString::from(specifier)));
    }
    for name in export_names {
        let _ = add.call(
            &JsValue::from(exports_set.clone()),
            &[JsValue::from(JsString::from(name))],
            context,
        )?;
    }
    let reexports = JsValue::from(JsArray::from_iter(reexports, context));
    Ok(JsValue::from(JsArray::from_iter(
        [JsValue::from(exports_set), reexports],
        context,
    )))
}

fn node_permission_has(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let scope = node_arg_string(args, 0, context)?;
    let reference = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    node_with_host(|host| {
        let state = host.bootstrap.borrow();
        if !state.permission_enabled {
            return Ok(JsValue::from(true));
        }
        let allowed = match scope.as_str() {
            "fs" | "fs.read" | "fs.write" => {
                if reference.is_null() || reference.is_undefined() {
                    true
                } else if let Some(path) = reference.as_string() {
                    let path = resolve_node_path(
                        &host.process.borrow().cwd,
                        &path.to_std_string_escaped(),
                    );
                    path.starts_with(&host.workspace_root) || path.starts_with(NODE_UPSTREAM_VFS_ROOT)
                } else {
                    true
                }
            }
            "child" | "child_process" | "worker" | "addons" | "inspector" | "wasi" => false,
            _ => false,
        };
        Ok(JsValue::from(allowed))
    })
    .map_err(js_error)
}

fn node_mksnapshot_run_embedder_preload(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let preload = args.first().and_then(JsValue::as_object);
    let receiver = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    node_with_host_js(context, |host, context| {
        node_debug_event(host, "bootstrap", "mksnapshot.runEmbedderPreload")?;
        if let Some(preload) = preload {
            let callable = JsValue::from(preload)
                .as_callable()
                .ok_or_else(|| sandbox_execution_error("mksnapshot preload must be callable"))?;
            callable
                .call(&receiver, &[], context)
                .map_err(sandbox_execution_error)?;
        }
        Ok(JsValue::undefined())
    })
}

fn node_mksnapshot_set_serialize_callback(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        if host.bootstrap.borrow().snapshot_serialize_callback.is_some() {
            return Err(sandbox_execution_error(
                "mksnapshot serialize callback already registered",
            ));
        }
        host.bootstrap.borrow_mut().snapshot_serialize_callback =
            args.first().and_then(JsValue::as_object);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_mksnapshot_set_deserialize_callback(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        if host.bootstrap.borrow().snapshot_deserialize_callback.is_some() {
            return Err(sandbox_execution_error(
                "mksnapshot deserialize callback already registered",
            ));
        }
        host.bootstrap.borrow_mut().snapshot_deserialize_callback =
            args.first().and_then(JsValue::as_object);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_mksnapshot_set_deserialize_main_function(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        if host.bootstrap.borrow().snapshot_deserialize_main_function.is_some() {
            return Err(sandbox_execution_error(
                "mksnapshot deserialize main function already registered",
            ));
        }
        host.bootstrap.borrow_mut().snapshot_deserialize_main_function =
            args.first().and_then(JsValue::as_object);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_mksnapshot_compile_serialize_main(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let filename = node_arg_string(args, 0, context)?;
    let source = node_arg_string(args, 1, context)?;
    let wrapped = format!(
        "(function(require, __filename, __dirname) {{\n{source}\n//# sourceURL={filename}\n}})"
    );
    context.eval(Source::from_bytes(wrapped.as_bytes()).with_path(&PathBuf::from(filename)))
}

fn node_performance_mark_bootstrap_complete(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().bootstrap_completed = true;
        let now_nanos = node_monotonic_now_ms(host) * 1_000_000.0;
        node_performance_set_milestone(
            host,
            context,
            NODE_PERFORMANCE_MILESTONE_BOOTSTRAP_COMPLETE,
            now_nanos,
        )?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_performance_now(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(node_monotonic_now_ms(host)))).map_err(js_error)
}

fn node_performance_loop_idle_time(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().performance_loop_idle_time_ms)))
        .map_err(js_error)
}

fn node_performance_uv_metrics_info(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let state = host.bootstrap.borrow();
        let value = JsValue::from_json(
            &serde_json::json!([
                state.performance_uv_loop_count,
                state.performance_uv_events,
                state.performance_uv_events_waiting
            ]),
            context,
        )
        .map_err(sandbox_execution_error)?;
        Ok(value)
    })
    .map_err(js_error)
}

fn node_performance_install_garbage_collection_tracking(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().gc_tracking_installed = true;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_performance_remove_garbage_collection_tracking(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().gc_tracking_installed = false;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_performance_setup_observers(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let callback = args
            .first()
            .and_then(JsValue::as_object)
            .filter(|object| object.is_callable())
            .ok_or_else(|| {
                sandbox_execution_error(node_error_with_code(
                    context,
                    JsNativeError::typ(),
                    "performance observer callback must be callable",
                    "ERR_INVALID_ARG_TYPE",
                ))
            })?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        bootstrap.performance_observer_callback = Some(callback);
        if let Some(observer_counts) = args.get(1).and_then(JsValue::as_object) {
            bootstrap.performance_observer_counts = Some(observer_counts.clone());
        } else if bootstrap.performance_observer_counts.is_none() {
            let counts = context
                .eval(Source::from_bytes(b"new Uint32Array(5)"))
                .map_err(sandbox_execution_error)?
                .as_object()
                .ok_or_else(|| sandbox_execution_error("observerCounts must be an object"))?;
            bootstrap.performance_observer_counts = Some(counts);
        }
        Ok(JsValue::undefined())
    })
}

fn node_performance_notify(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let entry_type = args
            .first()
            .cloned()
            .unwrap_or_else(JsValue::undefined)
            .to_string(context)
            .map_err(sandbox_execution_error)?
            .to_std_string_escaped();
        let entry = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
        let observer_index = match entry_type.as_str() {
            "gc" => Some(0u32),
            "http2" => Some(1u32),
            "http" => Some(2u32),
            "net" => Some(3u32),
            "dns" => Some(4u32),
            _ => None,
        };
        let state = host.bootstrap.borrow();
        let Some(callback) = state.performance_observer_callback.clone() else {
            return Ok(JsValue::undefined());
        };
        if let Some(index) = observer_index
            && let Some(observer_counts) = state.performance_observer_counts.clone()
        {
            let count = observer_counts
                .get(index, context)
                .map_err(sandbox_execution_error)?
                .to_u32(context)
                .map_err(sandbox_execution_error)?;
            if count == 0 {
                return Ok(JsValue::undefined());
            }
        }
        let callable = JsValue::from(callback)
            .as_callable()
            .ok_or_else(|| sandbox_execution_error("performance observer callback is not callable"))?;
        callable
            .call(&JsValue::undefined(), &[entry], context)
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::undefined())
    })
}

fn node_sea_is_sea(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        Ok(JsValue::from(host.bootstrap.borrow().sea.enabled))
    })
    .map_err(js_error)
}

fn node_sea_is_experimental_warning_needed(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let sea = &host.bootstrap.borrow().sea;
        Ok(JsValue::from(sea.enabled && sea.experimental_warning_needed))
    })
    .map_err(js_error)
}

fn node_sea_get_asset(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let key = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        let sea = &host.bootstrap.borrow().sea;
        let Some(asset) = sea.assets.get(&key) else {
            return Ok(JsValue::undefined());
        };
        let buffer = JsUint8Array::from_iter(asset.iter().copied(), context)
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::from(buffer))
    })
}

fn node_sea_get_asset_keys(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let keys = host
            .bootstrap
            .borrow()
            .sea
            .assets
            .keys()
            .cloned()
            .map(JsString::from)
            .map(JsValue::from)
            .collect::<Vec<_>>();
        Ok(JsValue::from(JsArray::from_iter(keys, context)))
    })
}

fn node_report_write_report(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let event = node_arg_string(args, 0, context)?;
    let trigger = node_arg_string(args, 1, context)?;
    let file = if args.get(2).is_some_and(|value| !value.is_null() && !value.is_undefined()) {
        Some(node_arg_string(args, 2, context)?)
    } else {
        None
    };
    let report_json = node_with_host(|host| {
        let report = node_report_json(host, &event, &trigger)?;
        let path = file.unwrap_or_else(|| {
            let state = host.bootstrap.borrow();
            format!("{}/{}", state.report.directory, state.report.filename)
        });
        if path == "stdout" {
            host.process.borrow_mut().stdout.push_str(&report);
            host.process.borrow_mut().stdout.push('\n');
            return Ok((report, path));
        }
        if path == "stderr" {
            host.process.borrow_mut().stderr.push_str(&report);
            host.process.borrow_mut().stderr.push('\n');
            return Ok((report, path));
        }
        let resolved = resolve_node_path(&host.process.borrow().cwd, &path);
        futures::executor::block_on(host.session.filesystem().write_file(
            &resolved,
            report.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        ))?;
        node_graph_invalidate(host);
        Ok((report, resolved))
    })
    .map_err(js_error)?;
    Ok(JsValue::from(JsString::from(report_json.1)))
}

fn node_report_get_report(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let (event, trigger) = if let Some(error) = args.first().filter(|value| !value.is_null() && !value.is_undefined()) {
        let message = error
            .as_object()
            .and_then(|object| object.get(js_string!("message"), context).ok())
            .and_then(|value| value.as_string().map(|value| value.to_std_string_escaped()))
            .unwrap_or_else(|| "Error".to_string());
        (message, "Exception".to_string())
    } else {
        ("JavaScript API".to_string(), "API".to_string())
    };
    node_with_host(|host| node_report_json(host, &event, &trigger).map(|report| JsValue::from(JsString::from(report))))
        .map_err(js_error)
}

fn node_report_get_directory(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(JsString::from(host.bootstrap.borrow().report.directory.clone()))))
        .map_err(js_error)
}

fn node_report_set_directory(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let directory = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.directory = directory;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_report_get_filename(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(JsString::from(host.bootstrap.borrow().report.filename.clone()))))
        .map_err(js_error)
}

fn node_report_set_filename(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let filename = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.filename = filename;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_report_get_compact(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().report.compact))).map_err(js_error)
}

fn node_report_set_compact(_this: &JsValue, args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.compact = args.first().is_some_and(JsValue::to_boolean);
        Ok(JsValue::undefined())
    }).map_err(js_error)
}

fn node_report_get_exclude_network(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().report.exclude_network))).map_err(js_error)
}

fn node_report_set_exclude_network(_this: &JsValue, args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.exclude_network = args.first().is_some_and(JsValue::to_boolean);
        Ok(JsValue::undefined())
    }).map_err(js_error)
}

fn node_report_get_signal(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(JsString::from(host.bootstrap.borrow().report.signal.clone())))).map_err(js_error)
}

fn node_report_set_signal(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let signal = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.signal = signal;
        Ok(JsValue::undefined())
    }).map_err(js_error)
}

fn node_report_get_report_on_fatal_error(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().report.report_on_fatal_error))).map_err(js_error)
}

fn node_report_set_report_on_fatal_error(_this: &JsValue, args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.report_on_fatal_error = args.first().is_some_and(JsValue::to_boolean);
        Ok(JsValue::undefined())
    }).map_err(js_error)
}

fn node_report_get_report_on_signal(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().report.report_on_signal))).map_err(js_error)
}

fn node_report_set_report_on_signal(_this: &JsValue, args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.report_on_signal = args.first().is_some_and(JsValue::to_boolean);
        Ok(JsValue::undefined())
    }).map_err(js_error)
}

fn node_report_get_report_on_uncaught_exception(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().report.report_on_uncaught_exception))).map_err(js_error)
}

fn node_report_set_report_on_uncaught_exception(_this: &JsValue, args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.report_on_uncaught_exception = args.first().is_some_and(JsValue::to_boolean);
        Ok(JsValue::undefined())
    }).map_err(js_error)
}

fn node_report_get_exclude_env(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().report.exclude_env))).map_err(js_error)
}

fn node_report_set_exclude_env(_this: &JsValue, args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().report.exclude_env = args.first().is_some_and(JsValue::to_boolean);
        Ok(JsValue::undefined())
    }).map_err(js_error)
}

fn node_signal_wrap_construct(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let active_symbol = node_host_private_symbol(&host, "signal.active").map_err(js_error)?;
    let signal_symbol = node_host_private_symbol(&host, "signal.number").map_err(js_error)?;
    let refed_symbol = node_host_private_symbol(&host, "signal.refed").map_err(js_error)?;
    let object = this.as_object().unwrap_or_else(JsObject::with_null_proto);
    object.set(active_symbol, JsValue::from(false), true, context)?;
    object.set(signal_symbol, JsValue::from(-1), true, context)?;
    object.set(refed_symbol, JsValue::from(true), true, context)?;
    Ok(JsValue::from(object))
}

fn node_signal_wrap_unref(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let refed_symbol = node_host_private_symbol(&host, "signal.refed").map_err(js_error)?;
    let object = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("Signal.unref receiver must be an object"))?;
    object.set(refed_symbol, JsValue::from(false), true, context)?;
    Ok(JsValue::undefined())
}

fn node_signal_wrap_start(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let active_symbol = node_host_private_symbol(&host, "signal.active").map_err(js_error)?;
    let signal_symbol = node_host_private_symbol(&host, "signal.number").map_err(js_error)?;
    let signal_number = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)?;
    let object = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("Signal.start receiver must be an object"))?;
    object.set(active_symbol, JsValue::from(true), true, context)?;
    object.set(signal_symbol, JsValue::from(signal_number), true, context)?;
    Ok(JsValue::from(0))
}

fn node_signal_wrap_close(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let active_symbol = node_host_private_symbol(&host, "signal.active").map_err(js_error)?;
    let object = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("Signal.close receiver must be an object"))?;
    object.set(active_symbol, JsValue::from(false), true, context)?;
    Ok(JsValue::undefined())
}

fn node_watchdog_construct(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let started_symbol = node_host_private_symbol(&host, "watchdog.started").map_err(js_error)?;
    let object = this.as_object().unwrap_or_else(JsObject::with_null_proto);
    object.set(started_symbol, JsValue::from(false), true, context)?;
    Ok(JsValue::from(object))
}

fn node_watchdog_start(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let started_symbol = node_host_private_symbol(&host, "watchdog.started").map_err(js_error)?;
    let object = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("TraceSigintWatchdog.start receiver must be an object"))?;
    object.set(started_symbol, JsValue::from(true), true, context)?;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().sigint_watchdog_active = true;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_watchdog_stop(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let started_symbol = node_host_private_symbol(&host, "watchdog.started").map_err(js_error)?;
    let object = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("TraceSigintWatchdog.stop receiver must be an object"))?;
    object.set(started_symbol, JsValue::from(false), true, context)?;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().sigint_watchdog_active = false;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_cjs_lexer_collect_named_exports(source: &str) -> Vec<String> {
    let mut exports = BTreeSet::new();
    for prefix in ["exports.", "module.exports."] {
        let mut cursor = source;
        while let Some(index) = cursor.find(prefix) {
            cursor = &cursor[index + prefix.len()..];
            let name_len = cursor
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '$')
                .count();
            if name_len > 0 {
                exports.insert(cursor[..name_len].to_string());
            }
        }
    }
    exports.into_iter().collect()
}

fn node_cjs_lexer_collect_reexports(source: &str) -> Vec<String> {
    let mut reexports = BTreeSet::new();
    let patterns = [
        "require(\"",
        "require('",
        "__exportStar(require(\"",
        "__exportStar(require('",
    ];
    for pattern in patterns {
        let quote = pattern.chars().last().unwrap_or('"');
        let mut cursor = source;
        while let Some(index) = cursor.find(pattern) {
            cursor = &cursor[index + pattern.len()..];
            if let Some(end) = cursor.find(quote) {
                if end > 0 {
                    reexports.insert(cursor[..end].to_string());
                }
                cursor = &cursor[end + 1..];
            } else {
                break;
            }
        }
    }
    reexports.into_iter().collect()
}

fn node_report_json(
    host: &NodeRuntimeHost,
    event: &str,
    trigger: &str,
) -> Result<String, SandboxError> {
    let process = host.process.borrow();
    let state = host.bootstrap.borrow();
    let report = serde_json::json!({
        "header": {
            "event": event,
            "trigger": trigger,
            "filename": state.report.filename,
            "dumpEventTime": node_monotonic_now_ms(host),
            "cwd": process.cwd,
            "commandLine": process.argv,
            "nodejsVersion": process.version,
            "platform": process.platform,
            "arch": process.arch,
            "threadId": 0
        },
        "javascriptStack": {
            "message": "",
            "stack": []
        },
        "javascriptHeap": {},
        "nativeStack": [],
        "resourceUsage": {
            "rss": process.stdout.len() + process.stderr.len(),
        },
        "environmentVariables": if state.report.exclude_env {
            JsonValue::Object(Default::default())
        } else {
            serde_json::to_value(&process.env).map_err(sandbox_execution_error)?
        },
        "userLimits": {},
        "sharedObjects": []
    });
    if state.report.compact {
        serde_json::to_string(&report).map_err(sandbox_execution_error)
    } else {
        serde_json::to_string_pretty(&report).map_err(sandbox_execution_error)
    }
}

fn node_bootstrap_cached_array(
    host: &NodeRuntimeHost,
    context: &mut Context,
    name: &str,
    initial: &serde_json::Value,
) -> Result<JsObject, SandboxError> {
    let existing = {
        let bootstrap = host.bootstrap.borrow();
        match name {
            "tickInfo" => bootstrap.tick_info.clone(),
            "immediateInfo" => bootstrap.immediate_info.clone(),
            "timeoutInfo" => bootstrap.timeout_info.clone(),
            other => {
                return Err(SandboxError::Service {
                    service: "node_runtime",
                    message: format!("unknown bootstrap cache `{other}`"),
                });
            }
        }
    };
    if let Some(existing) = existing {
        return Ok(existing);
    }
    let value = JsValue::from_json(initial, context).map_err(sandbox_execution_error)?;
    let object = value.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: format!("bootstrap cache `{name}` did not create an object"),
    })?;
    let mut bootstrap = host.bootstrap.borrow_mut();
    match name {
        "tickInfo" => bootstrap.tick_info = Some(object.clone()),
        "immediateInfo" => bootstrap.immediate_info = Some(object.clone()),
        "timeoutInfo" => bootstrap.timeout_info = Some(object.clone()),
        _ => {}
    }
    Ok(object)
}

fn node_trace_events_category_set_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let categories_symbol =
        node_host_private_symbol(&host, "trace_events.categories").map_err(js_error)?;
    let target = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("CategorySet receiver must be an object"))?;
    let categories = node_arg_json::<Vec<String>>(args, 0, context).unwrap_or_default();
    target
        .set(
            categories_symbol,
            JsValue::from_json(&serde_json::json!(categories), context)
                .map_err(sandbox_execution_error)
                .map_err(js_error)?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    for (name, function) in [
        ("enable", node_trace_events_category_set_enable as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("disable", node_trace_events_category_set_disable),
    ] {
        let value = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
            .name(JsString::from(name))
            .length(0)
            .constructor(false)
            .build();
        target
            .set(JsString::from(name), JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
    }
    Ok(JsValue::undefined())
}

fn node_trace_events_category_set_enable(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let categories_symbol =
        node_host_private_symbol(&host, "trace_events.categories").map_err(js_error)?;
    let categories = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("CategorySet receiver must be an object"))?
        .get(categories_symbol, context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    let categories = categories
        .to_json(context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .unwrap_or(JsonValue::Null);
    let categories = serde_json::from_value::<Vec<String>>(categories)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().trace_categories.extend(categories);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    node_with_host_js(context, |host, context| {
        node_trace_events_sync_category_buffers(host, context)?;
        if let Some(callback) = host
            .bootstrap
            .borrow()
            .trace_category_state_update_handler
            .clone()
        {
            let callable = JsValue::from(callback)
                .as_callable()
                .ok_or_else(|| sandbox_execution_error("trace category state update handler is not callable"))?;
            let _ = callable
                .call(&JsValue::undefined(), &[], context)
                .map_err(sandbox_execution_error)?;
        }
        Ok(JsValue::undefined())
    })
}

fn node_trace_events_category_set_disable(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let categories_symbol =
        node_host_private_symbol(&host, "trace_events.categories").map_err(js_error)?;
    let categories = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("CategorySet receiver must be an object"))?
        .get(categories_symbol, context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    let categories = categories
        .to_json(context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .unwrap_or(JsonValue::Null);
    let categories = serde_json::from_value::<Vec<String>>(categories)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    node_with_host(|host| {
        let mut state = host.bootstrap.borrow_mut();
        for category in categories {
            state.trace_categories.remove(&category);
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    node_with_host_js(context, |host, context| {
        node_trace_events_sync_category_buffers(host, context)?;
        if let Some(callback) = host
            .bootstrap
            .borrow()
            .trace_category_state_update_handler
            .clone()
        {
            let callable = JsValue::from(callback)
                .as_callable()
                .ok_or_else(|| sandbox_execution_error("trace category state update handler is not callable"))?;
            let _ = callable
                .call(&JsValue::undefined(), &[], context)
                .map_err(sandbox_execution_error)?;
        }
        Ok(JsValue::undefined())
    })
}

fn node_trace_events_get_enabled_categories(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let categories = host
            .bootstrap
            .borrow()
            .trace_categories
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        Ok(JsValue::from(JsString::from(categories)))
    })
    .map_err(js_error)
}

fn node_trace_events_set_category_state_update_handler(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().trace_category_state_update_handler = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_trace_events_get_category_enabled_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let category = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        if let Some(existing) = host
            .bootstrap
            .borrow()
            .trace_category_buffers
            .get(&category)
            .cloned()
        {
            return Ok(JsValue::from(existing));
        }
        let enabled = host.bootstrap.borrow().trace_categories.contains(&category);
        let value = context
            .eval(Source::from_bytes(
                if enabled { b"new Uint8Array([1])" } else { b"new Uint8Array([0])" },
            ))
            .map_err(sandbox_execution_error)?;
        let object = value.as_object().ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "failed to create trace category buffer".to_string(),
        })?;
        host.bootstrap
            .borrow_mut()
            .trace_category_buffers
            .insert(category, object.clone());
        Ok(JsValue::from(object))
    })
}

fn node_trace_events_trace(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let category = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    let enabled = node_with_host(|host| Ok(host.bootstrap.borrow().trace_categories.contains(&category)))
        .map_err(js_error)?;
    if enabled {
        let detail = args
            .iter()
            .map(|value| value.display().to_string())
            .collect::<Vec<_>>()
            .join(" ");
        node_with_host(|host| {
            node_debug_event(host, "trace", format!("{category} {detail}"))?;
            Ok(JsValue::undefined())
        })
        .map_err(js_error)?;
    }
    Ok(JsValue::undefined())
}

fn node_trace_events_is_category_enabled(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let category = node_arg_string(args, 0, context)?;
    node_with_host(|host| Ok(JsValue::from(host.bootstrap.borrow().trace_categories.contains(&category))))
        .map_err(js_error)
}

fn node_trace_events_sync_category_buffers(
    host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<(), SandboxError> {
    let state = host.bootstrap.borrow().clone();
    for (category, buffer) in &state.trace_category_buffers {
        let enabled = state.trace_categories.contains(category);
        buffer
            .set(0u32, JsValue::from(if enabled { 1 } else { 0 }), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(())
}

fn node_process_methods_hrtime_buffer(
    host: &NodeRuntimeHost,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    if let Some(existing) = host.bootstrap.borrow().hrtime_buffer.clone() {
        return Ok(existing);
    }
    let value = context
        .eval(Source::from_bytes(b"new Uint32Array(3)"))
        .map_err(sandbox_execution_error)?;
    let object = value.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "failed to create hrtime buffer".to_string(),
    })?;
    host.bootstrap.borrow_mut().hrtime_buffer = Some(object.clone());
    Ok(object)
}

fn node_set_array_like_values(
    context: &mut Context,
    object: &JsObject,
    values: &[f64],
) -> Result<(), SandboxError> {
    for (index, value) in values.iter().copied().enumerate() {
        object
            .set(index as u32, JsValue::from(value), true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(())
}

fn node_set_array_like_entry(
    context: &mut Context,
    object: &JsObject,
    index: u32,
    value: impl Into<JsValue>,
) -> Result<(), SandboxError> {
    object
        .set(index, value.into(), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_get_object_property_as_object(
    context: &mut Context,
    object: &JsObject,
    name: &str,
) -> Result<JsObject, SandboxError> {
    object
        .get(JsString::from(name), context)
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("property `{name}` is not an object"),
        })
}

fn node_extract_source_annotations(source: &str) -> (Option<String>, Option<String>) {
    let mut source_url = None;
    let mut source_map_url = None;
    for line in source.lines().rev() {
        let trimmed = line.trim();
        if source_url.is_none() {
            if let Some(value) = trimmed
                .strip_prefix("//# sourceURL=")
                .or_else(|| trimmed.strip_prefix("//@ sourceURL="))
            {
                source_url = Some(value.trim().to_string());
            }
        }
        if source_map_url.is_none() {
            if let Some(value) = trimmed
                .strip_prefix("//# sourceMappingURL=")
                .or_else(|| trimmed.strip_prefix("//@ sourceMappingURL="))
            {
                source_map_url = Some(value.trim().to_string());
            }
        }
        if source_url.is_some() && source_map_url.is_some() {
            break;
        }
    }
    (source_url, source_map_url)
}

fn node_process_methods_patch_process_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let target = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("process object must be an object"))?;
    node_with_host_js(context, |host, context| {
        let process = host.process.borrow();
        for (name, value) in [
            ("argv", JsValue::from_json(&serde_json::json!(process.argv), context).map_err(sandbox_execution_error)?),
            ("execArgv", JsValue::from_json(&serde_json::json!([]), context).map_err(sandbox_execution_error)?),
            ("execPath", JsValue::from(JsString::from(process.exec_path.clone()))),
            ("pid", JsValue::from(process.pid)),
            ("ppid", JsValue::from(process.ppid)),
            ("title", JsValue::from(JsString::from(process.title.clone()))),
        ] {
            target
                .set(JsString::from(name), value, true, context)
                .map_err(sandbox_execution_error)?;
        }
        Ok(JsValue::undefined())
    })
}

fn node_process_methods_uptime(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(node_monotonic_now_ms(host) / 1000.0))).map_err(js_error)
}

fn node_process_methods_cause_segfault(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Err(js_error(SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "process.causeSegfault() is intentionally unsupported in the sandbox".to_string(),
    }))
}

fn node_process_methods_make_named_handle(
    context: &mut Context,
    name: &str,
) -> Result<JsValue, SandboxError> {
    let handle = JsObject::with_null_proto();
    let constructor = JsObject::with_null_proto();
    constructor
        .set(
            js_string!("name"),
            JsValue::from(JsString::from(name)),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    handle
        .set(
            js_string!("constructor"),
            JsValue::from(constructor),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(JsValue::from(handle))
}

fn node_process_methods_get_active_handles(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let state = host.bootstrap.borrow();
        let mut handles = Vec::new();
        if state.next_timer_due_ms.is_some() {
            handles.push(node_process_methods_make_named_handle(context, "Timeout")?);
        }
        let immediate_count = state
            .immediate_info
            .as_ref()
            .and_then(|info| info.get(0u32, context).ok())
            .and_then(|value| value.to_u32(context).ok())
            .unwrap_or(0);
        for _ in 0..immediate_count {
            handles.push(node_process_methods_make_named_handle(context, "Immediate")?);
        }
        if state.sigint_watchdog_active {
            handles.push(node_process_methods_make_named_handle(
                context,
                "TraceSigintWatchdog",
            )?);
        }
        Ok(JsValue::from(JsArray::from_iter(handles, context)))
    })
}

fn node_process_methods_get_active_requests(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let mut requests = Vec::new();
        let inspector_async_task_count = host.bootstrap.borrow().inspector.async_tasks.len();
        for _ in 0..inspector_async_task_count {
            requests.push(node_process_methods_make_named_handle(
                context,
                "InspectorAsyncTask",
            )?);
        }
        let zlib_stream_count = host.zlib_streams.borrow().entries.len();
        for _ in 0..zlib_stream_count {
            requests.push(node_process_methods_make_named_handle(context, "ZlibStream")?);
        }
        Ok(JsValue::from(JsArray::from_iter(requests, context)))
    })
}

fn node_process_methods_get_active_resources_info(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let state = host.bootstrap.borrow();
        let mut resources = Vec::new();
        if !state.microtask_queue.is_empty() {
            resources.push(JsValue::from(JsString::from("Microtask")));
        }
        if state.next_timer_due_ms.is_some() {
            resources.push(JsValue::from(JsString::from("Timeout")));
        }
        let immediate_count = state
            .immediate_info
            .as_ref()
            .and_then(|info| info.get(0u32, context).ok())
            .and_then(|value| value.to_u32(context).ok())
            .unwrap_or(0);
        if immediate_count > 0 {
            resources.push(JsValue::from(JsString::from("Immediate")));
        }
        if state.sigint_watchdog_active {
            resources.push(JsValue::from(JsString::from("Watchdog")));
        }
        if !state.inspector.async_tasks.is_empty() {
            resources.push(JsValue::from(JsString::from("InspectorAsyncTask")));
        }
        Ok(JsValue::from(JsArray::from_iter(resources, context)))
    })
}

fn node_process_methods_dlopen(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Err(js_error(SandboxError::Service {
        service: "node_runtime",
        message: "native addon loading is not supported in the sandbox".to_string(),
    }))
}

fn node_process_methods_really_exit(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let code = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    node_with_host(|host| {
        host.process.borrow_mut().exit_code = code;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)

}

fn node_process_methods_kill(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let pid = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    let signal = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    node_with_host(|host| {
        let own_pid = host.process.borrow().pid as i32;
        if pid != own_pid && pid != 0 && pid != -1 {
            return Ok(JsValue::from(-3));
        }
        if signal > 0 {
            host.process.borrow_mut().exit_code = 128 + signal;
        }
        Ok(JsValue::from(0))
    })
    .map_err(js_error)
}

fn node_process_methods_cpu_usage(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let array = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("cpuUsage target must be an object"))?;
    node_with_host(|host| {
        let user_micros = node_monotonic_now_ms(host) * 1000.0;
        node_set_array_like_values(context, &array, &[user_micros, 0.0])?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_memory_usage(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let array = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("memoryUsage target must be an object"))?;
    node_with_host(|host| {
        let module_bytes = host
            .loaded_modules
            .borrow()
            .values()
            .map(|module| module.source.len() as f64)
            .sum::<f64>();
        let open_file_bytes = host
            .open_files
            .borrow()
            .entries
            .values()
            .map(|file| file.contents.len() as f64)
            .sum::<f64>();
        let process = host.process.borrow();
        let external = process.stdout.len() as f64 + process.stderr.len() as f64 + open_file_bytes;
        let heap_used = module_bytes + open_file_bytes;
        let heap_total = heap_used.max(1024.0 * 1024.0);
        let rss = heap_total + external;
        node_set_array_like_values(
            context,
            &array,
            &[rss, heap_total, heap_used, external, open_file_bytes],
        )?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_rss(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let module_bytes = host
            .loaded_modules
            .borrow()
            .values()
            .map(|module| module.source.len())
            .sum::<usize>() as f64;
        let open_file_bytes = host
            .open_files
            .borrow()
            .entries
            .values()
            .map(|file| file.contents.len())
            .sum::<usize>() as f64;
        let process = host.process.borrow();
        let rss = module_bytes + open_file_bytes + process.stdout.len() as f64 + process.stderr.len() as f64;
        Ok(JsValue::from(rss))
    })
    .map_err(js_error)
}

fn node_process_methods_resource_usage(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let array = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("resourceUsage target must be an object"))?;
    node_with_host(|host| {
        let user_micros = node_monotonic_now_ms(host) * 1000.0;
        let max_rss = host
            .loaded_modules
            .borrow()
            .values()
            .map(|module| module.source.len())
            .sum::<usize>() as f64;
        let fs_reads = host.debug_trace.borrow().fs_calls as f64;
        let ctx_switches = host.debug_trace.borrow().load_calls as f64;
        node_set_array_like_values(
            context,
            &array,
            &[
                user_micros,
                0.0,
                max_rss,
                fs_reads,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                ctx_switches,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
        )?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_execve(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let command = node_arg_string(args, 0, context)?;
    let argv = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_json(context)?
        .unwrap_or(JsonValue::Null);
    let argv = serde_json::from_value::<Vec<String>>(argv)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    let env = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_json(context)?
        .unwrap_or(JsonValue::Null);
    let env = serde_json::from_value::<BTreeMap<String, String>>(env)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    node_with_host(|host| {
        let cwd = host.process.borrow().cwd.clone();
        let result = execute_direct_child_process(host, command.clone(), argv.clone(), cwd, env, None)?;
        if let Some(error) = result.error {
            return Err(SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: error.message,
            });
        }
        let mut process = host.process.borrow_mut();
        process.stdout.push_str(&result.stdout);
        process.stderr.push_str(&result.stderr);
        process.exit_code = result.status.unwrap_or_default();
        Err(SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("process.execve() replaced the current process with `{command}`"),
        })
    })
    .map_err(js_error)
}

fn node_process_methods_constrained_memory(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(512_f64 * 1024_f64 * 1024_f64))
}

fn node_process_methods_available_memory(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let module_bytes = host
            .loaded_modules
            .borrow()
            .values()
            .map(|module| module.source.len() as f64)
            .sum::<f64>();
        let open_file_bytes = host
            .open_files
            .borrow()
            .entries
            .values()
            .map(|file| file.contents.len() as f64)
            .sum::<f64>();
        let process = host.process.borrow();
        let used = module_bytes + open_file_bytes + process.stdout.len() as f64 + process.stderr.len() as f64;
        let total = 512_f64 * 1024_f64 * 1024_f64;
        Ok(JsValue::from((total - used).max(0.0)))
    })
    .map_err(js_error)
}

fn node_process_methods_load_env_file(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = if args
        .first()
        .is_some_and(|value| !value.is_null() && !value.is_undefined())
    {
        Some(node_arg_string(args, 0, context)?)
    } else {
        None
    };
    node_with_host(|host| {
        let resolved = path
            .as_deref()
            .map(|path| resolve_node_path(&host.process.borrow().cwd, path))
            .unwrap_or_else(|| resolve_node_path(&host.process.borrow().cwd, ".env"));
        let Some(bytes) = node_read_file(host, &resolved)? else {
            return Ok(JsValue::undefined());
        };
        let mut process = host.process.borrow_mut();
        for line in String::from_utf8_lossy(&bytes).lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = trimmed.split_once('=') {
                process
                    .env
                    .insert(key.trim().to_string(), value.trim().to_string());
            }
        }
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_abort(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.process.borrow_mut().exit_code = 134;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    Err(JsNativeError::error()
        .with_message("process.abort() terminated the sandboxed process")
        .into())
}

fn node_process_methods_cwd(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(JsString::from(host.process.borrow().cwd.clone()))))
        .map_err(js_error)
}

fn node_process_methods_chdir(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        let current_cwd = host.process.borrow().cwd.clone();
        let next = resolve_node_path(&current_cwd, &path);
        let stats = node_read_stat(host, &next)?;
        let Some(stats) = stats else {
            return Err(SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: format!("ENOENT: no such file or directory, chdir '{next}'"),
            });
        };
        if !matches!(stats.kind, FileKind::Directory) {
            return Err(SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: format!("ENOTDIR: not a directory, chdir '{next}'"),
            });
        }
        host.process.borrow_mut().cwd = next;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_umask(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let mut process = host.process.borrow_mut();
        let previous = process.umask;
        if args
            .first()
            .is_some_and(|value| !value.is_null() && !value.is_undefined())
        {
            process.umask = args
                .first()
                .cloned()
                .unwrap_or_else(JsValue::undefined)
                .to_u32(context)
                .map_err(sandbox_execution_error)?;
        }
        Ok(JsValue::from(previous))
    })
    .map_err(js_error)
}

fn node_process_methods_set_emit_warning_sync(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().emit_warning_sync = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_raw_debug(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let message = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    node_with_host(|host| {
        host.process.borrow_mut().stderr.push_str(&message);
        host.process.borrow_mut().stderr.push('\n');
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_debug_process(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let pid = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    node_with_host(|host| {
        let own_pid = host.process.borrow().pid as i32;
        if pid != own_pid && pid != 0 && pid != -1 {
            return Err(SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: format!("ESRCH: no such process, _debugProcess '{pid}'"),
            });
        }
        node_debug_event(host, "process", format!("debug_process pid={pid}"))?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_debug_end(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        node_debug_event(host, "process", "debug_end".to_string())?;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_process_methods_hrtime(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let elapsed_nanos = (node_monotonic_now_ms(host) * 1_000_000.0) as u128;
        let secs = (elapsed_nanos / 1_000_000_000) as u64;
        let buffer = node_process_methods_hrtime_buffer(host, context)?;
        node_set_array_like_entry(context, &buffer, 0, (secs >> 32) as u32)?;
        node_set_array_like_entry(context, &buffer, 1, (secs & 0xffff_ffff) as u32)?;
        node_set_array_like_entry(context, &buffer, 2, (elapsed_nanos % 1_000_000_000) as u32)?;
        Ok(JsValue::undefined())
    })
}

fn node_process_methods_hrtime_bigint(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let nanos = (node_monotonic_now_ms(host) * 1_000_000.0) as u128;
        context
            .eval(Source::from_bytes(format!("BigInt(\"{nanos}\")").as_bytes()))
            .map_err(sandbox_execution_error)
    })
}

fn node_task_queue_run_microtasks(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        node_run_microtask_checkpoint(context, host)?;
        Ok(JsValue::undefined())
    })
}

fn node_task_queue_set_tick_callback(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().tick_callback = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_task_queue_enqueue_microtask(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("microtask callback must be callable"))?;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().microtask_queue.push(callback);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_task_queue_set_promise_reject_callback(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().promise_reject_callback = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_timers_setup_timers(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let process_immediate = args.first().and_then(JsValue::as_object);
    let process_timers = args.get(1).and_then(JsValue::as_object);
    node_with_host(|host| {
        let mut bootstrap = host.bootstrap.borrow_mut();
        bootstrap.process_immediate_callback = process_immediate;
        bootstrap.process_timers_callback = process_timers;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_timers_schedule_timer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let duration = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)? as i64;
    node_with_host(|host| {
        let now = node_monotonic_now_ms(host);
        let due = now + duration.max(0) as f64;
        host.bootstrap.borrow_mut().next_timer_due_ms = Some(due);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_timers_toggle_timer_ref(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let is_refed = args.first().is_some_and(JsValue::to_boolean);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().timer_is_refed = is_refed;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_timers_toggle_immediate_ref(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let is_refed = args.first().is_some_and(JsValue::to_boolean);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().immediate_is_refed = is_refed;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_timers_get_libuv_now(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| Ok(JsValue::from(node_monotonic_now_ms(host))))
        .map_err(js_error)
}

fn node_async_wrap_set_callback_trampoline(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().async_callback_trampoline = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_async_wrap_register_destroy_hook(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let async_id = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)?
        .max(0.0) as u64;
    node_with_host(|host| {
        host.bootstrap
            .borrow_mut()
            .registered_destroy_async_ids
            .insert(async_id);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_async_wrap_queue_destroy_async_id(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let async_id = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)?
        .max(0.0) as u64;
    let destroy = node_with_host(|host| {
        let mut state = host.bootstrap.borrow_mut();
        let should_emit = state.registered_destroy_async_ids.remove(&async_id);
        let destroy = state
            .async_hooks_object
            .as_ref()
            .and_then(|hooks| hooks.get(js_string!("destroy"), context).ok())
            .and_then(|value| value.as_object());
        Ok((should_emit, destroy))
    })
    .map_err(js_error)?;
    if let (true, Some(callback)) = destroy {
        let callable = JsValue::from(callback)
            .as_callable()
            .ok_or_else(|| JsNativeError::typ().with_message("async_hooks.destroy must be callable"))?;
        let _ = callable.call(
            &JsValue::undefined(),
            &[JsValue::from(async_id as f64)],
            context,
        )?;
    }
    Ok(JsValue::undefined())
}

fn node_async_wrap_set_promise_hooks(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let mut bootstrap = host.bootstrap.borrow_mut();
        bootstrap.promise_hook_init = args.first().and_then(JsValue::as_object);
        bootstrap.promise_hook_before = args.get(1).and_then(JsValue::as_object);
        bootstrap.promise_hook_after = args.get(2).and_then(JsValue::as_object);
        bootstrap.promise_hook_settled = args.get(3).and_then(JsValue::as_object);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_async_wrap_get_promise_hooks(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let state = host.bootstrap.borrow();
        let values = [
            state
                .promise_hook_init
                .clone()
                .map(JsValue::from)
                .unwrap_or_else(JsValue::undefined),
            state
                .promise_hook_before
                .clone()
                .map(JsValue::from)
                .unwrap_or_else(JsValue::undefined),
            state
                .promise_hook_after
                .clone()
                .map(JsValue::from)
                .unwrap_or_else(JsValue::undefined),
            state
                .promise_hook_settled
                .clone()
                .map(JsValue::from)
                .unwrap_or_else(JsValue::undefined),
        ];
        Ok(JsValue::from(JsArray::from_iter(values, context)))
    })
}

fn node_async_wrap_setup_hooks(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let hooks = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().async_hooks_object = hooks;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_async_wrap_push_async_context(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let binding = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("async_wrap receiver must be an object"))?;
    let async_hook_fields = node_get_object_property_as_object(context, &binding, "async_hook_fields")
        .map_err(js_error)?;
    let async_id_fields = node_get_object_property_as_object(context, &binding, "async_id_fields")
        .map_err(js_error)?;
    let async_ids_stack = node_get_object_property_as_object(context, &binding, "async_ids_stack")
        .map_err(js_error)?;
    let resources = node_get_object_property_as_object(context, &binding, "execution_async_resources")
        .map_err(js_error)?;

    let async_id = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)?;
    let trigger_async_id = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)?;
    let resource = args.get(2).cloned().unwrap_or_else(JsValue::undefined);
    let stack_length = async_hook_fields.get(4u32, context)?.to_length(context)? as u32;
    let execution_async_id = async_id_fields.get(0u32, context)?.to_number(context)?;
    let current_trigger_async_id = async_id_fields.get(1u32, context)?.to_number(context)?;

    resources.set(stack_length, resource.clone(), true, context)?;
    node_set_array_like_entry(context, &async_ids_stack, stack_length * 2, execution_async_id)
        .map_err(js_error)?;
    node_set_array_like_entry(
        context,
        &async_ids_stack,
        stack_length * 2 + 1,
        current_trigger_async_id,
    )
    .map_err(js_error)?;
    node_set_array_like_entry(context, &async_hook_fields, 4, stack_length + 1)
        .map_err(js_error)?;
    node_set_array_like_entry(context, &async_id_fields, 0, async_id).map_err(js_error)?;
    node_set_array_like_entry(context, &async_id_fields, 1, trigger_async_id)
        .map_err(js_error)?;
    node_with_host(|host| {
        let state = &mut *host.bootstrap.borrow_mut();
        state.execution_async_id = async_id.max(0.0) as u64;
        state.trigger_async_id = trigger_async_id.max(0.0) as u64;
        state.execution_async_resources.push(resource);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    Ok(JsValue::from(stack_length + 1))
}

fn node_async_wrap_pop_async_context(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let binding = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("async_wrap receiver must be an object"))?;
    let async_hook_fields = node_get_object_property_as_object(context, &binding, "async_hook_fields")
        .map_err(js_error)?;
    let async_id_fields = node_get_object_property_as_object(context, &binding, "async_id_fields")
        .map_err(js_error)?;
    let async_ids_stack = node_get_object_property_as_object(context, &binding, "async_ids_stack")
        .map_err(js_error)?;
    let resources = node_get_object_property_as_object(context, &binding, "execution_async_resources")
        .map_err(js_error)?;

    let expected_async_id = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)?;
    let stack_length = async_hook_fields.get(4u32, context)?.to_length(context)? as u32;
    if stack_length == 0 {
        return Ok(JsValue::from(false));
    }

    let current_execution_async_id = async_id_fields.get(0u32, context)?.to_number(context)?;
    if current_execution_async_id != expected_async_id {
        return Err(JsNativeError::error()
            .with_message("async context stack corruption detected")
            .into());
    }

    let offset = stack_length - 1;
    let previous_execution_async_id = async_ids_stack.get(offset * 2, context)?.to_number(context)?;
    let previous_trigger_async_id =
        async_ids_stack.get(offset * 2 + 1, context)?.to_number(context)?;
    node_set_array_like_entry(context, &async_id_fields, 0, previous_execution_async_id)
        .map_err(js_error)?;
    node_set_array_like_entry(context, &async_id_fields, 1, previous_trigger_async_id)
        .map_err(js_error)?;
    node_set_array_like_entry(context, &async_hook_fields, 4, offset).map_err(js_error)?;
    node_with_host(|host| {
        let state = &mut *host.bootstrap.borrow_mut();
        state.execution_async_id = previous_execution_async_id.max(0.0) as u64;
        state.trigger_async_id = previous_trigger_async_id.max(0.0) as u64;
        let _ = state.execution_async_resources.pop();
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    let _ = resources
        .get(js_string!("pop"), context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("execution_async_resources.pop is not callable"))?
        .call(&JsValue::from(resources.clone()), &[], context)?;
    Ok(JsValue::from(offset > 0))
}

fn node_async_wrap_execution_async_resource(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let binding = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("async_wrap receiver must be an object"))?;
    let resources = node_get_object_property_as_object(context, &binding, "execution_async_resources")
        .map_err(js_error)?;
    let index = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as u32;
    resources.get(index, context).map_err(sandbox_execution_error).map_err(js_error)
}

fn node_async_wrap_clear_async_id_stack(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let binding = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("async_wrap receiver must be an object"))?;
    let async_hook_fields = node_get_object_property_as_object(context, &binding, "async_hook_fields")
        .map_err(js_error)?;
    let async_id_fields = node_get_object_property_as_object(context, &binding, "async_id_fields")
        .map_err(js_error)?;
    let resources = node_get_object_property_as_object(context, &binding, "execution_async_resources")
        .map_err(js_error)?;
    node_set_array_like_entry(context, &async_hook_fields, 4, 0).map_err(js_error)?;
    node_set_array_like_entry(context, &async_id_fields, 0, 0).map_err(js_error)?;
    node_set_array_like_entry(context, &async_id_fields, 1, 0).map_err(js_error)?;
    node_with_host(|host| {
        let state = &mut *host.bootstrap.borrow_mut();
        state.execution_async_id = 0;
        state.trigger_async_id = 0;
        state.execution_async_resources.clear();
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    resources.set(js_string!("length"), JsValue::from(0), true, context)?;
    Ok(JsValue::undefined())
}

fn node_async_context_frame_get(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        Ok(host
            .bootstrap
            .borrow()
            .async_context_frame
            .clone()
            .unwrap_or_else(JsValue::undefined))
    })
    .map_err(js_error)
}

fn node_async_context_frame_set(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let value = args.first().cloned().unwrap_or_else(JsValue::undefined);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().async_context_frame = Some(value);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_contextify_private_symbol(
    context: &mut Context,
    name: &str,
) -> Result<JsSymbol, SandboxError> {
    let util_binding = node_internal_binding_util(context)?;
    let private_symbols = util_binding
        .get(js_string!("privateSymbols"), context)
        .map_err(sandbox_execution_error)?
        .as_object()
        .ok_or_else(|| sandbox_execution_error("util.privateSymbols must be an object"))?;
    private_symbols
        .get(JsString::from(name), context)
        .map_err(sandbox_execution_error)?
        .as_symbol()
        .ok_or_else(|| sandbox_execution_error(format!("private symbol `{name}` is missing")))
}

fn node_contextify_attach_host_defined_option(
    target: &JsObject,
    host_defined_option_id: Option<JsValue>,
    context: &mut Context,
) -> Result<(), SandboxError> {
    let Some(host_defined_option_id) = host_defined_option_id else {
        return Ok(());
    };
    let symbol = node_contextify_private_symbol(context, "host_defined_option_symbol")?;
    target
        .set(symbol, host_defined_option_id, true, context)
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_contextify_mark_context_object(
    target: &JsObject,
    name: &str,
    origin: Option<String>,
    allow_code_gen_strings: bool,
    allow_code_gen_wasm: bool,
    own_microtask_queue: bool,
    host_defined_option_id: Option<JsValue>,
    context: &mut Context,
) -> Result<(), SandboxError> {
    let marker = node_contextify_private_symbol(context, "contextify_context_private_symbol")?;
    let metadata = JsValue::from_json(
        &serde_json::json!({
            "name": name,
            "origin": origin,
            "allowCodeGenerationFromStrings": allow_code_gen_strings,
            "allowCodeGenerationFromWasm": allow_code_gen_wasm,
            "ownMicrotaskQueue": own_microtask_queue,
        }),
        context,
    )
    .map_err(sandbox_execution_error)?;
    target
        .set(marker, metadata, true, context)
        .map_err(sandbox_execution_error)?;
    node_contextify_attach_host_defined_option(target, host_defined_option_id, context)?;
    target
        .set(js_string!("global"), JsValue::from(target.clone()), true, context)
        .map_err(sandbox_execution_error)?;
    target
        .set(js_string!("globalThis"), JsValue::from(target.clone()), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(())
}

fn node_contextify_sync_context_into_global(
    sandbox: &JsObject,
    global: &JsObject,
    context: &mut Context,
) -> Result<Vec<(PropertyKey, Option<JsValue>)>, SandboxError> {
    let mut previous = Vec::new();
    for key in sandbox.own_property_keys(context).map_err(sandbox_execution_error)? {
        let current = global.get(key.clone(), context).map_err(sandbox_execution_error)?;
        let had_current = !current.is_undefined() || global.has_property(key.clone(), context).map_err(sandbox_execution_error)?;
        let next = sandbox.get(key.clone(), context).map_err(sandbox_execution_error)?;
        previous.push((key.clone(), had_current.then_some(current)));
        global
            .set(key, next, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(previous)
}

fn node_contextify_sync_global_back_into_context(
    sandbox: &JsObject,
    global: &JsObject,
    previous: &[(PropertyKey, Option<JsValue>)],
    context: &mut Context,
) -> Result<(), SandboxError> {
    for key in sandbox.own_property_keys(context).map_err(sandbox_execution_error)? {
        let value = global.get(key.clone(), context).map_err(sandbox_execution_error)?;
        sandbox
            .set(key, value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    for (key, maybe_previous) in previous.iter().cloned().rev() {
        if let Some(value) = maybe_previous {
            global
                .set(key, value, true, context)
                .map_err(sandbox_execution_error)?;
        } else {
            global
                .delete_property_or_throw(key, context)
                .map_err(sandbox_execution_error)?;
        }
    }
    Ok(())
}

fn node_contextify_contains_module_syntax(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let source = node_arg_string(args, 0, context)?;
    let filename = args
        .get(1)
        .map(|value| value.to_string(context).map(|s| s.to_std_string_escaped()))
        .transpose()?
        .unwrap_or_else(|| "evalmachine.<anonymous>".to_string());
    let resource_name = args
        .get(2)
        .map(|value| value.to_string(context).map(|s| s.to_std_string_escaped()))
        .transpose()?
        .unwrap_or_else(|| filename.clone());
    let is_cjs_scope = !args.get(3).is_some_and(JsValue::is_string);
    let module_parses = Module::parse(
        Source::from_bytes(source.as_bytes()).with_path(&PathBuf::from(resource_name)),
        None,
        context,
    )
    .is_ok();
    let cjs_probe_source = if is_cjs_scope {
        format!(
            "(function (exports, require, module, __filename, __dirname) {{\n{}\n}})",
            source
        )
    } else {
        source.clone()
    };
    let script_parses = Script::parse(
        Source::from_bytes(cjs_probe_source.as_bytes()).with_path(&PathBuf::from(filename)),
        None,
        context,
    )
    .is_ok();
    Ok(JsValue::from(module_parses && !script_parses))
}

fn node_contextify_make_context(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let sandbox_or_sentinel = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let name = node_arg_string(args, 1, context)?;
    let origin = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined);
    let origin = if origin.is_undefined() || origin.is_null() {
        None
    } else {
        Some(origin.to_string(context)?.to_std_string_escaped())
    };
    let allow_code_gen_strings = args
        .get(3)
        .map(JsValue::to_boolean)
        .unwrap_or(true);
    let allow_code_gen_wasm = args
        .get(4)
        .map(JsValue::to_boolean)
        .unwrap_or(true);
    let own_microtask_queue = args.get(5).cloned().unwrap_or_else(JsValue::undefined).to_boolean();
    let host_defined_option_id = args.get(6).cloned();

    let target = if let Some(sandbox) = sandbox_or_sentinel.as_object() {
        sandbox
    } else {
        ObjectInitializer::new(context).build()
    };
    node_contextify_mark_context_object(
        &target,
        &name,
        origin,
        allow_code_gen_strings,
        allow_code_gen_wasm,
        own_microtask_queue,
        host_defined_option_id,
        context,
    )
    .map_err(js_error)?;
    Ok(JsValue::from(target))
}

fn node_contextify_start_sigint_watchdog(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let mut state = host.bootstrap.borrow_mut();
        let already_active = state.sigint_watchdog_active;
        state.sigint_watchdog_active = true;
        Ok(JsValue::from(!already_active))
    })
    .map_err(js_error)
}

fn node_contextify_stop_sigint_watchdog(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        host.bootstrap.borrow_mut().sigint_watchdog_active = false;
        Ok(JsValue::from(false))
    })
    .map_err(js_error)
}

fn node_contextify_watchdog_has_pending_sigint(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(false))
}

fn node_contextify_measure_memory(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let mode = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or(0);
    let payload = node_with_host(|host| {
        let module_bytes = host
            .loaded_modules
            .borrow()
            .values()
            .map(|module| module.source.len())
            .sum::<usize>() as u64;
        let file_bytes = host
            .open_files
            .borrow()
            .entries
            .values()
            .map(|file| file.contents.len())
            .sum::<usize>() as u64;
        let process = host.process.borrow();
        let output_bytes = (process.stdout.len() + process.stderr.len()) as u64;
        let total = module_bytes + file_bytes + output_bytes;
        let range_hi = total.saturating_add((total / 8).max(1024));
        let payload = if mode == 1 {
            serde_json::json!({
                "total": {
                    "jsMemoryEstimate": total,
                    "jsMemoryRange": [total, range_hi],
                },
                "current": {
                    "jsMemoryEstimate": module_bytes + file_bytes,
                    "jsMemoryRange": [module_bytes + file_bytes, range_hi],
                },
                "other": [
                    {
                        "jsMemoryEstimate": output_bytes,
                        "jsMemoryRange": [output_bytes, output_bytes.saturating_add(1024)],
                    }
                ]
            })
        } else {
            serde_json::json!({
                "total": {
                    "jsMemoryEstimate": total,
                    "jsMemoryRange": [total, range_hi],
                }
            })
        };
        Ok(payload)
    })
    .map_err(js_error)?;
    let (promise, resolvers) = JsPromise::new_pending(context);
    context.enqueue_job(
        NativeAsyncJob::new(async move |context_cell| {
            let context = &mut context_cell.borrow_mut();
            let value = JsValue::from_json(&payload, context)
                .map_err(sandbox_execution_error)
                .map_err(js_error)?;
            resolvers
                .resolve
                .call(&JsValue::undefined(), &[value], context)?;
            Ok(JsValue::undefined())
        })
        .into(),
    );
    Ok(JsValue::from(promise))
}

fn node_contextify_compile_cache_bytes(
    kind: &str,
    content: &str,
    filename: &str,
    line_offset: i32,
    column_offset: i32,
    params: &[String],
) -> Result<Vec<u8>, SandboxError> {
    serde_json::to_vec(&serde_json::json!({
        "kind": kind,
        "content": content,
        "filename": filename,
        "lineOffset": line_offset,
        "columnOffset": column_offset,
        "params": params,
    }))
    .map_err(sandbox_execution_error)
}

fn node_js_buffer_from_bytes(bytes: Vec<u8>, context: &mut Context) -> JsResult<JsValue> {
    let buffer_ctor = context.global_object().get(js_string!("Buffer"), context)?;
    if let Some(buffer_ctor) = buffer_ctor.as_object() {
        let from = buffer_ctor.get(js_string!("from"), context)?;
        if let Some(from) = from.as_callable() {
            let view = JsUint8Array::from_iter(bytes, context)?;
            return from.call(&JsValue::from(buffer_ctor), &[JsValue::from(view)], context);
        }
    }
    Ok(JsValue::from(
        JsUint8Array::from_iter(bytes, context)?
            .buffer(context)?
            .clone(),
    ))
}

fn node_contextify_array_like_strings(
    value: Option<&JsValue>,
    context: &mut Context,
) -> Result<Vec<String>, SandboxError> {
    let Some(array) = value.and_then(JsValue::as_object) else {
        return Ok(Vec::new());
    };
    let length = array
        .get(js_string!("length"), context)
        .map_err(sandbox_execution_error)?
        .to_length(context)
        .map_err(sandbox_execution_error)?;
    let mut values = Vec::with_capacity(length as usize);
    for index in 0..length {
        values.push(
            array
                .get(index, context)
                .map_err(sandbox_execution_error)?
                .to_string(context)
                .map_err(sandbox_execution_error)?
                .to_std_string_escaped(),
        );
    }
    Ok(values)
}

fn node_contextify_array_like_objects(
    value: Option<&JsValue>,
    context: &mut Context,
) -> Result<Vec<JsObject>, SandboxError> {
    let Some(array) = value.and_then(JsValue::as_object) else {
        return Ok(Vec::new());
    };
    let length = array
        .get(js_string!("length"), context)
        .map_err(sandbox_execution_error)?
        .to_length(context)
        .map_err(sandbox_execution_error)?;
    let mut values = Vec::with_capacity(length as usize);
    for index in 0..length {
        let value = array
            .get(index, context)
            .map_err(sandbox_execution_error)?
            .as_object()
            .ok_or_else(|| sandbox_execution_error(format!("contextExtensions[{index}] must be an object")))?;
        values.push(value);
    }
    Ok(values)
}

fn node_contextify_cached_data_bytes(
    value: Option<&JsValue>,
    context: &mut Context,
) -> Result<Option<Vec<u8>>, SandboxError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    node_bytes_from_js(value, context).map(Some).map_err(sandbox_execution_error)
}

fn node_contextify_compile_function_for_cjs_loader(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let content = node_arg_string(args, 0, context)?;
    let filename = node_arg_string(args, 1, context)?;
    let _is_sea_main = args.get(2).is_some_and(JsValue::to_boolean);
    let should_detect_module = args
        .get(3)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_boolean();
    let (annotated_source_url, source_map_url) = node_extract_source_annotations(&content);
    let source_url = annotated_source_url.unwrap_or_else(|| filename.clone());
    let can_parse_as_esm = if should_detect_module {
        node_contextify_contains_module_syntax(
            &JsValue::undefined(),
            &[JsValue::from(JsString::from(content.clone())), JsValue::from(JsString::from(filename.clone()))],
            context,
        )?
        .to_boolean()
    } else {
        false
    };
    let wrapped = format!(
        "(function (exports, require, module, __filename, __dirname) {{\n{}\n//# sourceURL={}\n}})",
        content,
        filename.replace('\\', "\\\\")
    );
    let function = match context.eval(
        Source::from_bytes(wrapped.as_bytes()).with_path(&PathBuf::from(filename.clone())),
    ) {
        Ok(value) => Some(value),
        Err(error) => {
            if should_detect_module && can_parse_as_esm {
                None
            } else {
                return Err(js_error(sandbox_execution_error(error)));
            }
        }
    };
    let result = JsObject::with_null_proto();
    result
        .set(
            js_string!("cachedDataRejected"),
            JsValue::from(false),
            true,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    result
        .set(
            js_string!("sourceMapURL"),
            source_map_url
                .map(|value| JsValue::from(JsString::from(value)))
                .unwrap_or_else(JsValue::undefined),
            true,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    result
        .set(
            js_string!("sourceURL"),
            JsValue::from(JsString::from(source_url)),
            true,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    result
        .set(
            js_string!("function"),
            function.clone().unwrap_or_else(JsValue::undefined),
            true,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    result
        .set(
            js_string!("canParseAsESM"),
            JsValue::from(can_parse_as_esm),
            true,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    if let Some(function_object) = function.and_then(|value| value.as_object()) {
        let symbols = node_internal_binding_symbols(context).map_err(js_error)?;
        let host_defined_option_id = symbols
            .get(js_string!("vm_dynamic_import_default_internal"), context)
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
        node_contextify_attach_host_defined_option(
            &function_object,
            Some(host_defined_option_id),
            context,
        )
        .map_err(js_error)?;
    }
    Ok(JsValue::from(result))
}

fn node_contextify_compile_function(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let content = node_arg_string(args, 0, context)?;
    let filename = node_arg_string(args, 1, context)?;
    let line_offset = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    let column_offset = args
        .get(3)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    let cached_data =
        node_contextify_cached_data_bytes(args.get(4), context).map_err(js_error)?;
    let produce_cached_data = args.get(5).is_some_and(JsValue::to_boolean);
    let params = node_contextify_array_like_strings(args.get(8), context).map_err(js_error)?;
    let context_extensions =
        node_contextify_array_like_objects(args.get(7), context).map_err(js_error)?;
    let (source_url, source_map_url) = node_extract_source_annotations(&content);
    let parsing_context = if let Some(parsing_context) = args.get(6).and_then(JsValue::as_object) {
        let marker = node_contextify_private_symbol(context, "contextify_context_private_symbol")
            .map_err(js_error)?;
        let is_context = parsing_context
            .get(marker, context)
            .map_err(sandbox_execution_error)
            .map_err(js_error)?
            .is_object();
        if !is_context {
            return Err(JsNativeError::typ()
                .with_message("parsingContext must be a contextified object")
                .into());
        }
        Some(parsing_context)
    } else {
        None
    };
    let cache_bytes = node_contextify_compile_cache_bytes(
        "function",
        &content,
        &filename,
        line_offset,
        column_offset,
        &params,
    )
    .map_err(js_error)?;
    let cached_data_rejected = cached_data
        .as_ref()
        .map(|cached| *cached != cache_bytes)
        .unwrap_or(false);
    let global = context.global_object().clone();
    let previous = if let Some(parsing_context) = parsing_context.as_ref() {
        Some(
            node_contextify_sync_context_into_global(parsing_context, &global, context)
                .map_err(js_error)?,
        )
    } else {
        None
    };
    let padding = "\n".repeat(line_offset.max(0) as usize);
    let prefix = " ".repeat(column_offset.max(0) as usize);
    let extension_params = (0..context_extensions.len())
        .map(|index| format!("__ctxExt{index}"))
        .collect::<Vec<_>>();
    let with_prefix = extension_params
        .iter()
        .map(|name| format!("with ({name}) {{\n"))
        .collect::<String>();
    let with_suffix = "}\n".repeat(extension_params.len());
    let wrapped = format!(
        "(function({}) {{\n{}return function({}) {{\n{}{prefix}{}\n//# sourceURL={}\n}};\n{}}})",
        extension_params.join(", "),
        with_prefix,
        params.join(", "),
        padding,
        content,
        filename.replace('\\', "\\\\"),
        with_suffix,
    );
    let factory = context
        .eval(Source::from_bytes(wrapped.as_bytes()).with_path(&PathBuf::from(filename.clone())))
        .map_err(sandbox_execution_error)
        .map_err(js_error);
    if let (Some(parsing_context), Some(previous)) = (parsing_context.as_ref(), previous.as_ref()) {
        node_contextify_sync_global_back_into_context(parsing_context, &global, previous, context)
            .map_err(js_error)?;
    }
    let factory = factory?;
    let factory = factory
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("compileFunction factory is not callable"))?;
    let function = factory
        .call(
            &JsValue::undefined(),
            &context_extensions
                .iter()
                .cloned()
                .map(JsValue::from)
                .collect::<Vec<_>>(),
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    let result = JsObject::with_null_proto();
    result
        .set(js_string!("function"), function.clone(), true, context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    result
        .set(
            js_string!("sourceURL"),
            JsValue::from(JsString::from(source_url.unwrap_or_else(|| filename.clone()))),
            true,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    result
        .set(
            js_string!("sourceMapURL"),
            source_map_url
                .map(|value| JsValue::from(JsString::from(value)))
                .unwrap_or_else(JsValue::undefined),
            true,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    if let Some(_cached) = cached_data.as_ref() {
        result
            .set(
                js_string!("cachedDataRejected"),
                JsValue::from(cached_data_rejected),
                true,
                context,
            )
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
    }
    if produce_cached_data {
        result
            .set(js_string!("cachedDataProduced"), JsValue::from(true), true, context)
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
        result
            .set(
                js_string!("cachedData"),
                node_js_buffer_from_bytes(cache_bytes.clone(), context)?,
                true,
                context,
            )
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
    }
    if let Some(function_object) = result
        .get(js_string!("function"), context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .as_object()
    {
        node_contextify_attach_host_defined_option(
            &function_object,
            args.get(9).cloned(),
            context,
        )
        .map_err(js_error)?;
    }
    Ok(JsValue::from(result))
}

fn node_contextify_script_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let id_symbol = node_host_private_symbol(&host, "contextify_script.id").map_err(js_error)?;
    let cached_data_symbol =
        node_host_private_symbol(&host, "contextify.cached_data").map_err(js_error)?;
    let target = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("ContextifyScript receiver must be an object"))?;
    let code = node_arg_string(args, 0, context)?;
    let filename = node_arg_string(args, 1, context)?;
    let line_offset = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    let column_offset = args
        .get(3)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    let cached_data =
        node_contextify_cached_data_bytes(args.get(4), context).map_err(js_error)?;
    let produce_cached_data = args.get(5).is_some_and(JsValue::to_boolean);
    let host_defined_option_id = args.get(7).cloned();
    let (annotated_source_url, source_map_url) = node_extract_source_annotations(&code);
    let source_url = annotated_source_url.unwrap_or_else(|| filename.clone());
    let cache_bytes = node_contextify_compile_cache_bytes(
        "script",
        &code,
        &filename,
        line_offset,
        column_offset,
        &[],
    )
    .map_err(js_error)?;
    let cached_data_rejected = cached_data
        .as_ref()
        .map(|cached| *cached != cache_bytes)
        .unwrap_or(false);
    let script = Script::parse(
        Source::from_bytes(code.as_bytes()).with_path(&PathBuf::from(filename.clone())),
        None,
        context,
    )
    .map_err(sandbox_execution_error)
    .map_err(js_error)?;
    for (key, value) in [
        (PropertyKey::from(js_string!("sourceURL")), JsValue::from(JsString::from(source_url))),
        (
            PropertyKey::from(js_string!("sourceMapURL")),
            source_map_url
                .map(|value| JsValue::from(JsString::from(value)))
                .unwrap_or_else(JsValue::undefined),
        ),
    ] {
        target
            .set(key, value, true, context)
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
    }
    if cached_data.is_some() {
        target
            .set(
                PropertyKey::from(js_string!("cachedDataRejected")),
                JsValue::from(cached_data_rejected),
                true,
                context,
            )
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
    }
    if produce_cached_data {
        let cached_data_value = node_js_buffer_from_bytes(cache_bytes.clone(), context)?;
        target
            .set(
                PropertyKey::from(cached_data_symbol.clone()),
                cached_data_value.clone(),
                true,
                context,
            )
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
        target
            .set(
                PropertyKey::from(js_string!("cachedDataProduced")),
                JsValue::from(true),
                true,
                context,
            )
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
        target
            .set(
                PropertyKey::from(js_string!("cachedData")),
                cached_data_value,
                true,
                context,
            )
            .map_err(sandbox_execution_error)
            .map_err(js_error)?;
    }
    node_contextify_attach_host_defined_option(&target, host_defined_option_id, context)
        .map_err(js_error)?;
    node_with_host_js(context, |host, context| {
        let id = node_next_host_handle_id(host);
        target
            .set(id_symbol, JsValue::from(id as f64), true, context)
            .map_err(sandbox_execution_error)?;
        host.bootstrap.borrow_mut().contextify_scripts.insert(
            id,
            NodeContextifyScriptState {
                script,
                filename,
                cached_data: target.get(cached_data_symbol, context).ok().filter(|v| !v.is_undefined()),
            },
        );
        Ok(JsValue::undefined())
    })?;
    Ok(JsValue::undefined())
}

fn node_contextify_script_run_in_context(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let id_symbol = node_host_private_symbol(&host, "contextify_script.id").map_err(js_error)?;
    let target = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("ContextifyScript receiver must be an object"))?;
    let timeout = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| JsValue::from(-1))
        .to_number(context)
        .unwrap_or(-1.0) as i64;
    let display_errors = args.get(2).is_none_or(JsValue::to_boolean);
    let break_on_sigint = args.get(3).is_some_and(JsValue::to_boolean);
    let _break_on_first_line = args.get(4).is_some_and(JsValue::to_boolean);
    let sandbox = if let Some(sandbox) = args.first().filter(|value| !value.is_null() && !value.is_undefined()) {
        let sandbox = sandbox
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("sandbox must be a contextified object"))?;
        let marker = node_contextify_private_symbol(context, "contextify_context_private_symbol")
            .map_err(js_error)?;
        let is_context = sandbox
            .get(marker, context)
            .map_err(sandbox_execution_error)
            .map_err(js_error)?
            .is_object();
        if !is_context {
            return Err(JsNativeError::typ()
                .with_message("sandbox must be a contextified object")
                .into());
        }
        Some(sandbox)
    } else {
        None
    };
    let script_id = target
        .get(id_symbol, context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .to_number(context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)? as u64;
    let script_state = host
        .bootstrap
        .borrow()
        .contextify_scripts
        .get(&script_id)
        .cloned()
        .ok_or_else(|| JsNativeError::error().with_message("unknown ContextifyScript handle"))?;
    let global = context.global_object().clone();
    let previous = if let Some(sandbox) = sandbox.as_ref() {
        Some(
            node_contextify_sync_context_into_global(sandbox, &global, context)
                .map_err(js_error)?,
        )
    } else {
        None
    };
    let started_ms = node_with_host(|host| Ok(node_monotonic_now_ms(host))).map_err(js_error)?;
    let result = script_state
        .script
        .evaluate(context)
        .map_err(sandbox_execution_error)
        .map_err(js_error);
    if let (Some(sandbox), Some(previous)) = (sandbox.as_ref(), previous.as_ref()) {
        node_contextify_sync_global_back_into_context(sandbox, &global, previous, context)
            .map_err(js_error)?;
    }
    let result = match result {
        Ok(result) => result,
        Err(error) if display_errors => return Err(error),
        Err(error) => return Err(error),
    };
    let finished_ms = node_with_host(|host| Ok(node_monotonic_now_ms(host))).map_err(js_error)?;
    if timeout >= 0 && finished_ms - started_ms > timeout as f64 {
        return Err(JsNativeError::error()
            .with_message(format!("Script execution timed out after {timeout}ms"))
            .into());
    }
    if break_on_sigint
        && node_contextify_watchdog_has_pending_sigint(
            &JsValue::undefined(),
            &[],
            context,
        )?
        .to_boolean()
    {
        return Err(JsNativeError::error()
            .with_message("Script execution was interrupted by SIGINT")
            .into());
    }
    let microtask_mode = sandbox
        .as_ref()
        .unwrap_or(&target)
        .get(
            node_contextify_private_symbol(context, "contextify_context_private_symbol")
                .map_err(js_error)?,
            context,
        )
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    let run_microtasks = microtask_mode
        .as_object()
        .and_then(|metadata| metadata.get(js_string!("ownMicrotaskQueue"), context).ok())
        .is_some_and(|value| value.to_boolean());
    if run_microtasks {
        context.run_jobs().map_err(sandbox_execution_error).map_err(js_error)?;
    }
    Ok(result)
}

fn node_contextify_script_create_cached_data(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let target = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("ContextifyScript receiver must be an object"))?;
    let host = active_node_host().map_err(js_error)?;
    let id_symbol = node_host_private_symbol(&host, "contextify_script.id").map_err(js_error)?;
    let script_id = target
        .get(id_symbol, context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .to_number(context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)? as u64;
    let cached_data = host
        .bootstrap
        .borrow()
        .contextify_scripts
        .get(&script_id)
        .and_then(|state| state.cached_data.clone())
        .unwrap_or_else(JsValue::undefined);
    if cached_data.is_undefined() {
        return node_js_buffer_from_bytes(Vec::new(), context);
    }
    Ok(cached_data)
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

fn node_errors_set_prepare_stack_trace_callback(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().prepare_stack_trace_callback = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_errors_set_source_maps_enabled(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let enabled = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_boolean();
    node_with_host(|host| {
        host.bootstrap.borrow_mut().source_maps_enabled = enabled;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_errors_set_maybe_cache_generated_source_map(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().maybe_cache_generated_source_map_callback = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_errors_set_enhance_stack_for_fatal_exception(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        let mut state = host.bootstrap.borrow_mut();
        state.enhance_stack_before_inspector = args.first().and_then(JsValue::as_object);
        state.enhance_stack_after_inspector = args.get(1).and_then(JsValue::as_object);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_profiler_set_coverage_directory(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let directory = if args
        .first()
        .is_some_and(|value| !value.is_null() && !value.is_undefined())
    {
        Some(node_arg_string(args, 0, context)?)
    } else {
        None
    };
    node_with_host(|host| {
        host.bootstrap.borrow_mut().profiler_coverage_directory = directory;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_profiler_set_source_map_cache_getter(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap.borrow_mut().profiler_source_map_cache_getter = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
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

fn node_buffer_set_buffer_prototype(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let prototype = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("buffer prototype must be an object"))?;
    node_with_host(|host| {
        host.bootstrap.borrow_mut().buffer_prototype = Some(prototype);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_buffer_uint8_view_from_value(
    value: &JsValue,
    context: &mut Context,
) -> JsResult<JsUint8Array> {
    let object = value
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("value must be an object"))?;
    if let Ok(view) = JsUint8Array::from_object(object.clone()) {
        return Ok(view);
    }
    node_buffer_uint8_view_from_buffer(object, context)
}

fn node_buffer_length(value: &JsValue, context: &mut Context) -> JsResult<usize> {
    node_buffer_uint8_view_from_value(value, context)?.length(context)
}

fn node_buffer_bytes(value: &JsValue, context: &mut Context) -> JsResult<Vec<u8>> {
    node_buffer_uint8_view_from_value(value, context)?.to_vec(context)
}

fn node_buffer_bytes_range(
    value: &JsValue,
    start: usize,
    end: usize,
    context: &mut Context,
) -> JsResult<Vec<u8>> {
    let view = node_buffer_uint8_view_from_value(value, context)?;
    let length = view.length(context)?;
    let start = start.min(length);
    let end = end.min(length).max(start);
    let subarray = view.subarray(start as i64, end as i64, context)?;
    let subarray = JsUint8Array::from_object(subarray.into())?;
    subarray.to_vec(context)
}

fn node_buffer_write_bytes(
    target: &JsValue,
    offset: usize,
    bytes: &[u8],
    context: &mut Context,
) -> JsResult<usize> {
    let object = target
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("target must be an object"))?;
    let view = node_buffer_uint8_view_from_value(target, context)?;
    let length = view.length(context)?;
    if offset >= length {
        return Ok(0);
    }
    let count = bytes.len().min(length - offset);
    for (index, byte) in bytes.iter().copied().take(count).enumerate() {
        object.set((offset + index) as u32, JsValue::from(byte), true, context)?;
    }
    Ok(count)
}

fn node_buffer_compare_bytes(left: &[u8], right: &[u8]) -> i32 {
    use std::cmp::Ordering;
    match left.cmp(right) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn node_buffer_encoding_from_index(index: i32) -> &'static str {
    match index {
        0 => "ascii",
        1 => "base64",
        2 => "base64url",
        3 => "hex",
        4 => "latin1",
        5 | 6 | 7 | 8 => "utf16le",
        9 | 10 => "utf8",
        _ => "utf8",
    }
}

fn node_buffer_normalize_encoding(label: &str) -> Option<&'static str> {
    match label.to_ascii_lowercase().as_str() {
        "ascii" => Some("ascii"),
        "base64" => Some("base64"),
        "base64url" => Some("base64url"),
        "hex" => Some("hex"),
        "latin1" | "binary" => Some("latin1"),
        "ucs2" | "ucs-2" | "utf16le" | "utf-16le" => Some("utf16le"),
        "utf8" | "utf-8" => Some("utf8"),
        _ => None,
    }
}

fn node_buffer_decode_string(bytes: &[u8], encoding: &str) -> String {
    match encoding {
        "ascii" => bytes.iter().map(|byte| char::from(byte & 0x7f)).collect(),
        "base64" => BASE64_STANDARD.encode(bytes),
        "base64url" => BASE64_URL_SAFE_NO_PAD.encode(bytes),
        "hex" => hex::encode(bytes),
        "latin1" => bytes.iter().map(|byte| char::from(*byte)).collect(),
        "utf16le" => {
            let mut units = Vec::with_capacity(bytes.len() / 2);
            let mut chunks = bytes.chunks_exact(2);
            for chunk in &mut chunks {
                units.push(u16::from_le_bytes([chunk[0], chunk[1]]));
            }
            String::from_utf16_lossy(&units)
        }
        _ => String::from_utf8_lossy(bytes).into_owned(),
    }
}

fn node_buffer_decode_base64_bytes(input: &str, url_safe: bool) -> Option<Vec<u8>> {
    let mut sanitized = String::with_capacity(input.len());
    for ch in input.chars() {
        if !ch.is_ascii_whitespace() {
            sanitized.push(ch);
        }
    }
    if sanitized.is_empty() {
        return Some(Vec::new());
    }
    let remainder = sanitized.len() % 4;
    if remainder == 1 {
        return None;
    }
    let engine = if url_safe {
        &BASE64_URL_SAFE_NO_PAD
    } else {
        &BASE64_STANDARD
    };
    if url_safe {
        if remainder != 0 {
            sanitized.extend(std::iter::repeat_n('=', 4 - remainder));
        }
        sanitized = sanitized.replace('-', "+").replace('_', "/");
        BASE64_STANDARD.decode(sanitized).ok()
    } else {
        if remainder != 0 {
            sanitized.extend(std::iter::repeat_n('=', 4 - remainder));
        }
        engine.decode(sanitized).ok()
    }
}

fn node_buffer_decode_hex_bytes(input: &str) -> Vec<u8> {
    fn nibble(ch: u8) -> Option<u8> {
        match ch {
            b'0'..=b'9' => Some(ch - b'0'),
            b'a'..=b'f' => Some(ch - b'a' + 10),
            b'A'..=b'F' => Some(ch - b'A' + 10),
            _ => None,
        }
    }

    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0usize;
    while index + 1 < bytes.len() {
        let Some(high) = nibble(bytes[index]) else {
            break;
        };
        let Some(low) = nibble(bytes[index + 1]) else {
            break;
        };
        out.push((high << 4) | low);
        index += 2;
    }
    out
}

fn node_buffer_encode_string(input: &str, encoding: &str) -> Option<Vec<u8>> {
    match encoding {
        "ascii" => Some(input.chars().map(|ch| (ch as u32 as u8) & 0x7f).collect()),
        "base64" => node_buffer_decode_base64_bytes(input, false),
        "base64url" => node_buffer_decode_base64_bytes(input, true),
        "hex" => Some(node_buffer_decode_hex_bytes(input)),
        "latin1" => Some(input.chars().map(|ch| ch as u32 as u8).collect()),
        "utf16le" => Some(
            input
                .encode_utf16()
                .flat_map(|unit| unit.to_le_bytes())
                .collect(),
        ),
        _ => Some(input.as_bytes().to_vec()),
    }
}

fn node_buffer_find_subsequence(
    haystack: &[u8],
    needle: &[u8],
    offset: i64,
    is_forward: bool,
    step: usize,
) -> i32 {
    if needle.is_empty() {
        if offset < 0 {
            return 0;
        }
        return (offset as usize).min(haystack.len()) as i32;
    }
    if haystack.is_empty() || needle.len() > haystack.len() || step == 0 {
        return -1;
    }

    let max_start = haystack.len().saturating_sub(needle.len());
    let start = if offset < 0 {
        let adjusted = haystack.len() as i64 + offset;
        if adjusted < 0 {
            if is_forward { 0 } else { return -1 }
        } else {
            adjusted as usize
        }
    } else {
        (offset as usize).min(max_start)
    };

    if is_forward {
        let mut index = start - (start % step);
        while index <= max_start {
            if &haystack[index..index + needle.len()] == needle {
                return index as i32;
            }
            index = index.saturating_add(step);
        }
        -1
    } else {
        let mut index = start - (start % step);
        loop {
            if &haystack[index..index + needle.len()] == needle {
                return index as i32;
            }
            if index < step {
                break;
            }
            index -= step;
        }
        -1
    }
}

fn node_buffer_compare(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let left = node_buffer_bytes(&args[0], context)?;
    let right = node_buffer_bytes(&args[1], context)?;
    Ok(JsValue::from(node_buffer_compare_bytes(&left, &right)))
}

fn node_buffer_compare_offset(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let source = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let target = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    let target_start = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let source_start = args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let target_end = args.get(4).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let source_end = args.get(5).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let left = node_buffer_bytes_range(&source, source_start, source_end, context)?;
    let right = node_buffer_bytes_range(&target, target_start, target_end, context)?;
    Ok(JsValue::from(node_buffer_compare_bytes(&left, &right)))
}

fn node_buffer_copy(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let source = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let target = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    let target_start = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let source_start = args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let count = args.get(4).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let source_bytes = node_buffer_bytes_range(&source, source_start, source_start.saturating_add(count), context)?;
    let written = node_buffer_write_bytes(&target, target_start, &source_bytes, context)?;
    Ok(JsValue::from(written as i32))
}

fn node_buffer_fill(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let target = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let start = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let end = args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let encoding = args
        .get(4)
        .cloned()
        .unwrap_or_else(JsValue::undefined);

    let length = node_buffer_length(&target, context)?;
    if start > end || end > length {
        return Ok(JsValue::from(-2));
    }
    let fill_length = end - start;
    if fill_length == 0 {
        return Ok(JsValue::undefined());
    }

    let pattern = if args.get(1).is_some_and(JsValue::is_string) {
        let value = args[1].to_string(context)?.to_std_string_escaped();
        let encoding_name = node_buffer_normalize_encoding(
            &encoding.to_string(context)?.to_std_string_escaped(),
        )
        .unwrap_or("utf8");
        if value.is_empty() {
            vec![0]
        } else if value.len() == 1 && (encoding_name == "utf8" || encoding_name == "latin1") {
            vec![value.as_bytes()[0]]
        } else {
            let bytes = node_buffer_encode_string(&value, encoding_name).unwrap_or_default();
            if bytes.is_empty() {
                return Ok(JsValue::from(-1));
            }
            bytes
        }
    } else if let Some(value) = args.get(1) {
        if value.is_object() {
            let bytes = node_buffer_bytes(value, context)?;
            if bytes.is_empty() {
                return Ok(JsValue::from(-1));
            }
            bytes
        } else {
            vec![(value.clone().to_u32(context)? & 0xff) as u8]
        }
    } else {
        vec![0]
    };

    let target_object = target
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("fill target must be an object"))?;
    for index in 0..fill_length {
        let byte = pattern[index % pattern.len()];
        target_object.set((start + index) as u32, JsValue::from(byte), true, context)?;
    }
    Ok(JsValue::undefined())
}

fn node_buffer_is_ascii(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(node_buffer_bytes(&args[0], context)?.is_ascii()))
}

fn node_buffer_is_utf8(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(std::str::from_utf8(&node_buffer_bytes(&args[0], context)?).is_ok()))
}

fn node_buffer_index_of_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let haystack = node_buffer_bytes(&args[0], context)?;
    let needle = node_buffer_bytes(&args[1], context)?;
    let offset = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_i32(context)? as i64;
    let encoding = node_buffer_encoding_from_index(
        args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_i32(context)?,
    );
    let is_forward = args.get(4).cloned().unwrap_or_else(JsValue::undefined).to_boolean();
    let step = if encoding == "utf16le" { 2 } else { 1 };
    Ok(JsValue::from(node_buffer_find_subsequence(&haystack, &needle, offset, is_forward, step)))
}

fn node_buffer_index_of_number(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let haystack = node_buffer_bytes(&args[0], context)?;
    let needle = (args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_u32(context)? & 0xff) as u8;
    let offset = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_i32(context)? as i64;
    let is_forward = args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_boolean();
    let needle = [needle];
    Ok(JsValue::from(node_buffer_find_subsequence(&haystack, &needle, offset, is_forward, 1)))
}

fn node_buffer_index_of_string(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let haystack = node_buffer_bytes(&args[0], context)?;
    let needle_text = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_string(context)?.to_std_string_escaped();
    let offset = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_i32(context)? as i64;
    let encoding = node_buffer_encoding_from_index(
        args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_i32(context)?,
    );
    let is_forward = args.get(4).cloned().unwrap_or_else(JsValue::undefined).to_boolean();
    let needle = node_buffer_encode_string(&needle_text, encoding).unwrap_or_default();
    let step = if encoding == "utf16le" { 2 } else { 1 };
    Ok(JsValue::from(node_buffer_find_subsequence(&haystack, &needle, offset, is_forward, step)))
}

fn node_buffer_swap_impl(
    value: &JsValue,
    width: usize,
    context: &mut Context,
) -> JsResult<JsValue> {
    let mut bytes = node_buffer_bytes(value, context)?;
    for chunk in bytes.chunks_exact_mut(width) {
        chunk.reverse();
    }
    node_buffer_write_bytes(value, 0, &bytes, context)?;
    Ok(value.clone())
}

fn node_buffer_swap16(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_swap_impl(&args[0], 2, context)
}

fn node_buffer_swap32(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_swap_impl(&args[0], 4, context)
}

fn node_buffer_swap64(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_swap_impl(&args[0], 8, context)
}

fn node_buffer_btoa(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = args.first().cloned().unwrap_or_else(JsValue::undefined).to_string(context)?.to_std_string_escaped();
    let mut bytes = Vec::with_capacity(input.len());
    for ch in input.chars() {
        let code = ch as u32;
        if code > 0xff {
            return Ok(JsValue::from(-1));
        }
        bytes.push(code as u8);
    }
    Ok(JsValue::from(JsString::from(BASE64_STANDARD.encode(bytes))))
}

fn node_buffer_atob(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = args.first().cloned().unwrap_or_else(JsValue::undefined).to_string(context)?.to_std_string_escaped();
    let has_invalid_char = input
        .chars()
        .any(|ch| !ch.is_ascii_whitespace() && !matches!(ch, 'A'..='Z' | 'a'..='z' | '0'..='9' | '+' | '/' | '='));
    if has_invalid_char {
        return Ok(JsValue::from(-2));
    }
    let Some(bytes) = node_buffer_decode_base64_bytes(&input, false) else {
        return Ok(JsValue::from(-1));
    };
    Ok(JsValue::from(JsString::from(
        bytes.into_iter().map(char::from).collect::<String>(),
    )))
}

fn node_buffer_slice_with_encoding(
    args: &[JsValue],
    context: &mut Context,
    encoding: &'static str,
) -> JsResult<JsValue> {
    let buffer = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let start = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let end = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let bytes = node_buffer_bytes_range(&buffer, start, end, context)?;
    Ok(JsValue::from(JsString::from(node_buffer_decode_string(&bytes, encoding))))
}

fn node_buffer_ascii_slice(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_slice_with_encoding(args, context, "ascii")
}

fn node_buffer_base64_slice(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_slice_with_encoding(args, context, "base64")
}

fn node_buffer_base64url_slice(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_slice_with_encoding(args, context, "base64url")
}

fn node_buffer_latin1_slice(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_slice_with_encoding(args, context, "latin1")
}

fn node_buffer_hex_slice(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_slice_with_encoding(args, context, "hex")
}

fn node_buffer_ucs2_slice(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_slice_with_encoding(args, context, "utf16le")
}

fn node_buffer_utf8_slice(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_slice_with_encoding(args, context, "utf8")
}

fn node_buffer_write_string_with_encoding(
    args: &[JsValue],
    context: &mut Context,
    encoding: &'static str,
) -> JsResult<JsValue> {
    let target = args.first().cloned().unwrap_or_else(JsValue::undefined);
    let input = args.get(1).cloned().unwrap_or_else(JsValue::undefined).to_string(context)?.to_std_string_escaped();
    let offset = args.get(2).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let max_length = args.get(3).cloned().unwrap_or_else(JsValue::undefined).to_length(context)? as usize;
    let bytes = node_buffer_encode_string(&input, encoding).unwrap_or_default();
    let written = node_buffer_write_bytes(&target, offset, &bytes[..bytes.len().min(max_length)], context)?;
    Ok(JsValue::from(written as i32))
}

fn node_buffer_ascii_write_static(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_write_string_with_encoding(args, context, "ascii")
}

fn node_buffer_base64_write(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_write_string_with_encoding(args, context, "base64")
}

fn node_buffer_base64url_write(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_write_string_with_encoding(args, context, "base64url")
}

fn node_buffer_latin1_write_static(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_write_string_with_encoding(args, context, "latin1")
}

fn node_buffer_hex_write(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_write_string_with_encoding(args, context, "hex")
}

fn node_buffer_ucs2_write(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_write_string_with_encoding(args, context, "utf16le")
}

fn node_buffer_utf8_write_static(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    node_buffer_write_string_with_encoding(args, context, "utf8")
}

fn node_buffer_copy_array_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let destination = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("destination must be an ArrayBuffer-compatible object"))?;
    let destination_offset = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as u64;
    let source = args
        .get(2)
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("source must be an ArrayBuffer-compatible object"))?;
    let source_offset = args
        .get(3)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as i64;
    let bytes_to_copy = args
        .get(4)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as i64;

    let destination = node_buffer_uint8_view_from_buffer(destination, context)?;
    let source = node_buffer_uint8_view_from_buffer(source, context)?;
    let source_end = source_offset
        .checked_add(bytes_to_copy)
        .ok_or_else(|| JsNativeError::range().with_message("source range overflow"))?;
    let source_slice = source.subarray(source_offset, source_end, context)?;
    destination.set_values(JsValue::from(source_slice), Some(destination_offset), context)?;
    Ok(JsValue::undefined())
}

fn node_buffer_uint8_view_from_buffer(
    object: JsObject,
    context: &mut Context,
) -> JsResult<JsUint8Array> {
    if let Ok(buffer) = JsArrayBuffer::from_object(object.clone()) {
        return JsUint8Array::from_array_buffer(buffer, context);
    }
    if let Ok(buffer) = JsSharedArrayBuffer::from_object(object) {
        return JsUint8Array::from_shared_array_buffer(buffer, context);
    }
    Err(JsNativeError::typ()
        .with_message("value is not an ArrayBuffer or SharedArrayBuffer")
        .into())
}

fn node_blob_register_bytes(host: &NodeRuntimeHost, bytes: Vec<u8>) -> u64 {
    let mut state = host.bootstrap.borrow_mut();
    let id = state.blobs.next_id;
    state.blobs.next_id = state.blobs.next_id.saturating_add(1);
    state.blobs.blobs.insert(id, bytes);
    id
}

fn node_blob_bytes_from_id(host: &NodeRuntimeHost, blob_id: u64) -> Result<Vec<u8>, SandboxError> {
    host.bootstrap
        .borrow()
        .blobs
        .blobs
        .get(&blob_id)
        .cloned()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("unknown blob id {blob_id}"),
        })
}

fn node_blob_handle_blob_id(object: &JsObject, context: &mut Context) -> JsResult<u64> {
    let host = active_node_host().map_err(js_error)?;
    let blob_id_symbol = node_host_private_symbol(&host, "blob.id").map_err(js_error)?;
    object.get(blob_id_symbol, context)?.to_u32(context).map(u64::from)
}

fn node_blob_make_handle(context: &mut Context, blob_id: u64) -> Result<JsObject, SandboxError> {
    let host = active_node_host()?;
    let blob_id_symbol = node_host_private_symbol(&host, "blob.id")?;
    let object = ObjectInitializer::new(context).build();
    object
        .set(blob_id_symbol, JsValue::from(blob_id as f64), true, context)
        .map_err(sandbox_execution_error)?;
    for (name, length, function) in [
        ("slice", 2usize, node_blob_handle_slice as fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>),
        ("getReader", 0usize, node_blob_handle_get_reader),
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

fn node_blob_reader_make(
    context: &mut Context,
    blob_id: u64,
    offset: usize,
) -> Result<JsObject, SandboxError> {
    let host = active_node_host()?;
    let blob_id_symbol = node_host_private_symbol(&host, "blob.id")?;
    let blob_offset_symbol = node_host_private_symbol(&host, "blob.offset")?;
    let object = ObjectInitializer::new(context).build();
    object
        .set(blob_id_symbol, JsValue::from(blob_id as f64), true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(blob_offset_symbol, JsValue::from(offset as f64), true, context)
        .map_err(sandbox_execution_error)?;
    let pull = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(node_blob_reader_pull))
        .name(js_string!("pull"))
        .length(1)
        .constructor(false)
        .build();
    object
        .set(js_string!("pull"), JsValue::from(pull), true, context)
        .map_err(sandbox_execution_error)?;
    Ok(object)
}

fn node_blob_create_blob(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let sources = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("blob sources must be an array"))?;
    let length = sources
        .get(js_string!("length"), context)?
        .to_length(context)?;
    let mut bytes = Vec::new();
    for index in 0..length {
        let source = sources.get(index, context)?;
        if let Some(object) = source.as_object() {
            let host = active_node_host().map_err(js_error)?;
            let blob_id_symbol = node_host_private_symbol(&host, "blob.id").map_err(js_error)?;
            if object
                .get(blob_id_symbol, context)
                .ok()
                .is_some_and(|v| !v.is_undefined())
            {
                let blob_id = node_blob_handle_blob_id(&object, context)?;
                bytes.extend(node_with_host(|host| node_blob_bytes_from_id(host, blob_id)).map_err(js_error)?);
                continue;
            }
        }
        bytes.extend(node_buffer_bytes(&source, context)?);
    }
    node_with_host_js(context, |host, context| {
        let blob_id = node_blob_register_bytes(host, bytes);
        Ok(JsValue::from(node_blob_make_handle(context, blob_id)?))
    })
}

fn node_blob_create_blob_from_file_path(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        let resolved = resolve_node_path(&host.process.borrow().cwd, &path);
        let Some(bytes) = node_read_file(host, &resolved)? else {
            return Ok(JsValue::undefined());
        };
        let length = bytes.len() as u32;
        let blob_id = node_blob_register_bytes(host, bytes);
        let handle = node_blob_make_handle(context, blob_id)?;
        Ok(JsValue::from(JsArray::from_iter(
            [JsValue::from(handle), JsValue::from(length)],
            context,
        )))
    })
}

fn node_blob_concat(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let list = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("blob concat list must be an array"))?;
    let length = list.get(js_string!("length"), context)?.to_length(context)?;
    let mut bytes = Vec::new();
    for index in 0..length {
        bytes.extend(node_buffer_bytes(&list.get(index, context)?, context)?);
    }
    Ok(JsValue::from(
        JsUint8Array::from_iter(bytes, context)?
            .buffer(context)?
            .clone(),
    ))
}

fn node_blob_get_data_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let key = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        let Some(data) = host.bootstrap.borrow().blobs.data_objects.get(&key).cloned() else {
            return Ok(JsValue::undefined());
        };
        let handle = node_blob_make_handle(context, data.blob_id)?;
        Ok(JsValue::from(JsArray::from_iter(
            [
                JsValue::from(handle),
                JsValue::from(data.length),
                JsValue::from(JsString::from(data.mime_type)),
            ],
            context,
        )))
    })
}

fn node_blob_store_data_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let key = node_arg_string(args, 0, context)?;
    let handle = args
        .get(1)
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("blob handle must be an object"))?;
    let blob_id = node_blob_handle_blob_id(&handle, context)?;
    let length = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)?;
    let mime_type = args
        .get(3)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    node_with_host(|host| {
        host.bootstrap.borrow_mut().blobs.data_objects.insert(
            key,
            NodeBlobDataObject {
                blob_id,
                length,
                mime_type,
            },
        );
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_blob_revoke_object_url(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let url = node_arg_string(args, 0, context)?;
    let key = url
        .strip_prefix("blob:nodedata:")
        .unwrap_or(url.as_str())
        .to_string();
    node_with_host(|host| {
        host.bootstrap.borrow_mut().blobs.data_objects.remove(&key);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_blob_handle_slice(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let handle = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("blob handle receiver must be an object"))?;
    let blob_id = node_blob_handle_blob_id(&handle, context)?;
    let start = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    let end = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_length(context)? as usize;
    node_with_host_js(context, |host, context| {
        let bytes = node_blob_bytes_from_id(host, blob_id)?;
        let start = start.min(bytes.len());
        let end = end.min(bytes.len()).max(start);
        let new_blob_id = node_blob_register_bytes(host, bytes[start..end].to_vec());
        Ok(JsValue::from(node_blob_make_handle(context, new_blob_id)?))
    })
}

fn node_blob_handle_get_reader(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let handle = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("blob handle receiver must be an object"))?;
    let blob_id = node_blob_handle_blob_id(&handle, context)?;
    node_blob_reader_make(context, blob_id, 0).map(JsValue::from).map_err(js_error)
}

fn node_blob_reader_pull(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let host = active_node_host().map_err(js_error)?;
    let blob_id_symbol = node_host_private_symbol(&host, "blob.id").map_err(js_error)?;
    let blob_offset_symbol = node_host_private_symbol(&host, "blob.offset").map_err(js_error)?;
    let reader = this
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("blob reader receiver must be an object"))?;
    let callback = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("blob reader callback must be callable"))?;
    let blob_id = reader
        .get(blob_id_symbol, context)?
        .to_u32(context)
        .map(u64::from)?;
    let offset = reader
        .get(blob_offset_symbol.clone(), context)?
        .to_length(context)? as usize;
    let bytes = node_with_host(|host| node_blob_bytes_from_id(host, blob_id)).map_err(js_error)?;
    if offset >= bytes.len() {
        let _ = callback.call(&JsValue::undefined(), &[JsValue::from(0)], context)?;
        return Ok(JsValue::from(0));
    }
    reader.set(
        blob_offset_symbol,
        JsValue::from(bytes.len() as f64),
        true,
        context,
    )?;
    let chunk = JsValue::from(
        JsUint8Array::from_iter(bytes[offset..].iter().copied(), context)?
            .buffer(context)?
            .clone(),
    );
    let _ = callback.call(&JsValue::undefined(), &[JsValue::from(1), chunk], context)?;
    Ok(JsValue::from(1))
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

fn node_url_parse(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = node_arg_string(args, 0, context)?;
    let base = if args.get(1).is_some_and(|value| !value.is_null() && !value.is_undefined()) {
        Some(node_arg_string(args, 1, context)?)
    } else {
        None
    };
    let raise_exception = args.get(2).is_some_and(JsValue::to_boolean);
    match node_url_parse_resolved(&input, base.as_deref()) {
        Ok(url) => {
            node_url_update_components(this, &url, context)?;
            Ok(JsValue::from(JsString::from(url.as_str())))
        }
        Err(_) if raise_exception => Err(node_error_with_code(
            context,
            JsNativeError::typ(),
            "Invalid URL",
            "ERR_INVALID_URL",
        )),
        Err(_) => Ok(JsValue::undefined()),
    }
}

fn node_url_path_to_file_url(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = node_arg_string(args, 0, context)?;
    let windows = args.get(1).is_some_and(JsValue::to_boolean);
    let hostname = if args.get(2).is_some_and(|value| !value.is_null() && !value.is_undefined()) {
        Some(node_arg_string(args, 2, context)?)
    } else {
        None
    };
    let href = if windows {
        node_url_windows_path_to_file_href(&input, hostname.as_deref())
    } else {
        node_file_url_from_path(&input)
    }
    .map_err(js_error)?;
    let parsed = Url::parse(&href).map_err(|_| {
        node_error_with_code(context, JsNativeError::typ(), "Invalid URL", "ERR_INVALID_URL")
    })?;
    node_url_update_components(this, &parsed, context)?;
    Ok(JsValue::from(JsString::from(href)))
}

fn node_url_update(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let href = node_arg_string(args, 0, context)?;
    let action = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)?;
    let new_value = node_arg_string(args, 2, context)?;
    let mut url = Url::parse(&href)
        .map_err(|_| node_error_with_code(context, JsNativeError::typ(), "Invalid URL", "ERR_INVALID_URL"))?;
    let ok = match action {
        0 => url.set_scheme(new_value.trim_end_matches(':')).is_ok(),
        1 => url.set_host(Some(&new_value)).is_ok(),
        2 => url.set_host(Some(&new_value)).is_ok(),
        3 => {
            if new_value.is_empty() {
                url.set_port(None).is_ok()
            } else {
                match new_value.parse::<u16>() {
                    Ok(port) => url.set_port(Some(port)).is_ok(),
                    Err(_) => false,
                }
            }
        }
        4 => {
            url.set_username(&new_value).is_ok()
        }
        5 => {
            if new_value.is_empty() {
                url.set_password(None).is_ok()
            } else {
                url.set_password(Some(&new_value)).is_ok()
            }
        }
        6 => {
            url.set_path(&new_value);
            true
        }
        7 => {
            if new_value.is_empty() {
                url.set_query(None);
            } else {
                url.set_query(Some(new_value.trim_start_matches('?')));
            }
            true
        }
        8 => {
            if new_value.is_empty() {
                url.set_fragment(None);
            } else {
                url.set_fragment(Some(new_value.trim_start_matches('#')));
            }
            true
        }
        9 => match Url::parse(&new_value) {
            Ok(parsed) => {
                url = parsed;
                true
            }
            Err(_) => false,
        },
        _ => false,
    };
    if !ok {
        return Ok(JsValue::from(false));
    }
    node_url_update_components(this, &url, context)?;
    Ok(JsValue::from(JsString::from(url.as_str())))
}

fn node_url_domain_to_ascii(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = node_arg_string(args, 0, context)?;
    if input.is_empty() {
        return Ok(JsValue::from(js_string!()));
    }
    let parsed = Url::parse(&format!("ws://{input}"));
    Ok(JsValue::from(JsString::from(
        parsed.ok().and_then(|url| url.host_str().map(str::to_string)).unwrap_or_default(),
    )))
}

fn node_url_domain_to_unicode(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = node_arg_string(args, 0, context)?;
    if input.is_empty() {
        return Ok(JsValue::from(js_string!()));
    }
    Ok(JsValue::from(JsString::from(
        idna::domain_to_unicode(&input).0,
    )))
}

fn node_url_get_origin(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let input = node_arg_string(args, 0, context)?;
    let url = Url::parse(&input)
        .map_err(|_| node_error_with_code(context, JsNativeError::typ(), "Invalid URL", "ERR_INVALID_URL"))?;
    Ok(JsValue::from(JsString::from(url.origin().ascii_serialization())))
}

fn node_url_format(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let href = node_arg_string(args, 0, context)?;
    let hash = args.get(1).is_some_and(JsValue::to_boolean);
    let unicode = args.get(2).is_some_and(JsValue::to_boolean);
    let search = args.get(3).is_some_and(JsValue::to_boolean);
    let auth = args.get(4).is_some_and(JsValue::to_boolean);
    let mut url = Url::parse(&href)
        .map_err(|_| node_error_with_code(context, JsNativeError::typ(), "Invalid URL", "ERR_INVALID_URL"))?;
    if !hash {
        url.set_fragment(None);
    }
    if !search {
        url.set_query(None);
    }
    if !auth {
        let _ = url.set_username("");
        let _ = url.set_password(None);
    }
    let rendered = if unicode {
        idna::domain_to_unicode(url.as_str()).0
    } else {
        url.into()
    };
    Ok(JsValue::from(JsString::from(rendered)))
}

fn node_url_parse_resolved(input: &str, base: Option<&str>) -> Result<Url, url::ParseError> {
    base.map_or_else(|| Url::parse(input), |base| Url::parse(base).and_then(|base| base.join(input)))
}

fn node_url_windows_path_to_file_href(
    input: &str,
    hostname: Option<&str>,
) -> Result<String, SandboxError> {
    let normalized = input.replace('\\', "/");
    let path = if normalized.starts_with("//") {
        normalized
    } else if normalized.len() >= 2 && normalized.as_bytes()[1] == b':' {
        format!("/{normalized}")
    } else if normalized.starts_with('/') {
        normalized
    } else {
        format!("/{normalized}")
    };
    let mut href = format!("file://{}", hostname.unwrap_or(""));
    href.push_str(
        &path
            .split('/')
            .map(|segment| {
                utf8_percent_encode(segment, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("/"),
    );
    Ok(href)
}

fn node_url_update_components(
    binding: &JsValue,
    url: &Url,
    context: &mut Context,
) -> JsResult<()> {
    let Some(binding) = binding.as_object() else {
        return Ok(());
    };
    let components = binding.get(js_string!("urlComponents"), context)?;
    let Some(components) = components.as_object() else {
        return Ok(());
    };
    let href = url.as_str();
    let protocol_end = href.find(':').map(|index| index as u32 + 1).unwrap_or(0);
    let username_end = if url.username().is_empty() {
        protocol_end
    } else {
        href.find('@')
            .map(|index| index as u32)
            .unwrap_or(protocol_end)
    };
    let host_start = href.find("//").map(|index| index as u32 + 2).unwrap_or(protocol_end);
    let host_end = if let Some(start) = href[host_start as usize..].find(['/', '?', '#']) {
        host_start + start as u32
    } else {
        href.len() as u32
    };
    let port = url.port().map(u32::from).unwrap_or(0);
    let pathname_start = host_end;
    let search_start = href.find('?').map(|index| index as u32).unwrap_or(href.len() as u32);
    let hash_start = href.find('#').map(|index| index as u32).unwrap_or(href.len() as u32);
    let scheme_type = node_url_scheme_type(url);
    for (index, value) in [
        protocol_end,
        username_end,
        host_start,
        host_end,
        port,
        pathname_start,
        search_start,
        hash_start,
        scheme_type,
    ]
    .into_iter()
    .enumerate()
    {
        components.set(index, JsValue::from(value), true, context)?;
    }
    Ok(())
}

fn node_url_scheme_type(url: &Url) -> u32 {
    match url.scheme() {
        "http" => 0,
        "https" => 2,
        "file" => 6,
        _ => 1,
    }
}

fn node_url_pattern_id(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<u64, SandboxError> {
    let symbol = node_host_private_symbol(host, "url_pattern.id")?;
    object
        .get(symbol, context)
        .map_err(sandbox_execution_error)
        .and_then(|value| {
            value
                .as_number()
                .map(|number| number as u64)
                .ok_or_else(|| SandboxError::Execution {
                    entrypoint: "<node-runtime>".to_string(),
                    message: "URLPattern handle is missing host id".to_string(),
                })
        })
}

fn node_url_pattern_state(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<NodeUrlPatternState, SandboxError> {
    let id = node_url_pattern_id(host, object, context)?;
    host.bootstrap
        .borrow()
        .url_patterns
        .get(&id)
        .cloned()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "unknown URLPattern handle".to_string(),
        })
}

fn node_url_pattern_init_object_from_js(
    object: &JsObject,
    context: &mut Context,
) -> Result<quirks::UrlPatternInit, SandboxError> {
    let mut init = quirks::UrlPatternInit::default();
    for key in [
        "protocol",
        "username",
        "password",
        "hostname",
        "port",
        "pathname",
        "search",
        "hash",
        "baseURL",
    ] {
        let value = object
            .get(JsString::from(key), context)
            .map_err(sandbox_execution_error)?;
        if value.is_undefined() || value.is_null() {
            continue;
        }
        let value = value
            .to_string(context)
            .map_err(sandbox_execution_error)?
            .to_std_string_escaped();
        match key {
            "protocol" => init.protocol = Some(value),
            "username" => init.username = Some(value),
            "password" => init.password = Some(value),
            "hostname" => init.hostname = Some(value),
            "port" => init.port = Some(value),
            "pathname" => init.pathname = Some(value),
            "search" => init.search = Some(value),
            "hash" => init.hash = Some(value),
            "baseURL" => init.base_url = Some(value),
            _ => {}
        }
    }
    Ok(init)
}

fn node_url_pattern_input_spec_from_value(
    value: Option<&JsValue>,
    context: &mut Context,
) -> Result<quirks::StringOrInit<'static>, SandboxError> {
    let Some(value) = value else {
        return Ok(quirks::StringOrInit::Init(quirks::UrlPatternInit::default()));
    };
    if value.is_string() {
        return Ok(quirks::StringOrInit::String(Cow::Owned(
            value
                .to_string(context)
                .map_err(sandbox_execution_error)?
                .to_std_string_escaped(),
        )));
    }
    let object = value.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "URLPattern input needs to be a string or an object".to_string(),
    })?;
    Ok(quirks::StringOrInit::Init(node_url_pattern_init_object_from_js(
        &object, context,
    )?))
}

fn node_url_pattern_options_from_value(
    value: Option<&JsValue>,
    context: &mut Context,
) -> Result<RustUrlPatternOptions, SandboxError> {
    let mut options = RustUrlPatternOptions::default();
    let Some(value) = value else {
        return Ok(options);
    };
    if value.is_undefined() || value.is_null() {
        return Ok(options);
    }
    let object = value.as_object().ok_or_else(|| SandboxError::Execution {
        entrypoint: "<node-runtime>".to_string(),
        message: "URLPattern options needs to be an object".to_string(),
    })?;
    let ignore_case = object
        .get(js_string!("ignoreCase"), context)
        .map_err(sandbox_execution_error)?;
    if !ignore_case.is_undefined() {
        options.ignore_case = ignore_case.to_boolean();
    }
    Ok(options)
}

fn node_url_pattern_input_json(
    input: &quirks::StringOrInit<'_>,
) -> JsonValue {
    match input {
        quirks::StringOrInit::String(value) => JsonValue::String(value.to_string()),
        quirks::StringOrInit::Init(init) => serde_json::json!({
            "protocol": init.protocol,
            "username": init.username,
            "password": init.password,
            "hostname": init.hostname,
            "port": init.port,
            "pathname": init.pathname,
            "search": init.search,
            "hash": init.hash,
            "baseURL": init.base_url,
        }),
    }
}

fn node_url_pattern_exec_result_to_js(
    result: &urlpattern::UrlPatternResult,
    inputs: &(quirks::StringOrInit<'_>, Option<String>),
    context: &mut Context,
) -> Result<JsValue, SandboxError> {
    let mut payload = serde_json::Map::new();
    let mut input_values = vec![node_url_pattern_input_json(&inputs.0)];
    if let Some(base_url) = &inputs.1 {
        input_values.push(JsonValue::String(base_url.clone()));
    }
    payload.insert("inputs".to_string(), JsonValue::Array(input_values));
    for (name, component) in [
        ("protocol", &result.protocol),
        ("username", &result.username),
        ("password", &result.password),
        ("hostname", &result.hostname),
        ("port", &result.port),
        ("pathname", &result.pathname),
        ("search", &result.search),
        ("hash", &result.hash),
    ] {
        let groups = component
            .groups
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    value
                        .clone()
                        .map(JsonValue::String)
                        .unwrap_or(JsonValue::Null),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        payload.insert(
            name.to_string(),
            serde_json::json!({
                "input": component.input,
                "groups": groups,
            }),
        );
    }
    JsValue::from_json(&JsonValue::Object(payload), context).map_err(sandbox_execution_error)
}

fn node_url_pattern_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let object = this.as_object().unwrap_or_else(JsObject::with_null_proto);
    let input = node_url_pattern_input_spec_from_value(args.first(), context).map_err(js_error)?;
    let base = if args.get(1).is_some_and(|value| value.is_string()) {
        Some(node_arg_string(args, 1, context)?)
    } else {
        None
    };
    let options_index = if base.is_some() { 2 } else { 1 };
    let options =
        node_url_pattern_options_from_value(args.get(options_index), context).map_err(js_error)?;
    let init =
        quirks::process_construct_pattern_input(input, base.as_deref()).map_err(sandbox_execution_error).map_err(js_error)?;
    let compiled = RustUrlPattern::parse(init, options)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    let pattern = NodeUrlPatternState {
        protocol: compiled.protocol().to_string(),
        username: compiled.username().to_string(),
        password: compiled.password().to_string(),
        hostname: compiled.hostname().to_string(),
        port: compiled.port().to_string(),
        pathname: compiled.pathname().to_string(),
        search: compiled.search().to_string(),
        hash: compiled.hash().to_string(),
        has_regexp_groups: compiled.has_regexp_groups(),
        compiled: Rc::new(compiled),
    };
    node_with_host_js(context, |host, context| {
        let id = node_next_host_handle_id(host);
        let id_symbol = node_host_private_symbol(host, "url_pattern.id")?;
        object
            .set(id_symbol, JsValue::from(id as f64), true, context)
            .map_err(sandbox_execution_error)?;
        host.bootstrap.borrow_mut().url_patterns.insert(id, pattern.clone());
        Ok(JsValue::from(object))
    })
}

fn node_url_pattern_test(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::from(false));
    };
    let host = active_node_host().map_err(js_error)?;
    let pattern = node_url_pattern_state(&host, &object, context).map_err(js_error)?;
    let input = node_url_pattern_input_spec_from_value(args.first(), context).map_err(js_error)?;
    let base = args
        .get(1)
        .filter(|value| value.is_string())
        .map(|_| node_arg_string(args, 1, context))
        .transpose()?;
    let Some((match_input, _inputs)) =
        quirks::process_match_input(input, base.as_deref()).map_err(sandbox_execution_error).map_err(js_error)?
    else {
        return Ok(JsValue::from(false));
    };
    let matched = pattern
        .compiled
        .test(match_input)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    Ok(JsValue::from(matched))
}

fn node_url_pattern_exec(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::null());
    };
    let host = active_node_host().map_err(js_error)?;
    let pattern = node_url_pattern_state(&host, &object, context).map_err(js_error)?;
    let input = node_url_pattern_input_spec_from_value(args.first(), context).map_err(js_error)?;
    let base = args
        .get(1)
        .filter(|value| value.is_string())
        .map(|_| node_arg_string(args, 1, context))
        .transpose()?;
    let Some((match_input, inputs)) =
        quirks::process_match_input(input, base.as_deref()).map_err(sandbox_execution_error).map_err(js_error)?
    else {
        return Ok(JsValue::null());
    };
    let result = pattern
        .compiled
        .exec(match_input)
        .map_err(sandbox_execution_error)
        .map_err(js_error)?;
    match result {
        Some(result) => node_url_pattern_exec_result_to_js(&result, &inputs, context).map_err(js_error),
        None => Ok(JsValue::null()),
    }
}

fn node_url_pattern_get_component(
    this: &JsValue,
    context: &mut Context,
    project: impl FnOnce(&NodeUrlPatternState) -> String,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_url_pattern_state(&host, &object, context).map_err(js_error)?;
    Ok(JsValue::from(JsString::from(project(&state))))
}

fn node_url_pattern_get_protocol(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_url_pattern_get_component(this, context, |state| state.protocol.clone())
}

fn node_url_pattern_get_username(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_url_pattern_get_component(this, context, |state| state.username.clone())
}

fn node_url_pattern_get_password(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_url_pattern_get_component(this, context, |state| state.password.clone())
}

fn node_url_pattern_get_hostname(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_url_pattern_get_component(this, context, |state| state.hostname.clone())
}

fn node_url_pattern_get_port(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_url_pattern_get_component(this, context, |state| state.port.clone())
}

fn node_url_pattern_get_pathname(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_url_pattern_get_component(this, context, |state| state.pathname.clone())
}

fn node_url_pattern_get_search(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_url_pattern_get_component(this, context, |state| state.search.clone())
}

fn node_url_pattern_get_hash(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_url_pattern_get_component(this, context, |state| state.hash.clone())
}

fn node_url_pattern_get_has_regexp_groups(
    this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::from(false));
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_url_pattern_state(&host, &object, _context).map_err(js_error)?;
    Ok(JsValue::from(state.has_regexp_groups))
}

fn node_modules_enable_compile_cache(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let directory = args
        .first()
        .cloned()
        .filter(|value| !value.is_null() && !value.is_undefined())
        .map(|value| {
            value
                .to_string(context)
                .map(|value| value.to_std_string_escaped())
        })
        .transpose()?;
    let _portable = args.get(1).is_some_and(JsValue::to_boolean);
    node_with_host_js(context, |host, context| {
        let mut state = host.bootstrap.borrow_mut();
        if state.compile_cache_enabled {
            return Ok(JsValue::from(JsArray::from_iter(
                [
                    JsValue::from(2),
                    JsValue::undefined(),
                    state
                        .compile_cache_dir
                        .clone()
                        .map(|directory| JsValue::from(JsString::from(directory)))
                        .unwrap_or_else(JsValue::undefined),
                ],
                context,
            )));
        }
        let directory = directory.unwrap_or_else(|| {
            format!("{}/node-compile-cache", host.workspace_root)
        });
        state.compile_cache_enabled = true;
        state.compile_cache_dir = Some(directory.clone());
        Ok(JsValue::from(JsArray::from_iter(
            [
                JsValue::from(1),
                JsValue::undefined(),
                JsValue::from(JsString::from(directory)),
            ],
            context,
        )))
    })
}

fn node_modules_get_compile_cache_dir(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        Ok(host
            .bootstrap
            .borrow()
            .compile_cache_dir
            .clone()
            .map(|directory| JsValue::from(JsString::from(directory)))
            .unwrap_or_else(JsValue::undefined))
    })
    .map_err(js_error)
}

fn node_modules_get_nearest_parent_package_json_type(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        let resolved = resolve_node_path(&host.process.borrow().cwd, &path);
        let package_type = node_modules_traverse_parent_package_json(host, &resolved)?
            .and_then(|(package_json, _)| {
                package_json
                    .get("type")
                    .and_then(|value| value.as_str())
                    .filter(|value| *value == "commonjs" || *value == "module")
                    .map(str::to_string)
            });
        Ok(match package_type {
            Some(value) => JsValue::from(JsString::from(value)),
            None => JsValue::undefined(),
        })
    })
    .map_err(js_error)
}

fn node_modules_serialize_package_json(
    package_json: &serde_json::Value,
    package_json_path: Option<&str>,
) -> serde_json::Value {
    let imports = package_json.get("imports").map(|value| {
        value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string())
    });
    let exports = package_json.get("exports").map(|value| {
        value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string())
    });
    serde_json::json!([
        package_json.get("name").and_then(|value| value.as_str()),
        package_json.get("main").and_then(|value| value.as_str()),
        package_json
            .get("type")
            .and_then(|value| value.as_str())
            .filter(|value| *value == "commonjs" || *value == "module")
            .unwrap_or("none"),
        imports,
        exports,
        package_json_path,
    ])
}

fn node_modules_package_scope_start_path(
    host: &NodeRuntimeHost,
    input: &str,
) -> Result<String, SandboxError> {
    if let Ok(url) = Url::parse(input) {
        let path = url.to_file_path().map_err(|_| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: format!("invalid file URL: {input}"),
        })?;
        let normalized = normalize_node_path(&path.to_string_lossy());
        if normalized.ends_with("/package.json") {
            return Ok(normalized);
        }
        let base = std::path::Path::new(&normalized);
        let package_json = base
            .parent()
            .unwrap_or(base)
            .join("package.json");
        return Ok(normalize_node_path(&package_json.to_string_lossy()));
    }

    let resolved = resolve_node_path(&host.process.borrow().cwd, input);
    if resolved.ends_with("/package.json") {
        return Ok(resolved);
    }
    let path = std::path::Path::new(&resolved);
    let package_json = path.parent().unwrap_or(path).join("package.json");
    Ok(normalize_node_path(&package_json.to_string_lossy()))
}

fn node_modules_find_package_scope_config(
    host: &NodeRuntimeHost,
    input: &str,
) -> Result<(Option<serde_json::Value>, String), SandboxError> {
    let mut current = node_modules_package_scope_start_path(host, input)?;
    let initial = current.clone();
    loop {
        if current.ends_with("/node_modules/package.json") {
            break;
        }
        if let Some(package_json) = read_package_json(host, &current)? {
            return Ok((Some(package_json), current));
        }
        let parent = std::path::Path::new(&current)
            .parent()
            .and_then(|path| path.parent())
            .map(|path| normalize_node_path(&format!("{}/package.json", path.display())));
        let Some(parent) = parent else {
            break;
        };
        if parent == current {
            break;
        }
        current = parent;
    }
    Ok((None, initial))
}

fn node_modules_traverse_parent_package_json(
    host: &NodeRuntimeHost,
    path: &str,
) -> Result<Option<(serde_json::Value, String)>, SandboxError> {
    let mut current = std::path::PathBuf::from(path);
    loop {
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        current = parent.to_path_buf();
        if current
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|value| value == "node_modules")
        {
            return Ok(None);
        }
        let package_json_path =
            normalize_node_path(&format!("{}/package.json", current.display()));
        if let Some(package_json) = read_package_json(host, &package_json_path)? {
            return Ok(Some((package_json, package_json_path)));
        }
    }
    Ok(None)
}

fn node_modules_get_nearest_parent_package_json(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let value = node_with_host(|host| {
        let resolved = resolve_node_path(&host.process.borrow().cwd, &path);
        Ok(node_modules_traverse_parent_package_json(host, &resolved)?
            .map(|(package_json, package_json_path)| {
                node_modules_serialize_package_json(&package_json, Some(&package_json_path))
            }))
    })
    .map_err(js_error)?;
    match value {
        Some(value) => JsValue::from_json(&value, context),
        None => Ok(JsValue::undefined()),
    }
}

fn node_modules_get_package_scope_config(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host_js(context, |host, context| {
        let (package_json, package_json_path) =
            node_modules_find_package_scope_config(host, &path)?;
        match package_json {
            Some(package_json) => {
                JsValue::from_json(
                    &node_modules_serialize_package_json(&package_json, Some(&package_json_path)),
                    context,
                )
                .map_err(sandbox_execution_error)
            }
            None => Ok(JsValue::from(JsString::from(package_json_path))),
        }
    })
}

fn node_modules_get_package_type(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    node_with_host(|host| {
        let (package_json, _) = node_modules_find_package_scope_config(host, &path)?;
        let value = package_json
            .and_then(|package_json| {
                package_json
                    .get("type")
                    .and_then(|value| value.as_str())
                    .filter(|value| *value == "commonjs" || *value == "module")
                    .map(str::to_string)
            })
            .map(|value| JsValue::from(JsString::from(value)))
            .unwrap_or_else(JsValue::undefined);
        Ok(value)
    })
    .map_err(js_error)
}

fn node_modules_read_package_json(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let json_path = node_arg_string(args, 0, context)?;
    let value = node_with_host(|host| {
        let resolved = resolve_node_path(&host.process.borrow().cwd, &json_path);
        let package_json = read_package_json(host, &resolved)?;
        Ok(package_json.map(|value| node_modules_serialize_package_json(&value, Some(&resolved))))
    })
    .map_err(js_error)?;
    match value {
        Some(value) => JsValue::from_json(&value, context),
        None => Ok(JsValue::undefined()),
    }
}

fn node_modules_flush_compile_cache(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host(|host| {
        node_graph_invalidate(host);
        host.materialized_modules.borrow_mut().clear();
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_modules_compile_cache_key(
    source: &str,
    filename: &str,
    code_type: i32,
) -> String {
    format!("{code_type}:{filename}:{source}")
}

fn node_modules_get_compile_cache_entry(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let source = node_arg_string(args, 0, context)?;
    let filename = node_arg_string(args, 1, context)?;
    let code_type = args
        .get(2)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)?;
    node_with_host_js(context, |host, context| {
        let key = node_modules_compile_cache_key(&source, &filename, code_type);
        let mut bootstrap = host.bootstrap.borrow_mut();
        if !bootstrap.compile_cache_enabled {
            return Ok(JsValue::undefined());
        }
        let entry_id = bootstrap
            .compile_cache_entries
            .iter()
            .find_map(|(id, entry)| (entry.key == key).then_some(*id))
            .unwrap_or_else(|| {
                bootstrap.next_compile_cache_entry_id =
                    bootstrap.next_compile_cache_entry_id.saturating_add(1);
                let id = bootstrap.next_compile_cache_entry_id;
                bootstrap
                    .compile_cache_entries
                    .insert(id, NodeCompileCacheEntryState { key, transpiled: None });
                id
            });
        let entry = bootstrap
            .compile_cache_entries
            .get(&entry_id)
            .cloned()
            .ok_or_else(|| sandbox_execution_error("missing compile cache entry"))?;
        drop(bootstrap);

        let external = JsObject::with_null_proto();
        let symbol = node_host_private_symbol(host, "compile_cache.entry_id")?;
        external
            .set(symbol, JsValue::from(entry_id as f64), true, context)
            .map_err(sandbox_execution_error)?;
        let payload = JsObject::with_null_proto();
        payload
            .set(js_string!("external"), JsValue::from(external), true, context)
            .map_err(sandbox_execution_error)?;
        payload
            .set(
                js_string!("transpiled"),
                entry
                    .transpiled
                    .map(|value| JsValue::from(JsString::from(value)))
                    .unwrap_or_else(JsValue::undefined),
                true,
                context,
            )
            .map_err(sandbox_execution_error)?;
        Ok(JsValue::from(payload))
    })

}

fn node_modules_save_compile_cache_entry(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let external = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("compile cache external handle must be an object"))?;
    let transpiled = node_arg_string(args, 1, context)?;
    node_with_host_js(context, |host, context| {
        let symbol = node_host_private_symbol(host, "compile_cache.entry_id")?;
        let entry_id = external
            .get(symbol, context)
            .map_err(sandbox_execution_error)?
            .to_number(context)
            .map_err(sandbox_execution_error)? as u64;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let entry = bootstrap
            .compile_cache_entries
            .get_mut(&entry_id)
            .ok_or_else(|| sandbox_execution_error("unknown compile cache entry"))?;
        entry.transpiled = Some(transpiled);
        Ok(JsValue::undefined())
    })

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
            "--permission": false,
            "--allow-fs-read": [],
            "--allow-fs-write": [],
            "--allow-addons": false,
            "--allow-child-process": false,
            "--allow-inspector": false,
            "--allow-wasi": false,
            "--allow-worker": false,
            "--conditions": [],
            "--no-addons": false,
            "--require": [],
            "--experimental-loader": [],
            "--import": [],
            "--entry-url": false,
            "--enable-source-maps": false,
            "--experimental-require-module": false,
            "--experimental-vm-modules": false,
            "--strip-types": false,
            "--experimental-default-type": "commonjs",
            "--use-env-proxy": false,
            "--network-family-autoselection": true,
            "--network-family-autoselection-attempt-timeout": 250,
            "--warnings": true,
            "--no-experimental-websocket": false,
            "--experimental-eventsource": false,
            "--no-experimental-global-navigator": false,
            "--no-experimental-sqlite": false,
            "--experimental-default-config-file": false,
            "--experimental-config-file": false,
            "--trace-sigint": false,
            "--expose-internals": false,
            "--report-on-signal": false,
            "--heapsnapshot-signal": "",
            "--diagnostic-dir": "",
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

fn node_type_constructor_name(
    value: &JsValue,
    context: &mut Context,
) -> JsResult<Option<String>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    let constructor = object.get(js_string!("constructor"), context)?;
    let Some(constructor) = constructor.as_object() else {
        return Ok(None);
    };
    Ok(constructor
        .get(js_string!("name"), context)?
        .as_string()
        .map(|value| value.to_std_string_escaped()))
}

fn node_type_has_tag(
    value: &JsValue,
    context: &mut Context,
    expected: &str,
) -> JsResult<bool> {
    Ok(node_type_constructor_name(value, context)?.as_deref() == Some(expected))
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

fn node_type_is_arguments_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Arguments"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_array_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "ArrayBuffer"))
            .transpose()?
            .unwrap_or(false),
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
    let _ = object;
    Ok(JsValue::from(node_type_has_tag(value, context, "AsyncFunction")?))
}

fn node_type_is_bigint_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "BigInt"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_boolean_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Boolean"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_boxed_primitive(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let is_boxed = match args.first() {
        Some(value) => matches!(
            node_type_constructor_name(value, context)?.as_deref(),
            Some("Number" | "String" | "Boolean" | "BigInt" | "Symbol")
        ),
        None => false,
    };
    Ok(JsValue::from(is_boxed))
}

fn node_type_is_data_view(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "DataView"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_date(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Date"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_external(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(false))
}

fn node_type_is_generator_function(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "GeneratorFunction"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_generator_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Generator"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_map(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Map"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_map_iterator(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Map Iterator"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_module_namespace_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Module"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_native_error(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let is_error = match args.first() {
        Some(value) => matches!(
            node_type_constructor_name(value, context)?.as_deref(),
            Some("Error" | "TypeError" | "RangeError" | "SyntaxError" | "ReferenceError"
                | "URIError" | "EvalError" | "AggregateError")
        ),
        None => false,
    };
    Ok(JsValue::from(is_error))
}

fn node_type_is_number_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Number"))
            .transpose()?
            .unwrap_or(false),
    ))
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
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .and_then(JsValue::as_object)
            .is_some_and(|object| JsProxy::from_object(object.clone()).is_ok()),
    ))
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
    let _ = object;
    Ok(JsValue::from(node_type_has_tag(value, context, "RegExp")?))
}

fn node_type_is_set(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Set"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_set_iterator(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Set Iterator"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_shared_array_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "SharedArrayBuffer"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_string_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "String"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_symbol_object(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "Symbol"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_weak_map(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "WeakMap"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_weak_set(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from(
        args.first()
            .map(|value| node_type_has_tag(value, context, "WeakSet"))
            .transpose()?
            .unwrap_or(false),
    ))
}

fn node_type_is_any_array_buffer(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let is_any_array_buffer = match args.first() {
        Some(value) => matches!(
            node_type_constructor_name(value, context)?.as_deref(),
            Some("ArrayBuffer" | "SharedArrayBuffer")
        ),
        None => false,
    };
    Ok(JsValue::from(is_any_array_buffer))
}

fn node_module_wrap_id(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<u64, SandboxError> {
    let symbol = node_host_private_symbol(host, "module_wrap.id")?;
    object
        .get(symbol, context)
        .map_err(sandbox_execution_error)
        .and_then(|value| {
            value
                .as_number()
                .map(|number| number as u64)
                .ok_or_else(|| SandboxError::Execution {
                    entrypoint: "<node-runtime>".to_string(),
                    message: "ModuleWrap handle is missing host id".to_string(),
                })
        })
}

fn node_module_wrap_state(
    host: &NodeRuntimeHost,
    object: &JsObject,
    context: &mut Context,
) -> Result<NodeModuleWrapState, SandboxError> {
    let id = node_module_wrap_id(host, object, context)?;
    host.bootstrap
        .borrow()
        .module_wraps
        .get(&id)
        .cloned()
        .ok_or_else(|| SandboxError::Execution {
            entrypoint: "<node-runtime>".to_string(),
            message: "unknown ModuleWrap handle".to_string(),
        })
}

fn node_module_wrap_synthetic_namespace(
    state: &NodeModuleWrapState,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let namespace = JsObject::with_null_proto();
    for export_name in &state.synthetic_export_names {
        let value = state
            .synthetic_exports
            .get(export_name)
            .cloned()
            .unwrap_or_else(JsValue::undefined);
        namespace
            .set(JsString::from(export_name.clone()), value, true, context)
            .map_err(sandbox_execution_error)?;
    }
    Ok(namespace)
}

fn node_source_text_has_top_level_await(source: &str) -> bool {
    let mut interner = Interner::default();
    let scope = Scope::new_global();
    let Ok(module) = BoaParser::new(BoaParserSource::from_bytes(source))
        .parse_module(&scope, &mut interner) else {
        return false;
    };
    format!("{module:?}").contains("Await")
}

fn node_module_wrap_request_entries(source: &str) -> Vec<(String, JsonValue, i32)> {
    let mut interner = Interner::default();
    let scope = Scope::new_global();
    let Ok(module) = BoaParser::new(BoaParserSource::from_bytes(source))
        .parse_module(&scope, &mut interner) else {
        return Vec::new();
    };
    let phase = 1;
    #[derive(Debug)]
    struct RequestVisitor<'a> {
        interner: &'a Interner,
        phase: i32,
        entries: Vec<(String, JsonValue, i32)>,
    }

    impl RequestVisitor<'_> {
        fn attributes_json(
            &self,
            attributes: &[boa_ast::declaration::ImportAttribute],
        ) -> JsonValue {
            let mut map = serde_json::Map::new();
            for attribute in attributes {
                let Some(key) = self.interner.resolve_expect(attribute.key()).utf8() else {
                    continue;
                };
                let Some(value) = self.interner.resolve_expect(attribute.value()).utf8() else {
                    continue;
                };
                map.insert(key.to_owned(), JsonValue::String(value.to_owned()));
            }
            JsonValue::Object(map)
        }
    }

    impl<'ast> Visitor<'ast> for RequestVisitor<'_> {
        type BreakTy = std::convert::Infallible;

        fn visit_import_declaration(
            &mut self,
            node: &'ast boa_ast::declaration::ImportDeclaration,
        ) -> std::ops::ControlFlow<Self::BreakTy> {
            if let Some(specifier) = self.interner.resolve_expect(node.specifier().sym()).utf8() {
                self.entries.push((
                    specifier.to_owned(),
                    self.attributes_json(node.attributes()),
                    self.phase,
                ));
            }
            std::ops::ControlFlow::Continue(())
        }

        fn visit_export_declaration(
            &mut self,
            node: &'ast boa_ast::declaration::ExportDeclaration,
        ) -> std::ops::ControlFlow<Self::BreakTy> {
            if let boa_ast::declaration::ExportDeclaration::ReExport {
                specifier,
                attributes,
                ..
            } = node
                && let Some(specifier) = self.interner.resolve_expect(specifier.sym()).utf8()
            {
                self.entries.push((
                    specifier.to_owned(),
                    self.attributes_json(attributes),
                    self.phase,
                ));
            }
            std::ops::ControlFlow::Continue(())
        }

        fn visit_statement_list_item(
            &mut self,
            _: &'ast boa_ast::StatementListItem,
        ) -> std::ops::ControlFlow<Self::BreakTy> {
            std::ops::ControlFlow::Continue(())
        }
    }

    let mut visitor = RequestVisitor {
        interner: &interner,
        phase,
        entries: Vec::new(),
    };
    let _ = visitor.visit_module(&module);
    visitor.entries
}

fn node_module_wrap_compute_has_async_graph(
    module_wraps: &BTreeMap<u64, NodeModuleWrapState>,
    id: u64,
    visiting: &mut BTreeSet<u64>,
) -> bool {
    if !visiting.insert(id) {
        return false;
    }
    let Some(state) = module_wraps.get(&id) else {
        visiting.remove(&id);
        return false;
    };
    let own_async = state
        .source_text
        .as_deref()
        .is_some_and(node_source_text_has_top_level_await);
    let child_async = state
        .linked_request_ids
        .iter()
        .copied()
        .any(|child_id| node_module_wrap_compute_has_async_graph(module_wraps, child_id, visiting));
    visiting.remove(&id);
    own_async || child_async
}

fn node_module_wrap_construct(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let object = this.as_object().unwrap_or_else(JsObject::with_null_proto);
    let url = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?
        .to_std_string_escaped();
    node_with_host_js(context, |host, context| {
        let id = node_next_host_handle_id(host);
        let id_symbol = node_host_private_symbol(host, "module_wrap.id")?;
        object
            .set(id_symbol, JsValue::from(id as f64), true, context)
            .map_err(sandbox_execution_error)?;

        let state = if args.get(2).is_some_and(JsValue::is_string) {
            let source_text = args[2]
                .to_string(context)
                .map_err(sandbox_execution_error)?
                .to_std_string_escaped();
            let line_offset = args
                .get(3)
                .cloned()
                .unwrap_or_else(|| JsValue::from(0))
                .to_i32(context)
                .unwrap_or(0);
            let column_offset = args
                .get(4)
                .cloned()
                .unwrap_or_else(|| JsValue::from(0))
                .to_i32(context)
                .unwrap_or(0);
            let host_defined_option_id = args.get(5).cloned().filter(|value| !value.is_undefined());
            let module = Module::parse(
                Source::from_bytes(source_text.as_bytes()).with_path(&PathBuf::from(url.clone())),
                None,
                context,
            )
            .map_err(sandbox_execution_error)?;
            node_contextify_attach_host_defined_option(&object, host_defined_option_id.clone(), context)?;
            let (annotated_source_url, source_map_url) = node_extract_source_annotations(&source_text);
            let source_url = annotated_source_url.unwrap_or_else(|| url.clone());
            let has_top_level_await = node_source_text_has_top_level_await(&source_text);
            object
                .set(js_string!("url"), JsValue::from(JsString::from(url.clone())), true, context)
                .map_err(sandbox_execution_error)?;
            object
                .set(js_string!("synthetic"), JsValue::from(false), true, context)
                .map_err(sandbox_execution_error)?;
            object
                .set(
                    js_string!("hasTopLevelAwait"),
                    JsValue::from(has_top_level_await),
                    true,
                    context,
                )
                .map_err(sandbox_execution_error)?;
            object
                .set(js_string!("sourceURL"), JsValue::from(JsString::from(source_url.clone())), true, context)
                .map_err(sandbox_execution_error)?;
            object
                .set(
                    js_string!("sourceMapURL"),
                    source_map_url
                        .clone()
                        .map(|value| JsValue::from(JsString::from(value)))
                        .unwrap_or_else(JsValue::undefined),
                    true,
                    context,
                )
                .map_err(sandbox_execution_error)?;
            NodeModuleWrapState {
                url,
                wrapper: object.clone(),
                status: 0,
                synthetic: false,
                source_text: Some(source_text),
                line_offset,
                column_offset,
                host_defined_option_id,
                has_top_level_await,
                source_url: Some(source_url),
                source_map_url,
                synthetic_export_names: Vec::new(),
                synthetic_evaluation_steps: None,
                imported_cjs: None,
                synthetic_exports: BTreeMap::new(),
                module: Some(module),
                module_source_object: None,
                linked_request_ids: Vec::new(),
                linked: false,
                instantiated: false,
                has_async_graph: None,
                error: None,
            }
        } else {
            let export_names = args
                .get(2)
                .and_then(JsValue::as_object)
                .map(|array| {
                    let length = array
                        .get(js_string!("length"), context)
                        .ok()
                        .and_then(|value| value.as_number())
                        .unwrap_or(0.0) as u32;
                    (0..length)
                        .filter_map(|index| {
                            array
                                .get(index, context)
                                .ok()
                                .and_then(|value| value.to_string(context).ok())
                                .map(|value| value.to_std_string_escaped())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            object
                .set(js_string!("url"), JsValue::from(JsString::from(url.clone())), true, context)
                .map_err(sandbox_execution_error)?;
            object
                .set(js_string!("synthetic"), JsValue::from(true), true, context)
                .map_err(sandbox_execution_error)?;
            if let Some(imported_cjs) = args.get(4).and_then(JsValue::as_object) {
                let symbols = node_internal_binding_symbols(context)?;
                let imported_cjs_symbol = symbols
                    .get(js_string!("imported_cjs_symbol"), context)
                    .map_err(sandbox_execution_error)?
                    .as_symbol()
                    .ok_or_else(|| sandbox_execution_error("symbols.imported_cjs_symbol must be a symbol"))?;
                object
                    .set(imported_cjs_symbol, JsValue::from(imported_cjs.clone()), true, context)
                    .map_err(sandbox_execution_error)?;
            }
            NodeModuleWrapState {
                url,
                wrapper: object.clone(),
                status: 0,
                synthetic: true,
                source_text: None,
                line_offset: 0,
                column_offset: 0,
                host_defined_option_id: None,
                has_top_level_await: false,
                source_url: None,
                source_map_url: None,
                synthetic_export_names: export_names,
                synthetic_evaluation_steps: args.get(3).and_then(JsValue::as_object),
                imported_cjs: args.get(4).and_then(JsValue::as_object),
                synthetic_exports: BTreeMap::new(),
                module: None,
                module_source_object: None,
                linked_request_ids: Vec::new(),
                linked: true,
                instantiated: false,
                has_async_graph: Some(false),
                error: None,
            }
        };
        host.bootstrap.borrow_mut().module_wraps.insert(id, state);
        Ok(JsValue::from(object))
    })
}

fn node_module_wrap_link(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    node_with_host_js(context, |host, context| {
        let id = node_module_wrap_id(host, &object, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let requests = {
            let state = bootstrap
                .module_wraps
                .get(&id)
                .ok_or_else(|| sandbox_execution_error("unknown ModuleWrap handle"))?;
            if state.synthetic {
                Vec::new()
            } else {
                node_module_wrap_request_entries(state.source_text.as_deref().unwrap_or_default())
            }
        };
        let modules = args
            .first()
            .and_then(JsValue::as_object)
            .ok_or_else(|| {
                node_error_with_code(
                    context,
                    JsNativeError::typ(),
                    "module.link requires an array of ModuleWrap instances",
                    "ERR_INVALID_ARG_TYPE",
                )
            })
            .map_err(sandbox_execution_error)?;
        let length = modules
            .get(js_string!("length"), context)
            .map_err(sandbox_execution_error)?
            .to_length(context)
            .map_err(sandbox_execution_error)? as usize;
        if length != requests.len() {
            return Err(sandbox_execution_error(node_error_with_code(
                context,
                JsNativeError::error(),
                "linked modules must match the number of module requests",
                "ERR_VM_MODULE_LINK_FAILURE",
            )));
        }
        let mut linked_request_ids = Vec::with_capacity(length);
        for (index, (specifier, _attributes, _phase)) in requests.iter().enumerate() {
            let value = modules.get(index as u32, context).map_err(sandbox_execution_error)?;
            let module_object = value.as_object().ok_or_else(|| {
                node_error_with_code(
                    context,
                    JsNativeError::typ(),
                    "linked module must be a ModuleWrap",
                    "ERR_INVALID_ARG_TYPE",
                )
            })
            .map_err(sandbox_execution_error)?;
            let target_id = node_module_wrap_id(host, &module_object, context)?;
            if let Some(previous_index) = requests[..index]
                .iter()
                .position(|(previous_specifier, _, _)| previous_specifier == specifier)
            {
                let previous_id = linked_request_ids[previous_index];
                if previous_id != target_id {
                    return Err(sandbox_execution_error(node_error_with_code(
                        context,
                        JsNativeError::error(),
                        format!(
                            "Module request 'ModuleCacheKey(\"{}\")' at index {} must be linked to the same module requested at index {}",
                            specifier, index, previous_index
                        ),
                        "ERR_MODULE_LINK_MISMATCH",
                    )));
                }
            }
            linked_request_ids.push(target_id);
        }
        let state = bootstrap
            .module_wraps
            .get_mut(&id)
            .ok_or_else(|| sandbox_execution_error("unknown ModuleWrap handle"))?;
        state.linked_request_ids = linked_request_ids;
        state.linked = true;
        state.status = 1;
        Ok(JsValue::undefined())
    })
}

fn node_module_wrap_get_module_requests(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::from(JsArray::new(context)?));
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_module_wrap_state(&host, &object, context).map_err(js_error)?;
    if state.synthetic {
        return Ok(JsValue::from(JsArray::new(context)?));
    }
    let requests = node_module_wrap_request_entries(state.source_text.as_deref().unwrap_or_default());
    let request_values = requests
        .into_iter()
        .map(|(specifier, attributes, phase)| {
            serde_json::json!({
                "specifier": specifier,
                "attributes": attributes,
                "phase": phase,
            })
        })
        .collect::<Vec<_>>();
    JsValue::from_json(&JsonValue::Array(request_values), context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)
}

fn node_module_wrap_has_async_graph(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_module_wrap_state(&host, &object, context).map_err(js_error)?;
    if !state.instantiated {
        return Err(node_error_with_code(
            context,
            JsNativeError::error(),
            "Module status must not be unlinked or linking",
            "ERR_MODULE_NOT_INSTANTIATED",
        ));
    }
    Ok(JsValue::from(state.has_async_graph.unwrap_or(false)))
}

fn node_module_wrap_instantiate(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    node_with_host_js(context, |host, context| {
        let id = node_module_wrap_id(host, &object, context)?;
        let (linked, module) = {
            let bootstrap = host.bootstrap.borrow();
            let state = bootstrap
                .module_wraps
                .get(&id)
                .ok_or_else(|| sandbox_execution_error("unknown ModuleWrap handle"))?;
            (state.linked, state.module.clone())
        };
        if !linked {
            return Err(sandbox_execution_error(node_error_with_code(
                context,
                JsNativeError::error(),
                "module is not linked",
                "ERR_VM_MODULE_LINK_FAILURE",
            )));
        }
        if let Some(module) = module.as_ref() {
            module.link(context).map_err(sandbox_execution_error)?;
        }
        let has_async_graph = {
            let bootstrap = host.bootstrap.borrow();
            let mut visiting = BTreeSet::new();
            node_module_wrap_compute_has_async_graph(&bootstrap.module_wraps, id, &mut visiting)
        };
        let mut bootstrap = host.bootstrap.borrow_mut();
        let state = bootstrap
            .module_wraps
            .get_mut(&id)
            .ok_or_else(|| sandbox_execution_error("unknown ModuleWrap handle"))?;
        state.instantiated = true;
        state.has_async_graph = Some(has_async_graph);
        state.status = 2;
        Ok(JsValue::undefined())
    })
}

fn node_module_wrap_evaluate_sync(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    node_with_host_js(context, |host, context| {
        let id = node_module_wrap_id(host, &object, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let state = bootstrap
            .module_wraps
            .get_mut(&id)
            .ok_or_else(|| sandbox_execution_error("unknown ModuleWrap handle"))?;
        if !state.instantiated {
            return Err(sandbox_execution_error(node_error_with_code(
                context,
                JsNativeError::error(),
                "module is not instantiated",
                "ERR_VM_MODULE_STATUS",
            )));
        }
        state.status = 3;
        let namespace = if state.synthetic {
            if let Some(callback) = state.synthetic_evaluation_steps.take() {
                let callable = JsValue::from(callback.clone())
                    .as_callable()
                    .ok_or_else(|| sandbox_execution_error("synthetic evaluation step is not callable"))?;
                if let Err(error) = callable.call(&JsValue::from(object.clone()), &[], context) {
                    state.status = 5;
                    state.error = Some(
                        error
                            .clone()
                            .into_opaque(context)
                            .map_err(sandbox_execution_error)?,
                    );
                    return Err(sandbox_execution_error(error));
                }
            }
            node_module_wrap_synthetic_namespace(state, context).map_err(sandbox_execution_error)?
        } else if let Some(module) = state.module.clone() {
            let promise = module.evaluate(context).map_err(sandbox_execution_error)?;
            if let Err(error) = promise.await_blocking(context) {
                state.status = 5;
                state.error = Some(
                    error
                        .clone()
                        .into_opaque(context)
                        .map_err(sandbox_execution_error)?,
                );
                return Err(sandbox_execution_error(error));
            }
            module.namespace(context)
        } else {
            JsObject::with_null_proto()
        };
        state.status = 4;
        Ok(JsValue::from(namespace))
    })
}

fn node_module_wrap_evaluate(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    node_with_host_js(context, |host, context| {
        let id = node_module_wrap_id(host, &object, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let state = bootstrap
            .module_wraps
            .get_mut(&id)
            .ok_or_else(|| sandbox_execution_error("unknown ModuleWrap handle"))?;
        if !state.instantiated {
            return Err(sandbox_execution_error(node_error_with_code(
                context,
                JsNativeError::error(),
                "module is not instantiated",
                "ERR_VM_MODULE_STATUS",
            )));
        }
        state.status = 3;
        let result = if state.synthetic {
            if let Some(callback) = state.synthetic_evaluation_steps.take() {
                let callable = JsValue::from(callback.clone())
                    .as_callable()
                    .ok_or_else(|| sandbox_execution_error("synthetic evaluation step is not callable"))?;
                if let Err(error) = callable.call(&JsValue::from(object.clone()), &[], context) {
                    state.status = 5;
                    state.error = Some(
                        error
                            .clone()
                            .into_opaque(context)
                            .map_err(sandbox_execution_error)?,
                    );
                    return Err(sandbox_execution_error(error));
                }
            }
            state.status = 4;
            JsValue::from(JsPromise::resolve(JsValue::undefined(), context).map_err(sandbox_execution_error)?)
        } else if let Some(module) = state.module.clone() {
            let promise = module.evaluate(context).map_err(sandbox_execution_error)?;
            let on_fulfilled = FunctionObjectBuilder::new(
                context.realm(),
                NativeFunction::from_copy_closure_with_captures(
                    |_this, args, module_id, context| {
                        node_with_host_js(context, |host, _context| {
                            if let Some(state) = host.bootstrap.borrow_mut().module_wraps.get_mut(module_id) {
                                state.status = 4;
                            }
                            Ok(JsValue::undefined())
                        })?;
                        Ok(args.first().cloned().unwrap_or_else(JsValue::undefined))
                    },
                    id,
                ),
            )
            .name(js_string!("moduleWrapFulfilled"))
            .length(1)
            .constructor(false)
            .build();
            let on_rejected = FunctionObjectBuilder::new(
                context.realm(),
                NativeFunction::from_copy_closure_with_captures(
                    |_this, args, module_id, context| {
                        let reason = args.first().cloned().unwrap_or_else(JsValue::undefined);
                        node_with_host_js(context, |host, _context| {
                            if let Some(state) = host.bootstrap.borrow_mut().module_wraps.get_mut(module_id) {
                                state.status = 5;
                                state.error = Some(reason.clone());
                            }
                            Ok(JsValue::undefined())
                        })?;
                        Err(boa_engine::JsError::from_opaque(reason))
                    },
                    id,
                ),
            )
            .name(js_string!("moduleWrapRejected"))
            .length(1)
            .constructor(false)
            .build();
            JsValue::from(
                promise
                    .then(Some(on_fulfilled), Some(on_rejected), context)
                    .map_err(sandbox_execution_error)?,
            )
        } else {
            JsValue::undefined()
        };
        Ok(result)
    })
}

fn node_module_wrap_set_export(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let name = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_string(context)?;
    let value = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    node_with_host_js(context, |host, context| {
        let id = node_module_wrap_id(host, &object, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let state = bootstrap
            .module_wraps
            .get_mut(&id)
            .ok_or_else(|| sandbox_execution_error("unknown ModuleWrap handle"))?;
        state
            .synthetic_exports
            .insert(name.to_std_string_escaped(), value);
        Ok(JsValue::undefined())
    })
}

fn node_module_wrap_set_module_source_object(
    this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let source_object = args.first().and_then(JsValue::as_object);
    node_with_host_js(context, |host, context| {
        let id = node_module_wrap_id(host, &object, context)?;
        let mut bootstrap = host.bootstrap.borrow_mut();
        let state = bootstrap
            .module_wraps
            .get_mut(&id)
            .ok_or_else(|| sandbox_execution_error("unknown ModuleWrap handle"))?;
        if state.module_source_object.is_some() {
            return Err(sandbox_execution_error(
                "ModuleWrap source object is already initialized",
            ));
        }
        state.module_source_object = source_object;
        Ok(JsValue::undefined())
    })
}

fn node_module_wrap_get_module_source_object(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_module_wrap_state(&host, &object, context).map_err(js_error)?;
    state.module_source_object.map(JsValue::from).ok_or_else(|| {
        node_error_with_code(
            context,
            JsNativeError::error(),
            format!("Source phase not defined for module '{}'", state.url),
            "ERR_SOURCE_PHASE_NOT_DEFINED",
        )
    })
}

fn node_module_wrap_create_cached_data(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::from(JsUint8Array::from_iter(
            std::iter::empty::<u8>(),
            context,
        )?));
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_module_wrap_state(&host, &object, context).map_err(js_error)?;
    if state.synthetic {
        return Err(node_error_with_code(
            context,
            JsNativeError::typ(),
            "synthetic modules do not support createCachedData()",
            "ERR_INVALID_ARG_TYPE",
        ));
    }
    if state.status >= 3 {
        return Err(node_error_with_code(
            context,
            JsNativeError::error(),
            "cannot create cached data after module evaluation has started",
            "ERR_VM_MODULE_CANNOT_CREATE_CACHED_DATA",
        ));
    }
    let bytes = if state.synthetic {
        Vec::new()
    } else {
        node_contextify_compile_cache_bytes(
            "module_wrap",
            state.source_text.as_deref().unwrap_or_default(),
            &state.url,
            state.line_offset,
            state.column_offset,
            &[],
        )
        .map_err(js_error)?
    };
    node_js_buffer_from_bytes(bytes, context)
}

fn node_module_wrap_get_namespace(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_module_wrap_state(&host, &object, context).map_err(js_error)?;
    if !state.instantiated {
        return Err(node_error_with_code(
            context,
            JsNativeError::error(),
            "Module status must not be unlinked or linking",
            "ERR_MODULE_NOT_INSTANTIATED",
        ));
    }
    if state.synthetic {
        return Ok(JsValue::from(
            node_module_wrap_synthetic_namespace(&state, context).map_err(js_error)?,
        ));
    }
    if let Some(module) = state.module {
        return Ok(JsValue::from(module.namespace(context)));
    }
    Ok(JsValue::undefined())
}

fn node_module_wrap_get_status(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::from(0));
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_module_wrap_state(&host, &object, context).map_err(js_error)?;
    Ok(JsValue::from(state.status))
}

fn node_module_wrap_get_error(
    this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = this.as_object() else {
        return Ok(JsValue::undefined());
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_module_wrap_state(&host, &object, context).map_err(js_error)?;
    Ok(state.error.unwrap_or_else(JsValue::undefined))
}

fn node_module_wrap_set_import_module_dynamically_callback(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap
            .borrow_mut()
            .module_wrap_import_module_dynamically_callback = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_module_wrap_set_initialize_import_meta_object_callback(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let callback = args.first().and_then(JsValue::as_object);
    node_with_host(|host| {
        host.bootstrap
            .borrow_mut()
            .module_wrap_initialize_import_meta_object_callback = callback;
        Ok(JsValue::undefined())
    })
    .map_err(js_error)
}

fn node_module_wrap_facade_getter(
    _this: &JsValue,
    _args: &[JsValue],
    captures: &JsObject,
    context: &mut Context,
) -> JsResult<JsValue> {
    let namespace = captures
        .get(js_string!("namespace"), context)?
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("missing facade namespace"))?;
    let key = captures.get(js_string!("key"), context)?.to_string(context)?;
    namespace.get(key, context)
}

fn node_module_wrap_create_required_module_facade(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    let host = active_node_host().map_err(js_error)?;
    let state = node_module_wrap_state(&host, &object, context).map_err(js_error)?;
    let namespace = if state.synthetic {
        node_module_wrap_synthetic_namespace(&state, context).map_err(js_error)?
    } else if let Some(module) = state.module {
        module.namespace(context)
    } else {
        JsObject::with_null_proto()
    };
    let facade = JsObject::with_null_proto();
    let keys = namespace.own_property_keys(context)?;
    for key in keys {
        if let boa_engine::property::PropertyKey::String(name) = key {
            let captures = JsObject::with_null_proto();
            captures
                .set(js_string!("namespace"), JsValue::from(namespace.clone()), true, context)
                ?;
            captures
                .set(js_string!("key"), JsValue::from(name.clone()), true, context)
                ?;
            let getter = FunctionObjectBuilder::new(
                context.realm(),
                NativeFunction::from_copy_closure_with_captures(
                    node_module_wrap_facade_getter,
                    captures,
                ),
            )
            .name(name.clone())
            .length(0)
            .constructor(false)
            .build();
            facade
                .define_property_or_throw(
                    name,
                    PropertyDescriptor::builder()
                        .get(getter)
                        .enumerable(true)
                        .configurable(false),
                    context,
                )
                ?;
        }
    }
    facade.set(js_string!("__esModule"), JsValue::from(true), true, context)?;
    Ok(JsValue::from(facade))
}

fn node_module_wrap_throw_if_promise_rejected(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    let promise = JsPromise::from_object(object.clone())
        .map_err(|_| js_error(sandbox_execution_error("expected Promise")))?;
    if let PromiseState::Rejected(reason) = promise.state() {
        return Err(boa_engine::JsError::from_opaque(reason));
    }
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
    Ok(JsValue::from(JsSharedArrayBuffer::new(size, context)?))
}

fn node_util_lazy_property_getter(
    _this: &JsValue,
    _args: &[JsValue],
    captures: &JsObject,
    context: &mut Context,
) -> JsResult<JsValue> {
    let builtin_require = captures
        .get(js_string!("builtinRequire"), context)?
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("builtin require is not callable"))?;
    let id = captures.get(js_string!("id"), context)?;
    let key = captures.get(js_string!("key"), context)?.to_string(context)?;
    let exports = builtin_require.call(&JsValue::undefined(), &[id], context)?;
    Ok(exports
        .as_object()
        .and_then(|exports| exports.get(key, context).ok())
        .unwrap_or_else(JsValue::undefined))
}

fn node_util_guess_handle_type(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let fd = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or(-1);
    let code = node_with_host(|host| {
        let process = host.process.borrow();
        let code = match fd {
            0..=2 => 4, // PIPE
            _ if host
                .bootstrap
                .borrow()
                .udp_handles
                .values()
                .any(|state| state.fd == Some(fd) && !state.closed) => 2, // UDP
            _ if host.open_files.borrow().entries.contains_key(&fd) => 3, // FILE
            _ => 5, // UNKNOWN
        };
        let _ = &process;
        Ok(JsValue::from(code))
    })
    .map_err(js_error)?;
    Ok(code)
}

fn node_util_define_lazy_properties(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(target) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    let id = node_arg_string(args, 1, context)?;
    let Some(keys) = args.get(2).and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    let enumerable = args.get(3).is_some_and(JsValue::to_boolean);
    let builtin_require = context
        .global_object()
        .get(js_string!("__terraceBuiltinRequire"), context)?;
    let builtin_require = builtin_require
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("builtin require is not callable"))?;
    let length = keys.get(js_string!("length"), context)?.to_length(context)?;
    for index in 0..length {
        let key = keys.get(index, context)?.to_string(context)?;
        let captures = JsObject::with_null_proto();
        captures
            .set(js_string!("id"), JsValue::from(JsString::from(id.clone())), true, context)?;
        captures
            .set(js_string!("key"), JsValue::from(key.clone()), true, context)?;
        captures.set(
            js_string!("builtinRequire"),
            JsValue::from(builtin_require.clone()),
            true,
            context,
        )?;
        let getter = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_copy_closure_with_captures(
                node_util_lazy_property_getter,
                captures,
            ),
        )
        .name(key.clone())
        .length(0)
        .constructor(false)
        .build();
        target
            .define_property_or_throw(
                key,
                PropertyDescriptor::builder()
                    .get(getter)
                    .enumerable(enumerable)
                    .configurable(true),
                context,
            )
            ?;
    }
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
    let only_writable = filter & 1 != 0;
    let only_enumerable = filter & 2 != 0;
    let only_configurable = filter & 4 != 0;
    let skip_strings = filter & 8 != 0;
    let skip_symbols = filter & 16 != 0;
    let keys = target.own_property_keys(context)?;
    let object_ctor = context.global_object().get(js_string!("Object"), context)?;
    let object_ctor = object_ctor
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("Object is unavailable"))?;
    let get_own_property_descriptor = object_ctor
        .get(js_string!("getOwnPropertyDescriptor"), context)?
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("Object.getOwnPropertyDescriptor is not callable"))?;
    let mut result = Vec::new();
    for key in keys {
        let key_value = match &key {
            boa_engine::property::PropertyKey::Index(index) => JsValue::from(index.get()),
            boa_engine::property::PropertyKey::String(name) => JsValue::from(name.clone()),
            boa_engine::property::PropertyKey::Symbol(symbol) => JsValue::from(symbol.clone()),
        };
        let descriptor = get_own_property_descriptor.call(
            &JsValue::from(object_ctor.clone()),
            &[JsValue::from(target.clone()), key_value],
            context,
        )?;
        let Some(descriptor) = descriptor.as_object() else {
            continue;
        };
        if only_enumerable
            && !descriptor
                .get(js_string!("enumerable"), context)?
                .to_boolean()
        {
            continue;
        }
        if only_configurable
            && !descriptor
                .get(js_string!("configurable"), context)?
                .to_boolean()
        {
            continue;
        }
        if only_writable {
            let writable = descriptor.get(js_string!("writable"), context)?.to_boolean()
                || descriptor
                    .get(js_string!("set"), context)?
                    .as_object()
                    .is_some();
            if !writable {
                continue;
            }
        }
        match key {
            boa_engine::property::PropertyKey::Index(_) => {}
            boa_engine::property::PropertyKey::String(name) if !skip_strings => {
                result.push(JsValue::from(name))
            }
            boa_engine::property::PropertyKey::String(_) => {}
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
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let extra_frames = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_u32(context)
        .unwrap_or(0) as usize;
    let stack = context
        .eval(Source::from_bytes(b"(new Error()).stack"))
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .to_string(context)?
        .to_std_string_escaped();
    let line = stack.lines().skip(2 + extra_frames).find(|line| !line.trim().is_empty());
    Ok(JsValue::from(
        line.is_some_and(|line| line.contains("/node_modules/") || line.contains("\\node_modules\\")),
    ))
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

fn node_util_sleep(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let milliseconds = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_number(context)?
        .max(0.0);
    node_with_host(|host| {
        node_advance_monotonic_now_ms(host, milliseconds);
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
    Ok(JsValue::undefined())
}

fn node_util_preview_entries(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    let entries_method = object.get(js_string!("entries"), context)?;
    let Some(entries_method) = entries_method.as_callable() else {
        return Ok(JsValue::undefined());
    };
    let iterator = entries_method.call(&JsValue::from(object.clone()), &[], context)?;
    let Some(iterator) = iterator.as_object() else {
        return Ok(JsValue::undefined());
    };
    let next = iterator.get(js_string!("next"), context)?;
    let Some(next) = next.as_callable() else {
        return Ok(JsValue::undefined());
    };
    let mut values = Vec::new();
    loop {
        let step = next.call(&JsValue::from(iterator.clone()), &[], context)?;
        let Some(step) = step.as_object() else {
            break;
        };
        if step.get(js_string!("done"), context)?.to_boolean() {
            break;
        }
        values.push(step.get(js_string!("value"), context)?);
    }
    let entries = JsValue::from(JsArray::from_iter(values, context));
    if args.len() == 1 {
        return Ok(entries);
    }
    let constructor_name = object
        .get(js_string!("constructor"), context)?
        .as_object()
        .and_then(|ctor| ctor.get(js_string!("name"), context).ok())
        .and_then(|value| value.as_string().map(|value| value.to_std_string_escaped()))
        .unwrap_or_default();
    Ok(JsValue::from(JsArray::from_iter(
        [
            entries,
            JsValue::from(constructor_name == "Map"),
        ],
        context,
    )))
}

fn node_util_get_caller_location(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let stack = context
        .eval(Source::from_bytes(b"(new Error()).stack"))
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .to_string(context)?
        .to_std_string_escaped();
    let location = stack
        .lines()
        .skip(2)
        .find_map(|line| {
            let trimmed = line.trim();
            let start = trimmed.rfind('(').map(|index| index + 1).unwrap_or(0);
            let location = trimmed[start..].trim_end_matches(')');
            let mut parts = location.rsplitn(3, ':');
            let column = parts.next()?.parse::<u32>().ok()?;
            let line = parts.next()?.parse::<u32>().ok()?;
            let file = parts.next()?.to_string();
            Some((line, column, file))
        });
    let Some((line, column, file)) = location else {
        return Ok(JsValue::undefined());
    };
    Ok(JsValue::from(JsArray::from_iter(
        [
            JsValue::from(line),
            JsValue::from(column),
            JsValue::from(JsString::from(file)),
        ],
        context,
    )))
}

fn node_util_get_call_sites(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let frames = args
        .first()
        .cloned()
        .unwrap_or_else(|| JsValue::from(10))
        .to_u32(context)?
        .clamp(1, 200) as usize;
    let stack = context
        .eval(Source::from_bytes(b"(new Error()).stack"))
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .to_string(context)?
        .to_std_string_escaped();
    let mut callsites = Vec::new();
    for line in stack.lines().skip(2).take(frames) {
        let trimmed = line.trim();
        let function_name = trimmed
            .strip_prefix("at ")
            .and_then(|rest| rest.split(" (").next())
            .unwrap_or("")
            .to_string();
        let start = trimmed.rfind('(').map(|index| index + 1).unwrap_or_else(|| {
            trimmed
                .strip_prefix("at ")
                .and_then(|rest| rest.split(' ').next())
                .map(|prefix| trimmed.len().saturating_sub(prefix.len()))
                .unwrap_or(0)
        });
        let location = trimmed[start..].trim_end_matches(')');
        let mut parts = location.rsplitn(3, ':');
        let column = parts
            .next()
            .and_then(|part| part.parse::<u32>().ok())
            .unwrap_or(0);
        let line_number = parts
            .next()
            .and_then(|part| part.parse::<u32>().ok())
            .unwrap_or(0);
        let script_name = parts.next().unwrap_or(location).to_string();
        callsites.push(serde_json::json!({
            "functionName": function_name,
            "scriptId": "0",
            "scriptName": script_name,
            "lineNumber": line_number,
            "columnNumber": column,
            "column": column,
        }));
    }
    JsValue::from_json(&JsonValue::Array(callsites), context)
        .map_err(sandbox_execution_error)
        .map_err(js_error)
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
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    if JsProxy::from_object(object.clone()).is_err() {
        return Ok(JsValue::undefined());
    }
    if args.get(1).is_none() || args.get(1).is_some_and(JsValue::to_boolean) {
        return Ok(JsValue::from(JsArray::from_iter(
            [JsValue::from(object.clone()), JsValue::undefined()],
            context,
        )));
    }
    Ok(JsValue::from(object.clone()))
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
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let Some(object) = args.first().and_then(JsValue::as_object) else {
        return Ok(JsValue::undefined());
    };
    let host = active_node_host().map_err(js_error)?;
    let symbol = node_host_private_symbol(&host, "external.value").map_err(js_error)?;
    object.get(symbol, context)
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
    let map = context
        .eval(Source::from_bytes(b"new Map()"))
        .map_err(sandbox_execution_error)
        .map_err(js_error)?
        .as_object()
        .ok_or_else(|| {
            js_error(SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: "uv.getErrorMap() failed to allocate Map".to_string(),
            })
        })?;
    let set = map
        .get(js_string!("set"), context)?
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("Map.set is not callable"))?;
    for (code, name, message) in [
        (-1, "EOF", "end of file"),
        (-2, "ENOENT", "no such file or directory"),
        (-13, "EACCES", "permission denied"),
        (-17, "EEXIST", "file already exists"),
        (-20, "ENOTDIR", "not a directory"),
        (-21, "EISDIR", "illegal operation on a directory"),
        (-22, "EINVAL", "invalid argument"),
        (-1_001, "EPERM", "operation not permitted"),
        (-3_003, "EAI_AGAIN", "temporary failure"),
        (-4_048, "UNKNOWN", "unknown error"),
    ] {
        let pair = JsArray::from_iter(
            [
                JsValue::from(JsString::from(name)),
                JsValue::from(JsString::from(message)),
            ],
            context,
        );
        let _ = set.call(
            &JsValue::from(map.clone()),
            &[JsValue::from(code), JsValue::from(pair)],
            context,
        )?;
    }
    Ok(JsValue::from(map))
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
    node_with_host(|host| {
        let process = host.process.borrow();
        let module_bytes = host
            .loaded_modules
            .borrow()
            .values()
            .map(|module| module.source.len() as u64)
            .sum::<u64>();
        let open_file_bytes = host
            .open_files
            .borrow()
            .entries
            .values()
            .map(|file| file.contents.len() as u64)
            .sum::<u64>();
        let output_bytes = (process.stdout.len() + process.stderr.len()) as u64;
        let total_mem = 512_u64 * 1024 * 1024;
        let used = (module_bytes + open_file_bytes + output_bytes).min(total_mem);
        Ok(JsValue::from((total_mem - used) as f64))
    })
    .map_err(js_error)
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
    node_with_host(|host| {
        let hostname = host
            .process
            .borrow()
            .env
            .get("HOSTNAME")
            .cloned()
            .unwrap_or_else(|| "localhost".to_string());
        Ok(JsValue::from(JsString::from(hostname)))
    })
    .map_err(js_error)
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
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let pid = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    node_with_host(|host| {
        let own_pid = host.process.borrow().pid as i32;
        if pid != 0 && pid != own_pid {
            return Err(SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: format!("ESRCH: no such process, getPriority '{pid}'"),
            });
        }
        let priority = host
            .process
            .borrow()
            .env
            .get("__TERRACE_NODE_PRIORITY")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(0);
        Ok(JsValue::from(priority))
    })
    .map_err(js_error)
}

fn node_os_get_os_information(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    node_with_host_js(context, |host, context| {
        let process = host.process.borrow();
        JsValue::from_json(
            &serde_json::json!([
                if process.platform == "win32" { "Windows_NT" } else { "Linux" },
                process
                    .env
                    .get("HOSTNAME")
                    .cloned()
                    .unwrap_or_else(|| "localhost".to_string()),
                "5.10.0",
                process.arch
            ]),
            context,
        )
        .map_err(sandbox_execution_error)
    })
}

fn node_os_get_total_mem(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<JsValue> {
    Ok(JsValue::from((512_u64 * 1024 * 1024) as f64))
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
    node_with_host(|host| {
        Ok(JsValue::from(
            host.bootstrap.borrow().monotonic_now_ms / 1000.0,
        ))
    })
    .map_err(js_error)
}

fn node_os_set_priority(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let pid = args
        .first()
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    let priority = args
        .get(1)
        .cloned()
        .unwrap_or_else(JsValue::undefined)
        .to_i32(context)
        .unwrap_or_default();
    node_with_host(|host| {
        let own_pid = host.process.borrow().pid as i32;
        if pid != 0 && pid != own_pid {
            return Err(SandboxError::Execution {
                entrypoint: "<node-runtime>".to_string(),
                message: format!("ESRCH: no such process, setPriority '{pid}'"),
            });
        }
        host.process
            .borrow_mut()
            .env
            .insert("__TERRACE_NODE_PRIORITY".to_string(), priority.to_string());
        Ok(JsValue::undefined())
    })
    .map_err(js_error)?;
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

fn node_fs_open_impl(
    host: &NodeRuntimeHost,
    path: &str,
    flags: NodeFsOpenFlags,
) -> Result<i32, SandboxError> {
    node_debug_event(host, "fs", format!("open {path}"))?;
    let path = resolve_node_path(&host.process.borrow().cwd, path);
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
    Ok(fd)
}

fn node_fs_close_impl(host: &NodeRuntimeHost, fd: i32) -> Result<(), SandboxError> {
    node_debug_event(host, "fs", format!("close {fd}"))?;
    if fd == 0 || fd == 1 || fd == 2 {
        return Ok(());
    }
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
    Ok(())
}

fn node_fs_read_impl(
    host: &NodeRuntimeHost,
    fd: i32,
    length: usize,
    position: Option<usize>,
) -> Result<Vec<u8>, SandboxError> {
    node_debug_event(host, "fs", format!("read-fd {fd} len={length}"))?;
    if fd == 0 {
        return Ok(Vec::new());
    }
    if fd == 1 || fd == 2 {
        return Err(SandboxError::Service {
            service: "node_runtime",
            message: "EBADF: bad file descriptor, read".to_string(),
        });
    }
    let mut open_files = host.open_files.borrow_mut();
    let entry = open_files
        .entries
        .get_mut(&fd)
        .ok_or_else(|| SandboxError::Service {
            service: "node_runtime",
            message: "EBADF: bad file descriptor, read".to_string(),
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
    Ok(entry.contents[start..end].to_vec())
}

fn node_fs_write_impl(
    host: &NodeRuntimeHost,
    fd: i32,
    data: &[u8],
    position: Option<usize>,
) -> Result<usize, SandboxError> {
    node_debug_event(host, "fs", format!("write-fd {fd} len={}", data.len()))?;
    if fd == 1 {
        host.process
            .borrow_mut()
            .stdout
            .push_str(&String::from_utf8_lossy(data));
        return Ok(data.len());
    }
    if fd == 2 {
        host.process
            .borrow_mut()
            .stderr
            .push_str(&String::from_utf8_lossy(data));
        return Ok(data.len());
    }
    if fd == 0 {
        return Err(SandboxError::Service {
            service: "node_runtime",
            message: "EBADF: bad file descriptor, write".to_string(),
        });
    }
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
        entry.contents[start..end].copy_from_slice(data);
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
    Ok(data.len())
}

fn node_fs_open(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = node_arg_string(args, 0, context)?;
    let flags = node_fs_open_flags_from_js(args, 1, context)?;
    node_with_host(|host| node_fs_open_impl(host, &path, flags).map(JsValue::from))
    .map_err(js_error)
}

fn node_fs_close(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let fd = node_arg_i32(args, 0, context)?;
    node_with_host(|host| node_fs_close_impl(host, fd).map(|_| JsValue::undefined()))
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
        let bytes = node_fs_read_impl(host, fd, length, position)?;
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
        node_fs_write_impl(host, fd, &data, position).map(|written| JsValue::from(written as u32))
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

fn node_spawn_sync_spawn(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let options = args
        .first()
        .and_then(JsValue::as_object)
        .ok_or_else(|| JsNativeError::typ().with_message("options must be an object"))?;
    node_with_host_js(context, |host, context| {
        let request = node_spawn_sync_request_from_options(host, &options, context)?;
        let result = execute_child_process_request(host, request)?;
        node_spawn_sync_result_to_js(host, &result, &options, context)
    })
}

fn node_spawn_sync_request_from_options(
    host: &NodeRuntimeHost,
    options: &JsObject,
    context: &mut Context,
) -> Result<NodeChildProcessRequest, SandboxError> {
    let command = options
        .get(js_string!("file"), context)
        .map_err(sandbox_execution_error)?
        .to_string(context)
        .map_err(sandbox_execution_error)?
        .to_std_string_escaped();

    let args_value = options
        .get(js_string!("args"), context)
        .map_err(sandbox_execution_error)?;
    let mut argv = Vec::new();
    if let Some(args_array) = args_value.as_object() {
        let len = args_array
            .get(js_string!("length"), context)
            .map_err(sandbox_execution_error)?
            .to_length(context)
            .map_err(sandbox_execution_error)?;
        for index in 0..len {
            argv.push(
                args_array
                    .get(index, context)
                    .map_err(sandbox_execution_error)?
                    .to_string(context)
                    .map_err(sandbox_execution_error)?
                    .to_std_string_escaped(),
            );
        }
    }

    let cwd = options
        .get(js_string!("cwd"), context)
        .map_err(sandbox_execution_error)?
        .as_string()
        .map(|value| value.to_std_string_escaped());

    let mut env = BTreeMap::new();
    if let Some(env_pairs) = options
        .get(js_string!("envPairs"), context)
        .map_err(sandbox_execution_error)?
        .as_object()
    {
        let len = env_pairs
            .get(js_string!("length"), context)
            .map_err(sandbox_execution_error)?
            .to_length(context)
            .map_err(sandbox_execution_error)?;
        for index in 0..len {
            let pair = env_pairs
                .get(index, context)
                .map_err(sandbox_execution_error)?
                .to_string(context)
                .map_err(sandbox_execution_error)?
                .to_std_string_escaped();
            if let Some((key, value)) = pair.split_once('=') {
                env.insert(key.to_string(), value.to_string());
            }
        }
    }
    if env.is_empty() {
        env = host.process.borrow().env.clone();
    }

    let shell = options
        .get(js_string!("shell"), context)
        .map_err(sandbox_execution_error)?
        .to_boolean();

    let stdio_value = options
        .get(js_string!("stdio"), context)
        .map_err(sandbox_execution_error)?;
    let input = node_spawn_sync_input_from_stdio(&stdio_value, context)?;

    Ok(NodeChildProcessRequest {
        command,
        args: argv.into_iter().skip(1).collect(),
        cwd,
        env,
        shell,
        input,
    })
}

fn node_spawn_sync_input_from_stdio(
    stdio_value: &JsValue,
    context: &mut Context,
) -> Result<Option<String>, SandboxError> {
    let Some(stdio_array) = stdio_value.as_object() else {
        return Ok(None);
    };
    let stdin = stdio_array
        .get(0, context)
        .map_err(sandbox_execution_error)?;
    let Some(stdin) = stdin.as_object() else {
        return Ok(None);
    };
    let input = stdin
        .get(js_string!("input"), context)
        .map_err(sandbox_execution_error)?;
    if input.is_null() || input.is_undefined() {
        return Ok(None);
    }
    let bytes = node_bytes_from_js(&input, context).map_err(sandbox_execution_error)?;
    Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
}

fn node_spawn_sync_result_to_js(
    _host: &NodeRuntimeHost,
    result: &NodeChildProcessResult,
    options: &JsObject,
    context: &mut Context,
) -> Result<JsValue, SandboxError> {
    let object = ObjectInitializer::new(context).build();
    let pid = result.pid.unwrap_or(0);
    object
        .set(js_string!("pid"), JsValue::from(pid), true, context)
        .map_err(sandbox_execution_error)?;

    let status = match (&result.status, &result.signal) {
        (_, Some(_)) => JsValue::null(),
        (Some(value), None) => JsValue::from(*value),
        (None, None) => JsValue::null(),
    };
    object
        .set(js_string!("status"), status, true, context)
        .map_err(sandbox_execution_error)?;
    object
        .set(
            js_string!("signal"),
            result
                .signal
                .as_ref()
                .map(|value| JsValue::from(JsString::from(value.as_str())))
                .unwrap_or_else(JsValue::null),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;

    let output = if result.status.is_some() || result.signal.is_some() {
        Some(node_spawn_sync_output_array(result, options, context)?)
    } else {
        None
    };
    object
        .set(
            js_string!("output"),
            output.map(JsValue::from).unwrap_or_else(JsValue::null),
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;

    if let Some(error) = result.error.as_ref() {
        object
            .set(
                js_string!("error"),
                JsValue::from(node_spawn_sync_error_number(error)),
                true,
                context,
            )
            .map_err(sandbox_execution_error)?;
    }

    Ok(JsValue::from(object))
}

fn node_spawn_sync_output_array(
    result: &NodeChildProcessResult,
    options: &JsObject,
    context: &mut Context,
) -> Result<JsObject, SandboxError> {
    let stdio_len = options
        .get(js_string!("stdio"), context)
        .map_err(sandbox_execution_error)?
        .as_object()
        .map(|array| {
            array
                .get(js_string!("length"), context)
                .map_err(sandbox_execution_error)?
                .to_length(context)
                .map_err(sandbox_execution_error)
        })
        .transpose()?
        .unwrap_or(3)
        .max(3);

    let array = JsArray::new(context).map_err(sandbox_execution_error)?;
    for index in 0..stdio_len {
        array
            .set(index, JsValue::null(), true, context)
            .map_err(sandbox_execution_error)?;
    }
    array
        .set(
            1,
            node_js_buffer_from_bytes(result.stdout.as_bytes().to_vec(), context)
                .map_err(sandbox_execution_error)?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    array
        .set(
            2,
            node_js_buffer_from_bytes(result.stderr.as_bytes().to_vec(), context)
                .map_err(sandbox_execution_error)?,
            true,
            context,
        )
        .map_err(sandbox_execution_error)?;
    Ok(array.into())
}

fn node_spawn_sync_error_number(error: &NodeChildProcessError) -> i32 {
    match error.code.as_str().unwrap_or_default() {
        "ENOENT" => -2,
        "EACCES" => -13,
        "EINVAL" => NODE_UV_EINVAL,
        "ENOTCONN" => NODE_UV_ENOTCONN,
        "EDESTADDRREQ" => NODE_UV_EDESTADDRREQ,
        _ => NODE_UV_EINVAL,
    }
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
        bootstrap: Rc::new(RefCell::new(NodeBootstrapState {
            monotonic_now_ms: node_monotonic_now_ms(host),
            ..Default::default()
        })),
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
