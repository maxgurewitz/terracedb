use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use terracedb::{
    DomainCpuBudget, ExecutionDomainBudget, ExecutionDomainOwner, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionDomainSpec, ExecutionResourceUsage, InMemoryResourceManager,
};
use terracedb_sandbox::{
    CloseSessionOptions, ConflictPolicy, PackageCompatibilityMode, SandboxBaseLayer,
    SandboxBatchedDomainMemoryBudget, SandboxConfig, SandboxError, SandboxHarness,
    SandboxRuntimeMemoryBudget, SandboxServices,
    SandboxTrackedMemoryBudgetPolicy,
};
use terracedb_systemtest::{
    SimulationCaseContext, SimulationCaseSpec, SimulationDomainConfig, SimulationHarness,
    SimulationHarnessError, SimulationSuiteDefinition, SimulationSuiteReport,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeId, VolumeStore};
use tokio::sync::OnceCell;

#[path = "tracing.rs"]
mod tracing_support;

const CACHED_NODE_BASE_VOLUME_ID: VolumeId = VolumeId::new(0xA9F0_0000_0000_0001);
const SESSION_VOLUME_BASE: u128 = 0xAAF0_0000_0000_0000;
const GENERATED_UPSTREAM_NODE_MAX_WORKERS: usize = 3;
const GENERATED_UPSTREAM_NODE_TOTAL_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
const GENERATED_UPSTREAM_NODE_MIN_CASE_MEMORY_BUDGET_BYTES: u64 = 48 * 1024 * 1024;
const GENERATED_UPSTREAM_NODE_CASE_MEMORY_HEADROOM_MULTIPLIER: u64 = 2;
const GENERATED_UPSTREAM_NODE_ALLOCATION_BATCH_BYTES: u64 = 4 * 1024 * 1024;
const NODE_EXECUTION_TIMEOUT_HEADROOM: Duration = Duration::from_millis(250);
static NEXT_SESSION_SUFFIX: AtomicU64 = AtomicU64::new(1);
static SHARED_UPSTREAM_NODE_HARNESS: OnceCell<SandboxHarness<InMemoryVfsStore>> =
    OnceCell::const_new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GeneratedUpstreamNodeCase {
    pub case_id: &'static str,
    pub path: &'static str,
    pub timeout_secs: u64,
}

impl GeneratedUpstreamNodeCase {
    pub const fn new(case_id: &'static str, path: &'static str, timeout_secs: u64) -> Self {
        Self {
            case_id,
            path,
            timeout_secs,
        }
    }
}

#[derive(Clone)]
struct GeneratedUpstreamNodeFixture {
    harness: Arc<SandboxHarness<InMemoryVfsStore>>,
    base_volume_id: VolumeId,
}

struct GeneratedUpstreamNodeSuite {
    cases: Vec<GeneratedUpstreamNodeCase>,
    case_requested_usage: ExecutionResourceUsage,
    per_case_accounted_memory_budget_bytes: u64,
    calibration_case_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GeneratedUpstreamNodeBudget {
    case_requested_usage: ExecutionResourceUsage,
    per_case_accounted_memory_budget_bytes: u64,
    calibration_case_id: &'static str,
}

async fn shared_upstream_node_harness() -> &'static SandboxHarness<InMemoryVfsStore> {
    SHARED_UPSTREAM_NODE_HARNESS
        .get_or_init(|| async {
            tracing_support::init_tracing();
            let harness = SandboxHarness::deterministic(510, 121, SandboxServices::deterministic());
            SandboxBaseLayer::vendored_node_v24_14_1_js_tree()
                .ensure_volume(harness.volumes().as_ref(), CACHED_NODE_BASE_VOLUME_ID)
                .await
                .expect("import shared node base layer");
            harness
        })
        .await
}

pub fn stdout_stderr_exit(
    result: &terracedb_sandbox::SandboxExecutionResult,
) -> (String, String, i64) {
    let report = result
        .result
        .as_ref()
        .and_then(|value| value.as_object())
        .expect("node command report");
    let stdout = report
        .get("stdout")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let stderr = report
        .get("stderr")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let exit_code = report
        .get("exitCode")
        .and_then(|value| value.as_i64())
        .unwrap_or_default();
    (stdout, stderr, exit_code)
}

async fn exec_upstream_node_test_in_harness(
    harness: &SandboxHarness<InMemoryVfsStore>,
    base_volume_id: VolumeId,
    entrypoint: &str,
    wall_time_timeout: Option<Duration>,
    memory_budget: Option<Arc<dyn SandboxRuntimeMemoryBudget>>,
) -> Result<terracedb_sandbox::SandboxExecutionResult, SandboxError> {
    let session_suffix = NEXT_SESSION_SUFFIX.fetch_add(1, Ordering::Relaxed) as u128;
    let session_volume_id =
        VolumeId::new(SESSION_VOLUME_BASE + 0x1000_0000_0000_0000 + session_suffix);
    let session = harness
        .open_session_with(
            base_volume_id,
            session_volume_id,
            |config| SandboxConfig {
                workspace_root: "/workspace".to_string(),
                package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
                conflict_policy: ConflictPolicy::Fail,
                ..config
            },
        )
        .await
        .expect("open node sandbox session");
    let runtime_entrypoint = if entrypoint.starts_with("/node/") {
        entrypoint.to_string()
    } else {
        format!("/node{entrypoint}")
    };
    if let Some(memory_budget) = memory_budget {
        session.set_runtime_memory_budget(memory_budget);
    }
    let argv = vec!["/usr/bin/node".to_string(), runtime_entrypoint.clone()];
    let env = BTreeMap::from([
        ("HOME".to_string(), "/workspace/home".to_string()),
        ("NODE_SKIP_FLAG_CHECK".to_string(), "1".to_string()),
    ]);
    let result = if let Some(timeout) = wall_time_timeout.map(inner_node_execution_timeout) {
        session
            .exec_node_command_with_timeout(
                &runtime_entrypoint,
                argv,
                "/node/test/parallel".to_string(),
                env,
                timeout,
            )
            .await
    } else {
        session
            .exec_node_command(
                &runtime_entrypoint,
                argv,
                "/node/test/parallel".to_string(),
                env,
            )
            .await
    };
    let close_result = session.close(CloseSessionOptions { flush: false }).await;
    drop(session);
    let delete_result = harness.volumes().delete_volume(session_volume_id).await;
    match result {
        Ok(result) => {
            close_result?;
            delete_result.map_err(SandboxError::from)?;
            Ok(result)
        }
        Err(error) => {
            close_result?;
            delete_result.map_err(SandboxError::from)?;
            Err(error)
        }
    }
}

pub fn inner_node_execution_timeout(timeout: Duration) -> Duration {
    timeout
        .checked_sub(NODE_EXECUTION_TIMEOUT_HEADROOM)
        .filter(|timeout| !timeout.is_zero())
        .unwrap_or(timeout)
}

pub async fn open_node_session(
    now: u64,
    seed: u64,
    entrypoint: &str,
    source: &str,
) -> terracedb_sandbox::SandboxSession {
    tracing_support::init_tracing();
    let session_suffix = NEXT_SESSION_SUFFIX.fetch_add(1, Ordering::Relaxed) as u128;
    let harness = SandboxHarness::deterministic(now, seed, SandboxServices::deterministic());
    let session_volume_id =
        VolumeId::new(SESSION_VOLUME_BASE + ((seed as u128) << 16) + session_suffix);
    let session = harness
        .open_session_from_base_layer_with(
            &SandboxBaseLayer::vendored_node_v24_14_1_js_tree(),
            CACHED_NODE_BASE_VOLUME_ID,
            session_volume_id,
            |config| SandboxConfig {
                workspace_root: "/workspace".to_string(),
                package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
                conflict_policy: ConflictPolicy::Fail,
                ..config
            },
        )
        .await
        .expect("open node sandbox session");
    session
        .filesystem()
        .write_file(
            entrypoint,
            source.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write node fixture");
    session
}

pub async fn exec_node_fixture(
    source: &str,
) -> Result<terracedb_sandbox::SandboxExecutionResult, terracedb_sandbox::SandboxError> {
    exec_node_fixture_at("/workspace/app/index.cjs", source).await
}

pub async fn exec_node_fixture_at(
    entrypoint: &str,
    source: &str,
) -> Result<terracedb_sandbox::SandboxExecutionResult, terracedb_sandbox::SandboxError> {
    let session = open_node_session(510, 121, entrypoint, source).await;
    session
        .exec_node_command(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
}

pub async fn exec_node_fixture_with_seed(
    now: u64,
    seed: u64,
    source: &str,
) -> Result<terracedb_sandbox::SandboxExecutionResult, SandboxError> {
    let entrypoint = "/workspace/app/index.cjs";
    let session = open_node_session(now, seed, entrypoint, source).await;
    session
        .exec_node_command(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
}

pub async fn exec_upstream_node_test(
    entrypoint: &str,
) -> Result<terracedb_sandbox::SandboxExecutionResult, SandboxError> {
    let harness = shared_upstream_node_harness().await;
    exec_upstream_node_test_in_harness(
        harness,
        CACHED_NODE_BASE_VOLUME_ID,
        entrypoint,
        None,
        None,
    )
    .await
}

pub async fn run_generated_upstream_node_suite(
    cases: &'static [GeneratedUpstreamNodeCase],
) -> Result<SimulationSuiteReport, SimulationHarnessError> {
    let selected_cases = selected_generated_upstream_node_cases(cases);
    if selected_cases.is_empty() {
        return Ok(SimulationSuiteReport::default());
    }
    let budget = calibrate_generated_upstream_node_budget(&selected_cases).await?;
    let max_workers = generated_upstream_node_max_workers();
    let resource_manager = Arc::new(InMemoryResourceManager::new(
        ExecutionDomainBudget::default(),
    ));
    SimulationHarness::new()
        .with_max_workers(max_workers)
        .with_domain(SimulationDomainConfig {
            resource_manager,
            domain_spec: ExecutionDomainSpec {
                path: ExecutionDomainPath::new(["process", "sandbox-tests", "node-compat"]),
                owner: ExecutionDomainOwner::Subsystem {
                    database: None,
                    name: "node-compat".to_string(),
                },
                budget: ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(max_workers as u32),
                        weight: Some(max_workers as u32),
                    },
                    memory: terracedb::DomainMemoryBudget {
                        total_bytes: Some(GENERATED_UPSTREAM_NODE_TOTAL_MEMORY_BUDGET_BYTES),
                        cache_bytes: None,
                        mutable_bytes: None,
                    },
                    ..Default::default()
                },
                placement: ExecutionDomainPlacement::Dedicated,
                metadata: BTreeMap::from([(
                    "terracedb.systemtest.suite".to_string(),
                    "generated-node-upstream".to_string(),
                )]),
            },
            default_case_usage: budget.case_requested_usage,
        })
        .run_suite(Arc::new(GeneratedUpstreamNodeSuite {
            cases: selected_cases,
            case_requested_usage: budget.case_requested_usage,
            per_case_accounted_memory_budget_bytes: budget.per_case_accounted_memory_budget_bytes,
            calibration_case_id: budget.calibration_case_id.to_string(),
        }))
        .await
}

async fn calibrate_generated_upstream_node_budget(
    cases: &[GeneratedUpstreamNodeCase],
) -> Result<GeneratedUpstreamNodeBudget, SimulationHarnessError> {
    let calibration_case = cases[0];
    eprintln!(
        "generated-node-upstream calibration start {}",
        calibration_case.case_id
    );
    let harness = SandboxHarness::deterministic(510, 121, SandboxServices::deterministic());
    SandboxBaseLayer::vendored_node_v24_14_1_js_tree()
        .ensure_volume(harness.volumes().as_ref(), CACHED_NODE_BASE_VOLUME_ID)
        .await
        .map_err(|error| SimulationHarnessError::Setup {
            message: format!("import calibration node base layer: {error}"),
        })?;
    let result = exec_upstream_node_test_in_harness(
        &harness,
        CACHED_NODE_BASE_VOLUME_ID,
        calibration_case.path,
        Some(Duration::from_secs(calibration_case.timeout_secs)),
        None,
    )
    .await
    .map_err(|error| SimulationHarnessError::Runtime {
        message: format!(
            "sandbox execution failed for calibration case {}: {error}",
            calibration_case.path
        ),
    })?;
    let accounted_bytes =
        node_runtime_accounted_bytes(&result).ok_or_else(|| SimulationHarnessError::Runtime {
            message: format!(
                "node runtime did not publish accounted memory stats for calibration case {}",
                calibration_case.path
            ),
        })?;
    let per_case_accounted_memory_budget_bytes = accounted_bytes
        .max(GENERATED_UPSTREAM_NODE_MIN_CASE_MEMORY_BUDGET_BYTES)
        .saturating_mul(GENERATED_UPSTREAM_NODE_CASE_MEMORY_HEADROOM_MULTIPLIER);
    eprintln!(
        "generated-node-upstream calibration done {} accounted_bytes={}",
        calibration_case.case_id, accounted_bytes
    );
    Ok(GeneratedUpstreamNodeBudget {
        case_requested_usage: ExecutionResourceUsage {
            cpu_workers: 1,
            ..Default::default()
        },
        per_case_accounted_memory_budget_bytes,
        calibration_case_id: calibration_case.case_id,
    })
}

fn generated_upstream_node_max_workers() -> usize {
    std::env::var("TERRACE_GENERATED_NODE_MAX_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(GENERATED_UPSTREAM_NODE_MAX_WORKERS)
}

fn selected_generated_upstream_node_cases(
    cases: &[GeneratedUpstreamNodeCase],
) -> Vec<GeneratedUpstreamNodeCase> {
    let Some(filter) = std::env::var("TERRACE_GENERATED_NODE_CASE_FILTER")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return cases.to_vec();
    };

    cases
        .iter()
        .copied()
        .filter(|case| case.case_id.contains(&filter) || case.path.contains(&filter))
        .collect()
}

fn node_runtime_accounted_bytes(result: &terracedb_sandbox::SandboxExecutionResult) -> Option<u64> {
    result
        .metadata
        .get("node_runtime_accounted_bytes")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            result
                .metadata
                .get("node_runtime_managed_bytes")
                .and_then(serde_json::Value::as_u64)
        })
}

fn node_runtime_managed_bytes(result: &terracedb_sandbox::SandboxExecutionResult) -> Option<u64> {
    result
        .metadata
        .get("node_runtime_managed_bytes")
        .and_then(serde_json::Value::as_u64)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RealNodeExecution {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

pub fn real_node_available() -> bool {
    Command::new("node")
        .arg("-v")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn unique_temp_node_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("terrace-node-parity-{label}-{nanos}.cjs"))
}

fn unique_temp_node_path_with_extension(label: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("terrace-node-parity-{label}-{nanos}.{extension}"))
}

pub fn exec_real_node_file(source: &str) -> std::io::Result<RealNodeExecution> {
    exec_real_node_file_with_extension("file", "cjs", source)
}

pub fn exec_real_node_file_with_extension(
    label: &str,
    extension: &str,
    source: &str,
) -> std::io::Result<RealNodeExecution> {
    let path = unique_temp_node_path_with_extension(label, extension);
    fs::write(&path, source)?;
    let output = Command::new("node").arg(&path).output()?;
    let _ = fs::remove_file(&path);
    Ok(RealNodeExecution {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        exit_code: output.status.code().unwrap_or_default(),
    })
}

pub fn exec_real_node_eval(source: &str) -> std::io::Result<RealNodeExecution> {
    let output = Command::new("node").arg("-e").arg(source).output()?;
    Ok(RealNodeExecution {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        exit_code: output.status.code().unwrap_or_default(),
    })
}

pub fn exec_real_node_eval_with_args(
    args: &[&str],
    source: &str,
) -> std::io::Result<RealNodeExecution> {
    let output = Command::new("node").args(args).arg(source).output()?;
    Ok(RealNodeExecution {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        exit_code: output.status.code().unwrap_or_default(),
    })
}

#[async_trait(?Send)]
impl SimulationSuiteDefinition for GeneratedUpstreamNodeSuite {
    type Fixture = GeneratedUpstreamNodeFixture;
    type Case = GeneratedUpstreamNodeCase;

    async fn prepare(&self) -> Result<Arc<Self::Fixture>, SimulationHarnessError> {
        tracing_support::init_tracing();
        let harness = Arc::new(SandboxHarness::deterministic(
            510,
            121,
            SandboxServices::deterministic(),
        ));
        SandboxBaseLayer::vendored_node_v24_14_1_js_tree()
            .ensure_volume(harness.volumes().as_ref(), CACHED_NODE_BASE_VOLUME_ID)
            .await
            .map_err(|error| SimulationHarnessError::Setup {
                message: format!("import shared node base layer: {error}"),
            })?;
        Ok(Arc::new(GeneratedUpstreamNodeFixture {
            harness,
            base_volume_id: CACHED_NODE_BASE_VOLUME_ID,
        }))
    }

    async fn cases(
        &self,
        _fixture: Arc<Self::Fixture>,
    ) -> Result<Vec<SimulationCaseSpec<Self::Case>>, SimulationHarnessError> {
        Ok(self
            .cases
            .iter()
            .copied()
            .map(|case| {
                SimulationCaseSpec::new(
                    case.case_id,
                    case.path,
                    case,
                    Duration::from_secs(case.timeout_secs),
                )
                .with_requested_usage(self.case_requested_usage)
            })
            .collect())
    }

    async fn run_case(
        &self,
        fixture: Arc<Self::Fixture>,
        case: SimulationCaseSpec<Self::Case>,
        ctx: SimulationCaseContext,
    ) -> Result<(), SimulationHarnessError> {
        eprintln!("generated-node-upstream start {}", case.case_id);
        let memory_budget = ctx.domain_usage().map(|domain_usage| {
            Arc::new(SandboxBatchedDomainMemoryBudget::new(
                domain_usage,
                SandboxTrackedMemoryBudgetPolicy {
                    budget_bytes: Some(self.per_case_accounted_memory_budget_bytes),
                    allocation_batch_bytes: GENERATED_UPSTREAM_NODE_ALLOCATION_BATCH_BYTES,
                },
            )) as Arc<dyn SandboxRuntimeMemoryBudget>
        });
        let result = exec_upstream_node_test_in_harness(
            fixture.harness.as_ref(),
            fixture.base_volume_id,
            case.input.path,
            Some(case.timeout),
            memory_budget,
        )
        .await
        .map_err(|error| SimulationHarnessError::Runtime {
            message: format!("sandbox execution failed for {}: {error}", case.input.path),
        })?;
        let (stdout, stderr, exit_code) = stdout_stderr_exit(&result);
        if exit_code != 0 {
            return Err(SimulationHarnessError::Runtime {
                message: format!(
                    "non-zero exit for {}\nexit: {}\nstdout:\n{}\nstderr:\n{}",
                    case.input.path, exit_code, stdout, stderr
                ),
            });
        }
        let accounted_bytes = node_runtime_accounted_bytes(&result).ok_or_else(|| {
            SimulationHarnessError::Runtime {
                message: format!(
                    "missing node runtime accounted memory stats for {}",
                    case.input.path
                ),
            }
        })?;
        if accounted_bytes > self.per_case_accounted_memory_budget_bytes {
            return Err(SimulationHarnessError::Runtime {
                message: format!(
                    "node runtime accounted memory budget exceeded for {}\naccounted_bytes: {}\nmanaged_bytes: {:?}\nper_case_budget_bytes: {}\ncalibration_case: {}",
                    case.input.path,
                    accounted_bytes,
                    node_runtime_managed_bytes(&result),
                    self.per_case_accounted_memory_budget_bytes,
                    self.calibration_case_id,
                ),
            });
        }
        eprintln!(
            "generated-node-upstream done {} accounted_bytes={}",
            case.case_id, accounted_bytes
        );
        Ok(())
    }
}

pub fn graceful_fs_repro_source() -> &'static str {
    r#"
const fs = require("fs");

fs.mkdirSync("/workspace/app/graceful-fs");

fs.writeFileSync(
  "/workspace/app/graceful-fs/clone.js",
  `'use strict'
module.exports = clone
var getPrototypeOf = Object.getPrototypeOf || function (obj) { return obj.__proto__ }
function clone (obj) {
  if (obj === null || typeof obj !== 'object') return obj
  var copy = obj instanceof Object ? { __proto__: getPrototypeOf(obj) } : Object.create(null)
  Object.getOwnPropertyNames(obj).forEach(function (key) {
    Object.defineProperty(copy, key, Object.getOwnPropertyDescriptor(obj, key))
  })
  return copy
}`,
);

fs.writeFileSync(
  "/workspace/app/graceful-fs/legacy-streams.js",
  `module.exports = function legacy () { return { ReadStream: function(){}, WriteStream: function(){} } }`,
);

fs.writeFileSync(
  "/workspace/app/graceful-fs/polyfills.js",
  `var constants = require('constants')
var origCwd = process.cwd
var cwd = null
process.cwd = function() {
  if (!cwd) cwd = origCwd.call(process)
  return cwd
}
try { process.cwd() } catch (er) {}
module.exports = function patch (fs) {
  if (constants.hasOwnProperty('O_SYMLINK') && process.version.match(/^v0\\.6\\.[0-2]|^v0\\.5\\./)) {
    fs.__terraceNeverRuns = true
  }
  if (!fs.lutimes) fs.lutimes = function (_path, _at, _mt, cb) { if (cb) process.nextTick(cb) }
  fs.stat = fs.stat
  fs.statSync = fs.statSync
}`,
);

fs.writeFileSync(
  "/workspace/app/graceful-fs/graceful-fs.js",
  `var fs = require('fs')
var polyfills = require('./polyfills.js')
var legacy = require('./legacy-streams.js')
var clone = require('./clone.js')
var util = require('util')
var gracefulQueue
var previousSymbol
if (typeof Symbol === 'function' && typeof Symbol.for === 'function') {
  gracefulQueue = Symbol.for('graceful-fs.queue')
  previousSymbol = Symbol.for('graceful-fs.previous')
} else {
  gracefulQueue = '___graceful-fs.queue'
  previousSymbol = '___graceful-fs.previous'
}
function noop () {}
function publishQueue(context, queue) {
  Object.defineProperty(context, gracefulQueue, { get: function () { return queue } })
}
var debug = noop
if (util.debuglog) debug = util.debuglog('gfs4')
if (!fs[gracefulQueue]) {
  var queue = global[gracefulQueue] || []
  publishQueue(fs, queue)
  fs.close = (function (fs$close) {
    function close (fd, cb) {
      return fs$close.call(fs, fd, function (err) {
        if (!err) { /* resetQueue intentionally omitted */ }
        if (typeof cb === 'function') cb.apply(this, arguments)
      })
    }
    Object.defineProperty(close, previousSymbol, { value: fs$close })
    return close
  })(fs.close)
  fs.closeSync = (function (fs$closeSync) {
    function closeSync (fd) {
      fs$closeSync.apply(fs, arguments)
    }
    Object.defineProperty(closeSync, previousSymbol, { value: fs$closeSync })
    return closeSync
  })(fs.closeSync)
}
if (!global[gracefulQueue]) publishQueue(global, fs[gracefulQueue])
module.exports = patch(clone(fs))
function patch (fs) {
  polyfills(fs)
  fs.gracefulify = patch
  fs.createReadStream = createReadStream
  fs.createWriteStream = createWriteStream
  var fs$readFile = fs.readFile
  fs.readFile = function readFile (path, options, cb) {
    if (typeof options === 'function') cb = options, options = null
    return fs$readFile(path, options, cb)
  }
  var fs$WriteStream = fs.WriteStream
  if (fs$WriteStream) {
    WriteStream.prototype = Object.create(fs$WriteStream.prototype)
    WriteStream.prototype.open = function WriteStream$open () {}
  }
  Object.defineProperty(fs, 'FileReadStream', {
    get: function () { return ReadStream },
    set: function (val) { ReadStream = val },
    enumerable: true,
    configurable: true
  })
  Object.defineProperty(fs, 'FileWriteStream', {
    get: function () { return WriteStream },
    set: function (val) { WriteStream = val },
    enumerable: true,
    configurable: true
  })
  function ReadStream () {}
  function WriteStream () {}
  function createReadStream () { return new fs.FileReadStream() }
  function createWriteStream () { return new fs.FileWriteStream() }
  return fs
}`,
);

console.log("before-require");
const gracefulFs = require("/workspace/app/graceful-fs/graceful-fs.js");
console.log("after-require:" + typeof gracefulFs.gracefulify);
const nodeFs = require("node:fs");
console.log("before-gracefulify:" + typeof nodeFs.writeFileSync);
const patched = gracefulFs.gracefulify(nodeFs);
console.log("after-gracefulify:" + typeof patched.writeFileSync);
require("fs").writeFileSync("/workspace/app/after.txt", "ok");
console.log("after-write");
"#
}
