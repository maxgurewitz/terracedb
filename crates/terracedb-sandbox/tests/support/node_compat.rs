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

use terracedb_systemtest::{
    SimulationCaseSpec, SimulationHarness, SimulationHarnessError, SimulationSuiteDefinition,
    SimulationSuiteReport,
};
use terracedb_sandbox::{
    ConflictPolicy, PackageCompatibilityMode, SandboxBaseLayer, SandboxConfig, SandboxError,
    SandboxHarness, SandboxServices,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeId};
use tokio::sync::OnceCell;

#[path = "tracing.rs"]
mod tracing_support;

const CACHED_NODE_BASE_VOLUME_ID: VolumeId = VolumeId::new(0xA9F0_0000_0000_0001);
const SESSION_VOLUME_BASE: u128 = 0xAAF0_0000_0000_0000;
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
    cases: &'static [GeneratedUpstreamNodeCase],
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
) -> Result<terracedb_sandbox::SandboxExecutionResult, SandboxError> {
    let session_suffix = NEXT_SESSION_SUFFIX.fetch_add(1, Ordering::Relaxed) as u128;
    let session = harness
        .open_session_with(
            base_volume_id,
            VolumeId::new(SESSION_VOLUME_BASE + 0x1000_0000_0000_0000 + session_suffix),
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
    session
        .exec_node_command(
            &runtime_entrypoint,
            vec!["/usr/bin/node".to_string(), runtime_entrypoint.clone()],
            "/node/test/parallel".to_string(),
            BTreeMap::from([
                ("HOME".to_string(), "/workspace/home".to_string()),
                ("NODE_SKIP_FLAG_CHECK".to_string(), "1".to_string()),
            ]),
        )
        .await
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
    exec_upstream_node_test_in_harness(harness, CACHED_NODE_BASE_VOLUME_ID, entrypoint).await
}

pub async fn run_generated_upstream_node_suite(
    cases: &'static [GeneratedUpstreamNodeCase],
) -> Result<SimulationSuiteReport, SimulationHarnessError> {
    SimulationHarness::new()
        .with_max_workers(
            std::thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(1),
        )
        .run_suite(Arc::new(GeneratedUpstreamNodeSuite { cases }))
        .await
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
            })
            .collect())
    }

    async fn run_case(
        &self,
        fixture: Arc<Self::Fixture>,
        case: SimulationCaseSpec<Self::Case>,
    ) -> Result<(), SimulationHarnessError> {
        let result = exec_upstream_node_test_in_harness(
            fixture.harness.as_ref(),
            fixture.base_volume_id,
            case.input.path,
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
