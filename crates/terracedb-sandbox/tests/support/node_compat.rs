use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, PackageCompatibilityMode, SandboxConfig, SandboxError,
    SandboxServices, SandboxStore,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

#[path = "tracing.rs"]
mod tracing_support;

pub fn sandbox_store(
    now: u64,
    seed: u64,
) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(
        Arc::new(vfs.clone()),
        dependencies.clock,
        SandboxServices::deterministic(),
    );
    (vfs, sandbox)
}

pub async fn open_node_session(
    now: u64,
    seed: u64,
    entrypoint: &str,
    source: &str,
) -> terracedb_sandbox::SandboxSession {
    tracing_support::init_tracing();
    let (vfs, sandbox) = sandbox_store(now, seed);
    let base_volume_id = VolumeId::new(0xA910u128 + (seed as u128 % 0x100));
    let session_volume_id = VolumeId::new(0xAA10u128 + (seed as u128 % 0x100));
    let base = vfs
        .open_volume(
            VolumeConfig::new(base_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
    base.fs()
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

    sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open node sandbox session")
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
