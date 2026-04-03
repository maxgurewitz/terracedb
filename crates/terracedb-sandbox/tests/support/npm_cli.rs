use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};

use terracedb::{
    Clock, DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_sandbox::{
    CapabilityManifest, ConflictPolicy, DefaultSandboxStore, HoistMode, HoistedSource,
    NodeDebugExecutionOptions, PackageCompatibilityMode, SandboxConfig, SandboxServices,
    SandboxSession, SandboxStore,
    disk::apply_hoist_to_volume,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};
use tracing::info;

pub const SANDBOX_NPM_ROOT: &str = "/workspace/npm";
pub const SANDBOX_PROJECT_ROOT: &str = "/workspace/project";

const CACHED_NPM_BASE_VOLUME_ID: VolumeId = VolumeId::new(0xA7F0_0000_0000_0001);
const SESSION_VOLUME_BASE: u128 = 0xA8F0_0000_0000_0000;

static CACHED_NPM_BASE: OnceLock<tokio::sync::Mutex<Option<CachedNpmBase>>> = OnceLock::new();
static NEXT_SESSION_SUFFIX: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct CachedNpmBase {
    npm_root: String,
    vfs: InMemoryVfsStore,
    clock: Arc<dyn Clock>,
}

fn cached_npm_base_cell() -> &'static tokio::sync::Mutex<Option<CachedNpmBase>> {
    CACHED_NPM_BASE.get_or_init(|| tokio::sync::Mutex::new(None))
}

pub fn npm_cli_root() -> Option<String> {
    std::env::var("TERRACE_NPM_CLI_ROOT")
        .ok()
        .or_else(host_npm_root)
        .filter(|path| std::path::Path::new(path).exists())
}

fn host_npm_root() -> Option<String> {
    let which = std::process::Command::new("which")
        .arg("npm")
        .output()
        .ok()?;
    if !which.status.success() {
        return None;
    }
    let npm_bin = String::from_utf8(which.stdout).ok()?.trim().to_string();
    let canonical =
        std::fs::canonicalize(&npm_bin).unwrap_or_else(|_| std::path::PathBuf::from(npm_bin));
    Some(
        canonical
            .parent()
            .and_then(std::path::Path::parent)?
            .to_string_lossy()
            .to_string(),
    )
}

pub fn sandbox_store(
    now: u64,
    seed: u64,
    services: SandboxServices,
) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(Arc::new(vfs.clone()), dependencies.clock, services);
    (vfs, sandbox)
}

pub async fn seed_base(vfs: &InMemoryVfsStore, base_volume_id: VolumeId) {
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
            "/workspace/.keep",
            b"npm-cli".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write base workspace marker");
}

async fn cached_npm_base() -> Option<CachedNpmBase> {
    let npm_root = npm_cli_root()?;
    let cell = cached_npm_base_cell();
    let mut guard = cell.lock().await;
    if let Some(cached) = guard.as_ref() {
        return Some(cached.clone());
    }

    info!(
        target: "terracedb.sandbox.test",
        npm_root = %npm_root,
        "building cached npm base volume"
    );
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(0))),
        Arc::new(StubRng::seeded(0xC0FFEE)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    seed_base(&vfs, CACHED_NPM_BASE_VOLUME_ID).await;
    let base = vfs
        .open_volume(VolumeConfig::new(CACHED_NPM_BASE_VOLUME_ID))
        .await
        .expect("open cached npm base volume");
    apply_hoist_to_volume(
        base.as_ref(),
        &terracedb_sandbox::HoistRequest {
            source_path: npm_root.clone(),
            target_root: SANDBOX_NPM_ROOT.to_string(),
            mode: HoistMode::DirectorySnapshot,
            delete_missing: true,
        },
    )
    .await
    .expect("seed cached npm base volume");
    base.fs()
        .mkdir(
            SANDBOX_PROJECT_ROOT,
            terracedb_vfs::MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("create cached sandbox project root");

    let cached = CachedNpmBase {
        npm_root,
        vfs,
        clock: dependencies.clock,
    };
    *guard = Some(cached.clone());
    Some(cached)
}

pub async fn open_npm_cli_session(
    now: u64,
    seed: u64,
) -> Option<(SandboxSession, InMemoryVfsStore)> {
    let cached = cached_npm_base().await?;
    info!(
        target: "terracedb.sandbox.test",
        now,
        seed,
        npm_root = %cached.npm_root,
        "opening npm cli session from cached base"
    );
    let services = SandboxServices::deterministic();
    let vfs = cached.vfs.clone();
    let sandbox = DefaultSandboxStore::new(Arc::new(vfs.clone()), cached.clock.clone(), services);
    let session_suffix = NEXT_SESSION_SUFFIX.fetch_add(1, Ordering::Relaxed) as u128;
    let session_volume_id =
        VolumeId::new(SESSION_VOLUME_BASE + ((seed as u128) << 16) + session_suffix);
    info!(target: "terracedb.sandbox.test", "cached base ready, opening session");
    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id: CACHED_NPM_BASE_VOLUME_ID,
            durable_base: false,
            workspace_root: SANDBOX_NPM_ROOT.to_string(),
            package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: CapabilityManifest::default(),
            execution_policy: None,
            hoisted_source: Some(HoistedSource {
                source_path: cached.npm_root.clone(),
                mode: HoistMode::DirectorySnapshot,
            }),
            git_provenance: None,
        })
        .await
        .expect("open npm cli sandbox session");
    info!(target: "terracedb.sandbox.test", "npm cli session ready");
    Some((session, vfs))
}

pub async fn run_npm_command(
    session: &SandboxSession,
    cwd: &str,
    args: &[&str],
) -> Result<terracedb_sandbox::SandboxExecutionResult, terracedb_sandbox::SandboxError> {
    let cli_args = args
        .iter()
        .copied()
        .skip_while(|arg| *arg == "npm")
        .map(str::to_string)
        .collect::<Vec<_>>();
    let wrapper_args = cli_args.iter().map(String::as_str).collect::<Vec<_>>();
    run_inline_node_script(
        session,
        cwd,
        "/workspace/project/.terrace/run-npm-command.mjs",
        r#"
        const cliModule = await import("/workspace/npm/lib/cli.js");
        const cli = cliModule.default ?? cliModule;
        await cli(process);
        "#,
        &wrapper_args,
    )
    .await
}

pub async fn run_inline_node_script(
    session: &SandboxSession,
    cwd: &str,
    path: &str,
    source: &str,
    argv_tail: &[&str],
) -> Result<terracedb_sandbox::SandboxExecutionResult, terracedb_sandbox::SandboxError> {
    run_inline_node_script_with_debug(
        session,
        cwd,
        path,
        source,
        argv_tail,
        NodeDebugExecutionOptions::default(),
    )
    .await
}

pub async fn run_inline_node_script_with_debug(
    session: &SandboxSession,
    cwd: &str,
    path: &str,
    source: &str,
    argv_tail: &[&str],
    debug: NodeDebugExecutionOptions,
) -> Result<terracedb_sandbox::SandboxExecutionResult, terracedb_sandbox::SandboxError> {
    info!(target: "terracedb.sandbox.test", entrypoint = %path, cwd = %cwd, "running inline node script");
    session
        .filesystem()
        .write_file(
            path,
            source.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write inline node script");
    let mut argv = vec!["/usr/bin/node".to_string(), path.to_string()];
    argv.extend(argv_tail.iter().map(|value| value.to_string()));
    session
        .exec_node_command_with_debug(
            path.to_string(),
            argv,
            cwd.to_string(),
            std::collections::BTreeMap::from([
                ("HOME".to_string(), "/workspace/home".to_string()),
                ("npm_config_yes".to_string(), "true".to_string()),
            ]),
            debug,
        )
        .await
}

pub fn node_runtime_trace(result: &terracedb_sandbox::SandboxExecutionResult) -> Vec<String> {
    result
        .metadata
        .get("node_runtime_trace")
        .and_then(|value| value.as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub fn node_runtime_events(result: &terracedb_sandbox::SandboxExecutionResult) -> Vec<serde_json::Value> {
    result
        .metadata
        .get("node_runtime_events")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default()
}

pub fn node_runtime_events_with_labels(
    result: &terracedb_sandbox::SandboxExecutionResult,
    labels: &[&str],
) -> Vec<serde_json::Value> {
    node_runtime_events(result)
        .into_iter()
        .filter(|event| {
            event
                .get("label")
                .and_then(|value| value.as_str())
                .map(|label| labels.iter().any(|candidate| candidate == &label))
                .unwrap_or(false)
        })
        .collect()
}

pub fn node_runtime_events_matching(
    result: &terracedb_sandbox::SandboxExecutionResult,
    labels: Option<&[&str]>,
    module_substrings: Option<&[&str]>,
    limit: Option<usize>,
) -> Vec<serde_json::Value> {
    let mut matches = node_runtime_events(result)
        .into_iter()
        .filter(|event| {
            let label_ok = labels.map_or(true, |labels| {
                event.get("label")
                    .and_then(|value| value.as_str())
                    .map(|label| labels.iter().any(|candidate| candidate == &label))
                    .unwrap_or(false)
            });
            let module_ok = module_substrings.map_or(true, |module_substrings| {
                let module = event
                    .get("data")
                    .and_then(|value| value.get("module"))
                    .and_then(|value| value.as_str());
                module.map_or(false, |module| {
                    module_substrings
                        .iter()
                        .any(|candidate| module.contains(candidate))
                })
            });
            label_ok && module_ok
        })
        .collect::<Vec<_>>();
    if let Some(limit) = limit {
        if matches.len() > limit {
            matches = matches.split_off(matches.len() - limit);
        }
    }
    matches
}

pub fn node_runtime_last_exception(
    result: &terracedb_sandbox::SandboxExecutionResult,
) -> Option<serde_json::Value> {
    result.metadata.get("node_runtime_last_exception").cloned()
}
