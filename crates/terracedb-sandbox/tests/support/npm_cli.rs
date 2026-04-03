use std::sync::Arc;

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    CapabilityManifest, ConflictPolicy, DefaultSandboxStore, HoistMode, HoistedSource,
    PackageCompatibilityMode, SandboxConfig, SandboxServices, SandboxSession, SandboxStore,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};
use tracing::info;

pub const SANDBOX_NPM_ROOT: &str = "/workspace/npm";
pub const SANDBOX_PROJECT_ROOT: &str = "/workspace/project";

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

pub async fn open_npm_cli_session(
    now: u64,
    seed: u64,
) -> Option<(SandboxSession, InMemoryVfsStore)> {
    let npm_root = npm_cli_root()?;
    info!(target: "terracedb.sandbox.test", now, seed, npm_root = %npm_root, "opening npm cli session");
    let services = SandboxServices::deterministic();
    let (vfs, sandbox) = sandbox_store(now, seed, services);
    let base_volume_id = VolumeId::new(0xA700u128 + (seed as u128 % 0x100));
    let session_volume_id = VolumeId::new(0xA800u128 + (seed as u128 % 0x100));
    seed_base(&vfs, base_volume_id).await;
    info!(target: "terracedb.sandbox.test", "base seeded, opening session");
    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: SANDBOX_NPM_ROOT.to_string(),
            package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: CapabilityManifest::default(),
            execution_policy: None,
            hoisted_source: Some(HoistedSource {
                source_path: npm_root,
                mode: HoistMode::DirectorySnapshot,
            }),
            git_provenance: None,
        })
        .await
        .expect("open npm cli sandbox session");
    info!(target: "terracedb.sandbox.test", "session opened, creating project root");
    session
        .filesystem()
        .mkdir(
            SANDBOX_PROJECT_ROOT,
            terracedb_vfs::MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("create sandbox project root");
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
    let mut argv = vec![
        "/usr/bin/node".to_string(),
        format!("{SANDBOX_NPM_ROOT}/bin/npm-cli.js"),
    ];
    argv.extend(cli_args);
    session
        .exec_node_command(
            format!("{SANDBOX_NPM_ROOT}/bin/npm-cli.js"),
            argv,
            cwd.to_string(),
            std::collections::BTreeMap::from([
                ("HOME".to_string(), "/workspace/home".to_string()),
                ("npm_config_yes".to_string(), "true".to_string()),
            ]),
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
        .exec_node_command(
            path.to_string(),
            argv,
            cwd.to_string(),
            std::collections::BTreeMap::from([
                ("HOME".to_string(), "/workspace/home".to_string()),
                ("npm_config_yes".to_string(), "true".to_string()),
            ]),
        )
        .await
}
