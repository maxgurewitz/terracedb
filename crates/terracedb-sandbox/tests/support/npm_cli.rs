use tracing::info;

use terracedb_sandbox::{
    NodeDebugExecutionOptions, PackageCompatibilityMode, SandboxBaseLayer, SandboxConfig,
    SandboxHarness, SandboxServices, SandboxSession,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeId};

pub const SANDBOX_NPM_ROOT: &str = "/workspace/npm";
pub const SANDBOX_PROJECT_ROOT: &str = "/workspace/project";

const CACHED_NPM_BASE_VOLUME_ID: VolumeId = VolumeId::new(0xA7F0_0000_0000_0001);
const SESSION_VOLUME_BASE: u128 = 0xA8F0_0000_0000_0000;
static NEXT_SESSION_SUFFIX: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

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

pub async fn open_npm_cli_session(
    now: u64,
    seed: u64,
) -> Option<(SandboxSession, InMemoryVfsStore)> {
    let artifact_path =
        terracedb_sandbox::NODE_V24_14_1_NPM_CLI_V11_12_1_BASE_LAYER_ARTIFACT_PATH;
    if !std::path::Path::new(artifact_path).exists() {
        return None;
    }

    let harness = SandboxHarness::deterministic(now, seed, SandboxServices::deterministic());
    let vfs = harness.volumes().as_ref().clone();
    let session_suffix =
        NEXT_SESSION_SUFFIX.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u128;
    let session_volume_id =
        VolumeId::new(SESSION_VOLUME_BASE + ((seed as u128) << 16) + session_suffix);
    let session = harness
        .open_session_from_base_layer_with(
            &SandboxBaseLayer::vendored_node_v24_14_1_npm_cli_v11_12_1(),
            CACHED_NPM_BASE_VOLUME_ID,
            session_volume_id,
            |config| SandboxConfig {
                workspace_root: SANDBOX_PROJECT_ROOT.to_string(),
                package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
                ..config
            },
        )
        .await
        .expect("open npm cli sandbox session");
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
