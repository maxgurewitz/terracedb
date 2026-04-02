use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use serde_json::json;
use terracedb::{
    DbDependencies, DomainCpuBudget, ExecutionDomainBudget, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionResourceUsage, InMemoryResourceManager, StubClock,
    StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_capabilities::{
    CapabilityManifest as PolicyCapabilityManifest, ExecutionDomain, ExecutionDomainAssignment,
    ExecutionOperation, ExecutionPolicy, ManifestBinding, ResourceKind, ResourcePolicy,
    ResourceSelector, ShellCommandDescriptor, capability_module_specifier,
};
use terracedb_sandbox::{
    BashRequest, BashService, CapabilityRegistry, DefaultSandboxStore, DeterministicBashService,
    DeterministicCapabilityModule, DeterministicCapabilityRegistry, DeterministicPackageInstaller,
    DeterministicRuntimeBackend, DeterministicTypeScriptService, GitProvenance, HoistMode,
    HoistRequest, ManifestBoundCapabilityDispatcher, ManifestBoundCapabilityInvocation,
    ManifestBoundCapabilityRegistry, ManifestBoundCapabilityResult, PackageCompatibilityMode,
    PackageInstallRequest, PullRequestRequest, ReopenSessionOptions, SandboxCapability,
    SandboxConfig, SandboxExecutionDomainRoute, SandboxExecutionKind, SandboxExecutionPlacement,
    SandboxExecutionRequest, SandboxExecutionRouter, SandboxRuntimeBackend,
    SandboxRuntimeStateHandle, SandboxServices, SandboxStore, TERRACE_BASH_SESSION_STATE_PATH,
    TERRACE_NPM_INSTALL_MANIFEST_PATH, TERRACE_RUNTIME_MODULE_CACHE_PATH,
    TERRACE_SESSION_INFO_KV_KEY, TERRACE_SESSION_METADATA_PATH, TERRACE_TYPESCRIPT_MIRROR_PATH,
    TERRACE_TYPESCRIPT_STATE_PATH, TypeCheckRequest, TypeScriptService, TypeScriptTranspileRequest,
};
use terracedb_vfs::{
    CloneVolumeSource, CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore,
};

static HOST_GIT_TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn sandbox_store(now: u64, seed: u64) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    sandbox_store_with_services(now, seed, SandboxServices::deterministic())
}

fn sandbox_store_with_services(
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

async fn create_empty_base(store: &InMemoryVfsStore, volume_id: VolumeId) {
    store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open empty base");
}

fn unique_host_git_test_dir(name: &str) -> PathBuf {
    let id = HOST_GIT_TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "terracedb-sandbox-recovery-{name}-{}-{id}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn write_host_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create host parent");
    }
    fs::write(path, contents).expect("write host file");
}

fn sanitized_git_command() -> Command {
    let mut command = Command::new("git");
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_PREFIX",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ] {
        command.env_remove(key);
    }
    command
}

fn git(dir: &Path, args: &[&str]) {
    let output = sanitized_git_command()
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git -C {} {} failed: {}",
        dir.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_out(dir: &Path, args: &[&str]) -> String {
    let output = sanitized_git_command()
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git -C {} {} failed: {}",
        dir.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn init_git_repo(repo: &Path, remote: Option<&Path>) {
    fs::create_dir_all(repo).expect("create repo dir");
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.name", "Sandbox Tester"]);
    git(repo, &["config", "user.email", "sandbox@example.invalid"]);
    write_host_file(&repo.join("tracked.txt"), "tracked\n");
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "initial"]);
    if let Some(remote) = remote {
        fs::create_dir_all(remote).expect("create remote parent");
        let output = sanitized_git_command()
            .arg("init")
            .arg("--bare")
            .arg(remote)
            .output()
            .expect("init bare remote");
        assert!(
            output.status.success(),
            "git init --bare failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        git(
            repo,
            &["remote", "add", "origin", &remote.to_string_lossy()],
        );
        git(repo, &["push", "-u", "origin", "main"]);
    }
}

async fn seed_base(store: &InMemoryVfsStore, volume_id: VolumeId) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base");
    base.fs()
        .write_file(
            "/workspace/repo.txt",
            b"repo".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed base");
    base.fs()
        .write_file(
            "/workspace/tooling.ts",
            b"export const value: number = 1;\n".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed tooling");
}

async fn seed_runtime_module(store: &InMemoryVfsStore, volume_id: VolumeId) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base");
    base.fs()
        .write_file(
            "/workspace/main.js",
            br#"
            import { writeTextFile } from "@terracedb/sandbox/fs";
            await writeTextFile("/workspace/generated.txt", "generated");
            export default "ok";
            "#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed runtime module");
}

async fn seed_package_runtime_module(store: &InMemoryVfsStore, volume_id: VolumeId) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base");
    base.fs()
        .write_file(
            "/workspace/main.js",
            br#"
            import { camelCase } from "lodash";
            export default camelCase("flushed package install");
            "#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed package runtime module");
}

#[derive(Clone, Default)]
struct RecoveryShellBridgeDispatcher;

#[async_trait]
impl ManifestBoundCapabilityDispatcher for RecoveryShellBridgeDispatcher {
    async fn invoke_binding(
        &self,
        _session: &terracedb_sandbox::SandboxSession,
        binding: &ManifestBinding,
        request: ManifestBoundCapabilityInvocation,
    ) -> Result<ManifestBoundCapabilityResult, terracedb_sandbox::SandboxError> {
        match (binding.capability_family.as_str(), request.method.as_str()) {
            ("db.table.v1", "get") => {
                let key = request
                    .args
                    .first()
                    .and_then(|value| value.get("key"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("missing");
                Ok(ManifestBoundCapabilityResult {
                    value: json!({
                        "key": key,
                        "row": {
                            "owner_id": "user:alice",
                        },
                    }),
                    metadata: BTreeMap::from([("recovered".to_string(), json!(true))]),
                })
            }
            _ => Err(terracedb_sandbox::SandboxError::Service {
                service: "capabilities",
                message: format!(
                    "recovery shell dispatcher does not implement {}::{}",
                    binding.capability_family, request.method
                ),
            }),
        }
    }
}

fn recovery_shell_policy_manifest() -> PolicyCapabilityManifest {
    PolicyCapabilityManifest {
        subject: None,
        preset_name: Some("recovery".to_string()),
        profile_name: None,
        bindings: vec![ManifestBinding {
            binding_name: "tickets".to_string(),
            capability_family: "db.table.v1".to_string(),
            module_specifier: capability_module_specifier("tickets"),
            shell_command: Some(ShellCommandDescriptor::for_binding("tickets")),
            resource_policy: ResourcePolicy {
                allow: vec![ResourceSelector {
                    kind: ResourceKind::Table,
                    pattern: "tickets".to_string(),
                }],
                deny: vec![],
                tenant_scopes: vec![],
                row_scope_binding: None,
                visibility_index: None,
                metadata: BTreeMap::new(),
            },
            budget_policy: Default::default(),
            source_template_id: "db.table.v1".to_string(),
            source_grant_id: Some("grant-recovery".to_string()),
            allow_interactive_widening: false,
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::new(),
    }
}

#[tokio::test]
async fn reopen_prefers_file_metadata_and_repairs_missing_kv_mirror() {
    let (vfs, sandbox) = sandbox_store(40, 201);
    let base_volume_id = VolumeId::new(0x8000);
    let session_volume_id = VolumeId::new(0x8001);
    seed_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let mut updated = session.info().await;
    updated.revision += 10;
    updated.provenance.git = Some(GitProvenance {
        repo_root: "/repo".to_string(),
        head_commit: Some("cafebabe".to_string()),
        branch: Some("file-first".to_string()),
        remote_url: None,
        pathspec: vec![".".to_string()],
        dirty: false,
    });
    session
        .volume()
        .fs()
        .write_file(
            TERRACE_SESSION_METADATA_PATH,
            serde_json::to_vec_pretty(&updated).expect("encode updated info"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write session file only");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen session");
    let reopened_info = reopened.info().await;
    assert_eq!(reopened_info.revision, updated.revision);
    assert_eq!(
        reopened_info
            .provenance
            .git
            .clone()
            .and_then(|git| git.branch)
            .expect("branch"),
        "file-first"
    );
    assert_eq!(
        reopened
            .kv()
            .get_json(TERRACE_SESSION_INFO_KV_KEY)
            .await
            .expect("session kv"),
        Some(serde_json::to_value(&reopened_info).expect("encode reopened info"))
    );
}

#[tokio::test]
async fn durable_recovery_only_sees_flushed_provenance_updates() {
    let (source_vfs, sandbox) = sandbox_store(50, 202);
    let base_volume_id = VolumeId::new(0x8100);
    let session_volume_id = VolumeId::new(0x8101);
    seed_base(&source_vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session.flush().await.expect("flush initial session");
    session
        .update_provenance(|provenance| {
            provenance.git = Some(GitProvenance {
                repo_root: "/repo".to_string(),
                head_commit: Some("1".to_string()),
                branch: Some("pending".to_string()),
                remote_url: None,
                pathspec: vec![".".to_string()],
                dirty: false,
            });
        })
        .await
        .expect("update provenance");

    let unflushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut before flush");
    let (unflushed_vfs, unflushed_sandbox) = sandbox_store(60, 203);
    unflushed_vfs
        .import_volume(
            unflushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut before flush");
    let unflushed_session = unflushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen durable cut before flush");
    assert!(unflushed_session.info().await.provenance.git.is_none());

    session.flush().await.expect("flush updated provenance");

    let flushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut after flush");
    let (flushed_vfs, flushed_sandbox) = sandbox_store(70, 204);
    flushed_vfs
        .import_volume(
            flushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut after flush");
    let flushed_session = flushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen durable cut after flush");
    assert_eq!(
        flushed_session
            .info()
            .await
            .provenance
            .git
            .and_then(|git| git.branch)
            .expect("flushed branch"),
        "pending"
    );
}

#[tokio::test]
async fn reopen_preserves_typescript_and_bash_service_metadata() {
    let (vfs, sandbox) = sandbox_store(80, 205);
    let base_volume_id = VolumeId::new(0x8200);
    let session_volume_id = VolumeId::new(0x8201);
    seed_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let ts = DeterministicTypeScriptService::default();
    ts.transpile(
        &session,
        TypeScriptTranspileRequest {
            path: "/workspace/tooling.ts".to_string(),
            target: "es2022".to_string(),
            module_kind: "esm".to_string(),
            jsx: None,
        },
    )
    .await
    .expect("transpile tooling");
    ts.check(
        &session,
        TypeCheckRequest {
            roots: vec!["/workspace/tooling.ts".to_string()],
            ..Default::default()
        },
    )
    .await
    .expect("check tooling");
    let bash = DeterministicBashService::default();
    bash.run(
        &session,
        BashRequest {
            command: "mkdir -p notes && cd notes && export NAME=Terrace".to_string(),
            cwd: "/workspace".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("run bash");
    session.flush().await.expect("flush service state");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen session");
    assert!(
        reopened
            .filesystem()
            .read_file(TERRACE_TYPESCRIPT_STATE_PATH)
            .await
            .expect("read ts state")
            .is_some()
    );
    assert!(
        reopened
            .filesystem()
            .read_file(TERRACE_TYPESCRIPT_MIRROR_PATH)
            .await
            .expect("read ts mirror")
            .is_some()
    );
    assert!(
        reopened
            .filesystem()
            .read_file(TERRACE_BASH_SESSION_STATE_PATH)
            .await
            .expect("read bash state")
            .is_some()
    );

    let cached = ts
        .transpile(
            &reopened,
            TypeScriptTranspileRequest {
                path: "/workspace/tooling.ts".to_string(),
                target: "es2022".to_string(),
                module_kind: "esm".to_string(),
                jsx: None,
            },
        )
        .await
        .expect("transpile after reopen");
    assert!(cached.cache_hit);

    let resumed = bash
        .run(
            &reopened,
            BashRequest {
                command: "echo $NAME && pwd".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("resume bash");
    assert_eq!(resumed.stdout, "Terrace\n/workspace/notes\n");
    assert_eq!(resumed.cwd, "/workspace/notes");
}

#[tokio::test]
async fn reopen_rebuilds_manifest_bound_shell_bridge_from_session_manifest() {
    let registry = ManifestBoundCapabilityRegistry::new(
        recovery_shell_policy_manifest(),
        Arc::new(RecoveryShellBridgeDispatcher),
    )
    .expect("build recovery shell registry");
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store_with_services(85, 2051, services);
    let base_volume_id = VolumeId::new(0x8202);
    let session_volume_id = VolumeId::new(0x8203);
    seed_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_capabilities(registry.manifest()),
        )
        .await
        .expect("open shell bridge session");
    session.flush().await.expect("flush shell bridge session");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen shell bridge session");

    let help = reopened
        .run_bash(BashRequest {
            command: "terrace-call --help".to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("shell bridge help after reopen");
    assert_eq!(help.exit_code, 0);
    assert!(help.stdout.contains("terrace-call tickets"));

    let shell = reopened
        .run_bash(BashRequest {
            command: r#"terrace-call tickets get '{"key":"ticket:t-1"}'"#.to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("shell bridge call after reopen");
    let shell_json: serde_json::Value =
        serde_json::from_str(shell.stdout.trim()).expect("decode reopened shell JSON");
    assert_eq!(shell.exit_code, 0);
    assert_eq!(shell_json["ok"], json!(true));
    assert_eq!(shell_json["metadata"]["recovered"], json!(true));
}

#[tokio::test]
async fn runtime_cache_manifest_only_survives_durable_recovery_after_flush() {
    let (source_vfs, sandbox) = sandbox_store(90, 206);
    let base_volume_id = VolumeId::new(0x8210);
    let session_volume_id = VolumeId::new(0x8211);
    seed_runtime_module(&source_vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .flush()
        .await
        .expect("flush initial session metadata");
    session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute runtime module");

    let unflushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut before flush");
    let (unflushed_vfs, unflushed_sandbox) = sandbox_store(100, 207);
    unflushed_vfs
        .import_volume(
            unflushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut before flush");
    let unflushed_session = unflushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen durable cut before flush");
    assert_eq!(
        unflushed_session
            .filesystem()
            .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
            .await
            .expect("read unflushed runtime cache"),
        None
    );

    session
        .flush()
        .await
        .expect("flush session with runtime cache");

    let flushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut after flush");
    let (flushed_vfs, flushed_sandbox) = sandbox_store(110, 208);
    flushed_vfs
        .import_volume(
            flushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut after flush");
    let flushed_session = flushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen durable cut after flush");
    let cache = flushed_session
        .filesystem()
        .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
        .await
        .expect("read flushed runtime cache")
        .expect("runtime cache manifest should survive after flush");
    let manifest: serde_json::Value =
        serde_json::from_slice(&cache).expect("decode runtime cache manifest");
    assert!(
        manifest
            .as_array()
            .expect("runtime cache entries array")
            .iter()
            .any(|entry| entry["specifier"] == "terrace:/workspace/main.js")
    );
}

#[tokio::test]
async fn recovered_runtime_cache_preserves_inline_eval_and_host_service_entries() {
    let capabilities = DeterministicCapabilityRegistry::new(vec![
        DeterministicCapabilityModule::new(SandboxCapability::host_module("tickets"))
            .expect("valid capability")
            .with_echo_method("echo"),
    ])
    .expect("registry");
    let services =
        SandboxServices::deterministic().with_capabilities(Arc::new(capabilities.clone()));
    let (source_vfs, sandbox) = sandbox_store_with_services(115, 2081, services.clone());
    let base_volume_id = VolumeId::new(0x8220);
    let session_volume_id = VolumeId::new(0x8221);
    seed_base(&source_vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: terracedb_sandbox::ConflictPolicy::Fail,
            capabilities: capabilities.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open runtime session");
    session
        .flush()
        .await
        .expect("flush initial runtime session");

    let backend = DeterministicRuntimeBackend::default();
    let handle = backend
        .start_session(&session.info().await)
        .await
        .expect("start runtime session");
    let request = SandboxExecutionRequest {
        kind: SandboxExecutionKind::Eval {
            source: r#"
                import { readTextFile } from "@terracedb/sandbox/fs";
                import { echo } from "terrace:host/tickets";
                export default await echo({
                    value: await readTextFile("/workspace/repo.txt")
                });
            "#
            .to_string(),
            virtual_specifier: Some(
                "terrace:/workspace/.terrace/runtime/recovery-eval.mjs".to_string(),
            ),
        },
        metadata: Default::default(),
    };
    backend
        .execute(&session, &handle, request, SandboxRuntimeStateHandle::new())
        .await
        .expect("execute runtime eval");
    session
        .flush()
        .await
        .expect("flush session with runtime cache manifest");

    let flushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut after runtime cache flush");
    let (flushed_vfs, flushed_sandbox) = sandbox_store_with_services(116, 2082, services);
    flushed_vfs
        .import_volume(
            flushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut");
    let reopened = flushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen flushed runtime session");

    let reopened_backend = DeterministicRuntimeBackend::default();
    let reopened_handle = reopened_backend
        .start_session(&reopened.info().await)
        .await
        .expect("start reopened runtime session");
    reopened_backend
        .execute(
            &reopened,
            &reopened_handle,
            SandboxExecutionRequest {
                kind: SandboxExecutionKind::Eval {
                    source: "export default \"narrow\";".to_string(),
                    virtual_specifier: Some(
                        "terrace:/workspace/.terrace/runtime/recovery-narrow.mjs".to_string(),
                    ),
                },
                metadata: Default::default(),
            },
            SandboxRuntimeStateHandle::new(),
        )
        .await
        .expect("execute narrower reopened runtime eval");
    reopened
        .flush()
        .await
        .expect("flush reopened session with narrower eval cache");

    let cache = reopened
        .filesystem()
        .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
        .await
        .expect("read recovered runtime cache")
        .expect("runtime cache should survive durable recovery");
    let specifiers = serde_json::from_slice::<serde_json::Value>(&cache)
        .expect("decode recovered runtime cache")
        .as_array()
        .expect("runtime cache entries array")
        .iter()
        .map(|entry| {
            entry["specifier"]
                .as_str()
                .expect("runtime cache specifier")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert!(
        specifiers.contains(&"terrace:/workspace/.terrace/runtime/recovery-eval.mjs".to_string())
    );
    assert!(
        specifiers.contains(&"terrace:/workspace/.terrace/runtime/recovery-narrow.mjs".to_string())
    );
    assert!(specifiers.contains(&"@terracedb/sandbox/fs".to_string()));
    assert!(specifiers.contains(&"terrace:host/tickets".to_string()));
}

#[tokio::test]
async fn recovered_runtime_cache_restores_inline_eval_counter() {
    let (source_vfs, sandbox) = sandbox_store(117, 2083);
    let base_volume_id = VolumeId::new(0x8222);
    let session_volume_id = VolumeId::new(0x8223);
    seed_base(&source_vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: terracedb_sandbox::ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open runtime session");
    session
        .flush()
        .await
        .expect("flush initial runtime session");

    let backend = DeterministicRuntimeBackend::default();
    let handle = backend
        .start_session(&session.info().await)
        .await
        .expect("start runtime session");
    let first = backend
        .execute(
            &session,
            &handle,
            SandboxExecutionRequest::eval("export default \"first\";"),
            SandboxRuntimeStateHandle::new(),
        )
        .await
        .expect("execute first inline eval");
    assert_eq!(
        first.entrypoint,
        "terrace:/workspace/.terrace/runtime/eval-1.mjs"
    );
    session
        .flush()
        .await
        .expect("flush session with first inline eval");

    let flushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut after inline eval flush");
    let (flushed_vfs, flushed_sandbox) = sandbox_store(118, 2084);
    flushed_vfs
        .import_volume(
            flushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut");
    let reopened = flushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen flushed runtime session");

    let reopened_backend = DeterministicRuntimeBackend::default();
    let reopened_handle = reopened_backend
        .start_session(&reopened.info().await)
        .await
        .expect("start reopened runtime session");
    let second = reopened_backend
        .execute(
            &reopened,
            &reopened_handle,
            SandboxExecutionRequest::eval("export default \"second\";"),
            SandboxRuntimeStateHandle::new(),
        )
        .await
        .expect("execute second inline eval after recovery");
    assert_eq!(
        second.entrypoint,
        "terrace:/workspace/.terrace/runtime/eval-2.mjs"
    );
    assert!(
        second
            .cache_misses
            .contains(&"terrace:/workspace/.terrace/runtime/eval-2.mjs".to_string())
    );
    assert!(
        !second
            .cache_hits
            .contains(&"terrace:/workspace/.terrace/runtime/eval-2.mjs".to_string())
    );
}

#[tokio::test]
async fn package_install_manifest_and_compatibility_view_only_survive_durable_recovery_after_flush()
{
    let (source_vfs, sandbox) = sandbox_store(120, 209);
    let base_volume_id = VolumeId::new(0x8300);
    let session_volume_id = VolumeId::new(0x8301);
    seed_package_runtime_module(&source_vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmPureJs,
            conflict_policy: terracedb_sandbox::ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open package session");
    session.flush().await.expect("flush initial session");
    session
        .install_packages(PackageInstallRequest {
            packages: vec!["lodash".to_string()],
            materialize_compatibility_view: true,
        })
        .await
        .expect("install packages");

    let unflushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut before package flush");
    let (unflushed_vfs, unflushed_sandbox) = sandbox_store(130, 210);
    unflushed_vfs
        .import_volume(
            unflushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut before package flush");
    let unflushed_session = unflushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen durable cut before package flush");
    assert_eq!(
        unflushed_session
            .filesystem()
            .read_file(TERRACE_NPM_INSTALL_MANIFEST_PATH)
            .await
            .expect("read unflushed package manifest"),
        None
    );

    session
        .flush()
        .await
        .expect("flush session with package manifest");

    let flushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut after package flush");
    let (flushed_vfs, flushed_sandbox) = sandbox_store(140, 211);
    flushed_vfs
        .import_volume(
            flushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut after package flush");
    let flushed_session = flushed_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen durable cut after package flush");
    assert!(
        flushed_session
            .filesystem()
            .read_file(TERRACE_NPM_INSTALL_MANIFEST_PATH)
            .await
            .expect("read flushed package manifest")
            .is_some()
    );
    let result = flushed_session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute recovered package module");
    assert_eq!(result.result, Some(json!("flushedPackageInstall")));
}

#[tokio::test]
async fn durable_recovery_preserves_execution_policy_metadata_and_routed_reopen_behavior() {
    let policy = recovery_execution_policy();
    let services = SandboxServices::deterministic().with_execution_router(recovery_router());
    let (source_vfs, sandbox) = sandbox_store_with_services(150, 212, services);
    let base_volume_id = VolumeId::new(0x8400);
    let session_volume_id = VolumeId::new(0x8401);
    seed_runtime_module(&source_vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_execution_policy(policy.clone()),
        )
        .await
        .expect("open policy-enabled session");
    let first = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute routed module");
    session.flush().await.expect("flush policy-enabled session");

    let exported = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut");
    let reopened_services =
        SandboxServices::deterministic().with_execution_router(recovery_router());
    let (recovery_vfs, recovery_sandbox) = sandbox_store_with_services(160, 213, reopened_services);
    recovery_vfs
        .import_volume(
            exported,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut");
    let reopened = recovery_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen policy-enabled durable cut");

    let reopened_info = reopened.info().await;
    assert_eq!(reopened_info.provenance.execution_policy, Some(policy));
    assert_eq!(
        reopened.runtime_handle().backend,
        "recovery-runtime-dedicated"
    );
    assert_eq!(
        reopened_info.services.execution_bindings[&ExecutionOperation::DraftSession]
            .domain_path
            .as_string(),
        "process/sandbox-recovery/dedicated"
    );
    assert_eq!(
        reopened_info.services.execution_bindings[&ExecutionOperation::TypeCheck].backend,
        "recovery-typescript-owner-foreground"
    );
    assert_eq!(
        first.metadata["execution_domain_path"],
        json!("process/sandbox-recovery/dedicated")
    );

    let second = reopened
        .exec_module("/workspace/main.js")
        .await
        .expect("execute reopened routed module");
    assert_eq!(second.result, Some(json!("ok")));
    assert_eq!(
        second.metadata["execution_domain_path"],
        json!("process/sandbox-recovery/dedicated")
    );
    let third = reopened
        .exec_module("/workspace/main.js")
        .await
        .expect_err("durable recovery should preserve execution call budgets");
    assert!(matches!(
        third,
        terracedb_sandbox::SandboxError::Service { service, ref message }
            if service == "execution-policy"
                && message.contains("exceeded max_calls 2")
    ));
    assert_eq!(
        reopened
            .filesystem()
            .read_file("/workspace/generated.txt")
            .await
            .expect("read generated file"),
        Some(b"generated".to_vec())
    );
}

fn recovery_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::DedicatedSandbox,
            budget: None,
            placement_tags: vec!["dedicated".to_string()],
            metadata: Default::default(),
        },
        operations: [
            (
                ExecutionOperation::DraftSession,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: Some(terracedb_capabilities::BudgetPolicy {
                        max_calls: Some(2),
                        max_scanned_rows: None,
                        max_returned_rows: None,
                        max_bytes: None,
                        max_millis: None,
                        rate_limit_bucket: None,
                        labels: Default::default(),
                    }),
                    placement_tags: vec!["draft".to_string()],
                    metadata: Default::default(),
                },
            ),
            (
                ExecutionOperation::PackageInstall,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: None,
                    placement_tags: vec!["packages".to_string()],
                    metadata: Default::default(),
                },
            ),
            (
                ExecutionOperation::TypeCheck,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::OwnerForeground,
                    budget: None,
                    placement_tags: vec!["foreground".to_string()],
                    metadata: Default::default(),
                },
            ),
            (
                ExecutionOperation::BashHelper,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::OwnerForeground,
                    budget: None,
                    placement_tags: vec!["foreground".to_string()],
                    metadata: Default::default(),
                },
            ),
        ]
        .into_iter()
        .collect(),
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn reopen_restores_host_bridge_git_manifest_and_allows_export() {
    let repo = unique_host_git_test_dir("host-git-repo");
    let remote = unique_host_git_test_dir("host-git-remote");
    init_git_repo(&repo, Some(&remote));
    let head_commit = git_out(&repo, &["rev-parse", "HEAD"]);
    let source_path = repo.to_string_lossy().into_owned();
    let repo_root = fs::canonicalize(&repo)
        .expect("canonicalize host git repo")
        .to_string_lossy()
        .into_owned();
    let remote_url = remote.to_string_lossy().into_owned();

    let services = SandboxServices::deterministic_with_host_git();
    let (source_vfs, sandbox) = sandbox_store_with_services(118, 2084, services.clone());
    let base_volume_id = VolumeId::new(0x8224);
    let session_volume_id = VolumeId::new(0x8225);
    create_empty_base(&source_vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open host git recovery session");
    let hoist = session
        .hoist_from_disk(HoistRequest {
            source_path: source_path.clone(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist host git repo");
    let hoisted_git = hoist.git_provenance.expect("hoist git provenance");
    assert_eq!(hoisted_git.repo_root, repo_root);
    assert_eq!(
        hoisted_git.head_commit.as_deref(),
        Some(head_commit.as_str())
    );
    assert_eq!(hoisted_git.branch.as_deref(), Some("main"));
    assert_eq!(hoisted_git.remote_url.as_deref(), Some(remote_url.as_str()));
    assert_eq!(hoisted_git.pathspec, vec![".".to_string()]);
    assert!(!hoisted_git.dirty);
    session
        .flush()
        .await
        .expect("flush host git hoist for recovery");

    let flushed = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await
        .expect("export durable cut for host git recovery");
    let (recovery_vfs, recovery_sandbox) = sandbox_store_with_services(119, 2085, services);
    recovery_vfs
        .import_volume(
            flushed,
            VolumeConfig::new(session_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut for host git recovery");
    let reopened = recovery_sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen host git recovery session");

    let reopened_info = reopened.info().await;
    assert_eq!(
        reopened_info.provenance.hoisted_source,
        Some(terracedb_sandbox::HoistedSource {
            source_path: repo_root.clone(),
            mode: HoistMode::GitHead,
        })
    );
    let reopened_git = reopened_info
        .provenance
        .git
        .expect("reopened git provenance");
    assert_eq!(reopened_git.repo_root, repo_root);
    assert_eq!(
        reopened_git.head_commit.as_deref(),
        Some(head_commit.as_str())
    );
    assert_eq!(reopened_git.branch.as_deref(), Some("main"));
    assert_eq!(
        reopened_git.remote_url.as_deref(),
        Some(remote_url.as_str())
    );
    assert_eq!(reopened_git.pathspec, vec![".".to_string()]);
    assert!(!reopened_git.dirty);

    let manifest_bytes = reopened
        .filesystem()
        .read_file(terracedb_sandbox::disk::HOIST_MANIFEST_PATH)
        .await
        .expect("read persisted hoist manifest")
        .expect("persisted hoist manifest");
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("decode hoist manifest");
    assert_eq!(manifest["source_path"], json!(repo_root.clone()));
    assert_eq!(manifest["target_root"], json!("/workspace"));
    assert_eq!(manifest["mode"], json!("git_head"));
    assert_eq!(
        manifest["git_provenance"]["repo_root"],
        json!(repo_root.clone())
    );
    assert_eq!(
        manifest["git_provenance"]["head_commit"],
        json!(head_commit.clone())
    );
    assert_eq!(manifest["git_provenance"]["branch"], json!("main"));
    assert_eq!(
        manifest["git_provenance"]["remote_url"],
        json!(remote_url.clone())
    );
    assert!(
        manifest["entries"]
            .as_array()
            .expect("hoist manifest entries")
            .iter()
            .any(|entry| entry["path"] == json!("tracked.txt"))
    );

    reopened
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"reopened bridge change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update reopened sandbox file");

    let report = reopened
        .create_pull_request(PullRequestRequest {
            title: "Recovered host git PR".to_string(),
            body: "Created after reopen".to_string(),
            head_branch: "sandbox/recovered-host-git".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request after reopen");
    assert!(report.url.contains("example.invalid"));
    assert_eq!(
        report.metadata.get("eject_mode"),
        Some(&json!("apply_delta"))
    );
    assert_eq!(
        report.metadata.get("provenance_validated"),
        Some(&json!(true))
    );
    assert_eq!(report.metadata.get("committed"), Some(&json!(true)));
    assert_eq!(report.metadata.get("pushed"), Some(&json!(true)));

    let workspace = PathBuf::from(
        report
            .metadata
            .get("workspace_path")
            .and_then(serde_json::Value::as_str)
            .expect("workspace path"),
    );
    assert_eq!(
        git_out(&workspace, &["show", "HEAD:tracked.txt"]),
        "reopened bridge change"
    );
    let pushed_head = sanitized_git_command()
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/sandbox/recovered-host-git"])
        .output()
        .expect("inspect recovered remote branch");
    assert!(
        pushed_head.status.success(),
        "recovered remote branch missing: {}",
        String::from_utf8_lossy(&pushed_head.stderr)
    );

    cleanup(&repo);
    cleanup(&remote);
    cleanup(&workspace);
}

fn recovery_router() -> SandboxExecutionRouter {
    let manager = Arc::new(InMemoryResourceManager::new(
        ExecutionDomainBudget::default(),
    ));
    let dedicated_typescript = Arc::new(DeterministicTypeScriptService::new(
        "recovery-typescript-dedicated",
    ));
    let dedicated_bash = Arc::new(
        DeterministicBashService::new("recovery-bash-dedicated")
            .with_typescript_service(dedicated_typescript.clone()),
    );

    SandboxExecutionRouter::new(manager)
        .with_domain(
            ExecutionDomain::DedicatedSandbox,
            SandboxExecutionDomainRoute::new(
                SandboxExecutionPlacement::new(ExecutionDomainPath::new([
                    "process",
                    "sandbox-recovery",
                    "dedicated",
                ]))
                .with_placement(ExecutionDomainPlacement::Dedicated)
                .with_budget(ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(1),
                        weight: Some(1),
                    },
                    ..Default::default()
                })
                .with_usage(
                    ExecutionOperation::DraftSession,
                    ExecutionResourceUsage {
                        cpu_workers: 1,
                        ..Default::default()
                    },
                ),
            )
            .with_runtime(Arc::new(DeterministicRuntimeBackend::new(
                "recovery-runtime-dedicated",
            )))
            .with_packages(Arc::new(DeterministicPackageInstaller::new(
                "recovery-packages-dedicated",
            )))
            .with_typescript(dedicated_typescript)
            .with_bash(dedicated_bash),
        )
        .with_domain(
            ExecutionDomain::OwnerForeground,
            SandboxExecutionDomainRoute::new(
                SandboxExecutionPlacement::new(ExecutionDomainPath::new([
                    "process",
                    "sandbox-recovery",
                    "owner-foreground",
                ]))
                .with_placement(ExecutionDomainPlacement::SharedWeighted { weight: 2 })
                .with_budget(ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(1),
                        weight: Some(2),
                    },
                    ..Default::default()
                })
                .with_usage(
                    ExecutionOperation::TypeCheck,
                    ExecutionResourceUsage {
                        cpu_workers: 1,
                        ..Default::default()
                    },
                )
                .with_usage(
                    ExecutionOperation::BashHelper,
                    ExecutionResourceUsage {
                        cpu_workers: 1,
                        ..Default::default()
                    },
                ),
            )
            .with_typescript(Arc::new(DeterministicTypeScriptService::new(
                "recovery-typescript-owner-foreground",
            )))
            .with_bash(Arc::new(DeterministicBashService::new(
                "recovery-bash-owner-foreground",
            ))),
        )
}
