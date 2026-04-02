use std::{
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
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_git::HostGitBridge;
use terracedb_sandbox::{
    CloseSessionOptions, ConflictPolicy, DefaultSandboxStore, DeterministicPackageInstaller,
    DeterministicPullRequestProviderClient, DeterministicReadonlyViewProvider,
    DeterministicRuntimeBackend, EjectMode, EjectRequest, HoistMode, HoistRequest,
    HostGitWorkspaceManager, PullRequestProviderClient, PullRequestReport, PullRequestRequest,
    SANDBOX_GIT_LIBRARY_SPECIFIER, SandboxConfig, SandboxError, SandboxServices, SandboxStore,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_test_dir(name: &str) -> PathBuf {
    let id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "terracedb-sandbox-{name}-{}-{id}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn sandbox_store(
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
        .expect("open base volume");
}

fn deterministic_services() -> SandboxServices {
    SandboxServices::deterministic()
}

fn configured_host_git_bridge() -> Arc<HostGitBridge> {
    Arc::new(HostGitBridge::new(
        "host-git",
        "https://sandbox-bridge.invalid",
    ))
}

fn host_git_services() -> SandboxServices {
    host_git_services_with_provider(Arc::new(DeterministicPullRequestProviderClient::default()))
}

fn host_git_services_with_provider(
    provider: Arc<dyn PullRequestProviderClient>,
) -> SandboxServices {
    let bridge = configured_host_git_bridge();
    SandboxServices::new(
        Arc::new(DeterministicRuntimeBackend::default()),
        Arc::new(DeterministicPackageInstaller::default()),
        Arc::new(HostGitWorkspaceManager::default().with_bridge(bridge)),
        provider,
        Arc::new(DeterministicReadonlyViewProvider::default()),
    )
}

#[derive(Clone, Debug)]
struct FailingPullRequestProvider;

#[async_trait]
impl PullRequestProviderClient for FailingPullRequestProvider {
    fn name(&self) -> &str {
        "failing-pr-provider"
    }

    async fn create_pull_request(
        &self,
        _session: &terracedb_sandbox::SandboxSession,
        _request: PullRequestRequest,
    ) -> Result<PullRequestReport, SandboxError> {
        Err(SandboxError::Service {
            service: "pull-request",
            message: "legacy provider should not be called".to_string(),
        })
    }
}

fn write_host_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent directories");
    }
    fs::write(path, contents).expect("write host file");
}

fn read_host_file(path: &Path) -> String {
    fs::read_to_string(path).expect("read host file")
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

#[tokio::test]
async fn directory_hoist_round_trip_applies_delta_to_host_directory() {
    let source = unique_test_dir("round-trip-src");
    let target = unique_test_dir("round-trip-target");
    write_host_file(&source.join("tracked.txt"), "hello\n");
    write_host_file(&source.join("dir/keep.txt"), "keep\n");
    fs::create_dir_all(&target).expect("create target dir");
    write_host_file(&target.join("tracked.txt"), "hello\n");
    write_host_file(&target.join("dir/keep.txt"), "keep\n");

    let (vfs, sandbox) = sandbox_store(100, 501, deterministic_services());
    let base_volume_id = VolumeId::new(0x9000);
    let session_volume_id = VolumeId::new(0x9001);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let hoist = session
        .hoist_from_disk(HoistRequest {
            source_path: source.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::DirectorySnapshot,
            delete_missing: true,
        })
        .await
        .expect("hoist directory");
    assert_eq!(hoist.hoisted_paths, 3);

    let fs = session.filesystem();
    fs.write_file(
        "/workspace/tracked.txt",
        b"updated\n".to_vec(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("update tracked file");
    fs.rename("/workspace/dir/keep.txt", "/workspace/dir/renamed.txt")
        .await
        .expect("rename tracked file");
    fs.write_file(
        "/workspace/new.txt",
        b"new\n".to_vec(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("create new file");

    let eject = session
        .eject_to_disk(EjectRequest {
            target_path: target.to_string_lossy().into_owned(),
            mode: EjectMode::ApplyDelta,
            conflict_policy: ConflictPolicy::Fail,
        })
        .await
        .expect("eject delta");
    assert!(eject.conflicts.is_empty());
    assert_eq!(read_host_file(&target.join("tracked.txt")), "updated\n");
    assert!(!target.join("dir/keep.txt").exists());
    assert_eq!(read_host_file(&target.join("dir/renamed.txt")), "keep\n");
    assert_eq!(read_host_file(&target.join("new.txt")), "new\n");

    session
        .close(CloseSessionOptions::default())
        .await
        .expect("close session");
    cleanup(&source);
    cleanup(&target);
}

#[tokio::test]
async fn eject_conflicts_can_fall_back_to_patch_bundle() {
    let source = unique_test_dir("patch-src");
    let target = unique_test_dir("patch-target");
    write_host_file(&source.join("tracked.txt"), "one\n");
    fs::create_dir_all(&target).expect("create target dir");
    write_host_file(&target.join("tracked.txt"), "external\n");

    let (vfs, sandbox) = sandbox_store(110, 502, deterministic_services());
    let base_volume_id = VolumeId::new(0x9010);
    let session_volume_id = VolumeId::new(0x9011);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: source.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::DirectorySnapshot,
            delete_missing: true,
        })
        .await
        .expect("hoist directory");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"two\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");

    let report = session
        .eject_to_disk(EjectRequest {
            target_path: target.to_string_lossy().into_owned(),
            mode: EjectMode::ApplyDelta,
            conflict_policy: ConflictPolicy::CreatePatchBundle,
        })
        .await
        .expect("eject with patch bundle fallback");
    assert!(!report.conflicts.is_empty());
    let patch_bundle = PathBuf::from(report.patch_bundle_path.expect("patch bundle path"));
    assert!(patch_bundle.exists());
    assert_eq!(read_host_file(&target.join("tracked.txt")), "external\n");

    cleanup(&source);
    cleanup(&target);
}

#[tokio::test]
async fn git_working_tree_hoist_captures_dirty_provenance_and_untracked_files() {
    let repo = unique_test_dir("git-working-tree");
    init_git_repo(&repo, None);
    write_host_file(&repo.join("tracked.txt"), "dirty tracked\n");
    write_host_file(&repo.join("untracked.txt"), "untracked\n");

    let (vfs, sandbox) = sandbox_store(120, 503, host_git_services());
    let base_volume_id = VolumeId::new(0x9020);
    let session_volume_id = VolumeId::new(0x9021);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitWorkingTree {
                include_untracked: true,
                include_ignored: false,
            },
            delete_missing: true,
        })
        .await
        .expect("hoist git working tree");

    assert_eq!(
        session
            .filesystem()
            .read_file("/workspace/tracked.txt")
            .await
            .expect("read tracked file"),
        Some(b"dirty tracked\n".to_vec())
    );
    assert_eq!(
        session
            .filesystem()
            .read_file("/workspace/untracked.txt")
            .await
            .expect("read untracked file"),
        Some(b"untracked\n".to_vec())
    );
    let info = session.info().await;
    let provenance = info.provenance.git.expect("git provenance");
    assert!(provenance.dirty);
    assert_eq!(provenance.branch.as_deref(), Some("main"));
    assert_eq!(
        info.provenance.hoisted_source.expect("hoisted source").mode,
        HoistMode::GitWorkingTree {
            include_untracked: true,
            include_ignored: false,
        }
    );

    cleanup(&repo);
}

#[tokio::test]
async fn git_hoist_without_host_bridge_fails_closed() {
    let repo = unique_test_dir("git-no-bridge");
    init_git_repo(&repo, None);

    let (vfs, sandbox) = sandbox_store(121, 5031, deterministic_services());
    let base_volume_id = VolumeId::new(0x9022);
    let session_volume_id = VolumeId::new(0x9023);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let error = session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect_err("git hoist should require a host bridge");
    assert!(matches!(
        error,
        SandboxError::Service { service: "git", .. }
    ));

    cleanup(&repo);
}

#[tokio::test]
async fn create_pull_request_exports_to_real_git_worktree_and_pushes_branch() {
    let repo = unique_test_dir("git-pr-repo");
    let remote = unique_test_dir("git-pr-remote");
    init_git_repo(&repo, Some(&remote));

    let (vfs, sandbox) = sandbox_store(130, 504, host_git_services());
    let base_volume_id = VolumeId::new(0x9030);
    let session_volume_id = VolumeId::new(0x9031);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"pr branch change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Sandbox PR".to_string(),
            body: "Generated from sandbox".to_string(),
            head_branch: "sandbox/feature".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request");
    assert!(report.url.contains("sandbox-bridge.invalid"));
    assert_eq!(
        report.metadata.get("committed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        report.metadata.get("pushed"),
        Some(&serde_json::Value::Bool(true))
    );

    let workspace = PathBuf::from(
        report
            .metadata
            .get("workspace_path")
            .and_then(|value| value.as_str())
            .expect("workspace path"),
    );
    assert_eq!(
        read_host_file(&workspace.join("tracked.txt")),
        "pr branch change\n"
    );
    let pushed_head = sanitized_git_command()
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/sandbox/feature"])
        .output()
        .expect("inspect remote branch");
    assert!(
        pushed_head.status.success(),
        "remote branch missing: {}",
        String::from_utf8_lossy(&pushed_head.stderr)
    );
    assert_eq!(
        git_out(&workspace, &["show", "HEAD:tracked.txt"]),
        "pr branch change"
    );

    cleanup(&repo);
    cleanup(&remote);
    cleanup(&workspace);
}

#[tokio::test]
async fn repo_backed_pull_request_uses_git_bridge_not_legacy_provider() {
    let repo = unique_test_dir("git-pr-bridge-provider-repo");
    let remote = unique_test_dir("git-pr-bridge-provider-remote");
    init_git_repo(&repo, Some(&remote));

    let services = host_git_services_with_provider(Arc::new(FailingPullRequestProvider));
    let (vfs, sandbox) = sandbox_store(131, 5041, services);
    let base_volume_id = VolumeId::new(0x9032);
    let session_volume_id = VolumeId::new(0x9033);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"bridge provider path\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Bridge Provider PR".to_string(),
            body: "Should bypass legacy provider".to_string(),
            head_branch: "sandbox/bridge-provider".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request through git bridge");
    assert_eq!(report.provider, "host-git");
    assert_eq!(report.metadata.get("pushed"), Some(&json!(true)));

    cleanup(&repo);
    cleanup(&remote);
}

#[tokio::test]
async fn repo_backed_js_git_library_can_create_pull_request() {
    let repo = unique_test_dir("git-pr-js-repo");
    let remote = unique_test_dir("git-pr-js-remote");
    init_git_repo(&repo, Some(&remote));

    let (vfs, sandbox) = sandbox_store(132, 5042, host_git_services());
    let base_volume_id = VolumeId::new(0x9034);
    let session_volume_id = VolumeId::new(0x9035);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"js bridge change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");
    session
        .filesystem()
        .write_file(
            "/workspace/main.js",
            format!(
                "import {{ createPullRequest }} from \"{SANDBOX_GIT_LIBRARY_SPECIFIER}\";\n\
const report = await createPullRequest({{\n  title: \"JS Sandbox PR\",\n  body: \"Created through the sandbox git host surface\",\n  headBranch: \"sandbox/js-bridge\",\n  baseBranch: \"main\"\n}});\n\
export default report;\n"
            )
            .into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write js entrypoint");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute js git module");
    let report = result.result.expect("js result");
    assert_eq!(report["provider"], json!("host-git"));
    assert_eq!(report["metadata"]["pushed"], json!(true));
    assert!(
        result
            .module_graph
            .contains(&SANDBOX_GIT_LIBRARY_SPECIFIER.to_string())
    );

    let pushed_head = sanitized_git_command()
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/sandbox/js-bridge"])
        .output()
        .expect("inspect js remote branch");
    assert!(
        pushed_head.status.success(),
        "js remote branch missing: {}",
        String::from_utf8_lossy(&pushed_head.stderr)
    );

    cleanup(&repo);
    cleanup(&remote);
}

#[tokio::test]
async fn replacing_git_workspace_manager_keeps_host_bridge_in_sync_for_exports() {
    let repo = unique_test_dir("git-pr-manager-sync-repo");
    let remote = unique_test_dir("git-pr-manager-sync-remote");
    init_git_repo(&repo, Some(&remote));

    let services = SandboxServices::deterministic().with_git_workspace_manager(Arc::new(
        HostGitWorkspaceManager::default().with_bridge(configured_host_git_bridge()),
    ));
    let (vfs, sandbox) = sandbox_store(140, 505, services);
    let base_volume_id = VolumeId::new(0x9040);
    let session_volume_id = VolumeId::new(0x9041);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open synced host git session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist synced host git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"synced host bridge change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update synced sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Synced Sandbox PR".to_string(),
            body: "Generated from replaced git manager".to_string(),
            head_branch: "sandbox/manager-sync".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create synced pull request");
    assert_eq!(
        report.metadata.get("committed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        report.metadata.get("pushed"),
        Some(&serde_json::Value::Bool(true))
    );

    let workspace = PathBuf::from(
        report
            .metadata
            .get("workspace_path")
            .and_then(|value| value.as_str())
            .expect("workspace path"),
    );
    assert_eq!(
        git_out(&workspace, &["show", "HEAD:tracked.txt"]),
        "synced host bridge change"
    );
    let pushed_head = sanitized_git_command()
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/sandbox/manager-sync"])
        .output()
        .expect("inspect synced remote branch");
    assert!(
        pushed_head.status.success(),
        "synced remote branch missing: {}",
        String::from_utf8_lossy(&pushed_head.stderr)
    );

    cleanup(&repo);
    cleanup(&remote);
    cleanup(&workspace);
}

#[tokio::test]
async fn replacing_git_host_bridge_keeps_workspace_manager_in_sync_for_exports() {
    let repo = unique_test_dir("git-pr-bridge-sync-repo");
    let remote = unique_test_dir("git-pr-bridge-sync-remote");
    init_git_repo(&repo, Some(&remote));

    let services =
        SandboxServices::deterministic().with_git_host_bridge(configured_host_git_bridge());
    let (vfs, sandbox) = sandbox_store(150, 506, services);
    let base_volume_id = VolumeId::new(0x9050);
    let session_volume_id = VolumeId::new(0x9051);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open bridge-synced host git session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist bridge-synced host git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"bridge-synced host change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update bridge-synced sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Bridge Synced Sandbox PR".to_string(),
            body: "Generated from replaced git bridge".to_string(),
            head_branch: "sandbox/bridge-sync".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create bridge-synced pull request");
    assert_eq!(
        report.metadata.get("committed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        report.metadata.get("pushed"),
        Some(&serde_json::Value::Bool(true))
    );

    let workspace = PathBuf::from(
        report
            .metadata
            .get("workspace_path")
            .and_then(|value| value.as_str())
            .expect("workspace path"),
    );
    assert_eq!(
        git_out(&workspace, &["show", "HEAD:tracked.txt"]),
        "bridge-synced host change"
    );
    let pushed_head = sanitized_git_command()
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/sandbox/bridge-sync"])
        .output()
        .expect("inspect bridge-synced remote branch");
    assert!(
        pushed_head.status.success(),
        "bridge-synced remote branch missing: {}",
        String::from_utf8_lossy(&pushed_head.stderr)
    );

    cleanup(&repo);
    cleanup(&remote);
    cleanup(&workspace);
}
