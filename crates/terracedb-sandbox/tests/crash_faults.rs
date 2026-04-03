mod support;

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

use support::fake_github::{FakeGithubServer, configured_remote_github_bridge};
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_git::GitObjectFormat;
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, DeterministicPackageInstaller,
    DeterministicPullRequestProviderClient, DeterministicReadonlyViewProvider,
    DeterministicRuntimeBackend, DeterministicTypeScriptService, GitProvenance, HoistMode,
    HoistRequest, PackageCompatibilityMode, PullRequestRequest, ReadonlyViewCut,
    ReadonlyViewRequest, ReopenSessionOptions, SandboxConfig, SandboxError, SandboxServices,
    SandboxStore, TERRACE_SESSION_METADATA_PATH, TERRACE_TYPESCRIPT_MIRROR_PATH,
    TERRACE_TYPESCRIPT_STATE_PATH, TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR, TypeCheckRequest,
    TypeScriptService, TypeScriptTranspileRequest,
};
use terracedb_vfs::{
    CloneVolumeSource, CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore,
};
use uuid::Uuid;

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

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

fn deterministic_services() -> SandboxServices {
    SandboxServices::deterministic()
}

fn remote_github_services(host: &str) -> SandboxServices {
    SandboxServices::new(
        Arc::new(DeterministicRuntimeBackend::default()),
        Arc::new(DeterministicPackageInstaller::default()),
        Arc::new(terracedb_sandbox::DeterministicGitRepositoryStore::default()),
        Arc::new(DeterministicPullRequestProviderClient::default()),
        Arc::new(DeterministicReadonlyViewProvider::default()),
    )
    .with_git_host_bridge(configured_remote_github_bridge(host))
}

async fn create_empty_base(
    store: &InMemoryVfsStore,
    volume_id: VolumeId,
) -> Result<(), SandboxError> {
    store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await?;
    Ok(())
}

async fn seed_base(store: &InMemoryVfsStore, volume_id: VolumeId) -> Result<(), SandboxError> {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await?;
    let fs = base.fs();
    fs.write_file(
        "/workspace/base.txt",
        b"base".to_vec(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await?;
    fs.write_file(
        "/workspace/tooling.ts",
        b"export const value: number = 7;\n".to_vec(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await?;
    fs.write_file(
        "/workspace/bad.ts",
        b"export const broken: number = \"oops\";\n".to_vec(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn reopen_durable_cut(
    source_vfs: &InMemoryVfsStore,
    session_volume_id: VolumeId,
    now: u64,
    seed: u64,
    services: SandboxServices,
) -> Result<terracedb_sandbox::SandboxSession, SandboxError> {
    let exported = source_vfs
        .export_volume(CloneVolumeSource::new(session_volume_id).durable(true))
        .await?;
    let (vfs, sandbox) = sandbox_store(now, seed, services);
    vfs.import_volume(
        exported,
        VolumeConfig::new(session_volume_id)
            .with_chunk_size(4096)
            .with_create_if_missing(true),
    )
    .await?;
    sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
}

fn deterministic_temp_dir(label: &str, seed: u64) -> PathBuf {
    let unique = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    loop {
        let temp_id = Uuid::new_v4();
        let path = std::env::temp_dir().join(format!(
            "terracedb-sandbox-{label}-{seed:x}-{unique}-{temp_id}"
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("create temp dir {path:?}: {error}"),
        }
    }
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

fn read_host_file(path: &Path) -> String {
    fs::read_to_string(path).expect("read host file")
}

#[test]
fn deterministic_temp_dir_allocates_unique_paths() {
    let first = deterministic_temp_dir("crash-faults", 0x5302);
    let second = deterministic_temp_dir("crash-faults", 0x5302);
    assert_ne!(first, second);
    assert!(first.starts_with(std::env::temp_dir()));
    assert!(second.starts_with(std::env::temp_dir()));
    assert!(first.is_dir());
    assert!(second.is_dir());
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
        .expect("run git command");
    assert!(
        output.status.success(),
        "git -C {} {} failed: {}",
        dir.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_git_repo(repo: &Path, remote_url: Option<&str>, seed: u64) {
    cleanup(repo);
    fs::create_dir_all(repo).expect("create repo dir");
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.name", "Sandbox Tester"]);
    git(repo, &["config", "user.email", "sandbox@example.invalid"]);
    write_host_file(&repo.join("tracked.txt"), &format!("tracked-{seed:x}\n"));
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "initial"]);
    if let Some(remote_url) = remote_url {
        git(repo, &["remote", "add", "origin", remote_url]);
    }
}

async fn chronological_tool_names(
    session: &terracedb_sandbox::SandboxSession,
) -> Result<Vec<String>, SandboxError> {
    let mut runs = session.volume().tools().recent(None).await?;
    runs.reverse();
    Ok(runs.into_iter().map(|run| run.name).collect())
}

#[tokio::test]
async fn session_creation_metadata_only_survives_durable_recovery_after_flush() {
    let (source_vfs, sandbox) = sandbox_store(40, 801, deterministic_services());
    let base_volume_id = VolumeId::new(0xb100);
    let session_volume_id = VolumeId::new(0xb101);
    seed_base(&source_vfs, base_volume_id)
        .await
        .expect("seed base");

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");

    let before_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        41,
        802,
        deterministic_services(),
    )
    .await;
    assert!(matches!(
        before_flush,
        Err(SandboxError::MissingSessionMetadata { ref path }) if path == TERRACE_SESSION_METADATA_PATH
    ));

    session
        .flush()
        .await
        .expect("flush initial session metadata");
    let after_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        42,
        803,
        deterministic_services(),
    )
    .await
    .expect("reopen durable cut after flush");
    assert_eq!(
        after_flush
            .filesystem()
            .read_file("/workspace/base.txt")
            .await
            .expect("read durable base file"),
        Some(b"base".to_vec())
    );
}

#[tokio::test]
async fn typescript_cache_and_mirror_only_survive_durable_recovery_after_flush() {
    let (source_vfs, sandbox) = sandbox_store(50, 804, deterministic_services());
    let base_volume_id = VolumeId::new(0xb110);
    let session_volume_id = VolumeId::new(0xb111);
    seed_base(&source_vfs, base_volume_id)
        .await
        .expect("seed base");

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session.flush().await.expect("flush initial session state");

    let typescript = DeterministicTypeScriptService::default();
    let transpile = typescript
        .transpile(
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
    let diagnostics = typescript
        .check(
            &session,
            TypeCheckRequest {
                roots: vec!["/workspace/bad.ts".to_string()],
                ..Default::default()
            },
        )
        .await
        .expect("check tooling");
    assert_eq!(diagnostics.diagnostics.len(), 1);

    let before_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        51,
        805,
        deterministic_services(),
    )
    .await
    .expect("reopen durable cut before typescript flush");
    assert_eq!(
        before_flush
            .filesystem()
            .read_file(TERRACE_TYPESCRIPT_MIRROR_PATH)
            .await
            .expect("read durable ts mirror before flush"),
        None
    );
    assert_eq!(
        before_flush
            .filesystem()
            .read_file(TERRACE_TYPESCRIPT_STATE_PATH)
            .await
            .expect("read durable ts state before flush"),
        None
    );
    assert_eq!(
        before_flush
            .filesystem()
            .read_file(&format!(
                "{TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR}/{}.json",
                transpile.cache_key
            ))
            .await
            .expect("read durable transpile metadata before flush"),
        None
    );

    session
        .flush()
        .await
        .expect("flush session with typescript state");
    let after_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        52,
        806,
        deterministic_services(),
    )
    .await
    .expect("reopen durable cut after typescript flush");
    assert!(
        after_flush
            .filesystem()
            .read_file(TERRACE_TYPESCRIPT_MIRROR_PATH)
            .await
            .expect("read durable ts mirror after flush")
            .is_some()
    );
    assert!(
        after_flush
            .filesystem()
            .read_file(TERRACE_TYPESCRIPT_STATE_PATH)
            .await
            .expect("read durable ts state after flush")
            .is_some()
    );
    let cached = typescript
        .transpile(
            &after_flush,
            TypeScriptTranspileRequest {
                path: "/workspace/tooling.ts".to_string(),
                target: "es2022".to_string(),
                module_kind: "esm".to_string(),
                jsx: None,
            },
        )
        .await
        .expect("transpile after durable reopen");
    assert!(cached.cache_hit);
}

#[tokio::test]
async fn readonly_view_handles_only_survive_durable_recovery_after_flush() {
    let (source_vfs, sandbox) = sandbox_store(60, 807, deterministic_services());
    let base_volume_id = VolumeId::new(0xb120);
    let session_volume_id = VolumeId::new(0xb121);
    seed_base(&source_vfs, base_volume_id)
        .await
        .expect("seed base");

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session.flush().await.expect("flush initial session");

    let handle = session
        .open_readonly_view(ReadonlyViewRequest {
            cut: ReadonlyViewCut::Visible,
            path: "/workspace".to_string(),
            label: Some("workspace".to_string()),
        })
        .await
        .expect("open readonly view");

    let before_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        61,
        808,
        deterministic_services(),
    )
    .await
    .expect("reopen durable cut before view flush");
    assert!(before_flush.active_readonly_views().await.is_empty());

    session
        .flush()
        .await
        .expect("flush session with readonly view handles");
    let after_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        62,
        809,
        deterministic_services(),
    )
    .await
    .expect("reopen durable cut after view flush");
    assert_eq!(
        after_flush
            .active_readonly_views()
            .await
            .into_iter()
            .map(|view| view.handle_id)
            .collect::<Vec<_>>(),
        vec![handle.handle_id]
    );
}

#[tokio::test]
async fn hoist_manifest_and_apply_delta_only_survive_durable_recovery_after_flush() {
    let seed = 0xb130_u64;
    let source = deterministic_temp_dir("crash-hoist-source", seed);
    let target = deterministic_temp_dir("crash-hoist-target", seed);
    cleanup(&source);
    cleanup(&target);
    write_host_file(&source.join("tracked.txt"), "alpha\n");
    fs::create_dir_all(&target).expect("create target dir");
    write_host_file(&target.join("tracked.txt"), "alpha\n");

    let (source_vfs, sandbox) = sandbox_store(70, 810, deterministic_services());
    let base_volume_id = VolumeId::new(0xb130);
    let session_volume_id = VolumeId::new(0xb131);
    create_empty_base(&source_vfs, base_volume_id)
        .await
        .expect("create empty base");

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session.flush().await.expect("flush initial session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: source.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::DirectorySnapshot,
            delete_missing: true,
        })
        .await
        .expect("hoist source directory");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"beta\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update tracked file");

    let before_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        71,
        811,
        deterministic_services(),
    )
    .await
    .expect("reopen durable cut before hoist flush");
    assert_eq!(
        before_flush
            .filesystem()
            .read_file(terracedb_sandbox::disk::HOIST_MANIFEST_PATH)
            .await
            .expect("read durable hoist manifest before flush"),
        None
    );
    let before_error = before_flush
        .eject_to_disk(terracedb_sandbox::EjectRequest {
            target_path: target.to_string_lossy().into_owned(),
            mode: terracedb_sandbox::EjectMode::ApplyDelta,
            conflict_policy: ConflictPolicy::Fail,
        })
        .await
        .expect_err("apply-delta should require flushed hoist provenance");
    assert!(matches!(before_error, SandboxError::MissingHoistProvenance));

    session
        .flush()
        .await
        .expect("flush session with hoist provenance");
    let after_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        72,
        812,
        deterministic_services(),
    )
    .await
    .expect("reopen durable cut after hoist flush");
    assert!(
        after_flush
            .filesystem()
            .read_file(terracedb_sandbox::disk::HOIST_MANIFEST_PATH)
            .await
            .expect("read durable hoist manifest after flush")
            .is_some()
    );
    after_flush
        .eject_to_disk(terracedb_sandbox::EjectRequest {
            target_path: target.to_string_lossy().into_owned(),
            mode: terracedb_sandbox::EjectMode::ApplyDelta,
            conflict_policy: ConflictPolicy::Fail,
        })
        .await
        .expect("apply durable delta after flush");
    assert_eq!(read_host_file(&target.join("tracked.txt")), "beta\n");

    cleanup(&source);
    cleanup(&target);
}

#[tokio::test]
async fn pr_export_bookkeeping_tool_runs_only_survive_durable_recovery_after_flush() {
    let seed = 0xb140_u64;
    let fake = FakeGithubServer::spawn().await;
    let repo = deterministic_temp_dir("crash-pr-repo", seed);
    init_git_repo(&repo, Some(&fake.remote_url), seed);

    let (source_vfs, sandbox) = sandbox_store(80, 813, remote_github_services("127.0.0.1"));
    let base_volume_id = VolumeId::new(0xb140);
    let session_volume_id = VolumeId::new(0xb141);
    create_empty_base(&source_vfs, base_volume_id)
        .await
        .expect("create empty base");

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmPureJs,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: Some(GitProvenance {
                repo_root: repo.to_string_lossy().into_owned(),
                origin: terracedb_sandbox::GitRepositoryOrigin::RemoteImport,
                head_commit: Some("HEAD".to_string()),
                branch: Some("main".to_string()),
                remote_url: Some(fake.remote_url.clone()),
                remote_bridge_metadata: BTreeMap::new(),
                object_format: Some(GitObjectFormat::Sha1),
                pathspec: vec![".".to_string()],
                dirty: false,
            }),
        })
        .await
        .expect("open host git session");
    session.flush().await.expect("flush initial session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session.flush().await.expect("flush hoisted git provenance");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            format!("pr-{seed:x}\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update tracked file");
    session
        .create_pull_request(PullRequestRequest {
            title: "Crash PR".to_string(),
            body: "Bookkeeping recovery".to_string(),
            head_branch: format!("sandbox/crash-{seed:x}"),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request");
    assert_eq!(
        fake.branch_file_text(&format!("sandbox/crash-{seed:x}"), "tracked.txt")
            .await,
        Some(format!("pr-{seed:x}\n"))
    );

    let before_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        81,
        814,
        remote_github_services("127.0.0.1"),
    )
    .await
    .expect("reopen durable cut before pr flush");
    let before_tools = chronological_tool_names(&before_flush)
        .await
        .expect("read durable tool names before flush");
    assert!(
        !before_tools.contains(&"sandbox.pr.create".to_string()),
        "pr bookkeeping should not survive before flush"
    );

    session
        .flush()
        .await
        .expect("flush session with pr bookkeeping");
    let after_flush = reopen_durable_cut(
        &source_vfs,
        session_volume_id,
        82,
        815,
        remote_github_services("127.0.0.1"),
    )
    .await
    .expect("reopen durable cut after pr flush");
    let after_tools = chronological_tool_names(&after_flush)
        .await
        .expect("read durable tool names after flush");
    assert!(after_tools.contains(&"sandbox.pr.create".to_string()));

    cleanup(&repo);
}
