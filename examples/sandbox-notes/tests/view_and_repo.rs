use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use terracedb_example_sandbox_notes::{
    ExampleHostApp, cleanup, create_empty_base, install_example_packages, run_example_bash,
    run_example_review, unique_temp_path, write_companion_project,
};
use terracedb_sandbox::{
    AuthenticatedReadonlyViewRemoteEndpoint, CapabilityRegistry, ConflictPolicy, HoistMode,
    HoistRequest, HostGitWorkspaceManager, LocalReadonlyViewBridge, PackageCompatibilityMode,
    PullRequestRequest, ReadonlyViewCut, ReadonlyViewProtocolRequest, ReadonlyViewProtocolResponse,
    ReadonlyViewProtocolTransport, ReadonlyViewRequest, RemoteReadonlyViewBridge, SandboxConfig,
    SandboxServices, SandboxStore, StaticReadonlyViewRegistry,
};
use terracedb_vfs::VolumeId;

fn session_config(
    base_volume_id: VolumeId,
    session_volume_id: VolumeId,
    capabilities: terracedb_sandbox::CapabilityManifest,
) -> SandboxConfig {
    SandboxConfig {
        session_volume_id,
        session_chunk_size: Some(4096),
        base_volume_id,
        durable_base: false,
        workspace_root: "/workspace".to_string(),
        package_compat: PackageCompatibilityMode::NpmPureJs,
        conflict_policy: ConflictPolicy::Fail,
        capabilities,
        hoisted_source: None,
        git_provenance: None,
    }
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

fn git_stdout(dir: &Path, args: &[&str]) -> String {
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

fn init_example_repo(repo: &Path, remote: &Path) {
    fs::create_dir_all(repo).expect("create repo dir");
    fs::create_dir_all(remote.parent().expect("remote parent")).expect("create remote parent");
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.name", "Sandbox Example"]);
    git(repo, &["config", "user.email", "sandbox@example.invalid"]);
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
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "initial"]);
    git(repo, &["push", "-u", "origin", "main"]);
}

fn host_git_services(app: &ExampleHostApp) -> SandboxServices {
    let mut services = app.host_git_services();
    services.git = std::sync::Arc::new(HostGitWorkspaceManager::default());
    services
}

#[tokio::test]
async fn local_and_remote_view_bridges_browse_the_example_project() {
    let app = ExampleHostApp::sample();
    let repo = unique_temp_path("view-repo");
    let remote = unique_temp_path("view-remote");
    write_companion_project(&repo)
        .await
        .expect("write companion project");
    init_example_repo(&repo, &remote);

    let (vfs, sandbox) =
        terracedb_example_sandbox_notes::in_memory_store(30, 74, host_git_services(&app)).await;
    let base_volume_id = VolumeId::new(0xa200);
    let session_volume_id = VolumeId::new(0xa201);
    create_empty_base(&vfs, base_volume_id)
        .await
        .expect("create base volume");

    let session = sandbox
        .open_session(session_config(
            base_volume_id,
            session_volume_id,
            app.notes_registry().manifest(),
        ))
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
        .expect("hoist example repo");
    install_example_packages(&session)
        .await
        .expect("install packages");
    run_example_review(&session)
        .await
        .expect("run example review");
    run_example_bash(&session).await.expect("run bash");

    let registry = std::sync::Arc::new(StaticReadonlyViewRegistry::new([session.clone()]));
    let service = std::sync::Arc::new(terracedb_sandbox::ReadonlyViewService::new(registry));
    let local = LocalReadonlyViewBridge::new(service.clone());
    let remote_bridge = RemoteReadonlyViewBridge::new(
        std::sync::Arc::new(AuthenticatedReadonlyViewRemoteEndpoint::new(
            service.clone(),
            "sandbox-notes-token",
        )),
        "sandbox-notes-token",
    );

    let sessions = local
        .send(ReadonlyViewProtocolRequest::ListSessions)
        .await
        .expect("list local sessions");
    let ReadonlyViewProtocolResponse::Sessions { sessions } = sessions else {
        panic!("expected sessions response");
    };
    assert_eq!(sessions.len(), 1);

    let local_view = local
        .send(ReadonlyViewProtocolRequest::OpenView {
            session_volume_id,
            request: ReadonlyViewRequest {
                cut: ReadonlyViewCut::Visible,
                path: "/workspace/generated".to_string(),
                label: Some("generated".to_string()),
            },
        })
        .await
        .expect("open local view");
    let ReadonlyViewProtocolResponse::View { handle } = local_view else {
        panic!("expected view response");
    };

    let local_dir = local
        .send(ReadonlyViewProtocolRequest::ReadDir {
            location: handle.location.clone(),
        })
        .await
        .expect("read generated dir");
    let ReadonlyViewProtocolResponse::Directory { entries } = local_dir else {
        panic!("expected directory response");
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "triage-summary.json");

    let local_file = local
        .send(ReadonlyViewProtocolRequest::ReadFile {
            location: entries[0].location.clone(),
        })
        .await
        .expect("read generated file");
    let ReadonlyViewProtocolResponse::File { bytes } = local_file else {
        panic!("expected file response");
    };
    let local_bytes = bytes.expect("summary file should exist");
    assert!(String::from_utf8_lossy(&local_bytes).contains("notes-inbox"));

    remote_bridge
        .reconnect()
        .await
        .expect("reconnect remote bridge");
    let remote_file = remote_bridge
        .send(ReadonlyViewProtocolRequest::ReadFile {
            location: entries[0].location.clone(),
        })
        .await
        .expect("read file over remote bridge");
    let ReadonlyViewProtocolResponse::File { bytes } = remote_file else {
        panic!("expected file response");
    };
    assert_eq!(bytes.expect("remote bytes"), local_bytes);

    let pr = session
        .create_pull_request(PullRequestRequest {
            title: "Sandbox notes update".to_string(),
            body: "Generated from the example.".to_string(),
            head_branch: "sandbox/example-notes".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request");
    let workspace = PathBuf::from(
        pr.metadata
            .get("workspace_path")
            .and_then(|value| value.as_str())
            .expect("workspace path"),
    );

    assert_eq!(
        pr.metadata.get("committed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        pr.metadata.get("pushed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert!(
        fs::read_to_string(workspace.join("generated/triage-summary.json"))
            .expect("read exported summary")
            .contains("documentRepoExportFlow")
    );
    assert_eq!(
        git_stdout(&workspace, &["rev-parse", "--abbrev-ref", "HEAD"]),
        "sandbox/example-notes"
    );
    let remote_branch = sanitized_git_command()
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/sandbox/example-notes"])
        .output()
        .expect("inspect remote branch");
    assert!(
        remote_branch.status.success(),
        "expected pushed remote branch: {}",
        String::from_utf8_lossy(&remote_branch.stderr)
    );

    cleanup(&repo);
    cleanup(&remote);
    cleanup(&workspace);
}
