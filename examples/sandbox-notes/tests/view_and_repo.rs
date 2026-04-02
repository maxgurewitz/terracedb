use std::{fs, path::Path, process::Command, sync::Arc};

use terracedb_example_sandbox_notes::{
    ExampleHostApp, cleanup, install_example_packages, run_example_bash, run_example_review,
    unique_temp_path, write_companion_project,
};
use terracedb_sandbox::{
    AuthenticatedReadonlyViewRemoteEndpoint, HoistMode, HoistRequest, ReadonlyViewClient,
    SandboxHarness, StaticReadonlyViewRegistry,
};
use terracedb_vfs::VolumeId;

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

#[tokio::test]
async fn local_and_remote_view_bridges_browse_the_example_project() {
    let app = ExampleHostApp::sample();
    let repo = unique_temp_path("view-repo");
    let remote = unique_temp_path("view-remote");
    write_companion_project(&repo)
        .await
        .expect("write companion project");
    init_example_repo(&repo, &remote);

    let base_volume_id = VolumeId::new(0xa200);
    let session_volume_id = VolumeId::new(0xa201);
    let policy_session_id = format!("view-{session_volume_id:?}");
    let prepared = app
        .prepare_notes_draft_session(policy_session_id.clone())
        .expect("prepare preset session");
    let services = app
        .host_git_services_for_prepared_session(policy_session_id, &prepared)
        .expect("policy-aware host git services");
    let sandbox_manifest = services.capabilities.manifest();
    let harness = SandboxHarness::deterministic(30, 74, services);

    let session = harness
        .open_session_with(base_volume_id, session_volume_id, |config| {
            config
                .with_capabilities(sandbox_manifest)
                .with_package_compat(terracedb_sandbox::PackageCompatibilityMode::NpmPureJs)
                .with_execution_policy(prepared.resolved.execution_policy.clone())
        })
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

    let registry = Arc::new(StaticReadonlyViewRegistry::new([session.clone()]));
    let service = Arc::new(terracedb_sandbox::ReadonlyViewService::new(registry));
    let local: ReadonlyViewClient<_> = service.local_client();
    let remote_endpoint = Arc::new(AuthenticatedReadonlyViewRemoteEndpoint::new(
        service.clone(),
        "sandbox-notes-token",
    ));
    let remote_client: ReadonlyViewClient<_> = remote_endpoint.client("sandbox-notes-token");

    let sessions = local.list_sessions().await.expect("list local sessions");
    assert_eq!(sessions.len(), 1);

    let handle = local
        .open_visible(session_volume_id, "/workspace/generated", Some("generated"))
        .await
        .expect("open local view");

    let entries = local
        .read_dir(&handle.location)
        .await
        .expect("read generated dir");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "triage-summary.json");

    let local_bytes = local
        .read_file(&entries[0].location)
        .await
        .expect("read generated file")
        .expect("summary file should exist");
    assert!(String::from_utf8_lossy(&local_bytes).contains("notes-inbox"));

    remote_client
        .reconnect()
        .await
        .expect("reconnect remote bridge");
    let remote_bytes = remote_client
        .read_file(&entries[0].location)
        .await
        .expect("read file over remote bridge")
        .expect("remote bytes");
    assert_eq!(remote_bytes, local_bytes);

    let pr = session
        .create_pull_request(terracedb_sandbox::PullRequestRequest {
            title: "Sandbox notes update".to_string(),
            body: "Generated from the example.".to_string(),
            head_branch: "sandbox/example-notes".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request");

    assert_eq!(
        pr.metadata.get("committed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        pr.metadata.get("pushed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert!(pr.metadata.get("workspace_path").is_none());
    assert!({
        let remote_summary = sanitized_git_command()
            .arg("--git-dir")
            .arg(&remote)
            .args([
                "show",
                "refs/heads/sandbox/example-notes:generated/triage-summary.json",
            ])
            .output()
            .expect("read pushed summary");
        assert!(
            remote_summary.status.success(),
            "expected pushed summary: {}",
            String::from_utf8_lossy(&remote_summary.stderr)
        );
        String::from_utf8_lossy(&remote_summary.stdout).contains("documentRepoExportFlow")
    });
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
}
