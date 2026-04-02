use std::fs;

use terracedb_capabilities::ExecutionOperation;
use terracedb_example_sandbox_notes::{
    ExampleHostApp, GENERATED_SUMMARY_PATH, cleanup, direct_add_comment, emit_example_typescript,
    guest_add_comment, hoist_companion_project, install_example_packages, open_generated_view,
    read_summary_from_session, run_example_bash, run_example_review, typecheck_example,
    unique_temp_path,
};
use terracedb_sandbox::{ConflictPolicy, EjectMode, EjectRequest, SandboxError, SandboxHarness};
use terracedb_vfs::VolumeId;

#[tokio::test]
async fn documented_happy_path_runs_end_to_end() {
    let app = ExampleHostApp::sample();
    let base_volume_id = VolumeId::new(0xa100);
    let session_volume_id = VolumeId::new(0xa101);
    let policy_session_id = format!("happy-path-{session_volume_id:?}");
    let prepared = app
        .prepare_notes_draft_session(policy_session_id.clone())
        .expect("prepare preset session");
    let services = app
        .deterministic_services_for_prepared_session(policy_session_id, &prepared)
        .expect("policy-aware services");
    let sandbox_manifest = services.capabilities.manifest();
    let harness = SandboxHarness::deterministic(10, 71, services);

    let session = harness
        .open_session_with(base_volume_id, session_volume_id, |config| {
            config
                .with_chunk_size(4096)
                .with_package_compat(terracedb_sandbox::PackageCompatibilityMode::NpmPureJs)
                .with_capabilities(sandbox_manifest)
                .with_execution_policy(prepared.resolved.execution_policy.clone())
        })
        .await
        .expect("open example session");

    hoist_companion_project(&session)
        .await
        .expect("hoist companion project");
    let packages = install_example_packages(&session)
        .await
        .expect("install example packages");
    let typecheck = typecheck_example(&session)
        .await
        .expect("typecheck example");
    let emitted = emit_example_typescript(&session)
        .await
        .expect("emit typescript");
    let (runtime_summary, runtime_result) = run_example_review(&session)
        .await
        .expect("execute review entrypoint");
    let bash = run_example_bash(&session).await.expect("run bash flow");
    let view = open_generated_view(&session)
        .await
        .expect("open readonly generated view");
    let summary = read_summary_from_session(&session)
        .await
        .expect("read summary file");
    let session_info = session.info().await;

    let export_dir = unique_temp_path("happy-export");
    fs::create_dir_all(&export_dir).expect("create export dir");
    session
        .eject_to_disk(EjectRequest {
            target_path: export_dir.to_string_lossy().into_owned(),
            mode: EjectMode::MaterializeSnapshot,
            conflict_policy: ConflictPolicy::Fail,
        })
        .await
        .expect("export materialized snapshot");

    let tool_names = session
        .volume()
        .tools()
        .recent(None)
        .await
        .expect("read tool history")
        .into_iter()
        .map(|run| run.name)
        .collect::<Vec<_>>();

    assert_eq!(prepared.preset.name, "notes-review");
    assert_eq!(
        prepared
            .profile
            .as_ref()
            .map(|profile| profile.name.as_str()),
        Some("foreground")
    );
    assert_eq!(
        prepared.audit_metadata.get("expanded_manifest"),
        Some(&serde_json::to_value(&prepared.resolved.manifest).expect("manifest json"))
    );
    assert_eq!(
        session_info.services.runtime_backend,
        "notes-runtime-dedicated-sandbox"
    );
    assert_eq!(
        session_info.services.package_installer,
        "notes-packages-dedicated-sandbox"
    );
    assert_eq!(
        session_info.services.typescript_service,
        "notes-typescript-owner-foreground"
    );
    assert_eq!(
        session_info.services.bash_service,
        "notes-bash-owner-foreground"
    );
    assert_eq!(
        session_info.provenance.execution_policy,
        Some(prepared.resolved.execution_policy.clone())
    );
    assert_eq!(
        session_info.services.execution_bindings[&ExecutionOperation::DraftSession]
            .domain_path
            .as_string(),
        "process/sandbox-notes/dedicated-sandbox"
    );
    assert_eq!(
        session_info.services.execution_bindings[&ExecutionOperation::PackageInstall]
            .domain_path
            .as_string(),
        "process/sandbox-notes/dedicated-sandbox"
    );
    assert_eq!(
        session_info.services.execution_bindings[&ExecutionOperation::TypeCheck]
            .domain_path
            .as_string(),
        "process/sandbox-notes/owner-foreground"
    );
    assert_eq!(
        session_info.services.execution_bindings[&ExecutionOperation::BashHelper]
            .domain_path
            .as_string(),
        "process/sandbox-notes/owner-foreground"
    );
    assert!(typecheck.diagnostics.is_empty());
    assert_eq!(packages, vec!["lodash", "zod"]);
    assert_eq!(runtime_summary, summary);
    assert_eq!(summary.project, "notes-inbox");
    assert_eq!(summary.open_count, 2);
    assert_eq!(
        summary.slugs,
        vec!["addReviewSummaryPanel", "documentRepoExportFlow"]
    );
    assert_eq!(
        emitted.emitted_files,
        vec!["/workspace/src/render.js".to_string()]
    );
    assert_eq!(bash.cwd, "/workspace");
    assert_eq!(bash.stdout, "/workspace\n");
    assert!(view.uri.starts_with("terrace-view://"));
    assert_eq!(
        runtime_result["summary"]["openCount"],
        serde_json::json!(summary.open_count)
    );
    assert_eq!(
        session.runtime_handle().metadata["execution_domain_path"],
        serde_json::json!("process/sandbox-notes/dedicated-sandbox")
    );
    assert_eq!(
        app.notes_snapshot()
            .await
            .first()
            .and_then(|note| note.comments.last())
            .map(|comment| comment.body.as_str()),
        Some("Reviewed notes-inbox with 2 open notes")
    );
    assert!(
        fs::read_to_string(export_dir.join("generated/triage-summary.json"))
            .expect("read exported summary")
            .contains("documentRepoExportFlow")
    );
    assert!(tool_names.contains(&"sandbox.disk.hoist".to_string()));
    assert!(tool_names.contains(&"sandbox.package.install".to_string()));
    assert!(tool_names.contains(&"sandbox.typescript.check".to_string()));
    assert!(tool_names.contains(&"sandbox.typescript.emit".to_string()));
    assert!(tool_names.contains(&"sandbox.runtime.exec_module".to_string()));
    assert!(tool_names.contains(&"sandbox.bash.exec".to_string()));
    assert!(tool_names.contains(&"sandbox.view.open".to_string()));
    assert!(
        session
            .filesystem()
            .read_file(GENERATED_SUMMARY_PATH)
            .await
            .expect("read summary bytes")
            .is_some()
    );

    cleanup(&export_dir);
}

#[tokio::test]
async fn injected_note_capability_matches_direct_and_guest_calls() {
    let direct_app = ExampleHostApp::sample();
    let direct_base_volume_id = VolumeId::new(0xa110);
    let direct_session_volume_id = VolumeId::new(0xa111);
    let direct_policy_session_id = format!("direct-{direct_session_volume_id:?}");
    let direct_prepared = direct_app
        .prepare_notes_draft_session(direct_policy_session_id.clone())
        .expect("prepare direct preset session");
    let direct_services = direct_app
        .deterministic_services_for_prepared_session(direct_policy_session_id, &direct_prepared)
        .expect("direct policy-aware services");
    let direct_manifest = direct_services.capabilities.manifest();
    let direct_harness = SandboxHarness::deterministic(20, 72, direct_services);
    let direct_session = direct_harness
        .open_session_with(direct_base_volume_id, direct_session_volume_id, |config| {
            config
                .with_capabilities(direct_manifest)
                .with_execution_policy(direct_prepared.resolved.execution_policy.clone())
        })
        .await
        .expect("open direct session");

    let guest_app = ExampleHostApp::sample();
    let guest_base_volume_id = VolumeId::new(0xa120);
    let guest_session_volume_id = VolumeId::new(0xa121);
    let guest_policy_session_id = format!("guest-{guest_session_volume_id:?}");
    let guest_prepared = guest_app
        .prepare_notes_draft_session(guest_policy_session_id.clone())
        .expect("prepare guest preset session");
    let guest_services = guest_app
        .deterministic_services_for_prepared_session(guest_policy_session_id, &guest_prepared)
        .expect("guest policy-aware services");
    let guest_manifest = guest_services.capabilities.manifest();
    let guest_harness = SandboxHarness::deterministic(21, 73, guest_services);
    let guest_session = guest_harness
        .open_session_with(guest_base_volume_id, guest_session_volume_id, |config| {
            config
                .with_capabilities(guest_manifest)
                .with_execution_policy(guest_prepared.resolved.execution_policy.clone())
        })
        .await
        .expect("open guest session");

    let direct = direct_add_comment(&direct_session, "note-1", "sandbox-bot", "Direct API path")
        .await
        .expect("direct capability call");
    let guest = guest_add_comment(&guest_session, "note-1", "sandbox-bot", "Direct API path")
        .await
        .expect("guest capability call");

    assert_eq!(direct, guest);
    assert_eq!(
        direct["comments"]
            .as_array()
            .expect("comments array")
            .last()
            .and_then(|comment| comment.get("body"))
            .and_then(|value| value.as_str()),
        Some("Direct API path")
    );
}

#[tokio::test]
async fn prepared_preset_budget_is_enforced_for_host_capability_calls() {
    let app = ExampleHostApp::sample();
    let base_volume_id = VolumeId::new(0xa130);
    let session_volume_id = VolumeId::new(0xa131);
    let policy_session_id = format!("budget-{session_volume_id:?}");
    let prepared = app
        .prepare_notes_draft_session(policy_session_id.clone())
        .expect("prepare preset session");
    let services = app
        .deterministic_services_for_prepared_session(policy_session_id, &prepared)
        .expect("policy-aware services");
    let manifest = services.capabilities.manifest();
    let harness = SandboxHarness::deterministic(22, 74, services);
    let session = harness
        .open_session_with(base_volume_id, session_volume_id, |config| {
            config
                .with_capabilities(manifest)
                .with_execution_policy(prepared.resolved.execution_policy.clone())
        })
        .await
        .expect("open budget session");

    for index in 0..8 {
        direct_add_comment(
            &session,
            "note-1",
            "sandbox-bot",
            &format!("budget call {index}"),
        )
        .await
        .expect("within preset budget");
    }

    let error = direct_add_comment(&session, "note-1", "sandbox-bot", "budget call overflow")
        .await
        .expect_err("ninth capability call should exceed preset max_calls");
    assert!(matches!(
        error,
        SandboxError::Service { service, message }
        if service == "sandbox-notes"
            && message.contains("requested 9 calls exceeds max_calls 8")
    ));
}
