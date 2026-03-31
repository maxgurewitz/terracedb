use std::fs;

use terracedb_example_sandbox_notes::{
    ExampleHostApp, GENERATED_SUMMARY_PATH, cleanup, direct_add_comment, emit_example_typescript,
    guest_add_comment, hoist_companion_project, install_example_packages, open_generated_view,
    read_summary_from_session, run_example_bash, run_example_review, typecheck_example,
    unique_temp_path,
};
use terracedb_sandbox::{
    CapabilityRegistry, ConflictPolicy, EjectMode, EjectRequest, SandboxHarness,
};
use terracedb_vfs::VolumeId;

#[tokio::test]
async fn documented_happy_path_runs_end_to_end() {
    let app = ExampleHostApp::sample();
    let harness = SandboxHarness::deterministic(10, 71, app.deterministic_services());
    let base_volume_id = VolumeId::new(0xa100);
    let session_volume_id = VolumeId::new(0xa101);

    let session = harness
        .open_session_with(base_volume_id, session_volume_id, |config| {
            config
                .with_chunk_size(4096)
                .with_package_compat(terracedb_sandbox::PackageCompatibilityMode::NpmPureJs)
                .with_capabilities(app.notes_registry().manifest())
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
    let direct_harness = SandboxHarness::deterministic(20, 72, direct_app.deterministic_services());
    let direct_base_volume_id = VolumeId::new(0xa110);
    let direct_session_volume_id = VolumeId::new(0xa111);
    let direct_session = direct_harness
        .open_session_with(direct_base_volume_id, direct_session_volume_id, |config| {
            config.with_capabilities(direct_app.notes_registry().manifest())
        })
        .await
        .expect("open direct session");

    let guest_app = ExampleHostApp::sample();
    let guest_harness = SandboxHarness::deterministic(21, 73, guest_app.deterministic_services());
    let guest_base_volume_id = VolumeId::new(0xa120);
    let guest_session_volume_id = VolumeId::new(0xa121);
    let guest_session = guest_harness
        .open_session_with(guest_base_volume_id, guest_session_volume_id, |config| {
            config.with_capabilities(guest_app.notes_registry().manifest())
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
