use std::{sync::Arc, time::Duration};

use terracedb_example_sandbox_notes::{
    ExampleHostApp, install_example_packages, run_example_bash, run_example_review,
    seed_example_project_into_base,
};
use terracedb_sandbox::{
    ReadonlyViewClient, ReadonlyViewLocation, SandboxHarness, StaticReadonlyViewRegistry,
};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{InMemoryVfsStore, VolumeId};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SimulationCapture {
    summary_json: String,
    note_comment_count: usize,
    view_bytes: Vec<u8>,
    tool_names: Vec<String>,
}

fn run_seeded_example(seed: u64) -> turmoil::Result<SimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |context| async move {
            let app = ExampleHostApp::sample();
            let vfs = InMemoryVfsStore::new(context.clock(), context.rng());
            let base_volume_id = VolumeId::new(0xa300 + seed as u128);
            let session_volume_id = VolumeId::new(0xa400 + seed as u128);
            let policy_session_id = format!("simulation-{session_volume_id:?}");
            let prepared = app
                .prepare_notes_draft_session(policy_session_id.clone())
                .expect("prepare preset session");
            let services = app
                .deterministic_services_for_prepared_session(policy_session_id, &prepared)
                .expect("policy-aware services");
            let harness =
                SandboxHarness::new(Arc::new(vfs.clone()), context.clock(), services.clone());

            harness
                .ensure_volume(base_volume_id)
                .await
                .expect("create base");
            seed_example_project_into_base(&vfs, base_volume_id)
                .await
                .expect("seed project into base");
            let sandbox_manifest = services.capabilities.manifest();

            let session = harness
                .open_session_with(base_volume_id, session_volume_id, |config| {
                    config
                        .with_capabilities(sandbox_manifest)
                        .with_package_compat(terracedb_sandbox::PackageCompatibilityMode::NpmPureJs)
                        .with_execution_policy(prepared.resolved.execution_policy.clone())
                })
                .await
                .expect("open session");
            install_example_packages(&session)
                .await
                .expect("install packages");
            run_example_review(&session)
                .await
                .expect("run review entrypoint");
            run_example_bash(&session).await.expect("run bash");

            let registry = Arc::new(StaticReadonlyViewRegistry::new([session.clone()]));
            let service = Arc::new(terracedb_sandbox::ReadonlyViewService::new(registry));
            let client: ReadonlyViewClient<_> = service.local_client();
            client
                .open_visible(session_volume_id, "/workspace/generated", Some("generated"))
                .await
                .expect("open readonly view");
            let bytes = client
                .read_file(&ReadonlyViewLocation {
                    session_volume_id,
                    cut: terracedb_sandbox::ReadonlyViewCut::Visible,
                    path: "/workspace/generated/triage-summary.json".to_string(),
                })
                .await
                .expect("read summary file")
                .expect("view bytes");

            let summary_json = String::from_utf8(
                session
                    .filesystem()
                    .read_file("/workspace/generated/triage-summary.json")
                    .await
                    .expect("read summary file from session")
                    .expect("summary bytes"),
            )
            .expect("summary utf8");
            let tool_names = session
                .volume()
                .tools()
                .recent(None)
                .await
                .expect("recent tool runs")
                .into_iter()
                .map(|run| run.name)
                .collect::<Vec<_>>();
            Ok(SimulationCapture {
                summary_json,
                note_comment_count: app
                    .notes_snapshot()
                    .await
                    .first()
                    .map(|note| note.comments.len())
                    .unwrap_or_default(),
                view_bytes: bytes,
                tool_names: {
                    let mut names = tool_names;
                    names.sort();
                    names
                },
            })
        })
}

#[test]
fn example_replays_identically_under_seeded_simulation() -> turmoil::Result {
    let first = run_seeded_example(0x55aa)?;
    let second = run_seeded_example(0x55aa)?;
    let summary: serde_json::Value =
        serde_json::from_str(&first.summary_json).expect("decode summary json");

    assert_eq!(first, second);
    assert_eq!(summary["project"], serde_json::json!("notes-inbox"));
    assert_eq!(summary["openCount"], serde_json::json!(2));
    assert_eq!(first.note_comment_count, 2);
    assert!(String::from_utf8_lossy(&first.view_bytes).contains("openCount"));
    assert!(
        first
            .tool_names
            .contains(&"host_api.notes.addComment".to_string())
    );
    assert!(
        first
            .tool_names
            .contains(&"sandbox.package.install".to_string())
    );
    assert!(
        first
            .tool_names
            .contains(&"sandbox.runtime.exec_module".to_string())
    );
    assert!(first.tool_names.contains(&"sandbox.bash.exec".to_string()));
    Ok(())
}
