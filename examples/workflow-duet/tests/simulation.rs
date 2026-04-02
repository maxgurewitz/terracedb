use std::{sync::Arc, time::Duration};

use terracedb::{Db, DbDependencies, DeterministicRng, SimulatedFileSystem, SimulatedObjectStore};
use terracedb_example_workflow_duet::{
    ReviewStage, WorkflowDuetApp, WorkflowDuetFlavor, WorkflowDuetInspection,
    workflow_duet_db_config,
};
use terracedb_simulation::{SeededSimulationRunner, TurmoilClock};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SimulationCapture {
    paired_native: WorkflowDuetInspection,
    paired_sandbox: WorkflowDuetInspection,
    native_timeout: WorkflowDuetInspection,
    sandbox_timeout: WorkflowDuetInspection,
}

fn run_seeded_example(seed: u64) -> turmoil::Result<SimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(500))
        .run_with(move |_context| async move {
            let file_system = Arc::new(SimulatedFileSystem::default());
            let object_store = Arc::new(SimulatedObjectStore::default());
            let clock = Arc::new(TurmoilClock);
            let db = Db::open(
                workflow_duet_db_config(
                    &format!("/workflow-duet-sim-{seed}/ssd"),
                    &format!("workflow-duet/sim/{seed}"),
                ),
                DbDependencies::new(
                    file_system.clone(),
                    object_store.clone(),
                    clock.clone(),
                    Arc::new(DeterministicRng::seeded(seed)),
                ),
            )
            .await?;

            let app = WorkflowDuetApp::open(
                db,
                clock.clone(),
                Arc::new(DeterministicRng::seeded(seed ^ 0x4444)),
            )
            .await?;
            let handles = app.start().await?;

            app.kick_off_pair("paired-ok").await?;
            app.kick_off(WorkflowDuetFlavor::Native, "native-timeout")
                .await?;
            app.kick_off(WorkflowDuetFlavor::Sandbox, "sandbox-timeout")
                .await?;

            let paired_native = app
                .wait_for_stage(
                    WorkflowDuetFlavor::Native,
                    "paired-ok",
                    ReviewStage::Approved,
                    Duration::from_millis(250),
                )
                .await?;
            let paired_sandbox = app
                .wait_for_stage(
                    WorkflowDuetFlavor::Sandbox,
                    "paired-ok",
                    ReviewStage::Approved,
                    Duration::from_millis(250),
                )
                .await?;
            let native_timeout_waiting = app
                .wait_for_stage(
                    WorkflowDuetFlavor::Native,
                    "native-timeout",
                    ReviewStage::WaitingApproval,
                    Duration::from_millis(250),
                )
                .await?;
            let timeout_waiting = app
                .wait_for_stage(
                    WorkflowDuetFlavor::Sandbox,
                    "sandbox-timeout",
                    ReviewStage::WaitingApproval,
                    Duration::from_millis(250),
                )
                .await?;
            assert_eq!(paired_native.attempt, 2);
            assert_eq!(paired_sandbox.attempt, 2);
            assert_eq!(native_timeout_waiting.attempt, 2);
            assert_eq!(timeout_waiting.attempt, 2);

            handles.abort().await?;
            drop(app);

            let reopened_db = Db::open(
                workflow_duet_db_config(
                    &format!("/workflow-duet-sim-{seed}/ssd"),
                    &format!("workflow-duet/sim/{seed}"),
                ),
                DbDependencies::new(
                    file_system,
                    object_store,
                    clock.clone(),
                    Arc::new(DeterministicRng::seeded(seed.wrapping_add(1))),
                ),
            )
            .await?;
            let reopened = WorkflowDuetApp::open(
                reopened_db,
                clock,
                Arc::new(DeterministicRng::seeded(seed ^ 0x8888)),
            )
            .await?;
            let reopened_handles = reopened.start().await?;

            reopened
                .wait_for_stage(
                    WorkflowDuetFlavor::Native,
                    "paired-ok",
                    ReviewStage::Approved,
                    Duration::from_millis(250),
                )
                .await?;
            reopened
                .wait_for_stage(
                    WorkflowDuetFlavor::Sandbox,
                    "paired-ok",
                    ReviewStage::Approved,
                    Duration::from_millis(250),
                )
                .await?;
            reopened
                .wait_for_stage(
                    WorkflowDuetFlavor::Sandbox,
                    "sandbox-timeout",
                    ReviewStage::TimedOut,
                    Duration::from_millis(250),
                )
                .await?;
            reopened
                .wait_for_stage(
                    WorkflowDuetFlavor::Native,
                    "native-timeout",
                    ReviewStage::TimedOut,
                    Duration::from_millis(250),
                )
                .await?;

            let capture = SimulationCapture {
                paired_native: reopened
                    .inspect_instance(WorkflowDuetFlavor::Native, "paired-ok")
                    .await?
                    .expect("paired native inspection"),
                paired_sandbox: reopened
                    .inspect_instance(WorkflowDuetFlavor::Sandbox, "paired-ok")
                    .await?
                    .expect("paired sandbox inspection"),
                native_timeout: reopened
                    .inspect_instance(WorkflowDuetFlavor::Native, "native-timeout")
                    .await?
                    .expect("native timeout inspection"),
                sandbox_timeout: reopened
                    .inspect_instance(WorkflowDuetFlavor::Sandbox, "sandbox-timeout")
                    .await?
                    .expect("timeout inspection"),
            };
            reopened_handles.shutdown().await?;
            Ok(capture)
        })
}

#[test]
fn workflow_duet_replays_under_seeded_simulation() -> turmoil::Result {
    let first = run_seeded_example(0x7711)?;
    let second = run_seeded_example(0x7711)?;

    assert_eq!(first, second);
    assert_eq!(first.paired_native.status, "approved");
    assert_eq!(first.paired_sandbox.status, "approved");
    assert_eq!(first.native_timeout.status, "timed-out");
    assert_eq!(first.sandbox_timeout.status, "timed-out");
    assert_eq!(first.paired_native.attempt, 2);
    assert_eq!(first.paired_sandbox.attempt, 2);
    assert_eq!(first.native_timeout.attempt, 2);
    assert_eq!(first.sandbox_timeout.attempt, 2);
    assert_eq!(first.paired_native.lifecycle, "completed");
    assert_eq!(first.paired_sandbox.lifecycle, "completed");
    assert_eq!(first.native_timeout.lifecycle, "failed");
    assert_eq!(first.sandbox_timeout.lifecycle, "failed");
    assert_eq!(
        first
            .paired_sandbox
            .visibility_summary
            .get("workflow_slug")
            .map(String::as_str),
        Some("workflowDuetSandbox")
    );
    assert!(
        first
            .paired_native
            .visible_statuses
            .contains(&"approved".to_string())
    );
    assert!(
        first
            .paired_sandbox
            .visible_statuses
            .contains(&"approved".to_string())
    );
    assert!(
        first
            .native_timeout
            .visible_statuses
            .contains(&"timed-out".to_string())
    );
    assert!(
        first
            .sandbox_timeout
            .visible_statuses
            .contains(&"timed-out".to_string())
    );
    Ok(())
}
