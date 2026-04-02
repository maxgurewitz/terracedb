use std::{sync::Arc, time::Duration};

use terracedb::{
    Db, DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_example_workflow_duet::{
    NATIVE_BUNDLE_ID, ReviewStage, SANDBOX_BUNDLE_ID, WorkflowDuetApp, WorkflowDuetFlavor,
    workflow_duet_db_config,
};

async fn open_stubbed_app(
    label: &str,
) -> Result<(WorkflowDuetApp, Arc<StubClock>), Box<dyn std::error::Error>> {
    let clock = Arc::new(StubClock::new(Timestamp::new(0)));
    let db = Db::open(
        workflow_duet_db_config(&format!("/{label}/ssd"), &format!("workflow-duet/{label}")),
        DbDependencies::new(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
            clock.clone(),
            Arc::new(StubRng::seeded(0x4411)),
        ),
    )
    .await?;
    let app = WorkflowDuetApp::open(db, clock.clone(), Arc::new(StubRng::seeded(0x5511))).await?;
    Ok((app, clock))
}

#[tokio::test]
async fn example_can_be_inspected_through_public_workflow_apis()
-> Result<(), Box<dyn std::error::Error>> {
    let (app, clock) = open_stubbed_app("workflow-duet-public-apis").await?;
    let handles = app.start().await?;

    app.kick_off(WorkflowDuetFlavor::Native, "native-1").await?;
    app.kick_off(WorkflowDuetFlavor::Sandbox, "sandbox-1")
        .await?;

    clock.advance(Duration::from_millis(12));
    app.wait_for_stage(
        WorkflowDuetFlavor::Native,
        "native-1",
        ReviewStage::WaitingApproval,
        Duration::from_millis(200),
    )
    .await?;
    app.wait_for_stage(
        WorkflowDuetFlavor::Sandbox,
        "sandbox-1",
        ReviewStage::WaitingApproval,
        Duration::from_millis(200),
    )
    .await?;

    app.approve(WorkflowDuetFlavor::Native, "native-1").await?;
    app.approve(WorkflowDuetFlavor::Sandbox, "sandbox-1")
        .await?;

    let native_state = app
        .wait_for_stage(
            WorkflowDuetFlavor::Native,
            "native-1",
            ReviewStage::Approved,
            Duration::from_millis(200),
        )
        .await?;
    let sandbox_state = app
        .wait_for_stage(
            WorkflowDuetFlavor::Sandbox,
            "sandbox-1",
            ReviewStage::Approved,
            Duration::from_millis(200),
        )
        .await?;

    assert_eq!(native_state.attempt, 2);
    assert_eq!(sandbox_state.attempt, 2);

    let native = app
        .inspect_instance(WorkflowDuetFlavor::Native, "native-1")
        .await?
        .expect("native inspection");
    let sandbox = app
        .inspect_instance(WorkflowDuetFlavor::Sandbox, "sandbox-1")
        .await?
        .expect("sandbox inspection");

    assert_eq!(native.bundle_id, NATIVE_BUNDLE_ID);
    assert_eq!(sandbox.bundle_id, SANDBOX_BUNDLE_ID);
    assert_eq!(native.lifecycle, "completed");
    assert_eq!(sandbox.lifecycle, "completed");
    assert!(
        native
            .history_event_kinds
            .contains(&"run-created".to_string())
    );
    assert!(
        native
            .history_event_kinds
            .contains(&"task-admitted".to_string())
    );
    assert!(
        native
            .history_event_kinds
            .contains(&"task-applied".to_string())
    );
    assert!(
        native
            .history_event_kinds
            .contains(&"run-completed".to_string())
    );
    assert!(
        sandbox
            .history_event_kinds
            .contains(&"run-completed".to_string())
    );

    let native_runs = app.list_runs(WorkflowDuetFlavor::Native).await?;
    let sandbox_runs = app.list_runs(WorkflowDuetFlavor::Sandbox).await?;
    assert_eq!(native_runs.entries.len(), 1);
    assert_eq!(sandbox_runs.entries.len(), 1);
    assert_eq!(
        native_runs.entries[0].record.bundle_id.to_string(),
        NATIVE_BUNDLE_ID
    );
    assert_eq!(
        sandbox_runs.entries[0].record.bundle_id.to_string(),
        SANDBOX_BUNDLE_ID
    );

    handles.shutdown().await?;
    Ok(())
}
