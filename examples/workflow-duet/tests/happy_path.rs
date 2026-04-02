use std::{sync::Arc, time::Duration};

use terracedb::{
    Db, DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_example_workflow_duet::{
    NATIVE_REGISTRATION_ID, ReviewStage, SANDBOX_BUNDLE_ID, WorkflowDuetApp, WorkflowDuetFlavor,
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
async fn paired_native_and_sandbox_workflows_cross_approve_through_the_public_apis()
-> Result<(), Box<dyn std::error::Error>> {
    let (app, clock) = open_stubbed_app("workflow-duet-public-apis").await?;
    let handles = app.start().await?;

    app.kick_off_pair("paired-review").await?;

    clock.advance(Duration::from_millis(12));

    let native_state = app
        .wait_for_stage(
            WorkflowDuetFlavor::Native,
            "paired-review",
            ReviewStage::Approved,
            Duration::from_millis(200),
        )
        .await?;
    let sandbox_state = app
        .wait_for_stage(
            WorkflowDuetFlavor::Sandbox,
            "paired-review",
            ReviewStage::Approved,
            Duration::from_millis(200),
        )
        .await?;

    assert_eq!(native_state.attempt, 2);
    assert_eq!(sandbox_state.attempt, 2);

    let native = app
        .inspect_instance(WorkflowDuetFlavor::Native, "paired-review")
        .await?
        .expect("native inspection");
    let sandbox = app
        .inspect_instance(WorkflowDuetFlavor::Sandbox, "paired-review")
        .await?
        .expect("sandbox inspection");

    assert_eq!(native.target, NATIVE_REGISTRATION_ID);
    assert_eq!(sandbox.target, SANDBOX_BUNDLE_ID);
    assert_eq!(native.lifecycle, "completed");
    assert_eq!(sandbox.lifecycle, "completed");
    assert!(
        native
            .latest_savepoint_id
            .as_deref()
            .is_some_and(|id| id.starts_with("savepoint:"))
    );
    assert!(
        sandbox
            .latest_savepoint_id
            .as_deref()
            .is_some_and(|id| id.starts_with("savepoint:"))
    );
    assert!(native.visible_statuses.contains(&"approved".to_string()));
    assert!(sandbox.visible_statuses.contains(&"approved".to_string()));

    handles.shutdown().await?;
    Ok(())
}
