use tempfile::tempdir;
use terracedb::{ExecutionDomainPlacement, ResourceAdmissionDecision};
use terracedb_example_domains_api::{
    ANALYTICS_DATABASE_NAME, AdmissionProbeRequest, BackgroundMaintenanceRequest,
    ControlPlaneTableRequest, CreatePrimaryItemRequest, DomainsApp, DomainsExampleProfile,
    ExampleDatabase, ExampleLane, HelperLoadRequest, PRIMARY_DATABASE_NAME, ProbeUsage,
    domains_db_builder,
};

struct AppFixture {
    _dir: tempfile::TempDir,
    app: DomainsApp,
}

async fn open_fixture(
    profile: DomainsExampleProfile,
) -> Result<AppFixture, Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let data_root = dir.path();
    let deployment = profile.deployment()?;
    let object_store_root = data_root.join("object-store");

    let primary = domains_db_builder(
        data_root.join("primary-ssd").to_string_lossy().as_ref(),
        "fixture/primary",
    )
    .local_object_store(object_store_root.clone())
    .colocated_database(&deployment, PRIMARY_DATABASE_NAME)?
    .open()
    .await?;

    let analytics = domains_db_builder(
        data_root.join("analytics-ssd").to_string_lossy().as_ref(),
        "fixture/analytics",
    )
    .local_object_store(object_store_root)
    .colocated_database(&deployment, ANALYTICS_DATABASE_NAME)?
    .open()
    .await?;

    Ok(AppFixture {
        _dir: dir,
        app: DomainsApp::open(primary, analytics, profile).await?,
    })
}

#[tokio::test]
async fn default_profile_reopens_with_reserved_control_plane_and_domain_introspection() {
    let dir = tempdir().expect("tempdir");
    let data_root = dir.path();
    let deployment = DomainsExampleProfile::Conservative
        .deployment()
        .expect("deployment");
    let object_store_root = data_root.join("object-store");

    let primary = domains_db_builder(
        data_root.join("primary-ssd").to_string_lossy().as_ref(),
        "reopen/primary",
    )
    .local_object_store(object_store_root.clone())
    .colocated_database(&deployment, PRIMARY_DATABASE_NAME)
    .expect("bind primary")
    .open()
    .await
    .expect("open primary");
    let analytics = domains_db_builder(
        data_root.join("analytics-ssd").to_string_lossy().as_ref(),
        "reopen/analytics",
    )
    .local_object_store(object_store_root.clone())
    .colocated_database(&deployment, ANALYTICS_DATABASE_NAME)
    .expect("bind analytics")
    .open()
    .await
    .expect("open analytics");

    let app = DomainsApp::open(primary, analytics, DomainsExampleProfile::Conservative)
        .await
        .expect("open app");
    app.state()
        .create_primary_item(CreatePrimaryItemRequest {
            item_id: "ticket-1".to_string(),
            title: "Keep the foreground path snappy".to_string(),
        })
        .await
        .expect("create primary item");
    app.state()
        .run_helper_load(HelperLoadRequest {
            batch_id: "batch-a".to_string(),
            report_count: 4,
            hold_foreground_cpu_workers: 2,
            hold_background_tasks: 1,
            background_in_flight_bytes: 256,
            queued_work_items: 3,
            queued_bytes: 512,
            flush_after_write: true,
        })
        .await
        .expect("run helper load");
    app.state()
        .ensure_control_plane_table(ControlPlaneTableRequest {
            database: ExampleDatabase::Primary,
            table_name: "audit_log".to_string(),
        })
        .await
        .expect("create control-plane table");

    let report = app.state().observability_report().expect("report");
    assert_eq!(report.profile, DomainsExampleProfile::Conservative);
    assert_eq!(
        report.primary.foreground.binding.domain.to_string(),
        "process/dbs/primary/foreground"
    );
    assert_eq!(
        report
            .analytics
            .foreground
            .snapshot
            .as_ref()
            .expect("analytics fg")
            .spec
            .placement,
        ExecutionDomainPlacement::SharedWeighted { weight: 1 }
    );
    let control = report
        .primary
        .control_plane
        .snapshot
        .as_ref()
        .expect("control-plane snapshot");
    assert_eq!(control.spec.placement, ExecutionDomainPlacement::Dedicated);
    assert_eq!(
        control.spec.metadata.get("reserved").map(String::as_str),
        Some("true")
    );

    app.shutdown().await.expect("shutdown");

    let reopened_primary = domains_db_builder(
        data_root.join("primary-ssd").to_string_lossy().as_ref(),
        "reopen/primary",
    )
    .local_object_store(object_store_root.clone())
    .colocated_database(&deployment, PRIMARY_DATABASE_NAME)
    .expect("rebind primary")
    .open()
    .await
    .expect("reopen primary");
    let reopened_analytics = domains_db_builder(
        data_root.join("analytics-ssd").to_string_lossy().as_ref(),
        "reopen/analytics",
    )
    .local_object_store(object_store_root)
    .colocated_database(&deployment, ANALYTICS_DATABASE_NAME)
    .expect("rebind analytics")
    .open()
    .await
    .expect("reopen analytics");

    let reopened = DomainsApp::open(
        reopened_primary,
        reopened_analytics,
        DomainsExampleProfile::Conservative,
    )
    .await
    .expect("reopen app");
    assert_eq!(
        reopened
            .state()
            .list_primary_items()
            .await
            .expect("list primary items"),
        vec![terracedb_example_domains_api::PrimaryItemRecord {
            item_id: "ticket-1".to_string(),
            title: "Keep the foreground path snappy".to_string(),
        }]
    );
    assert_eq!(
        reopened
            .state()
            .list_helper_reports()
            .await
            .expect("list helper reports")
            .len(),
        4
    );
    assert!(
        reopened
            .state()
            .primary_db()
            .try_table("audit_log")
            .is_some(),
        "control-plane table should survive reopen"
    );
}

async fn run_profile_workload(
    profile: DomainsExampleProfile,
) -> Result<
    (
        Vec<terracedb_example_domains_api::PrimaryItemRecord>,
        Vec<terracedb_example_domains_api::HelperReportRecord>,
        ResourceAdmissionDecision,
    ),
    Box<dyn std::error::Error>,
> {
    let fixture = open_fixture(profile).await?;
    let state = fixture.app.state();
    state
        .create_primary_item(CreatePrimaryItemRequest {
            item_id: "item-a".to_string(),
            title: "Primary traffic".to_string(),
        })
        .await?;
    state
        .create_primary_item(CreatePrimaryItemRequest {
            item_id: "item-b".to_string(),
            title: "Still correct under load".to_string(),
        })
        .await?;
    state
        .apply_primary_maintenance(BackgroundMaintenanceRequest {
            flush_now: true,
            hold_background_tasks: 1,
            background_in_flight_bytes: 128,
            queued_work_items: 5,
            queued_bytes: 1024,
        })
        .await?;
    state
        .run_helper_load(HelperLoadRequest {
            batch_id: "batch-b".to_string(),
            report_count: 3,
            hold_foreground_cpu_workers: 2,
            hold_background_tasks: 1,
            background_in_flight_bytes: 256,
            queued_work_items: 6,
            queued_bytes: 1536,
            flush_after_write: true,
        })
        .await?;

    let probe = state.probe_admission(AdmissionProbeRequest {
        database: ExampleDatabase::Primary,
        lane: ExampleLane::Foreground,
        usage: ProbeUsage {
            cpu_workers: 4,
            ..ProbeUsage::default()
        },
    })?;

    Ok((
        state.list_primary_items().await?,
        state.list_helper_reports().await?,
        probe.decision,
    ))
}

#[tokio::test]
async fn placement_profiles_change_admission_behavior_without_changing_logical_answers() {
    let conservative = run_profile_workload(DomainsExampleProfile::Conservative)
        .await
        .expect("conservative workload");
    let balanced = run_profile_workload(DomainsExampleProfile::Balanced)
        .await
        .expect("balanced workload");

    assert_eq!(conservative.0, balanced.0);
    assert_eq!(conservative.1, balanced.1);
    assert_ne!(
        conservative.2.effective_budget.cpu.worker_slots,
        balanced.2.effective_budget.cpu.worker_slots
    );
    assert!(conservative.2.admitted);
    assert!(!balanced.2.admitted);
    assert_eq!(conservative.2.effective_budget.cpu.worker_slots, Some(4));
    assert_eq!(balanced.2.effective_budget.cpu.worker_slots, Some(3));
}
