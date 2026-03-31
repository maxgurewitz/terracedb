use tempfile::tempdir;
use terracedb::{DbComponents, ExecutionDomainPlacement, FailpointMode};
use terracedb_example_domains_api::{
    ANALYTICS_DATABASE_NAME, BackgroundMaintenanceRequest, ControlPlaneTableRequest,
    CreatePrimaryItemRequest, DomainsApp, DomainsExampleProfile, ExampleDatabase,
    HelperLoadRequest, PRIMARY_DATABASE_NAME, PRIMARY_PRESSURE_PROBE_WRITE_BYTES,
    PrimaryBurstRequest, domains_db_settings,
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
    let components = DbComponents::production_local(object_store_root);
    let mut databases = deployment
        .open_all(
            [
                (
                    PRIMARY_DATABASE_NAME.to_string(),
                    domains_db_settings(
                        data_root.join("primary-ssd").to_string_lossy().as_ref(),
                        "fixture/primary",
                    ),
                ),
                (
                    ANALYTICS_DATABASE_NAME.to_string(),
                    domains_db_settings(
                        data_root.join("analytics-ssd").to_string_lossy().as_ref(),
                        "fixture/analytics",
                    ),
                ),
            ],
            components,
        )
        .await?;
    let primary = databases
        .remove(PRIMARY_DATABASE_NAME)
        .expect("primary fixture db");
    let analytics = databases
        .remove(ANALYTICS_DATABASE_NAME)
        .expect("analytics fixture db");

    Ok(AppFixture {
        _dir: dir,
        app: DomainsApp::open(deployment, primary, analytics, profile).await?,
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
    let components = DbComponents::production_local(object_store_root.clone());
    let mut opened = deployment
        .open_all(
            [
                (
                    PRIMARY_DATABASE_NAME.to_string(),
                    domains_db_settings(
                        data_root.join("primary-ssd").to_string_lossy().as_ref(),
                        "reopen/primary",
                    ),
                ),
                (
                    ANALYTICS_DATABASE_NAME.to_string(),
                    domains_db_settings(
                        data_root.join("analytics-ssd").to_string_lossy().as_ref(),
                        "reopen/analytics",
                    ),
                ),
            ],
            components.clone(),
        )
        .await
        .expect("open colocated databases");
    let primary = opened.remove(PRIMARY_DATABASE_NAME).expect("open primary");
    let analytics = opened
        .remove(ANALYTICS_DATABASE_NAME)
        .expect("open analytics");

    let app = DomainsApp::open(
        deployment.clone(),
        primary,
        analytics,
        DomainsExampleProfile::Conservative,
    )
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

    let report = app.state().observability_report().await.expect("report");
    assert_eq!(report.profile, DomainsExampleProfile::Conservative);
    assert_eq!(
        report.deployment.databases[PRIMARY_DATABASE_NAME]
            .foreground
            .binding
            .domain
            .to_string(),
        "process/dbs/primary/foreground"
    );
    assert_eq!(
        report.deployment.databases[ANALYTICS_DATABASE_NAME]
            .foreground
            .snapshot
            .as_ref()
            .expect("analytics fg")
            .spec
            .placement,
        ExecutionDomainPlacement::SharedWeighted { weight: 1 }
    );
    let control = report.deployment.databases[PRIMARY_DATABASE_NAME]
        .control_plane
        .snapshot
        .as_ref()
        .expect("control-plane snapshot");
    assert_eq!(control.spec.placement, ExecutionDomainPlacement::Dedicated);
    assert_eq!(
        control.spec.metadata.get("reserved").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        report.primary_writer.sample_batch_write_bytes,
        PRIMARY_PRESSURE_PROBE_WRITE_BYTES
    );
    assert!(
        report
            .primary_writer
            .current_pressure
            .local
            .mutable_dirty_bytes
            > 0
    );
    assert_eq!(
        report
            .primary_writer
            .current_pressure
            .local
            .immutable_queued_bytes,
        0
    );
    assert!(!report.primary_writer.throttle_active);
    assert_eq!(
        report
            .helper_writer
            .current_pressure
            .local
            .immutable_queued_bytes,
        0
    );
    assert_eq!(
        report
            .helper_writer
            .current_pressure
            .local
            .immutable_flushing_bytes,
        0
    );
    assert!(!report.helper_writer.throttle_active);

    app.shutdown().await.expect("shutdown");

    let reopened_components = DbComponents::production_local(object_store_root);
    let mut reopened_databases = deployment
        .open_all(
            [
                (
                    PRIMARY_DATABASE_NAME.to_string(),
                    domains_db_settings(
                        data_root.join("primary-ssd").to_string_lossy().as_ref(),
                        "reopen/primary",
                    ),
                ),
                (
                    ANALYTICS_DATABASE_NAME.to_string(),
                    domains_db_settings(
                        data_root.join("analytics-ssd").to_string_lossy().as_ref(),
                        "reopen/analytics",
                    ),
                ),
            ],
            reopened_components,
        )
        .await
        .expect("reopen colocated databases");
    let reopened_primary = reopened_databases
        .remove(PRIMARY_DATABASE_NAME)
        .expect("reopen primary");
    let reopened_analytics = reopened_databases
        .remove(ANALYTICS_DATABASE_NAME)
        .expect("reopen analytics");

    let reopened = DomainsApp::open(
        deployment,
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
        terracedb_example_domains_api::DomainsObservabilityResponse,
    ),
    Box<dyn std::error::Error>,
> {
    let fixture = open_fixture(profile).await?;
    let state = fixture.app.state();
    state
        .run_primary_burst(PrimaryBurstRequest {
            batch_id: "profile-burst".to_string(),
            item_count: 2,
            title_bytes: 1_024,
        })
        .await?;
    let helper = state
        .run_helper_load(HelperLoadRequest {
            batch_id: "batch-b".to_string(),
            report_count: 3,
            hold_foreground_cpu_workers: 2,
            hold_background_tasks: 1,
            background_in_flight_bytes: 256,
            queued_work_items: 6,
            queued_bytes: 1536,
            flush_after_write: false,
        })
        .await?;
    assert_eq!(helper.written_reports, 3);
    let maintenance = state
        .apply_primary_maintenance(BackgroundMaintenanceRequest {
            flush_now: false,
            hold_background_tasks: 1,
            background_in_flight_bytes: 512,
            queued_work_items: 5,
            queued_bytes: 1024,
        })
        .await?;
    assert!(maintenance.background_admission.is_some());

    Ok((
        state.list_primary_items().await?,
        state.list_helper_reports().await?,
        state.observability_report().await?,
    ))
}

#[tokio::test]
async fn placement_profiles_change_admission_behavior_without_changing_logical_answers() {
    let conservative = run_profile_workload(DomainsExampleProfile::Conservative)
        .await
        .expect("conservative workload");
    let protected = run_profile_workload(DomainsExampleProfile::PrimaryProtected)
        .await
        .expect("primary-protected workload");

    assert_eq!(conservative.0, protected.0);
    assert_eq!(conservative.1, protected.1);
    assert!(
        conservative
            .2
            .primary_writer
            .next_write_admission
            .projected_pressure
            .mutable_dirty_bytes
            >= protected
                .2
                .primary_writer
                .next_write_admission
                .projected_pressure
                .mutable_dirty_bytes
    );
    assert!(
        conservative.2.primary_writer.next_write_admission.metadata["mutable_hard_limit_bytes"]
            .as_u64()
            .unwrap()
            > protected.2.primary_writer.next_write_admission.metadata["mutable_hard_limit_bytes"]
                .as_u64()
                .unwrap()
    );
    assert!(
        protected.2.primary_writer.next_write_admission.level
            >= conservative.2.primary_writer.next_write_admission.level
    );
}

#[tokio::test]
async fn pressure_report_tracks_dirty_queued_flushing_and_recovery() {
    let fixture = open_fixture(DomainsExampleProfile::PrimaryProtected)
        .await
        .expect("open protected fixture");
    let state = fixture.app.state();

    state
        .run_primary_burst(PrimaryBurstRequest {
            batch_id: "pressure-burst".to_string(),
            item_count: 4,
            title_bytes: 1_024,
        })
        .await
        .expect("run primary burst");
    state
        .run_helper_load(HelperLoadRequest {
            batch_id: "pressure-helper".to_string(),
            report_count: 3,
            hold_foreground_cpu_workers: 2,
            hold_background_tasks: 1,
            background_in_flight_bytes: 512,
            queued_work_items: 3,
            queued_bytes: 1_024,
            flush_after_write: false,
        })
        .await
        .expect("run helper load");

    let after_burst = state
        .observability_report()
        .await
        .expect("observe burst pressure");
    assert!(
        after_burst
            .primary_writer
            .current_pressure
            .local
            .mutable_dirty_bytes
            > 0
    );
    assert_eq!(
        after_burst
            .primary_writer
            .current_pressure
            .local
            .immutable_queued_bytes,
        0
    );
    assert_eq!(
        after_burst
            .primary_writer
            .current_pressure
            .local
            .immutable_flushing_bytes,
        0
    );
    assert!(
        after_burst
            .primary_writer
            .current_pressure
            .local
            .unified_log_pinned_bytes
            > 0
    );
    assert!(after_burst.primary_writer.throttle_active);
    assert!(
        after_burst
            .helper_writer
            .current_pressure
            .local
            .mutable_dirty_bytes
            > 0,
        "helper load should leave helper-side dirty bytes visible before helper flushing runs"
    );

    state.primary_db().__failpoint_registry().arm_error(
        terracedb::failpoints::names::DB_FLUSH_INPUTS_MARKED_FLUSHING,
        terracedb::StorageError::timeout("pause primary flush after inputs are marked"),
        FailpointMode::Once,
    );
    state
        .primary_db()
        .flush()
        .await
        .expect_err("failpoint should bounce primary bytes back to queued");

    let queued = state
        .observability_report()
        .await
        .expect("observe queued pressure");
    assert_eq!(
        queued
            .primary_writer
            .current_pressure
            .local
            .mutable_dirty_bytes,
        0
    );
    assert!(
        queued
            .primary_writer
            .current_pressure
            .local
            .immutable_queued_bytes
            > 0
    );
    assert_eq!(
        queued
            .primary_writer
            .current_pressure
            .local
            .immutable_flushing_bytes,
        0
    );

    let blocker = state.primary_db().__failpoint_registry().arm_pause(
        terracedb::failpoints::names::DB_FLUSH_INPUTS_MARKED_FLUSHING,
        FailpointMode::Once,
    );
    let primary_db = state.primary_db().clone();
    let flush_task = tokio::spawn(async move { primary_db.flush().await });
    blocker.wait_until_hit().await;

    let flushing = state
        .observability_report()
        .await
        .expect("observe flushing pressure");
    assert_eq!(
        flushing
            .primary_writer
            .current_pressure
            .local
            .mutable_dirty_bytes,
        0
    );
    assert_eq!(
        flushing
            .primary_writer
            .current_pressure
            .local
            .immutable_queued_bytes,
        0
    );
    assert!(
        flushing
            .primary_writer
            .current_pressure
            .local
            .immutable_flushing_bytes
            > 0
    );
    assert!(flushing.primary_writer.throttle_active);

    blocker.release();
    flush_task
        .await
        .expect("join flush task")
        .expect("flush after failpoint release");
    state
        .release_helper_pressure()
        .expect("release helper pressure");

    let recovered = state
        .observability_report()
        .await
        .expect("observe recovered pressure");
    assert_eq!(
        recovered
            .primary_writer
            .current_pressure
            .local
            .mutable_dirty_bytes,
        0
    );
    assert_eq!(
        recovered
            .primary_writer
            .current_pressure
            .local
            .immutable_queued_bytes,
        0
    );
    assert_eq!(
        recovered
            .primary_writer
            .current_pressure
            .local
            .immutable_flushing_bytes,
        0
    );
    assert_eq!(
        recovered
            .primary_writer
            .current_pressure
            .local
            .unified_log_pinned_bytes,
        0
    );
    assert!(!recovered.primary_writer.throttle_active);
    assert!(!recovered.primary_writer.stall_active);
}
