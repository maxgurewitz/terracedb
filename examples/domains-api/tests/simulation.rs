use std::{sync::Arc, time::Duration};

use terracedb::{
    AdmissionPressureLevel, DbComponents, DeterministicRng, SimulatedFileSystem,
    SimulatedObjectStore,
};
use terracedb_example_domains_api::{
    ANALYTICS_DATABASE_NAME, BackgroundMaintenanceRequest, ControlPlaneTableRequest, DomainsApp,
    DomainsExampleProfile, ExampleDatabase, HelperLoadRequest, PRIMARY_DATABASE_NAME,
    PrimaryBurstRequest, domains_db_settings,
};
use terracedb_simulation::{SeededSimulationRunner, TurmoilClock};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SimulationCapture {
    primary_titles: Vec<String>,
    helper_report_ids: Vec<String>,
    control_domain_reserved: bool,
    primary_pressure_level: AdmissionPressureLevel,
    primary_throttle_active: bool,
    helper_dirty_bytes: u64,
}

fn run_seeded_example(
    profile: DomainsExampleProfile,
    seed: u64,
) -> turmoil::Result<SimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_secs(5))
        .run_with(move |_context| async move {
            let deployment = profile.deployment().expect("deployment");
            let file_system = Arc::new(SimulatedFileSystem::default());
            let object_store = Arc::new(SimulatedObjectStore::default());
            let clock = Arc::new(TurmoilClock);
            let primary = deployment
                .open_database(
                    PRIMARY_DATABASE_NAME,
                    domains_db_settings("/domains-sim/primary", "domains/sim/primary"),
                    DbComponents::new(
                        file_system.clone(),
                        object_store.clone(),
                        clock.clone(),
                        Arc::new(DeterministicRng::seeded(seed)),
                    ),
                )
                .await
                .expect("open primary");
            let analytics = deployment
                .open_database(
                    ANALYTICS_DATABASE_NAME,
                    domains_db_settings("/domains-sim/analytics", "domains/sim/analytics"),
                    DbComponents::new(
                        file_system,
                        object_store,
                        clock,
                        Arc::new(DeterministicRng::seeded(seed.wrapping_add(1))),
                    ),
                )
                .await
                .expect("open analytics");

            let app = DomainsApp::open(deployment, primary, analytics, profile)
                .await
                .expect("open app");
            let state = app.state();
            let primary_background = state
                .primary_db()
                .execution_profile()
                .background
                .domain
                .clone();
            let helper_background = state
                .analytics_db()
                .execution_profile()
                .background
                .domain
                .clone();
            let mut resource_updates = state.primary_db().subscribe_resource_manager();

            state
                .run_primary_burst(PrimaryBurstRequest {
                    batch_id: "sim-burst".to_string(),
                    item_count: 2,
                    title_bytes: 768,
                })
                .await
                .expect("run primary burst");
            state
                .apply_primary_maintenance(BackgroundMaintenanceRequest {
                    flush_now: false,
                    hold_background_tasks: 1,
                    background_in_flight_bytes: 128,
                    queued_work_items: 5,
                    queued_bytes: 1024,
                })
                .await
                .expect("apply primary maintenance");
            state
                .run_helper_load(HelperLoadRequest {
                    batch_id: "batch-sim".to_string(),
                    report_count: 3,
                    hold_foreground_cpu_workers: 2,
                    hold_background_tasks: 1,
                    background_in_flight_bytes: 256,
                    queued_work_items: 6,
                    queued_bytes: 2048,
                    flush_after_write: false,
                })
                .await
                .expect("run helper load");
            let _ = resource_updates
                .wait_for(|snapshot| {
                    let primary_background = &snapshot.domains[&primary_background];
                    let helper_background = &snapshot.domains[&helper_background];
                    primary_background.backlog.queued_bytes >= 1_024
                        && helper_background.backlog.queued_bytes >= 2_048
                })
                .await
                .expect("resource publication");
            state
                .ensure_control_plane_table(ControlPlaneTableRequest {
                    database: ExampleDatabase::Primary,
                    table_name: "audit_log".to_string(),
                })
                .await
                .expect("create control-plane table");

            let report = state.observability_report().await.expect("observe domains");
            let primary_items = state.list_primary_items().await.expect("list primary");
            let helper_reports = state.list_helper_reports().await.expect("list helper");
            assert!(state.primary_db().try_table("audit_log").is_some());

            let control_domain = &report.deployment.databases[PRIMARY_DATABASE_NAME]
                .control_plane
                .binding
                .domain;
            let capture = SimulationCapture {
                primary_titles: primary_items.into_iter().map(|item| item.title).collect(),
                helper_report_ids: helper_reports
                    .into_iter()
                    .map(|report| report.report_id)
                    .collect(),
                control_domain_reserved: report.deployment.domain_topology[control_domain]
                    .spec
                    .metadata
                    .get("reserved")
                    .map(String::as_str)
                    == Some("true"),
                primary_pressure_level: report.primary_writer.next_write_admission.level,
                primary_throttle_active: report.primary_writer.throttle_active,
                helper_dirty_bytes: report
                    .helper_writer
                    .current_pressure
                    .local
                    .mutable_dirty_bytes,
            };

            app.shutdown().await.expect("shutdown app");
            Ok(capture)
        })
}

#[test]
fn domains_api_simulation_preserves_primary_answers_while_pressure_profiles_change_behavior()
-> turmoil::Result {
    let conservative_a = run_seeded_example(DomainsExampleProfile::Conservative, 0x7070)?;
    let conservative_b = run_seeded_example(DomainsExampleProfile::Conservative, 0x7070)?;
    let protected = run_seeded_example(DomainsExampleProfile::PrimaryProtected, 0x7070)?;

    assert_eq!(conservative_a, conservative_b);
    assert_eq!(conservative_a.primary_titles, protected.primary_titles);
    assert_eq!(
        conservative_a.helper_report_ids,
        protected.helper_report_ids
    );
    assert_eq!(
        conservative_a.primary_titles.len(),
        2,
        "bursty primary writes should stay logically correct under both profiles"
    );
    assert_eq!(conservative_a.helper_report_ids.len(), 3);
    assert!(conservative_a.control_domain_reserved);
    assert!(protected.control_domain_reserved);
    assert!(conservative_a.helper_dirty_bytes > 0);
    assert!(protected.helper_dirty_bytes > 0);
    assert!(
        protected.primary_pressure_level >= conservative_a.primary_pressure_level,
        "the primary-protected profile should throttle or stall no later than conservative"
    );
    assert!(protected.primary_throttle_active);
    Ok(())
}
