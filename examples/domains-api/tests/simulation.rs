use std::{sync::Arc, time::Duration};

use terracedb::{DeterministicRng, SimulatedFileSystem, SimulatedObjectStore};
use terracedb_example_domains_api::{
    ANALYTICS_DATABASE_NAME, AdmissionProbeRequest, BackgroundMaintenanceRequest,
    ControlPlaneTableRequest, CreatePrimaryItemRequest, DomainsApp, DomainsExampleProfile,
    ExampleDatabase, ExampleLane, HelperLoadRequest, PRIMARY_DATABASE_NAME, ProbeUsage,
    domains_db_builder,
};
use terracedb_simulation::{SeededSimulationRunner, TurmoilClock};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SimulationCapture {
    primary_titles: Vec<String>,
    helper_report_ids: Vec<String>,
    control_domain_reserved: bool,
    primary_foreground_cpu_budget: Option<u32>,
}

fn run_seeded_example(seed: u64) -> turmoil::Result<SimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |_context| async move {
            let deployment = DomainsExampleProfile::Conservative
                .deployment()
                .expect("deployment");
            let file_system = Arc::new(SimulatedFileSystem::default());
            let object_store = Arc::new(SimulatedObjectStore::default());
            let clock = Arc::new(TurmoilClock);

            let primary = domains_db_builder("/domains-sim/primary", "domains/sim/primary")
                .file_system(file_system.clone())
                .object_store(object_store.clone())
                .clock(clock.clone())
                .rng(Arc::new(DeterministicRng::seeded(seed)))
                .colocated_database(&deployment, PRIMARY_DATABASE_NAME)
                .expect("bind primary")
                .open()
                .await
                .expect("open primary");
            let analytics = domains_db_builder("/domains-sim/analytics", "domains/sim/analytics")
                .file_system(file_system)
                .object_store(object_store)
                .clock(clock)
                .rng(Arc::new(DeterministicRng::seeded(seed.wrapping_add(1))))
                .colocated_database(&deployment, ANALYTICS_DATABASE_NAME)
                .expect("bind analytics")
                .open()
                .await
                .expect("open analytics");

            let app = DomainsApp::open(primary, analytics, DomainsExampleProfile::Conservative)
                .await
                .expect("open app");
            let state = app.state();

            state
                .create_primary_item(CreatePrimaryItemRequest {
                    item_id: "ticket-1".to_string(),
                    title: "Keep primary reads correct".to_string(),
                })
                .await
                .expect("create primary item");
            state
                .apply_primary_maintenance(BackgroundMaintenanceRequest {
                    flush_now: true,
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
                    report_count: 5,
                    hold_foreground_cpu_workers: 2,
                    hold_background_tasks: 1,
                    background_in_flight_bytes: 256,
                    queued_work_items: 6,
                    queued_bytes: 2048,
                    flush_after_write: true,
                })
                .await
                .expect("run helper load");
            state
                .ensure_control_plane_table(ControlPlaneTableRequest {
                    database: ExampleDatabase::Primary,
                    table_name: "audit_log".to_string(),
                })
                .await
                .expect("create control-plane table");

            let probe = state
                .probe_admission(AdmissionProbeRequest {
                    database: ExampleDatabase::Primary,
                    lane: ExampleLane::Foreground,
                    usage: ProbeUsage {
                        cpu_workers: 4,
                        ..ProbeUsage::default()
                    },
                })
                .expect("probe primary foreground");
            assert!(probe.decision.admitted);

            let report = state.observability_report().expect("observe domains");
            let primary_items = state.list_primary_items().await.expect("list primary");
            let helper_reports = state.list_helper_reports().await.expect("list helper");
            assert!(state.primary_db().try_table("audit_log").is_some());

            let control_domain = &report.primary.control_plane.binding.domain;
            let capture = SimulationCapture {
                primary_titles: primary_items.into_iter().map(|item| item.title).collect(),
                helper_report_ids: helper_reports
                    .into_iter()
                    .map(|report| report.report_id)
                    .collect(),
                control_domain_reserved: report.resource_manager.domains[control_domain]
                    .spec
                    .metadata
                    .get("reserved")
                    .map(String::as_str)
                    == Some("true"),
                primary_foreground_cpu_budget: probe.decision.effective_budget.cpu.worker_slots,
            };

            app.shutdown().await.expect("shutdown app");
            Ok(capture)
        })
}

#[test]
fn domains_api_simulation_protects_primary_and_control_plane_under_helper_pressure()
-> turmoil::Result {
    let first = run_seeded_example(0x7070)?;
    let second = run_seeded_example(0x7070)?;

    assert_eq!(first, second);
    assert_eq!(
        first.primary_titles,
        vec!["Keep primary reads correct".to_string()]
    );
    assert_eq!(first.helper_report_ids.len(), 5);
    assert!(first.control_domain_reserved);
    assert_eq!(first.primary_foreground_cpu_budget, Some(4));
    Ok(())
}
