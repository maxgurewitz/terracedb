use std::{collections::BTreeMap, sync::Arc};

use futures::StreamExt;
use parking_lot::Mutex;
use terracedb::{
    ColocatedDatabasePlacement, ColocatedDeployment, ColocatedSubsystemPlacement, CommitOptions,
    CompactionStrategy, Db, DbComponents, DbExecutionProfile, DbSettings, DomainBackgroundBudget,
    DomainCpuBudget, DomainIoBudget, DomainMemoryBudget, DomainTaggedWork, DurabilityClass,
    ExecutionDomainBacklogSnapshot, ExecutionDomainBudget, ExecutionDomainLifecycleEvent,
    ExecutionDomainLifecycleHook, ExecutionDomainOwner, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionDomainSpec, ExecutionLane, ExecutionLaneBinding,
    ExecutionLanePlacementConfig, ExecutionResourceKind, ExecutionResourceUsage,
    InMemoryResourceManager, NoopScheduler, PendingWork, PendingWorkType, PressureScope,
    ResourceManager, S3Location, ScanOptions, Scheduler, ShardReadyPlacementLayout, StubClock,
    StubFileSystem, StubObjectStore, StubRng, TableConfig, TableFormat, TableStats,
    ThrottleDecision, Value,
};
use terracedb_simulation::{
    ColocatedDbWorkloadSpec, ContentionClass, DomainBudgetCharge, DomainBudgetOracle,
    InMemoryDomainBudgetOracle,
};

fn tiered_settings(path: &str) -> DbSettings {
    DbSettings::tiered(
        path,
        S3Location {
            bucket: "terracedb-execution-tests".to_string(),
            prefix: "phase13".to_string(),
        },
    )
}

fn row_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Row,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(10),
        history_retention_sequences: None,
        compaction_strategy: CompactionStrategy::Leveled,
        schema: None,
        metadata: BTreeMap::new(),
    }
}

fn execution_profile(prefix: &str) -> DbExecutionProfile {
    DbExecutionProfile::default()
        .with_foreground(ExecutionLaneBinding::new(
            ExecutionDomainPath::new(["process", prefix, "foreground"]),
            DurabilityClass::UserData,
        ))
        .with_background(ExecutionLaneBinding::new(
            ExecutionDomainPath::new(["process", prefix, "background"]),
            DurabilityClass::UserData,
        ))
        .with_control_plane(ExecutionLaneBinding::new(
            ExecutionDomainPath::new(["process", prefix, "control"]),
            DurabilityClass::ControlPlane,
        ))
}

fn stub_components(resource_manager: Arc<dyn ResourceManager>) -> DbComponents {
    stub_components_with_scheduler(resource_manager, Arc::new(NoopScheduler))
}

fn stub_components_with_scheduler(
    resource_manager: Arc<dyn ResourceManager>,
    scheduler: Arc<dyn Scheduler>,
) -> DbComponents {
    DbComponents::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(17)),
    )
    .with_scheduler(scheduler)
    .with_resource_manager(resource_manager)
}

fn shared_stub_components(
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> DbComponents {
    DbComponents::new(
        file_system,
        object_store,
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(17)),
    )
    .with_scheduler(Arc::new(NoopScheduler))
}

fn process_budget() -> ExecutionDomainBudget {
    ExecutionDomainBudget {
        cpu: DomainCpuBudget {
            worker_slots: Some(6),
            weight: None,
        },
        memory: DomainMemoryBudget {
            total_bytes: Some(1024),
            cache_bytes: Some(768),
            mutable_bytes: Some(768),
        },
        io: DomainIoBudget {
            local_concurrency: Some(4),
            local_bytes_per_second: Some(4096),
            remote_concurrency: Some(3),
            remote_bytes_per_second: Some(3072),
        },
        background: DomainBackgroundBudget {
            task_slots: Some(4),
            max_in_flight_bytes: Some(2048),
        },
    }
}

fn shared_spec(
    path: ExecutionDomainPath,
    owner: ExecutionDomainOwner,
    weight: u32,
    budget: ExecutionDomainBudget,
) -> ExecutionDomainSpec {
    ExecutionDomainSpec {
        path,
        owner,
        budget,
        placement: ExecutionDomainPlacement::SharedWeighted { weight },
        metadata: BTreeMap::new(),
    }
}

fn dedicated_spec(
    path: ExecutionDomainPath,
    owner: ExecutionDomainOwner,
    budget: ExecutionDomainBudget,
) -> ExecutionDomainSpec {
    ExecutionDomainSpec {
        path,
        owner,
        budget,
        placement: ExecutionDomainPlacement::Dedicated,
        metadata: BTreeMap::new(),
    }
}

struct RecordingLifecycleHook {
    events: Mutex<Vec<ExecutionDomainLifecycleEvent>>,
}

impl RecordingLifecycleHook {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    fn events(&self) -> Vec<ExecutionDomainLifecycleEvent> {
        self.events.lock().clone()
    }
}

impl ExecutionDomainLifecycleHook for RecordingLifecycleHook {
    fn on_event(
        &self,
        _snapshot: &terracedb::ExecutionDomainSnapshot,
        event: ExecutionDomainLifecycleEvent,
    ) {
        self.events.lock().push(event);
    }
}

#[derive(Debug)]
struct PressureThresholdScheduler {
    threshold_bytes: u64,
}

impl Scheduler for PressureThresholdScheduler {
    fn on_work_available(&self, _work: &[PendingWork]) -> Vec<terracedb::ScheduleDecision> {
        Vec::new()
    }

    fn should_throttle(&self, _table: &terracedb::Table, _stats: &TableStats) -> ThrottleDecision {
        ThrottleDecision::default()
    }

    fn admission_decision_in_domain(
        &self,
        _table: &terracedb::Table,
        _stats: &TableStats,
        signals: &terracedb::AdmissionSignals,
        _tag: &terracedb::WorkRuntimeTag,
        _domain_budget: Option<&ExecutionDomainBudget>,
    ) -> ThrottleDecision {
        let projected_bytes = signals
            .pressure
            .local
            .mutable_dirty_bytes
            .saturating_add(signals.batch_write_bytes);
        if projected_bytes >= self.threshold_bytes {
            return ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: None,
                stall: false,
            };
        }
        ThrottleDecision::default()
    }
}

#[tokio::test]
async fn execution_contracts_compose_with_builder_scheduler_maintenance_and_simulation_seams() {
    let profile = execution_profile("compose");
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::new(
        ExecutionDomainBudget::default(),
    ));
    let hook = Arc::new(RecordingLifecycleHook::new());
    resource_manager.register_lifecycle_hook(hook.clone());
    resource_manager.register_domain(shared_spec(
        profile.background.domain.clone(),
        ExecutionDomainOwner::Database {
            name: "compose-db".to_string(),
        },
        2,
        ExecutionDomainBudget::default(),
    ));

    let db = Db::builder()
        .settings(tiered_settings("/execution-compose").with_execution_profile(profile.clone()))
        .components(stub_components(resource_manager.clone()))
        .open()
        .await
        .expect("open db with execution profile");

    assert_eq!(db.execution_profile(), &profile);
    assert!(Arc::ptr_eq(&db.resource_manager(), &resource_manager));
    assert!(db.pending_work().await.is_empty());
    assert!(db.pending_work_with_runtime_tags().await.is_empty());
    assert_eq!(
        hook.events(),
        vec![
            ExecutionDomainLifecycleEvent::Registered,
            ExecutionDomainLifecycleEvent::Registered,
            ExecutionDomainLifecycleEvent::Updated,
            ExecutionDomainLifecycleEvent::Registered,
        ]
    );

    let tagged: DomainTaggedWork<PendingWork> = db.tag_pending_work(PendingWork {
        id: "compaction:1".to_string(),
        work_type: PendingWorkType::Compaction,
        table: "events".to_string(),
        level: Some(1),
        estimated_bytes: 64,
    });
    assert_eq!(tagged.tag.domain, profile.background.domain);
    assert_eq!(tagged.tag.durability_class, DurabilityClass::UserData);

    let snapshot = db.resource_manager_snapshot();
    assert!(
        snapshot
            .invariants
            .contains(terracedb::ExecutionDomainInvariant::DomainMovementPreservesLogicalOutcome)
    );
    assert_eq!(snapshot.domains.len(), 5);
    assert!(
        snapshot
            .domains
            .contains_key(&ExecutionDomainPath::root("process"))
    );
    assert!(
        snapshot
            .domains
            .contains_key(&ExecutionDomainPath::new(["process", "compose"]))
    );
    assert!(snapshot.domains.contains_key(&profile.foreground.domain));
    assert!(snapshot.domains.contains_key(&profile.background.domain));
    assert!(snapshot.domains.contains_key(&profile.control_plane.domain));

    let colocated = ColocatedDbWorkloadSpec {
        db_name: "compose-db".to_string(),
        execution_profile: profile,
        operation_count: 3,
        metadata: BTreeMap::from([("kind".to_string(), "compile-only".to_string())]),
    };
    assert_eq!(colocated.operation_count, 3);
}

#[tokio::test]
async fn fake_runtimes_can_tag_work_and_account_domain_budgets_hierarchically() {
    let profile = execution_profile("budget");
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());
    let oracle = InMemoryDomainBudgetOracle::default();

    let db = Db::builder()
        .settings(tiered_settings("/execution-budget").with_execution_profile(profile.clone()))
        .components(stub_components(resource_manager))
        .open()
        .await
        .expect("open db with fake runtimes");

    let user_work = db.tag_pending_work(PendingWork {
        id: "flush:1".to_string(),
        work_type: PendingWorkType::Flush,
        table: "events".to_string(),
        level: None,
        estimated_bytes: 256,
    });
    let control_work = db.tag_control_plane_work("catalog-sync");

    oracle.record(
        &user_work.tag,
        DomainBudgetCharge {
            cpu_millis: 5,
            memory_bytes: 128,
            local_io_bytes: 512,
            remote_io_bytes: 0,
            background_tasks: 1,
        },
    );
    oracle.record(
        &control_work.tag,
        DomainBudgetCharge {
            cpu_millis: 2,
            memory_bytes: 32,
            local_io_bytes: 0,
            remote_io_bytes: 64,
            background_tasks: 1,
        },
    );

    let snapshot = oracle.snapshot();
    let user_budget = snapshot
        .get(&profile.background.domain)
        .expect("user background domain should be tracked");
    assert_eq!(user_budget.total.cpu_millis, 5);
    assert_eq!(
        user_budget.by_contention_class[&ContentionClass::UserData].local_io_bytes,
        512
    );

    let control_budget = snapshot
        .get(&profile.control_plane.domain)
        .expect("control-plane domain should be tracked");
    assert_eq!(
        control_budget.by_contention_class[&ContentionClass::ControlPlane].remote_io_bytes,
        64
    );

    let process_budget = snapshot
        .get(&ExecutionDomainPath::root("process"))
        .expect("root process domain should aggregate descendants");
    assert_eq!(process_budget.total.cpu_millis, 7);
    assert_eq!(process_budget.total.remote_io_bytes, 64);
}

#[tokio::test]
async fn pressure_contracts_compose_with_builder_scheduler_and_public_db_apis() {
    let profile = execution_profile("pressure-compose");
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());

    let db = Db::builder()
        .settings(
            tiered_settings("/execution-pressure-compose").with_execution_profile(profile.clone()),
        )
        .components(stub_components(resource_manager))
        .open()
        .await
        .expect("open db with pressure contracts");

    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create pressure table");
    table
        .write(b"user:1".to_vec(), Value::bytes("v1"))
        .await
        .expect("write pressure seed");

    let process = db.process_pressure_stats().await;
    assert!(matches!(process.scope, PressureScope::Process));
    assert!(process.local.mutable_dirty_bytes > 0);
    assert_eq!(process.process_total, Some(process.local));

    let domain = db.domain_pressure_stats(&profile.foreground.domain).await;
    assert!(matches!(domain.scope, PressureScope::Domain(_)));
    assert_eq!(
        domain.local.mutable_dirty_bytes,
        process.local.mutable_dirty_bytes
    );

    let table_pressure = db.table_pressure_stats(&table).await;
    match &table_pressure.scope {
        PressureScope::Table(name) => assert_eq!(name, "events"),
        other => panic!("expected table pressure scope, got {other:?}"),
    }
    assert!(table_pressure.local.mutable_dirty_bytes > 0);
    assert_eq!(table_pressure.process_total, Some(process.local));

    let signals = db.write_admission_signals(&table, 64).await;
    assert_eq!(signals.table, "events");
    assert_eq!(signals.runtime_tag.domain, profile.foreground.domain);
    assert_eq!(
        signals.pressure.local.mutable_dirty_bytes,
        table_pressure.local.mutable_dirty_bytes
    );
    assert_eq!(
        signals.domain_budget,
        db.resource_manager_snapshot()
            .domains
            .get(&profile.foreground.domain)
            .map(|snapshot| snapshot.spec.budget)
    );

    let flush_candidates = db.pending_flush_pressure_candidates().await;
    assert!(flush_candidates.is_empty());
    assert!(
        NoopScheduler
            .on_flush_pressure_available(&flush_candidates)
            .is_empty()
    );
}

#[tokio::test]
async fn pressure_stats_reconstruct_deterministically_after_reopen() {
    let profile = execution_profile("pressure-reopen");
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());
    let config =
        tiered_settings("/execution-pressure-reopen").with_execution_profile(profile.clone());

    let open_components = || {
        DbComponents::new(
            file_system.clone(),
            object_store.clone(),
            Arc::new(StubClock::default()),
            Arc::new(StubRng::seeded(17)),
        )
        .with_scheduler(Arc::new(NoopScheduler))
        .with_resource_manager(resource_manager.clone())
    };

    let db = Db::builder()
        .settings(config.clone())
        .components(open_components())
        .open()
        .await
        .expect("open initial pressure db");
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create pressure table");
    table
        .write(b"user:1".to_vec(), Value::bytes("v1"))
        .await
        .expect("write unflushed value");

    let before_pressure = db.table_pressure_stats(&table).await;
    let before_signals = db.write_admission_signals(&table, 128).await;
    drop(db);

    let reopened = Db::builder()
        .settings(config)
        .components(open_components())
        .open()
        .await
        .expect("reopen pressure db");
    let reopened_table = reopened.table("events");
    let after_pressure = reopened.table_pressure_stats(&reopened_table).await;
    let after_signals = reopened.write_admission_signals(&reopened_table, 128).await;

    assert_eq!(before_pressure, after_pressure);
    assert_eq!(before_signals.pressure, after_signals.pressure);
    assert_eq!(before_signals.domain_budget, after_signals.domain_budget);
}

#[tokio::test]
async fn pressure_thresholds_change_admission_observability_not_logical_results() {
    async fn run_with_threshold(
        path: &str,
        threshold_bytes: u64,
    ) -> (Vec<(Vec<u8>, Vec<u8>)>, u64) {
        let scheduler: Arc<dyn Scheduler> =
            Arc::new(PressureThresholdScheduler { threshold_bytes });
        let resource_manager: Arc<dyn ResourceManager> =
            Arc::new(InMemoryResourceManager::default());
        let db = Db::builder()
            .settings(tiered_settings(path))
            .components(stub_components_with_scheduler(resource_manager, scheduler))
            .open()
            .await
            .expect("open threshold db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create threshold table");

        for index in 0..3 {
            table
                .write(
                    format!("k{index}").into_bytes(),
                    Value::bytes(format!("v{index}")),
                )
                .await
                .expect("commit threshold write");
        }

        let throttled = db
            .scheduler_observability_snapshot()
            .throttled_writes_by_domain
            .values()
            .copied()
            .sum::<u64>();
        db.flush().await.expect("flush threshold workload");
        (read_existing_rows(&db).await, throttled)
    }

    let (aggressive_rows, aggressive_throttled) =
        run_with_threshold("/execution-pressure-aggressive", 1).await;
    let (relaxed_rows, relaxed_throttled) =
        run_with_threshold("/execution-pressure-relaxed", u64::MAX).await;

    assert_eq!(aggressive_rows, relaxed_rows);
    assert!(aggressive_throttled > relaxed_throttled);
}

#[test]
fn per_domain_budgets_are_enforced_for_cpu_memory_io_and_background_limits() {
    let manager = InMemoryResourceManager::new(process_budget());
    let background = ExecutionDomainPath::new(["process", "db-a", "background"]);
    manager.register_domain(shared_spec(
        background.clone(),
        ExecutionDomainOwner::Database {
            name: "db-a".to_string(),
        },
        1,
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(2),
                weight: None,
            },
            memory: DomainMemoryBudget {
                total_bytes: Some(256),
                cache_bytes: Some(128),
                mutable_bytes: Some(128),
            },
            io: DomainIoBudget {
                local_concurrency: Some(1),
                local_bytes_per_second: Some(1024),
                remote_concurrency: Some(1),
                remote_bytes_per_second: Some(1024),
            },
            background: DomainBackgroundBudget {
                task_slots: Some(1),
                max_in_flight_bytes: Some(512),
            },
        },
    ));

    let admitted = manager.try_acquire(
        &background,
        ExecutionResourceUsage {
            cpu_workers: 2,
            memory_bytes: 200,
            cache_bytes: 96,
            mutable_bytes: 96,
            local_io_concurrency: 1,
            local_io_bytes_per_second: 1024,
            remote_io_concurrency: 1,
            remote_io_bytes_per_second: 768,
            background_tasks: 1,
            background_in_flight_bytes: 256,
        },
    );
    assert!(admitted.admitted);
    assert_eq!(admitted.snapshot.usage.cpu_workers_in_use, 2);
    assert_eq!(admitted.snapshot.usage.memory_bytes, 200);
    assert_eq!(admitted.snapshot.usage.local_io_in_flight, 1);
    assert_eq!(admitted.snapshot.usage.background_tasks, 1);

    let blocked = manager.try_acquire(
        &background,
        ExecutionResourceUsage {
            cpu_workers: 1,
            memory_bytes: 80,
            cache_bytes: 40,
            mutable_bytes: 40,
            local_io_concurrency: 1,
            local_io_bytes_per_second: 128,
            remote_io_concurrency: 1,
            remote_io_bytes_per_second: 512,
            background_tasks: 1,
            background_in_flight_bytes: 300,
        },
    );
    assert!(!blocked.admitted);
    assert!(
        blocked
            .blocked_by
            .contains(&ExecutionResourceKind::CpuWorkers)
    );
    assert!(
        blocked
            .blocked_by
            .contains(&ExecutionResourceKind::MemoryBytes)
    );
    assert!(
        blocked
            .blocked_by
            .contains(&ExecutionResourceKind::LocalIoConcurrency)
    );
    assert!(
        blocked
            .blocked_by
            .contains(&ExecutionResourceKind::BackgroundTasks)
    );
    assert_eq!(blocked.snapshot.contention.blocked_requests, 1);
}

#[test]
fn shared_domains_can_borrow_idle_capacity_but_not_reserved_or_busy_capacity() {
    let manager = InMemoryResourceManager::new(process_budget());
    let control = ExecutionDomainPath::new(["process", "control"]);
    let user_a = ExecutionDomainPath::new(["process", "tenant-a", "foreground"]);
    let user_b = ExecutionDomainPath::new(["process", "tenant-b", "foreground"]);

    manager.register_domain(dedicated_spec(
        control,
        ExecutionDomainOwner::ProcessControl,
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(2),
                weight: None,
            },
            memory: DomainMemoryBudget::default(),
            io: DomainIoBudget::default(),
            background: DomainBackgroundBudget::default(),
        },
    ));
    manager.register_domain(shared_spec(
        user_a.clone(),
        ExecutionDomainOwner::Database {
            name: "tenant-a".to_string(),
        },
        1,
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(6),
                weight: None,
            },
            memory: DomainMemoryBudget::default(),
            io: DomainIoBudget::default(),
            background: DomainBackgroundBudget::default(),
        },
    ));
    manager.register_domain(shared_spec(
        user_b.clone(),
        ExecutionDomainOwner::Database {
            name: "tenant-b".to_string(),
        },
        1,
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(6),
                weight: None,
            },
            memory: DomainMemoryBudget::default(),
            io: DomainIoBudget::default(),
            background: DomainBackgroundBudget::default(),
        },
    ));

    let borrowed = manager.try_acquire(
        &user_a,
        ExecutionResourceUsage {
            cpu_workers: 4,
            ..ExecutionResourceUsage::default()
        },
    );
    assert!(borrowed.admitted);
    assert!(borrowed.borrowed_shared_capacity);
    assert_eq!(borrowed.snapshot.effective_budget.cpu.worker_slots, Some(4));

    manager.release(
        &user_a,
        ExecutionResourceUsage {
            cpu_workers: 2,
            ..ExecutionResourceUsage::default()
        },
    );
    manager.set_backlog(
        &user_b,
        ExecutionDomainBacklogSnapshot {
            queued_work_items: 1,
            queued_bytes: 64,
        },
    );

    let peer_share = manager.try_acquire(
        &user_b,
        ExecutionResourceUsage {
            cpu_workers: 2,
            ..ExecutionResourceUsage::default()
        },
    );
    assert!(peer_share.admitted);
    assert!(!peer_share.borrowed_shared_capacity);
    assert_eq!(
        peer_share.snapshot.effective_budget.cpu.worker_slots,
        Some(2)
    );

    let blocked = manager.try_acquire(
        &user_a,
        ExecutionResourceUsage {
            cpu_workers: 1,
            ..ExecutionResourceUsage::default()
        },
    );
    assert!(!blocked.admitted);
    assert_eq!(blocked.blocked_by, vec![ExecutionResourceKind::CpuWorkers]);
}

#[test]
fn update_domain_replaces_explicit_contracts_so_budgets_can_relax_or_change_shape() {
    let manager = InMemoryResourceManager::new(process_budget());
    let path = ExecutionDomainPath::new(["process", "tenant-a", "background"]);
    let owner = ExecutionDomainOwner::Database {
        name: "tenant-a".to_string(),
    };

    manager.register_domain(dedicated_spec(
        path.clone(),
        owner.clone(),
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(2),
                weight: None,
            },
            memory: DomainMemoryBudget::default(),
            io: DomainIoBudget::default(),
            background: DomainBackgroundBudget::default(),
        },
    ));

    let updated = manager.update_domain(shared_spec(
        path.clone(),
        owner,
        4,
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(5),
                weight: Some(4),
            },
            memory: DomainMemoryBudget::default(),
            io: DomainIoBudget::default(),
            background: DomainBackgroundBudget::default(),
        },
    ));

    assert_eq!(
        updated.spec.placement,
        ExecutionDomainPlacement::SharedWeighted { weight: 4 }
    );
    assert_eq!(updated.effective_budget.cpu.worker_slots, Some(5));

    let admitted = manager.try_acquire(
        &path,
        ExecutionResourceUsage {
            cpu_workers: 5,
            ..ExecutionResourceUsage::default()
        },
    );
    assert!(admitted.admitted);
    assert_eq!(admitted.snapshot.effective_budget.cpu.worker_slots, Some(5));
}

#[test]
fn shard_ready_layout_reserves_a_future_shard_namespace_without_claiming_physical_sharding() {
    let layout = ShardReadyPlacementLayout::new("warehouse");
    assert_eq!(
        layout.database_lane_path(ExecutionLane::UserForeground),
        ExecutionDomainPath::new(["process", "shards", "warehouse", "foreground"])
    );
    assert_eq!(
        layout.future_shard_namespace,
        ExecutionDomainPath::new(["process", "shards", "warehouse", "shards"])
    );
    assert_eq!(
        layout.future_shard_lane_path("0003", ExecutionLane::UserBackground),
        ExecutionDomainPath::new([
            "process",
            "shards",
            "warehouse",
            "shards",
            "0003",
            "background"
        ])
    );
    assert_eq!(
        layout.shard_owner("0003"),
        ExecutionDomainOwner::Shard {
            database: "warehouse".to_string(),
            shard: "0003".to_string(),
        }
    );

    let deployment =
        ColocatedDeployment::shard_ready(process_budget(), "warehouse").expect("shard-ready");
    let report = deployment.report();
    let background = report.databases["warehouse"]
        .background
        .snapshot
        .as_ref()
        .expect("background snapshot");
    assert_eq!(
        background
            .spec
            .metadata
            .get("terracedb.execution.layout")
            .map(String::as_str),
        Some("shard-ready")
    );
    assert_eq!(
        background
            .spec
            .metadata
            .get("terracedb.execution.future_shard_namespace")
            .map(String::as_str),
        Some("process/shards/warehouse/shards")
    );
    assert_eq!(
        background
            .spec
            .metadata
            .get("terracedb.execution.physical_sharding")
            .map(String::as_str),
        Some("not-enabled")
    );
}

#[test]
fn reserved_control_plane_progress_survives_shared_shard_background_pressure() {
    let process_budget = ExecutionDomainBudget {
        cpu: DomainCpuBudget {
            worker_slots: Some(4),
            weight: None,
        },
        memory: DomainMemoryBudget::default(),
        io: DomainIoBudget::default(),
        background: DomainBackgroundBudget {
            task_slots: Some(4),
            max_in_flight_bytes: None,
        },
    };
    let manager = InMemoryResourceManager::new(process_budget);
    let layout = ShardReadyPlacementLayout::new("warehouse");
    let control_plane = layout.database_lane_path(ExecutionLane::ControlPlane);
    let shard_a = layout.future_shard_lane_path("0001", ExecutionLane::UserBackground);
    let shard_b = layout.future_shard_lane_path("0002", ExecutionLane::UserBackground);

    manager.register_domain(dedicated_spec(
        control_plane.clone(),
        ExecutionDomainOwner::Subsystem {
            database: Some("warehouse".to_string()),
            name: "control-plane".to_string(),
        },
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(1),
                weight: None,
            },
            memory: DomainMemoryBudget::default(),
            io: DomainIoBudget::default(),
            background: DomainBackgroundBudget {
                task_slots: Some(1),
                max_in_flight_bytes: None,
            },
        },
    ));
    manager.register_domain(shared_spec(
        shard_a.clone(),
        layout.shard_owner("0001"),
        1,
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(4),
                weight: None,
            },
            memory: DomainMemoryBudget::default(),
            io: DomainIoBudget::default(),
            background: DomainBackgroundBudget {
                task_slots: Some(4),
                max_in_flight_bytes: None,
            },
        },
    ));
    manager.register_domain(shared_spec(
        shard_b.clone(),
        layout.shard_owner("0002"),
        1,
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(4),
                weight: None,
            },
            memory: DomainMemoryBudget::default(),
            io: DomainIoBudget::default(),
            background: DomainBackgroundBudget {
                task_slots: Some(4),
                max_in_flight_bytes: None,
            },
        },
    ));

    assert!(
        manager
            .try_acquire(
                &shard_a,
                ExecutionResourceUsage {
                    cpu_workers: 2,
                    background_tasks: 2,
                    ..ExecutionResourceUsage::default()
                },
            )
            .admitted
    );
    assert!(
        manager
            .try_acquire(
                &shard_b,
                ExecutionResourceUsage {
                    cpu_workers: 1,
                    background_tasks: 1,
                    ..ExecutionResourceUsage::default()
                },
            )
            .admitted
    );

    let control_progress = manager.try_acquire(
        &control_plane,
        ExecutionResourceUsage {
            cpu_workers: 1,
            background_tasks: 1,
            ..ExecutionResourceUsage::default()
        },
    );
    assert!(control_progress.admitted);

    let blocked_shared = manager.try_acquire(
        &shard_a,
        ExecutionResourceUsage {
            cpu_workers: 1,
            background_tasks: 1,
            ..ExecutionResourceUsage::default()
        },
    );
    assert!(!blocked_shared.admitted);
    assert!(
        blocked_shared
            .blocked_by
            .contains(&ExecutionResourceKind::CpuWorkers)
    );
    assert!(
        blocked_shared
            .blocked_by
            .contains(&ExecutionResourceKind::BackgroundTasks)
    );
}

#[tokio::test]
async fn db_open_registers_a_reserved_control_plane_domain_when_missing() {
    let profile = execution_profile("reserved");
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());

    let db = Db::builder()
        .settings(tiered_settings("/execution-reserved").with_execution_profile(profile.clone()))
        .components(stub_components(resource_manager))
        .open()
        .await
        .expect("open db with reserved control-plane domain");

    let snapshot = db.resource_manager_snapshot();
    let control = snapshot
        .domains
        .get(&profile.control_plane.domain)
        .expect("control-plane domain should be auto-registered");

    assert_eq!(
        control.spec.metadata.get("reserved").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        control
            .spec
            .metadata
            .get("durability_class")
            .map(String::as_str),
        Some("control-plane")
    );
    assert_eq!(control.spec.budget.cpu.worker_slots, Some(1));
    assert_eq!(control.spec.budget.io.remote_concurrency, Some(1));
    assert_eq!(control.spec.budget.background.task_slots, Some(1));
}

async fn run_logical_workload(path: &str, profile: DbExecutionProfile) -> Vec<(Vec<u8>, Vec<u8>)> {
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());
    let db = Db::builder()
        .settings(tiered_settings(path).with_execution_profile(profile))
        .components(stub_components(resource_manager))
        .open()
        .await
        .expect("open db for invariant test");
    run_workload(&db).await
}

async fn run_workload(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");

    let mut batch = db.write_batch();
    batch.put(&table, b"a".to_vec(), Value::bytes("v1"));
    batch.put(&table, b"b".to_vec(), Value::bytes("v2"));
    batch.delete(&table, b"b".to_vec());
    batch.put(&table, b"c".to_vec(), Value::bytes("v3"));
    db.commit(batch, CommitOptions::default())
        .await
        .expect("commit deterministic workload");

    let snapshot = db.snapshot().await;
    let rows = snapshot
        .scan(&table, b"a".to_vec(), b"z".to_vec(), ScanOptions::default())
        .await
        .expect("scan logical result")
        .map(|(key, value)| match value {
            Value::Bytes(bytes) => (key, bytes),
            Value::Record(_) => panic!("invariant test only uses byte values"),
        })
        .collect::<Vec<_>>()
        .await;
    snapshot.release();
    rows
}

async fn read_existing_rows(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let table = db.table("events");
    let snapshot = db.snapshot().await;
    let rows = snapshot
        .scan(&table, b"a".to_vec(), b"z".to_vec(), ScanOptions::default())
        .await
        .expect("scan recovered logical result")
        .map(|(key, value)| match value {
            Value::Bytes(bytes) => (key, bytes),
            Value::Record(_) => panic!("invariant test only uses byte values"),
        })
        .collect::<Vec<_>>()
        .await;
    snapshot.release();
    rows
}

async fn open_deployed_db(
    deployment: &ColocatedDeployment,
    database: &str,
    path: &str,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> Db {
    Db::builder()
        .settings(tiered_settings(path))
        .components(shared_stub_components(file_system, object_store))
        .colocated_database(deployment, database)
        .expect("bind builder to colocated deployment")
        .open()
        .await
        .expect("open deployed db")
}

#[tokio::test]
async fn colocated_databases_can_share_one_resource_manager_with_distinct_domain_assignments() {
    let manager: Arc<dyn ResourceManager> =
        Arc::new(InMemoryResourceManager::new(process_budget()));
    let left_profile = execution_profile("colo-left");
    let right_profile = execution_profile("colo-right");

    let left = Db::builder()
        .settings(
            tiered_settings("/execution-colocated-left")
                .with_execution_profile(left_profile.clone()),
        )
        .components(stub_components(manager.clone()))
        .open()
        .await
        .expect("open left colocated db");
    let right = Db::builder()
        .settings(
            tiered_settings("/execution-colocated-right")
                .with_execution_profile(right_profile.clone()),
        )
        .components(stub_components(manager.clone()))
        .open()
        .await
        .expect("open right colocated db");

    let left_rows = run_workload(&left).await;
    let right_rows = run_workload(&right).await;
    assert_eq!(left_rows, right_rows);

    let snapshot = manager.snapshot();
    for path in [
        left_profile.foreground.domain,
        left_profile.background.domain,
        left_profile.control_plane.domain,
        right_profile.foreground.domain,
        right_profile.background.domain,
        right_profile.control_plane.domain,
    ] {
        assert!(snapshot.domains.contains_key(&path));
    }
}

#[tokio::test]
async fn changing_execution_domain_placement_does_not_change_logical_db_outcomes() {
    let left = run_logical_workload("/execution-invariant-left", execution_profile("left")).await;
    let right =
        run_logical_workload("/execution-invariant-right", execution_profile("right")).await;

    assert_eq!(left, right);
    assert_eq!(
        left,
        vec![
            (b"a".to_vec(), b"v1".to_vec()),
            (b"c".to_vec(), b"v3".to_vec()),
        ]
    );
}

#[test]
fn colocated_deployment_presets_publish_default_profiles_and_budgets() {
    let single =
        ColocatedDeployment::single_database(process_budget(), "primary").expect("single layout");
    let single_report = single.report();
    let primary = &single_report.databases["primary"];
    assert_eq!(
        primary.foreground.binding.domain,
        ExecutionDomainPath::new(["process", "dbs", "primary", "foreground"])
    );
    assert_eq!(
        primary.background.binding.domain,
        ExecutionDomainPath::new(["process", "dbs", "primary", "background"])
    );
    let control = primary
        .control_plane
        .snapshot
        .as_ref()
        .expect("single preset should register control-plane domain");
    assert_eq!(control.spec.placement, ExecutionDomainPlacement::Dedicated);
    assert_eq!(control.effective_budget.cpu.worker_slots, Some(1));
    assert_eq!(
        control.spec.metadata.get("terracedb.execution.database"),
        Some(&"primary".to_string())
    );

    let paired =
        ColocatedDeployment::primary_with_analytics(process_budget(), "primary", "analytics")
            .expect("primary+analytics layout");
    let paired_report = paired.report();
    assert_eq!(
        paired_report.databases["primary"]
            .foreground
            .snapshot
            .as_ref()
            .expect("primary foreground")
            .spec
            .placement,
        ExecutionDomainPlacement::SharedWeighted { weight: 3 }
    );
    assert_eq!(
        paired_report.databases["analytics"]
            .foreground
            .snapshot
            .as_ref()
            .expect("analytics foreground")
            .spec
            .metadata
            .get("terracedb.execution.role")
            .map(String::as_str),
        Some("analytics-helper")
    );

    let shard_ready =
        ColocatedDeployment::shard_ready(process_budget(), "warehouse").expect("shard-ready");
    assert_eq!(
        shard_ready.report().databases["warehouse"]
            .foreground
            .binding
            .domain,
        ExecutionDomainPath::new(["process", "shards", "warehouse", "foreground"])
    );
    assert_eq!(
        shard_ready.report().databases["warehouse"]
            .foreground
            .snapshot
            .as_ref()
            .expect("shard-ready foreground")
            .spec
            .metadata
            .get("terracedb.execution.future_shard_namespace")
            .map(String::as_str),
        Some("process/shards/warehouse/shards")
    );
}

#[tokio::test]
async fn single_db_defaults_stay_simple_without_explicit_deployment_setup() {
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());
    let db = Db::builder()
        .settings(tiered_settings("/execution-default-profile"))
        .components(stub_components(resource_manager))
        .open()
        .await
        .expect("open single db with defaults");

    let report = db.execution_placement_report();
    assert_eq!(report.database, "execution-default-profile");
    assert_eq!(
        report.foreground.binding.domain,
        ExecutionDomainPath::new(["process", "db", "foreground"])
    );
    assert_eq!(
        report.background.binding.domain,
        ExecutionDomainPath::new(["process", "db", "background"])
    );
    assert_eq!(
        report.control_plane.binding.domain,
        ExecutionDomainPath::new(["process", "control"])
    );
    assert_eq!(
        report
            .control_plane
            .snapshot
            .as_ref()
            .expect("default control-plane domain")
            .spec
            .metadata
            .get("reserved")
            .map(String::as_str),
        Some("true")
    );
}

#[tokio::test]
async fn colocated_deployment_builder_supports_multi_db_open_and_recovery() {
    let deployment =
        ColocatedDeployment::primary_with_analytics(process_budget(), "primary", "analytics")
            .expect("declare colocated deployment");
    let deployment_manager = deployment.resource_manager();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());

    let primary = open_deployed_db(
        &deployment,
        "primary",
        "/execution-deployment-primary",
        file_system.clone(),
        object_store.clone(),
    )
    .await;
    let analytics = open_deployed_db(
        &deployment,
        "analytics",
        "/execution-deployment-analytics",
        file_system.clone(),
        object_store.clone(),
    )
    .await;

    let primary_rows = run_workload(&primary).await;
    let analytics_rows = run_workload(&analytics).await;
    assert_eq!(primary_rows, analytics_rows);

    drop(primary);
    drop(analytics);

    let reopened_primary = open_deployed_db(
        &deployment,
        "primary",
        "/execution-deployment-primary",
        file_system.clone(),
        object_store.clone(),
    )
    .await;
    let reopened_analytics = open_deployed_db(
        &deployment,
        "analytics",
        "/execution-deployment-analytics",
        file_system,
        object_store,
    )
    .await;

    assert!(Arc::ptr_eq(
        &reopened_primary.resource_manager(),
        &deployment_manager
    ));
    assert!(Arc::ptr_eq(
        &reopened_analytics.resource_manager(),
        &deployment_manager
    ));
    assert_eq!(read_existing_rows(&reopened_primary).await, primary_rows);
    assert_eq!(
        read_existing_rows(&reopened_analytics).await,
        analytics_rows
    );
    assert_ne!(
        reopened_primary
            .execution_placement_report()
            .foreground
            .binding
            .domain,
        reopened_analytics
            .execution_placement_report()
            .foreground
            .binding
            .domain
    );
}

#[tokio::test]
async fn shard_ready_reopen_preserves_correctness_after_background_pressure_and_partial_drain() {
    let deployment =
        ColocatedDeployment::shard_ready(process_budget(), "warehouse").expect("shard-ready");
    let deployment_manager = deployment.resource_manager();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());

    let db = open_deployed_db(
        &deployment,
        "warehouse",
        "/execution-shard-ready-reopen",
        file_system.clone(),
        object_store.clone(),
    )
    .await;
    let rows = run_workload(&db).await;
    let background_path = db.execution_placement_report().background.binding.domain;

    let admitted = deployment_manager.try_acquire(
        &background_path,
        ExecutionResourceUsage {
            cpu_workers: 1,
            background_tasks: 1,
            ..ExecutionResourceUsage::default()
        },
    );
    assert!(admitted.admitted);
    deployment_manager.set_backlog(
        &background_path,
        ExecutionDomainBacklogSnapshot {
            queued_work_items: 2,
            queued_bytes: 256,
        },
    );

    drop(db);

    let reopened = open_deployed_db(
        &deployment,
        "warehouse",
        "/execution-shard-ready-reopen",
        file_system,
        object_store,
    )
    .await;
    assert_eq!(read_existing_rows(&reopened).await, rows);
    let background_snapshot = reopened
        .execution_placement_report()
        .background
        .snapshot
        .expect("reopened background snapshot");
    assert_eq!(background_snapshot.backlog.queued_work_items, 2);
    assert_eq!(background_snapshot.backlog.queued_bytes, 256);
    assert_eq!(background_snapshot.usage.background_tasks, 1);
}

#[tokio::test]
async fn placement_reports_include_attached_subsystems_and_match_runtime_topology() {
    let subsystem_path =
        ExecutionDomainPath::new(["process", "dbs", "primary", "subsystems", "projection"]);
    let deployment = ColocatedDeployment::builder(process_budget())
        .with_database(ColocatedDatabasePlacement::shared("primary"))
        .expect("declare primary db")
        .with_subsystem(ColocatedSubsystemPlacement::database_local(
            "primary",
            "projection",
            ExecutionLane::UserBackground,
            ExecutionLanePlacementConfig::shared(subsystem_path.clone(), DurabilityClass::UserData),
        ))
        .expect("declare projection subsystem")
        .build();
    let deployment_report = deployment.report();
    let primary = &deployment_report.databases["primary"];
    assert_eq!(primary.attached_subsystems.len(), 1);
    let subsystem_snapshot = primary
        .attached_subsystems
        .values()
        .next()
        .expect("projection subsystem should be reported");
    assert_eq!(
        subsystem_snapshot
            .spec
            .metadata
            .get("terracedb.execution.subsystem")
            .map(String::as_str),
        Some("projection")
    );
    assert!(primary.domain_topology.contains_key(&subsystem_path));

    let db = open_deployed_db(
        &deployment,
        "primary",
        "/execution-deployment-subsystem",
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
    )
    .await;
    let runtime_report = db.execution_placement_report();
    assert_eq!(
        runtime_report.foreground.binding,
        primary.foreground.binding
    );
    assert_eq!(
        runtime_report.background.binding,
        primary.background.binding
    );
    assert_eq!(
        runtime_report.control_plane.binding,
        primary.control_plane.binding
    );
    assert_eq!(
        runtime_report.attached_subsystems,
        primary.attached_subsystems
    );
    assert!(runtime_report.domain_topology.contains_key(&subsystem_path));
}
