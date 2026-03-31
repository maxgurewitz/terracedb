use std::{collections::BTreeMap, sync::Arc, time::Duration};

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
    FileSystemFailure, FileSystemOperation, InMemoryResourceManager, NoopScheduler, PendingWork,
    PendingWorkType, PressureScope, ResourceManager, S3Location, ScanOptions, Scheduler,
    ShardReadyPlacementLayout, StubClock, StubFileSystem, StubObjectStore, StubRng, TableConfig,
    TableFormat, TableStats, ThrottleDecision, TieredDurabilityMode, Value,
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

fn deferred_tiered_settings(path: &str) -> DbSettings {
    let mut settings = tiered_settings(path);
    if let terracedb::StorageConfig::Tiered(storage) = settings.storage_config() {
        let mut storage = storage.clone();
        storage.durability = TieredDurabilityMode::Deferred;
        settings = DbSettings::tiered_storage(storage);
    }
    settings
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

fn shared_stub_components_with_runtime(
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
    clock: Arc<StubClock>,
    scheduler: Arc<dyn Scheduler>,
) -> DbComponents {
    DbComponents::new(
        file_system,
        object_store,
        clock,
        Arc::new(StubRng::seeded(17)),
    )
    .with_scheduler(scheduler)
}

fn tiered_settings_with_durability(
    path: &str,
    durability: terracedb::TieredDurabilityMode,
) -> DbSettings {
    DbSettings::tiered_storage(terracedb::TieredStorageConfig {
        ssd: terracedb::SsdConfig {
            path: path.to_string(),
        },
        s3: S3Location {
            bucket: "terracedb-execution-tests".to_string(),
            prefix: "phase13".to_string(),
        },
        max_local_bytes: 1024 * 1024,
        durability,
        local_retention: terracedb::TieredLocalRetentionMode::Offload,
    })
}

fn whole_system_process_budget() -> ExecutionDomainBudget {
    ExecutionDomainBudget {
        cpu: DomainCpuBudget {
            worker_slots: Some(10),
            weight: None,
        },
        memory: DomainMemoryBudget {
            total_bytes: Some(4096),
            cache_bytes: Some(2048),
            mutable_bytes: Some(2048),
        },
        io: DomainIoBudget {
            local_concurrency: Some(6),
            local_bytes_per_second: Some(8192),
            remote_concurrency: Some(4),
            remote_bytes_per_second: Some(6144),
        },
        background: DomainBackgroundBudget {
            task_slots: Some(6),
            max_in_flight_bytes: Some(4096),
        },
    }
}

fn lane_budget(
    mutable_bytes: u64,
    cpu_slots: u32,
    remote_concurrency: u32,
    background_slots: u32,
) -> ExecutionDomainBudget {
    ExecutionDomainBudget {
        cpu: DomainCpuBudget {
            worker_slots: Some(cpu_slots),
            weight: Some(cpu_slots.max(1)),
        },
        memory: DomainMemoryBudget {
            total_bytes: Some(mutable_bytes.saturating_mul(2)),
            cache_bytes: Some(mutable_bytes),
            mutable_bytes: Some(mutable_bytes),
        },
        io: DomainIoBudget {
            local_concurrency: Some(remote_concurrency.max(1)),
            local_bytes_per_second: Some(2048),
            remote_concurrency: Some(remote_concurrency.max(1)),
            remote_bytes_per_second: Some(2048),
        },
        background: DomainBackgroundBudget {
            task_slots: Some(background_slots.max(1)),
            max_in_flight_bytes: Some(mutable_bytes.saturating_mul(4)),
        },
    }
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

#[derive(Debug, Default)]
struct MutableBudgetThrottleScheduler;

impl Scheduler for MutableBudgetThrottleScheduler {
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
        tag: &terracedb::WorkRuntimeTag,
        domain_budget: Option<&ExecutionDomainBudget>,
    ) -> ThrottleDecision {
        if tag.durability_class == DurabilityClass::ControlPlane {
            return ThrottleDecision::default();
        }

        let Some(limit) = domain_budget.and_then(|budget| budget.memory.mutable_bytes) else {
            return ThrottleDecision::default();
        };
        let projected_bytes = signals
            .pressure
            .local
            .mutable_dirty_bytes
            .saturating_add(signals.batch_write_bytes);
        if projected_bytes >= limit {
            return ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: None,
                stall: false,
            };
        }

        ThrottleDecision::default()
    }
}

#[derive(Debug)]
struct RateLimitedMutableBudgetScheduler {
    minimum_rate: u64,
}

impl Scheduler for RateLimitedMutableBudgetScheduler {
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
        _signals: &terracedb::AdmissionSignals,
        tag: &terracedb::WorkRuntimeTag,
        domain_budget: Option<&ExecutionDomainBudget>,
    ) -> ThrottleDecision {
        if tag.durability_class == DurabilityClass::ControlPlane {
            return ThrottleDecision::default();
        }

        let rate = domain_budget
            .and_then(|budget| budget.memory.mutable_bytes)
            .unwrap_or(self.minimum_rate)
            .max(self.minimum_rate);
        ThrottleDecision {
            throttle: true,
            max_write_bytes_per_second: Some(rate),
            stall: false,
        }
    }
}

#[derive(Clone, Debug)]
struct CampaignRng {
    state: u64,
}

impl CampaignRng {
    fn seeded(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut next = self.state;
        next ^= next << 7;
        next ^= next >> 9;
        next ^= next << 8;
        self.state = next;
        next
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct WholeSystemCampaignOutcome {
    database_order: Vec<String>,
    durable_rows_by_db: BTreeMap<String, DurableRows>,
    control_tables_by_db: BTreeMap<String, Vec<String>>,
    visible_sequence_by_db: BTreeMap<String, u64>,
    durable_sequence_by_db: BTreeMap<String, u64>,
    throttled_writes_by_domain: BTreeMap<String, u64>,
    mutable_budget_by_domain: BTreeMap<String, Option<u64>>,
    background_slots_by_domain: BTreeMap<String, Option<u32>>,
    backlog_items_by_domain: BTreeMap<String, u64>,
    backlog_bytes_by_domain: BTreeMap<String, u64>,
    oracle_cpu_millis_by_domain: BTreeMap<String, u64>,
    admissions: BTreeMap<String, bool>,
}

type DurableRow = (Vec<u8>, Vec<u8>);
type DurableRows = Vec<DurableRow>;

#[derive(Clone)]
struct TestRuntimeHandle {
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
    clock: Arc<StubClock>,
    scheduler: Arc<dyn Scheduler>,
    durability: terracedb::TieredDurabilityMode,
}

impl TestRuntimeHandle {
    fn new(
        file_system: Arc<StubFileSystem>,
        object_store: Arc<StubObjectStore>,
        clock: Arc<StubClock>,
        scheduler: Arc<dyn Scheduler>,
        durability: terracedb::TieredDurabilityMode,
    ) -> Self {
        Self {
            file_system,
            object_store,
            clock,
            scheduler,
            durability,
        }
    }

    fn settings(&self, path: &str) -> DbSettings {
        tiered_settings_with_durability(path, self.durability)
    }

    fn components(&self) -> DbComponents {
        shared_stub_components_with_runtime(
            self.file_system.clone(),
            self.object_store.clone(),
            self.clock.clone(),
            self.scheduler.clone(),
        )
    }
}

fn whole_system_deployment() -> (ColocatedDeployment, ExecutionDomainPath) {
    let mut primary = ColocatedDatabasePlacement::shared("primary")
        .with_metadata("terracedb.execution.role", "primary");
    primary.foreground.placement = ExecutionDomainPlacement::SharedWeighted { weight: 3 };
    primary.background.placement = ExecutionDomainPlacement::SharedWeighted { weight: 2 };
    primary.foreground.budget = lane_budget(512, 3, 2, 2);
    primary.background.budget = lane_budget(256, 2, 2, 2);

    let mut analytics = ColocatedDatabasePlacement::analytics_helper("analytics");
    analytics.foreground.budget = lane_budget(192, 1, 1, 1);
    analytics.background.budget = lane_budget(128, 1, 1, 1);

    let mut warehouse = ColocatedDatabasePlacement::shard_ready("warehouse");
    warehouse.foreground.budget = lane_budget(256, 2, 1, 1);
    warehouse.background.budget = lane_budget(160, 1, 1, 1);

    let maintenance_path =
        ExecutionDomainPath::new(["process", "dbs", "primary", "subsystems", "maintenance"]);
    let maintenance_budget = ExecutionDomainBudget {
        cpu: DomainCpuBudget {
            worker_slots: Some(1),
            weight: None,
        },
        memory: DomainMemoryBudget {
            total_bytes: Some(128),
            cache_bytes: Some(64),
            mutable_bytes: Some(64),
        },
        io: DomainIoBudget {
            local_concurrency: Some(1),
            local_bytes_per_second: Some(512),
            remote_concurrency: Some(1),
            remote_bytes_per_second: Some(512),
        },
        background: DomainBackgroundBudget {
            task_slots: Some(1),
            max_in_flight_bytes: Some(256),
        },
    };

    let deployment = ColocatedDeployment::builder(whole_system_process_budget())
        .with_database(primary)
        .expect("register primary deployment")
        .with_database(analytics)
        .expect("register analytics deployment")
        .with_database(warehouse)
        .expect("register shard-ready deployment")
        .with_subsystem(ColocatedSubsystemPlacement::database_local(
            "primary",
            "maintenance",
            ExecutionLane::UserBackground,
            ExecutionLanePlacementConfig::reserved(
                maintenance_path.clone(),
                DurabilityClass::UserData,
                maintenance_budget,
            ),
        ))
        .expect("register primary maintenance subsystem")
        .build();

    (deployment, maintenance_path)
}

async fn open_deployed_db_with_components(
    deployment: &ColocatedDeployment,
    database: &str,
    settings: DbSettings,
    components: DbComponents,
) -> Db {
    Db::builder()
        .settings(settings)
        .components(components)
        .colocated_database(deployment, database)
        .expect("bind builder to colocated deployment")
        .open()
        .await
        .expect("open deployed db")
}

async fn open_deployed_db_with_runtime(
    deployment: &ColocatedDeployment,
    database: &str,
    path: &str,
    runtime: &TestRuntimeHandle,
) -> Db {
    open_deployed_db_with_components(
        deployment,
        database,
        runtime.settings(path),
        runtime.components(),
    )
    .await
}

fn update_domain_spec(
    manager: &Arc<dyn ResourceManager>,
    path: &ExecutionDomainPath,
    mutate: impl FnOnce(&mut ExecutionDomainSpec),
) {
    let mut spec = manager
        .snapshot()
        .domains
        .get(path)
        .unwrap_or_else(|| panic!("missing execution domain {}", path.as_string()))
        .spec
        .clone();
    mutate(&mut spec);
    manager.update_domain(spec);
}

async fn wait_for_failpoint_hit_with_clock(
    handle: &terracedb::FailpointHandle,
    clock: &StubClock,
    step: Duration,
    max_steps: usize,
) -> terracedb::FailpointHit {
    let mut wait = Box::pin(handle.next_hit());
    for _ in 0..max_steps {
        tokio::select! {
            hit = &mut wait => return hit,
            _ = tokio::task::yield_now() => {
                clock.advance(step);
            }
        }
    }

    panic!("failpoint was not hit within {max_steps} virtual-clock advances");
}

async fn advance_clock_until_finished<T>(
    clock: &StubClock,
    handle: &tokio::task::JoinHandle<T>,
    step: Duration,
    max_steps: usize,
) -> u64 {
    let start = terracedb::Clock::now(clock).get();
    for _ in 0..max_steps {
        if handle.is_finished() {
            return terracedb::Clock::now(clock).get().saturating_sub(start);
        }
        clock.advance(step);
        tokio::task::yield_now().await;
    }

    panic!("task was not finished within {max_steps} virtual-clock advances");
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
    assert_eq!(flush_candidates.len(), 1);
    assert_eq!(flush_candidates[0].work.work.table, "events");
    assert_eq!(
        flush_candidates[0]
            .work
            .estimated_relief
            .mutable_dirty_bytes,
        table_pressure.local.mutable_dirty_bytes
    );
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
async fn pressure_stats_reconstruct_after_failed_flush_reopen() {
    let profile = execution_profile("pressure-failed-flush");
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());
    let config =
        tiered_settings("/execution-pressure-failed-flush").with_execution_profile(profile.clone());

    let open_components = || {
        DbComponents::new(
            file_system.clone(),
            object_store.clone(),
            Arc::new(StubClock::default()),
            Arc::new(StubRng::seeded(23)),
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
    let sequence = table
        .write(b"user:1".to_vec(), Value::bytes("v1"))
        .await
        .expect("write value before failed flush");

    file_system.inject_failure(FileSystemFailure::timeout(
        FileSystemOperation::SyncDir,
        "/execution-pressure-failed-flush/manifest",
    ));
    db.flush().await.expect_err("flush should fail");

    let after_failure = db.table_pressure_stats(&table).await;
    assert_eq!(after_failure.local.mutable_dirty_bytes, 0);
    assert!(after_failure.local.immutable_queued_bytes > 0);
    assert_eq!(after_failure.local.immutable_flushing_bytes, 0);
    assert_eq!(
        after_failure.local.unified_log_pinned_bytes,
        after_failure.local.immutable_queued_bytes
    );
    assert_eq!(
        after_failure.metadata.get("oldest_unflushed_sequence"),
        Some(&serde_json::Value::from(sequence.get()))
    );

    drop(db);
    file_system.crash();

    let reopened = Db::builder()
        .settings(config)
        .components(open_components())
        .open()
        .await
        .expect("reopen after failed flush");
    let reopened_table = reopened.table("events");
    let after_reopen = reopened.table_pressure_stats(&reopened_table).await;

    assert_eq!(
        after_reopen.local.mutable_dirty_bytes,
        after_failure.local.unified_log_pinned_bytes
    );
    assert_eq!(after_reopen.local.immutable_queued_bytes, 0);
    assert_eq!(after_reopen.local.immutable_flushing_bytes, 0);
    assert_eq!(
        after_reopen.local.unified_log_pinned_bytes,
        after_failure.local.unified_log_pinned_bytes
    );
    assert_eq!(
        after_reopen.metadata.get("oldest_unflushed_sequence"),
        Some(&serde_json::Value::from(sequence.get()))
    );
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
    deployment
        .open_database(
            database,
            tiered_settings(path),
            shared_stub_components(file_system, object_store),
        )
        .await
        .expect("open deployed db")
}

async fn run_whole_system_campaign(seed: u64) -> WholeSystemCampaignOutcome {
    let scheduler: Arc<dyn Scheduler> = Arc::new(MutableBudgetThrottleScheduler);
    let clock = Arc::new(StubClock::default());
    let durability = terracedb::TieredDurabilityMode::Deferred;
    let (deployment, maintenance_path) = whole_system_deployment();
    let manager = deployment.resource_manager();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let runtime = TestRuntimeHandle::new(
        file_system.clone(),
        object_store.clone(),
        clock.clone(),
        scheduler.clone(),
        durability,
    );

    let primary = open_deployed_db_with_runtime(
        &deployment,
        "primary",
        &format!("/execution-whole-system-primary-{seed:x}"),
        &runtime,
    )
    .await;
    let analytics = open_deployed_db_with_runtime(
        &deployment,
        "analytics",
        &format!("/execution-whole-system-analytics-{seed:x}"),
        &runtime,
    )
    .await;
    let warehouse = open_deployed_db_with_runtime(
        &deployment,
        "warehouse",
        &format!("/execution-whole-system-warehouse-{seed:x}"),
        &runtime,
    )
    .await;

    let primary_events = primary
        .create_table(row_table_config("events"))
        .await
        .expect("create primary events table");
    let analytics_events = analytics
        .create_table(row_table_config("events"))
        .await
        .expect("create analytics events table");
    let warehouse_events = warehouse
        .create_table(row_table_config("events"))
        .await
        .expect("create warehouse events table");

    let oracle = InMemoryDomainBudgetOracle::default();
    let mut pending = BTreeMap::from([
        ("primary".to_string(), BTreeMap::<Vec<u8>, Vec<u8>>::new()),
        ("analytics".to_string(), BTreeMap::<Vec<u8>, Vec<u8>>::new()),
        ("warehouse".to_string(), BTreeMap::<Vec<u8>, Vec<u8>>::new()),
    ]);
    let mut durable = pending.clone();
    let mut control_tables = BTreeMap::from([
        ("primary".to_string(), Vec::<String>::new()),
        ("analytics".to_string(), Vec::<String>::new()),
        ("warehouse".to_string(), Vec::<String>::new()),
    ]);

    for (name, db, table, control_bytes) in [
        ("primary", &primary, &primary_events, 24_u64),
        ("analytics", &analytics, &analytics_events, 16_u64),
        ("warehouse", &warehouse, &warehouse_events, 20_u64),
    ] {
        oracle.record(
            &db.tag_control_plane_work(format!("create-table:{name}:events"))
                .tag,
            DomainBudgetCharge {
                cpu_millis: 1,
                memory_bytes: control_bytes,
                local_io_bytes: control_bytes,
                remote_io_bytes: 0,
                background_tasks: 0,
            },
        );

        let value = format!("{name}-bootstrap-{seed:04x}").into_bytes();
        table
            .write(b"bootstrap".to_vec(), Value::bytes(value.clone()))
            .await
            .expect("write bootstrap value");
        pending
            .get_mut(name)
            .expect("bootstrap pending domain")
            .insert(b"bootstrap".to_vec(), value.clone());
        if db.current_durable_sequence() == db.current_sequence() {
            durable
                .get_mut(name)
                .expect("bootstrap durable domain")
                .extend(pending.get(name).expect("bootstrap pending state").clone());
            pending
                .get_mut(name)
                .expect("clear bootstrap pending state after implicit durability")
                .clear();
        }
        db.flush().await.expect("flush bootstrap write");
        durable
            .get_mut(name)
            .expect("bootstrap durable domain")
            .extend(pending.get(name).expect("bootstrap pending state").clone());
        pending
            .get_mut(name)
            .expect("clear bootstrap pending state")
            .clear();
        oracle.record(
            &db.tag_user_foreground_work(()).tag,
            DomainBudgetCharge {
                cpu_millis: 2,
                memory_bytes: value.len() as u64,
                local_io_bytes: value.len() as u64,
                remote_io_bytes: 0,
                background_tasks: 0,
            },
        );
    }

    let primary_report = primary.execution_placement_report();
    let analytics_report = analytics.execution_placement_report();
    let warehouse_report = warehouse.execution_placement_report();
    let analytics_foreground = analytics_report.foreground.binding.domain.clone();
    let analytics_background = analytics_report.background.binding.domain.clone();
    let primary_control = primary_report.control_plane.binding.domain.clone();
    let warehouse_background = warehouse_report.background.binding.domain.clone();

    let mut rng = CampaignRng::seeded(seed);
    let mut database_order = Vec::new();
    for round in 0..6 {
        let target = match rng.next_u64() % 3 {
            0 => "primary",
            1 => "analytics",
            _ => "warehouse",
        };
        database_order.push(target.to_string());

        let (db, table) = match target {
            "primary" => (&primary, &primary_events),
            "analytics" => (&analytics, &analytics_events),
            "warehouse" => (&warehouse, &warehouse_events),
            _ => unreachable!("campaign only targets declared databases"),
        };
        let key = format!("seed-{seed:04x}-round-{round}").into_bytes();
        let value = format!("{target}-{:016x}", rng.next_u64()).into_bytes();
        table
            .write(key.clone(), Value::bytes(value.clone()))
            .await
            .expect("write seeded campaign row");
        pending
            .get_mut(target)
            .expect("seeded pending domain")
            .insert(key, value.clone());
        if db.current_durable_sequence() == db.current_sequence() {
            durable
                .get_mut(target)
                .expect("seeded durable domain")
                .extend(pending.get(target).expect("seeded pending state").clone());
            pending
                .get_mut(target)
                .expect("clear seeded pending state after implicit durability")
                .clear();
        }
        oracle.record(
            &db.tag_user_foreground_work(()).tag,
            DomainBudgetCharge {
                cpu_millis: 3,
                memory_bytes: value.len() as u64,
                local_io_bytes: value.len() as u64,
                remote_io_bytes: if target == "primary" { 0 } else { 64 },
                background_tasks: 0,
            },
        );

        if round % 2 == 0 && target != "analytics" {
            db.flush().await.expect("flush seeded workload round");
            durable
                .get_mut(target)
                .expect("round durable domain")
                .extend(pending.get(target).expect("round pending state").clone());
            pending
                .get_mut(target)
                .expect("clear round pending state")
                .clear();
        }
    }

    let primary_control_table = format!("audit_{seed:04x}");
    primary
        .create_table(row_table_config(&primary_control_table))
        .await
        .expect("create primary control-plane table");
    primary
        .flush()
        .await
        .expect("flush primary control-plane table");
    durable
        .get_mut("primary")
        .expect("primary durable domain")
        .extend(
            pending
                .get("primary")
                .expect("primary pending state")
                .clone(),
        );
    pending
        .get_mut("primary")
        .expect("clear primary pending state")
        .clear();
    control_tables
        .get_mut("primary")
        .expect("primary control-table set")
        .push(primary_control_table.clone());
    oracle.record(
        &primary.tag_control_plane_work(primary_control_table).tag,
        DomainBudgetCharge {
            cpu_millis: 2,
            memory_bytes: 48,
            local_io_bytes: 48,
            remote_io_bytes: 0,
            background_tasks: 0,
        },
    );

    let warehouse_control_table = format!("warehouse_meta_{seed:04x}");
    warehouse
        .create_table(row_table_config(&warehouse_control_table))
        .await
        .expect("create shard-ready control-plane table");
    warehouse
        .flush()
        .await
        .expect("flush shard-ready control-plane table");
    durable
        .get_mut("warehouse")
        .expect("warehouse durable domain")
        .extend(
            pending
                .get("warehouse")
                .expect("warehouse pending state")
                .clone(),
        );
    pending
        .get_mut("warehouse")
        .expect("clear warehouse pending state")
        .clear();
    control_tables
        .get_mut("warehouse")
        .expect("warehouse control-table set")
        .push(warehouse_control_table.clone());
    oracle.record(
        &warehouse
            .tag_control_plane_work(warehouse_control_table)
            .tag,
        DomainBudgetCharge {
            cpu_millis: 2,
            memory_bytes: 52,
            local_io_bytes: 52,
            remote_io_bytes: 0,
            background_tasks: 0,
        },
    );

    let analytics_pressure_admitted = manager
        .try_acquire(
            &analytics_background,
            ExecutionResourceUsage {
                remote_io_concurrency: 1,
                remote_io_bytes_per_second: 256,
                background_tasks: 1,
                background_in_flight_bytes: 128,
                ..ExecutionResourceUsage::default()
            },
        )
        .admitted;
    let warehouse_pressure_admitted = manager
        .try_acquire(
            &warehouse_background,
            ExecutionResourceUsage {
                remote_io_concurrency: 1,
                remote_io_bytes_per_second: 256,
                background_tasks: 1,
                background_in_flight_bytes: 128,
                ..ExecutionResourceUsage::default()
            },
        )
        .admitted;

    update_domain_spec(&manager, &analytics_foreground, |spec| {
        spec.budget.memory.mutable_bytes = Some(64);
    });
    update_domain_spec(&manager, &warehouse_background, |spec| {
        spec.budget.background.task_slots = Some(1);
        spec.budget.io.remote_concurrency = Some(1);
    });

    for burst in 0..3_usize {
        let key = format!("analytics-burst-{seed:04x}-{burst}").into_bytes();
        let value = vec![b'a'; 48 + (burst * 8)];
        analytics_events
            .write(key.clone(), Value::bytes(value.clone()))
            .await
            .expect("write analytics burst row");
        pending
            .get_mut("analytics")
            .expect("analytics pending domain")
            .insert(key, value.clone());
        if analytics.current_durable_sequence() == analytics.current_sequence() {
            durable
                .get_mut("analytics")
                .expect("analytics durable domain")
                .extend(
                    pending
                        .get("analytics")
                        .expect("analytics pending state")
                        .clone(),
                );
            pending
                .get_mut("analytics")
                .expect("clear analytics pending state after implicit durability")
                .clear();
        }
        oracle.record(
            &analytics.tag_user_foreground_work(()).tag,
            DomainBudgetCharge {
                cpu_millis: 4,
                memory_bytes: value.len() as u64,
                local_io_bytes: value.len() as u64,
                remote_io_bytes: 96,
                background_tasks: 0,
            },
        );
    }

    manager.set_backlog(
        &warehouse_background,
        ExecutionDomainBacklogSnapshot {
            queued_work_items: 2,
            queued_bytes: 256,
        },
    );
    let warehouse_shared_overflow_blocked = !manager
        .try_acquire(
            &warehouse_background,
            ExecutionResourceUsage {
                remote_io_concurrency: 1,
                remote_io_bytes_per_second: 128,
                background_tasks: 1,
                background_in_flight_bytes: 64,
                ..ExecutionResourceUsage::default()
            },
        )
        .admitted;

    let maintenance_tag = manager.placement_tag(terracedb::WorkPlacementRequest {
        owner: ExecutionDomainOwner::Subsystem {
            database: Some("primary".to_string()),
            name: "maintenance".to_string(),
        },
        lane: ExecutionLane::UserBackground,
        contention_class: ContentionClass::UserData,
        binding: ExecutionLaneBinding::new(maintenance_path.clone(), DurabilityClass::UserData),
    });
    oracle.record(
        &maintenance_tag,
        DomainBudgetCharge {
            cpu_millis: 2,
            memory_bytes: 32,
            local_io_bytes: 0,
            remote_io_bytes: 32,
            background_tasks: 1,
        },
    );
    let maintenance_admitted = manager
        .try_acquire(
            &maintenance_path,
            ExecutionResourceUsage {
                background_tasks: 1,
                background_in_flight_bytes: 64,
                ..ExecutionResourceUsage::default()
            },
        )
        .admitted;
    let control_plane_admitted = manager
        .try_acquire(
            &primary_control,
            ExecutionResourceUsage {
                cpu_workers: 1,
                background_tasks: 1,
                ..ExecutionResourceUsage::default()
            },
        )
        .admitted;

    primary.flush().await.expect("final primary flush");
    durable
        .get_mut("primary")
        .expect("final primary durable domain")
        .extend(
            pending
                .get("primary")
                .expect("final primary pending state")
                .clone(),
        );
    pending
        .get_mut("primary")
        .expect("clear final primary pending state")
        .clear();
    warehouse.flush().await.expect("final warehouse flush");
    durable
        .get_mut("warehouse")
        .expect("final warehouse durable domain")
        .extend(
            pending
                .get("warehouse")
                .expect("final warehouse pending state")
                .clone(),
        );
    pending
        .get_mut("warehouse")
        .expect("clear final warehouse pending state")
        .clear();

    file_system.crash();
    drop(primary_events);
    drop(analytics_events);
    drop(warehouse_events);
    drop(primary);
    drop(analytics);
    drop(warehouse);

    let reopened_runtime = TestRuntimeHandle::new(
        file_system.clone(),
        object_store.clone(),
        Arc::new(StubClock::default()),
        Arc::new(MutableBudgetThrottleScheduler),
        durability,
    );
    let reopened_primary = open_deployed_db_with_runtime(
        &deployment,
        "primary",
        &format!("/execution-whole-system-primary-{seed:x}"),
        &reopened_runtime,
    )
    .await;
    let reopened_analytics = open_deployed_db_with_runtime(
        &deployment,
        "analytics",
        &format!("/execution-whole-system-analytics-{seed:x}"),
        &reopened_runtime,
    )
    .await;
    let reopened_warehouse = open_deployed_db_with_runtime(
        &deployment,
        "warehouse",
        &format!("/execution-whole-system-warehouse-{seed:x}"),
        &reopened_runtime,
    )
    .await;

    let actual_primary_rows = read_existing_rows(&reopened_primary).await;
    let actual_analytics_rows = read_existing_rows(&reopened_analytics).await;
    let actual_warehouse_rows = read_existing_rows(&reopened_warehouse).await;
    for table_name in control_tables
        .get("primary")
        .expect("primary control tables")
        .iter()
    {
        assert!(reopened_primary.try_table(table_name.clone()).is_some());
    }
    for table_name in control_tables
        .get("warehouse")
        .expect("warehouse control tables")
        .iter()
    {
        assert!(reopened_warehouse.try_table(table_name.clone()).is_some());
    }

    let mut throttled_writes_by_domain = BTreeMap::new();
    for db in [&reopened_primary, &reopened_analytics, &reopened_warehouse] {
        for (path, count) in db
            .scheduler_observability_snapshot()
            .throttled_writes_by_domain
        {
            *throttled_writes_by_domain
                .entry(path.as_string())
                .or_insert(0) += count;
        }
    }

    let snapshot = manager.snapshot();
    WholeSystemCampaignOutcome {
        database_order,
        durable_rows_by_db: BTreeMap::from([
            ("primary".to_string(), actual_primary_rows),
            ("analytics".to_string(), actual_analytics_rows),
            ("warehouse".to_string(), actual_warehouse_rows),
        ]),
        control_tables_by_db: control_tables,
        visible_sequence_by_db: BTreeMap::from([
            (
                "primary".to_string(),
                reopened_primary.current_sequence().get(),
            ),
            (
                "analytics".to_string(),
                reopened_analytics.current_sequence().get(),
            ),
            (
                "warehouse".to_string(),
                reopened_warehouse.current_sequence().get(),
            ),
        ]),
        durable_sequence_by_db: BTreeMap::from([
            (
                "primary".to_string(),
                reopened_primary.current_durable_sequence().get(),
            ),
            (
                "analytics".to_string(),
                reopened_analytics.current_durable_sequence().get(),
            ),
            (
                "warehouse".to_string(),
                reopened_warehouse.current_durable_sequence().get(),
            ),
        ]),
        throttled_writes_by_domain,
        mutable_budget_by_domain: BTreeMap::from([
            (
                analytics_foreground.as_string(),
                snapshot.domains[&analytics_foreground]
                    .spec
                    .budget
                    .memory
                    .mutable_bytes,
            ),
            (
                warehouse_background.as_string(),
                snapshot.domains[&warehouse_background]
                    .spec
                    .budget
                    .memory
                    .mutable_bytes,
            ),
            (
                maintenance_path.as_string(),
                snapshot.domains[&maintenance_path]
                    .spec
                    .budget
                    .memory
                    .mutable_bytes,
            ),
        ]),
        background_slots_by_domain: BTreeMap::from([
            (
                warehouse_background.as_string(),
                snapshot.domains[&warehouse_background]
                    .spec
                    .budget
                    .background
                    .task_slots,
            ),
            (
                maintenance_path.as_string(),
                snapshot.domains[&maintenance_path]
                    .spec
                    .budget
                    .background
                    .task_slots,
            ),
        ]),
        backlog_items_by_domain: BTreeMap::from([(
            warehouse_background.as_string(),
            u64::from(
                snapshot.domains[&warehouse_background]
                    .backlog
                    .queued_work_items,
            ),
        )]),
        backlog_bytes_by_domain: BTreeMap::from([(
            warehouse_background.as_string(),
            snapshot.domains[&warehouse_background].backlog.queued_bytes,
        )]),
        oracle_cpu_millis_by_domain: oracle
            .snapshot()
            .into_iter()
            .map(|(path, usage)| (path.as_string(), usage.total.cpu_millis))
            .collect(),
        admissions: BTreeMap::from([
            (
                "analytics-background-pressure".to_string(),
                analytics_pressure_admitted,
            ),
            (
                "warehouse-background-pressure".to_string(),
                warehouse_pressure_admitted,
            ),
            (
                "warehouse-shared-overflow-blocked".to_string(),
                warehouse_shared_overflow_blocked,
            ),
            ("primary-maintenance".to_string(), maintenance_admitted),
            ("primary-control-plane".to_string(), control_plane_admitted),
        ]),
    }
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
    let single_report = single.runtime_report();
    assert_eq!(single.report(), single_report);
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

    let mut opened = deployment
        .open_all(
            [
                (
                    "primary".to_string(),
                    tiered_settings("/execution-deployment-primary"),
                ),
                (
                    "analytics".to_string(),
                    tiered_settings("/execution-deployment-analytics"),
                ),
            ],
            shared_stub_components(file_system.clone(), object_store.clone()),
        )
        .await
        .expect("open colocated deployment");
    let primary = opened.remove("primary").expect("primary db");
    let analytics = opened.remove("analytics").expect("analytics db");

    let primary_rows = run_workload(&primary).await;
    let analytics_rows = run_workload(&analytics).await;
    assert_eq!(primary_rows, analytics_rows);

    drop(primary);
    drop(analytics);

    let mut reopened = deployment
        .open_all(
            [
                (
                    "primary".to_string(),
                    tiered_settings("/execution-deployment-primary"),
                ),
                (
                    "analytics".to_string(),
                    tiered_settings("/execution-deployment-analytics"),
                ),
            ],
            shared_stub_components(file_system, object_store),
        )
        .await
        .expect("reopen colocated deployment");
    let reopened_primary = reopened.remove("primary").expect("reopened primary");
    let reopened_analytics = reopened.remove("analytics").expect("reopened analytics");

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
async fn db_lane_helpers_offer_drop_safe_admission_and_backlog_paths() {
    let deployment =
        ColocatedDeployment::single_database(process_budget(), "primary").expect("single layout");
    let db = open_deployed_db(
        &deployment,
        "primary",
        "/execution-lane-helpers",
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
    )
    .await;

    let foreground_path = db
        .execution_lane_binding(ExecutionLane::UserForeground)
        .domain
        .clone();
    let background_path = db
        .execution_lane_binding(ExecutionLane::UserBackground)
        .domain
        .clone();
    assert_eq!(db.tag_user_background_work(()).tag.domain, background_path);
    assert_eq!(
        db.tag_work(
            (),
            ExecutionDomainOwner::Subsystem {
                database: Some("primary".to_string()),
                name: "repair".to_string(),
            },
            ExecutionLane::ControlPlane,
            terracedb::ContentionClass::ControlPlane,
        )
        .tag
        .domain,
        db.execution_lane_binding(ExecutionLane::ControlPlane)
            .domain
            .clone()
    );

    let probe = db.probe_lane_admission(
        ExecutionLane::UserForeground,
        ExecutionResourceUsage {
            cpu_workers: 1,
            ..ExecutionResourceUsage::default()
        },
    );
    assert!(probe.admitted);
    assert!(db.with_lane_usage(
        ExecutionLane::UserForeground,
        ExecutionResourceUsage {
            cpu_workers: 1,
            ..ExecutionResourceUsage::default()
        },
        |decision| decision.admitted
    ));
    let observed_during_scoped_usage = db.with_lane_usage(
        ExecutionLane::UserForeground,
        ExecutionResourceUsage {
            cpu_workers: 1,
            ..ExecutionResourceUsage::default()
        },
        |_| {
            db.resource_manager_snapshot().domains[&foreground_path]
                .usage
                .cpu_workers_in_use
        },
    );
    assert_eq!(observed_during_scoped_usage, 1);
    assert_eq!(
        db.resource_manager_snapshot().domains[&foreground_path]
            .usage
            .cpu_workers_in_use,
        0
    );
    assert!(
        db.with_lane_usage_async(
            ExecutionLane::UserForeground,
            ExecutionResourceUsage {
                cpu_workers: 1,
                ..ExecutionResourceUsage::default()
            },
            |decision| async move { decision.admitted }
        )
        .await
    );
    assert_eq!(
        db.resource_manager_snapshot().domains[&foreground_path]
            .usage
            .cpu_workers_in_use,
        0
    );

    {
        let lease = db.acquire_lane_usage(
            ExecutionLane::UserForeground,
            ExecutionResourceUsage {
                cpu_workers: 1,
                ..ExecutionResourceUsage::default()
            },
        );
        assert!(lease.admitted());
        assert_eq!(
            db.resource_manager_snapshot().domains[&foreground_path]
                .usage
                .cpu_workers_in_use,
            1
        );
    }
    assert_eq!(
        db.resource_manager_snapshot().domains[&foreground_path]
            .usage
            .cpu_workers_in_use,
        0
    );

    {
        let _backlog = db.set_lane_backlog(
            ExecutionLane::UserBackground,
            ExecutionDomainBacklogSnapshot {
                queued_work_items: 2,
                queued_bytes: 64,
            },
        );
        let snapshot = db.resource_manager_snapshot();
        assert_eq!(
            snapshot.domains[&background_path].backlog.queued_work_items,
            2
        );
        assert_eq!(snapshot.domains[&background_path].backlog.queued_bytes, 64);
    }
    let snapshot = db.resource_manager_snapshot();
    assert_eq!(
        snapshot.domains[&background_path].backlog.queued_work_items,
        0
    );
    assert_eq!(snapshot.domains[&background_path].backlog.queued_bytes, 0);
    let queued = db.with_lane_backlog(
        ExecutionLane::UserBackground,
        ExecutionDomainBacklogSnapshot {
            queued_work_items: 3,
            queued_bytes: 96,
        },
        || db.resource_manager_snapshot().domains[&background_path].backlog,
    );
    assert_eq!(queued.queued_work_items, 3);
    assert_eq!(queued.queued_bytes, 96);
    let queued_async = db
        .with_lane_backlog_async(
            ExecutionLane::UserBackground,
            ExecutionDomainBacklogSnapshot {
                queued_work_items: 4,
                queued_bytes: 128,
            },
            || async {
                db.resource_manager_snapshot().domains[&background_path]
                    .backlog
                    .queued_work_items
            },
        )
        .await;
    assert_eq!(queued_async, 4);
    let snapshot = db.resource_manager_snapshot();
    assert_eq!(
        snapshot.domains[&background_path].backlog.queued_work_items,
        0
    );
    assert_eq!(snapshot.domains[&background_path].backlog.queued_bytes, 0);
}

#[tokio::test]
async fn ensure_table_and_flush_with_status_surface_happy_path_state() {
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());
    let db = Db::builder()
        .settings(deferred_tiered_settings("/execution-flush-status"))
        .components(stub_components(resource_manager))
        .open()
        .await
        .expect("open db");

    let table = db
        .ensure_table(row_table_config("events"))
        .await
        .expect("create events table");
    let reopened = db
        .ensure_table(row_table_config("events"))
        .await
        .expect("reuse events table");
    assert_eq!(table.id(), reopened.id());

    let sequence = table
        .write(b"k".to_vec(), Value::Bytes(b"v".to_vec()))
        .await
        .expect("write row");
    let status = db.flush_with_status().await.expect("flush with status");
    assert_eq!(status.current_sequence_before, sequence);
    assert_eq!(status.current_sequence_after, sequence);
    assert!(!status.visible_sequence_advanced());
    assert!(status.durable_sequence_before < sequence);
    assert!(status.had_visible_non_durable_writes());
    assert!(status.durable_sequence_advanced());
    assert!(status.changed_any_frontier());
    assert!(status.durable_sequence_after >= sequence);
}

#[test]
fn checked_release_returns_structured_error_on_underflow() {
    let manager = InMemoryResourceManager::new(process_budget());
    let path = ExecutionDomainPath::new(["process", "db-a", "foreground"]);
    manager.register_domain(shared_spec(
        path.clone(),
        ExecutionDomainOwner::Database {
            name: "db-a".to_string(),
        },
        1,
        ExecutionDomainBudget::default(),
    ));

    let error = manager
        .checked_release(
            &path,
            ExecutionResourceUsage {
                cpu_workers: 1,
                ..ExecutionResourceUsage::default()
            },
        )
        .expect_err("checked release should report underflow");
    assert_eq!(error.path(), &path);
    assert_eq!(error.held_usage(), ExecutionResourceUsage::default());
    assert_eq!(
        error.requested_release(),
        ExecutionResourceUsage {
            cpu_workers: 1,
            ..ExecutionResourceUsage::default()
        }
    );
}

#[test]
#[should_panic(expected = "attempted to release more execution-domain usage than acquired")]
fn release_panics_when_usage_would_underflow() {
    let manager = InMemoryResourceManager::new(process_budget());
    let path = ExecutionDomainPath::new(["process", "db-a", "foreground"]);
    manager.register_domain(shared_spec(
        path.clone(),
        ExecutionDomainOwner::Database {
            name: "db-a".to_string(),
        },
        1,
        ExecutionDomainBudget::default(),
    ));

    manager.release(
        &path,
        ExecutionResourceUsage {
            cpu_workers: 1,
            ..ExecutionResourceUsage::default()
        },
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

#[tokio::test]
async fn whole_system_execution_domain_campaigns_remain_deterministic_across_seeds() {
    let seeds = [0x6901_u64, 0x6902, 0x6903];

    for seed in seeds {
        let first = run_whole_system_campaign(seed).await;
        let second = run_whole_system_campaign(seed).await;
        assert_eq!(first, second, "seed {seed:#x} should be reproducible");
        assert!(
            first.admissions["warehouse-shared-overflow-blocked"],
            "seed {seed:#x} should block extra shard-ready background work after tightening budgets"
        );
        assert!(
            first.admissions["primary-control-plane"],
            "seed {seed:#x} should keep the protected control-plane domain progressing"
        );
        assert_eq!(
            first.mutable_budget_by_domain["process/dbs/analytics/foreground"],
            Some(64)
        );
        assert_eq!(
            first.background_slots_by_domain["process/shards/warehouse/background"],
            Some(1)
        );
        assert_eq!(
            first.backlog_items_by_domain["process/shards/warehouse/background"],
            2
        );
        assert_eq!(
            first.backlog_bytes_by_domain["process/shards/warehouse/background"],
            256
        );
        assert_eq!(first.control_tables_by_db["primary"].len(), 1);
        assert_eq!(first.control_tables_by_db["warehouse"].len(), 1);
        assert!(
            first.oracle_cpu_millis_by_domain.contains_key("process"),
            "seed {seed:#x} should aggregate the oracle at the process root"
        );
    }
}

#[tokio::test]
#[ignore = "slow whole-system domain chaos matrix"]
async fn execution_domain_whole_system_nightly_seed_matrix() {
    let seeds = [0x6904_u64, 0x6905, 0x6906, 0x6907, 0x6908, 0x6909];
    let mut orders = Vec::new();

    for seed in seeds {
        let outcome = run_whole_system_campaign(seed).await;
        assert!(
            outcome.admissions["primary-control-plane"],
            "seed {seed:#x} should keep the control plane progressing"
        );
        orders.push(outcome.database_order);
    }

    assert!(
        orders.windows(2).any(|window| window[0] != window[1]),
        "nightly seed matrix should exercise more than one seeded execution order"
    );
}

#[tokio::test]
async fn execution_domain_chaos_suite_preserves_protected_progress_under_stalls_and_timing_skew() {
    let scheduler: Arc<dyn Scheduler> =
        Arc::new(RateLimitedMutableBudgetScheduler { minimum_rate: 16 });
    let clock = Arc::new(StubClock::default());
    let durability = terracedb::TieredDurabilityMode::Deferred;
    let (deployment, _maintenance_path) = whole_system_deployment();
    let manager = deployment.resource_manager();
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let runtime = TestRuntimeHandle::new(
        file_system.clone(),
        object_store.clone(),
        clock.clone(),
        scheduler.clone(),
        durability,
    );

    let primary =
        open_deployed_db_with_runtime(&deployment, "primary", "/execution-chaos-primary", &runtime)
            .await;
    let analytics = open_deployed_db_with_runtime(
        &deployment,
        "analytics",
        "/execution-chaos-analytics",
        &runtime,
    )
    .await;
    let warehouse = open_deployed_db_with_runtime(
        &deployment,
        "warehouse",
        "/execution-chaos-warehouse",
        &runtime,
    )
    .await;

    let primary_events = primary
        .create_table(row_table_config("events"))
        .await
        .expect("create primary events table");
    let analytics_events = analytics
        .create_table(row_table_config("events"))
        .await
        .expect("create analytics events table");
    let warehouse_events = warehouse
        .create_table(row_table_config("events"))
        .await
        .expect("create shard-ready events table");

    for (table, key, value) in [
        (
            primary_events.clone(),
            b"bootstrap".to_vec(),
            b"primary".to_vec(),
        ),
        (
            analytics_events.clone(),
            b"bootstrap".to_vec(),
            b"analytics".to_vec(),
        ),
        (
            warehouse_events.clone(),
            b"bootstrap".to_vec(),
            b"warehouse".to_vec(),
        ),
    ] {
        let write = tokio::spawn(async move { table.write(key, Value::bytes(value)).await });
        advance_clock_until_finished(clock.as_ref(), &write, Duration::from_millis(25), 256).await;
        write
            .await
            .expect("bootstrap task should finish")
            .expect("bootstrap write should succeed");
    }
    primary.flush().await.expect("flush primary bootstrap");
    analytics.flush().await.expect("flush analytics bootstrap");
    warehouse.flush().await.expect("flush warehouse bootstrap");

    let analytics_foreground = analytics
        .execution_placement_report()
        .foreground
        .binding
        .domain;
    let stall = analytics.__failpoint_registry().arm_pause(
        terracedb::failpoints::names::DB_COMMIT_BEFORE_MEMTABLE_INSERT,
        terracedb::FailpointMode::Once,
    );
    let stalled_write = tokio::spawn({
        let table = analytics_events.clone();
        async move {
            table
                .write(b"stalled".to_vec(), Value::bytes(vec![b's'; 96]))
                .await
        }
    });

    let hit =
        wait_for_failpoint_hit_with_clock(&stall, clock.as_ref(), Duration::from_millis(25), 512)
            .await;
    assert_eq!(
        hit.name,
        terracedb::failpoints::names::DB_COMMIT_BEFORE_MEMTABLE_INSERT
    );

    update_domain_spec(&manager, &analytics_foreground, |spec| {
        spec.budget.memory.mutable_bytes = Some(32);
    });
    primary
        .create_table(row_table_config("audit_chaos"))
        .await
        .expect("control-plane progress should succeed while analytics is stalled");
    primary
        .flush()
        .await
        .expect("flush protected control-plane progress");

    stall.release();
    advance_clock_until_finished(
        clock.as_ref(),
        &stalled_write,
        Duration::from_millis(25),
        512,
    )
    .await;
    stalled_write
        .await
        .expect("stalled write task should finish")
        .expect("stalled analytics write should succeed");
    analytics
        .flush()
        .await
        .expect("flush recovered analytics write");

    let fast_write = tokio::spawn({
        let table = primary_events.clone();
        async move {
            table
                .write(b"primary-tail".to_vec(), Value::bytes(vec![b'p'; 128]))
                .await
        }
    });
    let slow_write = tokio::spawn({
        let table = analytics_events.clone();
        async move {
            table
                .write(b"analytics-tail".to_vec(), Value::bytes(vec![b'a'; 128]))
                .await
        }
    });

    let fast_elapsed =
        advance_clock_until_finished(clock.as_ref(), &fast_write, Duration::from_millis(25), 512)
            .await;
    assert!(
        !slow_write.is_finished(),
        "analytics write should still be throttled when the primary write completes"
    );
    fast_write
        .await
        .expect("primary tail task should finish")
        .expect("primary tail write should succeed");
    let slow_elapsed =
        advance_clock_until_finished(clock.as_ref(), &slow_write, Duration::from_millis(25), 512)
            .await;
    slow_write
        .await
        .expect("analytics tail task should finish")
        .expect("analytics tail write should succeed");

    assert!(
        slow_elapsed > fast_elapsed,
        "tightened analytics budget should produce more virtual delay: fast={fast_elapsed}ms slow={slow_elapsed}ms"
    );
    assert!(
        analytics
            .scheduler_observability_snapshot()
            .throttled_writes_by_domain
            .get(&analytics_foreground)
            .copied()
            .unwrap_or_default()
            > 0,
        "analytics writes should be recorded as throttled under the tightened mutable budget"
    );

    file_system.crash();
    drop(primary_events);
    drop(analytics_events);
    drop(warehouse_events);
    drop(primary);
    drop(analytics);
    drop(warehouse);

    let reopened_runtime = TestRuntimeHandle::new(
        file_system.clone(),
        object_store.clone(),
        Arc::new(StubClock::default()),
        Arc::new(RateLimitedMutableBudgetScheduler { minimum_rate: 16 }),
        durability,
    );
    let reopened_primary = open_deployed_db_with_runtime(
        &deployment,
        "primary",
        "/execution-chaos-primary",
        &reopened_runtime,
    )
    .await;
    let reopened_analytics = open_deployed_db_with_runtime(
        &deployment,
        "analytics",
        "/execution-chaos-analytics",
        &reopened_runtime,
    )
    .await;
    let reopened_warehouse = open_deployed_db_with_runtime(
        &deployment,
        "warehouse",
        "/execution-chaos-warehouse",
        &reopened_runtime,
    )
    .await;

    let reopened_primary_events = reopened_primary.table("events");
    let reopened_analytics_events = reopened_analytics.table("events");
    let reopened_warehouse_events = reopened_warehouse.table("events");
    assert!(reopened_primary.try_table("audit_chaos").is_some());
    assert_eq!(
        reopened_primary_events
            .read(b"bootstrap".to_vec())
            .await
            .expect("read primary bootstrap"),
        Some(Value::bytes("primary"))
    );
    assert_eq!(
        reopened_analytics_events
            .read(b"bootstrap".to_vec())
            .await
            .expect("read analytics bootstrap"),
        Some(Value::bytes("analytics"))
    );
    assert_eq!(
        reopened_warehouse_events
            .read(b"bootstrap".to_vec())
            .await
            .expect("read warehouse bootstrap"),
        Some(Value::bytes("warehouse"))
    );
    assert_eq!(
        reopened_analytics_events
            .read(b"stalled".to_vec())
            .await
            .expect("read durable analytics row"),
        Some(Value::bytes(vec![b's'; 96]))
    );
    assert_eq!(
        reopened_primary_events
            .read(b"primary-tail".to_vec())
            .await
            .expect("read volatile primary tail"),
        None
    );
    assert_eq!(
        reopened_analytics_events
            .read(b"analytics-tail".to_vec())
            .await
            .expect("read volatile analytics tail"),
        None
    );
    assert_eq!(
        reopened_warehouse
            .execution_placement_report()
            .foreground
            .binding
            .domain,
        ExecutionDomainPath::new(["process", "shards", "warehouse", "foreground"])
    );
    assert_eq!(
        manager.snapshot().domains[&analytics_foreground]
            .spec
            .budget
            .memory
            .mutable_bytes,
        Some(32)
    );
}
