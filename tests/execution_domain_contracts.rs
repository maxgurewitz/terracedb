use std::{collections::BTreeMap, sync::Arc};

use futures::StreamExt;
use parking_lot::Mutex;
use terracedb::{
    CommitOptions, CompactionStrategy, Db, DbComponents, DbExecutionProfile, DbSettings,
    DomainBackgroundBudget, DomainCpuBudget, DomainIoBudget, DomainMemoryBudget, DomainTaggedWork,
    DurabilityClass, ExecutionDomainBacklogSnapshot, ExecutionDomainBudget,
    ExecutionDomainLifecycleEvent, ExecutionDomainLifecycleHook, ExecutionDomainOwner,
    ExecutionDomainPath, ExecutionDomainPlacement, ExecutionDomainSpec, ExecutionLaneBinding,
    ExecutionResourceKind, ExecutionResourceUsage, InMemoryResourceManager, NoopScheduler,
    PendingWork, PendingWorkType, ResourceManager, S3Location, ScanOptions, StubClock,
    StubFileSystem, StubObjectStore, StubRng, TableConfig, TableFormat, Value,
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
    DbComponents::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(17)),
    )
    .with_scheduler(Arc::new(NoopScheduler))
    .with_resource_manager(resource_manager)
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
