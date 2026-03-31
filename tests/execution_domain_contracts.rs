use std::{collections::BTreeMap, sync::Arc};

use futures::StreamExt;
use parking_lot::Mutex;
use terracedb::{
    CommitOptions, CompactionStrategy, Db, DbComponents, DbExecutionProfile, DbSettings,
    DomainTaggedWork, DurabilityClass, ExecutionDomainBudget, ExecutionDomainLifecycleEvent,
    ExecutionDomainLifecycleHook, ExecutionDomainOwner, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionDomainSpec, ExecutionLaneBinding, InMemoryResourceManager,
    NoopScheduler, PendingWork, PendingWorkType, ResourceManager, S3Location, ScanOptions,
    StubClock, StubFileSystem, StubObjectStore, StubRng, TableConfig, TableFormat, Value,
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
    resource_manager.register_domain(ExecutionDomainSpec {
        path: profile.background.domain.clone(),
        owner: ExecutionDomainOwner::Database {
            name: "compose-db".to_string(),
        },
        budget: ExecutionDomainBudget::default(),
        placement: ExecutionDomainPlacement::SharedWeighted { weight: 2 },
        metadata: BTreeMap::new(),
    });

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
        vec![ExecutionDomainLifecycleEvent::Registered]
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
    assert_eq!(snapshot.domains.len(), 1);

    let colocated = ColocatedDbWorkloadSpec {
        db_name: "compose-db".to_string(),
        execution_profile: profile,
        operation_count: 3,
        metadata: BTreeMap::from([("kind".to_string(), "compile-only".to_string())]),
    };
    assert_eq!(colocated.operation_count, 3);
}

#[tokio::test]
async fn fake_runtimes_can_tag_work_and_account_domain_budgets_deterministically() {
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
}

async fn run_logical_workload(path: &str, profile: DbExecutionProfile) -> Vec<(Vec<u8>, Vec<u8>)> {
    let resource_manager: Arc<dyn ResourceManager> = Arc::new(InMemoryResourceManager::default());
    let db = Db::builder()
        .settings(tiered_settings(path).with_execution_profile(profile))
        .components(stub_components(resource_manager))
        .open()
        .await
        .expect("open db for invariant test");
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
