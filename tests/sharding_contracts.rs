use std::{collections::BTreeMap, sync::Arc};

use terracedb::{
    AdmissionDiagnostics, AdmissionPressureLevel, BatchShardLocalityError,
    ColocatedDatabasePlacement, CommitId, CommitOptions, Db, DurabilityClass, ExecutionDomainOwner,
    ExecutionDomainPath, ExecutionLane, LogCursor, ObjectKeyLayout, PendingWork, PhysicalShardId,
    PublishShardMapError, ReshardPlanSkeleton, S3Location, ScheduleDecision, Scheduler,
    SequenceNumber, ShardChangeCursor, ShardCommitLaneId, ShardHashAlgorithm, ShardMapRevision,
    ShardMemtableOwner, ShardOpenRequest, ShardReadyPlacementLayout, ShardSstableOwnership,
    ShardingConfig, ShardingError, TableConfig, TableId, TableStats, Value,
    VirtualPartitionCoverage, VirtualPartitionId, WriteBatchShardingError,
};
use terracedb_simulation::SeededSimulationRunner;

#[cfg(any(test, feature = "test-support"))]
use terracedb::test_support::{row_table_config, test_dependencies, tiered_test_config};
use terracedb::{StubFileSystem, StubObjectStore};

#[derive(Default)]
struct ShardThrottleScheduler;

impl Scheduler for ShardThrottleScheduler {
    fn on_work_available(&self, _work: &[PendingWork]) -> Vec<ScheduleDecision> {
        Vec::new()
    }

    fn should_throttle(
        &self,
        _table: &terracedb::Table,
        _stats: &TableStats,
    ) -> terracedb::ThrottleDecision {
        terracedb::ThrottleDecision::default()
    }

    fn admission_diagnostics(
        &self,
        _table: &terracedb::Table,
        _stats: &TableStats,
        _signals: &terracedb::AdmissionSignals,
        _tag: &terracedb::WorkRuntimeTag,
        _domain_budget: Option<&terracedb::ExecutionDomainBudget>,
    ) -> Option<AdmissionDiagnostics> {
        Some(AdmissionDiagnostics {
            level: AdmissionPressureLevel::RateLimit,
            ..AdmissionDiagnostics::default()
        })
    }
}

fn sharded_row_table_config(name: &str) -> TableConfig {
    let mut config = row_table_config(name);
    config.sharding = ShardingConfig::hash(
        4,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(7),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("valid sharding config");
    config
}

fn find_key_for_shard(config: &ShardingConfig, target: PhysicalShardId, prefix: &str) -> Vec<u8> {
    for index in 0..10_000_u32 {
        let candidate = format!("{prefix}-{index}").into_bytes();
        let route = config.route_key(&candidate).expect("route candidate");
        if route.physical_shard == target {
            return candidate;
        }
    }
    panic!("failed to find key for shard {target}");
}

fn find_key_for_partition(
    config: &ShardingConfig,
    target: VirtualPartitionId,
    prefix: &str,
) -> Vec<u8> {
    for index in 0..10_000_u32 {
        let candidate = format!("{prefix}-{index}").into_bytes();
        let route = config.route_key(&candidate).expect("route candidate");
        if route.virtual_partition == target {
            return candidate;
        }
    }
    panic!("failed to find key for partition {target}");
}

async fn open_test_db(
    path: &str,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> Db {
    Db::open(
        tiered_test_config(path),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open test db")
}

#[tokio::test]
async fn shard_ready_tagging_uses_future_shard_paths_without_breaking_unsharded_fast_path() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let profile = ColocatedDatabasePlacement::shard_ready("warehouse").execution_profile();
    let layout = ShardReadyPlacementLayout::new("warehouse");
    let db = Db::open(
        tiered_test_config("/sharding-contracts-shard-tagging"),
        test_dependencies(file_system, object_store)
            .with_execution_profile(profile.clone())
            .with_execution_identity("warehouse"),
    )
    .await
    .expect("open shard-ready db");

    let background = db.tag_shard_background_work("flush", PhysicalShardId::new(3));
    assert_eq!(
        background.tag.owner,
        ExecutionDomainOwner::Shard {
            database: "warehouse".to_string(),
            shard: "0003".to_string(),
        }
    );
    assert_eq!(
        background.tag.domain,
        layout.future_shard_lane_path("0003", ExecutionLane::UserBackground)
    );
    assert_eq!(background.tag.durability_class, DurabilityClass::UserData);

    let control = db.tag_shard_control_plane_work("recovery", PhysicalShardId::new(3));
    assert_eq!(
        control.tag.domain,
        layout.future_shard_lane_path("0003", ExecutionLane::ControlPlane)
    );
    assert_eq!(control.tag.durability_class, DurabilityClass::ControlPlane);

    let compat = db.tag_shard_background_work("compat", PhysicalShardId::UNSHARDED);
    assert_eq!(compat.tag.owner, layout.database_owner());
    assert_eq!(compat.tag.domain, profile.background.domain);
    assert_eq!(compat.tag.durability_class, DurabilityClass::UserData);
}

#[tokio::test]
async fn shard_scoped_write_throttling_is_published_by_physical_shard() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let profile = ColocatedDatabasePlacement::shard_ready("warehouse").execution_profile();
    let layout = ShardReadyPlacementLayout::new("warehouse");
    let db = Db::open(
        terracedb::DbConfig {
            scheduler: Some(Arc::new(ShardThrottleScheduler)),
            ..tiered_test_config("/sharding-contracts-shard-throttle")
        },
        test_dependencies(file_system, object_store)
            .with_execution_profile(profile)
            .with_execution_identity("warehouse"),
    )
    .await
    .expect("open shard-aware throttle db");

    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");
    let shard_one = find_key_for_shard(
        &table.sharding_state().expect("table sharding").config,
        PhysicalShardId::new(1),
        "hot",
    );

    table
        .write(shard_one, Value::bytes("payload"))
        .await
        .expect("single-shard write should succeed");

    let snapshot = db.scheduler_observability_snapshot();
    assert_eq!(
        snapshot
            .throttled_writes_by_physical_shard
            .get(&PhysicalShardId::new(1)),
        Some(&1)
    );
    assert_eq!(
        snapshot
            .throttled_writes_by_domain
            .get(&layout.future_shard_lane_path("0001", ExecutionLane::UserForeground)),
        Some(&1)
    );

    let current = snapshot
        .current_admission_diagnostics_by_physical_shard
        .get(&PhysicalShardId::new(1))
        .expect("current shard admission should be published");
    assert_eq!(current.diagnostics.level, AdmissionPressureLevel::RateLimit);
    assert_eq!(
        current
            .diagnostics
            .metadata
            .get(terracedb::telemetry_attrs::PHYSICAL_SHARD)
            .and_then(serde_json::Value::as_str),
        Some("0001")
    );
}

#[tokio::test]
async fn sharding_state_survives_reopen_and_reports_partition_counts() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/sharding-contracts-reopen";
    let config = sharded_row_table_config("orders");

    let db = open_test_db(path, file_system.clone(), object_store.clone()).await;
    let table = db
        .create_table(config.clone())
        .await
        .expect("create sharded table");

    let initial = table.sharding_state().expect("table sharding state");
    assert_eq!(initial.config, config.sharding);
    assert_eq!(initial.current_revision(), ShardMapRevision::new(7));
    assert_eq!(
        initial.partition_counts_per_shard(),
        BTreeMap::from([(PhysicalShardId::new(0), 2), (PhysicalShardId::new(1), 2),])
    );

    drop(db);

    let reopened = open_test_db(path, file_system, object_store).await;
    let reopened_state = reopened
        .table("orders")
        .sharding_state()
        .expect("reopened table sharding state");
    assert_eq!(reopened_state, initial);
}

#[tokio::test]
async fn unsharded_tables_default_to_shard_zero_and_publish_noop_maps() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_test_db(
        "/sharding-contracts-unsharded-default",
        file_system,
        object_store,
    )
    .await;
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create unsharded table");

    let initial = table.sharding_state().expect("unsharded state");
    assert!(!initial.is_sharded());
    assert_eq!(initial.current_revision(), ShardMapRevision::new(0));
    assert_eq!(initial.virtual_partition_count(), 1);
    assert_eq!(
        initial.shard_assignments(),
        vec![terracedb::ShardAssignment {
            virtual_partition: VirtualPartitionId::new(0),
            physical_shard: PhysicalShardId::UNSHARDED,
        }]
    );
    assert_eq!(
        initial.partition_counts_per_shard(),
        BTreeMap::from([(PhysicalShardId::UNSHARDED, 1)])
    );

    let routed = table.route_key(b"event-1").expect("route unsharded key");
    assert_eq!(routed.virtual_partition, VirtualPartitionId::new(0));
    assert_eq!(routed.physical_shard, PhysicalShardId::UNSHARDED);

    let validated = table
        .validate_shard_map(&ShardingConfig::unsharded())
        .expect("validate unsharded noop");
    assert_eq!(validated, initial);

    let published = table
        .publish_shard_map(ShardingConfig::unsharded())
        .await
        .expect("publish unsharded noop");
    assert_eq!(published, initial);
}

#[test]
fn routing_is_stable_and_independent_of_execution_domain_placement() {
    let config = sharded_row_table_config("orders");
    let shard_one = find_key_for_shard(&config.sharding, PhysicalShardId::new(1), "user");
    let route = config.sharding.route_key(&shard_one).expect("route");

    for _ in 0..16 {
        assert_eq!(
            config.sharding.route_key(&shard_one).expect("reroute"),
            route
        );
    }

    let commit_id = CommitId::with_shard_hint(SequenceNumber::new(44), route.physical_shard);
    let decoded = CommitId::decode(&commit_id.encode()).expect("decode commit id");
    assert_eq!(decoded.sequence(), SequenceNumber::new(44));
    assert_eq!(decoded.physical_shard_hint(), route.physical_shard);

    let layout = ShardReadyPlacementLayout::new("warehouse");
    let placement = route.physical_shard.execution_placement(&layout);
    assert_eq!(
        placement.owner,
        ExecutionDomainOwner::Shard {
            database: "warehouse".to_string(),
            shard: route.physical_shard.to_string(),
        }
    );
    assert_eq!(
        placement.foreground,
        ExecutionDomainPath::new([
            "process",
            "shards",
            "warehouse",
            "shards",
            route.physical_shard.to_string().as_str(),
            "foreground",
        ])
    );

    assert_eq!(
        config.sharding.route_key(&shard_one).expect("route again"),
        route
    );
}

#[tokio::test]
async fn batch_planning_fails_closed_for_cross_shard_single_table_batches_only() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_test_db(
        "/sharding-contracts-batch-validation",
        file_system,
        object_store,
    )
    .await;

    let sharded_config = sharded_row_table_config("orders");
    let shard_zero_key = find_key_for_shard(&sharded_config.sharding, PhysicalShardId::new(0), "k");
    let shard_one_key = find_key_for_shard(&sharded_config.sharding, PhysicalShardId::new(1), "k");
    let second_shard_zero_key =
        find_key_for_shard(&sharded_config.sharding, PhysicalShardId::new(0), "other");

    let sharded = db
        .create_table(sharded_config)
        .await
        .expect("create sharded table");
    let unsharded = db
        .create_table(row_table_config("events"))
        .await
        .expect("create unsharded table");

    let mut illegal = db.write_batch();
    illegal.put(&sharded, shard_zero_key.clone(), Value::bytes("left"));
    illegal.put(&sharded, shard_one_key.clone(), Value::bytes("right"));

    let planned = db
        .explain_write_batch_sharding(&illegal)
        .expect_err("cross-shard single-table batch should fail closed");
    assert_eq!(
        planned,
        WriteBatchShardingError::Locality(BatchShardLocalityError {
            table_name: "orders".to_string(),
            first_shard: PhysicalShardId::new(0),
            conflicting_shard: PhysicalShardId::new(1),
            first_virtual_partition: config_route_partition(&sharded, &shard_zero_key),
            conflicting_virtual_partition: config_route_partition(&sharded, &shard_one_key),
        })
    );

    let commit_error = db
        .commit(illegal, CommitOptions::default())
        .await
        .expect_err("commit should surface shard-locality failure");
    assert!(
        commit_error
            .to_string()
            .contains("write batch for table orders spans physical shards 0000 and 0001")
    );

    let mut legal_single_shard = db.write_batch();
    legal_single_shard.put(&sharded, shard_zero_key.clone(), Value::bytes("a"));
    legal_single_shard.put(&sharded, second_shard_zero_key, Value::bytes("b"));
    let plan = db
        .explain_write_batch_sharding(&legal_single_shard)
        .expect("single-shard batch plan");
    assert_eq!(plan.tables.len(), 1);
    assert_eq!(
        plan.table("orders").expect("orders plan").single_shard(),
        Some(PhysicalShardId::new(0))
    );
    assert_eq!(plan.commit_shard_hint(), Some(PhysicalShardId::new(0)));
    db.commit(legal_single_shard, CommitOptions::default())
        .await
        .expect("single-shard batch should commit");

    let mut legal_multi_table = db.write_batch();
    legal_multi_table.put(&sharded, shard_one_key, Value::bytes("sharded"));
    legal_multi_table.put(&unsharded, b"event-1".to_vec(), Value::bytes("event"));
    let mixed_plan = db
        .explain_write_batch_sharding(&legal_multi_table)
        .expect("mixed-table batch plan");
    assert_eq!(mixed_plan.tables.len(), 2);
    assert_eq!(mixed_plan.commit_shard_hint(), None);
    db.commit(legal_multi_table, CommitOptions::default())
        .await
        .expect("mixed-table batch should still commit");
}

#[tokio::test]
async fn shard_map_publication_updates_routing_and_survives_reopen() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/sharding-contracts-publish";
    let db = open_test_db(path, file_system.clone(), object_store.clone()).await;
    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");

    let target = ShardingConfig::hash(
        4,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(8),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("target sharding");
    let moved_key = find_key_for_partition(
        &table.sharding_state().expect("state").config,
        VirtualPartitionId::new(1),
        "publish",
    );

    let initial_route = table.route_key(&moved_key).expect("initial route");
    assert_eq!(initial_route.virtual_partition, VirtualPartitionId::new(1));
    assert_eq!(initial_route.physical_shard, PhysicalShardId::new(0));

    let preview = table
        .validate_shard_map(&target)
        .expect("validate shard-map publication");
    assert_eq!(preview.current_revision(), ShardMapRevision::new(8));
    assert_eq!(
        preview.partition_counts_per_shard(),
        BTreeMap::from([(PhysicalShardId::new(0), 1), (PhysicalShardId::new(1), 3)])
    );
    assert_eq!(
        table
            .sharding_state()
            .expect("state after preview")
            .current_revision(),
        ShardMapRevision::new(7)
    );

    let published = table
        .publish_shard_map(target)
        .await
        .expect("publish shard map");
    assert_eq!(published, preview);

    let updated_route = table.route_key(&moved_key).expect("updated route");
    assert_eq!(updated_route.virtual_partition, VirtualPartitionId::new(1));
    assert_eq!(updated_route.physical_shard, PhysicalShardId::new(1));
    assert_eq!(updated_route.shard_map_revision, ShardMapRevision::new(8));

    drop(db);

    let reopened = open_test_db(path, file_system, object_store).await;
    let reopened_table = reopened.table("orders");
    let reopened_state = reopened_table
        .sharding_state()
        .expect("reopened sharding state");
    assert_eq!(reopened_state, published);
    assert_eq!(
        reopened_table
            .route_key(&moved_key)
            .expect("reopened route"),
        updated_route
    );
}

#[tokio::test]
async fn shard_map_publication_rejects_identity_changes_and_stale_revisions() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_test_db(
        "/sharding-contracts-invalid-publish",
        file_system,
        object_store,
    )
    .await;
    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");
    let initial = table.sharding_state().expect("initial state");

    let stale_revision = ShardingConfig::hash(
        4,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(7),
        vec![
            PhysicalShardId::new(1),
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(0),
        ],
    )
    .expect("stale revision config");
    let stale_error = table
        .validate_shard_map(&stale_revision)
        .expect_err("stale shard-map revision should fail");
    assert_eq!(
        stale_error,
        PublishShardMapError::InvalidShardMap {
            table_name: "orders".to_string(),
            error: ShardingError::ShardMapRevisionNotIncreasing {
                current_revision: ShardMapRevision::new(7),
                published_revision: ShardMapRevision::new(7),
            },
        }
    );

    let incompatible = ShardingConfig::hash(
        8,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(8),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
            PhysicalShardId::new(0),
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("incompatible config");
    let publish_error = table
        .publish_shard_map(incompatible)
        .await
        .expect_err("incompatible shard-map identity should fail");
    assert_eq!(
        publish_error,
        PublishShardMapError::InvalidShardMap {
            table_name: "orders".to_string(),
            error: ShardingError::IncompatibleReshardIdentity {
                source_virtual_partition_count: 4,
                target_virtual_partition_count: 8,
                source_hash_algorithm: Some(ShardHashAlgorithm::Crc32),
                target_hash_algorithm: Some(ShardHashAlgorithm::Crc32),
            },
        }
    );

    assert_eq!(
        table.sharding_state().expect("state after rejection"),
        initial
    );
}

#[tokio::test]
async fn shard_map_publication_requires_cutover_once_table_contains_data() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/sharding-contracts-publish-with-data";
    let db = open_test_db(path, file_system.clone(), object_store.clone()).await;
    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");
    let key = find_key_for_partition(
        &table.sharding_state().expect("initial state").config,
        VirtualPartitionId::new(1),
        "existing-row",
    );

    table
        .write(key.clone(), Value::bytes("before"))
        .await
        .expect("write row before publication");

    let moved_assignments = ShardingConfig::hash(
        4,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(8),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("moved assignments");
    let publish_error = table
        .publish_shard_map(moved_assignments)
        .await
        .expect_err("publishing moved assignments with data should fail closed");
    assert_eq!(
        publish_error,
        PublishShardMapError::DataMovementRequired {
            table_name: "orders".to_string(),
            current_revision: ShardMapRevision::new(7),
            published_revision: ShardMapRevision::new(8),
        }
    );

    assert_eq!(
        table
            .read(key.clone())
            .await
            .expect("read after blocked publication"),
        Some(Value::bytes("before"))
    );
    assert_eq!(
        table
            .sharding_state()
            .expect("state after blocked publication")
            .current_revision(),
        ShardMapRevision::new(7)
    );

    let revision_only = ShardingConfig::hash(
        4,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(8),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("revision-only config");
    table
        .publish_shard_map(revision_only)
        .await
        .expect("revision-only publication should stay allowed");
    assert_eq!(
        table
            .read(key.clone())
            .await
            .expect("read after revision bump"),
        Some(Value::bytes("before"))
    );

    drop(db);

    let reopened = open_test_db(path, file_system, object_store).await;
    let reopened_table = reopened.table("orders");
    assert_eq!(
        reopened_table
            .read(key)
            .await
            .expect("read after reopen with revision bump"),
        Some(Value::bytes("before"))
    );
    assert_eq!(
        reopened_table
            .sharding_state()
            .expect("reopened state after revision bump")
            .current_revision(),
        ShardMapRevision::new(8)
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ShardingSimulationOutcome {
    seed: u64,
    initial_route: (VirtualPartitionId, PhysicalShardId, ShardMapRevision),
    published_route: (VirtualPartitionId, PhysicalShardId, ShardMapRevision),
    published_partition_counts: BTreeMap<PhysicalShardId, usize>,
    mixed_commit_hint: Option<PhysicalShardId>,
}

fn run_sharding_simulation(seed: u64) -> turmoil::Result<ShardingSimulationOutcome> {
    SeededSimulationRunner::new(seed).run_with(move |context| async move {
        let path = format!("/terracedb/sim/sharding-{seed}");
        let db = Db::open(tiered_test_config(&path), context.dependencies())
            .await
            .expect("open simulated db");
        let orders = db
            .create_table(sharded_row_table_config("orders"))
            .await
            .expect("create sharded table");
        let audit = db
            .create_table(row_table_config("audit"))
            .await
            .expect("create unsharded audit table");

        let key = find_key_for_partition(
            &orders.sharding_state().expect("initial state").config,
            VirtualPartitionId::new(1),
            &format!("seed-{seed}"),
        );
        let initial_route = orders.route_key(&key).expect("initial route");

        let mut mixed = db.write_batch();
        mixed.put(&orders, key.clone(), Value::bytes("order"));
        mixed.put(
            &audit,
            format!("audit-{seed}").into_bytes(),
            Value::bytes("audit"),
        );
        let mixed_plan = db
            .explain_write_batch_sharding(&mixed)
            .expect("mixed-table batch plan");

        orders
            .publish_shard_map(
                ShardingConfig::hash(
                    4,
                    ShardHashAlgorithm::Crc32,
                    ShardMapRevision::new(8),
                    vec![
                        PhysicalShardId::new(0),
                        PhysicalShardId::new(1),
                        PhysicalShardId::new(1),
                        PhysicalShardId::new(1),
                    ],
                )
                .expect("target sharding"),
            )
            .await
            .expect("publish simulated shard map");
        drop(db);

        let reopened = Db::open(tiered_test_config(&path), context.dependencies())
            .await
            .expect("reopen simulated db");
        let reopened_orders = reopened.table("orders");
        let published_state = reopened_orders.sharding_state().expect("published state");
        let published_route = reopened_orders.route_key(&key).expect("published route");

        Ok(ShardingSimulationOutcome {
            seed,
            initial_route: (
                initial_route.virtual_partition,
                initial_route.physical_shard,
                initial_route.shard_map_revision,
            ),
            published_route: (
                published_route.virtual_partition,
                published_route.physical_shard,
                published_route.shard_map_revision,
            ),
            published_partition_counts: published_state.partition_counts_per_shard(),
            mixed_commit_hint: mixed_plan.commit_shard_hint(),
        })
    })
}

#[test]
fn sharding_simulation_replays_same_seed_routing_and_restart_behavior() -> turmoil::Result {
    let first = run_sharding_simulation(0x78_01)?;
    let second = run_sharding_simulation(0x78_01)?;

    assert_eq!(first, second);
    assert_eq!(
        first.initial_route,
        (
            VirtualPartitionId::new(1),
            PhysicalShardId::new(0),
            ShardMapRevision::new(7)
        )
    );
    assert_eq!(
        first.published_route,
        (
            VirtualPartitionId::new(1),
            PhysicalShardId::new(1),
            ShardMapRevision::new(8)
        )
    );
    assert_eq!(
        first.published_partition_counts,
        BTreeMap::from([(PhysicalShardId::new(0), 1), (PhysicalShardId::new(1), 3)])
    );
    assert_eq!(first.mixed_commit_hint, Some(PhysicalShardId::new(0)));

    Ok(())
}

#[test]
fn shard_local_contract_types_and_reshard_skeleton_are_available() {
    let source = sharded_row_table_config("orders").sharding;
    let target = ShardingConfig::hash(
        4,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(8),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("target sharding");

    let plan =
        ReshardPlanSkeleton::build(TableId::new(9), "orders", &source, &target).expect("plan");
    assert_eq!(plan.source_revision, ShardMapRevision::new(7));
    assert_eq!(plan.target_revision, ShardMapRevision::new(8));
    assert_eq!(plan.moves.len(), 1);
    assert_eq!(plan.moves[0].virtual_partition, VirtualPartitionId::new(1));
    assert_eq!(plan.moves[0].from_physical_shard, PhysicalShardId::new(0));
    assert_eq!(plan.moves[0].to_physical_shard, PhysicalShardId::new(1));

    let shard = PhysicalShardId::new(3);
    let lane = ShardCommitLaneId::new(TableId::new(9), shard);
    let memtable = ShardMemtableOwner::new(TableId::new(9), shard, ShardMapRevision::new(8));
    let sstable = ShardSstableOwnership::new(
        TableId::new(9),
        shard,
        ShardMapRevision::new(8),
        VirtualPartitionCoverage::single(VirtualPartitionId::new(1)),
    );
    let open = ShardOpenRequest::new(TableId::new(9), shard, ShardMapRevision::new(8));
    let cursor = ShardChangeCursor::new(
        TableId::new(9),
        shard,
        CommitId::with_shard_hint(SequenceNumber::new(10), shard),
        LogCursor::new(SequenceNumber::new(10), 2),
    );
    assert_eq!(lane.table_id, TableId::new(9));
    assert_eq!(memtable.physical_shard, shard);
    assert_eq!(
        sstable.virtual_partitions,
        VirtualPartitionCoverage::single(VirtualPartitionId::new(1))
    );
    assert_eq!(open.shard_map_revision, ShardMapRevision::new(8));
    assert_eq!(cursor.commit_id.physical_shard_hint(), shard);

    let layout = ObjectKeyLayout::new(&S3Location {
        bucket: "bucket".to_string(),
        prefix: "tenant-a/db-01".to_string(),
    });
    assert_eq!(
        layout.backup_sstable_in_shard(TableId::new(9), shard, "SST-000123"),
        "tenant-a/db-01/backup/sst/table-000009/0003/SST-000123.sst"
    );
    assert_eq!(
        layout.cold_sstable_in_shard(
            TableId::new(9),
            shard,
            SequenceNumber::new(44),
            SequenceNumber::new(88),
            "SST-000123",
        ),
        "tenant-a/db-01/cold/table-000009/0003/00000000000000000044-00000000000000000088/SST-000123.sst"
    );
}

fn config_route_partition(table: &terracedb::Table, key: &[u8]) -> VirtualPartitionId {
    table
        .route_key(key)
        .expect("route through public table helper")
        .virtual_partition
}
