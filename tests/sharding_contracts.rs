use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::StreamExt;
use terracedb::{
    AdmissionDiagnostics, AdmissionPressureLevel, BatchShardLocalityError,
    ColocatedDatabasePlacement, CommitError, CommitId, CommitOptions, Db, DurabilityClass,
    ExecutionDomainBacklogSnapshot, ExecutionDomainOwner, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionLane, ExecutionResourceUsage, FailpointMode, FileSystem,
    LogCursor, ObjectKeyLayout, OpenOptions, PendingWork, PhysicalShardId, PublishShardMapError,
    ReshardPlanError, ReshardPlanPhase, ReshardPlanSkeleton, S3Location, ScanOptions,
    ScheduleDecision, Scheduler, SequenceNumber, ShardChangeCursor, ShardCommitLaneId,
    ShardHashAlgorithm, ShardMapRevision, ShardMemtableOwner, ShardOpenRequest,
    ShardReadyPlacementLayout, ShardSstableOwnership, ShardingConfig, ShardingError,
    StorageErrorKind, StubClock, TableConfig, TableId, TableStats, Value, VirtualPartitionCoverage,
    VirtualPartitionId, WriteBatchShardingError,
};
use terracedb_simulation::SeededSimulationRunner;

#[cfg(any(test, feature = "test-support"))]
use terracedb::test_support::{
    ClockProgressProbe, row_table_config, test_dependencies, test_dependencies_with_clock,
    tiered_test_config,
};
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

fn base_sharding(revision: u64) -> ShardingConfig {
    ShardingConfig::hash(
        4,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(revision),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("valid base sharding config")
}

fn sharded_row_table_config(name: &str) -> TableConfig {
    let mut config = row_table_config(name);
    config.sharding = base_sharding(7);
    config
}

fn moved_partition_one_sharding() -> ShardingConfig {
    moved_partition_one_sharding_with_revision(8)
}

fn moved_partition_one_sharding_with_revision(revision: u64) -> ShardingConfig {
    ShardingConfig::hash(
        4,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(revision),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("valid target sharding")
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

async fn read_all_file(file_system: &StubFileSystem, path: &str) -> Vec<u8> {
    let handle = file_system
        .open(
            path,
            OpenOptions {
                create: false,
                read: true,
                write: false,
                truncate: false,
                append: false,
            },
        )
        .await
        .expect("open file for read");
    let mut offset = 0_u64;
    let mut bytes = Vec::new();
    loop {
        let chunk = file_system
            .read_at(&handle, offset, 8 * 1024)
            .await
            .expect("read file chunk");
        if chunk.is_empty() {
            break;
        }
        offset = offset.saturating_add(chunk.len() as u64);
        bytes.extend(chunk);
    }
    bytes
}

fn sstable_dir_in_shard(root: &str, table_id: TableId, shard: PhysicalShardId) -> String {
    let shard_dir = if shard == PhysicalShardId::UNSHARDED {
        "0000".to_string()
    } else {
        shard.to_string()
    };
    format!("{root}/sst/table-{:06}/{shard_dir}", table_id.get())
}

async fn scan_string_rows(db: &Db, table_name: &str) -> BTreeMap<String, String> {
    let table = db.table(table_name);
    let snapshot = db.snapshot().await;
    let rows = snapshot
        .scan(&table, Vec::new(), vec![0xff], ScanOptions::default())
        .await
        .expect("scan string rows")
        .map(|(key, value)| match value {
            Value::Bytes(bytes) => (
                String::from_utf8(key).expect("utf8 test key"),
                String::from_utf8(bytes).expect("utf8 test value"),
            ),
            Value::Record(_) => panic!("sharding contract tests only use byte rows"),
        })
        .collect::<Vec<_>>()
        .await;
    snapshot.release();
    rows.into_iter().collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RouteSnapshot {
    virtual_partition: VirtualPartitionId,
    physical_shard: PhysicalShardId,
    shard_map_revision: ShardMapRevision,
}

fn route_snapshot(table: &terracedb::Table, key: &[u8]) -> RouteSnapshot {
    let route = table.route_key(key).expect("route key");
    RouteSnapshot {
        virtual_partition: route.virtual_partition,
        physical_shard: route.physical_shard,
        shard_map_revision: route.shard_map_revision,
    }
}

fn assert_route_stable(table: &terracedb::Table, key: &[u8], expected: &RouteSnapshot) {
    for _ in 0..8 {
        assert_eq!(route_snapshot(table, key), *expected);
    }
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

#[tokio::test]
async fn reshard_plan_moves_live_artifacts_without_rewriting_row_bytes() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/sharding-contracts-reshard-success";
    let db = open_test_db(path, file_system.clone(), object_store.clone()).await;
    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");
    let table_id = table.id().expect("table id");
    let key = find_key_for_partition(
        &table.sharding_state().expect("initial state").config,
        VirtualPartitionId::new(1),
        "move-me",
    );
    let source_route = table.route_key(&key).expect("source route");

    table
        .write(key.clone(), Value::bytes("before"))
        .await
        .expect("write row before reshard");
    db.flush().await.expect("flush source artifact");

    let source_dir = sstable_dir_in_shard(path, table_id, source_route.physical_shard);
    let source_paths = file_system
        .list(&source_dir)
        .await
        .expect("list source shard files");
    assert_eq!(source_paths.len(), 1);
    let source_bytes = read_all_file(file_system.as_ref(), &source_paths[0]).await;

    let completed = table
        .reshard_to(moved_partition_one_sharding())
        .await
        .expect("run reshard plan");
    assert_eq!(completed.phase, ReshardPlanPhase::Completed);
    assert_eq!(completed.published_revision, Some(ShardMapRevision::new(8)));
    assert_eq!(completed.artifacts_total, 1);
    assert_eq!(completed.artifacts_moved, 1);

    let moved_route = table.route_key(&key).expect("moved route");
    assert_eq!(
        moved_route.virtual_partition,
        source_route.virtual_partition
    );
    assert_eq!(moved_route.physical_shard, PhysicalShardId::new(1));
    assert_eq!(moved_route.shard_map_revision, ShardMapRevision::new(8));
    assert_eq!(
        table.read(key.clone()).await.expect("read after reshard"),
        Some(Value::bytes("before"))
    );

    assert!(
        file_system
            .list(&source_dir)
            .await
            .expect("list source shard after cleanup")
            .is_empty()
    );
    let target_dir = sstable_dir_in_shard(path, table_id, moved_route.physical_shard);
    let target_paths = file_system
        .list(&target_dir)
        .await
        .expect("list target shard files");
    assert_eq!(target_paths.len(), 1);
    let target_bytes = read_all_file(file_system.as_ref(), &target_paths[0]).await;
    assert_eq!(target_bytes, source_bytes);

    drop(db);

    let reopened = open_test_db(path, file_system, object_store).await;
    let reopened_table = reopened.table("orders");
    assert_eq!(
        reopened_table
            .read(key)
            .await
            .expect("read moved value after reopen"),
        Some(Value::bytes("before"))
    );
    assert_eq!(
        reopened_table
            .resharding_state()
            .expect("persisted completed reshard state")
            .phase,
        ReshardPlanPhase::Completed
    );
}

#[tokio::test]
async fn reshard_plan_can_be_aborted_before_cutover_begins() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_test_db(
        "/sharding-contracts-reshard-abort",
        file_system,
        object_store,
    )
    .await;
    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");
    let key = find_key_for_partition(
        &table.sharding_state().expect("initial state").config,
        VirtualPartitionId::new(1),
        "abort-me",
    );

    let planned = table
        .create_reshard_plan(moved_partition_one_sharding())
        .await
        .expect("create reshard plan");
    assert_eq!(planned.phase, ReshardPlanPhase::Planned);

    let aborted = table
        .abort_reshard_plan()
        .await
        .expect("abort planned reshard");
    assert_eq!(aborted.phase, ReshardPlanPhase::Aborted);
    assert!(aborted.paused_partitions.is_empty());
    assert_eq!(
        table
            .route_key(&key)
            .expect("route after abort")
            .physical_shard,
        PhysicalShardId::new(0)
    );

    table
        .write(key.clone(), Value::bytes("after-abort"))
        .await
        .expect("writes should resume after abort");
    assert_eq!(
        table.read(key).await.expect("read after abort"),
        Some(Value::bytes("after-abort"))
    );
}

#[tokio::test]
async fn reshard_plan_fails_closed_when_a_compacted_artifact_would_need_rewriting() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_test_db(
        "/sharding-contracts-reshard-no-rewrite-fail",
        file_system,
        object_store,
    )
    .await;
    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");
    let initial = table.sharding_state().expect("initial state");
    let key_zero = find_key_for_partition(&initial.config, VirtualPartitionId::new(0), "left");
    let key_one = find_key_for_partition(&initial.config, VirtualPartitionId::new(1), "right");

    table
        .write(key_zero.clone(), Value::bytes("v0"))
        .await
        .expect("write partition zero");
    db.flush().await.expect("flush partition zero");
    table
        .write(key_one.clone(), Value::bytes("v1"))
        .await
        .expect("write partition one");
    db.flush().await.expect("flush partition one");
    assert!(db.run_next_compaction().await.expect("run compaction"));

    table
        .create_reshard_plan(moved_partition_one_sharding())
        .await
        .expect("create reshard plan");
    let error = table
        .run_reshard_plan()
        .await
        .expect_err("mixed-coverage compaction output should fail closed");
    assert!(matches!(
        error,
        ReshardPlanError::ArtifactRewriteRequired { .. }
    ));

    let failed = table
        .resharding_state()
        .expect("persisted failed reshard state");
    assert_eq!(failed.phase, ReshardPlanPhase::Copying);
    assert!(
        failed
            .failure
            .as_deref()
            .is_some_and(|message| message.contains("without rewriting bytes"))
    );

    let blocked = table
        .write(key_zero.clone(), Value::bytes("blocked"))
        .await
        .expect_err("failed reshard should keep writes paused until resolved");
    match blocked {
        terracedb::WriteError::Commit(CommitError::Storage(error)) => {
            assert_eq!(error.kind(), StorageErrorKind::Timeout);
            assert!(error.message().contains("paused for resharding"));
        }
        other => panic!("expected paused write timeout, got {other:?}"),
    }

    let aborted = table
        .abort_reshard_plan()
        .await
        .expect("abort failed copy-phase reshard");
    assert_eq!(aborted.phase, ReshardPlanPhase::Aborted);
    table
        .write(key_zero.clone(), Value::bytes("after-abort"))
        .await
        .expect("writes should resume after abort");
    assert_eq!(
        table.read(key_zero).await.expect("read after abort"),
        Some(Value::bytes("after-abort"))
    );
}

#[tokio::test]
async fn reshard_plan_resumes_after_restart_from_manifest_installed_phase() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/sharding-contracts-reshard-restart";
    let db = open_test_db(path, file_system.clone(), object_store.clone()).await;
    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");
    let key = find_key_for_partition(
        &table.sharding_state().expect("initial state").config,
        VirtualPartitionId::new(1),
        "restart-me",
    );

    table
        .write(key.clone(), Value::bytes("before"))
        .await
        .expect("write before restart");
    db.flush().await.expect("flush before restart");
    table
        .create_reshard_plan(moved_partition_one_sharding())
        .await
        .expect("create reshard plan");

    let _fail = db.__failpoint_registry().arm_timeout(
        terracedb::failpoints::names::DB_RESHARD_MANIFEST_INSTALLED,
        "interrupt after manifest install",
        FailpointMode::Once,
    );
    let interrupted = table
        .run_reshard_plan()
        .await
        .expect_err("manifest-installed failpoint should interrupt the plan");
    match interrupted {
        ReshardPlanError::Storage(error) => {
            assert_eq!(error.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("expected storage timeout, got {other:?}"),
    }

    let interrupted_state = table
        .resharding_state()
        .expect("manifest-installed state should persist");
    assert_eq!(interrupted_state.phase, ReshardPlanPhase::ManifestInstalled);
    assert!(interrupted_state.failure.is_some());
    assert_eq!(
        table
            .route_key(&key)
            .expect("route before resume")
            .physical_shard,
        PhysicalShardId::new(0)
    );
    assert_eq!(
        table.read(key.clone()).await.expect("read before resume"),
        Some(Value::bytes("before"))
    );

    let blocked = table
        .write(key.clone(), Value::bytes("blocked"))
        .await
        .expect_err("writes should remain paused while the plan is incomplete");
    match blocked {
        terracedb::WriteError::Commit(CommitError::Storage(error)) => {
            assert_eq!(error.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("expected paused write timeout, got {other:?}"),
    }

    drop(db);

    let reopened = open_test_db(path, file_system, object_store).await;
    let reopened_table = reopened.table("orders");
    assert_eq!(
        reopened_table
            .resharding_state()
            .expect("reopened reshard state")
            .phase,
        ReshardPlanPhase::ManifestInstalled
    );
    assert_eq!(
        reopened_table
            .read(key.clone())
            .await
            .expect("read across restart"),
        Some(Value::bytes("before"))
    );

    let completed = reopened_table
        .run_reshard_plan()
        .await
        .expect("resume reshard after restart");
    assert_eq!(completed.phase, ReshardPlanPhase::Completed);
    assert_eq!(
        reopened_table
            .route_key(&key)
            .expect("route after resume")
            .physical_shard,
        PhysicalShardId::new(1)
    );
    assert_eq!(
        reopened_table
            .read(key)
            .await
            .expect("read after resumed reshard"),
        Some(Value::bytes("before"))
    );
}

#[tokio::test]
async fn reshard_plan_resumes_after_restart_from_revision_published_phase() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/sharding-contracts-reshard-restart-revision-published";
    let db = open_test_db(path, file_system.clone(), object_store.clone()).await;
    let table = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create sharded table");
    let table_id = table.id().expect("table id");
    let key = find_key_for_partition(
        &table.sharding_state().expect("initial state").config,
        VirtualPartitionId::new(1),
        "restart-after-publish",
    );
    let source_route = table.route_key(&key).expect("source route");

    table
        .write(key.clone(), Value::bytes("before"))
        .await
        .expect("write before restart");
    db.flush().await.expect("flush before restart");
    table
        .create_reshard_plan(moved_partition_one_sharding())
        .await
        .expect("create reshard plan");

    let source_dir = sstable_dir_in_shard(path, table_id, source_route.physical_shard);
    assert_eq!(
        file_system
            .list(&source_dir)
            .await
            .expect("list source shard before reshard")
            .len(),
        1
    );

    let _fail = db.__failpoint_registry().arm_timeout(
        terracedb::failpoints::names::DB_RESHARD_REVISION_PUBLISHED,
        "interrupt after revision publish",
        FailpointMode::Once,
    );
    let interrupted = table
        .run_reshard_plan()
        .await
        .expect_err("revision-published failpoint should interrupt the plan");
    match interrupted {
        ReshardPlanError::Storage(error) => {
            assert_eq!(error.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("expected storage timeout, got {other:?}"),
    }

    let interrupted_state = table
        .resharding_state()
        .expect("revision-published state should persist");
    assert_eq!(interrupted_state.phase, ReshardPlanPhase::RevisionPublished);
    assert!(interrupted_state.failure.is_some());
    assert_eq!(
        table
            .route_key(&key)
            .expect("route after published revision")
            .physical_shard,
        PhysicalShardId::new(1)
    );
    assert_eq!(
        file_system
            .list(&source_dir)
            .await
            .expect("source cleanup should still be pending")
            .len(),
        1
    );

    drop(db);

    let reopened = open_test_db(path, file_system.clone(), object_store).await;
    let reopened_table = reopened.table("orders");
    assert_eq!(
        reopened_table
            .resharding_state()
            .expect("reopened published reshard state")
            .phase,
        ReshardPlanPhase::RevisionPublished
    );
    assert_eq!(
        reopened_table
            .route_key(&key)
            .expect("route after reopen")
            .physical_shard,
        PhysicalShardId::new(1)
    );
    assert_eq!(
        file_system
            .list(&source_dir)
            .await
            .expect("source cleanup should still be pending after reopen")
            .len(),
        1
    );

    let completed = reopened_table
        .run_reshard_plan()
        .await
        .expect("resume reshard cleanup after restart");
    assert_eq!(completed.phase, ReshardPlanPhase::Completed);
    assert!(
        file_system
            .list(&source_dir)
            .await
            .expect("source shard should be cleaned after resume")
            .is_empty()
    );
    assert_eq!(
        reopened_table
            .read(key)
            .await
            .expect("read after resumed cleanup"),
        Some(Value::bytes("before"))
    );
}

#[derive(Clone, Debug)]
struct WholeSystemShardingCampaignRng {
    state: u64,
}

impl WholeSystemShardingCampaignRng {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WholeSystemRestartPhase {
    ManifestInstalled,
    RevisionPublished,
}

impl WholeSystemRestartPhase {
    fn failpoint_name(self) -> &'static str {
        match self {
            Self::ManifestInstalled => terracedb::failpoints::names::DB_RESHARD_MANIFEST_INSTALLED,
            Self::RevisionPublished => terracedb::failpoints::names::DB_RESHARD_REVISION_PUBLISHED,
        }
    }

    fn expected_phase(self) -> ReshardPlanPhase {
        match self {
            Self::ManifestInstalled => ReshardPlanPhase::ManifestInstalled,
            Self::RevisionPublished => ReshardPlanPhase::RevisionPublished,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WholeSystemShardingScenario {
    seed: u64,
    restart_phase: WholeSystemRestartPhase,
    pre_reshard_rounds: usize,
    post_restart_rounds: usize,
    pre_reshard_catalog_churn_rounds: u64,
    hot_shard_backlog_bytes: u64,
    hot_shard_placement: ExecutionDomainPlacement,
}

fn generate_whole_system_sharding_scenario(seed: u64) -> WholeSystemShardingScenario {
    WholeSystemShardingScenario {
        seed,
        restart_phase: if seed & 1 == 0 {
            WholeSystemRestartPhase::ManifestInstalled
        } else {
            WholeSystemRestartPhase::RevisionPublished
        },
        pre_reshard_rounds: 6 + (((seed >> 1) & 1) as usize),
        post_restart_rounds: 4 + (((seed >> 2) & 1) as usize),
        pre_reshard_catalog_churn_rounds: 2 + ((seed >> 3) & 1),
        hot_shard_backlog_bytes: 160 + (((seed >> 4) & 0b11) * 64),
        hot_shard_placement: if (seed >> 6) & 1 == 0 {
            ExecutionDomainPlacement::Dedicated
        } else {
            ExecutionDomainPlacement::SharedWeighted { weight: 3 }
        },
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WholeSystemShardingOutcome {
    seed: u64,
    operation_order: Vec<String>,
    orders_move_key: String,
    orders_hot_key: String,
    invoices_hot_key: String,
    catalog_key: String,
    audit_root_key: String,
    interrupted_phase: ReshardPlanPhase,
    orders_route_before_reshard: RouteSnapshot,
    orders_route_during_interruption: RouteSnapshot,
    orders_route_after_resume: RouteSnapshot,
    catalog_route_after_churn: RouteSnapshot,
    audit_route: RouteSnapshot,
    hot_shard_owner: ExecutionDomainOwner,
    hot_shard_placement: ExecutionDomainPlacement,
    hot_shard_backlog: ExecutionDomainBacklogSnapshot,
    hot_shard_throttled_writes: u64,
    shard_zero_throttled_writes: u64,
    blocked_orders_write_isolated: bool,
    invoices_reads_during_orders_pause: bool,
    audit_reads_during_orders_pause: bool,
    orders_rows: BTreeMap<String, String>,
    invoices_rows: BTreeMap<String, String>,
    catalog_rows: BTreeMap<String, String>,
    audit_rows: BTreeMap<String, String>,
}

fn run_whole_system_sharding_campaign(
    seed: u64,
) -> turmoil::Result<(WholeSystemShardingScenario, WholeSystemShardingOutcome)> {
    let scenario = generate_whole_system_sharding_scenario(seed);
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_secs(5))
        .run_with(move |context| {
            let scenario = scenario.clone();
            async move {
                let path = format!("/terracedb/sim/sharding-whole-system-{seed:x}");
                let mut config = tiered_test_config(&path);
                config.scheduler = Some(Arc::new(ShardThrottleScheduler));
                let profile =
                    ColocatedDatabasePlacement::shard_ready("warehouse").execution_profile();
                let layout = ShardReadyPlacementLayout::new("warehouse");
                let dependencies = context
                    .dependencies()
                    .with_execution_profile(profile.clone())
                    .with_execution_identity("warehouse");
                let db = Db::open(config.clone(), dependencies.clone())
                    .await
                    .expect("open whole-system sharding simulation db");
                let manager = db.resource_manager();
                let orders = db
                    .create_table(sharded_row_table_config("orders"))
                    .await
                    .expect("create sharded orders table");
                let invoices = db
                    .create_table(sharded_row_table_config("invoices"))
                    .await
                    .expect("create sharded invoices table");
                let catalog = db
                    .create_table(sharded_row_table_config("catalog"))
                    .await
                    .expect("create sharded catalog table");
                let audit = db
                    .create_table(row_table_config("audit"))
                    .await
                    .expect("create unsharded audit table");

                let orders_move_key = find_key_for_partition(
                    &orders.sharding_state().expect("orders sharding").config,
                    VirtualPartitionId::new(1),
                    &format!("orders-move-{seed:x}"),
                );
                let orders_hot_key = find_key_for_shard(
                    &orders.sharding_state().expect("orders sharding").config,
                    PhysicalShardId::new(1),
                    &format!("orders-hot-{seed:x}"),
                );
                let invoices_hot_key = find_key_for_shard(
                    &invoices.sharding_state().expect("invoices sharding").config,
                    PhysicalShardId::new(1),
                    &format!("invoices-hot-{seed:x}"),
                );
                let catalog_key = find_key_for_shard(
                    &catalog.sharding_state().expect("catalog sharding").config,
                    PhysicalShardId::new(1),
                    &format!("catalog-hot-{seed:x}"),
                );
                let audit_root_key = format!("audit-root-{seed:04x}").into_bytes();
                let orders_move_key_string =
                    String::from_utf8(orders_move_key.clone()).expect("utf8 orders move key");
                let orders_hot_key_string =
                    String::from_utf8(orders_hot_key.clone()).expect("utf8 orders hot key");
                let invoices_hot_key_string =
                    String::from_utf8(invoices_hot_key.clone()).expect("utf8 invoices hot key");
                let catalog_key_string =
                    String::from_utf8(catalog_key.clone()).expect("utf8 catalog key");
                let audit_root_key_string =
                    String::from_utf8(audit_root_key.clone()).expect("utf8 audit root key");

                let mut expected_orders = BTreeMap::new();
                let mut expected_invoices = BTreeMap::new();
                let mut expected_catalog = BTreeMap::new();
                let mut expected_audit = BTreeMap::new();

                let initial_orders_move_value = format!("orders-move-bootstrap-{seed:04x}");
                orders
                    .write(
                        orders_move_key.clone(),
                        Value::bytes(initial_orders_move_value.clone()),
                    )
                    .await
                    .expect("write orders move bootstrap");
                expected_orders.insert(
                    orders_move_key_string.clone(),
                    initial_orders_move_value.clone(),
                );

                let initial_orders_hot_value = format!("orders-hot-bootstrap-{seed:04x}");
                orders
                    .write(
                        orders_hot_key.clone(),
                        Value::bytes(initial_orders_hot_value.clone()),
                    )
                    .await
                    .expect("write orders hot bootstrap");
                expected_orders.insert(
                    orders_hot_key_string.clone(),
                    initial_orders_hot_value.clone(),
                );

                let initial_invoices_hot_value = format!("invoices-hot-bootstrap-{seed:04x}");
                invoices
                    .write(
                        invoices_hot_key.clone(),
                        Value::bytes(initial_invoices_hot_value.clone()),
                    )
                    .await
                    .expect("write invoices hot bootstrap");
                expected_invoices.insert(
                    invoices_hot_key_string.clone(),
                    initial_invoices_hot_value.clone(),
                );

                let initial_catalog_value = format!("catalog-bootstrap-{seed:04x}");
                catalog
                    .write(
                        catalog_key.clone(),
                        Value::bytes(initial_catalog_value.clone()),
                    )
                    .await
                    .expect("write catalog bootstrap");
                expected_catalog.insert(catalog_key_string.clone(), initial_catalog_value.clone());

                let audit_root_value = format!("audit-bootstrap-{seed:04x}");
                audit
                    .write(
                        audit_root_key.clone(),
                        Value::bytes(audit_root_value.clone()),
                    )
                    .await
                    .expect("write audit bootstrap");
                expected_audit.insert(audit_root_key_string.clone(), audit_root_value.clone());

                db.flush().await.expect("flush whole-system bootstrap");

                let orders_route_before_reshard = route_snapshot(&orders, &orders_move_key);
                assert_eq!(
                    orders_route_before_reshard,
                    RouteSnapshot {
                        virtual_partition: VirtualPartitionId::new(1),
                        physical_shard: PhysicalShardId::new(0),
                        shard_map_revision: ShardMapRevision::new(7),
                    }
                );
                assert_route_stable(&orders, &orders_move_key, &orders_route_before_reshard);

                let mut catalog_revision = 7_u64;
                for revision in 0..scenario.pre_reshard_catalog_churn_rounds {
                    catalog_revision = 8 + revision;
                    catalog
                        .publish_shard_map(base_sharding(catalog_revision))
                        .await
                        .expect("publish catalog shard-map churn");
                    assert_route_stable(
                        &catalog,
                        &catalog_key,
                        &route_snapshot(&catalog, &catalog_key),
                    );
                }

                let foreground_path = db
                    .execution_lane_binding(ExecutionLane::UserForeground)
                    .domain
                    .clone();
                let background_path = db
                    .execution_lane_binding(ExecutionLane::UserBackground)
                    .domain
                    .clone();
                let hot_foreground =
                    layout.future_shard_lane_path("0001", ExecutionLane::UserForeground);
                let hot_background =
                    layout.future_shard_lane_path("0001", ExecutionLane::UserBackground);
                let manager_snapshot = manager.snapshot();
                let mut foreground_spec = manager_snapshot.domains[&foreground_path].spec.clone();
                foreground_spec.path = hot_foreground.clone();
                foreground_spec.owner = layout.shard_owner("0001");
                foreground_spec.placement = scenario.hot_shard_placement;
                foreground_spec.budget.memory.mutable_bytes = Some(96);
                manager.update_domain(foreground_spec);

                let mut background_spec = manager_snapshot.domains[&background_path].spec.clone();
                background_spec.path = hot_background.clone();
                background_spec.owner = layout.shard_owner("0001");
                background_spec.placement = scenario.hot_shard_placement;
                background_spec.budget.background.task_slots = Some(1);
                background_spec.budget.background.max_in_flight_bytes =
                    Some(scenario.hot_shard_backlog_bytes.saturating_mul(2));
                manager.update_domain(background_spec);
                manager.set_backlog(
                    &hot_background,
                    ExecutionDomainBacklogSnapshot {
                        queued_work_items: 2,
                        queued_bytes: scenario.hot_shard_backlog_bytes,
                    },
                );
                let shard_usage = ExecutionResourceUsage {
                    cpu_workers: 1,
                    ..ExecutionResourceUsage::default()
                };
                assert!(
                    manager.try_acquire(&hot_foreground, shard_usage).admitted,
                    "hot shard foreground domain should admit one worker after reassignment"
                );
                manager.release(&hot_foreground, shard_usage);
                assert_route_stable(
                    &orders,
                    &orders_hot_key,
                    &route_snapshot(&orders, &orders_hot_key),
                );

                let mut rng = WholeSystemShardingCampaignRng::seeded(seed);
                let mut operation_order = Vec::new();
                for round in 0..scenario.pre_reshard_rounds {
                    let value = format!("pre-{round:02}-{:016x}", rng.next_u64());
                    match rng.next_u64() % 3 {
                        0 => {
                            operation_order.push("pre:orders-hot".to_string());
                            orders
                                .write(orders_hot_key.clone(), Value::bytes(value.clone()))
                                .await
                                .expect("pre-reshard orders hot write");
                            expected_orders.insert(orders_hot_key_string.clone(), value);
                        }
                        1 => {
                            operation_order.push("pre:invoices-hot".to_string());
                            invoices
                                .write(invoices_hot_key.clone(), Value::bytes(value.clone()))
                                .await
                                .expect("pre-reshard invoices hot write");
                            expected_invoices.insert(invoices_hot_key_string.clone(), value);
                        }
                        _ => {
                            operation_order.push("pre:audit".to_string());
                            let audit_key = format!("audit-pre-{seed:04x}-{round:02}");
                            audit
                                .write(audit_key.as_bytes().to_vec(), Value::bytes(value.clone()))
                                .await
                                .expect("pre-reshard audit write");
                            expected_audit.insert(audit_key, value);
                        }
                    }

                    let _ = scan_string_rows(&db, "orders").await;
                    let _ = scan_string_rows(&db, "invoices").await;
                    let _ = scan_string_rows(&db, "catalog").await;
                    let _ = scan_string_rows(&db, "audit").await;

                    if round % 3 == 2 {
                        db.flush().await.expect("flush pre-reshard hot-skew round");
                    }
                }

                let orders_move_value = format!("orders-move-before-cutover-{seed:04x}");
                orders
                    .write(
                        orders_move_key.clone(),
                        Value::bytes(orders_move_value.clone()),
                    )
                    .await
                    .expect("write moved orders key before cutover");
                expected_orders.insert(orders_move_key_string.clone(), orders_move_value.clone());
                db.flush()
                    .await
                    .expect("flush moved orders row before cutover");

                let orders_target_revision =
                    orders_route_before_reshard.shard_map_revision.get() + 1;
                orders
                    .create_reshard_plan(moved_partition_one_sharding_with_revision(
                        orders_target_revision,
                    ))
                    .await
                    .expect("create whole-system orders reshard plan");

                let _fail = db.__failpoint_registry().arm_timeout(
                    scenario.restart_phase.failpoint_name(),
                    "interrupt whole-system sharding campaign",
                    FailpointMode::Once,
                );
                orders
                    .run_reshard_plan()
                    .await
                    .expect_err("whole-system campaign should interrupt the first reshard run");

                let interrupted_phase = orders
                    .resharding_state()
                    .expect("whole-system interrupted state")
                    .phase;
                let orders_route_during_interruption = route_snapshot(&orders, &orders_move_key);
                let blocked_orders_write_isolated = matches!(
                    orders
                        .write(
                            orders_hot_key.clone(),
                            Value::bytes("orders-blocked-during-reshard"),
                        )
                        .await,
                    Err(terracedb::WriteError::Commit(CommitError::Storage(error)))
                        if error.kind() == StorageErrorKind::Timeout
                );

                let invoices_reads_during_orders_pause =
                    scan_string_rows(&db, "invoices").await == expected_invoices;
                let audit_reads_during_orders_pause =
                    scan_string_rows(&db, "audit").await == expected_audit;

                let before_restart_scheduler = db.scheduler_observability_snapshot();
                drop(db);

                let reopened = Db::open(config, dependencies.clone())
                    .await
                    .expect("reopen whole-system sharding simulation db");
                let reopened_orders = reopened.table("orders");
                let _reopened_invoices = reopened.table("invoices");
                let reopened_catalog = reopened.table("catalog");
                let reopened_audit = reopened.table("audit");
                assert_eq!(
                    reopened_orders
                        .resharding_state()
                        .expect("reopened whole-system reshard state")
                        .phase,
                    interrupted_phase
                );
                assert_eq!(
                    scan_string_rows(&reopened, "invoices").await,
                    expected_invoices
                );
                assert_eq!(scan_string_rows(&reopened, "audit").await, expected_audit);

                let completed = reopened_orders
                    .run_reshard_plan()
                    .await
                    .expect("resume whole-system orders reshard plan");
                assert_eq!(completed.phase, ReshardPlanPhase::Completed);

                let mut catalog_final_revision = catalog_revision;
                catalog_final_revision += 1;
                reopened_catalog
                    .publish_shard_map(base_sharding(catalog_final_revision))
                    .await
                    .expect("publish catalog shard-map churn after reopen");

                for _round in 0..scenario.post_restart_rounds {
                    match rng.next_u64() % 4 {
                        0 => {
                            operation_order.push("post:orders-scan".to_string());
                            assert_eq!(
                                scan_string_rows(&reopened, "orders").await,
                                expected_orders
                            );
                        }
                        1 => {
                            operation_order.push("post:invoices-scan".to_string());
                            assert_eq!(
                                scan_string_rows(&reopened, "invoices").await,
                                expected_invoices
                            );
                        }
                        2 => {
                            operation_order.push("post:audit-scan".to_string());
                            assert_eq!(scan_string_rows(&reopened, "audit").await, expected_audit);
                        }
                        _ => {
                            operation_order.push("post:catalog-scan".to_string());
                            assert_eq!(
                                scan_string_rows(&reopened, "catalog").await,
                                expected_catalog
                            );
                        }
                    }
                }

                let after_restart_scheduler = reopened.scheduler_observability_snapshot();
                let hot_shard_throttled_writes = before_restart_scheduler
                    .throttled_writes_by_physical_shard
                    .get(&PhysicalShardId::new(1))
                    .copied()
                    .unwrap_or_default()
                    + after_restart_scheduler
                        .throttled_writes_by_physical_shard
                        .get(&PhysicalShardId::new(1))
                        .copied()
                        .unwrap_or_default();
                let shard_zero_throttled_writes = before_restart_scheduler
                    .throttled_writes_by_physical_shard
                    .get(&PhysicalShardId::new(0))
                    .copied()
                    .unwrap_or_default()
                    + after_restart_scheduler
                        .throttled_writes_by_physical_shard
                        .get(&PhysicalShardId::new(0))
                        .copied()
                        .unwrap_or_default();

                let orders_route_after_resume = route_snapshot(&reopened_orders, &orders_move_key);
                let catalog_route_after_churn = route_snapshot(&reopened_catalog, &catalog_key);
                let audit_route = route_snapshot(&reopened_audit, &audit_root_key);

                let hot_shard_owner = reopened.resource_manager_snapshot().domains[&hot_foreground]
                    .spec
                    .owner
                    .clone();
                let hot_shard_placement = reopened.resource_manager_snapshot().domains
                    [&hot_foreground]
                    .spec
                    .placement;
                let hot_shard_backlog =
                    reopened.resource_manager_snapshot().domains[&hot_background].backlog;

                Ok((
                    scenario,
                    WholeSystemShardingOutcome {
                        seed,
                        operation_order,
                        orders_move_key: orders_move_key_string,
                        orders_hot_key: orders_hot_key_string,
                        invoices_hot_key: invoices_hot_key_string,
                        catalog_key: catalog_key_string,
                        audit_root_key: audit_root_key_string,
                        interrupted_phase,
                        orders_route_before_reshard,
                        orders_route_during_interruption,
                        orders_route_after_resume,
                        catalog_route_after_churn,
                        audit_route,
                        hot_shard_owner,
                        hot_shard_placement,
                        hot_shard_backlog,
                        hot_shard_throttled_writes,
                        shard_zero_throttled_writes,
                        blocked_orders_write_isolated,
                        invoices_reads_during_orders_pause,
                        audit_reads_during_orders_pause,
                        orders_rows: scan_string_rows(&reopened, "orders").await,
                        invoices_rows: scan_string_rows(&reopened, "invoices").await,
                        catalog_rows: scan_string_rows(&reopened, "catalog").await,
                        audit_rows: scan_string_rows(&reopened, "audit").await,
                    },
                ))
            }
        })
}

fn assert_whole_system_sharding_campaign_outcome(
    scenario: &WholeSystemShardingScenario,
    outcome: &WholeSystemShardingOutcome,
) {
    assert_eq!(
        outcome.operation_order.len(),
        scenario.pre_reshard_rounds + scenario.post_restart_rounds,
        "seed {:#x} should preserve the scripted round count",
        scenario.seed
    );
    assert_eq!(
        outcome.interrupted_phase,
        scenario.restart_phase.expected_phase()
    );
    assert_eq!(
        outcome.orders_route_before_reshard,
        RouteSnapshot {
            virtual_partition: VirtualPartitionId::new(1),
            physical_shard: PhysicalShardId::new(0),
            shard_map_revision: ShardMapRevision::new(7),
        }
    );
    match scenario.restart_phase {
        WholeSystemRestartPhase::ManifestInstalled => {
            assert_eq!(
                outcome.orders_route_during_interruption,
                RouteSnapshot {
                    virtual_partition: VirtualPartitionId::new(1),
                    physical_shard: PhysicalShardId::new(0),
                    shard_map_revision: ShardMapRevision::new(7),
                }
            );
        }
        WholeSystemRestartPhase::RevisionPublished => {
            assert_eq!(
                outcome.orders_route_during_interruption,
                RouteSnapshot {
                    virtual_partition: VirtualPartitionId::new(1),
                    physical_shard: PhysicalShardId::new(1),
                    shard_map_revision: ShardMapRevision::new(8),
                }
            );
        }
    }
    assert_eq!(
        outcome.orders_route_after_resume,
        RouteSnapshot {
            virtual_partition: VirtualPartitionId::new(1),
            physical_shard: PhysicalShardId::new(1),
            shard_map_revision: ShardMapRevision::new(8),
        }
    );
    assert_eq!(
        outcome.audit_route.virtual_partition,
        VirtualPartitionId::new(0)
    );
    assert_eq!(
        outcome.audit_route.physical_shard,
        PhysicalShardId::UNSHARDED
    );
    assert_eq!(
        outcome.audit_route.shard_map_revision,
        ShardMapRevision::new(0)
    );
    assert_eq!(
        outcome.hot_shard_owner,
        ExecutionDomainOwner::Shard {
            database: "warehouse".to_string(),
            shard: "0001".to_string(),
        }
    );
    assert_eq!(outcome.hot_shard_placement, scenario.hot_shard_placement);
    assert_eq!(
        outcome.hot_shard_backlog,
        ExecutionDomainBacklogSnapshot {
            queued_work_items: 2,
            queued_bytes: scenario.hot_shard_backlog_bytes,
        }
    );
    assert!(
        outcome.hot_shard_throttled_writes > outcome.shard_zero_throttled_writes,
        "seed {:#x} should record more throttling on the hot shard than shard 0000",
        scenario.seed
    );
    assert!(
        outcome.blocked_orders_write_isolated,
        "seed {:#x} should fail closed for writes on the resharding table only",
        scenario.seed
    );
    assert!(
        outcome.invoices_reads_during_orders_pause,
        "seed {:#x} should keep unrelated sharded-table reads serving while orders is paused",
        scenario.seed
    );
    assert!(
        outcome.audit_reads_during_orders_pause,
        "seed {:#x} should keep unsharded-table reads serving while orders is paused",
        scenario.seed
    );
    assert_eq!(
        outcome.orders_rows.get(&outcome.orders_move_key),
        Some(&format!("orders-move-before-cutover-{:04x}", scenario.seed))
    );
    assert!(
        outcome.orders_rows.contains_key(&outcome.orders_hot_key),
        "seed {:#x} should preserve the hot orders key across the whole-system run",
        scenario.seed
    );
    assert!(
        outcome
            .invoices_rows
            .contains_key(&outcome.invoices_hot_key),
        "seed {:#x} should preserve the hot invoices key across control-plane churn",
        scenario.seed
    );
    assert_eq!(
        outcome.catalog_rows.get(&outcome.catalog_key),
        Some(&format!("catalog-bootstrap-{:04x}", scenario.seed))
    );
    assert_eq!(
        outcome.audit_rows.get(&outcome.audit_root_key),
        Some(&format!("audit-bootstrap-{:04x}", scenario.seed))
    );
    assert_eq!(
        outcome.catalog_route_after_churn.shard_map_revision,
        ShardMapRevision::new(8 + scenario.pre_reshard_catalog_churn_rounds),
        "seed {:#x} should advance catalog through the scripted churn revisions",
        scenario.seed
    );
    assert_eq!(
        outcome.catalog_route_after_churn.physical_shard,
        PhysicalShardId::new(1),
        "seed {:#x} should keep catalog ownership stable while only the mapping revision changes",
        scenario.seed
    );
}

#[test]
fn whole_system_sharding_campaign_seed_0x8201_is_reproducible() -> turmoil::Result {
    let (scenario, first) = run_whole_system_sharding_campaign(0x8201)?;
    let (_, replay) = run_whole_system_sharding_campaign(0x8201)?;

    assert_eq!(first, replay, "seed 0x8201 should be reproducible");
    assert_whole_system_sharding_campaign_outcome(&scenario, &first);
    Ok(())
}

#[test]
fn whole_system_sharding_campaign_seed_variation_changes_cutover_shape() -> turmoil::Result {
    let (first_scenario, first) = run_whole_system_sharding_campaign(0x8201)?;
    let (second_scenario, second) = run_whole_system_sharding_campaign(0x8202)?;

    assert_whole_system_sharding_campaign_outcome(&first_scenario, &first);
    assert_whole_system_sharding_campaign_outcome(&second_scenario, &second);
    assert_ne!(
        first.interrupted_phase, second.interrupted_phase,
        "seed variation should exercise more than one reshard interruption phase"
    );
    assert_ne!(
        first.orders_route_during_interruption, second.orders_route_during_interruption,
        "seed variation should exercise more than one routed cutover shape"
    );
    Ok(())
}

#[test]
#[ignore = "slow whole-system sharding seed matrix"]
fn whole_system_sharding_campaign_nightly_seed_matrix() -> turmoil::Result {
    let seeds = [0x8203_u64, 0x8204, 0x8205, 0x8206];
    let mut operation_orders = Vec::new();
    let mut interrupted_phases = Vec::new();

    for seed in seeds {
        let (scenario, outcome) = run_whole_system_sharding_campaign(seed)?;
        assert_whole_system_sharding_campaign_outcome(&scenario, &outcome);
        operation_orders.push(outcome.operation_order);
        interrupted_phases.push(outcome.interrupted_phase);
    }

    assert!(
        operation_orders
            .windows(2)
            .any(|window| window[0] != window[1]),
        "nightly sharding seed matrix should vary the scripted traffic order"
    );
    assert!(
        interrupted_phases
            .windows(2)
            .any(|window| window[0] != window[1]),
        "nightly sharding seed matrix should cover multiple interruption phases"
    );
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ShardingRuntimeChaosScenario {
    seed: u64,
    stalled_payload_bytes: usize,
    audit_payload_bytes: usize,
}

fn generate_sharding_runtime_chaos_scenario(seed: u64) -> ShardingRuntimeChaosScenario {
    ShardingRuntimeChaosScenario {
        seed,
        stalled_payload_bytes: 96 + ((((seed >> 1) & 0b11) as usize) * 16),
        audit_payload_bytes: 80 + ((((seed >> 3) & 0b11) as usize) * 16),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ShardingRuntimeChaosOutcome {
    orders_hot_route: RouteSnapshot,
    orders_move_route: RouteSnapshot,
    stalled_orders_hot_present: bool,
    invoices_progress_during_stall: bool,
    audit_progress_during_stall: bool,
    invoices_progress_after_reopen: bool,
    audit_progress_after_reopen: bool,
    audit_route: RouteSnapshot,
    hot_shard_throttled_writes: u64,
}

async fn run_sharding_runtime_chaos_scenario(
    scenario: ShardingRuntimeChaosScenario,
) -> ShardingRuntimeChaosOutcome {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let mut config = tiered_test_config(&format!("/sharding-runtime-chaos-{:x}", scenario.seed));
    config.scheduler = Some(Arc::new(ShardThrottleScheduler));
    let profile = ColocatedDatabasePlacement::shard_ready("warehouse").execution_profile();
    let dependencies =
        test_dependencies_with_clock(file_system.clone(), object_store.clone(), clock.clone())
            .with_execution_profile(profile)
            .with_execution_identity("warehouse");

    let db = Db::open(config.clone(), dependencies.clone())
        .await
        .expect("open sharding runtime chaos db");
    let orders = db
        .create_table(sharded_row_table_config("orders"))
        .await
        .expect("create runtime chaos orders table");
    let invoices = db
        .create_table(sharded_row_table_config("invoices"))
        .await
        .expect("create runtime chaos invoices table");
    let audit = db
        .create_table(row_table_config("audit"))
        .await
        .expect("create runtime chaos audit table");

    let orders_move_key = find_key_for_partition(
        &orders.sharding_state().expect("orders sharding").config,
        VirtualPartitionId::new(1),
        &format!("runtime-move-{:x}", scenario.seed),
    );
    let orders_hot_key = find_key_for_shard(
        &orders.sharding_state().expect("orders sharding").config,
        PhysicalShardId::new(1),
        &format!("runtime-hot-{:x}", scenario.seed),
    );
    let invoices_hot_key = find_key_for_shard(
        &invoices.sharding_state().expect("invoices sharding").config,
        PhysicalShardId::new(1),
        &format!("runtime-invoices-{:x}", scenario.seed),
    );
    let audit_root_key = format!("runtime-audit-root-{:04x}", scenario.seed).into_bytes();

    orders
        .write(orders_move_key.clone(), Value::bytes("orders-root"))
        .await
        .expect("write runtime chaos moved key");
    orders
        .write(orders_hot_key.clone(), Value::bytes("orders-hot-root"))
        .await
        .expect("write runtime chaos hot key");
    invoices
        .write(invoices_hot_key.clone(), Value::bytes("invoices-root"))
        .await
        .expect("write runtime chaos invoice key");
    audit
        .write(audit_root_key.clone(), Value::bytes("audit-root"))
        .await
        .expect("write runtime chaos audit root");
    db.flush().await.expect("flush runtime chaos bootstrap");

    let commit_stall = db.__failpoint_registry().arm_pause(
        terracedb::failpoints::names::DB_COMMIT_BEFORE_MEMTABLE_INSERT,
        FailpointMode::Once,
    );
    let stalled_value = vec![b's'; scenario.stalled_payload_bytes];
    let stalled_write = tokio::spawn({
        let table = orders.clone();
        let key = orders_hot_key.clone();
        let value = stalled_value.clone();
        async move { table.write(key, Value::Bytes(value)).await }
    });
    ClockProgressProbe::new(clock.as_ref(), Duration::from_millis(25), 512)
        .wait_for_failpoint_hit(&commit_stall)
        .await;

    let invoices_stall_value = format!("invoice-stall-{:04x}", scenario.seed);
    invoices
        .write(
            invoices_hot_key.clone(),
            Value::bytes(invoices_stall_value.clone()),
        )
        .await
        .expect("write invoices while orders hot shard is stalled");
    let audit_stall_key = format!("runtime-audit-stall-{:04x}", scenario.seed);
    let audit_stall_value = String::from_utf8(vec![b'a'; scenario.audit_payload_bytes])
        .expect("ascii audit stall payload");
    audit
        .write(
            audit_stall_key.as_bytes().to_vec(),
            Value::bytes(audit_stall_value.clone()),
        )
        .await
        .expect("write audit while orders hot shard is stalled");
    let invoices_progress_during_stall = true;
    let audit_progress_during_stall = true;

    commit_stall.release();
    ClockProgressProbe::new(clock.as_ref(), Duration::from_millis(25), 512)
        .wait_for_task(&stalled_write)
        .await;
    stalled_write
        .await
        .expect("stalled orders task should finish")
        .expect("stalled orders write should succeed");
    db.flush()
        .await
        .expect("flush recovered stalled orders write");

    let scheduler = db.scheduler_observability_snapshot();
    let hot_shard_throttled_writes = scheduler
        .throttled_writes_by_physical_shard
        .get(&PhysicalShardId::new(1))
        .copied()
        .unwrap_or_default();

    file_system.crash();
    drop(db);

    let reopened = Db::open(config, dependencies)
        .await
        .expect("reopen runtime chaos db");
    let reopened_orders = reopened.table("orders");
    let reopened_invoices = reopened.table("invoices");
    let reopened_audit = reopened.table("audit");

    ShardingRuntimeChaosOutcome {
        orders_hot_route: route_snapshot(&reopened_orders, &orders_hot_key),
        orders_move_route: route_snapshot(&reopened_orders, &orders_move_key),
        stalled_orders_hot_present: reopened_orders
            .read(orders_hot_key)
            .await
            .expect("read stalled orders hot key after reopen")
            == Some(Value::Bytes(stalled_value)),
        invoices_progress_during_stall,
        audit_progress_during_stall,
        invoices_progress_after_reopen: reopened_invoices
            .read(invoices_hot_key)
            .await
            .expect("read invoices progress after reopen")
            == Some(Value::bytes(invoices_stall_value)),
        audit_progress_after_reopen: reopened_audit
            .read(audit_stall_key.as_bytes().to_vec())
            .await
            .expect("read audit progress after reopen")
            == Some(Value::bytes(audit_stall_value)),
        audit_route: route_snapshot(&reopened_audit, &audit_root_key),
        hot_shard_throttled_writes,
    }
}

fn assert_sharding_runtime_chaos_outcome(
    scenario: &ShardingRuntimeChaosScenario,
    outcome: &ShardingRuntimeChaosOutcome,
) {
    assert_eq!(
        outcome.orders_move_route,
        RouteSnapshot {
            virtual_partition: VirtualPartitionId::new(1),
            physical_shard: PhysicalShardId::new(0),
            shard_map_revision: ShardMapRevision::new(7),
        }
    );
    assert_eq!(
        outcome.orders_hot_route.physical_shard,
        PhysicalShardId::new(1)
    );
    assert_eq!(
        outcome.orders_hot_route.shard_map_revision,
        ShardMapRevision::new(7)
    );
    assert!(
        outcome.stalled_orders_hot_present,
        "seed {:#x} should recover the stalled hot-shard write after reopen",
        scenario.seed
    );
    assert!(
        outcome.invoices_progress_during_stall,
        "seed {:#x} should commit unrelated sharded-table writes during the hot-shard stall",
        scenario.seed
    );
    assert!(
        outcome.audit_progress_during_stall,
        "seed {:#x} should commit unsharded-table writes during the hot-shard stall",
        scenario.seed
    );
    assert!(
        outcome.invoices_progress_after_reopen,
        "seed {:#x} should preserve the unrelated sharded-table write across crash/reopen",
        scenario.seed
    );
    assert!(
        outcome.audit_progress_after_reopen,
        "seed {:#x} should preserve the unsharded-table write across crash/reopen",
        scenario.seed
    );
    assert_eq!(
        outcome.audit_route.virtual_partition,
        VirtualPartitionId::new(0)
    );
    assert_eq!(
        outcome.audit_route.physical_shard,
        PhysicalShardId::UNSHARDED
    );
    assert!(
        outcome.hot_shard_throttled_writes > 0,
        "seed {:#x} should record hot-shard throttling during runtime chaos",
        scenario.seed
    );
}

#[tokio::test]
async fn sharding_runtime_chaos_seed_0x8211_preserves_isolation_under_hot_shard_stalls() {
    let scenario = generate_sharding_runtime_chaos_scenario(0x8211);
    let outcome = run_sharding_runtime_chaos_scenario(scenario.clone()).await;
    assert_sharding_runtime_chaos_outcome(&scenario, &outcome);
}

#[tokio::test]
async fn sharding_runtime_chaos_seed_0x8212_preserves_isolation_under_hot_shard_stalls() {
    let scenario = generate_sharding_runtime_chaos_scenario(0x8212);
    let outcome = run_sharding_runtime_chaos_scenario(scenario.clone()).await;
    assert_sharding_runtime_chaos_outcome(&scenario, &outcome);
}

#[tokio::test]
#[ignore = "slow sharding runtime chaos seed matrix"]
async fn sharding_runtime_chaos_seed_matrix() {
    for seed in [0x8213_u64, 0x8214, 0x8215, 0x8216] {
        let scenario = generate_sharding_runtime_chaos_scenario(seed);
        let outcome = run_sharding_runtime_chaos_scenario(scenario.clone()).await;
        assert_sharding_runtime_chaos_outcome(&scenario, &outcome);
    }
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReshardSimulationOutcome {
    seed: u64,
    interrupted_phase: ReshardPlanPhase,
    paused_before_resume: bool,
    final_phase: ReshardPlanPhase,
    final_route: (VirtualPartitionId, PhysicalShardId, ShardMapRevision),
    final_value_preserved: bool,
}

fn run_reshard_resume_simulation(seed: u64) -> turmoil::Result<ReshardSimulationOutcome> {
    SeededSimulationRunner::new(seed).run_with(move |context| async move {
        let path = format!("/terracedb/sim/reshard-restart-{seed}");
        let db = Db::open(tiered_test_config(&path), context.dependencies())
            .await
            .expect("open simulated db");
        let table = db
            .create_table(sharded_row_table_config("orders"))
            .await
            .expect("create sharded table");
        let key = find_key_for_partition(
            &table.sharding_state().expect("initial state").config,
            VirtualPartitionId::new(1),
            &format!("seed-{seed}"),
        );

        table
            .write(key.clone(), Value::bytes("before"))
            .await
            .expect("write before simulated restart");
        db.flush().await.expect("flush before simulated restart");
        table
            .create_reshard_plan(moved_partition_one_sharding())
            .await
            .expect("create simulated reshard plan");

        let _fail = db.__failpoint_registry().arm_timeout(
            terracedb::failpoints::names::DB_RESHARD_MANIFEST_INSTALLED,
            "interrupt after manifest install",
            FailpointMode::Once,
        );
        table
            .run_reshard_plan()
            .await
            .expect_err("simulated reshard should be interrupted");
        let interrupted_phase = table.resharding_state().expect("interrupted state").phase;
        let paused_before_resume = matches!(
            table.write(key.clone(), Value::bytes("blocked")).await,
            Err(terracedb::WriteError::Commit(CommitError::Storage(error)))
                if error.kind() == StorageErrorKind::Timeout
        );

        drop(db);

        let reopened = Db::open(tiered_test_config(&path), context.dependencies())
            .await
            .expect("reopen simulated db");
        let reopened_table = reopened.table("orders");
        let completed = reopened_table
            .run_reshard_plan()
            .await
            .expect("resume simulated reshard");
        let final_route = reopened_table.route_key(&key).expect("final route");
        let final_value_preserved = reopened_table
            .read(key)
            .await
            .expect("read after simulated resume")
            == Some(Value::bytes("before"));

        Ok(ReshardSimulationOutcome {
            seed,
            interrupted_phase,
            paused_before_resume,
            final_phase: completed.phase,
            final_route: (
                final_route.virtual_partition,
                final_route.physical_shard,
                final_route.shard_map_revision,
            ),
            final_value_preserved,
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
fn reshard_simulation_replays_same_seed_manifest_restart_behavior() -> turmoil::Result {
    let first = run_reshard_resume_simulation(0x81_19)?;
    let second = run_reshard_resume_simulation(0x81_19)?;

    assert_eq!(first, second);
    assert_eq!(first.interrupted_phase, ReshardPlanPhase::ManifestInstalled);
    assert!(first.paused_before_resume);
    assert_eq!(first.final_phase, ReshardPlanPhase::Completed);
    assert_eq!(
        first.final_route,
        (
            VirtualPartitionId::new(1),
            PhysicalShardId::new(1),
            ShardMapRevision::new(8)
        )
    );
    assert!(first.final_value_preserved);

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
