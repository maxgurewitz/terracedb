use std::{collections::BTreeMap, sync::Arc};

use terracedb::{
    BatchShardLocalityError, CommitId, CommitOptions, Db, ExecutionDomainOwner,
    ExecutionDomainPath, LogCursor, ObjectKeyLayout, PhysicalShardId, ReshardPlanSkeleton,
    S3Location, SequenceNumber, ShardChangeCursor, ShardCommitLaneId, ShardHashAlgorithm,
    ShardMapRevision, ShardMemtableOwner, ShardOpenRequest, ShardReadyPlacementLayout,
    ShardSstableOwnership, ShardingConfig, TableConfig, TableId, Value, VirtualPartitionCoverage,
    VirtualPartitionId, WriteBatchShardingError,
};

#[cfg(any(test, feature = "test-support"))]
use terracedb::test_support::{row_table_config, test_dependencies, tiered_test_config};
use terracedb::{StubFileSystem, StubObjectStore};

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
