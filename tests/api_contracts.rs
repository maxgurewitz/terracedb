use std::sync::Arc;

use terracedb::{
    CommitOptions, CompactionStrategy, Db, DbConfig, DbDependencies, FieldDefinition, FieldId,
    FieldType, FieldValue, LogCursor, NoopScheduler, S3Location, ScanOptions, SchemaDefinition,
    SequenceNumber, SsdConfig, StorageConfig, StubClock, StubFileSystem, StubObjectStore, StubRng,
    TableConfig, TableFormat, TieredDurabilityMode, TieredStorageConfig, Value,
};

#[tokio::test]
async fn public_api_surface_compiles_and_is_instantiable() {
    let config = DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: "/tmp/terracedb".to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "dev".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability: TieredDurabilityMode::GroupCommit,
        }),
        scheduler: Some(Arc::new(NoopScheduler)),
    };

    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(123)),
    );

    let db = Db::open(config, dependencies).await.expect("open db");

    let row_table = db
        .create_table(TableConfig {
            name: "events".to_string(),
            format: TableFormat::Row,
            merge_operator: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(10),
            compaction_strategy: CompactionStrategy::Leveled,
            schema: None,
            metadata: Default::default(),
        })
        .await
        .expect("create row table");

    let _columnar = db
        .create_table(TableConfig {
            name: "metrics".to_string(),
            format: TableFormat::Columnar,
            merge_operator: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(8),
            compaction_strategy: CompactionStrategy::Tiered,
            schema: Some(SchemaDefinition {
                version: 1,
                fields: vec![
                    FieldDefinition {
                        id: FieldId::new(1),
                        name: "user_id".to_string(),
                        field_type: FieldType::String,
                        nullable: false,
                        default: None,
                    },
                    FieldDefinition {
                        id: FieldId::new(2),
                        name: "amount".to_string(),
                        field_type: FieldType::Int64,
                        nullable: false,
                        default: Some(FieldValue::Int64(0)),
                    },
                ],
            }),
            metadata: Default::default(),
        })
        .await
        .expect("create columnar table");

    let sync_lookup = db.table("events");
    assert_eq!(sync_lookup.name(), "events");
    assert_eq!(db.current_sequence(), SequenceNumber::new(0));
    assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));

    let mut batch = db.write_batch();
    batch.put(&row_table, b"k1".to_vec(), Value::bytes("v1"));
    batch.merge(&row_table, b"k2".to_vec(), Value::bytes("delta"));
    batch.delete(&row_table, b"k3".to_vec());

    let mut read_set = db.read_set();
    read_set.add(&row_table, b"k1".to_vec(), SequenceNumber::new(0));

    let committed = db
        .commit(batch, CommitOptions::default().with_read_set(read_set))
        .await
        .expect("commit");
    assert_eq!(committed, SequenceNumber::new(1));

    let snapshot = db.snapshot().await;
    assert_eq!(snapshot.sequence(), committed);
    assert!(snapshot.read(&row_table, b"k1".to_vec()).await.is_ok());
    assert!(
        snapshot
            .scan(
                &row_table,
                b"a".to_vec(),
                b"z".to_vec(),
                ScanOptions::default(),
            )
            .await
            .is_ok()
    );
    assert!(
        snapshot
            .scan_prefix(&row_table, b"pre".to_vec(), ScanOptions::default())
            .await
            .is_ok()
    );
    snapshot.release();
    assert!(snapshot.is_released());

    assert!(row_table.read(b"k1".to_vec()).await.is_ok());
    assert!(
        row_table
            .scan(
                b"a".to_vec(),
                b"z".to_vec(),
                ScanOptions {
                    reverse: true,
                    limit: Some(10),
                    columns: Some(vec!["amount".to_string()]),
                },
            )
            .await
            .is_ok()
    );
    assert!(
        row_table
            .scan_prefix(b"user:".to_vec(), ScanOptions::default())
            .await
            .is_ok()
    );
    assert!(row_table.read_at(b"k1".to_vec(), committed).await.is_ok());
    assert!(
        row_table
            .scan_at(
                b"a".to_vec(),
                b"z".to_vec(),
                committed,
                ScanOptions::default(),
            )
            .await
            .is_ok()
    );

    assert!(
        db.scan_since(&row_table, LogCursor::beginning(), ScanOptions::default())
            .await
            .is_ok()
    );
    assert!(
        db.scan_durable_since(&row_table, LogCursor::beginning(), ScanOptions::default())
            .await
            .is_ok()
    );

    let visible = db.subscribe(&row_table);
    assert_eq!(visible.current(), committed);
    let durable = db.subscribe_durable(&row_table);
    assert_eq!(durable.current(), committed);

    let stats = db.table_stats(&row_table).await;
    assert_eq!(stats.total_bytes, 0);
    assert!(db.pending_work().await.is_empty());
}

#[tokio::test]
async fn smoke_open_uses_fake_dependencies_only() {
    let config = DbConfig {
        storage: StorageConfig::S3Primary(terracedb::S3PrimaryStorageConfig {
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "smoke".to_string(),
            },
            mem_cache_size_bytes: 1024,
            auto_flush_interval: None,
        }),
        scheduler: None,
    };

    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(999)),
    );

    let db = Db::open(config, dependencies).await.expect("open db");
    assert_eq!(db.current_sequence(), SequenceNumber::new(0));
    assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));
}
