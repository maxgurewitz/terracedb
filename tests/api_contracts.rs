use std::{
    env, fs,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use terracedb::{
    CommitOptions, CompactionStrategy, Db, DbComponents, DbConfig, DbDependencies, DbSettings,
    FieldDefinition, FieldId, FieldType, FieldValue, LogCursor, MergeOperator, NoopScheduler,
    S3Location, ScanOptions, SchemaDefinition, SequenceNumber, StorageConfig, StorageError,
    StubClock, StubFileSystem, StubObjectStore, StubRng, TableConfig, TableFormat, Value,
};

#[derive(Debug)]
struct AppendMergeOperator;

impl MergeOperator for AppendMergeOperator {
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&Value>,
        operands: &[Value],
    ) -> Result<Value, StorageError> {
        let mut merged = match existing {
            Some(Value::Bytes(bytes)) => bytes.clone(),
            Some(_) => {
                return Err(StorageError::unsupported(
                    "append merge operator only supports byte values",
                ));
            }
            None => Vec::new(),
        };

        for operand in operands {
            let Value::Bytes(bytes) = operand else {
                return Err(StorageError::unsupported(
                    "append merge operator only supports byte operands",
                ));
            };
            if !merged.is_empty() && !bytes.is_empty() {
                merged.push(b'|');
            }
            merged.extend_from_slice(bytes);
        }

        Ok(Value::Bytes(merged))
    }

    fn partial_merge(
        &self,
        _key: &[u8],
        left: &Value,
        right: &Value,
    ) -> Result<Option<Value>, StorageError> {
        let Value::Bytes(left_bytes) = left else {
            return Err(StorageError::unsupported(
                "append merge operator only supports byte operands",
            ));
        };
        let Value::Bytes(right_bytes) = right else {
            return Err(StorageError::unsupported(
                "append merge operator only supports byte operands",
            ));
        };

        let mut merged = left_bytes.clone();
        if !merged.is_empty() && !right_bytes.is_empty() {
            merged.push(b'|');
        }
        merged.extend_from_slice(right_bytes);
        Ok(Some(Value::Bytes(merged)))
    }
}

#[tokio::test]
async fn public_api_surface_compiles_and_is_instantiable() {
    let db = Db::builder()
        .settings(DbSettings::tiered(
            "/tmp/terracedb",
            S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "dev".to_string(),
            },
        ))
        .components(
            DbComponents::new(
                Arc::new(StubFileSystem::default()),
                Arc::new(StubObjectStore::default()),
                Arc::new(StubClock::default()),
                Arc::new(StubRng::seeded(123)),
            )
            .with_scheduler(Arc::new(NoopScheduler)),
        )
        .open()
        .await
        .expect("open db");

    let row_table = db
        .create_table(TableConfig {
            name: "events".to_string(),
            format: TableFormat::Row,
            merge_operator: Some(Arc::new(AppendMergeOperator)),
            max_merge_operand_chain_length: Some(4),
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(10),
            history_retention_sequences: None,
            compaction_strategy: CompactionStrategy::Leveled,
            schema: None,
            metadata: Default::default(),
        })
        .await
        .expect("create row table");

    let metrics_schema = SchemaDefinition {
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
    };

    let columnar = db
        .create_table(TableConfig {
            name: "metrics".to_string(),
            format: TableFormat::Columnar,
            merge_operator: None,
            max_merge_operand_chain_length: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(8),
            history_retention_sequences: Some(128),
            compaction_strategy: CompactionStrategy::Tiered,
            schema: Some(metrics_schema.clone()),
            metadata: Default::default(),
        })
        .await
        .expect("create columnar table");

    let sync_lookup = db.table("events");
    let optional_lookup = db
        .try_table("events")
        .expect("existing table should be found");
    assert_eq!(sync_lookup.name(), "events");
    assert_eq!(optional_lookup.id(), row_table.id());
    assert!(db.try_table("missing").is_none());
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

    let columnar_committed = columnar
        .write(
            b"metric:1".to_vec(),
            Value::named_record(
                &metrics_schema,
                [
                    ("amount", FieldValue::Int64(42)),
                    ("user_id", FieldValue::String("alice".to_string())),
                ],
            )
            .expect("normalize named record"),
        )
        .await
        .expect("write columnar record");

    let snapshot = db.snapshot().await;
    assert_eq!(snapshot.sequence(), columnar_committed);
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
        hybrid_read: Default::default(),
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

#[tokio::test]
async fn builder_opens_with_production_defaults_and_local_object_store() {
    let unique = format!(
        "terracedb-builder-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let root = env::temp_dir().join(unique);
    let ssd_path = root.join("ssd");
    let object_store_root = root.join("object-store");

    let result = async {
        let db = Db::builder()
            .tiered(
                ssd_path.to_string_lossy().into_owned(),
                S3Location {
                    bucket: "terracedb-test".to_string(),
                    prefix: "builder-defaults".to_string(),
                },
            )
            .local_object_store(object_store_root.clone())
            .open()
            .await
            .expect("open db with builder defaults");

        assert_eq!(db.current_sequence(), SequenceNumber::new(0));
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));
    }
    .await;

    fs::remove_dir_all(&root).ok();
    result
}
