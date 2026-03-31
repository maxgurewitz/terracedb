use std::{io, sync::Arc};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use terracedb::{
    CompactionStrategy, Db, DbConfig, S3Location, ScanOptions, SsdConfig, StorageConfig,
    StubFileSystem, StubObjectStore, TableConfig, TableFormat, TieredDurabilityMode,
    TieredStorageConfig, Value,
    test_support::{row_table_config, test_dependencies},
};
use terracedb_records::{
    BigEndianU64Codec, CodecPhase, CodecTarget, JsonValueCodec, RecordReadError, RecordStream,
    RecordTable, RecordTransaction, RecordWriteError, Utf8StringCodec, ValueCodec,
};

type JsonStringTable = RecordTable<String, TestRecord, Utf8StringCodec, JsonValueCodec<TestRecord>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestRecord {
    title: String,
    visits: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ValidatedRecord {
    name: String,
}

#[derive(Clone, Copy, Debug, Default)]
struct ValidatedJsonCodec;

impl ValueCodec<ValidatedRecord> for ValidatedJsonCodec {
    fn encode_value(
        &self,
        value: &ValidatedRecord,
    ) -> Result<Value, terracedb_records::RecordCodecError> {
        validate_record(value)?;
        JsonValueCodec::new().encode_value(value)
    }

    fn decode_value(
        &self,
        value: &Value,
    ) -> Result<ValidatedRecord, terracedb_records::RecordCodecError> {
        let decoded = JsonValueCodec::new().decode_value(value)?;
        validate_record(&decoded)?;
        Ok(decoded)
    }
}

fn validate_record(value: &ValidatedRecord) -> Result<(), terracedb_records::RecordCodecError> {
    if value.name.trim().is_empty() {
        return Err(terracedb_records::RecordCodecError::validate_value(
            io::Error::other("name must not be empty"),
        ));
    }
    Ok(())
}

fn test_config(path: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-records-tests".to_string(),
                prefix: "records".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability: TieredDurabilityMode::GroupCommit,
            local_retention: terracedb::TieredLocalRetentionMode::Offload,
        }),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

async fn open_db(
    path: &str,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> Db {
    Db::builder()
        .config(test_config(path))
        .dependencies(test_dependencies(file_system, object_store))
        .open()
        .await
        .expect("open db")
}

async fn open_json_table(
    path: &str,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> (Db, JsonStringTable) {
    let db = open_db(path, file_system, object_store).await;
    let raw = match db.create_table(row_table_config("records")).await {
        Ok(table) => table,
        Err(_) => db.table("records"),
    };
    (
        db,
        RecordTable::with_codecs(raw, Utf8StringCodec, JsonValueCodec::new()),
    )
}

async fn collect_stream<K>(mut stream: RecordStream<K, TestRecord>) -> Vec<TestRecord> {
    let mut rows = Vec::new();
    while let Some((_key, value)) = stream.next().await {
        rows.push(value);
    }
    rows
}

#[tokio::test]
async fn typed_crud_works_for_direct_tables_and_transactions() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let (db, table) = open_json_table("/records/crud", file_system, object_store).await;

    let first = TestRecord {
        title: "first".to_string(),
        visits: 1,
    };
    let second = TestRecord {
        title: "second".to_string(),
        visits: 2,
    };

    table
        .write_str("todo:1", &first)
        .await
        .expect("direct write");
    assert_eq!(
        table.read_str("todo:1").await.expect("direct read"),
        Some(first.clone())
    );

    let mut tx = RecordTransaction::begin(&db).await;
    tx.write_str(&table, "todo:2", &second)
        .expect("transactional write");
    assert_eq!(
        tx.read_str(&table, "todo:2")
            .await
            .expect("read your own write"),
        Some(second.clone())
    );
    tx.commit().await.expect("commit");

    assert_eq!(
        table.read_str("todo:2").await.expect("post-commit read"),
        Some(second)
    );

    table.delete_str("todo:1").await.expect("direct delete");
    assert_eq!(table.read_str("todo:1").await.expect("deleted read"), None);
}

#[tokio::test]
async fn transaction_scans_include_local_writes_and_preserve_codec_order() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_db("/records/scan-order", file_system, object_store).await;
    let raw = db
        .create_table(TableConfig {
            name: "numeric".to_string(),
            format: TableFormat::Row,
            merge_operator: None,
            max_merge_operand_chain_length: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(10),
            history_retention_sequences: None,
            compaction_strategy: CompactionStrategy::Leveled,
            schema: None,
            metadata: Default::default(),
        })
        .await
        .expect("create numeric table");
    let table = RecordTable::with_codecs(raw, BigEndianU64Codec, Utf8StringCodec);

    table.write(&2, &"two".to_string()).await.expect("seed 2");
    table.write(&10, &"ten".to_string()).await.expect("seed 10");
    table
        .write(&30, &"thirty".to_string())
        .await
        .expect("seed 30");

    let mut tx = RecordTransaction::begin(&db).await;
    tx.write(&table, &1, &"one".to_string())
        .expect("local insert");
    tx.delete(&table, &30).expect("local delete");

    let mut stream = tx
        .scan(&table, &0, &100, ScanOptions::default())
        .await
        .expect("scan");
    let mut keys = Vec::new();
    while let Some((key, _value)) = stream.next().await {
        keys.push(key);
    }

    assert_eq!(keys, vec![1, 2, 10]);

    let mut reverse = tx
        .scan(
            &table,
            &0,
            &100,
            ScanOptions {
                reverse: true,
                ..ScanOptions::default()
            },
        )
        .await
        .expect("reverse scan");
    let mut reverse_keys = Vec::new();
    while let Some((key, _value)) = reverse.next().await {
        reverse_keys.push(key);
    }

    assert_eq!(reverse_keys, vec![10, 2, 1]);
}

#[tokio::test]
async fn committed_batches_remain_atomic_after_reopen() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/records/recovery";

    let (db, table) = open_json_table(path, file_system.clone(), object_store.clone()).await;
    let durable = TestRecord {
        title: "durable".to_string(),
        visits: 10,
    };
    let mut durable_tx = RecordTransaction::begin(&db).await;
    durable_tx
        .write_str(&table, "todo:a", &durable)
        .expect("durable write a");
    durable_tx
        .write_str(&table, "todo:b", &durable)
        .expect("durable write b");
    durable_tx.commit().await.expect("durable commit");

    let (_reopened_db, reopened_table) = open_json_table(path, file_system, object_store).await;
    assert_eq!(
        reopened_table
            .read_str("todo:a")
            .await
            .expect("reopened durable a"),
        Some(durable.clone())
    );
    assert_eq!(
        reopened_table
            .read_str("todo:b")
            .await
            .expect("reopened durable b"),
        Some(durable)
    );
}

#[tokio::test]
async fn decode_failures_and_validation_failures_stay_structured() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_db("/records/errors", file_system, object_store).await;
    let raw = db
        .create_table(row_table_config("validated"))
        .await
        .expect("create validated table");
    let validated = RecordTable::with_codecs(raw.clone(), Utf8StringCodec, ValidatedJsonCodec);

    raw.write(
        b"bad-json".to_vec(),
        Value::bytes("not-json".as_bytes().to_vec()),
    )
    .await
    .expect("seed invalid payload");

    let decode_error = validated
        .read_str("bad-json")
        .await
        .expect_err("invalid payload should fail to decode");
    let RecordReadError::Codec(codec_error) = decode_error else {
        panic!("expected codec error");
    };
    assert_eq!(codec_error.phase(), CodecPhase::Decode);
    assert_eq!(codec_error.target(), CodecTarget::Value);

    let validation_error = validated
        .write_str(
            "bad-validation",
            &ValidatedRecord {
                name: "   ".to_string(),
            },
        )
        .await
        .expect_err("invalid record should fail validation");
    let RecordWriteError::Codec(codec_error) = validation_error else {
        panic!("expected codec error");
    };
    assert_eq!(codec_error.phase(), CodecPhase::Validation);
    assert_eq!(codec_error.target(), CodecTarget::Value);
}

#[tokio::test]
async fn direct_table_scan_decodes_all_rows() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let (_db, table) = open_json_table("/records/direct-scan", file_system, object_store).await;

    table
        .write_str(
            "todo:1",
            &TestRecord {
                title: "alpha".to_string(),
                visits: 1,
            },
        )
        .await
        .expect("write alpha");
    table
        .write_str(
            "todo:2",
            &TestRecord {
                title: "beta".to_string(),
                visits: 2,
            },
        )
        .await
        .expect("write beta");

    let rows = collect_stream(table.scan_all(ScanOptions::default()).await.expect("scan")).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].title, "alpha");
    assert_eq!(rows[1].title, "beta");
}
