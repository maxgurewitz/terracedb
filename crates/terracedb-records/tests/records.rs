use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use terracedb::{
    ColumnarRecord, ColumnarTableConfigBuilder, CreateTableError, Db, DbConfig, FieldDefinition,
    FieldId, FieldType, FieldValue, HybridProfile, HybridProjectionSidecarConfig, HybridReadConfig,
    HybridTableFeatures, PhysicalShardId, S3Location, ScanOptions, ScanPredicate, SchemaDefinition,
    ShardHashAlgorithm, ShardMapRevision, ShardingConfig, SsdConfig, StorageConfig, StubFileSystem,
    StubObjectStore, TableConfig, TieredDurabilityMode, TieredStorageConfig, Value,
    VirtualPartitionId,
    test_support::{row_table_config, test_dependencies},
};
use terracedb_records::{
    BigEndianU64Codec, CodecPhase, CodecTarget, ColumnarProjection, ColumnarRecordCodec,
    ColumnarTable, JsonValueCodec, KeyCodec, ProjectionStream, RecordCodecError, RecordKeyspace,
    RecordKeyspaceError, RecordReadError, RecordStream, RecordTable, RecordTransaction,
    RecordTransactionFuture, RecordWriteError, Utf8StringCodec, ValueCodec,
};
use tokio::sync::Notify;

type JsonStringTable = RecordTable<String, TestRecord, Utf8StringCodec, JsonValueCodec<TestRecord>>;
type SensorReadingsTable = ColumnarTable<
    SensorReadingKey,
    SensorReadingRecord,
    SensorReadingKeyCodec,
    SensorReadingRecordCodec,
>;

const TEMPERATURE_C_FIELD_ID: FieldId = FieldId::new(1);
const ALERT_ACTIVE_FIELD_ID: FieldId = FieldId::new(2);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestRecord {
    title: String,
    visits: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ValidatedRecord {
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SensorReadingKey {
    device_id: String,
    reading_at_ms: u64,
}

impl SensorReadingKey {
    fn new(device_id: impl Into<String>, reading_at_ms: u64) -> Self {
        Self {
            device_id: device_id.into(),
            reading_at_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SensorReadingRecord {
    temperature_c: i64,
    alert_active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TemperatureProjectionRow {
    device_id: String,
    reading_at_ms: u64,
    temperature_c: i64,
}

#[derive(Clone, Copy, Debug, Default)]
struct SensorReadingKeyCodec;

impl KeyCodec<SensorReadingKey> for SensorReadingKeyCodec {
    fn encode_key(&self, key: &SensorReadingKey) -> Result<Vec<u8>, RecordCodecError> {
        Ok(format!("device:{}:{:020}", key.device_id, key.reading_at_ms).into_bytes())
    }

    fn decode_key(&self, key: &[u8]) -> Result<SensorReadingKey, RecordCodecError> {
        let key = std::str::from_utf8(key).map_err(RecordCodecError::decode_key)?;
        let (prefix, timestamp) = key.rsplit_once(':').ok_or_else(|| {
            RecordCodecError::decode_key(io::Error::other(format!(
                "sensor key {key} is missing a timestamp"
            )))
        })?;
        let device_id = prefix.strip_prefix("device:").ok_or_else(|| {
            RecordCodecError::decode_key(io::Error::other(format!(
                "sensor key {key} is missing the device prefix"
            )))
        })?;
        Ok(SensorReadingKey {
            device_id: device_id.to_string(),
            reading_at_ms: timestamp.parse().map_err(RecordCodecError::decode_key)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SensorReadingRecordCodec;

impl ColumnarRecordCodec<SensorReadingRecord> for SensorReadingRecordCodec {
    fn encode_record(
        &self,
        value: &SensorReadingRecord,
    ) -> Result<ColumnarRecord, RecordCodecError> {
        Ok(BTreeMap::from([
            (
                TEMPERATURE_C_FIELD_ID,
                FieldValue::Int64(value.temperature_c),
            ),
            (ALERT_ACTIVE_FIELD_ID, FieldValue::Bool(value.alert_active)),
        ]))
    }

    fn decode_record(
        &self,
        record: &ColumnarRecord,
    ) -> Result<SensorReadingRecord, RecordCodecError> {
        Ok(SensorReadingRecord {
            temperature_c: expect_i64(record, TEMPERATURE_C_FIELD_ID, "temperature_c")?,
            alert_active: expect_bool(record, ALERT_ACTIVE_FIELD_ID, "alert_active")?,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TemperatureProjection;

impl ColumnarProjection<SensorReadingKey, TemperatureProjectionRow> for TemperatureProjection {
    fn columns(&self) -> Vec<String> {
        vec!["temperature_c".to_string()]
    }

    fn decode_projection(
        &self,
        key: &SensorReadingKey,
        record: &ColumnarRecord,
    ) -> Result<TemperatureProjectionRow, RecordCodecError> {
        Ok(TemperatureProjectionRow {
            device_id: key.device_id.clone(),
            reading_at_ms: key.reading_at_ms,
            temperature_c: expect_i64(record, TEMPERATURE_C_FIELD_ID, "temperature_c")?,
        })
    }
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

fn test_config_with_hybrid(path: &str, hybrid_read: HybridReadConfig) -> DbConfig {
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
        hybrid_read,
        scheduler: None,
    }
}

async fn open_db(
    path: &str,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> Db {
    open_db_with_hybrid(path, file_system, object_store, HybridReadConfig::default()).await
}

async fn open_db_with_hybrid(
    path: &str,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
    hybrid_read: HybridReadConfig,
) -> Db {
    Db::builder()
        .config(test_config_with_hybrid(path, hybrid_read))
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
    let table: JsonStringTable = RecordTable::ensure(
        &db,
        row_table_config("records"),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
    .await
    .expect("ensure records table");
    (db, table)
}

async fn collect_stream<K>(mut stream: RecordStream<K, TestRecord>) -> Vec<TestRecord> {
    let mut rows = Vec::new();
    while let Some((_key, value)) = stream.next().await {
        rows.push(value);
    }
    rows
}

async fn collect_projection_stream<V>(mut stream: ProjectionStream<V>) -> Vec<V> {
    let mut rows = Vec::new();
    while let Some(value) = stream.next().await {
        rows.push(value);
    }
    rows
}

fn test_sharding_config() -> ShardingConfig {
    ShardingConfig::hash(
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
    .expect("test sharding config")
}

fn sharded_row_table_config(name: &str) -> TableConfig {
    TableConfig::row(name)
        .sharding(test_sharding_config())
        .build()
}

fn sensor_reading_schema() -> SchemaDefinition {
    SchemaDefinition {
        version: 1,
        fields: vec![
            FieldDefinition {
                id: TEMPERATURE_C_FIELD_ID,
                name: "temperature_c".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                default: None,
            },
            FieldDefinition {
                id: ALERT_ACTIVE_FIELD_ID,
                name: "alert_active".to_string(),
                field_type: FieldType::Bool,
                nullable: false,
                default: None,
            },
        ],
    }
}

fn expect_i64(
    record: &ColumnarRecord,
    field_id: FieldId,
    field_name: &str,
) -> Result<i64, RecordCodecError> {
    match record.get(&field_id) {
        Some(FieldValue::Int64(value)) => Ok(*value),
        Some(other) => Err(RecordCodecError::decode_value(io::Error::other(format!(
            "{field_name} had unexpected type {other:?}"
        )))),
        None => Err(RecordCodecError::decode_value(io::Error::other(format!(
            "missing {field_name}"
        )))),
    }
}

fn expect_bool(
    record: &ColumnarRecord,
    field_id: FieldId,
    field_name: &str,
) -> Result<bool, RecordCodecError> {
    match record.get(&field_id) {
        Some(FieldValue::Bool(value)) => Ok(*value),
        Some(other) => Err(RecordCodecError::decode_value(io::Error::other(format!(
            "{field_name} had unexpected type {other:?}"
        )))),
        None => Err(RecordCodecError::decode_value(io::Error::other(format!(
            "missing {field_name}"
        )))),
    }
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
        .create_table(TableConfig::row("numeric").build())
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

#[tokio::test]
async fn typed_ensure_detects_definition_mismatch() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_db("/records/ensure", file_system, object_store).await;

    let created: JsonStringTable = RecordTable::ensure(
        &db,
        row_table_config("records"),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
    .await
    .expect("create typed table");
    let ensured: JsonStringTable = RecordTable::ensure(
        &db,
        row_table_config("records"),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
    .await
    .expect("ensure matching typed table");
    assert_eq!(created.table().id(), ensured.table().id());

    let mismatch = TableConfig::row("records")
        .history_retention_sequences(Some(5))
        .build();
    let error = JsonStringTable::ensure(&db, mismatch, Utf8StringCodec, JsonValueCodec::new())
        .await
        .expect_err("mismatched config should fail");
    let CreateTableError::DefinitionMismatch {
        table_name,
        details,
    } = error
    else {
        panic!("expected definition mismatch");
    };
    assert_eq!(table_name, "records");
    assert!(details.contains("history_retention_sequences"));
}

#[tokio::test]
async fn typed_get_or_create_by_name_accepts_evolved_persisted_sharding() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_db("/records/get-or-create", file_system, object_store).await;

    let table: JsonStringTable = RecordTable::ensure(
        &db,
        sharded_row_table_config("rooms"),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
    .await
    .expect("create sharded table");
    let target = table
        .sharding_state()
        .expect("source sharding")
        .config
        .move_partition(VirtualPartitionId::new(0), PhysicalShardId::new(2))
        .expect("move partition");
    table
        .reshard_to(target.clone())
        .await
        .expect("reshard table");

    let reopened = JsonStringTable::get_or_create_by_name(
        &db,
        sharded_row_table_config("rooms"),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
    .await
    .expect("reopen by name with evolved sharding");
    assert_eq!(
        reopened.sharding_state().expect("reopened sharding").config,
        target
    );
}

#[tokio::test]
async fn record_keyspace_detects_misaligned_routes_and_reshards_members_together() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_db("/records/keyspace", file_system, object_store).await;

    let primary: JsonStringTable = RecordTable::ensure(
        &db,
        sharded_row_table_config("primary"),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
    .await
    .expect("create primary table");
    let follower: JsonStringTable = RecordTable::ensure(
        &db,
        sharded_row_table_config("follower"),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
    .await
    .expect("create follower table");

    let keyspace = RecordKeyspace::new(
        primary.table().clone(),
        [follower.table().clone()],
        Utf8StringCodec,
    )
    .expect("aligned keyspace");
    let initial = keyspace.sharding_state().expect("aligned sharding state");
    let target = initial
        .config
        .move_partition(VirtualPartitionId::new(0), PhysicalShardId::new(2))
        .expect("move partition");
    keyspace
        .reshard_to(target.clone())
        .await
        .expect("reshard both tables");

    assert_eq!(
        primary.sharding_state().expect("primary sharding").config,
        target
    );
    assert_eq!(
        follower.sharding_state().expect("follower sharding").config,
        target
    );

    let misaligned: JsonStringTable = RecordTable::ensure(
        &db,
        sharded_row_table_config("misaligned"),
        Utf8StringCodec,
        JsonValueCodec::new(),
    )
    .await
    .expect("create misaligned table");
    let misaligned_keyspace = RecordKeyspace::new(
        primary.table().clone(),
        [misaligned.table().clone()],
        Utf8StringCodec,
    )
    .expect("compatible keyspace identity");

    let divergent_key = (0..10_000_u64)
        .map(|index| format!("room:{index}"))
        .find(|key| {
            primary
                .route_key(key)
                .expect("route primary key")
                .virtual_partition
                == VirtualPartitionId::new(0)
        })
        .expect("key routed through moved partition");

    let error = misaligned_keyspace
        .route_key(&divergent_key)
        .expect_err("misaligned keyspace should reject divergent routing");
    let RecordKeyspaceError::MisalignedRouting {
        primary_table,
        table_name,
    } = error
    else {
        panic!("expected misaligned routing");
    };
    assert_eq!(primary_table, "primary");
    assert_eq!(table_name, "misaligned");
}

#[tokio::test]
async fn conflict_retry_loop_retries_until_conflict_is_resolved() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let (db, table) = open_json_table("/records/run-with-retry", file_system, object_store).await;

    table
        .write_str(
            "counter",
            &TestRecord {
                title: "seed".to_string(),
                visits: 0,
            },
        )
        .await
        .expect("seed counter");

    let attempts = Arc::new(AtomicUsize::new(0));
    let first_attempt_started = Arc::new(Notify::new());
    let allow_first_attempt_commit = Arc::new(Notify::new());
    let worker_table = table.clone();
    let worker_attempts = attempts.clone();
    let worker_started = first_attempt_started.clone();
    let worker_allow = allow_first_attempt_commit.clone();

    let worker = tokio::spawn(async move {
        RecordTransaction::run_conflict_retry_loop(
            &db,
            RecordTransaction::replay_safe(move |tx: &mut RecordTransaction| {
                let table = worker_table.clone();
                let attempts = worker_attempts.clone();
                let started = worker_started.clone();
                let allow = worker_allow.clone();
                let future: RecordTransactionFuture<'_, _, _> = Box::pin(async move {
                    let current = tx
                        .read_str(&table, "counter")
                        .await
                        .map_err(io::Error::other)?
                        .expect("seeded counter");
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if attempt == 1 {
                        started.notify_one();
                        allow.notified().await;
                    }

                    let next = TestRecord {
                        title: format!("attempt-{attempt}"),
                        visits: current.visits + 1,
                    };
                    tx.write_str(&table, "counter", &next)
                        .map_err(io::Error::other)?;
                    Ok::<_, io::Error>(next)
                });
                future
            }),
        )
        .await
    });

    first_attempt_started.notified().await;
    table
        .write_str(
            "counter",
            &TestRecord {
                title: "external".to_string(),
                visits: 10,
            },
        )
        .await
        .expect("create conflict");
    allow_first_attempt_commit.notify_one();

    let (result, _sequence) = worker
        .await
        .expect("join worker")
        .expect("transaction should retry and commit");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(result.visits, 11);
    assert_eq!(result.title, "attempt-2");
    assert_eq!(
        table.read_str("counter").await.expect("read final counter"),
        Some(result)
    );
}

#[tokio::test]
async fn columnar_table_projected_scans_push_down_predicates_and_report_execution() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_db_with_hybrid(
        "/records/columnar-projection",
        file_system,
        object_store,
        HybridReadConfig::for_profile(HybridProfile::Accelerated),
    )
    .await;
    let schema = sensor_reading_schema();
    let raw = db
        .create_table(
            ColumnarTableConfigBuilder::new("sensor_readings", schema.clone())
                .hybrid_features(HybridTableFeatures {
                    skip_indexes: Vec::new(),
                    projection_sidecars: vec![HybridProjectionSidecarConfig {
                        name: "temperature_only".to_string(),
                        fields: vec!["temperature_c".to_string(), "alert_active".to_string()],
                    }],
                })
                .build_for_profile(HybridProfile::Accelerated),
        )
        .await
        .expect("create columnar table");
    let table = SensorReadingsTable::with_codecs(
        raw,
        schema,
        SensorReadingKeyCodec,
        SensorReadingRecordCodec,
    );

    for (reading_at_ms, temperature_c, alert_active) in
        [(100, 71, true), (200, 72, false), (300, 73, true)]
    {
        table
            .write(
                &SensorReadingKey::new("device-01", reading_at_ms),
                &SensorReadingRecord {
                    temperature_c,
                    alert_active,
                },
            )
            .await
            .expect("write sensor reading");
    }
    db.flush().await.expect("flush sensor readings");

    let (stream, execution) = table
        .scan_projected_with_execution(
            &SensorReadingKey::new("device-01", 100),
            &SensorReadingKey::new("device-01", 301),
            &TemperatureProjection,
            ScanOptions {
                predicate: Some(ScanPredicate::bool_equals("alert_active", true)),
                ..ScanOptions::default()
            },
        )
        .await
        .expect("scan projected columnar rows");
    let rows = collect_projection_stream(stream).await;
    assert_eq!(
        rows,
        vec![
            TemperatureProjectionRow {
                device_id: "device-01".to_string(),
                reading_at_ms: 100,
                temperature_c: 71,
            },
            TemperatureProjectionRow {
                device_id: "device-01".to_string(),
                reading_at_ms: 300,
                temperature_c: 73,
            },
        ]
    );

    assert_eq!(execution.rows_returned, rows.len());
    let columnar = execution
        .columnar
        .expect("columnar projected scan should report columnar execution");
    assert_eq!(columnar.rows_returned, rows.len());
    assert_eq!(columnar.sstables_considered, 1);
    assert!(columnar.rows_evaluated >= rows.len());
    assert!(!columnar.parts.is_empty());
}
