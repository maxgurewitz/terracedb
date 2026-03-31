use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use rstest::rstest;
use terracedb::{
    CompactionStrategy, Db, DbConfig, FieldDefinition, FieldId, FieldType, FieldValue, S3Location,
    S3PrimaryStorageConfig, ScanOptions, SchemaDefinition, SequenceNumber, SsdConfig,
    StorageConfig, StubFileSystem, StubObjectStore, Table, TableConfig, TableFormat,
    TieredDurabilityMode, TieredStorageConfig, Value,
    test_support::{row_table_config, test_dependencies, tiered_test_config_with_durability},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn s3_primary_test_config(prefix: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::S3Primary(S3PrimaryStorageConfig {
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: prefix.to_string(),
            },
            mem_cache_size_bytes: 1024 * 1024,
            auto_flush_interval: Some(Duration::from_secs(60)),
        }),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

fn tiered_test_config_with_budget(
    path: &str,
    durability: TieredDurabilityMode,
    max_local_bytes: u64,
) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "tiered".to_string(),
            },
            max_local_bytes,
            durability,
            local_retention: terracedb::TieredLocalRetentionMode::Offload,
        }),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

async fn open_db(
    config: DbConfig,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> TestResult<Db> {
    Ok(Db::open(config, test_dependencies(file_system, object_store)).await?)
}

fn columnar_schema() -> SchemaDefinition {
    SchemaDefinition {
        version: 1,
        fields: vec![
            FieldDefinition {
                id: FieldId::new(1),
                name: "title".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            },
            FieldDefinition {
                id: FieldId::new(2),
                name: "count".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                default: None,
            },
        ],
    }
}

fn columnar_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Columnar,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(10),
        history_retention_sequences: None,
        compaction_strategy: CompactionStrategy::Leveled,
        schema: Some(columnar_schema()),
        metadata: Default::default(),
    }
}

#[derive(Clone, Copy, Debug)]
enum DurabilityCase {
    TieredGroupCommit,
    TieredDeferred,
    S3Primary,
}

impl DurabilityCase {
    fn name(self) -> &'static str {
        match self {
            Self::TieredGroupCommit => "tiered-group-commit",
            Self::TieredDeferred => "tiered-deferred",
            Self::S3Primary => "s3-primary",
        }
    }

    fn config(self) -> DbConfig {
        match self {
            Self::TieredGroupCommit => tiered_test_config_with_durability(
                &format!("/semantic-matrix/{}/durability", self.name()),
                TieredDurabilityMode::GroupCommit,
            ),
            Self::TieredDeferred => tiered_test_config_with_durability(
                &format!("/semantic-matrix/{}/durability", self.name()),
                TieredDurabilityMode::Deferred,
            ),
            Self::S3Primary => {
                s3_primary_test_config(&format!("semantic-matrix/{}/durability", self.name()))
            }
        }
    }

    fn expected_durable_before_flush(self, committed: SequenceNumber) -> SequenceNumber {
        match self {
            Self::TieredGroupCommit => committed,
            Self::TieredDeferred | Self::S3Primary => SequenceNumber::default(),
        }
    }
}

#[rstest]
#[case::tiered_group_commit(DurabilityCase::TieredGroupCommit)]
#[case::tiered_deferred(DurabilityCase::TieredDeferred)]
#[case::s3_primary(DurabilityCase::S3Primary)]
#[tokio::test]
async fn durability_matrix_preserves_expected_visible_and_durable_prefixes(
    #[case] case: DurabilityCase,
) -> TestResult {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = open_db(case.config(), file_system.clone(), object_store.clone()).await?;
    let table = db.create_table(row_table_config("events")).await?;

    let committed = table
        .write(b"event:1".to_vec(), Value::bytes("payload"))
        .await?;

    assert_eq!(db.current_sequence(), committed);
    assert_eq!(
        db.current_durable_sequence(),
        case.expected_durable_before_flush(committed)
    );

    db.flush().await?;
    assert_eq!(db.current_sequence(), committed);
    assert_eq!(db.current_durable_sequence(), committed);

    let reopened = open_db(case.config(), file_system, object_store).await?;
    assert_eq!(reopened.current_sequence(), committed);
    assert_eq!(reopened.current_durable_sequence(), committed);

    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum ReadMatrixCase {
    TieredRowLocal,
    TieredRowOffloaded,
    S3PrimaryRowRemote,
    TieredColumnarLocal,
    S3PrimaryColumnarRemote,
}

impl ReadMatrixCase {
    fn name(self) -> &'static str {
        match self {
            Self::TieredRowLocal => "tiered-row-local",
            Self::TieredRowOffloaded => "tiered-row-offloaded",
            Self::S3PrimaryRowRemote => "s3-primary-row-remote",
            Self::TieredColumnarLocal => "tiered-columnar-local",
            Self::S3PrimaryColumnarRemote => "s3-primary-columnar-remote",
        }
    }

    fn format(self) -> TableFormat {
        match self {
            Self::TieredRowLocal | Self::TieredRowOffloaded | Self::S3PrimaryRowRemote => {
                TableFormat::Row
            }
            Self::TieredColumnarLocal | Self::S3PrimaryColumnarRemote => TableFormat::Columnar,
        }
    }

    fn setup_config(self) -> DbConfig {
        match self {
            Self::TieredRowLocal | Self::TieredColumnarLocal => tiered_test_config_with_budget(
                &format!("/semantic-matrix/{}/reads", self.name()),
                TieredDurabilityMode::GroupCommit,
                1024 * 1024,
            ),
            Self::TieredRowOffloaded => tiered_test_config_with_budget(
                &format!("/semantic-matrix/{}/reads", self.name()),
                TieredDurabilityMode::GroupCommit,
                1024 * 1024,
            ),
            Self::S3PrimaryRowRemote | Self::S3PrimaryColumnarRemote => {
                s3_primary_test_config(&format!("semantic-matrix/{}/reads", self.name()))
            }
        }
    }

    fn reopen_config(self) -> DbConfig {
        match self {
            Self::TieredRowOffloaded => tiered_test_config_with_budget(
                &format!("/semantic-matrix/{}/reads", self.name()),
                TieredDurabilityMode::GroupCommit,
                1,
            ),
            Self::TieredRowLocal | Self::TieredColumnarLocal => tiered_test_config_with_budget(
                &format!("/semantic-matrix/{}/reads", self.name()),
                TieredDurabilityMode::GroupCommit,
                1024 * 1024,
            ),
            Self::S3PrimaryRowRemote | Self::S3PrimaryColumnarRemote => {
                s3_primary_test_config(&format!("semantic-matrix/{}/reads", self.name()))
            }
        }
    }

    fn table_config(self) -> TableConfig {
        match self.format() {
            TableFormat::Row => row_table_config("items"),
            TableFormat::Columnar => columnar_table_config("items"),
        }
    }

    fn schema(self) -> Option<SchemaDefinition> {
        match self.format() {
            TableFormat::Row => None,
            TableFormat::Columnar => Some(columnar_schema()),
        }
    }

    fn requires_separate_flushes(self) -> bool {
        matches!(self, Self::TieredRowOffloaded)
    }

    fn should_offload(self) -> bool {
        matches!(self, Self::TieredRowOffloaded)
    }
}

fn logical_record(title: &str, count: i64) -> serde_json::Value {
    serde_json::json!({
        "title": title,
        "count": count,
    })
}

async fn write_logical_record(
    table: &Table,
    format: TableFormat,
    schema: Option<&SchemaDefinition>,
    key: &str,
    title: &str,
    count: i64,
) -> TestResult {
    let value = match format {
        TableFormat::Row => Value::bytes(serde_json::to_vec(&logical_record(title, count))?),
        TableFormat::Columnar => Value::named_record(
            schema.expect("columnar cases require a schema"),
            [
                ("title", FieldValue::String(title.to_string())),
                ("count", FieldValue::Int64(count)),
            ],
        )?,
    };

    table.write(key.as_bytes().to_vec(), value).await?;
    Ok(())
}

fn normalize_value(value: Value, schema: Option<&SchemaDefinition>) -> serde_json::Value {
    match value {
        Value::Bytes(bytes) => serde_json::from_slice(&bytes).expect("decode row json payload"),
        Value::Record(record) => {
            let schema = schema.expect("columnar reads require a schema");
            let mut normalized = serde_json::Map::new();
            for field in &schema.fields {
                normalized.insert(
                    field.name.clone(),
                    serde_json::to_value(
                        record
                            .get(&field.id)
                            .expect("normalized columnar records should include every field"),
                    )
                    .expect("serialize field value"),
                );
            }
            serde_json::Value::Object(normalized)
        }
    }
}

async fn collect_normalized_rows(
    table: &Table,
    schema: Option<&SchemaDefinition>,
) -> TestResult<Vec<(String, serde_json::Value)>> {
    let mut rows = table
        .scan_prefix(b"item:".to_vec(), ScanOptions::default())
        .await?;
    let mut normalized = Vec::new();
    while let Some((key, value)) = rows.next().await {
        normalized.push((
            String::from_utf8(key).expect("keys should be valid utf-8"),
            normalize_value(value, schema),
        ));
    }
    Ok(normalized)
}

#[rstest]
#[case::tiered_row_local(ReadMatrixCase::TieredRowLocal)]
#[case::tiered_row_offloaded(ReadMatrixCase::TieredRowOffloaded)]
#[case::s3_primary_row_remote(ReadMatrixCase::S3PrimaryRowRemote)]
#[case::tiered_columnar_local(ReadMatrixCase::TieredColumnarLocal)]
#[case::s3_primary_columnar_remote(ReadMatrixCase::S3PrimaryColumnarRemote)]
#[tokio::test]
async fn normalized_reads_match_across_local_remote_and_columnar_layouts(
    #[case] case: ReadMatrixCase,
) -> TestResult {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let schema = case.schema();
    let format = case.format();

    let db = open_db(
        case.setup_config(),
        file_system.clone(),
        object_store.clone(),
    )
    .await?;
    let table = db.create_table(case.table_config()).await?;

    write_logical_record(&table, format, schema.as_ref(), "item:1", "alpha", 1).await?;
    if case.requires_separate_flushes() {
        db.flush().await?;
    }
    write_logical_record(&table, format, schema.as_ref(), "item:2", "beta", 2).await?;
    db.flush().await?;

    let mut reopened = open_db(
        case.reopen_config(),
        file_system.clone(),
        object_store.clone(),
    )
    .await?;
    if case.should_offload() {
        let mut offloaded = false;
        while reopened.run_next_offload().await? {
            offloaded = true;
        }
        assert!(
            offloaded,
            "expected the row fixture to offload at least one SSTable"
        );
        reopened = open_db(case.reopen_config(), file_system, object_store).await?;
    }

    let actual = collect_normalized_rows(&reopened.table("items"), schema.as_ref()).await?;
    let expected = vec![
        ("item:1".to_string(), logical_record("alpha", 1)),
        ("item:2".to_string(), logical_record("beta", 2)),
    ];
    assert_eq!(actual, expected);

    Ok(())
}
