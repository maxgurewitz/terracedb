use std::{collections::BTreeMap, sync::Arc};

use serde_json::json;
use terracedb::{
    CompactionStrategy, CreateTableError, Db, DbConfig, DbDependencies, FieldDefinition, FieldId,
    FieldType, FieldValue, S3Location, SchemaDefinition, SsdConfig, StorageConfig,
    StorageErrorKind, StubClock, StubFileSystem, StubObjectStore, StubRng, TableConfig,
    TableFormat, TieredDurabilityMode, TieredStorageConfig, Value, WriteError,
};

fn tiered_config(path: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "columnar".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability: TieredDurabilityMode::GroupCommit,
        }),
        scheduler: None,
    }
}

fn dependencies() -> DbDependencies {
    DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(17)),
    )
}

fn schema_with_nullable_defaults() -> SchemaDefinition {
    SchemaDefinition {
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
                name: "count".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                default: Some(FieldValue::Int64(0)),
            },
            FieldDefinition {
                id: FieldId::new(3),
                name: "active".to_string(),
                field_type: FieldType::Bool,
                nullable: true,
                default: None,
            },
        ],
    }
}

#[test]
fn schema_validation_rejects_duplicate_ids_and_unknown_json_fields() {
    let duplicate_ids = SchemaDefinition {
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
                id: FieldId::new(1),
                name: "count".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                default: None,
            },
        ],
    };

    let error = duplicate_ids
        .validate()
        .expect_err("duplicate field ids should be rejected");
    assert_eq!(error.kind(), StorageErrorKind::Unsupported);
    assert!(error.message().contains("duplicate field id 1"));

    let parse_error = serde_json::from_value::<SchemaDefinition>(json!({
        "version": 1,
        "fields": [{
            "id": 1,
            "name": "user_id",
            "type": "string",
            "nullable": false,
            "unexpected": true
        }]
    }))
    .expect_err("unknown schema JSON fields should be rejected");
    assert!(parse_error.to_string().contains("unexpected"));
}

#[test]
fn schema_validation_rejects_invalid_defaults_and_in_place_type_changes() {
    let invalid_default = SchemaDefinition {
        version: 1,
        fields: vec![FieldDefinition {
            id: FieldId::new(1),
            name: "count".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            default: Some(FieldValue::String("wrong".to_string())),
        }],
    };

    let error = invalid_default
        .validate()
        .expect_err("type-mismatched defaults should be rejected");
    assert_eq!(error.kind(), StorageErrorKind::Unsupported);
    assert!(error.message().contains("default value for field count"));

    let current = schema_with_nullable_defaults();
    let mut successor = current.clone();
    successor.version = 2;
    successor.fields[0].field_type = FieldType::Bytes;

    let error = current
        .validate_successor(&successor)
        .expect_err("in-place field type changes should be rejected");
    assert_eq!(error.kind(), StorageErrorKind::Unsupported);
    assert!(error.message().contains("changes type"));
}

#[test]
fn named_records_fill_defaults_and_reject_unknown_fields() {
    let schema = schema_with_nullable_defaults();

    let normalized = schema
        .record_from_names([
            ("count", FieldValue::Int64(7)),
            ("user_id", FieldValue::String("alice".to_string())),
        ])
        .expect("normalize named record");

    assert_eq!(
        normalized,
        BTreeMap::from([
            (FieldId::new(1), FieldValue::String("alice".to_string())),
            (FieldId::new(2), FieldValue::Int64(7)),
            (FieldId::new(3), FieldValue::Null),
        ])
    );

    let error = schema
        .record_from_names([("missing", FieldValue::Bool(true))])
        .expect_err("unknown field names should be rejected");
    assert_eq!(error.kind(), StorageErrorKind::Unsupported);
    assert!(error.message().contains("unknown field name missing"));

    let error = schema
        .record_from_names([("count", FieldValue::Int64(3))])
        .expect_err("missing required fields should be rejected");
    assert_eq!(error.kind(), StorageErrorKind::Unsupported);
    assert!(error.message().contains("missing required field user_id"));
}

#[test]
fn named_record_resolution_is_deterministic_by_field_id() {
    let schema = schema_with_nullable_defaults();

    let first = schema
        .record_from_names([
            ("active", FieldValue::Bool(true)),
            ("user_id", FieldValue::String("alice".to_string())),
            ("count", FieldValue::Int64(11)),
        ])
        .expect("normalize first record");
    let second = schema
        .record_from_names([
            ("count", FieldValue::Int64(11)),
            ("user_id", FieldValue::String("alice".to_string())),
            ("active", FieldValue::Bool(true)),
        ])
        .expect("normalize second record");

    assert_eq!(first, second);
    assert_eq!(
        first.keys().copied().collect::<Vec<_>>(),
        vec![FieldId::new(1), FieldId::new(2), FieldId::new(3)]
    );
}

#[tokio::test]
async fn columnar_tables_normalize_record_writes_and_reject_bytes() {
    let db = Db::open(tiered_config("/columnar-record-writes"), dependencies())
        .await
        .expect("open db");
    let schema = schema_with_nullable_defaults();
    let table = db
        .create_table(TableConfig {
            name: "metrics".to_string(),
            format: TableFormat::Columnar,
            merge_operator: None,
            max_merge_operand_chain_length: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(8),
            history_retention_sequences: Some(32),
            compaction_strategy: CompactionStrategy::Tiered,
            schema: Some(schema.clone()),
            metadata: Default::default(),
        })
        .await
        .expect("create columnar table");

    table
        .write(
            b"user:1".to_vec(),
            Value::named_record(
                &schema,
                [
                    ("count", FieldValue::Int64(5)),
                    ("user_id", FieldValue::String("alice".to_string())),
                ],
            )
            .expect("normalize named record"),
        )
        .await
        .expect("write columnar record");

    assert_eq!(
        table.read(b"user:1".to_vec()).await.expect("read record"),
        Some(Value::record(BTreeMap::from([
            (FieldId::new(1), FieldValue::String("alice".to_string())),
            (FieldId::new(2), FieldValue::Int64(5)),
            (FieldId::new(3), FieldValue::Null),
        ])))
    );

    let error = table
        .write(b"user:2".to_vec(), Value::bytes("not-a-record"))
        .await
        .expect_err("byte payloads should be rejected for columnar tables");
    match error {
        WriteError::Commit(terracedb::CommitError::Storage(error)) => {
            assert_eq!(error.kind(), StorageErrorKind::Unsupported);
            assert!(
                error
                    .message()
                    .contains("requires structured record values")
            );
        }
        other => panic!("unexpected write error: {other}"),
    }
}

#[tokio::test]
async fn table_creation_rejects_row_schemas_and_columnar_v1_merge_boundary() {
    let db = Db::open(tiered_config("/columnar-config-validation"), dependencies())
        .await
        .expect("open db");

    let mut row_with_schema = TableConfig {
        name: "events".to_string(),
        format: TableFormat::Row,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(10),
        history_retention_sequences: None,
        compaction_strategy: CompactionStrategy::Leveled,
        schema: Some(schema_with_nullable_defaults()),
        metadata: Default::default(),
    };
    let error = db
        .create_table(row_with_schema.clone())
        .await
        .expect_err("row tables should reject schemas");
    assert!(matches!(error, CreateTableError::InvalidConfig(_)));
    assert!(
        error
            .to_string()
            .contains("row tables do not accept a schema")
    );

    row_with_schema.schema = None;
    db.create_table(row_with_schema)
        .await
        .expect("create baseline row table");

    let error = db
        .create_table(TableConfig {
            name: "metrics".to_string(),
            format: TableFormat::Columnar,
            merge_operator: Some(Arc::new(RejectingMergeOperator)),
            max_merge_operand_chain_length: Some(2),
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(8),
            history_retention_sequences: Some(16),
            compaction_strategy: CompactionStrategy::Tiered,
            schema: Some(schema_with_nullable_defaults()),
            metadata: Default::default(),
        })
        .await
        .expect_err("columnar tables should reject merge operators in v1");
    assert!(matches!(error, CreateTableError::InvalidConfig(_)));
    assert!(error.to_string().contains("merge operators in v1"));
}

#[derive(Debug)]
struct RejectingMergeOperator;

impl terracedb::MergeOperator for RejectingMergeOperator {
    fn full_merge(
        &self,
        _key: &[u8],
        _existing: Option<&Value>,
        _operands: &[Value],
    ) -> Result<Value, terracedb::StorageError> {
        unreachable!("columnar merge boundary test should reject config before use")
    }

    fn partial_merge(
        &self,
        _key: &[u8],
        _left: &Value,
        _right: &Value,
    ) -> Result<Option<Value>, terracedb::StorageError> {
        unreachable!("columnar merge boundary test should reject config before use")
    }
}
