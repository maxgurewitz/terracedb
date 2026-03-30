use std::{collections::BTreeMap, sync::Arc};

use serde_json::json;
use terracedb::{
    CompactionStrategy, CreateTableError, Db, DbConfig, DbDependencies, FieldDefinition, FieldId,
    FieldType, FieldValue, MergeOperator, S3Location, SchemaDefinition, SsdConfig, StorageConfig,
    StorageError, StorageErrorKind, StubClock, StubFileSystem, StubObjectStore, StubRng,
    TableConfig, TableFormat, TieredDurabilityMode, TieredStorageConfig, Value, WriteError,
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
fn schema_successors_support_lazy_add_remove_and_rename_rules() {
    let current = schema_with_nullable_defaults();

    let mut renamed = current.clone();
    renamed.version = 2;
    renamed.fields[0].name = "account_id".to_string();
    current
        .validate_successor(&renamed)
        .expect("renames should preserve field ids");

    let mut removed = renamed.clone();
    removed.version = 3;
    removed.fields.retain(|field| field.id != FieldId::new(2));
    renamed
        .validate_successor(&removed)
        .expect("removing a field should stay lazy");

    let mut invalid_add = current.clone();
    invalid_add.version = 2;
    invalid_add.fields.push(FieldDefinition {
        id: FieldId::new(4),
        name: "region".to_string(),
        field_type: FieldType::String,
        nullable: false,
        default: None,
    });
    let error = current
        .validate_successor(&invalid_add)
        .expect_err("new required fields without defaults should be rejected");
    assert_eq!(error.kind(), StorageErrorKind::Unsupported);
    assert!(
        error
            .message()
            .contains("must be nullable or define a default")
    );

    let invalid_name_reuse = SchemaDefinition {
        version: 2,
        fields: vec![
            FieldDefinition {
                id: FieldId::new(4),
                name: "user_id".to_string(),
                field_type: FieldType::String,
                nullable: true,
                default: None,
            },
            current.fields[1].clone(),
            current.fields[2].clone(),
        ],
    };
    let error = current
        .validate_successor(&invalid_name_reuse)
        .expect_err("renames should not change field ids");
    assert_eq!(error.kind(), StorageErrorKind::Unsupported);
    assert!(error.message().contains("changes id"));
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
async fn table_creation_rejects_row_schemas_and_accepts_columnar_merge_config() {
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

    let table = db
        .create_table(TableConfig {
            name: "metrics".to_string(),
            format: TableFormat::Columnar,
            merge_operator: Some(Arc::new(SummingMergeOperator)),
            max_merge_operand_chain_length: Some(2),
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(8),
            history_retention_sequences: Some(16),
            compaction_strategy: CompactionStrategy::Tiered,
            schema: Some(schema_with_nullable_defaults()),
            metadata: Default::default(),
        })
        .await
        .expect("columnar tables should accept merge operators");

    table
        .write(
            b"user:1".to_vec(),
            Value::named_record(
                &schema_with_nullable_defaults(),
                [
                    ("user_id", FieldValue::String("alice".to_string())),
                    ("count", FieldValue::Int64(2)),
                ],
            )
            .expect("encode base row"),
        )
        .await
        .expect("write base row");
    table
        .merge(
            b"user:1".to_vec(),
            Value::record(BTreeMap::from([(FieldId::new(2), FieldValue::Int64(3))])),
        )
        .await
        .expect("apply partial columnar merge operand");

    assert_eq!(
        table
            .read(b"user:1".to_vec())
            .await
            .expect("read merged row"),
        Some(Value::record(BTreeMap::from([
            (FieldId::new(1), FieldValue::String("alice".to_string())),
            (FieldId::new(2), FieldValue::Int64(5)),
            (FieldId::new(3), FieldValue::Null),
        ])))
    );
}

#[derive(Debug)]
struct SummingMergeOperator;

impl MergeOperator for SummingMergeOperator {
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&Value>,
        operands: &[Value],
    ) -> Result<Value, StorageError> {
        let mut total = match existing {
            Some(value) => record_fields(value)?,
            None => (None, 0, None),
        };
        for operand in operands {
            let (user_id, count, active) = record_fields(operand)?;
            if total.0.is_none() {
                total.0 = user_id;
            }
            total.1 += count;
            if active.is_some() {
                total.2 = active;
            }
        }

        Ok(Value::record(BTreeMap::from([
            (
                FieldId::new(1),
                FieldValue::String(
                    total.0.ok_or_else(|| {
                        StorageError::unsupported("merge result is missing user_id")
                    })?,
                ),
            ),
            (FieldId::new(2), FieldValue::Int64(total.1)),
            (
                FieldId::new(3),
                total.2.map(FieldValue::Bool).unwrap_or(FieldValue::Null),
            ),
        ])))
    }

    fn partial_merge(
        &self,
        _key: &[u8],
        left: &Value,
        right: &Value,
    ) -> Result<Option<Value>, StorageError> {
        let (left_user_id, left_count, left_active) = record_fields(left)?;
        let (right_user_id, right_count, right_active) = record_fields(right)?;
        let mut record = BTreeMap::new();
        if let Some(user_id) = right_user_id.or(left_user_id) {
            record.insert(FieldId::new(1), FieldValue::String(user_id));
        }
        record.insert(FieldId::new(2), FieldValue::Int64(left_count + right_count));
        if let Some(active) = right_active.or(left_active) {
            record.insert(FieldId::new(3), FieldValue::Bool(active));
        }
        Ok(Some(Value::record(record)))
    }
}

fn record_fields(value: &Value) -> Result<(Option<String>, i64, Option<bool>), StorageError> {
    let Value::Record(record) = value else {
        return Err(StorageError::unsupported(
            "summing merge operator only supports record operands",
        ));
    };
    let user_id = match record.get(&FieldId::new(1)) {
        Some(FieldValue::String(value)) => Some(value.clone()),
        Some(FieldValue::Null) | None => None,
        Some(_) => {
            return Err(StorageError::unsupported(
                "user_id field must be a string or null",
            ));
        }
    };
    let count = match record.get(&FieldId::new(2)) {
        Some(FieldValue::Int64(value)) => *value,
        Some(FieldValue::Null) | None => 0,
        Some(_) => {
            return Err(StorageError::unsupported(
                "count field must be an int64 or null",
            ));
        }
    };
    let active = match record.get(&FieldId::new(3)) {
        Some(FieldValue::Bool(value)) => Some(*value),
        Some(FieldValue::Null) | None => None,
        Some(_) => {
            return Err(StorageError::unsupported(
                "active field must be a bool or null",
            ));
        }
    };
    Ok((user_id, count, active))
}
