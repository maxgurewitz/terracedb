use std::sync::Arc;

use serde_json::json;
use terracedb::{
    Db, StubFileSystem, StubObjectStore,
    test_support::{row_table_config, test_dependencies, tiered_test_config},
};
use terracedb_debezium::{
    DebeziumCodecError, DebeziumColumnProjection, DebeziumConnectorKind, DebeziumEnvelopeDecoder,
    DebeziumEvent, DebeziumEventKind, DebeziumMaterializationMode, DebeziumMaterializer,
    DebeziumMirrorMutationPlan, DebeziumMirrorRow, DebeziumMirrorTables, DebeziumOperationKind,
    DebeziumPartitionedTableLayout, DebeziumPostgresSourceMetadata, DebeziumPrimaryKey,
    DebeziumRowPredicate, DebeziumSnapshotMarker, DebeziumSourceTable, DebeziumTableFilter,
    DebeziumTransactionMetadata, PostgresDebeziumDecoder, event_log_workflow_source_config,
    mirror_workflow_source_config,
};

fn sample_event() -> DebeziumEvent {
    DebeziumEvent {
        connector: DebeziumConnectorKind::PostgreSql,
        source_table: DebeziumSourceTable::new("app", "public", "orders"),
        kind: DebeziumEventKind::Change {
            operation: DebeziumOperationKind::Read,
        },
        snapshot: Some(DebeziumSnapshotMarker::Initial),
        kafka: terracedb_debezium::DebeziumKafkaCoordinates {
            topic: "dbserver1.public.orders".to_string(),
            partition: 0,
            offset: 7,
            timestamp_millis: Some(1_700_000_000_000),
        },
        primary_key: DebeziumPrimaryKey::new([("id".to_string(), json!(7))].into_iter().collect()),
        before: None,
        after: Some(
            [
                ("id".to_string(), json!(7)),
                ("region".to_string(), json!("west")),
                ("status".to_string(), json!("open")),
            ]
            .into_iter()
            .collect(),
        ),
        source: DebeziumPostgresSourceMetadata {
            lsn: Some(42),
            tx_id: Some(9),
            xmin: Some(3),
            source_ts_ms: Some(1_700_000_000_001),
            payload_ts_ms: Some(1_700_000_000_002),
        },
        transaction: Some(DebeziumTransactionMetadata {
            id: "tx-9".to_string(),
            total_order: Some(1),
            data_collection_order: Some(1),
        }),
    }
}

fn update_event(before_region: &str, after_region: &str) -> DebeziumEvent {
    DebeziumEvent {
        kind: DebeziumEventKind::Change {
            operation: DebeziumOperationKind::Update,
        },
        snapshot: None,
        before: Some(
            [
                ("id".to_string(), json!(7)),
                ("region".to_string(), json!(before_region)),
                ("status".to_string(), json!("open")),
            ]
            .into_iter()
            .collect(),
        ),
        after: Some(
            [
                ("id".to_string(), json!(7)),
                ("region".to_string(), json!(after_region)),
                ("status".to_string(), json!("closed")),
            ]
            .into_iter()
            .collect(),
        ),
        ..sample_event()
    }
}

#[test]
fn normalized_event_and_mirror_row_round_trip_through_public_codecs() {
    let event = sample_event();
    let decoded =
        DebeziumEvent::from_value(&event.to_value().expect("encode event")).expect("decode event");
    assert_eq!(decoded, event);

    let mirror = DebeziumMirrorRow {
        source_table: event.source_table.clone(),
        primary_key: event.primary_key.clone(),
        values: event.after.clone().expect("snapshot row should exist"),
        snapshot: event.snapshot,
        kafka: event.kafka.clone(),
        transaction: event.transaction.clone(),
    };
    let decoded_mirror =
        DebeziumMirrorRow::from_value(&mirror.to_value().expect("encode mirror row"))
            .expect("decode mirror row");
    assert_eq!(decoded_mirror, mirror);

    assert!(matches!(
        DebeziumPrimaryKey::decode(
            &event
                .primary_key
                .encode()
                .expect("encode primary key")
        ),
        Ok(key) if key == event.primary_key
    ));

    assert!(DebeziumMaterializationMode::EventLog.writes_event_log());
    assert!(!DebeziumMaterializationMode::EventLog.writes_mirror());
    assert!(DebeziumMaterializationMode::Mirror.writes_mirror());
    assert!(!DebeziumMaterializationMode::Mirror.writes_event_log());
    assert!(DebeziumMaterializationMode::Hybrid.writes_event_log());
    assert!(DebeziumMaterializationMode::Hybrid.writes_mirror());
}

#[test]
fn table_filter_row_predicate_and_projection_drive_membership_plans() {
    let planner = DebeziumMaterializer::hybrid(
        terracedb_debezium::DebeziumEventLogTables::one_table_per_partition(Vec::new())
            .expect("empty event log mapping is valid"),
        DebeziumMirrorTables::new(Vec::new()).expect("empty mirror mapping is valid"),
    )
    .with_table_filter(DebeziumTableFilter::allow_only([DebeziumSourceTable::new(
        "app", "public", "orders",
    )]))
    .with_row_predicate(DebeziumRowPredicate::ColumnEquals {
        column: "region".to_string(),
        value: json!("west"),
    })
    .with_column_projection(
        DebeziumColumnProjection::default()
            .include_only(["id", "region", "status"])
            .with_redaction("status", json!("redacted")),
    );

    let exit_plan = planner.plan(&update_event("west", "east"));
    assert!(exit_plan.table_selected);
    assert!(exit_plan.row_membership.before);
    assert!(!exit_plan.row_membership.after);
    assert!(exit_plan.write_event_log);
    assert!(matches!(
        exit_plan.mirror,
        DebeziumMirrorMutationPlan::Delete { .. }
    ));

    let enter_plan = planner.plan(&update_event("east", "west"));
    assert!(!enter_plan.row_membership.before);
    assert!(enter_plan.row_membership.after);
    match enter_plan.mirror {
        DebeziumMirrorMutationPlan::Upsert { row, .. } => {
            assert_eq!(row.values.get("id"), Some(&json!(7)));
            assert_eq!(row.values.get("region"), Some(&json!("west")));
            assert_eq!(row.values.get("status"), Some(&json!("redacted")));
        }
        DebeziumMirrorMutationPlan::Delete { .. } | DebeziumMirrorMutationPlan::Ignore => {
            panic!("west-entering updates should upsert the current-state mirror")
        }
    }

    let filtered_out = planner.plan(&update_event("east", "east"));
    assert!(!filtered_out.write_event_log);
}

#[test]
fn postgres_decoder_fails_closed_on_malformed_or_unbound_envelopes() {
    let decoder = PostgresDebeziumDecoder::new().with_topic_binding(
        "dbserver1.public.orders",
        DebeziumSourceTable::new("app", "public", "orders"),
    );

    let mut tombstone = terracedb_kafka::KafkaRecord::new("unbound.public.orders", 0, 0);
    tombstone.key = Some(br#"{"payload":{"id":7}}"#.to_vec());
    let error = decoder
        .decode_record(&tombstone)
        .expect_err("tombstone should need binding");
    assert!(matches!(
        error,
        terracedb_debezium::DebeziumDecodeError::UnboundTombstoneTopic { .. }
    ));

    let mut malformed = terracedb_kafka::KafkaRecord::new("dbserver1.public.orders", 0, 1);
    malformed.key = Some(br#"{"payload":{"id":7}}"#.to_vec());
    malformed.value = Some(
        br#"{"payload":{"op":"x","source":{"db":"app","schema":"public","table":"orders"}}}"#
            .to_vec(),
    );
    let error = decoder
        .decode_record(&malformed)
        .expect_err("unsupported operations should fail closed");
    assert!(matches!(
        error,
        terracedb_debezium::DebeziumDecodeError::UnsupportedOperation { .. }
    ));

    let invalid = DebeziumEvent::from_value(&terracedb::Value::bytes(vec![9]))
        .expect_err("unknown format versions should not decode");
    assert!(matches!(
        invalid,
        DebeziumCodecError::UnknownVersion {
            subject: "normalized event",
            ..
        }
    ));
}

#[test]
fn postgres_decoder_accepts_first_snapshot_marker() {
    let decoder = PostgresDebeziumDecoder::new().with_topic_binding(
        "dbserver1.public.orders",
        DebeziumSourceTable::new("app", "public", "orders"),
    );

    let mut snapshot = terracedb_kafka::KafkaRecord::new("dbserver1.public.orders", 0, 7);
    snapshot.key = Some(br#"{"payload":{"id":"101"}}"#.to_vec());
    snapshot.value = Some(
        br#"{
            "payload":{
                "before":null,
                "after":{"id":"101","region":"west","status":"open"},
                "op":"r",
                "ts_ms":1700000000002,
                "source":{
                    "db":"app",
                    "schema":"public",
                    "table":"orders",
                    "snapshot":"first",
                    "ts_ms":1700000000001
                }
            }
        }"#
        .to_vec(),
    );

    let decoded = decoder
        .decode_record(&snapshot)
        .expect("snapshot markers emitted by Debezium should decode");
    assert_eq!(decoded.snapshot, Some(DebeziumSnapshotMarker::First));
    assert_eq!(decoded.operation(), Some(DebeziumOperationKind::Read));
}

#[test]
fn postgres_decoder_accepts_last_in_data_collection_snapshot_marker() {
    let decoder = PostgresDebeziumDecoder::new().with_topic_binding(
        "dbserver1.public.orders",
        DebeziumSourceTable::new("app", "public", "orders"),
    );

    let mut snapshot = terracedb_kafka::KafkaRecord::new("dbserver1.public.orders", 0, 8);
    snapshot.key = Some(br#"{"payload":{"id":"102"}}"#.to_vec());
    snapshot.value = Some(
        br#"{
            "payload":{
                "before":null,
                "after":{"id":"102","region":"west","status":"open"},
                "op":"r",
                "ts_ms":1700000000002,
                "source":{
                    "db":"app",
                    "schema":"public",
                    "table":"orders",
                    "snapshot":"last_in_data_collection",
                    "ts_ms":1700000000001
                }
            }
        }"#
        .to_vec(),
    );

    let decoded = decoder
        .decode_record(&snapshot)
        .expect("per-table snapshot boundary markers should decode");
    assert_eq!(
        decoded.snapshot,
        Some(DebeziumSnapshotMarker::LastInDataCollection)
    );
    assert_eq!(decoded.operation(), Some(DebeziumOperationKind::Read));
}

#[tokio::test]
async fn layout_helpers_open_projection_and_workflow_sources_without_kafka() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config("/debezium-layout-contracts"),
        test_dependencies(file_system, object_store),
    )
    .await
    .expect("open db");

    let layout = DebeziumPartitionedTableLayout::new(
        "debezium",
        "dbserver1.public.orders",
        DebeziumSourceTable::new("app", "public", "orders"),
        [0_u32, 1_u32],
    );

    for table_name in layout.cdc_table_names() {
        db.create_table(row_table_config(&table_name))
            .await
            .expect("create cdc table");
    }
    db.create_table(row_table_config(layout.current_table_name()))
        .await
        .expect("create current table");

    let projection_sources = layout.projection_sources(&db);
    assert_eq!(
        projection_sources
            .iter()
            .map(|table| table.name().to_string())
            .collect::<Vec<_>>(),
        layout.cdc_table_names()
    );

    let workflow_sources = layout.event_log_workflow_sources(&db);
    assert_eq!(workflow_sources.len(), 2);
    assert_eq!(
        workflow_sources[0].config(),
        &event_log_workflow_source_config()
    );

    let mirror_source = layout.mirror_workflow_source(&db);
    assert_eq!(mirror_source.table().name(), layout.current_table_name());
    assert_eq!(mirror_source.config(), &mirror_workflow_source_config());

    let decoder = PostgresDebeziumDecoder::new().with_layouts([&layout]);
    assert_eq!(
        decoder.topic_binding(layout.topic()),
        Some(layout.source_table())
    );
}
