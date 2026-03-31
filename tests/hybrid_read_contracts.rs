use std::{collections::BTreeMap, sync::Arc};

use futures::StreamExt;
use terracedb::{
    BaseZoneMapPruner, ByteRange, ColumnarCompression, ColumnarDecodeField, ColumnarDecodeMetadata,
    ColumnarEncoding, ColumnarFooter, ColumnarFooterPageDirectoryLoader, ColumnarFormatTag,
    ColumnarGranuleRef, ColumnarGranuleSynopsis, ColumnarHeader, ColumnarMark, ColumnarMarkOffset,
    ColumnarPageDirectory, ColumnarPageRef, ColumnarSequenceBounds, ColumnarSubstreamKind,
    ColumnarSubstreamRef, ColumnarSynopsisSidecar, CompactPartDigest,
    CompactToWidePromotionCandidate, CompactToWidePromotionDecision, CompactToWidePromotionPolicy,
    Db, DbConfig, DbDependencies, FieldDefinition, FieldId, FieldType, FieldValue, HybridKeyRange,
    HybridPartDescriptor, HybridSynopsisPruner, InMemoryRawByteSegmentCache,
    LateMaterializationPlan, PartDigestAlgorithm, PartRepairController,
    ProjectionSidecarDescriptor, RawByteSegmentCache, RepairState, RowProjection, S3Location,
    ScanOptions, SchemaDefinition, SkipIndexSidecarDescriptor, SsdConfig, StorageConfig,
    StorageSource, StubClock, StubColumnarFooterPageDirectoryLoader, StubFileSystem,
    StubObjectStore, StubPartRepairController, StubRng, TableConfig, TableFormat,
    TieredDurabilityMode, TieredStorageConfig, Value, ZoneMapPredicate, ZoneMapSynopsis,
};
use terracedb_projections::{PROJECTION_CURSOR_TABLE_NAME, ProjectionRuntime};
use terracedb_simulation::{
    HybridPredicate, HybridReadMutation, HybridReadOracle, HybridReadWorkloadGenerator,
    HybridSegmentCacheModel, HybridSidecarKind, HybridSidecarState, HybridTableSpec,
    HybridWorkloadConfig,
};

fn tiered_config(path: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "hybrid-contracts".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability: TieredDurabilityMode::GroupCommit,
            local_retention: terracedb::TieredLocalRetentionMode::Offload,
        }),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

fn dependencies() -> DbDependencies {
    DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(41)),
    )
}

fn row_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Row,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(8),
        history_retention_sequences: None,
        compaction_strategy: terracedb::CompactionStrategy::Leveled,
        schema: None,
        metadata: Default::default(),
    }
}

fn metric_schema() -> SchemaDefinition {
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

fn columnar_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Columnar,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(8),
        history_retention_sequences: None,
        compaction_strategy: terracedb::CompactionStrategy::Tiered,
        schema: Some(metric_schema()),
        metadata: Default::default(),
    }
}

fn metric_record(user_id: &str, count: i64, active: Option<bool>) -> Value {
    let mut record = BTreeMap::from([
        (FieldId::new(1), FieldValue::String(user_id.to_string())),
        (FieldId::new(2), FieldValue::Int64(count)),
    ]);
    record.insert(
        FieldId::new(3),
        active.map(FieldValue::Bool).unwrap_or(FieldValue::Null),
    );
    Value::record(record)
}

fn sample_digest() -> CompactPartDigest {
    CompactPartDigest {
        format_tag: ColumnarFormatTag::compact_digest(),
        algorithm: PartDigestAlgorithm::Crc32,
        logical_bytes: 96,
        digest_bytes: vec![0xde, 0xad, 0xbe, 0xef],
    }
}

fn sample_loader() -> StubColumnarFooterPageDirectoryLoader {
    StubColumnarFooterPageDirectoryLoader::new(
        ColumnarHeader {
            format_tag: ColumnarFormatTag::base_part(),
            table_id: terracedb::TableId::new(9),
            local_id: "SST-000009".to_string(),
            schema_version: 1,
            part_digest: sample_digest(),
        },
        ColumnarFooter {
            format_tag: ColumnarFormatTag::base_part(),
            table_id: terracedb::TableId::new(9),
            local_id: "SST-000009".to_string(),
            schema_version: 1,
            row_count: 2,
            data_range: ByteRange::new(8, 64),
            decode_metadata: ColumnarDecodeMetadata {
                schema_version: 1,
                fields: vec![ColumnarDecodeField {
                    field_id: FieldId::new(2),
                    field_type: FieldType::Int64,
                    nullable: false,
                    has_default: true,
                }],
            },
            substreams: vec![ColumnarSubstreamRef {
                ordinal: 0,
                field_id: Some(FieldId::new(2)),
                field_type: Some(FieldType::Int64),
                kind: ColumnarSubstreamKind::Int64Values,
                encoding: ColumnarEncoding::Plain,
                compression: ColumnarCompression::None,
                range: ByteRange::new(8, 24),
                checksum: 7,
            }],
            marks: vec![ColumnarMark {
                granule_index: 0,
                page_index: 0,
                row_ordinal: 0,
                offsets: vec![ColumnarMarkOffset {
                    substream_ordinal: 0,
                    offset: 8,
                }],
            }],
            synopsis: ColumnarSynopsisSidecar {
                format_tag: ColumnarFormatTag::synopsis_sidecar(),
                part_local_id: "SST-000009".to_string(),
                granules: vec![ColumnarGranuleSynopsis {
                    granule_index: 0,
                    row_range: ByteRange::new(0, 2),
                    zone_maps: vec![ZoneMapSynopsis {
                        field_id: FieldId::new(2),
                        min_value: Some(FieldValue::Int64(1)),
                        max_value: Some(FieldValue::Int64(9)),
                        null_count: 0,
                    }],
                }],
                checksum: 11,
            },
            optional_sidecars: vec![
                terracedb::ColumnarOptionalSidecar::SkipIndex(SkipIndexSidecarDescriptor {
                    format_tag: ColumnarFormatTag::skip_index_sidecar(),
                    part_local_id: "SST-000009".to_string(),
                    index_name: "count_ge_5".to_string(),
                    checksum: 12,
                }),
                terracedb::ColumnarOptionalSidecar::Projection(ProjectionSidecarDescriptor {
                    format_tag: ColumnarFormatTag::projection_sidecar(),
                    part_local_id: "SST-000009".to_string(),
                    projection_name: "count_only".to_string(),
                    projected_fields: vec![FieldId::new(2)],
                    checksum: 13,
                }),
            ],
            digests: vec![sample_digest()],
        },
        ColumnarPageDirectory {
            granules: vec![ColumnarGranuleRef {
                granule_index: 0,
                first_key: b"user:1".to_vec(),
                row_range: ByteRange::new(0, 2),
                page_range: ByteRange::new(0, 1),
                sequence_bounds: ColumnarSequenceBounds {
                    min_sequence: terracedb::SequenceNumber::new(1),
                    max_sequence: terracedb::SequenceNumber::new(2),
                },
                has_tombstones: false,
            }],
            pages: vec![ColumnarPageRef {
                granule_index: 0,
                substream_ordinal: 0,
                page_ordinal: 0,
                first_key: b"user:1".to_vec(),
                range: ByteRange::new(8, 24),
                row_range: ByteRange::new(0, 2),
                sequence_bounds: ColumnarSequenceBounds {
                    min_sequence: terracedb::SequenceNumber::new(1),
                    max_sequence: terracedb::SequenceNumber::new(2),
                },
                has_tombstones: false,
            }],
        },
    )
}

fn assert_loader<T: ColumnarFooterPageDirectoryLoader>(_loader: &T) {}
fn assert_cache<T: RawByteSegmentCache>(_cache: &T) {}
fn assert_repair<T: PartRepairController>(_repair: &T) {}
fn assert_policy<T: CompactToWidePromotionPolicy>(_policy: &T) {}

async fn collect_rows(stream: terracedb::KvStream) -> Vec<(Vec<u8>, Value)> {
    stream.collect::<Vec<_>>().await
}

fn record_i64(value: &Value, field_id: FieldId) -> Option<i64> {
    let Value::Record(record) = value else {
        return None;
    };
    match record.get(&field_id) {
        Some(FieldValue::Int64(value)) => Some(*value),
        _ => None,
    }
}

#[tokio::test]
async fn hybrid_contract_boundaries_compile_across_core_projection_and_simulation() {
    let loader = sample_loader();
    let cache = InMemoryRawByteSegmentCache::default();
    let repair = StubPartRepairController::default();
    let policy = terracedb::ConservativeCompactToWidePolicy::default();
    let generator = HybridReadWorkloadGenerator::new(0x4848);
    let scenario = generator.generate(&HybridWorkloadConfig::default());
    let _oracle = HybridReadOracle::new(&[HybridTableSpec::row("events")]);
    let _cache_model = HybridSegmentCacheModel::default();
    let _projection_runtime_type = std::any::type_name::<ProjectionRuntime>();
    let _projection_table_name = PROJECTION_CURSOR_TABLE_NAME;

    assert_loader(&loader);
    assert_cache(&cache);
    assert_repair(&repair);
    assert_policy(&policy);

    let source = StorageSource::remote_object("cold/table-000009/SST-000009.sst");
    let footer = loader.load_footer(&source).await.expect("load stub footer");
    cache
        .fill(source.target(), ByteRange::new(8, 24), b"count-bytes")
        .await
        .expect("fill in-memory cache");
    assert_eq!(
        cache
            .lookup(source.target(), ByteRange::new(8, 24))
            .await
            .expect("lookup in-memory cache"),
        Some(b"count-bytes".to_vec())
    );
    assert_eq!(
        repair
            .verify(&HybridPartDescriptor {
                table_id: terracedb::TableId::new(9),
                local_id: footer.local_id.clone(),
                source,
                format_tag: footer.format_tag,
                digests: footer.digests.clone(),
            })
            .await
            .expect("verify stub repair"),
        RepairState::Verified
    );
    assert_eq!(
        policy.decide(&CompactToWidePromotionCandidate {
            table_id: terracedb::TableId::new(9),
            local_id: footer.local_id.clone(),
            row_count: footer.row_count,
            projected_read_count: 5,
            full_row_read_count: 8,
            projected_bytes_read: 64,
        }),
        CompactToWidePromotionDecision::KeepCompact
    );
    assert!(!scenario.operations.is_empty());
}

#[tokio::test]
async fn hybrid_read_oracle_matches_current_row_and_columnar_behavior() {
    let db = Db::open(tiered_config("/hybrid-read-oracle"), dependencies())
        .await
        .expect("open db");
    let events = db
        .create_table(row_table_config("events"))
        .await
        .expect("create row table");
    let metrics = db
        .create_table(columnar_table_config("metrics"))
        .await
        .expect("create columnar table");

    let event1 = events
        .write(b"event:1".to_vec(), Value::bytes(b"alpha".to_vec()))
        .await
        .expect("write event 1");
    let event2 = events
        .write(b"event:2".to_vec(), Value::bytes(b"beta".to_vec()))
        .await
        .expect("write event 2");
    let metric1 = metrics
        .write(b"user:1".to_vec(), metric_record("alice", 3, Some(false)))
        .await
        .expect("write metric 1");
    let metric2 = metrics
        .write(b"user:2".to_vec(), metric_record("bob", 8, Some(true)))
        .await
        .expect("write metric 2");
    let metric3 = metrics
        .write(b"user:3".to_vec(), metric_record("carol", 12, None))
        .await
        .expect("write metric 3");
    db.flush().await.expect("flush");

    let visible = db.current_sequence();
    assert_eq!(visible, metric3);

    let mut oracle = HybridReadOracle::new(&[
        HybridTableSpec::row("events"),
        HybridTableSpec::columnar("metrics", metric_schema()),
    ]);
    oracle
        .apply(
            event1,
            HybridReadMutation::Put {
                table: "events".to_string(),
                key: b"event:1".to_vec(),
                value: Value::bytes(b"alpha".to_vec()),
            },
        )
        .expect("oracle event 1");
    oracle
        .apply(
            event2,
            HybridReadMutation::Put {
                table: "events".to_string(),
                key: b"event:2".to_vec(),
                value: Value::bytes(b"beta".to_vec()),
            },
        )
        .expect("oracle event 2");
    oracle
        .apply(
            metric1,
            HybridReadMutation::Put {
                table: "metrics".to_string(),
                key: b"user:1".to_vec(),
                value: metric_record("alice", 3, Some(false)),
            },
        )
        .expect("oracle metric 1");
    oracle
        .apply(
            metric2,
            HybridReadMutation::Put {
                table: "metrics".to_string(),
                key: b"user:2".to_vec(),
                value: metric_record("bob", 8, Some(true)),
            },
        )
        .expect("oracle metric 2");
    oracle
        .apply(
            metric3,
            HybridReadMutation::Put {
                table: "metrics".to_string(),
                key: b"user:3".to_vec(),
                value: metric_record("carol", 12, None),
            },
        )
        .expect("oracle metric 3");

    assert_eq!(
        events
            .read(b"event:1".to_vec())
            .await
            .expect("read event 1"),
        oracle
            .read("events", b"event:1", visible, &RowProjection::FullRow)
            .expect("oracle event read")
    );
    assert_eq!(
        metrics
            .read(b"user:2".to_vec())
            .await
            .expect("read metric 2"),
        oracle
            .read("metrics", b"user:2", visible, &RowProjection::FullRow)
            .expect("oracle metric read")
    );

    let actual_row_scan = collect_rows(
        events
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("scan row table"),
    )
    .await;
    let expected_row_scan = oracle
        .scan("events", visible, &RowProjection::FullRow)
        .expect("oracle row scan");
    assert_eq!(actual_row_scan, expected_row_scan);

    let actual_projection = collect_rows(
        metrics
            .scan(
                Vec::new(),
                vec![0xff],
                ScanOptions {
                    columns: Some(vec!["count".to_string()]),
                    ..ScanOptions::default()
                },
            )
            .await
            .expect("scan projected columnar rows"),
    )
    .await;
    let expected_projection = oracle
        .scan(
            "metrics",
            visible,
            &RowProjection::Fields(vec![FieldId::new(2)]),
        )
        .expect("oracle projected scan");
    assert_eq!(actual_projection, expected_projection);

    let (mask, expected_filtered) = oracle
        .scan_with_selection(
            "metrics",
            visible,
            &RowProjection::Fields(vec![FieldId::new(2)]),
            &HybridPredicate::Int64AtLeast {
                field_id: FieldId::new(2),
                value: 8,
            },
        )
        .expect("oracle filtered scan");
    let actual_filtered = actual_projection
        .into_iter()
        .filter(|(_, value)| record_i64(value, FieldId::new(2)).is_some_and(|count| count >= 8))
        .collect::<Vec<_>>();
    assert_eq!(expected_filtered, actual_filtered);
    assert_eq!(mask.selected_count(), actual_filtered.len());

    let missing_sidecar = HybridReadOracle::resolve_sidecar_fallback(
        HybridSidecarKind::Projection,
        HybridSidecarState::Absent,
    );
    let corrupt_sidecar = HybridReadOracle::resolve_sidecar_fallback(
        HybridSidecarKind::SkipIndex,
        HybridSidecarState::Corrupt,
    );
    assert!(missing_sidecar.fallback_to_base);
    assert!(corrupt_sidecar.fallback_to_base);
}

#[test]
fn hybrid_pruning_and_late_materialization_contracts_compile_together() {
    let loader = sample_loader();
    let footer = futures::executor::block_on(loader.load_footer(&StorageSource::remote_object(
        "cold/table-000009/SST-000009.sst",
    )))
    .expect("load footer");
    let page_directory = futures::executor::block_on(loader.load_page_directory(
        &StorageSource::remote_object("cold/table-000009/SST-000009.sst"),
        footer.as_ref(),
    ))
    .expect("load page directory");
    let plan = LateMaterializationPlan::new(
        RowProjection::Fields(vec![FieldId::new(2)]),
        RowProjection::Fields(vec![FieldId::new(1), FieldId::new(2), FieldId::new(3)]),
    );
    let outcome = BaseZoneMapPruner
        .prune(
            page_directory.as_ref(),
            &footer.synopsis,
            &HybridKeyRange {
                start_inclusive: Some(b"user:1".to_vec()),
                end_exclusive: Some(b"user:9".to_vec()),
            },
            &ZoneMapPredicate::Int64AtLeast {
                field_id: FieldId::new(2),
                value: 5,
            },
        )
        .expect("prune hybrid metadata");

    assert!(plan.needs_late_materialization());
    assert_eq!(
        plan.late_projection,
        RowProjection::Fields(vec![FieldId::new(1), FieldId::new(3)])
    );
    assert_eq!(outcome.selected.len(), 1);
    assert_eq!(outcome.selected[0].granule.granule_index, 0);
    assert_eq!(outcome.stats.selected_row_upper_bound, 2);
}

#[test]
fn hybrid_oracle_exposes_pruning_and_staged_scan_expectations() {
    let mut oracle =
        HybridReadOracle::new(&[HybridTableSpec::columnar("metrics", metric_schema())]);
    oracle
        .apply(
            terracedb::SequenceNumber::new(1),
            HybridReadMutation::Put {
                table: "metrics".to_string(),
                key: b"user:1".to_vec(),
                value: metric_record("alice", 3, Some(false)),
            },
        )
        .expect("oracle row 1");
    oracle
        .apply(
            terracedb::SequenceNumber::new(2),
            HybridReadMutation::Put {
                table: "metrics".to_string(),
                key: b"user:2".to_vec(),
                value: metric_record("bob", 8, Some(true)),
            },
        )
        .expect("oracle row 2");
    oracle
        .apply(
            terracedb::SequenceNumber::new(3),
            HybridReadMutation::Put {
                table: "metrics".to_string(),
                key: b"user:3".to_vec(),
                value: metric_record("carol", 12, None),
            },
        )
        .expect("oracle row 3");

    let pruning = oracle
        .pruning_expectation(
            "metrics",
            terracedb::SequenceNumber::new(3),
            &HybridPredicate::Int64AtLeast {
                field_id: FieldId::new(2),
                value: 8,
            },
            2,
        )
        .expect("oracle pruning expectation");
    let staged = oracle
        .staged_scan_with_selection(
            "metrics",
            terracedb::SequenceNumber::new(3),
            RowProjection::Fields(vec![FieldId::new(2)]),
            RowProjection::Fields(vec![FieldId::new(1), FieldId::new(2)]),
            &HybridPredicate::Int64AtLeast {
                field_id: FieldId::new(2),
                value: 8,
            },
        )
        .expect("oracle staged scan");

    assert_eq!(pruning.selected_granules, vec![0, 1]);
    assert_eq!(pruning.survivor_rows, 2);
    assert!(pruning.overread_rows_upper_bound >= pruning.survivor_rows as u64);
    assert_eq!(staged.selection.selected_count(), 2);
    assert_eq!(staged.survivors.len(), 2);
}
