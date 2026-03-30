use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use terracedb::{
    ChangeFeedError, Clock, CommitOptions, CompactionStrategy,
    DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT, DbConfig, FieldDefinition, FieldId, FieldType,
    FieldValue, FileSystem, FileSystemFailure, FileSystemOperation, LogCursor, ManifestId,
    ObjectKeyLayout, ObjectStore, ObjectStoreOperation, OpenError, PendingWork, PendingWorkType,
    RemoteCache, RemoteRecoveryHint, S3Location, S3PrimaryStorageConfig, ScanOptions,
    ScheduleAction, ScheduleDecision, Scheduler, SchemaDefinition, SequenceNumber, SsdConfig,
    StorageConfig, StorageErrorKind, StorageSource, TableConfig, TableFormat, TableStats,
    ThrottleDecision, TieredDurabilityMode, TieredStorageConfig, Transaction, UnifiedStorage,
    Value,
};
use terracedb_simulation::{
    CutPoint, DbGeneratedScenario, DbMutation, DbOracleChange, DbShadowOracle,
    DbSimulationScenarioConfig, DbWorkloadOperation, ObjectStoreFaultSpec, OperationResult,
    PointMutation, ScheduledFault, ScheduledFaultKind, SeededSimulationRunner, ShadowOracle,
    SimulationContext, SimulationMergeOperatorId, SimulationScenarioConfig, SimulationTableSpec,
    StubDbProcess, TraceEvent,
};

fn ttl_value(expires_at_millis: u64, payload: &str) -> Value {
    let mut encoded = expires_at_millis.to_be_bytes().to_vec();
    encoded.extend_from_slice(payload.as_bytes());
    Value::Bytes(encoded)
}

fn bytes(payload: &str) -> Value {
    Value::Bytes(payload.as_bytes().to_vec())
}

#[derive(Default)]
struct HostileSimulationScheduler;

impl Scheduler for HostileSimulationScheduler {
    fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
        work.iter()
            .map(|work| ScheduleDecision {
                work_id: work.id.clone(),
                action: ScheduleAction::Defer,
            })
            .collect()
    }

    fn should_throttle(&self, _table: &terracedb::Table, _stats: &TableStats) -> ThrottleDecision {
        ThrottleDecision::default()
    }
}

struct RandomSimulationScheduler {
    state: AtomicU64,
}

impl RandomSimulationScheduler {
    fn seeded(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed.max(1)),
        }
    }

    fn next_u64(&self) -> u64 {
        let mut current = self.state.load(Ordering::SeqCst);
        loop {
            let mut next = current;
            next ^= next << 7;
            next ^= next >> 9;
            next ^= next << 8;
            if self
                .state
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return next;
            }
            current = self.state.load(Ordering::SeqCst);
        }
    }
}

impl Scheduler for RandomSimulationScheduler {
    fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
        let selected = (!work.is_empty()).then(|| (self.next_u64() as usize) % work.len());
        let force_defer = self.next_u64().is_multiple_of(3);

        work.iter()
            .enumerate()
            .map(|(index, work)| ScheduleDecision {
                work_id: work.id.clone(),
                action: if !force_defer && selected == Some(index) {
                    ScheduleAction::Execute
                } else {
                    ScheduleAction::Defer
                },
            })
            .collect()
    }

    fn should_throttle(&self, _table: &terracedb::Table, stats: &TableStats) -> ThrottleDecision {
        if stats.l0_sstable_count >= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT.saturating_sub(1)
            || self.next_u64().is_multiple_of(5)
        {
            return ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: None,
                stall: false,
            };
        }

        ThrottleDecision::default()
    }
}

#[derive(Default)]
struct OffloadSimulationScheduler;

impl Scheduler for OffloadSimulationScheduler {
    fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
        let selected = work
            .iter()
            .find(|work| work.work_type == PendingWorkType::Offload)
            .map(|work| work.id.clone());
        work.iter()
            .map(|work| ScheduleDecision {
                work_id: work.id.clone(),
                action: if selected.as_ref() == Some(&work.id) {
                    ScheduleAction::Execute
                } else {
                    ScheduleAction::Defer
                },
            })
            .collect()
    }

    fn should_throttle(&self, _table: &terracedb::Table, _stats: &TableStats) -> ThrottleDecision {
        ThrottleDecision::default()
    }
}

fn simulation_db_config(
    root_path: &str,
    scheduler: Arc<dyn Scheduler>,
    max_local_bytes: u64,
) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: root_path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-sim".to_string(),
                prefix: "scheduler".to_string(),
            },
            max_local_bytes,
            durability: TieredDurabilityMode::GroupCommit,
        }),
        scheduler: Some(scheduler),
    }
}

fn simulation_tiered_config(root_path: &str, durability: TieredDurabilityMode) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: root_path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-sim".to_string(),
                prefix: "cdc".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability,
        }),
        scheduler: None,
    }
}

fn simulation_s3_primary_config(prefix: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::S3Primary(S3PrimaryStorageConfig {
            s3: S3Location {
                bucket: "terracedb-sim".to_string(),
                prefix: prefix.to_string(),
            },
            mem_cache_size_bytes: 1024 * 1024,
            auto_flush_interval: None,
        }),
        scheduler: None,
    }
}

async fn collect_change_feed(stream: terracedb::ChangeStream) -> Vec<DbOracleChange> {
    stream
        .map(|entry| {
            entry.map(|entry| DbOracleChange {
                table: entry.table.name().to_string(),
                key: entry.key,
                value: entry.value.map(|value| match value {
                    Value::Bytes(bytes) => bytes,
                    Value::Record(_) => {
                        panic!("simulation change-feed tests only support byte row values")
                    }
                }),
                cursor: entry.cursor,
                sequence: entry.sequence,
                kind: entry.kind,
            })
        })
        .try_collect::<Vec<_>>()
        .await
        .expect("collect change feed")
}

fn collected_sequences(changes: &[DbOracleChange]) -> Vec<SequenceNumber> {
    changes.iter().map(|change| change.sequence).collect()
}

fn next_schedule_u64(state: &mut u64) -> u64 {
    let mut next = (*state).max(1);
    next ^= next << 7;
    next ^= next >> 9;
    next ^= next << 8;
    *state = next;
    next
}

async fn simulated_active_commit_log_segment_path(
    context: &SimulationContext,
    root: &str,
) -> Result<String, terracedb::StorageError> {
    let mut candidates = context
        .file_system()
        .list(root)
        .await?
        .into_iter()
        .filter(|path| path.contains("/commitlog/SEG-"))
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.pop().ok_or_else(|| {
        terracedb::StorageError::not_found(format!(
            "missing active commit-log segment under {root}"
        ))
    })
}

async fn assert_simulated_failed_sequence_invariants(
    db: &terracedb::Db,
    table: &terracedb::Table,
    successful_sequences: &[SequenceNumber],
    failed_sequences: &BTreeSet<SequenceNumber>,
    previous_visible: &mut SequenceNumber,
    previous_durable: &mut SequenceNumber,
) -> turmoil::Result<()> {
    let visible = db.current_sequence();
    let durable = db.current_durable_sequence();
    assert!(visible >= *previous_visible);
    assert!(durable >= *previous_durable);
    assert!(durable <= visible);
    *previous_visible = visible;
    *previous_durable = durable;

    let visible_changes = collect_change_feed(
        db.scan_since(table, LogCursor::beginning(), ScanOptions::default())
            .await?,
    )
    .await;
    let durable_changes = collect_change_feed(
        db.scan_durable_since(table, LogCursor::beginning(), ScanOptions::default())
            .await?,
    )
    .await;
    assert_eq!(collected_sequences(&visible_changes), successful_sequences);
    assert_eq!(collected_sequences(&durable_changes), successful_sequences);
    assert!(
        visible_changes
            .iter()
            .all(|change| !failed_sequences.contains(&change.sequence))
    );
    assert!(
        durable_changes
            .iter()
            .all(|change| !failed_sequences.contains(&change.sequence))
    );

    Ok(())
}
#[test]
fn simulation_harness_replays_same_seed() -> turmoil::Result {
    let config = SimulationScenarioConfig {
        steps: 10,
        path_count: 3,
        key_count: 3,
        max_payload_len: 8,
        max_clock_advance_millis: 4,
    };

    let first = SeededSimulationRunner::new(0x5151)
        .with_scenario_config(config.clone())
        .run_generated()?;
    let second = SeededSimulationRunner::new(0x5151)
        .with_scenario_config(config)
        .run_generated()?;

    assert_eq!(first.scenario.workload, second.scenario.workload);
    assert_eq!(first.scenario.faults, second.scenario.faults);
    assert_eq!(first.trace, second.trace);

    Ok(())
}

#[test]
fn simulation_harness_changes_shape_for_different_seeds() -> turmoil::Result {
    let config = SimulationScenarioConfig {
        steps: 10,
        path_count: 3,
        key_count: 3,
        max_payload_len: 8,
        max_clock_advance_millis: 4,
    };

    let left = SeededSimulationRunner::new(7)
        .with_scenario_config(config.clone())
        .run_generated()?;
    let right = SeededSimulationRunner::new(8)
        .with_scenario_config(config)
        .run_generated()?;

    assert!(
        left.scenario.workload != right.scenario.workload
            || left.scenario.faults != right.scenario.faults
            || left.trace != right.trace
    );

    Ok(())
}

#[test]
fn stub_db_recovery_matches_oracle_prefix() -> turmoil::Result {
    SeededSimulationRunner::new(0x900d).run_with(|context| async move {
        let dependencies = context.dependencies();
        let mut oracle = ShadowOracle::default();

        let mut stub = StubDbProcess::open(dependencies.clone()).await?;

        let first = PointMutation::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        };
        let first_sequence = stub.apply(first.clone(), true).await?;
        oracle.apply(first_sequence, first.clone(), true);
        context.record(TraceEvent::StubCommit {
            sequence: first_sequence,
            durable_sequence: stub.durable_sequence(),
            mutation: first,
        });

        let second = PointMutation::Put {
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
        };
        let second_sequence = stub.apply(second.clone(), false).await?;
        oracle.apply(second_sequence, second.clone(), false);
        context.record(TraceEvent::StubCommit {
            sequence: second_sequence,
            durable_sequence: stub.durable_sequence(),
            mutation: second,
        });

        assert_eq!(stub.read(b"k1"), Some(b"v1".to_vec()));
        assert_eq!(stub.read(b"k2"), Some(b"v2".to_vec()));

        context.crash_filesystem(CutPoint::AfterStep);
        context.record(TraceEvent::Restart);

        let recovered = StubDbProcess::open(dependencies).await?;
        context.record(TraceEvent::StubRecovered {
            current_sequence: recovered.current_sequence(),
            durable_sequence: recovered.durable_sequence(),
            key_count: recovered.state().len(),
        });

        oracle.validate_sequence_ordering()?;
        oracle.validate_point_state(
            b"k1",
            recovered.current_sequence(),
            recovered.read(b"k1").as_deref(),
        )?;
        let matched = oracle.validate_recovery_prefix(recovered.state())?;
        assert_eq!(matched.durable_sequence, first_sequence);
        assert_eq!(matched.matched_sequence, first_sequence);
        assert_eq!(recovered.read(b"k2"), None);

        Ok(())
    })
}

#[test]
fn columnar_schema_and_normalized_records_survive_simulated_restart() -> turmoil::Result {
    SeededSimulationRunner::new(0x2424).run_with(|context| async move {
        let config = simulation_tiered_config(
            "/terracedb/sim/columnar-schema",
            TieredDurabilityMode::GroupCommit,
        );
        let schema = SchemaDefinition {
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
        };

        let db = context.open_db(config.clone()).await?;
        let metrics = db
            .create_table(TableConfig {
                name: "metrics".to_string(),
                format: TableFormat::Columnar,
                merge_operator: None,
                max_merge_operand_chain_length: None,
                compaction_filter: None,
                bloom_filter_bits_per_key: Some(8),
                history_retention_sequences: Some(16),
                compaction_strategy: CompactionStrategy::Tiered,
                schema: Some(schema.clone()),
                metadata: Default::default(),
            })
            .await
            .expect("create columnar table");

        metrics
            .write(
                b"user:1".to_vec(),
                Value::named_record(
                    &schema,
                    [
                        ("count", FieldValue::Int64(9)),
                        ("user_id", FieldValue::String("alice".to_string())),
                    ],
                )
                .expect("normalize named record"),
            )
            .await
            .expect("write columnar record");

        let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
        let metrics = reopened.table("metrics");
        assert!(metrics.id().is_some());
        assert_eq!(
            metrics
                .read(b"user:1".to_vec())
                .await
                .expect("read recovered"),
            Some(Value::record(BTreeMap::from([
                (FieldId::new(1), FieldValue::String("alice".to_string())),
                (FieldId::new(2), FieldValue::Int64(9)),
                (FieldId::new(3), FieldValue::Null),
            ])))
        );

        Ok(())
    })
}

#[test]
fn db_shadow_oracle_resolves_ordered_merge_history() {
    let specs = vec![SimulationTableSpec::merge_row(
        "events",
        SimulationMergeOperatorId::AppendBytes,
        Some(2),
    )];
    let mut oracle = DbShadowOracle::new(&specs);

    oracle
        .apply(
            SequenceNumber::new(1),
            DbMutation::Put {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"seed".to_vec(),
            },
            true,
        )
        .expect("apply base put");
    oracle
        .apply(
            SequenceNumber::new(2),
            DbMutation::Merge {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"A".to_vec(),
            },
            true,
        )
        .expect("apply merge A");
    oracle
        .apply(
            SequenceNumber::new(3),
            DbMutation::Merge {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"B".to_vec(),
            },
            true,
        )
        .expect("apply merge B");
    oracle
        .apply(
            SequenceNumber::new(4),
            DbMutation::Merge {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"C".to_vec(),
            },
            true,
        )
        .expect("apply merge C");

    assert_eq!(
        oracle
            .value_at("events", b"doc", SequenceNumber::new(1))
            .expect("read base sequence"),
        Some(b"seed".to_vec())
    );
    assert_eq!(
        oracle
            .value_at("events", b"doc", SequenceNumber::new(2))
            .expect("read first merge sequence"),
        Some(b"seed|A".to_vec())
    );
    assert_eq!(
        oracle
            .value_at("events", b"doc", SequenceNumber::new(3))
            .expect("read second merge sequence"),
        Some(b"seed|A|B".to_vec())
    );
    assert_eq!(
        oracle
            .value_at("events", b"doc", SequenceNumber::new(4))
            .expect("read third merge sequence"),
        Some(b"seed|A|B|C".to_vec())
    );
}

#[test]
fn db_shadow_oracle_matches_merge_recovery_prefix() {
    let specs = vec![SimulationTableSpec::merge_row(
        "events",
        SimulationMergeOperatorId::AppendBytes,
        Some(2),
    )];
    let mut oracle = DbShadowOracle::new(&specs);

    oracle
        .apply(
            SequenceNumber::new(1),
            DbMutation::Put {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"seed".to_vec(),
            },
            true,
        )
        .expect("apply base put");
    oracle
        .apply(
            SequenceNumber::new(2),
            DbMutation::Merge {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"A".to_vec(),
            },
            true,
        )
        .expect("apply durable merge");
    oracle
        .apply(
            SequenceNumber::new(3),
            DbMutation::Merge {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"B".to_vec(),
            },
            false,
        )
        .expect("apply non-durable merge");

    let recovered = oracle
        .table_state_at("events", SequenceNumber::new(2))
        .expect("recover durable prefix");
    let matched = oracle
        .validate_recovery_prefix("events", &recovered)
        .expect("validate recovery prefix");
    assert_eq!(matched.durable_sequence, SequenceNumber::new(2));
    assert_eq!(matched.matched_sequence, SequenceNumber::new(2));
}

#[test]
fn db_change_feed_simulation_tracks_visible_durable_and_crash_recovery() -> turmoil::Result {
    let events_spec = SimulationTableSpec::row("events");
    let audit_spec = SimulationTableSpec::row("audit");
    let specs = vec![events_spec.clone(), audit_spec.clone()];

    SeededSimulationRunner::new(0xcdc1).run_with(move |context| {
        let specs = specs.clone();
        let events_spec = events_spec.clone();
        let audit_spec = audit_spec.clone();

        async move {
            let config = simulation_tiered_config(
                "/terracedb/sim/cdc-deferred",
                TieredDurabilityMode::Deferred,
            );
            let db = context.open_db(config.clone()).await?;
            let events = db.create_table(events_spec.table_config()).await?;
            let audit = db.create_table(audit_spec.table_config()).await?;
            let mut oracle = DbShadowOracle::new(&specs);

            let mut batch = db.write_batch();
            batch.put(&events, b"user:1".to_vec(), bytes("v1"));
            batch.put(&audit, b"audit:1".to_vec(), bytes("ignore"));
            batch.delete(&events, b"user:2".to_vec());
            batch.put(&events, b"user:3".to_vec(), bytes("v3"));
            let first_sequence = db.commit(batch, CommitOptions::default()).await?;

            oracle.apply_with_cursor(
                LogCursor::new(first_sequence, 0),
                DbMutation::Put {
                    table: "events".to_string(),
                    key: b"user:1".to_vec(),
                    value: b"v1".to_vec(),
                },
                false,
            )?;
            oracle.apply_with_cursor(
                LogCursor::new(first_sequence, 1),
                DbMutation::Put {
                    table: "audit".to_string(),
                    key: b"audit:1".to_vec(),
                    value: b"ignore".to_vec(),
                },
                false,
            )?;
            oracle.apply_with_cursor(
                LogCursor::new(first_sequence, 2),
                DbMutation::Delete {
                    table: "events".to_string(),
                    key: b"user:2".to_vec(),
                },
                false,
            )?;
            oracle.apply_with_cursor(
                LogCursor::new(first_sequence, 3),
                DbMutation::Put {
                    table: "events".to_string(),
                    key: b"user:3".to_vec(),
                    value: b"v3".to_vec(),
                },
                false,
            )?;

            let visible = collect_change_feed(
                db.scan_since(&events, LogCursor::beginning(), ScanOptions::default())
                    .await?,
            )
            .await;
            assert_eq!(
                visible,
                oracle.visible_changes_since("events", LogCursor::beginning())?
            );

            let durable_before_flush = collect_change_feed(
                db.scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
                    .await?,
            )
            .await;
            assert_eq!(
                durable_before_flush,
                oracle.durable_changes_since("events", LogCursor::beginning())?
            );
            assert!(durable_before_flush.is_empty());

            db.flush().await?;
            oracle.mark_durable_through(db.current_durable_sequence());

            let durable_after_flush = collect_change_feed(
                db.scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
                    .await?,
            )
            .await;
            assert_eq!(
                durable_after_flush,
                oracle.durable_changes_since("events", LogCursor::beginning())?
            );

            let second_sequence = events.write(b"user:4".to_vec(), bytes("volatile")).await?;
            oracle.apply_with_cursor(
                LogCursor::new(second_sequence, 0),
                DbMutation::Put {
                    table: "events".to_string(),
                    key: b"user:4".to_vec(),
                    value: b"volatile".to_vec(),
                },
                false,
            )?;

            let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
            let reopened_events = reopened.table("events");

            let visible_after_restart = collect_change_feed(
                reopened
                    .scan_since(
                        &reopened_events,
                        LogCursor::beginning(),
                        ScanOptions::default(),
                    )
                    .await?,
            )
            .await;
            assert_eq!(
                visible_after_restart,
                oracle.durable_changes_since("events", LogCursor::beginning())?
            );

            let durable_after_restart = collect_change_feed(
                reopened
                    .scan_durable_since(
                        &reopened_events,
                        LogCursor::beginning(),
                        ScanOptions::default(),
                    )
                    .await?,
            )
            .await;
            assert_eq!(durable_after_restart, visible_after_restart);
            assert_eq!(
                reopened.current_sequence(),
                reopened.current_durable_sequence()
            );

            Ok(())
        }
    })
}

#[test]
fn db_change_feed_simulation_resumes_from_cursor_after_restart() -> turmoil::Result {
    let events_spec = SimulationTableSpec::row("events");
    let audit_spec = SimulationTableSpec::row("audit");
    let specs = vec![events_spec.clone(), audit_spec.clone()];

    SeededSimulationRunner::new(0xcdc2).run_with(move |context| {
        let specs = specs.clone();
        let events_spec = events_spec.clone();
        let audit_spec = audit_spec.clone();

        async move {
            let config = simulation_tiered_config(
                "/terracedb/sim/cdc-resume-restart",
                TieredDurabilityMode::GroupCommit,
            );
            let db = context.open_db(config.clone()).await?;
            let events = db.create_table(events_spec.table_config()).await?;
            let audit = db.create_table(audit_spec.table_config()).await?;
            let mut oracle = DbShadowOracle::new(&specs);

            let mut batch = db.write_batch();
            batch.put(&events, b"user:1".to_vec(), bytes("v1"));
            batch.put(&audit, b"audit:1".to_vec(), bytes("ignore"));
            batch.delete(&events, b"user:2".to_vec());
            batch.put(&events, b"user:3".to_vec(), bytes("v3"));
            let first_sequence = db.commit(batch, CommitOptions::default()).await?;
            oracle.apply_with_cursor(
                LogCursor::new(first_sequence, 0),
                DbMutation::Put {
                    table: "events".to_string(),
                    key: b"user:1".to_vec(),
                    value: b"v1".to_vec(),
                },
                true,
            )?;
            oracle.apply_with_cursor(
                LogCursor::new(first_sequence, 1),
                DbMutation::Put {
                    table: "audit".to_string(),
                    key: b"audit:1".to_vec(),
                    value: b"ignore".to_vec(),
                },
                true,
            )?;
            oracle.apply_with_cursor(
                LogCursor::new(first_sequence, 2),
                DbMutation::Delete {
                    table: "events".to_string(),
                    key: b"user:2".to_vec(),
                },
                true,
            )?;
            oracle.apply_with_cursor(
                LogCursor::new(first_sequence, 3),
                DbMutation::Put {
                    table: "events".to_string(),
                    key: b"user:3".to_vec(),
                    value: b"v3".to_vec(),
                },
                true,
            )?;

            let second_sequence = events.write(b"user:4".to_vec(), bytes("v4")).await?;
            oracle.apply_with_cursor(
                LogCursor::new(second_sequence, 0),
                DbMutation::Put {
                    table: "events".to_string(),
                    key: b"user:4".to_vec(),
                    value: b"v4".to_vec(),
                },
                true,
            )?;

            let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
            let reopened_events = reopened.table("events");
            let expected = oracle.visible_changes_since("events", LogCursor::beginning())?;

            let first_page = collect_change_feed(
                reopened
                    .scan_since(
                        &reopened_events,
                        LogCursor::beginning(),
                        ScanOptions {
                            limit: Some(2),
                            ..ScanOptions::default()
                        },
                    )
                    .await?,
            )
            .await;
            assert_eq!(first_page, expected[..2].to_vec());

            let resumed = collect_change_feed(
                reopened
                    .scan_since(
                        &reopened_events,
                        first_page.last().expect("page should not be empty").cursor,
                        ScanOptions::default(),
                    )
                    .await?,
            )
            .await;
            assert_eq!(resumed, expected[2..].to_vec());

            let durable = collect_change_feed(
                reopened
                    .scan_durable_since(
                        &reopened_events,
                        LogCursor::beginning(),
                        ScanOptions::default(),
                    )
                    .await?,
            )
            .await;
            assert_eq!(durable, expected);

            Ok(())
        }
    })
}

#[test]
fn db_change_feed_simulation_surfaces_snapshot_too_old_for_lagging_tables_after_restart()
-> turmoil::Result {
    let mut slow_spec = SimulationTableSpec::row("slow");
    slow_spec.history_retention_sequences = Some(8);
    let mut fast_spec = SimulationTableSpec::row("fast");
    fast_spec.history_retention_sequences = Some(1);

    SeededSimulationRunner::new(0xcdc3).run_with(move |context| {
        let slow_spec = slow_spec.clone();
        let fast_spec = fast_spec.clone();

        async move {
            let config = simulation_tiered_config(
                "/terracedb/sim/cdc-retention-restart",
                TieredDurabilityMode::GroupCommit,
            );
            let db = context.open_db(config.clone()).await?;
            let slow = db.create_table(slow_spec.table_config()).await?;
            let fast = db.create_table(fast_spec.table_config()).await?;

            let mut first = db.write_batch();
            first.put(&slow, b"slow:1".to_vec(), bytes("s1"));
            first.put(&fast, b"fast:1".to_vec(), bytes("f1"));
            let first_sequence = db.commit(first, CommitOptions::default()).await?;
            db.flush().await?;

            let mut second = db.write_batch();
            second.put(&slow, b"slow:2".to_vec(), bytes("s2"));
            second.put(&fast, b"fast:2".to_vec(), bytes("f2"));
            db.commit(second, CommitOptions::default()).await?;
            db.flush().await?;

            let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
            let reopened_slow = reopened.table("slow");
            let reopened_fast = reopened.table("fast");

            let fast_error = reopened
                .scan_since(
                    &reopened_fast,
                    LogCursor::new(first_sequence, 1),
                    ScanOptions::default(),
                )
                .await
                .err()
                .expect("fast table should be past its retained change-feed floor");
            match fast_error {
                ChangeFeedError::SnapshotTooOld(error) => {
                    assert_eq!(error.requested, first_sequence);
                    assert_eq!(error.oldest_available, SequenceNumber::new(2));
                }
                other => panic!("expected SnapshotTooOld, got {other:?}"),
            }

            let slow_changes = collect_change_feed(
                reopened
                    .scan_since(
                        &reopened_slow,
                        LogCursor::beginning(),
                        ScanOptions::default(),
                    )
                    .await?,
            )
            .await;
            assert_eq!(slow_changes.len(), 2);
            assert_eq!(slow_changes[0].sequence, SequenceNumber::new(1));
            assert_eq!(slow_changes[1].sequence, SequenceNumber::new(2));

            let fast_stats = reopened.table_stats(&reopened_fast).await;
            assert_eq!(
                fast_stats.change_feed_floor_sequence,
                Some(SequenceNumber::new(2))
            );

            Ok(())
        }
    })
}

#[test]
fn s3_primary_simulation_recovers_to_last_durable_prefix() -> turmoil::Result {
    let events_spec = SimulationTableSpec::row("events");
    let specs = vec![events_spec.clone()];

    SeededSimulationRunner::new(0xcdc3).run_with(move |context| {
        let specs = specs.clone();
        let events_spec = events_spec.clone();

        async move {
            let config = simulation_s3_primary_config("sim/s3-primary-prefix");
            let db = context.open_db(config.clone()).await?;
            let events = db.create_table(events_spec.table_config()).await?;
            let mut oracle = DbShadowOracle::new(&specs);

            let durable = events.write(b"user:1".to_vec(), bytes("durable")).await?;
            oracle.apply_with_cursor(
                LogCursor::new(durable, 0),
                DbMutation::Put {
                    table: "events".to_string(),
                    key: b"user:1".to_vec(),
                    value: b"durable".to_vec(),
                },
                false,
            )?;
            db.flush().await?;
            oracle.mark_durable_through(db.current_durable_sequence());

            let visible = events
                .write(b"user:2".to_vec(), bytes("visible-only"))
                .await?;
            oracle.apply_with_cursor(
                LogCursor::new(visible, 0),
                DbMutation::Put {
                    table: "events".to_string(),
                    key: b"user:2".to_vec(),
                    value: b"visible-only".to_vec(),
                },
                false,
            )?;

            assert_eq!(
                collect_change_feed(
                    db.scan_since(&events, LogCursor::beginning(), ScanOptions::default())
                        .await?,
                )
                .await,
                oracle.visible_changes_since("events", LogCursor::beginning())?
            );
            assert_eq!(
                collect_change_feed(
                    db.scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
                        .await?,
                )
                .await,
                oracle.durable_changes_since("events", LogCursor::beginning())?
            );

            let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
            let reopened_events = reopened.table("events");
            assert_eq!(reopened.current_sequence(), durable);
            assert_eq!(reopened.current_durable_sequence(), durable);
            assert_eq!(
                reopened_events.read(b"user:1".to_vec()).await?,
                Some(bytes("durable"))
            );
            assert_eq!(reopened_events.read(b"user:2".to_vec()).await?, None);
            assert_eq!(
                collect_change_feed(
                    reopened
                        .scan_since(
                            &reopened_events,
                            LogCursor::beginning(),
                            ScanOptions::default(),
                        )
                        .await?,
                )
                .await,
                oracle.durable_changes_since("events", LogCursor::beginning())?
            );

            Ok(())
        }
    })
}

#[test]
fn s3_primary_simulation_failed_flush_recovers_last_durable_prefix() -> turmoil::Result {
    SeededSimulationRunner::new(0xcdc4).run_with(|context| async move {
        let config = simulation_s3_primary_config("sim/s3-primary-flush-failure");
        let db = context.open_db(config.clone()).await?;
        let events = db
            .create_table(SimulationTableSpec::row("events").table_config())
            .await?;

        let durable = events.write(b"user:1".to_vec(), bytes("v1")).await?;
        db.flush().await?;

        let visible_only = events.write(b"user:1".to_vec(), bytes("v2")).await?;
        assert_eq!(db.current_sequence(), visible_only);
        assert_eq!(db.current_durable_sequence(), durable);
        assert_eq!(events.read(b"user:1".to_vec()).await?, Some(bytes("v2")));

        let layout = ObjectKeyLayout::new(&S3Location {
            bucket: "terracedb-sim".to_string(),
            prefix: "sim/s3-primary-flush-failure".to_string(),
        });
        context
            .object_store()
            .inject_failure(ObjectStoreFaultSpec::Timeout {
                operation: ObjectStoreOperation::Put,
                target_prefix: layout.backup_manifest(ManifestId::new(2)),
            })
            .await?;

        db.flush()
            .await
            .expect_err("remote manifest upload should fail");
        assert_eq!(db.current_durable_sequence(), durable);

        let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
        let reopened_events = reopened.table("events");
        assert_eq!(reopened.current_sequence(), durable);
        assert_eq!(reopened.current_durable_sequence(), durable);
        assert_eq!(
            reopened_events.read(b"user:1".to_vec()).await?,
            Some(bytes("v1"))
        );

        Ok(())
    })
}

#[test]
fn group_commit_failed_sequence_simulation_preserves_watermark_prefix_invariants() -> turmoil::Result
{
    SeededSimulationRunner::new(0x19e5).run_with(|context| async move {
        let root = "/terracedb/sim/group-failed-sequence-watermarks";
        let config = simulation_tiered_config(root, TieredDurabilityMode::GroupCommit);
        let db = context.open_db(config.clone()).await?;
        let events = db
            .create_table(SimulationTableSpec::row("events").table_config())
            .await?;
        let mut schedule_state = context.seed() ^ 0x5a5a_19e5;
        let mut successful_sequences = Vec::new();
        let mut failed_sequences = BTreeSet::new();
        let mut previous_visible = SequenceNumber::new(0);
        let mut previous_durable = SequenceNumber::new(0);

        let first = events
            .write(b"ok:before-gap".to_vec(), bytes("before-gap"))
            .await?;
        successful_sequences.push(first);
        assert_simulated_failed_sequence_invariants(
            &db,
            &events,
            &successful_sequences,
            &failed_sequences,
            &mut previous_visible,
            &mut previous_durable,
        )
        .await?;

        let sync_target = simulated_active_commit_log_segment_path(&context, root).await?;
        context
            .file_system()
            .inject_failure(FileSystemFailure::timeout(
                FileSystemOperation::Sync,
                sync_target,
            ));
        events
            .write(b"failed:before-gap".to_vec(), bytes("failed-before-gap"))
            .await
            .expect_err("group-commit sync should fail");
        failed_sequences.insert(db.current_sequence());
        assert_simulated_failed_sequence_invariants(
            &db,
            &events,
            &successful_sequences,
            &failed_sequences,
            &mut previous_visible,
            &mut previous_durable,
        )
        .await?;

        let second = events
            .write(b"ok:after-gap".to_vec(), bytes("after-gap"))
            .await?;
        successful_sequences.push(second);
        assert_simulated_failed_sequence_invariants(
            &db,
            &events,
            &successful_sequences,
            &failed_sequences,
            &mut previous_visible,
            &mut previous_durable,
        )
        .await?;

        for step in 0..10_u64 {
            match next_schedule_u64(&mut schedule_state) % 3 {
                0 => {
                    let sync_target =
                        simulated_active_commit_log_segment_path(&context, root).await?;
                    context
                        .file_system()
                        .inject_failure(FileSystemFailure::timeout(
                            FileSystemOperation::Sync,
                            sync_target,
                        ));
                    events
                        .write(
                            format!("failed:pre-restart:{step}").into_bytes(),
                            bytes("failed-pre-restart"),
                        )
                        .await
                        .expect_err("pre-restart sync should fail");
                    failed_sequences.insert(db.current_sequence());
                }
                _ => {
                    let sequence = events
                        .write(
                            format!("ok:pre-restart:{step}").into_bytes(),
                            bytes("ok-pre-restart"),
                        )
                        .await?;
                    successful_sequences.push(sequence);
                }
            }

            assert_simulated_failed_sequence_invariants(
                &db,
                &events,
                &successful_sequences,
                &failed_sequences,
                &mut previous_visible,
                &mut previous_durable,
            )
            .await?;
        }

        let durable_prefix = successful_sequences.last().copied().unwrap_or_default();
        failed_sequences.retain(|sequence| *sequence < durable_prefix);
        let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
        let reopened_events = reopened.table("events");
        assert_eq!(reopened.current_sequence(), durable_prefix);
        assert_eq!(reopened.current_durable_sequence(), durable_prefix);
        previous_visible = durable_prefix;
        previous_durable = durable_prefix;
        assert_simulated_failed_sequence_invariants(
            &reopened,
            &reopened_events,
            &successful_sequences,
            &failed_sequences,
            &mut previous_visible,
            &mut previous_durable,
        )
        .await?;

        for step in 0..10_u64 {
            match next_schedule_u64(&mut schedule_state) % 3 {
                0 => {
                    let sync_target =
                        simulated_active_commit_log_segment_path(&context, root).await?;
                    context
                        .file_system()
                        .inject_failure(FileSystemFailure::timeout(
                            FileSystemOperation::Sync,
                            sync_target,
                        ));
                    reopened_events
                        .write(
                            format!("failed:post-restart:{step}").into_bytes(),
                            bytes("failed-post-restart"),
                        )
                        .await
                        .expect_err("post-restart sync should fail");
                    failed_sequences.insert(reopened.current_sequence());
                }
                _ => {
                    let sequence = reopened_events
                        .write(
                            format!("ok:post-restart:{step}").into_bytes(),
                            bytes("ok-post-restart"),
                        )
                        .await?;
                    successful_sequences.push(sequence);
                }
            }

            assert_simulated_failed_sequence_invariants(
                &reopened,
                &reopened_events,
                &successful_sequences,
                &failed_sequences,
                &mut previous_visible,
                &mut previous_durable,
            )
            .await?;
        }

        Ok(())
    })
}

#[test]
fn db_merge_simulation_replays_same_seed() -> turmoil::Result {
    let config = DbSimulationScenarioConfig {
        root_path: "/terracedb/sim/db-seeded-replay".to_string(),
        tables: vec![SimulationTableSpec::merge_row(
            "events",
            SimulationMergeOperatorId::AppendBytes,
            Some(2),
        )],
        steps: 16,
        key_count: 3,
        max_payload_len: 6,
    };

    let first = SeededSimulationRunner::new(0x4141).run_db_generated(config.clone())?;
    let second = SeededSimulationRunner::new(0x4141).run_db_generated(config)?;

    assert_eq!(first.scenario, second.scenario);
    assert_eq!(first.trace, second.trace);
    assert!(
        first
            .scenario
            .workload
            .iter()
            .any(|operation| matches!(operation, DbWorkloadOperation::Merge { .. })),
        "merge-focused generated scenarios should include merge operations"
    );

    Ok(())
}

#[test]
fn db_merge_simulation_changes_shape_for_different_seeds() -> turmoil::Result {
    let config = DbSimulationScenarioConfig {
        root_path: "/terracedb/sim/db-seeded-variance".to_string(),
        tables: vec![SimulationTableSpec::merge_row(
            "events",
            SimulationMergeOperatorId::AppendBytes,
            Some(2),
        )],
        steps: 16,
        key_count: 3,
        max_payload_len: 6,
    };

    let left = SeededSimulationRunner::new(7).run_db_generated(config.clone())?;
    let right = SeededSimulationRunner::new(8).run_db_generated(config)?;

    assert!(
        left.scenario.workload != right.scenario.workload
            || left.scenario.faults != right.scenario.faults
            || left.trace != right.trace
    );

    Ok(())
}

#[test]
fn db_merge_simulation_seed_campaign_is_reproducible() -> turmoil::Result {
    let config = DbSimulationScenarioConfig {
        root_path: "/terracedb/sim/db-seeded-campaign".to_string(),
        tables: vec![SimulationTableSpec::merge_row(
            "events",
            SimulationMergeOperatorId::AppendBytes,
            Some(2),
        )],
        steps: 20,
        key_count: 4,
        max_payload_len: 8,
    };
    let seeds = [0x5101_u64, 0x5102, 0x5103];

    let first_pass = seeds
        .into_iter()
        .map(|seed| {
            SeededSimulationRunner::new(seed)
                .run_db_generated(config.clone())
                .map(|outcome| (seed, outcome))
        })
        .collect::<turmoil::Result<BTreeMap<_, _>>>()?;
    let second_pass = seeds
        .into_iter()
        .map(|seed| {
            SeededSimulationRunner::new(seed)
                .run_db_generated(config.clone())
                .map(|outcome| (seed, outcome))
        })
        .collect::<turmoil::Result<BTreeMap<_, _>>>()?;

    assert_eq!(first_pass, second_pass);
    assert!(
        first_pass.values().all(|outcome| outcome
            .trace
            .iter()
            .any(|event| matches!(event, TraceEvent::DbStepResult { .. }))),
        "every generated campaign run should record db step results"
    );

    Ok(())
}

#[test]
fn db_merge_simulation_recovers_after_crash_following_read_triggered_collapse() -> turmoil::Result {
    let scenario = DbGeneratedScenario {
        seed: 0xfeed,
        root_path: "/terracedb/sim/db-merge-collapse-crash".to_string(),
        tables: vec![SimulationTableSpec::merge_row(
            "events",
            SimulationMergeOperatorId::AppendBytes,
            Some(2),
        )],
        workload: vec![
            DbWorkloadOperation::Put {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"seed".to_vec(),
            },
            DbWorkloadOperation::Merge {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"A".to_vec(),
            },
            DbWorkloadOperation::Flush,
            DbWorkloadOperation::Merge {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"B".to_vec(),
            },
            DbWorkloadOperation::Merge {
                table: "events".to_string(),
                key: b"doc".to_vec(),
                value: b"C".to_vec(),
            },
            DbWorkloadOperation::ReadLatest {
                table: "events".to_string(),
                key: b"doc".to_vec(),
            },
            DbWorkloadOperation::ReadLatest {
                table: "events".to_string(),
                key: b"doc".to_vec(),
            },
        ],
        faults: vec![ScheduledFault {
            step: 5,
            cut_point: CutPoint::AfterStep,
            kind: ScheduledFaultKind::Crash,
        }],
    };

    let outcome = SeededSimulationRunner::new(scenario.seed).run_db_scenario(scenario.clone())?;

    assert_eq!(outcome.scenario, scenario);
    assert!(
        outcome.trace.iter().any(|event| matches!(
            event,
            TraceEvent::Crash {
                cut_point: CutPoint::AfterStep
            }
        )),
        "scenario should record the crash cut point"
    );
    assert!(
        outcome
            .trace
            .iter()
            .any(|event| matches!(event, TraceEvent::Restart)),
        "scenario should restart after the crash"
    );
    assert!(
        outcome
            .trace
            .iter()
            .any(|event| matches!(event, TraceEvent::DbRecovered { .. })),
        "real-db simulation should record recovery state"
    );

    let read_results = outcome
        .trace
        .iter()
        .filter_map(|event| match event {
            TraceEvent::DbStepResult {
                result: OperationResult::Value(value),
                ..
            } => Some(value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        read_results,
        vec![Some(b"seed|A|B|C".to_vec()), Some(b"seed|A|B|C".to_vec()),]
    );

    Ok(())
}
#[test]
fn ttl_simulation_supports_snapshot_guarded_compaction_across_restart() -> turmoil::Result {
    let expired = ttl_value(5, "apple");
    let banana = ttl_value(5_000, "banana");
    let scenario = DbGeneratedScenario {
        seed: 0x1515,
        root_path: "/terracedb/sim/t15-ttl".to_string(),
        tables: vec![SimulationTableSpec::ttl_row("events")],
        workload: vec![
            DbWorkloadOperation::Put {
                table: "events".to_string(),
                key: b"apple".to_vec(),
                value: match &expired {
                    Value::Bytes(bytes) => bytes.clone(),
                    Value::Record(_) => unreachable!("ttl simulation values are byte payloads"),
                },
            },
            DbWorkloadOperation::Flush,
            DbWorkloadOperation::ReadLatest {
                table: "events".to_string(),
                key: b"apple".to_vec(),
            },
            DbWorkloadOperation::AdvanceClock { millis: 10 },
            DbWorkloadOperation::Put {
                table: "events".to_string(),
                key: b"banana".to_vec(),
                value: match &banana {
                    Value::Bytes(bytes) => bytes.clone(),
                    Value::Record(_) => unreachable!("ttl simulation values are byte payloads"),
                },
            },
            DbWorkloadOperation::Flush,
            DbWorkloadOperation::RunCompaction,
            DbWorkloadOperation::ReadLatest {
                table: "events".to_string(),
                key: b"apple".to_vec(),
            },
            DbWorkloadOperation::ReadLatest {
                table: "events".to_string(),
                key: b"banana".to_vec(),
            },
        ],
        faults: vec![ScheduledFault {
            step: 5,
            cut_point: CutPoint::AfterStep,
            kind: ScheduledFaultKind::Crash,
        }],
    };

    let outcome = SeededSimulationRunner::new(scenario.seed).run_db_scenario(scenario.clone())?;

    assert_eq!(outcome.scenario, scenario);
    assert!(
        outcome.trace.iter().any(|event| matches!(
            event,
            TraceEvent::Crash {
                cut_point: CutPoint::AfterStep
            }
        )),
        "scenario should crash after the second flush"
    );
    assert!(
        outcome
            .trace
            .iter()
            .any(|event| matches!(event, TraceEvent::Restart)),
        "scenario should restart before the TTL compaction step"
    );
    assert!(
        outcome
            .trace
            .iter()
            .any(|event| matches!(event, TraceEvent::DbRecovered { .. })),
        "real-db simulation should record recovery state"
    );

    let read_results = outcome
        .trace
        .iter()
        .filter_map(|event| match event {
            TraceEvent::DbStepResult {
                result: OperationResult::Value(value),
                ..
            } => Some(value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        read_results,
        vec![
            Some(match expired {
                Value::Bytes(bytes) => bytes,
                Value::Record(_) => unreachable!("ttl simulation values are byte payloads"),
            }),
            None,
            Some(match banana {
                Value::Bytes(bytes) => bytes,
                Value::Record(_) => unreachable!("ttl simulation values are byte payloads"),
            }),
        ]
    );

    Ok(())
}

#[test]
fn occ_transaction_simulation_respects_flush_modes_across_restart() -> turmoil::Result {
    SeededSimulationRunner::new(0x2828).run_with(|context| async move {
        let config = DbConfig {
            storage: StorageConfig::Tiered(TieredStorageConfig {
                ssd: SsdConfig {
                    path: "/terracedb/sim/t28-occ-transactions".to_string(),
                },
                s3: S3Location {
                    bucket: "terracedb-sim".to_string(),
                    prefix: "occ-transactions".to_string(),
                },
                max_local_bytes: 1024 * 1024,
                durability: TieredDurabilityMode::Deferred,
            }),
            scheduler: None,
        };

        let db = context.open_db(config.clone()).await?;
        let table = db
            .create_table(SimulationTableSpec::row("events").table_config())
            .await?;

        let mut volatile_tx = Transaction::begin(&db).await;
        volatile_tx.write(&table, b"user:1".to_vec(), Value::bytes("volatile-a"));
        volatile_tx.write(&table, b"user:2".to_vec(), Value::bytes("volatile-b"));
        assert_eq!(
            volatile_tx.read(&table, b"user:1".to_vec()).await?,
            Some(Value::bytes("volatile-a"))
        );
        assert_eq!(volatile_tx.commit_no_flush().await?, SequenceNumber::new(1));
        assert_eq!(db.current_sequence(), SequenceNumber::new(1));
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));

        let reopened = context
            .restart_db(config.clone(), CutPoint::AfterStep)
            .await?;
        let reopened_table = reopened.table("events");
        assert_eq!(reopened.current_sequence(), SequenceNumber::new(0));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(0));
        assert_eq!(reopened_table.read(b"user:1".to_vec()).await?, None);
        assert_eq!(reopened_table.read(b"user:2".to_vec()).await?, None);

        let mut durable_tx = Transaction::begin(&reopened).await;
        durable_tx.write(
            &reopened_table,
            b"user:1".to_vec(),
            Value::bytes("durable-a"),
        );
        durable_tx.write(
            &reopened_table,
            b"user:2".to_vec(),
            Value::bytes("durable-b"),
        );
        assert_eq!(durable_tx.commit().await?, SequenceNumber::new(1));
        assert_eq!(reopened.current_sequence(), SequenceNumber::new(1));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(1));

        let durable_reopened = context.restart_db(config, CutPoint::AfterStep).await?;
        let durable_table = durable_reopened.table("events");
        assert_eq!(durable_reopened.current_sequence(), SequenceNumber::new(1));
        assert_eq!(
            durable_reopened.current_durable_sequence(),
            SequenceNumber::new(1)
        );
        assert_eq!(
            durable_table.read(b"user:1".to_vec()).await?,
            Some(Value::bytes("durable-a"))
        );
        assert_eq!(
            durable_table.read(b"user:2".to_vec()).await?,
            Some(Value::bytes("durable-b"))
        );

        Ok(())
    })
}

#[test]
fn hostile_scheduler_simulation_still_forces_flush_and_l0_progress() -> turmoil::Result {
    SeededSimulationRunner::new(0x1616)
        .with_simulation_duration(Duration::from_secs(5))
        .run_with(|context| async move {
            let db = context
                .open_db(simulation_db_config(
                    "/terracedb/sim/t16-hostile-scheduler",
                    Arc::new(HostileSimulationScheduler),
                    160,
                ))
                .await?;
            let flush_table = db
                .create_table(SimulationTableSpec::row("events").table_config())
                .await?;
            let l0_table = db
                .create_table(SimulationTableSpec::row("events-l0").table_config())
                .await?;

            flush_table
                .write(b"big-1".to_vec(), Value::Bytes(vec![b'x'; 80]))
                .await?;
            flush_table
                .write(b"big-2".to_vec(), Value::Bytes(vec![b'y'; 80]))
                .await?;
            assert!(
                db.table_stats(&flush_table).await.local_bytes > 0,
                "memory guardrail should have forced a flush under the hostile scheduler"
            );

            for index in 0..DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT {
                l0_table
                    .write(format!("l0-{index}").into_bytes(), Value::bytes("v"))
                    .await?;
                db.flush().await?;
            }

            let before = db.table_stats(&l0_table).await;
            assert!(before.l0_sstable_count > 0);
            assert!(before.l0_sstable_count <= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT);

            l0_table
                .write(b"trigger".to_vec(), Value::bytes("v"))
                .await?;

            let after = db.table_stats(&l0_table).await;
            assert!(
                after.l0_sstable_count < DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT,
                "bounded maintenance should keep L0 pressure below the hard ceiling"
            );
            assert_eq!(
                l0_table.read(b"trigger".to_vec()).await?,
                Some(Value::bytes("v"))
            );
            assert_eq!(
                db.current_sequence(),
                SequenceNumber::new(DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT as u64 + 3),
            );

            Ok(())
        })
}

#[test]
fn random_scheduler_simulation_keeps_real_db_progressing() -> turmoil::Result {
    SeededSimulationRunner::new(0x2727)
        .with_simulation_duration(Duration::from_secs(5))
        .run_with(|context| async move {
            let db = context
                .open_db(simulation_db_config(
                    "/terracedb/sim/t16-random-scheduler",
                    Arc::new(RandomSimulationScheduler::seeded(context.seed())),
                    1024 * 1024,
                ))
                .await?;
            let table = db
                .create_table(SimulationTableSpec::row("events").table_config())
                .await?;

            for index in 0..12_u64 {
                table
                    .write(
                        format!("k-{index}").into_bytes(),
                        Value::bytes(format!("v-{index}")),
                    )
                    .await?;
                if index % 2 == 1 {
                    db.flush().await?;
                }
            }

            assert_eq!(db.current_sequence(), SequenceNumber::new(12));
            assert_eq!(db.current_durable_sequence(), SequenceNumber::new(12));
            assert_eq!(
                table.read(b"k-11".to_vec()).await?,
                Some(Value::bytes("v-11"))
            );
            assert!(
                db.table_stats(&table).await.l0_sstable_count
                    <= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT,
                "random scheduling should never outrun the engine guardrails"
            );

            Ok(())
        })
}

#[test]
fn cold_offload_simulation_retries_after_network_fault_and_recovers_remote_state() -> turmoil::Result
{
    SeededSimulationRunner::new(0x2122).run_with(|context| async move {
        let setup_config = DbConfig {
            storage: StorageConfig::Tiered(TieredStorageConfig {
                ssd: SsdConfig {
                    path: "/terracedb/sim/t21-cold-offload".to_string(),
                },
                s3: S3Location {
                    bucket: "terracedb-sim".to_string(),
                    prefix: "cold-offload".to_string(),
                },
                max_local_bytes: 1024 * 1024,
                durability: TieredDurabilityMode::GroupCommit,
            }),
            scheduler: Some(Arc::new(OffloadSimulationScheduler)),
        };
        let config = DbConfig {
            storage: StorageConfig::Tiered(TieredStorageConfig {
                ssd: SsdConfig {
                    path: "/terracedb/sim/t21-cold-offload".to_string(),
                },
                s3: S3Location {
                    bucket: "terracedb-sim".to_string(),
                    prefix: "cold-offload".to_string(),
                },
                max_local_bytes: 1,
                durability: TieredDurabilityMode::GroupCommit,
            }),
            scheduler: Some(Arc::new(OffloadSimulationScheduler)),
        };

        let setup_db = context.open_db(setup_config).await?;
        let table = setup_db
            .create_table(SimulationTableSpec::row("events").table_config())
            .await?;

        let first = table
            .write(b"apple".to_vec(), Value::Bytes(vec![b'a'; 256]))
            .await?;
        setup_db.flush().await?;
        table
            .write(b"banana".to_vec(), Value::Bytes(vec![b'b'; 256]))
            .await?;
        setup_db.flush().await?;

        let db = context
            .restart_db(config.clone(), CutPoint::AfterStep)
            .await?;
        let table = db.table("events");

        let table_id = table.id().expect("simulation table id");
        let cold_prefix = format!("cold-offload/cold/table-{:06}/", table_id.get());
        context
            .object_store()
            .inject_failure(ObjectStoreFaultSpec::Timeout {
                operation: ObjectStoreOperation::Put,
                target_prefix: cold_prefix.clone(),
            })
            .await?;

        let first_error = db
            .run_next_offload()
            .await
            .expect_err("first offload should surface the injected timeout");
        assert_eq!(first_error.kind(), StorageErrorKind::Timeout);
        assert_eq!(
            table.read(b"apple".to_vec()).await?,
            Some(Value::Bytes(vec![b'a'; 256]))
        );
        assert!(
            db.pending_work()
                .await
                .iter()
                .any(|work| work.work_type == PendingWorkType::Offload)
        );

        assert!(db.run_next_offload().await?);
        let offloaded_stats = db.table_stats(&table).await;
        assert_eq!(offloaded_stats.local_bytes, 0);
        assert!(offloaded_stats.s3_bytes > 0);
        assert!(!context.object_store().list(&cold_prefix).await?.is_empty());

        let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
        let reopened_table = reopened.table("events");
        assert_eq!(
            reopened_table.read(b"apple".to_vec()).await?,
            Some(Value::Bytes(vec![b'a'; 256]))
        );
        assert_eq!(
            reopened_table.read_at(b"apple".to_vec(), first).await?,
            Some(Value::Bytes(vec![b'a'; 256]))
        );
        let reopened_stats = reopened.table_stats(&reopened_table).await;
        assert_eq!(reopened_stats.local_bytes, 0);
        assert!(reopened_stats.s3_bytes > 0);

        Ok(())
    })
}

#[test]
fn remote_cache_survives_simulated_restart_and_masks_warmed_network_faults() -> turmoil::Result {
    SeededSimulationRunner::new(0x2020).run_with(|context| async move {
        let key = "backup/sst/table-000001/0000/SST-000001.sst";
        let payload = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let cache_root = "/terracedb/sim/remote-cache";

        context.object_store().put(key, &payload).await?;

        let cache = Arc::new(RemoteCache::open(context.file_system(), cache_root).await?);
        let storage =
            UnifiedStorage::new(context.file_system(), context.object_store(), Some(cache));
        let source = StorageSource::remote_object(key);

        let first_range = storage.read_range(&source, 2..7).await?;
        assert_eq!(first_range, b"cdefg");

        context
            .object_store()
            .inject_failure(ObjectStoreFaultSpec::Timeout {
                operation: ObjectStoreOperation::GetRange,
                target_prefix: key.to_string(),
            })
            .await?;
        let cached_range = storage.read_range(&source, 2..7).await?;
        assert_eq!(cached_range, b"cdefg");

        let full = storage.read_all(&source).await?;
        assert_eq!(full, payload);

        context.crash_filesystem(CutPoint::AfterStep);
        let reopened_cache = Arc::new(RemoteCache::open(context.file_system(), cache_root).await?);
        let reopened_storage = UnifiedStorage::new(
            context.file_system(),
            context.object_store(),
            Some(reopened_cache),
        );

        context
            .object_store()
            .inject_failure(ObjectStoreFaultSpec::Timeout {
                operation: ObjectStoreOperation::Get,
                target_prefix: key.to_string(),
            })
            .await?;
        let reopened_full = reopened_storage.read_all(&source).await?;
        assert_eq!(reopened_full, payload);

        Ok(())
    })
}

#[test]
fn remote_list_failures_are_structured_in_simulation() -> turmoil::Result {
    SeededSimulationRunner::new(0x2121).run_with(|context| async move {
        let manifest_prefix = "backup/manifest/";
        let storage = UnifiedStorage::new(context.file_system(), context.object_store(), None);

        context
            .object_store()
            .put("backup/manifest/MANIFEST-000001", b"manifest-1")
            .await?;
        context
            .object_store()
            .inject_failure(ObjectStoreFaultSpec::StaleList {
                prefix: manifest_prefix.to_string(),
            })
            .await?;

        let error = storage
            .list_objects(manifest_prefix)
            .await
            .expect_err("stale list should surface a structured remote error");

        assert_eq!(error.kind(), StorageErrorKind::DurabilityBoundary);
        assert_eq!(error.recovery_hint(), RemoteRecoveryHint::RefreshListing);

        let listed = storage.list_objects(manifest_prefix).await?;
        assert_eq!(listed, vec!["backup/manifest/MANIFEST-000001".to_string()]);

        Ok(())
    })
}

#[test]
fn tiered_backup_recovery_restores_remote_state_after_simulated_disk_loss() -> turmoil::Result {
    SeededSimulationRunner::new(0x2222).run_with(|context| async move {
        let root = "/terracedb/sim/backup-dr";
        let config = simulation_tiered_config(root, TieredDurabilityMode::GroupCommit);
        let db = context.open_db(config.clone()).await?;
        let events = db
            .create_table(SimulationTableSpec::row("events").table_config())
            .await?;
        let audit = db
            .create_table(SimulationTableSpec::row("audit").table_config())
            .await?;

        events.write(b"apple".to_vec(), bytes("v1")).await?;
        db.flush().await?;
        events.write(b"banana".to_vec(), bytes("tail")).await?;
        audit.write(b"audit:1".to_vec(), bytes("entry")).await?;

        let existing = context.file_system().list(root).await?;
        for path in existing {
            context.file_system().delete(&path).await?;
        }

        let restored = context.open_db(config).await?;
        let restored_events = restored.table("events");
        let restored_audit = restored.table("audit");
        assert_eq!(
            restored_events.read(b"apple".to_vec()).await?,
            Some(bytes("v1"))
        );
        assert_eq!(
            restored_events.read(b"banana".to_vec()).await?,
            Some(bytes("tail"))
        );
        assert_eq!(
            restored_audit.read(b"audit:1".to_vec()).await?,
            Some(bytes("entry"))
        );

        Ok(())
    })
}

#[test]
fn tiered_backup_retries_after_interrupted_restore_in_simulation() -> turmoil::Result {
    SeededSimulationRunner::new(0x2323).run_with(|context| async move {
        let root = "/terracedb/sim/backup-retry";
        let config = simulation_tiered_config(root, TieredDurabilityMode::GroupCommit);
        let db = context.open_db(config.clone()).await?;
        let events = db
            .create_table(SimulationTableSpec::row("events").table_config())
            .await?;

        events.write(b"apple".to_vec(), bytes("v1")).await?;
        db.flush().await?;
        events.write(b"banana".to_vec(), bytes("tail")).await?;

        let existing = context.file_system().list(root).await?;
        for path in existing {
            context.file_system().delete(&path).await?;
        }

        context
            .file_system()
            .inject_failure(FileSystemFailure::timeout(
                FileSystemOperation::Rename,
                format!("{root}/manifest/MANIFEST-000001.tmp"),
            ));
        let first = context
            .open_db(config.clone())
            .await
            .expect_err("first restore should fail partway through");
        assert!(
            matches!(first, OpenError::Storage(_)),
            "interrupted restore should surface a storage error"
        );

        let restored = context.open_db(config).await?;
        let restored_events = restored.table("events");
        assert_eq!(
            restored_events.read(b"apple".to_vec()).await?,
            Some(bytes("v1"))
        );
        assert_eq!(
            restored_events.read(b"banana".to_vec()).await?,
            Some(bytes("tail"))
        );

        Ok(())
    })
}

#[test]
fn visible_subscription_simulation_catches_up_after_coalesced_wakes() -> turmoil::Result {
    SeededSimulationRunner::new(0x1818).run_with(|context| async move {
        let db = context
            .open_db(simulation_db_config(
                "/terracedb/sim/t18-visible-subscription",
                Arc::new(RandomSimulationScheduler::seeded(context.seed())),
                1024 * 1024,
            ))
            .await?;
        let table = db
            .create_table(SimulationTableSpec::row("events").table_config())
            .await?;

        let mut receiver = db.subscribe(&table);
        let first = table.write(b"k-1".to_vec(), Value::bytes("v-1")).await?;
        let second = table.write(b"k-2".to_vec(), Value::bytes("v-2")).await?;

        let mut processed = receiver.current();
        assert_eq!(processed, second);
        assert!(processed >= first);

        let writer_table = table.clone();
        let clock = context.clock();
        let writer = tokio::spawn(async move {
            clock.sleep(Duration::from_millis(1)).await;
            let third = writer_table
                .write(b"k-3".to_vec(), Value::bytes("v-3"))
                .await
                .expect("write third row");
            clock.sleep(Duration::from_millis(1)).await;
            let fourth = writer_table
                .write(b"k-4".to_vec(), Value::bytes("v-4"))
                .await
                .expect("write fourth row");
            (third, fourth)
        });

        processed = processed.max(receiver.changed().await.expect("subscription wake"));
        let (third, fourth) = writer.await.expect("join writer");
        assert!(fourth >= third);
        while processed < fourth {
            processed = processed.max(receiver.changed().await.expect("catch-up wake"));
        }

        assert_eq!(processed, fourth);
        assert_eq!(receiver.current(), fourth);

        Ok(())
    })
}
