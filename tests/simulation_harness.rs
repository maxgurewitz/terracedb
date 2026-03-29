use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use futures::StreamExt;
use terracedb::{
    Clock, CommitOptions, CompactionStrategy, CutPoint, DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT,
    DbConfig, DbGeneratedScenario, DbMutation, DbOracleChange, DbShadowOracle,
    DbSimulationScenarioConfig, DbWorkloadOperation, FieldDefinition, FieldId, FieldType,
    FieldValue, LogCursor, ObjectStore, ObjectStoreFaultSpec, ObjectStoreOperation,
    OperationResult, PendingWork, PointMutation, RemoteCache, RemoteRecoveryHint, S3Location,
    ScanOptions, ScheduleAction, ScheduleDecision, ScheduledFault, ScheduledFaultKind, Scheduler,
    SchemaDefinition, SeededSimulationRunner, SequenceNumber, ShadowOracle,
    SimulationMergeOperatorId, SimulationScenarioConfig, SimulationTableSpec, SsdConfig,
    StorageConfig, StorageErrorKind, StorageSource, StubDbProcess, TableConfig, TableFormat,
    TableStats, ThrottleDecision, TieredDurabilityMode,
    TieredStorageConfig, TraceEvent, Transaction, UnifiedStorage, Value,
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

async fn collect_change_feed(stream: terracedb::ChangeStream) -> Vec<DbOracleChange> {
    stream
        .map(|entry| DbOracleChange {
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
        .collect::<Vec<_>>()
        .await
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
    let banana = ttl_value(50, "banana");
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
    SeededSimulationRunner::new(0x1616).run_with(|context| async move {
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
        assert_eq!(
            before.l0_sstable_count,
            DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT
        );

        l0_table
            .write(b"trigger".to_vec(), Value::bytes("v"))
            .await?;

        let after = db.table_stats(&l0_table).await;
        assert!(
            after.l0_sstable_count < DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT,
            "forced L0 compaction should run even when the scheduler refuses all work"
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
    SeededSimulationRunner::new(0x2727).run_with(|context| async move {
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
            db.table_stats(&table).await.l0_sstable_count <= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT,
            "random scheduling should never outrun the engine guardrails"
        );

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
