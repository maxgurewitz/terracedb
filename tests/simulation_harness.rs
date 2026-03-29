use terracedb::{
    CutPoint, DbGeneratedScenario, DbMutation, DbShadowOracle, DbSimulationScenarioConfig,
    DbWorkloadOperation, OperationResult, PointMutation, ScheduledFault, ScheduledFaultKind,
    SeededSimulationRunner, SequenceNumber, ShadowOracle, SimulationMergeOperatorId,
    SimulationScenarioConfig, SimulationTableSpec, StubDbProcess, TraceEvent,
};

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
