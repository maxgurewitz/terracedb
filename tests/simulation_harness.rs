use terracedb::{
    CutPoint, PointMutation, SeededSimulationRunner, ShadowOracle, SimulationScenarioConfig,
    StubDbProcess, TraceEvent,
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
