use terracedb::{
    CurrentStateCompactionRowRemovalMode, CurrentStateDerivedOnlyReason, CurrentStateEffectiveMode,
    CurrentStateMissingValuePolicy, CurrentStateOracleRow, CurrentStateOrderingContract,
    CurrentStatePhysicalRetentionMode, CurrentStatePhysicalRetentionSeam, CurrentStatePlanner,
    CurrentStateProjectionOwnedRange, CurrentStateRankedMaterializationSeam,
    CurrentStateRebuildMode, CurrentStateRebuildSeam, CurrentStateRetentionContract,
    CurrentStateRetentionReason, CurrentStateSortDirection, CurrentStateThresholdCutoff,
};
use terracedb_simulation::{
    CurrentStateSimulationOperation, CurrentStateSimulationOutcome, CurrentStateSimulationScenario,
    SeededSimulationRunner, run_current_state_simulation,
};

fn row(name: &str, primary: u64, tie_break: u64, bytes: u64) -> CurrentStateOracleRow {
    CurrentStateOracleRow::new(
        name.as_bytes().to_vec(),
        Some(format!("{primary:03}").into_bytes()),
        Some(format!("{tie_break:03}").into_bytes()),
        bytes,
    )
}

fn decode_keys(keys: &[Vec<u8>]) -> Vec<String> {
    keys.iter()
        .map(|key| String::from_utf8(key.clone()).expect("simulation keys should be utf-8"))
        .collect()
}

fn threshold_contract(revision: u64, cutoff: u64) -> CurrentStateRetentionContract {
    CurrentStateRetentionContract::threshold(
        revision,
        CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending)
            .with_missing_values(CurrentStateMissingValuePolicy::ExcludeRow),
        CurrentStateThresholdCutoff::explicit(format!("{cutoff:03}").into_bytes()),
    )
    .with_planner(CurrentStatePlanner {
        compaction_row_removal: CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction,
        physical_retention: CurrentStatePhysicalRetentionSeam {
            mode: CurrentStatePhysicalRetentionMode::Delete,
            ..Default::default()
        },
        rebuild: CurrentStateRebuildSeam {
            on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
            on_revision: CurrentStateRebuildMode::RecomputeFromCurrentState,
        },
        ..Default::default()
    })
}

fn rank_contract(revision: u64, limit: usize) -> CurrentStateRetentionContract {
    CurrentStateRetentionContract::global_rank(
        revision,
        CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending)
            .with_missing_values(CurrentStateMissingValuePolicy::TreatAsLowest),
        limit,
    )
    .with_planner(CurrentStatePlanner {
        ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
            CurrentStateProjectionOwnedRange {
                output_table: "ranked_output".to_string(),
                range_start: b"recent:00".to_vec(),
                range_end: b"recent:ff".to_vec(),
            },
        ),
        rebuild: CurrentStateRebuildSeam {
            on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
            on_revision: CurrentStateRebuildMode::RecomputeDerivedState,
        },
        ..Default::default()
    })
}

fn run_threshold_simulation(seed: u64) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let scenario = CurrentStateSimulationScenario {
        initial_contract: threshold_contract(1, 50),
        operations: vec![
            CurrentStateSimulationOperation::Upsert(row("alpha", 10, 1, 12)),
            CurrentStateSimulationOperation::Upsert(row("bravo", 60, 2, 14)),
            CurrentStateSimulationOperation::Upsert(row("charlie", 80, 3, 16)),
            CurrentStateSimulationOperation::PinSnapshotRows {
                row_keys: vec![b"alpha".to_vec()],
            },
            CurrentStateSimulationOperation::Restart,
            CurrentStateSimulationOperation::Upsert(row("alpha", 90, 1, 18)),
            CurrentStateSimulationOperation::Delete {
                row_key: b"bravo".to_vec(),
            },
            CurrentStateSimulationOperation::ReviseContract {
                contract: threshold_contract(2, 70),
            },
        ],
    };

    SeededSimulationRunner::new(seed).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario).expect("threshold simulation should succeed"))
    })
}

fn run_rank_simulation(seed: u64) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let scenario = CurrentStateSimulationScenario {
        initial_contract: rank_contract(1, 2),
        operations: vec![
            CurrentStateSimulationOperation::Upsert(row("alpha", 90, 1, 10)),
            CurrentStateSimulationOperation::Upsert(row("bravo", 90, 1, 10)),
            CurrentStateSimulationOperation::Upsert(row("charlie", 85, 1, 10)),
            CurrentStateSimulationOperation::Upsert(row("delta", 80, 1, 10)),
            CurrentStateSimulationOperation::ReviseContract {
                contract: rank_contract(2, 3),
            },
            CurrentStateSimulationOperation::Restart,
            CurrentStateSimulationOperation::Delete {
                row_key: b"alpha".to_vec(),
            },
            CurrentStateSimulationOperation::Upsert(row("echo", 95, 0, 10)),
        ],
    };

    SeededSimulationRunner::new(seed).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario).expect("rank simulation should succeed"))
    })
}

#[test]
fn threshold_retention_simulation_is_deterministic_through_revision_and_restart() -> turmoil::Result
{
    let first = run_threshold_simulation(0x5959)?;
    let second = run_threshold_simulation(0x5959)?;
    assert_eq!(first, second);

    let final_step = first
        .steps
        .last()
        .expect("threshold simulation should have steps");
    assert_eq!(
        decode_keys(&final_step.evaluation.retained_row_keys),
        vec!["alpha".to_string(), "charlie".to_string()]
    );
    assert_eq!(final_step.evaluation.stats.policy_revision, 2);
    assert_eq!(final_step.evaluation.stats.reclaimed_rows, 0);
    assert_eq!(final_step.evaluation.stats.deferred_rows, 0);

    Ok(())
}

#[test]
fn rank_retention_simulation_is_deterministic_through_tie_storms_and_restart() -> turmoil::Result {
    let first = run_rank_simulation(0x5960)?;
    let second = run_rank_simulation(0x5960)?;
    assert_eq!(first, second);

    let final_step = first
        .steps
        .last()
        .expect("rank simulation should have steps");
    assert_eq!(
        decode_keys(&final_step.evaluation.retained_row_keys),
        vec![
            "echo".to_string(),
            "bravo".to_string(),
            "charlie".to_string()
        ]
    );
    assert_eq!(
        final_step.evaluation.stats.status.effective_mode,
        CurrentStateEffectiveMode::DerivedOnly
    );
    assert_eq!(
        final_step.evaluation.stats.status.reasons,
        vec![CurrentStateRetentionReason::DegradedToDerivedOnly {
            reason: CurrentStateDerivedOnlyReason::ProjectionOwnedWithoutPhysicalReclaim,
        }]
    );

    Ok(())
}
