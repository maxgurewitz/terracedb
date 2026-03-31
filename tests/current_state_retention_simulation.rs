use terracedb::{
    CurrentStateCompactionRowRemovalMode, CurrentStateDerivedOnlyReason, CurrentStateEffectiveMode,
    CurrentStateMissingValuePolicy, CurrentStateOracleRow, CurrentStateOrderingContract,
    CurrentStatePhysicalReclaimSemantics, CurrentStatePhysicalRetentionMode,
    CurrentStatePhysicalRetentionSeam, CurrentStatePlanner, CurrentStateProjectionOwnedRange,
    CurrentStateRankedMaterializationSeam, CurrentStateRebuildMode, CurrentStateRebuildSeam,
    CurrentStateRetentionContract, CurrentStateRetentionCoordinationContext,
    CurrentStateRetentionPlanPhase, CurrentStateRetentionReason, CurrentStateRetentionSemantics,
    CurrentStateRetentionTarget, CurrentStateSortDirection, CurrentStateThresholdCutoff,
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

fn row_without_primary(name: &str, tie_break: u64, bytes: u64) -> CurrentStateOracleRow {
    CurrentStateOracleRow::new(
        name.as_bytes().to_vec(),
        None,
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

fn rewrite_coordination() -> CurrentStateRetentionCoordinationContext {
    CurrentStateRetentionCoordinationContext {
        semantics: CurrentStateRetentionSemantics::new(
            CurrentStateRetentionTarget::RowTable,
            CurrentStatePhysicalReclaimSemantics::RewriteCompaction,
        ),
        cpu_cost_soft_limit: 8,
        rank_churn_soft_limit: 2,
        max_write_bytes_per_second: Some(32),
        ..Default::default()
    }
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
        coordination: CurrentStateRetentionCoordinationContext::default(),
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

fn run_threshold_missing_key_simulation(
    seed: u64,
) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let scenario = CurrentStateSimulationScenario {
        initial_contract: threshold_contract(1, 50),
        coordination: CurrentStateRetentionCoordinationContext::default(),
        operations: vec![
            CurrentStateSimulationOperation::Upsert(row_without_primary("alpha", 1, 12)),
            CurrentStateSimulationOperation::Upsert(row("bravo", 40, 2, 14)),
            CurrentStateSimulationOperation::Restart,
            CurrentStateSimulationOperation::Upsert(row("bravo", 90, 2, 16)),
            CurrentStateSimulationOperation::ReviseContract {
                contract: threshold_contract(2, 95),
            },
            CurrentStateSimulationOperation::Upsert(row("charlie", 100, 3, 18)),
        ],
    };

    SeededSimulationRunner::new(seed).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario)
            .expect("threshold missing-key simulation should succeed"))
    })
}

fn run_rank_simulation(seed: u64) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let scenario = CurrentStateSimulationScenario {
        initial_contract: rank_contract(1, 2),
        coordination: CurrentStateRetentionCoordinationContext::default(),
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

fn run_rank_publication_simulation(seed: u64) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let scenario = CurrentStateSimulationScenario {
        initial_contract: rank_contract(7, 2),
        coordination: CurrentStateRetentionCoordinationContext::default(),
        operations: vec![
            CurrentStateSimulationOperation::Upsert(row("alpha", 90, 1, 10)),
            CurrentStateSimulationOperation::Upsert(row("bravo", 80, 1, 10)),
            CurrentStateSimulationOperation::Upsert(row("charlie", 70, 1, 10)),
            CurrentStateSimulationOperation::PublishRetainedSet,
            CurrentStateSimulationOperation::Upsert(row("charlie", 95, 1, 10)),
            CurrentStateSimulationOperation::Upsert(row("bravo", 97, 1, 10)),
            CurrentStateSimulationOperation::Restart,
            CurrentStateSimulationOperation::PublishRetainedSet,
        ],
    };

    SeededSimulationRunner::new(seed).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario)
            .expect("rank publication simulation should succeed"))
    })
}

fn run_restart_cleanup_simulation(seed: u64) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let scenario = CurrentStateSimulationScenario {
        initial_contract: threshold_contract(1, 50),
        coordination: rewrite_coordination(),
        operations: vec![
            CurrentStateSimulationOperation::Upsert(row("alpha", 10, 1, 12)),
            CurrentStateSimulationOperation::Upsert(row("bravo", 60, 2, 14)),
            CurrentStateSimulationOperation::Upsert(row("charlie", 80, 3, 16)),
            CurrentStateSimulationOperation::PublishManifest,
            CurrentStateSimulationOperation::Restart,
            CurrentStateSimulationOperation::PublishManifest,
            CurrentStateSimulationOperation::CompleteLocalCleanup,
        ],
    };

    SeededSimulationRunner::new(seed).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario)
            .expect("restart cleanup simulation should succeed"))
    })
}

fn run_revision_cleanup_simulation(seed: u64) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let scenario = CurrentStateSimulationScenario {
        initial_contract: threshold_contract(1, 50),
        coordination: rewrite_coordination(),
        operations: vec![
            CurrentStateSimulationOperation::Upsert(row("alpha", 10, 1, 12)),
            CurrentStateSimulationOperation::Upsert(row("bravo", 60, 2, 14)),
            CurrentStateSimulationOperation::Upsert(row("charlie", 80, 3, 16)),
            CurrentStateSimulationOperation::PublishManifest,
            CurrentStateSimulationOperation::ReviseContract {
                contract: threshold_contract(2, 70),
            },
            CurrentStateSimulationOperation::PublishManifest,
            CurrentStateSimulationOperation::CompleteLocalCleanup,
        ],
    };

    SeededSimulationRunner::new(seed).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario)
            .expect("revision cleanup simulation should succeed"))
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

#[test]
fn threshold_retention_simulation_keeps_missing_keys_out_of_reclaim() -> turmoil::Result {
    let first = run_threshold_missing_key_simulation(0x5961)?;
    let second = run_threshold_missing_key_simulation(0x5961)?;
    assert_eq!(first, second);

    let final_step = first
        .steps
        .last()
        .expect("threshold missing-key simulation should have steps");
    assert_eq!(
        decode_keys(&final_step.evaluation.retained_row_keys),
        vec!["alpha".to_string(), "charlie".to_string()]
    );
    assert_eq!(
        decode_keys(&final_step.evaluation.non_retained_row_keys),
        vec!["bravo".to_string()]
    );
    assert_eq!(
        decode_keys(&final_step.evaluation.reclaimable_row_keys),
        vec!["bravo".to_string()]
    );
    assert!(final_step.evaluation.deferred_row_keys.is_empty());
    assert_eq!(final_step.evaluation.stats.policy_revision, 2);

    Ok(())
}

#[test]
fn rank_retention_simulation_tracks_publication_boundary_churn_and_restart() -> turmoil::Result {
    let first = run_rank_publication_simulation(0x5961)?;
    let second = run_rank_publication_simulation(0x5961)?;
    assert_eq!(first, second);

    let pre_restart = &first.steps[5].evaluation;
    assert_eq!(
        decode_keys(&pre_restart.retained_row_keys),
        vec!["bravo".to_string(), "charlie".to_string()]
    );
    assert_eq!(
        decode_keys(&pre_restart.entered_retained_row_keys),
        vec!["charlie".to_string()]
    );
    assert_eq!(
        decode_keys(&pre_restart.exited_retained_row_keys),
        vec!["alpha".to_string()]
    );
    assert_eq!(pre_restart.stats.membership_changes.entered_rows, 1);
    assert_eq!(pre_restart.stats.membership_changes.exited_rows, 1);
    assert_eq!(pre_restart.stats.evaluation_cost.rows_scanned, 3);
    assert_eq!(pre_restart.stats.evaluation_cost.rows_ranked, 3);
    assert_eq!(pre_restart.stats.evaluation_cost.rows_materialized, 2);

    let after_restart = &first.steps[6].evaluation;
    assert_eq!(
        pre_restart.retained_row_keys,
        after_restart.retained_row_keys
    );
    assert_eq!(
        pre_restart.entered_retained_row_keys,
        after_restart.entered_retained_row_keys
    );
    assert_eq!(
        pre_restart.exited_retained_row_keys,
        after_restart.exited_retained_row_keys
    );
    assert_eq!(
        pre_restart.stats.membership_changes,
        after_restart.stats.membership_changes
    );
    assert_eq!(
        pre_restart.stats.evaluation_cost,
        after_restart.stats.evaluation_cost
    );
    assert_eq!(
        after_restart
            .stats
            .coordination
            .retained_membership_change_count,
        0
    );

    let after_publish = &first.steps[7].evaluation;
    assert!(after_publish.entered_retained_row_keys.is_empty());
    assert!(after_publish.exited_retained_row_keys.is_empty());
    assert_eq!(after_publish.stats.membership_changes.entered_rows, 0);
    assert_eq!(after_publish.stats.membership_changes.exited_rows, 0);

    Ok(())
}

#[test]
fn retention_coordination_rebuilds_after_restart_and_finishes_cleanup_once() -> turmoil::Result {
    let first = run_restart_cleanup_simulation(0x5962)?;
    let second = run_restart_cleanup_simulation(0x5962)?;
    assert_eq!(first, second);

    let restart_step = &first.steps[4];
    let restart_plan = restart_step
        .evaluation
        .stats
        .coordination
        .active_plan
        .as_ref()
        .expect("restart should recompute a coordination plan");
    assert_eq!(restart_plan.phase, CurrentStateRetentionPlanPhase::Planned);
    assert!(
        restart_step
            .evaluation
            .stats
            .coordination
            .rebuild_fallback_pending
    );

    let final_step = first
        .steps
        .last()
        .expect("restart cleanup simulation should have steps");
    assert_eq!(
        decode_keys(&final_step.evaluation.retained_row_keys),
        vec!["bravo".to_string(), "charlie".to_string()]
    );
    assert!(final_step.evaluation.non_retained_row_keys.is_empty());
    assert!(
        final_step
            .evaluation
            .stats
            .coordination
            .active_plan
            .is_none()
    );
    assert_eq!(final_step.evaluation.stats.reclaimed_rows, 0);
    assert_eq!(final_step.evaluation.stats.deferred_rows, 0);

    Ok(())
}

#[test]
fn retention_coordination_revises_plan_without_duplicate_reclaim() -> turmoil::Result {
    let first = run_revision_cleanup_simulation(0x5963)?;
    let second = run_revision_cleanup_simulation(0x5963)?;
    assert_eq!(first, second);

    let revision_step = &first.steps[4];
    let revision_plan = revision_step
        .evaluation
        .stats
        .coordination
        .active_plan
        .as_ref()
        .expect("revision should install a replacement plan");
    assert_eq!(revision_plan.policy_revision, 2);
    assert_eq!(
        decode_keys(&revision_plan.logical_row_keys),
        vec!["alpha".to_string(), "bravo".to_string()]
    );
    assert_eq!(revision_plan.phase, CurrentStateRetentionPlanPhase::Planned);

    let final_step = first
        .steps
        .last()
        .expect("revision cleanup simulation should have steps");
    assert_eq!(
        decode_keys(&final_step.evaluation.retained_row_keys),
        vec!["charlie".to_string()]
    );
    assert!(final_step.evaluation.non_retained_row_keys.is_empty());
    assert!(
        final_step
            .evaluation
            .stats
            .coordination
            .active_plan
            .is_none()
    );

    Ok(())
}

#[test]
fn retention_coordination_fails_closed_for_logical_only_targets() -> turmoil::Result {
    let scenario = CurrentStateSimulationScenario {
        initial_contract: threshold_contract(1, 50),
        coordination: CurrentStateRetentionCoordinationContext {
            semantics: CurrentStateRetentionSemantics::new(
                CurrentStateRetentionTarget::ColumnarTable,
                CurrentStatePhysicalReclaimSemantics::LogicalOnly,
            ),
            ..Default::default()
        },
        operations: vec![
            CurrentStateSimulationOperation::Upsert(row("alpha", 10, 1, 12)),
            CurrentStateSimulationOperation::Upsert(row("bravo", 60, 2, 14)),
        ],
    };

    let outcome = SeededSimulationRunner::new(0x5964).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario)
            .expect("logical-only fail-closed simulation should succeed"))
    })?;
    let final_step = outcome
        .steps
        .last()
        .expect("logical-only simulation should have steps");
    assert_eq!(
        final_step.evaluation.stats.status.effective_mode,
        CurrentStateEffectiveMode::Skipped
    );
    assert_eq!(
        final_step.evaluation.stats.status.reasons,
        vec![CurrentStateRetentionReason::Skipped {
            reason: terracedb::CurrentStateRetentionSkipReason::UnsupportedPhysicalLayout,
        }]
    );
    assert!(final_step.evaluation.reclaimable_row_keys.is_empty());
    assert!(final_step.evaluation.deferred_row_keys.is_empty());

    Ok(())
}
