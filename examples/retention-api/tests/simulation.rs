use terracedb::{
    CurrentStateEffectiveMode, CurrentStateRetentionBackpressureSignal,
    CurrentStateRetentionPlanPhase, CurrentStateRetentionReason,
};
use terracedb_example_retention_api::{
    LeaderboardEntry, LeaderboardPolicyMode, LeaderboardPolicyRequest, LeaderboardTieBreak,
    SessionPolicyRequest, SessionRecord, ThresholdRetentionLayout, leaderboard_configuration,
    leaderboard_row, session_configuration, session_row,
};
use terracedb_simulation::{
    CurrentStateSimulationOperation, CurrentStateSimulationOutcome, CurrentStateSimulationScenario,
    SeededSimulationRunner, run_current_state_simulation,
};

fn decode_keys(keys: &[Vec<u8>]) -> Vec<String> {
    keys.iter()
        .map(|key| String::from_utf8(key.clone()).expect("keys should be utf-8"))
        .collect()
}

fn session_record(
    id: &str,
    user: &str,
    last_seen_ms: u64,
    estimated_row_bytes: u64,
) -> SessionRecord {
    SessionRecord {
        session_id: id.to_string(),
        user_id: user.to_string(),
        last_seen_ms,
        estimated_row_bytes,
    }
}

fn leaderboard_entry(
    id: &str,
    points: u64,
    created_at_ms: u64,
    estimated_row_bytes: u64,
) -> LeaderboardEntry {
    LeaderboardEntry {
        player_id: id.to_string(),
        points,
        created_at_ms,
        estimated_row_bytes,
    }
}

fn run_sessions_simulation(seed: u64) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let initial_policy = SessionPolicyRequest {
        revision: 1,
        minimum_last_seen_ms: 100,
        layout: ThresholdRetentionLayout::RewriteCompactionDelete,
    };
    let revised_policy = SessionPolicyRequest {
        revision: 2,
        minimum_last_seen_ms: 120,
        layout: ThresholdRetentionLayout::RewriteCompactionDelete,
    };
    let initial_configuration = session_configuration(&initial_policy);
    let revised_configuration = session_configuration(&revised_policy);
    let scenario = CurrentStateSimulationScenario {
        initial_contract: initial_configuration.contract.clone(),
        coordination: initial_configuration.context.clone(),
        operations: vec![
            CurrentStateSimulationOperation::Upsert(session_row(&session_record(
                "alpha", "user-a", 90, 12,
            ))),
            CurrentStateSimulationOperation::Upsert(session_row(&session_record(
                "bravo", "user-b", 110, 14,
            ))),
            CurrentStateSimulationOperation::Upsert(session_row(&session_record(
                "charlie", "user-c", 150, 16,
            ))),
            CurrentStateSimulationOperation::PinSnapshotRows {
                row_keys: vec![b"alpha".to_vec()],
            },
            CurrentStateSimulationOperation::ReviseContract {
                contract: revised_configuration.contract.clone(),
            },
            CurrentStateSimulationOperation::Restart,
            CurrentStateSimulationOperation::PublishManifest,
            CurrentStateSimulationOperation::CompleteLocalCleanup,
        ],
    };

    SeededSimulationRunner::new(seed).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario).expect("session simulation should succeed"))
    })
}

fn run_leaderboard_simulation(seed: u64) -> turmoil::Result<CurrentStateSimulationOutcome> {
    let initial_policy = LeaderboardPolicyRequest {
        revision: 1,
        limit: 2,
        mode: LeaderboardPolicyMode::DerivedOnly,
        tie_break: LeaderboardTieBreak::CreatedAtThenStableId,
    };
    let revised_policy = LeaderboardPolicyRequest {
        revision: 2,
        limit: 3,
        mode: LeaderboardPolicyMode::DerivedOnly,
        tie_break: LeaderboardTieBreak::CreatedAtThenStableId,
    };
    let initial_configuration = leaderboard_configuration(&initial_policy);
    let revised_configuration = leaderboard_configuration(&revised_policy);
    let scenario = CurrentStateSimulationScenario {
        initial_contract: initial_configuration.contract.clone(),
        coordination: initial_configuration.context.clone(),
        operations: vec![
            CurrentStateSimulationOperation::Upsert(leaderboard_row(
                &leaderboard_entry("alpha", 10, 1, 10),
                initial_policy.tie_break,
            )),
            CurrentStateSimulationOperation::Upsert(leaderboard_row(
                &leaderboard_entry("bravo", 10, 2, 10),
                initial_policy.tie_break,
            )),
            CurrentStateSimulationOperation::Upsert(leaderboard_row(
                &leaderboard_entry("charlie", 9, 3, 10),
                initial_policy.tie_break,
            )),
            CurrentStateSimulationOperation::PublishRetainedSet,
            CurrentStateSimulationOperation::Upsert(leaderboard_row(
                &leaderboard_entry("charlie", 10, 4, 10),
                initial_policy.tie_break,
            )),
            CurrentStateSimulationOperation::Upsert(leaderboard_row(
                &leaderboard_entry("delta", 11, 5, 10),
                initial_policy.tie_break,
            )),
            CurrentStateSimulationOperation::ReviseContract {
                contract: revised_configuration.contract.clone(),
            },
            CurrentStateSimulationOperation::Restart,
        ],
    };

    SeededSimulationRunner::new(seed).run_with(move |_context| async move {
        Ok(run_current_state_simulation(&scenario).expect("leaderboard simulation should succeed"))
    })
}

#[test]
fn session_example_simulation_is_seed_stable_through_snapshot_pins_and_cleanup() -> turmoil::Result
{
    let first = run_sessions_simulation(0x62a1)?;
    let second = run_sessions_simulation(0x62a1)?;
    assert_eq!(first, second);

    let final_step = first
        .steps
        .last()
        .expect("session simulation should have steps");
    assert_eq!(
        decode_keys(&final_step.evaluation.retained_row_keys),
        vec!["charlie".to_string()]
    );
    assert_eq!(
        decode_keys(&final_step.evaluation.non_retained_row_keys),
        vec!["alpha".to_string()]
    );
    assert_eq!(
        decode_keys(&final_step.evaluation.deferred_row_keys),
        vec!["alpha".to_string()]
    );
    assert_eq!(
        final_step
            .evaluation
            .stats
            .coordination
            .active_plan
            .as_ref()
            .map(|plan| plan.phase),
        Some(CurrentStateRetentionPlanPhase::Planned)
    );
    assert!(
        final_step
            .evaluation
            .stats
            .status
            .reasons
            .contains(&CurrentStateRetentionReason::BlockedBySnapshots)
    );
    assert!(
        final_step
            .evaluation
            .stats
            .coordination
            .backpressure
            .signals
            .contains(&CurrentStateRetentionBackpressureSignal::SnapshotPinned)
    );
    assert!(
        final_step
            .evaluation
            .stats
            .coordination
            .backpressure
            .signals
            .contains(&CurrentStateRetentionBackpressureSignal::RewriteCompactionBlocked)
    );

    Ok(())
}

#[test]
fn leaderboard_example_simulation_is_seed_stable_through_rank_churn_and_restart() -> turmoil::Result
{
    let first = run_leaderboard_simulation(0x62a2)?;
    let second = run_leaderboard_simulation(0x62a2)?;
    assert_eq!(first, second);

    let churn_step = &first.steps[5].evaluation;
    assert_eq!(
        decode_keys(&churn_step.retained_row_keys),
        vec!["delta".to_string(), "charlie".to_string()]
    );
    assert_eq!(
        decode_keys(&churn_step.entered_retained_row_keys),
        vec!["charlie".to_string(), "delta".to_string()]
    );
    assert_eq!(
        decode_keys(&churn_step.exited_retained_row_keys),
        vec!["alpha".to_string(), "bravo".to_string()]
    );
    assert_eq!(
        churn_step.stats.status.effective_mode,
        CurrentStateEffectiveMode::DerivedOnly
    );
    assert!(
        churn_step
            .stats
            .coordination
            .backpressure
            .signals
            .contains(&CurrentStateRetentionBackpressureSignal::RankChurnHeavy)
    );

    let final_step = first
        .steps
        .last()
        .expect("leaderboard simulation should have steps");
    assert_eq!(
        decode_keys(&final_step.evaluation.retained_row_keys),
        vec![
            "delta".to_string(),
            "charlie".to_string(),
            "bravo".to_string()
        ]
    );
    assert_eq!(
        final_step.evaluation.stats.membership_changes.entered_rows,
        2
    );
    assert_eq!(
        final_step.evaluation.stats.membership_changes.exited_rows,
        1
    );

    Ok(())
}
