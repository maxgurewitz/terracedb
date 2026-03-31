use terracedb::{
    CurrentStateCompactionRowRemovalMode, CurrentStateComputedOrderingKey,
    CurrentStateComputedOrderingKeys, CurrentStateMissingValuePolicy, CurrentStateOracleRow,
    CurrentStateOrderingContract, CurrentStatePhysicalReclaimSemantics,
    CurrentStatePhysicalRetentionMode, CurrentStatePhysicalRetentionSeam, CurrentStatePlanner,
    CurrentStateProjectionOwnedRange, CurrentStateRankSource,
    CurrentStateRankedMaterializationSeam, CurrentStateRebuildMode, CurrentStateRebuildSeam,
    CurrentStateRetentionContract, CurrentStateRetentionCoordinationContext,
    CurrentStateRetentionSemantics, CurrentStateRetentionTarget, CurrentStateSortDirection,
    CurrentStateThresholdCutoff,
};

use crate::{
    LeaderboardEntry, LeaderboardPolicyMode, LeaderboardPolicyRequest, LeaderboardTieBreak,
    SessionPolicyRequest, SessionRecord, ThresholdRetentionLayout,
};

pub const LEADERBOARD_OUTPUT_TABLE: &str = "leaderboard_top";
const LEADERBOARD_RANGE_START: &[u8] = b"leaderboard:00";
const LEADERBOARD_RANGE_END: &[u8] = b"leaderboard:ff";

pub fn encode_unsigned(value: u64) -> Vec<u8> {
    CurrentStateComputedOrderingKey::new()
        .unsigned(value)
        .into_bytes()
}

pub fn session_contract(policy: &SessionPolicyRequest) -> CurrentStateRetentionContract {
    CurrentStateRetentionContract::threshold(
        policy.revision,
        CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending)
            .with_missing_values(CurrentStateMissingValuePolicy::FailClosed),
        CurrentStateThresholdCutoff::explicit(encode_unsigned(policy.minimum_last_seen_ms)),
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

pub fn session_context(policy: &SessionPolicyRequest) -> CurrentStateRetentionCoordinationContext {
    let physical_reclaim = match policy.layout {
        ThresholdRetentionLayout::RewriteCompactionDelete => {
            CurrentStatePhysicalReclaimSemantics::RewriteCompaction
        }
        ThresholdRetentionLayout::LogicalOnly => CurrentStatePhysicalReclaimSemantics::LogicalOnly,
    };

    CurrentStateRetentionCoordinationContext {
        semantics: CurrentStateRetentionSemantics::new(
            CurrentStateRetentionTarget::RowTable,
            physical_reclaim,
        ),
        cpu_cost_soft_limit: 4,
        rank_churn_soft_limit: 2,
        max_write_bytes_per_second: Some(32),
        ..Default::default()
    }
}

pub fn session_row(record: &SessionRecord) -> CurrentStateOracleRow {
    CurrentStateOracleRow::new(
        record.session_id.as_bytes().to_vec(),
        Some(encode_unsigned(record.last_seen_ms)),
        None,
        record.estimated_row_bytes,
    )
}

pub fn leaderboard_contract(policy: &LeaderboardPolicyRequest) -> CurrentStateRetentionContract {
    let mut contract = CurrentStateRetentionContract::global_rank(
        policy.revision,
        CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending)
            .with_missing_values(CurrentStateMissingValuePolicy::TreatAsLowest),
        policy.limit,
    )
    .with_rank_source(
        CurrentStateRankSource::current_state().with_rebuildable(matches!(
            policy.mode,
            LeaderboardPolicyMode::DestructiveRebuildable
        )),
    )
    .with_planner(CurrentStatePlanner {
        ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
            CurrentStateProjectionOwnedRange {
                output_table: LEADERBOARD_OUTPUT_TABLE.to_string(),
                range_start: LEADERBOARD_RANGE_START.to_vec(),
                range_end: LEADERBOARD_RANGE_END.to_vec(),
            },
        ),
        rebuild: CurrentStateRebuildSeam {
            on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
            on_revision: CurrentStateRebuildMode::RecomputeFromCurrentState,
        },
        ..Default::default()
    });

    if matches!(
        policy.mode,
        LeaderboardPolicyMode::DestructiveRebuildable
            | LeaderboardPolicyMode::DestructiveUnrebuildable
    ) {
        contract.planner.compaction_row_removal =
            CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction;
        contract.planner.physical_retention = CurrentStatePhysicalRetentionSeam {
            mode: CurrentStatePhysicalRetentionMode::Delete,
            ..Default::default()
        };
    }

    contract
}

pub fn leaderboard_context(
    policy: &LeaderboardPolicyRequest,
) -> CurrentStateRetentionCoordinationContext {
    let (target, physical_reclaim, max_write_bytes_per_second) = match policy.mode {
        LeaderboardPolicyMode::DerivedOnly => (
            CurrentStateRetentionTarget::ProjectionOwnedOutput,
            CurrentStatePhysicalReclaimSemantics::DerivedOnly,
            Some(48),
        ),
        LeaderboardPolicyMode::DestructiveRebuildable
        | LeaderboardPolicyMode::DestructiveUnrebuildable => (
            CurrentStateRetentionTarget::RowTable,
            CurrentStatePhysicalReclaimSemantics::RewriteCompaction,
            Some(48),
        ),
    };

    CurrentStateRetentionCoordinationContext {
        semantics: CurrentStateRetentionSemantics::new(target, physical_reclaim),
        cpu_cost_soft_limit: 6,
        rank_churn_soft_limit: 2,
        max_write_bytes_per_second,
        ..Default::default()
    }
}

pub fn leaderboard_row(
    entry: &LeaderboardEntry,
    tie_break: LeaderboardTieBreak,
) -> CurrentStateOracleRow {
    let ordering = match tie_break {
        LeaderboardTieBreak::StableIdAscending => {
            CurrentStateComputedOrderingKeys::leaderboard(entry.points, &entry.player_id)
        }
        LeaderboardTieBreak::CreatedAtThenStableId => CurrentStateComputedOrderingKeys::hybrid(
            entry.points,
            entry.created_at_ms,
            &entry.player_id,
        ),
    };

    CurrentStateOracleRow::from_computed_ordering(
        entry.player_id.as_bytes().to_vec(),
        ordering,
        entry.estimated_row_bytes,
    )
}
