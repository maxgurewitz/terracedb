use terracedb::{CurrentStateOracleRow, CurrentStateRetentionConfiguration};
use terracedb_retention::{LeaderboardRetentionRecipe, ThresholdU64RetentionRecipe};

use crate::{
    LeaderboardEntry, LeaderboardPolicyRequest, LeaderboardTieBreak, SessionPolicyRequest,
    SessionRecord,
};

pub const LEADERBOARD_OUTPUT_TABLE: &str = "leaderboard_top";
const LEADERBOARD_RANGE_START: &[u8] = b"leaderboard:00";
const LEADERBOARD_RANGE_END: &[u8] = b"leaderboard:ff";

pub fn session_configuration(policy: &SessionPolicyRequest) -> CurrentStateRetentionConfiguration {
    ThresholdU64RetentionRecipe::new(policy.revision, policy.minimum_last_seen_ms)
        .layout(policy.layout)
        .configuration()
}

pub fn session_row(record: &SessionRecord) -> CurrentStateOracleRow {
    ThresholdU64RetentionRecipe::row(
        record.session_id.as_bytes().to_vec(),
        record.last_seen_ms,
        record.estimated_row_bytes,
    )
}

pub fn leaderboard_configuration(
    policy: &LeaderboardPolicyRequest,
) -> CurrentStateRetentionConfiguration {
    LeaderboardRetentionRecipe::new(policy.revision, policy.limit, LEADERBOARD_OUTPUT_TABLE)
        .with_output_range(LEADERBOARD_RANGE_START, LEADERBOARD_RANGE_END)
        .mode(policy.mode)
        .tie_break(policy.tie_break)
        .configuration()
}

pub fn leaderboard_row(
    entry: &LeaderboardEntry,
    tie_break: LeaderboardTieBreak,
) -> CurrentStateOracleRow {
    LeaderboardRetentionRecipe::row_with_tie_break(
        entry.player_id.as_bytes(),
        entry.points,
        entry.created_at_ms,
        entry.estimated_row_bytes,
        tie_break,
    )
}
