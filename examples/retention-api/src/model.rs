use serde::{Deserialize, Serialize};
pub use terracedb::{
    CurrentStateOperationalSemantics as ExampleOperationalSemantics,
    CurrentStateOperationalSummary as OperationalView,
};
pub use terracedb_retention::{
    LeaderboardTieBreak, RankedPolicyMode as LeaderboardPolicyMode, ThresholdRetentionLayout,
};

pub const HISTORY_RETENTION_NOTE: &str = "This example only changes generalized current-state retention. MVCC and CDC history retention remain sequence-based and are not modified by these policy updates.";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRecord {
    pub session_id: String,
    pub user_id: String,
    pub last_seen_ms: u64,
    pub estimated_row_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpsertSessionRequest {
    pub session_id: String,
    pub user_id: String,
    pub last_seen_ms: u64,
    pub estimated_row_bytes: u64,
}

impl From<UpsertSessionRequest> for SessionRecord {
    fn from(value: UpsertSessionRequest) -> Self {
        Self {
            session_id: value.session_id,
            user_id: value.user_id,
            last_seen_ms: value.last_seen_ms,
            estimated_row_bytes: value.estimated_row_bytes,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionPolicyRequest {
    pub revision: u64,
    pub minimum_last_seen_ms: u64,
    #[serde(default)]
    pub layout: ThresholdRetentionLayout,
}

impl Default for SessionPolicyRequest {
    fn default() -> Self {
        Self {
            revision: 1,
            minimum_last_seen_ms: 0,
            layout: ThresholdRetentionLayout::RewriteCompactionDelete,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderboardEntry {
    pub player_id: String,
    pub points: u64,
    pub created_at_ms: u64,
    pub estimated_row_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpsertLeaderboardEntryRequest {
    pub player_id: String,
    pub points: u64,
    pub created_at_ms: u64,
    pub estimated_row_bytes: u64,
}

impl From<UpsertLeaderboardEntryRequest> for LeaderboardEntry {
    fn from(value: UpsertLeaderboardEntryRequest) -> Self {
        Self {
            player_id: value.player_id,
            points: value.points,
            created_at_ms: value.created_at_ms,
            estimated_row_bytes: value.estimated_row_bytes,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderboardPolicyRequest {
    pub revision: u64,
    pub limit: usize,
    #[serde(default)]
    pub mode: LeaderboardPolicyMode,
    #[serde(default)]
    pub tie_break: LeaderboardTieBreak,
}

impl Default for LeaderboardPolicyRequest {
    fn default() -> Self {
        Self {
            revision: 1,
            limit: 3,
            mode: LeaderboardPolicyMode::DerivedOnly,
            tie_break: LeaderboardTieBreak::StableIdAscending,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicationChanges {
    pub entered_ids: Vec<String>,
    pub exited_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionPolicyView {
    pub revision: u64,
    pub minimum_last_seen_ms: u64,
    pub layout: ThresholdRetentionLayout,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInspection {
    pub policy: SessionPolicyView,
    pub retained_sessions: Vec<SessionRecord>,
    pub non_retained_sessions: Vec<SessionRecord>,
    pub reclaimable_session_ids: Vec<String>,
    pub deferred_session_ids: Vec<String>,
    pub snapshot_pins: Vec<String>,
    pub operational: OperationalView,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderboardBoundaryView {
    pub player_id: String,
    pub points: u64,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderboardPolicyView {
    pub revision: u64,
    pub limit: usize,
    pub mode: LeaderboardPolicyMode,
    pub tie_break: LeaderboardTieBreak,
    pub output_table: String,
    pub boundary: Option<LeaderboardBoundaryView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderboardInspection {
    pub policy: LeaderboardPolicyView,
    pub source_rows: Vec<LeaderboardEntry>,
    pub derived_top_players: Vec<LeaderboardEntry>,
    pub outside_limit_rows: Vec<LeaderboardEntry>,
    pub publication_changes: PublicationChanges,
    pub operational: OperationalView,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExampleInspection {
    pub history_retention_note: String,
    pub sessions: SessionInspection,
    pub leaderboard: LeaderboardInspection,
}
