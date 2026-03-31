use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Frozen ordering contract for generalized current-state retention.
///
/// Callers must encode primary sort keys and optional tie-break keys into byte
/// strings whose lexicographic order matches the desired natural order. Primary
/// keys are compared lexicographically using the configured `direction`.
/// Tie-break keys are always compared lexicographically in ascending order, and
/// row keys are the final ascending tie-breaker to guarantee a deterministic
/// total order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateOrderingContract {
    pub direction: CurrentStateSortDirection,
    pub missing_values: CurrentStateMissingValuePolicy,
}

impl CurrentStateOrderingContract {
    pub fn new(direction: CurrentStateSortDirection) -> Self {
        Self {
            direction,
            missing_values: CurrentStateMissingValuePolicy::FailClosed,
        }
    }

    pub fn with_missing_values(mut self, missing_values: CurrentStateMissingValuePolicy) -> Self {
        self.missing_values = missing_values;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateSortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateMissingValuePolicy {
    FailClosed,
    ExcludeRow,
    TreatAsLowest,
    TreatAsHighest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateThresholdCutoff {
    pub source: CurrentStateCutoffSource,
    pub encoded_value: Vec<u8>,
}

impl CurrentStateThresholdCutoff {
    pub fn explicit(encoded_value: impl Into<Vec<u8>>) -> Self {
        Self {
            source: CurrentStateCutoffSource::Explicit,
            encoded_value: encoded_value.into(),
        }
    }

    pub fn explicit_ordering_key(encoded_value: CurrentStateComputedOrderingKey) -> Self {
        Self::explicit(encoded_value.into_bytes())
    }

    pub fn explicit_unsigned(value: u64) -> Self {
        Self::explicit_ordering_key(CurrentStateComputedOrderingKey::new().unsigned(value))
    }

    pub fn explicit_signed(value: i64) -> Self {
        Self::explicit_ordering_key(CurrentStateComputedOrderingKey::new().signed(value))
    }

    pub fn explicit_timestamp_millis(value: u64) -> Self {
        Self::explicit_ordering_key(CurrentStateComputedOrderingKey::new().timestamp_millis(value))
    }

    pub fn explicit_bytes(value: impl AsRef<[u8]>) -> Self {
        Self::explicit_ordering_key(CurrentStateComputedOrderingKey::new().bytes(value))
    }

    pub fn explicit_utf8(value: impl AsRef<str>) -> Self {
        Self::explicit_bytes(value.as_ref().as_bytes())
    }

    pub fn engine_derived(
        source_name: impl Into<String>,
        encoded_value: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            source: CurrentStateCutoffSource::EngineDerived {
                source_name: source_name.into(),
            },
            encoded_value: encoded_value.into(),
        }
    }

    pub fn engine_derived_ordering_key(
        source_name: impl Into<String>,
        encoded_value: CurrentStateComputedOrderingKey,
    ) -> Self {
        Self::engine_derived(source_name, encoded_value.into_bytes())
    }

    pub fn engine_derived_unsigned(source_name: impl Into<String>, value: u64) -> Self {
        Self::engine_derived_ordering_key(
            source_name,
            CurrentStateComputedOrderingKey::new().unsigned(value),
        )
    }

    pub fn engine_derived_signed(source_name: impl Into<String>, value: i64) -> Self {
        Self::engine_derived_ordering_key(
            source_name,
            CurrentStateComputedOrderingKey::new().signed(value),
        )
    }

    pub fn engine_derived_timestamp_millis(source_name: impl Into<String>, value: u64) -> Self {
        Self::engine_derived_ordering_key(
            source_name,
            CurrentStateComputedOrderingKey::new().timestamp_millis(value),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CurrentStateCutoffSource {
    Explicit,
    EngineDerived { source_name: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CurrentStateRetentionPolicy {
    Threshold(CurrentStateThresholdRetentionPolicy),
    GlobalRank(CurrentStateRankRetentionPolicy),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateThresholdRetentionPolicy {
    pub order: CurrentStateOrderingContract,
    pub cutoff: CurrentStateThresholdCutoff,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRankRetentionPolicy {
    pub order: CurrentStateOrderingContract,
    pub limit: usize,
    #[serde(default)]
    pub source: CurrentStateRankSource,
}

impl CurrentStateRankRetentionPolicy {
    pub fn with_source(mut self, source: CurrentStateRankSource) -> Self {
        self.source = source;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRankSource {
    pub scope: CurrentStateRankSourceScope,
    pub rebuildable: bool,
}

impl CurrentStateRankSource {
    pub fn current_state() -> Self {
        Self {
            scope: CurrentStateRankSourceScope::CurrentState,
            rebuildable: false,
        }
    }

    pub fn projection_owned_range(
        source_table: impl Into<String>,
        range_start: impl Into<Vec<u8>>,
        range_end: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            scope: CurrentStateRankSourceScope::ProjectionOwnedRange(CurrentStateRankSourceRange {
                source_table: source_table.into(),
                range_start: range_start.into(),
                range_end: range_end.into(),
            }),
            rebuildable: false,
        }
    }

    pub fn with_rebuildable(mut self, rebuildable: bool) -> Self {
        self.rebuildable = rebuildable;
        self
    }
}

impl Default for CurrentStateRankSource {
    fn default() -> Self {
        Self::current_state()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CurrentStateRankSourceScope {
    #[default]
    CurrentState,
    ProjectionOwnedRange(CurrentStateRankSourceRange),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRankSourceRange {
    pub source_table: String,
    pub range_start: Vec<u8>,
    pub range_end: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionContract {
    pub revision: u64,
    pub policy: CurrentStateRetentionPolicy,
    pub planner: CurrentStatePlanner,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionConfiguration {
    pub contract: CurrentStateRetentionContract,
    pub context: CurrentStateRetentionCoordinationContext,
}

impl CurrentStateRetentionConfiguration {
    pub fn new(
        contract: CurrentStateRetentionContract,
        context: CurrentStateRetentionCoordinationContext,
    ) -> Self {
        Self { contract, context }
    }

    pub fn into_parts(
        self,
    ) -> (
        CurrentStateRetentionContract,
        CurrentStateRetentionCoordinationContext,
    ) {
        (self.contract, self.context)
    }
}

impl CurrentStateRetentionContract {
    pub fn threshold(
        revision: u64,
        order: CurrentStateOrderingContract,
        cutoff: CurrentStateThresholdCutoff,
    ) -> Self {
        Self {
            revision,
            policy: CurrentStateRetentionPolicy::Threshold(CurrentStateThresholdRetentionPolicy {
                order,
                cutoff,
            }),
            planner: CurrentStatePlanner::default(),
        }
    }

    pub fn global_rank(revision: u64, order: CurrentStateOrderingContract, limit: usize) -> Self {
        Self {
            revision,
            policy: CurrentStateRetentionPolicy::GlobalRank(CurrentStateRankRetentionPolicy {
                order,
                limit,
                source: CurrentStateRankSource::default(),
            }),
            planner: CurrentStatePlanner::default(),
        }
    }

    pub fn with_planner(mut self, planner: CurrentStatePlanner) -> Self {
        self.planner = planner;
        self
    }

    pub fn with_rank_source(mut self, source: CurrentStateRankSource) -> Self {
        if let CurrentStateRetentionPolicy::GlobalRank(policy) = &mut self.policy {
            policy.source = source;
        }
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStatePlanner {
    pub compaction_row_removal: CurrentStateCompactionRowRemovalMode,
    pub ranked_materialization: CurrentStateRankedMaterializationSeam,
    pub physical_retention: CurrentStatePhysicalRetentionSeam,
    pub rebuild: CurrentStateRebuildSeam,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateCompactionRowRemovalMode {
    #[default]
    Disabled,
    RemoveDuringCompaction,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum CurrentStateRankedMaterializationSeam {
    #[default]
    None,
    ProjectionOwned(CurrentStateProjectionOwnedRange),
    DerivedOnly {
        name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateProjectionOwnedRange {
    pub output_table: String,
    pub range_start: Vec<u8>,
    pub range_end: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStatePhysicalRetentionSeam {
    pub mode: CurrentStatePhysicalRetentionMode,
    pub exactness: CurrentStateExactnessRequirement,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStatePhysicalRetentionMode {
    #[default]
    None,
    Offload,
    Delete,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateExactnessRequirement {
    #[default]
    BestEffort,
    FailClosed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRebuildSeam {
    pub on_restart: CurrentStateRebuildMode,
    pub on_revision: CurrentStateRebuildMode,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateRebuildMode {
    #[default]
    None,
    RecomputeFromCurrentState,
    RecomputeDerivedState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetainedSetSummary {
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionStats {
    pub policy_revision: u64,
    pub effective_logical_floor: Option<CurrentStateLogicalFloor>,
    pub retained_set: CurrentStateRetainedSetSummary,
    pub membership_changes: CurrentStateRetentionMembershipChanges,
    pub evaluation_cost: CurrentStateRetentionEvaluationCost,
    pub reclaimed_rows: u64,
    pub reclaimed_bytes: u64,
    pub deferred_rows: u64,
    pub deferred_bytes: u64,
    pub coordination: CurrentStateRetentionCoordinationStats,
    pub status: CurrentStateRetentionStatus,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionMembershipChanges {
    pub entered_rows: u64,
    pub exited_rows: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionEvaluationCost {
    pub rows_scanned: u64,
    pub rows_ranked: u64,
    pub rows_materialized: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionCoordinationStats {
    pub active_plan: Option<CurrentStateRetentionCoordinationPlan>,
    pub logical_rows_pending: u64,
    pub physical_rows_pending: u64,
    pub deferred_physical_rows: u64,
    pub physical_bytes_pending: u64,
    pub evaluation_cost: CurrentStateRetentionEvaluationCost,
    pub retained_membership_change_count: u64,
    pub rebuild_fallback_pending: bool,
    pub backpressure: CurrentStateRetentionBackpressure,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionBackpressure {
    pub signals: Vec<CurrentStateRetentionBackpressureSignal>,
    pub throttle_writes: bool,
    pub stall_writes: bool,
    pub max_write_bytes_per_second: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateRetentionBackpressureSignal {
    CpuBound,
    RankChurnHeavy,
    RewriteCompactionBlocked,
    SnapshotPinned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionCoordinationPlan {
    pub policy_revision: u64,
    pub phase: CurrentStateRetentionPlanPhase,
    pub semantics: CurrentStateRetentionSemantics,
    pub logical_action: CurrentStateLogicalReclaimAction,
    pub physical_action: CurrentStatePhysicalCleanupAction,
    pub logical_row_keys: Vec<Vec<u8>>,
    pub physical_row_keys: Vec<Vec<u8>>,
    pub deferred_row_keys: Vec<Vec<u8>>,
    pub physical_bytes_pending: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateManifestPublicationResult {
    pub published: bool,
    pub plan: Option<CurrentStateRetentionCoordinationPlan>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateLocalCleanupResult {
    pub completed: bool,
    pub removed_row_keys: Vec<Vec<u8>>,
    pub completed_plan: Option<CurrentStateRetentionCoordinationPlan>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateRetentionPlanPhase {
    Planned,
    ManifestPublished,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionSemantics {
    pub target: CurrentStateRetentionTarget,
    pub physical_reclaim: CurrentStatePhysicalReclaimSemantics,
}

impl CurrentStateRetentionSemantics {
    pub fn new(
        target: CurrentStateRetentionTarget,
        physical_reclaim: CurrentStatePhysicalReclaimSemantics,
    ) -> Self {
        Self {
            target,
            physical_reclaim,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateRetentionTarget {
    RowTable,
    ColumnarTable,
    ProjectionOwnedOutput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStatePhysicalReclaimSemantics {
    LogicalOnly,
    DerivedOnly,
    ExactFileSelection,
    RewriteCompaction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateLogicalReclaimAction {
    None,
    RewriteCompaction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStatePhysicalCleanupAction {
    None,
    Delete,
    Offload,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionCoordinationContext {
    pub semantics: CurrentStateRetentionSemantics,
    pub requires_manifest_publication: bool,
    pub requires_local_cleanup: bool,
    pub cpu_cost_soft_limit: u64,
    pub rank_churn_soft_limit: u64,
    pub max_write_bytes_per_second: Option<u64>,
}

impl CurrentStateRetentionCoordinationContext {
    pub fn new(semantics: CurrentStateRetentionSemantics) -> Self {
        Self {
            semantics,
            ..Default::default()
        }
    }
}

impl Default for CurrentStateRetentionCoordinationContext {
    fn default() -> Self {
        Self {
            semantics: CurrentStateRetentionSemantics::new(
                CurrentStateRetentionTarget::RowTable,
                CurrentStatePhysicalReclaimSemantics::ExactFileSelection,
            ),
            requires_manifest_publication: true,
            requires_local_cleanup: true,
            cpu_cost_soft_limit: 1_000,
            rank_churn_soft_limit: 128,
            max_write_bytes_per_second: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionStatus {
    pub effective_mode: CurrentStateEffectiveMode,
    pub reasons: Vec<CurrentStateRetentionReason>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateOperationalSummary {
    pub semantics: CurrentStateOperationalSemantics,
    pub effective_mode: CurrentStateEffectiveMode,
    pub reasons: Vec<CurrentStateRetentionReason>,
    pub active_plan_phase: Option<CurrentStateRetentionPlanPhase>,
    pub blocked_by_snapshots: bool,
    pub waiting_for_manifest_publication: bool,
    pub waiting_for_local_cleanup: bool,
    pub backpressure_signals: Vec<CurrentStateRetentionBackpressureSignal>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateOperationalSemantics {
    PhysicalReclaim,
    WaitingOnPhysicalReclaim,
    DerivedOnly,
    LogicalOnly,
    Skipped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateEffectiveMode {
    PhysicalReclaim,
    DerivedOnly,
    Skipped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CurrentStateRetentionReason {
    BlockedBySnapshots,
    Deferred {
        reason: CurrentStateRetentionDeferredReason,
    },
    Skipped {
        reason: CurrentStateRetentionSkipReason,
    },
    DegradedToDerivedOnly {
        reason: CurrentStateDerivedOnlyReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateRetentionDeferredReason {
    ExactReclaimNotAvailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateRetentionSkipReason {
    NoPlannerSeamConfigured,
    UnsupportedPhysicalLayout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateDerivedOnlyReason {
    ProjectionOwnedWithoutPhysicalReclaim,
    DerivedMaterializationOnly,
    PhysicalReclaimNotSupportedByTarget,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CurrentStateLogicalFloor {
    Threshold {
        cutoff: CurrentStateThresholdCutoff,
    },
    Rank {
        limit: usize,
        boundary: Option<CurrentStateRankBoundary>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRankBoundary {
    pub primary_sort_key: Option<Vec<u8>>,
    pub tie_break_key: Option<Vec<u8>>,
    pub row_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateOracleRow {
    pub row_key: Vec<u8>,
    pub primary_sort_key: Option<Vec<u8>>,
    pub tie_break_key: Option<Vec<u8>>,
    pub estimated_row_bytes: u64,
}

impl CurrentStateOracleRow {
    pub fn new(
        row_key: impl Into<Vec<u8>>,
        primary_sort_key: Option<Vec<u8>>,
        tie_break_key: Option<Vec<u8>>,
        estimated_row_bytes: u64,
    ) -> Self {
        Self {
            row_key: row_key.into(),
            primary_sort_key,
            tie_break_key,
            estimated_row_bytes,
        }
    }

    pub fn from_computed_ordering(
        row_key: impl Into<Vec<u8>>,
        ordering: CurrentStateComputedOrderingKeys,
        estimated_row_bytes: u64,
    ) -> Self {
        Self::new(
            row_key,
            Some(ordering.primary_sort_key),
            Some(ordering.tie_break_key),
            estimated_row_bytes,
        )
    }

    pub fn from_threshold(
        row_key: impl Into<Vec<u8>>,
        primary_sort_key: impl Into<Vec<u8>>,
        estimated_row_bytes: u64,
    ) -> Self {
        Self::new(
            row_key,
            Some(primary_sort_key.into()),
            None,
            estimated_row_bytes,
        )
    }

    pub fn from_threshold_ordering_key(
        row_key: impl Into<Vec<u8>>,
        primary_sort_key: CurrentStateComputedOrderingKey,
        estimated_row_bytes: u64,
    ) -> Self {
        Self::from_threshold(row_key, primary_sort_key.into_bytes(), estimated_row_bytes)
    }

    pub fn from_threshold_unsigned(
        row_key: impl Into<Vec<u8>>,
        value: u64,
        estimated_row_bytes: u64,
    ) -> Self {
        Self::from_threshold_ordering_key(
            row_key,
            CurrentStateComputedOrderingKey::new().unsigned(value),
            estimated_row_bytes,
        )
    }

    pub fn from_threshold_signed(
        row_key: impl Into<Vec<u8>>,
        value: i64,
        estimated_row_bytes: u64,
    ) -> Self {
        Self::from_threshold_ordering_key(
            row_key,
            CurrentStateComputedOrderingKey::new().signed(value),
            estimated_row_bytes,
        )
    }

    pub fn from_threshold_timestamp_millis(
        row_key: impl Into<Vec<u8>>,
        value: u64,
        estimated_row_bytes: u64,
    ) -> Self {
        Self::from_threshold_ordering_key(
            row_key,
            CurrentStateComputedOrderingKey::new().timestamp_millis(value),
            estimated_row_bytes,
        )
    }

    pub fn from_threshold_bytes(
        row_key: impl Into<Vec<u8>>,
        value: impl AsRef<[u8]>,
        estimated_row_bytes: u64,
    ) -> Self {
        Self::from_threshold_ordering_key(
            row_key,
            CurrentStateComputedOrderingKey::new().bytes(value),
            estimated_row_bytes,
        )
    }

    pub fn from_threshold_utf8(
        row_key: impl Into<Vec<u8>>,
        value: impl AsRef<str>,
        estimated_row_bytes: u64,
    ) -> Self {
        Self::from_threshold_bytes(row_key, value.as_ref().as_bytes(), estimated_row_bytes)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateComputedOrderingKey {
    encoded: Vec<u8>,
}

impl CurrentStateComputedOrderingKey {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn unsigned(mut self, value: u64) -> Self {
        self.encoded.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn signed(mut self, value: i64) -> Self {
        let encoded = ((value as u64) ^ 0x8000_0000_0000_0000_u64).to_be_bytes();
        self.encoded.extend_from_slice(&encoded);
        self
    }

    pub fn timestamp_millis(self, value: u64) -> Self {
        self.unsigned(value)
    }

    pub fn bytes(mut self, value: impl AsRef<[u8]>) -> Self {
        append_escaped_ordering_component(&mut self.encoded, value.as_ref());
        self
    }

    pub fn utf8(self, value: impl AsRef<str>) -> Self {
        self.bytes(value.as_ref().as_bytes())
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.encoded
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateComputedOrderingKeys {
    pub primary_sort_key: Vec<u8>,
    pub tie_break_key: Vec<u8>,
}

impl CurrentStateComputedOrderingKeys {
    pub fn new(primary_sort_key: impl Into<Vec<u8>>, tie_break_key: impl Into<Vec<u8>>) -> Self {
        Self {
            primary_sort_key: primary_sort_key.into(),
            tie_break_key: tie_break_key.into(),
        }
    }

    pub fn leaderboard(points: u64, stable_id: impl AsRef<[u8]>) -> Self {
        Self::new(
            CurrentStateComputedOrderingKey::new()
                .unsigned(points)
                .into_bytes(),
            CurrentStateComputedOrderingKey::new()
                .bytes(stable_id)
                .into_bytes(),
        )
    }

    pub fn recent_item(updated_at_millis: u64, stable_id: impl AsRef<[u8]>) -> Self {
        Self::new(
            CurrentStateComputedOrderingKey::new()
                .timestamp_millis(updated_at_millis)
                .into_bytes(),
            CurrentStateComputedOrderingKey::new()
                .bytes(stable_id)
                .into_bytes(),
        )
    }

    pub fn hybrid(points: u64, created_at_millis: u64, stable_id: impl AsRef<[u8]>) -> Self {
        Self::new(
            CurrentStateComputedOrderingKey::new()
                .unsigned(points)
                .timestamp_millis(created_at_millis)
                .into_bytes(),
            CurrentStateComputedOrderingKey::new()
                .bytes(stable_id)
                .into_bytes(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CurrentStateOracleMutation {
    Upsert(CurrentStateOracleRow),
    Delete { row_key: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionEvaluation {
    pub retained_row_keys: Vec<Vec<u8>>,
    pub entered_retained_row_keys: Vec<Vec<u8>>,
    pub exited_retained_row_keys: Vec<Vec<u8>>,
    pub non_retained_row_keys: Vec<Vec<u8>>,
    pub reclaimable_row_keys: Vec<Vec<u8>>,
    pub deferred_row_keys: Vec<Vec<u8>>,
    pub stats: CurrentStateRetentionStats,
}

impl CurrentStateRetentionEvaluation {
    pub fn operational_summary(&self) -> CurrentStateOperationalSummary {
        let active_plan_phase = self
            .stats
            .coordination
            .active_plan
            .as_ref()
            .map(|plan| plan.phase);
        let blocked_by_snapshots = self
            .stats
            .status
            .reasons
            .iter()
            .any(|reason| matches!(reason, CurrentStateRetentionReason::BlockedBySnapshots));
        let waiting_for_manifest_publication =
            active_plan_phase == Some(CurrentStateRetentionPlanPhase::Planned);
        let waiting_for_local_cleanup = active_plan_phase
            == Some(CurrentStateRetentionPlanPhase::ManifestPublished)
            || self.stats.coordination.physical_rows_pending > 0
            || self.stats.coordination.deferred_physical_rows > 0
            || self.stats.deferred_rows > 0;
        let semantics = if self.stats.status.reasons.iter().any(|reason| {
            matches!(
                reason,
                CurrentStateRetentionReason::Skipped {
                    reason: CurrentStateRetentionSkipReason::UnsupportedPhysicalLayout,
                }
            )
        }) {
            CurrentStateOperationalSemantics::LogicalOnly
        } else {
            match self.stats.status.effective_mode {
                CurrentStateEffectiveMode::PhysicalReclaim => {
                    if waiting_for_manifest_publication || waiting_for_local_cleanup {
                        CurrentStateOperationalSemantics::WaitingOnPhysicalReclaim
                    } else {
                        CurrentStateOperationalSemantics::PhysicalReclaim
                    }
                }
                CurrentStateEffectiveMode::DerivedOnly => {
                    CurrentStateOperationalSemantics::DerivedOnly
                }
                CurrentStateEffectiveMode::Skipped => CurrentStateOperationalSemantics::Skipped,
            }
        };

        CurrentStateOperationalSummary {
            semantics,
            effective_mode: self.stats.status.effective_mode,
            reasons: self.stats.status.reasons.clone(),
            active_plan_phase,
            blocked_by_snapshots,
            waiting_for_manifest_publication,
            waiting_for_local_cleanup,
            backpressure_signals: self.stats.coordination.backpressure.signals.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionOracleSnapshot {
    pub contract: CurrentStateRetentionContract,
    pub rows: Vec<CurrentStateOracleRow>,
    pub snapshot_pins: Vec<Vec<u8>>,
    pub published_ranked_row_keys: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionCoordinatorSnapshot {
    pub oracle: CurrentStateRetentionOracleSnapshot,
    pub context: CurrentStateRetentionCoordinationContext,
    pub state: CurrentStateRetentionCoordinatorState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionCoordinatorState {
    pub active_plan: Option<CurrentStateRetentionCoordinationPlan>,
    pub last_observed_retained_row_keys: Option<Vec<Vec<u8>>>,
    pub rebuild_fallback_pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentStateRetentionOracle {
    contract: CurrentStateRetentionContract,
    rows: BTreeMap<Vec<u8>, CurrentStateOracleRow>,
    snapshot_pins: BTreeSet<Vec<u8>>,
    published_ranked_row_keys: BTreeSet<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentStateRetentionCoordinator {
    oracle: CurrentStateRetentionOracle,
    context: CurrentStateRetentionCoordinationContext,
    state: CurrentStateRetentionCoordinatorState,
}

type OracleEvaluationPartition = (
    Vec<CurrentStateOracleRow>,
    Vec<CurrentStateOracleRow>,
    Option<CurrentStateLogicalFloor>,
    CurrentStateRetentionEvaluationCost,
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ThresholdRetentionDisposition {
    Retained,
    NonRetained,
    ExcludedFromPolicy,
}

impl CurrentStateRetentionOracle {
    pub fn new(contract: CurrentStateRetentionContract) -> Self {
        Self {
            contract,
            rows: BTreeMap::new(),
            snapshot_pins: BTreeSet::new(),
            published_ranked_row_keys: BTreeSet::new(),
        }
    }

    pub fn from_snapshot(snapshot: CurrentStateRetentionOracleSnapshot) -> Self {
        Self {
            contract: snapshot.contract,
            rows: snapshot
                .rows
                .into_iter()
                .map(|row| (row.row_key.clone(), row))
                .collect(),
            snapshot_pins: snapshot.snapshot_pins.into_iter().collect(),
            published_ranked_row_keys: snapshot.published_ranked_row_keys.into_iter().collect(),
        }
    }

    pub fn snapshot(&self) -> CurrentStateRetentionOracleSnapshot {
        CurrentStateRetentionOracleSnapshot {
            contract: self.contract.clone(),
            rows: self.rows.values().cloned().collect(),
            snapshot_pins: self.snapshot_pins.iter().cloned().collect(),
            published_ranked_row_keys: self.published_ranked_row_keys.iter().cloned().collect(),
        }
    }

    pub fn contract(&self) -> &CurrentStateRetentionContract {
        &self.contract
    }

    pub fn set_contract(&mut self, contract: CurrentStateRetentionContract) {
        self.contract = contract;
    }

    pub fn apply(&mut self, mutation: CurrentStateOracleMutation) {
        match mutation {
            CurrentStateOracleMutation::Upsert(row) => {
                self.rows.insert(row.row_key.clone(), row);
            }
            CurrentStateOracleMutation::Delete { row_key } => {
                self.rows.remove(&row_key);
                self.snapshot_pins.remove(&row_key);
            }
        }
    }

    pub fn set_snapshot_pins<I>(&mut self, row_keys: I)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        self.snapshot_pins = row_keys.into_iter().collect();
    }

    pub fn clear_snapshot_pins(&mut self) {
        self.snapshot_pins.clear();
    }

    pub fn publish_retained_set(&mut self) -> Result<(), CurrentStateRetentionError> {
        let evaluation = self.evaluate()?;
        self.published_ranked_row_keys = evaluation.retained_row_keys.into_iter().collect();
        Ok(())
    }

    pub fn evaluate(&self) -> Result<CurrentStateRetentionEvaluation, CurrentStateRetentionError> {
        self.validate_planner()?;

        let (retained_rows, non_retained_rows, effective_logical_floor, evaluation_cost) =
            match &self.contract.policy {
                CurrentStateRetentionPolicy::Threshold(policy) => {
                    self.evaluate_threshold(policy)?
                }
                CurrentStateRetentionPolicy::GlobalRank(policy) => self.evaluate_rank(policy)?,
            };

        let (effective_mode, mut reasons) = self.effective_mode_and_reasons();
        let non_retained_set = non_retained_rows
            .iter()
            .map(|row| row.row_key.clone())
            .collect::<BTreeSet<_>>();
        let retained_set = retained_rows
            .iter()
            .map(|row| row.row_key.clone())
            .collect::<BTreeSet<_>>();
        let (entered_retained_row_keys, exited_retained_row_keys) = if matches!(
            &self.contract.policy,
            CurrentStateRetentionPolicy::GlobalRank(_)
        ) {
            (
                retained_set
                    .difference(&self.published_ranked_row_keys)
                    .cloned()
                    .collect::<Vec<_>>(),
                self.published_ranked_row_keys
                    .difference(&retained_set)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        } else {
            (Vec::new(), Vec::new())
        };
        let exact_reclaim_is_deferred = self.threshold_exact_reclaim_is_deferred();
        let exact_reclaim_blocks_reclaim =
            exact_reclaim_is_deferred && !non_retained_rows.is_empty();
        let snapshot_blocks_reclaim = non_retained_rows
            .iter()
            .any(|row| self.snapshot_pins.contains(&row.row_key));

        let (reclaimable_rows, deferred_rows) = match effective_mode {
            CurrentStateEffectiveMode::PhysicalReclaim => {
                let mut reclaimable = Vec::new();
                let mut deferred = Vec::new();
                for row in &non_retained_rows {
                    if exact_reclaim_blocks_reclaim || self.snapshot_pins.contains(&row.row_key) {
                        deferred.push(row.clone());
                    } else {
                        reclaimable.push(row.clone());
                    }
                }
                if exact_reclaim_blocks_reclaim {
                    reasons.push(CurrentStateRetentionReason::Deferred {
                        reason: CurrentStateRetentionDeferredReason::ExactReclaimNotAvailable,
                    });
                }
                if snapshot_blocks_reclaim {
                    reasons.push(CurrentStateRetentionReason::BlockedBySnapshots);
                }
                (reclaimable, deferred)
            }
            CurrentStateEffectiveMode::DerivedOnly | CurrentStateEffectiveMode::Skipped => {
                (Vec::new(), Vec::new())
            }
        };

        debug_assert_eq!(
            non_retained_set.len(),
            non_retained_rows.len(),
            "oracle row keys should remain unique"
        );

        Ok(CurrentStateRetentionEvaluation {
            retained_row_keys: retained_rows
                .iter()
                .map(|row| row.row_key.clone())
                .collect(),
            entered_retained_row_keys: entered_retained_row_keys.clone(),
            exited_retained_row_keys: exited_retained_row_keys.clone(),
            non_retained_row_keys: non_retained_rows
                .iter()
                .map(|row| row.row_key.clone())
                .collect(),
            reclaimable_row_keys: reclaimable_rows
                .iter()
                .map(|row| row.row_key.clone())
                .collect(),
            deferred_row_keys: deferred_rows
                .iter()
                .map(|row| row.row_key.clone())
                .collect(),
            stats: CurrentStateRetentionStats {
                policy_revision: self.contract.revision,
                effective_logical_floor,
                retained_set: CurrentStateRetainedSetSummary {
                    rows: retained_rows.len() as u64,
                    bytes: retained_rows
                        .iter()
                        .map(|row| row.estimated_row_bytes)
                        .sum(),
                },
                membership_changes: CurrentStateRetentionMembershipChanges {
                    entered_rows: entered_retained_row_keys.len() as u64,
                    exited_rows: exited_retained_row_keys.len() as u64,
                },
                evaluation_cost,
                reclaimed_rows: reclaimable_rows.len() as u64,
                reclaimed_bytes: reclaimable_rows
                    .iter()
                    .map(|row| row.estimated_row_bytes)
                    .sum(),
                deferred_rows: deferred_rows.len() as u64,
                deferred_bytes: deferred_rows
                    .iter()
                    .map(|row| row.estimated_row_bytes)
                    .sum(),
                coordination: CurrentStateRetentionCoordinationStats::default(),
                status: CurrentStateRetentionStatus {
                    effective_mode,
                    reasons,
                },
            },
        })
    }

    fn evaluate_threshold(
        &self,
        policy: &CurrentStateThresholdRetentionPolicy,
    ) -> Result<OracleEvaluationPartition, CurrentStateRetentionError> {
        let mut retained = Vec::new();
        let mut non_retained = Vec::new();

        for row in self.rows.values() {
            match threshold_retention_disposition(policy, row)? {
                ThresholdRetentionDisposition::Retained
                | ThresholdRetentionDisposition::ExcludedFromPolicy => retained.push(row.clone()),
                ThresholdRetentionDisposition::NonRetained => non_retained.push(row.clone()),
            }
        }

        retained.sort_by(|left, right| left.row_key.cmp(&right.row_key));
        non_retained.sort_by(|left, right| left.row_key.cmp(&right.row_key));

        Ok((
            retained,
            non_retained,
            Some(CurrentStateLogicalFloor::Threshold {
                cutoff: policy.cutoff.clone(),
            }),
            CurrentStateRetentionEvaluationCost {
                rows_scanned: self.rows.len() as u64,
                rows_ranked: 0,
                rows_materialized: 0,
            },
        ))
    }

    fn evaluate_rank(
        &self,
        policy: &CurrentStateRankRetentionPolicy,
    ) -> Result<OracleEvaluationPartition, CurrentStateRetentionError> {
        let mut candidates = Vec::new();
        let mut non_retained = Vec::new();

        for row in self.rows.values() {
            if rank_row_participates(policy, row)? {
                candidates.push(row.clone());
            } else {
                non_retained.push(row.clone());
            }
        }

        candidates.sort_by(|left, right| compare_rank_rows(&policy.order, left, right));

        let retained_count = policy.limit.min(candidates.len());
        let retained = candidates[..retained_count].to_vec();
        non_retained.extend_from_slice(&candidates[retained_count..]);
        non_retained.sort_by(|left, right| left.row_key.cmp(&right.row_key));

        let boundary = retained.last().map(|row| CurrentStateRankBoundary {
            primary_sort_key: row.primary_sort_key.clone(),
            tie_break_key: row.tie_break_key.clone(),
            row_key: row.row_key.clone(),
        });

        Ok((
            retained,
            non_retained,
            Some(CurrentStateLogicalFloor::Rank {
                limit: policy.limit,
                boundary,
            }),
            CurrentStateRetentionEvaluationCost {
                rows_scanned: self.rows.len() as u64,
                rows_ranked: candidates.len() as u64,
                rows_materialized: retained_count as u64,
            },
        ))
    }

    fn validate_planner(&self) -> Result<(), CurrentStateRetentionError> {
        let CurrentStateRetentionPolicy::GlobalRank(policy) = &self.contract.policy else {
            return Ok(());
        };

        let requests_physical_reclaim = self.contract.planner.compaction_row_removal
            != CurrentStateCompactionRowRemovalMode::Disabled
            || self.contract.planner.physical_retention.mode
                != CurrentStatePhysicalRetentionMode::None;
        if requests_physical_reclaim && !policy.source.rebuildable {
            return Err(
                CurrentStateRetentionError::RankPhysicalRetentionRequiresRebuildableSource {
                    rank_source: policy.source.clone(),
                },
            );
        }

        Ok(())
    }

    fn effective_mode_and_reasons(
        &self,
    ) -> (CurrentStateEffectiveMode, Vec<CurrentStateRetentionReason>) {
        if self.contract.planner.compaction_row_removal
            != CurrentStateCompactionRowRemovalMode::Disabled
            || self.contract.planner.physical_retention.mode
                != CurrentStatePhysicalRetentionMode::None
        {
            return (CurrentStateEffectiveMode::PhysicalReclaim, Vec::new());
        }

        match &self.contract.planner.ranked_materialization {
            CurrentStateRankedMaterializationSeam::ProjectionOwned(_) => (
                CurrentStateEffectiveMode::DerivedOnly,
                vec![CurrentStateRetentionReason::DegradedToDerivedOnly {
                    reason: CurrentStateDerivedOnlyReason::ProjectionOwnedWithoutPhysicalReclaim,
                }],
            ),
            CurrentStateRankedMaterializationSeam::DerivedOnly { .. } => (
                CurrentStateEffectiveMode::DerivedOnly,
                vec![CurrentStateRetentionReason::DegradedToDerivedOnly {
                    reason: CurrentStateDerivedOnlyReason::DerivedMaterializationOnly,
                }],
            ),
            CurrentStateRankedMaterializationSeam::None => (
                CurrentStateEffectiveMode::Skipped,
                vec![CurrentStateRetentionReason::Skipped {
                    reason: CurrentStateRetentionSkipReason::NoPlannerSeamConfigured,
                }],
            ),
        }
    }

    fn threshold_exact_reclaim_is_deferred(&self) -> bool {
        matches!(
            self.contract.policy,
            CurrentStateRetentionPolicy::Threshold(_)
        ) && self.contract.planner.compaction_row_removal
            == CurrentStateCompactionRowRemovalMode::Disabled
            && self.contract.planner.physical_retention.mode
                != CurrentStatePhysicalRetentionMode::None
            && self.contract.planner.physical_retention.exactness
                == CurrentStateExactnessRequirement::FailClosed
    }
}

impl CurrentStateRetentionCoordinator {
    pub fn from_configuration(configuration: CurrentStateRetentionConfiguration) -> Self {
        let (contract, context) = configuration.into_parts();
        Self::new(contract, context)
    }

    pub fn new(
        contract: CurrentStateRetentionContract,
        context: CurrentStateRetentionCoordinationContext,
    ) -> Self {
        Self {
            oracle: CurrentStateRetentionOracle::new(contract),
            context,
            state: CurrentStateRetentionCoordinatorState::default(),
        }
    }

    pub fn from_snapshot(snapshot: CurrentStateRetentionCoordinatorSnapshot) -> Self {
        let mut state = snapshot.state;
        if snapshot.oracle.contract.planner.rebuild.on_restart != CurrentStateRebuildMode::None
            && state.active_plan.is_some()
        {
            state.active_plan = None;
            state.rebuild_fallback_pending = true;
        }

        Self {
            oracle: CurrentStateRetentionOracle::from_snapshot(snapshot.oracle),
            context: snapshot.context,
            state,
        }
    }

    pub fn snapshot(&self) -> CurrentStateRetentionCoordinatorSnapshot {
        CurrentStateRetentionCoordinatorSnapshot {
            oracle: self.oracle.snapshot(),
            context: self.context.clone(),
            state: self.state.clone(),
        }
    }

    pub fn contract(&self) -> &CurrentStateRetentionContract {
        self.oracle.contract()
    }

    pub fn context(&self) -> &CurrentStateRetentionCoordinationContext {
        &self.context
    }

    pub fn set_context(&mut self, context: CurrentStateRetentionCoordinationContext) {
        self.context = context;
        self.state.active_plan = None;
    }

    pub fn reconfigure(&mut self, configuration: CurrentStateRetentionConfiguration) {
        let rows = self.snapshot().oracle.rows;
        self.reconfigure_with_rows(configuration, rows);
    }

    pub fn reconfigure_with_rows<I>(
        &mut self,
        configuration: CurrentStateRetentionConfiguration,
        rows: I,
    ) where
        I: IntoIterator<Item = CurrentStateOracleRow>,
    {
        let snapshot = self.snapshot();
        let revision_changed = snapshot.oracle.contract.revision != configuration.contract.revision;
        let rebuild_fallback_pending = snapshot.state.rebuild_fallback_pending
            || (revision_changed
                && configuration.contract.planner.rebuild.on_revision
                    != CurrentStateRebuildMode::None);

        self.oracle =
            CurrentStateRetentionOracle::from_snapshot(CurrentStateRetentionOracleSnapshot {
                contract: configuration.contract,
                rows: rows.into_iter().collect(),
                snapshot_pins: snapshot.oracle.snapshot_pins,
                published_ranked_row_keys: snapshot.oracle.published_ranked_row_keys,
            });
        self.context = configuration.context;
        self.state = CurrentStateRetentionCoordinatorState {
            active_plan: None,
            last_observed_retained_row_keys: snapshot.state.last_observed_retained_row_keys,
            rebuild_fallback_pending,
        };
    }

    pub fn set_contract(&mut self, contract: CurrentStateRetentionContract) {
        let revision_changed = self.oracle.contract().revision != contract.revision;
        let rebuild_on_revision = contract.planner.rebuild.on_revision;
        self.oracle.set_contract(contract);
        if revision_changed && rebuild_on_revision != CurrentStateRebuildMode::None {
            self.state.active_plan = None;
            self.state.rebuild_fallback_pending = true;
        }
    }

    pub fn apply(&mut self, mutation: CurrentStateOracleMutation) {
        self.oracle.apply(mutation);
    }

    pub fn set_snapshot_pins<I>(&mut self, row_keys: I)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        self.oracle.set_snapshot_pins(row_keys);
    }

    pub fn clear_snapshot_pins(&mut self) {
        self.oracle.clear_snapshot_pins();
    }

    pub fn publish_retained_set(&mut self) -> Result<(), CurrentStateRetentionError> {
        self.oracle.publish_retained_set()
    }

    pub fn publish_manifest(&mut self) -> bool {
        self.publish_manifest_result().published
    }

    pub fn publish_manifest_result(&mut self) -> CurrentStateManifestPublicationResult {
        let Some(plan) = self.state.active_plan.as_mut() else {
            return CurrentStateManifestPublicationResult::default();
        };
        if !self.context.requires_manifest_publication
            || plan.phase != CurrentStateRetentionPlanPhase::Planned
        {
            return CurrentStateManifestPublicationResult {
                published: false,
                plan: Some(plan.clone()),
            };
        }

        plan.phase = CurrentStateRetentionPlanPhase::ManifestPublished;
        CurrentStateManifestPublicationResult {
            published: true,
            plan: Some(plan.clone()),
        }
    }

    pub fn complete_local_cleanup(&mut self) -> bool {
        self.complete_local_cleanup_result().completed
    }

    pub fn complete_local_cleanup_result(&mut self) -> CurrentStateLocalCleanupResult {
        let Some(plan) = self.state.active_plan.clone() else {
            return CurrentStateLocalCleanupResult::default();
        };
        if self.context.requires_manifest_publication
            && plan.phase != CurrentStateRetentionPlanPhase::ManifestPublished
        {
            return CurrentStateLocalCleanupResult::default();
        }
        if !self.context.requires_local_cleanup || plan.physical_row_keys.is_empty() {
            return CurrentStateLocalCleanupResult {
                completed: false,
                removed_row_keys: Vec::new(),
                completed_plan: Some(plan),
            };
        }

        for row_key in &plan.physical_row_keys {
            self.oracle.apply(CurrentStateOracleMutation::Delete {
                row_key: row_key.clone(),
            });
        }
        self.state.active_plan = None;
        CurrentStateLocalCleanupResult {
            completed: true,
            removed_row_keys: plan.physical_row_keys.clone(),
            completed_plan: Some(plan),
        }
    }

    pub fn plan(&mut self) -> Result<CurrentStateRetentionEvaluation, CurrentStateRetentionError> {
        let mut evaluation = self.oracle.evaluate()?;
        let retained_membership_change_count =
            self.retained_membership_change_count(&evaluation.retained_row_keys);
        let plan = self.build_plan(&mut evaluation);
        let active_plan = self
            .state
            .active_plan
            .as_ref()
            .filter(|existing| {
                plan.as_ref()
                    .is_some_and(|planned| plan_matches(existing, planned))
            })
            .cloned()
            .or(plan);

        self.state.active_plan = active_plan.clone();
        self.state.last_observed_retained_row_keys = Some(evaluation.retained_row_keys.clone());

        let logical_rows_pending = evaluation.non_retained_row_keys.len() as u64;
        let physical_rows_pending = evaluation.reclaimable_row_keys.len() as u64;
        let deferred_physical_rows = evaluation.deferred_row_keys.len() as u64;
        let physical_bytes_pending = evaluation
            .stats
            .reclaimed_bytes
            .saturating_add(evaluation.stats.deferred_bytes);
        let backpressure = self.backpressure_for(
            &evaluation,
            logical_rows_pending,
            physical_rows_pending,
            deferred_physical_rows,
            physical_bytes_pending,
            retained_membership_change_count,
        );

        evaluation.stats.coordination = CurrentStateRetentionCoordinationStats {
            active_plan,
            logical_rows_pending,
            physical_rows_pending,
            deferred_physical_rows,
            physical_bytes_pending,
            evaluation_cost: evaluation.stats.evaluation_cost.clone(),
            retained_membership_change_count,
            rebuild_fallback_pending: self.state.rebuild_fallback_pending,
            backpressure,
        };
        self.state.rebuild_fallback_pending = false;

        Ok(evaluation)
    }

    fn retained_membership_change_count(&self, retained_row_keys: &[Vec<u8>]) -> u64 {
        let Some(previous) = self.state.last_observed_retained_row_keys.as_ref() else {
            return 0;
        };
        let previous = previous.iter().cloned().collect::<BTreeSet<_>>();
        let current = retained_row_keys.iter().cloned().collect::<BTreeSet<_>>();
        previous.symmetric_difference(&current).count() as u64
    }

    fn build_plan(
        &self,
        evaluation: &mut CurrentStateRetentionEvaluation,
    ) -> Option<CurrentStateRetentionCoordinationPlan> {
        if evaluation.non_retained_row_keys.is_empty() {
            return None;
        }
        if !policy_requests_physical_reclaim(self.oracle.contract()) {
            return None;
        }

        match self.context.semantics.physical_reclaim {
            CurrentStatePhysicalReclaimSemantics::LogicalOnly => {
                disable_physical_reclaim(
                    evaluation,
                    CurrentStateEffectiveMode::Skipped,
                    CurrentStateRetentionReason::Skipped {
                        reason: CurrentStateRetentionSkipReason::UnsupportedPhysicalLayout,
                    },
                );
                None
            }
            CurrentStatePhysicalReclaimSemantics::DerivedOnly => {
                disable_physical_reclaim(
                    evaluation,
                    CurrentStateEffectiveMode::DerivedOnly,
                    CurrentStateRetentionReason::DegradedToDerivedOnly {
                        reason: CurrentStateDerivedOnlyReason::PhysicalReclaimNotSupportedByTarget,
                    },
                );
                None
            }
            CurrentStatePhysicalReclaimSemantics::ExactFileSelection
            | CurrentStatePhysicalReclaimSemantics::RewriteCompaction => {
                let logical_action = logical_reclaim_action(self.oracle.contract());
                if self.context.semantics.physical_reclaim
                    == CurrentStatePhysicalReclaimSemantics::RewriteCompaction
                    && logical_action == CurrentStateLogicalReclaimAction::None
                    && !evaluation.reclaimable_row_keys.is_empty()
                {
                    move_reclaimable_rows_to_deferred(evaluation);
                }

                Some(CurrentStateRetentionCoordinationPlan {
                    policy_revision: self.oracle.contract().revision,
                    phase: self
                        .state
                        .active_plan
                        .as_ref()
                        .filter(|plan| {
                            plan_matches(
                                plan,
                                &CurrentStateRetentionCoordinationPlan {
                                    policy_revision: self.oracle.contract().revision,
                                    phase: CurrentStateRetentionPlanPhase::Planned,
                                    semantics: self.context.semantics.clone(),
                                    logical_action,
                                    physical_action: physical_cleanup_action(
                                        self.oracle.contract(),
                                    ),
                                    logical_row_keys: evaluation.non_retained_row_keys.clone(),
                                    physical_row_keys: evaluation.reclaimable_row_keys.clone(),
                                    deferred_row_keys: evaluation.deferred_row_keys.clone(),
                                    physical_bytes_pending: evaluation
                                        .stats
                                        .reclaimed_bytes
                                        .saturating_add(evaluation.stats.deferred_bytes),
                                },
                            )
                        })
                        .map(|plan| plan.phase)
                        .unwrap_or(CurrentStateRetentionPlanPhase::Planned),
                    semantics: self.context.semantics.clone(),
                    logical_action,
                    physical_action: physical_cleanup_action(self.oracle.contract()),
                    logical_row_keys: evaluation.non_retained_row_keys.clone(),
                    physical_row_keys: evaluation.reclaimable_row_keys.clone(),
                    deferred_row_keys: evaluation.deferred_row_keys.clone(),
                    physical_bytes_pending: evaluation
                        .stats
                        .reclaimed_bytes
                        .saturating_add(evaluation.stats.deferred_bytes),
                })
            }
        }
    }

    fn backpressure_for(
        &self,
        evaluation: &CurrentStateRetentionEvaluation,
        _logical_rows_pending: u64,
        _physical_rows_pending: u64,
        deferred_physical_rows: u64,
        _physical_bytes_pending: u64,
        retained_membership_change_count: u64,
    ) -> CurrentStateRetentionBackpressure {
        let mut signals = Vec::new();

        let evaluation_rows = evaluation
            .stats
            .evaluation_cost
            .rows_scanned
            .saturating_add(evaluation.stats.evaluation_cost.rows_ranked)
            .saturating_add(evaluation.stats.evaluation_cost.rows_materialized);
        if evaluation_rows >= self.context.cpu_cost_soft_limit {
            signals.push(CurrentStateRetentionBackpressureSignal::CpuBound);
        }
        if matches!(
            self.oracle.contract().policy,
            CurrentStateRetentionPolicy::GlobalRank(_)
        ) && retained_membership_change_count >= self.context.rank_churn_soft_limit
        {
            signals.push(CurrentStateRetentionBackpressureSignal::RankChurnHeavy);
        }
        if deferred_physical_rows > 0 {
            if evaluation
                .stats
                .status
                .reasons
                .iter()
                .any(|reason| matches!(reason, CurrentStateRetentionReason::BlockedBySnapshots))
            {
                signals.push(CurrentStateRetentionBackpressureSignal::SnapshotPinned);
            }
            if self.context.semantics.physical_reclaim
                == CurrentStatePhysicalReclaimSemantics::RewriteCompaction
            {
                signals.push(CurrentStateRetentionBackpressureSignal::RewriteCompactionBlocked);
            }
        }

        CurrentStateRetentionBackpressure {
            throttle_writes: !signals.is_empty()
                && self.context.max_write_bytes_per_second.is_some(),
            stall_writes: false,
            max_write_bytes_per_second: if signals.is_empty() {
                None
            } else {
                self.context.max_write_bytes_per_second
            },
            signals,
        }
    }
}

fn plan_matches(
    left: &CurrentStateRetentionCoordinationPlan,
    right: &CurrentStateRetentionCoordinationPlan,
) -> bool {
    left.policy_revision == right.policy_revision
        && left.semantics == right.semantics
        && left.logical_action == right.logical_action
        && left.physical_action == right.physical_action
        && left.logical_row_keys == right.logical_row_keys
        && left.physical_row_keys == right.physical_row_keys
        && left.deferred_row_keys == right.deferred_row_keys
        && left.physical_bytes_pending == right.physical_bytes_pending
}

fn policy_requests_physical_reclaim(contract: &CurrentStateRetentionContract) -> bool {
    contract.planner.compaction_row_removal != CurrentStateCompactionRowRemovalMode::Disabled
        || contract.planner.physical_retention.mode != CurrentStatePhysicalRetentionMode::None
}

fn logical_reclaim_action(
    contract: &CurrentStateRetentionContract,
) -> CurrentStateLogicalReclaimAction {
    match contract.planner.compaction_row_removal {
        CurrentStateCompactionRowRemovalMode::Disabled => CurrentStateLogicalReclaimAction::None,
        CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction => {
            CurrentStateLogicalReclaimAction::RewriteCompaction
        }
    }
}

fn physical_cleanup_action(
    contract: &CurrentStateRetentionContract,
) -> CurrentStatePhysicalCleanupAction {
    match contract.planner.physical_retention.mode {
        CurrentStatePhysicalRetentionMode::None => CurrentStatePhysicalCleanupAction::None,
        CurrentStatePhysicalRetentionMode::Delete => CurrentStatePhysicalCleanupAction::Delete,
        CurrentStatePhysicalRetentionMode::Offload => CurrentStatePhysicalCleanupAction::Offload,
    }
}

fn disable_physical_reclaim(
    evaluation: &mut CurrentStateRetentionEvaluation,
    effective_mode: CurrentStateEffectiveMode,
    reason: CurrentStateRetentionReason,
) {
    evaluation.reclaimable_row_keys.clear();
    evaluation.deferred_row_keys.clear();
    evaluation.stats.reclaimed_rows = 0;
    evaluation.stats.reclaimed_bytes = 0;
    evaluation.stats.deferred_rows = 0;
    evaluation.stats.deferred_bytes = 0;
    evaluation.stats.status.effective_mode = effective_mode;
    if !evaluation.stats.status.reasons.contains(&reason) {
        evaluation.stats.status.reasons.push(reason);
    }
}

fn move_reclaimable_rows_to_deferred(evaluation: &mut CurrentStateRetentionEvaluation) {
    if evaluation.reclaimable_row_keys.is_empty() {
        return;
    }

    let mut deferred = evaluation
        .deferred_row_keys
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    deferred.extend(evaluation.reclaimable_row_keys.iter().cloned());
    evaluation.deferred_row_keys = deferred.into_iter().collect();
    evaluation.reclaimable_row_keys.clear();
    evaluation.stats.deferred_rows = evaluation.deferred_row_keys.len() as u64;
    evaluation.stats.deferred_bytes = evaluation
        .stats
        .deferred_bytes
        .saturating_add(evaluation.stats.reclaimed_bytes);
    evaluation.stats.reclaimed_rows = 0;
    evaluation.stats.reclaimed_bytes = 0;

    let reason = CurrentStateRetentionReason::Deferred {
        reason: CurrentStateRetentionDeferredReason::ExactReclaimNotAvailable,
    };
    if !evaluation.stats.status.reasons.contains(&reason) {
        evaluation.stats.status.reasons.push(reason);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CurrentStateRetentionError {
    #[error("row {row_key:?} is missing required {field} for current-state retention ordering")]
    MissingOrderingKey {
        row_key: Vec<u8>,
        field: &'static str,
    },
    #[error(
        "rank-based physical retention requires an explicitly rebuildable source, got {rank_source:?}"
    )]
    RankPhysicalRetentionRequiresRebuildableSource { rank_source: CurrentStateRankSource },
}

fn threshold_retention_disposition(
    policy: &CurrentStateThresholdRetentionPolicy,
    row: &CurrentStateOracleRow,
) -> Result<ThresholdRetentionDisposition, CurrentStateRetentionError> {
    match row.primary_sort_key.as_deref() {
        Some(primary) => Ok(match policy.order.direction {
            CurrentStateSortDirection::Ascending => {
                if primary >= policy.cutoff.encoded_value.as_slice() {
                    ThresholdRetentionDisposition::Retained
                } else {
                    ThresholdRetentionDisposition::NonRetained
                }
            }
            CurrentStateSortDirection::Descending => {
                if primary <= policy.cutoff.encoded_value.as_slice() {
                    ThresholdRetentionDisposition::Retained
                } else {
                    ThresholdRetentionDisposition::NonRetained
                }
            }
        }),
        None => match policy.order.missing_values {
            CurrentStateMissingValuePolicy::FailClosed => {
                Err(CurrentStateRetentionError::MissingOrderingKey {
                    row_key: row.row_key.clone(),
                    field: "primary_sort_key",
                })
            }
            CurrentStateMissingValuePolicy::ExcludeRow => {
                Ok(ThresholdRetentionDisposition::ExcludedFromPolicy)
            }
            CurrentStateMissingValuePolicy::TreatAsLowest => Ok(
                if matches!(
                    policy.order.direction,
                    CurrentStateSortDirection::Descending
                ) {
                    ThresholdRetentionDisposition::Retained
                } else {
                    ThresholdRetentionDisposition::NonRetained
                },
            ),
            CurrentStateMissingValuePolicy::TreatAsHighest => Ok(
                if matches!(policy.order.direction, CurrentStateSortDirection::Ascending) {
                    ThresholdRetentionDisposition::Retained
                } else {
                    ThresholdRetentionDisposition::NonRetained
                },
            ),
        },
    }
}

fn rank_row_participates(
    policy: &CurrentStateRankRetentionPolicy,
    row: &CurrentStateOracleRow,
) -> Result<bool, CurrentStateRetentionError> {
    require_ordering_value(
        row,
        "primary_sort_key",
        row.primary_sort_key.as_deref(),
        policy.order.missing_values,
    )?;
    require_ordering_value(
        row,
        "tie_break_key",
        row.tie_break_key.as_deref(),
        policy.order.missing_values,
    )
}

fn require_ordering_value(
    row: &CurrentStateOracleRow,
    field: &'static str,
    value: Option<&[u8]>,
    missing_values: CurrentStateMissingValuePolicy,
) -> Result<bool, CurrentStateRetentionError> {
    if value.is_some() {
        return Ok(true);
    }

    match missing_values {
        CurrentStateMissingValuePolicy::FailClosed => {
            Err(CurrentStateRetentionError::MissingOrderingKey {
                row_key: row.row_key.clone(),
                field,
            })
        }
        CurrentStateMissingValuePolicy::ExcludeRow => Ok(false),
        CurrentStateMissingValuePolicy::TreatAsLowest
        | CurrentStateMissingValuePolicy::TreatAsHighest => Ok(true),
    }
}

fn compare_rank_rows(
    order: &CurrentStateOrderingContract,
    left: &CurrentStateOracleRow,
    right: &CurrentStateOracleRow,
) -> Ordering {
    compare_optional_primary(
        left.primary_sort_key.as_deref(),
        right.primary_sort_key.as_deref(),
        order,
    )
    .then_with(|| {
        compare_optional_tie_break(
            left.tie_break_key.as_deref(),
            right.tie_break_key.as_deref(),
            order.missing_values,
        )
    })
    .then_with(|| left.row_key.cmp(&right.row_key))
}

fn compare_optional_primary(
    left: Option<&[u8]>,
    right: Option<&[u8]>,
    order: &CurrentStateOrderingContract,
) -> Ordering {
    let natural = compare_optional_natural(left, right, order.missing_values);
    match order.direction {
        CurrentStateSortDirection::Ascending => natural,
        CurrentStateSortDirection::Descending => natural.reverse(),
    }
}

fn compare_optional_tie_break(
    left: Option<&[u8]>,
    right: Option<&[u8]>,
    missing_values: CurrentStateMissingValuePolicy,
) -> Ordering {
    compare_optional_natural(left, right, missing_values)
}

fn compare_optional_natural(
    left: Option<&[u8]>,
    right: Option<&[u8]>,
    missing_values: CurrentStateMissingValuePolicy,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(right),
        (Some(_), None) => compare_missing_against_present(false, missing_values),
        (None, Some(_)) => compare_missing_against_present(true, missing_values),
        (None, None) => Ordering::Equal,
    }
}

fn compare_missing_against_present(
    left_missing: bool,
    missing_values: CurrentStateMissingValuePolicy,
) -> Ordering {
    match missing_values {
        CurrentStateMissingValuePolicy::TreatAsLowest => {
            if left_missing {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        CurrentStateMissingValuePolicy::TreatAsHighest => {
            if left_missing {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        CurrentStateMissingValuePolicy::FailClosed | CurrentStateMissingValuePolicy::ExcludeRow => {
            Ordering::Equal
        }
    }
}

fn append_escaped_ordering_component(encoded: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        encoded.push(*byte);
        if *byte == 0 {
            encoded.push(0xff);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
}

#[cfg(test)]
mod tests {
    use super::{
        CurrentStateCompactionRowRemovalMode, CurrentStateCutoffSource,
        CurrentStateDerivedOnlyReason, CurrentStateEffectiveMode, CurrentStateExactnessRequirement,
        CurrentStateLogicalFloor, CurrentStateMissingValuePolicy, CurrentStateOperationalSemantics,
        CurrentStateOracleMutation, CurrentStateOracleRow, CurrentStatePhysicalReclaimSemantics,
        CurrentStatePhysicalRetentionMode, CurrentStatePhysicalRetentionSeam, CurrentStatePlanner,
        CurrentStateProjectionOwnedRange, CurrentStateRankBoundary, CurrentStateRankSource,
        CurrentStateRankedMaterializationSeam, CurrentStateRebuildMode, CurrentStateRebuildSeam,
        CurrentStateRetentionConfiguration, CurrentStateRetentionContract,
        CurrentStateRetentionCoordinationContext, CurrentStateRetentionDeferredReason,
        CurrentStateRetentionError, CurrentStateRetentionPlanPhase, CurrentStateRetentionReason,
        CurrentStateRetentionSemantics, CurrentStateRetentionSkipReason,
        CurrentStateRetentionTarget, CurrentStateSortDirection, CurrentStateThresholdCutoff,
    };

    fn row(
        name: &str,
        primary: Option<&str>,
        tie_break: Option<&str>,
        bytes: u64,
    ) -> CurrentStateOracleRow {
        CurrentStateOracleRow::new(
            name.as_bytes().to_vec(),
            primary.map(|value| value.as_bytes().to_vec()),
            tie_break.map(|value| value.as_bytes().to_vec()),
            bytes,
        )
    }

    #[test]
    fn rank_ordering_is_stable_across_reruns_and_tie_storms() {
        let contract = CurrentStateRetentionContract::global_rank(
            7,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
            4,
        );
        let mut oracle = super::CurrentStateRetentionOracle::new(contract);
        for row in [
            row("delta", Some("050"), Some("002"), 10),
            row("alpha", Some("050"), Some("001"), 10),
            row("aardvark", Some("050"), Some("001"), 10),
            row("bravo", Some("040"), Some("003"), 10),
        ] {
            oracle.apply(CurrentStateOracleMutation::Upsert(row));
        }

        let first = oracle.evaluate().expect("evaluate first ordering");
        let second = oracle.evaluate().expect("evaluate second ordering");

        assert_eq!(
            first.retained_row_keys,
            vec![
                b"aardvark".to_vec(),
                b"alpha".to_vec(),
                b"delta".to_vec(),
                b"bravo".to_vec(),
            ]
        );
        assert_eq!(first, second);
    }

    #[test]
    fn threshold_oracle_reports_cutoff_and_snapshot_pins() {
        let planner = CurrentStatePlanner {
            compaction_row_removal: CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction,
            ranked_materialization: CurrentStateRankedMaterializationSeam::None,
            physical_retention: CurrentStatePhysicalRetentionSeam {
                mode: CurrentStatePhysicalRetentionMode::Delete,
                ..Default::default()
            },
            rebuild: CurrentStateRebuildSeam {
                on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
                on_revision: CurrentStateRebuildMode::RecomputeFromCurrentState,
            },
        };
        let contract = CurrentStateRetentionContract::threshold(
            3,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending),
            CurrentStateThresholdCutoff::explicit("050"),
        )
        .with_planner(planner);
        let mut oracle = super::CurrentStateRetentionOracle::new(contract);
        for row in [
            row("alpha", Some("010"), Some("010"), 12),
            row("bravo", Some("060"), Some("060"), 20),
            row("charlie", Some("080"), Some("080"), 24),
        ] {
            oracle.apply(CurrentStateOracleMutation::Upsert(row));
        }
        oracle.set_snapshot_pins([b"alpha".to_vec()]);

        let evaluation = oracle.evaluate().expect("evaluate threshold policy");
        assert_eq!(
            evaluation.retained_row_keys,
            vec![b"bravo".to_vec(), b"charlie".to_vec()]
        );
        assert_eq!(evaluation.non_retained_row_keys, vec![b"alpha".to_vec()]);
        assert!(evaluation.reclaimable_row_keys.is_empty());
        assert_eq!(evaluation.deferred_row_keys, vec![b"alpha".to_vec()]);
        assert_eq!(evaluation.stats.retained_set.rows, 2);
        assert_eq!(evaluation.stats.retained_set.bytes, 44);
        assert_eq!(evaluation.stats.deferred_rows, 1);
        assert_eq!(
            evaluation.stats.effective_logical_floor,
            Some(CurrentStateLogicalFloor::Threshold {
                cutoff: CurrentStateThresholdCutoff {
                    source: CurrentStateCutoffSource::Explicit,
                    encoded_value: b"050".to_vec(),
                },
            })
        );
        assert_eq!(
            evaluation.stats.status.reasons,
            vec![CurrentStateRetentionReason::BlockedBySnapshots]
        );
    }

    #[test]
    fn threshold_policy_exclude_row_keeps_missing_keys_out_of_reclaim() {
        let planner = CurrentStatePlanner {
            compaction_row_removal: CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction,
            physical_retention: CurrentStatePhysicalRetentionSeam {
                mode: CurrentStatePhysicalRetentionMode::Delete,
                ..Default::default()
            },
            ..Default::default()
        };
        let contract = CurrentStateRetentionContract::threshold(
            9,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending)
                .with_missing_values(CurrentStateMissingValuePolicy::ExcludeRow),
            CurrentStateThresholdCutoff::explicit("050"),
        )
        .with_planner(planner);
        let mut oracle = super::CurrentStateRetentionOracle::new(contract);
        for row in [
            row("alpha", None, Some("001"), 11),
            row("bravo", Some("040"), Some("002"), 12),
            row("charlie", Some("060"), Some("003"), 13),
        ] {
            oracle.apply(CurrentStateOracleMutation::Upsert(row));
        }

        let evaluation = oracle
            .evaluate()
            .expect("evaluate threshold policy with excluded row");
        assert_eq!(
            evaluation.retained_row_keys,
            vec![b"alpha".to_vec(), b"charlie".to_vec()]
        );
        assert_eq!(evaluation.non_retained_row_keys, vec![b"bravo".to_vec()]);
        assert_eq!(evaluation.reclaimable_row_keys, vec![b"bravo".to_vec()]);
        assert!(evaluation.deferred_row_keys.is_empty());
        assert_eq!(evaluation.stats.retained_set.rows, 2);
        assert_eq!(evaluation.stats.retained_set.bytes, 24);
    }

    #[test]
    fn threshold_policy_defers_reclaim_when_exactness_is_fail_closed() {
        let planner = CurrentStatePlanner {
            physical_retention: CurrentStatePhysicalRetentionSeam {
                mode: CurrentStatePhysicalRetentionMode::Delete,
                exactness: CurrentStateExactnessRequirement::FailClosed,
            },
            ..Default::default()
        };
        let contract = CurrentStateRetentionContract::threshold(
            10,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending),
            CurrentStateThresholdCutoff::explicit("050"),
        )
        .with_planner(planner);
        let mut oracle = super::CurrentStateRetentionOracle::new(contract);
        for row in [
            row("alpha", Some("010"), Some("001"), 14),
            row("bravo", Some("080"), Some("002"), 16),
        ] {
            oracle.apply(CurrentStateOracleMutation::Upsert(row));
        }

        let evaluation = oracle
            .evaluate()
            .expect("evaluate threshold policy with exact reclaim deferral");
        assert_eq!(evaluation.retained_row_keys, vec![b"bravo".to_vec()]);
        assert_eq!(evaluation.non_retained_row_keys, vec![b"alpha".to_vec()]);
        assert!(evaluation.reclaimable_row_keys.is_empty());
        assert_eq!(evaluation.deferred_row_keys, vec![b"alpha".to_vec()]);
        assert_eq!(evaluation.stats.reclaimed_rows, 0);
        assert_eq!(evaluation.stats.deferred_rows, 1);
        assert_eq!(
            evaluation.stats.status.effective_mode,
            CurrentStateEffectiveMode::PhysicalReclaim
        );
        assert_eq!(
            evaluation.stats.status.reasons,
            vec![CurrentStateRetentionReason::Deferred {
                reason: CurrentStateRetentionDeferredReason::ExactReclaimNotAvailable,
            }]
        );
    }

    #[test]
    fn rank_oracle_round_trips_across_restart_and_policy_revision() {
        let contract = CurrentStateRetentionContract::global_rank(
            1,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
            2,
        )
        .with_planner(CurrentStatePlanner {
            ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
                CurrentStateProjectionOwnedRange {
                    output_table: "recent".to_string(),
                    range_start: b"recent:00".to_vec(),
                    range_end: b"recent:ff".to_vec(),
                },
            ),
            rebuild: CurrentStateRebuildSeam {
                on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
                on_revision: CurrentStateRebuildMode::RecomputeDerivedState,
            },
            ..Default::default()
        });
        let mut oracle = super::CurrentStateRetentionOracle::new(contract);
        for row in [
            row("alpha", Some("090"), Some("001"), 8),
            row("bravo", Some("090"), Some("001"), 8),
            row("charlie", Some("080"), Some("001"), 8),
        ] {
            oracle.apply(CurrentStateOracleMutation::Upsert(row));
        }

        let before_restart = oracle.evaluate().expect("evaluate before restart");
        let snapshot = oracle.snapshot();
        let restored = super::CurrentStateRetentionOracle::from_snapshot(snapshot);
        let after_restart = restored.evaluate().expect("evaluate after restart");
        assert_eq!(before_restart, after_restart);
        assert_eq!(
            before_restart.stats.status.effective_mode,
            CurrentStateEffectiveMode::DerivedOnly
        );
        assert_eq!(
            before_restart.stats.status.reasons,
            vec![CurrentStateRetentionReason::DegradedToDerivedOnly {
                reason: CurrentStateDerivedOnlyReason::ProjectionOwnedWithoutPhysicalReclaim,
            }]
        );

        let mut revised = restored;
        revised.set_contract(
            CurrentStateRetentionContract::global_rank(
                2,
                super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
                3,
            )
            .with_planner(revised.contract().planner.clone()),
        );
        let revised_eval = revised.evaluate().expect("evaluate after revision");
        assert_eq!(revised_eval.stats.policy_revision, 2);
        assert_eq!(
            revised_eval.stats.effective_logical_floor,
            Some(CurrentStateLogicalFloor::Rank {
                limit: 3,
                boundary: Some(CurrentStateRankBoundary {
                    primary_sort_key: Some(b"080".to_vec()),
                    tie_break_key: Some(b"001".to_vec()),
                    row_key: b"charlie".to_vec(),
                }),
            })
        );
    }

    #[test]
    fn rank_oracle_tracks_membership_changes_costs_and_publication_across_restart() {
        let contract = CurrentStateRetentionContract::global_rank(
            9,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
            2,
        )
        .with_planner(CurrentStatePlanner {
            ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
                CurrentStateProjectionOwnedRange {
                    output_table: "leaderboard".to_string(),
                    range_start: b"top:00".to_vec(),
                    range_end: b"top:ff".to_vec(),
                },
            ),
            rebuild: CurrentStateRebuildSeam {
                on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
                on_revision: CurrentStateRebuildMode::RecomputeDerivedState,
            },
            ..Default::default()
        });
        let mut oracle = super::CurrentStateRetentionOracle::new(contract);
        for row in [
            row("alpha", Some("090"), Some("001"), 8),
            row("bravo", Some("080"), Some("001"), 8),
            row("charlie", Some("070"), Some("001"), 8),
        ] {
            oracle.apply(CurrentStateOracleMutation::Upsert(row));
        }

        let initial = oracle.evaluate().expect("evaluate initial rank");
        assert_eq!(
            initial.retained_row_keys,
            vec![b"alpha".to_vec(), b"bravo".to_vec()]
        );
        assert_eq!(
            initial.entered_retained_row_keys,
            vec![b"alpha".to_vec(), b"bravo".to_vec()]
        );
        assert!(initial.exited_retained_row_keys.is_empty());
        assert_eq!(initial.stats.membership_changes.entered_rows, 2);
        assert_eq!(initial.stats.membership_changes.exited_rows, 0);
        assert_eq!(initial.stats.evaluation_cost.rows_scanned, 3);
        assert_eq!(initial.stats.evaluation_cost.rows_ranked, 3);
        assert_eq!(initial.stats.evaluation_cost.rows_materialized, 2);

        oracle
            .publish_retained_set()
            .expect("publish retained ranked set");
        let published = oracle.evaluate().expect("evaluate after publish");
        assert!(published.entered_retained_row_keys.is_empty());
        assert!(published.exited_retained_row_keys.is_empty());

        let snapshot = oracle.snapshot();
        let mut restored = super::CurrentStateRetentionOracle::from_snapshot(snapshot);
        restored.apply(CurrentStateOracleMutation::Upsert(row(
            "charlie",
            Some("095"),
            Some("001"),
            8,
        )));

        let after_restart = restored.evaluate().expect("evaluate after restart");
        assert_eq!(
            after_restart.retained_row_keys,
            vec![b"charlie".to_vec(), b"alpha".to_vec()]
        );
        assert_eq!(
            after_restart.entered_retained_row_keys,
            vec![b"charlie".to_vec()]
        );
        assert_eq!(
            after_restart.exited_retained_row_keys,
            vec![b"bravo".to_vec()]
        );
    }

    #[test]
    fn rank_policy_fails_closed_when_destructive_reclaim_lacks_rebuildable_source() {
        let contract = CurrentStateRetentionContract::global_rank(
            12,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
            1,
        )
        .with_planner(CurrentStatePlanner {
            physical_retention: CurrentStatePhysicalRetentionSeam {
                mode: CurrentStatePhysicalRetentionMode::Delete,
                ..Default::default()
            },
            ..Default::default()
        });
        let mut oracle = super::CurrentStateRetentionOracle::new(contract);
        oracle.apply(CurrentStateOracleMutation::Upsert(row(
            "alpha",
            Some("100"),
            Some("001"),
            16,
        )));

        assert_eq!(
            oracle.evaluate(),
            Err(
                CurrentStateRetentionError::RankPhysicalRetentionRequiresRebuildableSource {
                    rank_source: CurrentStateRankSource::current_state(),
                },
            )
        );
    }

    #[test]
    fn rank_policy_allows_destructive_reclaim_for_rebuildable_source() {
        let contract = CurrentStateRetentionContract::global_rank(
            13,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
            1,
        )
        .with_rank_source(CurrentStateRankSource::current_state().with_rebuildable(true))
        .with_planner(CurrentStatePlanner {
            physical_retention: CurrentStatePhysicalRetentionSeam {
                mode: CurrentStatePhysicalRetentionMode::Delete,
                ..Default::default()
            },
            ..Default::default()
        });
        let mut oracle = super::CurrentStateRetentionOracle::new(contract);
        for row in [
            row("alpha", Some("100"), Some("001"), 16),
            row("bravo", Some("050"), Some("001"), 12),
        ] {
            oracle.apply(CurrentStateOracleMutation::Upsert(row));
        }

        let evaluation = oracle
            .evaluate()
            .expect("rebuildable source should allow physical rank retention");
        assert_eq!(
            evaluation.stats.status.effective_mode,
            CurrentStateEffectiveMode::PhysicalReclaim
        );
        assert_eq!(evaluation.reclaimable_row_keys, vec![b"bravo".to_vec()]);
    }

    #[test]
    fn computed_ordering_recipes_cover_leaderboards_recent_items_and_hybrid_rankings() {
        let order = super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending);

        let mut leaderboard = super::CurrentStateRetentionOracle::new(
            CurrentStateRetentionContract::global_rank(20, order.clone(), 2),
        );
        for (name, ordering) in [
            (
                "alpha",
                super::CurrentStateComputedOrderingKeys::leaderboard(10, "alpha"),
            ),
            (
                "bravo",
                super::CurrentStateComputedOrderingKeys::leaderboard(15, "bravo"),
            ),
            (
                "charlie",
                super::CurrentStateComputedOrderingKeys::leaderboard(15, "charlie"),
            ),
        ] {
            leaderboard.apply(CurrentStateOracleMutation::Upsert(
                CurrentStateOracleRow::from_computed_ordering(name, ordering, 10),
            ));
        }
        assert_eq!(
            leaderboard
                .evaluate()
                .expect("evaluate leaderboard")
                .retained_row_keys,
            vec![b"bravo".to_vec(), b"charlie".to_vec()]
        );

        let mut recent = super::CurrentStateRetentionOracle::new(
            CurrentStateRetentionContract::global_rank(21, order.clone(), 2),
        );
        for (name, ordering) in [
            (
                "alpha",
                super::CurrentStateComputedOrderingKeys::recent_item(100, "alpha"),
            ),
            (
                "bravo",
                super::CurrentStateComputedOrderingKeys::recent_item(105, "bravo"),
            ),
            (
                "charlie",
                super::CurrentStateComputedOrderingKeys::recent_item(103, "charlie"),
            ),
        ] {
            recent.apply(CurrentStateOracleMutation::Upsert(
                CurrentStateOracleRow::from_computed_ordering(name, ordering, 10),
            ));
        }
        assert_eq!(
            recent
                .evaluate()
                .expect("evaluate recent")
                .retained_row_keys,
            vec![b"bravo".to_vec(), b"charlie".to_vec()]
        );

        let mut hybrid = super::CurrentStateRetentionOracle::new(
            CurrentStateRetentionContract::global_rank(22, order, 3),
        );
        for (name, ordering) in [
            (
                "alpha",
                super::CurrentStateComputedOrderingKeys::hybrid(20, 100, "alpha"),
            ),
            (
                "bravo",
                super::CurrentStateComputedOrderingKeys::hybrid(20, 110, "bravo"),
            ),
            (
                "charlie",
                super::CurrentStateComputedOrderingKeys::hybrid(18, 999, "charlie"),
            ),
        ] {
            hybrid.apply(CurrentStateOracleMutation::Upsert(
                CurrentStateOracleRow::from_computed_ordering(name, ordering, 10),
            ));
        }
        assert_eq!(
            hybrid
                .evaluate()
                .expect("evaluate hybrid")
                .retained_row_keys,
            vec![b"bravo".to_vec(), b"alpha".to_vec(), b"charlie".to_vec()]
        );
    }

    #[test]
    fn rank_policy_fails_closed_when_missing_ordering_key() {
        let mut oracle =
            super::CurrentStateRetentionOracle::new(CurrentStateRetentionContract::global_rank(
                11,
                super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending)
                    .with_missing_values(CurrentStateMissingValuePolicy::FailClosed),
                1,
            ));
        oracle.apply(CurrentStateOracleMutation::Upsert(row(
            "alpha",
            Some("100"),
            None,
            16,
        )));

        assert_eq!(
            oracle.evaluate(),
            Err(CurrentStateRetentionError::MissingOrderingKey {
                row_key: b"alpha".to_vec(),
                field: "tie_break_key",
            })
        );
    }

    #[test]
    fn threshold_helper_constructors_encode_sortable_values_consistently() {
        let cutoff = CurrentStateThresholdCutoff::explicit_unsigned(50);
        let row = CurrentStateOracleRow::from_threshold_unsigned("alpha", 50, 10);
        assert_eq!(row.primary_sort_key, Some(cutoff.encoded_value.clone()));

        let timestamp_cutoff =
            CurrentStateThresholdCutoff::engine_derived_timestamp_millis("clock", 123);
        assert_eq!(
            timestamp_cutoff.source,
            CurrentStateCutoffSource::EngineDerived {
                source_name: "clock".to_string(),
            }
        );
        let timestamp_row = CurrentStateOracleRow::from_threshold_timestamp_millis("beta", 123, 10);
        assert_eq!(
            timestamp_row.primary_sort_key,
            Some(timestamp_cutoff.encoded_value)
        );
    }

    #[test]
    fn manifest_and_cleanup_results_report_plan_details() {
        let contract = CurrentStateRetentionContract::threshold(
            40,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending),
            CurrentStateThresholdCutoff::explicit_unsigned(50),
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
        });
        let mut coordinator = super::CurrentStateRetentionCoordinator::from_configuration(
            CurrentStateRetentionConfiguration::new(
                contract,
                CurrentStateRetentionCoordinationContext::default(),
            ),
        );
        coordinator.apply(CurrentStateOracleMutation::Upsert(
            CurrentStateOracleRow::from_threshold_unsigned("alpha", 10, 12),
        ));

        let evaluation = coordinator.plan().expect("plan threshold reclaim");
        assert_eq!(evaluation.non_retained_row_keys, vec![b"alpha".to_vec()]);

        let publication = coordinator.publish_manifest_result();
        assert!(publication.published);
        assert_eq!(
            publication.plan.as_ref().map(|plan| plan.phase),
            Some(CurrentStateRetentionPlanPhase::ManifestPublished)
        );

        let cleanup = coordinator.complete_local_cleanup_result();
        assert!(cleanup.completed);
        assert_eq!(cleanup.removed_row_keys, vec![b"alpha".to_vec()]);
        assert_eq!(
            cleanup.completed_plan.as_ref().map(|plan| plan.phase),
            Some(CurrentStateRetentionPlanPhase::ManifestPublished)
        );
    }

    #[test]
    fn operational_summary_captures_derived_logical_and_waiting_modes() {
        let derived_contract = CurrentStateRetentionContract::global_rank(
            41,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
            1,
        )
        .with_planner(CurrentStatePlanner {
            ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
                CurrentStateProjectionOwnedRange {
                    output_table: "derived".to_string(),
                    range_start: b"derived:00".to_vec(),
                    range_end: b"derived:ff".to_vec(),
                },
            ),
            ..Default::default()
        });
        let mut derived = super::CurrentStateRetentionCoordinator::from_configuration(
            CurrentStateRetentionConfiguration::new(
                derived_contract,
                CurrentStateRetentionCoordinationContext {
                    semantics: CurrentStateRetentionSemantics::new(
                        CurrentStateRetentionTarget::ProjectionOwnedOutput,
                        CurrentStatePhysicalReclaimSemantics::DerivedOnly,
                    ),
                    ..Default::default()
                },
            ),
        );
        derived.apply(CurrentStateOracleMutation::Upsert(
            CurrentStateOracleRow::from_computed_ordering(
                "alpha",
                super::CurrentStateComputedOrderingKeys::leaderboard(10, "alpha"),
                10,
            ),
        ));
        assert_eq!(
            derived
                .plan()
                .expect("plan derived retention")
                .operational_summary()
                .semantics,
            CurrentStateOperationalSemantics::DerivedOnly
        );

        let logical_contract = CurrentStateRetentionContract::threshold(
            42,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending),
            CurrentStateThresholdCutoff::explicit_unsigned(50),
        )
        .with_planner(CurrentStatePlanner {
            compaction_row_removal: CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction,
            physical_retention: CurrentStatePhysicalRetentionSeam {
                mode: CurrentStatePhysicalRetentionMode::Delete,
                ..Default::default()
            },
            ..Default::default()
        });
        let mut logical = super::CurrentStateRetentionCoordinator::from_configuration(
            CurrentStateRetentionConfiguration::new(
                logical_contract,
                CurrentStateRetentionCoordinationContext {
                    semantics: CurrentStateRetentionSemantics::new(
                        CurrentStateRetentionTarget::ColumnarTable,
                        CurrentStatePhysicalReclaimSemantics::LogicalOnly,
                    ),
                    ..Default::default()
                },
            ),
        );
        logical.apply(CurrentStateOracleMutation::Upsert(
            CurrentStateOracleRow::from_threshold_unsigned("alpha", 10, 10),
        ));
        assert_eq!(
            logical
                .plan()
                .expect("plan logical-only retention")
                .operational_summary()
                .semantics,
            CurrentStateOperationalSemantics::LogicalOnly
        );

        let waiting_contract = CurrentStateRetentionContract::threshold(
            43,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending),
            CurrentStateThresholdCutoff::explicit_unsigned(50),
        )
        .with_planner(CurrentStatePlanner {
            compaction_row_removal: CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction,
            physical_retention: CurrentStatePhysicalRetentionSeam {
                mode: CurrentStatePhysicalRetentionMode::Delete,
                ..Default::default()
            },
            ..Default::default()
        });
        let mut waiting = super::CurrentStateRetentionCoordinator::from_configuration(
            CurrentStateRetentionConfiguration::new(
                waiting_contract,
                CurrentStateRetentionCoordinationContext::default(),
            ),
        );
        waiting.apply(CurrentStateOracleMutation::Upsert(
            CurrentStateOracleRow::from_threshold_unsigned("alpha", 10, 10),
        ));
        let waiting_summary = waiting
            .plan()
            .expect("plan waiting retention")
            .operational_summary();
        assert_eq!(
            waiting_summary.semantics,
            CurrentStateOperationalSemantics::WaitingOnPhysicalReclaim
        );
        assert_eq!(
            waiting_summary.active_plan_phase,
            Some(CurrentStateRetentionPlanPhase::Planned)
        );
    }

    #[test]
    fn coordinator_reconfigure_with_rows_preserves_publication_state_and_remaps_ordering() {
        let old_contract = CurrentStateRetentionContract::global_rank(
            50,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
            1,
        )
        .with_planner(CurrentStatePlanner {
            ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
                CurrentStateProjectionOwnedRange {
                    output_table: "ranked".to_string(),
                    range_start: b"ranked:00".to_vec(),
                    range_end: b"ranked:ff".to_vec(),
                },
            ),
            rebuild: CurrentStateRebuildSeam {
                on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
                on_revision: CurrentStateRebuildMode::RecomputeFromCurrentState,
            },
            ..Default::default()
        });
        let mut coordinator = super::CurrentStateRetentionCoordinator::from_configuration(
            CurrentStateRetentionConfiguration::new(
                old_contract,
                CurrentStateRetentionCoordinationContext {
                    semantics: CurrentStateRetentionSemantics::new(
                        CurrentStateRetentionTarget::ProjectionOwnedOutput,
                        CurrentStatePhysicalReclaimSemantics::DerivedOnly,
                    ),
                    ..Default::default()
                },
            ),
        );
        for row in [
            CurrentStateOracleRow::from_computed_ordering(
                "alpha",
                super::CurrentStateComputedOrderingKeys::leaderboard(10, "alpha"),
                10,
            ),
            CurrentStateOracleRow::from_computed_ordering(
                "bravo",
                super::CurrentStateComputedOrderingKeys::leaderboard(10, "bravo"),
                10,
            ),
            CurrentStateOracleRow::from_computed_ordering(
                "charlie",
                super::CurrentStateComputedOrderingKeys::leaderboard(9, "charlie"),
                10,
            ),
        ] {
            coordinator.apply(CurrentStateOracleMutation::Upsert(row));
        }
        coordinator
            .publish_retained_set()
            .expect("publish original retained set");

        let new_contract = CurrentStateRetentionContract::global_rank(
            51,
            super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending),
            1,
        )
        .with_planner(CurrentStatePlanner {
            ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
                CurrentStateProjectionOwnedRange {
                    output_table: "ranked".to_string(),
                    range_start: b"ranked:00".to_vec(),
                    range_end: b"ranked:ff".to_vec(),
                },
            ),
            rebuild: CurrentStateRebuildSeam {
                on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
                on_revision: CurrentStateRebuildMode::RecomputeFromCurrentState,
            },
            ..Default::default()
        });
        coordinator.reconfigure_with_rows(
            CurrentStateRetentionConfiguration::new(
                new_contract,
                CurrentStateRetentionCoordinationContext {
                    semantics: CurrentStateRetentionSemantics::new(
                        CurrentStateRetentionTarget::ProjectionOwnedOutput,
                        CurrentStatePhysicalReclaimSemantics::DerivedOnly,
                    ),
                    ..Default::default()
                },
            ),
            [
                CurrentStateOracleRow::from_computed_ordering(
                    "alpha",
                    super::CurrentStateComputedOrderingKeys::hybrid(10, 1, "alpha"),
                    10,
                ),
                CurrentStateOracleRow::from_computed_ordering(
                    "bravo",
                    super::CurrentStateComputedOrderingKeys::hybrid(10, 2, "bravo"),
                    10,
                ),
                CurrentStateOracleRow::from_computed_ordering(
                    "charlie",
                    super::CurrentStateComputedOrderingKeys::hybrid(9, 3, "charlie"),
                    10,
                ),
            ],
        );

        let evaluation = coordinator
            .plan()
            .expect("plan reconfigured ranked retention");
        assert_eq!(evaluation.retained_row_keys, vec![b"bravo".to_vec()]);
        assert_eq!(
            evaluation.entered_retained_row_keys,
            vec![b"bravo".to_vec()]
        );
        assert_eq!(evaluation.exited_retained_row_keys, vec![b"alpha".to_vec()]);
    }

    #[test]
    fn skipped_mode_surfaces_when_no_planner_seam_exists() {
        let mut oracle =
            super::CurrentStateRetentionOracle::new(CurrentStateRetentionContract::threshold(
                5,
                super::CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending),
                CurrentStateThresholdCutoff::explicit("050"),
            ));
        oracle.apply(CurrentStateOracleMutation::Upsert(row(
            "alpha",
            Some("100"),
            Some("001"),
            10,
        )));

        let evaluation = oracle.evaluate().expect("evaluate skipped contract");
        assert_eq!(
            evaluation.stats.status.reasons,
            vec![CurrentStateRetentionReason::Skipped {
                reason: CurrentStateRetentionSkipReason::NoPlannerSeamConfigured,
            }]
        );
        assert_eq!(
            evaluation.stats.status.effective_mode,
            CurrentStateEffectiveMode::Skipped
        );
    }
}
