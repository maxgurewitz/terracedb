use serde::{Deserialize, Serialize};
use terracedb::{
    CurrentStateCompactionRowRemovalMode, CurrentStateComputedOrderingKeys,
    CurrentStateMissingValuePolicy, CurrentStateOracleRow, CurrentStateOrderingContract,
    CurrentStatePhysicalReclaimSemantics, CurrentStatePhysicalRetentionMode,
    CurrentStatePhysicalRetentionSeam, CurrentStatePlanner, CurrentStateProjectionOwnedRange,
    CurrentStateRankSource, CurrentStateRankedMaterializationSeam, CurrentStateRebuildMode,
    CurrentStateRebuildSeam, CurrentStateRetentionConfiguration, CurrentStateRetentionContract,
    CurrentStateRetentionCoordinationContext, CurrentStateRetentionSemantics,
    CurrentStateRetentionTarget, CurrentStateSortDirection, CurrentStateThresholdCutoff,
};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdRetentionLayout {
    #[default]
    RewriteCompactionDelete,
    LogicalOnly,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RankedPolicyMode {
    #[default]
    DerivedOnly,
    DestructiveRebuildable,
    DestructiveUnrebuildable,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LeaderboardTieBreak {
    #[default]
    StableIdAscending,
    CreatedAtThenStableId,
}

pub fn projection_owned_full_range(output_table: impl AsRef<str>) -> (Vec<u8>, Vec<u8>) {
    let output_table = output_table.as_ref();
    (
        format!("{output_table}:00").into_bytes(),
        format!("{output_table}:ff").into_bytes(),
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThresholdU64RetentionRecipe {
    revision: u64,
    cutoff: u64,
    layout: ThresholdRetentionLayout,
}

impl ThresholdU64RetentionRecipe {
    pub fn new(revision: u64, cutoff: u64) -> Self {
        Self {
            revision,
            cutoff,
            layout: ThresholdRetentionLayout::default(),
        }
    }

    pub fn layout(mut self, layout: ThresholdRetentionLayout) -> Self {
        self.layout = layout;
        self
    }

    pub fn rewrite_compaction_delete(self) -> Self {
        self.layout(ThresholdRetentionLayout::RewriteCompactionDelete)
    }

    pub fn logical_only(self) -> Self {
        self.layout(ThresholdRetentionLayout::LogicalOnly)
    }

    pub fn configuration(&self) -> CurrentStateRetentionConfiguration {
        let contract = CurrentStateRetentionContract::threshold(
            self.revision,
            CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending)
                .with_missing_values(CurrentStateMissingValuePolicy::FailClosed),
            CurrentStateThresholdCutoff::explicit_unsigned(self.cutoff),
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
        let physical_reclaim = match self.layout {
            ThresholdRetentionLayout::RewriteCompactionDelete => {
                CurrentStatePhysicalReclaimSemantics::RewriteCompaction
            }
            ThresholdRetentionLayout::LogicalOnly => {
                CurrentStatePhysicalReclaimSemantics::LogicalOnly
            }
        };
        let context = CurrentStateRetentionCoordinationContext {
            semantics: CurrentStateRetentionSemantics::new(
                CurrentStateRetentionTarget::RowTable,
                physical_reclaim,
            ),
            cpu_cost_soft_limit: 4,
            rank_churn_soft_limit: 2,
            max_write_bytes_per_second: Some(32),
            ..Default::default()
        };

        CurrentStateRetentionConfiguration::new(contract, context)
    }

    pub fn row(
        row_key: impl Into<Vec<u8>>,
        value: u64,
        estimated_row_bytes: u64,
    ) -> CurrentStateOracleRow {
        CurrentStateOracleRow::from_threshold_unsigned(row_key, value, estimated_row_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderboardRetentionRecipe {
    revision: u64,
    limit: usize,
    output_table: String,
    range_start: Vec<u8>,
    range_end: Vec<u8>,
    mode: RankedPolicyMode,
    tie_break: LeaderboardTieBreak,
}

impl LeaderboardRetentionRecipe {
    pub fn new(revision: u64, limit: usize, output_table: impl Into<String>) -> Self {
        let output_table = output_table.into();
        let (range_start, range_end) = projection_owned_full_range(&output_table);
        Self {
            revision,
            limit,
            output_table,
            range_start,
            range_end,
            mode: RankedPolicyMode::default(),
            tie_break: LeaderboardTieBreak::default(),
        }
    }

    pub fn with_output_range(
        mut self,
        range_start: impl Into<Vec<u8>>,
        range_end: impl Into<Vec<u8>>,
    ) -> Self {
        self.range_start = range_start.into();
        self.range_end = range_end.into();
        self
    }

    pub fn mode(mut self, mode: RankedPolicyMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn derived_only(self) -> Self {
        self.mode(RankedPolicyMode::DerivedOnly)
    }

    pub fn destructive_rebuildable(self) -> Self {
        self.mode(RankedPolicyMode::DestructiveRebuildable)
    }

    pub fn destructive_unrebuildable(self) -> Self {
        self.mode(RankedPolicyMode::DestructiveUnrebuildable)
    }

    pub fn tie_break(mut self, tie_break: LeaderboardTieBreak) -> Self {
        self.tie_break = tie_break;
        self
    }

    pub fn configuration(&self) -> CurrentStateRetentionConfiguration {
        let mut contract = CurrentStateRetentionContract::global_rank(
            self.revision,
            CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending)
                .with_missing_values(CurrentStateMissingValuePolicy::TreatAsLowest),
            self.limit,
        )
        .with_rank_source(
            CurrentStateRankSource::current_state().with_rebuildable(matches!(
                self.mode,
                RankedPolicyMode::DestructiveRebuildable
            )),
        )
        .with_planner(CurrentStatePlanner {
            ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
                CurrentStateProjectionOwnedRange {
                    output_table: self.output_table.clone(),
                    range_start: self.range_start.clone(),
                    range_end: self.range_end.clone(),
                },
            ),
            rebuild: CurrentStateRebuildSeam {
                on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
                on_revision: CurrentStateRebuildMode::RecomputeFromCurrentState,
            },
            ..Default::default()
        });

        if matches!(
            self.mode,
            RankedPolicyMode::DestructiveRebuildable | RankedPolicyMode::DestructiveUnrebuildable
        ) {
            contract.planner.compaction_row_removal =
                CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction;
            contract.planner.physical_retention = CurrentStatePhysicalRetentionSeam {
                mode: CurrentStatePhysicalRetentionMode::Delete,
                ..Default::default()
            };
        }

        let (target, physical_reclaim, max_write_bytes_per_second) = match self.mode {
            RankedPolicyMode::DerivedOnly => (
                CurrentStateRetentionTarget::ProjectionOwnedOutput,
                CurrentStatePhysicalReclaimSemantics::DerivedOnly,
                Some(48),
            ),
            RankedPolicyMode::DestructiveRebuildable
            | RankedPolicyMode::DestructiveUnrebuildable => (
                CurrentStateRetentionTarget::RowTable,
                CurrentStatePhysicalReclaimSemantics::RewriteCompaction,
                Some(48),
            ),
        };
        let context = CurrentStateRetentionCoordinationContext {
            semantics: CurrentStateRetentionSemantics::new(target, physical_reclaim),
            cpu_cost_soft_limit: 6,
            rank_churn_soft_limit: 2,
            max_write_bytes_per_second,
            ..Default::default()
        };

        CurrentStateRetentionConfiguration::new(contract, context)
    }

    pub fn row(
        &self,
        stable_id: impl AsRef<[u8]>,
        points: u64,
        created_at_millis: u64,
        estimated_row_bytes: u64,
    ) -> CurrentStateOracleRow {
        Self::row_with_tie_break(
            stable_id,
            points,
            created_at_millis,
            estimated_row_bytes,
            self.tie_break,
        )
    }

    pub fn row_with_tie_break(
        stable_id: impl AsRef<[u8]>,
        points: u64,
        created_at_millis: u64,
        estimated_row_bytes: u64,
        tie_break: LeaderboardTieBreak,
    ) -> CurrentStateOracleRow {
        let stable_id = stable_id.as_ref();
        let ordering = match tie_break {
            LeaderboardTieBreak::StableIdAscending => {
                CurrentStateComputedOrderingKeys::leaderboard(points, stable_id)
            }
            LeaderboardTieBreak::CreatedAtThenStableId => {
                CurrentStateComputedOrderingKeys::hybrid(points, created_at_millis, stable_id)
            }
        };

        CurrentStateOracleRow::from_computed_ordering(
            stable_id.to_vec(),
            ordering,
            estimated_row_bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LeaderboardRetentionRecipe, LeaderboardTieBreak, ThresholdRetentionLayout,
        ThresholdU64RetentionRecipe, projection_owned_full_range,
    };

    #[test]
    fn threshold_recipe_builds_rewrite_and_logical_only_configurations() {
        let rewrite = ThresholdU64RetentionRecipe::new(1, 100)
            .rewrite_compaction_delete()
            .configuration();
        assert_eq!(rewrite.contract.revision, 1);
        assert_eq!(
            rewrite.context.semantics.physical_reclaim,
            terracedb::CurrentStatePhysicalReclaimSemantics::RewriteCompaction
        );

        let logical = ThresholdU64RetentionRecipe::new(2, 200)
            .layout(ThresholdRetentionLayout::LogicalOnly)
            .configuration();
        assert_eq!(
            logical.context.semantics.physical_reclaim,
            terracedb::CurrentStatePhysicalReclaimSemantics::LogicalOnly
        );
        let row = ThresholdU64RetentionRecipe::row("alpha", 200, 10);
        assert_eq!(row.row_key, b"alpha".to_vec());
        assert_eq!(
            row.primary_sort_key,
            Some(logical.contract.policy_threshold_cutoff())
        );
    }

    #[test]
    fn leaderboard_recipe_builds_derived_and_destructive_modes() {
        let recipe = LeaderboardRetentionRecipe::new(7, 2, "leaders")
            .tie_break(LeaderboardTieBreak::CreatedAtThenStableId);
        let derived = recipe.clone().derived_only().configuration();
        assert_eq!(derived.contract.revision, 7);
        assert_eq!(
            derived.context.semantics.target,
            terracedb::CurrentStateRetentionTarget::ProjectionOwnedOutput
        );

        let destructive = recipe.destructive_rebuildable().configuration();
        assert_eq!(
            destructive.context.semantics.target,
            terracedb::CurrentStateRetentionTarget::RowTable
        );
        let row = LeaderboardRetentionRecipe::row_with_tie_break(
            "bravo",
            10,
            2,
            10,
            LeaderboardTieBreak::CreatedAtThenStableId,
        );
        assert_eq!(row.row_key, b"bravo".to_vec());
        assert!(row.tie_break_key.is_some());
    }

    #[test]
    fn projection_range_helper_is_stable() {
        let (start, end) = projection_owned_full_range("leaders");
        assert_eq!(start, b"leaders:00".to_vec());
        assert_eq!(end, b"leaders:ff".to_vec());
    }

    trait ThresholdPolicyInspect {
        fn policy_threshold_cutoff(&self) -> Vec<u8>;
    }

    impl ThresholdPolicyInspect for terracedb::CurrentStateRetentionContract {
        fn policy_threshold_cutoff(&self) -> Vec<u8> {
            match &self.policy {
                terracedb::CurrentStateRetentionPolicy::Threshold(policy) => {
                    policy.cutoff.encoded_value.clone()
                }
                terracedb::CurrentStateRetentionPolicy::GlobalRank(_) => Vec::new(),
            }
        }
    }
}
