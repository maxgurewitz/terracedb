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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionContract {
    pub revision: u64,
    pub policy: CurrentStateRetentionPolicy,
    pub planner: CurrentStatePlanner,
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
            }),
            planner: CurrentStatePlanner::default(),
        }
    }

    pub fn with_planner(mut self, planner: CurrentStatePlanner) -> Self {
        self.planner = planner;
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
    pub reclaimed_rows: u64,
    pub reclaimed_bytes: u64,
    pub deferred_rows: u64,
    pub deferred_bytes: u64,
    pub status: CurrentStateRetentionStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionStatus {
    pub effective_mode: CurrentStateEffectiveMode,
    pub reasons: Vec<CurrentStateRetentionReason>,
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
    Skipped {
        reason: CurrentStateRetentionSkipReason,
    },
    DegradedToDerivedOnly {
        reason: CurrentStateDerivedOnlyReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateRetentionSkipReason {
    NoPlannerSeamConfigured,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentStateDerivedOnlyReason {
    ProjectionOwnedWithoutPhysicalReclaim,
    DerivedMaterializationOnly,
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CurrentStateOracleMutation {
    Upsert(CurrentStateOracleRow),
    Delete { row_key: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionEvaluation {
    pub retained_row_keys: Vec<Vec<u8>>,
    pub non_retained_row_keys: Vec<Vec<u8>>,
    pub reclaimable_row_keys: Vec<Vec<u8>>,
    pub deferred_row_keys: Vec<Vec<u8>>,
    pub stats: CurrentStateRetentionStats,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateRetentionOracleSnapshot {
    pub contract: CurrentStateRetentionContract,
    pub rows: Vec<CurrentStateOracleRow>,
    pub snapshot_pins: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentStateRetentionOracle {
    contract: CurrentStateRetentionContract,
    rows: BTreeMap<Vec<u8>, CurrentStateOracleRow>,
    snapshot_pins: BTreeSet<Vec<u8>>,
}

type OracleEvaluationPartition = (
    Vec<CurrentStateOracleRow>,
    Vec<CurrentStateOracleRow>,
    Option<CurrentStateLogicalFloor>,
);

impl CurrentStateRetentionOracle {
    pub fn new(contract: CurrentStateRetentionContract) -> Self {
        Self {
            contract,
            rows: BTreeMap::new(),
            snapshot_pins: BTreeSet::new(),
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
        }
    }

    pub fn snapshot(&self) -> CurrentStateRetentionOracleSnapshot {
        CurrentStateRetentionOracleSnapshot {
            contract: self.contract.clone(),
            rows: self.rows.values().cloned().collect(),
            snapshot_pins: self.snapshot_pins.iter().cloned().collect(),
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

    pub fn evaluate(&self) -> Result<CurrentStateRetentionEvaluation, CurrentStateRetentionError> {
        let (retained_rows, non_retained_rows, effective_logical_floor) = match &self
            .contract
            .policy
        {
            CurrentStateRetentionPolicy::Threshold(policy) => self.evaluate_threshold(policy)?,
            CurrentStateRetentionPolicy::GlobalRank(policy) => self.evaluate_rank(policy)?,
        };

        let (effective_mode, mut reasons) = self.effective_mode_and_reasons();
        let non_retained_set = non_retained_rows
            .iter()
            .map(|row| row.row_key.clone())
            .collect::<BTreeSet<_>>();

        let (reclaimable_rows, deferred_rows) = match effective_mode {
            CurrentStateEffectiveMode::PhysicalReclaim => {
                let mut reclaimable = Vec::new();
                let mut deferred = Vec::new();
                for row in &non_retained_rows {
                    if self.snapshot_pins.contains(&row.row_key) {
                        deferred.push(row.clone());
                    } else {
                        reclaimable.push(row.clone());
                    }
                }
                if !deferred.is_empty() {
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
            if threshold_matches(policy, row)? {
                retained.push(row.clone());
            } else {
                non_retained.push(row.clone());
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
        ))
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
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CurrentStateRetentionError {
    #[error("row {row_key:?} is missing required {field} for current-state retention ordering")]
    MissingOrderingKey {
        row_key: Vec<u8>,
        field: &'static str,
    },
}

fn threshold_matches(
    policy: &CurrentStateThresholdRetentionPolicy,
    row: &CurrentStateOracleRow,
) -> Result<bool, CurrentStateRetentionError> {
    match row.primary_sort_key.as_deref() {
        Some(primary) => Ok(match policy.order.direction {
            CurrentStateSortDirection::Ascending => {
                primary >= policy.cutoff.encoded_value.as_slice()
            }
            CurrentStateSortDirection::Descending => {
                primary <= policy.cutoff.encoded_value.as_slice()
            }
        }),
        None => match policy.order.missing_values {
            CurrentStateMissingValuePolicy::FailClosed => {
                Err(CurrentStateRetentionError::MissingOrderingKey {
                    row_key: row.row_key.clone(),
                    field: "primary_sort_key",
                })
            }
            CurrentStateMissingValuePolicy::ExcludeRow => Ok(false),
            CurrentStateMissingValuePolicy::TreatAsLowest => Ok(matches!(
                policy.order.direction,
                CurrentStateSortDirection::Descending
            )),
            CurrentStateMissingValuePolicy::TreatAsHighest => Ok(matches!(
                policy.order.direction,
                CurrentStateSortDirection::Ascending
            )),
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

#[cfg(test)]
mod tests {
    use super::{
        CurrentStateCompactionRowRemovalMode, CurrentStateCutoffSource,
        CurrentStateDerivedOnlyReason, CurrentStateEffectiveMode, CurrentStateLogicalFloor,
        CurrentStateMissingValuePolicy, CurrentStateOracleMutation, CurrentStateOracleRow,
        CurrentStatePhysicalRetentionMode, CurrentStatePhysicalRetentionSeam, CurrentStatePlanner,
        CurrentStateProjectionOwnedRange, CurrentStateRankBoundary,
        CurrentStateRankedMaterializationSeam, CurrentStateRebuildMode, CurrentStateRebuildSeam,
        CurrentStateRetentionContract, CurrentStateRetentionError, CurrentStateRetentionReason,
        CurrentStateRetentionSkipReason, CurrentStateSortDirection, CurrentStateThresholdCutoff,
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
