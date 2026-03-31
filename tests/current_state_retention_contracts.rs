use terracedb::{
    CurrentStateCompactionRowRemovalMode, CurrentStateDerivedOnlyReason,
    CurrentStateExactnessRequirement, CurrentStateMissingValuePolicy, CurrentStateOrderingContract,
    CurrentStatePhysicalRetentionMode, CurrentStatePhysicalRetentionSeam, CurrentStatePlanner,
    CurrentStateProjectionOwnedRange, CurrentStateRankSource,
    CurrentStateRankedMaterializationSeam, CurrentStateRebuildMode, CurrentStateRebuildSeam,
    CurrentStateRetentionContract, CurrentStateRetentionDeferredReason,
    CurrentStateRetentionReason, CurrentStateRetentionSkipReason, CurrentStateSortDirection,
    CurrentStateThresholdCutoff, TableStats,
};

#[test]
fn current_state_retention_contracts_cover_policy_families_and_planner_seams() {
    let threshold = CurrentStateRetentionContract::threshold(
        41,
        CurrentStateOrderingContract::new(CurrentStateSortDirection::Ascending)
            .with_missing_values(CurrentStateMissingValuePolicy::ExcludeRow),
        CurrentStateThresholdCutoff::engine_derived("virtual_clock_window", "050"),
    )
    .with_planner(CurrentStatePlanner {
        compaction_row_removal: CurrentStateCompactionRowRemovalMode::RemoveDuringCompaction,
        ranked_materialization: CurrentStateRankedMaterializationSeam::ProjectionOwned(
            CurrentStateProjectionOwnedRange {
                output_table: "recent_threshold".to_string(),
                range_start: b"threshold:00".to_vec(),
                range_end: b"threshold:ff".to_vec(),
            },
        ),
        physical_retention: CurrentStatePhysicalRetentionSeam {
            mode: CurrentStatePhysicalRetentionMode::Offload,
            exactness: CurrentStateExactnessRequirement::FailClosed,
        },
        rebuild: CurrentStateRebuildSeam {
            on_restart: CurrentStateRebuildMode::RecomputeFromCurrentState,
            on_revision: CurrentStateRebuildMode::RecomputeFromCurrentState,
        },
    });

    let rank = CurrentStateRetentionContract::global_rank(
        42,
        CurrentStateOrderingContract::new(CurrentStateSortDirection::Descending)
            .with_missing_values(CurrentStateMissingValuePolicy::TreatAsLowest),
        10,
    )
    .with_rank_source(
        CurrentStateRankSource::projection_owned_range("rank_input", b"players:00", b"players:ff")
            .with_rebuildable(true),
    )
    .with_planner(CurrentStatePlanner {
        ranked_materialization: CurrentStateRankedMaterializationSeam::DerivedOnly {
            name: "top-ten-derived".to_string(),
        },
        physical_retention: CurrentStatePhysicalRetentionSeam {
            mode: CurrentStatePhysicalRetentionMode::Delete,
            exactness: CurrentStateExactnessRequirement::BestEffort,
        },
        rebuild: CurrentStateRebuildSeam {
            on_restart: CurrentStateRebuildMode::RecomputeDerivedState,
            on_revision: CurrentStateRebuildMode::RecomputeFromCurrentState,
        },
        ..Default::default()
    });

    let skipped_stats = TableStats {
        current_state_retention: Some(terracedb::CurrentStateRetentionStats {
            policy_revision: rank.revision,
            effective_logical_floor: None,
            retained_set: terracedb::CurrentStateRetainedSetSummary { rows: 0, bytes: 0 },
            membership_changes: terracedb::CurrentStateRetentionMembershipChanges::default(),
            evaluation_cost: terracedb::CurrentStateRetentionEvaluationCost::default(),
            reclaimed_rows: 0,
            reclaimed_bytes: 0,
            deferred_rows: 0,
            deferred_bytes: 0,
            status: terracedb::CurrentStateRetentionStatus {
                effective_mode: terracedb::CurrentStateEffectiveMode::Skipped,
                reasons: vec![CurrentStateRetentionReason::Skipped {
                    reason: CurrentStateRetentionSkipReason::NoPlannerSeamConfigured,
                }],
            },
        }),
        ..Default::default()
    };

    assert_eq!(threshold.revision, 41);
    assert_eq!(rank.revision, 42);
    assert!(matches!(
        threshold.planner.ranked_materialization,
        CurrentStateRankedMaterializationSeam::ProjectionOwned(_)
    ));
    assert!(matches!(
        rank.planner.ranked_materialization,
        CurrentStateRankedMaterializationSeam::DerivedOnly { .. }
    ));
    assert!(matches!(
        skipped_stats
            .current_state_retention
            .expect("current-state stats should be present")
            .status
            .reasons
            .as_slice(),
        [CurrentStateRetentionReason::Skipped {
            reason: CurrentStateRetentionSkipReason::NoPlannerSeamConfigured
        }]
    ));
}

#[test]
fn current_state_retention_reason_variants_are_instantiable() {
    let reasons = [
        CurrentStateRetentionReason::BlockedBySnapshots,
        CurrentStateRetentionReason::Deferred {
            reason: CurrentStateRetentionDeferredReason::ExactReclaimNotAvailable,
        },
        CurrentStateRetentionReason::DegradedToDerivedOnly {
            reason: CurrentStateDerivedOnlyReason::ProjectionOwnedWithoutPhysicalReclaim,
        },
        CurrentStateRetentionReason::DegradedToDerivedOnly {
            reason: CurrentStateDerivedOnlyReason::DerivedMaterializationOnly,
        },
        CurrentStateRetentionReason::Skipped {
            reason: CurrentStateRetentionSkipReason::NoPlannerSeamConfigured,
        },
    ];

    assert_eq!(reasons.len(), 5);
}
