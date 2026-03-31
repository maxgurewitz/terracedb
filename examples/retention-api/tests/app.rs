use tempfile::tempdir;

use terracedb::{
    CurrentStateEffectiveMode, CurrentStateRetentionError, CurrentStateRetentionReason,
    CurrentStateRetentionSkipReason,
};
use terracedb_example_retention_api::{
    ExampleOperationalSemantics, LeaderboardPolicyMode, LeaderboardPolicyRequest,
    LeaderboardTieBreak, RetentionExampleApp, SessionPolicyRequest, ThresholdRetentionLayout,
    UpsertLeaderboardEntryRequest, UpsertSessionRequest,
};

fn session(
    id: &str,
    user: &str,
    last_seen_ms: u64,
    estimated_row_bytes: u64,
) -> UpsertSessionRequest {
    UpsertSessionRequest {
        session_id: id.to_string(),
        user_id: user.to_string(),
        last_seen_ms,
        estimated_row_bytes,
    }
}

fn player(
    player_id: &str,
    points: u64,
    created_at_ms: u64,
    estimated_row_bytes: u64,
) -> UpsertLeaderboardEntryRequest {
    UpsertLeaderboardEntryRequest {
        player_id: player_id.to_string(),
        points,
        created_at_ms,
        estimated_row_bytes,
    }
}

#[test]
fn reopen_preserves_policy_configuration_retained_outputs_and_introspection() {
    let dir = tempdir().expect("create temp dir");
    let mut app = RetentionExampleApp::open(dir.path()).expect("open app");

    app.configure_session_policy(SessionPolicyRequest {
        revision: 2,
        minimum_last_seen_ms: 100,
        layout: ThresholdRetentionLayout::RewriteCompactionDelete,
    })
    .expect("configure session policy");
    app.upsert_session(session("alpha", "user-a", 90, 12))
        .expect("insert alpha");
    app.upsert_session(session("bravo", "user-b", 120, 14))
        .expect("insert bravo");
    app.upsert_session(session("charlie", "user-c", 180, 16))
        .expect("insert charlie");
    app.pin_session_snapshots(["alpha".to_string()])
        .expect("pin alpha");

    app.configure_leaderboard_policy(LeaderboardPolicyRequest {
        revision: 2,
        limit: 2,
        mode: LeaderboardPolicyMode::DerivedOnly,
        tie_break: LeaderboardTieBreak::StableIdAscending,
    })
    .expect("configure leaderboard policy");
    app.upsert_leaderboard_entry(player("alpha", 10, 1, 10))
        .expect("insert player alpha");
    app.upsert_leaderboard_entry(player("bravo", 10, 2, 10))
        .expect("insert player bravo");
    app.upsert_leaderboard_entry(player("charlie", 9, 3, 10))
        .expect("insert player charlie");
    app.publish_leaderboard_retained_set()
        .expect("publish leaderboard");

    let before = app.inspect().expect("inspect before reopen");
    assert_eq!(before.sessions.policy.revision, 2);
    assert_eq!(before.sessions.policy.minimum_last_seen_ms, 100);
    assert_eq!(
        before
            .sessions
            .retained_sessions
            .iter()
            .map(|record| record.session_id.as_str())
            .collect::<Vec<_>>(),
        vec!["bravo", "charlie"]
    );
    assert_eq!(before.sessions.deferred_session_ids, vec!["alpha"]);
    assert_eq!(
        before
            .leaderboard
            .derived_top_players
            .iter()
            .map(|record| record.player_id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "bravo"]
    );
    assert_eq!(before.leaderboard.policy.limit, 2);

    let mut reopened = RetentionExampleApp::open(dir.path()).expect("reopen app");
    let after = reopened.inspect().expect("inspect after reopen");

    assert_eq!(after.sessions.policy, before.sessions.policy);
    assert_eq!(
        after.sessions.retained_sessions,
        before.sessions.retained_sessions
    );
    assert_eq!(
        after.sessions.deferred_session_ids,
        before.sessions.deferred_session_ids
    );
    assert_eq!(after.leaderboard.policy, before.leaderboard.policy);
    assert_eq!(
        after.leaderboard.derived_top_players,
        before.leaderboard.derived_top_players
    );
    assert_eq!(after.history_retention_note, before.history_retention_note);
}

#[test]
fn session_logical_only_layout_reports_explicit_semantics() {
    let dir = tempdir().expect("create temp dir");
    let mut app = RetentionExampleApp::open(dir.path()).expect("open app");

    app.upsert_session(session("alpha", "user-a", 90, 12))
        .expect("insert alpha");
    app.upsert_session(session("bravo", "user-b", 120, 14))
        .expect("insert bravo");

    let inspection = app
        .configure_session_policy(SessionPolicyRequest {
            revision: 2,
            minimum_last_seen_ms: 100,
            layout: ThresholdRetentionLayout::LogicalOnly,
        })
        .expect("configure logical-only session policy");

    assert_eq!(
        inspection.operational.semantics,
        ExampleOperationalSemantics::LogicalOnly
    );
    assert_eq!(
        inspection.operational.effective_mode,
        CurrentStateEffectiveMode::Skipped
    );
    assert!(inspection.reclaimable_session_ids.is_empty());
    assert!(inspection.deferred_session_ids.is_empty());
    assert!(
        inspection
            .operational
            .reasons
            .contains(&CurrentStateRetentionReason::Skipped {
                reason: CurrentStateRetentionSkipReason::UnsupportedPhysicalLayout,
            })
    );
}

#[test]
fn leaderboard_policy_toggle_fails_closed_for_unrebuildable_destructive_mode() {
    let dir = tempdir().expect("create temp dir");
    let mut app = RetentionExampleApp::open(dir.path()).expect("open app");

    app.upsert_leaderboard_entry(player("alpha", 10, 1, 10))
        .expect("insert player");

    let error = app
        .configure_leaderboard_policy(LeaderboardPolicyRequest {
            revision: 2,
            limit: 1,
            mode: LeaderboardPolicyMode::DestructiveUnrebuildable,
            tie_break: LeaderboardTieBreak::StableIdAscending,
        })
        .expect_err("unrebuildable destructive mode should fail");

    assert!(matches!(
        error,
        terracedb_example_retention_api::RetentionExampleError::Retention(
            CurrentStateRetentionError::RankPhysicalRetentionRequiresRebuildableSource { .. }
        )
    ));
}

#[test]
fn leaderboard_inspection_keeps_derived_only_and_destructive_modes_explicit() {
    let dir = tempdir().expect("create temp dir");
    let mut app = RetentionExampleApp::open(dir.path()).expect("open app");

    for request in [
        player("alpha", 10, 1, 10),
        player("bravo", 10, 2, 10),
        player("charlie", 9, 3, 10),
    ] {
        app.upsert_leaderboard_entry(request)
            .expect("insert leaderboard row");
    }

    let derived = app
        .configure_leaderboard_policy(LeaderboardPolicyRequest {
            revision: 2,
            limit: 2,
            mode: LeaderboardPolicyMode::DerivedOnly,
            tie_break: LeaderboardTieBreak::StableIdAscending,
        })
        .expect("configure derived-only leaderboard");

    assert_eq!(derived.policy.mode, LeaderboardPolicyMode::DerivedOnly);
    assert_eq!(
        derived.operational.semantics,
        ExampleOperationalSemantics::DerivedOnly
    );
    assert_eq!(
        derived.operational.effective_mode,
        CurrentStateEffectiveMode::DerivedOnly
    );
    assert_eq!(derived.source_rows.len(), 3);
    assert_eq!(
        derived
            .outside_limit_rows
            .iter()
            .map(|row| row.player_id.as_str())
            .collect::<Vec<_>>(),
        vec!["charlie"]
    );

    let destructive = app
        .configure_leaderboard_policy(LeaderboardPolicyRequest {
            revision: 3,
            limit: 2,
            mode: LeaderboardPolicyMode::DestructiveRebuildable,
            tie_break: LeaderboardTieBreak::StableIdAscending,
        })
        .expect("configure destructive leaderboard");

    assert_eq!(
        destructive.policy.mode,
        LeaderboardPolicyMode::DestructiveRebuildable
    );
    assert_eq!(
        destructive.operational.semantics,
        ExampleOperationalSemantics::WaitingOnPhysicalReclaim
    );
    assert_eq!(
        destructive.operational.effective_mode,
        CurrentStateEffectiveMode::PhysicalReclaim
    );
    assert!(destructive.operational.waiting_for_manifest_publication);
}
