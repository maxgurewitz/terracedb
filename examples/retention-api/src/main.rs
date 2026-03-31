use std::{env, error::Error, path::PathBuf, process};

use serde::Serialize;
use terracedb_example_retention_api::{
    LeaderboardPolicyMode, LeaderboardPolicyRequest, LeaderboardTieBreak, RetentionExampleApp,
    RetentionExampleError, SessionPolicyRequest, ThresholdRetentionLayout,
    UpsertLeaderboardEntryRequest, UpsertSessionRequest,
};

#[derive(Serialize)]
struct ChangeResponse<T> {
    changed: bool,
    state: T,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        print_usage();
        return Ok(());
    }

    let data_dir = parse_data_dir(&mut args);
    if args.is_empty() {
        print_usage();
        return Ok(());
    }

    let command = args.remove(0);
    let mut app = RetentionExampleApp::open(data_dir)?;

    match command.as_str() {
        "inspect" => print_json(&app.inspect()?)?,
        "session-put" => {
            let request = UpsertSessionRequest {
                session_id: take_arg(&mut args, "session_id")?,
                user_id: take_arg(&mut args, "user_id")?,
                last_seen_ms: parse_u64(&take_arg(&mut args, "last_seen_ms")?, "last_seen_ms")?,
                estimated_row_bytes: parse_u64(
                    &take_arg(&mut args, "estimated_row_bytes")?,
                    "estimated_row_bytes",
                )?,
            };
            print_json(&app.upsert_session(request)?)?;
        }
        "session-delete" => {
            let session_id = take_arg(&mut args, "session_id")?;
            let changed = app.delete_session(&session_id)?;
            print_json(&ChangeResponse {
                changed,
                state: app.inspect_sessions()?,
            })?;
        }
        "session-policy" => {
            let request = SessionPolicyRequest {
                revision: parse_u64(&take_arg(&mut args, "revision")?, "revision")?,
                minimum_last_seen_ms: parse_u64(
                    &take_arg(&mut args, "minimum_last_seen_ms")?,
                    "minimum_last_seen_ms",
                )?,
                layout: parse_threshold_layout(&take_arg(&mut args, "layout")?)?,
            };
            print_json(&app.configure_session_policy(request)?)?;
        }
        "session-pin" => {
            if args.is_empty() {
                return Err(Box::new(RetentionExampleError::Usage(
                    "session-pin requires at least one session id".to_string(),
                )));
            }
            app.pin_session_snapshots(args)?;
            print_json(&app.inspect_sessions()?)?;
        }
        "session-clear-pins" => {
            app.clear_session_snapshot_pins()?;
            print_json(&app.inspect_sessions()?)?;
        }
        "session-manifest" => {
            let changed = app.publish_session_manifest()?;
            print_json(&ChangeResponse {
                changed,
                state: app.inspect_sessions()?,
            })?;
        }
        "session-cleanup" => {
            let changed = app.complete_session_cleanup()?;
            print_json(&ChangeResponse {
                changed,
                state: app.inspect_sessions()?,
            })?;
        }
        "session-inspect" => print_json(&app.inspect_sessions()?)?,
        "leaderboard-put" => {
            let request = UpsertLeaderboardEntryRequest {
                player_id: take_arg(&mut args, "player_id")?,
                points: parse_u64(&take_arg(&mut args, "points")?, "points")?,
                created_at_ms: parse_u64(&take_arg(&mut args, "created_at_ms")?, "created_at_ms")?,
                estimated_row_bytes: parse_u64(
                    &take_arg(&mut args, "estimated_row_bytes")?,
                    "estimated_row_bytes",
                )?,
            };
            print_json(&app.upsert_leaderboard_entry(request)?)?;
        }
        "leaderboard-delete" => {
            let player_id = take_arg(&mut args, "player_id")?;
            let changed = app.delete_leaderboard_entry(&player_id)?;
            print_json(&ChangeResponse {
                changed,
                state: app.inspect_leaderboard()?,
            })?;
        }
        "leaderboard-policy" => {
            let request = LeaderboardPolicyRequest {
                revision: parse_u64(&take_arg(&mut args, "revision")?, "revision")?,
                limit: parse_usize(&take_arg(&mut args, "limit")?, "limit")?,
                mode: parse_leaderboard_mode(&take_arg(&mut args, "mode")?)?,
                tie_break: parse_leaderboard_tie_break(&take_arg(&mut args, "tie_break")?)?,
            };
            print_json(&app.configure_leaderboard_policy(request)?)?;
        }
        "leaderboard-publish" => {
            app.publish_leaderboard_retained_set()?;
            print_json(&app.inspect_leaderboard()?)?;
        }
        "leaderboard-manifest" => {
            let changed = app.publish_leaderboard_manifest()?;
            print_json(&ChangeResponse {
                changed,
                state: app.inspect_leaderboard()?,
            })?;
        }
        "leaderboard-cleanup" => {
            let changed = app.complete_leaderboard_cleanup()?;
            print_json(&ChangeResponse {
                changed,
                state: app.inspect_leaderboard()?,
            })?;
        }
        "leaderboard-inspect" => print_json(&app.inspect_leaderboard()?)?,
        "help" | "--help" | "-h" => print_usage(),
        other => {
            eprintln!("unknown command: {other}");
            print_usage();
            process::exit(2);
        }
    }

    Ok(())
}

fn parse_data_dir(args: &mut Vec<String>) -> PathBuf {
    if args.len() >= 2 && args[0] == "--data-dir" {
        let data_dir = args[1].clone();
        args.drain(0..2);
        return PathBuf::from(data_dir);
    }

    PathBuf::from(
        env::var("RETENTION_API_DATA_DIR").unwrap_or_else(|_| ".retention-api-data".to_string()),
    )
}

fn print_json(value: &impl Serialize) -> Result<(), RetentionExampleError> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn take_arg(args: &mut Vec<String>, name: &str) -> Result<String, RetentionExampleError> {
    if args.is_empty() {
        return Err(RetentionExampleError::Usage(format!(
            "missing required argument: {name}"
        )));
    }
    Ok(args.remove(0))
}

fn parse_u64(value: &str, name: &str) -> Result<u64, RetentionExampleError> {
    value.parse().map_err(|_| {
        RetentionExampleError::Usage(format!("{name} must be an unsigned integer, got {value}"))
    })
}

fn parse_usize(value: &str, name: &str) -> Result<usize, RetentionExampleError> {
    value.parse().map_err(|_| {
        RetentionExampleError::Usage(format!("{name} must be an unsigned integer, got {value}"))
    })
}

fn parse_threshold_layout(value: &str) -> Result<ThresholdRetentionLayout, RetentionExampleError> {
    match value {
        "rewrite_compaction_delete" => Ok(ThresholdRetentionLayout::RewriteCompactionDelete),
        "logical_only" => Ok(ThresholdRetentionLayout::LogicalOnly),
        _ => Err(RetentionExampleError::Usage(format!(
            "layout must be rewrite_compaction_delete or logical_only, got {value}"
        ))),
    }
}

fn parse_leaderboard_mode(value: &str) -> Result<LeaderboardPolicyMode, RetentionExampleError> {
    match value {
        "derived_only" => Ok(LeaderboardPolicyMode::DerivedOnly),
        "destructive_rebuildable" => Ok(LeaderboardPolicyMode::DestructiveRebuildable),
        "destructive_unrebuildable" => Ok(LeaderboardPolicyMode::DestructiveUnrebuildable),
        _ => Err(RetentionExampleError::Usage(format!(
            "mode must be derived_only, destructive_rebuildable, or destructive_unrebuildable, got {value}"
        ))),
    }
}

fn parse_leaderboard_tie_break(value: &str) -> Result<LeaderboardTieBreak, RetentionExampleError> {
    match value {
        "stable_id_ascending" => Ok(LeaderboardTieBreak::StableIdAscending),
        "created_at_then_stable_id" => Ok(LeaderboardTieBreak::CreatedAtThenStableId),
        _ => Err(RetentionExampleError::Usage(format!(
            "tie_break must be stable_id_ascending or created_at_then_stable_id, got {value}"
        ))),
    }
}

fn print_usage() {
    println!(
        "\
Usage:
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] inspect
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] session-put <session_id> <user_id> <last_seen_ms> <estimated_row_bytes>
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] session-policy <revision> <minimum_last_seen_ms> <rewrite_compaction_delete|logical_only>
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] session-pin <session_id>...
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] session-manifest
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] session-cleanup
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] leaderboard-put <player_id> <points> <created_at_ms> <estimated_row_bytes>
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] leaderboard-policy <revision> <limit> <derived_only|destructive_rebuildable|destructive_unrebuildable> <stable_id_ascending|created_at_then_stable_id>
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] leaderboard-publish
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] leaderboard-manifest
  cargo run -p terracedb-example-retention-api -- [--data-dir DIR] leaderboard-cleanup
"
    );
}
