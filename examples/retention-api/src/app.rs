use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use terracedb::{
    CurrentStateLogicalFloor, CurrentStateOracleMutation, CurrentStateRankedMaterializationSeam,
    CurrentStateRetentionCoordinator, CurrentStateRetentionCoordinatorSnapshot,
    CurrentStateRetentionError, CurrentStateRetentionEvaluation,
};

use crate::{
    ExampleInspection, HISTORY_RETENTION_NOTE, LeaderboardBoundaryView, LeaderboardEntry,
    LeaderboardInspection, LeaderboardPolicyRequest, LeaderboardPolicyView, PublicationChanges,
    SessionInspection, SessionPolicyRequest, SessionPolicyView, SessionRecord,
    UpsertLeaderboardEntryRequest, UpsertSessionRequest, leaderboard_configuration,
    leaderboard_row, session_configuration, session_row,
};

const SNAPSHOT_FILE_NAME: &str = "retention-example-state.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExampleSnapshot {
    version: u32,
    session_policy: SessionPolicyRequest,
    sessions: BTreeMap<String, SessionRecord>,
    session_coordinator: CurrentStateRetentionCoordinatorSnapshot,
    leaderboard_policy: LeaderboardPolicyRequest,
    leaderboard_entries: BTreeMap<String, LeaderboardEntry>,
    leaderboard_coordinator: CurrentStateRetentionCoordinatorSnapshot,
}

#[derive(Debug, Error)]
pub enum RetentionExampleError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Retention(#[from] CurrentStateRetentionError),
    #[error("row key {0:?} is not valid utf-8")]
    InvalidRowKey(Vec<u8>),
    #[error("row {row_id} is missing from example state")]
    MissingRow { row_id: String },
    #[error("{0}")]
    Usage(String),
}

pub struct RetentionExampleApp {
    snapshot_path: PathBuf,
    session_policy: SessionPolicyRequest,
    sessions: BTreeMap<String, SessionRecord>,
    session_coordinator: CurrentStateRetentionCoordinator,
    leaderboard_policy: LeaderboardPolicyRequest,
    leaderboard_entries: BTreeMap<String, LeaderboardEntry>,
    leaderboard_coordinator: CurrentStateRetentionCoordinator,
}

impl RetentionExampleApp {
    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self, RetentionExampleError> {
        let root_dir = root_dir.as_ref();
        fs::create_dir_all(root_dir)?;
        let snapshot_path = root_dir.join(SNAPSHOT_FILE_NAME);
        if snapshot_path.exists() {
            return Self::open_existing(snapshot_path);
        }

        let session_policy = SessionPolicyRequest::default();
        let leaderboard_policy = LeaderboardPolicyRequest::default();
        let mut app = Self {
            snapshot_path,
            session_policy: session_policy.clone(),
            sessions: BTreeMap::new(),
            session_coordinator: CurrentStateRetentionCoordinator::from_configuration(
                session_configuration(&session_policy),
            ),
            leaderboard_policy: leaderboard_policy.clone(),
            leaderboard_entries: BTreeMap::new(),
            leaderboard_coordinator: CurrentStateRetentionCoordinator::from_configuration(
                leaderboard_configuration(&leaderboard_policy),
            ),
        };
        app.save()?;
        Ok(app)
    }

    pub fn configure_session_policy(
        &mut self,
        policy: SessionPolicyRequest,
    ) -> Result<SessionInspection, RetentionExampleError> {
        self.session_policy = policy.clone();
        self.session_coordinator
            .reconfigure(session_configuration(&policy));
        self.inspect_sessions()
    }

    pub fn upsert_session(
        &mut self,
        request: UpsertSessionRequest,
    ) -> Result<SessionRecord, RetentionExampleError> {
        let record: SessionRecord = request.into();
        self.session_coordinator
            .apply(CurrentStateOracleMutation::Upsert(session_row(&record)));
        self.sessions
            .insert(record.session_id.clone(), record.clone());
        self.save()?;
        Ok(record)
    }

    pub fn delete_session(&mut self, session_id: &str) -> Result<bool, RetentionExampleError> {
        let removed = self.sessions.remove(session_id).is_some();
        self.session_coordinator
            .apply(CurrentStateOracleMutation::Delete {
                row_key: session_id.as_bytes().to_vec(),
            });
        self.save()?;
        Ok(removed)
    }

    pub fn pin_session_snapshots<I>(&mut self, session_ids: I) -> Result<(), RetentionExampleError>
    where
        I: IntoIterator<Item = String>,
    {
        self.session_coordinator
            .set_snapshot_pins(session_ids.into_iter().map(|id| id.into_bytes()));
        self.save()?;
        Ok(())
    }

    pub fn clear_session_snapshot_pins(&mut self) -> Result<(), RetentionExampleError> {
        self.session_coordinator.clear_snapshot_pins();
        self.save()?;
        Ok(())
    }

    pub fn publish_session_manifest(&mut self) -> Result<bool, RetentionExampleError> {
        let changed = self.session_coordinator.publish_manifest_result().published;
        self.save()?;
        Ok(changed)
    }

    pub fn complete_session_cleanup(&mut self) -> Result<bool, RetentionExampleError> {
        let cleanup = self.session_coordinator.complete_local_cleanup_result();
        if cleanup.completed {
            self.remove_sessions(cleanup.removed_row_keys)?;
        }
        self.save()?;
        Ok(cleanup.completed)
    }

    pub fn inspect_sessions(&mut self) -> Result<SessionInspection, RetentionExampleError> {
        let evaluation = self.session_coordinator.plan()?;
        let snapshot = self.session_coordinator.snapshot();
        let retained_sessions = self.lookup_sessions(&evaluation.retained_row_keys)?;
        let non_retained_sessions = self.lookup_sessions(&evaluation.non_retained_row_keys)?;
        let snapshot_pins = decode_row_keys(&snapshot.oracle.snapshot_pins)?;
        let inspection = SessionInspection {
            policy: SessionPolicyView {
                revision: self.session_policy.revision,
                minimum_last_seen_ms: self.session_policy.minimum_last_seen_ms,
                layout: self.session_policy.layout,
            },
            retained_sessions,
            non_retained_sessions,
            reclaimable_session_ids: decode_row_keys(&evaluation.reclaimable_row_keys)?,
            deferred_session_ids: decode_row_keys(&evaluation.deferred_row_keys)?,
            snapshot_pins,
            operational: evaluation.operational_summary(),
        };
        self.save()?;
        Ok(inspection)
    }

    pub fn configure_leaderboard_policy(
        &mut self,
        policy: LeaderboardPolicyRequest,
    ) -> Result<LeaderboardInspection, RetentionExampleError> {
        self.leaderboard_policy = policy.clone();
        self.leaderboard_coordinator.reconfigure_with_rows(
            leaderboard_configuration(&policy),
            self.leaderboard_entries
                .values()
                .map(|entry| leaderboard_row(entry, policy.tie_break)),
        );
        self.inspect_leaderboard()
    }

    pub fn upsert_leaderboard_entry(
        &mut self,
        request: UpsertLeaderboardEntryRequest,
    ) -> Result<LeaderboardEntry, RetentionExampleError> {
        let entry: LeaderboardEntry = request.into();
        self.leaderboard_coordinator
            .apply(CurrentStateOracleMutation::Upsert(leaderboard_row(
                &entry,
                self.leaderboard_policy.tie_break,
            )));
        self.leaderboard_entries
            .insert(entry.player_id.clone(), entry.clone());
        self.save()?;
        Ok(entry)
    }

    pub fn delete_leaderboard_entry(
        &mut self,
        player_id: &str,
    ) -> Result<bool, RetentionExampleError> {
        let removed = self.leaderboard_entries.remove(player_id).is_some();
        self.leaderboard_coordinator
            .apply(CurrentStateOracleMutation::Delete {
                row_key: player_id.as_bytes().to_vec(),
            });
        self.save()?;
        Ok(removed)
    }

    pub fn publish_leaderboard_retained_set(&mut self) -> Result<(), RetentionExampleError> {
        self.leaderboard_coordinator.publish_retained_set()?;
        self.save()?;
        Ok(())
    }

    pub fn publish_leaderboard_manifest(&mut self) -> Result<bool, RetentionExampleError> {
        let changed = self
            .leaderboard_coordinator
            .publish_manifest_result()
            .published;
        self.save()?;
        Ok(changed)
    }

    pub fn complete_leaderboard_cleanup(&mut self) -> Result<bool, RetentionExampleError> {
        let cleanup = self.leaderboard_coordinator.complete_local_cleanup_result();
        if cleanup.completed {
            self.remove_leaderboard_rows(cleanup.removed_row_keys)?;
        }
        self.save()?;
        Ok(cleanup.completed)
    }

    pub fn inspect_leaderboard(&mut self) -> Result<LeaderboardInspection, RetentionExampleError> {
        let evaluation = self.leaderboard_coordinator.plan()?;
        let retained = self.lookup_leaderboard(&evaluation.retained_row_keys)?;
        let outside_limit = self.lookup_leaderboard(&evaluation.non_retained_row_keys)?;
        let source_rows = self.leaderboard_entries.values().cloned().collect();
        let boundary = leaderboard_boundary(&self.leaderboard_entries, &evaluation);
        let inspection = LeaderboardInspection {
            policy: LeaderboardPolicyView {
                revision: self.leaderboard_policy.revision,
                limit: self.leaderboard_policy.limit,
                mode: self.leaderboard_policy.mode,
                tie_break: self.leaderboard_policy.tie_break,
                output_table: leaderboard_output_table(self.leaderboard_coordinator.contract()),
                boundary,
            },
            source_rows,
            derived_top_players: retained,
            outside_limit_rows: outside_limit,
            publication_changes: PublicationChanges {
                entered_ids: decode_row_keys(&evaluation.entered_retained_row_keys)?,
                exited_ids: decode_row_keys(&evaluation.exited_retained_row_keys)?,
            },
            operational: evaluation.operational_summary(),
        };
        self.save()?;
        Ok(inspection)
    }

    pub fn inspect(&mut self) -> Result<ExampleInspection, RetentionExampleError> {
        Ok(ExampleInspection {
            history_retention_note: HISTORY_RETENTION_NOTE.to_string(),
            sessions: self.inspect_sessions()?,
            leaderboard: self.inspect_leaderboard()?,
        })
    }

    fn open_existing(snapshot_path: PathBuf) -> Result<Self, RetentionExampleError> {
        let bytes = fs::read(&snapshot_path)?;
        let snapshot: ExampleSnapshot = serde_json::from_slice(&bytes)?;
        Ok(Self {
            snapshot_path,
            session_policy: snapshot.session_policy,
            sessions: snapshot.sessions,
            session_coordinator: CurrentStateRetentionCoordinator::from_snapshot(
                snapshot.session_coordinator,
            ),
            leaderboard_policy: snapshot.leaderboard_policy,
            leaderboard_entries: snapshot.leaderboard_entries,
            leaderboard_coordinator: CurrentStateRetentionCoordinator::from_snapshot(
                snapshot.leaderboard_coordinator,
            ),
        })
    }

    fn save(&mut self) -> Result<(), RetentionExampleError> {
        let snapshot = ExampleSnapshot {
            version: 1,
            session_policy: self.session_policy.clone(),
            sessions: self.sessions.clone(),
            session_coordinator: self.session_coordinator.snapshot(),
            leaderboard_policy: self.leaderboard_policy.clone(),
            leaderboard_entries: self.leaderboard_entries.clone(),
            leaderboard_coordinator: self.leaderboard_coordinator.snapshot(),
        };
        let bytes = serde_json::to_vec_pretty(&snapshot)?;
        fs::write(&self.snapshot_path, bytes)?;
        Ok(())
    }

    fn lookup_sessions(
        &self,
        row_keys: &[Vec<u8>],
    ) -> Result<Vec<SessionRecord>, RetentionExampleError> {
        row_keys
            .iter()
            .map(|row_key| {
                let row_id = decode_row_key(row_key)?;
                self.sessions
                    .get(&row_id)
                    .cloned()
                    .ok_or(RetentionExampleError::MissingRow { row_id })
            })
            .collect()
    }

    fn lookup_leaderboard(
        &self,
        row_keys: &[Vec<u8>],
    ) -> Result<Vec<LeaderboardEntry>, RetentionExampleError> {
        row_keys
            .iter()
            .map(|row_key| {
                let row_id = decode_row_key(row_key)?;
                self.leaderboard_entries
                    .get(&row_id)
                    .cloned()
                    .ok_or(RetentionExampleError::MissingRow { row_id })
            })
            .collect()
    }

    fn remove_sessions(&mut self, row_keys: Vec<Vec<u8>>) -> Result<(), RetentionExampleError> {
        for row_key in row_keys {
            let session_id = decode_row_key(&row_key)?;
            self.sessions.remove(&session_id);
        }
        Ok(())
    }

    fn remove_leaderboard_rows(
        &mut self,
        row_keys: Vec<Vec<u8>>,
    ) -> Result<(), RetentionExampleError> {
        for row_key in row_keys {
            let player_id = decode_row_key(&row_key)?;
            self.leaderboard_entries.remove(&player_id);
        }
        Ok(())
    }
}

fn decode_row_key(row_key: &[u8]) -> Result<String, RetentionExampleError> {
    String::from_utf8(row_key.to_vec())
        .map_err(|_| RetentionExampleError::InvalidRowKey(row_key.to_vec()))
}

fn decode_row_keys(row_keys: &[Vec<u8>]) -> Result<Vec<String>, RetentionExampleError> {
    row_keys
        .iter()
        .map(|row_key| decode_row_key(row_key))
        .collect()
}

fn leaderboard_output_table(contract: &terracedb::CurrentStateRetentionContract) -> String {
    match &contract.planner.ranked_materialization {
        CurrentStateRankedMaterializationSeam::ProjectionOwned(range) => range.output_table.clone(),
        CurrentStateRankedMaterializationSeam::DerivedOnly { name } => name.clone(),
        CurrentStateRankedMaterializationSeam::None => String::new(),
    }
}

fn leaderboard_boundary(
    entries: &BTreeMap<String, LeaderboardEntry>,
    evaluation: &CurrentStateRetentionEvaluation,
) -> Option<LeaderboardBoundaryView> {
    let CurrentStateLogicalFloor::Rank {
        boundary: Some(boundary),
        ..
    } = evaluation.stats.effective_logical_floor.as_ref()?
    else {
        return None;
    };

    let player_id = String::from_utf8(boundary.row_key.clone()).ok()?;
    let entry = entries.get(&player_id)?;
    Some(LeaderboardBoundaryView {
        player_id,
        points: entry.points,
        created_at_ms: entry.created_at_ms,
    })
}
