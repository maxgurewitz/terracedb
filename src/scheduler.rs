use std::{fmt, sync::Mutex};

use serde_json::Value as JsonValue;

use crate::{api::Table, current_state::CurrentStateRetentionStats, ids::SequenceNumber};

pub const DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT: u32 = 3;
pub const DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT: u32 = 4;

pub trait Scheduler: Send + Sync {
    fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision>;
    fn should_throttle(&self, table: &Table, stats: &TableStats) -> ThrottleDecision;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingWorkType {
    Flush,
    Compaction,
    Backup,
    Offload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingWork {
    pub id: String,
    pub work_type: PendingWorkType,
    pub table: String,
    pub level: Option<u32>,
    pub estimated_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduleAction {
    Execute,
    Defer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleDecision {
    pub work_id: String,
    pub action: ScheduleAction,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThrottleDecision {
    pub throttle: bool,
    pub max_write_bytes_per_second: Option<u64>,
    pub stall: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableStats {
    pub l0_sstable_count: u32,
    pub total_bytes: u64,
    pub local_bytes: u64,
    pub s3_bytes: u64,
    pub compaction_debt: u64,
    pub compaction_filter_removed_bytes: u64,
    pub compaction_filter_removed_keys: u64,
    pub pending_flush_bytes: u64,
    pub immutable_memtable_count: u32,
    pub history_retention_floor_sequence: Option<SequenceNumber>,
    pub history_gc_horizon_sequence: Option<SequenceNumber>,
    pub oldest_active_snapshot_sequence: Option<SequenceNumber>,
    pub active_snapshot_count: u64,
    pub history_pinned_by_snapshots: bool,
    pub change_feed_oldest_available_sequence: Option<SequenceNumber>,
    pub change_feed_floor_sequence: Option<SequenceNumber>,
    pub commit_log_recovery_floor_sequence: SequenceNumber,
    pub commit_log_gc_floor_sequence: SequenceNumber,
    pub change_feed_pins_commit_log_gc: bool,
    pub current_state_retention: Option<CurrentStateRetentionStats>,
    pub metadata: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Clone, Default)]
pub struct NoopScheduler;

impl Scheduler for NoopScheduler {
    fn on_work_available(&self, _work: &[PendingWork]) -> Vec<ScheduleDecision> {
        Vec::new()
    }

    fn should_throttle(&self, _table: &Table, _stats: &TableStats) -> ThrottleDecision {
        ThrottleDecision::default()
    }
}

impl fmt::Debug for NoopScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoopScheduler")
    }
}

#[derive(Default)]
pub struct RoundRobinScheduler {
    state: Mutex<RoundRobinSchedulerState>,
}

#[derive(Default)]
struct RoundRobinSchedulerState {
    last_table: Option<String>,
}

impl Scheduler for RoundRobinScheduler {
    fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
        if work.is_empty() {
            return Vec::new();
        }

        let mut ordered = work.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            pending_work_priority(left)
                .cmp(&pending_work_priority(right))
                .then_with(|| left.table.cmp(&right.table))
                .then_with(|| left.level.cmp(&right.level))
                .then_with(|| left.id.cmp(&right.id))
        });

        let top_priority = pending_work_priority(ordered[0]);
        let top_priority_work = ordered
            .into_iter()
            .filter(|work| pending_work_priority(work) == top_priority)
            .collect::<Vec<_>>();

        let mut tables = top_priority_work
            .iter()
            .map(|work| work.table.as_str())
            .collect::<Vec<_>>();
        tables.sort_unstable();
        tables.dedup();

        let selected_table = {
            let mut state = self
                .state
                .lock()
                .expect("round-robin scheduler mutex should not be poisoned");
            let next_index = state
                .last_table
                .as_deref()
                .and_then(|last| tables.iter().position(|table| *table > last))
                .unwrap_or(0);
            let table = tables[next_index].to_string();
            state.last_table = Some(table.clone());
            table
        };

        let selected_work_id = top_priority_work
            .into_iter()
            .filter(|work| work.table == selected_table)
            .min_by(|left, right| {
                left.level
                    .cmp(&right.level)
                    .then_with(|| left.id.cmp(&right.id))
            })
            .map(|work| work.id.clone())
            .expect("top-priority work should contain at least one item for the selected table");

        work.iter()
            .map(|work| ScheduleDecision {
                work_id: work.id.clone(),
                action: if work.id == selected_work_id {
                    ScheduleAction::Execute
                } else {
                    ScheduleAction::Defer
                },
            })
            .collect()
    }

    fn should_throttle(&self, _table: &Table, stats: &TableStats) -> ThrottleDecision {
        if stats.l0_sstable_count >= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT {
            return ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: None,
                stall: true,
            };
        }

        if stats.l0_sstable_count >= DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT {
            return ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: None,
                stall: false,
            };
        }

        ThrottleDecision::default()
    }
}

impl fmt::Debug for RoundRobinScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RoundRobinScheduler")
    }
}

fn pending_work_priority(work: &PendingWork) -> (u8, u32) {
    match work.work_type {
        PendingWorkType::Flush => (0, 0),
        PendingWorkType::Compaction => (1, work.level.unwrap_or(u32::MAX)),
        PendingWorkType::Backup => (2, 0),
        PendingWorkType::Offload => (3, 0),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use serde_json::json;

    use super::{
        DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT, DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT, PendingWork,
        PendingWorkType, RoundRobinScheduler, ScheduleAction, Scheduler, TableStats,
    };
    use crate::{
        Db, DbConfig, DbDependencies, S3Location, SsdConfig, StorageConfig, StubClock,
        StubFileSystem, StubObjectStore, StubRng, Table, TieredDurabilityMode, TieredStorageConfig,
    };

    fn make_table(name: &str) -> Table {
        futures::executor::block_on(async {
            let config = DbConfig {
                storage: StorageConfig::Tiered(TieredStorageConfig {
                    ssd: SsdConfig {
                        path: "/scheduler-round-robin".to_string(),
                    },
                    s3: S3Location {
                        bucket: "terracedb-test".to_string(),
                        prefix: "scheduler".to_string(),
                    },
                    max_local_bytes: 1024,
                    durability: TieredDurabilityMode::GroupCommit,
                    local_retention: crate::TieredLocalRetentionMode::Offload,
                }),
                hybrid_read: Default::default(),
                scheduler: None,
            };
            let dependencies = DbDependencies::new(
                Arc::new(StubFileSystem::default()),
                Arc::new(StubObjectStore::default()),
                Arc::new(StubClock::default()),
                Arc::new(StubRng::seeded(1)),
            );
            let db = Db::open(config, dependencies).await.expect("open db");
            db.create_table(crate::test_support::row_table_config(name))
                .await
                .expect("create scheduler test table")
        })
    }

    #[test]
    fn round_robin_scheduler_prioritizes_flush_and_rotates_tables() {
        let scheduler = RoundRobinScheduler::default();
        let work = vec![
            PendingWork {
                id: "compaction:events".to_string(),
                work_type: PendingWorkType::Compaction,
                table: "events".to_string(),
                level: Some(0),
                estimated_bytes: 128,
            },
            PendingWork {
                id: "flush:metrics".to_string(),
                work_type: PendingWorkType::Flush,
                table: "metrics".to_string(),
                level: None,
                estimated_bytes: 64,
            },
            PendingWork {
                id: "flush:events".to_string(),
                work_type: PendingWorkType::Flush,
                table: "events".to_string(),
                level: None,
                estimated_bytes: 32,
            },
        ];

        let first = scheduler.on_work_available(&work);
        let first_executed = first
            .iter()
            .find(|decision| decision.action == ScheduleAction::Execute)
            .expect("one work item should be selected");
        assert_eq!(first_executed.work_id, "flush:events");

        let second = scheduler.on_work_available(&work);
        let second_executed = second
            .iter()
            .find(|decision| decision.action == ScheduleAction::Execute)
            .expect("one work item should be selected");
        assert_eq!(second_executed.work_id, "flush:metrics");
    }

    #[test]
    fn round_robin_scheduler_throttles_from_l0_pressure_only() {
        let scheduler = RoundRobinScheduler::default();
        let table = make_table("events");
        let base = TableStats {
            metadata: BTreeMap::from([("priority".to_string(), json!("high"))]),
            ..TableStats::default()
        };

        assert!(!scheduler.should_throttle(&table, &base).throttle);
        assert!(
            scheduler
                .should_throttle(
                    &table,
                    &TableStats {
                        l0_sstable_count: DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT,
                        ..base.clone()
                    },
                )
                .throttle
        );
        assert!(
            scheduler
                .should_throttle(
                    &table,
                    &TableStats {
                        l0_sstable_count: DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT,
                        ..base
                    },
                )
                .stall
        );
    }
}
