use std::fmt;

use serde_json::Value as JsonValue;

use crate::{api::Table, ids::SequenceNumber};

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
