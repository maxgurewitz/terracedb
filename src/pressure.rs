use std::{collections::BTreeMap, time::Duration};

use serde_json::Value as JsonValue;

use crate::{
    execution::{ExecutionDomainBudget, ExecutionDomainPath, WorkRuntimeTag},
    ids::SequenceNumber,
    scheduler::PendingWork,
};

/// Byte-oriented pressure counters kept distinct from correctness metadata.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PressureBytes {
    pub mutable_dirty_bytes: u64,
    pub immutable_queued_bytes: u64,
    pub immutable_flushing_bytes: u64,
    pub unified_log_pinned_bytes: u64,
}

impl PressureBytes {
    pub fn total_memory_bytes(&self) -> u64 {
        self.mutable_dirty_bytes
            .saturating_add(self.immutable_queued_bytes)
            .saturating_add(self.immutable_flushing_bytes)
    }

    pub fn total_unified_log_pressure_bytes(&self) -> u64 {
        self.unified_log_pinned_bytes
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PressureBudget {
    pub mutable_soft_limit_bytes: Option<u64>,
    pub mutable_hard_limit_bytes: Option<u64>,
    pub unified_log_soft_limit_bytes: Option<u64>,
    pub unified_log_hard_limit_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PressureScope {
    Process,
    Domain(ExecutionDomainPath),
    Table(String),
}

impl Default for PressureScope {
    fn default() -> Self {
        Self::Process
    }
}

/// Pressure view for one scope plus optional wider totals.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PressureStats {
    pub scope: PressureScope,
    pub local: PressureBytes,
    pub domain_total: Option<PressureBytes>,
    pub process_total: Option<PressureBytes>,
    pub oldest_unflushed_age: Option<Duration>,
    pub budget: PressureBudget,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlushPressureCandidate {
    pub work: PendingWork,
    pub pressure: PressureStats,
    pub estimated_relief: PressureBytes,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdmissionCorrectnessContext {
    pub durable_sequence: SequenceNumber,
    pub visible_sequence: SequenceNumber,
    pub commit_log_recovery_floor_sequence: SequenceNumber,
    pub commit_log_gc_floor_sequence: SequenceNumber,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionSignals {
    pub table: String,
    pub runtime_tag: WorkRuntimeTag,
    pub domain_budget: Option<ExecutionDomainBudget>,
    pub pressure: PressureStats,
    pub batch_write_bytes: u64,
    pub l0_sstable_count: u32,
    pub immutable_memtable_count: u32,
    pub compaction_debt_bytes: u64,
    pub correctness: AdmissionCorrectnessContext,
    pub metadata: BTreeMap<String, JsonValue>,
}
