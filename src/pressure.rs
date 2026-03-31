use std::{collections::BTreeMap, time::Duration};

use serde_json::{Value as JsonValue, json};

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

    pub(crate) fn saturating_add(self, other: Self) -> Self {
        Self {
            mutable_dirty_bytes: self
                .mutable_dirty_bytes
                .saturating_add(other.mutable_dirty_bytes),
            immutable_queued_bytes: self
                .immutable_queued_bytes
                .saturating_add(other.immutable_queued_bytes),
            immutable_flushing_bytes: self
                .immutable_flushing_bytes
                .saturating_add(other.immutable_flushing_bytes),
            unified_log_pinned_bytes: self
                .unified_log_pinned_bytes
                .saturating_add(other.unified_log_pinned_bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PressureBudget {
    pub mutable_soft_limit_bytes: Option<u64>,
    pub mutable_hard_limit_bytes: Option<u64>,
    pub unified_log_soft_limit_bytes: Option<u64>,
    pub unified_log_hard_limit_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum PressureScope {
    #[default]
    Process,
    Domain(ExecutionDomainPath),
    Table(String),
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

pub(crate) const FLUSH_SCORE_METADATA_KEY: &str = "flush_score";
pub(crate) const FLUSH_SELECTION_REASON_METADATA_KEY: &str = "selection_reason";
pub(crate) const FLUSH_FORCE_REASON_METADATA_KEY: &str = "force_reason";
pub(crate) const FLUSH_SOFT_GUARDRAIL_METADATA_KEY: &str = "soft_guardrail";
pub(crate) const FLUSH_BUDGET_PRESSURE_BPS_METADATA_KEY: &str = "budget_pressure_bps";
pub(crate) const FLUSH_OLDEST_AGE_MILLIS_METADATA_KEY: &str = "oldest_unflushed_age_ms";
pub(crate) const FLUSH_L0_SSTABLE_COUNT_METADATA_KEY: &str = "l0_sstable_count";
pub(crate) const FLUSH_COMPACTION_DEBT_BYTES_METADATA_KEY: &str = "compaction_debt_bytes";

const PRESSURE_FLUSH_SOFT_LIMIT_NUMERATOR: u64 = 3;
const PRESSURE_FLUSH_SOFT_LIMIT_DENOMINATOR: u64 = 4;
const PRESSURE_FLUSH_FORCE_LIMIT_NUMERATOR: u64 = 7;
const PRESSURE_FLUSH_FORCE_LIMIT_DENOMINATOR: u64 = 8;
pub(crate) const PRESSURE_FLUSH_SOFT_AGE_TRIGGER: Duration = Duration::from_secs(2);
pub(crate) const PRESSURE_FLUSH_FORCE_AGE_CEILING: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FlushPressureGuardrail {
    pub soft_triggered: bool,
    pub force_triggered: bool,
    pub reason: Option<&'static str>,
}

pub(crate) fn derived_soft_limit_bytes(hard_limit: Option<u64>) -> Option<u64> {
    hard_limit.map(|limit| {
        limit.saturating_mul(PRESSURE_FLUSH_SOFT_LIMIT_NUMERATOR)
            / PRESSURE_FLUSH_SOFT_LIMIT_DENOMINATOR
    })
}

pub(crate) fn derived_force_limit_bytes(hard_limit: Option<u64>) -> Option<u64> {
    hard_limit.map(|limit| {
        limit.saturating_mul(PRESSURE_FLUSH_FORCE_LIMIT_NUMERATOR)
            / PRESSURE_FLUSH_FORCE_LIMIT_DENOMINATOR
    })
}

pub(crate) fn pressure_ratio_bps(bytes: u64, limit: Option<u64>) -> u64 {
    let Some(limit) = limit.filter(|limit| *limit > 0) else {
        return 0;
    };
    bytes.saturating_mul(10_000).div_ceil(limit)
}

pub(crate) fn evaluate_flush_guardrail(
    pressure: &PressureStats,
    projected_extra: PressureBytes,
) -> FlushPressureGuardrail {
    let projected_local = pressure.local.saturating_add(projected_extra);
    let projected_domain = pressure
        .domain_total
        .unwrap_or(pressure.local)
        .saturating_add(projected_extra);
    let projected_process = pressure
        .process_total
        .unwrap_or(projected_domain)
        .saturating_add(projected_extra);

    let projected_mutable_bytes = projected_local
        .mutable_dirty_bytes
        .max(projected_domain.mutable_dirty_bytes)
        .max(projected_process.mutable_dirty_bytes);
    let projected_unified_log_bytes = projected_local
        .unified_log_pinned_bytes
        .max(projected_domain.unified_log_pinned_bytes)
        .max(projected_process.unified_log_pinned_bytes);

    let mutable_soft_limit = pressure
        .budget
        .mutable_soft_limit_bytes
        .or_else(|| derived_soft_limit_bytes(pressure.budget.mutable_hard_limit_bytes));
    let mutable_force_limit =
        derived_force_limit_bytes(pressure.budget.mutable_hard_limit_bytes).or(mutable_soft_limit);
    let unified_hard_limit = pressure
        .budget
        .unified_log_hard_limit_bytes
        .or(pressure.budget.mutable_hard_limit_bytes);
    let unified_soft_limit = pressure
        .budget
        .unified_log_soft_limit_bytes
        .or_else(|| derived_soft_limit_bytes(unified_hard_limit));
    let unified_force_limit = derived_force_limit_bytes(unified_hard_limit).or(unified_soft_limit);

    let age = pressure.oldest_unflushed_age.unwrap_or_default();

    if projected_unified_log_bytes >= unified_force_limit.unwrap_or(u64::MAX) {
        return FlushPressureGuardrail {
            soft_triggered: true,
            force_triggered: true,
            reason: Some("unified-log pressure"),
        };
    }
    if projected_mutable_bytes >= mutable_force_limit.unwrap_or(u64::MAX) {
        return FlushPressureGuardrail {
            soft_triggered: true,
            force_triggered: true,
            reason: Some("dirty-byte pressure"),
        };
    }
    if age >= PRESSURE_FLUSH_FORCE_AGE_CEILING {
        return FlushPressureGuardrail {
            soft_triggered: true,
            force_triggered: true,
            reason: Some("oldest-unflushed age"),
        };
    }

    if projected_unified_log_bytes >= unified_soft_limit.unwrap_or(u64::MAX) {
        return FlushPressureGuardrail {
            soft_triggered: true,
            force_triggered: false,
            reason: Some("unified-log pressure"),
        };
    }
    if projected_mutable_bytes >= mutable_soft_limit.unwrap_or(u64::MAX) {
        return FlushPressureGuardrail {
            soft_triggered: true,
            force_triggered: false,
            reason: Some("dirty-byte pressure"),
        };
    }
    if age >= PRESSURE_FLUSH_SOFT_AGE_TRIGGER {
        return FlushPressureGuardrail {
            soft_triggered: true,
            force_triggered: false,
            reason: Some("oldest-unflushed age"),
        };
    }

    FlushPressureGuardrail::default()
}

pub(crate) fn flush_budget_pressure_bps(pressure: &PressureStats) -> u64 {
    let memory_limit = pressure.budget.mutable_hard_limit_bytes;
    let unified_limit = pressure
        .budget
        .unified_log_hard_limit_bytes
        .or(pressure.budget.mutable_hard_limit_bytes);

    [
        pressure_ratio_bps(pressure.local.total_memory_bytes(), memory_limit),
        pressure_ratio_bps(
            pressure
                .domain_total
                .unwrap_or(pressure.local)
                .total_memory_bytes(),
            memory_limit,
        ),
        pressure_ratio_bps(
            pressure
                .process_total
                .unwrap_or(pressure.domain_total.unwrap_or(pressure.local))
                .total_memory_bytes(),
            memory_limit,
        ),
        pressure_ratio_bps(
            pressure.local.total_unified_log_pressure_bytes(),
            unified_limit,
        ),
        pressure_ratio_bps(
            pressure
                .domain_total
                .unwrap_or(pressure.local)
                .total_unified_log_pressure_bytes(),
            unified_limit,
        ),
        pressure_ratio_bps(
            pressure
                .process_total
                .unwrap_or(pressure.domain_total.unwrap_or(pressure.local))
                .total_unified_log_pressure_bytes(),
            unified_limit,
        ),
    ]
    .into_iter()
    .max()
    .unwrap_or_default()
}

pub(crate) fn flush_pressure_score(
    pressure: &PressureStats,
    estimated_relief: PressureBytes,
    l0_sstable_count: u32,
    compaction_debt_bytes: u64,
) -> u64 {
    let budget_pressure_bps = flush_budget_pressure_bps(pressure);
    let age_millis = pressure
        .oldest_unflushed_age
        .map(|age| age.as_millis() as u64)
        .unwrap_or_default()
        .min(60_000);
    let guardrail = evaluate_flush_guardrail(pressure, PressureBytes::default());
    let guardrail_bonus: u64 = if guardrail.force_triggered {
        2_000_000
    } else if guardrail.soft_triggered {
        1_000_000
    } else {
        0
    };

    guardrail_bonus
        .saturating_add(budget_pressure_bps.saturating_mul(128))
        .saturating_add(estimated_relief.unified_log_pinned_bytes.saturating_mul(4))
        .saturating_add(estimated_relief.mutable_dirty_bytes.saturating_mul(3))
        .saturating_add(estimated_relief.immutable_queued_bytes.saturating_mul(2))
        .saturating_add(age_millis.saturating_mul(32))
        .saturating_add(u64::from(l0_sstable_count).saturating_mul(4_096))
        .saturating_add(compaction_debt_bytes.min(4 * 1024 * 1024) / 256)
}

pub(crate) fn flush_selection_reason(
    pressure: &PressureStats,
    estimated_relief: PressureBytes,
    l0_sstable_count: u32,
    compaction_debt_bytes: u64,
) -> String {
    let mut reasons = Vec::new();
    let guardrail = evaluate_flush_guardrail(pressure, PressureBytes::default());

    if let Some(reason) = guardrail.reason {
        reasons.push(format!("guardrail triggered by {reason}"));
    }
    if estimated_relief.unified_log_pinned_bytes > 0 {
        reasons.push("reclaims unified-log headroom".to_string());
    }
    if estimated_relief.mutable_dirty_bytes > 0 {
        reasons.push("drains mutable dirty bytes".to_string());
    }
    if pressure.oldest_unflushed_age >= Some(PRESSURE_FLUSH_SOFT_AGE_TRIGGER) {
        reasons.push("oldest unflushed data is aging".to_string());
    }
    if flush_budget_pressure_bps(pressure) >= 7_500 {
        reasons.push("budget pressure is near the configured ceiling".to_string());
    }
    if l0_sstable_count > 0 || compaction_debt_bytes > 0 {
        reasons.push("table already carries compaction pressure".to_string());
    }

    if reasons.is_empty() {
        "conservative flush fallback".to_string()
    } else {
        reasons.join(", ")
    }
}

pub(crate) fn build_flush_candidate_metadata(
    pressure: &PressureStats,
    estimated_relief: PressureBytes,
    l0_sstable_count: u32,
    compaction_debt_bytes: u64,
) -> BTreeMap<String, JsonValue> {
    let guardrail = evaluate_flush_guardrail(pressure, PressureBytes::default());
    let score = flush_pressure_score(
        pressure,
        estimated_relief,
        l0_sstable_count,
        compaction_debt_bytes,
    );
    let mut metadata = BTreeMap::from([
        (FLUSH_SCORE_METADATA_KEY.to_string(), json!(score)),
        (
            FLUSH_SELECTION_REASON_METADATA_KEY.to_string(),
            json!(flush_selection_reason(
                pressure,
                estimated_relief,
                l0_sstable_count,
                compaction_debt_bytes,
            )),
        ),
        (
            FLUSH_BUDGET_PRESSURE_BPS_METADATA_KEY.to_string(),
            json!(flush_budget_pressure_bps(pressure)),
        ),
        (
            FLUSH_OLDEST_AGE_MILLIS_METADATA_KEY.to_string(),
            json!(
                pressure
                    .oldest_unflushed_age
                    .map(|age| age.as_millis() as u64)
                    .unwrap_or_default()
            ),
        ),
        (
            FLUSH_L0_SSTABLE_COUNT_METADATA_KEY.to_string(),
            json!(l0_sstable_count),
        ),
        (
            FLUSH_COMPACTION_DEBT_BYTES_METADATA_KEY.to_string(),
            json!(compaction_debt_bytes),
        ),
    ]);
    if guardrail.soft_triggered {
        metadata.insert(
            FLUSH_SOFT_GUARDRAIL_METADATA_KEY.to_string(),
            JsonValue::Bool(true),
        );
    }
    if let Some(reason) = guardrail.reason.filter(|_| guardrail.force_triggered) {
        metadata.insert(FLUSH_FORCE_REASON_METADATA_KEY.to_string(), json!(reason));
    }
    metadata
}

pub(crate) fn flush_candidate_priority_key(candidate: &FlushPressureCandidate) -> (u8, u64, u64) {
    let forced = candidate
        .metadata
        .get(FLUSH_FORCE_REASON_METADATA_KEY)
        .and_then(JsonValue::as_str)
        .is_some() as u8;
    let score = candidate
        .metadata
        .get(FLUSH_SCORE_METADATA_KEY)
        .and_then(JsonValue::as_u64)
        .unwrap_or_else(|| {
            candidate
                .estimated_relief
                .unified_log_pinned_bytes
                .saturating_add(candidate.estimated_relief.mutable_dirty_bytes)
                .saturating_add(candidate.work.estimated_bytes)
        });
    (
        forced,
        score,
        candidate
            .estimated_relief
            .unified_log_pinned_bytes
            .saturating_add(candidate.estimated_relief.mutable_dirty_bytes),
    )
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
