use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdmissionPolicyProfile {
    #[default]
    Conservative,
    LatencySensitive,
    ThroughputOriented,
}

impl AdmissionPolicyProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conservative => "conservative",
            Self::LatencySensitive => "latency-sensitive",
            Self::ThroughputOriented => "throughput-oriented",
        }
    }

    pub fn from_signal_metadata(metadata: &BTreeMap<String, JsonValue>) -> Self {
        [
            "terracedb.pressure.write_policy",
            "terracedb.pressure.policy",
            "terracedb.execution.role",
        ]
        .into_iter()
        .find_map(|key| metadata.get(key).and_then(json_string))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
    }

    fn mutable_soft_ratio_basis_points(self) -> u64 {
        match self {
            Self::Conservative => 7_000,
            Self::LatencySensitive => 5_500,
            Self::ThroughputOriented => 8_500,
        }
    }

    fn unified_soft_ratio_basis_points(self) -> u64 {
        match self {
            Self::Conservative => 8_000,
            Self::LatencySensitive => 6_500,
            Self::ThroughputOriented => 9_000,
        }
    }

    fn backlog_soft_divisor(self) -> u64 {
        match self {
            Self::Conservative => 2,
            Self::LatencySensitive => 4,
            Self::ThroughputOriented => 1,
        }
    }

    fn backlog_hard_multiplier(self) -> u64 {
        match self {
            Self::Conservative => 2,
            Self::LatencySensitive => 3,
            Self::ThroughputOriented => 2,
        }
    }

    fn immutable_memtable_soft_limit(self) -> u32 {
        match self {
            Self::Conservative => 2,
            Self::LatencySensitive => 1,
            Self::ThroughputOriented => 3,
        }
    }

    fn immutable_memtable_hard_limit(self) -> u32 {
        match self {
            Self::Conservative => 4,
            Self::LatencySensitive => 3,
            Self::ThroughputOriented => 6,
        }
    }

    fn oldest_unflushed_soft_limit(self) -> Duration {
        match self {
            Self::Conservative => Duration::from_secs(15),
            Self::LatencySensitive => Duration::from_secs(5),
            Self::ThroughputOriented => Duration::from_secs(45),
        }
    }

    fn oldest_unflushed_hard_limit(self) -> Duration {
        match self {
            Self::Conservative => Duration::from_secs(60),
            Self::LatencySensitive => Duration::from_secs(20),
            Self::ThroughputOriented => Duration::from_secs(180),
        }
    }

    fn compaction_soft_multiplier(self) -> u64 {
        match self {
            Self::Conservative => 1,
            Self::LatencySensitive => 1,
            Self::ThroughputOriented => 2,
        }
    }

    fn compaction_hard_multiplier(self) -> u64 {
        match self {
            Self::Conservative => 2,
            Self::LatencySensitive => 2,
            Self::ThroughputOriented => 2,
        }
    }

    fn base_rate_limit_divisor(self) -> u64 {
        match self {
            Self::Conservative => 4,
            Self::LatencySensitive => 8,
            Self::ThroughputOriented => 2,
        }
    }
}

impl std::str::FromStr for AdmissionPolicyProfile {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "conservative" | "default" | "balanced" => Ok(Self::Conservative),
            "latency" | "latency-sensitive" | "oltp" | "primary" => Ok(Self::LatencySensitive),
            "throughput" | "throughput-oriented" | "olap" | "olap-ingest" | "analytics-helper" => {
                Ok(Self::ThroughputOriented)
            }
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdmissionPressureSignal {
    MutableBudget,
    UnifiedLogBudget,
    FlushBacklog,
    OldestUnflushedAge,
    ImmutableMemtableBacklog,
    L0Sstables,
    CompactionDebt,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdmissionPressureLevel {
    #[default]
    Open,
    RateLimit,
    Stall,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdmissionDiagnostics {
    pub level: AdmissionPressureLevel,
    pub policy: AdmissionPolicyProfile,
    pub triggered_by: Vec<AdmissionPressureSignal>,
    pub projected_pressure: PressureBytes,
    pub required_relief: PressureBytes,
    pub max_write_bytes_per_second: Option<u64>,
    pub metadata: BTreeMap<String, JsonValue>,
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

pub fn derive_pressure_budget(
    policy: AdmissionPolicyProfile,
    mutable_hard_limit_bytes: Option<u64>,
    background_in_flight_limit_bytes: Option<u64>,
) -> PressureBudget {
    let mutable_soft_limit_bytes = mutable_hard_limit_bytes
        .map(|hard| scale_bytes(hard, policy.mutable_soft_ratio_basis_points()));
    let unified_log_hard_limit_bytes = mutable_hard_limit_bytes.map(|hard| {
        let background_headroom = background_in_flight_limit_bytes
            .unwrap_or_else(|| hard.saturating_div(2))
            .max(hard.saturating_div(2))
            .max(1);
        hard.saturating_add(background_headroom)
    });
    let unified_log_soft_limit_bytes = unified_log_hard_limit_bytes
        .map(|hard| scale_bytes(hard, policy.unified_soft_ratio_basis_points()));
    PressureBudget {
        mutable_soft_limit_bytes,
        mutable_hard_limit_bytes,
        unified_log_soft_limit_bytes,
        unified_log_hard_limit_bytes,
    }
}

pub fn multi_signal_write_admission(signals: &AdmissionSignals) -> AdmissionDiagnostics {
    let policy = AdmissionPolicyProfile::from_signal_metadata(&signals.metadata);
    let effective = signals
        .pressure
        .domain_total
        .unwrap_or(signals.pressure.local);
    let reference_bytes = effective
        .total_memory_bytes()
        .max(effective.total_unified_log_pressure_bytes())
        .max(signals.batch_write_bytes)
        .max(
            signals
                .pressure
                .budget
                .mutable_hard_limit_bytes
                .unwrap_or_default(),
        )
        .max(
            signals
                .pressure
                .budget
                .unified_log_hard_limit_bytes
                .unwrap_or_default(),
        )
        .max(
            signals
                .domain_budget
                .and_then(|budget| budget.background.max_in_flight_bytes)
                .unwrap_or_default(),
        )
        .max(1);
    let derived_budget = derive_pressure_budget(
        policy,
        signals
            .pressure
            .budget
            .mutable_hard_limit_bytes
            .or(Some(reference_bytes)),
        signals
            .domain_budget
            .and_then(|budget| budget.background.max_in_flight_bytes),
    );
    let mutable_soft = signals
        .pressure
        .budget
        .mutable_soft_limit_bytes
        .or(derived_budget.mutable_soft_limit_bytes);
    let mutable_hard = signals
        .pressure
        .budget
        .mutable_hard_limit_bytes
        .or(derived_budget.mutable_hard_limit_bytes);
    let unified_soft = signals
        .pressure
        .budget
        .unified_log_soft_limit_bytes
        .or(derived_budget.unified_log_soft_limit_bytes);
    let unified_hard = signals
        .pressure
        .budget
        .unified_log_hard_limit_bytes
        .or(derived_budget.unified_log_hard_limit_bytes);

    let backlog_soft = signals
        .domain_budget
        .and_then(|budget| budget.background.max_in_flight_bytes)
        .unwrap_or_else(|| reference_bytes.saturating_div(policy.backlog_soft_divisor()))
        .max(signals.batch_write_bytes.max(1));
    let backlog_hard = backlog_soft
        .saturating_mul(policy.backlog_hard_multiplier())
        .max(backlog_soft);
    let immutable_soft = policy.immutable_memtable_soft_limit();
    let immutable_hard = policy.immutable_memtable_hard_limit();
    let age_soft = policy.oldest_unflushed_soft_limit();
    let age_hard = policy.oldest_unflushed_hard_limit();
    let compaction_soft = reference_bytes
        .max(backlog_soft)
        .saturating_mul(policy.compaction_soft_multiplier())
        .max(signals.batch_write_bytes.max(1));
    let compaction_hard = compaction_soft
        .saturating_mul(policy.compaction_hard_multiplier())
        .max(compaction_soft);

    let projected_pressure = PressureBytes {
        mutable_dirty_bytes: effective
            .mutable_dirty_bytes
            .saturating_add(signals.batch_write_bytes),
        immutable_queued_bytes: effective.immutable_queued_bytes,
        immutable_flushing_bytes: effective.immutable_flushing_bytes,
        unified_log_pinned_bytes: effective
            .unified_log_pinned_bytes
            .saturating_add(signals.batch_write_bytes),
    };
    let backlog_bytes = projected_pressure
        .immutable_queued_bytes
        .saturating_add(projected_pressure.immutable_flushing_bytes);

    let mut score = PressureSignalScore::default();

    score.note(
        projected_pressure.mutable_dirty_bytes,
        mutable_soft,
        mutable_hard,
        AdmissionPressureSignal::MutableBudget,
        2,
    );
    score.note(
        projected_pressure.unified_log_pinned_bytes,
        unified_soft,
        unified_hard,
        AdmissionPressureSignal::UnifiedLogBudget,
        1,
    );
    score.note(
        backlog_bytes,
        Some(backlog_soft),
        Some(backlog_hard),
        AdmissionPressureSignal::FlushBacklog,
        1,
    );
    score.note_u32(
        signals.immutable_memtable_count,
        Some(immutable_soft),
        Some(immutable_hard),
        AdmissionPressureSignal::ImmutableMemtableBacklog,
        1,
    );
    score.note_u32(
        signals.l0_sstable_count,
        Some(crate::scheduler::DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT),
        Some(crate::scheduler::DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT),
        AdmissionPressureSignal::L0Sstables,
        2,
    );
    score.note(
        signals.compaction_debt_bytes,
        Some(compaction_soft),
        Some(compaction_hard),
        AdmissionPressureSignal::CompactionDebt,
        1,
    );
    if let Some(age) = signals.pressure.oldest_unflushed_age {
        score.note_duration(
            age,
            Some(age_soft),
            Some(age_hard),
            AdmissionPressureSignal::OldestUnflushedAge,
            1,
        );
    }

    let level = if !score.hard_signals.is_empty() || score.soft_score >= 4 {
        AdmissionPressureLevel::Stall
    } else if score.soft_score > 0 {
        AdmissionPressureLevel::RateLimit
    } else {
        AdmissionPressureLevel::Open
    };

    let triggered_by = score
        .hard_signals
        .iter()
        .chain(score.soft_signals.iter())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let required_relief = required_relief(
        projected_pressure,
        mutable_soft.or(mutable_hard),
        unified_soft.or(unified_hard),
        backlog_soft,
    );
    let max_write_bytes_per_second = (level == AdmissionPressureLevel::RateLimit).then(|| {
        let divisor = policy
            .base_rate_limit_divisor()
            .saturating_add(u64::from(score.soft_score.saturating_sub(1)));
        reference_bytes
            .saturating_div(divisor.max(1))
            .max(signals.batch_write_bytes.max(1))
    });

    let mut metadata = BTreeMap::new();
    metadata.insert("policy".to_string(), json!(policy.as_str()));
    metadata.insert("soft_score".to_string(), json!(score.soft_score));
    metadata.insert("mutable_soft_limit_bytes".to_string(), json!(mutable_soft));
    metadata.insert("mutable_hard_limit_bytes".to_string(), json!(mutable_hard));
    metadata.insert(
        "unified_log_soft_limit_bytes".to_string(),
        json!(unified_soft),
    );
    metadata.insert(
        "unified_log_hard_limit_bytes".to_string(),
        json!(unified_hard),
    );
    metadata.insert(
        "flush_backlog_soft_limit_bytes".to_string(),
        json!(backlog_soft),
    );
    metadata.insert(
        "flush_backlog_hard_limit_bytes".to_string(),
        json!(backlog_hard),
    );
    metadata.insert(
        "oldest_unflushed_soft_limit_millis".to_string(),
        json!(age_soft.as_millis() as u64),
    );
    metadata.insert(
        "oldest_unflushed_hard_limit_millis".to_string(),
        json!(age_hard.as_millis() as u64),
    );
    metadata.insert(
        "compaction_debt_soft_limit_bytes".to_string(),
        json!(compaction_soft),
    );
    metadata.insert(
        "compaction_debt_hard_limit_bytes".to_string(),
        json!(compaction_hard),
    );

    AdmissionDiagnostics {
        level,
        policy,
        triggered_by,
        projected_pressure,
        required_relief,
        max_write_bytes_per_second,
        metadata,
    }
}

pub fn carry_write_delay_across_maintenance(
    max_write_bytes_per_second: Option<u64>,
    diagnostics: Option<&AdmissionDiagnostics>,
) -> bool {
    max_write_bytes_per_second.is_some()
        && !diagnostics.is_some_and(|diagnostics| {
            !diagnostics.triggered_by.is_empty()
                && diagnostics
                    .triggered_by
                    .iter()
                    .all(|signal| *signal == AdmissionPressureSignal::L0Sstables)
        })
}

fn scale_bytes(value: u64, basis_points: u64) -> u64 {
    value
        .saturating_mul(basis_points)
        .checked_div(10_000)
        .unwrap_or_default()
        .max(u64::from(value > 0))
}

fn json_string(value: &JsonValue) -> Option<&str> {
    value.as_str()
}

#[derive(Default)]
struct PressureSignalScore {
    soft_signals: BTreeSet<AdmissionPressureSignal>,
    hard_signals: BTreeSet<AdmissionPressureSignal>,
    soft_score: u32,
}

impl PressureSignalScore {
    fn note(
        &mut self,
        value: u64,
        soft_limit: Option<u64>,
        hard_limit: Option<u64>,
        signal: AdmissionPressureSignal,
        soft_weight: u32,
    ) {
        if hard_limit.is_some_and(|limit| value >= limit) {
            self.hard_signals.insert(signal);
            return;
        }
        if soft_limit.is_some_and(|limit| value >= limit) {
            self.soft_signals.insert(signal);
            self.soft_score = self.soft_score.saturating_add(soft_weight);
        }
    }

    fn note_u32(
        &mut self,
        value: u32,
        soft_limit: Option<u32>,
        hard_limit: Option<u32>,
        signal: AdmissionPressureSignal,
        soft_weight: u32,
    ) {
        self.note(
            u64::from(value),
            soft_limit.map(u64::from),
            hard_limit.map(u64::from),
            signal,
            soft_weight,
        );
    }

    fn note_duration(
        &mut self,
        value: Duration,
        soft_limit: Option<Duration>,
        hard_limit: Option<Duration>,
        signal: AdmissionPressureSignal,
        soft_weight: u32,
    ) {
        self.note(
            value.as_millis() as u64,
            soft_limit.map(|limit| limit.as_millis() as u64),
            hard_limit.map(|limit| limit.as_millis() as u64),
            signal,
            soft_weight,
        );
    }
}

fn required_relief(
    projected: PressureBytes,
    mutable_target: Option<u64>,
    unified_log_target: Option<u64>,
    backlog_target: u64,
) -> PressureBytes {
    let backlog_bytes = projected
        .immutable_queued_bytes
        .saturating_add(projected.immutable_flushing_bytes);
    let backlog_relief = backlog_bytes.saturating_sub(backlog_target);
    let queued_relief = backlog_relief.min(projected.immutable_queued_bytes);
    let flushing_relief = backlog_relief
        .saturating_sub(queued_relief)
        .min(projected.immutable_flushing_bytes);
    PressureBytes {
        mutable_dirty_bytes: mutable_target
            .map(|target| projected.mutable_dirty_bytes.saturating_sub(target))
            .unwrap_or_default(),
        immutable_queued_bytes: queued_relief,
        immutable_flushing_bytes: flushing_relief,
        unified_log_pinned_bytes: unified_log_target
            .map(|target| projected.unified_log_pinned_bytes.saturating_sub(target))
            .unwrap_or_default(),
    }
}
