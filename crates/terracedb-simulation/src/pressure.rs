use std::{collections::BTreeMap, time::Duration};

use serde_json::json;
use terracedb::{
    AdmissionCorrectnessContext, AdmissionSignals, DomainMemoryBudget, ExecutionDomainBudget,
    ExecutionDomainPath, FlushPressureCandidate, PendingWork, PendingWorkType, PressureBudget,
    PressureBytes, PressureScope, PressureStats, SequenceNumber, WorkRuntimeTag,
};

const SOFT_AGE_TRIGGER: Duration = Duration::from_secs(2);
const FORCE_AGE_CEILING: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeterministicPressureTableState {
    pub domain: Option<ExecutionDomainPath>,
    pub pressure: PressureBytes,
    pub oldest_unflushed_since: Option<Duration>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeterministicPressureSnapshot {
    pub elapsed: Duration,
    pub process_budget: PressureBudget,
    pub domain_budgets: BTreeMap<ExecutionDomainPath, PressureBudget>,
    pub tables: BTreeMap<String, DeterministicPressureTableState>,
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicPressureOracle {
    elapsed: Duration,
    process_budget: PressureBudget,
    domain_budgets: BTreeMap<ExecutionDomainPath, PressureBudget>,
    tables: BTreeMap<String, DeterministicPressureTableState>,
}

impl DeterministicPressureOracle {
    pub fn bind_table_domain(&mut self, table: impl Into<String>, domain: ExecutionDomainPath) {
        self.tables.entry(table.into()).or_default().domain = Some(domain);
    }

    pub fn set_process_budget(&mut self, budget: PressureBudget) {
        self.process_budget = budget;
    }

    pub fn set_domain_budget(&mut self, path: ExecutionDomainPath, budget: PressureBudget) {
        self.domain_budgets.insert(path, budget);
    }

    pub fn advance(&mut self, duration: Duration) {
        self.elapsed = self.elapsed.saturating_add(duration);
    }

    pub fn record_mutable_write(&mut self, table: impl Into<String>, bytes: u64) {
        let state = self.tables.entry(table.into()).or_default();
        state.pressure.mutable_dirty_bytes =
            state.pressure.mutable_dirty_bytes.saturating_add(bytes);
        Self::refresh_table_state(self.elapsed, state);
    }

    pub fn queue_flush(&mut self, table: &str) {
        let state = self.tables.entry(table.to_string()).or_default();
        state.pressure.immutable_queued_bytes = state
            .pressure
            .immutable_queued_bytes
            .saturating_add(state.pressure.mutable_dirty_bytes);
        state.pressure.mutable_dirty_bytes = 0;
        Self::refresh_table_state(self.elapsed, state);
    }

    pub fn begin_flush(&mut self, table: &str, bytes: u64) {
        let state = self.tables.entry(table.to_string()).or_default();
        let bytes = bytes.min(state.pressure.immutable_queued_bytes);
        state.pressure.immutable_queued_bytes =
            state.pressure.immutable_queued_bytes.saturating_sub(bytes);
        state.pressure.immutable_flushing_bytes = state
            .pressure
            .immutable_flushing_bytes
            .saturating_add(bytes);
        Self::refresh_table_state(self.elapsed, state);
    }

    pub fn finish_flush(&mut self, table: &str, bytes: u64) {
        let state = self.tables.entry(table.to_string()).or_default();
        let bytes = bytes.min(state.pressure.immutable_flushing_bytes);
        state.pressure.immutable_flushing_bytes = state
            .pressure
            .immutable_flushing_bytes
            .saturating_sub(bytes);
        Self::refresh_table_state(self.elapsed, state);
    }

    pub fn fail_flush(&mut self, table: &str, bytes: u64) {
        let state = self.tables.entry(table.to_string()).or_default();
        let bytes = bytes.min(state.pressure.immutable_flushing_bytes);
        state.pressure.immutable_flushing_bytes = state
            .pressure
            .immutable_flushing_bytes
            .saturating_sub(bytes);
        state.pressure.immutable_queued_bytes =
            state.pressure.immutable_queued_bytes.saturating_add(bytes);
        Self::refresh_table_state(self.elapsed, state);
    }

    pub fn snapshot(&self) -> DeterministicPressureSnapshot {
        DeterministicPressureSnapshot {
            elapsed: self.elapsed,
            process_budget: self.process_budget,
            domain_budgets: self.domain_budgets.clone(),
            tables: self.tables.clone(),
        }
    }

    pub fn from_snapshot(snapshot: DeterministicPressureSnapshot) -> Self {
        Self {
            elapsed: snapshot.elapsed,
            process_budget: snapshot.process_budget,
            domain_budgets: snapshot.domain_budgets,
            tables: snapshot.tables,
        }
    }

    pub fn recover_after_restart(&self) -> Self {
        let mut restored = self.clone();
        for state in restored.tables.values_mut() {
            state.pressure.mutable_dirty_bytes = state
                .pressure
                .mutable_dirty_bytes
                .saturating_add(state.pressure.immutable_queued_bytes)
                .saturating_add(state.pressure.immutable_flushing_bytes);
            state.pressure.immutable_queued_bytes = 0;
            state.pressure.immutable_flushing_bytes = 0;
            state.oldest_unflushed_since = None;
            Self::refresh_table_state(restored.elapsed, state);
        }
        restored
    }

    pub fn process_pressure_stats(&self) -> PressureStats {
        let total = self.process_total();
        PressureStats {
            scope: PressureScope::Process,
            local: total,
            domain_total: Some(total),
            process_total: Some(total),
            oldest_unflushed_age: self.oldest_unflushed_age(),
            budget: self.process_budget,
            metadata: BTreeMap::new(),
        }
    }

    pub fn domain_pressure_stats(&self, path: &ExecutionDomainPath) -> PressureStats {
        let local = self.domain_total(path);
        PressureStats {
            scope: PressureScope::Domain(path.clone()),
            local,
            domain_total: Some(local),
            process_total: Some(self.process_total()),
            oldest_unflushed_age: self.oldest_unflushed_age_for_domain(path),
            budget: self
                .domain_budgets
                .get(path)
                .copied()
                .unwrap_or(self.process_budget),
            metadata: BTreeMap::new(),
        }
    }

    pub fn table_pressure_stats(&self, table: &str) -> PressureStats {
        let state = self.tables.get(table).cloned().unwrap_or_default();
        PressureStats {
            scope: PressureScope::Table(table.to_string()),
            local: state.pressure,
            domain_total: state.domain.as_ref().map(|path| self.domain_total(path)),
            process_total: Some(self.process_total()),
            oldest_unflushed_age: self.age_since(state.oldest_unflushed_since),
            budget: state
                .domain
                .as_ref()
                .and_then(|path| self.domain_budgets.get(path).copied())
                .unwrap_or(self.process_budget),
            metadata: BTreeMap::new(),
        }
    }

    pub fn flush_candidate(&self, table: &str) -> Option<FlushPressureCandidate> {
        let pressure = self.table_pressure_stats(table);
        let estimated_bytes = pressure
            .local
            .mutable_dirty_bytes
            .saturating_add(pressure.local.immutable_queued_bytes);
        if estimated_bytes == 0 {
            return None;
        }

        let estimated_relief = PressureBytes {
            mutable_dirty_bytes: pressure.local.mutable_dirty_bytes,
            immutable_queued_bytes: pressure.local.immutable_queued_bytes,
            unified_log_pinned_bytes: pressure.local.unified_log_pinned_bytes,
            ..PressureBytes::default()
        };
        let score = flush_score(&pressure, estimated_relief);
        let force_reason = flush_force_reason(&pressure);
        let mut metadata = BTreeMap::from([
            ("flush_score".to_string(), json!(score)),
            (
                "selection_reason".to_string(),
                json!(flush_selection_reason(&pressure, estimated_relief)),
            ),
            (
                "soft_guardrail".to_string(),
                json!(flush_soft_guardrail_triggered(&pressure)),
            ),
        ]);
        if let Some(reason) = force_reason {
            metadata.insert("force_reason".to_string(), json!(reason));
        }

        Some(FlushPressureCandidate {
            work: PendingWork {
                id: format!("flush:{table}"),
                work_type: PendingWorkType::Flush,
                table: table.to_string(),
                level: None,
                estimated_bytes,
            },
            pressure,
            estimated_relief,
            metadata,
        })
    }

    pub fn admission_signals(
        &self,
        table: &str,
        runtime_tag: WorkRuntimeTag,
        batch_write_bytes: u64,
    ) -> AdmissionSignals {
        AdmissionSignals {
            table: table.to_string(),
            domain_budget: self.execution_domain_budget(&runtime_tag.domain),
            pressure: self.table_pressure_stats(table),
            batch_write_bytes,
            l0_sstable_count: 0,
            immutable_memtable_count: self
                .tables
                .get(table)
                .map(|state| {
                    u32::from(
                        state.pressure.immutable_queued_bytes > 0
                            || state.pressure.immutable_flushing_bytes > 0,
                    )
                })
                .unwrap_or_default(),
            compaction_debt_bytes: 0,
            correctness: AdmissionCorrectnessContext {
                durable_sequence: SequenceNumber::default(),
                visible_sequence: SequenceNumber::default(),
                commit_log_recovery_floor_sequence: SequenceNumber::default(),
                commit_log_gc_floor_sequence: SequenceNumber::default(),
            },
            metadata: BTreeMap::new(),
            runtime_tag,
        }
    }

    fn process_total(&self) -> PressureBytes {
        self.tables
            .values()
            .fold(PressureBytes::default(), |mut total, state| {
                total.mutable_dirty_bytes = total
                    .mutable_dirty_bytes
                    .saturating_add(state.pressure.mutable_dirty_bytes);
                total.immutable_queued_bytes = total
                    .immutable_queued_bytes
                    .saturating_add(state.pressure.immutable_queued_bytes);
                total.immutable_flushing_bytes = total
                    .immutable_flushing_bytes
                    .saturating_add(state.pressure.immutable_flushing_bytes);
                total.unified_log_pinned_bytes = total
                    .unified_log_pinned_bytes
                    .saturating_add(state.pressure.unified_log_pinned_bytes);
                total
            })
    }

    fn domain_total(&self, path: &ExecutionDomainPath) -> PressureBytes {
        self.tables
            .values()
            .fold(PressureBytes::default(), |mut total, state| {
                let matches_domain = state.domain.as_ref().is_some_and(|domain| {
                    domain.is_same_or_descendant_of(path) || path.is_same_or_descendant_of(domain)
                });
                if matches_domain {
                    total.mutable_dirty_bytes = total
                        .mutable_dirty_bytes
                        .saturating_add(state.pressure.mutable_dirty_bytes);
                    total.immutable_queued_bytes = total
                        .immutable_queued_bytes
                        .saturating_add(state.pressure.immutable_queued_bytes);
                    total.immutable_flushing_bytes = total
                        .immutable_flushing_bytes
                        .saturating_add(state.pressure.immutable_flushing_bytes);
                    total.unified_log_pinned_bytes = total
                        .unified_log_pinned_bytes
                        .saturating_add(state.pressure.unified_log_pinned_bytes);
                }
                total
            })
    }

    fn oldest_unflushed_age(&self) -> Option<Duration> {
        self.tables
            .values()
            .filter_map(|state| self.age_since(state.oldest_unflushed_since))
            .max()
    }

    fn oldest_unflushed_age_for_domain(&self, path: &ExecutionDomainPath) -> Option<Duration> {
        self.tables
            .values()
            .filter(|state| {
                state.domain.as_ref().is_some_and(|domain| {
                    domain.is_same_or_descendant_of(path) || path.is_same_or_descendant_of(domain)
                })
            })
            .filter_map(|state| self.age_since(state.oldest_unflushed_since))
            .max()
    }

    fn age_since(&self, since: Option<Duration>) -> Option<Duration> {
        since.map(|since| self.elapsed.checked_sub(since).unwrap_or_default())
    }

    fn execution_domain_budget(&self, path: &ExecutionDomainPath) -> Option<ExecutionDomainBudget> {
        self.domain_budgets
            .get(path)
            .copied()
            .map(|budget| ExecutionDomainBudget {
                memory: DomainMemoryBudget {
                    mutable_bytes: budget.mutable_hard_limit_bytes,
                    ..DomainMemoryBudget::default()
                },
                ..ExecutionDomainBudget::default()
            })
    }

    fn refresh_table_state(elapsed: Duration, state: &mut DeterministicPressureTableState) {
        state.pressure.unified_log_pinned_bytes = state.pressure.total_memory_bytes();
        if state.pressure.total_memory_bytes() > 0 {
            state.oldest_unflushed_since.get_or_insert(elapsed);
        } else {
            state.oldest_unflushed_since = None;
        }
    }
}

fn budget_pressure_bps(pressure: &PressureStats) -> u64 {
    let hard_limit = pressure
        .budget
        .mutable_hard_limit_bytes
        .or(pressure.budget.unified_log_hard_limit_bytes)
        .filter(|limit| *limit > 0);
    let Some(limit) = hard_limit else {
        return 0;
    };

    [
        pressure.local.total_memory_bytes(),
        pressure
            .domain_total
            .unwrap_or(pressure.local)
            .total_memory_bytes(),
        pressure
            .process_total
            .unwrap_or(pressure.domain_total.unwrap_or(pressure.local))
            .total_memory_bytes(),
        pressure.local.unified_log_pinned_bytes,
        pressure
            .domain_total
            .unwrap_or(pressure.local)
            .unified_log_pinned_bytes,
        pressure
            .process_total
            .unwrap_or(pressure.domain_total.unwrap_or(pressure.local))
            .unified_log_pinned_bytes,
    ]
    .into_iter()
    .map(|bytes| bytes.saturating_mul(10_000).div_ceil(limit))
    .max()
    .unwrap_or_default()
}

fn flush_soft_guardrail_triggered(pressure: &PressureStats) -> bool {
    let soft_limit = pressure
        .budget
        .mutable_soft_limit_bytes
        .or_else(|| {
            pressure
                .budget
                .mutable_hard_limit_bytes
                .map(|limit| limit * 3 / 4)
        })
        .or_else(|| {
            pressure.budget.unified_log_soft_limit_bytes.or(pressure
                .budget
                .unified_log_hard_limit_bytes
                .map(|limit| limit * 3 / 4))
        });
    pressure.oldest_unflushed_age >= Some(SOFT_AGE_TRIGGER)
        || pressure.local.total_memory_bytes() >= soft_limit.unwrap_or(u64::MAX)
        || pressure.local.unified_log_pinned_bytes >= soft_limit.unwrap_or(u64::MAX)
}

fn flush_force_reason(pressure: &PressureStats) -> Option<&'static str> {
    let force_limit = pressure
        .budget
        .mutable_hard_limit_bytes
        .or(pressure.budget.unified_log_hard_limit_bytes)
        .map(|limit| limit * 7 / 8);
    if pressure.local.unified_log_pinned_bytes >= force_limit.unwrap_or(u64::MAX) {
        return Some("unified-log pressure");
    }
    if pressure.local.mutable_dirty_bytes >= force_limit.unwrap_or(u64::MAX) {
        return Some("dirty-byte pressure");
    }
    if pressure.oldest_unflushed_age >= Some(FORCE_AGE_CEILING) {
        return Some("oldest-unflushed age");
    }
    None
}

fn flush_score(pressure: &PressureStats, estimated_relief: PressureBytes) -> u64 {
    let guardrail_bonus: u64 = if flush_force_reason(pressure).is_some() {
        2_000_000
    } else if flush_soft_guardrail_triggered(pressure) {
        1_000_000
    } else {
        0
    };
    let age_millis = pressure
        .oldest_unflushed_age
        .map(|age| age.as_millis() as u64)
        .unwrap_or_default()
        .min(60_000);

    guardrail_bonus
        .saturating_add(budget_pressure_bps(pressure).saturating_mul(128))
        .saturating_add(estimated_relief.unified_log_pinned_bytes.saturating_mul(4))
        .saturating_add(estimated_relief.mutable_dirty_bytes.saturating_mul(3))
        .saturating_add(estimated_relief.immutable_queued_bytes.saturating_mul(2))
        .saturating_add(age_millis.saturating_mul(32))
}

fn flush_selection_reason(pressure: &PressureStats, estimated_relief: PressureBytes) -> String {
    let mut reasons = Vec::new();
    if let Some(reason) = flush_force_reason(pressure) {
        reasons.push(format!("guardrail triggered by {reason}"));
    } else if flush_soft_guardrail_triggered(pressure) {
        reasons.push("pressure is above the soft guardrail".to_string());
    }
    if estimated_relief.unified_log_pinned_bytes > 0 {
        reasons.push("reclaims unified-log headroom".to_string());
    }
    if estimated_relief.mutable_dirty_bytes > 0 {
        reasons.push("drains mutable dirty bytes".to_string());
    }
    if pressure.oldest_unflushed_age >= Some(SOFT_AGE_TRIGGER) {
        reasons.push("oldest unflushed data is aging".to_string());
    }
    if reasons.is_empty() {
        "conservative flush fallback".to_string()
    } else {
        reasons.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use terracedb::{
        AdmissionPressureLevel, AdmissionPressureSignal, ContentionClass, DurabilityClass,
        ExecutionDomainOwner, ExecutionLane, carry_write_delay_across_maintenance,
        multi_signal_write_admission,
    };

    fn diagnostics_with_metadata(
        oracle: &DeterministicPressureOracle,
        table: &str,
        runtime_tag: &WorkRuntimeTag,
        batch_write_bytes: u64,
        metadata: &BTreeMap<String, serde_json::Value>,
    ) -> terracedb::AdmissionDiagnostics {
        let mut signals = oracle.admission_signals(table, runtime_tag.clone(), batch_write_bytes);
        signals.metadata.extend(metadata.clone());
        multi_signal_write_admission(&signals)
    }

    fn throttle_delay(bytes: u64, max_write_bytes_per_second: u64) -> Duration {
        if bytes == 0 || max_write_bytes_per_second == 0 {
            return Duration::ZERO;
        }

        let millis = ((bytes as u128).saturating_mul(1000))
            .div_ceil(max_write_bytes_per_second as u128) as u64;
        Duration::from_millis(millis)
    }

    fn simulate_write_backpressure_delay<F>(
        oracle: &mut DeterministicPressureOracle,
        table: &str,
        runtime_tag: &WorkRuntimeTag,
        batch_write_bytes: u64,
        metadata: &BTreeMap<String, serde_json::Value>,
        mut run_maintenance: F,
    ) -> Duration
    where
        F: FnMut(&mut DeterministicPressureOracle) -> bool,
    {
        let mut pending_delay = Duration::ZERO;

        loop {
            let diagnostics =
                diagnostics_with_metadata(oracle, table, runtime_tag, batch_write_bytes, metadata);
            let table_delay = diagnostics
                .max_write_bytes_per_second
                .map(|rate| throttle_delay(batch_write_bytes, rate))
                .unwrap_or_default();
            if carry_write_delay_across_maintenance(
                diagnostics.max_write_bytes_per_second,
                Some(&diagnostics),
            ) {
                pending_delay = pending_delay.max(table_delay);
            }

            let should_run_maintenance = diagnostics.level != AdmissionPressureLevel::Open;
            let must_stall = diagnostics.level == AdmissionPressureLevel::Stall;
            if should_run_maintenance {
                let progressed = run_maintenance(oracle);
                if progressed {
                    continue;
                }
                if must_stall {
                    break;
                }
            }
            break;
        }

        pending_delay
    }

    fn simulate_write_backpressure_delay_from_signals<F, G>(
        batch_write_bytes: u64,
        mut next_signals: F,
        mut run_maintenance: G,
    ) -> Duration
    where
        F: FnMut() -> AdmissionSignals,
        G: FnMut() -> bool,
    {
        let mut pending_delay = Duration::ZERO;

        loop {
            let diagnostics = multi_signal_write_admission(&next_signals());
            let table_delay = diagnostics
                .max_write_bytes_per_second
                .map(|rate| throttle_delay(batch_write_bytes, rate))
                .unwrap_or_default();
            if carry_write_delay_across_maintenance(
                diagnostics.max_write_bytes_per_second,
                Some(&diagnostics),
            ) {
                pending_delay = pending_delay.max(table_delay);
            }

            let should_run_maintenance = diagnostics.level != AdmissionPressureLevel::Open;
            let must_stall = diagnostics.level == AdmissionPressureLevel::Stall;
            if should_run_maintenance {
                let progressed = run_maintenance();
                if progressed {
                    continue;
                }
                if must_stall {
                    break;
                }
            }
            break;
        }

        pending_delay
    }

    #[test]
    fn deterministic_pressure_oracle_reconstructs_and_tags_work() {
        let domain = ExecutionDomainPath::new(["process", "db", "foreground"]);
        let tag = WorkRuntimeTag {
            owner: ExecutionDomainOwner::Database {
                name: "pressure-db".to_string(),
            },
            lane: ExecutionLane::UserForeground,
            contention_class: ContentionClass::UserData,
            domain: domain.clone(),
            durability_class: DurabilityClass::UserData,
        };

        let mut oracle = DeterministicPressureOracle::default();
        oracle.set_process_budget(PressureBudget {
            mutable_hard_limit_bytes: Some(512),
            unified_log_hard_limit_bytes: Some(512),
            ..PressureBudget::default()
        });
        oracle.set_domain_budget(
            domain.clone(),
            PressureBudget {
                mutable_hard_limit_bytes: Some(256),
                ..PressureBudget::default()
            },
        );
        oracle.bind_table_domain("events", domain.clone());
        oracle.record_mutable_write("events", 64);
        oracle.advance(Duration::from_millis(40));
        oracle.queue_flush("events");
        oracle.advance(Duration::from_millis(20));
        oracle.begin_flush("events", 32);
        oracle.fail_flush("events", 32);

        let candidate = oracle
            .flush_candidate("events")
            .expect("queued bytes should produce a flush candidate");
        assert_eq!(candidate.estimated_relief.mutable_dirty_bytes, 0);
        assert_eq!(candidate.estimated_relief.immutable_queued_bytes, 64);
        assert_eq!(candidate.estimated_relief.unified_log_pinned_bytes, 64);
        assert!(
            candidate
                .metadata
                .get("flush_score")
                .and_then(serde_json::Value::as_u64)
                .is_some()
        );

        let signals = oracle.admission_signals("events", tag.clone(), 32);
        assert_eq!(signals.runtime_tag, tag);
        assert_eq!(signals.pressure.local.mutable_dirty_bytes, 0);
        assert_eq!(signals.pressure.local.immutable_queued_bytes, 64);
        assert_eq!(signals.pressure.local.immutable_flushing_bytes, 0);
        assert_eq!(
            signals.pressure.oldest_unflushed_age,
            Some(Duration::from_millis(60))
        );
        assert_eq!(
            signals
                .domain_budget
                .and_then(|budget| budget.memory.mutable_bytes),
            Some(256)
        );

        let restored = DeterministicPressureOracle::from_snapshot(oracle.snapshot());
        assert_eq!(restored.flush_candidate("events"), Some(candidate));
        assert_eq!(
            restored.admission_signals("events", tag, 32).pressure,
            signals.pressure
        );

        let restarted = oracle.recover_after_restart();
        let restarted_pressure = restarted.table_pressure_stats("events");
        assert_eq!(restarted_pressure.local.mutable_dirty_bytes, 64);
        assert_eq!(restarted_pressure.local.immutable_queued_bytes, 0);
        assert_eq!(restarted_pressure.local.immutable_flushing_bytes, 0);
        assert_eq!(
            restarted_pressure.oldest_unflushed_age,
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn deterministic_pressure_oracle_scores_older_flush_higher_when_bytes_match() {
        let domain = ExecutionDomainPath::new(["process", "db", "background"]);
        let mut oracle = DeterministicPressureOracle::default();
        oracle.set_process_budget(PressureBudget {
            mutable_hard_limit_bytes: Some(256),
            unified_log_hard_limit_bytes: Some(256),
            ..PressureBudget::default()
        });
        oracle.bind_table_domain("hot", domain.clone());
        oracle.bind_table_domain("fresh", domain);

        oracle.record_mutable_write("hot", 64);
        oracle.advance(Duration::from_secs(3));
        oracle.record_mutable_write("fresh", 64);

        let hot = oracle.flush_candidate("hot").expect("hot candidate");
        let fresh = oracle.flush_candidate("fresh").expect("fresh candidate");
        let hot_score = hot
            .metadata
            .get("flush_score")
            .and_then(serde_json::Value::as_u64)
            .expect("hot score");
        let fresh_score = fresh
            .metadata
            .get("flush_score")
            .and_then(serde_json::Value::as_u64)
            .expect("fresh score");

        assert_eq!(hot.work.estimated_bytes, fresh.work.estimated_bytes);
        assert!(hot_score > fresh_score);
        assert_eq!(
            hot.metadata
                .get("selection_reason")
                .and_then(serde_json::Value::as_str),
            Some(
                "pressure is above the soft guardrail, reclaims unified-log headroom, drains mutable dirty bytes, oldest unflushed data is aging"
            )
        );
    }

    #[test]
    fn deterministic_pressure_oracle_marks_force_reasons_before_hard_ceiling() {
        let mut oracle = DeterministicPressureOracle::default();
        oracle.set_process_budget(PressureBudget {
            mutable_hard_limit_bytes: Some(160),
            unified_log_hard_limit_bytes: Some(160),
            ..PressureBudget::default()
        });
        oracle.record_mutable_write("events", 140);

        let candidate = oracle.flush_candidate("events").expect("force candidate");
        assert_eq!(
            candidate
                .metadata
                .get("force_reason")
                .and_then(serde_json::Value::as_str),
            Some("unified-log pressure")
        );
    }

    #[test]
    fn multi_signal_admission_reacts_before_l0_pressure_when_mutable_bytes_run_hot() {
        let domain = ExecutionDomainPath::new(["process", "db", "foreground"]);
        let tag = WorkRuntimeTag {
            owner: ExecutionDomainOwner::Database {
                name: "pressure-db".to_string(),
            },
            lane: ExecutionLane::UserForeground,
            contention_class: ContentionClass::UserData,
            domain: domain.clone(),
            durability_class: DurabilityClass::UserData,
        };

        let mut oracle = DeterministicPressureOracle::default();
        oracle.set_process_budget(PressureBudget {
            mutable_hard_limit_bytes: Some(256),
            ..PressureBudget::default()
        });
        oracle.bind_table_domain("events", domain);
        oracle.record_mutable_write("events", 176);

        let signals = oracle.admission_signals("events", tag, 48);
        assert_eq!(signals.l0_sstable_count, 0);

        let diagnostics = multi_signal_write_admission(&signals);
        assert_eq!(diagnostics.level, AdmissionPressureLevel::RateLimit);
        assert!(
            diagnostics
                .triggered_by
                .contains(&AdmissionPressureSignal::MutableBudget)
        );
        assert!(
            !diagnostics
                .triggered_by
                .contains(&AdmissionPressureSignal::L0Sstables)
        );
        assert!(diagnostics.max_write_bytes_per_second.is_some());
    }

    #[test]
    fn domain_budgets_and_workload_policies_shift_admission_without_changing_correctness_signals() {
        let tight_domain = ExecutionDomainPath::new(["process", "db", "primary"]);
        let relaxed_domain = ExecutionDomainPath::new(["process", "db", "analytics"]);
        let make_tag = |domain: &ExecutionDomainPath, name: &str| WorkRuntimeTag {
            owner: ExecutionDomainOwner::Database {
                name: name.to_string(),
            },
            lane: ExecutionLane::UserForeground,
            contention_class: ContentionClass::UserData,
            domain: domain.clone(),
            durability_class: DurabilityClass::UserData,
        };

        let mut oracle = DeterministicPressureOracle::default();
        oracle.set_process_budget(PressureBudget {
            mutable_hard_limit_bytes: Some(1024),
            ..PressureBudget::default()
        });
        oracle.set_domain_budget(
            tight_domain.clone(),
            PressureBudget {
                mutable_hard_limit_bytes: Some(320),
                ..PressureBudget::default()
            },
        );
        oracle.set_domain_budget(
            relaxed_domain.clone(),
            PressureBudget {
                mutable_hard_limit_bytes: Some(512),
                ..PressureBudget::default()
            },
        );
        oracle.bind_table_domain("orders", tight_domain.clone());
        oracle.bind_table_domain("rollups", relaxed_domain.clone());
        oracle.record_mutable_write("orders", 240);
        oracle.record_mutable_write("rollups", 240);

        let tight = oracle.admission_signals("orders", make_tag(&tight_domain, "primary"), 48);
        let relaxed =
            oracle.admission_signals("rollups", make_tag(&relaxed_domain, "analytics"), 48);

        assert_eq!(
            multi_signal_write_admission(&tight).level,
            AdmissionPressureLevel::RateLimit
        );
        assert_eq!(
            multi_signal_write_admission(&relaxed).level,
            AdmissionPressureLevel::Open
        );

        let mut primary_policy = relaxed.clone();
        primary_policy
            .metadata
            .insert("terracedb.execution.role".to_string(), json!("primary"));
        let mut analytics_policy = relaxed;
        analytics_policy.metadata.insert(
            "terracedb.execution.role".to_string(),
            json!("analytics-helper"),
        );

        assert_eq!(
            multi_signal_write_admission(&primary_policy).level,
            AdmissionPressureLevel::RateLimit
        );
        assert_eq!(
            multi_signal_write_admission(&analytics_policy).level,
            AdmissionPressureLevel::Open
        );
        assert_eq!(
            primary_policy.correctness, analytics_policy.correctness,
            "policy changes should not alter correctness metadata",
        );
    }

    #[test]
    fn domain_mutable_budget_stall_clears_once_flush_relief_finishes() {
        let domain = ExecutionDomainPath::new(["process", "db", "foreground"]);
        let tag = WorkRuntimeTag {
            owner: ExecutionDomainOwner::Database {
                name: "domain-mutable".to_string(),
            },
            lane: ExecutionLane::UserForeground,
            contention_class: ContentionClass::UserData,
            domain: domain.clone(),
            durability_class: DurabilityClass::UserData,
        };

        let mut oracle = DeterministicPressureOracle::default();
        oracle.set_process_budget(PressureBudget {
            mutable_hard_limit_bytes: Some(1024),
            ..PressureBudget::default()
        });
        oracle.set_domain_budget(
            domain.clone(),
            PressureBudget {
                mutable_hard_limit_bytes: Some(300),
                ..PressureBudget::default()
            },
        );
        oracle.bind_table_domain("events", domain);
        oracle.record_mutable_write("events", 220);

        let before_flush = oracle.admission_signals("events", tag.clone(), 200);
        let before_diagnostics = multi_signal_write_admission(&before_flush);
        assert_eq!(before_diagnostics.level, AdmissionPressureLevel::Stall);
        assert!(
            before_diagnostics
                .triggered_by
                .contains(&AdmissionPressureSignal::MutableBudget)
        );
        assert!(before_diagnostics.required_relief.mutable_dirty_bytes > 0);

        oracle.queue_flush("events");
        let queued_flush = oracle.admission_signals("events", tag.clone(), 200);
        assert_ne!(
            multi_signal_write_admission(&queued_flush).level,
            AdmissionPressureLevel::Open,
            "queued-but-unfinished flush pressure should still gate admission",
        );

        oracle.begin_flush("events", 220);
        oracle.finish_flush("events", 220);
        let after_flush = oracle.admission_signals("events", tag, 200);
        assert_eq!(
            multi_signal_write_admission(&after_flush).level,
            AdmissionPressureLevel::Open
        );
    }

    #[test]
    fn rate_limit_backoff_survives_maintenance_progress() {
        let domain = ExecutionDomainPath::new(["process", "db", "foreground"]);
        let tag = WorkRuntimeTag {
            owner: ExecutionDomainOwner::Database {
                name: "multi-signal".to_string(),
            },
            lane: ExecutionLane::UserForeground,
            contention_class: ContentionClass::UserData,
            domain: domain.clone(),
            durability_class: DurabilityClass::UserData,
        };

        let mut oracle = DeterministicPressureOracle::default();
        oracle.set_process_budget(PressureBudget {
            mutable_hard_limit_bytes: Some(1024),
            ..PressureBudget::default()
        });
        oracle.set_domain_budget(
            domain.clone(),
            PressureBudget {
                mutable_hard_limit_bytes: Some(512),
                ..PressureBudget::default()
            },
        );
        oracle.bind_table_domain("events", domain);
        oracle.record_mutable_write("events", 220);

        let policy_metadata =
            BTreeMap::from([("terracedb.execution.role".to_string(), json!("primary"))]);
        let before = diagnostics_with_metadata(&oracle, "events", &tag, 96, &policy_metadata);
        assert_eq!(before.level, AdmissionPressureLevel::RateLimit);
        assert!(
            before
                .triggered_by
                .contains(&AdmissionPressureSignal::MutableBudget)
        );

        let mut maintenance_ran = false;
        let delay = simulate_write_backpressure_delay(
            &mut oracle,
            "events",
            &tag,
            96,
            &policy_metadata,
            |oracle| {
                if maintenance_ran {
                    return false;
                }
                maintenance_ran = true;
                oracle.queue_flush("events");
                oracle.begin_flush("events", 220);
                oracle.finish_flush("events", 220);
                true
            },
        );

        let after = diagnostics_with_metadata(&oracle, "events", &tag, 96, &policy_metadata);
        assert_eq!(after.level, AdmissionPressureLevel::Open);
        assert!(
            delay > Duration::ZERO,
            "maintenance progress should not erase a previously observed rate-limit delay",
        );
    }

    #[test]
    fn l0_only_backoff_clears_once_maintenance_progresses() {
        let l0_sstable_count =
            std::cell::Cell::new(terracedb::DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT);
        let domain = ExecutionDomainPath::new(["process", "db", "foreground"]);
        let tag = WorkRuntimeTag {
            owner: ExecutionDomainOwner::Database {
                name: "workflow".to_string(),
            },
            lane: ExecutionLane::UserForeground,
            contention_class: ContentionClass::UserData,
            domain,
            durability_class: DurabilityClass::UserData,
        };

        let mut oracle = DeterministicPressureOracle::default();
        oracle.set_process_budget(PressureBudget {
            mutable_hard_limit_bytes: Some(1024 * 1024),
            ..PressureBudget::default()
        });

        let delay = simulate_write_backpressure_delay_from_signals(
            64,
            || {
                let mut signals = oracle.admission_signals("events", tag.clone(), 64);
                signals.l0_sstable_count = l0_sstable_count.get();
                signals
            },
            || {
                if l0_sstable_count.get() == 0 {
                    return false;
                }
                l0_sstable_count.set(0);
                true
            },
        );

        assert_eq!(
            delay,
            Duration::ZERO,
            "maintenance that clears pure L0 pressure should not leave a carried-over delay",
        );
    }
}
