use std::{collections::BTreeMap, time::Duration};

use terracedb::{
    AdmissionCorrectnessContext, AdmissionSignals, DomainMemoryBudget, ExecutionDomainBudget,
    ExecutionDomainPath, FlushPressureCandidate, PendingWork, PendingWorkType, PressureBudget,
    PressureBytes, PressureScope, PressureStats, SequenceNumber, WorkRuntimeTag,
};

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
        let estimated_bytes = pressure.local.immutable_queued_bytes;
        if estimated_bytes == 0 {
            return None;
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
            estimated_relief: PressureBytes {
                immutable_queued_bytes: estimated_bytes,
                unified_log_pinned_bytes: estimated_bytes,
                ..PressureBytes::default()
            },
            metadata: BTreeMap::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use terracedb::{ContentionClass, DurabilityClass, ExecutionDomainOwner, ExecutionLane};

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
        assert_eq!(candidate.estimated_relief.immutable_queued_bytes, 64);
        assert_eq!(candidate.estimated_relief.unified_log_pinned_bytes, 64);

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
    }
}
