use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::{broadcast, watch};

use crate::{
    Timestamp,
    api::Table,
    current_state::CurrentStateRetentionStats,
    execution::{
        ContentionClass, DomainTaggedWork, DurabilityClass, ExecutionDomainBudget,
        ExecutionDomainPath, ExecutionLane, WorkRuntimeTag,
    },
    ids::SequenceNumber,
    pressure::{
        AdmissionDiagnostics, AdmissionPressureLevel, AdmissionSignals, FlushPressureCandidate,
        multi_signal_write_admission,
    },
};

pub const DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT: u32 = 3;
pub const DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT: u32 = 4;

pub trait Scheduler: Send + Sync {
    fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision>;
    fn should_throttle(&self, table: &Table, stats: &TableStats) -> ThrottleDecision;
    fn on_domain_work_available(
        &self,
        work: &[DomainTaggedWork<PendingWork>],
    ) -> Vec<ScheduleDecision> {
        let untagged = work
            .iter()
            .map(|item| item.work.clone())
            .collect::<Vec<_>>();
        self.on_work_available(&untagged)
    }
    fn should_throttle_in_domain(
        &self,
        table: &Table,
        stats: &TableStats,
        _tag: &WorkRuntimeTag,
        _domain_budget: Option<&ExecutionDomainBudget>,
    ) -> ThrottleDecision {
        self.should_throttle(table, stats)
    }
    fn on_flush_pressure_available(
        &self,
        candidates: &[DomainTaggedWork<FlushPressureCandidate>],
    ) -> Vec<ScheduleDecision> {
        let untagged = candidates
            .iter()
            .map(|item| DomainTaggedWork::new(item.work.work.clone(), item.tag.clone()))
            .collect::<Vec<_>>();
        self.on_domain_work_available(&untagged)
    }
    fn admission_decision(
        &self,
        table: &Table,
        stats: &TableStats,
        _signals: &AdmissionSignals,
    ) -> ThrottleDecision {
        self.should_throttle(table, stats)
    }
    fn admission_decision_in_domain(
        &self,
        table: &Table,
        stats: &TableStats,
        _signals: &AdmissionSignals,
        tag: &WorkRuntimeTag,
        domain_budget: Option<&ExecutionDomainBudget>,
    ) -> ThrottleDecision {
        self.should_throttle_in_domain(table, stats, tag, domain_budget)
    }
    fn admission_diagnostics(
        &self,
        _table: &Table,
        _stats: &TableStats,
        _signals: &AdmissionSignals,
        _tag: &WorkRuntimeTag,
        _domain_budget: Option<&ExecutionDomainBudget>,
    ) -> Option<AdmissionDiagnostics> {
        None
    }
    fn work_budget(&self, _work: &PendingWork, _stats: &TableStats) -> PendingWorkBudget {
        PendingWorkBudget::default()
    }
    fn domain_work_budget(
        &self,
        work: &DomainTaggedWork<PendingWork>,
        stats: &TableStats,
        domain_budget: Option<&ExecutionDomainBudget>,
        priority_override: DomainPriorityOverride,
    ) -> PendingWorkBudget {
        let mut budget = self.work_budget(&work.work, stats);
        apply_domain_budget_overrides(&mut budget, work, domain_budget, priority_override);
        budget
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PendingWorkType {
    Flush,
    CurrentStateRetention,
    Compaction,
    Backup,
    Offload,
    Prefetch,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PendingWorkBudget {
    pub max_bytes_per_second: Option<u64>,
    pub max_in_flight_bytes: Option<u64>,
    pub max_in_flight_requests: Option<u32>,
    pub max_concurrency: Option<u32>,
    pub max_domain_in_flight_bytes: Option<u64>,
    pub max_domain_concurrency: Option<u32>,
    pub max_domain_local_requests: Option<u32>,
    pub max_domain_remote_requests: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingWorkBudgetBlockReason {
    InFlightBytes,
    InFlightRequests,
    Concurrency,
    DomainInFlightBytes,
    DomainConcurrency,
    DomainLocalRequests,
    DomainRemoteRequests,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum DomainPriorityOverride {
    #[default]
    None,
    EmergencyMaintenanceReserved,
    ControlPlaneReserved,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedAdmissionDiagnostics {
    pub diagnostics: AdmissionDiagnostics,
    pub recorded_at: Timestamp,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SchedulerObservabilitySnapshot {
    pub deferred_work: BTreeMap<String, u32>,
    pub deferred_work_by_domain: BTreeMap<ExecutionDomainPath, u32>,
    pub starved_domains: BTreeMap<ExecutionDomainPath, u32>,
    pub forced_executions: u64,
    pub forced_flushes: u64,
    pub forced_l0_compactions: u64,
    pub budget_blocked_executions: u64,
    pub budget_blocked_executions_by_domain: BTreeMap<ExecutionDomainPath, u64>,
    pub background_delay_events: u64,
    pub background_delay_millis: u64,
    pub background_delay_events_by_domain: BTreeMap<ExecutionDomainPath, u64>,
    pub background_delay_millis_by_domain: BTreeMap<ExecutionDomainPath, u64>,
    pub throttled_writes_by_domain: BTreeMap<ExecutionDomainPath, u64>,
    pub current_admission_diagnostics_by_domain:
        BTreeMap<ExecutionDomainPath, RecordedAdmissionDiagnostics>,
    pub last_non_open_admission_by_domain:
        BTreeMap<ExecutionDomainPath, RecordedAdmissionDiagnostics>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionObservation {
    pub domain: ExecutionDomainPath,
    pub current: RecordedAdmissionDiagnostics,
    pub last_non_open: Option<RecordedAdmissionDiagnostics>,
}

#[derive(Debug)]
pub struct AdmissionObservationReceiver {
    inner: broadcast::Receiver<AdmissionObservation>,
}

impl AdmissionObservationReceiver {
    pub(crate) fn new(inner: broadcast::Receiver<AdmissionObservation>) -> Self {
        Self { inner }
    }

    pub async fn recv(
        &mut self,
    ) -> Result<AdmissionObservation, crate::AdmissionObservationRecvError> {
        match self.inner.recv().await {
            Ok(observation) => Ok(observation),
            Err(broadcast::error::RecvError::Closed) => {
                Err(crate::AdmissionObservationRecvError::Closed)
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(crate::AdmissionObservationRecvError::Lagged(skipped))
            }
        }
    }

    pub fn try_recv(
        &mut self,
    ) -> Result<Option<AdmissionObservation>, crate::AdmissionObservationRecvError> {
        match self.inner.try_recv() {
            Ok(observation) => Ok(Some(observation)),
            Err(broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(broadcast::error::TryRecvError::Closed) => {
                Err(crate::AdmissionObservationRecvError::Closed)
            }
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                Err(crate::AdmissionObservationRecvError::Lagged(skipped))
            }
        }
    }

    pub async fn wait_for<F>(
        &mut self,
        mut predicate: F,
    ) -> Result<AdmissionObservation, crate::AdmissionObservationRecvError>
    where
        F: FnMut(&AdmissionObservation) -> bool,
    {
        loop {
            let observation = self.recv().await?;
            if predicate(&observation) {
                return Ok(observation);
            }
        }
    }
}

#[derive(Debug)]
pub struct SchedulerObservabilitySubscription {
    inner: watch::Receiver<Arc<SchedulerObservabilitySnapshot>>,
}

impl SchedulerObservabilitySubscription {
    pub(crate) fn new(inner: watch::Receiver<Arc<SchedulerObservabilitySnapshot>>) -> Self {
        Self { inner }
    }

    pub fn current(&self) -> SchedulerObservabilitySnapshot {
        self.inner.borrow().as_ref().clone()
    }

    pub async fn changed(
        &mut self,
    ) -> Result<SchedulerObservabilitySnapshot, crate::SubscriptionClosed> {
        self.inner
            .changed()
            .await
            .map_err(|_| crate::SubscriptionClosed)?;
        Ok(self.current())
    }

    /// Waits until the current or next published snapshot satisfies `predicate`.
    pub async fn wait_for<F>(
        &mut self,
        mut predicate: F,
    ) -> Result<SchedulerObservabilitySnapshot, crate::SubscriptionClosed>
    where
        F: FnMut(&SchedulerObservabilitySnapshot) -> bool,
    {
        let snapshot = self.current();
        if predicate(&snapshot) {
            return Ok(snapshot);
        }

        loop {
            let snapshot = self.changed().await?;
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
        }
    }
}

impl Clone for SchedulerObservabilitySubscription {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
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
    last_domain: Option<ExecutionDomainPath>,
    last_table_by_domain: BTreeMap<ExecutionDomainPath, String>,
}

impl Scheduler for RoundRobinScheduler {
    fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
        let tagged = work
            .iter()
            .cloned()
            .map(|work| DomainTaggedWork::new(work, default_scheduler_work_tag()))
            .collect::<Vec<_>>();
        self.on_domain_work_available(&tagged)
    }

    fn on_domain_work_available(
        &self,
        work: &[DomainTaggedWork<PendingWork>],
    ) -> Vec<ScheduleDecision> {
        if work.is_empty() {
            return Vec::new();
        }

        let mut ordered = work.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            tagged_pending_work_priority(left)
                .cmp(&tagged_pending_work_priority(right))
                .then_with(|| left.tag.domain.cmp(&right.tag.domain))
                .then_with(|| left.work.table.cmp(&right.work.table))
                .then_with(|| left.work.level.cmp(&right.work.level))
                .then_with(|| left.work.id.cmp(&right.work.id))
        });

        let top_priority = tagged_pending_work_priority(ordered[0]);
        let top_priority_work = ordered
            .into_iter()
            .filter(|work| tagged_pending_work_priority(work) == top_priority)
            .collect::<Vec<_>>();

        let mut domains = top_priority_work
            .iter()
            .map(|work| work.tag.domain.clone())
            .collect::<Vec<_>>();
        domains.sort_unstable();
        domains.dedup();

        let selected_domain = {
            let mut state = self
                .state
                .lock()
                .expect("round-robin scheduler mutex should not be poisoned");
            let next_index = state
                .last_domain
                .as_ref()
                .and_then(|last| domains.iter().position(|domain| domain > last))
                .unwrap_or(0);
            let domain = domains[next_index].clone();
            state.last_domain = Some(domain.clone());
            domain
        };

        let mut tables = top_priority_work
            .iter()
            .filter(|work| work.tag.domain == selected_domain)
            .map(|work| work.work.table.as_str())
            .collect::<Vec<_>>();
        tables.sort_unstable();
        tables.dedup();

        let selected_table = {
            let mut state = self
                .state
                .lock()
                .expect("round-robin scheduler mutex should not be poisoned");
            let next_index = state
                .last_table_by_domain
                .get(&selected_domain)
                .and_then(|last| tables.iter().position(|table| *table > last.as_str()))
                .unwrap_or(0);
            let table = tables[next_index].to_string();
            state
                .last_table_by_domain
                .insert(selected_domain.clone(), table.clone());
            table
        };

        let selected_work_id = top_priority_work
            .into_iter()
            .filter(|work| work.tag.domain == selected_domain && work.work.table == selected_table)
            .min_by(|left, right| {
                left.work
                    .level
                    .cmp(&right.work.level)
                    .then_with(|| left.work.id.cmp(&right.work.id))
            })
            .map(|work| work.work.id.clone())
            .expect("top-priority work should contain at least one item for the selected table");

        work.iter()
            .map(|work| ScheduleDecision {
                work_id: work.work.id.clone(),
                action: if work.work.id == selected_work_id {
                    ScheduleAction::Execute
                } else {
                    ScheduleAction::Defer
                },
            })
            .collect()
    }

    fn on_flush_pressure_available(
        &self,
        candidates: &[DomainTaggedWork<FlushPressureCandidate>],
    ) -> Vec<ScheduleDecision> {
        if candidates.is_empty() {
            return Vec::new();
        }

        let mut ordered = candidates.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            crate::pressure::flush_candidate_priority_key(&right.work)
                .cmp(&crate::pressure::flush_candidate_priority_key(&left.work))
                .then_with(|| left.tag.domain.cmp(&right.tag.domain))
                .then_with(|| left.work.work.table.cmp(&right.work.work.table))
                .then_with(|| left.work.work.id.cmp(&right.work.work.id))
        });

        let top_priority = crate::pressure::flush_candidate_priority_key(&ordered[0].work);
        let top_priority_work = ordered
            .into_iter()
            .filter(|candidate| {
                crate::pressure::flush_candidate_priority_key(&candidate.work) == top_priority
            })
            .collect::<Vec<_>>();

        let mut domains = top_priority_work
            .iter()
            .map(|candidate| candidate.tag.domain.clone())
            .collect::<Vec<_>>();
        domains.sort_unstable();
        domains.dedup();

        let selected_domain = {
            let mut state = self
                .state
                .lock()
                .expect("round-robin scheduler mutex should not be poisoned");
            let next_index = state
                .last_domain
                .as_ref()
                .and_then(|last| domains.iter().position(|domain| domain > last))
                .unwrap_or(0);
            let domain = domains[next_index].clone();
            state.last_domain = Some(domain.clone());
            domain
        };

        let mut tables = top_priority_work
            .iter()
            .filter(|candidate| candidate.tag.domain == selected_domain)
            .map(|candidate| candidate.work.work.table.as_str())
            .collect::<Vec<_>>();
        tables.sort_unstable();
        tables.dedup();

        let selected_table = {
            let mut state = self
                .state
                .lock()
                .expect("round-robin scheduler mutex should not be poisoned");
            let next_index = state
                .last_table_by_domain
                .get(&selected_domain)
                .and_then(|last| tables.iter().position(|table| *table > last.as_str()))
                .unwrap_or(0);
            let table = tables[next_index].to_string();
            state
                .last_table_by_domain
                .insert(selected_domain.clone(), table.clone());
            table
        };

        let selected_work_id = top_priority_work
            .into_iter()
            .filter(|candidate| {
                candidate.tag.domain == selected_domain
                    && candidate.work.work.table == selected_table
            })
            .min_by(|left, right| left.work.work.id.cmp(&right.work.work.id))
            .map(|candidate| candidate.work.work.id.clone())
            .expect("top-priority flush work should contain at least one item");

        candidates
            .iter()
            .map(|candidate| ScheduleDecision {
                work_id: candidate.work.work.id.clone(),
                action: if candidate.work.work.id == selected_work_id {
                    ScheduleAction::Execute
                } else {
                    ScheduleAction::Defer
                },
            })
            .collect()
    }

    fn should_throttle(&self, _table: &Table, stats: &TableStats) -> ThrottleDecision {
        if let Some(retention) = stats.current_state_retention.as_ref()
            && retention.coordination.backpressure.throttle_writes
        {
            return ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: retention
                    .coordination
                    .backpressure
                    .max_write_bytes_per_second,
                stall: retention.coordination.backpressure.stall_writes,
            };
        }

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

    fn should_throttle_in_domain(
        &self,
        table: &Table,
        stats: &TableStats,
        _tag: &WorkRuntimeTag,
        _domain_budget: Option<&ExecutionDomainBudget>,
    ) -> ThrottleDecision {
        self.should_throttle(table, stats)
    }

    fn admission_decision_in_domain(
        &self,
        table: &Table,
        stats: &TableStats,
        signals: &AdmissionSignals,
        tag: &WorkRuntimeTag,
        domain_budget: Option<&ExecutionDomainBudget>,
    ) -> ThrottleDecision {
        if let Some(retention) = stats.current_state_retention.as_ref()
            && retention.coordination.backpressure.throttle_writes
        {
            return ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: retention
                    .coordination
                    .backpressure
                    .max_write_bytes_per_second,
                stall: retention.coordination.backpressure.stall_writes,
            };
        }

        let diagnostics = self
            .admission_diagnostics(table, stats, signals, tag, domain_budget)
            .unwrap_or_else(|| multi_signal_write_admission(signals));
        match diagnostics.level {
            AdmissionPressureLevel::Open => ThrottleDecision::default(),
            AdmissionPressureLevel::RateLimit => ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: diagnostics.max_write_bytes_per_second,
                stall: false,
            },
            AdmissionPressureLevel::Stall => ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: None,
                stall: true,
            },
        }
    }

    fn admission_diagnostics(
        &self,
        _table: &Table,
        stats: &TableStats,
        signals: &AdmissionSignals,
        _tag: &WorkRuntimeTag,
        _domain_budget: Option<&ExecutionDomainBudget>,
    ) -> Option<AdmissionDiagnostics> {
        if let Some(retention) = stats.current_state_retention.as_ref()
            && retention.coordination.backpressure.throttle_writes
        {
            return Some(AdmissionDiagnostics {
                level: if retention.coordination.backpressure.stall_writes {
                    AdmissionPressureLevel::Stall
                } else {
                    AdmissionPressureLevel::RateLimit
                },
                max_write_bytes_per_second: retention
                    .coordination
                    .backpressure
                    .max_write_bytes_per_second,
                metadata: BTreeMap::from([(
                    "current_state_retention_backpressure".to_string(),
                    JsonValue::Bool(true),
                )]),
                ..AdmissionDiagnostics::default()
            });
        }
        Some(multi_signal_write_admission(signals))
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
        PendingWorkType::CurrentStateRetention => (1, 0),
        PendingWorkType::Compaction => (2, work.level.unwrap_or(u32::MAX)),
        PendingWorkType::Backup => (3, 0),
        PendingWorkType::Offload => (4, 0),
        PendingWorkType::Prefetch => (5, 0),
    }
}

pub(crate) fn pending_work_domain_priority_override(
    work: &DomainTaggedWork<PendingWork>,
) -> DomainPriorityOverride {
    match work.tag.contention_class {
        ContentionClass::ControlPlane => DomainPriorityOverride::ControlPlaneReserved,
        ContentionClass::EmergencyMaintenance => {
            DomainPriorityOverride::EmergencyMaintenanceReserved
        }
        ContentionClass::Recovery | ContentionClass::UserData => DomainPriorityOverride::None,
    }
}

pub(crate) fn apply_domain_budget_overrides(
    budget: &mut PendingWorkBudget,
    work: &DomainTaggedWork<PendingWork>,
    domain_budget: Option<&ExecutionDomainBudget>,
    priority_override: DomainPriorityOverride,
) {
    let Some(domain_budget) = domain_budget else {
        return;
    };

    merge_min_u64(
        &mut budget.max_domain_in_flight_bytes,
        domain_budget.background.max_in_flight_bytes,
    );
    merge_min_u32(
        &mut budget.max_domain_concurrency,
        domain_budget.background.task_slots,
    );
    if pending_work_uses_remote_io(work.work.work_type) {
        merge_min_u32(
            &mut budget.max_domain_remote_requests,
            domain_budget.io.remote_concurrency,
        );
    } else {
        merge_min_u32(
            &mut budget.max_domain_local_requests,
            domain_budget.io.local_concurrency,
        );
    }

    if matches!(
        priority_override,
        DomainPriorityOverride::EmergencyMaintenanceReserved
            | DomainPriorityOverride::ControlPlaneReserved
    ) {
        budget.max_domain_concurrency = Some(budget.max_domain_concurrency.unwrap_or(0).max(1));
        budget.max_domain_in_flight_bytes = Some(
            budget
                .max_domain_in_flight_bytes
                .unwrap_or(0)
                .max(work.work.estimated_bytes.max(1)),
        );
        if pending_work_uses_remote_io(work.work.work_type) {
            budget.max_domain_remote_requests =
                Some(budget.max_domain_remote_requests.unwrap_or(0).max(1));
        } else {
            budget.max_domain_local_requests =
                Some(budget.max_domain_local_requests.unwrap_or(0).max(1));
        }
    }
}

fn pending_work_uses_remote_io(work_type: PendingWorkType) -> bool {
    matches!(
        work_type,
        PendingWorkType::Backup | PendingWorkType::Offload | PendingWorkType::Prefetch
    )
}

fn merge_min_u32(target: &mut Option<u32>, candidate: Option<u32>) {
    *target = match (*target, candidate) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };
}

fn merge_min_u64(target: &mut Option<u64>, candidate: Option<u64>) {
    *target = match (*target, candidate) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };
}

fn default_scheduler_work_tag() -> WorkRuntimeTag {
    WorkRuntimeTag {
        owner: crate::ExecutionDomainOwner::ProcessControl,
        lane: ExecutionLane::UserBackground,
        contention_class: ContentionClass::UserData,
        domain: ExecutionDomainPath::new(["process", "scheduler", "default"]),
        durability_class: DurabilityClass::UserData,
    }
}

fn tagged_pending_work_priority(work: &DomainTaggedWork<PendingWork>) -> (u8, u8, u32) {
    let override_priority = match pending_work_domain_priority_override(work) {
        DomainPriorityOverride::ControlPlaneReserved => 0,
        DomainPriorityOverride::EmergencyMaintenanceReserved => 1,
        DomainPriorityOverride::None => 2,
    };
    let (kind_priority, level_priority) = pending_work_priority(&work.work);
    (override_priority, kind_priority, level_priority)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use serde_json::json;

    use super::{
        DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT, DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT,
        DomainPriorityOverride, PendingWork, PendingWorkType, RoundRobinScheduler, ScheduleAction,
        Scheduler, TableStats, default_scheduler_work_tag,
    };
    use crate::{
        CurrentStateEffectiveMode, CurrentStateRetainedSetSummary,
        CurrentStateRetentionBackpressure, CurrentStateRetentionBackpressureSignal,
        CurrentStateRetentionCoordinationStats, CurrentStateRetentionEvaluationCost,
        CurrentStateRetentionMembershipChanges, CurrentStateRetentionStats,
        CurrentStateRetentionStatus, Db, DbConfig, DbDependencies, DomainTaggedWork,
        ExecutionDomainBudget, ExecutionDomainPath, ExecutionLane, FlushPressureCandidate,
        PressureBudget, PressureBytes, PressureScope, PressureStats, S3Location, SsdConfig,
        StorageConfig, StubClock, StubFileSystem, StubObjectStore, StubRng, Table,
        TieredDurabilityMode, TieredStorageConfig,
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

    fn retention_stats_with_backpressure() -> CurrentStateRetentionStats {
        CurrentStateRetentionStats {
            policy_revision: 7,
            effective_logical_floor: None,
            retained_set: CurrentStateRetainedSetSummary { rows: 2, bytes: 96 },
            membership_changes: CurrentStateRetentionMembershipChanges::default(),
            evaluation_cost: CurrentStateRetentionEvaluationCost::default(),
            reclaimed_rows: 0,
            reclaimed_bytes: 0,
            deferred_rows: 2,
            deferred_bytes: 64,
            coordination: CurrentStateRetentionCoordinationStats {
                logical_rows_pending: 2,
                deferred_physical_rows: 2,
                physical_bytes_pending: 64,
                evaluation_cost: CurrentStateRetentionEvaluationCost::default(),
                backpressure: CurrentStateRetentionBackpressure {
                    signals: vec![
                        CurrentStateRetentionBackpressureSignal::CpuBound,
                        CurrentStateRetentionBackpressureSignal::RewriteCompactionBlocked,
                    ],
                    throttle_writes: true,
                    stall_writes: false,
                    max_write_bytes_per_second: Some(64),
                },
                ..Default::default()
            },
            status: CurrentStateRetentionStatus {
                effective_mode: CurrentStateEffectiveMode::PhysicalReclaim,
                reasons: Vec::new(),
            },
        }
    }

    fn flush_candidate(
        table: &str,
        score: u64,
        forced: bool,
    ) -> DomainTaggedWork<FlushPressureCandidate> {
        let tag = default_scheduler_work_tag();
        let mut metadata = BTreeMap::from([(
            crate::pressure::FLUSH_SCORE_METADATA_KEY.to_string(),
            json!(score),
        )]);
        if forced {
            metadata.insert(
                crate::pressure::FLUSH_FORCE_REASON_METADATA_KEY.to_string(),
                json!("dirty-byte pressure"),
            );
        }

        DomainTaggedWork::new(
            FlushPressureCandidate {
                work: PendingWork {
                    id: format!("flush:{table}"),
                    work_type: PendingWorkType::Flush,
                    table: table.to_string(),
                    level: None,
                    estimated_bytes: 64,
                },
                pressure: PressureStats {
                    scope: PressureScope::Table(table.to_string()),
                    local: PressureBytes {
                        mutable_dirty_bytes: 64,
                        unified_log_pinned_bytes: 64,
                        ..PressureBytes::default()
                    },
                    budget: PressureBudget {
                        mutable_hard_limit_bytes: Some(128),
                        unified_log_hard_limit_bytes: Some(128),
                        ..PressureBudget::default()
                    },
                    ..PressureStats::default()
                },
                estimated_relief: PressureBytes {
                    mutable_dirty_bytes: 64,
                    unified_log_pinned_bytes: 64,
                    ..PressureBytes::default()
                },
                metadata,
            },
            tag,
        )
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
    fn round_robin_scheduler_prefers_higher_pressure_flush_candidates() {
        let scheduler = RoundRobinScheduler::default();
        let candidates = vec![
            flush_candidate("alpha", 512, false),
            flush_candidate("beta", 4_096, false),
            flush_candidate("gamma", 1_024, true),
        ];

        let decisions = scheduler.on_flush_pressure_available(&candidates);
        let executed = decisions
            .iter()
            .find(|decision| decision.action == ScheduleAction::Execute)
            .expect("one flush candidate should be selected");
        assert_eq!(
            executed.work_id, "flush:gamma",
            "forced flush candidates should outrank ordinary scored flushes"
        );
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

    #[test]
    fn round_robin_scheduler_prioritizes_current_state_retention_before_compaction() {
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
                id: "retention:events".to_string(),
                work_type: PendingWorkType::CurrentStateRetention,
                table: "events".to_string(),
                level: None,
                estimated_bytes: 96,
            },
            PendingWork {
                id: "retention:metrics".to_string(),
                work_type: PendingWorkType::CurrentStateRetention,
                table: "metrics".to_string(),
                level: None,
                estimated_bytes: 64,
            },
        ];

        let first = scheduler.on_work_available(&work);
        let first_executed = first
            .iter()
            .find(|decision| decision.action == ScheduleAction::Execute)
            .expect("one current-state retention item should be selected");
        assert_eq!(first_executed.work_id, "retention:events");

        let second = scheduler.on_work_available(&work);
        let second_executed = second
            .iter()
            .find(|decision| decision.action == ScheduleAction::Execute)
            .expect("one current-state retention item should be selected");
        assert_eq!(second_executed.work_id, "retention:metrics");
    }

    #[test]
    fn round_robin_scheduler_honors_current_state_retention_backpressure() {
        let scheduler = RoundRobinScheduler::default();
        let table = make_table("events");
        let decision = scheduler.should_throttle(
            &table,
            &TableStats {
                current_state_retention: Some(retention_stats_with_backpressure()),
                ..TableStats::default()
            },
        );

        assert!(decision.throttle);
        assert!(!decision.stall);
        assert_eq!(decision.max_write_bytes_per_second, Some(64));
    }

    #[test]
    fn round_robin_scheduler_prioritizes_control_plane_domains_before_user_background() {
        let scheduler = RoundRobinScheduler::default();
        let mut background_tag = default_scheduler_work_tag();
        background_tag.domain = ExecutionDomainPath::new(["process", "db", "background"]);
        background_tag.lane = ExecutionLane::UserBackground;

        let mut control_tag = default_scheduler_work_tag();
        control_tag.domain = ExecutionDomainPath::new(["process", "db", "control"]);
        control_tag.lane = ExecutionLane::ControlPlane;
        control_tag.contention_class = crate::ContentionClass::ControlPlane;
        control_tag.durability_class = crate::DurabilityClass::ControlPlane;

        let work = vec![
            DomainTaggedWork::new(
                PendingWork {
                    id: "compaction:events".to_string(),
                    work_type: PendingWorkType::Compaction,
                    table: "events".to_string(),
                    level: Some(0),
                    estimated_bytes: 128,
                },
                background_tag,
            ),
            DomainTaggedWork::new(
                PendingWork {
                    id: "backup:catalog".to_string(),
                    work_type: PendingWorkType::Backup,
                    table: "_internal".to_string(),
                    level: None,
                    estimated_bytes: 64,
                },
                control_tag,
            ),
        ];

        let decisions = scheduler.on_domain_work_available(&work);
        let executed = decisions
            .into_iter()
            .find(|decision| decision.action == ScheduleAction::Execute)
            .expect("one domain-tagged item should be selected");
        assert_eq!(executed.work_id, "backup:catalog");
    }

    #[test]
    fn domain_work_budget_reserves_minimum_progress_for_control_plane_work() {
        let scheduler = RoundRobinScheduler::default();
        let mut control_tag = default_scheduler_work_tag();
        control_tag.domain = ExecutionDomainPath::new(["process", "db", "control"]);
        control_tag.lane = ExecutionLane::ControlPlane;
        control_tag.contention_class = crate::ContentionClass::ControlPlane;
        control_tag.durability_class = crate::DurabilityClass::ControlPlane;
        let work = DomainTaggedWork::new(
            PendingWork {
                id: "backup:catalog".to_string(),
                work_type: PendingWorkType::Backup,
                table: "_internal".to_string(),
                level: None,
                estimated_bytes: 64,
            },
            control_tag,
        );

        let budget = scheduler.domain_work_budget(
            &work,
            &TableStats::default(),
            Some(&ExecutionDomainBudget {
                background: crate::DomainBackgroundBudget {
                    task_slots: Some(0),
                    max_in_flight_bytes: Some(0),
                },
                io: crate::DomainIoBudget {
                    remote_concurrency: Some(0),
                    ..Default::default()
                },
                ..Default::default()
            }),
            DomainPriorityOverride::ControlPlaneReserved,
        );

        assert_eq!(budget.max_domain_concurrency, Some(1));
        assert_eq!(budget.max_domain_remote_requests, Some(1));
        assert_eq!(budget.max_domain_in_flight_bytes, Some(64));
    }
}
