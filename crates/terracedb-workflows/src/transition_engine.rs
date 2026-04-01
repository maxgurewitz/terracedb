use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use terracedb_workflows_core::{
    self as contracts, AppendOnlyWorkflowHistoryOrderer, DeterministicWorkflowWakeupPlanner,
    PassthroughWorkflowVisibilityProjector, WorkflowHistoryOrderer, WorkflowLifecycleRecord,
    WorkflowLifecycleState, WorkflowVisibilityProjector, WorkflowWakeupPlanner,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowWaitCondition {
    Callback {
        callback_id: String,
    },
    Timer {
        timer_id: Vec<u8>,
        fire_at_millis: u64,
    },
    Signal {
        signal: String,
    },
    Wakeup {
        token: String,
    },
    Completion {
        task_id: contracts::WorkflowTaskId,
    },
}

impl WorkflowWaitCondition {
    fn matches_trigger(&self, trigger: &contracts::WorkflowTrigger) -> bool {
        match (self, trigger) {
            (
                Self::Callback { callback_id },
                contracts::WorkflowTrigger::Callback {
                    callback_id: actual,
                    ..
                },
            ) => callback_id == actual,
            (
                Self::Timer {
                    timer_id,
                    fire_at_millis,
                },
                contracts::WorkflowTrigger::Timer {
                    timer_id: actual_timer_id,
                    fire_at_millis: actual_fire_at_millis,
                    ..
                },
            ) => timer_id == actual_timer_id && fire_at_millis == actual_fire_at_millis,
            _ => false,
        }
    }

    fn matches_wakeup(&self, wakeup: &WorkflowInternalWakeup) -> bool {
        match (self, wakeup) {
            (Self::Signal { signal }, WorkflowInternalWakeup::Signal { signal: actual }) => {
                signal == actual
            }
            (Self::Wakeup { token }, WorkflowInternalWakeup::Wakeup { token: actual }) => {
                token == actual
            }
            (
                Self::Completion { task_id },
                WorkflowInternalWakeup::Completion { completed_task_id },
            ) => task_id == completed_task_id,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowWaiterSet {
    conditions: BTreeSet<WorkflowWaitCondition>,
}

impl WorkflowWaiterSet {
    pub fn new(conditions: impl IntoIterator<Item = WorkflowWaitCondition>) -> Self {
        Self {
            conditions: conditions.into_iter().collect(),
        }
    }

    pub fn callback(callback_id: impl Into<String>) -> Self {
        Self::new([WorkflowWaitCondition::Callback {
            callback_id: callback_id.into(),
        }])
    }

    pub fn timer(timer_id: impl Into<Vec<u8>>, fire_at_millis: u64) -> Self {
        Self::new([WorkflowWaitCondition::Timer {
            timer_id: timer_id.into(),
            fire_at_millis,
        }])
    }

    pub fn signal(signal: impl Into<String>) -> Self {
        Self::new([WorkflowWaitCondition::Signal {
            signal: signal.into(),
        }])
    }

    pub fn wakeup(token: impl Into<String>) -> Self {
        Self::new([WorkflowWaitCondition::Wakeup {
            token: token.into(),
        }])
    }

    pub fn completion(task_id: contracts::WorkflowTaskId) -> Self {
        Self::new([WorkflowWaitCondition::Completion { task_id }])
    }

    pub fn insert(&mut self, condition: WorkflowWaitCondition) -> bool {
        self.conditions.insert(condition)
    }

    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    pub fn matches_trigger(&self, trigger: &contracts::WorkflowTrigger) -> bool {
        self.conditions
            .iter()
            .any(|condition| condition.matches_trigger(trigger))
    }

    pub fn matches_wakeup(&self, wakeup: &WorkflowInternalWakeup) -> bool {
        self.conditions
            .iter()
            .any(|condition| condition.matches_wakeup(wakeup))
    }

    pub fn iter(&self) -> impl Iterator<Item = &WorkflowWaitCondition> {
        self.conditions.iter()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRetryState {
    pub attempt: u32,
    pub wake_at_millis: u64,
    pub task_id: contracts::WorkflowTaskId,
    pub timer_id: Vec<u8>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowOwnedTimer {
    pub timer_id: Vec<u8>,
    pub fire_at_millis: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionSnapshot {
    pub state: contracts::WorkflowStateRecord,
    pub attempt: u32,
    pub waiter_set: WorkflowWaiterSet,
    pub retry_state: Option<WorkflowRetryState>,
    pub owned_timers: BTreeMap<Vec<u8>, WorkflowOwnedTimer>,
    pub applied_tasks: BTreeSet<contracts::WorkflowTaskId>,
}

impl WorkflowExecutionSnapshot {
    pub fn new(state: contracts::WorkflowStateRecord) -> Self {
        Self {
            state,
            attempt: 1,
            waiter_set: WorkflowWaiterSet::default(),
            retry_state: None,
            owned_timers: BTreeMap::new(),
            applied_tasks: BTreeSet::new(),
        }
    }

    pub fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    pub fn with_waiter_set(mut self, waiter_set: WorkflowWaiterSet) -> Self {
        self.waiter_set = waiter_set;
        self
    }

    pub fn with_retry_state(mut self, retry_state: WorkflowRetryState) -> Self {
        self.retry_state = Some(retry_state);
        self
    }

    pub fn with_owned_timer(mut self, timer: WorkflowOwnedTimer) -> Self {
        self.owned_timers.insert(timer.timer_id.clone(), timer);
        self
    }

    pub fn with_applied_task(mut self, task_id: contracts::WorkflowTaskId) -> Self {
        self.applied_tasks.insert(task_id);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowExecutorDirective {
    Suspend {
        waiters: WorkflowWaiterSet,
        reason: Option<String>,
    },
    ArmRetry {
        wake_at_millis: u64,
        reason: Option<String>,
    },
    CancelRetry,
    Pause {
        reason: Option<String>,
    },
    Complete,
    Fail {
        error_code: String,
        error_message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkflowTransitionProposal {
    pub output: contracts::WorkflowTransitionOutput,
    pub directives: Vec<WorkflowExecutorDirective>,
}

impl WorkflowTransitionProposal {
    pub fn new(output: contracts::WorkflowTransitionOutput) -> Self {
        Self {
            output,
            directives: Vec::new(),
        }
    }

    pub fn with_directive(mut self, directive: WorkflowExecutorDirective) -> Self {
        self.directives.push(directive);
        self
    }
}

impl From<contracts::WorkflowTransitionOutput> for WorkflowTransitionProposal {
    fn from(output: contracts::WorkflowTransitionOutput) -> Self {
        Self::new(output)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowInternalWakeup {
    Retry {
        attempt: u32,
        wake_at_millis: u64,
    },
    Signal {
        signal: String,
    },
    Wakeup {
        token: String,
    },
    Completion {
        completed_task_id: contracts::WorkflowTaskId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowEffectIntent {
    Outbox {
        entry: contracts::WorkflowOutboxCommand,
    },
    ScheduleTimer {
        timer_id: Vec<u8>,
        fire_at_millis: u64,
        payload: Vec<u8>,
    },
    CancelTimer {
        timer_id: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowPlannedHistoryEvent {
    pub sequence: u64,
    pub event: contracts::WorkflowHistoryEvent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTransitionPlan {
    pub next_snapshot: WorkflowExecutionSnapshot,
    pub history: Vec<WorkflowPlannedHistoryEvent>,
    pub lifecycle: Option<WorkflowLifecycleRecord>,
    pub visibility: Option<contracts::WorkflowVisibilityRecord>,
    pub effect_intents: Vec<WorkflowEffectIntent>,
    pub ready_task_id: Option<contracts::WorkflowTaskId>,
    pub duplicate_task: bool,
    pub stale_wakeup: bool,
}

impl WorkflowTransitionPlan {
    fn no_op(
        snapshot: &WorkflowExecutionSnapshot,
        duplicate_task: bool,
        stale_wakeup: bool,
    ) -> Self {
        Self {
            next_snapshot: snapshot.clone(),
            history: Vec::new(),
            lifecycle: None,
            visibility: None,
            effect_intents: Vec::new(),
            ready_task_id: None,
            duplicate_task,
            stale_wakeup,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum WorkflowTransitionEngineError {
    #[error("workflow run mismatch: expected {expected}, got {received}")]
    RunMismatch {
        expected: contracts::WorkflowRunId,
        received: contracts::WorkflowRunId,
    },
    #[error("workflow bundle mismatch: expected {expected}, got {received}")]
    BundleMismatch {
        expected: contracts::WorkflowBundleId,
        received: contracts::WorkflowBundleId,
    },
    #[error("workflow name mismatch: expected {expected}, got {received}")]
    WorkflowNameMismatch { expected: String, received: String },
    #[error("workflow instance mismatch: expected {expected}, got {received}")]
    InstanceMismatch { expected: String, received: String },
    #[error("workflow lifecycle mismatch: expected {expected:?}, got {received:?}")]
    LifecycleMismatch {
        expected: WorkflowLifecycleState,
        received: WorkflowLifecycleState,
    },
    #[error("workflow history length mismatch: expected {expected}, got {received}")]
    HistoryLenMismatch { expected: u64, received: u64 },
    #[error("workflow attempt mismatch: expected {expected}, got {received}")]
    AttemptMismatch { expected: u32, received: u32 },
}

#[derive(Clone, Debug)]
pub struct WorkflowTransitionEngine<
    O = AppendOnlyWorkflowHistoryOrderer,
    V = PassthroughWorkflowVisibilityProjector,
    W = DeterministicWorkflowWakeupPlanner,
> {
    history_orderer: O,
    visibility_projector: V,
    wakeup_planner: W,
}

impl Default
    for WorkflowTransitionEngine<
        AppendOnlyWorkflowHistoryOrderer,
        PassthroughWorkflowVisibilityProjector,
        DeterministicWorkflowWakeupPlanner,
    >
{
    fn default() -> Self {
        Self::new(
            AppendOnlyWorkflowHistoryOrderer,
            PassthroughWorkflowVisibilityProjector,
            DeterministicWorkflowWakeupPlanner,
        )
    }
}

impl<O, V, W> WorkflowTransitionEngine<O, V, W>
where
    O: WorkflowHistoryOrderer,
    V: WorkflowVisibilityProjector,
    W: WorkflowWakeupPlanner,
{
    pub fn new(history_orderer: O, visibility_projector: V, wakeup_planner: W) -> Self {
        Self {
            history_orderer,
            visibility_projector,
            wakeup_planner,
        }
    }

    pub fn plan_transition(
        &self,
        snapshot: &WorkflowExecutionSnapshot,
        input: contracts::WorkflowTransitionInput,
        proposal: WorkflowTransitionProposal,
        committed_at_millis: u64,
    ) -> Result<WorkflowTransitionPlan, WorkflowTransitionEngineError> {
        self.validate_input(snapshot, &input)?;
        if snapshot.applied_tasks.contains(&input.task_id) {
            return Ok(WorkflowTransitionPlan::no_op(snapshot, true, false));
        }
        let waiter_trigger_match = snapshot.waiter_set.matches_trigger(&input.trigger);
        if !snapshot.waiter_set.is_empty() && !snapshot.waiter_set.matches_trigger(&input.trigger) {
            return Ok(WorkflowTransitionPlan::no_op(snapshot, false, true));
        }

        let mut next_snapshot = snapshot.clone();
        next_snapshot.applied_tasks.insert(input.task_id.clone());
        next_snapshot.state.current_task_id = Some(input.task_id.clone());
        next_snapshot.state.updated_at_millis = committed_at_millis;
        next_snapshot.attempt = input.attempt;
        let mut effect_intents = Vec::new();
        if waiter_trigger_match {
            effect_intents.extend(disarm_waiter_timers(
                &mut next_snapshot,
                &snapshot.waiter_set,
                Some(&input.trigger),
            ));
            next_snapshot.waiter_set = WorkflowWaiterSet::default();
            if let Some(existing) = next_snapshot.retry_state.take() {
                effect_intents.push(WorkflowEffectIntent::CancelTimer {
                    timer_id: existing.timer_id.clone(),
                });
                next_snapshot.owned_timers.remove(&existing.timer_id);
            }
        }

        match &proposal.output.state {
            contracts::WorkflowStateMutation::Unchanged => {}
            contracts::WorkflowStateMutation::Put { state } => {
                next_snapshot.state.state = Some(state.clone());
            }
            contracts::WorkflowStateMutation::Delete => {
                next_snapshot.state.state = None;
            }
        }

        effect_intents.extend(reduce_effect_intents(&proposal.output.commands));
        apply_timer_commands(&mut next_snapshot, &proposal.output.commands);

        let mut lifecycle = proposal.output.lifecycle.unwrap_or({
            if waiter_trigger_match {
                WorkflowLifecycleState::Running
            } else {
                snapshot.state.lifecycle
            }
        });
        let mut lifecycle_reason = None;
        let mut run_failure = None;
        let mut continued_as_new = proposal.output.continue_as_new.clone();

        for directive in proposal.directives {
            match directive {
                WorkflowExecutorDirective::Suspend { waiters, reason } => {
                    next_snapshot.waiter_set = waiters;
                    lifecycle = WorkflowLifecycleState::Suspended;
                    lifecycle_reason = reason;
                }
                WorkflowExecutorDirective::ArmRetry {
                    wake_at_millis,
                    reason,
                } => {
                    if let Some(existing) = next_snapshot.retry_state.take() {
                        effect_intents.push(WorkflowEffectIntent::CancelTimer {
                            timer_id: existing.timer_id.clone(),
                        });
                        next_snapshot.owned_timers.remove(&existing.timer_id);
                    }
                    let retry_attempt = next_snapshot.attempt.saturating_add(1);
                    let retry_task_id = self.wakeup_planner.retry_task_id(
                        &next_snapshot.state.run_id,
                        retry_attempt,
                        wake_at_millis,
                    );
                    let timer_id = retry_timer_id(&retry_task_id);
                    let retry_state = WorkflowRetryState {
                        attempt: retry_attempt,
                        wake_at_millis,
                        task_id: retry_task_id.clone(),
                        timer_id: timer_id.clone(),
                        reason: reason.clone(),
                    };
                    next_snapshot.attempt = retry_attempt;
                    next_snapshot.owned_timers.insert(
                        timer_id.clone(),
                        WorkflowOwnedTimer {
                            timer_id: timer_id.clone(),
                            fire_at_millis: wake_at_millis,
                            payload: Vec::new(),
                        },
                    );
                    next_snapshot.retry_state = Some(retry_state);
                    effect_intents.push(WorkflowEffectIntent::ScheduleTimer {
                        timer_id,
                        fire_at_millis: wake_at_millis,
                        payload: Vec::new(),
                    });
                    lifecycle = WorkflowLifecycleState::RetryWaiting;
                    lifecycle_reason = reason;
                }
                WorkflowExecutorDirective::CancelRetry => {
                    if let Some(existing) = next_snapshot.retry_state.take() {
                        effect_intents.push(WorkflowEffectIntent::CancelTimer {
                            timer_id: existing.timer_id.clone(),
                        });
                        next_snapshot.owned_timers.remove(&existing.timer_id);
                    }
                    if matches!(lifecycle, WorkflowLifecycleState::RetryWaiting) {
                        lifecycle = WorkflowLifecycleState::Scheduled;
                    }
                    lifecycle_reason = Some("retry-cleared".to_string());
                }
                WorkflowExecutorDirective::Pause { reason } => {
                    lifecycle = WorkflowLifecycleState::Paused;
                    lifecycle_reason = reason;
                }
                WorkflowExecutorDirective::Complete => {
                    lifecycle = WorkflowLifecycleState::Completed;
                    lifecycle_reason = Some("completed".to_string());
                }
                WorkflowExecutorDirective::Fail {
                    error_code,
                    error_message,
                } => {
                    lifecycle = WorkflowLifecycleState::Failed;
                    lifecycle_reason = Some(error_code.clone());
                    run_failure = Some((error_code, error_message));
                }
            }
        }

        if continued_as_new.is_some() {
            lifecycle = WorkflowLifecycleState::Completed;
            lifecycle_reason = Some("continue-as-new".to_string());
        }

        if matches!(
            lifecycle,
            WorkflowLifecycleState::Completed | WorkflowLifecycleState::Failed
        ) {
            let pending_waiters = next_snapshot.waiter_set.clone();
            effect_intents.extend(disarm_waiter_timers(
                &mut next_snapshot,
                &pending_waiters,
                None,
            ));
            next_snapshot.waiter_set = WorkflowWaiterSet::default();
            if let Some(existing) = next_snapshot.retry_state.take() {
                effect_intents.push(WorkflowEffectIntent::CancelTimer {
                    timer_id: existing.timer_id.clone(),
                });
                next_snapshot.owned_timers.remove(&existing.timer_id);
            }
        }

        next_snapshot.state.lifecycle = lifecycle;

        let lifecycle_record = if lifecycle != snapshot.state.lifecycle
            || lifecycle_reason.is_some()
            || next_snapshot.attempt != snapshot.attempt
        {
            Some(WorkflowLifecycleRecord {
                run_id: next_snapshot.state.run_id.clone(),
                lifecycle,
                updated_at_millis: committed_at_millis,
                reason: lifecycle_reason.clone(),
                task_id: Some(input.task_id.clone()),
                attempt: next_snapshot.attempt,
            })
        } else {
            None
        };

        let mut history_count = 2u64;
        if lifecycle_record.is_some() {
            history_count += 1;
        }
        if continued_as_new.is_some() {
            history_count += 1;
        }
        if matches!(lifecycle, WorkflowLifecycleState::Completed) {
            history_count += 1;
        }
        if run_failure.is_some() {
            history_count += 1;
        }
        history_count += 1;
        next_snapshot.state.history_len = snapshot.state.history_len.saturating_add(history_count);

        let mut projected_output = proposal.output.clone();
        projected_output.lifecycle = Some(lifecycle);
        let visibility = self
            .visibility_projector
            .project_visibility(&next_snapshot.state, &projected_output);

        let mut sequence = snapshot.state.history_len;
        let mut history = Vec::new();
        push_history_event(
            &self.history_orderer,
            &mut sequence,
            &mut history,
            contracts::WorkflowHistoryEvent::TaskAdmitted {
                task_id: input.task_id.clone(),
                trigger: input.trigger.clone(),
                admitted_at_millis: input.admitted_at_millis,
            },
        );
        push_history_event(
            &self.history_orderer,
            &mut sequence,
            &mut history,
            contracts::WorkflowHistoryEvent::TaskApplied {
                task_id: input.task_id.clone(),
                output: proposal.output.clone(),
                committed_at_millis,
            },
        );
        if let Some(record) = lifecycle_record.clone() {
            push_history_event(
                &self.history_orderer,
                &mut sequence,
                &mut history,
                contracts::WorkflowHistoryEvent::LifecycleChanged { record },
            );
        }
        if let Some(next_run) = continued_as_new.take() {
            push_history_event(
                &self.history_orderer,
                &mut sequence,
                &mut history,
                contracts::WorkflowHistoryEvent::ContinuedAsNew {
                    from_run_id: next_snapshot.state.run_id.clone(),
                    next_run_id: next_run.next_run_id,
                    next_bundle_id: next_run.next_bundle_id,
                    continued_at_millis: committed_at_millis,
                },
            );
        }
        if let Some((error_code, error_message)) = run_failure {
            push_history_event(
                &self.history_orderer,
                &mut sequence,
                &mut history,
                contracts::WorkflowHistoryEvent::RunFailed {
                    run_id: next_snapshot.state.run_id.clone(),
                    failed_at_millis: committed_at_millis,
                    error_code,
                    error_message,
                },
            );
        } else if matches!(lifecycle, WorkflowLifecycleState::Completed) {
            push_history_event(
                &self.history_orderer,
                &mut sequence,
                &mut history,
                contracts::WorkflowHistoryEvent::RunCompleted {
                    run_id: next_snapshot.state.run_id.clone(),
                    completed_at_millis: committed_at_millis,
                },
            );
        }
        push_history_event(
            &self.history_orderer,
            &mut sequence,
            &mut history,
            contracts::WorkflowHistoryEvent::VisibilityUpdated {
                record: visibility.clone(),
            },
        );
        debug_assert_eq!(sequence, next_snapshot.state.history_len);

        Ok(WorkflowTransitionPlan {
            next_snapshot,
            history,
            lifecycle: lifecycle_record,
            visibility: Some(visibility),
            effect_intents,
            ready_task_id: None,
            duplicate_task: false,
            stale_wakeup: false,
        })
    }

    pub fn plan_wakeup(
        &self,
        snapshot: &WorkflowExecutionSnapshot,
        wakeup: WorkflowInternalWakeup,
        committed_at_millis: u64,
    ) -> WorkflowTransitionPlan {
        match wakeup {
            WorkflowInternalWakeup::Retry {
                attempt,
                wake_at_millis,
            } => self.plan_retry_wakeup(snapshot, attempt, wake_at_millis, committed_at_millis),
            wakeup => self.plan_waiter_wakeup(snapshot, &wakeup, committed_at_millis),
        }
    }

    fn plan_retry_wakeup(
        &self,
        snapshot: &WorkflowExecutionSnapshot,
        attempt: u32,
        wake_at_millis: u64,
        committed_at_millis: u64,
    ) -> WorkflowTransitionPlan {
        let Some(retry_state) = snapshot.retry_state.as_ref() else {
            return WorkflowTransitionPlan::no_op(snapshot, false, true);
        };
        if retry_state.attempt != attempt || retry_state.wake_at_millis != wake_at_millis {
            return WorkflowTransitionPlan::no_op(snapshot, false, true);
        }

        let mut next_snapshot = snapshot.clone();
        next_snapshot.retry_state = None;
        next_snapshot.owned_timers.remove(&retry_state.timer_id);
        next_snapshot.attempt = attempt;
        next_snapshot.state.lifecycle = WorkflowLifecycleState::Scheduled;
        next_snapshot.state.current_task_id = Some(retry_state.task_id.clone());
        let effect_intents = disarm_waiter_timers(&mut next_snapshot, &snapshot.waiter_set, None);
        next_snapshot.waiter_set = WorkflowWaiterSet::default();
        next_snapshot.state.updated_at_millis = committed_at_millis;
        next_snapshot.state.history_len = snapshot.state.history_len.saturating_add(2);

        let lifecycle = WorkflowLifecycleRecord {
            run_id: next_snapshot.state.run_id.clone(),
            lifecycle: WorkflowLifecycleState::Scheduled,
            updated_at_millis: committed_at_millis,
            reason: Some("retry-wakeup".to_string()),
            task_id: Some(retry_state.task_id.clone()),
            attempt,
        };
        let projected_output = contracts::WorkflowTransitionOutput {
            lifecycle: Some(WorkflowLifecycleState::Scheduled),
            ..contracts::WorkflowTransitionOutput::default()
        };
        let visibility = self
            .visibility_projector
            .project_visibility(&next_snapshot.state, &projected_output);

        let mut sequence = snapshot.state.history_len;
        let mut history = Vec::new();
        push_history_event(
            &self.history_orderer,
            &mut sequence,
            &mut history,
            contracts::WorkflowHistoryEvent::LifecycleChanged {
                record: lifecycle.clone(),
            },
        );
        push_history_event(
            &self.history_orderer,
            &mut sequence,
            &mut history,
            contracts::WorkflowHistoryEvent::VisibilityUpdated {
                record: visibility.clone(),
            },
        );
        debug_assert_eq!(sequence, next_snapshot.state.history_len);

        WorkflowTransitionPlan {
            next_snapshot,
            history,
            lifecycle: Some(lifecycle),
            visibility: Some(visibility),
            effect_intents,
            ready_task_id: Some(retry_state.task_id.clone()),
            duplicate_task: false,
            stale_wakeup: false,
        }
    }

    fn plan_waiter_wakeup(
        &self,
        snapshot: &WorkflowExecutionSnapshot,
        wakeup: &WorkflowInternalWakeup,
        committed_at_millis: u64,
    ) -> WorkflowTransitionPlan {
        if snapshot.waiter_set.is_empty() || !snapshot.waiter_set.matches_wakeup(wakeup) {
            return WorkflowTransitionPlan::no_op(snapshot, false, true);
        }

        let mut next_snapshot = snapshot.clone();
        let mut effect_intents =
            disarm_waiter_timers(&mut next_snapshot, &snapshot.waiter_set, None);
        next_snapshot.waiter_set = WorkflowWaiterSet::default();
        next_snapshot.state.lifecycle = WorkflowLifecycleState::Scheduled;
        let ready_task_id = waiter_wakeup_task_id(
            &next_snapshot.state.run_id,
            next_snapshot.state.current_task_id.as_ref(),
            wakeup,
        );
        next_snapshot.state.current_task_id = Some(ready_task_id.clone());
        if let Some(existing) = next_snapshot.retry_state.take() {
            effect_intents.push(WorkflowEffectIntent::CancelTimer {
                timer_id: existing.timer_id.clone(),
            });
            next_snapshot.owned_timers.remove(&existing.timer_id);
        }
        next_snapshot.state.updated_at_millis = committed_at_millis;
        next_snapshot.state.history_len = snapshot.state.history_len.saturating_add(2);

        let lifecycle = WorkflowLifecycleRecord {
            run_id: next_snapshot.state.run_id.clone(),
            lifecycle: WorkflowLifecycleState::Scheduled,
            updated_at_millis: committed_at_millis,
            reason: Some("waiter-wakeup".to_string()),
            task_id: next_snapshot.state.current_task_id.clone(),
            attempt: next_snapshot.attempt,
        };
        let projected_output = contracts::WorkflowTransitionOutput {
            lifecycle: Some(WorkflowLifecycleState::Scheduled),
            ..contracts::WorkflowTransitionOutput::default()
        };
        let visibility = self
            .visibility_projector
            .project_visibility(&next_snapshot.state, &projected_output);

        let mut sequence = snapshot.state.history_len;
        let mut history = Vec::new();
        push_history_event(
            &self.history_orderer,
            &mut sequence,
            &mut history,
            contracts::WorkflowHistoryEvent::LifecycleChanged {
                record: lifecycle.clone(),
            },
        );
        push_history_event(
            &self.history_orderer,
            &mut sequence,
            &mut history,
            contracts::WorkflowHistoryEvent::VisibilityUpdated {
                record: visibility.clone(),
            },
        );
        debug_assert_eq!(sequence, next_snapshot.state.history_len);

        WorkflowTransitionPlan {
            next_snapshot,
            history,
            lifecycle: Some(lifecycle),
            visibility: Some(visibility),
            effect_intents,
            ready_task_id: Some(ready_task_id),
            duplicate_task: false,
            stale_wakeup: false,
        }
    }

    fn validate_input(
        &self,
        snapshot: &WorkflowExecutionSnapshot,
        input: &contracts::WorkflowTransitionInput,
    ) -> Result<(), WorkflowTransitionEngineError> {
        if snapshot.state.run_id != input.run_id {
            return Err(WorkflowTransitionEngineError::RunMismatch {
                expected: snapshot.state.run_id.clone(),
                received: input.run_id.clone(),
            });
        }
        if snapshot.state.bundle_id != input.bundle_id {
            return Err(WorkflowTransitionEngineError::BundleMismatch {
                expected: snapshot.state.bundle_id.clone(),
                received: input.bundle_id.clone(),
            });
        }
        if snapshot.state.workflow_name != input.workflow_name {
            return Err(WorkflowTransitionEngineError::WorkflowNameMismatch {
                expected: snapshot.state.workflow_name.clone(),
                received: input.workflow_name.clone(),
            });
        }
        if snapshot.state.instance_id != input.instance_id {
            return Err(WorkflowTransitionEngineError::InstanceMismatch {
                expected: snapshot.state.instance_id.clone(),
                received: input.instance_id.clone(),
            });
        }
        if snapshot.state.lifecycle != input.lifecycle {
            return Err(WorkflowTransitionEngineError::LifecycleMismatch {
                expected: snapshot.state.lifecycle,
                received: input.lifecycle,
            });
        }
        if snapshot.state.history_len != input.history_len {
            return Err(WorkflowTransitionEngineError::HistoryLenMismatch {
                expected: snapshot.state.history_len,
                received: input.history_len,
            });
        }
        if snapshot.attempt != input.attempt {
            return Err(WorkflowTransitionEngineError::AttemptMismatch {
                expected: snapshot.attempt,
                received: input.attempt,
            });
        }
        Ok(())
    }
}

fn reduce_effect_intents(commands: &[contracts::WorkflowCommand]) -> Vec<WorkflowEffectIntent> {
    commands
        .iter()
        .map(|command| match command {
            contracts::WorkflowCommand::Outbox { entry } => WorkflowEffectIntent::Outbox {
                entry: entry.clone(),
            },
            contracts::WorkflowCommand::Timer {
                command:
                    contracts::WorkflowTimerCommand::Schedule {
                        timer_id,
                        fire_at_millis,
                        payload,
                    },
            } => WorkflowEffectIntent::ScheduleTimer {
                timer_id: timer_id.clone(),
                fire_at_millis: *fire_at_millis,
                payload: payload.clone(),
            },
            contracts::WorkflowCommand::Timer {
                command: contracts::WorkflowTimerCommand::Cancel { timer_id },
            } => WorkflowEffectIntent::CancelTimer {
                timer_id: timer_id.clone(),
            },
        })
        .collect()
}

fn apply_timer_commands(
    snapshot: &mut WorkflowExecutionSnapshot,
    commands: &[contracts::WorkflowCommand],
) {
    for command in commands {
        let contracts::WorkflowCommand::Timer { command } = command else {
            continue;
        };
        match command {
            contracts::WorkflowTimerCommand::Schedule {
                timer_id,
                fire_at_millis,
                payload,
            } => {
                snapshot.owned_timers.insert(
                    timer_id.clone(),
                    WorkflowOwnedTimer {
                        timer_id: timer_id.clone(),
                        fire_at_millis: *fire_at_millis,
                        payload: payload.clone(),
                    },
                );
            }
            contracts::WorkflowTimerCommand::Cancel { timer_id } => {
                snapshot.owned_timers.remove(timer_id);
            }
        }
    }
}

fn disarm_waiter_timers(
    snapshot: &mut WorkflowExecutionSnapshot,
    waiter_set: &WorkflowWaiterSet,
    consumed_trigger: Option<&contracts::WorkflowTrigger>,
) -> Vec<WorkflowEffectIntent> {
    let mut effect_intents = Vec::new();
    for condition in waiter_set.iter() {
        let WorkflowWaitCondition::Timer {
            timer_id,
            fire_at_millis,
        } = condition
        else {
            continue;
        };

        snapshot.owned_timers.remove(timer_id);
        let consumed_same_timer = matches!(
            consumed_trigger,
            Some(contracts::WorkflowTrigger::Timer {
                timer_id: trigger_timer_id,
                fire_at_millis: trigger_fire_at,
                ..
            }) if trigger_timer_id == timer_id && *trigger_fire_at == *fire_at_millis
        );
        if !consumed_same_timer {
            effect_intents.push(WorkflowEffectIntent::CancelTimer {
                timer_id: timer_id.clone(),
            });
        }
    }
    effect_intents
}

fn push_history_event<O>(
    orderer: &O,
    sequence: &mut u64,
    history: &mut Vec<WorkflowPlannedHistoryEvent>,
    event: contracts::WorkflowHistoryEvent,
) where
    O: WorkflowHistoryOrderer,
{
    *sequence = orderer.next_history_sequence(*sequence);
    history.push(WorkflowPlannedHistoryEvent {
        sequence: *sequence,
        event,
    });
}

fn retry_timer_id(task_id: &contracts::WorkflowTaskId) -> Vec<u8> {
    format!("__terracedb.retry:{}", task_id.as_str()).into_bytes()
}

fn waiter_wakeup_task_id(
    run_id: &contracts::WorkflowRunId,
    current_task_id: Option<&contracts::WorkflowTaskId>,
    wakeup: &WorkflowInternalWakeup,
) -> contracts::WorkflowTaskId {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(run_id.as_str().as_bytes());
    bytes.push(0xfd);
    if let Some(task_id) = current_task_id {
        bytes.extend_from_slice(task_id.as_str().as_bytes());
    }
    bytes.push(0xfc);
    bytes.extend_from_slice(
        &serde_json::to_vec(wakeup).expect("workflow wakeup should serialize deterministically"),
    );
    contracts::WorkflowTaskId::new(format!("wakeup:{:016x}", stable_hash(&bytes)))
        .expect("planner should always produce a non-empty waiter wakeup task id")
}

fn stable_hash(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
