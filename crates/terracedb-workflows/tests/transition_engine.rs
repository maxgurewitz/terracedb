use std::{collections::BTreeMap, convert::Infallible};

use serde::{Deserialize, Serialize};
use terracedb::{DeterministicRng, Rng as TerraceRng};
use terracedb_fuzz::{GeneratedScenarioHarness, assert_seed_replays, assert_seed_variation};
use terracedb_workflows::{
    contracts::{
        WorkflowBundleId, WorkflowChangeKind, WorkflowCommand, WorkflowContinueAsNew,
        WorkflowExecutionTarget, WorkflowLifecycleState, WorkflowOutboxCommand, WorkflowPayload,
        WorkflowRunId, WorkflowSourceEvent, WorkflowStateMutation, WorkflowStateRecord,
        WorkflowTaskId, WorkflowTransitionInput, WorkflowTransitionOutput, WorkflowTrigger,
        WorkflowVisibilityUpdate,
    },
    transition_engine::{
        WorkflowEffectIntent, WorkflowExecutionSnapshot, WorkflowExecutorDirective,
        WorkflowInternalWakeup, WorkflowOwnedTimer, WorkflowRetryState, WorkflowTransitionEngine,
        WorkflowTransitionProposal, WorkflowWaitCondition, WorkflowWaiterSet,
    },
};

fn workflow_state_record(lifecycle: WorkflowLifecycleState) -> WorkflowStateRecord {
    WorkflowStateRecord {
        run_id: WorkflowRunId::new("run:payments-1").expect("run id"),
        target: WorkflowExecutionTarget::Bundle {
            bundle_id: WorkflowBundleId::new("bundle:payments-v1").expect("bundle id"),
        },
        workflow_name: "payments".to_string(),
        instance_id: "acct-7".to_string(),
        lifecycle,
        current_task_id: None,
        history_len: 1,
        state: Some(WorkflowPayload::bytes("queued")),
        updated_at_millis: 10,
    }
}

fn workflow_snapshot(lifecycle: WorkflowLifecycleState) -> WorkflowExecutionSnapshot {
    WorkflowExecutionSnapshot::new(workflow_state_record(lifecycle))
}

fn callback_trigger(callback_id: &str, response: &[u8]) -> WorkflowTrigger {
    WorkflowTrigger::Callback {
        callback_id: callback_id.to_string(),
        response: response.to_vec(),
    }
}

fn event_trigger(key: &str, value: &str) -> WorkflowTrigger {
    WorkflowTrigger::Event {
        event: WorkflowSourceEvent {
            source_table: "payments_source".to_string(),
            key: key.as_bytes().to_vec(),
            value: Some(WorkflowPayload::bytes(value)),
            cursor: [7; 16],
            sequence: 22,
            kind: WorkflowChangeKind::Put,
            operation_context: None,
        },
    }
}

fn transition_input(
    snapshot: &WorkflowExecutionSnapshot,
    task_id: &str,
    trigger: WorkflowTrigger,
) -> WorkflowTransitionInput {
    WorkflowTransitionInput {
        run_id: snapshot.state.run_id.clone(),
        target: snapshot.state.target.clone(),
        task_id: WorkflowTaskId::new(task_id).expect("task id"),
        workflow_name: snapshot.state.workflow_name.clone(),
        instance_id: snapshot.state.instance_id.clone(),
        lifecycle: snapshot.state.lifecycle,
        history_len: snapshot.state.history_len,
        attempt: snapshot.attempt,
        admitted_at_millis: 15,
        state: snapshot.state.state.clone(),
        trigger,
    }
}

fn retry_state(attempt: u32, wake_at_millis: u64) -> WorkflowRetryState {
    WorkflowRetryState {
        attempt,
        wake_at_millis,
        task_id: WorkflowTaskId::new(format!("retry-task:{attempt}:{wake_at_millis}"))
            .expect("retry task id"),
        timer_id: format!("retry-timer:{attempt}:{wake_at_millis}").into_bytes(),
        reason: Some("backoff".to_string()),
    }
}

#[test]
fn transition_engine_plans_suspend_history_visibility_and_effect_intents() {
    let engine = WorkflowTransitionEngine::default();
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Scheduled);
    let input = transition_input(
        &snapshot,
        "task:acct-7:1",
        event_trigger("acct-7:created", "created"),
    );
    let proposal = WorkflowTransitionProposal::new(WorkflowTransitionOutput {
        state: WorkflowStateMutation::Put {
            state: WorkflowPayload::bytes("processing"),
        },
        lifecycle: Some(WorkflowLifecycleState::Running),
        visibility: Some(WorkflowVisibilityUpdate {
            summary: BTreeMap::from([("stage".to_string(), "processing".to_string())]),
            note: Some("waiting for approval".to_string()),
        }),
        continue_as_new: None,
        commands: vec![
            WorkflowCommand::Outbox {
                entry: WorkflowOutboxCommand {
                    outbox_id: b"payment-created".to_vec(),
                    idempotency_key: "payment-created".to_string(),
                    payload: b"created".to_vec(),
                },
            },
            WorkflowCommand::Timer {
                command: terracedb_workflows::contracts::WorkflowTimerCommand::Schedule {
                    timer_id: b"approval-timeout".to_vec(),
                    fire_at_millis: 50,
                    payload: b"timeout".to_vec(),
                },
            },
        ],
    })
    .with_directive(WorkflowExecutorDirective::Suspend {
        waiters: WorkflowWaiterSet::callback("approval-received"),
        reason: Some("waiting-for-approval".to_string()),
    });

    let plan = engine
        .plan_transition(&snapshot, input.clone(), proposal, 20)
        .expect("plan transition");

    assert!(!plan.duplicate_task);
    assert!(!plan.stale_wakeup);
    assert_eq!(plan.ready_task_id, None);
    assert_eq!(
        plan.next_snapshot.state.current_task_id,
        Some(input.task_id)
    );
    assert_eq!(
        plan.next_snapshot.state.state,
        Some(WorkflowPayload::bytes("processing"))
    );
    assert_eq!(
        plan.next_snapshot.state.lifecycle,
        WorkflowLifecycleState::Suspended
    );
    assert_eq!(plan.next_snapshot.state.history_len, 5);
    assert!(
        plan.next_snapshot
            .waiter_set
            .matches_trigger(&callback_trigger("approval-received", b"ok"))
    );
    assert!(
        plan.next_snapshot
            .owned_timers
            .contains_key(b"approval-timeout".as_slice())
    );
    assert_eq!(plan.effect_intents.len(), 2);
    assert!(matches!(
        &plan.effect_intents[0],
        WorkflowEffectIntent::Outbox { entry } if entry.idempotency_key == "payment-created"
    ));
    assert!(matches!(
        &plan.effect_intents[1],
        WorkflowEffectIntent::ScheduleTimer {
            timer_id,
            fire_at_millis,
            payload,
        } if timer_id == b"approval-timeout" && *fire_at_millis == 50 && payload == b"timeout"
    ));
    assert_eq!(plan.history.len(), 4);
    assert!(matches!(
        &plan.history[0].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::TaskAdmitted { .. }
    ));
    assert!(matches!(
        &plan.history[1].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::TaskApplied { .. }
    ));
    assert!(matches!(
        &plan.history[2].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::LifecycleChanged { record }
            if record.lifecycle == WorkflowLifecycleState::Suspended
    ));
    assert!(matches!(
        &plan.history[3].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::VisibilityUpdated { record }
            if record.lifecycle == WorkflowLifecycleState::Suspended
    ));
}

#[test]
fn transition_engine_suppresses_duplicate_task_ids() {
    let engine = WorkflowTransitionEngine::default();
    let task_id = WorkflowTaskId::new("task:acct-7:1").expect("task id");
    let snapshot =
        workflow_snapshot(WorkflowLifecycleState::Scheduled).with_applied_task(task_id.clone());
    let input = transition_input(
        &snapshot,
        task_id.as_str(),
        callback_trigger("cb-1", b"approved"),
    );

    let plan = engine
        .plan_transition(
            &snapshot,
            input,
            WorkflowTransitionOutput::default().into(),
            20,
        )
        .expect("duplicate plan");

    assert!(plan.duplicate_task);
    assert!(!plan.stale_wakeup);
    assert!(plan.history.is_empty());
    assert!(plan.effect_intents.is_empty());
    assert_eq!(plan.next_snapshot, snapshot);
}

#[test]
fn transition_engine_suppresses_unmatched_waiter_triggers() {
    let engine = WorkflowTransitionEngine::default();
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Suspended)
        .with_waiter_set(WorkflowWaiterSet::callback("approval-received"));
    let input = transition_input(
        &snapshot,
        "task:acct-7:2",
        callback_trigger("wrong-callback", b"approved"),
    );

    let plan = engine
        .plan_transition(
            &snapshot,
            input,
            WorkflowTransitionOutput::default().into(),
            25,
        )
        .expect("stale wakeup plan");

    assert!(!plan.duplicate_task);
    assert!(plan.stale_wakeup);
    assert!(plan.history.is_empty());
    assert_eq!(plan.next_snapshot, snapshot);
}

#[test]
fn transition_engine_arms_retry_and_only_matching_wakeup_resumes() {
    let engine = WorkflowTransitionEngine::default();
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Scheduled);
    let input = transition_input(
        &snapshot,
        "task:acct-7:1",
        callback_trigger("cb-1", b"approved"),
    );
    let armed = engine
        .plan_transition(
            &snapshot,
            input,
            WorkflowTransitionProposal::new(WorkflowTransitionOutput::default()).with_directive(
                WorkflowExecutorDirective::ArmRetry {
                    wake_at_millis: 50,
                    reason: Some("backoff".to_string()),
                },
            ),
            30,
        )
        .expect("arm retry");

    let retry_state = armed
        .next_snapshot
        .retry_state
        .clone()
        .expect("retry state should be stored");
    assert_eq!(
        armed.next_snapshot.state.lifecycle,
        WorkflowLifecycleState::RetryWaiting
    );
    assert_eq!(armed.next_snapshot.attempt, 2);
    assert!(matches!(
        armed.effect_intents.last(),
        Some(WorkflowEffectIntent::ScheduleTimer {
            timer_id,
            fire_at_millis,
            payload,
        }) if timer_id == &retry_state.timer_id && *fire_at_millis == 50 && payload.is_empty()
    ));

    let stale = engine.plan_wakeup(
        &armed.next_snapshot,
        WorkflowInternalWakeup::Retry {
            attempt: 9,
            wake_at_millis: 50,
        },
        40,
    );
    assert!(stale.stale_wakeup);
    assert_eq!(stale.next_snapshot, armed.next_snapshot);

    let resumed = engine.plan_wakeup(
        &armed.next_snapshot,
        WorkflowInternalWakeup::Retry {
            attempt: retry_state.attempt,
            wake_at_millis: retry_state.wake_at_millis,
        },
        50,
    );
    assert!(!resumed.stale_wakeup);
    assert_eq!(resumed.ready_task_id, Some(retry_state.task_id.clone()));
    assert_eq!(
        resumed.next_snapshot.state.lifecycle,
        WorkflowLifecycleState::Scheduled
    );
    assert_eq!(
        resumed.next_snapshot.state.current_task_id,
        Some(retry_state.task_id.clone())
    );
    assert_eq!(resumed.next_snapshot.retry_state, None);
    assert_eq!(resumed.next_snapshot.state.history_len, 7);
    assert_eq!(resumed.history.len(), 2);
    assert!(matches!(
        &resumed.history[0].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::LifecycleChanged { record }
            if record.lifecycle == WorkflowLifecycleState::Scheduled
    ));
}

#[test]
fn transition_engine_waiter_wakeup_mints_a_new_task_id_for_resumed_work() {
    let engine = WorkflowTransitionEngine::default();
    let prior_task_id = WorkflowTaskId::new("task:acct-7:seed").expect("prior task id");
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Suspended)
        .with_waiter_set(WorkflowWaiterSet::signal("approval-signal"))
        .with_applied_task(prior_task_id.clone());
    let snapshot = WorkflowExecutionSnapshot {
        state: WorkflowStateRecord {
            current_task_id: Some(prior_task_id.clone()),
            ..snapshot.state
        },
        ..snapshot
    };

    let resumed = engine.plan_wakeup(
        &snapshot,
        WorkflowInternalWakeup::Signal {
            signal: "approval-signal".to_string(),
        },
        60,
    );

    let resumed_task_id = resumed.ready_task_id.clone().expect("resumed task id");
    assert_ne!(resumed_task_id, prior_task_id);
    assert_eq!(
        resumed.next_snapshot.state.lifecycle,
        WorkflowLifecycleState::Scheduled
    );
    assert_eq!(
        resumed.next_snapshot.state.current_task_id,
        Some(resumed_task_id.clone())
    );
    assert!(matches!(
        &resumed.history[0].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::LifecycleChanged { record }
            if record.task_id.as_ref() == Some(&resumed_task_id)
    ));

    let follow_up = engine
        .plan_transition(
            &resumed.next_snapshot,
            transition_input(
                &resumed.next_snapshot,
                resumed_task_id.as_str(),
                callback_trigger("approval-received", b"approved"),
            ),
            WorkflowTransitionOutput {
                state: WorkflowStateMutation::Put {
                    state: WorkflowPayload::bytes("approved"),
                },
                ..WorkflowTransitionOutput::default()
            }
            .into(),
            65,
        )
        .expect("resumed work should execute");

    assert!(!follow_up.duplicate_task);
    assert!(!follow_up.stale_wakeup);
    assert_eq!(
        follow_up.next_snapshot.state.current_task_id,
        Some(resumed_task_id)
    );
    assert_eq!(
        follow_up.next_snapshot.state.state,
        Some(WorkflowPayload::bytes("approved"))
    );
    assert!(!follow_up.history.is_empty());
}

#[test]
fn transition_engine_consumed_waiter_trigger_cancels_competing_retry_timer() {
    let engine = WorkflowTransitionEngine::default();
    let retry = retry_state(2, 90);
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Suspended)
        .with_waiter_set(WorkflowWaiterSet::callback("approval-received"))
        .with_retry_state(retry.clone())
        .with_owned_timer(WorkflowOwnedTimer {
            timer_id: retry.timer_id.clone(),
            fire_at_millis: retry.wake_at_millis,
            payload: Vec::new(),
        });
    let input = transition_input(
        &snapshot,
        "task:acct-7:approve",
        callback_trigger("approval-received", b"approved"),
    );

    let plan = engine
        .plan_transition(
            &snapshot,
            input,
            WorkflowTransitionOutput {
                state: WorkflowStateMutation::Put {
                    state: WorkflowPayload::bytes("approved"),
                },
                lifecycle: Some(WorkflowLifecycleState::Running),
                ..WorkflowTransitionOutput::default()
            }
            .into(),
            40,
        )
        .expect("matching waiter trigger should plan");

    assert_eq!(plan.next_snapshot.retry_state, None);
    assert!(matches!(
        plan.effect_intents.first(),
        Some(WorkflowEffectIntent::CancelTimer { timer_id }) if timer_id == &retry.timer_id
    ));
}

#[test]
fn transition_engine_consumed_waiter_trigger_defaults_back_to_running() {
    let engine = WorkflowTransitionEngine::default();
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Suspended)
        .with_waiter_set(WorkflowWaiterSet::callback("approval-received"));

    let plan = engine
        .plan_transition(
            &snapshot,
            transition_input(
                &snapshot,
                "task:acct-7:resume",
                callback_trigger("approval-received", b"approved"),
            ),
            WorkflowTransitionOutput::default().into(),
            41,
        )
        .expect("waiter-triggered resume should plan");

    assert_eq!(
        plan.next_snapshot.state.lifecycle,
        WorkflowLifecycleState::Running
    );
}

#[test]
fn transition_engine_waiter_first_persists_retry_cancellation_across_recovery() {
    let engine = WorkflowTransitionEngine::default();
    let retry = retry_state(2, 90);
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Suspended)
        .with_waiter_set(WorkflowWaiterSet::callback("approval-received"))
        .with_retry_state(retry.clone())
        .with_owned_timer(WorkflowOwnedTimer {
            timer_id: retry.timer_id.clone(),
            fire_at_millis: retry.wake_at_millis,
            payload: Vec::new(),
        });

    let waiter_first = engine
        .plan_transition(
            &snapshot,
            transition_input(
                &snapshot,
                "task:acct-7:approve",
                callback_trigger("approval-received", b"approved"),
            ),
            WorkflowTransitionOutput {
                state: WorkflowStateMutation::Put {
                    state: WorkflowPayload::bytes("approved"),
                },
                lifecycle: Some(WorkflowLifecycleState::Running),
                ..WorkflowTransitionOutput::default()
            }
            .into(),
            40,
        )
        .expect("waiter-first path should plan");

    let recovered: WorkflowExecutionSnapshot =
        serde_json::from_slice(&serde_json::to_vec(&waiter_first.next_snapshot).expect("encode"))
            .expect("decode");

    let late_retry = engine.plan_wakeup(
        &recovered,
        WorkflowInternalWakeup::Retry {
            attempt: retry.attempt,
            wake_at_millis: retry.wake_at_millis,
        },
        50,
    );

    assert!(late_retry.stale_wakeup);
    assert_eq!(late_retry.next_snapshot, recovered);
}

#[test]
fn transition_engine_internal_waiter_wakeup_persists_retry_cancellation_across_recovery() {
    let engine = WorkflowTransitionEngine::default();
    let retry = retry_state(2, 90);
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Suspended)
        .with_waiter_set(WorkflowWaiterSet::signal("approval-signal"))
        .with_retry_state(retry.clone())
        .with_owned_timer(WorkflowOwnedTimer {
            timer_id: retry.timer_id.clone(),
            fire_at_millis: retry.wake_at_millis,
            payload: Vec::new(),
        });

    let waiter_first = engine.plan_wakeup(
        &snapshot,
        WorkflowInternalWakeup::Signal {
            signal: "approval-signal".to_string(),
        },
        45,
    );

    let recovered: WorkflowExecutionSnapshot =
        serde_json::from_slice(&serde_json::to_vec(&waiter_first.next_snapshot).expect("encode"))
            .expect("decode");

    let late_retry = engine.plan_wakeup(
        &recovered,
        WorkflowInternalWakeup::Retry {
            attempt: retry.attempt,
            wake_at_millis: retry.wake_at_millis,
        },
        50,
    );

    assert!(late_retry.stale_wakeup);
    assert_eq!(late_retry.next_snapshot, recovered);
}

#[test]
fn transition_engine_waiter_wakeup_snapshot_round_trips_for_recovery() {
    let engine = WorkflowTransitionEngine::default();
    let prior_task_id = WorkflowTaskId::new("task:acct-7:seed").expect("prior task id");
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Suspended)
        .with_waiter_set(WorkflowWaiterSet::wakeup("approval-ready"))
        .with_applied_task(prior_task_id.clone());
    let snapshot = WorkflowExecutionSnapshot {
        state: WorkflowStateRecord {
            current_task_id: Some(prior_task_id),
            ..snapshot.state
        },
        ..snapshot
    };

    let resumed = engine.plan_wakeup(
        &snapshot,
        WorkflowInternalWakeup::Wakeup {
            token: "approval-ready".to_string(),
        },
        70,
    );

    let encoded = serde_json::to_vec(&resumed.next_snapshot).expect("encode resumed snapshot");
    let recovered: WorkflowExecutionSnapshot =
        serde_json::from_slice(&encoded).expect("decode resumed snapshot");
    let recovered_task_id = recovered
        .state
        .current_task_id
        .clone()
        .expect("recovered task id");

    assert_eq!(recovered.state.lifecycle, WorkflowLifecycleState::Scheduled);
    assert_eq!(resumed.ready_task_id, Some(recovered_task_id.clone()));
    assert!(recovered.waiter_set.is_empty());

    let follow_up = engine
        .plan_transition(
            &recovered,
            transition_input(
                &recovered,
                recovered_task_id.as_str(),
                callback_trigger("approval-received", b"approved"),
            ),
            WorkflowTransitionOutput {
                state: WorkflowStateMutation::Put {
                    state: WorkflowPayload::bytes("recovered-approved"),
                },
                ..WorkflowTransitionOutput::default()
            }
            .into(),
            75,
        )
        .expect("recovered wakeup should execute");

    assert!(!follow_up.duplicate_task);
    assert!(!follow_up.stale_wakeup);
    assert_eq!(
        follow_up.next_snapshot.state.state,
        Some(WorkflowPayload::bytes("recovered-approved"))
    );
}

#[test]
fn transition_engine_cancel_retry_leaves_a_schedulable_lifecycle() {
    let engine = WorkflowTransitionEngine::default();
    let retry = retry_state(2, 90);
    let snapshot = workflow_snapshot(WorkflowLifecycleState::RetryWaiting)
        .with_retry_state(retry.clone())
        .with_owned_timer(WorkflowOwnedTimer {
            timer_id: retry.timer_id.clone(),
            fire_at_millis: retry.wake_at_millis,
            payload: Vec::new(),
        });
    let input = transition_input(
        &snapshot,
        "task:acct-7:cancel-retry",
        callback_trigger("cb-1", b"ok"),
    );

    let plan = engine
        .plan_transition(
            &snapshot,
            input,
            WorkflowTransitionProposal::new(WorkflowTransitionOutput::default())
                .with_directive(WorkflowExecutorDirective::CancelRetry),
            95,
        )
        .expect("cancel retry should plan");

    assert_eq!(plan.next_snapshot.retry_state, None);
    assert_eq!(
        plan.next_snapshot.state.lifecycle,
        WorkflowLifecycleState::Scheduled
    );
}

#[test]
fn transition_engine_terminal_transitions_cancel_waiter_owned_timers() {
    let engine = WorkflowTransitionEngine::default();
    let timer_id = b"approval-timeout".to_vec();
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Suspended)
        .with_waiter_set(WorkflowWaiterSet::new([
            WorkflowWaitCondition::Callback {
                callback_id: "approval-received".to_string(),
            },
            WorkflowWaitCondition::Timer {
                timer_id: timer_id.clone(),
                fire_at_millis: 50,
            },
        ]))
        .with_owned_timer(WorkflowOwnedTimer {
            timer_id: timer_id.clone(),
            fire_at_millis: 50,
            payload: b"timeout".to_vec(),
        });

    let plan = engine
        .plan_transition(
            &snapshot,
            transition_input(
                &snapshot,
                "task:acct-7:complete",
                callback_trigger("approval-received", b"approved"),
            ),
            WorkflowTransitionProposal::new(WorkflowTransitionOutput::default())
                .with_directive(WorkflowExecutorDirective::Complete),
            100,
        )
        .expect("terminal transition should plan");

    assert_eq!(
        plan.next_snapshot.state.lifecycle,
        WorkflowLifecycleState::Completed
    );
    assert!(plan.next_snapshot.waiter_set.is_empty());
    assert!(
        !plan
            .next_snapshot
            .owned_timers
            .contains_key(timer_id.as_slice())
    );
    assert!(matches!(
        plan.effect_intents.first(),
        Some(WorkflowEffectIntent::CancelTimer { timer_id: cancelled }) if cancelled == &timer_id
    ));
}

#[test]
fn transition_engine_retry_wakeup_clears_competing_waiters_and_stales_late_signal() {
    let engine = WorkflowTransitionEngine::default();
    let retry = retry_state(3, 110);
    let snapshot = workflow_snapshot(WorkflowLifecycleState::RetryWaiting)
        .with_waiter_set(WorkflowWaiterSet::signal("approval-signal"))
        .with_retry_state(retry.clone());

    let resumed = engine.plan_wakeup(
        &snapshot,
        WorkflowInternalWakeup::Retry {
            attempt: retry.attempt,
            wake_at_millis: retry.wake_at_millis,
        },
        115,
    );

    assert!(resumed.next_snapshot.waiter_set.is_empty());
    let late_signal = engine.plan_wakeup(
        &resumed.next_snapshot,
        WorkflowInternalWakeup::Signal {
            signal: "approval-signal".to_string(),
        },
        120,
    );
    assert!(late_signal.stale_wakeup);
    assert_eq!(late_signal.next_snapshot, resumed.next_snapshot);
}

#[test]
fn transition_engine_records_continue_as_new_edges() {
    let engine = WorkflowTransitionEngine::default();
    let snapshot = workflow_snapshot(WorkflowLifecycleState::Running);
    let input = transition_input(
        &snapshot,
        "task:acct-7:3",
        callback_trigger("cb-finished", b"done"),
    );
    let proposal = WorkflowTransitionProposal::new(WorkflowTransitionOutput {
        state: WorkflowStateMutation::Delete,
        lifecycle: None,
        visibility: None,
        continue_as_new: Some(WorkflowContinueAsNew {
            next_run_id: WorkflowRunId::new("run:payments-2").expect("next run id"),
            next_target: WorkflowExecutionTarget::Bundle {
                bundle_id: WorkflowBundleId::new("bundle:payments-v2").expect("next bundle id"),
            },
            state: Some(WorkflowPayload::bytes("carry-forward")),
        }),
        commands: Vec::new(),
    });

    let plan = engine
        .plan_transition(&snapshot, input, proposal, 80)
        .expect("continue as new");

    assert_eq!(
        plan.next_snapshot.state.lifecycle,
        WorkflowLifecycleState::Completed
    );
    assert_eq!(plan.next_snapshot.state.history_len, 7);
    assert!(matches!(
        &plan.history[2].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::LifecycleChanged { record }
            if record.lifecycle == WorkflowLifecycleState::Completed
    ));
    assert!(matches!(
        &plan.history[3].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::ContinuedAsNew {
            next_run_id, ..
        } if next_run_id.as_str() == "run:payments-2"
    ));
    assert!(matches!(
        &plan.history[4].event,
        terracedb_workflows::contracts::WorkflowHistoryEvent::RunCompleted { .. }
    ));
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RetryCampaignResolution {
    StaleThenMatch,
    RestartThenMatch,
    CancelThenStale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TerminalCampaignDirective {
    Complete,
    ContinueAsNew,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TransitionEngineCampaignScenario {
    seed: u64,
    stale_waiter_before_match: bool,
    restart_before_waiter_match: bool,
    retry_resolution: RetryCampaignResolution,
    duplicate_before_terminal: bool,
    terminal_directive: TerminalCampaignDirective,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TransitionSnapshotProjection {
    run_id: String,
    bundle_id: String,
    lifecycle: WorkflowLifecycleState,
    history_len: u64,
    attempt: u32,
    state: Option<String>,
    current_task_id: Option<String>,
    waiter_conditions: Vec<String>,
    retry_state: Option<String>,
    owned_timers: Vec<String>,
    applied_tasks: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TransitionStepCapture {
    step: String,
    projection: TransitionSnapshotProjection,
    duplicate_task: bool,
    stale_wakeup: bool,
    ready_task_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TransitionEngineCampaignOutcome {
    trace: Vec<TransitionStepCapture>,
    final_projection: TransitionSnapshotProjection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SnapshotRoundtripEnvelope {
    state: WorkflowStateRecord,
    attempt: u32,
    waiter_set: WorkflowWaiterSet,
    retry_state: Option<WorkflowRetryState>,
    owned_timers: Vec<WorkflowOwnedTimer>,
    applied_tasks: Vec<WorkflowTaskId>,
}

#[derive(Clone, Debug)]
struct TransitionShadowModel {
    snapshot: WorkflowExecutionSnapshot,
}

impl TransitionShadowModel {
    fn new() -> Self {
        Self {
            snapshot: workflow_snapshot(WorkflowLifecycleState::Scheduled),
        }
    }

    fn apply_progress(&mut self, task_id: &str, state: &str, history_delta: usize) {
        self.set_applied_task(task_id);
        self.snapshot.state.state = Some(WorkflowPayload::bytes(state));
        self.advance_history(history_delta);
    }

    fn apply_suspend(
        &mut self,
        task_id: &str,
        timer_id: &[u8],
        fire_at_millis: u64,
        history_delta: usize,
    ) {
        self.set_applied_task(task_id);
        self.snapshot.state.lifecycle = WorkflowLifecycleState::Suspended;
        self.snapshot.state.state = Some(WorkflowPayload::bytes("waiting"));
        self.snapshot.waiter_set = WorkflowWaiterSet::new([
            WorkflowWaitCondition::Callback {
                callback_id: "approval-received".to_string(),
            },
            WorkflowWaitCondition::Timer {
                timer_id: timer_id.to_vec(),
                fire_at_millis,
            },
        ]);
        self.snapshot.owned_timers.insert(
            timer_id.to_vec(),
            WorkflowOwnedTimer {
                timer_id: timer_id.to_vec(),
                fire_at_millis,
                payload: b"timeout".to_vec(),
            },
        );
        self.advance_history(history_delta);
    }

    fn consume_waiter(
        &mut self,
        task_id: &str,
        state: &str,
        timer_id: &[u8],
        history_delta: usize,
    ) {
        self.set_applied_task(task_id);
        self.snapshot.state.lifecycle = WorkflowLifecycleState::Running;
        self.snapshot.state.state = Some(WorkflowPayload::bytes(state));
        self.snapshot.waiter_set = WorkflowWaiterSet::default();
        self.snapshot.owned_timers.remove(timer_id);
        self.snapshot.retry_state = None;
        self.advance_history(history_delta);
    }

    fn arm_retry(&mut self, task_id: &str, retry_state: WorkflowRetryState, history_delta: usize) {
        self.set_applied_task(task_id);
        self.snapshot.state.lifecycle = WorkflowLifecycleState::RetryWaiting;
        self.snapshot.state.state = Some(WorkflowPayload::bytes("retry-armed"));
        self.snapshot.attempt = retry_state.attempt;
        self.snapshot.owned_timers.insert(
            retry_state.timer_id.clone(),
            WorkflowOwnedTimer {
                timer_id: retry_state.timer_id.clone(),
                fire_at_millis: retry_state.wake_at_millis,
                payload: Vec::new(),
            },
        );
        self.snapshot.retry_state = Some(retry_state);
        self.advance_history(history_delta);
    }

    fn resolve_retry_wakeup(
        &mut self,
        ready_task_id: &str,
        retry_timer_id: &[u8],
        history_delta: usize,
    ) {
        self.snapshot.state.lifecycle = WorkflowLifecycleState::Scheduled;
        self.snapshot.state.current_task_id =
            Some(WorkflowTaskId::new(ready_task_id).expect("ready task id"));
        self.snapshot.retry_state = None;
        self.snapshot.waiter_set = WorkflowWaiterSet::default();
        self.snapshot.owned_timers.remove(retry_timer_id);
        self.advance_history(history_delta);
    }

    fn follow_retry(&mut self, task_id: &str, state: &str, history_delta: usize) {
        self.set_applied_task(task_id);
        self.snapshot.state.state = Some(WorkflowPayload::bytes(state));
        self.advance_history(history_delta);
    }

    fn cancel_retry(&mut self, task_id: &str, retry_timer_id: &[u8], history_delta: usize) {
        self.set_applied_task(task_id);
        self.snapshot.state.lifecycle = WorkflowLifecycleState::Scheduled;
        self.snapshot.state.state = Some(WorkflowPayload::bytes("retry-cancelled"));
        self.snapshot.retry_state = None;
        self.snapshot.owned_timers.remove(retry_timer_id);
        self.advance_history(history_delta);
    }

    fn complete(&mut self, task_id: &str, state: &str, history_delta: usize) {
        self.set_applied_task(task_id);
        self.snapshot.state.lifecycle = WorkflowLifecycleState::Completed;
        self.snapshot.state.state = Some(WorkflowPayload::bytes(state));
        self.snapshot.waiter_set = WorkflowWaiterSet::default();
        self.snapshot.retry_state = None;
        self.snapshot.owned_timers.clear();
        self.advance_history(history_delta);
    }

    fn continue_as_new(&mut self, task_id: &str, history_delta: usize) {
        self.set_applied_task(task_id);
        self.snapshot.state.lifecycle = WorkflowLifecycleState::Completed;
        self.snapshot.state.state = None;
        self.snapshot.waiter_set = WorkflowWaiterSet::default();
        self.snapshot.retry_state = None;
        self.snapshot.owned_timers.clear();
        self.advance_history(history_delta);
    }

    fn restart_roundtrip(&mut self) {
        self.snapshot = snapshot_roundtrip(&self.snapshot);
    }

    fn projection(&self) -> TransitionSnapshotProjection {
        project_snapshot(&self.snapshot)
    }

    fn set_applied_task(&mut self, task_id: &str) {
        let task_id = WorkflowTaskId::new(task_id).expect("task id");
        self.snapshot.state.current_task_id = Some(task_id.clone());
        self.snapshot.applied_tasks.insert(task_id);
    }

    fn advance_history(&mut self, history_delta: usize) {
        self.snapshot.state.history_len = self
            .snapshot
            .state
            .history_len
            .saturating_add(history_delta as u64);
    }
}

struct TransitionEngineCampaignHarness;

impl GeneratedScenarioHarness for TransitionEngineCampaignHarness {
    type Scenario = TransitionEngineCampaignScenario;
    type Outcome = TransitionEngineCampaignOutcome;
    type Error = Infallible;

    fn generate(&self, seed: u64) -> Self::Scenario {
        let rng = DeterministicRng::seeded(seed);
        Self::Scenario {
            seed,
            stale_waiter_before_match: rng.next_u64() % 2 == 0,
            restart_before_waiter_match: rng.next_u64() % 2 == 0,
            retry_resolution: match rng.next_u64() % 3 {
                0 => RetryCampaignResolution::StaleThenMatch,
                1 => RetryCampaignResolution::RestartThenMatch,
                _ => RetryCampaignResolution::CancelThenStale,
            },
            duplicate_before_terminal: rng.next_u64() % 2 == 0,
            terminal_directive: if rng.next_u64() % 2 == 0 {
                TerminalCampaignDirective::Complete
            } else {
                TerminalCampaignDirective::ContinueAsNew
            },
        }
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        Ok(run_transition_engine_campaign(&scenario))
    }
}

fn run_transition_engine_campaign(
    scenario: &TransitionEngineCampaignScenario,
) -> TransitionEngineCampaignOutcome {
    let engine = WorkflowTransitionEngine::default();
    let waiter_timer_id = b"approval-timeout".to_vec();
    let waiter_fire_at = 90;
    let retry_wake_at = 140;

    let mut shadow = TransitionShadowModel::new();
    let mut snapshot = shadow.snapshot.clone();
    let mut trace = Vec::new();

    let progress = engine
        .plan_transition(
            &snapshot,
            transition_input(
                &snapshot,
                "task:acct-7:progress",
                event_trigger("acct-7:created", "created"),
            ),
            WorkflowTransitionOutput {
                state: WorkflowStateMutation::Put {
                    state: WorkflowPayload::bytes("progressed"),
                },
                ..WorkflowTransitionOutput::default()
            }
            .into(),
            20,
        )
        .expect("initial progress should plan");
    assert_history_append_invariant(
        "initial progress",
        &snapshot,
        &progress.next_snapshot,
        progress.history.len(),
        progress.duplicate_task,
        progress.stale_wakeup,
    );
    shadow.apply_progress("task:acct-7:progress", "progressed", progress.history.len());
    assert_eq!(
        project_snapshot(&progress.next_snapshot),
        shadow.projection(),
        "seed {:#x} initial progress diverged",
        scenario.seed
    );
    snapshot = progress.next_snapshot.clone();
    trace.push(capture_transition_step(
        "progress",
        &progress.next_snapshot,
        progress.duplicate_task,
        progress.stale_wakeup,
        progress.ready_task_id.as_ref(),
    ));

    let suspended = engine
        .plan_transition(
            &snapshot,
            transition_input(
                &snapshot,
                "task:acct-7:suspend",
                callback_trigger("approval-start", b"pending"),
            ),
            WorkflowTransitionProposal::new(WorkflowTransitionOutput {
                state: WorkflowStateMutation::Put {
                    state: WorkflowPayload::bytes("waiting"),
                },
                commands: vec![WorkflowCommand::Timer {
                    command: terracedb_workflows::contracts::WorkflowTimerCommand::Schedule {
                        timer_id: waiter_timer_id.clone(),
                        fire_at_millis: waiter_fire_at,
                        payload: b"timeout".to_vec(),
                    },
                }],
                ..WorkflowTransitionOutput::default()
            })
            .with_directive(WorkflowExecutorDirective::Suspend {
                waiters: WorkflowWaiterSet::new([
                    WorkflowWaitCondition::Callback {
                        callback_id: "approval-received".to_string(),
                    },
                    WorkflowWaitCondition::Timer {
                        timer_id: waiter_timer_id.clone(),
                        fire_at_millis: waiter_fire_at,
                    },
                ]),
                reason: Some("awaiting-approval".to_string()),
            }),
            30,
        )
        .expect("suspend should plan");
    assert_history_append_invariant(
        "suspend",
        &snapshot,
        &suspended.next_snapshot,
        suspended.history.len(),
        suspended.duplicate_task,
        suspended.stale_wakeup,
    );
    shadow.apply_suspend(
        "task:acct-7:suspend",
        &waiter_timer_id,
        waiter_fire_at,
        suspended.history.len(),
    );
    assert_eq!(
        project_snapshot(&suspended.next_snapshot),
        shadow.projection(),
        "seed {:#x} suspend diverged",
        scenario.seed
    );
    snapshot = suspended.next_snapshot.clone();
    trace.push(capture_transition_step(
        "suspend",
        &snapshot,
        suspended.duplicate_task,
        suspended.stale_wakeup,
        suspended.ready_task_id.as_ref(),
    ));

    if scenario.stale_waiter_before_match {
        let stale_waiter = engine
            .plan_transition(
                &snapshot,
                transition_input(
                    &snapshot,
                    "task:acct-7:stale-waiter",
                    callback_trigger("wrong-callback", b"wrong"),
                ),
                WorkflowTransitionOutput::default().into(),
                35,
            )
            .expect("stale waiter should be evaluated");
        assert_history_append_invariant(
            "stale waiter",
            &snapshot,
            &stale_waiter.next_snapshot,
            stale_waiter.history.len(),
            stale_waiter.duplicate_task,
            stale_waiter.stale_wakeup,
        );
        assert_eq!(
            project_snapshot(&stale_waiter.next_snapshot),
            shadow.projection(),
            "seed {:#x} stale waiter should not mutate snapshot",
            scenario.seed
        );
        trace.push(capture_transition_step(
            "stale-waiter",
            &snapshot,
            stale_waiter.duplicate_task,
            stale_waiter.stale_wakeup,
            stale_waiter.ready_task_id.as_ref(),
        ));
    }

    if scenario.restart_before_waiter_match {
        shadow.restart_roundtrip();
        snapshot = snapshot_roundtrip(&snapshot);
        trace.push(TransitionStepCapture {
            step: "restart-before-waiter-match".to_string(),
            projection: project_snapshot(&snapshot),
            duplicate_task: false,
            stale_wakeup: false,
            ready_task_id: None,
        });
    }

    let resumed = engine
        .plan_transition(
            &snapshot,
            transition_input(
                &snapshot,
                "task:acct-7:resume",
                callback_trigger("approval-received", b"approved"),
            ),
            WorkflowTransitionOutput {
                state: WorkflowStateMutation::Put {
                    state: WorkflowPayload::bytes("resumed"),
                },
                ..WorkflowTransitionOutput::default()
            }
            .into(),
            40,
        )
        .expect("matching waiter callback should plan");
    assert_history_append_invariant(
        "resume",
        &snapshot,
        &resumed.next_snapshot,
        resumed.history.len(),
        resumed.duplicate_task,
        resumed.stale_wakeup,
    );
    shadow.consume_waiter(
        "task:acct-7:resume",
        "resumed",
        &waiter_timer_id,
        resumed.history.len(),
    );
    assert_eq!(
        project_snapshot(&resumed.next_snapshot),
        shadow.projection(),
        "seed {:#x} waiter resume diverged",
        scenario.seed
    );
    snapshot = resumed.next_snapshot.clone();
    trace.push(capture_transition_step(
        "resume",
        &snapshot,
        resumed.duplicate_task,
        resumed.stale_wakeup,
        resumed.ready_task_id.as_ref(),
    ));

    let arm_retry = engine
        .plan_transition(
            &snapshot,
            transition_input(
                &snapshot,
                "task:acct-7:retry-arm",
                callback_trigger("cb-arm-retry", b"retry"),
            ),
            WorkflowTransitionProposal::new(WorkflowTransitionOutput {
                state: WorkflowStateMutation::Put {
                    state: WorkflowPayload::bytes("retry-armed"),
                },
                ..WorkflowTransitionOutput::default()
            })
            .with_directive(WorkflowExecutorDirective::ArmRetry {
                wake_at_millis: retry_wake_at,
                reason: Some("backoff".to_string()),
            }),
            50,
        )
        .expect("arm retry should plan");
    let retry_state = arm_retry
        .next_snapshot
        .retry_state
        .clone()
        .expect("retry state should be present");
    assert_history_append_invariant(
        "arm retry",
        &snapshot,
        &arm_retry.next_snapshot,
        arm_retry.history.len(),
        arm_retry.duplicate_task,
        arm_retry.stale_wakeup,
    );
    shadow.arm_retry(
        "task:acct-7:retry-arm",
        retry_state.clone(),
        arm_retry.history.len(),
    );
    assert_eq!(
        project_snapshot(&arm_retry.next_snapshot),
        shadow.projection(),
        "seed {:#x} retry arm diverged",
        scenario.seed
    );
    snapshot = arm_retry.next_snapshot.clone();
    trace.push(capture_transition_step(
        "arm-retry",
        &snapshot,
        arm_retry.duplicate_task,
        arm_retry.stale_wakeup,
        arm_retry.ready_task_id.as_ref(),
    ));

    match scenario.retry_resolution {
        RetryCampaignResolution::StaleThenMatch => {
            let stale_retry = engine.plan_wakeup(
                &snapshot,
                WorkflowInternalWakeup::Retry {
                    attempt: retry_state.attempt.saturating_add(7),
                    wake_at_millis: retry_state.wake_at_millis,
                },
                55,
            );
            assert_history_append_invariant(
                "stale retry",
                &snapshot,
                &stale_retry.next_snapshot,
                stale_retry.history.len(),
                stale_retry.duplicate_task,
                stale_retry.stale_wakeup,
            );
            assert_eq!(
                project_snapshot(&stale_retry.next_snapshot),
                shadow.projection(),
                "seed {:#x} stale retry should not mutate snapshot",
                scenario.seed
            );
            trace.push(capture_transition_step(
                "stale-retry",
                &snapshot,
                stale_retry.duplicate_task,
                stale_retry.stale_wakeup,
                stale_retry.ready_task_id.as_ref(),
            ));

            let resumed_retry = engine.plan_wakeup(
                &snapshot,
                WorkflowInternalWakeup::Retry {
                    attempt: retry_state.attempt,
                    wake_at_millis: retry_state.wake_at_millis,
                },
                60,
            );
            let ready_task_id = resumed_retry
                .ready_task_id
                .clone()
                .expect("matching retry should mint a ready task");
            assert_history_append_invariant(
                "retry wakeup",
                &snapshot,
                &resumed_retry.next_snapshot,
                resumed_retry.history.len(),
                resumed_retry.duplicate_task,
                resumed_retry.stale_wakeup,
            );
            shadow.resolve_retry_wakeup(
                ready_task_id.as_str(),
                &retry_state.timer_id,
                resumed_retry.history.len(),
            );
            assert_eq!(
                project_snapshot(&resumed_retry.next_snapshot),
                shadow.projection(),
                "seed {:#x} retry wakeup diverged",
                scenario.seed
            );
            snapshot = resumed_retry.next_snapshot.clone();
            trace.push(capture_transition_step(
                "retry-wakeup",
                &snapshot,
                resumed_retry.duplicate_task,
                resumed_retry.stale_wakeup,
                resumed_retry.ready_task_id.as_ref(),
            ));

            let retry_follow_up = engine
                .plan_transition(
                    &snapshot,
                    transition_input(
                        &snapshot,
                        ready_task_id.as_str(),
                        event_trigger("acct-7:retried", "retried"),
                    ),
                    WorkflowTransitionOutput {
                        state: WorkflowStateMutation::Put {
                            state: WorkflowPayload::bytes("retried"),
                        },
                        ..WorkflowTransitionOutput::default()
                    }
                    .into(),
                    65,
                )
                .expect("retry follow-up should plan");
            assert_history_append_invariant(
                "retry follow up",
                &snapshot,
                &retry_follow_up.next_snapshot,
                retry_follow_up.history.len(),
                retry_follow_up.duplicate_task,
                retry_follow_up.stale_wakeup,
            );
            shadow.follow_retry(
                ready_task_id.as_str(),
                "retried",
                retry_follow_up.history.len(),
            );
            assert_eq!(
                project_snapshot(&retry_follow_up.next_snapshot),
                shadow.projection(),
                "seed {:#x} retry follow-up diverged",
                scenario.seed
            );
            snapshot = retry_follow_up.next_snapshot.clone();
            trace.push(capture_transition_step(
                "retry-follow-up",
                &snapshot,
                retry_follow_up.duplicate_task,
                retry_follow_up.stale_wakeup,
                retry_follow_up.ready_task_id.as_ref(),
            ));
        }
        RetryCampaignResolution::RestartThenMatch => {
            shadow.restart_roundtrip();
            snapshot = snapshot_roundtrip(&snapshot);
            trace.push(TransitionStepCapture {
                step: "restart-before-retry-match".to_string(),
                projection: project_snapshot(&snapshot),
                duplicate_task: false,
                stale_wakeup: false,
                ready_task_id: None,
            });

            let resumed_retry = engine.plan_wakeup(
                &snapshot,
                WorkflowInternalWakeup::Retry {
                    attempt: retry_state.attempt,
                    wake_at_millis: retry_state.wake_at_millis,
                },
                60,
            );
            let ready_task_id = resumed_retry
                .ready_task_id
                .clone()
                .expect("matching retry should mint a ready task");
            assert_history_append_invariant(
                "retry wakeup after restart",
                &snapshot,
                &resumed_retry.next_snapshot,
                resumed_retry.history.len(),
                resumed_retry.duplicate_task,
                resumed_retry.stale_wakeup,
            );
            shadow.resolve_retry_wakeup(
                ready_task_id.as_str(),
                &retry_state.timer_id,
                resumed_retry.history.len(),
            );
            assert_eq!(
                project_snapshot(&resumed_retry.next_snapshot),
                shadow.projection(),
                "seed {:#x} retry wakeup after restart diverged",
                scenario.seed
            );
            snapshot = resumed_retry.next_snapshot.clone();
            trace.push(capture_transition_step(
                "retry-wakeup-after-restart",
                &snapshot,
                resumed_retry.duplicate_task,
                resumed_retry.stale_wakeup,
                resumed_retry.ready_task_id.as_ref(),
            ));

            let retry_follow_up = engine
                .plan_transition(
                    &snapshot,
                    transition_input(
                        &snapshot,
                        ready_task_id.as_str(),
                        event_trigger("acct-7:retried", "retried"),
                    ),
                    WorkflowTransitionOutput {
                        state: WorkflowStateMutation::Put {
                            state: WorkflowPayload::bytes("retried"),
                        },
                        ..WorkflowTransitionOutput::default()
                    }
                    .into(),
                    65,
                )
                .expect("retry follow-up after restart should plan");
            assert_history_append_invariant(
                "retry follow up after restart",
                &snapshot,
                &retry_follow_up.next_snapshot,
                retry_follow_up.history.len(),
                retry_follow_up.duplicate_task,
                retry_follow_up.stale_wakeup,
            );
            shadow.follow_retry(
                ready_task_id.as_str(),
                "retried",
                retry_follow_up.history.len(),
            );
            assert_eq!(
                project_snapshot(&retry_follow_up.next_snapshot),
                shadow.projection(),
                "seed {:#x} retry follow-up after restart diverged",
                scenario.seed
            );
            snapshot = retry_follow_up.next_snapshot.clone();
            trace.push(capture_transition_step(
                "retry-follow-up-after-restart",
                &snapshot,
                retry_follow_up.duplicate_task,
                retry_follow_up.stale_wakeup,
                retry_follow_up.ready_task_id.as_ref(),
            ));
        }
        RetryCampaignResolution::CancelThenStale => {
            let cancel_retry = engine
                .plan_transition(
                    &snapshot,
                    transition_input(
                        &snapshot,
                        "task:acct-7:retry-cancel",
                        callback_trigger("cb-cancel-retry", b"cancel"),
                    ),
                    WorkflowTransitionProposal::new(WorkflowTransitionOutput {
                        state: WorkflowStateMutation::Put {
                            state: WorkflowPayload::bytes("retry-cancelled"),
                        },
                        ..WorkflowTransitionOutput::default()
                    })
                    .with_directive(WorkflowExecutorDirective::CancelRetry),
                    60,
                )
                .expect("cancel retry should plan");
            assert_history_append_invariant(
                "cancel retry",
                &snapshot,
                &cancel_retry.next_snapshot,
                cancel_retry.history.len(),
                cancel_retry.duplicate_task,
                cancel_retry.stale_wakeup,
            );
            shadow.cancel_retry(
                "task:acct-7:retry-cancel",
                &retry_state.timer_id,
                cancel_retry.history.len(),
            );
            assert_eq!(
                project_snapshot(&cancel_retry.next_snapshot),
                shadow.projection(),
                "seed {:#x} retry cancel diverged",
                scenario.seed
            );
            snapshot = cancel_retry.next_snapshot.clone();
            trace.push(capture_transition_step(
                "cancel-retry",
                &snapshot,
                cancel_retry.duplicate_task,
                cancel_retry.stale_wakeup,
                cancel_retry.ready_task_id.as_ref(),
            ));

            let stale_retry = engine.plan_wakeup(
                &snapshot,
                WorkflowInternalWakeup::Retry {
                    attempt: retry_state.attempt,
                    wake_at_millis: retry_state.wake_at_millis,
                },
                65,
            );
            assert_history_append_invariant(
                "late retry after cancel",
                &snapshot,
                &stale_retry.next_snapshot,
                stale_retry.history.len(),
                stale_retry.duplicate_task,
                stale_retry.stale_wakeup,
            );
            assert_eq!(
                project_snapshot(&stale_retry.next_snapshot),
                shadow.projection(),
                "seed {:#x} late retry after cancel should be stale",
                scenario.seed
            );
            trace.push(capture_transition_step(
                "late-retry-after-cancel",
                &snapshot,
                stale_retry.duplicate_task,
                stale_retry.stale_wakeup,
                stale_retry.ready_task_id.as_ref(),
            ));
        }
    }

    if scenario.duplicate_before_terminal {
        let duplicate_task_id = snapshot
            .state
            .current_task_id
            .clone()
            .expect("current task id should exist before duplicate")
            .to_string();
        let duplicate = engine
            .plan_transition(
                &snapshot,
                transition_input(
                    &snapshot,
                    &duplicate_task_id,
                    callback_trigger("cb-duplicate", b"duplicate"),
                ),
                WorkflowTransitionOutput::default().into(),
                70,
            )
            .expect("duplicate should plan");
        assert_history_append_invariant(
            "duplicate",
            &snapshot,
            &duplicate.next_snapshot,
            duplicate.history.len(),
            duplicate.duplicate_task,
            duplicate.stale_wakeup,
        );
        assert_eq!(
            project_snapshot(&duplicate.next_snapshot),
            shadow.projection(),
            "seed {:#x} duplicate should not mutate snapshot",
            scenario.seed
        );
        trace.push(capture_transition_step(
            "duplicate-before-terminal",
            &snapshot,
            duplicate.duplicate_task,
            duplicate.stale_wakeup,
            duplicate.ready_task_id.as_ref(),
        ));
    }

    let terminal_task = "task:acct-7:terminal";
    let terminal = match scenario.terminal_directive {
        TerminalCampaignDirective::Complete => engine
            .plan_transition(
                &snapshot,
                transition_input(
                    &snapshot,
                    terminal_task,
                    callback_trigger("cb-complete", b"complete"),
                ),
                WorkflowTransitionProposal::new(WorkflowTransitionOutput {
                    state: WorkflowStateMutation::Put {
                        state: WorkflowPayload::bytes("completed"),
                    },
                    ..WorkflowTransitionOutput::default()
                })
                .with_directive(WorkflowExecutorDirective::Complete),
                80,
            )
            .expect("terminal complete should plan"),
        TerminalCampaignDirective::ContinueAsNew => engine
            .plan_transition(
                &snapshot,
                transition_input(
                    &snapshot,
                    terminal_task,
                    callback_trigger("cb-continue", b"continue"),
                ),
                WorkflowTransitionProposal::new(WorkflowTransitionOutput {
                    state: WorkflowStateMutation::Delete,
                    continue_as_new: Some(WorkflowContinueAsNew {
                        next_run_id: WorkflowRunId::new("run:payments-2").expect("next run id"),
                        next_bundle_id: WorkflowBundleId::new("bundle:payments-v2")
                            .expect("next bundle id"),
                        state: Some(WorkflowPayload::bytes("carry-forward")),
                    }),
                    ..WorkflowTransitionOutput::default()
                }),
                80,
            )
            .expect("continue-as-new should plan"),
    };
    assert_history_append_invariant(
        "terminal",
        &snapshot,
        &terminal.next_snapshot,
        terminal.history.len(),
        terminal.duplicate_task,
        terminal.stale_wakeup,
    );
    match scenario.terminal_directive {
        TerminalCampaignDirective::Complete => {
            shadow.complete(terminal_task, "completed", terminal.history.len());
        }
        TerminalCampaignDirective::ContinueAsNew => {
            shadow.continue_as_new(terminal_task, terminal.history.len());
        }
    }
    assert_eq!(
        project_snapshot(&terminal.next_snapshot),
        shadow.projection(),
        "seed {:#x} terminal transition diverged",
        scenario.seed
    );
    snapshot = terminal.next_snapshot.clone();
    trace.push(capture_transition_step(
        "terminal",
        &snapshot,
        terminal.duplicate_task,
        terminal.stale_wakeup,
        terminal.ready_task_id.as_ref(),
    ));

    let late_retry = engine.plan_wakeup(
        &snapshot,
        WorkflowInternalWakeup::Retry {
            attempt: retry_state.attempt,
            wake_at_millis: retry_state.wake_at_millis,
        },
        90,
    );
    assert_history_append_invariant(
        "late retry after terminal",
        &snapshot,
        &late_retry.next_snapshot,
        late_retry.history.len(),
        late_retry.duplicate_task,
        late_retry.stale_wakeup,
    );
    assert_eq!(
        project_snapshot(&late_retry.next_snapshot),
        shadow.projection(),
        "seed {:#x} late retry after terminal should not mutate snapshot",
        scenario.seed
    );
    trace.push(capture_transition_step(
        "late-retry-after-terminal",
        &snapshot,
        late_retry.duplicate_task,
        late_retry.stale_wakeup,
        late_retry.ready_task_id.as_ref(),
    ));

    TransitionEngineCampaignOutcome {
        final_projection: project_snapshot(&snapshot),
        trace,
    }
}

fn assert_history_append_invariant(
    label: &str,
    before: &WorkflowExecutionSnapshot,
    after: &WorkflowExecutionSnapshot,
    history_delta: usize,
    duplicate_task: bool,
    stale_wakeup: bool,
) {
    let expected = if duplicate_task || stale_wakeup {
        before.state.history_len
    } else {
        before
            .state
            .history_len
            .saturating_add(history_delta as u64)
    };
    assert_eq!(
        after.state.history_len, expected,
        "{label} should preserve append-only history accounting"
    );
}

fn snapshot_roundtrip(snapshot: &WorkflowExecutionSnapshot) -> WorkflowExecutionSnapshot {
    let envelope = SnapshotRoundtripEnvelope {
        state: snapshot.state.clone(),
        attempt: snapshot.attempt,
        waiter_set: snapshot.waiter_set.clone(),
        retry_state: snapshot.retry_state.clone(),
        owned_timers: snapshot.owned_timers.values().cloned().collect(),
        applied_tasks: snapshot.applied_tasks.iter().cloned().collect(),
    };
    let restored: SnapshotRoundtripEnvelope =
        serde_json::from_slice(&serde_json::to_vec(&envelope).expect("encode snapshot envelope"))
            .expect("decode snapshot envelope");

    let mut owned_timers = BTreeMap::new();
    for timer in restored.owned_timers {
        owned_timers.insert(timer.timer_id.clone(), timer);
    }

    WorkflowExecutionSnapshot {
        state: restored.state,
        attempt: restored.attempt,
        waiter_set: restored.waiter_set,
        retry_state: restored.retry_state,
        owned_timers,
        applied_tasks: restored.applied_tasks.into_iter().collect(),
    }
}

fn project_snapshot(snapshot: &WorkflowExecutionSnapshot) -> TransitionSnapshotProjection {
    TransitionSnapshotProjection {
        run_id: snapshot.state.run_id.to_string(),
        bundle_id: snapshot.state.bundle_id.to_string(),
        lifecycle: snapshot.state.lifecycle,
        history_len: snapshot.state.history_len,
        attempt: snapshot.attempt,
        state: snapshot.state.state.as_ref().map(payload_bytes_to_string),
        current_task_id: snapshot
            .state
            .current_task_id
            .as_ref()
            .map(ToString::to_string),
        waiter_conditions: snapshot
            .waiter_set
            .iter()
            .map(wait_condition_signature)
            .collect(),
        retry_state: snapshot.retry_state.as_ref().map(retry_state_signature),
        owned_timers: snapshot
            .owned_timers
            .values()
            .map(owned_timer_signature)
            .collect(),
        applied_tasks: snapshot
            .applied_tasks
            .iter()
            .map(ToString::to_string)
            .collect(),
    }
}

fn capture_transition_step(
    step: &str,
    snapshot: &WorkflowExecutionSnapshot,
    duplicate_task: bool,
    stale_wakeup: bool,
    ready_task_id: Option<&WorkflowTaskId>,
) -> TransitionStepCapture {
    TransitionStepCapture {
        step: step.to_string(),
        projection: project_snapshot(snapshot),
        duplicate_task,
        stale_wakeup,
        ready_task_id: ready_task_id.map(ToString::to_string),
    }
}

fn payload_bytes_to_string(payload: &WorkflowPayload) -> String {
    String::from_utf8(payload.bytes.clone()).expect("payload should be utf-8 in tests")
}

fn wait_condition_signature(condition: &WorkflowWaitCondition) -> String {
    match condition {
        WorkflowWaitCondition::Callback { callback_id } => format!("callback:{callback_id}"),
        WorkflowWaitCondition::Timer {
            timer_id,
            fire_at_millis,
        } => format!(
            "timer:{}@{}",
            String::from_utf8_lossy(timer_id),
            fire_at_millis
        ),
        WorkflowWaitCondition::Signal { signal } => format!("signal:{signal}"),
        WorkflowWaitCondition::Wakeup { token } => format!("wakeup:{token}"),
        WorkflowWaitCondition::Completion { task_id } => format!("completion:{task_id}"),
    }
}

fn retry_state_signature(retry: &WorkflowRetryState) -> String {
    format!(
        "attempt={}@{}:{}",
        retry.attempt,
        retry.wake_at_millis,
        String::from_utf8_lossy(&retry.timer_id)
    )
}

fn owned_timer_signature(timer: &WorkflowOwnedTimer) -> String {
    format!(
        "{}@{}:{}",
        String::from_utf8_lossy(&timer.timer_id),
        timer.fire_at_millis,
        String::from_utf8_lossy(&timer.payload)
    )
}

fn assert_transition_engine_cross_cutting_campaign_seed(seed: u64) {
    let harness = TransitionEngineCampaignHarness;
    let replay = assert_seed_replays(&harness, seed)
        .expect("cross-cutting transition-engine seed should replay identically");

    assert!(
        replay
            .outcome
            .trace
            .iter()
            .any(|step| step.step.contains("retry")),
        "seed {seed:#x} should retain retry-path trace metadata"
    );
    assert!(
        replay
            .outcome
            .trace
            .iter()
            .any(|step| step.step == "terminal"),
        "seed {seed:#x} should retain terminal-step metadata"
    );
    assert_eq!(replay.outcome.final_projection.run_id, "run:payments-1");
    assert!(
        replay
            .outcome
            .final_projection
            .bundle_id
            .starts_with("bundle:payments"),
        "seed {seed:#x} should preserve bundle identity for replay"
    );
}

#[test]
fn transition_engine_cross_cutting_campaign_seed_0x11301_replays_and_preserves_metadata() {
    assert_transition_engine_cross_cutting_campaign_seed(0x11301);
}

#[test]
fn transition_engine_cross_cutting_campaign_seed_0x11302_replays_and_preserves_metadata() {
    assert_transition_engine_cross_cutting_campaign_seed(0x11302);
}

#[test]
fn transition_engine_cross_cutting_campaign_seed_0x11303_replays_and_preserves_metadata() {
    assert_transition_engine_cross_cutting_campaign_seed(0x11303);
}

#[test]
fn transition_engine_cross_cutting_campaign_seed_variation_changes_wait_retry_shape() {
    let harness = TransitionEngineCampaignHarness;

    let _ = assert_seed_replays(&harness, 0x11301)
        .expect("same transition-engine seed should replay identically");
    let _ = assert_seed_variation(&harness, 0x11301, 0x11302, |left, right| {
        left.scenario != right.scenario || left.outcome.trace != right.outcome.trace
    })
    .expect("distinct transition-engine seeds should vary");
}
