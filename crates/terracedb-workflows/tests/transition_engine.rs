use std::collections::BTreeMap;

use terracedb_workflows::{
    contracts::{
        WorkflowBundleId, WorkflowChangeKind, WorkflowCommand, WorkflowContinueAsNew,
        WorkflowLifecycleState, WorkflowOutboxCommand, WorkflowPayload, WorkflowRunId,
        WorkflowSourceEvent, WorkflowStateMutation, WorkflowStateRecord, WorkflowTaskId,
        WorkflowTransitionInput, WorkflowTransitionOutput, WorkflowTrigger,
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
        bundle_id: WorkflowBundleId::new("bundle:payments-v1").expect("bundle id"),
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
        bundle_id: snapshot.state.bundle_id.clone(),
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
            next_bundle_id: WorkflowBundleId::new("bundle:payments-v2").expect("next bundle id"),
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
