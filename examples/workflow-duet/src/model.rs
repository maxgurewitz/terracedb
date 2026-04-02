use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use terracedb_workflows::{
    WORKFLOW_RUNTIME_SURFACE_LABEL, WORKFLOW_RUNTIME_SURFACE_STATE_OUTBOX_TIMERS_V1,
    contracts::{
        WorkflowBundleId, WorkflowBundleKind, WorkflowBundleMetadata, WorkflowCommand,
        WorkflowLifecycleState, WorkflowOutboxCommand, WorkflowPayload, WorkflowTaskError,
        WorkflowTimerCommand, WorkflowTransitionInput, WorkflowTransitionOutput, WorkflowTrigger,
        WorkflowVisibilityUpdate,
    },
};
use terracedb_workflows_sandbox::WORKFLOW_TASK_V1_ABI;

pub const NATIVE_WORKFLOW_NAME: &str = "workflow-duet-native";
pub const SANDBOX_WORKFLOW_NAME: &str = "workflow-duet-sandbox";
pub const NATIVE_BUNDLE_ID: &str = "native:workflow-duet:review:v1";
pub const SANDBOX_BUNDLE_ID: &str = "bundle:workflow-duet:review-sandbox:v1";
pub const SANDBOX_MODULE_PATH: &str = "/workspace/review_workflow.js";
pub const START_CALLBACK_ID: &str = "start";
pub const APPROVE_CALLBACK_ID: &str = "approve";
pub const RETRY_DELAY_MILLIS: u64 = 10;
pub const APPROVAL_TIMEOUT_MILLIS: u64 = 40;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReviewStage {
    RetryBackoff,
    WaitingApproval,
    Approved,
    TimedOut,
}

impl ReviewStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RetryBackoff => "retry-backoff",
            Self::WaitingApproval => "waiting-approval",
            Self::Approved => "approved",
            Self::TimedOut => "timed-out",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewState {
    pub stage: ReviewStage,
    pub attempt: u32,
    pub started_at_millis: u64,
    pub last_updated_at_millis: u64,
    pub waiting_for_callback: Option<String>,
    pub deadline_millis: Option<u64>,
    pub last_trigger: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewOutboxMessage {
    pub workflow: String,
    pub instance_id: String,
    pub action: String,
    pub attempt: u32,
    pub trigger: String,
}

pub fn native_bundle() -> WorkflowBundleMetadata {
    WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new(NATIVE_BUNDLE_ID).expect("native bundle id"),
        workflow_name: NATIVE_WORKFLOW_NAME.to_string(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "terracedb-example-workflow-duet/native-review@v1".to_string(),
        },
        created_at_millis: 1,
        labels: runtime_labels("native"),
    }
}

pub fn sandbox_bundle() -> WorkflowBundleMetadata {
    WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new(SANDBOX_BUNDLE_ID).expect("sandbox bundle id"),
        workflow_name: SANDBOX_WORKFLOW_NAME.to_string(),
        kind: WorkflowBundleKind::Sandbox {
            abi: WORKFLOW_TASK_V1_ABI.to_string(),
            module: SANDBOX_MODULE_PATH.to_string(),
            entrypoint: "default".to_string(),
        },
        created_at_millis: 1,
        labels: runtime_labels("sandbox"),
    }
}

pub fn decode_review_state(
    payload: Option<&WorkflowPayload>,
) -> Result<Option<ReviewState>, WorkflowTaskError> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    if payload.encoding != "application/octet-stream" {
        return Err(WorkflowTaskError::invalid_contract(format!(
            "workflow-duet state must use application/octet-stream, got {}",
            payload.encoding
        )));
    }
    serde_json::from_slice(&payload.bytes)
        .map(Some)
        .map_err(|error| WorkflowTaskError::serialization("workflow-duet state", error))
}

pub fn encode_review_state(state: &ReviewState) -> Result<WorkflowPayload, WorkflowTaskError> {
    serde_json::to_vec(state)
        .map(WorkflowPayload::bytes)
        .map_err(|error| WorkflowTaskError::serialization("workflow-duet state", error))
}

pub fn review_transition_output(
    workflow_name: &str,
    input: &WorkflowTransitionInput,
) -> Result<WorkflowTransitionOutput, WorkflowTaskError> {
    let current = decode_review_state(input.state.as_ref())?;
    let trigger_label = trigger_label(&input.trigger);

    match current {
        None => handle_start_transition(workflow_name, input, &trigger_label),
        Some(state) => handle_existing_transition(workflow_name, input, state, &trigger_label),
    }
}

pub fn retry_timer_id(instance_id: &str) -> Vec<u8> {
    format!("{instance_id}:retry").into_bytes()
}

pub fn approval_timeout_timer_id(instance_id: &str) -> Vec<u8> {
    format!("{instance_id}:approval-timeout").into_bytes()
}

fn handle_start_transition(
    workflow_name: &str,
    input: &WorkflowTransitionInput,
    trigger_label: &str,
) -> Result<WorkflowTransitionOutput, WorkflowTaskError> {
    let WorkflowTrigger::Callback { callback_id, .. } = &input.trigger else {
        return Err(WorkflowTaskError::invalid_contract(
            "workflow-duet instances must start from callback:start",
        ));
    };
    if callback_id != START_CALLBACK_ID {
        return Err(WorkflowTaskError::invalid_contract(format!(
            "workflow-duet instances must start from callback:{START_CALLBACK_ID}"
        )));
    }

    let next_state = ReviewState {
        stage: ReviewStage::RetryBackoff,
        attempt: 1,
        started_at_millis: input.admitted_at_millis,
        last_updated_at_millis: input.admitted_at_millis,
        waiting_for_callback: None,
        deadline_millis: None,
        last_trigger: trigger_label.to_string(),
    };
    let retry_at = input.admitted_at_millis.saturating_add(RETRY_DELAY_MILLIS);
    build_transition(
        workflow_name,
        &input.instance_id,
        next_state,
        Some(WorkflowLifecycleState::Running),
        vec![
            outbox_command(
                workflow_name,
                &input.instance_id,
                "requested-check",
                1,
                trigger_label,
            )?,
            schedule_timer_command(retry_timer_id(&input.instance_id), retry_at, "retry"),
        ],
    )
}

fn handle_existing_transition(
    workflow_name: &str,
    input: &WorkflowTransitionInput,
    state: ReviewState,
    trigger_label: &str,
) -> Result<WorkflowTransitionOutput, WorkflowTaskError> {
    match (&state.stage, &input.trigger) {
        (
            ReviewStage::RetryBackoff,
            WorkflowTrigger::Timer {
                timer_id,
                fire_at_millis,
                ..
            },
        ) if *timer_id == retry_timer_id(&input.instance_id) => {
            let deadline = fire_at_millis.saturating_add(APPROVAL_TIMEOUT_MILLIS);
            let next_state = ReviewState {
                stage: ReviewStage::WaitingApproval,
                attempt: state.attempt.saturating_add(1),
                started_at_millis: state.started_at_millis,
                last_updated_at_millis: input.admitted_at_millis,
                waiting_for_callback: Some(APPROVE_CALLBACK_ID.to_string()),
                deadline_millis: Some(deadline),
                last_trigger: trigger_label.to_string(),
            };
            build_transition(
                workflow_name,
                &input.instance_id,
                next_state.clone(),
                Some(WorkflowLifecycleState::Running),
                vec![
                    outbox_command(
                        workflow_name,
                        &input.instance_id,
                        "requested-approval",
                        next_state.attempt,
                        trigger_label,
                    )?,
                    schedule_timer_command(
                        approval_timeout_timer_id(&input.instance_id),
                        deadline,
                        "approval-timeout",
                    ),
                ],
            )
        }
        (ReviewStage::WaitingApproval, WorkflowTrigger::Callback { callback_id, .. })
            if callback_id == APPROVE_CALLBACK_ID =>
        {
            let next_state = ReviewState {
                stage: ReviewStage::Approved,
                attempt: state.attempt,
                started_at_millis: state.started_at_millis,
                last_updated_at_millis: input.admitted_at_millis,
                waiting_for_callback: None,
                deadline_millis: None,
                last_trigger: trigger_label.to_string(),
            };
            build_transition(
                workflow_name,
                &input.instance_id,
                next_state.clone(),
                Some(WorkflowLifecycleState::Completed),
                vec![
                    cancel_timer_command(approval_timeout_timer_id(&input.instance_id)),
                    outbox_command(
                        workflow_name,
                        &input.instance_id,
                        "approved",
                        next_state.attempt,
                        trigger_label,
                    )?,
                ],
            )
        }
        (ReviewStage::WaitingApproval, WorkflowTrigger::Timer { timer_id, .. })
            if *timer_id == approval_timeout_timer_id(&input.instance_id) =>
        {
            let next_state = ReviewState {
                stage: ReviewStage::TimedOut,
                attempt: state.attempt,
                started_at_millis: state.started_at_millis,
                last_updated_at_millis: input.admitted_at_millis,
                waiting_for_callback: None,
                deadline_millis: None,
                last_trigger: trigger_label.to_string(),
            };
            build_transition(
                workflow_name,
                &input.instance_id,
                next_state.clone(),
                Some(WorkflowLifecycleState::Failed),
                vec![outbox_command(
                    workflow_name,
                    &input.instance_id,
                    "timed-out",
                    next_state.attempt,
                    trigger_label,
                )?],
            )
        }
        _ => Ok(WorkflowTransitionOutput {
            state: terracedb_workflows::contracts::WorkflowStateMutation::Unchanged,
            lifecycle: None,
            visibility: Some(visibility_update(workflow_name, &state)),
            continue_as_new: None,
            commands: Vec::new(),
        }),
    }
}

fn build_transition(
    workflow_name: &str,
    instance_id: &str,
    state: ReviewState,
    lifecycle: Option<WorkflowLifecycleState>,
    commands: Vec<WorkflowCommand>,
) -> Result<WorkflowTransitionOutput, WorkflowTaskError> {
    Ok(WorkflowTransitionOutput {
        state: terracedb_workflows::contracts::WorkflowStateMutation::Put {
            state: encode_review_state(&state)?,
        },
        lifecycle,
        visibility: Some(visibility_update(workflow_name, &state)),
        continue_as_new: None,
        commands: {
            let mut commands = commands;
            if state.stage == ReviewStage::RetryBackoff {
                commands.insert(
                    0,
                    outbox_command(
                        workflow_name,
                        instance_id,
                        "accepted-start",
                        state.attempt,
                        &state.last_trigger,
                    )?,
                );
            }
            commands
        },
    })
}

fn visibility_update(workflow_name: &str, state: &ReviewState) -> WorkflowVisibilityUpdate {
    let mut summary = BTreeMap::from([
        ("status".to_string(), state.stage.as_str().to_string()),
        ("attempt".to_string(), state.attempt.to_string()),
        ("workflow".to_string(), workflow_name.to_string()),
        (
            "waiting-for".to_string(),
            state
                .waiting_for_callback
                .clone()
                .unwrap_or_else(|| "-".to_string()),
        ),
    ]);
    if let Some(deadline) = state.deadline_millis {
        summary.insert("deadline-millis".to_string(), deadline.to_string());
    }
    WorkflowVisibilityUpdate {
        summary,
        note: Some(format!(
            "{} via {}",
            state.stage.as_str(),
            state.last_trigger
        )),
    }
}

fn outbox_command(
    workflow_name: &str,
    instance_id: &str,
    action: &str,
    attempt: u32,
    trigger: &str,
) -> Result<WorkflowCommand, WorkflowTaskError> {
    let key = format!("{instance_id}:{action}:{attempt}");
    let payload = serde_json::to_vec(&ReviewOutboxMessage {
        workflow: workflow_name.to_string(),
        instance_id: instance_id.to_string(),
        action: action.to_string(),
        attempt,
        trigger: trigger.to_string(),
    })
    .map_err(|error| WorkflowTaskError::serialization("workflow-duet outbox payload", error))?;
    Ok(WorkflowCommand::Outbox {
        entry: WorkflowOutboxCommand {
            outbox_id: key.as_bytes().to_vec(),
            idempotency_key: key,
            payload,
        },
    })
}

fn schedule_timer_command(
    timer_id: Vec<u8>,
    fire_at_millis: u64,
    payload: &str,
) -> WorkflowCommand {
    WorkflowCommand::Timer {
        command: WorkflowTimerCommand::Schedule {
            timer_id,
            fire_at_millis,
            payload: payload.as_bytes().to_vec(),
        },
    }
}

fn cancel_timer_command(timer_id: Vec<u8>) -> WorkflowCommand {
    WorkflowCommand::Timer {
        command: WorkflowTimerCommand::Cancel { timer_id },
    }
}

fn runtime_labels(handler_path: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            WORKFLOW_RUNTIME_SURFACE_LABEL.to_string(),
            WORKFLOW_RUNTIME_SURFACE_STATE_OUTBOX_TIMERS_V1.to_string(),
        ),
        ("example".to_string(), "workflow-duet".to_string()),
        ("handler-path".to_string(), handler_path.to_string()),
    ])
}

fn trigger_label(trigger: &WorkflowTrigger) -> String {
    match trigger {
        WorkflowTrigger::Event { event } => {
            format!("event:{}", String::from_utf8_lossy(&event.key))
        }
        WorkflowTrigger::Timer { timer_id, .. } => {
            format!("timer:{}", String::from_utf8_lossy(timer_id))
        }
        WorkflowTrigger::Callback { callback_id, .. } => {
            format!("callback:{callback_id}")
        }
    }
}
