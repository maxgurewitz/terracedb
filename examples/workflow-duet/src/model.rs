use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use terracedb_workflows::{
    WORKFLOW_RUNTIME_SURFACE_LABEL, WORKFLOW_RUNTIME_SURFACE_STATE_OUTBOX_TIMERS_V1,
    contracts::{
        WorkflowBundleId, WorkflowBundleKind, WorkflowBundleMetadata,
        WorkflowCallbackDeliveryCommand, WorkflowCommand, WorkflowLifecycleState,
        WorkflowNativeRegistrationMetadata, WorkflowPayload, WorkflowRegistrationId,
        WorkflowSandboxPackageCompatibility, WorkflowSandboxPreparation, WorkflowSandboxSourceKind,
        WorkflowTaskError, WorkflowTimerCommand, WorkflowTransitionInput, WorkflowTransitionOutput,
        WorkflowTrigger, WorkflowVisibilityUpdate,
    },
};
use terracedb_workflows_sandbox::WORKFLOW_TASK_V1_ABI;

pub const NATIVE_WORKFLOW_NAME: &str = "workflow-duet-native";
pub const SANDBOX_WORKFLOW_NAME: &str = "workflow-duet-sandbox";
pub const NATIVE_REGISTRATION_ID: &str = "registration:workflow-duet:review:v1";
pub const SANDBOX_BUNDLE_ID: &str = "bundle:workflow-duet:review-sandbox:v1";
pub const SANDBOX_MODULE_PATH: &str = "/workspace/review_workflow.ts";
pub const SANDBOX_PACKAGE_JSON_PATH: &str = "/workspace/package.json";
pub const SANDBOX_TSCONFIG_PATH: &str = "/workspace/tsconfig.json";
pub const START_CALLBACK_ID: &str = "start";
pub const APPROVE_CALLBACK_ID: &str = "approve";
pub const RETRY_DELAY_MILLIS: u64 = 10;
pub const APPROVAL_TIMEOUT_MILLIS: u64 = 120;

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
    pub approval_received: bool,
    pub waiting_for_callback: Option<String>,
    pub deadline_millis: Option<u64>,
    pub last_trigger: String,
}

pub fn native_registration() -> WorkflowNativeRegistrationMetadata {
    WorkflowNativeRegistrationMetadata {
        registration_id: WorkflowRegistrationId::new(NATIVE_REGISTRATION_ID)
            .expect("native registration id"),
        workflow_name: NATIVE_WORKFLOW_NAME.to_string(),
        registration: "terracedb-example-workflow-duet/native-review@v1".to_string(),
        registered_at_millis: 1,
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
            preparation: WorkflowSandboxPreparation {
                source_kind: Some(WorkflowSandboxSourceKind::TypeScript),
                package_compat: Some(WorkflowSandboxPackageCompatibility::NpmPureJs),
                package_manifest_path: Some(SANDBOX_PACKAGE_JSON_PATH.to_string()),
                tsconfig_path: Some(SANDBOX_TSCONFIG_PATH.to_string()),
            },
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
        approval_received: false,
        waiting_for_callback: None,
        deadline_millis: None,
        last_trigger: trigger_label.to_string(),
    };
    let retry_at = input.admitted_at_millis.saturating_add(RETRY_DELAY_MILLIS);
    build_transition(
        workflow_name,
        next_state,
        Some(WorkflowLifecycleState::Running),
        vec![schedule_timer_command(
            retry_timer_id(&input.instance_id),
            retry_at,
            "retry",
        )],
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
            let next_attempt = state.attempt.saturating_add(1);
            let counterpart = counterpart_workflow_name(workflow_name)?;
            if state.approval_received {
                let next_state = ReviewState {
                    stage: ReviewStage::Approved,
                    attempt: next_attempt,
                    started_at_millis: state.started_at_millis,
                    last_updated_at_millis: input.admitted_at_millis,
                    approval_received: true,
                    waiting_for_callback: None,
                    deadline_millis: None,
                    last_trigger: trigger_label.to_string(),
                };
                build_transition(
                    workflow_name,
                    next_state,
                    Some(WorkflowLifecycleState::Completed),
                    vec![deliver_callback_command(
                        counterpart,
                        &input.instance_id,
                        APPROVE_CALLBACK_ID,
                        b"approved".to_vec(),
                    )],
                )
            } else {
                let deadline = fire_at_millis.saturating_add(APPROVAL_TIMEOUT_MILLIS);
                let next_state = ReviewState {
                    stage: ReviewStage::WaitingApproval,
                    attempt: next_attempt,
                    started_at_millis: state.started_at_millis,
                    last_updated_at_millis: input.admitted_at_millis,
                    approval_received: false,
                    waiting_for_callback: Some(APPROVE_CALLBACK_ID.to_string()),
                    deadline_millis: Some(deadline),
                    last_trigger: trigger_label.to_string(),
                };
                build_transition(
                    workflow_name,
                    next_state,
                    Some(WorkflowLifecycleState::Running),
                    vec![
                        deliver_callback_command(
                            counterpart,
                            &input.instance_id,
                            APPROVE_CALLBACK_ID,
                            b"approved".to_vec(),
                        ),
                        schedule_timer_command(
                            approval_timeout_timer_id(&input.instance_id),
                            deadline,
                            "approval-timeout",
                        ),
                    ],
                )
            }
        }
        (ReviewStage::RetryBackoff, WorkflowTrigger::Callback { callback_id, .. })
            if callback_id == APPROVE_CALLBACK_ID =>
        {
            let next_state = ReviewState {
                stage: ReviewStage::RetryBackoff,
                attempt: state.attempt,
                started_at_millis: state.started_at_millis,
                last_updated_at_millis: input.admitted_at_millis,
                approval_received: true,
                waiting_for_callback: None,
                deadline_millis: None,
                last_trigger: trigger_label.to_string(),
            };
            build_transition(
                workflow_name,
                next_state,
                Some(WorkflowLifecycleState::Running),
                Vec::new(),
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
                approval_received: true,
                waiting_for_callback: None,
                deadline_millis: None,
                last_trigger: trigger_label.to_string(),
            };
            build_transition(
                workflow_name,
                next_state,
                Some(WorkflowLifecycleState::Completed),
                vec![cancel_timer_command(approval_timeout_timer_id(
                    &input.instance_id,
                ))],
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
                approval_received: state.approval_received,
                waiting_for_callback: None,
                deadline_millis: None,
                last_trigger: trigger_label.to_string(),
            };
            build_transition(
                workflow_name,
                next_state,
                Some(WorkflowLifecycleState::Failed),
                Vec::new(),
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
        commands,
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
        (
            "approval-received".to_string(),
            state.approval_received.to_string(),
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

fn counterpart_workflow_name(workflow_name: &str) -> Result<&'static str, WorkflowTaskError> {
    match workflow_name {
        NATIVE_WORKFLOW_NAME => Ok(SANDBOX_WORKFLOW_NAME),
        SANDBOX_WORKFLOW_NAME => Ok(NATIVE_WORKFLOW_NAME),
        other => Err(WorkflowTaskError::invalid_contract(format!(
            "workflow-duet does not know counterpart for workflow {other}"
        ))),
    }
}

fn deliver_callback_command(
    target_workflow: &str,
    instance_id: &str,
    callback_id: &str,
    response: Vec<u8>,
) -> WorkflowCommand {
    WorkflowCommand::DeliverCallback {
        delivery: WorkflowCallbackDeliveryCommand {
            target_workflow: target_workflow.to_string(),
            instance_id: instance_id.to_string(),
            callback_id: callback_id.to_string(),
            response,
        },
    }
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
