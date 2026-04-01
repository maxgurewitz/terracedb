use std::{collections::BTreeMap, sync::Arc};

use terracedb_workflows_core::{
    NativeWorkflowHandlerAdapter, NoopWorkflowObservability, StrictWorkflowParityComparator,
    WorkflowBundleId, WorkflowBundleKind, WorkflowBundleMetadata, WorkflowCommand,
    WorkflowDeterministicContext, WorkflowHandlerContract, WorkflowOutboxCommand,
    WorkflowParityComparator, WorkflowPayload, WorkflowSourceEvent, WorkflowStateMutation,
    WorkflowTaskError, WorkflowTaskId, WorkflowTransitionInput, WorkflowTransitionOutput,
    WorkflowTrigger,
};
use terracedb_workflows_sandbox::{
    SandboxWorkflowHandlerAdapter, WORKFLOW_TASK_V1_ABI, WorkflowTaskV1Handler,
    WorkflowTaskV1Request, WorkflowTaskV1Response, WorkflowTaskV1RouteRequest,
    WorkflowTaskV1RouteResponse,
};

#[derive(Clone, Debug, Default)]
struct Logic;

impl Logic {
    fn route(&self, event: &WorkflowSourceEvent) -> String {
        String::from_utf8_lossy(&event.key)
            .split_once(':')
            .expect("event key should contain an instance prefix")
            .0
            .to_string()
    }

    fn handle(
        &self,
        input: &WorkflowTransitionInput,
        seed: &terracedb_workflows_core::WorkflowDeterministicSeed,
    ) -> WorkflowTransitionOutput {
        let sequence = input.history_len.saturating_add(1);
        WorkflowTransitionOutput {
            state: WorkflowStateMutation::Put {
                state: WorkflowPayload::bytes(format!("state:{sequence}")),
            },
            lifecycle: Some(input.lifecycle),
            visibility: Some(terracedb_workflows_core::WorkflowVisibilityUpdate {
                summary: BTreeMap::from([
                    ("workflow".to_string(), input.workflow_name.clone()),
                    ("task".to_string(), input.task_id.to_string()),
                ]),
                note: Some(seed.stable_id("note")),
            }),
            continue_as_new: None,
            commands: vec![WorkflowCommand::Outbox {
                entry: WorkflowOutboxCommand {
                    outbox_id: seed.stable_id("outbox").into_bytes(),
                    idempotency_key: seed.stable_id("idempotency"),
                    payload: seed.stable_time("payload").get().to_be_bytes().to_vec(),
                },
            }],
        }
    }
}

struct NativeHandler {
    logic: Logic,
}

#[async_trait::async_trait]
impl WorkflowHandlerContract for NativeHandler {
    async fn route_event(&self, event: &WorkflowSourceEvent) -> Result<String, WorkflowTaskError> {
        Ok(self.logic.route(event))
    }

    async fn handle_task(
        &self,
        input: WorkflowTransitionInput,
        ctx: WorkflowDeterministicContext,
    ) -> Result<WorkflowTransitionOutput, WorkflowTaskError> {
        Ok(self.logic.handle(&input, &ctx.seed()))
    }
}

struct SandboxHandler {
    logic: Logic,
}

#[async_trait::async_trait]
impl WorkflowTaskV1Handler for SandboxHandler {
    async fn route_event_v1(
        &self,
        request: WorkflowTaskV1RouteRequest,
    ) -> Result<WorkflowTaskV1RouteResponse, WorkflowTaskError> {
        assert_eq!(request.abi, WORKFLOW_TASK_V1_ABI);
        Ok(WorkflowTaskV1RouteResponse {
            abi: request.abi,
            instance_id: self.logic.route(&request.event),
        })
    }

    async fn handle_task_v1(
        &self,
        request: WorkflowTaskV1Request,
    ) -> Result<WorkflowTaskV1Response, WorkflowTaskError> {
        assert_eq!(request.abi, WORKFLOW_TASK_V1_ABI);
        Ok(WorkflowTaskV1Response {
            abi: request.abi,
            output: self.logic.handle(&request.input, &request.deterministic),
        })
    }
}

fn sample_bundle() -> WorkflowBundleMetadata {
    WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:v1").expect("bundle id"),
        workflow_name: "billing".to_string(),
        kind: WorkflowBundleKind::Sandbox {
            abi: WORKFLOW_TASK_V1_ABI.to_string(),
            module: "billing.js".to_string(),
            entrypoint: "default".to_string(),
        },
        created_at_millis: 1,
        labels: BTreeMap::from([("track".to_string(), "T108".to_string())]),
    }
}

fn sample_event() -> WorkflowSourceEvent {
    WorkflowSourceEvent {
        source_table: "orders".to_string(),
        key: b"acct-7:created".to_vec(),
        value: Some(WorkflowPayload::bytes("created")),
        cursor: [3; 16],
        sequence: 44,
        kind: terracedb_workflows_core::WorkflowChangeKind::Put,
        operation_context: None,
    }
}

fn sample_input(bundle: &WorkflowBundleMetadata) -> WorkflowTransitionInput {
    WorkflowTransitionInput {
        run_id: terracedb_workflows_core::WorkflowRunId::new("run:acct-7").expect("run id"),
        bundle_id: bundle.bundle_id.clone(),
        task_id: WorkflowTaskId::new("task:acct-7:1").expect("task id"),
        workflow_name: bundle.workflow_name.clone(),
        instance_id: "acct-7".to_string(),
        lifecycle: terracedb_workflows_core::WorkflowLifecycleState::Running,
        history_len: 3,
        attempt: 1,
        admitted_at_millis: 55,
        state: Some(WorkflowPayload::bytes("existing-state")),
        trigger: WorkflowTrigger::Event {
            event: sample_event(),
        },
    }
}

#[tokio::test]
async fn frozen_contracts_instantiate_with_native_and_sandbox_adapters() {
    let bundle = sample_bundle();
    let native = NativeWorkflowHandlerAdapter::new(NativeHandler { logic: Logic });
    let sandbox = SandboxWorkflowHandlerAdapter::new(SandboxHandler { logic: Logic });

    let input = sample_input(&bundle);
    let ctx = WorkflowDeterministicContext::new(&input, Arc::new(NoopWorkflowObservability))
        .expect("build deterministic context");

    assert_eq!(
        native
            .route_event(&sample_event())
            .await
            .expect("native route"),
        "acct-7"
    );
    assert_eq!(
        sandbox
            .route_event(&sample_event())
            .await
            .expect("sandbox route"),
        "acct-7"
    );

    let native_output = native
        .handle_task(input.clone(), ctx.clone())
        .await
        .expect("native handle");
    let sandbox_output = sandbox
        .handle_task(input, ctx)
        .await
        .expect("sandbox handle");
    assert_eq!(native_output, sandbox_output);
}

#[tokio::test]
async fn deterministic_smoke_runs_rust_and_sandbox_handlers_through_same_contract() {
    let bundle = sample_bundle();
    let input = sample_input(&bundle);
    let ctx = WorkflowDeterministicContext::new(&input, Arc::new(NoopWorkflowObservability))
        .expect("build deterministic context");

    let native = NativeWorkflowHandlerAdapter::new(NativeHandler { logic: Logic });
    let sandbox = SandboxWorkflowHandlerAdapter::new(SandboxHandler { logic: Logic });

    let native_output = native
        .handle_task(input.clone(), ctx.clone())
        .await
        .expect("native output");
    let sandbox_output = sandbox
        .handle_task(input, ctx)
        .await
        .expect("sandbox output");

    let comparator = StrictWorkflowParityComparator;
    assert!(comparator.equivalent(&native_output, &sandbox_output));
}
