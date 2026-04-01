use std::{collections::BTreeMap, sync::Arc};

use terracedb_workflows_core::{
    AppendOnlyWorkflowHistoryOrderer, DeterministicWorkflowWakeupPlanner,
    NativeWorkflowHandlerAdapter, NoopWorkflowObservability,
    PassthroughWorkflowVisibilityProjector, StaticWorkflowBundleResolver,
    StrictWorkflowParityComparator, WorkflowBundleId, WorkflowBundleKind, WorkflowBundleMetadata,
    WorkflowBundleResolver, WorkflowCommand, WorkflowContinueAsNew, WorkflowDeterministicContext,
    WorkflowHandlerContract, WorkflowHistoryEvent, WorkflowHistoryOrderer, WorkflowLifecycleRecord,
    WorkflowLifecycleState, WorkflowOutboxCommand, WorkflowParityComparator, WorkflowPayload,
    WorkflowRunId, WorkflowSourceEvent, WorkflowStateMutation, WorkflowStateRecord,
    WorkflowTaskError, WorkflowTaskId, WorkflowTransitionInput, WorkflowTransitionOutput,
    WorkflowTrigger, WorkflowVisibilityProjector, WorkflowWakeupPlanner,
};

#[test]
fn bundle_metadata_round_trips() {
    let bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:v1").expect("bundle id"),
        workflow_name: "payments".to_string(),
        kind: WorkflowBundleKind::Sandbox {
            abi: "workflow-task/v1".to_string(),
            module: "payments.js".to_string(),
            entrypoint: "default".to_string(),
        },
        created_at_millis: 42,
        labels: BTreeMap::from([("channel".to_string(), "stable".to_string())]),
    };

    let encoded = serde_json::to_vec(&bundle).expect("encode bundle");
    let decoded: WorkflowBundleMetadata = serde_json::from_slice(&encoded).expect("decode bundle");
    assert_eq!(decoded, bundle);
}

#[test]
fn lifecycle_record_round_trips() {
    let record = WorkflowLifecycleRecord {
        run_id: WorkflowRunId::new("run:1").expect("run id"),
        lifecycle: WorkflowLifecycleState::RetryWaiting,
        updated_at_millis: 99,
        reason: Some("waiting-for-retry".to_string()),
        task_id: Some(WorkflowTaskId::new("task:3").expect("task id")),
        attempt: 3,
    };

    let encoded = serde_json::to_vec(&record).expect("encode lifecycle");
    let decoded: WorkflowLifecycleRecord =
        serde_json::from_slice(&encoded).expect("decode lifecycle");
    assert_eq!(decoded, record);
}

#[test]
fn history_event_round_trips() {
    let event = WorkflowHistoryEvent::TaskApplied {
        task_id: WorkflowTaskId::new("task:4").expect("task id"),
        output: WorkflowTransitionOutput {
            state: WorkflowStateMutation::Put {
                state: WorkflowPayload::bytes("next"),
            },
            lifecycle: Some(WorkflowLifecycleState::Running),
            visibility: None,
            continue_as_new: Some(WorkflowContinueAsNew {
                next_run_id: WorkflowRunId::new("run:2").expect("next run"),
                next_bundle_id: WorkflowBundleId::new("bundle:v2").expect("next bundle"),
                state: Some(WorkflowPayload::bytes("carry")),
            }),
            commands: vec![WorkflowCommand::Outbox {
                entry: WorkflowOutboxCommand {
                    outbox_id: b"outbox-1".to_vec(),
                    idempotency_key: "outbox-1".to_string(),
                    payload: b"hello".to_vec(),
                },
            }],
        },
        committed_at_millis: 500,
    };

    let encoded = serde_json::to_vec(&event).expect("encode history event");
    let decoded: WorkflowHistoryEvent =
        serde_json::from_slice(&encoded).expect("decode history event");
    assert_eq!(decoded, event);
}

#[tokio::test]
async fn deterministic_contract_seams_instantiate_together() {
    struct FakeHandler;

    #[async_trait::async_trait]
    impl WorkflowHandlerContract for FakeHandler {
        async fn route_event(
            &self,
            event: &WorkflowSourceEvent,
        ) -> Result<String, WorkflowTaskError> {
            Ok(String::from_utf8_lossy(&event.key).into_owned())
        }

        async fn handle_task(
            &self,
            _input: WorkflowTransitionInput,
            _ctx: WorkflowDeterministicContext,
        ) -> Result<WorkflowTransitionOutput, WorkflowTaskError> {
            Ok(WorkflowTransitionOutput::default())
        }
    }

    let bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:v1").expect("bundle id"),
        workflow_name: "orders".to_string(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/native".to_string(),
        },
        created_at_millis: 7,
        labels: BTreeMap::new(),
    };
    let resolver = StaticWorkflowBundleResolver::new([bundle.clone()]);
    assert_eq!(
        resolver
            .resolve_bundle(&bundle.bundle_id)
            .expect("bundle should resolve"),
        bundle
    );

    let orderer = AppendOnlyWorkflowHistoryOrderer;
    assert_eq!(orderer.next_history_sequence(12), 13);

    let planner = DeterministicWorkflowWakeupPlanner;
    assert_eq!(
        planner.timer_task_id(
            &WorkflowRunId::new("run:orders").expect("run id"),
            b"deadline",
            77
        ),
        planner.timer_task_id(
            &WorkflowRunId::new("run:orders").expect("run id"),
            b"deadline",
            77
        )
    );

    let state = WorkflowStateRecord {
        run_id: WorkflowRunId::new("run:orders").expect("run id"),
        bundle_id: bundle.bundle_id.clone(),
        workflow_name: "orders".to_string(),
        instance_id: "instance-1".to_string(),
        lifecycle: WorkflowLifecycleState::Scheduled,
        current_task_id: Some(WorkflowTaskId::new("task:1").expect("task id")),
        history_len: 1,
        state: Some(WorkflowPayload::bytes("queued")),
        updated_at_millis: 88,
    };
    let projector = PassthroughWorkflowVisibilityProjector;
    let visibility = projector.project_visibility(&state, &WorkflowTransitionOutput::default());
    assert_eq!(visibility.lifecycle, WorkflowLifecycleState::Scheduled);

    let handler = NativeWorkflowHandlerAdapter::new(FakeHandler);
    let event = WorkflowSourceEvent {
        source_table: "orders".to_string(),
        key: b"instance-1".to_vec(),
        value: Some(WorkflowPayload::bytes("created")),
        cursor: [0; 16],
        sequence: 1,
        kind: terracedb_workflows_core::WorkflowChangeKind::Put,
        operation_context: None,
    };
    assert_eq!(
        handler.route_event(&event).await.expect("route event"),
        "instance-1"
    );

    let input = WorkflowTransitionInput {
        run_id: state.run_id.clone(),
        bundle_id: state.bundle_id.clone(),
        task_id: WorkflowTaskId::new("task:2").expect("task id"),
        workflow_name: state.workflow_name.clone(),
        instance_id: state.instance_id.clone(),
        lifecycle: state.lifecycle,
        history_len: state.history_len,
        attempt: 1,
        admitted_at_millis: 89,
        state: state.state.clone(),
        trigger: WorkflowTrigger::Event {
            event: event.clone(),
        },
    };
    let ctx = WorkflowDeterministicContext::new(&input, Arc::new(NoopWorkflowObservability))
        .expect("build deterministic context");
    let output = handler
        .handle_task(input.clone(), ctx.clone())
        .await
        .expect("handle task");
    assert_eq!(output, WorkflowTransitionOutput::default());

    let comparator = StrictWorkflowParityComparator;
    assert!(comparator.equivalent(&output, &WorkflowTransitionOutput::default()));

    let rebuilt =
        WorkflowDeterministicContext::from_seed(ctx.seed(), Arc::new(NoopWorkflowObservability));
    assert_eq!(ctx.stable_id("scope"), rebuilt.stable_id("scope"));
    assert_eq!(ctx.stable_time("scope"), rebuilt.stable_time("scope"));
}
