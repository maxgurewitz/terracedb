use std::{collections::BTreeMap, sync::Arc};

use terracedb_workflows_core::{
    AppendOnlyWorkflowHistoryOrderer, DeterministicWorkflowWakeupPlanner,
    NativeWorkflowHandlerAdapter, NoopWorkflowObservability,
    PassthroughWorkflowVisibilityProjector, StaticWorkflowBundleResolver,
    StaticWorkflowCompatibilityChecker, StaticWorkflowDeploymentManager,
    StaticWorkflowExecutionResolver, StaticWorkflowInteractionApi, StaticWorkflowVisibilityApi,
    StrictWorkflowParityComparator, WorkflowBundleId, WorkflowBundleKind, WorkflowBundleMetadata,
    WorkflowBundleResolver, WorkflowCommand, WorkflowCompatibilityCheck,
    WorkflowCompatibilityChecker, WorkflowCompatibilityDisposition, WorkflowCompatibilityManifest,
    WorkflowContinueAsNew, WorkflowDeploymentActivationRequest, WorkflowDeploymentEnvironment,
    WorkflowDeploymentManager, WorkflowDeploymentPreviewRequest, WorkflowDeploymentRecord,
    WorkflowDeploymentResolutionRequest, WorkflowDescribeRequest, WorkflowDescribeResponse,
    WorkflowDeterministicContext, WorkflowExecutionEpoch, WorkflowExecutionResolver,
    WorkflowExecutionTarget, WorkflowHandlerContract, WorkflowHistoryEvent, WorkflowHistoryOrderer,
    WorkflowHistoryPageEntry, WorkflowHistoryPageRequest, WorkflowInteractionApi,
    WorkflowLifecycleRecord, WorkflowLifecycleState, WorkflowNativeRegistrationMetadata,
    WorkflowOutboxCommand, WorkflowParityComparator, WorkflowPayload, WorkflowQueryId,
    WorkflowQueryRequest, WorkflowQueryResponse, WorkflowQueryTarget, WorkflowRegistrationId,
    WorkflowRolloutPolicy, WorkflowRunAssignment, WorkflowRunId, WorkflowSourceEvent,
    WorkflowStateMutation, WorkflowStateRecord, WorkflowTaskError, WorkflowTaskId,
    WorkflowTransitionInput, WorkflowTransitionOutput, WorkflowTrigger, WorkflowUpdateAdmission,
    WorkflowUpdateId, WorkflowUpdateLane, WorkflowUpdateRequest, WorkflowUpgradePolicy,
    WorkflowVisibilityApi, WorkflowVisibilityEntry, WorkflowVisibilityListRequest,
    WorkflowVisibilityProjector, WorkflowWakeupPlanner,
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
fn native_registration_metadata_round_trips() {
    let registration = WorkflowNativeRegistrationMetadata {
        registration_id: WorkflowRegistrationId::new("native:orders:v1").expect("registration id"),
        workflow_name: "orders".to_string(),
        registration: "orders/native".to_string(),
        registered_at_millis: 55,
        labels: BTreeMap::from([("channel".to_string(), "stable".to_string())]),
    };

    let encoded = serde_json::to_vec(&registration).expect("encode registration");
    let decoded: WorkflowNativeRegistrationMetadata =
        serde_json::from_slice(&encoded).expect("decode registration");
    assert_eq!(decoded, registration);
}

#[test]
fn deployment_and_compatibility_metadata_round_trip() {
    let deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new("deploy:orders:prod:v2")
            .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Production,
        target: WorkflowExecutionTarget::Bundle {
            bundle_id: WorkflowBundleId::new("bundle:orders:v2").expect("bundle id"),
        },
        rollout: WorkflowRolloutPolicy::ByInstanceHash {
            numerator: 1,
            denominator: 10,
        },
        created_at_millis: 101,
        labels: BTreeMap::from([("channel".to_string(), "canary".to_string())]),
    };
    let encoded = serde_json::to_vec(&deployment).expect("encode deployment");
    let decoded: WorkflowDeploymentRecord =
        serde_json::from_slice(&encoded).expect("decode deployment");
    assert_eq!(decoded, deployment);

    let manifest = WorkflowCompatibilityManifest {
        target: decoded.target.clone(),
        workflow_name: "orders".to_string(),
        runtime_surface: Some("state-outbox-timers/v1".to_string()),
        compatible_predecessors: vec![WorkflowExecutionTarget::NativeRegistration {
            registration_id: WorkflowRegistrationId::new("native:orders:v1")
                .expect("registration id"),
        }],
        upgrade_policy: WorkflowUpgradePolicy {
            allow_continue_as_new: true,
            allow_restart_as_new: false,
        },
        created_at_millis: 102,
        labels: BTreeMap::from([("reason".to_string(), "upgrade".to_string())]),
    };
    let encoded = serde_json::to_vec(&manifest).expect("encode compatibility");
    let decoded: WorkflowCompatibilityManifest =
        serde_json::from_slice(&encoded).expect("decode compatibility");
    assert_eq!(decoded, manifest);
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
    let registration = WorkflowNativeRegistrationMetadata {
        registration_id: WorkflowRegistrationId::new("native:orders:v1").expect("registration id"),
        workflow_name: "orders".to_string(),
        registration: "orders/native".to_string(),
        registered_at_millis: 6,
        labels: BTreeMap::from([("channel".to_string(), "stable".to_string())]),
    };
    let execution_resolver =
        StaticWorkflowExecutionResolver::new([bundle.clone()], [registration.clone()]);
    assert_eq!(
        execution_resolver
            .resolve_target(&bundle.target())
            .expect("bundle target should resolve")
            .workflow_name(),
        "orders"
    );
    assert_eq!(
        execution_resolver
            .resolve_target(&registration.target())
            .expect("registration target should resolve")
            .target(),
        registration.target()
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
    let describe = WorkflowDescribeResponse {
        state: state.clone(),
        lifecycle: Some(WorkflowLifecycleRecord {
            run_id: state.run_id.clone(),
            lifecycle: state.lifecycle,
            updated_at_millis: state.updated_at_millis,
            reason: Some("seed".to_string()),
            task_id: state.current_task_id.clone(),
            attempt: 1,
        }),
        visibility: WorkflowVisibilityEntry {
            record: visibility.clone(),
            execution: Some(WorkflowExecutionEpoch::from_assignment(
                state.run_id.clone(),
                WorkflowRunAssignment {
                    workflow_name: state.workflow_name.clone(),
                    environment: WorkflowDeploymentEnvironment::Production,
                    deployment_id: Some(
                        terracedb_workflows_core::WorkflowDeploymentId::new(
                            "deploy:orders:prod:v1",
                        )
                        .expect("deployment id"),
                    ),
                    target: bundle.target(),
                    pinned_epoch: 1,
                    assigned_at_millis: state.updated_at_millis,
                },
                state.updated_at_millis,
            )),
        },
    };
    let visibility_api = StaticWorkflowVisibilityApi::new(
        [describe.clone()],
        [(
            state.run_id.clone(),
            vec![WorkflowHistoryPageEntry {
                sequence: 1,
                event: WorkflowHistoryEvent::RunCreated {
                    run_id: state.run_id.clone(),
                    bundle_id: bundle.bundle_id.clone(),
                    instance_id: state.instance_id.clone(),
                    scheduled_at_millis: state.updated_at_millis,
                },
            }],
        )],
    );
    assert_eq!(
        visibility_api
            .list(WorkflowVisibilityListRequest {
                workflow_name: Some("orders".to_string()),
                lifecycle: Some(WorkflowLifecycleState::Scheduled),
                deployment_id: None,
                target: None,
                page_size: 10,
                cursor: None,
            })
            .await
            .expect("list visibility")
            .entries,
        vec![describe.visibility.clone()]
    );
    assert_eq!(
        visibility_api
            .describe(WorkflowDescribeRequest {
                run_id: state.run_id.clone(),
            })
            .await
            .expect("describe run"),
        describe
    );
    assert_eq!(
        visibility_api
            .history(WorkflowHistoryPageRequest {
                run_id: state.run_id.clone(),
                after_sequence: None,
                limit: 10,
            })
            .await
            .expect("history page")
            .entries
            .len(),
        1
    );

    let query_id = WorkflowQueryId::new("query:orders:1").expect("query id");
    let update_id = WorkflowUpdateId::new("update:orders:1").expect("update id");
    let interaction_api = StaticWorkflowInteractionApi::new(
        [(
            query_id.clone(),
            WorkflowQueryResponse::Accepted {
                query_id: query_id.clone(),
                payload: Some(WorkflowPayload::bytes("inspection")),
                answered_at_millis: 90,
            },
        )],
        [(
            update_id.clone(),
            WorkflowUpdateAdmission::Accepted {
                update_id: update_id.clone(),
                task_id: WorkflowTaskId::new("task:update:1").expect("update task"),
            },
        )],
    );
    assert_eq!(
        interaction_api
            .query(WorkflowQueryRequest {
                query_id: query_id.clone(),
                run_id: state.run_id.clone(),
                target: WorkflowQueryTarget::State,
                name: "inspect".to_string(),
                payload: None,
                requested_at_millis: 89,
            })
            .await
            .expect("query request"),
        WorkflowQueryResponse::Accepted {
            query_id,
            payload: Some(WorkflowPayload::bytes("inspection")),
            answered_at_millis: 90,
        }
    );
    assert_eq!(
        interaction_api
            .admit_update(WorkflowUpdateRequest {
                update_id: update_id.clone(),
                run_id: state.run_id.clone(),
                lane: WorkflowUpdateLane::Control,
                name: "pause".to_string(),
                payload: None,
                requested_at_millis: 91,
            })
            .await
            .expect("update admission"),
        WorkflowUpdateAdmission::Accepted {
            update_id,
            task_id: WorkflowTaskId::new("task:update:1").expect("update task"),
        }
    );

    let compatibility = StaticWorkflowCompatibilityChecker::new([WorkflowCompatibilityManifest {
        target: bundle.target(),
        workflow_name: "orders".to_string(),
        runtime_surface: Some("state-outbox-timers/v1".to_string()),
        compatible_predecessors: vec![registration.target()],
        upgrade_policy: WorkflowUpgradePolicy {
            allow_continue_as_new: true,
            allow_restart_as_new: false,
        },
        created_at_millis: 92,
        labels: BTreeMap::new(),
    }]);
    assert_eq!(
        compatibility
            .evaluate(WorkflowCompatibilityCheck {
                workflow_name: "orders".to_string(),
                current: Some(registration.target()),
                candidate: bundle.target(),
            })
            .await
            .expect("compatibility check")
            .disposition,
        WorkflowCompatibilityDisposition::RequiresContinueAsNew
    );

    let deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new("deploy:orders:prod:v1")
            .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Production,
        target: bundle.target(),
        rollout: WorkflowRolloutPolicy::Immediate,
        created_at_millis: 93,
        labels: BTreeMap::new(),
    };
    let deployment_manager = StaticWorkflowDeploymentManager::new([deployment.clone()]);
    assert_eq!(
        deployment_manager
            .activate(WorkflowDeploymentActivationRequest {
                deployment_id: deployment.deployment_id.clone(),
                updated_at_millis: 94,
            })
            .await
            .expect("activate deployment")
            .active_deployment_id,
        Some(deployment.deployment_id.clone())
    );
    assert_eq!(
        deployment_manager
            .resolve_for_new_run(WorkflowDeploymentResolutionRequest {
                workflow_name: "orders".to_string(),
                environment: WorkflowDeploymentEnvironment::Production,
                instance_id: "instance-1".to_string(),
                labels: BTreeMap::new(),
                requested_at_millis: 95,
            })
            .await
            .expect("resolve production run")
            .target,
        bundle.target()
    );

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

#[tokio::test]
async fn deployment_manager_resolves_preview_and_production_deterministically() {
    let preview_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:preview").expect("bundle id"),
        workflow_name: "orders".to_string(),
        kind: WorkflowBundleKind::Sandbox {
            abi: "workflow-task/v1".to_string(),
            module: "orders.preview.js".to_string(),
            entrypoint: "default".to_string(),
        },
        created_at_millis: 10,
        labels: BTreeMap::from([("channel".to_string(), "preview".to_string())]),
    };
    let production_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:prod").expect("bundle id"),
        workflow_name: "orders".to_string(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/native".to_string(),
        },
        created_at_millis: 11,
        labels: BTreeMap::from([("channel".to_string(), "stable".to_string())]),
    };
    let preview_deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new(
            "deploy:orders:preview:v1",
        )
        .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Preview,
        target: preview_bundle.target(),
        rollout: WorkflowRolloutPolicy::Immediate,
        created_at_millis: 12,
        labels: BTreeMap::new(),
    };
    let production_deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new("deploy:orders:prod:v1")
            .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Production,
        target: production_bundle.target(),
        rollout: WorkflowRolloutPolicy::Immediate,
        created_at_millis: 13,
        labels: BTreeMap::new(),
    };
    let manager = StaticWorkflowDeploymentManager::new([
        preview_deployment.clone(),
        production_deployment.clone(),
    ]);

    let preview_assignment = manager
        .preview(WorkflowDeploymentPreviewRequest {
            deployment: preview_deployment.clone(),
            instance_id: "instance-1".to_string(),
            labels: BTreeMap::new(),
            previewed_at_millis: 20,
        })
        .await
        .expect("preview deployment");
    assert_eq!(
        preview_assignment.environment,
        WorkflowDeploymentEnvironment::Preview
    );
    assert_eq!(preview_assignment.target, preview_bundle.target());

    manager
        .activate(WorkflowDeploymentActivationRequest {
            deployment_id: production_deployment.deployment_id.clone(),
            updated_at_millis: 21,
        })
        .await
        .expect("activate production deployment");
    let production_assignment = manager
        .resolve_for_new_run(WorkflowDeploymentResolutionRequest {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            instance_id: "instance-1".to_string(),
            labels: BTreeMap::new(),
            requested_at_millis: 22,
        })
        .await
        .expect("resolve production deployment");
    assert_eq!(
        production_assignment.environment,
        WorkflowDeploymentEnvironment::Production
    );
    assert_eq!(production_assignment.target, production_bundle.target());
    assert_eq!(production_assignment.pinned_epoch, 1);

    let repeat_assignment = manager
        .resolve_for_new_run(WorkflowDeploymentResolutionRequest {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            instance_id: "instance-1".to_string(),
            labels: BTreeMap::new(),
            requested_at_millis: 22,
        })
        .await
        .expect("resolve production deployment again");
    assert_eq!(repeat_assignment, production_assignment);
}

#[tokio::test]
async fn deployment_manager_enforces_rollout_policy_contract() {
    let manual_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:manual").expect("bundle id"),
        workflow_name: "orders".to_string(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/manual".to_string(),
        },
        created_at_millis: 30,
        labels: BTreeMap::new(),
    };
    let zero_hash_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:hashed").expect("bundle id"),
        workflow_name: "orders".to_string(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/hashed".to_string(),
        },
        created_at_millis: 31,
        labels: BTreeMap::new(),
    };
    let full_hash_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:all").expect("bundle id"),
        workflow_name: "orders".to_string(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/all".to_string(),
        },
        created_at_millis: 32,
        labels: BTreeMap::new(),
    };
    let manual_deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new(
            "deploy:orders:manual:v1",
        )
        .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Production,
        target: manual_bundle.target(),
        rollout: WorkflowRolloutPolicy::Manual,
        created_at_millis: 33,
        labels: BTreeMap::new(),
    };
    let zero_hash_deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new(
            "deploy:orders:hashed:v1",
        )
        .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Production,
        target: zero_hash_bundle.target(),
        rollout: WorkflowRolloutPolicy::ByInstanceHash {
            numerator: 0,
            denominator: 10,
        },
        created_at_millis: 34,
        labels: BTreeMap::new(),
    };
    let full_hash_deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new("deploy:orders:all:v1")
            .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Production,
        target: full_hash_bundle.target(),
        rollout: WorkflowRolloutPolicy::ByInstanceHash {
            numerator: 1,
            denominator: 1,
        },
        created_at_millis: 35,
        labels: BTreeMap::new(),
    };
    let manager = StaticWorkflowDeploymentManager::new([
        manual_deployment.clone(),
        zero_hash_deployment.clone(),
        full_hash_deployment.clone(),
    ]);

    manager
        .activate(WorkflowDeploymentActivationRequest {
            deployment_id: manual_deployment.deployment_id.clone(),
            updated_at_millis: 40,
        })
        .await
        .expect("activate manual deployment");
    let manual_error = manager
        .resolve_for_new_run(WorkflowDeploymentResolutionRequest {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            instance_id: "instance-1".to_string(),
            labels: BTreeMap::new(),
            requested_at_millis: 41,
        })
        .await
        .expect_err("manual rollout should block automatic resolution");
    assert_eq!(manual_error.code, "rollout-paused");

    manager
        .activate(WorkflowDeploymentActivationRequest {
            deployment_id: zero_hash_deployment.deployment_id.clone(),
            updated_at_millis: 42,
        })
        .await
        .expect("activate hashed deployment");
    let zero_hash_error = manager
        .resolve_for_new_run(WorkflowDeploymentResolutionRequest {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            instance_id: "instance-1".to_string(),
            labels: BTreeMap::new(),
            requested_at_millis: 43,
        })
        .await
        .expect_err("zero-percent rollout should reject every instance");
    assert_eq!(zero_hash_error.code, "rollout-not-selected");

    manager
        .activate(WorkflowDeploymentActivationRequest {
            deployment_id: full_hash_deployment.deployment_id.clone(),
            updated_at_millis: 44,
        })
        .await
        .expect("activate full rollout deployment");
    let assignment = manager
        .resolve_for_new_run(WorkflowDeploymentResolutionRequest {
            workflow_name: "orders".to_string(),
            environment: WorkflowDeploymentEnvironment::Production,
            instance_id: "instance-1".to_string(),
            labels: BTreeMap::from([("tenant".to_string(), "west".to_string())]),
            requested_at_millis: 45,
        })
        .await
        .expect("full rollout should resolve");
    assert_eq!(assignment.target, full_hash_bundle.target());
}

#[tokio::test]
async fn deactivating_an_inactive_deployment_preserves_the_actual_active_state() {
    let active_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:active").expect("bundle id"),
        workflow_name: "orders".to_string(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/active".to_string(),
        },
        created_at_millis: 50,
        labels: BTreeMap::new(),
    };
    let inactive_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:inactive").expect("bundle id"),
        workflow_name: "orders".to_string(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/inactive".to_string(),
        },
        created_at_millis: 51,
        labels: BTreeMap::new(),
    };
    let active_deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new("deploy:orders:a:v1")
            .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Production,
        target: active_bundle.target(),
        rollout: WorkflowRolloutPolicy::Immediate,
        created_at_millis: 52,
        labels: BTreeMap::new(),
    };
    let inactive_deployment = WorkflowDeploymentRecord {
        deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new("deploy:orders:b:v1")
            .expect("deployment id"),
        workflow_name: "orders".to_string(),
        environment: WorkflowDeploymentEnvironment::Production,
        target: inactive_bundle.target(),
        rollout: WorkflowRolloutPolicy::Immediate,
        created_at_millis: 53,
        labels: BTreeMap::new(),
    };
    let manager =
        StaticWorkflowDeploymentManager::new([active_deployment.clone(), inactive_deployment]);

    manager
        .activate(WorkflowDeploymentActivationRequest {
            deployment_id: active_deployment.deployment_id.clone(),
            updated_at_millis: 54,
        })
        .await
        .expect("activate primary deployment");
    let activation = manager
        .deactivate(
            terracedb_workflows_core::WorkflowDeploymentDeactivationRequest {
                deployment_id: terracedb_workflows_core::WorkflowDeploymentId::new(
                    "deploy:orders:b:v1",
                )
                .expect("deployment id"),
                updated_at_millis: 55,
            },
        )
        .await
        .expect("deactivate inactive deployment");
    assert_eq!(
        activation.active_deployment_id,
        Some(active_deployment.deployment_id.clone())
    );
    assert_eq!(activation.active_target, Some(active_bundle.target()));
}

#[tokio::test]
async fn hash_rollout_selection_pins_stable_environment_strings() {
    let labels = BTreeMap::from([("tenant".to_string(), "west".to_string())]);
    let workflow_name = "orders".to_string();
    let environment = WorkflowDeploymentEnvironment::Production;
    let deployment_id =
        terracedb_workflows_core::WorkflowDeploymentId::new("deploy:orders:stable-hash:v1")
            .expect("deployment id");
    let denominator = 17;
    let expected_bucket = rollout_bucket_fixture(
        &workflow_name,
        environment,
        deployment_id.as_str(),
        "instance-1",
        &labels,
    ) % denominator;

    let rejecting_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:reject").expect("bundle id"),
        workflow_name: workflow_name.clone(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/reject".to_string(),
        },
        created_at_millis: 60,
        labels: BTreeMap::new(),
    };
    let rejecting_deployment = WorkflowDeploymentRecord {
        deployment_id: deployment_id.clone(),
        workflow_name: workflow_name.clone(),
        environment,
        target: rejecting_bundle.target(),
        rollout: WorkflowRolloutPolicy::ByInstanceHash {
            numerator: expected_bucket,
            denominator,
        },
        created_at_millis: 61,
        labels: BTreeMap::new(),
    };
    let rejecting_manager = StaticWorkflowDeploymentManager::new([rejecting_deployment.clone()]);
    rejecting_manager
        .activate(WorkflowDeploymentActivationRequest {
            deployment_id: rejecting_deployment.deployment_id.clone(),
            updated_at_millis: 62,
        })
        .await
        .expect("activate rejecting deployment");
    let rejection = rejecting_manager
        .resolve_for_new_run(WorkflowDeploymentResolutionRequest {
            workflow_name: workflow_name.clone(),
            environment,
            instance_id: "instance-1".to_string(),
            labels: labels.clone(),
            requested_at_millis: 63,
        })
        .await
        .expect_err("expected exact bucket numerator to reject this instance");
    assert_eq!(rejection.code, "rollout-not-selected");

    let accepting_bundle = WorkflowBundleMetadata {
        bundle_id: WorkflowBundleId::new("bundle:orders:accept").expect("bundle id"),
        workflow_name: workflow_name.clone(),
        kind: WorkflowBundleKind::NativeRust {
            registration: "orders/accept".to_string(),
        },
        created_at_millis: 64,
        labels: BTreeMap::new(),
    };
    let accepting_deployment = WorkflowDeploymentRecord {
        deployment_id,
        workflow_name: workflow_name.clone(),
        environment,
        target: accepting_bundle.target(),
        rollout: WorkflowRolloutPolicy::ByInstanceHash {
            numerator: expected_bucket.saturating_add(1),
            denominator,
        },
        created_at_millis: 65,
        labels: BTreeMap::new(),
    };
    let accepting_manager = StaticWorkflowDeploymentManager::new([accepting_deployment.clone()]);
    accepting_manager
        .activate(WorkflowDeploymentActivationRequest {
            deployment_id: accepting_deployment.deployment_id.clone(),
            updated_at_millis: 66,
        })
        .await
        .expect("activate accepting deployment");
    let assignment = accepting_manager
        .resolve_for_new_run(WorkflowDeploymentResolutionRequest {
            workflow_name,
            environment,
            instance_id: "instance-1".to_string(),
            labels,
            requested_at_millis: 67,
        })
        .await
        .expect("expected bucket-plus-one numerator to accept this instance");
    assert_eq!(assignment.target, accepting_bundle.target());
}

fn rollout_bucket_fixture(
    workflow_name: &str,
    environment: WorkflowDeploymentEnvironment,
    deployment_id: &str,
    instance_id: &str,
    labels: &BTreeMap<String, String>,
) -> u32 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(workflow_name.as_bytes());
    bytes.push(0xff);
    bytes.extend_from_slice(
        match environment {
            WorkflowDeploymentEnvironment::Preview => "preview",
            WorkflowDeploymentEnvironment::Production => "production",
        }
        .as_bytes(),
    );
    bytes.push(0xfe);
    bytes.extend_from_slice(deployment_id.as_bytes());
    bytes.push(0xfd);
    bytes.extend_from_slice(instance_id.as_bytes());
    for (key, value) in labels {
        bytes.push(0xfc);
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(0xfb);
        bytes.extend_from_slice(value.as_bytes());
    }
    stable_hash_fixture(&bytes) as u32
}

fn stable_hash_fixture(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
