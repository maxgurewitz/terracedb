use std::{collections::BTreeMap, sync::Arc};

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    CapabilityRegistry, ConflictPolicy, DefaultSandboxStore, DeterministicCapabilityModule,
    DeterministicCapabilityRegistry, PackageCompatibilityMode, SandboxCapability, SandboxConfig,
    SandboxServices, SandboxStore,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};
use terracedb_workflows_core::{
    NativeWorkflowHandlerAdapter, NoopWorkflowObservability, StrictWorkflowParityComparator,
    WorkflowBundleId, WorkflowBundleKind, WorkflowBundleMetadata, WorkflowCommand,
    WorkflowDeterministicContext, WorkflowHandlerContract, WorkflowOutboxCommand,
    WorkflowParityComparator, WorkflowPayload, WorkflowSourceEvent, WorkflowStateMutation,
    WorkflowTaskError, WorkflowTaskId, WorkflowTransitionInput, WorkflowTransitionOutput,
    WorkflowTrigger,
};
use terracedb_workflows_sandbox::{
    SandboxModuleWorkflowTaskV1Handler, SandboxWorkflowHandlerAdapter, WORKFLOW_TASK_V1_ABI,
    WorkflowTaskV1Handler, WorkflowTaskV1Request, WorkflowTaskV1Response,
    WorkflowTaskV1RouteRequest, WorkflowTaskV1RouteResponse,
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
            module: "/workspace/billing.js".to_string(),
            entrypoint: "default".to_string(),
        },
        created_at_millis: 1,
        labels: BTreeMap::from([
            ("track".to_string(), "T108".to_string()),
            (
                "terracedb.workflow.runtime-surface".to_string(),
                "state-outbox-timers/v1".to_string(),
            ),
        ]),
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
        target: bundle.target(),
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

fn sandbox_store(now: u64, seed: u64) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(
        Arc::new(vfs.clone()),
        dependencies.clock,
        SandboxServices::deterministic(),
    );
    (vfs, sandbox)
}

fn sandbox_store_with_services(
    now: u64,
    seed: u64,
    services: SandboxServices,
) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(Arc::new(vfs.clone()), dependencies.clock, services);
    (vfs, sandbox)
}

fn deterministic_capabilities() -> DeterministicCapabilityRegistry {
    DeterministicCapabilityRegistry::new(vec![
        DeterministicCapabilityModule::new(SandboxCapability::host_module("tickets"))
            .expect("valid capability")
            .with_echo_method("echo"),
    ])
    .expect("build registry")
}

async fn seed_module(store: &InMemoryVfsStore, volume_id: VolumeId, path: &str, source: &str) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
    base.fs()
        .write_file(
            path,
            source.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write workflow module");
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

#[tokio::test]
async fn module_backed_handler_round_trips_workflow_task_v1_requests() {
    let (vfs, sandbox) = sandbox_store(100, 91);
    let base_volume_id = VolumeId::new(0x9300);
    let session_volume_id = VolumeId::new(0x9301);
    seed_module(
        &vfs,
        base_volume_id,
        "/workspace/billing.js",
        r#"
        const bytes = (value) => Array.from(value).map((char) => char.charCodeAt(0));
        const keyToInstance = (key) => String.fromCharCode(...key).split(":")[0];

        export default {
          routeEventV1(request) {
            return {
              abi: request.abi,
              instance_id: keyToInstance(request.event.key),
            };
          },

          async handleTaskV1(request) {
            const sequence = request.input.history_len + 1;
            return {
              abi: request.abi,
              output: {
                state: {
                  kind: "put",
                  state: {
                    encoding: "application/octet-stream",
                    bytes: bytes(`state:${sequence}`),
                  },
                },
                lifecycle: null,
                visibility: null,
                continue_as_new: null,
                commands: [],
              },
            };
          },
        };
        "#,
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let bundle = sample_bundle();
    let handler =
        SandboxModuleWorkflowTaskV1Handler::new(session, bundle.clone()).expect("module handler");

    let route = handler
        .route_event_v1(WorkflowTaskV1RouteRequest {
            abi: WORKFLOW_TASK_V1_ABI.to_string(),
            event: sample_event(),
        })
        .await
        .expect("route event");
    assert_eq!(route.instance_id, "acct-7");

    let response = handler
        .handle_task_v1(WorkflowTaskV1Request {
            abi: WORKFLOW_TASK_V1_ABI.to_string(),
            input: sample_input(&bundle),
            deterministic: terracedb_workflows_core::WorkflowDeterministicSeed {
                workflow_name: "billing".to_string(),
                instance_id: "acct-7".to_string(),
                run_id: terracedb_workflows_core::WorkflowRunId::new("run:acct-7").expect("run id"),
                task_id: WorkflowTaskId::new("task:acct-7:1").expect("task id"),
                trigger_hash: 11,
                state_hash: 22,
            },
        })
        .await
        .expect("handle task");

    assert_eq!(response.abi, WORKFLOW_TASK_V1_ABI);
    assert_eq!(
        response.output.state,
        WorkflowStateMutation::Put {
            state: WorkflowPayload::bytes("state:4"),
        }
    );
}

#[tokio::test]
async fn module_backed_handler_surfaces_structured_rejections() {
    let (vfs, sandbox) = sandbox_store(110, 92);
    let base_volume_id = VolumeId::new(0x9310);
    let session_volume_id = VolumeId::new(0x9311);
    seed_module(
        &vfs,
        base_volume_id,
        "/workspace/billing.js",
        r#"
        export default {
          routeEventV1(request) {
            return {
              abi: request.abi,
              instance_id: "acct-7",
            };
          },

          async handleTaskV1(_request) {
            throw {
              code: "guest-rejected",
              message: "task rejected",
              metadata: { retryable: false },
            };
          },
        };
        "#,
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let handler =
        SandboxModuleWorkflowTaskV1Handler::new(session, sample_bundle()).expect("module handler");
    let error = handler
        .handle_task_v1(WorkflowTaskV1Request {
            abi: WORKFLOW_TASK_V1_ABI.to_string(),
            input: sample_input(&sample_bundle()),
            deterministic: terracedb_workflows_core::WorkflowDeterministicSeed {
                workflow_name: "billing".to_string(),
                instance_id: "acct-7".to_string(),
                run_id: terracedb_workflows_core::WorkflowRunId::new("run:acct-7").expect("run id"),
                task_id: WorkflowTaskId::new("task:acct-7:1").expect("task id"),
                trigger_hash: 11,
                state_hash: 22,
            },
        })
        .await
        .expect_err("guest rejection should surface");

    assert_eq!(error.code, "guest-rejected");
    assert!(error.message.contains("task rejected"));
    assert!(error.message.contains("retryable"));
}

#[tokio::test]
async fn sdk_defined_module_wraps_plain_handle_logic_into_workflow_task_contract() {
    let (vfs, sandbox) = sandbox_store(120, 93);
    let base_volume_id = VolumeId::new(0x9320);
    let session_volume_id = VolumeId::new(0x9321);
    seed_module(
        &vfs,
        base_volume_id,
        "/workspace/billing.js",
        r#"
        import { schema, text, wf } from "@terrace/workflow";

        const BillingState = wf.jsonState(
          schema.object({
            status: schema.string(),
            task: schema.string(),
          }),
        );

        const taskLabel = (taskId) => taskId.replace(/^task:/, "");

        export default wf.define({
          state: BillingState,

          routeEvent({ event }) {
            return text(event.key).split(":")[0];
          },

          async handle({ input, state, running, visibility }) {
            const task = taskLabel(input.task_id);
            return running({
              putState: {
                status: state ? "updated" : "created",
                task,
              },
              visibility: visibility({
                workflow: input.workflow_name,
                task,
              }),
            });
          },
        });
        "#,
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let bundle = sample_bundle();
    let handler =
        SandboxModuleWorkflowTaskV1Handler::new(session, bundle.clone()).expect("module handler");

    let route = handler
        .route_event_v1(WorkflowTaskV1RouteRequest {
            abi: WORKFLOW_TASK_V1_ABI.to_string(),
            event: sample_event(),
        })
        .await
        .expect("route event");
    assert_eq!(route.instance_id, "acct-7");

    let mut input = sample_input(&bundle);
    input.state = None;
    input.history_len = 0;
    let response = handler
        .handle_task_v1(WorkflowTaskV1Request {
            abi: WORKFLOW_TASK_V1_ABI.to_string(),
            input,
            deterministic: terracedb_workflows_core::WorkflowDeterministicSeed {
                workflow_name: "billing".to_string(),
                instance_id: "acct-7".to_string(),
                run_id: terracedb_workflows_core::WorkflowRunId::new("run:acct-7").expect("run id"),
                task_id: WorkflowTaskId::new("task:acct-7:1").expect("task id"),
                trigger_hash: 11,
                state_hash: 22,
            },
        })
        .await
        .expect("handle task");

    assert_eq!(response.abi, WORKFLOW_TASK_V1_ABI);
    assert_eq!(
        response.output.state,
        WorkflowStateMutation::Put {
            state: WorkflowPayload::bytes(r#"{"status":"created","task":"acct-7:1"}"#),
        }
    );
    assert_eq!(
        response.output.visibility.expect("visibility").summary,
        BTreeMap::from([
            ("workflow".to_string(), "billing".to_string()),
            ("task".to_string(), "acct-7:1".to_string()),
        ])
    );
}

#[tokio::test]
async fn sdk_defined_module_can_import_generated_capability_catalog() {
    let registry = deterministic_capabilities();
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store_with_services(130, 94, services);
    let base_volume_id = VolumeId::new(0x9330);
    let session_volume_id = VolumeId::new(0x9331);
    seed_module(
        &vfs,
        base_volume_id,
        "/workspace/billing.js",
        r#"
        import { Tickets } from "@terrace/capabilities";
        import { schema, wf } from "@terrace/workflow";

        const BillingState = wf.jsonState(
          schema.object({
            echoed: schema.string(),
          }),
        );

        export default wf.define({
          state: BillingState,

          routeEvent({ event }) {
            return String.fromCharCode(...event.key).split(":")[0];
          },

          async handle({ workflowName, instanceId, running }) {
            const echoed = await Tickets.echo({
              workflow: workflowName,
              instance_id: instanceId,
            });
            return running({
              putState: {
                echoed: `${echoed.method}:${echoed.specifier}`,
              },
            });
          },
        });
        "#,
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let bundle = sample_bundle();
    let handler =
        SandboxModuleWorkflowTaskV1Handler::new(session, bundle.clone()).expect("module handler");
    let mut input = sample_input(&bundle);
    input.state = None;
    input.history_len = 0;

    let response = handler
        .handle_task_v1(WorkflowTaskV1Request {
            abi: WORKFLOW_TASK_V1_ABI.to_string(),
            input,
            deterministic: terracedb_workflows_core::WorkflowDeterministicSeed {
                workflow_name: "billing".to_string(),
                instance_id: "acct-7".to_string(),
                run_id: terracedb_workflows_core::WorkflowRunId::new("run:acct-7").expect("run id"),
                task_id: WorkflowTaskId::new("task:acct-7:1").expect("task id"),
                trigger_hash: 11,
                state_hash: 22,
            },
        })
        .await
        .expect("handle task");

    assert_eq!(response.abi, WORKFLOW_TASK_V1_ABI);
    assert_eq!(
        response.output.state,
        WorkflowStateMutation::Put {
            state: WorkflowPayload::bytes(r#"{"echoed":"echo:terrace:host/tickets"}"#),
        }
    );
}
