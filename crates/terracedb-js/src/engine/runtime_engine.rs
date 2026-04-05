use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use serde_json::{Value as JsonValue, json};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    DeterministicJsEntropySource, FixedJsClock, JsCancellationToken, JsClock,
    JsContextServiceConfiguration, JsExecutionHooks, JsExecutionKind, JsExecutionReport,
    JsExecutionRequest, JsForkPolicy, JsHeapObjectId, JsHostServiceCallRecord,
    JsHostServiceRequest, JsHostServices, JsLoadedModule, JsModuleGraphNode, JsModuleId,
    JsModuleLoader, JsResolvedModule, JsRuntime, JsRuntimeAttachmentState, JsRuntimeConfiguration,
    JsRuntimeContext, JsRuntimeEnvironment, JsRuntimeErrorReport, JsRuntimeHandle, JsRuntimeHost,
    JsRuntimeOpenRequest, JsRuntimeSuspendedState, JsRuntimeTurn, JsRuntimeTurnCompletion,
    JsRuntimeTurnKind, JsRuntimeTurnOutcome, JsScheduledTask, JsScheduler, JsSubstrateError,
    JsTaskQueue, JsTraceEvent, JsTracePhase,
};

use super::{
    artifact::{
        JsBinaryOperator, JsCodeBlockId, JsCompileError, JsCompiledArtifact,
        JsCompiledArtifactKind, JsCompiledCodeBlock, JsCompiledExport, JsInstruction,
        JsUnaryOperator, compile_module_artifact, compile_script_artifact,
        compile_synthetic_host_module_artifact,
    },
    context::{JsAttachedRuntimeContext, JsContextError},
};

#[derive(Clone)]
pub struct EngineJsRuntimeHost {
    name: Arc<str>,
    scheduler: Arc<dyn JsScheduler>,
    clock: Arc<dyn JsClock>,
    entropy: Arc<dyn crate::JsEntropySource>,
    module_loader: Arc<dyn JsModuleLoader>,
    host_services: Arc<dyn JsHostServices>,
    hooks: Arc<dyn JsExecutionHooks>,
    fork_policy: JsForkPolicy,
}

impl EngineJsRuntimeHost {
    pub fn new(
        module_loader: Arc<dyn JsModuleLoader>,
        host_services: Arc<dyn JsHostServices>,
    ) -> Self {
        Self {
            name: Arc::from("terrace-js"),
            scheduler: Arc::new(crate::DeterministicJsScheduler::default()),
            clock: Arc::new(FixedJsClock::default()),
            entropy: Arc::new(DeterministicJsEntropySource::default()),
            module_loader,
            host_services,
            hooks: Arc::new(crate::NoopJsExecutionHooks),
            fork_policy: JsForkPolicy::simulation_native_baseline(),
        }
    }

    pub fn with_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_scheduler(mut self, scheduler: Arc<dyn JsScheduler>) -> Self {
        self.scheduler = scheduler;
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn JsClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_entropy(mut self, entropy: Arc<dyn crate::JsEntropySource>) -> Self {
        self.entropy = entropy;
        self
    }

    pub fn with_hooks(mut self, hooks: Arc<dyn JsExecutionHooks>) -> Self {
        self.hooks = hooks;
        self
    }

    pub fn with_fork_policy(mut self, fork_policy: JsForkPolicy) -> Self {
        self.fork_policy = fork_policy;
        self
    }
}

#[async_trait(?Send)]
impl JsRuntimeHost for EngineJsRuntimeHost {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn fork_policy(&self) -> JsForkPolicy {
        self.fork_policy.clone()
    }

    fn scheduler(&self) -> Arc<dyn JsScheduler> {
        self.scheduler.clone()
    }

    fn module_loader(&self) -> Arc<dyn JsModuleLoader> {
        self.module_loader.clone()
    }

    fn host_services(&self) -> Arc<dyn JsHostServices> {
        self.host_services.clone()
    }

    async fn open_runtime(
        &self,
        request: JsRuntimeOpenRequest,
    ) -> Result<Arc<dyn JsRuntime>, JsSubstrateError> {
        let handle = JsRuntimeHandle {
            runtime_id: request.runtime_id.clone(),
            backend: self.name.to_string(),
            policy: request.policy,
            provenance: request.provenance,
            environment: request.environment,
            metadata: request.metadata,
        };
        let configuration = JsRuntimeConfiguration::from_handle(&handle);
        self.hooks.on_runtime_open(&handle).await?;
        let context = JsRuntimeContext::builder(configuration.clone())
            .with_services(JsContextServiceConfiguration::default())
            .build()
            .map_err(context_error_to_substrate)?;
        Ok(Arc::new(EngineJsRuntime {
            handle,
            configuration,
            scheduler: self.scheduler.clone(),
            clock: self.clock.clone(),
            entropy: self.entropy.clone(),
            module_loader: self.module_loader.clone(),
            host_services: self.host_services.clone(),
            hooks: self.hooks.clone(),
            context,
            execution_lock: AsyncMutex::new(()),
            attachment: AsyncMutex::new(JsRuntimeAttachmentState::default()),
            state: AsyncMutex::new(EngineRuntimeState::default()),
        }))
    }
}

struct EngineJsRuntime {
    handle: JsRuntimeHandle,
    configuration: JsRuntimeConfiguration,
    scheduler: Arc<dyn JsScheduler>,
    clock: Arc<dyn JsClock>,
    entropy: Arc<dyn crate::JsEntropySource>,
    module_loader: Arc<dyn JsModuleLoader>,
    host_services: Arc<dyn JsHostServices>,
    hooks: Arc<dyn JsExecutionHooks>,
    context: JsRuntimeContext,
    execution_lock: AsyncMutex<()>,
    attachment: AsyncMutex<JsRuntimeAttachmentState>,
    state: AsyncMutex<EngineRuntimeState>,
}

#[async_trait(?Send)]
impl JsRuntime for EngineJsRuntime {
    fn handle(&self) -> JsRuntimeHandle {
        self.handle.clone()
    }

    fn configuration(&self) -> JsRuntimeConfiguration {
        self.configuration.clone()
    }

    async fn attachment_state(&self) -> Result<JsRuntimeAttachmentState, JsSubstrateError> {
        Ok(self.attachment.lock().await.clone())
    }

    async fn suspended_state(&self) -> Result<JsRuntimeSuspendedState, JsSubstrateError> {
        Ok(self.state.lock().await.suspended_snapshot())
    }

    async fn deliver_host_completion(
        &self,
        completion: crate::JsCompletedHostOperation,
    ) -> Result<(), JsSubstrateError> {
        let _guard = self.execution_lock.lock().await;
        let mut state = self.state.lock().await;
        state.accept_host_completion(completion)
    }

    async fn run_turn(
        &self,
        turn: JsRuntimeTurn,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Result<JsRuntimeTurnOutcome, JsSubstrateError> {
        let _guard = self.execution_lock.lock().await;
        if cancellation.is_cancelled() {
            return Err(JsSubstrateError::Cancelled {
                runtime_id: self.handle.runtime_id.clone(),
            });
        }

        self.attach(js_turn_kind_label(&turn.kind)).await;
        let mut attached = self
            .context
            .attach(js_turn_kind_label(&turn.kind))
            .map_err(context_error_to_substrate)?;
        attached.begin_turn(js_turn_kind_label(&turn.kind));

        let outcome = {
            let entropy_sample = self.entropy.fill_bytes(8);
            let mut state = self.state.lock().await;
            state.run_turn(
                &self.handle,
                &self.configuration.environment,
                self.clock.now_millis(),
                &entropy_sample,
                &turn,
                &mut attached,
            )
        };

        attached.reach_safepoint(js_turn_kind_label(&turn.kind));
        let _ = attached.collect_garbage();
        let _ = attached.detach().map_err(context_error_to_substrate)?;
        self.detach().await;
        outcome
    }

    async fn execute(
        &self,
        request: JsExecutionRequest,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Result<JsExecutionReport, JsSubstrateError> {
        {
            let mut state = self.state.lock().await;
            state
                .prepare_execution(
                    &self.handle,
                    &request,
                    self.clock.now_millis(),
                    self.module_loader.clone(),
                    self.scheduler.clone(),
                    self.hooks.clone(),
                )
                .await?;
        }

        let mut next_turn = JsRuntimeTurn::evaluate_entrypoint(request.clone());
        loop {
            match self
                .run_turn(next_turn.clone(), cancellation.clone())
                .await?
            {
                JsRuntimeTurnOutcome::Completed { completion } => {
                    if let Some(mut report) = completion.execution {
                        report.scheduled_tasks = self.scheduler.drain().await;
                        return Ok(report);
                    }
                    let mut state = self.state.lock().await;
                    if let Some(mut report) = state.try_finalize_report(
                        &self.handle,
                        self.clock.now_millis(),
                        self.entropy.fill_bytes(8),
                    )? {
                        drop(state);
                        report.scheduled_tasks = self.scheduler.drain().await;
                        return Ok(report);
                    }
                    if state.has_pending_microtasks() {
                        next_turn = JsRuntimeTurn {
                            kind: JsRuntimeTurnKind::DrainMicrotasks,
                            metadata: BTreeMap::new(),
                        };
                        continue;
                    }
                    return Err(JsSubstrateError::EvaluationFailed {
                        entrypoint: self.handle.runtime_id.clone(),
                        message: "runtime completed without an execution report".to_string(),
                    });
                }
                JsRuntimeTurnOutcome::PendingMicrotasks => {
                    next_turn = JsRuntimeTurn {
                        kind: JsRuntimeTurnKind::DrainMicrotasks,
                        metadata: BTreeMap::new(),
                    };
                }
                JsRuntimeTurnOutcome::PendingHostOp { op_id } => {
                    let operation = {
                        let state = self.state.lock().await;
                        state.pending_operation(op_id)?
                    };
                    match operation {
                        EnginePendingOperation::HostService(call) => {
                            let response = self.host_services.call(call.request.clone()).await?;
                            let record = JsHostServiceCallRecord {
                                service: call.request.service.clone(),
                                operation: call.request.operation.clone(),
                                arguments: call.request.arguments.clone(),
                                result: response.result.clone(),
                                metadata: response.metadata.clone(),
                            };
                            self.hooks
                                .on_host_service_call(&call.request, &record)
                                .await?;
                            self.deliver_host_completion(crate::JsCompletedHostOperation {
                                op_id,
                                result: response.result,
                                error: None,
                                metadata: response.metadata,
                            })
                            .await?;
                        }
                        EnginePendingOperation::ModuleLoad(load) => {
                            let resolved = self
                                .module_loader
                                .resolve(load.requested.as_str(), load.referrer.as_deref())
                                .await?;
                            let loaded = self.module_loader.load(&resolved).await?;
                            self.hooks.on_module_loaded(&loaded).await?;
                            self.deliver_host_completion(crate::JsCompletedHostOperation {
                                op_id,
                                result: Some(encode_loaded_module_completion(&loaded)?),
                                error: None,
                                metadata: BTreeMap::from([(
                                    "operation_kind".to_string(),
                                    JsonValue::String("module_load".to_string()),
                                )]),
                            })
                            .await?;
                        }
                    }
                    next_turn = JsRuntimeTurn {
                        kind: JsRuntimeTurnKind::DeliverHostCompletion { op_id },
                        metadata: BTreeMap::new(),
                    };
                }
                JsRuntimeTurnOutcome::PendingTimer { deadline_millis } => {
                    return Err(JsSubstrateError::EvaluationFailed {
                        entrypoint: self.handle.runtime_id.clone(),
                        message: format!("runtime is waiting on timer deadline {deadline_millis}"),
                    });
                }
                JsRuntimeTurnOutcome::Yielded { .. } => {
                    next_turn = JsRuntimeTurn {
                        kind: JsRuntimeTurnKind::DrainMicrotasks,
                        metadata: BTreeMap::new(),
                    };
                }
                JsRuntimeTurnOutcome::Threw { error } => {
                    return Err(JsSubstrateError::EvaluationFailed {
                        entrypoint: self.handle.runtime_id.clone(),
                        message: error.message,
                    });
                }
                JsRuntimeTurnOutcome::Terminated { reason } => {
                    return Err(JsSubstrateError::EvaluationFailed {
                        entrypoint: self.handle.runtime_id.clone(),
                        message: reason,
                    });
                }
            }
        }
    }

    async fn close(&self) -> Result<(), JsSubstrateError> {
        self.hooks.on_runtime_close(&self.handle).await
    }
}

impl EngineJsRuntime {
    async fn attach(&self, turn: &str) {
        let mut attachment = self.attachment.lock().await;
        attachment.attached = true;
        attachment.attachment_epoch = attachment.attachment_epoch.saturating_add(1);
        attachment.worker_hint = Some(format!("{:?}", std::thread::current().id()));
        attachment.current_turn = Some(turn.to_string());
    }

    async fn detach(&self) {
        let mut attachment = self.attachment.lock().await;
        attachment.attached = false;
        attachment.current_turn = None;
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct JsEnvironmentId(u64);

impl JsEnvironmentId {
    fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct JsFunctionId(u64);

impl JsFunctionId {
    fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum EngineValue {
    Undefined,
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Object(JsHeapObjectId),
    Function(JsFunctionId),
    HostFunction(EngineHostFunction),
    Promise(crate::JsPromiseId),
}

#[derive(Clone, Debug, PartialEq)]
enum EngineHostFunction {
    DateNow,
    MathRandom,
    PromiseResolve,
    Capability { service: String, operation: String },
    PromiseThen { promise_id: crate::JsPromiseId },
}

#[derive(Clone, Debug)]
struct EngineObject {
    properties: BTreeMap<String, EngineValue>,
}

#[derive(Clone, Debug)]
struct EngineEnvironment {
    parent: Option<JsEnvironmentId>,
    bindings: BTreeMap<String, EngineValue>,
}

#[derive(Clone, Debug)]
struct EngineFunction {
    code_block: JsCodeBlockId,
    captured_env: JsEnvironmentId,
    async_function: bool,
}

#[derive(Clone, Debug)]
struct EnginePromise {
    state: EnginePromiseState,
}

#[derive(Clone, Debug)]
enum EnginePromiseState {
    Pending {
        op_id: Option<crate::JsHostOpId>,
        reactions: Vec<EnginePromiseReaction>,
    },
    Fulfilled(EngineValue),
    Rejected(JsRuntimeErrorReport),
}

#[derive(Clone, Debug)]
struct EnginePromiseReaction {
    callback: EngineValue,
    chained_promise: crate::JsPromiseId,
}

#[derive(Clone, Debug)]
struct EngineMicrotask {
    callback: EngineValue,
    argument: EngineValue,
    chained_promise: crate::JsPromiseId,
}

#[derive(Clone, Debug)]
struct EnginePendingHostCall {
    request: JsHostServiceRequest,
    promise_id: crate::JsPromiseId,
}

#[derive(Clone, Debug)]
struct EnginePendingModuleLoad {
    requested: String,
    referrer: Option<String>,
    parent: Option<JsModuleId>,
}

#[derive(Clone, Debug)]
enum EnginePendingOperation {
    HostService(EnginePendingHostCall),
    ModuleLoad(EnginePendingModuleLoad),
}

#[derive(Clone, Debug)]
struct EngineModuleRecord {
    module_id: JsModuleId,
    resolved: JsResolvedModule,
    artifact: JsCompiledArtifact,
    dependencies: Vec<JsModuleId>,
    exports: BTreeMap<String, EngineValue>,
    evaluated: bool,
}

#[derive(Clone, Debug)]
struct EngineExecutionSession {
    entrypoint: String,
    root_kind: EngineRootKind,
    root_module_id: Option<JsModuleId>,
    module_frames_initialized: bool,
    pending_module_requests: VecDeque<EnginePendingModuleLoad>,
    frames: Vec<EngineFrame>,
    result: Option<EngineValue>,
    trace: Vec<JsTraceEvent>,
    host_calls: Vec<JsHostServiceCallRecord>,
    module_graph: Vec<String>,
}

#[derive(Clone, Debug)]
enum EngineRootKind {
    Script,
    Module,
}

#[derive(Clone, Debug)]
struct EngineFrame {
    code_block: JsCodeBlockId,
    env_id: Option<JsEnvironmentId>,
    pc: usize,
    stack: Vec<EngineValue>,
    kind: EngineFrameKind,
}

#[derive(Clone, Debug)]
enum EngineFrameKind {
    RootScript,
    Module { module_id: JsModuleId },
    Call { return_target: EngineReturnTarget },
    Microtask { promise_id: crate::JsPromiseId },
}

#[derive(Clone, Debug)]
enum EngineReturnTarget {
    ParentFrame,
}

#[derive(Default)]
struct EngineRuntimeState {
    bootstrapped: bool,
    intrinsics_initialized: bool,
    next_env_id: u64,
    next_function_id: u64,
    environments: BTreeMap<JsEnvironmentId, EngineEnvironment>,
    global_env_id: Option<JsEnvironmentId>,
    functions: BTreeMap<JsFunctionId, EngineFunction>,
    objects: BTreeMap<JsHeapObjectId, EngineObject>,
    promises: BTreeMap<crate::JsPromiseId, EnginePromise>,
    microtasks: VecDeque<EngineMicrotask>,
    module_ids_by_specifier: BTreeMap<String, JsModuleId>,
    modules: BTreeMap<JsModuleId, EngineModuleRecord>,
    pending_host_ops: BTreeMap<crate::JsHostOpId, EnginePendingOperation>,
    completed_host_ops: BTreeMap<crate::JsHostOpId, crate::JsCompletedHostOperation>,
    execution: Option<EngineExecutionSession>,
    next_timer_id: u64,
    next_task_id: u64,
    next_host_op_id: u64,
    next_callback_id: u64,
    next_promise_id: u64,
    next_module_id: u64,
    current_clock_now_millis: u64,
    current_entropy_seed: [u8; 8],
    current_entropy_counter: u64,
}

impl EngineRuntimeState {
    fn suspended_snapshot(&self) -> JsRuntimeSuspendedState {
        JsRuntimeSuspendedState {
            next_timer_id: self.next_timer_id,
            next_task_id: self.next_task_id,
            next_host_op_id: self.next_host_op_id,
            next_callback_id: self.next_callback_id,
            next_promise_id: self.next_promise_id,
            next_module_id: self.next_module_id,
            pending_timers: Vec::new(),
            pending_tasks: Vec::new(),
            pending_host_ops: self
                .pending_host_ops
                .iter()
                .map(|(op_id, op)| {
                    let (operation, metadata) = match op {
                        EnginePendingOperation::HostService(call) => (
                            format!("{}::{}", call.request.service, call.request.operation),
                            call.request.metadata.clone(),
                        ),
                        EnginePendingOperation::ModuleLoad(load) => (
                            "module_loader::load".to_string(),
                            BTreeMap::from([
                                (
                                    "requested".to_string(),
                                    JsonValue::String(load.requested.clone()),
                                ),
                                (
                                    "referrer".to_string(),
                                    load.referrer
                                        .clone()
                                        .map(JsonValue::String)
                                        .unwrap_or(JsonValue::Null),
                                ),
                            ]),
                        ),
                    };
                    (
                        *op_id,
                        crate::JsPendingHostOperation {
                            op_id: *op_id,
                            callback_id: None,
                            operation,
                            metadata,
                        },
                    )
                })
                .collect(),
            completed_host_ops: self.completed_host_ops.clone(),
            pending_microtasks: self.microtasks.len(),
            module_graph: self
                .modules
                .values()
                .map(|module| {
                    (
                        module.module_id,
                        JsModuleGraphNode {
                            module_id: module.module_id,
                            specifier: module.resolved.canonical_specifier.clone(),
                            dependencies: module.dependencies.clone(),
                            evaluated: module.evaluated,
                        },
                    )
                })
                .collect(),
            terminated: false,
            metadata: BTreeMap::new(),
        }
    }

    async fn prepare_execution(
        &mut self,
        handle: &JsRuntimeHandle,
        request: &JsExecutionRequest,
        _clock_now_millis: u64,
        module_loader: Arc<dyn JsModuleLoader>,
        scheduler: Arc<dyn JsScheduler>,
        hooks: Arc<dyn JsExecutionHooks>,
    ) -> Result<(), JsSubstrateError> {
        self.bootstrap()?;
        self.execution = None;

        let mut session = EngineExecutionSession {
            entrypoint: String::new(),
            root_kind: EngineRootKind::Script,
            root_module_id: None,
            module_frames_initialized: false,
            pending_module_requests: VecDeque::new(),
            frames: Vec::new(),
            result: None,
            trace: Vec::new(),
            host_calls: Vec::new(),
            module_graph: Vec::new(),
        };

        match &request.kind {
            JsExecutionKind::Module { specifier } => {
                session.entrypoint = specifier.clone();
                session.root_kind = EngineRootKind::Module;
                session
                    .pending_module_requests
                    .push_back(EnginePendingModuleLoad {
                        requested: specifier.clone(),
                        referrer: None,
                        parent: None,
                    });
            }
            JsExecutionKind::Eval {
                source,
                virtual_specifier,
            } => {
                let specifier = virtual_specifier
                    .clone()
                    .unwrap_or_else(|| "terrace:/eval/inline.mjs".to_string());
                let artifact = self.compile_eval_artifact(&specifier, source)?;
                let module_id = self.next_module_id();
                session.entrypoint = specifier.clone();
                session.trace.push(JsTraceEvent {
                    phase: JsTracePhase::ModuleLoad,
                    label: specifier.clone(),
                    metadata: BTreeMap::from([("eval".to_string(), JsonValue::Bool(true))]),
                });
                session.frames.push(EngineFrame {
                    code_block: artifact.main,
                    env_id: None,
                    pc: 0,
                    stack: Vec::new(),
                    kind: EngineFrameKind::RootScript,
                });
                self.modules.insert(
                    module_id,
                    EngineModuleRecord {
                        module_id,
                        resolved: JsResolvedModule {
                            requested_specifier: specifier.clone(),
                            canonical_specifier: specifier.clone(),
                            kind: crate::JsModuleKind::Workspace,
                        },
                        artifact,
                        dependencies: Vec::new(),
                        exports: BTreeMap::new(),
                        evaluated: false,
                    },
                );
                session.root_kind = EngineRootKind::Script;
            }
        }

        scheduler
            .schedule(JsScheduledTask::ready(
                JsTaskQueue::ModuleLoader,
                session.entrypoint.clone(),
            ))
            .await;
        self.execution = Some(session);
        let _ = (handle, module_loader, hooks);
        Ok(())
    }

    fn bootstrap(&mut self) -> Result<(), JsSubstrateError> {
        if self.bootstrapped {
            return Ok(());
        }
        let global_env_id = self.allocate_env(None);
        self.global_env_id = Some(global_env_id);
        self.bootstrapped = true;
        Ok(())
    }

    fn begin_turn(&mut self, clock_now_millis: u64, entropy_sample: &[u8]) {
        self.current_clock_now_millis = clock_now_millis;
        let mut seed = [0_u8; 8];
        for (index, byte) in entropy_sample.iter().copied().enumerate().take(8) {
            seed[index] = byte;
        }
        self.current_entropy_seed = seed;
        self.current_entropy_counter = 0;
    }

    fn ensure_intrinsics(
        &mut self,
        attached: &mut JsAttachedRuntimeContext<'_>,
    ) -> Result<(), JsSubstrateError> {
        if self.intrinsics_initialized {
            return Ok(());
        }
        let global_env_id = self
            .global_env_id
            .expect("bootstrap must initialize the global environment");
        let date_object = self.create_plain_object_with_heap(attached)?;
        self.set_object_property(
            date_object,
            "now",
            EngineValue::HostFunction(EngineHostFunction::DateNow),
        )?;
        let math_object = self.create_plain_object_with_heap(attached)?;
        self.set_object_property(
            math_object,
            "random",
            EngineValue::HostFunction(EngineHostFunction::MathRandom),
        )?;
        let promise_object = self.create_plain_object_with_heap(attached)?;
        self.set_object_property(
            promise_object,
            "resolve",
            EngineValue::HostFunction(EngineHostFunction::PromiseResolve),
        )?;
        self.define_binding(global_env_id, "Date", EngineValue::Object(date_object));
        self.define_binding(global_env_id, "Math", EngineValue::Object(math_object));
        self.define_binding(
            global_env_id,
            "Promise",
            EngineValue::Object(promise_object),
        );
        self.intrinsics_initialized = true;
        Ok(())
    }

    fn compile_loaded_module(
        &self,
        loaded: &JsLoadedModule,
    ) -> Result<JsCompiledArtifact, JsSubstrateError> {
        if loaded.resolved.kind == crate::JsModuleKind::HostCapability {
            let service = loaded
                .metadata
                .get("host_service")
                .and_then(JsonValue::as_str)
                .unwrap_or(loaded.resolved.canonical_specifier.as_str());
            let exports = loaded
                .metadata
                .get("host_exports")
                .and_then(JsonValue::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(JsonValue::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            return compile_synthetic_host_module_artifact(
                loaded.resolved.canonical_specifier.clone(),
                service.to_string(),
                exports,
            )
            .map_err(compile_error_to_substrate);
        }
        compile_module_artifact(
            loaded.resolved.canonical_specifier.clone(),
            loaded.source.clone(),
        )
        .map_err(compile_error_to_substrate)
    }

    fn compile_eval_artifact(
        &self,
        specifier: &str,
        source: &str,
    ) -> Result<JsCompiledArtifact, JsSubstrateError> {
        match compile_module_artifact(specifier.to_string(), source.to_string()) {
            Ok(artifact) => Ok(artifact),
            Err(_) => compile_script_artifact(specifier.to_string(), source.to_string())
                .map_err(compile_error_to_substrate),
        }
    }

    fn module_evaluation_order(
        &self,
        root: JsModuleId,
    ) -> Result<Vec<JsModuleId>, JsSubstrateError> {
        fn visit(
            current: JsModuleId,
            modules: &BTreeMap<JsModuleId, EngineModuleRecord>,
            seen: &mut BTreeSet<JsModuleId>,
            order: &mut Vec<JsModuleId>,
        ) -> Result<(), JsSubstrateError> {
            if !seen.insert(current) {
                return Ok(());
            }
            let module =
                modules
                    .get(&current)
                    .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                        entrypoint: format!("module-{current:?}"),
                        message: "missing module during evaluation ordering".to_string(),
                    })?;
            for dependency in &module.dependencies {
                visit(*dependency, modules, seen, order)?;
            }
            order.push(current);
            Ok(())
        }
        let mut seen = BTreeSet::new();
        let mut order = Vec::new();
        visit(root, &self.modules, &mut seen, &mut order)?;
        Ok(order)
    }

    fn run_turn(
        &mut self,
        handle: &JsRuntimeHandle,
        environment: &JsRuntimeEnvironment,
        clock_now_millis: u64,
        entropy_sample: &[u8],
        turn: &JsRuntimeTurn,
        attached: &mut JsAttachedRuntimeContext<'_>,
    ) -> Result<JsRuntimeTurnOutcome, JsSubstrateError> {
        self.bootstrap()?;
        self.begin_turn(clock_now_millis, entropy_sample);
        self.ensure_intrinsics(attached)?;
        match &turn.kind {
            JsRuntimeTurnKind::Bootstrap => Ok(JsRuntimeTurnOutcome::Completed {
                completion: JsRuntimeTurnCompletion::default(),
            }),
            JsRuntimeTurnKind::EvaluateEntrypoint { .. }
            | JsRuntimeTurnKind::DrainMicrotasks
            | JsRuntimeTurnKind::DeliverHostCompletion { .. } => {
                self.run_execution_turn(handle, environment, clock_now_millis, turn, attached)
            }
            JsRuntimeTurnKind::EvaluateModule { .. }
            | JsRuntimeTurnKind::DeliverTimer { .. }
            | JsRuntimeTurnKind::DeliverTask { .. } => Err(JsSubstrateError::UnsupportedTurn {
                turn: js_turn_kind_label(&turn.kind).to_string(),
            }),
        }
    }

    fn run_execution_turn(
        &mut self,
        handle: &JsRuntimeHandle,
        environment: &JsRuntimeEnvironment,
        clock_now_millis: u64,
        turn: &JsRuntimeTurn,
        attached: &mut JsAttachedRuntimeContext<'_>,
    ) -> Result<JsRuntimeTurnOutcome, JsSubstrateError> {
        if let JsRuntimeTurnKind::DeliverHostCompletion { op_id } = turn.kind {
            self.deliver_host_completion(op_id)?;
        } else if matches!(turn.kind, JsRuntimeTurnKind::DrainMicrotasks) {
            self.begin_microtask_turn()?;
        }

        if let Some(outcome) = self.poll_module_loading(handle)? {
            return Ok(outcome);
        }

        while self
            .execution
            .as_ref()
            .is_some_and(|execution| !execution.frames.is_empty())
        {
            let instruction = {
                let execution = self.execution.as_ref().expect("execution session exists");
                let frame = execution.frames.last().expect("frame exists");
                let module = self.code_block(frame.code_block)?;
                module.instructions.get(frame.pc).cloned().ok_or_else(|| {
                    JsSubstrateError::EvaluationFailed {
                        entrypoint: handle.runtime_id.clone(),
                        message: format!("program counter out of bounds in {}", module.label),
                    }
                })?
            };
            let outcome = self.execute_instruction(
                handle,
                environment,
                clock_now_millis,
                attached,
                instruction,
            )?;
            if let Some(outcome) = outcome {
                return Ok(outcome);
            }
        }

        if self.has_pending_microtasks() {
            return Ok(JsRuntimeTurnOutcome::PendingMicrotasks);
        }

        if let Some(report) = self.try_finalize_report(handle, clock_now_millis, Vec::new())? {
            return Ok(JsRuntimeTurnOutcome::Completed {
                completion: JsRuntimeTurnCompletion {
                    execution: Some(report),
                    metadata: BTreeMap::new(),
                },
            });
        }

        Ok(JsRuntimeTurnOutcome::Completed {
            completion: JsRuntimeTurnCompletion::default(),
        })
    }

    fn poll_module_loading(
        &mut self,
        handle: &JsRuntimeHandle,
    ) -> Result<Option<JsRuntimeTurnOutcome>, JsSubstrateError> {
        if self
            .execution
            .as_ref()
            .is_some_and(|execution| !execution.frames.is_empty())
        {
            return Ok(None);
        }

        let pending_request = self
            .execution
            .as_mut()
            .and_then(|execution| execution.pending_module_requests.pop_front());
        if let Some(request) = pending_request {
            let op_id = self.next_host_op_id();
            self.pending_host_ops
                .insert(op_id, EnginePendingOperation::ModuleLoad(request));
            return Ok(Some(JsRuntimeTurnOutcome::PendingHostOp { op_id }));
        }

        let Some((should_initialize, root_module_id)) = self.execution.as_ref().map(|execution| {
            (
                matches!(execution.root_kind, EngineRootKind::Module)
                    && !execution.module_frames_initialized
                    && execution.frames.is_empty(),
                execution.root_module_id,
            )
        }) else {
            return Ok(None);
        };

        if !should_initialize {
            return Ok(None);
        }

        let root_module_id = root_module_id.ok_or_else(|| JsSubstrateError::EvaluationFailed {
            entrypoint: handle.runtime_id.clone(),
            message: "module execution started before the root module was loaded".to_string(),
        })?;
        let order = self.module_evaluation_order(root_module_id)?;
        let mut frames = Vec::with_capacity(order.len());
        let mut module_graph = Vec::with_capacity(order.len());
        for module_id in order.iter().rev().copied() {
            let module = self
                .modules
                .get(&module_id)
                .expect("module order references a known module");
            module_graph.push(module.resolved.canonical_specifier.clone());
            frames.push(EngineFrame {
                code_block: module.artifact.main,
                env_id: None,
                pc: 0,
                stack: Vec::new(),
                kind: EngineFrameKind::Module { module_id },
            });
        }
        if let Some(execution) = self.execution.as_mut() {
            execution.module_frames_initialized = true;
            execution.module_graph.extend(module_graph);
            execution.frames.extend(frames);
        }
        Ok(None)
    }

    fn execute_instruction(
        &mut self,
        handle: &JsRuntimeHandle,
        environment: &JsRuntimeEnvironment,
        clock_now_millis: u64,
        attached: &mut JsAttachedRuntimeContext<'_>,
        instruction: JsInstruction,
    ) -> Result<Option<JsRuntimeTurnOutcome>, JsSubstrateError> {
        self.initialize_top_frame(environment, attached)?;
        self.advance_pc()?;
        match instruction {
            JsInstruction::PushUndefined => self.push_stack(EngineValue::Undefined)?,
            JsInstruction::PushNull => self.push_stack(EngineValue::Null)?,
            JsInstruction::PushBool { value } => self.push_stack(EngineValue::Bool(value))?,
            JsInstruction::PushNumber { value } => self.push_stack(EngineValue::Number(value))?,
            JsInstruction::PushString { value } => self.push_stack(EngineValue::String(value))?,
            JsInstruction::PushBuiltin { name } => {
                let value = self
                    .lookup_name(self.global_env_id.expect("global env"), &name)
                    .unwrap_or(EngineValue::Undefined);
                self.push_stack(value)?;
            }
            JsInstruction::LoadName { name } => {
                let value = self
                    .lookup_name(self.current_frame_env_id()?, &name)
                    .unwrap_or(EngineValue::Undefined);
                self.push_stack(value)?;
            }
            JsInstruction::DeclareName { name, .. } => {
                self.define_binding(self.current_frame_env_id()?, &name, EngineValue::Undefined);
            }
            JsInstruction::StoreName { name } => {
                let value = self.pop_stack();
                self.store_name(self.current_frame_env_id()?, &name, value.clone());
                self.push_stack(value)?;
            }
            JsInstruction::Pop => {
                let _ = self.pop_stack();
            }
            JsInstruction::Dup => {
                let value = self.peek_stack().unwrap_or(EngineValue::Undefined);
                self.push_stack(value)?;
            }
            JsInstruction::CreateObject => {
                let object_id = self.create_plain_object_with_heap(attached)?;
                self.push_stack(EngineValue::Object(object_id))?;
            }
            JsInstruction::CreateArray { element_count } => {
                let mut elements = self.pop_arguments(element_count);
                elements.reverse();
                let object = self.create_plain_object_with_heap(attached)?;
                for (index, value) in elements.into_iter().enumerate() {
                    self.set_object_property(object, index.to_string(), value)?;
                }
                self.push_stack(EngineValue::Object(object))?;
            }
            JsInstruction::DefineProperty { name } => {
                let value = self.pop_stack();
                let object = self.pop_stack();
                let object_id = self.expect_object(object, handle)?;
                self.set_object_property(object_id, name, value)?;
                self.push_stack(EngineValue::Object(object_id))?;
            }
            JsInstruction::GetProperty { name } => {
                let object = self.pop_stack();
                let value = self.get_named_property(object, &name, handle)?;
                self.push_stack(value)?;
            }
            JsInstruction::GetPropertyDynamic => {
                let field = self.pop_stack();
                let object = self.pop_stack();
                let name = self.expect_string(field, handle)?;
                let value = self.get_named_property(object, &name, handle)?;
                self.push_stack(value)?;
            }
            JsInstruction::SetProperty { name } => {
                let value = self.pop_stack();
                let object = self.pop_stack();
                let object_id = self.expect_object(object, handle)?;
                self.set_object_property(object_id, name, value.clone())?;
                self.push_stack(value)?;
            }
            JsInstruction::SetPropertyDynamic => {
                let field = self.pop_stack();
                let value = self.pop_stack();
                let object = self.pop_stack();
                let name = self.expect_string(field, handle)?;
                let object_id = self.expect_object(object, handle)?;
                self.set_object_property(object_id, name, value.clone())?;
                self.push_stack(value)?;
            }
            JsInstruction::CreateFunction {
                code_block,
                async_function,
            } => {
                let function_id = self.allocate_function(
                    code_block,
                    self.current_frame_env_id()?,
                    async_function,
                );
                self.push_stack(EngineValue::Function(function_id))?;
            }
            JsInstruction::Call { argc } => {
                let mut args = self.pop_arguments(argc);
                let callee = self.pop_stack();
                if let Some(outcome) =
                    self.invoke_callable(callee, None, &mut args, handle, attached)?
                {
                    return Ok(Some(outcome));
                }
            }
            JsInstruction::CallMethod { name, argc } => {
                let mut args = self.pop_arguments(argc);
                let receiver = self.pop_stack();
                let callee = self.get_named_property(receiver.clone(), &name, handle)?;
                if let Some(outcome) =
                    self.invoke_callable(callee, Some(receiver), &mut args, handle, attached)?
                {
                    return Ok(Some(outcome));
                }
            }
            JsInstruction::Await => {
                let awaited = self.pop_stack();
                match awaited {
                    EngineValue::Promise(promise_id) => match self.promise_state(promise_id)? {
                        EnginePromiseState::Fulfilled(value) => self.push_stack(value)?,
                        EnginePromiseState::Rejected(error) => {
                            return Ok(Some(JsRuntimeTurnOutcome::Threw { error }));
                        }
                        EnginePromiseState::Pending {
                            op_id: Some(op_id), ..
                        } => {
                            self.rewind_pc()?;
                            self.push_stack(EngineValue::Promise(promise_id))?;
                            return Ok(Some(JsRuntimeTurnOutcome::PendingHostOp { op_id }));
                        }
                        EnginePromiseState::Pending { op_id: None, .. } => {
                            self.rewind_pc()?;
                            self.push_stack(EngineValue::Promise(promise_id))?;
                            return Ok(Some(JsRuntimeTurnOutcome::PendingMicrotasks));
                        }
                    },
                    other => self.push_stack(other)?,
                }
            }
            JsInstruction::TypeOf => {
                let value = self.pop_stack();
                self.push_stack(EngineValue::String(type_of_value(&value).to_string()))?;
            }
            JsInstruction::Unary { op } => {
                let value = self.pop_stack();
                self.push_stack(apply_unary(op, value, handle)?)?;
            }
            JsInstruction::Binary { op } => {
                let rhs = self.pop_stack();
                let lhs = self.pop_stack();
                self.push_stack(apply_binary(op, lhs, rhs, handle)?)?;
            }
            JsInstruction::Return => {
                let value = self.pop_stack();
                self.complete_frame(value, handle)?;
            }
        }

        let _ = clock_now_millis;
        Ok(None)
    }

    fn initialize_top_frame(
        &mut self,
        environment: &JsRuntimeEnvironment,
        attached: &mut JsAttachedRuntimeContext<'_>,
    ) -> Result<(), JsSubstrateError> {
        let Some((needs_init, frame_kind)) = self.execution.as_ref().and_then(|execution| {
            execution
                .frames
                .last()
                .map(|frame| (frame.env_id.is_none(), frame.kind.clone()))
        }) else {
            return Ok(());
        };
        if !needs_init {
            return Ok(());
        }

        let env_id = self.allocate_env(self.global_env_id);
        match frame_kind {
            EngineFrameKind::RootScript => {}
            EngineFrameKind::Module { module_id } => {
                let module = self
                    .modules
                    .get(&module_id)
                    .expect("module frame references an existing module");
                let artifact_kind = module.artifact.kind.clone();
                let artifact_exports = module.artifact.exports.clone();
                let artifact_imports = module.artifact.imports.clone();
                let synthetic_service = module
                    .artifact
                    .metadata
                    .get("synthetic_host_service")
                    .and_then(JsonValue::as_str)
                    .map(ToOwned::to_owned);
                if artifact_kind == JsCompiledArtifactKind::SyntheticModule {
                    if let Some(service) = synthetic_service {
                        for export in &artifact_exports {
                            self.define_binding(
                                env_id,
                                export.local_name.clone(),
                                EngineValue::HostFunction(EngineHostFunction::Capability {
                                    service: service.clone(),
                                    operation: export.exported_name.clone(),
                                }),
                            );
                        }
                    }
                }
                for import in &artifact_imports {
                    let dependency =
                        self.resolve_module_dependency(module_id, import.request.as_str())?;
                    let exports = self
                        .modules
                        .get(&dependency)
                        .expect("dependency exists")
                        .exports
                        .clone();
                    if let Some(default_binding) = &import.default_binding {
                        self.define_binding(
                            env_id,
                            default_binding,
                            exports
                                .get("default")
                                .cloned()
                                .unwrap_or(EngineValue::Undefined),
                        );
                    }
                    if let Some(namespace_binding) = &import.namespace_binding {
                        let object = self.create_plain_object_with_heap(attached)?;
                        for (key, value) in &exports {
                            self.set_object_property(object, key.clone(), value.clone())?;
                        }
                        self.define_binding(env_id, namespace_binding, EngineValue::Object(object));
                    }
                    for (local_name, import_name) in &import.named_bindings {
                        self.define_binding(
                            env_id,
                            local_name,
                            exports
                                .get(import_name)
                                .cloned()
                                .unwrap_or(EngineValue::Undefined),
                        );
                    }
                }
            }
            EngineFrameKind::Call { .. } | EngineFrameKind::Microtask { .. } => {}
        }
        if let Some(frame) = self
            .execution
            .as_mut()
            .and_then(|execution| execution.frames.last_mut())
        {
            frame.env_id = Some(env_id);
        }
        let _ = environment;
        Ok(())
    }

    fn complete_frame(
        &mut self,
        value: EngineValue,
        handle: &JsRuntimeHandle,
    ) -> Result<(), JsSubstrateError> {
        let frame = self
            .execution
            .as_mut()
            .and_then(|execution| execution.frames.pop())
            .expect("frame exists");
        match frame.kind {
            EngineFrameKind::RootScript => {
                if let Some(execution) = self.execution.as_mut() {
                    execution.result = Some(value);
                }
            }
            EngineFrameKind::Module { module_id } => {
                let env_id = frame.env_id.expect("module env initialized");
                let module = self.modules.get(&module_id).expect("module exists").clone();
                let mut exports = BTreeMap::new();
                for JsCompiledExport {
                    exported_name,
                    local_name,
                } in module.artifact.exports
                {
                    exports.insert(
                        exported_name,
                        self.lookup_name(env_id, &local_name)
                            .unwrap_or(EngineValue::Undefined),
                    );
                }
                if let Some(module) = self.modules.get_mut(&module_id) {
                    module.exports = exports;
                    module.evaluated = true;
                }
                let module_result = self
                    .modules
                    .get(&module_id)
                    .and_then(|module| module.exports.get("default"))
                    .cloned()
                    .unwrap_or(value);
                if let Some(execution) = self.execution.as_mut() {
                    execution.result = Some(module_result);
                }
            }
            EngineFrameKind::Call {
                return_target: EngineReturnTarget::ParentFrame,
            } => {
                let parent = self
                    .execution
                    .as_mut()
                    .and_then(|execution| execution.frames.last_mut())
                    .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                        entrypoint: handle.runtime_id.clone(),
                        message: "call returned without a parent frame".to_string(),
                    })?;
                parent.stack.push(value);
            }
            EngineFrameKind::Microtask { promise_id } => {
                self.resolve_promise(promise_id, value)?;
            }
        }
        Ok(())
    }

    fn current_frame(&self) -> Result<&EngineFrame, JsSubstrateError> {
        self.execution
            .as_ref()
            .and_then(|execution| execution.frames.last())
            .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                entrypoint: "runtime".to_string(),
                message: "expected an active execution frame".to_string(),
            })
    }

    fn current_frame_mut(&mut self) -> Result<&mut EngineFrame, JsSubstrateError> {
        self.execution
            .as_mut()
            .and_then(|execution| execution.frames.last_mut())
            .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                entrypoint: "runtime".to_string(),
                message: "expected an active execution frame".to_string(),
            })
    }

    fn current_frame_env_id(&self) -> Result<JsEnvironmentId, JsSubstrateError> {
        self.current_frame()?
            .env_id
            .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                entrypoint: "runtime".to_string(),
                message: "frame environment has not been initialized".to_string(),
            })
    }

    fn advance_pc(&mut self) -> Result<(), JsSubstrateError> {
        let frame = self.current_frame_mut()?;
        frame.pc = frame.pc.saturating_add(1);
        Ok(())
    }

    fn rewind_pc(&mut self) -> Result<(), JsSubstrateError> {
        let frame = self.current_frame_mut()?;
        frame.pc = frame.pc.saturating_sub(1);
        Ok(())
    }

    fn push_stack(&mut self, value: EngineValue) -> Result<(), JsSubstrateError> {
        self.current_frame_mut()?.stack.push(value);
        Ok(())
    }

    fn pop_stack(&mut self) -> EngineValue {
        self.execution
            .as_mut()
            .and_then(|execution| execution.frames.last_mut())
            .and_then(|frame| frame.stack.pop())
            .unwrap_or(EngineValue::Undefined)
    }

    fn peek_stack(&self) -> Option<EngineValue> {
        self.execution
            .as_ref()
            .and_then(|execution| execution.frames.last())
            .and_then(|frame| frame.stack.last())
            .cloned()
    }

    fn pop_arguments(&mut self, argc: usize) -> Vec<EngineValue> {
        let mut args = Vec::with_capacity(argc);
        for _ in 0..argc {
            args.push(self.pop_stack());
        }
        args
    }

    fn begin_microtask_turn(&mut self) -> Result<(), JsSubstrateError> {
        if self
            .execution
            .as_ref()
            .is_some_and(|execution| !execution.frames.is_empty())
        {
            return Ok(());
        }
        let Some(microtask) = self.microtasks.pop_front() else {
            return Ok(());
        };
        match microtask.callback.clone() {
            EngineValue::Function(function_id) => {
                let (code_block_id, captured_env, first_parameter) = {
                    let closure = self.functions.get(&function_id).ok_or_else(|| {
                        JsSubstrateError::EvaluationFailed {
                            entrypoint: "microtask".to_string(),
                            message: "microtask callback function not found".to_string(),
                        }
                    })?;
                    let first_parameter = self
                        .code_block(closure.code_block)?
                        .parameters
                        .first()
                        .cloned();
                    (closure.code_block, closure.captured_env, first_parameter)
                };
                let env_id = self.allocate_env(Some(captured_env));
                if let Some(parameter) = first_parameter {
                    self.define_binding(env_id, parameter, microtask.argument);
                }
                self.execution
                    .as_mut()
                    .expect("execution session exists")
                    .frames
                    .push(EngineFrame {
                        code_block: code_block_id,
                        env_id: Some(env_id),
                        pc: 0,
                        stack: Vec::new(),
                        kind: EngineFrameKind::Microtask {
                            promise_id: microtask.chained_promise,
                        },
                    });
            }
            EngineValue::HostFunction(host) => {
                let value = self.invoke_host_function(host, None, vec![microtask.argument])?;
                match value {
                    HostInvocation::Immediate(result) => {
                        self.resolve_promise(microtask.chained_promise, result)?;
                    }
                    HostInvocation::Pending {
                        promise_id,
                        op_id: _,
                    } => {
                        self.resolve_promise(
                            microtask.chained_promise,
                            EngineValue::Promise(promise_id),
                        )?;
                        return Ok(());
                    }
                }
            }
            _ => {
                self.resolve_promise(microtask.chained_promise, EngineValue::Undefined)?;
            }
        }
        Ok(())
    }

    fn has_pending_microtasks(&self) -> bool {
        !self.microtasks.is_empty()
    }

    fn try_finalize_report(
        &mut self,
        handle: &JsRuntimeHandle,
        clock_now_millis: u64,
        entropy_sample: Vec<u8>,
    ) -> Result<Option<JsExecutionReport>, JsSubstrateError> {
        if self
            .execution
            .as_ref()
            .is_some_and(|execution| !execution.frames.is_empty())
        {
            return Ok(None);
        }
        let Some(execution) = self.execution.take() else {
            return Ok(None);
        };
        let result = execution.result.and_then(|value| value.to_json(self));
        Ok(Some(JsExecutionReport {
            runtime_id: handle.runtime_id.clone(),
            backend: handle.backend.clone(),
            entrypoint: execution.entrypoint,
            result,
            module_graph: execution.module_graph,
            host_calls: execution.host_calls,
            trace: execution.trace,
            clock_now_millis,
            entropy_sample,
            scheduled_tasks: Vec::new(),
            metadata: handle.metadata.clone(),
        }))
    }

    fn pending_operation(
        &self,
        op_id: crate::JsHostOpId,
    ) -> Result<EnginePendingOperation, JsSubstrateError> {
        self.pending_host_ops.get(&op_id).cloned().ok_or_else(|| {
            JsSubstrateError::EvaluationFailed {
                entrypoint: format!("host-op-{}", op_id.get()),
                message: "missing pending host operation".to_string(),
            }
        })
    }

    fn accept_host_completion(
        &mut self,
        completion: crate::JsCompletedHostOperation,
    ) -> Result<(), JsSubstrateError> {
        if !self.pending_host_ops.contains_key(&completion.op_id) {
            return Err(JsSubstrateError::EvaluationFailed {
                entrypoint: format!("host-op-{}", completion.op_id.get()),
                message: "missing pending host operation".to_string(),
            });
        }
        self.completed_host_ops.insert(completion.op_id, completion);
        Ok(())
    }

    fn deliver_host_completion(
        &mut self,
        op_id: crate::JsHostOpId,
    ) -> Result<(), JsSubstrateError> {
        let completion = self.completed_host_ops.remove(&op_id).ok_or_else(|| {
            JsSubstrateError::EvaluationFailed {
                entrypoint: format!("host-op-{}", op_id.get()),
                message: "missing completed host operation".to_string(),
            }
        })?;
        let pending = self.pending_host_ops.remove(&op_id).ok_or_else(|| {
            JsSubstrateError::EvaluationFailed {
                entrypoint: format!("host-op-{}", op_id.get()),
                message: "missing pending host operation".to_string(),
            }
        })?;
        match pending {
            EnginePendingOperation::HostService(call) => {
                let record = JsHostServiceCallRecord {
                    service: call.request.service.clone(),
                    operation: call.request.operation.clone(),
                    arguments: call.request.arguments.clone(),
                    result: completion.result.clone(),
                    metadata: completion.metadata.clone(),
                };
                if let Some(execution) = self.execution.as_mut() {
                    execution.host_calls.push(record);
                }
                if let Some(error) = completion.error {
                    self.reject_promise(call.promise_id, error)?;
                } else {
                    let value = completion
                        .result
                        .map(EngineValue::from_json)
                        .unwrap_or(EngineValue::Undefined);
                    self.resolve_promise(call.promise_id, value)?;
                }
            }
            EnginePendingOperation::ModuleLoad(load) => {
                let loaded = decode_loaded_module_completion(&completion)?;
                self.register_loaded_module(load, loaded)?;
            }
        }
        Ok(())
    }

    fn register_loaded_module(
        &mut self,
        load: EnginePendingModuleLoad,
        loaded: JsLoadedModule,
    ) -> Result<(), JsSubstrateError> {
        if let Some(execution) = self.execution.as_mut() {
            execution.trace.push(JsTraceEvent {
                phase: JsTracePhase::ModuleResolve,
                label: loaded.resolved.canonical_specifier.clone(),
                metadata: BTreeMap::from([
                    (
                        "requested".to_string(),
                        JsonValue::String(load.requested.clone()),
                    ),
                    (
                        "kind".to_string(),
                        JsonValue::String(format!("{:?}", loaded.resolved.kind).to_lowercase()),
                    ),
                    (
                        "referrer".to_string(),
                        load.referrer
                            .clone()
                            .map(JsonValue::String)
                            .unwrap_or(JsonValue::Null),
                    ),
                ]),
            });
            execution.trace.extend(loaded.trace.clone());
        }

        if let Some(existing) = self
            .module_ids_by_specifier
            .get(&loaded.resolved.canonical_specifier)
            .copied()
        {
            if let Some(parent) = load.parent {
                self.record_module_dependency(parent, existing);
            } else if let Some(execution) = self.execution.as_mut() {
                execution.root_module_id = Some(existing);
            }
            return Ok(());
        }

        let artifact = self.compile_loaded_module(&loaded)?;
        let dependency_requests = artifact.requests.clone();
        let module_id = self.next_module_id();
        self.module_ids_by_specifier
            .insert(loaded.resolved.canonical_specifier.clone(), module_id);
        self.modules.insert(
            module_id,
            EngineModuleRecord {
                module_id,
                resolved: loaded.resolved.clone(),
                artifact,
                dependencies: Vec::new(),
                exports: BTreeMap::new(),
                evaluated: false,
            },
        );

        if let Some(parent) = load.parent {
            self.record_module_dependency(parent, module_id);
        } else if let Some(execution) = self.execution.as_mut() {
            execution.root_module_id = Some(module_id);
        }

        if let Some(execution) = self.execution.as_mut() {
            for request in dependency_requests {
                execution
                    .pending_module_requests
                    .push_back(EnginePendingModuleLoad {
                        requested: request,
                        referrer: Some(loaded.resolved.canonical_specifier.clone()),
                        parent: Some(module_id),
                    });
            }
        }
        Ok(())
    }

    fn record_module_dependency(&mut self, parent: JsModuleId, dependency: JsModuleId) {
        if let Some(module) = self.modules.get_mut(&parent) {
            if !module.dependencies.contains(&dependency) {
                module.dependencies.push(dependency);
            }
        }
    }

    fn resolve_promise(
        &mut self,
        promise_id: crate::JsPromiseId,
        value: EngineValue,
    ) -> Result<(), JsSubstrateError> {
        let reactions = match self.promises.get_mut(&promise_id) {
            Some(EnginePromise {
                state: EnginePromiseState::Pending { reactions, .. },
            }) => std::mem::take(reactions),
            Some(EnginePromise {
                state: EnginePromiseState::Fulfilled(_),
            }) => return Ok(()),
            Some(EnginePromise {
                state: EnginePromiseState::Rejected(_),
            }) => return Ok(()),
            None => {
                return Err(JsSubstrateError::EvaluationFailed {
                    entrypoint: format!("promise-{}", promise_id.get()),
                    message: "promise not found".to_string(),
                });
            }
        };
        if let Some(promise) = self.promises.get_mut(&promise_id) {
            promise.state = EnginePromiseState::Fulfilled(value.clone());
        }
        for reaction in reactions {
            self.microtasks.push_back(EngineMicrotask {
                callback: reaction.callback,
                argument: value.clone(),
                chained_promise: reaction.chained_promise,
            });
        }
        Ok(())
    }

    fn reject_promise(
        &mut self,
        promise_id: crate::JsPromiseId,
        error: JsRuntimeErrorReport,
    ) -> Result<(), JsSubstrateError> {
        let reactions = match self.promises.get_mut(&promise_id) {
            Some(EnginePromise {
                state: EnginePromiseState::Pending { reactions, .. },
            }) => std::mem::take(reactions),
            Some(EnginePromise {
                state: EnginePromiseState::Fulfilled(_),
            })
            | Some(EnginePromise {
                state: EnginePromiseState::Rejected(_),
            }) => return Ok(()),
            None => {
                return Err(JsSubstrateError::EvaluationFailed {
                    entrypoint: format!("promise-{}", promise_id.get()),
                    message: "promise not found".to_string(),
                });
            }
        };
        if let Some(promise) = self.promises.get_mut(&promise_id) {
            promise.state = EnginePromiseState::Rejected(error.clone());
        }
        for reaction in reactions {
            if let Some(promise) = self.promises.get_mut(&reaction.chained_promise) {
                promise.state = EnginePromiseState::Rejected(error.clone());
            }
        }
        Ok(())
    }

    fn promise_state(
        &self,
        promise_id: crate::JsPromiseId,
    ) -> Result<EnginePromiseState, JsSubstrateError> {
        self.promises
            .get(&promise_id)
            .map(|promise| promise.state.clone())
            .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                entrypoint: format!("promise-{}", promise_id.get()),
                message: "promise not found".to_string(),
            })
    }

    fn invoke_callable(
        &mut self,
        callee: EngineValue,
        receiver: Option<EngineValue>,
        args: &mut Vec<EngineValue>,
        handle: &JsRuntimeHandle,
        _attached: &mut JsAttachedRuntimeContext<'_>,
    ) -> Result<Option<JsRuntimeTurnOutcome>, JsSubstrateError> {
        match callee {
            EngineValue::Function(function_id) => {
                let closure = self.functions.get(&function_id).cloned().ok_or_else(|| {
                    JsSubstrateError::EvaluationFailed {
                        entrypoint: handle.runtime_id.clone(),
                        message: "function not found".to_string(),
                    }
                })?;
                if closure.async_function {
                    return Err(JsSubstrateError::EvaluationFailed {
                        entrypoint: handle.runtime_id.clone(),
                        message: "async function calls are not yet implemented".to_string(),
                    });
                }
                let env_id = self.allocate_env(Some(closure.captured_env));
                let code_block = self.code_block(closure.code_block)?.clone();
                for (index, parameter) in code_block.parameters.iter().enumerate() {
                    self.define_binding(
                        env_id,
                        parameter,
                        args.get(index).cloned().unwrap_or(EngineValue::Undefined),
                    );
                }
                self.execution
                    .as_mut()
                    .expect("execution exists")
                    .frames
                    .push(EngineFrame {
                        code_block: closure.code_block,
                        env_id: Some(env_id),
                        pc: 0,
                        stack: Vec::new(),
                        kind: EngineFrameKind::Call {
                            return_target: EngineReturnTarget::ParentFrame,
                        },
                    });
                Ok(None)
            }
            EngineValue::HostFunction(host) => {
                match self.invoke_host_function(host, receiver, args.clone())? {
                    HostInvocation::Immediate(value) => {
                        self.execution
                            .as_mut()
                            .expect("execution exists")
                            .frames
                            .last_mut()
                            .expect("frame exists")
                            .stack
                            .push(value);
                        Ok(None)
                    }
                    HostInvocation::Pending { promise_id, op_id } => {
                        self.execution
                            .as_mut()
                            .expect("execution exists")
                            .frames
                            .last_mut()
                            .expect("frame exists")
                            .stack
                            .push(EngineValue::Promise(promise_id));
                        Ok(Some(JsRuntimeTurnOutcome::PendingHostOp { op_id }))
                    }
                }
            }
            unsupported => Err(JsSubstrateError::EvaluationFailed {
                entrypoint: handle.runtime_id.clone(),
                message: format!("value is not callable: {unsupported:?}"),
            }),
        }
    }

    fn invoke_host_function(
        &mut self,
        host: EngineHostFunction,
        receiver: Option<EngineValue>,
        args: Vec<EngineValue>,
    ) -> Result<HostInvocation, JsSubstrateError> {
        match host {
            EngineHostFunction::DateNow => Ok(HostInvocation::Immediate(EngineValue::Number(
                self.current_clock_now_millis as f64,
            ))),
            EngineHostFunction::MathRandom => Ok(HostInvocation::Immediate(EngineValue::Number(
                self.next_random_f64(),
            ))),
            EngineHostFunction::PromiseResolve => {
                let promise_id = self.next_promise_id();
                self.promises.insert(
                    promise_id,
                    EnginePromise {
                        state: EnginePromiseState::Fulfilled(
                            args.first().cloned().unwrap_or(EngineValue::Undefined),
                        ),
                    },
                );
                Ok(HostInvocation::Immediate(EngineValue::Promise(promise_id)))
            }
            EngineHostFunction::Capability { service, operation } => {
                let promise_id = self.next_promise_id();
                let op_id = self.next_host_op_id();
                let arguments = args
                    .into_iter()
                    .next()
                    .unwrap_or(EngineValue::Undefined)
                    .to_json(self)
                    .unwrap_or(JsonValue::Null);
                self.pending_host_ops.insert(
                    op_id,
                    EnginePendingOperation::HostService(EnginePendingHostCall {
                        request: JsHostServiceRequest {
                            service,
                            operation,
                            arguments,
                            metadata: BTreeMap::new(),
                        },
                        promise_id,
                    }),
                );
                self.promises.insert(
                    promise_id,
                    EnginePromise {
                        state: EnginePromiseState::Pending {
                            op_id: Some(op_id),
                            reactions: Vec::new(),
                        },
                    },
                );
                Ok(HostInvocation::Pending { promise_id, op_id })
            }
            EngineHostFunction::PromiseThen { promise_id } => {
                let callback = args.first().cloned().unwrap_or(EngineValue::Undefined);
                let chained_promise = self.next_promise_id();
                self.promises.insert(
                    chained_promise,
                    EnginePromise {
                        state: EnginePromiseState::Pending {
                            op_id: None,
                            reactions: Vec::new(),
                        },
                    },
                );
                match self.promise_state(promise_id)? {
                    EnginePromiseState::Fulfilled(value) => {
                        self.microtasks.push_back(EngineMicrotask {
                            callback,
                            argument: value,
                            chained_promise,
                        });
                    }
                    EnginePromiseState::Pending { reactions, op_id } => {
                        if let Some(promise) = self.promises.get_mut(&promise_id) {
                            promise.state = EnginePromiseState::Pending {
                                op_id,
                                reactions: {
                                    let mut reactions = reactions;
                                    reactions.push(EnginePromiseReaction {
                                        callback,
                                        chained_promise,
                                    });
                                    reactions
                                },
                            };
                        }
                    }
                    EnginePromiseState::Rejected(error) => {
                        if let Some(promise) = self.promises.get_mut(&chained_promise) {
                            promise.state = EnginePromiseState::Rejected(error);
                        }
                    }
                }
                let _ = receiver;
                Ok(HostInvocation::Immediate(EngineValue::Promise(
                    chained_promise,
                )))
            }
        }
    }

    fn code_block(
        &self,
        code_block: JsCodeBlockId,
    ) -> Result<&JsCompiledCodeBlock, JsSubstrateError> {
        self.modules
            .values()
            .find_map(|module| module.artifact.code_blocks.get(&code_block))
            .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                entrypoint: format!("code-block-{}", code_block.0),
                message: "compiled code block not found".to_string(),
            })
    }

    fn resolve_module_dependency(
        &self,
        module_id: JsModuleId,
        request: &str,
    ) -> Result<JsModuleId, JsSubstrateError> {
        let module =
            self.modules
                .get(&module_id)
                .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                    entrypoint: format!("module-{}", module_id.get()),
                    message: "module record missing".to_string(),
                })?;
        let dependency = module
            .dependencies
            .iter()
            .copied()
            .find(|dependency| {
                self.modules.get(dependency).is_some_and(|module| {
                    module.resolved.requested_specifier == request
                        || module.resolved.canonical_specifier == request
                })
            })
            .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                entrypoint: module.resolved.canonical_specifier.clone(),
                message: format!("missing dependency for request {request}"),
            })?;
        Ok(dependency)
    }

    fn create_plain_object_with_heap(
        &mut self,
        attached: &mut JsAttachedRuntimeContext<'_>,
    ) -> Result<JsHeapObjectId, JsSubstrateError> {
        let realm = attached
            .current_realm()
            .map_err(context_error_to_substrate)?
            .label
            .clone();
        let object_id = attached
            .allocate_heap_object("object", format!("{realm}::object"), 64)
            .map_err(context_error_to_substrate)?;
        self.objects.insert(
            object_id,
            EngineObject {
                properties: BTreeMap::new(),
            },
        );
        Ok(object_id)
    }

    fn set_object_property(
        &mut self,
        object_id: JsHeapObjectId,
        name: impl Into<String>,
        value: EngineValue,
    ) -> Result<(), JsSubstrateError> {
        let object =
            self.objects
                .get_mut(&object_id)
                .ok_or_else(|| JsSubstrateError::EvaluationFailed {
                    entrypoint: format!("object-{}", object_id.get()),
                    message: "object not found".to_string(),
                })?;
        object.properties.insert(name.into(), value);
        Ok(())
    }

    fn get_named_property(
        &self,
        object: EngineValue,
        name: &str,
        handle: &JsRuntimeHandle,
    ) -> Result<EngineValue, JsSubstrateError> {
        match object {
            EngineValue::Object(object_id) => Ok(self
                .objects
                .get(&object_id)
                .and_then(|object| object.properties.get(name))
                .cloned()
                .unwrap_or(EngineValue::Undefined)),
            EngineValue::Promise(promise_id) if name == "then" => {
                Ok(EngineValue::HostFunction(EngineHostFunction::PromiseThen {
                    promise_id,
                }))
            }
            EngineValue::String(value) if name == "length" => {
                Ok(EngineValue::Number(value.chars().count() as f64))
            }
            unsupported => Err(JsSubstrateError::EvaluationFailed {
                entrypoint: handle.runtime_id.clone(),
                message: format!("cannot read property {name} from {unsupported:?}"),
            }),
        }
    }

    fn expect_object(
        &self,
        value: EngineValue,
        handle: &JsRuntimeHandle,
    ) -> Result<JsHeapObjectId, JsSubstrateError> {
        match value {
            EngineValue::Object(object_id) => Ok(object_id),
            other => Err(JsSubstrateError::EvaluationFailed {
                entrypoint: handle.runtime_id.clone(),
                message: format!("expected object, found {other:?}"),
            }),
        }
    }

    fn expect_string(
        &self,
        value: EngineValue,
        handle: &JsRuntimeHandle,
    ) -> Result<String, JsSubstrateError> {
        match value {
            EngineValue::String(value) => Ok(value),
            EngineValue::Number(number) => Ok(number.to_string()),
            other => Err(JsSubstrateError::EvaluationFailed {
                entrypoint: handle.runtime_id.clone(),
                message: format!("expected string-compatible value, found {other:?}"),
            }),
        }
    }

    fn define_binding(
        &mut self,
        env_id: JsEnvironmentId,
        name: impl Into<String>,
        value: EngineValue,
    ) {
        if let Some(environment) = self.environments.get_mut(&env_id) {
            environment.bindings.insert(name.into(), value);
        }
    }

    fn lookup_name(&self, env_id: JsEnvironmentId, name: &str) -> Option<EngineValue> {
        let mut current = Some(env_id);
        while let Some(id) = current {
            let environment = self.environments.get(&id)?;
            if let Some(value) = environment.bindings.get(name) {
                return Some(value.clone());
            }
            current = environment.parent;
        }
        None
    }

    fn store_name(&mut self, env_id: JsEnvironmentId, name: &str, value: EngineValue) {
        let mut current = Some(env_id);
        while let Some(id) = current {
            let environment = self.environments.get_mut(&id).expect("environment exists");
            if environment.bindings.contains_key(name) {
                environment.bindings.insert(name.to_string(), value);
                return;
            }
            current = environment.parent;
        }
        if let Some(environment) = self.environments.get_mut(&env_id) {
            environment.bindings.insert(name.to_string(), value);
        }
    }

    fn allocate_env(&mut self, parent: Option<JsEnvironmentId>) -> JsEnvironmentId {
        self.next_env_id = self.next_env_id.saturating_add(1);
        let env_id = JsEnvironmentId::new(self.next_env_id);
        self.environments.insert(
            env_id,
            EngineEnvironment {
                parent,
                bindings: BTreeMap::new(),
            },
        );
        env_id
    }

    fn allocate_function(
        &mut self,
        code_block: JsCodeBlockId,
        captured_env: JsEnvironmentId,
        async_function: bool,
    ) -> JsFunctionId {
        self.next_function_id = self.next_function_id.saturating_add(1);
        let function_id = JsFunctionId::new(self.next_function_id);
        self.functions.insert(
            function_id,
            EngineFunction {
                code_block,
                captured_env,
                async_function,
            },
        );
        function_id
    }

    fn next_promise_id(&mut self) -> crate::JsPromiseId {
        self.next_promise_id = self.next_promise_id.saturating_add(1);
        crate::JsPromiseId::new(self.next_promise_id)
    }

    fn next_host_op_id(&mut self) -> crate::JsHostOpId {
        self.next_host_op_id = self.next_host_op_id.saturating_add(1);
        crate::JsHostOpId::new(self.next_host_op_id)
    }

    fn next_module_id(&mut self) -> JsModuleId {
        self.next_module_id = self.next_module_id.saturating_add(1);
        JsModuleId::new(self.next_module_id)
    }

    fn next_random_f64(&mut self) -> f64 {
        self.current_entropy_counter = self.current_entropy_counter.saturating_add(1);
        let salt = self
            .current_entropy_counter
            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let value = u64::from_le_bytes(self.current_entropy_seed) ^ salt;
        (value as f64) / (u64::MAX as f64)
    }
}

enum HostInvocation {
    Immediate(EngineValue),
    Pending {
        promise_id: crate::JsPromiseId,
        op_id: crate::JsHostOpId,
    },
}

impl EngineValue {
    fn to_json(&self, state: &EngineRuntimeState) -> Option<JsonValue> {
        match self {
            EngineValue::Undefined => None,
            EngineValue::Null => Some(JsonValue::Null),
            EngineValue::Bool(value) => Some(JsonValue::Bool(*value)),
            EngineValue::Number(value) => Some(json!(value)),
            EngineValue::String(value) => Some(JsonValue::String(value.clone())),
            EngineValue::Object(object_id) => state.objects.get(object_id).map(|object| {
                JsonValue::Object(
                    object
                        .properties
                        .iter()
                        .filter_map(|(key, value)| {
                            value.to_json(state).map(|value| (key.clone(), value))
                        })
                        .collect(),
                )
            }),
            EngineValue::Function(_) | EngineValue::HostFunction(_) => {
                Some(JsonValue::String("[function]".to_string()))
            }
            EngineValue::Promise(promise_id) => match state.promises.get(promise_id) {
                Some(EnginePromise {
                    state: EnginePromiseState::Fulfilled(value),
                }) => value.to_json(state),
                Some(EnginePromise {
                    state: EnginePromiseState::Rejected(error),
                }) => Some(JsonValue::String(error.message.clone())),
                _ => Some(JsonValue::String("[promise]".to_string())),
            },
        }
    }

    fn from_json(value: JsonValue) -> Self {
        match value {
            JsonValue::Null => EngineValue::Null,
            JsonValue::Bool(value) => EngineValue::Bool(value),
            JsonValue::Number(value) => EngineValue::Number(value.as_f64().unwrap_or_default()),
            JsonValue::String(value) => EngineValue::String(value),
            other => EngineValue::String(other.to_string()),
        }
    }
}

fn apply_unary(
    op: JsUnaryOperator,
    value: EngineValue,
    _handle: &JsRuntimeHandle,
) -> Result<EngineValue, JsSubstrateError> {
    match op {
        JsUnaryOperator::Plus => Ok(EngineValue::Number(to_number(&value))),
        JsUnaryOperator::Minus => Ok(EngineValue::Number(-to_number(&value))),
        JsUnaryOperator::Not => Ok(EngineValue::Bool(!is_truthy(&value))),
        JsUnaryOperator::TypeOf => Ok(EngineValue::String(type_of_value(&value).to_string())),
    }
}

fn apply_binary(
    op: JsBinaryOperator,
    lhs: EngineValue,
    rhs: EngineValue,
    _handle: &JsRuntimeHandle,
) -> Result<EngineValue, JsSubstrateError> {
    let value = match op {
        JsBinaryOperator::Add => match (lhs, rhs) {
            (EngineValue::String(lhs), rhs) => {
                EngineValue::String(format!("{lhs}{}", to_string(&rhs)))
            }
            (lhs, EngineValue::String(rhs)) => {
                EngineValue::String(format!("{}{rhs}", to_string(&lhs)))
            }
            (lhs, rhs) => EngineValue::Number(to_number(&lhs) + to_number(&rhs)),
        },
        JsBinaryOperator::StrictEq => EngineValue::Bool(lhs == rhs),
        JsBinaryOperator::StrictNe => EngineValue::Bool(lhs != rhs),
        JsBinaryOperator::Eq => EngineValue::Bool(loose_equals(&lhs, &rhs)),
        JsBinaryOperator::Ne => EngineValue::Bool(!loose_equals(&lhs, &rhs)),
        JsBinaryOperator::LogicalAnd => {
            if is_truthy(&lhs) {
                rhs
            } else {
                lhs
            }
        }
        JsBinaryOperator::LogicalOr => {
            if is_truthy(&lhs) {
                lhs
            } else {
                rhs
            }
        }
        JsBinaryOperator::NullishCoalesce => {
            if matches!(lhs, EngineValue::Undefined | EngineValue::Null) {
                rhs
            } else {
                lhs
            }
        }
    };
    Ok(value)
}

fn to_number(value: &EngineValue) -> f64 {
    match value {
        EngineValue::Undefined => f64::NAN,
        EngineValue::Null => 0.0,
        EngineValue::Bool(value) => {
            if *value {
                1.0
            } else {
                0.0
            }
        }
        EngineValue::Number(value) => *value,
        EngineValue::String(value) => value.parse::<f64>().unwrap_or(f64::NAN),
        EngineValue::Object(_) | EngineValue::Function(_) | EngineValue::HostFunction(_) => {
            f64::NAN
        }
        EngineValue::Promise(_) => f64::NAN,
    }
}

fn to_string(value: &EngineValue) -> String {
    match value {
        EngineValue::Undefined => "undefined".to_string(),
        EngineValue::Null => "null".to_string(),
        EngineValue::Bool(value) => value.to_string(),
        EngineValue::Number(value) => value.to_string(),
        EngineValue::String(value) => value.clone(),
        EngineValue::Object(_) => "[object Object]".to_string(),
        EngineValue::Function(_) | EngineValue::HostFunction(_) => "[function]".to_string(),
        EngineValue::Promise(_) => "[promise]".to_string(),
    }
}

fn type_of_value(value: &EngineValue) -> &'static str {
    match value {
        EngineValue::Undefined => "undefined",
        EngineValue::Null => "object",
        EngineValue::Bool(_) => "boolean",
        EngineValue::Number(_) => "number",
        EngineValue::String(_) => "string",
        EngineValue::Object(_) | EngineValue::Promise(_) => "object",
        EngineValue::Function(_) | EngineValue::HostFunction(_) => "function",
    }
}

fn is_truthy(value: &EngineValue) -> bool {
    match value {
        EngineValue::Undefined | EngineValue::Null => false,
        EngineValue::Bool(value) => *value,
        EngineValue::Number(value) => *value != 0.0 && !value.is_nan(),
        EngineValue::String(value) => !value.is_empty(),
        EngineValue::Object(_) | EngineValue::Function(_) | EngineValue::HostFunction(_) => true,
        EngineValue::Promise(_) => true,
    }
}

fn loose_equals(lhs: &EngineValue, rhs: &EngineValue) -> bool {
    match (lhs, rhs) {
        (EngineValue::Null, EngineValue::Undefined)
        | (EngineValue::Undefined, EngineValue::Null) => true,
        _ => lhs == rhs,
    }
}

fn js_turn_kind_label(kind: &JsRuntimeTurnKind) -> &'static str {
    match kind {
        JsRuntimeTurnKind::Bootstrap => "bootstrap",
        JsRuntimeTurnKind::EvaluateEntrypoint { .. } => "evaluate_entrypoint",
        JsRuntimeTurnKind::EvaluateModule { .. } => "evaluate_module",
        JsRuntimeTurnKind::DeliverTimer { .. } => "deliver_timer",
        JsRuntimeTurnKind::DeliverTask { .. } => "deliver_task",
        JsRuntimeTurnKind::DeliverHostCompletion { .. } => "deliver_host_completion",
        JsRuntimeTurnKind::DrainMicrotasks => "drain_microtasks",
    }
}

fn context_error_to_substrate(error: JsContextError) -> JsSubstrateError {
    JsSubstrateError::EvaluationFailed {
        entrypoint: "terrace-js-context".to_string(),
        message: error.to_string(),
    }
}

fn compile_error_to_substrate(error: JsCompileError) -> JsSubstrateError {
    JsSubstrateError::EvaluationFailed {
        entrypoint: "terrace-js-compiler".to_string(),
        message: error.to_string(),
    }
}

fn encode_loaded_module_completion(loaded: &JsLoadedModule) -> Result<JsonValue, JsSubstrateError> {
    serde_json::to_value(loaded).map_err(JsSubstrateError::from)
}

fn decode_loaded_module_completion(
    completion: &crate::JsCompletedHostOperation,
) -> Result<JsLoadedModule, JsSubstrateError> {
    serde_json::from_value(completion.result.clone().ok_or_else(|| {
        JsSubstrateError::EvaluationFailed {
            entrypoint: format!("host-op-{}", completion.op_id.get()),
            message: "module load completion was missing a payload".to_string(),
        }
    })?)
    .map_err(JsSubstrateError::from)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use async_trait::async_trait;
    use serde_json::json;

    use crate::{
        DeterministicJsEntropySource, DeterministicJsHostServices, DeterministicJsRuntimeHost,
        DeterministicJsServiceOutcome, FixedJsClock, JsCompletedHostOperation, JsExecutionRequest,
        JsForkPolicy, JsLoadedModule, JsModuleKind, JsModuleLoader, JsResolvedModule, JsRuntime,
        JsRuntimeEnvironment, JsRuntimeHost, JsRuntimeOpenRequest, JsRuntimePolicy,
        JsRuntimeProvenance, JsRuntimeTurn, JsRuntimeTurnKind, JsRuntimeTurnOutcome,
        JsSubstrateError, NeverCancel,
    };

    use super::EngineJsRuntimeHost;

    #[derive(Clone, Default)]
    struct StaticModuleLoader {
        modules: BTreeMap<String, String>,
    }

    impl StaticModuleLoader {
        fn with_module(mut self, specifier: impl Into<String>, source: impl Into<String>) -> Self {
            self.modules.insert(specifier.into(), source.into());
            self
        }
    }

    #[async_trait(?Send)]
    impl JsModuleLoader for StaticModuleLoader {
        async fn resolve(
            &self,
            specifier: &str,
            referrer: Option<&str>,
        ) -> Result<JsResolvedModule, JsSubstrateError> {
            if specifier.starts_with("terrace:host/") {
                return Ok(JsResolvedModule {
                    requested_specifier: specifier.to_string(),
                    canonical_specifier: specifier.to_string(),
                    kind: JsModuleKind::HostCapability,
                });
            }
            let canonical_specifier = if specifier.starts_with("terrace:/") {
                specifier.to_string()
            } else if specifier.starts_with("./") || specifier.starts_with("../") {
                let referrer = referrer.ok_or_else(|| JsSubstrateError::UnsupportedSpecifier {
                    specifier: specifier.to_string(),
                })?;
                resolve_relative_specifier(referrer, specifier)
            } else {
                return Err(JsSubstrateError::UnsupportedSpecifier {
                    specifier: specifier.to_string(),
                });
            };
            Ok(JsResolvedModule {
                requested_specifier: specifier.to_string(),
                canonical_specifier,
                kind: JsModuleKind::Workspace,
            })
        }

        async fn load(
            &self,
            resolved: &JsResolvedModule,
        ) -> Result<JsLoadedModule, JsSubstrateError> {
            match resolved.kind {
                JsModuleKind::HostCapability => Ok(JsLoadedModule {
                    resolved: resolved.clone(),
                    source: String::new(),
                    trace: Vec::new(),
                    metadata: BTreeMap::from([
                        ("host_service".to_string(), json!("capability")),
                        (
                            "host_exports".to_string(),
                            json!([resolved
                                .canonical_specifier
                                .rsplit('/')
                                .next()
                                .unwrap_or("invoke")]),
                        ),
                    ]),
                }),
                JsModuleKind::Workspace => Ok(JsLoadedModule {
                    resolved: resolved.clone(),
                    source: self
                        .modules
                        .get(&resolved.canonical_specifier)
                        .cloned()
                        .ok_or_else(|| JsSubstrateError::ModuleNotFound {
                            specifier: resolved.canonical_specifier.clone(),
                        })?,
                    trace: Vec::new(),
                    metadata: BTreeMap::new(),
                }),
                JsModuleKind::Package => Err(JsSubstrateError::UnsupportedSpecifier {
                    specifier: resolved.canonical_specifier.clone(),
                }),
            }
        }
    }

    fn resolve_relative_specifier(referrer: &str, specifier: &str) -> String {
        let referrer_path = referrer.trim_start_matches("terrace:");
        let mut segments = referrer_path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let _ = segments.pop();
        for segment in specifier.split('/') {
            match segment {
                "." | "" => {}
                ".." => {
                    let _ = segments.pop();
                }
                other => segments.push(other.to_string()),
            }
        }
        format!("terrace:/{}", segments.join("/"))
    }

    async fn open_runtime(
        host: Arc<dyn JsRuntimeHost>,
        policy: JsRuntimePolicy,
    ) -> Arc<dyn JsRuntime> {
        host.open_runtime(JsRuntimeOpenRequest {
            runtime_id: "engine-runtime".to_string(),
            policy,
            provenance: JsRuntimeProvenance {
                backend: host.name().to_string(),
                host_model: "test".to_string(),
                module_root: "/workspace".to_string(),
                volume_id: None,
                snapshot_sequence: None,
                durable_snapshot: false,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            environment: JsRuntimeEnvironment::default(),
            metadata: BTreeMap::new(),
        })
        .await
        .expect("open runtime")
    }

    #[tokio::test]
    async fn deterministic_runtime_host_executes_module_graph_on_new_engine() {
        let loader: Arc<dyn JsModuleLoader> = Arc::new(
            StaticModuleLoader::default()
                .with_module(
                    "terrace:/workspace/helper.mjs",
                    r#"export default { helper: true, message: "from helper" };"#,
                )
                .with_module(
                    "terrace:/workspace/main.mjs",
                    r#"
import helper from "./helper.mjs";
import { echo } from "terrace:host/echo";

const response = await echo({ message: helper.message });
export default {
  status: "ok",
  helper: helper.helper,
  echoed: response.echoed,
  now: Date.now(),
  console_type: typeof console,
  process_type: typeof process
};
"#,
                ),
        );
        let host_services = DeterministicJsHostServices::new();
        host_services
            .register_outcome(
                "capability",
                "echo",
                DeterministicJsServiceOutcome::Response {
                    result: json!({ "echoed": "from helper" }),
                    metadata: BTreeMap::new(),
                },
            )
            .await;
        let runtime = open_runtime(
            Arc::new(
                DeterministicJsRuntimeHost::new(loader, Arc::new(host_services))
                    .with_clock(Arc::new(FixedJsClock::new(9_001)))
                    .with_entropy(Arc::new(DeterministicJsEntropySource::new(0xfeed))),
            ),
            JsRuntimePolicy {
                visible_host_services: vec!["capability::echo".to_string()],
                ..Default::default()
            },
        )
        .await;

        let report = runtime
            .execute(
                JsExecutionRequest::module("terrace:/workspace/main.mjs"),
                Arc::new(NeverCancel),
            )
            .await
            .expect("execute module");

        assert_eq!(
            report.result,
            Some(json!({
                "status": "ok",
                "helper": true,
                "echoed": "from helper",
                "now": 9001.0,
                "console_type": "undefined",
                "process_type": "undefined"
            }))
        );
        assert_eq!(report.host_calls.len(), 1);
        assert_eq!(
            report.host_calls[0].arguments,
            json!({ "message": "from helper" })
        );
        assert_eq!(
            report.module_graph,
            vec![
                "terrace:/workspace/main.mjs".to_string(),
                "terrace:host/echo".to_string(),
                "terrace:/workspace/helper.mjs".to_string(),
            ]
        );
        assert!(
            report
                .scheduled_tasks
                .iter()
                .any(|task| task.label == "terrace:/workspace/main.mjs")
        );
    }

    #[tokio::test]
    async fn engine_runtime_exposes_pending_host_completion_across_turns() {
        let loader: Arc<dyn JsModuleLoader> = Arc::new(StaticModuleLoader::default().with_module(
            "terrace:/workspace/main.mjs",
            r#"
import { echo } from "terrace:host/echo";
const response = await echo({ message: "manual" });
export default { echoed: response.echoed };
"#,
        ));
        let runtime = open_runtime(
            Arc::new(
                EngineJsRuntimeHost::new(loader, Arc::new(DeterministicJsHostServices::new()))
                    .with_clock(Arc::new(FixedJsClock::new(321))),
            ),
            JsRuntimePolicy {
                visible_host_services: vec!["capability::echo".to_string()],
                ..Default::default()
            },
        )
        .await;

        let first = runtime
            .run_turn(
                JsRuntimeTurn::evaluate_entrypoint(JsExecutionRequest::module(
                    "terrace:/workspace/main.mjs",
                )),
                Arc::new(NeverCancel),
            )
            .await
            .expect("first turn");
        let op_id = match first {
            JsRuntimeTurnOutcome::PendingHostOp { op_id } => op_id,
            other => panic!("expected pending host op, got {other:?}"),
        };

        let suspended = runtime.suspended_state().await.expect("suspended state");
        assert_eq!(
            suspended
                .pending_host_ops
                .get(&op_id)
                .expect("pending op")
                .operation,
            "capability::echo"
        );

        runtime
            .deliver_host_completion(JsCompletedHostOperation {
                op_id,
                result: Some(json!({ "echoed": "manual" })),
                error: None,
                metadata: BTreeMap::new(),
            })
            .await
            .expect("deliver completion");

        let second = runtime
            .run_turn(
                JsRuntimeTurn {
                    kind: JsRuntimeTurnKind::DeliverHostCompletion { op_id },
                    metadata: BTreeMap::new(),
                },
                Arc::new(NeverCancel),
            )
            .await
            .expect("completion turn");

        let report = match second {
            JsRuntimeTurnOutcome::Completed { completion } => {
                completion.execution.expect("execution report")
            }
            other => panic!("expected completed turn, got {other:?}"),
        };
        assert_eq!(report.result, Some(json!({ "echoed": "manual" })));
        assert_eq!(report.host_calls.len(), 1);
        assert_eq!(
            report.host_calls[0].arguments,
            json!({ "message": "manual" })
        );
    }

    #[tokio::test]
    async fn engine_runtime_drains_promise_microtasks_in_follow_up_turns() {
        let loader: Arc<dyn JsModuleLoader> = Arc::new(StaticModuleLoader::default().with_module(
            "terrace:/workspace/main.mjs",
            r#"
const result = await Promise.resolve("ok").then((value) => value + "!");
export default { result };
"#,
        ));
        let runtime = open_runtime(
            Arc::new(
                EngineJsRuntimeHost::new(loader, Arc::new(DeterministicJsHostServices::new()))
                    .with_clock(Arc::new(FixedJsClock::new(111))),
            ),
            JsRuntimePolicy::default(),
        )
        .await;

        let first = runtime
            .run_turn(
                JsRuntimeTurn::evaluate_entrypoint(JsExecutionRequest::module(
                    "terrace:/workspace/main.mjs",
                )),
                Arc::new(NeverCancel),
            )
            .await
            .expect("first turn");
        assert!(matches!(first, JsRuntimeTurnOutcome::PendingMicrotasks));

        let second = runtime
            .run_turn(
                JsRuntimeTurn {
                    kind: JsRuntimeTurnKind::DrainMicrotasks,
                    metadata: BTreeMap::new(),
                },
                Arc::new(NeverCancel),
            )
            .await
            .expect("microtask turn");

        let report = match second {
            JsRuntimeTurnOutcome::Completed { completion } => {
                completion.execution.expect("execution report")
            }
            other => panic!("expected completed turn, got {other:?}"),
        };
        assert_eq!(report.result, Some(json!({ "result": "ok!" })));
    }
}
