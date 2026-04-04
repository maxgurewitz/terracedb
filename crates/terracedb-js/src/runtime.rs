use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    JsForkPolicy, JsSubstrateError,
    entropy::JsEntropySource,
    host::JsHostServices,
    loader::{JsLoadedModule, JsModuleLoader, JsResolvedModule},
    scheduler::{JsScheduledTask, JsScheduler, JsTaskQueue},
    time::JsClock,
    types::{
        JsExecutionKind, JsExecutionReport, JsExecutionRequest, JsHostServiceCallRecord,
        JsHostServiceRequest, JsRuntimeAttachmentState, JsRuntimeConfiguration,
        JsRuntimeErrorReport, JsRuntimeHandle, JsRuntimeOpenRequest, JsRuntimePolicy,
        JsRuntimeSuspendedState, JsRuntimeTurn, JsRuntimeTurnCompletion, JsRuntimeTurnKind,
        JsRuntimeTurnOutcome, JsTraceEvent, JsTracePhase,
    },
};

pub trait JsCancellationToken: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

#[derive(Clone, Debug, Default)]
pub struct NeverCancel;

impl JsCancellationToken for NeverCancel {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[async_trait(?Send)]
pub trait JsExecutionHooks: Send + Sync {
    async fn on_runtime_open(&self, _handle: &JsRuntimeHandle) -> Result<(), JsSubstrateError> {
        Ok(())
    }

    async fn on_module_loaded(&self, _module: &JsLoadedModule) -> Result<(), JsSubstrateError> {
        Ok(())
    }

    async fn on_host_service_call(
        &self,
        _request: &JsHostServiceRequest,
        _record: &JsHostServiceCallRecord,
    ) -> Result<(), JsSubstrateError> {
        Ok(())
    }

    async fn on_runtime_close(&self, _handle: &JsRuntimeHandle) -> Result<(), JsSubstrateError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct NoopJsExecutionHooks;

#[async_trait(?Send)]
impl JsExecutionHooks for NoopJsExecutionHooks {}

#[async_trait(?Send)]
pub trait JsRuntime: Send + Sync {
    fn handle(&self) -> JsRuntimeHandle;
    fn configuration(&self) -> JsRuntimeConfiguration {
        JsRuntimeConfiguration::from_handle(&self.handle())
    }
    async fn attachment_state(&self) -> Result<JsRuntimeAttachmentState, JsSubstrateError> {
        Ok(JsRuntimeAttachmentState::default())
    }
    async fn suspended_state(&self) -> Result<JsRuntimeSuspendedState, JsSubstrateError> {
        Ok(JsRuntimeSuspendedState::default())
    }
    async fn run_turn(
        &self,
        turn: JsRuntimeTurn,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Result<JsRuntimeTurnOutcome, JsSubstrateError> {
        match turn.kind {
            JsRuntimeTurnKind::Bootstrap => Ok(JsRuntimeTurnOutcome::Completed {
                completion: JsRuntimeTurnCompletion::default(),
            }),
            JsRuntimeTurnKind::EvaluateEntrypoint { request } => {
                let execution = self.execute(request, cancellation).await?;
                Ok(JsRuntimeTurnOutcome::Completed {
                    completion: JsRuntimeTurnCompletion {
                        execution: Some(execution),
                        metadata: BTreeMap::new(),
                    },
                })
            }
            unsupported => Err(JsSubstrateError::UnsupportedTurn {
                turn: js_turn_kind_label(&unsupported).to_string(),
            }),
        }
    }
    async fn execute(
        &self,
        request: JsExecutionRequest,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Result<JsExecutionReport, JsSubstrateError> {
        match self
            .run_turn(JsRuntimeTurn::evaluate_entrypoint(request), cancellation)
            .await?
        {
            JsRuntimeTurnOutcome::Completed { completion } => {
                completion
                    .execution
                    .ok_or(JsSubstrateError::EvaluationFailed {
                        entrypoint: self.handle().runtime_id,
                        message: "runtime turn completed without an execution report".to_string(),
                    })
            }
            JsRuntimeTurnOutcome::Threw { error } => Err(js_runtime_error_to_substrate(
                self.handle().runtime_id,
                error,
            )),
            JsRuntimeTurnOutcome::Terminated { reason } => {
                Err(JsSubstrateError::EvaluationFailed {
                    entrypoint: self.handle().runtime_id,
                    message: format!("runtime terminated: {reason}"),
                })
            }
            outcome => Err(JsSubstrateError::UnsupportedTurn {
                turn: format!("evaluate_entrypoint -> {}", js_turn_outcome_label(&outcome)),
            }),
        }
    }
    async fn close(&self) -> Result<(), JsSubstrateError>;
}

#[async_trait(?Send)]
pub trait JsRuntimeHost: Send + Sync {
    fn name(&self) -> &str;
    fn fork_policy(&self) -> JsForkPolicy;
    fn scheduler(&self) -> Arc<dyn JsScheduler>;
    fn module_loader(&self) -> Arc<dyn JsModuleLoader>;
    fn host_services(&self) -> Arc<dyn JsHostServices>;
    async fn open_runtime(
        &self,
        request: JsRuntimeOpenRequest,
    ) -> Result<Arc<dyn JsRuntime>, JsSubstrateError>;
}

#[derive(Clone)]
pub struct DeterministicJsRuntimeHost {
    name: Arc<str>,
    scheduler: Arc<dyn JsScheduler>,
    clock: Arc<dyn JsClock>,
    entropy: Arc<dyn JsEntropySource>,
    module_loader: Arc<dyn JsModuleLoader>,
    host_services: Arc<dyn JsHostServices>,
    hooks: Arc<dyn JsExecutionHooks>,
    fork_policy: JsForkPolicy,
}

impl DeterministicJsRuntimeHost {
    pub fn new(
        module_loader: Arc<dyn JsModuleLoader>,
        host_services: Arc<dyn JsHostServices>,
    ) -> Self {
        Self {
            name: Arc::from("deterministic-js"),
            scheduler: Arc::new(crate::DeterministicJsScheduler::default()),
            clock: Arc::new(crate::FixedJsClock::default()),
            entropy: Arc::new(crate::DeterministicJsEntropySource::default()),
            module_loader,
            host_services,
            hooks: Arc::new(NoopJsExecutionHooks),
            fork_policy: JsForkPolicy::simulation_native_baseline(),
        }
    }

    pub fn with_scheduler(mut self, scheduler: Arc<dyn JsScheduler>) -> Self {
        self.scheduler = scheduler;
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn JsClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_entropy(mut self, entropy: Arc<dyn JsEntropySource>) -> Self {
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
impl JsRuntimeHost for DeterministicJsRuntimeHost {
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
        Ok(Arc::new(DeterministicJsRuntime {
            handle,
            configuration,
            scheduler: self.scheduler.clone(),
            clock: self.clock.clone(),
            entropy: self.entropy.clone(),
            module_loader: self.module_loader.clone(),
            host_services: self.host_services.clone(),
            hooks: self.hooks.clone(),
            attachment: Arc::new(AsyncMutex::new(JsRuntimeAttachmentState::default())),
            suspended: Arc::new(AsyncMutex::new(JsRuntimeSuspendedState::default())),
        }))
    }
}

struct DeterministicJsRuntime {
    handle: JsRuntimeHandle,
    configuration: JsRuntimeConfiguration,
    scheduler: Arc<dyn JsScheduler>,
    clock: Arc<dyn JsClock>,
    entropy: Arc<dyn JsEntropySource>,
    module_loader: Arc<dyn JsModuleLoader>,
    host_services: Arc<dyn JsHostServices>,
    hooks: Arc<dyn JsExecutionHooks>,
    attachment: Arc<AsyncMutex<JsRuntimeAttachmentState>>,
    suspended: Arc<AsyncMutex<JsRuntimeSuspendedState>>,
}

#[async_trait(?Send)]
impl JsRuntime for DeterministicJsRuntime {
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
        Ok(self.suspended.lock().await.clone())
    }

    async fn run_turn(
        &self,
        turn: JsRuntimeTurn,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Result<JsRuntimeTurnOutcome, JsSubstrateError> {
        self.attach(js_turn_kind_label(&turn.kind)).await;
        let result = match turn.kind {
            JsRuntimeTurnKind::Bootstrap => Ok(JsRuntimeTurnOutcome::Completed {
                completion: JsRuntimeTurnCompletion {
                    execution: None,
                    metadata: BTreeMap::from([(
                        "runtime_id".to_string(),
                        JsonValue::from(self.handle.runtime_id.clone()),
                    )]),
                },
            }),
            JsRuntimeTurnKind::EvaluateEntrypoint { request } => {
                let report = self.execute_request(request, cancellation).await?;
                self.update_suspended_from_report(&report).await;
                Ok(JsRuntimeTurnOutcome::Completed {
                    completion: JsRuntimeTurnCompletion {
                        execution: Some(report),
                        metadata: BTreeMap::new(),
                    },
                })
            }
            unsupported => Err(JsSubstrateError::UnsupportedTurn {
                turn: js_turn_kind_label(&unsupported).to_string(),
            }),
        };
        self.detach().await;
        result
    }

    async fn execute(
        &self,
        request: JsExecutionRequest,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Result<JsExecutionReport, JsSubstrateError> {
        self.execute_request(request, cancellation).await
    }

    async fn close(&self) -> Result<(), JsSubstrateError> {
        self.hooks.on_runtime_close(&self.handle).await
    }
}

#[derive(Default)]
struct ExecutionState {
    module_graph: Vec<String>,
    seen: BTreeSet<String>,
    host_calls: Vec<JsHostServiceCallRecord>,
    trace: Vec<JsTraceEvent>,
    result: Option<JsonValue>,
}

impl DeterministicJsRuntime {
    async fn execute_request(
        &self,
        request: JsExecutionRequest,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Result<JsExecutionReport, JsSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(JsSubstrateError::Cancelled {
                runtime_id: self.handle.runtime_id.clone(),
            });
        }
        let clock_now_millis = self.clock.now_millis();
        let entropy_sample = self.entropy.fill_bytes(8);
        let mut state = ExecutionState::default();
        let (entrypoint, root_source) = match request.kind {
            JsExecutionKind::Module { specifier } => {
                let resolved = self.module_loader.resolve(&specifier, None).await?;
                state.trace.push(module_resolve_event(
                    &specifier,
                    &resolved.canonical_specifier,
                    None,
                    resolved.kind,
                ));
                ensure_module_kind_allowed(&request.metadata, &self.handle.policy, &resolved)?;
                let loaded = self.module_loader.load(&resolved).await?;
                (resolved.canonical_specifier, loaded)
            }
            JsExecutionKind::Eval {
                source,
                virtual_specifier,
            } => {
                let specifier = virtual_specifier.unwrap_or_else(|| {
                    "terrace:/workspace/.terrace/runtime/eval/inline.mjs".to_string()
                });
                let loaded = JsLoadedModule {
                    resolved: crate::JsResolvedModule {
                        requested_specifier: specifier.clone(),
                        canonical_specifier: specifier.clone(),
                        kind: crate::JsModuleKind::Workspace,
                    },
                    source,
                    trace: Vec::new(),
                    metadata: BTreeMap::new(),
                };
                (specifier, loaded)
            }
        };
        self.walk_module(
            root_source,
            &entrypoint,
            &self.handle.policy,
            cancellation.clone(),
            &mut state,
        )
        .await?;
        state.trace.push(JsTraceEvent {
            phase: JsTracePhase::Scheduler,
            label: self.scheduler.name().to_string(),
            metadata: BTreeMap::from([(
                "clock_now_millis".to_string(),
                JsonValue::from(clock_now_millis),
            )]),
        });
        let scheduled_tasks = self.scheduler.drain().await;
        Ok(JsExecutionReport {
            runtime_id: self.handle.runtime_id.clone(),
            backend: self.handle.backend.clone(),
            entrypoint,
            result: state.result,
            module_graph: state.module_graph,
            host_calls: state.host_calls,
            trace: state.trace,
            clock_now_millis,
            entropy_sample,
            scheduled_tasks,
            metadata: self.handle.metadata.clone(),
        })
    }

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

    async fn update_suspended_from_report(&self, report: &JsExecutionReport) {
        let mut suspended = self.suspended.lock().await;
        suspended.pending_microtasks = 0;
        suspended.terminated = false;
        suspended.module_graph.clear();
        for (index, specifier) in report.module_graph.iter().enumerate() {
            let module_id = crate::JsModuleId::new((index + 1) as u64);
            suspended.module_graph.insert(
                module_id,
                crate::JsModuleGraphNode {
                    module_id,
                    specifier: specifier.clone(),
                    dependencies: Vec::new(),
                    evaluated: true,
                },
            );
        }
        suspended.metadata.insert(
            "last_entrypoint".to_string(),
            JsonValue::from(report.entrypoint.clone()),
        );
    }

    async fn walk_module(
        &self,
        module: JsLoadedModule,
        entrypoint: &str,
        policy: &JsRuntimePolicy,
        cancellation: Arc<dyn JsCancellationToken>,
        state: &mut ExecutionState,
    ) -> Result<(), JsSubstrateError> {
        let mut stack = vec![module];
        while let Some(module) = stack.pop() {
            if cancellation.is_cancelled() {
                return Err(JsSubstrateError::Cancelled {
                    runtime_id: self.handle.runtime_id.clone(),
                });
            }
            let canonical = module.resolved.canonical_specifier.clone();
            if !state.seen.insert(canonical.clone()) {
                continue;
            }
            self.scheduler
                .schedule(JsScheduledTask::ready(
                    JsTaskQueue::ModuleLoader,
                    canonical.clone(),
                ))
                .await;
            self.hooks.on_module_loaded(&module).await?;
            state.module_graph.push(canonical.clone());
            state.trace.push(JsTraceEvent {
                phase: JsTracePhase::ModuleLoad,
                label: canonical.clone(),
                metadata: BTreeMap::new(),
            });
            let directives = parse_fake_module(&module)?;
            let mut imports = Vec::new();
            for import in directives.imports {
                let resolved = self
                    .module_loader
                    .resolve(&import, Some(&canonical))
                    .await?;
                state.trace.push(module_resolve_event(
                    &import,
                    &resolved.canonical_specifier,
                    Some(&canonical),
                    resolved.kind,
                ));
                ensure_module_kind_allowed(&BTreeMap::new(), policy, &resolved)?;
                imports.push(self.module_loader.load(&resolved).await?);
            }
            imports.reverse();
            stack.extend(imports);
            for host_call in directives.host_calls {
                if !policy.allows_host_service(&host_call.service, &host_call.operation) {
                    return Err(JsSubstrateError::HostServiceDenied {
                        service: host_call.service,
                        operation: host_call.operation,
                        message: "host service is not visible in this runtime policy".to_string(),
                    });
                }
                self.scheduler
                    .schedule(JsScheduledTask::ready(
                        JsTaskQueue::HostCallbacks,
                        format!("{}::{}", host_call.service, host_call.operation),
                    ))
                    .await;
                let response = self.host_services.call(host_call.clone()).await?;
                let record = JsHostServiceCallRecord {
                    service: host_call.service.clone(),
                    operation: host_call.operation.clone(),
                    arguments: host_call.arguments.clone(),
                    result: response.result.clone(),
                    metadata: response.metadata.clone(),
                };
                self.hooks.on_host_service_call(&host_call, &record).await?;
                state.trace.push(JsTraceEvent {
                    phase: JsTracePhase::HostCall,
                    label: format!("{}::{}", host_call.service, host_call.operation),
                    metadata: response.metadata.clone(),
                });
                state.host_calls.push(record);
            }
            if canonical == entrypoint
                && let Some(result) = directives.export_default
            {
                state.result = Some(result);
            }
        }
        Ok(())
    }
}

struct ParsedFakeModule {
    imports: Vec<String>,
    host_calls: Vec<JsHostServiceRequest>,
    export_default: Option<JsonValue>,
}

fn parse_fake_module(module: &JsLoadedModule) -> Result<ParsedFakeModule, JsSubstrateError> {
    let mut imports = Vec::new();
    let mut host_calls = Vec::new();
    let mut export_default = None;

    for raw_line in module.source.lines() {
        let line = raw_line.trim();
        if let Some(specifier) = parse_import_line(line) {
            imports.push(specifier);
            continue;
        }
        if let Some(remainder) = line.strip_prefix("// terrace-host-call:") {
            let remainder = remainder.trim();
            let mut parts = remainder.splitn(3, ' ');
            let service = parts
                .next()
                .ok_or_else(|| JsSubstrateError::InvalidDirective {
                    module: module.resolved.canonical_specifier.clone(),
                    message: "missing service name".to_string(),
                })?;
            let operation = parts
                .next()
                .ok_or_else(|| JsSubstrateError::InvalidDirective {
                    module: module.resolved.canonical_specifier.clone(),
                    message: "missing operation name".to_string(),
                })?;
            let arguments = parts
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(serde_json::from_str)
                .transpose()?
                .unwrap_or(JsonValue::Null);
            host_calls.push(JsHostServiceRequest {
                service: service.to_string(),
                operation: operation.to_string(),
                arguments,
                metadata: BTreeMap::new(),
            });
            continue;
        }
        if let Some(remainder) = line.strip_prefix("export default ") {
            let value = remainder.trim_end_matches(';').trim();
            export_default = Some(serde_json::from_str(value)?);
        }
    }

    Ok(ParsedFakeModule {
        imports,
        host_calls,
        export_default,
    })
}

fn parse_import_line(line: &str) -> Option<String> {
    if !line.starts_with("import ") {
        return None;
    }
    extract_quoted_string(line)
}

fn extract_quoted_string(line: &str) -> Option<String> {
    let quote = if line.contains('"') {
        '"'
    } else if line.contains('\'') {
        '\''
    } else {
        return None;
    };
    let (_, tail) = line.split_once(quote)?;
    let (value, _) = tail.split_once(quote)?;
    Some(value.to_string())
}

fn module_resolve_event(
    requested_specifier: &str,
    canonical_specifier: &str,
    referrer: Option<&str>,
    kind: crate::JsModuleKind,
) -> JsTraceEvent {
    JsTraceEvent {
        phase: JsTracePhase::ModuleResolve,
        label: canonical_specifier.to_string(),
        metadata: BTreeMap::from([
            (
                "requested_specifier".to_string(),
                JsonValue::String(requested_specifier.to_string()),
            ),
            (
                "referrer".to_string(),
                referrer
                    .map(|value| JsonValue::String(value.to_string()))
                    .unwrap_or(JsonValue::Null),
            ),
            (
                "module_kind".to_string(),
                JsonValue::String(
                    match kind {
                        crate::JsModuleKind::Workspace => "workspace",
                        crate::JsModuleKind::HostCapability => "host_capability",
                        crate::JsModuleKind::Package => "package",
                    }
                    .to_string(),
                ),
            ),
        ]),
    }
}

pub(crate) fn js_turn_kind_label(kind: &JsRuntimeTurnKind) -> &'static str {
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

pub(crate) fn js_turn_outcome_label(outcome: &JsRuntimeTurnOutcome) -> &'static str {
    match outcome {
        JsRuntimeTurnOutcome::Completed { .. } => "completed",
        JsRuntimeTurnOutcome::Yielded { .. } => "yielded",
        JsRuntimeTurnOutcome::PendingHostOp { .. } => "pending_host_op",
        JsRuntimeTurnOutcome::PendingTimer { .. } => "pending_timer",
        JsRuntimeTurnOutcome::PendingMicrotasks => "pending_microtasks",
        JsRuntimeTurnOutcome::Threw { .. } => "threw",
        JsRuntimeTurnOutcome::Terminated { .. } => "terminated",
    }
}

fn js_runtime_error_to_substrate(
    entrypoint: String,
    error: JsRuntimeErrorReport,
) -> JsSubstrateError {
    JsSubstrateError::EvaluationFailed {
        entrypoint,
        message: error.message,
    }
}

pub(crate) fn ensure_module_kind_allowed(
    _metadata: &BTreeMap<String, JsonValue>,
    policy: &JsRuntimePolicy,
    resolved: &JsResolvedModule,
) -> Result<(), JsSubstrateError> {
    if policy.allows_module_kind(resolved.kind) {
        return Ok(());
    }
    Err(JsSubstrateError::ModulePolicyDenied {
        specifier: resolved.canonical_specifier.clone(),
        kind: resolved.kind,
        message: match resolved.kind {
            crate::JsModuleKind::Workspace => {
                "workspace module loading is disabled by policy".to_string()
            }
            crate::JsModuleKind::HostCapability => {
                "host capability modules are disabled by policy".to_string()
            }
            crate::JsModuleKind::Package => {
                "package module loading is disabled by policy".to_string()
            }
        },
    })
}
