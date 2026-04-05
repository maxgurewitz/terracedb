use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::{
    EngineJsRuntimeHost, JsForkPolicy, JsSubstrateError,
    entropy::JsEntropySource,
    host::JsHostServices,
    loader::{JsLoadedModule, JsModuleLoader, JsResolvedModule},
    scheduler::JsScheduler,
    time::JsClock,
    types::{
        JsCompletedHostOperation, JsExecutionReport, JsExecutionRequest, JsHostServiceCallRecord,
        JsHostServiceRequest, JsRuntimeAttachmentState, JsRuntimeConfiguration,
        JsRuntimeErrorReport, JsRuntimeHandle, JsRuntimeOpenRequest, JsRuntimePolicy,
        JsRuntimeSuspendedState, JsRuntimeTurn, JsRuntimeTurnCompletion, JsRuntimeTurnKind,
        JsRuntimeTurnOutcome,
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
    async fn deliver_host_completion(
        &self,
        completion: JsCompletedHostOperation,
    ) -> Result<(), JsSubstrateError> {
        Err(JsSubstrateError::UnsupportedTurn {
            turn: format!("deliver_host_completion(op_id={})", completion.op_id.get()),
        })
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
        EngineJsRuntimeHost::new(self.module_loader.clone(), self.host_services.clone())
            .with_name(self.name.clone())
            .with_scheduler(self.scheduler.clone())
            .with_clock(self.clock.clone())
            .with_entropy(self.entropy.clone())
            .with_hooks(self.hooks.clone())
            .with_fork_policy(self.fork_policy.clone())
            .open_runtime(request)
            .await
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
