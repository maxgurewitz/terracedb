use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    mem,
    path::PathBuf,
    rc::Rc,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use boa_engine::{
    Context, Finalize, JsNativeError, JsResult, JsString, JsValue, Module, NativeFunction, Script,
    Source, Trace,
    builtins::promise::PromiseState,
    context::{Clock as BoaClock, DefaultHooks, time::JsInstant},
    job::{GenericJob, Job, JobExecutor, NativeAsyncJob, PromiseJob, TimeoutJob},
    js_string,
    module::{
        ModuleLoader as BoaModuleLoader, ModuleRequest, Referrer, SyntheticModuleInitializer,
    },
    object::builtins::JsPromise,
    object::{FunctionObjectBuilder, JsData, ObjectInitializer},
    property::{Attribute, PropertyDescriptor},
};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    JsForkPolicy, JsLoadedModule, JsModuleKind, JsModuleLoader, JsResolvedModule, JsRuntimeHandle,
    JsRuntimeOpenRequest, JsRuntimePolicy, JsSubstrateError, JsonValue,
    entropy::JsEntropySource,
    host::JsHostServices,
    runtime::{
        JsCancellationToken, JsRuntime, JsRuntimeHost, NoopJsExecutionHooks,
        ensure_module_kind_allowed,
    },
    scheduler::{JsScheduledTask, JsScheduler, JsSchedulerSnapshot, JsTaskQueue},
    time::JsClock,
    types::{
        JsExecutionKind, JsExecutionReport, JsExecutionRequest, JsHostServiceCallRecord,
        JsHostServiceRequest, JsTraceEvent, JsTracePhase,
    },
};

const HOST_CAPTURE_SEPARATOR: &str = "\u{001f}";

pub trait BoaJsScheduler: Send + Sync {
    fn name(&self) -> &str;
    fn schedule(&self, task: JsScheduledTask);
    fn drain(&self) -> Vec<JsScheduledTask>;
    fn snapshot(&self) -> JsSchedulerSnapshot;
}

pub trait BoaJsExecutionHooks: Send + Sync {
    fn on_runtime_open(&self, _handle: &JsRuntimeHandle) -> Result<(), JsSubstrateError> {
        Ok(())
    }

    fn on_module_loaded(&self, _module: &JsLoadedModule) -> Result<(), JsSubstrateError> {
        Ok(())
    }

    fn on_host_service_call(
        &self,
        _request: &JsHostServiceRequest,
        _record: &JsHostServiceCallRecord,
    ) -> Result<(), JsSubstrateError> {
        Ok(())
    }

    fn on_runtime_close(&self, _handle: &JsRuntimeHandle) -> Result<(), JsSubstrateError> {
        Ok(())
    }
}

struct AsyncBoaSchedulerAdapter {
    inner: Arc<dyn BoaJsScheduler>,
}

impl AsyncBoaSchedulerAdapter {
    fn new(inner: Arc<dyn BoaJsScheduler>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl JsScheduler for AsyncBoaSchedulerAdapter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn schedule(&self, task: JsScheduledTask) {
        self.inner.schedule(task);
    }

    async fn drain(&self) -> Vec<JsScheduledTask> {
        self.inner.drain()
    }

    async fn snapshot(&self) -> JsSchedulerSnapshot {
        self.inner.snapshot()
    }
}

impl BoaJsScheduler for crate::DeterministicJsScheduler {
    fn name(&self) -> &str {
        <Self as JsScheduler>::name(self)
    }

    fn schedule(&self, task: JsScheduledTask) {
        self.scheduled_tasks
            .lock()
            .expect("boa scheduler mutex poisoned")
            .push(task);
    }

    fn drain(&self) -> Vec<JsScheduledTask> {
        let mut tasks = self
            .scheduled_tasks
            .lock()
            .expect("boa scheduler mutex poisoned");
        std::mem::take(tasks.as_mut())
    }

    fn snapshot(&self) -> JsSchedulerSnapshot {
        JsSchedulerSnapshot {
            scheduler: BoaJsScheduler::name(self).to_string(),
            scheduled_tasks: self
                .scheduled_tasks
                .lock()
                .expect("boa scheduler mutex poisoned")
                .clone(),
        }
    }
}

impl BoaJsExecutionHooks for NoopJsExecutionHooks {}

#[derive(Clone)]
pub struct BoaJsRuntimeHost {
    name: Arc<str>,
    scheduler: Arc<dyn BoaJsScheduler>,
    scheduler_async: Arc<dyn JsScheduler>,
    clock: Arc<dyn JsClock>,
    entropy: Arc<dyn JsEntropySource>,
    module_loader: Arc<dyn JsModuleLoader>,
    host_services: Arc<dyn JsHostServices>,
    hooks: Arc<dyn BoaJsExecutionHooks>,
    fork_policy: JsForkPolicy,
}

impl BoaJsRuntimeHost {
    pub fn new(
        module_loader: Arc<dyn JsModuleLoader>,
        host_services: Arc<dyn JsHostServices>,
    ) -> Self {
        let scheduler: Arc<dyn BoaJsScheduler> =
            Arc::new(crate::DeterministicJsScheduler::default());
        Self::from_parts(scheduler, module_loader, host_services)
    }

    fn from_parts(
        scheduler: Arc<dyn BoaJsScheduler>,
        module_loader: Arc<dyn JsModuleLoader>,
        host_services: Arc<dyn JsHostServices>,
    ) -> Self {
        Self {
            name: Arc::from("boa-js"),
            scheduler_async: Arc::new(AsyncBoaSchedulerAdapter::new(scheduler.clone())),
            scheduler,
            clock: Arc::new(crate::FixedJsClock::default()),
            entropy: Arc::new(crate::DeterministicJsEntropySource::default()),
            module_loader,
            host_services,
            hooks: Arc::new(NoopJsExecutionHooks),
            fork_policy: JsForkPolicy::simulation_native_baseline(),
        }
    }

    pub fn with_scheduler(mut self, scheduler: Arc<dyn BoaJsScheduler>) -> Self {
        self.scheduler_async = Arc::new(AsyncBoaSchedulerAdapter::new(scheduler.clone()));
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

    pub fn with_hooks(mut self, hooks: Arc<dyn BoaJsExecutionHooks>) -> Self {
        self.hooks = hooks;
        self
    }

    pub fn with_fork_policy(mut self, fork_policy: JsForkPolicy) -> Self {
        self.fork_policy = fork_policy;
        self
    }
}

#[async_trait(?Send)]
impl JsRuntimeHost for BoaJsRuntimeHost {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn fork_policy(&self) -> JsForkPolicy {
        self.fork_policy.clone()
    }

    fn scheduler(&self) -> Arc<dyn JsScheduler> {
        self.scheduler_async.clone()
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
            metadata: request.metadata,
        };
        self.hooks.on_runtime_open(&handle)?;
        Ok(Arc::new(BoaJsRuntime {
            handle,
            scheduler: self.scheduler.clone(),
            clock: self.clock.clone(),
            entropy: self.entropy.clone(),
            module_loader: self.module_loader.clone(),
            host_services: self.host_services.clone(),
            hooks: self.hooks.clone(),
            execution_lock: AsyncMutex::new(()),
        }))
    }
}

struct BoaJsRuntime {
    handle: JsRuntimeHandle,
    scheduler: Arc<dyn BoaJsScheduler>,
    clock: Arc<dyn JsClock>,
    entropy: Arc<dyn JsEntropySource>,
    module_loader: Arc<dyn JsModuleLoader>,
    host_services: Arc<dyn JsHostServices>,
    hooks: Arc<dyn BoaJsExecutionHooks>,
    execution_lock: AsyncMutex<()>,
}

enum PreparedBoaExecution {
    Module {
        entrypoint: String,
        loaded: JsLoadedModule,
    },
    Eval {
        source: String,
        virtual_specifier: Option<String>,
    },
}

#[async_trait(?Send)]
impl JsRuntime for BoaJsRuntime {
    fn handle(&self) -> JsRuntimeHandle {
        self.handle.clone()
    }

    async fn execute(
        &self,
        request: JsExecutionRequest,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Result<JsExecutionReport, JsSubstrateError> {
        let _guard = self.execution_lock.lock().await;
        if cancellation.is_cancelled() {
            return Err(JsSubstrateError::Cancelled {
                runtime_id: self.handle.runtime_id.clone(),
            });
        }

        let clock_now_millis = self.clock.now_millis();
        let entropy_sample = self.entropy.fill_bytes(8);
        let execution_state = Arc::new(SharedExecutionState::default());
        let context_state = BoaContextRuntimeState {
            policy: self.handle.policy.clone(),
            entropy: self.entropy.clone(),
            host_services: self.host_services.clone(),
            hooks: self.hooks.clone(),
            scheduler: self.scheduler.clone(),
            state: execution_state.clone(),
        };
        let module_loader = Rc::new(BoaModuleLoaderBridge::new(
            self.handle.runtime_id.clone(),
            self.handle.policy.clone(),
            self.module_loader.clone(),
            self.scheduler.clone(),
            self.hooks.clone(),
            execution_state.clone(),
            cancellation.clone(),
        ));
        let job_executor = Rc::new(BoaJobExecutor::new(self.scheduler.clone()));
        let mut context = Context::builder()
            .clock(Rc::new(BoaClockAdapter::new(self.clock.clone())))
            .host_hooks(Rc::new(DefaultHooks))
            .job_executor(job_executor.clone())
            .module_loader(module_loader.clone())
            .build()
            .map_err(|error| execution_error("<runtime>", error))?;
        context.insert_data(context_state);
        install_runtime_overrides(&mut context)?;
        let prepared_request = self
            .prepare_request(request, module_loader.as_ref())
            .await?;

        let (entrypoint, result) = self
            .execute_request(
                prepared_request,
                module_loader.as_ref(),
                job_executor.clone(),
                &mut context,
            )
            .await?;
        job_executor.flush_scheduled_tasks();
        let (module_graph, host_calls, trace) = execution_state.snapshot();
        let mut trace = trace;
        trace.push(JsTraceEvent {
            phase: JsTracePhase::Scheduler,
            label: self.scheduler.name().to_string(),
            metadata: BTreeMap::from([(
                "clock_now_millis".to_string(),
                JsonValue::from(clock_now_millis),
            )]),
        });

        Ok(JsExecutionReport {
            runtime_id: self.handle.runtime_id.clone(),
            backend: self.handle.backend.clone(),
            entrypoint,
            result,
            module_graph,
            host_calls,
            trace,
            clock_now_millis,
            entropy_sample,
            scheduled_tasks: self.scheduler.drain(),
            metadata: self.handle.metadata.clone(),
        })
    }

    async fn close(&self) -> Result<(), JsSubstrateError> {
        self.hooks.on_runtime_close(&self.handle)
    }
}

impl BoaJsRuntime {
    async fn prepare_request(
        &self,
        request: JsExecutionRequest,
        loader: &BoaModuleLoaderBridge,
    ) -> Result<PreparedBoaExecution, JsSubstrateError> {
        match request.kind {
            JsExecutionKind::Module { specifier } => {
                let resolved = loader.resolve_requested(&specifier, None).await?;
                let entrypoint = resolved.canonical_specifier.clone();
                let loaded = loader.load_resolved(&resolved).await?;
                Ok(PreparedBoaExecution::Module { entrypoint, loaded })
            }
            JsExecutionKind::Eval {
                source,
                virtual_specifier,
            } => Ok(PreparedBoaExecution::Eval {
                source,
                virtual_specifier,
            }),
        }
    }

    async fn execute_request(
        &self,
        request: PreparedBoaExecution,
        loader: &BoaModuleLoaderBridge,
        job_executor: Rc<BoaJobExecutor>,
        context: &mut Context,
    ) -> Result<(String, Option<JsonValue>), JsSubstrateError> {
        match request {
            PreparedBoaExecution::Module { entrypoint, loaded } => {
                let module = loader.materialize_module(loaded, context)?;
                let result =
                    run_module_to_completion(&module, job_executor, context, &entrypoint).await?;
                Ok((entrypoint, result))
            }
            PreparedBoaExecution::Eval {
                source,
                virtual_specifier,
            } => {
                let entrypoint = virtual_specifier.unwrap_or_else(|| {
                    "terrace:/workspace/.terrace/runtime/eval/inline.mjs".to_string()
                });
                let entrypoint_path = PathBuf::from(entrypoint.clone());
                let result = match Script::parse(
                    Source::from_bytes(source.as_bytes()).with_path(&entrypoint_path),
                    None,
                    context,
                ) {
                    Ok(script) => {
                        let value = script
                            .evaluate(context)
                            .map_err(|error| execution_error(&entrypoint, error))?;
                        drain_jobs(job_executor, context, &entrypoint).await?;
                        value
                            .to_json(context)
                            .map_err(|error| execution_error(&entrypoint, error))?
                    }
                    Err(_) => {
                        let module =
                            loader.load_inline_module(entrypoint.clone(), source, context)?;
                        run_module_to_completion(&module, job_executor, context, &entrypoint)
                            .await?
                    }
                };
                Ok((entrypoint, result))
            }
        }
    }
}

#[derive(Clone, Finalize)]
struct BoaContextRuntimeState {
    policy: JsRuntimePolicy,
    entropy: Arc<dyn JsEntropySource>,
    host_services: Arc<dyn JsHostServices>,
    hooks: Arc<dyn BoaJsExecutionHooks>,
    scheduler: Arc<dyn BoaJsScheduler>,
    state: Arc<SharedExecutionState>,
}

impl JsData for BoaContextRuntimeState {}

// SAFETY: runtime state only stores Rust-owned handles and does not contain GC-managed values.
unsafe impl Trace for BoaContextRuntimeState {
    boa_gc::empty_trace!();
}

#[derive(Default)]
struct SharedExecutionState {
    module_graph: Mutex<Vec<String>>,
    module_graph_seen: Mutex<BTreeSet<String>>,
    loaded_seen: Mutex<BTreeSet<String>>,
    host_calls: Mutex<Vec<JsHostServiceCallRecord>>,
    trace: Mutex<Vec<JsTraceEvent>>,
}

impl SharedExecutionState {
    fn record_resolve(
        &self,
        requested_specifier: &str,
        canonical_specifier: &str,
        referrer: Option<&str>,
    ) {
        if self
            .module_graph_seen
            .lock()
            .expect("boa module graph seen mutex poisoned")
            .insert(canonical_specifier.to_string())
        {
            self.module_graph
                .lock()
                .expect("boa module graph mutex poisoned")
                .push(canonical_specifier.to_string());
        }
        self.trace
            .lock()
            .expect("boa trace mutex poisoned")
            .push(JsTraceEvent {
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
                ]),
            });
    }

    fn record_module_loaded(&self, module: &JsLoadedModule) {
        let canonical = module.resolved.canonical_specifier.clone();
        if !self
            .loaded_seen
            .lock()
            .expect("boa loaded-seen mutex poisoned")
            .insert(canonical.clone())
        {
            return;
        }
        self.trace
            .lock()
            .expect("boa trace mutex poisoned")
            .push(JsTraceEvent {
                phase: JsTracePhase::ModuleLoad,
                label: canonical,
                metadata: module.metadata.clone(),
            });
    }

    fn record_host_call(&self, record: JsHostServiceCallRecord) {
        self.trace
            .lock()
            .expect("boa trace mutex poisoned")
            .push(JsTraceEvent {
                phase: JsTracePhase::HostCall,
                label: format!("{}::{}", record.service, record.operation),
                metadata: record.metadata.clone(),
            });
        self.host_calls
            .lock()
            .expect("boa host call mutex poisoned")
            .push(record);
    }

    fn snapshot(&self) -> (Vec<String>, Vec<JsHostServiceCallRecord>, Vec<JsTraceEvent>) {
        (
            self.module_graph
                .lock()
                .expect("boa module graph mutex poisoned")
                .clone(),
            self.host_calls
                .lock()
                .expect("boa host call mutex poisoned")
                .clone(),
            self.trace.lock().expect("boa trace mutex poisoned").clone(),
        )
    }
}

struct BoaModuleLoaderBridge {
    runtime_id: String,
    policy: JsRuntimePolicy,
    module_loader: Arc<dyn JsModuleLoader>,
    scheduler: Arc<dyn BoaJsScheduler>,
    hooks: Arc<dyn BoaJsExecutionHooks>,
    state: Arc<SharedExecutionState>,
    cancellation: Arc<dyn JsCancellationToken>,
    modules: RefCell<BTreeMap<String, Module>>,
}

impl BoaModuleLoaderBridge {
    fn new(
        runtime_id: String,
        policy: JsRuntimePolicy,
        module_loader: Arc<dyn JsModuleLoader>,
        scheduler: Arc<dyn BoaJsScheduler>,
        hooks: Arc<dyn BoaJsExecutionHooks>,
        state: Arc<SharedExecutionState>,
        cancellation: Arc<dyn JsCancellationToken>,
    ) -> Self {
        Self {
            runtime_id,
            policy,
            module_loader,
            scheduler,
            hooks,
            state,
            cancellation,
            modules: RefCell::new(BTreeMap::new()),
        }
    }

    fn load_inline_module(
        &self,
        specifier: String,
        source: String,
        context: &mut Context,
    ) -> Result<Module, JsSubstrateError> {
        let loaded = JsLoadedModule {
            resolved: JsResolvedModule {
                requested_specifier: specifier.clone(),
                canonical_specifier: specifier.clone(),
                kind: JsModuleKind::Workspace,
            },
            source,
            trace: Vec::new(),
            metadata: BTreeMap::new(),
        };
        self.materialize_module(loaded, context)
    }

    async fn resolve_requested(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<JsResolvedModule, JsSubstrateError> {
        if self.cancellation.is_cancelled() {
            return Err(JsSubstrateError::Cancelled {
                runtime_id: self.runtime_id.clone(),
            });
        }
        let resolved = self.module_loader.resolve(specifier, referrer).await?;
        self.state
            .record_resolve(specifier, &resolved.canonical_specifier, referrer);
        ensure_module_kind_allowed(&BTreeMap::new(), &self.policy, &resolved)?;
        Ok(resolved)
    }

    async fn load_resolved(
        &self,
        resolved: &JsResolvedModule,
    ) -> Result<JsLoadedModule, JsSubstrateError> {
        if self.cancellation.is_cancelled() {
            return Err(JsSubstrateError::Cancelled {
                runtime_id: self.runtime_id.clone(),
            });
        }
        self.module_loader.load(resolved).await
    }

    fn materialize_module(
        &self,
        loaded: JsLoadedModule,
        context: &mut Context,
    ) -> Result<Module, JsSubstrateError> {
        let canonical = loaded.resolved.canonical_specifier.clone();
        if let Some(module) = self.modules.borrow().get(&canonical).cloned() {
            return Ok(module);
        }

        if matches!(loaded.resolved.kind, JsModuleKind::HostCapability) {
            ensure_visible_host_surface(&self.policy, &loaded)?;
        }
        self.scheduler.schedule(JsScheduledTask {
            queue: JsTaskQueue::ModuleLoader,
            label: canonical.clone(),
            deadline_millis: None,
        });
        self.hooks.on_module_loaded(&loaded)?;
        self.state.record_module_loaded(&loaded);

        let module = match loaded.resolved.kind {
            JsModuleKind::HostCapability => synthetic_host_module(&loaded, context)
                .map_err(|error| execution_error(&canonical, error))?,
            JsModuleKind::Workspace | JsModuleKind::Package => {
                let path = PathBuf::from(loaded.resolved.canonical_specifier.clone());
                Module::parse(
                    Source::from_bytes(loaded.source.as_bytes()).with_path(&path),
                    None,
                    context,
                )
                .map_err(|error| execution_error(&canonical, error))?
            }
        };
        self.modules.borrow_mut().insert(canonical, module.clone());
        Ok(module)
    }
}

impl BoaModuleLoader for BoaModuleLoaderBridge {
    async fn load_imported_module(
        self: Rc<Self>,
        referrer: Referrer,
        request: ModuleRequest,
        context: &RefCell<&mut Context>,
    ) -> JsResult<Module> {
        let requested = request.specifier().to_std_string_escaped();
        let referrer = referrer
            .path()
            .map(|path| path.to_string_lossy().into_owned());
        if self.cancellation.is_cancelled() {
            return Err(js_error(JsSubstrateError::Cancelled {
                runtime_id: self.runtime_id.clone(),
            }));
        }
        let resolved = self
            .resolve_requested(&requested, referrer.as_deref())
            .await
            .map_err(js_error)?;
        let loaded = self.load_resolved(&resolved).await.map_err(js_error)?;
        let module = {
            let mut context = context.borrow_mut();
            self.materialize_module(loaded, &mut context)
                .map_err(js_error)?
        };
        Ok(module)
    }
}

struct BoaClockAdapter {
    clock: Arc<dyn JsClock>,
}

impl BoaClockAdapter {
    fn new(clock: Arc<dyn JsClock>) -> Self {
        Self { clock }
    }
}

impl BoaClock for BoaClockAdapter {
    fn now(&self) -> JsInstant {
        let millis = self.clock.now_millis();
        JsInstant::new(millis / 1000, ((millis % 1000) * 1_000_000) as u32)
    }

    fn system_time_millis(&self) -> i64 {
        self.clock.now_millis() as i64
    }
}

struct BoaJobExecutor {
    scheduler: Arc<dyn BoaJsScheduler>,
    buffered_tasks: Mutex<Vec<JsScheduledTask>>,
    promise_jobs: RefCell<VecDeque<PromiseJob>>,
    async_jobs: RefCell<VecDeque<NativeAsyncJob>>,
    timeout_jobs: RefCell<BTreeMap<JsInstant, Vec<TimeoutJob>>>,
    generic_jobs: RefCell<VecDeque<GenericJob>>,
}

impl BoaJobExecutor {
    fn new(scheduler: Arc<dyn BoaJsScheduler>) -> Self {
        Self {
            scheduler,
            buffered_tasks: Mutex::new(Vec::new()),
            promise_jobs: RefCell::new(VecDeque::new()),
            async_jobs: RefCell::new(VecDeque::new()),
            timeout_jobs: RefCell::new(BTreeMap::new()),
            generic_jobs: RefCell::new(VecDeque::new()),
        }
    }

    fn clear(&self) {
        self.buffered_tasks
            .lock()
            .expect("boa job buffer mutex poisoned")
            .clear();
        self.promise_jobs.borrow_mut().clear();
        self.async_jobs.borrow_mut().clear();
        self.timeout_jobs.borrow_mut().clear();
        self.generic_jobs.borrow_mut().clear();
    }

    fn flush_scheduled_tasks(&self) {
        let tasks: Vec<JsScheduledTask> = {
            let mut buffered = self
                .buffered_tasks
                .lock()
                .expect("boa job buffer mutex poisoned");
            mem::take(buffered.as_mut())
        };
        for task in tasks {
            self.scheduler.schedule(task);
        }
    }

    fn has_ready_timeout_jobs(&self, context: &RefCell<&mut Context>) -> bool {
        let now = context.borrow().clock().now();
        self.timeout_jobs
            .borrow()
            .iter()
            .any(|(instant, _)| &now >= instant)
    }

    fn run_ready_timeout_jobs(&self, context: &RefCell<&mut Context>) -> JsResult<()> {
        let now = context.borrow().clock().now();
        let ready_instants = self
            .timeout_jobs
            .borrow()
            .range(..=now)
            .map(|(instant, _)| *instant)
            .collect::<Vec<_>>();
        let mut ready_jobs = Vec::new();
        {
            let mut timeouts = self.timeout_jobs.borrow_mut();
            for instant in ready_instants {
                if let Some(mut jobs) = timeouts.remove(&instant) {
                    jobs.retain(|job| !job.cancelled());
                    ready_jobs.extend(jobs);
                }
            }
        }
        for job in ready_jobs {
            job.call(&mut context.borrow_mut())?;
        }
        Ok(())
    }

    fn run_promise_jobs(&self, context: &RefCell<&mut Context>) -> JsResult<()> {
        let promise_jobs = mem::take(&mut *self.promise_jobs.borrow_mut());
        for job in promise_jobs {
            job.call(&mut context.borrow_mut())?;
        }
        Ok(())
    }

    fn run_generic_jobs(&self, context: &RefCell<&mut Context>) -> JsResult<()> {
        let generic_jobs = mem::take(&mut *self.generic_jobs.borrow_mut());
        for job in generic_jobs {
            job.call(&mut context.borrow_mut())?;
        }
        Ok(())
    }
}

impl JobExecutor for BoaJobExecutor {
    fn enqueue_job(self: Rc<Self>, job: Job, context: &mut Context) {
        match job {
            Job::PromiseJob(job) => {
                self.promise_jobs.borrow_mut().push_back(job);
                self.buffered_tasks
                    .lock()
                    .expect("boa job buffer mutex poisoned")
                    .push(JsScheduledTask {
                        queue: JsTaskQueue::PromiseJobs,
                        label: "promise-job".to_string(),
                        deadline_millis: None,
                    });
            }
            Job::AsyncJob(job) => {
                self.async_jobs.borrow_mut().push_back(job);
                self.buffered_tasks
                    .lock()
                    .expect("boa job buffer mutex poisoned")
                    .push(JsScheduledTask {
                        queue: JsTaskQueue::PromiseJobs,
                        label: "async-job".to_string(),
                        deadline_millis: None,
                    });
            }
            Job::TimeoutJob(job) => {
                let now = context.clock().now();
                let deadline = now + job.timeout();
                self.timeout_jobs
                    .borrow_mut()
                    .entry(deadline)
                    .or_default()
                    .push(job);
                self.buffered_tasks
                    .lock()
                    .expect("boa job buffer mutex poisoned")
                    .push(JsScheduledTask {
                        queue: JsTaskQueue::Timers,
                        label: "timeout-job".to_string(),
                        deadline_millis: Some(deadline.millis_since_epoch()),
                    });
            }
            Job::GenericJob(job) => {
                self.generic_jobs.borrow_mut().push_back(job);
                self.buffered_tasks
                    .lock()
                    .expect("boa job buffer mutex poisoned")
                    .push(JsScheduledTask {
                        queue: JsTaskQueue::PromiseJobs,
                        label: "generic-job".to_string(),
                        deadline_millis: None,
                    });
            }
            _ => {}
        }
    }

    fn run_jobs(self: Rc<Self>, context: &mut Context) -> JsResult<()> {
        futures::executor::block_on(self.run_jobs_async(&RefCell::new(context)))
    }

    async fn run_jobs_async(self: Rc<Self>, context: &RefCell<&mut Context>) -> JsResult<()>
    where
        Self: Sized,
    {
        loop {
            if self.promise_jobs.borrow().is_empty()
                && self.async_jobs.borrow().is_empty()
                && self.generic_jobs.borrow().is_empty()
                && !self.has_ready_timeout_jobs(context)
            {
                break;
            }

            self.run_ready_timeout_jobs(context)?;
            self.run_promise_jobs(context)?;
            self.run_generic_jobs(context)?;

            let next_async_job = { self.async_jobs.borrow_mut().pop_front() };
            if let Some(job) = next_async_job
                && let Err(error) = job.call(context).await
            {
                self.clear();
                return Err(error);
            }

            context.borrow_mut().clear_kept_objects();
        }

        Ok(())
    }
}

fn install_runtime_overrides(context: &mut Context) -> Result<(), JsSubstrateError> {
    let random = FunctionObjectBuilder::new(
        context.realm(),
        NativeFunction::from_copy_closure(math_random_dispatch),
    )
    .name(js_string!("random"))
    .length(0)
    .constructor(false)
    .build();
    let math = context
        .global_object()
        .get(js_string!("Math"), context)
        .map_err(|error| execution_error("<runtime>", error))?;
    let math = math
        .as_object()
        .ok_or_else(|| JsSubstrateError::EvaluationFailed {
            entrypoint: "<runtime>".to_string(),
            message: "Math intrinsic is unavailable".to_string(),
        })?;
    math.define_property_or_throw(
        js_string!("random"),
        PropertyDescriptor::builder()
            .value(random)
            .writable(true)
            .enumerable(false)
            .configurable(true),
        context,
    )
    .map_err(|error| execution_error("<runtime>", error))?;
    Ok(())
}

fn synthetic_host_module(loaded: &JsLoadedModule, context: &mut Context) -> JsResult<Module> {
    let surface = host_module_surface(loaded);
    let mut exports = surface
        .exports
        .iter()
        .map(|export| JsString::from(export.as_str()))
        .collect::<Vec<_>>();
    exports.push(js_string!("default"));
    let encoded = format!(
        "{}{HOST_CAPTURE_SEPARATOR}{}",
        surface.service,
        surface.exports.join(HOST_CAPTURE_SEPARATOR)
    );
    Ok(Module::synthetic(
        &exports,
        SyntheticModuleInitializer::from_copy_closure_with_captures(
            |module, encoded, context| {
                let encoded = encoded.to_std_string_escaped();
                let (service, exports) = encoded
                    .split_once(HOST_CAPTURE_SEPARATOR)
                    .ok_or_else(|| js_error_message("host capability capture is invalid"))?;
                let export_names = exports
                    .split(HOST_CAPTURE_SEPARATOR)
                    .filter(|candidate| !candidate.is_empty())
                    .collect::<Vec<_>>();
                let mut functions = Vec::with_capacity(export_names.len());
                let mut default_value = None;
                for export_name in &export_names {
                    let capture =
                        JsString::from(format!("{service}{HOST_CAPTURE_SEPARATOR}{export_name}"));
                    let function = FunctionObjectBuilder::new(
                        context.realm(),
                        NativeFunction::from_copy_closure_with_captures(
                            host_capability_dispatch,
                            capture,
                        ),
                    )
                    .name(JsString::from(*export_name))
                    .length(1)
                    .constructor(false)
                    .build();
                    if default_value.is_none() && export_names.len() == 1 {
                        default_value = Some(function.clone().into());
                    }
                    module.set_export(&JsString::from(*export_name), function.clone().into())?;
                    functions.push((*export_name, function));
                }
                let default_export = if export_names.len() == 1 {
                    default_value.expect("single export default should be set")
                } else {
                    let mut default_object = ObjectInitializer::new(context);
                    for (export_name, function) in functions {
                        default_object.property(
                            JsString::from(export_name),
                            function,
                            Attribute::all(),
                        );
                    }
                    default_object.build().into()
                };
                let default_name = js_string!("default");
                module.set_export(&default_name, default_export)?;
                Ok(())
            },
            JsString::from(encoded),
        ),
        Some(PathBuf::from(loaded.resolved.canonical_specifier.clone())),
        None,
        context,
    ))
}

fn host_capability_dispatch(
    _this: &JsValue,
    args: &[JsValue],
    capture: &JsString,
    context: &mut Context,
) -> JsResult<JsValue> {
    let encoded = capture.to_std_string_escaped();
    let (service, operation) = encoded
        .split_once(HOST_CAPTURE_SEPARATOR)
        .ok_or_else(|| js_error_message("host capability capture is invalid"))?;
    let arguments = js_args_to_json(args, context)?;
    let runtime = context_runtime(context)?;
    if !runtime.policy.allows_host_service(service, operation) {
        return Err(js_error(JsSubstrateError::HostServiceDenied {
            service: service.to_string(),
            operation: operation.to_string(),
            message: "host service is not visible in this runtime policy".to_string(),
        }));
    }
    let request = JsHostServiceRequest {
        service: service.to_string(),
        operation: operation.to_string(),
        arguments,
        metadata: BTreeMap::new(),
    };
    runtime.scheduler.schedule(JsScheduledTask {
        queue: JsTaskQueue::HostCallbacks,
        label: format!("{service}::{operation}"),
        deadline_millis: None,
    });
    let host_services = runtime.host_services.clone();
    let hooks = runtime.hooks.clone();
    let state = runtime.state.clone();
    let promise = JsPromise::from_async_fn(
        async move |context: &RefCell<&mut Context>| {
            let response = host_services
                .call(request.clone())
                .await
                .map_err(js_error)?;
            let record = JsHostServiceCallRecord {
                service: request.service.clone(),
                operation: request.operation.clone(),
                arguments: request.arguments.clone(),
                result: response.result.clone(),
                metadata: response.metadata.clone(),
            };
            hooks
                .on_host_service_call(&request, &record)
                .map_err(js_error)?;
            state.record_host_call(record);

            let context = &mut context.borrow_mut();
            match response.result {
                Some(result) => JsValue::from_json(&result, context),
                None => Ok(JsValue::undefined()),
            }
        },
        context,
    );
    Ok(promise.into())
}

fn math_random_dispatch(
    _this: &JsValue,
    _args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let runtime = context_runtime(context)?;
    let bytes = runtime.entropy.fill_bytes(8);
    let mut sample = [0_u8; 8];
    sample.copy_from_slice(&bytes[..8]);
    let mantissa = u64::from_le_bytes(sample) >> 11;
    Ok(JsValue::from(mantissa as f64 / ((1_u64 << 53) as f64)))
}

fn context_runtime(context: &Context) -> JsResult<&BoaContextRuntimeState> {
    context
        .get_data::<BoaContextRuntimeState>()
        .ok_or_else(|| js_error_message("boa runtime host is not active"))
}

fn ensure_visible_host_surface(
    policy: &JsRuntimePolicy,
    module: &JsLoadedModule,
) -> Result<(), JsSubstrateError> {
    let surface = host_module_surface(module);
    if surface
        .exports
        .iter()
        .all(|operation| policy.allows_host_service(&surface.service, operation))
    {
        return Ok(());
    }
    Err(JsSubstrateError::HostServiceDenied {
        service: surface.service,
        operation: surface.exports.join(","),
        message: "host module exports are not visible in this runtime policy".to_string(),
    })
}

struct HostModuleSurface {
    service: String,
    exports: Vec<String>,
}

fn host_module_surface(module: &JsLoadedModule) -> HostModuleSurface {
    let service = module
        .metadata
        .get("host_service")
        .and_then(JsonValue::as_str)
        .unwrap_or("capability")
        .to_string();
    let exports = module
        .metadata
        .get("host_exports")
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(JsonValue::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            vec![
                module
                    .resolved
                    .canonical_specifier
                    .trim_start_matches("terrace:host/")
                    .rsplit('/')
                    .next()
                    .filter(|value| !value.is_empty())
                    .unwrap_or("capability")
                    .to_string(),
            ]
        });
    HostModuleSurface { service, exports }
}

async fn run_module_to_completion(
    module: &Module,
    job_executor: Rc<BoaJobExecutor>,
    context: &mut Context,
    entrypoint: &str,
) -> Result<Option<JsonValue>, JsSubstrateError> {
    let promise = module.load_link_evaluate(context);
    drain_jobs(job_executor, context, entrypoint).await?;
    match promise.state() {
        PromiseState::Pending => Err(JsSubstrateError::EvaluationFailed {
            entrypoint: entrypoint.to_string(),
            message: "module evaluation is still pending after the job queue drained".to_string(),
        }),
        PromiseState::Fulfilled(_) => export_default_value(module, context, entrypoint),
        PromiseState::Rejected(reason) => Err(JsSubstrateError::EvaluationFailed {
            entrypoint: entrypoint.to_string(),
            message: format!("{}", reason.display()),
        }),
    }
}

async fn drain_jobs(
    job_executor: Rc<BoaJobExecutor>,
    context: &mut Context,
    entrypoint: &str,
) -> Result<(), JsSubstrateError> {
    let context = RefCell::new(context);
    job_executor
        .run_jobs_async(&context)
        .await
        .map_err(|error| execution_error(entrypoint, error))
}

fn export_default_value(
    module: &Module,
    context: &mut Context,
    entrypoint: &str,
) -> Result<Option<JsonValue>, JsSubstrateError> {
    let value = module
        .namespace(context)
        .get(js_string!("default"), context)
        .map_err(|error| execution_error(entrypoint, error))?;
    if value.is_undefined() {
        return Ok(None);
    }
    value
        .to_json(context)
        .map_err(|error| execution_error(entrypoint, error))
}

fn js_args_to_json(args: &[JsValue], context: &mut Context) -> JsResult<JsonValue> {
    match args {
        [] => Ok(JsonValue::Null),
        [value] => value
            .to_json(context)?
            .ok_or_else(|| js_error_message("host service arguments must be JSON-serializable")),
        many => {
            let mut values = Vec::with_capacity(many.len());
            for value in many {
                values.push(value.to_json(context)?.ok_or_else(|| {
                    js_error_message("host service arguments must be JSON-serializable")
                })?);
            }
            Ok(JsonValue::Array(values))
        }
    }
}

fn execution_error(entrypoint: &str, error: impl std::fmt::Display) -> JsSubstrateError {
    JsSubstrateError::EvaluationFailed {
        entrypoint: entrypoint.to_string(),
        message: error.to_string(),
    }
}

fn js_error(error: JsSubstrateError) -> boa_engine::JsError {
    JsNativeError::typ().with_message(error.to_string()).into()
}

fn js_error_message(message: impl Into<String>) -> boa_engine::JsError {
    JsNativeError::typ().with_message(message.into()).into()
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::BTreeMap, rc::Rc, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use boa_engine::{
        Context, JsValue,
        job::{Job, JobExecutor, TimeoutJob},
    };
    use serde_json::json;

    use super::{BoaClockAdapter, BoaJobExecutor, BoaJsRuntimeHost, BoaJsScheduler};
    use crate::{
        DeterministicJsClock, DeterministicJsEntropySource, DeterministicJsHostServices,
        DeterministicJsScheduler, DeterministicJsServiceOutcome, FixedJsClock, JsExecutionRequest,
        JsForkPolicy, JsHostServiceRequest, JsHostServices, JsLoadedModule, JsModuleKind,
        JsModuleLoader, JsResolvedModule, JsRuntimeHost, JsRuntimeOpenRequest, JsRuntimePolicy,
        JsRuntimeProvenance, JsScheduledTask, JsScheduler, JsSubstrateError, JsTaskQueue,
        NeverCancel,
    };

    struct EmptyLoader;

    #[async_trait(?Send)]
    impl JsModuleLoader for EmptyLoader {
        async fn resolve(
            &self,
            specifier: &str,
            _referrer: Option<&str>,
        ) -> Result<JsResolvedModule, JsSubstrateError> {
            Err(JsSubstrateError::UnsupportedSpecifier {
                specifier: specifier.to_string(),
            })
        }

        async fn load(
            &self,
            resolved: &JsResolvedModule,
        ) -> Result<JsLoadedModule, JsSubstrateError> {
            Err(JsSubstrateError::ModuleNotFound {
                specifier: resolved.canonical_specifier.clone(),
            })
        }
    }

    struct ImportingLoader;

    #[async_trait(?Send)]
    impl JsModuleLoader for ImportingLoader {
        async fn resolve(
            &self,
            specifier: &str,
            referrer: Option<&str>,
        ) -> Result<JsResolvedModule, JsSubstrateError> {
            let canonical_specifier = match (specifier, referrer) {
                ("terrace:/importing-root.mjs", None) => "terrace:/importing-root.mjs",
                ("./helper.mjs", Some("terrace:/importing-root.mjs")) => "terrace:/helper.mjs",
                _ => {
                    return Err(JsSubstrateError::UnsupportedSpecifier {
                        specifier: specifier.to_string(),
                    });
                }
            };
            Ok(JsResolvedModule {
                requested_specifier: specifier.to_string(),
                canonical_specifier: canonical_specifier.to_string(),
                kind: JsModuleKind::Workspace,
            })
        }

        async fn load(
            &self,
            resolved: &JsResolvedModule,
        ) -> Result<JsLoadedModule, JsSubstrateError> {
            let source = match resolved.canonical_specifier.as_str() {
                "terrace:/importing-root.mjs" => {
                    "import helper from './helper.mjs'; export default helper;"
                }
                "terrace:/helper.mjs" => "export default { ok: true };",
                _ => {
                    return Err(JsSubstrateError::ModuleNotFound {
                        specifier: resolved.canonical_specifier.clone(),
                    });
                }
            };
            Ok(JsLoadedModule {
                resolved: resolved.clone(),
                source: source.to_string(),
                trace: Vec::new(),
                metadata: BTreeMap::new(),
            })
        }
    }

    #[tokio::test]
    async fn eval_prefers_valid_script_parse_even_when_module_tokens_appear_in_strings() {
        let runtime = BoaJsRuntimeHost::new(
            Arc::new(EmptyLoader),
            Arc::new(DeterministicJsHostServices::new()),
        )
        .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
        .with_clock(Arc::new(FixedJsClock::new(123)))
        .with_entropy(Arc::new(DeterministicJsEntropySource::new(7)))
        .open_runtime(JsRuntimeOpenRequest {
            runtime_id: "script-vs-module".to_string(),
            policy: JsRuntimePolicy::default(),
            provenance: JsRuntimeProvenance {
                backend: "boa-js".to_string(),
                host_model: "host-owned".to_string(),
                module_root: "/workspace".to_string(),
                volume_id: None,
                snapshot_sequence: None,
                durable_snapshot: false,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            metadata: Default::default(),
        })
        .await
        .expect("open runtime");

        let report = runtime
            .execute(
                JsExecutionRequest::eval(
                    r#"
const text = "import export await";
text;
"#,
                ),
                Arc::new(NeverCancel),
            )
            .await
            .expect("execute script eval");

        assert_eq!(
            report.result,
            Some(serde_json::Value::String("import export await".to_string()))
        );
    }

    #[tokio::test]
    async fn script_eval_drains_promise_jobs_before_serializing_result() {
        let runtime = BoaJsRuntimeHost::new(
            Arc::new(EmptyLoader),
            Arc::new(DeterministicJsHostServices::new()),
        )
        .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
        .with_clock(Arc::new(FixedJsClock::new(321)))
        .with_entropy(Arc::new(DeterministicJsEntropySource::new(13)))
        .open_runtime(JsRuntimeOpenRequest {
            runtime_id: "script-job-drain".to_string(),
            policy: JsRuntimePolicy::default(),
            provenance: JsRuntimeProvenance {
                backend: "boa-js".to_string(),
                host_model: "host-owned".to_string(),
                module_root: "/workspace".to_string(),
                volume_id: None,
                snapshot_sequence: None,
                durable_snapshot: false,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            metadata: Default::default(),
        })
        .await
        .expect("open runtime");

        let report = runtime
            .execute(
                JsExecutionRequest::eval(
                    r#"
const state = { done: false };
Promise.resolve().then(() => {
  state.done = true;
});
state;
"#,
                ),
                Arc::new(NeverCancel),
            )
            .await
            .expect("execute script eval");

        assert_eq!(report.result, Some(json!({ "done": true })));
    }

    #[tokio::test]
    async fn module_execution_supports_sync_imported_modules() {
        let runtime = BoaJsRuntimeHost::new(
            Arc::new(ImportingLoader),
            Arc::new(DeterministicJsHostServices::new()),
        )
        .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
        .with_clock(Arc::new(FixedJsClock::new(456)))
        .with_entropy(Arc::new(DeterministicJsEntropySource::new(11)))
        .open_runtime(JsRuntimeOpenRequest {
            runtime_id: "sync-importing-loader".to_string(),
            policy: JsRuntimePolicy::default(),
            provenance: JsRuntimeProvenance {
                backend: "boa-js".to_string(),
                host_model: "host-owned".to_string(),
                module_root: "/workspace".to_string(),
                volume_id: None,
                snapshot_sequence: None,
                durable_snapshot: false,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            metadata: Default::default(),
        })
        .await
        .expect("open runtime");

        let report = runtime
            .execute(
                JsExecutionRequest::module("terrace:/importing-root.mjs"),
                Arc::new(NeverCancel),
            )
            .await
            .expect("execute module");

        assert_eq!(report.result, Some(json!({ "ok": true })));
        assert_eq!(
            report.module_graph,
            vec![
                "terrace:/importing-root.mjs".to_string(),
                "terrace:/helper.mjs".to_string(),
            ]
        );
    }

    #[test]
    fn zero_delay_timeout_jobs_run_at_exact_deadline() {
        let job_executor = Rc::new(BoaJobExecutor::new(Arc::new(
            DeterministicJsScheduler::default(),
        )));
        let mut context = Context::builder()
            .clock(Rc::new(BoaClockAdapter::new(Arc::new(FixedJsClock::new(
                1_000,
            )))))
            .build()
            .expect("build context");
        let fired = Rc::new(Cell::new(false));
        let fired_clone = fired.clone();

        job_executor.clone().enqueue_job(
            Job::TimeoutJob(TimeoutJob::from_duration(
                move |_context| {
                    fired_clone.set(true);
                    Ok(JsValue::undefined())
                },
                Duration::ZERO,
            )),
            &mut context,
        );

        job_executor
            .clone()
            .run_jobs(&mut context)
            .expect("run jobs");

        assert!(fired.get());
    }

    #[test]
    fn delayed_timeout_jobs_wait_for_clock_advance_and_record_deadline() {
        let scheduler = Arc::new(DeterministicJsScheduler::default());
        let job_executor = Rc::new(BoaJobExecutor::new(scheduler.clone()));
        let clock = Arc::new(DeterministicJsClock::new(1_000));
        let mut context = Context::builder()
            .clock(Rc::new(BoaClockAdapter::new(clock.clone())))
            .build()
            .expect("build context");
        let fired = Rc::new(Cell::new(false));
        let fired_clone = fired.clone();

        job_executor.clone().enqueue_job(
            Job::TimeoutJob(TimeoutJob::from_duration(
                move |_context| {
                    fired_clone.set(true);
                    Ok(JsValue::undefined())
                },
                Duration::from_millis(250),
            )),
            &mut context,
        );
        job_executor.flush_scheduled_tasks();
        assert_eq!(
            BoaJsScheduler::snapshot(scheduler.as_ref()).scheduled_tasks,
            vec![JsScheduledTask::deadline(
                JsTaskQueue::Timers,
                "timeout-job",
                1_250,
            )]
        );

        job_executor
            .clone()
            .run_jobs(&mut context)
            .expect("run jobs before timer deadline");
        assert!(!fired.get());

        clock.advance_millis(250);
        job_executor
            .clone()
            .run_jobs(&mut context)
            .expect("run jobs after timer deadline");
        assert!(fired.get());
    }

    #[tokio::test]
    async fn deterministic_boa_adapters_tolerate_async_observers() {
        let scheduler = DeterministicJsScheduler::default();
        let scheduled = JsScheduledTask {
            queue: JsTaskQueue::PromiseJobs,
            label: "observer-safe".to_string(),
            deadline_millis: None,
        };
        let scheduler_guard = scheduler.gate.lock().await;
        BoaJsScheduler::schedule(&scheduler, scheduled.clone());
        drop(scheduler_guard);
        assert_eq!(
            JsScheduler::snapshot(&scheduler).await.scheduled_tasks,
            vec![scheduled.clone()]
        );
        assert_eq!(BoaJsScheduler::drain(&scheduler), vec![scheduled]);
        assert!(
            JsScheduler::snapshot(&scheduler)
                .await
                .scheduled_tasks
                .is_empty()
        );

        let host_services = DeterministicJsHostServices::new();
        host_services
            .register_outcome(
                "capability",
                "echo",
                DeterministicJsServiceOutcome::Response {
                    result: json!({"ok": true}),
                    metadata: BTreeMap::new(),
                },
            )
            .await;
        let response = JsHostServices::call(
            &host_services,
            JsHostServiceRequest {
                service: "capability".to_string(),
                operation: "echo".to_string(),
                arguments: json!({"message":"hello"}),
                metadata: BTreeMap::new(),
            },
        )
        .await
        .expect("call host service");
        assert_eq!(response.result, Some(json!({"ok": true})));
        assert_eq!(JsHostServices::calls(&host_services).await.len(), 1);
    }
}
