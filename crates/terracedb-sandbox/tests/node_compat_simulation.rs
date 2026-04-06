use std::{
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use serde_json::Value as JsonValue;
use terracedb::{
    DomainCpuBudget, ExecutionDomainBudget, ExecutionDomainOwner, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionDomainSpec, ExecutionResourceUsage, ExecutionUsageHandle,
    InMemoryResourceManager, ResourceManager,
};
use terracedb_sandbox::{
    SandboxBatchedDomainMemoryBudget, SandboxRuntimeMemoryBudget, SandboxRuntimeStateHandle,
    SandboxTrackedMemoryBudgetPolicy,
};
use terracedb_simulation::{SeededSimulationExecution, SeededSimulationRunner, SimulationContext, TraceEvent};
use terracedb_systemtest::{
    SimulationCaseContext, SimulationCaseSpec, SimulationCaseStatus, SimulationHarness,
    SimulationHarnessError, SteppedSimulationCaseExecution, SteppedSimulationSuiteDefinition,
};
use terracedb_vfs::CreateOptions;

#[path = "support/node_compat.rs"]
mod node_compat_support;

const DOMAIN_SIMULATION_CASE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct SingleCaseSimulationDiagnostics {
    runtime_state: Option<SandboxRuntimeStateHandle>,
    simulation_context: Option<SimulationContext>,
}

struct SingleCaseSimulationSuite<C> {
    case_id: &'static str,
    label: &'static str,
    seed: u64,
    timeout: Duration,
    start: fn(
        u64,
        Duration,
        Arc<StdMutex<SingleCaseSimulationDiagnostics>>,
    ) -> SeededSimulationExecution<C>,
    // Temporary test-side handoff for the current stepped harness API.
    // We intend to expose this harness in production apps, so this lock-based
    // capture path must be designed out before that point.
    capture: Arc<StdMutex<Option<C>>>,
    diagnostics: Arc<StdMutex<SingleCaseSimulationDiagnostics>>,
}

struct SingleCaseSimulationExecution<C> {
    execution: SeededSimulationExecution<C>,
    capture: Arc<StdMutex<Option<C>>>,
}

impl<C> SteppedSimulationCaseExecution for SingleCaseSimulationExecution<C>
where
    C: Clone + Send + Sync + 'static,
{
    fn step(&mut self) -> Result<bool, SimulationHarnessError> {
        self.execution
            .step()
            .map_err(|error| SimulationHarnessError::Runtime {
                message: error.to_string(),
            })
    }

    fn finish(self) -> Result<(), SimulationHarnessError> {
        let capture = self
            .execution
            .finish()
            .map_err(|error| SimulationHarnessError::Runtime {
                message: error.to_string(),
            })?;
        *self
            .capture
            .lock()
            .map_err(|_| SimulationHarnessError::InternalState {
                message: "single-case simulation capture mutex poisoned".to_string(),
            })? = Some(capture);
        Ok(())
    }
}

impl<C> SteppedSimulationSuiteDefinition for SingleCaseSimulationSuite<C>
where
    C: Clone + Send + Sync + 'static,
{
    type Fixture = StdMutex<Option<C>>;
    type Case = u64;
    type Execution = SingleCaseSimulationExecution<C>;

    fn prepare_stepped(&self) -> Result<Arc<Self::Fixture>, SimulationHarnessError> {
        Ok(self.capture.clone())
    }

    fn cases_stepped(
        &self,
        _fixture: Arc<Self::Fixture>,
    ) -> Result<Vec<SimulationCaseSpec<Self::Case>>, SimulationHarnessError> {
        Ok(vec![SimulationCaseSpec::new(
            self.case_id,
            self.label,
            self.seed,
            self.timeout,
        )])
    }

    fn start_case_stepped(
        &self,
        fixture: Arc<Self::Fixture>,
        case: SimulationCaseSpec<Self::Case>,
        _ctx: SimulationCaseContext,
    ) -> Result<Self::Execution, SimulationHarnessError> {
        Ok(SingleCaseSimulationExecution {
            execution: (self.start)(case.input, case.timeout, self.diagnostics.clone()),
            capture: fixture,
        })
    }
}

fn run_simulation_case_with_harness<C>(
    case_id: &'static str,
    label: &'static str,
    seed: u64,
    timeout: Duration,
    start: fn(
        u64,
        Duration,
        Arc<StdMutex<SingleCaseSimulationDiagnostics>>,
    ) -> SeededSimulationExecution<C>,
) -> turmoil::Result<C>
where
    C: Clone + Send + Sync + 'static,
{
    let diagnostics = Arc::new(StdMutex::new(SingleCaseSimulationDiagnostics::default()));
    let suite = Arc::new(SingleCaseSimulationSuite {
        case_id,
        label,
        seed,
        timeout,
        start,
        capture: Arc::new(StdMutex::new(None)),
        diagnostics: diagnostics.clone(),
    });
    let format_diagnostics = || {
        let simulation_trace = diagnostics
            .lock()
            .ok()
            .and_then(|diagnostics| diagnostics.simulation_context.clone())
            .map(|context| format_simulation_trace_for_timeout(&context))
            .filter(|trace| !trace.is_empty())
            .map(|trace| format!("\nsimulation trace:\n{trace}"))
            .unwrap_or_default();
        let runtime_trace = diagnostics
            .lock()
            .ok()
            .and_then(|diagnostics| diagnostics.runtime_state.clone())
            .map(|state| format_node_runtime_trace_for_timeout(&state))
            .filter(|trace| !trace.is_empty())
            .map(|trace| format!("\nnode runtime trace:\n{trace}"))
            .unwrap_or_default();
        format!("{simulation_trace}{runtime_trace}")
    };
    let report = SimulationHarness::new()
        .with_max_workers(1)
        .run_stepped_suite(suite.clone())
        .map_err(|error| {
            format!(
                "run simulation harness suite {case_id}: {error}{}",
                format_diagnostics()
            )
        })?;
    let case_report = report
        .cases
        .first()
        .ok_or_else(|| format!("simulation harness reported no cases for {case_id}"))?;
    match &case_report.status {
        SimulationCaseStatus::Passed => {}
        SimulationCaseStatus::Failed { message } => {
            return Err(format!(
                "simulation case {case_id} failed after {:?}: {message}{}",
                case_report.elapsed,
                format_diagnostics(),
            )
            .into())
        }
        SimulationCaseStatus::TimedOut { after } => {
            return Err(format!(
                "simulation case {case_id} timed out after {:?}: {}{}",
                after,
                report.failure_summary(),
                format_diagnostics(),
            )
            .into())
        }
        SimulationCaseStatus::Panicked { message } => {
            return Err(format!(
                "simulation case {case_id} panicked after {:?}: {message}{}",
                case_report.elapsed,
                format_diagnostics(),
            )
            .into())
        }
    }
    suite
        .capture
        .lock()
        .map_err(|_| format!("read simulation harness capture {case_id}: mutex poisoned"))?
        .take()
        .ok_or_else(|| format!("simulation harness did not capture result for {case_id}").into())
}

fn format_node_runtime_trace_for_timeout(state: &SandboxRuntimeStateHandle) -> String {
    let snapshot = state.node_runtime_trace_snapshot();
    if snapshot.recent_events.is_empty() && snapshot.last_exception.is_none() {
        return String::new();
    }
    let mut lines = vec![format!(
        "resolve_calls={} load_calls={} fs_calls={}",
        snapshot.resolve_calls, snapshot.load_calls, snapshot.fs_calls
    )];
    if let Some(exception) = snapshot.last_exception {
        lines.push(format!("last_exception={exception}"));
    }
    lines.extend(
        snapshot
            .recent_events
            .iter()
            .map(|event| format!("  - {event}")),
    );
    lines.join("\n")
}

fn format_simulation_trace_for_timeout(context: &SimulationContext) -> String {
    let trace = context.trace();
    if trace.is_empty() {
        return String::new();
    }
    let lines = trace
        .iter()
        .filter_map(|event| match event {
            TraceEvent::Checkpoint { label, metadata } => {
                let metadata = if metadata.is_empty() {
                    String::new()
                } else {
                    let entries = metadata
                        .iter()
                        .map(|(key, value)| format!("{key}={value}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(" ({entries})")
                };
                Some(format!("  - checkpoint: {label}{metadata}"))
            }
            other => Some(format!("  - {other:?}")),
        })
        .collect::<Vec<_>>();
    lines.join("\n")
}

async fn exec_node_command_with_case_timeout(
    session: &terracedb_sandbox::SandboxSession,
    entrypoint: &str,
    cwd: &str,
    timeout: Duration,
) -> Result<terracedb_sandbox::SandboxExecutionResult, terracedb_sandbox::SandboxError> {
    session
        .exec_node_command_with_timeout(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            cwd.to_string(),
            std::collections::BTreeMap::from([(
                "HOME".to_string(),
                "/workspace/home".to_string(),
            )]),
            node_compat_support::inner_node_execution_timeout(timeout),
        )
        .await
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatSimulationCapture {
    stdout: String,
    exit_code: i64,
    module_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatMemorySimulationCapture {
    exit_code: i64,
    managed_bytes: u64,
    gc_bytes: u64,
    accounted_bytes: u64,
    context_runtime_bytes: u64,
    task_queue_bytes: u64,
    compiled_code_bytes: u64,
    parser_retained_bytes: u64,
    module_cache_bytes: u64,
    host_buffer_bytes: u64,
    node_compat_state_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatBudgetFailureCapture {
    message: String,
    charged_bytes: u64,
    current_bytes: u64,
    gc_bytes: u64,
    batch_requests: u64,
    terminated_for_budget: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatProcessStateBudgetFailureCapture {
    message: String,
    budget_bytes: u64,
    charged_bytes: u64,
    current_bytes: u64,
    gc_bytes: u64,
    host_buffer_bytes: u64,
    node_compat_state_bytes: u64,
    batch_requests: u64,
    terminated_for_budget: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatTaskQueueBudgetFailureCapture {
    message: String,
    charged_bytes: u64,
    current_bytes: u64,
    gc_bytes: u64,
    task_queue_bytes: u64,
    batch_requests: u64,
    terminated_for_budget: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatModuleCacheBudgetFailureCapture {
    message: String,
    baseline_accounted_bytes: u64,
    budget_bytes: u64,
    charged_bytes: u64,
    current_bytes: u64,
    gc_bytes: u64,
    module_cache_bytes: u64,
    batch_requests: u64,
    terminated_for_budget: bool,
}

fn metadata_u64(
    metadata: &std::collections::BTreeMap<String, JsonValue>,
    key: &str,
) -> turmoil::Result<u64> {
    metadata
        .get(key)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| format!("missing runtime metadata field {key}").into())
}

async fn write_module_fixture_files(
    session: &terracedb_sandbox::SandboxSession,
    module_count: usize,
    payload_len: usize,
) -> Result<(), String> {
    let payload = "x".repeat(payload_len);
    for i in 0..module_count {
        let module_path = format!("/workspace/app/module-{i}.cjs");
        let module_source = format!("module.exports = \"m\" + {:?} + {};\n", payload, i);
        session
            .filesystem()
            .write_file(
                &module_path,
                module_source.into_bytes(),
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| format!("write module fixture {module_path}: {error}"))?;
    }
    Ok(())
}

async fn write_package_scope_fixture_files(
    session: &terracedb_sandbox::SandboxSession,
    package_count: usize,
) -> Result<(), String> {
    for i in 0..package_count {
        let package_json_path = format!("/workspace/app/pkg-{i}/package.json");
        let package_json = format!(
            "{{\"name\":\"pkg-{i}\",\"type\":\"commonjs\",\"main\":\"./subdir/file.js\"}}\n"
        );
        session
            .filesystem()
            .write_file(
                &package_json_path,
                package_json.into_bytes(),
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| format!("write package fixture {package_json_path}: {error}"))?;
        let module_path = format!("/workspace/app/pkg-{i}/subdir/file.js");
        let module_source = format!("module.exports = \"pkg-{i}\";\n");
        session
            .filesystem()
            .write_file(
                &module_path,
                module_source.into_bytes(),
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| format!("write module fixture {module_path}: {error}"))?;
    }
    Ok(())
}

async fn write_node_modules_package_fixtures(
    session: &terracedb_sandbox::SandboxSession,
    package_count: usize,
    payload_len: usize,
) -> Result<(), String> {
    for i in 0..package_count {
        let package_json_path = format!("/workspace/app/node_modules/pkg-{i}/package.json");
        let package_json =
            format!("{{\"name\":\"pkg-{i}\",\"type\":\"commonjs\",\"main\":\"./index.js\"}}\n");
        session
            .filesystem()
            .write_file(
                &package_json_path,
                package_json.into_bytes(),
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| format!("write package fixture {package_json_path}: {error}"))?;
        let module_path = format!("/workspace/app/node_modules/pkg-{i}/index.js");
        let module_source = format!(
            "module.exports = {{ name: \"pkg-{i}\", payload: \"x\".repeat({payload_len}) }};\n"
        );
        session
            .filesystem()
            .write_file(
                &module_path,
                module_source.into_bytes(),
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| format!("write module fixture {module_path}: {error}"))?;
    }
    Ok(())
}

fn run_graceful_fs_simulation(seed: u64) -> turmoil::Result<NodeCompatSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |_context| async move {
            let result = node_compat_support::exec_node_fixture_with_seed(
                610 + seed,
                221 + seed,
                node_compat_support::graceful_fs_repro_source(),
            )
            .await;
            let result = match result {
                Ok(value) => value,
                Err(error) => return Err(error.to_string().into()),
            };
            let report = result.result.clone();
            let report = match report {
                Some(value) => value,
                None => return Err("missing node command report".into()),
            };
            Ok(NodeCompatSimulationCapture {
                stdout: report["stdout"].as_str().unwrap_or_default().to_string(),
                exit_code: report["exitCode"].as_i64().unwrap_or_default(),
                module_count: result.module_graph.len(),
            })
        })
}

fn run_next_tick_order_simulation(seed: u64) -> turmoil::Result<NodeCompatSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |_context| async move {
            let result = node_compat_support::exec_node_fixture_with_seed(
                810 + seed,
                421 + seed,
                r#"
                const out = [];
                process.nextTick(() => out.push("nextTick"));
                Promise.resolve().then(() => out.push("promise"));
                queueMicrotask(() => out.push("queueMicrotask"));
                queueMicrotask(() => console.log(out.join(",")));
                out.push("sync");
                "#,
            )
            .await;
            let result = match result {
                Ok(value) => value,
                Err(error) => return Err(error.to_string().into()),
            };
            let report = match result.result.clone() {
                Some(value) => value,
                None => return Err("missing node command report".into()),
            };
            Ok(NodeCompatSimulationCapture {
                stdout: report["stdout"].as_str().unwrap_or_default().to_string(),
                exit_code: report["exitCode"].as_i64().unwrap_or_default(),
                module_count: result.module_graph.len(),
            })
        })
}

fn run_node_memory_tracking_simulation(
    seed: u64,
) -> turmoil::Result<NodeCompatMemorySimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |_context| async move {
            let result = node_compat_support::exec_node_fixture_with_seed(
                910 + seed,
                521 + seed,
                r#"
                const { URLPattern } = require("url");
                const path = require("path");

                globalThis.pattern = new URLPattern({ pathname: "/:id" });
                globalThis.buffer = Buffer.from(path.join("memory", "sim"));
                console.log(globalThis.buffer.toString());
                "#,
            )
            .await;
            let result = match result {
                Ok(value) => value,
                Err(error) => return Err(error.to_string().into()),
            };
            let report = match result.result.clone() {
                Some(value) => value,
                None => return Err("missing node command report".into()),
            };
            let managed = match result.metadata.get("node_runtime_managed_memory") {
                Some(JsonValue::Object(value)) => value,
                _ => return Err("missing node runtime managed memory snapshot".into()),
            };
            let read_bucket = |key: &str| {
                managed
                    .get(key)
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| format!("missing managed memory bucket {key}"))
            };
            let context_runtime_bytes = match read_bucket("context_runtime_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let task_queue_bytes = match read_bucket("task_queue_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let compiled_code_bytes = match read_bucket("compiled_code_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let parser_retained_bytes = match read_bucket("parser_retained_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let module_cache_bytes = match read_bucket("module_cache_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let host_buffer_bytes = match read_bucket("host_buffer_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let node_compat_state_bytes = match read_bucket("node_compat_state_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            Ok(NodeCompatMemorySimulationCapture {
                exit_code: report["exitCode"].as_i64().unwrap_or_default(),
                managed_bytes: result
                    .metadata
                    .get("node_runtime_managed_bytes")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or_default(),
                gc_bytes: result
                    .metadata
                    .get("node_runtime_gc_bytes_allocated")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or_default(),
                accounted_bytes: result
                    .metadata
                    .get("node_runtime_accounted_bytes")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or_default(),
                context_runtime_bytes,
                task_queue_bytes,
                compiled_code_bytes,
                parser_retained_bytes,
                module_cache_bytes,
                host_buffer_bytes,
                node_compat_state_bytes,
            })
        })
}

fn run_node_allocator_budget_failure_simulation(
    seed: u64,
    timeout: Duration,
    diagnostics: Arc<StdMutex<SingleCaseSimulationDiagnostics>>,
) -> SeededSimulationExecution<NodeCompatBudgetFailureCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .prepare_run(move |_context| async move {
            let entrypoint = "/workspace/app/index.cjs";
            let session = node_compat_support::open_node_session(
                1010 + seed,
                641 + seed,
                entrypoint,
                r#"
                const values = [];
                for (let i = 0; i < 20000; i += 1) {
                  values.push({ index: i, payload: "x".repeat(64) });
                }
                console.log(values.length);
                "#,
            )
            .await;
            diagnostics
                .lock()
                .expect("allocator-budget diagnostics")
                .runtime_state = Some(session.runtime_state_handle());

            let resource_manager = std::sync::Arc::new(InMemoryResourceManager::new(
                ExecutionDomainBudget::default(),
            ));
            let domain_path =
                ExecutionDomainPath::new(["process", "sandbox-tests", "allocator-budget"]);
            resource_manager.register_domain(ExecutionDomainSpec {
                path: domain_path.clone(),
                owner: ExecutionDomainOwner::Subsystem {
                    database: None,
                    name: "allocator-budget".to_string(),
                },
                budget: ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(1),
                        weight: Some(1),
                    },
                    memory: terracedb::DomainMemoryBudget {
                        total_bytes: Some(256 * 1024),
                        cache_bytes: None,
                        mutable_bytes: None,
                    },
                    ..Default::default()
                },
                placement: ExecutionDomainPlacement::Dedicated,
                metadata: Default::default(),
            });
            let usage_handle = ExecutionUsageHandle::acquire(
                resource_manager,
                domain_path,
                ExecutionResourceUsage {
                    cpu_workers: 1,
                    ..Default::default()
                },
            );
            if !usage_handle.admitted() {
                return Err("allocator-budget domain was not admitted".into());
            }
            let budget = std::sync::Arc::new(SandboxBatchedDomainMemoryBudget::new(
                usage_handle,
                SandboxTrackedMemoryBudgetPolicy {
                    budget_bytes: None,
                    allocation_batch_bytes: 8 * 1024,
                },
            ));
            session.set_runtime_memory_budget(budget.clone());

            match exec_node_command_with_case_timeout(&session, entrypoint, "/workspace/app", timeout)
                .await
            {
                Ok(result) => Err(format!(
                    "expected budget failure, got success: snapshot={:#?} result={result:#?}",
                    budget.snapshot()
                )
                .into()),
                Err(error) => {
                    let snapshot = budget.snapshot();
                    Ok(NodeCompatBudgetFailureCapture {
                        message: error.to_string(),
                        charged_bytes: snapshot.charged_bytes,
                        current_bytes: snapshot.current.total_bytes,
                        gc_bytes: snapshot.current.gc_heap_bytes,
                        batch_requests: snapshot.batch_requests,
                        terminated_for_budget: snapshot.terminated_for_budget,
                    })
                }
            }
        })
}

fn run_node_process_state_budget_failure_simulation(
    seed: u64,
    timeout: Duration,
    diagnostics: Arc<StdMutex<SingleCaseSimulationDiagnostics>>,
) -> SeededSimulationExecution<NodeCompatProcessStateBudgetFailureCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .prepare_run(move |context| async move {
            diagnostics
                .lock()
                .expect("process-state diagnostics")
                .simulation_context = Some(context.clone());
            let entrypoint = "/workspace/app/index.cjs";
            let source = r#"
                const stderrChunk = "e".repeat(8 * 1024);
                const stdoutChunk = "o".repeat(8 * 1024);
                process.title = "worker-" + "t".repeat(64 * 1024);
                for (let i = 0; i < 16; i += 1) {
                  process.stderr.write(stderrChunk);
                }
                for (let i = 0; i < 16; i += 1) {
                  process.stdout.write(stdoutChunk);
                }
                console.log("done");
                "#;
            let budget_bytes = 128 * 1024;

            let session = node_compat_support::open_node_session(
                1210 + seed,
                761 + seed,
                entrypoint,
                source,
            )
            .await;
            diagnostics
                .lock()
                .expect("process-state diagnostics")
                .runtime_state = Some(session.runtime_state_handle());
            context.checkpoint("process-state:budgeted-session:opened", Default::default());

            let resource_manager = std::sync::Arc::new(InMemoryResourceManager::new(
                ExecutionDomainBudget::default(),
            ));
            let domain_path =
                ExecutionDomainPath::new(["process", "sandbox-tests", "process-state-budget"]);
            resource_manager.register_domain(ExecutionDomainSpec {
                path: domain_path.clone(),
                owner: ExecutionDomainOwner::Subsystem {
                    database: None,
                    name: "process-state-budget".to_string(),
                },
                budget: ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(1),
                        weight: Some(1),
                    },
                    memory: terracedb::DomainMemoryBudget {
                        total_bytes: Some(budget_bytes),
                        cache_bytes: None,
                        mutable_bytes: None,
                    },
                    ..Default::default()
                },
                placement: ExecutionDomainPlacement::Dedicated,
                metadata: Default::default(),
            });
            let usage_handle = ExecutionUsageHandle::acquire(
                resource_manager,
                domain_path,
                ExecutionResourceUsage {
                    cpu_workers: 1,
                    ..Default::default()
                },
            );
            if !usage_handle.admitted() {
                return Err("process-state-budget domain was not admitted".into());
            }
            let budget = std::sync::Arc::new(SandboxBatchedDomainMemoryBudget::new(
                usage_handle,
                SandboxTrackedMemoryBudgetPolicy {
                    budget_bytes: None,
                    allocation_batch_bytes: 4 * 1024,
                },
            ));
            session.set_runtime_memory_budget(budget.clone());
            context.checkpoint(
                "process-state:budgeted-session:exec:start",
                std::collections::BTreeMap::from([(
                    "budget_bytes".to_string(),
                    budget_bytes.to_string(),
                )]),
            );

            match exec_node_command_with_case_timeout(&session, entrypoint, "/workspace/app", timeout)
                .await
            {
                Ok(result) => {
                    context.checkpoint(
                        "process-state:budgeted-session:exec:unexpected-success",
                        Default::default(),
                    );
                    session
                        .close(terracedb_sandbox::CloseSessionOptions::default())
                        .await
                        .map_err(|error| format!("process-state success close failed: {error}"))?;
                    Err(format!(
                        "expected budget failure, got success: snapshot={:#?} result={result:#?}",
                        budget.snapshot()
                    )
                    .into())
                }
                Err(error) => {
                    context.checkpoint("process-state:budgeted-session:exec:error", Default::default());
                    let snapshot = budget.snapshot();
                    context.checkpoint("process-state:budgeted-session:close:start", Default::default());
                    session
                        .close(terracedb_sandbox::CloseSessionOptions::default())
                        .await
                        .map_err(|close_error| {
                            format!("process-state error close failed after `{error}`: {close_error}")
                        })?;
                    context.checkpoint("process-state:budgeted-session:close:done", Default::default());
                    Ok(NodeCompatProcessStateBudgetFailureCapture {
                        message: error.to_string(),
                        budget_bytes,
                        charged_bytes: snapshot.charged_bytes,
                        current_bytes: snapshot.current.total_bytes,
                        gc_bytes: snapshot.current.gc_heap_bytes,
                        host_buffer_bytes: snapshot.current.host_buffer_bytes,
                        node_compat_state_bytes: snapshot.current.node_compat_state_bytes,
                        batch_requests: snapshot.batch_requests,
                        terminated_for_budget: snapshot.terminated_for_budget,
                    })
                }
            }
        })
}

fn run_node_task_queue_budget_failure_simulation(
    seed: u64,
    timeout: Duration,
    diagnostics: Arc<StdMutex<SingleCaseSimulationDiagnostics>>,
) -> SeededSimulationExecution<NodeCompatTaskQueueBudgetFailureCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .prepare_run(move |_context| async move {
            let entrypoint = "/workspace/app/index.cjs";
            let baseline = node_compat_support::open_node_session(
                1310 + seed,
                841 + seed,
                entrypoint,
                r#"
                console.log("baseline");
                "#,
            )
            .await;
            diagnostics
                .lock()
                .expect("task-queue diagnostics")
                .runtime_state = Some(baseline.runtime_state_handle());
            let baseline_result = exec_node_command_with_case_timeout(
                &baseline,
                entrypoint,
                "/workspace/app",
                timeout,
            )
                .await
                .map_err(|error| format!("task-queue baseline failed: {error}"))?;
            let baseline_accounted_bytes =
                metadata_u64(&baseline_result.metadata, "node_runtime_accounted_bytes")?;
            let source = r#"
                const callback = () => {};
                for (let i = 0; i < 100000; i += 1) {
                  queueMicrotask(callback);
                }
                console.log("queued");
                "#;
            let budget_bytes = baseline_accounted_bytes.saturating_add(128 * 1024);

            let session =
                node_compat_support::open_node_session(1410 + seed, 941 + seed, entrypoint, source)
                    .await;
            diagnostics
                .lock()
                .expect("task-queue diagnostics")
                .runtime_state = Some(session.runtime_state_handle());

            let resource_manager = std::sync::Arc::new(InMemoryResourceManager::new(
                ExecutionDomainBudget::default(),
            ));
            let domain_path =
                ExecutionDomainPath::new(["process", "sandbox-tests", "task-queue-budget"]);
            resource_manager.register_domain(ExecutionDomainSpec {
                path: domain_path.clone(),
                owner: ExecutionDomainOwner::Subsystem {
                    database: None,
                    name: "task-queue-budget".to_string(),
                },
                budget: ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(1),
                        weight: Some(1),
                    },
                    memory: terracedb::DomainMemoryBudget {
                        total_bytes: Some(budget_bytes),
                        cache_bytes: None,
                        mutable_bytes: None,
                    },
                    ..Default::default()
                },
                placement: ExecutionDomainPlacement::Dedicated,
                metadata: Default::default(),
            });
            let usage_handle = ExecutionUsageHandle::acquire(
                resource_manager,
                domain_path,
                ExecutionResourceUsage {
                    cpu_workers: 1,
                    ..Default::default()
                },
            );
            if !usage_handle.admitted() {
                return Err("task-queue-budget domain was not admitted".into());
            }
            let budget = std::sync::Arc::new(SandboxBatchedDomainMemoryBudget::new(
                usage_handle,
                SandboxTrackedMemoryBudgetPolicy {
                    budget_bytes: None,
                    allocation_batch_bytes: 4 * 1024,
                },
            ));
            session.set_runtime_memory_budget(budget.clone());

            match exec_node_command_with_case_timeout(&session, entrypoint, "/workspace/app", timeout)
                .await
            {
                Ok(result) => {
                    Err(format!("expected budget failure, got success: {result:#?}").into())
                }
                Err(error) => {
                    let snapshot = budget.snapshot();
                    Ok(NodeCompatTaskQueueBudgetFailureCapture {
                        message: error.to_string(),
                        charged_bytes: snapshot.charged_bytes,
                        current_bytes: snapshot.current.total_bytes,
                        gc_bytes: snapshot.current.gc_heap_bytes,
                        task_queue_bytes: snapshot.current.task_queue_bytes,
                        batch_requests: snapshot.batch_requests,
                        terminated_for_budget: snapshot.terminated_for_budget,
                    })
                }
            }
        })
}

fn run_node_module_cache_budget_failure_simulation(
    seed: u64,
    timeout: Duration,
    diagnostics: Arc<StdMutex<SingleCaseSimulationDiagnostics>>,
) -> SeededSimulationExecution<NodeCompatModuleCacheBudgetFailureCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .prepare_run(move |_context| async move {
            let entrypoint = "/workspace/app/index.cjs";
            let package_count = 2usize;
            let payload_len = 128 * 1024usize;
            let budget_headroom_bytes = 8 * 1024u64;
            let baseline_source = r#"
                console.log("baseline");
                "#;
            let module_entry_source = format!(
                r#"
                const loaded = [];
                for (let i = 0; i < {package_count}; i += 1) {{
                  const value = require(`pkg-${{i}}`);
                  if (!value || value.name !== `pkg-${{i}}` || typeof value.payload !== "string") {{
                    throw new Error(`unexpected module export for pkg-${{i}}: ${{value}}`);
                  }}
                  loaded.push(value);
                }}
                console.log(loaded.length, loaded[0]?.payload.length ?? 0);
                "#
            );
            let source = module_entry_source.clone();
            let baseline = node_compat_support::open_node_session(
                1500 + seed,
                931 + seed,
                entrypoint,
                baseline_source,
            )
            .await;
            let baseline_result = exec_node_command_with_case_timeout(
                &baseline,
                entrypoint,
                "/workspace/app",
                timeout,
            )
                .await
                .map_err(|error| format!("module-cache baseline failed: {error}"))?;
            let baseline_accounted_bytes =
                metadata_u64(&baseline_result.metadata, "node_runtime_accounted_bytes")?;
            let budget_bytes = baseline_accounted_bytes.saturating_add(budget_headroom_bytes);

            let session = node_compat_support::open_node_session(
                1610 + seed,
                1041 + seed,
                entrypoint,
                &source,
            )
            .await;
            diagnostics
                .lock()
                .expect("module-cache diagnostics")
                .runtime_state = Some(session.runtime_state_handle());
            write_node_modules_package_fixtures(&session, package_count, payload_len).await?;

            let resource_manager = std::sync::Arc::new(InMemoryResourceManager::new(
                ExecutionDomainBudget::default(),
            ));
            let domain_path =
                ExecutionDomainPath::new(["process", "sandbox-tests", "module-cache-budget"]);
            resource_manager.register_domain(ExecutionDomainSpec {
                path: domain_path.clone(),
                owner: ExecutionDomainOwner::Subsystem {
                    database: None,
                    name: "module-cache-budget".to_string(),
                },
                budget: ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(1),
                        weight: Some(1),
                    },
                    memory: terracedb::DomainMemoryBudget {
                        total_bytes: Some(budget_bytes),
                        cache_bytes: None,
                        mutable_bytes: None,
                    },
                    ..Default::default()
                },
                placement: ExecutionDomainPlacement::Dedicated,
                metadata: Default::default(),
            });
            let usage_handle = ExecutionUsageHandle::acquire(
                resource_manager,
                domain_path,
                ExecutionResourceUsage {
                    cpu_workers: 1,
                    ..Default::default()
                },
            );
            if !usage_handle.admitted() {
                return Err("module-cache-budget domain was not admitted".into());
            }
            let budget = std::sync::Arc::new(SandboxBatchedDomainMemoryBudget::new(
                usage_handle,
                SandboxTrackedMemoryBudgetPolicy {
                    budget_bytes: None,
                    allocation_batch_bytes: 8 * 1024,
                },
            ));
            session.set_runtime_memory_budget(budget.clone());

            match exec_node_command_with_case_timeout(&session, entrypoint, "/workspace/app", timeout)
                .await
            {
                Ok(result) => Err(format!(
                    "expected budget failure, got success: snapshot={:#?} result={result:#?}",
                    budget.snapshot()
                )
                .into()),
                Err(error) => {
                    let snapshot = budget.snapshot();
                    Ok(NodeCompatModuleCacheBudgetFailureCapture {
                        message: error.to_string(),
                        baseline_accounted_bytes,
                        budget_bytes,
                        charged_bytes: snapshot.charged_bytes,
                        current_bytes: snapshot.current.total_bytes,
                        gc_bytes: snapshot.current.gc_heap_bytes,
                        module_cache_bytes: snapshot.current.module_cache_bytes,
                        batch_requests: snapshot.batch_requests,
                        terminated_for_budget: snapshot.terminated_for_budget,
                    })
                }
            }
        })
}

#[test]
fn seeded_graceful_fs_repro_keeps_executing_under_simulation() -> turmoil::Result<()> {
    let capture = run_graceful_fs_simulation(771)?;
    assert_eq!(capture.exit_code, 0, "{capture:#?}");
    assert!(
        capture.stdout.contains("before-require"),
        "expected pre-require marker, got: {capture:#?}"
    );
    assert!(
        capture.stdout.contains("after-gracefulify:function"),
        "expected gracefulify marker, got: {capture:#?}"
    );
    assert!(
        capture.stdout.contains("after-write"),
        "expected post-write marker, got: {capture:#?}"
    );
    assert!(capture.module_count >= 4, "{capture:#?}");
    Ok(())
}

#[test]
fn seeded_next_tick_order_stays_stable_under_simulation() -> turmoil::Result<()> {
    let capture = run_next_tick_order_simulation(913)?;
    assert_eq!(capture.exit_code, 0, "{capture:#?}");
    assert!(
        capture
            .stdout
            .contains("sync,nextTick,promise,queueMicrotask"),
        "expected Node-style nextTick ordering, got: {capture:#?}"
    );
    Ok(())
}

#[test]
fn seeded_node_memory_tracking_reports_named_buckets() -> turmoil::Result<()> {
    let capture = run_node_memory_tracking_simulation(977)?;
    assert_eq!(capture.exit_code, 0, "{capture:#?}");
    assert_eq!(
        capture.accounted_bytes,
        capture.managed_bytes + capture.gc_bytes,
        "{capture:#?}"
    );
    assert_eq!(
        capture.managed_bytes,
        capture.context_runtime_bytes
            + capture.task_queue_bytes
            + capture.compiled_code_bytes
            + capture.parser_retained_bytes
            + capture.module_cache_bytes
            + capture.host_buffer_bytes
            + capture.node_compat_state_bytes,
        "{capture:#?}"
    );
    assert!(capture.gc_bytes > 0, "{capture:#?}");
    assert!(capture.compiled_code_bytes > 0, "{capture:#?}");
    assert!(capture.node_compat_state_bytes > 0, "{capture:#?}");
    Ok(())
}

#[test]
fn batched_memory_budget_only_grows_on_batch_boundaries() {
    let resource_manager = std::sync::Arc::new(InMemoryResourceManager::new(
        ExecutionDomainBudget::default(),
    ));
    let domain_path = ExecutionDomainPath::new(["process", "sandbox-tests", "batched-memory"]);
    resource_manager.register_domain(ExecutionDomainSpec {
        path: domain_path.clone(),
        owner: ExecutionDomainOwner::Subsystem {
            database: None,
            name: "batched-memory".to_string(),
        },
        budget: ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(1),
                weight: Some(1),
            },
            memory: terracedb::DomainMemoryBudget {
                total_bytes: Some(256 * 1024),
                cache_bytes: None,
                mutable_bytes: None,
            },
            ..Default::default()
        },
        placement: ExecutionDomainPlacement::Dedicated,
        metadata: Default::default(),
    });
    let usage_handle = ExecutionUsageHandle::acquire(
        resource_manager,
        domain_path,
        ExecutionResourceUsage {
            cpu_workers: 1,
            ..Default::default()
        },
    );
    assert!(usage_handle.admitted());
    let budget = SandboxBatchedDomainMemoryBudget::new(
        usage_handle,
        SandboxTrackedMemoryBudgetPolicy {
            budget_bytes: Some(256 * 1024),
            allocation_batch_bytes: 8 * 1024,
        },
    );
    budget
        .update_tracked_memory_usage(terracedb_sandbox::SandboxTrackedMemoryUsage {
            total_bytes: 1_000,
            ..Default::default()
        })
        .expect("first batch request");
    let first = budget.snapshot();
    assert_eq!(first.charged_bytes, 8 * 1024, "{first:#?}");
    assert_eq!(first.batch_requests, 1, "{first:#?}");

    budget
        .update_tracked_memory_usage(terracedb_sandbox::SandboxTrackedMemoryUsage {
            total_bytes: 2_000,
            ..Default::default()
        })
        .expect("same batch update");
    let second = budget.snapshot();
    assert_eq!(second.charged_bytes, 8 * 1024, "{second:#?}");
    assert_eq!(second.batch_requests, 1, "{second:#?}");

    budget
        .update_tracked_memory_usage(terracedb_sandbox::SandboxTrackedMemoryUsage {
            total_bytes: 9_000,
            ..Default::default()
        })
        .expect("second batch request");
    let third = budget.snapshot();
    assert_eq!(third.charged_bytes, 16 * 1024, "{third:#?}");
    assert_eq!(third.batch_requests, 2, "{third:#?}");
}

#[test]
fn seeded_node_budget_failure_happens_from_allocator_accounting() -> turmoil::Result<()> {
    let capture = run_simulation_case_with_harness(
        "allocator-budget",
        "allocator budget",
        991,
        DOMAIN_SIMULATION_CASE_TIMEOUT,
        run_node_allocator_budget_failure_simulation,
    )?;
    assert!(
        capture
            .message
            .contains("domain denied tracked memory batch"),
        "{capture:#?}"
    );
    assert!(capture.gc_bytes > 0, "{capture:#?}");
    assert!(capture.current_bytes > 0, "{capture:#?}");
    assert!(capture.batch_requests > 0, "{capture:#?}");
    assert!(
        capture.current_bytes >= capture.charged_bytes,
        "{capture:#?}"
    );
    assert!(
        capture.current_bytes - capture.charged_bytes <= 8 * 1024,
        "{capture:#?}"
    );
    assert!(capture.terminated_for_budget, "{capture:#?}");
    Ok(())
}

#[test]
fn seeded_node_budget_failure_happens_from_task_queue_accounting() -> turmoil::Result<()> {
    let capture = run_simulation_case_with_harness(
        "task-queue-budget",
        "task queue budget",
        995,
        DOMAIN_SIMULATION_CASE_TIMEOUT,
        run_node_task_queue_budget_failure_simulation,
    )?;
    assert!(
        capture
            .message
            .contains("domain denied tracked memory batch"),
        "{capture:#?}"
    );
    assert!(capture.task_queue_bytes > 0, "{capture:#?}");
    assert!(capture.batch_requests > 0, "{capture:#?}");
    assert!(
        capture.current_bytes >= capture.charged_bytes,
        "{capture:#?}"
    );
    assert!(
        capture.current_bytes - capture.charged_bytes <= 8 * 1024,
        "{capture:#?}"
    );
    assert!(capture.task_queue_bytes >= 64 * 1024, "{capture:#?}");
    assert!(capture.terminated_for_budget, "{capture:#?}");
    Ok(())
}

#[test]
fn seeded_node_budget_failure_happens_from_module_cache_accounting() -> turmoil::Result<()> {
    let capture = run_simulation_case_with_harness(
        "module-cache-budget",
        "module cache budget",
        996,
        DOMAIN_SIMULATION_CASE_TIMEOUT,
        run_node_module_cache_budget_failure_simulation,
    )?;
    assert!(
        capture
            .message
            .contains("domain denied tracked memory batch"),
        "{capture:#?}"
    );
    assert!(capture.gc_bytes > 0, "{capture:#?}");
    assert!(capture.module_cache_bytes > 0, "{capture:#?}");
    assert!(capture.batch_requests > 0, "{capture:#?}");
    assert!(
        capture.current_bytes > capture.baseline_accounted_bytes,
        "{capture:#?}"
    );
    assert!(
        capture.charged_bytes >= capture.budget_bytes,
        "{capture:#?}"
    );
    assert!(
        capture.current_bytes >= capture.charged_bytes,
        "{capture:#?}"
    );
    assert!(
        capture.current_bytes - capture.charged_bytes <= 8 * 1024,
        "{capture:#?}"
    );
    assert!(capture.terminated_for_budget, "{capture:#?}");
    Ok(())
}

#[test]
fn seeded_node_budget_failure_happens_from_process_state_accounting() -> turmoil::Result<()> {
    let capture = run_simulation_case_with_harness(
        "process-state-budget",
        "process state budget",
        997,
        DOMAIN_SIMULATION_CASE_TIMEOUT,
        run_node_process_state_budget_failure_simulation,
    )?;
    assert!(
        capture
            .message
            .contains("domain denied tracked memory batch"),
        "{capture:#?}"
    );
    assert!(capture.node_compat_state_bytes > 0, "{capture:#?}");
    assert!(capture.current_bytes > capture.budget_bytes, "{capture:#?}");
    assert!(
        capture.host_buffer_bytes + capture.node_compat_state_bytes >= 64 * 1024,
        "{capture:#?}"
    );
    assert!(capture.terminated_for_budget, "{capture:#?}");
    Ok(())
}
