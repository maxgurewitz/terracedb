use std::{
    collections::VecDeque,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex as StdMutex},
    thread,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::FutureExt as _;
use terracedb::{
    ExecutionDomainBudget, ExecutionDomainPath, ExecutionDomainSpec, ExecutionResourceKind,
    ExecutionResourceUsage, ExecutionUsageHandle, ResourceManager,
};
use thiserror::Error;
use tokio::{runtime::Builder, time::timeout};

#[derive(Clone, Debug)]
pub struct SimulationCaseSpec<C> {
    pub case_id: String,
    pub label: String,
    pub input: C,
    pub timeout: Duration,
    pub requested_usage: Option<ExecutionResourceUsage>,
}

impl<C> SimulationCaseSpec<C> {
    pub fn new(
        case_id: impl Into<String>,
        label: impl Into<String>,
        input: C,
        timeout: Duration,
    ) -> Self {
        Self {
            case_id: case_id.into(),
            label: label.into(),
            input,
            timeout,
            requested_usage: None,
        }
    }

    pub fn with_requested_usage(mut self, requested_usage: ExecutionResourceUsage) -> Self {
        self.requested_usage = Some(requested_usage);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulationCaseStatus {
    Passed,
    Failed { message: String },
    TimedOut { after: Duration },
    Panicked { message: String },
}

#[derive(Clone, Debug)]
pub struct SimulationCaseReport {
    pub case_id: String,
    pub label: String,
    pub status: SimulationCaseStatus,
    pub elapsed: Duration,
}

impl SimulationCaseReport {
    pub fn is_success(&self) -> bool {
        matches!(self.status, SimulationCaseStatus::Passed)
    }
}

#[derive(Clone, Debug, Default)]
pub struct SimulationSuiteReport {
    pub cases: Vec<SimulationCaseReport>,
}

impl SimulationSuiteReport {
    pub fn total_cases(&self) -> usize {
        self.cases.len()
    }

    pub fn passed_cases(&self) -> usize {
        self.cases.iter().filter(|case| case.is_success()).count()
    }

    pub fn failed_cases(&self) -> usize {
        self.total_cases().saturating_sub(self.passed_cases())
    }

    pub fn is_success(&self) -> bool {
        self.failed_cases() == 0
    }

    pub fn summary_line(&self) -> String {
        format!(
            "{} passed / {} failed / {} total",
            self.passed_cases(),
            self.failed_cases(),
            self.total_cases()
        )
    }

    pub fn failure_summary(&self) -> String {
        let mut output = String::new();
        for case in self.cases.iter().filter(|case| !case.is_success()) {
            let status = match &case.status {
                SimulationCaseStatus::Passed => "passed".to_string(),
                SimulationCaseStatus::Failed { message } => {
                    format!("failed after {:?}: {}", case.elapsed, message)
                }
                SimulationCaseStatus::TimedOut { after } => {
                    format!("timed out after {:?}", after)
                }
                SimulationCaseStatus::Panicked { message } => {
                    format!("panicked after {:?}: {}", case.elapsed, message)
                }
            };
            output.push_str(&format!("[{}] {}: {}\n", case.case_id, case.label, status));
        }
        output
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimulationHarnessConfig {
    pub max_workers: usize,
}

impl Default for SimulationHarnessConfig {
    fn default() -> Self {
        Self {
            max_workers: thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(1),
        }
    }
}

#[derive(Clone)]
pub struct SimulationDomainConfig {
    pub resource_manager: Arc<dyn ResourceManager>,
    pub domain_spec: ExecutionDomainSpec,
    pub default_case_usage: ExecutionResourceUsage,
}

#[derive(Clone, Default)]
pub struct SimulationCaseContext {
    domain_usage: Option<ExecutionUsageHandle>,
}

impl SimulationCaseContext {
    fn with_domain_usage(domain_usage: Option<ExecutionUsageHandle>) -> Self {
        Self { domain_usage }
    }

    pub fn domain_usage(&self) -> Option<ExecutionUsageHandle> {
        self.domain_usage.clone()
    }
}

impl std::fmt::Debug for SimulationDomainConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulationDomainConfig")
            .field("domain_spec", &self.domain_spec)
            .field("default_case_usage", &self.default_case_usage)
            .finish()
    }
}

#[derive(Clone, Debug, Default)]
pub struct SimulationHarness {
    config: SimulationHarnessConfig,
    domain: Option<SimulationDomainConfig>,
}

impl SimulationHarness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_workers(mut self, max_workers: usize) -> Self {
        self.config.max_workers = max_workers.max(1);
        self
    }

    pub fn with_domain(mut self, domain: SimulationDomainConfig) -> Self {
        self.domain = Some(domain);
        self
    }

    pub fn config(&self) -> SimulationHarnessConfig {
        self.config
    }

    pub async fn run_suite<S>(
        &self,
        suite: Arc<S>,
    ) -> Result<SimulationSuiteReport, SimulationHarnessError>
    where
        S: SimulationSuiteDefinition,
    {
        if let Some(domain) = &self.domain {
            domain
                .resource_manager
                .register_domain(domain.domain_spec.clone());
        }
        let fixture = suite.prepare().await?;
        let cases = suite.cases(fixture.clone()).await?;
        if cases.is_empty() {
            return Ok(SimulationSuiteReport::default());
        }

        let queue = Arc::new(StdMutex::new(VecDeque::from(cases)));
        let results = Arc::new(StdMutex::new(Vec::new()));
        let worker_count = self.config.max_workers.min(queue_len(&queue).max(1));
        let mut workers = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
            let suite = suite.clone();
            let fixture = fixture.clone();
            let queue = queue.clone();
            let results = results.clone();
            let domain = self.domain.clone();
            workers.push(thread::spawn(move || {
                let runtime =
                    Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| SimulationHarnessError::Runtime {
                            message: error.to_string(),
                        })?;
                runtime.block_on(async move {
                    loop {
                        let Some(case) = pop_case(&queue)? else {
                            return Ok::<(), SimulationHarnessError>(());
                        };
                        let domain_usage = if let Some(domain) = &domain {
                            Some(
                                acquire_case_lease(
                                    domain,
                                    &case.case_id,
                                    case.requested_usage.unwrap_or(domain.default_case_usage),
                                )
                                .await?,
                            )
                        } else {
                            None
                        };
                        let started = Instant::now();
                        let status = match timeout(
                            case.timeout,
                            AssertUnwindSafe(suite.run_case(
                                fixture.clone(),
                                case.clone(),
                                SimulationCaseContext::with_domain_usage(domain_usage.clone()),
                            ))
                            .catch_unwind(),
                        )
                        .await
                        {
                            Ok(Ok(Ok(()))) => SimulationCaseStatus::Passed,
                            Ok(Ok(Err(error))) => SimulationCaseStatus::Failed {
                                message: error.to_string(),
                            },
                            Ok(Err(payload)) => SimulationCaseStatus::Panicked {
                                message: panic_message(payload.as_ref()),
                            },
                            Err(_) => SimulationCaseStatus::TimedOut {
                                after: case.timeout,
                            },
                        };
                        let report = SimulationCaseReport {
                            case_id: case.case_id,
                            label: case.label,
                            status,
                            elapsed: started.elapsed(),
                        };
                        push_report(&results, report)?;
                    }
                })
            }));
            let _ = worker_index;
        }

        for (worker_index, worker) in workers.into_iter().enumerate() {
            let thread_result =
                worker
                    .join()
                    .map_err(|payload| SimulationHarnessError::WorkerPanicked {
                        worker_index,
                        message: panic_message(payload.as_ref()),
                    })?;
            thread_result?;
        }

        let mut cases = results
            .lock()
            .map_err(|_| SimulationHarnessError::InternalState {
                message: "simulation results mutex poisoned".to_string(),
            })?
            .clone();
        cases.sort_by(|left, right| left.case_id.cmp(&right.case_id));
        Ok(SimulationSuiteReport { cases })
    }
}

#[async_trait(?Send)]
pub trait SimulationSuiteDefinition: Send + Sync + 'static {
    type Fixture: Send + Sync + 'static;
    type Case: Clone + Send + Sync + 'static;

    async fn prepare(&self) -> Result<Arc<Self::Fixture>, SimulationHarnessError>;

    async fn cases(
        &self,
        fixture: Arc<Self::Fixture>,
    ) -> Result<Vec<SimulationCaseSpec<Self::Case>>, SimulationHarnessError>;

    async fn run_case(
        &self,
        fixture: Arc<Self::Fixture>,
        case: SimulationCaseSpec<Self::Case>,
        ctx: SimulationCaseContext,
    ) -> Result<(), SimulationHarnessError>;
}

#[derive(Debug, Error)]
pub enum SimulationHarnessError {
    #[error("simulation suite setup failed: {message}")]
    Setup { message: String },
    #[error("simulation suite runtime error: {message}")]
    Runtime { message: String },
    #[error("simulation worker {worker_index} panicked: {message}")]
    WorkerPanicked {
        worker_index: usize,
        message: String,
    },
    #[error("simulation harness internal state error: {message}")]
    InternalState { message: String },
    #[error(
        "simulation case {case_id} cannot fit in execution domain {path}: requested {requested_usage:?}, blocked by {blocked_by:?}"
    )]
    DomainUnschedulable {
        case_id: String,
        path: ExecutionDomainPath,
        requested_usage: ExecutionResourceUsage,
        blocked_by: Vec<ExecutionResourceKind>,
    },
}

async fn acquire_case_lease(
    domain: &SimulationDomainConfig,
    case_id: &str,
    requested_usage: ExecutionResourceUsage,
) -> Result<ExecutionUsageHandle, SimulationHarnessError> {
    let mut subscription = domain.resource_manager.subscribe();
    loop {
        let lease = ExecutionUsageHandle::acquire(
            domain.resource_manager.clone(),
            domain.domain_spec.path.clone(),
            requested_usage,
        );
        if lease.admitted() {
            return Ok(lease);
        }
        if !budget_can_fit(&lease.decision().effective_budget, requested_usage) {
            return Err(SimulationHarnessError::DomainUnschedulable {
                case_id: case_id.to_string(),
                path: domain.domain_spec.path.clone(),
                requested_usage,
                blocked_by: lease.decision().blocked_by.clone(),
            });
        }
        subscription
            .changed()
            .await
            .map_err(|_| SimulationHarnessError::Runtime {
                message: format!(
                    "resource manager subscription closed while waiting for domain {}",
                    domain.domain_spec.path
                ),
            })?;
    }
}

fn budget_can_fit(budget: &ExecutionDomainBudget, usage: ExecutionResourceUsage) -> bool {
    metric_fits(
        budget.cpu.worker_slots.map(u64::from),
        u64::from(usage.cpu_workers),
    ) && metric_fits(budget.memory.total_bytes, usage.memory_bytes)
        && metric_fits(budget.memory.cache_bytes, usage.cache_bytes)
        && metric_fits(budget.memory.mutable_bytes, usage.mutable_bytes)
        && metric_fits(
            budget.io.local_concurrency.map(u64::from),
            u64::from(usage.local_io_concurrency),
        )
        && metric_fits(
            budget.io.local_bytes_per_second,
            usage.local_io_bytes_per_second,
        )
        && metric_fits(
            budget.io.remote_concurrency.map(u64::from),
            u64::from(usage.remote_io_concurrency),
        )
        && metric_fits(
            budget.io.remote_bytes_per_second,
            usage.remote_io_bytes_per_second,
        )
        && metric_fits(
            budget.background.task_slots.map(u64::from),
            u64::from(usage.background_tasks),
        )
        && metric_fits(
            budget.background.max_in_flight_bytes,
            usage.background_in_flight_bytes,
        )
}

fn metric_fits(limit: Option<u64>, requested: u64) -> bool {
    limit.is_none_or(|limit| requested <= limit)
}

fn queue_len<C>(queue: &Arc<StdMutex<VecDeque<SimulationCaseSpec<C>>>>) -> usize {
    queue.lock().map(|queue| queue.len()).unwrap_or_default()
}

fn pop_case<C>(
    queue: &Arc<StdMutex<VecDeque<SimulationCaseSpec<C>>>>,
) -> Result<Option<SimulationCaseSpec<C>>, SimulationHarnessError> {
    queue
        .lock()
        .map_err(|_| SimulationHarnessError::InternalState {
            message: "simulation case queue mutex poisoned".to_string(),
        })
        .map(|mut queue| queue.pop_front())
}

fn push_report(
    results: &Arc<StdMutex<Vec<SimulationCaseReport>>>,
    report: SimulationCaseReport,
) -> Result<(), SimulationHarnessError> {
    results
        .lock()
        .map_err(|_| SimulationHarnessError::InternalState {
            message: "simulation results mutex poisoned".to_string(),
        })?
        .push(report);
    Ok(())
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use terracedb::{
        DomainCpuBudget, ExecutionDomainBudget, ExecutionDomainOwner, ExecutionDomainPath,
        ExecutionDomainPlacement, ExecutionDomainSpec, ExecutionResourceUsage,
        InMemoryResourceManager,
    };

    use super::{
        SimulationCaseContext, SimulationCaseSpec, SimulationCaseStatus, SimulationDomainConfig,
        SimulationHarness, SimulationHarnessError, SimulationSuiteDefinition,
    };

    #[derive(Clone)]
    struct Fixture {
        prepare_calls: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
    }

    struct Suite {
        prepare_calls: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
    }

    #[async_trait(?Send)]
    impl SimulationSuiteDefinition for Suite {
        type Fixture = Fixture;
        type Case = &'static str;

        async fn prepare(&self) -> Result<Arc<Self::Fixture>, SimulationHarnessError> {
            self.prepare_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(Fixture {
                prepare_calls: self.prepare_calls.clone(),
                barrier: self.barrier.clone(),
            }))
        }

        async fn cases(
            &self,
            _fixture: Arc<Self::Fixture>,
        ) -> Result<Vec<SimulationCaseSpec<Self::Case>>, SimulationHarnessError> {
            Ok(vec![
                SimulationCaseSpec::new("alpha", "alpha", "alpha", Duration::from_millis(100)),
                SimulationCaseSpec::new("beta", "beta", "beta", Duration::from_millis(100)),
            ])
        }

        async fn run_case(
            &self,
            fixture: Arc<Self::Fixture>,
            case: SimulationCaseSpec<Self::Case>,
            _ctx: SimulationCaseContext,
        ) -> Result<(), SimulationHarnessError> {
            assert_eq!(fixture.prepare_calls.load(Ordering::SeqCst), 1);
            if case.input == "alpha" || case.input == "beta" {
                fixture.barrier.wait();
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn harness_prepares_once_and_runs_cases_in_parallel() {
        let suite = Arc::new(Suite {
            prepare_calls: Arc::new(AtomicUsize::new(0)),
            barrier: Arc::new(Barrier::new(2)),
        });
        let report = SimulationHarness::new()
            .with_max_workers(2)
            .run_suite(suite.clone())
            .await
            .expect("run suite");
        assert!(report.is_success());
        assert_eq!(report.total_cases(), 2);
        assert_eq!(suite.prepare_calls.load(Ordering::SeqCst), 1);
    }

    struct TimeoutSuite;

    #[async_trait(?Send)]
    impl SimulationSuiteDefinition for TimeoutSuite {
        type Fixture = ();
        type Case = ();

        async fn prepare(&self) -> Result<Arc<Self::Fixture>, SimulationHarnessError> {
            Ok(Arc::new(()))
        }

        async fn cases(
            &self,
            _fixture: Arc<Self::Fixture>,
        ) -> Result<Vec<SimulationCaseSpec<Self::Case>>, SimulationHarnessError> {
            Ok(vec![SimulationCaseSpec::new(
                "timeout",
                "timeout",
                (),
                Duration::from_millis(10),
            )])
        }

        async fn run_case(
            &self,
            _fixture: Arc<Self::Fixture>,
            _case: SimulationCaseSpec<Self::Case>,
            _ctx: SimulationCaseContext,
        ) -> Result<(), SimulationHarnessError> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn harness_enforces_per_case_timeouts() {
        let report = SimulationHarness::new()
            .with_max_workers(1)
            .run_suite(Arc::new(TimeoutSuite))
            .await
            .expect("run timeout suite");
        assert_eq!(report.failed_cases(), 1);
        assert!(matches!(
            report.cases[0].status,
            SimulationCaseStatus::TimedOut { .. }
        ));
    }

    #[derive(Clone)]
    struct DomainFixture {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    struct DomainSuite {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    #[async_trait(?Send)]
    impl SimulationSuiteDefinition for DomainSuite {
        type Fixture = DomainFixture;
        type Case = &'static str;

        async fn prepare(&self) -> Result<Arc<Self::Fixture>, SimulationHarnessError> {
            Ok(Arc::new(DomainFixture {
                active: self.active.clone(),
                max_active: self.max_active.clone(),
            }))
        }

        async fn cases(
            &self,
            _fixture: Arc<Self::Fixture>,
        ) -> Result<Vec<SimulationCaseSpec<Self::Case>>, SimulationHarnessError> {
            Ok(vec![
                SimulationCaseSpec::new("one", "one", "one", Duration::from_secs(1)),
                SimulationCaseSpec::new("two", "two", "two", Duration::from_secs(1)),
                SimulationCaseSpec::new("three", "three", "three", Duration::from_secs(1)),
            ])
        }

        async fn run_case(
            &self,
            fixture: Arc<Self::Fixture>,
            _case: SimulationCaseSpec<Self::Case>,
            _ctx: SimulationCaseContext,
        ) -> Result<(), SimulationHarnessError> {
            let current = fixture.active.fetch_add(1, Ordering::SeqCst) + 1;
            loop {
                let observed = fixture.max_active.load(Ordering::SeqCst);
                if current <= observed {
                    break;
                }
                if fixture
                    .max_active
                    .compare_exchange(observed, current, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            fixture.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn harness_respects_execution_domain_cpu_budget() {
        let resource_manager = Arc::new(InMemoryResourceManager::new(
            ExecutionDomainBudget::default(),
        ));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let report = SimulationHarness::new()
            .with_max_workers(3)
            .with_domain(SimulationDomainConfig {
                resource_manager,
                domain_spec: ExecutionDomainSpec {
                    path: ExecutionDomainPath::new(["process", "systemtest", "cpu-serial"]),
                    owner: ExecutionDomainOwner::Subsystem {
                        database: None,
                        name: "systemtest".to_string(),
                    },
                    budget: ExecutionDomainBudget {
                        cpu: DomainCpuBudget {
                            worker_slots: Some(1),
                            weight: Some(1),
                        },
                        ..Default::default()
                    },
                    placement: ExecutionDomainPlacement::Dedicated,
                    metadata: Default::default(),
                },
                default_case_usage: ExecutionResourceUsage {
                    cpu_workers: 1,
                    ..Default::default()
                },
            })
            .run_suite(Arc::new(DomainSuite {
                active: active.clone(),
                max_active: max_active.clone(),
            }))
            .await
            .expect("run domain-limited suite");
        assert!(report.is_success());
        assert_eq!(report.total_cases(), 3);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    struct UnschedulableSuite;

    #[async_trait(?Send)]
    impl SimulationSuiteDefinition for UnschedulableSuite {
        type Fixture = ();
        type Case = ();

        async fn prepare(&self) -> Result<Arc<Self::Fixture>, SimulationHarnessError> {
            Ok(Arc::new(()))
        }

        async fn cases(
            &self,
            _fixture: Arc<Self::Fixture>,
        ) -> Result<Vec<SimulationCaseSpec<Self::Case>>, SimulationHarnessError> {
            Ok(vec![
                SimulationCaseSpec::new("too-big", "too-big", (), Duration::from_secs(1))
                    .with_requested_usage(ExecutionResourceUsage {
                        cpu_workers: 2,
                        ..Default::default()
                    }),
            ])
        }

        async fn run_case(
            &self,
            _fixture: Arc<Self::Fixture>,
            _case: SimulationCaseSpec<Self::Case>,
            _ctx: SimulationCaseContext,
        ) -> Result<(), SimulationHarnessError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn harness_fails_fast_for_unschedulable_case_usage() {
        let resource_manager = Arc::new(InMemoryResourceManager::new(
            ExecutionDomainBudget::default(),
        ));
        let error = SimulationHarness::new()
            .with_domain(SimulationDomainConfig {
                resource_manager,
                domain_spec: ExecutionDomainSpec {
                    path: ExecutionDomainPath::new(["process", "systemtest", "cpu-tight"]),
                    owner: ExecutionDomainOwner::Subsystem {
                        database: None,
                        name: "systemtest".to_string(),
                    },
                    budget: ExecutionDomainBudget {
                        cpu: DomainCpuBudget {
                            worker_slots: Some(1),
                            weight: Some(1),
                        },
                        ..Default::default()
                    },
                    placement: ExecutionDomainPlacement::Dedicated,
                    metadata: Default::default(),
                },
                default_case_usage: ExecutionResourceUsage {
                    cpu_workers: 1,
                    ..Default::default()
                },
            })
            .run_suite(Arc::new(UnschedulableSuite))
            .await
            .expect_err("unschedulable case should fail");
        assert!(matches!(
            error,
            SimulationHarnessError::DomainUnschedulable { .. }
        ));
    }
}
