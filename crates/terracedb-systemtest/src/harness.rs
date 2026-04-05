use std::{
    collections::VecDeque,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex as StdMutex},
    thread,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::FutureExt as _;
use thiserror::Error;
use tokio::{runtime::Builder, time::timeout};

#[derive(Clone, Debug)]
pub struct SimulationCaseSpec<C> {
    pub case_id: String,
    pub label: String,
    pub input: C,
    pub timeout: Duration,
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
        }
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

#[derive(Clone, Debug, Default)]
pub struct SimulationHarness {
    config: SimulationHarnessConfig,
}

impl SimulationHarness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_workers(mut self, max_workers: usize) -> Self {
        self.config.max_workers = max_workers.max(1);
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
            workers.push(thread::spawn(move || {
                let runtime = Builder::new_current_thread()
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
                        let started = Instant::now();
                        let status = match timeout(
                            case.timeout,
                            AssertUnwindSafe(suite.run_case(fixture.clone(), case.clone()))
                                .catch_unwind(),
                        )
                        .await
                        {
                            Ok(Ok(Ok(()))) => SimulationCaseStatus::Passed,
                            Ok(Ok(Err(error))) => {
                                SimulationCaseStatus::Failed {
                                    message: error.to_string(),
                                }
                            }
                            Ok(Err(payload)) => SimulationCaseStatus::Panicked {
                                message: panic_message(payload.as_ref()),
                            },
                            Err(_) => SimulationCaseStatus::TimedOut { after: case.timeout },
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
            let thread_result = worker
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
}

fn queue_len<C>(
    queue: &Arc<StdMutex<VecDeque<SimulationCaseSpec<C>>>>,
) -> usize {
    queue.lock().map(|queue| queue.len()).unwrap_or_default()
}

fn pop_case<C>(
    queue: &Arc<StdMutex<VecDeque<SimulationCaseSpec<C>>>>,
) -> Result<Option<SimulationCaseSpec<C>>, SimulationHarnessError> {
    queue.lock()
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
        Arc,
        atomic::{AtomicUsize, Ordering},
        Barrier,
    };
    use std::time::Duration;

    use super::{
        SimulationCaseSpec, SimulationCaseStatus, SimulationHarness, SimulationHarnessError,
        SimulationSuiteDefinition,
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
                SimulationCaseSpec::new(
                    "alpha",
                    "alpha",
                    "alpha",
                    Duration::from_millis(100),
                ),
                SimulationCaseSpec::new(
                    "beta",
                    "beta",
                    "beta",
                    Duration::from_millis(100),
                ),
            ])
        }

        async fn run_case(
            &self,
            fixture: Arc<Self::Fixture>,
            case: SimulationCaseSpec<Self::Case>,
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
}
