use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use terracedb::{
    ChangeFeedError, Clock, Db, FileSystemFailure, FileSystemOperation, LogCursor, OutboxEntry,
    StorageError, StorageErrorKind, StubClock, StubFileSystem, StubObjectStore, Table,
    TieredDurabilityMode, Timestamp, Value,
    test_support::{
        FailpointMode, db_failpoint_registry, failpoint_names, row_table_config, test_dependencies,
        test_dependencies_with_clock, tiered_test_config_with_durability,
    },
};
use terracedb_simulation::{
    CutPoint, SeededSimulationRunner, SimulationStackBuilder, TerracedbSimulationHarness,
};
use terracedb_workflows::{
    DEFAULT_TIMER_POLL_INTERVAL, RecurringSchedule, RecurringTickOutput,
    RecurringWorkflowDefinition, RecurringWorkflowHandle, RecurringWorkflowHandler,
    RecurringWorkflowRuntime, RecurringWorkflowState, WorkflowContext, WorkflowDefinition,
    WorkflowError, WorkflowHandle, WorkflowHandler, WorkflowHandlerError, WorkflowOutput,
    WorkflowProgressMode, WorkflowRuntime, WorkflowStateMutation, WorkflowTimerCommand,
};

const WORKFLOW_SIMULATION_DURATION: Duration = Duration::from_millis(600);
const WORKFLOW_TIMER_SIMULATION_DURATION: Duration = Duration::from_millis(1_200);
const SIMULATION_MIN_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const SIMULATION_MAX_MESSAGE_LATENCY: Duration = Duration::from_millis(1);

fn decode_count(value: Option<Value>) -> usize {
    match value {
        None => 0,
        Some(Value::Bytes(bytes)) => std::str::from_utf8(&bytes)
            .expect("state bytes should be utf-8")
            .parse()
            .expect("state should encode a count"),
        Some(Value::Record(_)) => panic!("tests only expect byte state"),
    }
}

#[derive(Clone, Default)]
struct ExecutionStats {
    order: Arc<Mutex<Vec<String>>>,
    active: Arc<Mutex<BTreeMap<String, usize>>>,
    max_active: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl ExecutionStats {
    fn enter(&self, instance_id: &str) {
        self.order
            .lock()
            .expect("order lock poisoned")
            .push(instance_id.to_string());

        let mut active = self.active.lock().expect("active lock poisoned");
        let next = active.get(instance_id).copied().unwrap_or_default() + 1;
        active.insert(instance_id.to_string(), next);

        let mut max_active = self.max_active.lock().expect("max-active lock poisoned");
        let current_max = max_active.get(instance_id).copied().unwrap_or_default();
        if next > current_max {
            max_active.insert(instance_id.to_string(), next);
        }
    }

    fn exit(&self, instance_id: &str) {
        let mut active = self.active.lock().expect("active lock poisoned");
        let next = active
            .get(instance_id)
            .copied()
            .expect("instance must be active")
            - 1;
        active.insert(instance_id.to_string(), next);
    }
}

struct RecordingHandler {
    stats: ExecutionStats,
}

#[async_trait]
impl WorkflowHandler for RecordingHandler {
    async fn route_event(
        &self,
        entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        let key = std::str::from_utf8(&entry.key).expect("source key should be utf-8");
        Ok(key
            .split_once(':')
            .expect("source key should contain an instance prefix")
            .0
            .to_string())
    }

    async fn handle(
        &self,
        instance_id: &str,
        state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        self.stats.enter(instance_id);
        tokio::task::yield_now().await;
        self.stats.exit(instance_id);

        let next = decode_count(state) + 1;
        let payload = match trigger {
            terracedb_workflows::WorkflowTrigger::Event(entry) => entry.key.clone(),
            terracedb_workflows::WorkflowTrigger::Timer { payload, .. } => payload.clone(),
            terracedb_workflows::WorkflowTrigger::Callback { response, .. } => response.clone(),
        };

        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(Value::bytes(next.to_string())),
            outbox_entries: vec![OutboxEntry {
                outbox_id: format!("{instance_id}:{next}").into_bytes(),
                idempotency_key: format!("{instance_id}:{next}"),
                payload,
            }],
            timers: Vec::new(),
        })
    }
}

struct CallbackReplayHandler;

#[async_trait]
impl WorkflowHandler for CallbackReplayHandler {
    async fn route_event(
        &self,
        _entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        Err(WorkflowHandlerError::new(std::io::Error::other(
            "callback workflow does not route source events",
        )))
    }

    async fn handle(
        &self,
        instance_id: &str,
        _state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        let terracedb_workflows::WorkflowTrigger::Callback {
            callback_id,
            response,
        } = trigger
        else {
            panic!("replay test only admits callbacks");
        };

        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(Value::bytes(format!("processed:{callback_id}"))),
            outbox_entries: vec![OutboxEntry {
                outbox_id: format!("{instance_id}:{callback_id}").into_bytes(),
                idempotency_key: format!("{instance_id}:{callback_id}"),
                payload: response.clone(),
            }],
            timers: Vec::new(),
        })
    }
}

struct TimerHandler;

#[async_trait]
impl WorkflowHandler for TimerHandler {
    async fn route_event(
        &self,
        _entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        Err(WorkflowHandlerError::new(std::io::Error::other(
            "timer workflow does not route source events",
        )))
    }

    async fn handle(
        &self,
        _instance_id: &str,
        _state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        match trigger {
            terracedb_workflows::WorkflowTrigger::Callback { .. } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("scheduled")),
                outbox_entries: Vec::new(),
                timers: vec![WorkflowTimerCommand::Schedule {
                    timer_id: b"payment-timeout".to_vec(),
                    fire_at: terracedb::Timestamp::new(5),
                    payload: b"timeout".to_vec(),
                }],
            }),
            terracedb_workflows::WorkflowTrigger::Timer { .. } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("fired")),
                outbox_entries: Vec::new(),
                timers: Vec::new(),
            }),
            terracedb_workflows::WorkflowTrigger::Event(_) => {
                panic!("timer workflow does not process source events")
            }
        }
    }
}

struct RecurringTimerHandler;

#[async_trait]
impl RecurringWorkflowHandler for RecurringTimerHandler {
    async fn tick(
        &self,
        instance_id: &str,
        state: &RecurringWorkflowState,
        fire_at: Timestamp,
        _ctx: &WorkflowContext,
    ) -> Result<RecurringTickOutput, WorkflowHandlerError> {
        let tick = state.tick_count + 1;
        Ok(RecurringTickOutput {
            outbox_entries: vec![OutboxEntry {
                outbox_id: format!("{instance_id}:{tick}").into_bytes(),
                idempotency_key: format!("{instance_id}:{tick}"),
                payload: fire_at.get().to_be_bytes().to_vec(),
            }],
            timers: Vec::new(),
        })
    }
}

#[tokio::test]
async fn workflow_runtime_surfaces_typed_change_feed_storage_errors() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-change-feed-timeout",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system.clone(), object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create workflow source");

    source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write workflow backlog");
    file_system.inject_failure(
        FileSystemFailure::timeout(
            FileSystemOperation::ReadAt,
            "/workflow-change-feed-timeout/commitlog/SEG-000001",
        )
        .persistent(),
    );

    let runtime = WorkflowRuntime::open(
        db,
        clock,
        WorkflowDefinition::new(
            "orders",
            [source],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");
    let handle = runtime.start().await.expect("start workflow runtime");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let error = handle
        .shutdown()
        .await
        .expect_err("workflow runtime should fail on change-feed scan");
    match error {
        WorkflowError::Storage(storage)
        | WorkflowError::ChangeFeed(ChangeFeedError::Storage(storage)) => {
            assert_eq!(storage.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("expected typed workflow change-feed failure, got {other:?}"),
    }
}

#[tokio::test]
async fn callback_admission_failpoint_surfaces_storage_error_before_commit() {
    let clock = Arc::new(StubClock::default());
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-callback-failpoint",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = WorkflowRuntime::open(
        db.clone(),
        clock,
        WorkflowDefinition::new(
            "callbacks",
            std::iter::empty::<Table>(),
            CallbackReplayHandler,
        ),
    )
    .await
    .expect("open workflow runtime");

    db_failpoint_registry(&db).arm_error(
        failpoint_names::WORKFLOW_CALLBACK_ADMISSION_BEFORE_COMMIT,
        StorageError::io("simulated workflow failpoint"),
        FailpointMode::Once,
    );

    let error = runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect_err("failpoint should fail callback admission");
    match error {
        WorkflowError::Storage(storage) => {
            assert_eq!(storage.kind(), StorageErrorKind::Io);
            assert!(
                storage.to_string().contains("simulated workflow failpoint"),
                "expected injected workflow failpoint context, got {storage}"
            );
        }
        other => panic!("expected workflow storage error from failpoint, got {other:?}"),
    }

    let sequence = runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("retry callback after one-shot failpoint");
    assert_eq!(sequence, terracedb::SequenceNumber::new(1));

    let handle = runtime.start().await.expect("start workflow runtime");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime
                .load_state("order-1")
                .await
                .expect("load callback workflow state")
                == Some(Value::bytes("processed:cb-1"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("workflow should process the retried callback");

    handle.shutdown().await.expect("shutdown workflow runtime");
}

struct WorkflowStack<H> {
    runtime: WorkflowRuntime<H>,
    handle: Option<WorkflowHandle>,
    source: Option<Table>,
}

impl<H> WorkflowStack<H>
where
    H: WorkflowHandler + 'static,
{
    async fn start(&mut self) -> Result<(), WorkflowError> {
        if self.handle.is_none() {
            self.handle = Some(self.runtime.start().await?);
        }
        Ok(())
    }

    async fn shutdown(self) -> Result<(), WorkflowError> {
        if let Some(handle) = self.handle {
            handle.abort().await?;
        }
        Ok(())
    }
}

fn backlog_stack_builder(
    stats: ExecutionStats,
) -> SimulationStackBuilder<WorkflowStack<RecordingHandler>> {
    SimulationStackBuilder::new(
        move |context, db| {
            let stats = stats.clone();
            async move {
                let source = db.create_table(row_table_config("workflow_source")).await?;
                let runtime = WorkflowRuntime::open(
                    db.clone(),
                    context.clock(),
                    WorkflowDefinition::new("orders", [source.clone()], RecordingHandler { stats })
                        .with_progress_mode(WorkflowProgressMode::Buffered),
                )
                .await?;

                Ok(WorkflowStack {
                    runtime,
                    handle: None,
                    source: Some(source),
                })
            }
        },
        |stack| async move {
            stack.shutdown().await?;
            Ok(())
        },
    )
}

fn callback_stack_builder() -> SimulationStackBuilder<WorkflowStack<CallbackReplayHandler>> {
    SimulationStackBuilder::new(
        |context, db| async move {
            let runtime = WorkflowRuntime::open(
                db,
                context.clock(),
                WorkflowDefinition::new(
                    "callbacks",
                    std::iter::empty::<Table>(),
                    CallbackReplayHandler,
                )
                .with_timer_poll_interval(DEFAULT_TIMER_POLL_INTERVAL),
            )
            .await?;

            Ok(WorkflowStack {
                runtime,
                handle: None,
                source: None,
            })
        },
        |stack| async move {
            stack.shutdown().await?;
            Ok(())
        },
    )
}

fn timer_stack_builder(
    progress_mode: Option<WorkflowProgressMode>,
) -> SimulationStackBuilder<WorkflowStack<TimerHandler>> {
    SimulationStackBuilder::new(
        move |context, db| async move {
            let mut definition =
                WorkflowDefinition::new("timers", std::iter::empty::<Table>(), TimerHandler)
                    .with_timer_poll_interval(Duration::from_millis(1));
            if let Some(progress_mode) = progress_mode {
                definition = definition.with_progress_mode(progress_mode);
            }
            let runtime = WorkflowRuntime::open(db, context.clock(), definition).await?;

            Ok(WorkflowStack {
                runtime,
                handle: None,
                source: None,
            })
        },
        |stack| async move {
            stack.shutdown().await?;
            Ok(())
        },
    )
}

struct RecurringWorkflowStack<H> {
    runtime: RecurringWorkflowRuntime<H>,
    handle: Option<RecurringWorkflowHandle>,
}

impl<H> RecurringWorkflowStack<H>
where
    H: RecurringWorkflowHandler + 'static,
{
    async fn start(&mut self) -> Result<(), WorkflowError> {
        if self.handle.is_none() {
            self.handle = Some(self.runtime.start().await?);
        }
        Ok(())
    }

    async fn shutdown(self) -> Result<(), WorkflowError> {
        if let Some(handle) = self.handle {
            handle.abort().await?;
        }
        Ok(())
    }
}

fn recurring_stack_builder() -> SimulationStackBuilder<RecurringWorkflowStack<RecurringTimerHandler>>
{
    SimulationStackBuilder::new(
        |context, db| async move {
            let runtime = RecurringWorkflowRuntime::open(
                db,
                context.clock(),
                RecurringWorkflowDefinition::new(
                    "recurring",
                    "job-1",
                    RecurringSchedule::custom(
                        |now| Some(Timestamp::new(now.get().saturating_add(10))),
                        |_state, fire_at| Some(Timestamp::new(fire_at.get().saturating_add(10))),
                    ),
                    RecurringTimerHandler,
                )
                .with_timer_poll_interval(Duration::from_millis(1)),
            )
            .await?;

            Ok(RecurringWorkflowStack {
                runtime,
                handle: None,
            })
        },
        |stack| async move {
            stack.shutdown().await?;
            Ok(())
        },
    )
}

async fn checkpoint_workflow<H>(
    harness: &mut TerracedbSimulationHarness<WorkflowStack<H>>,
    label: impl Into<String>,
    instance_ids: Vec<&str>,
) -> Result<(), terracedb_simulation::SimulationHarnessError>
where
    H: WorkflowHandler + 'static,
{
    let instance_ids = instance_ids
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    harness
        .checkpoint_with(label, move |_db, stack| {
            Box::pin(async move {
                let mut metadata = BTreeMap::from([
                    (
                        "workflow.name".to_string(),
                        stack.runtime.name().to_string(),
                    ),
                    (
                        "workflow.running".to_string(),
                        stack.handle.is_some().to_string(),
                    ),
                ]);
                if let Some(source) = &stack.source {
                    metadata.insert(
                        "workflow.source_cursor".to_string(),
                        format!("{:?}", stack.runtime.load_source_cursor(source).await?),
                    );
                }
                for instance_id in &instance_ids {
                    metadata.insert(
                        format!("workflow.state.{instance_id}"),
                        format!("{:?}", stack.runtime.load_state(instance_id).await?),
                    );
                }
                Ok(metadata)
            })
        })
        .await
}

async fn wait_for_workflow_state<H>(
    harness: &TerracedbSimulationHarness<WorkflowStack<H>>,
    instance_id: &str,
    expected: &str,
) -> Result<(), terracedb_simulation::SimulationHarnessError>
where
    H: WorkflowHandler + 'static,
{
    let state_table = harness.stack().runtime.tables().state_table().clone();
    let inbox_table = harness.stack().runtime.tables().inbox_table().clone();
    let instance_id = instance_id.to_string();
    let expected = Value::bytes(expected);
    harness
        .wait_for_change(
            format!("workflow state {instance_id} -> {:?}", expected),
            [&state_table],
            [&inbox_table],
            move |_db, stack| {
                let instance_id = instance_id.clone();
                let expected = expected.clone();
                Box::pin(async move {
                    Ok(stack.runtime.load_state(&instance_id).await? == Some(expected))
                })
            },
        )
        .await
}

async fn wait_for_visible_inbox_row<H>(
    harness: &TerracedbSimulationHarness<WorkflowStack<H>>,
    inbox_key: Vec<u8>,
) -> Result<(), terracedb_simulation::SimulationHarnessError>
where
    H: WorkflowHandler + 'static,
{
    let inbox_table = harness.stack().runtime.tables().inbox_table().clone();
    harness
        .wait_for_visible(
            "workflow visible inbox row",
            [&inbox_table],
            move |_db, stack| {
                let inbox_key = inbox_key.clone();
                Box::pin(async move {
                    Ok(stack
                        .runtime
                        .tables()
                        .inbox_table()
                        .read(inbox_key)
                        .await?
                        .is_some())
                })
            },
        )
        .await
}

async fn wait_for_recurring_state<H, P>(
    runtime: &RecurringWorkflowRuntime<H>,
    predicate: P,
) -> Result<RecurringWorkflowState, WorkflowError>
where
    H: RecurringWorkflowHandler + 'static,
    P: Fn(&RecurringWorkflowState) -> bool,
{
    for _ in 0..100 {
        if let Some(state) = runtime.load_state().await? {
            if predicate(&state) {
                return Ok(state);
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    Err(WorkflowError::Handler {
        name: runtime.name().to_string(),
        source: WorkflowHandlerError::new(std::io::Error::other(
            "timed out waiting for recurring state",
        )),
    })
}

async fn wait_for_simulation_recurring_tick_count<H>(
    harness: &TerracedbSimulationHarness<RecurringWorkflowStack<H>>,
    expected: u64,
) -> Result<(), terracedb_simulation::SimulationHarnessError>
where
    H: RecurringWorkflowHandler + 'static,
{
    let state_table = harness.stack().runtime.tables().state_table().clone();
    harness
        .wait_for_visible(
            format!("recurring tick count -> {expected}"),
            [&state_table],
            move |_db, stack| {
                Box::pin(async move {
                    Ok(stack
                        .runtime
                        .load_state()
                        .await?
                        .map(|state| state.tick_count == expected)
                        .unwrap_or(false))
                })
            },
        )
        .await
}

#[test]
fn workflow_replays_startup_backlog_round_robin_and_outbox_order() -> turmoil::Result {
    SeededSimulationRunner::new(0x4101)
        .with_simulation_duration(WORKFLOW_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let stats = ExecutionStats::default();
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-backlog-round-robin",
                    TieredDurabilityMode::GroupCommit,
                ),
                backlog_stack_builder(stats.clone()),
            )
            .await?;
            let source = harness
                .stack()
                .source
                .clone()
                .expect("backlog workflow source should exist");

            let mut batch = harness.db().write_batch();
            batch.put(&source, b"alpha:1".to_vec(), Value::bytes("a1"));
            batch.put(&source, b"alpha:2".to_vec(), Value::bytes("a2"));
            batch.put(&source, b"beta:1".to_vec(), Value::bytes("b1"));
            let sequence = harness.db().commit(batch, Default::default()).await?;

            harness.stack_mut().start().await?;
            checkpoint_workflow(&mut harness, "workflow-started", vec!["alpha", "beta"]).await?;
            wait_for_workflow_state(&harness, "alpha", "2").await?;
            wait_for_workflow_state(&harness, "beta", "1").await?;

            checkpoint_workflow(
                &mut harness,
                "workflow-backlog-drained",
                vec!["alpha", "beta"],
            )
            .await?;

            let order = stats.order.lock().expect("order lock poisoned").clone();
            harness.require_eq(
                "workflow execution order",
                &order,
                &vec!["alpha".to_string(), "beta".to_string(), "alpha".to_string()],
            )?;
            let runtime = &harness.stack().runtime;
            harness.require_eq(
                "alpha workflow state",
                &runtime.load_state("alpha").await?,
                &Some(Value::bytes("2")),
            )?;
            harness.require_eq(
                "beta workflow state",
                &runtime.load_state("beta").await?,
                &Some(Value::bytes("1")),
            )?;
            harness.require_eq(
                "workflow source cursor",
                &runtime.load_source_cursor(&source).await?,
                &LogCursor::new(sequence, 2),
            )?;

            let max_active = stats.max_active.lock().expect("max-active lock poisoned");
            harness.require_eq(
                "alpha max concurrency",
                &max_active.get("alpha").copied(),
                &Some(1),
            )?;
            harness.require_eq(
                "beta max concurrency",
                &max_active.get("beta").copied(),
                &Some(1),
            )?;

            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"alpha:1".to_vec())
                    .await?
                    .is_some(),
                "alpha first outbox entry should be present",
            )?;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"alpha:2".to_vec())
                    .await?
                    .is_some(),
                "alpha second outbox entry should be present",
            )?;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"beta:1".to_vec())
                    .await?
                    .is_some(),
                "beta outbox entry should be present",
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[test]
fn callback_admission_is_durable_before_return_and_replays_after_restart() -> turmoil::Result {
    SeededSimulationRunner::new(0x4102)
        .with_simulation_duration(WORKFLOW_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-callback-replay",
                    TieredDurabilityMode::Deferred,
                ),
                callback_stack_builder(),
            )
            .await?;

            let durable_sequence = harness
                .stack()
                .runtime
                .admit_callback("order-1", "cb-1", b"approved".to_vec())
                .await?;
            checkpoint_workflow(&mut harness, "callback-admitted", vec!["order-1"]).await?;
            harness.require_eq(
                "durable callback admission sequence",
                &harness.db().current_durable_sequence(),
                &durable_sequence,
            )?;

            harness.restart(CutPoint::AfterStep).await?;
            harness.stack_mut().start().await?;
            wait_for_workflow_state(&harness, "order-1", "processed:cb-1").await?;

            checkpoint_workflow(&mut harness, "callback-replayed", vec!["order-1"]).await?;
            harness.db().flush().await?;
            let runtime = &harness.stack().runtime;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"order-1:cb-1".to_vec())
                    .await?
                    .is_some(),
                "replayed outbox entry should be present after restart",
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[test]
fn timer_loop_waits_for_durable_timer_rows_before_firing() -> turmoil::Result {
    SeededSimulationRunner::new(0x4103)
        .with_simulation_duration(WORKFLOW_TIMER_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-timer-fence",
                    TieredDurabilityMode::Deferred,
                ),
                timer_stack_builder(Some(WorkflowProgressMode::Buffered)),
            )
            .await?;
            harness.stack_mut().start().await?;

            harness
                .stack()
                .runtime
                .admit_callback("order-7", "start", b"begin".to_vec())
                .await?;
            checkpoint_workflow(&mut harness, "timer-scheduled", vec!["order-7"]).await?;
            wait_for_workflow_state(&harness, "order-7", "scheduled").await?;

            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(10))
                .await;
            harness.require_eq(
                "timer state before durable flush",
                &harness.stack().runtime.load_state("order-7").await?,
                &Some(Value::bytes("scheduled")),
            )?;

            harness.db().flush().await?;
            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(1))
                .await;
            wait_for_visible_inbox_row(&harness, b"order-7\0\0\0\0\0\0\0\0\x02".to_vec()).await?;

            harness.db().flush().await?;
            wait_for_workflow_state(&harness, "order-7", "fired").await?;
            checkpoint_workflow(&mut harness, "timer-fired", vec!["order-7"]).await?;
            harness.require_eq(
                "timer final state",
                &harness.stack().runtime.load_state("order-7").await?,
                &Some(Value::bytes("fired")),
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[test]
fn workflow_auto_progress_allows_timer_chains_without_manual_flush() -> turmoil::Result {
    SeededSimulationRunner::new(0x4104)
        .with_simulation_duration(WORKFLOW_TIMER_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/workflow-durable-progress",
                    TieredDurabilityMode::Deferred,
                ),
                timer_stack_builder(None),
            )
            .await?;
            harness.stack_mut().start().await?;

            harness
                .stack()
                .runtime
                .admit_callback("order-9", "start", b"begin".to_vec())
                .await?;
            wait_for_workflow_state(&harness, "order-9", "scheduled").await?;

            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(10))
                .await;
            wait_for_workflow_state(&harness, "order-9", "fired").await?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[tokio::test]
async fn recurring_workflow_skips_duplicate_bootstrap_callbacks_and_updates_state() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::new(Timestamp::new(3)));
    let db = Db::open(
        tiered_test_config_with_durability(
            "/recurring-bootstrap-dedupe",
            TieredDurabilityMode::Deferred,
        ),
        test_dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");

    let runtime = RecurringWorkflowRuntime::open(
        db,
        clock.clone(),
        RecurringWorkflowDefinition::new(
            "planner",
            "planner-1",
            RecurringSchedule::fixed_interval(10),
            RecurringTimerHandler,
        )
        .with_timer_poll_interval(Duration::from_millis(1)),
    )
    .await
    .expect("open recurring workflow runtime");

    assert!(
        runtime
            .ensure_bootstrapped()
            .await
            .expect("admit first bootstrap")
            .is_some()
    );
    assert!(
        runtime
            .ensure_bootstrapped()
            .await
            .expect("admit duplicate bootstrap")
            .is_some()
    );

    let handle = runtime
        .start()
        .await
        .expect("start recurring workflow runtime");
    let bootstrapped = wait_for_recurring_state(&runtime, |state| state.next_fire_at.is_some())
        .await
        .expect("wait for recurring bootstrap state");
    assert_eq!(bootstrapped.bootstrapped_at, Timestamp::new(3));
    assert_eq!(bootstrapped.last_tick_at, None);
    assert_eq!(bootstrapped.next_fire_at, Some(Timestamp::new(13)));
    assert_eq!(bootstrapped.tick_count, 0);

    clock.set(Timestamp::new(15));
    let fired = wait_for_recurring_state(&runtime, |state| state.tick_count == 1)
        .await
        .expect("wait for recurring timer tick");
    assert_eq!(fired.last_tick_at, Some(Timestamp::new(13)));
    assert_eq!(fired.next_fire_at, Some(Timestamp::new(23)));
    assert_eq!(fired.tick_count, 1);

    assert!(
        runtime
            .tables()
            .outbox_table()
            .read(b"planner-1:1".to_vec())
            .await
            .expect("read first recurring outbox entry")
            .is_some()
    );
    assert!(
        runtime
            .tables()
            .outbox_table()
            .read(b"planner-1:2".to_vec())
            .await
            .expect("read duplicate recurring outbox entry")
            .is_none()
    );

    handle
        .shutdown()
        .await
        .expect("shutdown recurring workflow runtime");
}

#[test]
fn recurring_workflow_recovers_after_restart_with_custom_schedule() -> turmoil::Result {
    SeededSimulationRunner::new(0x4105)
        .with_simulation_duration(WORKFLOW_TIMER_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .run_with(|context| async move {
            let mut harness = TerracedbSimulationHarness::open(
                context,
                tiered_test_config_with_durability(
                    "/recurring-restart",
                    TieredDurabilityMode::Deferred,
                ),
                recurring_stack_builder(),
            )
            .await?;
            harness.stack_mut().start().await?;

            wait_for_simulation_recurring_tick_count(&harness, 0).await?;
            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(10))
                .await;
            wait_for_simulation_recurring_tick_count(&harness, 1).await?;

            harness.restart(CutPoint::AfterStep).await?;
            harness.stack_mut().start().await?;
            harness
                .context()
                .clock()
                .sleep(Duration::from_millis(10))
                .await;
            wait_for_simulation_recurring_tick_count(&harness, 2).await?;

            let runtime = &harness.stack().runtime;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"job-1:1".to_vec())
                    .await?
                    .is_some(),
                "first recurring outbox entry should survive restart",
            )?;
            harness.require(
                runtime
                    .tables()
                    .outbox_table()
                    .read(b"job-1:2".to_vec())
                    .await?
                    .is_some(),
                "second recurring outbox entry should be emitted after restart",
            )?;

            harness.shutdown().await?;
            Ok(())
        })
}

#[tokio::test]
async fn workflow_runtime_surfaces_change_feed_scan_failures_without_panicking() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config_with_durability(
            "/workflow-change-feed-failure",
            TieredDurabilityMode::GroupCommit,
        ),
        test_dependencies(file_system.clone(), object_store),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create source table");

    source
        .write(b"order-1:created".to_vec(), Value::bytes("created"))
        .await
        .expect("write source event");

    file_system.inject_failure(
        FileSystemFailure::timeout(
            FileSystemOperation::ReadAt,
            "/workflow-change-feed-failure/commitlog/SEG-000001",
        )
        .persistent(),
    );

    let runtime = WorkflowRuntime::open(
        db.clone(),
        Arc::new(StubClock::default()),
        WorkflowDefinition::new(
            "orders",
            [source.clone()],
            RecordingHandler {
                stats: ExecutionStats::default(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");
    let handle = runtime.start().await.expect("start workflow runtime");

    tokio::time::sleep(Duration::from_millis(10)).await;

    match tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("workflow shutdown should return promptly")
        .expect_err("workflow runtime should surface the underlying change-feed error")
    {
        WorkflowError::ChangeFeed(ChangeFeedError::Storage(error))
        | WorkflowError::Storage(error) => {
            assert_eq!(error.kind(), StorageErrorKind::Timeout);
        }
        other => panic!("unexpected workflow error: {other}"),
    }

    assert_eq!(
        runtime
            .load_source_cursor(&source)
            .await
            .expect("load source cursor after failed workflow runtime"),
        LogCursor::beginning()
    );
}
