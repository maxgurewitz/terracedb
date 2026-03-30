use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use terracedb::{
    Db, DbConfig, DbDependencies, LogCursor, OutboxEntry, S3Location, SsdConfig, StorageConfig,
    StubClock, StubFileSystem, StubObjectStore, TieredDurabilityMode, TieredStorageConfig, Value,
    test_support::{row_table_config, test_dependencies_with_clock},
};
use terracedb_workflows::{
    DEFAULT_TIMER_POLL_INTERVAL, WorkflowContext, WorkflowDefinition, WorkflowHandler,
    WorkflowHandlerError, WorkflowOutput, WorkflowRuntime, WorkflowStateMutation,
    WorkflowTimerCommand,
};

fn tiered_config(path: &str, durability: TieredDurabilityMode) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "workflows".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability,
        }),
        scheduler: None,
    }
}

#[derive(Clone)]
struct TestEnv {
    config: DbConfig,
    dependencies: DbDependencies,
    file_system: Arc<StubFileSystem>,
    clock: Arc<StubClock>,
}

impl TestEnv {
    fn new(path: &str, durability: TieredDurabilityMode) -> Self {
        let file_system = Arc::new(StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let clock = Arc::new(StubClock::default());
        let dependencies =
            test_dependencies_with_clock(file_system.clone(), object_store, clock.clone());

        Self {
            config: tiered_config(path, durability),
            dependencies,
            file_system,
            clock,
        }
    }

    async fn open(&self) -> Db {
        Db::open(self.config.clone(), self.dependencies.clone())
            .await
            .expect("open db")
    }
}

async fn wait_until<F, Fut>(mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(1), async move {
        loop {
            if predicate().await {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition should become true");
}

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

#[tokio::test]
async fn workflow_replays_startup_backlog_round_robin_and_outbox_order() {
    let env = TestEnv::new(
        "/workflow-backlog-round-robin",
        TieredDurabilityMode::GroupCommit,
    );
    let db = env.open().await;
    let source = db
        .create_table(row_table_config("workflow_source"))
        .await
        .expect("create source table");

    let mut batch = db.write_batch();
    batch.put(&source, b"alpha:1".to_vec(), Value::bytes("a1"));
    batch.put(&source, b"alpha:2".to_vec(), Value::bytes("a2"));
    batch.put(&source, b"beta:1".to_vec(), Value::bytes("b1"));
    let sequence = db
        .commit(batch, Default::default())
        .await
        .expect("commit backlog");

    let stats = ExecutionStats::default();
    let runtime = WorkflowRuntime::open(
        db.clone(),
        env.clock.clone(),
        WorkflowDefinition::new(
            "orders",
            [source.clone()],
            RecordingHandler {
                stats: stats.clone(),
            },
        ),
    )
    .await
    .expect("open workflow runtime");
    let handle = runtime.start().await.expect("start workflow runtime");

    wait_until(|| {
        let order = stats.order.clone();
        async move { order.lock().expect("order lock poisoned").len() == 3 }
    })
    .await;
    wait_until(|| {
        let runtime = &runtime;
        async move {
            runtime.load_state("alpha").await.expect("load alpha state") == Some(Value::bytes("2"))
                && runtime.load_state("beta").await.expect("load beta state")
                    == Some(Value::bytes("1"))
        }
    })
    .await;

    assert_eq!(
        stats.order.lock().expect("order lock poisoned").as_slice(),
        &["alpha".to_string(), "beta".to_string(), "alpha".to_string()]
    );
    assert_eq!(
        runtime.load_state("alpha").await.expect("load alpha state"),
        Some(Value::bytes("2"))
    );
    assert_eq!(
        runtime.load_state("beta").await.expect("load beta state"),
        Some(Value::bytes("1"))
    );
    assert_eq!(
        runtime
            .load_source_cursor(&source)
            .await
            .expect("load source cursor"),
        LogCursor::new(sequence, 2)
    );

    let max_active = stats.max_active.lock().expect("max-active lock poisoned");
    assert_eq!(max_active.get("alpha"), Some(&1));
    assert_eq!(max_active.get("beta"), Some(&1));

    assert!(
        runtime
            .tables()
            .outbox_table()
            .read(b"alpha:1".to_vec())
            .await
            .expect("read alpha outbox 1")
            .is_some()
    );
    assert!(
        runtime
            .tables()
            .outbox_table()
            .read(b"alpha:2".to_vec())
            .await
            .expect("read alpha outbox 2")
            .is_some()
    );
    assert!(
        runtime
            .tables()
            .outbox_table()
            .read(b"beta:1".to_vec())
            .await
            .expect("read beta outbox")
            .is_some()
    );

    handle.shutdown().await.expect("stop workflow runtime");
}

#[tokio::test]
async fn callback_admission_is_durable_before_return_and_replays_after_restart() {
    let env = TestEnv::new("/workflow-callback-replay", TieredDurabilityMode::Deferred);
    let db = env.open().await;

    let runtime = WorkflowRuntime::open(
        db.clone(),
        env.clock.clone(),
        WorkflowDefinition::new("callbacks", std::iter::empty(), CallbackReplayHandler)
            .with_timer_poll_interval(DEFAULT_TIMER_POLL_INTERVAL),
    )
    .await
    .expect("open workflow runtime");

    let durable_sequence = runtime
        .admit_callback("order-1", "cb-1", b"approved".to_vec())
        .await
        .expect("admit callback");
    assert_eq!(db.current_durable_sequence(), durable_sequence);

    env.file_system.crash();

    let reopened = env.open().await;
    let reopened_runtime = WorkflowRuntime::open(
        reopened.clone(),
        env.clock.clone(),
        WorkflowDefinition::new("callbacks", std::iter::empty(), CallbackReplayHandler),
    )
    .await
    .expect("reopen workflow runtime");
    let handle = reopened_runtime
        .start()
        .await
        .expect("start replay runtime");

    wait_until(|| {
        let reopened_runtime = &reopened_runtime;
        async move {
            reopened_runtime
                .load_state("order-1")
                .await
                .expect("load replayed state")
                == Some(Value::bytes("processed:cb-1"))
        }
    })
    .await;

    reopened.flush().await.expect("flush replayed outbox");
    assert!(
        reopened_runtime
            .tables()
            .outbox_table()
            .read(b"order-1:cb-1".to_vec())
            .await
            .expect("read replayed outbox")
            .is_some()
    );

    handle.shutdown().await.expect("stop replay runtime");
}

#[tokio::test]
async fn timer_loop_waits_for_durable_timer_rows_before_firing() {
    let env = TestEnv::new("/workflow-timer-fence", TieredDurabilityMode::Deferred);
    let db = env.open().await;
    let runtime = WorkflowRuntime::open(
        db.clone(),
        env.clock.clone(),
        WorkflowDefinition::new("timers", std::iter::empty(), TimerHandler)
            .with_timer_poll_interval(Duration::from_millis(1)),
    )
    .await
    .expect("open timer runtime");
    let handle = runtime.start().await.expect("start timer runtime");

    runtime
        .admit_callback("order-7", "start", b"begin".to_vec())
        .await
        .expect("admit scheduling callback");

    wait_until(|| {
        let runtime = &runtime;
        async move {
            runtime
                .load_state("order-7")
                .await
                .expect("load scheduled state")
                == Some(Value::bytes("scheduled"))
        }
    })
    .await;

    env.clock.advance(Duration::from_millis(10));
    tokio::task::yield_now().await;
    assert_eq!(
        runtime
            .load_state("order-7")
            .await
            .expect("load state before durable timer flush"),
        Some(Value::bytes("scheduled"))
    );

    db.flush().await.expect("flush scheduled timer");
    env.clock.advance(Duration::from_millis(1));
    wait_until(|| {
        let runtime = &runtime;
        async move {
            runtime
                .tables()
                .inbox_table()
                .read(b"order-7\0\0\0\0\0\0\0\0\x02".to_vec())
                .await
                .expect("read admitted timer inbox row")
                .is_some()
        }
    })
    .await;

    db.flush().await.expect("flush admitted timer trigger");
    wait_until(|| {
        let runtime = &runtime;
        async move {
            runtime
                .load_state("order-7")
                .await
                .expect("load fired state")
                == Some(Value::bytes("fired"))
        }
    })
    .await;

    handle.shutdown().await.expect("stop timer runtime");
}
