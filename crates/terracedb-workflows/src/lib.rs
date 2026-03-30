use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error as StdError,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};
use tracing::{Instrument, instrument::WithSubscriber};

use terracedb::{
    ChangeEntry, ChangeFeedError, ChangeKind, Clock, CommitError, CreateTableError, Db,
    DurableCursorStore, DurableTimerSet, Key, LogCursor, OperationContext, OutboxEntry, ReadError,
    ScanOptions, ScheduledTimer, SnapshotTooOld, StorageError, Table, TableConfig, TableFormat,
    Timestamp, Transaction, TransactionCommitError, TransactionalOutbox, Value,
};
use terracedb::{
    CompactionStrategy, SequenceNumber, SpanRelation, set_span_attribute, telemetry_attrs,
};

pub const DEFAULT_TIMER_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub const DEFAULT_SOURCE_BATCH_LIMIT: usize = 128;
pub const DEFAULT_TIMER_BATCH_LIMIT: usize = 128;

const WORKFLOW_FORMAT_VERSION: u8 = 1;
const WORKFLOW_TRIGGER_ORDER_FORMAT_VERSION: u8 = 1;
const WORKFLOW_TABLE_PREFIX: &str = "_workflow_";
const INBOX_KEY_SEPARATOR: u8 = 0;
const FULL_SCAN_START: &[u8] = b"";
const FULL_SCAN_END: &[u8] = &[0xff];

fn workflow_trigger_kind(trigger: &WorkflowTrigger) -> &'static str {
    match trigger {
        WorkflowTrigger::Event(_) => "event",
        WorkflowTrigger::Timer { .. } => "timer",
        WorkflowTrigger::Callback { .. } => "callback",
    }
}

fn apply_workflow_span_attributes(
    span: &tracing::Span,
    db: &Db,
    workflow_name: &str,
    instance_id: Option<&str>,
) {
    set_span_attribute(span, telemetry_attrs::DB_NAME, db.telemetry_db_name());
    set_span_attribute(
        span,
        telemetry_attrs::DB_INSTANCE,
        db.telemetry_db_instance(),
    );
    set_span_attribute(
        span,
        telemetry_attrs::STORAGE_MODE,
        db.telemetry_storage_mode(),
    );
    set_span_attribute(
        span,
        telemetry_attrs::WORKFLOW_NAME,
        workflow_name.to_string(),
    );
    if let Some(instance_id) = instance_id {
        set_span_attribute(
            span,
            telemetry_attrs::WORKFLOW_INSTANCE_ID,
            instance_id.to_string(),
        );
    }
}

fn attach_operation_context(
    span: &tracing::Span,
    operation_context: Option<&OperationContext>,
) -> bool {
    operation_context
        .filter(|context| !context.is_empty())
        .map(|context| context.attach_to_span(span, SpanRelation::Parent))
        .unwrap_or(false)
}

fn attach_operation_contexts(span: &tracing::Span, contexts: &[OperationContext]) {
    if let Some(parent) = contexts.first() {
        parent.attach_to_span(span, SpanRelation::Parent);
        for context in contexts.iter().skip(1) {
            context.attach_to_span(span, SpanRelation::Link);
        }
    }
}

fn collect_operation_contexts(entries: &[ChangeEntry]) -> Vec<OperationContext> {
    let mut seen = BTreeSet::new();
    let mut contexts = Vec::new();
    for entry in entries {
        let Some(context) = entry.operation_context.clone() else {
            continue;
        };
        if context.is_empty() {
            continue;
        }
        let key = (
            context.traceparent().map(str::to_string),
            context.tracestate().map(str::to_string),
        );
        if seen.insert(key) {
            contexts.push(context);
        }
    }
    contexts
}

#[derive(Debug)]
pub struct WorkflowHandlerError {
    inner: Box<dyn StdError + Send + Sync + 'static>,
}

impl WorkflowHandlerError {
    pub fn new<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(error),
        }
    }
}

impl std::fmt::Display for WorkflowHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl StdError for WorkflowHandlerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.inner.as_ref())
    }
}

impl From<ChangeFeedError> for WorkflowHandlerError {
    fn from(error: ChangeFeedError) -> Self {
        Self::new(error)
    }
}

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error(transparent)]
    CreateTable(#[from] CreateTableError),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    SnapshotTooOld(#[from] SnapshotTooOld),
    #[error(transparent)]
    ChangeFeed(#[from] ChangeFeedError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    TransactionCommit(#[from] TransactionCommitError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("workflow name cannot be empty")]
    EmptyName,
    #[error("workflow instance id cannot be empty")]
    EmptyInstanceId,
    #[error("workflow {name} is already running")]
    AlreadyRunning { name: String },
    #[error("workflow {name} subscription closed unexpectedly")]
    SubscriptionClosed { name: String },
    #[error("workflow {name} handler failed")]
    Handler {
        name: String,
        #[source]
        source: WorkflowHandlerError,
    },
    #[error("workflow {name} panicked: {reason}")]
    Panic { name: String, reason: String },
}

impl WorkflowError {
    pub fn snapshot_too_old(&self) -> Option<&SnapshotTooOld> {
        match self {
            Self::ChangeFeed(error) => error.snapshot_too_old(),
            Self::SnapshotTooOld(error) => Some(error),
            Self::CreateTable(_)
            | Self::Read(_)
            | Self::Storage(_)
            | Self::TransactionCommit(_)
            | Self::Join(_)
            | Self::EmptyName
            | Self::EmptyInstanceId
            | Self::AlreadyRunning { .. }
            | Self::SubscriptionClosed { .. }
            | Self::Handler { .. }
            | Self::Panic { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum WorkflowTrigger {
    Event(ChangeEntry),
    Timer {
        timer_id: Key,
        fire_at: Timestamp,
        payload: Vec<u8>,
    },
    Callback {
        callback_id: String,
        response: Vec<u8>,
    },
}

#[derive(Clone, Debug, Default)]
pub enum WorkflowStateMutation {
    #[default]
    Unchanged,
    Put(Value),
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowTimerCommand {
    Schedule {
        timer_id: Key,
        fire_at: Timestamp,
        payload: Vec<u8>,
    },
    Cancel {
        timer_id: Key,
    },
}

#[derive(Clone, Debug, Default)]
pub struct WorkflowOutput {
    pub state: WorkflowStateMutation,
    pub outbox_entries: Vec<OutboxEntry>,
    pub timers: Vec<WorkflowTimerCommand>,
}

#[derive(Clone, Debug)]
pub struct WorkflowContext {
    workflow_name: String,
    instance_id: String,
    trigger_hash: u64,
    state_hash: u64,
}

impl WorkflowContext {
    fn new(
        workflow_name: &str,
        instance_id: &str,
        trigger: &WorkflowTrigger,
        state: Option<&Value>,
    ) -> Result<Self, WorkflowError> {
        let trigger_hash = hash_bytes(&encode_trigger_context(trigger)?);
        let state_hash = hash_state(state)?;
        Ok(Self {
            workflow_name: workflow_name.to_string(),
            instance_id: instance_id.to_string(),
            trigger_hash,
            state_hash,
        })
    }

    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn stable_id(&self, scope: &str) -> String {
        format!("{scope}:{:016x}", self.scope_hash(scope))
    }

    pub fn stable_time(&self, scope: &str) -> Timestamp {
        Timestamp::new(self.scope_hash(scope))
    }

    fn scope_hash(&self, scope: &str) -> u64 {
        let mut bytes = Vec::with_capacity(
            self.workflow_name.len() + self.instance_id.len() + scope.len() + 32,
        );
        bytes.extend_from_slice(self.workflow_name.as_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(self.instance_id.as_bytes());
        bytes.push(0xfe);
        bytes.extend_from_slice(scope.as_bytes());
        bytes.extend_from_slice(&self.trigger_hash.to_be_bytes());
        bytes.extend_from_slice(&self.state_hash.to_be_bytes());
        hash_bytes(&bytes)
    }
}

#[async_trait]
pub trait WorkflowHandler: Send + Sync {
    async fn route_event(&self, entry: &ChangeEntry) -> Result<String, WorkflowHandlerError>;

    async fn handle(
        &self,
        instance_id: &str,
        state: Option<Value>,
        trigger: &WorkflowTrigger,
        ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError>;
}

#[async_trait]
pub trait WorkflowScheduler: Send + Sync {
    fn mark_ready(&self, instance_id: String);
    async fn next_ready_instance(&self) -> Option<String>;
    fn on_instance_yield(&self, instance_id: &str, has_more_pending: bool);
}

#[derive(Debug, Default)]
pub struct RoundRobinWorkflowScheduler {
    state: Mutex<RoundRobinSchedulerState>,
}

#[derive(Debug, Default)]
struct RoundRobinSchedulerState {
    queue: VecDeque<String>,
    queued: BTreeSet<String>,
}

#[async_trait]
impl WorkflowScheduler for RoundRobinWorkflowScheduler {
    fn mark_ready(&self, instance_id: String) {
        let mut state = self.state.lock().expect("scheduler lock poisoned");
        if state.queued.insert(instance_id.clone()) {
            state.queue.push_back(instance_id);
        }
    }

    async fn next_ready_instance(&self) -> Option<String> {
        let mut state = self.state.lock().expect("scheduler lock poisoned");
        let instance_id = state.queue.pop_front()?;
        state.queued.remove(&instance_id);
        Some(instance_id)
    }

    fn on_instance_yield(&self, instance_id: &str, has_more_pending: bool) {
        if has_more_pending {
            self.mark_ready(instance_id.to_string());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowTableNames {
    pub state: String,
    pub inbox: String,
    pub trigger_order: String,
    pub source_cursors: String,
    pub timer_schedule: String,
    pub timer_lookup: String,
    pub outbox: String,
}

impl WorkflowTableNames {
    pub fn for_workflow(name: &str) -> Self {
        let prefix = format!("{WORKFLOW_TABLE_PREFIX}{name}");
        Self {
            state: format!("{prefix}_state"),
            inbox: format!("{prefix}_inbox"),
            trigger_order: format!("{prefix}_trigger_order"),
            source_cursors: format!("{prefix}_source_cursors"),
            timer_schedule: format!("{prefix}_timer_schedule"),
            timer_lookup: format!("{prefix}_timer_lookup"),
            outbox: format!("{prefix}_outbox"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkflowTables {
    state: Table,
    inbox: Table,
    trigger_order: Table,
    source_cursors: Table,
    timers: DurableTimerSet,
    outbox: TransactionalOutbox,
}

impl WorkflowTables {
    pub fn state_table(&self) -> &Table {
        &self.state
    }

    pub fn inbox_table(&self) -> &Table {
        &self.inbox
    }

    pub fn trigger_order_table(&self) -> &Table {
        &self.trigger_order
    }

    pub fn source_cursor_table(&self) -> &Table {
        &self.source_cursors
    }

    pub fn timer_schedule_table(&self) -> &Table {
        self.timers.schedule_table()
    }

    pub fn timer_lookup_table(&self) -> &Table {
        self.timers.lookup_table()
    }

    pub fn timers(&self) -> &DurableTimerSet {
        &self.timers
    }

    pub fn outbox_table(&self) -> &Table {
        self.outbox.outbox_table()
    }

    pub fn outbox(&self) -> &TransactionalOutbox {
        &self.outbox
    }
}

pub struct WorkflowDefinition<H> {
    name: String,
    sources: Vec<Table>,
    handler: H,
    scheduler: Option<Arc<dyn WorkflowScheduler>>,
    table_names: WorkflowTableNames,
    timer_poll_interval: Duration,
    source_batch_limit: usize,
    timer_batch_limit: usize,
    durable_progress: bool,
}

impl<H> WorkflowDefinition<H> {
    pub fn new<I>(name: impl Into<String>, sources: I, handler: H) -> Self
    where
        I: IntoIterator<Item = Table>,
    {
        let name = name.into();
        Self {
            table_names: WorkflowTableNames::for_workflow(&name),
            name,
            sources: sources.into_iter().collect(),
            handler,
            scheduler: None,
            timer_poll_interval: DEFAULT_TIMER_POLL_INTERVAL,
            source_batch_limit: DEFAULT_SOURCE_BATCH_LIMIT,
            timer_batch_limit: DEFAULT_TIMER_BATCH_LIMIT,
            durable_progress: false,
        }
    }

    pub fn with_scheduler(mut self, scheduler: Arc<dyn WorkflowScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_table_names(mut self, table_names: WorkflowTableNames) -> Self {
        self.table_names = table_names;
        self
    }

    pub fn with_timer_poll_interval(mut self, interval: Duration) -> Self {
        self.timer_poll_interval = interval;
        self
    }

    pub fn with_source_batch_limit(mut self, batch_limit: usize) -> Self {
        self.source_batch_limit = batch_limit.max(1);
        self
    }

    pub fn with_timer_batch_limit(mut self, batch_limit: usize) -> Self {
        self.timer_batch_limit = batch_limit.max(1);
        self
    }

    pub fn with_durable_progress(mut self, durable_progress: bool) -> Self {
        self.durable_progress = durable_progress;
        self
    }
}

pub struct WorkflowRuntime<H> {
    inner: Arc<WorkflowRuntimeInner<H>>,
}

impl<H> Clone for WorkflowRuntime<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowSourceTelemetrySnapshot {
    pub source_table: String,
    pub durable_sequence: SequenceNumber,
    pub cursor_sequence: SequenceNumber,
    pub lag: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowTelemetrySnapshot {
    pub workflow_name: String,
    pub inbox_depth: usize,
    pub timer_depth: usize,
    pub outbox_depth: usize,
    pub in_flight_instances: usize,
    pub source_lags: Vec<WorkflowSourceTelemetrySnapshot>,
}

struct WorkflowRuntimeInner<H> {
    name: String,
    db: Db,
    clock: Arc<dyn Clock>,
    sources: Vec<Table>,
    handler: Arc<H>,
    scheduler: Arc<dyn WorkflowScheduler>,
    tables: WorkflowTables,
    source_cursors: DurableCursorStore,
    timer_poll_interval: Duration,
    source_batch_limit: usize,
    timer_batch_limit: usize,
    durable_progress: bool,
    running: Mutex<bool>,
    in_flight_instances: Mutex<BTreeSet<String>>,
    ready_notify: Notify,
}

impl<H> WorkflowRuntime<H>
where
    H: WorkflowHandler + 'static,
{
    pub async fn open(
        db: Db,
        clock: Arc<dyn Clock>,
        definition: WorkflowDefinition<H>,
    ) -> Result<Self, WorkflowError> {
        let workflow_name = definition.name.clone();
        let span = tracing::info_span!("terracedb.workflow.runtime.open");
        apply_workflow_span_attributes(&span, &db, &workflow_name, None);

        async move {
            if definition.name.trim().is_empty() {
                return Err(WorkflowError::EmptyName);
            }

            let tables = ensure_workflow_tables(&db, &definition.table_names).await?;
            let scheduler = definition
                .scheduler
                .unwrap_or_else(|| Arc::new(RoundRobinWorkflowScheduler::default()));

            Ok(Self {
                inner: Arc::new(WorkflowRuntimeInner {
                    name: definition.name,
                    db,
                    clock,
                    sources: definition.sources,
                    handler: Arc::new(definition.handler),
                    scheduler,
                    source_cursors: DurableCursorStore::new(tables.source_cursors.clone()),
                    tables,
                    timer_poll_interval: definition.timer_poll_interval,
                    source_batch_limit: definition.source_batch_limit,
                    timer_batch_limit: definition.timer_batch_limit,
                    durable_progress: definition.durable_progress,
                    running: Mutex::new(false),
                    in_flight_instances: Mutex::new(BTreeSet::new()),
                    ready_notify: Notify::new(),
                }),
            })
        }
        .instrument(span.clone())
        .await
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn tables(&self) -> &WorkflowTables {
        &self.inner.tables
    }

    pub async fn load_state(&self, instance_id: &str) -> Result<Option<Value>, WorkflowError> {
        Ok(self
            .inner
            .tables
            .state_table()
            .read(instance_id.as_bytes().to_vec())
            .await?)
    }

    pub async fn load_source_cursor(&self, source: &Table) -> Result<LogCursor, WorkflowError> {
        Ok(self
            .inner
            .source_cursors
            .load(&source_cursor_key(source))
            .await?)
    }

    pub async fn admit_callback(
        &self,
        instance_id: impl Into<String>,
        callback_id: impl Into<String>,
        response: Vec<u8>,
    ) -> Result<SequenceNumber, WorkflowError> {
        self.admit_callback_with_context(
            instance_id,
            callback_id,
            response,
            Some(OperationContext::current()).filter(|context| !context.is_empty()),
        )
        .await
    }

    pub async fn admit_callback_with_context(
        &self,
        instance_id: impl Into<String>,
        callback_id: impl Into<String>,
        response: Vec<u8>,
        operation_context: Option<OperationContext>,
    ) -> Result<SequenceNumber, WorkflowError> {
        let instance_id = instance_id.into();
        if instance_id.is_empty() {
            return Err(WorkflowError::EmptyInstanceId);
        }
        let callback_id = callback_id.into();

        let trigger = WorkflowTrigger::Callback {
            callback_id: callback_id.clone(),
            response,
        };
        let span = tracing::info_span!("terracedb.workflow.callback.admit");
        apply_workflow_span_attributes(&span, &self.inner.db, &self.inner.name, Some(&instance_id));
        set_span_attribute(
            &span,
            telemetry_attrs::WORKFLOW_TRIGGER_KIND,
            workflow_trigger_kind(&trigger),
        );
        set_span_attribute(&span, telemetry_attrs::CALLBACK_ID, callback_id);
        attach_operation_context(&span, operation_context.as_ref());
        let span_for_attrs = span.clone();

        async move {
            loop {
                let mut tx = Transaction::begin(&self.inner.db).await;
                let trigger_seq = stage_trigger_admission(
                    &self.inner,
                    &mut tx,
                    &instance_id,
                    &trigger,
                    operation_context.clone(),
                )
                .await?;
                match tx.commit().await {
                    Ok(sequence) => {
                        set_span_attribute(
                            &span_for_attrs,
                            telemetry_attrs::WORKFLOW_TRIGGER_SEQ,
                            trigger_seq,
                        );
                        self.inner.scheduler.mark_ready(instance_id.clone());
                        self.inner.ready_notify.notify_one();
                        return Ok(sequence);
                    }
                    Err(TransactionCommitError::Commit(CommitError::Conflict)) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        .instrument(span.clone())
        .await
    }

    pub async fn start(&self) -> Result<WorkflowHandle, WorkflowError> {
        {
            let mut running = self.inner.running.lock().expect("running lock poisoned");
            if *running {
                return Err(WorkflowError::AlreadyRunning {
                    name: self.inner.name.clone(),
                });
            }
            *running = true;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runtime = self.inner.clone();
        let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
        let task = tokio::spawn(
            async move {
                let result = AssertUnwindSafe(run_workflow_runtime(runtime.clone(), shutdown_rx))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|payload| {
                        Err(WorkflowError::Panic {
                            name: runtime.name.clone(),
                            reason: panic_payload_to_string(payload),
                        })
                    });

                *runtime.running.lock().expect("running lock poisoned") = false;
                result
            }
            .with_subscriber(dispatch),
        );

        Ok(WorkflowHandle {
            name: self.inner.name.clone(),
            shutdown: shutdown_tx,
            task,
        })
    }

    pub async fn telemetry_snapshot(&self) -> Result<WorkflowTelemetrySnapshot, WorkflowError> {
        let durable_sequence = self.inner.db.current_durable_sequence();
        let inbox_depth =
            count_rows_at_sequence(self.inner.tables.inbox_table(), durable_sequence).await?;
        let timer_depth =
            count_rows_at_sequence(self.inner.tables.timer_schedule_table(), durable_sequence)
                .await?;
        let outbox_depth =
            count_rows_at_sequence(self.inner.tables.outbox_table(), durable_sequence).await?;
        let in_flight_instances = self
            .inner
            .in_flight_instances
            .lock()
            .expect("in-flight lock poisoned")
            .len();
        let mut source_lags = Vec::with_capacity(self.inner.sources.len());
        for source in &self.inner.sources {
            let durable_source_sequence = self.inner.db.subscribe_durable(source).current();
            let cursor = self.load_source_cursor(source).await?;
            source_lags.push(WorkflowSourceTelemetrySnapshot {
                source_table: source.name().to_string(),
                durable_sequence: durable_source_sequence,
                cursor_sequence: cursor.sequence(),
                lag: durable_source_sequence
                    .get()
                    .saturating_sub(cursor.sequence().get()),
            });
        }

        Ok(WorkflowTelemetrySnapshot {
            workflow_name: self.inner.name.clone(),
            inbox_depth,
            timer_depth,
            outbox_depth,
            in_flight_instances,
            source_lags,
        })
    }
}

pub struct WorkflowHandle {
    name: String,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), WorkflowError>>,
}

impl WorkflowHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn abort(self) -> Result<(), WorkflowError> {
        self.shutdown.send_replace(true);
        self.task.abort();
        match self.task.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn shutdown(self) -> Result<(), WorkflowError> {
        self.shutdown.send_replace(true);
        self.task.await?
    }
}

async fn commit_runtime_transaction<H>(
    runtime: &WorkflowRuntimeInner<H>,
    tx: Transaction,
) -> Result<SequenceNumber, TransactionCommitError> {
    if runtime.durable_progress {
        tx.commit().await
    } else {
        tx.commit_no_flush().await
    }
}

async fn run_workflow_runtime<H>(
    runtime: Arc<WorkflowRuntimeInner<H>>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let span = tracing::info_span!("terracedb.workflow.runtime");
    apply_workflow_span_attributes(&span, &runtime.db, &runtime.name, None);

    async move {
        let mut tasks = tokio::task::JoinSet::new();
        let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
        tasks.spawn(
            run_inbox_executor(runtime.clone(), shutdown_rx.clone())
                .with_subscriber(dispatch.clone()),
        );
        tasks.spawn(
            run_timer_loop(runtime.clone(), shutdown_rx.clone()).with_subscriber(dispatch.clone()),
        );
        for source in runtime.sources.clone() {
            tasks.spawn(
                run_source_admission_loop(runtime.clone(), source, shutdown_rx.clone())
                    .with_subscriber(dispatch.clone()),
            );
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(error);
                }
                Err(error) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(error.into());
                }
            }
        }

        Ok(())
    }
    .instrument(span.clone())
    .await
}

async fn run_source_admission_loop<H>(
    runtime: Arc<WorkflowRuntimeInner<H>>,
    source: Table,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let mut cursor = runtime
        .source_cursors
        .load(&source_cursor_key(&source))
        .await?;
    let mut durable_wakes = runtime.db.subscribe_durable(&source);

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        while admit_source_page(&runtime, &source, &mut cursor).await? {
            if *shutdown_rx.borrow() {
                return Ok(());
            }
        }

        if *shutdown_rx.borrow() {
            return Ok(());
        }

        if durable_wakes.current() > cursor.sequence() {
            continue;
        }

        tokio::select! {
            changed = durable_wakes.changed() => {
                if changed.is_err() {
                    return Err(WorkflowError::SubscriptionClosed {
                        name: runtime.name.clone(),
                    });
                }
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn admit_source_page<H>(
    runtime: &WorkflowRuntimeInner<H>,
    source: &Table,
    cursor: &mut LogCursor,
) -> Result<bool, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let mut stream = runtime
        .db
        .scan_durable_since(
            source,
            *cursor,
            ScanOptions {
                limit: Some(runtime.source_batch_limit),
                ..ScanOptions::default()
            },
        )
        .await?;

    let mut page = Vec::new();
    while let Some(entry) = stream
        .try_next()
        .await
        .map_err(ChangeFeedError::Storage)
        .map_err(WorkflowError::ChangeFeed)?
    {
        page.push(entry);
    }

    if page.is_empty() {
        return Ok(false);
    }
    let operation_contexts = collect_operation_contexts(&page);
    let span = tracing::info_span!("terracedb.workflow.source_batch");
    apply_workflow_span_attributes(&span, &runtime.db, &runtime.name, None);
    set_span_attribute(
        &span,
        telemetry_attrs::SOURCE_TABLE,
        source.name().to_string(),
    );
    set_span_attribute(
        &span,
        "terracedb.workflow.batch.entry_count",
        page.len() as u64,
    );
    attach_operation_contexts(&span, &operation_contexts);
    let span_for_attrs = span.clone();

    async move {
        loop {
            let mut tx = Transaction::begin(&runtime.db).await;
            let mut ready_seen = BTreeSet::new();
            let mut newly_ready = Vec::new();
            let mut new_cursor = *cursor;

            for entry in &page {
                let instance_id = runtime.handler.route_event(entry).await.map_err(|source| {
                    WorkflowError::Handler {
                        name: runtime.name.clone(),
                        source,
                    }
                })?;
                if instance_id.is_empty() {
                    return Err(WorkflowError::EmptyInstanceId);
                }

                stage_trigger_admission(
                    runtime,
                    &mut tx,
                    &instance_id,
                    &WorkflowTrigger::Event(entry.clone()),
                    entry.operation_context.clone(),
                )
                .await?;
                if ready_seen.insert(instance_id.clone()) {
                    newly_ready.push(instance_id);
                }
                new_cursor = entry.cursor;
            }

            runtime.source_cursors.stage_persist_in_transaction(
                &mut tx,
                source_cursor_key(source),
                new_cursor,
            );

            match commit_runtime_transaction(runtime, tx).await {
                Ok(_) => {
                    *cursor = new_cursor;
                    set_span_attribute(
                        &span_for_attrs,
                        telemetry_attrs::LOG_CURSOR,
                        format!("{}:{}", new_cursor.sequence().get(), new_cursor.op_index()),
                    );
                    for instance_id in newly_ready {
                        runtime.scheduler.mark_ready(instance_id);
                    }
                    runtime.ready_notify.notify_one();
                    return Ok(true);
                }
                Err(TransactionCommitError::Commit(CommitError::Conflict)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
    .instrument(span.clone())
    .await
}

async fn run_timer_loop<H>(
    runtime: Arc<WorkflowRuntimeInner<H>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let mut timer_wakes = runtime.tables.timers.subscribe_durable(&runtime.db);

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        let _ = admit_due_timer_batch(&runtime).await?;

        if *shutdown_rx.borrow() {
            return Ok(());
        }

        tokio::select! {
            _ = runtime.clock.sleep(runtime.timer_poll_interval) => {}
            changed = timer_wakes.changed() => {
                if changed.is_err() {
                    return Err(WorkflowError::SubscriptionClosed {
                        name: runtime.name.clone(),
                    });
                }
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn admit_due_timer_batch<H>(runtime: &WorkflowRuntimeInner<H>) -> Result<bool, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let due = runtime
        .tables
        .timers
        .scan_due_durable(
            &runtime.db,
            runtime.clock.now(),
            Some(runtime.timer_batch_limit),
        )
        .await?;
    if due.is_empty() {
        return Ok(false);
    }
    let contexts = due
        .timers
        .iter()
        .filter_map(|timer| {
            decode_payload::<StoredWorkflowTimer>(&timer.payload, "workflow timer payload")
                .ok()
                .and_then(|stored| stored.operation_context)
        })
        .collect::<Vec<_>>();
    let span = tracing::info_span!("terracedb.workflow.timer.fire_batch");
    apply_workflow_span_attributes(&span, &runtime.db, &runtime.name, None);
    set_span_attribute(
        &span,
        "terracedb.workflow.timer.count",
        due.timers.len() as u64,
    );
    attach_operation_contexts(&span, &contexts);

    async move {
        loop {
            let mut tx = Transaction::begin(&runtime.db).await;
            let mut ready_seen = BTreeSet::new();
            let mut newly_ready = Vec::new();

            for timer in &due.timers {
                let stored_timer = decode_payload::<StoredWorkflowTimer>(
                    &timer.payload,
                    "workflow timer payload",
                )?;
                stage_trigger_admission(
                    runtime,
                    &mut tx,
                    &stored_timer.workflow_instance,
                    &WorkflowTrigger::Timer {
                        timer_id: timer.timer_id.clone(),
                        fire_at: timer.fire_at,
                        payload: stored_timer.payload.clone(),
                    },
                    stored_timer.operation_context.clone(),
                )
                .await?;
                runtime.tables.timers.stage_delete_at_in_transaction(
                    &mut tx,
                    timer.timer_id.clone(),
                    timer.fire_at,
                );
                if ready_seen.insert(stored_timer.workflow_instance.clone()) {
                    newly_ready.push(stored_timer.workflow_instance);
                }
            }

            match tx.commit().await {
                Ok(_) => {
                    for instance_id in newly_ready {
                        runtime.scheduler.mark_ready(instance_id);
                    }
                    runtime.ready_notify.notify_one();
                    return Ok(true);
                }
                Err(TransactionCommitError::Commit(CommitError::Conflict)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
    .instrument(span.clone())
    .await
}

async fn run_inbox_executor<H>(
    runtime: Arc<WorkflowRuntimeInner<H>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let mut inbox_wakes = runtime.db.subscribe_durable(runtime.tables.inbox_table());
    seed_ready_instances_from_durable_inbox(&runtime).await?;

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        drain_ready_instances(&runtime, &shutdown_rx).await?;

        if *shutdown_rx.borrow() {
            return Ok(());
        }

        tokio::select! {
            _ = runtime.ready_notify.notified() => {}
            changed = inbox_wakes.changed() => {
                if changed.is_err() {
                    return Err(WorkflowError::SubscriptionClosed {
                        name: runtime.name.clone(),
                    });
                }
                seed_ready_instances_from_durable_inbox(&runtime).await?;
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn seed_ready_instances_from_durable_inbox<H>(
    runtime: &WorkflowRuntimeInner<H>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let durable_sequence = runtime.db.current_durable_sequence();
    let mut rows = runtime
        .tables
        .inbox_table()
        .scan_at(
            Vec::new(),
            vec![0xff],
            durable_sequence,
            ScanOptions::default(),
        )
        .await?;

    while let Some((_, value)) = rows.next().await {
        let admitted = decode_admitted_trigger(&runtime.db, &value)?;
        runtime.scheduler.mark_ready(admitted.workflow_instance);
    }

    Ok(())
}

async fn drain_ready_instances<H>(
    runtime: &WorkflowRuntimeInner<H>,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        let Some(instance_id) = runtime.scheduler.next_ready_instance().await else {
            return Ok(());
        };

        if !begin_instance_execution(runtime, &instance_id) {
            continue;
        }

        let result = process_one_ready_instance(runtime, &instance_id).await;
        finish_instance_execution(runtime, &instance_id);

        if *shutdown_rx.borrow() {
            return Ok(());
        }

        match result {
            Ok(has_more_pending) => {
                runtime
                    .scheduler
                    .on_instance_yield(&instance_id, has_more_pending);
            }
            Err(error) => return Err(error),
        }
    }
}

fn begin_instance_execution<H>(runtime: &WorkflowRuntimeInner<H>, instance_id: &str) -> bool {
    runtime
        .in_flight_instances
        .lock()
        .expect("in-flight lock poisoned")
        .insert(instance_id.to_string())
}

fn finish_instance_execution<H>(runtime: &WorkflowRuntimeInner<H>, instance_id: &str) {
    runtime
        .in_flight_instances
        .lock()
        .expect("in-flight lock poisoned")
        .remove(instance_id);
}

async fn process_one_ready_instance<H>(
    runtime: &WorkflowRuntimeInner<H>,
    instance_id: &str,
) -> Result<bool, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let durable_sequence = runtime.db.current_durable_sequence();
    let mut pending = runtime
        .tables
        .inbox_table()
        .scan_at(
            inbox_start_key(instance_id),
            inbox_end_key(instance_id),
            durable_sequence,
            ScanOptions {
                limit: Some(2),
                ..ScanOptions::default()
            },
        )
        .await?;

    let mut rows = Vec::new();
    while let Some((inbox_key, value)) = pending.next().await {
        rows.push((inbox_key, decode_admitted_trigger(&runtime.db, &value)?));
    }

    let Some((inbox_key, admitted)) = rows.first().cloned() else {
        return Ok(false);
    };

    process_workflow_trigger(
        runtime,
        instance_id,
        admitted.trigger,
        admitted.trigger_seq,
        admitted.operation_context,
        inbox_key,
    )
    .await?;
    Ok(rows.len() > 1)
}

async fn process_workflow_trigger<H>(
    runtime: &WorkflowRuntimeInner<H>,
    instance_id: &str,
    trigger: WorkflowTrigger,
    trigger_seq: u64,
    operation_context: Option<OperationContext>,
    inbox_key: Key,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let span = tracing::info_span!("terracedb.workflow.step");
    apply_workflow_span_attributes(&span, &runtime.db, &runtime.name, Some(instance_id));
    set_span_attribute(
        &span,
        telemetry_attrs::WORKFLOW_TRIGGER_KIND,
        workflow_trigger_kind(&trigger),
    );
    set_span_attribute(&span, telemetry_attrs::WORKFLOW_TRIGGER_SEQ, trigger_seq);
    match &trigger {
        WorkflowTrigger::Event(entry) => {
            set_span_attribute(
                &span,
                telemetry_attrs::SOURCE_TABLE,
                entry.table.name().to_string(),
            );
            set_span_attribute(&span, telemetry_attrs::SEQUENCE, entry.sequence.get());
        }
        WorkflowTrigger::Timer {
            timer_id, fire_at, ..
        } => {
            set_span_attribute(
                &span,
                telemetry_attrs::TIMER_ID,
                String::from_utf8_lossy(timer_id).into_owned(),
            );
            set_span_attribute(&span, "terracedb.timer.fire_at", fire_at.get());
        }
        WorkflowTrigger::Callback { callback_id, .. } => {
            set_span_attribute(&span, telemetry_attrs::CALLBACK_ID, callback_id.clone());
        }
    }
    attach_operation_context(&span, operation_context.as_ref());

    async move {
        let mut tx = Transaction::begin(&runtime.db).await;
        let current_state = tx
            .read(runtime.tables.state_table(), instance_key(instance_id))
            .await?;
        let ctx =
            WorkflowContext::new(&runtime.name, instance_id, &trigger, current_state.as_ref())?;
        let output = runtime
            .handler
            .handle(instance_id, current_state, &trigger, &ctx)
            .await
            .map_err(|source| WorkflowError::Handler {
                name: runtime.name.clone(),
                source,
            })?;

        tx.delete(runtime.tables.inbox_table(), inbox_key);

        match output.state {
            WorkflowStateMutation::Unchanged => {}
            WorkflowStateMutation::Put(value) => {
                tx.write(
                    runtime.tables.state_table(),
                    instance_key(instance_id),
                    value,
                );
            }
            WorkflowStateMutation::Delete => {
                tx.delete(runtime.tables.state_table(), instance_key(instance_id));
            }
        }

        for entry in output.outbox_entries {
            runtime
                .tables
                .outbox
                .stage_entry_in_transaction(&mut tx, entry)?;
        }

        for timer in output.timers {
            match timer {
                WorkflowTimerCommand::Schedule {
                    timer_id,
                    fire_at,
                    payload,
                } => runtime.tables.timers.stage_schedule_in_transaction(
                    &mut tx,
                    ScheduledTimer {
                        timer_id,
                        fire_at,
                        payload: encode_payload(
                            &StoredWorkflowTimer {
                                workflow_instance: instance_id.to_string(),
                                payload,
                                operation_context: operation_context.clone(),
                            },
                            "workflow timer payload",
                        )?,
                    },
                )?,
                WorkflowTimerCommand::Cancel { timer_id } => {
                    runtime
                        .tables
                        .timers
                        .cancel_in_transaction(&mut tx, timer_id)
                        .await?;
                }
            }
        }

        commit_runtime_transaction(runtime, tx).await?;
        Ok(())
    }
    .instrument(span.clone())
    .await
}

async fn stage_trigger_admission<H>(
    runtime: &WorkflowRuntimeInner<H>,
    tx: &mut Transaction,
    instance_id: &str,
    trigger: &WorkflowTrigger,
    operation_context: Option<OperationContext>,
) -> Result<u64, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    if instance_id.is_empty() {
        return Err(WorkflowError::EmptyInstanceId);
    }

    let current = tx
        .read(
            runtime.tables.trigger_order_table(),
            instance_key(instance_id),
        )
        .await?;
    let trigger_sequence = decode_trigger_order(current.as_ref())? + 1;

    tx.write(
        runtime.tables.trigger_order_table(),
        instance_key(instance_id),
        Value::bytes(encode_trigger_order(trigger_sequence)),
    );
    tx.write(
        runtime.tables.inbox_table(),
        inbox_key(instance_id, trigger_sequence),
        Value::bytes(encode_payload(
            &StoredAdmittedWorkflowTrigger {
                workflow_instance: instance_id.to_string(),
                trigger_seq: trigger_sequence,
                trigger: StoredWorkflowTrigger::from_runtime_trigger(trigger),
                operation_context,
            },
            "workflow inbox trigger",
        )?),
    );

    Ok(trigger_sequence)
}

async fn ensure_workflow_tables(
    db: &Db,
    table_names: &WorkflowTableNames,
) -> Result<WorkflowTables, WorkflowError> {
    let state = ensure_table(db, &table_names.state).await?;
    let inbox = ensure_table(db, &table_names.inbox).await?;
    let trigger_order = ensure_table(db, &table_names.trigger_order).await?;
    let source_cursors = ensure_table(db, &table_names.source_cursors).await?;
    let timer_schedule = ensure_table(db, &table_names.timer_schedule).await?;
    let timer_lookup = ensure_table(db, &table_names.timer_lookup).await?;
    let outbox = ensure_table(db, &table_names.outbox).await?;

    Ok(WorkflowTables {
        state,
        inbox,
        trigger_order,
        source_cursors,
        timers: DurableTimerSet::new(timer_schedule, timer_lookup),
        outbox: TransactionalOutbox::new(outbox),
    })
}

async fn ensure_table(db: &Db, name: &str) -> Result<Table, WorkflowError> {
    match db.create_table(workflow_table_config(name)).await {
        Ok(table) => Ok(table),
        Err(CreateTableError::AlreadyExists(_)) => lookup_table(db, name).map_err(Into::into),
        Err(error) => Err(error.into()),
    }
}

fn lookup_table(db: &Db, name: &str) -> Result<Table, StorageError> {
    db.try_table(name)
        .ok_or_else(|| StorageError::not_found(format!("table does not exist: {name}")))
}

fn workflow_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Row,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(8),
        history_retention_sequences: None,
        compaction_strategy: CompactionStrategy::Leveled,
        schema: None,
        metadata: BTreeMap::new(),
    }
}

fn instance_key(instance_id: &str) -> Key {
    instance_id.as_bytes().to_vec()
}

fn source_cursor_key(source: &Table) -> Key {
    source.name().as_bytes().to_vec()
}

fn inbox_key(instance_id: &str, trigger_seq: u64) -> Key {
    let mut key = Vec::with_capacity(instance_id.len() + 1 + 8);
    key.extend_from_slice(instance_id.as_bytes());
    key.push(INBOX_KEY_SEPARATOR);
    key.extend_from_slice(&trigger_seq.to_be_bytes());
    key
}

fn inbox_start_key(instance_id: &str) -> Key {
    inbox_key(instance_id, 0)
}

fn inbox_end_key(instance_id: &str) -> Key {
    inbox_key(instance_id, u64::MAX)
}

fn encode_trigger_order(trigger_seq: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9);
    bytes.push(WORKFLOW_TRIGGER_ORDER_FORMAT_VERSION);
    bytes.extend_from_slice(&trigger_seq.to_be_bytes());
    bytes
}

fn decode_trigger_order(value: Option<&Value>) -> Result<u64, WorkflowError> {
    let Some(value) = value else {
        return Ok(0);
    };
    let bytes = expect_bytes_value(value, "workflow trigger order")?;
    if bytes.len() != 9 || bytes[0] != WORKFLOW_TRIGGER_ORDER_FORMAT_VERSION {
        return Err(
            StorageError::corruption("workflow trigger-order value encoding is invalid").into(),
        );
    }

    let mut sequence = [0_u8; 8];
    sequence.copy_from_slice(&bytes[1..]);
    Ok(u64::from_be_bytes(sequence))
}

fn encode_payload<T>(value: &T, context: &str) -> Result<Vec<u8>, WorkflowError>
where
    T: Serialize,
{
    let json = serde_json::to_vec(value).map_err(|error| {
        StorageError::corruption(format!("{context} serialization failed: {error}"))
    })?;
    let mut bytes = Vec::with_capacity(1 + json.len());
    bytes.push(WORKFLOW_FORMAT_VERSION);
    bytes.extend_from_slice(&json);
    Ok(bytes)
}

fn decode_payload<T>(bytes: &[u8], context: &str) -> Result<T, WorkflowError>
where
    T: for<'de> Deserialize<'de>,
{
    if bytes.first().copied() != Some(WORKFLOW_FORMAT_VERSION) {
        return Err(StorageError::corruption(format!("{context} version is invalid")).into());
    }

    serde_json::from_slice(&bytes[1..]).map_err(|error| {
        StorageError::corruption(format!("{context} decoding failed: {error}")).into()
    })
}

fn decode_admitted_trigger(
    db: &Db,
    value: &Value,
) -> Result<AdmittedWorkflowTrigger, WorkflowError> {
    let bytes = expect_bytes_value(value, "workflow inbox trigger")?;
    let stored = decode_payload::<StoredAdmittedWorkflowTrigger>(bytes, "workflow inbox trigger")?;
    Ok(AdmittedWorkflowTrigger {
        workflow_instance: stored.workflow_instance,
        trigger_seq: stored.trigger_seq,
        trigger: stored.trigger.into_runtime_trigger(db)?,
        operation_context: stored.operation_context,
    })
}

fn encode_trigger_context(trigger: &WorkflowTrigger) -> Result<Vec<u8>, WorkflowError> {
    encode_payload(
        &StoredWorkflowTrigger::from_runtime_trigger(trigger),
        "workflow context trigger",
    )
}

fn hash_state(state: Option<&Value>) -> Result<u64, WorkflowError> {
    let Some(state) = state else {
        return Ok(0);
    };
    let bytes = serde_json::to_vec(state).map_err(|error| {
        StorageError::corruption(format!("workflow state hash failed: {error}"))
    })?;
    Ok(hash_bytes(&bytes))
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn expect_bytes_value<'a>(value: &'a Value, context: &str) -> Result<&'a [u8], WorkflowError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        Value::Record(_) => {
            Err(StorageError::corruption(format!("{context} expected a byte value")).into())
        }
    }
}

async fn count_rows_at_sequence(
    table: &Table,
    sequence: SequenceNumber,
) -> Result<usize, WorkflowError> {
    let mut rows = table
        .scan_at(
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            sequence,
            ScanOptions::default(),
        )
        .await?;
    let mut count = 0_usize;
    while rows.next().await.is_some() {
        count += 1;
    }
    Ok(count)
}

#[derive(Clone, Debug)]
struct AdmittedWorkflowTrigger {
    workflow_instance: String,
    trigger_seq: u64,
    trigger: WorkflowTrigger,
    operation_context: Option<OperationContext>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredAdmittedWorkflowTrigger {
    workflow_instance: String,
    trigger_seq: u64,
    trigger: StoredWorkflowTrigger,
    operation_context: Option<OperationContext>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredWorkflowTimer {
    workflow_instance: String,
    payload: Vec<u8>,
    operation_context: Option<OperationContext>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StoredWorkflowTrigger {
    Event {
        entry: StoredChangeEntry,
    },
    Timer {
        timer_id: Key,
        fire_at: Timestamp,
        payload: Vec<u8>,
    },
    Callback {
        callback_id: String,
        response: Vec<u8>,
    },
}

impl StoredWorkflowTrigger {
    fn from_runtime_trigger(trigger: &WorkflowTrigger) -> Self {
        match trigger {
            WorkflowTrigger::Event(entry) => Self::Event {
                entry: StoredChangeEntry::from_change_entry(entry),
            },
            WorkflowTrigger::Timer {
                timer_id,
                fire_at,
                payload,
            } => Self::Timer {
                timer_id: timer_id.clone(),
                fire_at: *fire_at,
                payload: payload.clone(),
            },
            WorkflowTrigger::Callback {
                callback_id,
                response,
            } => Self::Callback {
                callback_id: callback_id.clone(),
                response: response.clone(),
            },
        }
    }

    fn into_runtime_trigger(self, db: &Db) -> Result<WorkflowTrigger, WorkflowError> {
        match self {
            Self::Event { entry } => Ok(WorkflowTrigger::Event(entry.into_change_entry(db)?)),
            Self::Timer {
                timer_id,
                fire_at,
                payload,
            } => Ok(WorkflowTrigger::Timer {
                timer_id,
                fire_at,
                payload,
            }),
            Self::Callback {
                callback_id,
                response,
            } => Ok(WorkflowTrigger::Callback {
                callback_id,
                response,
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredChangeEntry {
    source_table: String,
    key: Key,
    value: Option<Value>,
    #[serde(with = "log_cursor_serde")]
    cursor: LogCursor,
    sequence: SequenceNumber,
    kind: ChangeKind,
    operation_context: Option<OperationContext>,
}

impl StoredChangeEntry {
    fn from_change_entry(entry: &ChangeEntry) -> Self {
        Self {
            source_table: entry.table.name().to_string(),
            key: entry.key.clone(),
            value: entry.value.clone(),
            cursor: entry.cursor,
            sequence: entry.sequence,
            kind: entry.kind,
            operation_context: entry.operation_context.clone(),
        }
    }

    fn into_change_entry(self, db: &Db) -> Result<ChangeEntry, StorageError> {
        Ok(ChangeEntry {
            key: self.key,
            value: self.value,
            cursor: self.cursor,
            sequence: self.sequence,
            kind: self.kind,
            table: lookup_table(db, &self.source_table)?,
            operation_context: self.operation_context,
        })
    }
}

mod log_cursor_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    use terracedb::LogCursor;

    pub fn serialize<S>(cursor: &LogCursor, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&cursor.encode())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<LogCursor, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        LogCursor::decode(&bytes).map_err(serde::de::Error::custom)
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
