use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};

use terracedb::{
    ChangeEntry, CommitError, CommitOptions, CompactionStrategy, CreateTableError, Db, LogCursor,
    ReadError, ScanOptions, SequenceNumber, SnapshotTooOld, StorageError, Table, TableConfig,
    TableFormat, Value, WriteBatch,
};

pub const PROJECTION_CURSOR_TABLE_NAME: &str = "_projection_cursors";
const PROJECTION_CURSOR_FORMAT_VERSION: u8 = 1;

#[derive(Debug)]
pub struct ProjectionHandlerError {
    inner: Box<dyn StdError + Send + Sync + 'static>,
}

impl ProjectionHandlerError {
    pub fn new<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(error),
        }
    }
}

impl std::fmt::Display for ProjectionHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl StdError for ProjectionHandlerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.inner.as_ref())
    }
}

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error(transparent)]
    CreateTable(#[from] CreateTableError),
    #[error(transparent)]
    Commit(#[from] CommitError),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    SnapshotTooOld(#[from] SnapshotTooOld),
    #[error("projection runtime subscription closed unexpectedly")]
    SubscriptionClosed,
    #[error("projection {name} is already running")]
    AlreadyRunning { name: String },
    #[error("projection name cannot be empty")]
    EmptyName,
    #[error("projection {name} handler failed")]
    Handler {
        name: String,
        #[source]
        source: ProjectionHandlerError,
    },
    #[error("projection {name} stopped before reaching watermark {target}")]
    StoppedBeforeWatermark {
        name: String,
        target: SequenceNumber,
    },
    #[error("projection {name} failed while waiting for watermark: {reason}")]
    Runtime { name: String, reason: String },
    #[error("projection {name} panicked: {reason}")]
    Panic { name: String, reason: String },
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("projection cursor for {name} is corrupt: {reason}")]
    CursorCorruption { name: String, reason: String },
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[async_trait]
pub trait ProjectionHandler: Send + Sync {
    async fn apply(
        &self,
        run: &ProjectionSequenceRun,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError>;
}

pub struct SingleSourceProjection<H> {
    name: String,
    source: Table,
    outputs: Vec<Table>,
    handler: H,
}

impl<H> SingleSourceProjection<H> {
    pub fn new(name: impl Into<String>, source: Table, handler: H) -> Self {
        Self {
            name: name.into(),
            source,
            outputs: Vec::new(),
            handler,
        }
    }

    pub fn with_outputs<I>(mut self, outputs: I) -> Self
    where
        I: IntoIterator<Item = Table>,
    {
        self.outputs = outputs.into_iter().collect();
        self
    }
}

#[derive(Debug)]
pub struct ProjectionSequenceRun {
    sequence: SequenceNumber,
    first_cursor: LogCursor,
    last_cursor: LogCursor,
    entries: Vec<ChangeEntry>,
}

impl ProjectionSequenceRun {
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    pub fn first_cursor(&self) -> LogCursor {
        self.first_cursor
    }

    pub fn last_cursor(&self) -> LogCursor {
        self.last_cursor
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[ChangeEntry] {
        &self.entries
    }
}

#[derive(Debug)]
pub struct ProjectionTransaction {
    batch: WriteBatch,
    source_cursor: LogCursor,
}

impl ProjectionTransaction {
    fn new(source_cursor: LogCursor) -> Self {
        Self {
            batch: WriteBatch::default(),
            source_cursor,
        }
    }

    pub fn put(&mut self, table: &Table, key: Vec<u8>, value: Value) {
        self.batch.put(table, key, value);
    }

    pub fn merge(&mut self, table: &Table, key: Vec<u8>, value: Value) {
        self.batch.merge(table, key, value);
    }

    pub fn delete(&mut self, table: &Table, key: Vec<u8>) {
        self.batch.delete(table, key);
    }

    pub fn source_cursor(&self) -> LogCursor {
        self.source_cursor
    }

    pub fn source_sequence(&self) -> SequenceNumber {
        self.source_cursor.sequence()
    }

    pub fn operation_count(&self) -> usize {
        self.batch.len()
    }

    fn finish(mut self, cursor_table: &Table, projection_name: &str) -> WriteBatch {
        self.batch.put(
            cursor_table,
            projection_name.as_bytes().to_vec(),
            encode_cursor_value(self.source_cursor),
        );
        self.batch
    }
}

#[derive(Debug)]
pub struct ProjectionRuntime {
    db: Db,
    cursor_table: Table,
    running: Arc<Mutex<BTreeSet<String>>>,
}

impl ProjectionRuntime {
    pub async fn open(db: Db) -> Result<Self, ProjectionError> {
        let cursor_table = ensure_projection_cursor_table(&db).await?;
        Ok(Self {
            db,
            cursor_table,
            running: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    pub async fn load_projection_cursor(&self, name: &str) -> Result<LogCursor, ProjectionError> {
        load_projection_cursor(&self.cursor_table, name).await
    }

    pub async fn scan_whole_sequence_run(
        &self,
        source: &Table,
        cursor: LogCursor,
    ) -> Result<Option<ProjectionSequenceRun>, ProjectionError> {
        scan_whole_sequence_run(&self.db, source, cursor).await
    }

    pub async fn start_single_source<H>(
        &self,
        projection: SingleSourceProjection<H>,
    ) -> Result<ProjectionHandle, ProjectionError>
    where
        H: ProjectionHandler + 'static,
    {
        if projection.name.is_empty() {
            return Err(ProjectionError::EmptyName);
        }

        {
            let mut running = self.running.lock().expect("running-set lock poisoned");
            if !running.insert(projection.name.clone()) {
                return Err(ProjectionError::AlreadyRunning {
                    name: projection.name,
                });
            }
        }

        let initial_cursor = match self.load_projection_cursor(&projection.name).await {
            Ok(cursor) => cursor,
            Err(error) => {
                self.running
                    .lock()
                    .expect("running-set lock poisoned")
                    .remove(&projection.name);
                return Err(error);
            }
        };

        let (watermark_tx, watermark_rx) = watch::channel(initial_cursor.sequence());
        let (status_tx, status_rx) = watch::channel(ProjectionTaskStatus::Running);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let runtime = ProjectionTaskRuntime {
            name: projection.name.clone(),
            source: projection.source,
            outputs: projection.outputs,
            handler: Arc::new(projection.handler),
            db: self.db.clone(),
            cursor_table: self.cursor_table.clone(),
            running: self.running.clone(),
        };

        let task = tokio::spawn(run_projection_task(
            runtime,
            initial_cursor,
            watermark_tx,
            status_tx,
            shutdown_rx,
        ));

        Ok(ProjectionHandle {
            name: projection.name,
            watermark: watermark_rx,
            status: status_rx,
            shutdown: shutdown_tx,
            task,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProjectionTaskStatus {
    Running,
    Stopped,
    Failed(String),
}

struct ProjectionTaskRuntime {
    name: String,
    source: Table,
    outputs: Vec<Table>,
    handler: Arc<dyn ProjectionHandler>,
    db: Db,
    cursor_table: Table,
    running: Arc<Mutex<BTreeSet<String>>>,
}

impl ProjectionTaskRuntime {
    fn release_running_slot(&self) {
        self.running
            .lock()
            .expect("running-set lock poisoned")
            .remove(&self.name);
    }
}

pub struct ProjectionHandle {
    name: String,
    watermark: watch::Receiver<SequenceNumber>,
    status: watch::Receiver<ProjectionTaskStatus>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), ProjectionError>>,
}

impl ProjectionHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn current_watermark(&self) -> SequenceNumber {
        *self.watermark.borrow()
    }

    pub async fn wait_for_watermark(
        &mut self,
        target: SequenceNumber,
    ) -> Result<(), ProjectionError> {
        if self.current_watermark() >= target {
            return Ok(());
        }
        if let Some(error) = self.status_error(target) {
            return Err(error);
        }

        loop {
            tokio::select! {
                changed = self.watermark.changed() => {
                    if changed.is_err() {
                        return Err(ProjectionError::StoppedBeforeWatermark {
                            name: self.name.clone(),
                            target,
                        });
                    }
                    if self.current_watermark() >= target {
                        return Ok(());
                    }
                }
                changed = self.status.changed() => {
                    if changed.is_err() {
                        return Err(ProjectionError::StoppedBeforeWatermark {
                            name: self.name.clone(),
                            target,
                        });
                    }
                    if let Some(error) = self.status_error(target) {
                        return Err(error);
                    }
                }
            }
        }
    }

    pub async fn shutdown(self) -> Result<(), ProjectionError> {
        self.shutdown.send_replace(true);
        self.task.await?
    }

    fn status_error(&self, target: SequenceNumber) -> Option<ProjectionError> {
        match self.status.borrow().clone() {
            ProjectionTaskStatus::Running => None,
            ProjectionTaskStatus::Stopped => Some(ProjectionError::StoppedBeforeWatermark {
                name: self.name.clone(),
                target,
            }),
            ProjectionTaskStatus::Failed(reason) => Some(ProjectionError::Runtime {
                name: self.name.clone(),
                reason,
            }),
        }
    }
}

async fn run_projection_task(
    runtime: ProjectionTaskRuntime,
    initial_cursor: LogCursor,
    watermark_tx: watch::Sender<SequenceNumber>,
    status_tx: watch::Sender<ProjectionTaskStatus>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ProjectionError> {
    let result = AssertUnwindSafe(run_projection_loop(
        &runtime,
        initial_cursor,
        watermark_tx,
        shutdown_rx,
    ))
    .catch_unwind()
    .await
    .unwrap_or_else(|payload| {
        Err(ProjectionError::Panic {
            name: runtime.name.clone(),
            reason: panic_payload_to_string(payload),
        })
    });

    runtime.release_running_slot();
    status_tx.send_replace(match &result {
        Ok(()) => ProjectionTaskStatus::Stopped,
        Err(error) => ProjectionTaskStatus::Failed(error.to_string()),
    });

    result
}

async fn run_projection_loop(
    runtime: &ProjectionTaskRuntime,
    mut cursor: LogCursor,
    watermark_tx: watch::Sender<SequenceNumber>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ProjectionError> {
    let _ = &runtime.outputs;
    watermark_tx.send_replace(cursor.sequence());

    let mut durable_wakes = runtime.db.subscribe_durable(&runtime.source);

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        while let Some(run) = scan_whole_sequence_run(&runtime.db, &runtime.source, cursor).await? {
            let mut tx = ProjectionTransaction::new(run.last_cursor());
            runtime
                .handler
                .apply(&run, &mut tx)
                .await
                .map_err(|source| ProjectionError::Handler {
                    name: runtime.name.clone(),
                    source,
                })?;

            runtime
                .db
                .commit(
                    tx.finish(&runtime.cursor_table, &runtime.name),
                    CommitOptions::default(),
                )
                .await?;

            cursor = run.last_cursor();
            watermark_tx.send_replace(cursor.sequence());

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
                    return Err(ProjectionError::SubscriptionClosed);
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

async fn ensure_projection_cursor_table(db: &Db) -> Result<Table, ProjectionError> {
    match db.create_table(projection_cursor_table_config()).await {
        Ok(table) => Ok(table),
        Err(CreateTableError::AlreadyExists(_)) => Ok(db.table(PROJECTION_CURSOR_TABLE_NAME)),
        Err(error) => Err(error.into()),
    }
}

async fn load_projection_cursor(
    cursor_table: &Table,
    name: &str,
) -> Result<LogCursor, ProjectionError> {
    match cursor_table.read(name.as_bytes().to_vec()).await? {
        Some(Value::Bytes(bytes)) => decode_cursor_value(name, &bytes),
        Some(Value::Record(_)) => Err(ProjectionError::CursorCorruption {
            name: name.to_string(),
            reason: "expected byte cursor payload".to_string(),
        }),
        None => Ok(LogCursor::beginning()),
    }
}

async fn scan_whole_sequence_run(
    db: &Db,
    source: &Table,
    cursor: LogCursor,
) -> Result<Option<ProjectionSequenceRun>, ProjectionError> {
    let mut stream = db
        .scan_durable_since(source, cursor, ScanOptions::default())
        .await?;

    let Some(first) = stream.next().await else {
        return Ok(None);
    };

    let sequence = first.sequence;
    let first_cursor = first.cursor;
    let mut last_cursor = first.cursor;
    let mut entries = vec![first];

    while let Some(entry) = stream.next().await {
        if entry.sequence != sequence {
            break;
        }
        last_cursor = entry.cursor;
        entries.push(entry);
    }

    Ok(Some(ProjectionSequenceRun {
        sequence,
        first_cursor,
        last_cursor,
        entries,
    }))
}

fn projection_cursor_table_config() -> TableConfig {
    TableConfig {
        name: PROJECTION_CURSOR_TABLE_NAME.to_string(),
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

fn encode_cursor_value(cursor: LogCursor) -> Value {
    let mut encoded = Vec::with_capacity(1 + LogCursor::ENCODED_LEN);
    encoded.push(PROJECTION_CURSOR_FORMAT_VERSION);
    encoded.extend_from_slice(&cursor.encode());
    Value::bytes(encoded)
}

fn decode_cursor_value(name: &str, bytes: &[u8]) -> Result<LogCursor, ProjectionError> {
    if bytes.first().copied() != Some(PROJECTION_CURSOR_FORMAT_VERSION) {
        return Err(ProjectionError::CursorCorruption {
            name: name.to_string(),
            reason: "unknown cursor format version".to_string(),
        });
    }

    LogCursor::decode(&bytes[1..]).map_err(|error| ProjectionError::CursorCorruption {
        name: name.to_string(),
        reason: error.to_string(),
    })
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
