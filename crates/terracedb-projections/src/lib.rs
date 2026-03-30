use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    marker::PhantomData,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, TryStreamExt};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};
use tracing::{Instrument, instrument::WithSubscriber};

use terracedb::{
    ChangeEntry, ChangeFeedError, ChangeKind, CommitError, CommitOptions, CompactionStrategy,
    CreateTableError, Db, KvStream, LogCursor, OperationContext, ReadError, ScanOptions,
    SequenceNumber, Snapshot, SnapshotTooOld, SpanRelation, StorageError, SubscriptionClosed,
    Table, TableConfig, TableFormat, Value, WriteBatch, set_span_attribute, telemetry_attrs,
};

pub const PROJECTION_CURSOR_TABLE_NAME: &str = "_projection_cursors";
const PROJECTION_CURSOR_FORMAT_VERSION: u8 = 1;
const PROJECTION_CURSOR_STATE_FORMAT_VERSION: u8 = 2;
const PROJECTION_CURSOR_KEY_SEPARATOR: u8 = 0;
const FULL_SCAN_START: &[u8] = b"";
const FULL_SCAN_END: &[u8] = &[0xff];

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

fn attach_operation_contexts(span: &tracing::Span, contexts: &[OperationContext]) {
    if let Some(parent) = contexts.first() {
        parent.attach_to_span(span, SpanRelation::Parent);
        for context in contexts.iter().skip(1) {
            context.attach_to_span(span, SpanRelation::Link);
        }
    }
}

fn projection_mode_name(mode: ProjectionMode) -> &'static str {
    match mode {
        ProjectionMode::SingleSource => "single_source",
        ProjectionMode::MultiSource => "multi_source",
    }
}

fn projection_status_name(status: &ProjectionTaskStatus) -> &'static str {
    match status {
        ProjectionTaskStatus::Running => "running",
        ProjectionTaskStatus::Stopped => "stopped",
        ProjectionTaskStatus::Failed(_) => "failed",
    }
}

fn apply_projection_span_attributes(
    span: &tracing::Span,
    projection_name: &str,
    mode: ProjectionMode,
    source_count: usize,
    output_count: usize,
) {
    set_span_attribute(
        span,
        telemetry_attrs::PROJECTION_NAME,
        projection_name.to_string(),
    );
    set_span_attribute(
        span,
        telemetry_attrs::PROJECTION_MODE,
        projection_mode_name(mode),
    );
    set_span_attribute(
        span,
        telemetry_attrs::PROJECTION_SOURCE_COUNT,
        source_count as u64,
    );
    set_span_attribute(
        span,
        telemetry_attrs::PROJECTION_OUTPUT_COUNT,
        output_count as u64,
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionTelemetrySnapshot {
    pub projection_name: String,
    pub projection_mode: String,
    pub source_table: String,
    pub source_count: usize,
    pub output_count: usize,
    pub frontier_sequence: SequenceNumber,
    pub durable_sequence: SequenceNumber,
    pub lag: u64,
    pub status: String,
}

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

    fn snapshot_too_old(&self) -> Option<SnapshotTooOld> {
        if let Some(error) = self.inner.downcast_ref::<ProjectionContextError>() {
            return error.snapshot_too_old().cloned();
        }
        if let Some(error) = self.inner.downcast_ref::<ChangeFeedError>() {
            return error.snapshot_too_old().cloned();
        }
        if let Some(error) = self.inner.downcast_ref::<ReadError>() {
            return error.snapshot_too_old().cloned();
        }
        if let Some(error) = self.inner.downcast_ref::<SnapshotTooOld>() {
            return Some(error.clone());
        }

        None
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

impl From<ProjectionContextError> for ProjectionHandlerError {
    fn from(error: ProjectionContextError) -> Self {
        Self::new(error)
    }
}

impl From<ReadError> for ProjectionHandlerError {
    fn from(error: ReadError) -> Self {
        Self::new(error)
    }
}

impl From<ChangeFeedError> for ProjectionHandlerError {
    fn from(error: ChangeFeedError) -> Self {
        Self::new(error)
    }
}

impl From<SnapshotTooOld> for ProjectionHandlerError {
    fn from(error: SnapshotTooOld) -> Self {
        Self::new(error)
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
    #[error(transparent)]
    ChangeFeed(#[from] ChangeFeedError),
    #[error("projection runtime subscription closed unexpectedly")]
    SubscriptionClosed,
    #[error("projection {name} is already running")]
    AlreadyRunning { name: String },
    #[error("projection name cannot be empty")]
    EmptyName,
    #[error("projection {name} must declare at least one source")]
    EmptySources { name: String },
    #[error("projection {name} declares source {table_name} more than once")]
    DuplicateSource { name: String, table_name: String },
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
    #[error("projection {name} stopped before reaching {table}@{target}")]
    StoppedBeforeFrontier {
        name: String,
        table: String,
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
    #[error("projection {name} is not registered or running")]
    UnknownProjection { name: String },
    #[error("projection {name} cannot wait on unsupported source {table}")]
    UnsupportedWaitTarget { name: String, table: String },
    #[error("projection dependency cycle detected at {name}")]
    DependencyCycle { name: String },
    #[error(
        "projection {name} requires recomputation for source {source_name}, but the configured rebuild mode is conservative"
    )]
    UnsupportedRecomputation { name: String, source_name: String },
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl From<SubscriptionClosed> for ProjectionError {
    fn from(_: SubscriptionClosed) -> Self {
        Self::SubscriptionClosed
    }
}

impl ProjectionError {
    pub fn snapshot_too_old(&self) -> Option<&SnapshotTooOld> {
        match self {
            Self::ChangeFeed(error) => error.snapshot_too_old(),
            Self::SnapshotTooOld(error) => Some(error),
            Self::CreateTable(_)
            | Self::Commit(_)
            | Self::Read(_)
            | Self::SubscriptionClosed
            | Self::AlreadyRunning { .. }
            | Self::EmptyName
            | Self::EmptySources { .. }
            | Self::DuplicateSource { .. }
            | Self::Handler { .. }
            | Self::StoppedBeforeWatermark { .. }
            | Self::StoppedBeforeFrontier { .. }
            | Self::Runtime { .. }
            | Self::Panic { .. }
            | Self::Join(_)
            | Self::CursorCorruption { .. }
            | Self::UnknownProjection { .. }
            | Self::UnsupportedWaitTarget { .. }
            | Self::DependencyCycle { .. }
            | Self::UnsupportedRecomputation { .. }
            | Self::Storage(_) => None,
        }
    }
}

/// Controls how the runtime recovers after `SnapshotTooOld`.
///
/// `FailClosed` is the safe default for history-dependent projections.
/// `RebuildFromCurrentState` is only correct when scanning each source table at a
/// pinned frontier fully captures the projection's rebuild input, such as:
///
/// - current-state read models and indexes over mutable tables, or
/// - append-only/event tables whose keys encode the deterministic replay order the
///   projection relies on.
///
/// It does not reconstruct collapsed mutation history from mutable tables.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecomputeStrategy {
    #[default]
    FailClosed,
    RebuildFromCurrentState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionCursorState {
    cursor: LogCursor,
    sequence: SequenceNumber,
}

impl ProjectionCursorState {
    pub fn beginning() -> Self {
        Self {
            cursor: LogCursor::beginning(),
            sequence: SequenceNumber::new(0),
        }
    }

    pub fn cursor(self) -> LogCursor {
        self.cursor
    }

    pub fn sequence(self) -> SequenceNumber {
        self.sequence
    }
}

#[derive(Debug, Error)]
pub enum ProjectionContextError {
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error("table {table} is not part of the projection frontier")]
    MissingFrontier { table: String },
}

impl ProjectionContextError {
    pub fn snapshot_too_old(&self) -> Option<&SnapshotTooOld> {
        match self {
            Self::Read(error) => error.snapshot_too_old(),
            Self::MissingFrontier { .. } => None,
        }
    }
}

#[async_trait]
pub trait ProjectionHandler: Send + Sync {
    /// Compatibility hook for simple projections that only transform the current
    /// batch. New code should prefer `apply_with_context`.
    async fn apply(
        &self,
        _run: &ProjectionSequenceRun,
        _tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        Err(ProjectionHandlerError::new(std::io::Error::other(
            "projection handler must implement apply or apply_with_context",
        )))
    }

    /// Canonical handler entry point for both single-source and multi-source
    /// projections.
    async fn apply_with_context(
        &self,
        run: &ProjectionSequenceRun,
        _ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        self.apply(run, tx).await
    }
}

#[async_trait]
pub trait MultiSourceProjectionHandler: Send + Sync {
    async fn apply(
        &self,
        run: &ProjectionSequenceRun,
        ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError>;
}

#[async_trait]
impl<T> ProjectionHandler for T
where
    T: MultiSourceProjectionHandler + Send + Sync,
{
    async fn apply_with_context(
        &self,
        run: &ProjectionSequenceRun,
        ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        MultiSourceProjectionHandler::apply(self, run, ctx, tx).await
    }
}

pub struct SingleSourceProjection<H> {
    name: String,
    source: Table,
    outputs: Vec<Table>,
    dependencies: Vec<String>,
    recompute: RecomputeStrategy,
    handler: H,
}

impl<H> SingleSourceProjection<H> {
    pub fn new(name: impl Into<String>, source: Table, handler: H) -> Self {
        Self {
            name: name.into(),
            source,
            outputs: Vec::new(),
            dependencies: Vec::new(),
            recompute: RecomputeStrategy::default(),
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

    pub fn with_dependencies<I, S>(mut self, dependencies: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.dependencies = dependencies.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_recompute_strategy(mut self, recompute: RecomputeStrategy) -> Self {
        self.recompute = recompute;
        self
    }
}

pub struct MultiSourceProjection<H> {
    name: String,
    sources: Vec<Table>,
    outputs: Vec<Table>,
    dependencies: Vec<String>,
    recompute: RecomputeStrategy,
    handler: H,
}

impl<H> MultiSourceProjection<H> {
    pub fn new<I>(name: impl Into<String>, sources: I, handler: H) -> Self
    where
        I: IntoIterator<Item = Table>,
    {
        Self {
            name: name.into(),
            sources: sources.into_iter().collect(),
            outputs: Vec::new(),
            dependencies: Vec::new(),
            recompute: RecomputeStrategy::default(),
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

    pub fn with_dependencies<I, S>(mut self, dependencies: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.dependencies = dependencies.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_recompute_strategy(mut self, recompute: RecomputeStrategy) -> Self {
        self.recompute = recompute;
        self
    }
}

#[derive(Debug)]
pub struct ProjectionSequenceRun {
    source: Table,
    sequence: SequenceNumber,
    first_cursor: LogCursor,
    last_cursor: LogCursor,
    entries: Vec<ChangeEntry>,
}

impl ProjectionSequenceRun {
    pub fn source(&self) -> &Table {
        &self.source
    }

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

    fn into_batch(self) -> WriteBatch {
        self.batch
    }
}

#[derive(Clone, Debug)]
pub struct ProjectionContext {
    frontier: BTreeMap<String, (Table, SequenceNumber)>,
    snapshot: Option<Snapshot>,
}

impl ProjectionContext {
    fn new(frontier: BTreeMap<String, (Table, SequenceNumber)>) -> Self {
        Self {
            frontier,
            snapshot: None,
        }
    }

    fn with_snapshot(
        frontier: BTreeMap<String, (Table, SequenceNumber)>,
        snapshot: Snapshot,
    ) -> Self {
        Self {
            frontier,
            snapshot: Some(snapshot),
        }
    }

    pub fn frontier(&self) -> BTreeMap<String, SequenceNumber> {
        self.frontier
            .iter()
            .map(|(name, (_table, sequence))| (name.clone(), *sequence))
            .collect()
    }

    pub fn frontier_sequence(&self, table: &Table) -> Option<SequenceNumber> {
        self.frontier
            .get(table.name())
            .map(|(_table, sequence)| *sequence)
    }

    pub async fn read(
        &self,
        table: &Table,
        key: Vec<u8>,
    ) -> Result<Option<Value>, ProjectionContextError> {
        if self.frontier_sequence(table).is_none() {
            return Err(ProjectionContextError::MissingFrontier {
                table: table.name().to_string(),
            });
        }

        match &self.snapshot {
            Some(snapshot) => snapshot.read(table, key).await.map_err(Into::into),
            None => table
                .read_at(
                    key,
                    self.frontier_sequence(table)
                        .expect("missing frontier already handled"),
                )
                .await
                .map_err(Into::into),
        }
    }

    pub async fn scan(
        &self,
        table: &Table,
        start: Vec<u8>,
        end: Vec<u8>,
        opts: ScanOptions,
    ) -> Result<KvStream, ProjectionContextError> {
        if self.frontier_sequence(table).is_none() {
            return Err(ProjectionContextError::MissingFrontier {
                table: table.name().to_string(),
            });
        }

        match &self.snapshot {
            Some(snapshot) => snapshot
                .scan(table, start, end, opts)
                .await
                .map_err(Into::into),
            None => table
                .scan_at(
                    start,
                    end,
                    self.frontier_sequence(table)
                        .expect("missing frontier already handled"),
                    opts,
                )
                .await
                .map_err(Into::into),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankedMaterializationRange {
    start: Vec<u8>,
    end: Vec<u8>,
}

impl RankedMaterializationRange {
    pub fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self { start, end }
    }
}

impl Default for RankedMaterializationRange {
    fn default() -> Self {
        Self {
            start: FULL_SCAN_START.to_vec(),
            end: FULL_SCAN_END.to_vec(),
        }
    }
}

/// Full-recompute helper for projections that materialize the top `N` rows from a
/// single source table into an owned output range.
///
/// On every applied batch, the helper rescans the configured source range at the
/// projection frontier, ranks rows using the caller-provided ordering hooks, keeps
/// the highest-ranked `N`, and rewrites the configured output range deterministically.
///
/// This intentionally favors correctness and replayability over incremental
/// efficiency. Callers should treat the configured output range as projection-owned.
pub struct RankedMaterializedProjection<Row, Decode, Rank, TieBreak, OutputKey, Encode> {
    source: Table,
    output: Table,
    limit: usize,
    source_range: RankedMaterializationRange,
    output_range: RankedMaterializationRange,
    decode: Decode,
    rank: Rank,
    tie_break: TieBreak,
    output_key: OutputKey,
    encode: Encode,
    _row: PhantomData<fn() -> Row>,
}

impl<Row, Decode, Rank, TieBreak, OutputKey, Encode>
    RankedMaterializedProjection<Row, Decode, Rank, TieBreak, OutputKey, Encode>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: Table,
        output: Table,
        limit: usize,
        decode: Decode,
        rank: Rank,
        tie_break: TieBreak,
        output_key: OutputKey,
        encode: Encode,
    ) -> Self {
        Self {
            source,
            output,
            limit,
            source_range: RankedMaterializationRange::default(),
            output_range: RankedMaterializationRange::default(),
            decode,
            rank,
            tie_break,
            output_key,
            encode,
            _row: PhantomData,
        }
    }

    pub fn with_source_range(mut self, start: Vec<u8>, end: Vec<u8>) -> Self {
        self.source_range = RankedMaterializationRange::new(start, end);
        self
    }

    pub fn with_output_range(mut self, start: Vec<u8>, end: Vec<u8>) -> Self {
        self.output_range = RankedMaterializationRange::new(start, end);
        self
    }
}

struct RankedMaterializedRow<Row, RankKey, TieBreakKey> {
    source_key: Vec<u8>,
    row: Row,
    rank_key: RankKey,
    tie_break_key: TieBreakKey,
}

#[async_trait]
impl<Row, Decode, Rank, TieBreak, OutputKey, Encode, RankKey, TieBreakKey>
    MultiSourceProjectionHandler
    for RankedMaterializedProjection<Row, Decode, Rank, TieBreak, OutputKey, Encode>
where
    Row: Send + Sync,
    Decode: Fn(Vec<u8>, Value) -> Result<Option<Row>, ProjectionHandlerError> + Send + Sync,
    Rank: Fn(&Row) -> RankKey + Send + Sync,
    TieBreak: Fn(&Row) -> TieBreakKey + Send + Sync,
    OutputKey: Fn(usize, &Row) -> Vec<u8> + Send + Sync,
    Encode: Fn(&Row) -> Result<Value, ProjectionHandlerError> + Send + Sync,
    RankKey: Ord + Send,
    TieBreakKey: Ord + Send,
{
    async fn apply(
        &self,
        _run: &ProjectionSequenceRun,
        ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        let mut source_rows = ctx
            .scan(
                &self.source,
                self.source_range.start.clone(),
                self.source_range.end.clone(),
                ScanOptions::default(),
            )
            .await?;
        let mut ranked_rows = Vec::new();

        while let Some((source_key, value)) = source_rows.next().await {
            let Some(row) = (self.decode)(source_key.clone(), value)? else {
                continue;
            };
            let rank_key = (self.rank)(&row);
            let tie_break_key = (self.tie_break)(&row);
            ranked_rows.push(RankedMaterializedRow {
                source_key,
                row,
                rank_key,
                tie_break_key,
            });
        }

        ranked_rows.sort_by(|left, right| {
            right
                .rank_key
                .cmp(&left.rank_key)
                .then_with(|| left.tie_break_key.cmp(&right.tie_break_key))
                .then_with(|| left.source_key.cmp(&right.source_key))
        });
        ranked_rows.truncate(self.limit);

        let mut next_rows = Vec::with_capacity(ranked_rows.len());
        let mut next_keys = BTreeSet::new();
        for (index, ranked_row) in ranked_rows.into_iter().enumerate() {
            let output_key = (self.output_key)(index, &ranked_row.row);
            if !next_keys.insert(output_key.clone()) {
                return Err(ProjectionHandlerError::new(std::io::Error::other(format!(
                    "ranked projection {} produced duplicate output key {:?}",
                    self.output.name(),
                    output_key
                ))));
            }
            let value = (self.encode)(&ranked_row.row)?;
            next_rows.push((output_key, value));
        }

        let mut existing_rows = self
            .output
            .scan(
                self.output_range.start.clone(),
                self.output_range.end.clone(),
                ScanOptions::default(),
            )
            .await?;
        while let Some((key, _value)) = existing_rows.next().await {
            if !next_keys.contains(&key) {
                tx.delete(&self.output, key);
            }
        }

        for (key, value) in next_rows {
            tx.put(&self.output, key, value);
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct ProjectionRuntime {
    db: Db,
    cursor_table: Table,
    running: Arc<Mutex<BTreeSet<String>>>,
    monitors: Arc<Mutex<BTreeMap<String, ProjectionMonitor>>>,
}

impl std::fmt::Debug for ProjectionRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionRuntime")
            .field("cursor_table", &self.cursor_table)
            .finish()
    }
}

impl ProjectionRuntime {
    pub async fn open(db: Db) -> Result<Self, ProjectionError> {
        let span = tracing::info_span!("terracedb.projection.runtime.open");
        set_span_attribute(&span, telemetry_attrs::DB_NAME, db.telemetry_db_name());
        set_span_attribute(
            &span,
            telemetry_attrs::DB_INSTANCE,
            db.telemetry_db_instance(),
        );
        set_span_attribute(
            &span,
            telemetry_attrs::STORAGE_MODE,
            db.telemetry_storage_mode(),
        );

        async move {
            let cursor_table = ensure_projection_cursor_table(&db).await?;
            Ok(Self {
                db,
                cursor_table,
                running: Arc::new(Mutex::new(BTreeSet::new())),
                monitors: Arc::new(Mutex::new(BTreeMap::new())),
            })
        }
        .instrument(span.clone())
        .await
    }

    pub async fn load_projection_cursor(&self, name: &str) -> Result<LogCursor, ProjectionError> {
        Ok(self
            .load_projection_cursor_state_by_name(name, None)
            .await?
            .cursor())
    }

    pub async fn load_projection_cursor_state(
        &self,
        name: &str,
        source: &Table,
    ) -> Result<ProjectionCursorState, ProjectionError> {
        self.load_projection_cursor_state_by_name(name, Some(source.name()))
            .await
    }

    pub async fn load_projection_frontier<'a, I>(
        &self,
        name: &str,
        sources: I,
    ) -> Result<BTreeMap<String, ProjectionCursorState>, ProjectionError>
    where
        I: IntoIterator<Item = &'a Table>,
    {
        let mut frontier = BTreeMap::new();
        for source in sources {
            frontier.insert(
                source.name().to_string(),
                self.load_projection_cursor_state(name, source).await?,
            );
        }
        Ok(frontier)
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
        self.start_projection(ProjectionSpec {
            name: projection.name,
            sources: vec![projection.source],
            outputs: projection.outputs,
            dependencies: projection.dependencies,
            recompute: projection.recompute,
            mode: ProjectionMode::SingleSource,
            handler: Arc::new(projection.handler),
        })
        .await
    }

    pub async fn start_multi_source<H>(
        &self,
        projection: MultiSourceProjection<H>,
    ) -> Result<ProjectionHandle, ProjectionError>
    where
        H: ProjectionHandler + 'static,
    {
        self.start_projection(ProjectionSpec {
            name: projection.name,
            sources: projection.sources,
            outputs: projection.outputs,
            dependencies: projection.dependencies,
            recompute: projection.recompute,
            mode: ProjectionMode::MultiSource,
            handler: Arc::new(projection.handler),
        })
        .await
    }

    pub fn current_frontier(
        &self,
        name: &str,
    ) -> Result<BTreeMap<String, SequenceNumber>, ProjectionError> {
        Ok(self.monitor(name)?.frontier.borrow().clone())
    }

    pub fn telemetry_snapshots(&self) -> Vec<ProjectionTelemetrySnapshot> {
        let monitors = self
            .monitors
            .lock()
            .expect("projection monitors lock poisoned")
            .clone();
        let mut snapshots = Vec::new();
        for (name, monitor) in monitors {
            let frontier = monitor.frontier.borrow().clone();
            let status = projection_status_name(&monitor.status.borrow().clone()).to_string();
            for source_name in &monitor.metadata.sources {
                let source = self.db.table(source_name);
                let durable_sequence = self.db.subscribe_durable(&source).current();
                let frontier_sequence = frontier
                    .get(source_name.as_str())
                    .copied()
                    .unwrap_or(SequenceNumber::new(0));
                snapshots.push(ProjectionTelemetrySnapshot {
                    projection_name: name.clone(),
                    projection_mode: projection_mode_name(monitor.metadata.mode).to_string(),
                    source_table: source_name.clone(),
                    source_count: monitor.metadata.sources.len(),
                    output_count: monitor.metadata.outputs.len(),
                    frontier_sequence,
                    durable_sequence,
                    lag: durable_sequence
                        .get()
                        .saturating_sub(frontier_sequence.get()),
                    status: status.clone(),
                });
            }
        }
        snapshots
    }

    /// Waits for visible projection output whose recorded source frontier covers the
    /// requested sequences.
    ///
    /// Exact transitive waits are supported for dependency chains whose nodes are
    /// single-source. When a dependency path includes a multi-source node, the wait
    /// remains conservative unless the requested source is part of the current
    /// projection's direct frontier.
    pub async fn wait_for_frontier<'a, I>(
        &self,
        name: &str,
        targets: I,
    ) -> Result<(), ProjectionError>
    where
        I: IntoIterator<Item = (&'a Table, SequenceNumber)>,
    {
        let requested = targets
            .into_iter()
            .map(|(table, sequence)| (table.name().to_string(), sequence))
            .collect::<BTreeMap<_, _>>();
        let mut visiting = BTreeSet::new();
        self.wait_for_frontier_inner(name.to_string(), requested, &mut visiting)
            .await
    }

    async fn start_projection(
        &self,
        projection: ProjectionSpec,
    ) -> Result<ProjectionHandle, ProjectionError> {
        if projection.name.is_empty() {
            return Err(ProjectionError::EmptyName);
        }
        if projection.sources.is_empty() {
            return Err(ProjectionError::EmptySources {
                name: projection.name,
            });
        }

        let mut seen_sources = BTreeSet::new();
        for source in &projection.sources {
            if !seen_sources.insert(source.name().to_string()) {
                return Err(ProjectionError::DuplicateSource {
                    name: projection.name.clone(),
                    table_name: source.name().to_string(),
                });
            }
        }

        {
            let mut running = self.running.lock().expect("running-set lock poisoned");
            if !running.insert(projection.name.clone()) {
                return Err(ProjectionError::AlreadyRunning {
                    name: projection.name,
                });
            }
        }

        let mut source_states = Vec::with_capacity(projection.sources.len());
        for (declaration_index, source) in projection.sources.iter().cloned().enumerate() {
            let persisted = self
                .load_projection_cursor_state_for_start(
                    &projection.name,
                    &source,
                    projection.mode == ProjectionMode::SingleSource,
                )
                .await;
            let persisted = match persisted {
                Ok(state) => state,
                Err(error) => {
                    self.release_running_slot(&projection.name);
                    return Err(error);
                }
            };

            source_states.push(SourceRuntimeState {
                table: source,
                cursor: persisted.cursor(),
                sequence: persisted.sequence(),
                declaration_index,
            });
        }

        let initial_frontier = frontier_from_states(&source_states);
        let (frontier_tx, frontier_rx) = watch::channel(initial_frontier);
        let (status_tx, status_rx) = watch::channel(ProjectionTaskStatus::Running);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let metadata = ProjectionMetadata {
            sources: projection
                .sources
                .iter()
                .map(|source| source.name().to_string())
                .collect(),
            outputs: projection
                .outputs
                .iter()
                .map(|table| table.name().to_string())
                .collect(),
            dependencies: projection.dependencies.clone(),
            mode: projection.mode,
        };
        self.monitors
            .lock()
            .expect("projection monitors lock poisoned")
            .insert(
                projection.name.clone(),
                ProjectionMonitor {
                    metadata: metadata.clone(),
                    frontier: frontier_rx.clone(),
                    status: status_rx.clone(),
                },
            );

        let runtime = ProjectionTaskRuntime {
            name: projection.name.clone(),
            sources: source_states,
            outputs: projection.outputs,
            recompute: projection.recompute,
            mode: projection.mode,
            handler: projection.handler,
            db: self.db.clone(),
            cursor_table: self.cursor_table.clone(),
            running: self.running.clone(),
        };

        let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
        let task = tokio::spawn(
            run_projection_task(runtime, frontier_tx, status_tx, shutdown_rx)
                .with_subscriber(dispatch),
        );

        Ok(ProjectionHandle {
            name: projection.name,
            primary_source: metadata.sources[0].clone(),
            runtime: self.clone(),
            frontier: frontier_rx,
            status: status_rx,
            shutdown: shutdown_tx,
            task,
        })
    }

    fn monitor(&self, name: &str) -> Result<ProjectionMonitor, ProjectionError> {
        self.monitors
            .lock()
            .expect("projection monitors lock poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| ProjectionError::UnknownProjection {
                name: name.to_string(),
            })
    }

    fn release_running_slot(&self, name: &str) {
        self.running
            .lock()
            .expect("running-set lock poisoned")
            .remove(name);
    }

    async fn load_projection_cursor_state_by_name(
        &self,
        name: &str,
        source_name: Option<&str>,
    ) -> Result<ProjectionCursorState, ProjectionError> {
        load_projection_cursor_state(&self.cursor_table, name, source_name).await
    }

    async fn load_projection_cursor_state_for_start(
        &self,
        name: &str,
        source: &Table,
        legacy_single_source: bool,
    ) -> Result<ProjectionCursorState, ProjectionError> {
        if legacy_single_source {
            match load_projection_cursor_state(&self.cursor_table, name, None).await? {
                state if state != ProjectionCursorState::beginning() => return Ok(state),
                _ => {}
            }
        }

        load_projection_cursor_state(&self.cursor_table, name, Some(source.name())).await
    }

    fn reachable_sources(
        &self,
        name: &str,
        visiting: &mut BTreeSet<String>,
    ) -> Result<BTreeSet<String>, ProjectionError> {
        if !visiting.insert(name.to_string()) {
            return Err(ProjectionError::DependencyCycle {
                name: name.to_string(),
            });
        }

        let monitor = self.monitor(name)?;
        let mut sources = monitor
            .metadata
            .sources
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for dependency in &monitor.metadata.dependencies {
            sources.extend(self.reachable_sources(dependency, visiting)?);
        }

        visiting.remove(name);
        Ok(sources)
    }

    fn direct_wait_error(
        &self,
        name: &str,
        status: ProjectionTaskStatus,
        targets: &BTreeMap<String, SequenceNumber>,
        frontier: &BTreeMap<String, SequenceNumber>,
    ) -> Option<ProjectionError> {
        let first_unmet = targets.iter().find(|(table, target)| {
            frontier
                .get(table.as_str())
                .copied()
                .unwrap_or(SequenceNumber::new(0))
                < **target
        });

        match status {
            ProjectionTaskStatus::Running => None,
            ProjectionTaskStatus::Stopped => {
                first_unmet.map(|(table, target)| ProjectionError::StoppedBeforeFrontier {
                    name: name.to_string(),
                    table: table.clone(),
                    target: *target,
                })
            }
            ProjectionTaskStatus::Failed(reason) => Some(ProjectionError::Runtime {
                name: name.to_string(),
                reason,
            }),
        }
    }

    async fn wait_for_direct_frontier(
        &self,
        name: &str,
        targets: BTreeMap<String, SequenceNumber>,
    ) -> Result<(), ProjectionError> {
        if targets.is_empty() {
            return Ok(());
        }

        let monitor = self.monitor(name)?;
        for table in targets.keys() {
            if !monitor
                .metadata
                .sources
                .iter()
                .any(|source| source == table)
            {
                return Err(ProjectionError::UnsupportedWaitTarget {
                    name: name.to_string(),
                    table: table.clone(),
                });
            }
        }

        let mut frontier = monitor.frontier.clone();
        let mut status = monitor.status.clone();
        {
            let current_frontier = frontier.borrow();
            if frontier_satisfies(&current_frontier, &targets) {
                return Ok(());
            }
        }
        if let Some(error) = {
            let current_frontier = frontier.borrow();
            self.direct_wait_error(name, status.borrow().clone(), &targets, &current_frontier)
        } {
            return Err(error);
        }

        loop {
            tokio::select! {
                changed = frontier.changed() => {
                    if changed.is_err() {
                        let current_frontier = frontier.borrow();
                        if let Some(error) = self.direct_wait_error(
                            name,
                            status.borrow().clone(),
                            &targets,
                            &current_frontier,
                        ) {
                            return Err(error);
                        }
                        return Err(ProjectionError::StoppedBeforeFrontier {
                            name: name.to_string(),
                            table: targets.keys().next().cloned().unwrap_or_default(),
                            target: targets.values().next().copied().unwrap_or(SequenceNumber::new(0)),
                        });
                    }
                    let current_frontier = frontier.borrow();
                    if frontier_satisfies(&current_frontier, &targets) {
                        return Ok(());
                    }
                }
                changed = status.changed() => {
                    if changed.is_err() {
                        let current_frontier = frontier.borrow();
                        if let Some(error) = self.direct_wait_error(
                            name,
                            status.borrow().clone(),
                            &targets,
                            &current_frontier,
                        ) {
                            return Err(error);
                        }
                        return Err(ProjectionError::StoppedBeforeFrontier {
                            name: name.to_string(),
                            table: targets.keys().next().cloned().unwrap_or_default(),
                            target: targets.values().next().copied().unwrap_or(SequenceNumber::new(0)),
                        });
                    }
                    let current_frontier = frontier.borrow();
                    if let Some(error) = self.direct_wait_error(
                        name,
                        status.borrow().clone(),
                        &targets,
                        &current_frontier,
                    ) {
                        return Err(error);
                    }
                }
            }
        }
    }

    fn wait_for_frontier_inner<'a>(
        &'a self,
        name: String,
        requested: BTreeMap<String, SequenceNumber>,
        visiting: &'a mut BTreeSet<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), ProjectionError>> + Send + 'a>> {
        Box::pin(async move {
            let monitor = self.monitor(&name)?;
            let source_set = monitor
                .metadata
                .sources
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();

            let mut direct_targets = BTreeMap::new();
            let mut remaining = BTreeMap::new();
            for (table, sequence) in requested {
                if source_set.contains(&table) {
                    merge_target(&mut direct_targets, table, sequence);
                } else {
                    remaining.insert(table, sequence);
                }
            }

            if !visiting.insert(name.clone()) {
                return Err(ProjectionError::DependencyCycle { name });
            }

            for dependency in &monitor.metadata.dependencies {
                if remaining.is_empty() {
                    break;
                }

                let dependency_roots = self.reachable_sources(dependency, &mut BTreeSet::new())?;
                let covered = remaining
                    .iter()
                    .filter(|(table, _sequence)| dependency_roots.contains(table.as_str()))
                    .map(|(table, sequence)| (table.clone(), *sequence))
                    .collect::<BTreeMap<_, _>>();
                if covered.is_empty() {
                    continue;
                }

                self.wait_for_frontier_inner(dependency.clone(), covered.clone(), visiting)
                    .await?;

                let dependency_monitor = self.monitor(dependency)?;
                for source in monitor
                    .metadata
                    .sources
                    .iter()
                    .filter(|source| dependency_monitor.metadata.outputs.contains(*source))
                {
                    let source_table = lookup_table(&self.db, source)?;
                    merge_target(
                        &mut direct_targets,
                        source.clone(),
                        self.db.subscribe(&source_table).current(),
                    );
                }

                for covered_table in covered.keys() {
                    remaining.remove(covered_table);
                }
            }

            visiting.remove(&name);

            if let Some((table, _sequence)) = remaining.into_iter().next() {
                return Err(ProjectionError::UnsupportedWaitTarget { name, table });
            }

            self.wait_for_direct_frontier(&name, direct_targets).await
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProjectionTaskStatus {
    Running,
    Stopped,
    Failed(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProjectionMode {
    SingleSource,
    MultiSource,
}

#[derive(Clone)]
struct ProjectionMonitor {
    metadata: ProjectionMetadata,
    frontier: watch::Receiver<BTreeMap<String, SequenceNumber>>,
    status: watch::Receiver<ProjectionTaskStatus>,
}

#[derive(Clone, Debug)]
struct ProjectionMetadata {
    sources: Vec<String>,
    outputs: Vec<String>,
    dependencies: Vec<String>,
    mode: ProjectionMode,
}

struct ProjectionSpec {
    name: String,
    sources: Vec<Table>,
    outputs: Vec<Table>,
    dependencies: Vec<String>,
    recompute: RecomputeStrategy,
    mode: ProjectionMode,
    handler: Arc<dyn ProjectionHandler>,
}

struct ProjectionTaskRuntime {
    name: String,
    sources: Vec<SourceRuntimeState>,
    outputs: Vec<Table>,
    recompute: RecomputeStrategy,
    mode: ProjectionMode,
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

#[derive(Clone, Debug)]
struct SourceRuntimeState {
    table: Table,
    cursor: LogCursor,
    sequence: SequenceNumber,
    declaration_index: usize,
}

pub struct ProjectionHandle {
    name: String,
    primary_source: String,
    runtime: ProjectionRuntime,
    frontier: watch::Receiver<BTreeMap<String, SequenceNumber>>,
    status: watch::Receiver<ProjectionTaskStatus>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), ProjectionError>>,
}

impl ProjectionHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn current_watermark(&self) -> SequenceNumber {
        self.frontier
            .borrow()
            .get(self.primary_source.as_str())
            .copied()
            .unwrap_or(SequenceNumber::new(0))
    }

    pub fn current_frontier(&self) -> BTreeMap<String, SequenceNumber> {
        self.frontier.borrow().clone()
    }

    pub async fn wait_for_watermark(
        &mut self,
        target: SequenceNumber,
    ) -> Result<(), ProjectionError> {
        let table = lookup_table(&self.runtime.db, &self.primary_source)?;
        self.wait_for_sources([(&table, target)]).await
    }

    pub async fn wait_for_sources<'a, I>(&mut self, targets: I) -> Result<(), ProjectionError>
    where
        I: IntoIterator<Item = (&'a Table, SequenceNumber)>,
    {
        self.runtime.wait_for_frontier(&self.name, targets).await
    }

    pub async fn shutdown(self) -> Result<(), ProjectionError> {
        self.shutdown.send_replace(true);
        self.task.await?
    }

    #[allow(dead_code)]
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
    mut runtime: ProjectionTaskRuntime,
    frontier_tx: watch::Sender<BTreeMap<String, SequenceNumber>>,
    status_tx: watch::Sender<ProjectionTaskStatus>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ProjectionError> {
    let span = tracing::info_span!("terracedb.projection.runtime");
    set_span_attribute(
        &span,
        telemetry_attrs::DB_NAME,
        runtime.db.telemetry_db_name(),
    );
    set_span_attribute(
        &span,
        telemetry_attrs::DB_INSTANCE,
        runtime.db.telemetry_db_instance(),
    );
    set_span_attribute(
        &span,
        telemetry_attrs::STORAGE_MODE,
        runtime.db.telemetry_storage_mode(),
    );
    apply_projection_span_attributes(
        &span,
        &runtime.name,
        runtime.mode,
        runtime.sources.len(),
        runtime.outputs.len(),
    );

    async move {
        let result = AssertUnwindSafe(run_projection_loop(&mut runtime, frontier_tx, shutdown_rx))
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
    .instrument(span.clone())
    .await
}

async fn run_projection_loop(
    runtime: &mut ProjectionTaskRuntime,
    frontier_tx: watch::Sender<BTreeMap<String, SequenceNumber>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ProjectionError> {
    frontier_tx.send_replace(frontier_from_states(&runtime.sources));

    let source_tables = runtime
        .sources
        .iter()
        .map(|source| &source.table)
        .collect::<Vec<_>>();
    let mut durable_wakes = runtime.db.subscribe_durable_set(source_tables);

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        while drain_next_ready_run(runtime, &frontier_tx).await? {
            if *shutdown_rx.borrow() {
                return Ok(());
            }
        }

        if *shutdown_rx.borrow() {
            return Ok(());
        }

        tokio::select! {
            changed = durable_wakes.changed() => {
                if changed.is_err() {
                    return Err(ProjectionError::SubscriptionClosed);
                }
                tracing::debug!(
                    projection = %runtime.name,
                    "projection durable wake"
                );
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn drain_next_ready_run(
    runtime: &mut ProjectionTaskRuntime,
    frontier_tx: &watch::Sender<BTreeMap<String, SequenceNumber>>,
) -> Result<bool, ProjectionError> {
    let mut ready = Vec::new();
    let mut snapshot_too_old_source = None;

    for (index, source) in runtime.sources.iter().enumerate() {
        match scan_whole_sequence_run(&runtime.db, &source.table, source.cursor).await {
            Ok(Some(run)) => ready.push((index, run)),
            Ok(None) => {}
            Err(error) if error.snapshot_too_old().is_some() => {
                snapshot_too_old_source = Some(index);
                break;
            }
            Err(error) => return Err(error),
        }
    }

    if let Some(index) = snapshot_too_old_source {
        if runtime.recompute == RecomputeStrategy::FailClosed {
            return Err(ProjectionError::UnsupportedRecomputation {
                name: runtime.name.clone(),
                source_name: runtime.sources[index].table.name().to_string(),
            });
        }

        rebuild_from_current_state(runtime, frontier_tx).await?;
        return Ok(true);
    }

    let Some((chosen_index, run)) = ready.into_iter().min_by_key(|(index, run)| {
        (
            run.sequence().get(),
            runtime.sources[*index].declaration_index,
        )
    }) else {
        return Ok(false);
    };

    let operation_contexts = collect_operation_contexts(run.entries());
    let span = tracing::info_span!("terracedb.projection.batch");
    apply_projection_span_attributes(
        &span,
        &runtime.name,
        runtime.mode,
        runtime.sources.len(),
        runtime.outputs.len(),
    );
    set_span_attribute(
        &span,
        telemetry_attrs::SOURCE_TABLE,
        run.source().name().to_string(),
    );
    set_span_attribute(&span, telemetry_attrs::SEQUENCE, run.sequence().get());
    set_span_attribute(
        &span,
        telemetry_attrs::LOG_CURSOR,
        format!(
            "{}:{}",
            run.last_cursor().sequence().get(),
            run.last_cursor().op_index()
        ),
    );
    set_span_attribute(
        &span,
        "terracedb.projection.batch.entry_count",
        run.len() as u64,
    );
    attach_operation_contexts(&span, &operation_contexts);
    let span_for_attrs = span.clone();

    async move {
        let chosen_source = runtime.sources[chosen_index].table.clone();
        let mut frontier = frontier_from_states(&runtime.sources);
        frontier.insert(chosen_source.name().to_string(), run.sequence());
        let context = ProjectionContext::new(
            runtime
                .sources
                .iter()
                .map(|source| {
                    let sequence = frontier
                        .get(source.table.name())
                        .copied()
                        .unwrap_or(source.sequence);
                    (
                        source.table.name().to_string(),
                        (source.table.clone(), sequence),
                    )
                })
                .collect(),
        );

        let mut tx = ProjectionTransaction::new(run.last_cursor());
        if let Err(source) = runtime
            .handler
            .apply_with_context(&run, &context, &mut tx)
            .await
        {
            if source.snapshot_too_old().is_some() {
                if runtime.recompute == RecomputeStrategy::FailClosed {
                    return Err(ProjectionError::UnsupportedRecomputation {
                        name: runtime.name.clone(),
                        source_name: chosen_source.name().to_string(),
                    });
                }

                rebuild_from_current_state(runtime, frontier_tx).await?;
                return Ok(true);
            }

            return Err(ProjectionError::Handler {
                name: runtime.name.clone(),
                source,
            });
        }

        let operation_count = tx.operation_count();
        let mut batch = tx.into_batch();
        let new_state = ProjectionCursorState {
            cursor: run.last_cursor(),
            sequence: run.sequence(),
        };
        stage_projection_cursor_state(
            &mut batch,
            &runtime.cursor_table,
            &runtime.name,
            &chosen_source,
            new_state,
            runtime.mode == ProjectionMode::SingleSource,
        );

        set_span_attribute(
            &span_for_attrs,
            telemetry_attrs::PROJECTION_OPERATION_COUNT,
            operation_count as u64,
        );
        runtime
            .db
            .commit(batch, CommitOptions::default().with_current_context())
            .await?;

        runtime.sources[chosen_index].cursor = new_state.cursor();
        runtime.sources[chosen_index].sequence = new_state.sequence();
        frontier_tx.send_replace(frontier_from_states(&runtime.sources));

        Ok(true)
    }
    .instrument(span.clone())
    .await
}

async fn rebuild_from_current_state(
    runtime: &mut ProjectionTaskRuntime,
    frontier_tx: &watch::Sender<BTreeMap<String, SequenceNumber>>,
) -> Result<(), ProjectionError> {
    let span = tracing::info_span!("terracedb.projection.rebuild");
    apply_projection_span_attributes(
        &span,
        &runtime.name,
        runtime.mode,
        runtime.sources.len(),
        runtime.outputs.len(),
    );
    set_span_attribute(
        &span,
        telemetry_attrs::WORK_KIND,
        "rebuild_from_current_state",
    );

    async move {
        let rebuild_frontier = runtime
            .sources
            .iter()
            .map(|source| {
                (
                    source.table.name().to_string(),
                    runtime.db.subscribe_durable(&source.table).current(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let durable_snapshot = runtime.db.durable_snapshot().await;

        let mut reset_batch = WriteBatch::default();
        for output in &runtime.outputs {
            clear_table(output, &mut reset_batch).await?;
        }
        for source in &runtime.sources {
            stage_projection_cursor_state(
                &mut reset_batch,
                &runtime.cursor_table,
                &runtime.name,
                &source.table,
                ProjectionCursorState::beginning(),
                runtime.mode == ProjectionMode::SingleSource,
            );
        }
        if !reset_batch.is_empty() {
            runtime
                .db
                .commit(reset_batch, CommitOptions::default())
                .await?;
        }

        for source in &mut runtime.sources {
            source.cursor = LogCursor::beginning();
            source.sequence = SequenceNumber::new(0);
        }
        frontier_tx.send_replace(frontier_from_states(&runtime.sources));

        for index in 0..runtime.sources.len() {
            let source_table = runtime.sources[index].table.clone();
            let target_sequence = rebuild_frontier
                .get(source_table.name())
                .copied()
                .unwrap_or(SequenceNumber::new(0));
            let synthetic_runs =
                build_recompute_runs(&durable_snapshot, &source_table, target_sequence).await?;
            if synthetic_runs.is_empty() {
                if target_sequence == SequenceNumber::new(0) {
                    continue;
                }

                let empty_state = ProjectionCursorState {
                    cursor: LogCursor::new(target_sequence, u16::MAX),
                    sequence: target_sequence,
                };
                let mut batch = WriteBatch::default();
                stage_projection_cursor_state(
                    &mut batch,
                    &runtime.cursor_table,
                    &runtime.name,
                    &source_table,
                    empty_state,
                    runtime.mode == ProjectionMode::SingleSource,
                );
                runtime.db.commit(batch, CommitOptions::default()).await?;
                runtime.sources[index].cursor = empty_state.cursor();
                runtime.sources[index].sequence = empty_state.sequence();
                frontier_tx.send_replace(frontier_from_states(&runtime.sources));
                continue;
            }

            for run in synthetic_runs {
                let context = ProjectionContext::with_snapshot(
                    runtime
                        .sources
                        .iter()
                        .map(|source| {
                            let sequence = rebuild_frontier
                                .get(source.table.name())
                                .copied()
                                .unwrap_or(source.sequence);
                            (
                                source.table.name().to_string(),
                                (source.table.clone(), sequence),
                            )
                        })
                        .collect(),
                    durable_snapshot.clone(),
                );
                let mut tx = ProjectionTransaction::new(run.last_cursor());
                runtime
                    .handler
                    .apply_with_context(&run, &context, &mut tx)
                    .await
                    .map_err(|source| ProjectionError::Handler {
                        name: runtime.name.clone(),
                        source,
                    })?;

                let mut batch = tx.into_batch();
                let new_state = ProjectionCursorState {
                    cursor: run.last_cursor(),
                    sequence: run.sequence(),
                };
                stage_projection_cursor_state(
                    &mut batch,
                    &runtime.cursor_table,
                    &runtime.name,
                    &source_table,
                    new_state,
                    runtime.mode == ProjectionMode::SingleSource,
                );
                runtime.db.commit(batch, CommitOptions::default()).await?;
                runtime.sources[index].cursor = new_state.cursor();
                runtime.sources[index].sequence = new_state.sequence();
                frontier_tx.send_replace(frontier_from_states(&runtime.sources));
            }
        }

        Ok(())
    }
    .instrument(span.clone())
    .await
}

async fn clear_table(table: &Table, batch: &mut WriteBatch) -> Result<(), ProjectionError> {
    let mut rows = table
        .scan(
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            ScanOptions::default(),
        )
        .await?;

    while let Some((key, _value)) = rows.next().await {
        batch.delete(table, key);
    }

    Ok(())
}

async fn build_recompute_runs(
    snapshot: &Snapshot,
    source: &Table,
    logical_sequence: SequenceNumber,
) -> Result<Vec<ProjectionSequenceRun>, ProjectionError> {
    if logical_sequence == SequenceNumber::new(0) {
        return Ok(Vec::new());
    }

    let mut rows = snapshot
        .scan(
            source,
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            ScanOptions::default(),
        )
        .await?;
    let mut entries = Vec::new();
    while let Some((key, value)) = rows.next().await {
        entries.push((key, value));
    }

    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut runs = Vec::new();
    for chunk in entries.chunks(usize::from(u16::MAX) + 1) {
        let first_index = runs.len() * (usize::from(u16::MAX) + 1);
        let last_index = first_index + chunk.len() - 1;
        let first_cursor = LogCursor::new(
            logical_sequence,
            u16::try_from(first_index).map_err(|_| {
                StorageError::unsupported("projection recomputation chunk index exceeds u16")
            })?,
        );
        let last_cursor = LogCursor::new(
            logical_sequence,
            u16::try_from(last_index).map_err(|_| {
                StorageError::unsupported("projection recomputation chunk index exceeds u16")
            })?,
        );
        let entries = chunk
            .iter()
            .enumerate()
            .map(|(offset, (key, value))| ChangeEntry {
                key: key.clone(),
                value: Some(value.clone()),
                cursor: LogCursor::new(
                    logical_sequence,
                    u16::try_from(first_index + offset).expect("chunk offset already bounded"),
                ),
                sequence: logical_sequence,
                kind: ChangeKind::Put,
                table: source.clone(),
                operation_context: None,
            })
            .collect();

        runs.push(ProjectionSequenceRun {
            source: source.clone(),
            sequence: logical_sequence,
            first_cursor,
            last_cursor,
            entries,
        });
    }

    Ok(runs)
}

async fn ensure_projection_cursor_table(db: &Db) -> Result<Table, ProjectionError> {
    match db.create_table(projection_cursor_table_config()).await {
        Ok(table) => Ok(table),
        Err(CreateTableError::AlreadyExists(_)) => {
            lookup_table(db, PROJECTION_CURSOR_TABLE_NAME).map_err(Into::into)
        }
        Err(error) => Err(error.into()),
    }
}

fn lookup_table(db: &Db, name: &str) -> Result<Table, StorageError> {
    db.try_table(name)
        .ok_or_else(|| StorageError::not_found(format!("table does not exist: {name}")))
}

async fn load_projection_cursor_state(
    cursor_table: &Table,
    name: &str,
    source_name: Option<&str>,
) -> Result<ProjectionCursorState, ProjectionError> {
    let key = match source_name {
        Some(source_name) => projection_source_cursor_key(name, source_name),
        None => legacy_projection_cursor_key(name),
    };

    match cursor_table.read(key).await? {
        Some(Value::Bytes(bytes)) => decode_cursor_state_value(name, source_name, &bytes),
        Some(Value::Record(_)) => Err(ProjectionError::CursorCorruption {
            name: cursor_label(name, source_name),
            reason: "expected byte cursor payload".to_string(),
        }),
        None => Ok(ProjectionCursorState::beginning()),
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

    let Some(first) = stream
        .try_next()
        .await
        .map_err(ChangeFeedError::Storage)
        .map_err(ProjectionError::ChangeFeed)?
    else {
        return Ok(None);
    };

    let sequence = first.sequence;
    let first_cursor = first.cursor;
    let mut last_cursor = first.cursor;
    let mut entries = vec![first];

    while let Some(entry) = stream
        .try_next()
        .await
        .map_err(ChangeFeedError::Storage)
        .map_err(ProjectionError::ChangeFeed)?
    {
        if entry.sequence != sequence {
            break;
        }
        last_cursor = entry.cursor;
        entries.push(entry);
    }

    Ok(Some(ProjectionSequenceRun {
        source: source.clone(),
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

fn legacy_projection_cursor_key(name: &str) -> Vec<u8> {
    name.as_bytes().to_vec()
}

fn projection_source_cursor_key(name: &str, source_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.len() + 1 + source_name.len());
    key.extend_from_slice(name.as_bytes());
    key.push(PROJECTION_CURSOR_KEY_SEPARATOR);
    key.extend_from_slice(source_name.as_bytes());
    key
}

fn stage_projection_cursor_state(
    batch: &mut WriteBatch,
    cursor_table: &Table,
    projection_name: &str,
    source: &Table,
    state: ProjectionCursorState,
    write_legacy: bool,
) {
    batch.put(
        cursor_table,
        projection_source_cursor_key(projection_name, source.name()),
        encode_cursor_state_value(state),
    );

    if write_legacy {
        batch.put(
            cursor_table,
            legacy_projection_cursor_key(projection_name),
            encode_cursor_value(state.cursor()),
        );
    }
}

fn encode_cursor_value(cursor: LogCursor) -> Value {
    let mut encoded = Vec::with_capacity(1 + LogCursor::ENCODED_LEN);
    encoded.push(PROJECTION_CURSOR_FORMAT_VERSION);
    encoded.extend_from_slice(&cursor.encode());
    Value::bytes(encoded)
}

fn encode_cursor_state_value(state: ProjectionCursorState) -> Value {
    let mut encoded = Vec::with_capacity(1 + LogCursor::ENCODED_LEN + 8);
    encoded.push(PROJECTION_CURSOR_STATE_FORMAT_VERSION);
    encoded.extend_from_slice(&state.cursor().encode());
    encoded.extend_from_slice(&state.sequence().get().to_be_bytes());
    Value::bytes(encoded)
}

fn decode_cursor_state_value(
    name: &str,
    source_name: Option<&str>,
    bytes: &[u8],
) -> Result<ProjectionCursorState, ProjectionError> {
    match bytes.first().copied() {
        Some(PROJECTION_CURSOR_FORMAT_VERSION) => {
            decode_legacy_cursor_value(name, source_name, bytes)
        }
        Some(PROJECTION_CURSOR_STATE_FORMAT_VERSION) => {
            let encoded_len = 1 + LogCursor::ENCODED_LEN + 8;
            if bytes.len() != encoded_len {
                return Err(ProjectionError::CursorCorruption {
                    name: cursor_label(name, source_name),
                    reason: format!(
                        "invalid cursor state length: expected {encoded_len} bytes, got {}",
                        bytes.len()
                    ),
                });
            }

            let cursor =
                LogCursor::decode(&bytes[1..1 + LogCursor::ENCODED_LEN]).map_err(|error| {
                    ProjectionError::CursorCorruption {
                        name: cursor_label(name, source_name),
                        reason: error.to_string(),
                    }
                })?;
            let mut sequence = [0_u8; 8];
            sequence.copy_from_slice(&bytes[1 + LogCursor::ENCODED_LEN..]);
            Ok(ProjectionCursorState {
                cursor,
                sequence: SequenceNumber::new(u64::from_be_bytes(sequence)),
            })
        }
        _ => Err(ProjectionError::CursorCorruption {
            name: cursor_label(name, source_name),
            reason: "unknown cursor format version".to_string(),
        }),
    }
}

fn decode_legacy_cursor_value(
    name: &str,
    source_name: Option<&str>,
    bytes: &[u8],
) -> Result<ProjectionCursorState, ProjectionError> {
    if bytes.first().copied() != Some(PROJECTION_CURSOR_FORMAT_VERSION) {
        return Err(ProjectionError::CursorCorruption {
            name: cursor_label(name, source_name),
            reason: "unknown cursor format version".to_string(),
        });
    }

    let cursor =
        LogCursor::decode(&bytes[1..]).map_err(|error| ProjectionError::CursorCorruption {
            name: cursor_label(name, source_name),
            reason: error.to_string(),
        })?;
    Ok(ProjectionCursorState {
        cursor,
        sequence: cursor.sequence(),
    })
}

fn frontier_from_states(states: &[SourceRuntimeState]) -> BTreeMap<String, SequenceNumber> {
    states
        .iter()
        .map(|source| (source.table.name().to_string(), source.sequence))
        .collect()
}

fn frontier_satisfies(
    frontier: &BTreeMap<String, SequenceNumber>,
    targets: &BTreeMap<String, SequenceNumber>,
) -> bool {
    targets.iter().all(|(table, target)| {
        frontier
            .get(table.as_str())
            .copied()
            .unwrap_or(SequenceNumber::new(0))
            >= *target
    })
}

fn merge_target(
    targets: &mut BTreeMap<String, SequenceNumber>,
    table: String,
    sequence: SequenceNumber,
) {
    let entry = targets.entry(table).or_insert(SequenceNumber::new(0));
    *entry = (*entry).max(sequence);
}

fn cursor_label(name: &str, source_name: Option<&str>) -> String {
    match source_name {
        Some(source_name) => format!("{name}/{source_name}"),
        None => name.to_string(),
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
