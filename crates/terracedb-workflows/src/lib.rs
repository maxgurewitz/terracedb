use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error as StdError,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};
use tracing::{Instrument, instrument::WithSubscriber};

use terracedb::{
    ChangeEntry, ChangeFeedError, ChangeKind, Clock, CommitError, CreateTableError, Db,
    DurableTimerSet, Key, LogCursor, ObjectStore, OperationContext, OutboxEntry, ReadError,
    ScanOptions, ScheduledTimer, SnapshotTooOld, StorageError, StorageErrorKind, Table,
    TableConfig, TableFormat, Timestamp, Transaction, TransactionCommitError, TransactionalOutbox,
    Value,
};
use terracedb::{
    CompactionStrategy, SequenceNumber, SpanRelation, set_span_attribute, telemetry_attrs,
};

pub mod failpoints;

pub const DEFAULT_TIMER_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub const DEFAULT_SOURCE_BATCH_LIMIT: usize = 128;
pub const DEFAULT_TIMER_BATCH_LIMIT: usize = 128;
/// Replayable workflow sources need some retained table history so fail-closed recovery can
/// resume from a durable fence without immediately surfacing `SnapshotTooOld`.
pub const DEFAULT_REPLAYABLE_SOURCE_HISTORY_RETENTION_SEQUENCES: u64 = 128;

const WORKFLOW_FORMAT_VERSION: u8 = 1;
const WORKFLOW_SOURCE_PROGRESS_FORMAT_VERSION: u8 = 2;
const WORKFLOW_TRIGGER_ORDER_FORMAT_VERSION: u8 = 1;
const WORKFLOW_TRIGGER_JOURNAL_HEAD_FORMAT_VERSION: u8 = 1;
const WORKFLOW_CHECKPOINT_ARTIFACT_FORMAT_VERSION: u8 = 1;
const WORKFLOW_CHECKPOINT_MANIFEST_FORMAT_VERSION: u8 = 1;
const WORKFLOW_CHECKPOINT_LATEST_POINTER_FORMAT_VERSION: u8 = 1;
const WORKFLOW_TABLE_PREFIX: &str = "_workflow_";
const INBOX_KEY_SEPARATOR: u8 = 0;
const TRIGGER_JOURNAL_ENTRY_PREFIX: u8 = 0;
const TRIGGER_JOURNAL_HEAD_KEY: &[u8] = &[0xff];
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
    #[error("workflow {name} does not have a checkpoint store configured")]
    MissingCheckpointStore { name: String },
    #[error("workflow {name} is already running")]
    AlreadyRunning { name: String },
    #[error("workflow {name} cannot restore a checkpoint while running")]
    RestoreWhileRunning { name: String },
    #[error("workflow {name} subscription closed unexpectedly")]
    SubscriptionClosed { name: String },
    #[error("workflow {name} checkpoint store failed")]
    CheckpointStore {
        name: String,
        #[source]
        source: WorkflowCheckpointStoreError,
    },
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
            | Self::MissingCheckpointStore { .. }
            | Self::AlreadyRunning { .. }
            | Self::RestoreWhileRunning { .. }
            | Self::SubscriptionClosed { .. }
            | Self::CheckpointStore { .. }
            | Self::Handler { .. }
            | Self::Panic { .. } => None,
        }
    }
}

/// First-attach behavior for a workflow source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowSourceBootstrapPolicy {
    /// Replay retained history from the start of the source.
    #[default]
    Beginning,
    /// Skip historical backlog and attach live from the current durable frontier.
    CurrentDurable,
    /// Restore from checkpoint when available, otherwise replay from the beginning.
    CheckpointOrBeginning,
    /// Restore from checkpoint when available, otherwise attach live from current durable.
    CheckpointOrCurrentDurable,
}

/// Recovery behavior when persisted workflow source progress is no longer resumable.
///
/// `FailClosed` is the safe default. More permissive modes must be selected
/// deliberately because they may drop history or depend on replay/checkpoint
/// support outside the source table itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowSourceRecoveryPolicy {
    #[default]
    FailClosed,
    RestoreCheckpoint,
    RestoreCheckpointOrFastForward,
    ReplayFromHistory,
    FastForwardToCurrentDurable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowReplayableSourceKind {
    #[default]
    NonReplayable,
    AppendOnlyOrdered,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowHistoricalArtifactSupport {
    #[default]
    Unsupported,
    Optional,
    Required,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSourceCapabilities {
    pub replay: WorkflowReplayableSourceKind,
    pub checkpoint: WorkflowHistoricalArtifactSupport,
    pub trigger_journal: WorkflowHistoricalArtifactSupport,
}

impl WorkflowSourceCapabilities {
    pub fn replayable_append_only() -> Self {
        Self {
            replay: WorkflowReplayableSourceKind::AppendOnlyOrdered,
            ..Self::default()
        }
    }

    pub fn supports_checkpoint_restore(self) -> bool {
        self.checkpoint != WorkflowHistoricalArtifactSupport::Unsupported
    }

    pub fn supports_trigger_journal(self) -> bool {
        self.trigger_journal != WorkflowHistoricalArtifactSupport::Unsupported
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSourceConfig {
    pub bootstrap: WorkflowSourceBootstrapPolicy,
    pub recovery: WorkflowSourceRecoveryPolicy,
    pub capabilities: WorkflowSourceCapabilities,
}

impl WorkflowSourceConfig {
    pub fn historical_replayable_source() -> Self {
        Self::default()
            .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::Beginning)
            .with_recovery_policy(WorkflowSourceRecoveryPolicy::ReplayFromHistory)
            .with_capabilities(WorkflowSourceCapabilities::replayable_append_only())
    }

    pub fn live_only_current_state_source() -> Self {
        Self::default()
            .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CurrentDurable)
            .with_recovery_policy(WorkflowSourceRecoveryPolicy::FailClosed)
    }

    pub fn live_only_replayable_append_only_source() -> Self {
        Self::live_only_current_state_source()
            .with_capabilities(WorkflowSourceCapabilities::replayable_append_only())
    }

    pub fn with_bootstrap_policy(mut self, bootstrap: WorkflowSourceBootstrapPolicy) -> Self {
        self.bootstrap = bootstrap;
        self
    }

    pub fn with_recovery_policy(mut self, recovery: WorkflowSourceRecoveryPolicy) -> Self {
        self.recovery = recovery;
        self
    }

    pub fn with_capabilities(mut self, capabilities: WorkflowSourceCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn with_replay_kind(mut self, replay: WorkflowReplayableSourceKind) -> Self {
        self.capabilities.replay = replay;
        self
    }

    /// Applies the table-level defaults needed for the configured source mode.
    ///
    /// In particular, replayable append-only sources need retained row history so
    /// `CurrentDurable` bootstrap and fail-closed recovery can safely resume from their
    /// durable-sequence fences.
    pub fn prepare_source_table_config(self, mut config: TableConfig) -> TableConfig {
        if self.capabilities.replay == WorkflowReplayableSourceKind::AppendOnlyOrdered
            && config.history_retention_sequences.is_none()
        {
            config.history_retention_sequences =
                Some(DEFAULT_REPLAYABLE_SOURCE_HISTORY_RETENTION_SEQUENCES);
        }
        config
    }

    pub fn with_checkpoint_support(
        mut self,
        checkpoint: WorkflowHistoricalArtifactSupport,
    ) -> Self {
        self.capabilities.checkpoint = checkpoint;
        self
    }

    pub fn with_trigger_journal_support(
        mut self,
        trigger_journal: WorkflowHistoricalArtifactSupport,
    ) -> Self {
        self.capabilities.trigger_journal = trigger_journal;
        self
    }

    pub fn initial_resolution(
        self,
        checkpoint_available: bool,
        current_durable_sequence: SequenceNumber,
    ) -> WorkflowHistoricalSourceResolution {
        match self.bootstrap {
            WorkflowSourceBootstrapPolicy::Beginning => {
                WorkflowHistoricalSourceResolution::AttachFromBeginning
            }
            WorkflowSourceBootstrapPolicy::CurrentDurable => {
                WorkflowHistoricalSourceResolution::AttachFromCurrentDurable {
                    durable_sequence: current_durable_sequence,
                }
            }
            WorkflowSourceBootstrapPolicy::CheckpointOrBeginning => {
                if checkpoint_available {
                    WorkflowHistoricalSourceResolution::RestoreCheckpoint
                } else {
                    WorkflowHistoricalSourceResolution::AttachFromBeginning
                }
            }
            WorkflowSourceBootstrapPolicy::CheckpointOrCurrentDurable => {
                if checkpoint_available {
                    WorkflowHistoricalSourceResolution::RestoreCheckpoint
                } else {
                    WorkflowHistoricalSourceResolution::AttachFromCurrentDurable {
                        durable_sequence: current_durable_sequence,
                    }
                }
            }
        }
    }

    pub fn recovery_resolution(
        self,
        checkpoint_available: bool,
        current_durable_sequence: SequenceNumber,
    ) -> WorkflowHistoricalSourceResolution {
        match self.recovery {
            WorkflowSourceRecoveryPolicy::FailClosed => {
                WorkflowHistoricalSourceResolution::FailClosedSnapshotTooOld
            }
            WorkflowSourceRecoveryPolicy::RestoreCheckpoint => {
                WorkflowHistoricalSourceResolution::RestoreCheckpoint
            }
            WorkflowSourceRecoveryPolicy::RestoreCheckpointOrFastForward => {
                if checkpoint_available {
                    WorkflowHistoricalSourceResolution::RestoreCheckpoint
                } else {
                    WorkflowHistoricalSourceResolution::FastForwardToCurrentDurable {
                        durable_sequence: current_durable_sequence,
                    }
                }
            }
            WorkflowSourceRecoveryPolicy::ReplayFromHistory => {
                WorkflowHistoricalSourceResolution::ReplayFromHistory
            }
            WorkflowSourceRecoveryPolicy::FastForwardToCurrentDurable => {
                WorkflowHistoricalSourceResolution::FastForwardToCurrentDurable {
                    durable_sequence: current_durable_sequence,
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkflowSource {
    table: Table,
    config: WorkflowSourceConfig,
}

impl WorkflowSource {
    pub fn new(table: Table) -> Self {
        Self {
            table,
            config: WorkflowSourceConfig::default(),
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn config(&self) -> &WorkflowSourceConfig {
        &self.config
    }

    pub fn with_config(mut self, config: WorkflowSourceConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_bootstrap_policy(mut self, bootstrap: WorkflowSourceBootstrapPolicy) -> Self {
        self.config.bootstrap = bootstrap;
        self
    }

    pub fn with_recovery_policy(mut self, recovery: WorkflowSourceRecoveryPolicy) -> Self {
        self.config.recovery = recovery;
        self
    }

    pub fn with_capabilities(mut self, capabilities: WorkflowSourceCapabilities) -> Self {
        self.config.capabilities = capabilities;
        self
    }

    pub fn with_replay_kind(mut self, replay: WorkflowReplayableSourceKind) -> Self {
        self.config.capabilities.replay = replay;
        self
    }

    pub fn with_checkpoint_support(
        mut self,
        checkpoint: WorkflowHistoricalArtifactSupport,
    ) -> Self {
        self.config.capabilities.checkpoint = checkpoint;
        self
    }

    pub fn with_trigger_journal_support(
        mut self,
        trigger_journal: WorkflowHistoricalArtifactSupport,
    ) -> Self {
        self.config.capabilities.trigger_journal = trigger_journal;
        self
    }

    pub fn initial_resolution(
        &self,
        checkpoint_available: bool,
        current_durable_sequence: SequenceNumber,
    ) -> WorkflowHistoricalSourceResolution {
        self.config
            .initial_resolution(checkpoint_available, current_durable_sequence)
    }

    pub fn recovery_resolution(
        &self,
        checkpoint_available: bool,
        current_durable_sequence: SequenceNumber,
    ) -> WorkflowHistoricalSourceResolution {
        self.config
            .recovery_resolution(checkpoint_available, current_durable_sequence)
    }
}

impl From<Table> for WorkflowSource {
    fn from(table: Table) -> Self {
        Self::new(table)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowHistoricalEvent {
    FirstAttach,
    SnapshotTooOld,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowSourceAttachMode {
    Historical,
    LiveOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowHistoricalSourceResolution {
    AttachFromBeginning,
    AttachFromCurrentDurable { durable_sequence: SequenceNumber },
    RestoreCheckpoint,
    ReplayFromHistory,
    FastForwardToCurrentDurable { durable_sequence: SequenceNumber },
    FailClosedSnapshotTooOld,
}

impl WorkflowHistoricalSourceResolution {
    pub fn attach_mode(self) -> Option<WorkflowSourceAttachMode> {
        match self {
            Self::AttachFromBeginning | Self::RestoreCheckpoint | Self::ReplayFromHistory => {
                Some(WorkflowSourceAttachMode::Historical)
            }
            Self::AttachFromCurrentDurable { .. } | Self::FastForwardToCurrentDurable { .. } => {
                Some(WorkflowSourceAttachMode::LiveOnly)
            }
            Self::FailClosedSnapshotTooOld => None,
        }
    }

    pub fn is_lossy(self) -> bool {
        matches!(self, Self::FastForwardToCurrentDurable { .. })
    }

    pub fn surfaces_snapshot_too_old(self) -> bool {
        matches!(self, Self::FailClosedSnapshotTooOld)
    }

    pub fn progress_origin(self) -> Option<WorkflowSourceProgressOrigin> {
        match self {
            Self::AttachFromBeginning => Some(WorkflowSourceProgressOrigin::BeginningBootstrap),
            Self::AttachFromCurrentDurable { .. } => {
                Some(WorkflowSourceProgressOrigin::CurrentDurableBootstrap)
            }
            Self::RestoreCheckpoint => Some(WorkflowSourceProgressOrigin::CheckpointRestore),
            Self::ReplayFromHistory => Some(WorkflowSourceProgressOrigin::ReplayFromHistory),
            Self::FastForwardToCurrentDurable { .. } => {
                Some(WorkflowSourceProgressOrigin::FastForwardToCurrentDurable)
            }
            Self::FailClosedSnapshotTooOld => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowHistoricalSourceScenario {
    pub source_name: String,
    pub config: WorkflowSourceConfig,
    pub event: WorkflowHistoricalEvent,
    pub checkpoint_available: bool,
    pub current_durable_sequence: SequenceNumber,
}

impl WorkflowHistoricalSourceScenario {
    pub fn new(
        source_name: impl Into<String>,
        config: WorkflowSourceConfig,
        event: WorkflowHistoricalEvent,
        current_durable_sequence: SequenceNumber,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            config,
            event,
            checkpoint_available: false,
            current_durable_sequence,
        }
    }

    pub fn with_checkpoint_available(mut self, checkpoint_available: bool) -> Self {
        self.checkpoint_available = checkpoint_available;
        self
    }

    pub fn resolve(&self) -> WorkflowHistoricalSourceResolution {
        match self.event {
            WorkflowHistoricalEvent::FirstAttach => self
                .config
                .initial_resolution(self.checkpoint_available, self.current_durable_sequence),
            WorkflowHistoricalEvent::SnapshotTooOld => self
                .config
                .recovery_resolution(self.checkpoint_available, self.current_durable_sequence),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowSourceProgressOrigin {
    #[default]
    DurableCursor,
    BeginningBootstrap,
    CurrentDurableBootstrap,
    CheckpointRestore,
    ReplayFromHistory,
    FastForwardToCurrentDurable,
}

impl WorkflowSourceProgressOrigin {
    pub fn attach_mode(self) -> Option<WorkflowSourceAttachMode> {
        match self {
            Self::DurableCursor => None,
            Self::BeginningBootstrap | Self::CheckpointRestore | Self::ReplayFromHistory => {
                Some(WorkflowSourceAttachMode::Historical)
            }
            Self::CurrentDurableBootstrap | Self::FastForwardToCurrentDurable => {
                Some(WorkflowSourceAttachMode::LiveOnly)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkflowSourceResumePoint {
    Cursor {
        #[serde(with = "log_cursor_serde")]
        cursor: LogCursor,
    },
    DurableSequenceFence {
        sequence: SequenceNumber,
    },
}

impl WorkflowSourceResumePoint {
    pub fn sequence(self) -> SequenceNumber {
        match self {
            Self::Cursor { cursor } => cursor.sequence(),
            Self::DurableSequenceFence { sequence } => sequence,
        }
    }

    pub fn as_log_cursor(self) -> LogCursor {
        match self {
            Self::Cursor { cursor } => cursor,
            // A durable-sequence fence means "skip everything durable through this sequence".
            Self::DurableSequenceFence { sequence } => LogCursor::new(sequence, u16::MAX),
        }
    }

    fn order_key(self) -> (SequenceNumber, u32) {
        match self {
            Self::Cursor { cursor } => (cursor.sequence(), u32::from(cursor.op_index())),
            Self::DurableSequenceFence { sequence } => (sequence, u32::MAX),
        }
    }
}

impl PartialOrd for WorkflowSourceResumePoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorkflowSourceResumePoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_key().cmp(&other.order_key())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSourceProgress {
    resume_from: WorkflowSourceResumePoint,
    origin: WorkflowSourceProgressOrigin,
}

impl Default for WorkflowSourceProgress {
    fn default() -> Self {
        Self::from_cursor(LogCursor::beginning())
    }
}

impl WorkflowSourceProgress {
    pub fn from_cursor(cursor: LogCursor) -> Self {
        Self {
            resume_from: WorkflowSourceResumePoint::Cursor { cursor },
            origin: WorkflowSourceProgressOrigin::DurableCursor,
        }
    }

    pub fn from_durable_sequence(sequence: SequenceNumber) -> Self {
        Self {
            resume_from: WorkflowSourceResumePoint::DurableSequenceFence { sequence },
            origin: WorkflowSourceProgressOrigin::CurrentDurableBootstrap,
        }
    }

    pub fn resume_point(self) -> WorkflowSourceResumePoint {
        self.resume_from
    }

    pub fn as_log_cursor(self) -> LogCursor {
        self.resume_from.as_log_cursor()
    }

    pub fn sequence(self) -> SequenceNumber {
        self.resume_from.sequence()
    }

    pub fn origin(self) -> WorkflowSourceProgressOrigin {
        self.origin
    }

    pub fn with_origin(mut self, origin: WorkflowSourceProgressOrigin) -> Self {
        self.origin = origin;
        self
    }

    pub fn encode(self) -> Result<Vec<u8>, StorageError> {
        encode_workflow_source_progress(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        decode_workflow_source_progress(bytes)
    }
}

impl PartialOrd for WorkflowSourceProgress {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorkflowSourceProgress {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.resume_from
            .cmp(&other.resume_from)
            .then_with(|| self.origin.cmp(&other.origin))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkflowCheckpointRestoreOnOpen {
    #[default]
    Disabled,
    Latest,
    Specific(WorkflowCheckpointId),
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct WorkflowCheckpointId(u64);

impl WorkflowCheckpointId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for WorkflowCheckpointId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:06}", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowCheckpointArtifactKind {
    State,
    Inbox,
    TriggerOrder,
    SourceProgress,
    TimerSchedule,
    TimerLookup,
    Outbox,
    TriggerJournal,
}

impl WorkflowCheckpointArtifactKind {
    pub fn filename_stem(self) -> &'static str {
        match self {
            Self::State => "state",
            Self::Inbox => "inbox",
            Self::TriggerOrder => "trigger-order",
            Self::SourceProgress => "source-progress",
            Self::TimerSchedule => "timer-schedule",
            Self::TimerLookup => "timer-lookup",
            Self::Outbox => "outbox",
            Self::TriggerJournal => "trigger-journal",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCheckpointArtifact {
    pub kind: WorkflowCheckpointArtifactKind,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowCheckpointArtifactPayload {
    pub kind: WorkflowCheckpointArtifactKind,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCheckpointManifest {
    pub workflow_name: String,
    pub checkpoint_id: WorkflowCheckpointId,
    pub captured_at: Timestamp,
    pub source_frontier: BTreeMap<String, WorkflowSourceProgress>,
    #[serde(default)]
    pub trigger_journal_high_watermark: Option<u64>,
    pub artifacts: Vec<WorkflowCheckpointArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCheckpointLayout {
    pub workflow_name: String,
    pub checkpoint_id: WorkflowCheckpointId,
}

impl WorkflowCheckpointLayout {
    pub fn new(workflow_name: impl Into<String>, checkpoint_id: WorkflowCheckpointId) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            checkpoint_id,
        }
    }

    pub fn latest_pointer_path_for(workflow_name: impl AsRef<str>) -> String {
        format!(
            "workflow/{}/checkpoints/LATEST.json",
            workflow_name.as_ref()
        )
    }

    pub fn checkpoint_prefix(&self) -> String {
        format!(
            "workflow/{}/checkpoints/CHK-{}",
            self.workflow_name, self.checkpoint_id
        )
    }

    pub fn latest_pointer_path(&self) -> String {
        Self::latest_pointer_path_for(&self.workflow_name)
    }

    pub fn manifest_path(&self) -> String {
        format!("{}/MANIFEST.json", self.checkpoint_prefix())
    }

    pub fn artifact_path(&self, kind: WorkflowCheckpointArtifactKind) -> String {
        format!("{}/{}.bin", self.checkpoint_prefix(), kind.filename_stem())
    }

    pub fn manifest_with_frontier(
        &self,
        captured_at: Timestamp,
        source_frontier: BTreeMap<String, WorkflowSourceProgress>,
        artifact_kinds: impl IntoIterator<Item = WorkflowCheckpointArtifactKind>,
    ) -> WorkflowCheckpointManifest {
        WorkflowCheckpointManifest {
            workflow_name: self.workflow_name.clone(),
            checkpoint_id: self.checkpoint_id,
            captured_at,
            source_frontier,
            trigger_journal_high_watermark: None,
            artifacts: artifact_kinds
                .into_iter()
                .map(|kind| WorkflowCheckpointArtifact {
                    kind,
                    path: self.artifact_path(kind),
                })
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct WorkflowCheckpointStoreError {
    inner: Box<dyn StdError + Send + Sync + 'static>,
}

impl WorkflowCheckpointStoreError {
    pub fn new<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(error),
        }
    }
}

impl std::fmt::Display for WorkflowCheckpointStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl StdError for WorkflowCheckpointStoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.inner.as_ref())
    }
}

#[async_trait]
pub trait WorkflowCheckpointStore: Send + Sync {
    async fn load_latest_manifest(
        &self,
        workflow_name: &str,
    ) -> Result<Option<WorkflowCheckpointManifest>, WorkflowCheckpointStoreError>;

    async fn load_manifest(
        &self,
        workflow_name: &str,
        checkpoint_id: WorkflowCheckpointId,
    ) -> Result<Option<WorkflowCheckpointManifest>, WorkflowCheckpointStoreError>;

    async fn read_artifact(
        &self,
        workflow_name: &str,
        checkpoint_id: WorkflowCheckpointId,
        kind: WorkflowCheckpointArtifactKind,
    ) -> Result<Option<Vec<u8>>, WorkflowCheckpointStoreError>;

    async fn publish_checkpoint(
        &self,
        manifest: WorkflowCheckpointManifest,
        artifacts: Vec<WorkflowCheckpointArtifactPayload>,
    ) -> Result<(), WorkflowCheckpointStoreError>;
}

#[derive(Clone)]
pub struct WorkflowObjectStoreCheckpointStore {
    object_store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl WorkflowObjectStoreCheckpointStore {
    pub fn new(object_store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            object_store,
            prefix: prefix.into(),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn latest_manifest_key(&self, workflow_name: &str) -> String {
        self.object_key(&WorkflowCheckpointLayout::latest_pointer_path_for(
            workflow_name,
        ))
    }

    fn object_key(&self, relative: &str) -> String {
        join_object_key(&self.prefix, relative)
    }
}

impl std::fmt::Debug for WorkflowObjectStoreCheckpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowObjectStoreCheckpointStore")
            .field("prefix", &self.prefix)
            .field("object_store", &"<dyn ObjectStore>")
            .finish()
    }
}

#[async_trait]
impl WorkflowCheckpointStore for WorkflowObjectStoreCheckpointStore {
    async fn load_latest_manifest(
        &self,
        workflow_name: &str,
    ) -> Result<Option<WorkflowCheckpointManifest>, WorkflowCheckpointStoreError> {
        let latest_key = self.latest_manifest_key(workflow_name);
        let bytes = match self.object_store.get(&latest_key).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(WorkflowCheckpointStoreError::new(error)),
        };
        let latest = decode_versioned_json::<WorkflowCheckpointLatestPointer>(
            WORKFLOW_CHECKPOINT_LATEST_POINTER_FORMAT_VERSION,
            &bytes,
            "workflow checkpoint latest pointer",
        )
        .map_err(WorkflowCheckpointStoreError::new)?;
        self.load_manifest(workflow_name, latest.checkpoint_id)
            .await
    }

    async fn load_manifest(
        &self,
        workflow_name: &str,
        checkpoint_id: WorkflowCheckpointId,
    ) -> Result<Option<WorkflowCheckpointManifest>, WorkflowCheckpointStoreError> {
        let layout = WorkflowCheckpointLayout::new(workflow_name, checkpoint_id);
        let bytes = match self
            .object_store
            .get(&self.object_key(&layout.manifest_path()))
            .await
        {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(WorkflowCheckpointStoreError::new(error)),
        };
        let manifest = decode_versioned_json::<WorkflowCheckpointManifest>(
            WORKFLOW_CHECKPOINT_MANIFEST_FORMAT_VERSION,
            &bytes,
            "workflow checkpoint manifest",
        )
        .map_err(WorkflowCheckpointStoreError::new)?;
        Ok(Some(manifest))
    }

    async fn read_artifact(
        &self,
        workflow_name: &str,
        checkpoint_id: WorkflowCheckpointId,
        kind: WorkflowCheckpointArtifactKind,
    ) -> Result<Option<Vec<u8>>, WorkflowCheckpointStoreError> {
        let layout = WorkflowCheckpointLayout::new(workflow_name, checkpoint_id);
        match self
            .object_store
            .get(&self.object_key(&layout.artifact_path(kind)))
            .await
        {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
            Err(error) => Err(WorkflowCheckpointStoreError::new(error)),
        }
    }

    async fn publish_checkpoint(
        &self,
        manifest: WorkflowCheckpointManifest,
        artifacts: Vec<WorkflowCheckpointArtifactPayload>,
    ) -> Result<(), WorkflowCheckpointStoreError> {
        let layout = WorkflowCheckpointLayout::new(&manifest.workflow_name, manifest.checkpoint_id);
        let mut payloads = artifacts
            .into_iter()
            .map(|artifact| (artifact.kind, artifact.bytes))
            .collect::<BTreeMap<_, _>>();

        for artifact in &manifest.artifacts {
            let expected_path = layout.artifact_path(artifact.kind);
            if artifact.path != expected_path {
                return Err(WorkflowCheckpointStoreError::new(StorageError::corruption(
                    format!(
                        "checkpoint artifact {:?} expected path {expected_path}, got {}",
                        artifact.kind, artifact.path
                    ),
                )));
            }
            let Some(bytes) = payloads.remove(&artifact.kind) else {
                return Err(WorkflowCheckpointStoreError::new(StorageError::corruption(
                    format!(
                        "checkpoint artifact payload for {:?} is missing",
                        artifact.kind
                    ),
                )));
            };
            self.object_store
                .put(&self.object_key(&artifact.path), &bytes)
                .await
                .map_err(WorkflowCheckpointStoreError::new)?;
        }

        if !payloads.is_empty() {
            return Err(WorkflowCheckpointStoreError::new(StorageError::corruption(
                "checkpoint payload set contains unexpected artifacts",
            )));
        }

        let manifest_bytes = encode_versioned_json(
            WORKFLOW_CHECKPOINT_MANIFEST_FORMAT_VERSION,
            &manifest,
            "workflow checkpoint manifest",
        )
        .map_err(WorkflowCheckpointStoreError::new)?;
        self.object_store
            .put(&self.object_key(&layout.manifest_path()), &manifest_bytes)
            .await
            .map_err(WorkflowCheckpointStoreError::new)?;

        let latest_bytes = encode_versioned_json(
            WORKFLOW_CHECKPOINT_LATEST_POINTER_FORMAT_VERSION,
            &WorkflowCheckpointLatestPointer {
                checkpoint_id: manifest.checkpoint_id,
            },
            "workflow checkpoint latest pointer",
        )
        .map_err(WorkflowCheckpointStoreError::new)?;
        self.object_store
            .put(
                &self.latest_manifest_key(&manifest.workflow_name),
                &latest_bytes,
            )
            .await
            .map_err(WorkflowCheckpointStoreError::new)?;

        Ok(())
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkflowProgressMode {
    #[default]
    Auto,
    Buffered,
    Durable,
}

impl WorkflowProgressMode {
    fn uses_durable_commits(self) -> bool {
        !matches!(self, Self::Buffered)
    }
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
    pub trigger_journal: String,
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
            trigger_journal: format!("{prefix}_trigger_journal"),
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
    trigger_journal: Table,
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

    pub fn source_progress_table(&self) -> &Table {
        &self.source_cursors
    }

    pub fn source_cursor_table(&self) -> &Table {
        self.source_progress_table()
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

    pub fn trigger_journal_table(&self) -> &Table {
        &self.trigger_journal
    }
}

pub struct WorkflowDefinition<H> {
    name: String,
    sources: Vec<WorkflowSource>,
    handler: H,
    scheduler: Option<Arc<dyn WorkflowScheduler>>,
    checkpoint_store: Option<Arc<dyn WorkflowCheckpointStore>>,
    table_names: WorkflowTableNames,
    timer_poll_interval: Duration,
    source_batch_limit: usize,
    timer_batch_limit: usize,
    progress_mode: WorkflowProgressMode,
    checkpoint_restore_on_open: WorkflowCheckpointRestoreOnOpen,
}

impl<H> WorkflowDefinition<H> {
    pub fn new<I, S>(name: impl Into<String>, sources: I, handler: H) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<WorkflowSource>,
    {
        let name = name.into();
        Self {
            table_names: WorkflowTableNames::for_workflow(&name),
            name,
            sources: sources.into_iter().map(Into::into).collect(),
            handler,
            scheduler: None,
            checkpoint_store: None,
            timer_poll_interval: DEFAULT_TIMER_POLL_INTERVAL,
            source_batch_limit: DEFAULT_SOURCE_BATCH_LIMIT,
            timer_batch_limit: DEFAULT_TIMER_BATCH_LIMIT,
            progress_mode: WorkflowProgressMode::Auto,
            checkpoint_restore_on_open: WorkflowCheckpointRestoreOnOpen::Disabled,
        }
    }

    pub fn with_scheduler(mut self, scheduler: Arc<dyn WorkflowScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_checkpoint_store(
        mut self,
        checkpoint_store: Arc<dyn WorkflowCheckpointStore>,
    ) -> Self {
        self.checkpoint_store = Some(checkpoint_store);
        self
    }

    fn with_checkpoint_store_opt(
        mut self,
        checkpoint_store: Option<Arc<dyn WorkflowCheckpointStore>>,
    ) -> Self {
        self.checkpoint_store = checkpoint_store;
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

    pub fn with_progress_mode(mut self, progress_mode: WorkflowProgressMode) -> Self {
        self.progress_mode = progress_mode;
        self
    }

    pub fn with_durable_progress(mut self, durable_progress: bool) -> Self {
        self.progress_mode = if durable_progress {
            WorkflowProgressMode::Durable
        } else {
            WorkflowProgressMode::Buffered
        };
        self
    }

    pub fn with_restore_latest_checkpoint_on_open(mut self) -> Self {
        self.checkpoint_restore_on_open = WorkflowCheckpointRestoreOnOpen::Latest;
        self
    }

    pub fn with_restore_checkpoint_on_open(mut self, checkpoint_id: WorkflowCheckpointId) -> Self {
        self.checkpoint_restore_on_open = WorkflowCheckpointRestoreOnOpen::Specific(checkpoint_id);
        self
    }

    fn with_checkpoint_restore_on_open_mode(
        mut self,
        checkpoint_restore_on_open: WorkflowCheckpointRestoreOnOpen,
    ) -> Self {
        self.checkpoint_restore_on_open = checkpoint_restore_on_open;
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
    pub progress_origin: WorkflowSourceProgressOrigin,
    pub attach_mode: Option<WorkflowSourceAttachMode>,
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
    sources: Vec<WorkflowSource>,
    handler: Arc<H>,
    scheduler: Arc<dyn WorkflowScheduler>,
    tables: WorkflowTables,
    checkpoint_store: Option<Arc<dyn WorkflowCheckpointStore>>,
    timer_poll_interval: Duration,
    source_batch_limit: usize,
    timer_batch_limit: usize,
    progress_mode: WorkflowProgressMode,
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
            let checkpoint_restore_on_open = definition.checkpoint_restore_on_open;
            let scheduler = definition
                .scheduler
                .unwrap_or_else(|| Arc::new(RoundRobinWorkflowScheduler::default()));

            let runtime = Self {
                inner: Arc::new(WorkflowRuntimeInner {
                    name: definition.name,
                    db,
                    clock,
                    sources: definition.sources,
                    handler: Arc::new(definition.handler),
                    scheduler,
                    checkpoint_store: definition.checkpoint_store,
                    tables,
                    timer_poll_interval: definition.timer_poll_interval,
                    source_batch_limit: definition.source_batch_limit,
                    timer_batch_limit: definition.timer_batch_limit,
                    progress_mode: definition.progress_mode,
                    running: Mutex::new(false),
                    in_flight_instances: Mutex::new(BTreeSet::new()),
                    ready_notify: Notify::new(),
                }),
            };
            restore_checkpoint_on_open(&runtime.inner, checkpoint_restore_on_open).await?;
            Ok(runtime)
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

    pub fn checkpoint_store(&self) -> Option<&Arc<dyn WorkflowCheckpointStore>> {
        self.inner.checkpoint_store.as_ref()
    }

    pub async fn load_state(&self, instance_id: &str) -> Result<Option<Value>, WorkflowError> {
        Ok(self
            .inner
            .tables
            .state_table()
            .read(instance_id.as_bytes().to_vec())
            .await?)
    }

    pub async fn load_source_progress(
        &self,
        source: &Table,
    ) -> Result<WorkflowSourceProgress, WorkflowError> {
        load_workflow_source_progress(self.inner.tables.source_progress_table(), source).await
    }

    pub async fn load_source_cursor(&self, source: &Table) -> Result<LogCursor, WorkflowError> {
        Ok(self.load_source_progress(source).await?.as_log_cursor())
    }

    pub async fn capture_checkpoint(
        &self,
        checkpoint_id: WorkflowCheckpointId,
    ) -> Result<WorkflowCheckpointManifest, WorkflowError> {
        let Some(checkpoint_store) = self.inner.checkpoint_store.clone() else {
            return Err(WorkflowError::MissingCheckpointStore {
                name: self.inner.name.clone(),
            });
        };
        capture_workflow_checkpoint(&self.inner, checkpoint_store, checkpoint_id).await
    }

    pub async fn restore_latest_checkpoint(
        &self,
    ) -> Result<Option<WorkflowCheckpointManifest>, WorkflowError> {
        ensure_runtime_stopped_for_checkpoint_restore(&self.inner)?;
        let Some(checkpoint_store) = self.inner.checkpoint_store.clone() else {
            return Err(WorkflowError::MissingCheckpointStore {
                name: self.inner.name.clone(),
            });
        };
        restore_checkpoint_selection(
            &self.inner,
            checkpoint_store,
            WorkflowCheckpointRestoreOnOpen::Latest,
        )
        .await
    }

    pub async fn restore_checkpoint(
        &self,
        checkpoint_id: WorkflowCheckpointId,
    ) -> Result<Option<WorkflowCheckpointManifest>, WorkflowError> {
        ensure_runtime_stopped_for_checkpoint_restore(&self.inner)?;
        let Some(checkpoint_store) = self.inner.checkpoint_store.clone() else {
            return Err(WorkflowError::MissingCheckpointStore {
                name: self.inner.name.clone(),
            });
        };
        restore_checkpoint_selection(
            &self.inner,
            checkpoint_store,
            WorkflowCheckpointRestoreOnOpen::Specific(checkpoint_id),
        )
        .await
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
                let _ = self
                    .inner
                    .db
                    .__run_failpoint(
                        crate::failpoints::names::WORKFLOW_CALLBACK_ADMISSION_BEFORE_COMMIT,
                        BTreeMap::from([
                            ("workflow".to_string(), self.inner.name.clone()),
                            ("instance_id".to_string(), instance_id.clone()),
                            ("trigger_seq".to_string(), trigger_seq.to_string()),
                        ]),
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
            let durable_source_sequence = self.inner.db.subscribe_durable(source.table()).current();
            let progress = self.load_source_progress(source.table()).await?;
            source_lags.push(WorkflowSourceTelemetrySnapshot {
                source_table: source.table().name().to_string(),
                durable_sequence: durable_source_sequence,
                cursor_sequence: progress.sequence(),
                lag: durable_source_sequence
                    .get()
                    .saturating_sub(progress.sequence().get()),
                progress_origin: progress.origin(),
                attach_mode: progress.origin().attach_mode(),
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

const RECURRING_BOOTSTRAP_CALLBACK_ID: &str = "__terracedb.recurring.bootstrap";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecurringWorkflowState {
    pub bootstrapped_at: Timestamp,
    pub last_tick_at: Option<Timestamp>,
    pub next_fire_at: Option<Timestamp>,
    pub tick_count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RecurringTickOutput {
    pub outbox_entries: Vec<OutboxEntry>,
    pub timers: Vec<WorkflowTimerCommand>,
}

#[async_trait]
pub trait RecurringWorkflowHandler: Send + Sync {
    async fn tick(
        &self,
        instance_id: &str,
        state: &RecurringWorkflowState,
        fire_at: Timestamp,
        ctx: &WorkflowContext,
    ) -> Result<RecurringTickOutput, WorkflowHandlerError>;
}

type InitialRecurringFireFn = dyn Fn(Timestamp) -> Option<Timestamp> + Send + Sync;
type NextRecurringFireFn =
    dyn Fn(&RecurringWorkflowState, Timestamp) -> Option<Timestamp> + Send + Sync;

#[derive(Clone)]
pub struct RecurringSchedule {
    initial_fire_at: Arc<InitialRecurringFireFn>,
    next_fire_at: Arc<NextRecurringFireFn>,
}

impl RecurringSchedule {
    pub fn fixed_interval(interval: u64) -> Self {
        Self::custom(
            move |now| Some(Timestamp::new(now.get().saturating_add(interval))),
            move |_state, fire_at| Some(Timestamp::new(fire_at.get().saturating_add(interval))),
        )
    }

    pub fn custom<F, G>(initial_fire_at: F, next_fire_at: G) -> Self
    where
        F: Fn(Timestamp) -> Option<Timestamp> + Send + Sync + 'static,
        G: Fn(&RecurringWorkflowState, Timestamp) -> Option<Timestamp> + Send + Sync + 'static,
    {
        Self {
            initial_fire_at: Arc::new(initial_fire_at),
            next_fire_at: Arc::new(next_fire_at),
        }
    }

    fn initial_fire_at(&self, now: Timestamp) -> Option<Timestamp> {
        (self.initial_fire_at)(now)
    }

    fn next_fire_at(
        &self,
        state: &RecurringWorkflowState,
        fire_at: Timestamp,
    ) -> Option<Timestamp> {
        (self.next_fire_at)(state, fire_at)
    }
}

pub struct RecurringWorkflowDefinition<H> {
    name: String,
    instance_id: String,
    schedule: RecurringSchedule,
    handler: H,
    scheduler: Option<Arc<dyn WorkflowScheduler>>,
    checkpoint_store: Option<Arc<dyn WorkflowCheckpointStore>>,
    table_names: WorkflowTableNames,
    timer_poll_interval: Duration,
    timer_batch_limit: usize,
    progress_mode: WorkflowProgressMode,
    checkpoint_restore_on_open: WorkflowCheckpointRestoreOnOpen,
}

impl<H> RecurringWorkflowDefinition<H> {
    pub fn new(
        name: impl Into<String>,
        instance_id: impl Into<String>,
        schedule: RecurringSchedule,
        handler: H,
    ) -> Self {
        let name = name.into();
        Self {
            table_names: WorkflowTableNames::for_workflow(&name),
            name,
            instance_id: instance_id.into(),
            schedule,
            handler,
            scheduler: None,
            checkpoint_store: None,
            timer_poll_interval: DEFAULT_TIMER_POLL_INTERVAL,
            timer_batch_limit: DEFAULT_TIMER_BATCH_LIMIT,
            progress_mode: WorkflowProgressMode::Auto,
            checkpoint_restore_on_open: WorkflowCheckpointRestoreOnOpen::Disabled,
        }
    }

    pub fn with_scheduler(mut self, scheduler: Arc<dyn WorkflowScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_checkpoint_store(
        mut self,
        checkpoint_store: Arc<dyn WorkflowCheckpointStore>,
    ) -> Self {
        self.checkpoint_store = Some(checkpoint_store);
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

    pub fn with_timer_batch_limit(mut self, batch_limit: usize) -> Self {
        self.timer_batch_limit = batch_limit.max(1);
        self
    }

    pub fn with_progress_mode(mut self, progress_mode: WorkflowProgressMode) -> Self {
        self.progress_mode = progress_mode;
        self
    }

    pub fn with_restore_latest_checkpoint_on_open(mut self) -> Self {
        self.checkpoint_restore_on_open = WorkflowCheckpointRestoreOnOpen::Latest;
        self
    }

    pub fn with_restore_checkpoint_on_open(mut self, checkpoint_id: WorkflowCheckpointId) -> Self {
        self.checkpoint_restore_on_open = WorkflowCheckpointRestoreOnOpen::Specific(checkpoint_id);
        self
    }
}

pub struct RecurringWorkflowRuntime<H> {
    runtime: WorkflowRuntime<RecurringWorkflowAdapter<H>>,
    instance_id: String,
    clock: Arc<dyn Clock>,
}

impl<H> Clone for RecurringWorkflowRuntime<H> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            instance_id: self.instance_id.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<H> RecurringWorkflowRuntime<H>
where
    H: RecurringWorkflowHandler + 'static,
{
    pub async fn open(
        db: Db,
        clock: Arc<dyn Clock>,
        definition: RecurringWorkflowDefinition<H>,
    ) -> Result<Self, WorkflowError> {
        if definition.instance_id.trim().is_empty() {
            return Err(WorkflowError::EmptyInstanceId);
        }

        let checkpoint_restore_on_open = definition.checkpoint_restore_on_open;

        let runtime = WorkflowRuntime::open(
            db,
            clock.clone(),
            WorkflowDefinition::new(
                definition.name,
                std::iter::empty::<Table>(),
                RecurringWorkflowAdapter {
                    instance_id: definition.instance_id.clone(),
                    schedule: definition.schedule,
                    handler: definition.handler,
                },
            )
            .with_scheduler(
                definition
                    .scheduler
                    .unwrap_or_else(|| Arc::new(RoundRobinWorkflowScheduler::default())),
            )
            .with_checkpoint_store_opt(definition.checkpoint_store)
            .with_table_names(definition.table_names)
            .with_timer_poll_interval(definition.timer_poll_interval)
            .with_timer_batch_limit(definition.timer_batch_limit)
            .with_progress_mode(definition.progress_mode)
            .with_checkpoint_restore_on_open_mode(checkpoint_restore_on_open),
        )
        .await?;

        Ok(Self {
            runtime,
            instance_id: definition.instance_id,
            clock,
        })
    }

    pub fn name(&self) -> &str {
        self.runtime.name()
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn tables(&self) -> &WorkflowTables {
        self.runtime.tables()
    }

    pub async fn capture_checkpoint(
        &self,
        checkpoint_id: WorkflowCheckpointId,
    ) -> Result<WorkflowCheckpointManifest, WorkflowError> {
        self.runtime.capture_checkpoint(checkpoint_id).await
    }

    pub async fn restore_latest_checkpoint(
        &self,
    ) -> Result<Option<WorkflowCheckpointManifest>, WorkflowError> {
        self.runtime.restore_latest_checkpoint().await
    }

    pub async fn restore_checkpoint(
        &self,
        checkpoint_id: WorkflowCheckpointId,
    ) -> Result<Option<WorkflowCheckpointManifest>, WorkflowError> {
        self.runtime.restore_checkpoint(checkpoint_id).await
    }

    pub async fn telemetry_snapshot(&self) -> Result<WorkflowTelemetrySnapshot, WorkflowError> {
        self.runtime.telemetry_snapshot().await
    }

    pub async fn load_state(&self) -> Result<Option<RecurringWorkflowState>, WorkflowError> {
        decode_recurring_state(self.runtime.load_state(&self.instance_id).await?.as_ref())
    }

    pub async fn next_fire_at(&self) -> Result<Option<Timestamp>, WorkflowError> {
        Ok(self
            .load_state()
            .await?
            .and_then(|state| state.next_fire_at))
    }

    pub async fn ensure_bootstrapped(&self) -> Result<Option<SequenceNumber>, WorkflowError> {
        if self.load_state().await?.is_some() {
            return Ok(None);
        }

        Ok(Some(
            self.runtime
                .admit_callback(
                    self.instance_id.clone(),
                    RECURRING_BOOTSTRAP_CALLBACK_ID,
                    encode_u64_bytes(self.clock.now().get()),
                )
                .await?,
        ))
    }

    pub async fn start(&self) -> Result<RecurringWorkflowHandle, WorkflowError> {
        self.ensure_bootstrapped().await?;
        Ok(RecurringWorkflowHandle {
            inner: self.runtime.start().await?,
        })
    }
}

pub struct RecurringWorkflowHandle {
    inner: WorkflowHandle,
}

impl RecurringWorkflowHandle {
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub async fn abort(self) -> Result<(), WorkflowError> {
        self.inner.abort().await
    }

    pub async fn shutdown(self) -> Result<(), WorkflowError> {
        self.inner.shutdown().await
    }
}

struct RecurringWorkflowAdapter<H> {
    instance_id: String,
    schedule: RecurringSchedule,
    handler: H,
}

#[async_trait]
impl<H> WorkflowHandler for RecurringWorkflowAdapter<H>
where
    H: RecurringWorkflowHandler + 'static,
{
    async fn route_event(&self, _entry: &ChangeEntry) -> Result<String, WorkflowHandlerError> {
        Err(WorkflowHandlerError::new(std::io::Error::other(
            "recurring workflows only accept helper-managed callbacks and timers",
        )))
    }

    async fn handle(
        &self,
        instance_id: &str,
        state: Option<Value>,
        trigger: &WorkflowTrigger,
        ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        if instance_id != self.instance_id {
            return Err(WorkflowHandlerError::new(std::io::Error::other(format!(
                "recurring workflow {} is bound to instance {}",
                ctx.workflow_name(),
                self.instance_id,
            ))));
        }

        let recurring_state =
            decode_recurring_state(state.as_ref()).map_err(WorkflowHandlerError::new)?;
        match trigger {
            WorkflowTrigger::Callback {
                callback_id,
                response,
            } if callback_id == RECURRING_BOOTSTRAP_CALLBACK_ID => {
                self.handle_bootstrap(recurring_state, response)
            }
            WorkflowTrigger::Timer {
                timer_id, fire_at, ..
            } => {
                self.handle_timer(instance_id, recurring_state, timer_id, *fire_at, ctx)
                    .await
            }
            WorkflowTrigger::Callback { .. } => Err(WorkflowHandlerError::new(
                std::io::Error::other("unknown recurring workflow callback"),
            )),
            WorkflowTrigger::Event(_) => Err(WorkflowHandlerError::new(std::io::Error::other(
                "recurring workflows do not consume source events",
            ))),
        }
    }
}

impl<H> RecurringWorkflowAdapter<H>
where
    H: RecurringWorkflowHandler + 'static,
{
    fn handle_bootstrap(
        &self,
        state: Option<RecurringWorkflowState>,
        response: &[u8],
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        if state.is_some() {
            return Ok(WorkflowOutput::default());
        }

        let bootstrapped_at = Timestamp::new(
            decode_u64_bytes(response, "recurring workflow bootstrap clock")
                .map_err(WorkflowHandlerError::new)?,
        );
        let next_fire_at = self.schedule.initial_fire_at(bootstrapped_at);
        let state = RecurringWorkflowState {
            bootstrapped_at,
            last_tick_at: None,
            next_fire_at,
            tick_count: 0,
        };

        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(encode_recurring_state(&state)?),
            outbox_entries: Vec::new(),
            timers: recurring_timer_schedule(&self.instance_id, next_fire_at),
        })
    }

    async fn handle_timer(
        &self,
        instance_id: &str,
        state: Option<RecurringWorkflowState>,
        timer_id: &[u8],
        fire_at: Timestamp,
        ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        let Some(state) = state else {
            return Ok(WorkflowOutput::default());
        };
        if timer_id != recurring_timer_id(instance_id) {
            return Ok(WorkflowOutput::default());
        }
        if state.next_fire_at != Some(fire_at) {
            return Ok(WorkflowOutput::default());
        }

        let tick_output = self.handler.tick(instance_id, &state, fire_at, ctx).await?;
        let next_fire_at = self.schedule.next_fire_at(&state, fire_at);
        let next_state = RecurringWorkflowState {
            bootstrapped_at: state.bootstrapped_at,
            last_tick_at: Some(fire_at),
            next_fire_at,
            tick_count: state.tick_count.saturating_add(1),
        };
        let mut timers = tick_output.timers;
        timers.extend(recurring_timer_schedule(instance_id, next_fire_at));

        Ok(WorkflowOutput {
            state: WorkflowStateMutation::Put(encode_recurring_state(&next_state)?),
            outbox_entries: tick_output.outbox_entries,
            timers,
        })
    }
}

async fn commit_runtime_transaction<H>(
    runtime: &WorkflowRuntimeInner<H>,
    tx: Transaction,
) -> Result<SequenceNumber, TransactionCommitError> {
    if runtime.progress_mode.uses_durable_commits() {
        tx.commit().await
    } else {
        tx.commit_no_flush().await
    }
}

fn workflow_historical_event_name(event: WorkflowHistoricalEvent) -> &'static str {
    match event {
        WorkflowHistoricalEvent::FirstAttach => "first-attach",
        WorkflowHistoricalEvent::SnapshotTooOld => "snapshot-too-old",
    }
}

fn workflow_historical_resolution_name(
    resolution: WorkflowHistoricalSourceResolution,
) -> &'static str {
    match resolution {
        WorkflowHistoricalSourceResolution::AttachFromBeginning => "attach-from-beginning",
        WorkflowHistoricalSourceResolution::AttachFromCurrentDurable { .. } => {
            "attach-from-current-durable"
        }
        WorkflowHistoricalSourceResolution::RestoreCheckpoint => "restore-checkpoint",
        WorkflowHistoricalSourceResolution::ReplayFromHistory => "replay-from-history",
        WorkflowHistoricalSourceResolution::FastForwardToCurrentDurable { .. } => {
            "fast-forward-to-current-durable"
        }
        WorkflowHistoricalSourceResolution::FailClosedSnapshotTooOld => {
            "fail-closed-snapshot-too-old"
        }
    }
}

async fn resolve_workflow_source_progress<H>(
    runtime: &WorkflowRuntimeInner<H>,
    source: &WorkflowSource,
    event: WorkflowHistoricalEvent,
    snapshot_too_old: Option<&SnapshotTooOld>,
) -> Result<WorkflowSourceProgress, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let current_durable_sequence = runtime.db.subscribe_durable(source.table()).current();
    let checkpoint_progress = load_checkpoint_source_progress(runtime, source).await?;
    let checkpoint_available = checkpoint_progress.is_some();
    let resolution = match event {
        WorkflowHistoricalEvent::FirstAttach => {
            source.initial_resolution(checkpoint_available, current_durable_sequence)
        }
        WorkflowHistoricalEvent::SnapshotTooOld => {
            source.recovery_resolution(checkpoint_available, current_durable_sequence)
        }
    };
    let span = tracing::info_span!("terracedb.workflow.source_resolution");
    apply_workflow_span_attributes(&span, &runtime.db, &runtime.name, None);
    set_span_attribute(
        &span,
        telemetry_attrs::SOURCE_TABLE,
        source.table().name().to_string(),
    );
    set_span_attribute(
        &span,
        telemetry_attrs::DURABLE_SEQUENCE,
        current_durable_sequence,
    );
    set_span_attribute(
        &span,
        "terracedb.workflow.source.event",
        workflow_historical_event_name(event),
    );
    set_span_attribute(
        &span,
        "terracedb.workflow.source.resolution",
        workflow_historical_resolution_name(resolution),
    );
    set_span_attribute(
        &span,
        "terracedb.workflow.source.checkpoint_available",
        checkpoint_available,
    );
    set_span_attribute(
        &span,
        "terracedb.workflow.source.lossy",
        resolution.is_lossy(),
    );
    if let Some(attach_mode) = resolution.attach_mode() {
        set_span_attribute(
            &span,
            "terracedb.workflow.source.attach_mode",
            match attach_mode {
                WorkflowSourceAttachMode::Historical => "historical",
                WorkflowSourceAttachMode::LiveOnly => "live-only",
            },
        );
    }
    if let Some(snapshot_too_old) = snapshot_too_old {
        set_span_attribute(
            &span,
            "terracedb.workflow.snapshot_too_old.requested",
            snapshot_too_old.requested,
        );
        set_span_attribute(
            &span,
            "terracedb.workflow.snapshot_too_old.oldest_available",
            snapshot_too_old.oldest_available,
        );
    }

    let span_for_attrs = span.clone();

    async move {
        if resolution.surfaces_snapshot_too_old() {
            return Err(snapshot_too_old
                .cloned()
                .map(WorkflowError::SnapshotTooOld)
                .unwrap_or_else(|| {
                    WorkflowError::Storage(StorageError::unsupported(
                        "workflow source fail-closed recovery requires a SnapshotTooOld cause",
                    ))
                }));
        }

        let progress = source_progress_for_resolution(source, resolution, checkpoint_progress)?;
        persist_workflow_source_progress(runtime, source, progress).await?;
        set_span_attribute(
            &span_for_attrs,
            telemetry_attrs::LOG_CURSOR,
            format!(
                "{}:{}",
                progress.sequence().get(),
                progress.as_log_cursor().op_index()
            ),
        );
        Ok(progress)
    }
    .instrument(span.clone())
    .await
}

fn source_progress_for_resolution(
    source: &WorkflowSource,
    resolution: WorkflowHistoricalSourceResolution,
    checkpoint_progress: Option<WorkflowSourceProgress>,
) -> Result<WorkflowSourceProgress, WorkflowError> {
    match resolution {
        WorkflowHistoricalSourceResolution::AttachFromBeginning => Ok(
            WorkflowSourceProgress::from_cursor(LogCursor::beginning())
                .with_origin(WorkflowSourceProgressOrigin::BeginningBootstrap),
        ),
        WorkflowHistoricalSourceResolution::AttachFromCurrentDurable { durable_sequence } => Ok(
            WorkflowSourceProgress::from_durable_sequence(durable_sequence)
                .with_origin(WorkflowSourceProgressOrigin::CurrentDurableBootstrap),
        ),
        WorkflowHistoricalSourceResolution::RestoreCheckpoint => checkpoint_progress
            .map(|progress| progress.with_origin(WorkflowSourceProgressOrigin::CheckpointRestore))
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "checkpoint restore requested for source {}, but no checkpoint frontier is available",
                    source.table().name(),
                ))
                .into()
            }),
        WorkflowHistoricalSourceResolution::ReplayFromHistory => {
            if source.config().capabilities.replay != WorkflowReplayableSourceKind::AppendOnlyOrdered
            {
                return Err(StorageError::unsupported(format!(
                    "workflow source {} cannot replay from history unless it is configured as append-only ordered",
                    source.table().name(),
                ))
                .into());
            }
            Ok(WorkflowSourceProgress::from_cursor(LogCursor::beginning())
                .with_origin(WorkflowSourceProgressOrigin::ReplayFromHistory))
        }
        WorkflowHistoricalSourceResolution::FastForwardToCurrentDurable { durable_sequence } => Ok(
            WorkflowSourceProgress::from_durable_sequence(durable_sequence)
                .with_origin(WorkflowSourceProgressOrigin::FastForwardToCurrentDurable),
        ),
        WorkflowHistoricalSourceResolution::FailClosedSnapshotTooOld => Err(
            StorageError::unsupported(
                "workflow source fail-closed recovery must be handled before progress resolution",
            )
            .into(),
        ),
    }
}

async fn load_checkpoint_source_progress<H>(
    runtime: &WorkflowRuntimeInner<H>,
    source: &WorkflowSource,
) -> Result<Option<WorkflowSourceProgress>, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    if !source.config().capabilities.supports_checkpoint_restore() {
        return Ok(None);
    }
    let Some(checkpoint_store) = runtime.checkpoint_store.as_ref() else {
        return Ok(None);
    };
    let Some(manifest) = checkpoint_store
        .load_latest_manifest(&runtime.name)
        .await
        .map_err(|error| checkpoint_store_error(&runtime.name, error))?
    else {
        return Ok(None);
    };
    Ok(manifest.source_frontier.get(source.table().name()).copied())
}

async fn persist_workflow_source_progress<H>(
    runtime: &WorkflowRuntimeInner<H>,
    source: &WorkflowSource,
    progress: WorkflowSourceProgress,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    loop {
        let mut tx = Transaction::begin(&runtime.db).await;
        stage_workflow_source_progress_in_transaction(
            &mut tx,
            runtime.tables.source_progress_table(),
            source.table(),
            progress,
        )?;
        match tx.commit().await {
            Ok(_) => return Ok(()),
            Err(TransactionCommitError::Commit(CommitError::Conflict)) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

async fn retag_restored_source_progress<H>(
    runtime: &WorkflowRuntimeInner<H>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    for source in &runtime.sources {
        let Some(progress) =
            read_workflow_source_progress(runtime.tables.source_progress_table(), source.table())
                .await?
        else {
            continue;
        };
        persist_workflow_source_progress(
            runtime,
            source,
            progress.with_origin(WorkflowSourceProgressOrigin::CheckpointRestore),
        )
        .await?;
    }
    Ok(())
}

async fn detect_snapshot_too_old_for_source_progress<H>(
    runtime: &WorkflowRuntimeInner<H>,
    source: &WorkflowSource,
    progress: WorkflowSourceProgress,
) -> Result<Option<SnapshotTooOld>, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    match runtime
        .db
        .scan_durable_since(
            source.table(),
            progress.as_log_cursor(),
            ScanOptions {
                limit: Some(1),
                ..ScanOptions::default()
            },
        )
        .await
    {
        Ok(_) => Ok(None),
        Err(error) => match error.snapshot_too_old().cloned() {
            Some(snapshot_too_old) => Ok(Some(snapshot_too_old)),
            None => Err(error.into()),
        },
    }
}

async fn resolve_workflow_source_progress_on_start<H>(
    runtime: &WorkflowRuntimeInner<H>,
    source: &WorkflowSource,
    event: WorkflowHistoricalEvent,
    snapshot_too_old: Option<&SnapshotTooOld>,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let current_durable_sequence = runtime.db.subscribe_durable(source.table()).current();
    let checkpoint_progress = load_checkpoint_source_progress(runtime, source).await?;
    let checkpoint_available = checkpoint_progress.is_some();
    let resolution = match event {
        WorkflowHistoricalEvent::FirstAttach => {
            source.initial_resolution(checkpoint_available, current_durable_sequence)
        }
        WorkflowHistoricalEvent::SnapshotTooOld => {
            source.recovery_resolution(checkpoint_available, current_durable_sequence)
        }
    };

    if resolution == WorkflowHistoricalSourceResolution::RestoreCheckpoint {
        let Some(checkpoint_store) = runtime.checkpoint_store.clone() else {
            return Err(WorkflowError::MissingCheckpointStore {
                name: runtime.name.clone(),
            });
        };
        let Some(manifest) = checkpoint_store
            .load_latest_manifest(&runtime.name)
            .await
            .map_err(|error| checkpoint_store_error(&runtime.name, error))?
        else {
            return Err(StorageError::not_found(format!(
                "workflow {} requested checkpoint restore for source {}, but no latest checkpoint exists",
                runtime.name,
                source.table().name(),
            ))
            .into());
        };
        restore_checkpoint_manifest(runtime, checkpoint_store, manifest).await?;
        retag_restored_source_progress(runtime).await?;
        resume_local_durable_work(runtime, shutdown_rx).await?;
        return Ok(());
    }

    let _ = resolve_workflow_source_progress(runtime, source, event, snapshot_too_old).await?;
    Ok(())
}

async fn prepare_workflow_sources_on_start<H>(
    runtime: &WorkflowRuntimeInner<H>,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    for source in &runtime.sources {
        let mut prepared = false;
        let mut last_snapshot_progress = None;

        for _ in 0..4 {
            if *shutdown_rx.borrow() {
                return Ok(());
            }

            let progress = read_workflow_source_progress(
                runtime.tables.source_progress_table(),
                source.table(),
            )
            .await?;
            let Some(progress) = progress else {
                resolve_workflow_source_progress_on_start(
                    runtime,
                    source,
                    WorkflowHistoricalEvent::FirstAttach,
                    None,
                    shutdown_rx,
                )
                .await?;
                continue;
            };

            let Some(snapshot_too_old) =
                detect_snapshot_too_old_for_source_progress(runtime, source, progress).await?
            else {
                prepared = true;
                break;
            };

            if last_snapshot_progress == Some(progress) {
                return Err(WorkflowError::SnapshotTooOld(snapshot_too_old));
            }
            last_snapshot_progress = Some(progress);

            resolve_workflow_source_progress_on_start(
                runtime,
                source,
                WorkflowHistoricalEvent::SnapshotTooOld,
                Some(&snapshot_too_old),
                shutdown_rx,
            )
            .await?;
        }

        if !prepared {
            return Err(StorageError::unsupported(format!(
                "workflow source {} could not establish resumable historical progress during startup",
                source.table().name(),
            ))
            .into());
        }
    }

    Ok(())
}

async fn resume_local_durable_work<H>(
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

        seed_ready_instances_from_durable_inbox(runtime).await?;
        let _ = admit_due_timer_batch(runtime).await?;
        drain_ready_instances(runtime, shutdown_rx).await?;

        if *shutdown_rx.borrow() {
            return Ok(());
        }

        if !has_durable_inbox_entries(runtime).await? && !has_due_durable_timers(runtime).await? {
            return Ok(());
        }
    }
}

async fn has_durable_inbox_entries<H>(
    runtime: &WorkflowRuntimeInner<H>,
) -> Result<bool, WorkflowError>
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
            ScanOptions {
                limit: Some(1),
                ..ScanOptions::default()
            },
        )
        .await?;
    Ok(rows.next().await.is_some())
}

async fn has_due_durable_timers<H>(runtime: &WorkflowRuntimeInner<H>) -> Result<bool, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    Ok(!runtime
        .tables
        .timers
        .scan_due_durable(&runtime.db, runtime.clock.now(), Some(1))
        .await?
        .is_empty())
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
        if !runtime.sources.is_empty() {
            resume_local_durable_work(&runtime, &shutdown_rx).await?;
            prepare_workflow_sources_on_start(&runtime, &shutdown_rx).await?;
        }

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
    source: WorkflowSource,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let mut progress =
        match read_workflow_source_progress(runtime.tables.source_progress_table(), source.table())
            .await?
        {
            Some(progress) => progress,
            None => {
                resolve_workflow_source_progress(
                    runtime.as_ref(),
                    &source,
                    WorkflowHistoricalEvent::FirstAttach,
                    None,
                )
                .await?
            }
        };
    let mut durable_wakes = runtime.db.subscribe_durable(source.table());

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        loop {
            match admit_source_page(&runtime, &source, &mut progress).await {
                Ok(true) => {
                    if *shutdown_rx.borrow() {
                        return Ok(());
                    }
                }
                Ok(false) => break,
                Err(error) if error.snapshot_too_old().is_some() => {
                    let snapshot_too_old = error
                        .snapshot_too_old()
                        .cloned()
                        .expect("SnapshotTooOld guard should guarantee an error payload");
                    progress = resolve_workflow_source_progress(
                        runtime.as_ref(),
                        &source,
                        WorkflowHistoricalEvent::SnapshotTooOld,
                        Some(&snapshot_too_old),
                    )
                    .await?;
                }
                Err(error) => return Err(error),
            }
        }

        if *shutdown_rx.borrow() {
            return Ok(());
        }

        if durable_wakes.current() > progress.sequence() {
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
    source: &WorkflowSource,
    progress: &mut WorkflowSourceProgress,
) -> Result<bool, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let mut stream = runtime
        .db
        .scan_durable_since(
            source.table(),
            progress.as_log_cursor(),
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
        source.table().name().to_string(),
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
            let mut new_progress = *progress;

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
                new_progress = WorkflowSourceProgress::from_cursor(entry.cursor)
                    .with_origin(progress.origin());
            }

            stage_workflow_source_progress_in_transaction(
                &mut tx,
                runtime.tables.source_progress_table(),
                source.table(),
                new_progress,
            )?;

            match commit_runtime_transaction(runtime, tx).await {
                Ok(_) => {
                    *progress = new_progress;
                    set_span_attribute(
                        &span_for_attrs,
                        telemetry_attrs::LOG_CURSOR,
                        format!(
                            "{}:{}",
                            new_progress.sequence().get(),
                            new_progress.as_log_cursor().op_index()
                        ),
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

            let _ = runtime
                .db
                .__run_failpoint(
                    crate::failpoints::names::WORKFLOW_TIMER_ADMISSION_BEFORE_COMMIT,
                    BTreeMap::from([
                        ("workflow".to_string(), runtime.name.clone()),
                        ("timer_count".to_string(), due.timers.len().to_string()),
                    ]),
                )
                .await?;
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

        let _ = runtime
            .db
            .__run_failpoint(
                crate::failpoints::names::WORKFLOW_EXECUTION_BEFORE_COMMIT,
                BTreeMap::from([
                    ("workflow".to_string(), runtime.name.clone()),
                    ("instance_id".to_string(), instance_id.to_string()),
                ]),
            )
            .await?;
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
                operation_context: operation_context.clone(),
            },
            "workflow inbox trigger",
        )?),
    );
    stage_trigger_journal_admission(
        tx,
        runtime.tables.trigger_journal_table(),
        StoredAdmittedWorkflowTrigger {
            workflow_instance: instance_id.to_string(),
            trigger_seq: trigger_sequence,
            trigger: StoredWorkflowTrigger::from_runtime_trigger(trigger),
            operation_context,
        },
    )
    .await?;

    Ok(trigger_sequence)
}

async fn stage_trigger_journal_admission(
    tx: &mut Transaction,
    trigger_journal: &Table,
    admitted: StoredAdmittedWorkflowTrigger,
) -> Result<(), WorkflowError> {
    let current = tx.read(trigger_journal, trigger_journal_head_key()).await?;
    let journal_sequence = decode_trigger_journal_head(current.as_ref())?.saturating_add(1);

    tx.write(
        trigger_journal,
        trigger_journal_head_key(),
        Value::bytes(encode_trigger_journal_head(journal_sequence)?),
    );
    tx.write(
        trigger_journal,
        trigger_journal_entry_key(journal_sequence),
        Value::bytes(encode_payload(&admitted, "workflow trigger journal entry")?),
    );
    Ok(())
}

fn checkpoint_store_error(
    workflow_name: &str,
    source: WorkflowCheckpointStoreError,
) -> WorkflowError {
    WorkflowError::CheckpointStore {
        name: workflow_name.to_string(),
        source,
    }
}

fn ensure_runtime_stopped_for_checkpoint_restore<H>(
    runtime: &WorkflowRuntimeInner<H>,
) -> Result<(), WorkflowError> {
    if *runtime.running.lock().expect("running lock poisoned") {
        return Err(WorkflowError::RestoreWhileRunning {
            name: runtime.name.clone(),
        });
    }
    Ok(())
}

async fn restore_checkpoint_on_open<H>(
    runtime: &WorkflowRuntimeInner<H>,
    checkpoint_restore_on_open: WorkflowCheckpointRestoreOnOpen,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    if checkpoint_restore_on_open == WorkflowCheckpointRestoreOnOpen::Disabled {
        return Ok(());
    }

    let Some(checkpoint_store) = runtime.checkpoint_store.clone() else {
        return Err(WorkflowError::MissingCheckpointStore {
            name: runtime.name.clone(),
        });
    };
    restore_checkpoint_selection(runtime, checkpoint_store, checkpoint_restore_on_open)
        .await
        .map(|_| ())
}

async fn restore_checkpoint_selection<H>(
    runtime: &WorkflowRuntimeInner<H>,
    checkpoint_store: Arc<dyn WorkflowCheckpointStore>,
    selection: WorkflowCheckpointRestoreOnOpen,
) -> Result<Option<WorkflowCheckpointManifest>, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let manifest = match selection {
        WorkflowCheckpointRestoreOnOpen::Disabled => return Ok(None),
        WorkflowCheckpointRestoreOnOpen::Latest => checkpoint_store
            .load_latest_manifest(&runtime.name)
            .await
            .map_err(|error| checkpoint_store_error(&runtime.name, error))?,
        WorkflowCheckpointRestoreOnOpen::Specific(checkpoint_id) => checkpoint_store
            .load_manifest(&runtime.name, checkpoint_id)
            .await
            .map_err(|error| checkpoint_store_error(&runtime.name, error))?,
    };

    let Some(manifest) = manifest else {
        return Ok(None);
    };
    restore_checkpoint_manifest(runtime, checkpoint_store, manifest.clone()).await?;
    Ok(Some(manifest))
}

async fn capture_workflow_checkpoint<H>(
    runtime: &WorkflowRuntimeInner<H>,
    checkpoint_store: Arc<dyn WorkflowCheckpointStore>,
    checkpoint_id: WorkflowCheckpointId,
) -> Result<WorkflowCheckpointManifest, WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    let durable_sequence = runtime.db.current_durable_sequence();
    let captured_at = runtime.clock.now();
    let layout = WorkflowCheckpointLayout::new(&runtime.name, checkpoint_id);

    let state_rows =
        scan_table_rows_at_sequence(runtime.tables.state_table(), durable_sequence).await?;
    let inbox_rows =
        scan_table_rows_at_sequence(runtime.tables.inbox_table(), durable_sequence).await?;
    let trigger_order_rows =
        scan_table_rows_at_sequence(runtime.tables.trigger_order_table(), durable_sequence).await?;
    let source_progress_rows =
        scan_table_rows_at_sequence(runtime.tables.source_progress_table(), durable_sequence)
            .await?;
    let timer_schedule_rows =
        scan_table_rows_at_sequence(runtime.tables.timer_schedule_table(), durable_sequence)
            .await?;
    let timer_lookup_rows =
        scan_table_rows_at_sequence(runtime.tables.timer_lookup_table(), durable_sequence).await?;
    let outbox_rows =
        scan_table_rows_at_sequence(runtime.tables.outbox_table(), durable_sequence).await?;
    let trigger_journal_rows =
        scan_table_rows_at_sequence(runtime.tables.trigger_journal_table(), durable_sequence)
            .await?;

    let source_frontier = decode_source_frontier_from_rows(&source_progress_rows)?;
    let trigger_journal_high_watermark =
        trigger_journal_high_watermark_from_rows(&trigger_journal_rows)?;

    let mut artifacts = vec![
        WorkflowCheckpointArtifactPayload {
            kind: WorkflowCheckpointArtifactKind::State,
            bytes: encode_checkpoint_table_artifact(
                WorkflowCheckpointArtifactKind::State,
                &state_rows,
            )?,
        },
        WorkflowCheckpointArtifactPayload {
            kind: WorkflowCheckpointArtifactKind::Inbox,
            bytes: encode_checkpoint_table_artifact(
                WorkflowCheckpointArtifactKind::Inbox,
                &inbox_rows,
            )?,
        },
        WorkflowCheckpointArtifactPayload {
            kind: WorkflowCheckpointArtifactKind::TriggerOrder,
            bytes: encode_checkpoint_table_artifact(
                WorkflowCheckpointArtifactKind::TriggerOrder,
                &trigger_order_rows,
            )?,
        },
        WorkflowCheckpointArtifactPayload {
            kind: WorkflowCheckpointArtifactKind::SourceProgress,
            bytes: encode_checkpoint_table_artifact(
                WorkflowCheckpointArtifactKind::SourceProgress,
                &source_progress_rows,
            )?,
        },
        WorkflowCheckpointArtifactPayload {
            kind: WorkflowCheckpointArtifactKind::TimerSchedule,
            bytes: encode_checkpoint_table_artifact(
                WorkflowCheckpointArtifactKind::TimerSchedule,
                &timer_schedule_rows,
            )?,
        },
        WorkflowCheckpointArtifactPayload {
            kind: WorkflowCheckpointArtifactKind::TimerLookup,
            bytes: encode_checkpoint_table_artifact(
                WorkflowCheckpointArtifactKind::TimerLookup,
                &timer_lookup_rows,
            )?,
        },
        WorkflowCheckpointArtifactPayload {
            kind: WorkflowCheckpointArtifactKind::Outbox,
            bytes: encode_checkpoint_table_artifact(
                WorkflowCheckpointArtifactKind::Outbox,
                &outbox_rows,
            )?,
        },
    ];

    let mut artifact_kinds = artifacts
        .iter()
        .map(|artifact| artifact.kind)
        .collect::<Vec<_>>();
    if trigger_journal_high_watermark > 0 {
        artifacts.push(WorkflowCheckpointArtifactPayload {
            kind: WorkflowCheckpointArtifactKind::TriggerJournal,
            bytes: encode_checkpoint_table_artifact(
                WorkflowCheckpointArtifactKind::TriggerJournal,
                &trigger_journal_rows,
            )?,
        });
        artifact_kinds.push(WorkflowCheckpointArtifactKind::TriggerJournal);
    }

    let mut manifest = layout.manifest_with_frontier(captured_at, source_frontier, artifact_kinds);
    manifest.trigger_journal_high_watermark =
        (trigger_journal_high_watermark > 0).then_some(trigger_journal_high_watermark);

    checkpoint_store
        .publish_checkpoint(manifest.clone(), artifacts)
        .await
        .map_err(|error| checkpoint_store_error(&runtime.name, error))?;

    Ok(manifest)
}

async fn restore_checkpoint_manifest<H>(
    runtime: &WorkflowRuntimeInner<H>,
    checkpoint_store: Arc<dyn WorkflowCheckpointStore>,
    manifest: WorkflowCheckpointManifest,
) -> Result<(), WorkflowError>
where
    H: WorkflowHandler + 'static,
{
    if manifest.workflow_name != runtime.name {
        return Err(StorageError::corruption(format!(
            "checkpoint workflow {} does not match runtime {}",
            manifest.workflow_name, runtime.name
        ))
        .into());
    }

    let state_rows = load_required_checkpoint_rows(
        checkpoint_store.as_ref(),
        &runtime.name,
        manifest.checkpoint_id,
        WorkflowCheckpointArtifactKind::State,
    )
    .await?;
    let inbox_rows = load_required_checkpoint_rows(
        checkpoint_store.as_ref(),
        &runtime.name,
        manifest.checkpoint_id,
        WorkflowCheckpointArtifactKind::Inbox,
    )
    .await?;
    let trigger_order_rows = load_required_checkpoint_rows(
        checkpoint_store.as_ref(),
        &runtime.name,
        manifest.checkpoint_id,
        WorkflowCheckpointArtifactKind::TriggerOrder,
    )
    .await?;
    let source_progress_rows = load_required_checkpoint_rows(
        checkpoint_store.as_ref(),
        &runtime.name,
        manifest.checkpoint_id,
        WorkflowCheckpointArtifactKind::SourceProgress,
    )
    .await?;
    let timer_schedule_rows = load_required_checkpoint_rows(
        checkpoint_store.as_ref(),
        &runtime.name,
        manifest.checkpoint_id,
        WorkflowCheckpointArtifactKind::TimerSchedule,
    )
    .await?;
    let timer_lookup_rows = load_required_checkpoint_rows(
        checkpoint_store.as_ref(),
        &runtime.name,
        manifest.checkpoint_id,
        WorkflowCheckpointArtifactKind::TimerLookup,
    )
    .await?;
    let outbox_rows = load_required_checkpoint_rows(
        checkpoint_store.as_ref(),
        &runtime.name,
        manifest.checkpoint_id,
        WorkflowCheckpointArtifactKind::Outbox,
    )
    .await?;
    let mut trigger_journal_rows = if manifest
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == WorkflowCheckpointArtifactKind::TriggerJournal)
    {
        load_required_checkpoint_rows(
            checkpoint_store.as_ref(),
            &runtime.name,
            manifest.checkpoint_id,
            WorkflowCheckpointArtifactKind::TriggerJournal,
        )
        .await?
    } else {
        Vec::new()
    };
    if !trigger_journal_rows.is_empty()
        && !trigger_journal_rows
            .iter()
            .any(|row| row.key == trigger_journal_head_key())
    {
        let high_watermark = manifest.trigger_journal_high_watermark.unwrap_or(
            trigger_journal_high_watermark_from_rows(&trigger_journal_rows)?,
        );
        if high_watermark > 0 {
            trigger_journal_rows.push(WorkflowCheckpointTableRow {
                key: trigger_journal_head_key(),
                value: Value::bytes(encode_trigger_journal_head(high_watermark)?),
            });
        }
    }

    let durable_sequence = runtime.db.current_durable_sequence();
    let existing_state_keys =
        scan_table_keys_at_sequence(runtime.tables.state_table(), durable_sequence).await?;
    let existing_inbox_keys =
        scan_table_keys_at_sequence(runtime.tables.inbox_table(), durable_sequence).await?;
    let existing_trigger_order_keys =
        scan_table_keys_at_sequence(runtime.tables.trigger_order_table(), durable_sequence).await?;
    let existing_source_progress_keys =
        scan_table_keys_at_sequence(runtime.tables.source_progress_table(), durable_sequence)
            .await?;
    let existing_timer_schedule_keys =
        scan_table_keys_at_sequence(runtime.tables.timer_schedule_table(), durable_sequence)
            .await?;
    let existing_timer_lookup_keys =
        scan_table_keys_at_sequence(runtime.tables.timer_lookup_table(), durable_sequence).await?;
    let existing_outbox_keys =
        scan_table_keys_at_sequence(runtime.tables.outbox_table(), durable_sequence).await?;
    let existing_trigger_journal_keys =
        scan_table_keys_at_sequence(runtime.tables.trigger_journal_table(), durable_sequence)
            .await?;

    let mut tx = Transaction::begin(&runtime.db).await;
    replace_table_rows_in_transaction(
        &mut tx,
        runtime.tables.state_table(),
        &existing_state_keys,
        &state_rows,
    );
    replace_table_rows_in_transaction(
        &mut tx,
        runtime.tables.inbox_table(),
        &existing_inbox_keys,
        &inbox_rows,
    );
    replace_table_rows_in_transaction(
        &mut tx,
        runtime.tables.trigger_order_table(),
        &existing_trigger_order_keys,
        &trigger_order_rows,
    );
    replace_table_rows_in_transaction(
        &mut tx,
        runtime.tables.source_progress_table(),
        &existing_source_progress_keys,
        &source_progress_rows,
    );
    replace_table_rows_in_transaction(
        &mut tx,
        runtime.tables.timer_schedule_table(),
        &existing_timer_schedule_keys,
        &timer_schedule_rows,
    );
    replace_table_rows_in_transaction(
        &mut tx,
        runtime.tables.timer_lookup_table(),
        &existing_timer_lookup_keys,
        &timer_lookup_rows,
    );
    replace_table_rows_in_transaction(
        &mut tx,
        runtime.tables.outbox_table(),
        &existing_outbox_keys,
        &outbox_rows,
    );
    replace_table_rows_in_transaction(
        &mut tx,
        runtime.tables.trigger_journal_table(),
        &existing_trigger_journal_keys,
        &trigger_journal_rows,
    );

    let _ = runtime
        .db
        .__run_failpoint(
            crate::failpoints::names::WORKFLOW_CHECKPOINT_RESTORE_BEFORE_COMMIT,
            BTreeMap::from([
                ("workflow".to_string(), runtime.name.clone()),
                (
                    "checkpoint_id".to_string(),
                    manifest.checkpoint_id.get().to_string(),
                ),
            ]),
        )
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn load_required_checkpoint_rows(
    checkpoint_store: &dyn WorkflowCheckpointStore,
    workflow_name: &str,
    checkpoint_id: WorkflowCheckpointId,
    kind: WorkflowCheckpointArtifactKind,
) -> Result<Vec<WorkflowCheckpointTableRow>, WorkflowError> {
    let bytes = checkpoint_store
        .read_artifact(workflow_name, checkpoint_id, kind)
        .await
        .map_err(|error| checkpoint_store_error(workflow_name, error))?
        .ok_or_else(|| {
            StorageError::corruption(format!(
                "checkpoint {} is missing {:?} artifact",
                checkpoint_id, kind
            ))
        })?;
    decode_checkpoint_table_artifact(kind, &bytes)
}

fn replace_table_rows_in_transaction(
    tx: &mut Transaction,
    table: &Table,
    existing_keys: &[Key],
    rows: &[WorkflowCheckpointTableRow],
) {
    for key in existing_keys {
        tx.delete(table, key.clone());
    }
    for row in rows {
        tx.write(table, row.key.clone(), row.value.clone());
    }
}

async fn scan_table_rows_at_sequence(
    table: &Table,
    sequence: SequenceNumber,
) -> Result<Vec<WorkflowCheckpointTableRow>, WorkflowError> {
    let mut rows = table
        .scan_at(
            FULL_SCAN_START.to_vec(),
            FULL_SCAN_END.to_vec(),
            sequence,
            ScanOptions::default(),
        )
        .await?;
    let mut captured = Vec::new();
    while let Some((key, value)) = rows.next().await {
        captured.push(WorkflowCheckpointTableRow { key, value });
    }
    Ok(captured)
}

async fn scan_table_keys_at_sequence(
    table: &Table,
    sequence: SequenceNumber,
) -> Result<Vec<Key>, WorkflowError> {
    Ok(scan_table_rows_at_sequence(table, sequence)
        .await?
        .into_iter()
        .map(|row| row.key)
        .collect())
}

fn decode_source_frontier_from_rows(
    rows: &[WorkflowCheckpointTableRow],
) -> Result<BTreeMap<String, WorkflowSourceProgress>, WorkflowError> {
    let mut source_frontier = BTreeMap::new();
    for row in rows {
        let source_name = std::str::from_utf8(&row.key).map_err(|error| {
            StorageError::corruption(format!(
                "workflow source progress key must be valid utf-8: {error}"
            ))
        })?;
        source_frontier.insert(
            source_name.to_string(),
            decode_workflow_source_progress_value(&row.value)?,
        );
    }
    Ok(source_frontier)
}

fn trigger_journal_high_watermark_from_rows(
    rows: &[WorkflowCheckpointTableRow],
) -> Result<u64, WorkflowError> {
    let mut max_sequence = 0;
    for row in rows {
        if row.key == trigger_journal_head_key() {
            return decode_trigger_journal_head(Some(&row.value));
        }
        if let Some(sequence) = decode_trigger_journal_entry_sequence(&row.key) {
            max_sequence = max_sequence.max(sequence);
        }
    }
    Ok(max_sequence)
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
    let trigger_journal = ensure_table(db, &table_names.trigger_journal).await?;

    Ok(WorkflowTables {
        state,
        inbox,
        trigger_order,
        source_cursors,
        timers: DurableTimerSet::new(timer_schedule, timer_lookup),
        outbox: TransactionalOutbox::new(outbox),
        trigger_journal,
    })
}

async fn ensure_table(db: &Db, name: &str) -> Result<Table, WorkflowError> {
    db.ensure_table(workflow_table_config(name))
        .await
        .map_err(Into::into)
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

async fn load_workflow_source_progress(
    progress_table: &Table,
    source: &Table,
) -> Result<WorkflowSourceProgress, WorkflowError> {
    Ok(read_workflow_source_progress(progress_table, source)
        .await?
        .unwrap_or_default())
}

async fn read_workflow_source_progress(
    progress_table: &Table,
    source: &Table,
) -> Result<Option<WorkflowSourceProgress>, WorkflowError> {
    let Some(value) = progress_table.read(source_cursor_key(source)).await? else {
        return Ok(None);
    };
    decode_workflow_source_progress_value(&value)
        .map(Some)
        .map_err(Into::into)
}

fn stage_workflow_source_progress_in_transaction(
    tx: &mut Transaction,
    progress_table: &Table,
    source: &Table,
    progress: WorkflowSourceProgress,
) -> Result<(), WorkflowError> {
    tx.write(
        progress_table,
        source_cursor_key(source),
        Value::bytes(progress.encode()?),
    );
    Ok(())
}

fn decode_workflow_source_progress_value(
    value: &Value,
) -> Result<WorkflowSourceProgress, StorageError> {
    let bytes = match value {
        Value::Bytes(bytes) => bytes.as_slice(),
        Value::Record(_) => {
            return Err(StorageError::corruption(
                "workflow source progress expected a byte value",
            ));
        }
    };
    WorkflowSourceProgress::decode(bytes)
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

fn encode_u64_bytes(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_u64_bytes(bytes: &[u8], context: &str) -> Result<u64, std::io::Error> {
    if bytes.len() != 8 {
        return Err(std::io::Error::other(format!(
            "{context} must encode exactly 8 bytes"
        )));
    }

    let mut raw = [0_u8; 8];
    raw.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(raw))
}

fn trigger_journal_head_key() -> Key {
    TRIGGER_JOURNAL_HEAD_KEY.to_vec()
}

fn trigger_journal_entry_key(journal_sequence: u64) -> Key {
    let mut key = Vec::with_capacity(9);
    key.push(TRIGGER_JOURNAL_ENTRY_PREFIX);
    key.extend_from_slice(&journal_sequence.to_be_bytes());
    key
}

fn decode_trigger_journal_entry_sequence(key: &[u8]) -> Option<u64> {
    if key.len() != 9 || key.first().copied() != Some(TRIGGER_JOURNAL_ENTRY_PREFIX) {
        return None;
    }
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&key[1..]);
    Some(u64::from_be_bytes(raw))
}

fn encode_trigger_journal_head(journal_sequence: u64) -> Result<Vec<u8>, WorkflowError> {
    encode_versioned_json(
        WORKFLOW_TRIGGER_JOURNAL_HEAD_FORMAT_VERSION,
        &journal_sequence,
        "workflow trigger journal head",
    )
    .map_err(Into::into)
}

fn decode_trigger_journal_head(value: Option<&Value>) -> Result<u64, WorkflowError> {
    let Some(value) = value else {
        return Ok(0);
    };
    let bytes = expect_bytes_value(value, "workflow trigger journal head")?;
    decode_versioned_json(
        WORKFLOW_TRIGGER_JOURNAL_HEAD_FORMAT_VERSION,
        bytes,
        "workflow trigger journal head",
    )
    .map_err(Into::into)
}

fn encode_workflow_source_progress(
    progress: WorkflowSourceProgress,
) -> Result<Vec<u8>, StorageError> {
    let json = serde_json::to_vec(&progress).map_err(|error| {
        StorageError::corruption(format!(
            "workflow source progress serialization failed: {error}"
        ))
    })?;
    let mut bytes = Vec::with_capacity(1 + json.len());
    // Version 1 was the legacy bare-cursor encoding from DurableCursorStore.
    bytes.push(WORKFLOW_SOURCE_PROGRESS_FORMAT_VERSION);
    bytes.extend_from_slice(&json);
    Ok(bytes)
}

fn decode_workflow_source_progress(bytes: &[u8]) -> Result<WorkflowSourceProgress, StorageError> {
    if bytes.first().copied() == Some(WORKFLOW_SOURCE_PROGRESS_FORMAT_VERSION) {
        return serde_json::from_slice(&bytes[1..]).map_err(|error| {
            StorageError::corruption(format!("workflow source progress decoding failed: {error}"))
        });
    }

    if bytes.len() == 1 + LogCursor::ENCODED_LEN && bytes.first().copied() == Some(1) {
        let cursor = LogCursor::decode(&bytes[1..])
            .map_err(|error| StorageError::corruption(error.to_string()))?;
        return Ok(WorkflowSourceProgress::from_cursor(cursor));
    }

    Err(StorageError::corruption(
        "workflow source progress encoding is invalid",
    ))
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

fn encode_versioned_json<T>(version: u8, value: &T, context: &str) -> Result<Vec<u8>, StorageError>
where
    T: Serialize,
{
    let json = serde_json::to_vec(value).map_err(|error| {
        StorageError::corruption(format!("{context} serialization failed: {error}"))
    })?;
    let mut bytes = Vec::with_capacity(1 + json.len());
    bytes.push(version);
    bytes.extend_from_slice(&json);
    Ok(bytes)
}

fn decode_versioned_json<T>(
    expected_version: u8,
    bytes: &[u8],
    context: &str,
) -> Result<T, StorageError>
where
    T: DeserializeOwned,
{
    if bytes.first().copied() != Some(expected_version) {
        return Err(StorageError::corruption(format!(
            "{context} version is invalid"
        )));
    }
    serde_json::from_slice(&bytes[1..])
        .map_err(|error| StorageError::corruption(format!("{context} decoding failed: {error}")))
}

fn encode_checkpoint_table_artifact(
    kind: WorkflowCheckpointArtifactKind,
    rows: &[WorkflowCheckpointTableRow],
) -> Result<Vec<u8>, WorkflowError> {
    encode_versioned_json(
        WORKFLOW_CHECKPOINT_ARTIFACT_FORMAT_VERSION,
        &WorkflowCheckpointTableArtifact {
            kind,
            rows: rows.to_vec(),
        },
        "workflow checkpoint artifact",
    )
    .map_err(Into::into)
}

fn decode_checkpoint_table_artifact(
    kind: WorkflowCheckpointArtifactKind,
    bytes: &[u8],
) -> Result<Vec<WorkflowCheckpointTableRow>, WorkflowError> {
    let artifact = decode_versioned_json::<WorkflowCheckpointTableArtifact>(
        WORKFLOW_CHECKPOINT_ARTIFACT_FORMAT_VERSION,
        bytes,
        "workflow checkpoint artifact",
    )?;
    if artifact.kind != kind {
        return Err(StorageError::corruption(format!(
            "workflow checkpoint artifact kind mismatch: expected {:?}, got {:?}",
            kind, artifact.kind
        ))
        .into());
    }
    Ok(artifact.rows)
}

fn join_object_key(prefix: &str, relative: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let relative = relative.trim_matches('/');
    if prefix.is_empty() {
        relative.to_string()
    } else if relative.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}/{relative}")
    }
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

fn encode_recurring_state(state: &RecurringWorkflowState) -> Result<Value, WorkflowHandlerError> {
    Ok(Value::bytes(
        encode_payload(state, "recurring workflow state").map_err(WorkflowHandlerError::new)?,
    ))
}

fn decode_recurring_state(
    value: Option<&Value>,
) -> Result<Option<RecurringWorkflowState>, WorkflowError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let bytes = expect_bytes_value(value, "recurring workflow state")?;
    Ok(Some(decode_payload(bytes, "recurring workflow state")?))
}

fn recurring_timer_id(instance_id: &str) -> Key {
    format!("__terracedb.recurring.timer:{instance_id}").into_bytes()
}

fn recurring_timer_schedule(
    instance_id: &str,
    next_fire_at: Option<Timestamp>,
) -> Vec<WorkflowTimerCommand> {
    next_fire_at
        .into_iter()
        .map(|fire_at| WorkflowTimerCommand::Schedule {
            timer_id: recurring_timer_id(instance_id),
            fire_at,
            payload: Vec::new(),
        })
        .collect()
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WorkflowCheckpointLatestPointer {
    checkpoint_id: WorkflowCheckpointId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct WorkflowCheckpointTableRow {
    key: Key,
    value: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct WorkflowCheckpointTableArtifact {
    kind: WorkflowCheckpointArtifactKind,
    rows: Vec<WorkflowCheckpointTableRow>,
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
