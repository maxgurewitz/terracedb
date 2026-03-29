use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    hash::{Hash, Hasher},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};

use crc32fast::hash as checksum32;
use futures::{Stream, stream};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::sync::{Mutex as AsyncMutex, watch};

use crate::{
    config::{
        CompactionStrategy, DbConfig, S3PrimaryStorageConfig, StorageConfig, TableConfig,
        TableFormat, TableMetadata, TieredDurabilityMode, TieredStorageConfig,
    },
    engine::commit_log::{CommitEntry, CommitRecord, SegmentManager, SegmentOptions},
    error::{
        CommitError, CreateTableError, FlushError, OpenError, ReadError, SnapshotTooOld,
        StorageError, StorageErrorKind, SubscriptionClosed, WriteError,
    },
    ids::{CommitId, FieldId, LogCursor, ManifestId, SequenceNumber, TableId},
    io::{DbDependencies, OpenOptions},
    scheduler::{PendingWork, TableStats},
};

pub type Key = Vec<u8>;
pub type KeyPrefix = Vec<u8>;
pub type KvStream = Pin<Box<dyn Stream<Item = (Key, Value)> + Send + 'static>>;
pub type ChangeStream = Pin<Box<dyn Stream<Item = ChangeEntry> + Send + 'static>>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub version: u32,
    pub fields: Vec<FieldDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub id: FieldId,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    pub nullable: bool,
    pub default: Option<FieldValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Int64,
    Float64,
    String,
    Bytes,
    Bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldValue {
    Null,
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Bool(bool),
}

pub type ColumnarRecord = BTreeMap<FieldId, FieldValue>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Bytes(Vec<u8>),
    Record(ColumnarRecord),
}

impl Value {
    pub fn bytes(value: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(value.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeKind {
    Put,
    Delete,
    Merge,
}

#[derive(Clone, Debug)]
pub struct ChangeEntry {
    pub key: Key,
    pub value: Option<Value>,
    pub cursor: LogCursor,
    pub sequence: SequenceNumber,
    pub kind: ChangeKind,
    pub table: Table,
}

#[derive(Clone, Debug)]
pub enum BatchOperation {
    Put {
        table: Table,
        key: Key,
        value: Value,
    },
    Merge {
        table: Table,
        key: Key,
        value: Value,
    },
    Delete {
        table: Table,
        key: Key,
    },
}

#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

impl WriteBatch {
    pub fn put(&mut self, table: &Table, key: Key, value: Value) {
        self.operations.push(BatchOperation::Put {
            table: table.clone(),
            key,
            value,
        });
    }

    pub fn merge(&mut self, table: &Table, key: Key, value: Value) {
        self.operations.push(BatchOperation::Merge {
            table: table.clone(),
            key,
            value,
        });
    }

    pub fn delete(&mut self, table: &Table, key: Key) {
        self.operations.push(BatchOperation::Delete {
            table: table.clone(),
            key,
        });
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn operations(&self) -> &[BatchOperation] {
        &self.operations
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSetEntry {
    pub table: Table,
    pub key: Key,
    pub at_sequence: SequenceNumber,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadSet {
    entries: Vec<ReadSetEntry>,
}

impl ReadSet {
    pub fn add(&mut self, table: &Table, key: Key, at_sequence: SequenceNumber) {
        self.entries.push(ReadSetEntry {
            table: table.clone(),
            key,
            at_sequence,
        });
    }

    pub fn entries(&self) -> &[ReadSetEntry] {
        &self.entries
    }
}

#[derive(Clone, Debug, Default)]
pub struct CommitOptions {
    pub read_set: Option<ReadSet>,
}

impl CommitOptions {
    pub fn with_read_set(mut self, read_set: ReadSet) -> Self {
        self.read_set = Some(read_set);
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanOptions {
    pub reverse: bool,
    pub limit: Option<usize>,
    pub columns: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct WatermarkReceiver {
    inner: watch::Receiver<SequenceNumber>,
}

impl WatermarkReceiver {
    pub fn current(&self) -> SequenceNumber {
        *self.inner.borrow()
    }

    pub async fn changed(&mut self) -> Result<SequenceNumber, SubscriptionClosed> {
        self.inner.changed().await.map_err(|_| SubscriptionClosed)?;
        Ok(*self.inner.borrow_and_update())
    }
}

impl Clone for WatermarkReceiver {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

const CATALOG_FORMAT_VERSION: u32 = 1;
const CATALOG_READ_CHUNK_LEN: usize = 8 * 1024;
const LOCAL_CATALOG_RELATIVE_PATH: &str = "catalog/CATALOG.json";
const LOCAL_CATALOG_TEMP_SUFFIX: &str = ".tmp";
const LOCAL_COMMIT_LOG_RELATIVE_DIR: &str = "commitlog";
const OBJECT_CATALOG_RELATIVE_KEY: &str = "catalog/CATALOG.json";
const LOCAL_CURRENT_RELATIVE_PATH: &str = "CURRENT";
const LOCAL_MANIFEST_DIR_RELATIVE_PATH: &str = "manifest";
const LOCAL_MANIFEST_TEMP_SUFFIX: &str = ".tmp";
const LOCAL_SSTABLE_RELATIVE_DIR: &str = "sst";
const LOCAL_SSTABLE_SHARD_DIR: &str = "0000";
const MANIFEST_FORMAT_VERSION: u32 = 1;
const LEVELED_BASE_LEVEL_TARGET_BYTES: u64 = 4 * 1024;
const LEVELED_LEVEL_SIZE_MULTIPLIER: u64 = 10;
const LEVELED_L0_COMPACTION_TRIGGER: usize = 2;
const TIERED_LEVEL_RUN_COMPACTION_TRIGGER: usize = 3;
const FIFO_MAX_LIVE_SSTABLES: usize = 2;
const ROW_SSTABLE_FORMAT_VERSION: u32 = 1;
const MVCC_KEY_SEPARATOR: u8 = 0;
const DEFAULT_MAX_MERGE_OPERAND_CHAIN_LENGTH: usize = 8;

#[derive(Clone)]
pub struct Db {
    inner: Arc<DbInner>,
}

struct DbInner {
    config: DbConfig,
    dependencies: DbDependencies,
    catalog_location: CatalogLocation,
    catalog_write_lock: AsyncMutex<()>,
    commit_lock: AsyncMutex<()>,
    maintenance_lock: AsyncMutex<()>,
    commit_runtime: AsyncMutex<CommitRuntime>,
    commit_coordinator: Mutex<CommitCoordinator>,
    next_table_id: AtomicU32,
    next_sequence: AtomicU64,
    current_sequence: AtomicU64,
    current_durable_sequence: AtomicU64,
    tables: RwLock<BTreeMap<String, StoredTable>>,
    memtables: RwLock<MemtableState>,
    sstables: RwLock<SstableState>,
    next_sstable_id: AtomicU64,
    snapshot_tracker: Mutex<SnapshotTracker>,
    next_snapshot_id: AtomicU64,
    visible_watchers: Mutex<BTreeMap<String, watch::Sender<SequenceNumber>>>,
    durable_watchers: Mutex<BTreeMap<String, watch::Sender<SequenceNumber>>>,
    #[cfg(test)]
    commit_phase_blocks: Mutex<Vec<CommitPhaseBlock>>,
    #[cfg(test)]
    compaction_phase_blocks: Mutex<Vec<CompactionPhaseBlock>>,
}

#[derive(Clone)]
struct StoredTable {
    id: TableId,
    config: TableConfig,
}

#[derive(Clone, Debug)]
enum CatalogLocation {
    LocalFile { path: String, temp_path: String },
    ObjectStore { key: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedCatalog {
    format_version: u32,
    tables: Vec<PersistedCatalogEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedCatalogEntry {
    id: TableId,
    config: PersistedTableConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedTableConfig {
    name: String,
    format: TableFormat,
    max_merge_operand_chain_length: Option<u32>,
    bloom_filter_bits_per_key: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    history_retention_sequences: Option<u64>,
    compaction_strategy: CompactionStrategy,
    schema: Option<SchemaDefinition>,
    metadata: TableMetadata,
}

#[derive(Clone, Debug, Default)]
struct SstableState {
    manifest_generation: ManifestId,
    last_flushed_sequence: SequenceNumber,
    live: Vec<ResidentRowSstable>,
}

#[derive(Clone, Debug)]
struct ResidentRowSstable {
    meta: PersistedManifestSstable,
    rows: Vec<SstableRow>,
    user_key_bloom_filter: Option<UserKeyBloomFilter>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedManifestSstable {
    table_id: TableId,
    level: u32,
    local_id: String,
    file_path: String,
    length: u64,
    checksum: u32,
    data_checksum: u32,
    min_key: Key,
    max_key: Key,
    min_sequence: SequenceNumber,
    max_sequence: SequenceNumber,
}

#[derive(Clone, Debug, Default)]
struct LoadedManifest {
    generation: ManifestId,
    last_flushed_sequence: SequenceNumber,
    live_sstables: Vec<ResidentRowSstable>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedManifestBody {
    format_version: u32,
    generation: ManifestId,
    last_flushed_sequence: SequenceNumber,
    sstables: Vec<PersistedManifestSstable>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedManifestFile {
    body: PersistedManifestBody,
    checksum: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedRowSstableBody {
    format_version: u32,
    table_id: TableId,
    level: u32,
    local_id: String,
    min_key: Key,
    max_key: Key,
    min_sequence: SequenceNumber,
    max_sequence: SequenceNumber,
    rows: Vec<SstableRow>,
    data_checksum: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user_key_bloom_filter: Option<UserKeyBloomFilter>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedRowSstableFile {
    body: PersistedRowSstableBody,
    checksum: u32,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactionJobKind {
    Rewrite,
    DeleteOnly,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
struct CompactionJob {
    id: String,
    table_id: TableId,
    table_name: String,
    source_level: u32,
    target_level: u32,
    kind: CompactionJobKind,
    estimated_bytes: u64,
    input_local_ids: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct TableCompactionState {
    compaction_debt: u64,
    next_job: Option<CompactionJob>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SstableRow {
    key: Key,
    sequence: SequenceNumber,
    kind: ChangeKind,
    value: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct UserKeyBloomFilter {
    bits_per_key: u32,
    hash_count: u32,
    bytes: Vec<u8>,
}

impl UserKeyBloomFilter {
    fn build(rows: &[SstableRow], bits_per_key: Option<u32>) -> Option<Self> {
        let bits_per_key = bits_per_key?.max(1);

        let mut unique_keys = BTreeSet::new();
        for row in rows {
            unique_keys.insert(row.key.as_slice());
        }

        if unique_keys.is_empty() {
            return None;
        }

        let bit_count = unique_keys
            .len()
            .saturating_mul(bits_per_key as usize)
            .max(8);
        let byte_len = bit_count.div_ceil(8);
        let total_bits = byte_len * 8;
        let hash_count = ((bits_per_key.saturating_mul(69)).saturating_add(50) / 100).max(1);
        let mut filter = Self {
            bits_per_key,
            hash_count,
            bytes: vec![0; byte_len],
        };

        for key in unique_keys {
            filter.insert(key, total_bits);
        }

        Some(filter)
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        if self.bytes.is_empty() || self.hash_count == 0 {
            return true;
        }

        let total_bits = self.bytes.len() * 8;
        let (h1, h2) = bloom_hash_pair(key);
        for i in 0..self.hash_count {
            let bit_index = h1.wrapping_add((i as u64).wrapping_mul(h2)) % total_bits as u64;
            let byte_index = (bit_index / 8) as usize;
            let bit_mask = 1_u8 << ((bit_index % 8) as u8);
            if self.bytes[byte_index] & bit_mask == 0 {
                return false;
            }
        }

        true
    }

    fn insert(&mut self, key: &[u8], total_bits: usize) {
        let (h1, h2) = bloom_hash_pair(key);
        for i in 0..self.hash_count {
            let bit_index = h1.wrapping_add((i as u64).wrapping_mul(h2)) % total_bits as u64;
            let byte_index = (bit_index / 8) as usize;
            let bit_mask = 1_u8 << ((bit_index % 8) as u8);
            self.bytes[byte_index] |= bit_mask;
        }
    }
}

#[derive(Clone, Debug)]
struct ResolvedBatchOperation {
    table_id: TableId,
    table_name: String,
    key: Key,
    kind: ChangeKind,
    value: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
struct MergeCollapse {
    sequence: SequenceNumber,
    value: Value,
}

#[derive(Clone, Debug, PartialEq)]
struct VisibleValueResolution {
    value: Option<Value>,
    collapse: Option<MergeCollapse>,
}

#[derive(Clone, Debug)]
struct ResolvedReadSetEntry {
    table_id: TableId,
    key: Key,
    at_sequence: SequenceNumber,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CommitConflictKey {
    table_id: TableId,
    key: Key,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitPhase {
    BeforeDurabilitySync,
    AfterDurabilitySync,
    BeforeMemtableInsert,
    AfterMemtableInsert,
    AfterVisibilityPublish,
    AfterDurablePublish,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactionPhase {
    OutputWritten,
    ManifestSwitched,
    InputCleanupFinished,
}

#[cfg(test)]
struct CommitPhaseBlock {
    phase: CommitPhase,
    sequence_tx: Option<oneshot::Sender<SequenceNumber>>,
    release_rx: Option<oneshot::Receiver<()>>,
}

#[cfg(test)]
struct CommitPhaseBlocker {
    sequence_rx: Option<oneshot::Receiver<SequenceNumber>>,
    release_tx: Option<oneshot::Sender<()>>,
}

#[cfg(test)]
struct CompactionPhaseBlock {
    phase: CompactionPhase,
    reached_tx: Option<oneshot::Sender<()>>,
    release_rx: Option<oneshot::Receiver<()>>,
}

#[cfg(test)]
struct CompactionPhaseBlocker {
    reached_rx: Option<oneshot::Receiver<()>>,
    release_tx: Option<oneshot::Sender<()>>,
}

#[cfg(test)]
impl CommitPhaseBlocker {
    async fn sequence(&mut self) -> SequenceNumber {
        self.sequence_rx
            .take()
            .expect("commit phase blocker sequence receiver should be available")
            .await
            .expect("commit phase blocker should observe a sequence")
    }

    fn release(&mut self) {
        if let Some(release_tx) = self.release_tx.take() {
            let _ = release_tx.send(());
        }
    }
}

#[cfg(test)]
impl CompactionPhaseBlocker {
    async fn wait_until_reached(&mut self) {
        self.reached_rx
            .take()
            .expect("compaction phase blocker should have a receiver")
            .await
            .expect("compaction phase blocker should observe the phase");
    }

    fn release(&mut self) {
        if let Some(release_tx) = self.release_tx.take() {
            let _ = release_tx.send(());
        }
    }
}

struct CommitRuntime {
    backend: CommitLogBackend,
}

#[derive(Clone, Debug, Default)]
struct RecoveredCommitLogState {
    memtables: MemtableState,
    max_sequence: SequenceNumber,
}

enum CommitLogBackend {
    Local(Box<SegmentManager>),
    Memory(MemoryCommitLog),
}

#[derive(Default)]
struct MemoryCommitLog {
    records: Vec<CommitRecord>,
}

impl CommitRuntime {
    async fn append(&mut self, record: CommitRecord) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(manager) => {
                manager.append(record).await?;
            }
            CommitLogBackend::Memory(log) => {
                log.records.push(record);
            }
        }

        Ok(())
    }

    async fn sync(&mut self) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(manager) => manager.sync_active().await,
            CommitLogBackend::Memory(_) => Ok(()),
        }
    }

    async fn recover_after(
        &self,
        after_sequence: SequenceNumber,
    ) -> Result<RecoveredCommitLogState, StorageError> {
        let records = match &self.backend {
            CommitLogBackend::Local(manager) => manager.scan_from_sequence(after_sequence).await?,
            CommitLogBackend::Memory(_) => Vec::new(),
        };

        let mut recovered = RecoveredCommitLogState {
            max_sequence: after_sequence,
            ..RecoveredCommitLogState::default()
        };
        for record in records {
            recovered.max_sequence = recovered.max_sequence.max(record.sequence());
            recovered.memtables.apply_recovered_record(&record);
        }

        Ok(recovered)
    }
}

#[derive(Default)]
struct CommitCoordinator {
    keys: BTreeMap<CommitConflictKey, BTreeSet<SequenceNumber>>,
    sequences: BTreeMap<SequenceNumber, SequenceCommitState>,
    current_batch: Option<Arc<GroupCommitBatch>>,
}

#[derive(Clone, Debug, Default)]
struct SequenceCommitState {
    touched_tables: BTreeSet<String>,
    memtable_inserted: bool,
    durable_confirmed: bool,
    visible_published: bool,
    durable_published: bool,
    aborted: bool,
}

struct GroupCommitBatch {
    state: Mutex<GroupCommitBatchState>,
    notify: Notify,
}

#[derive(Clone, Debug, Default)]
struct GroupCommitBatchState {
    sealed: bool,
    result: Option<Result<(), StorageError>>,
}

impl GroupCommitBatch {
    fn new() -> Self {
        Self {
            state: Mutex::new(GroupCommitBatchState::default()),
            notify: Notify::new(),
        }
    }

    fn is_open(&self) -> bool {
        !mutex_lock(&self.state).sealed
    }

    fn result(&self) -> Option<Result<(), StorageError>> {
        mutex_lock(&self.state).result.clone()
    }
}

struct CommitParticipant {
    sequence: SequenceNumber,
    operations: Vec<ResolvedBatchOperation>,
    conflict_keys: Vec<CommitConflictKey>,
    group_batch: Option<Arc<GroupCommitBatch>>,
}

#[derive(Default)]
struct WatermarkAdvance {
    visible_sequence: Option<SequenceNumber>,
    visible_tables: BTreeMap<String, SequenceNumber>,
    durable_sequence: Option<SequenceNumber>,
    durable_tables: BTreeMap<String, SequenceNumber>,
}

#[derive(Clone, Debug)]
struct MemtableEntry {
    user_key: Key,
    sequence: SequenceNumber,
    kind: ChangeKind,
    value: Option<Value>,
    size_bytes: u64,
}

impl MemtableEntry {
    fn new(
        user_key: Key,
        sequence: SequenceNumber,
        kind: ChangeKind,
        value: Option<Value>,
    ) -> Self {
        let size_bytes = (user_key.len()
            + encode_mvcc_key(&user_key, CommitId::new(sequence)).len()
            + value.as_ref().map(value_size_bytes).unwrap_or_default())
            as u64;

        Self {
            user_key,
            sequence,
            kind,
            value,
            size_bytes,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct TableMemtable {
    entries: BTreeMap<Vec<u8>, MemtableEntry>,
    pending_flush_bytes: u64,
}

impl TableMemtable {
    fn insert(&mut self, entry: MemtableEntry) {
        let encoded_key = encode_mvcc_key(&entry.user_key, CommitId::new(entry.sequence));
        if let Some(replaced) = self.entries.insert(encoded_key, entry.clone()) {
            self.pending_flush_bytes = self.pending_flush_bytes.saturating_sub(replaced.size_bytes);
        }
        self.pending_flush_bytes = self.pending_flush_bytes.saturating_add(entry.size_bytes);
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn read_at(&self, key: &[u8], sequence: SequenceNumber) -> Option<MemtableEntry> {
        let seek = encode_mvcc_key(key, CommitId::new(sequence));
        let (_encoded_key, entry) = self.entries.range(seek..).next()?;
        (entry.user_key.as_slice() == key).then(|| entry.clone())
    }

    fn collect_matching_keys(&self, matcher: &KeyMatcher<'_>, keys: &mut BTreeSet<Key>) {
        for entry in self.entries.values() {
            if matcher.matches(&entry.user_key) {
                keys.insert(entry.user_key.clone());
            }
        }
    }

    fn collect_visible_rows(
        &self,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        let seek = encode_mvcc_key(key, CommitId::new(sequence));
        for (_encoded_key, entry) in self.entries.range(seek..) {
            match entry.user_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    if entry.sequence <= sequence {
                        rows.push(SstableRow {
                            key: entry.user_key.clone(),
                            sequence: entry.sequence,
                            kind: entry.kind,
                            value: entry.value.clone(),
                        });
                    }
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
struct Memtable {
    tables: BTreeMap<TableId, TableMemtable>,
    max_sequence: SequenceNumber,
}

impl Memtable {
    fn apply(&mut self, sequence: SequenceNumber, operation: &ResolvedBatchOperation) {
        self.max_sequence = self.max_sequence.max(sequence);
        self.tables
            .entry(operation.table_id)
            .or_default()
            .insert(MemtableEntry::new(
                operation.key.clone(),
                sequence,
                operation.kind,
                operation.value.clone(),
            ));
    }

    fn apply_recovered_entry(&mut self, sequence: SequenceNumber, entry: &CommitEntry) {
        self.max_sequence = self.max_sequence.max(sequence);
        self.tables
            .entry(entry.table_id)
            .or_default()
            .insert(MemtableEntry::new(
                entry.key.clone(),
                sequence,
                entry.kind,
                entry.value.clone(),
            ));
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn read_at(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Option<MemtableEntry> {
        self.tables.get(&table_id)?.read_at(key, sequence)
    }

    fn collect_matching_keys(
        &self,
        table_id: TableId,
        matcher: &KeyMatcher<'_>,
        keys: &mut BTreeSet<Key>,
    ) {
        if let Some(table) = self.tables.get(&table_id) {
            table.collect_matching_keys(matcher, keys);
        }
    }

    fn collect_visible_rows(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        if let Some(table) = self.tables.get(&table_id) {
            table.collect_visible_rows(key, sequence, rows);
        }
    }

    fn force_collapse(
        &mut self,
        table_id: TableId,
        key: Key,
        sequence: SequenceNumber,
        value: Value,
    ) {
        self.max_sequence = self.max_sequence.max(sequence);
        self.tables
            .entry(table_id)
            .or_default()
            .insert(MemtableEntry::new(
                key,
                sequence,
                ChangeKind::Put,
                Some(value),
            ));
    }

    fn pending_flush_bytes(&self, table_id: TableId) -> u64 {
        self.tables
            .get(&table_id)
            .map(|table| table.pending_flush_bytes)
            .unwrap_or_default()
    }

    fn has_entries_for_table(&self, table_id: TableId) -> bool {
        self.tables
            .get(&table_id)
            .map(|table| !table.is_empty())
            .unwrap_or(false)
    }

    fn is_empty(&self) -> bool {
        self.tables.values().all(TableMemtable::is_empty)
    }
}

impl ResidentRowSstable {
    fn collect_matching_keys(&self, matcher: &KeyMatcher<'_>, keys: &mut BTreeSet<Key>) {
        let start = match matcher {
            KeyMatcher::Range { start, .. } | KeyMatcher::Prefix(start) => self.lower_bound(start),
        };
        let mut last_key: Option<&[u8]> = None;

        for row in &self.rows[start..] {
            if !matcher.matches(&row.key) {
                if matcher.is_past_end(&row.key) {
                    break;
                }
                continue;
            }

            if last_key == Some(row.key.as_slice()) {
                continue;
            }

            keys.insert(row.key.clone());
            last_key = Some(row.key.as_slice());
        }
    }

    fn collect_visible_rows(
        &self,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        if key < self.meta.min_key.as_slice()
            || key > self.meta.max_key.as_slice()
            || sequence < self.meta.min_sequence
        {
            return;
        }

        if let Some(filter) = &self.user_key_bloom_filter
            && !filter.may_contain(key)
        {
            return;
        }

        let start = self.lower_bound(key);
        for row in &self.rows[start..] {
            match row.key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    if row.sequence <= sequence {
                        rows.push(row.clone());
                    }
                }
            }
        }
    }

    fn lower_bound(&self, target: &[u8]) -> usize {
        self.rows.partition_point(|row| row.key.as_slice() < target)
    }
}

impl SstableState {
    fn collect_matching_keys(
        &self,
        table_id: TableId,
        matcher: &KeyMatcher<'_>,
        keys: &mut BTreeSet<Key>,
    ) {
        for sstable in &self.live {
            if sstable.meta.table_id != table_id {
                continue;
            }

            sstable.collect_matching_keys(matcher, keys);
        }
    }

    fn collect_visible_rows(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        for sstable in &self.live {
            if sstable.meta.table_id != table_id {
                continue;
            }

            sstable.collect_visible_rows(key, sequence, rows);
        }
    }

    fn table_stats(&self, table_id: TableId) -> (u32, u64, u64) {
        let matching = self
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table_id)
            .collect::<Vec<_>>();

        let l0_count = matching
            .iter()
            .filter(|sstable| sstable.meta.level == 0)
            .count() as u32;
        let local_bytes = matching
            .iter()
            .map(|sstable| sstable.meta.length)
            .sum::<u64>();
        (l0_count, local_bytes, local_bytes)
    }
}

#[derive(Clone, Debug)]
struct ImmutableMemtable {
    max_sequence: SequenceNumber,
    memtable: Memtable,
}

#[derive(Clone, Debug, Default)]
struct MemtableState {
    mutable: Memtable,
    immutables: Vec<ImmutableMemtable>,
}

impl MemtableState {
    fn apply(&mut self, sequence: SequenceNumber, operations: &[ResolvedBatchOperation]) {
        for operation in operations {
            self.mutable.apply(sequence, operation);
        }
    }

    fn apply_recovered_record(&mut self, record: &CommitRecord) {
        for entry in &record.entries {
            self.mutable.apply_recovered_entry(record.sequence(), entry);
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn read_at(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Option<MemtableEntry> {
        let mut best = self.mutable.read_at(table_id, key, sequence);

        for immutable in self.immutables.iter().rev() {
            if immutable.max_sequence < sequence
                && let Some(current) = best.as_ref()
                && current.sequence >= immutable.max_sequence
            {
                continue;
            }

            let Some(candidate) = immutable.memtable.read_at(table_id, key, sequence) else {
                continue;
            };

            let replace = match best.as_ref() {
                Some(current) => candidate.sequence > current.sequence,
                None => true,
            };
            if replace {
                best = Some(candidate);
            }
        }

        best
    }

    fn collect_matching_keys(
        &self,
        table_id: TableId,
        matcher: &KeyMatcher<'_>,
        keys: &mut BTreeSet<Key>,
    ) {
        self.mutable.collect_matching_keys(table_id, matcher, keys);
        for immutable in &self.immutables {
            immutable
                .memtable
                .collect_matching_keys(table_id, matcher, keys);
        }
    }

    fn collect_visible_rows(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        self.mutable
            .collect_visible_rows(table_id, key, sequence, rows);
        for immutable in &self.immutables {
            immutable
                .memtable
                .collect_visible_rows(table_id, key, sequence, rows);
        }
    }

    fn force_collapse(
        &mut self,
        table_id: TableId,
        key: Key,
        sequence: SequenceNumber,
        value: Value,
    ) {
        self.mutable.force_collapse(table_id, key, sequence, value);
    }

    fn rotate_mutable(&mut self) -> Option<SequenceNumber> {
        if self.mutable.is_empty() {
            return None;
        }

        let rotated = std::mem::take(&mut self.mutable);
        let max_sequence = rotated.max_sequence;
        self.immutables.push(ImmutableMemtable {
            max_sequence,
            memtable: rotated,
        });
        Some(max_sequence)
    }

    fn pending_flush_bytes(&self, table_id: TableId) -> u64 {
        let mutable_bytes = self.mutable.pending_flush_bytes(table_id);
        let immutable_bytes = self
            .immutables
            .iter()
            .map(|immutable| immutable.memtable.pending_flush_bytes(table_id))
            .sum::<u64>();

        mutable_bytes.saturating_add(immutable_bytes)
    }

    fn immutable_memtable_count(&self, table_id: TableId) -> u32 {
        self.immutables
            .iter()
            .filter(|immutable| immutable.memtable.has_entries_for_table(table_id))
            .count() as u32
    }
}

#[derive(Clone, Debug)]
enum KeyMatcher<'a> {
    Range { start: &'a [u8], end: &'a [u8] },
    Prefix(&'a [u8]),
}

impl KeyMatcher<'_> {
    fn matches(&self, key: &[u8]) -> bool {
        match self {
            Self::Range { start, end } => key >= *start && key < *end,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }

    fn is_past_end(&self, key: &[u8]) -> bool {
        match self {
            Self::Range { end, .. } => key >= *end,
            Self::Prefix(prefix) => key > *prefix,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SnapshotTracker {
    registrations: BTreeMap<u64, SequenceNumber>,
    counts_by_sequence: BTreeMap<SequenceNumber, usize>,
}

impl SnapshotTracker {
    fn register(&mut self, id: u64, sequence: SequenceNumber) {
        self.registrations.insert(id, sequence);
        *self.counts_by_sequence.entry(sequence).or_default() += 1;
    }

    fn release(&mut self, id: u64) {
        let Some(sequence) = self.registrations.remove(&id) else {
            return;
        };

        let mut remove_key = false;
        if let Some(count) = self.counts_by_sequence.get_mut(&sequence) {
            *count = count.saturating_sub(1);
            remove_key = *count == 0;
        }

        if remove_key {
            self.counts_by_sequence.remove(&sequence);
        }
    }

    fn oldest_active(&self) -> Option<SequenceNumber> {
        self.counts_by_sequence.keys().next().copied()
    }

    fn count(&self) -> u64 {
        self.registrations.len() as u64
    }
}

fn encode_mvcc_key(user_key: &[u8], commit_id: CommitId) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(user_key.len() + 1 + CommitId::ENCODED_LEN);
    encoded.extend_from_slice(user_key);
    encoded.push(MVCC_KEY_SEPARATOR);
    encoded.extend(commit_id.encode().into_iter().map(|byte| !byte));
    encoded
}

#[cfg(test)]
fn decode_mvcc_key(encoded: &[u8]) -> Result<(Key, CommitId), StorageError> {
    if encoded.len() < CommitId::ENCODED_LEN + 1 {
        return Err(StorageError::corruption("mvcc key is too short"));
    }

    let separator_index = encoded.len() - CommitId::ENCODED_LEN - 1;
    if encoded[separator_index] != MVCC_KEY_SEPARATOR {
        return Err(StorageError::corruption("mvcc key missing separator"));
    }

    let mut commit_bytes = [0_u8; CommitId::ENCODED_LEN];
    for (decoded, source) in commit_bytes.iter_mut().zip(&encoded[separator_index + 1..]) {
        *decoded = !*source;
    }

    let commit_id = CommitId::decode(&commit_bytes)
        .map_err(|error| StorageError::corruption(format!("decode mvcc key failed: {error}")))?;
    Ok((encoded[..separator_index].to_vec(), commit_id))
}

fn value_size_bytes(value: &Value) -> usize {
    match value {
        Value::Bytes(bytes) => bytes.len(),
        Value::Record(record) => record.values().map(field_value_size_bytes).sum(),
    }
}

fn field_value_size_bytes(value: &FieldValue) -> usize {
    match value {
        FieldValue::Null => 0,
        FieldValue::Int64(_) | FieldValue::Float64(_) => 8,
        FieldValue::String(value) => value.len(),
        FieldValue::Bytes(value) => value.len(),
        FieldValue::Bool(_) => 1,
    }
}

impl PersistedCatalog {
    fn from_tables(tables: &BTreeMap<String, StoredTable>) -> Self {
        Self {
            format_version: CATALOG_FORMAT_VERSION,
            tables: tables
                .values()
                .map(PersistedCatalogEntry::from_stored)
                .collect(),
        }
    }
}

impl Default for PersistedCatalog {
    fn default() -> Self {
        Self {
            format_version: CATALOG_FORMAT_VERSION,
            tables: Vec::new(),
        }
    }
}

impl PersistedCatalogEntry {
    fn from_stored(table: &StoredTable) -> Self {
        Self {
            id: table.id,
            config: PersistedTableConfig {
                name: table.config.name.clone(),
                format: table.config.format,
                max_merge_operand_chain_length: table.config.max_merge_operand_chain_length,
                bloom_filter_bits_per_key: table.config.bloom_filter_bits_per_key,
                history_retention_sequences: table.config.history_retention_sequences,
                compaction_strategy: table.config.compaction_strategy,
                schema: table.config.schema.clone(),
                metadata: table.config.metadata.clone(),
            },
        }
    }

    fn into_stored(self) -> StoredTable {
        StoredTable {
            id: self.id,
            config: TableConfig {
                name: self.config.name,
                format: self.config.format,
                merge_operator: None,
                max_merge_operand_chain_length: self.config.max_merge_operand_chain_length,
                compaction_filter: None,
                bloom_filter_bits_per_key: self.config.bloom_filter_bits_per_key,
                history_retention_sequences: self.config.history_retention_sequences,
                compaction_strategy: self.config.compaction_strategy,
                schema: self.config.schema,
                metadata: self.config.metadata,
            },
        }
    }
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("storage", &self.inner.config.storage)
            .field("current_sequence", &self.current_sequence())
            .field("current_durable_sequence", &self.current_durable_sequence())
            .finish()
    }
}

impl Db {
    pub async fn open(config: DbConfig, dependencies: DbDependencies) -> Result<Self, OpenError> {
        Self::validate_storage_config(&config.storage)?;
        let catalog_location = Self::catalog_location(&config.storage);
        let (tables, next_table_id) = Self::load_tables(&dependencies, &catalog_location).await?;
        let commit_runtime = Self::open_commit_runtime(&config.storage, &dependencies).await?;
        let loaded_manifest = Self::load_local_manifest(&config.storage, &dependencies).await?;
        let recovered_commit_log = commit_runtime
            .recover_after(loaded_manifest.last_flushed_sequence)
            .await?;
        let recovered_sequence = recovered_commit_log.max_sequence;
        let next_sstable_id = loaded_manifest
            .live_sstables
            .iter()
            .filter_map(|sstable| Self::parse_sstable_local_id(&sstable.meta.local_id))
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(1);

        Ok(Self {
            inner: Arc::new(DbInner {
                config,
                dependencies,
                catalog_location,
                catalog_write_lock: AsyncMutex::new(()),
                commit_lock: AsyncMutex::new(()),
                maintenance_lock: AsyncMutex::new(()),
                commit_runtime: AsyncMutex::new(commit_runtime),
                commit_coordinator: Mutex::new(CommitCoordinator::default()),
                next_table_id: AtomicU32::new(next_table_id),
                next_sequence: AtomicU64::new(recovered_sequence.get()),
                current_sequence: AtomicU64::new(recovered_sequence.get()),
                current_durable_sequence: AtomicU64::new(recovered_sequence.get()),
                tables: RwLock::new(tables),
                memtables: RwLock::new(recovered_commit_log.memtables),
                sstables: RwLock::new(SstableState {
                    manifest_generation: loaded_manifest.generation,
                    last_flushed_sequence: loaded_manifest.last_flushed_sequence,
                    live: loaded_manifest.live_sstables,
                }),
                next_sstable_id: AtomicU64::new(next_sstable_id),
                snapshot_tracker: Mutex::new(SnapshotTracker::default()),
                next_snapshot_id: AtomicU64::new(0),
                visible_watchers: Mutex::new(BTreeMap::new()),
                durable_watchers: Mutex::new(BTreeMap::new()),
                #[cfg(test)]
                commit_phase_blocks: Mutex::new(Vec::new()),
                #[cfg(test)]
                compaction_phase_blocks: Mutex::new(Vec::new()),
            }),
        })
    }

    fn validate_storage_config(storage: &StorageConfig) -> Result<(), OpenError> {
        match storage {
            StorageConfig::Tiered(TieredStorageConfig {
                ssd,
                s3,
                max_local_bytes,
                ..
            }) => {
                if ssd.path.is_empty() {
                    return Err(OpenError::InvalidConfig(
                        "tiered storage requires an SSD path".to_string(),
                    ));
                }
                if s3.bucket.is_empty() {
                    return Err(OpenError::InvalidConfig(
                        "tiered storage requires an S3 bucket".to_string(),
                    ));
                }
                if *max_local_bytes == 0 {
                    return Err(OpenError::InvalidConfig(
                        "tiered storage requires max_local_bytes > 0".to_string(),
                    ));
                }
            }
            StorageConfig::S3Primary(S3PrimaryStorageConfig {
                s3,
                mem_cache_size_bytes,
                ..
            }) => {
                if s3.bucket.is_empty() {
                    return Err(OpenError::InvalidConfig(
                        "s3-primary storage requires an S3 bucket".to_string(),
                    ));
                }
                if *mem_cache_size_bytes == 0 {
                    return Err(OpenError::InvalidConfig(
                        "s3-primary storage requires mem_cache_size_bytes > 0".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_table_config(config: &TableConfig) -> Result<(), CreateTableError> {
        if config.name.is_empty() {
            return Err(CreateTableError::InvalidConfig(
                "table name cannot be empty".to_string(),
            ));
        }
        if config.history_retention_sequences == Some(0) {
            return Err(CreateTableError::InvalidConfig(
                "history_retention_sequences must be greater than zero".to_string(),
            ));
        }
        if matches!(config.max_merge_operand_chain_length, Some(0)) {
            return Err(CreateTableError::InvalidConfig(
                "max_merge_operand_chain_length must be greater than zero".to_string(),
            ));
        }
        if matches!(config.format, TableFormat::Columnar) && config.schema.is_none() {
            return Err(CreateTableError::InvalidConfig(
                "columnar tables require a schema".to_string(),
            ));
        }

        Ok(())
    }

    fn same_persisted_table_config(left: &TableConfig, right: &TableConfig) -> bool {
        left.name == right.name
            && left.format == right.format
            && left.max_merge_operand_chain_length == right.max_merge_operand_chain_length
            && left.bloom_filter_bits_per_key == right.bloom_filter_bits_per_key
            && left.compaction_strategy == right.compaction_strategy
            && left.schema == right.schema
            && left.metadata == right.metadata
    }

    fn catalog_location(storage: &StorageConfig) -> CatalogLocation {
        match storage {
            StorageConfig::Tiered(config) => {
                let path = Self::join_fs_path(&config.ssd.path, LOCAL_CATALOG_RELATIVE_PATH);
                CatalogLocation::LocalFile {
                    temp_path: format!("{path}{LOCAL_CATALOG_TEMP_SUFFIX}"),
                    path,
                }
            }
            StorageConfig::S3Primary(config) => CatalogLocation::ObjectStore {
                key: Self::join_object_key(&config.s3.prefix, OBJECT_CATALOG_RELATIVE_KEY),
            },
        }
    }

    async fn open_commit_runtime(
        storage: &StorageConfig,
        dependencies: &DbDependencies,
    ) -> Result<CommitRuntime, OpenError> {
        let backend = match storage {
            StorageConfig::Tiered(config) => {
                let dir = Self::local_commit_log_dir(&config.ssd.path);
                CommitLogBackend::Local(Box::new(
                    SegmentManager::open(
                        dependencies.file_system.clone(),
                        dir,
                        SegmentOptions::default(),
                    )
                    .await?,
                ))
            }
            StorageConfig::S3Primary(_) => CommitLogBackend::Memory(MemoryCommitLog::default()),
        };

        Ok(CommitRuntime { backend })
    }

    async fn load_tables(
        dependencies: &DbDependencies,
        catalog_location: &CatalogLocation,
    ) -> Result<(BTreeMap<String, StoredTable>, u32), OpenError> {
        let persisted = Self::load_catalog(dependencies, catalog_location).await?;
        let mut tables = BTreeMap::new();
        let mut max_table_id = 0_u32;

        for entry in persisted.tables {
            let stored = entry.into_stored();
            Self::validate_table_config(&stored.config).map_err(|error| match error {
                CreateTableError::InvalidConfig(message) => {
                    OpenError::Storage(StorageError::corruption(message))
                }
                CreateTableError::Storage(error) => OpenError::Storage(error),
                CreateTableError::AlreadyExists(_) | CreateTableError::Unimplemented(_) => {
                    OpenError::Storage(StorageError::corruption(
                        "catalog contains invalid table definition",
                    ))
                }
            })?;

            max_table_id = max_table_id.max(stored.id.get());
            if tables
                .insert(stored.config.name.clone(), stored.clone())
                .is_some()
            {
                return Err(OpenError::Storage(StorageError::corruption(
                    "catalog contains duplicate table names",
                )));
            }
        }

        let next_table_id = max_table_id.checked_add(1).unwrap_or(max_table_id).max(1);

        Ok((tables, next_table_id))
    }

    async fn load_catalog(
        dependencies: &DbDependencies,
        catalog_location: &CatalogLocation,
    ) -> Result<PersistedCatalog, OpenError> {
        let bytes = match catalog_location {
            CatalogLocation::LocalFile { path, .. } => {
                match dependencies
                    .file_system
                    .open(
                        path,
                        OpenOptions {
                            create: false,
                            read: true,
                            write: false,
                            truncate: false,
                            append: false,
                        },
                    )
                    .await
                {
                    Ok(handle) => Some(read_all_file(dependencies, &handle).await?),
                    Err(error) if error.kind() == StorageErrorKind::NotFound => None,
                    Err(error) => return Err(OpenError::Storage(error)),
                }
            }
            CatalogLocation::ObjectStore { key } => {
                match dependencies.object_store.get(key).await {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == StorageErrorKind::NotFound => None,
                    Err(error) => return Err(OpenError::Storage(error)),
                }
            }
        };

        match bytes {
            Some(bytes) => Self::decode_catalog(&bytes).map_err(OpenError::Storage),
            None => Ok(PersistedCatalog::default()),
        }
    }

    async fn persist_tables(
        &self,
        tables: &BTreeMap<String, StoredTable>,
    ) -> Result<(), CreateTableError> {
        let payload = Self::encode_catalog(tables)?;

        match &self.inner.catalog_location {
            CatalogLocation::LocalFile { path, temp_path } => {
                self.persist_catalog_file(path, temp_path, &payload).await?
            }
            CatalogLocation::ObjectStore { key } => {
                self.inner
                    .dependencies
                    .object_store
                    .put(key, &payload)
                    .await?
            }
        }

        Ok(())
    }

    async fn persist_catalog_file(
        &self,
        path: &str,
        temp_path: &str,
        payload: &[u8],
    ) -> Result<(), StorageError> {
        let temp_handle = self
            .inner
            .dependencies
            .file_system
            .open(
                temp_path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .write_at(&temp_handle, 0, payload)
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync(&temp_handle)
            .await?;
        self.inner
            .dependencies
            .file_system
            .rename(temp_path, path)
            .await?;

        let catalog_handle = self
            .inner
            .dependencies
            .file_system
            .open(
                path,
                OpenOptions {
                    create: false,
                    read: true,
                    write: true,
                    truncate: false,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync(&catalog_handle)
            .await?;

        Ok(())
    }

    fn encode_catalog(tables: &BTreeMap<String, StoredTable>) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec_pretty(&PersistedCatalog::from_tables(tables))
            .map_err(|error| StorageError::corruption(format!("encode catalog failed: {error}")))
    }

    fn decode_catalog(bytes: &[u8]) -> Result<PersistedCatalog, StorageError> {
        let catalog: PersistedCatalog = serde_json::from_slice(bytes)
            .map_err(|error| StorageError::corruption(format!("decode catalog failed: {error}")))?;

        if catalog.format_version != CATALOG_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported catalog version {}",
                catalog.format_version
            )));
        }

        Ok(catalog)
    }

    fn join_fs_path(root: &str, relative: &str) -> String {
        PathBuf::from(root)
            .join(relative)
            .to_string_lossy()
            .into_owned()
    }

    fn join_object_key(prefix: &str, relative: &str) -> String {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            relative.to_string()
        } else {
            format!("{prefix}/{relative}")
        }
    }

    fn local_storage_root_for(storage: &StorageConfig) -> Option<&str> {
        match storage {
            StorageConfig::Tiered(config) => Some(config.ssd.path.as_str()),
            StorageConfig::S3Primary(_) => None,
        }
    }

    fn local_storage_root(&self) -> Option<&str> {
        Self::local_storage_root_for(&self.inner.config.storage)
    }

    fn local_current_path(root: &str) -> String {
        Self::join_fs_path(root, LOCAL_CURRENT_RELATIVE_PATH)
    }

    fn local_manifest_dir(root: &str) -> String {
        Self::join_fs_path(root, LOCAL_MANIFEST_DIR_RELATIVE_PATH)
    }

    fn local_commit_log_dir(root: &str) -> String {
        Self::join_fs_path(root, LOCAL_COMMIT_LOG_RELATIVE_DIR)
    }

    fn manifest_filename(generation: ManifestId) -> String {
        format!("MANIFEST-{:06}", generation.get())
    }

    fn local_manifest_path(root: &str, generation: ManifestId) -> String {
        Self::join_fs_path(
            root,
            &format!(
                "{LOCAL_MANIFEST_DIR_RELATIVE_PATH}/{}",
                Self::manifest_filename(generation)
            ),
        )
    }

    fn local_sstable_path(root: &str, table_id: TableId, local_id: &str) -> String {
        Self::join_fs_path(
            root,
            &format!(
                "{LOCAL_SSTABLE_RELATIVE_DIR}/table-{:06}/{LOCAL_SSTABLE_SHARD_DIR}/{local_id}.sst",
                table_id.get()
            ),
        )
    }

    fn parse_manifest_generation(path: &str) -> Option<ManifestId> {
        let path_buf = PathBuf::from(path);
        let file_name = path_buf.file_name()?.to_str()?;
        let suffix = file_name.strip_prefix("MANIFEST-")?;
        if suffix.is_empty() || suffix.contains('.') {
            return None;
        }
        suffix.parse::<u64>().ok().map(ManifestId::new)
    }

    fn parse_sstable_local_id(local_id: &str) -> Option<u64> {
        local_id.strip_prefix("SST-")?.parse::<u64>().ok()
    }

    async fn load_local_manifest(
        storage: &StorageConfig,
        dependencies: &DbDependencies,
    ) -> Result<LoadedManifest, OpenError> {
        let Some(root) = Self::local_storage_root_for(storage) else {
            return Ok(LoadedManifest::default());
        };

        let current_path = Self::local_current_path(root);
        let manifest_dir = Self::local_manifest_dir(root);
        let mut candidates = Vec::new();
        let mut last_error = None;

        if let Some(pointer) = read_optional_path(dependencies, &current_path).await? {
            match String::from_utf8(pointer) {
                Ok(pointer) => {
                    let pointer = pointer.trim();
                    if !pointer.is_empty() {
                        candidates.push(Self::join_fs_path(
                            root,
                            &format!("{LOCAL_MANIFEST_DIR_RELATIVE_PATH}/{pointer}"),
                        ));
                    }
                }
                Err(error) => {
                    last_error = Some(StorageError::corruption(format!(
                        "decode CURRENT pointer failed: {error}"
                    )));
                }
            }
        }

        let mut listed = dependencies.file_system.list(&manifest_dir).await?;
        listed.retain(|path| Self::parse_manifest_generation(path).is_some());
        listed.sort_by_key(|path| {
            std::cmp::Reverse(
                Self::parse_manifest_generation(path)
                    .map(ManifestId::get)
                    .unwrap_or_default(),
            )
        });
        for path in listed {
            if !candidates.iter().any(|candidate| candidate == &path) {
                candidates.push(path);
            }
        }

        let mut saw_manifest = false;
        for path in candidates {
            saw_manifest = true;
            match Self::read_manifest_at_path(dependencies, &path).await {
                Ok(manifest) => return Ok(manifest),
                Err(error) => last_error = Some(error),
            }
        }

        if saw_manifest {
            Err(OpenError::Storage(last_error.unwrap_or_else(|| {
                StorageError::corruption("no valid manifest generation found")
            })))
        } else {
            Ok(LoadedManifest::default())
        }
    }

    async fn read_manifest_at_path(
        dependencies: &DbDependencies,
        path: &str,
    ) -> Result<LoadedManifest, StorageError> {
        let bytes = read_path(dependencies, path).await?;
        let file: PersistedManifestFile = serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::corruption(format!("decode manifest {path} failed: {error}"))
        })?;

        if file.body.format_version != MANIFEST_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported manifest version {}",
                file.body.format_version
            )));
        }

        let encoded_body = serde_json::to_vec(&file.body).map_err(|error| {
            StorageError::corruption(format!("encode manifest body failed: {error}"))
        })?;
        if checksum32(&encoded_body) != file.checksum {
            return Err(StorageError::corruption(format!(
                "manifest checksum mismatch for {path}"
            )));
        }

        let mut live_sstables = Vec::with_capacity(file.body.sstables.len());
        let mut max_sequence = SequenceNumber::new(0);
        for sstable in &file.body.sstables {
            max_sequence = max_sequence.max(sstable.max_sequence);
            live_sstables.push(Self::load_resident_sstable(dependencies, sstable).await?);
        }

        if max_sequence > file.body.last_flushed_sequence {
            return Err(StorageError::corruption(format!(
                "manifest {path} flush watermark is behind referenced SSTables"
            )));
        }

        Ok(LoadedManifest {
            generation: file.body.generation,
            last_flushed_sequence: file.body.last_flushed_sequence,
            live_sstables,
        })
    }

    async fn load_resident_sstable(
        dependencies: &DbDependencies,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        let bytes = read_path(dependencies, &meta.file_path).await?;
        if bytes.len() as u64 != meta.length {
            return Err(StorageError::corruption(format!(
                "sstable {} length mismatch: manifest={}, file={}",
                meta.file_path,
                meta.length,
                bytes.len()
            )));
        }

        let file: PersistedRowSstableFile = serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::corruption(format!("decode SSTable {} failed: {error}", meta.file_path))
        })?;
        if file.body.format_version != ROW_SSTABLE_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported SSTable version {}",
                file.body.format_version
            )));
        }

        let encoded_body = serde_json::to_vec(&file.body).map_err(|error| {
            StorageError::corruption(format!("encode SSTable body failed: {error}"))
        })?;
        if checksum32(&encoded_body) != file.checksum || file.checksum != meta.checksum {
            return Err(StorageError::corruption(format!(
                "SSTable checksum mismatch for {}",
                meta.file_path
            )));
        }

        let rows_bytes = serde_json::to_vec(&file.body.rows).map_err(|error| {
            StorageError::corruption(format!("encode SSTable rows failed: {error}"))
        })?;
        if checksum32(&rows_bytes) != file.body.data_checksum
            || file.body.data_checksum != meta.data_checksum
        {
            return Err(StorageError::corruption(format!(
                "SSTable data checksum mismatch for {}",
                meta.file_path
            )));
        }

        if file.body.table_id != meta.table_id
            || file.body.level != meta.level
            || file.body.local_id != meta.local_id
            || file.body.min_key != meta.min_key
            || file.body.max_key != meta.max_key
            || file.body.min_sequence != meta.min_sequence
            || file.body.max_sequence != meta.max_sequence
        {
            return Err(StorageError::corruption(format!(
                "manifest metadata does not match SSTable {}",
                meta.file_path
            )));
        }

        let computed = Self::summarize_sstable_rows(
            meta.table_id,
            meta.level,
            &meta.local_id,
            &file.body.rows,
        )?;
        if computed.min_key != meta.min_key
            || computed.max_key != meta.max_key
            || computed.min_sequence != meta.min_sequence
            || computed.max_sequence != meta.max_sequence
        {
            return Err(StorageError::corruption(format!(
                "SSTable row summary does not match metadata for {}",
                meta.file_path
            )));
        }

        Ok(ResidentRowSstable {
            meta: meta.clone(),
            rows: file.body.rows,
            user_key_bloom_filter: file.body.user_key_bloom_filter,
        })
    }

    fn summarize_sstable_rows(
        table_id: TableId,
        level: u32,
        local_id: &str,
        rows: &[SstableRow],
    ) -> Result<PersistedManifestSstable, StorageError> {
        let min_key = rows
            .iter()
            .map(|row| row.key.clone())
            .min()
            .ok_or_else(|| StorageError::unsupported("cannot build an empty SSTable"))?;
        let max_key = rows
            .iter()
            .map(|row| row.key.clone())
            .max()
            .expect("max key");
        let min_sequence = rows
            .iter()
            .map(|row| row.sequence)
            .min()
            .expect("min sequence");
        let max_sequence = rows
            .iter()
            .map(|row| row.sequence)
            .max()
            .expect("max sequence");

        Ok(PersistedManifestSstable {
            table_id,
            level,
            local_id: local_id.to_string(),
            file_path: String::new(),
            length: 0,
            checksum: 0,
            data_checksum: 0,
            min_key,
            max_key,
            min_sequence,
            max_sequence,
        })
    }

    async fn flush_immutable(
        &self,
        local_root: &str,
        immutable: &ImmutableMemtable,
    ) -> Result<Vec<ResidentRowSstable>, FlushError> {
        let mut outputs = Vec::new();
        let tables = self.tables_read().clone();

        for (&table_id, table_memtable) in &immutable.memtable.tables {
            if table_memtable.is_empty() {
                continue;
            }

            let stored = tables
                .values()
                .find(|table| table.id == table_id)
                .cloned()
                .ok_or_else(|| {
                    FlushError::Storage(StorageError::corruption(format!(
                        "flush references unknown table id {}",
                        table_id.get()
                    )))
                })?;

            if stored.config.format != TableFormat::Row {
                return Err(FlushError::Unimplemented(
                    "columnar flush path is implemented in T25",
                ));
            }

            let rows = table_memtable
                .entries
                .values()
                .map(|entry| SstableRow {
                    key: entry.user_key.clone(),
                    sequence: entry.sequence,
                    kind: entry.kind,
                    value: entry.value.clone(),
                })
                .collect::<Vec<_>>();
            let local_id = format!(
                "SST-{:06}",
                self.inner.next_sstable_id.fetch_add(1, Ordering::SeqCst)
            );
            let path = Self::local_sstable_path(local_root, table_id, &local_id);
            outputs.push(
                self.write_row_sstable(
                    &path,
                    table_id,
                    0,
                    local_id,
                    rows,
                    stored.config.bloom_filter_bits_per_key,
                )
                .await
                .map_err(FlushError::Storage)?,
            );
        }

        Ok(outputs)
    }

    async fn write_row_sstable(
        &self,
        path: &str,
        table_id: TableId,
        level: u32,
        local_id: String,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let mut meta = Self::summarize_sstable_rows(table_id, level, &local_id, &rows)?;
        let user_key_bloom_filter = UserKeyBloomFilter::build(&rows, bloom_filter_bits_per_key);
        let rows_bytes = serde_json::to_vec(&rows).map_err(|error| {
            StorageError::corruption(format!("encode SSTable rows failed: {error}"))
        })?;
        let data_checksum = checksum32(&rows_bytes);

        let body = PersistedRowSstableBody {
            format_version: ROW_SSTABLE_FORMAT_VERSION,
            table_id,
            level,
            local_id: local_id.clone(),
            min_key: meta.min_key.clone(),
            max_key: meta.max_key.clone(),
            min_sequence: meta.min_sequence,
            max_sequence: meta.max_sequence,
            rows: rows.clone(),
            data_checksum,
            user_key_bloom_filter: user_key_bloom_filter.clone(),
        };
        let encoded_body = serde_json::to_vec(&body).map_err(|error| {
            StorageError::corruption(format!("encode SSTable body failed: {error}"))
        })?;
        let checksum = checksum32(&encoded_body);
        let file = PersistedRowSstableFile { body, checksum };
        let bytes = serde_json::to_vec_pretty(&file).map_err(|error| {
            StorageError::corruption(format!("encode SSTable file failed: {error}"))
        })?;

        let handle = self
            .inner
            .dependencies
            .file_system
            .open(
                path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .write_at(&handle, 0, &bytes)
            .await?;
        self.inner.dependencies.file_system.sync(&handle).await?;

        meta.file_path = path.to_string();
        meta.length = bytes.len() as u64;
        meta.checksum = checksum;
        meta.data_checksum = data_checksum;

        Ok(ResidentRowSstable {
            meta,
            rows,
            user_key_bloom_filter,
        })
    }

    async fn install_manifest(
        &self,
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        live: &[ResidentRowSstable],
    ) -> Result<(), StorageError> {
        let root = self.local_storage_root().ok_or_else(|| {
            StorageError::unsupported("local manifest is only used in tiered mode")
        })?;
        let manifest_dir = Self::local_manifest_dir(root);
        let manifest_path = Self::local_manifest_path(root, generation);
        let manifest_temp_path = format!("{manifest_path}{LOCAL_MANIFEST_TEMP_SUFFIX}");
        let body = PersistedManifestBody {
            format_version: MANIFEST_FORMAT_VERSION,
            generation,
            last_flushed_sequence,
            sstables: live.iter().map(|sstable| sstable.meta.clone()).collect(),
        };
        let encoded_body = serde_json::to_vec(&body).map_err(|error| {
            StorageError::corruption(format!("encode manifest body failed: {error}"))
        })?;
        let checksum = checksum32(&encoded_body);
        let payload = serde_json::to_vec_pretty(&PersistedManifestFile { body, checksum })
            .map_err(|error| {
                StorageError::corruption(format!("encode manifest file failed: {error}"))
            })?;

        let manifest_handle = self
            .inner
            .dependencies
            .file_system
            .open(
                &manifest_temp_path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .write_at(&manifest_handle, 0, &payload)
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync(&manifest_handle)
            .await?;
        self.inner
            .dependencies
            .file_system
            .rename(&manifest_temp_path, &manifest_path)
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync_dir(&manifest_dir)
            .await?;

        let current_path = Self::local_current_path(root);
        let current_temp_path = format!("{current_path}{LOCAL_CATALOG_TEMP_SUFFIX}");
        let current_payload = format!("{}\n", Self::manifest_filename(generation)).into_bytes();
        let current_handle = self
            .inner
            .dependencies
            .file_system
            .open(
                &current_temp_path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .write_at(&current_handle, 0, &current_payload)
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync(&current_handle)
            .await?;
        self.inner
            .dependencies
            .file_system
            .rename(&current_temp_path, &current_path)
            .await?;
        self.inner.dependencies.file_system.sync_dir(root).await?;

        Ok(())
    }

    fn sort_live_sstables(live: &mut [ResidentRowSstable]) {
        live.sort_by(|left, right| {
            (
                left.meta.table_id.get(),
                left.meta.level,
                left.meta.min_key.as_slice(),
                left.meta.max_key.as_slice(),
                left.meta.local_id.as_str(),
            )
                .cmp(&(
                    right.meta.table_id.get(),
                    right.meta.level,
                    right.meta.min_key.as_slice(),
                    right.meta.max_key.as_slice(),
                    right.meta.local_id.as_str(),
                ))
        });
    }

    fn leveled_level_target_bytes(level: u32) -> u64 {
        if level == 0 {
            return 0;
        }

        let mut target = LEVELED_BASE_LEVEL_TARGET_BYTES;
        for _ in 1..level {
            target = target.saturating_mul(LEVELED_LEVEL_SIZE_MULTIPLIER);
        }
        target
    }

    fn table_compaction_state(
        table: &StoredTable,
        live: &[ResidentRowSstable],
    ) -> TableCompactionState {
        if table.config.format != TableFormat::Row {
            return TableCompactionState::default();
        }

        match table.config.compaction_strategy {
            CompactionStrategy::Leveled => Self::leveled_compaction_state(table, live),
            CompactionStrategy::Tiered => Self::tiered_compaction_state(table, live),
            CompactionStrategy::Fifo => Self::fifo_compaction_state(table, live),
        }
    }

    fn table_live_sstables(
        table_id: TableId,
        live: &[ResidentRowSstable],
    ) -> Vec<ResidentRowSstable> {
        live.iter()
            .filter(|sstable| sstable.meta.table_id == table_id)
            .cloned()
            .collect()
    }

    fn sstables_at_level(table_live: &[ResidentRowSstable], level: u32) -> Vec<ResidentRowSstable> {
        table_live
            .iter()
            .filter(|sstable| sstable.meta.level == level)
            .cloned()
            .collect()
    }

    fn leveled_compaction_state(
        table: &StoredTable,
        live: &[ResidentRowSstable],
    ) -> TableCompactionState {
        let table_live = Self::table_live_sstables(table.id, live);
        if table_live.is_empty() {
            return TableCompactionState::default();
        }

        let mut compaction_debt = 0_u64;
        let mut next_job = None;

        let l0 = Self::sstables_at_level(&table_live, 0);
        if l0.len() >= LEVELED_L0_COMPACTION_TRIGGER {
            compaction_debt = compaction_debt
                .saturating_add(l0.iter().map(|sstable| sstable.meta.length).sum::<u64>());
            next_job = Self::build_leveled_compaction_job(table, &table_live, 0, &l0);
        }

        let max_level = table_live
            .iter()
            .map(|sstable| sstable.meta.level)
            .max()
            .unwrap_or_default();
        for level in 1..=max_level {
            let level_files = Self::sstables_at_level(&table_live, level);
            if level_files.is_empty() {
                continue;
            }

            let level_bytes = level_files
                .iter()
                .map(|sstable| sstable.meta.length)
                .sum::<u64>();
            let target_bytes = Self::leveled_level_target_bytes(level);
            if level_bytes > target_bytes {
                compaction_debt =
                    compaction_debt.saturating_add(level_bytes.saturating_sub(target_bytes));
                if next_job.is_none() {
                    next_job =
                        Self::build_leveled_compaction_job(table, &table_live, level, &level_files);
                }
            }
        }

        TableCompactionState {
            compaction_debt,
            next_job,
        }
    }

    fn tiered_compaction_state(
        table: &StoredTable,
        live: &[ResidentRowSstable],
    ) -> TableCompactionState {
        let table_live = Self::table_live_sstables(table.id, live);
        if table_live.is_empty() {
            return TableCompactionState::default();
        }

        let mut compaction_debt = 0_u64;
        let mut next_job = None;
        let max_level = table_live
            .iter()
            .map(|sstable| sstable.meta.level)
            .max()
            .unwrap_or_default();

        for level in 0..=max_level {
            let level_files = Self::sstables_at_level(&table_live, level);
            if level_files.len() < TIERED_LEVEL_RUN_COMPACTION_TRIGGER {
                continue;
            }

            compaction_debt = compaction_debt.saturating_add(
                level_files
                    .iter()
                    .map(|sstable| sstable.meta.length)
                    .sum::<u64>(),
            );
            if next_job.is_none() {
                next_job = Self::build_rewrite_compaction_job(
                    table,
                    level,
                    level.saturating_add(1),
                    &level_files,
                );
            }
        }

        TableCompactionState {
            compaction_debt,
            next_job,
        }
    }

    fn fifo_compaction_state(
        table: &StoredTable,
        live: &[ResidentRowSstable],
    ) -> TableCompactionState {
        let mut table_live = Self::table_live_sstables(table.id, live);
        if table_live.len() <= FIFO_MAX_LIVE_SSTABLES {
            return TableCompactionState::default();
        }

        table_live.sort_by(|left, right| {
            (
                left.meta.max_sequence.get(),
                left.meta.min_sequence.get(),
                left.meta.local_id.as_str(),
            )
                .cmp(&(
                    right.meta.max_sequence.get(),
                    right.meta.min_sequence.get(),
                    right.meta.local_id.as_str(),
                ))
        });

        let delete_count = table_live.len().saturating_sub(FIFO_MAX_LIVE_SSTABLES);
        let expired = table_live
            .into_iter()
            .take(delete_count)
            .collect::<Vec<_>>();
        let compaction_debt = expired
            .iter()
            .map(|sstable| sstable.meta.length)
            .sum::<u64>();

        TableCompactionState {
            compaction_debt,
            next_job: Self::build_delete_only_compaction_job(table, &expired),
        }
    }

    fn build_leveled_compaction_job(
        table: &StoredTable,
        table_live: &[ResidentRowSstable],
        source_level: u32,
        source_inputs: &[ResidentRowSstable],
    ) -> Option<CompactionJob> {
        let target_level = source_level.saturating_add(1);
        let (min_key, max_key) = Self::sstable_key_span(source_inputs)?;
        let mut inputs = source_inputs.to_vec();

        for sstable in table_live.iter().filter(|sstable| {
            sstable.meta.level == target_level
                && Self::key_ranges_overlap(
                    sstable.meta.min_key.as_slice(),
                    sstable.meta.max_key.as_slice(),
                    min_key.as_slice(),
                    max_key.as_slice(),
                )
        }) {
            inputs.push(sstable.clone());
        }

        Self::build_rewrite_compaction_job(table, source_level, target_level, &inputs)
    }

    fn build_rewrite_compaction_job(
        table: &StoredTable,
        source_level: u32,
        target_level: u32,
        inputs: &[ResidentRowSstable],
    ) -> Option<CompactionJob> {
        Self::build_compaction_job(
            table,
            source_level,
            target_level,
            CompactionJobKind::Rewrite,
            inputs,
        )
    }

    fn build_delete_only_compaction_job(
        table: &StoredTable,
        inputs: &[ResidentRowSstable],
    ) -> Option<CompactionJob> {
        let source_level = inputs
            .iter()
            .map(|sstable| sstable.meta.level)
            .min()
            .unwrap_or_default();
        Self::build_compaction_job(
            table,
            source_level,
            source_level,
            CompactionJobKind::DeleteOnly,
            inputs,
        )
    }

    fn build_compaction_job(
        table: &StoredTable,
        source_level: u32,
        target_level: u32,
        kind: CompactionJobKind,
        inputs: &[ResidentRowSstable],
    ) -> Option<CompactionJob> {
        if inputs.is_empty() {
            return None;
        }

        let mut input_local_ids = inputs
            .iter()
            .map(|sstable| sstable.meta.local_id.clone())
            .collect::<Vec<_>>();
        input_local_ids.sort();
        input_local_ids.dedup();

        Some(CompactionJob {
            id: format!(
                "compaction:{}:L{}:{}",
                table.config.name,
                source_level,
                input_local_ids.join("+")
            ),
            table_id: table.id,
            table_name: table.config.name.clone(),
            source_level,
            target_level,
            kind,
            estimated_bytes: inputs
                .iter()
                .map(|sstable| sstable.meta.length)
                .sum::<u64>(),
            input_local_ids,
        })
    }

    fn sstable_key_span(sstables: &[ResidentRowSstable]) -> Option<(Key, Key)> {
        Some((
            sstables
                .iter()
                .map(|sstable| sstable.meta.min_key.clone())
                .min()?,
            sstables
                .iter()
                .map(|sstable| sstable.meta.max_key.clone())
                .max()?,
        ))
    }

    fn key_ranges_overlap(
        left_min: &[u8],
        left_max: &[u8],
        right_min: &[u8],
        right_max: &[u8],
    ) -> bool {
        left_min <= right_max && right_min <= left_max
    }

    fn pending_compaction_jobs(&self) -> Vec<CompactionJob> {
        let tables = self.tables_read().clone();
        let live = self.sstables_read().live.clone();
        let mut jobs = tables
            .values()
            .filter_map(|table| Self::table_compaction_state(table, &live).next_job)
            .collect::<Vec<_>>();
        jobs.sort_by(|left, right| {
            (
                left.source_level,
                left.table_name.as_str(),
                left.id.as_str(),
            )
                .cmp(&(
                    right.source_level,
                    right.table_name.as_str(),
                    right.id.as_str(),
                ))
        });
        jobs
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn estimated_sstable_row_bytes(row: &SstableRow) -> u64 {
        (row.key.len() + row.value.as_ref().map(value_size_bytes).unwrap_or_default() + 32) as u64
    }

    fn retain_rows_within_horizon(
        rows: Vec<SstableRow>,
        horizon: Option<SequenceNumber>,
    ) -> Vec<SstableRow> {
        let Some(horizon) = horizon else {
            return rows;
        };

        let mut retained = Vec::with_capacity(rows.len());
        let mut index = 0_usize;
        while index < rows.len() {
            let key = rows[index].key.clone();
            let mut anchor_kept = false;

            while index < rows.len() && rows[index].key == key {
                let row = rows[index].clone();
                if row.sequence > horizon {
                    retained.push(row);
                } else if !anchor_kept {
                    retained.push(row);
                    anchor_kept = true;
                }
                index += 1;
            }
        }

        retained
    }

    fn rewrite_compaction_rows(
        tables: &BTreeMap<String, StoredTable>,
        memtables: &MemtableState,
        sstables: &SstableState,
        table_id: TableId,
        rows: Vec<SstableRow>,
    ) -> Result<Vec<SstableRow>, StorageError> {
        let mut rewritten = Vec::with_capacity(rows.len());
        for row in rows {
            if row.kind != ChangeKind::Merge {
                rewritten.push(row);
                continue;
            }

            let resolved = Self::resolve_visible_value_with_state(
                tables,
                memtables,
                sstables,
                table_id,
                &row.key,
                row.sequence,
            )?;
            let value = resolved.value.ok_or_else(|| {
                StorageError::corruption("merge resolution unexpectedly produced a tombstone")
            })?;
            rewritten.push(SstableRow {
                key: row.key,
                sequence: row.sequence,
                kind: ChangeKind::Put,
                value: Some(value),
            });
        }

        Ok(rewritten)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn write_compaction_outputs(
        &self,
        local_root: &str,
        table_id: TableId,
        level: u32,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<Vec<ResidentRowSstable>, StorageError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let target_bytes =
            Self::leveled_level_target_bytes(level).max(LEVELED_BASE_LEVEL_TARGET_BYTES);
        let mut outputs = Vec::new();
        let mut start = 0_usize;

        while start < rows.len() {
            let mut end = start;
            let mut estimated_bytes = 0_u64;
            while end < rows.len() {
                estimated_bytes =
                    estimated_bytes.saturating_add(Self::estimated_sstable_row_bytes(&rows[end]));
                end += 1;

                let next_shares_key = end < rows.len() && rows[end - 1].key == rows[end].key;
                if estimated_bytes >= target_bytes && !next_shares_key {
                    break;
                }
            }

            let local_id = format!(
                "SST-{:06}",
                self.inner.next_sstable_id.fetch_add(1, Ordering::SeqCst)
            );
            let path = Self::local_sstable_path(local_root, table_id, &local_id);
            outputs.push(
                self.write_row_sstable(
                    &path,
                    table_id,
                    level,
                    local_id,
                    rows[start..end].to_vec(),
                    bloom_filter_bits_per_key,
                )
                .await?,
            );
            start = end;
        }

        Ok(outputs)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn run_next_compaction(&self) -> Result<bool, StorageError> {
        let Some(local_root) = self.local_storage_root().map(str::to_string) else {
            return Ok(false);
        };
        let _maintenance_guard = self.inner.maintenance_lock.lock().await;
        let Some(job) = self.pending_compaction_jobs().into_iter().next() else {
            return Ok(false);
        };

        self.execute_compaction_job(&local_root, job).await?;
        Ok(true)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn execute_compaction_job(
        &self,
        local_root: &str,
        job: CompactionJob,
    ) -> Result<(), StorageError> {
        let tables = self.tables_read().clone();
        let memtables = self.memtables_read().clone();
        let sstables = self.sstables_read().clone();
        let table = Self::stored_table_by_id(&tables, job.table_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::corruption(format!(
                    "compaction references unknown table id {}",
                    job.table_id.get()
                ))
            })?;
        let live = sstables.live.clone();
        let input_local_ids = job.input_local_ids.iter().cloned().collect::<BTreeSet<_>>();
        let inputs = live
            .iter()
            .filter(|sstable| input_local_ids.contains(&sstable.meta.local_id))
            .cloned()
            .collect::<Vec<_>>();
        if inputs.len() != job.input_local_ids.len() {
            return Err(StorageError::corruption(format!(
                "compaction job {} resolved {} of {} inputs",
                job.id,
                inputs.len(),
                job.input_local_ids.len()
            )));
        }

        let outputs = match job.kind {
            CompactionJobKind::Rewrite => {
                let mut merged_rows = inputs
                    .iter()
                    .flat_map(|sstable| sstable.rows.iter().cloned())
                    .collect::<Vec<_>>();
                merged_rows
                    .sort_by_key(|row| encode_mvcc_key(&row.key, CommitId::new(row.sequence)));
                merged_rows = Self::rewrite_compaction_rows(
                    &tables,
                    &memtables,
                    &sstables,
                    job.table_id,
                    merged_rows,
                )?;
                merged_rows = Self::retain_rows_within_horizon(
                    merged_rows,
                    self.history_gc_horizon(job.table_id),
                );

                let outputs = self
                    .write_compaction_outputs(
                        local_root,
                        job.table_id,
                        job.target_level,
                        merged_rows,
                        table.config.bloom_filter_bits_per_key,
                    )
                    .await?;
                if outputs.is_empty() {
                    return Err(StorageError::corruption(format!(
                        "compaction job {} produced no outputs",
                        job.id
                    )));
                }

                #[cfg(test)]
                self.maybe_pause_compaction_phase(CompactionPhase::OutputWritten)
                    .await;

                outputs
            }
            CompactionJobKind::DeleteOnly => Vec::new(),
        };

        let current_state = self.sstables_read().clone();
        let mut new_live = current_state
            .live
            .into_iter()
            .filter(|sstable| !input_local_ids.contains(&sstable.meta.local_id))
            .collect::<Vec<_>>();
        new_live.extend(outputs);
        Self::sort_live_sstables(&mut new_live);

        let next_generation =
            ManifestId::new(current_state.manifest_generation.get().saturating_add(1));
        self.install_manifest(
            next_generation,
            current_state.last_flushed_sequence,
            &new_live,
        )
        .await?;

        {
            let mut sstables = self.sstables_write();
            sstables.manifest_generation = next_generation;
            sstables.live = new_live;
        }

        #[cfg(test)]
        self.maybe_pause_compaction_phase(CompactionPhase::ManifestSwitched)
            .await;

        for input in &inputs {
            self.inner
                .dependencies
                .file_system
                .delete(&input.meta.file_path)
                .await?;
        }

        #[cfg(test)]
        self.maybe_pause_compaction_phase(CompactionPhase::InputCleanupFinished)
            .await;

        Ok(())
    }

    pub fn table(&self, name: impl Into<String>) -> Table {
        let name = name.into();
        let id = self.tables_read().get(&name).map(|table| table.id);
        Table {
            db: self.clone(),
            name: Arc::from(name),
            id,
        }
    }

    pub async fn create_table(&self, config: TableConfig) -> Result<Table, CreateTableError> {
        Self::validate_table_config(&config)?;

        let _catalog_guard = self.inner.catalog_write_lock.lock().await;
        let name = config.name.clone();
        let next_table_id = self.inner.next_table_id.load(Ordering::SeqCst);
        let id = TableId::new(next_table_id);
        let next_table_id = next_table_id.checked_add(1).ok_or_else(|| {
            CreateTableError::Storage(StorageError::unsupported("table id space exhausted"))
        })?;

        let mut updated_tables = self.tables_read().clone();
        if let Some(existing) = updated_tables.get(&name).cloned() {
            if Self::same_persisted_table_config(&existing.config, &config)
                && (config.merge_operator.is_some() || config.compaction_filter.is_some())
            {
                let mut refreshed = existing.clone();
                refreshed.config.merge_operator = config.merge_operator;
                refreshed.config.compaction_filter = config.compaction_filter;
                updated_tables.insert(name.clone(), refreshed);
                *self.tables_write() = updated_tables;

                return Ok(Table {
                    db: self.clone(),
                    name: Arc::from(name),
                    id: Some(existing.id),
                });
            }
            return Err(CreateTableError::AlreadyExists(name));
        }

        updated_tables.insert(name.clone(), StoredTable { id, config });
        self.persist_tables(&updated_tables).await?;

        self.inner
            .next_table_id
            .store(next_table_id, Ordering::SeqCst);
        *self.tables_write() = updated_tables;

        Ok(Table {
            db: self.clone(),
            name: Arc::from(name),
            id: Some(id),
        })
    }

    pub async fn snapshot(&self) -> Snapshot {
        let sequence = self.current_sequence();
        let registration_id = self.inner.next_snapshot_id.fetch_add(1, Ordering::SeqCst) + 1;
        mutex_lock(&self.inner.snapshot_tracker).register(registration_id, sequence);

        Snapshot {
            registration: Arc::new(SnapshotRegistration {
                db: self.clone(),
                id: registration_id,
                sequence,
                released: AtomicBool::new(false),
            }),
        }
    }

    pub fn current_sequence(&self) -> SequenceNumber {
        SequenceNumber::new(self.inner.current_sequence.load(Ordering::SeqCst))
    }

    pub fn current_durable_sequence(&self) -> SequenceNumber {
        SequenceNumber::new(self.inner.current_durable_sequence.load(Ordering::SeqCst))
    }

    pub fn write_batch(&self) -> WriteBatch {
        WriteBatch::default()
    }

    pub fn read_set(&self) -> ReadSet {
        ReadSet::default()
    }

    pub async fn commit(
        &self,
        batch: WriteBatch,
        opts: CommitOptions,
    ) -> Result<SequenceNumber, CommitError> {
        if batch.is_empty() {
            return Err(CommitError::EmptyBatch);
        }

        let resolved_operations = self.resolve_batch_operations(batch.operations())?;
        let resolved_read_set = self.resolve_read_set_entries(opts.read_set.as_ref())?;

        let participant = {
            let _commit_guard = self.inner.commit_lock.lock().await;
            self.check_read_conflicts(&resolved_read_set)?;

            let sequence = SequenceNumber::new(self.inner.next_sequence.load(Ordering::SeqCst) + 1);
            let record = Self::build_commit_record(sequence, &resolved_operations)?;
            self.inner
                .commit_runtime
                .lock()
                .await
                .append(record)
                .await
                .map_err(CommitError::Storage)?;
            self.inner
                .next_sequence
                .store(sequence.get(), Ordering::SeqCst);

            self.register_commit(sequence, resolved_operations.clone())
        };

        if let Some(batch) = participant.group_batch.clone() {
            if let Err(error) = self.await_group_commit(batch, participant.sequence).await {
                self.abort_commit(&participant);
                self.publish_watermarks();
                return Err(error);
            }

            self.mark_durable_confirmed(participant.sequence);
            self.maybe_pause_commit_phase(CommitPhase::AfterDurabilitySync, participant.sequence)
                .await;
        }

        self.maybe_pause_commit_phase(CommitPhase::BeforeMemtableInsert, participant.sequence)
            .await;
        self.memtables_write()
            .apply(participant.sequence, &participant.operations);
        self.mark_memtable_inserted(participant.sequence);
        self.maybe_pause_commit_phase(CommitPhase::AfterMemtableInsert, participant.sequence)
            .await;

        self.publish_watermarks();

        if self.current_sequence() >= participant.sequence {
            self.maybe_pause_commit_phase(
                CommitPhase::AfterVisibilityPublish,
                participant.sequence,
            )
            .await;
        }
        if self.current_durable_sequence() >= participant.sequence {
            self.maybe_pause_commit_phase(CommitPhase::AfterDurablePublish, participant.sequence)
                .await;
        }

        Ok(participant.sequence)
    }

    pub async fn flush(&self) -> Result<(), FlushError> {
        let _maintenance_guard = self.inner.maintenance_lock.lock().await;
        let local_root = self.local_storage_root().map(str::to_string);
        let immutables = {
            let _commit_guard = self.inner.commit_lock.lock().await;
            if local_root.is_some() {
                self.memtables_write().rotate_mutable();
            }

            self.inner
                .commit_runtime
                .lock()
                .await
                .sync()
                .await
                .map_err(FlushError::Storage)?;
            self.mark_all_commits_durable();
            self.publish_watermarks();

            if local_root.is_some() {
                self.memtables_read().immutables.clone()
            } else {
                return Ok(());
            }
        };

        let local_root = local_root.expect("local storage root should exist");
        let mut flushed_count = 0_usize;
        let mut sstable_state = self.sstables_read().clone();
        let mut new_live = sstable_state.live.clone();
        let mut manifest_generation = sstable_state.manifest_generation;

        for immutable in &immutables {
            let outputs = self.flush_immutable(&local_root, immutable).await?;
            if outputs.is_empty() {
                flushed_count += 1;
                continue;
            }

            new_live.extend(outputs);
            Self::sort_live_sstables(&mut new_live);
            manifest_generation = ManifestId::new(manifest_generation.get().saturating_add(1));
            self.install_manifest(manifest_generation, immutable.max_sequence, &new_live)
                .await
                .map_err(FlushError::Storage)?;
            flushed_count += 1;
        }

        if flushed_count > 0 {
            let mut memtables = self.memtables_write();
            memtables.immutables.drain(0..flushed_count);

            sstable_state.live = new_live;
            sstable_state.manifest_generation = manifest_generation;
            sstable_state.last_flushed_sequence = sstable_state.last_flushed_sequence.max(
                immutables
                    .iter()
                    .take(flushed_count)
                    .map(|immutable| immutable.max_sequence)
                    .max()
                    .unwrap_or_default(),
            );
            *self.sstables_write() = sstable_state;
        }

        Ok(())
    }

    pub async fn scan_since(
        &self,
        _table: &Table,
        _cursor: LogCursor,
        _opts: ScanOptions,
    ) -> Result<ChangeStream, SnapshotTooOld> {
        Ok(Box::pin(stream::empty()))
    }

    pub fn subscribe(&self, table: &Table) -> WatermarkReceiver {
        WatermarkReceiver {
            inner: self
                .watch_sender(
                    &self.inner.visible_watchers,
                    table.name(),
                    self.current_sequence(),
                )
                .subscribe(),
        }
    }

    pub async fn scan_durable_since(
        &self,
        _table: &Table,
        _cursor: LogCursor,
        _opts: ScanOptions,
    ) -> Result<ChangeStream, SnapshotTooOld> {
        Ok(Box::pin(stream::empty()))
    }

    pub fn subscribe_durable(&self, table: &Table) -> WatermarkReceiver {
        WatermarkReceiver {
            inner: self
                .watch_sender(
                    &self.inner.durable_watchers,
                    table.name(),
                    self.current_durable_sequence(),
                )
                .subscribe(),
        }
    }

    pub async fn table_stats(&self, table: &Table) -> TableStats {
        let tables = self.tables_read().clone();
        let live = self.sstables_read().live.clone();
        let oldest_active_snapshot_sequence = self.oldest_active_snapshot_sequence();
        let active_snapshot_count = self.active_snapshot_count();
        let metadata = tables
            .get(table.name())
            .map(|table| table.config.metadata.clone())
            .unwrap_or_default();
        let (
            l0_sstable_count,
            total_bytes,
            local_bytes,
            compaction_debt,
            pending_flush_bytes,
            immutable_memtable_count,
            history_retention_floor_sequence,
            history_gc_horizon_sequence,
            history_pinned_by_snapshots,
        ) = table
            .resolve_id()
            .map(|table_id| {
                let memtables = self.memtables_read();
                let (l0_sstable_count, total_bytes, local_bytes) = SstableState {
                    manifest_generation: ManifestId::default(),
                    last_flushed_sequence: SequenceNumber::default(),
                    live: live.clone(),
                }
                .table_stats(table_id);
                let compaction_debt = tables
                    .values()
                    .find(|stored| stored.id == table_id)
                    .map(|stored| Self::table_compaction_state(stored, &live).compaction_debt)
                    .unwrap_or_default();
                let history_retention_floor_sequence =
                    self.history_retention_floor_sequence(table_id);
                let history_gc_horizon_sequence = self.history_gc_horizon(table_id);
                let history_pinned_by_snapshots = history_retention_floor_sequence
                    .zip(oldest_active_snapshot_sequence)
                    .is_some_and(|(retention_floor, oldest_snapshot)| {
                        oldest_snapshot < retention_floor
                    });

                (
                    l0_sstable_count,
                    total_bytes,
                    local_bytes,
                    compaction_debt,
                    memtables.pending_flush_bytes(table_id),
                    memtables.immutable_memtable_count(table_id),
                    history_retention_floor_sequence,
                    history_gc_horizon_sequence,
                    history_pinned_by_snapshots,
                )
            })
            .unwrap_or((0, 0, 0, 0, 0, 0, None, None, false));

        TableStats {
            l0_sstable_count,
            total_bytes,
            local_bytes,
            compaction_debt,
            pending_flush_bytes,
            immutable_memtable_count,
            history_retention_floor_sequence,
            history_gc_horizon_sequence,
            oldest_active_snapshot_sequence,
            active_snapshot_count,
            history_pinned_by_snapshots,
            metadata,
            ..TableStats::default()
        }
    }

    pub async fn pending_work(&self) -> Vec<PendingWork> {
        self.pending_compaction_jobs()
            .into_iter()
            .map(|job| PendingWork {
                id: job.id,
                work_type: crate::scheduler::PendingWorkType::Compaction,
                table: job.table_name,
                level: Some(job.source_level),
                estimated_bytes: job.estimated_bytes,
            })
            .collect()
    }

    fn resolve_read_set_entries(
        &self,
        read_set: Option<&ReadSet>,
    ) -> Result<Vec<ResolvedReadSetEntry>, CommitError> {
        read_set
            .into_iter()
            .flat_map(ReadSet::entries)
            .map(|entry| {
                let table_id = self.resolve_table_id(&entry.table).ok_or_else(|| {
                    CommitError::Storage(StorageError::not_found(format!(
                        "table does not exist: {}",
                        entry.table.name()
                    )))
                })?;
                Ok(ResolvedReadSetEntry {
                    table_id,
                    key: entry.key.clone(),
                    at_sequence: entry.at_sequence,
                })
            })
            .collect()
    }

    fn check_read_conflicts(&self, read_set: &[ResolvedReadSetEntry]) -> Result<(), CommitError> {
        let coordinator = mutex_lock(&self.inner.commit_coordinator);
        for entry in read_set {
            let conflict_key = CommitConflictKey {
                table_id: entry.table_id,
                key: entry.key.clone(),
            };
            let changed = coordinator
                .keys
                .get(&conflict_key)
                .and_then(|sequences| sequences.iter().next_back().copied())
                .is_some_and(|sequence| sequence > entry.at_sequence);
            if changed {
                return Err(CommitError::Conflict);
            }
        }

        Ok(())
    }

    fn build_commit_record(
        sequence: SequenceNumber,
        operations: &[ResolvedBatchOperation],
    ) -> Result<CommitRecord, CommitError> {
        let entries = operations
            .iter()
            .enumerate()
            .map(|(index, operation)| {
                let op_index = u16::try_from(index).map_err(|_| {
                    CommitError::Storage(StorageError::unsupported(
                        "write batch contains more than 65535 operations",
                    ))
                })?;
                Ok(CommitEntry {
                    op_index,
                    table_id: operation.table_id,
                    kind: operation.kind,
                    key: operation.key.clone(),
                    value: operation.value.clone(),
                })
            })
            .collect::<Result<Vec<_>, CommitError>>()?;

        Ok(CommitRecord {
            id: CommitId::new(sequence),
            entries,
        })
    }

    fn register_commit(
        &self,
        sequence: SequenceNumber,
        operations: Vec<ResolvedBatchOperation>,
    ) -> CommitParticipant {
        let touched_tables = operations
            .iter()
            .map(|operation| operation.table_name.clone())
            .collect::<BTreeSet<_>>();
        let conflict_keys = operations
            .iter()
            .map(|operation| CommitConflictKey {
                table_id: operation.table_id,
                key: operation.key.clone(),
            })
            .collect::<Vec<_>>();

        let group_batch = {
            let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
            for key in &conflict_keys {
                coordinator
                    .keys
                    .entry(key.clone())
                    .or_default()
                    .insert(sequence);
            }
            coordinator.sequences.insert(
                sequence,
                SequenceCommitState {
                    touched_tables: touched_tables.clone(),
                    ..SequenceCommitState::default()
                },
            );

            if self.durable_on_commit() {
                let batch = match coordinator.current_batch.as_ref() {
                    Some(batch) if batch.is_open() => batch.clone(),
                    _ => {
                        let batch = Arc::new(GroupCommitBatch::new());
                        coordinator.current_batch = Some(batch.clone());
                        batch
                    }
                };
                Some(batch)
            } else {
                None
            }
        };

        CommitParticipant {
            sequence,
            operations,
            conflict_keys,
            group_batch,
        }
    }

    fn mark_memtable_inserted(&self, sequence: SequenceNumber) {
        if let Some(state) = mutex_lock(&self.inner.commit_coordinator)
            .sequences
            .get_mut(&sequence)
        {
            state.memtable_inserted = true;
        }
    }

    fn mark_durable_confirmed(&self, sequence: SequenceNumber) {
        if let Some(state) = mutex_lock(&self.inner.commit_coordinator)
            .sequences
            .get_mut(&sequence)
        {
            state.durable_confirmed = true;
        }
    }

    fn mark_all_commits_durable(&self) {
        let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
        for state in coordinator.sequences.values_mut() {
            if !state.aborted {
                state.durable_confirmed = true;
            }
        }
    }

    fn abort_commit(&self, participant: &CommitParticipant) {
        let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
        for key in &participant.conflict_keys {
            let remove_key = if let Some(sequences) = coordinator.keys.get_mut(key) {
                sequences.remove(&participant.sequence);
                sequences.is_empty()
            } else {
                false
            };
            if remove_key {
                coordinator.keys.remove(key);
            }
        }

        if let Some(state) = coordinator.sequences.get_mut(&participant.sequence) {
            state.aborted = true;
        }
    }

    async fn await_group_commit(
        &self,
        batch: Arc<GroupCommitBatch>,
        sequence: SequenceNumber,
    ) -> Result<(), CommitError> {
        loop {
            let notified = batch.notify.notified();
            if let Some(result) = batch.result() {
                return result.map_err(CommitError::Storage);
            }

            self.maybe_pause_commit_phase(CommitPhase::BeforeDurabilitySync, sequence)
                .await;

            let should_lead = {
                let mut state = mutex_lock(&batch.state);
                if state.result.is_some() {
                    false
                } else if !state.sealed {
                    state.sealed = true;
                    true
                } else {
                    false
                }
            };

            if should_lead {
                let result = self.inner.commit_runtime.lock().await.sync().await;
                let notify_result = result.clone();
                {
                    let mut state = mutex_lock(&batch.state);
                    state.result = Some(notify_result);
                }
                batch.notify.notify_waiters();
                return result.map_err(CommitError::Storage);
            }

            notified.await;
        }
    }

    fn publish_watermarks(&self) {
        let advance = {
            let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
            let mut advance = WatermarkAdvance::default();
            let mut visible = self.current_sequence();
            let mut durable = self.current_durable_sequence();

            loop {
                let next = SequenceNumber::new(visible.get() + 1);
                let Some(state) = coordinator.sequences.get_mut(&next) else {
                    break;
                };

                if state.aborted {
                    state.visible_published = true;
                    visible = next;
                    continue;
                }
                if !state.memtable_inserted {
                    break;
                }

                state.visible_published = true;
                visible = next;
                advance.visible_sequence = Some(visible);
                Self::record_table_notifications(
                    &mut advance.visible_tables,
                    &state.touched_tables,
                    visible,
                );
            }

            loop {
                let next = SequenceNumber::new(durable.get() + 1);
                if next > visible {
                    break;
                }

                let Some(state) = coordinator.sequences.get_mut(&next) else {
                    break;
                };

                if state.aborted {
                    state.durable_published = true;
                    durable = next;
                    continue;
                }
                if !state.durable_confirmed {
                    break;
                }

                state.durable_published = true;
                durable = next;
                advance.durable_sequence = Some(durable);
                Self::record_table_notifications(
                    &mut advance.durable_tables,
                    &state.touched_tables,
                    durable,
                );
            }

            coordinator
                .sequences
                .retain(|_, state| !state.visible_published || !state.durable_published);

            advance
        };

        if let Some(sequence) = advance.visible_sequence {
            self.inner
                .current_sequence
                .store(sequence.get(), Ordering::SeqCst);
        }
        if let Some(sequence) = advance.durable_sequence {
            self.inner
                .current_durable_sequence
                .store(sequence.get(), Ordering::SeqCst);
        }

        self.notify_table_sequences(&self.inner.visible_watchers, &advance.visible_tables);
        self.notify_table_sequences(&self.inner.durable_watchers, &advance.durable_tables);
    }

    fn record_table_notifications(
        notifications: &mut BTreeMap<String, SequenceNumber>,
        tables: &BTreeSet<String>,
        sequence: SequenceNumber,
    ) {
        for table in tables {
            notifications.insert(table.clone(), sequence);
        }
    }

    fn notify_table_sequences(
        &self,
        watchers: &Mutex<BTreeMap<String, watch::Sender<SequenceNumber>>>,
        updates: &BTreeMap<String, SequenceNumber>,
    ) {
        let watchers = mutex_lock(watchers);
        for (table, sequence) in updates {
            if let Some(sender) = watchers.get(table) {
                sender.send_replace(*sequence);
            }
        }
    }

    fn durable_on_commit(&self) -> bool {
        matches!(
            &self.inner.config.storage,
            StorageConfig::Tiered(TieredStorageConfig {
                durability: TieredDurabilityMode::GroupCommit,
                ..
            })
        )
    }

    #[cfg(test)]
    fn block_next_commit_phase(&self, phase: CommitPhase) -> CommitPhaseBlocker {
        let (sequence_tx, sequence_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        mutex_lock(&self.inner.commit_phase_blocks).push(CommitPhaseBlock {
            phase,
            sequence_tx: Some(sequence_tx),
            release_rx: Some(release_rx),
        });

        CommitPhaseBlocker {
            sequence_rx: Some(sequence_rx),
            release_tx: Some(release_tx),
        }
    }

    #[cfg(test)]
    fn block_next_compaction_phase(&self, phase: CompactionPhase) -> CompactionPhaseBlocker {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        mutex_lock(&self.inner.compaction_phase_blocks).push(CompactionPhaseBlock {
            phase,
            reached_tx: Some(reached_tx),
            release_rx: Some(release_rx),
        });

        CompactionPhaseBlocker {
            reached_rx: Some(reached_rx),
            release_tx: Some(release_tx),
        }
    }

    #[cfg(test)]
    async fn maybe_pause_commit_phase(&self, phase: CommitPhase, sequence: SequenceNumber) {
        let block = {
            let mut blocks = mutex_lock(&self.inner.commit_phase_blocks);
            blocks
                .iter()
                .position(|block| block.phase == phase)
                .map(|index| blocks.remove(index))
        };

        if let Some(mut block) = block {
            if let Some(sequence_tx) = block.sequence_tx.take() {
                let _ = sequence_tx.send(sequence);
            }
            if let Some(release_rx) = block.release_rx.take() {
                let _ = release_rx.await;
            }
        }
    }

    #[cfg(not(test))]
    async fn maybe_pause_commit_phase(&self, _phase: CommitPhase, _sequence: SequenceNumber) {}

    #[cfg(test)]
    async fn maybe_pause_compaction_phase(&self, phase: CompactionPhase) {
        let block = {
            let mut blocks = mutex_lock(&self.inner.compaction_phase_blocks);
            blocks
                .iter()
                .position(|block| block.phase == phase)
                .map(|index| blocks.remove(index))
        };

        if let Some(mut block) = block {
            if let Some(reached_tx) = block.reached_tx.take() {
                let _ = reached_tx.send(());
            }
            if let Some(release_rx) = block.release_rx.take() {
                let _ = release_rx.await;
            }
        }
    }

    #[cfg(not(test))]
    #[allow(dead_code)]
    async fn maybe_pause_compaction_phase(&self, _phase: CompactionPhase) {}

    #[allow(dead_code)]
    pub(crate) fn dependencies(&self) -> &DbDependencies {
        &self.inner.dependencies
    }

    fn resolve_batch_operations(
        &self,
        operations: &[BatchOperation],
    ) -> Result<Vec<ResolvedBatchOperation>, CommitError> {
        operations
            .iter()
            .map(|operation| match operation {
                BatchOperation::Put { table, key, value } => {
                    let stored = self.resolve_stored_table(table).ok_or_else(|| {
                        CommitError::Storage(StorageError::not_found(format!(
                            "table does not exist: {}",
                            table.name()
                        )))
                    })?;
                    Ok(ResolvedBatchOperation {
                        table_id: stored.id,
                        table_name: table.name().to_string(),
                        key: key.clone(),
                        kind: ChangeKind::Put,
                        value: Some(value.clone()),
                    })
                }
                BatchOperation::Merge { table, key, value } => {
                    let stored = self.resolve_stored_table(table).ok_or_else(|| {
                        CommitError::Storage(StorageError::not_found(format!(
                            "table does not exist: {}",
                            table.name()
                        )))
                    })?;
                    if stored.config.merge_operator.is_none() {
                        return Err(CommitError::Storage(StorageError::unsupported(format!(
                            "merge operator is not configured for table {}",
                            table.name()
                        ))));
                    }
                    Ok(ResolvedBatchOperation {
                        table_id: stored.id,
                        table_name: table.name().to_string(),
                        key: key.clone(),
                        kind: ChangeKind::Merge,
                        value: Some(value.clone()),
                    })
                }
                BatchOperation::Delete { table, key } => {
                    let stored = self.resolve_stored_table(table).ok_or_else(|| {
                        CommitError::Storage(StorageError::not_found(format!(
                            "table does not exist: {}",
                            table.name()
                        )))
                    })?;
                    Ok(ResolvedBatchOperation {
                        table_id: stored.id,
                        table_name: table.name().to_string(),
                        key: key.clone(),
                        kind: ChangeKind::Delete,
                        value: None,
                    })
                }
            })
            .collect()
    }

    fn resolve_stored_table(&self, table: &Table) -> Option<StoredTable> {
        let table_id = self.resolve_table_id(table)?;
        Self::stored_table_by_id(&self.tables_read(), table_id).cloned()
    }

    fn resolve_table_id(&self, table: &Table) -> Option<TableId> {
        table
            .id
            .or_else(|| self.tables_read().get(table.name()).map(|stored| stored.id))
    }

    fn stored_table_by_id(
        tables: &BTreeMap<String, StoredTable>,
        table_id: TableId,
    ) -> Option<&StoredTable> {
        tables.values().find(|stored| stored.id == table_id)
    }

    fn max_merge_operand_chain_length(config: &TableConfig) -> usize {
        config
            .max_merge_operand_chain_length
            .map(|limit| limit.max(1) as usize)
            .unwrap_or(DEFAULT_MAX_MERGE_OPERAND_CHAIN_LENGTH)
    }

    fn visible_row_priority(kind: ChangeKind) -> u8 {
        match kind {
            ChangeKind::Merge => 1,
            ChangeKind::Put | ChangeKind::Delete => 0,
        }
    }

    fn collapse_merge_operands(
        operator: &dyn crate::config::MergeOperator,
        key: &[u8],
        operands: &[Value],
    ) -> Result<Vec<Value>, StorageError> {
        let mut collapsed = Vec::with_capacity(operands.len());
        for operand in operands.iter().cloned() {
            collapsed.push(operand);
            while collapsed.len() >= 2 {
                let right = collapsed.pop().expect("right operand should exist");
                let left = collapsed.pop().expect("left operand should exist");
                match operator.partial_merge(key, &left, &right)? {
                    Some(merged) => collapsed.push(merged),
                    None => {
                        collapsed.push(left);
                        collapsed.push(right);
                        break;
                    }
                }
            }
        }

        Ok(collapsed)
    }

    fn resolve_visible_value_with_state(
        tables: &BTreeMap<String, StoredTable>,
        memtables: &MemtableState,
        sstables: &SstableState,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<VisibleValueResolution, StorageError> {
        let table = Self::stored_table_by_id(tables, table_id).ok_or_else(|| {
            StorageError::not_found(format!("table id {} is not registered", table_id.get()))
        })?;
        let mut rows = Vec::new();
        memtables.collect_visible_rows(table_id, key, sequence, &mut rows);
        sstables.collect_visible_rows(table_id, key, sequence, &mut rows);
        rows.sort_by(|left, right| {
            right.sequence.cmp(&left.sequence).then_with(|| {
                Self::visible_row_priority(left.kind).cmp(&Self::visible_row_priority(right.kind))
            })
        });

        let Some(head) = rows.first() else {
            return Ok(VisibleValueResolution {
                value: None,
                collapse: None,
            });
        };

        match head.kind {
            ChangeKind::Put => Ok(VisibleValueResolution {
                value: Some(
                    head.value
                        .clone()
                        .ok_or_else(|| StorageError::corruption("put row is missing a value"))?,
                ),
                collapse: None,
            }),
            ChangeKind::Delete => Ok(VisibleValueResolution {
                value: None,
                collapse: None,
            }),
            ChangeKind::Merge => {
                let operator = table.config.merge_operator.as_ref().ok_or_else(|| {
                    StorageError::unsupported(format!(
                        "merge operator is not configured for table {}",
                        table.config.name
                    ))
                })?;
                let mut operands = Vec::new();
                let mut existing = None;
                for row in &rows {
                    match row.kind {
                        ChangeKind::Merge => operands.push(row.value.clone().ok_or_else(|| {
                            StorageError::corruption("merge row is missing an operand value")
                        })?),
                        ChangeKind::Put => {
                            existing = Some(row.value.as_ref().ok_or_else(|| {
                                StorageError::corruption("put row is missing a value")
                            })?);
                            break;
                        }
                        ChangeKind::Delete => break,
                    }
                }
                operands.reverse();
                let collapsed = Self::collapse_merge_operands(operator.as_ref(), key, &operands)?;
                let value = operator.full_merge(key, existing, &collapsed)?;
                let collapse = (operands.len()
                    > Self::max_merge_operand_chain_length(&table.config))
                .then(|| MergeCollapse {
                    sequence: head.sequence,
                    value: value.clone(),
                });

                Ok(VisibleValueResolution {
                    value: Some(value),
                    collapse,
                })
            }
        }
    }

    fn force_collapse_merge_chain(&self, table_id: TableId, key: &[u8], collapse: MergeCollapse) {
        self.memtables_write().force_collapse(
            table_id,
            key.to_vec(),
            collapse.sequence,
            collapse.value,
        );
    }

    fn read_visible_value(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<Value>, StorageError> {
        let resolution = {
            let tables = self.tables_read();
            let memtables = self.memtables_read();
            let sstables = self.sstables_read();
            Self::resolve_visible_value_with_state(
                &tables, &memtables, &sstables, table_id, key, sequence,
            )?
        };

        if let Some(collapse) = resolution.collapse.clone() {
            self.force_collapse_merge_chain(table_id, key, collapse);
        }

        Ok(resolution.value)
    }

    fn scan_visible(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
        matcher: KeyMatcher<'_>,
        opts: &ScanOptions,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let limit = opts.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut keys = BTreeSet::new();
        self.memtables_read()
            .collect_matching_keys(table_id, &matcher, &mut keys);
        self.sstables_read()
            .collect_matching_keys(table_id, &matcher, &mut keys);

        let mut ordered_keys = keys.into_iter().collect::<Vec<_>>();
        if opts.reverse {
            ordered_keys.reverse();
        }

        let mut rows = Vec::new();
        for key in ordered_keys {
            let Some(value) = self.read_visible_value(table_id, &key, sequence)? else {
                continue;
            };

            rows.push((key, value));
            if rows.len() >= limit {
                break;
            }
        }

        Ok(rows)
    }

    fn release_snapshot_registration(&self, id: u64) {
        mutex_lock(&self.inner.snapshot_tracker).release(id);
    }

    fn oldest_active_snapshot_sequence(&self) -> Option<SequenceNumber> {
        mutex_lock(&self.inner.snapshot_tracker).oldest_active()
    }

    fn active_snapshot_count(&self) -> u64 {
        mutex_lock(&self.inner.snapshot_tracker).count()
    }

    fn history_retention_sequences(&self, table_id: TableId) -> Option<u64> {
        self.tables_read()
            .values()
            .find(|table| table.id == table_id)
            .and_then(|table| table.config.history_retention_sequences)
    }

    fn history_retention_floor_sequence(&self, table_id: TableId) -> Option<SequenceNumber> {
        let retained = self.history_retention_sequences(table_id)?;
        Some(SequenceNumber::new(
            self.current_sequence()
                .get()
                .saturating_sub(retained.saturating_sub(1)),
        ))
    }

    fn history_gc_horizon(&self, table_id: TableId) -> Option<SequenceNumber> {
        let retention_floor = self.history_retention_floor_sequence(table_id)?;
        Some(
            self.oldest_active_snapshot_sequence()
                .map(|snapshot| snapshot.min(retention_floor))
                .unwrap_or(retention_floor),
        )
    }

    fn validate_historical_read(
        &self,
        table_id: TableId,
        requested: SequenceNumber,
    ) -> Result<(), ReadError> {
        let Some(oldest_available) = self.history_gc_horizon(table_id) else {
            return Ok(());
        };
        if requested < oldest_available {
            return Err(SnapshotTooOld {
                requested,
                oldest_available,
            }
            .into());
        }

        Ok(())
    }

    #[cfg(test)]
    fn snapshot_gc_horizon(&self) -> SequenceNumber {
        self.oldest_active_snapshot_sequence()
            .unwrap_or_else(|| self.current_sequence())
    }

    fn watch_sender(
        &self,
        watchers: &Mutex<BTreeMap<String, watch::Sender<SequenceNumber>>>,
        table_name: &str,
        current: SequenceNumber,
    ) -> watch::Sender<SequenceNumber> {
        let mut watchers = mutex_lock(watchers);
        watchers
            .entry(table_name.to_string())
            .or_insert_with(|| {
                let (sender, _receiver) = watch::channel(current);
                sender
            })
            .clone()
    }

    fn tables_read(&self) -> RwLockReadGuard<'_, BTreeMap<String, StoredTable>> {
        self.inner.tables.read()
    }

    fn tables_write(&self) -> RwLockWriteGuard<'_, BTreeMap<String, StoredTable>> {
        self.inner.tables.write()
    }

    fn memtables_read(&self) -> RwLockReadGuard<'_, MemtableState> {
        self.inner.memtables.read()
    }

    fn memtables_write(&self) -> RwLockWriteGuard<'_, MemtableState> {
        self.inner.memtables.write()
    }

    fn sstables_read(&self) -> RwLockReadGuard<'_, SstableState> {
        self.inner.sstables.read()
    }

    fn sstables_write(&self) -> RwLockWriteGuard<'_, SstableState> {
        self.inner.sstables.write()
    }
}

#[derive(Clone)]
pub struct Table {
    db: Db,
    name: Arc<str>,
    id: Option<TableId>,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> Option<TableId> {
        self.id
    }

    pub async fn read(&self, key: Key) -> Result<Option<Value>, ReadError> {
        self.read_at(key, self.db.current_sequence()).await
    }

    pub async fn write(&self, key: Key, value: Value) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.put(self, key, value);
        self.db
            .commit(batch, CommitOptions::default())
            .await
            .map_err(Into::into)
    }

    pub async fn delete(&self, key: Key) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.delete(self, key);
        self.db
            .commit(batch, CommitOptions::default())
            .await
            .map_err(Into::into)
    }

    pub async fn merge(&self, key: Key, delta: Value) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.merge(self, key, delta);
        self.db
            .commit(batch, CommitOptions::default())
            .await
            .map_err(Into::into)
    }

    pub async fn scan(
        &self,
        start: Key,
        end: Key,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        self.scan_at(start, end, self.db.current_sequence(), opts)
            .await
    }

    pub async fn scan_prefix(
        &self,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        self.scan_prefix_at(prefix, self.db.current_sequence(), opts)
            .await
    }

    pub async fn read_at(
        &self,
        key: Key,
        sequence: SequenceNumber,
    ) -> Result<Option<Value>, ReadError> {
        let Some(table_id) = self.resolve_id() else {
            return Ok(None);
        };
        self.db.validate_historical_read(table_id, sequence)?;

        Ok(self.db.read_visible_value(table_id, &key, sequence)?)
    }

    pub async fn scan_at(
        &self,
        start: Key,
        end: Key,
        sequence: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        let Some(table_id) = self.resolve_id() else {
            return Ok(Box::pin(stream::empty()));
        };
        self.db.validate_historical_read(table_id, sequence)?;

        let rows = self.db.scan_visible(
            table_id,
            sequence,
            KeyMatcher::Range {
                start: &start,
                end: &end,
            },
            &opts,
        )?;
        Ok(Box::pin(stream::iter(rows)))
    }

    async fn scan_prefix_at(
        &self,
        prefix: KeyPrefix,
        sequence: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        let Some(table_id) = self.resolve_id() else {
            return Ok(Box::pin(stream::empty()));
        };
        self.db.validate_historical_read(table_id, sequence)?;

        let rows = self
            .db
            .scan_visible(table_id, sequence, KeyMatcher::Prefix(&prefix), &opts)?;
        Ok(Box::pin(stream::iter(rows)))
    }

    fn resolve_id(&self) -> Option<TableId> {
        self.db.resolve_table_id(self)
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("id", &self.id)
            .finish()
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.id == other.id
    }
}

impl Eq for Table {}

impl Hash for Table {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.id.hash(state);
    }
}

#[derive(Clone)]
pub struct Snapshot {
    registration: Arc<SnapshotRegistration>,
}

struct SnapshotRegistration {
    db: Db,
    id: u64,
    sequence: SequenceNumber,
    released: AtomicBool,
}

impl Snapshot {
    pub fn sequence(&self) -> SequenceNumber {
        self.registration.sequence
    }

    pub async fn read(&self, table: &Table, key: Key) -> Result<Option<Value>, ReadError> {
        table.read_at(key, self.sequence()).await
    }

    pub async fn scan(
        &self,
        table: &Table,
        start: Key,
        end: Key,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        table.scan_at(start, end, self.sequence(), opts).await
    }

    pub async fn scan_prefix(
        &self,
        table: &Table,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        table.scan_prefix_at(prefix, self.sequence(), opts).await
    }

    pub fn release(&self) {
        if self.registration.released.swap(true, Ordering::SeqCst) {
            return;
        }

        self.registration
            .db
            .release_snapshot_registration(self.registration.id);
    }

    pub fn is_released(&self) -> bool {
        self.registration.released.load(Ordering::SeqCst)
    }

    pub fn db(&self) -> &Db {
        &self.registration.db
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("sequence", &self.sequence())
            .field("released", &self.is_released())
            .finish()
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        if Arc::strong_count(&self.registration) == 1 {
            self.release();
        }
    }
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}

async fn read_all_file(
    dependencies: &DbDependencies,
    handle: &crate::FileHandle,
) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    let mut offset = 0;

    loop {
        let chunk = dependencies
            .file_system
            .read_at(handle, offset, CATALOG_READ_CHUNK_LEN)
            .await?;
        if chunk.is_empty() {
            break;
        }

        offset += chunk.len() as u64;
        bytes.extend_from_slice(&chunk);
        if chunk.len() < CATALOG_READ_CHUNK_LEN {
            break;
        }
    }

    Ok(bytes)
}

async fn read_path(dependencies: &DbDependencies, path: &str) -> Result<Vec<u8>, StorageError> {
    let handle = dependencies
        .file_system
        .open(
            path,
            OpenOptions {
                create: false,
                read: true,
                write: false,
                truncate: false,
                append: false,
            },
        )
        .await?;
    read_all_file(dependencies, &handle).await
}

async fn read_optional_path(
    dependencies: &DbDependencies,
    path: &str,
) -> Result<Option<Vec<u8>>, StorageError> {
    match dependencies
        .file_system
        .open(
            path,
            OpenOptions {
                create: false,
                read: true,
                write: false,
                truncate: false,
                append: false,
            },
        )
        .await
    {
        Ok(handle) => Ok(Some(read_all_file(dependencies, &handle).await?)),
        Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn bloom_hash_pair(key: &[u8]) -> (u64, u64) {
    fn hash_with_seed(key: &[u8], seed: u64) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^ (hash >> 32)
    }

    let first = hash_with_seed(key, 0x9e37_79b9_7f4a_7c15);
    let second = hash_with_seed(key, 0xc2b2_ae3d_27d4_eb4f) | 1;
    (first, second)
}
#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::{collections::BTreeMap, sync::Arc};

    use futures::StreamExt;
    use serde_json::json;

    use super::{
        CommitPhase, CompactionJobKind, CompactionPhase, Db, LOCAL_CATALOG_RELATIVE_PATH,
        LOCAL_CATALOG_TEMP_SUFFIX, LOCAL_COMMIT_LOG_RELATIVE_DIR, LOCAL_MANIFEST_TEMP_SUFFIX,
        LOCAL_SSTABLE_RELATIVE_DIR, LOCAL_SSTABLE_SHARD_DIR, ManifestId, PersistedRowSstableFile,
        SchemaDefinition, StoredTable, decode_mvcc_key, encode_mvcc_key, read_path,
    };
    use crate::{
        ChangeKind, CommitError, CommitId, CommitOptions, CompactionStrategy, DbConfig,
        DbDependencies, FieldDefinition, FieldId, FieldType, FieldValue, FileSystem,
        FileSystemFailure, FileSystemOperation, MergeOperator, MergeOperatorRef, PointMutation,
        ReadError, Rng, S3Location, S3PrimaryStorageConfig, ScanOptions, SequenceNumber,
        ShadowOracle, SsdConfig, StorageConfig, StorageError, StubClock, StubObjectStore, StubRng,
        TableConfig, TableFormat, TableId, TieredDurabilityMode, TieredStorageConfig, Value,
    };

    fn tiered_config_with_durability(path: &str, durability: TieredDurabilityMode) -> DbConfig {
        DbConfig {
            storage: StorageConfig::Tiered(TieredStorageConfig {
                ssd: SsdConfig {
                    path: path.to_string(),
                },
                s3: S3Location {
                    bucket: "terracedb-test".to_string(),
                    prefix: "tiered".to_string(),
                },
                max_local_bytes: 1024 * 1024,
                durability,
            }),
            scheduler: None,
        }
    }

    fn tiered_config(path: &str) -> DbConfig {
        tiered_config_with_durability(path, TieredDurabilityMode::GroupCommit)
    }

    fn s3_primary_config(prefix: &str) -> DbConfig {
        DbConfig {
            storage: StorageConfig::S3Primary(S3PrimaryStorageConfig {
                s3: S3Location {
                    bucket: "terracedb-test".to_string(),
                    prefix: prefix.to_string(),
                },
                mem_cache_size_bytes: 1024 * 1024,
                auto_flush_interval: None,
            }),
            scheduler: None,
        }
    }

    fn dependencies(
        file_system: Arc<crate::StubFileSystem>,
        object_store: Arc<StubObjectStore>,
    ) -> DbDependencies {
        DbDependencies::new(
            file_system,
            object_store,
            Arc::new(StubClock::default()),
            Arc::new(StubRng::seeded(7)),
        )
    }

    #[derive(Debug)]
    struct AppendMergeOperator;

    impl MergeOperator for AppendMergeOperator {
        fn full_merge(
            &self,
            _key: &[u8],
            existing: Option<&Value>,
            operands: &[Value],
        ) -> Result<Value, StorageError> {
            let mut merged = match existing {
                Some(Value::Bytes(bytes)) => bytes.clone(),
                Some(_) => {
                    return Err(StorageError::unsupported(
                        "append merge operator only supports byte values",
                    ));
                }
                None => Vec::new(),
            };

            for operand in operands {
                let Value::Bytes(bytes) = operand else {
                    return Err(StorageError::unsupported(
                        "append merge operator only supports byte operands",
                    ));
                };
                if !merged.is_empty() && !bytes.is_empty() {
                    merged.push(b'|');
                }
                merged.extend_from_slice(bytes);
            }

            Ok(Value::Bytes(merged))
        }

        fn partial_merge(
            &self,
            _key: &[u8],
            left: &Value,
            right: &Value,
        ) -> Result<Option<Value>, StorageError> {
            let Value::Bytes(left_bytes) = left else {
                return Err(StorageError::unsupported(
                    "append merge operator only supports byte operands",
                ));
            };
            let Value::Bytes(right_bytes) = right else {
                return Err(StorageError::unsupported(
                    "append merge operator only supports byte operands",
                ));
            };

            let mut merged = left_bytes.clone();
            if !merged.is_empty() && !right_bytes.is_empty() {
                merged.push(b'|');
            }
            merged.extend_from_slice(right_bytes);
            Ok(Some(Value::Bytes(merged)))
        }
    }

    fn append_merge_operator() -> MergeOperatorRef {
        Arc::new(AppendMergeOperator)
    }

    fn merge_row_table_config(
        name: &str,
        max_merge_operand_chain_length: Option<u32>,
    ) -> TableConfig {
        let mut config = row_table_config(name);
        config.merge_operator = Some(append_merge_operator());
        config.max_merge_operand_chain_length = max_merge_operand_chain_length;
        config
    }

    fn bytes(value: &str) -> Value {
        Value::bytes(value.as_bytes().to_vec())
    }

    fn row_table_config(name: &str) -> TableConfig {
        row_table_config_with_strategy(name, CompactionStrategy::Leveled)
    }

    fn row_table_config_with_strategy(
        name: &str,
        compaction_strategy: CompactionStrategy,
    ) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            format: TableFormat::Row,
            merge_operator: None,
            max_merge_operand_chain_length: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(10),
            history_retention_sequences: None,
            compaction_strategy,
            schema: None,
            metadata: BTreeMap::from([
                ("priority".to_string(), json!("high")),
                ("tenant".to_string(), json!("alpha")),
            ]),
        }
    }

    fn row_table_config_with_history_retention(name: &str, retained_sequences: u64) -> TableConfig {
        let mut config = row_table_config(name);
        config.history_retention_sequences = Some(retained_sequences);
        config
    }

    fn columnar_table_config(name: &str) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            format: TableFormat::Columnar,
            merge_operator: None,
            max_merge_operand_chain_length: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(6),
            history_retention_sequences: Some(30),
            compaction_strategy: CompactionStrategy::Tiered,
            schema: Some(SchemaDefinition {
                version: 3,
                fields: vec![
                    FieldDefinition {
                        id: FieldId::new(1),
                        name: "user_id".to_string(),
                        field_type: FieldType::String,
                        nullable: false,
                        default: None,
                    },
                    FieldDefinition {
                        id: FieldId::new(2),
                        name: "count".to_string(),
                        field_type: FieldType::Int64,
                        nullable: false,
                        default: Some(FieldValue::Int64(0)),
                    },
                ],
            }),
            metadata: BTreeMap::from([
                ("retention".to_string(), json!("30d")),
                ("schedule_group".to_string(), json!("metrics")),
            ]),
        }
    }

    fn assert_catalog_entry(table: &StoredTable, expected: &TableConfig) {
        assert_eq!(table.config.name, expected.name);
        assert_eq!(table.config.format, expected.format);
        assert_eq!(
            table.config.max_merge_operand_chain_length,
            expected.max_merge_operand_chain_length
        );
        assert_eq!(
            table.config.bloom_filter_bits_per_key,
            expected.bloom_filter_bits_per_key
        );
        assert_eq!(
            table.config.history_retention_sequences,
            expected.history_retention_sequences
        );
        assert_eq!(
            table.config.compaction_strategy,
            expected.compaction_strategy
        );
        assert_eq!(table.config.schema, expected.schema);
        assert_eq!(table.config.metadata, expected.metadata);
        assert!(table.config.merge_operator.is_none());
        assert!(table.config.compaction_filter.is_none());
    }

    async fn collect_rows(stream: crate::KvStream) -> Vec<(crate::Key, Value)> {
        stream.collect::<Vec<_>>().await
    }

    fn assert_snapshot_too_old(
        error: ReadError,
        requested: SequenceNumber,
        oldest_available: SequenceNumber,
    ) {
        let snapshot_too_old = error
            .snapshot_too_old()
            .expect("expected SnapshotTooOld read error");
        assert_eq!(snapshot_too_old.requested, requested);
        assert_eq!(snapshot_too_old.oldest_available, oldest_available);
    }

    fn oracle_rows(
        oracle: &ShadowOracle,
        sequence: SequenceNumber,
        start: &[u8],
        end: &[u8],
        reverse: bool,
        limit: Option<usize>,
    ) -> Vec<(crate::Key, Value)> {
        let mut rows = oracle
            .point_state_at(sequence)
            .into_iter()
            .filter(|(key, _value)| key.as_slice() >= start && key.as_slice() < end)
            .map(|(key, value)| (key, Value::bytes(value)))
            .collect::<Vec<_>>();

        if reverse {
            rows.reverse();
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }

        rows
    }

    fn oracle_prefix_rows(
        oracle: &ShadowOracle,
        sequence: SequenceNumber,
        prefix: &[u8],
        reverse: bool,
        limit: Option<usize>,
    ) -> Vec<(crate::Key, Value)> {
        let mut rows = oracle
            .point_state_at(sequence)
            .into_iter()
            .filter(|(key, _value)| key.starts_with(prefix))
            .map(|(key, value)| (key, Value::bytes(value)))
            .collect::<Vec<_>>();

        if reverse {
            rows.reverse();
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }

        rows
    }

    fn sstable_dir(root: &str, table_id: TableId) -> String {
        Db::join_fs_path(
            root,
            &format!(
                "{LOCAL_SSTABLE_RELATIVE_DIR}/table-{:06}/{LOCAL_SSTABLE_SHARD_DIR}",
                table_id.get()
            ),
        )
    }

    fn commit_log_segment_path(root: &str, segment_id: u64) -> String {
        Db::join_fs_path(
            root,
            &format!("{LOCAL_COMMIT_LOG_RELATIVE_DIR}/SEG-{segment_id:06}"),
        )
    }

    async fn overwrite_file(file_system: &crate::StubFileSystem, path: &str, bytes: &[u8]) {
        let handle = file_system
            .open(
                path,
                crate::OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await
            .expect("open file for overwrite");
        file_system
            .write_at(&handle, 0, bytes)
            .await
            .expect("overwrite file bytes");
        file_system
            .sync(&handle)
            .await
            .expect("sync overwritten file");
    }

    async fn seed_compaction_fixture(
        root: &str,
        table_name: &str,
        compaction_strategy: CompactionStrategy,
    ) -> (
        Db,
        crate::Table,
        Arc<crate::StubFileSystem>,
        DbDependencies,
        ShadowOracle,
    ) {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);
        let db = Db::open(tiered_config(root), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config_with_strategy(
                table_name,
                compaction_strategy,
            ))
            .await
            .expect("create table");
        let mut oracle = ShadowOracle::default();

        let first = table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write apple v1");
        oracle.apply(
            first,
            PointMutation::Put {
                key: b"apple".to_vec(),
                value: b"v1".to_vec(),
            },
            true,
        );
        let second = table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write banana");
        oracle.apply(
            second,
            PointMutation::Put {
                key: b"banana".to_vec(),
                value: b"yellow".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush first l0");

        let third = table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write apple v2");
        oracle.apply(
            third,
            PointMutation::Put {
                key: b"apple".to_vec(),
                value: b"v2".to_vec(),
            },
            true,
        );
        let fourth = table
            .write(b"carrot".to_vec(), Value::bytes("orange"))
            .await
            .expect("write carrot");
        oracle.apply(
            fourth,
            PointMutation::Put {
                key: b"carrot".to_vec(),
                value: b"orange".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush second l0");

        let fifth = table
            .delete(b"banana".to_vec())
            .await
            .expect("delete banana");
        oracle.apply(
            fifth,
            PointMutation::Delete {
                key: b"banana".to_vec(),
            },
            true,
        );
        let sixth = table
            .write(b"apricot".to_vec(), Value::bytes("soft"))
            .await
            .expect("write apricot");
        oracle.apply(
            sixth,
            PointMutation::Put {
                key: b"apricot".to_vec(),
                value: b"soft".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush third l0");

        (db, table, file_system, dependencies, oracle)
    }

    async fn seed_leveled_compaction_fixture(
        root: &str,
    ) -> (
        Db,
        crate::Table,
        Arc<crate::StubFileSystem>,
        DbDependencies,
        ShadowOracle,
    ) {
        seed_compaction_fixture(root, "events", CompactionStrategy::Leveled).await
    }

    #[test]
    fn mvcc_key_encoding_orders_newer_versions_first() {
        let mut encoded = [
            encode_mvcc_key(b"user:1", CommitId::new(SequenceNumber::new(3))),
            encode_mvcc_key(b"user:1", CommitId::new(SequenceNumber::new(9))),
            encode_mvcc_key(b"user:1", CommitId::new(SequenceNumber::new(5))),
        ];
        encoded.sort();

        let decoded_sequences = encoded
            .iter()
            .map(|key| {
                let (user_key, commit_id) = decode_mvcc_key(key).expect("decode mvcc key");
                assert_eq!(user_key, b"user:1".to_vec());
                commit_id.sequence()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            decoded_sequences,
            vec![
                SequenceNumber::new(9),
                SequenceNumber::new(5),
                SequenceNumber::new(3),
            ]
        );
    }

    #[tokio::test]
    async fn table_lookup_is_synchronous_for_existing_tables() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/sync-lookup"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");

        let created = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");
        let lookup = db.table("events");

        assert_eq!(lookup.name(), "events");
        assert_eq!(lookup.id(), created.id());
    }

    #[tokio::test]
    async fn tiered_catalog_survives_reopen_with_stable_ids_and_metadata() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);

        let db = Db::open(tiered_config("/tiered-reopen"), dependencies.clone())
            .await
            .expect("open db");
        let row_config = row_table_config("events");
        let columnar_config = columnar_table_config("metrics");
        let row_table = db
            .create_table(row_config.clone())
            .await
            .expect("create row table");
        let columnar_table = db
            .create_table(columnar_config.clone())
            .await
            .expect("create columnar table");

        let reopened = Db::open(tiered_config("/tiered-reopen"), dependencies)
            .await
            .expect("reopen db");
        assert_eq!(reopened.table("events").id(), row_table.id());
        assert_eq!(reopened.table("metrics").id(), columnar_table.id());

        let tables = reopened.tables_read();
        let row_entry = tables.get("events").expect("row table in catalog");
        assert_catalog_entry(row_entry, &row_config);

        let columnar_entry = tables.get("metrics").expect("columnar table in catalog");
        assert_catalog_entry(columnar_entry, &columnar_config);
    }

    #[tokio::test]
    async fn s3_primary_catalog_survives_reopen() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);

        let db = Db::open(s3_primary_config("catalog-test"), dependencies.clone())
            .await
            .expect("open db");
        let config = row_table_config("events");
        let created = db.create_table(config.clone()).await.expect("create table");

        let reopened = Db::open(s3_primary_config("catalog-test"), dependencies)
            .await
            .expect("reopen db");
        assert_eq!(reopened.table("events").id(), created.id());

        let tables = reopened.tables_read();
        let entry = tables.get("events").expect("table in catalog");
        assert_catalog_entry(entry, &config);
    }

    #[tokio::test]
    async fn create_table_is_all_or_nothing_across_recovery() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/crash-safe-catalog"), dependencies.clone())
            .await
            .expect("open db");
        let catalog_path = Db::join_fs_path("/crash-safe-catalog", LOCAL_CATALOG_RELATIVE_PATH);
        let temp_catalog_path = format!("{catalog_path}{LOCAL_CATALOG_TEMP_SUFFIX}");
        file_system.inject_failure(FileSystemFailure::timeout(
            FileSystemOperation::Rename,
            temp_catalog_path,
        ));

        db.create_table(row_table_config("events"))
            .await
            .expect_err("catalog rename failure");

        file_system.crash();
        let reopened = Db::open(tiered_config("/crash-safe-catalog"), dependencies.clone())
            .await
            .expect("reopen after failed create");
        assert_eq!(reopened.table("events").id(), None);

        let created = reopened
            .create_table(row_table_config("events"))
            .await
            .expect("create table after failed attempt");
        file_system.crash();

        let recovered = Db::open(tiered_config("/crash-safe-catalog"), dependencies)
            .await
            .expect("reopen after successful create");
        assert_eq!(recovered.table("events").id(), created.id());
        let entry = recovered
            .tables_read()
            .get("events")
            .cloned()
            .expect("recovered table");
        assert_catalog_entry(&entry, &row_table_config("events"));
    }

    #[tokio::test]
    async fn snapshot_reads_remain_stable_while_newer_writes_arrive() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/snapshot-stability"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"user:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("write initial value");
        let snapshot = db.snapshot().await;

        table
            .write(b"user:1".to_vec(), Value::bytes("v2"))
            .await
            .expect("write newer value");

        assert_eq!(
            snapshot
                .read(&table, b"user:1".to_vec())
                .await
                .expect("snapshot read"),
            Some(Value::bytes("v1"))
        );
        assert_eq!(
            table.read(b"user:1".to_vec()).await.expect("latest read"),
            Some(Value::bytes("v2"))
        );

        let rows = collect_rows(
            snapshot
                .scan(
                    &table,
                    b"user:".to_vec(),
                    b"user;".to_vec(),
                    ScanOptions::default(),
                )
                .await
                .expect("snapshot scan"),
        )
        .await;
        assert_eq!(rows, vec![(b"user:1".to_vec(), Value::bytes("v1"))]);
    }

    #[tokio::test]
    async fn reads_and_scans_span_mutable_and_immutable_memtables() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/memtable-handoff"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let apple_v1 = table
            .write(b"apple".to_vec(), Value::bytes("old"))
            .await
            .expect("write apple v1");
        let apricot_v1 = table
            .write(b"apricot".to_vec(), Value::bytes("stone"))
            .await
            .expect("write apricot v1");

        let rotated = db.memtables_write().rotate_mutable();
        assert_eq!(rotated, Some(apricot_v1));

        table
            .write(b"apple".to_vec(), Value::bytes("new"))
            .await
            .expect("write apple v2");
        table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write banana");
        table
            .delete(b"apricot".to_vec())
            .await
            .expect("delete apricot");

        assert_eq!(
            table
                .read_at(b"apple".to_vec(), apple_v1)
                .await
                .expect("historical apple read"),
            Some(Value::bytes("old"))
        );
        assert_eq!(
            table.read(b"apple".to_vec()).await.expect("latest apple"),
            Some(Value::bytes("new"))
        );
        assert_eq!(
            table
                .read_at(b"apricot".to_vec(), apricot_v1)
                .await
                .expect("historical apricot read"),
            Some(Value::bytes("stone"))
        );
        assert_eq!(
            table
                .read(b"apricot".to_vec())
                .await
                .expect("latest apricot"),
            None
        );

        let prefix_rows = collect_rows(
            table
                .scan_prefix(b"ap".to_vec(), ScanOptions::default())
                .await
                .expect("prefix scan"),
        )
        .await;
        assert_eq!(prefix_rows, vec![(b"apple".to_vec(), Value::bytes("new"))]);

        let reverse_rows = collect_rows(
            table
                .scan(
                    b"a".to_vec(),
                    b"c".to_vec(),
                    ScanOptions {
                        reverse: true,
                        limit: None,
                        columns: None,
                    },
                )
                .await
                .expect("reverse scan"),
        )
        .await;
        assert_eq!(
            reverse_rows,
            vec![
                (b"banana".to_vec(), Value::bytes("yellow")),
                (b"apple".to_vec(), Value::bytes("new")),
            ]
        );

        let stats = db.table_stats(&table).await;
        assert!(stats.pending_flush_bytes > 0);
        assert_eq!(stats.immutable_memtable_count, 1);
    }

    #[tokio::test]
    async fn historical_reads_and_scans_span_sstables_and_newer_memtable_versions() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/historical-read-path"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let apple_v1 = table
            .write(b"apple".to_vec(), Value::bytes("old"))
            .await
            .expect("write apple v1");
        let apricot_v1 = table
            .write(b"apricot".to_vec(), Value::bytes("stone"))
            .await
            .expect("write apricot v1");
        let historical = table
            .write(b"banana".to_vec(), Value::bytes("ripe"))
            .await
            .expect("write banana v1");

        db.flush().await.expect("flush historical state");

        table
            .write(b"apple".to_vec(), Value::bytes("new"))
            .await
            .expect("write apple v2");
        table
            .delete(b"apricot".to_vec())
            .await
            .expect("delete apricot");
        table
            .write(b"cherry".to_vec(), Value::bytes("red"))
            .await
            .expect("write cherry");

        assert_eq!(
            table
                .read_at(b"apple".to_vec(), apple_v1)
                .await
                .expect("historical apple"),
            Some(Value::bytes("old"))
        );
        assert_eq!(
            table.read(b"apple".to_vec()).await.expect("latest apple"),
            Some(Value::bytes("new"))
        );
        assert_eq!(
            table
                .read_at(b"apricot".to_vec(), apricot_v1)
                .await
                .expect("historical apricot"),
            Some(Value::bytes("stone"))
        );
        assert_eq!(
            table
                .read(b"apricot".to_vec())
                .await
                .expect("latest apricot"),
            None
        );

        let historical_rows = collect_rows(
            table
                .scan_at(
                    b"a".to_vec(),
                    b"d".to_vec(),
                    historical,
                    ScanOptions::default(),
                )
                .await
                .expect("historical scan"),
        )
        .await;
        assert_eq!(
            historical_rows,
            vec![
                (b"apple".to_vec(), Value::bytes("old")),
                (b"apricot".to_vec(), Value::bytes("stone")),
                (b"banana".to_vec(), Value::bytes("ripe")),
            ]
        );

        let historical_prefix_rows = collect_rows(
            table
                .scan_prefix_at(b"ap".to_vec(), historical, ScanOptions::default())
                .await
                .expect("historical prefix scan"),
        )
        .await;
        assert_eq!(
            historical_prefix_rows,
            vec![
                (b"apple".to_vec(), Value::bytes("old")),
                (b"apricot".to_vec(), Value::bytes("stone")),
            ]
        );

        let latest_reverse_rows = collect_rows(
            table
                .scan_at(
                    b"a".to_vec(),
                    b"d".to_vec(),
                    db.current_sequence(),
                    ScanOptions {
                        reverse: true,
                        limit: Some(2),
                        columns: None,
                    },
                )
                .await
                .expect("latest reverse scan"),
        )
        .await;
        assert_eq!(
            latest_reverse_rows,
            vec![
                (b"cherry".to_vec(), Value::bytes("red")),
                (b"banana".to_vec(), Value::bytes("ripe")),
            ]
        );
    }

    #[tokio::test]
    async fn row_sstable_flush_persists_user_key_bloom_filters() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/sstable-bloom"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write apple v1");
        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write apple v2");
        table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write banana");

        db.flush().await.expect("flush");

        let live = db
            .sstables_read()
            .live
            .first()
            .cloned()
            .expect("live sstable");
        let filter = live
            .user_key_bloom_filter
            .as_ref()
            .expect("resident bloom filter");
        assert!(filter.may_contain(b"apple"));
        assert!(filter.may_contain(b"banana"));
        assert!(!filter.may_contain(b"carrot"));

        let file_bytes = read_path(&dependencies, &live.meta.file_path)
            .await
            .expect("read persisted sstable");
        let persisted: PersistedRowSstableFile =
            serde_json::from_slice(&file_bytes).expect("decode persisted sstable");
        let persisted_filter = persisted
            .body
            .user_key_bloom_filter
            .expect("persisted bloom filter");
        assert_eq!(persisted_filter.bits_per_key, 10);
        assert!(persisted_filter.may_contain(b"apple"));
        assert!(persisted_filter.may_contain(b"banana"));
        assert!(!persisted_filter.may_contain(b"carrot"));
    }

    #[tokio::test]
    async fn flush_persists_row_sstables_manifest_and_reopenable_reads() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/flush-persist"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("old"))
            .await
            .expect("write apple v1");
        table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write banana");
        let flushed = table
            .write(b"apple".to_vec(), Value::bytes("new"))
            .await
            .expect("write apple v2");

        db.flush().await.expect("flush to SSTable");

        {
            let sstables = db.sstables_read();
            assert_eq!(sstables.manifest_generation, ManifestId::new(1));
            assert_eq!(sstables.last_flushed_sequence, flushed);
            assert_eq!(sstables.live.len(), 1);
            let live = sstables.live.first().expect("live SSTable");
            assert_eq!(live.meta.table_id, table.id().expect("table id"));
            assert_eq!(live.meta.level, 0);
            assert_eq!(live.meta.min_key, b"apple".to_vec());
            assert_eq!(live.meta.max_key, b"banana".to_vec());
            assert_eq!(live.meta.min_sequence, SequenceNumber::new(1));
            assert_eq!(live.meta.max_sequence, flushed);
            assert_eq!(live.rows.len(), 3);
            assert!(live.meta.length > 0);
        }

        let current_path = Db::local_current_path("/flush-persist");
        let current_bytes = read_path(&dependencies, &current_path)
            .await
            .expect("read CURRENT");
        assert_eq!(
            String::from_utf8(current_bytes)
                .expect("decode CURRENT")
                .trim(),
            "MANIFEST-000001"
        );

        let loaded_manifest = Db::read_manifest_at_path(
            &dependencies,
            &Db::local_manifest_path("/flush-persist", ManifestId::new(1)),
        )
        .await
        .expect("load manifest");
        assert_eq!(loaded_manifest.last_flushed_sequence, flushed);
        assert_eq!(loaded_manifest.live_sstables.len(), 1);
        assert_eq!(
            loaded_manifest.live_sstables[0].meta.min_key,
            b"apple".to_vec()
        );
        assert_eq!(
            loaded_manifest.live_sstables[0].meta.max_key,
            b"banana".to_vec()
        );

        assert_eq!(
            table
                .read(b"apple".to_vec())
                .await
                .expect("read flushed apple"),
            Some(Value::bytes("new"))
        );

        let stats = db.table_stats(&table).await;
        assert_eq!(stats.pending_flush_bytes, 0);
        assert_eq!(stats.immutable_memtable_count, 0);
        assert_eq!(stats.l0_sstable_count, 1);
        assert!(stats.local_bytes > 0);

        let reopened = Db::open(tiered_config("/flush-persist"), dependencies)
            .await
            .expect("reopen after flush");
        let reopened_table = reopened.table("events");
        assert_eq!(reopened.current_sequence(), flushed);
        assert_eq!(reopened.current_durable_sequence(), flushed);
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("reopened apple"),
            Some(Value::bytes("new"))
        );
        let rows = collect_rows(
            reopened_table
                .scan(b"a".to_vec(), b"c".to_vec(), ScanOptions::default())
                .await
                .expect("reopened scan"),
        )
        .await;
        assert_eq!(
            rows,
            vec![
                (b"apple".to_vec(), Value::bytes("new")),
                (b"banana".to_vec(), Value::bytes("yellow")),
            ]
        );
    }

    #[tokio::test]
    async fn randomized_read_path_matches_shadow_oracle_across_flushes() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/randomized-read-oracle"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");
        let rng = StubRng::seeded(0x0ddc_0ffe_e123_4567);
        let keys = [
            b"apple".to_vec(),
            b"apricot".to_vec(),
            b"banana".to_vec(),
            b"blueberry".to_vec(),
            b"cherry".to_vec(),
        ];
        let mut oracle = ShadowOracle::default();

        for step in 0..48_u64 {
            let key = keys[(rng.next_u64() as usize) % keys.len()].clone();
            let committed = if rng.next_u64().is_multiple_of(4) {
                let sequence = table
                    .delete(key.clone())
                    .await
                    .expect("delete key in randomized oracle test");
                oracle.apply(sequence, PointMutation::Delete { key }, true);
                sequence
            } else {
                let value = format!("value-{step}").into_bytes();
                let sequence = table
                    .write(key.clone(), Value::bytes(value.clone()))
                    .await
                    .expect("write key in randomized oracle test");
                oracle.apply(sequence, PointMutation::Put { key, value }, true);
                sequence
            };

            if rng.next_u64().is_multiple_of(3) {
                db.flush().await.expect("flush randomized state");
            }

            let historical = SequenceNumber::new(rng.next_u64() % (committed.get() + 1));
            for key in &keys {
                assert_eq!(
                    table
                        .read_at(key.clone(), historical)
                        .await
                        .expect("historical point read"),
                    oracle.value_at(key, historical).map(Value::bytes)
                );
            }

            let latest_rows = collect_rows(
                table
                    .scan_at(
                        b"a".to_vec(),
                        b"z".to_vec(),
                        historical,
                        ScanOptions::default(),
                    )
                    .await
                    .expect("historical scan"),
            )
            .await;
            assert_eq!(
                latest_rows,
                oracle_rows(&oracle, historical, b"a", b"z", false, None)
            );

            let reverse_limit = ((rng.next_u64() % 3) + 1) as usize;
            let reverse_rows = collect_rows(
                table
                    .scan_at(
                        b"a".to_vec(),
                        b"z".to_vec(),
                        historical,
                        ScanOptions {
                            reverse: true,
                            limit: Some(reverse_limit),
                            columns: None,
                        },
                    )
                    .await
                    .expect("reverse scan"),
            )
            .await;
            assert_eq!(
                reverse_rows,
                oracle_rows(&oracle, historical, b"a", b"z", true, Some(reverse_limit))
            );

            let prefix_rows = collect_rows(
                table
                    .scan_prefix_at(b"ap".to_vec(), historical, ScanOptions::default())
                    .await
                    .expect("historical prefix scan"),
            )
            .await;
            assert_eq!(
                prefix_rows,
                oracle_prefix_rows(&oracle, historical, b"ap", false, None)
            );
        }
    }

    #[tokio::test]
    async fn restart_replays_commit_log_tail_newer_than_manifest() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/tail-replay"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let committed = table
            .write(b"apple".to_vec(), Value::bytes("tail"))
            .await
            .expect("write tail value");
        assert_eq!(db.current_sequence(), committed);
        assert_eq!(db.current_durable_sequence(), committed);

        file_system.crash();
        let reopened = Db::open(tiered_config("/tail-replay"), dependencies)
            .await
            .expect("reopen after crash");
        let reopened_table = reopened.table("events");

        assert_eq!(reopened.current_sequence(), committed);
        assert_eq!(reopened.current_durable_sequence(), committed);
        assert_eq!(
            reopened.sstables_read().last_flushed_sequence,
            SequenceNumber::new(0)
        );
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read replayed tail"),
            Some(Value::bytes("tail"))
        );
        let stats = reopened.table_stats(&reopened_table).await;
        assert!(stats.pending_flush_bytes > 0);
        assert_eq!(stats.immutable_memtable_count, 0);
    }

    #[tokio::test]
    async fn deferred_recovery_drops_non_durable_tail() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(
            tiered_config_with_durability("/deferred-tail-loss", TieredDurabilityMode::Deferred),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("volatile"))
            .await
            .expect("write deferred value");
        assert_eq!(db.current_sequence(), SequenceNumber::new(1));
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));

        file_system.crash();
        let reopened = Db::open(
            tiered_config_with_durability("/deferred-tail-loss", TieredDurabilityMode::Deferred),
            dependencies,
        )
        .await
        .expect("reopen deferred db");
        let reopened_table = reopened.table("events");

        assert_eq!(reopened.current_sequence(), SequenceNumber::new(0));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(0));
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read after deferred crash"),
            None
        );
    }

    #[tokio::test]
    async fn manifest_sync_failure_preserves_prior_generation_on_reopen() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(
            tiered_config("/manifest-sync-failure"),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write v1");
        db.flush().await.expect("first flush");

        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write v2");
        file_system.inject_failure(FileSystemFailure::timeout(
            FileSystemOperation::SyncDir,
            Db::local_manifest_dir("/manifest-sync-failure"),
        ));
        db.flush().await.expect_err("manifest sync failure");

        file_system.crash();
        let reopened = Db::open(tiered_config("/manifest-sync-failure"), dependencies)
            .await
            .expect("reopen after failed manifest sync");
        let reopened_table = reopened.table("events");
        assert_eq!(reopened.current_sequence(), SequenceNumber::new(2));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(2));
        assert_eq!(
            reopened.sstables_read().manifest_generation,
            ManifestId::new(1)
        );
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read recovered value"),
            Some(Value::bytes("v2"))
        );
        let stats = reopened.table_stats(&reopened_table).await;
        assert!(stats.pending_flush_bytes > 0);
    }

    #[tokio::test]
    async fn recovery_ignores_orphan_sstables_after_current_pointer_failure() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/orphan-sstable"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write v1");
        db.flush().await.expect("first flush");

        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write v2");
        let current_path = Db::local_current_path("/orphan-sstable");
        let current_temp_path = format!("{current_path}{LOCAL_CATALOG_TEMP_SUFFIX}");
        file_system.inject_failure(FileSystemFailure::timeout(
            FileSystemOperation::Rename,
            current_temp_path,
        ));
        db.flush().await.expect_err("CURRENT rename failure");

        let sstable_files = file_system
            .list(&sstable_dir(
                "/orphan-sstable",
                table.id().expect("table id"),
            ))
            .await
            .expect("list sstable dir");
        assert_eq!(sstable_files.len(), 2);

        file_system.crash();
        let reopened = Db::open(tiered_config("/orphan-sstable"), dependencies)
            .await
            .expect("reopen after orphan SSTable");
        let reopened_table = reopened.table("events");
        assert_eq!(
            reopened.sstables_read().manifest_generation,
            ManifestId::new(1)
        );
        assert_eq!(reopened.sstables_read().live.len(), 1);
        assert_eq!(reopened.current_sequence(), SequenceNumber::new(2));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(2));
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read reopened apple"),
            Some(Value::bytes("v2"))
        );
    }

    #[tokio::test]
    async fn corrupt_current_pointer_falls_back_to_latest_valid_manifest() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/corrupt-current"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write apple");
        db.flush().await.expect("first flush");
        table
            .write(b"banana".to_vec(), Value::bytes("v2"))
            .await
            .expect("write banana");
        db.flush().await.expect("second flush");

        let current_path = Db::local_current_path("/corrupt-current");
        overwrite_file(file_system.as_ref(), &current_path, &[0xff, 0xfe, 0xfd]).await;

        file_system.crash();
        let reopened = Db::open(tiered_config("/corrupt-current"), dependencies)
            .await
            .expect("reopen with corrupt CURRENT");
        let reopened_table = reopened.table("events");

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            ManifestId::new(2)
        );
        assert_eq!(reopened.current_sequence(), SequenceNumber::new(2));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(2));
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read apple"),
            Some(Value::bytes("v1"))
        );
        assert_eq!(
            reopened_table
                .read(b"banana".to_vec())
                .await
                .expect("read banana"),
            Some(Value::bytes("v2"))
        );
    }

    #[tokio::test]
    async fn corrupt_latest_manifest_falls_back_and_replays_commit_log_tail() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(
            tiered_config("/corrupt-latest-manifest"),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write v1");
        db.flush().await.expect("first flush");
        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write v2");
        db.flush().await.expect("second flush");

        let manifest_path = Db::local_manifest_path("/corrupt-latest-manifest", ManifestId::new(2));
        let mut manifest_bytes = read_path(&dependencies, &manifest_path)
            .await
            .expect("read latest manifest");
        let last = manifest_bytes.len() - 1;
        manifest_bytes[last] ^= 0x55;
        overwrite_file(file_system.as_ref(), &manifest_path, &manifest_bytes).await;

        file_system.crash();
        let reopened = Db::open(tiered_config("/corrupt-latest-manifest"), dependencies)
            .await
            .expect("reopen with corrupt latest manifest");
        let reopened_table = reopened.table("events");

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            ManifestId::new(1)
        );
        assert_eq!(reopened.current_sequence(), SequenceNumber::new(2));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(2));
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read recovered apple"),
            Some(Value::bytes("v2"))
        );
        let stats = reopened.table_stats(&reopened_table).await;
        assert!(stats.pending_flush_bytes > 0);
    }

    #[tokio::test]
    async fn repeated_open_recovery_is_idempotent() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/idempotent-recovery"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("tail"))
            .await
            .expect("write tail");

        file_system.crash();
        let reopened = Db::open(tiered_config("/idempotent-recovery"), dependencies.clone())
            .await
            .expect("first reopen");
        assert_eq!(reopened.current_sequence(), SequenceNumber::new(1));
        file_system.crash();

        let reopened_again = Db::open(tiered_config("/idempotent-recovery"), dependencies)
            .await
            .expect("second reopen");
        let reopened_table = reopened_again.table("events");
        assert_eq!(reopened_again.current_sequence(), SequenceNumber::new(1));
        assert_eq!(
            reopened_again.current_durable_sequence(),
            SequenceNumber::new(1)
        );
        assert_eq!(
            reopened_table
                .write(b"banana".to_vec(), Value::bytes("fresh"))
                .await
                .expect("write after repeated reopen"),
            SequenceNumber::new(2)
        );
    }

    #[tokio::test]
    async fn recovery_truncates_partial_active_segment_suffix() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(
            tiered_config("/partial-active-segment"),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("stable"))
            .await
            .expect("write stable value");

        let segment_path = commit_log_segment_path("/partial-active-segment", 1);
        let stable_bytes = read_path(&dependencies, &segment_path)
            .await
            .expect("read stable segment");
        let mut corrupted = stable_bytes.clone();
        corrupted.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        overwrite_file(file_system.as_ref(), &segment_path, &corrupted).await;

        file_system.crash();
        let reopened = Db::open(
            tiered_config("/partial-active-segment"),
            dependencies.clone(),
        )
        .await
        .expect("reopen with partial active segment");
        let reopened_table = reopened.table("events");

        assert_eq!(reopened.current_sequence(), SequenceNumber::new(1));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(1));
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read recovered value"),
            Some(Value::bytes("stable"))
        );

        let repaired = read_path(&dependencies, &segment_path)
            .await
            .expect("read repaired segment");
        assert_eq!(repaired, stable_bytes);
    }

    #[tokio::test]
    async fn recovery_prefers_manifest_references_over_extra_local_files() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/extra-local-files"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write apple v1");
        db.flush().await.expect("first flush");

        table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write banana");
        db.flush().await.expect("second flush");

        let active_live = db.sstables_read().live.clone();
        assert_eq!(active_live.len(), 2);
        let stale_copy_path = Db::local_sstable_path(
            "/extra-local-files",
            table.id().expect("table id"),
            "SST-999999",
        );
        let stale_bytes = read_path(&dependencies, &active_live[0].meta.file_path)
            .await
            .expect("read source sstable");
        let stale_handle = file_system
            .open(
                &stale_copy_path,
                crate::OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await
            .expect("open stale copy");
        file_system
            .write_at(&stale_handle, 0, &stale_bytes)
            .await
            .expect("write stale copy");
        file_system
            .sync(&stale_handle)
            .await
            .expect("sync stale copy");

        file_system.crash();
        let reopened = Db::open(tiered_config("/extra-local-files"), dependencies)
            .await
            .expect("reopen with extra local file");
        let reopened_table = reopened.table("events");
        assert_eq!(reopened.sstables_read().live.len(), 2);
        let on_disk = file_system
            .list(&sstable_dir(
                "/extra-local-files",
                table.id().expect("table id"),
            ))
            .await
            .expect("list on-disk SSTables");
        assert_eq!(on_disk.len(), 3);
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read apple"),
            Some(Value::bytes("v1"))
        );
        assert_eq!(
            reopened_table
                .read(b"banana".to_vec())
                .await
                .expect("read banana"),
            Some(Value::bytes("yellow"))
        );
    }

    #[tokio::test]
    async fn leveled_compaction_preserves_state_and_reports_backlog() {
        let (db, table, _file_system, _dependencies, oracle) =
            seed_leveled_compaction_fixture("/leveled-compaction").await;

        let before_stats = db.table_stats(&table).await;
        assert_eq!(before_stats.l0_sstable_count, 3);
        assert!(before_stats.compaction_debt > 0);

        let pending = db.pending_work().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].table, "events");
        assert_eq!(pending[0].level, Some(0));

        let max_sequence = db.current_sequence();
        for raw in 0..=max_sequence.get() {
            let sequence = SequenceNumber::new(raw);
            let rows = collect_rows(
                table
                    .scan_at(
                        b"a".to_vec(),
                        b"z".to_vec(),
                        sequence,
                        ScanOptions::default(),
                    )
                    .await
                    .expect("scan before compaction"),
            )
            .await;
            assert_eq!(
                rows,
                oracle_rows(&oracle, sequence, b"a", b"z", false, None)
            );
        }

        assert!(db.run_next_compaction().await.expect("run compaction"));

        {
            let sstables = db.sstables_read();
            assert_eq!(sstables.live.len(), 1);
            let output = sstables.live.first().expect("compaction output");
            assert_eq!(output.meta.level, 1);
            assert_eq!(output.meta.min_key, b"apple".to_vec());
            assert_eq!(output.meta.max_key, b"carrot".to_vec());
            assert_eq!(output.meta.min_sequence, SequenceNumber::new(1));
            assert_eq!(output.meta.max_sequence, SequenceNumber::new(6));
        }

        let after_stats = db.table_stats(&table).await;
        assert_eq!(after_stats.l0_sstable_count, 0);
        assert_eq!(after_stats.compaction_debt, 0);
        assert!(db.pending_work().await.is_empty());

        for raw in 0..=max_sequence.get() {
            let sequence = SequenceNumber::new(raw);
            let rows = collect_rows(
                table
                    .scan_at(
                        b"a".to_vec(),
                        b"z".to_vec(),
                        sequence,
                        ScanOptions::default(),
                    )
                    .await
                    .expect("scan after compaction"),
            )
            .await;
            assert_eq!(
                rows,
                oracle_rows(&oracle, sequence, b"a", b"z", false, None)
            );
        }
    }

    #[tokio::test]
    async fn tiered_compaction_preserves_state_without_pulling_in_existing_next_tier_runs() {
        let (db, table, _file_system, _dependencies, mut oracle) =
            seed_compaction_fixture("/tiered-compaction", "events", CompactionStrategy::Tiered)
                .await;

        let initial_job = db
            .pending_compaction_jobs()
            .into_iter()
            .next()
            .expect("initial tiered compaction job");
        assert_eq!(initial_job.kind, CompactionJobKind::Rewrite);
        assert_eq!(initial_job.source_level, 0);
        assert_eq!(initial_job.target_level, 1);
        assert_eq!(initial_job.input_local_ids.len(), 3);

        assert!(
            db.run_next_compaction()
                .await
                .expect("run first tiered compaction")
        );
        assert_eq!(db.table_stats(&table).await.l0_sstable_count, 0);
        assert_eq!(db.sstables_read().live.len(), 1);
        assert_eq!(db.sstables_read().live[0].meta.level, 1);

        let seventh = table
            .write(b"apple".to_vec(), Value::bytes("v3"))
            .await
            .expect("write apple v3");
        oracle.apply(
            seventh,
            PointMutation::Put {
                key: b"apple".to_vec(),
                value: b"v3".to_vec(),
            },
            true,
        );
        let eighth = table
            .write(b"blueberry".to_vec(), Value::bytes("blue"))
            .await
            .expect("write blueberry");
        oracle.apply(
            eighth,
            PointMutation::Put {
                key: b"blueberry".to_vec(),
                value: b"blue".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush fourth l0");

        let ninth = table
            .write(b"banana".to_vec(), Value::bytes("green"))
            .await
            .expect("rewrite banana");
        oracle.apply(
            ninth,
            PointMutation::Put {
                key: b"banana".to_vec(),
                value: b"green".to_vec(),
            },
            true,
        );
        let tenth = table
            .write(b"date".to_vec(), Value::bytes("brown"))
            .await
            .expect("write date");
        oracle.apply(
            tenth,
            PointMutation::Put {
                key: b"date".to_vec(),
                value: b"brown".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush fifth l0");

        let eleventh = table
            .delete(b"carrot".to_vec())
            .await
            .expect("delete carrot");
        oracle.apply(
            eleventh,
            PointMutation::Delete {
                key: b"carrot".to_vec(),
            },
            true,
        );
        let twelfth = table
            .write(b"avocado".to_vec(), Value::bytes("creamy"))
            .await
            .expect("write avocado");
        oracle.apply(
            twelfth,
            PointMutation::Put {
                key: b"avocado".to_vec(),
                value: b"creamy".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush sixth l0");

        let pending = db
            .pending_compaction_jobs()
            .into_iter()
            .next()
            .expect("second tiered compaction job");
        assert_eq!(pending.kind, CompactionJobKind::Rewrite);
        assert_eq!(pending.source_level, 0);
        assert_eq!(pending.target_level, 1);

        let selected_levels = db
            .sstables_read()
            .live
            .iter()
            .filter(|sstable| pending.input_local_ids.contains(&sstable.meta.local_id))
            .map(|sstable| sstable.meta.level)
            .collect::<Vec<_>>();
        assert_eq!(selected_levels.len(), 3);
        assert!(selected_levels.iter().all(|level| *level == 0));

        let max_sequence = db.current_sequence();
        for raw in 0..=max_sequence.get() {
            let sequence = SequenceNumber::new(raw);
            let rows = collect_rows(
                table
                    .scan_at(
                        b"a".to_vec(),
                        b"z".to_vec(),
                        sequence,
                        ScanOptions::default(),
                    )
                    .await
                    .expect("scan before second tiered compaction"),
            )
            .await;
            assert_eq!(
                rows,
                oracle_rows(&oracle, sequence, b"a", b"z", false, None)
            );
        }

        assert!(
            db.run_next_compaction()
                .await
                .expect("run second tiered compaction")
        );

        {
            let sstables = db.sstables_read();
            assert_eq!(sstables.live.len(), 2);
            assert!(sstables.live.iter().all(|sstable| sstable.meta.level == 1));
        }

        let after_stats = db.table_stats(&table).await;
        assert_eq!(after_stats.l0_sstable_count, 0);
        assert_eq!(after_stats.compaction_debt, 0);
        assert!(db.pending_work().await.is_empty());

        for raw in 0..=max_sequence.get() {
            let sequence = SequenceNumber::new(raw);
            let rows = collect_rows(
                table
                    .scan_at(
                        b"a".to_vec(),
                        b"z".to_vec(),
                        sequence,
                        ScanOptions::default(),
                    )
                    .await
                    .expect("scan after second tiered compaction"),
            )
            .await;
            assert_eq!(
                rows,
                oracle_rows(&oracle, sequence, b"a", b"z", false, None)
            );
        }
    }

    #[tokio::test]
    async fn fifo_compaction_ages_out_oldest_sstables_with_delete_only_jobs() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);
        let db = Db::open(tiered_config("/fifo-compaction"), dependencies)
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config_with_strategy(
                "events",
                CompactionStrategy::Fifo,
            ))
            .await
            .expect("create table");

        table
            .write(b"archive".to_vec(), Value::bytes("oldest"))
            .await
            .expect("write archive");
        table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write apple v1");
        db.flush().await.expect("flush first fifo sstable");

        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write apple v2");
        table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write banana");
        db.flush().await.expect("flush second fifo sstable");

        table
            .delete(b"banana".to_vec())
            .await
            .expect("delete banana");
        table
            .write(b"carrot".to_vec(), Value::bytes("orange"))
            .await
            .expect("write carrot");
        db.flush().await.expect("flush third fifo sstable");

        let before_stats = db.table_stats(&table).await;
        assert_eq!(before_stats.l0_sstable_count, 3);
        assert!(before_stats.compaction_debt > 0);

        let job = db
            .pending_compaction_jobs()
            .into_iter()
            .next()
            .expect("fifo compaction job");
        assert_eq!(job.kind, CompactionJobKind::DeleteOnly);
        assert_eq!(job.input_local_ids.len(), 1);

        let deleted_input = db
            .sstables_read()
            .live
            .iter()
            .find(|sstable| sstable.meta.local_id == job.input_local_ids[0])
            .expect("oldest fifo input")
            .clone();

        assert!(db.run_next_compaction().await.expect("run fifo compaction"));

        let after_stats = db.table_stats(&table).await;
        assert_eq!(after_stats.l0_sstable_count, 2);
        assert_eq!(after_stats.compaction_debt, 0);
        assert!(db.pending_work().await.is_empty());
        assert!(
            db.sstables_read()
                .live
                .iter()
                .all(|sstable| sstable.meta.local_id != deleted_input.meta.local_id)
        );

        let on_disk = file_system
            .list(&sstable_dir(
                "/fifo-compaction",
                table.id().expect("table id"),
            ))
            .await
            .expect("list fifo sstables");
        assert_eq!(on_disk.len(), 2);
        assert!(!on_disk.contains(&deleted_input.meta.file_path));

        assert_eq!(
            table
                .read(b"archive".to_vec())
                .await
                .expect("read aged-out key"),
            None
        );
        assert_eq!(
            table
                .read(b"apple".to_vec())
                .await
                .expect("read surviving key"),
            Some(Value::bytes("v2"))
        );
        assert_eq!(
            table
                .read(b"banana".to_vec())
                .await
                .expect("read tombstoned key"),
            None
        );
        assert_eq!(
            table
                .read(b"carrot".to_vec())
                .await
                .expect("read newest key"),
            Some(Value::bytes("orange"))
        );
    }

    #[tokio::test]
    async fn compaction_strategies_are_selected_per_table_without_cross_table_interference() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/mixed-compaction-strategies"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let fifo = db
            .create_table(row_table_config_with_strategy(
                "fifo",
                CompactionStrategy::Fifo,
            ))
            .await
            .expect("create fifo table");
        let leveled = db
            .create_table(row_table_config_with_strategy(
                "leveled",
                CompactionStrategy::Leveled,
            ))
            .await
            .expect("create leveled table");
        let tiered = db
            .create_table(row_table_config_with_strategy(
                "tiered",
                CompactionStrategy::Tiered,
            ))
            .await
            .expect("create tiered table");

        for round in 0..3_u8 {
            fifo.write(
                format!("fifo:{round}").into_bytes(),
                Value::bytes(format!("fifo-{round}")),
            )
            .await
            .expect("write fifo row");
            leveled
                .write(
                    format!("leveled:{round}").into_bytes(),
                    Value::bytes(format!("leveled-{round}")),
                )
                .await
                .expect("write leveled row");
            tiered
                .write(
                    format!("tiered:{round}").into_bytes(),
                    Value::bytes(format!("tiered-{round}")),
                )
                .await
                .expect("write tiered row");
            db.flush().await.expect("flush mixed strategy round");
        }

        let jobs = db.pending_compaction_jobs();
        assert_eq!(jobs.len(), 3);
        assert_eq!(
            jobs.iter()
                .find(|job| job.table_name == "fifo")
                .expect("fifo job")
                .kind,
            CompactionJobKind::DeleteOnly
        );
        assert_eq!(
            jobs.iter()
                .find(|job| job.table_name == "leveled")
                .expect("leveled job")
                .kind,
            CompactionJobKind::Rewrite
        );
        assert_eq!(
            jobs.iter()
                .find(|job| job.table_name == "tiered")
                .expect("tiered job")
                .kind,
            CompactionJobKind::Rewrite
        );

        let leveled_job = jobs
            .into_iter()
            .find(|job| job.table_name == "leveled")
            .expect("leveled compaction job");
        db.execute_compaction_job("/mixed-compaction-strategies", leveled_job)
            .await
            .expect("run leveled compaction only");

        assert_eq!(db.table_stats(&leveled).await.l0_sstable_count, 0);
        assert_eq!(db.table_stats(&tiered).await.l0_sstable_count, 3);
        assert_eq!(db.table_stats(&fifo).await.l0_sstable_count, 3);
        assert!(db.table_stats(&tiered).await.compaction_debt > 0);
        assert!(db.table_stats(&fifo).await.compaction_debt > 0);

        assert_eq!(
            tiered
                .read(b"tiered:0".to_vec())
                .await
                .expect("read tiered key"),
            Some(Value::bytes("tiered-0"))
        );
        assert_eq!(
            fifo.read(b"fifo:0".to_vec()).await.expect("read fifo key"),
            Some(Value::bytes("fifo-0"))
        );

        let remaining_jobs = db.pending_compaction_jobs();
        assert_eq!(remaining_jobs.len(), 2);
        assert!(remaining_jobs.iter().all(|job| job.table_name != "leveled"));
        assert!(remaining_jobs.iter().any(|job| job.table_name == "tiered"));
        assert!(remaining_jobs.iter().any(|job| job.table_name == "fifo"));
    }

    #[tokio::test]
    async fn compaction_output_without_manifest_switch_recovers_prior_generation() {
        let (db, table, file_system, dependencies, _oracle) =
            seed_leveled_compaction_fixture("/compaction-manifest-before-switch").await;

        let prior_generation = db.sstables_read().manifest_generation;
        let next_generation = ManifestId::new(prior_generation.get().saturating_add(1));
        let manifest_temp_path = format!(
            "{}{}",
            Db::local_manifest_path("/compaction-manifest-before-switch", next_generation),
            LOCAL_MANIFEST_TEMP_SUFFIX
        );
        file_system.inject_failure(FileSystemFailure::for_target(
            FileSystemOperation::Rename,
            manifest_temp_path,
            StorageError::io("simulated manifest rename failure"),
        ));

        let error = db
            .run_next_compaction()
            .await
            .expect_err("compaction should fail before manifest switch");
        assert!(
            error
                .message()
                .contains("simulated manifest rename failure")
        );

        let on_disk = file_system
            .list(&sstable_dir(
                "/compaction-manifest-before-switch",
                table.id().expect("table id"),
            ))
            .await
            .expect("list on-disk sstables");
        assert!(on_disk.len() > db.sstables_read().live.len());

        file_system.crash();
        let reopened = Db::open(
            tiered_config("/compaction-manifest-before-switch"),
            dependencies,
        )
        .await
        .expect("reopen after failed compaction");
        let reopened_table = reopened.table("events");

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            prior_generation
        );
        assert_eq!(reopened.sstables_read().live.len(), 3);
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read apple"),
            Some(Value::bytes("v2"))
        );
        assert_eq!(
            reopened_table
                .read(b"banana".to_vec())
                .await
                .expect("read banana"),
            None
        );
    }

    #[tokio::test]
    async fn compaction_manifest_switch_reopen_ignores_old_inputs_when_cleanup_fails() {
        let (db, table, file_system, dependencies, _oracle) =
            seed_leveled_compaction_fixture("/compaction-cleanup-failure").await;

        let job = db
            .pending_compaction_jobs()
            .into_iter()
            .next()
            .expect("pending compaction job");
        let first_input_path = db
            .sstables_read()
            .live
            .iter()
            .find(|sstable| sstable.meta.local_id == job.input_local_ids[0])
            .expect("first compaction input")
            .meta
            .file_path
            .clone();

        file_system.inject_failure(FileSystemFailure::for_target(
            FileSystemOperation::Delete,
            first_input_path,
            StorageError::io("simulated compaction delete failure"),
        ));

        let prior_generation = db.sstables_read().manifest_generation;
        let error = db
            .run_next_compaction()
            .await
            .expect_err("compaction should fail during cleanup");
        assert!(
            error
                .message()
                .contains("simulated compaction delete failure")
        );
        assert_eq!(
            db.sstables_read().manifest_generation,
            ManifestId::new(prior_generation.get().saturating_add(1))
        );
        assert_eq!(db.table_stats(&table).await.l0_sstable_count, 0);

        file_system.crash();
        let reopened = Db::open(tiered_config("/compaction-cleanup-failure"), dependencies)
            .await
            .expect("reopen after cleanup failure");
        let reopened_table = reopened.table("events");

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            ManifestId::new(prior_generation.get().saturating_add(1))
        );
        assert_eq!(
            reopened.table_stats(&reopened_table).await.l0_sstable_count,
            0
        );
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read apple"),
            Some(Value::bytes("v2"))
        );
        assert_eq!(
            reopened_table
                .read(b"banana".to_vec())
                .await
                .expect("read banana"),
            None
        );
    }

    #[tokio::test]
    async fn successful_compaction_recovers_cleanly_after_post_cleanup_crash() {
        let (db, _table, file_system, dependencies, _oracle) =
            seed_leveled_compaction_fixture("/compaction-post-cleanup-crash").await;

        assert!(db.run_next_compaction().await.expect("run compaction"));
        let manifest_generation = db.sstables_read().manifest_generation;

        file_system.crash();
        let reopened = Db::open(
            tiered_config("/compaction-post-cleanup-crash"),
            dependencies,
        )
        .await
        .expect("reopen after compaction crash");
        let reopened_table = reopened.table("events");

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            manifest_generation
        );
        assert_eq!(
            reopened.table_stats(&reopened_table).await.l0_sstable_count,
            0
        );
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read apple"),
            Some(Value::bytes("v2"))
        );
        assert_eq!(
            reopened_table
                .read(b"banana".to_vec())
                .await
                .expect("read banana"),
            None
        );
    }

    #[tokio::test]
    async fn reads_and_writes_remain_consistent_while_compaction_runs() {
        let (db, table, _file_system, _dependencies, _oracle) =
            seed_leveled_compaction_fixture("/compaction-concurrency").await;

        let mut blocker = db.block_next_compaction_phase(CompactionPhase::OutputWritten);
        let compact_db = db.clone();
        let compaction = tokio::spawn(async move { compact_db.run_next_compaction().await });

        blocker.wait_until_reached().await;

        let snapshot = db.snapshot().await;
        assert_eq!(
            snapshot
                .read(&table, b"apple".to_vec())
                .await
                .expect("snapshot read before concurrent write"),
            Some(Value::bytes("v2"))
        );

        let latest = table
            .write(b"apple".to_vec(), Value::bytes("v3"))
            .await
            .expect("write during compaction");
        assert_eq!(
            table
                .read(b"apple".to_vec())
                .await
                .expect("latest read during compaction"),
            Some(Value::bytes("v3"))
        );
        assert_eq!(
            snapshot
                .read(&table, b"apple".to_vec())
                .await
                .expect("snapshot read after concurrent write"),
            Some(Value::bytes("v2"))
        );

        blocker.release();
        assert!(
            compaction
                .await
                .expect("join compaction task")
                .expect("compaction result")
        );

        assert_eq!(
            table
                .read_at(b"apple".to_vec(), snapshot.sequence())
                .await
                .expect("historical apple"),
            Some(Value::bytes("v2"))
        );
        assert_eq!(
            table
                .read_at(b"apple".to_vec(), latest)
                .await
                .expect("latest apple"),
            Some(Value::bytes("v3"))
        );
        assert_eq!(db.table_stats(&table).await.l0_sstable_count, 0);
    }

    #[tokio::test]
    async fn unreleased_snapshots_pin_gc_horizon_until_release() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/snapshot-horizon"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"user:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("write first value");
        let first = db.snapshot().await;
        assert_eq!(db.snapshot_gc_horizon(), first.sequence());

        table
            .write(b"user:2".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second value");
        let second = db.snapshot().await;

        assert_eq!(db.oldest_active_snapshot_sequence(), Some(first.sequence()));
        assert_eq!(db.snapshot_gc_horizon(), first.sequence());

        first.release();
        assert_eq!(
            db.oldest_active_snapshot_sequence(),
            Some(second.sequence())
        );
        assert_eq!(db.snapshot_gc_horizon(), second.sequence());

        second.release();
        assert_eq!(db.oldest_active_snapshot_sequence(), None);
        assert_eq!(db.snapshot_gc_horizon(), db.current_sequence());
    }

    #[tokio::test]
    async fn historical_reads_past_history_retention_return_snapshot_too_old() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/history-retention"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config_with_history_retention("events", 2))
            .await
            .expect("create table");

        let first = table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write first value");
        let oldest_available = table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second value");
        let latest = table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write third value");

        assert_snapshot_too_old(
            table
                .read_at(b"apple".to_vec(), first)
                .await
                .expect_err("historical read should be too old"),
            first,
            oldest_available,
        );
        let historical_scan = table
            .scan_at(b"a".to_vec(), b"z".to_vec(), first, ScanOptions::default())
            .await;
        let historical_scan_error = match historical_scan {
            Ok(_) => panic!("historical scan should be too old"),
            Err(error) => error,
        };
        assert_snapshot_too_old(historical_scan_error, first, oldest_available);

        assert_eq!(
            table
                .read_at(b"apple".to_vec(), oldest_available)
                .await
                .expect("retained historical read"),
            Some(Value::bytes("v2"))
        );
        assert_eq!(
            collect_rows(
                table
                    .scan_at(b"a".to_vec(), b"z".to_vec(), latest, ScanOptions::default(),)
                    .await
                    .expect("retained historical scan"),
            )
            .await,
            vec![
                (b"apple".to_vec(), Value::bytes("v2")),
                (b"banana".to_vec(), Value::bytes("yellow")),
            ]
        );
    }

    #[tokio::test]
    async fn table_stats_report_history_horizon_and_snapshot_pinning() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/history-retention-stats"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config_with_history_retention("events", 2))
            .await
            .expect("create table");

        let oldest_snapshot = table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write first value");
        let snapshot = db.snapshot().await;
        assert_eq!(snapshot.sequence(), oldest_snapshot);

        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second value");
        table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write third value");

        let pinned = db.table_stats(&table).await;
        assert_eq!(
            pinned.history_retention_floor_sequence,
            Some(SequenceNumber::new(2))
        );
        assert_eq!(pinned.history_gc_horizon_sequence, Some(oldest_snapshot));
        assert_eq!(
            pinned.oldest_active_snapshot_sequence,
            Some(oldest_snapshot)
        );
        assert_eq!(pinned.active_snapshot_count, 1);
        assert!(pinned.history_pinned_by_snapshots);

        snapshot.release();

        let unpinned = db.table_stats(&table).await;
        assert_eq!(
            unpinned.history_retention_floor_sequence,
            Some(SequenceNumber::new(2))
        );
        assert_eq!(
            unpinned.history_gc_horizon_sequence,
            Some(SequenceNumber::new(2))
        );
        assert_eq!(unpinned.oldest_active_snapshot_sequence, None);
        assert_eq!(unpinned.active_snapshot_count, 0);
        assert!(!unpinned.history_pinned_by_snapshots);
    }

    #[tokio::test]
    async fn compaction_reclaims_versions_older_than_retained_horizon() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/history-retention-compaction"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config_with_history_retention("events", 2))
            .await
            .expect("create table");
        let mut oracle = ShadowOracle::default();

        let first = table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write first value");
        oracle.apply(
            first,
            PointMutation::Put {
                key: b"apple".to_vec(),
                value: b"v1".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush first version");

        let oldest_available = table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second value");
        oracle.apply(
            oldest_available,
            PointMutation::Put {
                key: b"apple".to_vec(),
                value: b"v2".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush second version");

        let latest = table
            .write(b"apple".to_vec(), Value::bytes("v3"))
            .await
            .expect("write third value");
        oracle.apply(
            latest,
            PointMutation::Put {
                key: b"apple".to_vec(),
                value: b"v3".to_vec(),
            },
            true,
        );
        db.flush().await.expect("flush third version");

        for raw in oldest_available.get()..=latest.get() {
            let sequence = SequenceNumber::new(raw);
            assert_eq!(
                table
                    .read_at(b"apple".to_vec(), sequence)
                    .await
                    .expect("retained pre-compaction read"),
                oracle.value_at(b"apple", sequence).map(Value::bytes)
            );
        }

        assert!(db.run_next_compaction().await.expect("run compaction"));

        let retained_sequences = {
            let sstables = db.sstables_read();
            assert_eq!(sstables.live.len(), 1);
            sstables.live[0]
                .rows
                .iter()
                .filter(|row| row.key == b"apple".to_vec())
                .map(|row| row.sequence)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            retained_sequences,
            vec![SequenceNumber::new(3), SequenceNumber::new(2)]
        );

        for raw in oldest_available.get()..=latest.get() {
            let sequence = SequenceNumber::new(raw);
            assert_eq!(
                table
                    .read_at(b"apple".to_vec(), sequence)
                    .await
                    .expect("retained post-compaction read"),
                oracle.value_at(b"apple", sequence).map(Value::bytes)
            );
        }
        assert_snapshot_too_old(
            table
                .read_at(b"apple".to_vec(), first)
                .await
                .expect_err("gc'd read should be too old"),
            first,
            oldest_available,
        );
    }

    #[tokio::test]
    async fn active_snapshots_protect_compaction_history_until_release() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/snapshot-pinned-history"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config_with_history_retention("events", 1))
            .await
            .expect("create table");

        let first = table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write first value");
        db.flush().await.expect("flush first value");
        let snapshot = db.snapshot().await;
        assert_eq!(snapshot.sequence(), first);

        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second value");
        db.flush().await.expect("flush second value");
        let latest = table
            .write(b"apple".to_vec(), Value::bytes("v3"))
            .await
            .expect("write third value");
        db.flush().await.expect("flush third value");

        assert!(db.run_next_compaction().await.expect("run compaction"));

        let retained_sequences = {
            let sstables = db.sstables_read();
            assert_eq!(sstables.live.len(), 1);
            sstables.live[0]
                .rows
                .iter()
                .filter(|row| row.key == b"apple".to_vec())
                .map(|row| row.sequence)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            retained_sequences,
            vec![
                SequenceNumber::new(3),
                SequenceNumber::new(2),
                SequenceNumber::new(1),
            ]
        );
        assert_eq!(
            snapshot
                .read(&table, b"apple".to_vec())
                .await
                .expect("snapshot should stay readable while pinned"),
            Some(Value::bytes("v1"))
        );

        snapshot.release();
        assert_snapshot_too_old(
            snapshot
                .read(&table, b"apple".to_vec())
                .await
                .expect_err("released snapshot should become too old"),
            first,
            latest,
        );
        assert_eq!(
            table
                .read_at(b"apple".to_vec(), latest)
                .await
                .expect("latest read"),
            Some(Value::bytes("v3"))
        );
    }

    #[tokio::test]
    async fn concurrent_commits_assign_gap_free_sequences() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/concurrent-commit-sequences"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let mut tasks = Vec::new();
        for index in 0..32_u8 {
            let table = table.clone();
            tasks.push(tokio::spawn(async move {
                table
                    .write(vec![b'k', index], Value::Bytes(vec![index]))
                    .await
                    .expect("write")
            }));
        }

        let mut sequences = Vec::new();
        for task in tasks {
            sequences.push(task.await.expect("join task"));
        }
        sequences.sort();

        assert_eq!(
            sequences,
            (1..=32)
                .map(SequenceNumber::new)
                .collect::<Vec<SequenceNumber>>()
        );
        assert_eq!(db.current_sequence(), SequenceNumber::new(32));
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(32));
    }

    #[tokio::test]
    async fn read_set_conflicts_when_a_key_changes_after_the_read() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/read-set-conflict"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"user:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("write initial value");

        let snapshot = db.snapshot().await;
        assert_eq!(
            snapshot
                .read(&table, b"user:1".to_vec())
                .await
                .expect("snapshot read"),
            Some(Value::bytes("v1"))
        );

        table
            .write(b"user:1".to_vec(), Value::bytes("v2"))
            .await
            .expect("write newer value");

        let mut batch = db.write_batch();
        batch.put(&table, b"user:2".to_vec(), Value::bytes("v3"));
        let mut read_set = db.read_set();
        read_set.add(&table, b"user:1".to_vec(), snapshot.sequence());

        let error = db
            .commit(batch, CommitOptions::default().with_read_set(read_set))
            .await
            .expect_err("commit should conflict");
        assert_eq!(error, CommitError::Conflict);
    }

    #[tokio::test]
    async fn group_commit_batches_waiters_under_one_sync() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/group-commit-batch"),
            dependencies(file_system.clone(), object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let before_syncs = file_system.operation_count(FileSystemOperation::Sync);
        let mut sync_blocker = db.block_next_commit_phase(CommitPhase::BeforeDurabilitySync);

        let first_table = table.clone();
        let first = tokio::spawn(async move {
            first_table
                .write(b"user:1".to_vec(), Value::bytes("v1"))
                .await
                .expect("first write")
        });

        let first_sequence = sync_blocker.sequence().await;
        assert_eq!(first_sequence, SequenceNumber::new(1));

        let second_table = table.clone();
        let second = tokio::spawn(async move {
            second_table
                .write(b"user:2".to_vec(), Value::bytes("v2"))
                .await
                .expect("second write")
        });

        while db.inner.next_sequence.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }

        sync_blocker.release();

        let committed = vec![
            first.await.expect("join first"),
            second.await.expect("join second"),
        ];
        assert_eq!(
            committed,
            vec![SequenceNumber::new(1), SequenceNumber::new(2)]
        );
        assert_eq!(
            file_system.operation_count(FileSystemOperation::Sync) - before_syncs,
            1
        );
    }

    #[tokio::test]
    async fn visible_prefix_does_not_skip_an_earlier_commit() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config_with_durability("/visible-prefix-order", TieredDurabilityMode::Deferred),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let mut blocker = db.block_next_commit_phase(CommitPhase::BeforeMemtableInsert);
        let first_table = table.clone();
        let first = tokio::spawn(async move {
            first_table
                .write(b"user:1".to_vec(), Value::bytes("v1"))
                .await
                .expect("first write")
        });

        let first_sequence = blocker.sequence().await;
        assert_eq!(first_sequence, SequenceNumber::new(1));

        let second_sequence = table
            .write(b"user:2".to_vec(), Value::bytes("v2"))
            .await
            .expect("second write");
        assert_eq!(second_sequence, SequenceNumber::new(2));
        assert_eq!(db.current_sequence(), SequenceNumber::new(0));
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));

        blocker.release();

        assert_eq!(first.await.expect("join first"), SequenceNumber::new(1));
        assert_eq!(db.current_sequence(), SequenceNumber::new(2));
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));
    }

    #[test]
    fn append_merge_operator_partial_merge_is_associative_and_full_merge_is_deterministic() {
        let operator = AppendMergeOperator;
        let existing = bytes("seed");
        let a = bytes("A");
        let b = bytes("B");
        let c = bytes("C");

        let merged_ab = operator
            .partial_merge(b"doc", &a, &b)
            .expect("partial merge A+B")
            .expect("append merge should collapse adjacent operands");
        let merged_bc = operator
            .partial_merge(b"doc", &b, &c)
            .expect("partial merge B+C")
            .expect("append merge should collapse adjacent operands");

        let direct = operator
            .full_merge(b"doc", Some(&existing), &[a.clone(), b.clone(), c.clone()])
            .expect("deterministic full merge");
        let left_assoc = operator
            .full_merge(b"doc", Some(&existing), &[merged_ab, c.clone()])
            .expect("left-associated merge");
        let right_assoc = operator
            .full_merge(b"doc", Some(&existing), &[a.clone(), merged_bc])
            .expect("right-associated merge");

        assert_eq!(direct, bytes("seed|A|B|C"));
        assert_eq!(direct, left_assoc);
        assert_eq!(direct, right_assoc);
        assert_eq!(
            operator
                .full_merge(b"doc", Some(&existing), &[a, b, c])
                .expect("repeat deterministic full merge"),
            direct
        );
    }

    #[tokio::test]
    async fn merge_resolution_preserves_non_commutative_commit_order_across_memtables_and_sstables()
    {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/merge-ordering"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(merge_row_table_config("events", Some(2)))
            .await
            .expect("create merge table");

        let base = table
            .write(b"doc".to_vec(), bytes("seed"))
            .await
            .expect("write base value");
        let first_merge = table
            .merge(b"doc".to_vec(), bytes("A"))
            .await
            .expect("write first merge operand");
        db.flush().await.expect("flush merge chain");
        let second_merge = table
            .merge(b"doc".to_vec(), bytes("B"))
            .await
            .expect("write second merge operand");

        assert_eq!(
            table
                .read(b"doc".to_vec())
                .await
                .expect("latest merged read"),
            Some(bytes("seed|A|B"))
        );
        assert_eq!(
            table
                .read_at(b"doc".to_vec(), base)
                .await
                .expect("read base sequence"),
            Some(bytes("seed"))
        );
        assert_eq!(
            table
                .read_at(b"doc".to_vec(), first_merge)
                .await
                .expect("read first merged sequence"),
            Some(bytes("seed|A"))
        );
        assert_eq!(
            table
                .read_at(b"doc".to_vec(), second_merge)
                .await
                .expect("read second merged sequence"),
            Some(bytes("seed|A|B"))
        );
        assert_eq!(
            collect_rows(
                table
                    .scan(b"doc".to_vec(), b"doe".to_vec(), ScanOptions::default(),)
                    .await
                    .expect("scan merged key"),
            )
            .await,
            vec![(b"doc".to_vec(), bytes("seed|A|B"))]
        );
    }

    #[tokio::test]
    async fn long_merge_chains_force_a_collapsed_shadow_value_on_read() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/merge-collapse"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(merge_row_table_config("events", Some(2)))
            .await
            .expect("create merge table");

        table
            .write(b"doc".to_vec(), bytes("seed"))
            .await
            .expect("write base value");
        table
            .merge(b"doc".to_vec(), bytes("A"))
            .await
            .expect("write operand A");
        table
            .merge(b"doc".to_vec(), bytes("B"))
            .await
            .expect("write operand B");
        let latest = table
            .merge(b"doc".to_vec(), bytes("C"))
            .await
            .expect("write operand C");
        db.flush().await.expect("flush unresolved operands");

        let table_id = table.id().expect("table id");
        assert!(
            db.memtables_read()
                .read_at(table_id, b"doc", latest)
                .is_none(),
            "no collapsed shadow row should exist before the read"
        );

        assert_eq!(
            table
                .read(b"doc".to_vec())
                .await
                .expect("resolve long chain"),
            Some(bytes("seed|A|B|C"))
        );

        let collapsed = db
            .memtables_read()
            .read_at(table_id, b"doc", latest)
            .expect("forced collapse should install a shadow row");
        assert_eq!(collapsed.kind, ChangeKind::Put);
        assert_eq!(collapsed.value, Some(bytes("seed|A|B|C")));
    }

    #[tokio::test]
    async fn compaction_rewrites_merge_rows_without_changing_historical_reads() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/merge-compaction"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(merge_row_table_config("events", Some(2)))
            .await
            .expect("create merge table");

        let base = table
            .write(b"doc".to_vec(), bytes("seed"))
            .await
            .expect("write base value");
        db.flush().await.expect("flush base value");
        let first_merge = table
            .merge(b"doc".to_vec(), bytes("A"))
            .await
            .expect("write operand A");
        db.flush().await.expect("flush operand A");
        let second_merge = table
            .merge(b"doc".to_vec(), bytes("B"))
            .await
            .expect("write operand B");
        db.flush().await.expect("flush operand B");

        let before = vec![
            table
                .read_at(b"doc".to_vec(), base)
                .await
                .expect("read base before compaction"),
            table
                .read_at(b"doc".to_vec(), first_merge)
                .await
                .expect("read first merge before compaction"),
            table
                .read_at(b"doc".to_vec(), second_merge)
                .await
                .expect("read second merge before compaction"),
        ];

        assert!(
            db.run_next_compaction()
                .await
                .expect("run merge compaction")
        );

        let after = vec![
            table
                .read_at(b"doc".to_vec(), base)
                .await
                .expect("read base after compaction"),
            table
                .read_at(b"doc".to_vec(), first_merge)
                .await
                .expect("read first merge after compaction"),
            table
                .read_at(b"doc".to_vec(), second_merge)
                .await
                .expect("read second merge after compaction"),
        ];
        assert_eq!(after, before);

        let live = db.sstables_read().live.clone();
        assert_eq!(live.len(), 1);
        let doc_rows = live[0]
            .rows
            .iter()
            .filter(|row| row.key == b"doc")
            .collect::<Vec<_>>();
        assert_eq!(doc_rows.len(), 3);
        assert!(doc_rows.iter().all(|row| row.kind == ChangeKind::Put));
        assert_eq!(
            doc_rows
                .iter()
                .map(|row| row.value.clone())
                .collect::<Vec<_>>(),
            vec![
                Some(bytes("seed|A|B")),
                Some(bytes("seed|A")),
                Some(bytes("seed")),
            ]
        );
    }

    #[tokio::test]
    async fn reopen_recovers_unresolved_merge_operands_from_sstables_and_memtables() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);
        let db = Db::open(tiered_config("/merge-recovery"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(merge_row_table_config("events", Some(2)))
            .await
            .expect("create merge table");

        table
            .write(b"doc".to_vec(), bytes("seed"))
            .await
            .expect("write base value");
        let first_merge = table
            .merge(b"doc".to_vec(), bytes("A"))
            .await
            .expect("write first operand");
        db.flush().await.expect("flush unresolved SSTable operand");
        let latest = table
            .merge(b"doc".to_vec(), bytes("B"))
            .await
            .expect("write tail operand");

        let reopened = Db::open(tiered_config("/merge-recovery"), dependencies)
            .await
            .expect("reopen db");
        let reopened_table = reopened
            .create_table(merge_row_table_config("events", Some(2)))
            .await
            .expect("reattach merge operator through public API");
        let table_id = reopened_table.id().expect("table id after reopen");

        assert!(
            reopened
                .sstables_read()
                .live
                .iter()
                .flat_map(|sstable| sstable.rows.iter())
                .any(|row| row.key == b"doc" && row.kind == ChangeKind::Merge),
            "reopened SSTables should still contain unresolved merge operands"
        );
        assert_eq!(
            reopened
                .memtables_read()
                .read_at(table_id, b"doc", latest)
                .expect("recover memtable tail")
                .kind,
            ChangeKind::Merge
        );

        assert_eq!(
            reopened_table
                .read(b"doc".to_vec())
                .await
                .expect("latest merged read after reopen"),
            Some(bytes("seed|A|B"))
        );
        assert_eq!(
            reopened_table
                .read_at(b"doc".to_vec(), first_merge)
                .await
                .expect("historical read after reopen"),
            Some(bytes("seed|A"))
        );
    }

    #[tokio::test]
    async fn durability_modes_publish_expected_watermarks() {
        let group_fs = Arc::new(crate::StubFileSystem::default());
        let deferred_fs = Arc::new(crate::StubFileSystem::default());
        let s3_fs = Arc::new(crate::StubFileSystem::default());

        let group_db = Db::open(
            tiered_config("/group-watermarks"),
            dependencies(group_fs, Arc::new(StubObjectStore::default())),
        )
        .await
        .expect("open group db");
        let deferred_db = Db::open(
            tiered_config_with_durability("/deferred-watermarks", TieredDurabilityMode::Deferred),
            dependencies(deferred_fs, Arc::new(StubObjectStore::default())),
        )
        .await
        .expect("open deferred db");
        let s3_db = Db::open(
            s3_primary_config("s3-watermarks"),
            dependencies(s3_fs, Arc::new(StubObjectStore::default())),
        )
        .await
        .expect("open s3 db");

        let group_table = group_db
            .create_table(row_table_config("events"))
            .await
            .expect("create group table");
        let deferred_table = deferred_db
            .create_table(row_table_config("events"))
            .await
            .expect("create deferred table");
        let s3_table = s3_db
            .create_table(row_table_config("events"))
            .await
            .expect("create s3 table");

        assert_eq!(
            group_table
                .write(b"user:1".to_vec(), Value::bytes("v1"))
                .await
                .expect("group write"),
            SequenceNumber::new(1)
        );
        assert_eq!(group_db.current_sequence(), SequenceNumber::new(1));
        assert_eq!(group_db.current_durable_sequence(), SequenceNumber::new(1));

        assert_eq!(
            deferred_table
                .write(b"user:1".to_vec(), Value::bytes("v1"))
                .await
                .expect("deferred write"),
            SequenceNumber::new(1)
        );
        assert_eq!(deferred_db.current_sequence(), SequenceNumber::new(1));
        assert_eq!(
            deferred_db.current_durable_sequence(),
            SequenceNumber::new(0)
        );
        deferred_db.flush().await.expect("flush deferred");
        assert_eq!(
            deferred_db.current_durable_sequence(),
            SequenceNumber::new(1)
        );

        assert_eq!(
            s3_table
                .write(b"user:1".to_vec(), Value::bytes("v1"))
                .await
                .expect("s3 write"),
            SequenceNumber::new(1)
        );
        assert_eq!(s3_db.current_sequence(), SequenceNumber::new(1));
        assert_eq!(s3_db.current_durable_sequence(), SequenceNumber::new(0));
        s3_db.flush().await.expect("flush s3");
        assert_eq!(s3_db.current_durable_sequence(), SequenceNumber::new(1));
    }
}
