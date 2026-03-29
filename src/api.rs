use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    hash::{Hash, Hasher},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};

use futures::{Stream, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, watch};

use crate::{
    config::{
        CompactionStrategy, DbConfig, S3PrimaryStorageConfig, StorageConfig, TableConfig,
        TableFormat, TableMetadata, TieredDurabilityMode, TieredStorageConfig,
    },
    error::{
        CommitError, CreateTableError, FlushError, OpenError, ReadError, SnapshotTooOld,
        StorageError, StorageErrorKind, SubscriptionClosed, WriteError,
    },
    ids::{CommitId, FieldId, LogCursor, SequenceNumber, TableId},
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
const OBJECT_CATALOG_RELATIVE_KEY: &str = "catalog/CATALOG.json";
const MVCC_KEY_SEPARATOR: u8 = 0;

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
    next_table_id: AtomicU32,
    next_sequence: AtomicU64,
    current_sequence: AtomicU64,
    current_durable_sequence: AtomicU64,
    tables: RwLock<BTreeMap<String, StoredTable>>,
    memtables: RwLock<MemtableState>,
    snapshot_tracker: Mutex<SnapshotTracker>,
    next_snapshot_id: AtomicU64,
    visible_watchers: Mutex<BTreeMap<String, watch::Sender<SequenceNumber>>>,
    durable_watchers: Mutex<BTreeMap<String, watch::Sender<SequenceNumber>>>,
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
    bloom_filter_bits_per_key: Option<u32>,
    compaction_strategy: CompactionStrategy,
    schema: Option<SchemaDefinition>,
    metadata: TableMetadata,
}

#[derive(Clone, Debug)]
struct ResolvedBatchOperation {
    table_id: TableId,
    table_name: String,
    key: Key,
    kind: ChangeKind,
    value: Option<Value>,
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

    fn visible_value(&self) -> Option<Value> {
        match self.kind {
            ChangeKind::Delete => None,
            ChangeKind::Put | ChangeKind::Merge => self.value.clone(),
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

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.tables.values().all(TableMemtable::is_empty)
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

    fn scan(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
        matcher: KeyMatcher<'_>,
        opts: &ScanOptions,
    ) -> Vec<(Key, Value)> {
        let limit = opts.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Vec::new();
        }

        let mut keys = BTreeSet::new();
        self.mutable
            .collect_matching_keys(table_id, &matcher, &mut keys);
        for immutable in &self.immutables {
            immutable
                .memtable
                .collect_matching_keys(table_id, &matcher, &mut keys);
        }

        let mut ordered_keys = keys.into_iter().collect::<Vec<_>>();
        if opts.reverse {
            ordered_keys.reverse();
        }

        let mut rows = Vec::new();
        for key in ordered_keys {
            let Some(entry) = self.read_at(table_id, &key, sequence) else {
                continue;
            };
            let Some(value) = entry.visible_value() else {
                continue;
            };

            rows.push((key, value));
            if rows.len() >= limit {
                break;
            }
        }

        rows
    }

    #[cfg(test)]
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

    #[cfg(test)]
    fn oldest_active(&self) -> Option<SequenceNumber> {
        self.counts_by_sequence.keys().next().copied()
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
                bloom_filter_bits_per_key: table.config.bloom_filter_bits_per_key,
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
                compaction_filter: None,
                bloom_filter_bits_per_key: self.config.bloom_filter_bits_per_key,
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

        Ok(Self {
            inner: Arc::new(DbInner {
                config,
                dependencies,
                catalog_location,
                catalog_write_lock: AsyncMutex::new(()),
                commit_lock: AsyncMutex::new(()),
                next_table_id: AtomicU32::new(next_table_id),
                next_sequence: AtomicU64::new(0),
                current_sequence: AtomicU64::new(0),
                current_durable_sequence: AtomicU64::new(0),
                tables: RwLock::new(tables),
                memtables: RwLock::new(MemtableState::default()),
                snapshot_tracker: Mutex::new(SnapshotTracker::default()),
                next_snapshot_id: AtomicU64::new(0),
                visible_watchers: Mutex::new(BTreeMap::new()),
                durable_watchers: Mutex::new(BTreeMap::new()),
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
        if matches!(config.format, TableFormat::Columnar) && config.schema.is_none() {
            return Err(CreateTableError::InvalidConfig(
                "columnar tables require a schema".to_string(),
            ));
        }

        Ok(())
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
        if updated_tables.contains_key(&name) {
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
        _opts: CommitOptions,
    ) -> Result<SequenceNumber, CommitError> {
        if batch.is_empty() {
            return Err(CommitError::EmptyBatch);
        }

        let _commit_guard = self.inner.commit_lock.lock().await;
        let sequence =
            SequenceNumber::new(self.inner.next_sequence.fetch_add(1, Ordering::SeqCst) + 1);
        let resolved_operations = self.resolve_batch_operations(batch.operations())?;
        self.memtables_write().apply(sequence, &resolved_operations);

        self.inner
            .current_sequence
            .store(sequence.get(), Ordering::SeqCst);

        let touched_tables: BTreeSet<String> = resolved_operations
            .iter()
            .map(|operation| operation.table_name.clone())
            .collect();

        self.notify_watchers(&self.inner.visible_watchers, &touched_tables, sequence);

        if self.durable_on_commit() {
            self.inner
                .current_durable_sequence
                .store(sequence.get(), Ordering::SeqCst);
            self.notify_watchers(&self.inner.durable_watchers, &touched_tables, sequence);
        }

        Ok(sequence)
    }

    pub async fn flush(&self) -> Result<(), FlushError> {
        let durable = self.current_sequence();
        self.inner
            .current_durable_sequence
            .store(durable.get(), Ordering::SeqCst);

        let tables: BTreeSet<String> = self.tables_read().keys().cloned().collect();
        self.notify_watchers(&self.inner.durable_watchers, &tables, durable);

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
        let metadata = self
            .tables_read()
            .get(table.name())
            .map(|table| table.config.metadata.clone())
            .unwrap_or_default();
        let (pending_flush_bytes, immutable_memtable_count) = table
            .resolve_id()
            .map(|table_id| {
                let memtables = self.memtables_read();
                (
                    memtables.pending_flush_bytes(table_id),
                    memtables.immutable_memtable_count(table_id),
                )
            })
            .unwrap_or_default();

        TableStats {
            pending_flush_bytes,
            immutable_memtable_count,
            metadata,
            ..TableStats::default()
        }
    }

    pub async fn pending_work(&self) -> Vec<PendingWork> {
        Vec::new()
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
                BatchOperation::Put { table, key, value } => Ok(ResolvedBatchOperation {
                    table_id: self.resolve_table_id(table).ok_or_else(|| {
                        CommitError::Storage(StorageError::not_found(format!(
                            "table does not exist: {}",
                            table.name()
                        )))
                    })?,
                    table_name: table.name().to_string(),
                    key: key.clone(),
                    kind: ChangeKind::Put,
                    value: Some(value.clone()),
                }),
                BatchOperation::Merge { table, key, value } => Ok(ResolvedBatchOperation {
                    table_id: self.resolve_table_id(table).ok_or_else(|| {
                        CommitError::Storage(StorageError::not_found(format!(
                            "table does not exist: {}",
                            table.name()
                        )))
                    })?,
                    table_name: table.name().to_string(),
                    key: key.clone(),
                    kind: ChangeKind::Merge,
                    value: Some(value.clone()),
                }),
                BatchOperation::Delete { table, key } => Ok(ResolvedBatchOperation {
                    table_id: self.resolve_table_id(table).ok_or_else(|| {
                        CommitError::Storage(StorageError::not_found(format!(
                            "table does not exist: {}",
                            table.name()
                        )))
                    })?,
                    table_name: table.name().to_string(),
                    key: key.clone(),
                    kind: ChangeKind::Delete,
                    value: None,
                }),
            })
            .collect()
    }

    fn resolve_table_id(&self, table: &Table) -> Option<TableId> {
        table
            .id
            .or_else(|| self.tables_read().get(table.name()).map(|stored| stored.id))
    }

    fn read_memtable_value(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Option<Value> {
        self.memtables_read()
            .read_at(table_id, key, sequence)
            .and_then(|entry| entry.visible_value())
    }

    fn scan_memtable(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
        matcher: KeyMatcher<'_>,
        opts: &ScanOptions,
    ) -> Vec<(Key, Value)> {
        self.memtables_read()
            .scan(table_id, sequence, matcher, opts)
    }

    fn release_snapshot_registration(&self, id: u64) {
        mutex_lock(&self.inner.snapshot_tracker).release(id);
    }

    #[cfg(test)]
    fn oldest_active_snapshot_sequence(&self) -> Option<SequenceNumber> {
        mutex_lock(&self.inner.snapshot_tracker).oldest_active()
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

    fn notify_watchers(
        &self,
        watchers: &Mutex<BTreeMap<String, watch::Sender<SequenceNumber>>>,
        touched_tables: &BTreeSet<String>,
        sequence: SequenceNumber,
    ) {
        let watchers = mutex_lock(watchers);
        for table in touched_tables {
            if let Some(sender) = watchers.get(table) {
                sender.send_replace(sequence);
            }
        }
    }

    fn tables_read(&self) -> RwLockReadGuard<'_, BTreeMap<String, StoredTable>> {
        self.inner.tables.read().expect("db table lock poisoned")
    }

    fn tables_write(&self) -> RwLockWriteGuard<'_, BTreeMap<String, StoredTable>> {
        self.inner.tables.write().expect("db table lock poisoned")
    }

    fn memtables_read(&self) -> RwLockReadGuard<'_, MemtableState> {
        self.inner
            .memtables
            .read()
            .expect("db memtable lock poisoned")
    }

    fn memtables_write(&self) -> RwLockWriteGuard<'_, MemtableState> {
        self.inner
            .memtables
            .write()
            .expect("db memtable lock poisoned")
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

        Ok(self.db.read_memtable_value(table_id, &key, sequence))
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

        let rows = self.db.scan_memtable(
            table_id,
            sequence,
            KeyMatcher::Range {
                start: &start,
                end: &end,
            },
            &opts,
        );
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

        let rows = self
            .db
            .scan_memtable(table_id, sequence, KeyMatcher::Prefix(&prefix), &opts);
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
    mutex.lock().expect("db mutex poisoned")
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use futures::StreamExt;
    use serde_json::json;

    use super::{
        Db, LOCAL_CATALOG_RELATIVE_PATH, LOCAL_CATALOG_TEMP_SUFFIX, SchemaDefinition, StoredTable,
        decode_mvcc_key, encode_mvcc_key,
    };
    use crate::{
        CommitId, CompactionStrategy, DbConfig, DbDependencies, FieldDefinition, FieldId,
        FieldType, FieldValue, FileSystemFailure, FileSystemOperation, S3Location,
        S3PrimaryStorageConfig, ScanOptions, SequenceNumber, SsdConfig, StorageConfig, StubClock,
        StubObjectStore, StubRng, TableConfig, TableFormat, TieredDurabilityMode,
        TieredStorageConfig, Value,
    };

    fn tiered_config(path: &str) -> DbConfig {
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
                durability: TieredDurabilityMode::GroupCommit,
            }),
            scheduler: None,
        }
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

    fn row_table_config(name: &str) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            format: TableFormat::Row,
            merge_operator: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(10),
            compaction_strategy: CompactionStrategy::Leveled,
            schema: None,
            metadata: BTreeMap::from([
                ("priority".to_string(), json!("high")),
                ("tenant".to_string(), json!("alpha")),
            ]),
        }
    }

    fn columnar_table_config(name: &str) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            format: TableFormat::Columnar,
            merge_operator: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(6),
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
            table.config.bloom_filter_bits_per_key,
            expected.bloom_filter_bits_per_key
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
}
