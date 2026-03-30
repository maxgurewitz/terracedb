use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    hash::{Hash, Hasher},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
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
        CompactionDecision, CompactionDecisionContext, CompactionStrategy, DbConfig, S3Location,
        S3PrimaryStorageConfig, StorageConfig, TableConfig, TableFormat, TableMetadata,
        TieredDurabilityMode, TieredStorageConfig,
    },
    engine::commit_log::{
        CommitEntry, CommitRecord, SegmentFooter, SegmentManager, SegmentOptions,
        encode_segment_bytes, scan_table_from_segment_bytes, segment_footer_from_bytes,
    },
    error::{
        CommitError, CreateTableError, FlushError, OpenError, ReadError, SnapshotTooOld,
        StorageError, StorageErrorKind, SubscriptionClosed, WriteError,
    },
    ids::{CommitId, FieldId, LogCursor, ManifestId, SegmentId, SequenceNumber, TableId},
    io::{DbDependencies, ObjectStore, OpenOptions},
    remote::{ObjectKeyLayout, StorageSource, UnifiedStorage},
    scheduler::{
        DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT, PendingWork, PendingWorkType, RoundRobinScheduler,
        ScheduleAction, Scheduler, TableStats,
    },
};

pub type Key = Vec<u8>;
pub type KeyPrefix = Vec<u8>;
pub type KvStream = Pin<Box<dyn Stream<Item = (Key, Value)> + Send + 'static>>;
pub type ChangeStream = Pin<Box<dyn Stream<Item = ChangeEntry> + Send + 'static>>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaDefinition {
    pub version: u32,
    pub fields: Vec<FieldDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
pub type NamedColumnarRecord = BTreeMap<String, FieldValue>;

impl SchemaDefinition {
    pub fn validate(&self) -> Result<(), StorageError> {
        SchemaValidation::new(self).map(|_| ())
    }

    pub fn validate_successor(&self, successor: &Self) -> Result<(), StorageError> {
        let current = SchemaValidation::new(self)?;
        let next = SchemaValidation::new(successor)?;

        if successor.version <= self.version {
            return Err(StorageError::unsupported(format!(
                "schema version must increase: current={}, successor={}",
                self.version, successor.version
            )));
        }

        for (&field_id, current_field) in &current.fields_by_id {
            let Some(next_field) = next.fields_by_id.get(&field_id) else {
                continue;
            };
            if next_field.field_type != current_field.field_type {
                return Err(StorageError::unsupported(format!(
                    "field {} ({}) changes type from {} to {}",
                    field_id.get(),
                    current_field.name,
                    current_field.field_type.as_str(),
                    next_field.field_type.as_str()
                )));
            }
        }

        for (&field_id, next_field) in &next.fields_by_id {
            if current.fields_by_id.contains_key(&field_id) {
                continue;
            }
            if !next_field.nullable && next_field.default.is_none() {
                return Err(StorageError::unsupported(format!(
                    "new field {} ({}) must be nullable or define a default for lazy schema evolution",
                    field_id.get(),
                    next_field.name
                )));
            }
        }

        for (name, &current_field_id) in &current.field_ids_by_name {
            let Some(&next_field_id) = next.field_ids_by_name.get(name) else {
                continue;
            };
            if next_field_id != current_field_id {
                return Err(StorageError::unsupported(format!(
                    "field name {} changes id from {} to {}; renames must preserve field ids",
                    name,
                    current_field_id.get(),
                    next_field_id.get()
                )));
            }
        }

        Ok(())
    }

    pub fn normalize_record(
        &self,
        record: &ColumnarRecord,
    ) -> Result<ColumnarRecord, StorageError> {
        SchemaValidation::new(self)?.normalize_record(record)
    }

    pub fn normalize_merge_operand(
        &self,
        record: &ColumnarRecord,
    ) -> Result<ColumnarRecord, StorageError> {
        SchemaValidation::new(self)?.normalize_merge_operand(record)
    }

    pub fn record_from_names<I, S>(&self, fields: I) -> Result<ColumnarRecord, StorageError>
    where
        I: IntoIterator<Item = (S, FieldValue)>,
        S: Into<String>,
    {
        SchemaValidation::new(self)?.record_from_names(fields)
    }
}

impl FieldType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::Float64 => "float64",
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Bool => "bool",
        }
    }
}

struct SchemaValidation<'a> {
    schema: &'a SchemaDefinition,
    fields_by_id: BTreeMap<FieldId, &'a FieldDefinition>,
    field_ids_by_name: BTreeMap<String, FieldId>,
}

impl<'a> SchemaValidation<'a> {
    fn new(schema: &'a SchemaDefinition) -> Result<Self, StorageError> {
        if schema.version == 0 {
            return Err(StorageError::unsupported(
                "schema version must be greater than zero",
            ));
        }
        if schema.fields.is_empty() {
            return Err(StorageError::unsupported(
                "columnar schemas must define at least one field",
            ));
        }

        let mut fields_by_id = BTreeMap::new();
        let mut field_ids_by_name = BTreeMap::new();

        for field in &schema.fields {
            if field.id.get() == 0 {
                return Err(StorageError::unsupported(format!(
                    "field {} ({}) uses reserved field id 0",
                    field.id.get(),
                    field.name
                )));
            }
            if field.name.trim().is_empty() {
                return Err(StorageError::unsupported(format!(
                    "field {} has an empty name",
                    field.id.get()
                )));
            }
            if fields_by_id.insert(field.id, field).is_some() {
                return Err(StorageError::unsupported(format!(
                    "schema contains duplicate field id {}",
                    field.id.get()
                )));
            }
            if field_ids_by_name
                .insert(field.name.clone(), field.id)
                .is_some()
            {
                return Err(StorageError::unsupported(format!(
                    "schema contains duplicate field name {}",
                    field.name
                )));
            }
            if let Some(default) = &field.default {
                validate_field_value_against_definition(field, default, "default value")?;
            }
        }

        Ok(Self {
            schema,
            fields_by_id,
            field_ids_by_name,
        })
    }

    fn normalize_record(&self, record: &ColumnarRecord) -> Result<ColumnarRecord, StorageError> {
        for (&field_id, value) in record {
            let field = self.fields_by_id.get(&field_id).ok_or_else(|| {
                StorageError::unsupported(format!(
                    "record contains unknown field id {}",
                    field_id.get()
                ))
            })?;
            validate_field_value_against_definition(field, value, "record value")?;
        }

        let mut normalized = BTreeMap::new();
        for field in &self.schema.fields {
            let value = match record.get(&field.id) {
                Some(value) => value.clone(),
                None => self.missing_field_value(field)?,
            };
            normalized.insert(field.id, value);
        }

        Ok(normalized)
    }

    fn normalize_merge_operand(
        &self,
        record: &ColumnarRecord,
    ) -> Result<ColumnarRecord, StorageError> {
        for (&field_id, value) in record {
            let field = self.fields_by_id.get(&field_id).ok_or_else(|| {
                StorageError::unsupported(format!(
                    "record contains unknown field id {}",
                    field_id.get()
                ))
            })?;
            validate_merge_operand_value_against_definition(field, value, "merge operand value")?;
        }

        let mut normalized = BTreeMap::new();
        for field in &self.schema.fields {
            let value = record.get(&field.id).cloned().unwrap_or(FieldValue::Null);
            validate_merge_operand_value_against_definition(field, &value, "merge operand value")?;
            normalized.insert(field.id, value);
        }

        Ok(normalized)
    }

    fn record_from_names<I, S>(&self, fields: I) -> Result<ColumnarRecord, StorageError>
    where
        I: IntoIterator<Item = (S, FieldValue)>,
        S: Into<String>,
    {
        let mut resolved = BTreeMap::new();
        for (name, value) in fields {
            let name = name.into();
            let field_id = self.field_ids_by_name.get(&name).copied().ok_or_else(|| {
                StorageError::unsupported(format!("record contains unknown field name {}", name))
            })?;
            if resolved.insert(field_id, value).is_some() {
                return Err(StorageError::unsupported(format!(
                    "record contains duplicate field name {}",
                    name
                )));
            }
        }

        self.normalize_record(&resolved)
    }

    fn missing_field_value(&self, field: &FieldDefinition) -> Result<FieldValue, StorageError> {
        missing_field_value_for_definition(field)
    }
}

fn missing_field_value_for_definition(field: &FieldDefinition) -> Result<FieldValue, StorageError> {
    if let Some(default) = &field.default {
        return Ok(default.clone());
    }
    if field.nullable {
        return Ok(FieldValue::Null);
    }

    Err(StorageError::unsupported(format!(
        "record is missing required field {}",
        field.name
    )))
}

fn validate_field_value_against_definition(
    field: &FieldDefinition,
    value: &FieldValue,
    context: &str,
) -> Result<(), StorageError> {
    if matches!(value, FieldValue::Null) {
        return if field.nullable {
            Ok(())
        } else {
            Err(StorageError::unsupported(format!(
                "{context} for field {} cannot be null",
                field.name
            )))
        };
    }

    if field_value_matches_type(field.field_type, value) {
        Ok(())
    } else {
        Err(StorageError::unsupported(format!(
            "{context} for field {} has type {}, expected {}",
            field.name,
            field_value_type_name(value),
            field.field_type.as_str()
        )))
    }
}

fn validate_merge_operand_value_against_definition(
    field: &FieldDefinition,
    value: &FieldValue,
    context: &str,
) -> Result<(), StorageError> {
    if matches!(value, FieldValue::Null) {
        return Ok(());
    }

    validate_field_value_against_definition(field, value, context)
}

fn field_value_matches_type(field_type: FieldType, value: &FieldValue) -> bool {
    matches!(
        (field_type, value),
        (FieldType::Int64, FieldValue::Int64(_))
            | (FieldType::Float64, FieldValue::Float64(_))
            | (FieldType::String, FieldValue::String(_))
            | (FieldType::Bytes, FieldValue::Bytes(_))
            | (FieldType::Bool, FieldValue::Bool(_))
    )
}

fn field_value_type_name(value: &FieldValue) -> &'static str {
    match value {
        FieldValue::Null => "null",
        FieldValue::Int64(_) => "int64",
        FieldValue::Float64(_) => "float64",
        FieldValue::String(_) => "string",
        FieldValue::Bytes(_) => "bytes",
        FieldValue::Bool(_) => "bool",
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Bytes(Vec<u8>),
    Record(ColumnarRecord),
}

impl Value {
    pub fn bytes(value: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(value.into())
    }

    pub fn record(record: ColumnarRecord) -> Self {
        Self::Record(record)
    }

    pub fn named_record<I, S>(schema: &SchemaDefinition, fields: I) -> Result<Self, StorageError>
    where
        I: IntoIterator<Item = (S, FieldValue)>,
        S: Into<String>,
    {
        Ok(Self::Record(schema.record_from_names(fields)?))
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
    subscription: WatermarkSubscription,
}

impl WatermarkReceiver {
    fn new(
        registry: &Arc<WatermarkRegistry>,
        table_name: &str,
        inner: watch::Receiver<SequenceNumber>,
    ) -> Self {
        Self {
            inner,
            subscription: WatermarkSubscription::new(registry, table_name),
        }
    }

    pub fn current(&self) -> SequenceNumber {
        *self.inner.borrow()
    }

    pub async fn changed(&mut self) -> Result<SequenceNumber, SubscriptionClosed> {
        self.inner.changed().await.map_err(|_| SubscriptionClosed)?;
        Ok(*self.inner.borrow_and_update())
    }

    #[cfg(test)]
    fn has_changed(&self) -> Result<bool, SubscriptionClosed> {
        self.inner.has_changed().map_err(|_| SubscriptionClosed)
    }
}

impl Clone for WatermarkReceiver {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            subscription: self.subscription.clone(),
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
const REMOTE_MANIFEST_FORMAT_VERSION: u32 = 1;
const BACKUP_GC_METADATA_FORMAT_VERSION: u32 = 1;
const BACKUP_MANIFEST_RETENTION_LIMIT: usize = 1;
const BACKUP_GC_GRACE_PERIOD_MILLIS: u64 = 60_000;
const LOCAL_BACKUP_RESTORE_MARKER_RELATIVE_PATH: &str = "backup/RESTORE_INCOMPLETE";
const LEVELED_BASE_LEVEL_TARGET_BYTES: u64 = 4 * 1024;
const LEVELED_LEVEL_SIZE_MULTIPLIER: u64 = 10;
const LEVELED_L0_COMPACTION_TRIGGER: usize = 2;
const TIERED_LEVEL_RUN_COMPACTION_TRIGGER: usize = 3;
const FIFO_MAX_LIVE_SSTABLES: usize = 2;
const ROW_SSTABLE_FORMAT_VERSION: u32 = 1;
const COLUMNAR_SSTABLE_FORMAT_VERSION: u32 = 1;
const COLUMNAR_SSTABLE_MAGIC: &[u8; 8] = b"TDBCOL1\n";
const MVCC_KEY_SEPARATOR: u8 = 0;
const DEFAULT_MAX_MERGE_OPERAND_CHAIN_LENGTH: usize = 8;
const MAX_SCHEDULER_DEFER_CYCLES: u32 = 3;

#[derive(Clone)]
pub struct Db {
    inner: Arc<DbInner>,
}

struct DbInner {
    config: DbConfig,
    scheduler: Arc<dyn Scheduler>,
    dependencies: DbDependencies,
    catalog_location: CatalogLocation,
    catalog_write_lock: AsyncMutex<()>,
    commit_lock: AsyncMutex<()>,
    maintenance_lock: AsyncMutex<()>,
    backup_lock: AsyncMutex<()>,
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
    compaction_filter_stats: Mutex<BTreeMap<TableId, CompactionFilterStats>>,
    visible_watchers: Arc<WatermarkRegistry>,
    durable_watchers: Arc<WatermarkRegistry>,
    work_deferrals: Mutex<BTreeMap<String, u32>>,
    #[cfg(test)]
    commit_phase_blocks: Mutex<Vec<CommitPhaseBlock>>,
    #[cfg(test)]
    compaction_phase_blocks: Mutex<Vec<CompactionPhaseBlock>>,
    #[cfg(test)]
    offload_phase_blocks: Mutex<Vec<OffloadPhaseBlock>>,
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
    columnar: Option<ResidentColumnarSstable>,
}

#[derive(Clone, Debug)]
struct ResidentColumnarSstable {
    source: StorageSource,
}

#[derive(Clone, Debug)]
struct ColumnProjection {
    fields: Vec<FieldDefinition>,
}

#[derive(Clone, Debug)]
struct ColumnarRowRef {
    local_id: String,
    key: Key,
    sequence: SequenceNumber,
    kind: ChangeKind,
    row_index: usize,
}

#[derive(Clone, Debug)]
struct LoadedColumnarMetadata {
    footer: PersistedColumnarSstableFooter,
    footer_start: usize,
    key_index: Vec<Key>,
    sequences: Vec<SequenceNumber>,
    tombstones: Vec<bool>,
    row_kinds: Vec<ChangeKind>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedManifestSstable {
    table_id: TableId,
    level: u32,
    local_id: String,
    file_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    remote_key: Option<String>,
    length: u64,
    checksum: u32,
    data_checksum: u32,
    min_key: Key,
    max_key: Key,
    min_sequence: SequenceNumber,
    max_sequence: SequenceNumber,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    schema_version: Option<u32>,
}

impl PersistedManifestSstable {
    fn storage_source(&self) -> StorageSource {
        if !self.file_path.is_empty() {
            StorageSource::local_file(self.file_path.clone())
        } else if let Some(remote_key) = &self.remote_key {
            StorageSource::remote_object(remote_key.clone())
        } else {
            StorageSource::local_file(self.file_path.clone())
        }
    }

    fn storage_descriptor(&self) -> &str {
        if !self.file_path.is_empty() {
            &self.file_path
        } else {
            self.remote_key.as_deref().unwrap_or("")
        }
    }
}

#[derive(Clone, Debug, Default)]
struct LoadedManifest {
    generation: ManifestId,
    last_flushed_sequence: SequenceNumber,
    live_sstables: Vec<ResidentRowSstable>,
    durable_commit_log_segments: Vec<DurableRemoteCommitLogSegment>,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DurableRemoteCommitLogSegment {
    object_key: String,
    footer: SegmentFooter,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedRemoteManifestBody {
    format_version: u32,
    generation: ManifestId,
    last_flushed_sequence: SequenceNumber,
    sstables: Vec<PersistedManifestSstable>,
    commit_log_segments: Vec<DurableRemoteCommitLogSegment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedRemoteManifestFile {
    body: PersistedRemoteManifestBody,
    checksum: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BackupObjectBirthRecord {
    format_version: u32,
    object_key: String,
    first_uploaded_at_millis: u64,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ColumnEncoding {
    Plain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ColumnCompression {
    None,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ColumnarBlockLocation {
    offset: u64,
    length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedColumnarColumnFooter {
    field_id: FieldId,
    #[serde(rename = "type")]
    field_type: FieldType,
    encoding: ColumnEncoding,
    compression: ColumnCompression,
    block: ColumnarBlockLocation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedColumnarSstableFooter {
    format_version: u32,
    table_id: TableId,
    level: u32,
    local_id: String,
    row_count: u64,
    min_key: Key,
    max_key: Key,
    min_sequence: SequenceNumber,
    max_sequence: SequenceNumber,
    schema_version: u32,
    data_checksum: u32,
    key_index: ColumnarBlockLocation,
    sequence_column: ColumnarBlockLocation,
    tombstone_bitmap: ColumnarBlockLocation,
    row_kind_column: ColumnarBlockLocation,
    columns: Vec<PersistedColumnarColumnFooter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user_key_bloom_filter: Option<UserKeyBloomFilter>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PersistedNullableColumn<T> {
    present_bitmap: Vec<bool>,
    values: Vec<T>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PersistedFloat64Column {
    present_bitmap: Vec<bool>,
    values_bits: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum PersistedColumnBlock {
    Int64(PersistedNullableColumn<i64>),
    Float64(PersistedFloat64Column),
    String(PersistedNullableColumn<String>),
    Bytes(PersistedNullableColumn<Vec<u8>>),
    Bool(PersistedNullableColumn<bool>),
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

#[derive(Clone, Debug)]
struct OffloadJob {
    id: String,
    table_id: TableId,
    table_name: String,
    input_local_ids: Vec<String>,
    estimated_bytes: u64,
}

#[derive(Clone, Debug, Default)]
struct TableCompactionState {
    compaction_debt: u64,
    next_job: Option<CompactionJob>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CompactionFilterStats {
    removed_bytes: u64,
    removed_keys: u64,
}

#[derive(Clone, Debug)]
enum PendingWorkSpec {
    Flush,
    Compaction(CompactionJob),
    Offload(OffloadJob),
}

#[derive(Clone, Debug)]
struct PendingWorkCandidate {
    pending: PendingWork,
    spec: PendingWorkSpec,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SstableRow {
    key: Key,
    sequence: SequenceNumber,
    kind: ChangeKind,
    value: Option<Value>,
}

#[derive(Clone, Debug)]
struct CompactionRow {
    level: u32,
    row: SstableRow,
}

#[derive(Clone, Debug, Default)]
struct FilteredCompactionRows {
    rows: Vec<CompactionRow>,
    removed_bytes: u64,
    removed_keys: u64,
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

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OffloadPhase {
    UploadComplete,
    ManifestSwitched,
    LocalCleanupFinished,
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
struct OffloadPhaseBlock {
    phase: OffloadPhase,
    reached_tx: Option<oneshot::Sender<()>>,
    release_rx: Option<oneshot::Receiver<()>>,
}

#[cfg(test)]
struct CompactionPhaseBlocker {
    reached_rx: Option<oneshot::Receiver<()>>,
    release_tx: Option<oneshot::Sender<()>>,
}

#[cfg(test)]
#[allow(dead_code)]
struct OffloadPhaseBlocker {
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

#[cfg(test)]
#[allow(dead_code)]
impl OffloadPhaseBlocker {
    async fn wait_until_reached(&mut self) {
        self.reached_rx
            .take()
            .expect("offload phase blocker should have a receiver")
            .await
            .expect("offload phase blocker should observe the phase");
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

struct MemoryCommitLog {
    object_store: Arc<dyn ObjectStore>,
    records: Vec<CommitRecord>,
    durable_commit_log_segments: Vec<DurableRemoteCommitLogSegment>,
    next_segment_id: u64,
}

impl MemoryCommitLog {
    fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            records: Vec::new(),
            durable_commit_log_segments: Vec::new(),
            next_segment_id: 1,
        }
    }
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

    async fn append_group_batch(&mut self, records: &[CommitRecord]) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(manager) => manager.append_batch_and_sync(records).await,
            CommitLogBackend::Memory(log) => {
                log.records.extend(records.iter().cloned());
                Ok(())
            }
        }
    }

    async fn sync(&mut self) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(manager) => manager.sync_active().await,
            CommitLogBackend::Memory(_) => Ok(()),
        }
    }

    async fn maybe_seal_active(&mut self) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(manager) => {
                manager.seal_active().await?;
                Ok(())
            }
            CommitLogBackend::Memory(_) => Ok(()),
        }
    }

    async fn prune_segments_before(
        &mut self,
        sequence_exclusive: SequenceNumber,
    ) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(manager) => {
                manager.prune_sealed_before(sequence_exclusive).await?;
                Ok(())
            }
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

    async fn scan_table_from_sequence(
        &self,
        table_id: TableId,
        sequence_inclusive: SequenceNumber,
    ) -> Result<Vec<CommitRecord>, StorageError> {
        match &self.backend {
            CommitLogBackend::Local(manager) => {
                manager
                    .scan_table_from_sequence(table_id, sequence_inclusive)
                    .await
            }
            CommitLogBackend::Memory(log) => {
                let mut records = Vec::new();
                for segment in &log.durable_commit_log_segments {
                    let Some(table_meta) = segment
                        .footer
                        .tables
                        .iter()
                        .find(|table| table.table_id == table_id)
                    else {
                        continue;
                    };
                    if table_meta.max_sequence < sequence_inclusive {
                        continue;
                    }

                    let bytes = log.object_store.get(&segment.object_key).await?;
                    records.extend(scan_table_from_segment_bytes(
                        &bytes,
                        table_id,
                        sequence_inclusive,
                    )?);
                }
                records.extend(
                    log.records
                        .iter()
                        .filter(|record| record.sequence() >= sequence_inclusive)
                        .filter(|record| {
                            record
                                .entries
                                .iter()
                                .any(|entry| entry.table_id == table_id)
                        })
                        .cloned(),
                );
                Ok(records)
            }
        }
    }

    fn oldest_sequence_for_table(&self, table_id: TableId) -> Option<SequenceNumber> {
        match &self.backend {
            CommitLogBackend::Local(manager) => manager.oldest_sequence_for_table(table_id),
            CommitLogBackend::Memory(log) => {
                let durable_oldest = log
                    .durable_commit_log_segments
                    .iter()
                    .flat_map(|segment| segment.footer.tables.iter())
                    .filter(|table| table.table_id == table_id)
                    .map(|table| table.min_sequence)
                    .min();
                let buffered_oldest = log
                    .records
                    .iter()
                    .filter(|record| {
                        record
                            .entries
                            .iter()
                            .any(|entry| entry.table_id == table_id)
                    })
                    .map(CommitRecord::sequence)
                    .min();

                durable_oldest.into_iter().chain(buffered_oldest).min()
            }
        }
    }

    fn oldest_segment_id(&self) -> Option<SegmentId> {
        match &self.backend {
            CommitLogBackend::Local(manager) => manager.oldest_segment_id(),
            CommitLogBackend::Memory(log) => log
                .durable_commit_log_segments
                .first()
                .map(|segment| segment.footer.segment_id)
                .or_else(|| {
                    (!log.records.is_empty()).then_some(SegmentId::new(log.next_segment_id))
                }),
        }
    }

    #[cfg(test)]
    fn enumerate_segments(&self) -> Vec<crate::engine::commit_log::SegmentDescriptor> {
        match &self.backend {
            CommitLogBackend::Local(manager) => manager.enumerate_segments(),
            CommitLogBackend::Memory(log) => {
                let mut descriptors = log
                    .durable_commit_log_segments
                    .iter()
                    .map(|segment| {
                        crate::engine::commit_log::SegmentDescriptor::from(&segment.footer)
                    })
                    .collect::<Vec<_>>();
                if !log.records.is_empty() {
                    let mut tables =
                        BTreeMap::<TableId, (SequenceNumber, SequenceNumber, u32)>::new();
                    for record in &log.records {
                        for entry in &record.entries {
                            tables
                                .entry(entry.table_id)
                                .and_modify(|table| {
                                    table.0 = table.0.min(record.sequence());
                                    table.1 = table.1.max(record.sequence());
                                    table.2 = table.2.saturating_add(1);
                                })
                                .or_insert((record.sequence(), record.sequence(), 1));
                        }
                    }
                    descriptors.push(crate::engine::commit_log::SegmentDescriptor {
                        segment_id: SegmentId::new(log.next_segment_id),
                        sealed: false,
                        min_sequence: log.records.first().map(CommitRecord::sequence),
                        max_sequence: log.records.last().map(CommitRecord::sequence),
                        record_count: log.records.len() as u64,
                        entry_count: log
                            .records
                            .iter()
                            .map(|record| record.entries.len() as u64)
                            .sum(),
                        tables: tables
                            .into_iter()
                            .map(|(table_id, (min_sequence, max_sequence, entry_count))| {
                                crate::engine::commit_log::TableSegmentMeta {
                                    table_id,
                                    min_sequence,
                                    max_sequence,
                                    entry_count,
                                }
                            })
                            .collect(),
                    });
                }
                descriptors
            }
        }
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
    records: Vec<CommitRecord>,
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

    fn stage_record(&self, record: CommitRecord) {
        mutex_lock(&self.state).records.push(record);
    }

    fn staged_records(&self) -> Vec<CommitRecord> {
        mutex_lock(&self.state).records.clone()
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

#[derive(Debug)]
struct WatermarkSubscription {
    registry: Weak<WatermarkRegistry>,
    table_name: String,
}

impl WatermarkSubscription {
    fn new(registry: &Arc<WatermarkRegistry>, table_name: &str) -> Self {
        Self {
            registry: Arc::downgrade(registry),
            table_name: table_name.to_string(),
        }
    }
}

impl Clone for WatermarkSubscription {
    fn clone(&self) -> Self {
        if let Some(registry) = self.registry.upgrade() {
            registry.increment(&self.table_name);
        }

        Self {
            registry: self.registry.clone(),
            table_name: self.table_name.clone(),
        }
    }
}

impl Drop for WatermarkSubscription {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.decrement(&self.table_name);
        }
    }
}

#[derive(Clone, Debug)]
struct WatermarkTableState {
    current: SequenceNumber,
    sender: Option<watch::Sender<SequenceNumber>>,
    subscribers: usize,
}

impl WatermarkTableState {
    fn new(current: SequenceNumber) -> Self {
        Self {
            current,
            sender: None,
            subscribers: 0,
        }
    }
}

#[derive(Debug)]
struct WatermarkRegistry {
    tables: Mutex<BTreeMap<String, WatermarkTableState>>,
    pulse: watch::Sender<u64>,
}

impl WatermarkRegistry {
    fn new(initial: BTreeMap<String, SequenceNumber>) -> Self {
        let (pulse, _receiver) = watch::channel(0);
        let tables = initial
            .into_iter()
            .map(|(table, sequence)| (table, WatermarkTableState::new(sequence)))
            .collect();
        Self {
            tables: Mutex::new(tables),
            pulse,
        }
    }

    fn subscribe(self: &Arc<Self>, table_name: &str) -> WatermarkReceiver {
        let receiver = {
            let mut tables = mutex_lock(&self.tables);
            let state = tables
                .entry(table_name.to_string())
                .or_insert_with(|| WatermarkTableState::new(SequenceNumber::default()));
            state.subscribers += 1;
            let current = state.current;
            let sender = state.sender.get_or_insert_with(|| {
                let (sender, _receiver) = watch::channel(current);
                sender
            });
            sender.subscribe()
        };

        WatermarkReceiver::new(self, table_name, receiver)
    }

    fn increment(&self, table_name: &str) {
        if let Some(state) = mutex_lock(&self.tables).get_mut(table_name) {
            state.subscribers += 1;
        }
    }

    fn decrement(&self, table_name: &str) {
        if let Some(state) = mutex_lock(&self.tables).get_mut(table_name) {
            state.subscribers = state.subscribers.saturating_sub(1);
            if state.subscribers == 0 {
                state.sender = None;
            }
        }
    }

    fn notify(&self, updates: &BTreeMap<String, SequenceNumber>) {
        if updates.is_empty() {
            return;
        }

        let mut advanced = false;
        let mut tables = mutex_lock(&self.tables);
        for (table, sequence) in updates {
            let state = tables
                .entry(table.clone())
                .or_insert_with(|| WatermarkTableState::new(SequenceNumber::default()));
            if *sequence <= state.current {
                continue;
            }

            state.current = *sequence;
            if let Some(sender) = &state.sender {
                sender.send_replace(*sequence);
                advanced = true;
            }
        }
        drop(tables);

        if advanced {
            let next = (*self.pulse.borrow()).wrapping_add(1);
            self.pulse.send_replace(next);
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn pulse(&self) -> watch::Receiver<u64> {
        self.pulse.subscribe()
    }

    #[cfg(test)]
    fn active_subscriber_count(&self, table_name: &str) -> usize {
        mutex_lock(&self.tables)
            .get(table_name)
            .map(|state| state.subscribers)
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatermarkUpdate {
    pub table: String,
    pub sequence: SequenceNumber,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
struct NamedWatermarkReceiver {
    table: String,
    receiver: WatermarkReceiver,
}

#[derive(Debug)]
pub struct WatermarkSubscriptionSet {
    pulse: watch::Receiver<u64>,
    receivers: Vec<NamedWatermarkReceiver>,
    observed: Vec<SequenceNumber>,
}

impl WatermarkSubscriptionSet {
    fn new(registry: &Arc<WatermarkRegistry>, receivers: Vec<(String, WatermarkReceiver)>) -> Self {
        let mut receivers = receivers;
        receivers.sort_by(|(left, _), (right, _)| left.cmp(right));

        let observed = receivers
            .iter()
            .map(|(_, receiver)| receiver.current())
            .collect();

        Self {
            pulse: registry.pulse(),
            receivers: receivers
                .into_iter()
                .map(|(table, receiver)| NamedWatermarkReceiver { table, receiver })
                .collect(),
            observed,
        }
    }

    pub fn drain_pending(&mut self) -> Vec<WatermarkUpdate> {
        let mut pending = Vec::new();
        for (observed, receiver) in self.observed.iter_mut().zip(self.receivers.iter_mut()) {
            let current = receiver.receiver.current();
            if current <= *observed {
                continue;
            }

            *observed = current;
            pending.push(WatermarkUpdate {
                table: receiver.table.clone(),
                sequence: current,
            });
        }

        if !pending.is_empty() {
            let _ = *self.pulse.borrow_and_update();
        }

        pending
    }

    pub async fn changed(&mut self) -> Result<Vec<WatermarkUpdate>, SubscriptionClosed> {
        loop {
            let pending = self.drain_pending();
            if !pending.is_empty() {
                return Ok(pending);
            }

            self.pulse.changed().await.map_err(|_| SubscriptionClosed)?;
        }
    }
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

    fn pending_flush_bytes_by_table(&self) -> BTreeMap<TableId, u64> {
        self.tables
            .iter()
            .filter_map(|(&table_id, table)| {
                (table.pending_flush_bytes > 0).then_some((table_id, table.pending_flush_bytes))
            })
            .collect()
    }

    fn total_pending_flush_bytes(&self) -> u64 {
        self.tables
            .values()
            .map(|table| table.pending_flush_bytes)
            .sum()
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

    fn record_table_watermarks(&self, watermarks: &mut BTreeMap<TableId, SequenceNumber>) {
        for (&table_id, table) in &self.tables {
            let Some(sequence) = table.entries.values().map(|entry| entry.sequence).max() else {
                continue;
            };
            update_table_watermark(watermarks, table_id, sequence);
        }
    }
}

impl ResidentRowSstable {
    fn is_columnar(&self) -> bool {
        self.columnar.is_some()
    }

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

    async fn load_columnar_metadata(
        &self,
        dependencies: &DbDependencies,
    ) -> Result<LoadedColumnarMetadata, StorageError> {
        let columnar = self.columnar.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} is missing a storage source",
                self.meta.storage_descriptor()
            ))
        })?;
        let location = self.meta.storage_descriptor();
        let (footer, footer_start) = Db::columnar_footer_from_source(
            dependencies,
            &columnar.source,
            self.meta.length,
            location,
        )
        .await?;
        Db::validate_loaded_columnar_footer(location, &self.meta, &footer)?;

        let row_count = usize::try_from(footer.row_count).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} row count exceeds platform limits"
            ))
        })?;
        let key_index_range =
            Db::columnar_block_range(location, footer_start, "key index", &footer.key_index)?;
        let sequence_range = Db::columnar_block_range(
            location,
            footer_start,
            "sequence column",
            &footer.sequence_column,
        )?;
        let tombstone_range = Db::columnar_block_range(
            location,
            footer_start,
            "tombstone bitmap",
            &footer.tombstone_bitmap,
        )?;
        let row_kind_range = Db::columnar_block_range(
            location,
            footer_start,
            "row-kind column",
            &footer.row_kind_column,
        )?;

        let key_index: Vec<Key> = serde_json::from_slice(
            &Db::read_exact_source_range(
                dependencies,
                &columnar.source,
                key_index_range,
                location,
                "key index",
            )
            .await?,
        )
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar key index for {location} failed: {error}"
            ))
        })?;
        let sequences: Vec<SequenceNumber> = serde_json::from_slice(
            &Db::read_exact_source_range(
                dependencies,
                &columnar.source,
                sequence_range,
                location,
                "sequence column",
            )
            .await?,
        )
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar sequence column for {location} failed: {error}"
            ))
        })?;
        let tombstones: Vec<bool> = serde_json::from_slice(
            &Db::read_exact_source_range(
                dependencies,
                &columnar.source,
                tombstone_range,
                location,
                "tombstone bitmap",
            )
            .await?,
        )
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar tombstone bitmap for {location} failed: {error}"
            ))
        })?;
        let row_kinds: Vec<ChangeKind> = serde_json::from_slice(
            &Db::read_exact_source_range(
                dependencies,
                &columnar.source,
                row_kind_range,
                location,
                "row-kind column",
            )
            .await?,
        )
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar row-kind column for {location} failed: {error}"
            ))
        })?;

        if key_index.len() != row_count
            || sequences.len() != row_count
            || tombstones.len() != row_count
            || row_kinds.len() != row_count
        {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} contains inconsistent block lengths",
            )));
        }

        Ok(LoadedColumnarMetadata {
            footer,
            footer_start,
            key_index,
            sequences,
            tombstones,
            row_kinds,
        })
    }

    async fn collect_visible_row_refs_for_key_columnar(
        &self,
        dependencies: &DbDependencies,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Vec<ColumnarRowRef>, StorageError> {
        if key < self.meta.min_key.as_slice()
            || key > self.meta.max_key.as_slice()
            || sequence < self.meta.min_sequence
        {
            return Ok(Vec::new());
        }
        if let Some(filter) = &self.user_key_bloom_filter
            && !filter.may_contain(key)
        {
            return Ok(Vec::new());
        }

        let metadata = self.load_columnar_metadata(dependencies).await?;
        let start = metadata
            .key_index
            .partition_point(|candidate| candidate.as_slice() < key);
        let mut rows = Vec::new();

        for row_index in start..metadata.key_index.len() {
            match metadata.key_index[row_index].as_slice().cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    if metadata.sequences[row_index] > sequence {
                        continue;
                    }
                    rows.push(ColumnarRowRef {
                        local_id: self.meta.local_id.clone(),
                        key: metadata.key_index[row_index].clone(),
                        sequence: metadata.sequences[row_index],
                        kind: Db::normalized_columnar_row_kind(
                            self.meta.storage_descriptor(),
                            row_index,
                            metadata.tombstones[row_index],
                            metadata.row_kinds[row_index],
                        )?,
                        row_index,
                    });
                }
            }
        }

        Ok(rows)
    }

    async fn collect_scan_row_refs_columnar(
        &self,
        dependencies: &DbDependencies,
        matcher: &KeyMatcher<'_>,
        sequence: SequenceNumber,
    ) -> Result<Vec<ColumnarRowRef>, StorageError> {
        if sequence < self.meta.min_sequence {
            return Ok(Vec::new());
        }

        let metadata = self.load_columnar_metadata(dependencies).await?;
        let start = match matcher {
            KeyMatcher::Range { start, .. } | KeyMatcher::Prefix(start) => metadata
                .key_index
                .partition_point(|key| key.as_slice() < *start),
        };
        let mut rows = Vec::new();

        for row_index in start..metadata.key_index.len() {
            let key = &metadata.key_index[row_index];
            if !matcher.matches(key) {
                if matcher.is_past_end(key) {
                    break;
                }
                continue;
            }
            if metadata.sequences[row_index] > sequence {
                continue;
            }
            rows.push(ColumnarRowRef {
                local_id: self.meta.local_id.clone(),
                key: key.clone(),
                sequence: metadata.sequences[row_index],
                kind: Db::normalized_columnar_row_kind(
                    self.meta.storage_descriptor(),
                    row_index,
                    metadata.tombstones[row_index],
                    metadata.row_kinds[row_index],
                )?,
                row_index,
            });
        }

        Ok(rows)
    }

    async fn materialize_columnar_rows(
        &self,
        dependencies: &DbDependencies,
        projection: &ColumnProjection,
        row_indexes: &BTreeSet<usize>,
    ) -> Result<BTreeMap<usize, Value>, StorageError> {
        if row_indexes.is_empty() {
            return Ok(BTreeMap::new());
        }

        let columnar = self.columnar.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} is missing a storage source",
                self.meta.storage_descriptor()
            ))
        })?;
        let metadata = self.load_columnar_metadata(dependencies).await?;
        let location = self.meta.storage_descriptor();
        let row_count = metadata.key_index.len();
        let columns_by_field = metadata
            .footer
            .columns
            .iter()
            .map(|column| (column.field_id, column))
            .collect::<BTreeMap<_, _>>();
        let mut values_by_field = BTreeMap::<FieldId, Vec<FieldValue>>::new();

        for field in &projection.fields {
            if let Some(column) = columns_by_field.get(&field.id) {
                if column.field_type != field.field_type {
                    return Err(StorageError::corruption(format!(
                        "columnar SSTable {location} field {} type metadata does not match schema",
                        field.id.get()
                    )));
                }
                let range = Db::columnar_block_range(
                    location,
                    metadata.footer_start,
                    "column block",
                    &column.block,
                )?;
                let block = Db::read_exact_source_range(
                    dependencies,
                    &columnar.source,
                    range,
                    location,
                    "column block",
                )
                .await?;
                values_by_field.insert(
                    field.id,
                    Db::decode_columnar_field_values(location, column, row_count, &block)?,
                );
            }
        }

        let mut materialized = BTreeMap::new();
        for &row_index in row_indexes {
            let row_kind = Db::normalized_columnar_row_kind(
                location,
                row_index,
                metadata.tombstones[row_index],
                metadata.row_kinds[row_index],
            )?;
            if row_kind == ChangeKind::Delete {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} delete row {row_index} was requested for materialization",
                )));
            }
            let mut record = ColumnarRecord::new();
            for field in &projection.fields {
                let value = match values_by_field.get(&field.id) {
                    Some(values) => values[row_index].clone(),
                    None => Db::missing_columnar_projection_value(field, row_kind)?,
                };
                record.insert(field.id, value);
            }
            materialized.insert(row_index, Value::Record(record));
        }

        Ok(materialized)
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
        let total_bytes = matching
            .iter()
            .map(|sstable| sstable.meta.length)
            .sum::<u64>();
        let local_bytes = matching
            .iter()
            .filter(|sstable| !sstable.meta.file_path.is_empty())
            .map(|sstable| sstable.meta.length)
            .sum::<u64>();
        (l0_count, total_bytes, local_bytes)
    }

    fn table_watermarks(&self) -> BTreeMap<TableId, SequenceNumber> {
        let mut watermarks = BTreeMap::new();
        for sstable in &self.live {
            update_table_watermark(
                &mut watermarks,
                sstable.meta.table_id,
                sstable.meta.max_sequence,
            );
        }
        watermarks
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

    fn immutable_flush_backlog_by_table(&self) -> BTreeMap<TableId, u64> {
        let mut backlog = BTreeMap::new();
        for immutable in &self.immutables {
            for (table_id, bytes) in immutable.memtable.pending_flush_bytes_by_table() {
                backlog
                    .entry(table_id)
                    .and_modify(|current: &mut u64| *current = current.saturating_add(bytes))
                    .or_insert(bytes);
            }
        }
        backlog
    }

    fn total_pending_flush_bytes(&self) -> u64 {
        self.mutable.total_pending_flush_bytes().saturating_add(
            self.immutables
                .iter()
                .map(|immutable| immutable.memtable.total_pending_flush_bytes())
                .sum::<u64>(),
        )
    }

    fn immutable_memtable_count(&self, table_id: TableId) -> u32 {
        self.immutables
            .iter()
            .filter(|immutable| immutable.memtable.has_entries_for_table(table_id))
            .count() as u32
    }

    fn table_watermarks(&self) -> BTreeMap<TableId, SequenceNumber> {
        let mut watermarks = BTreeMap::new();
        self.mutable.record_table_watermarks(&mut watermarks);
        for immutable in &self.immutables {
            immutable.memtable.record_table_watermarks(&mut watermarks);
        }
        watermarks
    }
}

fn update_table_watermark(
    watermarks: &mut BTreeMap<TableId, SequenceNumber>,
    table_id: TableId,
    sequence: SequenceNumber,
) {
    watermarks
        .entry(table_id)
        .and_modify(|current| *current = (*current).max(sequence))
        .or_insert(sequence);
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
    pub async fn open(
        mut config: DbConfig,
        dependencies: DbDependencies,
    ) -> Result<Self, OpenError> {
        Self::validate_storage_config(&config.storage)?;
        let scheduler = config
            .scheduler
            .clone()
            .unwrap_or_else(|| Arc::new(RoundRobinScheduler::default()));
        config.scheduler = Some(scheduler.clone());
        if let StorageConfig::Tiered(tiered) = &config.storage {
            Self::maybe_restore_tiered_from_backup(tiered, &dependencies).await?;
        }
        let catalog_location = Self::catalog_location(&config.storage);
        let (tables, next_table_id) = Self::load_tables(&dependencies, &catalog_location).await?;
        let loaded_manifest = Self::load_local_manifest(&config.storage, &dependencies).await?;
        let mut commit_runtime = Self::open_commit_runtime(&config.storage, &dependencies).await?;
        if let CommitLogBackend::Memory(log) = &mut commit_runtime.backend {
            log.durable_commit_log_segments = loaded_manifest.durable_commit_log_segments.clone();
            log.next_segment_id = loaded_manifest
                .durable_commit_log_segments
                .iter()
                .map(|segment| segment.footer.segment_id.get())
                .max()
                .unwrap_or(0)
                .saturating_add(1)
                .max(1);
        }
        let recovered_commit_log = commit_runtime
            .recover_after(loaded_manifest.last_flushed_sequence)
            .await?;
        let recovered_sequence = recovered_commit_log
            .max_sequence
            .max(loaded_manifest.last_flushed_sequence);
        let next_sstable_id = loaded_manifest
            .live_sstables
            .iter()
            .filter_map(|sstable| Self::parse_sstable_local_id(&sstable.meta.local_id))
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(1);
        let memtables = recovered_commit_log.memtables;
        let sstables = SstableState {
            manifest_generation: loaded_manifest.generation,
            last_flushed_sequence: loaded_manifest.last_flushed_sequence,
            live: loaded_manifest.live_sstables,
        };
        let initial_table_watermarks =
            Self::initial_table_watermarks(&tables, &memtables, &sstables);

        let db = Self {
            inner: Arc::new(DbInner {
                config,
                scheduler,
                dependencies,
                catalog_location,
                catalog_write_lock: AsyncMutex::new(()),
                commit_lock: AsyncMutex::new(()),
                maintenance_lock: AsyncMutex::new(()),
                backup_lock: AsyncMutex::new(()),
                commit_runtime: AsyncMutex::new(commit_runtime),
                commit_coordinator: Mutex::new(CommitCoordinator::default()),
                next_table_id: AtomicU32::new(next_table_id),
                next_sequence: AtomicU64::new(recovered_sequence.get()),
                current_sequence: AtomicU64::new(recovered_sequence.get()),
                current_durable_sequence: AtomicU64::new(recovered_sequence.get()),
                tables: RwLock::new(tables),
                memtables: RwLock::new(memtables),
                sstables: RwLock::new(sstables),
                next_sstable_id: AtomicU64::new(next_sstable_id),
                snapshot_tracker: Mutex::new(SnapshotTracker::default()),
                next_snapshot_id: AtomicU64::new(0),
                compaction_filter_stats: Mutex::new(BTreeMap::new()),
                visible_watchers: Arc::new(WatermarkRegistry::new(
                    initial_table_watermarks.clone(),
                )),
                durable_watchers: Arc::new(WatermarkRegistry::new(initial_table_watermarks)),
                work_deferrals: Mutex::new(BTreeMap::new()),
                #[cfg(test)]
                commit_phase_blocks: Mutex::new(Vec::new()),
                #[cfg(test)]
                compaction_phase_blocks: Mutex::new(Vec::new()),
                #[cfg(test)]
                offload_phase_blocks: Mutex::new(Vec::new()),
            }),
        };
        db.prune_commit_log(true)
            .await
            .map_err(OpenError::Storage)?;

        Ok(db)
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
        match config.format {
            TableFormat::Row => {
                if config.schema.is_some() {
                    return Err(CreateTableError::InvalidConfig(
                        "row tables do not accept a schema".to_string(),
                    ));
                }
            }
            TableFormat::Columnar => {
                let schema = config.schema.as_ref().ok_or_else(|| {
                    CreateTableError::InvalidConfig("columnar tables require a schema".to_string())
                })?;
                schema.validate().map_err(|error| {
                    CreateTableError::InvalidConfig(format!(
                        "invalid columnar schema: {}",
                        error.message()
                    ))
                })?;
            }
        }

        Ok(())
    }

    fn same_persisted_table_config(left: &TableConfig, right: &TableConfig) -> bool {
        left.name == right.name
            && left.format == right.format
            && left.max_merge_operand_chain_length == right.max_merge_operand_chain_length
            && left.bloom_filter_bits_per_key == right.bloom_filter_bits_per_key
            && left.history_retention_sequences == right.history_retention_sequences
            && left.compaction_strategy == right.compaction_strategy
            && left.schema == right.schema
            && left.metadata == right.metadata
    }

    fn normalize_value_for_table(
        stored: &StoredTable,
        value: &Value,
    ) -> Result<Value, StorageError> {
        match stored.config.format {
            TableFormat::Row => match value {
                Value::Bytes(_) => Ok(value.clone()),
                Value::Record(_) => Err(StorageError::unsupported(format!(
                    "row table {} only accepts byte values",
                    stored.config.name
                ))),
            },
            TableFormat::Columnar => {
                let schema = stored.config.schema.as_ref().ok_or_else(|| {
                    StorageError::corruption(format!(
                        "columnar table {} is missing a schema",
                        stored.config.name
                    ))
                })?;
                match value {
                    Value::Record(record) => Ok(Value::Record(schema.normalize_record(record)?)),
                    Value::Bytes(_) => Err(StorageError::unsupported(format!(
                        "columnar table {} requires structured record values",
                        stored.config.name
                    ))),
                }
            }
        }
    }

    fn normalize_merge_operand_for_table(
        stored: &StoredTable,
        value: &Value,
    ) -> Result<Value, StorageError> {
        match stored.config.format {
            TableFormat::Row => match value {
                Value::Bytes(_) => Ok(value.clone()),
                Value::Record(_) => Err(StorageError::unsupported(format!(
                    "row table {} only accepts byte values",
                    stored.config.name
                ))),
            },
            TableFormat::Columnar => {
                let schema = stored.config.schema.as_ref().ok_or_else(|| {
                    StorageError::corruption(format!(
                        "columnar table {} is missing a schema",
                        stored.config.name
                    ))
                })?;
                match value {
                    Value::Record(record) => {
                        Ok(Value::Record(schema.normalize_merge_operand(record)?))
                    }
                    Value::Bytes(_) => Err(StorageError::unsupported(format!(
                        "columnar table {} requires structured record values",
                        stored.config.name
                    ))),
                }
            }
        }
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
            StorageConfig::S3Primary(_) => {
                CommitLogBackend::Memory(MemoryCommitLog::new(dependencies.object_store.clone()))
            }
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

    fn parse_segment_id(path: &str) -> Option<SegmentId> {
        let path_buf = PathBuf::from(path);
        let file_name = path_buf.file_name()?.to_str()?;
        let suffix = file_name.strip_prefix("SEG-")?;
        if suffix.is_empty() || suffix.contains('.') {
            return None;
        }
        suffix.parse::<u64>().ok().map(SegmentId::new)
    }

    fn local_commit_log_segment_path(root: &str, segment_id: SegmentId) -> String {
        Self::join_fs_path(
            root,
            &format!(
                "{LOCAL_COMMIT_LOG_RELATIVE_DIR}/SEG-{:06}",
                segment_id.get()
            ),
        )
    }

    fn backup_restore_marker_path(root: &str) -> String {
        Self::join_fs_path(root, LOCAL_BACKUP_RESTORE_MARKER_RELATIVE_PATH)
    }

    fn object_key_layout(location: &S3Location) -> ObjectKeyLayout {
        ObjectKeyLayout::new(location)
    }

    async fn load_local_manifest(
        storage: &StorageConfig,
        dependencies: &DbDependencies,
    ) -> Result<LoadedManifest, OpenError> {
        let StorageConfig::Tiered(config) = storage else {
            let StorageConfig::S3Primary(config) = storage else {
                return Ok(LoadedManifest::default());
            };
            return Self::load_remote_manifest(config, dependencies).await;
        };

        let current_path = Self::local_current_path(&config.ssd.path);
        let manifest_dir = Self::local_manifest_dir(&config.ssd.path);
        let mut candidates = Vec::new();
        let mut last_error = None;

        if let Some(pointer) = read_optional_path(dependencies, &current_path).await? {
            match String::from_utf8(pointer) {
                Ok(pointer) => {
                    let pointer = pointer.trim();
                    if !pointer.is_empty() {
                        candidates.push(Self::join_fs_path(
                            &config.ssd.path,
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

    fn remote_object_layout(config: &S3PrimaryStorageConfig) -> ObjectKeyLayout {
        Self::object_key_layout(&config.s3)
    }

    fn tiered_object_layout(config: &TieredStorageConfig) -> ObjectKeyLayout {
        Self::object_key_layout(&config.s3)
    }

    fn remote_manifest_path(config: &S3PrimaryStorageConfig, generation: ManifestId) -> String {
        Self::remote_object_layout(config).backup_manifest(generation)
    }

    fn remote_manifest_latest_key(config: &S3PrimaryStorageConfig) -> String {
        Self::remote_object_layout(config).backup_manifest_latest()
    }

    fn remote_commit_log_segment_key(
        config: &S3PrimaryStorageConfig,
        segment_id: crate::SegmentId,
    ) -> String {
        Self::remote_object_layout(config).backup_commit_log_segment(segment_id)
    }

    fn remote_sstable_key(
        config: &S3PrimaryStorageConfig,
        table_id: TableId,
        local_id: &str,
    ) -> String {
        Self::remote_object_layout(config).backup_sstable(table_id, 0, local_id)
    }

    async fn load_remote_manifest(
        config: &S3PrimaryStorageConfig,
        dependencies: &DbDependencies,
    ) -> Result<LoadedManifest, OpenError> {
        Self::load_remote_manifest_from_layout(&Self::remote_object_layout(config), dependencies)
            .await
    }

    async fn load_remote_manifest_from_layout(
        layout: &ObjectKeyLayout,
        dependencies: &DbDependencies,
    ) -> Result<LoadedManifest, OpenError> {
        let Some((key, file)) =
            Self::load_remote_manifest_file_from_layout(layout, dependencies).await?
        else {
            return Ok(LoadedManifest::default());
        };
        Self::loaded_manifest_from_remote_file(dependencies, &key, file)
            .await
            .map_err(OpenError::Storage)
    }

    async fn load_remote_manifest_file_from_layout(
        layout: &ObjectKeyLayout,
        dependencies: &DbDependencies,
    ) -> Result<Option<(String, PersistedRemoteManifestFile)>, OpenError> {
        let latest_key = layout.backup_manifest_latest();
        let manifest_prefix = layout.backup_manifest_prefix();
        let mut candidates = Vec::new();
        let mut last_error = None;

        match dependencies.object_store.get(&latest_key).await {
            Ok(pointer) => match String::from_utf8(pointer) {
                Ok(pointer) => {
                    let pointer = pointer.trim();
                    if !pointer.is_empty() {
                        candidates.push(pointer.to_string());
                    }
                }
                Err(error) => {
                    last_error = Some(StorageError::corruption(format!(
                        "decode remote latest pointer failed: {error}"
                    )));
                }
            },
            Err(error) if error.kind() == StorageErrorKind::NotFound => {}
            Err(error) => last_error = Some(error),
        }

        let mut listed = dependencies.object_store.list(&manifest_prefix).await?;
        listed.retain(|key| key != &latest_key && Self::parse_manifest_generation(key).is_some());
        listed.sort_by_key(|key| {
            std::cmp::Reverse(
                Self::parse_manifest_generation(key)
                    .map(ManifestId::get)
                    .unwrap_or_default(),
            )
        });
        for key in listed {
            if !candidates.iter().any(|candidate| candidate == &key) {
                candidates.push(key);
            }
        }

        let mut saw_manifest = false;
        for key in candidates {
            saw_manifest = true;
            match Self::read_remote_manifest_file_at_key(dependencies, &key).await {
                Ok(file) => return Ok(Some((key, file))),
                Err(error) => last_error = Some(error),
            }
        }

        if saw_manifest {
            Err(OpenError::Storage(last_error.unwrap_or_else(|| {
                StorageError::corruption("no valid remote manifest generation found")
            })))
        } else {
            Ok(None)
        }
    }

    async fn read_manifest_at_path(
        dependencies: &DbDependencies,
        path: &str,
    ) -> Result<LoadedManifest, StorageError> {
        let bytes = read_source(dependencies, &StorageSource::local_file(path)).await?;
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
            durable_commit_log_segments: Vec::new(),
        })
    }

    async fn read_remote_manifest_file_at_key(
        dependencies: &DbDependencies,
        key: &str,
    ) -> Result<PersistedRemoteManifestFile, StorageError> {
        let bytes = read_source(dependencies, &StorageSource::remote_object(key)).await?;
        let file: PersistedRemoteManifestFile =
            serde_json::from_slice(&bytes).map_err(|error| {
                StorageError::corruption(format!("decode remote manifest {key} failed: {error}"))
            })?;

        if file.body.format_version != REMOTE_MANIFEST_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported remote manifest version {}",
                file.body.format_version
            )));
        }

        let encoded_body = serde_json::to_vec(&file.body).map_err(|error| {
            StorageError::corruption(format!("encode remote manifest body failed: {error}"))
        })?;
        if checksum32(&encoded_body) != file.checksum {
            return Err(StorageError::corruption(format!(
                "remote manifest checksum mismatch for {key}"
            )));
        }

        Ok(file)
    }

    async fn loaded_manifest_from_remote_file(
        dependencies: &DbDependencies,
        key: &str,
        file: PersistedRemoteManifestFile,
    ) -> Result<LoadedManifest, StorageError> {
        let mut live_sstables = Vec::with_capacity(file.body.sstables.len());
        let mut max_sstable_sequence = SequenceNumber::new(0);
        for sstable in &file.body.sstables {
            max_sstable_sequence = max_sstable_sequence.max(sstable.max_sequence);
            live_sstables.push(Self::load_resident_sstable(dependencies, sstable).await?);
        }

        let max_log_sequence = file
            .body
            .commit_log_segments
            .iter()
            .map(|segment| segment.footer.max_sequence)
            .max()
            .unwrap_or_default();

        if max_sstable_sequence.max(max_log_sequence) > file.body.last_flushed_sequence {
            return Err(StorageError::corruption(format!(
                "remote manifest {key} flush watermark is behind durable objects"
            )));
        }

        Ok(LoadedManifest {
            generation: file.body.generation,
            last_flushed_sequence: file.body.last_flushed_sequence,
            live_sstables,
            durable_commit_log_segments: file.body.commit_log_segments,
        })
    }

    async fn maybe_restore_tiered_from_backup(
        config: &TieredStorageConfig,
        dependencies: &DbDependencies,
    ) -> Result<(), OpenError> {
        let root = &config.ssd.path;
        let marker_path = Self::backup_restore_marker_path(root);
        let restore_incomplete = read_optional_path(dependencies, &marker_path)
            .await?
            .is_some();
        let has_local_catalog = read_optional_path(
            dependencies,
            &Self::join_fs_path(root, LOCAL_CATALOG_RELATIVE_PATH),
        )
        .await?
        .is_some();
        let has_local_current = read_optional_path(dependencies, &Self::local_current_path(root))
            .await?
            .is_some();
        let has_local_manifests = !dependencies
            .file_system
            .list(&Self::local_manifest_dir(root))
            .await?
            .is_empty();
        let has_local_commit_log = !dependencies
            .file_system
            .list(&Self::local_commit_log_dir(root))
            .await?
            .is_empty();

        if !restore_incomplete
            && (has_local_catalog
                || has_local_current
                || has_local_manifests
                || has_local_commit_log)
        {
            return Ok(());
        }

        let layout = Self::tiered_object_layout(config);
        let remote_catalog =
            read_optional_remote_object(dependencies, &layout.backup_catalog()).await?;
        let remote_manifest =
            Self::load_remote_manifest_file_from_layout(&layout, dependencies).await?;
        let mut remote_commit_log_keys = dependencies
            .object_store
            .list(&layout.backup_commit_log_prefix())
            .await?;

        let Some(catalog_bytes) = remote_catalog else {
            if remote_manifest.is_some() || !remote_commit_log_keys.is_empty() {
                return Err(OpenError::Storage(StorageError::corruption(
                    "backup recovery requires a replicated catalog",
                )));
            }
            return Ok(());
        };

        write_local_file_atomic(dependencies, &marker_path, b"incomplete backup restore\n").await?;
        write_local_file_atomic(
            dependencies,
            &Self::join_fs_path(root, LOCAL_CATALOG_RELATIVE_PATH),
            &catalog_bytes,
        )
        .await?;

        if let Some((_key, file)) = remote_manifest {
            for sstable in &file.body.sstables {
                let remote_key = sstable.remote_key.as_ref().ok_or_else(|| {
                    OpenError::Storage(StorageError::corruption(format!(
                        "remote manifest SSTable {} is missing a remote key",
                        sstable.local_id
                    )))
                })?;
                let local_path = if sstable.file_path.is_empty() {
                    Self::local_sstable_path(root, sstable.table_id, &sstable.local_id)
                } else {
                    sstable.file_path.clone()
                };
                let bytes = read_source(
                    dependencies,
                    &StorageSource::remote_object(remote_key.clone()),
                )
                .await?;
                write_local_file_atomic(dependencies, &local_path, &bytes).await?;
            }

            remote_commit_log_keys.extend(
                file.body
                    .commit_log_segments
                    .iter()
                    .map(|segment| segment.object_key.clone()),
            );
            let local_manifest_bytes = Self::encode_manifest_payload_from_sstables(
                file.body.generation,
                file.body.last_flushed_sequence,
                &file.body.sstables,
            )?;
            write_local_file_atomic(
                dependencies,
                &Self::local_manifest_path(root, file.body.generation),
                &local_manifest_bytes,
            )
            .await?;
            write_local_file_atomic(
                dependencies,
                &Self::local_current_path(root),
                format!("{}\n", Self::manifest_filename(file.body.generation)).as_bytes(),
            )
            .await?;
        }

        remote_commit_log_keys.sort();
        remote_commit_log_keys.dedup();
        for object_key in remote_commit_log_keys {
            let Some(segment_id) = Self::parse_segment_id(&object_key) else {
                continue;
            };
            let bytes =
                read_source(dependencies, &StorageSource::remote_object(object_key)).await?;
            write_local_file_atomic(
                dependencies,
                &Self::local_commit_log_segment_path(root, segment_id),
                &bytes,
            )
            .await?;
        }

        delete_local_file_if_exists(dependencies, &marker_path).await?;
        Ok(())
    }

    async fn load_resident_sstable(
        dependencies: &DbDependencies,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        if meta.schema_version.is_some() {
            Self::load_lazy_columnar_sstable(dependencies, meta).await
        } else {
            let source = meta.storage_source();
            let location = meta.storage_descriptor();
            let bytes = read_source(dependencies, &source).await?;
            if bytes.len() as u64 != meta.length {
                return Err(StorageError::corruption(format!(
                    "sstable {} length mismatch: manifest={}, file={}",
                    location,
                    meta.length,
                    bytes.len()
                )));
            }
            Self::decode_resident_row_sstable(location, meta, &bytes)
        }
    }

    async fn read_exact_source_range(
        dependencies: &DbDependencies,
        source: &StorageSource,
        range: std::ops::Range<u64>,
        location: &str,
        label: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let expected_len =
            usize::try_from(range.end.saturating_sub(range.start)).map_err(|_| {
                StorageError::corruption(format!(
                    "columnar SSTable {location} {label} length exceeds platform limits",
                ))
            })?;
        let bytes = UnifiedStorage::from_dependencies(dependencies)
            .read_range(source, range)
            .await
            .map_err(|error| error.into_storage_error())?;
        if bytes.len() != expected_len {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} is truncated",
            )));
        }
        Ok(bytes)
    }

    async fn columnar_footer_from_source(
        dependencies: &DbDependencies,
        source: &StorageSource,
        length: u64,
        location: &str,
    ) -> Result<(PersistedColumnarSstableFooter, usize), StorageError> {
        let min_len = (COLUMNAR_SSTABLE_MAGIC.len() * 2 + std::mem::size_of::<u64>()) as u64;
        if length < min_len {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is too short",
            )));
        }

        let trailer_start = length
            .saturating_sub((COLUMNAR_SSTABLE_MAGIC.len() + std::mem::size_of::<u64>()) as u64);
        let trailer = Self::read_exact_source_range(
            dependencies,
            source,
            trailer_start..length,
            location,
            "footer trailer",
        )
        .await?;
        let footer_len = u64::from_le_bytes(
            trailer[..std::mem::size_of::<u64>()]
                .try_into()
                .map_err(|_| {
                    StorageError::corruption(format!(
                        "columnar SSTable {location} footer length trailer is truncated",
                    ))
                })?,
        );
        if &trailer[std::mem::size_of::<u64>()..] != COLUMNAR_SSTABLE_MAGIC {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is missing the trailer magic",
            )));
        }

        let footer_start_u64 = trailer_start.checked_sub(footer_len).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {location} footer length points before the file start",
            ))
        })?;
        let footer_bytes = Self::read_exact_source_range(
            dependencies,
            source,
            footer_start_u64..trailer_start,
            location,
            "footer",
        )
        .await?;
        let footer = serde_json::from_slice(&footer_bytes).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar SSTable footer for {location} failed: {error}"
            ))
        })?;

        let footer_start = usize::try_from(footer_start_u64).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} footer offset exceeds platform limits",
            ))
        })?;
        Ok((footer, footer_start))
    }

    fn validate_loaded_columnar_footer(
        location: &str,
        meta: &PersistedManifestSstable,
        footer: &PersistedColumnarSstableFooter,
    ) -> Result<(), StorageError> {
        if footer.format_version != COLUMNAR_SSTABLE_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported columnar SSTable version {}",
                footer.format_version
            )));
        }
        if footer.table_id != meta.table_id
            || footer.level != meta.level
            || footer.local_id != meta.local_id
            || footer.min_key != meta.min_key
            || footer.max_key != meta.max_key
            || footer.min_sequence != meta.min_sequence
            || footer.max_sequence != meta.max_sequence
            || Some(footer.schema_version) != meta.schema_version
        {
            return Err(StorageError::corruption(format!(
                "manifest metadata does not match SSTable {location}",
            )));
        }
        Ok(())
    }

    async fn load_lazy_columnar_sstable(
        dependencies: &DbDependencies,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        let source = meta.storage_source();
        let location = meta.storage_descriptor();
        let (footer, _) =
            Self::columnar_footer_from_source(dependencies, &source, meta.length, location).await?;
        Self::validate_loaded_columnar_footer(location, meta, &footer)?;

        Ok(ResidentRowSstable {
            meta: meta.clone(),
            rows: Vec::new(),
            user_key_bloom_filter: footer.user_key_bloom_filter.clone(),
            columnar: Some(ResidentColumnarSstable { source }),
        })
    }

    fn decode_resident_row_sstable(
        location: &str,
        meta: &PersistedManifestSstable,
        bytes: &[u8],
    ) -> Result<ResidentRowSstable, StorageError> {
        let file: PersistedRowSstableFile = serde_json::from_slice(bytes).map_err(|error| {
            StorageError::corruption(format!("decode SSTable {location} failed: {error}"))
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
                "SSTable checksum mismatch for {location}",
            )));
        }

        let rows_bytes = serde_json::to_vec(&file.body.rows).map_err(|error| {
            StorageError::corruption(format!("encode SSTable rows failed: {error}"))
        })?;
        if checksum32(&rows_bytes) != file.body.data_checksum
            || file.body.data_checksum != meta.data_checksum
        {
            return Err(StorageError::corruption(format!(
                "SSTable data checksum mismatch for {location}",
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
                "manifest metadata does not match SSTable {location}",
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
                "SSTable row summary does not match metadata for {location}",
            )));
        }

        Ok(ResidentRowSstable {
            meta: meta.clone(),
            rows: file.body.rows,
            user_key_bloom_filter: file.body.user_key_bloom_filter,
            columnar: None,
        })
    }

    #[allow(dead_code)]
    fn decode_resident_columnar_sstable(
        location: &str,
        meta: &PersistedManifestSstable,
        bytes: &[u8],
    ) -> Result<ResidentRowSstable, StorageError> {
        if checksum32(bytes) != meta.checksum {
            return Err(StorageError::corruption(format!(
                "SSTable checksum mismatch for {location}",
            )));
        }

        let (footer, footer_start) = Self::columnar_footer_from_bytes(location, bytes)?;
        if footer.format_version != COLUMNAR_SSTABLE_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported columnar SSTable version {}",
                footer.format_version
            )));
        }

        if footer.table_id != meta.table_id
            || footer.level != meta.level
            || footer.local_id != meta.local_id
            || footer.min_key != meta.min_key
            || footer.max_key != meta.max_key
            || footer.min_sequence != meta.min_sequence
            || footer.max_sequence != meta.max_sequence
            || Some(footer.schema_version) != meta.schema_version
        {
            return Err(StorageError::corruption(format!(
                "manifest metadata does not match SSTable {location}",
            )));
        }

        let data_region = &bytes[COLUMNAR_SSTABLE_MAGIC.len()..footer_start];
        if checksum32(data_region) != footer.data_checksum
            || footer.data_checksum != meta.data_checksum
        {
            return Err(StorageError::corruption(format!(
                "SSTable data checksum mismatch for {location}",
            )));
        }

        let row_count = usize::try_from(footer.row_count).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} row count exceeds platform limits"
            ))
        })?;
        let key_index: Vec<Key> = serde_json::from_slice(Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "key index",
            &footer.key_index,
        )?)
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar key index for {location} failed: {error}"
            ))
        })?;
        let sequences: Vec<SequenceNumber> = serde_json::from_slice(Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "sequence column",
            &footer.sequence_column,
        )?)
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar sequence column for {location} failed: {error}"
            ))
        })?;
        let tombstones: Vec<bool> = serde_json::from_slice(Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "tombstone bitmap",
            &footer.tombstone_bitmap,
        )?)
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar tombstone bitmap for {location} failed: {error}"
            ))
        })?;
        let row_kinds: Vec<ChangeKind> = serde_json::from_slice(Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "row-kind column",
            &footer.row_kind_column,
        )?)
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar row-kind column for {location} failed: {error}"
            ))
        })?;

        if key_index.len() != row_count
            || sequences.len() != row_count
            || tombstones.len() != row_count
            || row_kinds.len() != row_count
        {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} contains inconsistent block lengths",
            )));
        }

        let mut values_by_field = BTreeMap::<FieldId, Vec<FieldValue>>::new();
        let mut seen_field_ids = BTreeSet::new();
        for column in &footer.columns {
            if !seen_field_ids.insert(column.field_id) {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} contains duplicate field id {}",
                    column.field_id.get()
                )));
            }
            let block = Self::columnar_block_slice(
                location,
                bytes,
                footer_start,
                "column block",
                &column.block,
            )?;
            values_by_field.insert(
                column.field_id,
                Self::decode_columnar_field_values(location, column, row_count, block)?,
            );
        }

        let mut rows = Vec::with_capacity(row_count);
        for row_index in 0..row_count {
            let kind = row_kinds[row_index];
            let tombstone = tombstones[row_index];
            let value = if tombstone {
                if kind != ChangeKind::Delete {
                    return Err(StorageError::corruption(format!(
                        "columnar SSTable {location} marks non-delete row {} as a tombstone",
                        row_index
                    )));
                }
                None
            } else {
                if kind == ChangeKind::Delete {
                    return Err(StorageError::corruption(format!(
                        "columnar SSTable {location} stores delete row {} without a tombstone",
                        row_index
                    )));
                }

                let mut record = ColumnarRecord::new();
                for column in &footer.columns {
                    let values = values_by_field.get(&column.field_id).ok_or_else(|| {
                        StorageError::corruption(format!(
                            "columnar SSTable {location} omitted field {} during decode",
                            column.field_id.get()
                        ))
                    })?;
                    record.insert(column.field_id, values[row_index].clone());
                }
                Some(Value::Record(record))
            };

            rows.push(SstableRow {
                key: key_index[row_index].clone(),
                sequence: sequences[row_index],
                kind,
                value,
            });
        }

        let computed =
            Self::summarize_sstable_rows(meta.table_id, meta.level, &meta.local_id, &rows)?;
        if computed.min_key != meta.min_key
            || computed.max_key != meta.max_key
            || computed.min_sequence != meta.min_sequence
            || computed.max_sequence != meta.max_sequence
        {
            return Err(StorageError::corruption(format!(
                "SSTable row summary does not match metadata for {location}",
            )));
        }

        Ok(ResidentRowSstable {
            meta: meta.clone(),
            rows,
            user_key_bloom_filter: footer.user_key_bloom_filter,
            columnar: Some(ResidentColumnarSstable {
                source: meta.storage_source(),
            }),
        })
    }

    #[allow(dead_code)]
    fn columnar_footer_from_bytes(
        location: &str,
        bytes: &[u8],
    ) -> Result<(PersistedColumnarSstableFooter, usize), StorageError> {
        let min_len = COLUMNAR_SSTABLE_MAGIC.len() * 2 + std::mem::size_of::<u64>();
        if bytes.len() < min_len {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is too short",
            )));
        }
        if !bytes.starts_with(COLUMNAR_SSTABLE_MAGIC) {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is missing the header magic",
            )));
        }

        let footer_magic_offset = bytes.len() - COLUMNAR_SSTABLE_MAGIC.len();
        if &bytes[footer_magic_offset..] != COLUMNAR_SSTABLE_MAGIC {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is missing the trailer magic",
            )));
        }

        let footer_len_offset = footer_magic_offset - std::mem::size_of::<u64>();
        let footer_len = u64::from_le_bytes(
            bytes[footer_len_offset..footer_magic_offset]
                .try_into()
                .map_err(|_| {
                    StorageError::corruption(format!(
                        "columnar SSTable {location} footer length trailer is truncated",
                    ))
                })?,
        ) as usize;
        let footer_start = footer_len_offset.checked_sub(footer_len).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {location} footer length points before the file start",
            ))
        })?;
        let footer_bytes = &bytes[footer_start..footer_len_offset];
        let footer = serde_json::from_slice(footer_bytes).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar SSTable footer for {location} failed: {error}"
            ))
        })?;

        Ok((footer, footer_start))
    }

    #[allow(dead_code)]
    fn columnar_block_slice<'a>(
        location: &str,
        bytes: &'a [u8],
        footer_start: usize,
        block_name: &str,
        block: &ColumnarBlockLocation,
    ) -> Result<&'a [u8], StorageError> {
        let range = Self::columnar_block_range(location, footer_start, block_name, block)?;
        let start = usize::try_from(range.start).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} offset exceeds platform limits",
            ))
        })?;
        let end = usize::try_from(range.end).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} length exceeds platform limits",
            ))
        })?;
        Ok(&bytes[start..end])
    }

    fn columnar_block_range(
        location: &str,
        footer_start: usize,
        block_name: &str,
        block: &ColumnarBlockLocation,
    ) -> Result<std::ops::Range<u64>, StorageError> {
        let offset = usize::try_from(block.offset).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} offset exceeds platform limits",
            ))
        })?;
        let length = usize::try_from(block.length).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} length exceeds platform limits",
            ))
        })?;
        let end = offset.checked_add(length).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} range overflows",
            ))
        })?;
        if offset < COLUMNAR_SSTABLE_MAGIC.len() || end > footer_start {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} range is outside the data region",
            )));
        }

        Ok(block.offset..block.offset.saturating_add(block.length))
    }

    fn normalized_columnar_row_kind(
        location: &str,
        row_index: usize,
        tombstone: bool,
        kind: ChangeKind,
    ) -> Result<ChangeKind, StorageError> {
        if tombstone {
            if kind != ChangeKind::Delete {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} marks non-delete row {} as a tombstone",
                    row_index
                )));
            }
            return Ok(ChangeKind::Delete);
        }
        if kind == ChangeKind::Delete {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} stores delete row {} without a tombstone",
                row_index
            )));
        }

        Ok(kind)
    }

    fn decode_columnar_field_values(
        location: &str,
        column: &PersistedColumnarColumnFooter,
        row_count: usize,
        block: &[u8],
    ) -> Result<Vec<FieldValue>, StorageError> {
        let decoded: PersistedColumnBlock = serde_json::from_slice(block).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar field {} for {location} failed: {error}",
                column.field_id.get()
            ))
        })?;
        match (column.field_type, decoded) {
            (FieldType::Int64, PersistedColumnBlock::Int64(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values.values.into_iter().map(FieldValue::Int64).collect(),
                )
            }
            (FieldType::Float64, PersistedColumnBlock::Float64(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values
                        .values_bits
                        .into_iter()
                        .map(|bits| FieldValue::Float64(f64::from_bits(bits)))
                        .collect(),
                )
            }
            (FieldType::String, PersistedColumnBlock::String(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values.values.into_iter().map(FieldValue::String).collect(),
                )
            }
            (FieldType::Bytes, PersistedColumnBlock::Bytes(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values.values.into_iter().map(FieldValue::Bytes).collect(),
                )
            }
            (FieldType::Bool, PersistedColumnBlock::Bool(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values.values.into_iter().map(FieldValue::Bool).collect(),
                )
            }
            _ => Err(StorageError::corruption(format!(
                "columnar SSTable {location} field {} type metadata does not match its block",
                column.field_id.get()
            ))),
        }
    }

    fn decode_nullable_column_values(
        location: &str,
        column: &PersistedColumnarColumnFooter,
        row_count: usize,
        present_bitmap: &[bool],
        values: Vec<FieldValue>,
    ) -> Result<Vec<FieldValue>, StorageError> {
        if present_bitmap.len() != row_count {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} field {} bitmap length mismatch",
                column.field_id.get()
            )));
        }
        if present_bitmap.iter().filter(|present| **present).count() != values.len() {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} field {} present bitmap does not match its value count",
                column.field_id.get()
            )));
        }

        let mut decoded = Vec::with_capacity(row_count);
        let mut values = values.into_iter();
        for present in present_bitmap {
            decoded.push(if *present {
                values.next().ok_or_else(|| {
                    StorageError::corruption(format!(
                        "columnar SSTable {location} field {} ran out of values during decode",
                        column.field_id.get()
                    ))
                })?
            } else {
                FieldValue::Null
            });
        }
        if values.next().is_some() {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} field {} contains extra decoded values",
                column.field_id.get()
            )));
        }

        Ok(decoded)
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
            remote_key: None,
            length: 0,
            checksum: 0,
            data_checksum: 0,
            min_key,
            max_key,
            min_sequence,
            max_sequence,
            schema_version: None,
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
            let output = match stored.config.format {
                TableFormat::Row => {
                    self.write_row_sstable(
                        &path,
                        table_id,
                        0,
                        local_id,
                        rows,
                        stored.config.bloom_filter_bits_per_key,
                    )
                    .await
                }
                TableFormat::Columnar => {
                    self.write_columnar_sstable(&path, 0, local_id, &stored, rows)
                        .await
                }
            }
            .map_err(FlushError::Storage)?;
            outputs.push(output);
        }

        Ok(outputs)
    }

    async fn flush_immutable_remote(
        &self,
        config: &S3PrimaryStorageConfig,
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
            let object_key = Self::remote_sstable_key(config, table_id, &local_id);
            let output = match stored.config.format {
                TableFormat::Row => {
                    self.write_row_sstable_remote(
                        &object_key,
                        table_id,
                        0,
                        local_id,
                        rows,
                        stored.config.bloom_filter_bits_per_key,
                    )
                    .await
                }
                TableFormat::Columnar => {
                    self.write_columnar_sstable_remote(&object_key, 0, local_id, &stored, rows)
                        .await
                }
            }
            .map_err(FlushError::Storage)?;
            outputs.push(output);
        }

        Ok(outputs)
    }

    fn encode_row_sstable(
        table_id: TableId,
        level: u32,
        local_id: String,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<(ResidentRowSstable, Vec<u8>), StorageError> {
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

        meta.length = bytes.len() as u64;
        meta.checksum = checksum;
        meta.data_checksum = data_checksum;

        Ok((
            ResidentRowSstable {
                meta,
                rows,
                user_key_bloom_filter,
                columnar: None,
            },
            bytes,
        ))
    }

    fn encode_json_block<T: Serialize>(label: &str, value: &T) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(value)
            .map_err(|error| StorageError::corruption(format!("encode {label} failed: {error}")))
    }

    fn append_columnar_block(bytes: &mut Vec<u8>, block_bytes: &[u8]) -> ColumnarBlockLocation {
        let location = ColumnarBlockLocation {
            offset: bytes.len() as u64,
            length: block_bytes.len() as u64,
        };
        bytes.extend_from_slice(block_bytes);
        location
    }

    fn columnar_row_is_tombstone(row: &SstableRow) -> Result<bool, StorageError> {
        match row.kind {
            ChangeKind::Delete => {
                if row.value.is_some() {
                    return Err(StorageError::corruption(
                        "columnar delete row unexpectedly contains a value",
                    ));
                }
                Ok(true)
            }
            ChangeKind::Put | ChangeKind::Merge => {
                if row.value.is_none() {
                    return Err(StorageError::corruption(
                        "columnar non-delete row is missing a record value",
                    ));
                }
                Ok(false)
            }
        }
    }

    fn columnar_field_value_for_row(
        field: &FieldDefinition,
        row: &SstableRow,
    ) -> Result<Option<FieldValue>, StorageError> {
        if Self::columnar_row_is_tombstone(row)? {
            return Ok(None);
        }

        let value = row.value.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar row for field {} is missing a value",
                field.name
            ))
        })?;
        let Value::Record(record) = value else {
            return Err(StorageError::corruption(format!(
                "columnar row for field {} is stored as bytes",
                field.name
            )));
        };
        let field_value = record.get(&field.id).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar row is missing field {} ({})",
                field.id.get(),
                field.name
            ))
        })?;
        match row.kind {
            ChangeKind::Put => {
                validate_field_value_against_definition(field, field_value, "columnar row value")?;
            }
            ChangeKind::Merge => {
                validate_merge_operand_value_against_definition(
                    field,
                    field_value,
                    "columnar merge operand value",
                )?;
            }
            ChangeKind::Delete => {
                return Err(StorageError::corruption(
                    "columnar tombstone row unexpectedly reached field extraction",
                ));
            }
        }

        if matches!(field_value, FieldValue::Null) {
            Ok(None)
        } else {
            Ok(Some(field_value.clone()))
        }
    }

    fn encode_columnar_field_block(
        field: &FieldDefinition,
        rows: &[SstableRow],
    ) -> Result<Vec<u8>, StorageError> {
        match field.field_type {
            FieldType::Int64 => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::Int64(value)) => {
                            present_bitmap.push(true);
                            values.push(value);
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected int64",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::Int64(PersistedNullableColumn {
                        present_bitmap,
                        values,
                    }),
                )
            }
            FieldType::Float64 => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values_bits = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::Float64(value)) => {
                            present_bitmap.push(true);
                            values_bits.push(value.to_bits());
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected float64",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::Float64(PersistedFloat64Column {
                        present_bitmap,
                        values_bits,
                    }),
                )
            }
            FieldType::String => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::String(value)) => {
                            present_bitmap.push(true);
                            values.push(value);
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected string",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::String(PersistedNullableColumn {
                        present_bitmap,
                        values,
                    }),
                )
            }
            FieldType::Bytes => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::Bytes(value)) => {
                            present_bitmap.push(true);
                            values.push(value);
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected bytes",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::Bytes(PersistedNullableColumn {
                        present_bitmap,
                        values,
                    }),
                )
            }
            FieldType::Bool => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::Bool(value)) => {
                            present_bitmap.push(true);
                            values.push(value);
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected bool",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::Bool(PersistedNullableColumn {
                        present_bitmap,
                        values,
                    }),
                )
            }
        }
    }

    fn encode_columnar_sstable(
        table_id: TableId,
        level: u32,
        local_id: String,
        schema: &SchemaDefinition,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<(ResidentRowSstable, Vec<u8>), StorageError> {
        schema.validate()?;

        let mut meta = Self::summarize_sstable_rows(table_id, level, &local_id, &rows)?;
        meta.schema_version = Some(schema.version);
        let user_key_bloom_filter = UserKeyBloomFilter::build(&rows, bloom_filter_bits_per_key);

        let mut bytes = Vec::from(&COLUMNAR_SSTABLE_MAGIC[..]);
        let key_index = Self::append_columnar_block(
            &mut bytes,
            &Self::encode_json_block(
                "columnar key index",
                &rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
            )?,
        );
        let sequence_column = Self::append_columnar_block(
            &mut bytes,
            &Self::encode_json_block(
                "columnar sequence column",
                &rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            )?,
        );

        let mut tombstones = Vec::with_capacity(rows.len());
        for row in &rows {
            tombstones.push(Self::columnar_row_is_tombstone(row)?);
        }
        let tombstone_bitmap = Self::append_columnar_block(
            &mut bytes,
            &Self::encode_json_block("columnar tombstone bitmap", &tombstones)?,
        );
        let row_kind_column = Self::append_columnar_block(
            &mut bytes,
            &Self::encode_json_block(
                "columnar row-kind column",
                &rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
            )?,
        );

        let mut columns = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            let block = Self::append_columnar_block(
                &mut bytes,
                &Self::encode_columnar_field_block(field, &rows)?,
            );
            columns.push(PersistedColumnarColumnFooter {
                field_id: field.id,
                field_type: field.field_type,
                encoding: ColumnEncoding::Plain,
                compression: ColumnCompression::None,
                block,
            });
        }

        let data_checksum = checksum32(&bytes[COLUMNAR_SSTABLE_MAGIC.len()..]);
        let footer = PersistedColumnarSstableFooter {
            format_version: COLUMNAR_SSTABLE_FORMAT_VERSION,
            table_id,
            level,
            local_id: local_id.clone(),
            row_count: rows.len() as u64,
            min_key: meta.min_key.clone(),
            max_key: meta.max_key.clone(),
            min_sequence: meta.min_sequence,
            max_sequence: meta.max_sequence,
            schema_version: schema.version,
            data_checksum,
            key_index,
            sequence_column,
            tombstone_bitmap,
            row_kind_column,
            columns,
            user_key_bloom_filter: user_key_bloom_filter.clone(),
        };
        let footer_bytes = Self::encode_json_block("columnar SSTable footer", &footer)?;
        bytes.extend_from_slice(&footer_bytes);
        bytes.extend_from_slice(&(footer_bytes.len() as u64).to_le_bytes());
        bytes.extend_from_slice(COLUMNAR_SSTABLE_MAGIC);

        let checksum = checksum32(&bytes);
        meta.length = bytes.len() as u64;
        meta.checksum = checksum;
        meta.data_checksum = data_checksum;

        Ok((
            ResidentRowSstable {
                meta,
                rows,
                user_key_bloom_filter,
                columnar: None,
            },
            bytes,
        ))
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
        let (mut resident, bytes) =
            Self::encode_row_sstable(table_id, level, local_id, rows, bloom_filter_bits_per_key)?;

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

        resident.meta.file_path = path.to_string();
        Ok(resident)
    }

    async fn write_columnar_sstable(
        &self,
        path: &str,
        level: u32,
        local_id: String,
        stored: &StoredTable,
        rows: Vec<SstableRow>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let schema = stored.config.schema.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar table {} is missing a schema",
                stored.config.name
            ))
        })?;
        let (mut resident, bytes) = Self::encode_columnar_sstable(
            stored.id,
            level,
            local_id,
            schema,
            rows,
            stored.config.bloom_filter_bits_per_key,
        )?;

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

        resident.meta.file_path = path.to_string();
        resident.columnar = Some(ResidentColumnarSstable {
            source: StorageSource::local_file(path.to_string()),
        });
        Ok(resident)
    }

    async fn write_row_sstable_remote(
        &self,
        object_key: &str,
        table_id: TableId,
        level: u32,
        local_id: String,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let (mut resident, bytes) =
            Self::encode_row_sstable(table_id, level, local_id, rows, bloom_filter_bits_per_key)?;
        self.inner
            .dependencies
            .object_store
            .put(object_key, &bytes)
            .await?;
        resident.meta.remote_key = Some(object_key.to_string());
        Ok(resident)
    }

    async fn write_columnar_sstable_remote(
        &self,
        object_key: &str,
        level: u32,
        local_id: String,
        stored: &StoredTable,
        rows: Vec<SstableRow>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let schema = stored.config.schema.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar table {} is missing a schema",
                stored.config.name
            ))
        })?;
        let (mut resident, bytes) = Self::encode_columnar_sstable(
            stored.id,
            level,
            local_id,
            schema,
            rows,
            stored.config.bloom_filter_bits_per_key,
        )?;
        self.inner
            .dependencies
            .object_store
            .put(object_key, &bytes)
            .await?;
        resident.meta.remote_key = Some(object_key.to_string());
        resident.columnar = Some(ResidentColumnarSstable {
            source: StorageSource::remote_object(object_key.to_string()),
        });
        Ok(resident)
    }

    fn encode_manifest_payload_from_sstables(
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        sstables: &[PersistedManifestSstable],
    ) -> Result<Vec<u8>, StorageError> {
        let body = PersistedManifestBody {
            format_version: MANIFEST_FORMAT_VERSION,
            generation,
            last_flushed_sequence,
            sstables: sstables.to_vec(),
        };
        let encoded_body = serde_json::to_vec(&body).map_err(|error| {
            StorageError::corruption(format!("encode manifest body failed: {error}"))
        })?;
        let checksum = checksum32(&encoded_body);
        serde_json::to_vec_pretty(&PersistedManifestFile { body, checksum }).map_err(|error| {
            StorageError::corruption(format!("encode manifest file failed: {error}"))
        })
    }

    fn encode_remote_manifest_payload(
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        sstables: &[PersistedManifestSstable],
        durable_commit_log_segments: &[DurableRemoteCommitLogSegment],
    ) -> Result<Vec<u8>, StorageError> {
        let body = PersistedRemoteManifestBody {
            format_version: REMOTE_MANIFEST_FORMAT_VERSION,
            generation,
            last_flushed_sequence,
            sstables: sstables.to_vec(),
            commit_log_segments: durable_commit_log_segments.to_vec(),
        };
        let encoded_body = serde_json::to_vec(&body).map_err(|error| {
            StorageError::corruption(format!("encode remote manifest body failed: {error}"))
        })?;
        let checksum = checksum32(&encoded_body);
        serde_json::to_vec_pretty(&PersistedRemoteManifestFile { body, checksum }).map_err(
            |error| {
                StorageError::corruption(format!("encode remote manifest file failed: {error}"))
            },
        )
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
        let payload = Self::encode_manifest_payload_from_sstables(
            generation,
            last_flushed_sequence,
            &live
                .iter()
                .map(|sstable| sstable.meta.clone())
                .collect::<Vec<_>>(),
        )?;

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

    async fn install_remote_manifest(
        &self,
        config: &S3PrimaryStorageConfig,
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        live: &[ResidentRowSstable],
        durable_commit_log_segments: &[DurableRemoteCommitLogSegment],
    ) -> Result<(), StorageError> {
        let manifest_key = Self::remote_manifest_path(config, generation);
        let latest_key = Self::remote_manifest_latest_key(config);
        let payload = Self::encode_remote_manifest_payload(
            generation,
            last_flushed_sequence,
            &live
                .iter()
                .map(|sstable| sstable.meta.clone())
                .collect::<Vec<_>>(),
            durable_commit_log_segments,
        )?;

        self.inner
            .dependencies
            .object_store
            .put(&manifest_key, &payload)
            .await?;
        self.inner
            .dependencies
            .object_store
            .put(&latest_key, format!("{manifest_key}\n").as_bytes())
            .await?;
        Ok(())
    }

    fn tiered_backup_layout(&self) -> Option<ObjectKeyLayout> {
        match &self.inner.config.storage {
            StorageConfig::Tiered(config) => Some(Self::tiered_object_layout(config)),
            StorageConfig::S3Primary(_) => None,
        }
    }

    async fn write_remote_backup_object(
        &self,
        layout: &ObjectKeyLayout,
        object_key: &str,
        bytes: &[u8],
        track_for_gc: bool,
    ) -> Result<(), StorageError> {
        self.inner
            .dependencies
            .object_store
            .put(object_key, bytes)
            .await?;
        if track_for_gc {
            Self::note_backup_object_birth(&self.inner.dependencies, layout, object_key).await;
        }
        Ok(())
    }

    async fn note_backup_object_birth(
        dependencies: &DbDependencies,
        layout: &ObjectKeyLayout,
        object_key: &str,
    ) {
        let metadata_key = layout.backup_gc_metadata(object_key);
        if read_optional_remote_object(dependencies, &metadata_key)
            .await
            .ok()
            .flatten()
            .is_some()
        {
            return;
        }

        let record = BackupObjectBirthRecord {
            format_version: BACKUP_GC_METADATA_FORMAT_VERSION,
            object_key: object_key.to_string(),
            first_uploaded_at_millis: dependencies.clock.now().get(),
        };
        let Ok(payload) = serde_json::to_vec_pretty(&record) else {
            return;
        };
        let _ = dependencies.object_store.put(&metadata_key, &payload).await;
    }

    async fn read_backup_object_birth(
        dependencies: &DbDependencies,
        layout: &ObjectKeyLayout,
        object_key: &str,
    ) -> Option<BackupObjectBirthRecord> {
        let metadata_key = layout.backup_gc_metadata(object_key);
        let bytes = read_optional_remote_object(dependencies, &metadata_key)
            .await
            .ok()
            .flatten()?;
        let record: BackupObjectBirthRecord = serde_json::from_slice(&bytes).ok()?;
        (record.format_version == BACKUP_GC_METADATA_FORMAT_VERSION
            && record.object_key == object_key)
            .then_some(record)
    }

    async fn collect_tiered_commit_log_snapshots(
        &self,
        layout: &ObjectKeyLayout,
    ) -> Result<Vec<(DurableRemoteCommitLogSegment, Vec<u8>)>, StorageError> {
        let mut runtime = self.inner.commit_runtime.lock().await;
        let CommitLogBackend::Local(manager) = &mut runtime.backend else {
            return Ok(Vec::new());
        };

        let mut snapshots = Vec::new();
        for descriptor in manager.enumerate_segments() {
            if descriptor.record_count == 0 {
                continue;
            }

            let bytes = manager.read_segment_bytes(descriptor.segment_id).await?;
            let footer = segment_footer_from_bytes(descriptor.segment_id, &bytes)?;
            snapshots.push((
                DurableRemoteCommitLogSegment {
                    object_key: layout.backup_commit_log_segment(descriptor.segment_id),
                    footer,
                },
                bytes,
            ));
        }

        Ok(snapshots)
    }

    async fn sync_tiered_backup_catalog(&self) -> Result<(), StorageError> {
        let Some(layout) = self.tiered_backup_layout() else {
            return Ok(());
        };
        let payload = Self::encode_catalog(&self.tables_read().clone())?;
        let _backup_guard = self.inner.backup_lock.lock().await;
        self.write_remote_backup_object(&layout, &layout.backup_catalog(), &payload, false)
            .await
    }

    async fn sync_tiered_commit_log_tail(&self) -> Result<(), StorageError> {
        let Some(layout) = self.tiered_backup_layout() else {
            return Ok(());
        };
        let _backup_guard = self.inner.backup_lock.lock().await;
        let snapshots = self.collect_tiered_commit_log_snapshots(&layout).await?;
        for (segment, bytes) in snapshots {
            self.write_remote_backup_object(&layout, &segment.object_key, &bytes, true)
                .await?;
        }
        Ok(())
    }

    async fn sync_tiered_backup_manifest(
        &self,
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        live: &[ResidentRowSstable],
    ) -> Result<(), StorageError> {
        let Some(layout) = self.tiered_backup_layout() else {
            return Ok(());
        };

        let catalog_payload = Self::encode_catalog(&self.tables_read().clone())?;
        let mut manifest_sstables = Vec::with_capacity(live.len());
        let mut local_sstable_uploads = Vec::<(String, Vec<u8>)>::new();

        for sstable in live {
            let mut meta = sstable.meta.clone();
            if !meta.file_path.is_empty() {
                let backup_key = layout.backup_sstable(meta.table_id, 0, &meta.local_id);
                let bytes = read_path(&self.inner.dependencies, &meta.file_path).await?;
                meta.file_path.clear();
                meta.remote_key = Some(backup_key.clone());
                local_sstable_uploads.push((backup_key, bytes));
            }
            manifest_sstables.push(meta);
        }

        let _backup_guard = self.inner.backup_lock.lock().await;
        self.write_remote_backup_object(&layout, &layout.backup_catalog(), &catalog_payload, false)
            .await?;

        for (object_key, bytes) in local_sstable_uploads {
            self.write_remote_backup_object(&layout, &object_key, &bytes, true)
                .await?;
        }

        let segment_snapshots = self.collect_tiered_commit_log_snapshots(&layout).await?;
        let durable_segments = segment_snapshots
            .iter()
            .map(|(segment, _)| segment.clone())
            .filter(|segment| segment.footer.max_sequence <= last_flushed_sequence)
            .collect::<Vec<_>>();
        for (segment, bytes) in &segment_snapshots {
            self.write_remote_backup_object(&layout, &segment.object_key, bytes, true)
                .await?;
        }

        let payload = Self::encode_remote_manifest_payload(
            generation,
            last_flushed_sequence,
            &manifest_sstables,
            &durable_segments,
        )?;
        let manifest_key = layout.backup_manifest(generation);
        self.write_remote_backup_object(&layout, &manifest_key, &payload, true)
            .await?;
        self.inner
            .dependencies
            .object_store
            .put(
                &layout.backup_manifest_latest(),
                format!("{manifest_key}\n").as_bytes(),
            )
            .await?;
        self.run_tiered_backup_gc_locked(&layout).await
    }

    async fn run_tiered_backup_gc_locked(
        &self,
        layout: &ObjectKeyLayout,
    ) -> Result<(), StorageError> {
        let latest_key = layout.backup_manifest_latest();
        let mut referenced = BTreeSet::from([latest_key.clone(), layout.backup_catalog()]);
        let mut manifest_keys = self
            .inner
            .dependencies
            .object_store
            .list(&layout.backup_manifest_prefix())
            .await?;
        manifest_keys
            .retain(|key| key != &latest_key && Self::parse_manifest_generation(key).is_some());
        manifest_keys.sort_by_key(|key| {
            std::cmp::Reverse(
                Self::parse_manifest_generation(key)
                    .map(ManifestId::get)
                    .unwrap_or_default(),
            )
        });

        let retained_manifest_keys = manifest_keys
            .iter()
            .take(BACKUP_MANIFEST_RETENTION_LIMIT)
            .cloned()
            .collect::<Vec<_>>();
        let mut min_last_flushed_sequence = None;
        for key in &retained_manifest_keys {
            referenced.insert(key.clone());
            let Ok(file) =
                Self::read_remote_manifest_file_at_key(&self.inner.dependencies, key).await
            else {
                continue;
            };
            min_last_flushed_sequence = Some(
                min_last_flushed_sequence
                    .map(|current: SequenceNumber| current.min(file.body.last_flushed_sequence))
                    .unwrap_or(file.body.last_flushed_sequence),
            );
            for sstable in &file.body.sstables {
                if let Some(remote_key) = &sstable.remote_key {
                    referenced.insert(remote_key.clone());
                }
            }
        }

        if let Some(min_last_flushed_sequence) = min_last_flushed_sequence {
            let commit_log_keys = self
                .inner
                .dependencies
                .object_store
                .list(&layout.backup_commit_log_prefix())
                .await?;
            for key in commit_log_keys {
                let Some(segment_id) = Self::parse_segment_id(&key) else {
                    continue;
                };
                let Ok(bytes) = self.inner.dependencies.object_store.get(&key).await else {
                    continue;
                };
                let Ok(footer) = segment_footer_from_bytes(segment_id, &bytes) else {
                    continue;
                };
                if footer.max_sequence > min_last_flushed_sequence {
                    referenced.insert(key);
                }
            }
        }

        let mut candidates = Vec::new();
        for prefix in [
            layout.backup_manifest_prefix(),
            layout.backup_commit_log_prefix(),
            layout.backup_sstable_prefix(),
            layout.cold_prefix(),
        ] {
            candidates.extend(self.inner.dependencies.object_store.list(&prefix).await?);
        }
        candidates.sort();
        candidates.dedup();

        let now_millis = self.inner.dependencies.clock.now().get();
        for key in candidates {
            if key.starts_with(&layout.backup_gc_metadata_prefix()) || referenced.contains(&key) {
                continue;
            }
            let Some(record) =
                Self::read_backup_object_birth(&self.inner.dependencies, layout, &key).await
            else {
                continue;
            };
            if now_millis.saturating_sub(record.first_uploaded_at_millis)
                < BACKUP_GC_GRACE_PERIOD_MILLIS
            {
                continue;
            }
            self.inner.dependencies.object_store.delete(&key).await?;
            let _ = self
                .inner
                .dependencies
                .object_store
                .delete(&layout.backup_gc_metadata(&key))
                .await;
        }

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

    fn build_offload_job(table: &StoredTable, inputs: &[ResidentRowSstable]) -> Option<OffloadJob> {
        if inputs.is_empty() {
            return None;
        }

        let input_local_ids = inputs
            .iter()
            .map(|sstable| sstable.meta.local_id.clone())
            .collect::<Vec<_>>();
        Some(OffloadJob {
            id: format!(
                "offload:{}:{}",
                table.config.name,
                input_local_ids.join("+")
            ),
            table_id: table.id,
            table_name: table.config.name.clone(),
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

    fn pending_flush_candidates(&self) -> Vec<PendingWorkCandidate> {
        if self.local_storage_root().is_none() {
            return Vec::new();
        }

        let tables = self.tables_read().clone();
        let mut candidates = self
            .memtables_read()
            .immutable_flush_backlog_by_table()
            .into_iter()
            .filter_map(|(table_id, estimated_bytes)| {
                let stored = Self::stored_table_by_id(&tables, table_id)?;
                Some(PendingWorkCandidate {
                    pending: PendingWork {
                        id: format!("flush:{}", stored.config.name),
                        work_type: PendingWorkType::Flush,
                        table: stored.config.name.clone(),
                        level: None,
                        estimated_bytes,
                    },
                    spec: PendingWorkSpec::Flush,
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.pending
                .table
                .cmp(&right.pending.table)
                .then_with(|| left.pending.id.cmp(&right.pending.id))
        });
        candidates
    }

    fn pending_offload_candidates(&self) -> Vec<PendingWorkCandidate> {
        let Some(max_local_bytes) = self.local_sstable_budget_bytes() else {
            return Vec::new();
        };

        let tables = self.tables_read().clone();
        let live = self.sstables_read().live.clone();
        let mut candidates = tables
            .values()
            .filter_map(|table| {
                if table.config.format != TableFormat::Row {
                    return None;
                }

                let mut local_sstables = live
                    .iter()
                    .filter(|sstable| {
                        sstable.meta.table_id == table.id && !sstable.meta.file_path.is_empty()
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let local_bytes = local_sstables
                    .iter()
                    .map(|sstable| sstable.meta.length)
                    .sum::<u64>();
                if local_bytes <= max_local_bytes {
                    return None;
                }

                local_sstables.sort_by(|left, right| {
                    (
                        left.meta.max_sequence.get(),
                        left.meta.min_sequence.get(),
                        left.meta.level,
                        left.meta.local_id.as_str(),
                    )
                        .cmp(&(
                            right.meta.max_sequence.get(),
                            right.meta.min_sequence.get(),
                            right.meta.level,
                            right.meta.local_id.as_str(),
                        ))
                });

                let mut remaining_local_bytes = local_bytes;
                let mut selected = Vec::new();
                for sstable in local_sstables {
                    if remaining_local_bytes <= max_local_bytes {
                        break;
                    }
                    remaining_local_bytes =
                        remaining_local_bytes.saturating_sub(sstable.meta.length);
                    selected.push(sstable);
                }

                let job = Self::build_offload_job(table, &selected)?;
                Some(PendingWorkCandidate {
                    pending: PendingWork {
                        id: job.id.clone(),
                        work_type: PendingWorkType::Offload,
                        table: job.table_name.clone(),
                        level: None,
                        estimated_bytes: job.estimated_bytes,
                    },
                    spec: PendingWorkSpec::Offload(job),
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.pending
                .table
                .cmp(&right.pending.table)
                .then_with(|| left.pending.id.cmp(&right.pending.id))
        });
        candidates
    }

    fn pending_work_candidates(&self) -> Vec<PendingWorkCandidate> {
        let mut candidates = self.pending_flush_candidates();
        candidates.extend(self.pending_compaction_jobs().into_iter().map(|job| {
            PendingWorkCandidate {
                pending: PendingWork {
                    id: job.id.clone(),
                    work_type: PendingWorkType::Compaction,
                    table: job.table_name.clone(),
                    level: Some(job.source_level),
                    estimated_bytes: job.estimated_bytes,
                },
                spec: PendingWorkSpec::Compaction(job),
            }
        }));
        candidates.extend(self.pending_offload_candidates());
        candidates.sort_by(|left, right| {
            pending_work_sort_key(&left.pending)
                .cmp(&pending_work_sort_key(&right.pending))
                .then_with(|| left.pending.id.cmp(&right.pending.id))
        });
        candidates
    }

    fn prune_work_deferrals(&self, candidates: &[PendingWorkCandidate]) {
        let live_work_ids = candidates
            .iter()
            .map(|candidate| candidate.pending.id.as_str())
            .collect::<BTreeSet<_>>();
        mutex_lock(&self.inner.work_deferrals)
            .retain(|work_id, _| live_work_ids.contains(work_id.as_str()));
    }

    fn record_deferred_work(
        &self,
        candidates: &[PendingWorkCandidate],
        decisions: &BTreeMap<String, ScheduleAction>,
    ) {
        let mut deferrals = mutex_lock(&self.inner.work_deferrals);
        for candidate in candidates {
            match decisions
                .get(&candidate.pending.id)
                .copied()
                .unwrap_or(ScheduleAction::Defer)
            {
                ScheduleAction::Execute => {
                    deferrals.remove(&candidate.pending.id);
                }
                ScheduleAction::Defer => {
                    *deferrals.entry(candidate.pending.id.clone()).or_default() += 1;
                }
            }
        }
    }

    fn reset_work_deferral(&self, work_id: &str) {
        mutex_lock(&self.inner.work_deferrals).remove(work_id);
    }

    fn deferred_work_candidate(
        &self,
        candidates: &[PendingWorkCandidate],
    ) -> Option<PendingWorkCandidate> {
        let deferrals = mutex_lock(&self.inner.work_deferrals);
        candidates
            .iter()
            .find(|candidate| {
                deferrals
                    .get(&candidate.pending.id)
                    .copied()
                    .unwrap_or_default()
                    >= MAX_SCHEDULER_DEFER_CYCLES
            })
            .cloned()
    }

    fn forced_l0_compaction_candidate(
        &self,
        candidates: &[PendingWorkCandidate],
    ) -> Option<PendingWorkCandidate> {
        let live = self.sstables_read().live.clone();
        candidates
            .iter()
            .find(|candidate| match &candidate.spec {
                PendingWorkSpec::Compaction(job) if job.source_level == 0 => {
                    SstableState {
                        manifest_generation: ManifestId::default(),
                        last_flushed_sequence: SequenceNumber::default(),
                        live: live.clone(),
                    }
                    .table_stats(job.table_id)
                    .0 >= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT
                }
                PendingWorkSpec::Flush
                | PendingWorkSpec::Compaction(_)
                | PendingWorkSpec::Offload(_) => false,
            })
            .cloned()
    }

    async fn execute_pending_work(
        &self,
        local_root: &str,
        candidate: PendingWorkCandidate,
    ) -> Result<(), StorageError> {
        self.reset_work_deferral(&candidate.pending.id);
        match candidate.spec {
            PendingWorkSpec::Flush => self
                .flush_internal(false)
                .await
                .map_err(Self::flush_error_into_storage),
            PendingWorkSpec::Compaction(job) => self.execute_compaction_job(local_root, job).await,
            PendingWorkSpec::Offload(job) => self.execute_offload_job(job).await,
        }
    }

    async fn run_scheduler_pass(&self, allow_forced_execution: bool) -> Result<bool, StorageError> {
        let Some(local_root) = self.local_storage_root().map(str::to_string) else {
            return Ok(false);
        };

        let attempts = if allow_forced_execution {
            MAX_SCHEDULER_DEFER_CYCLES
        } else {
            1
        };

        for _ in 0..attempts {
            let candidates = self.pending_work_candidates();
            self.prune_work_deferrals(&candidates);
            if candidates.is_empty() {
                return Ok(false);
            }

            if let Some(candidate) = self.forced_l0_compaction_candidate(&candidates) {
                self.execute_pending_work(&local_root, candidate).await?;
                return Ok(true);
            }

            let pending = candidates
                .iter()
                .map(|candidate| candidate.pending.clone())
                .collect::<Vec<_>>();
            let decisions = self
                .inner
                .scheduler
                .on_work_available(&pending)
                .into_iter()
                .map(|decision| (decision.work_id, decision.action))
                .collect::<BTreeMap<_, _>>();

            if let Some(candidate) = candidates
                .iter()
                .find(|candidate| {
                    decisions
                        .get(&candidate.pending.id)
                        .copied()
                        .unwrap_or(ScheduleAction::Defer)
                        == ScheduleAction::Execute
                })
                .cloned()
            {
                self.execute_pending_work(&local_root, candidate).await?;
                return Ok(true);
            }

            self.record_deferred_work(&candidates, &decisions);
            if allow_forced_execution
                && let Some(candidate) = self.deferred_work_candidate(&candidates)
            {
                self.execute_pending_work(&local_root, candidate).await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    #[cfg(test)]
    async fn run_next_scheduled_work(&self) -> Result<bool, StorageError> {
        self.run_scheduler_pass(true).await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn estimated_sstable_row_bytes(row: &SstableRow) -> u64 {
        (row.key.len() + row.value.as_ref().map(value_size_bytes).unwrap_or_default() + 32) as u64
    }

    fn retain_rows_within_horizon(
        rows: Vec<CompactionRow>,
        horizon: Option<SequenceNumber>,
    ) -> Vec<CompactionRow> {
        let Some(horizon) = horizon else {
            return rows;
        };

        let mut retained = Vec::with_capacity(rows.len());
        let mut index = 0_usize;
        while index < rows.len() {
            let key = rows[index].row.key.clone();
            let mut anchor_kept = false;

            while index < rows.len() && rows[index].row.key == key {
                let row = rows[index].clone();
                if row.row.sequence > horizon {
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

    async fn load_compaction_input_rows(
        &self,
        table: &StoredTable,
        inputs: &[ResidentRowSstable],
    ) -> Result<Vec<CompactionRow>, StorageError> {
        let projection = match table.config.format {
            TableFormat::Row => None,
            TableFormat::Columnar => {
                Some(Self::resolve_scan_projection(table, None)?.ok_or_else(|| {
                    StorageError::corruption("columnar table is missing a schema")
                })?)
            }
        };
        let mut rows = Vec::new();

        for sstable in inputs {
            if !sstable.is_columnar() {
                rows.extend(sstable.rows.iter().cloned().map(|row| CompactionRow {
                    level: sstable.meta.level,
                    row,
                }));
                continue;
            }

            let projection = projection.as_ref().ok_or_else(|| {
                StorageError::corruption(format!(
                    "row table {} unexpectedly references a columnar SSTable",
                    table.config.name
                ))
            })?;
            let metadata = sstable
                .load_columnar_metadata(&self.inner.dependencies)
                .await?;
            let location = sstable.meta.storage_descriptor();
            let mut row_kinds = Vec::with_capacity(metadata.key_index.len());
            let mut row_indexes = BTreeSet::new();
            for row_index in 0..metadata.key_index.len() {
                let kind = Self::normalized_columnar_row_kind(
                    location,
                    row_index,
                    metadata.tombstones[row_index],
                    metadata.row_kinds[row_index],
                )?;
                row_kinds.push(kind);
                if kind != ChangeKind::Delete {
                    row_indexes.insert(row_index);
                }
            }
            let values = sstable
                .materialize_columnar_rows(&self.inner.dependencies, projection, &row_indexes)
                .await?;

            for (row_index, kind) in row_kinds.iter().copied().enumerate() {
                let value = if kind == ChangeKind::Delete {
                    None
                } else {
                    Some(values.get(&row_index).cloned().ok_or_else(|| {
                        StorageError::corruption(format!(
                            "columnar SSTable {} row {} was not materialized for compaction",
                            location, row_index
                        ))
                    })?)
                };
                rows.push(CompactionRow {
                    level: sstable.meta.level,
                    row: SstableRow {
                        key: metadata.key_index[row_index].clone(),
                        sequence: metadata.sequences[row_index],
                        kind,
                        value,
                    },
                });
            }
        }

        Ok(rows)
    }

    async fn rewrite_compaction_rows(
        &self,
        tables: &BTreeMap<String, StoredTable>,
        memtables: &MemtableState,
        sstables: &SstableState,
        table: &StoredTable,
        rows: Vec<CompactionRow>,
    ) -> Result<Vec<CompactionRow>, StorageError> {
        let columnar_projection =
            if table.config.format == TableFormat::Columnar {
                Some(Self::resolve_scan_projection(table, None)?.ok_or_else(|| {
                    StorageError::corruption("columnar table is missing a schema")
                })?)
            } else {
                None
            };
        let mut rewritten = Vec::with_capacity(rows.len());
        for row in rows {
            if row.row.kind != ChangeKind::Merge {
                rewritten.push(row);
                continue;
            }

            let resolved = match table.config.format {
                TableFormat::Row => Self::resolve_visible_value_with_state(
                    tables,
                    memtables,
                    sstables,
                    table.id,
                    &row.row.key,
                    row.row.sequence,
                )?,
                TableFormat::Columnar => {
                    self.resolve_visible_value_columnar_with_state(
                        table,
                        memtables,
                        sstables,
                        &row.row.key,
                        row.row.sequence,
                        columnar_projection
                            .as_ref()
                            .expect("columnar projection should exist"),
                    )
                    .await?
                }
            };
            let value = resolved.value.ok_or_else(|| {
                StorageError::corruption("merge resolution unexpectedly produced a tombstone")
            })?;
            rewritten.push(CompactionRow {
                level: row.level,
                row: SstableRow {
                    key: row.row.key,
                    sequence: row.row.sequence,
                    kind: ChangeKind::Put,
                    value: Some(value),
                },
            });
        }

        Ok(rewritten)
    }

    fn apply_compaction_filter(
        &self,
        table: &StoredTable,
        rows: Vec<CompactionRow>,
    ) -> FilteredCompactionRows {
        let Some(filter) = table.config.compaction_filter.as_ref() else {
            return FilteredCompactionRows {
                rows,
                ..FilteredCompactionRows::default()
            };
        };

        let now = self.inner.dependencies.clock.now();
        let oldest_active_snapshot = self.oldest_active_snapshot_sequence();
        let mut retained = Vec::with_capacity(rows.len());
        let mut filtered_keys = BTreeSet::new();
        let mut removed_bytes = 0_u64;

        for row in rows {
            let snapshot_protected = oldest_active_snapshot
                .is_some_and(|oldest_snapshot| row.row.sequence >= oldest_snapshot);
            if snapshot_protected {
                retained.push(row);
                continue;
            }

            let decision = filter.decide(CompactionDecisionContext {
                level: row.level,
                key: &row.row.key,
                value: row.row.value.as_ref(),
                sequence: row.row.sequence,
                kind: row.row.kind,
                now,
            });
            if decision == CompactionDecision::Remove {
                removed_bytes =
                    removed_bytes.saturating_add(Self::estimated_sstable_row_bytes(&row.row));
                filtered_keys.insert(row.row.key.clone());
            } else {
                retained.push(row);
            }
        }

        let retained_keys = retained
            .iter()
            .map(|row| row.row.key.clone())
            .collect::<BTreeSet<_>>();
        let removed_keys = filtered_keys
            .into_iter()
            .filter(|key| !retained_keys.contains(key))
            .count() as u64;

        FilteredCompactionRows {
            rows: retained,
            removed_bytes,
            removed_keys,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn write_compaction_outputs(
        &self,
        local_root: &str,
        table: &StoredTable,
        level: u32,
        rows: Vec<SstableRow>,
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
            let path = Self::local_sstable_path(local_root, table.id, &local_id);
            let output = match table.config.format {
                TableFormat::Row => {
                    self.write_row_sstable(
                        &path,
                        table.id,
                        level,
                        local_id,
                        rows[start..end].to_vec(),
                        table.config.bloom_filter_bits_per_key,
                    )
                    .await?
                }
                TableFormat::Columnar => {
                    self.write_columnar_sstable(
                        &path,
                        level,
                        local_id,
                        table,
                        rows[start..end].to_vec(),
                    )
                    .await?
                }
            };
            outputs.push(output);
            start = end;
        }

        Ok(outputs)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn run_next_offload(&self) -> Result<bool, StorageError> {
        let Some(_) = self.local_storage_root() else {
            return Ok(false);
        };
        let _maintenance_guard = self.inner.maintenance_lock.lock().await;
        let Some(job) = self
            .pending_offload_candidates()
            .into_iter()
            .find_map(|candidate| match candidate.spec {
                PendingWorkSpec::Offload(job) => Some(job),
                PendingWorkSpec::Flush | PendingWorkSpec::Compaction(_) => None,
            })
        else {
            return Ok(false);
        };

        self.execute_offload_job(job).await?;
        Ok(true)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn run_next_compaction(&self) -> Result<bool, StorageError> {
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
    async fn execute_offload_job(&self, job: OffloadJob) -> Result<(), StorageError> {
        let StorageConfig::Tiered(config) = &self.inner.config.storage else {
            return Err(StorageError::unsupported(
                "cold offload is only supported in tiered mode",
            ));
        };

        let tables = self.tables_read().clone();
        let table = Self::stored_table_by_id(&tables, job.table_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::corruption(format!(
                    "offload references unknown table id {}",
                    job.table_id.get()
                ))
            })?;
        if table.config.format != TableFormat::Row {
            return Err(StorageError::unsupported(
                "cold offload only supports row tables",
            ));
        }

        let live = self.sstables_read().live.clone();
        let by_local_id = live
            .into_iter()
            .map(|sstable| (sstable.meta.local_id.clone(), sstable))
            .collect::<BTreeMap<_, _>>();
        let mut inputs = Vec::with_capacity(job.input_local_ids.len());
        for local_id in &job.input_local_ids {
            let input = by_local_id.get(local_id).cloned().ok_or_else(|| {
                StorageError::corruption(format!(
                    "offload job {} resolved no SSTable for {}",
                    job.id, local_id
                ))
            })?;
            if input.meta.table_id != job.table_id {
                return Err(StorageError::corruption(format!(
                    "offload job {} references SSTable {} from another table",
                    job.id, local_id
                )));
            }
            if input.meta.file_path.is_empty() {
                return Err(StorageError::corruption(format!(
                    "offload job {} references non-local SSTable {}",
                    job.id, local_id
                )));
            }
            inputs.push(input);
        }

        let layout = ObjectKeyLayout::new(&config.s3);
        let storage = UnifiedStorage::from_dependencies(&self.inner.dependencies);
        let mut updated_inputs = BTreeMap::new();
        for input in &inputs {
            let remote_key = layout.cold_sstable(
                input.meta.table_id,
                0,
                input.meta.min_sequence,
                input.meta.max_sequence,
                &input.meta.local_id,
            );
            let bytes = read_path(&self.inner.dependencies, &input.meta.file_path).await?;
            storage
                .put_object(&remote_key, &bytes)
                .await
                .map_err(|error| error.into_storage_error())?;
            Self::note_backup_object_birth(&self.inner.dependencies, &layout, &remote_key).await;

            let mut updated = input.clone();
            updated.meta.file_path.clear();
            updated.meta.remote_key = Some(remote_key);
            updated_inputs.insert(updated.meta.local_id.clone(), updated);
        }

        #[cfg(test)]
        self.maybe_pause_offload_phase(OffloadPhase::UploadComplete)
            .await;

        let current_state = self.sstables_read().clone();
        let mut replaced = 0_usize;
        let mut new_live = current_state
            .live
            .into_iter()
            .map(|sstable| {
                if let Some(updated) = updated_inputs.get(&sstable.meta.local_id) {
                    replaced += 1;
                    updated.clone()
                } else {
                    sstable
                }
            })
            .collect::<Vec<_>>();
        if replaced != job.input_local_ids.len() {
            return Err(StorageError::corruption(format!(
                "offload job {} replaced {} of {} SSTables",
                job.id,
                replaced,
                job.input_local_ids.len()
            )));
        }
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
        let state = self.sstables_read().clone();
        let _ = self
            .sync_tiered_backup_manifest(
                state.manifest_generation,
                state.last_flushed_sequence,
                &state.live,
            )
            .await;

        #[cfg(test)]
        self.maybe_pause_offload_phase(OffloadPhase::ManifestSwitched)
            .await;

        for input in &inputs {
            self.inner
                .dependencies
                .file_system
                .delete(&input.meta.file_path)
                .await?;
        }

        #[cfg(test)]
        self.maybe_pause_offload_phase(OffloadPhase::LocalCleanupFinished)
            .await;

        Ok(())
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

        let filtered = match job.kind {
            CompactionJobKind::Rewrite => {
                let mut merged_rows = self.load_compaction_input_rows(&table, &inputs).await?;
                merged_rows.sort_by_key(|row| {
                    encode_mvcc_key(&row.row.key, CommitId::new(row.row.sequence))
                });
                merged_rows = self
                    .rewrite_compaction_rows(&tables, &memtables, &sstables, &table, merged_rows)
                    .await?;
                merged_rows = Self::retain_rows_within_horizon(
                    merged_rows,
                    self.history_gc_horizon(job.table_id),
                );
                self.apply_compaction_filter(&table, merged_rows)
            }
            CompactionJobKind::DeleteOnly => FilteredCompactionRows::default(),
        };

        let outputs = match job.kind {
            CompactionJobKind::Rewrite => {
                let outputs = self
                    .write_compaction_outputs(
                        local_root,
                        &table,
                        job.target_level,
                        filtered
                            .rows
                            .iter()
                            .cloned()
                            .map(|row| row.row)
                            .collect::<Vec<_>>(),
                    )
                    .await?;

                #[cfg(test)]
                if !outputs.is_empty() {
                    self.maybe_pause_compaction_phase(CompactionPhase::OutputWritten)
                        .await;
                }

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
        let state = self.sstables_read().clone();
        let _ = self
            .sync_tiered_backup_manifest(
                state.manifest_generation,
                state.last_flushed_sequence,
                &state.live,
            )
            .await;
        self.record_compaction_filter_stats(
            job.table_id,
            filtered.removed_bytes,
            filtered.removed_keys,
        );

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

        let table = Table {
            db: self.clone(),
            name: Arc::from(name),
            id: Some(id),
        };
        let _ = self.sync_tiered_backup_catalog().await;
        Ok(table)
    }

    pub async fn snapshot(&self) -> Snapshot {
        let sequence = self.current_sequence();
        self.register_snapshot(sequence)
    }

    pub async fn durable_snapshot(&self) -> Snapshot {
        let sequence = self.current_durable_sequence();
        self.register_snapshot(sequence)
    }

    fn register_snapshot(&self, sequence: SequenceNumber) -> Snapshot {
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
        self.apply_write_backpressure(&resolved_operations).await?;

        let participant = {
            let _commit_guard = self.inner.commit_lock.lock().await;
            self.check_read_conflicts(&resolved_read_set)?;

            let sequence = SequenceNumber::new(self.inner.next_sequence.load(Ordering::SeqCst) + 1);
            let record = Self::build_commit_record(sequence, &resolved_operations)?;
            let participant = if self.durable_on_commit() {
                let participant = self.register_commit(sequence, resolved_operations.clone());
                participant
                    .group_batch
                    .as_ref()
                    .expect("group-commit participants should always have a batch")
                    .stage_record(record);
                participant
            } else {
                self.inner
                    .commit_runtime
                    .lock()
                    .await
                    .append(record)
                    .await
                    .map_err(CommitError::Storage)?;
                self.register_commit(sequence, resolved_operations.clone())
            };
            self.inner
                .next_sequence
                .store(sequence.get(), Ordering::SeqCst);
            participant
        };

        if let Some(batch) = participant.group_batch.clone() {
            if let Err(error) = self.await_group_commit(batch, participant.sequence).await {
                self.abort_commit(&participant);
                self.publish_watermarks();
                return Err(error);
            }

            self.mark_durable_confirmed(participant.sequence);
            let _ = self.sync_tiered_commit_log_tail().await;
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
        self.flush_internal(true).await
    }

    async fn flush_internal(&self, _allow_scheduler_follow_up: bool) -> Result<(), FlushError> {
        let _maintenance_guard = self.inner.maintenance_lock.lock().await;
        match &self.inner.config.storage {
            StorageConfig::Tiered(_) => {
                let local_root = self
                    .local_storage_root()
                    .expect("tiered storage should have local root")
                    .to_string();
                let immutables = {
                    let _commit_guard = self.inner.commit_lock.lock().await;
                    self.memtables_write().rotate_mutable();

                    self.inner
                        .commit_runtime
                        .lock()
                        .await
                        .sync()
                        .await
                        .map_err(FlushError::Storage)?;
                    self.mark_all_commits_durable();
                    self.publish_watermarks();
                    self.memtables_read().immutables.clone()
                };

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
                    manifest_generation =
                        ManifestId::new(manifest_generation.get().saturating_add(1));
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

                let backup_result = if flushed_count > 0 {
                    let state = self.sstables_read().clone();
                    self.sync_tiered_backup_manifest(
                        state.manifest_generation,
                        state.last_flushed_sequence,
                        &state.live,
                    )
                    .await
                } else {
                    self.sync_tiered_commit_log_tail().await
                };
                let _ = backup_result;

                self.prune_commit_log(true)
                    .await
                    .map_err(FlushError::Storage)?;
            }
            StorageConfig::S3Primary(config) => {
                let (immutables, buffered_records, mut durable_segments, next_segment_id) = {
                    let _commit_guard = self.inner.commit_lock.lock().await;
                    self.memtables_write().rotate_mutable();

                    let immutables = self.memtables_read().immutables.clone();
                    let mut commit_runtime = self.inner.commit_runtime.lock().await;
                    commit_runtime.sync().await.map_err(FlushError::Storage)?;
                    match &mut commit_runtime.backend {
                        CommitLogBackend::Memory(log) => (
                            immutables,
                            log.records.clone(),
                            log.durable_commit_log_segments.clone(),
                            log.next_segment_id,
                        ),
                        CommitLogBackend::Local(_) => {
                            return Err(FlushError::Storage(StorageError::unsupported(
                                "s3-primary flush requires the memory commit-log backend",
                            )));
                        }
                    }
                };

                if immutables.is_empty() && buffered_records.is_empty() {
                    return Ok(());
                }

                if !buffered_records.is_empty() {
                    let (segment_bytes, footer) = encode_segment_bytes(
                        crate::SegmentId::new(next_segment_id),
                        &buffered_records,
                        SegmentOptions::default().records_per_block,
                    )
                    .map_err(FlushError::Storage)?;
                    let object_key = Self::remote_commit_log_segment_key(config, footer.segment_id);
                    self.inner
                        .dependencies
                        .object_store
                        .put(&object_key, &segment_bytes)
                        .await
                        .map_err(FlushError::Storage)?;
                    durable_segments.push(DurableRemoteCommitLogSegment { object_key, footer });
                    durable_segments.sort_by_key(|segment| segment.footer.segment_id.get());
                }

                let mut sstable_state = self.sstables_read().clone();
                let mut new_live = sstable_state.live.clone();
                for immutable in &immutables {
                    new_live.extend(self.flush_immutable_remote(config, immutable).await?);
                }
                Self::sort_live_sstables(&mut new_live);

                let flushed_through = buffered_records
                    .last()
                    .map(CommitRecord::sequence)
                    .into_iter()
                    .chain(immutables.iter().map(|immutable| immutable.max_sequence))
                    .max()
                    .unwrap_or(sstable_state.last_flushed_sequence)
                    .max(sstable_state.last_flushed_sequence);
                let next_generation =
                    ManifestId::new(sstable_state.manifest_generation.get().saturating_add(1));
                self.install_remote_manifest(
                    config,
                    next_generation,
                    flushed_through,
                    &new_live,
                    &durable_segments,
                )
                .await
                .map_err(FlushError::Storage)?;

                {
                    let _commit_guard = self.inner.commit_lock.lock().await;
                    let mut commit_runtime = self.inner.commit_runtime.lock().await;
                    match &mut commit_runtime.backend {
                        CommitLogBackend::Memory(log) => {
                            log.records
                                .retain(|record| record.sequence() > flushed_through);
                            log.durable_commit_log_segments = durable_segments.clone();
                            if !buffered_records.is_empty() {
                                log.next_segment_id = next_segment_id.saturating_add(1);
                            }
                        }
                        CommitLogBackend::Local(_) => {
                            return Err(FlushError::Storage(StorageError::unsupported(
                                "s3-primary flush requires the memory commit-log backend",
                            )));
                        }
                    }

                    if !immutables.is_empty() {
                        let mut memtables = self.memtables_write();
                        memtables.immutables.drain(0..immutables.len());
                    }

                    self.mark_commits_durable_through(flushed_through);
                    self.publish_watermarks();
                }

                sstable_state.live = new_live;
                sstable_state.manifest_generation = next_generation;
                sstable_state.last_flushed_sequence = flushed_through;
                *self.sstables_write() = sstable_state;
            }
        }

        Ok(())
    }

    pub async fn scan_since(
        &self,
        table: &Table,
        cursor: LogCursor,
        opts: ScanOptions,
    ) -> Result<ChangeStream, SnapshotTooOld> {
        self.scan_change_feed(table, cursor, self.current_sequence(), opts)
            .await
    }

    pub fn subscribe(&self, table: &Table) -> WatermarkReceiver {
        self.inner.visible_watchers.subscribe(table.name())
    }

    pub async fn scan_durable_since(
        &self,
        table: &Table,
        cursor: LogCursor,
        opts: ScanOptions,
    ) -> Result<ChangeStream, SnapshotTooOld> {
        self.scan_change_feed(table, cursor, self.current_durable_sequence(), opts)
            .await
    }

    pub fn subscribe_durable(&self, table: &Table) -> WatermarkReceiver {
        self.inner.durable_watchers.subscribe(table.name())
    }

    pub fn subscribe_visible_set<'a, I>(&self, tables: I) -> WatermarkSubscriptionSet
    where
        I: IntoIterator<Item = &'a Table>,
    {
        Self::subscribe_set(&self.inner.visible_watchers, tables)
    }

    pub fn subscribe_durable_set<'a, I>(&self, tables: I) -> WatermarkSubscriptionSet
    where
        I: IntoIterator<Item = &'a Table>,
    {
        Self::subscribe_set(&self.inner.durable_watchers, tables)
    }

    pub async fn table_stats(&self, table: &Table) -> TableStats {
        let tables = self.tables_read().clone();
        let live = self.sstables_read().live.clone();
        let current_sequence = self.current_sequence();
        let recovery_floor_sequence = self.sstables_read().last_flushed_sequence;
        let cdc_gc_min_sequence = self.cdc_gc_min_sequence(current_sequence);
        let commit_log_gc_floor_sequence = cdc_gc_min_sequence
            .map(|cdc_min| recovery_floor_sequence.min(cdc_min))
            .unwrap_or(recovery_floor_sequence);
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
            compaction_filter_removed_bytes,
            compaction_filter_removed_keys,
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
                let compaction_filter_stats = self.compaction_filter_stats(table_id);
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
                    compaction_filter_stats.removed_bytes,
                    compaction_filter_stats.removed_keys,
                    memtables.pending_flush_bytes(table_id),
                    memtables.immutable_memtable_count(table_id),
                    history_retention_floor_sequence,
                    history_gc_horizon_sequence,
                    history_pinned_by_snapshots,
                )
            })
            .unwrap_or((0, 0, 0, 0, 0, 0, 0, 0, None, None, false));
        let (
            change_feed_oldest_available_sequence,
            change_feed_floor_sequence,
            change_feed_pins_commit_log_gc,
        ) = if let Some(table_id) = table.resolve_id() {
            let table_watermark = self.table_change_feed_watermark(table_id);
            let runtime = self.inner.commit_runtime.lock().await;
            let logical_floor = self.cdc_retention_floor_sequence(table_id, current_sequence);
            let floor = self.change_feed_floor_from_state(
                table_id,
                current_sequence,
                runtime.oldest_sequence_for_table(table_id),
                runtime.oldest_segment_id(),
                table_watermark,
            );
            let pins =
                logical_floor
                    .zip(cdc_gc_min_sequence)
                    .is_some_and(|(table_floor, gc_min)| {
                        table_floor == gc_min && gc_min <= recovery_floor_sequence
                    });
            (runtime.oldest_sequence_for_table(table_id), floor, pins)
        } else {
            (None, None, false)
        };

        TableStats {
            l0_sstable_count,
            total_bytes,
            local_bytes,
            s3_bytes: total_bytes.saturating_sub(local_bytes),
            compaction_debt,
            compaction_filter_removed_bytes,
            compaction_filter_removed_keys,
            pending_flush_bytes,
            immutable_memtable_count,
            history_retention_floor_sequence,
            history_gc_horizon_sequence,
            oldest_active_snapshot_sequence,
            active_snapshot_count,
            history_pinned_by_snapshots,
            change_feed_oldest_available_sequence,
            change_feed_floor_sequence,
            commit_log_recovery_floor_sequence: recovery_floor_sequence,
            commit_log_gc_floor_sequence,
            change_feed_pins_commit_log_gc,
            metadata,
        }
    }

    pub async fn pending_work(&self) -> Vec<PendingWork> {
        self.pending_work_candidates()
            .into_iter()
            .map(|candidate| candidate.pending)
            .collect()
    }

    async fn apply_write_backpressure(
        &self,
        operations: &[ResolvedBatchOperation],
    ) -> Result<(), CommitError> {
        let batch_bytes_by_table = Self::estimated_batch_bytes_by_table(operations);
        let total_batch_bytes = batch_bytes_by_table.values().copied().sum::<u64>();
        if self.memtable_budget_exceeded_by(total_batch_bytes) {
            self.flush_internal(false)
                .await
                .map_err(|error| CommitError::Storage(Self::flush_error_into_storage(error)))?;
        }

        let touched_tables = operations
            .iter()
            .map(|operation| {
                (
                    operation.table_id,
                    Table {
                        db: self.clone(),
                        name: Arc::from(operation.table_name.clone()),
                        id: Some(operation.table_id),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();

        loop {
            let mut max_delay = Duration::ZERO;
            let mut should_run_maintenance = false;
            let mut must_stall = false;

            for table in &touched_tables {
                let stats = self.table_stats(table).await;
                let decision = self.inner.scheduler.should_throttle(table, &stats);
                let table_bytes = table
                    .id()
                    .and_then(|table_id| batch_bytes_by_table.get(&table_id).copied())
                    .unwrap_or(total_batch_bytes);

                if let Some(rate) = decision.max_write_bytes_per_second {
                    max_delay = max_delay.max(Self::throttle_delay(table_bytes, rate));
                }
                if decision.throttle {
                    should_run_maintenance = true;
                }
                if decision.stall || stats.l0_sstable_count >= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT
                {
                    should_run_maintenance = true;
                    must_stall = true;
                }
            }

            if should_run_maintenance {
                let progressed = self
                    .run_scheduler_pass(true)
                    .await
                    .map_err(CommitError::Storage)?;
                if progressed {
                    continue;
                }
                if must_stall {
                    break;
                }
            }

            if max_delay > Duration::ZERO {
                self.inner.dependencies.clock.sleep(max_delay).await;
            }
            break;
        }

        Ok(())
    }

    fn memtable_budget_bytes(&self) -> Option<u64> {
        match &self.inner.config.storage {
            StorageConfig::Tiered(config) => Some(config.max_local_bytes),
            StorageConfig::S3Primary(_) => None,
        }
    }

    fn local_sstable_budget_bytes(&self) -> Option<u64> {
        self.memtable_budget_bytes()
    }

    fn memtable_budget_exceeded_by(&self, additional_bytes: u64) -> bool {
        self.memtable_budget_bytes().is_some_and(|budget| {
            self.memtables_read()
                .total_pending_flush_bytes()
                .saturating_add(additional_bytes)
                > budget
        })
    }

    fn estimated_batch_bytes_by_table(
        operations: &[ResolvedBatchOperation],
    ) -> BTreeMap<TableId, u64> {
        let mut bytes_by_table = BTreeMap::new();
        for operation in operations {
            let entry_bytes = Self::estimated_operation_size_bytes(operation);
            bytes_by_table
                .entry(operation.table_id)
                .and_modify(|bytes: &mut u64| *bytes = bytes.saturating_add(entry_bytes))
                .or_insert(entry_bytes);
        }
        bytes_by_table
    }

    fn estimated_operation_size_bytes(operation: &ResolvedBatchOperation) -> u64 {
        (operation.key.len()
            + encode_mvcc_key(&operation.key, CommitId::new(SequenceNumber::default())).len()
            + operation
                .value
                .as_ref()
                .map(value_size_bytes)
                .unwrap_or_default()) as u64
    }

    fn throttle_delay(bytes: u64, max_write_bytes_per_second: u64) -> Duration {
        if bytes == 0 || max_write_bytes_per_second == 0 {
            return Duration::ZERO;
        }

        let millis = ((bytes as u128).saturating_mul(1000))
            .div_ceil(max_write_bytes_per_second as u128) as u64;
        Duration::from_millis(millis)
    }

    fn flush_error_into_storage(error: FlushError) -> StorageError {
        match error {
            FlushError::Storage(error) => error,
            FlushError::Unimplemented(message) => StorageError::unsupported(message),
        }
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

    fn mark_commits_durable_through(&self, upper_bound: SequenceNumber) {
        let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
        for (&sequence, state) in coordinator.sequences.iter_mut() {
            if !state.aborted && sequence <= upper_bound {
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
                let _commit_guard = self.inner.commit_lock.lock().await;
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
                let records = batch.staged_records();
                let result = self
                    .inner
                    .commit_runtime
                    .lock()
                    .await
                    .append_group_batch(&records)
                    .await;
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
                    advance.visible_sequence = Some(visible);
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
                    advance.durable_sequence = Some(durable);
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
        watchers: &Arc<WatermarkRegistry>,
        updates: &BTreeMap<String, SequenceNumber>,
    ) {
        watchers.notify(updates);
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

    async fn scan_change_feed(
        &self,
        table: &Table,
        cursor: LogCursor,
        upper_bound: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<ChangeStream, SnapshotTooOld> {
        let Some(table_id) = self.resolve_table_id(table) else {
            return Ok(Box::pin(stream::empty()));
        };
        if cursor.sequence() > upper_bound || matches!(opts.limit, Some(0)) {
            return Ok(Box::pin(stream::empty()));
        }
        let table_watermark = self.table_change_feed_watermark(table_id);

        let table_handle = Table {
            db: self.clone(),
            name: Arc::from(table.name()),
            id: Some(table_id),
        };
        let records = {
            let runtime = self.inner.commit_runtime.lock().await;
            let floor = self.change_feed_floor_from_state(
                table_id,
                upper_bound,
                runtime.oldest_sequence_for_table(table_id),
                runtime.oldest_segment_id(),
                table_watermark,
            );
            if let Some(oldest_available) = floor
                && cursor.sequence() < oldest_available
            {
                return Err(SnapshotTooOld {
                    requested: cursor.sequence(),
                    oldest_available,
                });
            }

            runtime
                .scan_table_from_sequence(table_id, cursor.sequence())
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "change capture scan failed for table {}: {error}",
                        table.name()
                    )
                })
        };

        let mut changes = Vec::new();
        for record in records {
            let sequence = record.sequence();
            if sequence > upper_bound {
                break;
            }

            for entry in record.entries {
                if entry.table_id != table_id {
                    continue;
                }

                let entry_cursor = LogCursor::new(sequence, entry.op_index);
                if entry_cursor <= cursor {
                    continue;
                }

                changes.push(ChangeEntry {
                    key: entry.key,
                    value: entry.value,
                    cursor: entry_cursor,
                    sequence,
                    kind: entry.kind,
                    table: table_handle.clone(),
                });

                if opts.limit.is_some_and(|limit| changes.len() >= limit) {
                    return Ok(Box::pin(stream::iter(changes)));
                }
            }
        }

        Ok(Box::pin(stream::iter(changes)))
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
    fn block_next_offload_phase(&self, phase: OffloadPhase) -> OffloadPhaseBlocker {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        mutex_lock(&self.inner.offload_phase_blocks).push(OffloadPhaseBlock {
            phase,
            reached_tx: Some(reached_tx),
            release_rx: Some(release_rx),
        });

        OffloadPhaseBlocker {
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

    #[cfg(test)]
    async fn maybe_pause_offload_phase(&self, phase: OffloadPhase) {
        let block = {
            let mut blocks = mutex_lock(&self.inner.offload_phase_blocks);
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
    async fn maybe_pause_offload_phase(&self, _phase: OffloadPhase) {}

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
                    let value = Self::normalize_value_for_table(&stored, value)
                        .map_err(CommitError::Storage)?;
                    Ok(ResolvedBatchOperation {
                        table_id: stored.id,
                        table_name: table.name().to_string(),
                        key: key.clone(),
                        kind: ChangeKind::Put,
                        value: Some(value),
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
                    let value = Self::normalize_merge_operand_for_table(&stored, value)
                        .map_err(CommitError::Storage)?;
                    Ok(ResolvedBatchOperation {
                        table_id: stored.id,
                        table_name: table.name().to_string(),
                        key: key.clone(),
                        kind: ChangeKind::Merge,
                        value: Some(value),
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

    fn read_visible_value_row(
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

    fn scan_visible_row(
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
            let Some(value) = self.read_visible_value_row(table_id, &key, sequence)? else {
                continue;
            };

            rows.push((key, value));
            if rows.len() >= limit {
                break;
            }
        }

        Ok(rows)
    }

    fn resolve_scan_projection(
        table: &StoredTable,
        requested_columns: Option<&[String]>,
    ) -> Result<Option<ColumnProjection>, StorageError> {
        let Some(schema) = table.config.schema.as_ref() else {
            return Ok(None);
        };

        let fields = if let Some(requested_columns) = requested_columns {
            let validation = SchemaValidation::new(schema)?;
            let mut seen = BTreeSet::new();
            let mut fields = Vec::with_capacity(requested_columns.len());
            for column_name in requested_columns {
                let field_id = validation
                    .field_ids_by_name
                    .get(column_name)
                    .copied()
                    .ok_or_else(|| {
                        StorageError::unsupported(format!(
                            "columnar table {} does not contain column {}",
                            table.config.name, column_name
                        ))
                    })?;
                if !seen.insert(field_id) {
                    continue;
                }
                let field = validation.fields_by_id.get(&field_id).ok_or_else(|| {
                    StorageError::corruption(format!(
                        "columnar table {} schema is missing field id {}",
                        table.config.name,
                        field_id.get()
                    ))
                })?;
                fields.push((*field).clone());
            }
            fields
        } else {
            schema.fields.clone()
        };

        Ok(Some(ColumnProjection { fields }))
    }

    fn missing_columnar_projection_value(
        field: &FieldDefinition,
        kind: ChangeKind,
    ) -> Result<FieldValue, StorageError> {
        match kind {
            ChangeKind::Merge => Ok(FieldValue::Null),
            ChangeKind::Put | ChangeKind::Delete => missing_field_value_for_definition(field),
        }
    }

    fn project_columnar_value(
        value: &Value,
        projection: &ColumnProjection,
        kind: ChangeKind,
    ) -> Result<Value, StorageError> {
        let Value::Record(record) = value else {
            return Err(StorageError::corruption(
                "columnar value is stored as bytes during read projection",
            ));
        };

        let mut projected = ColumnarRecord::new();
        for field in &projection.fields {
            let value = match record.get(&field.id) {
                Some(value) => value.clone(),
                None => Self::missing_columnar_projection_value(field, kind)?,
            };
            projected.insert(field.id, value);
        }

        Ok(Value::Record(projected))
    }

    fn columnar_overwritten_history_error(table: &StoredTable, key: &[u8]) -> StorageError {
        StorageError::unsupported(format!(
            "columnar table {} does not support historical overwritten-key reads in v1 (key {:?})",
            table.config.name, key
        ))
    }

    fn materialized_columnar_value(
        materialized_by_sstable: &BTreeMap<String, BTreeMap<usize, Value>>,
        row: &ColumnarRowRef,
    ) -> Result<Value, StorageError> {
        let values = materialized_by_sstable.get(&row.local_id).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} materialization is missing",
                row.local_id
            ))
        })?;
        values.get(&row.row_index).cloned().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} row {} was not materialized",
                row.local_id, row.row_index
            ))
        })
    }

    async fn resolve_visible_value_columnar_with_state(
        &self,
        table: &StoredTable,
        memtables: &MemtableState,
        sstables: &SstableState,
        key: &[u8],
        sequence: SequenceNumber,
        projection: &ColumnProjection,
    ) -> Result<VisibleValueResolution, StorageError> {
        let mut mem_rows = Vec::new();
        memtables.collect_visible_rows(table.id, key, sequence, &mut mem_rows);

        let mut sstables_by_local_id = BTreeMap::new();
        let mut columnar_rows = Vec::new();
        for sstable in sstables
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table.id && sstable.is_columnar())
        {
            sstables_by_local_id.insert(sstable.meta.local_id.clone(), sstable.clone());
            columnar_rows.extend(
                sstable
                    .collect_visible_row_refs_for_key_columnar(
                        &self.inner.dependencies,
                        key,
                        sequence,
                    )
                    .await?,
            );
        }

        enum VisibleCandidate {
            Memtable(SstableRow),
            Columnar(ColumnarRowRef),
        }

        let mut candidates = mem_rows
            .into_iter()
            .map(VisibleCandidate::Memtable)
            .chain(columnar_rows.into_iter().map(VisibleCandidate::Columnar))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            let (left_sequence, left_kind) = match left {
                VisibleCandidate::Memtable(row) => (row.sequence, row.kind),
                VisibleCandidate::Columnar(row) => (row.sequence, row.kind),
            };
            let (right_sequence, right_kind) = match right {
                VisibleCandidate::Memtable(row) => (row.sequence, row.kind),
                VisibleCandidate::Columnar(row) => (row.sequence, row.kind),
            };
            right_sequence.cmp(&left_sequence).then_with(|| {
                Self::visible_row_priority(left_kind).cmp(&Self::visible_row_priority(right_kind))
            })
        });

        let Some(head) = candidates.first() else {
            return Ok(VisibleValueResolution {
                value: None,
                collapse: None,
            });
        };
        let head_sequence = match head {
            VisibleCandidate::Memtable(row) => row.sequence,
            VisibleCandidate::Columnar(row) => row.sequence,
        };

        match head {
            VisibleCandidate::Memtable(row) => match row.kind {
                ChangeKind::Put => {
                    let value = row
                        .value
                        .as_ref()
                        .ok_or_else(|| StorageError::corruption("put row is missing a value"))?;
                    return Ok(VisibleValueResolution {
                        value: Some(Self::project_columnar_value(
                            value,
                            projection,
                            ChangeKind::Put,
                        )?),
                        collapse: None,
                    });
                }
                ChangeKind::Delete => {
                    return Ok(VisibleValueResolution {
                        value: None,
                        collapse: None,
                    });
                }
                ChangeKind::Merge => {}
            },
            VisibleCandidate::Columnar(row) => match row.kind {
                ChangeKind::Put => {
                    let sstable = sstables_by_local_id.get(&row.local_id).ok_or_else(|| {
                        StorageError::corruption(format!(
                            "columnar SSTable {} disappeared during point read",
                            row.local_id
                        ))
                    })?;
                    let values = sstable
                        .materialize_columnar_rows(
                            &self.inner.dependencies,
                            projection,
                            &BTreeSet::from([row.row_index]),
                        )
                        .await?;
                    return Ok(VisibleValueResolution {
                        value: values.get(&row.row_index).cloned(),
                        collapse: None,
                    });
                }
                ChangeKind::Delete => {
                    return Ok(VisibleValueResolution {
                        value: None,
                        collapse: None,
                    });
                }
                ChangeKind::Merge => {}
            },
        }

        let operator = table.config.merge_operator.as_ref().ok_or_else(|| {
            StorageError::unsupported(format!(
                "merge operator is not configured for table {}",
                table.config.name
            ))
        })?;
        let full_projection = Self::resolve_scan_projection(table, None)?
            .ok_or_else(|| StorageError::corruption("columnar table is missing a schema"))?;
        let mut needed_by_sstable = BTreeMap::<String, BTreeSet<usize>>::new();
        for candidate in &candidates {
            match candidate {
                VisibleCandidate::Memtable(row) => {
                    if matches!(row.kind, ChangeKind::Put | ChangeKind::Delete) {
                        break;
                    }
                }
                VisibleCandidate::Columnar(row) => {
                    if row.kind != ChangeKind::Delete {
                        needed_by_sstable
                            .entry(row.local_id.clone())
                            .or_default()
                            .insert(row.row_index);
                    }
                    if row.kind != ChangeKind::Merge {
                        break;
                    }
                }
            }
        }

        let mut materialized_by_sstable = BTreeMap::<String, BTreeMap<usize, Value>>::new();
        for (local_id, row_indexes) in needed_by_sstable {
            let sstable = sstables_by_local_id.get(&local_id).ok_or_else(|| {
                StorageError::corruption(format!(
                    "columnar SSTable {} disappeared during merge resolution",
                    local_id
                ))
            })?;
            materialized_by_sstable.insert(
                local_id.clone(),
                sstable
                    .materialize_columnar_rows(
                        &self.inner.dependencies,
                        &full_projection,
                        &row_indexes,
                    )
                    .await?,
            );
        }

        let mut operands = Vec::new();
        let mut existing_owned = None;
        for candidate in &candidates {
            match candidate {
                VisibleCandidate::Memtable(row) => match row.kind {
                    ChangeKind::Merge => operands.push(Self::project_columnar_value(
                        row.value.as_ref().ok_or_else(|| {
                            StorageError::corruption("merge row is missing an operand value")
                        })?,
                        &full_projection,
                        ChangeKind::Merge,
                    )?),
                    ChangeKind::Put => {
                        existing_owned = Some(Self::project_columnar_value(
                            row.value.as_ref().ok_or_else(|| {
                                StorageError::corruption("put row is missing a value")
                            })?,
                            &full_projection,
                            ChangeKind::Put,
                        )?);
                        break;
                    }
                    ChangeKind::Delete => break,
                },
                VisibleCandidate::Columnar(row) => match row.kind {
                    ChangeKind::Merge => {
                        operands.push(Self::materialized_columnar_value(
                            &materialized_by_sstable,
                            row,
                        )?);
                    }
                    ChangeKind::Put => {
                        existing_owned = Some(Self::materialized_columnar_value(
                            &materialized_by_sstable,
                            row,
                        )?);
                        break;
                    }
                    ChangeKind::Delete => break,
                },
            }
        }

        operands.reverse();
        let collapsed = Self::collapse_merge_operands(operator.as_ref(), key, &operands)?;
        let full_value = Self::normalize_value_for_table(
            table,
            &operator.full_merge(key, existing_owned.as_ref(), &collapsed)?,
        )?;
        let collapse =
            (operands.len() > Self::max_merge_operand_chain_length(&table.config)).then(|| {
                MergeCollapse {
                    sequence: head_sequence,
                    value: full_value.clone(),
                }
            });

        Ok(VisibleValueResolution {
            value: Some(Self::project_columnar_value(
                &full_value,
                projection,
                ChangeKind::Put,
            )?),
            collapse,
        })
    }

    async fn read_visible_value(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<Value>, StorageError> {
        let table = Self::stored_table_by_id(&self.tables_read(), table_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("table id {} is not registered", table_id.get()))
            })?;
        if table.config.format == TableFormat::Row {
            return self.read_visible_value_row(table_id, key, sequence);
        }

        let projection = Self::resolve_scan_projection(&table, None)?
            .ok_or_else(|| StorageError::corruption("columnar table is missing a schema"))?;
        let memtables = self.memtables_read().clone();
        let sstables = self.sstables_read().clone();
        let current_sequence = self.current_sequence();

        let mut persisted_version_count = 0_usize;
        for sstable in sstables
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table_id && sstable.is_columnar())
        {
            if sequence < current_sequence {
                persisted_version_count += sstable
                    .collect_visible_row_refs_for_key_columnar(
                        &self.inner.dependencies,
                        key,
                        current_sequence,
                    )
                    .await?
                    .len();
            }
        }

        if sequence < current_sequence && persisted_version_count > 1 {
            return Err(Self::columnar_overwritten_history_error(&table, key));
        }
        let resolution = self
            .resolve_visible_value_columnar_with_state(
                &table,
                &memtables,
                &sstables,
                key,
                sequence,
                &projection,
            )
            .await?;
        if let Some(collapse) = resolution.collapse.clone() {
            self.force_collapse_merge_chain(table_id, key, collapse);
        }

        Ok(resolution.value)
    }

    async fn scan_visible(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
        matcher: KeyMatcher<'_>,
        opts: &ScanOptions,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let table = Self::stored_table_by_id(&self.tables_read(), table_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("table id {} is not registered", table_id.get()))
            })?;
        if table.config.format == TableFormat::Row {
            return self.scan_visible_row(table_id, sequence, matcher, opts);
        }

        let limit = opts.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let projection = Self::resolve_scan_projection(&table, opts.columns.as_deref())?
            .ok_or_else(|| StorageError::corruption("columnar table is missing a schema"))?;
        let memtables = self.memtables_read().clone();
        let sstables = self.sstables_read().clone();
        let current_sequence = self.current_sequence();

        let mut keys = BTreeSet::new();
        memtables.collect_matching_keys(table_id, &matcher, &mut keys);

        let mut persisted_versions_by_key = BTreeMap::<Key, usize>::new();
        for sstable in sstables
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table_id && sstable.is_columnar())
        {
            for row in sstable
                .collect_scan_row_refs_columnar(
                    &self.inner.dependencies,
                    &matcher,
                    if sequence < current_sequence {
                        current_sequence
                    } else {
                        sequence
                    },
                )
                .await?
            {
                if sequence < current_sequence {
                    *persisted_versions_by_key
                        .entry(row.key.clone())
                        .or_default() += 1;
                }
                if row.sequence <= sequence {
                    keys.insert(row.key.clone());
                }
            }
        }

        let mut ordered_keys = keys.into_iter().collect::<Vec<_>>();
        if opts.reverse {
            ordered_keys.reverse();
        }

        let mut rows = Vec::new();
        for key in ordered_keys {
            if sequence < current_sequence
                && persisted_versions_by_key
                    .get(&key)
                    .copied()
                    .unwrap_or_default()
                    > 1
            {
                return Err(Self::columnar_overwritten_history_error(&table, &key));
            }
            let resolution = self
                .resolve_visible_value_columnar_with_state(
                    &table,
                    &memtables,
                    &sstables,
                    &key,
                    sequence,
                    &projection,
                )
                .await?;
            if let Some(collapse) = resolution.collapse.clone() {
                self.force_collapse_merge_chain(table_id, &key, collapse);
            }
            let Some(value) = resolution.value else {
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

    fn record_compaction_filter_stats(
        &self,
        table_id: TableId,
        removed_bytes: u64,
        removed_keys: u64,
    ) {
        if removed_bytes == 0 && removed_keys == 0 {
            return;
        }

        let mut stats = mutex_lock(&self.inner.compaction_filter_stats);
        let entry = stats.entry(table_id).or_default();
        entry.removed_bytes = entry.removed_bytes.saturating_add(removed_bytes);
        entry.removed_keys = entry.removed_keys.saturating_add(removed_keys);
    }

    fn compaction_filter_stats(&self, table_id: TableId) -> CompactionFilterStats {
        mutex_lock(&self.inner.compaction_filter_stats)
            .get(&table_id)
            .copied()
            .unwrap_or_default()
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

    fn cdc_retention_floor_sequence(
        &self,
        table_id: TableId,
        upper_bound: SequenceNumber,
    ) -> Option<SequenceNumber> {
        let retained = self.history_retention_sequences(table_id)?;
        Some(SequenceNumber::new(
            upper_bound.get().saturating_sub(retained.saturating_sub(1)),
        ))
    }

    fn cdc_gc_min_sequence(&self, upper_bound: SequenceNumber) -> Option<SequenceNumber> {
        self.tables_read()
            .values()
            .filter_map(|table| self.cdc_retention_floor_sequence(table.id, upper_bound))
            .min()
    }

    fn table_change_feed_watermark(&self, table_id: TableId) -> Option<SequenceNumber> {
        let mut watermark = None;

        if let Some(sequence) = self
            .memtables_read()
            .table_watermarks()
            .get(&table_id)
            .copied()
        {
            watermark = Some(sequence);
        }
        if let Some(sequence) = self
            .sstables_read()
            .table_watermarks()
            .get(&table_id)
            .copied()
        {
            watermark = Some(
                watermark
                    .map(|current| current.max(sequence))
                    .unwrap_or(sequence),
            );
        }

        watermark
    }

    fn change_feed_floor_from_state(
        &self,
        table_id: TableId,
        upper_bound: SequenceNumber,
        physical_oldest: Option<SequenceNumber>,
        oldest_segment_id: Option<SegmentId>,
        table_watermark: Option<SequenceNumber>,
    ) -> Option<SequenceNumber> {
        let mut floor = self
            .cdc_retention_floor_sequence(table_id, upper_bound)
            .filter(|logical| physical_oldest.is_some_and(|oldest| oldest < *logical));

        if oldest_segment_id.is_some_and(|segment_id| segment_id.get() > 1)
            && let Some(physical_floor) = physical_oldest.or(table_watermark)
        {
            floor = Some(
                floor
                    .map(|current| current.max(physical_floor))
                    .unwrap_or(physical_floor),
            );
        }

        floor.filter(|sequence| sequence.get() > 0)
    }

    async fn prune_commit_log(&self, seal_active: bool) -> Result<(), StorageError> {
        if self.local_storage_root().is_none() {
            return Ok(());
        }

        let recovery_min = self.sstables_read().last_flushed_sequence;
        let cdc_min = self.cdc_gc_min_sequence(self.current_sequence());
        let gc_floor = cdc_min
            .map(|cdc_min| recovery_min.min(cdc_min))
            .unwrap_or(recovery_min);
        if gc_floor == SequenceNumber::new(0) {
            return Ok(());
        }

        let mut runtime = self.inner.commit_runtime.lock().await;
        if seal_active {
            runtime.maybe_seal_active().await?;
        }
        runtime.prune_segments_before(gc_floor).await
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

    #[cfg(test)]
    fn visible_subscriber_count(&self, table: &Table) -> usize {
        self.inner
            .visible_watchers
            .active_subscriber_count(table.name())
    }

    #[cfg(test)]
    fn durable_subscriber_count(&self, table: &Table) -> usize {
        self.inner
            .durable_watchers
            .active_subscriber_count(table.name())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn subscribe_set<'a, I>(
        registry: &Arc<WatermarkRegistry>,
        tables: I,
    ) -> WatermarkSubscriptionSet
    where
        I: IntoIterator<Item = &'a Table>,
    {
        WatermarkSubscriptionSet::new(
            registry,
            tables
                .into_iter()
                .map(|table| (table.name().to_string(), registry.subscribe(table.name())))
                .collect(),
        )
    }

    fn initial_table_watermarks(
        tables: &BTreeMap<String, StoredTable>,
        memtables: &MemtableState,
        sstables: &SstableState,
    ) -> BTreeMap<String, SequenceNumber> {
        let mut by_id = sstables.table_watermarks();
        for (table_id, sequence) in memtables.table_watermarks() {
            update_table_watermark(&mut by_id, table_id, sequence);
        }

        tables
            .iter()
            .filter_map(|(name, table)| {
                by_id
                    .get(&table.id)
                    .copied()
                    .map(|sequence| (name.clone(), sequence))
            })
            .collect()
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

        Ok(self.db.read_visible_value(table_id, &key, sequence).await?)
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

        let rows = self
            .db
            .scan_visible(
                table_id,
                sequence,
                KeyMatcher::Range {
                    start: &start,
                    end: &end,
                },
                &opts,
            )
            .await?;
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
            .scan_visible(table_id, sequence, KeyMatcher::Prefix(&prefix), &opts)
            .await?;
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

fn pending_work_sort_key(work: &PendingWork) -> (u8, &str, Option<u32>) {
    let priority = match work.work_type {
        PendingWorkType::Flush => 0,
        PendingWorkType::Compaction => 1,
        PendingWorkType::Backup => 2,
        PendingWorkType::Offload => 3,
    };
    (priority, work.table.as_str(), work.level)
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
    read_source(dependencies, &StorageSource::local_file(path)).await
}

async fn read_source(
    dependencies: &DbDependencies,
    source: &StorageSource,
) -> Result<Vec<u8>, StorageError> {
    UnifiedStorage::from_dependencies(dependencies)
        .read_all(source)
        .await
        .map_err(|error| error.into_storage_error())
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

async fn read_optional_remote_object(
    dependencies: &DbDependencies,
    key: &str,
) -> Result<Option<Vec<u8>>, StorageError> {
    match dependencies.object_store.get(key).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

async fn write_local_file_atomic(
    dependencies: &DbDependencies,
    path: &str,
    bytes: &[u8],
) -> Result<(), StorageError> {
    let temp_path = format!("{path}.tmp");
    let handle = dependencies
        .file_system
        .open(
            &temp_path,
            OpenOptions {
                create: true,
                read: true,
                write: true,
                truncate: true,
                append: false,
            },
        )
        .await?;
    dependencies.file_system.write_at(&handle, 0, bytes).await?;
    dependencies.file_system.sync(&handle).await?;
    dependencies.file_system.rename(&temp_path, path).await?;
    if let Some(parent) = PathBuf::from(path).parent()
        && !parent.as_os_str().is_empty()
    {
        dependencies
            .file_system
            .sync_dir(parent.to_string_lossy().as_ref())
            .await?;
    }
    Ok(())
}

async fn delete_local_file_if_exists(
    dependencies: &DbDependencies,
    path: &str,
) -> Result<(), StorageError> {
    match dependencies.file_system.delete(path).await {
        Ok(()) => {
            if let Some(parent) = PathBuf::from(path).parent()
                && !parent.as_os_str().is_empty()
            {
                dependencies
                    .file_system
                    .sync_dir(parent.to_string_lossy().as_ref())
                    .await?;
            }
            Ok(())
        }
        Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(()),
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
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use futures::StreamExt;
    use parking_lot::Mutex;
    use serde_json::json;

    use super::{
        COLUMNAR_SSTABLE_FORMAT_VERSION, COLUMNAR_SSTABLE_MAGIC, CommitPhase, CompactionJobKind,
        CompactionPhase, Db, LOCAL_CATALOG_RELATIVE_PATH, LOCAL_CATALOG_TEMP_SUFFIX,
        LOCAL_COMMIT_LOG_RELATIVE_DIR, LOCAL_MANIFEST_TEMP_SUFFIX, LOCAL_SSTABLE_RELATIVE_DIR,
        LOCAL_SSTABLE_SHARD_DIR, ManifestId, OffloadPhase, PendingWorkSpec,
        PersistedRowSstableFile, SchemaDefinition, StoredTable, WatermarkUpdate, decode_mvcc_key,
        encode_mvcc_key, read_path,
    };
    use crate::simulation::{
        CutPoint, PointMutation, SeededSimulationRunner, ShadowOracle, TraceEvent,
    };
    use crate::{
        ChangeKind, CommitError, CommitId, CommitOptions, CompactionStrategy, DbConfig,
        DbDependencies, FieldDefinition, FieldId, FieldType, FieldValue, FileSystem,
        FileSystemFailure, FileSystemOperation, LogCursor, MergeOperator, MergeOperatorRef,
        ObjectKeyLayout, ObjectStore, ObjectStoreFailure, ObjectStoreOperation, PendingWork,
        PendingWorkType, ReadError, Rng, S3Location, S3PrimaryStorageConfig, ScanOptions,
        ScheduleAction, ScheduleDecision, Scheduler, SegmentId, SequenceNumber, SnapshotTooOld,
        SsdConfig, StorageConfig, StorageError, StorageErrorKind, StubClock, StubObjectStore,
        StubRng, TableConfig, TableFormat, TableId, TableStats, ThrottleDecision,
        TieredDurabilityMode, TieredStorageConfig, Timestamp, TtlCompactionFilter, Value,
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

    fn tiered_layout() -> ObjectKeyLayout {
        ObjectKeyLayout::new(&S3Location {
            bucket: "terracedb-test".to_string(),
            prefix: "tiered".to_string(),
        })
    }

    fn tiered_config_with_max_local_bytes(path: &str, max_local_bytes: u64) -> DbConfig {
        DbConfig {
            storage: StorageConfig::Tiered(TieredStorageConfig {
                ssd: SsdConfig {
                    path: path.to_string(),
                },
                s3: S3Location {
                    bucket: "terracedb-test".to_string(),
                    prefix: "tiered".to_string(),
                },
                max_local_bytes,
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
        dependencies_with_clock(file_system, object_store, Arc::new(StubClock::default()))
    }

    fn dependencies_with_clock(
        file_system: Arc<crate::StubFileSystem>,
        object_store: Arc<StubObjectStore>,
        clock: Arc<StubClock>,
    ) -> DbDependencies {
        DbDependencies::new(
            file_system,
            object_store,
            clock,
            Arc::new(StubRng::seeded(7)),
        )
    }

    #[derive(Clone)]
    struct RecordingObjectStore {
        inner: Arc<StubObjectStore>,
        get_calls: Arc<Mutex<Vec<String>>>,
        range_calls: Arc<Mutex<Vec<(String, u64, u64)>>>,
    }

    impl RecordingObjectStore {
        fn new(inner: Arc<StubObjectStore>) -> Self {
            Self {
                inner,
                get_calls: Arc::new(Mutex::new(Vec::new())),
                range_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn clear_calls(&self) {
            self.get_calls.lock().clear();
            self.range_calls.lock().clear();
        }

        fn get_calls(&self) -> Vec<String> {
            self.get_calls.lock().clone()
        }

        fn range_calls(&self) -> Vec<(String, u64, u64)> {
            self.range_calls.lock().clone()
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingObjectStore {
        async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
            self.inner.put(key, data).await
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            self.get_calls.lock().push(key.to_string());
            self.inner.get(key).await
        }

        async fn get_range(
            &self,
            key: &str,
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, StorageError> {
            self.range_calls.lock().push((key.to_string(), start, end));
            self.inner.get_range(key, start, end).await
        }

        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }

        async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
            self.inner.copy(from, to).await
        }
    }

    async fn install_columnar_schema_successor(
        db: &Db,
        table_name: &str,
        successor: SchemaDefinition,
    ) {
        let mut tables = db.tables_read().clone();
        let stored = tables
            .get_mut(table_name)
            .expect("table should exist for schema successor");
        let current = stored
            .config
            .schema
            .clone()
            .expect("columnar table should have a current schema");
        current
            .validate_successor(&successor)
            .expect("schema successor should be valid");
        stored.config.schema = Some(successor);
        db.persist_tables(&tables)
            .await
            .expect("persist schema successor");
        *db.tables_write() = tables;
    }

    fn tiered_config_with_scheduler(path: &str, scheduler: Arc<dyn Scheduler>) -> DbConfig {
        let mut config = tiered_config(path);
        config.scheduler = Some(scheduler);
        config
    }

    fn tiered_config_with_limits_and_scheduler(
        path: &str,
        max_local_bytes: u64,
        scheduler: Arc<dyn Scheduler>,
    ) -> DbConfig {
        let mut config = tiered_config_with_max_local_bytes(path, max_local_bytes);
        config.scheduler = Some(scheduler);
        config
    }

    #[derive(Default)]
    struct RefusingScheduler;

    impl Scheduler for RefusingScheduler {
        fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
            work.iter()
                .map(|work| ScheduleDecision {
                    work_id: work.id.clone(),
                    action: ScheduleAction::Defer,
                })
                .collect()
        }

        fn should_throttle(&self, _table: &crate::Table, _stats: &TableStats) -> ThrottleDecision {
            ThrottleDecision::default()
        }
    }

    struct PreferredTableScheduler {
        preferred_table: String,
    }

    impl Scheduler for PreferredTableScheduler {
        fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
            let selected = work
                .iter()
                .find(|item| item.table == self.preferred_table)
                .map(|item| item.id.clone());
            work.iter()
                .map(|item| ScheduleDecision {
                    work_id: item.id.clone(),
                    action: if selected.as_ref() == Some(&item.id) {
                        ScheduleAction::Execute
                    } else {
                        ScheduleAction::Defer
                    },
                })
                .collect()
        }

        fn should_throttle(&self, _table: &crate::Table, _stats: &TableStats) -> ThrottleDecision {
            ThrottleDecision::default()
        }
    }

    #[derive(Default)]
    struct MetadataRateLimitScheduler {
        seen: Mutex<Vec<(String, BTreeMap<String, serde_json::Value>)>>,
        max_write_bytes_per_second: u64,
    }

    impl MetadataRateLimitScheduler {
        fn seen(&self) -> Vec<(String, BTreeMap<String, serde_json::Value>)> {
            self.seen.lock().clone()
        }
    }

    impl Scheduler for MetadataRateLimitScheduler {
        fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
            work.iter()
                .map(|work| ScheduleDecision {
                    work_id: work.id.clone(),
                    action: ScheduleAction::Defer,
                })
                .collect()
        }

        fn should_throttle(&self, table: &crate::Table, stats: &TableStats) -> ThrottleDecision {
            self.seen
                .lock()
                .push((table.name().to_string(), stats.metadata.clone()));
            ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: Some(self.max_write_bytes_per_second),
                stall: false,
            }
        }
    }

    #[derive(Default)]
    struct DeferringThrottleScheduler;

    impl Scheduler for DeferringThrottleScheduler {
        fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
            work.iter()
                .map(|work| ScheduleDecision {
                    work_id: work.id.clone(),
                    action: ScheduleAction::Defer,
                })
                .collect()
        }

        fn should_throttle(&self, _table: &crate::Table, _stats: &TableStats) -> ThrottleDecision {
            if _stats.l0_sstable_count >= crate::DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT {
                return ThrottleDecision {
                    throttle: true,
                    max_write_bytes_per_second: None,
                    stall: false,
                };
            }

            ThrottleDecision::default()
        }
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

    #[derive(Debug)]
    struct SumColumnarCountMergeOperator;

    impl MergeOperator for SumColumnarCountMergeOperator {
        fn full_merge(
            &self,
            _key: &[u8],
            existing: Option<&Value>,
            operands: &[Value],
        ) -> Result<Value, StorageError> {
            let mut total = existing
                .map(count_from_record_value)
                .transpose()?
                .unwrap_or(0);
            for operand in operands {
                total += count_from_record_value(operand)?;
            }
            Ok(count_record(total))
        }

        fn partial_merge(
            &self,
            _key: &[u8],
            left: &Value,
            right: &Value,
        ) -> Result<Option<Value>, StorageError> {
            Ok(Some(count_record(
                count_from_record_value(left)? + count_from_record_value(right)?,
            )))
        }
    }

    fn sum_columnar_count_merge_operator() -> MergeOperatorRef {
        Arc::new(SumColumnarCountMergeOperator)
    }

    fn count_record(count: i64) -> Value {
        Value::record(BTreeMap::from([(
            FieldId::new(1),
            FieldValue::Int64(count),
        )]))
    }

    fn count_from_record_value(value: &Value) -> Result<i64, StorageError> {
        let Value::Record(record) = value else {
            return Err(StorageError::unsupported(
                "count merge operator only supports record values",
            ));
        };
        match record.get(&FieldId::new(1)) {
            Some(FieldValue::Int64(count)) => Ok(*count),
            Some(FieldValue::Null) | None => Ok(0),
            Some(_) => Err(StorageError::unsupported(
                "count merge operator expects an int64 count field",
            )),
        }
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

    fn merge_columnar_table_config(
        name: &str,
        max_merge_operand_chain_length: Option<u32>,
    ) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            format: TableFormat::Columnar,
            merge_operator: Some(sum_columnar_count_merge_operator()),
            max_merge_operand_chain_length,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(6),
            history_retention_sequences: Some(30),
            compaction_strategy: CompactionStrategy::Tiered,
            schema: Some(SchemaDefinition {
                version: 1,
                fields: vec![FieldDefinition {
                    id: FieldId::new(1),
                    name: "count".to_string(),
                    field_type: FieldType::Int64,
                    nullable: false,
                    default: Some(FieldValue::Int64(0)),
                }],
            }),
            metadata: BTreeMap::from([("mode".to_string(), json!("merge"))]),
        }
    }

    fn bytes(value: &str) -> Value {
        Value::bytes(value.as_bytes().to_vec())
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum MergePointMutation {
        Put { key: Vec<u8>, value: Vec<u8> },
        Merge { key: Vec<u8>, value: Vec<u8> },
        Delete { key: Vec<u8> },
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct MergePointVersion {
        sequence: SequenceNumber,
        mutation: MergePointMutation,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct MergeShadowOracle {
        versions: BTreeMap<Vec<u8>, Vec<MergePointVersion>>,
    }

    impl MergeShadowOracle {
        fn apply(&mut self, sequence: SequenceNumber, mutation: MergePointMutation) {
            let key = match &mutation {
                MergePointMutation::Put { key, .. }
                | MergePointMutation::Merge { key, .. }
                | MergePointMutation::Delete { key } => key.clone(),
            };
            self.versions
                .entry(key)
                .or_default()
                .push(MergePointVersion { sequence, mutation });
        }

        fn value_at(&self, key: &[u8], sequence: SequenceNumber) -> Option<Value> {
            let versions = self.versions.get(key)?;
            let operator = AppendMergeOperator;
            let mut operands = Vec::new();
            let mut existing = None;

            for version in versions.iter().rev() {
                if version.sequence > sequence {
                    continue;
                }
                match &version.mutation {
                    MergePointMutation::Put { value, .. } => {
                        existing = Some(Value::bytes(value.clone()));
                        break;
                    }
                    MergePointMutation::Merge { value, .. } => {
                        operands.push(Value::bytes(value.clone()));
                    }
                    MergePointMutation::Delete { .. } => break,
                }
            }

            match (existing, operands.is_empty()) {
                (Some(value), true) => Some(value),
                (None, true) => None,
                (existing, false) => {
                    operands.reverse();
                    let collapsed =
                        collapse_merge_operands_for_oracle(&operator, key, &operands).ok()?;
                    operator.full_merge(key, existing.as_ref(), &collapsed).ok()
                }
            }
        }

        fn point_state_at(&self, sequence: SequenceNumber) -> BTreeMap<Vec<u8>, Value> {
            let mut state = BTreeMap::new();
            for key in self.versions.keys() {
                if let Some(value) = self.value_at(key, sequence) {
                    state.insert(key.clone(), value);
                }
            }
            state
        }
    }

    fn collapse_merge_operands_for_oracle(
        operator: &dyn MergeOperator,
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

    fn expiry_prefixed_value(expires_at: u64, payload: &str) -> Value {
        let mut encoded = expires_at.to_be_bytes().to_vec();
        encoded.extend_from_slice(payload.as_bytes());
        Value::Bytes(encoded)
    }

    fn expiry_from_prefixed_bytes(value: &Value) -> Option<Timestamp> {
        let Value::Bytes(bytes) = value else {
            return None;
        };
        let prefix = bytes.get(..8)?;
        Some(Timestamp::new(u64::from_be_bytes(prefix.try_into().ok()?)))
    }

    fn ttl_row_table_config(name: &str) -> TableConfig {
        let mut config = row_table_config(name);
        config.compaction_filter = Some(Arc::new(TtlCompactionFilter::new(
            expiry_from_prefixed_bytes,
        )));
        config
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

    fn columnar_all_types_table_config(name: &str) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            format: TableFormat::Columnar,
            merge_operator: None,
            max_merge_operand_chain_length: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(7),
            history_retention_sequences: Some(16),
            compaction_strategy: CompactionStrategy::Tiered,
            schema: Some(SchemaDefinition {
                version: 4,
                fields: vec![
                    FieldDefinition {
                        id: FieldId::new(1),
                        name: "metric".to_string(),
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
                    FieldDefinition {
                        id: FieldId::new(3),
                        name: "ratio".to_string(),
                        field_type: FieldType::Float64,
                        nullable: true,
                        default: None,
                    },
                    FieldDefinition {
                        id: FieldId::new(4),
                        name: "payload".to_string(),
                        field_type: FieldType::Bytes,
                        nullable: true,
                        default: None,
                    },
                    FieldDefinition {
                        id: FieldId::new(5),
                        name: "active".to_string(),
                        field_type: FieldType::Bool,
                        nullable: false,
                        default: Some(FieldValue::Bool(false)),
                    },
                ],
            }),
            metadata: BTreeMap::from([("retention".to_string(), json!("16h"))]),
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

    #[derive(Clone, Debug, PartialEq)]
    struct CollectedChange {
        sequence: SequenceNumber,
        cursor: LogCursor,
        kind: ChangeKind,
        key: crate::Key,
        value: Option<Value>,
        table: String,
    }

    async fn collect_changes(stream: crate::ChangeStream) -> Vec<CollectedChange> {
        stream
            .map(|entry| CollectedChange {
                sequence: entry.sequence,
                cursor: entry.cursor,
                kind: entry.kind,
                key: entry.key,
                value: entry.value,
                table: entry.table.name().to_string(),
            })
            .collect::<Vec<_>>()
            .await
    }

    fn collected_change(
        sequence: u64,
        op_index: u16,
        kind: ChangeKind,
        key: &[u8],
        value: Option<Value>,
        table: &str,
    ) -> CollectedChange {
        CollectedChange {
            sequence: SequenceNumber::new(sequence),
            cursor: LogCursor::new(SequenceNumber::new(sequence), op_index),
            kind,
            key: key.to_vec(),
            value,
            table: table.to_string(),
        }
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

    fn assert_change_feed_snapshot_too_old(
        error: SnapshotTooOld,
        requested: SequenceNumber,
        oldest_available: SequenceNumber,
    ) {
        assert_eq!(error.requested, requested);
        assert_eq!(error.oldest_available, oldest_available);
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

    fn merge_oracle_rows(
        oracle: &MergeShadowOracle,
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
            .collect::<Vec<_>>();

        if reverse {
            rows.reverse();
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }

        rows
    }

    fn merge_oracle_prefix_rows(
        oracle: &MergeShadowOracle,
        sequence: SequenceNumber,
        prefix: &[u8],
        reverse: bool,
        limit: Option<usize>,
    ) -> Vec<(crate::Key, Value)> {
        let mut rows = oracle
            .point_state_at(sequence)
            .into_iter()
            .filter(|(key, _value)| key.starts_with(prefix))
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

    fn assert_group_commit_sync_failure(error: crate::WriteError) {
        let crate::WriteError::Commit(CommitError::Storage(storage)) = error else {
            panic!("expected storage-backed write error from failed group commit");
        };
        assert_eq!(storage.kind(), StorageErrorKind::Timeout);
        assert!(storage.message().contains("simulated timeout"));
    }

    async fn visible_changes(db: &Db, table: &crate::Table) -> Vec<CollectedChange> {
        collect_changes(
            db.scan_since(table, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("scan visible changes"),
        )
        .await
    }

    async fn durable_changes(db: &Db, table: &crate::Table) -> Vec<CollectedChange> {
        collect_changes(
            db.scan_durable_since(table, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("scan durable changes"),
        )
        .await
    }

    async fn assert_no_visible_or_durable_changes(db: &Db, table: &crate::Table) {
        assert!(visible_changes(db, table).await.is_empty());
        assert!(durable_changes(db, table).await.is_empty());
    }

    async fn commit_log_segment_ids(db: &Db) -> Vec<SegmentId> {
        db.inner
            .commit_runtime
            .lock()
            .await
            .enumerate_segments()
            .into_iter()
            .map(|segment| segment.segment_id)
            .collect()
    }

    async fn force_seal_commit_log(db: &Db) {
        db.inner
            .commit_runtime
            .lock()
            .await
            .maybe_seal_active()
            .await
            .expect("seal active commit log segment");
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

    async fn seed_columnar_compaction_fixture(
        root: &str,
    ) -> (
        Db,
        crate::Table,
        Arc<crate::StubFileSystem>,
        DbDependencies,
        SchemaDefinition,
    ) {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);
        let db = Db::open(tiered_config(root), dependencies.clone())
            .await
            .expect("open columnar compaction db");
        let config = columnar_table_config("metrics");
        let schema = config.schema.clone().expect("columnar schema");
        let table = db
            .create_table(config)
            .await
            .expect("create columnar compaction table");

        for (key, user_id, count) in [
            (b"user:1".to_vec(), "alice", 1),
            (b"user:2".to_vec(), "bob", 2),
            (b"user:3".to_vec(), "carol", 3),
        ] {
            table
                .write(
                    key,
                    Value::named_record(
                        &schema,
                        [
                            ("user_id", FieldValue::String(user_id.to_string())),
                            ("count", FieldValue::Int64(count)),
                        ],
                    )
                    .expect("encode columnar fixture row"),
                )
                .await
                .expect("write columnar fixture row");
            db.flush().await.expect("flush columnar fixture row");
        }

        (db, table, file_system, dependencies, schema)
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

    async fn seed_offload_fixture(
        root: &str,
        max_local_bytes: u64,
    ) -> (
        Db,
        crate::Table,
        Arc<crate::StubFileSystem>,
        Arc<StubObjectStore>,
        DbDependencies,
        SequenceNumber,
    ) {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store.clone());
        let db = Db::open(
            tiered_config_with_max_local_bytes(root, 1024 * 1024),
            dependencies.clone(),
        )
        .await
        .expect("open offload db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create offload table");

        let first = table
            .write(b"apple".to_vec(), Value::Bytes(vec![b'a'; 256]))
            .await
            .expect("write first offload row");
        db.flush().await.expect("flush first offload sstable");

        let second = table
            .write(b"banana".to_vec(), Value::Bytes(vec![b'b'; 256]))
            .await
            .expect("write second offload row");
        db.flush().await.expect("flush second offload sstable");

        assert!(second > first);

        drop(table);
        drop(db);

        let reopened = Db::open(
            tiered_config_with_max_local_bytes(root, max_local_bytes),
            dependencies.clone(),
        )
        .await
        .expect("reopen offload fixture with target budget");
        let reopened_table = reopened.table("events");

        (
            reopened,
            reopened_table,
            file_system,
            object_store,
            dependencies,
            first,
        )
    }

    #[tokio::test]
    async fn pending_work_reports_flush_and_compaction_backlog() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/scheduler-pending-work"),
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
        db.memtables_write().rotate_mutable();

        let flush_stats = db.table_stats(&table).await;
        assert_eq!(flush_stats.immutable_memtable_count, 1);
        assert!(flush_stats.pending_flush_bytes > 0);
        let flush_work = db.pending_work().await;
        assert!(flush_work.iter().any(|work| {
            work.work_type == PendingWorkType::Flush
                && work.table == "events"
                && work.estimated_bytes > 0
        }));

        db.flush().await.expect("flush immutable backlog");
        table
            .write(b"user:2".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second value");
        db.flush().await.expect("flush second l0");

        let compaction_work = db.pending_work().await;
        assert!(compaction_work.iter().any(|work| {
            work.work_type == PendingWorkType::Compaction && work.table == "events"
        }));
    }

    #[tokio::test]
    async fn scheduler_receives_metadata_untouched_and_rate_limits_writes() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let clock = Arc::new(StubClock::default());
        let scheduler = Arc::new(MetadataRateLimitScheduler {
            seen: Mutex::new(Vec::new()),
            max_write_bytes_per_second: 1,
        });
        let db = Db::open(
            tiered_config_with_scheduler("/scheduler-metadata", scheduler.clone()),
            dependencies_with_clock(file_system, object_store, clock.clone()),
        )
        .await
        .expect("open db");
        let config = row_table_config("events");
        let expected_metadata = config.metadata.clone();
        let table = db.create_table(config).await.expect("create table");

        let write_table = table.clone();
        let write = tokio::spawn(async move {
            write_table
                .write(b"user:1".to_vec(), Value::Bytes(vec![b'x'; 64]))
                .await
                .expect("rate-limited write")
        });

        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(
            !write.is_finished(),
            "write should still be waiting on the clock"
        );

        let seen = scheduler.seen();
        assert!(
            seen.iter()
                .any(|(table_name, metadata)| table_name == "events"
                    && metadata == &expected_metadata)
        );

        for _ in 0..180 {
            if write.is_finished() {
                break;
            }
            clock.advance(Duration::from_secs(1));
            tokio::task::yield_now().await;
        }
        assert!(
            write.is_finished(),
            "write should finish once the simulated clock advances far enough"
        );
        assert_eq!(
            write.await.expect("join write task"),
            SequenceNumber::new(1)
        );
    }

    #[tokio::test]
    async fn scheduler_choice_controls_which_compaction_runs_first() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let scheduler = Arc::new(PreferredTableScheduler {
            preferred_table: "beta".to_string(),
        });
        let db = Db::open(
            tiered_config_with_scheduler("/scheduler-priority", scheduler),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let alpha = db
            .create_table(row_table_config("alpha"))
            .await
            .expect("create alpha");
        let beta = db
            .create_table(row_table_config("beta"))
            .await
            .expect("create beta");

        for round in 0..2_u8 {
            alpha
                .write(vec![b'a', round], Value::Bytes(vec![round]))
                .await
                .expect("write alpha");
            beta.write(vec![b'b', round], Value::Bytes(vec![round]))
                .await
                .expect("write beta");
            db.flush().await.expect("flush round");
        }

        assert!(db.table_stats(&alpha).await.compaction_debt > 0);
        assert!(db.table_stats(&beta).await.compaction_debt > 0);
        assert!(
            db.run_next_scheduled_work()
                .await
                .expect("run scheduled work")
        );

        assert!(db.table_stats(&alpha).await.compaction_debt > 0);
        assert_eq!(db.table_stats(&beta).await.compaction_debt, 0);
    }

    #[tokio::test]
    async fn forced_flush_ignores_scheduler_deferrals_when_memtable_budget_is_exhausted() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config_with_limits_and_scheduler(
                "/forced-flush-guardrail",
                160,
                Arc::new(RefusingScheduler),
            ),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"user:1".to_vec(), Value::Bytes(vec![b'x'; 80]))
            .await
            .expect("write first large value");
        assert_eq!(db.sstables_read().live.len(), 0);

        table
            .write(b"user:2".to_vec(), Value::Bytes(vec![b'y'; 80]))
            .await
            .expect("write second large value");

        let stats = db.table_stats(&table).await;
        assert_eq!(stats.immutable_memtable_count, 0);
        assert!(
            stats.local_bytes > 0,
            "forced flush should install SSTables"
        );
        assert!(!db.sstables_read().live.is_empty());
    }

    #[tokio::test]
    async fn forced_l0_compaction_ignores_scheduler_deferrals_at_the_hard_ceiling() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config_with_scheduler("/forced-l0-compaction", Arc::new(RefusingScheduler)),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        for index in 0..crate::DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT {
            table
                .write(format!("key-{index}").into_bytes(), Value::bytes("v"))
                .await
                .expect("write l0 seed");
            db.flush().await.expect("flush l0 seed");
        }

        assert_eq!(
            db.table_stats(&table).await.l0_sstable_count,
            crate::DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT
        );
        table
            .write(b"trigger".to_vec(), Value::bytes("v"))
            .await
            .expect("write through hard-ceiling guardrail");

        let stats = db.table_stats(&table).await;
        assert!(
            stats.l0_sstable_count < crate::DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT,
            "forced compaction should reduce L0 pressure before accepting more writes"
        );
    }

    #[tokio::test]
    async fn deferred_scheduler_work_is_forced_before_reaching_the_hard_ceiling() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config_with_scheduler(
                "/forced-deferred-work",
                Arc::new(DeferringThrottleScheduler),
            ),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        for index in 0..crate::DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT {
            table
                .write(format!("seed-{index}").into_bytes(), Value::bytes("v"))
                .await
                .expect("write seed");
            db.flush().await.expect("flush seed");
        }

        assert_eq!(
            db.table_stats(&table).await.l0_sstable_count,
            crate::DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT
        );
        table
            .write(b"trigger".to_vec(), Value::bytes("v"))
            .await
            .expect("write through deferred-work guardrail");

        let stats = db.table_stats(&table).await;
        assert!(
            stats.l0_sstable_count < crate::DEFAULT_WRITE_THROTTLE_L0_SSTABLE_COUNT,
            "bounded deferral should still execute pending compaction work"
        );
    }

    #[tokio::test]
    async fn cold_offload_selects_oldest_local_sstables_until_table_is_back_under_budget() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store.clone());
        let setup_db = Db::open(
            tiered_config_with_max_local_bytes("/cold-offload-selection", 1024 * 1024),
            dependencies.clone(),
        )
        .await
        .expect("open offload selection setup db");
        let setup_table = setup_db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        for (key, byte) in [
            (b"apple".to_vec(), b'a'),
            (b"banana".to_vec(), b'b'),
            (b"carrot".to_vec(), b'c'),
        ] {
            setup_table
                .write(key, Value::Bytes(vec![byte; 512]))
                .await
                .expect("write offload seed");
            setup_db.flush().await.expect("flush offload seed");
        }

        drop(setup_table);
        drop(setup_db);

        let config = tiered_config_with_max_local_bytes("/cold-offload-selection", 1200);
        let db = Db::open(config.clone(), dependencies)
            .await
            .expect("reopen offload selection db");
        let table = db.table("events");

        let budget = match &config.storage {
            StorageConfig::Tiered(config) => config.max_local_bytes,
            StorageConfig::S3Primary(_) => unreachable!("selection test uses tiered storage"),
        };
        let table_id = table.id().expect("table id");
        let live_before = db
            .sstables_read()
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table_id)
            .cloned()
            .collect::<Vec<_>>();
        let local_before = live_before
            .iter()
            .map(|sstable| sstable.meta.length)
            .sum::<u64>();
        assert!(
            local_before > budget,
            "fixture should exceed the local-byte budget"
        );

        let mut expected = live_before.clone();
        expected.sort_by(|left, right| {
            (
                left.meta.max_sequence.get(),
                left.meta.min_sequence.get(),
                left.meta.level,
                left.meta.local_id.as_str(),
            )
                .cmp(&(
                    right.meta.max_sequence.get(),
                    right.meta.min_sequence.get(),
                    right.meta.level,
                    right.meta.local_id.as_str(),
                ))
        });
        let mut remaining = local_before;
        let mut expected_ids = Vec::new();
        for sstable in &expected {
            if remaining <= budget {
                break;
            }
            remaining = remaining.saturating_sub(sstable.meta.length);
            expected_ids.push(sstable.meta.local_id.clone());
        }

        let offload_job = db
            .pending_offload_candidates()
            .into_iter()
            .find_map(|candidate| match candidate.spec {
                PendingWorkSpec::Offload(job) => Some(job),
                PendingWorkSpec::Flush | PendingWorkSpec::Compaction(_) => None,
            })
            .expect("pending offload job");
        assert_eq!(offload_job.input_local_ids, expected_ids);

        assert!(db.run_next_offload().await.expect("run offload"));

        let stats = db.table_stats(&table).await;
        assert!(stats.local_bytes <= budget);
        assert_eq!(stats.total_bytes, stats.local_bytes + stats.s3_bytes);
        assert!(stats.s3_bytes > 0);

        let remote_only_ids = db
            .sstables_read()
            .live
            .iter()
            .filter(|sstable| {
                sstable.meta.table_id == table_id
                    && sstable.meta.file_path.is_empty()
                    && sstable.meta.remote_key.is_some()
            })
            .map(|sstable| sstable.meta.local_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(remote_only_ids, expected_ids);

        let cold_prefix = format!("tiered/cold/table-{:06}/", table_id.get());
        let cold_keys = object_store
            .list(&cold_prefix)
            .await
            .expect("list cold objects");
        assert_eq!(cold_keys.len(), expected_ids.len());
    }

    #[tokio::test]
    async fn reads_are_identical_before_and_after_remote_offload() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);
        let setup_db = Db::open(
            tiered_config_with_max_local_bytes("/cold-offload-reads", 1024 * 1024),
            dependencies.clone(),
        )
        .await
        .expect("open offload read setup db");
        let setup_table = setup_db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let first = setup_table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write apple v1");
        setup_db.flush().await.expect("flush first");

        setup_table
            .write(b"banana".to_vec(), Value::bytes("yellow"))
            .await
            .expect("write banana");
        setup_db.flush().await.expect("flush second");

        setup_table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write apple v2");
        setup_db.flush().await.expect("flush third");

        drop(setup_table);
        drop(setup_db);

        let config = tiered_config_with_max_local_bytes("/cold-offload-reads", 1);
        let db = Db::open(config.clone(), dependencies.clone())
            .await
            .expect("reopen offload read db");
        let table = db.table("events");

        let expected_latest = table
            .read(b"apple".to_vec())
            .await
            .expect("read latest before offload");
        let expected_historical = table
            .read_at(b"apple".to_vec(), first)
            .await
            .expect("historical read before offload");
        let expected_scan = table
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("scan before offload")
            .collect::<Vec<_>>()
            .await;

        assert!(db.run_next_offload().await.expect("run offload"));

        assert_eq!(
            table
                .read(b"apple".to_vec())
                .await
                .expect("read latest after offload"),
            expected_latest
        );
        assert_eq!(
            table
                .read_at(b"apple".to_vec(), first)
                .await
                .expect("historical read after offload"),
            expected_historical
        );
        let after_scan = table
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("scan after offload")
            .collect::<Vec<_>>()
            .await;
        assert_eq!(after_scan, expected_scan);

        file_system.crash();
        let reopened = Db::open(config, dependencies)
            .await
            .expect("reopen offloaded db");
        let reopened_table = reopened.table("events");

        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("reopen latest read"),
            expected_latest
        );
        assert_eq!(
            reopened_table
                .read_at(b"apple".to_vec(), first)
                .await
                .expect("reopen historical read"),
            expected_historical
        );
        let reopened_scan = reopened_table
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("reopen scan")
            .collect::<Vec<_>>()
            .await;
        assert_eq!(reopened_scan, expected_scan);
        assert_eq!(reopened.table_stats(&reopened_table).await.local_bytes, 0);
        assert!(reopened.table_stats(&reopened_table).await.s3_bytes > 0);
    }

    #[tokio::test]
    async fn offload_upload_without_manifest_switch_recovers_prior_local_generation() {
        let root = "/cold-offload-upload-before-manifest";
        let (db, table, file_system, object_store, dependencies, first_sequence) =
            seed_offload_fixture(root, 1).await;
        let prior_generation = db.sstables_read().manifest_generation;
        let table_id = table.id().expect("table id");
        let cold_prefix = format!("tiered/cold/table-{:06}/", table_id.get());

        let mut blocker = db.block_next_offload_phase(OffloadPhase::UploadComplete);
        let offload_db = db.clone();
        let handle = tokio::spawn(async move { offload_db.run_next_offload().await });

        blocker.wait_until_reached().await;
        let uploaded = object_store
            .list(&cold_prefix)
            .await
            .expect("list uploaded cold objects");
        assert!(!uploaded.is_empty());

        handle.abort();
        let join_error = handle.await.expect_err("offload task should be cancelled");
        assert!(join_error.is_cancelled());

        file_system.crash();
        let reopened = Db::open(tiered_config_with_max_local_bytes(root, 1), dependencies)
            .await
            .expect("reopen prior generation");
        let reopened_table = reopened.table("events");
        let stats = reopened.table_stats(&reopened_table).await;

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            prior_generation
        );
        assert!(stats.local_bytes > 0);
        assert_eq!(stats.s3_bytes, 0);
        assert_eq!(
            reopened_table
                .read_at(b"apple".to_vec(), first_sequence)
                .await
                .expect("historical apple"),
            Some(Value::Bytes(vec![b'a'; 256]))
        );
    }

    #[tokio::test]
    async fn offload_manifest_switch_survives_reopen_even_if_local_cleanup_did_not_run() {
        let root = "/cold-offload-manifest-switched";
        let (db, table, file_system, _object_store, dependencies, first_sequence) =
            seed_offload_fixture(root, 1).await;
        let prior_generation = db.sstables_read().manifest_generation;
        let table_id = table.id().expect("table id");

        let mut blocker = db.block_next_offload_phase(OffloadPhase::ManifestSwitched);
        let offload_db = db.clone();
        let handle = tokio::spawn(async move { offload_db.run_next_offload().await });

        blocker.wait_until_reached().await;
        let still_on_disk = file_system
            .list(&sstable_dir(root, table_id))
            .await
            .expect("list local sstables before cleanup");
        assert!(!still_on_disk.is_empty());

        handle.abort();
        let join_error = handle.await.expect_err("offload task should be cancelled");
        assert!(join_error.is_cancelled());

        file_system.crash();
        let reopened = Db::open(tiered_config_with_max_local_bytes(root, 1), dependencies)
            .await
            .expect("reopen manifest-switched offload");
        let reopened_table = reopened.table("events");
        let stats = reopened.table_stats(&reopened_table).await;

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            ManifestId::new(prior_generation.get().saturating_add(1))
        );
        assert_eq!(stats.local_bytes, 0);
        assert!(stats.s3_bytes > 0);
        assert_eq!(
            reopened_table
                .read_at(b"apple".to_vec(), first_sequence)
                .await
                .expect("historical apple after manifest switch"),
            Some(Value::Bytes(vec![b'a'; 256]))
        );
        let lingering_local = file_system
            .list(&sstable_dir(root, table_id))
            .await
            .expect("list lingering local sstables");
        assert!(!lingering_local.is_empty());
    }

    #[tokio::test]
    async fn offload_after_local_file_deletion_recovers_remote_only_state() {
        let root = "/cold-offload-local-cleanup";
        let (db, table, file_system, _object_store, dependencies, first_sequence) =
            seed_offload_fixture(root, 1).await;
        let prior_generation = db.sstables_read().manifest_generation;
        let table_id = table.id().expect("table id");

        let mut blocker = db.block_next_offload_phase(OffloadPhase::LocalCleanupFinished);
        let offload_db = db.clone();
        let handle = tokio::spawn(async move { offload_db.run_next_offload().await });

        blocker.wait_until_reached().await;
        let local_after_cleanup = file_system
            .list(&sstable_dir(root, table_id))
            .await
            .expect("list local sstables after cleanup");
        assert!(local_after_cleanup.is_empty());

        handle.abort();
        let join_error = handle.await.expect_err("offload task should be cancelled");
        assert!(join_error.is_cancelled());

        file_system.crash();
        let reopened = Db::open(tiered_config_with_max_local_bytes(root, 1), dependencies)
            .await
            .expect("reopen remote-only offload");
        let reopened_table = reopened.table("events");
        let stats = reopened.table_stats(&reopened_table).await;

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            ManifestId::new(prior_generation.get().saturating_add(1))
        );
        assert_eq!(stats.local_bytes, 0);
        assert!(stats.s3_bytes > 0);
        assert_eq!(
            reopened_table
                .read_at(b"apple".to_vec(), first_sequence)
                .await
                .expect("historical apple after local cleanup"),
            Some(Value::Bytes(vec![b'a'; 256]))
        );
        assert!(
            reopened
                .sstables_read()
                .live
                .iter()
                .filter(|sstable| sstable.meta.table_id == table_id)
                .all(|sstable| sstable.meta.file_path.is_empty()
                    && sstable.meta.remote_key.is_some())
        );
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
    async fn flush_persists_columnar_sstables_manifest_footer_and_reopenable_reads() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(
            tiered_config("/columnar-flush-persist"),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let config = columnar_table_config("metrics");
        let schema = config.schema.clone().expect("columnar schema");
        let table = db.create_table(config).await.expect("create table");

        table
            .write(
                b"user:1".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("user_id", FieldValue::String("alice".to_string())),
                        ("count", FieldValue::Int64(1)),
                    ],
                )
                .expect("encode first record"),
            )
            .await
            .expect("write first record");
        table
            .write(
                b"user:2".to_vec(),
                Value::named_record(
                    &schema,
                    vec![("user_id", FieldValue::String("bob".to_string()))],
                )
                .expect("encode second record"),
            )
            .await
            .expect("write second record");
        let updated = Value::named_record(
            &schema,
            vec![
                ("user_id", FieldValue::String("alice".to_string())),
                ("count", FieldValue::Int64(2)),
            ],
        )
        .expect("encode updated record");
        let flushed = table
            .write(b"user:1".to_vec(), updated.clone())
            .await
            .expect("write updated record");

        db.flush().await.expect("flush to columnar SSTable");

        let live = db
            .sstables_read()
            .live
            .first()
            .cloned()
            .expect("live columnar sstable");
        assert_eq!(live.meta.schema_version, Some(schema.version));
        assert_eq!(live.rows.len(), 3);
        let filter = live
            .user_key_bloom_filter
            .as_ref()
            .expect("columnar resident bloom filter");
        assert!(filter.may_contain(b"user:1"));
        assert!(filter.may_contain(b"user:2"));

        let file_bytes = read_path(&dependencies, &live.meta.file_path)
            .await
            .expect("read columnar sstable");
        let (footer, footer_start) =
            Db::columnar_footer_from_bytes(&live.meta.file_path, &file_bytes)
                .expect("decode columnar footer");
        assert_eq!(footer.format_version, COLUMNAR_SSTABLE_FORMAT_VERSION);
        assert_eq!(footer.schema_version, schema.version);
        assert_eq!(footer.row_count, 3);
        assert_eq!(footer.columns.len(), 2);
        assert!(footer_start > COLUMNAR_SSTABLE_MAGIC.len());
        assert_eq!(
            footer
                .user_key_bloom_filter
                .expect("persisted columnar bloom filter")
                .bits_per_key,
            6
        );

        let loaded_manifest = Db::read_manifest_at_path(
            &dependencies,
            &Db::local_manifest_path("/columnar-flush-persist", ManifestId::new(1)),
        )
        .await
        .expect("load manifest");
        assert_eq!(loaded_manifest.last_flushed_sequence, flushed);
        assert_eq!(loaded_manifest.live_sstables.len(), 1);
        assert_eq!(
            loaded_manifest.live_sstables[0].meta.schema_version,
            Some(schema.version)
        );

        let expected_defaulted = Value::named_record(
            &schema,
            vec![("user_id", FieldValue::String("bob".to_string()))],
        )
        .expect("encode defaulted record");
        assert_eq!(
            table
                .read(b"user:1".to_vec())
                .await
                .expect("read flushed updated row"),
            Some(updated.clone())
        );
        assert_eq!(
            table
                .read(b"user:2".to_vec())
                .await
                .expect("read flushed defaulted row"),
            Some(expected_defaulted.clone())
        );

        let reopened = Db::open(tiered_config("/columnar-flush-persist"), dependencies)
            .await
            .expect("reopen after columnar flush");
        let reopened_table = reopened.table("metrics");
        assert_eq!(reopened.current_sequence(), flushed);
        assert_eq!(reopened.current_durable_sequence(), flushed);
        assert_eq!(
            reopened_table
                .read(b"user:1".to_vec())
                .await
                .expect("read reopened updated row"),
            Some(updated)
        );
        assert_eq!(
            reopened_table
                .read(b"user:2".to_vec())
                .await
                .expect("read reopened defaulted row"),
            Some(expected_defaulted)
        );
    }

    #[tokio::test]
    async fn columnar_sstables_round_trip_all_supported_field_types_and_tombstones() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);

        let db = Db::open(tiered_config("/columnar-all-types"), dependencies.clone())
            .await
            .expect("open db");
        let config = columnar_all_types_table_config("metrics");
        let schema = config.schema.clone().expect("columnar schema");
        let table = db.create_table(config).await.expect("create table");

        let full_record = Value::named_record(
            &schema,
            vec![
                ("metric", FieldValue::String("cpu".to_string())),
                ("count", FieldValue::Int64(7)),
                ("ratio", FieldValue::Float64(1.25)),
                ("payload", FieldValue::Bytes(vec![1, 2, 3, 4])),
                ("active", FieldValue::Bool(true)),
            ],
        )
        .expect("encode full record");
        let defaulted_record = Value::named_record(
            &schema,
            vec![("metric", FieldValue::String("memory".to_string()))],
        )
        .expect("encode defaulted record");

        table
            .write(b"row:1".to_vec(), full_record.clone())
            .await
            .expect("write full record");
        table
            .write(b"row:2".to_vec(), defaulted_record.clone())
            .await
            .expect("write defaulted record");
        table
            .write(
                b"row:3".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("metric", FieldValue::String("disk".to_string())),
                        ("count", FieldValue::Int64(3)),
                        ("active", FieldValue::Bool(true)),
                    ],
                )
                .expect("encode tombstoned record"),
            )
            .await
            .expect("write tombstoned record");
        table
            .delete(b"row:3".to_vec())
            .await
            .expect("delete tombstoned record");

        db.flush().await.expect("flush all-types columnar sstable");

        let live = db
            .sstables_read()
            .live
            .first()
            .cloned()
            .expect("live all-types sstable");
        let file_bytes = read_path(&dependencies, &live.meta.file_path)
            .await
            .expect("read all-types sstable");
        let (footer, _) = Db::columnar_footer_from_bytes(&live.meta.file_path, &file_bytes)
            .expect("decode all-types footer");
        assert_eq!(
            footer
                .columns
                .iter()
                .map(|column| column.field_type)
                .collect::<Vec<_>>(),
            vec![
                FieldType::String,
                FieldType::Int64,
                FieldType::Float64,
                FieldType::Bytes,
                FieldType::Bool,
            ]
        );

        let reopened = Db::open(tiered_config("/columnar-all-types"), dependencies)
            .await
            .expect("reopen all-types db");
        let reopened_table = reopened.table("metrics");
        assert_eq!(
            reopened_table
                .read(b"row:1".to_vec())
                .await
                .expect("read full record"),
            Some(full_record)
        );
        assert_eq!(
            reopened_table
                .read(b"row:2".to_vec())
                .await
                .expect("read defaulted record"),
            Some(defaulted_record)
        );
        assert_eq!(
            reopened_table
                .read(b"row:3".to_vec())
                .await
                .expect("read tombstoned record"),
            None
        );
    }

    #[tokio::test]
    async fn columnar_sstable_flush_is_byte_for_byte_deterministic() {
        let first_file_system = Arc::new(crate::StubFileSystem::default());
        let first_object_store = Arc::new(StubObjectStore::default());
        let first_dependencies = dependencies(first_file_system.clone(), first_object_store);
        let second_file_system = Arc::new(crate::StubFileSystem::default());
        let second_object_store = Arc::new(StubObjectStore::default());
        let second_dependencies = dependencies(second_file_system.clone(), second_object_store);

        let config = columnar_all_types_table_config("metrics");
        let schema = config.schema.clone().expect("columnar schema");

        let first_db = Db::open(
            tiered_config("/columnar-deterministic-a"),
            first_dependencies.clone(),
        )
        .await
        .expect("open first db");
        let first_table = first_db
            .create_table(config.clone())
            .await
            .expect("create first table");
        first_table
            .write(
                b"row:1".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("metric", FieldValue::String("cpu".to_string())),
                        ("count", FieldValue::Int64(9)),
                        ("ratio", FieldValue::Float64(2.5)),
                        ("payload", FieldValue::Bytes(vec![9, 8, 7])),
                        ("active", FieldValue::Bool(true)),
                    ],
                )
                .expect("encode first deterministic record"),
            )
            .await
            .expect("write first deterministic record");
        first_table
            .write(
                b"row:2".to_vec(),
                Value::named_record(
                    &schema,
                    vec![("metric", FieldValue::String("memory".to_string()))],
                )
                .expect("encode second deterministic record"),
            )
            .await
            .expect("write second deterministic record");
        first_table
            .delete(b"row:2".to_vec())
            .await
            .expect("delete second deterministic row");
        first_db.flush().await.expect("flush first db");
        let first_path = first_db.sstables_read().live[0].meta.file_path.clone();
        let first_bytes = read_path(&first_dependencies, &first_path)
            .await
            .expect("read first deterministic sstable");

        let second_db = Db::open(
            tiered_config("/columnar-deterministic-b"),
            second_dependencies.clone(),
        )
        .await
        .expect("open second db");
        let second_table = second_db
            .create_table(config)
            .await
            .expect("create second table");
        second_table
            .write(
                b"row:1".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("metric", FieldValue::String("cpu".to_string())),
                        ("count", FieldValue::Int64(9)),
                        ("ratio", FieldValue::Float64(2.5)),
                        ("payload", FieldValue::Bytes(vec![9, 8, 7])),
                        ("active", FieldValue::Bool(true)),
                    ],
                )
                .expect("encode mirrored deterministic record"),
            )
            .await
            .expect("write mirrored deterministic record");
        second_table
            .write(
                b"row:2".to_vec(),
                Value::named_record(
                    &schema,
                    vec![("metric", FieldValue::String("memory".to_string()))],
                )
                .expect("encode mirrored deterministic defaulted record"),
            )
            .await
            .expect("write mirrored deterministic defaulted record");
        second_table
            .delete(b"row:2".to_vec())
            .await
            .expect("delete mirrored deterministic row");
        second_db.flush().await.expect("flush second db");
        let second_path = second_db.sstables_read().live[0].meta.file_path.clone();
        let second_bytes = read_path(&second_dependencies, &second_path)
            .await
            .expect("read second deterministic sstable");

        assert_eq!(first_bytes, second_bytes);
    }

    #[tokio::test]
    async fn columnar_manifest_sync_failure_preserves_prior_generation_on_reopen() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(
            tiered_config("/columnar-manifest-sync-failure"),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let config = columnar_table_config("metrics");
        let schema = config.schema.clone().expect("columnar schema");
        let table = db.create_table(config).await.expect("create table");

        table
            .write(
                b"user:1".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("user_id", FieldValue::String("alice".to_string())),
                        ("count", FieldValue::Int64(1)),
                    ],
                )
                .expect("encode initial record"),
            )
            .await
            .expect("write initial record");
        db.flush().await.expect("flush initial record");

        let updated = Value::named_record(
            &schema,
            vec![
                ("user_id", FieldValue::String("alice".to_string())),
                ("count", FieldValue::Int64(2)),
            ],
        )
        .expect("encode updated record");
        let second = table
            .write(b"user:1".to_vec(), updated.clone())
            .await
            .expect("write updated record");
        file_system.inject_failure(FileSystemFailure::timeout(
            FileSystemOperation::SyncDir,
            Db::local_manifest_dir("/columnar-manifest-sync-failure"),
        ));
        db.flush().await.expect_err("manifest sync failure");

        file_system.crash();
        let reopened = Db::open(
            tiered_config("/columnar-manifest-sync-failure"),
            dependencies,
        )
        .await
        .expect("reopen after failed columnar manifest sync");
        let reopened_table = reopened.table("metrics");
        assert_eq!(reopened.current_sequence(), second);
        assert_eq!(reopened.current_durable_sequence(), second);
        assert_eq!(
            reopened.sstables_read().manifest_generation,
            ManifestId::new(1)
        );
        assert_eq!(
            reopened_table
                .read(b"user:1".to_vec())
                .await
                .expect("read recovered updated record"),
            Some(updated)
        );
        assert!(
            reopened
                .table_stats(&reopened_table)
                .await
                .pending_flush_bytes
                > 0
        );
    }

    #[tokio::test]
    async fn s3_primary_columnar_flush_persists_remote_sstables_and_reopens() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store.clone());
        let config = s3_primary_config("columnar-remote-flush");

        let db = Db::open(config.clone(), dependencies.clone())
            .await
            .expect("open s3 db");
        let table_config = columnar_table_config("metrics");
        let schema = table_config.schema.clone().expect("columnar schema");
        let table = db
            .create_table(table_config)
            .await
            .expect("create columnar table");

        let expected = Value::named_record(
            &schema,
            vec![
                ("user_id", FieldValue::String("alice".to_string())),
                ("count", FieldValue::Int64(4)),
            ],
        )
        .expect("encode remote record");
        let flushed = table
            .write(b"user:1".to_vec(), expected.clone())
            .await
            .expect("write remote record");

        db.flush().await.expect("flush remote columnar sstable");
        assert_eq!(db.current_durable_sequence(), flushed);

        let live = db
            .sstables_read()
            .live
            .first()
            .cloned()
            .expect("live remote columnar sstable");
        let remote_key = live.meta.remote_key.clone().expect("remote sstable key");
        let file_bytes = object_store
            .get(&remote_key)
            .await
            .expect("read remote sstable");
        let (footer, _) =
            Db::columnar_footer_from_bytes(&remote_key, &file_bytes).expect("decode remote footer");
        assert_eq!(footer.schema_version, schema.version);
        assert_eq!(footer.row_count, 1);
        assert_eq!(live.meta.schema_version, Some(schema.version));

        let reopened = Db::open(config, dependencies)
            .await
            .expect("reopen remote db");
        let reopened_table = reopened.table("metrics");
        assert_eq!(reopened.current_sequence(), flushed);
        assert_eq!(reopened.current_durable_sequence(), flushed);
        assert_eq!(
            reopened_table
                .read(b"user:1".to_vec())
                .await
                .expect("read reopened remote record"),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn columnar_reads_and_scans_reconstruct_rows_and_guard_historical_overwrites() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);

        let db = Db::open(
            tiered_config("/columnar-read-semantics"),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let config = columnar_table_config("metrics");
        let schema = config.schema.clone().expect("columnar schema");
        let table = db.create_table(config).await.expect("create table");

        let first = table
            .write(
                b"user:1".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("user_id", FieldValue::String("alice".to_string())),
                        ("count", FieldValue::Int64(1)),
                    ],
                )
                .expect("encode first record"),
            )
            .await
            .expect("write first record");
        let second = table
            .write(
                b"user:2".to_vec(),
                Value::named_record(
                    &schema,
                    vec![("user_id", FieldValue::String("bob".to_string()))],
                )
                .expect("encode second record"),
            )
            .await
            .expect("write second record");
        db.flush().await.expect("flush first batch");

        let updated = Value::named_record(
            &schema,
            vec![
                ("user_id", FieldValue::String("alice".to_string())),
                ("count", FieldValue::Int64(9)),
            ],
        )
        .expect("encode updated record");
        let latest = table
            .write(b"user:1".to_vec(), updated.clone())
            .await
            .expect("write updated record");
        db.flush().await.expect("flush overwritten key");

        let defaulted = Value::named_record(
            &schema,
            vec![("user_id", FieldValue::String("bob".to_string()))],
        )
        .expect("encode defaulted row");
        assert_eq!(
            table
                .read(b"user:1".to_vec())
                .await
                .expect("latest point read"),
            Some(updated.clone())
        );
        assert_eq!(
            collect_rows(
                table
                    .scan_prefix(b"user:".to_vec(), ScanOptions::default())
                    .await
                    .expect("latest scan"),
            )
            .await,
            vec![
                (b"user:1".to_vec(), updated.clone()),
                (b"user:2".to_vec(), defaulted.clone()),
            ]
        );
        assert_eq!(
            table
                .read_at(b"user:2".to_vec(), second)
                .await
                .expect("historical single-version read"),
            Some(defaulted)
        );
        assert_eq!(
            table
                .read_at(b"user:1".to_vec(), latest)
                .await
                .expect("latest read_at still succeeds"),
            Some(updated)
        );

        let point_error = table
            .read_at(b"user:1".to_vec(), first)
            .await
            .expect_err("historical overwritten key should fail closed");
        match point_error {
            ReadError::Storage(error) => {
                assert!(error.message().contains("historical overwritten-key"));
            }
            other => panic!("expected unsupported storage error, got {other:?}"),
        }

        let scan_error = match table
            .scan_at(Vec::new(), vec![0xff], first, ScanOptions::default())
            .await
        {
            Ok(_) => panic!("historical scan with overwritten key should fail closed"),
            Err(error) => error,
        };
        match scan_error {
            ReadError::Storage(error) => {
                assert!(error.message().contains("historical overwritten-key"));
            }
            other => panic!("expected unsupported storage error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn columnar_scan_pruning_skips_unrequested_column_decodes() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/columnar-pruned-scan"), dependencies.clone())
            .await
            .expect("open db");
        let config = columnar_all_types_table_config("metrics");
        let schema = config.schema.clone().expect("columnar schema");
        let table = db.create_table(config).await.expect("create table");

        table
            .write(
                b"row:1".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("metric", FieldValue::String("cpu".to_string())),
                        ("count", FieldValue::Int64(7)),
                        ("ratio", FieldValue::Float64(1.25)),
                        ("payload", FieldValue::Bytes(vec![1, 2, 3])),
                        ("active", FieldValue::Bool(true)),
                    ],
                )
                .expect("encode row"),
            )
            .await
            .expect("write row");
        db.flush().await.expect("flush columnar row");

        let live = db
            .sstables_read()
            .live
            .first()
            .cloned()
            .expect("live columnar sstable");
        let file_bytes = read_path(&dependencies, &live.meta.file_path)
            .await
            .expect("read local columnar file");
        let (footer, _) = Db::columnar_footer_from_bytes(&live.meta.file_path, &file_bytes)
            .expect("decode local columnar footer");
        let count_block = footer
            .columns
            .iter()
            .find(|column| column.field_id == FieldId::new(2))
            .expect("count column");
        let handle = file_system
            .open(
                &live.meta.file_path,
                crate::OpenOptions {
                    create: false,
                    read: true,
                    write: true,
                    truncate: false,
                    append: false,
                },
            )
            .await
            .expect("open local columnar file for corruption");
        file_system
            .write_at(&handle, count_block.block.offset, b"{not-valid-json")
            .await
            .expect("corrupt unrequested count column");
        file_system
            .sync(&handle)
            .await
            .expect("sync corrupted column");

        let pruned_rows = collect_rows(
            table
                .scan(
                    Vec::new(),
                    vec![0xff],
                    ScanOptions {
                        columns: Some(vec!["metric".to_string()]),
                        ..ScanOptions::default()
                    },
                )
                .await
                .expect("pruned scan should ignore corrupted count column"),
        )
        .await;
        assert_eq!(
            pruned_rows,
            vec![(
                b"row:1".to_vec(),
                Value::record(BTreeMap::from([(
                    FieldId::new(1),
                    FieldValue::String("cpu".to_string()),
                )])),
            )]
        );

        let full_scan_error = match table
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
        {
            Ok(_) => panic!("full scan should decode the corrupted count column"),
            Err(error) => error,
        };
        match full_scan_error {
            ReadError::Storage(error) => {
                assert!(error.message().contains("decode columnar field 2"));
            }
            other => panic!("expected storage error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn remote_columnar_scan_fetches_only_requested_ranges() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let inner_store = Arc::new(StubObjectStore::default());
        let recording_store = Arc::new(RecordingObjectStore::new(inner_store.clone()));
        let dependencies = DbDependencies::new(
            file_system,
            recording_store.clone(),
            Arc::new(StubClock::default()),
            Arc::new(StubRng::seeded(7)),
        );

        let db = Db::open(
            s3_primary_config("columnar-range-fetch"),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let config = columnar_all_types_table_config("metrics");
        let schema = config.schema.clone().expect("columnar schema");
        let table = db.create_table(config).await.expect("create table");

        table
            .write(
                b"row:1".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("metric", FieldValue::String("cpu".to_string())),
                        ("count", FieldValue::Int64(11)),
                        ("ratio", FieldValue::Float64(2.5)),
                        ("payload", FieldValue::Bytes(vec![9, 8, 7])),
                        ("active", FieldValue::Bool(true)),
                    ],
                )
                .expect("encode row:1"),
            )
            .await
            .expect("write row:1");
        table
            .write(
                b"row:2".to_vec(),
                Value::named_record(
                    &schema,
                    vec![
                        ("metric", FieldValue::String("mem".to_string())),
                        ("count", FieldValue::Int64(5)),
                        ("ratio", FieldValue::Float64(1.0)),
                        ("payload", FieldValue::Bytes(vec![1, 2])),
                        ("active", FieldValue::Bool(false)),
                    ],
                )
                .expect("encode row:2"),
            )
            .await
            .expect("write row:2");
        db.flush().await.expect("flush remote columnar rows");

        let live = db
            .sstables_read()
            .live
            .first()
            .cloned()
            .expect("live remote columnar sstable");
        let remote_key = live.meta.remote_key.clone().expect("remote object key");
        let file_bytes = inner_store
            .get(&remote_key)
            .await
            .expect("read remote bytes from inner store");
        let (footer, footer_start) =
            Db::columnar_footer_from_bytes(&remote_key, &file_bytes).expect("decode footer");
        let metric_block = footer
            .columns
            .iter()
            .find(|column| column.field_id == FieldId::new(1))
            .expect("metric block");
        let count_block = footer
            .columns
            .iter()
            .find(|column| column.field_id == FieldId::new(2))
            .expect("count block");

        let footer_range = (
            remote_key.clone(),
            footer_start as u64,
            live.meta.length
                - (COLUMNAR_SSTABLE_MAGIC.len() as u64 + std::mem::size_of::<u64>() as u64),
        );
        let expected_ranges = [
            (
                remote_key.clone(),
                live.meta.length
                    - (COLUMNAR_SSTABLE_MAGIC.len() as u64 + std::mem::size_of::<u64>() as u64),
                live.meta.length,
            ),
            footer_range.clone(),
            (
                remote_key.clone(),
                footer.key_index.offset,
                footer.key_index.offset + footer.key_index.length,
            ),
            (
                remote_key.clone(),
                footer.sequence_column.offset,
                footer.sequence_column.offset + footer.sequence_column.length,
            ),
            (
                remote_key.clone(),
                footer.tombstone_bitmap.offset,
                footer.tombstone_bitmap.offset + footer.tombstone_bitmap.length,
            ),
            (
                remote_key.clone(),
                footer.row_kind_column.offset,
                footer.row_kind_column.offset + footer.row_kind_column.length,
            ),
            (
                remote_key.clone(),
                metric_block.block.offset,
                metric_block.block.offset + metric_block.block.length,
            ),
        ];

        recording_store.clear_calls();
        let rows = collect_rows(
            table
                .scan(
                    Vec::new(),
                    vec![0xff],
                    ScanOptions {
                        columns: Some(vec!["metric".to_string()]),
                        ..ScanOptions::default()
                    },
                )
                .await
                .expect("remote pruned scan"),
        )
        .await;
        assert_eq!(
            rows,
            vec![
                (
                    b"row:1".to_vec(),
                    Value::record(BTreeMap::from([(
                        FieldId::new(1),
                        FieldValue::String("cpu".to_string()),
                    )])),
                ),
                (
                    b"row:2".to_vec(),
                    Value::record(BTreeMap::from([(
                        FieldId::new(1),
                        FieldValue::String("mem".to_string()),
                    )])),
                ),
            ]
        );

        assert!(
            recording_store
                .get_calls()
                .into_iter()
                .all(|key| key != remote_key),
            "columnar scan should not issue a full-object get for the SSTable",
        );

        let range_calls = recording_store
            .range_calls()
            .into_iter()
            .filter(|(key, _, _)| key == &remote_key)
            .collect::<Vec<_>>();
        assert!(!range_calls.is_empty(), "scan should issue range reads");
        assert!(
            range_calls
                .iter()
                .all(|range| expected_ranges.contains(range)),
            "all remote range reads should stay within the footer, metadata columns, and requested metric column",
        );
        assert!(
            !range_calls.contains(&(
                remote_key,
                count_block.block.offset,
                count_block.block.offset + count_block.block.length,
            )),
            "count column should not be fetched for a metric-only scan",
        );
    }

    #[tokio::test]
    async fn columnar_reads_fill_defaults_when_older_sstables_lack_new_fields() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);

        let db = Db::open(
            tiered_config("/columnar-schema-default-fill"),
            dependencies.clone(),
        )
        .await
        .expect("open db");

        let legacy_schema = SchemaDefinition {
            version: 1,
            fields: vec![FieldDefinition {
                id: FieldId::new(1),
                name: "user_id".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            }],
        };
        let mut legacy_config = columnar_table_config("metrics");
        legacy_config.schema = Some(legacy_schema.clone());
        let table = db
            .create_table(legacy_config)
            .await
            .expect("create legacy columnar table");

        let first = table
            .write(
                b"user:1".to_vec(),
                Value::named_record(
                    &legacy_schema,
                    vec![("user_id", FieldValue::String("alice".to_string()))],
                )
                .expect("encode legacy row"),
            )
            .await
            .expect("write legacy row");
        db.flush().await.expect("flush legacy row");

        let successor_schema = columnar_table_config("metrics")
            .schema
            .expect("successor schema");
        install_columnar_schema_successor(&db, "metrics", successor_schema.clone()).await;
        let second_record = Value::named_record(
            &successor_schema,
            vec![
                ("user_id", FieldValue::String("bob".to_string())),
                ("count", FieldValue::Int64(7)),
            ],
        )
        .expect("encode successor row");
        table
            .write(b"user:2".to_vec(), second_record.clone())
            .await
            .expect("write successor row");
        db.flush().await.expect("flush successor row");

        let expected_legacy = Value::named_record(
            &successor_schema,
            vec![("user_id", FieldValue::String("alice".to_string()))],
        )
        .expect("encode default-filled legacy row");
        assert_eq!(
            table
                .read(b"user:1".to_vec())
                .await
                .expect("read default-filled legacy row"),
            Some(expected_legacy.clone())
        );
        assert_eq!(
            table
                .read_at(b"user:1".to_vec(), first)
                .await
                .expect("historical read of single-version legacy row"),
            Some(expected_legacy.clone())
        );

        let reopened = Db::open(tiered_config("/columnar-schema-default-fill"), dependencies)
            .await
            .expect("reopen mixed-schema db");
        let reopened_table = reopened.table("metrics");
        assert_eq!(
            reopened_table
                .read(b"user:1".to_vec())
                .await
                .expect("read reopened default-filled legacy row"),
            Some(expected_legacy.clone())
        );
        assert_eq!(
            collect_rows(
                reopened_table
                    .scan(Vec::new(), vec![0xff], ScanOptions::default())
                    .await
                    .expect("scan mixed-schema rows"),
            )
            .await,
            vec![
                (b"user:1".to_vec(), expected_legacy),
                (b"user:2".to_vec(), second_record),
            ]
        );
    }

    #[tokio::test]
    async fn columnar_compaction_rewrites_mixed_schema_versions_without_changing_reads() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);

        let db = Db::open(
            tiered_config("/columnar-mixed-schema-compaction"),
            dependencies.clone(),
        )
        .await
        .expect("open db");

        let legacy_schema = SchemaDefinition {
            version: 1,
            fields: vec![FieldDefinition {
                id: FieldId::new(1),
                name: "user_id".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            }],
        };
        let mut legacy_config = columnar_table_config("metrics");
        legacy_config.schema = Some(legacy_schema.clone());
        let table = db
            .create_table(legacy_config)
            .await
            .expect("create legacy columnar table");

        table
            .write(
                b"user:1".to_vec(),
                Value::named_record(
                    &legacy_schema,
                    [("user_id", FieldValue::String("alice".to_string()))],
                )
                .expect("encode legacy row"),
            )
            .await
            .expect("write legacy row");
        db.flush().await.expect("flush legacy row");

        let renamed_and_added_schema = SchemaDefinition {
            version: 2,
            fields: vec![
                FieldDefinition {
                    id: FieldId::new(1),
                    name: "account_id".to_string(),
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
        };
        install_columnar_schema_successor(&db, "metrics", renamed_and_added_schema.clone()).await;
        table
            .write(
                b"user:2".to_vec(),
                Value::named_record(
                    &renamed_and_added_schema,
                    [
                        ("account_id", FieldValue::String("bob".to_string())),
                        ("count", FieldValue::Int64(7)),
                    ],
                )
                .expect("encode renamed row"),
            )
            .await
            .expect("write renamed row");
        db.flush().await.expect("flush renamed row");

        let removed_schema = SchemaDefinition {
            version: 3,
            fields: vec![FieldDefinition {
                id: FieldId::new(1),
                name: "account_id".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            }],
        };
        install_columnar_schema_successor(&db, "metrics", removed_schema.clone()).await;
        table
            .write(
                b"user:3".to_vec(),
                Value::named_record(
                    &removed_schema,
                    [("account_id", FieldValue::String("carol".to_string()))],
                )
                .expect("encode removed-column row"),
            )
            .await
            .expect("write removed-column row");
        db.flush().await.expect("flush removed-column row");

        let expected_rows = vec![
            (
                b"user:1".to_vec(),
                Value::record(BTreeMap::from([(
                    FieldId::new(1),
                    FieldValue::String("alice".to_string()),
                )])),
            ),
            (
                b"user:2".to_vec(),
                Value::record(BTreeMap::from([(
                    FieldId::new(1),
                    FieldValue::String("bob".to_string()),
                )])),
            ),
            (
                b"user:3".to_vec(),
                Value::record(BTreeMap::from([(
                    FieldId::new(1),
                    FieldValue::String("carol".to_string()),
                )])),
            ),
        ];

        assert_eq!(
            collect_rows(
                table
                    .scan(Vec::new(), vec![0xff], ScanOptions::default())
                    .await
                    .expect("scan mixed-schema rows before compaction"),
            )
            .await,
            expected_rows
        );
        assert_eq!(
            collect_rows(
                table
                    .scan(
                        Vec::new(),
                        vec![0xff],
                        ScanOptions {
                            reverse: false,
                            limit: None,
                            columns: Some(vec!["account_id".to_string()]),
                        },
                    )
                    .await
                    .expect("scan renamed projection"),
            )
            .await,
            expected_rows
        );
        let projection_error = match table
            .scan(
                Vec::new(),
                vec![0xff],
                ScanOptions {
                    reverse: false,
                    limit: None,
                    columns: Some(vec!["user_id".to_string()]),
                },
            )
            .await
        {
            Ok(_) => panic!("removed column name should be rejected"),
            Err(error) => error,
        };
        match projection_error {
            ReadError::Storage(error) => {
                assert!(error.message().contains("does not contain column user_id"));
            }
            other => panic!("expected unsupported storage error, got {other:?}"),
        }

        assert!(
            db.run_next_compaction()
                .await
                .expect("run mixed-schema compaction")
        );

        assert_eq!(
            collect_rows(
                table
                    .scan(Vec::new(), vec![0xff], ScanOptions::default())
                    .await
                    .expect("scan mixed-schema rows after compaction"),
            )
            .await,
            expected_rows
        );

        let live = db.sstables_read().live.clone();
        assert_eq!(live.len(), 1);
        assert!(live[0].is_columnar());
        assert_eq!(live[0].meta.schema_version, Some(removed_schema.version));
        let metadata = live[0]
            .load_columnar_metadata(db.dependencies())
            .await
            .expect("load compacted columnar metadata");
        assert_eq!(metadata.footer.columns.len(), 1);
        assert_eq!(metadata.footer.columns[0].field_id, FieldId::new(1));
    }

    #[tokio::test]
    async fn columnar_merge_reads_match_compacted_results() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/columnar-merge-compaction"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(merge_columnar_table_config("metrics", Some(2)))
            .await
            .expect("create merge-capable columnar table");

        table
            .write(b"doc".to_vec(), count_record(1))
            .await
            .expect("write base count");
        db.flush().await.expect("flush base count");
        table
            .merge(b"doc".to_vec(), count_record(2))
            .await
            .expect("write persisted delta");
        db.flush().await.expect("flush persisted delta");
        table
            .merge(b"doc".to_vec(), count_record(3))
            .await
            .expect("write memtable delta");

        assert_eq!(
            table
                .read(b"doc".to_vec())
                .await
                .expect("resolve merge chain"),
            Some(count_record(6))
        );
        assert_eq!(
            collect_rows(
                table
                    .scan(b"doc".to_vec(), b"doe".to_vec(), ScanOptions::default())
                    .await
                    .expect("scan merged row"),
            )
            .await,
            vec![(b"doc".to_vec(), count_record(6))]
        );

        db.flush().await.expect("flush tail delta");
        assert_eq!(
            table
                .read(b"doc".to_vec())
                .await
                .expect("resolve fully persisted merge chain"),
            Some(count_record(6))
        );
        assert!(
            db.run_next_compaction()
                .await
                .expect("run columnar merge compaction")
        );
        assert_eq!(
            table
                .read(b"doc".to_vec())
                .await
                .expect("read merged row after compaction"),
            Some(count_record(6))
        );

        let live = db.sstables_read().live.clone();
        assert_eq!(live.len(), 1);
        assert!(live[0].is_columnar());
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
                Some(count_record(6)),
                Some(count_record(3)),
                Some(count_record(1)),
            ]
        );
    }

    #[tokio::test]
    async fn columnar_compaction_output_without_manifest_switch_recovers_prior_generation() {
        let (db, _table, file_system, dependencies, schema) =
            seed_columnar_compaction_fixture("/columnar-compaction-manifest-before-switch").await;

        let prior_generation = db.sstables_read().manifest_generation;
        let next_generation = ManifestId::new(prior_generation.get().saturating_add(1));
        let manifest_temp_path = format!(
            "{}{}",
            Db::local_manifest_path(
                "/columnar-compaction-manifest-before-switch",
                next_generation
            ),
            LOCAL_MANIFEST_TEMP_SUFFIX
        );
        file_system.inject_failure(FileSystemFailure::for_target(
            FileSystemOperation::Rename,
            manifest_temp_path,
            StorageError::io("simulated columnar manifest rename failure"),
        ));

        let error = db
            .run_next_compaction()
            .await
            .expect_err("columnar compaction should fail before manifest switch");
        assert!(
            error
                .message()
                .contains("simulated columnar manifest rename failure")
        );

        file_system.crash();
        let reopened = Db::open(
            tiered_config("/columnar-compaction-manifest-before-switch"),
            dependencies,
        )
        .await
        .expect("reopen after failed columnar compaction");
        let reopened_table = reopened.table("metrics");

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            prior_generation
        );
        assert_eq!(
            collect_rows(
                reopened_table
                    .scan(Vec::new(), vec![0xff], ScanOptions::default())
                    .await
                    .expect("scan reopened columnar rows"),
            )
            .await,
            vec![
                (
                    b"user:1".to_vec(),
                    Value::named_record(
                        &schema,
                        [
                            ("user_id", FieldValue::String("alice".to_string())),
                            ("count", FieldValue::Int64(1)),
                        ],
                    )
                    .expect("encode alice"),
                ),
                (
                    b"user:2".to_vec(),
                    Value::named_record(
                        &schema,
                        [
                            ("user_id", FieldValue::String("bob".to_string())),
                            ("count", FieldValue::Int64(2)),
                        ],
                    )
                    .expect("encode bob"),
                ),
                (
                    b"user:3".to_vec(),
                    Value::named_record(
                        &schema,
                        [
                            ("user_id", FieldValue::String("carol".to_string())),
                            ("count", FieldValue::Int64(3)),
                        ],
                    )
                    .expect("encode carol"),
                ),
            ]
        );
    }

    #[test]
    fn simulated_columnar_mixed_schema_compaction_recovers_after_crash() -> turmoil::Result {
        SeededSimulationRunner::new(0x27c0_1001).run_with(|context| async move {
            let config = tiered_config("/terracedb/sim/columnar-mixed-schema-compaction-crash");
            let db = context.open_db(config.clone()).await?;

            let legacy_schema = SchemaDefinition {
                version: 1,
                fields: vec![FieldDefinition {
                    id: FieldId::new(1),
                    name: "user_id".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    default: None,
                }],
            };
            let mut config_v1 = columnar_table_config("metrics");
            config_v1.schema = Some(legacy_schema.clone());
            config_v1.compaction_strategy = CompactionStrategy::Leveled;
            let table = db
                .create_table(config_v1)
                .await
                .expect("create legacy columnar table");

            table
                .write(
                    b"user:1".to_vec(),
                    Value::named_record(
                        &legacy_schema,
                        [("user_id", FieldValue::String("alice".to_string()))],
                    )
                    .expect("encode legacy row"),
                )
                .await
                .expect("write legacy row");
            db.flush().await.expect("flush legacy row");

            let successor_schema = SchemaDefinition {
                version: 2,
                fields: vec![
                    FieldDefinition {
                        id: FieldId::new(1),
                        name: "account_id".to_string(),
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
            };
            install_columnar_schema_successor(&db, "metrics", successor_schema.clone()).await;
            table
                .write(
                    b"user:2".to_vec(),
                    Value::named_record(
                        &successor_schema,
                        [
                            ("account_id", FieldValue::String("bob".to_string())),
                            ("count", FieldValue::Int64(7)),
                        ],
                    )
                    .expect("encode successor row"),
                )
                .await
                .expect("write successor row");
            db.flush().await.expect("flush successor row");

            let pending = db.pending_compaction_jobs();
            assert!(
                pending.iter().any(|job| job.table_name == "metrics"),
                "tiered columnar runs should schedule compaction before the crash",
            );

            let mut blocker = db.block_next_compaction_phase(CompactionPhase::OutputWritten);
            let compact_db = db.clone();
            let compaction = tokio::spawn(async move { compact_db.run_next_compaction().await });
            blocker.wait_until_reached().await;

            compaction.abort();
            let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
            let reopened_table = reopened.table("metrics");
            assert!(
                context.trace().iter().any(|event| matches!(
                    event,
                    TraceEvent::Crash {
                        cut_point: CutPoint::AfterStep
                    }
                )),
                "simulation trace should record the crash cut point",
            );
            assert!(
                context
                    .trace()
                    .iter()
                    .any(|event| matches!(event, TraceEvent::Restart)),
                "simulation trace should record the restart",
            );

            assert_eq!(
                collect_rows(
                    reopened_table
                        .scan(Vec::new(), vec![0xff], ScanOptions::default())
                        .await
                        .expect("scan reopened mixed-schema rows"),
                )
                .await,
                vec![
                    (
                        b"user:1".to_vec(),
                        Value::named_record(
                            &successor_schema,
                            [
                                ("account_id", FieldValue::String("alice".to_string())),
                                ("count", FieldValue::Int64(0)),
                            ],
                        )
                        .expect("encode default-filled alice"),
                    ),
                    (
                        b"user:2".to_vec(),
                        Value::named_record(
                            &successor_schema,
                            [
                                ("account_id", FieldValue::String("bob".to_string())),
                                ("count", FieldValue::Int64(7)),
                            ],
                        )
                        .expect("encode bob"),
                    ),
                ],
            );

            assert!(
                reopened
                    .pending_compaction_jobs()
                    .iter()
                    .any(|job| job.table_name == "metrics"),
                "reopened db should still surface the pending columnar compaction",
            );
            assert!(
                reopened
                    .run_next_compaction()
                    .await
                    .expect("retry mixed-schema compaction after restart"),
            );
            assert_eq!(
                collect_rows(
                    reopened_table
                        .scan(Vec::new(), vec![0xff], ScanOptions::default())
                        .await
                        .expect("scan compacted mixed-schema rows"),
                )
                .await,
                vec![
                    (
                        b"user:1".to_vec(),
                        Value::named_record(
                            &successor_schema,
                            [
                                ("account_id", FieldValue::String("alice".to_string())),
                                ("count", FieldValue::Int64(0)),
                            ],
                        )
                        .expect("encode compacted alice"),
                    ),
                    (
                        b"user:2".to_vec(),
                        Value::named_record(
                            &successor_schema,
                            [
                                ("account_id", FieldValue::String("bob".to_string())),
                                ("count", FieldValue::Int64(7)),
                            ],
                        )
                        .expect("encode compacted bob"),
                    ),
                ],
            );

            Ok(())
        })
    }

    #[test]
    fn simulated_columnar_merge_compaction_recovers_after_crash() -> turmoil::Result {
        SeededSimulationRunner::new(0x27c0_1002).run_with(|context| async move {
            let config = tiered_config("/terracedb/sim/columnar-merge-compaction-crash");
            let db = context.open_db(config.clone()).await?;
            let table = db
                .create_table(merge_columnar_table_config("metrics", Some(2)))
                .await
                .expect("create merge-capable columnar table");

            table
                .write(b"doc".to_vec(), count_record(1))
                .await
                .expect("write base count");
            db.flush().await.expect("flush base count");
            table
                .merge(b"doc".to_vec(), count_record(2))
                .await
                .expect("write first delta");
            db.flush().await.expect("flush first delta");
            table
                .merge(b"doc".to_vec(), count_record(3))
                .await
                .expect("write second delta");
            db.flush().await.expect("flush second delta");

            assert_eq!(
                table
                    .read(b"doc".to_vec())
                    .await
                    .expect("resolve merge chain"),
                Some(count_record(6)),
            );
            assert!(
                db.pending_compaction_jobs()
                    .iter()
                    .any(|job| job.table_name == "metrics"),
                "columnar merge runs should schedule compaction before the crash",
            );

            let mut blocker = db.block_next_compaction_phase(CompactionPhase::OutputWritten);
            let compact_db = db.clone();
            let compaction = tokio::spawn(async move { compact_db.run_next_compaction().await });
            blocker.wait_until_reached().await;

            compaction.abort();
            let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
            let reopened_table = reopened
                .create_table(merge_columnar_table_config("metrics", Some(2)))
                .await
                .expect("reattach columnar merge operator after restart");

            assert_eq!(
                reopened_table
                    .read(b"doc".to_vec())
                    .await
                    .expect("recover merged value"),
                Some(count_record(6)),
            );
            assert!(
                reopened
                    .pending_compaction_jobs()
                    .iter()
                    .any(|job| job.table_name == "metrics"),
                "reopened db should still surface the pending merge compaction",
            );
            assert!(
                reopened
                    .run_next_compaction()
                    .await
                    .expect("retry merge compaction after restart"),
            );
            assert_eq!(
                reopened_table
                    .read(b"doc".to_vec())
                    .await
                    .expect("read merged value after retried compaction"),
                Some(count_record(6)),
            );

            Ok(())
        })
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
    async fn tiered_backup_restores_catalog_sstables_and_commit_log_tail_after_local_loss() {
        let primary_file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let primary_dependencies = dependencies(primary_file_system, object_store.clone());

        let db = Db::open(tiered_config("/remote-dr"), primary_dependencies)
            .await
            .expect("open primary db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create events table");
        let audit = db
            .create_table(row_table_config("audit"))
            .await
            .expect("create audit table");

        events
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write flushed value");
        db.flush().await.expect("flush backup snapshot");
        events
            .write(b"banana".to_vec(), Value::bytes("tail"))
            .await
            .expect("write tail value");
        audit
            .write(b"audit:1".to_vec(), Value::bytes("entry"))
            .await
            .expect("write audit tail");

        let restored_file_system = Arc::new(crate::StubFileSystem::default());
        let restored_dependencies = dependencies(restored_file_system, object_store);
        let restored = Db::open(tiered_config("/remote-dr"), restored_dependencies)
            .await
            .expect("restore from remote backup");

        let restored_events = restored.table("events");
        let restored_audit = restored.table("audit");
        assert_eq!(
            restored_events
                .read(b"apple".to_vec())
                .await
                .expect("read restored flushed value"),
            Some(Value::bytes("v1"))
        );
        assert_eq!(
            restored_events
                .read(b"banana".to_vec())
                .await
                .expect("read restored tail value"),
            Some(Value::bytes("tail"))
        );
        assert_eq!(
            restored_audit
                .read(b"audit:1".to_vec())
                .await
                .expect("read restored audit tail"),
            Some(Value::bytes("entry"))
        );
    }

    #[tokio::test]
    async fn tiered_backup_falls_back_to_generation_list_when_latest_pointer_is_corrupt() {
        let primary_file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let primary_dependencies = dependencies(primary_file_system, object_store.clone());

        let db = Db::open(
            tiered_config("/remote-latest-fallback"),
            primary_dependencies,
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
        db.flush().await.expect("flush first generation");
        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write v2");
        db.flush().await.expect("flush second generation");

        object_store
            .put(
                &tiered_layout().backup_manifest_latest(),
                &[0xff, 0xfe, 0xfd],
            )
            .await
            .expect("corrupt latest pointer");

        let restored = Db::open(
            tiered_config("/remote-latest-fallback"),
            dependencies(Arc::new(crate::StubFileSystem::default()), object_store),
        )
        .await
        .expect("recover using immutable manifest generations");
        let restored_table = restored.table("events");
        assert_eq!(
            restored_table
                .read(b"apple".to_vec())
                .await
                .expect("read restored value"),
            Some(Value::bytes("v2"))
        );
    }

    #[tokio::test]
    async fn orphaned_backup_sstables_are_harmless_before_manifest_publish() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db_dependencies = dependencies(file_system, object_store.clone());
        let layout = tiered_layout();

        let db = Db::open(tiered_config("/remote-orphan-manifest"), db_dependencies)
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        table
            .write(b"apple".to_vec(), Value::bytes("v1"))
            .await
            .expect("write first value");
        db.flush().await.expect("flush first generation");

        table
            .write(b"apple".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second value");
        object_store.inject_failure(ObjectStoreFailure::timeout(
            ObjectStoreOperation::Put,
            layout.backup_manifest(ManifestId::new(2)),
        ));
        db.flush()
            .await
            .expect("local flush should succeed despite backup manifest failure");

        let latest_pointer = String::from_utf8(
            object_store
                .get(&layout.backup_manifest_latest())
                .await
                .expect("read latest manifest pointer"),
        )
        .expect("decode latest manifest pointer");
        assert_eq!(
            latest_pointer.trim(),
            layout.backup_manifest(ManifestId::new(1))
        );
        assert_eq!(
            object_store
                .list(&layout.backup_sstable_prefix())
                .await
                .expect("list backup sstables")
                .len(),
            2,
            "the second SSTable upload should already exist even though its manifest did not publish"
        );

        let restored = Db::open(
            tiered_config("/remote-orphan-manifest"),
            dependencies(Arc::new(crate::StubFileSystem::default()), object_store),
        )
        .await
        .expect("recover from previous remote manifest and commit-log tail");
        let restored_table = restored.table("events");
        assert_eq!(
            restored_table
                .read(b"apple".to_vec())
                .await
                .expect("read recovered value"),
            Some(Value::bytes("v2"))
        );
    }

    #[tokio::test]
    async fn tiered_backup_gc_keeps_live_remote_objects_and_removes_old_backup_copies() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let clock = Arc::new(StubClock::default());
        let dependencies =
            dependencies_with_clock(file_system.clone(), object_store.clone(), clock.clone());
        let layout = tiered_layout();

        let setup = Db::open(
            tiered_config_with_max_local_bytes("/remote-gc-offload", 1024 * 1024),
            dependencies.clone(),
        )
        .await
        .expect("open setup db");
        let setup_table = setup
            .create_table(row_table_config("events"))
            .await
            .expect("create table");
        setup_table
            .write(b"apple".to_vec(), Value::Bytes(vec![b'a'; 256]))
            .await
            .expect("write first offload seed");
        setup.flush().await.expect("flush first offload seed");
        setup_table
            .write(b"banana".to_vec(), Value::Bytes(vec![b'b'; 256]))
            .await
            .expect("write second offload seed");
        setup.flush().await.expect("flush second offload seed");
        drop(setup_table);
        drop(setup);

        clock.advance(Duration::from_secs(120));

        let offload_db = Db::open(
            tiered_config_with_max_local_bytes("/remote-gc-offload", 1),
            dependencies,
        )
        .await
        .expect("reopen with offload budget");
        assert!(offload_db.run_next_offload().await.expect("run offload"));

        let backup_keys = object_store
            .list(&layout.backup_sstable_prefix())
            .await
            .expect("list backup copies");
        let cold_keys = object_store
            .list(&layout.cold_prefix())
            .await
            .expect("list cold objects");
        assert!(
            backup_keys.len() < 2,
            "GC should remove at least one unreferenced backup copy once the cold manifest is published"
        );
        assert!(
            !cold_keys.is_empty(),
            "the offloaded cold object(s) must remain live"
        );
        assert_eq!(
            backup_keys.len() + cold_keys.len(),
            2,
            "all live SSTables should still exist across the backup and cold prefixes"
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
    async fn compaction_filters_wait_for_active_snapshots_before_removing_expired_rows() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let clock = Arc::new(StubClock::new(Timestamp::new(10)));
        let db = Db::open(
            tiered_config("/ttl-snapshot-guard"),
            dependencies_with_clock(file_system, object_store, clock),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(ttl_row_table_config("events"))
            .await
            .expect("create ttl table");

        let expired = expiry_prefixed_value(5, "apple");
        table
            .write(b"apple".to_vec(), expired.clone())
            .await
            .expect("write expired value");
        db.flush().await.expect("flush expired value");
        let snapshot = db.snapshot().await;

        table
            .write(b"banana".to_vec(), expiry_prefixed_value(50, "banana"))
            .await
            .expect("write fresh value");
        db.flush().await.expect("flush fresh value");
        assert!(
            db.run_next_compaction()
                .await
                .expect("run guarded compaction")
        );

        assert_eq!(
            snapshot
                .read(&table, b"apple".to_vec())
                .await
                .expect("snapshot read"),
            Some(expired.clone())
        );
        assert_eq!(
            db.table_stats(&table).await.compaction_filter_removed_keys,
            0
        );

        snapshot.release();
        table
            .write(b"aardvark".to_vec(), expiry_prefixed_value(50, "aardvark"))
            .await
            .expect("write third value");
        db.flush().await.expect("flush third value");
        table
            .write(b"apricot".to_vec(), expiry_prefixed_value(50, "apricot"))
            .await
            .expect("write fourth value");
        db.flush().await.expect("flush fourth value");

        assert!(
            db.run_next_compaction()
                .await
                .expect("run compaction after snapshot release")
        );
        assert_eq!(
            table.read(b"apple".to_vec()).await.expect("expired read"),
            None
        );

        let stats = db.table_stats(&table).await;
        assert!(stats.compaction_filter_removed_bytes > 0);
        assert_eq!(stats.compaction_filter_removed_keys, 1);
    }

    #[tokio::test]
    async fn ttl_compaction_filter_advances_with_the_virtual_clock() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let clock = Arc::new(StubClock::new(Timestamp::new(10)));
        let db = Db::open(
            tiered_config("/ttl-virtual-clock"),
            dependencies_with_clock(file_system, object_store, clock.clone()),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(ttl_row_table_config("events"))
            .await
            .expect("create ttl table");

        let soon_expiring = expiry_prefixed_value(15, "apple");
        table
            .write(b"apple".to_vec(), soon_expiring.clone())
            .await
            .expect("write soon-expiring value");
        db.flush().await.expect("flush first value");
        table
            .write(b"banana".to_vec(), expiry_prefixed_value(50, "banana"))
            .await
            .expect("write stable value");
        db.flush().await.expect("flush second value");

        assert!(
            db.run_next_compaction()
                .await
                .expect("run first compaction")
        );
        assert_eq!(
            table
                .read(b"apple".to_vec())
                .await
                .expect("read before clock advance"),
            Some(soon_expiring)
        );
        assert_eq!(
            db.table_stats(&table).await.compaction_filter_removed_keys,
            0
        );

        clock.set(Timestamp::new(20));
        table
            .write(b"aardvark".to_vec(), expiry_prefixed_value(50, "aardvark"))
            .await
            .expect("write third value");
        db.flush().await.expect("flush third value");
        table
            .write(b"apricot".to_vec(), expiry_prefixed_value(50, "apricot"))
            .await
            .expect("write fourth value");
        db.flush().await.expect("flush fourth value");

        assert!(
            db.run_next_compaction()
                .await
                .expect("run second compaction")
        );
        assert_eq!(
            table
                .read(b"apple".to_vec())
                .await
                .expect("read after clock advance"),
            None
        );
        assert_eq!(
            db.table_stats(&table).await.compaction_filter_removed_keys,
            1
        );
    }

    #[tokio::test]
    async fn ttl_compaction_filter_uses_the_injected_clock_not_system_time() {
        let expired = expiry_prefixed_value(5, "apple");

        let keep_db = Db::open(
            tiered_config("/ttl-injected-clock-keep"),
            dependencies_with_clock(
                Arc::new(crate::StubFileSystem::default()),
                Arc::new(StubObjectStore::default()),
                Arc::new(StubClock::new(Timestamp::new(0))),
            ),
        )
        .await
        .expect("open keep db");
        let keep_table = keep_db
            .create_table(ttl_row_table_config("events"))
            .await
            .expect("create keep table");
        keep_table
            .write(b"apple".to_vec(), expired.clone())
            .await
            .expect("write keep apple");
        keep_db.flush().await.expect("flush keep apple");
        keep_table
            .write(b"banana".to_vec(), expiry_prefixed_value(50, "banana"))
            .await
            .expect("write keep banana");
        keep_db.flush().await.expect("flush keep banana");
        assert!(
            keep_db
                .run_next_compaction()
                .await
                .expect("compact keep db")
        );

        let remove_db = Db::open(
            tiered_config("/ttl-injected-clock-remove"),
            dependencies_with_clock(
                Arc::new(crate::StubFileSystem::default()),
                Arc::new(StubObjectStore::default()),
                Arc::new(StubClock::new(Timestamp::new(10))),
            ),
        )
        .await
        .expect("open remove db");
        let remove_table = remove_db
            .create_table(ttl_row_table_config("events"))
            .await
            .expect("create remove table");
        remove_table
            .write(b"apple".to_vec(), expired.clone())
            .await
            .expect("write remove apple");
        remove_db.flush().await.expect("flush remove apple");
        remove_table
            .write(b"banana".to_vec(), expiry_prefixed_value(50, "banana"))
            .await
            .expect("write remove banana");
        remove_db.flush().await.expect("flush remove banana");
        assert!(
            remove_db
                .run_next_compaction()
                .await
                .expect("compact remove db")
        );

        assert_eq!(
            keep_table
                .read(b"apple".to_vec())
                .await
                .expect("read keep apple"),
            Some(expired)
        );
        assert_eq!(
            remove_table
                .read(b"apple".to_vec())
                .await
                .expect("read removed apple"),
            None
        );
    }

    #[tokio::test]
    async fn filtered_compaction_without_manifest_switch_recovers_prior_generation() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let clock = Arc::new(StubClock::new(Timestamp::new(10)));
        let dependencies =
            dependencies_with_clock(file_system.clone(), object_store, clock.clone());
        let db = Db::open(
            tiered_config("/ttl-compaction-manifest-before-switch"),
            dependencies.clone(),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(ttl_row_table_config("events"))
            .await
            .expect("create ttl table");

        let expired = expiry_prefixed_value(5, "apple");
        let fresh = expiry_prefixed_value(50, "banana");
        table
            .write(b"apple".to_vec(), expired.clone())
            .await
            .expect("write expired value");
        db.flush().await.expect("flush first value");
        table
            .write(b"banana".to_vec(), fresh.clone())
            .await
            .expect("write fresh value");
        db.flush().await.expect("flush second value");

        let prior_generation = db.sstables_read().manifest_generation;
        let next_generation = ManifestId::new(prior_generation.get().saturating_add(1));
        let manifest_temp_path = format!(
            "{}{}",
            Db::local_manifest_path("/ttl-compaction-manifest-before-switch", next_generation),
            LOCAL_MANIFEST_TEMP_SUFFIX
        );
        file_system.inject_failure(FileSystemFailure::for_target(
            FileSystemOperation::Rename,
            manifest_temp_path,
            StorageError::io("simulated filtered manifest rename failure"),
        ));

        let error = db
            .run_next_compaction()
            .await
            .expect_err("filtered compaction should fail before manifest switch");
        assert!(
            error
                .message()
                .contains("simulated filtered manifest rename failure")
        );
        assert_eq!(
            table
                .read(b"apple".to_vec())
                .await
                .expect("read apple after failure"),
            Some(expired.clone())
        );

        file_system.crash();
        let reopened = Db::open(
            tiered_config("/ttl-compaction-manifest-before-switch"),
            dependencies,
        )
        .await
        .expect("reopen after failed filtered compaction");
        let reopened_table = reopened.table("events");

        assert_eq!(
            reopened.sstables_read().manifest_generation,
            prior_generation
        );
        assert_eq!(
            reopened_table
                .read(b"apple".to_vec())
                .await
                .expect("read apple after reopen"),
            Some(expired)
        );
        assert_eq!(
            reopened_table
                .read(b"banana".to_vec())
                .await
                .expect("read banana after reopen"),
            Some(fresh)
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
    async fn group_commit_sync_failure_flush_and_reopen_do_not_resurrect_failed_write() {
        let root = "/group-commit-sync-failure-flush";
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);
        let db = Db::open(tiered_config(root), dependencies.clone())
            .await
            .expect("open db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        file_system.inject_failure(FileSystemFailure::timeout(
            FileSystemOperation::Sync,
            commit_log_segment_path(root, 1),
        ));

        let error = events
            .write(b"user:failed".to_vec(), bytes("failed"))
            .await
            .expect_err("group-commit sync should fail");
        assert_group_commit_sync_failure(error);

        assert_eq!(
            events
                .read(b"user:failed".to_vec())
                .await
                .expect("read failed key after rejected commit"),
            None
        );
        assert_no_visible_or_durable_changes(&db, &events).await;

        db.flush()
            .await
            .expect("flush should not resurrect failed group-commit writes");

        assert_eq!(
            events
                .read(b"user:failed".to_vec())
                .await
                .expect("read failed key after flush"),
            None
        );
        assert_no_visible_or_durable_changes(&db, &events).await;

        file_system.crash();

        let reopened = Db::open(tiered_config(root), dependencies)
            .await
            .expect("reopen db");
        let reopened_events = reopened.table("events");

        assert_eq!(
            reopened_events
                .read(b"user:failed".to_vec())
                .await
                .expect("read failed key after reopen"),
            None
        );
        assert_no_visible_or_durable_changes(&reopened, &reopened_events).await;
    }

    #[tokio::test]
    async fn group_commit_sync_failure_keeps_failed_write_out_of_change_feeds_and_watermarks() {
        let root = "/group-commit-sync-failure-watermarks";
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);
        let db = Db::open(tiered_config(root), dependencies.clone())
            .await
            .expect("open db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");
        let mut visible = db.subscribe(&events);
        let mut durable = db.subscribe_durable(&events);

        file_system.inject_failure(FileSystemFailure::timeout(
            FileSystemOperation::Sync,
            commit_log_segment_path(root, 1),
        ));

        let error = events
            .write(b"user:failed".to_vec(), bytes("failed"))
            .await
            .expect_err("group-commit sync should fail");
        assert_group_commit_sync_failure(error);

        assert_eq!(
            events
                .read(b"user:failed".to_vec())
                .await
                .expect("read failed key after rejected commit"),
            None
        );
        assert_no_visible_or_durable_changes(&db, &events).await;
        assert_eq!(visible.current(), SequenceNumber::default());
        assert_eq!(durable.current(), SequenceNumber::default());
        assert!(
            !visible
                .has_changed()
                .expect("visible subscription should stay idle")
        );
        assert!(
            !durable
                .has_changed()
                .expect("durable subscription should stay idle")
        );

        let committed = events
            .write(b"user:ok".to_vec(), bytes("ok"))
            .await
            .expect("write later successful value");
        let expected = vec![collected_change(
            committed.get(),
            0,
            ChangeKind::Put,
            b"user:ok",
            Some(bytes("ok")),
            "events",
        )];

        assert_eq!(
            events
                .read(b"user:failed".to_vec())
                .await
                .expect("failed key should remain absent"),
            None
        );
        assert_eq!(
            events
                .read(b"user:ok".to_vec())
                .await
                .expect("read later successful key"),
            Some(bytes("ok"))
        );
        assert_eq!(
            visible.changed().await.expect("visible watermark update"),
            committed
        );
        assert_eq!(
            durable.changed().await.expect("durable watermark update"),
            committed
        );
        assert_eq!(visible.current(), committed);
        assert_eq!(durable.current(), committed);
        assert_eq!(visible_changes(&db, &events).await, expected);
        assert_eq!(durable_changes(&db, &events).await, expected);

        file_system.crash();

        let reopened = Db::open(tiered_config(root), dependencies)
            .await
            .expect("reopen db");
        let reopened_events = reopened.table("events");

        assert_eq!(
            reopened_events
                .read(b"user:failed".to_vec())
                .await
                .expect("failed key should stay absent after reopen"),
            None
        );
        assert_eq!(
            reopened_events
                .read(b"user:ok".to_vec())
                .await
                .expect("read successful key after reopen"),
            Some(bytes("ok"))
        );
        assert_eq!(visible_changes(&reopened, &reopened_events).await, expected);
        assert_eq!(durable_changes(&reopened, &reopened_events).await, expected);
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

    #[tokio::test]
    async fn randomized_merge_read_path_matches_merge_shadow_oracle_across_flushes_and_compaction()
    {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/randomized-merge-read-oracle"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(merge_row_table_config("events", Some(3)))
            .await
            .expect("create merge table");
        let rng = StubRng::seeded(0x1a2b_3c4d_5e6f_7081);
        let keys = [
            b"apple".to_vec(),
            b"apricot".to_vec(),
            b"banana".to_vec(),
            b"blueberry".to_vec(),
            b"cherry".to_vec(),
        ];
        let mut oracle = MergeShadowOracle::default();

        for step in 0..64_u64 {
            let key = keys[(rng.next_u64() as usize) % keys.len()].clone();
            let committed = match rng.next_u64() % 6 {
                0 => {
                    let sequence = table
                        .delete(key.clone())
                        .await
                        .expect("delete key in randomized merge oracle test");
                    oracle.apply(sequence, MergePointMutation::Delete { key });
                    sequence
                }
                1..=3 => {
                    let value = format!("merge-{step}").into_bytes();
                    let sequence = table
                        .merge(key.clone(), Value::bytes(value.clone()))
                        .await
                        .expect("merge key in randomized merge oracle test");
                    oracle.apply(sequence, MergePointMutation::Merge { key, value });
                    sequence
                }
                _ => {
                    let value = format!("put-{step}").into_bytes();
                    let sequence = table
                        .write(key.clone(), Value::bytes(value.clone()))
                        .await
                        .expect("put key in randomized merge oracle test");
                    oracle.apply(sequence, MergePointMutation::Put { key, value });
                    sequence
                }
            };

            if rng.next_u64().is_multiple_of(3) {
                db.flush().await.expect("flush randomized merge state");
            }
            if rng.next_u64().is_multiple_of(5) {
                let _ = db
                    .run_next_compaction()
                    .await
                    .expect("run merge compaction during randomized workload");
            }

            let historical = SequenceNumber::new(rng.next_u64() % (committed.get() + 1));
            for key in &keys {
                assert_eq!(
                    table
                        .read_at(key.clone(), historical)
                        .await
                        .expect("historical merge point read"),
                    oracle.value_at(key, historical)
                );
            }

            let rows = collect_rows(
                table
                    .scan_at(
                        b"a".to_vec(),
                        b"z".to_vec(),
                        historical,
                        ScanOptions::default(),
                    )
                    .await
                    .expect("historical merge scan"),
            )
            .await;
            assert_eq!(
                rows,
                merge_oracle_rows(&oracle, historical, b"a", b"z", false, None)
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
                    .expect("historical reverse merge scan"),
            )
            .await;
            assert_eq!(
                reverse_rows,
                merge_oracle_rows(&oracle, historical, b"a", b"z", true, Some(reverse_limit))
            );

            let prefix_rows = collect_rows(
                table
                    .scan_prefix_at(b"ap".to_vec(), historical, ScanOptions::default())
                    .await
                    .expect("historical merge prefix scan"),
            )
            .await;
            assert_eq!(
                prefix_rows,
                merge_oracle_prefix_rows(&oracle, historical, b"ap", false, None)
            );
        }
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
    async fn crash_drops_unflushed_merge_collapse_shadow_but_preserves_unresolved_operands() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);
        let db = Db::open(tiered_config("/merge-collapse-crash"), dependencies.clone())
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
            "collapse shadow row should not exist before the read"
        );
        assert_eq!(
            table
                .read(b"doc".to_vec())
                .await
                .expect("resolve long chain before crash"),
            Some(bytes("seed|A|B|C"))
        );
        assert_eq!(
            db.memtables_read()
                .read_at(table_id, b"doc", latest)
                .expect("collapse shadow row should be installed")
                .kind,
            ChangeKind::Put
        );

        file_system.crash();

        let reopened = Db::open(tiered_config("/merge-collapse-crash"), dependencies)
            .await
            .expect("reopen db after crash");
        let reopened_table = reopened
            .create_table(merge_row_table_config("events", Some(2)))
            .await
            .expect("reattach merge operator through public API");
        let reopened_table_id = reopened_table.id().expect("table id after reopen");

        assert!(
            reopened
                .memtables_read()
                .read_at(reopened_table_id, b"doc", latest)
                .is_none(),
            "unflushed collapse shadow row should not survive crash recovery"
        );
        assert!(
            reopened
                .sstables_read()
                .live
                .iter()
                .flat_map(|sstable| sstable.rows.iter())
                .any(|row| row.key == b"doc" && row.kind == ChangeKind::Merge),
            "recovery should still have unresolved merge operands available"
        );
        assert_eq!(
            reopened_table
                .read(b"doc".to_vec())
                .await
                .expect("resolve long chain after crash"),
            Some(bytes("seed|A|B|C"))
        );
        assert_eq!(
            reopened
                .memtables_read()
                .read_at(reopened_table_id, b"doc", latest)
                .expect("read after crash should re-install collapse shadow row")
                .kind,
            ChangeKind::Put
        );
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
        let dependencies = dependencies(file_system.clone(), object_store);
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

        file_system.crash();

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
    async fn subscriptions_start_from_per_table_watermarks_not_global_prefix() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/subscription-table-watermarks"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let users = db
            .create_table(row_table_config("users"))
            .await
            .expect("create users table");
        let orders = db
            .create_table(row_table_config("orders"))
            .await
            .expect("create orders table");

        let committed = users
            .write(b"user:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("write users row");

        assert_eq!(db.current_sequence(), committed);
        assert_eq!(db.subscribe(&users).current(), committed);
        assert_eq!(db.subscribe_durable(&users).current(), committed);
        assert_eq!(db.subscribe(&orders).current(), SequenceNumber::new(0));
        assert_eq!(
            db.subscribe_durable(&orders).current(),
            SequenceNumber::new(0)
        );
    }

    #[tokio::test]
    async fn cloned_subscribers_share_notifications_and_drop_cleans_up_registry() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/subscription-drop-cleanup"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let mut first = db.subscribe(&table);
        assert_eq!(db.visible_subscriber_count(&table), 1);
        let mut second = first.clone();
        assert_eq!(db.visible_subscriber_count(&table), 2);

        let first_sequence = table
            .write(b"user:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("first write");
        assert_eq!(first.changed().await.expect("first wake"), first_sequence);
        assert_eq!(second.changed().await.expect("second wake"), first_sequence);

        drop(second);
        assert_eq!(db.visible_subscriber_count(&table), 1);
        drop(first);
        assert_eq!(db.visible_subscriber_count(&table), 0);

        let latest = table
            .write(b"user:2".to_vec(), Value::bytes("v2"))
            .await
            .expect("second write");
        assert_eq!(db.subscribe(&table).current(), latest);
    }

    #[tokio::test]
    async fn subscriptions_coalesce_multiple_commits_before_receivers_wake() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/subscription-coalescing"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let mut receiver = db.subscribe(&table);
        let first = table
            .write(b"user:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("write one");
        let _second = table
            .write(b"user:2".to_vec(), Value::bytes("v2"))
            .await
            .expect("write two");
        let third = table
            .write(b"user:3".to_vec(), Value::bytes("v3"))
            .await
            .expect("write three");

        assert_eq!(first, SequenceNumber::new(1));
        assert_eq!(receiver.current(), third);
        assert_eq!(receiver.changed().await.expect("coalesced wake"), third);
        assert!(
            !receiver
                .has_changed()
                .expect("receiver should still be open after coalesced wake"),
            "coalescing subscriptions should not queue one wake per commit"
        );
    }

    #[tokio::test]
    async fn visible_and_durable_subscriptions_diverge_until_flush_in_deferred_mode() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config_with_durability(
                "/subscription-durable-divergence",
                TieredDurabilityMode::Deferred,
            ),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let mut visible = db.subscribe(&table);
        let mut durable = db.subscribe_durable(&table);
        let mut durable_set = db.subscribe_durable_set([&table]);
        assert_eq!(db.visible_subscriber_count(&table), 1);
        assert_eq!(db.durable_subscriber_count(&table), 2);

        let committed = table
            .write(b"user:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("deferred write");

        assert_eq!(visible.changed().await.expect("visible wake"), committed);
        assert_eq!(visible.current(), committed);
        assert_eq!(durable.current(), SequenceNumber::new(0));
        assert!(
            !durable
                .has_changed()
                .expect("durable receiver should remain open before flush"),
            "durable subscriptions should not advance before flush"
        );

        db.flush().await.expect("flush deferred writes");
        assert_eq!(durable.changed().await.expect("durable wake"), committed);
        assert_eq!(
            durable_set.changed().await.expect("durable set wake"),
            vec![WatermarkUpdate {
                table: "events".to_string(),
                sequence: committed,
            }]
        );

        drop(visible);
        drop(durable);
        assert_eq!(db.visible_subscriber_count(&table), 0);
        drop(durable_set);
        assert_eq!(db.durable_subscriber_count(&table), 0);
    }

    #[tokio::test]
    async fn merged_subscription_sets_drain_pending_work_in_deterministic_table_order() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/merged-subscription-set"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let users = db
            .create_table(row_table_config("users"))
            .await
            .expect("create users table");
        let orders = db
            .create_table(row_table_config("orders"))
            .await
            .expect("create orders table");

        let mut merged = db.subscribe_visible_set([&users, &orders]);
        assert!(merged.drain_pending().is_empty());

        let users_first = users
            .write(b"user:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("write users row");
        let orders_first = orders
            .write(b"order:1".to_vec(), Value::bytes("v1"))
            .await
            .expect("write orders row");
        assert_eq!(
            merged.drain_pending(),
            vec![
                WatermarkUpdate {
                    table: "orders".to_string(),
                    sequence: orders_first,
                },
                WatermarkUpdate {
                    table: "users".to_string(),
                    sequence: users_first,
                },
            ]
        );
        assert!(merged.drain_pending().is_empty());

        let orders_second = orders
            .write(b"order:2".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second orders row");
        let _users_second = users
            .write(b"user:2".to_vec(), Value::bytes("v2"))
            .await
            .expect("write second users row");
        let users_third = users
            .write(b"user:3".to_vec(), Value::bytes("v3"))
            .await
            .expect("write third users row");

        let updates = tokio::time::timeout(Duration::from_millis(50), merged.changed())
            .await
            .expect("merged receiver should drain pending work without blocking")
            .expect("merged receiver should stay open");
        assert_eq!(
            updates,
            vec![
                WatermarkUpdate {
                    table: "orders".to_string(),
                    sequence: orders_second,
                },
                WatermarkUpdate {
                    table: "users".to_string(),
                    sequence: users_third,
                },
            ]
        );
        assert!(merged.drain_pending().is_empty());
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

    #[tokio::test]
    async fn s3_primary_flush_persists_state_and_durable_change_feed_across_reopen() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);
        let config = s3_primary_config("s3-flush-reopen");

        let db = Db::open(config.clone(), dependencies.clone())
            .await
            .expect("open s3-primary db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create events table");

        let first = events
            .write(b"user:1".to_vec(), bytes("v1"))
            .await
            .expect("write first value");
        let second = events
            .write(b"user:2".to_vec(), bytes("v2"))
            .await
            .expect("write second value");

        let visible_before_flush = collect_changes(
            db.scan_since(&events, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("visible change feed before flush"),
        )
        .await;
        assert_eq!(db.current_sequence(), second);
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));
        assert!(
            collect_changes(
                db.scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
                    .await
                    .expect("durable change feed before flush"),
            )
            .await
            .is_empty()
        );

        db.flush().await.expect("flush s3-primary state");
        assert_eq!(db.current_durable_sequence(), second);

        let durable_after_flush = collect_changes(
            db.scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("durable change feed after flush"),
        )
        .await;
        assert_eq!(durable_after_flush, visible_before_flush);

        let reopened = Db::open(config, dependencies)
            .await
            .expect("reopen durable db");
        let reopened_events = reopened.table("events");
        assert_eq!(reopened.current_sequence(), second);
        assert_eq!(reopened.current_durable_sequence(), second);
        assert_eq!(
            reopened_events
                .read(b"user:1".to_vec())
                .await
                .expect("read first durable value"),
            Some(bytes("v1"))
        );
        assert_eq!(
            reopened_events
                .read(b"user:2".to_vec())
                .await
                .expect("read second durable value"),
            Some(bytes("v2"))
        );
        assert_eq!(
            collect_changes(
                reopened
                    .scan_since(
                        &reopened_events,
                        LogCursor::beginning(),
                        ScanOptions::default(),
                    )
                    .await
                    .expect("reopened visible change feed"),
            )
            .await,
            visible_before_flush
        );
        assert_eq!(first, SequenceNumber::new(1));
    }

    #[tokio::test]
    async fn s3_primary_visible_scan_is_hybrid_but_new_process_reads_only_flushed_state() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store);
        let config = s3_primary_config("s3-hybrid-visible");

        let db = Db::open(config.clone(), dependencies.clone())
            .await
            .expect("open s3-primary db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create events table");

        let durable = events
            .write(b"user:1".to_vec(), bytes("durable"))
            .await
            .expect("write durable event");
        db.flush().await.expect("flush durable event");

        let visible_only = events
            .write(b"user:2".to_vec(), bytes("visible-only"))
            .await
            .expect("write visible-only event");
        assert_eq!(db.current_sequence(), visible_only);
        assert_eq!(db.current_durable_sequence(), durable);

        let visible = collect_changes(
            db.scan_since(&events, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("same-process visible change feed"),
        )
        .await;
        let durable_only = collect_changes(
            db.scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("same-process durable change feed"),
        )
        .await;
        assert_eq!(visible.len(), 2);
        assert_eq!(durable_only.len(), 1);
        assert_eq!(durable_only[0].sequence, durable);
        assert_eq!(visible[1].sequence, visible_only);

        let peer = Db::open(config, dependencies)
            .await
            .expect("open second s3-primary process");
        let peer_events = peer.table("events");
        assert_eq!(peer.current_sequence(), durable);
        assert_eq!(peer.current_durable_sequence(), durable);
        assert_eq!(
            peer_events
                .read(b"user:1".to_vec())
                .await
                .expect("peer durable read"),
            Some(bytes("durable"))
        );
        assert_eq!(
            peer_events
                .read(b"user:2".to_vec())
                .await
                .expect("peer should not see visible-only row"),
            None
        );
        assert_eq!(
            collect_changes(
                peer.scan_since(&peer_events, LogCursor::beginning(), ScanOptions::default())
                    .await
                    .expect("peer visible scan should be durable-only"),
            )
            .await,
            durable_only
        );
    }

    #[tokio::test]
    async fn s3_primary_crash_recovery_drops_unflushed_visible_tail() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);
        let config = s3_primary_config("s3-crash-tail-loss");

        let db = Db::open(config.clone(), dependencies.clone())
            .await
            .expect("open s3-primary db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create events table");

        let durable = events
            .write(b"user:1".to_vec(), bytes("durable"))
            .await
            .expect("write durable value");
        db.flush().await.expect("flush durable value");

        let volatile = events
            .write(b"user:1".to_vec(), bytes("volatile"))
            .await
            .expect("write volatile tail");
        assert_eq!(db.current_sequence(), volatile);
        assert_eq!(db.current_durable_sequence(), durable);

        file_system.crash();

        let reopened = Db::open(config, dependencies)
            .await
            .expect("reopen after simulated crash");
        let reopened_events = reopened.table("events");
        assert_eq!(reopened.current_sequence(), durable);
        assert_eq!(reopened.current_durable_sequence(), durable);
        assert_eq!(
            reopened_events
                .read(b"user:1".to_vec())
                .await
                .expect("recovered durable value"),
            Some(bytes("durable"))
        );
        let recovered_visible = collect_changes(
            reopened
                .scan_since(
                    &reopened_events,
                    LogCursor::beginning(),
                    ScanOptions::default(),
                )
                .await
                .expect("recovered visible change feed"),
        )
        .await;
        assert_eq!(recovered_visible.len(), 1);
        assert_eq!(recovered_visible[0].sequence, durable);
    }

    #[tokio::test]
    async fn s3_primary_failed_manifest_upload_preserves_last_durable_prefix_until_retry() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system, object_store.clone());
        let config = s3_primary_config("s3-flush-failure");

        let db = Db::open(config.clone(), dependencies.clone())
            .await
            .expect("open s3-primary db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create events table");

        let first = events
            .write(b"user:1".to_vec(), bytes("v1"))
            .await
            .expect("write first value");
        db.flush().await.expect("flush first value");

        let second = events
            .write(b"user:1".to_vec(), bytes("v2"))
            .await
            .expect("write second value");
        let StorageConfig::S3Primary(s3_config) = &config.storage else {
            panic!("test config should use s3-primary storage");
        };
        object_store.inject_failure(ObjectStoreFailure::timeout(
            ObjectStoreOperation::Put,
            Db::remote_manifest_path(s3_config, ManifestId::new(2)),
        ));

        db.flush().await.expect_err("manifest upload should fail");
        assert_eq!(db.current_sequence(), second);
        assert_eq!(db.current_durable_sequence(), first);
        assert_eq!(
            events
                .read(b"user:1".to_vec())
                .await
                .expect("same-process visible read after failed flush"),
            Some(bytes("v2"))
        );

        let peer_before_retry = Db::open(config.clone(), dependencies.clone())
            .await
            .expect("open peer after failed flush");
        let peer_table = peer_before_retry.table("events");
        assert_eq!(peer_before_retry.current_sequence(), first);
        assert_eq!(peer_before_retry.current_durable_sequence(), first);
        assert_eq!(
            peer_table
                .read(b"user:1".to_vec())
                .await
                .expect("peer should only see last durable value"),
            Some(bytes("v1"))
        );

        db.flush().await.expect("retry flush should succeed");
        assert_eq!(db.current_durable_sequence(), second);

        let peer_after_retry = Db::open(config, dependencies)
            .await
            .expect("open peer after successful retry");
        let peer_after_retry_table = peer_after_retry.table("events");
        assert_eq!(peer_after_retry.current_sequence(), second);
        assert_eq!(peer_after_retry.current_durable_sequence(), second);
        assert_eq!(
            peer_after_retry_table
                .read(b"user:1".to_vec())
                .await
                .expect("peer should observe retried durable value"),
            Some(bytes("v2"))
        );
    }

    #[tokio::test]
    async fn scan_since_resumes_after_cursor_within_interleaved_batch() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/cdc-resume"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create events table");
        let audit = db
            .create_table(row_table_config("audit"))
            .await
            .expect("create audit table");

        let mut batch = db.write_batch();
        batch.put(&events, b"user:1".to_vec(), bytes("v1"));
        batch.put(&audit, b"audit:1".to_vec(), bytes("ignored"));
        batch.delete(&events, b"user:2".to_vec());
        batch.put(&events, b"user:3".to_vec(), bytes("v3"));
        let first_sequence = db
            .commit(batch, CommitOptions::default())
            .await
            .expect("commit interleaved batch");
        let second_sequence = events
            .write(b"user:4".to_vec(), bytes("v4"))
            .await
            .expect("write trailing event");

        let all = collect_changes(
            db.scan_since(&events, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("scan all changes"),
        )
        .await;
        assert_eq!(
            all,
            vec![
                collected_change(
                    1,
                    0,
                    ChangeKind::Put,
                    b"user:1",
                    Some(bytes("v1")),
                    "events"
                ),
                collected_change(1, 2, ChangeKind::Delete, b"user:2", None, "events"),
                collected_change(
                    1,
                    3,
                    ChangeKind::Put,
                    b"user:3",
                    Some(bytes("v3")),
                    "events"
                ),
                collected_change(
                    second_sequence.get(),
                    0,
                    ChangeKind::Put,
                    b"user:4",
                    Some(bytes("v4")),
                    "events",
                ),
            ]
        );

        let resumed = collect_changes(
            db.scan_since(
                &events,
                LogCursor::new(first_sequence, 0),
                ScanOptions::default(),
            )
            .await
            .expect("resume after first entry"),
        )
        .await;
        assert_eq!(
            resumed,
            vec![
                collected_change(1, 2, ChangeKind::Delete, b"user:2", None, "events"),
                collected_change(
                    1,
                    3,
                    ChangeKind::Put,
                    b"user:3",
                    Some(bytes("v3")),
                    "events"
                ),
                collected_change(
                    second_sequence.get(),
                    0,
                    ChangeKind::Put,
                    b"user:4",
                    Some(bytes("v4")),
                    "events",
                ),
            ]
        );
    }

    #[tokio::test]
    async fn visible_change_scans_can_lead_durable_scans_in_deferred_mode() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config_with_durability(
                "/cdc-visible-vs-durable",
                TieredDurabilityMode::Deferred,
            ),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open deferred db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        let mut batch = db.write_batch();
        batch.put(&events, b"user:1".to_vec(), bytes("v1"));
        batch.delete(&events, b"user:2".to_vec());
        let committed = db
            .commit(batch, CommitOptions::default())
            .await
            .expect("commit deferred batch");

        assert_eq!(db.current_sequence(), committed);
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));

        let visible = collect_changes(
            db.scan_since(&events, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("scan visible changes"),
        )
        .await;
        assert_eq!(
            visible,
            vec![
                collected_change(
                    1,
                    0,
                    ChangeKind::Put,
                    b"user:1",
                    Some(bytes("v1")),
                    "events"
                ),
                collected_change(1, 1, ChangeKind::Delete, b"user:2", None, "events"),
            ]
        );
        assert!(
            visible
                .iter()
                .all(|change| change.sequence <= db.current_sequence())
        );

        let durable_before_flush = collect_changes(
            db.scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("scan durable changes before flush"),
        )
        .await;
        assert!(durable_before_flush.is_empty());

        db.flush().await.expect("flush deferred changes");

        let durable_after_flush = collect_changes(
            db.scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
                .await
                .expect("scan durable changes after flush"),
        )
        .await;
        assert_eq!(durable_after_flush, visible);
        assert!(
            durable_after_flush
                .iter()
                .all(|change| change.sequence <= db.current_durable_sequence())
        );
    }

    #[tokio::test]
    async fn deferred_crash_recovery_hides_non_durable_change_feed_entries() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(
            tiered_config_with_durability("/cdc-deferred-recovery", TieredDurabilityMode::Deferred),
            dependencies.clone(),
        )
        .await
        .expect("open deferred db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create table");

        events
            .write(b"user:1".to_vec(), bytes("volatile"))
            .await
            .expect("write volatile event");
        assert_eq!(db.current_sequence(), SequenceNumber::new(1));
        assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));

        file_system.crash();

        let reopened = Db::open(
            tiered_config_with_durability("/cdc-deferred-recovery", TieredDurabilityMode::Deferred),
            dependencies,
        )
        .await
        .expect("reopen deferred db");
        let reopened_events = reopened.table("events");

        assert_eq!(reopened.current_sequence(), SequenceNumber::new(0));
        assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(0));
        assert!(
            collect_changes(
                reopened
                    .scan_since(
                        &reopened_events,
                        LogCursor::beginning(),
                        ScanOptions::default(),
                    )
                    .await
                    .expect("scan visible changes after crash"),
            )
            .await
            .is_empty()
        );
        assert!(
            collect_changes(
                reopened
                    .scan_durable_since(
                        &reopened_events,
                        LogCursor::beginning(),
                        ScanOptions::default(),
                    )
                    .await
                    .expect("scan durable changes after crash"),
            )
            .await
            .is_empty()
        );
    }

    #[tokio::test]
    async fn change_feed_order_matches_committed_write_order_exactly_after_reopen() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/cdc-ordering"), dependencies.clone())
            .await
            .expect("open db");
        let events = db
            .create_table(row_table_config("events"))
            .await
            .expect("create events table");
        let audit = db
            .create_table(row_table_config("audit"))
            .await
            .expect("create audit table");

        let mut expected = Vec::new();

        let mut first = db.write_batch();
        first.put(&events, b"apple".to_vec(), bytes("v1"));
        first.put(&audit, b"audit:1".to_vec(), bytes("ignore-1"));
        first.delete(&events, b"banana".to_vec());
        let first_sequence = db
            .commit(first, CommitOptions::default())
            .await
            .expect("commit first batch");
        expected.push(collected_change(
            first_sequence.get(),
            0,
            ChangeKind::Put,
            b"apple",
            Some(bytes("v1")),
            "events",
        ));
        expected.push(collected_change(
            first_sequence.get(),
            2,
            ChangeKind::Delete,
            b"banana",
            None,
            "events",
        ));

        db.flush().await.expect("flush after first batch");

        let mut second = db.write_batch();
        second.put(&audit, b"audit:2".to_vec(), bytes("ignore-2"));
        second.put(&events, b"carrot".to_vec(), bytes("v3"));
        let second_sequence = db
            .commit(second, CommitOptions::default())
            .await
            .expect("commit second batch");
        expected.push(collected_change(
            second_sequence.get(),
            1,
            ChangeKind::Put,
            b"carrot",
            Some(bytes("v3")),
            "events",
        ));

        let mut third = db.write_batch();
        third.put(&events, b"apple".to_vec(), bytes("v2"));
        third.delete(&audit, b"audit:1".to_vec());
        third.delete(&events, b"carrot".to_vec());
        let third_sequence = db
            .commit(third, CommitOptions::default())
            .await
            .expect("commit third batch");
        expected.push(collected_change(
            third_sequence.get(),
            0,
            ChangeKind::Put,
            b"apple",
            Some(bytes("v2")),
            "events",
        ));
        expected.push(collected_change(
            third_sequence.get(),
            2,
            ChangeKind::Delete,
            b"carrot",
            None,
            "events",
        ));

        file_system.crash();

        let reopened = Db::open(tiered_config("/cdc-ordering"), dependencies)
            .await
            .expect("reopen db");
        let reopened_events = reopened.table("events");

        let actual = collect_changes(
            reopened
                .scan_since(
                    &reopened_events,
                    LogCursor::beginning(),
                    ScanOptions::default(),
                )
                .await
                .expect("scan all reopened changes"),
        )
        .await;
        assert_eq!(actual, expected);

        let first_page = collect_changes(
            reopened
                .scan_since(
                    &reopened_events,
                    LogCursor::beginning(),
                    ScanOptions {
                        limit: Some(2),
                        ..ScanOptions::default()
                    },
                )
                .await
                .expect("scan first page"),
        )
        .await;
        assert_eq!(first_page, expected[..2].to_vec());

        let resumed = collect_changes(
            reopened
                .scan_since(
                    &reopened_events,
                    first_page
                        .last()
                        .expect("first page should contain entries")
                        .cursor,
                    ScanOptions::default(),
                )
                .await
                .expect("resume after first page"),
        )
        .await;
        assert_eq!(resumed, expected[2..].to_vec());
    }

    #[tokio::test]
    async fn change_feed_retention_returns_snapshot_too_old_and_reports_stats() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/cdc-retention-stats"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let table = db
            .create_table(row_table_config_with_history_retention("events", 2))
            .await
            .expect("create events table");

        let first = table
            .write(b"user:1".to_vec(), bytes("v1"))
            .await
            .expect("write first event");
        let oldest_available = table
            .write(b"user:2".to_vec(), bytes("v2"))
            .await
            .expect("write second event");
        table
            .write(b"user:3".to_vec(), bytes("v3"))
            .await
            .expect("write third event");

        assert_change_feed_snapshot_too_old(
            db.scan_since(&table, LogCursor::new(first, 0), ScanOptions::default())
                .await
                .err()
                .expect("visible change feed should be too old"),
            first,
            oldest_available,
        );
        assert_change_feed_snapshot_too_old(
            db.scan_durable_since(&table, LogCursor::new(first, 0), ScanOptions::default())
                .await
                .err()
                .expect("durable change feed should be too old"),
            first,
            oldest_available,
        );

        let stats = db.table_stats(&table).await;
        assert_eq!(
            stats.change_feed_oldest_available_sequence,
            Some(SequenceNumber::new(1))
        );
        assert_eq!(stats.change_feed_floor_sequence, Some(oldest_available));
        assert_eq!(
            stats.commit_log_recovery_floor_sequence,
            SequenceNumber::new(0)
        );
        assert_eq!(stats.commit_log_gc_floor_sequence, SequenceNumber::new(0));
        assert!(!stats.change_feed_pins_commit_log_gc);
    }

    #[tokio::test]
    async fn physically_retained_segments_can_still_be_logically_too_old_for_other_tables() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let db = Db::open(
            tiered_config("/cdc-physical-vs-logical"),
            dependencies(file_system, object_store),
        )
        .await
        .expect("open db");
        let slow = db
            .create_table(row_table_config_with_history_retention("slow", 8))
            .await
            .expect("create slow table");
        let fast = db
            .create_table(row_table_config_with_history_retention("fast", 1))
            .await
            .expect("create fast table");

        let mut first = db.write_batch();
        first.put(&slow, b"slow:1".to_vec(), bytes("s1"));
        first.put(&fast, b"fast:1".to_vec(), bytes("f1"));
        let first_sequence = db
            .commit(first, CommitOptions::default())
            .await
            .expect("commit first batch");
        force_seal_commit_log(&db).await;

        let mut second = db.write_batch();
        second.put(&slow, b"slow:2".to_vec(), bytes("s2"));
        second.put(&fast, b"fast:2".to_vec(), bytes("f2"));
        db.commit(second, CommitOptions::default())
            .await
            .expect("commit second batch");
        force_seal_commit_log(&db).await;
        db.flush().await.expect("flush durable state");

        assert!(
            commit_log_segment_ids(&db)
                .await
                .contains(&SegmentId::new(1)),
            "the oldest commit-log segment should stay physically retained for the slow table"
        );

        assert_change_feed_snapshot_too_old(
            db.scan_since(
                &fast,
                LogCursor::new(first_sequence, 1),
                ScanOptions::default(),
            )
            .await
            .err()
            .expect("fast table should already be past its logical retention floor"),
            first_sequence,
            SequenceNumber::new(2),
        );

        assert_eq!(
            collect_changes(
                db.scan_since(&slow, LogCursor::beginning(), ScanOptions::default())
                    .await
                    .expect("slow table should still scan retained history"),
            )
            .await,
            vec![
                collected_change(1, 0, ChangeKind::Put, b"slow:1", Some(bytes("s1")), "slow",),
                collected_change(2, 0, ChangeKind::Put, b"slow:2", Some(bytes("s2")), "slow",),
            ]
        );

        let fast_stats = db.table_stats(&fast).await;
        assert_eq!(
            fast_stats.change_feed_floor_sequence,
            Some(SequenceNumber::new(2))
        );
        assert_eq!(
            fast_stats.change_feed_oldest_available_sequence,
            Some(SequenceNumber::new(1))
        );
        assert!(!fast_stats.change_feed_pins_commit_log_gc);
    }

    #[tokio::test]
    async fn recovery_only_log_gc_drops_segments_once_a_flush_makes_them_unnecessary() {
        let file_system = Arc::new(crate::StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let dependencies = dependencies(file_system.clone(), object_store);

        let db = Db::open(tiered_config("/cdc-recovery-only-gc"), dependencies.clone())
            .await
            .expect("open db");
        let table = db
            .create_table(row_table_config("events"))
            .await
            .expect("create events table");

        let first = table
            .write(b"user:1".to_vec(), bytes("v1"))
            .await
            .expect("write first event");
        force_seal_commit_log(&db).await;

        table
            .write(b"user:2".to_vec(), bytes("v2"))
            .await
            .expect("write second event");
        force_seal_commit_log(&db).await;

        assert_eq!(
            commit_log_segment_ids(&db).await,
            vec![SegmentId::new(1), SegmentId::new(2), SegmentId::new(3)]
        );

        db.flush().await.expect("flush recovery state");
        assert_eq!(
            commit_log_segment_ids(&db).await,
            vec![SegmentId::new(2), SegmentId::new(3)]
        );

        file_system.crash();

        let reopened = Db::open(tiered_config("/cdc-recovery-only-gc"), dependencies)
            .await
            .expect("reopen db");
        let reopened_table = reopened.table("events");

        assert_eq!(
            reopened_table
                .read(b"user:2".to_vec())
                .await
                .expect("read latest value"),
            Some(bytes("v2"))
        );
        assert_change_feed_snapshot_too_old(
            reopened
                .scan_since(
                    &reopened_table,
                    LogCursor::new(first, 0),
                    ScanOptions::default(),
                )
                .await
                .err()
                .expect("recovery-only GC should make the oldest cursor too old"),
            first,
            SequenceNumber::new(2),
        );
    }
}
