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
    ids::{FieldId, LogCursor, SequenceNumber, TableId},
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

#[derive(Clone)]
pub struct Db {
    inner: Arc<DbInner>,
}

struct DbInner {
    config: DbConfig,
    dependencies: DbDependencies,
    catalog_location: CatalogLocation,
    catalog_write_lock: AsyncMutex<()>,
    next_table_id: AtomicU32,
    current_sequence: AtomicU64,
    current_durable_sequence: AtomicU64,
    tables: RwLock<BTreeMap<String, StoredTable>>,
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
                next_table_id: AtomicU32::new(next_table_id),
                current_sequence: AtomicU64::new(0),
                current_durable_sequence: AtomicU64::new(0),
                tables: RwLock::new(tables),
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
        Snapshot {
            db: self.clone(),
            sequence: self.current_sequence(),
            released: Arc::new(AtomicBool::new(false)),
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

        let sequence =
            SequenceNumber::new(self.inner.current_sequence.fetch_add(1, Ordering::SeqCst) + 1);

        let touched_tables: BTreeSet<String> = batch
            .operations()
            .iter()
            .map(|op| match op {
                BatchOperation::Put { table, .. }
                | BatchOperation::Merge { table, .. }
                | BatchOperation::Delete { table, .. } => table.name().to_string(),
            })
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

        TableStats {
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

    pub async fn read(&self, _key: Key) -> Result<Option<Value>, ReadError> {
        Ok(None)
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
        _start: Key,
        _end: Key,
        _opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(Box::pin(stream::empty()))
    }

    pub async fn scan_prefix(
        &self,
        _prefix: KeyPrefix,
        _opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(Box::pin(stream::empty()))
    }

    pub async fn read_at(
        &self,
        _key: Key,
        _sequence: SequenceNumber,
    ) -> Result<Option<Value>, ReadError> {
        Ok(None)
    }

    pub async fn scan_at(
        &self,
        _start: Key,
        _end: Key,
        _sequence: SequenceNumber,
        _opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(Box::pin(stream::empty()))
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
    db: Db,
    sequence: SequenceNumber,
    released: Arc<AtomicBool>,
}

impl Snapshot {
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    pub async fn read(&self, table: &Table, key: Key) -> Result<Option<Value>, ReadError> {
        table.read_at(key, self.sequence).await
    }

    pub async fn scan(
        &self,
        table: &Table,
        start: Key,
        end: Key,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        table.scan_at(start, end, self.sequence, opts).await
    }

    pub async fn scan_prefix(
        &self,
        table: &Table,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        table.scan_prefix(prefix, opts).await
    }

    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }

    pub fn is_released(&self) -> bool {
        self.released.load(Ordering::SeqCst)
    }

    pub fn db(&self) -> &Db {
        &self.db
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("sequence", &self.sequence)
            .field("released", &self.is_released())
            .finish()
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

    use serde_json::json;

    use super::{
        Db, LOCAL_CATALOG_RELATIVE_PATH, LOCAL_CATALOG_TEMP_SUFFIX, SchemaDefinition, StoredTable,
    };
    use crate::{
        CompactionStrategy, DbConfig, DbDependencies, FieldDefinition, FieldId, FieldType,
        FieldValue, FileSystemFailure, FileSystemOperation, S3Location, S3PrimaryStorageConfig,
        SsdConfig, StorageConfig, StubClock, StubObjectStore, StubRng, TableConfig, TableFormat,
        TieredDurabilityMode, TieredStorageConfig,
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
}
