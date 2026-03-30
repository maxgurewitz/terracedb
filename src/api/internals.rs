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

#[derive(Clone)]
enum ChangeFeedSourcePlan {
    LocalSegment(LocalSegmentScanPlan),
    RemoteSegment(DurableRemoteCommitLogSegment),
    BufferedRecords(Arc<Vec<CommitRecord>>),
}

struct ChangeFeedScanner {
    cursor: LogCursor,
    upper_bound: SequenceNumber,
    table_id: TableId,
    table: Table,
    remaining: Option<usize>,
    sources: VecDeque<ChangeFeedSourcePlan>,
    current_source: Option<ActiveChangeFeedSource>,
}

enum ActiveChangeFeedSource {
    Segment(ChangeFeedSegmentSource),
    BufferedRecords(ChangeFeedBufferedRecordsSource),
}

struct ChangeFeedSegmentSource {
    scanner: SegmentRecordScanner,
    pending: VecDeque<ChangeEntry>,
    upper_bound_reached: bool,
}

struct ChangeFeedBufferedRecordsSource {
    records: Arc<Vec<CommitRecord>>,
    next_index: usize,
    pending: VecDeque<ChangeEntry>,
    upper_bound_reached: bool,
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

#[derive(Clone, Debug, Default, PartialEq)]
struct RowScanResult {
    rows: Vec<(Key, Value)>,
    collapses: Vec<(Key, MergeCollapse)>,
    visited_key_groups: usize,
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
    records: Arc<Vec<CommitRecord>>,
    durable_commit_log_segments: Vec<DurableRemoteCommitLogSegment>,
    next_segment_id: u64,
}

impl MemoryCommitLog {
    fn new() -> Self {
        Self {
            records: Arc::new(Vec::new()),
            durable_commit_log_segments: Vec::new(),
            next_segment_id: 1,
        }
    }
}

impl ChangeFeedScanner {
    fn new(
        table_id: TableId,
        table: Table,
        cursor: LogCursor,
        upper_bound: SequenceNumber,
        remaining: Option<usize>,
        sources: VecDeque<ChangeFeedSourcePlan>,
    ) -> Self {
        Self {
            cursor,
            upper_bound,
            table_id,
            table,
            remaining,
            sources,
            current_source: None,
        }
    }

    async fn next_change(&mut self) -> Result<Option<ChangeEntry>, StorageError> {
        if matches!(self.remaining, Some(0)) {
            return Ok(None);
        }

        loop {
            if let Some(source) = self.current_source.as_mut() {
                match source.next_change(
                    self.table_id,
                    &self.table,
                    self.cursor,
                    self.upper_bound,
                )? {
                    Some(entry) => {
                        if let Some(remaining) = &mut self.remaining {
                            *remaining = remaining.saturating_sub(1);
                        }
                        return Ok(Some(entry));
                    }
                    None if source.reached_upper_bound() => {
                        self.current_source = None;
                        self.sources.clear();
                        return Ok(None);
                    }
                    None => {
                        self.current_source = None;
                        continue;
                    }
                }
            }

            let Some(source) = self.sources.pop_front() else {
                return Ok(None);
            };
            self.current_source = self.load_source(source).await?;
        }
    }

    async fn load_source(
        &self,
        source: ChangeFeedSourcePlan,
    ) -> Result<Option<ActiveChangeFeedSource>, StorageError> {
        match source {
            ChangeFeedSourcePlan::LocalSegment(plan) => {
                if plan
                    .max_sequence
                    .is_some_and(|max| max < self.cursor.sequence())
                {
                    return Ok(None);
                }
                if plan.min_sequence.is_some_and(|min| min > self.upper_bound) {
                    return Ok(None);
                }

                let bytes = read_change_feed_file(&plan.fs, &plan.path).await?;
                Ok(Some(ActiveChangeFeedSource::Segment(
                    ChangeFeedSegmentSource {
                        scanner: SegmentRecordScanner::new(
                            plan.segment_id,
                            bytes,
                            self.cursor.sequence(),
                        )?,
                        pending: VecDeque::new(),
                        upper_bound_reached: false,
                    },
                )))
            }
            ChangeFeedSourcePlan::RemoteSegment(segment) => {
                if segment.footer.max_sequence < self.cursor.sequence()
                    || segment.footer.min_sequence > self.upper_bound
                {
                    return Ok(None);
                }

                let bytes = self
                    .table
                    .db
                    .inner
                    .dependencies
                    .object_store
                    .get(&segment.object_key)
                    .await?;
                Ok(Some(ActiveChangeFeedSource::Segment(
                    ChangeFeedSegmentSource {
                        scanner: SegmentRecordScanner::new(
                            segment.footer.segment_id,
                            bytes,
                            self.cursor.sequence(),
                        )?,
                        pending: VecDeque::new(),
                        upper_bound_reached: false,
                    },
                )))
            }
            ChangeFeedSourcePlan::BufferedRecords(records) => {
                Ok(Some(ActiveChangeFeedSource::BufferedRecords(
                    ChangeFeedBufferedRecordsSource::new(records, self.cursor.sequence()),
                )))
            }
        }
    }
}

impl ActiveChangeFeedSource {
    fn next_change(
        &mut self,
        table_id: TableId,
        table: &Table,
        cursor: LogCursor,
        upper_bound: SequenceNumber,
    ) -> Result<Option<ChangeEntry>, StorageError> {
        match self {
            Self::Segment(source) => source.next_change(table_id, table, cursor, upper_bound),
            Self::BufferedRecords(source) => {
                source.next_change(table_id, table, cursor, upper_bound)
            }
        }
    }

    fn reached_upper_bound(&self) -> bool {
        match self {
            Self::Segment(source) => source.upper_bound_reached,
            Self::BufferedRecords(source) => source.upper_bound_reached,
        }
    }
}

impl ChangeFeedSegmentSource {
    fn next_change(
        &mut self,
        table_id: TableId,
        table: &Table,
        cursor: LogCursor,
        upper_bound: SequenceNumber,
    ) -> Result<Option<ChangeEntry>, StorageError> {
        if let Some(entry) = self.pending.pop_front() {
            return Ok(Some(entry));
        }

        while let Some(record) = self.scanner.next_record()? {
            if record.sequence() > upper_bound {
                self.upper_bound_reached = true;
                return Ok(None);
            }

            self.pending.extend(change_entries_for_record(
                record,
                table_id,
                table,
                cursor,
                upper_bound,
            ));
            if let Some(entry) = self.pending.pop_front() {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}

impl ChangeFeedBufferedRecordsSource {
    fn new(records: Arc<Vec<CommitRecord>>, cursor_sequence: SequenceNumber) -> Self {
        let next_index = records
            .iter()
            .position(|record| record.sequence() >= cursor_sequence)
            .unwrap_or(records.len());
        Self {
            records,
            next_index,
            pending: VecDeque::new(),
            upper_bound_reached: false,
        }
    }

    fn next_change(
        &mut self,
        table_id: TableId,
        table: &Table,
        cursor: LogCursor,
        upper_bound: SequenceNumber,
    ) -> Result<Option<ChangeEntry>, StorageError> {
        if let Some(entry) = self.pending.pop_front() {
            return Ok(Some(entry));
        }

        while let Some(record) = self.records.get(self.next_index).cloned() {
            self.next_index = self.next_index.saturating_add(1);
            if record.sequence() > upper_bound {
                self.upper_bound_reached = true;
                return Ok(None);
            }

            self.pending.extend(change_entries_for_record(
                record,
                table_id,
                table,
                cursor,
                upper_bound,
            ));
            if let Some(entry) = self.pending.pop_front() {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}

fn change_entries_for_record(
    record: CommitRecord,
    table_id: TableId,
    table: &Table,
    cursor: LogCursor,
    upper_bound: SequenceNumber,
) -> VecDeque<ChangeEntry> {
    let sequence = record.sequence();
    if sequence > upper_bound {
        return VecDeque::new();
    }

    record
        .entries
        .into_iter()
        .filter(|entry| entry.table_id == table_id)
        .filter_map(|entry| {
            let entry_cursor = LogCursor::new(sequence, entry.op_index);
            (entry_cursor > cursor).then(|| ChangeEntry {
                key: entry.key,
                value: entry.value,
                cursor: entry_cursor,
                sequence,
                kind: entry.kind,
                table: table.clone(),
            })
        })
        .collect()
}

async fn read_change_feed_file(
    fs: &Arc<dyn crate::FileSystem>,
    path: &str,
) -> Result<Vec<u8>, StorageError> {
    let handle = FileHandle::new(path);
    let mut bytes = Vec::new();
    let mut offset = 0_u64;
    loop {
        let chunk = fs
            .read_at(&handle, offset, CHANGE_FEED_READ_CHUNK_BYTES)
            .await?;
        if chunk.is_empty() {
            break;
        }
        offset = offset.saturating_add(chunk.len() as u64);
        bytes.extend_from_slice(&chunk);
        if chunk.len() < CHANGE_FEED_READ_CHUNK_BYTES {
            break;
        }
    }
    Ok(bytes)
}

impl CommitRuntime {
    async fn append(&mut self, record: CommitRecord) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(manager) => {
                manager.append(record).await?;
            }
            CommitLogBackend::Memory(log) => {
                Arc::make_mut(&mut log.records).push(record);
            }
        }

        Ok(())
    }

    async fn append_group_batch(&mut self, records: &[CommitRecord]) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(manager) => manager.append_batch_and_sync(records).await,
            CommitLogBackend::Memory(log) => {
                Arc::make_mut(&mut log.records).extend(records.iter().cloned());
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

    fn change_feed_scan_plan(
        &self,
        table_id: TableId,
        sequence_inclusive: SequenceNumber,
    ) -> VecDeque<ChangeFeedSourcePlan> {
        match &self.backend {
            CommitLogBackend::Local(manager) => manager
                .table_scan_plans_since(table_id, sequence_inclusive)
                .into_iter()
                .map(ChangeFeedSourcePlan::LocalSegment)
                .collect(),
            CommitLogBackend::Memory(log) => {
                let mut sources = log
                    .durable_commit_log_segments
                    .iter()
                    .filter(|segment| {
                        segment
                            .footer
                            .tables
                            .iter()
                            .find(|table| table.table_id == table_id)
                            .is_some_and(|table| table.max_sequence >= sequence_inclusive)
                    })
                    .cloned()
                    .map(ChangeFeedSourcePlan::RemoteSegment)
                    .collect::<VecDeque<_>>();
                if log.records.iter().any(|record| {
                    record.sequence() >= sequence_inclusive
                        && record
                            .entries
                            .iter()
                            .any(|entry| entry.table_id == table_id)
                }) {
                    sources.push_back(ChangeFeedSourcePlan::BufferedRecords(log.records.clone()));
                }
                sources
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
                    for record in log.records.iter() {
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
