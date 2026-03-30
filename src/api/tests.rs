use std::sync::atomic::Ordering;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;
use serde_json::json;

use super::{
    COLUMNAR_SSTABLE_FORMAT_VERSION, COLUMNAR_SSTABLE_MAGIC, ColumnarCacheStatsSnapshot,
    ColumnarReadAccessPattern, CommitPhase, CompactionJobKind, CompactionPhase, Db, KeyMatcher,
    LOCAL_CATALOG_RELATIVE_PATH, LOCAL_CATALOG_TEMP_SUFFIX, LOCAL_COMMIT_LOG_RELATIVE_DIR,
    LOCAL_MANIFEST_TEMP_SUFFIX, LOCAL_SSTABLE_RELATIVE_DIR, LOCAL_SSTABLE_SHARD_DIR, ManifestId,
    OffloadPhase, PendingWorkSpec, PersistedRowSstableFile, ResidentColumnarSstable,
    SchemaDefinition, StorageSource, StoredTable, WatermarkUpdate, decode_mvcc_key,
    encode_mvcc_key, read_path,
};
use crate::simulation::{
    CutPoint, PointMutation, SeededSimulationRunner, ShadowOracle, TraceEvent,
};
use crate::{
    ChangeFeedError, ChangeKind, Clock, CommitError, CommitId, CommitOptions, CompactionStrategy,
    DbConfig, DbDependencies, FieldDefinition, FieldId, FieldType, FieldValue, FileSystem,
    FileSystemFailure, FileSystemOperation, LogCursor, MergeOperator, MergeOperatorRef,
    NoopScheduler, ObjectKeyLayout, ObjectStore, ObjectStoreFailure, ObjectStoreOperation,
    PendingWork, PendingWorkType, ReadError, Rng, S3Location, S3PrimaryStorageConfig, ScanOptions,
    ScheduleAction, ScheduleDecision, Scheduler, SegmentId, SequenceNumber, SsdConfig,
    StorageConfig, StorageError, StorageErrorKind, StubClock, StubObjectStore, StubRng, Table,
    TableConfig, TableFormat, TableId, TableStats, ThrottleDecision, TieredDurabilityMode,
    TieredStorageConfig, Timestamp, Transaction, TtlCompactionFilter, Value,
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

#[tokio::test]
async fn builder_component_overrides_flow_through_runtime() {
    let file_system: Arc<dyn FileSystem> = Arc::new(crate::StubFileSystem::default());
    let object_store: Arc<dyn ObjectStore> = Arc::new(StubObjectStore::default());
    let clock: Arc<dyn Clock> = Arc::new(StubClock::default());
    let rng: Arc<dyn Rng> = Arc::new(StubRng::seeded(17));
    let scheduler: Arc<dyn Scheduler> = Arc::new(NoopScheduler);

    let db = Db::builder()
        .config(tiered_config("/builder-component-overrides"))
        .file_system(file_system.clone())
        .object_store(object_store.clone())
        .clock(clock.clone())
        .rng(rng.clone())
        .scheduler(scheduler.clone())
        .open()
        .await
        .expect("open db through builder");

    assert!(Arc::ptr_eq(&db.dependencies().file_system, &file_system));
    assert!(Arc::ptr_eq(&db.dependencies().object_store, &object_store));
    assert!(Arc::ptr_eq(&db.dependencies().clock, &clock));
    assert!(Arc::ptr_eq(&db.dependencies().rng, &rng));
    assert!(Arc::ptr_eq(&db.inner.scheduler, &scheduler));
}

#[tokio::test]
async fn builder_into_open_parts_matches_low_level_inputs() {
    let config = tiered_config("/builder-open-parts");
    let file_system: Arc<dyn FileSystem> = Arc::new(crate::StubFileSystem::default());
    let object_store: Arc<dyn ObjectStore> = Arc::new(StubObjectStore::default());
    let clock: Arc<dyn Clock> = Arc::new(StubClock::default());
    let rng: Arc<dyn Rng> = Arc::new(StubRng::seeded(29));
    let dependencies = DbDependencies::new(
        file_system.clone(),
        object_store.clone(),
        clock.clone(),
        rng.clone(),
    );

    let builder = Db::builder()
        .config(config.clone())
        .dependencies(dependencies.clone());
    let (built_config, built_dependencies) = builder
        .clone()
        .into_open_parts()
        .expect("builder should resolve open parts");

    assert_eq!(built_config.storage, config.storage);
    assert!(built_config.scheduler.is_none());
    assert!(Arc::ptr_eq(
        &built_dependencies.file_system,
        &dependencies.file_system
    ));
    assert!(Arc::ptr_eq(
        &built_dependencies.object_store,
        &dependencies.object_store
    ));
    assert!(Arc::ptr_eq(&built_dependencies.clock, &dependencies.clock));
    assert!(Arc::ptr_eq(&built_dependencies.rng, &dependencies.rng));

    let built_db = builder.open().await.expect("open db through builder");
    let low_level_db = Db::open(config, dependencies)
        .await
        .expect("open db through low-level api");

    assert_eq!(
        built_db.telemetry_db_name(),
        low_level_db.telemetry_db_name()
    );
    assert_eq!(
        built_db.telemetry_db_instance(),
        low_level_db.telemetry_db_instance()
    );
    assert_eq!(
        built_db.telemetry_storage_mode(),
        low_level_db.telemetry_storage_mode()
    );
    assert_eq!(built_db.current_sequence(), low_level_db.current_sequence());
    assert_eq!(
        built_db.current_durable_sequence(),
        low_level_db.current_durable_sequence()
    );
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

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
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

async fn install_columnar_schema_successor(db: &Db, table_name: &str, successor: SchemaDefinition) {
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

async fn move_live_columnar_sstable_to_remote_for_test(db: &Db) -> String {
    let live = db
        .sstables_read()
        .live
        .first()
        .cloned()
        .expect("live columnar sstable");
    let bytes = read_path(db.dependencies(), &live.meta.file_path)
        .await
        .expect("read local columnar sstable");
    let remote_key = tiered_layout().cold_sstable(
        live.meta.table_id,
        0,
        live.meta.min_sequence,
        live.meta.max_sequence,
        &live.meta.local_id,
    );
    db.dependencies()
        .object_store
        .put(&remote_key, &bytes)
        .await
        .expect("upload columnar sstable to remote object");

    let mut updated = live.clone();
    updated.meta.file_path.clear();
    updated.meta.remote_key = Some(remote_key.clone());
    updated.rows.clear();
    updated.columnar = Some(ResidentColumnarSstable {
        source: StorageSource::remote_object(remote_key.clone()),
    });

    let state = db.sstables_read().clone();
    let next_generation = ManifestId::new(state.manifest_generation.get().saturating_add(1));
    db.install_manifest(
        next_generation,
        state.last_flushed_sequence,
        &[updated.clone()],
    )
    .await
    .expect("install remote-only manifest for columnar test");

    let mut sstables = db.sstables_write();
    sstables.manifest_generation = next_generation;
    sstables.last_flushed_sequence = state.last_flushed_sequence;
    sstables.live = vec![updated];

    remote_key
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

#[derive(Default)]
struct TableRateLimitScheduler {
    rates_by_table: BTreeMap<String, u64>,
}

impl Scheduler for TableRateLimitScheduler {
    fn on_work_available(&self, work: &[PendingWork]) -> Vec<ScheduleDecision> {
        work.iter()
            .map(|work| ScheduleDecision {
                work_id: work.id.clone(),
                action: ScheduleAction::Defer,
            })
            .collect()
    }

    fn should_throttle(&self, table: &crate::Table, _stats: &TableStats) -> ThrottleDecision {
        self.rates_by_table
            .get(table.name())
            .copied()
            .map(|max_write_bytes_per_second| ThrottleDecision {
                throttle: true,
                max_write_bytes_per_second: Some(max_write_bytes_per_second),
                stall: false,
            })
            .unwrap_or_default()
    }
}

async fn advance_clock_until_task_finishes<T>(
    clock: &StubClock,
    handle: &tokio::task::JoinHandle<T>,
    step: Duration,
    max_steps: usize,
) -> u64 {
    let start = clock.now().get();
    for _ in 0..max_steps {
        if handle.is_finished() {
            break;
        }
        clock.advance(step);
        tokio::task::yield_now().await;
    }
    assert!(
        handle.is_finished(),
        "task should finish after advancing the simulated clock"
    );
    clock.now().get().saturating_sub(start)
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

fn merge_row_table_config(name: &str, max_merge_operand_chain_length: Option<u32>) -> TableConfig {
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
        .map(|entry| {
            entry.map(|entry| CollectedChange {
                sequence: entry.sequence,
                cursor: entry.cursor,
                kind: entry.kind,
                key: entry.key,
                value: entry.value,
                table: entry.table.name().to_string(),
            })
        })
        .try_collect::<Vec<_>>()
        .await
        .expect("collect change stream")
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
    error: ChangeFeedError,
    requested: SequenceNumber,
    oldest_available: SequenceNumber,
) {
    let snapshot_too_old = error
        .snapshot_too_old()
        .expect("expected SnapshotTooOld change-feed error");
    assert_eq!(snapshot_too_old.requested, requested);
    assert_eq!(snapshot_too_old.oldest_available, oldest_available);
}

fn assert_change_feed_storage_error(
    error: ChangeFeedError,
    kind: crate::StorageErrorKind,
    message_fragment: &str,
) {
    let storage = error
        .storage()
        .expect("expected storage-backed change-feed error");
    assert_eq!(storage.kind(), kind);
    assert!(
        storage.message().contains(message_fragment),
        "expected {:?} to contain {message_fragment:?}",
        storage.message(),
    );
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
    assert!(
        compaction_work.iter().any(|work| {
            work.work_type == PendingWorkType::Compaction && work.table == "events"
        })
    );
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
            .any(|(table_name, metadata)| table_name == "events" && metadata == &expected_metadata)
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
async fn round_robin_scheduler_services_three_backlogged_tables_within_three_passes() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config_with_scheduler(
            "/scheduler-round-robin-fairness",
            Arc::new(crate::RoundRobinScheduler::default()),
        ),
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
    let gamma = db
        .create_table(row_table_config("gamma"))
        .await
        .expect("create gamma");

    for round in 0..2_u8 {
        alpha
            .write(vec![b'a', round], Value::Bytes(vec![round]))
            .await
            .expect("write alpha");
        beta.write(vec![b'b', round], Value::Bytes(vec![round]))
            .await
            .expect("write beta");
        gamma
            .write(vec![b'g', round], Value::Bytes(vec![round]))
            .await
            .expect("write gamma");
        db.flush().await.expect("flush round");
    }

    assert!(db.table_stats(&alpha).await.compaction_debt > 0);
    assert!(db.table_stats(&beta).await.compaction_debt > 0);
    assert!(db.table_stats(&gamma).await.compaction_debt > 0);

    assert!(
        db.run_next_scheduled_work()
            .await
            .expect("run first scheduled work")
    );
    assert_eq!(db.table_stats(&alpha).await.compaction_debt, 0);
    assert!(db.table_stats(&beta).await.compaction_debt > 0);
    assert!(db.table_stats(&gamma).await.compaction_debt > 0);
    let mut remaining = db
        .pending_work()
        .await
        .into_iter()
        .map(|work| work.table)
        .collect::<Vec<_>>();
    remaining.sort();
    assert_eq!(remaining, vec!["beta".to_string(), "gamma".to_string()]);

    assert!(
        db.run_next_scheduled_work()
            .await
            .expect("run second scheduled work")
    );
    assert_eq!(db.table_stats(&beta).await.compaction_debt, 0);
    assert!(db.table_stats(&gamma).await.compaction_debt > 0);
    let remaining = db
        .pending_work()
        .await
        .into_iter()
        .map(|work| work.table)
        .collect::<Vec<_>>();
    assert_eq!(remaining, vec!["gamma".to_string()]);

    assert!(
        db.run_next_scheduled_work()
            .await
            .expect("run third scheduled work")
    );
    assert_eq!(db.table_stats(&gamma).await.compaction_debt, 0);
    assert!(db.pending_work().await.is_empty());
}

#[tokio::test]
async fn multi_table_commit_waits_for_the_slowest_rate_limited_table() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let clock = Arc::new(StubClock::default());
    let scheduler = Arc::new(TableRateLimitScheduler {
        rates_by_table: BTreeMap::from([
            ("fast".to_string(), 128_u64),
            ("slow".to_string(), 16_u64),
        ]),
    });
    let db = Db::open(
        tiered_config_with_scheduler("/scheduler-slowest-table-rate-limit", scheduler),
        dependencies_with_clock(file_system, object_store, clock.clone()),
    )
    .await
    .expect("open db");
    let fast = db
        .create_table(row_table_config("fast"))
        .await
        .expect("create fast");
    let slow = db
        .create_table(row_table_config("slow"))
        .await
        .expect("create slow");

    let fast_only_db = db.clone();
    let fast_only_table = fast.clone();
    let fast_only = tokio::spawn(async move {
        let mut tx = Transaction::begin(&fast_only_db).await;
        tx.write(
            &fast_only_table,
            b"fast-only".to_vec(),
            Value::Bytes(vec![b'f'; 64]),
        );
        tx.commit_no_flush().await.expect("commit fast-only tx")
    });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        !fast_only.is_finished(),
        "fast-only commit should wait for the simulated clock"
    );
    let fast_elapsed = advance_clock_until_task_finishes(
        clock.as_ref(),
        &fast_only,
        Duration::from_millis(250),
        40,
    )
    .await;
    assert_eq!(
        fast_only.await.expect("join fast-only commit"),
        SequenceNumber::new(1)
    );

    let mixed_db = db.clone();
    let mixed_fast = fast.clone();
    let mixed_slow = slow.clone();
    let mixed = tokio::spawn(async move {
        let mut tx = Transaction::begin(&mixed_db).await;
        tx.write(
            &mixed_fast,
            b"fast-mixed".to_vec(),
            Value::Bytes(vec![b'f'; 64]),
        );
        tx.write(
            &mixed_slow,
            b"slow-mixed".to_vec(),
            Value::Bytes(vec![b's'; 64]),
        );
        tx.commit_no_flush().await.expect("commit mixed tx")
    });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        !mixed.is_finished(),
        "mixed commit should wait for the simulated clock"
    );
    let mixed_elapsed =
        advance_clock_until_task_finishes(clock.as_ref(), &mixed, Duration::from_millis(250), 80)
            .await;
    assert_eq!(
        mixed.await.expect("join mixed commit"),
        SequenceNumber::new(2)
    );

    assert!(
        fast_elapsed >= 500,
        "fast-only commit should incur a modeled delay"
    );
    assert!(
        mixed_elapsed > fast_elapsed,
        "mixed commit should be gated by the slower table budget"
    );
    assert!(
        mixed_elapsed >= fast_elapsed + 2_000,
        "slow-table budget should materially dominate the mixed commit: fast={fast_elapsed}ms mixed={mixed_elapsed}ms"
    );
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
            .all(|sstable| sstable.meta.file_path.is_empty() && sstable.meta.remote_key.is_some())
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
async fn try_table_returns_none_for_missing_tables() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config("/missing-table-lookup"),
        dependencies(file_system, object_store),
    )
    .await
    .expect("open db");

    assert!(db.try_table("events").is_none());
}

#[test]
#[should_panic(expected = "table does not exist: events")]
fn table_lookup_panics_for_missing_tables() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = futures::executor::block_on(Db::open(
        tiered_config("/missing-table-panic"),
        dependencies(file_system, object_store),
    ))
    .expect("open db");

    let _ = db.table("events");
}

#[tokio::test]
async fn missing_table_handles_return_not_found_errors() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config("/missing-table-handle"),
        dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let missing = Table {
        db: db.clone(),
        name: Arc::from("events"),
        id: Some(TableId::new(1)),
    };

    let read_error = match missing.read(b"user:1".to_vec()).await {
        Ok(_) => panic!("missing table read should fail loudly"),
        Err(error) => error,
    };
    match read_error {
        ReadError::Storage(error) => {
            assert_eq!(error.kind(), crate::StorageErrorKind::NotFound);
            assert_eq!(error.message(), "table does not exist: events");
        }
        other => panic!("expected storage error, got {other:?}"),
    }

    let scan_error = match missing
        .scan(Vec::new(), vec![0xff], ScanOptions::default())
        .await
    {
        Ok(_) => panic!("missing table scan should fail loudly"),
        Err(error) => error,
    };
    match scan_error {
        ReadError::Storage(error) => {
            assert_eq!(error.kind(), crate::StorageErrorKind::NotFound);
            assert_eq!(error.message(), "table does not exist: events");
        }
        other => panic!("expected storage error, got {other:?}"),
    }

    let change_feed_error = match db
        .scan_since(&missing, LogCursor::default(), ScanOptions::default())
        .await
    {
        Ok(_) => panic!("missing table change feed should fail loudly"),
        Err(error) => error,
    };
    let storage = change_feed_error
        .storage()
        .expect("missing table change feed should surface a storage error");
    assert_eq!(storage.kind(), crate::StorageErrorKind::NotFound);
    assert_eq!(storage.message(), "table does not exist: events");
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
    assert!(reopened.try_table("events").is_none());

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
async fn row_scan_limit_short_circuits_after_requested_key_groups() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config("/row-scan-limit-short-circuit"),
        dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");

    table
        .write(b"apple".to_vec(), Value::bytes("red"))
        .await
        .expect("write apple");
    db.flush().await.expect("flush apple");
    table
        .write(b"banana".to_vec(), Value::bytes("yellow"))
        .await
        .expect("write banana");
    let rotated = db.memtables_write().rotate_mutable();
    assert!(
        rotated.is_some(),
        "banana should rotate into an immutable memtable"
    );
    table
        .write(b"carrot".to_vec(), Value::bytes("orange"))
        .await
        .expect("write carrot");
    table
        .write(b"date".to_vec(), Value::bytes("brown"))
        .await
        .expect("write date");

    let table_id = table.id().expect("table id");
    let scan = {
        let tables = db.tables_read();
        let memtables = db.memtables_read();
        let sstables = db.sstables_read();
        Db::scan_visible_row_with_state(
            &tables,
            &memtables,
            &sstables,
            table_id,
            db.current_sequence(),
            KeyMatcher::Range {
                start: b"a",
                end: b"z",
            },
            &ScanOptions {
                reverse: false,
                limit: Some(2),
                columns: None,
            },
        )
        .expect("scan visible rows with state")
    };

    assert_eq!(
        scan.rows,
        vec![
            (b"apple".to_vec(), Value::bytes("red")),
            (b"banana".to_vec(), Value::bytes("yellow")),
        ]
    );
    assert_eq!(scan.visited_key_groups, 2);
}

#[tokio::test]
async fn reverse_row_scan_limit_short_circuits_after_requested_key_groups() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config("/reverse-row-scan-limit-short-circuit"),
        dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");

    table
        .write(b"apple".to_vec(), Value::bytes("red"))
        .await
        .expect("write apple");
    db.flush().await.expect("flush apple");
    table
        .write(b"banana".to_vec(), Value::bytes("yellow"))
        .await
        .expect("write banana");
    let rotated = db.memtables_write().rotate_mutable();
    assert!(
        rotated.is_some(),
        "banana should rotate into an immutable memtable"
    );
    table
        .write(b"carrot".to_vec(), Value::bytes("orange"))
        .await
        .expect("write carrot");
    table
        .write(b"date".to_vec(), Value::bytes("brown"))
        .await
        .expect("write date");

    let table_id = table.id().expect("table id");
    let scan = {
        let tables = db.tables_read();
        let memtables = db.memtables_read();
        let sstables = db.sstables_read();
        Db::scan_visible_row_with_state(
            &tables,
            &memtables,
            &sstables,
            table_id,
            db.current_sequence(),
            KeyMatcher::Range {
                start: b"a",
                end: b"z",
            },
            &ScanOptions {
                reverse: true,
                limit: Some(2),
                columns: None,
            },
        )
        .expect("reverse scan visible rows with state")
    };

    assert_eq!(
        scan.rows,
        vec![
            (b"date".to_vec(), Value::bytes("brown")),
            (b"carrot".to_vec(), Value::bytes("orange")),
        ]
    );
    assert_eq!(scan.visited_key_groups, 2);
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
    let (footer, footer_start) = Db::columnar_footer_from_bytes(&live.meta.file_path, &file_bytes)
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
async fn local_columnar_point_reads_reuse_decoded_cache_without_extra_file_reads() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let dependencies = dependencies(file_system.clone(), object_store);

    let db = Db::open(
        tiered_config("/columnar-local-read-cache"),
        dependencies.clone(),
    )
    .await
    .expect("open db");
    let config = columnar_table_config("metrics");
    let schema = config.schema.clone().expect("columnar schema");
    let table = db.create_table(config).await.expect("create table");
    let expected = Value::named_record(
        &schema,
        vec![
            ("user_id", FieldValue::String("alice".to_string())),
            ("count", FieldValue::Int64(7)),
        ],
    )
    .expect("encode record");
    table
        .write(b"user:1".to_vec(), expected.clone())
        .await
        .expect("write row");
    db.flush().await.expect("flush local columnar row");

    let reopened = Db::open(tiered_config("/columnar-local-read-cache"), dependencies)
        .await
        .expect("reopen db");
    let reopened_table = reopened.table("metrics");
    reopened.clear_columnar_decoded_cache();
    reopened.reset_columnar_cache_stats();

    let reads_before = file_system.operation_count(FileSystemOperation::ReadAt);
    assert_eq!(
        reopened_table
            .read(b"user:1".to_vec())
            .await
            .expect("first lazy point read"),
        Some(expected.clone())
    );
    let reads_after_first = file_system.operation_count(FileSystemOperation::ReadAt);
    assert!(
        reads_after_first > reads_before,
        "first lazy point read should touch local columnar blocks",
    );
    let first_stats = reopened.columnar_cache_stats_snapshot();
    assert!(first_stats.decoded_footer_misses > 0);
    assert!(first_stats.decoded_metadata_misses > 0);
    assert!(first_stats.decoded_column_block_misses > 0);

    assert_eq!(
        reopened_table
            .read(b"user:1".to_vec())
            .await
            .expect("second lazy point read"),
        Some(expected)
    );
    let reads_after_second = file_system.operation_count(FileSystemOperation::ReadAt);
    assert_eq!(
        reads_after_second, reads_after_first,
        "second point read should reuse decoded cache instead of re-reading local blocks",
    );
    let second_stats = reopened.columnar_cache_stats_snapshot();
    assert!(second_stats.decoded_footer_hits > first_stats.decoded_footer_hits);
    assert!(second_stats.decoded_metadata_hits > first_stats.decoded_metadata_hits);
    assert!(second_stats.decoded_column_block_hits > first_stats.decoded_column_block_hits);
}

#[tokio::test]
async fn remote_columnar_point_reads_reuse_raw_byte_cache_in_s3_primary_mode() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let inner_store = Arc::new(StubObjectStore::default());
    let recording_store = Arc::new(RecordingObjectStore::new(inner_store));
    let dependencies = DbDependencies::new(
        file_system,
        recording_store.clone(),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(7)),
    );
    let config = s3_primary_config("columnar-raw-cache-s3");

    let db = Db::open(config.clone(), dependencies.clone())
        .await
        .expect("open db");
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
            ("count", FieldValue::Int64(9)),
        ],
    )
    .expect("encode remote record");
    table
        .write(b"user:1".to_vec(), expected.clone())
        .await
        .expect("write remote row");
    db.flush().await.expect("flush remote row");

    let reopened = Db::open(config, dependencies).await.expect("reopen db");
    let reopened_table = reopened.table("metrics");
    reopened.clear_columnar_decoded_cache();
    recording_store.clear_calls();

    assert_eq!(
        reopened_table
            .read(b"user:1".to_vec())
            .await
            .expect("first remote point read"),
        Some(expected.clone())
    );
    let first_range_calls = recording_store.range_calls();
    assert!(
        !first_range_calls.is_empty(),
        "first remote point read should fetch ranges from object storage",
    );

    reopened.clear_columnar_decoded_cache();
    reopened.reset_columnar_cache_stats();
    recording_store.clear_calls();
    assert_eq!(
        reopened_table
            .read(b"user:1".to_vec())
            .await
            .expect("second remote point read"),
        Some(expected)
    );
    assert!(
        recording_store.range_calls().is_empty(),
        "second remote point read should come entirely from the raw-byte cache after decoded cache reset",
    );
    assert!(reopened.columnar_cache_stats_snapshot().raw_byte_hits > 0);
}

#[tokio::test]
async fn remote_columnar_point_reads_reuse_raw_byte_cache_in_tiered_cold_mode() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let inner_store = Arc::new(StubObjectStore::default());
    let recording_store = Arc::new(RecordingObjectStore::new(inner_store));
    let dependencies = DbDependencies::new(
        file_system,
        recording_store.clone(),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(7)),
    );

    let db = Db::open(
        tiered_config("/columnar-raw-cache-tiered"),
        dependencies.clone(),
    )
    .await
    .expect("open tiered db");
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
            ("count", FieldValue::Int64(11)),
        ],
    )
    .expect("encode tiered record");
    table
        .write(b"user:1".to_vec(), expected.clone())
        .await
        .expect("write tiered row");
    db.flush().await.expect("flush tiered row");
    let remote_key = move_live_columnar_sstable_to_remote_for_test(&db).await;

    let reopened = Db::open(tiered_config("/columnar-raw-cache-tiered"), dependencies)
        .await
        .expect("reopen tiered db");
    let reopened_table = reopened.table("metrics");
    reopened.clear_columnar_decoded_cache();
    recording_store.clear_calls();

    assert_eq!(
        reopened_table
            .read(b"user:1".to_vec())
            .await
            .expect("first cold point read"),
        Some(expected.clone())
    );
    let first_remote_calls = recording_store
        .range_calls()
        .into_iter()
        .filter(|(key, _, _)| key == &remote_key)
        .collect::<Vec<_>>();
    assert!(
        !first_remote_calls.is_empty(),
        "first cold point read should fetch remote ranges",
    );

    reopened.clear_columnar_decoded_cache();
    reopened.reset_columnar_cache_stats();
    recording_store.clear_calls();
    assert_eq!(
        reopened_table
            .read(b"user:1".to_vec())
            .await
            .expect("second cold point read"),
        Some(expected)
    );
    let second_remote_calls = recording_store
        .range_calls()
        .into_iter()
        .filter(|(key, _, _)| key == &remote_key)
        .collect::<Vec<_>>();
    assert!(
        second_remote_calls.is_empty(),
        "second cold point read should hit the raw-byte cache instead of refetching ranges",
    );
    assert!(reopened.columnar_cache_stats_snapshot().raw_byte_hits > 0);
}

#[tokio::test]
async fn columnar_scans_avoid_column_block_cache_pollution_while_point_reads_can_warm_it() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let inner_store = Arc::new(StubObjectStore::default());
    let recording_store = Arc::new(RecordingObjectStore::new(inner_store.clone()));
    let dependencies = DbDependencies::new(
        file_system,
        recording_store.clone(),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(7)),
    );
    let config = s3_primary_config("columnar-scan-cache-admission");

    let db = Db::open(config.clone(), dependencies.clone())
        .await
        .expect("open db");
    let table_config = columnar_all_types_table_config("metrics");
    let schema = table_config.schema.clone().expect("columnar schema");
    let table = db
        .create_table(table_config)
        .await
        .expect("create columnar table");
    let full_record = Value::named_record(
        &schema,
        vec![
            ("metric", FieldValue::String("cpu".to_string())),
            ("count", FieldValue::Int64(3)),
            ("ratio", FieldValue::Float64(1.5)),
            ("payload", FieldValue::Bytes(vec![1, 2, 3])),
            ("active", FieldValue::Bool(true)),
        ],
    )
    .expect("encode full record");
    table
        .write(b"row:1".to_vec(), full_record.clone())
        .await
        .expect("write row");
    db.flush().await.expect("flush row");

    let live = db
        .sstables_read()
        .live
        .first()
        .cloned()
        .expect("live remote sstable");
    let remote_key = live.meta.remote_key.clone().expect("remote key");
    let file_bytes = inner_store
        .get(&remote_key)
        .await
        .expect("read remote bytes");
    let (footer, _) =
        Db::columnar_footer_from_bytes(&remote_key, &file_bytes).expect("decode footer");
    let metric_block = footer
        .columns
        .iter()
        .find(|column| column.field_id == FieldId::new(1))
        .expect("metric block");

    let reopened = Db::open(config, dependencies).await.expect("reopen db");
    let reopened_table = reopened.table("metrics");
    reopened.clear_columnar_decoded_cache();
    reopened.reset_columnar_cache_stats();
    recording_store.clear_calls();

    let expected_projection = vec![(
        b"row:1".to_vec(),
        Value::record(BTreeMap::from([(
            FieldId::new(1),
            FieldValue::String("cpu".to_string()),
        )])),
    )];
    assert_eq!(
        collect_rows(
            reopened_table
                .scan(
                    Vec::new(),
                    vec![0xff],
                    ScanOptions {
                        columns: Some(vec!["metric".to_string()]),
                        ..ScanOptions::default()
                    },
                )
                .await
                .expect("first projected scan"),
        )
        .await,
        expected_projection
    );
    let scan_stats = reopened.columnar_cache_stats_snapshot();
    assert_eq!(
        scan_stats.decoded_column_block_admissions, 0,
        "scan-only column blocks should not enter the decoded hot cache",
    );
    assert!(scan_stats.decoded_metadata_admissions > 0);

    reopened.clear_columnar_decoded_cache();
    recording_store.clear_calls();
    assert_eq!(
        collect_rows(
            reopened_table
                .scan(
                    Vec::new(),
                    vec![0xff],
                    ScanOptions {
                        columns: Some(vec!["metric".to_string()]),
                        ..ScanOptions::default()
                    },
                )
                .await
                .expect("second projected scan"),
        )
        .await,
        expected_projection
    );
    let second_scan_calls = recording_store
        .range_calls()
        .into_iter()
        .filter(|(key, _, _)| key == &remote_key)
        .collect::<Vec<_>>();
    assert_eq!(
        second_scan_calls,
        vec![(
            remote_key.clone(),
            metric_block.block.offset,
            metric_block.block.offset + metric_block.block.length,
        )],
        "scan should reuse cached metadata but refetch the projected column block to avoid cache pollution",
    );

    reopened.clear_columnar_decoded_cache();
    reopened.reset_columnar_cache_stats();
    recording_store.clear_calls();
    assert_eq!(
        reopened_table
            .read(b"row:1".to_vec())
            .await
            .expect("first point read"),
        Some(full_record.clone())
    );
    assert!(
        reopened
            .columnar_cache_stats_snapshot()
            .decoded_column_block_admissions
            > 0,
        "point reads should admit hot column blocks into the decoded cache",
    );

    reopened.clear_columnar_decoded_cache();
    recording_store.clear_calls();
    assert_eq!(
        reopened_table
            .read(b"row:1".to_vec())
            .await
            .expect("second point read"),
        Some(full_record)
    );
    assert!(
        recording_store.range_calls().is_empty(),
        "point-read-warmed column blocks should be reusable from the raw-byte cache after decoded cache reset",
    );
}

#[tokio::test]
async fn columnar_cache_disable_preserves_read_results_and_default_filling() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let dependencies = dependencies(file_system, object_store);

    let db = Db::open(
        tiered_config("/columnar-cache-equivalence"),
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
        .expect("create legacy table");
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
    let successor_record = Value::named_record(
        &successor_schema,
        vec![
            ("user_id", FieldValue::String("bob".to_string())),
            ("count", FieldValue::Int64(5)),
        ],
    )
    .expect("encode successor row");
    table
        .write(b"user:2".to_vec(), successor_record.clone())
        .await
        .expect("write successor row");
    db.flush().await.expect("flush successor row");

    let reopened = Db::open(tiered_config("/columnar-cache-equivalence"), dependencies)
        .await
        .expect("reopen db");
    let reopened_table = reopened.table("metrics");
    let expected_legacy = Value::named_record(
        &successor_schema,
        vec![("user_id", FieldValue::String("alice".to_string()))],
    )
    .expect("encode default-filled legacy row");

    let cached_point = reopened_table
        .read(b"user:1".to_vec())
        .await
        .expect("cached point read");
    let cached_historical = reopened_table
        .read_at(b"user:1".to_vec(), first)
        .await
        .expect("cached historical read");
    let cached_scan = collect_rows(
        reopened_table
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("cached scan"),
    )
    .await;

    reopened.clear_columnar_decoded_cache();
    reopened.reset_columnar_cache_stats();
    reopened.set_columnar_cache_enabled(false, false);

    let uncached_point = reopened_table
        .read(b"user:1".to_vec())
        .await
        .expect("uncached point read");
    let uncached_historical = reopened_table
        .read_at(b"user:1".to_vec(), first)
        .await
        .expect("uncached historical read");
    let uncached_scan = collect_rows(
        reopened_table
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("uncached scan"),
    )
    .await;

    assert_eq!(cached_point, Some(expected_legacy.clone()));
    assert_eq!(cached_point, uncached_point);
    assert_eq!(cached_historical, Some(expected_legacy.clone()));
    assert_eq!(cached_historical, uncached_historical);
    assert_eq!(
        cached_scan,
        vec![
            (b"user:1".to_vec(), expected_legacy),
            (b"user:2".to_vec(), successor_record),
        ]
    );
    assert_eq!(cached_scan, uncached_scan);
    assert_eq!(
        reopened.columnar_cache_stats_snapshot(),
        ColumnarCacheStatsSnapshot::default(),
        "cache-disabled reads should preserve results without recording cache hits or admissions",
    );
}

#[tokio::test]
async fn columnar_decoded_cache_drops_on_restart_while_raw_byte_cache_rebuilds() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let inner_store = Arc::new(StubObjectStore::default());
    let recording_store = Arc::new(RecordingObjectStore::new(inner_store));
    let dependencies = DbDependencies::new(
        file_system,
        recording_store.clone(),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(7)),
    );
    let config = s3_primary_config("columnar-cache-restart");

    let db = Db::open(config.clone(), dependencies.clone())
        .await
        .expect("open db");
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
            ("count", FieldValue::Int64(13)),
        ],
    )
    .expect("encode row");
    table
        .write(b"user:1".to_vec(), expected.clone())
        .await
        .expect("write row");
    db.flush().await.expect("flush row");

    let warmed = Db::open(config.clone(), dependencies.clone())
        .await
        .expect("reopen warmed db");
    let warmed_table = warmed.table("metrics");
    warmed.clear_columnar_decoded_cache();
    warmed.reset_columnar_cache_stats();
    recording_store.clear_calls();
    assert_eq!(
        warmed_table
            .read(b"user:1".to_vec())
            .await
            .expect("warm raw cache"),
        Some(expected.clone())
    );
    let warmed_stats = warmed.columnar_cache_stats_snapshot();
    assert!(warmed_stats.raw_byte_misses > 0);
    assert!(warmed_stats.decoded_column_block_misses > 0);

    let reopened = Db::open(config, dependencies)
        .await
        .expect("reopen db again");
    let reopened_table = reopened.table("metrics");
    reopened.clear_columnar_decoded_cache();
    reopened.reset_columnar_cache_stats();
    recording_store.clear_calls();
    assert_eq!(
        reopened_table
            .read(b"user:1".to_vec())
            .await
            .expect("post-restart read"),
        Some(expected)
    );
    assert!(
        recording_store.range_calls().is_empty(),
        "reopened DB should rebuild raw-byte cache metadata and satisfy reads without new remote range calls",
    );
    let restarted_stats = reopened.columnar_cache_stats_snapshot();
    assert!(restarted_stats.raw_byte_hits > 0);
    assert!(restarted_stats.decoded_column_block_misses > 0);
    assert_eq!(restarted_stats.decoded_column_block_hits, 0);
}

#[tokio::test]
async fn row_sstable_reads_do_not_touch_columnar_lazy_cache_stats() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let dependencies = dependencies(file_system, object_store);

    let db = Db::open(tiered_config("/row-cache-scope"), dependencies.clone())
        .await
        .expect("open db");
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create row table");
    table
        .write(b"event:1".to_vec(), bytes("payload"))
        .await
        .expect("write row");
    db.flush().await.expect("flush row table");

    let reopened = Db::open(tiered_config("/row-cache-scope"), dependencies)
        .await
        .expect("reopen db");
    let reopened_table = reopened.table("events");
    reopened.reset_columnar_cache_stats();
    assert_eq!(
        reopened_table
            .read(b"event:1".to_vec())
            .await
            .expect("row table read"),
        Some(bytes("payload"))
    );
    assert_eq!(
        reopened.columnar_cache_stats_snapshot(),
        ColumnarCacheStatsSnapshot::default(),
        "row SSTables are out of scope for the lazy columnar cache because they stay resident after open",
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
        .load_columnar_metadata(db.columnar_read_context(), ColumnarReadAccessPattern::Scan)
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
async fn backup_manifest_failpoint_can_coordinate_latest_pointer_timeout() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config("/backup-manifest-failpoint-latest-pointer"),
        dependencies(file_system, object_store.clone()),
    )
    .await
    .expect("open db");
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");
    let layout = tiered_layout();

    table
        .write(b"apple".to_vec(), Value::bytes("v1"))
        .await
        .expect("write first value");
    db.flush().await.expect("flush first generation");

    table
        .write(b"apple".to_vec(), Value::bytes("v2"))
        .await
        .expect("write second value");

    let blocker = db.__failpoint_registry().arm_pause(
        crate::failpoints::names::DB_BACKUP_MANIFEST_BEFORE_LATEST_POINTER,
        crate::failpoints::FailpointMode::Once,
    );
    let flush_db = db.clone();
    let flush_task = tokio::spawn(async move { flush_db.flush().await });

    blocker.wait_until_hit().await;
    object_store.inject_failure(ObjectStoreFailure::timeout(
        ObjectStoreOperation::Put,
        layout.backup_manifest_latest(),
    ));
    blocker.release();

    flush_task
        .await
        .expect("join flush task")
        .expect("tiered flush should ignore backup pointer failure");

    let latest_pointer = String::from_utf8(
        object_store
            .get(&layout.backup_manifest_latest())
            .await
            .expect("read latest backup manifest pointer"),
    )
    .expect("decode latest pointer");
    assert_eq!(
        latest_pointer.trim(),
        layout.backup_manifest(ManifestId::new(1)),
        "the injected pointer timeout should keep the previous backup manifest published"
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
        seed_compaction_fixture("/tiered-compaction", "events", CompactionStrategy::Tiered).await;

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
    let dependencies = dependencies_with_clock(file_system.clone(), object_store, clock.clone());
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

    let committed = reopened_events
        .write(b"user:retry".to_vec(), bytes("retry"))
        .await
        .expect("write after failed group commit");
    assert_eq!(committed, SequenceNumber::new(1));
    assert_eq!(
        visible_changes(&reopened, &reopened_events).await,
        vec![collected_change(
            1,
            0,
            ChangeKind::Put,
            b"user:retry",
            Some(bytes("retry")),
            "events",
        )]
    );
    assert_eq!(
        durable_changes(&reopened, &reopened_events).await,
        vec![collected_change(
            1,
            0,
            ChangeKind::Put,
            b"user:retry",
            Some(bytes("retry")),
            "events",
        )]
    );
}

#[tokio::test]
async fn commit_failpoint_before_durability_sync_surfaces_exact_storage_error() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config("/commit-failpoint-before-sync"),
        dependencies(file_system, object_store),
    )
    .await
    .expect("open db");
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");

    db.__failpoint_registry().arm_error(
        crate::failpoints::names::DB_COMMIT_BEFORE_DURABILITY_SYNC,
        StorageError::io("simulated commit failpoint"),
        crate::failpoints::FailpointMode::Once,
    );

    let error = table
        .write(b"user:1".to_vec(), Value::bytes("v1"))
        .await
        .expect_err("failpoint should fail the commit");
    match error {
        crate::WriteError::Commit(CommitError::Storage(storage)) => {
            assert_eq!(storage.kind(), StorageErrorKind::Io);
            assert!(
                storage.to_string().contains("simulated commit failpoint"),
                "expected injected failpoint context, got {storage}"
            );
        }
        other => panic!("expected exact storage failure from commit failpoint, got {other:?}"),
    }

    assert_eq!(
        table
            .read(b"user:1".to_vec())
            .await
            .expect("read after failed commit"),
        None
    );

    let retry_sequence = table
        .write(b"user:1".to_vec(), Value::bytes("v2"))
        .await
        .expect("retry write after one-shot failpoint");
    assert_eq!(retry_sequence, SequenceNumber::new(2));
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
    assert_eq!(committed, SequenceNumber::new(1));
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
async fn group_commit_sync_failure_discards_later_provisional_batches_in_same_tail() {
    let root = "/group-commit-overlapping-failure";
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

    let mut batch_one_blocker = db.block_next_commit_phase(CommitPhase::AfterBatchSeal);
    let first_events = events.clone();
    let first =
        tokio::spawn(async move { first_events.write(b"user:1".to_vec(), bytes("v1")).await });
    assert_eq!(batch_one_blocker.sequence().await, SequenceNumber::new(1));

    let second_events = events.clone();
    let second =
        tokio::spawn(async move { second_events.write(b"user:2".to_vec(), bytes("v2")).await });

    while db.inner.next_sequence.load(Ordering::SeqCst) < 2 {
        tokio::task::yield_now().await;
    }

    batch_one_blocker.release();

    assert_group_commit_sync_failure(
        first
            .await
            .expect("join first")
            .expect_err("first write should fail"),
    );
    assert_group_commit_sync_failure(
        second
            .await
            .expect("join second")
            .expect_err("second write should fail"),
    );

    assert_eq!(db.current_sequence(), SequenceNumber::default());
    assert_eq!(db.current_durable_sequence(), SequenceNumber::default());
    assert_eq!(
        events.read(b"user:1".to_vec()).await.expect("read user:1"),
        None
    );
    assert_eq!(
        events.read(b"user:2".to_vec()).await.expect("read user:2"),
        None
    );
    assert_no_visible_or_durable_changes(&db, &events).await;

    db.flush()
        .await
        .expect("flush should not resurrect overlapping failed writes");
    assert_eq!(
        events
            .read(b"user:1".to_vec())
            .await
            .expect("read user:1 after flush"),
        None
    );
    assert_eq!(
        events
            .read(b"user:2".to_vec())
            .await
            .expect("read user:2 after flush"),
        None
    );

    file_system.crash();

    let reopened = Db::open(tiered_config(root), dependencies)
        .await
        .expect("reopen db");
    let reopened_events = reopened.table("events");
    assert_eq!(reopened.current_sequence(), SequenceNumber::default());
    assert_eq!(
        reopened.current_durable_sequence(),
        SequenceNumber::default()
    );
    assert_eq!(
        reopened_events
            .read(b"user:1".to_vec())
            .await
            .expect("read user:1 after reopen"),
        None
    );
    assert_eq!(
        reopened_events
            .read(b"user:2".to_vec())
            .await
            .expect("read user:2 after reopen"),
        None
    );
    assert_no_visible_or_durable_changes(&reopened, &reopened_events).await;

    let committed = reopened_events
        .write(b"user:3".to_vec(), bytes("v3"))
        .await
        .expect("write after overlapping failure");
    assert_eq!(committed, SequenceNumber::new(1));
    assert_eq!(
        visible_changes(&reopened, &reopened_events).await,
        vec![collected_change(
            1,
            0,
            ChangeKind::Put,
            b"user:3",
            Some(bytes("v3")),
            "events",
        )]
    );
}

#[tokio::test]
async fn group_commit_sync_failure_cascades_across_multiple_sealed_batches() {
    let root = "/group-commit-chained-overlapping-failure";
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config(root),
        dependencies(file_system.clone(), object_store),
    )
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

    let mut batch_one_blocker = db.block_next_commit_phase(CommitPhase::AfterBatchSeal);
    let first_events = events.clone();
    let first =
        tokio::spawn(async move { first_events.write(b"user:1".to_vec(), bytes("v1")).await });
    assert_eq!(batch_one_blocker.sequence().await, SequenceNumber::new(1));

    let mut batch_two_blocker = db.block_next_commit_phase(CommitPhase::AfterBatchSeal);
    let second_events = events.clone();
    let second =
        tokio::spawn(async move { second_events.write(b"user:2".to_vec(), bytes("v2")).await });
    assert_eq!(batch_two_blocker.sequence().await, SequenceNumber::new(2));

    let third_events = events.clone();
    let third =
        tokio::spawn(async move { third_events.write(b"user:3".to_vec(), bytes("v3")).await });

    while db.inner.next_sequence.load(Ordering::SeqCst) < 3 {
        tokio::task::yield_now().await;
    }

    batch_one_blocker.release();
    assert_group_commit_sync_failure(
        first
            .await
            .expect("join first")
            .expect_err("first write should fail"),
    );

    batch_two_blocker.release();
    assert_group_commit_sync_failure(
        second
            .await
            .expect("join second")
            .expect_err("second write should fail"),
    );
    assert_group_commit_sync_failure(
        third
            .await
            .expect("join third")
            .expect_err("third write should fail"),
    );

    assert_eq!(db.current_sequence(), SequenceNumber::default());
    assert_eq!(db.current_durable_sequence(), SequenceNumber::default());
    assert_eq!(
        events.read(b"user:1".to_vec()).await.expect("read user:1"),
        None
    );
    assert_eq!(
        events.read(b"user:2".to_vec()).await.expect("read user:2"),
        None
    );
    assert_eq!(
        events.read(b"user:3".to_vec()).await.expect("read user:3"),
        None
    );

    let committed = events
        .write(b"user:4".to_vec(), bytes("v4"))
        .await
        .expect("write after chained failure");
    assert_eq!(committed, SequenceNumber::new(1));
    assert_eq!(db.current_sequence(), committed);
    assert_eq!(db.current_durable_sequence(), committed);
}

#[test]
fn simulated_group_commit_sync_failure_discards_provisional_tail_across_restart() -> turmoil::Result
{
    SeededSimulationRunner::new(0x6a71_1001).run_with(|context| async move {
        let root = "/terracedb/sim/group-commit-sync-failure";
        let config = tiered_config(root);
        let db = context.open_db(config.clone()).await?;
        let events = db.create_table(row_table_config("events")).await?;

        context
            .file_system()
            .inject_failure(FileSystemFailure::timeout(
                FileSystemOperation::Sync,
                commit_log_segment_path(root, 1),
            ));

        let error = events
            .write(b"user:failed".to_vec(), bytes("failed"))
            .await
            .expect_err("group-commit sync should fail");
        assert_group_commit_sync_failure(error);
        assert_eq!(db.current_sequence(), SequenceNumber::default());
        assert_eq!(db.current_durable_sequence(), SequenceNumber::default());
        assert_eq!(events.read(b"user:failed".to_vec()).await?, None);
        assert_no_visible_or_durable_changes(&db, &events).await;

        let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
        let reopened_events = reopened.table("events");
        assert_eq!(reopened.current_sequence(), SequenceNumber::default());
        assert_eq!(
            reopened.current_durable_sequence(),
            SequenceNumber::default()
        );
        assert_eq!(reopened_events.read(b"user:failed".to_vec()).await?, None);

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

        let committed = reopened_events
            .write(b"user:ok".to_vec(), bytes("ok"))
            .await?;
        assert_eq!(committed, SequenceNumber::new(1));

        Ok(())
    })
}

#[test]
fn simulated_group_commit_sync_failure_discards_follow_on_batches_across_restart() -> turmoil::Result
{
    SeededSimulationRunner::new(0x6a71_1002).run_with(|context| async move {
        let root = "/terracedb/sim/group-commit-overlap-failure";
        let config = tiered_config(root);
        let db = context.open_db(config.clone()).await?;
        let events = db.create_table(row_table_config("events")).await?;

        context
            .file_system()
            .inject_failure(FileSystemFailure::timeout(
                FileSystemOperation::Sync,
                commit_log_segment_path(root, 1),
            ));

        let mut batch_one_blocker = db.block_next_commit_phase(CommitPhase::AfterBatchSeal);
        let first_events = events.clone();
        let first =
            tokio::spawn(async move { first_events.write(b"user:1".to_vec(), bytes("v1")).await });
        assert_eq!(batch_one_blocker.sequence().await, SequenceNumber::new(1));

        let second_events = events.clone();
        let second =
            tokio::spawn(async move { second_events.write(b"user:2".to_vec(), bytes("v2")).await });

        while db.inner.next_sequence.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }

        batch_one_blocker.release();

        assert_group_commit_sync_failure(
            first
                .await
                .expect("join first")
                .expect_err("first write should fail"),
        );
        assert_group_commit_sync_failure(
            second
                .await
                .expect("join second")
                .expect_err("second write should fail"),
        );
        assert_eq!(db.current_sequence(), SequenceNumber::default());
        assert_eq!(db.current_durable_sequence(), SequenceNumber::default());

        let reopened = context.restart_db(config, CutPoint::AfterStep).await?;
        let reopened_events = reopened.table("events");
        assert_eq!(reopened.current_sequence(), SequenceNumber::default());
        assert_eq!(
            reopened.current_durable_sequence(),
            SequenceNumber::default()
        );
        assert_eq!(reopened_events.read(b"user:1".to_vec()).await?, None);
        assert_eq!(reopened_events.read(b"user:2".to_vec()).await?, None);

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

        let committed = reopened_events
            .write(b"user:3".to_vec(), bytes("v3"))
            .await?;
        assert_eq!(committed, SequenceNumber::new(1));

        Ok(())
    })
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
async fn randomized_merge_read_path_matches_merge_shadow_oracle_across_flushes_and_compaction() {
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
async fn merge_resolution_preserves_non_commutative_commit_order_across_memtables_and_sstables() {
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
async fn long_merge_chains_force_a_collapsed_shadow_value_on_scan() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config("/merge-collapse-scan"),
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
        "no collapsed shadow row should exist before the scan"
    );

    assert_eq!(
        collect_rows(
            table
                .scan(b"doc".to_vec(), b"doe".to_vec(), ScanOptions::default())
                .await
                .expect("scan long merge chain"),
        )
        .await,
        vec![(b"doc".to_vec(), bytes("seed|A|B|C"))]
    );

    let collapsed = db
        .memtables_read()
        .read_at(table_id, b"doc", latest)
        .expect("scan should install a collapse shadow row");
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
    let dependencies = dependencies(file_system, object_store.clone());
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

    let layout = ObjectKeyLayout::new(&S3Location {
        bucket: "terracedb-test".to_string(),
        prefix: "s3-flush-reopen".to_string(),
    });
    object_store.inject_failure(ObjectStoreFailure::timeout(
        ObjectStoreOperation::Get,
        layout.backup_commit_log_segment(SegmentId::new(1)),
    ));
    object_store.inject_failure(ObjectStoreFailure::timeout(
        ObjectStoreOperation::Get,
        layout.backup_commit_log_segment(SegmentId::new(1)),
    ));

    let mut visible = db
        .scan_since(&events, LogCursor::beginning(), ScanOptions::default())
        .await
        .expect("visible remote change-feed scan should open");
    assert_change_feed_storage_error(
        match visible.try_next().await {
            Ok(Some(_)) | Ok(None) => {
                panic!("visible remote change-feed scan should surface a storage error")
            }
            Err(error) => ChangeFeedError::Storage(error),
        },
        crate::StorageErrorKind::Timeout,
        "simulated timeout",
    );
    let mut durable = db
        .scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
        .await
        .expect("durable remote change-feed scan should open");
    assert_change_feed_storage_error(
        match durable.try_next().await {
            Ok(Some(_)) | Ok(None) => {
                panic!("durable remote change-feed scan should surface a storage error")
            }
            Err(error) => ChangeFeedError::Storage(error),
        },
        crate::StorageErrorKind::Timeout,
        "simulated timeout",
    );

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
async fn local_change_feed_scan_failures_return_typed_storage_errors() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config("/change-feed-local-scan-failure"),
        dependencies(file_system.clone(), object_store),
    )
    .await
    .expect("open db");
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create events table");

    table
        .write(b"user:1".to_vec(), bytes("v1"))
        .await
        .expect("write event");

    file_system.inject_failure(
        FileSystemFailure::timeout(
            FileSystemOperation::ReadAt,
            commit_log_segment_path("/change-feed-local-scan-failure", 1),
        )
        .persistent(),
    );

    let mut visible = db
        .scan_since(&table, LogCursor::beginning(), ScanOptions::default())
        .await
        .expect("visible change-feed scan should open");
    assert_change_feed_storage_error(
        match visible.try_next().await {
            Ok(Some(_)) | Ok(None) => {
                panic!("visible change-feed scan should surface a storage error")
            }
            Err(error) => ChangeFeedError::Storage(error),
        },
        crate::StorageErrorKind::Timeout,
        "simulated timeout",
    );
    let mut durable = db
        .scan_durable_since(&table, LogCursor::beginning(), ScanOptions::default())
        .await
        .expect("durable change-feed scan should open");
    assert_change_feed_storage_error(
        match durable.try_next().await {
            Ok(Some(_)) | Ok(None) => {
                panic!("durable change-feed scan should surface a storage error")
            }
            Err(error) => ChangeFeedError::Storage(error),
        },
        crate::StorageErrorKind::Timeout,
        "simulated timeout",
    );
}

#[tokio::test]
async fn visible_change_scans_can_lead_durable_scans_in_deferred_mode() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_config_with_durability("/cdc-visible-vs-durable", TieredDurabilityMode::Deferred),
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
async fn remote_durable_change_feed_limit_stops_before_fetching_later_segments() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let inner_store = Arc::new(StubObjectStore::default());
    let recording_store = Arc::new(RecordingObjectStore::new(inner_store));
    let dependencies = DbDependencies::new(
        file_system,
        recording_store.clone(),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(7)),
    );
    let config = s3_primary_config("cdc-remote-limit");
    let remote_config = match &config.storage {
        StorageConfig::S3Primary(config) => config,
        _ => unreachable!("expected s3-primary config"),
    };

    let db = Db::open(config.clone(), dependencies)
        .await
        .expect("open s3-primary db");
    let events = db
        .create_table(row_table_config("events"))
        .await
        .expect("create events table");

    let first = events
        .write(b"user:1".to_vec(), bytes("v1"))
        .await
        .expect("write first event");
    db.flush().await.expect("flush first segment");

    let second = events
        .write(b"user:2".to_vec(), bytes("v2"))
        .await
        .expect("write second event");
    db.flush().await.expect("flush second segment");

    recording_store.clear_calls();
    let first_page = db
        .scan_durable_since(
            &events,
            LogCursor::beginning(),
            ScanOptions {
                limit: Some(1),
                ..ScanOptions::default()
            },
        )
        .await
        .expect("scan first durable page")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect first durable page");

    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].sequence, first);
    assert_eq!(
        recording_store.get_calls(),
        vec![Db::remote_commit_log_segment_key(
            remote_config,
            SegmentId::new(1),
        )]
    );
    assert_eq!(second, SequenceNumber::new(2));
}

#[tokio::test]
async fn remote_durable_change_feed_surfaces_mid_stream_get_errors_after_yielding_prefix() {
    let file_system = Arc::new(crate::StubFileSystem::default());
    let inner_store = Arc::new(StubObjectStore::default());
    let recording_store = Arc::new(RecordingObjectStore::new(inner_store.clone()));
    let dependencies = DbDependencies::new(
        file_system,
        recording_store,
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(7)),
    );
    let config = s3_primary_config("cdc-mid-stream-error");
    let remote_config = match &config.storage {
        StorageConfig::S3Primary(config) => config,
        _ => unreachable!("expected s3-primary config"),
    };

    let db = Db::open(config.clone(), dependencies)
        .await
        .expect("open s3-primary db");
    let events = db
        .create_table(row_table_config("events"))
        .await
        .expect("create events table");

    let first = events
        .write(b"user:1".to_vec(), bytes("v1"))
        .await
        .expect("write first event");
    db.flush().await.expect("flush first segment");

    events
        .write(b"user:2".to_vec(), bytes("v2"))
        .await
        .expect("write second event");
    db.flush().await.expect("flush second segment");

    let second_key = Db::remote_commit_log_segment_key(remote_config, SegmentId::new(2));
    inner_store.inject_failure(ObjectStoreFailure::timeout(
        ObjectStoreOperation::Get,
        second_key.clone(),
    ));

    let mut stream = db
        .scan_durable_since(&events, LogCursor::beginning(), ScanOptions::default())
        .await
        .expect("open durable change feed");

    let first_entry = stream
        .try_next()
        .await
        .expect("first segment should stream successfully")
        .expect("first entry should exist");
    assert_eq!(first_entry.sequence, first);
    assert_eq!(first_entry.key, b"user:1".to_vec());

    let error = stream
        .try_next()
        .await
        .expect_err("second segment get should fail mid-stream");
    assert_eq!(error.kind(), crate::StorageErrorKind::Timeout);
    assert!(error.message().contains("simulated timeout"));
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
