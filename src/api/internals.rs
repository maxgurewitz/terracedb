use super::*;
use crate::{FileSystem, Timestamp};
use arc_swap::ArcSwap;

pub(super) const CATALOG_FORMAT_VERSION: u32 = 1;
pub(super) const CATALOG_READ_CHUNK_LEN: usize = 8 * 1024;
pub(super) const LOCAL_CATALOG_RELATIVE_PATH: &str = "catalog/CATALOG.json";
pub(super) const LOCAL_CATALOG_TEMP_SUFFIX: &str = ".tmp";
pub(super) const LOCAL_COMMIT_LOG_RELATIVE_DIR: &str = "commitlog";
pub(super) const OBJECT_CATALOG_RELATIVE_KEY: &str = "control/catalog/CATALOG.json";
pub(super) const LOCAL_CURRENT_RELATIVE_PATH: &str = "CURRENT";
pub(super) const LOCAL_MANIFEST_DIR_RELATIVE_PATH: &str = "manifest";
pub(super) const LOCAL_MANIFEST_TEMP_SUFFIX: &str = ".tmp";
pub(super) const LOCAL_SSTABLE_RELATIVE_DIR: &str = "sst";
pub(super) const LOCAL_SSTABLE_SHARD_DIR: &str = "0000";
pub(super) const LOCAL_REMOTE_CACHE_RELATIVE_DIR: &str = "cache/remote";
pub(super) const MANIFEST_FORMAT_VERSION: u32 = 1;
pub(super) const REMOTE_MANIFEST_FORMAT_VERSION: u32 = 1;
pub(super) const BACKUP_GC_METADATA_FORMAT_VERSION: u32 = 1;
pub(super) const BACKUP_MANIFEST_RETENTION_LIMIT: usize = 1;
pub(super) const BACKUP_GC_GRACE_PERIOD_MILLIS: u64 = 60_000;
pub(super) const LOCAL_BACKUP_RESTORE_MARKER_RELATIVE_PATH: &str = "backup/RESTORE_INCOMPLETE";
pub(super) const LEVELED_BASE_LEVEL_TARGET_BYTES: u64 = 4 * 1024;
pub(super) const LEVELED_LEVEL_SIZE_MULTIPLIER: u64 = 10;
pub(super) const LEVELED_L0_COMPACTION_TRIGGER: usize = 2;
pub(super) const TIERED_LEVEL_RUN_COMPACTION_TRIGGER: usize = 3;
pub(super) const FIFO_MAX_LIVE_SSTABLES: usize = 2;
pub(super) const ROW_SSTABLE_FORMAT_VERSION: u32 = 1;
pub(super) const COLUMNAR_SSTABLE_FORMAT_VERSION: u32 = 1;
pub(super) const COLUMNAR_SSTABLE_MAGIC: &[u8; 8] = b"TDBCOL1\n";
pub(super) const MVCC_KEY_SEPARATOR: u8 = 0;
pub(super) const DEFAULT_MAX_MERGE_OPERAND_CHAIN_LENGTH: usize = 8;
pub(super) const MAX_SCHEDULER_DEFER_CYCLES: u32 = 3;

pub(super) struct DbInner {
    pub(super) config: DbConfig,
    pub(super) execution_identity: String,
    pub(super) scheduler: Arc<dyn Scheduler>,
    pub(super) resource_manager: Arc<dyn crate::execution::ResourceManager>,
    pub(super) execution_profile: crate::execution::DbExecutionProfile,
    pub(super) dependencies: DbDependencies,
    // Row SSTables stay fully resident after open; this cache only applies to the
    // lazy columnar read path.
    pub(super) columnar_read_context: Arc<ColumnarReadContext>,
    pub(super) catalog_location: CatalogLocation,
    pub(super) catalog_write_lock: AsyncMutex<()>,
    pub(super) commit_lock: AsyncMutex<()>,
    pub(super) maintenance_lock: AsyncMutex<()>,
    pub(super) backup_lock: AsyncMutex<()>,
    pub(super) commit_runtime: AsyncMutex<CommitRuntime>,
    pub(super) commit_coordinator: Mutex<CommitCoordinator>,
    pub(super) commit_log_scans: Mutex<CommitLogScanRegistry>,
    pub(super) next_table_id: AtomicU32,
    pub(super) next_sequence: AtomicU64,
    pub(super) current_sequence: AtomicU64,
    pub(super) current_durable_sequence: AtomicU64,
    pub(super) tables: RwLock<BTreeMap<String, StoredTable>>,
    pub(super) memtables: RwLock<MemtableState>,
    pub(super) sstables: RwLock<SstableState>,
    pub(super) next_sstable_id: AtomicU64,
    pub(super) snapshot_tracker: Mutex<SnapshotTracker>,
    pub(super) next_snapshot_id: AtomicU64,
    pub(super) compaction_filter_stats: Mutex<BTreeMap<TableId, CompactionFilterStats>>,
    pub(super) visible_watchers: Arc<WatermarkRegistry>,
    pub(super) durable_watchers: Arc<WatermarkRegistry>,
    pub(super) db_progress: DbProgressPublisher,
    pub(super) pending_work_budget_state: Mutex<PendingWorkBudgetState>,
    pub(super) scheduler_observability: SchedulerObservabilityStats,
    pub(super) compact_to_wide_stats: Mutex<BTreeMap<CompactToWideStatsKey, CompactToWideStats>>,
}

#[derive(Clone)]
pub(super) struct StoredTable {
    pub(super) id: TableId,
    pub(super) config: TableConfig,
    pub(super) resharding: Option<crate::sharding::PersistedTableReshardingPlan>,
}

#[derive(Clone, Debug)]
pub(super) enum CatalogLocation {
    LocalFile { path: String, temp_path: String },
    ObjectStore { key: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedCatalog {
    pub(super) format_version: u32,
    pub(super) tables: Vec<PersistedCatalogEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedCatalogEntry {
    pub(super) id: TableId,
    pub(super) config: PersistedTableConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedTableConfig {
    pub(super) name: String,
    pub(super) format: TableFormat,
    pub(super) max_merge_operand_chain_length: Option<u32>,
    pub(super) bloom_filter_bits_per_key: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) history_retention_sequences: Option<u64>,
    pub(super) compaction_strategy: CompactionStrategy,
    pub(super) schema: Option<SchemaDefinition>,
    #[serde(default)]
    pub(super) sharding: crate::ShardingConfig,
    pub(super) metadata: TableMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) resharding: Option<crate::sharding::PersistedTableReshardingPlan>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct SstableState {
    pub(super) manifest_generation: ManifestId,
    pub(super) last_flushed_sequence: SequenceNumber,
    pub(super) live: Vec<ResidentRowSstable>,
}

#[derive(Clone, Debug)]
pub(super) struct ResidentRowSstable {
    pub(super) meta: PersistedManifestSstable,
    pub(super) rows: Vec<SstableRow>,
    pub(super) user_key_bloom_filter: Option<UserKeyBloomFilter>,
    pub(super) columnar: Option<ResidentColumnarSstable>,
}

#[derive(Clone, Debug)]
pub(super) struct ResidentColumnarSstable {
    pub(super) source: StorageSource,
}

#[derive(Clone, Debug)]
pub(super) struct ColumnProjection {
    pub(super) fields: Vec<FieldDefinition>,
}

#[derive(Clone, Debug)]
pub(super) struct ColumnarRowRef {
    pub(super) local_id: String,
    pub(super) key: Key,
    pub(super) sequence: SequenceNumber,
    pub(super) kind: ChangeKind,
    pub(super) row_index: usize,
}

#[derive(Clone, Debug)]
pub(super) struct LoadedColumnarMetadata {
    pub(super) footer: Arc<PersistedColumnarSstableFooter>,
    pub(super) footer_start: usize,
    pub(super) key_index: Arc<Vec<Key>>,
    pub(super) sequences: Arc<Vec<SequenceNumber>>,
    pub(super) tombstones: Arc<Vec<bool>>,
    pub(super) row_kinds: Arc<Vec<ChangeKind>>,
}

#[derive(Clone, Debug)]
pub(super) struct ColumnarMaterialization {
    pub(super) rows: BTreeMap<usize, Value>,
    pub(super) source: ScanMaterializationSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ColumnarReadAccessPattern {
    Point,
    Scan,
    Background,
    Recovery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ColumnarReadArtifact {
    Footer,
    Metadata,
    ColumnBlock,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ColumnarCachePolicy {
    pub(super) use_raw_byte_cache: bool,
    pub(super) populate_raw_byte_cache: bool,
    pub(super) use_decoded_cache: bool,
    pub(super) populate_decoded_cache: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct ColumnarCacheStatsSnapshot {
    pub(super) raw_byte_hits: u64,
    pub(super) raw_byte_misses: u64,
    pub(super) decoded_footer_hits: u64,
    pub(super) decoded_footer_misses: u64,
    pub(super) decoded_footer_admissions: u64,
    pub(super) decoded_metadata_hits: u64,
    pub(super) decoded_metadata_misses: u64,
    pub(super) decoded_metadata_admissions: u64,
    pub(super) decoded_column_block_hits: u64,
    pub(super) decoded_column_block_misses: u64,
    pub(super) decoded_column_block_admissions: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnarCacheUsageSnapshot {
    pub raw_byte_entries: usize,
    pub raw_byte_bytes: u64,
    pub raw_byte_budget_bytes: u64,
    pub decoded_metadata_entries: usize,
    pub decoded_metadata_entry_limit: usize,
    pub decoded_metadata_bytes: u64,
    pub decoded_column_entries: usize,
    pub decoded_column_entry_limit: usize,
    pub decoded_column_bytes: u64,
    pub by_domain: BTreeMap<crate::ExecutionDomainPath, DomainColumnarCacheUsageSnapshot>,
}

#[derive(Debug)]
pub struct ColumnarCacheUsageSubscription {
    inner: watch::Receiver<Arc<ColumnarCacheUsageSnapshot>>,
}

impl ColumnarCacheUsageSubscription {
    pub(super) fn new(inner: watch::Receiver<Arc<ColumnarCacheUsageSnapshot>>) -> Self {
        Self { inner }
    }

    pub fn current(&self) -> ColumnarCacheUsageSnapshot {
        self.inner.borrow().as_ref().clone()
    }

    pub async fn changed(
        &mut self,
    ) -> Result<ColumnarCacheUsageSnapshot, crate::SubscriptionClosed> {
        self.inner
            .changed()
            .await
            .map_err(|_| crate::SubscriptionClosed)?;
        Ok(self.current())
    }

    pub async fn wait_for<F>(
        &mut self,
        mut predicate: F,
    ) -> Result<ColumnarCacheUsageSnapshot, crate::SubscriptionClosed>
    where
        F: FnMut(&ColumnarCacheUsageSnapshot) -> bool,
    {
        let snapshot = self.current();
        if predicate(&snapshot) {
            return Ok(snapshot);
        }

        loop {
            let snapshot = self.changed().await?;
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
        }
    }
}

impl Clone for ColumnarCacheUsageSubscription {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DomainColumnarCacheUsageSnapshot {
    pub raw_byte_entries: usize,
    pub raw_byte_bytes: u64,
    pub raw_byte_budget_bytes: u64,
    pub decoded_metadata_entries: usize,
    pub decoded_metadata_entry_limit: usize,
    pub decoded_metadata_bytes: u64,
    pub decoded_column_entries: usize,
    pub decoded_column_entry_limit: usize,
    pub decoded_column_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ColumnarCacheLaneBudget {
    pub(super) raw_byte_budget_bytes: u64,
    pub(super) decoded_metadata_entry_limit: usize,
    pub(super) decoded_column_entry_limit: usize,
}

pub(super) struct ColumnarReadContext {
    pub(super) dependencies: DbDependencies,
    pub(super) remote_cache: Option<Arc<RemoteCache>>,
    pub(super) decoded_cache: DecodedColumnarCache,
    pub(super) raw_byte_cache_enabled: AtomicBool,
    pub(super) raw_byte_cache_population_enabled: AtomicBool,
    pub(super) decoded_cache_enabled: AtomicBool,
    pub(super) raw_byte_cache_budget_bytes: u64,
    pub(super) raw_byte_cache_budget_state: Mutex<RawByteCacheBudgetState>,
    pub(super) cache_domain_paths: BTreeMap<crate::ExecutionLane, crate::ExecutionDomainPath>,
    pub(super) cache_lane_budgets: BTreeMap<crate::ExecutionLane, ColumnarCacheLaneBudget>,
    latest_usage_snapshot: ArcSwap<ColumnarCacheUsageSnapshot>,
    published_usage_snapshot: watch::Sender<Arc<ColumnarCacheUsageSnapshot>>,
    pub(super) skip_indexes_enabled: bool,
    pub(super) projection_sidecars_enabled: bool,
    #[allow(dead_code)]
    pub(super) aggressive_background_repair: bool,
}

pub(super) struct ColumnarReadContextInit {
    pub(super) dependencies: DbDependencies,
    pub(super) remote_cache: Option<Arc<RemoteCache>>,
    pub(super) decoded_cache: DecodedColumnarCache,
    pub(super) raw_byte_cache_enabled: bool,
    pub(super) raw_byte_cache_population_enabled: bool,
    pub(super) decoded_cache_enabled: bool,
    pub(super) raw_byte_cache_budget_bytes: u64,
    pub(super) raw_byte_cache_budget_state: RawByteCacheBudgetState,
    pub(super) cache_domain_paths: BTreeMap<crate::ExecutionLane, crate::ExecutionDomainPath>,
    pub(super) cache_lane_budgets: BTreeMap<crate::ExecutionLane, ColumnarCacheLaneBudget>,
    pub(super) skip_indexes_enabled: bool,
    pub(super) projection_sidecars_enabled: bool,
    pub(super) aggressive_background_repair: bool,
}

pub(super) struct DecodedColumnarCache {
    pub(super) footers: RwLock<BTreeMap<ColumnarSstableIdentity, CachedColumnarFooter>>,
    pub(super) key_indexes: RwLock<BTreeMap<ColumnarSstableIdentity, Arc<Vec<Key>>>>,
    pub(super) sequence_columns:
        RwLock<BTreeMap<ColumnarSstableIdentity, Arc<Vec<SequenceNumber>>>>,
    pub(super) tombstone_bitmaps: RwLock<BTreeMap<ColumnarSstableIdentity, Arc<Vec<bool>>>>,
    pub(super) row_kind_columns: RwLock<BTreeMap<ColumnarSstableIdentity, Arc<Vec<ChangeKind>>>>,
    pub(super) column_blocks: RwLock<BTreeMap<ColumnarColumnCacheKey, Arc<Vec<FieldValue>>>>,
    pub(super) stats: ColumnarCacheStats,
    pub(super) metadata_entry_limit: usize,
    pub(super) column_entry_limit: usize,
    pub(super) metadata_entry_limits: BTreeMap<crate::ExecutionLane, usize>,
    pub(super) column_entry_limits: BTreeMap<crate::ExecutionLane, usize>,
    pub(super) metadata_owners: RwLock<BTreeMap<ColumnarSstableIdentity, crate::ExecutionLane>>,
    pub(super) column_owners: RwLock<BTreeMap<ColumnarColumnCacheKey, crate::ExecutionLane>>,
    pub(super) metadata_order: Mutex<VecDeque<ColumnarSstableIdentity>>,
    pub(super) column_order: Mutex<VecDeque<ColumnarColumnCacheKey>>,
}

impl ColumnarReadContext {
    pub(super) fn new(init: ColumnarReadContextInit) -> Self {
        let raw_byte_cache_budget_state = Mutex::new(init.raw_byte_cache_budget_state);
        let initial_usage_snapshot = Self::build_usage_snapshot(
            init.raw_byte_cache_budget_bytes,
            &raw_byte_cache_budget_state,
            &init.decoded_cache,
            &init.cache_domain_paths,
            &init.cache_lane_budgets,
        );
        let (published_usage_snapshot, _receiver) =
            watch::channel(Arc::new(initial_usage_snapshot.clone()));
        Self {
            dependencies: init.dependencies,
            remote_cache: init.remote_cache,
            decoded_cache: init.decoded_cache,
            raw_byte_cache_enabled: AtomicBool::new(init.raw_byte_cache_enabled),
            raw_byte_cache_population_enabled: AtomicBool::new(
                init.raw_byte_cache_population_enabled,
            ),
            decoded_cache_enabled: AtomicBool::new(init.decoded_cache_enabled),
            raw_byte_cache_budget_bytes: init.raw_byte_cache_budget_bytes,
            raw_byte_cache_budget_state,
            cache_domain_paths: init.cache_domain_paths,
            cache_lane_budgets: init.cache_lane_budgets,
            latest_usage_snapshot: ArcSwap::from_pointee(initial_usage_snapshot),
            published_usage_snapshot,
            skip_indexes_enabled: init.skip_indexes_enabled,
            projection_sidecars_enabled: init.projection_sidecars_enabled,
            aggressive_background_repair: init.aggressive_background_repair,
        }
    }

    fn build_usage_snapshot(
        raw_byte_budget_bytes: u64,
        raw_byte_cache_budget_state: &Mutex<RawByteCacheBudgetState>,
        decoded_cache: &DecodedColumnarCache,
        cache_domain_paths: &BTreeMap<crate::ExecutionLane, crate::ExecutionDomainPath>,
        cache_lane_budgets: &BTreeMap<crate::ExecutionLane, ColumnarCacheLaneBudget>,
    ) -> ColumnarCacheUsageSnapshot {
        let mut usage = decoded_cache.usage_snapshot(
            raw_byte_budget_bytes,
            cache_domain_paths,
            cache_lane_budgets,
        );
        let state = raw_byte_cache_budget_state.lock();
        usage.raw_byte_entries = state.lengths.len();
        usage.raw_byte_bytes = state.total_bytes;
        for (domain, domain_usage) in &mut usage.by_domain {
            if let Some((lane, _)) = cache_domain_paths
                .iter()
                .find(|(_, mapped_domain)| *mapped_domain == domain)
            {
                domain_usage.raw_byte_entries = state
                    .owners
                    .values()
                    .filter(|owner| **owner == *lane)
                    .count();
                domain_usage.raw_byte_bytes = state
                    .owners
                    .iter()
                    .filter(|(_, owner)| **owner == *lane)
                    .map(|(key, _)| state.lengths.get(key).copied().unwrap_or_default())
                    .sum();
            }
        }
        usage
    }

    pub(super) fn publish_usage_snapshot(&self) {
        let snapshot = Self::build_usage_snapshot(
            self.raw_byte_cache_budget_bytes,
            &self.raw_byte_cache_budget_state,
            &self.decoded_cache,
            &self.cache_domain_paths,
            &self.cache_lane_budgets,
        );
        let snapshot = Arc::new(snapshot);
        self.latest_usage_snapshot.store(snapshot.clone());
        self.published_usage_snapshot.send_replace(snapshot);
    }

    pub(super) fn usage_snapshot(&self) -> ColumnarCacheUsageSnapshot {
        self.latest_usage_snapshot.load_full().as_ref().clone()
    }

    pub(super) fn subscribe_usage(&self) -> ColumnarCacheUsageSubscription {
        ColumnarCacheUsageSubscription::new(self.published_usage_snapshot.subscribe())
    }
}

#[derive(Default)]
pub(super) struct ColumnarCacheStats {
    pub(super) raw_byte_hits: AtomicU64,
    pub(super) raw_byte_misses: AtomicU64,
    pub(super) decoded_footer_hits: AtomicU64,
    pub(super) decoded_footer_misses: AtomicU64,
    pub(super) decoded_footer_admissions: AtomicU64,
    pub(super) decoded_metadata_hits: AtomicU64,
    pub(super) decoded_metadata_misses: AtomicU64,
    pub(super) decoded_metadata_admissions: AtomicU64,
    pub(super) decoded_column_block_hits: AtomicU64,
    pub(super) decoded_column_block_misses: AtomicU64,
    pub(super) decoded_column_block_admissions: AtomicU64,
}

#[derive(Default)]
struct SchedulerObservabilityState {
    snapshot: crate::SchedulerObservabilitySnapshot,
    deferred_work_domains: BTreeMap<String, crate::ExecutionDomainPath>,
    deferred_work_physical_shards: BTreeMap<String, crate::PhysicalShardId>,
}

pub(super) struct SchedulerObservabilityStats {
    state: Mutex<SchedulerObservabilityState>,
    latest_snapshot: ArcSwap<crate::SchedulerObservabilitySnapshot>,
    published_snapshot: watch::Sender<Arc<crate::SchedulerObservabilitySnapshot>>,
    admission_observations: tokio::sync::broadcast::Sender<crate::AdmissionObservation>,
}

impl SchedulerObservabilityStats {
    pub(super) fn new() -> Self {
        let initial_snapshot = Arc::new(crate::SchedulerObservabilitySnapshot::default());
        let (published_snapshot, _receiver) = watch::channel(initial_snapshot.clone());
        let (admission_observations, _receiver) = tokio::sync::broadcast::channel(1024);
        Self {
            state: Mutex::default(),
            latest_snapshot: ArcSwap::from(initial_snapshot),
            published_snapshot,
            admission_observations,
        }
    }

    fn refresh_deferred_work(state: &mut SchedulerObservabilityState) {
        let snapshot = &mut state.snapshot;
        let domains = &state.deferred_work_domains;
        let physical_shards = &state.deferred_work_physical_shards;
        snapshot.deferred_work_by_domain.clear();
        snapshot.deferred_work_by_physical_shard.clear();
        snapshot.starved_domains.clear();
        snapshot.starved_physical_shards.clear();
        for (work_id, cycles) in &snapshot.deferred_work {
            let Some(domain) = domains.get(work_id) else {
                if let Some(physical_shard) = physical_shards.get(work_id) {
                    *snapshot
                        .deferred_work_by_physical_shard
                        .entry(*physical_shard)
                        .or_default() += *cycles;
                    if *cycles >= MAX_SCHEDULER_DEFER_CYCLES {
                        *snapshot
                            .starved_physical_shards
                            .entry(*physical_shard)
                            .or_default() += 1;
                    }
                }
                continue;
            };
            *snapshot
                .deferred_work_by_domain
                .entry(domain.clone())
                .or_default() += *cycles;
            if let Some(physical_shard) = physical_shards.get(work_id) {
                *snapshot
                    .deferred_work_by_physical_shard
                    .entry(*physical_shard)
                    .or_default() += *cycles;
            }
            if *cycles >= MAX_SCHEDULER_DEFER_CYCLES {
                *snapshot.starved_domains.entry(domain.clone()).or_default() += 1;
                if let Some(physical_shard) = physical_shards.get(work_id) {
                    *snapshot
                        .starved_physical_shards
                        .entry(*physical_shard)
                        .or_default() += 1;
                }
            }
        }
    }

    fn pending_work_physical_shard(
        candidate: &PendingWorkCandidate,
    ) -> Option<crate::PhysicalShardId> {
        candidate
            .pending
            .physical_shard
            .or_else(|| candidate.tag.physical_shard())
    }

    fn publish_locked(&self, state: &SchedulerObservabilityState) {
        let snapshot = Arc::new(state.snapshot.clone());
        self.latest_snapshot.store(snapshot.clone());
        self.published_snapshot.send_replace(snapshot);
    }

    pub(super) fn snapshot(&self) -> crate::SchedulerObservabilitySnapshot {
        self.latest_snapshot.load_full().as_ref().clone()
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<Arc<crate::SchedulerObservabilitySnapshot>> {
        self.published_snapshot.subscribe()
    }

    pub(super) fn subscribe_admission_observations(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::AdmissionObservation> {
        self.admission_observations.subscribe()
    }

    pub(super) fn prune_deferred_work(&self, live_work_ids: &BTreeSet<&str>) {
        let mut state = mutex_lock(&self.state);
        state
            .snapshot
            .deferred_work
            .retain(|work_id, _| live_work_ids.contains(work_id.as_str()));
        state
            .deferred_work_domains
            .retain(|work_id, _| live_work_ids.contains(work_id.as_str()));
        state
            .deferred_work_physical_shards
            .retain(|work_id, _| live_work_ids.contains(work_id.as_str()));
        Self::refresh_deferred_work(&mut state);
        self.publish_locked(&state);
    }

    pub(super) fn record_deferred_work(
        &self,
        candidates: &[PendingWorkCandidate],
        decisions: &BTreeMap<String, ScheduleAction>,
    ) {
        let mut state = mutex_lock(&self.state);
        for candidate in candidates {
            match decisions
                .get(&candidate.pending.id)
                .copied()
                .unwrap_or(ScheduleAction::Defer)
            {
                ScheduleAction::Execute => {
                    state.snapshot.deferred_work.remove(&candidate.pending.id);
                    state.deferred_work_domains.remove(&candidate.pending.id);
                    state
                        .deferred_work_physical_shards
                        .remove(&candidate.pending.id);
                }
                ScheduleAction::Defer => {
                    *state
                        .snapshot
                        .deferred_work
                        .entry(candidate.pending.id.clone())
                        .or_default() += 1;
                    state
                        .deferred_work_domains
                        .insert(candidate.pending.id.clone(), candidate.tag.domain.clone());
                    if let Some(physical_shard) = Self::pending_work_physical_shard(candidate) {
                        state
                            .deferred_work_physical_shards
                            .insert(candidate.pending.id.clone(), physical_shard);
                    } else {
                        state
                            .deferred_work_physical_shards
                            .remove(&candidate.pending.id);
                    }
                }
            }
        }
        Self::refresh_deferred_work(&mut state);
        self.publish_locked(&state);
    }

    pub(super) fn reset_work_deferral(&self, work_id: &str) {
        let mut state = mutex_lock(&self.state);
        state.snapshot.deferred_work.remove(work_id);
        state.deferred_work_domains.remove(work_id);
        state.deferred_work_physical_shards.remove(work_id);
        Self::refresh_deferred_work(&mut state);
        self.publish_locked(&state);
    }

    pub(super) fn work_deferral_cycles(&self, work_id: &str) -> u32 {
        mutex_lock(&self.state)
            .snapshot
            .deferred_work
            .get(work_id)
            .copied()
            .unwrap_or_default()
    }

    pub(super) fn record_forced_execution(&self) {
        let mut state = mutex_lock(&self.state);
        state.snapshot.forced_executions += 1;
        self.publish_locked(&state);
    }

    pub(super) fn record_forced_flush(&self) {
        let mut state = mutex_lock(&self.state);
        state.snapshot.forced_flushes += 1;
        self.publish_locked(&state);
    }

    pub(super) fn record_forced_l0_compaction(&self) {
        let mut state = mutex_lock(&self.state);
        state.snapshot.forced_l0_compactions += 1;
        self.publish_locked(&state);
    }

    pub(super) fn record_budget_blocked_execution(&self, tag: &crate::WorkRuntimeTag) {
        let mut state = mutex_lock(&self.state);
        state.snapshot.budget_blocked_executions += 1;
        *state
            .snapshot
            .budget_blocked_executions_by_domain
            .entry(tag.domain.clone())
            .or_default() += 1;
        if let Some(physical_shard) = tag.physical_shard() {
            *state
                .snapshot
                .budget_blocked_executions_by_physical_shard
                .entry(physical_shard)
                .or_default() += 1;
        }
        self.publish_locked(&state);
    }

    pub(super) fn record_background_delay(&self, tag: &crate::WorkRuntimeTag, delay: Duration) {
        if delay.is_zero() {
            return;
        }
        let mut state = mutex_lock(&self.state);
        state.snapshot.background_delay_events += 1;
        state.snapshot.background_delay_millis += delay.as_millis() as u64;
        *state
            .snapshot
            .background_delay_events_by_domain
            .entry(tag.domain.clone())
            .or_default() += 1;
        *state
            .snapshot
            .background_delay_millis_by_domain
            .entry(tag.domain.clone())
            .or_default() += delay.as_millis() as u64;
        if let Some(physical_shard) = tag.physical_shard() {
            *state
                .snapshot
                .background_delay_events_by_physical_shard
                .entry(physical_shard)
                .or_default() += 1;
            *state
                .snapshot
                .background_delay_millis_by_physical_shard
                .entry(physical_shard)
                .or_default() += delay.as_millis() as u64;
        }
        self.publish_locked(&state);
    }

    pub(super) fn record_admission_diagnostics(
        &self,
        tag: &crate::WorkRuntimeTag,
        mut recorded: crate::RecordedAdmissionDiagnostics,
    ) {
        let physical_shard = tag.physical_shard();
        if let Some(physical_shard) = physical_shard {
            recorded.diagnostics.metadata.insert(
                crate::telemetry_attrs::PHYSICAL_SHARD.to_string(),
                serde_json::Value::String(physical_shard.to_string()),
            );
        }
        let mut state = mutex_lock(&self.state);
        state
            .snapshot
            .current_admission_diagnostics_by_domain
            .insert(tag.domain.clone(), recorded.clone());
        if let Some(physical_shard) = physical_shard {
            state
                .snapshot
                .current_admission_diagnostics_by_physical_shard
                .insert(physical_shard, recorded.clone());
        }
        let last_non_open = if recorded.diagnostics.level != crate::AdmissionPressureLevel::Open {
            state
                .snapshot
                .last_non_open_admission_by_domain
                .insert(tag.domain.clone(), recorded.clone());
            if let Some(physical_shard) = physical_shard {
                state
                    .snapshot
                    .last_non_open_admission_by_physical_shard
                    .insert(physical_shard, recorded.clone());
            }
            Some(recorded.clone())
        } else {
            physical_shard
                .and_then(|physical_shard| {
                    state
                        .snapshot
                        .last_non_open_admission_by_physical_shard
                        .get(&physical_shard)
                        .cloned()
                })
                .or_else(|| {
                    state
                        .snapshot
                        .last_non_open_admission_by_domain
                        .get(&tag.domain)
                        .cloned()
                })
        };
        self.publish_locked(&state);
        drop(state);
        let _ = self
            .admission_observations
            .send(crate::AdmissionObservation {
                domain: tag.domain.clone(),
                physical_shard,
                current: recorded,
                last_non_open,
            });
    }

    pub(super) fn record_throttled_write_domain(&self, tag: &crate::WorkRuntimeTag) {
        let mut state = mutex_lock(&self.state);
        *state
            .snapshot
            .throttled_writes_by_domain
            .entry(tag.domain.clone())
            .or_default() += 1;
        if let Some(physical_shard) = tag.physical_shard() {
            *state
                .snapshot
                .throttled_writes_by_physical_shard
                .entry(physical_shard)
                .or_default() += 1;
        }
        self.publish_locked(&state);
    }
}

impl Default for SchedulerObservabilityStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CompactToWideStatsKey {
    pub(super) table_id: TableId,
    pub(super) local_id: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CompactToWideStats {
    pub(super) projected_read_count: u64,
    pub(super) full_row_read_count: u64,
    pub(super) projected_bytes_read: u64,
}

#[derive(Default)]
pub(super) struct PendingWorkBudgetUsage {
    pub(super) in_flight_bytes: u64,
    pub(super) in_flight_requests: u32,
    pub(super) concurrency: u32,
}

#[derive(Default)]
pub(super) struct DomainPendingWorkBudgetUsage {
    pub(super) in_flight_bytes: u64,
    pub(super) concurrency: u32,
    pub(super) local_requests: u32,
    pub(super) remote_requests: u32,
}

#[derive(Default)]
pub(super) struct PendingWorkBudgetState {
    pub(super) by_work_type: BTreeMap<PendingWorkType, PendingWorkBudgetUsage>,
    pub(super) by_domain: BTreeMap<crate::ExecutionDomainPath, DomainPendingWorkBudgetUsage>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RawByteCacheBudgetKey {
    pub(super) object_key: String,
    pub(super) span: crate::remote::CacheSpan,
}

#[derive(Default)]
pub(super) struct RawByteCacheBudgetState {
    pub(super) total_bytes: u64,
    pub(super) order: VecDeque<RawByteCacheBudgetKey>,
    pub(super) lengths: BTreeMap<RawByteCacheBudgetKey, u64>,
    pub(super) owners: BTreeMap<RawByteCacheBudgetKey, crate::ExecutionLane>,
}

#[derive(Clone, Debug)]
pub(super) struct CachedColumnarFooter {
    pub(super) footer: Arc<PersistedColumnarSstableFooter>,
    pub(super) footer_start: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ColumnarSstableIdentity {
    pub(super) table_id: TableId,
    pub(super) local_id: String,
    pub(super) checksum: u32,
    pub(super) data_checksum: u32,
    pub(super) length: u64,
    pub(super) schema_version: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ColumnarColumnCacheKey {
    pub(super) sstable: ColumnarSstableIdentity,
    pub(super) field_id: FieldId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedManifestSstable {
    pub(super) table_id: TableId,
    pub(super) level: u32,
    pub(super) local_id: String,
    pub(super) file_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) remote_key: Option<String>,
    pub(super) length: u64,
    pub(super) checksum: u32,
    pub(super) data_checksum: u32,
    pub(super) min_key: Key,
    pub(super) max_key: Key,
    pub(super) min_sequence: SequenceNumber,
    pub(super) max_sequence: SequenceNumber,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) schema_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) shard_ownership: Option<crate::ShardSstableOwnership>,
}

impl PersistedManifestSstable {
    pub(super) fn shard_ownership(&self) -> crate::ShardSstableOwnership {
        self.shard_ownership.clone().unwrap_or_else(|| {
            crate::ShardSstableOwnership::new(
                self.table_id,
                crate::PhysicalShardId::UNSHARDED,
                crate::ShardMapRevision::default(),
                crate::VirtualPartitionCoverage::single(crate::VirtualPartitionId::new(0)),
            )
        })
    }

    pub(super) fn physical_shard(&self) -> crate::PhysicalShardId {
        self.shard_ownership().physical_shard
    }

    pub(super) fn storage_source(&self) -> StorageSource {
        if !self.file_path.is_empty() {
            StorageSource::local_file(self.file_path.clone())
        } else if let Some(remote_key) = &self.remote_key {
            StorageSource::remote_object(remote_key.clone())
        } else {
            StorageSource::local_file(self.file_path.clone())
        }
    }

    pub(super) fn storage_descriptor(&self) -> &str {
        if !self.file_path.is_empty() {
            &self.file_path
        } else {
            self.remote_key.as_deref().unwrap_or("")
        }
    }

    pub(super) fn columnar_identity(&self) -> ColumnarSstableIdentity {
        ColumnarSstableIdentity {
            table_id: self.table_id,
            local_id: self.local_id.clone(),
            checksum: self.checksum,
            data_checksum: self.data_checksum,
            length: self.length,
            schema_version: self.schema_version,
        }
    }
}

impl DecodedColumnarCache {
    pub(super) fn new(
        metadata_entry_limits: BTreeMap<crate::ExecutionLane, usize>,
        column_entry_limits: BTreeMap<crate::ExecutionLane, usize>,
    ) -> Self {
        let metadata_entry_limit = metadata_entry_limits.values().copied().sum();
        let column_entry_limit = column_entry_limits.values().copied().sum();
        Self {
            footers: RwLock::new(BTreeMap::new()),
            key_indexes: RwLock::new(BTreeMap::new()),
            sequence_columns: RwLock::new(BTreeMap::new()),
            tombstone_bitmaps: RwLock::new(BTreeMap::new()),
            row_kind_columns: RwLock::new(BTreeMap::new()),
            column_blocks: RwLock::new(BTreeMap::new()),
            stats: ColumnarCacheStats::default(),
            metadata_entry_limit,
            column_entry_limit,
            metadata_entry_limits,
            column_entry_limits,
            metadata_owners: RwLock::new(BTreeMap::new()),
            column_owners: RwLock::new(BTreeMap::new()),
            metadata_order: Mutex::new(VecDeque::new()),
            column_order: Mutex::new(VecDeque::new()),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn snapshot(&self) -> ColumnarCacheStatsSnapshot {
        ColumnarCacheStatsSnapshot {
            raw_byte_hits: self.stats.raw_byte_hits.load(Ordering::Relaxed),
            raw_byte_misses: self.stats.raw_byte_misses.load(Ordering::Relaxed),
            decoded_footer_hits: self.stats.decoded_footer_hits.load(Ordering::Relaxed),
            decoded_footer_misses: self.stats.decoded_footer_misses.load(Ordering::Relaxed),
            decoded_footer_admissions: self.stats.decoded_footer_admissions.load(Ordering::Relaxed),
            decoded_metadata_hits: self.stats.decoded_metadata_hits.load(Ordering::Relaxed),
            decoded_metadata_misses: self.stats.decoded_metadata_misses.load(Ordering::Relaxed),
            decoded_metadata_admissions: self
                .stats
                .decoded_metadata_admissions
                .load(Ordering::Relaxed),
            decoded_column_block_hits: self.stats.decoded_column_block_hits.load(Ordering::Relaxed),
            decoded_column_block_misses: self
                .stats
                .decoded_column_block_misses
                .load(Ordering::Relaxed),
            decoded_column_block_admissions: self
                .stats
                .decoded_column_block_admissions
                .load(Ordering::Relaxed),
        }
    }

    pub(super) fn usage_snapshot(
        &self,
        raw_byte_budget_bytes: u64,
        cache_domain_paths: &BTreeMap<crate::ExecutionLane, crate::ExecutionDomainPath>,
        cache_lane_budgets: &BTreeMap<crate::ExecutionLane, ColumnarCacheLaneBudget>,
    ) -> ColumnarCacheUsageSnapshot {
        let metadata_owners = self.metadata_owners.read().clone();
        let column_owners = self.column_owners.read().clone();
        let mut by_domain = BTreeMap::new();
        for (&lane, domain) in cache_domain_paths {
            let lane_budget = cache_lane_budgets.get(&lane).copied().unwrap_or_default();
            by_domain.insert(
                domain.clone(),
                DomainColumnarCacheUsageSnapshot {
                    raw_byte_budget_bytes: lane_budget.raw_byte_budget_bytes,
                    decoded_metadata_entries: metadata_owners
                        .values()
                        .filter(|owner| **owner == lane)
                        .count(),
                    decoded_metadata_entry_limit: lane_budget.decoded_metadata_entry_limit,
                    decoded_metadata_bytes: self
                        .metadata_usage_bytes_for_lane(lane, &metadata_owners),
                    decoded_column_entries: column_owners
                        .values()
                        .filter(|owner| **owner == lane)
                        .count(),
                    decoded_column_entry_limit: lane_budget.decoded_column_entry_limit,
                    decoded_column_bytes: self.column_usage_bytes_for_lane(lane, &column_owners),
                    ..Default::default()
                },
            );
        }

        ColumnarCacheUsageSnapshot {
            raw_byte_entries: 0,
            raw_byte_bytes: 0,
            raw_byte_budget_bytes,
            decoded_metadata_entries: metadata_owners.len(),
            decoded_metadata_entry_limit: self.metadata_entry_limit,
            decoded_metadata_bytes: self.metadata_usage_bytes(),
            decoded_column_entries: column_owners.len(),
            decoded_column_entry_limit: self.column_entry_limit,
            decoded_column_bytes: self.column_usage_bytes(),
            by_domain,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn reset_stats(&self) {
        self.stats.raw_byte_hits.store(0, Ordering::Relaxed);
        self.stats.raw_byte_misses.store(0, Ordering::Relaxed);
        self.stats.decoded_footer_hits.store(0, Ordering::Relaxed);
        self.stats.decoded_footer_misses.store(0, Ordering::Relaxed);
        self.stats
            .decoded_footer_admissions
            .store(0, Ordering::Relaxed);
        self.stats.decoded_metadata_hits.store(0, Ordering::Relaxed);
        self.stats
            .decoded_metadata_misses
            .store(0, Ordering::Relaxed);
        self.stats
            .decoded_metadata_admissions
            .store(0, Ordering::Relaxed);
        self.stats
            .decoded_column_block_hits
            .store(0, Ordering::Relaxed);
        self.stats
            .decoded_column_block_misses
            .store(0, Ordering::Relaxed);
        self.stats
            .decoded_column_block_admissions
            .store(0, Ordering::Relaxed);
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn clear(&self) {
        self.footers.write().clear();
        self.key_indexes.write().clear();
        self.sequence_columns.write().clear();
        self.tombstone_bitmaps.write().clear();
        self.row_kind_columns.write().clear();
        self.column_blocks.write().clear();
        self.metadata_owners.write().clear();
        self.column_owners.write().clear();
        self.metadata_order.lock().clear();
        self.column_order.lock().clear();
    }

    pub(super) fn footer(
        &self,
        identity: &ColumnarSstableIdentity,
    ) -> Option<CachedColumnarFooter> {
        let cached = self.footers.read().get(identity).cloned();
        if cached.is_some() {
            self.stats
                .decoded_footer_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .decoded_footer_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    pub(super) fn insert_footer(
        &self,
        identity: ColumnarSstableIdentity,
        footer: CachedColumnarFooter,
        lane: crate::ExecutionLane,
    ) {
        self.footers.write().insert(identity.clone(), footer);
        self.touch_metadata_identity(&identity, lane);
        self.trim_metadata_to_limit();
        self.stats
            .decoded_footer_admissions
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn key_index(&self, identity: &ColumnarSstableIdentity) -> Option<Arc<Vec<Key>>> {
        let cached = self.key_indexes.read().get(identity).cloned();
        if cached.is_some() {
            self.stats
                .decoded_metadata_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .decoded_metadata_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    pub(super) fn insert_key_index(
        &self,
        identity: ColumnarSstableIdentity,
        values: Arc<Vec<Key>>,
        lane: crate::ExecutionLane,
    ) {
        self.key_indexes.write().insert(identity.clone(), values);
        self.touch_metadata_identity(&identity, lane);
        self.trim_metadata_to_limit();
        self.stats
            .decoded_metadata_admissions
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn sequence_column(
        &self,
        identity: &ColumnarSstableIdentity,
    ) -> Option<Arc<Vec<SequenceNumber>>> {
        let cached = self.sequence_columns.read().get(identity).cloned();
        if cached.is_some() {
            self.stats
                .decoded_metadata_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .decoded_metadata_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    pub(super) fn insert_sequence_column(
        &self,
        identity: ColumnarSstableIdentity,
        values: Arc<Vec<SequenceNumber>>,
        lane: crate::ExecutionLane,
    ) {
        self.sequence_columns
            .write()
            .insert(identity.clone(), values);
        self.touch_metadata_identity(&identity, lane);
        self.trim_metadata_to_limit();
        self.stats
            .decoded_metadata_admissions
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn tombstone_bitmap(
        &self,
        identity: &ColumnarSstableIdentity,
    ) -> Option<Arc<Vec<bool>>> {
        let cached = self.tombstone_bitmaps.read().get(identity).cloned();
        if cached.is_some() {
            self.stats
                .decoded_metadata_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .decoded_metadata_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    pub(super) fn insert_tombstone_bitmap(
        &self,
        identity: ColumnarSstableIdentity,
        values: Arc<Vec<bool>>,
        lane: crate::ExecutionLane,
    ) {
        self.tombstone_bitmaps
            .write()
            .insert(identity.clone(), values);
        self.touch_metadata_identity(&identity, lane);
        self.trim_metadata_to_limit();
        self.stats
            .decoded_metadata_admissions
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn row_kind_column(
        &self,
        identity: &ColumnarSstableIdentity,
    ) -> Option<Arc<Vec<ChangeKind>>> {
        let cached = self.row_kind_columns.read().get(identity).cloned();
        if cached.is_some() {
            self.stats
                .decoded_metadata_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .decoded_metadata_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    pub(super) fn insert_row_kind_column(
        &self,
        identity: ColumnarSstableIdentity,
        values: Arc<Vec<ChangeKind>>,
        lane: crate::ExecutionLane,
    ) {
        self.row_kind_columns
            .write()
            .insert(identity.clone(), values);
        self.touch_metadata_identity(&identity, lane);
        self.trim_metadata_to_limit();
        self.stats
            .decoded_metadata_admissions
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn column_block(
        &self,
        key: &ColumnarColumnCacheKey,
    ) -> Option<Arc<Vec<FieldValue>>> {
        let cached = self.column_blocks.read().get(key).cloned();
        if cached.is_some() {
            self.stats
                .decoded_column_block_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .decoded_column_block_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    pub(super) fn insert_column_block(
        &self,
        key: ColumnarColumnCacheKey,
        values: Arc<Vec<FieldValue>>,
        lane: crate::ExecutionLane,
    ) {
        self.column_blocks.write().insert(key.clone(), values);
        self.touch_column_key(&key, lane);
        self.trim_column_blocks_to_limit();
        self.stats
            .decoded_column_block_admissions
            .fetch_add(1, Ordering::Relaxed);
    }

    fn touch_metadata_identity(
        &self,
        identity: &ColumnarSstableIdentity,
        lane: crate::ExecutionLane,
    ) {
        let mut owners = self.metadata_owners.write();
        let mut order = self.metadata_order.lock();
        if let Some(position) = order.iter().position(|cached| cached == identity) {
            order.remove(position);
        }
        order.push_back(identity.clone());
        owners.insert(identity.clone(), lane);
    }

    fn trim_metadata_to_limit(&self) {
        for (&lane, &limit) in &self.metadata_entry_limits {
            while self.metadata_entries_for_lane(lane) > limit {
                let Some(identity) = self.pop_oldest_metadata_for_lane(lane) else {
                    break;
                };
                self.remove_metadata_identity(&identity);
            }
        }
    }

    fn touch_column_key(&self, key: &ColumnarColumnCacheKey, lane: crate::ExecutionLane) {
        let mut owners = self.column_owners.write();
        let mut order = self.column_order.lock();
        if let Some(position) = order.iter().position(|cached| cached == key) {
            order.remove(position);
        }
        order.push_back(key.clone());
        owners.insert(key.clone(), lane);
    }

    fn trim_column_blocks_to_limit(&self) {
        for (&lane, &limit) in &self.column_entry_limits {
            while self.column_entries_for_lane(lane) > limit {
                let Some(key) = self.pop_oldest_column_for_lane(lane) else {
                    break;
                };
                self.column_blocks.write().remove(&key);
                self.column_owners.write().remove(&key);
            }
        }
    }

    fn metadata_entries_for_lane(&self, lane: crate::ExecutionLane) -> usize {
        self.metadata_owners
            .read()
            .values()
            .filter(|owner| **owner == lane)
            .count()
    }

    fn column_entries_for_lane(&self, lane: crate::ExecutionLane) -> usize {
        self.column_owners
            .read()
            .values()
            .filter(|owner| **owner == lane)
            .count()
    }

    fn pop_oldest_metadata_for_lane(
        &self,
        lane: crate::ExecutionLane,
    ) -> Option<ColumnarSstableIdentity> {
        let owners = self.metadata_owners.read();
        let mut order = self.metadata_order.lock();
        let position = order
            .iter()
            .position(|identity| owners.get(identity).copied() == Some(lane))?;
        order.remove(position)
    }

    fn pop_oldest_column_for_lane(
        &self,
        lane: crate::ExecutionLane,
    ) -> Option<ColumnarColumnCacheKey> {
        let owners = self.column_owners.read();
        let mut order = self.column_order.lock();
        let position = order
            .iter()
            .position(|key| owners.get(key).copied() == Some(lane))?;
        order.remove(position)
    }

    fn remove_metadata_identity(&self, identity: &ColumnarSstableIdentity) {
        self.footers.write().remove(identity);
        self.key_indexes.write().remove(identity);
        self.sequence_columns.write().remove(identity);
        self.tombstone_bitmaps.write().remove(identity);
        self.row_kind_columns.write().remove(identity);
        self.metadata_owners.write().remove(identity);
    }

    fn metadata_usage_bytes(&self) -> u64 {
        let footer_bytes = self
            .footers
            .read()
            .values()
            .map(estimate_cached_footer_bytes)
            .sum::<u64>();
        let key_index_bytes = self
            .key_indexes
            .read()
            .values()
            .map(|values| estimate_keys_bytes(values.as_ref().as_slice()))
            .sum::<u64>();
        let sequence_bytes = self
            .sequence_columns
            .read()
            .values()
            .map(|values| (values.len() as u64).saturating_mul(std::mem::size_of::<u64>() as u64))
            .sum::<u64>();
        let tombstone_bytes = self
            .tombstone_bitmaps
            .read()
            .values()
            .map(|values| values.len() as u64)
            .sum::<u64>();
        let row_kind_bytes = self
            .row_kind_columns
            .read()
            .values()
            .map(|values| values.len() as u64)
            .sum::<u64>();
        footer_bytes
            .saturating_add(key_index_bytes)
            .saturating_add(sequence_bytes)
            .saturating_add(tombstone_bytes)
            .saturating_add(row_kind_bytes)
    }

    fn metadata_usage_bytes_for_lane(
        &self,
        lane: crate::ExecutionLane,
        owners: &BTreeMap<ColumnarSstableIdentity, crate::ExecutionLane>,
    ) -> u64 {
        owners
            .iter()
            .filter(|(_, owner)| **owner == lane)
            .map(|(identity, _)| self.metadata_usage_bytes_for_identity(identity))
            .sum()
    }

    fn metadata_usage_bytes_for_identity(&self, identity: &ColumnarSstableIdentity) -> u64 {
        let footer_bytes = self
            .footers
            .read()
            .get(identity)
            .map(estimate_cached_footer_bytes)
            .unwrap_or_default();
        let key_index_bytes = self
            .key_indexes
            .read()
            .get(identity)
            .map(|values| estimate_keys_bytes(values.as_ref().as_slice()))
            .unwrap_or_default();
        let sequence_bytes = self
            .sequence_columns
            .read()
            .get(identity)
            .map(|values| (values.len() as u64).saturating_mul(std::mem::size_of::<u64>() as u64))
            .unwrap_or_default();
        let tombstone_bytes = self
            .tombstone_bitmaps
            .read()
            .get(identity)
            .map(|values| values.len() as u64)
            .unwrap_or_default();
        let row_kind_bytes = self
            .row_kind_columns
            .read()
            .get(identity)
            .map(|values| values.len() as u64)
            .unwrap_or_default();
        footer_bytes
            .saturating_add(key_index_bytes)
            .saturating_add(sequence_bytes)
            .saturating_add(tombstone_bytes)
            .saturating_add(row_kind_bytes)
    }

    fn column_usage_bytes(&self) -> u64 {
        self.column_blocks
            .read()
            .values()
            .map(|values| estimate_field_values_bytes(values.as_ref().as_slice()))
            .sum()
    }

    fn column_usage_bytes_for_lane(
        &self,
        lane: crate::ExecutionLane,
        owners: &BTreeMap<ColumnarColumnCacheKey, crate::ExecutionLane>,
    ) -> u64 {
        owners
            .iter()
            .filter(|(_, owner)| **owner == lane)
            .map(|(key, _)| {
                self.column_blocks
                    .read()
                    .get(key)
                    .map(|values| estimate_field_values_bytes(values.as_ref().as_slice()))
                    .unwrap_or_default()
            })
            .sum()
    }
}

impl Default for DecodedColumnarCache {
    fn default() -> Self {
        let config = crate::HybridReadConfig::default();
        Self::new(
            BTreeMap::from([(
                crate::ExecutionLane::UserForeground,
                config.decoded_metadata_cache_entries,
            )]),
            BTreeMap::from([(
                crate::ExecutionLane::UserForeground,
                config.decoded_column_cache_entries,
            )]),
        )
    }
}

fn estimate_cached_footer_bytes(footer: &CachedColumnarFooter) -> u64 {
    serde_json::to_vec(footer.footer.as_ref())
        .map(|bytes| bytes.len() as u64)
        .unwrap_or_default()
        .saturating_add(std::mem::size_of::<usize>() as u64)
}

fn estimate_keys_bytes(values: &[Key]) -> u64 {
    values
        .iter()
        .map(|key| key.len() as u64 + std::mem::size_of::<Key>() as u64)
        .sum()
}

fn estimate_field_values_bytes(values: &[FieldValue]) -> u64 {
    values.iter().map(estimate_field_value_bytes).sum()
}

fn estimate_field_value_bytes(value: &FieldValue) -> u64 {
    match value {
        FieldValue::Null => 0,
        FieldValue::String(value) => value.len() as u64 + std::mem::size_of::<String>() as u64,
        FieldValue::Int64(_) => std::mem::size_of::<i64>() as u64,
        FieldValue::Float64(_) => std::mem::size_of::<f64>() as u64,
        FieldValue::Bytes(value) => value.len() as u64 + std::mem::size_of::<Vec<u8>>() as u64,
        FieldValue::Bool(_) => std::mem::size_of::<bool>() as u64,
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct LoadedManifest {
    pub(super) generation: ManifestId,
    pub(super) last_flushed_sequence: SequenceNumber,
    pub(super) live_sstables: Vec<ResidentRowSstable>,
    pub(super) durable_commit_log_segments: Vec<DurableRemoteCommitLogSegment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedManifestBody {
    pub(super) format_version: u32,
    pub(super) generation: ManifestId,
    pub(super) last_flushed_sequence: SequenceNumber,
    pub(super) sstables: Vec<PersistedManifestSstable>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedManifestFile {
    pub(super) body: PersistedManifestBody,
    pub(super) checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct DurableRemoteCommitLogSegment {
    pub(super) object_key: String,
    pub(super) footer: SegmentFooter,
}

#[derive(Clone)]
pub(super) enum ChangeFeedSourcePlan {
    LocalSegment(LocalSegmentScanPlan),
    RemoteSegment(DurableRemoteCommitLogSegment),
    BufferedRecords(Arc<Vec<CommitRecord>>),
}

pub(super) struct ChangeFeedScanner {
    pub(super) cursor: LogCursor,
    pub(super) upper_bound: SequenceNumber,
    pub(super) table_id: TableId,
    pub(super) table: Table,
    pub(super) remaining: Option<usize>,
    pub(super) _scan_guard: CommitLogScanGuard,
    pub(super) sources: BTreeMap<crate::PhysicalShardId, VecDeque<ChangeFeedSourcePlan>>,
    pub(super) active_sources: Vec<LoadedChangeFeedSource>,
}

pub(super) enum ActiveChangeFeedSource {
    Segment(ChangeFeedSegmentSource),
    BufferedRecords(ChangeFeedBufferedRecordsSource),
}

pub(super) struct LoadedChangeFeedSource {
    pub(super) shard: crate::PhysicalShardId,
    pub(super) source: ActiveChangeFeedSource,
    pub(super) next_entry: ChangeEntry,
}

pub(super) struct ChangeFeedSegmentSource {
    pub(super) scanner: SegmentRecordScanner,
    pub(super) pending: VecDeque<ChangeEntry>,
    pub(super) upper_bound_reached: bool,
}

pub(super) struct ChangeFeedBufferedRecordsSource {
    pub(super) records: Arc<Vec<CommitRecord>>,
    pub(super) next_index: usize,
    pub(super) pending: VecDeque<ChangeEntry>,
    pub(super) upper_bound_reached: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedRemoteManifestBody {
    pub(super) format_version: u32,
    pub(super) generation: ManifestId,
    pub(super) last_flushed_sequence: SequenceNumber,
    pub(super) sstables: Vec<PersistedManifestSstable>,
    pub(super) commit_log_segments: Vec<DurableRemoteCommitLogSegment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedRemoteManifestFile {
    pub(super) body: PersistedRemoteManifestBody,
    pub(super) checksum: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct BackupObjectBirthRecord {
    pub(super) format_version: u32,
    pub(super) object_key: String,
    pub(super) first_uploaded_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedRowSstableBody {
    pub(super) format_version: u32,
    pub(super) table_id: TableId,
    pub(super) level: u32,
    pub(super) local_id: String,
    pub(super) min_key: Key,
    pub(super) max_key: Key,
    pub(super) min_sequence: SequenceNumber,
    pub(super) max_sequence: SequenceNumber,
    pub(super) rows: Vec<SstableRow>,
    pub(super) data_checksum: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) user_key_bloom_filter: Option<UserKeyBloomFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) shard_ownership: Option<crate::ShardSstableOwnership>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedRowSstableFile {
    pub(super) body: PersistedRowSstableBody,
    pub(super) checksum: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum ColumnEncoding {
    Plain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum ColumnCompression {
    None,
    Lz4,
    Zstd,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ColumnarBlockLocation {
    pub(super) offset: u64,
    pub(super) length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedColumnarColumnFooter {
    pub(super) field_id: FieldId,
    #[serde(rename = "type")]
    pub(super) field_type: FieldType,
    pub(super) encoding: ColumnEncoding,
    pub(super) compression: ColumnCompression,
    pub(super) block: ColumnarBlockLocation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedSkipIndexSidecarDescriptor {
    pub(super) file_name: String,
    pub(super) index_name: String,
    pub(super) family: HybridSkipIndexFamily,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) field_id: Option<FieldId>,
    pub(super) checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedProjectionSidecarDescriptor {
    pub(super) file_name: String,
    pub(super) projection_name: String,
    pub(super) projected_fields: Vec<FieldId>,
    pub(super) checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum PersistedOptionalSidecarDescriptor {
    SkipIndex(PersistedSkipIndexSidecarDescriptor),
    Projection(PersistedProjectionSidecarDescriptor),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PersistedColumnarSstableFooter {
    pub(super) format_version: u32,
    pub(super) table_id: TableId,
    pub(super) level: u32,
    pub(super) local_id: String,
    pub(super) row_count: u64,
    pub(super) min_key: Key,
    pub(super) max_key: Key,
    pub(super) min_sequence: SequenceNumber,
    pub(super) max_sequence: SequenceNumber,
    pub(super) schema_version: u32,
    pub(super) data_checksum: u32,
    pub(super) key_index: ColumnarBlockLocation,
    pub(super) sequence_column: ColumnarBlockLocation,
    pub(super) tombstone_bitmap: ColumnarBlockLocation,
    pub(super) row_kind_column: ColumnarBlockLocation,
    pub(super) columns: Vec<PersistedColumnarColumnFooter>,
    pub(super) layout: crate::hybrid::ColumnarFooter,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) applied_generation: Option<ManifestId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) digests: Vec<CompactPartDigest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) optional_sidecars: Vec<PersistedOptionalSidecarDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) user_key_bloom_filter: Option<UserKeyBloomFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) shard_ownership: Option<crate::ShardSstableOwnership>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub(super) enum PersistedSkipIndexSidecarPayload {
    UserKeyBloom {
        filter: UserKeyBloomFilter,
    },
    FieldValueBloom {
        filter: UserKeyBloomFilter,
    },
    BoundedSet {
        values: Vec<FieldValue>,
        saturated: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct PersistedSkipIndexSidecarBody {
    pub(super) format_version: u32,
    pub(super) table_id: TableId,
    pub(super) local_id: String,
    pub(super) index_name: String,
    pub(super) family: HybridSkipIndexFamily,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) field_id: Option<FieldId>,
    pub(super) schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) applied_generation: Option<ManifestId>,
    pub(super) row_count: u64,
    pub(super) digest: CompactPartDigest,
    pub(super) payload: PersistedSkipIndexSidecarPayload,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct PersistedSkipIndexSidecarFile {
    pub(super) body: PersistedSkipIndexSidecarBody,
    pub(super) checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct PersistedProjectionSidecarBody {
    pub(super) format_version: u32,
    pub(super) table_id: TableId,
    pub(super) local_id: String,
    pub(super) projection_name: String,
    pub(super) schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) applied_generation: Option<ManifestId>,
    pub(super) row_count: u64,
    pub(super) projected_fields: Vec<FieldId>,
    pub(super) digest: CompactPartDigest,
    pub(super) rows: Vec<Option<Value>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct PersistedProjectionSidecarFile {
    pub(super) body: PersistedProjectionSidecarBody,
    pub(super) checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedArtifactQuarantineMarker {
    pub(super) format_version: u32,
    pub(super) local_id: String,
    pub(super) reason: String,
    pub(super) quarantined_at_millis: u64,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct PersistedNullableColumn<T> {
    pub(super) present_bitmap: Vec<bool>,
    pub(super) values: Vec<T>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct PersistedFloat64Column {
    pub(super) present_bitmap: Vec<bool>,
    pub(super) values_bits: Vec<u64>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub(super) enum PersistedColumnBlock {
    Int64(PersistedNullableColumn<i64>),
    Float64(PersistedFloat64Column),
    String(PersistedNullableColumn<String>),
    Bytes(PersistedNullableColumn<Vec<u8>>),
    Bool(PersistedNullableColumn<bool>),
}
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CompactionJobKind {
    Rewrite,
    DeleteOnly,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
pub(super) struct CompactionJob {
    pub(super) id: String,
    pub(super) table_id: TableId,
    pub(super) table_name: String,
    pub(super) physical_shard: Option<crate::PhysicalShardId>,
    pub(super) source_level: u32,
    pub(super) target_level: u32,
    pub(super) kind: CompactionJobKind,
    pub(super) estimated_bytes: u64,
    pub(super) input_local_ids: Vec<String>,
}

#[derive(Clone, Debug)]
pub(super) struct OffloadJob {
    pub(super) id: String,
    pub(super) table_id: TableId,
    pub(super) table_name: String,
    pub(super) physical_shard: Option<crate::PhysicalShardId>,
    pub(super) kind: OffloadJobKind,
    pub(super) input_local_ids: Vec<String>,
    pub(super) estimated_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OffloadJobKind {
    Offload,
    Delete,
}

#[derive(Clone, Debug, Default)]
pub(super) struct TableCompactionState {
    pub(super) compaction_debt: u64,
    pub(super) next_job: Option<CompactionJob>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CompactionFilterStats {
    pub(super) removed_bytes: u64,
    pub(super) removed_keys: u64,
}

#[derive(Clone, Debug)]
pub(super) enum PendingWorkSpec {
    Flush,
    Compaction(CompactionJob),
    Offload(OffloadJob),
}

#[derive(Clone, Debug)]
pub(super) struct PendingWorkCandidate {
    pub(super) pending: PendingWork,
    pub(super) tag: crate::WorkRuntimeTag,
    pub(super) spec: PendingWorkSpec,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct SstableRow {
    pub(super) key: Key,
    pub(super) sequence: SequenceNumber,
    pub(super) kind: ChangeKind,
    pub(super) value: Option<Value>,
}

#[derive(Clone, Debug)]
pub(super) struct CompactionRow {
    pub(super) level: u32,
    pub(super) row: SstableRow,
}

#[derive(Clone, Debug, Default)]
pub(super) struct FilteredCompactionRows {
    pub(super) rows: Vec<CompactionRow>,
    pub(super) removed_bytes: u64,
    pub(super) removed_keys: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct UserKeyBloomFilter {
    pub(super) bits_per_key: u32,
    pub(super) hash_count: u32,
    pub(super) bytes: Vec<u8>,
}

impl UserKeyBloomFilter {
    pub(super) fn build(rows: &[SstableRow], bits_per_key: Option<u32>) -> Option<Self> {
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

    pub(super) fn may_contain(&self, key: &[u8]) -> bool {
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

    pub(super) fn insert(&mut self, key: &[u8], total_bits: usize) {
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
pub(super) struct ResolvedBatchOperation {
    pub(super) table_id: TableId,
    pub(super) table_name: String,
    pub(super) key: Key,
    pub(super) kind: ChangeKind,
    pub(super) value: Option<Value>,
    pub(super) physical_shard: crate::PhysicalShardId,
    pub(super) virtual_partition: crate::VirtualPartitionId,
    pub(super) shard_map_revision: crate::ShardMapRevision,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct MergeCollapse {
    pub(super) sequence: SequenceNumber,
    pub(super) value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct VisibleValueResolution {
    pub(super) value: Option<Value>,
    pub(super) collapse: Option<MergeCollapse>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct RowScanResult {
    pub(super) rows: Vec<(Key, Value)>,
    pub(super) collapses: Vec<(Key, MergeCollapse)>,
    pub(super) visited_key_groups: usize,
}

#[derive(Clone, Debug)]
pub(super) struct ResolvedReadSetEntry {
    pub(super) table_id: TableId,
    pub(super) key: Key,
    pub(super) at_sequence: SequenceNumber,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CommitConflictKey {
    pub(super) table_id: TableId,
    pub(super) key: Key,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CommitPhase {
    BeforeDurabilitySync,
    AfterBatchSeal,
    AfterDurabilitySync,
    BeforeMemtableInsert,
    AfterMemtableInsert,
    AfterVisibilityPublish,
    AfterDurablePublish,
}

impl CommitPhase {
    pub(super) fn failpoint_name(self) -> &'static str {
        match self {
            Self::BeforeDurabilitySync => {
                crate::failpoints::names::DB_COMMIT_BEFORE_DURABILITY_SYNC
            }
            Self::AfterBatchSeal => crate::failpoints::names::DB_COMMIT_AFTER_BATCH_SEAL,
            Self::AfterDurabilitySync => crate::failpoints::names::DB_COMMIT_AFTER_DURABILITY_SYNC,
            Self::BeforeMemtableInsert => {
                crate::failpoints::names::DB_COMMIT_BEFORE_MEMTABLE_INSERT
            }
            Self::AfterMemtableInsert => crate::failpoints::names::DB_COMMIT_AFTER_MEMTABLE_INSERT,
            Self::AfterVisibilityPublish => {
                crate::failpoints::names::DB_COMMIT_AFTER_VISIBILITY_PUBLISH
            }
            Self::AfterDurablePublish => crate::failpoints::names::DB_COMMIT_AFTER_DURABLE_PUBLISH,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FlushPhase {
    InputsMarkedFlushing,
}

impl FlushPhase {
    pub(super) fn failpoint_name(self) -> &'static str {
        match self {
            Self::InputsMarkedFlushing => crate::failpoints::names::DB_FLUSH_INPUTS_MARKED_FLUSHING,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CompactionPhase {
    OutputWritten,
    ManifestSwitched,
    InputCleanupFinished,
}

impl CompactionPhase {
    pub(super) fn failpoint_name(self) -> &'static str {
        match self {
            Self::OutputWritten => crate::failpoints::names::DB_COMPACTION_OUTPUT_WRITTEN,
            Self::ManifestSwitched => crate::failpoints::names::DB_COMPACTION_MANIFEST_SWITCHED,
            Self::InputCleanupFinished => {
                crate::failpoints::names::DB_COMPACTION_INPUT_CLEANUP_FINISHED
            }
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OffloadPhase {
    UploadComplete,
    ManifestSwitched,
    LocalCleanupFinished,
}

impl OffloadPhase {
    pub(super) fn failpoint_name(self) -> &'static str {
        match self {
            Self::UploadComplete => crate::failpoints::names::DB_OFFLOAD_UPLOAD_COMPLETE,
            Self::ManifestSwitched => crate::failpoints::names::DB_OFFLOAD_MANIFEST_SWITCHED,
            Self::LocalCleanupFinished => {
                crate::failpoints::names::DB_OFFLOAD_LOCAL_CLEANUP_FINISHED
            }
        }
    }
}

#[cfg(test)]
pub(super) struct CommitPhaseBlocker {
    pub(super) handle: crate::failpoints::FailpointHandle,
}

#[cfg(test)]
pub(super) struct FlushPhaseBlocker {
    pub(super) handle: crate::failpoints::FailpointHandle,
}

#[cfg(test)]
pub(super) struct CompactionPhaseBlocker {
    pub(super) handle: crate::failpoints::FailpointHandle,
}

#[cfg(test)]
#[allow(dead_code)]
pub(super) struct OffloadPhaseBlocker {
    pub(super) handle: crate::failpoints::FailpointHandle,
}

#[cfg(test)]
impl CommitPhaseBlocker {
    pub(super) async fn sequence(&mut self) -> SequenceNumber {
        let hit = self.handle.next_hit().await;
        let value = hit
            .metadata
            .get("sequence")
            .expect("commit phase failpoint should report a sequence");
        let value = value
            .parse::<u64>()
            .expect("commit phase failpoint sequence should parse");
        SequenceNumber::new(value)
    }

    pub(super) fn release(&mut self) {
        self.handle.release();
    }
}

#[cfg(test)]
impl FlushPhaseBlocker {
    pub(super) async fn wait_until_reached(&mut self) {
        self.handle.wait_until_hit().await;
    }

    pub(super) fn release(&mut self) {
        self.handle.release();
    }
}

#[cfg(test)]
impl CompactionPhaseBlocker {
    pub(super) async fn wait_until_reached(&mut self) {
        self.handle.wait_until_hit().await;
    }

    pub(super) fn release(&mut self) {
        self.handle.release();
    }
}

#[cfg(test)]
#[allow(dead_code)]
impl OffloadPhaseBlocker {
    pub(super) async fn wait_until_reached(&mut self) {
        self.handle.wait_until_hit().await;
    }

    pub(super) fn release(&mut self) {
        self.handle.release();
    }
}

pub(super) struct CommitRuntime {
    pub(super) backend: CommitLogBackend,
}

#[derive(Clone, Debug, Default)]
pub(super) struct RecoveredCommitLogState {
    pub(super) memtables: MemtableState,
    pub(super) max_sequence: SequenceNumber,
}

pub(super) enum CommitLogBackend {
    Local(ShardLocalSegmentManagers),
    Memory(MemoryCommitLog),
}

pub(super) struct ShardLocalSegmentManagers {
    pub(super) fs: Arc<dyn FileSystem>,
    pub(super) root_dir: String,
    pub(super) options: SegmentOptions,
    pub(super) lanes: BTreeMap<crate::PhysicalShardId, SegmentManager>,
}

#[derive(Clone)]
pub(super) struct MemoryCommitLogLane {
    pub(super) records: Arc<Vec<CommitRecord>>,
    pub(super) durable_commit_log_segments: Vec<DurableRemoteCommitLogSegment>,
    pub(super) next_segment_id: u64,
}

impl MemoryCommitLogLane {
    pub(super) fn new() -> Self {
        Self {
            records: Arc::new(Vec::new()),
            durable_commit_log_segments: Vec::new(),
            next_segment_id: 1,
        }
    }
}

pub(super) struct MemoryCommitLog {
    pub(super) lanes: BTreeMap<crate::PhysicalShardId, MemoryCommitLogLane>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ChangeFeedShardRetentionState {
    pub(super) oldest_sequence: SequenceNumber,
    pub(super) oldest_segment_id: SegmentId,
}

impl MemoryCommitLog {
    pub(super) fn new() -> Self {
        Self {
            lanes: BTreeMap::new(),
        }
    }

    pub(super) fn lane_mut(&mut self, shard: crate::PhysicalShardId) -> &mut MemoryCommitLogLane {
        self.lanes
            .entry(shard)
            .or_insert_with(MemoryCommitLogLane::new)
    }
}

impl ShardLocalSegmentManagers {
    pub(super) async fn open(
        fs: Arc<dyn FileSystem>,
        root_dir: String,
        options: SegmentOptions,
    ) -> Result<Self, StorageError> {
        let mut shards = BTreeSet::new();
        for path in fs.list(&root_dir).await? {
            if Db::parse_segment_id(&path).is_some() {
                let relative = path.strip_prefix(&(root_dir.clone() + "/"));
                match relative.and_then(|value| value.split('/').next()) {
                    Some(shard) if shard.starts_with("SEG-") => {
                        shards.insert(crate::PhysicalShardId::UNSHARDED);
                    }
                    Some(shard) => {
                        if let Ok(shard) = shard.parse::<u32>() {
                            shards.insert(crate::PhysicalShardId::new(shard));
                        }
                    }
                    None => {}
                }
            }
        }
        if shards.is_empty() {
            shards.insert(crate::PhysicalShardId::UNSHARDED);
        }

        let mut lanes = BTreeMap::new();
        for shard in shards {
            let dir = Self::lane_dir_for_root(&root_dir, shard);
            lanes.insert(
                shard,
                SegmentManager::open(fs.clone(), dir, options.clone()).await?,
            );
        }

        Ok(Self {
            fs,
            root_dir,
            options,
            lanes,
        })
    }

    fn lane_dir_for_root(root_dir: &str, shard: crate::PhysicalShardId) -> String {
        if shard == crate::PhysicalShardId::UNSHARDED {
            return root_dir.to_string();
        }

        format!("{root_dir}/{shard}")
    }

    pub(super) fn lane_dir(&self, shard: crate::PhysicalShardId) -> String {
        Self::lane_dir_for_root(&self.root_dir, shard)
    }

    pub(super) async fn lane_mut(
        &mut self,
        shard: crate::PhysicalShardId,
    ) -> Result<&mut SegmentManager, StorageError> {
        if !self.lanes.contains_key(&shard) {
            let dir = self.lane_dir(shard);
            let manager = SegmentManager::open(self.fs.clone(), dir, self.options.clone()).await?;
            self.lanes.insert(shard, manager);
        }

        self.lanes.get_mut(&shard).ok_or_else(|| {
            StorageError::corruption(format!("commit-log shard lane {shard} did not open"))
        })
    }

    pub(super) fn enumerate_segments(
        &self,
    ) -> Vec<(
        crate::PhysicalShardId,
        crate::engine::commit_log::SegmentDescriptor,
    )> {
        let mut descriptors = self
            .lanes
            .iter()
            .flat_map(|(&shard, manager)| {
                manager
                    .enumerate_segments()
                    .into_iter()
                    .map(move |descriptor| (shard, descriptor))
            })
            .collect::<Vec<_>>();
        descriptors.sort_by_key(|(shard, descriptor)| (shard.get(), descriptor.segment_id.get()));
        descriptors
    }

    pub(super) async fn read_segment_bytes(
        &self,
        shard: crate::PhysicalShardId,
        segment_id: SegmentId,
    ) -> Result<Vec<u8>, StorageError> {
        let manager = self.lanes.get(&shard).ok_or_else(|| {
            StorageError::not_found(format!("unknown commit-log shard lane {shard}"))
        })?;
        manager.read_segment_bytes(segment_id).await
    }
}

impl ChangeFeedScanner {
    pub(super) fn new(
        table_id: TableId,
        table: Table,
        cursor: LogCursor,
        upper_bound: SequenceNumber,
        remaining: Option<usize>,
        scan_guard: CommitLogScanGuard,
        sources: BTreeMap<crate::PhysicalShardId, VecDeque<ChangeFeedSourcePlan>>,
    ) -> Self {
        Self {
            cursor,
            upper_bound,
            table_id,
            table,
            remaining,
            _scan_guard: scan_guard,
            sources,
            active_sources: Vec::new(),
        }
    }

    pub(super) async fn next_change(&mut self) -> Result<Option<ChangeEntry>, StorageError> {
        if matches!(self.remaining, Some(0)) {
            return Ok(None);
        }

        self.fill_active_sources().await?;

        let Some((index, _)) = self
            .active_sources
            .iter()
            .enumerate()
            .min_by_key(|(_, source)| source.next_entry.cursor)
        else {
            return Ok(None);
        };

        let mut source = self.active_sources.swap_remove(index);
        let entry = source.next_entry;
        if let Some(next_entry) =
            source
                .source
                .next_change(self.table_id, &self.table, self.cursor, self.upper_bound)?
        {
            source.next_entry = next_entry;
            self.active_sources.push(source);
        }

        if let Some(remaining) = &mut self.remaining {
            *remaining = remaining.saturating_sub(1);
        }
        Ok(Some(entry))
    }

    async fn fill_active_sources(&mut self) -> Result<(), StorageError> {
        let active_shards = self
            .active_sources
            .iter()
            .map(|source| source.shard)
            .collect::<BTreeSet<_>>();
        let shards_to_fill = self
            .sources
            .keys()
            .filter(|shard| !active_shards.contains(shard))
            .copied()
            .collect::<Vec<_>>();

        for shard in shards_to_fill {
            self.load_next_source_for_shard(shard).await?;
        }

        Ok(())
    }

    async fn load_next_source_for_shard(
        &mut self,
        shard: crate::PhysicalShardId,
    ) -> Result<(), StorageError> {
        while let Some(source) = self.pop_source_for_shard(shard) {
            let Some(mut source) = self.load_source(source).await? else {
                continue;
            };
            let Some(next_entry) =
                source.next_change(self.table_id, &self.table, self.cursor, self.upper_bound)?
            else {
                continue;
            };
            self.active_sources.push(LoadedChangeFeedSource {
                shard,
                source,
                next_entry,
            });
            break;
        }

        Ok(())
    }

    fn pop_source_for_shard(
        &mut self,
        shard: crate::PhysicalShardId,
    ) -> Option<ChangeFeedSourcePlan> {
        let (source, remove_shard) = {
            let sources = self.sources.get_mut(&shard)?;
            let source = sources.pop_front();
            (source, sources.is_empty())
        };
        if remove_shard {
            self.sources.remove(&shard);
        }
        source
    }

    pub(super) async fn load_source(
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

                let bytes = read_change_feed_file(&plan).await?;
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
    pub(super) fn next_change(
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
}

impl ChangeFeedSegmentSource {
    pub(super) fn next_change(
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
    pub(super) fn new(records: Arc<Vec<CommitRecord>>, cursor_sequence: SequenceNumber) -> Self {
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

    pub(super) fn next_change(
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

pub(super) fn change_entries_for_record(
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
                operation_context: entry.operation_context,
            })
        })
        .collect()
}

pub(super) fn pinned_segment_ids(
    sources: &BTreeMap<crate::PhysicalShardId, VecDeque<ChangeFeedSourcePlan>>,
) -> Vec<SegmentId> {
    sources
        .iter()
        .flat_map(|(_, sources)| sources.iter())
        .filter_map(|source| match source {
            ChangeFeedSourcePlan::LocalSegment(plan) => Some(plan.segment_id),
            ChangeFeedSourcePlan::RemoteSegment(_) | ChangeFeedSourcePlan::BufferedRecords(_) => {
                None
            }
        })
        .collect()
}

pub(super) async fn read_change_feed_file(
    plan: &LocalSegmentScanPlan,
) -> Result<Vec<u8>, StorageError> {
    let handle = FileHandle::new(&plan.path);
    let mut bytes = Vec::new();
    let mut offset = 0_u64;
    loop {
        let Some(read_len) = plan.read_len else {
            let chunk = plan
                .fs
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
            continue;
        };

        if offset >= read_len {
            break;
        }
        let chunk = plan
            .fs
            .read_at(
                &handle,
                offset,
                CHANGE_FEED_READ_CHUNK_BYTES.min(read_len.saturating_sub(offset) as usize),
            )
            .await?;
        if chunk.is_empty() {
            return Err(StorageError::corruption(format!(
                "segment prefix read ended early for {}: expected {read_len} bytes, reached {offset}",
                plan.path
            )));
        }
        offset = offset.saturating_add(chunk.len() as u64);
        bytes.extend_from_slice(&chunk);
        if chunk.len() < CHANGE_FEED_READ_CHUNK_BYTES || offset >= read_len {
            break;
        }
    }
    Ok(bytes)
}

impl CommitRuntime {
    pub(super) async fn append(&mut self, record: CommitRecord) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(managers) => {
                managers
                    .lane_mut(record.id.physical_shard_hint())
                    .await?
                    .append(record)
                    .await?;
            }
            CommitLogBackend::Memory(log) => {
                Arc::make_mut(&mut log.lane_mut(record.id.physical_shard_hint()).records)
                    .push(record);
            }
        }

        Ok(())
    }

    pub(super) async fn append_group_batch(
        &mut self,
        records: &[CommitRecord],
    ) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(managers) => {
                let mut by_shard = BTreeMap::<crate::PhysicalShardId, Vec<CommitRecord>>::new();
                for record in records {
                    by_shard
                        .entry(record.id.physical_shard_hint())
                        .or_default()
                        .push(record.clone());
                }
                for (shard, records) in by_shard {
                    managers
                        .lane_mut(shard)
                        .await?
                        .append_batch_and_sync(&records)
                        .await?;
                }
                Ok(())
            }
            CommitLogBackend::Memory(log) => {
                for record in records {
                    Arc::make_mut(&mut log.lane_mut(record.id.physical_shard_hint()).records)
                        .push(record.clone());
                }
                Ok(())
            }
        }
    }

    pub(super) async fn sync(&mut self) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(managers) => {
                for manager in managers.lanes.values_mut() {
                    manager.sync_active().await?;
                }
                Ok(())
            }
            CommitLogBackend::Memory(_) => Ok(()),
        }
    }

    pub(super) async fn maybe_seal_active(&mut self) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(managers) => {
                for manager in managers.lanes.values_mut() {
                    manager.seal_active().await?;
                }
                Ok(())
            }
            CommitLogBackend::Memory(_) => Ok(()),
        }
    }

    pub(super) async fn prune_segments_before(
        &mut self,
        sequence_exclusive: SequenceNumber,
        protected_segments: &BTreeSet<SegmentId>,
    ) -> Result<(), StorageError> {
        match &mut self.backend {
            CommitLogBackend::Local(managers) => {
                for manager in managers.lanes.values_mut() {
                    manager
                        .prune_sealed_before(sequence_exclusive, protected_segments)
                        .await?;
                }
                Ok(())
            }
            CommitLogBackend::Memory(_) => Ok(()),
        }
    }

    pub(super) async fn recover_after(
        &self,
        after_sequence: SequenceNumber,
        tables: &BTreeMap<String, StoredTable>,
        recovered_at: Timestamp,
    ) -> Result<RecoveredCommitLogState, StorageError> {
        let records = match &self.backend {
            CommitLogBackend::Local(managers) => {
                let mut records = Vec::new();
                for manager in managers.lanes.values() {
                    records.extend(manager.scan_from_sequence(after_sequence).await?);
                }
                records.sort_by_key(|record| record.id);
                records
            }
            CommitLogBackend::Memory(_) => Vec::new(),
        };

        let mut recovered = RecoveredCommitLogState {
            max_sequence: after_sequence,
            ..RecoveredCommitLogState::default()
        };
        for record in records {
            recovered.max_sequence = recovered.max_sequence.max(record.sequence());
            recovered
                .memtables
                .apply_recovered_record(&record, tables, recovered_at)?;
        }

        Ok(recovered)
    }

    pub(super) fn change_feed_scan_plan(
        &self,
        table_id: TableId,
        sequence_inclusive: SequenceNumber,
    ) -> BTreeMap<crate::PhysicalShardId, VecDeque<ChangeFeedSourcePlan>> {
        match &self.backend {
            CommitLogBackend::Local(managers) => managers
                .lanes
                .iter()
                .filter_map(|(&shard, manager)| {
                    let sources = manager
                        .table_scan_plans_since(table_id, sequence_inclusive)
                        .into_iter()
                        .map(ChangeFeedSourcePlan::LocalSegment)
                        .collect::<VecDeque<_>>();
                    (!sources.is_empty()).then_some((shard, sources))
                })
                .collect(),
            CommitLogBackend::Memory(log) => {
                let mut sources = BTreeMap::new();
                for (&shard, lane) in &log.lanes {
                    let mut lane_sources = VecDeque::new();
                    lane_sources.extend(
                        lane.durable_commit_log_segments
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
                            .map(ChangeFeedSourcePlan::RemoteSegment),
                    );
                    if lane.records.iter().any(|record| {
                        record.sequence() >= sequence_inclusive
                            && record
                                .entries
                                .iter()
                                .any(|entry| entry.table_id == table_id)
                    }) {
                        lane_sources
                            .push_back(ChangeFeedSourcePlan::BufferedRecords(lane.records.clone()));
                    }
                    if !lane_sources.is_empty() {
                        sources.insert(shard, lane_sources);
                    }
                }
                sources
            }
        }
    }

    pub(super) fn oldest_sequence_for_table(&self, table_id: TableId) -> Option<SequenceNumber> {
        match &self.backend {
            CommitLogBackend::Local(managers) => managers
                .lanes
                .values()
                .filter_map(|manager| manager.oldest_sequence_for_table(table_id))
                .min(),
            CommitLogBackend::Memory(log) => log
                .lanes
                .values()
                .filter_map(|lane| {
                    let durable_oldest = lane
                        .durable_commit_log_segments
                        .iter()
                        .flat_map(|segment| segment.footer.tables.iter())
                        .filter(|table| table.table_id == table_id)
                        .map(|table| table.min_sequence)
                        .min();
                    let buffered_oldest = lane
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
                })
                .min(),
        }
    }

    pub(super) fn shard_retention_states_for_table(
        &self,
        table_id: TableId,
    ) -> BTreeMap<crate::PhysicalShardId, ChangeFeedShardRetentionState> {
        match &self.backend {
            CommitLogBackend::Local(managers) => managers
                .lanes
                .iter()
                .filter_map(|(&shard, manager)| {
                    Some((
                        shard,
                        ChangeFeedShardRetentionState {
                            oldest_sequence: manager.oldest_sequence_for_table(table_id)?,
                            oldest_segment_id: manager.oldest_segment_id_for_table(table_id)?,
                        },
                    ))
                })
                .collect(),
            CommitLogBackend::Memory(log) => log
                .lanes
                .iter()
                .filter_map(|(&shard, lane)| {
                    let durable_state = lane
                        .durable_commit_log_segments
                        .iter()
                        .filter_map(|segment| {
                            segment
                                .footer
                                .tables
                                .iter()
                                .find(|table| table.table_id == table_id)
                                .map(|table| ChangeFeedShardRetentionState {
                                    oldest_sequence: table.min_sequence,
                                    oldest_segment_id: segment.footer.segment_id,
                                })
                        })
                        .min_by_key(|state| {
                            (state.oldest_segment_id.get(), state.oldest_sequence.get())
                        });
                    let buffered_state = lane
                        .records
                        .iter()
                        .filter(|record| {
                            record
                                .entries
                                .iter()
                                .any(|entry| entry.table_id == table_id)
                        })
                        .map(CommitRecord::sequence)
                        .min()
                        .map(|oldest_sequence| ChangeFeedShardRetentionState {
                            oldest_sequence,
                            oldest_segment_id: SegmentId::new(lane.next_segment_id),
                        });

                    durable_state
                        .into_iter()
                        .chain(buffered_state)
                        .min_by_key(|state| {
                            (state.oldest_segment_id.get(), state.oldest_sequence.get())
                        })
                        .map(|state| (shard, state))
                })
                .collect(),
        }
    }

    #[cfg(test)]
    pub(super) fn enumerate_segments(&self) -> Vec<crate::engine::commit_log::SegmentDescriptor> {
        match &self.backend {
            CommitLogBackend::Local(managers) => managers
                .enumerate_segments()
                .into_iter()
                .map(|(_, descriptor)| descriptor)
                .collect(),
            CommitLogBackend::Memory(log) => {
                let mut descriptors = Vec::new();
                for lane in log.lanes.values() {
                    descriptors.extend(lane.durable_commit_log_segments.iter().map(|segment| {
                        crate::engine::commit_log::SegmentDescriptor::from(&segment.footer)
                    }));
                    if !lane.records.is_empty() {
                        let mut tables =
                            BTreeMap::<TableId, (SequenceNumber, SequenceNumber, u32)>::new();
                        for record in lane.records.iter() {
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
                            segment_id: SegmentId::new(lane.next_segment_id),
                            sealed: false,
                            min_sequence: lane.records.first().map(CommitRecord::sequence),
                            max_sequence: lane.records.last().map(CommitRecord::sequence),
                            record_count: lane.records.len() as u64,
                            entry_count: lane
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
                }
                descriptors
            }
        }
    }
}

#[derive(Default)]
pub(super) struct CommitCoordinator {
    pub(super) keys: BTreeMap<CommitConflictKey, BTreeSet<SequenceNumber>>,
    pub(super) sequences: BTreeMap<SequenceNumber, SequenceCommitState>,
    pub(super) current_batch: Option<Arc<GroupCommitBatch>>,
    pub(super) pending_failed_tail: Option<PendingFailedTail>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct SequenceCommitState {
    pub(super) touched_tables: BTreeSet<String>,
    pub(super) conflict_keys: Vec<CommitConflictKey>,
    pub(super) group_batch: Option<Arc<GroupCommitBatch>>,
    pub(super) memtable_inserted: bool,
    pub(super) durable_confirmed: bool,
    pub(super) visible_published: bool,
    pub(super) durable_published: bool,
    pub(super) aborted: bool,
}

#[derive(Clone, Debug)]
pub(super) struct PendingFailedTail {
    pub(super) start_sequence: SequenceNumber,
    pub(super) error: StorageError,
}

#[derive(Debug)]
pub(super) struct GroupCommitBatch {
    pub(super) first_sequence: SequenceNumber,
    pub(super) predecessor: Option<Arc<GroupCommitBatch>>,
    pub(super) state: Mutex<GroupCommitBatchState>,
    pub(super) notify: Notify,
}

#[derive(Clone, Debug, Default)]
pub(super) struct GroupCommitBatchState {
    pub(super) sealed: bool,
    pub(super) records: Vec<CommitRecord>,
    pub(super) result: Option<Result<(), StorageError>>,
}

impl GroupCommitBatch {
    pub(super) fn new(
        first_sequence: SequenceNumber,
        predecessor: Option<Arc<GroupCommitBatch>>,
    ) -> Self {
        Self {
            first_sequence,
            predecessor,
            state: Mutex::new(GroupCommitBatchState::default()),
            notify: Notify::new(),
        }
    }

    pub(super) fn first_sequence(&self) -> SequenceNumber {
        self.first_sequence
    }

    pub(super) fn is_open(&self) -> bool {
        !mutex_lock(&self.state).sealed
    }

    pub(super) fn result(&self) -> Option<Result<(), StorageError>> {
        mutex_lock(&self.state).result.clone()
    }

    pub(super) fn stage_record(&self, record: CommitRecord) {
        mutex_lock(&self.state).records.push(record);
    }

    pub(super) fn staged_records(&self) -> Vec<CommitRecord> {
        mutex_lock(&self.state).records.clone()
    }

    pub(super) fn predecessor(&self) -> Option<Arc<GroupCommitBatch>> {
        self.predecessor.clone()
    }

    pub(super) fn finish(&self, result: Result<(), StorageError>) {
        let mut state = mutex_lock(&self.state);
        if state.result.is_none() {
            state.result = Some(result);
            drop(state);
            self.notify.notify_waiters();
        }
    }

    pub(super) async fn await_result(&self) -> Result<(), StorageError> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self.result() {
                return result;
            }
            notified.await;
        }
    }
}

pub(super) struct CommitParticipant {
    pub(super) sequence: SequenceNumber,
    pub(super) operations: Vec<ResolvedBatchOperation>,
    pub(super) group_batch: Option<Arc<GroupCommitBatch>>,
}

#[derive(Default)]
pub(super) struct CommitLogScanRegistry {
    pub(super) pinned_segments: BTreeMap<SegmentId, u64>,
}

impl CommitLogScanRegistry {
    pub(super) fn register(&mut self, segment_ids: &[SegmentId]) {
        for &segment_id in segment_ids {
            *self.pinned_segments.entry(segment_id).or_default() += 1;
        }
    }

    pub(super) fn release(&mut self, segment_ids: &[SegmentId]) {
        for &segment_id in segment_ids {
            let std::collections::btree_map::Entry::Occupied(mut entry) =
                self.pinned_segments.entry(segment_id)
            else {
                continue;
            };
            let current = entry.get_mut();
            if *current <= 1 {
                entry.remove();
            } else {
                *current -= 1;
            }
        }
    }

    pub(super) fn pinned_segments(&self) -> BTreeSet<SegmentId> {
        self.pinned_segments.keys().copied().collect()
    }
}

pub(super) struct CommitLogScanGuard {
    pub(super) db: Weak<DbInner>,
    pub(super) segment_ids: Vec<SegmentId>,
}

impl Drop for CommitLogScanGuard {
    fn drop(&mut self) {
        let Some(db) = self.db.upgrade() else {
            return;
        };
        mutex_lock(&db.commit_log_scans).release(&self.segment_ids);
    }
}
