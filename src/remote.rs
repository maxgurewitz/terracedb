use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::Range,
    path::PathBuf,
    sync::Arc,
};

use arc_swap::ArcSwap;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{Notify, watch},
    task,
};

use crate::{
    config::S3Location,
    error::{StorageError, StorageErrorKind},
    ids::{ManifestId, SegmentId, SequenceNumber, TableId},
    io::{DbDependencies, FileHandle, FileSystem, ObjectStore, OpenOptions},
    metadata_flatbuffers as metadata_fb,
};

const LOCAL_READ_CHUNK_BYTES: usize = 64 * 1024;
const CACHE_FORMAT_VERSION: u32 = 1;
const FB_CACHE_SPAN_FULL: u8 = 1;
const FB_CACHE_SPAN_RANGE: u8 = 2;
const DEFAULT_REMOTE_CACHE_SEGMENT_BYTES: u64 = 128;
const DEFAULT_REMOTE_CACHE_COALESCE_GAP_BYTES: u64 = 128;
pub(crate) const DEFAULT_REMOTE_CACHE_PREFETCH_BYTES: u64 = 128;
const DEFAULT_REMOTE_CACHE_MAX_BYTES: u64 = 16 * 1024 * 1024;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}

fn join_fs_path(root: &str, relative: &str) -> String {
    PathBuf::from(root)
        .join(relative)
        .to_string_lossy()
        .into_owned()
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

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn emit_remote_error(error: &RemoteStorageError) {
    let operation = format!("{:?}", error.operation());
    let kind = format!("{:?}", error.kind());
    let recovery_hint = format!("{:?}", error.recovery_hint());
    if error.is_retryable() {
        tracing::warn!(
            terracedb_remote_operation = %operation,
            terracedb_remote_target = %error.target(),
            terracedb_remote_kind = %kind,
            terracedb_remote_recovery = %recovery_hint,
            "terracedb remote operation failed"
        );
    } else {
        tracing::error!(
            terracedb_remote_operation = %operation,
            terracedb_remote_target = %error.target(),
            terracedb_remote_kind = %kind,
            terracedb_remote_recovery = %recovery_hint,
            "terracedb remote operation failed"
        );
    }
}

async fn open_local_file(
    file_system: &dyn FileSystem,
    path: &str,
) -> Result<FileHandle, StorageError> {
    file_system
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
}

async fn read_local_file_all(
    file_system: &dyn FileSystem,
    path: &str,
) -> Result<Vec<u8>, StorageError> {
    let handle = open_local_file(file_system, path).await?;
    let mut bytes = Vec::new();
    let mut offset = 0_u64;

    loop {
        let chunk = file_system
            .read_at(&handle, offset, LOCAL_READ_CHUNK_BYTES)
            .await?;
        if chunk.is_empty() {
            break;
        }

        offset += chunk.len() as u64;
        bytes.extend_from_slice(&chunk);
        if chunk.len() < LOCAL_READ_CHUNK_BYTES {
            break;
        }
    }

    Ok(bytes)
}

async fn read_local_file_range(
    file_system: &dyn FileSystem,
    path: &str,
    range: Range<u64>,
) -> Result<Vec<u8>, StorageError> {
    if range.start >= range.end {
        return Ok(Vec::new());
    }

    let handle = open_local_file(file_system, path).await?;
    file_system
        .read_at(
            &handle,
            range.start,
            (range.end.saturating_sub(range.start)) as usize,
        )
        .await
}

async fn write_bytes_atomic(
    file_system: &dyn FileSystem,
    path: &str,
    bytes: &[u8],
) -> Result<(), StorageError> {
    let temp_path = format!("{path}.tmp");
    let handle = file_system
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
    file_system.write_at(&handle, 0, bytes).await?;
    file_system.sync(&handle).await?;
    file_system.rename(&temp_path, path).await?;
    if let Some(parent) = PathBuf::from(path).parent()
        && !parent.as_os_str().is_empty()
    {
        file_system
            .sync_dir(parent.to_string_lossy().as_ref())
            .await?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectKeyLayout {
    prefix: String,
}

impl ObjectKeyLayout {
    pub fn new(location: &S3Location) -> Self {
        Self {
            prefix: location.prefix.trim_matches('/').to_string(),
        }
    }

    fn key(&self, relative: &str) -> String {
        join_object_key(&self.prefix, relative)
    }

    pub fn backup_manifest_prefix(&self) -> String {
        self.key("backup/manifest/")
    }

    pub fn backup_prefix(&self) -> String {
        self.key("backup/")
    }

    pub fn control_prefix(&self) -> String {
        self.key("control/")
    }

    pub fn control_catalog(&self) -> String {
        self.key("control/catalog/CATALOG.json")
    }

    pub fn control_manifest_prefix(&self) -> String {
        self.key("control/manifest/")
    }

    pub fn control_manifest(&self, generation: ManifestId) -> String {
        self.key(&format!(
            "control/manifest/MANIFEST-{:06}",
            generation.get()
        ))
    }

    pub fn control_manifest_latest(&self) -> String {
        self.key("control/manifest/latest")
    }

    pub fn backup_catalog(&self) -> String {
        self.key("backup/catalog/CATALOG.json")
    }

    pub fn backup_manifest(&self, generation: ManifestId) -> String {
        self.key(&format!("backup/manifest/MANIFEST-{:06}", generation.get()))
    }

    pub fn backup_manifest_latest(&self) -> String {
        self.key("backup/manifest/latest")
    }

    pub fn backup_commit_log_prefix(&self) -> String {
        self.key("backup/commitlog/")
    }

    pub fn backup_commit_log_segment(&self, segment_id: SegmentId) -> String {
        self.key(&format!("backup/commitlog/SEG-{:06}", segment_id.get()))
    }

    pub fn backup_sstable(&self, table_id: TableId, shard: u32, local_id: &str) -> String {
        self.key(&format!(
            "backup/sst/table-{:06}/{shard:04}/{local_id}.sst",
            table_id.get()
        ))
    }

    pub fn backup_sstable_prefix(&self) -> String {
        self.key("backup/sst/")
    }

    pub fn backup_gc_metadata_prefix(&self) -> String {
        self.key("backup/gc/objects/")
    }

    pub fn backup_gc_metadata(&self, object_key: &str) -> String {
        self.key(&format!(
            "backup/gc/objects/{}.json",
            hex_encode(object_key.as_bytes())
        ))
    }

    pub fn cold_sstable(
        &self,
        table_id: TableId,
        shard: u32,
        min_sequence: SequenceNumber,
        max_sequence: SequenceNumber,
        local_id: &str,
    ) -> String {
        self.key(&format!(
            "cold/table-{:06}/{shard:04}/{:020}-{:020}/{local_id}.sst",
            table_id.get(),
            min_sequence.get(),
            max_sequence.get()
        ))
    }

    pub fn cold_prefix(&self) -> String {
        self.key("cold/")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CacheSpan {
    Full,
    Range { start: u64, end: u64 },
}

impl CacheSpan {
    fn contains(self, range: Range<u64>) -> bool {
        match self {
            Self::Full => true,
            Self::Range { start, end } => range.start >= start && range.end <= end,
        }
    }

    fn suffix(self) -> String {
        match self {
            Self::Full => "full".to_string(),
            Self::Range { start, end } => format!("{start:020}-{end:020}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteCacheEntrySnapshot {
    pub object_key: String,
    pub span: CacheSpan,
    pub data_len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteCacheFetchKind {
    Demand,
    Prefetch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteCacheInFlightSnapshot {
    pub object_key: String,
    pub span: CacheSpan,
    pub fetch_kind: RemoteCacheFetchKind,
    pub waiter_count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoteCacheProgressSnapshot {
    pub entry_count: usize,
    pub total_cached_bytes: u64,
    pub entries: Vec<RemoteCacheEntrySnapshot>,
    pub in_flight: Vec<RemoteCacheInFlightSnapshot>,
}

#[derive(Debug)]
pub struct RemoteCacheProgressSubscription {
    inner: watch::Receiver<Arc<RemoteCacheProgressSnapshot>>,
}

impl RemoteCacheProgressSubscription {
    fn new(inner: watch::Receiver<Arc<RemoteCacheProgressSnapshot>>) -> Self {
        Self { inner }
    }

    pub fn current(&self) -> RemoteCacheProgressSnapshot {
        self.inner.borrow().as_ref().clone()
    }

    pub async fn changed(
        &mut self,
    ) -> Result<RemoteCacheProgressSnapshot, crate::SubscriptionClosed> {
        self.inner
            .changed()
            .await
            .map_err(|_| crate::SubscriptionClosed)?;
        Ok(self.current())
    }

    pub async fn wait_for<F>(
        &mut self,
        mut predicate: F,
    ) -> Result<RemoteCacheProgressSnapshot, crate::SubscriptionClosed>
    where
        F: FnMut(&RemoteCacheProgressSnapshot) -> bool,
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

impl Clone for RemoteCacheProgressSubscription {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CacheEntryRecord {
    object_key: String,
    span: CacheSpan,
    data_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CacheEntryFile {
    format_version: u32,
    record: CacheEntryRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CacheIndexEntry {
    record: CacheEntryRecord,
    metadata_path: String,
    data_path: String,
    last_access_tick: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CoalescedReadRange {
    fetch: Range<u64>,
    members: Vec<(usize, Range<u64>)>,
}

#[derive(Clone, Debug)]
struct InFlightSegment {
    notify: Arc<Notify>,
    fetch_kind: RemoteCacheFetchKind,
    waiter_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RemoteCacheConfig {
    pub(crate) max_bytes: u64,
    pub(crate) segment_bytes: u64,
    pub(crate) coalesce_gap_bytes: u64,
    pub(crate) prefetch_bytes: u64,
}

impl Default for RemoteCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_REMOTE_CACHE_MAX_BYTES,
            segment_bytes: DEFAULT_REMOTE_CACHE_SEGMENT_BYTES,
            coalesce_gap_bytes: DEFAULT_REMOTE_CACHE_COALESCE_GAP_BYTES,
            prefetch_bytes: DEFAULT_REMOTE_CACHE_PREFETCH_BYTES,
        }
    }
}

impl RemoteCacheConfig {
    fn normalized(self) -> Self {
        let segment_bytes = self.segment_bytes.max(1);
        let coalesce_gap_bytes = self.coalesce_gap_bytes.min(segment_bytes);
        let prefetch_bytes = self.prefetch_bytes - (self.prefetch_bytes % segment_bytes);
        let max_bytes = self.max_bytes.max(segment_bytes);
        Self {
            max_bytes,
            segment_bytes,
            coalesce_gap_bytes,
            prefetch_bytes,
        }
    }
}

#[derive(Default)]
struct RemoteCacheState {
    entries: BTreeMap<String, Vec<CacheIndexEntry>>,
    in_flight: BTreeMap<(String, CacheSpan), InFlightSegment>,
    total_bytes: u64,
    next_access_tick: u64,
}

fn encode_cache_span_flatbuffer(span: CacheSpan) -> (u8, u64, u64) {
    match span {
        CacheSpan::Full => (FB_CACHE_SPAN_FULL, 0, 0),
        CacheSpan::Range { start, end } => (FB_CACHE_SPAN_RANGE, start, end),
    }
}

fn decode_cache_span_flatbuffer(kind: u8, start: u64, end: u64) -> Result<CacheSpan, StorageError> {
    match kind {
        FB_CACHE_SPAN_FULL => Ok(CacheSpan::Full),
        FB_CACHE_SPAN_RANGE => Ok(CacheSpan::Range { start, end }),
        _ => Err(StorageError::corruption(format!(
            "unknown remote cache span tag {kind}"
        ))),
    }
}

fn encode_cache_entry_file_flatbuffer(file: &CacheEntryFile) -> Result<Vec<u8>, StorageError> {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let object_key = fbb.create_string(&file.record.object_key);
    let (span_kind, start, end) = encode_cache_span_flatbuffer(file.record.span);
    let root = metadata_fb::create_remote_cache_entry(
        &mut fbb,
        file.format_version,
        object_key,
        span_kind,
        start,
        end,
        file.record.data_len,
    );
    fbb.finish(root, Some(metadata_fb::REMOTE_CACHE_IDENTIFIER));
    Ok(fbb.finished_data().to_vec())
}

fn decode_cache_entry_file_flatbuffer(bytes: &[u8]) -> Result<CacheEntryFile, StorageError> {
    let file = metadata_fb::root_with_identifier::<metadata_fb::RemoteCacheEntry<'_>>(
        bytes,
        metadata_fb::REMOTE_CACHE_IDENTIFIER,
        "remote cache metadata",
    )
    .map_err(|error| {
        StorageError::corruption(format!("decode remote cache metadata failed: {error}"))
    })?;

    Ok(CacheEntryFile {
        format_version: file.format_version(),
        record: CacheEntryRecord {
            object_key: file.object_key().to_string(),
            span: decode_cache_span_flatbuffer(file.span_kind(), file.start(), file.end())?,
            data_len: file.data_len(),
        },
    })
}

pub struct RemoteCache {
    file_system: Arc<dyn FileSystem>,
    root: String,
    data_dir: String,
    metadata_dir: String,
    config: RemoteCacheConfig,
    state: Mutex<RemoteCacheState>,
    latest_progress_snapshot: ArcSwap<RemoteCacheProgressSnapshot>,
    published_progress_snapshot: watch::Sender<Arc<RemoteCacheProgressSnapshot>>,
}

impl fmt::Debug for RemoteCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteCache")
            .field("root", &self.root)
            .field("data_dir", &self.data_dir)
            .field("metadata_dir", &self.metadata_dir)
            .field("config", &self.config)
            .field("entry_count", &self.entry_count())
            .finish()
    }
}

impl RemoteCache {
    fn build_progress_snapshot_locked(state: &RemoteCacheState) -> RemoteCacheProgressSnapshot {
        let mut entries = state
            .entries
            .values()
            .flatten()
            .map(|entry| RemoteCacheEntrySnapshot {
                object_key: entry.record.object_key.clone(),
                span: entry.record.span,
                data_len: entry.record.data_len,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.object_key
                .cmp(&right.object_key)
                .then_with(|| left.span.suffix().cmp(&right.span.suffix()))
        });
        let mut in_flight = state
            .in_flight
            .iter()
            .map(
                |((object_key, span), segment)| RemoteCacheInFlightSnapshot {
                    object_key: object_key.clone(),
                    span: *span,
                    fetch_kind: segment.fetch_kind,
                    waiter_count: segment.waiter_count,
                },
            )
            .collect::<Vec<_>>();
        in_flight.sort_by(|left, right| {
            left.object_key
                .cmp(&right.object_key)
                .then_with(|| left.span.suffix().cmp(&right.span.suffix()))
        });
        RemoteCacheProgressSnapshot {
            entry_count: entries.len(),
            total_cached_bytes: state.total_bytes,
            entries,
            in_flight,
        }
    }

    fn publish_progress_locked(&self, state: &RemoteCacheState) {
        let snapshot = Arc::new(Self::build_progress_snapshot_locked(state));
        self.latest_progress_snapshot.store(snapshot.clone());
        self.published_progress_snapshot.send_replace(snapshot);
    }

    pub async fn open(
        file_system: Arc<dyn FileSystem>,
        root: impl Into<String>,
    ) -> Result<Self, StorageError> {
        Self::open_with_config(file_system, root, RemoteCacheConfig::default()).await
    }

    pub(crate) async fn open_with_config(
        file_system: Arc<dyn FileSystem>,
        root: impl Into<String>,
        config: RemoteCacheConfig,
    ) -> Result<Self, StorageError> {
        let root = root.into();
        let data_dir = join_fs_path(&root, "data");
        let metadata_dir = join_fs_path(&root, "meta");
        file_system.sync_dir(&data_dir).await?;
        file_system.sync_dir(&metadata_dir).await?;

        let cache = Self {
            file_system,
            root,
            data_dir,
            metadata_dir,
            config: config.normalized(),
            state: Mutex::new(RemoteCacheState::default()),
            latest_progress_snapshot: ArcSwap::from_pointee(RemoteCacheProgressSnapshot::default()),
            published_progress_snapshot: {
                let (sender, _receiver) =
                    watch::channel(Arc::new(RemoteCacheProgressSnapshot::default()));
                sender
            },
        };
        cache.rebuild_index().await?;
        Ok(cache)
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn entry_count(&self) -> usize {
        self.progress_snapshot().entry_count
    }

    pub(crate) fn total_cached_bytes(&self) -> u64 {
        self.progress_snapshot().total_cached_bytes
    }

    pub fn progress_snapshot(&self) -> RemoteCacheProgressSnapshot {
        self.latest_progress_snapshot.load_full().as_ref().clone()
    }

    pub fn subscribe_progress(&self) -> RemoteCacheProgressSubscription {
        RemoteCacheProgressSubscription::new(self.published_progress_snapshot.subscribe())
    }

    async fn rebuild_index(&self) -> Result<(), StorageError> {
        let mut listed = self.file_system.list(&self.metadata_dir).await?;
        listed.sort();
        let mut rebuilt = BTreeMap::new();
        let mut known_data_paths = BTreeSet::new();
        let mut total_bytes = 0_u64;
        let mut next_access_tick = 0_u64;

        for metadata_path in listed.into_iter().filter(|path| path.ends_with(".json")) {
            let Some(entry) = self.load_index_entry(&metadata_path).await? else {
                let _ = self.file_system.delete(&metadata_path).await;
                continue;
            };
            known_data_paths.insert(entry.data_path.clone());
            total_bytes = total_bytes.saturating_add(entry.record.data_len);
            rebuilt
                .entry(entry.record.object_key.clone())
                .or_insert_with(Vec::new)
                .push(CacheIndexEntry {
                    last_access_tick: next_access_tick,
                    ..entry
                });
            next_access_tick = next_access_tick.saturating_add(1);
        }

        let mut data_paths = self.file_system.list(&self.data_dir).await?;
        data_paths.sort();
        for data_path in data_paths {
            if (data_path.ends_with(".bin") || data_path.ends_with(".tmp"))
                && !known_data_paths.contains(&data_path)
            {
                let _ = self.file_system.delete(&data_path).await;
            }
        }

        let evicted = {
            let mut state = lock(&self.state);
            state.entries = rebuilt;
            state.in_flight.clear();
            state.total_bytes = total_bytes;
            state.next_access_tick = next_access_tick;
            let evicted = self.plan_budget_evictions_locked(&mut state);
            self.publish_progress_locked(&state);
            evicted
        };
        self.delete_entries(evicted).await?;
        Ok(())
    }

    async fn load_index_entry(
        &self,
        metadata_path: &str,
    ) -> Result<Option<CacheIndexEntry>, StorageError> {
        let bytes = match read_local_file_all(self.file_system.as_ref(), metadata_path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let file: CacheEntryFile = match decode_cache_entry_file_flatbuffer(&bytes) {
            Ok(file) => file,
            Err(_) => return Ok(None),
        };
        if file.format_version != CACHE_FORMAT_VERSION {
            return Ok(None);
        }

        let data_path = self.data_path(&file.record.object_key, file.record.span);
        match open_local_file(self.file_system.as_ref(), &data_path).await {
            Ok(_) => Ok(Some(CacheIndexEntry {
                record: file.record,
                metadata_path: metadata_path.to_string(),
                data_path,
                last_access_tick: 0,
            })),
            Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn cache_stem(&self, object_key: &str, span: CacheSpan) -> String {
        format!("{}--{}", hex_encode(object_key.as_bytes()), span.suffix())
    }

    fn data_path(&self, object_key: &str, span: CacheSpan) -> String {
        join_fs_path(
            &self.data_dir,
            &format!("{}.bin", self.cache_stem(object_key, span)),
        )
    }

    fn metadata_path(&self, object_key: &str, span: CacheSpan) -> String {
        join_fs_path(
            &self.metadata_dir,
            &format!("{}.json", self.cache_stem(object_key, span)),
        )
    }

    fn next_access_tick_locked(state: &mut RemoteCacheState) -> u64 {
        let tick = state.next_access_tick;
        state.next_access_tick = state.next_access_tick.saturating_add(1);
        tick
    }

    fn lookup_covering_entry_locked(
        state: &mut RemoteCacheState,
        object_key: &str,
        range: Range<u64>,
    ) -> Option<CacheIndexEntry> {
        let tick = Self::next_access_tick_locked(state);
        let object_entries = state.entries.get_mut(object_key)?;
        let entry = object_entries
            .iter_mut()
            .find(|entry| entry.record.span.contains(range.clone()))?;
        entry.last_access_tick = tick;
        Some(entry.clone())
    }

    fn lookup_exact_entry_locked(
        state: &mut RemoteCacheState,
        object_key: &str,
        span: CacheSpan,
    ) -> Option<CacheIndexEntry> {
        let tick = Self::next_access_tick_locked(state);
        let object_entries = state.entries.get_mut(object_key)?;
        let entry = object_entries
            .iter_mut()
            .find(|entry| entry.record.span == span)?;
        entry.last_access_tick = tick;
        Some(entry.clone())
    }

    fn lookup_full_entry_locked(
        state: &mut RemoteCacheState,
        object_key: &str,
    ) -> Option<CacheIndexEntry> {
        Self::lookup_exact_entry_locked(state, object_key, CacheSpan::Full)
    }

    fn range_entry_slice(entry: &CacheIndexEntry, range: Range<u64>, bytes: &[u8]) -> Vec<u8> {
        match entry.record.span {
            CacheSpan::Full => {
                if range.start as usize >= bytes.len() {
                    Vec::new()
                } else {
                    let end = (range.end as usize).min(bytes.len());
                    bytes[range.start as usize..end].to_vec()
                }
            }
            CacheSpan::Range { start, .. } => {
                let relative_start = range.start.saturating_sub(start) as usize;
                if relative_start >= bytes.len() {
                    Vec::new()
                } else {
                    let relative_end = relative_start
                        .saturating_add(range.end.saturating_sub(range.start) as usize)
                        .min(bytes.len());
                    bytes[relative_start..relative_end].to_vec()
                }
            }
        }
    }

    fn range_slice_from_span(span: CacheSpan, range: Range<u64>, bytes: &[u8]) -> Vec<u8> {
        match span {
            CacheSpan::Full => {
                if range.start as usize >= bytes.len() {
                    Vec::new()
                } else {
                    let end = (range.end as usize).min(bytes.len());
                    bytes[range.start as usize..end].to_vec()
                }
            }
            CacheSpan::Range { start, end } => {
                let slice_start = range.start.max(start);
                let slice_end = range.end.min(end);
                if slice_start >= slice_end {
                    Vec::new()
                } else {
                    let relative_start = slice_start.saturating_sub(start) as usize;
                    let relative_end = slice_end.saturating_sub(start) as usize;
                    let available_end = relative_end.min(bytes.len());
                    let available_start = relative_start.min(available_end);
                    bytes[available_start..available_end].to_vec()
                }
            }
        }
    }

    fn aligned_segment_span(&self, offset: u64) -> CacheSpan {
        let segment_start = offset - (offset % self.config.segment_bytes);
        CacheSpan::Range {
            start: segment_start,
            end: segment_start.saturating_add(self.config.segment_bytes),
        }
    }

    fn segment_spans_for_ranges(&self, ranges: &[Range<u64>]) -> Vec<CacheSpan> {
        let mut spans = BTreeSet::new();
        for range in ranges {
            if range.start >= range.end {
                continue;
            }
            let mut cursor = range.start;
            while cursor < range.end {
                let span = self.aligned_segment_span(cursor);
                spans.insert(span);
                cursor = match span {
                    CacheSpan::Range { end, .. } => end,
                    CacheSpan::Full => range.end,
                };
            }
        }
        spans.into_iter().collect()
    }

    fn prefetch_spans_for_ranges(&self, ranges: &[Range<u64>]) -> Vec<CacheSpan> {
        let prefetch_segments = (self.config.prefetch_bytes / self.config.segment_bytes) as usize;
        if prefetch_segments == 0 {
            return Vec::new();
        }
        let required = self.segment_spans_for_ranges(ranges);
        let last_end = required
            .iter()
            .filter_map(|span| match span {
                CacheSpan::Range { end, .. } => Some(*end),
                CacheSpan::Full => None,
            })
            .max();
        let Some(mut cursor) = last_end else {
            return Vec::new();
        };
        let mut spans = Vec::with_capacity(prefetch_segments);
        while spans.len() < prefetch_segments {
            let span = self.aligned_segment_span(cursor);
            spans.push(span);
            cursor = match span {
                CacheSpan::Range { end, .. } => end,
                CacheSpan::Full => break,
            };
        }
        spans
    }

    fn spans_exceed_cache_budget(&self, spans: &[CacheSpan]) -> bool {
        (spans.len() as u64).saturating_mul(self.config.segment_bytes) > self.config.max_bytes
    }

    fn coalesce_ranges(ranges: &[Range<u64>], gap_bytes: u64) -> Vec<CoalescedReadRange> {
        let mut sorted = ranges.iter().cloned().enumerate().collect::<Vec<_>>();
        sorted.sort_by_key(|(_, range)| (range.start, range.end));
        let mut merged: Vec<CoalescedReadRange> = Vec::new();
        for (index, range) in sorted {
            if range.start >= range.end {
                continue;
            }
            match merged.last_mut() {
                Some(current) if range.start <= current.fetch.end.saturating_add(gap_bytes) => {
                    current.fetch.end = current.fetch.end.max(range.end);
                    current.members.push((index, range));
                }
                _ => merged.push(CoalescedReadRange {
                    fetch: range.clone(),
                    members: vec![(index, range)],
                }),
            }
        }
        merged
    }

    fn can_satisfy_segment_locked(
        state: &RemoteCacheState,
        object_key: &str,
        span: CacheSpan,
    ) -> bool {
        state
            .entries
            .get(object_key)
            .into_iter()
            .flat_map(|entries| entries.iter())
            .any(|entry| {
                entry.record.span.contains(match span {
                    CacheSpan::Full => 0..0,
                    CacheSpan::Range { start, end } => start..end,
                })
            })
    }

    fn claim_segment_downloads(
        &self,
        object_key: &str,
        required: &[CacheSpan],
        prefetch: &[CacheSpan],
    ) -> (Vec<CacheSpan>, Vec<CacheSpan>, Vec<Arc<Notify>>) {
        let mut state = lock(&self.state);
        let mut to_download = Vec::new();
        let mut background = Vec::new();
        let mut waiters = Vec::new();
        let mut progress_changed = false;

        for &span in required {
            if Self::can_satisfy_segment_locked(&state, object_key, span) {
                continue;
            }
            let key = (object_key.to_string(), span);
            if let Some(segment) = state.in_flight.get_mut(&key) {
                segment.waiter_count = segment.waiter_count.saturating_add(1);
                let notify = segment.notify.clone();
                if !waiters
                    .iter()
                    .any(|existing: &Arc<Notify>| Arc::ptr_eq(existing, &notify))
                {
                    waiters.push(notify);
                }
                progress_changed = true;
                continue;
            }
            let notify = Arc::new(Notify::new());
            state.in_flight.insert(
                key,
                InFlightSegment {
                    notify,
                    fetch_kind: RemoteCacheFetchKind::Demand,
                    waiter_count: 0,
                },
            );
            to_download.push(span);
            progress_changed = true;
        }

        for &span in prefetch {
            if Self::can_satisfy_segment_locked(&state, object_key, span) {
                continue;
            }
            let key = (object_key.to_string(), span);
            if state.in_flight.contains_key(&key) {
                continue;
            }
            let notify = Arc::new(Notify::new());
            state.in_flight.insert(
                key,
                InFlightSegment {
                    notify,
                    fetch_kind: RemoteCacheFetchKind::Prefetch,
                    waiter_count: 0,
                },
            );
            background.push(span);
            progress_changed = true;
        }

        if progress_changed {
            self.publish_progress_locked(&state);
        }

        (to_download, background, waiters)
    }

    fn finish_in_flight_segments(&self, object_key: &str, spans: &[CacheSpan]) -> Vec<Arc<Notify>> {
        let mut state = lock(&self.state);
        let mut notifies = Vec::new();
        let mut progress_changed = false;
        for &span in spans {
            if let Some(in_flight) = state.in_flight.remove(&(object_key.to_string(), span)) {
                notifies.push(in_flight.notify);
                progress_changed = true;
            }
        }
        if progress_changed {
            self.publish_progress_locked(&state);
        }
        notifies
    }

    fn plan_budget_evictions_locked(&self, state: &mut RemoteCacheState) -> Vec<CacheIndexEntry> {
        let mut removed = Vec::new();
        while state.total_bytes > self.config.max_bytes {
            let Some((object_key, span, _)) = state
                .entries
                .iter()
                .flat_map(|(object_key, entries)| {
                    entries.iter().map(move |entry| {
                        (
                            object_key.clone(),
                            entry.record.span,
                            entry.last_access_tick,
                        )
                    })
                })
                .min_by_key(|(_, _, tick)| *tick)
            else {
                break;
            };
            let Some(entries) = state.entries.get_mut(&object_key) else {
                continue;
            };
            let Some(index) = entries.iter().position(|entry| entry.record.span == span) else {
                continue;
            };
            let removed_entry = entries.remove(index);
            state.total_bytes = state
                .total_bytes
                .saturating_sub(removed_entry.record.data_len);
            removed.push(removed_entry);
            if entries.is_empty() {
                state.entries.remove(&object_key);
            }
        }
        removed
    }

    async fn delete_entries(&self, entries: Vec<CacheIndexEntry>) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        for entry in entries {
            match self.file_system.delete(&entry.data_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == StorageErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            match self.file_system.delete(&entry.metadata_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == StorageErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        self.file_system.sync_dir(&self.data_dir).await?;
        self.file_system.sync_dir(&self.metadata_dir).await?;
        Ok(())
    }

    async fn try_read_range_from_cache(
        &self,
        object_key: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if range.start >= range.end {
            return Ok(Some(Vec::new()));
        }

        if let Some(entry) = {
            let mut state = lock(&self.state);
            Self::lookup_covering_entry_locked(&mut state, object_key, range.clone())
        } {
            let bytes = read_local_file_all(self.file_system.as_ref(), &entry.data_path).await?;
            return Ok(Some(Self::range_entry_slice(&entry, range, &bytes)));
        }

        let segments = self.segment_spans_for_ranges(std::slice::from_ref(&range));
        let segment_entries = {
            let mut state = lock(&self.state);
            let mut entries = Vec::with_capacity(segments.len());
            for span in &segments {
                let Some(entry) = Self::lookup_exact_entry_locked(&mut state, object_key, *span)
                else {
                    return Ok(None);
                };
                entries.push(entry);
            }
            entries
        };

        let mut stitched = Vec::new();
        for entry in segment_entries {
            let bytes = read_local_file_all(self.file_system.as_ref(), &entry.data_path).await?;
            let CacheSpan::Range { start, end } = entry.record.span else {
                continue;
            };
            let slice_start = range.start.max(start);
            let slice_end = range.end.min(end);
            if slice_start >= slice_end {
                continue;
            }
            stitched.extend_from_slice(&Self::range_entry_slice(
                &entry,
                slice_start..slice_end,
                &bytes,
            ));
        }
        Ok(Some(stitched))
    }

    async fn fetch_remote_ranges(
        object_store: &dyn ObjectStore,
        key: &str,
        ranges: &[Range<u64>],
        gap_bytes: u64,
    ) -> Result<Vec<(Range<u64>, Vec<u8>)>, RemoteStorageError> {
        let mut fetched = Vec::new();
        for merged in Self::coalesce_ranges(ranges, gap_bytes) {
            let bytes = object_store
                .get_range(key, merged.fetch.start, merged.fetch.end)
                .await
                .map_err(|error| {
                    let error = RemoteStorageError::classify(RemoteOperation::GetRange, key, error);
                    emit_remote_error(&error);
                    error
                })?;
            fetched.push((merged.fetch, bytes));
        }
        Ok(fetched)
    }

    async fn store_downloaded_segments(
        &self,
        object_key: &str,
        downloaded_segments: &BTreeMap<CacheSpan, Vec<u8>>,
    ) -> Result<(), StorageError> {
        for (&span, bytes) in downloaded_segments {
            self.store(object_key, span, bytes).await?;
        }
        Ok(())
    }

    fn extract_downloaded_segments(
        spans: &[CacheSpan],
        fetched_groups: &[(Range<u64>, Vec<u8>)],
    ) -> BTreeMap<CacheSpan, Vec<u8>> {
        let mut downloaded = BTreeMap::new();
        for &span in spans {
            let CacheSpan::Range { start, end } = span else {
                continue;
            };
            let mut segment_bytes = None;
            for (fetched_range, bytes) in fetched_groups {
                if fetched_range.start <= start && fetched_range.end >= end {
                    let relative_start = start.saturating_sub(fetched_range.start) as usize;
                    let relative_end = end.saturating_sub(fetched_range.start) as usize;
                    let available_end = relative_end.min(bytes.len());
                    let available_start = relative_start.min(available_end);
                    segment_bytes = Some(bytes[available_start..available_end].to_vec());
                    break;
                }
            }
            downloaded.insert(span, segment_bytes.unwrap_or_default());
        }
        downloaded
    }

    async fn spawn_background_prefetch(
        self: Arc<Self>,
        object_store: Arc<dyn ObjectStore>,
        object_key: String,
        spans: Vec<CacheSpan>,
    ) {
        if spans.is_empty() {
            return;
        }
        let ranges = spans
            .iter()
            .filter_map(|span| match span {
                CacheSpan::Range { start, end } => Some(*start..*end),
                CacheSpan::Full => None,
            })
            .collect::<Vec<_>>();
        let outcome = Self::fetch_remote_ranges(
            object_store.as_ref(),
            &object_key,
            &ranges,
            self.config.coalesce_gap_bytes,
        )
        .await;

        match outcome {
            Err(error) => {
                tracing::warn!(
                    terracedb_remote_target = %object_key,
                    error = %error,
                    "terracedb background segment prefetch failed"
                );
            }
            Ok(fetched) => {
                let downloaded = Self::extract_downloaded_segments(&spans, &fetched);
                if let Err(error) = self
                    .store_downloaded_segments(&object_key, &downloaded)
                    .await
                {
                    tracing::warn!(
                        terracedb_remote_target = %object_key,
                        error = %error,
                        "terracedb background segment cache write failed"
                    );
                }
            }
        }
        for notify in self.finish_in_flight_segments(&object_key, &spans) {
            notify.notify_waiters();
        }
    }

    async fn try_read_range_from_cache_or_downloads(
        &self,
        object_key: &str,
        range: Range<u64>,
        downloaded_segments: &BTreeMap<CacheSpan, Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(bytes) = self
            .try_read_range_from_cache(object_key, range.clone())
            .await?
        {
            return Ok(Some(bytes));
        }

        let segments = self.segment_spans_for_ranges(std::slice::from_ref(&range));
        if segments.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let mut stitched = Vec::new();
        for span in segments {
            let Some(bytes) = downloaded_segments.get(&span) else {
                return Ok(None);
            };
            stitched.extend_from_slice(&Self::range_slice_from_span(span, range.clone(), bytes));
        }
        Ok(Some(stitched))
    }

    async fn read_through_ranges(
        self: &Arc<Self>,
        object_store: Arc<dyn ObjectStore>,
        object_key: &str,
        ranges: &[Range<u64>],
    ) -> Result<Vec<u8>, UnifiedStorageError> {
        let required = self.segment_spans_for_ranges(ranges);
        if self.spans_exceed_cache_budget(&required) {
            let parts =
                UnifiedStorage::coalesced_remote_read(object_store.as_ref(), object_key, ranges)
                    .await?;
            return Ok(parts.concat());
        }

        let mut downloaded_segments = BTreeMap::<CacheSpan, Vec<u8>>::new();
        loop {
            let mut stitched = Vec::new();
            let mut all_hit = true;
            for range in ranges {
                match self
                    .try_read_range_from_cache_or_downloads(
                        object_key,
                        range.clone(),
                        &downloaded_segments,
                    )
                    .await?
                {
                    Some(bytes) => stitched.extend_from_slice(&bytes),
                    None => {
                        all_hit = false;
                        break;
                    }
                }
            }
            if all_hit {
                return Ok(stitched);
            }

            let prefetch = self.prefetch_spans_for_ranges(ranges);
            let (to_download, background, waiters) =
                self.claim_segment_downloads(object_key, &required, &prefetch);

            if !to_download.is_empty() {
                let download_ranges = to_download
                    .iter()
                    .filter_map(|span| match span {
                        CacheSpan::Range { start, end } => Some(*start..*end),
                        CacheSpan::Full => None,
                    })
                    .collect::<Vec<_>>();
                let fetched = match Self::fetch_remote_ranges(
                    object_store.as_ref(),
                    object_key,
                    &download_ranges,
                    self.config.coalesce_gap_bytes,
                )
                .await
                {
                    Ok(fetched) => fetched,
                    Err(error) => {
                        for notify in self.finish_in_flight_segments(object_key, &to_download) {
                            notify.notify_waiters();
                        }
                        return Err(UnifiedStorageError::Remote(error));
                    }
                };
                let downloaded = Self::extract_downloaded_segments(&to_download, &fetched);
                if let Err(error) = self
                    .store_downloaded_segments(object_key, &downloaded)
                    .await
                {
                    for notify in self.finish_in_flight_segments(object_key, &to_download) {
                        notify.notify_waiters();
                    }
                    return Err(UnifiedStorageError::Local(error));
                }
                downloaded_segments.extend(downloaded);
                for notify in self.finish_in_flight_segments(object_key, &to_download) {
                    notify.notify_waiters();
                }
            }

            if !background.is_empty() {
                let cache = self.clone();
                let object_store = object_store.clone();
                let object_key = object_key.to_string();
                task::spawn(async move {
                    cache
                        .spawn_background_prefetch(object_store, object_key, background)
                        .await;
                });
            }

            if to_download.is_empty() && waiters.is_empty() {
                continue;
            }

            for waiter in waiters {
                waiter.notified().await;
            }
        }
    }

    pub async fn read_full(&self, object_key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(entry) = ({
            let mut state = lock(&self.state);
            Self::lookup_full_entry_locked(&mut state, object_key)
        }) else {
            return Ok(None);
        };
        read_local_file_all(self.file_system.as_ref(), &entry.data_path)
            .await
            .map(Some)
    }

    pub async fn read_range(
        &self,
        object_key: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.try_read_range_from_cache(object_key, range).await
    }

    pub async fn store(
        &self,
        object_key: &str,
        span: CacheSpan,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        if bytes.len() as u64 > self.config.max_bytes {
            return Ok(());
        }
        let data_path = self.data_path(object_key, span);
        let metadata_path = self.metadata_path(object_key, span);
        write_bytes_atomic(self.file_system.as_ref(), &data_path, bytes).await?;

        let metadata = CacheEntryFile {
            format_version: CACHE_FORMAT_VERSION,
            record: CacheEntryRecord {
                object_key: object_key.to_string(),
                span,
                data_len: bytes.len() as u64,
            },
        };
        let payload = encode_cache_entry_file_flatbuffer(&metadata)?;
        write_bytes_atomic(self.file_system.as_ref(), &metadata_path, &payload).await?;
        self.file_system.sync_dir(&self.data_dir).await?;
        self.file_system.sync_dir(&self.metadata_dir).await?;

        let evicted = {
            let mut state = lock(&self.state);
            let tick = Self::next_access_tick_locked(&mut state);
            let removed_bytes = {
                let object_entries = state.entries.entry(object_key.to_string()).or_default();
                if let Some(index) = object_entries
                    .iter()
                    .position(|entry| entry.record.span == span)
                {
                    let removed = object_entries.remove(index);
                    Some(removed.record.data_len)
                } else {
                    None
                }
            };
            if let Some(removed_bytes) = removed_bytes {
                state.total_bytes = state.total_bytes.saturating_sub(removed_bytes);
            }
            let record = metadata.record.clone();
            let object_entries = state.entries.entry(object_key.to_string()).or_default();
            object_entries.push(CacheIndexEntry {
                record,
                metadata_path,
                data_path,
                last_access_tick: tick,
            });
            state.total_bytes = state.total_bytes.saturating_add(metadata.record.data_len);
            let evicted = self.plan_budget_evictions_locked(&mut state);
            self.publish_progress_locked(&state);
            evicted
        };
        self.delete_entries(evicted).await?;
        Ok(())
    }

    pub(crate) async fn remove_span(
        &self,
        object_key: &str,
        span: CacheSpan,
    ) -> Result<(), StorageError> {
        let removed = {
            let mut state = lock(&self.state);
            let removed = {
                let object_entries = state.entries.entry(object_key.to_string()).or_default();
                let mut removed = Vec::new();
                object_entries.retain(|entry| {
                    let matches = entry.record.span == span;
                    if matches {
                        removed.push(entry.clone());
                    }
                    !matches
                });
                removed
            };
            for entry in &removed {
                state.total_bytes = state.total_bytes.saturating_sub(entry.record.data_len);
            }
            let remove_object = state
                .entries
                .get(object_key)
                .is_some_and(|entries| entries.is_empty());
            if remove_object {
                state.entries.remove(object_key);
            }
            self.publish_progress_locked(&state);
            removed
        };

        self.delete_entries(removed).await
    }

    pub async fn remove_object(&self, object_key: &str) -> Result<(), StorageError> {
        let (removed, notifies) = {
            let mut state = lock(&self.state);
            let removed = state.entries.remove(object_key).unwrap_or_default();
            for entry in &removed {
                state.total_bytes = state.total_bytes.saturating_sub(entry.record.data_len);
            }
            let keys = state
                .in_flight
                .keys()
                .filter(|(key, _)| key == object_key)
                .cloned()
                .collect::<Vec<_>>();
            let mut notifies = Vec::new();
            for key in keys {
                if let Some(in_flight) = state.in_flight.remove(&key) {
                    notifies.push(in_flight.notify);
                }
            }
            self.publish_progress_locked(&state);
            (removed, notifies)
        };
        for notify in notifies {
            notify.notify_waiters();
        }
        self.delete_entries(removed).await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteOperation {
    Put,
    Get,
    GetRange,
    Delete,
    List,
    Copy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteRecoveryHint {
    Retry,
    RefreshListing,
    RebuildLocalState,
    FailClosed,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("remote {operation:?} failed for {target} ({kind:?}, hint={recovery_hint:?}): {message}")]
pub struct RemoteStorageError {
    operation: RemoteOperation,
    target: String,
    kind: StorageErrorKind,
    message: String,
    recovery_hint: RemoteRecoveryHint,
}

impl RemoteStorageError {
    fn classify(
        operation: RemoteOperation,
        target: impl Into<String>,
        error: StorageError,
    ) -> Self {
        let recovery_hint = match error.kind() {
            StorageErrorKind::Timeout | StorageErrorKind::Io => RemoteRecoveryHint::Retry,
            StorageErrorKind::DurabilityBoundary => RemoteRecoveryHint::RefreshListing,
            StorageErrorKind::NotFound => RemoteRecoveryHint::RebuildLocalState,
            StorageErrorKind::Corruption | StorageErrorKind::Unsupported => {
                RemoteRecoveryHint::FailClosed
            }
        };
        Self {
            operation,
            target: target.into(),
            kind: error.kind(),
            message: error.message().to_string(),
            recovery_hint,
        }
    }

    pub fn operation(&self) -> RemoteOperation {
        self.operation
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    pub fn recovery_hint(&self) -> RemoteRecoveryHint {
        self.recovery_hint
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self.recovery_hint, RemoteRecoveryHint::Retry)
    }

    pub fn into_storage_error(self) -> StorageError {
        StorageError::new(self.kind, self.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum UnifiedStorageError {
    #[error(transparent)]
    Local(#[from] StorageError),
    #[error(transparent)]
    Remote(#[from] RemoteStorageError),
}

impl UnifiedStorageError {
    pub fn into_storage_error(self) -> StorageError {
        match self {
            Self::Local(error) => error,
            Self::Remote(error) => error.into_storage_error(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageSource {
    LocalFile { path: String },
    RemoteObject { key: String },
}

impl StorageSource {
    pub fn local_file(path: impl Into<String>) -> Self {
        Self::LocalFile { path: path.into() }
    }

    pub fn remote_object(key: impl Into<String>) -> Self {
        Self::RemoteObject { key: key.into() }
    }

    pub fn target(&self) -> &str {
        match self {
            Self::LocalFile { path } => path,
            Self::RemoteObject { key } => key,
        }
    }
}

#[derive(Clone)]
pub struct UnifiedStorage {
    file_system: Arc<dyn FileSystem>,
    object_store: Arc<dyn ObjectStore>,
    cache: Option<Arc<RemoteCache>>,
}

impl fmt::Debug for UnifiedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnifiedStorage")
            .field("file_system", &"<dyn FileSystem>")
            .field("object_store", &"<dyn ObjectStore>")
            .field("cache", &self.cache.as_ref().map(|cache| cache.root()))
            .finish()
    }
}

impl UnifiedStorage {
    pub fn new(
        file_system: Arc<dyn FileSystem>,
        object_store: Arc<dyn ObjectStore>,
        cache: Option<Arc<RemoteCache>>,
    ) -> Self {
        Self {
            file_system,
            object_store,
            cache,
        }
    }

    pub fn from_dependencies(dependencies: &DbDependencies) -> Self {
        Self::new(
            dependencies.file_system.clone(),
            dependencies.object_store.clone(),
            None,
        )
    }

    pub fn with_cache(mut self, cache: Arc<RemoteCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub async fn read_all(&self, source: &StorageSource) -> Result<Vec<u8>, UnifiedStorageError> {
        match source {
            StorageSource::LocalFile { path } => {
                read_local_file_all(self.file_system.as_ref(), path)
                    .await
                    .map_err(UnifiedStorageError::Local)
            }
            StorageSource::RemoteObject { key } => {
                if let Some(cache) = &self.cache
                    && let Ok(Some(bytes)) = cache.read_full(key).await
                {
                    return Ok(bytes);
                }

                let bytes = self.object_store.get(key).await.map_err(|error| {
                    let error = RemoteStorageError::classify(RemoteOperation::Get, key, error);
                    emit_remote_error(&error);
                    UnifiedStorageError::Remote(error)
                })?;
                if let Some(cache) = &self.cache {
                    cache
                        .store(key, CacheSpan::Full, &bytes)
                        .await
                        .map_err(UnifiedStorageError::Local)?;
                }
                Ok(bytes)
            }
        }
    }

    pub async fn read_range(
        &self,
        source: &StorageSource,
        range: Range<u64>,
    ) -> Result<Vec<u8>, UnifiedStorageError> {
        match source {
            StorageSource::LocalFile { path } => {
                read_local_file_range(self.file_system.as_ref(), path, range)
                    .await
                    .map_err(UnifiedStorageError::Local)
            }
            StorageSource::RemoteObject { key } => {
                if let Some(cache) = &self.cache {
                    return cache
                        .read_through_ranges(
                            self.object_store.clone(),
                            key,
                            std::slice::from_ref(&range),
                        )
                        .await;
                }

                let mut stitched = Self::coalesced_remote_read(
                    self.object_store.as_ref(),
                    key,
                    std::slice::from_ref(&range),
                )
                .await?;
                Ok(stitched.pop().unwrap_or_default())
            }
        }
    }

    pub async fn read_ranges(
        &self,
        source: &StorageSource,
        ranges: &[Range<u64>],
    ) -> Result<Vec<u8>, UnifiedStorageError> {
        match source {
            StorageSource::LocalFile { .. } => {
                let mut stitched = Vec::new();
                for range in ranges {
                    stitched.extend_from_slice(&self.read_range(source, range.clone()).await?);
                }
                Ok(stitched)
            }
            StorageSource::RemoteObject { key } => {
                if let Some(cache) = &self.cache {
                    return cache
                        .read_through_ranges(self.object_store.clone(), key, ranges)
                        .await;
                }
                let parts =
                    Self::coalesced_remote_read(self.object_store.as_ref(), key, ranges).await?;
                Ok(parts.concat())
            }
        }
    }

    async fn coalesced_remote_read(
        object_store: &dyn ObjectStore,
        key: &str,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Vec<u8>>, UnifiedStorageError> {
        let merged = RemoteCache::coalesce_ranges(ranges, DEFAULT_REMOTE_CACHE_COALESCE_GAP_BYTES);
        let mut responses = vec![Vec::new(); ranges.len()];
        for group in merged {
            let bytes = object_store
                .get_range(key, group.fetch.start, group.fetch.end)
                .await
                .map_err(|error| {
                    let error = RemoteStorageError::classify(RemoteOperation::GetRange, key, error);
                    emit_remote_error(&error);
                    UnifiedStorageError::Remote(error)
                })?;
            for (member_index, member) in &group.members {
                let relative_start = member.start.saturating_sub(group.fetch.start) as usize;
                let relative_end = member.end.saturating_sub(group.fetch.start) as usize;
                let available_end = relative_end.min(bytes.len());
                let available_start = relative_start.min(available_end);
                responses[*member_index] = bytes[available_start..available_end].to_vec();
            }
        }
        Ok(responses)
    }

    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, RemoteStorageError> {
        self.object_store.list(prefix).await.map_err(|error| {
            let error = RemoteStorageError::classify(RemoteOperation::List, prefix, error);
            emit_remote_error(&error);
            error
        })
    }

    pub async fn put_object(&self, key: &str, data: &[u8]) -> Result<(), RemoteStorageError> {
        self.object_store.put(key, data).await.map_err(|error| {
            let error = RemoteStorageError::classify(RemoteOperation::Put, key, error);
            emit_remote_error(&error);
            error
        })?;
        if let Some(cache) = &self.cache {
            cache.remove_object(key).await.map_err(|error| {
                let error = RemoteStorageError::classify(RemoteOperation::Put, key, error);
                emit_remote_error(&error);
                error
            })?;
        }
        Ok(())
    }

    pub async fn copy_object(&self, from: &str, to: &str) -> Result<(), RemoteStorageError> {
        self.object_store.copy(from, to).await.map_err(|error| {
            let error = RemoteStorageError::classify(RemoteOperation::Copy, from, error);
            emit_remote_error(&error);
            error
        })?;
        if let Some(cache) = &self.cache {
            cache.remove_object(to).await.map_err(|error| {
                let error = RemoteStorageError::classify(RemoteOperation::Copy, to, error);
                emit_remote_error(&error);
                error
            })?;
        }
        Ok(())
    }

    pub async fn delete_object(&self, key: &str) -> Result<(), RemoteStorageError> {
        self.object_store.delete(key).await.map_err(|error| {
            let error = RemoteStorageError::classify(RemoteOperation::Delete, key, error);
            emit_remote_error(&error);
            error
        })?;
        if let Some(cache) = &self.cache {
            cache.remove_object(key).await.map_err(|error| {
                let error = RemoteStorageError::classify(RemoteOperation::Delete, key, error);
                emit_remote_error(&error);
                error
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    };

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use proptest::prelude::*;

    use super::*;
    use crate::{
        LocalDirObjectStore, ObjectStoreFailure, ObjectStoreOperation, SequenceNumber,
        SimulatedObjectStore, TokioFileSystem,
    };

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_test_dir(name: &str) -> PathBuf {
        let id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "terracedb-remote-{name}-{}-{id}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        path
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    fn sample_cache_entry_file() -> CacheEntryFile {
        CacheEntryFile {
            format_version: CACHE_FORMAT_VERSION,
            record: CacheEntryRecord {
                object_key:
                    "tenant-a/db-01/cold/table-000009/0000/00000000000000000044-00000000000000000088/SST-000123.sst"
                        .to_string(),
                span: CacheSpan::Range {
                    start: 128,
                    end: 512,
                },
                data_len: 384,
            },
        }
    }

    fn test_cache_config(
        segment_bytes: u64,
        max_bytes: u64,
        prefetch_bytes: u64,
    ) -> RemoteCacheConfig {
        RemoteCacheConfig {
            max_bytes,
            segment_bytes,
            coalesce_gap_bytes: segment_bytes,
            prefetch_bytes,
        }
    }

    #[derive(Clone)]
    struct CountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        get_calls: Arc<Mutex<Vec<String>>>,
        range_calls: Arc<Mutex<Vec<(String, u64, u64)>>>,
        list_calls: Arc<Mutex<Vec<String>>>,
    }

    impl CountingObjectStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                get_calls: Arc::new(Mutex::new(Vec::new())),
                range_calls: Arc::new(Mutex::new(Vec::new())),
                list_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_count(&self) -> usize {
            lock(&self.get_calls).len()
        }

        fn range_calls(&self) -> Vec<(String, u64, u64)> {
            lock(&self.range_calls).clone()
        }
    }

    #[derive(Clone)]
    struct BlockingRangeObjectStore {
        inner: Arc<dyn ObjectStore>,
        range_calls: Arc<Mutex<Vec<(String, u64, u64)>>>,
        range_call_notify: Arc<Notify>,
        block_ranges: Arc<AtomicBool>,
        release_ranges: Arc<Notify>,
    }

    impl BlockingRangeObjectStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                range_calls: Arc::new(Mutex::new(Vec::new())),
                range_call_notify: Arc::new(Notify::new()),
                block_ranges: Arc::new(AtomicBool::new(true)),
                release_ranges: Arc::new(Notify::new()),
            }
        }

        async fn wait_for_range_calls(&self, expected: usize) {
            loop {
                if lock(&self.range_calls).len() >= expected {
                    return;
                }
                self.range_call_notify.notified().await;
            }
        }

        fn release(&self) {
            self.block_ranges.store(false, Ordering::Relaxed);
            self.release_ranges.notify_waiters();
        }

        fn range_calls(&self) -> Vec<(String, u64, u64)> {
            lock(&self.range_calls).clone()
        }
    }

    #[async_trait]
    impl ObjectStore for CountingObjectStore {
        async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
            self.inner.put(key, data).await
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            lock(&self.get_calls).push(key.to_string());
            self.inner.get(key).await
        }

        async fn get_range(
            &self,
            key: &str,
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, StorageError> {
            lock(&self.range_calls).push((key.to_string(), start, end));
            self.inner.get_range(key, start, end).await
        }

        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            lock(&self.list_calls).push(prefix.to_string());
            self.inner.list(prefix).await
        }

        async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
            self.inner.copy(from, to).await
        }
    }

    #[async_trait]
    impl ObjectStore for BlockingRangeObjectStore {
        async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
            self.inner.put(key, data).await
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            self.inner.get(key).await
        }

        async fn get_range(
            &self,
            key: &str,
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, StorageError> {
            lock(&self.range_calls).push((key.to_string(), start, end));
            self.range_call_notify.notify_waiters();
            if self.block_ranges.load(Ordering::Relaxed) {
                self.release_ranges.notified().await;
            }
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

    #[test]
    fn object_key_layout_is_stable() {
        let layout = ObjectKeyLayout::new(&S3Location {
            bucket: "bucket".to_string(),
            prefix: "/tenant-a/db-01/".to_string(),
        });

        assert_eq!(layout.backup_prefix(), "tenant-a/db-01/backup");
        assert_eq!(layout.control_prefix(), "tenant-a/db-01/control");
        assert_eq!(
            layout.control_catalog(),
            "tenant-a/db-01/control/catalog/CATALOG.json"
        );
        assert_eq!(
            layout.control_manifest(ManifestId::new(7)),
            "tenant-a/db-01/control/manifest/MANIFEST-000007"
        );
        assert_eq!(
            layout.control_manifest_latest(),
            "tenant-a/db-01/control/manifest/latest"
        );
        assert_eq!(
            layout.backup_catalog(),
            "tenant-a/db-01/backup/catalog/CATALOG.json"
        );
        assert_eq!(
            layout.backup_manifest(ManifestId::new(7)),
            "tenant-a/db-01/backup/manifest/MANIFEST-000007"
        );
        assert_eq!(
            layout.backup_manifest_latest(),
            "tenant-a/db-01/backup/manifest/latest"
        );
        assert_eq!(
            layout.backup_commit_log_segment(SegmentId::new(11)),
            "tenant-a/db-01/backup/commitlog/SEG-000011"
        );
        assert_eq!(
            layout.backup_sstable(TableId::new(9), 0, "SST-000123"),
            "tenant-a/db-01/backup/sst/table-000009/0000/SST-000123.sst"
        );
        assert_eq!(layout.backup_sstable_prefix(), "tenant-a/db-01/backup/sst");
        assert_eq!(
            layout.backup_gc_metadata_prefix(),
            "tenant-a/db-01/backup/gc/objects"
        );
        assert_eq!(
            layout.backup_gc_metadata("tenant-a/db-01/backup/sst/table-000009/0000/SST-000123.sst"),
            "tenant-a/db-01/backup/gc/objects/74656e616e742d612f64622d30312f6261636b75702f7373742f7461626c652d3030303030392f303030302f5353542d3030303132332e737374.json"
        );
        assert_eq!(
            layout.cold_sstable(
                TableId::new(9),
                0,
                SequenceNumber::new(44),
                SequenceNumber::new(88),
                "SST-000123"
            ),
            "tenant-a/db-01/cold/table-000009/0000/00000000000000000044-00000000000000000088/SST-000123.sst"
        );
        assert_eq!(layout.cold_prefix(), "tenant-a/db-01/cold");
    }

    proptest! {
        #[test]
        fn object_key_layout_trims_boundary_slashes_without_creating_double_separators(
            segments in prop::collection::vec("[a-z0-9-]{1,8}", 0..4),
            manifest_generation in any::<u64>(),
            segment_id in any::<u64>(),
            table_id in 1_u32..1000,
            shard in any::<u16>(),
            min_sequence in any::<u64>(),
            span in 0_u64..1024,
            local_id in "[A-Z0-9-]{1,16}",
        ) {
            let logical_prefix = segments.join("/");
            let normalized = ObjectKeyLayout::new(&S3Location {
                bucket: "bucket".to_string(),
                prefix: logical_prefix.clone(),
            });
            let slashed = ObjectKeyLayout::new(&S3Location {
                bucket: "bucket".to_string(),
                prefix: format!("/{logical_prefix}/"),
            });
            let max_sequence = min_sequence.saturating_add(span);

            let normalized_keys = [
                normalized.backup_manifest(ManifestId::new(manifest_generation)),
                normalized.backup_commit_log_segment(SegmentId::new(segment_id)),
                normalized.backup_sstable(TableId::new(table_id), shard as u32, &local_id),
                normalized.cold_sstable(
                    TableId::new(table_id),
                    shard as u32,
                    SequenceNumber::new(min_sequence),
                    SequenceNumber::new(max_sequence),
                    &local_id,
                ),
            ];
            let slashed_keys = [
                slashed.backup_manifest(ManifestId::new(manifest_generation)),
                slashed.backup_commit_log_segment(SegmentId::new(segment_id)),
                slashed.backup_sstable(TableId::new(table_id), shard as u32, &local_id),
                slashed.cold_sstable(
                    TableId::new(table_id),
                    shard as u32,
                    SequenceNumber::new(min_sequence),
                    SequenceNumber::new(max_sequence),
                    &local_id,
                ),
            ];

            prop_assert_eq!(normalized_keys.as_slice(), slashed_keys.as_slice());
            for key in &normalized_keys {
                prop_assert!(!key.contains("//"));
            }
        }
    }

    #[test]
    fn durable_format_fixtures_match_golden_files() {
        let metadata = sample_cache_entry_file();
        let payload = encode_cache_entry_file_flatbuffer(&metadata)
            .expect("encode remote cache metadata fixture");
        let decoded = decode_cache_entry_file_flatbuffer(&payload)
            .expect("decode remote cache metadata fixture");
        assert_eq!(decoded, metadata);
        crate::test_support::assert_durable_format_fixture("remote-cache-entry-v1.bin", &payload);
    }

    #[tokio::test]
    async fn remote_cache_hits_and_misses_behave_as_expected() {
        let cache_root = unique_test_dir("cache-hit");
        let object_root = unique_test_dir("cache-hit-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(CountingObjectStore::new(inner_store));
        let cache = Arc::new(
            RemoteCache::open(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
            )
            .await
            .expect("open remote cache"),
        );
        let storage = UnifiedStorage::new(file_system, store.clone(), Some(cache));
        let source = StorageSource::remote_object("backup/sst/table-000001/0000/SST-000001.sst");

        store
            .put(source.target(), b"hello remote cache")
            .await
            .expect("seed remote object");

        let first = storage.read_all(&source).await.expect("read through cache");
        let second = storage.read_all(&source).await.expect("read cached object");

        assert_eq!(first, b"hello remote cache");
        assert_eq!(second, b"hello remote cache");
        assert_eq!(
            store.get_count(),
            1,
            "first read should miss and populate cache"
        );

        cleanup(&cache_root);
        cleanup(&object_root);
    }

    #[tokio::test]
    async fn remote_cache_rebuilds_metadata_after_restart() {
        let cache_root = unique_test_dir("cache-restart");
        let object_root = unique_test_dir("cache-restart-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(CountingObjectStore::new(inner_store));
        let key = "cold/table-000001/0000/00000000000000000001-00000000000000000009/SST-000001.sst";
        store
            .put(key, b"restartable bytes")
            .await
            .expect("seed object store");

        let first_cache = Arc::new(
            RemoteCache::open(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
            )
            .await
            .expect("open first cache"),
        );
        let first_storage =
            UnifiedStorage::new(file_system.clone(), store.clone(), Some(first_cache));
        let source = StorageSource::remote_object(key);
        let first = first_storage.read_all(&source).await.expect("first read");
        assert_eq!(first, b"restartable bytes");
        assert_eq!(store.get_count(), 1);

        let rebuilt_cache = Arc::new(
            RemoteCache::open(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
            )
            .await
            .expect("reopen cache"),
        );
        assert!(rebuilt_cache.entry_count() >= 1);
        lock(&store.get_calls).clear();
        let rebuilt_storage = UnifiedStorage::new(file_system, store.clone(), Some(rebuilt_cache));
        let second = rebuilt_storage
            .read_all(&source)
            .await
            .expect("cached read after restart");
        assert_eq!(second, b"restartable bytes");
        assert_eq!(
            store.get_count(),
            0,
            "reopened cache should satisfy the read"
        );

        cleanup(&cache_root);
        cleanup(&object_root);
    }

    #[tokio::test]
    async fn corrupt_or_unsupported_cache_metadata_is_ignored_on_rebuild() {
        let cache_root = unique_test_dir("cache-invalid-metadata");
        let object_root = unique_test_dir("cache-invalid-metadata-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(CountingObjectStore::new(inner_store));
        let cache = RemoteCache::open_with_config(
            file_system.clone(),
            cache_root.to_string_lossy().into_owned(),
            test_cache_config(8, 1024 * 1024, 0),
        )
        .await
        .expect("open cache");

        let full_key = "backup/sst/table-000001/0000/SST-000001.sst";
        let range_key =
            "cold/table-000001/0000/00000000000000000001-00000000000000000009/SST-000001.sst";
        store
            .put(full_key, b"hello invalid cache")
            .await
            .expect("seed full object");
        store
            .put(range_key, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .expect("seed range object");

        cache
            .store(full_key, CacheSpan::Full, b"hello invalid cache")
            .await
            .expect("store cached full object");
        cache
            .store(range_key, CacheSpan::Range { start: 2, end: 6 }, b"cdef")
            .await
            .expect("store cached range");

        let corrupt_path = cache.metadata_path(full_key, CacheSpan::Full);
        write_bytes_atomic(file_system.as_ref(), &corrupt_path, b"not a flatbuffer")
            .await
            .expect("rewrite corrupt metadata");

        let unsupported = CacheEntryFile {
            format_version: CACHE_FORMAT_VERSION + 1,
            record: CacheEntryRecord {
                object_key: range_key.to_string(),
                span: CacheSpan::Range { start: 2, end: 6 },
                data_len: 4,
            },
        };
        let unsupported_path =
            cache.metadata_path(range_key, CacheSpan::Range { start: 2, end: 6 });
        let unsupported_payload =
            encode_cache_entry_file_flatbuffer(&unsupported).expect("encode unsupported metadata");
        write_bytes_atomic(
            file_system.as_ref(),
            &unsupported_path,
            &unsupported_payload,
        )
        .await
        .expect("rewrite unsupported metadata");

        let rebuilt = Arc::new(
            RemoteCache::open_with_config(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
                test_cache_config(8, 1024 * 1024, 0),
            )
            .await
            .expect("reopen cache"),
        );
        assert_eq!(
            rebuilt.entry_count(),
            0,
            "invalid cache metadata should be ignored during rebuild"
        );

        lock(&store.get_calls).clear();
        lock(&store.range_calls).clear();
        let storage = UnifiedStorage::new(file_system, store.clone(), Some(rebuilt));

        let full = storage
            .read_all(&StorageSource::remote_object(full_key))
            .await
            .expect("reload full object after invalid metadata");
        let range = storage
            .read_range(&StorageSource::remote_object(range_key), 2..6)
            .await
            .expect("reload range after invalid metadata");

        assert_eq!(full, b"hello invalid cache");
        assert_eq!(range, b"cdef");
        assert_eq!(store.get_count(), 1);
        assert_eq!(store.range_calls(), vec![(range_key.to_string(), 0, 8)]);

        cleanup(&cache_root);
        cleanup(&object_root);
    }

    #[tokio::test]
    async fn range_reads_coalesce_nearby_windows_into_aligned_segment_fetches() {
        let cache_root = unique_test_dir("range-reads");
        let object_root = unique_test_dir("range-reads-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(CountingObjectStore::new(inner_store));
        let cache = Arc::new(
            RemoteCache::open_with_config(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
                test_cache_config(8, 1024 * 1024, 0),
            )
            .await
            .expect("open cache"),
        );
        let storage = UnifiedStorage::new(file_system, store.clone(), Some(cache));
        let key = "backup/sst/table-000001/0000/SST-000777.sst";
        store
            .put(key, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .expect("seed object store");
        let source = StorageSource::remote_object(key);

        let stitched = storage
            .read_ranges(&source, &[2..5, 10..13, 20..26])
            .await
            .expect("stitched range read");

        assert_eq!(stitched, b"cdeklmuvwxyz");
        assert_eq!(store.range_calls(), vec![(key.to_string(), 0, 32)]);

        cleanup(&cache_root);
        cleanup(&object_root);
    }

    #[tokio::test]
    async fn overlapping_range_reads_reuse_aligned_segments() {
        let cache_root = unique_test_dir("overlapping-segment-reuse");
        let object_root = unique_test_dir("overlapping-segment-reuse-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(CountingObjectStore::new(inner_store));
        let cache = Arc::new(
            RemoteCache::open_with_config(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
                test_cache_config(8, 1024 * 1024, 0),
            )
            .await
            .expect("open cache"),
        );
        let storage = UnifiedStorage::new(file_system, store.clone(), Some(cache));
        let key = "cold/table-000001/0000/00000000000000000001-00000000000000000009/SST-000321.sst";
        store
            .put(key, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .expect("seed object store");
        let source = StorageSource::remote_object(key);

        let first = storage
            .read_range(&source, 2..6)
            .await
            .expect("first range read");
        let second = storage
            .read_range(&source, 4..7)
            .await
            .expect("second overlapping read");

        assert_eq!(first, b"cdef");
        assert_eq!(second, b"efg");
        assert_eq!(store.range_calls(), vec![(key.to_string(), 0, 8)]);

        cleanup(&cache_root);
        cleanup(&object_root);
    }

    #[tokio::test]
    async fn downloader_election_prevents_duplicate_remote_segment_fetches() {
        let cache_root = unique_test_dir("segment-election");
        let object_root = unique_test_dir("segment-election-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(BlockingRangeObjectStore::new(inner_store));
        let cache = Arc::new(
            RemoteCache::open_with_config(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
                test_cache_config(8, 1024 * 1024, 0),
            )
            .await
            .expect("open cache"),
        );
        let mut progress = cache.subscribe_progress();
        let storage = Arc::new(UnifiedStorage::new(file_system, store.clone(), Some(cache)));
        let key = "backup/sst/table-000001/0000/SST-000654.sst";
        store
            .put(key, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .expect("seed object store");
        let source = StorageSource::remote_object(key);

        let storage_a = storage.clone();
        let source_a = source.clone();
        let first = tokio::spawn(async move { storage_a.read_range(&source_a, 2..6).await });
        store.wait_for_range_calls(1).await;

        let storage_b = storage.clone();
        let source_b = source.clone();
        let second = tokio::spawn(async move { storage_b.read_range(&source_b, 2..6).await });
        let _ = progress
            .wait_for(|snapshot| {
                snapshot.in_flight.iter().any(|entry| {
                    entry.object_key == key
                        && entry.span == CacheSpan::Range { start: 0, end: 8 }
                        && entry.fetch_kind == RemoteCacheFetchKind::Demand
                        && entry.waiter_count >= 1
                })
            })
            .await
            .expect("wait for duplicate download waiter");
        assert_eq!(store.range_calls(), vec![(key.to_string(), 0, 8)]);

        store.release();
        assert_eq!(
            first.await.expect("join first").expect("first read"),
            b"cdef"
        );
        assert_eq!(
            second.await.expect("join second").expect("second read"),
            b"cdef"
        );
        assert_eq!(store.range_calls(), vec![(key.to_string(), 0, 8)]);

        cleanup(&cache_root);
        cleanup(&object_root);
    }

    #[tokio::test]
    async fn background_prefetch_completes_next_segment_within_budget() {
        let cache_root = unique_test_dir("background-prefetch");
        let object_root = unique_test_dir("background-prefetch-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(CountingObjectStore::new(inner_store));
        let cache = Arc::new(
            RemoteCache::open_with_config(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
                test_cache_config(8, 1024 * 1024, 8),
            )
            .await
            .expect("open cache"),
        );
        let storage = UnifiedStorage::new(file_system, store.clone(), Some(cache.clone()));
        let mut progress = cache.subscribe_progress();
        let key = "backup/sst/table-000001/0000/SST-000888.sst";
        store
            .put(key, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .expect("seed object store");
        let source = StorageSource::remote_object(key);

        let first = storage.read_range(&source, 2..6).await.expect("first read");
        assert_eq!(first, b"cdef");

        let in_flight = progress
            .wait_for(|snapshot| {
                snapshot.in_flight.iter().any(|entry| {
                    entry.object_key == key
                        && entry.span == CacheSpan::Range { start: 8, end: 16 }
                        && entry.fetch_kind == RemoteCacheFetchKind::Prefetch
                })
            })
            .await
            .expect("background prefetch progress");
        assert!(in_flight.in_flight.iter().any(|entry| {
            entry.object_key == key
                && entry.span == CacheSpan::Range { start: 8, end: 16 }
                && entry.fetch_kind == RemoteCacheFetchKind::Prefetch
        }));
        let warmed = progress
            .wait_for(|snapshot| {
                snapshot.entries.iter().any(|entry| {
                    entry.object_key == key && entry.span == CacheSpan::Range { start: 8, end: 16 }
                })
            })
            .await
            .expect("background prefetch warmed entry");
        assert!(
            warmed.entries.iter().any(|entry| {
                entry.object_key == key && entry.span == CacheSpan::Range { start: 8, end: 16 }
            }),
            "background prefetch should publish the warmed segment once stored"
        );
        assert!(
            cache.entry_count() >= 2,
            "background prefetch should warm the next segment"
        );
        assert!(
            cache
                .read_range(key, 8..12)
                .await
                .expect("prefetch lookup")
                .is_some()
        );

        let second = storage
            .read_range(&source, 8..12)
            .await
            .expect("prefetched read");
        assert_eq!(second, b"ijkl");
        assert_eq!(
            store.range_calls(),
            vec![(key.to_string(), 0, 8), (key.to_string(), 8, 16)]
        );

        cleanup(&cache_root);
        cleanup(&object_root);
    }

    #[tokio::test]
    async fn rebuild_discards_orphaned_segment_files() {
        let cache_root = unique_test_dir("orphaned-segments");
        let file_system = Arc::new(TokioFileSystem::new());
        let root = cache_root.to_string_lossy().into_owned();
        let cache = RemoteCache::open_with_config(
            file_system.clone(),
            root.clone(),
            test_cache_config(8, 1024 * 1024, 0),
        )
        .await
        .expect("open cache");

        let orphan_data_path = join_fs_path(&cache.data_dir, "orphan.bin");
        write_bytes_atomic(file_system.as_ref(), &orphan_data_path, b"orphan-bytes")
            .await
            .expect("write orphan data");

        let reopened = RemoteCache::open_with_config(
            file_system.clone(),
            root,
            test_cache_config(8, 1024 * 1024, 0),
        )
        .await
        .expect("reopen cache");

        assert_eq!(reopened.entry_count(), 0);
        assert!(
            file_system
                .list(&reopened.data_dir)
                .await
                .expect("list data dir")
                .into_iter()
                .all(|path| !path.ends_with("orphan.bin"))
        );

        cleanup(&cache_root);
    }

    #[tokio::test]
    async fn bounded_segment_cache_evicts_oldest_entries() {
        let cache_root = unique_test_dir("cache-eviction");
        let object_root = unique_test_dir("cache-eviction-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(CountingObjectStore::new(inner_store));
        let cache = Arc::new(
            RemoteCache::open_with_config(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
                test_cache_config(8, 8, 0),
            )
            .await
            .expect("open cache"),
        );
        let storage = UnifiedStorage::new(file_system, store.clone(), Some(cache));
        let key = "backup/sst/table-000001/0000/SST-000999.sst";
        store
            .put(key, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .expect("seed object store");
        let source = StorageSource::remote_object(key);

        let first = storage
            .read_range(&source, 0..4)
            .await
            .expect("first segment read");
        let second = storage
            .read_range(&source, 8..12)
            .await
            .expect("second segment read");
        let third = storage
            .read_range(&source, 0..4)
            .await
            .expect("reloaded first segment");

        assert_eq!(first, b"abcd");
        assert_eq!(second, b"ijkl");
        assert_eq!(third, b"abcd");
        assert_eq!(
            store.range_calls(),
            vec![
                (key.to_string(), 0, 8),
                (key.to_string(), 8, 16),
                (key.to_string(), 0, 8)
            ]
        );

        cleanup(&cache_root);
        cleanup(&object_root);
    }

    #[tokio::test]
    async fn remote_failures_are_classified_for_retry_and_recovery() {
        let file_system = Arc::new(TokioFileSystem::new());
        let simulated = Arc::new(SimulatedObjectStore::default());
        let storage = UnifiedStorage::new(file_system, simulated.clone(), None);

        simulated
            .put("backup/sst/table-000001/0000/SST-000001.sst", b"abcdef")
            .await
            .expect("seed object");
        simulated.inject_failure(ObjectStoreFailure::timeout(
            ObjectStoreOperation::GetRange,
            "backup/sst/table-000001/0000/SST-000001.sst",
        ));
        let timeout = storage
            .read_range(
                &StorageSource::remote_object("backup/sst/table-000001/0000/SST-000001.sst"),
                0..3,
            )
            .await
            .expect_err("timeout should be surfaced");
        let timeout = match timeout {
            UnifiedStorageError::Remote(error) => error,
            other => panic!("expected remote timeout, got {other:?}"),
        };
        assert_eq!(timeout.operation(), RemoteOperation::GetRange);
        assert_eq!(timeout.recovery_hint(), RemoteRecoveryHint::Retry);
        assert!(timeout.is_retryable());

        simulated.inject_failure(ObjectStoreFailure::stale_list("backup/manifest/"));
        let stale_list = storage
            .list_objects("backup/manifest/")
            .await
            .expect_err("stale list should be surfaced");
        assert_eq!(stale_list.operation(), RemoteOperation::List);
        assert_eq!(
            stale_list.recovery_hint(),
            RemoteRecoveryHint::RefreshListing
        );

        simulated.inject_failure(ObjectStoreFailure::partial_read(
            ObjectStoreOperation::Get,
            "backup/sst/table-000001/0000/SST-000001.sst",
        ));
        let partial = storage
            .read_all(&StorageSource::remote_object(
                "backup/sst/table-000001/0000/SST-000001.sst",
            ))
            .await
            .expect_err("partial read should fail closed");
        let partial = match partial {
            UnifiedStorageError::Remote(error) => error,
            other => panic!("expected remote corruption, got {other:?}"),
        };
        assert_eq!(partial.operation(), RemoteOperation::Get);
        assert_eq!(partial.recovery_hint(), RemoteRecoveryHint::FailClosed);
    }
}
