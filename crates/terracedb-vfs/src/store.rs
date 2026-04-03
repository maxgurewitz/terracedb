use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::watch;

use terracedb::{Clock, DbDependencies, LogCursor, Rng, SequenceNumber, Timestamp};

use crate::{
    ActivityEntry, ActivityId, ActivityKind, ActivityOptions, ActivityReceiver, ActivityStream,
    AllocatorKind, CreateOptions, DirEntry, DirEntryPlus, FileKind, InodeId, JsonValue,
    MkdirOptions, ReadOnlyToolRunStore, ReadOnlyVfsFileSystem, ReadOnlyVfsKvStore, Stats, ToolRun,
    ToolRunId, ToolRunStatus, ToolRunStore, VfsBatchOperation, VfsError, VfsFileSystem, VfsKvStore,
};

pub const VFS_FORMAT_VERSION: u8 = 1;
pub const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024;
pub const ROOT_INODE_ID: InodeId = InodeId::new(1);
const DEFAULT_ALLOCATOR_BLOCK_SIZE: u64 = 32;

const MAX_SYMLINK_DEPTH: usize = 40;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeConfig {
    pub volume_id: crate::VolumeId,
    pub chunk_size: Option<u32>,
    pub create_if_missing: bool,
}

impl VolumeConfig {
    pub fn new(volume_id: crate::VolumeId) -> Self {
        Self {
            volume_id,
            chunk_size: None,
            create_if_missing: false,
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: u32) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }

    pub fn with_create_if_missing(mut self, create_if_missing: bool) -> Self {
        self.create_if_missing = create_if_missing;
        self
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotOptions {
    pub durable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloneVolumeSource {
    pub volume_id: crate::VolumeId,
    pub durable: bool,
}

impl CloneVolumeSource {
    pub fn new(volume_id: crate::VolumeId) -> Self {
        Self {
            volume_id,
            durable: false,
        }
    }

    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverlayBaseDescriptor {
    pub volume_id: crate::VolumeId,
    pub sequence: SequenceNumber,
    pub durable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeInfo {
    pub volume_id: crate::VolumeId,
    pub chunk_size: u32,
    pub format_version: u8,
    pub root_inode: InodeId,
    pub created_at: Timestamp,
    pub overlay_base: Option<OverlayBaseDescriptor>,
}

#[async_trait]
pub trait VolumeStore: Send + Sync {
    async fn open_volume(&self, config: VolumeConfig) -> Result<Arc<dyn Volume>, VfsError>;
    async fn clone_volume(
        &self,
        source: CloneVolumeSource,
        target: VolumeConfig,
    ) -> Result<Arc<dyn Volume>, VfsError>;
    async fn create_overlay(
        &self,
        base: Arc<dyn VolumeSnapshot>,
        target: VolumeConfig,
    ) -> Result<Arc<dyn OverlayVolume>, VfsError>;
    async fn export_volume(&self, source: CloneVolumeSource) -> Result<VolumeExport, VfsError>;
    async fn import_volume(
        &self,
        export: VolumeExport,
        target: VolumeConfig,
    ) -> Result<Arc<dyn Volume>, VfsError>;
}

#[async_trait]
pub trait Volume: Send + Sync {
    fn info(&self) -> VolumeInfo;
    fn fs(&self) -> Arc<dyn VfsFileSystem>;
    fn kv(&self) -> Arc<dyn VfsKvStore>;
    fn tools(&self) -> Arc<dyn ToolRunStore>;
    async fn snapshot(&self, opts: SnapshotOptions) -> Result<Arc<dyn VolumeSnapshot>, VfsError>;
    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: ActivityOptions,
    ) -> Result<ActivityStream, VfsError>;
    fn subscribe_activity(&self, opts: ActivityOptions) -> ActivityReceiver;
    async fn flush(&self) -> Result<(), VfsError>;
}

pub trait OverlayVolume: Volume {
    fn base(&self) -> Arc<dyn VolumeSnapshot>;
}

pub trait VolumeSnapshot: Send + Sync + Any {
    fn volume_id(&self) -> crate::VolumeId;
    fn sequence(&self) -> SequenceNumber;
    fn durable(&self) -> bool;
    fn info(&self) -> VolumeInfo;
    fn fs(&self) -> Arc<dyn ReadOnlyVfsFileSystem>;
    fn kv(&self) -> Arc<dyn ReadOnlyVfsKvStore>;
    fn tools(&self) -> Arc<dyn ReadOnlyToolRunStore>;
}

#[derive(Clone)]
pub struct VolumeExport {
    snapshot: SnapshotState,
}

impl VolumeExport {
    pub fn volume_id(&self) -> crate::VolumeId {
        self.snapshot.info.volume_id
    }

    pub fn sequence(&self) -> SequenceNumber {
        self.snapshot.sequence
    }

    pub fn durable(&self) -> bool {
        self.snapshot.durable
    }
}

#[derive(Clone)]
pub struct InMemoryVfsStore {
    inner: Arc<InMemoryStoreInner>,
}

struct InMemoryStoreInner {
    clock: Arc<dyn Clock>,
    _rng: Arc<dyn Rng>,
    allocator_block_size: u64,
    volumes: Mutex<BTreeMap<crate::VolumeId, StoredVolume>>,
}

impl InMemoryVfsStore {
    pub fn new(clock: Arc<dyn Clock>, rng: Arc<dyn Rng>) -> Self {
        Self {
            inner: Arc::new(InMemoryStoreInner {
                clock,
                _rng: rng,
                allocator_block_size: DEFAULT_ALLOCATOR_BLOCK_SIZE,
                volumes: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub fn with_dependencies(dependencies: DbDependencies) -> Self {
        Self::new(dependencies.clock, dependencies.rng)
    }
}

#[derive(Clone)]
enum StoredVolume {
    Regular(Arc<InMemoryVolumeInner>),
    Overlay(Arc<InMemoryOverlayEntry>),
}

impl StoredVolume {
    fn info(&self) -> VolumeInfo {
        match self {
            Self::Regular(volume) => volume.info(),
            Self::Overlay(overlay) => overlay.info(),
        }
    }

    fn open_handle(&self) -> Arc<dyn Volume> {
        match self {
            Self::Regular(volume) => Arc::new(InMemoryVolume {
                inner: volume.clone(),
            }),
            Self::Overlay(overlay) => Arc::new(InMemoryOverlayVolume {
                entry: overlay.clone(),
            }),
        }
    }

    fn snapshot_state(&self, durable: bool) -> SnapshotState {
        match self {
            Self::Regular(volume) => volume.snapshot_state(durable),
            Self::Overlay(overlay) => overlay.snapshot_state(durable),
        }
    }
}

#[derive(Clone)]
struct InMemoryOverlayEntry {
    delta: Arc<InMemoryVolumeInner>,
    base: Arc<SnapshotState>,
    snapshot_cache: Arc<Mutex<OverlaySnapshotCache>>,
}

#[derive(Default)]
struct OverlaySnapshotCache {
    visible: Option<CachedOverlaySnapshot>,
    durable: Option<CachedOverlaySnapshot>,
}

#[derive(Clone)]
struct CachedOverlaySnapshot {
    sequence: SequenceNumber,
    snapshot: SnapshotState,
}

impl InMemoryOverlayEntry {
    fn info(&self) -> VolumeInfo {
        self.delta.info()
    }

    fn snapshot_state(&self, durable: bool) -> SnapshotState {
        let state = self.delta.state.lock().expect("volume state lock poisoned");
        let sequence = if durable {
            state.durable.sequence
        } else {
            state.sequence
        };
        let mut cache = self
            .snapshot_cache
            .lock()
            .expect("overlay snapshot cache lock poisoned");
        let slot = if durable {
            &mut cache.durable
        } else {
            &mut cache.visible
        };
        if let Some(cached) = slot
            && cached.sequence == sequence
        {
            return cached.snapshot.clone();
        }
        let delta = if durable {
            state.durable.snapshot(state.info.clone())
        } else {
            state.visible_snapshot()
        };
        let snapshot = build_overlay_snapshot(&delta, self.base.as_ref());
        *slot = Some(CachedOverlaySnapshot {
            sequence,
            snapshot: snapshot.clone(),
        });
        snapshot
    }

    fn base_snapshot(&self) -> Arc<dyn VolumeSnapshot> {
        Arc::new(InMemoryVolumeSnapshot {
            state: self.base.clone(),
        })
    }
}

#[async_trait]
impl VolumeStore for InMemoryVfsStore {
    async fn open_volume(&self, config: VolumeConfig) -> Result<Arc<dyn Volume>, VfsError> {
        let chunk_size = configured_chunk_size(config.chunk_size)?;
        let mut volumes = self
            .inner
            .volumes
            .lock()
            .expect("in-memory volume registry lock poisoned");

        if let Some(existing) = volumes.get(&config.volume_id) {
            let info = existing.info();
            if info.chunk_size != chunk_size {
                return Err(VfsError::VolumeConfigMismatch {
                    volume_id: config.volume_id,
                    existing_chunk_size: info.chunk_size,
                    requested_chunk_size: chunk_size,
                });
            }

            return Ok(existing.open_handle());
        }

        if !config.create_if_missing {
            return Err(VfsError::VolumeNotFound {
                volume_id: config.volume_id,
            });
        }

        let info = VolumeInfo {
            volume_id: config.volume_id,
            chunk_size,
            format_version: VFS_FORMAT_VERSION,
            root_inode: ROOT_INODE_ID,
            created_at: self.inner.clock.now(),
            overlay_base: None,
        };
        let volume = Arc::new(InMemoryVolumeInner::new_empty(
            info,
            self.inner.clock.clone(),
            self.inner.allocator_block_size,
        ));
        volumes.insert(config.volume_id, StoredVolume::Regular(volume.clone()));
        Ok(Arc::new(InMemoryVolume { inner: volume }))
    }

    async fn clone_volume(
        &self,
        source: CloneVolumeSource,
        target: VolumeConfig,
    ) -> Result<Arc<dyn Volume>, VfsError> {
        let source_volume = {
            let volumes = self
                .inner
                .volumes
                .lock()
                .expect("in-memory volume registry lock poisoned");
            volumes
                .get(&source.volume_id)
                .cloned()
                .ok_or(VfsError::VolumeNotFound {
                    volume_id: source.volume_id,
                })?
        };

        let snapshot = source_volume.snapshot_state(source.durable);
        let chunk_size = target.chunk_size.unwrap_or(snapshot.info.chunk_size);
        if chunk_size == 0 {
            return Err(VfsError::InvalidChunkSize);
        }

        let target_info = VolumeInfo {
            volume_id: target.volume_id,
            chunk_size,
            format_version: VFS_FORMAT_VERSION,
            root_inode: ROOT_INODE_ID,
            created_at: self.inner.clock.now(),
            overlay_base: None,
        };

        let mut volumes = self
            .inner
            .volumes
            .lock()
            .expect("in-memory volume registry lock poisoned");
        if volumes.contains_key(&target.volume_id) {
            return Err(VfsError::VolumeAlreadyExists {
                volume_id: target.volume_id,
            });
        }

        let volume = Arc::new(InMemoryVolumeInner::from_snapshot(
            target_info,
            self.inner.clock.clone(),
            &snapshot,
            self.inner.allocator_block_size,
            true,
        ));
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "source_volume_id".to_string(),
            json!(source.volume_id.to_string()),
        );
        volume.append_activity(ActivityKind::VolumeCloned, None, None, metadata);
        volumes.insert(target.volume_id, StoredVolume::Regular(volume.clone()));
        Ok(Arc::new(InMemoryVolume { inner: volume }))
    }

    async fn create_overlay(
        &self,
        base: Arc<dyn VolumeSnapshot>,
        target: VolumeConfig,
    ) -> Result<Arc<dyn OverlayVolume>, VfsError> {
        let base_snapshot = (base.as_ref() as &dyn Any)
            .downcast_ref::<InMemoryVolumeSnapshot>()
            .ok_or(VfsError::IncompatibleSnapshotImplementation)?;
        let chunk_size = target
            .chunk_size
            .unwrap_or(base_snapshot.state.info.chunk_size);
        if chunk_size == 0 {
            return Err(VfsError::InvalidChunkSize);
        }

        let target_info = VolumeInfo {
            volume_id: target.volume_id,
            chunk_size,
            format_version: VFS_FORMAT_VERSION,
            root_inode: ROOT_INODE_ID,
            created_at: self.inner.clock.now(),
            overlay_base: Some(OverlayBaseDescriptor {
                volume_id: base_snapshot.state.info.volume_id,
                sequence: base_snapshot.state.sequence,
                durable: base_snapshot.state.durable,
            }),
        };

        let mut volumes = self
            .inner
            .volumes
            .lock()
            .expect("in-memory volume registry lock poisoned");
        if volumes.contains_key(&target.volume_id) {
            return Err(VfsError::VolumeAlreadyExists {
                volume_id: target.volume_id,
            });
        }

        let volume = Arc::new(InMemoryVolumeInner::new_overlay(
            target_info,
            self.inner.clock.clone(),
            &base_snapshot.state,
            self.inner.allocator_block_size,
        ));
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "base_volume_id".to_string(),
            json!(base_snapshot.state.info.volume_id.to_string()),
        );
        metadata.insert(
            "base_sequence".to_string(),
            json!(base_snapshot.state.sequence.get()),
        );
        volume.append_activity(ActivityKind::OverlayCreated, None, None, metadata);
        let overlay = Arc::new(InMemoryOverlayEntry {
            delta: volume,
            base: base_snapshot.state.clone(),
            snapshot_cache: Arc::new(Mutex::new(OverlaySnapshotCache::default())),
        });
        volumes.insert(target.volume_id, StoredVolume::Overlay(overlay.clone()));

        Ok(Arc::new(InMemoryOverlayVolume { entry: overlay }))
    }

    async fn export_volume(&self, source: CloneVolumeSource) -> Result<VolumeExport, VfsError> {
        let volume = {
            let volumes = self
                .inner
                .volumes
                .lock()
                .expect("in-memory volume registry lock poisoned");
            volumes
                .get(&source.volume_id)
                .cloned()
                .ok_or(VfsError::VolumeNotFound {
                    volume_id: source.volume_id,
                })?
        };

        Ok(VolumeExport {
            snapshot: volume.snapshot_state(source.durable),
        })
    }

    async fn import_volume(
        &self,
        export: VolumeExport,
        target: VolumeConfig,
    ) -> Result<Arc<dyn Volume>, VfsError> {
        let chunk_size = target.chunk_size.unwrap_or(export.snapshot.info.chunk_size);
        if chunk_size == 0 {
            return Err(VfsError::InvalidChunkSize);
        }

        let target_info = VolumeInfo {
            volume_id: target.volume_id,
            chunk_size,
            format_version: VFS_FORMAT_VERSION,
            root_inode: ROOT_INODE_ID,
            created_at: self.inner.clock.now(),
            overlay_base: None,
        };

        let mut volumes = self
            .inner
            .volumes
            .lock()
            .expect("in-memory volume registry lock poisoned");
        if volumes.contains_key(&target.volume_id) {
            return Err(VfsError::VolumeAlreadyExists {
                volume_id: target.volume_id,
            });
        }

        let volume = Arc::new(InMemoryVolumeInner::from_snapshot(
            target_info,
            self.inner.clock.clone(),
            &export.snapshot,
            self.inner.allocator_block_size,
            true,
        ));
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "source_volume_id".to_string(),
            json!(export.snapshot.info.volume_id.to_string()),
        );
        metadata.insert(
            "source_sequence".to_string(),
            json!(export.snapshot.sequence.get()),
        );
        metadata.insert("source_durable".to_string(), json!(export.snapshot.durable));
        volume.append_activity(ActivityKind::VolumeCloned, None, None, metadata);
        volumes.insert(target.volume_id, StoredVolume::Regular(volume.clone()));
        Ok(Arc::new(InMemoryVolume { inner: volume }))
    }
}

#[derive(Clone)]
struct InMemoryVolume {
    inner: Arc<InMemoryVolumeInner>,
}

#[async_trait]
impl Volume for InMemoryVolume {
    fn info(&self) -> VolumeInfo {
        self.inner.info()
    }

    fn fs(&self) -> Arc<dyn VfsFileSystem> {
        Arc::new(InMemoryVfsFileSystem {
            volume: self.inner.clone(),
        })
    }

    fn kv(&self) -> Arc<dyn VfsKvStore> {
        Arc::new(InMemoryVfsKv {
            volume: self.inner.clone(),
        })
    }

    fn tools(&self) -> Arc<dyn ToolRunStore> {
        Arc::new(InMemoryToolRunStore {
            volume: self.inner.clone(),
        })
    }

    async fn snapshot(&self, opts: SnapshotOptions) -> Result<Arc<dyn VolumeSnapshot>, VfsError> {
        Ok(Arc::new(InMemoryVolumeSnapshot {
            state: Arc::new(self.inner.snapshot_state(opts.durable)),
        }))
    }

    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: ActivityOptions,
    ) -> Result<ActivityStream, VfsError> {
        let entries = {
            let state = self.inner.state.lock().expect("volume state lock poisoned");
            let activity_slice = if opts.durable {
                &state.activities[..state.durable.activity_len]
            } else {
                &state.activities[..]
            };
            let mut entries = activity_slice
                .iter()
                .filter(|entry| entry.cursor > cursor)
                .cloned()
                .collect::<Vec<_>>();
            if let Some(limit) = opts.limit {
                entries.truncate(limit);
            }
            entries
        };

        Ok(Box::pin(stream::iter(entries.into_iter().map(Ok))))
    }

    fn subscribe_activity(&self, opts: ActivityOptions) -> ActivityReceiver {
        if opts.durable {
            ActivityReceiver::new(self.inner.durable_activity_watch.subscribe())
        } else {
            ActivityReceiver::new(self.inner.activity_watch.subscribe())
        }
    }

    async fn flush(&self) -> Result<(), VfsError> {
        self.inner.promote_durable_cut();
        Ok(())
    }
}

struct InMemoryOverlayVolume {
    entry: Arc<InMemoryOverlayEntry>,
}

#[async_trait]
impl Volume for InMemoryOverlayVolume {
    fn info(&self) -> VolumeInfo {
        self.entry.info()
    }

    fn fs(&self) -> Arc<dyn VfsFileSystem> {
        Arc::new(InMemoryOverlayFileSystem {
            overlay: self.entry.clone(),
        })
    }

    fn kv(&self) -> Arc<dyn VfsKvStore> {
        Arc::new(InMemoryVfsKv {
            volume: self.entry.delta.clone(),
        })
    }

    fn tools(&self) -> Arc<dyn ToolRunStore> {
        Arc::new(InMemoryToolRunStore {
            volume: self.entry.delta.clone(),
        })
    }

    async fn snapshot(&self, opts: SnapshotOptions) -> Result<Arc<dyn VolumeSnapshot>, VfsError> {
        Ok(Arc::new(InMemoryVolumeSnapshot {
            state: Arc::new(self.entry.snapshot_state(opts.durable)),
        }))
    }

    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: ActivityOptions,
    ) -> Result<ActivityStream, VfsError> {
        InMemoryVolume {
            inner: self.entry.delta.clone(),
        }
        .activity_since(cursor, opts)
        .await
    }

    fn subscribe_activity(&self, opts: ActivityOptions) -> ActivityReceiver {
        InMemoryVolume {
            inner: self.entry.delta.clone(),
        }
        .subscribe_activity(opts)
    }

    async fn flush(&self) -> Result<(), VfsError> {
        InMemoryVolume {
            inner: self.entry.delta.clone(),
        }
        .flush()
        .await
    }
}

impl OverlayVolume for InMemoryOverlayVolume {
    fn base(&self) -> Arc<dyn VolumeSnapshot> {
        self.entry.base_snapshot()
    }
}

struct InMemoryVolumeSnapshot {
    state: Arc<SnapshotState>,
}

impl VolumeSnapshot for InMemoryVolumeSnapshot {
    fn volume_id(&self) -> crate::VolumeId {
        self.state.info.volume_id
    }

    fn sequence(&self) -> SequenceNumber {
        self.state.sequence
    }

    fn durable(&self) -> bool {
        self.state.durable
    }

    fn info(&self) -> VolumeInfo {
        self.state.info.clone()
    }

    fn fs(&self) -> Arc<dyn ReadOnlyVfsFileSystem> {
        Arc::new(InMemorySnapshotFileSystem {
            state: self.state.clone(),
        })
    }

    fn kv(&self) -> Arc<dyn ReadOnlyVfsKvStore> {
        Arc::new(InMemorySnapshotKv {
            state: self.state.clone(),
        })
    }

    fn tools(&self) -> Arc<dyn ReadOnlyToolRunStore> {
        Arc::new(InMemorySnapshotTools {
            state: self.state.clone(),
        })
    }
}

struct InMemoryVfsFileSystem {
    volume: Arc<InMemoryVolumeInner>,
}

struct InMemoryOverlayFileSystem {
    overlay: Arc<InMemoryOverlayEntry>,
}

struct InMemorySnapshotFileSystem {
    state: Arc<SnapshotState>,
}

struct InMemoryVfsKv {
    volume: Arc<InMemoryVolumeInner>,
}

struct InMemorySnapshotKv {
    state: Arc<SnapshotState>,
}

struct InMemoryToolRunStore {
    volume: Arc<InMemoryVolumeInner>,
}

struct InMemorySnapshotTools {
    state: Arc<SnapshotState>,
}

#[async_trait]
impl ReadOnlyVfsFileSystem for InMemoryVfsFileSystem {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        lookup_stats(&state, path, true)
    }

    async fn lstat(&self, path: &str) -> Result<Option<Stats>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        lookup_stats(&state, path, false)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_file_bytes(&state, path)
    }

    async fn pread(&self, path: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        pread_file_bytes(&state, path, offset, len)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_dir_entries(&state, path)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_dir_entries_plus(&state, path)
    }

    async fn readlink(&self, path: &str) -> Result<String, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_link_target(&state.paths, &state.inodes, path)
    }
}

#[async_trait]
impl ReadOnlyVfsFileSystem for InMemorySnapshotFileSystem {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, VfsError> {
        lookup_snapshot_stats(self.state.as_ref(), path, true)
    }

    async fn lstat(&self, path: &str) -> Result<Option<Stats>, VfsError> {
        lookup_snapshot_stats(self.state.as_ref(), path, false)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, VfsError> {
        read_snapshot_file_bytes(self.state.as_ref(), path)
    }

    async fn pread(&self, path: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, VfsError> {
        pread_snapshot_file_bytes(self.state.as_ref(), path, offset, len)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, VfsError> {
        read_snapshot_dir_entries(self.state.as_ref(), path)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, VfsError> {
        read_snapshot_dir_entries_plus(self.state.as_ref(), path)
    }

    async fn readlink(&self, path: &str) -> Result<String, VfsError> {
        read_link_target(&self.state.paths, &self.state.inodes, path)
    }
}

#[async_trait]
impl ReadOnlyVfsFileSystem for InMemoryOverlayFileSystem {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, VfsError> {
        lookup_snapshot_stats(&self.overlay.snapshot_state(false), path, true)
    }

    async fn lstat(&self, path: &str) -> Result<Option<Stats>, VfsError> {
        lookup_snapshot_stats(&self.overlay.snapshot_state(false), path, false)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, VfsError> {
        read_snapshot_file_bytes(&self.overlay.snapshot_state(false), path)
    }

    async fn pread(&self, path: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, VfsError> {
        pread_snapshot_file_bytes(&self.overlay.snapshot_state(false), path, offset, len)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, VfsError> {
        read_snapshot_dir_entries(&self.overlay.snapshot_state(false), path)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, VfsError> {
        read_snapshot_dir_entries_plus(&self.overlay.snapshot_state(false), path)
    }

    async fn readlink(&self, path: &str) -> Result<String, VfsError> {
        let snapshot = self.overlay.snapshot_state(false);
        read_link_target(&snapshot.paths, &snapshot.inodes, path)
    }
}

#[async_trait]
impl VfsFileSystem for InMemoryVfsFileSystem {
    async fn apply_batch(&self, ops: &[VfsBatchOperation]) -> Result<(), VfsError> {
        self.volume.mutate(|state, now| {
            apply_regular_batch_to_state(state, ops, now)?;
            Ok(None)
        })
    }

    async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        opts: CreateOptions,
    ) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let opts = opts.clone();
        self.volume.mutate(|state, now| {
            mutate_write_file_state(state, &path, &data, &opts, now)?;
            Ok(Some((
                (),
                activity_spec(ActivityKind::FileWritten, Some(path.clone()), None, None),
            )))
        })
    }

    async fn pwrite(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, now| {
            mutate_pwrite_state(state, &path, offset, &data, now)?;
            Ok(Some((
                (),
                activity_spec(ActivityKind::FilePatched, Some(path.clone()), None, None),
            )))
        })
    }

    async fn truncate(&self, path: &str, size: u64) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, now| {
            mutate_truncate_state(state, &path, size, now)?;
            Ok(Some((
                (),
                activity_spec(ActivityKind::FileTruncated, Some(path.clone()), None, None),
            )))
        })
    }

    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        if path == "/" {
            return if opts.recursive {
                Ok(())
            } else {
                Err(VfsError::AlreadyExists { path })
            };
        }

        let opts = opts.clone();
        self.volume.mutate(|state, now| {
            if mutate_mkdir_state(state, &path, &opts, now)? {
                Ok(Some((
                    (),
                    activity_spec(
                        ActivityKind::DirectoryCreated,
                        Some(path.clone()),
                        None,
                        None,
                    ),
                )))
            } else {
                Ok(None)
            }
        })
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), VfsError> {
        let from = normalize_path(from)?;
        let to = normalize_path(to)?;
        self.volume.mutate(|state, now| {
            if !mutate_rename_state(state, &from, &to, now)? {
                return Ok(None);
            }
            let mut metadata = BTreeMap::new();
            metadata.insert("to".to_string(), json!(to.clone()));
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::PathRenamed,
                    Some(from.clone()),
                    None,
                    Some(metadata),
                ),
            )))
        })
    }

    async fn link(&self, from: &str, to: &str) -> Result<(), VfsError> {
        let from = normalize_path(from)?;
        let to = normalize_path(to)?;
        self.volume.mutate(|state, now| {
            mutate_link_state(state, &from, &to, now)?;
            let mut metadata = BTreeMap::new();
            metadata.insert("from".to_string(), json!(from.clone()));
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::HardLinkCreated,
                    Some(to.clone()),
                    None,
                    Some(metadata),
                ),
            )))
        })
    }

    async fn symlink(&self, target: &str, linkpath: &str) -> Result<(), VfsError> {
        let linkpath = normalize_path(linkpath)?;
        let target = target.to_string();
        self.volume.mutate(|state, now| {
            mutate_symlink_state(state, &target, &linkpath, now)?;
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::SymlinkCreated,
                    Some(linkpath.clone()),
                    None,
                    None,
                ),
            )))
        })
    }

    async fn unlink(&self, path: &str) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, now| {
            mutate_unlink_state(state, &path, now)?;
            Ok(Some((
                (),
                activity_spec(ActivityKind::PathDeleted, Some(path.clone()), None, None),
            )))
        })
    }

    async fn rmdir(&self, path: &str) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, now| {
            mutate_rmdir_state(state, &path, now)?;
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::DirectoryRemoved,
                    Some(path.clone()),
                    None,
                    None,
                ),
            )))
        })
    }

    async fn fsync(&self, path: Option<&str>) -> Result<(), VfsError> {
        if let Some(path) = path {
            let path = normalize_path(path)?;
            let state = self
                .volume
                .state
                .lock()
                .expect("volume state lock poisoned");
            if resolve_existing_path(&state, &path, true)?.is_none() {
                return Err(VfsError::NotFound { path });
            }
        }

        self.volume.promote_durable_cut();
        Ok(())
    }
}

#[async_trait]
impl VfsFileSystem for InMemoryOverlayFileSystem {
    async fn apply_batch(&self, ops: &[VfsBatchOperation]) -> Result<(), VfsError> {
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            apply_overlay_batch_to_state(state, base.as_ref(), ops, now)?;
            Ok(None)
        })
    }

    async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        opts: CreateOptions,
    ) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let opts = opts.clone();
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            if let Some(resolved_path) =
                try_fast_overlay_new_target_path(state, base.as_ref(), &path)?
            {
                let delta_opts = CreateOptions {
                    create_parents: false,
                    ..opts.clone()
                };
                mutate_write_file_state(state, &resolved_path, &data, &delta_opts, now)?;
                return Ok(Some((
                    (),
                    activity_spec(ActivityKind::FileWritten, Some(path.clone()), None, None),
                )));
            }
            let merged = overlay_visible_snapshot(state, base.as_ref());
            let resolved_path = if let Some((resolved_path, _)) =
                resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, true)?
            {
                ensure_overlay_existing_path(
                    state,
                    base.as_ref(),
                    &merged,
                    &resolved_path,
                    false,
                    now,
                )?;
                resolved_path
            } else {
                materialize_overlay_parent_chain(
                    state,
                    base.as_ref(),
                    &merged,
                    &path,
                    opts.create_parents,
                    now,
                )?;
                resolve_target_path(state, &path)?
            };

            let delta_opts = CreateOptions {
                create_parents: false,
                ..opts.clone()
            };
            mutate_write_file_state(state, &resolved_path, &data, &delta_opts, now)?;
            Ok(Some((
                (),
                activity_spec(ActivityKind::FileWritten, Some(path.clone()), None, None),
            )))
        })
    }

    async fn pwrite(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            let merged = overlay_visible_snapshot(state, base.as_ref());
            let (resolved_path, _) =
                resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, true)?
                    .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
            ensure_overlay_existing_path(
                state,
                base.as_ref(),
                &merged,
                &resolved_path,
                false,
                now,
            )?;
            mutate_pwrite_state(state, &resolved_path, offset, &data, now)?;
            Ok(Some((
                (),
                activity_spec(ActivityKind::FilePatched, Some(path.clone()), None, None),
            )))
        })
    }

    async fn truncate(&self, path: &str, size: u64) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            let merged = overlay_visible_snapshot(state, base.as_ref());
            let (resolved_path, _) =
                resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, true)?
                    .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
            ensure_overlay_existing_path(
                state,
                base.as_ref(),
                &merged,
                &resolved_path,
                false,
                now,
            )?;
            mutate_truncate_state(state, &resolved_path, size, now)?;
            Ok(Some((
                (),
                activity_spec(ActivityKind::FileTruncated, Some(path.clone()), None, None),
            )))
        })
    }

    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        if path == "/" {
            return if opts.recursive {
                Ok(())
            } else {
                Err(VfsError::AlreadyExists { path })
            };
        }

        let opts = opts.clone();
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            if opts.recursive
                && let Some((resolved_path, inode_id)) = resolve_existing_path(state, &path, false)?
            {
                let inode = state
                    .inodes
                    .get(&inode_id)
                    .ok_or_else(|| VfsError::NotFound {
                        path: resolved_path.clone(),
                    })?;
                if inode.stats.kind == FileKind::Directory {
                    return Ok(None);
                }
            }
            if let Some(resolved_path) =
                try_fast_overlay_new_target_path(state, base.as_ref(), &path)?
            {
                let delta_opts = MkdirOptions {
                    recursive: false,
                    ..opts.clone()
                };
                mutate_mkdir_state(state, &resolved_path, &delta_opts, now)?;
                return Ok(Some((
                    (),
                    activity_spec(
                        ActivityKind::DirectoryCreated,
                        Some(path.clone()),
                        None,
                        None,
                    ),
                )));
            }
            let merged = overlay_visible_snapshot(state, base.as_ref());
            if let Some((resolved_path, inode_id)) =
                resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, false)?
            {
                let inode = merged
                    .inodes
                    .get(&inode_id)
                    .ok_or_else(|| VfsError::NotFound {
                        path: resolved_path.clone(),
                    })?;
                if inode.stats.kind == FileKind::Directory && opts.recursive {
                    return Ok(None);
                }
                return Err(VfsError::AlreadyExists {
                    path: resolved_path,
                });
            }

            materialize_overlay_parent_chain(
                state,
                base.as_ref(),
                &merged,
                &path,
                opts.recursive,
                now,
            )?;
            let resolved_path = resolve_target_path(state, &path)?;
            let delta_opts = MkdirOptions {
                recursive: false,
                ..opts.clone()
            };
            mutate_mkdir_state(state, &resolved_path, &delta_opts, now)?;
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::DirectoryCreated,
                    Some(path.clone()),
                    None,
                    None,
                ),
            )))
        })
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), VfsError> {
        let from = normalize_path(from)?;
        let to = normalize_path(to)?;
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            if from == "/" || to == "/" {
                return Err(VfsError::RootInvariant);
            }
            if from == to {
                return Ok(None);
            }

            let merged = overlay_visible_snapshot(state, base.as_ref());
            let (resolved_from, from_inode_id) =
                resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &from, false)?
                    .ok_or_else(|| VfsError::NotFound { path: from.clone() })?;
            let resolved_to = resolve_target_path_in_maps(&merged.paths, &merged.inodes, &to)?;
            if resolved_from == resolved_to {
                return Ok(None);
            }

            let from_kind = merged
                .inodes
                .get(&from_inode_id)
                .ok_or_else(|| VfsError::NotFound {
                    path: resolved_from.clone(),
                })?
                .stats
                .kind;

            let source_was_delta = state.paths.contains_key(&resolved_from);
            if !source_was_delta {
                if from_kind == FileKind::Directory {
                    copy_up_overlay_directory_subtree(
                        state,
                        base.as_ref(),
                        &merged,
                        &resolved_from,
                        now,
                    )?;
                } else {
                    ensure_overlay_existing_path(
                        state,
                        base.as_ref(),
                        &merged,
                        &resolved_from,
                        false,
                        now,
                    )?;
                }
            }

            let target_existing = resolve_existing_in_maps_with_mode(
                &merged.paths,
                &merged.inodes,
                &resolved_to,
                false,
            )?;
            let mut target_was_base = false;
            if let Some((target_path, target_inode_id)) = target_existing {
                target_was_base = !state.paths.contains_key(&target_path);
                let target_kind = merged
                    .inodes
                    .get(&target_inode_id)
                    .ok_or_else(|| VfsError::NotFound {
                        path: target_path.clone(),
                    })?
                    .stats
                    .kind;
                if from_kind == FileKind::Directory
                    && target_kind == FileKind::Directory
                    && merged.paths.keys().any(|candidate| {
                        candidate != &target_path && is_descendant_path(&target_path, candidate)
                    })
                {
                    return Err(VfsError::DirectoryNotEmpty { path: target_path });
                }
                if target_was_base {
                    ensure_overlay_existing_path(
                        state,
                        base.as_ref(),
                        &merged,
                        &target_path,
                        false,
                        now,
                    )?;
                }
            } else {
                materialize_overlay_parent_chain(
                    state,
                    base.as_ref(),
                    &merged,
                    &resolved_to,
                    false,
                    now,
                )?;
            }

            if !mutate_rename_state(state, &resolved_from, &resolved_to, now)? {
                return Ok(None);
            }
            if !source_was_delta {
                state.whiteouts.insert(resolved_from.clone());
            }
            if target_was_base {
                state.whiteouts.insert(resolved_to.clone());
            }

            let mut metadata = BTreeMap::new();
            metadata.insert("to".to_string(), json!(to.clone()));
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::PathRenamed,
                    Some(from.clone()),
                    None,
                    Some(metadata),
                ),
            )))
        })
    }

    async fn link(&self, from: &str, to: &str) -> Result<(), VfsError> {
        let from = normalize_path(from)?;
        let to = normalize_path(to)?;
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            let merged = overlay_visible_snapshot(state, base.as_ref());
            let (resolved_from, _) =
                resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &from, false)?
                    .ok_or_else(|| VfsError::NotFound { path: from.clone() })?;
            ensure_overlay_existing_path(
                state,
                base.as_ref(),
                &merged,
                &resolved_from,
                false,
                now,
            )?;
            let resolved_to = resolve_target_path_in_maps(&merged.paths, &merged.inodes, &to)?;
            materialize_overlay_parent_chain(
                state,
                base.as_ref(),
                &merged,
                &resolved_to,
                false,
                now,
            )?;
            mutate_link_state(state, &resolved_from, &resolved_to, now)?;
            let mut metadata = BTreeMap::new();
            metadata.insert("from".to_string(), json!(from.clone()));
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::HardLinkCreated,
                    Some(to.clone()),
                    None,
                    Some(metadata),
                ),
            )))
        })
    }

    async fn symlink(&self, target: &str, linkpath: &str) -> Result<(), VfsError> {
        let linkpath = normalize_path(linkpath)?;
        let target = target.to_string();
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            if let Some(resolved_linkpath) =
                try_fast_overlay_new_target_path(state, base.as_ref(), &linkpath)?
            {
                mutate_symlink_state(state, &target, &resolved_linkpath, now)?;
                return Ok(Some((
                    (),
                    activity_spec(
                        ActivityKind::SymlinkCreated,
                        Some(linkpath.clone()),
                        None,
                        None,
                    ),
                )));
            }
            let merged = overlay_visible_snapshot(state, base.as_ref());
            materialize_overlay_parent_chain(state, base.as_ref(), &merged, &linkpath, false, now)?;
            let resolved_linkpath = resolve_target_path(state, &linkpath)?;
            mutate_symlink_state(state, &target, &resolved_linkpath, now)?;
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::SymlinkCreated,
                    Some(linkpath.clone()),
                    None,
                    None,
                ),
            )))
        })
    }

    async fn unlink(&self, path: &str) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            let merged = overlay_visible_snapshot(state, base.as_ref());
            let (resolved_path, _) =
                resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, false)?
                    .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
            let source_was_delta = state.paths.contains_key(&resolved_path);
            if !source_was_delta {
                ensure_overlay_existing_path(
                    state,
                    base.as_ref(),
                    &merged,
                    &resolved_path,
                    false,
                    now,
                )?;
            }
            mutate_unlink_state(state, &resolved_path, now)?;
            if !source_was_delta {
                state.whiteouts.insert(resolved_path);
            }
            Ok(Some((
                (),
                activity_spec(ActivityKind::PathDeleted, Some(path.clone()), None, None),
            )))
        })
    }

    async fn rmdir(&self, path: &str) -> Result<(), VfsError> {
        let path = normalize_path(path)?;
        let base = self.overlay.base.clone();
        self.overlay.delta.mutate(|state, now| {
            let merged = overlay_visible_snapshot(state, base.as_ref());
            let (resolved_path, _) =
                resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, false)?
                    .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
            let source_was_delta = state.paths.contains_key(&resolved_path);
            if !source_was_delta {
                ensure_overlay_existing_path(
                    state,
                    base.as_ref(),
                    &merged,
                    &resolved_path,
                    false,
                    now,
                )?;
            }
            mutate_rmdir_state(state, &resolved_path, now)?;
            if !source_was_delta {
                state.whiteouts.insert(resolved_path);
            }
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::DirectoryRemoved,
                    Some(path.clone()),
                    None,
                    None,
                ),
            )))
        })
    }

    async fn fsync(&self, path: Option<&str>) -> Result<(), VfsError> {
        if let Some(path) = path {
            let path = normalize_path(path)?;
            if resolve_existing_in_maps_with_mode(
                &self.overlay.snapshot_state(false).paths,
                &self.overlay.snapshot_state(false).inodes,
                &path,
                true,
            )?
            .is_none()
            {
                return Err(VfsError::NotFound { path });
            }
        }

        self.overlay.delta.promote_durable_cut();
        Ok(())
    }
}

#[async_trait]
impl ReadOnlyVfsKvStore for InMemoryVfsKv {
    async fn get_json(&self, key: &str) -> Result<Option<JsonValue>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        Ok(state.kv.get(key).cloned())
    }

    async fn list_keys(&self) -> Result<Vec<String>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        Ok(state.kv.keys().cloned().collect())
    }
}

#[async_trait]
impl ReadOnlyVfsKvStore for InMemorySnapshotKv {
    async fn get_json(&self, key: &str) -> Result<Option<JsonValue>, VfsError> {
        Ok(self.state.kv.get(key).cloned())
    }

    async fn list_keys(&self) -> Result<Vec<String>, VfsError> {
        Ok(self.state.kv.keys().cloned().collect())
    }
}

#[async_trait]
impl VfsKvStore for InMemoryVfsKv {
    async fn set_json(&self, key: &str, value: JsonValue) -> Result<(), VfsError> {
        let key = key.to_string();
        self.volume.mutate(|state, _now| {
            state.kv.insert(key.clone(), value);
            Ok(Some((
                (),
                activity_spec(ActivityKind::KvSet, Some(key), None, None),
            )))
        })
    }

    async fn delete(&self, key: &str) -> Result<(), VfsError> {
        let key = key.to_string();
        self.volume.mutate(|state, _now| {
            state.kv.remove(&key);
            Ok(Some((
                (),
                activity_spec(ActivityKind::KvDeleted, Some(key), None, None),
            )))
        })
    }
}

#[async_trait]
impl ReadOnlyToolRunStore for InMemoryToolRunStore {
    async fn get(&self, id: ToolRunId) -> Result<Option<ToolRun>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        Ok(state.tool_runs.get(&id).cloned())
    }

    async fn recent(&self, limit: Option<usize>) -> Result<Vec<ToolRun>, VfsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        Ok(recent_tool_runs(&state.tool_runs, limit))
    }
}

#[async_trait]
impl ReadOnlyToolRunStore for InMemorySnapshotTools {
    async fn get(&self, id: ToolRunId) -> Result<Option<ToolRun>, VfsError> {
        Ok(self.state.tool_runs.get(&id).cloned())
    }

    async fn recent(&self, limit: Option<usize>) -> Result<Vec<ToolRun>, VfsError> {
        Ok(recent_tool_runs(&self.state.tool_runs, limit))
    }
}

#[async_trait]
impl ToolRunStore for InMemoryToolRunStore {
    async fn start(&self, name: &str, params: Option<JsonValue>) -> Result<ToolRunId, VfsError> {
        let name = name.to_string();
        self.volume.mutate(|state, now| {
            let tool_run_id = allocate_tool_run_id(state);
            let run = ToolRun {
                id: tool_run_id,
                name: name.clone(),
                status: ToolRunStatus::Pending,
                params,
                result: None,
                error: None,
                started_at: now,
                completed_at: None,
            };
            state.tool_runs.insert(tool_run_id, run);
            Ok(Some((
                tool_run_id,
                activity_spec(
                    ActivityKind::ToolStarted,
                    Some(name),
                    Some(tool_run_id),
                    Some(tool_metadata(tool_run_id)),
                ),
            )))
        })
    }

    async fn success(&self, id: ToolRunId, result: Option<JsonValue>) -> Result<(), VfsError> {
        self.volume.mutate(|state, now| {
            let run = state
                .tool_runs
                .get_mut(&id)
                .ok_or(VfsError::ToolRunNotFound { tool_run_id: id })?;
            if run.status != ToolRunStatus::Pending {
                return Err(VfsError::ToolRunAlreadyCompleted { tool_run_id: id });
            }
            run.status = ToolRunStatus::Success;
            run.result = result;
            run.completed_at = Some(now);
            let mut metadata = tool_metadata(id);
            metadata.insert("name".to_string(), json!(run.name.clone()));
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::ToolSucceeded,
                    Some(run.name.clone()),
                    Some(id),
                    Some(metadata),
                ),
            )))
        })
    }

    async fn error(&self, id: ToolRunId, message: String) -> Result<(), VfsError> {
        self.volume.mutate(|state, now| {
            let run = state
                .tool_runs
                .get_mut(&id)
                .ok_or(VfsError::ToolRunNotFound { tool_run_id: id })?;
            if run.status != ToolRunStatus::Pending {
                return Err(VfsError::ToolRunAlreadyCompleted { tool_run_id: id });
            }
            run.status = ToolRunStatus::Error;
            run.error = Some(message.clone());
            run.completed_at = Some(now);
            let mut metadata = tool_metadata(id);
            metadata.insert("name".to_string(), json!(run.name.clone()));
            metadata.insert("error".to_string(), json!(message));
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::ToolFailed,
                    Some(run.name.clone()),
                    Some(id),
                    Some(metadata),
                ),
            )))
        })
    }

    async fn record_completed(
        &self,
        input: crate::CompletedToolRun,
    ) -> Result<ToolRunId, VfsError> {
        self.volume.mutate(|state, now| {
            let tool_run_id = allocate_tool_run_id(state);
            let (status, result, error, activity_kind) = match input.outcome.clone() {
                crate::CompletedToolRunOutcome::Success { result } => (
                    ToolRunStatus::Success,
                    result,
                    None,
                    ActivityKind::ToolSucceeded,
                ),
                crate::CompletedToolRunOutcome::Error { message } => (
                    ToolRunStatus::Error,
                    None,
                    Some(message),
                    ActivityKind::ToolFailed,
                ),
            };
            let run = ToolRun {
                id: tool_run_id,
                name: input.name.clone(),
                status,
                params: input.params,
                result,
                error: error.clone(),
                started_at: now,
                completed_at: Some(now),
            };
            state.tool_runs.insert(tool_run_id, run);

            let mut metadata = tool_metadata(tool_run_id);
            if let Some(error) = error {
                metadata.insert("error".to_string(), json!(error));
            }
            Ok(Some((
                tool_run_id,
                activity_spec(
                    activity_kind,
                    Some(input.name),
                    Some(tool_run_id),
                    Some(metadata),
                ),
            )))
        })
    }
}

struct InMemoryVolumeInner {
    clock: Arc<dyn Clock>,
    state: Mutex<VolumeState>,
    activity_watch: watch::Sender<SequenceNumber>,
    durable_activity_watch: watch::Sender<SequenceNumber>,
}

impl InMemoryVolumeInner {
    fn new_empty(info: VolumeInfo, clock: Arc<dyn Clock>, allocator_block_size: u64) -> Self {
        let state = VolumeState::new(info, allocator_block_size);
        let (activity_watch, _receiver) = watch::channel(state.sequence);
        let (durable_activity_watch, _receiver) = watch::channel(state.durable.sequence);
        Self {
            clock,
            state: Mutex::new(state),
            activity_watch,
            durable_activity_watch,
        }
    }

    fn from_snapshot(
        info: VolumeInfo,
        clock: Arc<dyn Clock>,
        snapshot: &SnapshotState,
        allocator_block_size: u64,
        initialize_durable: bool,
    ) -> Self {
        let state =
            VolumeState::from_snapshot(info, snapshot, allocator_block_size, initialize_durable);
        let (activity_watch, _receiver) = watch::channel(state.sequence);
        let (durable_activity_watch, _receiver) = watch::channel(state.durable.sequence);
        Self {
            clock,
            state: Mutex::new(state),
            activity_watch,
            durable_activity_watch,
        }
    }

    fn new_overlay(
        info: VolumeInfo,
        clock: Arc<dyn Clock>,
        base: &SnapshotState,
        allocator_block_size: u64,
    ) -> Self {
        let state = VolumeState::new_overlay(info, base, allocator_block_size);
        let (activity_watch, _receiver) = watch::channel(state.sequence);
        let (durable_activity_watch, _receiver) = watch::channel(state.durable.sequence);
        Self {
            clock,
            state: Mutex::new(state),
            activity_watch,
            durable_activity_watch,
        }
    }

    fn info(&self) -> VolumeInfo {
        self.state
            .lock()
            .expect("volume state lock poisoned")
            .info
            .clone()
    }

    fn snapshot_state(&self, durable: bool) -> SnapshotState {
        let state = self.state.lock().expect("volume state lock poisoned");
        if durable {
            state.durable.snapshot(state.info.clone())
        } else {
            state.visible_snapshot()
        }
    }

    fn append_activity(
        &self,
        kind: ActivityKind,
        subject: Option<String>,
        tool_run_id: Option<ToolRunId>,
        metadata: BTreeMap<String, JsonValue>,
    ) {
        let (next_sequence, durable_sequence) = {
            let mut state = self.state.lock().expect("volume state lock poisoned");
            append_activity(
                &mut state,
                self.clock.now(),
                kind,
                subject,
                tool_run_id,
                metadata,
            );
            (state.sequence, state.durable.sequence)
        };
        self.activity_watch.send_replace(next_sequence);
        self.durable_activity_watch.send_replace(durable_sequence);
    }

    fn mutate<T, F>(&self, f: F) -> Result<T, VfsError>
    where
        F: FnOnce(&mut VolumeState, Timestamp) -> Result<Option<(T, ActivitySpec)>, VfsError>,
        T: Default,
    {
        let now = self.clock.now();
        let (result, next_sequence, durable_sequence) = {
            let mut state = self.state.lock().expect("volume state lock poisoned");
            let change = f(&mut state, now)?;
            let result = if let Some((result, activity)) = change {
                append_activity(
                    &mut state,
                    now,
                    activity.kind,
                    activity.subject,
                    activity.tool_run_id,
                    activity.metadata,
                );
                result
            } else {
                T::default()
            };
            (result, state.sequence, state.durable.sequence)
        };
        self.activity_watch.send_replace(next_sequence);
        self.durable_activity_watch.send_replace(durable_sequence);
        Ok(result)
    }

    fn promote_durable_cut(&self) {
        let (visible_sequence, durable_sequence) = {
            let mut state = self.state.lock().expect("volume state lock poisoned");
            state.promote_durable_cut();
            (state.sequence, state.durable.sequence)
        };
        self.activity_watch.send_replace(visible_sequence);
        self.durable_activity_watch.send_replace(durable_sequence);
    }
}

#[derive(Clone)]
struct SnapshotState {
    info: VolumeInfo,
    sequence: SequenceNumber,
    durable: bool,
    paths: BTreeMap<String, InodeId>,
    inodes: BTreeMap<InodeId, InodeRecord>,
    kv: BTreeMap<String, JsonValue>,
    tool_runs: BTreeMap<ToolRunId, ToolRun>,
    whiteouts: BTreeSet<String>,
    origins: BTreeMap<InodeId, OriginRecord>,
}

#[derive(Clone, Default)]
struct DurableViewState {
    sequence: SequenceNumber,
    paths: BTreeMap<String, InodeId>,
    inodes: BTreeMap<InodeId, InodeRecord>,
    kv: BTreeMap<String, JsonValue>,
    tool_runs: BTreeMap<ToolRunId, ToolRun>,
    whiteouts: BTreeSet<String>,
    origins: BTreeMap<InodeId, OriginRecord>,
    activity_len: usize,
}

impl DurableViewState {
    fn snapshot(&self, info: VolumeInfo) -> SnapshotState {
        SnapshotState {
            info,
            sequence: self.sequence,
            durable: true,
            paths: self.paths.clone(),
            inodes: self.inodes.clone(),
            kv: self.kv.clone(),
            tool_runs: self.tool_runs.clone(),
            whiteouts: self.whiteouts.clone(),
            origins: self.origins.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OriginRecord {
    base_volume_id: crate::VolumeId,
    base_sequence: SequenceNumber,
    base_durable: bool,
    base_inode: InodeId,
}

struct VolumeState {
    info: VolumeInfo,
    sequence: SequenceNumber,
    allocators: BTreeMap<AllocatorKind, BlockLeaseAllocator>,
    paths: BTreeMap<String, InodeId>,
    inodes: BTreeMap<InodeId, InodeRecord>,
    kv: BTreeMap<String, JsonValue>,
    tool_runs: BTreeMap<ToolRunId, ToolRun>,
    whiteouts: BTreeSet<String>,
    origins: BTreeMap<InodeId, OriginRecord>,
    activities: Vec<ActivityEntry>,
    durable: DurableViewState,
}

impl VolumeState {
    fn new(info: VolumeInfo, allocator_block_size: u64) -> Self {
        let root = InodeRecord {
            stats: inode_stats(
                ROOT_INODE_ID,
                FileKind::Directory,
                0o755,
                info.created_at,
                0,
            ),
            data: InodeData::Directory,
        };
        let mut paths = BTreeMap::new();
        paths.insert("/".to_string(), ROOT_INODE_ID);
        let mut inodes = BTreeMap::new();
        inodes.insert(ROOT_INODE_ID, root);
        let durable = DurableViewState {
            sequence: SequenceNumber::new(0),
            paths: paths.clone(),
            inodes: inodes.clone(),
            kv: BTreeMap::new(),
            tool_runs: BTreeMap::new(),
            whiteouts: BTreeSet::new(),
            origins: BTreeMap::new(),
            activity_len: 0,
        };

        Self {
            info,
            sequence: SequenceNumber::new(0),
            allocators: allocator_map(ROOT_INODE_ID.get() + 1, 1, 1, allocator_block_size),
            paths,
            inodes,
            kv: BTreeMap::new(),
            tool_runs: BTreeMap::new(),
            whiteouts: BTreeSet::new(),
            origins: BTreeMap::new(),
            activities: Vec::new(),
            durable,
        }
    }

    fn from_snapshot(
        info: VolumeInfo,
        snapshot: &SnapshotState,
        allocator_block_size: u64,
        initialize_durable: bool,
    ) -> Self {
        let inodes = clone_inodes_for_chunk_size(
            &snapshot.inodes,
            snapshot.info.chunk_size,
            info.chunk_size,
        );
        let next_inode = snapshot
            .inodes
            .keys()
            .map(|inode| inode.get())
            .max()
            .unwrap_or(ROOT_INODE_ID.get())
            + 1;
        let next_tool_run_id = snapshot
            .tool_runs
            .keys()
            .map(|tool_run_id| tool_run_id.get())
            .max()
            .unwrap_or(0)
            + 1;
        let durable = if initialize_durable {
            DurableViewState {
                sequence: SequenceNumber::new(0),
                paths: snapshot.paths.clone(),
                inodes: inodes.clone(),
                kv: snapshot.kv.clone(),
                tool_runs: snapshot.tool_runs.clone(),
                whiteouts: BTreeSet::new(),
                origins: BTreeMap::new(),
                activity_len: 0,
            }
        } else {
            DurableViewState::default()
        };

        Self {
            info,
            sequence: SequenceNumber::new(0),
            allocators: allocator_map(next_inode, 1, next_tool_run_id, allocator_block_size),
            paths: snapshot.paths.clone(),
            inodes,
            kv: snapshot.kv.clone(),
            tool_runs: snapshot.tool_runs.clone(),
            whiteouts: BTreeSet::new(),
            origins: BTreeMap::new(),
            activities: Vec::new(),
            durable,
        }
    }

    fn new_overlay(info: VolumeInfo, base: &SnapshotState, allocator_block_size: u64) -> Self {
        let root = base
            .inodes
            .get(&ROOT_INODE_ID)
            .cloned()
            .unwrap_or_else(|| InodeRecord {
                stats: inode_stats(
                    ROOT_INODE_ID,
                    FileKind::Directory,
                    0o755,
                    info.created_at,
                    0,
                ),
                data: InodeData::Directory,
            });
        let mut paths = BTreeMap::new();
        paths.insert("/".to_string(), ROOT_INODE_ID);
        let mut inodes = BTreeMap::new();
        inodes.insert(ROOT_INODE_ID, root);
        let durable = DurableViewState {
            sequence: SequenceNumber::new(0),
            paths: paths.clone(),
            inodes: inodes.clone(),
            kv: base.kv.clone(),
            tool_runs: base.tool_runs.clone(),
            whiteouts: BTreeSet::new(),
            origins: BTreeMap::new(),
            activity_len: 0,
        };

        let next_inode = base
            .inodes
            .keys()
            .map(|inode| inode.get())
            .max()
            .unwrap_or(ROOT_INODE_ID.get())
            + 1;
        let next_tool_run_id = base
            .tool_runs
            .keys()
            .map(|tool_run_id| tool_run_id.get())
            .max()
            .unwrap_or(0)
            + 1;

        Self {
            info,
            sequence: SequenceNumber::new(0),
            allocators: allocator_map(next_inode, 1, next_tool_run_id, allocator_block_size),
            paths,
            inodes,
            kv: base.kv.clone(),
            tool_runs: base.tool_runs.clone(),
            whiteouts: BTreeSet::new(),
            origins: BTreeMap::new(),
            activities: Vec::new(),
            durable,
        }
    }

    fn visible_snapshot(&self) -> SnapshotState {
        SnapshotState {
            info: self.info.clone(),
            sequence: self.sequence,
            durable: false,
            paths: self.paths.clone(),
            inodes: self.inodes.clone(),
            kv: self.kv.clone(),
            tool_runs: self.tool_runs.clone(),
            whiteouts: self.whiteouts.clone(),
            origins: self.origins.clone(),
        }
    }

    fn promote_durable_cut(&mut self) {
        self.durable.sequence = self.sequence;
        self.durable.paths = self.paths.clone();
        self.durable.inodes = self.inodes.clone();
        self.durable.kv = self.kv.clone();
        self.durable.tool_runs = self.tool_runs.clone();
        self.durable.whiteouts = self.whiteouts.clone();
        self.durable.origins = self.origins.clone();
        self.durable.activity_len = self.activities.len();
    }

    fn allocate(&mut self, kind: AllocatorKind) -> u64 {
        self.allocators
            .entry(kind)
            .or_insert_with(|| BlockLeaseAllocator::new(1, DEFAULT_ALLOCATOR_BLOCK_SIZE))
            .allocate()
    }
}

#[derive(Clone)]
struct InodeRecord {
    stats: Stats,
    data: InodeData,
}

#[derive(Clone)]
enum InodeData {
    Directory,
    File(FileContent),
    Symlink(String),
}

#[derive(Clone, Default)]
struct FileContent {
    chunks: BTreeMap<u64, Vec<u8>>,
}

#[derive(Clone, Debug)]
struct BlockLeaseAllocator {
    next_persisted: u64,
    lease_next: u64,
    lease_end: u64,
    block_size: u64,
}

impl BlockLeaseAllocator {
    fn new(next_available: u64, block_size: u64) -> Self {
        let block_size = block_size.max(1);
        Self {
            next_persisted: next_available,
            lease_next: next_available,
            lease_end: next_available,
            block_size,
        }
    }

    fn allocate(&mut self) -> u64 {
        if self.lease_next == self.lease_end {
            self.refresh_lease();
        }
        let next = self.lease_next;
        self.lease_next = self.lease_next.saturating_add(1);
        next
    }

    fn refresh_lease(&mut self) {
        let start = self.next_persisted;
        self.next_persisted = self.next_persisted.saturating_add(self.block_size);
        self.lease_next = start;
        self.lease_end = self.next_persisted;
    }
}

fn allocator_map(
    next_inode: u64,
    next_activity: u64,
    next_tool_run_id: u64,
    block_size: u64,
) -> BTreeMap<AllocatorKind, BlockLeaseAllocator> {
    BTreeMap::from([
        (
            AllocatorKind::Inode,
            BlockLeaseAllocator::new(next_inode, block_size),
        ),
        (
            AllocatorKind::Activity,
            BlockLeaseAllocator::new(next_activity, block_size),
        ),
        (
            AllocatorKind::ToolRun,
            BlockLeaseAllocator::new(next_tool_run_id, block_size),
        ),
    ])
}

#[derive(Clone)]
struct ActivitySpec {
    kind: ActivityKind,
    subject: Option<String>,
    tool_run_id: Option<ToolRunId>,
    metadata: BTreeMap<String, JsonValue>,
}

fn activity_spec(
    kind: ActivityKind,
    subject: Option<String>,
    tool_run_id: Option<ToolRunId>,
    metadata: Option<BTreeMap<String, JsonValue>>,
) -> ActivitySpec {
    ActivitySpec {
        kind,
        subject,
        tool_run_id,
        metadata: metadata.unwrap_or_default(),
    }
}

fn append_activity(
    state: &mut VolumeState,
    now: Timestamp,
    kind: ActivityKind,
    subject: Option<String>,
    tool_run_id: Option<ToolRunId>,
    metadata: BTreeMap<String, JsonValue>,
) {
    let activity_id = ActivityId::new(state.allocate(AllocatorKind::Activity));
    let sequence = SequenceNumber::new(state.sequence.get().saturating_add(1));
    state.sequence = sequence;
    state.activities.push(ActivityEntry {
        volume_id: state.info.volume_id,
        activity_id,
        sequence,
        cursor: LogCursor::new(sequence, 0),
        timestamp: now,
        kind,
        subject,
        tool_run_id,
        metadata,
    });
}

fn allocate_inode(state: &mut VolumeState) -> InodeId {
    InodeId::new(state.allocate(AllocatorKind::Inode))
}

fn allocate_tool_run_id(state: &mut VolumeState) -> ToolRunId {
    ToolRunId::new(state.allocate(AllocatorKind::ToolRun))
}

fn recent_tool_runs(
    tool_runs: &BTreeMap<ToolRunId, ToolRun>,
    limit: Option<usize>,
) -> Vec<ToolRun> {
    let mut runs = tool_runs.values().rev().cloned().collect::<Vec<_>>();
    if let Some(limit) = limit {
        runs.truncate(limit);
    }
    runs
}

fn tool_metadata(tool_run_id: ToolRunId) -> BTreeMap<String, JsonValue> {
    let mut metadata = BTreeMap::new();
    metadata.insert("tool_run_id".to_string(), json!(tool_run_id.get()));
    metadata
}

fn configured_chunk_size(chunk_size: Option<u32>) -> Result<u32, VfsError> {
    let chunk_size = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);
    if chunk_size == 0 {
        return Err(VfsError::InvalidChunkSize);
    }
    Ok(chunk_size)
}

fn clone_inodes_for_chunk_size(
    inodes: &BTreeMap<InodeId, InodeRecord>,
    source_chunk_size: u32,
    target_chunk_size: u32,
) -> BTreeMap<InodeId, InodeRecord> {
    inodes
        .iter()
        .map(|(inode_id, record)| {
            (
                *inode_id,
                clone_inode_for_chunk_size(record, source_chunk_size, target_chunk_size, *inode_id),
            )
        })
        .collect()
}

fn clone_inode_for_chunk_size(
    record: &InodeRecord,
    source_chunk_size: u32,
    target_chunk_size: u32,
    inode_id: InodeId,
) -> InodeRecord {
    let mut cloned = record.clone();
    cloned.stats.inode = inode_id;
    if let InodeData::File(content) = &record.data {
        let bytes = file_content_to_bytes(content, record.stats.size, source_chunk_size);
        cloned.data = InodeData::File(file_content_from_bytes(&bytes, target_chunk_size));
    }
    cloned
}

fn mutate_write_file_state(
    state: &mut VolumeState,
    path: &str,
    data: &[u8],
    opts: &CreateOptions,
    now: Timestamp,
) -> Result<(), VfsError> {
    if path == "/" {
        return Err(VfsError::RootInvariant);
    }
    ensure_parent_directory(state, path, opts.create_parents, now)?;
    let exact_exists = state.paths.contains_key(path);
    match resolve_existing_path(state, path, true)? {
        Some((resolved_path, inode_id)) => {
            let inode = state
                .inodes
                .get_mut(&inode_id)
                .ok_or_else(|| VfsError::NotFound {
                    path: resolved_path.clone(),
                })?;
            let InodeData::File(content) = &mut inode.data else {
                return match inode.stats.kind {
                    FileKind::Directory => Err(VfsError::IsDirectory {
                        path: resolved_path,
                    }),
                    _ => Err(VfsError::NotFile {
                        path: resolved_path,
                    }),
                };
            };
            if !opts.overwrite {
                return Err(VfsError::AlreadyExists {
                    path: resolved_path,
                });
            }
            *content = file_content_from_bytes(data, state.info.chunk_size);
            inode.stats.size = data.len() as u64;
            inode.stats.modified_at = now;
            inode.stats.changed_at = now;
            inode.stats.accessed_at = now;
        }
        None if exact_exists => {
            return Err(VfsError::NotFound {
                path: path.to_string(),
            });
        }
        None => {
            let resolved_path = resolve_target_path(state, path)?;
            if state.paths.contains_key(&resolved_path) {
                return Err(VfsError::AlreadyExists {
                    path: resolved_path,
                });
            }

            let inode_id = allocate_inode(state);
            let stats = inode_stats(inode_id, FileKind::File, opts.mode, now, data.len() as u64);
            state.paths.insert(resolved_path.clone(), inode_id);
            state.inodes.insert(
                inode_id,
                InodeRecord {
                    stats,
                    data: InodeData::File(file_content_from_bytes(data, state.info.chunk_size)),
                },
            );
            touch_parent_directory(state, &resolved_path, now)?;
        }
    }

    Ok(())
}

fn mutate_pwrite_state(
    state: &mut VolumeState,
    path: &str,
    offset: u64,
    data: &[u8],
    now: Timestamp,
) -> Result<(), VfsError> {
    let (resolved_path, inode_id) =
        resolve_existing_path(state, path, true)?.ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
    let inode = state
        .inodes
        .get_mut(&inode_id)
        .ok_or_else(|| VfsError::NotFound {
            path: resolved_path.clone(),
        })?;
    let InodeData::File(content) = &mut inode.data else {
        return match inode.stats.kind {
            FileKind::Directory => Err(VfsError::IsDirectory {
                path: resolved_path.clone(),
            }),
            _ => Err(VfsError::NotFile {
                path: resolved_path.clone(),
            }),
        };
    };

    let new_size = write_file_content_at(
        content,
        inode.stats.size,
        state.info.chunk_size,
        offset,
        data,
    );
    inode.stats.size = new_size;
    inode.stats.modified_at = now;
    inode.stats.changed_at = now;
    inode.stats.accessed_at = now;
    Ok(())
}

fn mutate_truncate_state(
    state: &mut VolumeState,
    path: &str,
    size: u64,
    now: Timestamp,
) -> Result<(), VfsError> {
    let (resolved_path, inode_id) =
        resolve_existing_path(state, path, true)?.ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
    let inode = state
        .inodes
        .get_mut(&inode_id)
        .ok_or_else(|| VfsError::NotFound {
            path: resolved_path.clone(),
        })?;
    let InodeData::File(content) = &mut inode.data else {
        return match inode.stats.kind {
            FileKind::Directory => Err(VfsError::IsDirectory {
                path: resolved_path.clone(),
            }),
            _ => Err(VfsError::NotFile {
                path: resolved_path.clone(),
            }),
        };
    };

    truncate_file_content(content, state.info.chunk_size, size);
    inode.stats.size = size;
    inode.stats.modified_at = now;
    inode.stats.changed_at = now;
    inode.stats.accessed_at = now;
    Ok(())
}

fn apply_regular_batch_to_state(
    state: &mut VolumeState,
    ops: &[VfsBatchOperation],
    now: Timestamp,
) -> Result<(), VfsError> {
    for op in ops {
        match op {
            VfsBatchOperation::WriteFile { path, data, opts } => {
                let path = normalize_path(path)?;
                mutate_write_file_state(state, &path, data, opts, now)?;
                append_activity(
                    state,
                    now,
                    ActivityKind::FileWritten,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Pwrite { path, offset, data } => {
                let path = normalize_path(path)?;
                mutate_pwrite_state(state, &path, *offset, data, now)?;
                append_activity(
                    state,
                    now,
                    ActivityKind::FilePatched,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Truncate { path, size } => {
                let path = normalize_path(path)?;
                mutate_truncate_state(state, &path, *size, now)?;
                append_activity(
                    state,
                    now,
                    ActivityKind::FileTruncated,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Mkdir { path, opts } => {
                let path = normalize_path(path)?;
                if path == "/" {
                    if !opts.recursive {
                        return Err(VfsError::AlreadyExists { path });
                    }
                    continue;
                }
                if mutate_mkdir_state(state, &path, opts, now)? {
                    append_activity(
                        state,
                        now,
                        ActivityKind::DirectoryCreated,
                        Some(path),
                        None,
                        BTreeMap::new(),
                    );
                }
            }
            VfsBatchOperation::Rename { from, to } => {
                let from = normalize_path(from)?;
                let to = normalize_path(to)?;
                if mutate_rename_state(state, &from, &to, now)? {
                    let mut metadata = BTreeMap::new();
                    metadata.insert("to".to_string(), json!(to));
                    append_activity(
                        state,
                        now,
                        ActivityKind::PathRenamed,
                        Some(from),
                        None,
                        metadata,
                    );
                }
            }
            VfsBatchOperation::Link { from, to } => {
                let from = normalize_path(from)?;
                let to = normalize_path(to)?;
                mutate_link_state(state, &from, &to, now)?;
                let mut metadata = BTreeMap::new();
                metadata.insert("from".to_string(), json!(from));
                append_activity(
                    state,
                    now,
                    ActivityKind::HardLinkCreated,
                    Some(to),
                    None,
                    metadata,
                );
            }
            VfsBatchOperation::Symlink { target, linkpath } => {
                let linkpath = normalize_path(linkpath)?;
                mutate_symlink_state(state, target, &linkpath, now)?;
                append_activity(
                    state,
                    now,
                    ActivityKind::SymlinkCreated,
                    Some(linkpath),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Unlink { path } => {
                let path = normalize_path(path)?;
                mutate_unlink_state(state, &path, now)?;
                append_activity(
                    state,
                    now,
                    ActivityKind::PathDeleted,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Rmdir { path } => {
                let path = normalize_path(path)?;
                mutate_rmdir_state(state, &path, now)?;
                append_activity(
                    state,
                    now,
                    ActivityKind::DirectoryRemoved,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
        }
    }
    Ok(())
}

fn apply_overlay_batch_to_state(
    state: &mut VolumeState,
    base: &SnapshotState,
    ops: &[VfsBatchOperation],
    now: Timestamp,
) -> Result<(), VfsError> {
    for op in ops {
        match op {
            VfsBatchOperation::WriteFile { path, data, opts } => {
                let path = normalize_path(path)?;
                if let Some(resolved_path) = try_fast_overlay_new_target_path(state, base, &path)? {
                    let delta_opts = CreateOptions {
                        create_parents: false,
                        ..opts.clone()
                    };
                    mutate_write_file_state(state, &resolved_path, data, &delta_opts, now)?;
                } else {
                    let merged = overlay_visible_snapshot(state, base);
                    let resolved_path = if let Some((resolved_path, _)) =
                        resolve_existing_in_maps_with_mode(
                            &merged.paths,
                            &merged.inodes,
                            &path,
                            true,
                        )? {
                        ensure_overlay_existing_path(
                            state,
                            base,
                            &merged,
                            &resolved_path,
                            false,
                            now,
                        )?;
                        resolved_path
                    } else {
                        materialize_overlay_parent_chain(
                            state,
                            base,
                            &merged,
                            &path,
                            opts.create_parents,
                            now,
                        )?;
                        resolve_target_path(state, &path)?
                    };
                    let delta_opts = CreateOptions {
                        create_parents: false,
                        ..opts.clone()
                    };
                    mutate_write_file_state(state, &resolved_path, data, &delta_opts, now)?;
                }
                append_activity(
                    state,
                    now,
                    ActivityKind::FileWritten,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Pwrite { path, offset, data } => {
                let path = normalize_path(path)?;
                let merged = overlay_visible_snapshot(state, base);
                let (resolved_path, _) =
                    resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, true)?
                        .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
                ensure_overlay_existing_path(state, base, &merged, &resolved_path, false, now)?;
                mutate_pwrite_state(state, &resolved_path, *offset, data, now)?;
                append_activity(
                    state,
                    now,
                    ActivityKind::FilePatched,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Truncate { path, size } => {
                let path = normalize_path(path)?;
                let merged = overlay_visible_snapshot(state, base);
                let (resolved_path, _) =
                    resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, true)?
                        .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
                ensure_overlay_existing_path(state, base, &merged, &resolved_path, false, now)?;
                mutate_truncate_state(state, &resolved_path, *size, now)?;
                append_activity(
                    state,
                    now,
                    ActivityKind::FileTruncated,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Mkdir { path, opts } => {
                let path = normalize_path(path)?;
                if path == "/" {
                    if !opts.recursive {
                        return Err(VfsError::AlreadyExists { path });
                    }
                    continue;
                }
                if opts.recursive
                    && let Some((resolved_path, inode_id)) =
                        resolve_existing_path(state, &path, false)?
                {
                    let inode = state
                        .inodes
                        .get(&inode_id)
                        .ok_or_else(|| VfsError::NotFound {
                            path: resolved_path.clone(),
                        })?;
                    if inode.stats.kind == FileKind::Directory {
                        continue;
                    }
                }
                if let Some(resolved_path) = try_fast_overlay_new_target_path(state, base, &path)? {
                    let delta_opts = MkdirOptions {
                        recursive: false,
                        ..opts.clone()
                    };
                    if mutate_mkdir_state(state, &resolved_path, &delta_opts, now)? {
                        append_activity(
                            state,
                            now,
                            ActivityKind::DirectoryCreated,
                            Some(path),
                            None,
                            BTreeMap::new(),
                        );
                    }
                    continue;
                }
                let merged = overlay_visible_snapshot(state, base);
                if let Some((resolved_path, inode_id)) =
                    resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &path, false)?
                {
                    let inode = merged
                        .inodes
                        .get(&inode_id)
                        .ok_or_else(|| VfsError::NotFound {
                            path: resolved_path.clone(),
                        })?;
                    if inode.stats.kind == FileKind::Directory && opts.recursive {
                        continue;
                    }
                    return Err(VfsError::AlreadyExists {
                        path: resolved_path,
                    });
                }
                materialize_overlay_parent_chain(state, base, &merged, &path, opts.recursive, now)?;
                let resolved_path = resolve_target_path(state, &path)?;
                let delta_opts = MkdirOptions {
                    recursive: false,
                    ..opts.clone()
                };
                if mutate_mkdir_state(state, &resolved_path, &delta_opts, now)? {
                    append_activity(
                        state,
                        now,
                        ActivityKind::DirectoryCreated,
                        Some(path),
                        None,
                        BTreeMap::new(),
                    );
                }
            }
            VfsBatchOperation::Rename { from, to } => {
                let from = normalize_path(from)?;
                let to = normalize_path(to)?;
                if from == "/" || to == "/" {
                    return Err(VfsError::RootInvariant);
                }
                if from == to {
                    continue;
                }
                let merged = overlay_visible_snapshot(state, base);
                let (resolved_from, from_inode_id) = resolve_existing_in_maps_with_mode(
                    &merged.paths,
                    &merged.inodes,
                    &from,
                    false,
                )?
                .ok_or_else(|| VfsError::NotFound { path: from.clone() })?;
                let resolved_to = resolve_target_path_in_maps(&merged.paths, &merged.inodes, &to)?;
                if resolved_from == resolved_to {
                    continue;
                }
                let from_kind = merged
                    .inodes
                    .get(&from_inode_id)
                    .ok_or_else(|| VfsError::NotFound {
                        path: resolved_from.clone(),
                    })?
                    .stats
                    .kind;
                let source_was_delta = state.paths.contains_key(&resolved_from);
                if !source_was_delta {
                    if from_kind == FileKind::Directory {
                        copy_up_overlay_directory_subtree(
                            state,
                            base,
                            &merged,
                            &resolved_from,
                            now,
                        )?;
                    } else {
                        ensure_overlay_existing_path(
                            state,
                            base,
                            &merged,
                            &resolved_from,
                            false,
                            now,
                        )?;
                    }
                }
                let target_existing = resolve_existing_in_maps_with_mode(
                    &merged.paths,
                    &merged.inodes,
                    &resolved_to,
                    false,
                )?;
                let mut target_was_base = false;
                if let Some((target_path, target_inode_id)) = target_existing {
                    target_was_base = !state.paths.contains_key(&target_path);
                    let target_kind = merged
                        .inodes
                        .get(&target_inode_id)
                        .ok_or_else(|| VfsError::NotFound {
                            path: target_path.clone(),
                        })?
                        .stats
                        .kind;
                    if from_kind == FileKind::Directory
                        && target_kind == FileKind::Directory
                        && merged.paths.keys().any(|candidate| {
                            candidate != &target_path && is_descendant_path(&target_path, candidate)
                        })
                    {
                        return Err(VfsError::DirectoryNotEmpty { path: target_path });
                    }
                    if target_was_base {
                        ensure_overlay_existing_path(
                            state,
                            base,
                            &merged,
                            &target_path,
                            false,
                            now,
                        )?;
                    }
                } else {
                    materialize_overlay_parent_chain(
                        state,
                        base,
                        &merged,
                        &resolved_to,
                        false,
                        now,
                    )?;
                }
                if mutate_rename_state(state, &resolved_from, &resolved_to, now)? {
                    if !source_was_delta {
                        state.whiteouts.insert(resolved_from.clone());
                    }
                    if target_was_base {
                        state.whiteouts.insert(resolved_to.clone());
                    }
                    let mut metadata = BTreeMap::new();
                    metadata.insert("to".to_string(), json!(to));
                    append_activity(
                        state,
                        now,
                        ActivityKind::PathRenamed,
                        Some(from),
                        None,
                        metadata,
                    );
                }
            }
            VfsBatchOperation::Link { from, to } => {
                let from = normalize_path(from)?;
                let to = normalize_path(to)?;
                let merged = overlay_visible_snapshot(state, base);
                let (resolved_from, _) = resolve_existing_in_maps_with_mode(
                    &merged.paths,
                    &merged.inodes,
                    &from,
                    false,
                )?
                .ok_or_else(|| VfsError::NotFound { path: from.clone() })?;
                ensure_overlay_existing_path(state, base, &merged, &resolved_from, false, now)?;
                let resolved_to = resolve_target_path_in_maps(&merged.paths, &merged.inodes, &to)?;
                materialize_overlay_parent_chain(state, base, &merged, &resolved_to, false, now)?;
                mutate_link_state(state, &resolved_from, &resolved_to, now)?;
                let mut metadata = BTreeMap::new();
                metadata.insert("from".to_string(), json!(from));
                append_activity(
                    state,
                    now,
                    ActivityKind::HardLinkCreated,
                    Some(to),
                    None,
                    metadata,
                );
            }
            VfsBatchOperation::Symlink { target, linkpath } => {
                let linkpath = normalize_path(linkpath)?;
                if let Some(resolved_linkpath) =
                    try_fast_overlay_new_target_path(state, base, &linkpath)?
                {
                    mutate_symlink_state(state, target, &resolved_linkpath, now)?;
                } else {
                    let merged = overlay_visible_snapshot(state, base);
                    materialize_overlay_parent_chain(state, base, &merged, &linkpath, false, now)?;
                    let resolved_linkpath = resolve_target_path(state, &linkpath)?;
                    mutate_symlink_state(state, target, &resolved_linkpath, now)?;
                }
                append_activity(
                    state,
                    now,
                    ActivityKind::SymlinkCreated,
                    Some(linkpath),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Unlink { path } => {
                let path = normalize_path(path)?;
                let merged = overlay_visible_snapshot(state, base);
                let (resolved_path, resolved_inode) = resolve_existing_in_maps_with_mode(
                    &merged.paths,
                    &merged.inodes,
                    &path,
                    false,
                )?
                .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
                let resolved_kind = merged
                    .inodes
                    .get(&resolved_inode)
                    .ok_or_else(|| VfsError::NotFound {
                        path: resolved_path.clone(),
                    })?
                    .stats
                    .kind;
                let was_base = !state.paths.contains_key(&resolved_path);
                if was_base {
                    ensure_overlay_existing_path(state, base, &merged, &resolved_path, false, now)?;
                }
                if resolved_kind == FileKind::Directory {
                    return Err(VfsError::IsDirectory {
                        path: resolved_path,
                    });
                }
                mutate_unlink_state(state, &resolved_path, now)?;
                if was_base {
                    state.whiteouts.insert(resolved_path);
                }
                append_activity(
                    state,
                    now,
                    ActivityKind::PathDeleted,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
            VfsBatchOperation::Rmdir { path } => {
                let path = normalize_path(path)?;
                let merged = overlay_visible_snapshot(state, base);
                let (resolved_path, resolved_inode) = resolve_existing_in_maps_with_mode(
                    &merged.paths,
                    &merged.inodes,
                    &path,
                    false,
                )?
                .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
                let resolved_kind = merged
                    .inodes
                    .get(&resolved_inode)
                    .ok_or_else(|| VfsError::NotFound {
                        path: resolved_path.clone(),
                    })?
                    .stats
                    .kind;
                if resolved_kind != FileKind::Directory {
                    return Err(VfsError::NotDirectory {
                        path: resolved_path,
                    });
                }
                let was_base = !state.paths.contains_key(&resolved_path);
                if was_base {
                    ensure_overlay_existing_path(state, base, &merged, &resolved_path, true, now)?;
                }
                mutate_rmdir_state(state, &resolved_path, now)?;
                if was_base {
                    state.whiteouts.insert(resolved_path);
                }
                append_activity(
                    state,
                    now,
                    ActivityKind::DirectoryRemoved,
                    Some(path),
                    None,
                    BTreeMap::new(),
                );
            }
        }
    }
    Ok(())
}

fn mutate_mkdir_state(
    state: &mut VolumeState,
    path: &str,
    opts: &MkdirOptions,
    now: Timestamp,
) -> Result<bool, VfsError> {
    if let Some((resolved_path, inode_id)) = resolve_existing_path(state, path, false)? {
        let inode = state
            .inodes
            .get(&inode_id)
            .ok_or_else(|| VfsError::NotFound {
                path: resolved_path.clone(),
            })?;
        if inode.stats.kind == FileKind::Directory && opts.recursive {
            return Ok(false);
        }
        return Err(VfsError::AlreadyExists {
            path: resolved_path,
        });
    }

    ensure_parent_directory(state, path, opts.recursive, now)?;
    let resolved_path = resolve_target_path(state, path)?;
    let inode_id = allocate_inode(state);
    let stats = inode_stats(inode_id, FileKind::Directory, opts.mode, now, 0);
    state.paths.insert(resolved_path.clone(), inode_id);
    state.inodes.insert(
        inode_id,
        InodeRecord {
            stats,
            data: InodeData::Directory,
        },
    );
    increment_directory_nlink(state, &resolved_path, now)?;
    Ok(true)
}

fn mutate_rename_state(
    state: &mut VolumeState,
    from: &str,
    to: &str,
    now: Timestamp,
) -> Result<bool, VfsError> {
    if from == "/" || to == "/" {
        return Err(VfsError::RootInvariant);
    }
    if from == to {
        return Ok(false);
    }
    let (resolved_from, from_inode_id) =
        resolve_existing_path(state, from, false)?.ok_or_else(|| VfsError::NotFound {
            path: from.to_string(),
        })?;
    let resolved_to = resolve_target_path(state, to)?;
    if resolved_from == resolved_to {
        return Ok(false);
    }
    if is_descendant_path(&resolved_from, &resolved_to) {
        return Err(VfsError::InvalidPath {
            path: resolved_to.clone(),
        });
    }

    let from_kind = state
        .inodes
        .get(&from_inode_id)
        .ok_or_else(|| VfsError::NotFound {
            path: resolved_from.clone(),
        })?
        .stats
        .kind;
    let from_parent = parent_path(&resolved_from).ok_or(VfsError::RootInvariant)?;
    let to_parent = parent_path(&resolved_to).ok_or(VfsError::RootInvariant)?;

    if let Some(target_inode_id) = state.paths.get(&resolved_to).copied() {
        let target_kind = state
            .inodes
            .get(&target_inode_id)
            .ok_or_else(|| VfsError::NotFound {
                path: resolved_to.clone(),
            })?
            .stats
            .kind;
        match (from_kind, target_kind) {
            (FileKind::Directory, FileKind::Directory) => {
                if state.paths.keys().any(|candidate| {
                    candidate != &resolved_to && is_descendant_path(&resolved_to, candidate)
                }) {
                    return Err(VfsError::DirectoryNotEmpty {
                        path: resolved_to.clone(),
                    });
                }
                state.paths.remove(&resolved_to);
                state.inodes.remove(&target_inode_id);
                decrement_directory_nlink(state, &resolved_to, now)?;
            }
            (FileKind::Directory, _) => {
                return Err(VfsError::NotDirectory {
                    path: resolved_to.clone(),
                });
            }
            (_, FileKind::Directory) => {
                return Err(VfsError::IsDirectory {
                    path: resolved_to.clone(),
                });
            }
            (_, _) => {
                remove_non_directory_path(state, &resolved_to, now)?;
            }
        }
    }

    let renames = state
        .paths
        .iter()
        .filter_map(|(path, inode)| {
            rebase_path(path, &resolved_from, &resolved_to)
                .map(|new_path| (path.clone(), new_path, *inode))
        })
        .collect::<Vec<_>>();

    for (old_path, _, _) in &renames {
        state.paths.remove(old_path);
    }
    for (_, new_path, inode) in renames {
        state.paths.insert(new_path, inode);
    }
    touch_directory_path_at(state, &from_parent, now)?;
    touch_directory_path_at(state, &to_parent, now)?;
    if from_kind == FileKind::Directory && from_parent != to_parent {
        adjust_directory_parent_links(state, &from_parent, &to_parent)?;
    }
    if let Some(inode) = state.inodes.get_mut(&from_inode_id) {
        inode.stats.changed_at = now;
    }
    Ok(true)
}

fn mutate_link_state(
    state: &mut VolumeState,
    from: &str,
    to: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    if to == "/" {
        return Err(VfsError::RootInvariant);
    }
    let (resolved_from, inode_id) =
        resolve_existing_path(state, from, false)?.ok_or_else(|| VfsError::NotFound {
            path: from.to_string(),
        })?;
    let resolved_to = resolve_target_path(state, to)?;
    if state.paths.contains_key(&resolved_to) {
        return Err(VfsError::AlreadyExists { path: resolved_to });
    }
    let kind = state
        .inodes
        .get(&inode_id)
        .ok_or_else(|| VfsError::NotFound {
            path: resolved_from.clone(),
        })?
        .stats
        .kind;
    if kind == FileKind::Directory {
        return Err(VfsError::UnsupportedOperation {
            operation: "hard-link directories",
        });
    }
    let inode = state
        .inodes
        .get_mut(&inode_id)
        .ok_or_else(|| VfsError::NotFound {
            path: resolved_from.clone(),
        })?;
    inode.stats.nlink = inode.stats.nlink.saturating_add(1);
    inode.stats.changed_at = now;
    state.paths.insert(resolved_to.clone(), inode_id);
    touch_parent_directory(state, &resolved_to, now)?;
    Ok(())
}

fn mutate_symlink_state(
    state: &mut VolumeState,
    target: &str,
    linkpath: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    if linkpath == "/" {
        return Err(VfsError::RootInvariant);
    }
    let resolved_linkpath = resolve_target_path(state, linkpath)?;
    if state.paths.contains_key(&resolved_linkpath) {
        return Err(VfsError::AlreadyExists {
            path: resolved_linkpath,
        });
    }
    let inode_id = allocate_inode(state);
    let stats = inode_stats(inode_id, FileKind::Symlink, 0o777, now, target.len() as u64);
    state.paths.insert(resolved_linkpath.clone(), inode_id);
    state.inodes.insert(
        inode_id,
        InodeRecord {
            stats,
            data: InodeData::Symlink(target.to_string()),
        },
    );
    touch_parent_directory(state, &resolved_linkpath, now)?;
    Ok(())
}

fn mutate_unlink_state(
    state: &mut VolumeState,
    path: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    if path == "/" {
        return Err(VfsError::RootInvariant);
    }
    let (resolved_path, _) =
        resolve_existing_path(state, path, false)?.ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
    remove_non_directory_path(state, &resolved_path, now)?;
    touch_parent_directory(state, &resolved_path, now)?;
    Ok(())
}

fn mutate_rmdir_state(state: &mut VolumeState, path: &str, now: Timestamp) -> Result<(), VfsError> {
    if path == "/" {
        return Err(VfsError::RootInvariant);
    }
    let (resolved_path, inode_id) =
        resolve_existing_path(state, path, false)?.ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
    let inode = state
        .inodes
        .get(&inode_id)
        .ok_or_else(|| VfsError::NotFound {
            path: resolved_path.clone(),
        })?;
    if inode.stats.kind != FileKind::Directory {
        return Err(VfsError::NotDirectory {
            path: resolved_path.clone(),
        });
    }
    if state.paths.keys().any(|candidate| {
        candidate != &resolved_path && is_descendant_path(&resolved_path, candidate)
    }) {
        return Err(VfsError::DirectoryNotEmpty {
            path: resolved_path.clone(),
        });
    }
    state.paths.remove(&resolved_path);
    state.inodes.remove(&inode_id);
    decrement_directory_nlink(state, &resolved_path, now)?;
    touch_parent_directory(state, &resolved_path, now)?;
    Ok(())
}

fn build_overlay_snapshot(delta: &SnapshotState, base: &SnapshotState) -> SnapshotState {
    let mut paths = BTreeMap::new();
    let mut inodes = BTreeMap::new();

    for (path, inode_id) in &base.paths {
        if delta.paths.contains_key(path) || overlay_path_whiteouted(&delta.whiteouts, path) {
            continue;
        }
        paths.insert(path.clone(), *inode_id);
        if let Some(inode) = base.inodes.get(inode_id) {
            inodes.insert(*inode_id, inode.clone());
        }
    }

    for (path, inode_id) in &delta.paths {
        paths.insert(path.clone(), *inode_id);
    }
    inodes.extend(delta.inodes.clone());

    SnapshotState {
        info: delta.info.clone(),
        sequence: delta.sequence,
        durable: delta.durable,
        paths,
        inodes,
        kv: delta.kv.clone(),
        tool_runs: delta.tool_runs.clone(),
        whiteouts: delta.whiteouts.clone(),
        origins: delta.origins.clone(),
    }
}

fn overlay_visible_snapshot(state: &VolumeState, base: &SnapshotState) -> SnapshotState {
    build_overlay_snapshot(&state.visible_snapshot(), base)
}

fn try_fast_overlay_new_target_path(
    state: &VolumeState,
    base: &SnapshotState,
    path: &str,
) -> Result<Option<String>, VfsError> {
    if !state.whiteouts.is_empty() {
        return Ok(None);
    }
    let Some(parent) = parent_path(path) else {
        return Err(VfsError::RootInvariant);
    };
    let Some(name) = basename(path) else {
        return Err(VfsError::RootInvariant);
    };
    let Some((resolved_parent, parent_inode)) = resolve_existing_path(state, &parent, true)? else {
        return Ok(None);
    };
    let parent_record = state
        .inodes
        .get(&parent_inode)
        .ok_or_else(|| VfsError::NotDirectory {
            path: resolved_parent.clone(),
        })?;
    if parent_record.stats.kind != FileKind::Directory {
        return Err(VfsError::NotDirectory {
            path: resolved_parent,
        });
    }
    let resolved_path = join_path(&resolved_parent, name);
    if state.paths.contains_key(&resolved_path) || base.paths.contains_key(&resolved_path) {
        return Ok(None);
    }
    Ok(Some(resolved_path))
}

fn overlay_path_whiteouted(whiteouts: &BTreeSet<String>, path: &str) -> bool {
    whiteouts
        .iter()
        .any(|whiteout| path == whiteout || is_descendant_path(whiteout, path))
}

fn overlay_origin_record(state: &VolumeState, base_inode: InodeId) -> Option<OriginRecord> {
    state.info.overlay_base.as_ref().map(|base| OriginRecord {
        base_volume_id: base.volume_id,
        base_sequence: base.sequence,
        base_durable: base.durable,
        base_inode,
    })
}

fn visible_directory_nlink(merged: &SnapshotState, path: &str) -> Result<u32, VfsError> {
    let mut nlink = 2_u32;
    for entry in read_snapshot_dir_entries(merged, path)? {
        if entry.kind == FileKind::Directory {
            nlink = nlink.saturating_add(1);
        }
    }
    Ok(nlink)
}

fn count_inode_paths(paths: &BTreeMap<String, InodeId>, inode_id: InodeId) -> u32 {
    paths
        .values()
        .filter(|candidate| **candidate == inode_id)
        .count() as u32
}

fn materialize_overlay_parent_chain(
    state: &mut VolumeState,
    base: &SnapshotState,
    merged: &SnapshotState,
    path: &str,
    create_missing: bool,
    now: Timestamp,
) -> Result<(), VfsError> {
    let Some(parent) = parent_path(path) else {
        return Err(VfsError::RootInvariant);
    };
    if parent == "/" {
        return Ok(());
    }

    let mut current = String::new();
    for segment in parent.trim_start_matches('/').split('/') {
        current.push('/');
        current.push_str(segment);
        if let Some(inode_id) = state.paths.get(&current).copied() {
            let inode = state
                .inodes
                .get(&inode_id)
                .ok_or_else(|| VfsError::NotDirectory {
                    path: current.clone(),
                })?;
            if inode.stats.kind != FileKind::Directory {
                return Err(VfsError::NotDirectory {
                    path: current.clone(),
                });
            }
            continue;
        }

        if let Some((resolved_path, inode_id)) =
            resolve_existing_in_maps_with_mode(&merged.paths, &merged.inodes, &current, true)?
        {
            let inode = merged
                .inodes
                .get(&inode_id)
                .ok_or_else(|| VfsError::NotDirectory {
                    path: resolved_path.clone(),
                })?;
            if inode.stats.kind != FileKind::Directory {
                return Err(VfsError::NotDirectory {
                    path: resolved_path,
                });
            }
            copy_up_overlay_directory_exact(state, base, merged, &resolved_path, now)?;
            continue;
        }

        if !create_missing {
            return Err(VfsError::NotFound { path: current });
        }

        let inode_id = allocate_inode(state);
        let stats = inode_stats(inode_id, FileKind::Directory, 0o755, now, 0);
        state.paths.insert(current.clone(), inode_id);
        state.inodes.insert(
            inode_id,
            InodeRecord {
                stats,
                data: InodeData::Directory,
            },
        );
        increment_directory_nlink(state, &current, now)?;
    }

    Ok(())
}

fn copy_up_overlay_directory_exact(
    state: &mut VolumeState,
    base: &SnapshotState,
    merged: &SnapshotState,
    path: &str,
    _now: Timestamp,
) -> Result<InodeId, VfsError> {
    if let Some(inode_id) = state.paths.get(path).copied() {
        return Ok(inode_id);
    }

    let (resolved_path, base_inode_id) =
        resolve_existing_in_maps_with_mode(&base.paths, &base.inodes, path, false)?.ok_or_else(
            || VfsError::NotFound {
                path: path.to_string(),
            },
        )?;
    let base_record = base
        .inodes
        .get(&base_inode_id)
        .ok_or_else(|| VfsError::NotDirectory {
            path: resolved_path.clone(),
        })?;
    if base_record.stats.kind != FileKind::Directory {
        return Err(VfsError::NotDirectory {
            path: resolved_path,
        });
    }

    let inode_id = if path == "/" {
        ROOT_INODE_ID
    } else {
        allocate_inode(state)
    };
    let mut record = clone_inode_for_chunk_size(
        base_record,
        base.info.chunk_size,
        state.info.chunk_size,
        inode_id,
    );
    record.stats.nlink = visible_directory_nlink(merged, path)?;
    state.paths.insert(path.to_string(), inode_id);
    state.inodes.insert(inode_id, record);
    if inode_id != ROOT_INODE_ID
        && let Some(origin) = overlay_origin_record(state, base_inode_id)
    {
        state.origins.insert(inode_id, origin);
    }
    Ok(inode_id)
}

fn copy_up_overlay_inode_aliases(
    state: &mut VolumeState,
    base: &SnapshotState,
    merged: &SnapshotState,
    path: &str,
    now: Timestamp,
) -> Result<InodeId, VfsError> {
    let (resolved_path, base_inode_id) =
        resolve_existing_in_maps_with_mode(&base.paths, &base.inodes, path, false)?.ok_or_else(
            || VfsError::NotFound {
                path: path.to_string(),
            },
        )?;
    let base_record = base
        .inodes
        .get(&base_inode_id)
        .ok_or_else(|| VfsError::NotFound {
            path: resolved_path.clone(),
        })?;
    if base_record.stats.kind == FileKind::Directory {
        return Err(VfsError::UnsupportedOperation {
            operation: "copy-up directory aliases",
        });
    }

    let existing_delta_inode = state.origins.iter().find_map(|(delta_inode, origin)| {
        (origin.base_volume_id == base.info.volume_id
            && origin.base_sequence == base.sequence
            && origin.base_durable == base.durable
            && origin.base_inode == base_inode_id)
            .then_some(*delta_inode)
    });

    let delta_inode = if let Some(delta_inode) = existing_delta_inode {
        delta_inode
    } else {
        let delta_inode = allocate_inode(state);
        let record = clone_inode_for_chunk_size(
            base_record,
            base.info.chunk_size,
            state.info.chunk_size,
            delta_inode,
        );
        state.inodes.insert(delta_inode, record);
        if let Some(origin) = overlay_origin_record(state, base_inode_id) {
            state.origins.insert(delta_inode, origin);
        }
        delta_inode
    };

    let aliases = merged
        .paths
        .iter()
        .filter_map(|(alias_path, inode_id)| {
            (*inode_id == base_inode_id && !state.paths.contains_key(alias_path))
                .then_some(alias_path.clone())
        })
        .collect::<Vec<_>>();

    for alias in aliases {
        materialize_overlay_parent_chain(state, base, merged, &alias, false, now)?;
        state.paths.insert(alias, delta_inode);
    }

    if let Some(inode) = state.inodes.get_mut(&delta_inode) {
        inode.stats.nlink = count_inode_paths(&state.paths, delta_inode).max(1);
    }
    Ok(delta_inode)
}

fn copy_up_overlay_directory_subtree(
    state: &mut VolumeState,
    base: &SnapshotState,
    merged: &SnapshotState,
    path: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    let mut paths = merged
        .paths
        .keys()
        .filter(|candidate| **candidate == path || is_descendant_path(path, candidate))
        .cloned()
        .collect::<Vec<_>>();
    paths.sort_by_key(|candidate| (path_segments(candidate).len(), candidate.clone()));

    for candidate in paths {
        if state.paths.contains_key(&candidate) {
            continue;
        }
        let inode_id = merged
            .paths
            .get(&candidate)
            .copied()
            .ok_or_else(|| VfsError::NotFound {
                path: candidate.clone(),
            })?;
        let inode = merged
            .inodes
            .get(&inode_id)
            .ok_or_else(|| VfsError::NotFound {
                path: candidate.clone(),
            })?;
        match inode.stats.kind {
            FileKind::Directory => {
                materialize_overlay_parent_chain(state, base, merged, &candidate, false, now)?;
                copy_up_overlay_directory_exact(state, base, merged, &candidate, now)?;
            }
            _ => {
                copy_up_overlay_inode_aliases(state, base, merged, &candidate, now)?;
            }
        }
    }

    Ok(())
}

fn ensure_overlay_existing_path(
    state: &mut VolumeState,
    base: &SnapshotState,
    merged: &SnapshotState,
    path: &str,
    recursive_dir: bool,
    now: Timestamp,
) -> Result<(), VfsError> {
    if state.paths.contains_key(path) {
        return Ok(());
    }
    let inode_id = merged
        .paths
        .get(path)
        .copied()
        .ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
    let inode = merged
        .inodes
        .get(&inode_id)
        .ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
    match inode.stats.kind {
        FileKind::Directory => {
            materialize_overlay_parent_chain(state, base, merged, path, false, now)?;
            if recursive_dir {
                copy_up_overlay_directory_subtree(state, base, merged, path, now)?;
            } else {
                copy_up_overlay_directory_exact(state, base, merged, path, now)?;
            }
        }
        _ => {
            copy_up_overlay_inode_aliases(state, base, merged, path, now)?;
        }
    }
    Ok(())
}

fn inode_stats(inode: InodeId, kind: FileKind, mode: u32, now: Timestamp, size: u64) -> Stats {
    Stats {
        inode,
        kind,
        mode,
        nlink: if kind == FileKind::Directory { 2 } else { 1 },
        uid: 0,
        gid: 0,
        size,
        created_at: now,
        modified_at: now,
        changed_at: now,
        accessed_at: now,
        rdev: 0,
    }
}

fn normalize_path(path: &str) -> Result<String, VfsError> {
    if !path.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }
    if path == "/" {
        return Ok("/".to_string());
    }

    let mut parts = Vec::new();
    for part in path.split('/') {
        if part.is_empty() {
            continue;
        }
        if part == "." || part == ".." {
            return Err(VfsError::InvalidPath {
                path: path.to_string(),
            });
        }
        parts.push(part);
    }

    if parts.is_empty() {
        return Ok("/".to_string());
    }

    Ok(format!("/{}", parts.join("/")))
}

fn normalize_internal_path(path: &str) -> Result<String, VfsError> {
    if !path.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }

    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }

    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn parent_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let index = path.rfind('/')?;
    if index == 0 {
        Some("/".to_string())
    } else {
        Some(path[..index].to_string())
    }
}

fn path_segments(path: &str) -> Vec<&str> {
    if path == "/" {
        Vec::new()
    } else {
        path.trim_start_matches('/').split('/').collect()
    }
}

fn basename(path: &str) -> Option<&str> {
    if path == "/" {
        None
    } else {
        path.rsplit('/').next()
    }
}

fn join_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn ensure_parent_directory(
    state: &mut VolumeState,
    path: &str,
    create_parents: bool,
    now: Timestamp,
) -> Result<(), VfsError> {
    let Some(parent) = parent_path(path) else {
        return Err(VfsError::RootInvariant);
    };

    if create_parents {
        create_missing_directories(state, &parent, now)?;
    }
    resolve_target_path(state, path).map(|_| ())
}

fn create_missing_directories(
    state: &mut VolumeState,
    path: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    if path == "/" {
        return Ok(());
    }

    let mut current = String::new();
    for segment in path.trim_start_matches('/').split('/') {
        current.push('/');
        current.push_str(segment);
        if let Some(inode_id) = state.paths.get(&current).copied() {
            let inode = state
                .inodes
                .get(&inode_id)
                .ok_or_else(|| VfsError::NotDirectory {
                    path: current.clone(),
                })?;
            if inode.stats.kind != FileKind::Directory {
                return Err(VfsError::NotDirectory {
                    path: current.clone(),
                });
            }
            continue;
        }

        let inode_id = allocate_inode(state);
        let stats = inode_stats(inode_id, FileKind::Directory, 0o755, now, 0);
        state.paths.insert(current.clone(), inode_id);
        state.inodes.insert(
            inode_id,
            InodeRecord {
                stats,
                data: InodeData::Directory,
            },
        );
        increment_directory_nlink(state, &current, now)?;
    }

    Ok(())
}

fn resolve_target_path(state: &VolumeState, path: &str) -> Result<String, VfsError> {
    resolve_target_path_in_maps(&state.paths, &state.inodes, path)
}

fn resolve_target_path_in_maps(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<String, VfsError> {
    let path = normalize_path(path)?;
    let Some(parent) = parent_path(&path) else {
        return Err(VfsError::RootInvariant);
    };
    let name = basename(&path).ok_or(VfsError::RootInvariant)?;
    let (resolved_parent, parent_inode) =
        resolve_existing_in_maps_with_mode(paths, inodes, &parent, true)?.ok_or_else(|| {
            VfsError::NotFound {
                path: parent.clone(),
            }
        })?;
    let inode = inodes
        .get(&parent_inode)
        .ok_or_else(|| VfsError::NotDirectory {
            path: resolved_parent.clone(),
        })?;
    if inode.stats.kind != FileKind::Directory {
        return Err(VfsError::NotDirectory {
            path: resolved_parent,
        });
    }
    Ok(join_path(&resolved_parent, name))
}

fn resolve_existing_path(
    state: &VolumeState,
    path: &str,
    follow_final_symlink: bool,
) -> Result<Option<(String, InodeId)>, VfsError> {
    resolve_existing_in_maps_with_mode(&state.paths, &state.inodes, path, follow_final_symlink)
}

fn resolve_existing_in_maps_with_mode(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
    follow_final_symlink: bool,
) -> Result<Option<(String, InodeId)>, VfsError> {
    let original = normalize_path(path)?;
    let mut current = original.clone();
    for _ in 0..MAX_SYMLINK_DEPTH {
        if current == "/" {
            return Ok(paths.get(&current).copied().map(|inode| (current, inode)));
        }

        let segments = path_segments(&current);
        let mut prefix = "/".to_string();
        let mut redirected = None;

        for (index, segment) in segments.iter().enumerate() {
            let candidate = join_path(&prefix, segment);
            let Some(inode_id) = paths.get(&candidate).copied() else {
                return Ok(None);
            };
            let inode = inodes.get(&inode_id).ok_or_else(|| VfsError::NotFound {
                path: candidate.clone(),
            })?;
            let is_final = index + 1 == segments.len();
            if let InodeData::Symlink(target) = &inode.data
                && (!is_final || follow_final_symlink)
            {
                current = resolve_symlink_target(&candidate, target, &segments[index + 1..])?;
                redirected = Some(());
                break;
            }
            prefix = candidate;
        }

        if redirected.is_none() {
            let inode_id = paths
                .get(&prefix)
                .copied()
                .ok_or_else(|| VfsError::NotFound {
                    path: prefix.clone(),
                })?;
            return Ok(Some((prefix, inode_id)));
        }
    }

    Err(VfsError::SymlinkLoop { path: original })
}

fn resolve_symlink_target(
    symlink_path: &str,
    target: &str,
    remainder: &[&str],
) -> Result<String, VfsError> {
    let parent = parent_path(symlink_path).unwrap_or_else(|| "/".to_string());
    let mut combined = if target.starts_with('/') {
        target.to_string()
    } else if parent == "/" {
        format!("/{target}")
    } else {
        format!("{parent}/{target}")
    };
    if !remainder.is_empty() {
        if combined != "/" {
            combined.push('/');
        }
        combined.push_str(&remainder.join("/"));
    }
    normalize_internal_path(&combined)
}

fn lookup_stats(
    state: &VolumeState,
    path: &str,
    follow_final_symlink: bool,
) -> Result<Option<Stats>, VfsError> {
    lookup_stats_in_maps(&state.paths, &state.inodes, path, follow_final_symlink)
}

fn lookup_snapshot_stats(
    state: &SnapshotState,
    path: &str,
    follow_final_symlink: bool,
) -> Result<Option<Stats>, VfsError> {
    lookup_stats_in_maps(&state.paths, &state.inodes, path, follow_final_symlink)
}

fn lookup_stats_in_maps(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
    follow_final_symlink: bool,
) -> Result<Option<Stats>, VfsError> {
    let Some((resolved_path, inode_id)) =
        resolve_existing_in_maps_with_mode(paths, inodes, path, follow_final_symlink)?
    else {
        return Ok(None);
    };
    let inode = inodes.get(&inode_id).ok_or(VfsError::NotFound {
        path: resolved_path,
    })?;
    Ok(Some(inode.stats.clone()))
}

fn read_file_bytes(state: &VolumeState, path: &str) -> Result<Option<Vec<u8>>, VfsError> {
    read_file_bytes_in_maps(&state.paths, &state.inodes, state.info.chunk_size, path)
}

fn read_snapshot_file_bytes(
    state: &SnapshotState,
    path: &str,
) -> Result<Option<Vec<u8>>, VfsError> {
    read_file_bytes_in_maps(&state.paths, &state.inodes, state.info.chunk_size, path)
}

fn read_file_bytes_in_maps(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    chunk_size: u32,
    path: &str,
) -> Result<Option<Vec<u8>>, VfsError> {
    let Some((resolved_path, inode_id)) =
        resolve_existing_in_maps_with_mode(paths, inodes, path, true)?
    else {
        return Ok(None);
    };
    let inode = inodes.get(&inode_id).ok_or_else(|| VfsError::NotFound {
        path: resolved_path.clone(),
    })?;
    match &inode.data {
        InodeData::File(content) => Ok(Some(file_content_to_bytes(
            content,
            inode.stats.size,
            chunk_size,
        ))),
        InodeData::Directory => Err(VfsError::IsDirectory {
            path: resolved_path,
        }),
        InodeData::Symlink(_) => Err(VfsError::NotFile {
            path: resolved_path,
        }),
    }
}

fn pread_file_bytes(
    state: &VolumeState,
    path: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>, VfsError> {
    pread_file_bytes_in_maps(
        &state.paths,
        &state.inodes,
        state.info.chunk_size,
        path,
        offset,
        len,
    )
}

fn pread_snapshot_file_bytes(
    state: &SnapshotState,
    path: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>, VfsError> {
    pread_file_bytes_in_maps(
        &state.paths,
        &state.inodes,
        state.info.chunk_size,
        path,
        offset,
        len,
    )
}

fn pread_file_bytes_in_maps(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    chunk_size: u32,
    path: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>, VfsError> {
    let Some((resolved_path, inode_id)) =
        resolve_existing_in_maps_with_mode(paths, inodes, path, true)?
    else {
        return Ok(None);
    };
    let inode = inodes.get(&inode_id).ok_or_else(|| VfsError::NotFound {
        path: resolved_path.clone(),
    })?;
    match &inode.data {
        InodeData::File(content) => Ok(Some(read_file_content_range(
            content,
            inode.stats.size,
            chunk_size,
            offset,
            len,
        ))),
        InodeData::Directory => Err(VfsError::IsDirectory {
            path: resolved_path,
        }),
        InodeData::Symlink(_) => Err(VfsError::NotFile {
            path: resolved_path,
        }),
    }
}

fn read_dir_entries(state: &VolumeState, path: &str) -> Result<Vec<DirEntry>, VfsError> {
    read_dir_entries_in_maps(&state.paths, &state.inodes, path)
}

fn read_snapshot_dir_entries(state: &SnapshotState, path: &str) -> Result<Vec<DirEntry>, VfsError> {
    read_dir_entries_in_maps(&state.paths, &state.inodes, path)
}

fn read_dir_entries_in_maps(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<Vec<DirEntry>, VfsError> {
    let normalized = normalize_path(path)?;
    let (resolved_path, inode_id) = resolve_existing_in_maps_with_mode(paths, inodes, path, true)?
        .ok_or_else(|| VfsError::NotFound {
            path: normalized.clone(),
        })?;
    let inode = inodes
        .get(&inode_id)
        .ok_or_else(|| VfsError::NotDirectory {
            path: resolved_path.clone(),
        })?;
    if inode.stats.kind != FileKind::Directory {
        return Err(VfsError::NotDirectory {
            path: resolved_path,
        });
    }

    let mut entries = Vec::new();
    for (candidate, inode_id) in paths {
        if let Some(name) = direct_child_name(&resolved_path, candidate) {
            let inode = inodes.get(inode_id).ok_or_else(|| VfsError::NotFound {
                path: candidate.clone(),
            })?;
            entries.push(DirEntry {
                name,
                inode: *inode_id,
                kind: inode.stats.kind,
            });
        }
    }
    Ok(entries)
}

fn read_dir_entries_plus(state: &VolumeState, path: &str) -> Result<Vec<DirEntryPlus>, VfsError> {
    read_dir_entries_plus_in_maps(&state.paths, &state.inodes, path)
}

fn read_snapshot_dir_entries_plus(
    state: &SnapshotState,
    path: &str,
) -> Result<Vec<DirEntryPlus>, VfsError> {
    read_dir_entries_plus_in_maps(&state.paths, &state.inodes, path)
}

fn read_dir_entries_plus_in_maps(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<Vec<DirEntryPlus>, VfsError> {
    read_dir_entries_in_maps(paths, inodes, path)?
        .into_iter()
        .map(|entry| {
            let stats = inodes
                .get(&entry.inode)
                .ok_or_else(|| VfsError::NotFound {
                    path: format!("{path}/{}", entry.name),
                })?
                .stats
                .clone();
            Ok(DirEntryPlus { entry, stats })
        })
        .collect()
}

fn read_link_target(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<String, VfsError> {
    let normalized = normalize_path(path)?;
    let (resolved_path, inode_id) = resolve_existing_in_maps_with_mode(paths, inodes, path, false)?
        .ok_or_else(|| VfsError::NotFound {
            path: normalized.clone(),
        })?;
    let inode = inodes.get(&inode_id).ok_or_else(|| VfsError::NotFound {
        path: resolved_path.clone(),
    })?;
    match &inode.data {
        InodeData::Symlink(target) => Ok(target.clone()),
        _ => Err(VfsError::NotSymlink {
            path: resolved_path,
        }),
    }
}

fn touch_directory_path_at(
    state: &mut VolumeState,
    path: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    let inode_id = state
        .paths
        .get(path)
        .copied()
        .ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
    let inode = state
        .inodes
        .get_mut(&inode_id)
        .ok_or_else(|| VfsError::NotDirectory {
            path: path.to_string(),
        })?;
    if inode.stats.kind != FileKind::Directory {
        return Err(VfsError::NotDirectory {
            path: path.to_string(),
        });
    }
    inode.stats.modified_at = now;
    inode.stats.changed_at = now;
    Ok(())
}

fn touch_parent_directory(
    state: &mut VolumeState,
    child_path: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    let Some(parent) = parent_path(child_path) else {
        return Ok(());
    };
    touch_directory_path_at(state, &parent, now)
}

fn increment_directory_nlink(
    state: &mut VolumeState,
    child_path: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    let Some(parent) = parent_path(child_path) else {
        return Ok(());
    };
    let parent_inode_id = state
        .paths
        .get(&parent)
        .copied()
        .ok_or_else(|| VfsError::NotFound {
            path: parent.clone(),
        })?;
    let parent_inode =
        state
            .inodes
            .get_mut(&parent_inode_id)
            .ok_or_else(|| VfsError::NotDirectory {
                path: parent.clone(),
            })?;
    if parent_inode.stats.kind != FileKind::Directory {
        return Err(VfsError::NotDirectory { path: parent });
    }
    parent_inode.stats.nlink = parent_inode.stats.nlink.saturating_add(1);
    parent_inode.stats.modified_at = now;
    parent_inode.stats.changed_at = now;
    Ok(())
}

fn decrement_directory_nlink(
    state: &mut VolumeState,
    child_path: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    let Some(parent) = parent_path(child_path) else {
        return Ok(());
    };
    let parent_inode_id = state
        .paths
        .get(&parent)
        .copied()
        .ok_or_else(|| VfsError::NotFound {
            path: parent.clone(),
        })?;
    let parent_inode =
        state
            .inodes
            .get_mut(&parent_inode_id)
            .ok_or_else(|| VfsError::NotDirectory {
                path: parent.clone(),
            })?;
    if parent_inode.stats.kind != FileKind::Directory {
        return Err(VfsError::NotDirectory { path: parent });
    }
    parent_inode.stats.nlink = parent_inode.stats.nlink.saturating_sub(1);
    parent_inode.stats.modified_at = now;
    parent_inode.stats.changed_at = now;
    Ok(())
}

fn adjust_directory_parent_links(
    state: &mut VolumeState,
    old_parent: &str,
    new_parent: &str,
) -> Result<(), VfsError> {
    if old_parent == new_parent {
        return Ok(());
    }
    let old_parent_inode =
        state
            .paths
            .get(old_parent)
            .copied()
            .ok_or_else(|| VfsError::NotFound {
                path: old_parent.to_string(),
            })?;
    let new_parent_inode =
        state
            .paths
            .get(new_parent)
            .copied()
            .ok_or_else(|| VfsError::NotFound {
                path: new_parent.to_string(),
            })?;
    state
        .inodes
        .get_mut(&old_parent_inode)
        .ok_or_else(|| VfsError::NotDirectory {
            path: old_parent.to_string(),
        })?
        .stats
        .nlink = state
        .inodes
        .get(&old_parent_inode)
        .expect("old parent exists")
        .stats
        .nlink
        .saturating_sub(1);
    state
        .inodes
        .get_mut(&new_parent_inode)
        .ok_or_else(|| VfsError::NotDirectory {
            path: new_parent.to_string(),
        })?
        .stats
        .nlink = state
        .inodes
        .get(&new_parent_inode)
        .expect("new parent exists")
        .stats
        .nlink
        .saturating_add(1);
    Ok(())
}

fn remove_non_directory_path(
    state: &mut VolumeState,
    path: &str,
    now: Timestamp,
) -> Result<(), VfsError> {
    let inode_id = state.paths.remove(path).ok_or_else(|| VfsError::NotFound {
        path: path.to_string(),
    })?;
    let remove_inode = {
        let inode = state
            .inodes
            .get_mut(&inode_id)
            .ok_or_else(|| VfsError::NotFound {
                path: path.to_string(),
            })?;
        if inode.stats.kind == FileKind::Directory {
            return Err(VfsError::IsDirectory {
                path: path.to_string(),
            });
        }
        inode.stats.nlink = inode.stats.nlink.saturating_sub(1);
        inode.stats.changed_at = now;
        inode.stats.nlink == 0
    };
    if remove_inode {
        state.inodes.remove(&inode_id);
    }
    Ok(())
}

fn direct_child_name(parent: &str, child: &str) -> Option<String> {
    if parent == child {
        return None;
    }

    if parent == "/" {
        let remainder = child.strip_prefix('/')?;
        if remainder.is_empty() || remainder.contains('/') {
            return None;
        }
        return Some(remainder.to_string());
    }

    let prefix = format!("{parent}/");
    let remainder = child.strip_prefix(&prefix)?;
    if remainder.is_empty() || remainder.contains('/') {
        return None;
    }
    Some(remainder.to_string())
}

fn is_descendant_path(parent: &str, candidate: &str) -> bool {
    if parent == "/" {
        return candidate != "/";
    }
    candidate.starts_with(&format!("{parent}/"))
}

fn rebase_path(path: &str, from: &str, to: &str) -> Option<String> {
    if path == from {
        return Some(to.to_string());
    }
    let suffix = path.strip_prefix(&format!("{from}/"))?;
    Some(format!("{to}/{suffix}"))
}

fn file_content_from_bytes(bytes: &[u8], chunk_size: u32) -> FileContent {
    let mut content = FileContent::default();
    for (index, chunk) in bytes.chunks(chunk_size as usize).enumerate() {
        content.chunks.insert(index as u64, chunk.to_vec());
    }
    content
}

fn file_content_to_bytes(content: &FileContent, size: u64, chunk_size: u32) -> Vec<u8> {
    read_file_content_range(content, size, chunk_size, 0, size)
}

fn read_file_content_range(
    content: &FileContent,
    size: u64,
    chunk_size: u32,
    offset: u64,
    len: u64,
) -> Vec<u8> {
    if len == 0 || offset >= size {
        return Vec::new();
    }

    let chunk_size = u64::from(chunk_size);
    let end = size.min(offset.saturating_add(len));
    let mut out = vec![0; (end - offset) as usize];
    let first_chunk = offset / chunk_size;
    let last_chunk = (end - 1) / chunk_size;

    for chunk_index in first_chunk..=last_chunk {
        let Some(chunk) = content.chunks.get(&chunk_index) else {
            continue;
        };
        let chunk_start = chunk_index * chunk_size;
        let copy_start = offset.max(chunk_start);
        let copy_end = end.min(chunk_start + chunk.len() as u64);
        if copy_start >= copy_end {
            continue;
        }

        let src_start = (copy_start - chunk_start) as usize;
        let bytes_to_copy = (copy_end - copy_start) as usize;
        let dst_start = (copy_start - offset) as usize;
        out[dst_start..dst_start + bytes_to_copy]
            .copy_from_slice(&chunk[src_start..src_start + bytes_to_copy]);
    }

    out
}

fn write_file_content_at(
    content: &mut FileContent,
    current_size: u64,
    chunk_size: u32,
    offset: u64,
    data: &[u8],
) -> u64 {
    if data.is_empty() {
        return current_size;
    }

    let chunk_size_u64 = u64::from(chunk_size);
    let new_size = current_size.max(offset.saturating_add(data.len() as u64));
    let first_chunk = offset / chunk_size_u64;
    let last_chunk = (offset + data.len() as u64 - 1) / chunk_size_u64;

    for chunk_index in first_chunk..=last_chunk {
        let chunk_start = chunk_index * chunk_size_u64;
        let desired_len = (new_size - chunk_start).min(chunk_size_u64) as usize;
        let mut chunk = vec![0; desired_len];
        if let Some(existing) = content.chunks.get(&chunk_index) {
            let preserved = existing.len().min(chunk.len());
            chunk[..preserved].copy_from_slice(&existing[..preserved]);
        }

        let write_start = offset.max(chunk_start);
        let write_end = (offset + data.len() as u64).min(chunk_start + chunk.len() as u64);
        let src_start = (write_start - offset) as usize;
        let bytes_to_copy = (write_end - write_start) as usize;
        let dst_start = (write_start - chunk_start) as usize;
        chunk[dst_start..dst_start + bytes_to_copy]
            .copy_from_slice(&data[src_start..src_start + bytes_to_copy]);
        content.chunks.insert(chunk_index, chunk);
    }

    new_size
}

fn truncate_file_content(content: &mut FileContent, chunk_size: u32, size: u64) {
    if size == 0 {
        content.chunks.clear();
        return;
    }

    let chunk_size_u64 = u64::from(chunk_size);
    let last_chunk = (size - 1) / chunk_size_u64;
    content.chunks.retain(|index, _| *index <= last_chunk);

    let final_len = ((size - 1) % chunk_size_u64 + 1) as usize;
    if let Some(chunk) = content.chunks.get_mut(&last_chunk) {
        chunk.truncate(final_len);
    }
}
