use std::{
    any::Any,
    collections::BTreeMap,
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
    AgentFileSystem, AgentFsError, AgentKvStore, AgentToolRuns, AllocatorKind, CreateOptions,
    DirEntry, DirEntryPlus, FileKind, InodeId, JsonValue, MkdirOptions, ReadOnlyAgentFileSystem,
    ReadOnlyAgentKvStore, ReadOnlyAgentToolRuns, Stats, ToolRun, ToolRunId, ToolRunStatus,
};

pub const VFS_FORMAT_VERSION: u8 = 1;
pub const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024;
pub const ROOT_INODE_ID: InodeId = InodeId::new(1);
const DEFAULT_ALLOCATOR_BLOCK_SIZE: u64 = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentFsConfig {
    pub volume_id: crate::VolumeId,
    pub chunk_size: Option<u32>,
    pub create_if_missing: bool,
}

impl AgentFsConfig {
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
pub struct AgentFsVolumeInfo {
    pub volume_id: crate::VolumeId,
    pub chunk_size: u32,
    pub format_version: u8,
    pub root_inode: InodeId,
    pub created_at: Timestamp,
    pub overlay_base: Option<OverlayBaseDescriptor>,
}

#[async_trait]
pub trait AgentFsStore: Send + Sync {
    async fn open_volume(
        &self,
        config: AgentFsConfig,
    ) -> Result<Arc<dyn AgentFsVolume>, AgentFsError>;
    async fn clone_volume(
        &self,
        source: CloneVolumeSource,
        target: AgentFsConfig,
    ) -> Result<Arc<dyn AgentFsVolume>, AgentFsError>;
    async fn create_overlay(
        &self,
        base: Arc<dyn AgentFsSnapshot>,
        target: AgentFsConfig,
    ) -> Result<Arc<dyn AgentFsOverlay>, AgentFsError>;
}

#[async_trait]
pub trait AgentFsVolume: Send + Sync {
    fn info(&self) -> AgentFsVolumeInfo;
    fn fs(&self) -> Arc<dyn AgentFileSystem>;
    fn kv(&self) -> Arc<dyn AgentKvStore>;
    fn tools(&self) -> Arc<dyn AgentToolRuns>;
    async fn snapshot(
        &self,
        opts: SnapshotOptions,
    ) -> Result<Arc<dyn AgentFsSnapshot>, AgentFsError>;
    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: ActivityOptions,
    ) -> Result<ActivityStream, AgentFsError>;
    fn subscribe_activity(&self, opts: ActivityOptions) -> ActivityReceiver;
    async fn flush(&self) -> Result<(), AgentFsError>;
}

pub trait AgentFsOverlay: AgentFsVolume {
    fn base(&self) -> Arc<dyn AgentFsSnapshot>;
}

pub trait AgentFsSnapshot: Send + Sync + Any {
    fn volume_id(&self) -> crate::VolumeId;
    fn sequence(&self) -> SequenceNumber;
    fn durable(&self) -> bool;
    fn info(&self) -> AgentFsVolumeInfo;
    fn fs(&self) -> Arc<dyn ReadOnlyAgentFileSystem>;
    fn kv(&self) -> Arc<dyn ReadOnlyAgentKvStore>;
    fn tools(&self) -> Arc<dyn ReadOnlyAgentToolRuns>;
}

#[derive(Clone)]
pub struct InMemoryAgentFsStore {
    inner: Arc<InMemoryStoreInner>,
}

struct InMemoryStoreInner {
    clock: Arc<dyn Clock>,
    _rng: Arc<dyn Rng>,
    allocator_block_size: u64,
    volumes: Mutex<BTreeMap<crate::VolumeId, Arc<InMemoryVolumeInner>>>,
}

impl InMemoryAgentFsStore {
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

#[async_trait]
impl AgentFsStore for InMemoryAgentFsStore {
    async fn open_volume(
        &self,
        config: AgentFsConfig,
    ) -> Result<Arc<dyn AgentFsVolume>, AgentFsError> {
        let chunk_size = configured_chunk_size(config.chunk_size)?;
        let mut volumes = self
            .inner
            .volumes
            .lock()
            .expect("in-memory volume registry lock poisoned");

        if let Some(existing) = volumes.get(&config.volume_id) {
            let info = existing.info();
            if info.chunk_size != chunk_size {
                return Err(AgentFsError::VolumeConfigMismatch {
                    volume_id: config.volume_id,
                    existing_chunk_size: info.chunk_size,
                    requested_chunk_size: chunk_size,
                });
            }

            return Ok(Arc::new(InMemoryAgentFsVolume {
                inner: existing.clone(),
            }));
        }

        if !config.create_if_missing {
            return Err(AgentFsError::VolumeNotFound {
                volume_id: config.volume_id,
            });
        }

        let info = AgentFsVolumeInfo {
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
        volumes.insert(config.volume_id, volume.clone());
        Ok(Arc::new(InMemoryAgentFsVolume { inner: volume }))
    }

    async fn clone_volume(
        &self,
        source: CloneVolumeSource,
        target: AgentFsConfig,
    ) -> Result<Arc<dyn AgentFsVolume>, AgentFsError> {
        let source_volume = {
            let volumes = self
                .inner
                .volumes
                .lock()
                .expect("in-memory volume registry lock poisoned");
            volumes
                .get(&source.volume_id)
                .cloned()
                .ok_or(AgentFsError::VolumeNotFound {
                    volume_id: source.volume_id,
                })?
        };

        let snapshot = source_volume.snapshot_state(source.durable);
        let chunk_size = target.chunk_size.unwrap_or(snapshot.info.chunk_size);
        if chunk_size == 0 {
            return Err(AgentFsError::InvalidChunkSize);
        }

        let target_info = AgentFsVolumeInfo {
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
            return Err(AgentFsError::VolumeAlreadyExists {
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
        volumes.insert(target.volume_id, volume.clone());
        Ok(Arc::new(InMemoryAgentFsVolume { inner: volume }))
    }

    async fn create_overlay(
        &self,
        base: Arc<dyn AgentFsSnapshot>,
        target: AgentFsConfig,
    ) -> Result<Arc<dyn AgentFsOverlay>, AgentFsError> {
        let base_snapshot = (base.as_ref() as &dyn Any)
            .downcast_ref::<InMemoryAgentFsSnapshot>()
            .ok_or(AgentFsError::IncompatibleSnapshotImplementation)?;
        let chunk_size = target
            .chunk_size
            .unwrap_or(base_snapshot.state.info.chunk_size);
        if chunk_size == 0 {
            return Err(AgentFsError::InvalidChunkSize);
        }

        let target_info = AgentFsVolumeInfo {
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
            return Err(AgentFsError::VolumeAlreadyExists {
                volume_id: target.volume_id,
            });
        }

        let volume = Arc::new(InMemoryVolumeInner::from_snapshot(
            target_info,
            self.inner.clock.clone(),
            &base_snapshot.state,
            self.inner.allocator_block_size,
            true,
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
        volumes.insert(target.volume_id, volume.clone());

        Ok(Arc::new(InMemoryAgentFsOverlay {
            volume: Arc::new(InMemoryAgentFsVolume { inner: volume }),
            base,
        }))
    }
}

#[derive(Clone)]
struct InMemoryAgentFsVolume {
    inner: Arc<InMemoryVolumeInner>,
}

#[async_trait]
impl AgentFsVolume for InMemoryAgentFsVolume {
    fn info(&self) -> AgentFsVolumeInfo {
        self.inner.info()
    }

    fn fs(&self) -> Arc<dyn AgentFileSystem> {
        Arc::new(InMemoryAgentFileSystem {
            volume: self.inner.clone(),
        })
    }

    fn kv(&self) -> Arc<dyn AgentKvStore> {
        Arc::new(InMemoryAgentKv {
            volume: self.inner.clone(),
        })
    }

    fn tools(&self) -> Arc<dyn AgentToolRuns> {
        Arc::new(InMemoryAgentTools {
            volume: self.inner.clone(),
        })
    }

    async fn snapshot(
        &self,
        opts: SnapshotOptions,
    ) -> Result<Arc<dyn AgentFsSnapshot>, AgentFsError> {
        Ok(Arc::new(InMemoryAgentFsSnapshot {
            state: Arc::new(self.inner.snapshot_state(opts.durable)),
        }))
    }

    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: ActivityOptions,
    ) -> Result<ActivityStream, AgentFsError> {
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

    async fn flush(&self) -> Result<(), AgentFsError> {
        self.inner.promote_durable_cut();
        Ok(())
    }
}

struct InMemoryAgentFsOverlay {
    volume: Arc<InMemoryAgentFsVolume>,
    base: Arc<dyn AgentFsSnapshot>,
}

#[async_trait]
impl AgentFsVolume for InMemoryAgentFsOverlay {
    fn info(&self) -> AgentFsVolumeInfo {
        self.volume.info()
    }

    fn fs(&self) -> Arc<dyn AgentFileSystem> {
        self.volume.fs()
    }

    fn kv(&self) -> Arc<dyn AgentKvStore> {
        self.volume.kv()
    }

    fn tools(&self) -> Arc<dyn AgentToolRuns> {
        self.volume.tools()
    }

    async fn snapshot(
        &self,
        opts: SnapshotOptions,
    ) -> Result<Arc<dyn AgentFsSnapshot>, AgentFsError> {
        self.volume.snapshot(opts).await
    }

    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: ActivityOptions,
    ) -> Result<ActivityStream, AgentFsError> {
        self.volume.activity_since(cursor, opts).await
    }

    fn subscribe_activity(&self, opts: ActivityOptions) -> ActivityReceiver {
        self.volume.subscribe_activity(opts)
    }

    async fn flush(&self) -> Result<(), AgentFsError> {
        self.volume.flush().await
    }
}

impl AgentFsOverlay for InMemoryAgentFsOverlay {
    fn base(&self) -> Arc<dyn AgentFsSnapshot> {
        self.base.clone()
    }
}

struct InMemoryAgentFsSnapshot {
    state: Arc<SnapshotState>,
}

impl AgentFsSnapshot for InMemoryAgentFsSnapshot {
    fn volume_id(&self) -> crate::VolumeId {
        self.state.info.volume_id
    }

    fn sequence(&self) -> SequenceNumber {
        self.state.sequence
    }

    fn durable(&self) -> bool {
        self.state.durable
    }

    fn info(&self) -> AgentFsVolumeInfo {
        self.state.info.clone()
    }

    fn fs(&self) -> Arc<dyn ReadOnlyAgentFileSystem> {
        Arc::new(InMemorySnapshotFileSystem {
            state: self.state.clone(),
        })
    }

    fn kv(&self) -> Arc<dyn ReadOnlyAgentKvStore> {
        Arc::new(InMemorySnapshotKv {
            state: self.state.clone(),
        })
    }

    fn tools(&self) -> Arc<dyn ReadOnlyAgentToolRuns> {
        Arc::new(InMemorySnapshotTools {
            state: self.state.clone(),
        })
    }
}

struct InMemoryAgentFileSystem {
    volume: Arc<InMemoryVolumeInner>,
}

struct InMemorySnapshotFileSystem {
    state: Arc<SnapshotState>,
}

struct InMemoryAgentKv {
    volume: Arc<InMemoryVolumeInner>,
}

struct InMemorySnapshotKv {
    state: Arc<SnapshotState>,
}

struct InMemoryAgentTools {
    volume: Arc<InMemoryVolumeInner>,
}

struct InMemorySnapshotTools {
    state: Arc<SnapshotState>,
}

#[async_trait]
impl ReadOnlyAgentFileSystem for InMemoryAgentFileSystem {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, AgentFsError> {
        self.lstat(path).await
    }

    async fn lstat(&self, path: &str) -> Result<Option<Stats>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        lookup_stats(&state.read_view(), path)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_file_bytes(&state.read_view(), path)
    }

    async fn pread(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        pread_file_bytes(&state.read_view(), path, offset, len)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_dir_entries(&state.read_view(), path)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_dir_entries_plus(&state.read_view(), path)
    }

    async fn readlink(&self, path: &str) -> Result<String, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_link_target(&state.read_view(), path)
    }
}

#[async_trait]
impl ReadOnlyAgentFileSystem for InMemorySnapshotFileSystem {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, AgentFsError> {
        self.lstat(path).await
    }

    async fn lstat(&self, path: &str) -> Result<Option<Stats>, AgentFsError> {
        lookup_stats(&self.state.read_view(), path)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, AgentFsError> {
        read_file_bytes(&self.state.read_view(), path)
    }

    async fn pread(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, AgentFsError> {
        pread_file_bytes(&self.state.read_view(), path, offset, len)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, AgentFsError> {
        read_dir_entries(&self.state.read_view(), path)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, AgentFsError> {
        read_dir_entries_plus(&self.state.read_view(), path)
    }

    async fn readlink(&self, path: &str) -> Result<String, AgentFsError> {
        read_link_target(&self.state.read_view(), path)
    }
}

#[async_trait]
impl AgentFileSystem for InMemoryAgentFileSystem {
    async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        opts: CreateOptions,
    ) -> Result<(), AgentFsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, now| {
            if path == "/" {
                return Err(AgentFsError::RootInvariant);
            }
            ensure_parent_directory(state, &path, opts.create_parents, now)?;
            let chunk_size = state.info.chunk_size;

            match state.paths.get(&path).copied() {
                Some(inode_id) => {
                    let inode = state
                        .inodes
                        .get_mut(&inode_id)
                        .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
                    match &mut inode.data {
                        InodeData::Directory => {
                            return Err(AgentFsError::IsDirectory { path: path.clone() });
                        }
                        InodeData::Symlink(_) if !opts.overwrite => {
                            return Err(AgentFsError::AlreadyExists { path: path.clone() });
                        }
                        InodeData::Symlink(_) => {
                            return Err(AgentFsError::NotFile { path: path.clone() });
                        }
                        InodeData::File(chunks) => {
                            if !opts.overwrite {
                                return Err(AgentFsError::AlreadyExists { path: path.clone() });
                            }
                            chunks.overwrite_all(&data, chunk_size);
                            inode.stats.size = data.len() as u64;
                            inode.stats.modified_at = now;
                            inode.stats.changed_at = now;
                            inode.stats.accessed_at = now;
                        }
                    }
                }
                None => {
                    let inode_id = allocate_inode(state);
                    let stats =
                        file_stats(inode_id, FileKind::File, opts.mode, now, data.len() as u64);
                    state.paths.insert(path.clone(), inode_id);
                    state.inodes.insert(
                        inode_id,
                        InodeRecord {
                            stats,
                            data: InodeData::File(FileChunks::from_bytes(&data, chunk_size)),
                        },
                    );
                }
            }

            Ok(Some((
                (),
                activity_spec(ActivityKind::FileWritten, Some(path), None, None),
            )))
        })
    }

    async fn pwrite(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), AgentFsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, now| {
            let inode_id = *state
                .paths
                .get(&path)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            let chunk_size = state.info.chunk_size;
            let inode = state
                .inodes
                .get_mut(&inode_id)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            let InodeData::File(chunks) = &mut inode.data else {
                return match inode.stats.kind {
                    FileKind::Directory => Err(AgentFsError::IsDirectory { path: path.clone() }),
                    _ => Err(AgentFsError::NotFile { path: path.clone() }),
                };
            };

            inode.stats.size = chunks.write_at(offset, &data, inode.stats.size, chunk_size);
            inode.stats.modified_at = now;
            inode.stats.changed_at = now;
            inode.stats.accessed_at = now;

            Ok(Some((
                (),
                activity_spec(ActivityKind::FilePatched, Some(path), None, None),
            )))
        })
    }

    async fn truncate(&self, path: &str, size: u64) -> Result<(), AgentFsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, now| {
            let inode_id = *state
                .paths
                .get(&path)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            let chunk_size = state.info.chunk_size;
            let inode = state
                .inodes
                .get_mut(&inode_id)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            let InodeData::File(chunks) = &mut inode.data else {
                return match inode.stats.kind {
                    FileKind::Directory => Err(AgentFsError::IsDirectory { path: path.clone() }),
                    _ => Err(AgentFsError::NotFile { path: path.clone() }),
                };
            };

            chunks.truncate(size, inode.stats.size, chunk_size);
            inode.stats.size = size;
            inode.stats.modified_at = now;
            inode.stats.changed_at = now;
            inode.stats.accessed_at = now;

            Ok(Some((
                (),
                activity_spec(ActivityKind::FileTruncated, Some(path), None, None),
            )))
        })
    }

    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<(), AgentFsError> {
        let path = normalize_path(path)?;
        if path == "/" {
            return if opts.recursive {
                Ok(())
            } else {
                Err(AgentFsError::AlreadyExists { path })
            };
        }

        self.volume.mutate(|state, now| {
            if let Some(inode_id) = state.paths.get(&path).copied() {
                let inode = state
                    .inodes
                    .get(&inode_id)
                    .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
                if inode.stats.kind == FileKind::Directory && opts.recursive {
                    return Ok(None);
                }
                return Err(AgentFsError::AlreadyExists { path: path.clone() });
            }

            ensure_parent_directory(state, &path, opts.recursive, now)?;
            let inode_id = allocate_inode(state);
            let stats = file_stats(inode_id, FileKind::Directory, opts.mode, now, 0);
            state.paths.insert(path.clone(), inode_id);
            state.inodes.insert(
                inode_id,
                InodeRecord {
                    stats,
                    data: InodeData::Directory,
                },
            );

            Ok(Some((
                (),
                activity_spec(ActivityKind::DirectoryCreated, Some(path), None, None),
            )))
        })
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), AgentFsError> {
        let from = normalize_path(from)?;
        let to = normalize_path(to)?;
        self.volume.mutate(|state, _now| {
            if from == "/" || to == "/" {
                return Err(AgentFsError::RootInvariant);
            }
            if !state.paths.contains_key(&from) {
                return Err(AgentFsError::NotFound { path: from.clone() });
            }
            if state.paths.contains_key(&to) {
                return Err(AgentFsError::AlreadyExists { path: to.clone() });
            }
            if is_descendant_path(&from, &to) {
                return Err(AgentFsError::InvalidPath { path: to.clone() });
            }

            ensure_existing_parent_directory(state, &to)?;
            let renames = state
                .paths
                .iter()
                .filter_map(|(path, inode)| {
                    rebase_path(path, &from, &to).map(|new_path| (path.clone(), new_path, *inode))
                })
                .collect::<Vec<_>>();

            for (old_path, _, _) in &renames {
                state.paths.remove(old_path);
            }
            for (_, new_path, inode) in renames {
                state.paths.insert(new_path, inode);
            }

            let mut metadata = BTreeMap::new();
            metadata.insert("to".to_string(), json!(to.clone()));
            Ok(Some((
                (),
                activity_spec(ActivityKind::PathRenamed, Some(from), None, Some(metadata)),
            )))
        })
    }

    async fn link(&self, from: &str, to: &str) -> Result<(), AgentFsError> {
        let from = normalize_path(from)?;
        let to = normalize_path(to)?;
        self.volume.mutate(|state, _now| {
            if to == "/" {
                return Err(AgentFsError::RootInvariant);
            }
            let inode_id = *state
                .paths
                .get(&from)
                .ok_or_else(|| AgentFsError::NotFound { path: from.clone() })?;
            ensure_existing_parent_directory(state, &to)?;
            if state.paths.contains_key(&to) {
                return Err(AgentFsError::AlreadyExists { path: to.clone() });
            }
            let kind = state
                .inodes
                .get(&inode_id)
                .ok_or_else(|| AgentFsError::NotFound { path: from.clone() })?
                .stats
                .kind;
            if kind == FileKind::Directory {
                return Err(AgentFsError::UnsupportedOperation {
                    operation: "hard-link directories",
                });
            }
            let inode = state
                .inodes
                .get_mut(&inode_id)
                .ok_or_else(|| AgentFsError::NotFound { path: from.clone() })?;
            inode.stats.nlink = inode.stats.nlink.saturating_add(1);
            state.paths.insert(to.clone(), inode_id);
            let mut metadata = BTreeMap::new();
            metadata.insert("from".to_string(), json!(from));
            Ok(Some((
                (),
                activity_spec(
                    ActivityKind::HardLinkCreated,
                    Some(to),
                    None,
                    Some(metadata),
                ),
            )))
        })
    }

    async fn symlink(&self, target: &str, linkpath: &str) -> Result<(), AgentFsError> {
        let linkpath = normalize_path(linkpath)?;
        let target = target.to_string();
        self.volume.mutate(|state, now| {
            if linkpath == "/" {
                return Err(AgentFsError::RootInvariant);
            }
            if state.paths.contains_key(&linkpath) {
                return Err(AgentFsError::AlreadyExists {
                    path: linkpath.clone(),
                });
            }
            ensure_existing_parent_directory(state, &linkpath)?;
            let inode_id = allocate_inode(state);
            let stats = file_stats(inode_id, FileKind::Symlink, 0o777, now, target.len() as u64);
            state.paths.insert(linkpath.clone(), inode_id);
            state.inodes.insert(
                inode_id,
                InodeRecord {
                    stats,
                    data: InodeData::Symlink(target),
                },
            );

            Ok(Some((
                (),
                activity_spec(ActivityKind::SymlinkCreated, Some(linkpath), None, None),
            )))
        })
    }

    async fn unlink(&self, path: &str) -> Result<(), AgentFsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, _now| {
            if path == "/" {
                return Err(AgentFsError::RootInvariant);
            }
            let inode_id = state
                .paths
                .remove(&path)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            let remove_inode = {
                let inode = state
                    .inodes
                    .get_mut(&inode_id)
                    .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
                if inode.stats.kind == FileKind::Directory {
                    return Err(AgentFsError::IsDirectory { path: path.clone() });
                }
                inode.stats.nlink = inode.stats.nlink.saturating_sub(1);
                inode.stats.nlink == 0
            };
            if remove_inode {
                state.inodes.remove(&inode_id);
            }
            Ok(Some((
                (),
                activity_spec(ActivityKind::PathDeleted, Some(path), None, None),
            )))
        })
    }

    async fn rmdir(&self, path: &str) -> Result<(), AgentFsError> {
        let path = normalize_path(path)?;
        self.volume.mutate(|state, _now| {
            if path == "/" {
                return Err(AgentFsError::RootInvariant);
            }
            let inode_id = *state
                .paths
                .get(&path)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            let inode = state
                .inodes
                .get(&inode_id)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            if inode.stats.kind != FileKind::Directory {
                return Err(AgentFsError::NotDirectory { path: path.clone() });
            }
            if state
                .paths
                .keys()
                .any(|candidate| candidate != &path && is_descendant_path(&path, candidate))
            {
                return Err(AgentFsError::DirectoryNotEmpty { path: path.clone() });
            }
            state.paths.remove(&path);
            state.inodes.remove(&inode_id);

            Ok(Some((
                (),
                activity_spec(ActivityKind::DirectoryRemoved, Some(path), None, None),
            )))
        })
    }

    async fn fsync(&self, path: Option<&str>) -> Result<(), AgentFsError> {
        if let Some(path) = path {
            let path = normalize_path(path)?;
            let state = self
                .volume
                .state
                .lock()
                .expect("volume state lock poisoned");
            if !state.paths.contains_key(&path) {
                return Err(AgentFsError::NotFound { path });
            }
        }

        self.volume.promote_durable_cut();
        Ok(())
    }
}

#[async_trait]
impl ReadOnlyAgentKvStore for InMemoryAgentKv {
    async fn get_json(&self, key: &str) -> Result<Option<JsonValue>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        Ok(state.kv.get(key).cloned())
    }

    async fn list_keys(&self) -> Result<Vec<String>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        Ok(state.kv.keys().cloned().collect())
    }
}

#[async_trait]
impl ReadOnlyAgentKvStore for InMemorySnapshotKv {
    async fn get_json(&self, key: &str) -> Result<Option<JsonValue>, AgentFsError> {
        Ok(self.state.kv.get(key).cloned())
    }

    async fn list_keys(&self) -> Result<Vec<String>, AgentFsError> {
        Ok(self.state.kv.keys().cloned().collect())
    }
}

#[async_trait]
impl AgentKvStore for InMemoryAgentKv {
    async fn set_json(&self, key: &str, value: JsonValue) -> Result<(), AgentFsError> {
        let key = key.to_string();
        self.volume.mutate(|state, _now| {
            state.kv.insert(key.clone(), value);
            Ok(Some((
                (),
                activity_spec(ActivityKind::KvSet, Some(key), None, None),
            )))
        })
    }

    async fn delete(&self, key: &str) -> Result<(), AgentFsError> {
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
impl ReadOnlyAgentToolRuns for InMemoryAgentTools {
    async fn get(&self, id: ToolRunId) -> Result<Option<ToolRun>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        Ok(state.tool_runs.get(&id).cloned())
    }

    async fn recent(&self, limit: Option<usize>) -> Result<Vec<ToolRun>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        Ok(recent_tool_runs(&state.tool_runs, limit))
    }
}

#[async_trait]
impl ReadOnlyAgentToolRuns for InMemorySnapshotTools {
    async fn get(&self, id: ToolRunId) -> Result<Option<ToolRun>, AgentFsError> {
        Ok(self.state.tool_runs.get(&id).cloned())
    }

    async fn recent(&self, limit: Option<usize>) -> Result<Vec<ToolRun>, AgentFsError> {
        Ok(recent_tool_runs(&self.state.tool_runs, limit))
    }
}

#[async_trait]
impl AgentToolRuns for InMemoryAgentTools {
    async fn start(
        &self,
        name: &str,
        params: Option<JsonValue>,
    ) -> Result<ToolRunId, AgentFsError> {
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

    async fn success(&self, id: ToolRunId, result: Option<JsonValue>) -> Result<(), AgentFsError> {
        self.volume.mutate(|state, now| {
            let run = state
                .tool_runs
                .get_mut(&id)
                .ok_or(AgentFsError::ToolRunNotFound { tool_run_id: id })?;
            if run.status != ToolRunStatus::Pending {
                return Err(AgentFsError::ToolRunAlreadyCompleted { tool_run_id: id });
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

    async fn error(&self, id: ToolRunId, message: String) -> Result<(), AgentFsError> {
        self.volume.mutate(|state, now| {
            let run = state
                .tool_runs
                .get_mut(&id)
                .ok_or(AgentFsError::ToolRunNotFound { tool_run_id: id })?;
            if run.status != ToolRunStatus::Pending {
                return Err(AgentFsError::ToolRunAlreadyCompleted { tool_run_id: id });
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
    ) -> Result<ToolRunId, AgentFsError> {
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
    fn new_empty(
        info: AgentFsVolumeInfo,
        clock: Arc<dyn Clock>,
        allocator_block_size: u64,
    ) -> Self {
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
        info: AgentFsVolumeInfo,
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

    fn info(&self) -> AgentFsVolumeInfo {
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

    fn mutate<T, F>(&self, f: F) -> Result<T, AgentFsError>
    where
        F: FnOnce(&mut VolumeState, Timestamp) -> Result<Option<(T, ActivitySpec)>, AgentFsError>,
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
    info: AgentFsVolumeInfo,
    sequence: SequenceNumber,
    durable: bool,
    paths: BTreeMap<String, InodeId>,
    inodes: BTreeMap<InodeId, InodeRecord>,
    kv: BTreeMap<String, JsonValue>,
    tool_runs: BTreeMap<ToolRunId, ToolRun>,
}

impl SnapshotState {
    fn read_view(&self) -> VolumeReadView<'_> {
        VolumeReadView::new(&self.info, &self.paths, &self.inodes)
    }
}

struct VolumeState {
    info: AgentFsVolumeInfo,
    sequence: SequenceNumber,
    allocators: BTreeMap<AllocatorKind, BlockLeaseAllocator>,
    paths: BTreeMap<String, InodeId>,
    inodes: BTreeMap<InodeId, InodeRecord>,
    kv: BTreeMap<String, JsonValue>,
    tool_runs: BTreeMap<ToolRunId, ToolRun>,
    activities: Vec<ActivityEntry>,
    durable: DurableViewState,
}

impl VolumeState {
    fn new(info: AgentFsVolumeInfo, allocator_block_size: u64) -> Self {
        let root = InodeRecord {
            stats: file_stats(
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
            activities: Vec::new(),
            durable,
        }
    }

    fn from_snapshot(
        info: AgentFsVolumeInfo,
        snapshot: &SnapshotState,
        allocator_block_size: u64,
        initialize_durable: bool,
    ) -> Self {
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
                inodes: snapshot.inodes.clone(),
                kv: snapshot.kv.clone(),
                tool_runs: snapshot.tool_runs.clone(),
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
            inodes: snapshot.inodes.clone(),
            kv: snapshot.kv.clone(),
            tool_runs: snapshot.tool_runs.clone(),
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
        }
    }

    fn read_view(&self) -> VolumeReadView<'_> {
        VolumeReadView::new(&self.info, &self.paths, &self.inodes)
    }

    fn promote_durable_cut(&mut self) {
        self.durable.sequence = self.sequence;
        self.durable.paths = self.paths.clone();
        self.durable.inodes = self.inodes.clone();
        self.durable.kv = self.kv.clone();
        self.durable.tool_runs = self.tool_runs.clone();
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
    File(FileChunks),
    Symlink(String),
}

#[derive(Clone, Default)]
struct DurableViewState {
    sequence: SequenceNumber,
    paths: BTreeMap<String, InodeId>,
    inodes: BTreeMap<InodeId, InodeRecord>,
    kv: BTreeMap<String, JsonValue>,
    tool_runs: BTreeMap<ToolRunId, ToolRun>,
    activity_len: usize,
}

impl DurableViewState {
    fn snapshot(&self, info: AgentFsVolumeInfo) -> SnapshotState {
        SnapshotState {
            info,
            sequence: self.sequence,
            durable: true,
            paths: self.paths.clone(),
            inodes: self.inodes.clone(),
            kv: self.kv.clone(),
            tool_runs: self.tool_runs.clone(),
        }
    }
}

#[derive(Clone, Default)]
struct FileChunks {
    chunks: BTreeMap<u64, Vec<u8>>,
}

impl FileChunks {
    fn from_bytes(bytes: &[u8], chunk_size: u32) -> Self {
        let mut chunks = BTreeMap::new();
        if bytes.is_empty() {
            return Self { chunks };
        }

        let chunk_len = chunk_size as usize;
        for (index, chunk) in bytes.chunks(chunk_len).enumerate() {
            chunks.insert(index as u64, chunk.to_vec());
        }
        Self { chunks }
    }

    fn read_all(&self, size: u64, chunk_size: u32) -> Vec<u8> {
        self.read_range(0, size, size, chunk_size)
    }

    fn read_range(&self, offset: u64, len: u64, size: u64, chunk_size: u32) -> Vec<u8> {
        if len == 0 || offset >= size {
            return Vec::new();
        }

        let end = size.min(offset.saturating_add(len));
        let first_chunk = offset / chunk_size as u64;
        let last_chunk = end.saturating_sub(1) / chunk_size as u64 + 1;
        let mut bytes = vec![0; end.saturating_sub(offset) as usize];

        for chunk in self.scan_range(first_chunk, last_chunk) {
            let chunk_start = chunk.chunk_index * chunk_size as u64;
            let chunk_end = chunk_start.saturating_add(chunk.bytes.len() as u64);
            let read_start = offset.max(chunk_start);
            let read_end = end.min(chunk_end);
            if read_start >= read_end {
                continue;
            }

            let source_start = (read_start - chunk_start) as usize;
            let source_end = source_start + (read_end - read_start) as usize;
            let target_start = (read_start - offset) as usize;
            let target_end = target_start + (read_end - read_start) as usize;
            bytes[target_start..target_end].copy_from_slice(&chunk.bytes[source_start..source_end]);
        }

        bytes
    }

    fn overwrite_all(&mut self, bytes: &[u8], chunk_size: u32) {
        *self = Self::from_bytes(bytes, chunk_size);
    }

    fn write_at(&mut self, offset: u64, data: &[u8], size: u64, chunk_size: u32) -> u64 {
        let mut bytes = self.read_all(size, chunk_size);
        let offset = offset as usize;
        if bytes.len() < offset {
            bytes.resize(offset, 0);
        }
        let end = offset.saturating_add(data.len());
        if bytes.len() < end {
            bytes.resize(end, 0);
        }
        bytes[offset..end].copy_from_slice(data);
        self.overwrite_all(&bytes, chunk_size);
        bytes.len() as u64
    }

    fn truncate(&mut self, size: u64, current_size: u64, chunk_size: u32) {
        let mut bytes = self.read_all(current_size, chunk_size);
        bytes.resize(size as usize, 0);
        self.overwrite_all(&bytes, chunk_size);
    }

    fn scan_range(&self, first_chunk: u64, last_chunk: u64) -> Vec<ChunkRecord> {
        self.chunks
            .range(first_chunk..last_chunk)
            .map(|(chunk_index, bytes)| ChunkRecord {
                chunk_index: *chunk_index,
                bytes: bytes.clone(),
            })
            .collect()
    }
}

#[derive(Clone)]
struct ChunkRecord {
    chunk_index: u64,
    bytes: Vec<u8>,
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

    #[allow(dead_code)]
    fn persisted_next(&self) -> u64 {
        self.next_persisted
    }

    #[allow(dead_code)]
    fn recover_after_crash(&self) -> Self {
        Self::new(self.next_persisted, self.block_size)
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

fn configured_chunk_size(chunk_size: Option<u32>) -> Result<u32, AgentFsError> {
    let chunk_size = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);
    if chunk_size == 0 {
        return Err(AgentFsError::InvalidChunkSize);
    }
    Ok(chunk_size)
}

fn file_stats(inode: InodeId, kind: FileKind, mode: u32, now: Timestamp, size: u64) -> Stats {
    Stats {
        inode,
        kind,
        mode,
        nlink: 1,
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

fn normalize_path(path: &str) -> Result<String, AgentFsError> {
    if !path.starts_with('/') {
        return Err(AgentFsError::InvalidPath {
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
            return Err(AgentFsError::InvalidPath {
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

fn ensure_parent_directory(
    state: &mut VolumeState,
    path: &str,
    create_parents: bool,
    now: Timestamp,
) -> Result<(), AgentFsError> {
    let Some(parent) = parent_path(path) else {
        return Err(AgentFsError::RootInvariant);
    };

    if create_parents {
        create_missing_directories(state, &parent, now)?;
    }
    ensure_existing_parent_directory(state, path)
}

fn create_missing_directories(
    state: &mut VolumeState,
    path: &str,
    now: Timestamp,
) -> Result<(), AgentFsError> {
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
                .ok_or_else(|| AgentFsError::NotDirectory {
                    path: current.clone(),
                })?;
            if inode.stats.kind != FileKind::Directory {
                return Err(AgentFsError::NotDirectory {
                    path: current.clone(),
                });
            }
            continue;
        }

        let inode_id = allocate_inode(state);
        let stats = file_stats(inode_id, FileKind::Directory, 0o755, now, 0);
        state.paths.insert(current.clone(), inode_id);
        state.inodes.insert(
            inode_id,
            InodeRecord {
                stats,
                data: InodeData::Directory,
            },
        );
    }

    Ok(())
}

fn ensure_existing_parent_directory(state: &VolumeState, path: &str) -> Result<(), AgentFsError> {
    let Some(parent) = parent_path(path) else {
        return Err(AgentFsError::RootInvariant);
    };
    let inode_id = state
        .paths
        .get(&parent)
        .ok_or_else(|| AgentFsError::NotFound {
            path: parent.clone(),
        })?;
    let inode = state
        .inodes
        .get(inode_id)
        .ok_or_else(|| AgentFsError::NotDirectory {
            path: parent.clone(),
        })?;
    if inode.stats.kind != FileKind::Directory {
        return Err(AgentFsError::NotDirectory { path: parent });
    }
    Ok(())
}

struct VolumeReadView<'a> {
    info: &'a AgentFsVolumeInfo,
    paths: &'a BTreeMap<String, InodeId>,
    inodes: &'a BTreeMap<InodeId, InodeRecord>,
}

impl<'a> VolumeReadView<'a> {
    fn new(
        info: &'a AgentFsVolumeInfo,
        paths: &'a BTreeMap<String, InodeId>,
        inodes: &'a BTreeMap<InodeId, InodeRecord>,
    ) -> Self {
        Self {
            info,
            paths,
            inodes,
        }
    }

    fn resolve_path(&self, path: &str) -> Result<Option<ResolvedPath<'a>>, AgentFsError> {
        let path = normalize_path(path)?;
        let Some(inode_id) = self.paths.get(&path).copied() else {
            return Ok(None);
        };
        let inode = self
            .inodes
            .get(&inode_id)
            .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
        Ok(Some(ResolvedPath { path, inode }))
    }

    #[allow(dead_code)]
    fn parent_lookup(&self, path: &str) -> Result<ResolvedPath<'a>, AgentFsError> {
        let path = normalize_path(path)?;
        let Some(parent) = parent_path(&path) else {
            return Err(AgentFsError::RootInvariant);
        };
        let Some(resolved) = self.resolve_path(&parent)? else {
            return Err(AgentFsError::NotFound { path: parent });
        };
        if resolved.inode.stats.kind != FileKind::Directory {
            return Err(AgentFsError::NotDirectory {
                path: resolved.path.clone(),
            });
        }
        Ok(resolved)
    }

    fn dentry_scan(&self, path: &str) -> Result<Vec<ScannedDentry<'a>>, AgentFsError> {
        let resolved = self
            .resolve_path(path)?
            .ok_or_else(|| AgentFsError::NotFound {
                path: normalize_path(path).unwrap_or_else(|_| path.to_string()),
            })?;
        if resolved.inode.stats.kind != FileKind::Directory {
            return Err(AgentFsError::NotDirectory {
                path: resolved.path.clone(),
            });
        }

        let mut entries = Vec::new();
        for (candidate, inode_id) in self.paths {
            if let Some(name) = direct_child_name(&resolved.path, candidate) {
                let inode = self
                    .inodes
                    .get(inode_id)
                    .ok_or_else(|| AgentFsError::NotFound {
                        path: candidate.clone(),
                    })?;
                entries.push(ScannedDentry {
                    name,
                    inode_id: *inode_id,
                    inode,
                });
            }
        }
        Ok(entries)
    }

    fn chunk_range_scan(
        &self,
        resolved: &ResolvedPath<'a>,
        offset: u64,
        len: u64,
    ) -> Result<Vec<ChunkRecord>, AgentFsError> {
        match &resolved.inode.data {
            InodeData::File(chunks) => {
                if len == 0 || offset >= resolved.inode.stats.size {
                    return Ok(Vec::new());
                }
                let chunk_size = self.info.chunk_size as u64;
                let first_chunk = offset / chunk_size;
                let end = resolved.inode.stats.size.min(offset.saturating_add(len));
                let last_chunk = end.saturating_sub(1) / chunk_size + 1;
                Ok(chunks.scan_range(first_chunk, last_chunk))
            }
            InodeData::Directory => Err(AgentFsError::IsDirectory {
                path: resolved.path.clone(),
            }),
            InodeData::Symlink(_) => Err(AgentFsError::NotFile {
                path: resolved.path.clone(),
            }),
        }
    }
}

struct ResolvedPath<'a> {
    path: String,
    inode: &'a InodeRecord,
}

struct ScannedDentry<'a> {
    name: String,
    inode_id: InodeId,
    inode: &'a InodeRecord,
}

fn lookup_stats(view: &VolumeReadView<'_>, path: &str) -> Result<Option<Stats>, AgentFsError> {
    Ok(view
        .resolve_path(path)?
        .map(|resolved| resolved.inode.stats.clone()))
}

fn read_file_bytes(view: &VolumeReadView<'_>, path: &str) -> Result<Option<Vec<u8>>, AgentFsError> {
    let Some(resolved) = view.resolve_path(path)? else {
        return Ok(None);
    };

    match &resolved.inode.data {
        InodeData::File(chunks) => Ok(Some(
            chunks.read_all(resolved.inode.stats.size, view.info.chunk_size),
        )),
        InodeData::Directory => Err(AgentFsError::IsDirectory {
            path: resolved.path,
        }),
        InodeData::Symlink(_) => Err(AgentFsError::NotFile {
            path: resolved.path,
        }),
    }
}

fn pread_file_bytes(
    view: &VolumeReadView<'_>,
    path: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>, AgentFsError> {
    let Some(resolved) = view.resolve_path(path)? else {
        return Ok(None);
    };

    match &resolved.inode.data {
        InodeData::File(chunks) => {
            let _scan = view.chunk_range_scan(&resolved, offset, len)?;
            Ok(Some(chunks.read_range(
                offset,
                len,
                resolved.inode.stats.size,
                view.info.chunk_size,
            )))
        }
        InodeData::Directory => Err(AgentFsError::IsDirectory {
            path: resolved.path,
        }),
        InodeData::Symlink(_) => Err(AgentFsError::NotFile {
            path: resolved.path,
        }),
    }
}

fn read_dir_entries(view: &VolumeReadView<'_>, path: &str) -> Result<Vec<DirEntry>, AgentFsError> {
    view.dentry_scan(path)?
        .into_iter()
        .map(|entry| {
            Ok(DirEntry {
                name: entry.name,
                inode: entry.inode_id,
                kind: entry.inode.stats.kind,
            })
        })
        .collect()
}

fn read_dir_entries_plus(
    view: &VolumeReadView<'_>,
    path: &str,
) -> Result<Vec<DirEntryPlus>, AgentFsError> {
    view.dentry_scan(path)?
        .into_iter()
        .map(|entry| {
            Ok(DirEntryPlus {
                entry: DirEntry {
                    name: entry.name,
                    inode: entry.inode_id,
                    kind: entry.inode.stats.kind,
                },
                stats: entry.inode.stats.clone(),
            })
        })
        .collect()
}

fn read_link_target(view: &VolumeReadView<'_>, path: &str) -> Result<String, AgentFsError> {
    let resolved = view
        .resolve_path(path)?
        .ok_or_else(|| AgentFsError::NotFound {
            path: normalize_path(path).unwrap_or_else(|_| path.to_string()),
        })?;
    match &resolved.inode.data {
        InodeData::Symlink(target) => Ok(target.clone()),
        _ => Err(AgentFsError::NotSymlink {
            path: resolved.path,
        }),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_volume_info() -> AgentFsVolumeInfo {
        AgentFsVolumeInfo {
            volume_id: crate::VolumeId::new(1),
            chunk_size: 4,
            format_version: VFS_FORMAT_VERSION,
            root_inode: ROOT_INODE_ID,
            created_at: Timestamp::new(7),
            overlay_base: None,
        }
    }

    #[test]
    fn block_allocator_recovers_monotonicity_after_crash_during_lease_refresh() {
        let mut allocator = BlockLeaseAllocator::new(2, 4);
        allocator.refresh_lease();
        assert_eq!(allocator.persisted_next(), 6);

        let mut recovered = allocator.recover_after_crash();
        assert_eq!(recovered.allocate(), 6);
    }

    #[test]
    fn durable_cut_promotes_visible_state_without_mixing_versions() {
        let mut state = VolumeState::new(test_volume_info(), 2);
        let inode = allocate_inode(&mut state);
        state.paths.insert("/notes.txt".to_string(), inode);
        state.inodes.insert(
            inode,
            InodeRecord {
                stats: file_stats(inode, FileKind::File, 0o644, Timestamp::new(9), 8),
                data: InodeData::File(FileChunks::from_bytes(b"abcdefgh", 4)),
            },
        );
        append_activity(
            &mut state,
            Timestamp::new(9),
            ActivityKind::FileWritten,
            Some("/notes.txt".to_string()),
            None,
            BTreeMap::new(),
        );

        let durable_before = state.durable.snapshot(state.info.clone());
        assert_eq!(
            read_file_bytes(&durable_before.read_view(), "/notes.txt").expect("read durable"),
            None
        );

        state.promote_durable_cut();
        let durable_after = state.durable.snapshot(state.info.clone());
        assert_eq!(
            read_file_bytes(&durable_after.read_view(), "/notes.txt").expect("read durable"),
            Some(b"abcdefgh".to_vec())
        );
    }
}
