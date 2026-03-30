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
    AgentFileSystem, AgentFsError, AgentKvStore, AgentToolRuns, CreateOptions, DirEntry,
    DirEntryPlus, FileKind, InodeId, JsonValue, MkdirOptions, ReadOnlyAgentFileSystem,
    ReadOnlyAgentKvStore, ReadOnlyAgentToolRuns, Stats, ToolRun, ToolRunId, ToolRunStatus,
};

pub const VFS_FORMAT_VERSION: u8 = 1;
pub const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024;
pub const ROOT_INODE_ID: InodeId = InodeId::new(1);

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
    volumes: Mutex<BTreeMap<crate::VolumeId, Arc<InMemoryVolumeInner>>>,
}

impl InMemoryAgentFsStore {
    pub fn new(clock: Arc<dyn Clock>, rng: Arc<dyn Rng>) -> Self {
        Self {
            inner: Arc::new(InMemoryStoreInner {
                clock,
                _rng: rng,
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
            let mut entries = state
                .activities
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

    fn subscribe_activity(&self, _opts: ActivityOptions) -> ActivityReceiver {
        ActivityReceiver::new(self.inner.activity_watch.subscribe())
    }

    async fn flush(&self) -> Result<(), AgentFsError> {
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
        lookup_stats(&state.paths, &state.inodes, path)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_file_bytes(&state.paths, &state.inodes, path)
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
        pread_file_bytes(&state.paths, &state.inodes, path, offset, len)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_dir_entries(&state.paths, &state.inodes, path)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_dir_entries_plus(&state.paths, &state.inodes, path)
    }

    async fn readlink(&self, path: &str) -> Result<String, AgentFsError> {
        let state = self
            .volume
            .state
            .lock()
            .expect("volume state lock poisoned");
        read_link_target(&state.paths, &state.inodes, path)
    }
}

#[async_trait]
impl ReadOnlyAgentFileSystem for InMemorySnapshotFileSystem {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, AgentFsError> {
        self.lstat(path).await
    }

    async fn lstat(&self, path: &str) -> Result<Option<Stats>, AgentFsError> {
        lookup_stats(&self.state.paths, &self.state.inodes, path)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, AgentFsError> {
        read_file_bytes(&self.state.paths, &self.state.inodes, path)
    }

    async fn pread(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, AgentFsError> {
        pread_file_bytes(&self.state.paths, &self.state.inodes, path, offset, len)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, AgentFsError> {
        read_dir_entries(&self.state.paths, &self.state.inodes, path)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, AgentFsError> {
        read_dir_entries_plus(&self.state.paths, &self.state.inodes, path)
    }

    async fn readlink(&self, path: &str) -> Result<String, AgentFsError> {
        read_link_target(&self.state.paths, &self.state.inodes, path)
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
                        InodeData::File(bytes) => {
                            if !opts.overwrite {
                                return Err(AgentFsError::AlreadyExists { path: path.clone() });
                            }
                            *bytes = data;
                            inode.stats.size = bytes.len() as u64;
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
                            data: InodeData::File(data),
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
            let inode = state
                .inodes
                .get_mut(&inode_id)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            let InodeData::File(bytes) = &mut inode.data else {
                return match inode.stats.kind {
                    FileKind::Directory => Err(AgentFsError::IsDirectory { path: path.clone() }),
                    _ => Err(AgentFsError::NotFile { path: path.clone() }),
                };
            };

            let offset = offset as usize;
            if bytes.len() < offset {
                bytes.resize(offset, 0);
            }
            let end = offset.saturating_add(data.len());
            if bytes.len() < end {
                bytes.resize(end, 0);
            }
            bytes[offset..end].copy_from_slice(&data);
            inode.stats.size = bytes.len() as u64;
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
            let inode = state
                .inodes
                .get_mut(&inode_id)
                .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
            let InodeData::File(bytes) = &mut inode.data else {
                return match inode.stats.kind {
                    FileKind::Directory => Err(AgentFsError::IsDirectory { path: path.clone() }),
                    _ => Err(AgentFsError::NotFile { path: path.clone() }),
                };
            };

            bytes.resize(size as usize, 0);
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
}

impl InMemoryVolumeInner {
    fn new_empty(info: AgentFsVolumeInfo, clock: Arc<dyn Clock>) -> Self {
        let (activity_watch, _receiver) = watch::channel(SequenceNumber::new(0));
        Self {
            clock,
            state: Mutex::new(VolumeState::new(info)),
            activity_watch,
        }
    }

    fn from_snapshot(
        info: AgentFsVolumeInfo,
        clock: Arc<dyn Clock>,
        snapshot: &SnapshotState,
    ) -> Self {
        let (activity_watch, _receiver) = watch::channel(SequenceNumber::new(0));
        Self {
            clock,
            state: Mutex::new(VolumeState::from_snapshot(info, snapshot)),
            activity_watch,
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
        SnapshotState {
            info: state.info.clone(),
            sequence: state.sequence,
            durable,
            paths: state.paths.clone(),
            inodes: state.inodes.clone(),
            kv: state.kv.clone(),
            tool_runs: state.tool_runs.clone(),
        }
    }

    fn append_activity(
        &self,
        kind: ActivityKind,
        subject: Option<String>,
        tool_run_id: Option<ToolRunId>,
        metadata: BTreeMap<String, JsonValue>,
    ) {
        let next_sequence = {
            let mut state = self.state.lock().expect("volume state lock poisoned");
            append_activity(
                &mut state,
                self.clock.now(),
                kind,
                subject,
                tool_run_id,
                metadata,
            );
            state.sequence
        };
        self.activity_watch.send_replace(next_sequence);
    }

    fn mutate<T, F>(&self, f: F) -> Result<T, AgentFsError>
    where
        F: FnOnce(&mut VolumeState, Timestamp) -> Result<Option<(T, ActivitySpec)>, AgentFsError>,
        T: Default,
    {
        let now = self.clock.now();
        let (result, next_sequence) = {
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
            (result, state.sequence)
        };
        self.activity_watch.send_replace(next_sequence);
        Ok(result)
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

struct VolumeState {
    info: AgentFsVolumeInfo,
    sequence: SequenceNumber,
    next_inode: u64,
    next_activity: u64,
    next_tool_run_id: u64,
    paths: BTreeMap<String, InodeId>,
    inodes: BTreeMap<InodeId, InodeRecord>,
    kv: BTreeMap<String, JsonValue>,
    tool_runs: BTreeMap<ToolRunId, ToolRun>,
    activities: Vec<ActivityEntry>,
}

impl VolumeState {
    fn new(info: AgentFsVolumeInfo) -> Self {
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

        Self {
            info,
            sequence: SequenceNumber::new(0),
            next_inode: ROOT_INODE_ID.get() + 1,
            next_activity: 1,
            next_tool_run_id: 1,
            paths,
            inodes,
            kv: BTreeMap::new(),
            tool_runs: BTreeMap::new(),
            activities: Vec::new(),
        }
    }

    fn from_snapshot(info: AgentFsVolumeInfo, snapshot: &SnapshotState) -> Self {
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

        Self {
            info,
            sequence: SequenceNumber::new(0),
            next_inode,
            next_activity: 1,
            next_tool_run_id,
            paths: snapshot.paths.clone(),
            inodes: snapshot.inodes.clone(),
            kv: snapshot.kv.clone(),
            tool_runs: snapshot.tool_runs.clone(),
            activities: Vec::new(),
        }
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
    File(Vec<u8>),
    Symlink(String),
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
    let activity_id = ActivityId::new(state.next_activity);
    state.next_activity = state.next_activity.saturating_add(1);
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
    let inode = InodeId::new(state.next_inode);
    state.next_inode = state.next_inode.saturating_add(1);
    inode
}

fn allocate_tool_run_id(state: &mut VolumeState) -> ToolRunId {
    let tool_run_id = ToolRunId::new(state.next_tool_run_id);
    state.next_tool_run_id = state.next_tool_run_id.saturating_add(1);
    tool_run_id
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

fn lookup_stats(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<Option<Stats>, AgentFsError> {
    let path = normalize_path(path)?;
    let Some(inode_id) = paths.get(&path) else {
        return Ok(None);
    };
    let inode = inodes
        .get(inode_id)
        .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
    Ok(Some(inode.stats.clone()))
}

fn read_file_bytes(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<Option<Vec<u8>>, AgentFsError> {
    let path = normalize_path(path)?;
    let Some(inode_id) = paths.get(&path) else {
        return Ok(None);
    };
    let inode = inodes
        .get(inode_id)
        .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
    match &inode.data {
        InodeData::File(bytes) => Ok(Some(bytes.clone())),
        InodeData::Directory => Err(AgentFsError::IsDirectory { path }),
        InodeData::Symlink(_) => Err(AgentFsError::NotFile { path }),
    }
}

fn pread_file_bytes(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>, AgentFsError> {
    let Some(bytes) = read_file_bytes(paths, inodes, path)? else {
        return Ok(None);
    };
    let offset = offset as usize;
    if offset >= bytes.len() {
        return Ok(Some(Vec::new()));
    }
    let end = bytes.len().min(offset.saturating_add(len as usize));
    Ok(Some(bytes[offset..end].to_vec()))
}

fn read_dir_entries(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<Vec<DirEntry>, AgentFsError> {
    let path = normalize_path(path)?;
    ensure_directory(paths, inodes, &path)?;
    let mut entries = Vec::new();
    for (candidate, inode_id) in paths {
        if let Some(name) = direct_child_name(&path, candidate) {
            let inode = inodes.get(inode_id).ok_or_else(|| AgentFsError::NotFound {
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

fn read_dir_entries_plus(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<Vec<DirEntryPlus>, AgentFsError> {
    read_dir_entries(paths, inodes, path)?
        .into_iter()
        .map(|entry| {
            let stats = inodes
                .get(&entry.inode)
                .ok_or_else(|| AgentFsError::NotFound {
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
) -> Result<String, AgentFsError> {
    let path = normalize_path(path)?;
    let inode_id = paths
        .get(&path)
        .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
    let inode = inodes
        .get(inode_id)
        .ok_or_else(|| AgentFsError::NotFound { path: path.clone() })?;
    match &inode.data {
        InodeData::Symlink(target) => Ok(target.clone()),
        _ => Err(AgentFsError::NotSymlink { path }),
    }
}

fn ensure_directory(
    paths: &BTreeMap<String, InodeId>,
    inodes: &BTreeMap<InodeId, InodeRecord>,
    path: &str,
) -> Result<(), AgentFsError> {
    let inode_id = paths.get(path).ok_or_else(|| AgentFsError::NotFound {
        path: path.to_string(),
    })?;
    let inode = inodes
        .get(inode_id)
        .ok_or_else(|| AgentFsError::NotDirectory {
            path: path.to_string(),
        })?;
    if inode.stats.kind != FileKind::Directory {
        return Err(AgentFsError::NotDirectory {
            path: path.to_string(),
        });
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
