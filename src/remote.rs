use std::{collections::BTreeMap, fmt, ops::Range, path::PathBuf, sync::Arc};

use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    config::S3Location,
    error::{StorageError, StorageErrorKind},
    ids::{ManifestId, SegmentId, SequenceNumber, TableId},
    io::{DbDependencies, FileHandle, FileSystem, ObjectStore, OpenOptions},
};

const LOCAL_READ_CHUNK_BYTES: usize = 64 * 1024;
const CACHE_FORMAT_VERSION: u32 = 1;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
}

pub struct RemoteCache {
    file_system: Arc<dyn FileSystem>,
    root: String,
    data_dir: String,
    metadata_dir: String,
    entries: Mutex<BTreeMap<String, Vec<CacheIndexEntry>>>,
}

impl fmt::Debug for RemoteCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteCache")
            .field("root", &self.root)
            .field("data_dir", &self.data_dir)
            .field("metadata_dir", &self.metadata_dir)
            .field("entry_count", &self.entry_count())
            .finish()
    }
}

impl RemoteCache {
    pub async fn open(
        file_system: Arc<dyn FileSystem>,
        root: impl Into<String>,
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
            entries: Mutex::new(BTreeMap::new()),
        };
        cache.rebuild_index().await?;
        Ok(cache)
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn entry_count(&self) -> usize {
        lock(&self.entries).values().map(Vec::len).sum()
    }

    async fn rebuild_index(&self) -> Result<(), StorageError> {
        let listed = self.file_system.list(&self.metadata_dir).await?;
        let mut rebuilt = BTreeMap::new();

        for metadata_path in listed.into_iter().filter(|path| path.ends_with(".json")) {
            let Some(entry) = self.load_index_entry(&metadata_path).await? else {
                continue;
            };
            rebuilt
                .entry(entry.record.object_key.clone())
                .or_insert_with(Vec::new)
                .push(entry);
        }

        *lock(&self.entries) = rebuilt;
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
        let file: CacheEntryFile = match serde_json::from_slice(&bytes) {
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

    fn lookup_covering_entry(
        &self,
        object_key: &str,
        range: Range<u64>,
    ) -> Option<CacheIndexEntry> {
        let entries = lock(&self.entries);
        let object_entries = entries.get(object_key)?;
        object_entries.iter().find_map(|entry| {
            entry
                .record
                .span
                .contains(range.clone())
                .then(|| entry.clone())
        })
    }

    pub async fn read_full(&self, object_key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(entry) = self.lookup_covering_entry(object_key, 0..0) else {
            return Ok(None);
        };
        if entry.record.span != CacheSpan::Full {
            return Ok(None);
        }
        read_local_file_all(self.file_system.as_ref(), &entry.data_path)
            .await
            .map(Some)
    }

    pub async fn read_range(
        &self,
        object_key: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if range.start >= range.end {
            return Ok(Some(Vec::new()));
        }

        let Some(entry) = self.lookup_covering_entry(object_key, range.clone()) else {
            return Ok(None);
        };
        let bytes = read_local_file_all(self.file_system.as_ref(), &entry.data_path).await?;
        let slice = match entry.record.span {
            CacheSpan::Full => {
                if range.start as usize >= bytes.len() {
                    Vec::new()
                } else {
                    let end = (range.end as usize).min(bytes.len());
                    bytes[range.start as usize..end].to_vec()
                }
            }
            CacheSpan::Range { start, end: _ } => {
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
        };
        Ok(Some(slice))
    }

    pub async fn store(
        &self,
        object_key: &str,
        span: CacheSpan,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
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
        let payload = serde_json::to_vec_pretty(&metadata).map_err(|error| {
            StorageError::corruption(format!("encode remote cache metadata failed: {error}"))
        })?;
        write_bytes_atomic(self.file_system.as_ref(), &metadata_path, &payload).await?;
        self.file_system.sync_dir(&self.data_dir).await?;
        self.file_system.sync_dir(&self.metadata_dir).await?;

        let mut entries = lock(&self.entries);
        let object_entries = entries.entry(object_key.to_string()).or_default();
        object_entries.retain(|entry| entry.record.span != span);
        object_entries.push(CacheIndexEntry {
            record: metadata.record,
            metadata_path,
            data_path,
        });
        Ok(())
    }

    pub async fn remove_object(&self, object_key: &str) -> Result<(), StorageError> {
        let removed = lock(&self.entries).remove(object_key).unwrap_or_default();
        for entry in removed {
            self.file_system.delete(&entry.data_path).await?;
            self.file_system.delete(&entry.metadata_path).await?;
        }
        self.file_system.sync_dir(&self.data_dir).await?;
        self.file_system.sync_dir(&self.metadata_dir).await?;
        Ok(())
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
                if let Some(cache) = &self.cache
                    && let Ok(Some(bytes)) = cache.read_range(key, range.clone()).await
                {
                    return Ok(bytes);
                }

                let bytes = self
                    .object_store
                    .get_range(key, range.start, range.end)
                    .await
                    .map_err(|error| {
                        let error =
                            RemoteStorageError::classify(RemoteOperation::GetRange, key, error);
                        emit_remote_error(&error);
                        UnifiedStorageError::Remote(error)
                    })?;
                if let Some(cache) = &self.cache {
                    cache
                        .store(
                            key,
                            CacheSpan::Range {
                                start: range.start,
                                end: range.end,
                            },
                            &bytes,
                        )
                        .await
                        .map_err(UnifiedStorageError::Local)?;
                }
                Ok(bytes)
            }
        }
    }

    pub async fn read_ranges(
        &self,
        source: &StorageSource,
        ranges: &[Range<u64>],
    ) -> Result<Vec<u8>, UnifiedStorageError> {
        let mut stitched = Vec::new();
        for range in ranges {
            stitched.extend_from_slice(&self.read_range(source, range.clone()).await?);
        }
        Ok(stitched)
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
            cache
                .store(key, CacheSpan::Full, data)
                .await
                .map_err(|error| {
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
            atomic::{AtomicU64, Ordering},
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

    #[test]
    fn object_key_layout_is_stable() {
        let layout = ObjectKeyLayout::new(&S3Location {
            bucket: "bucket".to_string(),
            prefix: "/tenant-a/db-01/".to_string(),
        });

        assert_eq!(layout.backup_prefix(), "tenant-a/db-01/backup");
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
    async fn range_reads_fetch_exact_windows_and_stitch_them() {
        let cache_root = unique_test_dir("range-reads");
        let object_root = unique_test_dir("range-reads-objects");
        let file_system = Arc::new(TokioFileSystem::new());
        let inner_store: Arc<dyn ObjectStore> = Arc::new(LocalDirObjectStore::new(&object_root));
        let store = Arc::new(CountingObjectStore::new(inner_store));
        let cache = Arc::new(
            RemoteCache::open(
                file_system.clone(),
                cache_root.to_string_lossy().into_owned(),
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
        assert_eq!(
            store.range_calls(),
            vec![
                (key.to_string(), 2, 5),
                (key.to_string(), 10, 13),
                (key.to_string(), 20, 26)
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
