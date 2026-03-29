use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt, fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    CopyOptions as StandardCopyOptions, Error as StandardObjectStoreError,
    GetOptions as StandardGetOptions, GetResult as StandardGetResult,
    ListResult as StandardListResult, MultipartUpload as StandardMultipartUpload,
    ObjectMeta as StandardObjectMeta, ObjectStore as StandardObjectStore,
    ObjectStoreExt as StandardObjectStoreExt, PutMultipartOptions as StandardPutMultipartOptions,
    PutOptions as StandardPutOptions, PutPayload, PutResult as StandardPutResult,
    local::LocalFileSystem, memory::InMemory, path::Path as StandardObjectPath,
};
use parking_lot::{Mutex, MutexGuard};
use rand::{Rng as _, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};
use tokio::{sync::watch, task};
use uuid::Builder as UuidBuilder;
use walkdir::WalkDir;

use crate::{
    error::StorageError,
    ids::Timestamp,
    io::{Clock, FileHandle, FileSystem, ObjectStore, OpenOptions, Rng},
};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}

fn uuid_from_words(a: u64, b: u64) -> String {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&a.to_be_bytes());
    bytes[8..].copy_from_slice(&b.to_be_bytes());
    UuidBuilder::from_random_bytes(bytes)
        .into_uuid()
        .hyphenated()
        .to_string()
}

fn map_io_error(action: &str, target: &str, error: std::io::Error) -> StorageError {
    match error.kind() {
        std::io::ErrorKind::NotFound => {
            StorageError::not_found(format!("{action} failed for {target}: {error}"))
        }
        std::io::ErrorKind::TimedOut => {
            StorageError::timeout(format!("{action} timed out for {target}: {error}"))
        }
        std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof => {
            StorageError::corruption(format!("{action} failed for {target}: {error}"))
        }
        _ => StorageError::io(format!("{action} failed for {target}: {error}")),
    }
}

async fn run_blocking<T, F>(action: &'static str, target: String, f: F) -> Result<T, StorageError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, StorageError> + Send + 'static,
{
    task::spawn_blocking(f)
        .await
        .map_err(|error| StorageError::io(format!("{action} failed for {target}: {error}")))?
}

fn collect_files(dir: &Path, files: &mut Vec<String>) -> Result<(), StorageError> {
    if !dir.exists() {
        return Ok(());
    }

    for entry in WalkDir::new(dir) {
        let entry = entry.map_err(|error| {
            let target = error.path().unwrap_or(dir);
            match error.io_error() {
                Some(io_error) => map_io_error(
                    "list directory",
                    target.to_string_lossy().as_ref(),
                    std::io::Error::new(io_error.kind(), io_error.to_string()),
                ),
                None => StorageError::io(format!(
                    "list directory failed for {}: {error}",
                    target.to_string_lossy()
                )),
            }
        })?;
        if entry.file_type().is_file() {
            files.push(entry.path().to_string_lossy().into_owned());
        }
    }

    Ok(())
}

fn standard_object_key(key: &str) -> Result<StandardObjectPath, StorageError> {
    let path = StandardObjectPath::parse(key)
        .map_err(|error| StorageError::unsupported(format!("invalid object key {key}: {error}")))?;
    if path.as_ref().is_empty() {
        return Err(StorageError::unsupported("object key cannot be empty"));
    }
    Ok(path)
}

fn standard_object_prefix(prefix: &str) -> Result<Option<StandardObjectPath>, StorageError> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }

    StandardObjectPath::parse(prefix)
        .map(Some)
        .map_err(|error| {
            StorageError::unsupported(format!("invalid object prefix {prefix}: {error}"))
        })
}

fn map_standard_object_store_error(error: StandardObjectStoreError) -> StorageError {
    match error {
        StandardObjectStoreError::NotFound { source, .. } => source
            .downcast_ref::<StorageError>()
            .cloned()
            .unwrap_or_else(|| StorageError::not_found(source.to_string())),
        StandardObjectStoreError::NotSupported { source } => source
            .downcast_ref::<StorageError>()
            .cloned()
            .unwrap_or_else(|| StorageError::unsupported(source.to_string())),
        StandardObjectStoreError::Generic { source, .. }
        | StandardObjectStoreError::AlreadyExists { source, .. }
        | StandardObjectStoreError::Precondition { source, .. }
        | StandardObjectStoreError::NotModified { source, .. }
        | StandardObjectStoreError::PermissionDenied { source, .. }
        | StandardObjectStoreError::Unauthenticated { source, .. } => source
            .downcast_ref::<StorageError>()
            .cloned()
            .unwrap_or_else(|| StorageError::io(source.to_string())),
        StandardObjectStoreError::NotImplemented { .. }
        | StandardObjectStoreError::InvalidPath { .. }
        | StandardObjectStoreError::UnknownConfigurationKey { .. } => {
            StorageError::unsupported(error.to_string())
        }
        _ => StorageError::io(error.to_string()),
    }
}

fn map_storage_error_to_standard(
    implementer: &'static str,
    target: &str,
    error: StorageError,
) -> StandardObjectStoreError {
    match error.kind() {
        crate::error::StorageErrorKind::NotFound => StandardObjectStoreError::NotFound {
            path: target.to_string(),
            source: Box::new(error),
        },
        crate::error::StorageErrorKind::Unsupported => StandardObjectStoreError::NotSupported {
            source: Box::new(error),
        },
        _ => StandardObjectStoreError::Generic {
            store: implementer,
            source: Box::new(error),
        },
    }
}

#[derive(Clone, Debug, Default)]
pub struct TokioFileSystem;

impl TokioFileSystem {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FileSystem for TokioFileSystem {
    async fn open(&self, path: &str, opts: OpenOptions) -> Result<FileHandle, StorageError> {
        let path_string = path.to_string();
        run_blocking("open file", path_string.clone(), move || {
            let path_buf = PathBuf::from(&path_string);
            if (opts.create || opts.write || opts.append || opts.truncate)
                && let Some(parent) = path_buf.parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).map_err(|error| {
                    map_io_error(
                        "create parent directory",
                        parent.to_string_lossy().as_ref(),
                        error,
                    )
                })?;
            }

            let mut open_options = fs::OpenOptions::new();
            open_options
                .create(opts.create)
                .read(opts.read)
                .write(opts.write || opts.append || opts.truncate)
                .append(opts.append)
                .truncate(opts.truncate);

            open_options
                .open(&path_buf)
                .map_err(|error| map_io_error("open file", &path_string, error))?;
            Ok(FileHandle::new(path_string))
        })
        .await
    }

    async fn write_at(
        &self,
        handle: &FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        let path = handle.path().to_string();
        let data = data.to_vec();
        run_blocking("write file", path.clone(), move || {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|error| map_io_error("open file for write", &path, error))?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|error| map_io_error("seek file", &path, error))?;
            file.write_all(&data)
                .map_err(|error| map_io_error("write file", &path, error))?;
            Ok(())
        })
        .await
    }

    async fn read_at(
        &self,
        handle: &FileHandle,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let path = handle.path().to_string();
        run_blocking("read file", path.clone(), move || {
            let mut file = fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .map_err(|error| map_io_error("open file for read", &path, error))?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|error| map_io_error("seek file", &path, error))?;
            let mut buf = vec![0_u8; len];
            let bytes_read = file
                .read(&mut buf)
                .map_err(|error| map_io_error("read file", &path, error))?;
            buf.truncate(bytes_read);
            Ok(buf)
        })
        .await
    }

    async fn sync(&self, handle: &FileHandle) -> Result<(), StorageError> {
        let path = handle.path().to_string();
        run_blocking("sync file", path.clone(), move || {
            let file = fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .map_err(|error| map_io_error("open file for sync", &path, error))?;
            file.sync_all()
                .map_err(|error| map_io_error("sync file", &path, error))?;
            Ok(())
        })
        .await
    }

    async fn sync_dir(&self, path: &str) -> Result<(), StorageError> {
        let path = path.to_string();
        run_blocking("sync directory", path.clone(), move || {
            fs::create_dir_all(&path)
                .map_err(|error| map_io_error("create directory", &path, error))?;
            let dir = fs::File::open(&path)
                .map_err(|error| map_io_error("open directory", &path, error))?;
            dir.sync_all()
                .map_err(|error| map_io_error("sync directory", &path, error))?;
            Ok(())
        })
        .await
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let from = from.to_string();
        let to = to.to_string();
        run_blocking("rename file", format!("{from} -> {to}"), move || {
            let to_path = PathBuf::from(&to);
            if let Some(parent) = to_path.parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).map_err(|error| {
                    map_io_error(
                        "create parent directory",
                        parent.to_string_lossy().as_ref(),
                        error,
                    )
                })?;
            }

            fs::rename(&from, &to).map_err(|error| map_io_error("rename file", &from, error))?;
            Ok(())
        })
        .await
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let path = path.to_string();
        run_blocking("delete file", path.clone(), move || {
            match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(map_io_error("delete file", &path, error)),
            }
        })
        .await
    }

    async fn list(&self, dir: &str) -> Result<Vec<String>, StorageError> {
        let dir = dir.to_string();
        run_blocking("list directory", dir.clone(), move || {
            let mut files = Vec::new();
            collect_files(Path::new(&dir), &mut files)?;
            files.sort();
            Ok(files)
        })
        .await
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDirObjectStore {
    root: PathBuf,
}

impl LocalDirObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn standard_store(&self) -> Result<LocalFileSystem, StandardObjectStoreError> {
        fs::create_dir_all(&self.root).map_err(|error| StandardObjectStoreError::Generic {
            store: "LocalDirObjectStore",
            source: Box::new(error),
        })?;
        LocalFileSystem::new_with_prefix(&self.root).map_err(|error| {
            StandardObjectStoreError::Generic {
                store: "LocalDirObjectStore",
                source: Box::new(error),
            }
        })
    }
}

impl fmt::Display for LocalDirObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalDirObjectStore({})", self.root.display())
    }
}

#[async_trait]
impl StandardObjectStore for LocalDirObjectStore {
    async fn put_opts(
        &self,
        location: &StandardObjectPath,
        payload: PutPayload,
        opts: StandardPutOptions,
    ) -> object_store::Result<StandardPutResult> {
        self.standard_store()?
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &StandardObjectPath,
        opts: StandardPutMultipartOptions,
    ) -> object_store::Result<Box<dyn StandardMultipartUpload>> {
        self.standard_store()?
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(
        &self,
        location: &StandardObjectPath,
        options: StandardGetOptions,
    ) -> object_store::Result<StandardGetResult> {
        self.standard_store()?.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StandardObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<StandardObjectPath>> {
        match self.standard_store() {
            Ok(store) => store.delete_stream(locations),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    fn list(
        &self,
        prefix: Option<&StandardObjectPath>,
    ) -> BoxStream<'static, object_store::Result<StandardObjectMeta>> {
        match self.standard_store() {
            Ok(store) => store.list(prefix),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StandardObjectPath>,
    ) -> object_store::Result<StandardListResult> {
        self.standard_store()?.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StandardObjectPath,
        to: &StandardObjectPath,
        options: StandardCopyOptions,
    ) -> object_store::Result<()> {
        self.standard_store()?.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl ObjectStore for LocalDirObjectStore {
    async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let key = standard_object_key(key)?;
        StandardObjectStoreExt::put(self, &key, Bytes::copy_from_slice(data).into())
            .await
            .map(|_| ())
            .map_err(map_standard_object_store_error)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let key = standard_object_key(key)?;
        StandardObjectStoreExt::get(self, &key)
            .await
            .map_err(map_standard_object_store_error)?
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(map_standard_object_store_error)
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        if start >= end {
            return Ok(Vec::new());
        }
        let key = standard_object_key(key)?;
        StandardObjectStoreExt::get_range(self, &key, start..end)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(map_standard_object_store_error)
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let key = standard_object_key(key)?;
        StandardObjectStoreExt::delete(self, &key)
            .await
            .map_err(map_standard_object_store_error)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let prefix = standard_object_prefix(prefix)?;
        let mut keys = StandardObjectStore::list(self, prefix.as_ref())
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .map_err(map_standard_object_store_error)?;
        keys.sort();
        Ok(keys)
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let from = standard_object_key(from)?;
        let to = standard_object_key(to)?;
        StandardObjectStoreExt::copy(self, &from, &to)
            .await
            .map_err(map_standard_object_store_error)
    }
}

#[derive(Clone, Debug, Default)]
pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Timestamp::new(millis)
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[derive(Debug)]
pub struct DeterministicRng {
    state: Mutex<ChaCha8Rng>,
}

impl Default for DeterministicRng {
    fn default() -> Self {
        Self::seeded(0x4d59_5952_4e47_0001)
    }
}

impl DeterministicRng {
    pub fn seeded(seed: u64) -> Self {
        Self {
            state: Mutex::new(ChaCha8Rng::seed_from_u64(seed)),
        }
    }

    fn next_state(&self) -> u64 {
        lock(&self.state).next_u64()
    }
}

impl Rng for DeterministicRng {
    fn next_u64(&self) -> u64 {
        self.next_state()
    }

    fn uuid(&self) -> String {
        uuid_from_words(self.next_state(), self.next_state())
    }
}

#[derive(Debug, Default)]
pub struct SystemRng {
    fallback_counter: AtomicU64,
}

impl SystemRng {
    fn fallback_random(&self) -> u64 {
        let counter = self.fallback_counter.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        now.rotate_left(17) ^ counter.wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }
}

impl Rng for SystemRng {
    fn next_u64(&self) -> u64 {
        getrandom::u64().unwrap_or_else(|_| self.fallback_random())
    }

    fn uuid(&self) -> String {
        uuid_from_words(self.next_u64(), self.next_u64())
    }
}

#[derive(Debug)]
pub struct SimulatedClock {
    now: Mutex<Timestamp>,
    watch: watch::Sender<Timestamp>,
}

impl Default for SimulatedClock {
    fn default() -> Self {
        Self::new(Timestamp::new(0))
    }
}

impl SimulatedClock {
    pub fn new(now: Timestamp) -> Self {
        let (watch, _receiver) = watch::channel(now);
        Self {
            now: Mutex::new(now),
            watch,
        }
    }

    pub fn set(&self, now: Timestamp) {
        *lock(&self.now) = now;
        self.watch.send_replace(now);
    }

    pub fn advance(&self, duration: Duration) {
        let millis = duration.as_millis() as u64;
        let mut now = lock(&self.now);
        *now = Timestamp::new(now.get().saturating_add(millis));
        self.watch.send_replace(*now);
    }
}

#[async_trait]
impl Clock for SimulatedClock {
    fn now(&self) -> Timestamp {
        *lock(&self.now)
    }

    async fn sleep(&self, duration: Duration) {
        let deadline = Timestamp::new(self.now().get().saturating_add(duration.as_millis() as u64));
        let mut receiver = self.watch.subscribe();
        loop {
            if *receiver.borrow() >= deadline {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileSystemOperation {
    Open,
    WriteAt,
    ReadAt,
    Sync,
    SyncDir,
    Rename,
    Delete,
    List,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSystemFailure {
    operation: FileSystemOperation,
    target_prefix: Option<String>,
    error: StorageError,
    persistent: bool,
}

impl FileSystemFailure {
    pub fn new(
        operation: FileSystemOperation,
        target_prefix: Option<String>,
        error: StorageError,
    ) -> Self {
        Self {
            operation,
            target_prefix,
            error,
            persistent: false,
        }
    }

    pub fn for_target(
        operation: FileSystemOperation,
        target_prefix: impl Into<String>,
        error: StorageError,
    ) -> Self {
        Self::new(operation, Some(target_prefix.into()), error)
    }

    pub fn global(operation: FileSystemOperation, error: StorageError) -> Self {
        Self::new(operation, None, error)
    }

    pub fn persistent(mut self) -> Self {
        self.persistent = true;
        self
    }

    pub fn disk_full(target_prefix: impl Into<String>) -> Self {
        Self::for_target(
            FileSystemOperation::WriteAt,
            target_prefix,
            StorageError::io("simulated disk full"),
        )
    }

    pub fn timeout(operation: FileSystemOperation, target_prefix: impl Into<String>) -> Self {
        Self::for_target(
            operation,
            target_prefix,
            StorageError::timeout("simulated timeout"),
        )
    }

    pub fn partial_read(target_prefix: impl Into<String>) -> Self {
        Self::for_target(
            FileSystemOperation::ReadAt,
            target_prefix,
            StorageError::corruption("simulated partial read"),
        )
    }

    fn matches(&self, operation: FileSystemOperation, target: &str) -> bool {
        self.operation == operation
            && self
                .target_prefix
                .as_ref()
                .is_none_or(|prefix| target.starts_with(prefix))
    }
}

#[derive(Debug, Default)]
pub struct SimulatedFileSystem {
    state: Mutex<SimulatedFileSystemState>,
}

#[derive(Debug, Default)]
struct SimulatedFileSystemState {
    live: BTreeMap<String, Vec<u8>>,
    durable: BTreeMap<String, Vec<u8>>,
    failures: VecDeque<FileSystemFailure>,
    operation_counts: HashMap<FileSystemOperation, u64>,
}

impl SimulatedFileSystem {
    pub fn inject_failure(&self, failure: FileSystemFailure) {
        lock(&self.state).failures.push_back(failure);
    }

    pub fn checkpoint(&self) {
        let mut state = lock(&self.state);
        state.durable = state.live.clone();
    }

    pub fn crash(&self) {
        let mut state = lock(&self.state);
        state.live = state.durable.clone();
    }

    fn maybe_fail(
        state: &mut SimulatedFileSystemState,
        operation: FileSystemOperation,
        target: &str,
    ) -> Result<(), StorageError> {
        if let Some(index) = state
            .failures
            .iter()
            .position(|failure| failure.matches(operation, target))
        {
            let failure = if state.failures[index].persistent {
                state.failures[index].clone()
            } else {
                state.failures.remove(index).expect("failure exists")
            };
            return Err(failure.error);
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn operation_count(&self, operation: FileSystemOperation) -> u64 {
        lock(&self.state)
            .operation_counts
            .get(&operation)
            .copied()
            .unwrap_or_default()
    }
}

#[async_trait]
impl FileSystem for SimulatedFileSystem {
    async fn open(&self, path: &str, opts: OpenOptions) -> Result<FileHandle, StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, FileSystemOperation::Open, path)?;
        *state
            .operation_counts
            .entry(FileSystemOperation::Open)
            .or_default() += 1;

        if opts.create {
            state.live.entry(path.to_string()).or_default();
        } else if !state.live.contains_key(path) {
            return Err(StorageError::not_found(format!("missing file: {path}")));
        }

        if opts.truncate {
            state.live.insert(path.to_string(), Vec::new());
        }

        Ok(FileHandle::new(path))
    }

    async fn write_at(
        &self,
        handle: &FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, FileSystemOperation::WriteAt, handle.path())?;
        *state
            .operation_counts
            .entry(FileSystemOperation::WriteAt)
            .or_default() += 1;

        let buf = state
            .live
            .get_mut(handle.path())
            .ok_or_else(|| StorageError::not_found(format!("missing file: {}", handle.path())))?;

        let offset = offset as usize;
        if buf.len() < offset {
            buf.resize(offset, 0);
        }
        if buf.len() < offset + data.len() {
            buf.resize(offset + data.len(), 0);
        }
        buf[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    async fn read_at(
        &self,
        handle: &FileHandle,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, FileSystemOperation::ReadAt, handle.path())?;
        *state
            .operation_counts
            .entry(FileSystemOperation::ReadAt)
            .or_default() += 1;

        let buf = state
            .live
            .get(handle.path())
            .ok_or_else(|| StorageError::not_found(format!("missing file: {}", handle.path())))?;

        let offset = offset as usize;
        if offset >= buf.len() {
            return Ok(Vec::new());
        }
        let end = (offset + len).min(buf.len());
        Ok(buf[offset..end].to_vec())
    }

    async fn sync(&self, handle: &FileHandle) -> Result<(), StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, FileSystemOperation::Sync, handle.path())?;
        *state
            .operation_counts
            .entry(FileSystemOperation::Sync)
            .or_default() += 1;

        let bytes =
            state.live.get(handle.path()).cloned().ok_or_else(|| {
                StorageError::not_found(format!("missing file: {}", handle.path()))
            })?;
        state.durable.insert(handle.path().to_string(), bytes);
        Ok(())
    }

    async fn sync_dir(&self, path: &str) -> Result<(), StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, FileSystemOperation::SyncDir, path)?;

        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };

        state
            .durable
            .retain(|candidate, _| !(candidate == path || candidate.starts_with(&prefix)));

        let live_entries = state
            .live
            .iter()
            .filter(|(candidate, _)| *candidate == path || candidate.starts_with(&prefix))
            .map(|(candidate, bytes)| (candidate.clone(), bytes.clone()))
            .collect::<Vec<_>>();

        for (candidate, bytes) in live_entries {
            state.durable.insert(candidate, bytes);
        }

        Ok(())
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, FileSystemOperation::Rename, from)?;
        *state
            .operation_counts
            .entry(FileSystemOperation::Rename)
            .or_default() += 1;

        let bytes = state
            .live
            .remove(from)
            .ok_or_else(|| StorageError::not_found(format!("missing file: {from}")))?;
        state.live.insert(to.to_string(), bytes);
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, FileSystemOperation::Delete, path)?;
        *state
            .operation_counts
            .entry(FileSystemOperation::Delete)
            .or_default() += 1;
        state.live.remove(path);
        Ok(())
    }

    async fn list(&self, dir: &str) -> Result<Vec<String>, StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, FileSystemOperation::List, dir)?;
        *state
            .operation_counts
            .entry(FileSystemOperation::List)
            .or_default() += 1;
        let mut files = state
            .live
            .keys()
            .filter(|path| path.starts_with(dir))
            .cloned()
            .collect::<Vec<_>>();
        files.sort();
        Ok(files)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectStoreOperation {
    Put,
    Get,
    GetRange,
    Delete,
    List,
    Copy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreFailure {
    operation: ObjectStoreOperation,
    target_prefix: Option<String>,
    error: StorageError,
    persistent: bool,
}

impl ObjectStoreFailure {
    pub fn new(
        operation: ObjectStoreOperation,
        target_prefix: Option<String>,
        error: StorageError,
    ) -> Self {
        Self {
            operation,
            target_prefix,
            error,
            persistent: false,
        }
    }

    pub fn for_target(
        operation: ObjectStoreOperation,
        target_prefix: impl Into<String>,
        error: StorageError,
    ) -> Self {
        Self::new(operation, Some(target_prefix.into()), error)
    }

    pub fn global(operation: ObjectStoreOperation, error: StorageError) -> Self {
        Self::new(operation, None, error)
    }

    pub fn persistent(mut self) -> Self {
        self.persistent = true;
        self
    }

    pub fn timeout(operation: ObjectStoreOperation, target_prefix: impl Into<String>) -> Self {
        Self::for_target(
            operation,
            target_prefix,
            StorageError::timeout("simulated timeout"),
        )
    }

    pub fn stale_list(target_prefix: impl Into<String>) -> Self {
        Self::for_target(
            ObjectStoreOperation::List,
            target_prefix,
            StorageError::durability_boundary("simulated stale list"),
        )
    }

    pub fn partial_read(operation: ObjectStoreOperation, target_prefix: impl Into<String>) -> Self {
        Self::for_target(
            operation,
            target_prefix,
            StorageError::corruption("simulated partial read"),
        )
    }

    fn matches(&self, operation: ObjectStoreOperation, target: &str) -> bool {
        self.operation == operation
            && self
                .target_prefix
                .as_ref()
                .is_none_or(|prefix| target.starts_with(prefix))
    }
}

#[derive(Debug, Clone, Default)]
pub struct SimulatedObjectStore {
    inner: InMemory,
    failures: Arc<Mutex<VecDeque<ObjectStoreFailure>>>,
}

impl SimulatedObjectStore {
    pub fn inject_failure(&self, failure: ObjectStoreFailure) {
        lock(&self.failures).push_back(failure);
    }

    fn maybe_fail(
        &self,
        operation: ObjectStoreOperation,
        target: &str,
    ) -> Result<(), StorageError> {
        let mut failures = lock(&self.failures);
        if let Some(index) = failures
            .iter()
            .position(|failure| failure.matches(operation, target))
        {
            let failure = if failures[index].persistent {
                failures[index].clone()
            } else {
                failures.remove(index).expect("failure exists")
            };
            return Err(failure.error);
        }

        Ok(())
    }
}

impl fmt::Display for SimulatedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SimulatedObjectStore")
    }
}

#[async_trait]
impl StandardObjectStore for SimulatedObjectStore {
    async fn put_opts(
        &self,
        location: &StandardObjectPath,
        payload: PutPayload,
        opts: StandardPutOptions,
    ) -> object_store::Result<StandardPutResult> {
        let target = location.to_string();
        self.maybe_fail(ObjectStoreOperation::Put, &target)
            .map_err(|error| {
                map_storage_error_to_standard("SimulatedObjectStore", &target, error)
            })?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StandardObjectPath,
        opts: StandardPutMultipartOptions,
    ) -> object_store::Result<Box<dyn StandardMultipartUpload>> {
        let target = location.to_string();
        self.maybe_fail(ObjectStoreOperation::Put, &target)
            .map_err(|error| {
                map_storage_error_to_standard("SimulatedObjectStore", &target, error)
            })?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StandardObjectPath,
        options: StandardGetOptions,
    ) -> object_store::Result<StandardGetResult> {
        let target = location.to_string();
        let operation = if options.range.is_some() {
            ObjectStoreOperation::GetRange
        } else {
            ObjectStoreOperation::Get
        };
        self.maybe_fail(operation, &target).map_err(|error| {
            map_storage_error_to_standard("SimulatedObjectStore", &target, error)
        })?;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StandardObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<StandardObjectPath>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let target = location.to_string();
                    store
                        .maybe_fail(ObjectStoreOperation::Delete, &target)
                        .map_err(|error| {
                            map_storage_error_to_standard("SimulatedObjectStore", &target, error)
                        })?;
                    StandardObjectStoreExt::delete(&store.inner, &location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&StandardObjectPath>,
    ) -> BoxStream<'static, object_store::Result<StandardObjectMeta>> {
        let target = prefix.map(ToString::to_string).unwrap_or_default();
        if let Err(error) = self.maybe_fail(ObjectStoreOperation::List, &target) {
            let error = map_storage_error_to_standard("SimulatedObjectStore", &target, error);
            return stream::once(async move { Err(error) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StandardObjectPath>,
    ) -> object_store::Result<StandardListResult> {
        let target = prefix.map(ToString::to_string).unwrap_or_default();
        self.maybe_fail(ObjectStoreOperation::List, &target)
            .map_err(|error| {
                map_storage_error_to_standard("SimulatedObjectStore", &target, error)
            })?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StandardObjectPath,
        to: &StandardObjectPath,
        options: StandardCopyOptions,
    ) -> object_store::Result<()> {
        let target = from.to_string();
        self.maybe_fail(ObjectStoreOperation::Copy, &target)
            .map_err(|error| {
                map_storage_error_to_standard("SimulatedObjectStore", &target, error)
            })?;
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl ObjectStore for SimulatedObjectStore {
    async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let key = standard_object_key(key)?;
        StandardObjectStoreExt::put(self, &key, Bytes::copy_from_slice(data).into())
            .await
            .map(|_| ())
            .map_err(map_standard_object_store_error)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let key = standard_object_key(key)?;
        StandardObjectStoreExt::get(self, &key)
            .await
            .map_err(map_standard_object_store_error)?
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(map_standard_object_store_error)
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        if start >= end {
            return Ok(Vec::new());
        }
        let key = standard_object_key(key)?;
        StandardObjectStoreExt::get_range(self, &key, start..end)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(map_standard_object_store_error)
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let key = standard_object_key(key)?;
        StandardObjectStoreExt::delete(self, &key)
            .await
            .map_err(map_standard_object_store_error)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.maybe_fail(ObjectStoreOperation::List, prefix)?;
        let prefix = standard_object_prefix(prefix)?;
        let mut keys = self
            .inner
            .list(prefix.as_ref())
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .map_err(map_standard_object_store_error)?;
        keys.sort();
        Ok(keys)
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let from = standard_object_key(from)?;
        let to = standard_object_key(to)?;
        StandardObjectStoreExt::copy(self, &from, &to)
            .await
            .map_err(map_standard_object_store_error)
    }
}
