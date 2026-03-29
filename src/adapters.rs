use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{sync::watch, task};

use crate::{
    error::StorageError,
    ids::Timestamp,
    io::{Clock, FileHandle, FileSystem, ObjectStore, OpenOptions, Rng},
};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("adapter mutex poisoned")
}

fn format_uuid(bytes: [u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

fn uuid_from_words(a: u64, b: u64) -> String {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&a.to_be_bytes());
    bytes[8..].copy_from_slice(&b.to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format_uuid(bytes)
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
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(map_io_error(
                "list directory",
                dir.to_string_lossy().as_ref(),
                error,
            ));
        }
    };

    for entry in entries {
        let entry = entry.map_err(|error| {
            map_io_error("list directory", dir.to_string_lossy().as_ref(), error)
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files)?;
        } else if path.is_file() {
            files.push(path.to_string_lossy().into_owned());
        }
    }

    Ok(())
}

fn normalized_relative_key(root: &Path, path: &Path) -> Result<String, StorageError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        StorageError::unsupported(format!(
            "object path {} escapes root {}",
            path.display(),
            root.display()
        ))
    })?;

    let mut key = String::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => {
                if !key.is_empty() {
                    key.push('/');
                }
                key.push_str(part.to_string_lossy().as_ref());
            }
            Component::CurDir => {}
            _ => {
                return Err(StorageError::unsupported(format!(
                    "object path {} contains unsupported components",
                    path.display()
                )));
            }
        }
    }

    Ok(key)
}

fn resolve_object_key(root: &Path, key: &str) -> Result<PathBuf, StorageError> {
    let trimmed = key.trim_matches('/');
    if trimmed.is_empty() {
        return Err(StorageError::unsupported("object key cannot be empty"));
    }

    let mut path = root.to_path_buf();
    for component in Path::new(trimmed).components() {
        match component {
            Component::Normal(part) => path.push(part),
            Component::CurDir => {}
            _ => {
                return Err(StorageError::unsupported(format!(
                    "object key {key} contains unsupported path components"
                )));
            }
        }
    }

    Ok(path)
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
}

#[async_trait]
impl ObjectStore for LocalDirObjectStore {
    async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let root = self.root.clone();
        let key = key.to_string();
        let data = data.to_vec();
        run_blocking("put object", key.clone(), move || {
            let path = resolve_object_key(&root, &key)?;
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    map_io_error(
                        "create object parent directory",
                        parent.to_string_lossy().as_ref(),
                        error,
                    )
                })?;
            }
            fs::write(&path, &data)
                .map_err(|error| map_io_error("put object", path.to_string_lossy().as_ref(), error))
        })
        .await
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let root = self.root.clone();
        let key = key.to_string();
        run_blocking("get object", key.clone(), move || {
            let path = resolve_object_key(&root, &key)?;
            fs::read(&path)
                .map_err(|error| map_io_error("get object", path.to_string_lossy().as_ref(), error))
        })
        .await
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        let bytes = self.get(key).await?;
        let start = start as usize;
        let end = (end as usize).min(bytes.len());
        if start >= end {
            return Ok(Vec::new());
        }
        Ok(bytes[start..end].to_vec())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let root = self.root.clone();
        let key = key.to_string();
        run_blocking("delete object", key.clone(), move || {
            let path = resolve_object_key(&root, &key)?;
            match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(map_io_error(
                    "delete object",
                    path.to_string_lossy().as_ref(),
                    error,
                )),
            }
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let root = self.root.clone();
        let prefix = prefix.to_string();
        run_blocking("list objects", prefix.clone(), move || {
            let mut paths = Vec::new();
            collect_files(&root, &mut paths)?;

            let mut keys = Vec::new();
            for path in paths {
                let key = normalized_relative_key(&root, Path::new(&path))?;
                if key.starts_with(&prefix) {
                    keys.push(key);
                }
            }

            keys.sort();
            Ok(keys)
        })
        .await
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let root = self.root.clone();
        let from = from.to_string();
        let to = to.to_string();
        run_blocking("copy object", format!("{from} -> {to}"), move || {
            let from_path = resolve_object_key(&root, &from)?;
            let to_path = resolve_object_key(&root, &to)?;
            if let Some(parent) = to_path.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    map_io_error(
                        "create object parent directory",
                        parent.to_string_lossy().as_ref(),
                        error,
                    )
                })?;
            }

            fs::copy(&from_path, &to_path).map_err(|error| {
                map_io_error("copy object", from_path.to_string_lossy().as_ref(), error)
            })?;
            Ok(())
        })
        .await
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
    state: Mutex<u64>,
}

impl Default for DeterministicRng {
    fn default() -> Self {
        Self::seeded(0x4d59_5952_4e47_0001)
    }
}

impl DeterministicRng {
    pub fn seeded(seed: u64) -> Self {
        Self {
            state: Mutex::new(seed),
        }
    }

    fn next_state(&self) -> u64 {
        let mut state = lock(&self.state);
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state
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

#[derive(Debug, Default)]
pub struct SimulatedObjectStore {
    state: Mutex<SimulatedObjectStoreState>,
}

#[derive(Debug, Default)]
struct SimulatedObjectStoreState {
    objects: BTreeMap<String, Vec<u8>>,
    failures: VecDeque<ObjectStoreFailure>,
}

impl SimulatedObjectStore {
    pub fn inject_failure(&self, failure: ObjectStoreFailure) {
        lock(&self.state).failures.push_back(failure);
    }

    fn maybe_fail(
        state: &mut SimulatedObjectStoreState,
        operation: ObjectStoreOperation,
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
}

#[async_trait]
impl ObjectStore for SimulatedObjectStore {
    async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, ObjectStoreOperation::Put, key)?;
        state.objects.insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, ObjectStoreOperation::Get, key)?;
        state
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| StorageError::not_found(format!("missing key: {key}")))
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, ObjectStoreOperation::GetRange, key)?;
        let object = state
            .objects
            .get(key)
            .ok_or_else(|| StorageError::not_found(format!("missing key: {key}")))?;
        let start = start as usize;
        let end = (end as usize).min(object.len());
        if start >= end {
            return Ok(Vec::new());
        }
        Ok(object[start..end].to_vec())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, ObjectStoreOperation::Delete, key)?;
        state.objects.remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, ObjectStoreOperation::List, prefix)?;
        let mut keys = state
            .objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        Ok(keys)
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let mut state = lock(&self.state);
        Self::maybe_fail(&mut state, ObjectStoreOperation::Copy, from)?;
        let value = state
            .objects
            .get(from)
            .cloned()
            .ok_or_else(|| StorageError::not_found(format!("missing key: {from}")))?;
        state.objects.insert(to.to_string(), value);
        Ok(())
    }
}
