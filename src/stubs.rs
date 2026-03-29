use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use async_trait::async_trait;

use crate::{
    error::{StorageError, StorageErrorKind},
    ids::Timestamp,
    io::{Clock, FileHandle, FileSystem, ObjectStore, OpenOptions, Rng},
};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("stub mutex poisoned")
}

#[derive(Debug, Default)]
pub struct StubFileSystem {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
}

#[async_trait]
impl FileSystem for StubFileSystem {
    async fn open(&self, path: &str, opts: OpenOptions) -> Result<FileHandle, StorageError> {
        let mut files = lock(&self.files);
        if opts.create {
            files.entry(path.to_string()).or_default();
        } else if !files.contains_key(path) {
            return Err(StorageError::new(
                StorageErrorKind::NotFound,
                format!("missing file: {path}"),
            ));
        }

        if opts.truncate {
            files.insert(path.to_string(), Vec::new());
        }

        Ok(FileHandle::new(path))
    }

    async fn write_at(
        &self,
        handle: &FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        let mut files = lock(&self.files);
        let buf = files.entry(handle.path().to_string()).or_default();
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
        let files = lock(&self.files);
        let buf = files.get(handle.path()).ok_or_else(|| {
            StorageError::new(
                StorageErrorKind::NotFound,
                format!("missing file: {}", handle.path()),
            )
        })?;

        let offset = offset as usize;
        if offset >= buf.len() {
            return Ok(Vec::new());
        }
        let end = (offset + len).min(buf.len());
        Ok(buf[offset..end].to_vec())
    }

    async fn sync(&self, _handle: &FileHandle) -> Result<(), StorageError> {
        Ok(())
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let mut files = lock(&self.files);
        let data = files.remove(from).ok_or_else(|| {
            StorageError::new(StorageErrorKind::NotFound, format!("missing file: {from}"))
        })?;
        files.insert(to.to_string(), data);
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let mut files = lock(&self.files);
        files.remove(path);
        Ok(())
    }

    async fn list(&self, dir: &str) -> Result<Vec<String>, StorageError> {
        let files = lock(&self.files);
        Ok(files
            .keys()
            .filter(|path| path.starts_with(dir))
            .cloned()
            .collect())
    }
}

#[derive(Debug, Default)]
pub struct StubObjectStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

#[async_trait]
impl ObjectStore for StubObjectStore {
    async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        lock(&self.objects).insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        lock(&self.objects).get(key).cloned().ok_or_else(|| {
            StorageError::new(StorageErrorKind::NotFound, format!("missing key: {key}"))
        })
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        let object = self.get(key).await?;
        let start = start as usize;
        let end = (end as usize).min(object.len());
        if start >= end {
            return Ok(Vec::new());
        }
        Ok(object[start..end].to_vec())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        lock(&self.objects).remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        Ok(lock(&self.objects)
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let value = self.get(from).await?;
        self.put(to, &value).await
    }
}

#[derive(Debug)]
pub struct StubClock {
    now: Mutex<Timestamp>,
}

impl Default for StubClock {
    fn default() -> Self {
        Self {
            now: Mutex::new(Timestamp::new(0)),
        }
    }
}

impl StubClock {
    pub fn advance(&self, duration: Duration) {
        let mut now = lock(&self.now);
        *now = Timestamp::new(now.get() + duration.as_millis() as u64);
    }
}

#[async_trait]
impl Clock for StubClock {
    fn now(&self) -> Timestamp {
        *lock(&self.now)
    }

    async fn sleep(&self, duration: Duration) {
        self.advance(duration);
    }
}

#[derive(Debug)]
pub struct StubRng {
    state: Mutex<u64>,
}

impl Default for StubRng {
    fn default() -> Self {
        Self {
            state: Mutex::new(0x4D59_5952_4E47_0001),
        }
    }
}

impl StubRng {
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

impl Rng for StubRng {
    fn next_u64(&self) -> u64 {
        self.next_state()
    }

    fn uuid(&self) -> String {
        let a = self.next_state();
        let b = self.next_state();
        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            (a >> 32) as u32,
            ((a >> 16) & 0xFFFF) as u16,
            (a & 0xFFFF) as u16,
            (b >> 48) as u16,
            b & 0x0000_FFFF_FFFF_FFFF
        )
    }
}
