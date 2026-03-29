use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{error::StorageError, ids::Timestamp};

pub use object_store::{
    ObjectStore as StandardObjectStore, ObjectStoreExt as StandardObjectStoreExt,
    path::Path as StandardObjectPath,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    pub create: bool,
    pub read: bool,
    pub write: bool,
    pub truncate: bool,
    pub append: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FileHandle {
    path: String,
}

impl FileHandle {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[async_trait]
pub trait FileSystem: Send + Sync {
    async fn open(&self, path: &str, opts: OpenOptions) -> Result<FileHandle, StorageError>;
    async fn write_at(
        &self,
        handle: &FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StorageError>;
    async fn read_at(
        &self,
        handle: &FileHandle,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, StorageError>;
    async fn sync(&self, handle: &FileHandle) -> Result<(), StorageError>;
    async fn sync_dir(&self, path: &str) -> Result<(), StorageError>;
    async fn rename(&self, from: &str, to: &str) -> Result<(), StorageError>;
    async fn delete(&self, path: &str) -> Result<(), StorageError>;
    async fn list(&self, dir: &str) -> Result<Vec<String>, StorageError>;
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, data: &[u8]) -> Result<(), StorageError>;
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError>;
    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, StorageError>;
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError>;
}

#[async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
    async fn sleep(&self, duration: Duration);
}

pub trait Rng: Send + Sync {
    fn next_u64(&self) -> u64;
    fn uuid(&self) -> String;
}

#[derive(Clone)]
pub struct DbDependencies {
    pub file_system: Arc<dyn FileSystem>,
    pub object_store: Arc<dyn ObjectStore>,
    pub clock: Arc<dyn Clock>,
    pub rng: Arc<dyn Rng>,
}

impl DbDependencies {
    pub fn new(
        file_system: Arc<dyn FileSystem>,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        rng: Arc<dyn Rng>,
    ) -> Self {
        Self {
            file_system,
            object_store,
            clock,
            rng,
        }
    }
}

impl fmt::Debug for DbDependencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbDependencies")
            .field("file_system", &"<dyn FileSystem>")
            .field("object_store", &"<dyn ObjectStore>")
            .field("clock", &"<dyn Clock>")
            .field("rng", &"<dyn Rng>")
            .finish()
    }
}
