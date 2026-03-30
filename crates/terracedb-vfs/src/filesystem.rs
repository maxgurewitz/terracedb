use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::Timestamp;

use crate::{InodeId, VfsError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stats {
    pub inode: InodeId,
    pub kind: FileKind,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub created_at: Timestamp,
    pub modified_at: Timestamp,
    pub changed_at: Timestamp,
    pub accessed_at: Timestamp,
    pub rdev: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub inode: InodeId,
    pub kind: FileKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntryPlus {
    pub entry: DirEntry,
    pub stats: Stats,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateOptions {
    pub create_parents: bool,
    pub overwrite: bool,
    pub mode: u32,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            create_parents: false,
            overwrite: true,
            mode: 0o644,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MkdirOptions {
    pub recursive: bool,
    pub mode: u32,
}

impl Default for MkdirOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            mode: 0o755,
        }
    }
}

#[async_trait]
pub trait ReadOnlyVfsFileSystem: Send + Sync {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, VfsError>;
    async fn lstat(&self, path: &str) -> Result<Option<Stats>, VfsError>;
    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, VfsError>;
    async fn pread(&self, path: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, VfsError>;
    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, VfsError>;
    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, VfsError>;
    async fn readlink(&self, path: &str) -> Result<String, VfsError>;
}

#[async_trait]
pub trait VfsFileSystem: ReadOnlyVfsFileSystem {
    async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        opts: CreateOptions,
    ) -> Result<(), VfsError>;
    async fn pwrite(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), VfsError>;
    async fn truncate(&self, path: &str, size: u64) -> Result<(), VfsError>;
    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<(), VfsError>;
    async fn rename(&self, from: &str, to: &str) -> Result<(), VfsError>;
    async fn link(&self, from: &str, to: &str) -> Result<(), VfsError>;
    async fn symlink(&self, target: &str, linkpath: &str) -> Result<(), VfsError>;
    async fn unlink(&self, path: &str) -> Result<(), VfsError>;
    async fn rmdir(&self, path: &str) -> Result<(), VfsError>;
    async fn fsync(&self, path: Option<&str>) -> Result<(), VfsError>;
}
