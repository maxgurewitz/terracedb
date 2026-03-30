use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::Timestamp;

use crate::{AgentFsError, InodeId};

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
pub trait ReadOnlyAgentFileSystem: Send + Sync {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, AgentFsError>;
    async fn lstat(&self, path: &str) -> Result<Option<Stats>, AgentFsError>;
    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, AgentFsError>;
    async fn pread(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, AgentFsError>;
    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, AgentFsError>;
    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, AgentFsError>;
    async fn readlink(&self, path: &str) -> Result<String, AgentFsError>;
}

#[async_trait]
pub trait AgentFileSystem: ReadOnlyAgentFileSystem {
    async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        opts: CreateOptions,
    ) -> Result<(), AgentFsError>;
    async fn pwrite(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), AgentFsError>;
    async fn truncate(&self, path: &str, size: u64) -> Result<(), AgentFsError>;
    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<(), AgentFsError>;
    async fn rename(&self, from: &str, to: &str) -> Result<(), AgentFsError>;
    async fn link(&self, from: &str, to: &str) -> Result<(), AgentFsError>;
    async fn symlink(&self, target: &str, linkpath: &str) -> Result<(), AgentFsError>;
    async fn unlink(&self, path: &str) -> Result<(), AgentFsError>;
    async fn rmdir(&self, path: &str) -> Result<(), AgentFsError>;
    async fn fsync(&self, path: Option<&str>) -> Result<(), AgentFsError>;
}
