use std::sync::Arc;

use async_trait::async_trait;
use terracedb_vfs::{
    CreateOptions, DirEntry, DirEntryPlus, MkdirOptions, ReadOnlyVfsFileSystem, Stats,
    VfsFileSystem,
};

use crate::SandboxError;

#[async_trait]
pub trait SandboxFilesystemShim: Send + Sync {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, SandboxError>;
    async fn lstat(&self, path: &str) -> Result<Option<Stats>, SandboxError>;
    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, SandboxError>;
    async fn pread(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, SandboxError>;
    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, SandboxError>;
    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, SandboxError>;
    async fn readlink(&self, path: &str) -> Result<String, SandboxError>;
    async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        opts: CreateOptions,
    ) -> Result<(), SandboxError>;
    async fn pwrite(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), SandboxError>;
    async fn truncate(&self, path: &str, size: u64) -> Result<(), SandboxError>;
    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<(), SandboxError>;
    async fn rename(&self, from: &str, to: &str) -> Result<(), SandboxError>;
    async fn link(&self, from: &str, to: &str) -> Result<(), SandboxError>;
    async fn symlink(&self, target: &str, linkpath: &str) -> Result<(), SandboxError>;
    async fn unlink(&self, path: &str) -> Result<(), SandboxError>;
    async fn rmdir(&self, path: &str) -> Result<(), SandboxError>;
    async fn fsync(&self, path: Option<&str>) -> Result<(), SandboxError>;
}

#[derive(Clone)]
pub struct VfsSandboxFilesystemShim {
    inner: Arc<dyn VfsFileSystem>,
}

impl VfsSandboxFilesystemShim {
    pub fn new(inner: Arc<dyn VfsFileSystem>) -> Self {
        Self { inner }
    }

    pub fn readonly(inner: Arc<dyn ReadOnlyVfsFileSystem>) -> ReadonlySandboxFilesystemShim {
        ReadonlySandboxFilesystemShim { inner }
    }
}

#[derive(Clone)]
pub struct ReadonlySandboxFilesystemShim {
    inner: Arc<dyn ReadOnlyVfsFileSystem>,
}

#[async_trait]
impl SandboxFilesystemShim for VfsSandboxFilesystemShim {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, SandboxError> {
        self.inner.stat(path).await.map_err(Into::into)
    }

    async fn lstat(&self, path: &str) -> Result<Option<Stats>, SandboxError> {
        self.inner.lstat(path).await.map_err(Into::into)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, SandboxError> {
        self.inner.read_file(path).await.map_err(Into::into)
    }

    async fn pread(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, SandboxError> {
        self.inner
            .pread(path, offset, len)
            .await
            .map_err(Into::into)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, SandboxError> {
        self.inner.readdir(path).await.map_err(Into::into)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, SandboxError> {
        self.inner.readdir_plus(path).await.map_err(Into::into)
    }

    async fn readlink(&self, path: &str) -> Result<String, SandboxError> {
        self.inner.readlink(path).await.map_err(Into::into)
    }

    async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        opts: CreateOptions,
    ) -> Result<(), SandboxError> {
        self.inner
            .write_file(path, data, opts)
            .await
            .map_err(Into::into)
    }

    async fn pwrite(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), SandboxError> {
        self.inner
            .pwrite(path, offset, data)
            .await
            .map_err(Into::into)
    }

    async fn truncate(&self, path: &str, size: u64) -> Result<(), SandboxError> {
        self.inner.truncate(path, size).await.map_err(Into::into)
    }

    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<(), SandboxError> {
        self.inner.mkdir(path, opts).await.map_err(Into::into)
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), SandboxError> {
        self.inner.rename(from, to).await.map_err(Into::into)
    }

    async fn link(&self, from: &str, to: &str) -> Result<(), SandboxError> {
        self.inner.link(from, to).await.map_err(Into::into)
    }

    async fn symlink(&self, target: &str, linkpath: &str) -> Result<(), SandboxError> {
        self.inner
            .symlink(target, linkpath)
            .await
            .map_err(Into::into)
    }

    async fn unlink(&self, path: &str) -> Result<(), SandboxError> {
        self.inner.unlink(path).await.map_err(Into::into)
    }

    async fn rmdir(&self, path: &str) -> Result<(), SandboxError> {
        self.inner.rmdir(path).await.map_err(Into::into)
    }

    async fn fsync(&self, path: Option<&str>) -> Result<(), SandboxError> {
        self.inner.fsync(path).await.map_err(Into::into)
    }
}

#[async_trait]
impl SandboxFilesystemShim for ReadonlySandboxFilesystemShim {
    async fn stat(&self, path: &str) -> Result<Option<Stats>, SandboxError> {
        self.inner.stat(path).await.map_err(Into::into)
    }

    async fn lstat(&self, path: &str) -> Result<Option<Stats>, SandboxError> {
        self.inner.lstat(path).await.map_err(Into::into)
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, SandboxError> {
        self.inner.read_file(path).await.map_err(Into::into)
    }

    async fn pread(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, SandboxError> {
        self.inner
            .pread(path, offset, len)
            .await
            .map_err(Into::into)
    }

    async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, SandboxError> {
        self.inner.readdir(path).await.map_err(Into::into)
    }

    async fn readdir_plus(&self, path: &str) -> Result<Vec<DirEntryPlus>, SandboxError> {
        self.inner.readdir_plus(path).await.map_err(Into::into)
    }

    async fn readlink(&self, path: &str) -> Result<String, SandboxError> {
        self.inner.readlink(path).await.map_err(Into::into)
    }

    async fn write_file(
        &self,
        _path: &str,
        _data: Vec<u8>,
        _opts: CreateOptions,
    ) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support write_file".to_string(),
        })
    }

    async fn pwrite(&self, _path: &str, _offset: u64, _data: Vec<u8>) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support pwrite".to_string(),
        })
    }

    async fn truncate(&self, _path: &str, _size: u64) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support truncate".to_string(),
        })
    }

    async fn mkdir(&self, _path: &str, _opts: MkdirOptions) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support mkdir".to_string(),
        })
    }

    async fn rename(&self, _from: &str, _to: &str) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support rename".to_string(),
        })
    }

    async fn link(&self, _from: &str, _to: &str) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support link".to_string(),
        })
    }

    async fn symlink(&self, _target: &str, _linkpath: &str) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support symlink".to_string(),
        })
    }

    async fn unlink(&self, _path: &str) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support unlink".to_string(),
        })
    }

    async fn rmdir(&self, _path: &str) -> Result<(), SandboxError> {
        Err(SandboxError::Service {
            service: "filesystem",
            message: "read-only filesystem shim does not support rmdir".to_string(),
        })
    }

    async fn fsync(&self, _path: Option<&str>) -> Result<(), SandboxError> {
        Ok(())
    }
}
