use std::{collections::BTreeMap, ops::Range, pin::Pin, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use parking_lot::{Mutex, MutexGuard};

use terracedb::{ObjectStore, StorageErrorKind, Timestamp};

use crate::{BlobContractError, BlobStoreError};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
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

pub type BlobStoreByteStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, BlobStoreError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobByteRange {
    start: u64,
    end: u64,
}

impl BlobByteRange {
    pub fn new(start: u64, end: u64) -> Result<Self, BlobStoreError> {
        if end < start {
            return Err(BlobStoreError::InvalidRange {
                key: "<pending>".to_string(),
                start,
                end,
                size_bytes: 0,
            });
        }
        Ok(Self { start, end })
    }

    pub fn start(self) -> u64 {
        self.start
    }

    pub fn end(self) -> u64 {
        self.end
    }

    pub fn is_empty(self) -> bool {
        self.start == self.end
    }

    pub fn len(self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    fn to_range(self, key: &str, size_bytes: u64) -> Result<Range<usize>, BlobStoreError> {
        if self.end > size_bytes || self.start > size_bytes {
            return Err(BlobStoreError::InvalidRange {
                key: key.to_string(),
                start: self.start,
                end: self.end,
                size_bytes,
            });
        }

        Ok(self.start as usize..self.end as usize)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlobPutOptions {
    pub content_type: Option<String>,
    pub expected_size: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobGetOptions {
    pub range: Option<BlobByteRange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobObjectInfo {
    pub key: String,
    pub size_bytes: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobObjectLayout {
    root_prefix: String,
    namespace: String,
}

impl BlobObjectLayout {
    pub fn new(
        root_prefix: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, BlobContractError> {
        let namespace = namespace.into();
        if namespace.is_empty() {
            return Err(BlobContractError::EmptyNamespace);
        }
        if namespace.as_bytes().contains(&0) {
            return Err(BlobContractError::NulByteInKeyPart { field: "namespace" });
        }

        Ok(Self {
            root_prefix: root_prefix.into().trim_matches('/').to_string(),
            namespace: namespace.trim_matches('/').to_string(),
        })
    }

    pub fn root_prefix(&self) -> &str {
        &self.root_prefix
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn object_prefix(&self) -> String {
        join_object_key(&self.root_prefix, &format!("blobs/{}", self.namespace))
    }

    pub fn object_key(&self, relative: &str) -> String {
        join_object_key(&self.object_prefix(), relative)
    }
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(
        &self,
        key: &str,
        data: BlobStoreByteStream,
        opts: BlobPutOptions,
    ) -> Result<BlobObjectInfo, BlobStoreError>;

    async fn get(
        &self,
        key: &str,
        opts: BlobGetOptions,
    ) -> Result<BlobStoreByteStream, BlobStoreError>;

    async fn stat(&self, key: &str) -> Result<Option<BlobObjectInfo>, BlobStoreError>;

    async fn delete(&self, key: &str) -> Result<(), BlobStoreError>;
}

#[derive(Clone, Default)]
pub struct InMemoryBlobStore {
    inner: Arc<Mutex<BTreeMap<String, StoredBlobObject>>>,
}

#[derive(Clone)]
struct StoredBlobObject {
    bytes: Vec<u8>,
    info: BlobObjectInfo,
}

impl std::fmt::Debug for InMemoryBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryBlobStore")
            .field("object_count", &lock(&self.inner).len())
            .finish()
    }
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
    async fn put(
        &self,
        key: &str,
        data: BlobStoreByteStream,
        opts: BlobPutOptions,
    ) -> Result<BlobObjectInfo, BlobStoreError> {
        let bytes = collect_stream_bytes(data).await?;
        if let Some(expected_size) = opts.expected_size
            && expected_size != bytes.len() as u64
        {
            return Err(BlobStoreError::Storage(
                terracedb::StorageError::corruption(format!(
                    "blob-store put expected {expected_size} bytes for {key}, got {}",
                    bytes.len()
                )),
            ));
        }

        let info = BlobObjectInfo {
            key: key.to_string(),
            size_bytes: bytes.len() as u64,
            content_type: opts.content_type,
            etag: Some(format!("mem-{:016x}", bytes.len())),
            last_modified: None,
        };
        lock(&self.inner).insert(
            key.to_string(),
            StoredBlobObject {
                bytes,
                info: info.clone(),
            },
        );
        Ok(info)
    }

    async fn get(
        &self,
        key: &str,
        opts: BlobGetOptions,
    ) -> Result<BlobStoreByteStream, BlobStoreError> {
        let stored =
            lock(&self.inner)
                .get(key)
                .cloned()
                .ok_or_else(|| BlobStoreError::NotFound {
                    key: key.to_string(),
                })?;

        let bytes = if let Some(range) = opts.range {
            let slice = range.to_range(key, stored.info.size_bytes)?;
            stored.bytes[slice].to_vec()
        } else {
            stored.bytes
        };
        Ok(stream_from_bytes(bytes))
    }

    async fn stat(&self, key: &str) -> Result<Option<BlobObjectInfo>, BlobStoreError> {
        Ok(lock(&self.inner).get(key).map(|stored| stored.info.clone()))
    }

    async fn delete(&self, key: &str) -> Result<(), BlobStoreError> {
        lock(&self.inner).remove(key);
        Ok(())
    }
}

#[derive(Clone)]
pub struct BlobStoreObjectStoreAdapter {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for BlobStoreObjectStoreAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobStoreObjectStoreAdapter")
            .field("inner", &"<dyn ObjectStore>")
            .finish()
    }
}

impl BlobStoreObjectStoreAdapter {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl BlobStore for BlobStoreObjectStoreAdapter {
    async fn put(
        &self,
        key: &str,
        data: BlobStoreByteStream,
        opts: BlobPutOptions,
    ) -> Result<BlobObjectInfo, BlobStoreError> {
        let bytes = collect_stream_bytes(data).await?;
        if let Some(expected_size) = opts.expected_size
            && expected_size != bytes.len() as u64
        {
            return Err(BlobStoreError::Storage(
                terracedb::StorageError::corruption(format!(
                    "blob-store adapter expected {expected_size} bytes for {key}, got {}",
                    bytes.len()
                )),
            ));
        }

        self.inner.put(key, &bytes).await?;
        Ok(BlobObjectInfo {
            key: key.to_string(),
            size_bytes: bytes.len() as u64,
            content_type: opts.content_type,
            etag: None,
            last_modified: None,
        })
    }

    async fn get(
        &self,
        key: &str,
        opts: BlobGetOptions,
    ) -> Result<BlobStoreByteStream, BlobStoreError> {
        let bytes = match opts.range {
            Some(range) if range.is_empty() => Ok(Vec::new()),
            Some(range) => self.inner.get_range(key, range.start(), range.end()).await,
            None => self.inner.get(key).await,
        }
        .map_err(|error| {
            if error.kind() == StorageErrorKind::NotFound {
                BlobStoreError::NotFound {
                    key: key.to_string(),
                }
            } else {
                BlobStoreError::Storage(error)
            }
        })?;

        Ok(stream_from_bytes(bytes))
    }

    async fn stat(&self, key: &str) -> Result<Option<BlobObjectInfo>, BlobStoreError> {
        match self.inner.get(key).await {
            Ok(bytes) => Ok(Some(BlobObjectInfo {
                key: key.to_string(),
                size_bytes: bytes.len() as u64,
                content_type: None,
                etag: None,
                last_modified: None,
            })),
            Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
            Err(error) => Err(BlobStoreError::Storage(error)),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), BlobStoreError> {
        self.inner.delete(key).await?;
        Ok(())
    }
}

pub(crate) fn stream_from_bytes(bytes: Vec<u8>) -> BlobStoreByteStream {
    Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) }))
}

async fn collect_stream_bytes(mut stream: BlobStoreByteStream) -> Result<Vec<u8>, BlobStoreError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}
