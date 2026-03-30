use std::{
    collections::{BTreeMap, VecDeque},
    ops::Range,
    pin::Pin,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use object_store::{
    Error as StandardObjectStoreError, GetOptions as StandardGetOptions,
    MultipartUpload as StandardMultipartUpload, ObjectMeta as StandardObjectMeta,
    ObjectStore as StandardObjectStore, PutOptions as StandardPutOptions, PutPayloadMut,
    path::Path as StandardObjectPath,
};
use parking_lot::{Mutex, MutexGuard};
use sha2::{Digest as _, Sha256};

use terracedb::{ObjectStore, StandardObjectStoreExt, StorageError, StorageErrorKind, Timestamp};

use crate::{BlobContractError, BlobStoreError, BlobStoreOperation, BlobStoreRecoveryHint};

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

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn standard_object_key(key: &str) -> Result<StandardObjectPath, BlobStoreError> {
    let path = StandardObjectPath::parse(key).map_err(|error| {
        BlobStoreError::Contract(BlobContractError::InvalidKey {
            reason: format!("invalid object key {key}: {error}"),
        })
    })?;
    if path.as_ref().is_empty() {
        return Err(BlobStoreError::Contract(BlobContractError::InvalidKey {
            reason: "object key cannot be empty".to_string(),
        }));
    }
    Ok(path)
}

fn standard_object_prefix(prefix: &str) -> Result<Option<StandardObjectPath>, BlobStoreError> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }

    StandardObjectPath::parse(prefix)
        .map(Some)
        .map_err(|error| {
            BlobStoreError::Contract(BlobContractError::InvalidKey {
                reason: format!("invalid object prefix {prefix}: {error}"),
            })
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

fn map_storage_blob_error(key: &str, error: StorageError) -> BlobStoreError {
    if error.kind() == StorageErrorKind::NotFound {
        BlobStoreError::NotFound {
            key: key.to_string(),
        }
    } else {
        BlobStoreError::Storage(error)
    }
}

fn map_standard_blob_error(key: &str, error: StandardObjectStoreError) -> BlobStoreError {
    map_storage_blob_error(key, map_standard_object_store_error(error))
}

fn object_info_from_meta(meta: StandardObjectMeta) -> BlobObjectInfo {
    let last_modified = meta
        .last_modified
        .timestamp_millis()
        .try_into()
        .ok()
        .map(Timestamp::new);
    BlobObjectInfo {
        key: meta.location.to_string(),
        size_bytes: meta.size,
        content_type: None,
        etag: meta.e_tag,
        last_modified,
    }
}

pub const DEFAULT_MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;

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
pub struct BlobUploadReceipt {
    pub object: BlobObjectInfo,
    pub digest: String,
}

impl BlobUploadReceipt {
    pub fn object_key(&self) -> &str {
        &self.object.key
    }
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

    pub fn content_addressed_key(&self, digest: &str) -> Result<String, BlobContractError> {
        let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
        if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(BlobContractError::InvalidKey {
                reason: format!("invalid sha256 digest for content-addressed key: {digest}"),
            });
        }
        Ok(self.object_key(&format!(
            "objects/sha256/{}/{}/{}",
            &hex[..2],
            &hex[2..4],
            hex
        )))
    }
}

pub fn compute_blob_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{}", hex_encode(digest.as_slice()))
}

pub async fn collect_blob_stream_bytes(
    mut stream: BlobStoreByteStream,
) -> Result<Vec<u8>, BlobStoreError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

pub async fn read_blob_bytes(
    store: &dyn BlobStore,
    key: &str,
    opts: BlobGetOptions,
) -> Result<Vec<u8>, BlobStoreError> {
    let stream = store.get(key, opts).await?;
    collect_blob_stream_bytes(stream).await
}

pub async fn upload_blob_stream(
    store: &dyn BlobStore,
    layout: &BlobObjectLayout,
    data: BlobStoreByteStream,
    opts: BlobPutOptions,
) -> Result<BlobUploadReceipt, BlobStoreError> {
    let bytes = collect_blob_stream_bytes(data).await?;
    upload_blob_bytes(store, layout, Bytes::from(bytes), opts).await
}

pub async fn upload_blob_bytes(
    store: &dyn BlobStore,
    layout: &BlobObjectLayout,
    bytes: Bytes,
    mut opts: BlobPutOptions,
) -> Result<BlobUploadReceipt, BlobStoreError> {
    let digest = compute_blob_digest(bytes.as_ref());
    let object_key = layout.content_addressed_key(&digest)?;
    let size_bytes = bytes.len() as u64;

    if let Some(expected_size) = opts.expected_size
        && expected_size != size_bytes
    {
        return Err(BlobStoreError::Storage(StorageError::corruption(format!(
            "blob-store upload expected {expected_size} bytes for {object_key}, got {size_bytes}",
        ))));
    }
    opts.expected_size = Some(size_bytes);

    let result = store
        .put(&object_key, stream_from_bytes(bytes.to_vec()), opts.clone())
        .await;
    match result {
        Ok(object) => Ok(BlobUploadReceipt { object, digest }),
        Err(error)
            if error.recovery_hint(BlobStoreOperation::Put)
                == BlobStoreRecoveryHint::ConfirmWithStat =>
        {
            match store.stat(&object_key).await {
                Ok(Some(object)) if object.size_bytes == size_bytes => {
                    Ok(BlobUploadReceipt { object, digest })
                }
                Ok(_) | Err(_) => Err(error),
            }
        }
        Err(error) => Err(error),
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

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<BlobObjectInfo>, BlobStoreError> {
        let _ = prefix;
        Err(BlobStoreError::UnsupportedOperation {
            operation: "list_prefix",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BlobStoreFailureMode {
    Error {
        error: StorageError,
        after_write: bool,
    },
    InterruptedRead {
        after_bytes: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobStoreFailure {
    operation: BlobStoreOperation,
    target_prefix: Option<String>,
    mode: BlobStoreFailureMode,
    persistent: bool,
}

impl BlobStoreFailure {
    pub fn new(
        operation: BlobStoreOperation,
        target_prefix: Option<String>,
        error: StorageError,
    ) -> Self {
        Self {
            operation,
            target_prefix,
            mode: BlobStoreFailureMode::Error {
                error,
                after_write: false,
            },
            persistent: false,
        }
    }

    pub fn for_target(
        operation: BlobStoreOperation,
        target_prefix: impl Into<String>,
        error: StorageError,
    ) -> Self {
        Self::new(operation, Some(target_prefix.into()), error)
    }

    pub fn global(operation: BlobStoreOperation, error: StorageError) -> Self {
        Self::new(operation, None, error)
    }

    pub fn persistent(mut self) -> Self {
        self.persistent = true;
        self
    }

    pub fn timeout(operation: BlobStoreOperation, target_prefix: impl Into<String>) -> Self {
        Self::for_target(
            operation,
            target_prefix,
            StorageError::timeout("simulated blob-store timeout"),
        )
    }

    pub fn stale_list(target_prefix: impl Into<String>) -> Self {
        Self::for_target(
            BlobStoreOperation::List,
            target_prefix,
            StorageError::durability_boundary("simulated stale blob-store list"),
        )
    }

    pub fn lost_put_response(target_prefix: impl Into<String>) -> Self {
        Self {
            operation: BlobStoreOperation::Put,
            target_prefix: Some(target_prefix.into()),
            mode: BlobStoreFailureMode::Error {
                error: StorageError::timeout("simulated upload success with lost response"),
                after_write: true,
            },
            persistent: false,
        }
    }

    pub fn interrupted_read(
        operation: BlobStoreOperation,
        target_prefix: impl Into<String>,
        after_bytes: usize,
    ) -> Self {
        Self {
            operation,
            target_prefix: Some(target_prefix.into()),
            mode: BlobStoreFailureMode::InterruptedRead { after_bytes },
            persistent: false,
        }
    }

    fn matches(&self, operation: BlobStoreOperation, target: &str) -> bool {
        self.operation == operation
            && self
                .target_prefix
                .as_ref()
                .is_none_or(|prefix| target.starts_with(prefix))
    }
}

#[derive(Clone, Default)]
pub struct InMemoryBlobStore {
    inner: Arc<InMemoryBlobStoreInner>,
}

#[derive(Default)]
struct InMemoryBlobStoreInner {
    objects: Mutex<BTreeMap<String, StoredBlobObject>>,
    failures: Mutex<VecDeque<BlobStoreFailure>>,
}

#[derive(Clone)]
struct StoredBlobObject {
    bytes: Vec<u8>,
    info: BlobObjectInfo,
}

impl std::fmt::Debug for InMemoryBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryBlobStore")
            .field("object_count", &lock(&self.inner.objects).len())
            .finish()
    }
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inject_failure(&self, failure: BlobStoreFailure) {
        lock(&self.inner.failures).push_back(failure);
    }

    fn take_failure(
        &self,
        operation: BlobStoreOperation,
        target: &str,
    ) -> Option<BlobStoreFailure> {
        let mut failures = lock(&self.inner.failures);
        let index = failures
            .iter()
            .position(|failure| failure.matches(operation, target))?;
        if failures[index].persistent {
            Some(failures[index].clone())
        } else {
            failures.remove(index)
        }
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
        let bytes = collect_blob_stream_bytes(data).await?;
        if let Some(expected_size) = opts.expected_size
            && expected_size != bytes.len() as u64
        {
            return Err(BlobStoreError::Storage(StorageError::corruption(format!(
                "blob-store put expected {expected_size} bytes for {key}, got {}",
                bytes.len()
            ))));
        }

        let failure = self.take_failure(BlobStoreOperation::Put, key);
        if let Some(BlobStoreFailure {
            mode:
                BlobStoreFailureMode::Error {
                    error,
                    after_write: false,
                },
            ..
        }) = failure.as_ref()
        {
            return Err(BlobStoreError::Storage(error.clone()));
        }

        let info = BlobObjectInfo {
            key: key.to_string(),
            size_bytes: bytes.len() as u64,
            content_type: opts.content_type,
            etag: Some(format!("mem-{:016x}", bytes.len())),
            last_modified: None,
        };
        lock(&self.inner.objects).insert(
            key.to_string(),
            StoredBlobObject {
                bytes,
                info: info.clone(),
            },
        );

        if let Some(BlobStoreFailure {
            mode:
                BlobStoreFailureMode::Error {
                    error,
                    after_write: true,
                },
            ..
        }) = failure
        {
            return Err(BlobStoreError::Storage(error));
        }

        Ok(info)
    }

    async fn get(
        &self,
        key: &str,
        opts: BlobGetOptions,
    ) -> Result<BlobStoreByteStream, BlobStoreError> {
        let failure = self.take_failure(BlobStoreOperation::Get, key);
        if let Some(BlobStoreFailure {
            mode:
                BlobStoreFailureMode::Error {
                    error,
                    after_write: _,
                },
            ..
        }) = failure.as_ref()
        {
            return Err(BlobStoreError::Storage(error.clone()));
        }

        let stored = lock(&self.inner.objects).get(key).cloned().ok_or_else(|| {
            BlobStoreError::NotFound {
                key: key.to_string(),
            }
        })?;

        let bytes = if let Some(range) = opts.range {
            let slice = range.to_range(key, stored.info.size_bytes)?;
            stored.bytes[slice].to_vec()
        } else {
            stored.bytes
        };

        if let Some(BlobStoreFailure {
            mode: BlobStoreFailureMode::InterruptedRead { after_bytes },
            ..
        }) = failure
        {
            return Ok(interrupted_stream_from_bytes(
                bytes,
                after_bytes,
                BlobStoreError::Storage(StorageError::corruption(
                    "simulated interrupted blob-store read",
                )),
            ));
        }

        Ok(stream_from_bytes(bytes))
    }

    async fn stat(&self, key: &str) -> Result<Option<BlobObjectInfo>, BlobStoreError> {
        if let Some(BlobStoreFailure {
            mode:
                BlobStoreFailureMode::Error {
                    error,
                    after_write: _,
                },
            ..
        }) = self.take_failure(BlobStoreOperation::Stat, key)
        {
            return Err(BlobStoreError::Storage(error));
        }

        Ok(lock(&self.inner.objects)
            .get(key)
            .map(|stored| stored.info.clone()))
    }

    async fn delete(&self, key: &str) -> Result<(), BlobStoreError> {
        if let Some(BlobStoreFailure {
            mode:
                BlobStoreFailureMode::Error {
                    error,
                    after_write: _,
                },
            ..
        }) = self.take_failure(BlobStoreOperation::Delete, key)
        {
            return Err(BlobStoreError::Storage(error));
        }

        lock(&self.inner.objects).remove(key);
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<BlobObjectInfo>, BlobStoreError> {
        if let Some(BlobStoreFailure {
            mode:
                BlobStoreFailureMode::Error {
                    error,
                    after_write: _,
                },
            ..
        }) = self.take_failure(BlobStoreOperation::List, prefix)
        {
            return Err(BlobStoreError::Storage(error));
        }

        Ok(lock(&self.inner.objects)
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(_, stored)| stored.info.clone())
            .collect())
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
        let bytes = collect_blob_stream_bytes(data).await?;
        if let Some(expected_size) = opts.expected_size
            && expected_size != bytes.len() as u64
        {
            return Err(BlobStoreError::Storage(StorageError::corruption(format!(
                "blob-store adapter expected {expected_size} bytes for {key}, got {}",
                bytes.len()
            ))));
        }

        self.inner
            .put(key, &bytes)
            .await
            .map_err(|error| map_storage_blob_error(key, error))?;
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
        .map_err(|error| map_storage_blob_error(key, error))?;

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
        self.inner
            .delete(key)
            .await
            .map_err(|error| map_storage_blob_error(key, error))?;
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<BlobObjectInfo>, BlobStoreError> {
        let mut objects = Vec::new();
        for key in self
            .inner
            .list(prefix)
            .await
            .map_err(BlobStoreError::Storage)?
        {
            if let Some(info) = self.stat(&key).await? {
                objects.push(info);
            }
        }
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(objects)
    }
}

#[derive(Clone)]
pub struct BlobStoreStandardObjectStoreAdapter {
    inner: Arc<dyn StandardObjectStore>,
    multipart_chunk_bytes: usize,
}

impl std::fmt::Debug for BlobStoreStandardObjectStoreAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobStoreStandardObjectStoreAdapter")
            .field("inner", &"<dyn StandardObjectStore>")
            .field("multipart_chunk_bytes", &self.multipart_chunk_bytes)
            .finish()
    }
}

impl BlobStoreStandardObjectStoreAdapter {
    pub fn new(inner: Arc<dyn StandardObjectStore>) -> Self {
        Self {
            inner,
            multipart_chunk_bytes: DEFAULT_MULTIPART_CHUNK_BYTES,
        }
    }

    pub fn with_multipart_chunk_bytes(mut self, multipart_chunk_bytes: usize) -> Self {
        self.multipart_chunk_bytes = multipart_chunk_bytes.max(1);
        self
    }

    async fn abort_upload(upload: &mut Box<dyn StandardMultipartUpload>) {
        let _ = upload.abort().await;
    }
}

#[async_trait]
impl BlobStore for BlobStoreStandardObjectStoreAdapter {
    async fn put(
        &self,
        key: &str,
        mut data: BlobStoreByteStream,
        opts: BlobPutOptions,
    ) -> Result<BlobObjectInfo, BlobStoreError> {
        let location = standard_object_key(key)?;
        let mut total_size = 0_u64;
        let mut upload: Option<Box<dyn StandardMultipartUpload>> = None;
        let mut buffer = PutPayloadMut::new().with_block_size(self.multipart_chunk_bytes);

        while let Some(chunk) = data.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    if let Some(upload) = upload.as_mut() {
                        Self::abort_upload(upload).await;
                    }
                    return Err(error);
                }
            };
            total_size = total_size.saturating_add(chunk.len() as u64);
            buffer.push(chunk);

            if buffer.content_length() < self.multipart_chunk_bytes {
                continue;
            }

            if upload.is_none() {
                upload = Some(
                    self.inner
                        .put_multipart_opts(&location, Default::default())
                        .await
                        .map_err(|error| map_standard_blob_error(key, error))?,
                );
            }

            let payload = std::mem::replace(
                &mut buffer,
                PutPayloadMut::new().with_block_size(self.multipart_chunk_bytes),
            )
            .freeze();
            if payload.content_length() == 0 {
                continue;
            }

            if let Err(error) = upload
                .as_mut()
                .expect("multipart upload must exist after first flush")
                .put_part(payload)
                .await
                .map_err(|error| map_standard_blob_error(key, error))
            {
                if let Some(upload) = upload.as_mut() {
                    Self::abort_upload(upload).await;
                }
                return Err(error);
            }
        }

        if let Some(expected_size) = opts.expected_size
            && expected_size != total_size
        {
            if let Some(upload) = upload.as_mut() {
                Self::abort_upload(upload).await;
            }
            return Err(BlobStoreError::Storage(StorageError::corruption(format!(
                "blob-store standard adapter expected {expected_size} bytes for {key}, got {total_size}",
            ))));
        }

        let etag = if let Some(mut upload) = upload {
            let payload = buffer.freeze();
            if payload.content_length() > 0 {
                upload
                    .put_part(payload)
                    .await
                    .map_err(|error| map_standard_blob_error(key, error))?;
            }
            upload
                .complete()
                .await
                .map_err(|error| map_standard_blob_error(key, error))?
                .e_tag
        } else {
            self.inner
                .put_opts(&location, buffer.freeze(), StandardPutOptions::default())
                .await
                .map_err(|error| map_standard_blob_error(key, error))?
                .e_tag
        };

        Ok(BlobObjectInfo {
            key: key.to_string(),
            size_bytes: total_size,
            content_type: opts.content_type,
            etag,
            last_modified: None,
        })
    }

    async fn get(
        &self,
        key: &str,
        opts: BlobGetOptions,
    ) -> Result<BlobStoreByteStream, BlobStoreError> {
        let location = standard_object_key(key)?;
        let key = key.to_string();
        let get_options = StandardGetOptions::default()
            .with_range(opts.range.map(|range| range.start()..range.end()));
        let result = self
            .inner
            .get_opts(&location, get_options)
            .await
            .map_err(|error| map_standard_blob_error(&key, error))?;
        Ok(Box::pin(result.into_stream().map(move |chunk| {
            chunk.map_err(|error| map_standard_blob_error(&key, error))
        })))
    }

    async fn stat(&self, key: &str) -> Result<Option<BlobObjectInfo>, BlobStoreError> {
        let location = standard_object_key(key)?;
        match self.inner.head(&location).await {
            Ok(meta) => Ok(Some(object_info_from_meta(meta))),
            Err(StandardObjectStoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(map_standard_blob_error(key, error)),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), BlobStoreError> {
        let location = standard_object_key(key)?;
        self.inner
            .delete(&location)
            .await
            .map_err(|error| map_standard_blob_error(key, error))?;
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<BlobObjectInfo>, BlobStoreError> {
        let prefix = standard_object_prefix(prefix)?;
        let mut objects = self
            .inner
            .list(prefix.as_ref())
            .map_ok(object_info_from_meta)
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| {
                map_standard_blob_error(prefix.as_ref().map_or("", |p| p.as_ref()), error)
            })?;
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(objects)
    }
}

pub(crate) fn stream_from_bytes(bytes: Vec<u8>) -> BlobStoreByteStream {
    Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) }))
}

fn interrupted_stream_from_bytes(
    bytes: Vec<u8>,
    after_bytes: usize,
    error: BlobStoreError,
) -> BlobStoreByteStream {
    let split = after_bytes.min(bytes.len());
    let first = if split > 0 {
        Some(Ok(Bytes::from(bytes[..split].to_vec())))
    } else {
        None
    };
    let stream = first.into_iter().chain(std::iter::once(Err(error)));
    Box::pin(stream::iter(stream))
}
