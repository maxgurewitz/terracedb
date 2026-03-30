use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use parking_lot::{Mutex, MutexGuard};
use tokio::sync::watch;

use terracedb::{LogCursor, SequenceNumber, Timestamp};

use crate::{
    BlobActivityEntry, BlobActivityId, BlobActivityKind, BlobActivityOptions, BlobActivityReceiver,
    BlobActivityStream, BlobAlias, BlobByteRange, BlobContractError, BlobError, BlobId,
    BlobObjectLayout, BlobPutOptions, BlobStore, BlobStoreByteStream, BlobStoreError, JsonValue,
    upload_blob_bytes,
};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}

fn map_store_stream(stream: BlobStoreByteStream) -> BlobByteStream {
    Box::pin(stream.map(|result| result.map_err(BlobError::from)))
}

pub type BlobByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, BlobError>> + Send + 'static>>;
pub type BlobSearchStream =
    Pin<Box<dyn Stream<Item = Result<BlobSearchRow, BlobError>> + Send + 'static>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobCollectionConfig {
    namespace: String,
    pub create_if_missing: bool,
}

pub type BlobLibraryConfig = BlobCollectionConfig;

impl BlobCollectionConfig {
    pub fn new(namespace: impl Into<String>) -> Result<Self, BlobContractError> {
        let namespace = namespace.into();
        if namespace.is_empty() {
            return Err(BlobContractError::EmptyNamespace);
        }
        if namespace.as_bytes().contains(&0) {
            return Err(BlobContractError::NulByteInKeyPart { field: "namespace" });
        }

        Ok(Self {
            namespace,
            create_if_missing: false,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn with_create_if_missing(mut self, create_if_missing: bool) -> Self {
        self.create_if_missing = create_if_missing;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobPublishOrdering {
    UploadBeforePublish,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobDeleteSemantics {
    MetadataDeleteBeforeGc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobObjectReclamation {
    DeferredGcOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobIndexDurability {
    DurableActivityOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobMissingObjectSemantics {
    FailClosed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobSemantics {
    pub publish_ordering: BlobPublishOrdering,
    pub delete_visibility: BlobDeleteSemantics,
    pub object_reclamation: BlobObjectReclamation,
    pub indexing: BlobIndexDurability,
    pub missing_object_reads: BlobMissingObjectSemantics,
}

pub const BLOB_COLLECTION_SEMANTICS: BlobSemantics = BlobSemantics {
    publish_ordering: BlobPublishOrdering::UploadBeforePublish,
    delete_visibility: BlobDeleteSemantics::MetadataDeleteBeforeGc,
    object_reclamation: BlobObjectReclamation::DeferredGcOnly,
    indexing: BlobIndexDurability::DurableActivityOnly,
    missing_object_reads: BlobMissingObjectSemantics::FailClosed,
};

pub enum BlobWriteData {
    Bytes(Bytes),
    Stream(BlobStoreByteStream),
}

impl std::fmt::Debug for BlobWriteData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bytes(bytes) => f
                .debug_tuple("BlobWriteData::Bytes")
                .field(&bytes.len())
                .finish(),
            Self::Stream(_) => f.write_str("BlobWriteData::Stream(<stream>)"),
        }
    }
}

impl BlobWriteData {
    pub fn bytes(data: impl Into<Bytes>) -> Self {
        Self::Bytes(data.into())
    }

    async fn into_bytes(self) -> Result<Vec<u8>, BlobStoreError> {
        match self {
            Self::Bytes(bytes) => Ok(bytes.to_vec()),
            Self::Stream(mut stream) => {
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    bytes.extend_from_slice(&chunk?);
                }
                Ok(bytes)
            }
        }
    }
}

#[derive(Debug)]
pub struct BlobWrite {
    pub alias: Option<BlobAlias>,
    pub data: BlobWriteData,
    pub content_type: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

impl BlobWrite {
    pub fn from_bytes(data: impl Into<Bytes>) -> Self {
        Self {
            alias: None,
            data: BlobWriteData::bytes(data),
            content_type: None,
            tags: BTreeMap::new(),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobHandle {
    pub id: BlobId,
    pub object_key: String,
    pub digest: String,
    pub size_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BlobIndexState {
    #[default]
    Pending,
    Indexed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobMetadata {
    pub id: BlobId,
    pub namespace: String,
    pub alias: Option<BlobAlias>,
    pub object_key: String,
    pub digest: String,
    pub size_bytes: u64,
    pub content_type: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, JsonValue>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub index_state: BlobIndexState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobLocator {
    Id(BlobId),
    Alias(BlobAlias),
}

impl From<BlobId> for BlobLocator {
    fn from(value: BlobId) -> Self {
        Self::Id(value)
    }
}

impl From<BlobAlias> for BlobLocator {
    fn from(value: BlobAlias) -> Self {
        Self::Alias(value)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobReadOptions {
    pub range: Option<BlobByteRange>,
}

pub struct BlobReadResult {
    pub metadata: BlobMetadata,
    pub range: Option<BlobByteRange>,
    pub data: BlobByteStream,
}

impl std::fmt::Debug for BlobReadResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReadResult")
            .field("metadata", &self.metadata)
            .field("range", &self.range)
            .field("data", &"<stream>")
            .finish()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlobQuery {
    pub alias_prefix: Option<String>,
    pub content_type: Option<String>,
    pub required_tags: BTreeMap<String, String>,
    pub required_metadata: BTreeMap<String, JsonValue>,
    pub terms: Vec<String>,
    pub min_size_bytes: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub created_after: Option<Timestamp>,
    pub created_before: Option<Timestamp>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobSearchRow {
    pub metadata: BlobMetadata,
    pub snippet: Option<String>,
}

#[async_trait]
pub trait BlobCollection: Send + Sync {
    fn config(&self) -> &BlobCollectionConfig;

    fn semantics(&self) -> BlobSemantics {
        BLOB_COLLECTION_SEMANTICS
    }

    async fn put(&self, input: BlobWrite) -> Result<BlobHandle, BlobError>;
    async fn stat(&self, target: BlobLocator) -> Result<Option<BlobMetadata>, BlobError>;
    async fn get(
        &self,
        target: BlobLocator,
        opts: BlobReadOptions,
    ) -> Result<BlobReadResult, BlobError>;
    async fn delete(&self, target: BlobLocator) -> Result<(), BlobError>;
    async fn search(&self, query: BlobQuery) -> Result<BlobSearchStream, BlobError>;
    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: BlobActivityOptions,
    ) -> Result<BlobActivityStream, BlobError>;
    fn subscribe_activity(&self, opts: BlobActivityOptions) -> BlobActivityReceiver;
}

#[derive(Clone)]
pub struct InMemoryBlobCollection {
    inner: Arc<InMemoryBlobCollectionInner>,
}

struct InMemoryBlobCollectionInner {
    config: BlobCollectionConfig,
    layout: BlobObjectLayout,
    blob_store: Arc<dyn BlobStore>,
    state: Mutex<CollectionState>,
    activity_watch: watch::Sender<SequenceNumber>,
    durable_activity_watch: watch::Sender<SequenceNumber>,
}

#[derive(Default)]
struct CollectionState {
    next_blob_id: u128,
    next_activity_id: u64,
    next_sequence: u64,
    next_timestamp: u64,
    blobs: BTreeMap<BlobId, BlobMetadata>,
    aliases: BTreeMap<BlobAlias, BlobId>,
    activities: Vec<BlobActivityEntry>,
}

impl std::fmt::Debug for InMemoryBlobCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = lock(&self.inner.state);
        f.debug_struct("InMemoryBlobCollection")
            .field("namespace", &self.inner.config.namespace())
            .field("blob_count", &state.blobs.len())
            .field("activity_count", &state.activities.len())
            .finish()
    }
}

impl InMemoryBlobCollection {
    pub fn new(config: BlobCollectionConfig) -> Self {
        Self::with_blob_store(config, Arc::new(crate::InMemoryBlobStore::new()))
    }

    pub fn with_blob_store(config: BlobCollectionConfig, blob_store: Arc<dyn BlobStore>) -> Self {
        let layout = BlobObjectLayout::new("", config.namespace())
            .expect("validated blob collection config must produce a valid object layout");
        let (activity_watch, _receiver) = watch::channel(SequenceNumber::new(0));
        let (durable_activity_watch, _receiver) = watch::channel(SequenceNumber::new(0));

        Self {
            inner: Arc::new(InMemoryBlobCollectionInner {
                config,
                layout,
                blob_store,
                state: Mutex::new(CollectionState {
                    next_blob_id: 1,
                    next_activity_id: 1,
                    next_sequence: 0,
                    next_timestamp: 1,
                    blobs: BTreeMap::new(),
                    aliases: BTreeMap::new(),
                    activities: Vec::new(),
                }),
                activity_watch,
                durable_activity_watch,
            }),
        }
    }

    fn resolve_metadata(
        state: &CollectionState,
        target: BlobLocator,
    ) -> Result<Option<BlobMetadata>, BlobError> {
        match target {
            BlobLocator::Id(id) => Ok(state.blobs.get(&id).cloned()),
            BlobLocator::Alias(alias) => match state.aliases.get(&alias).copied() {
                Some(id) => Ok(state.blobs.get(&id).cloned()),
                None => Ok(None),
            },
        }
    }

    fn next_sequence(state: &mut CollectionState) -> SequenceNumber {
        state.next_sequence += 1;
        SequenceNumber::new(state.next_sequence)
    }

    fn next_timestamp(state: &mut CollectionState) -> Timestamp {
        let timestamp = Timestamp::new(state.next_timestamp);
        state.next_timestamp += 1;
        timestamp
    }

    fn next_blob_id(state: &mut CollectionState) -> BlobId {
        let id = BlobId::new(state.next_blob_id);
        state.next_blob_id += 1;
        id
    }

    fn next_activity_id(state: &mut CollectionState) -> BlobActivityId {
        let id = BlobActivityId::new(state.next_activity_id);
        state.next_activity_id += 1;
        id
    }

    fn push_activity(
        state: &mut CollectionState,
        namespace: &str,
        sequence: SequenceNumber,
        op_index: u16,
        kind: BlobActivityKind,
        blob_id: Option<BlobId>,
        alias: Option<BlobAlias>,
        object_key: Option<String>,
        metadata: BTreeMap<String, JsonValue>,
    ) {
        let activity_id = Self::next_activity_id(state);
        let timestamp = Self::next_timestamp(state);
        state.activities.push(BlobActivityEntry {
            namespace: namespace.to_string(),
            activity_id,
            sequence,
            cursor: LogCursor::new(sequence, op_index),
            timestamp,
            kind,
            blob_id,
            alias,
            object_key,
            metadata,
        });
    }
}

#[async_trait]
impl BlobCollection for InMemoryBlobCollection {
    fn config(&self) -> &BlobCollectionConfig {
        &self.inner.config
    }

    async fn put(&self, input: BlobWrite) -> Result<BlobHandle, BlobError> {
        let alias = input.alias.clone();
        let content_type = input.content_type.clone();
        let tags = input.tags.clone();
        let metadata_fields = input.metadata.clone();
        let bytes = input.data.into_bytes().await?;
        let upload = upload_blob_bytes(
            self.inner.blob_store.as_ref(),
            &self.inner.layout,
            Bytes::from(bytes.clone()),
            BlobPutOptions {
                content_type: content_type.clone(),
                expected_size: Some(bytes.len() as u64),
            },
        )
        .await?;
        let digest = upload.digest.clone();
        let object_key = upload.object.key.clone();
        let size_bytes = upload.object.size_bytes;

        let blob_id = {
            let mut state = lock(&self.inner.state);
            Self::next_blob_id(&mut state)
        };

        let handle = BlobHandle {
            id: blob_id,
            object_key: object_key.clone(),
            digest: digest.clone(),
            size_bytes,
        };

        let sequence = {
            let mut state = lock(&self.inner.state);
            let sequence = Self::next_sequence(&mut state);
            let timestamp = Self::next_timestamp(&mut state);

            if let Some(alias) = alias.as_ref()
                && let Some(previous_blob_id) = state.aliases.insert(alias.clone(), blob_id)
                && let Some(previous) = state.blobs.get_mut(&previous_blob_id)
            {
                previous.alias = None;
                previous.updated_at = timestamp;
            }

            let metadata = BlobMetadata {
                id: blob_id,
                namespace: self.inner.config.namespace().to_string(),
                alias: alias.clone(),
                object_key: object_key.clone(),
                digest,
                size_bytes,
                content_type,
                tags,
                metadata: metadata_fields,
                created_at: timestamp,
                updated_at: timestamp,
                index_state: BlobIndexState::Pending,
            };
            state.blobs.insert(blob_id, metadata);

            Self::push_activity(
                &mut state,
                self.inner.config.namespace(),
                sequence,
                0,
                BlobActivityKind::BlobPublished,
                Some(blob_id),
                alias.clone(),
                Some(object_key.clone()),
                BTreeMap::new(),
            );
            if alias.is_some() {
                Self::push_activity(
                    &mut state,
                    self.inner.config.namespace(),
                    sequence,
                    1,
                    BlobActivityKind::AliasUpserted,
                    Some(blob_id),
                    alias.clone(),
                    Some(object_key.clone()),
                    BTreeMap::new(),
                );
            }

            sequence
        };

        self.inner.activity_watch.send_replace(sequence);
        self.inner.durable_activity_watch.send_replace(sequence);
        Ok(handle)
    }

    async fn stat(&self, target: BlobLocator) -> Result<Option<BlobMetadata>, BlobError> {
        let state = lock(&self.inner.state);
        Self::resolve_metadata(&state, target)
    }

    async fn get(
        &self,
        target: BlobLocator,
        opts: BlobReadOptions,
    ) -> Result<BlobReadResult, BlobError> {
        let metadata = {
            let state = lock(&self.inner.state);
            Self::resolve_metadata(&state, target.clone())?
        };
        let Some(metadata) = metadata else {
            return match target {
                BlobLocator::Id(id) => Err(BlobError::NotFound { id }),
                BlobLocator::Alias(alias) => Err(BlobError::AliasNotFound { alias }),
            };
        };

        let store_result = self
            .inner
            .blob_store
            .get(
                &metadata.object_key,
                crate::BlobGetOptions { range: opts.range },
            )
            .await;
        let stream = match store_result {
            Ok(stream) => map_store_stream(stream),
            Err(BlobStoreError::NotFound { .. }) => {
                return Err(BlobError::MissingObject {
                    object_key: metadata.object_key.clone(),
                });
            }
            Err(error) => return Err(BlobError::Store(error)),
        };

        Ok(BlobReadResult {
            metadata,
            range: opts.range,
            data: stream,
        })
    }

    async fn delete(&self, target: BlobLocator) -> Result<(), BlobError> {
        let sequence = {
            let mut state = lock(&self.inner.state);
            let Some(metadata) = Self::resolve_metadata(&state, target.clone())? else {
                return match target {
                    BlobLocator::Id(id) => Err(BlobError::NotFound { id }),
                    BlobLocator::Alias(alias) => Err(BlobError::AliasNotFound { alias }),
                };
            };

            let sequence = Self::next_sequence(&mut state);
            if let Some(alias) = metadata.alias.as_ref() {
                state.aliases.remove(alias);
                Self::push_activity(
                    &mut state,
                    self.inner.config.namespace(),
                    sequence,
                    0,
                    BlobActivityKind::AliasRemoved,
                    Some(metadata.id),
                    Some(alias.clone()),
                    Some(metadata.object_key.clone()),
                    BTreeMap::new(),
                );
                Self::push_activity(
                    &mut state,
                    self.inner.config.namespace(),
                    sequence,
                    1,
                    BlobActivityKind::BlobDeleted,
                    Some(metadata.id),
                    Some(alias.clone()),
                    Some(metadata.object_key.clone()),
                    BTreeMap::new(),
                );
            } else {
                Self::push_activity(
                    &mut state,
                    self.inner.config.namespace(),
                    sequence,
                    0,
                    BlobActivityKind::BlobDeleted,
                    Some(metadata.id),
                    None,
                    Some(metadata.object_key.clone()),
                    BTreeMap::new(),
                );
            }
            state.blobs.remove(&metadata.id);
            sequence
        };

        self.inner.activity_watch.send_replace(sequence);
        self.inner.durable_activity_watch.send_replace(sequence);
        Ok(())
    }

    async fn search(&self, query: BlobQuery) -> Result<BlobSearchStream, BlobError> {
        let mut rows = {
            let state = lock(&self.inner.state);
            state
                .blobs
                .values()
                .filter(|metadata| matches_query(metadata, &query))
                .cloned()
                .map(|metadata| {
                    Ok(BlobSearchRow {
                        snippet: query
                            .terms
                            .first()
                            .filter(|term| {
                                searchable_text(&metadata).contains(&term.to_lowercase())
                            })
                            .map(|term| format!("matched:{term}")),
                        metadata,
                    })
                })
                .collect::<Vec<_>>()
        };
        if let Some(limit) = query.limit {
            rows.truncate(limit);
        }
        Ok(Box::pin(stream::iter(rows)))
    }

    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: BlobActivityOptions,
    ) -> Result<BlobActivityStream, BlobError> {
        let entries = {
            let state = lock(&self.inner.state);
            let mut entries = state
                .activities
                .iter()
                .filter(|entry| entry.cursor > cursor)
                .cloned()
                .collect::<Vec<_>>();
            if let Some(limit) = opts.limit {
                entries.truncate(limit);
            }
            entries
        };
        Ok(Box::pin(stream::iter(entries.into_iter().map(Ok))))
    }

    fn subscribe_activity(&self, opts: BlobActivityOptions) -> BlobActivityReceiver {
        if opts.durable {
            BlobActivityReceiver::new(self.inner.durable_activity_watch.subscribe())
        } else {
            BlobActivityReceiver::new(self.inner.activity_watch.subscribe())
        }
    }
}

fn matches_query(metadata: &BlobMetadata, query: &BlobQuery) -> bool {
    if let Some(alias_prefix) = query.alias_prefix.as_ref()
        && metadata
            .alias
            .as_ref()
            .map(|alias| !alias.as_str().starts_with(alias_prefix))
            .unwrap_or(true)
    {
        return false;
    }

    if let Some(content_type) = query.content_type.as_ref()
        && metadata.content_type.as_ref() != Some(content_type)
    {
        return false;
    }

    if query
        .required_tags
        .iter()
        .any(|(key, value)| metadata.tags.get(key) != Some(value))
    {
        return false;
    }

    if query
        .required_metadata
        .iter()
        .any(|(key, value)| metadata.metadata.get(key) != Some(value))
    {
        return false;
    }

    if let Some(min_size) = query.min_size_bytes
        && metadata.size_bytes < min_size
    {
        return false;
    }
    if let Some(max_size) = query.max_size_bytes
        && metadata.size_bytes > max_size
    {
        return false;
    }
    if let Some(created_after) = query.created_after
        && metadata.created_at < created_after
    {
        return false;
    }
    if let Some(created_before) = query.created_before
        && metadata.created_at > created_before
    {
        return false;
    }

    let searchable = searchable_text(metadata);
    query
        .terms
        .iter()
        .all(|term| searchable.contains(&term.to_lowercase()))
}

fn searchable_text(metadata: &BlobMetadata) -> String {
    let mut fields = Vec::new();
    fields.push(metadata.digest.to_lowercase());
    if let Some(alias) = metadata.alias.as_ref() {
        fields.push(alias.as_str().to_lowercase());
    }
    if let Some(content_type) = metadata.content_type.as_ref() {
        fields.push(content_type.to_lowercase());
    }
    fields.extend(
        metadata
            .tags
            .iter()
            .flat_map(|(key, value)| [key.to_lowercase(), value.to_lowercase()]),
    );
    fields.extend(
        metadata
            .metadata
            .iter()
            .map(|(key, value)| format!("{}={}", key.to_lowercase(), value)),
    );
    fields.join(" ")
}
