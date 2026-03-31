use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;

use terracedb::{
    ChangeFeedError, ChangeKind, CommitError, CommitOptions, CreateTableError, Db, LogCursor,
    ReadError, ReadSet, ScanOptions, Snapshot, StorageError, StorageErrorKind, Table, Timestamp,
    Value,
};

#[cfg(test)]
use parking_lot::Mutex;

use crate::store::stream_from_bytes;
use crate::{
    BLOB_ACTIVITY_TABLE_NAME, BLOB_ALIAS_TABLE_NAME, BLOB_CATALOG_TABLE_NAME,
    BLOB_OBJECT_GC_TABLE_NAME, BlobActivityEntry, BlobActivityId, BlobActivityKey,
    BlobActivityKind, BlobActivityOptions, BlobActivityReceiver, BlobActivityStream, BlobAlias,
    BlobAliasKey, BlobCatalogKey, BlobCollection, BlobCollectionConfig, BlobError, BlobHandle,
    BlobId, BlobIndexState, BlobLocator, BlobMetadata, BlobObjectGcKey, BlobObjectLayout,
    BlobPutOptions, BlobReadOptions, BlobReadResult, BlobSearchRow, BlobSearchStream, BlobStore,
    BlobStoreByteStream, BlobStoreError, BlobWrite, BlobWriteData, JsonValue,
    frozen_table_descriptors,
};

const MAX_METADATA_CONFLICT_RETRIES: usize = 16;

#[cfg(test)]
const INJECTED_FAILPOINT_MESSAGE: &str = "injected terracedb-bricks failpoint";

#[derive(Clone)]
pub struct TerracedbBlobCollection {
    inner: Arc<TerracedbBlobCollectionInner>,
}

struct TerracedbBlobCollectionInner {
    config: BlobCollectionConfig,
    layout: BlobObjectLayout,
    db: Db,
    blob_store: Arc<dyn BlobStore>,
    catalog_table: Table,
    alias_table: Table,
    gc_table: Table,
    activity_table: Table,
    blob_id_nonce: u64,
    next_blob_counter: AtomicU64,
    next_activity_id: AtomicU64,
    #[cfg(test)]
    failpoints: Mutex<Vec<TestFailpoint>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BlobCatalogRow {
    alias: Option<BlobAlias>,
    object_key: String,
    digest: String,
    size_bytes: u64,
    content_type: Option<String>,
    tags: BTreeMap<String, String>,
    metadata: BTreeMap<String, JsonValue>,
    created_at: Timestamp,
    updated_at: Timestamp,
    index_state: BlobIndexState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BlobAliasRow {
    blob_id: BlobId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BlobObjectGcRow {
    object_key: String,
    digest: String,
    size_bytes: u64,
    content_type: Option<String>,
    first_published_at: Timestamp,
    last_published_at: Timestamp,
    current_blob_id: Option<BlobId>,
    pending_delete_at: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct BlobActivityRow {
    timestamp: Timestamp,
    kind: BlobActivityKind,
    blob_id: Option<BlobId>,
    alias: Option<BlobAlias>,
    object_key: Option<String>,
    metadata: BTreeMap<String, JsonValue>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TestFailpoint {
    AfterUploadBeforePublish,
    AfterPublishBeforeAck,
}

#[derive(Clone, Debug)]
struct ResolvedBlob {
    metadata: BlobMetadata,
}

impl std::fmt::Debug for TerracedbBlobCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TerracedbBlobCollection")
            .field("namespace", &self.inner.config.namespace())
            .field("layout", &self.inner.layout)
            .finish()
    }
}

impl TerracedbBlobCollection {
    pub async fn open(
        db: Db,
        config: BlobCollectionConfig,
        blob_store: Arc<dyn BlobStore>,
    ) -> Result<Self, BlobError> {
        let layout = BlobObjectLayout::new("", config.namespace())?;

        let mut catalog_table = None;
        let mut alias_table = None;
        let mut gc_table = None;
        let mut activity_table = None;

        for descriptor in frozen_table_descriptors()
            .iter()
            .filter(|descriptor| descriptor.bootstrap)
        {
            let table = ensure_bootstrap_table(&db, descriptor, config.create_if_missing).await?;
            match descriptor.name {
                BLOB_CATALOG_TABLE_NAME => catalog_table = Some(table),
                BLOB_ALIAS_TABLE_NAME => alias_table = Some(table),
                BLOB_OBJECT_GC_TABLE_NAME => gc_table = Some(table),
                BLOB_ACTIVITY_TABLE_NAME => activity_table = Some(table),
                _ => {}
            }
        }

        Ok(Self {
            inner: Arc::new(TerracedbBlobCollectionInner {
                config,
                layout,
                db,
                blob_store,
                catalog_table: catalog_table.expect("bootstrap should resolve blob_catalog"),
                alias_table: alias_table.expect("bootstrap should resolve blob_alias"),
                gc_table: gc_table.expect("bootstrap should resolve blob_object_gc"),
                activity_table: activity_table.expect("bootstrap should resolve blob_activity"),
                blob_id_nonce: seeded_u64(0x626c6f62),
                next_blob_counter: AtomicU64::new(1),
                next_activity_id: AtomicU64::new(seeded_u64(0x61637479).max(1)),
                #[cfg(test)]
                failpoints: Mutex::new(Vec::new()),
            }),
        })
    }

    fn next_blob_id(&self) -> BlobId {
        let counter = self.inner.next_blob_counter.fetch_add(1, Ordering::SeqCst);
        BlobId::new(((self.inner.blob_id_nonce as u128) << 64) | counter as u128)
    }

    fn next_activity_id(&self) -> BlobActivityId {
        BlobActivityId::new(self.inner.next_activity_id.fetch_add(1, Ordering::SeqCst))
    }

    async fn resolve_at_snapshot(
        &self,
        snapshot: &Snapshot,
        target: BlobLocator,
    ) -> Result<Option<ResolvedBlob>, BlobError> {
        match target {
            BlobLocator::Id(id) => Ok(read_catalog_row_at(
                snapshot,
                &self.inner.catalog_table,
                self.inner.config.namespace(),
                id,
            )
            .await?
            .map(|row| ResolvedBlob {
                metadata: catalog_row_to_metadata(self.inner.config.namespace(), id, row),
            })),
            BlobLocator::Alias(alias) => {
                let alias_row = read_alias_row_at(
                    snapshot,
                    &self.inner.alias_table,
                    self.inner.config.namespace(),
                    &alias,
                )
                .await?;
                let Some(alias_row) = alias_row else {
                    return Ok(None);
                };

                let Some(catalog_row) = read_catalog_row_at(
                    snapshot,
                    &self.inner.catalog_table,
                    self.inner.config.namespace(),
                    alias_row.blob_id,
                )
                .await?
                else {
                    return Err(corruption(format!(
                        "blob alias {} in namespace {} points at missing catalog row {}",
                        alias,
                        self.inner.config.namespace(),
                        alias_row.blob_id
                    )));
                };
                if catalog_row.alias.as_ref() != Some(&alias) {
                    return Err(corruption(format!(
                        "blob alias {} in namespace {} points at catalog row {} with mismatched alias {:?}",
                        alias,
                        self.inner.config.namespace(),
                        alias_row.blob_id,
                        catalog_row.alias
                    )));
                }

                Ok(Some(ResolvedBlob {
                    metadata: catalog_row_to_metadata(
                        self.inner.config.namespace(),
                        alias_row.blob_id,
                        catalog_row,
                    ),
                }))
            }
        }
    }

    async fn find_other_reference(
        &self,
        snapshot: &Snapshot,
        object_key: &str,
        exclude_blob_id: BlobId,
    ) -> Result<Option<BlobMetadata>, BlobError> {
        let prefix = BlobCatalogKey::namespace_prefix(self.inner.config.namespace())?;
        let mut rows = snapshot
            .scan_prefix(&self.inner.catalog_table, prefix, ScanOptions::default())
            .await
            .map_err(map_read_error)?;

        while let Some((key, value)) = rows.next().await {
            let catalog_key = decode_catalog_key(&key)?;
            if catalog_key.blob_id == exclude_blob_id {
                continue;
            }

            let row = decode_row::<BlobCatalogRow>(value, BLOB_CATALOG_TABLE_NAME)?;
            if row.object_key == object_key {
                return Ok(Some(catalog_row_to_metadata(
                    self.inner.config.namespace(),
                    catalog_key.blob_id,
                    row,
                )));
            }
        }

        Ok(None)
    }

    async fn publish_with_retry(
        &self,
        blob_id: BlobId,
        object_key: String,
        alias: Option<BlobAlias>,
        digest: String,
        size_bytes: u64,
        content_type: Option<String>,
        tags: BTreeMap<String, String>,
        metadata_fields: BTreeMap<String, JsonValue>,
    ) -> Result<(), BlobError> {
        for _attempt in 0..MAX_METADATA_CONFLICT_RETRIES {
            let snapshot = self.inner.db.snapshot().await;
            let published_at = now_timestamp();

            let existing_alias = if let Some(alias_ref) = alias.as_ref() {
                read_alias_row_at(
                    &snapshot,
                    &self.inner.alias_table,
                    self.inner.config.namespace(),
                    alias_ref,
                )
                .await?
            } else {
                None
            };

            let previous = if let Some(alias_row) = existing_alias {
                let Some(row) = read_catalog_row_at(
                    &snapshot,
                    &self.inner.catalog_table,
                    self.inner.config.namespace(),
                    alias_row.blob_id,
                )
                .await?
                else {
                    return Err(corruption(format!(
                        "blob alias {:?} in namespace {} points at missing catalog row {}",
                        alias,
                        self.inner.config.namespace(),
                        alias_row.blob_id
                    )));
                };
                Some(catalog_row_to_metadata(
                    self.inner.config.namespace(),
                    alias_row.blob_id,
                    row,
                ))
            } else {
                None
            };

            let previous_other_reference = if let Some(previous) = previous.as_ref() {
                self.find_other_reference(&snapshot, &previous.object_key, previous.id)
                    .await?
            } else {
                None
            };

            let new_catalog_key = BlobCatalogKey {
                namespace: self.inner.config.namespace().to_string(),
                blob_id,
            };
            let new_gc_key = BlobObjectGcKey {
                namespace: self.inner.config.namespace().to_string(),
                object_key: object_key.clone(),
            };

            let existing_new_gc = read_gc_row_at(
                &snapshot,
                &self.inner.gc_table,
                self.inner.config.namespace(),
                &object_key,
            )
            .await?;

            let catalog_row = BlobCatalogRow {
                alias: alias.clone(),
                object_key: object_key.clone(),
                digest: digest.clone(),
                size_bytes,
                content_type: content_type.clone(),
                tags: tags.clone(),
                metadata: metadata_fields.clone(),
                created_at: published_at,
                updated_at: published_at,
                index_state: BlobIndexState::Pending,
            };

            let new_gc_row = BlobObjectGcRow {
                object_key: object_key.clone(),
                digest: digest.clone(),
                size_bytes,
                content_type: content_type.clone(),
                first_published_at: existing_new_gc
                    .as_ref()
                    .map(|row| row.first_published_at)
                    .unwrap_or(published_at),
                last_published_at: published_at,
                current_blob_id: Some(blob_id),
                pending_delete_at: None,
            };

            let mut batch = self.inner.db.write_batch();
            let mut read_set = ReadSet::default();

            batch.put(
                &self.inner.catalog_table,
                new_catalog_key.encode()?,
                encode_row(&catalog_row)?,
            );
            read_set.add(
                &self.inner.catalog_table,
                new_catalog_key.encode()?,
                snapshot.sequence(),
            );

            batch.put(
                &self.inner.gc_table,
                new_gc_key.encode()?,
                encode_row(&new_gc_row)?,
            );
            read_set.add(
                &self.inner.gc_table,
                new_gc_key.encode()?,
                snapshot.sequence(),
            );

            if let Some(alias_ref) = alias.as_ref() {
                let alias_key = BlobAliasKey {
                    namespace: self.inner.config.namespace().to_string(),
                    alias: alias_ref.clone(),
                };
                batch.put(
                    &self.inner.alias_table,
                    alias_key.encode()?,
                    encode_row(&BlobAliasRow { blob_id })?,
                );
                read_set.add(
                    &self.inner.alias_table,
                    alias_key.encode()?,
                    snapshot.sequence(),
                );
            }

            let publish_activity_metadata = BTreeMap::new();
            batch.put(
                &self.inner.activity_table,
                BlobActivityKey {
                    namespace: self.inner.config.namespace().to_string(),
                    activity_id: self.next_activity_id(),
                }
                .encode()?,
                encode_row(&BlobActivityRow {
                    timestamp: published_at,
                    kind: BlobActivityKind::BlobPublished,
                    blob_id: Some(blob_id),
                    alias: alias.clone(),
                    object_key: Some(object_key.clone()),
                    metadata: publish_activity_metadata,
                })?,
            );

            if let Some(alias_ref) = alias.as_ref() {
                let mut alias_metadata = BTreeMap::new();
                if let Some(previous) = previous.as_ref() {
                    alias_metadata.insert(
                        "replaced_blob_id".to_string(),
                        json!(previous.id.to_string()),
                    );
                }
                batch.put(
                    &self.inner.activity_table,
                    BlobActivityKey {
                        namespace: self.inner.config.namespace().to_string(),
                        activity_id: self.next_activity_id(),
                    }
                    .encode()?,
                    encode_row(&BlobActivityRow {
                        timestamp: published_at,
                        kind: BlobActivityKind::AliasUpserted,
                        blob_id: Some(blob_id),
                        alias: Some(alias_ref.clone()),
                        object_key: Some(object_key.clone()),
                        metadata: alias_metadata,
                    })?,
                );
            }

            if let Some(previous) = previous.as_ref() {
                let previous_catalog_key = BlobCatalogKey {
                    namespace: self.inner.config.namespace().to_string(),
                    blob_id: previous.id,
                };
                batch.delete(&self.inner.catalog_table, previous_catalog_key.encode()?);
                read_set.add(
                    &self.inner.catalog_table,
                    previous_catalog_key.encode()?,
                    snapshot.sequence(),
                );

                if previous.object_key != object_key {
                    let previous_gc_key = BlobObjectGcKey {
                        namespace: self.inner.config.namespace().to_string(),
                        object_key: previous.object_key.clone(),
                    };
                    let previous_gc_row = read_gc_row_at(
                        &snapshot,
                        &self.inner.gc_table,
                        self.inner.config.namespace(),
                        &previous.object_key,
                    )
                    .await?;

                    let updated_gc_row = updated_gc_row_after_unpublish(
                        previous,
                        previous_gc_row,
                        previous_other_reference.as_ref(),
                        published_at,
                    );
                    batch.put(
                        &self.inner.gc_table,
                        previous_gc_key.encode()?,
                        encode_row(&updated_gc_row)?,
                    );
                    read_set.add(
                        &self.inner.gc_table,
                        previous_gc_key.encode()?,
                        snapshot.sequence(),
                    );
                }

                let mut delete_metadata = BTreeMap::new();
                delete_metadata.insert("reason".to_string(), json!("alias_replaced"));
                delete_metadata.insert(
                    "replacement_blob_id".to_string(),
                    json!(blob_id.to_string()),
                );
                batch.put(
                    &self.inner.activity_table,
                    BlobActivityKey {
                        namespace: self.inner.config.namespace().to_string(),
                        activity_id: self.next_activity_id(),
                    }
                    .encode()?,
                    encode_row(&BlobActivityRow {
                        timestamp: published_at,
                        kind: BlobActivityKind::BlobDeleted,
                        blob_id: Some(previous.id),
                        alias: previous.alias.clone(),
                        object_key: Some(previous.object_key.clone()),
                        metadata: delete_metadata,
                    })?,
                );
            }

            match self
                .inner
                .db
                .commit(batch, CommitOptions::default().with_read_set(read_set))
                .await
            {
                Ok(_) => return Ok(()),
                Err(CommitError::Conflict) => continue,
                Err(error) => return Err(map_commit_error(error)),
            }
        }

        Err(BlobError::Storage(StorageError::timeout(
            "blob metadata publish conflicted too many times",
        )))
    }

    async fn delete_with_retry(&self, target: BlobLocator) -> Result<(), BlobError> {
        for _attempt in 0..MAX_METADATA_CONFLICT_RETRIES {
            let snapshot = self.inner.db.snapshot().await;
            let resolved = self.resolve_at_snapshot(&snapshot, target.clone()).await?;
            let Some(resolved) = resolved else {
                return match target {
                    BlobLocator::Id(id) => Err(BlobError::NotFound { id }),
                    BlobLocator::Alias(alias) => Err(BlobError::AliasNotFound { alias }),
                };
            };

            let deleted_at = now_timestamp();
            let other_reference = self
                .find_other_reference(
                    &snapshot,
                    &resolved.metadata.object_key,
                    resolved.metadata.id,
                )
                .await?;
            let existing_gc_row = read_gc_row_at(
                &snapshot,
                &self.inner.gc_table,
                self.inner.config.namespace(),
                &resolved.metadata.object_key,
            )
            .await?;

            let mut batch = self.inner.db.write_batch();
            let mut read_set = ReadSet::default();

            let catalog_key = BlobCatalogKey {
                namespace: self.inner.config.namespace().to_string(),
                blob_id: resolved.metadata.id,
            };
            batch.delete(&self.inner.catalog_table, catalog_key.encode()?);
            read_set.add(
                &self.inner.catalog_table,
                catalog_key.encode()?,
                snapshot.sequence(),
            );

            if let Some(alias) = resolved.metadata.alias.as_ref() {
                let alias_key = BlobAliasKey {
                    namespace: self.inner.config.namespace().to_string(),
                    alias: alias.clone(),
                };
                batch.delete(&self.inner.alias_table, alias_key.encode()?);
                read_set.add(
                    &self.inner.alias_table,
                    alias_key.encode()?,
                    snapshot.sequence(),
                );

                batch.put(
                    &self.inner.activity_table,
                    BlobActivityKey {
                        namespace: self.inner.config.namespace().to_string(),
                        activity_id: self.next_activity_id(),
                    }
                    .encode()?,
                    encode_row(&BlobActivityRow {
                        timestamp: deleted_at,
                        kind: BlobActivityKind::AliasRemoved,
                        blob_id: Some(resolved.metadata.id),
                        alias: Some(alias.clone()),
                        object_key: Some(resolved.metadata.object_key.clone()),
                        metadata: BTreeMap::new(),
                    })?,
                );
            }

            let gc_key = BlobObjectGcKey {
                namespace: self.inner.config.namespace().to_string(),
                object_key: resolved.metadata.object_key.clone(),
            };
            batch.put(
                &self.inner.gc_table,
                gc_key.encode()?,
                encode_row(&updated_gc_row_after_unpublish(
                    &resolved.metadata,
                    existing_gc_row,
                    other_reference.as_ref(),
                    deleted_at,
                ))?,
            );
            read_set.add(&self.inner.gc_table, gc_key.encode()?, snapshot.sequence());

            let mut delete_metadata = BTreeMap::new();
            delete_metadata.insert("reason".to_string(), json!("explicit_delete"));
            batch.put(
                &self.inner.activity_table,
                BlobActivityKey {
                    namespace: self.inner.config.namespace().to_string(),
                    activity_id: self.next_activity_id(),
                }
                .encode()?,
                encode_row(&BlobActivityRow {
                    timestamp: deleted_at,
                    kind: BlobActivityKind::BlobDeleted,
                    blob_id: Some(resolved.metadata.id),
                    alias: resolved.metadata.alias.clone(),
                    object_key: Some(resolved.metadata.object_key.clone()),
                    metadata: delete_metadata,
                })?,
            );

            match self
                .inner
                .db
                .commit(batch, CommitOptions::default().with_read_set(read_set))
                .await
            {
                Ok(_) => return Ok(()),
                Err(CommitError::Conflict) => continue,
                Err(error) => return Err(map_commit_error(error)),
            }
        }

        Err(BlobError::Storage(StorageError::timeout(
            "blob metadata delete conflicted too many times",
        )))
    }

    #[cfg(test)]
    fn arm_failpoint(&self, failpoint: TestFailpoint) {
        self.inner.failpoints.lock().push(failpoint);
    }

    #[cfg(test)]
    fn trigger_failpoint(&self, failpoint: TestFailpoint) -> Result<(), BlobError> {
        let mut armed = self.inner.failpoints.lock();
        if let Some(index) = armed.iter().position(|candidate| *candidate == failpoint) {
            armed.remove(index);
            return Err(BlobError::Storage(StorageError::timeout(
                INJECTED_FAILPOINT_MESSAGE,
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl BlobCollection for TerracedbBlobCollection {
    fn config(&self) -> &BlobCollectionConfig {
        &self.inner.config
    }

    async fn put(&self, input: BlobWrite) -> Result<BlobHandle, BlobError> {
        let alias = input.alias.clone();
        let content_type = input.content_type.clone();
        let tags = input.tags.clone();
        let metadata_fields = input.metadata.clone();
        let bytes = collect_write_bytes(input.data).await?;
        let digest = compute_stub_digest(&bytes);
        let blob_id = self.next_blob_id();
        let object_key = self.inner.layout.object_key(&format!("stub/{blob_id}"));

        let info = self
            .inner
            .blob_store
            .put(
                &object_key,
                stream_from_bytes(bytes.clone()),
                BlobPutOptions {
                    content_type: content_type.clone(),
                    expected_size: Some(bytes.len() as u64),
                },
            )
            .await?;

        if info.key != object_key {
            return Err(corruption(format!(
                "blob store acknowledged upload for {} with mismatched key {}",
                object_key, info.key
            )));
        }
        if info.size_bytes != bytes.len() as u64 {
            return Err(corruption(format!(
                "blob store acknowledged upload for {} with mismatched size {} (expected {})",
                object_key,
                info.size_bytes,
                bytes.len()
            )));
        }

        #[cfg(test)]
        self.trigger_failpoint(TestFailpoint::AfterUploadBeforePublish)?;

        self.publish_with_retry(
            blob_id,
            object_key.clone(),
            alias,
            digest.clone(),
            bytes.len() as u64,
            content_type,
            tags,
            metadata_fields,
        )
        .await?;

        #[cfg(test)]
        self.trigger_failpoint(TestFailpoint::AfterPublishBeforeAck)?;

        Ok(BlobHandle {
            id: blob_id,
            object_key,
            digest,
            size_bytes: bytes.len() as u64,
        })
    }

    async fn stat(&self, target: BlobLocator) -> Result<Option<BlobMetadata>, BlobError> {
        let snapshot = self.inner.db.snapshot().await;
        Ok(self
            .resolve_at_snapshot(&snapshot, target)
            .await?
            .map(|resolved| resolved.metadata))
    }

    async fn get(
        &self,
        target: BlobLocator,
        opts: BlobReadOptions,
    ) -> Result<BlobReadResult, BlobError> {
        let snapshot = self.inner.db.snapshot().await;
        let resolved = self.resolve_at_snapshot(&snapshot, target.clone()).await?;
        let Some(resolved) = resolved else {
            return match target {
                BlobLocator::Id(id) => Err(BlobError::NotFound { id }),
                BlobLocator::Alias(alias) => Err(BlobError::AliasNotFound { alias }),
            };
        };

        let stream = match self
            .inner
            .blob_store
            .get(
                &resolved.metadata.object_key,
                crate::BlobGetOptions { range: opts.range },
            )
            .await
        {
            Ok(stream) => map_store_stream(stream),
            Err(BlobStoreError::NotFound { .. }) => {
                return Err(BlobError::MissingObject {
                    object_key: resolved.metadata.object_key.clone(),
                });
            }
            Err(error) => return Err(BlobError::Store(error)),
        };

        Ok(BlobReadResult {
            metadata: resolved.metadata,
            range: opts.range,
            data: stream,
        })
    }

    async fn delete(&self, target: BlobLocator) -> Result<(), BlobError> {
        self.delete_with_retry(target).await
    }

    async fn search(&self, query: crate::BlobQuery) -> Result<BlobSearchStream, BlobError> {
        let snapshot = self.inner.db.snapshot().await;
        let prefix = BlobCatalogKey::namespace_prefix(self.inner.config.namespace())?;
        let mut rows = snapshot
            .scan_prefix(&self.inner.catalog_table, prefix, ScanOptions::default())
            .await
            .map_err(map_read_error)?;

        let mut matches = Vec::new();
        while let Some((key, value)) = rows.next().await {
            let catalog_key = decode_catalog_key(&key)?;
            let catalog_row = decode_row::<BlobCatalogRow>(value, BLOB_CATALOG_TABLE_NAME)?;
            let metadata = catalog_row_to_metadata(
                self.inner.config.namespace(),
                catalog_key.blob_id,
                catalog_row,
            );
            if !matches_query(&metadata, &query) {
                continue;
            }

            matches.push(Ok(BlobSearchRow {
                snippet: query
                    .terms
                    .first()
                    .filter(|term| searchable_text(&metadata).contains(&term.to_lowercase()))
                    .map(|term| format!("matched:{term}")),
                metadata,
            }));

            if let Some(limit) = query.limit
                && matches.len() >= limit
            {
                break;
            }
        }

        Ok(Box::pin(futures::stream::iter(matches)))
    }

    async fn activity_since(
        &self,
        cursor: LogCursor,
        opts: BlobActivityOptions,
    ) -> Result<BlobActivityStream, BlobError> {
        let source = if opts.durable {
            self.inner
                .db
                .scan_durable_since(&self.inner.activity_table, cursor, ScanOptions::default())
                .await
                .map_err(map_change_feed_error)?
        } else {
            self.inner
                .db
                .scan_since(&self.inner.activity_table, cursor, ScanOptions::default())
                .await
                .map_err(map_change_feed_error)?
        };

        let namespace = self.inner.config.namespace().to_string();
        let stream = source.filter_map(move |result| {
            let namespace = namespace.clone();
            let output = match result {
                Ok(change) => match decode_activity_change(&namespace, change) {
                    Ok(Some(entry)) => Some(Ok(entry)),
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                },
                Err(error) => Some(Err(map_change_feed_storage_error(error))),
            };
            async move { output }
        });

        if let Some(limit) = opts.limit {
            Ok(Box::pin(stream.take(limit)))
        } else {
            Ok(Box::pin(stream))
        }
    }

    fn subscribe_activity(&self, opts: BlobActivityOptions) -> BlobActivityReceiver {
        if opts.durable {
            BlobActivityReceiver::from_watermark(
                self.inner.db.subscribe_durable(&self.inner.activity_table),
            )
        } else {
            BlobActivityReceiver::from_watermark(
                self.inner.db.subscribe(&self.inner.activity_table),
            )
        }
    }
}

fn seeded_u64(salt: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    nanos.rotate_left(17) ^ salt ^ (std::process::id() as u64).rotate_left(9)
}

fn now_timestamp() -> Timestamp {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u64::MAX as u128) as u64;
    Timestamp::new(micros)
}

async fn ensure_bootstrap_table(
    db: &Db,
    descriptor: &crate::FrozenTableDescriptor,
    create_if_missing: bool,
) -> Result<Table, BlobError> {
    if let Some(table) = db.try_table(descriptor.name) {
        return Ok(table);
    }

    if !create_if_missing {
        return Err(BlobError::Storage(StorageError::not_found(format!(
            "missing terracedb-bricks bootstrap table {}",
            descriptor.name
        ))));
    }

    match db.create_table(descriptor.table_config()).await {
        Ok(table) => Ok(table),
        Err(CreateTableError::AlreadyExists(_)) => db.try_table(descriptor.name).ok_or_else(|| {
            BlobError::Storage(StorageError::corruption(format!(
                "terracedb-bricks bootstrap table {} raced into existence but cannot be opened",
                descriptor.name
            )))
        }),
        Err(error) => Err(map_create_table_error(error)),
    }
}

async fn read_catalog_row_at(
    snapshot: &Snapshot,
    table: &Table,
    namespace: &str,
    blob_id: BlobId,
) -> Result<Option<BlobCatalogRow>, BlobError> {
    let key = BlobCatalogKey {
        namespace: namespace.to_string(),
        blob_id,
    }
    .encode()?;
    let value = snapshot.read(table, key).await.map_err(map_read_error)?;
    value
        .map(|value| decode_row(value, BLOB_CATALOG_TABLE_NAME))
        .transpose()
}

async fn read_alias_row_at(
    snapshot: &Snapshot,
    table: &Table,
    namespace: &str,
    alias: &BlobAlias,
) -> Result<Option<BlobAliasRow>, BlobError> {
    let key = BlobAliasKey {
        namespace: namespace.to_string(),
        alias: alias.clone(),
    }
    .encode()?;
    let value = snapshot.read(table, key).await.map_err(map_read_error)?;
    value
        .map(|value| decode_row(value, BLOB_ALIAS_TABLE_NAME))
        .transpose()
}

async fn read_gc_row_at(
    snapshot: &Snapshot,
    table: &Table,
    namespace: &str,
    object_key: &str,
) -> Result<Option<BlobObjectGcRow>, BlobError> {
    let key = BlobObjectGcKey {
        namespace: namespace.to_string(),
        object_key: object_key.to_string(),
    }
    .encode()?;
    let value = snapshot.read(table, key).await.map_err(map_read_error)?;
    value
        .map(|value| decode_row(value, BLOB_OBJECT_GC_TABLE_NAME))
        .transpose()
}

fn encode_row<T: Serialize>(row: &T) -> Result<Value, BlobError> {
    serde_json::to_vec(row)
        .map(Value::Bytes)
        .map_err(|error| corruption(format!("failed to encode terracedb-bricks row: {error}")))
}

fn decode_row<T: DeserializeOwned>(value: Value, table_name: &str) -> Result<T, BlobError> {
    match value {
        Value::Bytes(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
            corruption(format!(
                "failed to decode terracedb-bricks row in {}: {error}",
                table_name
            ))
        }),
        Value::Record(_) => Err(corruption(format!(
            "terracedb-bricks row table {} stored a columnar record unexpectedly",
            table_name
        ))),
    }
}

fn decode_catalog_key(bytes: &[u8]) -> Result<BlobCatalogKey, BlobError> {
    BlobCatalogKey::decode(bytes).map_err(|error| {
        corruption(format!(
            "invalid terracedb-bricks catalog key in {}: {error}",
            BLOB_CATALOG_TABLE_NAME
        ))
    })
}

fn decode_activity_key(bytes: &[u8]) -> Result<BlobActivityKey, BlobError> {
    BlobActivityKey::decode(bytes).map_err(|error| {
        corruption(format!(
            "invalid terracedb-bricks activity key in {}: {error}",
            BLOB_ACTIVITY_TABLE_NAME
        ))
    })
}

fn catalog_row_to_metadata(namespace: &str, blob_id: BlobId, row: BlobCatalogRow) -> BlobMetadata {
    BlobMetadata {
        id: blob_id,
        namespace: namespace.to_string(),
        alias: row.alias,
        object_key: row.object_key,
        digest: row.digest,
        size_bytes: row.size_bytes,
        content_type: row.content_type,
        tags: row.tags,
        metadata: row.metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
        index_state: row.index_state,
    }
}

fn updated_gc_row_after_unpublish(
    metadata: &BlobMetadata,
    existing: Option<BlobObjectGcRow>,
    still_referenced: Option<&BlobMetadata>,
    unpublished_at: Timestamp,
) -> BlobObjectGcRow {
    BlobObjectGcRow {
        object_key: metadata.object_key.clone(),
        digest: metadata.digest.clone(),
        size_bytes: metadata.size_bytes,
        content_type: metadata.content_type.clone(),
        first_published_at: existing
            .as_ref()
            .map(|row| row.first_published_at)
            .unwrap_or(metadata.created_at),
        last_published_at: existing
            .as_ref()
            .map(|row| row.last_published_at)
            .unwrap_or(metadata.updated_at),
        current_blob_id: still_referenced.map(|metadata| metadata.id),
        pending_delete_at: still_referenced
            .map(|_| None)
            .unwrap_or(Some(unpublished_at)),
    }
}

fn compute_stub_digest(bytes: &[u8]) -> String {
    let checksum = bytes.iter().fold(0_u64, |acc, byte| {
        acc.wrapping_mul(1099511628211).wrapping_add(*byte as u64)
    });
    format!("stub:{checksum:016x}:{:016x}", bytes.len())
}

async fn collect_write_bytes(data: BlobWriteData) -> Result<Vec<u8>, BlobStoreError> {
    match data {
        BlobWriteData::Bytes(bytes) => Ok(bytes.to_vec()),
        BlobWriteData::Stream(mut stream) => {
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            Ok(bytes)
        }
    }
}

fn map_store_stream(stream: BlobStoreByteStream) -> crate::collection::BlobByteStream {
    Box::pin(stream.map(|result| result.map_err(BlobError::from)))
}

fn map_read_error(error: ReadError) -> BlobError {
    match error {
        ReadError::Storage(error) => BlobError::Storage(error),
        ReadError::SnapshotTooOld(error) => BlobError::SnapshotTooOld(error),
        ReadError::Unimplemented(operation) => BlobError::Storage(StorageError::unsupported(
            format!("terracedb read path is unimplemented: {operation}"),
        )),
    }
}

fn map_change_feed_error(error: ChangeFeedError) -> BlobError {
    match error {
        ChangeFeedError::SnapshotTooOld(error) => BlobError::SnapshotTooOld(error),
        ChangeFeedError::Storage(error) => BlobError::Storage(error),
    }
}

fn map_change_feed_storage_error(error: StorageError) -> BlobError {
    if error.kind() == StorageErrorKind::NotFound {
        BlobError::Storage(StorageError::not_found(error.message().to_string()))
    } else {
        BlobError::Storage(error)
    }
}

fn map_create_table_error(error: CreateTableError) -> BlobError {
    match error {
        CreateTableError::Storage(error) => BlobError::Storage(error),
        CreateTableError::AlreadyExists(name) => {
            BlobError::Storage(StorageError::unsupported(format!(
                "terracedb-bricks bootstrap table {name} already exists with a mismatched configuration"
            )))
        }
        CreateTableError::InvalidConfig(message) => BlobError::Storage(StorageError::unsupported(
            format!("invalid terracedb-bricks bootstrap table configuration: {message}"),
        )),
        CreateTableError::Unimplemented(operation) => {
            BlobError::Storage(StorageError::unsupported(format!(
                "terracedb create_table is unimplemented: {operation}"
            )))
        }
    }
}

fn map_commit_error(error: CommitError) -> BlobError {
    match error {
        CommitError::Conflict => BlobError::Storage(StorageError::timeout(
            "terracedb-bricks metadata batch conflicted",
        )),
        CommitError::EmptyBatch => BlobError::Storage(StorageError::unsupported(
            "terracedb-bricks attempted to commit an empty batch",
        )),
        CommitError::Storage(error) => BlobError::Storage(error),
        CommitError::Unimplemented(operation) => BlobError::Storage(StorageError::unsupported(
            format!("terracedb commit path is unimplemented: {operation}"),
        )),
    }
}

fn corruption(message: impl Into<String>) -> BlobError {
    BlobError::Storage(StorageError::corruption(message))
}

fn decode_activity_change(
    namespace: &str,
    change: terracedb::ChangeEntry,
) -> Result<Option<BlobActivityEntry>, BlobError> {
    if change.kind != ChangeKind::Put {
        return Err(corruption(format!(
            "terracedb-bricks activity table observed unexpected {:?} operation at {:?}",
            change.kind, change.cursor
        )));
    }

    let activity_key = decode_activity_key(&change.key)?;
    if activity_key.namespace != namespace {
        return Ok(None);
    }

    let Some(value) = change.value else {
        return Err(corruption(format!(
            "terracedb-bricks activity table observed a tombstone at {:?}",
            change.cursor
        )));
    };
    let row = decode_row::<BlobActivityRow>(value, BLOB_ACTIVITY_TABLE_NAME)?;

    Ok(Some(BlobActivityEntry {
        namespace: namespace.to_string(),
        activity_id: activity_key.activity_id,
        sequence: change.sequence,
        cursor: change.cursor,
        timestamp: row.timestamp,
        kind: row.kind,
        blob_id: row.blob_id,
        alias: row.alias,
        object_key: row.object_key,
        metadata: row.metadata,
    }))
}

fn matches_query(metadata: &BlobMetadata, query: &crate::BlobQuery) -> bool {
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use futures::TryStreamExt;

    use terracedb::{
        Db, DbConfig, DbDependencies, S3Location, StorageConfig, StubClock, StubFileSystem,
        StubObjectStore, StubRng, TieredDurabilityMode, TieredStorageConfig,
    };

    use crate::{BlobCollection as _, BlobError, BlobWrite};

    use super::{TerracedbBlobCollection, TestFailpoint};

    fn test_db_config(path: &str) -> DbConfig {
        DbConfig {
            storage: StorageConfig::Tiered(TieredStorageConfig {
                ssd: terracedb::SsdConfig {
                    path: path.to_string(),
                },
                s3: S3Location {
                    bucket: "terracedb-bricks-test".to_string(),
                    prefix: "dev".to_string(),
                },
                max_local_bytes: 1024 * 1024,
                durability: TieredDurabilityMode::GroupCommit,
                local_retention: terracedb::TieredLocalRetentionMode::Offload,
            }),
            hybrid_read: Default::default(),
            scheduler: None,
        }
    }

    struct TestHarness {
        path: &'static str,
        file_system: Arc<StubFileSystem>,
        object_store: Arc<StubObjectStore>,
        clock: Arc<StubClock>,
        blob_store: Arc<InMemoryBlobStore>,
    }

    impl TestHarness {
        fn new(path: &'static str) -> Self {
            Self {
                path,
                file_system: Arc::new(StubFileSystem::default()),
                object_store: Arc::new(StubObjectStore::default()),
                clock: Arc::new(StubClock::default()),
                blob_store: Arc::new(InMemoryBlobStore::new()),
            }
        }

        async fn open_collection(&self, create_if_missing: bool) -> TerracedbBlobCollection {
            let db = Db::open(
                test_db_config(self.path),
                DbDependencies::new(
                    self.file_system.clone(),
                    self.object_store.clone(),
                    self.clock.clone(),
                    Arc::new(StubRng::seeded(44)),
                ),
            )
            .await
            .expect("open db");
            TerracedbBlobCollection::open(
                db,
                crate::BlobCollectionConfig::new("docs")
                    .expect("namespace")
                    .with_create_if_missing(create_if_missing),
                self.blob_store.clone(),
            )
            .await
            .expect("open bricks collection")
        }
    }

    use crate::{BlobAlias, BlobStore, InMemoryBlobStore};

    #[tokio::test]
    async fn upload_complete_before_publish_failure_leaves_no_visible_metadata() {
        let harness = TestHarness::new("/bricks-fail-after-upload");
        let collection = harness.open_collection(true).await;
        collection.arm_failpoint(TestFailpoint::AfterUploadBeforePublish);

        let error = collection
            .put(BlobWrite {
                alias: Some(BlobAlias::new("docs/latest").expect("alias")),
                data: crate::BlobWriteData::bytes("hello"),
                content_type: Some("text/plain".to_string()),
                tags: BTreeMap::new(),
                metadata: BTreeMap::new(),
            })
            .await
            .err()
            .expect("failpoint should fail");
        assert!(matches!(error, BlobError::Storage(_)));

        let reopened = harness.open_collection(false).await;
        let results = reopened
            .search(crate::BlobQuery::default())
            .await
            .expect("search")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect search");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn metadata_published_before_ack_failure_reopens_cleanly() {
        let harness = TestHarness::new("/bricks-fail-after-publish");
        let collection = harness.open_collection(true).await;
        let alias = BlobAlias::new("docs/latest").expect("alias");
        collection.arm_failpoint(TestFailpoint::AfterPublishBeforeAck);

        let error = collection
            .put(BlobWrite {
                alias: Some(alias.clone()),
                data: crate::BlobWriteData::bytes("hello"),
                content_type: Some("text/plain".to_string()),
                tags: BTreeMap::new(),
                metadata: BTreeMap::new(),
            })
            .await
            .err()
            .expect("failpoint should fail");
        assert!(matches!(error, BlobError::Storage(_)));

        let reopened = harness.open_collection(false).await;
        let metadata = reopened
            .stat(crate::BlobLocator::Alias(alias))
            .await
            .expect("stat after reopen")
            .expect("metadata should survive");
        assert_eq!(metadata.size_bytes, 5);
    }

    #[tokio::test]
    async fn delete_only_removes_metadata_and_leaves_object_for_later_gc() {
        let harness = TestHarness::new("/bricks-delete-metadata-first");
        let collection = harness.open_collection(true).await;
        let alias = BlobAlias::new("docs/latest").expect("alias");

        let handle = collection
            .put(BlobWrite {
                alias: Some(alias.clone()),
                data: crate::BlobWriteData::bytes("hello"),
                content_type: Some("text/plain".to_string()),
                tags: BTreeMap::new(),
                metadata: BTreeMap::new(),
            })
            .await
            .expect("put");
        collection
            .delete(crate::BlobLocator::Alias(alias.clone()))
            .await
            .expect("delete metadata");

        let reopened = harness.open_collection(false).await;
        assert!(
            reopened
                .stat(crate::BlobLocator::Alias(alias))
                .await
                .expect("stat alias")
                .is_none()
        );
        assert!(
            harness
                .blob_store
                .stat(&handle.object_key)
                .await
                .expect("stat object")
                .is_some()
        );
    }
}
