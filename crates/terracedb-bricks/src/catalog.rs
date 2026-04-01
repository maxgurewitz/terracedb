use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;

use terracedb::{
    ChangeFeedError, ChangeKind, CommitError, CommitOptions, CreateTableError, Db, LogCursor,
    ReadError, ReadSet, ScanOptions, Snapshot, StorageError, StorageErrorKind, Table, Timestamp,
    Value,
};
use terracedb_projections::{
    ProjectionContext, ProjectionError, ProjectionHandler, ProjectionHandlerError,
    ProjectionRuntime, ProjectionSequenceRun, ProjectionTransaction, RecomputeStrategy,
    SingleSourceProjection,
};

#[cfg(test)]
use parking_lot::Mutex;

use crate::{
    BLOB_ACTIVITY_TABLE_NAME, BLOB_ALIAS_TABLE_NAME, BLOB_CATALOG_TABLE_NAME,
    BLOB_OBJECT_GC_TABLE_NAME, BLOB_TERM_INDEX_TABLE_NAME, BLOB_TEXT_CHUNK_TABLE_NAME,
    BlobActivityEntry, BlobActivityId, BlobActivityKey, BlobActivityKind, BlobActivityOptions,
    BlobActivityReceiver, BlobActivityStream, BlobAlias, BlobAliasKey, BlobCatalogKey,
    BlobCollection, BlobCollectionConfig, BlobError, BlobExtractedTextQuery, BlobGcOptions,
    BlobGcResult, BlobHandle, BlobId, BlobIndexState, BlobLocator, BlobMetadata, BlobObjectGcKey,
    BlobObjectInfo, BlobObjectLayout, BlobPutOptions, BlobQuery, BlobReadOptions, BlobReadResult,
    BlobSearchRow, BlobSearchStream, BlobStore, BlobStoreByteStream, BlobStoreError,
    BlobTermIndexKey, BlobTextChunkKey, BlobTextExtractionConfig, BlobWrite, BlobWriteData,
    JsonValue, frozen_table, frozen_table_descriptors, upload_blob_bytes,
};

const MAX_METADATA_CONFLICT_RETRIES: usize = 16;
const METADATA_INDEX_EXTRACTOR: &str = "metadata";
const ALL_BLOBS_TERM: &str = "__all__";

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
    #[serde(default = "default_gc_row_published_once")]
    published_once: bool,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BlobTextChunkRow {
    text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BlobTermIndexRow {
    source: String,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TestFailpoint {
    AfterUploadBeforePublish,
    AfterPublishBeforeAck,
    AfterGcDeleteBeforeCommit,
}

#[derive(Clone, Debug)]
struct ResolvedBlob {
    metadata: BlobMetadata,
}

#[derive(Clone)]
struct BlobSearchIndexProjection {
    namespace: String,
    catalog_table: Table,
    text_chunk_table: Table,
    term_index_table: Table,
    blob_store: Arc<dyn BlobStore>,
    extraction: Option<BlobTextExtractionConfig>,
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
                    .filter(|row| row.published_once)
                    .map(|row| row.first_published_at)
                    .unwrap_or(published_at),
                last_published_at: published_at,
                current_blob_id: Some(blob_id),
                pending_delete_at: None,
                published_once: true,
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

    async fn record_orphan_candidate_with_retry(
        &self,
        object: &BlobObjectInfo,
        digest: &str,
    ) -> Result<(), BlobError> {
        for _attempt in 0..MAX_METADATA_CONFLICT_RETRIES {
            let snapshot = self.inner.db.snapshot().await;
            let existing = read_gc_row_at(
                &snapshot,
                &self.inner.gc_table,
                self.inner.config.namespace(),
                &object.key,
            )
            .await?;
            if existing.is_some() {
                return Ok(());
            }

            let observed_at = now_timestamp();
            let gc_key = BlobObjectGcKey {
                namespace: self.inner.config.namespace().to_string(),
                object_key: object.key.clone(),
            };
            let gc_row = BlobObjectGcRow {
                object_key: object.key.clone(),
                digest: digest.to_string(),
                size_bytes: object.size_bytes,
                content_type: object.content_type.clone(),
                first_published_at: observed_at,
                last_published_at: observed_at,
                current_blob_id: None,
                pending_delete_at: Some(observed_at),
                published_once: false,
            };

            let mut batch = self.inner.db.write_batch();
            let mut read_set = ReadSet::default();
            batch.put(&self.inner.gc_table, gc_key.encode()?, encode_row(&gc_row)?);
            read_set.add(&self.inner.gc_table, gc_key.encode()?, snapshot.sequence());

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
            "blob orphan-gc bookkeeping conflicted too many times",
        )))
    }

    async fn collect_live_object_keys(
        &self,
        snapshot: &Snapshot,
    ) -> Result<BTreeSet<String>, BlobError> {
        let prefix = BlobCatalogKey::namespace_prefix(self.inner.config.namespace())?;
        let mut rows = snapshot
            .scan_prefix(&self.inner.catalog_table, prefix, ScanOptions::default())
            .await
            .map_err(map_read_error)?;

        let mut keys = BTreeSet::new();
        while let Some((_key, value)) = rows.next().await {
            let row = decode_row::<BlobCatalogRow>(value, BLOB_CATALOG_TABLE_NAME)?;
            keys.insert(row.object_key);
        }

        Ok(keys)
    }

    async fn collect_gc_rows(
        &self,
        snapshot: &Snapshot,
    ) -> Result<Vec<BlobObjectGcRow>, BlobError> {
        let prefix = BlobObjectGcKey::namespace_prefix(self.inner.config.namespace())?;
        let mut rows = snapshot
            .scan_prefix(&self.inner.gc_table, prefix, ScanOptions::default())
            .await
            .map_err(map_read_error)?;

        let mut gc_rows = Vec::new();
        while let Some((_key, value)) = rows.next().await {
            gc_rows.push(decode_row::<BlobObjectGcRow>(
                value,
                BLOB_OBJECT_GC_TABLE_NAME,
            )?);
        }

        Ok(gc_rows)
    }

    async fn sweep_gc_row_with_retry(&self, object_key: &str) -> Result<bool, BlobError> {
        for _attempt in 0..MAX_METADATA_CONFLICT_RETRIES {
            let snapshot = self.inner.db.snapshot().await;
            if self
                .find_blob_reference(&snapshot, object_key)
                .await?
                .is_some()
            {
                return Ok(false);
            }

            let Some(row) = read_gc_row_at(
                &snapshot,
                &self.inner.gc_table,
                self.inner.config.namespace(),
                object_key,
            )
            .await?
            else {
                return Ok(false);
            };
            if row.pending_delete_at.is_none() {
                return Ok(false);
            }

            match self.inner.blob_store.delete(object_key).await {
                Ok(()) | Err(BlobStoreError::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }

            #[cfg(test)]
            self.trigger_failpoint(TestFailpoint::AfterGcDeleteBeforeCommit)?;

            let gc_key = BlobObjectGcKey {
                namespace: self.inner.config.namespace().to_string(),
                object_key: object_key.to_string(),
            };
            let mut batch = self.inner.db.write_batch();
            let mut read_set = ReadSet::default();
            batch.delete(&self.inner.gc_table, gc_key.encode()?);
            read_set.add(&self.inner.gc_table, gc_key.encode()?, snapshot.sequence());

            match self
                .inner
                .db
                .commit(batch, CommitOptions::default().with_read_set(read_set))
                .await
            {
                Ok(_) => return Ok(true),
                Err(CommitError::Conflict) => continue,
                Err(error) => return Err(map_commit_error(error)),
            }
        }

        Err(BlobError::Storage(StorageError::timeout(
            "blob object gc conflicted too many times",
        )))
    }

    async fn find_blob_reference(
        &self,
        snapshot: &Snapshot,
        object_key: &str,
    ) -> Result<Option<BlobMetadata>, BlobError> {
        let prefix = BlobCatalogKey::namespace_prefix(self.inner.config.namespace())?;
        let mut rows = snapshot
            .scan_prefix(&self.inner.catalog_table, prefix, ScanOptions::default())
            .await
            .map_err(map_read_error)?;

        while let Some((key, value)) = rows.next().await {
            let catalog_key = decode_catalog_key(&key)?;
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
        let blob_id = self.next_blob_id();
        let receipt = upload_blob_bytes(
            self.inner.blob_store.as_ref(),
            &self.inner.layout,
            Bytes::from(bytes),
            BlobPutOptions {
                content_type: content_type.clone(),
                expected_size: None,
            },
        )
        .await?;
        self.record_orphan_candidate_with_retry(&receipt.object, &receipt.digest)
            .await?;

        #[cfg(test)]
        self.trigger_failpoint(TestFailpoint::AfterUploadBeforePublish)?;

        self.publish_with_retry(
            blob_id,
            receipt.object.key.clone(),
            alias,
            receipt.digest.clone(),
            receipt.object.size_bytes,
            content_type,
            tags,
            metadata_fields,
        )
        .await?;

        #[cfg(test)]
        self.trigger_failpoint(TestFailpoint::AfterPublishBeforeAck)?;

        Ok(BlobHandle {
            id: blob_id,
            object_key: receipt.object.key,
            digest: receipt.digest,
            size_bytes: receipt.object.size_bytes,
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

    async fn collect_garbage(&self, opts: BlobGcOptions) -> Result<BlobGcResult, BlobError> {
        let stats = self.inner.db.table_stats(&self.inner.catalog_table).await;
        let snapshots_pin_published_gc = stats.active_snapshot_count > 0;
        let snapshot = self.inner.db.snapshot().await;
        let live = self.collect_live_object_keys(&snapshot).await?;
        let gc_rows = self.collect_gc_rows(&snapshot).await?;
        let tracked_keys = gc_rows
            .iter()
            .map(|row| row.object_key.clone())
            .collect::<BTreeSet<_>>();
        let listed = self
            .inner
            .blob_store
            .list_prefix(&self.inner.layout.object_prefix())
            .await?;
        let now = now_timestamp();

        let mut result = BlobGcResult {
            live_objects: live.len(),
            tracked_objects: gc_rows.len(),
            discovered_orphans: listed
                .iter()
                .filter(|info| !live.contains(&info.key) && !tracked_keys.contains(&info.key))
                .count(),
            ..BlobGcResult::default()
        };
        let mut tracked_candidates = Vec::new();

        for row in gc_rows {
            if live.contains(&row.object_key) {
                result.retained_by_reference += 1;
                continue;
            }
            let Some(pending_delete_at) = row.pending_delete_at else {
                continue;
            };
            if !past_grace(Some(pending_delete_at), now, opts.grace_period) {
                result.retained_by_grace += 1;
                continue;
            }
            if row.published_once && snapshots_pin_published_gc {
                result.retained_by_snapshots += 1;
                continue;
            }
            tracked_candidates.push(row.object_key);
        }

        for object_key in tracked_candidates {
            if opts
                .max_objects
                .is_some_and(|limit| result.deleted_objects >= limit)
            {
                result.retained_by_grace += 1;
                continue;
            }
            if self.sweep_gc_row_with_retry(&object_key).await? {
                result.deleted_objects += 1;
            }
        }

        for info in listed {
            if live.contains(&info.key) || tracked_keys.contains(&info.key) {
                continue;
            }
            if !past_grace(info.last_modified, now, opts.grace_period) {
                result.retained_by_grace += 1;
                continue;
            }
            if opts
                .max_objects
                .is_some_and(|limit| result.deleted_objects >= limit)
            {
                result.retained_by_grace += 1;
                continue;
            }

            match self.inner.blob_store.delete(&info.key).await {
                Ok(()) | Err(BlobStoreError::NotFound { .. }) => {
                    result.deleted_objects += 1;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Ok(result)
    }

    async fn search(&self, query: crate::BlobQuery) -> Result<BlobSearchStream, BlobError> {
        let (text_chunk_table, term_index_table) =
            ensure_search_tables(&self.inner.db, self.inner.config.create_if_missing).await?;
        sync_search_indexes(
            &self.inner.db,
            &self.inner.catalog_table,
            &term_index_table,
            &text_chunk_table,
            self.inner.config.namespace(),
            self.inner.blob_store.clone(),
            self.inner.config.text_extraction().cloned(),
        )
        .await?;
        let rows = query_search_indexes(
            &self.inner.db,
            &self.inner.catalog_table,
            &term_index_table,
            &text_chunk_table,
            self.inner.config.namespace(),
            self.inner.config.text_extraction(),
            &query,
        )
        .await?;
        Ok(Box::pin(futures::stream::iter(
            rows.into_iter().map(Ok::<_, BlobError>),
        )))
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

#[async_trait]
impl ProjectionHandler for BlobSearchIndexProjection {
    async fn apply_with_context(
        &self,
        _run: &ProjectionSequenceRun,
        ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        clear_output_namespace(&self.term_index_table, &self.namespace, tx).await?;
        clear_output_namespace(&self.text_chunk_table, &self.namespace, tx).await?;

        let (catalog_start, catalog_end) =
            namespace_scan_range(&self.namespace).map_err(wrap_handler_blob_error)?;
        let mut rows = ctx
            .scan(
                &self.catalog_table,
                catalog_start,
                catalog_end,
                ScanOptions::default(),
            )
            .await?;

        while let Some((key, value)) = rows.next().await {
            let catalog_key = decode_catalog_key(&key).map_err(wrap_handler_blob_error)?;
            let catalog_row = decode_row::<BlobCatalogRow>(value, BLOB_CATALOG_TABLE_NAME)
                .map_err(wrap_handler_blob_error)?;
            let metadata =
                catalog_row_to_metadata(&self.namespace, catalog_key.blob_id, catalog_row);
            index_blob_metadata(
                &self.term_index_table,
                &self.text_chunk_table,
                self.blob_store.as_ref(),
                self.extraction.as_ref(),
                &metadata,
                tx,
            )
            .await?;
        }

        Ok(())
    }
}

async fn sync_search_indexes(
    db: &Db,
    catalog_table: &Table,
    term_index_table: &Table,
    text_chunk_table: &Table,
    namespace: &str,
    blob_store: Arc<dyn BlobStore>,
    extraction: Option<BlobTextExtractionConfig>,
) -> Result<(), BlobError> {
    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .map_err(map_projection_error)?;
    let projection_name = search_projection_name(namespace, extraction.as_ref());
    let mut handle = runtime
        .start_single_source(
            SingleSourceProjection::new(
                projection_name.clone(),
                catalog_table.clone(),
                BlobSearchIndexProjection {
                    namespace: namespace.to_string(),
                    catalog_table: catalog_table.clone(),
                    text_chunk_table: text_chunk_table.clone(),
                    term_index_table: term_index_table.clone(),
                    blob_store,
                    extraction,
                },
            )
            .with_outputs([term_index_table.clone(), text_chunk_table.clone()])
            .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
        )
        .await
        .map_err(map_projection_error)?;
    let target = db.subscribe_durable(catalog_table).current();
    let wait_result = handle.wait_for_sources([(catalog_table, target)]).await;
    let shutdown_result = handle.shutdown().await;

    match (wait_result, shutdown_result) {
        (Err(_wait_error), Err(shutdown_error)) => Err(map_projection_error(shutdown_error)),
        (Err(wait_error), _) => Err(map_projection_error(wait_error)),
        (Ok(_), Err(shutdown_error)) => Err(map_projection_error(shutdown_error)),
        (Ok(_), Ok(())) => Ok(()),
    }
}

async fn query_search_indexes(
    db: &Db,
    catalog_table: &Table,
    term_index_table: &Table,
    text_chunk_table: &Table,
    namespace: &str,
    extraction: Option<&BlobTextExtractionConfig>,
    query: &BlobQuery,
) -> Result<Vec<BlobSearchRow>, BlobError> {
    let snapshot = db.snapshot().await;
    let extracted_text =
        require_supported_extracted_text_query(extraction, query.extracted_text.as_ref())?;
    let mut candidate_ids: Option<BTreeSet<BlobId>> = None;

    if let Some(alias_prefix) = query.alias_prefix.as_ref() {
        let alias_term_prefix = alias_index_term(alias_prefix);
        let ids = collect_blob_ids_in_term_prefix(
            &snapshot,
            term_index_table,
            namespace,
            &alias_term_prefix,
            METADATA_INDEX_EXTRACTOR,
        )
        .await?;
        intersect_candidates(&mut candidate_ids, ids);
    }

    if let Some(content_type) = query.content_type.as_ref() {
        let ids = collect_blob_ids_for_exact_term(
            &snapshot,
            term_index_table,
            namespace,
            &content_type_index_term(content_type),
            METADATA_INDEX_EXTRACTOR,
        )
        .await?;
        intersect_candidates(&mut candidate_ids, ids);
    }

    for (key, value) in &query.required_tags {
        let ids = collect_blob_ids_for_exact_term(
            &snapshot,
            term_index_table,
            namespace,
            &tag_index_term(key, value),
            METADATA_INDEX_EXTRACTOR,
        )
        .await?;
        intersect_candidates(&mut candidate_ids, ids);
    }

    for (key, value) in &query.required_metadata {
        let ids = collect_blob_ids_for_exact_term(
            &snapshot,
            term_index_table,
            namespace,
            &metadata_exact_index_term(key, value),
            METADATA_INDEX_EXTRACTOR,
        )
        .await?;
        intersect_candidates(&mut candidate_ids, ids);
    }

    for term in normalize_query_terms(&query.terms) {
        let ids = collect_blob_ids_for_exact_term(
            &snapshot,
            term_index_table,
            namespace,
            &term,
            METADATA_INDEX_EXTRACTOR,
        )
        .await?;
        intersect_candidates(&mut candidate_ids, ids);
    }

    if let Some(extracted) = extracted_text {
        for term in normalize_query_terms(&extracted.terms) {
            let ids = collect_blob_ids_for_exact_term(
                &snapshot,
                term_index_table,
                namespace,
                &term,
                &extracted.extractor,
            )
            .await?;
            intersect_candidates(&mut candidate_ids, ids);
        }
    }

    if query.min_size_bytes.is_some() || query.max_size_bytes.is_some() {
        let ids = collect_blob_ids_in_numeric_term_range(
            &snapshot,
            term_index_table,
            namespace,
            "size",
            query.min_size_bytes.unwrap_or(0),
            query.max_size_bytes.unwrap_or(u64::MAX),
            METADATA_INDEX_EXTRACTOR,
        )
        .await?;
        intersect_candidates(&mut candidate_ids, ids);
    }

    if query.created_after.is_some() || query.created_before.is_some() {
        let ids = collect_blob_ids_in_numeric_term_range(
            &snapshot,
            term_index_table,
            namespace,
            "created_at",
            query.created_after.map(Timestamp::get).unwrap_or(0),
            query.created_before.map(Timestamp::get).unwrap_or(u64::MAX),
            METADATA_INDEX_EXTRACTOR,
        )
        .await?;
        intersect_candidates(&mut candidate_ids, ids);
    }

    let candidate_ids = match candidate_ids {
        Some(ids) => ids,
        None => {
            collect_blob_ids_for_exact_term(
                &snapshot,
                term_index_table,
                namespace,
                ALL_BLOBS_TERM,
                METADATA_INDEX_EXTRACTOR,
            )
            .await?
        }
    };

    let metadata_terms = normalize_query_terms(&query.terms);
    let extracted_terms = extracted_text
        .map(|value| normalize_query_terms(&value.terms))
        .unwrap_or_default();

    let mut rows = Vec::new();
    for blob_id in candidate_ids {
        let Some(catalog_row) =
            read_catalog_row_at(&snapshot, catalog_table, namespace, blob_id).await?
        else {
            continue;
        };
        let metadata = catalog_row_to_metadata(namespace, blob_id, catalog_row);
        if !matches_query(&metadata, query) {
            continue;
        }

        let snippet = if let Some(extracted) = extracted_text {
            find_text_snippet(
                &snapshot,
                text_chunk_table,
                namespace,
                blob_id,
                &extracted.extractor,
                &extracted_terms,
            )
            .await?
        } else {
            metadata_terms.first().map(|term| format!("matched:{term}"))
        };

        rows.push(BlobSearchRow { metadata, snippet });
    }

    rows.sort_by(|left, right| {
        left.metadata
            .created_at
            .get()
            .cmp(&right.metadata.created_at.get())
            .then_with(|| left.metadata.id.cmp(&right.metadata.id))
    });
    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

async fn ensure_search_tables(
    db: &Db,
    _create_if_missing: bool,
) -> Result<(Table, Table), BlobError> {
    let text_chunk_descriptor = frozen_table(BLOB_TEXT_CHUNK_TABLE_NAME)
        .expect("blob text chunk table descriptor should exist");
    let term_index_descriptor = frozen_table(BLOB_TERM_INDEX_TABLE_NAME)
        .expect("blob term index table descriptor should exist");
    let text_chunk_table = ensure_reserved_table(db, text_chunk_descriptor, true).await?;
    let term_index_table = ensure_reserved_table(db, term_index_descriptor, true).await?;
    Ok((text_chunk_table, term_index_table))
}

async fn ensure_reserved_table(
    db: &Db,
    descriptor: &crate::FrozenTableDescriptor,
    create_if_missing: bool,
) -> Result<Table, BlobError> {
    if let Some(table) = db.try_table(descriptor.name) {
        return Ok(table);
    }

    if !create_if_missing {
        return Err(BlobError::Storage(StorageError::not_found(format!(
            "missing terracedb-bricks reserved table {}",
            descriptor.name
        ))));
    }

    match db.create_table(descriptor.table_config()).await {
        Ok(table) => Ok(table),
        Err(CreateTableError::AlreadyExists(_)) => db.try_table(descriptor.name).ok_or_else(|| {
            BlobError::Storage(StorageError::corruption(format!(
                "terracedb-bricks reserved table {} raced into existence but cannot be opened",
                descriptor.name
            )))
        }),
        Err(error) => Err(map_create_table_error(error)),
    }
}

async fn clear_output_namespace(
    table: &Table,
    namespace: &str,
    tx: &mut ProjectionTransaction,
) -> Result<(), ProjectionHandlerError> {
    let (start, end) = namespace_scan_range(namespace).map_err(wrap_handler_blob_error)?;
    let mut rows = table
        .scan(start, end, ScanOptions::default())
        .await
        .map_err(ProjectionHandlerError::from)?;
    while let Some((key, _value)) = rows.next().await {
        tx.delete(table, key);
    }
    Ok(())
}

async fn index_blob_metadata(
    term_index_table: &Table,
    text_chunk_table: &Table,
    blob_store: &dyn BlobStore,
    extraction: Option<&BlobTextExtractionConfig>,
    metadata: &BlobMetadata,
    tx: &mut ProjectionTransaction,
) -> Result<(), ProjectionHandlerError> {
    for term in metadata_index_terms(metadata) {
        tx.put(
            term_index_table,
            BlobTermIndexKey {
                namespace: metadata.namespace.clone(),
                term,
                blob_id: metadata.id,
                extractor: METADATA_INDEX_EXTRACTOR.to_string(),
                chunk_index: 0,
            }
            .encode()
            .map_err(BlobError::from)
            .map_err(wrap_handler_blob_error)?,
            encode_row(&BlobTermIndexRow {
                source: METADATA_INDEX_EXTRACTOR.to_string(),
            })
            .map_err(wrap_handler_blob_error)?,
        );
    }

    let Some(extraction) = extraction else {
        return Ok(());
    };

    let extracted_chunks = extract_text_chunks(blob_store, metadata, extraction).await?;
    for (chunk_index, chunk) in extracted_chunks.into_iter().enumerate() {
        let chunk_index = u32::try_from(chunk_index).map_err(|_| {
            ProjectionHandlerError::new(std::io::Error::other(
                "blob extraction produced too many chunks for u32 chunk indexes",
            ))
        })?;
        tx.put(
            text_chunk_table,
            BlobTextChunkKey {
                namespace: metadata.namespace.clone(),
                blob_id: metadata.id,
                extractor: extraction.extractor.clone(),
                chunk_index,
            }
            .encode()
            .map_err(BlobError::from)
            .map_err(wrap_handler_blob_error)?,
            encode_row(&BlobTextChunkRow {
                text: chunk.clone(),
            })
            .map_err(wrap_handler_blob_error)?,
        );
        for term in tokenize_terms(&chunk) {
            tx.put(
                term_index_table,
                BlobTermIndexKey {
                    namespace: metadata.namespace.clone(),
                    term,
                    blob_id: metadata.id,
                    extractor: extraction.extractor.clone(),
                    chunk_index,
                }
                .encode()
                .map_err(BlobError::from)
                .map_err(wrap_handler_blob_error)?,
                encode_row(&BlobTermIndexRow {
                    source: extraction.extractor.clone(),
                })
                .map_err(wrap_handler_blob_error)?,
            );
        }
    }

    Ok(())
}

async fn extract_text_chunks(
    blob_store: &dyn BlobStore,
    metadata: &BlobMetadata,
    extraction: &BlobTextExtractionConfig,
) -> Result<Vec<String>, ProjectionHandlerError> {
    let mut stream = blob_store
        .get(&metadata.object_key, crate::BlobGetOptions::default())
        .await
        .map_err(ProjectionHandlerError::new)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(ProjectionHandlerError::new)?;
        bytes.extend_from_slice(&chunk);
    }
    let Ok(text) = String::from_utf8(bytes) else {
        return Ok(Vec::new());
    };
    Ok(split_text_chunks(&text, extraction.chunk_bytes.max(1)))
}

async fn collect_blob_ids_for_exact_term(
    snapshot: &Snapshot,
    table: &Table,
    namespace: &str,
    term: &str,
    extractor: &str,
) -> Result<BTreeSet<BlobId>, BlobError> {
    let prefix = exact_term_prefix(namespace, term)?;
    let mut rows = snapshot
        .scan_prefix(table, prefix, ScanOptions::default())
        .await
        .map_err(map_read_error)?;
    let mut ids = BTreeSet::new();
    while let Some((key, _value)) = rows.next().await {
        let term_key = decode_term_index_key(&key)?;
        if term_key.extractor == extractor {
            ids.insert(term_key.blob_id);
        }
    }
    Ok(ids)
}

async fn collect_blob_ids_in_term_prefix(
    snapshot: &Snapshot,
    table: &Table,
    namespace: &str,
    term_prefix: &str,
    extractor: &str,
) -> Result<BTreeSet<BlobId>, BlobError> {
    let start = term_prefix_start(namespace, term_prefix)?;
    let end = prefix_end(&start);
    collect_blob_ids_in_term_range(snapshot, table, start, end, extractor).await
}

async fn collect_blob_ids_in_numeric_term_range(
    snapshot: &Snapshot,
    table: &Table,
    namespace: &str,
    label: &str,
    minimum: u64,
    maximum: u64,
    extractor: &str,
) -> Result<BTreeSet<BlobId>, BlobError> {
    if minimum > maximum {
        return Ok(BTreeSet::new());
    }
    let start = term_prefix_start(namespace, &numeric_index_term(label, minimum))?;
    let end = prefix_end(&term_prefix_start(
        namespace,
        &numeric_index_term(label, maximum),
    )?);
    collect_blob_ids_in_term_range(snapshot, table, start, end, extractor).await
}

async fn collect_blob_ids_in_term_range(
    snapshot: &Snapshot,
    table: &Table,
    start: Vec<u8>,
    end: Vec<u8>,
    extractor: &str,
) -> Result<BTreeSet<BlobId>, BlobError> {
    let mut rows = snapshot
        .scan(table, start, end, ScanOptions::default())
        .await
        .map_err(map_read_error)?;
    let mut ids = BTreeSet::new();
    while let Some((key, _value)) = rows.next().await {
        let term_key = decode_term_index_key(&key)?;
        if term_key.extractor == extractor {
            ids.insert(term_key.blob_id);
        }
    }
    Ok(ids)
}

async fn find_text_snippet(
    snapshot: &Snapshot,
    text_chunk_table: &Table,
    namespace: &str,
    blob_id: BlobId,
    extractor: &str,
    normalized_terms: &[String],
) -> Result<Option<String>, BlobError> {
    if normalized_terms.is_empty() {
        return Ok(None);
    }

    let prefix = text_chunk_prefix(namespace, blob_id, extractor)?;
    let mut rows = snapshot
        .scan_prefix(text_chunk_table, prefix, ScanOptions::default())
        .await
        .map_err(map_read_error)?;
    while let Some((key, value)) = rows.next().await {
        let chunk_key = decode_text_chunk_key(&key)?;
        if chunk_key.extractor != extractor {
            continue;
        }
        let row = decode_row::<BlobTextChunkRow>(value, BLOB_TEXT_CHUNK_TABLE_NAME)?;
        let normalized = row.text.to_lowercase();
        if normalized_terms
            .iter()
            .all(|term| normalized.contains(term.as_str()))
        {
            return Ok(Some(compact_snippet(&row.text)));
        }
    }

    Ok(None)
}

fn require_supported_extracted_text_query<'a>(
    extraction: Option<&'a BlobTextExtractionConfig>,
    query: Option<&'a BlobExtractedTextQuery>,
) -> Result<Option<&'a BlobExtractedTextQuery>, BlobError> {
    let Some(query) = query else {
        return Ok(None);
    };
    let Some(extraction) = extraction else {
        return Err(BlobError::UnsupportedOperation {
            operation: "blob extracted-text search requires an enabled extractor",
        });
    };
    if extraction.extractor != query.extractor {
        return Err(BlobError::UnsupportedOperation {
            operation: "blob extracted-text search requested an unsupported extractor",
        });
    }
    Ok(Some(query))
}

fn metadata_index_terms(metadata: &BlobMetadata) -> BTreeSet<String> {
    let mut terms = BTreeSet::from([
        ALL_BLOBS_TERM.to_string(),
        numeric_index_term("size", metadata.size_bytes),
        numeric_index_term("created_at", metadata.created_at.get()),
        numeric_index_term("updated_at", metadata.updated_at.get()),
    ]);
    if let Some(alias) = metadata.alias.as_ref() {
        terms.insert(alias_index_term(alias.as_str()));
        terms.extend(tokenize_terms(alias.as_str()));
    }
    if let Some(content_type) = metadata.content_type.as_ref() {
        terms.insert(content_type_index_term(content_type));
        terms.extend(tokenize_terms(content_type));
    }
    terms.insert(format!("digest:{}", metadata.digest.to_lowercase()));
    terms.insert(metadata.digest.to_lowercase());

    for (key, value) in &metadata.tags {
        terms.insert(tag_index_term(key, value));
        terms.extend(tokenize_terms(key));
        terms.extend(tokenize_terms(value));
    }
    for (key, value) in &metadata.metadata {
        terms.insert(metadata_exact_index_term(key, value));
        terms.extend(tokenize_terms(key));
        terms.extend(tokenize_terms(&canonical_json(value)));
    }
    terms
}

fn alias_index_term(alias: &str) -> String {
    format!("alias:{}", alias.to_lowercase())
}

fn content_type_index_term(content_type: &str) -> String {
    format!("content_type:{}", content_type.to_lowercase())
}

fn tag_index_term(key: &str, value: &str) -> String {
    format!("tag:{}={}", key.to_lowercase(), value.to_lowercase())
}

fn metadata_exact_index_term(key: &str, value: &JsonValue) -> String {
    format!(
        "metadata:{}={}",
        key.to_lowercase(),
        canonical_json(value).to_lowercase()
    )
}

fn numeric_index_term(label: &str, value: u64) -> String {
    format!("{label}:{value:020}")
}

fn canonical_json(value: &JsonValue) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
}

fn tokenize_terms(text: &str) -> BTreeSet<String> {
    text.split(|char: char| !char.is_ascii_alphanumeric())
        .filter_map(normalize_term)
        .collect()
}

fn normalize_query_terms(terms: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::new();
    for term in terms.iter().filter_map(|term| normalize_term(term)) {
        if seen.insert(term.clone()) {
            normalized.push(term);
        }
    }
    normalized
}

fn normalize_term(term: &str) -> Option<String> {
    let trimmed = term.trim().to_lowercase();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn split_text_chunks(text: &str, chunk_bytes: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if !current.is_empty() && current.len() + ch.len_utf8() > chunk_bytes {
            chunks.push(current);
            current = String::new();
        }
        current.push(ch);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn compact_snippet(text: &str) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut snippet = String::new();
    for ch in compact.trim().chars().take(120) {
        snippet.push(ch);
    }
    snippet
}

fn search_projection_name(
    namespace: &str,
    extraction: Option<&BlobTextExtractionConfig>,
) -> String {
    format!(
        "terracedb-bricks-search-v1-{}-{}",
        namespace,
        extraction
            .map(|config| config.extractor.as_str())
            .unwrap_or("metadata-only")
    )
}

fn intersect_candidates(candidates: &mut Option<BTreeSet<BlobId>>, next: BTreeSet<BlobId>) {
    match candidates {
        Some(existing) => {
            *existing = existing.intersection(&next).copied().collect();
        }
        None => *candidates = Some(next),
    }
}

fn namespace_scan_range(namespace: &str) -> Result<(Vec<u8>, Vec<u8>), BlobError> {
    let prefix = BlobCatalogKey::namespace_prefix(namespace)?;
    let end = prefix_end(&prefix);
    Ok((prefix, end))
}

fn exact_term_prefix(namespace: &str, term: &str) -> Result<Vec<u8>, BlobError> {
    let mut prefix = BlobCatalogKey::namespace_prefix(namespace)?;
    prefix.extend_from_slice(term.as_bytes());
    prefix.push(0);
    Ok(prefix)
}

fn term_prefix_start(namespace: &str, term_prefix: &str) -> Result<Vec<u8>, BlobError> {
    let mut start = BlobCatalogKey::namespace_prefix(namespace)?;
    start.extend_from_slice(term_prefix.as_bytes());
    Ok(start)
}

fn text_chunk_prefix(
    namespace: &str,
    blob_id: BlobId,
    extractor: &str,
) -> Result<Vec<u8>, BlobError> {
    let mut prefix = BlobCatalogKey::namespace_prefix(namespace)?;
    prefix.extend_from_slice(&blob_id.encode());
    prefix.extend_from_slice(extractor.as_bytes());
    prefix.push(0);
    Ok(prefix)
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    end.push(0xff);
    end
}

fn decode_term_index_key(bytes: &[u8]) -> Result<BlobTermIndexKey, BlobError> {
    BlobTermIndexKey::decode(bytes).map_err(|error| {
        corruption(format!(
            "invalid terracedb-bricks term index key in {}: {error}",
            BLOB_TERM_INDEX_TABLE_NAME
        ))
    })
}

fn decode_text_chunk_key(bytes: &[u8]) -> Result<BlobTextChunkKey, BlobError> {
    BlobTextChunkKey::decode(bytes).map_err(|error| {
        corruption(format!(
            "invalid terracedb-bricks text chunk key in {}: {error}",
            BLOB_TEXT_CHUNK_TABLE_NAME
        ))
    })
}

fn wrap_handler_blob_error(error: BlobError) -> ProjectionHandlerError {
    ProjectionHandlerError::new(std::io::Error::other(error.to_string()))
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
        published_once: true,
    }
}

fn default_gc_row_published_once() -> bool {
    true
}

fn past_grace(reference: Option<Timestamp>, now: Timestamp, grace_period: Duration) -> bool {
    if grace_period.is_zero() {
        return true;
    }

    let Some(reference) = reference else {
        return false;
    };
    let grace_micros = grace_period.as_micros().min(u64::MAX as u128) as u64;
    reference.get().saturating_add(grace_micros) <= now.get()
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

fn map_projection_error(error: ProjectionError) -> BlobError {
    match error {
        ProjectionError::CreateTable(error) => map_create_table_error(error),
        ProjectionError::Commit(error) => map_commit_error(error),
        ProjectionError::Read(error) => map_read_error(error),
        ProjectionError::SnapshotTooOld(error) => BlobError::SnapshotTooOld(error),
        ProjectionError::ChangeFeed(error) => map_change_feed_error(error),
        ProjectionError::Storage(error) => BlobError::Storage(error),
        other => BlobError::Storage(StorageError::unsupported(format!(
            "terracedb-bricks search index runtime failed: {other}"
        ))),
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

    use futures::{StreamExt, TryStreamExt};

    use terracedb::{
        Db, DbConfig, DbDependencies, S3Location, ScanOptions, StorageConfig, StubClock,
        StubFileSystem, StubObjectStore, StubRng, Table, TieredDurabilityMode, TieredStorageConfig,
        test_support::{FailpointMode, db_failpoint_registry, row_table_config},
    };
    use terracedb_projections::{
        PROJECTION_CURSOR_TABLE_NAME, ProjectionRuntime, RecomputeStrategy, SingleSourceProjection,
        failpoints::names as projection_failpoint_names,
    };

    use crate::{
        BLOB_TERM_INDEX_TABLE_NAME, BLOB_TEXT_CHUNK_TABLE_NAME, BlobCatalogKey,
        BlobCollection as _, BlobError, BlobExtractedTextQuery, BlobGcOptions, BlobObjectLayout,
        BlobQuery, BlobTextExtractionConfig, BlobWrite, compute_blob_digest,
    };

    use super::{
        BlobCatalogRow, BlobSearchIndexProjection, TerracedbBlobCollection, TestFailpoint,
        encode_row, ensure_search_tables, query_search_indexes, search_projection_name,
    };

    fn test_db_config(path: &str, durability: TieredDurabilityMode) -> DbConfig {
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
                durability,
                local_retention: terracedb::TieredLocalRetentionMode::Offload,
            }),
            hybrid_read: Default::default(),
            scheduler: None,
        }
    }

    struct TestHarness {
        path: &'static str,
        durability: TieredDurabilityMode,
        file_system: Arc<StubFileSystem>,
        object_store: Arc<StubObjectStore>,
        clock: Arc<StubClock>,
        blob_store: Arc<InMemoryBlobStore>,
    }

    impl TestHarness {
        fn new(path: &'static str) -> Self {
            Self {
                path,
                durability: TieredDurabilityMode::GroupCommit,
                file_system: Arc::new(StubFileSystem::default()),
                object_store: Arc::new(StubObjectStore::default()),
                clock: Arc::new(StubClock::default()),
                blob_store: Arc::new(InMemoryBlobStore::new()),
            }
        }

        async fn open_db(&self) -> Db {
            Db::open(
                test_db_config(self.path, self.durability),
                DbDependencies::new(
                    self.file_system.clone(),
                    self.object_store.clone(),
                    self.clock.clone(),
                    Arc::new(StubRng::seeded(44)),
                ),
            )
            .await
            .expect("open db")
        }

        async fn open_collection(&self, create_if_missing: bool) -> TerracedbBlobCollection {
            self.open_collection_with_config(
                crate::BlobCollectionConfig::new("docs")
                    .expect("namespace")
                    .with_create_if_missing(create_if_missing),
            )
            .await
        }

        async fn open_collection_and_db(
            &self,
            config: crate::BlobCollectionConfig,
        ) -> (Db, TerracedbBlobCollection) {
            let db = self.open_db().await;
            let collection =
                TerracedbBlobCollection::open(db.clone(), config, self.blob_store.clone())
                    .await
                    .expect("open bricks collection");
            (db, collection)
        }

        async fn open_collection_with_config(
            &self,
            config: crate::BlobCollectionConfig,
        ) -> TerracedbBlobCollection {
            let db = self.open_db().await;
            TerracedbBlobCollection::open(db, config, self.blob_store.clone())
                .await
                .expect("open bricks collection")
        }
    }

    async fn collect_search(
        collection: &TerracedbBlobCollection,
        query: BlobQuery,
    ) -> Vec<crate::BlobSearchRow> {
        collection
            .search(query)
            .await
            .expect("search")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect search rows")
    }

    async fn collect_keys(table: &Table, prefix: Vec<u8>) -> Vec<Vec<u8>> {
        table
            .scan(
                prefix.clone(),
                super::prefix_end(&prefix),
                ScanOptions::default(),
            )
            .await
            .expect("scan reserved table")
            .map(|(key, _value)| key)
            .collect()
            .await
    }

    use crate::{BlobAlias, BlobStore, InMemoryBlobStore};

    #[tokio::test]
    async fn upload_complete_before_publish_failure_leaves_no_visible_metadata() {
        let harness = TestHarness::new("/bricks-fail-after-upload");
        let collection = harness.open_collection(true).await;
        let payload = b"hello";
        let layout = BlobObjectLayout::new("", "docs").expect("layout");
        let digest = compute_blob_digest(payload);
        let object_key = layout
            .content_addressed_key(&digest)
            .expect("content-addressed key");
        collection.arm_failpoint(TestFailpoint::AfterUploadBeforePublish);

        let error = collection
            .put(BlobWrite {
                alias: Some(BlobAlias::new("docs/latest").expect("alias")),
                data: crate::BlobWriteData::bytes(payload.as_slice()),
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

        let gc = reopened
            .collect_garbage(BlobGcOptions::default())
            .await
            .expect("gc orphan");
        assert_eq!(gc.deleted_objects, 1);
        assert!(
            harness
                .blob_store
                .stat(&object_key)
                .await
                .expect("stat reclaimed orphan")
                .is_none()
        );
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
    async fn search_rebuilds_from_current_catalog_and_stays_deterministic_across_reopen() {
        let harness = TestHarness::new("/bricks-search-rebuild");
        let initial_config = crate::BlobCollectionConfig::new("docs")
            .expect("namespace")
            .with_create_if_missing(true)
            .with_text_extraction(BlobTextExtractionConfig::plain_text());
        let collection = harness.open_collection_with_config(initial_config).await;

        collection
            .put(BlobWrite {
                alias: Some(BlobAlias::new("guide/one").expect("alias")),
                data: crate::BlobWriteData::bytes("hello terracedb"),
                content_type: Some("text/plain".to_string()),
                tags: [("kind".to_string(), "doc".to_string())]
                    .into_iter()
                    .collect(),
                metadata: [("title".to_string(), serde_json::json!("Guide One"))]
                    .into_iter()
                    .collect(),
            })
            .await
            .expect("put guide one");
        collection
            .put(BlobWrite {
                alias: Some(BlobAlias::new("guide/two").expect("alias")),
                data: crate::BlobWriteData::bytes("deterministic terracedb indexing"),
                content_type: Some("text/plain".to_string()),
                tags: [("kind".to_string(), "doc".to_string())]
                    .into_iter()
                    .collect(),
                metadata: [("title".to_string(), serde_json::json!("Guide Two"))]
                    .into_iter()
                    .collect(),
            })
            .await
            .expect("put guide two");
        collection
            .put(BlobWrite {
                alias: Some(BlobAlias::new("notes/three").expect("alias")),
                data: crate::BlobWriteData::bytes("notes only"),
                content_type: Some("text/plain".to_string()),
                tags: [("kind".to_string(), "note".to_string())]
                    .into_iter()
                    .collect(),
                metadata: [("title".to_string(), serde_json::json!("Note Three"))]
                    .into_iter()
                    .collect(),
            })
            .await
            .expect("put note three");

        let metadata_rows = collect_search(
            &collection,
            BlobQuery {
                alias_prefix: Some("guide/".to_string()),
                content_type: Some("text/plain".to_string()),
                required_tags: [("kind".to_string(), "doc".to_string())]
                    .into_iter()
                    .collect(),
                terms: vec!["guide".to_string()],
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            metadata_rows
                .iter()
                .map(|row| {
                    row.metadata
                        .alias
                        .as_ref()
                        .expect("guide row should have alias")
                        .to_string()
                })
                .collect::<Vec<_>>(),
            vec!["guide/one".to_string(), "guide/two".to_string()]
        );

        let text_rows = collect_search(
            &collection,
            BlobQuery {
                extracted_text: Some(BlobExtractedTextQuery::plain_text(["deterministic"])),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(text_rows.len(), 1);
        assert_eq!(
            text_rows[0]
                .metadata
                .alias
                .as_ref()
                .expect("text match should have alias")
                .as_str(),
            "guide/two"
        );
        assert!(
            text_rows[0]
                .snippet
                .as_deref()
                .expect("text search should return snippet")
                .contains("deterministic")
        );

        let reopened_config = crate::BlobCollectionConfig::new("docs")
            .expect("namespace")
            .with_text_extraction(BlobTextExtractionConfig::plain_text());
        let reopened = harness.open_collection_with_config(reopened_config).await;
        let reopened_metadata_rows = collect_search(
            &reopened,
            BlobQuery {
                alias_prefix: Some("guide/".to_string()),
                content_type: Some("text/plain".to_string()),
                required_tags: [("kind".to_string(), "doc".to_string())]
                    .into_iter()
                    .collect(),
                terms: vec!["guide".to_string()],
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            reopened_metadata_rows
                .iter()
                .map(|row| row.metadata.id)
                .collect::<Vec<_>>(),
            metadata_rows
                .iter()
                .map(|row| row.metadata.id)
                .collect::<Vec<_>>()
        );

        let reopened_text_rows = collect_search(
            &reopened,
            BlobQuery {
                extracted_text: Some(BlobExtractedTextQuery::plain_text(["deterministic"])),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            reopened_text_rows
                .iter()
                .map(|row| row.metadata.id)
                .collect::<Vec<_>>(),
            text_rows
                .iter()
                .map(|row| row.metadata.id)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            reopened_text_rows[0].snippet.as_deref(),
            text_rows[0].snippet.as_deref()
        );
    }

    #[tokio::test]
    async fn projection_runtime_rebuilds_search_indexes_from_current_catalog_state() {
        let harness = TestHarness::new("/bricks-search-runtime-rebuild");
        let config = crate::BlobCollectionConfig::new("docs")
            .expect("namespace")
            .with_create_if_missing(true)
            .with_text_extraction(BlobTextExtractionConfig::plain_text());
        let (db, collection) = harness.open_collection_and_db(config).await;

        for (alias, body) in [
            ("guide/one", "hello world"),
            ("guide/two", "rebuild path"),
            ("notes/three", "ignored"),
        ] {
            collection
                .put(BlobWrite {
                    alias: Some(BlobAlias::new(alias).expect("alias")),
                    data: crate::BlobWriteData::bytes(body),
                    content_type: Some("text/plain".to_string()),
                    tags: BTreeMap::new(),
                    metadata: BTreeMap::new(),
                })
                .await
                .expect("put blob");
        }

        let rebuild_source = {
            let mut config = row_table_config("blob_catalog_rebuild_source");
            config.history_retention_sequences = Some(1);
            db.create_table(config)
                .await
                .expect("create retained rebuild source")
        };

        let mut target_sequence = terracedb::SequenceNumber::new(0);
        for alias in ["guide/one", "guide/two", "notes/three"] {
            let metadata = collection
                .stat(crate::BlobLocator::Alias(
                    BlobAlias::new(alias).expect("alias"),
                ))
                .await
                .expect("stat blob")
                .expect("metadata should exist");
            let row = BlobCatalogRow {
                alias: metadata.alias.clone(),
                object_key: metadata.object_key.clone(),
                digest: metadata.digest.clone(),
                size_bytes: metadata.size_bytes,
                content_type: metadata.content_type.clone(),
                tags: metadata.tags.clone(),
                metadata: metadata.metadata.clone(),
                created_at: metadata.created_at,
                updated_at: metadata.updated_at,
                index_state: metadata.index_state,
            };
            target_sequence = rebuild_source
                .write(
                    BlobCatalogKey {
                        namespace: "docs".to_string(),
                        blob_id: metadata.id,
                    }
                    .encode()
                    .expect("encode rebuild source key"),
                    encode_row(&row).expect("encode rebuild source row"),
                )
                .await
                .expect("write rebuild source row");
        }

        let (text_chunk_table, term_index_table) = ensure_search_tables(&db, true)
            .await
            .expect("ensure search tables");
        let runtime = ProjectionRuntime::open(db.clone())
            .await
            .expect("open projection runtime");
        let mut handle = runtime
            .start_single_source(
                SingleSourceProjection::new(
                    "custom-search-rebuild",
                    rebuild_source.clone(),
                    BlobSearchIndexProjection {
                        namespace: "docs".to_string(),
                        catalog_table: rebuild_source.clone(),
                        text_chunk_table: text_chunk_table.clone(),
                        term_index_table: term_index_table.clone(),
                        blob_store: harness.blob_store.clone(),
                        extraction: Some(BlobTextExtractionConfig::plain_text()),
                    },
                )
                .with_outputs([term_index_table.clone(), text_chunk_table.clone()])
                .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
            )
            .await
            .expect("start search rebuild projection");
        handle
            .wait_for_sources([(&rebuild_source, target_sequence)])
            .await
            .expect("projection should rebuild from current source state");

        let rows = query_search_indexes(
            &db,
            &rebuild_source,
            &term_index_table,
            &text_chunk_table,
            "docs",
            Some(&BlobTextExtractionConfig::plain_text()),
            &BlobQuery {
                alias_prefix: Some("guide/".to_string()),
                extracted_text: Some(BlobExtractedTextQuery::plain_text(["rebuild"])),
                ..Default::default()
            },
        )
        .await
        .expect("query rebuilt index state");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .metadata
                .alias
                .as_ref()
                .expect("rebuilt row should have alias")
                .as_str(),
            "guide/two"
        );
        assert!(
            rows[0]
                .snippet
                .as_deref()
                .expect("rebuilt text search should return snippet")
                .contains("rebuild")
        );

        handle
            .shutdown()
            .await
            .expect("stop custom rebuild projection");
    }

    #[tokio::test]
    async fn extracted_text_search_requires_explicit_opt_in() {
        let harness = TestHarness::new("/bricks-search-opt-in");
        let collection = harness.open_collection(true).await;

        collection
            .put(BlobWrite {
                alias: Some(BlobAlias::new("docs/plain").expect("alias")),
                data: crate::BlobWriteData::bytes("search me"),
                content_type: Some("text/plain".to_string()),
                tags: BTreeMap::new(),
                metadata: BTreeMap::new(),
            })
            .await
            .expect("put text blob");

        let error = match collection
            .search(BlobQuery {
                extracted_text: Some(BlobExtractedTextQuery::plain_text(["search"])),
                ..Default::default()
            })
            .await
        {
            Ok(_) => panic!("extracted-text search should fail closed without explicit extractor"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            BlobError::UnsupportedOperation { operation }
                if operation.contains("extracted-text search")
        ));
    }

    #[tokio::test]
    async fn projection_failpoint_preserves_search_cursor_and_output_until_retry() {
        let harness = TestHarness::new("/bricks-search-failpoint");
        let config = crate::BlobCollectionConfig::new("docs")
            .expect("namespace")
            .with_create_if_missing(true)
            .with_text_extraction(BlobTextExtractionConfig::plain_text());
        let (db, collection) = harness.open_collection_and_db(config).await;

        collection
            .put(BlobWrite {
                alias: Some(BlobAlias::new("guide/failpoint").expect("alias")),
                data: crate::BlobWriteData::bytes("atomic deterministic indexing"),
                content_type: Some("text/plain".to_string()),
                tags: [("kind".to_string(), "doc".to_string())]
                    .into_iter()
                    .collect(),
                metadata: [("title".to_string(), serde_json::json!("Failpoint"))]
                    .into_iter()
                    .collect(),
            })
            .await
            .expect("put blob");

        db_failpoint_registry(&db).arm_error(
            projection_failpoint_names::PROJECTION_APPLY_BEFORE_COMMIT,
            terracedb::StorageError::io("simulated projection failpoint"),
            FailpointMode::Once,
        );

        let error = match collection
            .search(BlobQuery {
                extracted_text: Some(BlobExtractedTextQuery::plain_text(["atomic"])),
                ..Default::default()
            })
            .await
        {
            Ok(_) => panic!("search should surface the projection failpoint"),
            Err(error) => error,
        };
        assert!(matches!(error, BlobError::Storage(_)));

        let namespace_prefix =
            crate::BlobCatalogKey::namespace_prefix("docs").expect("namespace prefix");
        let term_keys = collect_keys(
            &db.table(BLOB_TERM_INDEX_TABLE_NAME),
            namespace_prefix.clone(),
        )
        .await;
        let text_keys = collect_keys(&db.table(BLOB_TEXT_CHUNK_TABLE_NAME), namespace_prefix).await;
        assert!(term_keys.is_empty());
        assert!(text_keys.is_empty());

        let runtime = ProjectionRuntime::open(db.clone())
            .await
            .expect("open projection runtime");
        assert_eq!(
            runtime
                .load_projection_cursor(&search_projection_name(
                    "docs",
                    Some(&BlobTextExtractionConfig::plain_text()),
                ))
                .await
                .expect("load search projection cursor"),
            terracedb::LogCursor::beginning()
        );
        assert!(
            db.table(PROJECTION_CURSOR_TABLE_NAME)
                .scan(Vec::new(), vec![0xff], ScanOptions::default())
                .await
                .expect("scan projection cursor table")
                .count()
                .await
                == 0
        );

        let rows = collect_search(
            &collection,
            BlobQuery {
                extracted_text: Some(BlobExtractedTextQuery::plain_text(["atomic"])),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(rows.len(), 1);
        assert!(
            !collect_keys(
                &db.table(BLOB_TERM_INDEX_TABLE_NAME),
                crate::BlobCatalogKey::namespace_prefix("docs").expect("namespace prefix"),
            )
            .await
            .is_empty()
        );
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

    #[tokio::test]
    async fn gc_recovers_after_interrupting_between_delete_and_gc_row_commit() {
        let harness = TestHarness::new("/bricks-gc-interrupted-after-delete");
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
            .delete(crate::BlobLocator::Alias(alias))
            .await
            .expect("delete metadata");
        collection.arm_failpoint(TestFailpoint::AfterGcDeleteBeforeCommit);

        let error = collection
            .collect_garbage(BlobGcOptions::default())
            .await
            .err()
            .expect("interrupted gc should fail");
        assert!(matches!(error, BlobError::Storage(_)));
        assert!(
            harness
                .blob_store
                .stat(&handle.object_key)
                .await
                .expect("stat after interrupted delete")
                .is_none()
        );

        let reopened = harness.open_collection(false).await;
        let retried = reopened
            .collect_garbage(BlobGcOptions::default())
            .await
            .expect("retry gc");
        assert_eq!(retried.deleted_objects, 1);
    }
}
