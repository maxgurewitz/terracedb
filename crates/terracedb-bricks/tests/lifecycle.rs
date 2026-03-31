use std::{collections::BTreeMap, sync::Arc};

use futures::{StreamExt, TryStreamExt};

use terracedb::{
    Db, DbConfig, DbDependencies, LogCursor, S3Location, ScanOptions, Snapshot, StorageConfig,
    StubClock, StubFileSystem, StubObjectStore, StubRng, Table, TieredDurabilityMode,
    TieredStorageConfig,
};
use terracedb_bricks::{
    BLOB_ACTIVITY_TABLE_NAME, BLOB_CATALOG_TABLE_NAME, BlobActivityKey, BlobActivityKind,
    BlobActivityOptions, BlobAlias, BlobCatalogKey, BlobCollection, BlobCollectionConfig,
    BlobError, BlobLocator, BlobQuery, BlobReadOptions, BlobStore, BlobWrite, BlobWriteData,
    InMemoryBlobStore, TerracedbBlobCollection,
};

struct Harness {
    path: &'static str,
    durability: TieredDurabilityMode,
    file_system: Arc<StubFileSystem>,
    db_object_store: Arc<StubObjectStore>,
    clock: Arc<StubClock>,
    blob_store: Arc<InMemoryBlobStore>,
}

impl Harness {
    fn new(path: &'static str, durability: TieredDurabilityMode) -> Self {
        Self {
            path,
            durability,
            file_system: Arc::new(StubFileSystem::default()),
            db_object_store: Arc::new(StubObjectStore::default()),
            clock: Arc::new(StubClock::default()),
            blob_store: Arc::new(InMemoryBlobStore::new()),
        }
    }

    async fn open_db(&self) -> Db {
        Db::open(
            DbConfig {
                storage: StorageConfig::Tiered(TieredStorageConfig {
                    ssd: terracedb::SsdConfig {
                        path: self.path.to_string(),
                    },
                    s3: S3Location {
                        bucket: "terracedb-bricks-test".to_string(),
                        prefix: "lifecycle".to_string(),
                    },
                    max_local_bytes: 1024 * 1024,
                    durability: self.durability,
                    local_retention: terracedb::TieredLocalRetentionMode::Offload,
                }),
                hybrid_read: Default::default(),
                scheduler: None,
            },
            DbDependencies::new(
                self.file_system.clone(),
                self.db_object_store.clone(),
                self.clock.clone(),
                Arc::new(StubRng::seeded(9)),
            ),
        )
        .await
        .expect("open db")
    }

    async fn open_collection(&self, create_if_missing: bool) -> (Db, TerracedbBlobCollection) {
        let db = self.open_db().await;
        let collection = TerracedbBlobCollection::open(
            db.clone(),
            BlobCollectionConfig::new("docs")
                .expect("namespace")
                .with_create_if_missing(create_if_missing),
            self.blob_store.clone(),
        )
        .await
        .expect("open collection");
        (db, collection)
    }
}

async fn count_rows(snapshot: &Snapshot, table: &Table, prefix: Vec<u8>) -> usize {
    snapshot
        .scan_prefix(table, prefix, ScanOptions::default())
        .await
        .expect("scan prefix")
        .collect::<Vec<_>>()
        .await
        .len()
}

#[tokio::test]
async fn persists_alias_reads_activity_and_metadata_first_delete_across_reopen() {
    let harness = Harness::new(
        "/terracedb-bricks-lifecycle",
        TieredDurabilityMode::GroupCommit,
    );
    let (_db, collection) = harness.open_collection(true).await;
    let alias = BlobAlias::new("guide/latest").expect("alias");

    let handle = collection
        .put(BlobWrite {
            alias: Some(alias.clone()),
            data: BlobWriteData::bytes("hello terracedb bricks"),
            content_type: Some("text/plain".to_string()),
            tags: [("kind".to_string(), "doc".to_string())]
                .into_iter()
                .collect(),
            metadata: [("title".to_string(), serde_json::json!("Guide"))]
                .into_iter()
                .collect(),
        })
        .await
        .expect("put blob");

    let by_id = collection
        .stat(BlobLocator::Id(handle.id))
        .await
        .expect("stat id")
        .expect("blob exists");
    assert_eq!(by_id.alias.as_ref(), Some(&alias));

    let ranged = collection
        .get(
            BlobLocator::Alias(alias.clone()),
            BlobReadOptions {
                range: Some(terracedb_bricks::BlobByteRange::new(6, 15).expect("range")),
            },
        )
        .await
        .expect("get by alias")
        .data
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
        .expect("collect read");
    assert_eq!(ranged, b"terracedb");

    let search = collection
        .search(BlobQuery {
            alias_prefix: Some("guide/".to_string()),
            required_tags: [("kind".to_string(), "doc".to_string())]
                .into_iter()
                .collect(),
            terms: vec!["guide".to_string()],
            ..Default::default()
        })
        .await
        .expect("search")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect search");
    assert_eq!(search.len(), 1);
    assert_eq!(search[0].metadata.id, handle.id);

    let activities = collection
        .activity_since(LogCursor::beginning(), BlobActivityOptions::default())
        .await
        .expect("activity")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect activity");
    assert!(
        activities
            .iter()
            .any(|entry| entry.kind == BlobActivityKind::BlobPublished)
    );
    assert!(
        activities
            .iter()
            .any(|entry| entry.kind == BlobActivityKind::AliasUpserted)
    );

    collection
        .delete(BlobLocator::Alias(alias.clone()))
        .await
        .expect("delete metadata");

    let (_reopened_db, reopened) = harness.open_collection(false).await;
    assert!(
        reopened
            .stat(BlobLocator::Alias(alias.clone()))
            .await
            .expect("stat alias after reopen")
            .is_none()
    );
    assert!(
        reopened
            .stat(BlobLocator::Id(handle.id))
            .await
            .expect("stat id after reopen")
            .is_none()
    );
    assert!(
        harness
            .blob_store
            .stat(&handle.object_key)
            .await
            .expect("stat deferred object")
            .is_some()
    );
}

#[tokio::test]
async fn visible_and_durable_catalog_and_activity_rows_appear_together() {
    let harness = Harness::new("/terracedb-bricks-durable", TieredDurabilityMode::Deferred);
    let (db, collection) = harness.open_collection(true).await;
    let alias = BlobAlias::new("guide/latest").expect("alias");

    let handle = collection
        .put(BlobWrite {
            alias: Some(alias),
            data: BlobWriteData::bytes("hello"),
            content_type: Some("text/plain".to_string()),
            tags: BTreeMap::new(),
            metadata: BTreeMap::new(),
        })
        .await
        .expect("put");

    let catalog_table = db.table(BLOB_CATALOG_TABLE_NAME);
    let activity_table = db.table(BLOB_ACTIVITY_TABLE_NAME);
    let catalog_key = BlobCatalogKey {
        namespace: "docs".to_string(),
        blob_id: handle.id,
    }
    .encode()
    .expect("catalog key");
    let activity_prefix = BlobActivityKey::namespace_prefix("docs").expect("activity prefix");

    let visible = db.snapshot().await;
    let durable = db.durable_snapshot().await;
    assert!(
        visible
            .read(&catalog_table, catalog_key.clone())
            .await
            .expect("visible catalog read")
            .is_some()
    );
    assert!(
        durable
            .read(&catalog_table, catalog_key.clone())
            .await
            .expect("durable catalog read before flush")
            .is_none()
    );
    assert_eq!(
        count_rows(&visible, &activity_table, activity_prefix.clone()).await,
        2
    );
    assert_eq!(
        count_rows(&durable, &activity_table, activity_prefix.clone()).await,
        0
    );

    db.flush().await.expect("flush db");
    let durable_after = db.durable_snapshot().await;
    assert!(
        durable_after
            .read(&catalog_table, catalog_key)
            .await
            .expect("durable catalog read after flush")
            .is_some()
    );
    assert_eq!(
        count_rows(&durable_after, &activity_table, activity_prefix).await,
        2
    );
}

#[tokio::test]
async fn missing_backing_objects_fail_closed() {
    let harness = Harness::new(
        "/terracedb-bricks-missing-object",
        TieredDurabilityMode::GroupCommit,
    );
    let (_db, collection) = harness.open_collection(true).await;

    let handle = collection
        .put(BlobWrite {
            alias: None,
            data: BlobWriteData::bytes("hello"),
            content_type: Some("text/plain".to_string()),
            tags: BTreeMap::new(),
            metadata: BTreeMap::new(),
        })
        .await
        .expect("put");

    harness
        .blob_store
        .delete(&handle.object_key)
        .await
        .expect("delete backing object");

    let error = collection
        .get(BlobLocator::Id(handle.id), BlobReadOptions::default())
        .await
        .err()
        .expect("missing object should fail");
    assert!(matches!(
        error,
        BlobError::MissingObject { object_key } if object_key == handle.object_key
    ));
}
