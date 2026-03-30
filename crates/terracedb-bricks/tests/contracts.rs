use std::sync::Arc;

use bytes::Bytes;
use futures::{TryStreamExt, stream};

use terracedb::{LogCursor, ObjectKeyLayout, S3Location, StubObjectStore};
use terracedb_bricks::{
    BLOB_ACTIVITY_TABLE_NAME, BLOB_ALIAS_TABLE_NAME, BLOB_CATALOG_TABLE_NAME,
    BLOB_COLLECTION_SEMANTICS, BLOB_OBJECT_GC_TABLE_NAME, BLOB_TERM_INDEX_TABLE_NAME,
    BLOB_TEXT_CHUNK_TABLE_NAME, BlobActivityKind, BlobActivityOptions, BlobAlias, BlobByteRange,
    BlobCollection, BlobCollectionConfig, BlobDeleteSemantics, BlobGetOptions, BlobHandle,
    BlobIndexDurability, BlobLocator, BlobMissingObjectSemantics, BlobObjectLayout,
    BlobObjectReclamation, BlobPublishOrdering, BlobQuery, BlobReadOptions, BlobSemantics,
    BlobStore, BlobStoreByteStream, BlobStoreObjectStoreAdapter, BlobWrite, BlobWriteData,
    FrozenTableOwner, InMemoryBlobCollection, InMemoryBlobStore, frozen_table_configs,
    frozen_table_descriptors,
};

fn stream_bytes(bytes: &'static [u8]) -> BlobStoreByteStream {
    Box::pin(stream::once(async move { Ok(Bytes::from_static(bytes)) }))
}

fn assert_blob_collection_surface(
    collection: &dyn BlobCollection,
    expected_semantics: BlobSemantics,
) {
    assert_eq!(collection.semantics(), expected_semantics);
    assert_eq!(collection.config().namespace(), "docs");
}

fn assert_blob_store_surface(_store: &dyn BlobStore) {}

#[tokio::test]
async fn public_bricks_surface_compiles_and_is_instantiable() {
    let config = BlobCollectionConfig::new("docs")
        .expect("valid namespace")
        .with_create_if_missing(true);
    let memory_store = Arc::new(InMemoryBlobStore::new());
    let collection = InMemoryBlobCollection::with_blob_store(config.clone(), memory_store.clone());

    assert_blob_collection_surface(&collection, BLOB_COLLECTION_SEMANTICS);
    assert_blob_store_surface(memory_store.as_ref());

    let adapter = BlobStoreObjectStoreAdapter::new(Arc::new(StubObjectStore::default()));
    assert_blob_store_surface(&adapter);
    let adapter_key = "blobs/docs/adapter/check.txt";
    let adapter_info = adapter
        .put(adapter_key, stream_bytes(b"adapter"), Default::default())
        .await
        .expect("put via compatibility adapter");
    assert_eq!(adapter_info.size_bytes, 7);
    assert_eq!(
        adapter
            .stat(adapter_key)
            .await
            .expect("stat via adapter")
            .expect("adapter object should exist")
            .size_bytes,
        7
    );
    let adapter_range = adapter
        .get(
            adapter_key,
            BlobGetOptions {
                range: Some(BlobByteRange::new(1, 4).expect("range")),
            },
        )
        .await
        .expect("range get via adapter")
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
        .expect("collect adapter range");
    assert_eq!(adapter_range, b"dap");

    let alias = BlobAlias::new("guide/latest").expect("alias");
    let handle: BlobHandle = collection
        .put(BlobWrite {
            alias: Some(alias.clone()),
            data: BlobWriteData::Stream(stream_bytes(b"hello terracedb bricks")),
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
        .expect("stat by id")
        .expect("metadata by id");
    assert_eq!(by_id.alias.as_ref(), Some(&alias));
    assert_eq!(by_id.object_key, handle.object_key);

    let by_alias = collection
        .stat(BlobLocator::Alias(alias.clone()))
        .await
        .expect("stat by alias")
        .expect("metadata by alias");
    assert_eq!(by_alias.id, handle.id);

    let read = collection
        .get(
            BlobLocator::Alias(alias.clone()),
            BlobReadOptions {
                range: Some(BlobByteRange::new(6, 15).expect("range")),
            },
        )
        .await
        .expect("read blob");
    let ranged = read
        .data
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
        .expect("collect range");
    assert_eq!(ranged, b"terracedb");

    let results = collection
        .search(BlobQuery {
            alias_prefix: Some("guide/".to_string()),
            content_type: Some("text/plain".to_string()),
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
        .expect("collect search rows");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].metadata.id, handle.id);

    let activities = collection
        .activity_since(LogCursor::beginning(), BlobActivityOptions::default())
        .await
        .expect("activity stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect activities");
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

    let mut receiver = collection.subscribe_activity(BlobActivityOptions::default());
    assert_eq!(
        receiver.current(),
        activities.last().expect("activity").sequence
    );

    collection
        .delete(BlobLocator::Alias(alias.clone()))
        .await
        .expect("delete blob metadata");
    let next = receiver.changed().await.expect("activity receiver changed");
    assert!(next > activities.last().expect("activity").sequence);
}

#[test]
fn frozen_tables_and_object_prefixes_are_stable_and_disjoint() {
    let table_names = frozen_table_descriptors()
        .iter()
        .map(|descriptor| descriptor.name)
        .collect::<Vec<_>>();
    assert_eq!(
        table_names,
        vec![
            BLOB_CATALOG_TABLE_NAME,
            BLOB_ALIAS_TABLE_NAME,
            BLOB_OBJECT_GC_TABLE_NAME,
            BLOB_ACTIVITY_TABLE_NAME,
            BLOB_TEXT_CHUNK_TABLE_NAME,
            BLOB_TERM_INDEX_TABLE_NAME,
            "blob_embedding_index",
        ]
    );

    let owners = frozen_table_descriptors()
        .iter()
        .map(|descriptor| descriptor.owner)
        .collect::<Vec<_>>();
    assert_eq!(
        owners,
        vec![
            FrozenTableOwner::Library,
            FrozenTableOwner::Library,
            FrozenTableOwner::Library,
            FrozenTableOwner::Library,
            FrozenTableOwner::Projection,
            FrozenTableOwner::Projection,
            FrozenTableOwner::Application,
        ]
    );

    let bootstrap_names = frozen_table_configs()
        .into_iter()
        .map(|config| config.name)
        .collect::<Vec<_>>();
    assert_eq!(
        bootstrap_names,
        vec![
            BLOB_CATALOG_TABLE_NAME.to_string(),
            BLOB_ALIAS_TABLE_NAME.to_string(),
            BLOB_OBJECT_GC_TABLE_NAME.to_string(),
            BLOB_ACTIVITY_TABLE_NAME.to_string(),
        ]
    );

    let layout = BlobObjectLayout::new("/tenant-a/db-01/", "photos").expect("layout");
    assert_eq!(layout.object_prefix(), "tenant-a/db-01/blobs/photos");
    assert_eq!(
        layout.object_key("stub/0001"),
        "tenant-a/db-01/blobs/photos/stub/0001"
    );

    let engine_layout = ObjectKeyLayout::new(&S3Location {
        bucket: "bucket".to_string(),
        prefix: "/tenant-a/db-01/".to_string(),
    });
    let blob_prefix = format!("{}/", layout.object_prefix());
    let backup_prefix = format!("{}/", engine_layout.backup_prefix());
    let cold_prefix = format!("{}/", engine_layout.cold_prefix());

    assert!(!blob_prefix.starts_with(&backup_prefix));
    assert!(!backup_prefix.starts_with(&blob_prefix));
    assert!(!blob_prefix.starts_with(&cold_prefix));
    assert!(!cold_prefix.starts_with(&blob_prefix));
}

#[test]
fn semantics_are_explicit_and_frozen() {
    assert_eq!(
        BLOB_COLLECTION_SEMANTICS,
        BlobSemantics {
            publish_ordering: BlobPublishOrdering::UploadBeforePublish,
            delete_visibility: BlobDeleteSemantics::MetadataDeleteBeforeGc,
            object_reclamation: BlobObjectReclamation::DeferredGcOnly,
            indexing: BlobIndexDurability::DurableActivityOnly,
            missing_object_reads: BlobMissingObjectSemantics::FailClosed,
        }
    );
}
