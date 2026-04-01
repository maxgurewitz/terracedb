use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::TryStreamExt;

use terracedb::{
    Clock, Db, DbConfig, DbDependencies, Rng, S3Location, StorageConfig, StubClock, StubFileSystem,
    StubObjectStore, StubRng, TieredDurabilityMode, TieredStorageConfig,
};
use terracedb_bricks::{
    BlobAlias, BlobCollection, BlobCollectionConfig, BlobExtractedTextQuery, BlobHandle,
    BlobLocator, BlobObjectLayout, BlobPutOptions, BlobQuery, BlobReadOptions, BlobStore,
    BlobStoreFailure, BlobWrite, BlobWriteData, InMemoryBlobStore, TerracedbBlobCollection,
    compute_blob_digest, read_blob_bytes, upload_blob_bytes,
};
use terracedb_simulation::{CutPoint, SeededSimulationRunner};

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlobStoreSimulationCapture {
    body: Vec<u8>,
    digest: String,
    object_key: String,
    listed_keys: Vec<String>,
    observed_at: u64,
}

fn run_blob_store_simulation(seed: u64) -> turmoil::Result<BlobStoreSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(25))
        .run_with(move |context| async move {
            let store = InMemoryBlobStore::new();
            let payload =
                format!("blob-sim-{seed:x}-{}", context.rng().next_u64() % 10_000).into_bytes();
            let layout =
                BlobObjectLayout::new(format!("tenant-{seed:04x}"), "sim").expect("layout");
            let digest = compute_blob_digest(&payload);
            let object_key = layout
                .content_addressed_key(&digest)
                .expect("content-addressed key");

            if seed % 2 == 0 {
                store.inject_failure(BlobStoreFailure::lost_put_response(object_key.clone()));
            }

            context
                .clock()
                .sleep(Duration::from_millis((seed % 3) + 1))
                .await;
            let receipt = upload_blob_bytes(
                &store,
                &layout,
                Bytes::from(payload.clone()),
                BlobPutOptions {
                    content_type: Some("application/octet-stream".to_string()),
                    expected_size: Some(payload.len() as u64),
                },
            )
            .await
            .expect("upload blob");

            context
                .clock()
                .sleep(Duration::from_millis((seed % 5) + 1))
                .await;
            let body = read_blob_bytes(&store, receipt.object_key(), Default::default())
                .await
                .expect("read blob");
            let listed_keys = store
                .list_prefix(&layout.object_prefix())
                .await
                .expect("list prefix")
                .into_iter()
                .map(|info| info.key)
                .collect::<Vec<_>>();

            Ok(BlobStoreSimulationCapture {
                body,
                digest: receipt.digest,
                object_key: receipt.object.key,
                listed_keys,
                observed_at: context.clock().now().get(),
            })
        })
}

#[test]
fn blob_store_simulation_replays_same_seed() -> turmoil::Result {
    let first = run_blob_store_simulation(0x4201)?;
    let second = run_blob_store_simulation(0x4201)?;
    assert_eq!(first, second);
    Ok(())
}

#[test]
fn blob_store_simulation_changes_shape_for_different_seeds() -> turmoil::Result {
    let left = run_blob_store_simulation(0x4202)?;
    let right = run_blob_store_simulation(0x4203)?;

    assert_ne!(left.body, right.body);
    assert_ne!(left.object_key, right.object_key);
    assert_ne!(left.digest, right.digest);
    assert_eq!(left.listed_keys, vec![left.object_key.clone()]);
    assert_eq!(right.listed_keys, vec![right.object_key.clone()]);
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexedSearchSimulationCapture {
    alias_hits: Vec<String>,
    text_hits: Vec<(String, Option<String>)>,
}

fn tiered_simulation_config(path: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: terracedb::SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-bricks-test".to_string(),
                prefix: "simulation".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability: TieredDurabilityMode::GroupCommit,
            local_retention: terracedb::TieredLocalRetentionMode::Offload,
        }),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

async fn collect_alias_hits(collection: &TerracedbBlobCollection) -> Vec<String> {
    collection
        .search(BlobQuery {
            alias_prefix: Some("guide/".to_string()),
            ..Default::default()
        })
        .await
        .expect("search current aliases")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect alias hits")
        .into_iter()
        .map(|row| {
            row.metadata
                .alias
                .expect("search hit should keep alias")
                .to_string()
        })
        .collect()
}

async fn collect_text_hits(collection: &TerracedbBlobCollection) -> Vec<(String, Option<String>)> {
    collection
        .search(BlobQuery {
            extracted_text: Some(BlobExtractedTextQuery::plain_text(["deterministic"])),
            ..Default::default()
        })
        .await
        .expect("search extracted text")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect text hits")
        .into_iter()
        .map(|row| {
            let alias = row
                .metadata
                .alias
                .expect("search hit should keep alias")
                .to_string();
            (alias, row.snippet)
        })
        .collect()
}

fn run_indexed_search_simulation(seed: u64) -> turmoil::Result<IndexedSearchSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_secs(1))
        .run_with(move |context| async move {
            let config = tiered_simulation_config(&format!("/terracedb-bricks-search-{seed}"));
            let blob_store = Arc::new(InMemoryBlobStore::new());
            let collection_config = BlobCollectionConfig::new("docs")
                .expect("namespace")
                .with_create_if_missing(true)
                .with_plain_text_extraction();

            let db = context.open_db(config.clone()).await.expect("open db");
            let collection =
                TerracedbBlobCollection::open(db, collection_config.clone(), blob_store.clone())
                    .await
                    .expect("open collection");

            for (alias, body) in [
                (
                    "guide/latest",
                    format!(
                        "deterministic search guide {seed:x} {}",
                        context.rng().next_u64() % 10_000
                    ),
                ),
                (
                    "guide/reference",
                    format!(
                        "deterministic reference notes {seed:x} {}",
                        context.rng().next_u64() % 10_000
                    ),
                ),
                (
                    "notes/today",
                    format!(
                        "private note {seed:x} {}",
                        context.rng().next_u64() % 10_000
                    ),
                ),
            ] {
                collection
                    .put(BlobWrite {
                        alias: Some(BlobAlias::new(alias).expect("alias")),
                        data: BlobWriteData::bytes(body.into_bytes()),
                        content_type: Some("text/plain".to_string()),
                        tags: BTreeMap::new(),
                        metadata: BTreeMap::new(),
                    })
                    .await
                    .expect("put blob");
            }

            let alias_hits = collect_alias_hits(&collection).await;
            let text_hits = collect_text_hits(&collection).await;

            let reopened_db = context
                .restart_db(config, CutPoint::AfterDurabilityBoundary)
                .await
                .expect("restart db");
            let reopened =
                TerracedbBlobCollection::open(reopened_db, collection_config, blob_store)
                    .await
                    .expect("reopen collection");

            let reopened_alias_hits = collect_alias_hits(&reopened).await;
            let reopened_text_hits = collect_text_hits(&reopened).await;

            assert_eq!(reopened_alias_hits, alias_hits);
            assert_eq!(reopened_text_hits, text_hits);

            Ok(IndexedSearchSimulationCapture {
                alias_hits,
                text_hits,
            })
        })
}

#[test]
fn blob_search_index_simulation_replays_same_seed_across_restart() -> turmoil::Result {
    let first = run_indexed_search_simulation(0x4420)?;
    let second = run_indexed_search_simulation(0x4420)?;

    assert_eq!(first, second);
    assert_eq!(
        first.alias_hits,
        vec!["guide/latest".to_string(), "guide/reference".to_string()]
    );
    assert_eq!(first.text_hits.len(), 2);
    assert!(
        first
            .text_hits
            .iter()
            .all(|(alias, snippet)| alias.starts_with("guide/") && snippet.is_some())
    );
    Ok(())
}

struct SimulationHarness {
    path: &'static str,
    file_system: Arc<StubFileSystem>,
    db_object_store: Arc<StubObjectStore>,
    clock: Arc<StubClock>,
    blob_store: Arc<InMemoryBlobStore>,
}

impl SimulationHarness {
    fn new(path: &'static str) -> Self {
        Self {
            path,
            file_system: Arc::new(StubFileSystem::default()),
            db_object_store: Arc::new(StubObjectStore::default()),
            clock: Arc::new(StubClock::default()),
            blob_store: Arc::new(InMemoryBlobStore::new()),
        }
    }

    async fn open_collection(&self, create_if_missing: bool) -> TerracedbBlobCollection {
        let db = Db::open(
            DbConfig {
                storage: StorageConfig::Tiered(TieredStorageConfig {
                    ssd: terracedb::SsdConfig {
                        path: self.path.to_string(),
                    },
                    s3: S3Location {
                        bucket: "terracedb-bricks-test".to_string(),
                        prefix: "simulation".to_string(),
                    },
                    max_local_bytes: 1024 * 1024,
                    durability: TieredDurabilityMode::GroupCommit,
                    local_retention: terracedb::TieredLocalRetentionMode::Offload,
                }),
                hybrid_read: Default::default(),
                scheduler: None,
            },
            DbDependencies::new(
                self.file_system.clone(),
                self.db_object_store.clone(),
                self.clock.clone(),
                Arc::new(StubRng::seeded(17)),
            ),
        )
        .await
        .expect("open db");

        TerracedbBlobCollection::open(
            db,
            BlobCollectionConfig::new("docs")
                .expect("namespace")
                .with_create_if_missing(create_if_missing),
            self.blob_store.clone(),
        )
        .await
        .expect("open collection")
    }
}

#[derive(Clone)]
struct ExpectedBlob {
    alias: BlobAlias,
    payload: Vec<u8>,
    handle: BlobHandle,
}

#[tokio::test]
async fn seeded_reopen_state_machine_preserves_current_alias_state() {
    let harness = SimulationHarness::new("/terracedb-bricks-simulation");
    let mut collection = harness.open_collection(true).await;
    let aliases = [
        BlobAlias::new("guide/latest").expect("alias"),
        BlobAlias::new("guide/draft").expect("alias"),
        BlobAlias::new("notes/today").expect("alias"),
    ];
    let mut expected = BTreeMap::<String, ExpectedBlob>::new();

    for step in 0..36_u64 {
        if step > 0 && step % 6 == 0 {
            collection = harness.open_collection(false).await;
        }

        let alias = aliases[(step as usize) % aliases.len()].clone();
        match step % 4 {
            0 | 1 => {
                let previous = expected.get(alias.as_str()).cloned();
                let payload = format!("seed-{step}-{}", alias.as_str()).into_bytes();
                let handle = collection
                    .put(BlobWrite {
                        alias: Some(alias.clone()),
                        data: BlobWriteData::Bytes(payload.clone().into()),
                        content_type: Some("text/plain".to_string()),
                        tags: [("slot".to_string(), alias.as_str().to_string())]
                            .into_iter()
                            .collect(),
                        metadata: [("step".to_string(), serde_json::json!(step))]
                            .into_iter()
                            .collect(),
                    })
                    .await
                    .expect("put alias");

                if let Some(previous) = previous {
                    assert!(
                        collection
                            .stat(BlobLocator::Id(previous.handle.id))
                            .await
                            .expect("stat superseded id")
                            .is_none()
                    );
                }

                let read = collection
                    .get(
                        BlobLocator::Alias(alias.clone()),
                        BlobReadOptions::default(),
                    )
                    .await
                    .expect("get alias")
                    .data
                    .try_fold(Vec::new(), |mut acc, chunk| async move {
                        acc.extend_from_slice(&chunk);
                        Ok(acc)
                    })
                    .await
                    .expect("collect payload");
                assert_eq!(read, payload);

                expected.insert(
                    alias.as_str().to_string(),
                    ExpectedBlob {
                        alias,
                        payload,
                        handle,
                    },
                );
            }
            2 => {
                if let Some(previous) = expected.remove(alias.as_str()) {
                    collection
                        .delete(BlobLocator::Alias(alias.clone()))
                        .await
                        .expect("delete alias");
                    assert!(
                        collection
                            .stat(BlobLocator::Alias(alias.clone()))
                            .await
                            .expect("stat deleted alias")
                            .is_none()
                    );
                    assert!(
                        collection
                            .stat(BlobLocator::Id(previous.handle.id))
                            .await
                            .expect("stat deleted id")
                            .is_none()
                    );
                    assert!(
                        harness
                            .blob_store
                            .stat(&previous.handle.object_key)
                            .await
                            .expect("stat deferred object")
                            .is_some()
                    );
                }
            }
            _ => {
                for current in expected.values() {
                    let metadata = collection
                        .stat(BlobLocator::Alias(current.alias.clone()))
                        .await
                        .expect("stat current alias")
                        .expect("current alias should exist");
                    assert_eq!(metadata.id, current.handle.id);
                    let payload = collection
                        .get(
                            BlobLocator::Alias(current.alias.clone()),
                            BlobReadOptions::default(),
                        )
                        .await
                        .expect("get current alias")
                        .data
                        .try_fold(Vec::new(), |mut acc, chunk| async move {
                            acc.extend_from_slice(&chunk);
                            Ok(acc)
                        })
                        .await
                        .expect("collect current payload");
                    assert_eq!(payload, current.payload);
                }
            }
        }

        let search = collection
            .search(BlobQuery {
                alias_prefix: Some("guide/".to_string()),
                ..Default::default()
            })
            .await
            .expect("search current aliases")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect search");
        let expected_guide = expected
            .values()
            .filter(|blob| blob.alias.as_str().starts_with("guide/"))
            .count();
        assert_eq!(search.len(), expected_guide);
    }

    let reopened = harness.open_collection(false).await;
    for current in expected.values() {
        let metadata = reopened
            .stat(BlobLocator::Alias(current.alias.clone()))
            .await
            .expect("stat after final reopen")
            .expect("metadata after reopen");
        assert_eq!(metadata.id, current.handle.id);
    }
}
