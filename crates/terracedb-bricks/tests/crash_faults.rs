use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use futures::TryStreamExt;

use terracedb::{
    Clock, Db, DbConfig, Rng, S3Location, StorageConfig, TieredDurabilityMode, TieredStorageConfig,
};
use terracedb_bricks::{
    BlobAlias, BlobByteRange, BlobCollection, BlobCollectionConfig, BlobError,
    BlobExtractedTextQuery, BlobGcOptions, BlobLocator, BlobObjectLayout, BlobQuery,
    BlobReadOptions, BlobSearchRow, BlobStore, BlobStoreFailure, BlobStoreOperation, BlobWrite,
    BlobWriteData, InMemoryBlobStore, TerracedbBlobCollection, compute_blob_digest,
    read_blob_bytes,
};
use terracedb_simulation::{CutPoint, SeededSimulationRunner};

const ALIAS_CHOICES: [(&str, &str); 5] = [
    ("guide/latest", "guide"),
    ("guide/reference", "guide"),
    ("guide/archive", "guide"),
    ("notes/today", "note"),
    ("notes/tomorrow", "note"),
];
const PRIMARY_TERMS: [&str; 5] = ["atlas", "beacon", "cinder", "drift", "ember"];
const SECONDARY_TERMS: [&str; 4] = ["replay", "fault", "index", "gc"];
const BODY_SEARCH_TERMS: [&str; 10] = [
    "atlas",
    "beacon",
    "cinder",
    "drift",
    "ember",
    "deterministic",
    "replay",
    "fault",
    "index",
    "gc",
];

#[derive(Clone, Debug, PartialEq, Eq)]
struct ShadowBlob {
    alias: String,
    kind: String,
    title: String,
    body_terms: BTreeSet<String>,
    body: Vec<u8>,
    object_key: String,
    digest: String,
    size_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ShadowState {
    aliases: BTreeMap<String, ShadowBlob>,
    store_objects: BTreeMap<String, Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VisibleBlobSnapshot {
    alias: String,
    object_key: String,
    digest: String,
    size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CampaignOperation {
    Publish {
        alias: String,
        kind: String,
        title: String,
        body: String,
    },
    Read {
        alias: String,
        range: Option<(u64, u64)>,
    },
    MetadataSearch {
        prefix: String,
        kind: String,
        term: String,
    },
    TextSearch {
        term: String,
    },
    Delete {
        alias: String,
    },
    GarbageCollect,
    Restart,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CampaignFault {
    LostPutResponse {
        object_key: String,
    },
    ReadTimeout {
        object_key: String,
    },
    InterruptedRead {
        object_key: String,
        after_bytes: usize,
    },
    SearchInterruptedRead {
        object_key: String,
        after_bytes: usize,
    },
    StaleList {
        prefix: String,
    },
    DeleteTimeout {
        object_key: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StepOutcome {
    Published { alias: String, object_key: String },
    Read { alias: String, bytes: Vec<u8> },
    MetadataSearch { aliases: Vec<String> },
    TextSearch { term: String, aliases: Vec<String> },
    Deleted { alias: String },
    GarbageCollected { deleted_objects: usize },
    Restarted { aliases: Vec<String> },
    Failed { detail: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StepTrace {
    step: usize,
    operation: CampaignOperation,
    fault: Option<CampaignFault>,
    outcome: StepOutcome,
    visible: Vec<VisibleBlobSnapshot>,
    store_keys: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CampaignCapture {
    workload: Vec<CampaignOperation>,
    fault_schedule: Vec<(usize, CampaignFault)>,
    trace: Vec<StepTrace>,
    final_visible: Vec<VisibleBlobSnapshot>,
    final_store_keys: Vec<String>,
}

fn simulation_db_config(path: &str, durability: TieredDurabilityMode) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: terracedb::SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-bricks-test".to_string(),
                prefix: "crash-faults".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability,
            local_retention: terracedb::TieredLocalRetentionMode::Offload,
        }),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

fn collection_config(create_if_missing: bool) -> BlobCollectionConfig {
    BlobCollectionConfig::new("docs")
        .expect("valid namespace")
        .with_create_if_missing(create_if_missing)
        .with_plain_text_extraction()
}

async fn open_collection(
    db: Db,
    create_if_missing: bool,
    blob_store: Arc<InMemoryBlobStore>,
) -> TerracedbBlobCollection {
    TerracedbBlobCollection::open(db, collection_config(create_if_missing), blob_store)
        .await
        .expect("open terracedb-bricks collection")
}

fn normalize_term(term: &str) -> Option<String> {
    let normalized = term.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn tokenize_terms(text: &str) -> BTreeSet<String> {
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(normalize_term)
        .collect()
}

fn alias_pool() -> Vec<String> {
    ALIAS_CHOICES
        .iter()
        .map(|(alias, _kind)| (*alias).to_string())
        .collect()
}

fn choose_index(roll: u64, bound: usize) -> usize {
    (roll as usize) % bound.max(1)
}

fn expected_visible_snapshots(state: &ShadowState) -> Vec<VisibleBlobSnapshot> {
    let mut visible = state
        .aliases
        .values()
        .map(|blob| VisibleBlobSnapshot {
            alias: blob.alias.clone(),
            object_key: blob.object_key.clone(),
            digest: blob.digest.clone(),
            size_bytes: blob.size_bytes,
        })
        .collect::<Vec<_>>();
    visible.sort_by(|left, right| left.alias.cmp(&right.alias));
    visible
}

fn expected_metadata_hits(
    state: &ShadowState,
    prefix: &str,
    kind: &str,
    term: &str,
) -> Vec<String> {
    let term = term.to_ascii_lowercase();
    let mut hits = state
        .aliases
        .values()
        .filter(|blob| blob.alias.starts_with(prefix))
        .filter(|blob| blob.kind == kind)
        .filter(|blob| {
            tokenize_terms(&blob.alias).contains(&term)
                || tokenize_terms(&blob.title).contains(&term)
        })
        .map(|blob| blob.alias.clone())
        .collect::<Vec<_>>();
    hits.sort();
    hits
}

fn expected_text_hits(state: &ShadowState, term: &str) -> Vec<String> {
    let term = term.to_ascii_lowercase();
    let mut hits = state
        .aliases
        .values()
        .filter(|blob| blob.body_terms.contains(&term))
        .map(|blob| blob.alias.clone())
        .collect::<Vec<_>>();
    hits.sort();
    hits
}

async fn collect_rows(
    collection: &TerracedbBlobCollection,
    query: BlobQuery,
) -> Result<Vec<BlobSearchRow>, BlobError> {
    collection
        .search(query)
        .await?
        .try_collect::<Vec<_>>()
        .await
}

async fn collect_visible_snapshots(
    collection: &TerracedbBlobCollection,
) -> Result<Vec<VisibleBlobSnapshot>, BlobError> {
    let mut visible = collect_rows(collection, BlobQuery::default())
        .await?
        .into_iter()
        .map(|row| VisibleBlobSnapshot {
            alias: row
                .metadata
                .alias
                .expect("visible blob rows should preserve aliases")
                .to_string(),
            object_key: row.metadata.object_key,
            digest: row.metadata.digest,
            size_bytes: row.metadata.size_bytes,
        })
        .collect::<Vec<_>>();
    visible.sort_by(|left, right| left.alias.cmp(&right.alias));
    Ok(visible)
}

async fn collect_metadata_hits(
    collection: &TerracedbBlobCollection,
    prefix: &str,
    kind: &str,
    term: &str,
) -> Result<Vec<String>, BlobError> {
    let mut hits = collect_rows(
        collection,
        BlobQuery {
            alias_prefix: Some(prefix.to_string()),
            required_tags: [("kind".to_string(), kind.to_string())]
                .into_iter()
                .collect(),
            terms: vec![term.to_string()],
            ..Default::default()
        },
    )
    .await?
    .into_iter()
    .map(|row| {
        row.metadata
            .alias
            .expect("metadata query rows should keep aliases")
            .to_string()
    })
    .collect::<Vec<_>>();
    hits.sort();
    Ok(hits)
}

async fn collect_text_hits(
    collection: &TerracedbBlobCollection,
    term: &str,
) -> Result<Vec<String>, BlobError> {
    let mut hits = collect_rows(
        collection,
        BlobQuery {
            extracted_text: Some(BlobExtractedTextQuery::plain_text([term])),
            ..Default::default()
        },
    )
    .await?
    .into_iter()
    .map(|row| {
        row.metadata
            .alias
            .expect("text query rows should keep aliases")
            .to_string()
    })
    .collect::<Vec<_>>();
    hits.sort();
    Ok(hits)
}

async fn read_alias(
    collection: &TerracedbBlobCollection,
    alias: &str,
    range: Option<(u64, u64)>,
) -> Result<Vec<u8>, BlobError> {
    let read = collection
        .get(
            BlobLocator::Alias(BlobAlias::new(alias).expect("alias")),
            BlobReadOptions {
                range: range.map(|(start, end)| BlobByteRange::new(start, end).expect("range")),
            },
        )
        .await?;
    read.data
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
}

async fn list_object_keys(blob_store: &InMemoryBlobStore) -> Result<Vec<String>, BlobError> {
    let prefix = BlobObjectLayout::new("", "docs")
        .expect("layout")
        .object_prefix();
    let mut keys = blob_store
        .list_prefix(&prefix)
        .await?
        .into_iter()
        .map(|info| info.key)
        .collect::<Vec<_>>();
    keys.sort();
    Ok(keys)
}

async fn assert_shadow_matches(
    collection: &TerracedbBlobCollection,
    blob_store: &InMemoryBlobStore,
    state: &ShadowState,
    step: usize,
) {
    let visible = collect_visible_snapshots(collection)
        .await
        .expect("collect visible snapshots");
    assert_eq!(visible, expected_visible_snapshots(state));

    for blob in state.aliases.values() {
        let metadata = collection
            .stat(BlobLocator::Alias(
                BlobAlias::new(blob.alias.clone()).expect("alias"),
            ))
            .await
            .expect("stat alias")
            .expect("shadow alias should exist");
        assert_eq!(metadata.object_key, blob.object_key);
        assert_eq!(metadata.digest, blob.digest);
        assert_eq!(metadata.size_bytes, blob.size_bytes);
    }

    for alias in alias_pool() {
        if state.aliases.contains_key(&alias) {
            continue;
        }
        assert!(
            collection
                .stat(BlobLocator::Alias(BlobAlias::new(alias).expect("alias")))
                .await
                .expect("stat missing alias")
                .is_none()
        );
    }

    let guide_term = PRIMARY_TERMS[step % PRIMARY_TERMS.len()];
    let note_term = PRIMARY_TERMS[(step + 2) % PRIMARY_TERMS.len()];
    let text_term = BODY_SEARCH_TERMS[step % BODY_SEARCH_TERMS.len()];

    assert_eq!(
        collect_metadata_hits(collection, "guide/", "guide", guide_term)
            .await
            .expect("collect guide metadata hits"),
        expected_metadata_hits(state, "guide/", "guide", guide_term),
        "guide metadata search should match the shadow state at step {step}"
    );
    assert_eq!(
        collect_metadata_hits(collection, "notes/", "note", note_term)
            .await
            .expect("collect note metadata hits"),
        expected_metadata_hits(state, "notes/", "note", note_term),
        "note metadata search should match the shadow state at step {step}"
    );
    assert_eq!(
        collect_text_hits(collection, text_term)
            .await
            .expect("collect extracted-text hits"),
        expected_text_hits(state, text_term),
        "text search should match the shadow state at step {step}"
    );

    let mut expected_store_keys = state.store_objects.keys().cloned().collect::<Vec<_>>();
    expected_store_keys.sort();
    assert_eq!(
        list_object_keys(blob_store)
            .await
            .expect("list blob-store keys"),
        expected_store_keys,
        "blob-store contents should match the shadow state at step {step}"
    );
}

fn format_put_body(kind: &str, primary: &str, secondary: &str) -> String {
    format!("{kind} {primary} deterministic {secondary}")
}

fn describe_error(error: &BlobError) -> String {
    error.to_string()
}

fn unreferenced_object_keys(state: &ShadowState) -> Vec<String> {
    let live = state
        .aliases
        .values()
        .map(|blob| blob.object_key.clone())
        .collect::<BTreeSet<_>>();
    state
        .store_objects
        .keys()
        .filter(|key| !live.contains(*key))
        .cloned()
        .collect()
}

fn choose_existing_alias(state: &ShadowState, roll: u64) -> String {
    let aliases = state.aliases.keys().cloned().collect::<Vec<_>>();
    aliases[choose_index(roll, aliases.len())].clone()
}

fn choose_term_from_prefix(state: &ShadowState, prefix: &str, fallback_index: usize) -> String {
    let matching = state
        .aliases
        .values()
        .filter(|blob| blob.alias.starts_with(prefix))
        .map(|blob| {
            tokenize_terms(&blob.title)
                .into_iter()
                .find(|term| PRIMARY_TERMS.contains(&term.as_str()))
                .unwrap_or_else(|| PRIMARY_TERMS[fallback_index % PRIMARY_TERMS.len()].to_string())
        })
        .collect::<Vec<_>>();
    matching
        .first()
        .cloned()
        .unwrap_or_else(|| PRIMARY_TERMS[fallback_index % PRIMARY_TERMS.len()].to_string())
}

fn choose_text_term(state: &ShadowState, fallback_index: usize) -> String {
    let available = state
        .aliases
        .values()
        .flat_map(|blob| blob.body_terms.iter().cloned())
        .filter(|term| BODY_SEARCH_TERMS.contains(&term.as_str()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    available
        .get(fallback_index % available.len().max(1))
        .cloned()
        .unwrap_or_else(|| BODY_SEARCH_TERMS[fallback_index % BODY_SEARCH_TERMS.len()].to_string())
}

fn run_fault_campaign(seed: u64) -> turmoil::Result<CampaignCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_secs(3))
        .run_with(move |context| async move {
            let config = simulation_db_config(
                &format!("/terracedb-bricks-campaign-{seed}"),
                TieredDurabilityMode::GroupCommit,
            );
            let blob_store = Arc::new(InMemoryBlobStore::new());
            let layout = BlobObjectLayout::new("", "docs").expect("layout");

            let mut db = context.open_db(config.clone()).await.expect("open db");
            let mut collection = open_collection(db.clone(), true, blob_store.clone()).await;
            let mut state = ShadowState::default();
            let mut workload = Vec::new();
            let mut fault_schedule = Vec::new();
            let mut trace = Vec::new();
            let mut publish_round = 0_usize;
            let mut read_round = 0_usize;
            let mut text_round = 0_usize;
            let mut gc_round = 0_usize;

            for step in 0..12_usize {
                context
                    .clock()
                    .sleep(Duration::from_millis((context.rng().next_u64() % 3) + 1))
                    .await;

                if step > 0 && step % 6 == 0 {
                    db.flush().await.expect("flush before restart");
                    db = context
                        .restart_db(config.clone(), CutPoint::AfterDurabilityBoundary)
                        .await
                        .expect("restart db");
                    collection = open_collection(db.clone(), false, blob_store.clone()).await;
                    assert_shadow_matches(&collection, blob_store.as_ref(), &state, step).await;

                    let operation = CampaignOperation::Restart;
                    let visible = collect_visible_snapshots(&collection)
                        .await
                        .expect("collect visible snapshots after restart");
                    let aliases = visible
                        .iter()
                        .map(|row| row.alias.clone())
                        .collect::<Vec<_>>();
                    let store_keys = list_object_keys(blob_store.as_ref())
                        .await
                        .expect("list object keys after restart");
                    workload.push(operation.clone());
                    trace.push(StepTrace {
                        step,
                        operation,
                        fault: None,
                        outcome: StepOutcome::Restarted { aliases },
                        visible,
                        store_keys,
                    });
                    continue;
                }

                let op_code = if state.aliases.is_empty() {
                    0
                } else {
                    (step % 6) as u64
                };
                let mut fault = None;

                let (operation, outcome) = match op_code {
                    0 | 1 => {
                        let (alias, kind) = ALIAS_CHOICES
                            [choose_index(context.rng().next_u64(), ALIAS_CHOICES.len())];
                        let primary = PRIMARY_TERMS
                            [choose_index(context.rng().next_u64(), PRIMARY_TERMS.len())];
                        let secondary = SECONDARY_TERMS
                            [choose_index(context.rng().next_u64(), SECONDARY_TERMS.len())];
                        let body = format_put_body(kind, primary, secondary);
                        let title = format!("{kind} {primary} title");
                        let digest = compute_blob_digest(body.as_bytes());
                        let object_key = layout
                            .content_addressed_key(&digest)
                            .expect("content-addressed key");

                        if publish_round.is_multiple_of(2) {
                            blob_store.inject_failure(BlobStoreFailure::lost_put_response(
                                object_key.clone(),
                            ));
                            fault = Some(CampaignFault::LostPutResponse {
                                object_key: object_key.clone(),
                            });
                        }
                        publish_round += 1;

                        let metadata = [("title".to_string(), serde_json::json!(title.clone()))]
                            .into_iter()
                            .collect::<BTreeMap<_, _>>();
                        let handle = collection
                            .put(BlobWrite {
                                alias: Some(BlobAlias::new(alias).expect("alias")),
                                data: BlobWriteData::bytes(body.as_bytes().to_vec()),
                                content_type: Some("text/plain".to_string()),
                                tags: [("kind".to_string(), kind.to_string())]
                                    .into_iter()
                                    .collect(),
                                metadata,
                            })
                            .await
                            .expect("publish blob");

                        state
                            .store_objects
                            .entry(handle.object_key.clone())
                            .or_insert_with(|| body.as_bytes().to_vec());
                        state.aliases.insert(
                            alias.to_string(),
                            ShadowBlob {
                                alias: alias.to_string(),
                                kind: kind.to_string(),
                                title: title.clone(),
                                body_terms: tokenize_terms(&body),
                                body: body.as_bytes().to_vec(),
                                object_key: handle.object_key.clone(),
                                digest: handle.digest,
                                size_bytes: handle.size_bytes,
                            },
                        );

                        (
                            CampaignOperation::Publish {
                                alias: alias.to_string(),
                                kind: kind.to_string(),
                                title,
                                body,
                            },
                            StepOutcome::Published {
                                alias: alias.to_string(),
                                object_key: handle.object_key,
                            },
                        )
                    }
                    2 => {
                        let alias = choose_existing_alias(&state, context.rng().next_u64());
                        let blob = state.aliases.get(&alias).expect("existing alias").clone();
                        let range =
                            if blob.body.len() > 4 && context.rng().next_u64().is_multiple_of(2) {
                                let start = 1_u64;
                                let end = (blob.body.len() as u64).saturating_sub(1);
                                Some((start, end))
                            } else {
                                None
                            };

                        match read_round % 3 {
                            0 => {
                                blob_store.inject_failure(BlobStoreFailure::timeout(
                                    BlobStoreOperation::Get,
                                    blob.object_key.clone(),
                                ));
                                fault = Some(CampaignFault::ReadTimeout {
                                    object_key: blob.object_key.clone(),
                                });
                            }
                            1 => {
                                let after_bytes = (blob.body.len() / 2).max(1);
                                blob_store.inject_failure(BlobStoreFailure::interrupted_read(
                                    BlobStoreOperation::Get,
                                    blob.object_key.clone(),
                                    after_bytes,
                                ));
                                fault = Some(CampaignFault::InterruptedRead {
                                    object_key: blob.object_key.clone(),
                                    after_bytes,
                                });
                            }
                            _ => {}
                        }
                        read_round += 1;

                        match read_alias(&collection, &alias, range).await {
                            Ok(bytes) => {
                                let expected = if let Some((start, end)) = range {
                                    blob.body[start as usize..end as usize].to_vec()
                                } else {
                                    blob.body.clone()
                                };
                                assert_eq!(bytes, expected);
                                (
                                    CampaignOperation::Read { alias, range },
                                    StepOutcome::Read {
                                        alias: blob.alias,
                                        bytes,
                                    },
                                )
                            }
                            Err(error) => (
                                CampaignOperation::Read { alias, range },
                                StepOutcome::Failed {
                                    detail: describe_error(&error),
                                },
                            ),
                        }
                    }
                    3 => {
                        let prefix = if context.rng().next_u64().is_multiple_of(2) {
                            "guide/"
                        } else {
                            "notes/"
                        };
                        let kind = if prefix == "guide/" { "guide" } else { "note" };
                        let term = choose_term_from_prefix(&state, prefix, step);
                        let hits = collect_metadata_hits(&collection, prefix, kind, &term)
                            .await
                            .expect("metadata search");
                        assert_eq!(hits, expected_metadata_hits(&state, prefix, kind, &term));
                        (
                            CampaignOperation::MetadataSearch {
                                prefix: prefix.to_string(),
                                kind: kind.to_string(),
                                term: term.clone(),
                            },
                            StepOutcome::MetadataSearch { aliases: hits },
                        )
                    }
                    4 => {
                        let term = choose_text_term(&state, step);
                        if text_round.is_multiple_of(2) {
                            let alias = choose_existing_alias(&state, context.rng().next_u64());
                            let blob = state.aliases.get(&alias).expect("existing alias");
                            let after_bytes = (blob.body.len() / 2).max(1);
                            blob_store.inject_failure(BlobStoreFailure::interrupted_read(
                                BlobStoreOperation::Get,
                                blob.object_key.clone(),
                                after_bytes,
                            ));
                            fault = Some(CampaignFault::SearchInterruptedRead {
                                object_key: blob.object_key.clone(),
                                after_bytes,
                            });
                        }
                        text_round += 1;

                        match collect_text_hits(&collection, &term).await {
                            Ok(hits) => {
                                assert_eq!(hits, expected_text_hits(&state, &term));
                                (
                                    CampaignOperation::TextSearch { term: term.clone() },
                                    StepOutcome::TextSearch {
                                        term,
                                        aliases: hits,
                                    },
                                )
                            }
                            Err(error) => (
                                CampaignOperation::TextSearch { term },
                                StepOutcome::Failed {
                                    detail: describe_error(&error),
                                },
                            ),
                        }
                    }
                    5 => {
                        let maybe_unreferenced = unreferenced_object_keys(&state);
                        if !maybe_unreferenced.is_empty() {
                            let target = maybe_unreferenced[0].clone();
                            let opts = match gc_round % 3 {
                                0 => {
                                    let prefix = layout.object_prefix();
                                    blob_store.inject_failure(BlobStoreFailure::stale_list(
                                        prefix.clone(),
                                    ));
                                    fault = Some(CampaignFault::StaleList {
                                        prefix: prefix.clone(),
                                    });
                                    BlobGcOptions::default()
                                }
                                1 => {
                                    blob_store.inject_failure(BlobStoreFailure::timeout(
                                        BlobStoreOperation::Delete,
                                        target.clone(),
                                    ));
                                    fault = Some(CampaignFault::DeleteTimeout {
                                        object_key: target.clone(),
                                    });
                                    BlobGcOptions {
                                        max_objects: Some(1),
                                        ..Default::default()
                                    }
                                }
                                _ => BlobGcOptions::default(),
                            };
                            gc_round += 1;

                            match collection.collect_garbage(opts).await {
                                Ok(gc) => {
                                    for key in unreferenced_object_keys(&state) {
                                        state.store_objects.remove(&key);
                                    }
                                    (
                                        CampaignOperation::GarbageCollect,
                                        StepOutcome::GarbageCollected {
                                            deleted_objects: gc.deleted_objects,
                                        },
                                    )
                                }
                                Err(error) => (
                                    CampaignOperation::GarbageCollect,
                                    StepOutcome::Failed {
                                        detail: describe_error(&error),
                                    },
                                ),
                            }
                        } else {
                            let alias = choose_existing_alias(&state, context.rng().next_u64());
                            collection
                                .delete(BlobLocator::Alias(
                                    BlobAlias::new(alias.clone()).expect("alias"),
                                ))
                                .await
                                .expect("delete alias");
                            state.aliases.remove(&alias);
                            (
                                CampaignOperation::Delete {
                                    alias: alias.clone(),
                                },
                                StepOutcome::Deleted { alias },
                            )
                        }
                    }
                    _ => unreachable!(),
                };

                workload.push(operation.clone());
                if let Some(applied) = fault.clone() {
                    fault_schedule.push((step, applied));
                }

                if let Some(get_fault_target) = match fault.as_ref() {
                    Some(CampaignFault::ReadTimeout { object_key })
                    | Some(CampaignFault::InterruptedRead { object_key, .. })
                    | Some(CampaignFault::SearchInterruptedRead { object_key, .. }) => {
                        Some(object_key)
                    }
                    _ => None,
                } {
                    let _ =
                        read_blob_bytes(blob_store.as_ref(), get_fault_target, Default::default())
                            .await;
                }

                assert_shadow_matches(&collection, blob_store.as_ref(), &state, step).await;
                let visible = collect_visible_snapshots(&collection)
                    .await
                    .expect("collect visible snapshots");
                let store_keys = list_object_keys(blob_store.as_ref())
                    .await
                    .expect("list object keys");
                trace.push(StepTrace {
                    step,
                    operation,
                    fault,
                    outcome,
                    visible,
                    store_keys,
                });
            }

            let final_visible = collect_visible_snapshots(&collection)
                .await
                .expect("collect final visible state");
            let final_store_keys = list_object_keys(blob_store.as_ref())
                .await
                .expect("list final store keys");

            Ok(CampaignCapture {
                workload,
                fault_schedule,
                trace,
                final_visible,
                final_store_keys,
            })
        })
}

#[test]
fn bricks_fault_campaign_replays_same_seed_with_the_same_workload_faults_and_trace()
-> turmoil::Result {
    let first = run_fault_campaign(0x46_01)?;
    let second = run_fault_campaign(0x46_01)?;

    assert_eq!(first, second);
    assert!(
        first
            .fault_schedule
            .iter()
            .any(|(_, fault)| matches!(fault, CampaignFault::LostPutResponse { .. })),
        "the deterministic campaign should exercise lost upload responses"
    );
    assert!(
        first
            .fault_schedule
            .iter()
            .any(|(_, fault)| matches!(fault, CampaignFault::StaleList { .. })),
        "the deterministic campaign should exercise stale LIST faults"
    );
    Ok(())
}

#[test]
fn bricks_fault_campaign_seed_sweep_preserves_visible_metadata_object_state_and_search_results()
-> turmoil::Result {
    let mut distinct_workloads = BTreeSet::new();
    for seed in [0x46_10, 0x46_11] {
        let capture = run_fault_campaign(seed)?;
        assert!(
            !capture.trace.is_empty(),
            "seed {seed:x} should produce a non-empty deterministic trace"
        );
        distinct_workloads.insert(format!("{:?}", capture.workload));
    }
    assert!(
        distinct_workloads.len() > 1,
        "different seeds should produce different deterministic workloads"
    );
    Ok(())
}

#[test]
fn deferred_recovery_rewinds_to_the_last_flushed_prefix_and_reclaims_orphans() -> turmoil::Result {
    SeededSimulationRunner::new(0x46_ff).run_with(|context| async move {
        let config = simulation_db_config(
            "/terracedb-bricks-deferred-recovery",
            TieredDurabilityMode::Deferred,
        );
        let blob_store = Arc::new(InMemoryBlobStore::new());

        let mut db = context.open_db(config.clone()).await.expect("open db");
        let mut collection = open_collection(db.clone(), true, blob_store.clone()).await;

        let persisted_alias = BlobAlias::new("guide/persisted").expect("alias");
        let volatile_alias = BlobAlias::new("guide/volatile").expect("alias");

        let persisted = collection
            .put(BlobWrite {
                alias: Some(persisted_alias.clone()),
                data: BlobWriteData::bytes("guide atlas deterministic replay"),
                content_type: Some("text/plain".to_string()),
                tags: [("kind".to_string(), "guide".to_string())]
                    .into_iter()
                    .collect(),
                metadata: [("title".to_string(), serde_json::json!("guide atlas title"))]
                    .into_iter()
                    .collect(),
            })
            .await
            .expect("publish persisted blob");
        db.flush().await.expect("flush persisted metadata");

        assert_eq!(
            collect_visible_snapshots(&collection)
                .await
                .expect("collect flushed state")
                .into_iter()
                .map(|row| row.alias)
                .collect::<Vec<_>>(),
            vec!["guide/persisted".to_string()]
        );

        let volatile = collection
            .put(BlobWrite {
                alias: Some(volatile_alias.clone()),
                data: BlobWriteData::bytes("guide cinder deterministic fault"),
                content_type: Some("text/plain".to_string()),
                tags: [("kind".to_string(), "guide".to_string())]
                    .into_iter()
                    .collect(),
                metadata: [("title".to_string(), serde_json::json!("guide cinder title"))]
                    .into_iter()
                    .collect(),
            })
            .await
            .expect("publish volatile blob");
        collection
            .delete(BlobLocator::Alias(persisted_alias.clone()))
            .await
            .expect("delete persisted alias before crash");

        assert!(
            collection
                .stat(BlobLocator::Alias(volatile_alias.clone()))
                .await
                .expect("stat volatile alias before crash")
                .is_some()
        );
        assert!(
            collection
                .stat(BlobLocator::Alias(persisted_alias.clone()))
                .await
                .expect("stat persisted alias before crash")
                .is_none()
        );
        assert_eq!(
            read_alias(&collection, "guide/volatile", None)
                .await
                .expect("read volatile blob before crash"),
            b"guide cinder deterministic fault"
        );

        db = context
            .restart_db(config.clone(), CutPoint::AfterStep)
            .await
            .expect("restart after crash");
        collection = open_collection(db.clone(), false, blob_store.clone()).await;

        let recovered_aliases = collect_visible_snapshots(&collection)
            .await
            .expect("collect recovered state")
            .into_iter()
            .map(|row| row.alias)
            .collect::<Vec<_>>();
        assert_eq!(recovered_aliases, vec!["guide/persisted".to_string()]);
        assert_eq!(
            collect_text_hits(&collection, "atlas")
                .await
                .expect("search recovered text state"),
            vec!["guide/persisted".to_string()]
        );
        assert!(
            collection
                .stat(BlobLocator::Alias(volatile_alias.clone()))
                .await
                .expect("stat volatile alias after crash")
                .is_none()
        );
        assert!(
            blob_store
                .stat(&volatile.object_key)
                .await
                .expect("stat volatile object after crash")
                .is_some(),
            "the unflushed publish should survive as an orphan object until GC"
        );

        let gc = collection
            .collect_garbage(BlobGcOptions::default())
            .await
            .expect("sweep orphaned volatile object");
        assert_eq!(gc.deleted_objects, 1);
        assert!(
            blob_store
                .stat(&volatile.object_key)
                .await
                .expect("stat volatile object after gc")
                .is_none()
        );
        assert!(
            blob_store
                .stat(&persisted.object_key)
                .await
                .expect("stat persisted object after gc")
                .is_some()
        );
        Ok(())
    })
}
