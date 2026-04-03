use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde_json::json;
use terracedb::Clock;
use terracedb_fuzz::{GeneratedScenarioHarness, assert_seed_replays, assert_seed_variation};
use terracedb_git::worktree::GitWorktreeMaterializer;
use terracedb_git::{
    DeterministicGitRepositoryStore, GitCancellationToken, GitCheckoutRequest, GitDiscoverRequest,
    GitForkPolicy, GitObjectDatabase, GitObjectFormat, GitOpenRequest, GitRepositoryImage,
    GitRepositoryPolicy, GitRepositoryProvenance, GitRepositoryStore, GitSubstrateError,
    NeverCancel, VfsGitRepositoryImage,
};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{
    CloneVolumeSource, CreateOptions, InMemoryVfsStore, SnapshotOptions, Volume, VolumeConfig,
    VolumeId, VolumeStore,
};

struct GitSeedHarness<T> {
    run: fn(u64) -> turmoil::Result<T>,
}

impl<T> GeneratedScenarioHarness for GitSeedHarness<T>
where
    T: Clone + std::fmt::Debug + PartialEq + Eq,
{
    type Scenario = u64;
    type Outcome = T;
    type Error = Box<dyn std::error::Error>;

    fn generate(&self, seed: u64) -> Self::Scenario {
        seed
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        (self.run)(scenario)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GitSimulationCapture {
    snapshot_root: String,
    overlay_root: String,
    imported_root: String,
    snapshot_head_oid: Option<String>,
    overlay_head_oid: Option<String>,
    imported_head_oid: Option<String>,
    ref_names: Vec<String>,
    imported_payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GitCheckoutCancellationCapture {
    delay_millis: u64,
    cancelled_repository_id: String,
}

async fn seed_repository(volume: Arc<dyn Volume>, root: &str, oid: &str, payload: Vec<u8>) {
    let blob_oid = blob_oid_for_commit(oid);
    let tree_oid = tree_oid_for_commit(oid);
    let source = source_bytes_for_commit(oid);
    volume
        .fs()
        .write_file(
            &format!("{root}/.git/HEAD"),
            b"ref: refs/heads/main\n".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write git head");
    volume
        .fs()
        .write_file(
            &format!("{root}/.git/refs/heads/main"),
            format!("{oid}\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write main ref");
    volume
        .fs()
        .write_file(
            &format!("{root}/.git/objects/{oid}"),
            [
                b"commit\n".as_slice(),
                format!("tree {tree_oid}\n").as_bytes(),
                payload.as_slice(),
            ]
            .concat(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write object");
    volume
        .fs()
        .write_file(
            &format!("{root}/.git/objects/{blob_oid}"),
            [b"blob\n".as_slice(), source.as_slice()].concat(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write blob object");
    volume
        .fs()
        .write_file(
            &format!("{root}/.git/objects/{tree_oid}"),
            format!("tree\n100644 blob {blob_oid}\tsrc/lib.rs\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write tree object");
    volume
        .fs()
        .write_file(
            &format!("{root}/src/lib.rs"),
            source,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write source file");
}

fn blob_oid_for_commit(oid: &str) -> String {
    format!("{oid}-blob")
}

fn tree_oid_for_commit(oid: &str) -> String {
    format!("{oid}-tree")
}

fn source_bytes_for_commit(oid: &str) -> Vec<u8> {
    format!("export const oid = \"{oid}\";\n").into_bytes()
}

fn open_request(repository_id: &str, image: &dyn GitRepositoryImage) -> GitOpenRequest {
    let descriptor = image.descriptor();
    GitOpenRequest {
        repository_id: repository_id.to_string(),
        repository_image: descriptor.clone(),
        policy: GitRepositoryPolicy::default(),
        provenance: GitRepositoryProvenance {
            backend: "deterministic-git".to_string(),
            repo_root: descriptor.root_path.clone(),
            origin: terracedb_git::GitRepositoryOrigin::Native,
            remote_url: None,
            object_format: GitObjectFormat::Sha256,
            volume_id: descriptor.volume_id,
            snapshot_sequence: descriptor.snapshot_sequence,
            durable_snapshot: descriptor.durable_snapshot,
            fork_policy: GitForkPolicy::simulation_native_baseline(),
        },
        metadata: BTreeMap::from([("seed".to_string(), json!(repository_id))]),
    }
}

fn run_git_simulation(seed: u64) -> turmoil::Result<GitSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(40))
        .run_with(move |context| async move {
            let store = Arc::new(InMemoryVfsStore::new(context.clock(), context.rng()));
            let volume = store
                .open_volume(
                    VolumeConfig::new(VolumeId::new(0x9400 + seed as u128))
                        .with_chunk_size(4096)
                        .with_create_if_missing(true),
                )
                .await
                .expect("open git volume");
            let base_oid = format!("{:08x}", seed ^ 0x3333_3333);
            seed_repository(
                volume.clone(),
                "/repo",
                &base_oid,
                format!("snapshot-{seed:x}\n").into_bytes(),
            )
            .await;

            let repo_store = DeterministicGitRepositoryStore::default();
            let snapshot_image = Arc::new(
                VfsGitRepositoryImage::from_volume(
                    volume.clone(),
                    "/repo",
                    SnapshotOptions::default(),
                )
                .await
                .expect("snapshot git image"),
            );
            let snapshot_root = repo_store
                .discover(
                    snapshot_image.clone(),
                    GitDiscoverRequest {
                        start_path: "/repo/src".to_string(),
                        policy: GitRepositoryPolicy::default(),
                        metadata: BTreeMap::new(),
                    },
                    Arc::new(NeverCancel),
                )
                .await
                .expect("discover snapshot repo")
                .repository_root;
            let snapshot_repo = repo_store
                .open(
                    snapshot_image.clone(),
                    open_request("snapshot", snapshot_image.as_ref()),
                    Arc::new(NeverCancel),
                )
                .await
                .expect("open snapshot repo");
            let snapshot_head_oid = snapshot_repo.head().await.expect("snapshot head").oid;

            let overlay = store
                .create_overlay(
                    volume
                        .snapshot(SnapshotOptions::default())
                        .await
                        .expect("base snapshot"),
                    VolumeConfig::new(VolumeId::new(0x9500 + seed as u128))
                        .with_chunk_size(4096)
                        .with_create_if_missing(true),
                )
                .await
                .expect("create overlay");
            let overlay_oid = format!("{:08x}", seed ^ 0x4444_4444);
            seed_repository(
                overlay.clone(),
                "/repo",
                &overlay_oid,
                format!("overlay-{seed:x}\n").into_bytes(),
            )
            .await;
            let overlay_image = Arc::new(
                VfsGitRepositoryImage::from_volume(
                    overlay.clone(),
                    "/repo",
                    SnapshotOptions::default(),
                )
                .await
                .expect("overlay git image"),
            );
            let overlay_root = repo_store
                .discover(
                    overlay_image.clone(),
                    GitDiscoverRequest {
                        start_path: "src".to_string(),
                        policy: GitRepositoryPolicy::default(),
                        metadata: BTreeMap::new(),
                    },
                    Arc::new(NeverCancel),
                )
                .await
                .expect("discover overlay repo")
                .repository_root;
            let overlay_repo = repo_store
                .open(
                    overlay_image.clone(),
                    open_request("overlay", overlay_image.as_ref()),
                    Arc::new(NeverCancel),
                )
                .await
                .expect("open overlay repo");
            let overlay_head_oid = overlay_repo.head().await.expect("overlay head").oid;
            let ref_names = overlay_repo
                .list_refs()
                .await
                .expect("overlay refs")
                .into_iter()
                .map(|reference| reference.name)
                .collect::<Vec<_>>();

            let imported_store = Arc::new(InMemoryVfsStore::new(context.clock(), context.rng()));
            let imported = imported_store
                .import_volume(
                    store
                        .export_volume(CloneVolumeSource::new(overlay.info().volume_id))
                        .await
                        .expect("export overlay repo"),
                    VolumeConfig::new(VolumeId::new(0x9600 + seed as u128))
                        .with_chunk_size(4096)
                        .with_create_if_missing(true),
                )
                .await
                .expect("import overlay repo");
            let imported_image = Arc::new(
                VfsGitRepositoryImage::from_volume(imported, "/repo", SnapshotOptions::default())
                    .await
                    .expect("imported git image"),
            );
            let imported_root = repo_store
                .discover(
                    imported_image.clone(),
                    GitDiscoverRequest {
                        start_path: "/repo/src".to_string(),
                        policy: GitRepositoryPolicy::default(),
                        metadata: BTreeMap::new(),
                    },
                    Arc::new(NeverCancel),
                )
                .await
                .expect("discover imported repo")
                .repository_root;
            let imported_repo = repo_store
                .open(
                    imported_image.clone(),
                    open_request("imported", imported_image.as_ref()),
                    Arc::new(NeverCancel),
                )
                .await
                .expect("open imported repo");
            let imported_head_oid = imported_repo.head().await.expect("imported head").oid;
            let imported_payload =
                GitObjectDatabase::read_object(imported_repo.as_ref(), &overlay_oid)
                    .await
                    .expect("read imported object")
                    .expect("imported object exists")
                    .data;

            Ok(GitSimulationCapture {
                snapshot_root,
                overlay_root,
                imported_root,
                snapshot_head_oid,
                overlay_head_oid,
                imported_head_oid,
                ref_names,
                imported_payload,
            })
        })
}

#[derive(Clone, Default)]
struct ToggleCancellation {
    cancelled: Arc<AtomicBool>,
}

impl ToggleCancellation {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }
}

impl GitCancellationToken for ToggleCancellation {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
struct SimulatedDelayMaterializer {
    clock: Arc<dyn Clock>,
    delay: Duration,
}

#[async_trait]
impl GitWorktreeMaterializer for SimulatedDelayMaterializer {
    async fn materialize(
        &self,
        repository_id: &str,
        request: GitCheckoutRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<terracedb_git::GitCheckoutReport, GitSubstrateError> {
        self.clock.sleep(self.delay).await;
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: repository_id.to_string(),
            });
        }
        Ok(terracedb_git::GitCheckoutReport {
            target_ref: request.target_ref,
            materialized_path: request.materialize_path,
            written_paths: 0,
            deleted_paths: 0,
            head_oid: None,
        })
    }
}

fn run_checkout_cancellation_simulation(
    seed: u64,
) -> turmoil::Result<GitCheckoutCancellationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(40))
        .run_with(move |context| async move {
            let store = Arc::new(InMemoryVfsStore::new(context.clock(), context.rng()));
            let volume = store
                .open_volume(
                    VolumeConfig::new(VolumeId::new(0x9700 + seed as u128))
                        .with_chunk_size(4096)
                        .with_create_if_missing(true),
                )
                .await
                .expect("open git volume");
            let oid = format!("{:08x}", seed ^ 0x5555_5555);
            seed_repository(
                volume.clone(),
                "/repo",
                &oid,
                format!("cancel-{seed:x}\n").into_bytes(),
            )
            .await;

            let repo_image = Arc::new(
                VfsGitRepositoryImage::from_volume(volume, "/repo", SnapshotOptions::default())
                    .await
                    .expect("git image"),
            );
            let delay_millis = (seed % 5) + 2;
            let repo_store = DeterministicGitRepositoryStore::default().with_materializer(
                Arc::new(SimulatedDelayMaterializer {
                    clock: context.clock(),
                    delay: Duration::from_millis(delay_millis),
                }),
            );
            let repo = repo_store
                .open(
                    repo_image.clone(),
                    open_request("checkout-cancel", repo_image.as_ref()),
                    Arc::new(NeverCancel),
                )
                .await
                .expect("open repo");

            let cancellation = Arc::new(ToggleCancellation::default());
            let checkout = {
                let repo = repo.clone();
                let cancellation = cancellation.clone();
                tokio::spawn(async move {
                    repo.checkout(
                        GitCheckoutRequest {
                            target_ref: "refs/heads/main".to_string(),
                            materialize_path: "/workspace".to_string(),
                            pathspec: Vec::new(),
                            update_head: false,
                        },
                        cancellation,
                    )
                    .await
                })
            };

            context
                .clock()
                .sleep(Duration::from_millis(delay_millis.saturating_sub(1)))
                .await;
            cancellation.cancel();

            let error = checkout
                .await
                .expect("join checkout task")
                .expect_err("checkout should be cancelled");
            match error {
                GitSubstrateError::Cancelled { repository_id } => {
                    Ok(GitCheckoutCancellationCapture {
                        delay_millis,
                        cancelled_repository_id: repository_id,
                    })
                }
                other => panic!("expected cancelled error, got {other:?}"),
            }
        })
}

#[test]
fn git_seeded_simulation_replays_snapshot_overlay_and_imported_images() -> turmoil::Result {
    let capture = assert_seed_replays(
        &GitSeedHarness {
            run: run_git_simulation,
        },
        0x6201,
    )?;
    assert_eq!(capture.outcome.snapshot_root, "/repo");
    assert_eq!(capture.outcome.overlay_root, "/repo");
    assert_eq!(capture.outcome.imported_root, "/repo");
    assert_ne!(
        capture.outcome.snapshot_head_oid, capture.outcome.overlay_head_oid,
        "overlay repo image should surface its own ref/object state"
    );
    assert_eq!(
        capture.outcome.overlay_head_oid,
        capture.outcome.imported_head_oid
    );
    assert_eq!(
        capture.outcome.ref_names,
        vec!["refs/heads/main".to_string()]
    );
    assert!(
        String::from_utf8_lossy(&capture.outcome.imported_payload).ends_with("overlay-6201\n"),
        "imported commit payload should preserve the overlay-specific commit body"
    );
    Ok(())
}

#[test]
fn git_seeded_simulation_varies_between_seeds() -> turmoil::Result {
    let _ = assert_seed_variation(
        &GitSeedHarness {
            run: run_git_simulation,
        },
        0x6202,
        0x6203,
        |left, right| left.outcome.imported_payload != right.outcome.imported_payload,
    )?;
    Ok(())
}

#[test]
fn git_checkout_cancellation_replays_under_seeded_simulation() -> turmoil::Result {
    let capture = assert_seed_replays(
        &GitSeedHarness {
            run: run_checkout_cancellation_simulation,
        },
        0x6204,
    )?;
    assert_eq!(capture.outcome.cancelled_repository_id, "checkout-cancel");
    assert!(capture.outcome.delay_millis >= 2);
    Ok(())
}
