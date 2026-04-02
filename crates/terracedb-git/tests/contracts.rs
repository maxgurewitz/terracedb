use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use serde_json::json;
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng};
use terracedb_git::worktree::GitWorktreeMaterializer;
use terracedb_git::{
    DeterministicGitRepositoryStore, GitCancellationToken, GitCheckoutReport, GitCheckoutRequest,
    GitDiffRequest, GitDiscoverRequest, GitExecutionHooks, GitForkPolicy, GitHeadState,
    GitIndexEntry, GitIndexSnapshot, GitObject, GitObjectDatabase, GitOpenRequest, GitRefUpdate,
    GitReference, GitRepositoryHandle, GitRepositoryImage, GitRepositoryPolicy,
    GitRepositoryProvenance, GitRepositoryStore, GitStatusKind, GitStatusOptions,
    GitSubstrateError, NeverCancel, VfsGitRepositoryImage,
};
use terracedb_vfs::{
    CloneVolumeSource, CreateOptions, InMemoryVfsStore, SnapshotOptions, Volume, VolumeConfig,
    VolumeId, VolumeStore,
};
use tokio::sync::{Mutex, Notify};

async fn open_seeded_volume(seed: u64) -> (Arc<InMemoryVfsStore>, Arc<dyn Volume>, String) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(seed)),
    );
    let store = Arc::new(InMemoryVfsStore::with_dependencies(dependencies));
    let volume = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x9100 + seed as u128))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open git volume");
    let base_oid = format!("{:08x}", seed ^ 0x1111_1111);
    seed_repository(
        volume.clone(),
        "/repo",
        &base_oid,
        format!("base-{seed:x}\n").as_bytes(),
    )
    .await;
    (store, volume, base_oid)
}

async fn seed_repository(volume: Arc<dyn Volume>, root: &str, oid: &str, payload: &[u8]) {
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
                payload,
            ]
            .concat(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write git object");
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
        .expect("write git blob object");
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
        .expect("write git tree object");
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
    volume
        .fs()
        .write_file(
            &format!("{root}/.git/index.json"),
            serde_json::to_vec(&GitIndexSnapshot {
                entries: vec![GitIndexEntry {
                    path: "src/lib.rs".to_string(),
                    oid: Some(blob_oid),
                    mode: 0o100644,
                }],
                metadata: BTreeMap::from([("seed".to_string(), json!(oid))]),
            })
            .expect("encode git index"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write git index");
}

fn blob_oid_for_commit(oid: &str) -> String {
    format!("{oid}-blob")
}

fn tree_oid_for_commit(oid: &str) -> String {
    format!("{oid}-tree")
}

fn source_bytes_for_commit(oid: &str) -> Vec<u8> {
    format!("pub const SEED: &str = \"{oid}\";\n").into_bytes()
}

fn open_request(
    repository_id: &str,
    image: &dyn GitRepositoryImage,
    metadata: BTreeMap<String, terracedb_git::JsonValue>,
) -> GitOpenRequest {
    let descriptor = image.descriptor();
    GitOpenRequest {
        repository_id: repository_id.to_string(),
        repository_image: descriptor.clone(),
        policy: GitRepositoryPolicy::default(),
        provenance: GitRepositoryProvenance {
            backend: "deterministic-git".to_string(),
            repo_root: descriptor.root_path.clone(),
            imported_from_host: false,
            volume_id: descriptor.volume_id,
            snapshot_sequence: descriptor.snapshot_sequence,
            durable_snapshot: descriptor.durable_snapshot,
            fork_policy: GitForkPolicy::simulation_native_baseline(),
        },
        metadata,
    }
}

#[tokio::test]
async fn public_git_surface_discovers_and_opens_snapshot_repositories() {
    let (_store, volume, base_oid) = open_seeded_volume(0x6101).await;
    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume, "/repo", SnapshotOptions::default())
            .await
            .expect("snapshot git image"),
    );
    let repo_store = DeterministicGitRepositoryStore::default();

    let discovered = repo_store
        .discover(
            image.clone(),
            GitDiscoverRequest {
                start_path: "/repo/src".to_string(),
                policy: GitRepositoryPolicy::default(),
                metadata: BTreeMap::from([("kind".to_string(), json!("contracts"))]),
            },
            Arc::new(NeverCancel),
        )
        .await
        .expect("discover snapshot repo");
    assert_eq!(discovered.repository_root, "/repo");
    assert_eq!(discovered.head_ref.as_deref(), Some("refs/heads/main"));

    let repo = repo_store
        .open(
            image.clone(),
            open_request("repo-snapshot", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open snapshot repo");

    let head = repo.head().await.expect("read head");
    assert_eq!(head.symbolic_ref.as_deref(), Some("refs/heads/main"));
    assert_eq!(head.oid.as_deref(), Some(base_oid.as_str()));

    let refs = repo.list_refs().await.expect("list refs");
    assert_eq!(
        refs,
        vec![GitReference {
            name: "refs/heads/main".to_string(),
            target: base_oid.clone(),
        }]
    );

    let object = GitObjectDatabase::read_object(repo.as_ref(), &base_oid)
        .await
        .expect("read object")
        .expect("object exists");
    assert_eq!(object.oid, base_oid);
    let object_text = String::from_utf8_lossy(&object.data);
    assert!(object_text.starts_with(&format!("tree {}\n", tree_oid_for_commit(&base_oid))));
    assert!(object_text.ends_with("base-6101\n"));

    let index = terracedb_git::GitIndexStore::index(repo.as_ref())
        .await
        .expect("read index");
    assert_eq!(index.entries.len(), 1);
    assert_eq!(index.entries[0].path, "src/lib.rs");
}

#[tokio::test]
async fn discover_is_bounded_to_the_explicit_repository_image_root() {
    let (_store, volume, _base_oid) = open_seeded_volume(0x6102).await;
    seed_repository(
        volume.clone(),
        "/repo-other",
        "deadbeef",
        b"outside-image-root\n",
    )
    .await;
    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume, "/repo", SnapshotOptions::default())
            .await
            .expect("snapshot git image"),
    );
    let repo_store = DeterministicGitRepositoryStore::default();

    let error = repo_store
        .discover(
            image,
            GitDiscoverRequest {
                start_path: "/repo-other/src".to_string(),
                policy: GitRepositoryPolicy::default(),
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancel),
        )
        .await
        .expect_err("discover should stay inside the image root");
    assert!(matches!(
        error,
        GitSubstrateError::RepositoryNotFound { path } if path == "/repo-other/src"
    ));
}

#[tokio::test]
async fn open_rejects_repository_image_descriptor_mismatches() {
    let (_store, volume, _base_oid) = open_seeded_volume(0x6103).await;
    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume, "/repo", SnapshotOptions::default())
            .await
            .expect("snapshot git image"),
    );
    let repo_store = DeterministicGitRepositoryStore::default();
    let mut request = open_request("repo-mismatch", image.as_ref(), BTreeMap::new());
    request.repository_image.root_path = "/repo-other".to_string();

    let error = match repo_store.open(image, request, Arc::new(NeverCancel)).await {
        Ok(_) => panic!("open should validate the repository image descriptor"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        GitSubstrateError::RepositoryImageDescriptorMismatch {
            field: "root_path",
            ..
        }
    ));
}

#[tokio::test]
async fn overlay_and_imported_repository_images_preserve_read_only_git_access() {
    let (store, volume, base_oid) = open_seeded_volume(0x6104).await;
    let base_snapshot = volume
        .snapshot(SnapshotOptions::default())
        .await
        .expect("base snapshot");
    let base_image = Arc::new(VfsGitRepositoryImage::new(base_snapshot.clone(), "/repo"));
    let overlay = store
        .create_overlay(
            base_snapshot,
            VolumeConfig::new(VolumeId::new(0x9204))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("create overlay");
    let overlay_oid = format!("{:08x}", 0x6104_u64 ^ 0x2222_2222);
    seed_repository(overlay.clone(), "/repo", &overlay_oid, b"overlay-6104\n").await;
    let overlay_image = Arc::new(
        VfsGitRepositoryImage::from_volume(overlay.clone(), "/repo", SnapshotOptions::default())
            .await
            .expect("overlay git image"),
    );

    let imported_store = Arc::new(InMemoryVfsStore::with_dependencies(DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(0x6104)),
    )));
    let imported = imported_store
        .import_volume(
            store
                .export_volume(CloneVolumeSource::new(overlay.info().volume_id))
                .await
                .expect("export overlay repo image"),
            VolumeConfig::new(VolumeId::new(0x9304))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("import overlay repo image");
    let imported_image = Arc::new(
        VfsGitRepositoryImage::from_volume(imported, "/repo", SnapshotOptions::default())
            .await
            .expect("imported git image"),
    );

    let repo_store = DeterministicGitRepositoryStore::default();
    let base_repo = repo_store
        .open(
            base_image.clone(),
            open_request("repo-base", base_image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open base repo");
    let overlay_repo = repo_store
        .open(
            overlay_image.clone(),
            open_request("repo-overlay", overlay_image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open overlay repo");
    let imported_repo = repo_store
        .open(
            imported_image.clone(),
            open_request("repo-imported", imported_image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open imported repo");

    assert_eq!(
        base_repo.head().await.expect("base head").oid,
        Some(base_oid)
    );
    assert_eq!(
        overlay_repo.head().await.expect("overlay head").oid,
        Some(overlay_oid.clone())
    );
    assert_eq!(
        imported_repo.head().await.expect("imported head").oid,
        Some(overlay_oid.clone())
    );
    let overlay_object = GitObjectDatabase::read_object(overlay_repo.as_ref(), &overlay_oid)
        .await
        .expect("read overlay object")
        .expect("overlay object");
    let imported_object = GitObjectDatabase::read_object(imported_repo.as_ref(), &overlay_oid)
        .await
        .expect("read imported object")
        .expect("imported object");
    assert!(
        String::from_utf8_lossy(&overlay_object.data).ends_with("overlay-6104\n"),
        "overlay commit payload should round-trip through the VFS-backed object store"
    );
    assert_eq!(imported_object.data, overlay_object.data);
}

#[derive(Clone, Default)]
struct RecordingHooks {
    events: Arc<Mutex<Vec<String>>>,
}

impl RecordingHooks {
    async fn snapshot(&self) -> Vec<String> {
        self.events.lock().await.clone()
    }
}

#[async_trait]
impl GitExecutionHooks for RecordingHooks {
    async fn on_open(&self, handle: &GitRepositoryHandle) -> Result<(), GitSubstrateError> {
        self.events
            .lock()
            .await
            .push(format!("open:{}", handle.repository_id));
        Ok(())
    }

    async fn on_read_refs(&self, refs: &[GitReference]) -> Result<(), GitSubstrateError> {
        self.events
            .lock()
            .await
            .push(format!("refs:{}", refs.len()));
        Ok(())
    }

    async fn on_read_head(&self, head: &GitHeadState) -> Result<(), GitSubstrateError> {
        self.events.lock().await.push(format!(
            "head:{}",
            head.symbolic_ref
                .clone()
                .or_else(|| head.oid.clone())
                .unwrap_or_default()
        ));
        Ok(())
    }

    async fn on_read_object(&self, object: &GitObject) -> Result<(), GitSubstrateError> {
        self.events
            .lock()
            .await
            .push(format!("object:{}", object.oid));
        Ok(())
    }

    async fn on_checkout(&self, report: &GitCheckoutReport) -> Result<(), GitSubstrateError> {
        self.events
            .lock()
            .await
            .push(format!("checkout:{}", report.target_ref));
        Ok(())
    }
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

#[derive(Clone, Default)]
struct BlockingMaterializer {
    started: Arc<Notify>,
    resume: Arc<Notify>,
}

#[async_trait]
impl GitWorktreeMaterializer for BlockingMaterializer {
    async fn materialize(
        &self,
        repository_id: &str,
        request: GitCheckoutRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCheckoutReport, GitSubstrateError> {
        self.started.notify_one();
        self.resume.notified().await;
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: repository_id.to_string(),
            });
        }
        Ok(GitCheckoutReport {
            target_ref: request.target_ref,
            materialized_path: request.materialize_path,
            written_paths: 0,
            deleted_paths: 0,
            head_oid: None,
        })
    }
}

#[tokio::test]
async fn execution_hooks_capture_head_refs_objects_and_checkout() {
    let (_store, volume, base_oid) = open_seeded_volume(0x6105).await;
    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume, "/repo", SnapshotOptions::default())
            .await
            .expect("snapshot git image"),
    );
    let hooks = RecordingHooks::default();
    let repo_store = DeterministicGitRepositoryStore::default().with_hooks(Arc::new(hooks.clone()));
    let repo = repo_store
        .open(
            image.clone(),
            open_request("repo-hooks", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo with hooks");

    repo.head().await.expect("head");
    repo.list_refs().await.expect("list refs");
    GitObjectDatabase::read_object(repo.as_ref(), &base_oid)
        .await
        .expect("read object")
        .expect("object exists");
    repo.checkout(
        GitCheckoutRequest {
            target_ref: "refs/heads/main".to_string(),
            materialize_path: "/workspace".to_string(),
            pathspec: Vec::new(),
            update_head: false,
        },
        Arc::new(NeverCancel),
    )
    .await
    .expect("checkout");

    assert_eq!(
        hooks.snapshot().await,
        vec![
            "open:repo-hooks".to_string(),
            "head:refs/heads/main".to_string(),
            "refs:1".to_string(),
            format!("object:{base_oid}"),
            format!("object:{base_oid}"),
            format!("object:{}", tree_oid_for_commit(&base_oid)),
            format!("object:{}", blob_oid_for_commit(&base_oid)),
            "checkout:refs/heads/main".to_string(),
        ]
    );
}

#[tokio::test]
async fn checkout_passes_cancellation_through_to_the_materializer() {
    let (_store, volume, _base_oid) = open_seeded_volume(0x6106).await;
    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume, "/repo", SnapshotOptions::default())
            .await
            .expect("snapshot git image"),
    );
    let materializer = BlockingMaterializer::default();
    let repo_store = DeterministicGitRepositoryStore::default()
        .with_materializer(Arc::new(materializer.clone()));
    let repo = repo_store
        .open(
            image.clone(),
            open_request("repo-cancel", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo");
    let cancellation = Arc::new(ToggleCancellation::default());
    let task = {
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

    materializer.started.notified().await;
    cancellation.cancel();
    materializer.resume.notify_one();

    let error = task
        .await
        .expect("join checkout task")
        .expect_err("checkout should observe cancellation inside the materializer");
    assert!(matches!(
        error,
        GitSubstrateError::Cancelled { repository_id } if repository_id == "repo-cancel"
    ));
}

#[tokio::test]
async fn vfs_native_status_and_diff_cover_modified_untracked_ignored_and_pathspec_flows() {
    let (_store, volume, base_oid) = open_seeded_volume(0x6107).await;
    volume
        .fs()
        .write_file(
            "/repo/.gitignore",
            b"ignored.txt\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write gitignore");
    volume
        .fs()
        .write_file(
            "/repo/src/lib.rs",
            b"pub const SEED: &str = \"modified\";\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("modify tracked file");
    volume
        .fs()
        .write_file(
            "/repo/src/new.rs",
            b"pub const NEW_FILE: bool = true;\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write untracked file");
    volume
        .fs()
        .write_file(
            "/repo/src/nested/deep.rs",
            b"pub const DEEP_FILE: bool = true;\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write nested untracked file");
    volume
        .fs()
        .write_file(
            "/repo/ignored.txt",
            b"ignored\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write ignored file");

    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume.clone(), "/repo", SnapshotOptions::default())
            .await
            .expect("git image"),
    );
    let repo = DeterministicGitRepositoryStore::default()
        .open(
            image.clone(),
            open_request("repo-status", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo");

    let status = repo
        .status_with_options(GitStatusOptions {
            include_ignored: true,
            ..Default::default()
        })
        .await
        .expect("status");
    assert!(status.dirty);
    assert_eq!(
        status.entries,
        vec![
            terracedb_git::GitStatusEntry {
                path: ".gitignore".to_string(),
                kind: GitStatusKind::Untracked,
                previous_path: None,
            },
            terracedb_git::GitStatusEntry {
                path: "ignored.txt".to_string(),
                kind: GitStatusKind::Ignored,
                previous_path: None,
            },
            terracedb_git::GitStatusEntry {
                path: "src/lib.rs".to_string(),
                kind: GitStatusKind::Modified,
                previous_path: None,
            },
            terracedb_git::GitStatusEntry {
                path: "src/nested/deep.rs".to_string(),
                kind: GitStatusKind::Untracked,
                previous_path: None,
            },
            terracedb_git::GitStatusEntry {
                path: "src/new.rs".to_string(),
                kind: GitStatusKind::Untracked,
                previous_path: None,
            },
        ]
    );

    let filtered = repo
        .status_with_options(GitStatusOptions {
            pathspec: vec!["src".to_string()],
            ..Default::default()
        })
        .await
        .expect("pathspec-filtered status");
    assert_eq!(
        filtered
            .entries
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>(),
        vec![
            "src/lib.rs".to_string(),
            "src/nested/deep.rs".to_string(),
            "src/new.rs".to_string()
        ]
    );

    let wildcard_filtered = repo
        .status_with_options(GitStatusOptions {
            pathspec: vec!["src/*.rs".to_string()],
            ..Default::default()
        })
        .await
        .expect("wildcard-filtered status");
    assert_eq!(
        wildcard_filtered
            .entries
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>(),
        vec!["src/lib.rs".to_string(), "src/new.rs".to_string()]
    );

    let diff = repo
        .diff(GitDiffRequest {
            pathspec: vec!["src/lib.rs".to_string()],
            ..Default::default()
        })
        .await
        .expect("diff");
    assert_eq!(diff.entries.len(), 1);
    assert_eq!(diff.entries[0].path, "src/lib.rs");
    assert_eq!(diff.entries[0].kind, terracedb_git::GitDiffKind::Modified);
    assert_eq!(
        diff.entries[0].old_oid.as_deref(),
        Some(blob_oid_for_commit(&base_oid).as_str())
    );
}

#[tokio::test]
async fn status_collapses_delete_plus_untracked_into_renamed_when_content_matches() {
    let (_store, volume, base_oid) = open_seeded_volume(0x6109).await;
    let original = source_bytes_for_commit(&base_oid);
    volume
        .fs()
        .unlink("/repo/src/lib.rs")
        .await
        .expect("remove tracked path");
    volume
        .fs()
        .write_file(
            "/repo/src/renamed.rs",
            original,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write renamed path");

    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume.clone(), "/repo", SnapshotOptions::default())
            .await
            .expect("git image"),
    );
    let repo = DeterministicGitRepositoryStore::default()
        .open(
            image.clone(),
            open_request("repo-rename", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo");

    let status = repo.status().await.expect("status");
    assert_eq!(
        status.entries,
        vec![terracedb_git::GitStatusEntry {
            path: "src/renamed.rs".to_string(),
            kind: GitStatusKind::Renamed,
            previous_path: Some("src/lib.rs".to_string()),
        }]
    );
}

#[tokio::test]
async fn status_fails_closed_when_head_tree_cannot_be_loaded() {
    let (_store, volume, base_oid) = open_seeded_volume(0x610a).await;
    volume
        .fs()
        .write_file(
            &format!("/repo/.git/objects/{base_oid}"),
            b"commit\ntree missing-tree\nbroken\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("corrupt head commit");

    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume.clone(), "/repo", SnapshotOptions::default())
            .await
            .expect("git image"),
    );
    let repo = DeterministicGitRepositoryStore::default()
        .open(
            image.clone(),
            open_request("repo-corrupt-head", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo");

    let error = repo.status().await.expect_err("status should fail closed");
    assert!(matches!(
        error,
        GitSubstrateError::ObjectNotFound { oid } if oid == "missing-tree"
    ));
}

#[tokio::test]
async fn checkout_materializes_target_tree_updates_head_and_moves_refs() {
    let (_store, volume, base_oid) = open_seeded_volume(0x6108).await;
    let feature_commit = "feature-6108";
    let feature_blob = blob_oid_for_commit(feature_commit);
    let feature_tree = tree_oid_for_commit(feature_commit);

    volume
        .fs()
        .write_file(
            "/repo/.git/objects/feature-6108",
            format!("commit\ntree {feature_tree}\nfeature-6108\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write feature commit");
    volume
        .fs()
        .write_file(
            &format!("/repo/.git/objects/{feature_blob}"),
            b"blob\npub const FEATURE: &str = \"feature-6108\";\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write feature blob");
    volume
        .fs()
        .write_file(
            &format!("/repo/.git/objects/{feature_tree}"),
            format!("tree\n100644 blob {feature_blob}\tsrc/lib.rs\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write feature tree");
    volume
        .fs()
        .write_file(
            "/repo/.git/refs/heads/feature",
            b"feature-6108\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write feature ref");

    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume.clone(), "/repo", SnapshotOptions::default())
            .await
            .expect("git image"),
    );
    let repo = DeterministicGitRepositoryStore::default()
        .open(
            image.clone(),
            open_request("repo-checkout", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo");

    let export = repo
        .checkout(
            GitCheckoutRequest {
                target_ref: "refs/heads/feature".to_string(),
                materialize_path: "/workspace/export".to_string(),
                pathspec: Vec::new(),
                update_head: false,
            },
            Arc::new(NeverCancel),
        )
        .await
        .expect("materialize feature tree");
    assert_eq!(export.head_oid.as_deref(), Some(feature_commit));
    assert_eq!(
        volume
            .fs()
            .read_file("/workspace/export/src/lib.rs")
            .await
            .expect("read exported file"),
        Some(b"pub const FEATURE: &str = \"feature-6108\";\n".to_vec())
    );

    let repo_root_checkout = repo
        .checkout(
            GitCheckoutRequest {
                target_ref: "refs/heads/feature".to_string(),
                materialize_path: "/repo".to_string(),
                pathspec: Vec::new(),
                update_head: false,
            },
            Arc::new(NeverCancel),
        )
        .await
        .expect("checkout feature into repo root without moving head");
    assert_eq!(repo_root_checkout.head_oid.as_deref(), Some(feature_commit));
    let still_main = repo.head().await.expect("read unchanged head");
    assert_eq!(still_main.symbolic_ref.as_deref(), Some("refs/heads/main"));
    let refreshed_index = terracedb_git::GitIndexStore::index(repo.as_ref())
        .await
        .expect("read refreshed index after repo-root checkout");
    assert_eq!(refreshed_index.entries.len(), 1);
    assert_eq!(
        refreshed_index.entries[0].oid.as_deref(),
        Some(feature_blob.as_str())
    );
    let staged_status = repo.status().await.expect("status after staged checkout");
    assert_eq!(
        staged_status.entries,
        vec![terracedb_git::GitStatusEntry {
            path: "src/lib.rs".to_string(),
            kind: GitStatusKind::Modified,
            previous_path: None,
        }]
    );

    let ref_update = repo
        .update_ref(
            GitRefUpdate {
                name: "refs/heads/main".to_string(),
                target: feature_commit.to_string(),
                previous_target: Some(base_oid.clone()),
                symbolic: false,
            },
            Arc::new(NeverCancel),
        )
        .await
        .expect("move main ref");
    assert_eq!(ref_update.reference.target, feature_commit);

    let report = repo
        .checkout(
            GitCheckoutRequest {
                target_ref: "refs/heads/feature".to_string(),
                materialize_path: "/repo".to_string(),
                pathspec: Vec::new(),
                update_head: true,
            },
            Arc::new(NeverCancel),
        )
        .await
        .expect("checkout feature into repo root");
    assert_eq!(report.head_oid.as_deref(), Some(feature_commit));

    let head = repo.head().await.expect("read updated head");
    assert_eq!(head.symbolic_ref.as_deref(), Some("refs/heads/feature"));
    assert_eq!(head.oid.as_deref(), Some(feature_commit));
    let index = terracedb_git::GitIndexStore::index(repo.as_ref())
        .await
        .expect("read updated index");
    assert_eq!(index.entries.len(), 1);
    assert_eq!(index.entries[0].oid.as_deref(), Some(feature_blob.as_str()));
}

#[tokio::test]
async fn pathspec_checkout_removes_stale_deleted_paths_in_selected_subtree() {
    let (_store, volume, _base_oid) = open_seeded_volume(0x6110).await;
    volume
        .fs()
        .write_file(
            "/repo/src/stale.rs",
            b"pub const STALE: bool = true;\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed stale file");
    volume
        .fs()
        .write_file(
            "/repo/keep.txt",
            b"keep me\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed unrelated file");

    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume.clone(), "/repo", SnapshotOptions::default())
            .await
            .expect("git image"),
    );
    let repo = DeterministicGitRepositoryStore::default()
        .open(
            image.clone(),
            open_request("repo-pathspec-checkout", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo");

    repo.checkout(
        GitCheckoutRequest {
            target_ref: "refs/heads/main".to_string(),
            materialize_path: "/repo".to_string(),
            pathspec: vec!["src".to_string()],
            update_head: false,
        },
        Arc::new(NeverCancel),
    )
    .await
    .expect("pathspec checkout");

    assert_eq!(
        volume
            .fs()
            .read_file("/repo/src/stale.rs")
            .await
            .expect("read stale path"),
        None
    );
    assert_eq!(
        volume
            .fs()
            .read_file("/repo/keep.txt")
            .await
            .expect("read unrelated path"),
        Some(b"keep me\n".to_vec())
    );
}

#[tokio::test]
async fn update_ref_normalizes_symbolic_ref_compare_and_swap_targets() {
    let (_store, volume, base_oid) = open_seeded_volume(0x6111).await;
    volume
        .fs()
        .write_file(
            "/repo/.git/refs/heads/feature",
            format!("{base_oid}\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write feature ref");

    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume.clone(), "/repo", SnapshotOptions::default())
            .await
            .expect("git image"),
    );
    let repo = DeterministicGitRepositoryStore::default()
        .open(
            image.clone(),
            open_request("repo-symbolic-update", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo");

    let report = repo
        .update_ref(
            GitRefUpdate {
                name: "HEAD".to_string(),
                target: "refs/heads/feature".to_string(),
                previous_target: Some("refs/heads/main".to_string()),
                symbolic: true,
            },
            Arc::new(NeverCancel),
        )
        .await
        .expect("update symbolic head ref");
    assert_eq!(report.previous_target.as_deref(), Some("refs/heads/main"));

    let head = repo.head().await.expect("read updated head");
    assert_eq!(head.symbolic_ref.as_deref(), Some("refs/heads/feature"));
    assert_eq!(head.oid.as_deref(), Some(base_oid.as_str()));
}

#[tokio::test]
async fn non_head_symbolic_refs_resolve_through_ref_reads_and_checkout() {
    let (_store, volume, base_oid) = open_seeded_volume(0x6112).await;
    let image = Arc::new(
        VfsGitRepositoryImage::from_volume(volume.clone(), "/repo", SnapshotOptions::default())
            .await
            .expect("git image"),
    );
    let repo = DeterministicGitRepositoryStore::default()
        .open(
            image.clone(),
            open_request("repo-symbolic-branch", image.as_ref(), BTreeMap::new()),
            Arc::new(NeverCancel),
        )
        .await
        .expect("open repo");

    repo.update_ref(
        GitRefUpdate {
            name: "refs/heads/release".to_string(),
            target: "refs/heads/main".to_string(),
            previous_target: None,
            symbolic: true,
        },
        Arc::new(NeverCancel),
    )
    .await
    .expect("create symbolic branch ref");

    let refs = repo.list_refs().await.expect("list refs");
    assert!(refs.contains(&GitReference {
        name: "refs/heads/release".to_string(),
        target: base_oid.clone(),
    }));

    let checkout = repo
        .checkout(
            GitCheckoutRequest {
                target_ref: "refs/heads/release".to_string(),
                materialize_path: "/workspace/release".to_string(),
                pathspec: Vec::new(),
                update_head: false,
            },
            Arc::new(NeverCancel),
        )
        .await
        .expect("checkout symbolic branch");
    assert_eq!(checkout.head_oid.as_deref(), Some(base_oid.as_str()));
}
