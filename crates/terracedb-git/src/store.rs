use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use sha1::Sha1;
use sha2::{Digest as _, Sha256};
use terracedb_vfs::{
    CreateOptions, FileKind, MkdirOptions, ReadOnlyVfsFileSystem, SnapshotOptions, Stats,
    VfsFileSystem, Volume, VolumeSnapshot,
};

use crate::{
    GitCheckoutReport, GitCheckoutRequest, GitCommitReport, GitCommitRequest, GitDiffEntry,
    GitDiffKind, GitDiffReport, GitDiffRequest, GitDiscoverReport, GitDiscoverRequest,
    GitHeadState, GitIndexEntry, GitIndexSnapshot, GitIndexStore, GitObject, GitObjectDatabase,
    GitObjectFormat, GitObjectKind, GitRefDatabase, GitRefUpdate, GitRefUpdateReport, GitReference,
    GitRepositoryHandle, GitRepositoryImageDescriptor, GitStatusEntry, GitStatusKind,
    GitStatusOptions, GitStatusReport, GitSubstrateError, worktree::GitWorktreeMaterializer,
};

pub trait GitCancellationToken: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GitObjectIdAlgorithm {
    Sha1,
    Sha256,
}

#[derive(Clone, Debug, Default)]
pub struct NeverCancel;

impl GitCancellationToken for NeverCancel {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[async_trait]
pub trait GitExecutionHooks: Send + Sync {
    async fn on_discover(&self, _report: &GitDiscoverReport) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_open(&self, _handle: &GitRepositoryHandle) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_read_refs(&self, _refs: &[GitReference]) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_read_head(&self, _head: &GitHeadState) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_read_object(&self, _object: &GitObject) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_status(&self, _report: &GitStatusReport) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_diff(&self, _report: &GitDiffReport) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_checkout(&self, _report: &GitCheckoutReport) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_commit(&self, _report: &GitCommitReport) -> Result<(), GitSubstrateError> {
        Ok(())
    }

    async fn on_update_ref(&self, _report: &GitRefUpdateReport) -> Result<(), GitSubstrateError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct NoopGitExecutionHooks;

#[async_trait]
impl GitExecutionHooks for NoopGitExecutionHooks {}

pub trait GitRepositoryImage: Send + Sync {
    fn descriptor(&self) -> GitRepositoryImageDescriptor;
    fn root_path(&self) -> &str;
    fn snapshot(&self) -> Arc<dyn VolumeSnapshot>;

    fn writable_volume(&self) -> Option<Arc<dyn Volume>> {
        None
    }
}

#[derive(Clone)]
pub struct VfsGitRepositoryImage {
    root_path: Arc<str>,
    snapshot: Arc<dyn VolumeSnapshot>,
    volume: Option<Arc<dyn Volume>>,
}

impl VfsGitRepositoryImage {
    pub fn new(snapshot: Arc<dyn VolumeSnapshot>, root_path: impl Into<Arc<str>>) -> Self {
        let root_path = root_path.into();
        Self {
            root_path: Arc::from(normalize_path(root_path.as_ref())),
            snapshot,
            volume: None,
        }
    }

    pub async fn from_volume(
        volume: Arc<dyn Volume>,
        root_path: impl Into<Arc<str>>,
        snapshot: SnapshotOptions,
    ) -> Result<Self, GitSubstrateError> {
        Ok(Self {
            root_path: Arc::from(normalize_path(root_path.into().as_ref())),
            snapshot: volume.snapshot(snapshot).await?,
            volume: Some(volume),
        })
    }
}

impl GitRepositoryImage for VfsGitRepositoryImage {
    fn descriptor(&self) -> GitRepositoryImageDescriptor {
        GitRepositoryImageDescriptor {
            root_path: self.root_path.to_string(),
            volume_id: Some(self.snapshot.volume_id()),
            snapshot_sequence: Some(self.snapshot.sequence().get()),
            durable_snapshot: self.snapshot.durable(),
        }
    }

    fn root_path(&self) -> &str {
        self.root_path.as_ref()
    }

    fn snapshot(&self) -> Arc<dyn VolumeSnapshot> {
        self.snapshot.clone()
    }

    fn writable_volume(&self) -> Option<Arc<dyn Volume>> {
        self.volume.clone()
    }
}

#[async_trait]
pub trait GitRepositoryStore: Send + Sync {
    fn name(&self) -> &str;
    fn fork_policy(&self) -> crate::GitForkPolicy;
    async fn discover(
        &self,
        image: Arc<dyn GitRepositoryImage>,
        request: GitDiscoverRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitDiscoverReport, GitSubstrateError>;
    async fn open(
        &self,
        image: Arc<dyn GitRepositoryImage>,
        request: crate::GitOpenRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<Arc<dyn GitRepository>, GitSubstrateError>;
}

#[async_trait]
pub trait GitRepository:
    Send + Sync + GitObjectDatabase + GitRefDatabase + crate::GitIndexStore
{
    fn handle(&self) -> GitRepositoryHandle;
    async fn head(&self) -> Result<GitHeadState, GitSubstrateError>;

    async fn status(&self) -> Result<GitStatusReport, GitSubstrateError> {
        self.status_with_options(GitStatusOptions::default()).await
    }

    async fn status_with_options(
        &self,
        options: GitStatusOptions,
    ) -> Result<GitStatusReport, GitSubstrateError>;

    async fn diff(&self, request: GitDiffRequest) -> Result<GitDiffReport, GitSubstrateError>;

    async fn checkout(
        &self,
        request: GitCheckoutRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCheckoutReport, GitSubstrateError>;

    async fn commit(
        &self,
        request: GitCommitRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCommitReport, GitSubstrateError>;

    async fn update_ref(
        &self,
        request: GitRefUpdate,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitRefUpdateReport, GitSubstrateError>;
}

#[derive(Clone)]
pub struct DeterministicGitRepositoryStore {
    name: Arc<str>,
    hooks: Arc<dyn GitExecutionHooks>,
    materializer: Option<Arc<dyn GitWorktreeMaterializer>>,
    fork_policy: crate::GitForkPolicy,
}

impl DeterministicGitRepositoryStore {
    pub fn new() -> Self {
        Self {
            name: Arc::from("deterministic-git"),
            hooks: Arc::new(NoopGitExecutionHooks),
            materializer: None,
            fork_policy: crate::GitForkPolicy::simulation_native_baseline(),
        }
    }

    pub fn with_hooks(mut self, hooks: Arc<dyn GitExecutionHooks>) -> Self {
        self.hooks = hooks;
        self
    }

    pub fn with_materializer(mut self, materializer: Arc<dyn GitWorktreeMaterializer>) -> Self {
        self.materializer = Some(materializer);
        self
    }
}

impl Default for DeterministicGitRepositoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl GitRepositoryStore for DeterministicGitRepositoryStore {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn fork_policy(&self) -> crate::GitForkPolicy {
        self.fork_policy.clone()
    }

    async fn discover(
        &self,
        image: Arc<dyn GitRepositoryImage>,
        request: GitDiscoverRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitDiscoverReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: "discover".to_string(),
            });
        }
        let root = discover_repository_root(
            image.snapshot().fs(),
            image.root_path(),
            &request.start_path,
        )
        .await?
        .ok_or_else(|| GitSubstrateError::RepositoryNotFound {
            path: request.start_path.clone(),
        })?;
        let head_ref = read_head_ref(image.snapshot().fs(), &root).await?;
        let report = GitDiscoverReport {
            repository_root: root,
            head_ref,
            metadata: request.metadata,
        };
        self.hooks.on_discover(&report).await?;
        Ok(report)
    }

    async fn open(
        &self,
        image: Arc<dyn GitRepositoryImage>,
        request: crate::GitOpenRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<Arc<dyn GitRepository>, GitSubstrateError> {
        let repository_id = request.repository_id.clone();
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled { repository_id });
        }
        let image_descriptor = image.descriptor();
        validate_image_descriptor(&request.repository_image, &image_descriptor)?;
        let root = image_descriptor.root_path.clone();
        let head_path = join_virtual(&root, ".git/HEAD");
        if image.snapshot().fs().read_file(&head_path).await?.is_none() {
            return Err(GitSubstrateError::RepositoryNotFound { path: root });
        }
        let handle = GitRepositoryHandle {
            repository_id: request.repository_id,
            store: self.name.to_string(),
            root_path: image_descriptor.root_path,
            policy: request.policy,
            provenance: request.provenance,
            metadata: request.metadata,
        };
        self.hooks.on_open(&handle).await?;
        Ok(Arc::new(DeterministicGitRepository {
            snapshot_view: RwLock::new(image.snapshot()),
            image,
            handle,
            hooks: self.hooks.clone(),
            materializer: self.materializer.clone(),
        }))
    }
}

struct DeterministicGitRepository {
    snapshot_view: RwLock<Arc<dyn VolumeSnapshot>>,
    image: Arc<dyn GitRepositoryImage>,
    handle: GitRepositoryHandle,
    hooks: Arc<dyn GitExecutionHooks>,
    materializer: Option<Arc<dyn GitWorktreeMaterializer>>,
}

#[async_trait]
impl GitObjectDatabase for DeterministicGitRepository {
    async fn read_object(&self, oid: &str) -> Result<Option<GitObject>, GitSubstrateError> {
        let path = join_virtual(&self.handle.root_path, &format!(".git/objects/{oid}"));
        let Some(bytes) = self.current_fs().await?.read_file(&path).await? else {
            return Ok(None);
        };
        let (kind, data) = match bytes
            .splitn(2, |byte| *byte == b'\n')
            .collect::<Vec<_>>()
            .as_slice()
        {
            [kind, data] => (parse_object_kind(kind), data.to_vec()),
            _ => (GitObjectKind::Unknown, bytes),
        };
        let object = GitObject {
            oid: oid.to_string(),
            kind,
            data,
        };
        self.hooks.on_read_object(&object).await?;
        Ok(Some(object))
    }
}

#[async_trait]
impl GitRefDatabase for DeterministicGitRepository {
    async fn list_refs(&self) -> Result<Vec<GitReference>, GitSubstrateError> {
        let refs_root = join_virtual(&self.handle.root_path, ".git/refs");
        let refs = collect_refs(
            self.current_fs().await?,
            &self.handle.root_path,
            &refs_root,
            "refs",
        )
        .await?;
        self.hooks.on_read_refs(&refs).await?;
        Ok(refs)
    }
}

#[async_trait]
impl crate::GitIndexStore for DeterministicGitRepository {
    async fn index(&self) -> Result<GitIndexSnapshot, GitSubstrateError> {
        let path = join_virtual(&self.handle.root_path, ".git/index.json");
        let Some(bytes) = self.current_fs().await?.read_file(&path).await? else {
            return Ok(GitIndexSnapshot::default());
        };
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[async_trait]
impl GitRepository for DeterministicGitRepository {
    fn handle(&self) -> GitRepositoryHandle {
        self.handle.clone()
    }

    async fn head(&self) -> Result<GitHeadState, GitSubstrateError> {
        let head_path = join_virtual(&self.handle.root_path, ".git/HEAD");
        let fs = self.current_fs().await?;
        let head = fs.read_file(&head_path).await?.ok_or_else(|| {
            GitSubstrateError::RepositoryNotFound {
                path: self.handle.root_path.clone(),
            }
        })?;
        let head_text = String::from_utf8_lossy(&head).trim().to_string();
        if let Some(reference) = head_text.strip_prefix("ref: ") {
            let reference = reference.to_string();
            let oid =
                resolve_reference_to_oid(fs.clone(), &self.handle.root_path, &reference).await?;
            let state = GitHeadState {
                symbolic_ref: Some(reference),
                oid,
            };
            self.hooks.on_read_head(&state).await?;
            Ok(state)
        } else {
            let state = GitHeadState {
                symbolic_ref: None,
                oid: Some(head_text),
            };
            self.hooks.on_read_head(&state).await?;
            Ok(state)
        }
    }

    async fn status_with_options(
        &self,
        options: GitStatusOptions,
    ) -> Result<GitStatusReport, GitSubstrateError> {
        let scan = self
            .scan_repository(&options.pathspec, options.include_ignored)
            .await?;
        let report = GitStatusReport {
            dirty: scan.dirty,
            entries: scan
                .entries
                .into_iter()
                .filter(|entry| match entry.kind {
                    GitStatusKind::Ignored => options.include_ignored,
                    GitStatusKind::Untracked => options.include_untracked,
                    _ => true,
                })
                .filter(|entry| status_entry_matches_pathspec(entry, &options.pathspec))
                .map(|entry| GitStatusEntry {
                    path: entry.path,
                    kind: entry.kind,
                    previous_path: entry.previous_path,
                })
                .collect(),
        };
        self.hooks.on_status(&report).await?;
        Ok(report)
    }

    async fn diff(&self, request: GitDiffRequest) -> Result<GitDiffReport, GitSubstrateError> {
        let scan = self
            .scan_repository(&request.pathspec, request.include_ignored)
            .await?;
        let report = GitDiffReport {
            dirty: scan.dirty,
            entries: scan
                .entries
                .into_iter()
                .filter(|entry| match entry.kind {
                    GitStatusKind::Ignored => request.include_ignored,
                    GitStatusKind::Untracked => request.include_untracked,
                    _ => true,
                })
                .filter(|entry| status_entry_matches_pathspec(entry, &request.pathspec))
                .map(|entry| GitDiffEntry {
                    path: entry.path,
                    kind: diff_kind_for_status(entry.kind),
                    previous_path: entry.previous_path,
                    old_oid: entry.old_oid,
                    new_oid: entry.new_oid,
                })
                .collect(),
        };
        self.hooks.on_diff(&report).await?;
        Ok(report)
    }

    async fn checkout(
        &self,
        request: GitCheckoutRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCheckoutReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: self.handle.repository_id.clone(),
            });
        }
        let report = if let Some(materializer) = &self.materializer {
            materializer
                .materialize(&self.handle.repository_id, request, cancellation)
                .await?
        } else {
            self.checkout_into_vfs(request, cancellation).await?
        };
        self.hooks.on_checkout(&report).await?;
        Ok(report)
    }

    async fn commit(
        &self,
        request: GitCommitRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCommitReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: self.handle.repository_id.clone(),
            });
        }
        let volume = self.writable_volume()?;
        let fs = volume.fs();
        let reference_name = normalize_ref_name(&request.target_ref);
        let base_ref = request.base_ref.as_deref().map(normalize_ref_name);
        let reference_path = git_control_path(&self.handle.root_path, &reference_name);
        let previous_target = fs
            .read_file(&reference_path)
            .await?
            .map(|bytes| decode_reference_target(&bytes));
        let parent_oid = if let Some(base_ref) = base_ref.as_deref() {
            Some(self.resolve_target_ref(base_ref).await?.oid)
        } else if let Some(previous_target) = previous_target.as_deref() {
            Some(previous_target.to_string())
        } else {
            self.head().await?.oid
        };
        let object_id_algorithm =
            repository_object_id_algorithm(self.handle.provenance.object_format);
        let worktree = collect_worktree_entries(fs.clone(), &self.handle.root_path).await?;
        let parent_tree_oid = if let Some(parent_oid) = parent_oid.as_deref() {
            self.commit_tree_oid(parent_oid).await?
        } else {
            None
        };
        let committed = match parent_oid.as_deref() {
            Some(parent_oid) => {
                !self
                    .worktree_matches_target_oid(&worktree, parent_oid)
                    .await?
            }
            None => !worktree.is_empty(),
        };
        if request.require_changes && !committed {
            return Err(GitSubstrateError::NoChanges {
                target_ref: reference_name,
            });
        }
        if !committed && parent_oid.is_none() {
            let report = GitCommitReport {
                target_ref: reference_name,
                previous_target,
                commit_oid: String::new(),
                tree_oid: String::new(),
                parent_oid: None,
                committed: false,
            };
            self.hooks.on_commit(&report).await?;
            return Ok(report);
        }
        let tree_oid = if committed {
            self.write_worktree_tree(
                fs.as_ref(),
                &worktree,
                cancellation.as_ref(),
                object_id_algorithm,
            )
            .await?
        } else {
            parent_tree_oid.clone().unwrap_or_default()
        };
        let commit_oid = if committed {
            self.write_commit_object(
                fs.as_ref(),
                &tree_oid,
                parent_oid.as_deref(),
                &request.title,
                &request.body,
                object_id_algorithm,
            )
            .await?
        } else {
            parent_oid.clone().unwrap_or_default()
        };
        ensure_parent_directory(fs.as_ref(), &reference_path).await?;
        fs.write_file(
            &reference_path,
            format!("{commit_oid}\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
        if request.update_head {
            self.write_checkout_head(
                fs.as_ref(),
                &reference_name,
                &ResolvedTarget {
                    oid: commit_oid.clone(),
                    symbolic_ref: Some(reference_name.clone()),
                },
            )
            .await?;
        }
        let existing_index = GitIndexStore::index(self).await?;
        write_index_snapshot(
            fs.as_ref(),
            &self.handle.root_path,
            worktree_index_snapshot(existing_index.metadata, &worktree, object_id_algorithm),
        )
        .await?;
        self.refresh_snapshot().await?;

        let report = GitCommitReport {
            target_ref: reference_name,
            previous_target,
            commit_oid,
            tree_oid,
            parent_oid,
            committed,
        };
        self.hooks.on_commit(&report).await?;
        Ok(report)
    }

    async fn update_ref(
        &self,
        request: GitRefUpdate,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitRefUpdateReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: self.handle.repository_id.clone(),
            });
        }
        let volume = self.writable_volume()?;
        let fs = volume.fs();
        let reference_name = normalize_ref_name(&request.name);
        let reference_path = git_control_path(&self.handle.root_path, &reference_name);
        let current_target = fs
            .read_file(&reference_path)
            .await?
            .map(|bytes| decode_reference_target(&bytes));
        if request.previous_target != current_target {
            return Err(GitSubstrateError::ReferenceConflict {
                reference: reference_name.clone(),
                expected: request.previous_target,
                found: current_target,
            });
        }

        let payload = if request.symbolic {
            format!("ref: {}\n", request.target)
        } else {
            format!("{}\n", request.target)
        };
        ensure_parent_directory(fs.as_ref(), &reference_path).await?;
        fs.write_file(
            &reference_path,
            payload.into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
        self.refresh_snapshot().await?;

        let report = GitRefUpdateReport {
            reference: GitReference {
                name: reference_name,
                target: request.target,
            },
            previous_target: current_target,
        };
        self.hooks.on_update_ref(&report).await?;
        Ok(report)
    }
}

impl DeterministicGitRepository {
    async fn current_snapshot(&self) -> Result<Arc<dyn VolumeSnapshot>, GitSubstrateError> {
        Ok(self
            .snapshot_view
            .read()
            .expect("git snapshot view lock poisoned")
            .clone())
    }

    async fn current_fs(&self) -> Result<Arc<dyn ReadOnlyVfsFileSystem>, GitSubstrateError> {
        Ok(self.current_snapshot().await?.fs())
    }

    async fn refresh_snapshot(&self) -> Result<(), GitSubstrateError> {
        let Some(volume) = self.image.writable_volume() else {
            return Ok(());
        };
        let snapshot = volume.snapshot(SnapshotOptions::default()).await?;
        *self
            .snapshot_view
            .write()
            .expect("git snapshot view lock poisoned") = snapshot;
        Ok(())
    }

    fn writable_volume(&self) -> Result<Arc<dyn Volume>, GitSubstrateError> {
        self.image
            .writable_volume()
            .ok_or_else(|| GitSubstrateError::RepositoryReadOnly {
                repository_id: self.handle.repository_id.clone(),
            })
    }

    async fn scan_repository(
        &self,
        pathspec: &[String],
        include_ignored: bool,
    ) -> Result<RepositoryScan, GitSubstrateError> {
        let fs = self.current_fs().await?;
        let head = self.head().await?;
        let head_tree = if let Some(oid) = head.oid.as_deref() {
            self.tree_entries_for_target_oid(oid).await?
        } else {
            BTreeMap::new()
        };
        let index = GitIndexStore::index(self).await?;
        let tracked = build_tracked_entries(&index, &head_tree);
        let ignore_patterns = load_ignore_patterns(fs.clone(), &self.handle.root_path).await?;
        let mut worktree = collect_worktree_entries(fs, &self.handle.root_path).await?;
        let mut entries = Vec::new();

        for (path, tracked_entry) in tracked {
            let current = worktree.remove(&path);
            let staged_kind = tracked_entry.staged_kind();
            let rename_fingerprint = tracked_entry.rename_fingerprint(self).await?;
            match current {
                None => {
                    if tracked_entry.index_oid.is_some() {
                        entries.push(ScanEntry::deleted(
                            path,
                            tracked_entry.comparison_oid(),
                            rename_fingerprint,
                        ));
                    } else if staged_kind != GitStatusKind::Clean {
                        entries.push(scan_entry_from_staged_kind(
                            path,
                            &tracked_entry,
                            rename_fingerprint,
                        ));
                    }
                }
                Some(current) => {
                    if tracked_entry.index_oid.is_some()
                        && tracked_entry.worktree_matches_index(self, &current).await?
                    {
                        if staged_kind != GitStatusKind::Clean {
                            entries.push(scan_entry_from_staged_kind(
                                path,
                                &tracked_entry,
                                rename_fingerprint,
                            ));
                        }
                        continue;
                    }
                    if staged_kind != GitStatusKind::Clean {
                        entries.push(scan_entry_from_staged_kind(
                            path,
                            &tracked_entry,
                            rename_fingerprint,
                        ));
                    } else {
                        entries.push(ScanEntry::modified(
                            path,
                            tracked_entry.comparison_oid(),
                            current.oid_hint(),
                        ));
                    }
                }
            }
        }

        for (path, current) in worktree {
            if is_ignored_path(&path, &ignore_patterns) {
                if include_ignored {
                    entries.push(ScanEntry::ignored(path));
                }
                continue;
            }
            entries.push(ScanEntry::untracked(
                path,
                current.oid_hint(),
                Some(content_fingerprint(&current.payload_bytes())),
            ));
        }

        collapse_renames(&mut entries);
        entries.retain(|entry| status_entry_matches_pathspec(entry, pathspec));
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let dirty = entries
            .iter()
            .any(|entry| !matches!(entry.kind, GitStatusKind::Ignored));
        Ok(RepositoryScan { dirty, entries })
    }

    async fn checkout_into_vfs(
        &self,
        request: GitCheckoutRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCheckoutReport, GitSubstrateError> {
        let volume = self.writable_volume()?;
        let fs = volume.fs();
        let target = normalize_path(&request.materialize_path);
        let resolved = self.resolve_target_ref(&request.target_ref).await?;
        let entries = self.tree_entries_for_target_oid(&resolved.oid).await?;
        let filtered = entries
            .into_values()
            .filter(|entry| pathspec_matches(&entry.path, &request.pathspec))
            .collect::<Vec<_>>();

        let deleted_paths = if request.pathspec.is_empty() {
            clear_checkout_target(
                fs.as_ref(),
                &target,
                target == self.handle.root_path,
                &self.handle.repository_id,
                cancellation.as_ref(),
            )
            .await?
        } else {
            clear_checkout_pathspec(
                fs.as_ref(),
                &target,
                target == self.handle.root_path,
                &request.pathspec,
                &self.handle.repository_id,
                cancellation.as_ref(),
            )
            .await?
        };

        ensure_directory(fs.as_ref(), &target).await?;
        let written_paths = self
            .materialize_tree_entries(fs.as_ref(), &target, &filtered, cancellation.as_ref())
            .await?;

        if target == self.handle.root_path {
            if request.update_head && request.pathspec.is_empty() {
                self.write_checkout_head(fs.as_ref(), &request.target_ref, &resolved)
                    .await?;
            }
            let existing_index = GitIndexStore::index(self).await?;
            write_index_snapshot(
                fs.as_ref(),
                &self.handle.root_path,
                checkout_index_snapshot(&existing_index, &filtered, &request.pathspec),
            )
            .await?;
        }
        self.refresh_snapshot().await?;

        Ok(GitCheckoutReport {
            target_ref: request.target_ref,
            materialized_path: target,
            written_paths,
            deleted_paths,
            head_oid: Some(resolved.oid),
        })
    }

    async fn materialize_tree_entries(
        &self,
        fs: &dyn VfsFileSystem,
        target_root: &str,
        entries: &[TreeEntry],
        cancellation: &dyn GitCancellationToken,
    ) -> Result<usize, GitSubstrateError> {
        let mut written = 0;
        for entry in entries {
            if cancellation.is_cancelled() {
                return Err(GitSubstrateError::Cancelled {
                    repository_id: self.handle.repository_id.clone(),
                });
            }
            let destination = join_virtual(target_root, &entry.path);
            if let Some(parent) = parent_path(&destination) {
                ensure_directory(fs, &parent).await?;
            }
            if is_symlink_mode(entry.mode) {
                let target =
                    String::from_utf8(self.read_blob_payload(&entry.oid).await?).map_err(|_| {
                        GitSubstrateError::InvalidObject {
                            oid: entry.oid.clone(),
                            message: "symlink blob is not valid UTF-8".to_string(),
                        }
                    })?;
                remove_existing_path(fs, &destination).await?;
                fs.symlink(&target, &destination).await?;
            } else {
                fs.write_file(
                    &destination,
                    self.read_blob_payload(&entry.oid).await?,
                    CreateOptions {
                        create_parents: true,
                        overwrite: true,
                        mode: entry.mode & 0o777,
                    },
                )
                .await?;
            }
            written += 1;
        }
        Ok(written)
    }

    async fn write_checkout_head(
        &self,
        fs: &dyn VfsFileSystem,
        target_ref: &str,
        resolved: &ResolvedTarget,
    ) -> Result<(), GitSubstrateError> {
        let head_path = git_control_path(&self.handle.root_path, "HEAD");
        let payload = if let Some(reference) = &resolved.symbolic_ref {
            format!("ref: {}\n", reference)
        } else if target_ref == "HEAD" {
            return Ok(());
        } else {
            format!("{}\n", resolved.oid)
        };
        fs.write_file(
            &head_path,
            payload.into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn resolve_target_ref(
        &self,
        target_ref: &str,
    ) -> Result<ResolvedTarget, GitSubstrateError> {
        if target_ref == "HEAD" {
            let head = self.head().await?;
            let oid = head
                .oid
                .ok_or_else(|| GitSubstrateError::ReferenceNotFound {
                    reference: "HEAD".to_string(),
                })?;
            return Ok(ResolvedTarget {
                oid,
                symbolic_ref: head.symbolic_ref,
            });
        }

        let candidates = if target_ref.starts_with("refs/") {
            vec![target_ref.to_string()]
        } else {
            vec![
                target_ref.to_string(),
                format!("refs/heads/{target_ref}"),
                format!("refs/tags/{target_ref}"),
            ]
        };
        for candidate in candidates {
            let path = git_control_path(&self.handle.root_path, &candidate);
            if self.current_fs().await?.read_file(&path).await?.is_some() {
                return Ok(ResolvedTarget {
                    oid: resolve_reference_to_oid(
                        self.current_fs().await?,
                        &self.handle.root_path,
                        &candidate,
                    )
                    .await?
                    .ok_or_else(|| GitSubstrateError::ReferenceNotFound {
                        reference: candidate.clone(),
                    })?,
                    symbolic_ref: Some(candidate),
                });
            }
        }
        if self.read_object(target_ref).await?.is_some() {
            return Ok(ResolvedTarget {
                oid: target_ref.to_string(),
                symbolic_ref: None,
            });
        }
        Err(GitSubstrateError::ReferenceNotFound {
            reference: target_ref.to_string(),
        })
    }

    async fn tree_entries_for_target_oid(
        &self,
        oid: &str,
    ) -> Result<BTreeMap<String, TreeEntry>, GitSubstrateError> {
        let object =
            self.read_object(oid)
                .await?
                .ok_or_else(|| GitSubstrateError::ObjectNotFound {
                    oid: oid.to_string(),
                })?;
        match object.kind {
            GitObjectKind::Commit => {
                let tree_oid = parse_commit_tree_oid(&object)?;
                let mut entries = BTreeMap::new();
                self.collect_tree_entries(&tree_oid, "", &mut entries)
                    .await?;
                Ok(entries)
            }
            GitObjectKind::Tree => {
                let mut entries = BTreeMap::new();
                self.collect_tree_entries(oid, "", &mut entries).await?;
                Ok(entries)
            }
            other => Err(GitSubstrateError::InvalidObject {
                oid: oid.to_string(),
                message: format!("expected commit or tree object, found {other:?}"),
            }),
        }
    }

    async fn collect_tree_entries(
        &self,
        tree_oid: &str,
        prefix: &str,
        output: &mut BTreeMap<String, TreeEntry>,
    ) -> Result<(), GitSubstrateError> {
        let object =
            self.read_object(tree_oid)
                .await?
                .ok_or_else(|| GitSubstrateError::ObjectNotFound {
                    oid: tree_oid.to_string(),
                })?;
        if object.kind != GitObjectKind::Tree {
            return Err(GitSubstrateError::InvalidObject {
                oid: tree_oid.to_string(),
                message: "tree traversal encountered a non-tree object".to_string(),
            });
        }

        for record in parse_tree_entries(&object)? {
            let path = if prefix.is_empty() {
                record.path.clone()
            } else {
                format!("{prefix}/{}", record.path)
            };
            if record.kind == GitObjectKind::Tree {
                Box::pin(self.collect_tree_entries(&record.oid, &path, output)).await?;
            } else {
                output.insert(
                    path.clone(),
                    TreeEntry {
                        path,
                        oid: record.oid,
                        mode: record.mode,
                    },
                );
            }
        }
        Ok(())
    }

    async fn read_blob_payload(&self, oid: &str) -> Result<Vec<u8>, GitSubstrateError> {
        let object =
            self.read_object(oid)
                .await?
                .ok_or_else(|| GitSubstrateError::ObjectNotFound {
                    oid: oid.to_string(),
                })?;
        match object.kind {
            GitObjectKind::Blob | GitObjectKind::Unknown => Ok(object.data),
            other => Err(GitSubstrateError::InvalidObject {
                oid: oid.to_string(),
                message: format!("expected blob object, found {other:?}"),
            }),
        }
    }

    async fn worktree_matches_target_oid(
        &self,
        worktree: &BTreeMap<String, WorktreeEntry>,
        target_oid: &str,
    ) -> Result<bool, GitSubstrateError> {
        let target_entries = self.tree_entries_for_target_oid(target_oid).await?;
        if worktree.len() != target_entries.len() {
            return Ok(false);
        }
        for (path, worktree_entry) in worktree {
            let Some(target_entry) = target_entries.get(path) else {
                return Ok(false);
            };
            if worktree_mode(worktree_entry) != target_entry.mode {
                return Ok(false);
            }
            if self.read_blob_payload(&target_entry.oid).await? != worktree_entry.payload_bytes() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn commit_tree_oid(&self, oid: &str) -> Result<Option<String>, GitSubstrateError> {
        let Some(object) = self.read_object(oid).await? else {
            return Ok(None);
        };
        match object.kind {
            GitObjectKind::Commit => Ok(Some(parse_commit_tree_oid(&object)?)),
            GitObjectKind::Tree => Ok(Some(oid.to_string())),
            _ => Ok(None),
        }
    }

    async fn write_worktree_tree(
        &self,
        fs: &dyn VfsFileSystem,
        worktree: &BTreeMap<String, WorktreeEntry>,
        cancellation: &dyn GitCancellationToken,
        object_id_algorithm: GitObjectIdAlgorithm,
    ) -> Result<String, GitSubstrateError> {
        let mut directories = BTreeMap::<String, Vec<TreeRecord>>::new();
        for (path, entry) in worktree {
            if cancellation.is_cancelled() {
                return Err(GitSubstrateError::Cancelled {
                    repository_id: self.handle.repository_id.clone(),
                });
            }
            let blob_oid = write_git_object(
                fs,
                &self.handle.root_path,
                GitObjectKind::Blob,
                &entry.payload_bytes(),
                object_id_algorithm,
            )
            .await?;
            let mode = worktree_mode(entry);
            let mut current_dir = String::new();
            let components = path_components_owned(path);
            for component in &components[..components.len().saturating_sub(1)] {
                directories
                    .entry(current_dir.clone())
                    .or_default()
                    .push(TreeRecord::directory(component.clone()));
                current_dir = if current_dir.is_empty() {
                    component.clone()
                } else {
                    format!("{current_dir}/{component}")
                };
            }
            let file_name =
                components
                    .last()
                    .cloned()
                    .ok_or_else(|| GitSubstrateError::Bridge {
                        operation: "commit",
                        message: format!("worktree path is missing a terminal component: {path}"),
                    })?;
            directories
                .entry(current_dir)
                .or_default()
                .push(TreeRecord::file(file_name, blob_oid, mode));
        }
        write_directory_tree_objects(
            fs,
            &self.handle.root_path,
            "",
            &mut directories,
            object_id_algorithm,
        )
        .await
    }

    async fn write_commit_object(
        &self,
        fs: &dyn VfsFileSystem,
        tree_oid: &str,
        parent_oid: Option<&str>,
        title: &str,
        body: &str,
        object_id_algorithm: GitObjectIdAlgorithm,
    ) -> Result<String, GitSubstrateError> {
        let mut payload = format!("tree {tree_oid}\n");
        if let Some(parent_oid) = parent_oid {
            payload.push_str(&format!("parent {parent_oid}\n"));
        }
        payload.push('\n');
        payload.push_str(title.trim());
        if !body.trim().is_empty() {
            payload.push_str("\n\n");
            payload.push_str(body.trim());
        }
        payload.push('\n');
        write_git_object(
            fs,
            &self.handle.root_path,
            GitObjectKind::Commit,
            payload.as_bytes(),
            object_id_algorithm,
        )
        .await
    }
}

#[derive(Clone, Debug, Default)]
struct RepositoryScan {
    dirty: bool,
    entries: Vec<ScanEntry>,
}

#[derive(Clone, Debug)]
struct ScanEntry {
    path: String,
    kind: GitStatusKind,
    previous_path: Option<String>,
    old_oid: Option<String>,
    new_oid: Option<String>,
    rename_fingerprint: Option<String>,
}

impl ScanEntry {
    fn modified(path: String, old_oid: Option<String>, new_oid: Option<String>) -> Self {
        Self {
            path,
            kind: GitStatusKind::Modified,
            previous_path: None,
            old_oid,
            new_oid,
            rename_fingerprint: None,
        }
    }

    fn added(path: String, new_oid: Option<String>, rename_fingerprint: Option<String>) -> Self {
        Self {
            path,
            kind: GitStatusKind::Added,
            previous_path: None,
            old_oid: None,
            new_oid,
            rename_fingerprint,
        }
    }

    fn deleted(path: String, old_oid: Option<String>, rename_fingerprint: Option<String>) -> Self {
        Self {
            path,
            kind: GitStatusKind::Deleted,
            previous_path: None,
            old_oid,
            new_oid: None,
            rename_fingerprint,
        }
    }

    fn untracked(
        path: String,
        new_oid: Option<String>,
        rename_fingerprint: Option<String>,
    ) -> Self {
        Self {
            path,
            kind: GitStatusKind::Untracked,
            previous_path: None,
            old_oid: None,
            new_oid,
            rename_fingerprint,
        }
    }

    fn ignored(path: String) -> Self {
        Self {
            path,
            kind: GitStatusKind::Ignored,
            previous_path: None,
            old_oid: None,
            new_oid: None,
            rename_fingerprint: None,
        }
    }
}

#[derive(Clone, Debug)]
struct TrackedEntry {
    head_oid: Option<String>,
    head_mode: Option<u32>,
    index_oid: Option<String>,
    index_mode: Option<u32>,
}

impl TrackedEntry {
    fn staged_kind(&self) -> GitStatusKind {
        match (&self.head_oid, &self.index_oid) {
            (None, Some(_)) => GitStatusKind::Added,
            (Some(_), None) => GitStatusKind::Deleted,
            (Some(head_oid), Some(index_oid))
                if head_oid != index_oid || self.head_mode != self.index_mode =>
            {
                GitStatusKind::Modified
            }
            _ => GitStatusKind::Clean,
        }
    }

    fn comparison_oid(&self) -> Option<String> {
        self.index_oid.clone().or_else(|| self.head_oid.clone())
    }

    fn comparison_mode(&self) -> Option<u32> {
        self.index_mode.or(self.head_mode)
    }

    async fn worktree_matches_index(
        &self,
        repository: &DeterministicGitRepository,
        current: &WorktreeEntry,
    ) -> Result<bool, GitSubstrateError> {
        let Some(mode) = self.comparison_mode() else {
            return Ok(false);
        };
        if !mode_matches(mode, current.kind, current.stats.mode) {
            return Ok(false);
        }
        let Some(oid) = self.comparison_oid() else {
            return Ok(true);
        };
        let Some(object) = repository.read_object(&oid).await? else {
            return Ok(true);
        };
        if !matches!(object.kind, GitObjectKind::Blob | GitObjectKind::Unknown) {
            return Ok(true);
        }
        Ok(object.data == current.payload_bytes())
    }

    async fn rename_fingerprint(
        &self,
        repository: &DeterministicGitRepository,
    ) -> Result<Option<String>, GitSubstrateError> {
        let Some(oid) = self.comparison_oid() else {
            return Ok(None);
        };
        let Some(object) = repository.read_object(&oid).await? else {
            return Ok(None);
        };
        if !matches!(object.kind, GitObjectKind::Blob | GitObjectKind::Unknown) {
            return Ok(None);
        }
        Ok(Some(content_fingerprint(&object.data)))
    }
}

#[derive(Clone, Debug)]
struct WorktreeEntry {
    kind: FileKind,
    stats: Stats,
    payload: WorktreePayload,
}

impl WorktreeEntry {
    fn payload_bytes(&self) -> Vec<u8> {
        match &self.payload {
            WorktreePayload::File(bytes) => bytes.clone(),
            WorktreePayload::Symlink(target) => target.as_bytes().to_vec(),
        }
    }

    fn oid_hint(&self) -> Option<String> {
        None
    }
}

#[derive(Clone, Debug)]
enum WorktreePayload {
    File(Vec<u8>),
    Symlink(String),
}

#[derive(Clone, Debug)]
struct TreeEntry {
    path: String,
    oid: String,
    mode: u32,
}

#[derive(Clone, Debug)]
struct ParsedTreeRecord {
    path: String,
    oid: String,
    mode: u32,
    kind: GitObjectKind,
}

#[derive(Clone, Debug)]
struct ResolvedTarget {
    oid: String,
    symbolic_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TreeRecord {
    name: String,
    oid: Option<String>,
    mode: u32,
    kind: GitObjectKind,
}

impl TreeRecord {
    fn file(name: String, oid: String, mode: u32) -> Self {
        Self {
            name,
            oid: Some(oid),
            mode,
            kind: GitObjectKind::Blob,
        }
    }

    fn directory(name: String) -> Self {
        Self {
            name,
            oid: None,
            mode: 0o040000,
            kind: GitObjectKind::Tree,
        }
    }
}

fn build_tracked_entries(
    index: &GitIndexSnapshot,
    head_tree: &BTreeMap<String, TreeEntry>,
) -> BTreeMap<String, TrackedEntry> {
    let mut tracked = BTreeMap::new();
    for (path, entry) in head_tree {
        tracked.insert(
            path.clone(),
            TrackedEntry {
                head_oid: Some(entry.oid.clone()),
                head_mode: Some(entry.mode),
                index_oid: None,
                index_mode: None,
            },
        );
    }
    if index.entries.is_empty() {
        return tracked;
    }
    for entry in &index.entries {
        tracked
            .entry(entry.path.clone())
            .and_modify(|tracked_entry| {
                tracked_entry.index_oid = entry.oid.clone();
                tracked_entry.index_mode = Some(entry.mode);
            })
            .or_insert_with(|| TrackedEntry {
                head_oid: None,
                head_mode: None,
                index_oid: entry.oid.clone(),
                index_mode: Some(entry.mode),
            });
    }
    tracked
}

fn checkout_index_snapshot(
    existing: &GitIndexSnapshot,
    target_entries: &[TreeEntry],
    pathspec: &[String],
) -> GitIndexSnapshot {
    let mut entries = existing
        .entries
        .iter()
        .filter(|entry| !pathspec_matches(&entry.path, pathspec))
        .cloned()
        .collect::<Vec<_>>();
    entries.extend(target_entries.iter().map(|entry| GitIndexEntry {
        path: entry.path.clone(),
        oid: Some(entry.oid.clone()),
        mode: entry.mode,
    }));
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    GitIndexSnapshot {
        entries,
        metadata: existing.metadata.clone(),
    }
}

fn worktree_index_snapshot(
    metadata: BTreeMap<String, JsonValue>,
    worktree: &BTreeMap<String, WorktreeEntry>,
    object_id_algorithm: GitObjectIdAlgorithm,
) -> GitIndexSnapshot {
    let mut entries = worktree
        .iter()
        .map(|(path, entry)| GitIndexEntry {
            path: path.clone(),
            oid: Some(git_object_oid(
                GitObjectKind::Blob,
                &entry.payload_bytes(),
                object_id_algorithm,
            )),
            mode: worktree_mode(entry),
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    GitIndexSnapshot { entries, metadata }
}

fn scan_entry_from_staged_kind(
    path: String,
    tracked_entry: &TrackedEntry,
    rename_fingerprint: Option<String>,
) -> ScanEntry {
    match tracked_entry.staged_kind() {
        GitStatusKind::Added => {
            ScanEntry::added(path, tracked_entry.index_oid.clone(), rename_fingerprint)
        }
        GitStatusKind::Deleted => {
            ScanEntry::deleted(path, tracked_entry.head_oid.clone(), rename_fingerprint)
        }
        GitStatusKind::Modified => ScanEntry::modified(
            path,
            tracked_entry.head_oid.clone(),
            tracked_entry.index_oid.clone(),
        ),
        GitStatusKind::Clean
        | GitStatusKind::Untracked
        | GitStatusKind::Ignored
        | GitStatusKind::Renamed => ScanEntry::modified(
            path,
            tracked_entry.head_oid.clone(),
            tracked_entry.index_oid.clone(),
        ),
    }
}

fn collapse_renames(entries: &mut Vec<ScanEntry>) {
    let deleted = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            entry.kind == GitStatusKind::Deleted && entry.rename_fingerprint.is_some()
        })
        .map(|(index, entry)| {
            (
                entry
                    .rename_fingerprint
                    .clone()
                    .expect("rename fingerprint"),
                index,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut paired_deleted = Vec::new();
    let mut renamed = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        if entry.kind != GitStatusKind::Untracked {
            continue;
        }
        let Some(fingerprint) = entry.rename_fingerprint.as_ref() else {
            continue;
        };
        let Some(deleted_index) = deleted.get(fingerprint) else {
            continue;
        };
        paired_deleted.push(*deleted_index);
        renamed.push((
            index,
            ScanEntry {
                path: entry.path.clone(),
                kind: GitStatusKind::Renamed,
                previous_path: Some(entries[*deleted_index].path.clone()),
                old_oid: entries[*deleted_index].old_oid.clone(),
                new_oid: entry.new_oid.clone(),
                rename_fingerprint: None,
            },
        ));
    }

    if renamed.is_empty() {
        return;
    }

    let deleted_set = paired_deleted
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let renamed_map = renamed.into_iter().collect::<BTreeMap<_, _>>();
    let mut next = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        if deleted_set.contains(&index) {
            continue;
        }
        if let Some(replacement) = renamed_map.get(&index) {
            next.push(replacement.clone());
            continue;
        }
        next.push(entry.clone());
    }
    *entries = next;
}

fn diff_kind_for_status(kind: GitStatusKind) -> GitDiffKind {
    match kind {
        GitStatusKind::Clean => GitDiffKind::Modified,
        GitStatusKind::Modified => GitDiffKind::Modified,
        GitStatusKind::Added => GitDiffKind::Added,
        GitStatusKind::Deleted => GitDiffKind::Deleted,
        GitStatusKind::Untracked => GitDiffKind::Untracked,
        GitStatusKind::Ignored => GitDiffKind::Ignored,
        GitStatusKind::Renamed => GitDiffKind::Renamed,
    }
}

fn content_fingerprint(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn repository_object_id_algorithm(object_format: GitObjectFormat) -> GitObjectIdAlgorithm {
    match object_format {
        GitObjectFormat::Sha1 => GitObjectIdAlgorithm::Sha1,
        GitObjectFormat::Sha256 => GitObjectIdAlgorithm::Sha256,
    }
}

fn status_entry_matches_pathspec(entry: &ScanEntry, pathspec: &[String]) -> bool {
    if pathspec.is_empty() {
        return true;
    }
    pathspec_matches(&entry.path, pathspec)
        || entry
            .previous_path
            .as_ref()
            .is_some_and(|path| pathspec_matches(path, pathspec))
}

async fn discover_repository_root(
    fs: Arc<dyn ReadOnlyVfsFileSystem>,
    image_root: &str,
    start_path: &str,
) -> Result<Option<String>, GitSubstrateError> {
    let normalized_root = normalize_path(image_root);
    let start_path = if start_path.starts_with('/') {
        normalize_path(start_path)
    } else {
        join_virtual(&normalized_root, start_path)
    };
    for candidate in ancestor_paths(&start_path, &normalized_root) {
        let head = join_virtual(&candidate, ".git/HEAD");
        if fs.read_file(&head).await?.is_some() {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

async fn read_head_ref(
    fs: Arc<dyn ReadOnlyVfsFileSystem>,
    root: &str,
) -> Result<Option<String>, GitSubstrateError> {
    let path = join_virtual(root, ".git/HEAD");
    let Some(bytes) = fs.read_file(&path).await? else {
        return Ok(None);
    };
    let text = String::from_utf8_lossy(&bytes).trim().to_string();
    Ok(text.strip_prefix("ref: ").map(str::to_string))
}

async fn collect_refs(
    fs: Arc<dyn ReadOnlyVfsFileSystem>,
    root: &str,
    path: &str,
    relative: &str,
) -> Result<Vec<GitReference>, GitSubstrateError> {
    let mut output = Vec::new();
    let mut stack = vec![(path.to_string(), relative.to_string())];
    while let Some((path, relative)) = stack.pop() {
        let entries = match fs.readdir(&path).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries {
            let child_path = join_virtual(&path, &entry.name);
            let child_relative = format!("{relative}/{}", entry.name);
            match entry.kind {
                FileKind::Directory => stack.push((child_path, child_relative)),
                FileKind::File => {
                    if let Some(bytes) = fs.read_file(&child_path).await? {
                        let target = if bytes.starts_with(b"ref: ") {
                            resolve_reference_to_oid(
                                fs.clone(),
                                root,
                                &decode_reference_target(&bytes),
                            )
                            .await?
                            .unwrap_or_else(|| decode_reference_target(&bytes))
                        } else {
                            decode_reference_target(&bytes)
                        };
                        output.push(GitReference {
                            name: child_relative,
                            target,
                        });
                    }
                }
                FileKind::Symlink => {}
            }
        }
    }
    output.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(output)
}

async fn resolve_reference_to_oid(
    fs: Arc<dyn ReadOnlyVfsFileSystem>,
    root: &str,
    reference: &str,
) -> Result<Option<String>, GitSubstrateError> {
    let mut current = reference.to_string();
    for _ in 0..16 {
        let path = git_control_path(root, &current);
        let Some(bytes) = fs.read_file(&path).await? else {
            return Ok(None);
        };
        let target = decode_reference_target(&bytes);
        if bytes.starts_with(b"ref: ") {
            current = target;
        } else {
            return Ok(Some(target));
        }
    }
    Err(GitSubstrateError::ReferenceNotFound {
        reference: reference.to_string(),
    })
}

async fn collect_worktree_entries(
    fs: Arc<dyn ReadOnlyVfsFileSystem>,
    root: &str,
) -> Result<BTreeMap<String, WorktreeEntry>, GitSubstrateError> {
    let mut output = BTreeMap::new();
    let mut stack = vec![(normalize_path(root), String::new())];
    while let Some((directory, relative_prefix)) = stack.pop() {
        for child in fs.readdir_plus(&directory).await? {
            if directory == normalize_path(root) && child.entry.name == ".git" {
                continue;
            }
            let child_path = join_virtual(&directory, &child.entry.name);
            let relative_path = if relative_prefix.is_empty() {
                child.entry.name.clone()
            } else {
                format!("{relative_prefix}/{}", child.entry.name)
            };
            match child.entry.kind {
                FileKind::Directory => stack.push((child_path, relative_path)),
                FileKind::File => {
                    let payload = fs.read_file(&child_path).await?.ok_or_else(|| {
                        GitSubstrateError::RepositoryNotFound {
                            path: child_path.clone(),
                        }
                    })?;
                    output.insert(
                        relative_path,
                        WorktreeEntry {
                            kind: FileKind::File,
                            stats: child.stats,
                            payload: WorktreePayload::File(payload),
                        },
                    );
                }
                FileKind::Symlink => {
                    let target = fs.readlink(&child_path).await?;
                    output.insert(
                        relative_path,
                        WorktreeEntry {
                            kind: FileKind::Symlink,
                            stats: child.stats,
                            payload: WorktreePayload::Symlink(target),
                        },
                    );
                }
            }
        }
    }
    Ok(output)
}

async fn load_ignore_patterns(
    fs: Arc<dyn ReadOnlyVfsFileSystem>,
    root: &str,
) -> Result<Vec<String>, GitSubstrateError> {
    let path = join_virtual(root, ".gitignore");
    let Some(bytes) = fs.read_file(&path).await? else {
        return Ok(Vec::new());
    };
    Ok(String::from_utf8_lossy(&bytes)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect())
}

fn is_ignored_path(path: &str, patterns: &[String]) -> bool {
    patterns
        .iter()
        .any(|pattern| ignore_pattern_matches(pattern, path))
}

fn ignore_pattern_matches(pattern: &str, path: &str) -> bool {
    let mut pattern = pattern.trim();
    let anchored = pattern.starts_with('/');
    if anchored {
        pattern = pattern.trim_start_matches('/');
    }
    let directory_only = pattern.ends_with('/');
    let pattern = pattern.trim_end_matches('/');
    if pattern.is_empty() {
        return false;
    }
    if directory_only {
        return if anchored {
            path == pattern || path.starts_with(&format!("{pattern}/"))
        } else {
            path_components(path)
                .into_iter()
                .scan(String::new(), |prefix, component| {
                    if prefix.is_empty() {
                        *prefix = component.to_string();
                    } else {
                        prefix.push('/');
                        prefix.push_str(component);
                    }
                    Some(prefix.clone())
                })
                .any(|candidate| {
                    candidate == pattern || candidate.starts_with(&format!("{pattern}/"))
                })
        };
    }
    if anchored || pattern.contains('/') {
        return pathspec_component_matches(path, pattern);
    }
    path_components(path)
        .into_iter()
        .any(|component| wildcard_matches(component, pattern))
}

fn pathspec_matches(path: &str, pathspec: &[String]) -> bool {
    if pathspec.is_empty() {
        return true;
    }
    pathspec
        .iter()
        .map(|entry| entry.trim())
        .any(|entry| match entry {
            "" | "." | "./" => true,
            _ => {
                let entry = entry.trim_start_matches("./").trim_start_matches('/');
                if entry.ends_with('/') {
                    path == entry.trim_end_matches('/')
                        || path.starts_with(&format!("{}/", entry.trim_end_matches('/')))
                } else if entry.contains('*') || entry.contains('?') {
                    pathspec_component_matches(path, entry)
                } else {
                    path == entry || path.starts_with(&format!("{entry}/"))
                }
            }
        })
}

fn pathspec_component_matches(path: &str, pattern: &str) -> bool {
    if !pattern.contains('/') {
        return wildcard_matches(path, pattern)
            || path_components(path)
                .into_iter()
                .any(|component| wildcard_matches(component, pattern));
    }
    wildcard_path_matches(path, pattern)
}

fn wildcard_matches(input: &str, pattern: &str) -> bool {
    wildcard_match_bytes(input.as_bytes(), pattern.as_bytes())
}

fn wildcard_path_matches(path: &str, pattern: &str) -> bool {
    let path_segments = path_components(path);
    let pattern_segments = path_components(pattern);
    path_segments.len() == pattern_segments.len()
        && path_segments
            .iter()
            .zip(pattern_segments.iter())
            .all(|(path, pattern)| wildcard_matches(path, pattern))
}

fn wildcard_match_bytes(input: &[u8], pattern: &[u8]) -> bool {
    if pattern.is_empty() {
        return input.is_empty();
    }
    match pattern[0] {
        b'*' => {
            wildcard_match_bytes(input, &pattern[1..])
                || (!input.is_empty() && wildcard_match_bytes(&input[1..], pattern))
        }
        b'?' => !input.is_empty() && wildcard_match_bytes(&input[1..], &pattern[1..]),
        byte => {
            !input.is_empty()
                && input[0] == byte
                && wildcard_match_bytes(&input[1..], &pattern[1..])
        }
    }
}

fn path_components(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|component| !component.is_empty())
        .collect()
}

fn parse_object_kind(kind: &[u8]) -> GitObjectKind {
    match kind {
        b"blob" => GitObjectKind::Blob,
        b"commit" => GitObjectKind::Commit,
        b"tree" => GitObjectKind::Tree,
        b"tag" => GitObjectKind::Tag,
        _ => GitObjectKind::Unknown,
    }
}

async fn write_git_object(
    fs: &dyn VfsFileSystem,
    root: &str,
    kind: GitObjectKind,
    payload: &[u8],
    object_id_algorithm: GitObjectIdAlgorithm,
) -> Result<String, GitSubstrateError> {
    let oid = git_object_oid(kind, payload, object_id_algorithm);
    fs.write_file(
        &git_control_path(root, &format!("objects/{oid}")),
        [object_kind_name(kind).as_bytes(), b"\n", payload].concat(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await?;
    Ok(oid)
}

fn git_object_oid(
    kind: GitObjectKind,
    payload: &[u8],
    object_id_algorithm: GitObjectIdAlgorithm,
) -> String {
    match object_id_algorithm {
        GitObjectIdAlgorithm::Sha1 => {
            let mut hasher = Sha1::new();
            hasher.update(git_object_header(kind, payload.len()).as_bytes());
            hasher.update(payload);
            let digest = hasher.finalize();
            let mut hex = String::with_capacity(digest.len() * 2);
            for byte in digest {
                hex.push_str(&format!("{byte:02x}"));
            }
            hex
        }
        GitObjectIdAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(git_object_header(kind, payload.len()).as_bytes());
            hasher.update(payload);
            let digest = hasher.finalize();
            let mut hex = String::with_capacity(digest.len() * 2);
            for byte in digest {
                hex.push_str(&format!("{byte:02x}"));
            }
            hex
        }
    }
}

fn git_object_header(kind: GitObjectKind, payload_len: usize) -> String {
    format!("{} {}\0", object_kind_name(kind), payload_len)
}

fn object_kind_name(kind: GitObjectKind) -> &'static str {
    match kind {
        GitObjectKind::Blob => "blob",
        GitObjectKind::Commit => "commit",
        GitObjectKind::Tree => "tree",
        GitObjectKind::Tag => "tag",
        GitObjectKind::Unknown => "unknown",
    }
}

fn parse_commit_tree_oid(object: &GitObject) -> Result<String, GitSubstrateError> {
    let text = String::from_utf8_lossy(&object.data);
    text.lines()
        .find_map(|line| {
            line.strip_prefix("tree ")
                .map(str::trim)
                .map(str::to_string)
        })
        .ok_or_else(|| GitSubstrateError::InvalidObject {
            oid: object.oid.clone(),
            message: "commit object is missing a tree header".to_string(),
        })
}

fn parse_tree_entries(object: &GitObject) -> Result<Vec<ParsedTreeRecord>, GitSubstrateError> {
    let mut entries = Vec::new();
    for line in String::from_utf8_lossy(&object.data).lines() {
        if line.trim().is_empty() {
            continue;
        }
        let (header, path) =
            line.split_once('\t')
                .ok_or_else(|| GitSubstrateError::InvalidObject {
                    oid: object.oid.clone(),
                    message: format!("tree entry is missing a tab separator: {line}"),
                })?;
        let mut parts = header.split_whitespace();
        let mode = parts
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: object.oid.clone(),
                message: format!("tree entry is missing a mode: {line}"),
            })
            .and_then(|mode| {
                u32::from_str_radix(mode, 8).map_err(|_| GitSubstrateError::InvalidObject {
                    oid: object.oid.clone(),
                    message: format!("tree entry has an invalid octal mode: {line}"),
                })
            })?;
        let kind = parts
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: object.oid.clone(),
                message: format!("tree entry is missing a kind: {line}"),
            })
            .map(|kind| match kind {
                "blob" => GitObjectKind::Blob,
                "tree" => GitObjectKind::Tree,
                "commit" => GitObjectKind::Commit,
                "tag" => GitObjectKind::Tag,
                _ => GitObjectKind::Unknown,
            })?;
        let oid = parts
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: object.oid.clone(),
                message: format!("tree entry is missing an oid: {line}"),
            })?
            .to_string();
        entries.push(ParsedTreeRecord {
            path: path.to_string(),
            oid,
            mode,
            kind,
        });
    }
    Ok(entries)
}

async fn write_directory_tree_objects(
    fs: &dyn VfsFileSystem,
    root: &str,
    directory: &str,
    directories: &mut BTreeMap<String, Vec<TreeRecord>>,
    object_id_algorithm: GitObjectIdAlgorithm,
) -> Result<String, GitSubstrateError> {
    let child_directories = directories
        .keys()
        .filter(|candidate| {
            if directory.is_empty() {
                !candidate.is_empty() && !candidate.contains('/')
            } else {
                candidate
                    .strip_prefix(directory)
                    .is_some_and(|suffix| suffix.starts_with('/'))
                    && candidate[directory.len() + 1..].find('/').is_none()
            }
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut entries = directories.remove(directory).unwrap_or_default();
    for child in child_directories {
        let name = child
            .rsplit('/')
            .next()
            .expect("child directory name should exist")
            .to_string();
        let oid = Box::pin(write_directory_tree_objects(
            fs,
            root,
            &child,
            directories,
            object_id_algorithm,
        ))
        .await?;
        entries.retain(|entry| !(entry.kind == GitObjectKind::Tree && entry.name == name));
        entries.push(TreeRecord {
            name,
            oid: Some(oid),
            mode: 0o040000,
            kind: GitObjectKind::Tree,
        });
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries.dedup_by(|left, right| left.kind == right.kind && left.name == right.name);
    let payload = entries
        .into_iter()
        .map(|entry| {
            format!(
                "{:06o} {} {}\t{}\n",
                entry.mode,
                object_kind_name(entry.kind),
                entry.oid.unwrap_or_default(),
                entry.name
            )
        })
        .collect::<String>();
    write_git_object(
        fs,
        root,
        GitObjectKind::Tree,
        payload.as_bytes(),
        object_id_algorithm,
    )
    .await
}

fn worktree_mode(entry: &WorktreeEntry) -> u32 {
    match entry.kind {
        FileKind::Symlink => 0o120000,
        FileKind::Directory => 0o040000,
        FileKind::File => {
            if (entry.stats.mode & 0o111) != 0 {
                0o100755
            } else {
                0o100644
            }
        }
    }
}

fn path_components_owned(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|component| !component.is_empty())
        .map(str::to_string)
        .collect()
}

async fn write_index_snapshot(
    fs: &dyn VfsFileSystem,
    root: &str,
    snapshot: GitIndexSnapshot,
) -> Result<(), GitSubstrateError> {
    fs.write_file(
        &git_control_path(root, "index.json"),
        serde_json::to_vec(&snapshot)?,
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn clear_checkout_target(
    fs: &dyn VfsFileSystem,
    target: &str,
    preserve_git: bool,
    repository_id: &str,
    cancellation: &dyn GitCancellationToken,
) -> Result<usize, GitSubstrateError> {
    let target = normalize_path(target);
    let Some(stats) = fs.stat(&target).await? else {
        ensure_directory(fs, &target).await?;
        return Ok(0);
    };
    if stats.kind != FileKind::Directory {
        remove_existing_path(fs, &target).await?;
        ensure_directory(fs, &target).await?;
        return Ok(1);
    }
    let mut removed = 0;
    for entry in fs.readdir(&target).await? {
        if preserve_git && entry.name == ".git" {
            continue;
        }
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: repository_id.to_string(),
            });
        }
        removed += remove_path_recursive(fs, &join_virtual(&target, &entry.name)).await?;
    }
    Ok(removed)
}

async fn clear_checkout_pathspec(
    fs: &dyn VfsFileSystem,
    target_root: &str,
    preserve_git: bool,
    pathspec: &[String],
    repository_id: &str,
    cancellation: &dyn GitCancellationToken,
) -> Result<usize, GitSubstrateError> {
    let target_root = normalize_path(target_root);
    let Some(stats) = fs.stat(&target_root).await? else {
        return Ok(0);
    };
    if stats.kind != FileKind::Directory {
        return Ok(0);
    }
    let mut removed = 0;
    for entry in fs.readdir(&target_root).await? {
        if preserve_git && entry.name == ".git" {
            continue;
        }
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: repository_id.to_string(),
            });
        }
        let child_path = join_virtual(&target_root, &entry.name);
        let relative = entry.name;
        removed += Box::pin(clear_checkout_pathspec_recursive(
            fs,
            &child_path,
            &relative,
            pathspec,
            repository_id,
            cancellation,
        ))
        .await?;
    }
    Ok(removed)
}

async fn clear_checkout_pathspec_recursive(
    fs: &dyn VfsFileSystem,
    path: &str,
    relative: &str,
    pathspec: &[String],
    repository_id: &str,
    cancellation: &dyn GitCancellationToken,
) -> Result<usize, GitSubstrateError> {
    if cancellation.is_cancelled() {
        return Err(GitSubstrateError::Cancelled {
            repository_id: repository_id.to_string(),
        });
    }
    let Some(stats) = fs.lstat(path).await? else {
        return Ok(0);
    };
    if pathspec_matches(relative, pathspec) {
        return remove_path_recursive(fs, path).await;
    }
    if stats.kind == FileKind::Directory {
        let mut removed = 0;
        for entry in fs.readdir(path).await? {
            let child_path = join_virtual(path, &entry.name);
            let child_relative = format!("{relative}/{}", entry.name);
            removed += Box::pin(clear_checkout_pathspec_recursive(
                fs,
                &child_path,
                &child_relative,
                pathspec,
                repository_id,
                cancellation,
            ))
            .await?;
        }
        if fs.readdir(path).await?.is_empty() {
            fs.rmdir(path).await?;
            removed += 1;
        }
        return Ok(removed);
    }
    Ok(0)
}

async fn remove_path_recursive(
    fs: &dyn VfsFileSystem,
    path: &str,
) -> Result<usize, GitSubstrateError> {
    let Some(stats) = fs.lstat(path).await? else {
        return Ok(0);
    };
    match stats.kind {
        FileKind::File | FileKind::Symlink => {
            fs.unlink(path).await?;
            Ok(1)
        }
        FileKind::Directory => {
            let mut removed = 0;
            for entry in fs.readdir(path).await? {
                removed +=
                    Box::pin(remove_path_recursive(fs, &join_virtual(path, &entry.name))).await?;
            }
            fs.rmdir(path).await?;
            Ok(removed + 1)
        }
    }
}

async fn remove_existing_path(fs: &dyn VfsFileSystem, path: &str) -> Result<(), GitSubstrateError> {
    if fs.lstat(path).await?.is_some() {
        let _ = remove_path_recursive(fs, path).await?;
    }
    Ok(())
}

async fn ensure_directory(fs: &dyn VfsFileSystem, path: &str) -> Result<(), GitSubstrateError> {
    let path = normalize_path(path);
    match fs.stat(&path).await? {
        Some(stats) if stats.kind == FileKind::Directory => Ok(()),
        Some(_) => {
            remove_existing_path(fs, &path).await?;
            fs.mkdir(
                &path,
                MkdirOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await?;
            Ok(())
        }
        None => {
            fs.mkdir(
                &path,
                MkdirOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await?;
            Ok(())
        }
    }
}

fn mode_matches(expected_mode: u32, actual_kind: FileKind, actual_mode: u32) -> bool {
    match actual_kind {
        FileKind::Symlink => is_symlink_mode(expected_mode),
        FileKind::Directory => is_directory_mode(expected_mode),
        FileKind::File => {
            !is_symlink_mode(expected_mode)
                && !is_directory_mode(expected_mode)
                && ((expected_mode & 0o111) != 0) == ((actual_mode & 0o111) != 0)
        }
    }
}

fn is_symlink_mode(mode: u32) -> bool {
    (mode & 0o170000) == 0o120000
}

fn is_directory_mode(mode: u32) -> bool {
    (mode & 0o170000) == 0o040000
}

fn git_control_path(root: &str, name: &str) -> String {
    if name.starts_with(".git/") {
        join_virtual(root, name)
    } else {
        join_virtual(root, &format!(".git/{}", name.trim_start_matches('/')))
    }
}

fn normalize_ref_name(name: &str) -> String {
    if name == "HEAD" {
        "HEAD".to_string()
    } else {
        name.trim_start_matches(".git/")
            .trim_start_matches('/')
            .to_string()
    }
}

fn decode_reference_target(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes).trim().to_string();
    text.strip_prefix("ref: ")
        .map(str::to_string)
        .unwrap_or(text)
}

async fn ensure_parent_directory(
    fs: &dyn VfsFileSystem,
    path: &str,
) -> Result<(), GitSubstrateError> {
    if let Some((parent, _)) = path.rsplit_once('/')
        && !parent.is_empty()
    {
        fs.mkdir(
            parent,
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

fn ancestor_paths(start_path: &str, root: &str) -> Vec<String> {
    let mut current = normalize_path(start_path);
    let root = normalize_path(root);
    let mut result = Vec::new();
    if !path_is_within_root(&current, &root) {
        return result;
    }
    while path_is_within_root(&current, &root) {
        result.push(current.clone());
        if current == root {
            break;
        }
        current = current
            .rsplit_once('/')
            .map(|(prefix, _)| {
                if prefix.is_empty() {
                    "/".to_string()
                } else {
                    prefix.to_string()
                }
            })
            .unwrap_or_else(|| root.clone());
    }
    result
}

fn path_is_within_root(path: &str, root: &str) -> bool {
    let normalized_path = normalize_path(path);
    let normalized_root = normalize_path(root);
    if normalized_root == "/" {
        return normalized_path.starts_with('/');
    }
    normalized_path == normalized_root
        || normalized_path
            .strip_prefix(&normalized_root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn validate_image_descriptor(
    requested: &GitRepositoryImageDescriptor,
    actual: &GitRepositoryImageDescriptor,
) -> Result<(), GitSubstrateError> {
    let requested_root = normalize_path(&requested.root_path);
    let actual_root = normalize_path(&actual.root_path);
    if requested_root != actual_root {
        return Err(GitSubstrateError::RepositoryImageDescriptorMismatch {
            field: "root_path",
            expected: actual_root,
            found: requested_root,
        });
    }
    if requested.volume_id != actual.volume_id {
        return Err(GitSubstrateError::RepositoryImageDescriptorMismatch {
            field: "volume_id",
            expected: format!("{:?}", actual.volume_id),
            found: format!("{:?}", requested.volume_id),
        });
    }
    if requested.snapshot_sequence != actual.snapshot_sequence {
        return Err(GitSubstrateError::RepositoryImageDescriptorMismatch {
            field: "snapshot_sequence",
            expected: format!("{:?}", actual.snapshot_sequence),
            found: format!("{:?}", requested.snapshot_sequence),
        });
    }
    if requested.durable_snapshot != actual.durable_snapshot {
        return Err(GitSubstrateError::RepositoryImageDescriptorMismatch {
            field: "durable_snapshot",
            expected: actual.durable_snapshot.to_string(),
            found: requested.durable_snapshot.to_string(),
        });
    }
    Ok(())
}

fn join_virtual(base: &str, child: &str) -> String {
    if child.starts_with('/') {
        normalize_path(child)
    } else {
        normalize_path(&format!("{}/{}", base.trim_end_matches('/'), child))
    }
}

fn normalize_path(path: &str) -> String {
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

fn parent_path(path: &str) -> Option<String> {
    let path = normalize_path(path);
    if path == "/" {
        None
    } else {
        path.rsplit_once('/').map(|(parent, _)| {
            if parent.is_empty() {
                "/".to_string()
            } else {
                parent.to_string()
            }
        })
    }
}
