use std::sync::Arc;

use async_trait::async_trait;
use terracedb_vfs::{FileKind, ReadOnlyVfsFileSystem, SnapshotOptions, Volume, VolumeSnapshot};

use crate::{
    GitCheckoutReport, GitCheckoutRequest, GitDiscoverReport, GitDiscoverRequest, GitHeadState,
    GitIndexSnapshot, GitObject, GitObjectDatabase, GitObjectKind, GitRefDatabase, GitReference,
    GitRepositoryHandle, GitRepositoryImageDescriptor, GitStatusReport, GitSubstrateError,
    worktree::GitWorktreeMaterializer,
};

pub trait GitCancellationToken: Send + Sync {
    fn is_cancelled(&self) -> bool;
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

    async fn on_checkout(&self, _report: &GitCheckoutReport) -> Result<(), GitSubstrateError> {
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
}

#[derive(Clone)]
pub struct VfsGitRepositoryImage {
    root_path: Arc<str>,
    snapshot: Arc<dyn VolumeSnapshot>,
}

impl VfsGitRepositoryImage {
    pub fn new(snapshot: Arc<dyn VolumeSnapshot>, root_path: impl Into<Arc<str>>) -> Self {
        let root_path = root_path.into();
        Self {
            root_path: Arc::from(normalize_path(root_path.as_ref())),
            snapshot,
        }
    }

    pub async fn from_volume(
        volume: Arc<dyn Volume>,
        root_path: impl Into<Arc<str>>,
        snapshot: SnapshotOptions,
    ) -> Result<Self, GitSubstrateError> {
        Ok(Self::new(volume.snapshot(snapshot).await?, root_path))
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
    async fn status(&self) -> Result<GitStatusReport, GitSubstrateError>;
    async fn checkout(
        &self,
        request: GitCheckoutRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCheckoutReport, GitSubstrateError>;
}

#[derive(Clone)]
pub struct DeterministicGitRepositoryStore {
    name: Arc<str>,
    hooks: Arc<dyn GitExecutionHooks>,
    materializer: Arc<dyn GitWorktreeMaterializer>,
    fork_policy: crate::GitForkPolicy,
}

impl DeterministicGitRepositoryStore {
    pub fn new() -> Self {
        Self {
            name: Arc::from("deterministic-git"),
            hooks: Arc::new(NoopGitExecutionHooks),
            materializer: Arc::new(crate::worktree::DeterministicGitWorktreeMaterializer),
            fork_policy: crate::GitForkPolicy::simulation_native_baseline(),
        }
    }

    pub fn with_hooks(mut self, hooks: Arc<dyn GitExecutionHooks>) -> Self {
        self.hooks = hooks;
        self
    }

    pub fn with_materializer(mut self, materializer: Arc<dyn GitWorktreeMaterializer>) -> Self {
        self.materializer = materializer;
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
            image,
            handle,
            hooks: self.hooks.clone(),
            materializer: self.materializer.clone(),
        }))
    }
}

struct DeterministicGitRepository {
    image: Arc<dyn GitRepositoryImage>,
    handle: GitRepositoryHandle,
    hooks: Arc<dyn GitExecutionHooks>,
    materializer: Arc<dyn GitWorktreeMaterializer>,
}

#[async_trait]
impl GitObjectDatabase for DeterministicGitRepository {
    async fn read_object(&self, oid: &str) -> Result<Option<GitObject>, GitSubstrateError> {
        let path = join_virtual(&self.handle.root_path, &format!(".git/objects/{oid}"));
        let Some(bytes) = self.image.snapshot().fs().read_file(&path).await? else {
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
        let refs = collect_refs(self.image.snapshot().fs(), &refs_root, "refs").await?;
        self.hooks.on_read_refs(&refs).await?;
        Ok(refs)
    }
}

#[async_trait]
impl crate::GitIndexStore for DeterministicGitRepository {
    async fn index(&self) -> Result<GitIndexSnapshot, GitSubstrateError> {
        let path = join_virtual(&self.handle.root_path, ".git/index.json");
        let Some(bytes) = self.image.snapshot().fs().read_file(&path).await? else {
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
        let head = self
            .image
            .snapshot()
            .fs()
            .read_file(&head_path)
            .await?
            .ok_or_else(|| GitSubstrateError::RepositoryNotFound {
                path: self.handle.root_path.clone(),
            })?;
        let head_text = String::from_utf8_lossy(&head).trim().to_string();
        if let Some(reference) = head_text.strip_prefix("ref: ") {
            let reference = reference.to_string();
            let ref_path = join_virtual(&self.handle.root_path, &format!(".git/{reference}"));
            let oid = self
                .image
                .snapshot()
                .fs()
                .read_file(&ref_path)
                .await?
                .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string());
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

    async fn status(&self) -> Result<GitStatusReport, GitSubstrateError> {
        Ok(GitStatusReport::default())
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
        let report = self
            .materializer
            .materialize(&self.handle.repository_id, request, cancellation)
            .await?;
        self.hooks.on_checkout(&report).await?;
        Ok(report)
    }
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
                        output.push(GitReference {
                            name: child_relative,
                            target: String::from_utf8_lossy(&bytes).trim().to_string(),
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

fn parse_object_kind(kind: &[u8]) -> GitObjectKind {
    match kind {
        b"blob" => GitObjectKind::Blob,
        b"commit" => GitObjectKind::Commit,
        b"tree" => GitObjectKind::Tree,
        b"tag" => GitObjectKind::Tag,
        _ => GitObjectKind::Unknown,
    }
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
