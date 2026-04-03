use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb_vfs::VolumeId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitAmbientDefault {
    CurrentWorkingDirectory,
    HostPathOnlyOpen,
    Tempfiles,
    Lockfiles,
    ThreadSpawn,
    InterruptState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitForkSurface {
    GixOpen,
    GixObject,
    GixRefs,
    GixIndex,
    GixWorktree,
    HostBridge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitOwnershipMode {
    Consumed,
    Wrapped,
    Forked,
    Forbidden,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitForkSurfacePolicy {
    pub surface: GitForkSurface,
    pub mode: GitOwnershipMode,
    pub notes: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitForkPolicy {
    pub surfaces: Vec<GitForkSurfacePolicy>,
    pub forbidden_ambient_defaults: Vec<GitAmbientDefault>,
}

impl Default for GitForkPolicy {
    fn default() -> Self {
        Self::simulation_native_baseline()
    }
}

impl GitForkPolicy {
    pub fn simulation_native_baseline() -> Self {
        Self {
            surfaces: vec![
                GitForkSurfacePolicy {
                    surface: GitForkSurface::GixOpen,
                    mode: GitOwnershipMode::Wrapped,
                    notes: "repository open and discovery stay behind terracedb-git root policies"
                        .to_string(),
                },
                GitForkSurfacePolicy {
                    surface: GitForkSurface::GixObject,
                    mode: GitOwnershipMode::Forked,
                    notes: "object access is re-hosted over explicit repository images".to_string(),
                },
                GitForkSurfacePolicy {
                    surface: GitForkSurface::GixRefs,
                    mode: GitOwnershipMode::Forked,
                    notes: "ref enumeration and updates are owned by deterministic stores"
                        .to_string(),
                },
                GitForkSurfacePolicy {
                    surface: GitForkSurface::GixIndex,
                    mode: GitOwnershipMode::Forked,
                    notes: "index state is mediated through explicit VFS-backed seams".to_string(),
                },
                GitForkSurfacePolicy {
                    surface: GitForkSurface::GixWorktree,
                    mode: GitOwnershipMode::Forked,
                    notes: "worktree materialization is isolated behind a dedicated seam"
                        .to_string(),
                },
                GitForkSurfacePolicy {
                    surface: GitForkSurface::HostBridge,
                    mode: GitOwnershipMode::Wrapped,
                    notes: "host import/export/push stays behind a narrow bridge".to_string(),
                },
            ],
            forbidden_ambient_defaults: vec![
                GitAmbientDefault::CurrentWorkingDirectory,
                GitAmbientDefault::HostPathOnlyOpen,
                GitAmbientDefault::Tempfiles,
                GitAmbientDefault::Lockfiles,
                GitAmbientDefault::ThreadSpawn,
                GitAmbientDefault::InterruptState,
            ],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRepositoryPolicy {
    pub execution_domain: Option<String>,
    pub allow_host_bridge: bool,
    pub allow_worktree_materialization: bool,
    pub forbidden_ambient_defaults: Vec<GitAmbientDefault>,
}

impl Default for GitRepositoryPolicy {
    fn default() -> Self {
        Self {
            execution_domain: None,
            allow_host_bridge: false,
            allow_worktree_materialization: true,
            forbidden_ambient_defaults: GitForkPolicy::simulation_native_baseline()
                .forbidden_ambient_defaults,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRepositoryImageDescriptor {
    pub root_path: String,
    pub volume_id: Option<VolumeId>,
    pub snapshot_sequence: Option<u64>,
    pub durable_snapshot: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitObjectFormat {
    Sha1,
    Sha256,
}

impl GitObjectFormat {
    pub fn from_oid(oid: &str) -> Option<Self> {
        if oid.len() == 40 && oid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            Some(Self::Sha1)
        } else if oid.len() == 64 && oid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            Some(Self::Sha256)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitRepositoryOrigin {
    #[default]
    Native,
    HostImport,
    RemoteImport,
}

impl GitRepositoryOrigin {
    pub fn uses_external_bridge(self) -> bool {
        !matches!(self, Self::Native)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRepositoryProvenance {
    pub backend: String,
    pub repo_root: String,
    pub origin: GitRepositoryOrigin,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_url: Option<String>,
    pub object_format: GitObjectFormat,
    pub volume_id: Option<VolumeId>,
    pub snapshot_sequence: Option<u64>,
    pub durable_snapshot: bool,
    pub fork_policy: GitForkPolicy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitOpenRequest {
    pub repository_id: String,
    pub repository_image: GitRepositoryImageDescriptor,
    pub policy: GitRepositoryPolicy,
    pub provenance: GitRepositoryProvenance,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitDiscoverRequest {
    pub start_path: String,
    pub policy: GitRepositoryPolicy,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitDiscoverReport {
    pub repository_root: String,
    pub head_ref: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitRepositoryHandle {
    pub repository_id: String,
    pub store: String,
    pub root_path: String,
    pub policy: GitRepositoryPolicy,
    pub provenance: GitRepositoryProvenance,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitReference {
    pub name: String,
    pub target: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHeadState {
    pub symbolic_ref: Option<String>,
    pub oid: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitObjectKind {
    Blob,
    Commit,
    Tree,
    Tag,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitObject {
    pub oid: String,
    pub kind: GitObjectKind,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitIndexEntry {
    pub path: String,
    pub oid: Option<String>,
    pub mode: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GitIndexSnapshot {
    pub entries: Vec<GitIndexEntry>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitStatusKind {
    Clean,
    Modified,
    Added,
    Deleted,
    Untracked,
    Ignored,
    Renamed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitStatusEntry {
    pub path: String,
    pub kind: GitStatusKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitStatusOptions {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pathspec: Vec<String>,
    #[serde(default = "git_status_include_untracked_default")]
    pub include_untracked: bool,
    #[serde(default)]
    pub include_ignored: bool,
}

impl Default for GitStatusOptions {
    fn default() -> Self {
        Self {
            pathspec: Vec::new(),
            include_untracked: git_status_include_untracked_default(),
            include_ignored: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitStatusReport {
    pub dirty: bool,
    pub entries: Vec<GitStatusEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitDiffKind {
    Modified,
    Added,
    Deleted,
    Untracked,
    Ignored,
    Renamed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitDiffEntry {
    pub path: String,
    pub kind: GitDiffKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_oid: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitDiffRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pathspec: Vec<String>,
    #[serde(default = "git_status_include_untracked_default")]
    pub include_untracked: bool,
    #[serde(default)]
    pub include_ignored: bool,
}

impl Default for GitDiffRequest {
    fn default() -> Self {
        Self {
            pathspec: Vec::new(),
            include_untracked: git_status_include_untracked_default(),
            include_ignored: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitDiffReport {
    pub dirty: bool,
    pub entries: Vec<GitDiffEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitCheckoutRequest {
    pub target_ref: String,
    pub materialize_path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pathspec: Vec<String>,
    #[serde(default)]
    pub update_head: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitCheckoutReport {
    pub target_ref: String,
    pub materialized_path: String,
    #[serde(default)]
    pub written_paths: usize,
    #[serde(default)]
    pub deleted_paths: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_oid: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRefUpdate {
    pub name: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_target: Option<String>,
    #[serde(default)]
    pub symbolic: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRefUpdateReport {
    pub reference: GitReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitCommitRequest {
    pub target_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    pub title: String,
    pub body: String,
    #[serde(default)]
    pub require_changes: bool,
    #[serde(default)]
    pub update_head: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitCommitReport {
    pub target_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_target: Option<String>,
    pub commit_oid: String,
    pub tree_oid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_oid: Option<String>,
    #[serde(default)]
    pub committed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitExportRequest {
    pub target_path: String,
    pub branch_name: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitExportReport {
    pub target_path: String,
    pub branch_name: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPushRequest {
    pub remote: String,
    pub branch_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_oid: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPushReport {
    pub remote: String,
    pub branch_name: String,
    pub pushed_oid: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPullRequestRequest {
    pub title: String,
    pub body: String,
    pub head_branch: String,
    pub base_branch: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPullRequestReport {
    pub url: String,
    pub head_branch: String,
    pub base_branch: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitTracePhase {
    Discover,
    Open,
    ReadHead,
    ReadRefs,
    ReadObject,
    Status,
    Diff,
    Checkout,
    UpdateRef,
    Bridge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitTraceEvent {
    pub phase: GitTracePhase,
    pub label: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

const fn git_status_include_untracked_default() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        GitDiscoverRequest, GitForkPolicy, GitObjectFormat, GitOpenRequest,
        GitRepositoryImageDescriptor, GitRepositoryOrigin, GitRepositoryPolicy,
        GitRepositoryProvenance, GitTraceEvent, GitTracePhase,
    };

    #[test]
    fn git_request_and_trace_metadata_round_trip() {
        let request = GitOpenRequest {
            repository_id: "repo-1".to_string(),
            repository_image: GitRepositoryImageDescriptor {
                root_path: "/repo".to_string(),
                volume_id: Some(terracedb_vfs::VolumeId::new(0x8000)),
                snapshot_sequence: Some(9),
                durable_snapshot: false,
            },
            policy: GitRepositoryPolicy::default(),
            provenance: GitRepositoryProvenance {
                backend: "deterministic-git".to_string(),
                repo_root: "/repo".to_string(),
                origin: GitRepositoryOrigin::Native,
                remote_url: Some("https://github.com/example/repo".to_string()),
                object_format: GitObjectFormat::Sha256,
                volume_id: Some(terracedb_vfs::VolumeId::new(0x8000)),
                snapshot_sequence: Some(9),
                durable_snapshot: false,
                fork_policy: GitForkPolicy::simulation_native_baseline(),
            },
            metadata: BTreeMap::from([("kind".to_string(), json!("compile-only"))]),
        };
        let encoded = serde_json::to_vec(&request).expect("encode git request");
        let decoded: GitOpenRequest = serde_json::from_slice(&encoded).expect("decode git request");
        assert_eq!(decoded, request);

        let discover = GitDiscoverRequest {
            start_path: "/repo/src".to_string(),
            policy: GitRepositoryPolicy::default(),
            metadata: BTreeMap::new(),
        };
        let encoded = serde_json::to_string(&discover).expect("encode discover");
        let decoded: GitDiscoverRequest = serde_json::from_str(&encoded).expect("decode discover");
        assert_eq!(decoded, discover);

        let trace = GitTraceEvent {
            phase: GitTracePhase::Bridge,
            label: "push".to_string(),
            metadata: BTreeMap::from([("remote".to_string(), json!("origin"))]),
        };
        let encoded = serde_json::to_vec(&trace).expect("encode trace");
        let decoded: GitTraceEvent = serde_json::from_slice(&encoded).expect("decode trace");
        assert_eq!(decoded, trace);
    }
}
