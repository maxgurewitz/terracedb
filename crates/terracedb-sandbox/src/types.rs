use serde::{Deserialize, Serialize};
use terracedb::{SequenceNumber, Timestamp};
use terracedb_vfs::VolumeId;

use crate::{CapabilityManifest, HoistMode, ReadonlyViewHandle};

pub const SANDBOX_SESSION_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_WORKSPACE_ROOT: &str = "/workspace";
pub const TERRACE_METADATA_DIR: &str = "/.terrace";
pub const TERRACE_SESSION_METADATA_PATH: &str = "/.terrace/session.json";
pub const TERRACE_SESSION_INFO_KV_KEY: &str = "sandbox.session.info";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseSnapshotIdentity {
    pub volume_id: VolumeId,
    pub sequence: SequenceNumber,
    pub durable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitProvenance {
    pub repo_root: String,
    pub head_commit: Option<String>,
    pub branch: Option<String>,
    pub remote_url: Option<String>,
    pub pathspec: Vec<String>,
    pub dirty: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoistedSource {
    pub source_path: String,
    pub mode: HoistMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageCompatibilityMode {
    TerraceOnly,
    NpmPureJs,
    NpmWithNodeBuiltins,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    Fail,
    CreatePatchBundle,
    ApplyNonConflicting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxSessionState {
    Open,
    Closed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxServiceBindings {
    pub runtime_backend: String,
    pub package_installer: String,
    pub git_workspace_manager: String,
    pub pull_request_provider: String,
    pub readonly_view_provider: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxSessionProvenance {
    pub base_snapshot: BaseSnapshotIdentity,
    pub hoisted_source: Option<HoistedSource>,
    pub git: Option<GitProvenance>,
    pub package_compat: PackageCompatibilityMode,
    pub capabilities: CapabilityManifest,
    pub active_view_handles: Vec<ReadonlyViewHandle>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxSessionInfo {
    pub format_version: u32,
    pub revision: u64,
    pub session_volume_id: VolumeId,
    pub workspace_root: String,
    pub state: SandboxSessionState,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub closed_at: Option<Timestamp>,
    pub conflict_policy: ConflictPolicy,
    pub services: SandboxServiceBindings,
    pub provenance: SandboxSessionProvenance,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxConfig {
    pub session_volume_id: VolumeId,
    pub session_chunk_size: Option<u32>,
    pub base_volume_id: VolumeId,
    pub durable_base: bool,
    pub workspace_root: String,
    pub package_compat: PackageCompatibilityMode,
    pub conflict_policy: ConflictPolicy,
    pub capabilities: CapabilityManifest,
    pub hoisted_source: Option<HoistedSource>,
    pub git_provenance: Option<GitProvenance>,
}

impl SandboxConfig {
    pub fn new(base_volume_id: VolumeId, session_volume_id: VolumeId) -> Self {
        Self {
            session_volume_id,
            session_chunk_size: None,
            base_volume_id,
            durable_base: false,
            workspace_root: DEFAULT_WORKSPACE_ROOT.to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: CapabilityManifest::default(),
            hoisted_source: None,
            git_provenance: None,
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: u32) -> Self {
        self.session_chunk_size = Some(chunk_size);
        self
    }

    pub fn with_durable_base(mut self, durable_base: bool) -> Self {
        self.durable_base = durable_base;
        self
    }
}
