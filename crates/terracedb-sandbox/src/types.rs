use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::{
    ExecutionDomainPath, ExecutionDomainPlacement, ExecutionResourceUsage, SequenceNumber,
    Timestamp,
};
use terracedb_capabilities::{BudgetPolicy, ExecutionDomain, ExecutionOperation, ExecutionPolicy};
use terracedb_vfs::VolumeId;

use crate::{CapabilityManifest, HoistMode, ReadonlyViewHandle};

pub const SANDBOX_SESSION_FORMAT_VERSION: u32 = 1;
pub const SANDBOX_EXECUTION_POLICY_STATE_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_WORKSPACE_ROOT: &str = "/workspace";
pub const TERRACE_METADATA_DIR: &str = "/.terrace";
pub const TERRACE_SESSION_METADATA_PATH: &str = "/.terrace/session.json";
pub const TERRACE_EXECUTION_POLICY_STATE_PATH: &str = "/.terrace/execution-policy-state.json";
pub const TERRACE_SESSION_INFO_KV_KEY: &str = "sandbox.session.info";
pub const TERRACE_RUNTIME_CACHE_DIR: &str = "/.terrace/cache/runtime";
pub const TERRACE_RUNTIME_MODULE_CACHE_PATH: &str = "/.terrace/cache/runtime/modules.json";
pub const TERRACE_NPM_DIR: &str = "/.terrace/npm";
pub const TERRACE_NPM_INSTALL_MANIFEST_PATH: &str = "/.terrace/npm/install-manifest.json";
pub const TERRACE_NPM_SESSION_CACHE_DIR: &str = "/.terrace/npm/cache/packages";
pub const TERRACE_NPM_COMPATIBILITY_ROOT: &str = "/.terrace/npm/node_modules";

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxExecutionBinding {
    pub domain: ExecutionDomain,
    pub backend: String,
    pub domain_path: ExecutionDomainPath,
    pub placement: ExecutionDomainPlacement,
    #[serde(default)]
    pub requested_usage: ExecutionResourceUsage,
    pub budget: Option<BudgetPolicy>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub placement_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxServiceBindings {
    pub runtime_backend: String,
    pub package_installer: String,
    pub git_workspace_manager: String,
    pub pull_request_provider: String,
    pub readonly_view_provider: String,
    #[serde(default)]
    pub typescript_service: String,
    #[serde(default)]
    pub bash_service: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub execution_bindings: BTreeMap<ExecutionOperation, SandboxExecutionBinding>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxSessionProvenance {
    pub base_snapshot: BaseSnapshotIdentity,
    pub hoisted_source: Option<HoistedSource>,
    pub git: Option<GitProvenance>,
    pub package_compat: PackageCompatibilityMode,
    pub capabilities: CapabilityManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_policy: Option<ExecutionPolicy>,
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
    pub execution_policy: Option<ExecutionPolicy>,
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
            package_compat: PackageCompatibilityMode::NpmPureJs,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: CapabilityManifest::default(),
            execution_policy: None,
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

    pub fn with_package_compat(mut self, package_compat: PackageCompatibilityMode) -> Self {
        self.package_compat = package_compat;
        self
    }

    pub fn with_conflict_policy(mut self, conflict_policy: ConflictPolicy) -> Self {
        self.conflict_policy = conflict_policy;
        self
    }

    pub fn with_workspace_root(mut self, workspace_root: impl Into<String>) -> Self {
        self.workspace_root = workspace_root.into();
        self
    }

    pub fn with_capabilities(mut self, capabilities: CapabilityManifest) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn with_execution_policy(mut self, execution_policy: ExecutionPolicy) -> Self {
        self.execution_policy = Some(execution_policy);
        self
    }

    pub fn with_hoisted_source(mut self, hoisted_source: HoistedSource) -> Self {
        self.hoisted_source = Some(hoisted_source);
        self
    }

    pub fn with_git_provenance(mut self, git_provenance: GitProvenance) -> Self {
        self.git_provenance = Some(git_provenance);
        self
    }
}
