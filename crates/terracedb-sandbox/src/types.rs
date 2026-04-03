use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::{
    ExecutionDomainPath, ExecutionDomainPlacement, ExecutionResourceUsage, SequenceNumber,
    Timestamp,
};
use terracedb_capabilities::{BudgetPolicy, ExecutionDomain, ExecutionOperation, ExecutionPolicy};
use terracedb_git::{GitImportSource, GitObjectFormat, GitRepositoryOrigin};
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
    #[serde(default)]
    pub origin: GitRepositoryOrigin,
    pub head_commit: Option<String>,
    pub branch: Option<String>,
    pub remote_url: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remote_bridge_metadata: BTreeMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_format: Option<GitObjectFormat>,
    pub pathspec: Vec<String>,
    pub dirty: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoistedSource {
    pub source_path: String,
    pub mode: HoistMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRemoteImportRequest {
    pub remote_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
    pub target_root: String,
    #[serde(default)]
    pub delete_missing: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRemoteImportReport {
    pub remote_url: String,
    pub target_root: String,
    pub imported_paths: usize,
    pub deleted_paths: usize,
    pub git_provenance: GitProvenance,
}

impl GitProvenance {
    pub(crate) fn sanitized_for_durability(&self) -> Self {
        let mut sanitized = self.clone();
        sanitized.remote_url = sanitized.remote_url.as_deref().map(sanitize_remote_url);
        sanitized.remote_bridge_metadata =
            durable_remote_bridge_metadata(&sanitized.remote_bridge_metadata);
        sanitized
    }
}

pub(crate) fn sanitize_git_import_source(source: &GitImportSource) -> GitImportSource {
    match source {
        GitImportSource::HostPath { path } => GitImportSource::HostPath { path: path.clone() },
        GitImportSource::RemoteRepository {
            remote_url,
            reference,
        } => GitImportSource::RemoteRepository {
            remote_url: sanitize_remote_url(remote_url),
            reference: reference.clone(),
        },
    }
}

pub(crate) fn sanitize_remote_url(remote_url: &str) -> String {
    let Some(authority_start) = remote_url.find("://").map(|index| index + 3) else {
        return remote_url.to_string();
    };
    let authority_end = remote_url[authority_start..]
        .find(['/', '?', '#'])
        .map(|index| authority_start + index)
        .unwrap_or(remote_url.len());
    let authority = &remote_url[authority_start..authority_end];
    let Some(userinfo_end) = authority.rfind('@') else {
        return remote_url.to_string();
    };
    format!(
        "{}{}{}",
        &remote_url[..authority_start],
        &authority[userinfo_end + 1..],
        &remote_url[authority_end..]
    )
}

pub(crate) fn durable_remote_bridge_metadata(
    metadata: &BTreeMap<String, JsonValue>,
) -> BTreeMap<String, JsonValue> {
    metadata
        .iter()
        .filter(|(key, _)| !is_sensitive_remote_bridge_metadata_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

pub(crate) fn sensitive_remote_bridge_metadata_keys(
    metadata: &BTreeMap<String, JsonValue>,
) -> Vec<String> {
    metadata
        .keys()
        .filter(|key| is_sensitive_remote_bridge_metadata_key(key))
        .cloned()
        .collect()
}

pub(crate) fn has_sensitive_remote_bridge_metadata(metadata: &BTreeMap<String, JsonValue>) -> bool {
    metadata
        .keys()
        .any(|key| is_sensitive_remote_bridge_metadata_key(key))
}

fn is_sensitive_remote_bridge_metadata_key(key: &str) -> bool {
    let normalized = key.trim().to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "authorization"
            | "auth_token"
            | "access_token"
            | "refresh_token"
            | "id_token"
            | "bearer_token"
            | "password"
            | "secret"
            | "client_secret"
            | "api_key"
            | "private_key"
    ) || normalized.ends_with("_token")
        || normalized.ends_with("_secret")
        || normalized.ends_with("_password")
        || normalized.ends_with("_api_key")
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
    #[serde(alias = "git_workspace_manager")]
    pub git_repository_store: String,
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
