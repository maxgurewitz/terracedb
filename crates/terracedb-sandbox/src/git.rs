use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb_git::{
    GitFinalizeExportReport, GitFinalizeExportRequest, GitHostBridge,
    GitWorkspaceRequest as BridgeGitWorkspaceRequest, HostGitBridge as DefaultHostGitBridge,
    NeverCancel,
};
use terracedb_vfs::JsonValue;

use crate::{SandboxError, SandboxSession};

static EXPORT_WORKSPACE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitWorkspaceRequest {
    pub branch_name: String,
    pub base_branch: Option<String>,
    pub target_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitWorkspaceReport {
    pub manager: String,
    pub branch_name: String,
    pub workspace_path: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait]
pub trait GitWorkspaceManager: Send + Sync {
    fn name(&self) -> &str;
    fn host_bridge(&self) -> Option<Arc<dyn GitHostBridge>> {
        None
    }
    async fn prepare_workspace(
        &self,
        session: &SandboxSession,
        request: GitWorkspaceRequest,
    ) -> Result<GitWorkspaceReport, SandboxError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicGitWorkspaceManager {
    name: String,
}

impl DeterministicGitWorkspaceManager {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for DeterministicGitWorkspaceManager {
    fn default() -> Self {
        Self::new("deterministic-git")
    }
}

#[async_trait]
impl GitWorkspaceManager for DeterministicGitWorkspaceManager {
    fn name(&self) -> &str {
        &self.name
    }

    async fn prepare_workspace(
        &self,
        session: &SandboxSession,
        request: GitWorkspaceRequest,
    ) -> Result<GitWorkspaceReport, SandboxError> {
        let info = session.info().await;
        let provenance = info.provenance.git.clone();
        let base_ref = provenance
            .as_ref()
            .and_then(|git| git.head_commit.clone())
            .or_else(|| request.base_branch.clone())
            .or_else(|| provenance.as_ref().and_then(|git| git.branch.clone()))
            .unwrap_or_else(|| "HEAD".to_string());
        Ok(GitWorkspaceReport {
            manager: self.name.clone(),
            branch_name: request.branch_name.clone(),
            workspace_path: request.target_path.clone(),
            metadata: BTreeMap::from([
                (
                    "session_volume_id".to_string(),
                    JsonValue::from(info.session_volume_id.to_string()),
                ),
                (
                    "base_branch".to_string(),
                    request
                        .base_branch
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
                ("base_ref".to_string(), JsonValue::from(base_ref)),
                (
                    "repo_root".to_string(),
                    provenance
                        .as_ref()
                        .map(|git| JsonValue::from(git.repo_root.clone()))
                        .unwrap_or(JsonValue::Null),
                ),
                (
                    "head_commit".to_string(),
                    provenance
                        .as_ref()
                        .and_then(|git| git.head_commit.clone())
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
                (
                    "remote_url".to_string(),
                    provenance
                        .and_then(|git| git.remote_url)
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
            ]),
        })
    }
}

#[derive(Clone)]
pub struct HostGitWorkspaceManager {
    name: String,
    bridge: Arc<dyn GitHostBridge>,
}

impl std::fmt::Debug for HostGitWorkspaceManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostGitWorkspaceManager")
            .field("name", &self.name)
            .field("bridge", &self.bridge.name())
            .finish()
    }
}

impl HostGitWorkspaceManager {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bridge: Arc::new(DefaultHostGitBridge::default()),
        }
    }

    pub fn with_bridge(mut self, bridge: Arc<dyn GitHostBridge>) -> Self {
        self.bridge = bridge;
        self
    }
}

impl Default for HostGitWorkspaceManager {
    fn default() -> Self {
        Self::new("host-git")
    }
}

#[async_trait]
impl GitWorkspaceManager for HostGitWorkspaceManager {
    fn name(&self) -> &str {
        &self.name
    }

    fn host_bridge(&self) -> Option<Arc<dyn GitHostBridge>> {
        Some(self.bridge.clone())
    }

    async fn prepare_workspace(
        &self,
        session: &SandboxSession,
        request: GitWorkspaceRequest,
    ) -> Result<GitWorkspaceReport, SandboxError> {
        let info = session.info().await;
        let provenance = info
            .provenance
            .git
            .ok_or(SandboxError::MissingGitProvenance)?;
        let base_ref = provenance
            .head_commit
            .clone()
            .or(request.base_branch.clone())
            .or(provenance.branch.clone())
            .unwrap_or_else(|| "HEAD".to_string());
        let report = self
            .bridge
            .prepare_workspace(
                BridgeGitWorkspaceRequest {
                    repo_root: provenance.repo_root.clone(),
                    branch_name: request.branch_name.clone(),
                    base_ref,
                    target_path: request.target_path.clone(),
                    metadata: BTreeMap::from([
                        (
                            "session_volume_id".to_string(),
                            JsonValue::from(info.session_volume_id.to_string()),
                        ),
                        (
                            "base_branch".to_string(),
                            request
                                .base_branch
                                .map(JsonValue::from)
                                .unwrap_or(JsonValue::Null),
                        ),
                    ]),
                },
                Arc::new(NeverCancel),
            )
            .await
            .map_err(git_substrate_error_to_sandbox)?;
        Ok(GitWorkspaceReport {
            manager: self.name.clone(),
            branch_name: report.branch_name,
            workspace_path: report.workspace_path,
            metadata: report.metadata,
        })
    }
}

pub(crate) fn default_export_workspace_path(session_id: &str, branch_name: &str) -> PathBuf {
    let branch = sanitize_label(branch_name);
    loop {
        let unique_suffix = EXPORT_WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "terracedb-sandbox-{session_id}-{branch}-{}-{unique_suffix}",
            std::process::id()
        ));
        if !path.exists() {
            return path;
        }
    }
}

pub(crate) fn finalize_export_report_metadata(
    report: GitFinalizeExportReport,
) -> BTreeMap<String, JsonValue> {
    report.metadata
}

pub(crate) fn finalize_export_request(
    workspace_path: String,
    title: String,
    body: String,
    head_branch: String,
    metadata: BTreeMap<String, JsonValue>,
) -> GitFinalizeExportRequest {
    GitFinalizeExportRequest {
        workspace_path,
        head_branch,
        title,
        body,
        metadata,
    }
}

pub(crate) fn git_substrate_error_to_sandbox(
    error: terracedb_git::GitSubstrateError,
) -> SandboxError {
    match error {
        terracedb_git::GitSubstrateError::ExportConflict { path, message } => {
            SandboxError::Io { path, message }
        }
        terracedb_git::GitSubstrateError::RemotePushFailed {
            remote,
            branch,
            message,
        } => SandboxError::Service {
            service: "git",
            message: format!("remote push failed for {remote}/{branch}: {message}"),
        },
        terracedb_git::GitSubstrateError::PullRequestProvider { provider, message } => {
            SandboxError::Service {
                service: "git",
                message: format!("pull-request provider {provider} failed: {message}"),
            }
        }
        terracedb_git::GitSubstrateError::Bridge { operation, message } => SandboxError::Service {
            service: "git",
            message: format!("{operation}: {message}"),
        },
        terracedb_git::GitSubstrateError::Cancelled { repository_id } => SandboxError::Service {
            service: "git",
            message: format!("cancelled: {repository_id}"),
        },
        terracedb_git::GitSubstrateError::RepositoryNotFound { path } => SandboxError::Io {
            path,
            message: "git repository was not found".to_string(),
        },
        terracedb_git::GitSubstrateError::RepositoryReadOnly { repository_id } => {
            SandboxError::Service {
                service: "git",
                message: format!("repository is read-only: {repository_id}"),
            }
        }
        terracedb_git::GitSubstrateError::RepositoryImageDescriptorMismatch {
            field,
            expected,
            found,
        } => SandboxError::Service {
            service: "git",
            message: format!(
                "repository image descriptor mismatch for {field}: expected {expected}, found {found}"
            ),
        },
        terracedb_git::GitSubstrateError::ReferenceNotFound { reference } => {
            SandboxError::Service {
                service: "git",
                message: format!("reference not found: {reference}"),
            }
        }
        terracedb_git::GitSubstrateError::ReferenceConflict {
            reference,
            expected,
            found,
        } => SandboxError::Service {
            service: "git",
            message: format!(
                "reference conflict for {reference}: expected {expected:?}, found {found:?}"
            ),
        },
        terracedb_git::GitSubstrateError::ObjectNotFound { oid } => SandboxError::Service {
            service: "git",
            message: format!("object not found: {oid}"),
        },
        terracedb_git::GitSubstrateError::InvalidObject { oid, message } => SandboxError::Service {
            service: "git",
            message: format!("invalid object {oid}: {message}"),
        },
        terracedb_git::GitSubstrateError::SerdeJson(error) => SandboxError::SerdeJson(error),
        terracedb_git::GitSubstrateError::Vfs(error) => SandboxError::Vfs(error),
    }
}

fn sanitize_label(value: &str) -> String {
    let mut sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    while sanitized.contains("--") {
        sanitized = sanitized.replace("--", "-");
    }
    sanitized.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::default_export_workspace_path;

    #[test]
    fn default_export_workspace_path_allocates_unique_paths() {
        let first = default_export_workspace_path("session", "feature/branch");
        let second = default_export_workspace_path("session", "feature/branch");

        assert_ne!(first, second);
        assert!(first.starts_with(std::env::temp_dir()));
        assert!(second.starts_with(std::env::temp_dir()));
        assert!(first.to_string_lossy().contains("feature-branch"));
        assert!(second.to_string_lossy().contains("feature-branch"));
    }
}
