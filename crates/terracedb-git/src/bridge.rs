use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::{
    GitCancellationToken, GitPullRequestReport, GitPullRequestRequest, GitPushReport,
    GitPushRequest, GitRepository, GitSubstrateError,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum GitImportMode {
    Head,
    WorkingTree {
        include_untracked: bool,
        include_ignored: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitImportEntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitImportEntry {
    pub path: String,
    pub kind: GitImportEntryKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symlink_target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitImportRequest {
    pub source_path: String,
    pub target_root: String,
    pub mode: GitImportMode,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitImportReport {
    pub source_path: String,
    pub target_root: String,
    pub repository_root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_url: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pathspec: Vec<String>,
    #[serde(default)]
    pub dirty: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<GitImportEntry>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitWorkspaceRequest {
    pub repo_root: String,
    pub branch_name: String,
    pub base_ref: String,
    pub target_path: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitWorkspaceReport {
    pub bridge: String,
    pub branch_name: String,
    pub workspace_path: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitFinalizeExportRequest {
    pub workspace_path: String,
    pub head_branch: String,
    pub title: String,
    pub body: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitFinalizeExportReport {
    pub workspace_path: String,
    pub head_branch: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait]
pub trait GitHostBridge: Send + Sync {
    fn name(&self) -> &str;
    fn supports_host_filesystem_bridge(&self) -> bool {
        false
    }

    async fn import_repository(
        &self,
        request: GitImportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitImportReport, GitSubstrateError>;

    async fn prepare_workspace(
        &self,
        _request: GitWorkspaceRequest,
        _cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitWorkspaceReport, GitSubstrateError> {
        Err(GitSubstrateError::Bridge {
            operation: "prepare_workspace",
            message: format!("{} does not support workspace preparation", self.name()),
        })
    }

    async fn finalize_export(
        &self,
        request: GitFinalizeExportRequest,
        _cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitFinalizeExportReport, GitSubstrateError> {
        Err(GitSubstrateError::Bridge {
            operation: "finalize_export",
            message: format!(
                "{} does not support export finalization for {}",
                self.name(),
                request.workspace_path
            ),
        })
    }

    async fn export_repository(
        &self,
        repository: Arc<dyn GitRepository>,
        request: crate::GitExportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<crate::GitExportReport, GitSubstrateError>;

    async fn push(
        &self,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError>;

    async fn create_pull_request(
        &self,
        request: GitPullRequestRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPullRequestReport, GitSubstrateError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicGitHostBridge {
    base_url: Arc<str>,
    pr_counter: Arc<Mutex<u64>>,
}

impl DeterministicGitHostBridge {
    pub fn new(base_url: impl Into<Arc<str>>) -> Self {
        Self {
            base_url: base_url.into(),
            pr_counter: Arc::new(Mutex::new(1)),
        }
    }
}

impl Default for DeterministicGitHostBridge {
    fn default() -> Self {
        Self::new("https://example.invalid")
    }
}

#[async_trait]
impl GitHostBridge for DeterministicGitHostBridge {
    fn name(&self) -> &str {
        self.base_url.as_ref()
    }

    async fn import_repository(
        &self,
        request: GitImportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitImportReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Bridge {
                operation: "import_repository",
                message: "cancelled".to_string(),
            });
        }
        Ok(GitImportReport {
            repository_root: request.source_path.clone(),
            source_path: request.source_path,
            target_root: request.target_root,
            head_commit: None,
            branch: None,
            remote_url: None,
            pathspec: vec![".".to_string()],
            dirty: false,
            entries: Vec::new(),
            metadata: request.metadata,
        })
    }

    async fn prepare_workspace(
        &self,
        request: GitWorkspaceRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitWorkspaceReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Bridge {
                operation: "prepare_workspace",
                message: "cancelled".to_string(),
            });
        }
        let mut metadata = request.metadata;
        metadata
            .entry("repo_root".to_string())
            .or_insert_with(|| JsonValue::from(request.repo_root.clone()));
        metadata
            .entry("base_ref".to_string())
            .or_insert_with(|| JsonValue::from(request.base_ref.clone()));
        Ok(GitWorkspaceReport {
            bridge: self.name().to_string(),
            branch_name: request.branch_name,
            workspace_path: request.target_path,
            metadata,
        })
    }

    async fn finalize_export(
        &self,
        request: GitFinalizeExportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitFinalizeExportReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Bridge {
                operation: "finalize_export",
                message: "cancelled".to_string(),
            });
        }
        let mut metadata = request.metadata;
        metadata.insert(
            "workspace_path".to_string(),
            JsonValue::from(request.workspace_path.clone()),
        );
        metadata.insert(
            "branch_name".to_string(),
            JsonValue::from(request.head_branch.clone()),
        );
        metadata.insert("committed".to_string(), JsonValue::from(false));
        metadata.insert("pushed".to_string(), JsonValue::from(false));
        Ok(GitFinalizeExportReport {
            workspace_path: request.workspace_path,
            head_branch: request.head_branch,
            metadata,
        })
    }

    async fn export_repository(
        &self,
        repository: Arc<dyn GitRepository>,
        request: crate::GitExportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<crate::GitExportReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Bridge {
                operation: "export_repository",
                message: "cancelled".to_string(),
            });
        }
        Ok(crate::GitExportReport {
            target_path: request.target_path,
            branch_name: request.branch_name,
            metadata: BTreeMap::from([(
                "repository_id".to_string(),
                JsonValue::from(repository.handle().repository_id),
            )]),
        })
    }

    async fn push(
        &self,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Bridge {
                operation: "push",
                message: "cancelled".to_string(),
            });
        }
        Ok(GitPushReport {
            remote: request.remote,
            branch_name: request.branch_name,
            pushed_oid: request.head_oid,
            metadata: request.metadata,
        })
    }

    async fn create_pull_request(
        &self,
        request: GitPullRequestRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPullRequestReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Bridge {
                operation: "create_pull_request",
                message: "cancelled".to_string(),
            });
        }
        let mut counter = self.pr_counter.lock().await;
        let number = *counter;
        *counter += 1;
        Ok(GitPullRequestReport {
            url: format!("{}/pull/{}", self.base_url, number),
            head_branch: request.head_branch,
            base_branch: request.base_branch,
            metadata: request.metadata,
        })
    }
}
