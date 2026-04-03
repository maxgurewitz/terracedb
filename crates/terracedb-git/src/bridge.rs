use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::{
    GitCancellationToken, GitObjectFormat, GitPullRequestReport, GitPullRequestRequest,
    GitPushReport, GitPushRequest, GitRepository, GitRepositoryOrigin, GitSubstrateError,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitImportLayout {
    #[default]
    RepositoryImage,
    TreeOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum GitImportSource {
    HostPath {
        path: String,
    },
    RemoteRepository {
        remote_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reference: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitImportEntry {
    pub path: String,
    pub kind: GitImportEntryKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symlink_target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitImportRequest {
    pub source: GitImportSource,
    pub target_root: String,
    pub mode: GitImportMode,
    #[serde(default)]
    pub layout: GitImportLayout,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pathspec: Vec<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitImportReport {
    pub source: GitImportSource,
    pub target_root: String,
    pub repository_root: String,
    pub origin: GitRepositoryOrigin,
    pub object_format: GitObjectFormat,
    #[serde(default)]
    pub layout: GitImportLayout,
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

#[async_trait]
pub trait GitHostBridge: Send + Sync {
    fn name(&self) -> &str;
    fn supports_host_filesystem_bridge(&self) -> bool {
        false
    }
    fn supports_remote_repository_bridge(&self) -> bool {
        false
    }

    async fn import_repository(
        &self,
        request: GitImportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitImportReport, GitSubstrateError>;

    async fn export_repository(
        &self,
        repository: Arc<dyn GitRepository>,
        request: crate::GitExportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<crate::GitExportReport, GitSubstrateError>;

    async fn push(
        &self,
        repository: Arc<dyn GitRepository>,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError>;

    async fn create_pull_request(
        &self,
        request: GitPullRequestRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPullRequestReport, GitSubstrateError>;
}

#[async_trait]
/// Boundary adapter for provider-owned remote import, transport, and pull-request APIs.
///
/// `terracedb-git` owns repository semantics over VFS-native repository images. Implementations of
/// this trait own only the external boundary crossings needed to source a repository image from a
/// remote service, publish commits to that service, and create a pull request through the
/// provider's API.
pub trait GitRemoteProvider: Send + Sync {
    fn name(&self) -> &str;

    fn supports_remote(&self, remote_url: &str, metadata: &BTreeMap<String, JsonValue>) -> bool;

    async fn import_repository(
        &self,
        request: GitImportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitImportReport, GitSubstrateError>;

    async fn push(
        &self,
        repository: Arc<dyn GitRepository>,
        remote_url: String,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError>;

    async fn create_pull_request(
        &self,
        remote_url: String,
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

    fn supports_remote_repository_bridge(&self) -> bool {
        true
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
            repository_root: match &request.source {
                GitImportSource::HostPath { path } => path.clone(),
                GitImportSource::RemoteRepository { remote_url, .. } => remote_url.clone(),
            },
            source: request.source.clone(),
            target_root: request.target_root,
            origin: match request.source {
                GitImportSource::HostPath { .. } => GitRepositoryOrigin::HostImport,
                GitImportSource::RemoteRepository { .. } => GitRepositoryOrigin::RemoteImport,
            },
            object_format: GitObjectFormat::Sha1,
            layout: request.layout.clone(),
            head_commit: None,
            branch: None,
            remote_url: match request.source {
                GitImportSource::RemoteRepository { remote_url, .. } => Some(remote_url),
                GitImportSource::HostPath { .. } => None,
            },
            pathspec: if request.pathspec.is_empty() {
                vec![".".to_string()]
            } else {
                request.pathspec.clone()
            },
            dirty: false,
            entries: Vec::new(),
            metadata: request.metadata,
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
        _repository: Arc<dyn GitRepository>,
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
