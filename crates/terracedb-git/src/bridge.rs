use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::{
    GitCancellationToken, GitPullRequestReport, GitPullRequestRequest, GitPushReport,
    GitPushRequest, GitRepository, GitSubstrateError,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitImportRequest {
    pub source_path: String,
    pub target_root: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitImportReport {
    pub source_path: String,
    pub target_root: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait]
pub trait GitHostBridge: Send + Sync {
    fn name(&self) -> &str;
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
            source_path: request.source_path,
            target_root: request.target_root,
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
        repository: Arc<dyn GitRepository>,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Bridge {
                operation: "push",
                message: "cancelled".to_string(),
            });
        }
        let head = repository.head().await?;
        Ok(GitPushReport {
            remote: request.remote,
            branch_name: request.branch_name,
            pushed_oid: head.oid,
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
