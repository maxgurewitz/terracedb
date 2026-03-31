use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb_vfs::JsonValue;

use crate::{SandboxError, SandboxSession};

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
        Ok(GitWorkspaceReport {
            manager: self.name.clone(),
            branch_name: request.branch_name.clone(),
            workspace_path: request.target_path.clone(),
            metadata: BTreeMap::from([
                (
                    "session_volume_id".to_string(),
                    JsonValue::from(session.info().await.session_volume_id.to_string()),
                ),
                (
                    "base_branch".to_string(),
                    request
                        .base_branch
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
            ]),
        })
    }
}
