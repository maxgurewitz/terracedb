use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb_vfs::JsonValue;

use crate::{SandboxError, SandboxSession};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestRequest {
    pub title: String,
    pub body: String,
    pub head_branch: String,
    pub base_branch: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PullRequestReport {
    pub provider: String,
    pub id: String,
    pub url: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait]
pub trait PullRequestProviderClient: Send + Sync {
    fn name(&self) -> &str;
    async fn create_pull_request(
        &self,
        session: &SandboxSession,
        request: PullRequestRequest,
    ) -> Result<PullRequestReport, SandboxError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicPullRequestProviderClient {
    name: String,
}

impl DeterministicPullRequestProviderClient {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for DeterministicPullRequestProviderClient {
    fn default() -> Self {
        Self::new("deterministic-pr")
    }
}

#[async_trait]
impl PullRequestProviderClient for DeterministicPullRequestProviderClient {
    fn name(&self) -> &str {
        &self.name
    }

    async fn create_pull_request(
        &self,
        session: &SandboxSession,
        request: PullRequestRequest,
    ) -> Result<PullRequestReport, SandboxError> {
        let id = format!(
            "{}-{}",
            session.info().await.session_volume_id,
            request.head_branch.replace('/', "-")
        );
        Ok(PullRequestReport {
            provider: self.name.clone(),
            url: format!("https://example.invalid/pr/{id}"),
            id,
            metadata: BTreeMap::from([
                (
                    "head_branch".to_string(),
                    JsonValue::from(request.head_branch.clone()),
                ),
                (
                    "base_branch".to_string(),
                    JsonValue::from(request.base_branch.clone()),
                ),
            ]),
        })
    }
}
