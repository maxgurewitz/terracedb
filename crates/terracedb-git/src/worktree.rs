use async_trait::async_trait;

use crate::{GitCheckoutReport, GitCheckoutRequest, GitSubstrateError};

#[async_trait]
pub trait GitWorktreeMaterializer: Send + Sync {
    async fn materialize(
        &self,
        request: GitCheckoutRequest,
    ) -> Result<GitCheckoutReport, GitSubstrateError>;
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicGitWorktreeMaterializer;

#[async_trait]
impl GitWorktreeMaterializer for DeterministicGitWorktreeMaterializer {
    async fn materialize(
        &self,
        request: GitCheckoutRequest,
    ) -> Result<GitCheckoutReport, GitSubstrateError> {
        Ok(GitCheckoutReport {
            target_ref: request.target_ref,
            materialized_path: request.materialize_path,
        })
    }
}
