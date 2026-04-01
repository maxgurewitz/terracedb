use std::sync::Arc;

use async_trait::async_trait;

use crate::{GitCancellationToken, GitCheckoutReport, GitCheckoutRequest, GitSubstrateError};

#[async_trait]
pub trait GitWorktreeMaterializer: Send + Sync {
    async fn materialize(
        &self,
        repository_id: &str,
        request: GitCheckoutRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCheckoutReport, GitSubstrateError>;
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicGitWorktreeMaterializer;

#[async_trait]
impl GitWorktreeMaterializer for DeterministicGitWorktreeMaterializer {
    async fn materialize(
        &self,
        repository_id: &str,
        request: GitCheckoutRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitCheckoutReport, GitSubstrateError> {
        if cancellation.is_cancelled() {
            return Err(GitSubstrateError::Cancelled {
                repository_id: repository_id.to_string(),
            });
        }
        Ok(GitCheckoutReport {
            target_ref: request.target_ref,
            materialized_path: request.materialize_path,
        })
    }
}
