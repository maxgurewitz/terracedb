use async_trait::async_trait;

use crate::{GitSubstrateError, types::GitIndexSnapshot};

#[async_trait]
pub trait GitIndexStore: Send + Sync {
    async fn index(&self) -> Result<GitIndexSnapshot, GitSubstrateError>;
}
