use async_trait::async_trait;

use crate::{GitReference, GitSubstrateError};

#[async_trait]
pub trait GitRefDatabase: Send + Sync {
    async fn list_refs(&self) -> Result<Vec<GitReference>, GitSubstrateError>;
}
