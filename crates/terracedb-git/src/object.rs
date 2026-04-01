use async_trait::async_trait;

use crate::{GitObject, GitSubstrateError};

#[async_trait]
pub trait GitObjectDatabase: Send + Sync {
    async fn read_object(&self, oid: &str) -> Result<Option<GitObject>, GitSubstrateError>;
}

pub use crate::types::GitObjectKind;
