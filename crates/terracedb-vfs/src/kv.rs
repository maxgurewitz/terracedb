use async_trait::async_trait;

use crate::{JsonValue, VfsError};

#[async_trait]
pub trait ReadOnlyVfsKvStore: Send + Sync {
    async fn get_json(&self, key: &str) -> Result<Option<JsonValue>, VfsError>;
    async fn list_keys(&self) -> Result<Vec<String>, VfsError>;
}

#[async_trait]
pub trait VfsKvStore: ReadOnlyVfsKvStore {
    async fn set_json(&self, key: &str, value: JsonValue) -> Result<(), VfsError>;
    async fn delete(&self, key: &str) -> Result<(), VfsError>;
}
