use async_trait::async_trait;

use crate::{AgentFsError, JsonValue};

#[async_trait]
pub trait ReadOnlyAgentKvStore: Send + Sync {
    async fn get_json(&self, key: &str) -> Result<Option<JsonValue>, AgentFsError>;
    async fn list_keys(&self) -> Result<Vec<String>, AgentFsError>;
}

#[async_trait]
pub trait AgentKvStore: ReadOnlyAgentKvStore {
    async fn set_json(&self, key: &str, value: JsonValue) -> Result<(), AgentFsError>;
    async fn delete(&self, key: &str) -> Result<(), AgentFsError>;
}
