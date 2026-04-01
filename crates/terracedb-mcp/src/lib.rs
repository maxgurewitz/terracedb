//! Frozen SSE-based MCP tool, resource, session, and auth contracts for TerraceDB adapters.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::Timestamp;
use terracedb_capabilities::{BudgetPolicy, ExecutionPolicy, PolicySubject, ResourcePolicy};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpToolDescriptor {
    pub name: String,
    pub description: Option<String>,
    pub capability_binding: Option<String>,
    pub resource_policy: Option<ResourcePolicy>,
    pub budget_policy: Option<BudgetPolicy>,
    pub input_schema: JsonValue,
    pub output_schema: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpResourceDescriptor {
    pub name: String,
    pub uri_template: String,
    pub description: Option<String>,
    pub mime_type: Option<String>,
    pub capability_binding: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpAuthContext {
    pub subject: Option<PolicySubject>,
    pub authentication_kind: Option<String>,
    pub trusted_draft: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpAuthenticationRequest {
    pub session_id: String,
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    pub peer_addr: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Error, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum McpAuthenticationError {
    #[error("the MCP request was unauthenticated")]
    Unauthenticated,
    #[error("the MCP request was rejected by the authenticator")]
    Forbidden,
    #[error("the authenticator could not evaluate the request: {message}")]
    Transport { message: String },
}

#[async_trait]
pub trait McpAuthenticator: Send + Sync {
    async fn authenticate(
        &self,
        request: &McpAuthenticationRequest,
    ) -> Result<McpAuthContext, McpAuthenticationError>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpSseEndpoint {
    pub events_url: String,
    pub post_url: Option<String>,
    pub last_event_id: Option<String>,
    pub retry_millis: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpSessionContext {
    pub session_id: String,
    pub stream_id: String,
    pub endpoint: McpSseEndpoint,
    pub auth: McpAuthContext,
    pub execution_policy: ExecutionPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_resources: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpSseConnectionState {
    Connecting,
    Active,
    Reconnecting,
    Closing,
    Closed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpSessionRecord {
    pub context: McpSessionContext,
    pub state: McpSseConnectionState,
    pub updated_at: Timestamp,
}

pub trait McpSessionRegistry {
    fn record(&mut self, record: McpSessionRecord);

    fn load(&self, stream_id: &str) -> Option<McpSessionRecord>;
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicMcpSessionRegistry {
    sessions: BTreeMap<String, McpSessionRecord>,
}

impl McpSessionRegistry for DeterministicMcpSessionRegistry {
    fn record(&mut self, record: McpSessionRecord) {
        self.sessions
            .insert(record.context.stream_id.clone(), record);
    }

    fn load(&self, stream_id: &str) -> Option<McpSessionRecord> {
        self.sessions.get(stream_id).cloned()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicMcpAuthenticator {
    bearer_tokens: BTreeMap<String, McpAuthContext>,
    default_context: Option<McpAuthContext>,
}

impl DeterministicMcpAuthenticator {
    pub fn with_bearer_token(mut self, token: impl Into<String>, context: McpAuthContext) -> Self {
        self.bearer_tokens.insert(token.into(), context);
        self
    }

    pub fn with_default_context(mut self, context: McpAuthContext) -> Self {
        self.default_context = Some(context);
        self
    }
}

#[async_trait]
impl McpAuthenticator for DeterministicMcpAuthenticator {
    async fn authenticate(
        &self,
        request: &McpAuthenticationRequest,
    ) -> Result<McpAuthContext, McpAuthenticationError> {
        if let Some(header) = request.headers.get("authorization") {
            if let Some(token) = header.strip_prefix("Bearer ") {
                return self
                    .bearer_tokens
                    .get(token)
                    .cloned()
                    .ok_or(McpAuthenticationError::Forbidden);
            }
        }

        self.default_context
            .clone()
            .ok_or(McpAuthenticationError::Unauthenticated)
    }
}
