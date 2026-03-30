use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::Timestamp;

use crate::{AgentFsError, JsonValue, ToolRunId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunStatus {
    Pending,
    Success,
    Error,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolRun {
    pub id: ToolRunId,
    pub name: String,
    pub status: ToolRunStatus,
    pub params: Option<JsonValue>,
    pub result: Option<JsonValue>,
    pub error: Option<String>,
    pub started_at: Timestamp,
    pub completed_at: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompletedToolRun {
    pub name: String,
    pub params: Option<JsonValue>,
    pub outcome: CompletedToolRunOutcome,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CompletedToolRunOutcome {
    Success { result: Option<JsonValue> },
    Error { message: String },
}

#[async_trait]
pub trait ReadOnlyAgentToolRuns: Send + Sync {
    async fn get(&self, id: ToolRunId) -> Result<Option<ToolRun>, AgentFsError>;
    async fn recent(&self, limit: Option<usize>) -> Result<Vec<ToolRun>, AgentFsError>;
}

#[async_trait]
pub trait AgentToolRuns: ReadOnlyAgentToolRuns {
    async fn start(&self, name: &str, params: Option<JsonValue>)
    -> Result<ToolRunId, AgentFsError>;
    async fn success(&self, id: ToolRunId, result: Option<JsonValue>) -> Result<(), AgentFsError>;
    async fn error(&self, id: ToolRunId, message: String) -> Result<(), AgentFsError>;
    async fn record_completed(&self, input: CompletedToolRun) -> Result<ToolRunId, AgentFsError>;
}
