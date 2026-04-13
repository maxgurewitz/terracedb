//! Frozen SSE-based MCP contracts plus a deterministic adapter over reviewed procedures
//! and approved sandbox read-only surfaces.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use terracedb::{Clock, Timestamp};
use terracedb_capabilities::{
    BudgetPolicy, CapabilityManifest, CapabilityUseMetrics, CapabilityUseRequest,
    DeterministicPolicyEngine, ExecutionOperation, ExecutionPolicy, ManifestBinding,
    PolicyDecisionRecord, PolicyError, PolicyOutcomeKind, PolicySubject, ResolvedSessionPolicy,
    ResourceKind, ResourcePolicy, ResourceTarget, SessionMode, VisibilityIndexReader,
};
use terracedb_procedures::{
    DeterministicProcedurePublicationStore, ProcedureAuditRecord, ProcedureDeployment,
    ProcedureInvocationContext, ProcedureInvocationReceipt, ProcedureInvocationRecord,
    ProcedureInvocationRequest, ProcedurePublicationStore, ProcedureVersionRef,
    ReviewedProcedurePublication, ReviewedProcedureRuntime, invoke_published_procedure,
};
use terracedb_sandbox::{
    READONLY_VIEW_URI_SCHEME, ReadonlyViewClient, ReadonlyViewLocation, ReadonlyViewNodeKind,
    ReadonlyViewProtocolTransport, ReadonlyViewSessionRegistry, ReadonlyViewSessionSummary,
};
use terracedb_vfs::{ToolRunStatus, VolumeId};
use thiserror::Error;
use tokio::sync::Mutex;

pub const MCP_TOOL_PROCEDURES_LIST: &str = "procedures.list";
pub const MCP_TOOL_PROCEDURES_INSPECT: &str = "procedures.inspect";
pub const MCP_TOOL_PROCEDURES_INVOKE: &str = "procedures.invoke";
pub const MCP_TOOL_PROCEDURES_AUDITS: &str = "procedures.audits";
pub const MCP_TOOL_SANDBOX_SESSIONS_LIST: &str = "sandbox.sessions.list";
pub const MCP_TOOL_SANDBOX_FILE_DIFF: &str = "sandbox.file.diff";
pub const MCP_TOOL_SANDBOX_TOOL_RUNS_LIST: &str = "sandbox.tool_runs.list";
pub const MCP_TOOL_DRAFT_QUERY_RUN: &str = "draft.query.run";

pub const MCP_RESOURCE_PROCEDURE_METADATA: &str = "procedure.metadata";
pub const MCP_RESOURCE_PROCEDURE_AUDITS: &str = "procedure.audits";
pub const MCP_RESOURCE_SANDBOX_VIEW: &str = "sandbox.view";
pub const MCP_RESOURCE_SANDBOX_TOOL_RUNS: &str = "sandbox.tool_runs";

pub const MCP_PROCEDURE_URI_SCHEME: &str = "terrace-procedure";
pub const MCP_PROCEDURE_AUDIT_URI_SCHEME: &str = "terrace-procedure-audit";
pub const MCP_TOOL_RUNS_URI_SCHEME: &str = "terrace-tool-runs";

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
pub struct McpAuditContext {
    pub session_id: String,
    pub stream_id: String,
    pub session_mode: SessionMode,
    pub subject: PolicySubject,
    pub effective_manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpSessionRecord {
    pub context: McpSessionContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<ResolvedSessionPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_context: Option<McpAuditContext>,
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

pub trait McpSessionPolicyResolver: Send + Sync {
    fn resolve_mcp_session(
        &self,
        subject: PolicySubject,
        session_id: &str,
        preset_name: Option<&str>,
        profile_name: Option<&str>,
    ) -> Result<ResolvedSessionPolicy, PolicyError>;
}

impl McpSessionPolicyResolver for DeterministicPolicyEngine {
    fn resolve_mcp_session(
        &self,
        subject: PolicySubject,
        _session_id: &str,
        preset_name: Option<&str>,
        profile_name: Option<&str>,
    ) -> Result<ResolvedSessionPolicy, PolicyError> {
        self.resolve_for_subject(
            subject,
            SessionMode::Mcp,
            preset_name.map(ToOwned::to_owned),
            profile_name.map(ToOwned::to_owned),
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpAdapterBindings {
    pub procedures: Option<String>,
    pub sandbox: Option<String>,
    pub draft_queries: Option<String>,
}

impl McpAdapterBindings {
    pub fn with_procedures(mut self, binding_name: impl Into<String>) -> Self {
        self.procedures = Some(binding_name.into());
        self
    }

    pub fn with_sandbox(mut self, binding_name: impl Into<String>) -> Self {
        self.sandbox = Some(binding_name.into());
        self
    }

    pub fn with_draft_queries(mut self, binding_name: impl Into<String>) -> Self {
        self.draft_queries = Some(binding_name.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpPublishedProcedureRoute {
    pub publication: ProcedureVersionRef,
    pub deployment: ProcedureDeployment,
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl McpPublishedProcedureRoute {
    pub fn new(publication: ProcedureVersionRef, deployment: ProcedureDeployment) -> Self {
        Self {
            publication,
            deployment,
            description: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: JsonValue) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct McpAdapterConfig {
    pub bindings: McpAdapterBindings,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub procedures: Vec<McpPublishedProcedureRoute>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl McpAdapterConfig {
    pub fn with_procedure(mut self, route: McpPublishedProcedureRoute) -> Self {
        self.procedures.push(route);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: JsonValue) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpPublishedProcedureSummary {
    pub publication: ProcedureVersionRef,
    pub entrypoint: String,
    pub code_hash: String,
    pub published_at: Timestamp,
    pub reviewed_by: String,
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpProcedureDetail {
    pub publication: ReviewedProcedurePublication,
    pub deployment: ProcedureDeployment,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audits: Vec<ProcedureAuditRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub invocations: Vec<ProcedureInvocationRecord>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpSandboxFileDiff {
    pub visible: Option<Vec<u8>>,
    pub durable: Option<Vec<u8>>,
    pub changed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpToolRunRecord {
    pub id: String,
    pub name: String,
    pub status: ToolRunStatus,
    pub params: Option<JsonValue>,
    pub result: Option<JsonValue>,
    pub error: Option<String>,
    pub started_at: Timestamp,
    pub completed_at: Option<Timestamp>,
}

impl From<terracedb_vfs::ToolRun> for McpToolRunRecord {
    fn from(value: terracedb_vfs::ToolRun) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            status: value.status,
            params: value.params,
            result: value.result,
            error: value.error,
            started_at: value.started_at,
            completed_at: value.completed_at,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct McpDraftQueryRequest {
    pub query: JsonValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_resources: Vec<ResourceTarget>,
    #[serde(default)]
    pub metrics: CapabilityUseMetrics,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpDraftQueryOutput {
    pub result: JsonValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum McpDraftQueryError {
    #[error("draft query execution failed: {message}")]
    Failed { message: String },
}

#[async_trait]
pub trait McpDraftQueryRuntime: Send + Sync {
    async fn run_query(
        &self,
        session: &McpSessionRecord,
        binding_name: &str,
        request: &McpDraftQueryRequest,
    ) -> Result<McpDraftQueryOutput, McpDraftQueryError>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeterministicDraftQueryBehavior {
    Return { result: JsonValue },
    Echo,
    Error { message: String },
}

#[derive(Default)]
pub struct DeterministicDraftQueryRuntime {
    behaviors: Mutex<BTreeMap<String, DeterministicDraftQueryBehavior>>,
    invocations: Mutex<Vec<(String, McpDraftQueryRequest)>>,
}

impl std::fmt::Debug for DeterministicDraftQueryRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeterministicDraftQueryRuntime").finish()
    }
}

impl DeterministicDraftQueryRuntime {
    pub fn with_result(self, binding_name: impl Into<String>, result: JsonValue) -> Self {
        self.with_behavior(
            binding_name,
            DeterministicDraftQueryBehavior::Return { result },
        )
    }

    pub fn with_echo(self, binding_name: impl Into<String>) -> Self {
        self.with_behavior(binding_name, DeterministicDraftQueryBehavior::Echo)
    }

    pub fn with_error(self, binding_name: impl Into<String>, message: impl Into<String>) -> Self {
        self.with_behavior(
            binding_name,
            DeterministicDraftQueryBehavior::Error {
                message: message.into(),
            },
        )
    }

    fn with_behavior(
        self,
        binding_name: impl Into<String>,
        behavior: DeterministicDraftQueryBehavior,
    ) -> Self {
        let mut this = self;
        this.behaviors
            .get_mut()
            .insert(binding_name.into(), behavior);
        this
    }

    pub async fn invocation_count(&self, binding_name: &str) -> usize {
        self.invocations
            .lock()
            .await
            .iter()
            .filter(|(binding, _)| binding == binding_name)
            .count()
    }
}

#[async_trait]
impl McpDraftQueryRuntime for DeterministicDraftQueryRuntime {
    async fn run_query(
        &self,
        _session: &McpSessionRecord,
        binding_name: &str,
        request: &McpDraftQueryRequest,
    ) -> Result<McpDraftQueryOutput, McpDraftQueryError> {
        self.invocations
            .lock()
            .await
            .push((binding_name.to_string(), request.clone()));
        match self.behaviors.lock().await.get(binding_name).cloned() {
            Some(DeterministicDraftQueryBehavior::Return { result }) => Ok(McpDraftQueryOutput {
                result,
                metadata: BTreeMap::new(),
            }),
            Some(DeterministicDraftQueryBehavior::Echo) => Ok(McpDraftQueryOutput {
                result: request.query.clone(),
                metadata: BTreeMap::new(),
            }),
            Some(DeterministicDraftQueryBehavior::Error { message }) => {
                Err(McpDraftQueryError::Failed { message })
            }
            None => Err(McpDraftQueryError::Failed {
                message: "no deterministic draft query behavior registered".to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum McpToolCall {
    ListProcedures,
    InspectProcedure {
        publication: ProcedureVersionRef,
    },
    InvokeProcedure {
        publication: ProcedureVersionRef,
        arguments: JsonValue,
    },
    ListProcedureAudits {
        publication: ProcedureVersionRef,
    },
    ListSandboxSessions,
    DiffSandboxFile {
        session_volume_id: VolumeId,
        path: String,
    },
    ListSandboxToolRuns {
        session_volume_id: VolumeId,
        limit: Option<usize>,
    },
    RunDraftQuery {
        request: McpDraftQueryRequest,
    },
}

impl McpToolCall {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ListProcedures => MCP_TOOL_PROCEDURES_LIST,
            Self::InspectProcedure { .. } => MCP_TOOL_PROCEDURES_INSPECT,
            Self::InvokeProcedure { .. } => MCP_TOOL_PROCEDURES_INVOKE,
            Self::ListProcedureAudits { .. } => MCP_TOOL_PROCEDURES_AUDITS,
            Self::ListSandboxSessions => MCP_TOOL_SANDBOX_SESSIONS_LIST,
            Self::DiffSandboxFile { .. } => MCP_TOOL_SANDBOX_FILE_DIFF,
            Self::ListSandboxToolRuns { .. } => MCP_TOOL_SANDBOX_TOOL_RUNS_LIST,
            Self::RunDraftQuery { .. } => MCP_TOOL_DRAFT_QUERY_RUN,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum McpToolResult {
    Procedures {
        procedures: Vec<McpPublishedProcedureSummary>,
    },
    Procedure {
        detail: McpProcedureDetail,
    },
    Invocation {
        receipt: ProcedureInvocationReceipt,
    },
    ProcedureAudits {
        audits: Vec<ProcedureAuditRecord>,
    },
    SandboxSessions {
        sessions: Vec<ReadonlyViewSessionSummary>,
    },
    SandboxFileDiff {
        diff: McpSandboxFileDiff,
    },
    SandboxToolRuns {
        runs: Vec<McpToolRunRecord>,
    },
    DraftQuery {
        output: McpDraftQueryOutput,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpToolCallOutput {
    pub tool_name: String,
    pub decision: PolicyDecisionRecord,
    pub result: McpToolResult,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum McpResourceContents {
    Json { mime_type: String, value: JsonValue },
    Bytes { mime_type: String, bytes: Vec<u8> },
    Missing,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpResourceReadResult {
    pub resource_name: String,
    pub decision: PolicyDecisionRecord,
    pub contents: McpResourceContents,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpOpenSessionRequest {
    pub authentication: McpAuthenticationRequest,
    pub endpoint: McpSseEndpoint,
    pub preset_name: Option<String>,
    pub profile_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_resources: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum McpProtocolRequest {
    OpenSession {
        request: McpOpenSessionRequest,
    },
    UpdateSessionState {
        stream_id: String,
        state: McpSseConnectionState,
    },
    LoadSession {
        stream_id: String,
    },
    ListTools {
        stream_id: String,
    },
    ListResources {
        stream_id: String,
    },
    CallTool {
        stream_id: String,
        call: McpToolCall,
    },
    ReadResource {
        stream_id: String,
        uri: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum McpProtocolResponse {
    Session {
        record: McpSessionRecord,
    },
    Tools {
        tools: Vec<McpToolDescriptor>,
    },
    Resources {
        resources: Vec<McpResourceDescriptor>,
    },
    ToolResult {
        result: McpToolCallOutput,
    },
    Resource {
        resource: McpResourceReadResult,
    },
    Ack,
}

#[async_trait]
pub trait McpProtocolTransport: Send + Sync {
    async fn send(
        &self,
        request: McpProtocolRequest,
    ) -> Result<McpProtocolResponse, McpAdapterError>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Error)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum McpAdapterError {
    #[error("mcp authentication failed: {error}")]
    Authentication { error: McpAuthenticationError },
    #[error("mcp session {session_id} could not be resolved to a subject")]
    SubjectMissing { session_id: String },
    #[error("mcp session policy resolution failed: {message}")]
    PolicyResolution { message: String },
    #[error("mcp stream {stream_id} was not found")]
    SessionNotFound { stream_id: String },
    #[error("mcp stream {stream_id} is not usable while {state:?}")]
    SessionUnavailable {
        stream_id: String,
        state: McpSseConnectionState,
    },
    #[error("mcp stream {stream_id} is missing resolved policy state")]
    SessionPolicyMissing { stream_id: String },
    #[error("mcp tool {tool_name} is not available")]
    UnknownTool { tool_name: String },
    #[error("mcp resource {uri} is not available")]
    UnknownResource { uri: String },
    #[error("mcp surface {surface} requires a trusted draft subject")]
    DraftTrustRequired { surface: String },
    #[error("draft query support is not configured for this adapter")]
    DraftQueriesDisabled,
    #[error("mcp tool {tool_name} was denied: {message}")]
    ToolForbidden {
        tool_name: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        policy: Option<PolicyDecisionRecord>,
    },
    #[error("mcp resource {uri} was denied: {message}")]
    ResourceForbidden {
        uri: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        policy: Option<PolicyDecisionRecord>,
    },
    #[error("reviewed procedure {procedure_id}@{version} is not exposed through MCP")]
    UnknownProcedure { procedure_id: String, version: u64 },
    #[error("procedure runtime failed: {message}")]
    Procedure { message: String },
    #[error("sandbox view surface failed: {message}")]
    Sandbox { message: String },
    #[error("tool-run history failed: {message}")]
    ToolRuns { message: String },
    #[error("draft query failed: {message}")]
    DraftQuery { message: String },
    #[error("serialization failed: {message}")]
    Serialization { message: String },
}

pub struct McpAdapterService<V>
where
    V: ReadonlyViewProtocolTransport + 'static,
{
    clock: Arc<dyn Clock>,
    authenticator: Arc<dyn McpAuthenticator>,
    policy_resolver: Arc<dyn McpSessionPolicyResolver>,
    sessions: Arc<Mutex<DeterministicMcpSessionRegistry>>,
    config: McpAdapterConfig,
    procedure_store: Arc<Mutex<DeterministicProcedurePublicationStore>>,
    procedure_runtime: Arc<Mutex<Box<dyn ReviewedProcedureRuntime + Send>>>,
    visibility_indexes: Arc<dyn VisibilityIndexReader + Send + Sync>,
    readonly_views: ReadonlyViewClient<V>,
    sandbox_sessions: Arc<dyn ReadonlyViewSessionRegistry>,
    draft_queries: Option<Arc<dyn McpDraftQueryRuntime>>,
}

impl<V> McpAdapterService<V>
where
    V: ReadonlyViewProtocolTransport + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        clock: Arc<dyn Clock>,
        authenticator: Arc<dyn McpAuthenticator>,
        policy_resolver: Arc<dyn McpSessionPolicyResolver>,
        config: McpAdapterConfig,
        procedure_store: DeterministicProcedurePublicationStore,
        procedure_runtime: Box<dyn ReviewedProcedureRuntime + Send>,
        visibility_indexes: Arc<dyn VisibilityIndexReader + Send + Sync>,
        readonly_views: ReadonlyViewClient<V>,
        sandbox_sessions: Arc<dyn ReadonlyViewSessionRegistry>,
    ) -> Self {
        Self {
            clock,
            authenticator,
            policy_resolver,
            sessions: Arc::new(Mutex::new(DeterministicMcpSessionRegistry::default())),
            config,
            procedure_store: Arc::new(Mutex::new(procedure_store)),
            procedure_runtime: Arc::new(Mutex::new(procedure_runtime)),
            visibility_indexes,
            readonly_views,
            sandbox_sessions,
            draft_queries: None,
        }
    }

    pub fn with_draft_query_runtime(mut self, runtime: Arc<dyn McpDraftQueryRuntime>) -> Self {
        self.draft_queries = Some(runtime);
        self
    }

    pub fn session_registry(&self) -> Arc<Mutex<DeterministicMcpSessionRegistry>> {
        self.sessions.clone()
    }

    pub fn local_client(self: &Arc<Self>) -> McpClient<LocalMcpTransport<V>> {
        McpClient::new(LocalMcpTransport::new(self.clone()))
    }

    pub async fn handle_protocol(
        &self,
        request: McpProtocolRequest,
    ) -> Result<McpProtocolResponse, McpAdapterError> {
        match request {
            McpProtocolRequest::OpenSession { request } => Ok(McpProtocolResponse::Session {
                record: self.open_session(request).await?,
            }),
            McpProtocolRequest::UpdateSessionState { stream_id, state } => {
                self.update_session_state(&stream_id, state).await?;
                Ok(McpProtocolResponse::Ack)
            }
            McpProtocolRequest::LoadSession { stream_id } => Ok(McpProtocolResponse::Session {
                record: self.load_session(&stream_id).await?,
            }),
            McpProtocolRequest::ListTools { stream_id } => Ok(McpProtocolResponse::Tools {
                tools: self.list_tools(&stream_id).await?,
            }),
            McpProtocolRequest::ListResources { stream_id } => Ok(McpProtocolResponse::Resources {
                resources: self.list_resources(&stream_id).await?,
            }),
            McpProtocolRequest::CallTool { stream_id, call } => {
                Ok(McpProtocolResponse::ToolResult {
                    result: self.call_tool(&stream_id, call).await?,
                })
            }
            McpProtocolRequest::ReadResource { stream_id, uri } => {
                Ok(McpProtocolResponse::Resource {
                    resource: self.read_resource(&stream_id, &uri).await?,
                })
            }
        }
    }

    pub async fn handle_protocol_json(&self, payload: &[u8]) -> Result<Vec<u8>, McpAdapterError> {
        let request = serde_json::from_slice::<McpProtocolRequest>(payload).map_err(|error| {
            McpAdapterError::Serialization {
                message: error.to_string(),
            }
        })?;
        let response = self.handle_protocol(request).await?;
        serde_json::to_vec(&response).map_err(|error| McpAdapterError::Serialization {
            message: error.to_string(),
        })
    }

    pub async fn open_session(
        &self,
        request: McpOpenSessionRequest,
    ) -> Result<McpSessionRecord, McpAdapterError> {
        let auth = self
            .authenticator
            .authenticate(&request.authentication)
            .await
            .map_err(|error| McpAdapterError::Authentication { error })?;
        let subject = auth
            .subject
            .clone()
            .ok_or_else(|| McpAdapterError::SubjectMissing {
                session_id: request.authentication.session_id.clone(),
            })?;
        let resolved = self
            .policy_resolver
            .resolve_mcp_session(
                subject.clone(),
                &request.authentication.session_id,
                request.preset_name.as_deref(),
                request.profile_name.as_deref(),
            )
            .map_err(|error| McpAdapterError::PolicyResolution {
                message: error.to_string(),
            })?;
        let stream_id = request
            .authentication
            .stream_id
            .clone()
            .unwrap_or_else(|| request.authentication.session_id.clone());
        let mut metadata = request.metadata.clone();
        if let Some(preset_name) = request.preset_name.clone() {
            metadata.insert("preset_name".to_string(), JsonValue::String(preset_name));
        }
        if let Some(profile_name) = request.profile_name.clone() {
            metadata.insert("profile_name".to_string(), JsonValue::String(profile_name));
        }
        let context = McpSessionContext {
            session_id: request.authentication.session_id.clone(),
            stream_id: stream_id.clone(),
            endpoint: request.endpoint,
            auth: auth.clone(),
            execution_policy: resolved.execution_policy.clone(),
            requested_tools: request.requested_tools,
            requested_resources: request.requested_resources,
            metadata,
        };
        let audit_context = McpAuditContext {
            session_id: context.session_id.clone(),
            stream_id: context.stream_id.clone(),
            session_mode: SessionMode::Mcp,
            subject,
            effective_manifest: resolved.manifest.clone(),
            execution_policy: resolved.execution_policy.clone(),
            metadata: session_audit_metadata(&context, request.preset_name, request.profile_name),
        };
        let record = McpSessionRecord {
            context,
            policy: Some(resolved),
            audit_context: Some(audit_context),
            state: McpSseConnectionState::Active,
            updated_at: self.clock.now(),
        };
        self.sessions.lock().await.record(record.clone());
        Ok(record)
    }

    pub async fn load_session(&self, stream_id: &str) -> Result<McpSessionRecord, McpAdapterError> {
        self.sessions
            .lock()
            .await
            .load(stream_id)
            .ok_or_else(|| McpAdapterError::SessionNotFound {
                stream_id: stream_id.to_string(),
            })
    }

    pub async fn update_session_state(
        &self,
        stream_id: &str,
        state: McpSseConnectionState,
    ) -> Result<(), McpAdapterError> {
        let mut registry = self.sessions.lock().await;
        let mut record =
            registry
                .load(stream_id)
                .ok_or_else(|| McpAdapterError::SessionNotFound {
                    stream_id: stream_id.to_string(),
                })?;
        record.state = state;
        record.updated_at = self.clock.now();
        registry.record(record);
        Ok(())
    }

    pub async fn list_tools(
        &self,
        stream_id: &str,
    ) -> Result<Vec<McpToolDescriptor>, McpAdapterError> {
        let record = self.require_usable_session(stream_id).await?;
        Ok(self
            .tool_surfaces()
            .into_iter()
            .filter_map(|surface| self.surface_tool_descriptor(&record, surface))
            .collect())
    }

    pub async fn list_resources(
        &self,
        stream_id: &str,
    ) -> Result<Vec<McpResourceDescriptor>, McpAdapterError> {
        let record = self.require_usable_session(stream_id).await?;
        Ok(self
            .resource_surfaces()
            .into_iter()
            .filter_map(|surface| self.surface_resource_descriptor(&record, surface))
            .collect())
    }

    pub async fn call_tool(
        &self,
        stream_id: &str,
        call: McpToolCall,
    ) -> Result<McpToolCallOutput, McpAdapterError> {
        let record = self.require_usable_session(stream_id).await?;
        match call {
            McpToolCall::ListProcedures => {
                let binding_name = self.config.bindings.procedures.as_deref().ok_or_else(|| {
                    McpAdapterError::UnknownTool {
                        tool_name: MCP_TOOL_PROCEDURES_LIST.to_string(),
                    }
                })?;
                self.ensure_requested_tool(&record, MCP_TOOL_PROCEDURES_LIST)?;
                let decision = self.authorize_tool(
                    &record,
                    MCP_TOOL_PROCEDURES_LIST,
                    binding_name,
                    Vec::new(),
                    false,
                )?;
                let store = self.procedure_store.lock().await;
                let procedures = self
                    .config
                    .procedures
                    .iter()
                    .filter_map(|route| {
                        let publication = store.load(&route.publication)?;
                        let procedure_allowed = self.decision_allowed(&self.evaluate_surface(
                            &record,
                            binding_name,
                            surface_targets(
                                ResourceKind::McpTool,
                                MCP_TOOL_PROCEDURES_LIST.to_string(),
                                vec![ResourceTarget {
                                    kind: ResourceKind::Procedure,
                                    identifier: publication.publication.procedure_id.clone(),
                                }],
                            ),
                        ));
                        procedure_allowed.then(|| McpPublishedProcedureSummary {
                            publication: publication.publication.clone(),
                            entrypoint: publication.entrypoint,
                            code_hash: publication.code_hash,
                            published_at: publication.published_at,
                            reviewed_by: publication.review.reviewed_by,
                            description: route.description.clone(),
                            metadata: route.metadata.clone(),
                        })
                    })
                    .collect();
                Ok(McpToolCallOutput {
                    tool_name: MCP_TOOL_PROCEDURES_LIST.to_string(),
                    decision,
                    result: McpToolResult::Procedures { procedures },
                })
            }
            McpToolCall::InspectProcedure { publication } => {
                let binding_name = self.config.bindings.procedures.as_deref().ok_or_else(|| {
                    McpAdapterError::UnknownTool {
                        tool_name: MCP_TOOL_PROCEDURES_INSPECT.to_string(),
                    }
                })?;
                self.ensure_requested_tool(&record, MCP_TOOL_PROCEDURES_INSPECT)?;
                let route = self.require_procedure_route(&publication)?;
                let decision = self.authorize_tool(
                    &record,
                    MCP_TOOL_PROCEDURES_INSPECT,
                    binding_name,
                    vec![ResourceTarget {
                        kind: ResourceKind::Procedure,
                        identifier: publication.procedure_id.clone(),
                    }],
                    false,
                )?;
                let store = self.procedure_store.lock().await;
                let publication_record =
                    store
                        .load(&publication)
                        .ok_or_else(|| McpAdapterError::UnknownProcedure {
                            procedure_id: publication.procedure_id.clone(),
                            version: publication.version,
                        })?;
                let detail = McpProcedureDetail {
                    publication: publication_record.clone(),
                    deployment: route.deployment.clone(),
                    audits: store.audits_for_publication(&publication),
                    invocations: store.invocations_for_publication(&publication),
                };
                Ok(McpToolCallOutput {
                    tool_name: MCP_TOOL_PROCEDURES_INSPECT.to_string(),
                    decision,
                    result: McpToolResult::Procedure { detail },
                })
            }
            McpToolCall::InvokeProcedure {
                publication,
                arguments,
            } => {
                let binding_name = self.config.bindings.procedures.as_deref().ok_or_else(|| {
                    McpAdapterError::UnknownTool {
                        tool_name: MCP_TOOL_PROCEDURES_INVOKE.to_string(),
                    }
                })?;
                self.ensure_requested_tool(&record, MCP_TOOL_PROCEDURES_INVOKE)?;
                let route = self.require_procedure_route(&publication)?;
                let decision = self.authorize_tool(
                    &record,
                    MCP_TOOL_PROCEDURES_INVOKE,
                    binding_name,
                    vec![ResourceTarget {
                        kind: ResourceKind::Procedure,
                        identifier: publication.procedure_id.clone(),
                    }],
                    false,
                )?;
                let invoked_at = self.clock.now();
                let completed_at = self.clock.now();
                let request = ProcedureInvocationRequest {
                    publication: publication.clone(),
                    arguments,
                    context: ProcedureInvocationContext {
                        caller: record.policy.as_ref().map(|policy| policy.subject.clone()),
                        session_mode: SessionMode::ReviewedProcedure,
                        session_id: Some(record.context.session_id.clone()),
                        dry_run: false,
                        metadata: BTreeMap::from([
                            (
                                "mcp_stream_id".to_string(),
                                JsonValue::String(record.context.stream_id.clone()),
                            ),
                            (
                                "mcp_session_mode".to_string(),
                                JsonValue::String("mcp".to_string()),
                            ),
                        ]),
                    },
                };
                let receipt = {
                    let mut store = self.procedure_store.lock().await;
                    let mut runtime = self.procedure_runtime.lock().await;
                    invoke_published_procedure(
                        &mut store,
                        runtime.as_mut(),
                        &route.deployment,
                        request,
                        invoked_at,
                        completed_at,
                        self.visibility_indexes.as_ref(),
                    )
                    .map_err(|error| McpAdapterError::Procedure {
                        message: error.to_string(),
                    })?
                };
                Ok(McpToolCallOutput {
                    tool_name: MCP_TOOL_PROCEDURES_INVOKE.to_string(),
                    decision,
                    result: McpToolResult::Invocation { receipt },
                })
            }
            McpToolCall::ListProcedureAudits { publication } => {
                let binding_name = self.config.bindings.procedures.as_deref().ok_or_else(|| {
                    McpAdapterError::UnknownTool {
                        tool_name: MCP_TOOL_PROCEDURES_AUDITS.to_string(),
                    }
                })?;
                self.ensure_requested_tool(&record, MCP_TOOL_PROCEDURES_AUDITS)?;
                self.require_procedure_route(&publication)?;
                let decision = self.authorize_tool(
                    &record,
                    MCP_TOOL_PROCEDURES_AUDITS,
                    binding_name,
                    vec![ResourceTarget {
                        kind: ResourceKind::Procedure,
                        identifier: publication.procedure_id.clone(),
                    }],
                    false,
                )?;
                let audits = self
                    .procedure_store
                    .lock()
                    .await
                    .audits_for_publication(&publication);
                Ok(McpToolCallOutput {
                    tool_name: MCP_TOOL_PROCEDURES_AUDITS.to_string(),
                    decision,
                    result: McpToolResult::ProcedureAudits { audits },
                })
            }
            McpToolCall::ListSandboxSessions => {
                let binding_name = self.config.bindings.sandbox.as_deref().ok_or_else(|| {
                    McpAdapterError::UnknownTool {
                        tool_name: MCP_TOOL_SANDBOX_SESSIONS_LIST.to_string(),
                    }
                })?;
                self.ensure_requested_tool(&record, MCP_TOOL_SANDBOX_SESSIONS_LIST)?;
                let decision = self.authorize_tool(
                    &record,
                    MCP_TOOL_SANDBOX_SESSIONS_LIST,
                    binding_name,
                    Vec::new(),
                    true,
                )?;
                let sessions = self
                    .sandbox_sessions
                    .list_sessions()
                    .await
                    .map_err(|error| McpAdapterError::Sandbox {
                        message: error.to_string(),
                    })?;
                let mut summaries = Vec::new();
                for session in sessions {
                    let info = session.info().await;
                    let session_decision = self.evaluate_surface(
                        &record,
                        binding_name,
                        surface_targets(
                            ResourceKind::McpTool,
                            MCP_TOOL_SANDBOX_SESSIONS_LIST.to_string(),
                            vec![ResourceTarget {
                                kind: ResourceKind::Session,
                                identifier: info.session_volume_id.to_string(),
                            }],
                        ),
                    );
                    if !self.decision_allowed(&session_decision) {
                        continue;
                    }
                    let label = format!("{} {}", info.session_volume_id, info.workspace_root);
                    summaries.push(ReadonlyViewSessionSummary {
                        session_volume_id: info.session_volume_id,
                        workspace_root: info.workspace_root,
                        state: info.state,
                        label,
                        provider: info.services.readonly_view_provider,
                        active_view_handles: info.provenance.active_view_handles.len(),
                        available_cuts: vec![
                            terracedb_sandbox::ReadonlyViewCut::Visible,
                            terracedb_sandbox::ReadonlyViewCut::Durable,
                        ],
                    });
                }
                summaries
                    .sort_by(|left, right| left.session_volume_id.cmp(&right.session_volume_id));
                Ok(McpToolCallOutput {
                    tool_name: MCP_TOOL_SANDBOX_SESSIONS_LIST.to_string(),
                    decision,
                    result: McpToolResult::SandboxSessions {
                        sessions: summaries,
                    },
                })
            }
            McpToolCall::DiffSandboxFile {
                session_volume_id,
                path,
            } => {
                let binding_name = self.config.bindings.sandbox.as_deref().ok_or_else(|| {
                    McpAdapterError::UnknownTool {
                        tool_name: MCP_TOOL_SANDBOX_FILE_DIFF.to_string(),
                    }
                })?;
                self.ensure_requested_tool(&record, MCP_TOOL_SANDBOX_FILE_DIFF)?;
                let decision = self.authorize_tool(
                    &record,
                    MCP_TOOL_SANDBOX_FILE_DIFF,
                    binding_name,
                    vec![ResourceTarget {
                        kind: ResourceKind::Session,
                        identifier: session_volume_id.to_string(),
                    }],
                    true,
                )?;
                let visible = self
                    .readonly_views
                    .read_file(&ReadonlyViewLocation {
                        session_volume_id,
                        cut: terracedb_sandbox::ReadonlyViewCut::Visible,
                        path: path.clone(),
                    })
                    .await
                    .map_err(|error| McpAdapterError::Sandbox {
                        message: error.to_string(),
                    })?;
                let durable = self
                    .readonly_views
                    .read_file(&ReadonlyViewLocation {
                        session_volume_id,
                        cut: terracedb_sandbox::ReadonlyViewCut::Durable,
                        path,
                    })
                    .await
                    .map_err(|error| McpAdapterError::Sandbox {
                        message: error.to_string(),
                    })?;
                let diff = McpSandboxFileDiff {
                    changed: visible != durable,
                    visible,
                    durable,
                };
                Ok(McpToolCallOutput {
                    tool_name: MCP_TOOL_SANDBOX_FILE_DIFF.to_string(),
                    decision,
                    result: McpToolResult::SandboxFileDiff { diff },
                })
            }
            McpToolCall::ListSandboxToolRuns {
                session_volume_id,
                limit,
            } => {
                let binding_name = self.config.bindings.sandbox.as_deref().ok_or_else(|| {
                    McpAdapterError::UnknownTool {
                        tool_name: MCP_TOOL_SANDBOX_TOOL_RUNS_LIST.to_string(),
                    }
                })?;
                self.ensure_requested_tool(&record, MCP_TOOL_SANDBOX_TOOL_RUNS_LIST)?;
                let decision = self.authorize_tool(
                    &record,
                    MCP_TOOL_SANDBOX_TOOL_RUNS_LIST,
                    binding_name,
                    vec![ResourceTarget {
                        kind: ResourceKind::Session,
                        identifier: session_volume_id.to_string(),
                    }],
                    true,
                )?;
                let runs = self.load_tool_runs(session_volume_id, limit).await?;
                Ok(McpToolCallOutput {
                    tool_name: MCP_TOOL_SANDBOX_TOOL_RUNS_LIST.to_string(),
                    decision,
                    result: McpToolResult::SandboxToolRuns { runs },
                })
            }
            McpToolCall::RunDraftQuery { request } => {
                let binding_name = self
                    .config
                    .bindings
                    .draft_queries
                    .as_deref()
                    .ok_or(McpAdapterError::DraftQueriesDisabled)?;
                self.ensure_requested_tool(&record, MCP_TOOL_DRAFT_QUERY_RUN)?;
                let decision = self.authorize_tool(
                    &record,
                    MCP_TOOL_DRAFT_QUERY_RUN,
                    binding_name,
                    request.target_resources.clone(),
                    true,
                )?;
                let runtime = self
                    .draft_queries
                    .as_ref()
                    .ok_or(McpAdapterError::DraftQueriesDisabled)?;
                let output = runtime
                    .run_query(&record, binding_name, &request)
                    .await
                    .map_err(|error| McpAdapterError::DraftQuery {
                        message: error.to_string(),
                    })?;
                Ok(McpToolCallOutput {
                    tool_name: MCP_TOOL_DRAFT_QUERY_RUN.to_string(),
                    decision,
                    result: McpToolResult::DraftQuery { output },
                })
            }
        }
    }

    pub async fn read_resource(
        &self,
        stream_id: &str,
        uri: &str,
    ) -> Result<McpResourceReadResult, McpAdapterError> {
        let record = self.require_usable_session(stream_id).await?;
        if let Some(publication) = parse_procedure_uri(MCP_PROCEDURE_URI_SCHEME, uri)? {
            let binding_name = self.config.bindings.procedures.as_deref().ok_or_else(|| {
                McpAdapterError::UnknownResource {
                    uri: uri.to_string(),
                }
            })?;
            self.ensure_requested_resource(&record, MCP_RESOURCE_PROCEDURE_METADATA)?;
            self.require_procedure_route(&publication)?;
            let decision = self.authorize_resource(
                &record,
                uri,
                binding_name,
                vec![ResourceTarget {
                    kind: ResourceKind::Procedure,
                    identifier: publication.procedure_id.clone(),
                }],
                false,
            )?;
            let detail = self.procedure_detail(&publication).await?;
            let value =
                serde_json::to_value(detail).map_err(|error| McpAdapterError::Serialization {
                    message: error.to_string(),
                })?;
            return Ok(McpResourceReadResult {
                resource_name: MCP_RESOURCE_PROCEDURE_METADATA.to_string(),
                decision,
                contents: McpResourceContents::Json {
                    mime_type: "application/json".to_string(),
                    value,
                },
            });
        }
        if let Some(publication) = parse_procedure_uri(MCP_PROCEDURE_AUDIT_URI_SCHEME, uri)? {
            let binding_name = self.config.bindings.procedures.as_deref().ok_or_else(|| {
                McpAdapterError::UnknownResource {
                    uri: uri.to_string(),
                }
            })?;
            self.ensure_requested_resource(&record, MCP_RESOURCE_PROCEDURE_AUDITS)?;
            self.require_procedure_route(&publication)?;
            let decision = self.authorize_resource(
                &record,
                uri,
                binding_name,
                vec![ResourceTarget {
                    kind: ResourceKind::Procedure,
                    identifier: publication.procedure_id.clone(),
                }],
                false,
            )?;
            let audits = self
                .procedure_store
                .lock()
                .await
                .audits_for_publication(&publication);
            let value =
                serde_json::to_value(audits).map_err(|error| McpAdapterError::Serialization {
                    message: error.to_string(),
                })?;
            return Ok(McpResourceReadResult {
                resource_name: MCP_RESOURCE_PROCEDURE_AUDITS.to_string(),
                decision,
                contents: McpResourceContents::Json {
                    mime_type: "application/json".to_string(),
                    value,
                },
            });
        }
        if let Some(session_volume_id) = parse_tool_runs_uri(uri)? {
            let binding_name = self.config.bindings.sandbox.as_deref().ok_or_else(|| {
                McpAdapterError::UnknownResource {
                    uri: uri.to_string(),
                }
            })?;
            self.ensure_requested_resource(&record, MCP_RESOURCE_SANDBOX_TOOL_RUNS)?;
            let decision = self.authorize_resource(
                &record,
                uri,
                binding_name,
                vec![ResourceTarget {
                    kind: ResourceKind::Session,
                    identifier: session_volume_id.to_string(),
                }],
                true,
            )?;
            let runs = self.load_tool_runs(session_volume_id, None).await?;
            let value =
                serde_json::to_value(runs).map_err(|error| McpAdapterError::Serialization {
                    message: error.to_string(),
                })?;
            return Ok(McpResourceReadResult {
                resource_name: MCP_RESOURCE_SANDBOX_TOOL_RUNS.to_string(),
                decision,
                contents: McpResourceContents::Json {
                    mime_type: "application/json".to_string(),
                    value,
                },
            });
        }
        if uri.starts_with(&format!("{READONLY_VIEW_URI_SCHEME}://")) {
            let binding_name = self.config.bindings.sandbox.as_deref().ok_or_else(|| {
                McpAdapterError::UnknownResource {
                    uri: uri.to_string(),
                }
            })?;
            self.ensure_requested_resource(&record, MCP_RESOURCE_SANDBOX_VIEW)?;
            let location =
                ReadonlyViewLocation::parse(uri).map_err(|error| McpAdapterError::Sandbox {
                    message: error.to_string(),
                })?;
            let decision = self.authorize_resource(
                &record,
                uri,
                binding_name,
                vec![ResourceTarget {
                    kind: ResourceKind::Session,
                    identifier: location.session_volume_id.to_string(),
                }],
                true,
            )?;
            let stat = self.readonly_views.stat(&location).await.map_err(|error| {
                McpAdapterError::Sandbox {
                    message: error.to_string(),
                }
            })?;
            let contents = match stat {
                None => McpResourceContents::Missing,
                Some(stat) if stat.kind == ReadonlyViewNodeKind::Directory => {
                    let entries =
                        self.readonly_views
                            .read_dir(&location)
                            .await
                            .map_err(|error| McpAdapterError::Sandbox {
                                message: error.to_string(),
                            })?;
                    McpResourceContents::Json {
                        mime_type: "application/json".to_string(),
                        value: serde_json::to_value(entries).map_err(|error| {
                            McpAdapterError::Serialization {
                                message: error.to_string(),
                            }
                        })?,
                    }
                }
                Some(_) => {
                    match self
                        .readonly_views
                        .read_file(&location)
                        .await
                        .map_err(|error| McpAdapterError::Sandbox {
                            message: error.to_string(),
                        })? {
                        Some(bytes) => McpResourceContents::Bytes {
                            mime_type: "application/octet-stream".to_string(),
                            bytes,
                        },
                        None => McpResourceContents::Missing,
                    }
                }
            };
            return Ok(McpResourceReadResult {
                resource_name: MCP_RESOURCE_SANDBOX_VIEW.to_string(),
                decision,
                contents,
            });
        }
        Err(McpAdapterError::UnknownResource {
            uri: uri.to_string(),
        })
    }

    async fn require_usable_session(
        &self,
        stream_id: &str,
    ) -> Result<McpSessionRecord, McpAdapterError> {
        let record = self.load_session(stream_id).await?;
        match record.state {
            McpSseConnectionState::Closing
            | McpSseConnectionState::Closed
            | McpSseConnectionState::Failed => Err(McpAdapterError::SessionUnavailable {
                stream_id: stream_id.to_string(),
                state: record.state,
            }),
            _ => Ok(record),
        }
    }

    fn require_policy<'a>(
        &self,
        record: &'a McpSessionRecord,
    ) -> Result<&'a ResolvedSessionPolicy, McpAdapterError> {
        record
            .policy
            .as_ref()
            .ok_or_else(|| McpAdapterError::SessionPolicyMissing {
                stream_id: record.context.stream_id.clone(),
            })
    }

    fn require_binding<'a>(
        &self,
        record: &'a McpSessionRecord,
        binding_name: &str,
    ) -> Result<&'a ManifestBinding, McpAdapterError> {
        self.require_policy(record)?
            .manifest
            .bindings
            .iter()
            .find(|binding| binding.binding_name == binding_name)
            .ok_or_else(|| McpAdapterError::ToolForbidden {
                tool_name: binding_name.to_string(),
                message: "binding is not present in the resolved MCP manifest".to_string(),
                policy: None,
            })
    }

    fn surface_tool_descriptor(
        &self,
        record: &McpSessionRecord,
        surface: ToolSurface,
    ) -> Option<McpToolDescriptor> {
        if surface.requires_trusted_draft && !record.context.auth.trusted_draft {
            return None;
        }
        if !record.context.requested_tools.is_empty()
            && !record
                .context
                .requested_tools
                .contains(&surface.descriptor.name)
        {
            return None;
        }
        let binding_name = surface.descriptor.capability_binding.as_ref()?;
        let binding = self.require_binding(record, binding_name).ok()?;
        let decision = self.evaluate_surface(
            record,
            binding_name,
            vec![ResourceTarget {
                kind: ResourceKind::McpTool,
                identifier: surface.descriptor.name.clone(),
            }],
        );
        if !self.decision_allowed(&decision) {
            return None;
        }
        let mut descriptor = surface.descriptor;
        descriptor.resource_policy = Some(binding.resource_policy.clone());
        descriptor.budget_policy = Some(binding.budget_policy.clone());
        Some(descriptor)
    }

    fn surface_resource_descriptor(
        &self,
        record: &McpSessionRecord,
        surface: ResourceSurface,
    ) -> Option<McpResourceDescriptor> {
        if surface.requires_trusted_draft && !record.context.auth.trusted_draft {
            return None;
        }
        if !record.context.requested_resources.is_empty()
            && !record
                .context
                .requested_resources
                .contains(&surface.descriptor.name)
        {
            return None;
        }
        let binding_name = surface.descriptor.capability_binding.as_ref()?;
        let _binding = self.require_binding(record, binding_name).ok()?;
        let decision = self.evaluate_surface(
            record,
            binding_name,
            vec![ResourceTarget {
                kind: ResourceKind::McpResource,
                identifier: surface.descriptor.name.clone(),
            }],
        );
        self.decision_allowed(&decision)
            .then_some(surface.descriptor)
    }

    fn ensure_requested_tool(
        &self,
        record: &McpSessionRecord,
        tool_name: &str,
    ) -> Result<(), McpAdapterError> {
        if record.context.requested_tools.is_empty()
            || record
                .context
                .requested_tools
                .iter()
                .any(|name| name == tool_name)
        {
            Ok(())
        } else {
            Err(McpAdapterError::UnknownTool {
                tool_name: tool_name.to_string(),
            })
        }
    }

    fn ensure_requested_resource(
        &self,
        record: &McpSessionRecord,
        resource_name: &str,
    ) -> Result<(), McpAdapterError> {
        if record.context.requested_resources.is_empty()
            || record
                .context
                .requested_resources
                .iter()
                .any(|name| name == resource_name)
        {
            Ok(())
        } else {
            Err(McpAdapterError::UnknownResource {
                uri: resource_name.to_string(),
            })
        }
    }

    fn authorize_tool(
        &self,
        record: &McpSessionRecord,
        tool_name: &str,
        binding_name: &str,
        extra_targets: Vec<ResourceTarget>,
        requires_trusted_draft: bool,
    ) -> Result<PolicyDecisionRecord, McpAdapterError> {
        if requires_trusted_draft && !record.context.auth.trusted_draft {
            return Err(McpAdapterError::DraftTrustRequired {
                surface: tool_name.to_string(),
            });
        }
        let decision = self.evaluate_surface(
            record,
            binding_name,
            surface_targets(ResourceKind::McpTool, tool_name.to_string(), extra_targets),
        );
        if self.decision_allowed(&decision) {
            Ok(decision)
        } else {
            Err(McpAdapterError::ToolForbidden {
                tool_name: tool_name.to_string(),
                message: decision
                    .outcome
                    .message
                    .clone()
                    .unwrap_or_else(|| "mcp tool denied by capability policy".to_string()),
                policy: Some(decision),
            })
        }
    }

    fn authorize_resource(
        &self,
        record: &McpSessionRecord,
        uri: &str,
        binding_name: &str,
        extra_targets: Vec<ResourceTarget>,
        requires_trusted_draft: bool,
    ) -> Result<PolicyDecisionRecord, McpAdapterError> {
        if requires_trusted_draft && !record.context.auth.trusted_draft {
            return Err(McpAdapterError::DraftTrustRequired {
                surface: uri.to_string(),
            });
        }
        let decision = self.evaluate_surface(
            record,
            binding_name,
            surface_targets(ResourceKind::McpResource, uri.to_string(), extra_targets),
        );
        if self.decision_allowed(&decision) {
            Ok(decision)
        } else {
            Err(McpAdapterError::ResourceForbidden {
                uri: uri.to_string(),
                message: decision
                    .outcome
                    .message
                    .clone()
                    .unwrap_or_else(|| "mcp resource denied by capability policy".to_string()),
                policy: Some(decision),
            })
        }
    }

    fn evaluate_surface(
        &self,
        record: &McpSessionRecord,
        binding_name: &str,
        targets: Vec<ResourceTarget>,
    ) -> PolicyDecisionRecord {
        let policy = self
            .require_policy(record)
            .expect("mcp session policy should exist");
        policy.evaluate_use(&CapabilityUseRequest {
            session_id: record.context.session_id.clone(),
            operation: ExecutionOperation::McpRequest,
            binding_name: binding_name.to_string(),
            capability_family: None,
            targets,
            metrics: CapabilityUseMetrics::default(),
            requested_at: self.clock.now(),
            metadata: BTreeMap::from([
                (
                    "mcp_stream_id".to_string(),
                    JsonValue::String(record.context.stream_id.clone()),
                ),
                (
                    "mcp_session_mode".to_string(),
                    JsonValue::String("mcp".to_string()),
                ),
            ]),
        })
    }

    fn decision_allowed(&self, decision: &PolicyDecisionRecord) -> bool {
        decision.outcome.outcome == PolicyOutcomeKind::Allowed
    }

    fn require_procedure_route(
        &self,
        publication: &ProcedureVersionRef,
    ) -> Result<&McpPublishedProcedureRoute, McpAdapterError> {
        self.config
            .procedures
            .iter()
            .find(|route| route.publication == *publication)
            .ok_or_else(|| McpAdapterError::UnknownProcedure {
                procedure_id: publication.procedure_id.clone(),
                version: publication.version,
            })
    }

    async fn procedure_detail(
        &self,
        publication: &ProcedureVersionRef,
    ) -> Result<McpProcedureDetail, McpAdapterError> {
        let route = self.require_procedure_route(publication)?;
        let store = self.procedure_store.lock().await;
        let publication_record =
            store
                .load(publication)
                .ok_or_else(|| McpAdapterError::UnknownProcedure {
                    procedure_id: publication.procedure_id.clone(),
                    version: publication.version,
                })?;
        Ok(McpProcedureDetail {
            publication: publication_record,
            deployment: route.deployment.clone(),
            audits: store.audits_for_publication(publication),
            invocations: store.invocations_for_publication(publication),
        })
    }

    async fn load_tool_runs(
        &self,
        session_volume_id: VolumeId,
        limit: Option<usize>,
    ) -> Result<Vec<McpToolRunRecord>, McpAdapterError> {
        let session = self
            .sandbox_sessions
            .get_session(session_volume_id)
            .await
            .map_err(|error| McpAdapterError::ToolRuns {
                message: error.to_string(),
            })?
            .ok_or_else(|| McpAdapterError::ToolRuns {
                message: format!("sandbox session {session_volume_id} not found"),
            })?;
        let runs = session
            .volume()
            .tools()
            .recent(limit)
            .await
            .map_err(|error| McpAdapterError::ToolRuns {
                message: error.to_string(),
            })?;
        Ok(runs.into_iter().map(McpToolRunRecord::from).collect())
    }

    fn tool_surfaces(&self) -> Vec<ToolSurface> {
        let mut tools = Vec::new();
        if let Some(binding_name) = self.config.bindings.procedures.as_ref()
            && !self.config.procedures.is_empty()
        {
            tools.push(ToolSurface::new(
                procedure_tool_descriptor(
                    MCP_TOOL_PROCEDURES_LIST,
                    "List reviewed procedures exposed through the MCP adapter",
                    binding_name,
                    json!({ "type": "object", "properties": {} }),
                    json!({ "type": "object" }),
                ),
                false,
            ));
            tools.push(ToolSurface::new(
                procedure_tool_descriptor(
                    MCP_TOOL_PROCEDURES_INSPECT,
                    "Inspect reviewed procedure metadata, deployment wiring, and audit history",
                    binding_name,
                    json!({
                        "type": "object",
                        "required": ["publication"],
                        "properties": {
                            "publication": { "type": "object" }
                        }
                    }),
                    json!({ "type": "object" }),
                ),
                false,
            ));
            tools.push(ToolSurface::new(
                procedure_tool_descriptor(
                    MCP_TOOL_PROCEDURES_INVOKE,
                    "Invoke an immutable reviewed procedure through the published runtime",
                    binding_name,
                    json!({
                        "type": "object",
                        "required": ["publication", "arguments"],
                        "properties": {
                            "publication": { "type": "object" },
                            "arguments": {}
                        }
                    }),
                    json!({ "type": "object" }),
                ),
                false,
            ));
            tools.push(ToolSurface::new(
                procedure_tool_descriptor(
                    MCP_TOOL_PROCEDURES_AUDITS,
                    "List audit records for an exposed reviewed procedure",
                    binding_name,
                    json!({
                        "type": "object",
                        "required": ["publication"],
                        "properties": {
                            "publication": { "type": "object" }
                        }
                    }),
                    json!({ "type": "object" }),
                ),
                false,
            ));
        }
        if let Some(binding_name) = self.config.bindings.sandbox.as_ref() {
            tools.push(ToolSurface::new(
                sandbox_tool_descriptor(
                    MCP_TOOL_SANDBOX_SESSIONS_LIST,
                    "List read-only sandbox sessions that MCP may inspect",
                    binding_name,
                    json!({ "type": "object", "properties": {} }),
                ),
                true,
            ));
            tools.push(ToolSurface::new(
                sandbox_tool_descriptor(
                    MCP_TOOL_SANDBOX_FILE_DIFF,
                    "Compare visible and durable sandbox file contents through the readonly view protocol",
                    binding_name,
                    json!({
                        "type": "object",
                        "required": ["session_volume_id", "path"],
                        "properties": {
                            "session_volume_id": { "type": "string" },
                            "path": { "type": "string" }
                        }
                    }),
                ),
                true,
            ));
            tools.push(ToolSurface::new(
                sandbox_tool_descriptor(
                    MCP_TOOL_SANDBOX_TOOL_RUNS_LIST,
                    "Inspect sandbox tool-run history for a session",
                    binding_name,
                    json!({
                        "type": "object",
                        "required": ["session_volume_id"],
                        "properties": {
                            "session_volume_id": { "type": "string" },
                            "limit": { "type": "integer" }
                        }
                    }),
                ),
                true,
            ));
        }
        if let Some(binding_name) = self.config.bindings.draft_queries.as_ref()
            && self.draft_queries.is_some()
        {
            tools.push(ToolSurface::new(
                draft_query_tool_descriptor(binding_name),
                true,
            ));
        }
        tools
    }

    fn resource_surfaces(&self) -> Vec<ResourceSurface> {
        let mut resources = Vec::new();
        if let Some(binding_name) = self.config.bindings.procedures.as_ref()
            && !self.config.procedures.is_empty()
        {
            resources.push(ResourceSurface::new(
                McpResourceDescriptor {
                    name: MCP_RESOURCE_PROCEDURE_METADATA.to_string(),
                    uri_template: format!(
                        "{MCP_PROCEDURE_URI_SCHEME}://{{procedure_id}}/{{version}}"
                    ),
                    description: Some(
                        "Metadata for an exposed reviewed procedure publication".to_string(),
                    ),
                    mime_type: Some("application/json".to_string()),
                    capability_binding: Some(binding_name.clone()),
                    metadata: BTreeMap::new(),
                },
                false,
            ));
            resources.push(ResourceSurface::new(
                McpResourceDescriptor {
                    name: MCP_RESOURCE_PROCEDURE_AUDITS.to_string(),
                    uri_template: format!(
                        "{MCP_PROCEDURE_AUDIT_URI_SCHEME}://{{procedure_id}}/{{version}}"
                    ),
                    description: Some(
                        "Audit history for an exposed reviewed procedure publication".to_string(),
                    ),
                    mime_type: Some("application/json".to_string()),
                    capability_binding: Some(binding_name.clone()),
                    metadata: BTreeMap::new(),
                },
                false,
            ));
        }
        if let Some(binding_name) = self.config.bindings.sandbox.as_ref() {
            resources.push(ResourceSurface::new(
                McpResourceDescriptor {
                    name: MCP_RESOURCE_SANDBOX_VIEW.to_string(),
                    uri_template: format!(
                        "{READONLY_VIEW_URI_SCHEME}://session/{{session_volume_id}}/{{cut}}/{{path}}"
                    ),
                    description: Some(
                        "Read-only sandbox file or directory content over the shared view protocol"
                            .to_string(),
                    ),
                    mime_type: None,
                    capability_binding: Some(binding_name.clone()),
                    metadata: BTreeMap::new(),
                },
                true,
            ));
            resources.push(ResourceSurface::new(
                McpResourceDescriptor {
                    name: MCP_RESOURCE_SANDBOX_TOOL_RUNS.to_string(),
                    uri_template: format!(
                        "{MCP_TOOL_RUNS_URI_SCHEME}://session/{{session_volume_id}}"
                    ),
                    description: Some("Sandbox tool-run history for a session".to_string()),
                    mime_type: Some("application/json".to_string()),
                    capability_binding: Some(binding_name.clone()),
                    metadata: BTreeMap::new(),
                },
                true,
            ));
        }
        resources
    }
}

#[derive(Clone)]
pub struct LocalMcpTransport<V>
where
    V: ReadonlyViewProtocolTransport + 'static,
{
    service: Arc<McpAdapterService<V>>,
}

impl<V> LocalMcpTransport<V>
where
    V: ReadonlyViewProtocolTransport + 'static,
{
    pub fn new(service: Arc<McpAdapterService<V>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<V> McpProtocolTransport for LocalMcpTransport<V>
where
    V: ReadonlyViewProtocolTransport + 'static,
{
    async fn send(
        &self,
        request: McpProtocolRequest,
    ) -> Result<McpProtocolResponse, McpAdapterError> {
        self.service.handle_protocol(request).await
    }
}

pub struct McpClient<T> {
    transport: T,
}

impl<T> McpClient<T>
where
    T: McpProtocolTransport,
{
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub async fn open_session(
        &self,
        request: McpOpenSessionRequest,
    ) -> Result<McpSessionRecord, McpAdapterError> {
        match self
            .transport
            .send(McpProtocolRequest::OpenSession { request })
            .await?
        {
            McpProtocolResponse::Session { record } => Ok(record),
            response => Err(unexpected_protocol_response("session", &response)),
        }
    }

    pub async fn load_session(
        &self,
        stream_id: impl Into<String>,
    ) -> Result<McpSessionRecord, McpAdapterError> {
        match self
            .transport
            .send(McpProtocolRequest::LoadSession {
                stream_id: stream_id.into(),
            })
            .await?
        {
            McpProtocolResponse::Session { record } => Ok(record),
            response => Err(unexpected_protocol_response("session", &response)),
        }
    }

    pub async fn list_tools(
        &self,
        stream_id: impl Into<String>,
    ) -> Result<Vec<McpToolDescriptor>, McpAdapterError> {
        match self
            .transport
            .send(McpProtocolRequest::ListTools {
                stream_id: stream_id.into(),
            })
            .await?
        {
            McpProtocolResponse::Tools { tools } => Ok(tools),
            response => Err(unexpected_protocol_response("tools", &response)),
        }
    }

    pub async fn list_resources(
        &self,
        stream_id: impl Into<String>,
    ) -> Result<Vec<McpResourceDescriptor>, McpAdapterError> {
        match self
            .transport
            .send(McpProtocolRequest::ListResources {
                stream_id: stream_id.into(),
            })
            .await?
        {
            McpProtocolResponse::Resources { resources } => Ok(resources),
            response => Err(unexpected_protocol_response("resources", &response)),
        }
    }

    pub async fn call_tool(
        &self,
        stream_id: impl Into<String>,
        call: McpToolCall,
    ) -> Result<McpToolCallOutput, McpAdapterError> {
        match self
            .transport
            .send(McpProtocolRequest::CallTool {
                stream_id: stream_id.into(),
                call,
            })
            .await?
        {
            McpProtocolResponse::ToolResult { result } => Ok(result),
            response => Err(unexpected_protocol_response("tool result", &response)),
        }
    }

    pub async fn read_resource(
        &self,
        stream_id: impl Into<String>,
        uri: impl Into<String>,
    ) -> Result<McpResourceReadResult, McpAdapterError> {
        match self
            .transport
            .send(McpProtocolRequest::ReadResource {
                stream_id: stream_id.into(),
                uri: uri.into(),
            })
            .await?
        {
            McpProtocolResponse::Resource { resource } => Ok(resource),
            response => Err(unexpected_protocol_response("resource result", &response)),
        }
    }
}

#[derive(Clone)]
struct ToolSurface {
    descriptor: McpToolDescriptor,
    requires_trusted_draft: bool,
}

impl ToolSurface {
    fn new(descriptor: McpToolDescriptor, requires_trusted_draft: bool) -> Self {
        Self {
            descriptor,
            requires_trusted_draft,
        }
    }
}

#[derive(Clone)]
struct ResourceSurface {
    descriptor: McpResourceDescriptor,
    requires_trusted_draft: bool,
}

impl ResourceSurface {
    fn new(descriptor: McpResourceDescriptor, requires_trusted_draft: bool) -> Self {
        Self {
            descriptor,
            requires_trusted_draft,
        }
    }
}

fn procedure_tool_descriptor(
    name: &str,
    description: &str,
    binding_name: &str,
    input_schema: JsonValue,
    output_schema: JsonValue,
) -> McpToolDescriptor {
    McpToolDescriptor {
        name: name.to_string(),
        description: Some(description.to_string()),
        capability_binding: Some(binding_name.to_string()),
        resource_policy: None,
        budget_policy: None,
        input_schema,
        output_schema: Some(output_schema),
        metadata: BTreeMap::from([(
            "surface".to_string(),
            JsonValue::String("reviewed_procedure".to_string()),
        )]),
    }
}

fn sandbox_tool_descriptor(
    name: &str,
    description: &str,
    binding_name: &str,
    input_schema: JsonValue,
) -> McpToolDescriptor {
    McpToolDescriptor {
        name: name.to_string(),
        description: Some(description.to_string()),
        capability_binding: Some(binding_name.to_string()),
        resource_policy: None,
        budget_policy: None,
        input_schema,
        output_schema: Some(json!({ "type": "object" })),
        metadata: BTreeMap::from([(
            "surface".to_string(),
            JsonValue::String("sandbox".to_string()),
        )]),
    }
}

fn draft_query_tool_descriptor(binding_name: &str) -> McpToolDescriptor {
    McpToolDescriptor {
        name: MCP_TOOL_DRAFT_QUERY_RUN.to_string(),
        description: Some(
            "Run a draft query only for trusted MCP subjects with an explicit draft binding"
                .to_string(),
        ),
        capability_binding: Some(binding_name.to_string()),
        resource_policy: None,
        budget_policy: None,
        input_schema: json!({
            "type": "object",
            "required": ["request"],
            "properties": {
                "request": { "type": "object" }
            }
        }),
        output_schema: Some(json!({ "type": "object" })),
        metadata: BTreeMap::from([(
            "surface".to_string(),
            JsonValue::String("draft_query".to_string()),
        )]),
    }
}

fn session_audit_metadata(
    context: &McpSessionContext,
    preset_name: Option<String>,
    profile_name: Option<String>,
) -> BTreeMap<String, JsonValue> {
    let mut metadata = BTreeMap::from([
        (
            "events_url".to_string(),
            JsonValue::String(context.endpoint.events_url.clone()),
        ),
        (
            "trusted_draft".to_string(),
            JsonValue::Bool(context.auth.trusted_draft),
        ),
        (
            "requested_tools".to_string(),
            JsonValue::Array(
                context
                    .requested_tools
                    .iter()
                    .cloned()
                    .map(JsonValue::String)
                    .collect(),
            ),
        ),
        (
            "requested_resources".to_string(),
            JsonValue::Array(
                context
                    .requested_resources
                    .iter()
                    .cloned()
                    .map(JsonValue::String)
                    .collect(),
            ),
        ),
    ]);
    if let Some(authentication_kind) = context.auth.authentication_kind.clone() {
        metadata.insert(
            "authentication_kind".to_string(),
            JsonValue::String(authentication_kind),
        );
    }
    if let Some(preset_name) = preset_name {
        metadata.insert("preset_name".to_string(), JsonValue::String(preset_name));
    }
    if let Some(profile_name) = profile_name {
        metadata.insert("profile_name".to_string(), JsonValue::String(profile_name));
    }
    for (key, value) in &context.auth.metadata {
        metadata.insert(format!("auth.{key}"), value.clone());
    }
    metadata.extend(context.metadata.clone());
    metadata
}

fn surface_targets(
    primary_kind: ResourceKind,
    primary_identifier: String,
    mut extra: Vec<ResourceTarget>,
) -> Vec<ResourceTarget> {
    let mut targets = vec![ResourceTarget {
        kind: primary_kind,
        identifier: primary_identifier,
    }];
    targets.append(&mut extra);
    targets
}

fn parse_procedure_uri(
    scheme: &str,
    uri: &str,
) -> Result<Option<ProcedureVersionRef>, McpAdapterError> {
    let prefix = format!("{scheme}://");
    let Some(rest) = uri.strip_prefix(&prefix) else {
        return Ok(None);
    };
    let mut parts = rest.splitn(2, '/');
    let procedure_id = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or_default();
    let version = version
        .parse::<u64>()
        .map_err(|error| McpAdapterError::Serialization {
            message: error.to_string(),
        })?;
    Ok(Some(ProcedureVersionRef {
        procedure_id: procedure_id.to_string(),
        version,
    }))
}

fn parse_tool_runs_uri(uri: &str) -> Result<Option<VolumeId>, McpAdapterError> {
    let prefix = format!("{MCP_TOOL_RUNS_URI_SCHEME}://session/");
    let Some(rest) = uri.strip_prefix(&prefix) else {
        return Ok(None);
    };
    let raw = u128::from_str_radix(rest, 16).map_err(|error| McpAdapterError::Serialization {
        message: error.to_string(),
    })?;
    Ok(Some(VolumeId::new(raw)))
}

fn unexpected_protocol_response(expected: &str, response: &McpProtocolResponse) -> McpAdapterError {
    McpAdapterError::Serialization {
        message: format!("expected {expected} response, got {response:?}"),
    }
}
