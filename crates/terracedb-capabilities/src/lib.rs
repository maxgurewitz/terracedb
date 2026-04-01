//! Frozen policy and capability contracts shared by sandbox, migrations, procedures, and MCP.
//!
//! The key rule in this crate is intentionally simple:
//!
//! - capability policy constrains authority,
//! - execution-domain policy constrains placement and resource consumption.
//!
//! The two layers compose, but neither replaces the other.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::Timestamp;
use thiserror::Error;

pub const HOST_CAPABILITY_PREFIX: &str = "terrace:host/";
pub const JUST_BASH_CAPABILITY_COMMAND: &str = "terrace-call";

pub fn capability_module_specifier(binding_name: &str) -> String {
    format!("{HOST_CAPABILITY_PREFIX}{binding_name}")
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySubject {
    pub subject_id: String,
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SubjectSelector {
    Exact { subject_id: String },
    Group { group: String },
    Tenant { tenant_id: String },
    AnyAuthenticated,
    Any,
}

impl SubjectSelector {
    pub fn matches(&self, subject: &PolicySubject) -> bool {
        match self {
            Self::Exact { subject_id } => subject.subject_id == *subject_id,
            Self::Group { group } => subject.groups.iter().any(|value| value == group),
            Self::Tenant { tenant_id } => subject.tenant_id.as_deref() == Some(tenant_id.as_str()),
            Self::AnyAuthenticated => !subject.subject_id.is_empty(),
            Self::Any => true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Database,
    Table,
    Procedure,
    Migration,
    McpTool,
    McpResource,
    Session,
    Custom,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSelector {
    pub kind: ResourceKind,
    pub pattern: String,
}

impl ResourceSelector {
    pub fn matches_target(&self, target: &ResourceTarget) -> bool {
        self.kind == target.kind && wildcard_pattern_matches(&self.pattern, &target.identifier)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourcePolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<ResourceSelector>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deny: Vec<ResourceSelector>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tenant_scopes: Vec<String>,
    pub row_scope_binding: Option<String>,
    pub visibility_index: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetPolicy {
    pub max_calls: Option<u64>,
    pub max_scanned_rows: Option<u64>,
    pub max_returned_rows: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_millis: Option<u64>,
    pub rate_limit_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShellCommandDescriptor {
    pub command_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub argv: Vec<String>,
    pub description: Option<String>,
}

impl ShellCommandDescriptor {
    pub fn for_binding(binding_name: impl Into<String>) -> Self {
        let binding_name = binding_name.into();
        Self {
            command_name: JUST_BASH_CAPABILITY_COMMAND.to_string(),
            argv: vec![binding_name.clone()],
            description: Some(format!("Invoke the {binding_name} host binding")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityTemplate {
    pub template_id: String,
    pub capability_family: String,
    pub default_binding: String,
    pub description: Option<String>,
    pub default_resource_policy: ResourcePolicy,
    pub default_budget_policy: BudgetPolicy,
    pub expose_in_just_bash: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityGrant {
    pub grant_id: String,
    pub subject: SubjectSelector,
    pub template_id: String,
    pub binding_name: Option<String>,
    pub resource_policy: Option<ResourcePolicy>,
    pub budget_policy: Option<BudgetPolicy>,
    pub allow_interactive_widening: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManifestBinding {
    pub binding_name: String,
    pub capability_family: String,
    pub module_specifier: String,
    pub shell_command: Option<ShellCommandDescriptor>,
    pub resource_policy: ResourcePolicy,
    pub budget_policy: BudgetPolicy,
    pub source_template_id: String,
    pub source_grant_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_interactive_widening: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    pub subject: Option<PolicySubject>,
    pub preset_name: Option<String>,
    pub profile_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bindings: Vec<ManifestBinding>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PresetBinding {
    pub template_id: String,
    pub binding_name: Option<String>,
    pub resource_policy: Option<ResourcePolicy>,
    pub budget_policy: Option<BudgetPolicy>,
    pub expose_in_just_bash: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityPresetDescriptor {
    pub name: String,
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bindings: Vec<PresetBinding>,
    pub default_session_mode: SessionMode,
    pub default_execution_policy: Option<ExecutionPolicy>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityProfileDescriptor {
    pub name: String,
    pub preset_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add_bindings: Vec<PresetBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drop_bindings: Vec<String>,
    pub execution_policy_override: Option<ExecutionPolicy>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMode {
    Draft,
    ReviewedProcedure,
    PublishedWorkflow,
    Mcp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DraftAuthorizationRequestKind {
    InjectMissingBinding,
    RetryDeniedOperation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationScope {
    OneCall,
    Session,
    PolicyUpdate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DraftAuthorizationOutcomeKind {
    Approved,
    Rejected,
    Pending,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DraftAuthorizationRequest {
    pub request_id: String,
    pub session_id: String,
    pub binding_name: String,
    pub capability_family: String,
    pub kind: DraftAuthorizationRequestKind,
    pub requested_scope: AuthorizationScope,
    pub reason: Option<String>,
    pub requested_at: Timestamp,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DraftAuthorizationDecision {
    pub request_id: String,
    pub outcome: DraftAuthorizationOutcomeKind,
    pub approved_scope: Option<AuthorizationScope>,
    pub decided_at: Timestamp,
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyOutcomeKind {
    Allowed,
    Denied,
    MissingBinding,
    RateLimited,
    BudgetExhausted,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolicyOutcomeRecord {
    pub binding_name: String,
    pub outcome: PolicyOutcomeKind,
    pub message: Option<String>,
    pub observed_at: Timestamp,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceTarget {
    pub kind: ResourceKind,
    pub identifier: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityUseMetrics {
    #[serde(default = "default_call_count")]
    pub call_count: u64,
    pub scanned_rows: Option<u64>,
    pub returned_rows: Option<u64>,
    pub bytes: Option<u64>,
    pub millis: Option<u64>,
}

impl Default for CapabilityUseMetrics {
    fn default() -> Self {
        Self {
            call_count: default_call_count(),
            scanned_rows: None,
            returned_rows: None,
            bytes: None,
            millis: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityUseRequest {
    pub session_id: String,
    pub operation: ExecutionOperation,
    pub binding_name: String,
    pub capability_family: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<ResourceTarget>,
    #[serde(default)]
    pub metrics: CapabilityUseMetrics,
    pub requested_at: Timestamp,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolicyAuditMetadata {
    pub session_id: String,
    pub session_mode: SessionMode,
    pub subject: PolicySubject,
    pub binding_name: String,
    pub capability_family: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_resources: Vec<ResourceTarget>,
    pub execution_domain: ExecutionDomain,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub placement_tags: Vec<String>,
    pub preset_name: Option<String>,
    pub profile_name: Option<String>,
    pub rate_limit: Option<RateLimitOutcome>,
    pub budget_hook: Option<BudgetAccountingHook>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolicyDecisionRecord {
    pub outcome: PolicyOutcomeRecord,
    pub audit: PolicyAuditMetadata,
}

impl PolicyDecisionRecord {
    pub fn draft_authorization_request(
        &self,
        request_id: impl Into<String>,
        requested_scope: AuthorizationScope,
        requested_at: Timestamp,
        reason: Option<String>,
    ) -> Option<DraftAuthorizationRequest> {
        if self.audit.session_mode != SessionMode::Draft {
            return None;
        }

        let kind = match self.outcome.outcome {
            PolicyOutcomeKind::MissingBinding => {
                DraftAuthorizationRequestKind::InjectMissingBinding
            }
            PolicyOutcomeKind::Denied => DraftAuthorizationRequestKind::RetryDeniedOperation,
            PolicyOutcomeKind::Allowed
            | PolicyOutcomeKind::RateLimited
            | PolicyOutcomeKind::BudgetExhausted => return None,
        };

        let capability_family = self.audit.capability_family.clone()?;
        let mut metadata = self.audit.metadata.clone();
        metadata.insert(
            "policy_outcome".to_string(),
            json_value(&self.outcome.outcome),
        );

        Some(DraftAuthorizationRequest {
            request_id: request_id.into(),
            session_id: self.audit.session_id.clone(),
            binding_name: self.audit.binding_name.clone(),
            capability_family,
            kind,
            requested_scope,
            reason: reason.or_else(|| self.outcome.message.clone()),
            requested_at,
            metadata,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionLifecycleState {
    Opening,
    Ready,
    WaitingForAuthorization,
    Busy,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatusSource {
    ToolRun,
    ActivityEntry,
    ViewState,
    PolicyOutcome,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ForegroundSessionStatusSnapshot {
    pub session_id: String,
    pub lifecycle: SessionLifecycleState,
    pub session_mode: SessionMode,
    pub subject: Option<PolicySubject>,
    pub manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    pub pending_authorization: Option<DraftAuthorizationRequest>,
    pub last_policy_outcome: Option<PolicyOutcomeRecord>,
    pub updated_at: Timestamp,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ForegroundSessionStatusRecord {
    pub session_id: String,
    pub revision: u64,
    pub snapshot: ForegroundSessionStatusSnapshot,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<SessionStatusSource>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SessionStatusUpdate {
    ToolRun {
        running: bool,
        updated_at: Timestamp,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, JsonValue>,
    },
    ActivityEntry {
        lifecycle: Option<SessionLifecycleState>,
        pending_authorization: Option<DraftAuthorizationRequest>,
        updated_at: Timestamp,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, JsonValue>,
    },
    ViewState {
        visible: bool,
        updated_at: Timestamp,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, JsonValue>,
    },
    PolicyOutcome {
        outcome: PolicyOutcomeRecord,
        updated_at: Timestamp,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, JsonValue>,
    },
    Closed {
        updated_at: Timestamp,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, JsonValue>,
    },
}

impl SessionStatusUpdate {
    fn source(&self) -> SessionStatusSource {
        match self {
            Self::ToolRun { .. } => SessionStatusSource::ToolRun,
            Self::ActivityEntry { .. } | Self::Closed { .. } => SessionStatusSource::ActivityEntry,
            Self::ViewState { .. } => SessionStatusSource::ViewState,
            Self::PolicyOutcome { .. } => SessionStatusSource::PolicyOutcome,
        }
    }

    fn updated_at(&self) -> Timestamp {
        match self {
            Self::ToolRun { updated_at, .. }
            | Self::ActivityEntry { updated_at, .. }
            | Self::ViewState { updated_at, .. }
            | Self::PolicyOutcome { updated_at, .. }
            | Self::Closed { updated_at, .. } => *updated_at,
        }
    }

    fn metadata(&self) -> &BTreeMap<String, JsonValue> {
        match self {
            Self::ToolRun { metadata, .. }
            | Self::ActivityEntry { metadata, .. }
            | Self::ViewState { metadata, .. }
            | Self::PolicyOutcome { metadata, .. }
            | Self::Closed { metadata, .. } => metadata,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ForegroundSessionStatusProjector {
    pub session_id: String,
    pub session_mode: SessionMode,
    pub subject: Option<PolicySubject>,
    pub manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl ForegroundSessionStatusProjector {
    pub fn new(
        session_id: impl Into<String>,
        session_mode: SessionMode,
        subject: Option<PolicySubject>,
        manifest: CapabilityManifest,
        execution_policy: ExecutionPolicy,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            session_mode,
            subject,
            manifest,
            execution_policy,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: JsonValue) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    pub fn project(
        &self,
        revision: u64,
        updates: &[SessionStatusUpdate],
    ) -> ForegroundSessionStatusRecord {
        let mut lifecycle = SessionLifecycleState::Opening;
        let mut pending_authorization = None;
        let mut last_policy_outcome = None;
        let mut updated_at = Timestamp::new(0);
        let mut metadata = self.metadata.clone();
        let mut sources = Vec::new();

        for update in updates {
            let source = update.source();
            if !sources.contains(&source) {
                sources.push(source);
            }
            metadata.extend(update.metadata().clone());
            updated_at = update.updated_at();

            match update {
                SessionStatusUpdate::ToolRun { running, .. } => {
                    if *running {
                        lifecycle = SessionLifecycleState::Busy;
                    } else if pending_authorization.is_some() {
                        lifecycle = SessionLifecycleState::WaitingForAuthorization;
                    } else if lifecycle != SessionLifecycleState::Closed {
                        lifecycle = SessionLifecycleState::Ready;
                    }
                }
                SessionStatusUpdate::ActivityEntry {
                    lifecycle: next_lifecycle,
                    pending_authorization: next_pending_authorization,
                    ..
                } => {
                    pending_authorization = next_pending_authorization.clone();
                    lifecycle = if let Some(next_lifecycle) = next_lifecycle {
                        *next_lifecycle
                    } else if pending_authorization.is_some() {
                        SessionLifecycleState::WaitingForAuthorization
                    } else if lifecycle == SessionLifecycleState::Closed {
                        SessionLifecycleState::Closed
                    } else {
                        SessionLifecycleState::Ready
                    };
                }
                SessionStatusUpdate::ViewState { visible, .. } => {
                    metadata.insert("view_visible".to_string(), JsonValue::Bool(*visible));
                    if *visible && lifecycle == SessionLifecycleState::Opening {
                        lifecycle = SessionLifecycleState::Ready;
                    }
                }
                SessionStatusUpdate::PolicyOutcome { outcome, .. } => {
                    last_policy_outcome = Some(outcome.clone());
                }
                SessionStatusUpdate::Closed { .. } => {
                    lifecycle = SessionLifecycleState::Closed;
                    pending_authorization = None;
                }
            }
        }

        ForegroundSessionStatusRecord {
            session_id: self.session_id.clone(),
            revision,
            snapshot: ForegroundSessionStatusSnapshot {
                session_id: self.session_id.clone(),
                lifecycle,
                session_mode: self.session_mode,
                subject: self.subject.clone(),
                manifest: self.manifest.clone(),
                execution_policy: self.execution_policy.clone(),
                pending_authorization,
                last_policy_outcome,
                updated_at,
                metadata,
            },
            sources,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOperation {
    DraftSession,
    PackageInstall,
    TypeCheck,
    BashHelper,
    MigrationPublication,
    MigrationApply,
    ProcedureInvocation,
    McpRequest,
}

impl ExecutionOperation {
    pub const ALL: [Self; 8] = [
        Self::DraftSession,
        Self::PackageInstall,
        Self::TypeCheck,
        Self::BashHelper,
        Self::MigrationPublication,
        Self::MigrationApply,
        Self::ProcedureInvocation,
        Self::McpRequest,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionDomain {
    OwnerForeground,
    SharedBackground,
    DedicatedSandbox,
    ProductionIsolate,
    RemoteWorker,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionDomainAssignment {
    pub domain: ExecutionDomain,
    pub budget: Option<BudgetPolicy>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub placement_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    pub default_assignment: ExecutionDomainAssignment,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub operations: BTreeMap<ExecutionOperation, ExecutionDomainAssignment>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl ExecutionPolicy {
    pub fn assignment_for(&self, operation: ExecutionOperation) -> &ExecutionDomainAssignment {
        self.operations
            .get(&operation)
            .unwrap_or(&self.default_assignment)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubjectResolutionRequest {
    pub session_id: String,
    pub auth_subject_hint: Option<String>,
    pub tenant_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicyRequest {
    pub session_mode: SessionMode,
    pub subject_id: String,
    pub preset_name: Option<String>,
    pub profile_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitRequest {
    pub subject_id: String,
    pub binding_name: String,
    pub bucket: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitOutcome {
    pub allowed: bool,
    pub bucket: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BudgetAccountingHook {
    pub hook_name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RateLimitEvaluation {
    pub binding_name: String,
    pub outcome: RateLimitOutcome,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedSessionPolicy {
    pub subject: PolicySubject,
    pub session_mode: SessionMode,
    pub manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rate_limits: Vec<RateLimitEvaluation>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub budget_hooks: BTreeMap<ExecutionOperation, BudgetAccountingHook>,
}

impl ResolvedSessionPolicy {
    pub fn evaluate_use(&self, request: &CapabilityUseRequest) -> PolicyDecisionRecord {
        let assignment = self
            .execution_policy
            .assignment_for(request.operation)
            .clone();
        let binding = self
            .manifest
            .bindings
            .iter()
            .find(|binding| binding.binding_name == request.binding_name);
        let rate_limit = self
            .rate_limits
            .iter()
            .find(|evaluation| evaluation.binding_name == request.binding_name)
            .map(|evaluation| evaluation.outcome.clone());
        let budget_hook = self
            .budget_hooks
            .get(&request.operation)
            .cloned()
            .or_else(|| {
                self.budget_hooks
                    .get(&ExecutionOperation::DraftSession)
                    .cloned()
            });

        let capability_family = binding
            .map(|binding| binding.capability_family.clone())
            .or_else(|| request.capability_family.clone());
        let audit = PolicyAuditMetadata {
            session_id: request.session_id.clone(),
            session_mode: self.session_mode,
            subject: self.subject.clone(),
            binding_name: request.binding_name.clone(),
            capability_family,
            target_resources: request.targets.clone(),
            execution_domain: assignment.domain,
            placement_tags: assignment.placement_tags.clone(),
            preset_name: self.manifest.preset_name.clone(),
            profile_name: self.manifest.profile_name.clone(),
            rate_limit: rate_limit.clone(),
            budget_hook,
            metadata: request.metadata.clone(),
        };

        let (outcome, message) = match binding {
            None => (
                PolicyOutcomeKind::MissingBinding,
                Some(format!(
                    "binding {} is not present in the resolved manifest",
                    request.binding_name
                )),
            ),
            Some(binding) => {
                if tenant_scope_denies(&self.subject, &binding.resource_policy) {
                    (
                        PolicyOutcomeKind::Denied,
                        Some(format!(
                            "subject {} is outside the binding tenant scopes",
                            self.subject.subject_id
                        )),
                    )
                } else if let Some(target) = denied_target(binding, &request.targets) {
                    (
                        PolicyOutcomeKind::Denied,
                        Some(format!(
                            "target {:?}:{} is outside the binding resource policy",
                            target.kind, target.identifier
                        )),
                    )
                } else if let Some(rate_limit) =
                    rate_limit.as_ref().filter(|rate_limit| !rate_limit.allowed)
                {
                    let message = rate_limit
                        .reason
                        .clone()
                        .unwrap_or_else(|| "rate limited by host policy".to_string());
                    (PolicyOutcomeKind::RateLimited, Some(message))
                } else if let Some(message) =
                    budget_exhausted_message(&binding.budget_policy, &request.metrics)
                {
                    (PolicyOutcomeKind::BudgetExhausted, Some(message))
                } else {
                    (PolicyOutcomeKind::Allowed, None)
                }
            }
        };

        PolicyDecisionRecord {
            outcome: PolicyOutcomeRecord {
                binding_name: request.binding_name.clone(),
                outcome,
                message,
                observed_at: request.requested_at,
                metadata: policy_outcome_metadata(
                    request,
                    &assignment,
                    audit.capability_family.as_deref(),
                    self.manifest.preset_name.as_deref(),
                    self.manifest.profile_name.as_deref(),
                ),
            },
            audit,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyResolutionRequest {
    pub subject: SubjectResolutionRequest,
    pub session_mode: SessionMode,
    pub preset_name: Option<String>,
    pub profile_name: Option<String>,
}

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("subject could not be resolved for session {session_id}")]
    SubjectNotFound { session_id: String },
    #[error("capability template {template_id} is not registered")]
    UnknownTemplate { template_id: String },
    #[error("capability preset {preset_name} is not registered")]
    UnknownPreset { preset_name: String },
    #[error("capability profile {profile_name} is not registered")]
    UnknownProfile { profile_name: String },
    #[error("preset {preset_name} references template {template_id} without a matching grant")]
    PresetBindingDenied {
        preset_name: String,
        template_id: String,
    },
    #[error("profile {profile_name} requires preset {preset_name}")]
    ProfilePresetMismatch {
        profile_name: String,
        preset_name: String,
    },
}

pub trait SubjectResolver {
    fn resolve_subject(
        &self,
        request: &SubjectResolutionRequest,
    ) -> Result<PolicySubject, PolicyError>;
}

pub trait RateLimiter {
    fn evaluate(&self, request: &RateLimitRequest) -> RateLimitOutcome;
}

pub trait BudgetAccountant {
    fn hook_for(
        &self,
        operation: ExecutionOperation,
        assignment: &ExecutionDomainAssignment,
    ) -> BudgetAccountingHook;
}

pub trait ExecutionPolicyResolver {
    fn resolve_execution_policy(
        &self,
        request: &ExecutionPolicyRequest,
    ) -> Result<ExecutionPolicy, PolicyError>;
}

pub trait DraftAuthorizationBroker {
    fn decide(&self, request: &DraftAuthorizationRequest) -> DraftAuthorizationDecision;
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicSubjectResolver {
    subjects_by_session: BTreeMap<String, PolicySubject>,
}

impl DeterministicSubjectResolver {
    pub fn with_session_subject(
        mut self,
        session_id: impl Into<String>,
        subject: PolicySubject,
    ) -> Self {
        self.subjects_by_session.insert(session_id.into(), subject);
        self
    }
}

impl SubjectResolver for DeterministicSubjectResolver {
    fn resolve_subject(
        &self,
        request: &SubjectResolutionRequest,
    ) -> Result<PolicySubject, PolicyError> {
        self.subjects_by_session
            .get(&request.session_id)
            .cloned()
            .ok_or_else(|| PolicyError::SubjectNotFound {
                session_id: request.session_id.clone(),
            })
    }
}

#[derive(Clone, Debug)]
pub struct DeterministicRateLimiter {
    overrides: BTreeMap<String, RateLimitOutcome>,
    default: RateLimitOutcome,
}

impl Default for DeterministicRateLimiter {
    fn default() -> Self {
        Self {
            overrides: BTreeMap::new(),
            default: RateLimitOutcome {
                allowed: true,
                bucket: None,
                reason: None,
            },
        }
    }
}

impl DeterministicRateLimiter {
    pub fn with_binding_outcome(
        mut self,
        binding_name: impl Into<String>,
        outcome: RateLimitOutcome,
    ) -> Self {
        self.overrides.insert(binding_name.into(), outcome);
        self
    }
}

impl RateLimiter for DeterministicRateLimiter {
    fn evaluate(&self, request: &RateLimitRequest) -> RateLimitOutcome {
        self.overrides
            .get(&request.binding_name)
            .cloned()
            .unwrap_or_else(|| self.default.clone())
    }
}

#[derive(Clone, Debug, Default)]
pub struct NoopBudgetAccountant;

impl BudgetAccountant for NoopBudgetAccountant {
    fn hook_for(
        &self,
        operation: ExecutionOperation,
        assignment: &ExecutionDomainAssignment,
    ) -> BudgetAccountingHook {
        BudgetAccountingHook {
            hook_name: format!("budget::{operation:?}"),
            labels: BTreeMap::from([(
                "domain".to_string(),
                format!("{:?}", assignment.domain).to_lowercase(),
            )]),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StaticExecutionPolicyResolver {
    default_policy: ExecutionPolicy,
    by_session_mode: BTreeMap<SessionMode, ExecutionPolicy>,
    by_preset_name: BTreeMap<String, ExecutionPolicy>,
    by_profile_name: BTreeMap<String, ExecutionPolicy>,
}

impl StaticExecutionPolicyResolver {
    pub fn new(default_policy: ExecutionPolicy) -> Self {
        Self {
            default_policy,
            by_session_mode: BTreeMap::new(),
            by_preset_name: BTreeMap::new(),
            by_profile_name: BTreeMap::new(),
        }
    }

    pub fn with_policy(mut self, mode: SessionMode, policy: ExecutionPolicy) -> Self {
        self.by_session_mode.insert(mode, policy);
        self
    }

    pub fn with_preset_policy(
        mut self,
        preset_name: impl Into<String>,
        policy: ExecutionPolicy,
    ) -> Self {
        self.by_preset_name.insert(preset_name.into(), policy);
        self
    }

    pub fn with_profile_policy(
        mut self,
        profile_name: impl Into<String>,
        policy: ExecutionPolicy,
    ) -> Self {
        self.by_profile_name.insert(profile_name.into(), policy);
        self
    }
}

impl ExecutionPolicyResolver for StaticExecutionPolicyResolver {
    fn resolve_execution_policy(
        &self,
        request: &ExecutionPolicyRequest,
    ) -> Result<ExecutionPolicy, PolicyError> {
        if let Some(profile_name) = request.profile_name.as_ref() {
            if let Some(policy) = self.by_profile_name.get(profile_name) {
                return Ok(policy.clone());
            }
        }
        if let Some(preset_name) = request.preset_name.as_ref() {
            if let Some(policy) = self.by_preset_name.get(preset_name) {
                return Ok(policy.clone());
            }
        }
        Ok(self
            .by_session_mode
            .get(&request.session_mode)
            .cloned()
            .unwrap_or_else(|| self.default_policy.clone()))
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicDraftAuthorizationBroker {
    decisions: BTreeMap<String, DraftAuthorizationDecision>,
}

impl DeterministicDraftAuthorizationBroker {
    pub fn with_decision(
        mut self,
        request_id: impl Into<String>,
        decision: DraftAuthorizationDecision,
    ) -> Self {
        self.decisions.insert(request_id.into(), decision);
        self
    }
}

impl DraftAuthorizationBroker for DeterministicDraftAuthorizationBroker {
    fn decide(&self, request: &DraftAuthorizationRequest) -> DraftAuthorizationDecision {
        self.decisions
            .get(&request.request_id)
            .cloned()
            .unwrap_or(DraftAuthorizationDecision {
                request_id: request.request_id.clone(),
                outcome: DraftAuthorizationOutcomeKind::Pending,
                approved_scope: None,
                decided_at: request.requested_at,
                note: None,
                metadata: BTreeMap::new(),
            })
    }
}

#[derive(Clone, Debug)]
pub struct DeterministicPolicyEngine {
    templates: BTreeMap<String, CapabilityTemplate>,
    grants: Vec<CapabilityGrant>,
    presets: BTreeMap<String, CapabilityPresetDescriptor>,
    profiles: BTreeMap<String, CapabilityProfileDescriptor>,
    subject_resolver: DeterministicSubjectResolver,
    execution_policy_resolver: StaticExecutionPolicyResolver,
    rate_limiter: DeterministicRateLimiter,
    budget_accountant: NoopBudgetAccountant,
}

impl DeterministicPolicyEngine {
    pub fn new(
        templates: Vec<CapabilityTemplate>,
        grants: Vec<CapabilityGrant>,
        subject_resolver: DeterministicSubjectResolver,
        execution_policy_resolver: StaticExecutionPolicyResolver,
    ) -> Self {
        let templates = templates
            .into_iter()
            .map(|template| (template.template_id.clone(), template))
            .collect();
        Self {
            templates,
            grants,
            presets: BTreeMap::new(),
            profiles: BTreeMap::new(),
            subject_resolver,
            execution_policy_resolver,
            rate_limiter: DeterministicRateLimiter::default(),
            budget_accountant: NoopBudgetAccountant,
        }
    }

    pub fn with_preset(mut self, preset: CapabilityPresetDescriptor) -> Self {
        self.presets.insert(preset.name.clone(), preset);
        self
    }

    pub fn with_profile(mut self, profile: CapabilityProfileDescriptor) -> Self {
        self.profiles.insert(profile.name.clone(), profile);
        self
    }

    pub fn with_rate_limiter(mut self, rate_limiter: DeterministicRateLimiter) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    pub fn resolve(
        &self,
        request: &PolicyResolutionRequest,
    ) -> Result<ResolvedSessionPolicy, PolicyError> {
        let subject = self.subject_resolver.resolve_subject(&request.subject)?;
        let grants = self.matching_grants(&subject);

        let manifest = self.resolve_manifest(&subject, &grants, request)?;
        let execution_policy =
            self.execution_policy_resolver
                .resolve_execution_policy(&ExecutionPolicyRequest {
                    session_mode: request.session_mode,
                    subject_id: subject.subject_id.clone(),
                    preset_name: request.preset_name.clone(),
                    profile_name: request.profile_name.clone(),
                })?;

        let rate_limits = manifest
            .bindings
            .iter()
            .map(|binding| RateLimitEvaluation {
                binding_name: binding.binding_name.clone(),
                outcome: self.rate_limiter.evaluate(&RateLimitRequest {
                    subject_id: subject.subject_id.clone(),
                    binding_name: binding.binding_name.clone(),
                    bucket: binding.budget_policy.rate_limit_bucket.clone(),
                }),
            })
            .collect();

        let mut budget_hooks = BTreeMap::new();
        for operation in ExecutionOperation::ALL {
            budget_hooks.insert(
                operation,
                self.budget_accountant
                    .hook_for(operation, execution_policy.assignment_for(operation)),
            );
        }

        Ok(ResolvedSessionPolicy {
            subject,
            session_mode: request.session_mode,
            manifest,
            execution_policy,
            rate_limits,
            budget_hooks,
        })
    }

    fn matching_grants<'a>(&'a self, subject: &PolicySubject) -> Vec<&'a CapabilityGrant> {
        self.grants
            .iter()
            .filter(|grant| grant.subject.matches(subject))
            .collect()
    }

    fn resolve_manifest(
        &self,
        subject: &PolicySubject,
        grants: &[&CapabilityGrant],
        request: &PolicyResolutionRequest,
    ) -> Result<CapabilityManifest, PolicyError> {
        let mut bindings = if let Some(preset_name) = &request.preset_name {
            let preset =
                self.presets
                    .get(preset_name)
                    .ok_or_else(|| PolicyError::UnknownPreset {
                        preset_name: preset_name.clone(),
                    })?;
            self.bindings_from_preset(preset_name, preset, grants)?
        } else {
            grants
                .iter()
                .map(|grant| self.binding_from_grant(grant, None))
                .collect::<Result<Vec<_>, _>>()?
        };

        if let Some(profile_name) = &request.profile_name {
            let profile =
                self.profiles
                    .get(profile_name)
                    .ok_or_else(|| PolicyError::UnknownProfile {
                        profile_name: profile_name.clone(),
                    })?;
            if request.preset_name.as_deref() != Some(profile.preset_name.as_str()) {
                return Err(PolicyError::ProfilePresetMismatch {
                    profile_name: profile_name.clone(),
                    preset_name: profile.preset_name.clone(),
                });
            }
            bindings.retain(|binding| !profile.drop_bindings.contains(&binding.binding_name));
            for add_binding in &profile.add_bindings {
                let grant =
                    find_matching_grant(grants, &add_binding.template_id).ok_or_else(|| {
                        PolicyError::PresetBindingDenied {
                            preset_name: profile.preset_name.clone(),
                            template_id: add_binding.template_id.clone(),
                        }
                    })?;
                bindings.push(self.binding_from_requested_binding(
                    grant,
                    add_binding,
                    Some(profile.name.as_str()),
                )?);
            }
        }

        Ok(CapabilityManifest {
            subject: Some(subject.clone()),
            preset_name: request.preset_name.clone(),
            profile_name: request.profile_name.clone(),
            bindings,
            metadata: BTreeMap::new(),
        })
    }

    fn bindings_from_preset(
        &self,
        preset_name: &str,
        preset: &CapabilityPresetDescriptor,
        grants: &[&CapabilityGrant],
    ) -> Result<Vec<ManifestBinding>, PolicyError> {
        preset
            .bindings
            .iter()
            .map(|binding| {
                let grant = find_matching_grant(grants, &binding.template_id).ok_or_else(|| {
                    PolicyError::PresetBindingDenied {
                        preset_name: preset_name.to_string(),
                        template_id: binding.template_id.clone(),
                    }
                })?;
                self.binding_from_requested_binding(grant, binding, Some(preset_name))
            })
            .collect()
    }

    fn binding_from_grant(
        &self,
        grant: &CapabilityGrant,
        requested: Option<&PresetBinding>,
    ) -> Result<ManifestBinding, PolicyError> {
        let template =
            self.templates
                .get(&grant.template_id)
                .ok_or_else(|| PolicyError::UnknownTemplate {
                    template_id: grant.template_id.clone(),
                })?;
        let binding_name = requested
            .and_then(|value| value.binding_name.clone())
            .or_else(|| grant.binding_name.clone())
            .unwrap_or_else(|| template.default_binding.clone());
        let expose_in_just_bash = requested
            .and_then(|value| value.expose_in_just_bash)
            .unwrap_or(template.expose_in_just_bash);
        let resource_policy = requested
            .and_then(|value| value.resource_policy.clone())
            .or_else(|| grant.resource_policy.clone())
            .unwrap_or_else(|| template.default_resource_policy.clone());
        let budget_policy = requested
            .and_then(|value| value.budget_policy.clone())
            .or_else(|| grant.budget_policy.clone())
            .unwrap_or_else(|| template.default_budget_policy.clone());

        Ok(ManifestBinding {
            binding_name: binding_name.clone(),
            capability_family: template.capability_family.clone(),
            module_specifier: capability_module_specifier(&binding_name),
            shell_command: expose_in_just_bash
                .then(|| ShellCommandDescriptor::for_binding(binding_name)),
            resource_policy,
            budget_policy,
            source_template_id: template.template_id.clone(),
            source_grant_id: Some(grant.grant_id.clone()),
            allow_interactive_widening: grant.allow_interactive_widening,
            metadata: template.metadata.clone(),
        })
    }

    fn binding_from_requested_binding(
        &self,
        grant: &CapabilityGrant,
        requested: &PresetBinding,
        profile_name: Option<&str>,
    ) -> Result<ManifestBinding, PolicyError> {
        let mut binding = self.binding_from_grant(grant, Some(requested))?;
        if let Some(profile_name) = profile_name {
            binding.metadata.insert(
                "profile_name".to_string(),
                JsonValue::String(profile_name.to_string()),
            );
        }
        Ok(binding)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DraftAuthorizationAttempt {
    pub request: DraftAuthorizationRequest,
    pub trigger_request: Option<CapabilityUseRequest>,
    pub trigger_decision: Option<PolicyDecisionRecord>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DraftAuthorizationHistoryKind {
    PolicyOutcome {
        request: CapabilityUseRequest,
        decision: PolicyDecisionRecord,
        authorization_request_id: Option<String>,
    },
    AuthorizationRequested {
        request: DraftAuthorizationRequest,
    },
    AuthorizationDecided {
        request: DraftAuthorizationRequest,
        decision: DraftAuthorizationDecision,
    },
    ManifestRefreshed {
        request_id: String,
        scope: AuthorizationScope,
        manifest: CapabilityManifest,
    },
    RetryOutcome {
        request_id: String,
        request: CapabilityUseRequest,
        decision: PolicyDecisionRecord,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DraftAuthorizationHistoryEntry {
    pub sequence: u64,
    pub session_id: String,
    pub occurred_at: Timestamp,
    pub kind: DraftAuthorizationHistoryKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppliedDraftAuthorizationDecision {
    pub request: DraftAuthorizationRequest,
    pub decision: DraftAuthorizationDecision,
    pub refreshed_manifest: Option<CapabilityManifest>,
    pub pending_authorization: Option<DraftAuthorizationRequest>,
}

#[derive(Debug, Error)]
pub enum DraftAuthorizationFlowError {
    #[error("interactive authorization is only available for trusted draft sessions")]
    InteractiveAuthorizationDisabled,
    #[error("draft authorization request {request_id} was not found")]
    RequestNotFound { request_id: String },
    #[error("session {session_id} already has a pending authorization request")]
    PendingRequestExists { session_id: String },
    #[error("approved authorization decision {request_id} is missing an approved scope")]
    MissingApprovedScope { request_id: String },
    #[error(
        "approved authorization decision {request_id} did not produce a policy that satisfies the request"
    )]
    ApprovalUnsatisfied { request_id: String },
    #[error(transparent)]
    Policy(#[from] PolicyError),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ActiveDraftAuthorization {
    attempt: DraftAuthorizationAttempt,
    scope: AuthorizationScope,
    policy: ResolvedSessionPolicy,
    remaining_uses: Option<u64>,
}

impl ActiveDraftAuthorization {
    fn matches(&self, request: &CapabilityUseRequest) -> bool {
        if let Some(trigger_request) = self.attempt.trigger_request.as_ref() {
            if self.scope == AuthorizationScope::OneCall {
                trigger_request == request
            } else {
                use_request_signature_matches(trigger_request, request)
            }
        } else {
            self.attempt.request.binding_name == request.binding_name
                && request
                    .capability_family
                    .as_ref()
                    .is_none_or(|family| family == &self.attempt.request.capability_family)
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeterministicDraftAuthorizationSession {
    engine: DeterministicPolicyEngine,
    resolution_request: PolicyResolutionRequest,
    trusted_draft: bool,
    policy: ResolvedSessionPolicy,
    pending_authorization: Option<DraftAuthorizationRequest>,
    attempts: BTreeMap<String, DraftAuthorizationAttempt>,
    active_authorizations: Vec<ActiveDraftAuthorization>,
    history: Vec<DraftAuthorizationHistoryEntry>,
    next_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct DeterministicDraftAuthorizationSessionState {
    resolution_request: PolicyResolutionRequest,
    trusted_draft: bool,
    policy: ResolvedSessionPolicy,
    pending_authorization: Option<DraftAuthorizationRequest>,
    attempts: BTreeMap<String, DraftAuthorizationAttempt>,
    active_authorizations: Vec<ActiveDraftAuthorization>,
    history: Vec<DraftAuthorizationHistoryEntry>,
    next_sequence: u64,
}

impl DeterministicDraftAuthorizationSessionState {
    fn from_session(session: &DeterministicDraftAuthorizationSession) -> Self {
        Self {
            resolution_request: session.resolution_request.clone(),
            trusted_draft: session.trusted_draft,
            policy: session.policy.clone(),
            pending_authorization: session.pending_authorization.clone(),
            attempts: session.attempts.clone(),
            active_authorizations: session.active_authorizations.clone(),
            history: session.history.clone(),
            next_sequence: session.next_sequence,
        }
    }
}

impl DeterministicDraftAuthorizationSession {
    pub fn open(
        engine: DeterministicPolicyEngine,
        resolution_request: PolicyResolutionRequest,
        trusted_draft: bool,
    ) -> Result<Self, PolicyError> {
        let policy = engine.resolve(&resolution_request)?;
        Ok(Self {
            engine,
            resolution_request,
            trusted_draft,
            policy,
            pending_authorization: None,
            attempts: BTreeMap::new(),
            active_authorizations: Vec::new(),
            history: Vec::new(),
            next_sequence: 1,
        })
    }

    pub fn trusted_draft(&self) -> bool {
        self.trusted_draft
    }

    pub fn resolved_policy(&self) -> &ResolvedSessionPolicy {
        &self.policy
    }

    pub fn pending_authorization(&self) -> Option<&DraftAuthorizationRequest> {
        self.pending_authorization.as_ref()
    }

    pub fn history(&self) -> &[DraftAuthorizationHistoryEntry] {
        &self.history
    }

    pub fn snapshot_json(&self) -> Result<JsonValue, serde_json::Error> {
        serde_json::to_value(DeterministicDraftAuthorizationSessionState::from_session(
            self,
        ))
    }

    pub fn restore_from_json(
        engine: DeterministicPolicyEngine,
        snapshot: JsonValue,
    ) -> Result<Self, serde_json::Error> {
        let state: DeterministicDraftAuthorizationSessionState = serde_json::from_value(snapshot)?;
        Ok(Self {
            engine,
            resolution_request: state.resolution_request,
            trusted_draft: state.trusted_draft,
            policy: state.policy,
            pending_authorization: state.pending_authorization,
            attempts: state.attempts,
            active_authorizations: state.active_authorizations,
            history: state.history,
            next_sequence: state.next_sequence,
        })
    }

    pub fn evaluate_use(&mut self, request: CapabilityUseRequest) -> PolicyDecisionRecord {
        let base_decision = self.policy.evaluate_use(&request);
        let mut final_decision = base_decision.clone();
        let mut authorization_request_id = None;

        if base_decision.outcome.outcome != PolicyOutcomeKind::Allowed {
            if let Some(index) = self.matching_active_authorization_index(&request) {
                let candidate = self.active_authorizations[index]
                    .policy
                    .evaluate_use(&request);
                if candidate.outcome.outcome == PolicyOutcomeKind::Allowed {
                    final_decision = candidate;
                    authorization_request_id = Some(
                        self.active_authorizations[index]
                            .attempt
                            .request
                            .request_id
                            .clone(),
                    );
                    if let Some(remaining_uses) =
                        self.active_authorizations[index].remaining_uses.as_mut()
                    {
                        *remaining_uses = remaining_uses.saturating_sub(1);
                    }
                }
            }
        } else if let Some(request_id) = self.matching_persistent_authorization_request_id(&request)
        {
            authorization_request_id = Some(request_id);
        }

        self.active_authorizations.retain(|authorization| {
            authorization
                .remaining_uses
                .is_none_or(|remaining| remaining > 0)
        });

        let occurred_at = final_decision.outcome.observed_at;
        if let Some(request_id) = authorization_request_id {
            self.push_history(
                occurred_at,
                DraftAuthorizationHistoryKind::RetryOutcome {
                    request_id,
                    request,
                    decision: final_decision.clone(),
                },
            );
        } else {
            self.push_history(
                occurred_at,
                DraftAuthorizationHistoryKind::PolicyOutcome {
                    request,
                    decision: final_decision.clone(),
                    authorization_request_id: None,
                },
            );
        }

        final_decision
    }

    pub fn request_authorization_for_outcome(
        &mut self,
        use_request: CapabilityUseRequest,
        decision: PolicyDecisionRecord,
        request_id: impl Into<String>,
        requested_scope: AuthorizationScope,
        requested_at: Timestamp,
        reason: Option<String>,
    ) -> Result<Option<DraftAuthorizationRequest>, DraftAuthorizationFlowError> {
        self.ensure_interactive_authorization_enabled()?;
        self.ensure_no_pending_request()?;

        if decision.outcome.outcome == PolicyOutcomeKind::Denied
            && !self.binding_allows_interactive_widening(&decision.audit.binding_name)
        {
            return Ok(None);
        }

        let Some(request) =
            decision.draft_authorization_request(request_id, requested_scope, requested_at, reason)
        else {
            return Ok(None);
        };

        self.attempts.insert(
            request.request_id.clone(),
            DraftAuthorizationAttempt {
                request: request.clone(),
                trigger_request: Some(use_request),
                trigger_decision: Some(decision),
            },
        );
        self.pending_authorization = Some(request.clone());
        self.push_history(
            request.requested_at,
            DraftAuthorizationHistoryKind::AuthorizationRequested {
                request: request.clone(),
            },
        );

        Ok(Some(request))
    }

    pub fn request_binding_authorization(
        &mut self,
        request_id: impl Into<String>,
        binding_name: impl Into<String>,
        capability_family: impl Into<String>,
        requested_scope: AuthorizationScope,
        requested_at: Timestamp,
        reason: Option<String>,
        metadata: BTreeMap<String, JsonValue>,
    ) -> Result<DraftAuthorizationRequest, DraftAuthorizationFlowError> {
        self.ensure_interactive_authorization_enabled()?;
        self.ensure_no_pending_request()?;

        let request = DraftAuthorizationRequest {
            request_id: request_id.into(),
            session_id: self.session_id().to_string(),
            binding_name: binding_name.into(),
            capability_family: capability_family.into(),
            kind: DraftAuthorizationRequestKind::InjectMissingBinding,
            requested_scope,
            reason,
            requested_at,
            metadata,
        };

        self.attempts.insert(
            request.request_id.clone(),
            DraftAuthorizationAttempt {
                request: request.clone(),
                trigger_request: None,
                trigger_decision: None,
            },
        );
        self.pending_authorization = Some(request.clone());
        self.push_history(
            request.requested_at,
            DraftAuthorizationHistoryKind::AuthorizationRequested {
                request: request.clone(),
            },
        );

        Ok(request)
    }

    pub fn apply_decision(
        &mut self,
        decision: DraftAuthorizationDecision,
        updated_engine: Option<DeterministicPolicyEngine>,
        updated_resolution_request: Option<PolicyResolutionRequest>,
    ) -> Result<AppliedDraftAuthorizationDecision, DraftAuthorizationFlowError> {
        let attempt = self
            .attempts
            .get(&decision.request_id)
            .cloned()
            .ok_or_else(|| DraftAuthorizationFlowError::RequestNotFound {
                request_id: decision.request_id.clone(),
            })?;

        self.push_history(
            decision.decided_at,
            DraftAuthorizationHistoryKind::AuthorizationDecided {
                request: attempt.request.clone(),
                decision: decision.clone(),
            },
        );

        let mut refreshed_manifest = None;

        match decision.outcome {
            DraftAuthorizationOutcomeKind::Pending => {
                self.pending_authorization = Some(attempt.request.clone());
            }
            DraftAuthorizationOutcomeKind::Rejected => {
                if self
                    .pending_authorization
                    .as_ref()
                    .is_some_and(|request| request.request_id == decision.request_id)
                {
                    self.pending_authorization = None;
                }
                self.active_authorizations.retain(|authorization| {
                    authorization.attempt.request.request_id != decision.request_id
                });
            }
            DraftAuthorizationOutcomeKind::Approved => {
                let scope = decision.approved_scope.ok_or_else(|| {
                    DraftAuthorizationFlowError::MissingApprovedScope {
                        request_id: decision.request_id.clone(),
                    }
                })?;
                let engine = updated_engine.unwrap_or_else(|| self.engine.clone());
                let resolution_request =
                    updated_resolution_request.unwrap_or_else(|| self.resolution_request.clone());
                let candidate_policy = engine.resolve(&resolution_request)?;

                if !authorization_attempt_is_satisfied(&attempt, &candidate_policy) {
                    return Err(DraftAuthorizationFlowError::ApprovalUnsatisfied {
                        request_id: decision.request_id.clone(),
                    });
                }

                if scope != AuthorizationScope::OneCall {
                    self.engine = engine;
                    self.resolution_request = resolution_request;
                    self.policy = candidate_policy.clone();
                    refreshed_manifest = Some(candidate_policy.manifest.clone());
                    self.push_history(
                        decision.decided_at,
                        DraftAuthorizationHistoryKind::ManifestRefreshed {
                            request_id: decision.request_id.clone(),
                            scope,
                            manifest: candidate_policy.manifest.clone(),
                        },
                    );
                }

                self.active_authorizations.retain(|authorization| {
                    authorization.attempt.request.request_id != decision.request_id
                });
                self.active_authorizations.push(ActiveDraftAuthorization {
                    attempt: attempt.clone(),
                    scope,
                    policy: candidate_policy,
                    remaining_uses: (scope == AuthorizationScope::OneCall).then_some(1),
                });
                if self
                    .pending_authorization
                    .as_ref()
                    .is_some_and(|request| request.request_id == decision.request_id)
                {
                    self.pending_authorization = None;
                }
            }
        }

        Ok(AppliedDraftAuthorizationDecision {
            request: attempt.request,
            decision,
            refreshed_manifest,
            pending_authorization: self.pending_authorization.clone(),
        })
    }

    fn ensure_interactive_authorization_enabled(&self) -> Result<(), DraftAuthorizationFlowError> {
        if self.policy.session_mode == SessionMode::Draft && self.trusted_draft {
            Ok(())
        } else {
            Err(DraftAuthorizationFlowError::InteractiveAuthorizationDisabled)
        }
    }

    fn ensure_no_pending_request(&self) -> Result<(), DraftAuthorizationFlowError> {
        if self.pending_authorization.is_some() {
            Err(DraftAuthorizationFlowError::PendingRequestExists {
                session_id: self.session_id().to_string(),
            })
        } else {
            Ok(())
        }
    }

    fn binding_allows_interactive_widening(&self, binding_name: &str) -> bool {
        self.policy
            .manifest
            .bindings
            .iter()
            .find(|binding| binding.binding_name == binding_name)
            .is_none_or(|binding| binding.allow_interactive_widening)
    }

    fn matching_active_authorization_index(&self, request: &CapabilityUseRequest) -> Option<usize> {
        self.active_authorizations
            .iter()
            .position(|authorization| authorization.matches(request))
    }

    fn matching_persistent_authorization_request_id(
        &self,
        request: &CapabilityUseRequest,
    ) -> Option<String> {
        self.active_authorizations
            .iter()
            .find(|authorization| {
                authorization.scope != AuthorizationScope::OneCall && authorization.matches(request)
            })
            .map(|authorization| authorization.attempt.request.request_id.clone())
    }

    fn push_history(&mut self, occurred_at: Timestamp, kind: DraftAuthorizationHistoryKind) {
        self.history.push(DraftAuthorizationHistoryEntry {
            sequence: self.next_sequence,
            session_id: self.session_id().to_string(),
            occurred_at,
            kind,
        });
        self.next_sequence += 1;
    }

    fn session_id(&self) -> &str {
        &self.resolution_request.subject.session_id
    }
}

fn find_matching_grant<'a>(
    grants: &[&'a CapabilityGrant],
    template_id: &str,
) -> Option<&'a CapabilityGrant> {
    grants
        .iter()
        .copied()
        .find(|grant| grant.template_id == template_id)
}

fn is_false(value: &bool) -> bool {
    !value
}

fn default_call_count() -> u64 {
    1
}

fn authorization_attempt_is_satisfied(
    attempt: &DraftAuthorizationAttempt,
    policy: &ResolvedSessionPolicy,
) -> bool {
    if let Some(trigger_request) = attempt.trigger_request.as_ref() {
        policy.evaluate_use(trigger_request).outcome.outcome == PolicyOutcomeKind::Allowed
    } else {
        policy.manifest.bindings.iter().any(|binding| {
            binding.binding_name == attempt.request.binding_name
                && binding.capability_family == attempt.request.capability_family
        })
    }
}

fn use_request_signature_matches(
    left: &CapabilityUseRequest,
    right: &CapabilityUseRequest,
) -> bool {
    left.session_id == right.session_id
        && left.operation == right.operation
        && left.binding_name == right.binding_name
        && left.targets == right.targets
        && capability_family_matches(
            left.capability_family.as_deref(),
            right.capability_family.as_deref(),
        )
}

fn capability_family_matches(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

fn tenant_scope_denies(subject: &PolicySubject, policy: &ResourcePolicy) -> bool {
    !policy.tenant_scopes.is_empty()
        && subject
            .tenant_id
            .as_deref()
            .is_none_or(|tenant_id| !policy.tenant_scopes.iter().any(|scope| scope == tenant_id))
}

fn denied_target<'a>(
    binding: &'a ManifestBinding,
    targets: &'a [ResourceTarget],
) -> Option<&'a ResourceTarget> {
    targets.iter().find(|target| {
        binding
            .resource_policy
            .deny
            .iter()
            .any(|selector| selector.matches_target(target))
            || (!binding.resource_policy.allow.is_empty()
                && !binding
                    .resource_policy
                    .allow
                    .iter()
                    .any(|selector| selector.matches_target(target)))
    })
}

fn budget_exhausted_message(
    policy: &BudgetPolicy,
    metrics: &CapabilityUseMetrics,
) -> Option<String> {
    if let Some(max_calls) = policy
        .max_calls
        .filter(|max_calls| metrics.call_count > *max_calls)
    {
        return Some(format!(
            "requested {} calls exceeds max_calls {}",
            metrics.call_count, max_calls
        ));
    }
    if let Some((requested, max)) = metrics
        .scanned_rows
        .zip(policy.max_scanned_rows)
        .filter(|(requested, max)| requested > max)
    {
        return Some(format!(
            "requested scanned_rows {} exceeds max_scanned_rows {}",
            requested, max
        ));
    }
    if let Some((requested, max)) = metrics
        .returned_rows
        .zip(policy.max_returned_rows)
        .filter(|(requested, max)| requested > max)
    {
        return Some(format!(
            "requested returned_rows {} exceeds max_returned_rows {}",
            requested, max
        ));
    }
    if let Some((requested, max)) = metrics
        .bytes
        .zip(policy.max_bytes)
        .filter(|(requested, max)| requested > max)
    {
        return Some(format!(
            "requested bytes {} exceeds max_bytes {}",
            requested, max
        ));
    }
    if let Some((requested, max)) = metrics
        .millis
        .zip(policy.max_millis)
        .filter(|(requested, max)| requested > max)
    {
        return Some(format!(
            "requested millis {} exceeds max_millis {}",
            requested, max
        ));
    }
    None
}

fn policy_outcome_metadata(
    request: &CapabilityUseRequest,
    assignment: &ExecutionDomainAssignment,
    capability_family: Option<&str>,
    preset_name: Option<&str>,
    profile_name: Option<&str>,
) -> BTreeMap<String, JsonValue> {
    let mut metadata = BTreeMap::from([
        (
            "session_id".to_string(),
            JsonValue::String(request.session_id.clone()),
        ),
        ("operation".to_string(), json_value(&request.operation)),
        (
            "execution_domain".to_string(),
            json_value(&assignment.domain),
        ),
        ("target_resources".to_string(), json_value(&request.targets)),
    ]);
    if let Some(capability_family) = capability_family {
        metadata.insert(
            "capability_family".to_string(),
            JsonValue::String(capability_family.to_string()),
        );
    }
    if let Some(preset_name) = preset_name {
        metadata.insert(
            "preset_name".to_string(),
            JsonValue::String(preset_name.to_string()),
        );
    }
    if let Some(profile_name) = profile_name {
        metadata.insert(
            "profile_name".to_string(),
            JsonValue::String(profile_name.to_string()),
        );
    }
    metadata
}

fn wildcard_pattern_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let parts = pattern.split('*').collect::<Vec<_>>();
    if parts.len() == 1 {
        return pattern == value;
    }

    let mut remainder = value;
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if index == 0 && !pattern.starts_with('*') {
            let Some(next) = remainder.strip_prefix(part) else {
                return false;
            };
            remainder = next;
            continue;
        }

        if index == parts.len() - 1 && !pattern.ends_with('*') {
            return remainder.ends_with(part);
        }

        let Some(position) = remainder.find(part) else {
            return false;
        };
        remainder = &remainder[position + part.len()..];
    }

    pattern.ends_with('*') || remainder.is_empty()
}

fn json_value<T>(value: &T) -> JsonValue
where
    T: Serialize,
{
    serde_json::to_value(value).unwrap_or(JsonValue::Null)
}
