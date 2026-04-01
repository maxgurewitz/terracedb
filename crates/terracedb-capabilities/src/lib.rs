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
    pub manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rate_limits: Vec<RateLimitEvaluation>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub budget_hooks: BTreeMap<ExecutionOperation, BudgetAccountingHook>,
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
}

impl StaticExecutionPolicyResolver {
    pub fn new(default_policy: ExecutionPolicy) -> Self {
        Self {
            default_policy,
            by_session_mode: BTreeMap::new(),
        }
    }

    pub fn with_policy(mut self, mode: SessionMode, policy: ExecutionPolicy) -> Self {
        self.by_session_mode.insert(mode, policy);
        self
    }
}

impl ExecutionPolicyResolver for StaticExecutionPolicyResolver {
    fn resolve_execution_policy(
        &self,
        request: &ExecutionPolicyRequest,
    ) -> Result<ExecutionPolicy, PolicyError> {
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
        budget_hooks.insert(
            ExecutionOperation::DraftSession,
            self.budget_accountant.hook_for(
                ExecutionOperation::DraftSession,
                &execution_policy.default_assignment,
            ),
        );
        for (operation, assignment) in &execution_policy.operations {
            budget_hooks.insert(
                *operation,
                self.budget_accountant.hook_for(*operation, assignment),
            );
        }

        Ok(ResolvedSessionPolicy {
            subject,
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

fn find_matching_grant<'a>(
    grants: &[&'a CapabilityGrant],
    template_id: &str,
) -> Option<&'a CapabilityGrant> {
    grants
        .iter()
        .copied()
        .find(|grant| grant.template_id == template_id)
}
