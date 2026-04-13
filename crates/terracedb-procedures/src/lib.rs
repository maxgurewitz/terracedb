//! Reviewed procedure publication, immutable artifacts, and deterministic invocation helpers.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::Timestamp;
use terracedb_capabilities::{
    BudgetPolicy, CapabilityManifest, CapabilityUseMetrics, CapabilityUseRequest,
    DatabaseCapabilityFamily, EffectiveRowScopeBinding, ExecutionDomain, ExecutionDomainAssignment,
    ExecutionOperation, ExecutionPolicy, ManifestBinding, ManifestBindingOverride, PolicyContext,
    PolicyDecisionRecord, PolicyOutcomeKind, PolicySubject, ResolvedSessionPolicy, ResourceKind,
    ResourcePolicy, ResourceSelector, ResourceTarget, SessionMode, VisibilityIndexReader,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProcedureVersionRef {
    pub procedure_id: String,
    pub version: u64,
}

impl ProcedureVersionRef {
    fn validate(&self) -> Result<(), ProcedureError> {
        if self.procedure_id.trim().is_empty() {
            return Err(ProcedureError::EmptyProcedureId);
        }
        if self.version == 0 {
            return Err(ProcedureError::InvalidVersion {
                procedure_id: self.procedure_id.clone(),
            });
        }
        Ok(())
    }
}

impl std::fmt::Display for ProcedureVersionRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.procedure_id, self.version)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ProcedureDraftSource {
    DraftSandboxSession { session_id: String },
}

impl ProcedureDraftSource {
    fn validate(&self, publication: &ProcedureVersionRef) -> Result<(), ProcedureError> {
        match self {
            Self::DraftSandboxSession { session_id } if session_id.trim().is_empty() => {
                Err(ProcedureError::EmptyDraftSessionId {
                    procedure_id: publication.procedure_id.clone(),
                    version: publication.version,
                })
            }
            Self::DraftSandboxSession { .. } => Ok(()),
        }
    }

    fn key(&self) -> &str {
        match self {
            Self::DraftSandboxSession { session_id } => session_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureDraft {
    pub publication: ProcedureVersionRef,
    pub source: ProcedureDraftSource,
    pub entrypoint: String,
    pub requested_manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    pub input_schema: JsonValue,
    pub output_schema: JsonValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl ProcedureDraft {
    fn validate_reviewed_surface(&self) -> Result<(), ProcedureError> {
        self.publication.validate()?;
        self.source.validate(&self.publication)?;
        if self.entrypoint.trim().is_empty() {
            return Err(ProcedureError::EmptyEntrypoint {
                procedure_id: self.publication.procedure_id.clone(),
                version: self.publication.version,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureReview {
    pub reviewed_by: String,
    pub source_revision: String,
    pub note: Option<String>,
    pub approved_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewedProcedureDraft {
    pub draft: ProcedureDraft,
    pub review: ProcedureReview,
    pub published_at: Timestamp,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl ReviewedProcedureDraft {
    pub fn validate_reviewed_surface(&self) -> Result<(), ProcedureError> {
        self.draft.validate_reviewed_surface()?;
        if self.review.reviewed_by.trim().is_empty() {
            return Err(ProcedureError::EmptyReviewer);
        }
        if self.review.source_revision.trim().is_empty() {
            return Err(ProcedureError::EmptySourceRevision);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ProcedureImmutableArtifact {
    SandboxSnapshot {
        snapshot_id: String,
    },
    ModuleBundle {
        bundle_id: String,
        root_module: String,
    },
}

impl ProcedureImmutableArtifact {
    fn validate(&self, publication: &ProcedureVersionRef) -> Result<(), ProcedureError> {
        match self {
            Self::SandboxSnapshot { snapshot_id } if snapshot_id.trim().is_empty() => {
                Err(ProcedureError::EmptyImmutableArtifactField {
                    procedure_id: publication.procedure_id.clone(),
                    version: publication.version,
                    field: "artifact.snapshot_id".to_string(),
                })
            }
            Self::ModuleBundle { bundle_id, .. } if bundle_id.trim().is_empty() => {
                Err(ProcedureError::EmptyImmutableArtifactField {
                    procedure_id: publication.procedure_id.clone(),
                    version: publication.version,
                    field: "artifact.bundle_id".to_string(),
                })
            }
            Self::ModuleBundle { root_module, .. } if root_module.trim().is_empty() => {
                Err(ProcedureError::EmptyImmutableArtifactField {
                    procedure_id: publication.procedure_id.clone(),
                    version: publication.version,
                    field: "artifact.root_module".to_string(),
                })
            }
            Self::SandboxSnapshot { .. } | Self::ModuleBundle { .. } => Ok(()),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::SandboxSnapshot { .. } => "sandbox_snapshot",
            Self::ModuleBundle { .. } => "module_bundle",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrozenProcedureArtifact {
    pub artifact: ProcedureImmutableArtifact,
    pub code_hash: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl FrozenProcedureArtifact {
    fn validate(&self, publication: &ProcedureVersionRef) -> Result<(), ProcedureError> {
        self.artifact.validate(publication)?;
        if self.code_hash.trim().is_empty() {
            return Err(ProcedureError::EmptyCodeHash {
                procedure_id: publication.procedure_id.clone(),
                version: publication.version,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewedProcedurePublication {
    pub publication: ProcedureVersionRef,
    pub artifact: ProcedureImmutableArtifact,
    pub code_hash: String,
    pub entrypoint: String,
    pub input_schema: JsonValue,
    pub output_schema: JsonValue,
    pub published_at: Timestamp,
    pub manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    pub review: ProcedureReview,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl ReviewedProcedurePublication {
    pub fn validate_reviewed_surface(&self) -> Result<(), ProcedureError> {
        self.publication.validate()?;
        self.artifact.validate(&self.publication)?;
        if self.code_hash.trim().is_empty() {
            return Err(ProcedureError::EmptyCodeHash {
                procedure_id: self.publication.procedure_id.clone(),
                version: self.publication.version,
            });
        }
        if self.entrypoint.trim().is_empty() {
            return Err(ProcedureError::EmptyEntrypoint {
                procedure_id: self.publication.procedure_id.clone(),
                version: self.publication.version,
            });
        }
        if self.review.reviewed_by.trim().is_empty() {
            return Err(ProcedureError::EmptyReviewer);
        }
        if self.review.source_revision.trim().is_empty() {
            return Err(ProcedureError::EmptySourceRevision);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedurePublicationReceipt {
    pub publication: ProcedureVersionRef,
    pub published_at: Timestamp,
    pub code_hash: String,
    pub execution_policy: ExecutionPolicy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureDeployment {
    pub deployment_id: String,
    pub invocation_binding: ManifestBinding,
    pub execution_policy: ExecutionPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binding_overrides: Vec<ManifestBindingOverride>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl ProcedureDeployment {
    fn validate(&self) -> Result<(), ProcedureError> {
        if self.deployment_id.trim().is_empty() {
            return Err(ProcedureError::EmptyDeploymentId);
        }
        if self.invocation_binding.capability_family
            != DatabaseCapabilityFamily::ProcedureInvokeV1.as_str()
        {
            return Err(ProcedureError::InvalidInvocationBinding {
                deployment_id: self.deployment_id.clone(),
                capability_family: self.invocation_binding.capability_family.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureInvocationContext {
    pub caller: Option<PolicySubject>,
    pub session_mode: SessionMode,
    pub session_id: Option<String>,
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureInvocationRequest {
    pub publication: ProcedureVersionRef,
    pub arguments: JsonValue,
    pub context: ProcedureInvocationContext,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureBindingResolution {
    pub binding_name: String,
    pub effective_row_scope: Option<EffectiveRowScopeBinding>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedProcedureInvocation {
    pub invocation_id: String,
    pub publication: ReviewedProcedurePublication,
    pub request: ProcedureInvocationRequest,
    pub deployment: ProcedureDeployment,
    pub effective_manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    pub execution_domain: ExecutionDomain,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binding_resolutions: Vec<ProcedureBindingResolution>,
    pub policy_outcome: PolicyDecisionRecord,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureRuntimeOutput {
    pub result: JsonValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureInvocationReceipt {
    pub invocation_id: String,
    pub publication: ProcedureVersionRef,
    pub accepted_at: Timestamp,
    pub completed_at: Timestamp,
    pub execution_policy: ExecutionPolicy,
    pub execution_domain: ExecutionDomain,
    pub effective_manifest: CapabilityManifest,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binding_resolutions: Vec<ProcedureBindingResolution>,
    pub output: JsonValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcedureInvocationState {
    Succeeded,
    Denied,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureInvocationRecord {
    pub invocation_id: String,
    pub publication: ProcedureVersionRef,
    pub state: ProcedureInvocationState,
    pub requested_at: Timestamp,
    pub completed_at: Timestamp,
    pub caller: Option<PolicySubject>,
    pub execution_policy: ExecutionPolicy,
    pub execution_domain: ExecutionDomain,
    pub effective_manifest: CapabilityManifest,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binding_resolutions: Vec<ProcedureBindingResolution>,
    pub policy_outcome: PolicyDecisionRecord,
    pub arguments: JsonValue,
    pub output: Option<JsonValue>,
    pub failure: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcedureAuditEventKind {
    ReviewApproved,
    PublicationStarted,
    PublicationSucceeded,
    InvocationStarted,
    InvocationDenied,
    InvocationSucceeded,
    InvocationFailed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureAuditRecord {
    pub audit_id: String,
    pub procedure_id: String,
    pub version: u64,
    pub invocation_id: Option<String>,
    pub event: ProcedureAuditEventKind,
    pub recorded_at: Timestamp,
    pub execution_operation: ExecutionOperation,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingProcedurePublication {
    pub publication: ReviewedProcedurePublication,
    pub recorded_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingProcedureInvocation {
    pub invocation: PreparedProcedureInvocation,
    pub recorded_at: Timestamp,
}

#[derive(Debug, Error)]
pub enum ProcedureStoreError {
    #[error(
        "procedure publication {procedure_id}@{version} already exists in the deterministic store"
    )]
    Duplicate { procedure_id: String, version: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ProcedureRuntimeError {
    #[error("procedure invocation {invocation_id} for {procedure_id}@{version} failed: {message}")]
    Failed {
        invocation_id: String,
        procedure_id: String,
        version: u64,
        message: String,
    },
}

#[derive(Debug, Error)]
pub enum ProcedureError {
    #[error("procedure id cannot be empty")]
    EmptyProcedureId,
    #[error("procedure {procedure_id} must use a version greater than zero")]
    InvalidVersion { procedure_id: String },
    #[error("reviewed procedure metadata must include a non-empty reviewer identity")]
    EmptyReviewer,
    #[error("reviewed procedure metadata must include a non-empty source revision")]
    EmptySourceRevision,
    #[error("reviewed procedure {procedure_id}@{version} must have a non-empty entrypoint")]
    EmptyEntrypoint { procedure_id: String, version: u64 },
    #[error("reviewed procedure {procedure_id}@{version} must have a non-empty code hash")]
    EmptyCodeHash { procedure_id: String, version: u64 },
    #[error(
        "reviewed procedure {procedure_id}@{version} must reference a non-empty draft session id"
    )]
    EmptyDraftSessionId { procedure_id: String, version: u64 },
    #[error(
        "reviewed procedure {procedure_id}@{version} must have a non-empty immutable artifact field {field}"
    )]
    EmptyImmutableArtifactField {
        procedure_id: String,
        version: u64,
        field: String,
    },
    #[error("procedure publication {procedure_id}@{version} already exists")]
    DuplicatePublication { procedure_id: String, version: u64 },
    #[error(
        "reviewed procedure {procedure_id}@{version} has incomplete publication metadata and must fail closed"
    )]
    PendingPublication { procedure_id: String, version: u64 },
    #[error("reviewed procedure {procedure_id}@{version} was not found")]
    NotPublished { procedure_id: String, version: u64 },
    #[error("procedure deployment metadata must include a non-empty deployment id")]
    EmptyDeploymentId,
    #[error(
        "procedure deployment {deployment_id} must expose capability family procedure.invoke.v1, got {capability_family}"
    )]
    InvalidInvocationBinding {
        deployment_id: String,
        capability_family: String,
    },
    #[error("procedure deployment {deployment_id} cannot widen binding {binding_name} via {field}")]
    UnsafeBindingOverride {
        deployment_id: String,
        binding_name: String,
        field: String,
    },
    #[error(
        "procedure deployment {deployment_id} references unknown binding override {binding_name}"
    )]
    UnknownBindingOverride {
        deployment_id: String,
        binding_name: String,
    },
    #[error(
        "procedure deployment {deployment_id} cannot change execution domain for {operation} from {base:?} to {requested:?}"
    )]
    ExecutionDomainMismatch {
        deployment_id: String,
        operation: String,
        base: ExecutionDomain,
        requested: ExecutionDomain,
    },
    #[error(
        "procedure invocation {invocation_id} for {procedure_id}@{version} requires session_mode reviewed_procedure, got {actual:?}"
    )]
    InvalidSessionMode {
        invocation_id: String,
        procedure_id: String,
        version: u64,
        actual: SessionMode,
    },
    #[error(
        "procedure invocation {invocation_id} for {procedure_id}@{version} was denied: {message}"
    )]
    InvocationDenied {
        invocation_id: String,
        procedure_id: String,
        version: u64,
        message: String,
    },
    #[error(
        "procedure invocation {invocation_id} for {procedure_id}@{version} failed {phase} validation at {path}: {message}"
    )]
    SchemaValidation {
        invocation_id: String,
        procedure_id: String,
        version: u64,
        phase: &'static str,
        path: String,
        message: String,
    },
    #[error(
        "procedure invocation {invocation_id} for {procedure_id}@{version} could not resolve row scope for binding {binding_name}: {message}"
    )]
    RowScopeResolution {
        invocation_id: String,
        procedure_id: String,
        version: u64,
        binding_name: String,
        message: String,
    },
    #[error("procedure invocation {invocation_id} is not pending")]
    InvocationNotPending { invocation_id: String },
    #[error(
        "failed to freeze reviewed procedure {procedure_id}@{version} from draft source: {message}"
    )]
    Freeze {
        procedure_id: String,
        version: u64,
        message: String,
    },
    #[error(transparent)]
    Runtime(#[from] ProcedureRuntimeError),
}

pub trait ProcedurePublicationStore {
    fn publish(
        &mut self,
        publication: ReviewedProcedurePublication,
    ) -> Result<(), ProcedureStoreError>;

    fn load(&self, publication: &ProcedureVersionRef) -> Option<ReviewedProcedurePublication>;
}

pub trait ProcedureArtifactPublisher {
    fn freeze(&mut self, draft: &ProcedureDraft)
    -> Result<FrozenProcedureArtifact, ProcedureError>;
}

pub trait ReviewedProcedureRuntime {
    fn invoke(
        &mut self,
        invocation: &PreparedProcedureInvocation,
    ) -> Result<ProcedureRuntimeOutput, ProcedureRuntimeError>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeterministicFreezeBehavior {
    Success { artifact: FrozenProcedureArtifact },
    Error { message: String },
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicProcedureArtifactPublisher {
    drafts: BTreeMap<String, DeterministicFreezeBehavior>,
    freeze_counts: BTreeMap<String, u64>,
}

impl DeterministicProcedureArtifactPublisher {
    pub fn with_snapshot(
        mut self,
        draft_session_id: impl Into<String>,
        snapshot_id: impl Into<String>,
        code_hash: impl Into<String>,
    ) -> Self {
        self.drafts.insert(
            draft_session_id.into(),
            DeterministicFreezeBehavior::Success {
                artifact: FrozenProcedureArtifact {
                    artifact: ProcedureImmutableArtifact::SandboxSnapshot {
                        snapshot_id: snapshot_id.into(),
                    },
                    code_hash: code_hash.into(),
                    metadata: BTreeMap::new(),
                },
            },
        );
        self
    }

    pub fn with_module_bundle(
        mut self,
        draft_session_id: impl Into<String>,
        bundle_id: impl Into<String>,
        root_module: impl Into<String>,
        code_hash: impl Into<String>,
    ) -> Self {
        self.drafts.insert(
            draft_session_id.into(),
            DeterministicFreezeBehavior::Success {
                artifact: FrozenProcedureArtifact {
                    artifact: ProcedureImmutableArtifact::ModuleBundle {
                        bundle_id: bundle_id.into(),
                        root_module: root_module.into(),
                    },
                    code_hash: code_hash.into(),
                    metadata: BTreeMap::new(),
                },
            },
        );
        self
    }

    pub fn with_error(
        mut self,
        draft_session_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        self.drafts.insert(
            draft_session_id.into(),
            DeterministicFreezeBehavior::Error {
                message: message.into(),
            },
        );
        self
    }

    pub fn freeze_count(&self, draft_session_id: &str) -> u64 {
        self.freeze_counts
            .get(draft_session_id)
            .copied()
            .unwrap_or_default()
    }
}

impl ProcedureArtifactPublisher for DeterministicProcedureArtifactPublisher {
    fn freeze(
        &mut self,
        draft: &ProcedureDraft,
    ) -> Result<FrozenProcedureArtifact, ProcedureError> {
        let key = draft.source.key().to_string();
        self.freeze_counts
            .entry(key.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        match self.drafts.get(&key) {
            Some(DeterministicFreezeBehavior::Success { artifact }) => Ok(artifact.clone()),
            Some(DeterministicFreezeBehavior::Error { message }) => Err(ProcedureError::Freeze {
                procedure_id: draft.publication.procedure_id.clone(),
                version: draft.publication.version,
                message: message.clone(),
            }),
            None => Err(ProcedureError::Freeze {
                procedure_id: draft.publication.procedure_id.clone(),
                version: draft.publication.version,
                message: "no deterministic freeze behavior registered".to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeterministicProcedureBehavior {
    Return { result: JsonValue },
    EchoArguments,
    Error { message: String },
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicReviewedProcedureRuntime {
    behaviors: BTreeMap<ProcedureVersionRef, DeterministicProcedureBehavior>,
    invocations: Vec<PreparedProcedureInvocation>,
}

impl DeterministicReviewedProcedureRuntime {
    pub fn with_result(mut self, publication: ProcedureVersionRef, result: JsonValue) -> Self {
        self.behaviors.insert(
            publication,
            DeterministicProcedureBehavior::Return { result },
        );
        self
    }

    pub fn with_echo(mut self, publication: ProcedureVersionRef) -> Self {
        self.behaviors
            .insert(publication, DeterministicProcedureBehavior::EchoArguments);
        self
    }

    pub fn with_error(
        mut self,
        publication: ProcedureVersionRef,
        message: impl Into<String>,
    ) -> Self {
        self.behaviors.insert(
            publication,
            DeterministicProcedureBehavior::Error {
                message: message.into(),
            },
        );
        self
    }

    pub fn invocation_count(&self, publication: &ProcedureVersionRef) -> u64 {
        self.invocations
            .iter()
            .filter(|invocation| invocation.publication.publication == *publication)
            .count() as u64
    }

    pub fn last_invocation(&self) -> Option<&PreparedProcedureInvocation> {
        self.invocations.last()
    }
}

impl ReviewedProcedureRuntime for DeterministicReviewedProcedureRuntime {
    fn invoke(
        &mut self,
        invocation: &PreparedProcedureInvocation,
    ) -> Result<ProcedureRuntimeOutput, ProcedureRuntimeError> {
        self.invocations.push(invocation.clone());
        match self.behaviors.get(&invocation.publication.publication) {
            Some(DeterministicProcedureBehavior::Return { result }) => Ok(ProcedureRuntimeOutput {
                result: result.clone(),
                metadata: BTreeMap::new(),
            }),
            Some(DeterministicProcedureBehavior::EchoArguments) => Ok(ProcedureRuntimeOutput {
                result: invocation.request.arguments.clone(),
                metadata: BTreeMap::new(),
            }),
            Some(DeterministicProcedureBehavior::Error { message }) => {
                Err(ProcedureRuntimeError::Failed {
                    invocation_id: invocation.invocation_id.clone(),
                    procedure_id: invocation.publication.publication.procedure_id.clone(),
                    version: invocation.publication.publication.version,
                    message: message.clone(),
                })
            }
            None => Err(ProcedureRuntimeError::Failed {
                invocation_id: invocation.invocation_id.clone(),
                procedure_id: invocation.publication.publication.procedure_id.clone(),
                version: invocation.publication.publication.version,
                message: "no deterministic runtime behavior registered".to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeterministicProcedurePublicationStore {
    publications: BTreeMap<ProcedureVersionRef, ReviewedProcedurePublication>,
    audits: BTreeMap<ProcedureVersionRef, Vec<ProcedureAuditRecord>>,
    invocations: BTreeMap<ProcedureVersionRef, Vec<ProcedureInvocationRecord>>,
    pending_publications: BTreeMap<ProcedureVersionRef, PendingProcedurePublication>,
    pending_invocations: BTreeMap<String, PendingProcedureInvocation>,
    next_audit: u64,
    next_invocation: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct DeterministicProcedurePublicationStoreState {
    publications: Vec<(ProcedureVersionRef, ReviewedProcedurePublication)>,
    audits: Vec<(ProcedureVersionRef, Vec<ProcedureAuditRecord>)>,
    invocations: Vec<(ProcedureVersionRef, Vec<ProcedureInvocationRecord>)>,
    pending_publications: Vec<(ProcedureVersionRef, PendingProcedurePublication)>,
    pending_invocations: Vec<(String, PendingProcedureInvocation)>,
    next_audit: u64,
    next_invocation: u64,
}

impl DeterministicProcedurePublicationStoreState {
    fn from_store(store: &DeterministicProcedurePublicationStore) -> Self {
        Self {
            publications: store
                .publications
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            audits: store
                .audits
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            invocations: store
                .invocations
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            pending_publications: store
                .pending_publications
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            pending_invocations: store
                .pending_invocations
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            next_audit: store.next_audit,
            next_invocation: store.next_invocation,
        }
    }

    fn into_store(self) -> DeterministicProcedurePublicationStore {
        DeterministicProcedurePublicationStore {
            publications: self.publications.into_iter().collect(),
            audits: self.audits.into_iter().collect(),
            invocations: self.invocations.into_iter().collect(),
            pending_publications: self.pending_publications.into_iter().collect(),
            pending_invocations: self.pending_invocations.into_iter().collect(),
            next_audit: self.next_audit,
            next_invocation: self.next_invocation,
        }
    }
}

impl DeterministicProcedurePublicationStore {
    pub fn snapshot_json(&self) -> Result<JsonValue, serde_json::Error> {
        serde_json::to_value(DeterministicProcedurePublicationStoreState::from_store(
            self,
        ))
    }

    pub fn restore_from_json(snapshot: JsonValue) -> Result<Self, serde_json::Error> {
        let state: DeterministicProcedurePublicationStoreState = serde_json::from_value(snapshot)?;
        Ok(state.into_store())
    }

    pub fn list_published_versions(&self, procedure_id: &str) -> Vec<ReviewedProcedurePublication> {
        self.publications
            .iter()
            .filter(|(publication, _)| publication.procedure_id == procedure_id)
            .map(|(_, publication)| publication.clone())
            .collect()
    }

    pub fn audits_for_publication(
        &self,
        publication: &ProcedureVersionRef,
    ) -> Vec<ProcedureAuditRecord> {
        self.audits.get(publication).cloned().unwrap_or_default()
    }

    pub fn invocations_for_publication(
        &self,
        publication: &ProcedureVersionRef,
    ) -> Vec<ProcedureInvocationRecord> {
        self.invocations
            .get(publication)
            .cloned()
            .unwrap_or_default()
    }

    pub fn pending_publication(
        &self,
        publication: &ProcedureVersionRef,
    ) -> Option<PendingProcedurePublication> {
        self.pending_publications.get(publication).cloned()
    }

    pub fn pending_invocation(&self, invocation_id: &str) -> Option<PendingProcedureInvocation> {
        self.pending_invocations.get(invocation_id).cloned()
    }

    pub fn record_incomplete_publication(&mut self, pending: PendingProcedurePublication) {
        self.pending_publications
            .insert(pending.publication.publication.clone(), pending);
    }

    pub fn record_incomplete_invocation(&mut self, pending: PendingProcedureInvocation) {
        self.pending_invocations
            .insert(pending.invocation.invocation_id.clone(), pending);
    }

    fn begin_publication(
        &mut self,
        pending: PendingProcedurePublication,
    ) -> Result<(), ProcedureError> {
        let publication = pending.publication.publication.clone();
        if self.publications.contains_key(&publication) {
            return Err(ProcedureError::DuplicatePublication {
                procedure_id: publication.procedure_id,
                version: publication.version,
            });
        }
        if self.pending_publications.contains_key(&publication) {
            return Err(ProcedureError::PendingPublication {
                procedure_id: publication.procedure_id,
                version: publication.version,
            });
        }
        self.pending_publications.insert(publication, pending);
        Ok(())
    }

    fn clear_publication(&mut self, publication: &ProcedureVersionRef) {
        self.pending_publications.remove(publication);
    }

    fn begin_invocation(&mut self, pending: PendingProcedureInvocation) {
        self.pending_invocations
            .insert(pending.invocation.invocation_id.clone(), pending);
    }

    fn clear_invocation(&mut self, invocation_id: &str) {
        self.pending_invocations.remove(invocation_id);
    }

    fn append_audit(
        &mut self,
        publication: &ProcedureVersionRef,
        invocation_id: Option<&str>,
        event: ProcedureAuditEventKind,
        recorded_at: Timestamp,
        execution_operation: ExecutionOperation,
        metadata: BTreeMap<String, JsonValue>,
    ) {
        let audit_id = format!(
            "{}@{}:audit-{}",
            publication.procedure_id, publication.version, self.next_audit
        );
        self.next_audit += 1;
        self.audits
            .entry(publication.clone())
            .or_default()
            .push(ProcedureAuditRecord {
                audit_id,
                procedure_id: publication.procedure_id.clone(),
                version: publication.version,
                invocation_id: invocation_id.map(ToOwned::to_owned),
                event,
                recorded_at,
                execution_operation,
                metadata,
            });
    }

    fn append_invocation(&mut self, record: ProcedureInvocationRecord) {
        self.invocations
            .entry(record.publication.clone())
            .or_default()
            .push(record);
    }

    fn next_invocation_id(&mut self, publication: &ProcedureVersionRef) -> String {
        let invocation_id = format!(
            "{}@{}:invoke-{}",
            publication.procedure_id, publication.version, self.next_invocation
        );
        self.next_invocation += 1;
        invocation_id
    }
}

impl ProcedurePublicationStore for DeterministicProcedurePublicationStore {
    fn publish(
        &mut self,
        publication: ReviewedProcedurePublication,
    ) -> Result<(), ProcedureStoreError> {
        let key = publication.publication.clone();
        if self.publications.contains_key(&key) || self.pending_publications.contains_key(&key) {
            return Err(ProcedureStoreError::Duplicate {
                procedure_id: key.procedure_id,
                version: key.version,
            });
        }
        self.publications.insert(key, publication);
        Ok(())
    }

    fn load(&self, publication: &ProcedureVersionRef) -> Option<ReviewedProcedurePublication> {
        self.publications.get(publication).cloned()
    }
}

pub fn publish_reviewed_procedure<P>(
    store: &mut DeterministicProcedurePublicationStore,
    publisher: &mut P,
    reviewed: ReviewedProcedureDraft,
) -> Result<ProcedurePublicationReceipt, ProcedureError>
where
    P: ProcedureArtifactPublisher,
{
    let publication = begin_reviewed_procedure_publication(store, publisher, reviewed)?;
    finish_reviewed_procedure_publication(store, &publication)
}

pub fn begin_reviewed_procedure_publication<P>(
    store: &mut DeterministicProcedurePublicationStore,
    publisher: &mut P,
    reviewed: ReviewedProcedureDraft,
) -> Result<ProcedureVersionRef, ProcedureError>
where
    P: ProcedureArtifactPublisher,
{
    reviewed.validate_reviewed_surface()?;
    let frozen = publisher.freeze(&reviewed.draft)?;
    frozen.validate(&reviewed.draft.publication)?;

    let mut metadata = reviewed.draft.metadata.clone();
    metadata.extend(frozen.metadata.clone());
    metadata.extend(reviewed.metadata.clone());

    let publication = ReviewedProcedurePublication {
        publication: reviewed.draft.publication.clone(),
        artifact: frozen.artifact,
        code_hash: frozen.code_hash,
        entrypoint: reviewed.draft.entrypoint.clone(),
        input_schema: reviewed.draft.input_schema.clone(),
        output_schema: reviewed.draft.output_schema.clone(),
        published_at: reviewed.published_at,
        manifest: reviewed.draft.requested_manifest.clone(),
        execution_policy: reviewed.draft.execution_policy.clone(),
        review: reviewed.review.clone(),
        metadata,
    };
    publication.validate_reviewed_surface()?;

    store.begin_publication(PendingProcedurePublication {
        publication: publication.clone(),
        recorded_at: reviewed.published_at,
    })?;
    store.append_audit(
        &publication.publication,
        None,
        ProcedureAuditEventKind::ReviewApproved,
        publication.review.approved_at,
        ExecutionOperation::DraftSession,
        BTreeMap::from([
            (
                "reviewed_by".to_string(),
                JsonValue::String(publication.review.reviewed_by.clone()),
            ),
            (
                "source_revision".to_string(),
                JsonValue::String(publication.review.source_revision.clone()),
            ),
        ]),
    );
    store.append_audit(
        &publication.publication,
        None,
        ProcedureAuditEventKind::PublicationStarted,
        publication.published_at,
        ExecutionOperation::DraftSession,
        BTreeMap::from([
            (
                "artifact_kind".to_string(),
                JsonValue::String(publication.artifact.kind().to_string()),
            ),
            (
                "code_hash".to_string(),
                JsonValue::String(publication.code_hash.clone()),
            ),
        ]),
    );

    Ok(publication.publication)
}

pub fn finish_reviewed_procedure_publication(
    store: &mut DeterministicProcedurePublicationStore,
    publication: &ProcedureVersionRef,
) -> Result<ProcedurePublicationReceipt, ProcedureError> {
    let pending =
        store
            .pending_publication(publication)
            .ok_or_else(|| ProcedureError::NotPublished {
                procedure_id: publication.procedure_id.clone(),
                version: publication.version,
            })?;
    if store.publications.contains_key(publication) {
        return Err(ProcedureError::DuplicatePublication {
            procedure_id: publication.procedure_id.clone(),
            version: publication.version,
        });
    }

    let reviewed = pending.publication;
    store
        .publications
        .insert(publication.clone(), reviewed.clone());
    store.append_audit(
        publication,
        None,
        ProcedureAuditEventKind::PublicationSucceeded,
        reviewed.published_at,
        ExecutionOperation::DraftSession,
        BTreeMap::new(),
    );
    store.clear_publication(publication);

    Ok(ProcedurePublicationReceipt {
        publication: publication.clone(),
        published_at: reviewed.published_at,
        code_hash: reviewed.code_hash,
        execution_policy: reviewed.execution_policy,
    })
}

pub fn invoke_published_procedure<R, V>(
    store: &mut DeterministicProcedurePublicationStore,
    runtime: &mut R,
    deployment: &ProcedureDeployment,
    request: ProcedureInvocationRequest,
    invoked_at: Timestamp,
    completed_at: Timestamp,
    visibility_indexes: &V,
) -> Result<ProcedureInvocationReceipt, ProcedureError>
where
    R: ReviewedProcedureRuntime + ?Sized,
    V: VisibilityIndexReader + ?Sized,
{
    let prepared = prepare_published_procedure_invocation(
        store,
        deployment,
        request,
        invoked_at,
        visibility_indexes,
    )?;
    let result = runtime.invoke(&prepared);
    complete_published_procedure_invocation(store, &prepared.invocation_id, result, completed_at)
}

pub fn prepare_published_procedure_invocation<V>(
    store: &mut DeterministicProcedurePublicationStore,
    deployment: &ProcedureDeployment,
    request: ProcedureInvocationRequest,
    invoked_at: Timestamp,
    visibility_indexes: &V,
) -> Result<PreparedProcedureInvocation, ProcedureError>
where
    V: VisibilityIndexReader + ?Sized,
{
    deployment.validate()?;
    request.publication.validate()?;
    let publication_ref = request.publication.clone();
    if store.pending_publication(&publication_ref).is_some() {
        return Err(ProcedureError::PendingPublication {
            procedure_id: publication_ref.procedure_id,
            version: publication_ref.version,
        });
    }
    let publication = store
        .load(&publication_ref)
        .ok_or_else(|| ProcedureError::NotPublished {
            procedure_id: publication_ref.procedure_id.clone(),
            version: publication_ref.version,
        })?;

    let invocation_id = store.next_invocation_id(&publication.publication);
    let failure = |error: ProcedureError,
                   store: &mut DeterministicProcedurePublicationStore,
                   publication: &ReviewedProcedurePublication,
                   request: &ProcedureInvocationRequest,
                   invocation_id: &str,
                   invoked_at: Timestamp,
                   policy_outcome: Option<PolicyDecisionRecord>,
                   execution_policy: Option<ExecutionPolicy>,
                   binding_resolutions: Vec<ProcedureBindingResolution>| {
        let execution_policy =
            execution_policy.unwrap_or_else(|| publication.execution_policy.clone());
        let execution_domain = execution_policy
            .assignment_for(ExecutionOperation::ProcedureInvocation)
            .domain;
        let policy_outcome = policy_outcome.unwrap_or_else(|| {
            fallback_policy_outcome(
                publication,
                request,
                invocation_id,
                invoked_at,
                execution_domain,
                error.to_string(),
            )
        });
        store.append_invocation(ProcedureInvocationRecord {
            invocation_id: invocation_id.to_string(),
            publication: publication.publication.clone(),
            state: ProcedureInvocationState::Failed,
            requested_at: invoked_at,
            completed_at: invoked_at,
            caller: request.context.caller.clone(),
            execution_policy: execution_policy.clone(),
            execution_domain,
            effective_manifest: publication.manifest.clone(),
            binding_resolutions,
            policy_outcome,
            arguments: request.arguments.clone(),
            output: None,
            failure: Some(error.to_string()),
            metadata: base_invocation_metadata(publication, deployment, request),
        });
        store.append_audit(
            &publication.publication,
            Some(invocation_id),
            ProcedureAuditEventKind::InvocationFailed,
            invoked_at,
            ExecutionOperation::ProcedureInvocation,
            BTreeMap::from([("message".to_string(), JsonValue::String(error.to_string()))]),
        );
        error
    };

    if request.context.session_mode != SessionMode::ReviewedProcedure {
        return Err(failure(
            ProcedureError::InvalidSessionMode {
                invocation_id: invocation_id.clone(),
                procedure_id: publication.publication.procedure_id.clone(),
                version: publication.publication.version,
                actual: request.context.session_mode,
            },
            store,
            &publication,
            &request,
            &invocation_id,
            invoked_at,
            None,
            None,
            Vec::new(),
        ));
    }

    let execution_policy = match intersect_execution_policy(
        &publication.execution_policy,
        &deployment.execution_policy,
        &deployment.deployment_id,
    ) {
        Ok(policy) => policy,
        Err(error) => {
            return Err(failure(
                error,
                store,
                &publication,
                &request,
                &invocation_id,
                invoked_at,
                None,
                None,
                Vec::new(),
            ));
        }
    };
    let execution_domain = execution_policy
        .assignment_for(ExecutionOperation::ProcedureInvocation)
        .domain;
    let subject = request
        .context
        .caller
        .clone()
        .unwrap_or_else(anonymous_subject);
    let policy = invocation_policy(
        deployment,
        subject.clone(),
        execution_policy.clone(),
        &publication,
    );
    let policy_outcome = policy.evaluate_use(&CapabilityUseRequest {
        session_id: request.context.session_id.clone().unwrap_or_else(|| {
            format!(
                "procedure:{}:{}",
                publication.publication.procedure_id, invocation_id
            )
        }),
        operation: ExecutionOperation::ProcedureInvocation,
        binding_name: deployment.invocation_binding.binding_name.clone(),
        capability_family: Some(
            DatabaseCapabilityFamily::ProcedureInvokeV1
                .as_str()
                .to_string(),
        ),
        targets: vec![ResourceTarget {
            kind: ResourceKind::Procedure,
            identifier: publication.publication.procedure_id.clone(),
        }],
        metrics: CapabilityUseMetrics::default(),
        requested_at: invoked_at,
        metadata: base_invocation_metadata(&publication, deployment, &request),
    });
    store.append_audit(
        &publication.publication,
        Some(&invocation_id),
        ProcedureAuditEventKind::InvocationStarted,
        invoked_at,
        ExecutionOperation::ProcedureInvocation,
        BTreeMap::from([
            (
                "deployment_id".to_string(),
                JsonValue::String(deployment.deployment_id.clone()),
            ),
            (
                "execution_domain".to_string(),
                json_value(&execution_domain),
            ),
        ]),
    );

    if policy_outcome.outcome.outcome != PolicyOutcomeKind::Allowed {
        let message = policy_outcome
            .outcome
            .message
            .clone()
            .unwrap_or_else(|| "invocation denied by deployment policy".to_string());
        store.append_invocation(ProcedureInvocationRecord {
            invocation_id: invocation_id.clone(),
            publication: publication.publication.clone(),
            state: ProcedureInvocationState::Denied,
            requested_at: invoked_at,
            completed_at: invoked_at,
            caller: request.context.caller.clone(),
            execution_policy: execution_policy.clone(),
            execution_domain,
            effective_manifest: publication.manifest.clone(),
            binding_resolutions: Vec::new(),
            policy_outcome: policy_outcome.clone(),
            arguments: request.arguments.clone(),
            output: None,
            failure: Some(message.clone()),
            metadata: base_invocation_metadata(&publication, deployment, &request),
        });
        store.append_audit(
            &publication.publication,
            Some(&invocation_id),
            ProcedureAuditEventKind::InvocationDenied,
            invoked_at,
            ExecutionOperation::ProcedureInvocation,
            BTreeMap::from([("message".to_string(), JsonValue::String(message.clone()))]),
        );
        return Err(ProcedureError::InvocationDenied {
            invocation_id,
            procedure_id: publication.publication.procedure_id.clone(),
            version: publication.publication.version,
            message,
        });
    }

    if let Err(error) = validate_contract_schema(&publication.input_schema, &request.arguments) {
        return Err(failure(
            ProcedureError::SchemaValidation {
                invocation_id: invocation_id.clone(),
                procedure_id: publication.publication.procedure_id.clone(),
                version: publication.publication.version,
                phase: "input",
                path: error.path,
                message: error.message,
            },
            store,
            &publication,
            &request,
            &invocation_id,
            invoked_at,
            Some(policy_outcome),
            Some(execution_policy),
            Vec::new(),
        ));
    }

    let mut effective_manifest = publication.manifest.clone();
    effective_manifest.subject = request.context.caller.clone();
    if let Err(error) = apply_manifest_binding_overrides(
        &mut effective_manifest.bindings,
        &deployment.binding_overrides,
        &deployment.deployment_id,
    ) {
        return Err(failure(
            error,
            store,
            &publication,
            &request,
            &invocation_id,
            invoked_at,
            Some(policy_outcome),
            Some(execution_policy),
            Vec::new(),
        ));
    }

    let caller_context = PolicyContext::from(subject);
    let mut binding_resolutions = Vec::with_capacity(effective_manifest.bindings.len());
    for binding in &effective_manifest.bindings {
        let effective_row_scope = match binding
            .resource_policy
            .resolve_row_scope(caller_context.clone(), visibility_indexes)
        {
            Ok(row_scope) => row_scope,
            Err(error) => {
                return Err(failure(
                    ProcedureError::RowScopeResolution {
                        invocation_id: invocation_id.clone(),
                        procedure_id: publication.publication.procedure_id.clone(),
                        version: publication.publication.version,
                        binding_name: binding.binding_name.clone(),
                        message: error.to_string(),
                    },
                    store,
                    &publication,
                    &request,
                    &invocation_id,
                    invoked_at,
                    Some(policy_outcome),
                    Some(execution_policy),
                    binding_resolutions,
                ));
            }
        };
        binding_resolutions.push(ProcedureBindingResolution {
            binding_name: binding.binding_name.clone(),
            effective_row_scope,
        });
    }

    let prepared = PreparedProcedureInvocation {
        invocation_id: invocation_id.clone(),
        publication: publication.clone(),
        request: request.clone(),
        deployment: deployment.clone(),
        effective_manifest,
        execution_policy: execution_policy.clone(),
        execution_domain,
        binding_resolutions: binding_resolutions.clone(),
        policy_outcome,
        metadata: base_invocation_metadata(&publication, deployment, &request),
    };
    store.begin_invocation(PendingProcedureInvocation {
        invocation: prepared.clone(),
        recorded_at: invoked_at,
    });
    Ok(prepared)
}

pub fn complete_published_procedure_invocation(
    store: &mut DeterministicProcedurePublicationStore,
    invocation_id: &str,
    runtime_result: Result<ProcedureRuntimeOutput, ProcedureRuntimeError>,
    completed_at: Timestamp,
) -> Result<ProcedureInvocationReceipt, ProcedureError> {
    let pending = store.pending_invocation(invocation_id).ok_or_else(|| {
        ProcedureError::InvocationNotPending {
            invocation_id: invocation_id.to_string(),
        }
    })?;
    store.clear_invocation(invocation_id);

    let invocation = pending.invocation;
    let publication = &invocation.publication;
    let publication_ref = publication.publication.clone();
    match runtime_result {
        Ok(output) => {
            if let Err(error) = validate_contract_schema(&publication.output_schema, &output.result)
            {
                let error = ProcedureError::SchemaValidation {
                    invocation_id: invocation.invocation_id.clone(),
                    procedure_id: publication.publication.procedure_id.clone(),
                    version: publication.publication.version,
                    phase: "output",
                    path: error.path,
                    message: error.message,
                };
                store.append_invocation(ProcedureInvocationRecord {
                    invocation_id: invocation.invocation_id.clone(),
                    publication: publication_ref.clone(),
                    state: ProcedureInvocationState::Failed,
                    requested_at: pending.recorded_at,
                    completed_at,
                    caller: invocation.request.context.caller.clone(),
                    execution_policy: invocation.execution_policy.clone(),
                    execution_domain: invocation.execution_domain,
                    effective_manifest: invocation.effective_manifest.clone(),
                    binding_resolutions: invocation.binding_resolutions.clone(),
                    policy_outcome: invocation.policy_outcome.clone(),
                    arguments: invocation.request.arguments.clone(),
                    output: None,
                    failure: Some(error.to_string()),
                    metadata: invocation.metadata.clone(),
                });
                store.append_audit(
                    &publication_ref,
                    Some(&invocation.invocation_id),
                    ProcedureAuditEventKind::InvocationFailed,
                    completed_at,
                    ExecutionOperation::ProcedureInvocation,
                    BTreeMap::from([("message".to_string(), JsonValue::String(error.to_string()))]),
                );
                return Err(error);
            }

            let mut metadata = invocation.metadata.clone();
            metadata.extend(output.metadata.clone());
            let receipt = ProcedureInvocationReceipt {
                invocation_id: invocation.invocation_id.clone(),
                publication: publication_ref.clone(),
                accepted_at: pending.recorded_at,
                completed_at,
                execution_policy: invocation.execution_policy.clone(),
                execution_domain: invocation.execution_domain,
                effective_manifest: invocation.effective_manifest.clone(),
                binding_resolutions: invocation.binding_resolutions.clone(),
                output: output.result.clone(),
                metadata: metadata.clone(),
            };
            store.append_invocation(ProcedureInvocationRecord {
                invocation_id: invocation.invocation_id.clone(),
                publication: publication_ref.clone(),
                state: ProcedureInvocationState::Succeeded,
                requested_at: pending.recorded_at,
                completed_at,
                caller: invocation.request.context.caller.clone(),
                execution_policy: invocation.execution_policy.clone(),
                execution_domain: invocation.execution_domain,
                effective_manifest: invocation.effective_manifest.clone(),
                binding_resolutions: invocation.binding_resolutions.clone(),
                policy_outcome: invocation.policy_outcome.clone(),
                arguments: invocation.request.arguments.clone(),
                output: Some(output.result),
                failure: None,
                metadata,
            });
            store.append_audit(
                &publication_ref,
                Some(&invocation.invocation_id),
                ProcedureAuditEventKind::InvocationSucceeded,
                completed_at,
                ExecutionOperation::ProcedureInvocation,
                BTreeMap::new(),
            );
            Ok(receipt)
        }
        Err(error) => {
            store.append_invocation(ProcedureInvocationRecord {
                invocation_id: invocation.invocation_id.clone(),
                publication: publication_ref.clone(),
                state: ProcedureInvocationState::Failed,
                requested_at: pending.recorded_at,
                completed_at,
                caller: invocation.request.context.caller.clone(),
                execution_policy: invocation.execution_policy.clone(),
                execution_domain: invocation.execution_domain,
                effective_manifest: invocation.effective_manifest.clone(),
                binding_resolutions: invocation.binding_resolutions.clone(),
                policy_outcome: invocation.policy_outcome.clone(),
                arguments: invocation.request.arguments.clone(),
                output: None,
                failure: Some(error.to_string()),
                metadata: invocation.metadata.clone(),
            });
            store.append_audit(
                &publication_ref,
                Some(&invocation.invocation_id),
                ProcedureAuditEventKind::InvocationFailed,
                completed_at,
                ExecutionOperation::ProcedureInvocation,
                BTreeMap::from([("message".to_string(), JsonValue::String(error.to_string()))]),
            );
            Err(error.into())
        }
    }
}

fn anonymous_subject() -> PolicySubject {
    PolicySubject {
        subject_id: "anonymous".to_string(),
        tenant_id: None,
        groups: Vec::new(),
        attributes: BTreeMap::new(),
    }
}

fn invocation_policy(
    deployment: &ProcedureDeployment,
    subject: PolicySubject,
    execution_policy: ExecutionPolicy,
    publication: &ReviewedProcedurePublication,
) -> ResolvedSessionPolicy {
    ResolvedSessionPolicy {
        subject: subject.clone(),
        session_mode: SessionMode::ReviewedProcedure,
        manifest: CapabilityManifest {
            subject: Some(subject),
            preset_name: None,
            profile_name: None,
            bindings: vec![deployment.invocation_binding.clone()],
            metadata: BTreeMap::from([(
                "deployment_id".to_string(),
                JsonValue::String(deployment.deployment_id.clone()),
            )]),
        },
        execution_policy: execution_policy.clone(),
        rate_limits: Vec::new(),
        budget_hooks: BTreeMap::from([(
            ExecutionOperation::ProcedureInvocation,
            terracedb_capabilities::BudgetAccountingHook {
                hook_name: format!("budget::{}", publication.publication.procedure_id),
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
            },
        )]),
    }
}

fn fallback_policy_outcome(
    publication: &ReviewedProcedurePublication,
    request: &ProcedureInvocationRequest,
    invocation_id: &str,
    requested_at: Timestamp,
    execution_domain: ExecutionDomain,
    message: String,
) -> PolicyDecisionRecord {
    let subject = request
        .context
        .caller
        .clone()
        .unwrap_or_else(anonymous_subject);
    let binding_name = "procedure.invoke.v1".to_string();
    PolicyDecisionRecord {
        outcome: terracedb_capabilities::PolicyOutcomeRecord {
            binding_name: binding_name.clone(),
            outcome: PolicyOutcomeKind::Denied,
            message: Some(message.clone()),
            observed_at: requested_at,
            row_visibility: None,
            metadata: BTreeMap::from([
                (
                    "invocation_id".to_string(),
                    JsonValue::String(invocation_id.to_string()),
                ),
                (
                    "execution_domain".to_string(),
                    json_value(&execution_domain),
                ),
            ]),
        },
        audit: terracedb_capabilities::PolicyAuditMetadata {
            session_id: request
                .context
                .session_id
                .clone()
                .unwrap_or_else(|| invocation_id.to_string()),
            session_mode: SessionMode::ReviewedProcedure,
            subject,
            binding_name,
            capability_family: Some(
                DatabaseCapabilityFamily::ProcedureInvokeV1
                    .as_str()
                    .to_string(),
            ),
            target_resources: vec![ResourceTarget {
                kind: ResourceKind::Procedure,
                identifier: publication.publication.procedure_id.clone(),
            }],
            execution_domain,
            placement_tags: Vec::new(),
            preset_name: None,
            profile_name: None,
            expanded_manifest: None,
            rate_limit: None,
            budget_hook: None,
            metadata: BTreeMap::new(),
        },
    }
}

fn base_invocation_metadata(
    publication: &ReviewedProcedurePublication,
    deployment: &ProcedureDeployment,
    request: &ProcedureInvocationRequest,
) -> BTreeMap<String, JsonValue> {
    let mut metadata = BTreeMap::from([
        (
            "deployment_id".to_string(),
            JsonValue::String(deployment.deployment_id.clone()),
        ),
        (
            "entrypoint".to_string(),
            JsonValue::String(publication.entrypoint.clone()),
        ),
        (
            "code_hash".to_string(),
            JsonValue::String(publication.code_hash.clone()),
        ),
        (
            "artifact_kind".to_string(),
            JsonValue::String(publication.artifact.kind().to_string()),
        ),
        (
            "dry_run".to_string(),
            JsonValue::Bool(request.context.dry_run),
        ),
    ]);
    metadata.extend(deployment.metadata.clone());
    metadata.extend(request.context.metadata.clone());
    metadata
}

fn apply_manifest_binding_overrides(
    bindings: &mut Vec<ManifestBinding>,
    overrides: &[ManifestBindingOverride],
    deployment_id: &str,
) -> Result<(), ProcedureError> {
    for override_spec in overrides {
        let Some(position) = bindings
            .iter()
            .position(|binding| binding.binding_name == override_spec.binding_name)
        else {
            return Err(ProcedureError::UnknownBindingOverride {
                deployment_id: deployment_id.to_string(),
                binding_name: override_spec.binding_name.clone(),
            });
        };

        if override_spec.drop_binding {
            bindings.remove(position);
            continue;
        }

        let binding = &mut bindings[position];
        if let Some(resource_policy) = override_spec.resource_policy.as_ref() {
            binding.resource_policy = intersect_resource_policy(
                &binding.resource_policy,
                resource_policy,
                deployment_id,
                &binding.binding_name,
            )?;
        }
        if let Some(budget_policy) = override_spec.budget_policy.as_ref() {
            binding.budget_policy = intersect_budget_policy(&binding.budget_policy, budget_policy);
        }
        if let Some(expose_in_just_bash) = override_spec.expose_in_just_bash {
            if expose_in_just_bash {
                if binding.shell_command.is_none() {
                    return Err(ProcedureError::UnsafeBindingOverride {
                        deployment_id: deployment_id.to_string(),
                        binding_name: binding.binding_name.clone(),
                        field: "shell_command".to_string(),
                    });
                }
            } else {
                binding.shell_command = None;
            }
        }
        binding.metadata.extend(override_spec.metadata.clone());
    }
    Ok(())
}

fn intersect_execution_policy(
    base: &ExecutionPolicy,
    requested: &ExecutionPolicy,
    deployment_id: &str,
) -> Result<ExecutionPolicy, ProcedureError> {
    let default_assignment = intersect_execution_assignment(
        &base.default_assignment,
        &requested.default_assignment,
        deployment_id,
        "default",
    )?;
    let mut operations = BTreeMap::new();
    for operation in ExecutionOperation::ALL {
        let effective = intersect_execution_assignment(
            base.assignment_for(operation),
            requested.assignment_for(operation),
            deployment_id,
            &format!("{operation:?}"),
        )?;
        if effective != default_assignment {
            operations.insert(operation, effective);
        }
    }

    let mut metadata = base.metadata.clone();
    metadata.extend(requested.metadata.clone());
    Ok(ExecutionPolicy {
        default_assignment,
        operations,
        metadata,
    })
}

fn intersect_execution_assignment(
    base: &ExecutionDomainAssignment,
    requested: &ExecutionDomainAssignment,
    deployment_id: &str,
    operation: &str,
) -> Result<ExecutionDomainAssignment, ProcedureError> {
    if base.domain != requested.domain {
        return Err(ProcedureError::ExecutionDomainMismatch {
            deployment_id: deployment_id.to_string(),
            operation: operation.to_string(),
            base: base.domain,
            requested: requested.domain,
        });
    }
    let mut placement_tags = base.placement_tags.clone();
    for tag in &requested.placement_tags {
        if !placement_tags.contains(tag) {
            placement_tags.push(tag.clone());
        }
    }
    let mut metadata = base.metadata.clone();
    metadata.extend(requested.metadata.clone());
    Ok(ExecutionDomainAssignment {
        domain: base.domain,
        budget: match (&base.budget, &requested.budget) {
            (Some(base), Some(requested)) => Some(intersect_budget_policy(base, requested)),
            (Some(base), None) => Some(base.clone()),
            (None, Some(requested)) => Some(requested.clone()),
            (None, None) => None,
        },
        placement_tags,
        metadata,
    })
}

fn intersect_resource_policy(
    base: &ResourcePolicy,
    requested: &ResourcePolicy,
    deployment_id: &str,
    binding_name: &str,
) -> Result<ResourcePolicy, ProcedureError> {
    let allow = match (base.allow.is_empty(), requested.allow.is_empty()) {
        (_, true) => base.allow.clone(),
        (true, false) => requested.allow.clone(),
        (false, false) => requested
            .allow
            .iter()
            .map(|selector| {
                if base
                    .allow
                    .iter()
                    .any(|base_selector| resource_selector_is_within(selector, base_selector))
                {
                    Ok(selector.clone())
                } else {
                    Err(ProcedureError::UnsafeBindingOverride {
                        deployment_id: deployment_id.to_string(),
                        binding_name: binding_name.to_string(),
                        field: format!(
                            "resource_policy.allow[{:?}:{}]",
                            selector.kind, selector.pattern
                        ),
                    })
                }
            })
            .collect::<Result<Vec<_>, _>>()?,
    };

    let mut deny = base.deny.clone();
    for selector in &requested.deny {
        if !deny.contains(selector) {
            deny.push(selector.clone());
        }
    }

    let tenant_scopes = intersect_string_scopes(&base.tenant_scopes, &requested.tenant_scopes);
    let row_scope_binding = match (&base.row_scope_binding, &requested.row_scope_binding) {
        (_, None) => base.row_scope_binding.clone(),
        (None, Some(binding)) => Some(binding.clone()),
        (Some(base_binding), Some(requested_binding)) if base_binding == requested_binding => {
            Some(base_binding.clone())
        }
        _ => {
            return Err(ProcedureError::UnsafeBindingOverride {
                deployment_id: deployment_id.to_string(),
                binding_name: binding_name.to_string(),
                field: "resource_policy.row_scope_binding".to_string(),
            });
        }
    };
    let visibility_index = match (&base.visibility_index, &requested.visibility_index) {
        (_, None) => base.visibility_index.clone(),
        (None, Some(index)) => Some(index.clone()),
        (Some(base_index), Some(requested_index)) if base_index == requested_index => {
            Some(base_index.clone())
        }
        _ => {
            return Err(ProcedureError::UnsafeBindingOverride {
                deployment_id: deployment_id.to_string(),
                binding_name: binding_name.to_string(),
                field: "resource_policy.visibility_index".to_string(),
            });
        }
    };

    let mut metadata = base.metadata.clone();
    metadata.extend(requested.metadata.clone());
    Ok(ResourcePolicy {
        allow,
        deny,
        tenant_scopes,
        row_scope_binding,
        visibility_index,
        metadata,
    })
}

fn intersect_budget_policy(base: &BudgetPolicy, requested: &BudgetPolicy) -> BudgetPolicy {
    let mut labels = base.labels.clone();
    labels.extend(requested.labels.clone());
    BudgetPolicy {
        max_calls: min_bound(base.max_calls, requested.max_calls),
        max_scanned_rows: min_bound(base.max_scanned_rows, requested.max_scanned_rows),
        max_returned_rows: min_bound(base.max_returned_rows, requested.max_returned_rows),
        max_bytes: min_bound(base.max_bytes, requested.max_bytes),
        max_millis: min_bound(base.max_millis, requested.max_millis),
        rate_limit_bucket: match (
            base.rate_limit_bucket.as_ref(),
            requested.rate_limit_bucket.as_ref(),
        ) {
            (Some(base_bucket), Some(requested_bucket)) if base_bucket == requested_bucket => {
                Some(base_bucket.clone())
            }
            (Some(base_bucket), Some(_)) => Some(base_bucket.clone()),
            (Some(base_bucket), None) => Some(base_bucket.clone()),
            (None, Some(requested_bucket)) => Some(requested_bucket.clone()),
            (None, None) => None,
        },
        labels,
    }
}

fn min_bound(base: Option<u64>, requested: Option<u64>) -> Option<u64> {
    match (base, requested) {
        (Some(base), Some(requested)) => Some(base.min(requested)),
        (Some(base), None) => Some(base),
        (None, Some(requested)) => Some(requested),
        (None, None) => None,
    }
}

fn resource_selector_is_within(candidate: &ResourceSelector, allowed: &ResourceSelector) -> bool {
    candidate.kind == allowed.kind
        && wildcard_pattern_is_within(&candidate.pattern, &allowed.pattern)
}

fn wildcard_pattern_is_within(candidate: &str, allowed: &str) -> bool {
    if candidate == allowed || allowed == "*" {
        return true;
    }
    if !candidate.contains('*') {
        return wildcard_pattern_matches(allowed, candidate);
    }
    if let Some(allowed_prefix) = allowed.strip_suffix('*')
        && !allowed_prefix.contains('*')
        && let Some(candidate_prefix) = candidate.strip_suffix('*')
        && !candidate_prefix.contains('*')
    {
        return candidate_prefix.starts_with(allowed_prefix);
    }
    if let Some(allowed_suffix) = allowed.strip_prefix('*')
        && !allowed_suffix.contains('*')
        && let Some(candidate_suffix) = candidate.strip_prefix('*')
        && !candidate_suffix.contains('*')
    {
        return candidate_suffix.ends_with(allowed_suffix);
    }
    false
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

fn intersect_string_scopes(base: &[String], requested: &[String]) -> Vec<String> {
    match (base.is_empty(), requested.is_empty()) {
        (true, true) => Vec::new(),
        (true, false) => requested.to_vec(),
        (false, true) => base.to_vec(),
        (false, false) => base
            .iter()
            .filter(|scope| requested.contains(scope))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

#[derive(Debug)]
struct ContractValidationError {
    path: String,
    message: String,
}

fn validate_contract_schema(
    schema: &JsonValue,
    value: &JsonValue,
) -> Result<(), ContractValidationError> {
    validate_contract_value(schema, value, "$")
}

fn validate_contract_value(
    schema: &JsonValue,
    value: &JsonValue,
    path: &str,
) -> Result<(), ContractValidationError> {
    match schema {
        JsonValue::Bool(true) | JsonValue::Null => return Ok(()),
        JsonValue::Bool(false) => {
            return Err(ContractValidationError {
                path: path.to_string(),
                message: "schema forbids every value".to_string(),
            });
        }
        JsonValue::Object(map) => {
            if let Some(expected) = map.get("const")
                && expected != value
            {
                return Err(ContractValidationError {
                    path: path.to_string(),
                    message: format!("expected const value {}, got {}", expected, value),
                });
            }
            if let Some(values) = map.get("enum") {
                let values = values.as_array().ok_or_else(|| ContractValidationError {
                    path: path.to_string(),
                    message: "enum must be an array".to_string(),
                })?;
                if !values.iter().any(|candidate| candidate == value) {
                    return Err(ContractValidationError {
                        path: path.to_string(),
                        message: "value is not in enum".to_string(),
                    });
                }
            }
            if let Some(type_constraint) = map.get("type")
                && !matches_type_constraint(type_constraint, value)?
            {
                return Err(ContractValidationError {
                    path: path.to_string(),
                    message: format!(
                        "expected type {}, got {}",
                        type_constraint,
                        json_type_name(value)
                    ),
                });
            }
            if let Some(properties) = map.get("properties")
                && let Some(object) = value.as_object()
            {
                let properties = properties
                    .as_object()
                    .ok_or_else(|| ContractValidationError {
                        path: path.to_string(),
                        message: "properties must be an object".to_string(),
                    })?;
                if let Some(required) = map.get("required") {
                    let required = required.as_array().ok_or_else(|| ContractValidationError {
                        path: path.to_string(),
                        message: "required must be an array".to_string(),
                    })?;
                    for field in required {
                        let field = field.as_str().ok_or_else(|| ContractValidationError {
                            path: path.to_string(),
                            message: "required entries must be strings".to_string(),
                        })?;
                        if !object.contains_key(field) {
                            return Err(ContractValidationError {
                                path: path.to_string(),
                                message: format!("missing required property {field}"),
                            });
                        }
                    }
                }
                for (field, field_schema) in properties {
                    if let Some(field_value) = object.get(field) {
                        validate_contract_value(
                            field_schema,
                            field_value,
                            &format!("{path}.{field}"),
                        )?;
                    }
                }
                if matches!(
                    map.get("additionalProperties"),
                    Some(JsonValue::Bool(false))
                ) {
                    for field in object.keys() {
                        if !properties.contains_key(field) {
                            return Err(ContractValidationError {
                                path: format!("{path}.{field}"),
                                message: "additional properties are not allowed".to_string(),
                            });
                        }
                    }
                }
            }
            if let Some(items) = map.get("items")
                && let Some(array) = value.as_array()
            {
                for (index, item) in array.iter().enumerate() {
                    validate_contract_value(items, item, &format!("{path}[{index}]"))?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn matches_type_constraint(
    type_constraint: &JsonValue,
    value: &JsonValue,
) -> Result<bool, ContractValidationError> {
    match type_constraint {
        JsonValue::String(kind) => Ok(matches_json_type(kind, value)?),
        JsonValue::Array(kinds) => {
            let mut any = false;
            for kind in kinds {
                let kind = kind.as_str().ok_or_else(|| ContractValidationError {
                    path: "$.type".to_string(),
                    message: "type arrays must contain only strings".to_string(),
                })?;
                any |= matches_json_type(kind, value)?;
            }
            Ok(any)
        }
        _ => Err(ContractValidationError {
            path: "$.type".to_string(),
            message: "type must be a string or array of strings".to_string(),
        }),
    }
}

fn matches_json_type(kind: &str, value: &JsonValue) -> Result<bool, ContractValidationError> {
    match kind {
        "null" => Ok(value.is_null()),
        "boolean" => Ok(value.is_boolean()),
        "string" => Ok(value.is_string()),
        "number" => Ok(value.is_number()),
        "integer" => Ok(value.as_i64().is_some()
            || value.as_u64().is_some()
            || value.as_f64().is_some_and(|number| number.fract() == 0.0)),
        "array" => Ok(value.is_array()),
        "object" => Ok(value.is_object()),
        other => Err(ContractValidationError {
            path: "$.type".to_string(),
            message: format!("unsupported type constraint {other}"),
        }),
    }
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(number) => {
            if number.as_i64().is_some() || number.as_u64().is_some() {
                "integer"
            } else {
                "number"
            }
        }
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn json_value<T>(value: &T) -> JsonValue
where
    T: Serialize,
{
    serde_json::to_value(value).unwrap_or(JsonValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use terracedb_capabilities::{
        DeterministicVisibilityIndexStore, RowDenialContract, RowQueryShape, RowScopeBinding,
        RowScopePolicy, RowWriteSemantics, VisibilityIndexSpec, VisibilityIndexSubjectKey,
        capability_module_specifier,
    };

    fn sample_execution_policy(
        domain: ExecutionDomain,
        max_calls: Option<u64>,
        placement_tags: &[&str],
    ) -> ExecutionPolicy {
        let assignment = ExecutionDomainAssignment {
            domain,
            budget: Some(BudgetPolicy {
                max_calls,
                max_scanned_rows: None,
                max_returned_rows: None,
                max_bytes: None,
                max_millis: None,
                rate_limit_bucket: None,
                labels: BTreeMap::new(),
            }),
            placement_tags: placement_tags
                .iter()
                .map(|tag| (*tag).to_string())
                .collect(),
            metadata: BTreeMap::new(),
        };
        ExecutionPolicy {
            default_assignment: assignment.clone(),
            operations: BTreeMap::from([(ExecutionOperation::ProcedureInvocation, assignment)]),
            metadata: BTreeMap::new(),
        }
    }

    fn sample_manifest(max_calls: Option<u64>) -> CapabilityManifest {
        CapabilityManifest {
            subject: None,
            preset_name: Some("reviewed-procedure".to_string()),
            profile_name: None,
            bindings: vec![ManifestBinding {
                binding_name: "tickets".to_string(),
                capability_family: "db.query.v1".to_string(),
                module_specifier: capability_module_specifier("tickets"),
                shell_command: None,
                resource_policy: ResourcePolicy {
                    allow: vec![ResourceSelector {
                        kind: ResourceKind::Table,
                        pattern: "tickets".to_string(),
                    }],
                    deny: vec![],
                    tenant_scopes: vec![],
                    row_scope_binding: Some(RowScopeBinding {
                        binding_id: "tickets.visible".to_string(),
                        policy: RowScopePolicy::VisibilityIndex {
                            index_name: "visible_by_subject".to_string(),
                        },
                        allowed_query_shapes: vec![
                            RowQueryShape::PointRead,
                            RowQueryShape::BoundedPrefixScan,
                        ],
                        write_semantics: RowWriteSemantics::default(),
                        denial_contract: RowDenialContract::default(),
                        metadata: BTreeMap::new(),
                    }),
                    visibility_index: Some(VisibilityIndexSpec {
                        index_name: "visible_by_subject".to_string(),
                        index_table: "visible_by_subject".to_string(),
                        subject_key: VisibilityIndexSubjectKey::Subject,
                        row_id_field: "row_id".to_string(),
                        membership_source: None,
                        read_mirror_table: None,
                        authoritative_sources: vec!["tickets".to_string()],
                        metadata: BTreeMap::new(),
                    }),
                    metadata: BTreeMap::new(),
                },
                budget_policy: BudgetPolicy {
                    max_calls,
                    max_scanned_rows: None,
                    max_returned_rows: None,
                    max_bytes: None,
                    max_millis: None,
                    rate_limit_bucket: None,
                    labels: BTreeMap::new(),
                },
                source_template_id: "db.query.v1".to_string(),
                source_grant_id: Some("grant-1".to_string()),
                allow_interactive_widening: false,
                metadata: BTreeMap::new(),
            }],
            metadata: BTreeMap::new(),
        }
    }

    fn sample_reviewed_draft(version: u64) -> ReviewedProcedureDraft {
        ReviewedProcedureDraft {
            draft: ProcedureDraft {
                publication: ProcedureVersionRef {
                    procedure_id: "sync-ticket".to_string(),
                    version,
                },
                source: ProcedureDraftSource::DraftSandboxSession {
                    session_id: "draft-session-1".to_string(),
                },
                entrypoint: "run".to_string(),
                requested_manifest: sample_manifest(Some(10)),
                execution_policy: sample_execution_policy(
                    ExecutionDomain::DedicatedSandbox,
                    Some(5),
                    &["reviewed"],
                ),
                input_schema: json!({
                    "type": "object",
                    "required": ["ticketId"],
                    "properties": {
                        "ticketId": { "type": "string" }
                    },
                    "additionalProperties": false
                }),
                output_schema: json!({
                    "type": "object",
                    "required": ["status"],
                    "properties": {
                        "status": { "type": "string" }
                    },
                    "additionalProperties": false
                }),
                metadata: BTreeMap::from([("draft".to_string(), json!("v1"))]),
            },
            review: ProcedureReview {
                reviewed_by: "reviewer".to_string(),
                source_revision: "abc123".to_string(),
                note: Some("looks good".to_string()),
                approved_at: Timestamp::new(10),
            },
            published_at: Timestamp::new(20),
            metadata: BTreeMap::from([("published".to_string(), JsonValue::Bool(true))]),
        }
    }

    fn sample_deployment(allowed_pattern: &str) -> ProcedureDeployment {
        ProcedureDeployment {
            deployment_id: "prod".to_string(),
            invocation_binding: ManifestBinding {
                binding_name: "procedures".to_string(),
                capability_family: DatabaseCapabilityFamily::ProcedureInvokeV1
                    .as_str()
                    .to_string(),
                module_specifier: capability_module_specifier("procedures"),
                shell_command: None,
                resource_policy: ResourcePolicy {
                    allow: vec![ResourceSelector {
                        kind: ResourceKind::Procedure,
                        pattern: allowed_pattern.to_string(),
                    }],
                    deny: vec![],
                    tenant_scopes: vec![],
                    row_scope_binding: None,
                    visibility_index: None,
                    metadata: BTreeMap::new(),
                },
                budget_policy: BudgetPolicy {
                    max_calls: Some(1),
                    max_scanned_rows: None,
                    max_returned_rows: None,
                    max_bytes: None,
                    max_millis: None,
                    rate_limit_bucket: None,
                    labels: BTreeMap::new(),
                },
                source_template_id: "procedure.invoke.v1".to_string(),
                source_grant_id: None,
                allow_interactive_widening: false,
                metadata: BTreeMap::new(),
            },
            execution_policy: sample_execution_policy(
                ExecutionDomain::DedicatedSandbox,
                Some(1),
                &["tenant-a"],
            ),
            binding_overrides: vec![ManifestBindingOverride {
                binding_name: "tickets".to_string(),
                drop_binding: false,
                resource_policy: None,
                budget_policy: Some(BudgetPolicy {
                    max_calls: Some(1),
                    max_scanned_rows: None,
                    max_returned_rows: None,
                    max_bytes: None,
                    max_millis: None,
                    rate_limit_bucket: None,
                    labels: BTreeMap::new(),
                }),
                expose_in_just_bash: None,
                metadata: BTreeMap::new(),
            }],
            metadata: BTreeMap::new(),
        }
    }

    fn sample_request(subject_id: &str) -> ProcedureInvocationRequest {
        ProcedureInvocationRequest {
            publication: ProcedureVersionRef {
                procedure_id: "sync-ticket".to_string(),
                version: 1,
            },
            arguments: json!({ "ticketId": "t-1" }),
            context: ProcedureInvocationContext {
                caller: Some(PolicySubject {
                    subject_id: subject_id.to_string(),
                    tenant_id: Some("tenant-a".to_string()),
                    groups: vec![],
                    attributes: BTreeMap::new(),
                }),
                session_mode: SessionMode::ReviewedProcedure,
                session_id: Some("invoke-session".to_string()),
                dry_run: false,
                metadata: BTreeMap::new(),
            },
        }
    }

    fn publish_v1(store: &mut DeterministicProcedurePublicationStore) {
        let mut publisher = DeterministicProcedureArtifactPublisher::default().with_snapshot(
            "draft-session-1",
            "snapshot-1",
            "sha256:abc123",
        );
        publish_reviewed_procedure(store, &mut publisher, sample_reviewed_draft(1))
            .expect("publish reviewed procedure");
    }

    #[test]
    fn publish_reviewed_procedure_freezes_draft_and_records_audits() {
        let mut store = DeterministicProcedurePublicationStore::default();
        let mut publisher = DeterministicProcedureArtifactPublisher::default().with_snapshot(
            "draft-session-1",
            "snapshot-1",
            "sha256:abc123",
        );

        let receipt =
            publish_reviewed_procedure(&mut store, &mut publisher, sample_reviewed_draft(1))
                .expect("publish");
        let published = store
            .load(&receipt.publication)
            .expect("published procedure should be stored");

        assert_eq!(publisher.freeze_count("draft-session-1"), 1);
        assert_eq!(receipt.code_hash, "sha256:abc123");
        assert_eq!(published.entrypoint, "run");
        assert_eq!(
            published.artifact,
            ProcedureImmutableArtifact::SandboxSnapshot {
                snapshot_id: "snapshot-1".to_string()
            }
        );
        assert_eq!(published.metadata.get("draft"), Some(&json!("v1")));
        assert_eq!(
            published.metadata.get("published"),
            Some(&JsonValue::Bool(true))
        );
        assert_eq!(
            store
                .audits_for_publication(&receipt.publication)
                .iter()
                .map(|record| record.event.clone())
                .collect::<Vec<_>>(),
            vec![
                ProcedureAuditEventKind::ReviewApproved,
                ProcedureAuditEventKind::PublicationStarted,
                ProcedureAuditEventKind::PublicationSucceeded,
            ]
        );
    }

    #[test]
    fn pending_publication_fails_closed_until_finish() {
        let mut store = DeterministicProcedurePublicationStore::default();
        let mut publisher = DeterministicProcedureArtifactPublisher::default().with_snapshot(
            "draft-session-1",
            "snapshot-1",
            "sha256:abc123",
        );
        let publication = begin_reviewed_procedure_publication(
            &mut store,
            &mut publisher,
            sample_reviewed_draft(1),
        )
        .expect("begin publication");

        let error = prepare_published_procedure_invocation(
            &mut store,
            &sample_deployment("sync-*"),
            sample_request("user:alice"),
            Timestamp::new(30),
            &DeterministicVisibilityIndexStore::default(),
        )
        .expect_err("pending publication should fail closed");
        assert!(matches!(
            error,
            ProcedureError::PendingPublication {
                procedure_id,
                version
            } if procedure_id == "sync-ticket" && version == 1
        ));
        assert!(store.pending_publication(&publication).is_some());

        finish_reviewed_procedure_publication(&mut store, &publication)
            .expect("finish publication");
        assert!(store.pending_publication(&publication).is_none());
        assert!(store.load(&publication).is_some());
    }

    #[test]
    fn pending_publication_recovery_fails_closed_until_completion_after_restore() {
        let mut store = DeterministicProcedurePublicationStore::default();
        let mut publisher = DeterministicProcedureArtifactPublisher::default().with_snapshot(
            "draft-session-1",
            "snapshot-1",
            "sha256:abc123",
        );
        let publication = begin_reviewed_procedure_publication(
            &mut store,
            &mut publisher,
            sample_reviewed_draft(1),
        )
        .expect("begin publication");

        let restored = DeterministicProcedurePublicationStore::restore_from_json(
            store.snapshot_json().expect("snapshot store"),
        )
        .expect("restore pending store");
        let mut restored = restored;

        let error = prepare_published_procedure_invocation(
            &mut restored,
            &sample_deployment("sync-*"),
            sample_request("user:alice"),
            Timestamp::new(30),
            &DeterministicVisibilityIndexStore::default(),
        )
        .expect_err("pending publication should fail closed after restore");
        assert!(matches!(
            error,
            ProcedureError::PendingPublication {
                procedure_id,
                version
            } if procedure_id == "sync-ticket" && version == 1
        ));

        let receipt = finish_reviewed_procedure_publication(&mut restored, &publication)
            .expect("finish after restore");
        assert_eq!(receipt.publication, publication);
        assert!(restored.pending_publication(&publication).is_none());
        assert!(restored.load(&publication).is_some());
    }

    #[test]
    fn invoke_published_procedure_applies_deployment_overrides_and_row_scope_resolution() {
        let mut store = DeterministicProcedurePublicationStore::default();
        publish_v1(&mut store);

        let visibility = DeterministicVisibilityIndexStore::default().with_visible_row(
            "visible_by_subject",
            "user:alice",
            "ticket:t-1",
        );
        let deployment = sample_deployment("sync-*");
        let publication = ProcedureVersionRef {
            procedure_id: "sync-ticket".to_string(),
            version: 1,
        };
        let mut runtime = DeterministicReviewedProcedureRuntime::default()
            .with_result(publication.clone(), json!({ "status": "queued" }));

        let receipt = invoke_published_procedure(
            &mut store,
            &mut runtime,
            &deployment,
            sample_request("user:alice"),
            Timestamp::new(31),
            Timestamp::new(32),
            &visibility,
        )
        .expect("invoke published procedure");

        assert_eq!(receipt.output, json!({ "status": "queued" }));
        let prepared = runtime
            .last_invocation()
            .expect("runtime should observe invocation");
        assert_eq!(
            prepared
                .effective_manifest
                .subject
                .as_ref()
                .map(|subject| subject.subject_id.as_str()),
            Some("user:alice")
        );
        assert_eq!(
            prepared.effective_manifest.bindings[0]
                .budget_policy
                .max_calls,
            Some(1)
        );
        assert_eq!(
            prepared
                .execution_policy
                .assignment_for(ExecutionOperation::ProcedureInvocation)
                .placement_tags,
            vec!["reviewed".to_string(), "tenant-a".to_string()]
        );
        assert_eq!(
            prepared.binding_resolutions[0]
                .effective_row_scope
                .as_ref()
                .and_then(|binding| binding.visibility_lookup.as_ref())
                .map(|lookup| lookup.visible_row_ids.clone()),
            Some(vec!["ticket:t-1".to_string()])
        );
        assert_eq!(runtime.invocation_count(&publication), 1);
        assert_eq!(
            store.invocations_for_publication(&publication)[0].state,
            ProcedureInvocationState::Succeeded
        );
    }

    #[test]
    fn denied_invocations_record_history_without_runtime_execution() {
        let mut store = DeterministicProcedurePublicationStore::default();
        publish_v1(&mut store);
        let publication = ProcedureVersionRef {
            procedure_id: "sync-ticket".to_string(),
            version: 1,
        };
        let mut runtime = DeterministicReviewedProcedureRuntime::default()
            .with_result(publication.clone(), json!({ "status": "queued" }));

        let error = invoke_published_procedure(
            &mut store,
            &mut runtime,
            &sample_deployment("other-*"),
            sample_request("user:alice"),
            Timestamp::new(40),
            Timestamp::new(41),
            &DeterministicVisibilityIndexStore::default(),
        )
        .expect_err("deployment should deny invocation");

        assert!(matches!(
            error,
            ProcedureError::InvocationDenied {
                procedure_id,
                version,
                ..
            } if procedure_id == "sync-ticket" && version == 1
        ));
        assert_eq!(runtime.invocation_count(&publication), 0);
        let history = store.invocations_for_publication(&publication);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].state, ProcedureInvocationState::Denied);
        assert_eq!(
            history[0].policy_outcome.outcome.outcome,
            PolicyOutcomeKind::Denied
        );
    }

    #[test]
    fn incomplete_and_invalid_output_invocations_fail_closed_and_clear_pending_state() {
        let mut store = DeterministicProcedurePublicationStore::default();
        publish_v1(&mut store);

        let visibility = DeterministicVisibilityIndexStore::default().with_visible_row(
            "visible_by_subject",
            "user:alice",
            "ticket:t-1",
        );
        let prepared = prepare_published_procedure_invocation(
            &mut store,
            &sample_deployment("sync-*"),
            sample_request("user:alice"),
            Timestamp::new(50),
            &visibility,
        )
        .expect("prepare invocation");
        assert!(store.pending_invocation(&prepared.invocation_id).is_some());

        let error = complete_published_procedure_invocation(
            &mut store,
            &prepared.invocation_id,
            Ok(ProcedureRuntimeOutput {
                result: json!({ "wrong": "shape" }),
                metadata: BTreeMap::new(),
            }),
            Timestamp::new(51),
        )
        .expect_err("invalid output should fail");

        assert!(matches!(
            error,
            ProcedureError::SchemaValidation { phase, .. } if phase == "output"
        ));
        assert!(store.pending_invocation(&prepared.invocation_id).is_none());
        let history = store.invocations_for_publication(&prepared.publication.publication);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].state, ProcedureInvocationState::Failed);
    }

    #[test]
    fn pending_invocation_recovery_preserves_bookkeeping_and_allows_completion() {
        let mut store = DeterministicProcedurePublicationStore::default();
        publish_v1(&mut store);

        let visibility = DeterministicVisibilityIndexStore::default().with_visible_row(
            "visible_by_subject",
            "user:alice",
            "ticket:t-1",
        );
        let prepared = prepare_published_procedure_invocation(
            &mut store,
            &sample_deployment("sync-*"),
            sample_request("user:alice"),
            Timestamp::new(60),
            &visibility,
        )
        .expect("prepare invocation");
        assert!(store.pending_invocation(&prepared.invocation_id).is_some());

        let restored = DeterministicProcedurePublicationStore::restore_from_json(
            store.snapshot_json().expect("snapshot invocation state"),
        )
        .expect("restore invocation state");
        let mut restored = restored;
        assert!(
            restored
                .pending_invocation(&prepared.invocation_id)
                .is_some()
        );

        let receipt = complete_published_procedure_invocation(
            &mut restored,
            &prepared.invocation_id,
            Ok(ProcedureRuntimeOutput {
                result: json!({ "status": "queued" }),
                metadata: BTreeMap::new(),
            }),
            Timestamp::new(61),
        )
        .expect("complete invocation after restore");

        assert_eq!(receipt.invocation_id, prepared.invocation_id);
        assert!(
            restored
                .pending_invocation(&prepared.invocation_id)
                .is_none()
        );
        let history = restored.invocations_for_publication(&prepared.publication.publication);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].state, ProcedureInvocationState::Succeeded);
        assert_eq!(history[0].output, Some(json!({ "status": "queued" })));
    }
}
