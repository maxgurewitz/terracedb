//! Reviewed migration contracts and deterministic helpers for catalog-scoped migrations.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::{SchemaDefinition, SequenceNumber, TableFormat, TableMetadata, Timestamp};
use terracedb_capabilities::{
    CapabilityManifest, DatabaseCapabilityFamily, ExecutionDomain, ExecutionOperation,
    ExecutionPolicy, ManifestBinding, ResourceTarget, capability_module_specifier,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStepKind {
    CatalogBootstrap,
    SchemaChange,
    DataBackfillReviewOnly,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationStep {
    pub step_id: String,
    pub label: String,
    pub module_specifier: String,
    pub checksum: String,
    pub kind: MigrationStepKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_bindings: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub plan_id: String,
    pub application_id: String,
    pub created_at: Timestamp,
    pub requested_manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<MigrationStep>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationState {
    Planned,
    Applied,
    Skipped,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationHistoryEntry {
    pub plan_id: String,
    pub step_id: String,
    pub state: MigrationState,
    pub applied_sequence: Option<SequenceNumber>,
    pub recorded_at: Timestamp,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationReview {
    pub reviewed_by: String,
    pub source_revision: String,
    pub note: Option<String>,
    pub approved_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewedMigrationPlan {
    pub plan: MigrationPlan,
    pub review: MigrationReview,
    pub published_at: Timestamp,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MigrationPlanRef {
    pub application_id: String,
    pub plan_id: String,
}

impl From<&MigrationPlan> for MigrationPlanRef {
    fn from(plan: &MigrationPlan) -> Self {
        Self {
            application_id: plan.application_id.clone(),
            plan_id: plan.plan_id.clone(),
        }
    }
}

impl From<&ReviewedMigrationPlan> for MigrationPlanRef {
    fn from(reviewed: &ReviewedMigrationPlan) -> Self {
        Self::from(&reviewed.plan)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationAuditEventKind {
    ReviewApproved,
    PublicationStarted,
    PublicationSucceeded,
    ApplyStarted,
    ApplySucceeded,
    ApplySkipped,
    ApplyFailed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationAuditRecord {
    pub audit_id: String,
    pub application_id: String,
    pub plan_id: String,
    pub step_id: Option<String>,
    pub event: MigrationAuditEventKind,
    pub recorded_at: Timestamp,
    pub execution_operation: ExecutionOperation,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationPublicationReceipt {
    pub plan: MigrationPlanRef,
    pub published_at: Timestamp,
    pub execution_policy: ExecutionPolicy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationApplicationReceipt {
    pub plan: MigrationPlanRef,
    pub applied_at: Timestamp,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub applied_steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub noop_steps: Vec<String>,
    pub execution_policy: ExecutionPolicy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnsureTableRequest {
    pub table: String,
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<TableFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaDefinition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnsureTableResult {
    pub applied: bool,
    pub table: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstallSchemaSuccessorRequest {
    pub table: String,
    pub database: Option<String>,
    pub successor: SchemaDefinition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSchemaSuccessorResult {
    pub applied: bool,
    pub table: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateTableMetadataRequest {
    pub table: String,
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: TableMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateTableMetadataResult {
    pub applied: bool,
    pub table: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MigrationPrecondition {
    TableExists,
    TableMissing,
    SchemaVersion {
        version: u32,
    },
    MetadataEquals {
        key: String,
        value: JsonValue,
    },
    All {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        checks: Vec<MigrationPrecondition>,
    },
    Any {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        checks: Vec<MigrationPrecondition>,
    },
    Not {
        check: Box<MigrationPrecondition>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CheckPreconditionRequest {
    pub table: Option<String>,
    pub database: Option<String>,
    pub precondition: MigrationPrecondition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckPreconditionResult {
    pub ok: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreconditionFailurePolicy {
    SkipStep,
    FailStep,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MigrationModuleAction {
    EnsureTable {
        binding_name: String,
        request: EnsureTableRequest,
    },
    InstallSchemaSuccessor {
        binding_name: String,
        request: InstallSchemaSuccessorRequest,
    },
    UpdateTableMetadata {
        binding_name: String,
        request: UpdateTableMetadataRequest,
    },
    CheckPrecondition {
        binding_name: String,
        request: CheckPreconditionRequest,
        on_false: PreconditionFailurePolicy,
    },
}

impl MigrationModuleAction {
    pub fn ensure_table(binding_name: impl Into<String>, request: EnsureTableRequest) -> Self {
        Self::EnsureTable {
            binding_name: binding_name.into(),
            request,
        }
    }

    pub fn install_schema_successor(
        binding_name: impl Into<String>,
        request: InstallSchemaSuccessorRequest,
    ) -> Self {
        Self::InstallSchemaSuccessor {
            binding_name: binding_name.into(),
            request,
        }
    }

    pub fn update_table_metadata(
        binding_name: impl Into<String>,
        request: UpdateTableMetadataRequest,
    ) -> Self {
        Self::UpdateTableMetadata {
            binding_name: binding_name.into(),
            request,
        }
    }

    pub fn require_precondition(
        binding_name: impl Into<String>,
        request: CheckPreconditionRequest,
        on_false: PreconditionFailurePolicy,
    ) -> Self {
        Self::CheckPrecondition {
            binding_name: binding_name.into(),
            request,
            on_false,
        }
    }

    pub fn binding_name(&self) -> &str {
        match self {
            Self::EnsureTable { binding_name, .. }
            | Self::InstallSchemaSuccessor { binding_name, .. }
            | Self::UpdateTableMetadata { binding_name, .. }
            | Self::CheckPrecondition { binding_name, .. } => binding_name,
        }
    }

    fn resource_targets(&self) -> Vec<ResourceTarget> {
        let (database, table) = match self {
            Self::EnsureTable { request, .. } => (request.database.as_ref(), Some(&request.table)),
            Self::InstallSchemaSuccessor { request, .. } => {
                (request.database.as_ref(), Some(&request.table))
            }
            Self::UpdateTableMetadata { request, .. } => {
                (request.database.as_ref(), Some(&request.table))
            }
            Self::CheckPrecondition { request, .. } => {
                (request.database.as_ref(), request.table.as_ref())
            }
        };

        let mut targets = Vec::new();
        if let Some(database) = database {
            targets.push(ResourceTarget {
                kind: terracedb_capabilities::ResourceKind::Database,
                identifier: database.clone(),
            });
        }
        if let Some(table) = table {
            targets.push(ResourceTarget {
                kind: terracedb_capabilities::ResourceKind::Table,
                identifier: table.clone(),
            });
        }
        targets
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MigrationModuleActionResult {
    EnsureTable(EnsureTableResult),
    InstallSchemaSuccessor(InstallSchemaSuccessorResult),
    UpdateTableMetadata(UpdateTableMetadataResult),
    CheckPrecondition(CheckPreconditionResult),
}

pub trait ReviewedMigrationRuntime {
    fn execute_step(
        &mut self,
        reviewed: &ReviewedMigrationPlan,
        step: &MigrationStep,
    ) -> Result<Vec<MigrationModuleAction>, MigrationError>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeterministicMigrationModuleBehavior {
    Actions {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        actions: Vec<MigrationModuleAction>,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicReviewedMigrationRuntime {
    modules: BTreeMap<String, DeterministicMigrationModuleBehavior>,
    invocations: BTreeMap<String, u64>,
}

impl DeterministicReviewedMigrationRuntime {
    pub fn with_actions(
        mut self,
        module_specifier: impl Into<String>,
        actions: Vec<MigrationModuleAction>,
    ) -> Self {
        self.modules.insert(
            module_specifier.into(),
            DeterministicMigrationModuleBehavior::Actions { actions },
        );
        self
    }

    pub fn with_error(
        mut self,
        module_specifier: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        self.modules.insert(
            module_specifier.into(),
            DeterministicMigrationModuleBehavior::Error {
                message: message.into(),
            },
        );
        self
    }

    pub fn invocation_count(&self, module_specifier: &str) -> u64 {
        self.invocations
            .get(module_specifier)
            .copied()
            .unwrap_or_default()
    }
}

impl ReviewedMigrationRuntime for DeterministicReviewedMigrationRuntime {
    fn execute_step(
        &mut self,
        _reviewed: &ReviewedMigrationPlan,
        step: &MigrationStep,
    ) -> Result<Vec<MigrationModuleAction>, MigrationError> {
        self.invocations
            .entry(step.module_specifier.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        match self.modules.get(&step.module_specifier) {
            Some(DeterministicMigrationModuleBehavior::Actions { actions }) => Ok(actions.clone()),
            Some(DeterministicMigrationModuleBehavior::Error { message }) => {
                Err(MigrationError::Runtime {
                    module_specifier: step.module_specifier.clone(),
                    message: message.clone(),
                })
            }
            None => Err(MigrationError::Runtime {
                module_specifier: step.module_specifier.clone(),
                message: "no deterministic module behavior registered".to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogTableState {
    pub table: String,
    pub database: Option<String>,
    pub format: TableFormat,
    pub schema: Option<SchemaDefinition>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: TableMetadata,
}

pub trait MigrationCatalog {
    fn ensure_table(
        &mut self,
        request: EnsureTableRequest,
    ) -> Result<EnsureTableResult, MigrationError>;

    fn install_schema_successor(
        &mut self,
        request: InstallSchemaSuccessorRequest,
    ) -> Result<InstallSchemaSuccessorResult, MigrationError>;

    fn update_table_metadata(
        &mut self,
        request: UpdateTableMetadataRequest,
    ) -> Result<UpdateTableMetadataResult, MigrationError>;

    fn check_precondition(
        &self,
        request: CheckPreconditionRequest,
    ) -> Result<CheckPreconditionResult, MigrationError>;

    fn load_table(&self, database: Option<&str>, table: &str) -> Option<CatalogTableState>;
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CatalogTableKey {
    database: Option<String>,
    table: String,
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicMigrationCatalog {
    tables: BTreeMap<CatalogTableKey, CatalogTableState>,
}

impl DeterministicMigrationCatalog {
    pub fn tables(&self) -> Vec<CatalogTableState> {
        self.tables.values().cloned().collect()
    }

    pub fn ensure_table(
        &mut self,
        request: EnsureTableRequest,
    ) -> Result<EnsureTableResult, MigrationError> {
        <Self as MigrationCatalog>::ensure_table(self, request)
    }

    pub fn install_schema_successor(
        &mut self,
        request: InstallSchemaSuccessorRequest,
    ) -> Result<InstallSchemaSuccessorResult, MigrationError> {
        <Self as MigrationCatalog>::install_schema_successor(self, request)
    }

    pub fn update_table_metadata(
        &mut self,
        request: UpdateTableMetadataRequest,
    ) -> Result<UpdateTableMetadataResult, MigrationError> {
        <Self as MigrationCatalog>::update_table_metadata(self, request)
    }

    pub fn check_precondition(
        &self,
        request: CheckPreconditionRequest,
    ) -> Result<CheckPreconditionResult, MigrationError> {
        <Self as MigrationCatalog>::check_precondition(self, request)
    }

    pub fn load_table(&self, database: Option<&str>, table: &str) -> Option<CatalogTableState> {
        <Self as MigrationCatalog>::load_table(self, database, table)
    }

    fn key(database: Option<&str>, table: &str) -> CatalogTableKey {
        CatalogTableKey {
            database: database.map(ToOwned::to_owned),
            table: table.to_string(),
        }
    }
}

impl MigrationCatalog for DeterministicMigrationCatalog {
    fn ensure_table(
        &mut self,
        request: EnsureTableRequest,
    ) -> Result<EnsureTableResult, MigrationError> {
        let key = Self::key(request.database.as_deref(), &request.table);
        let format = resolve_table_format(&request)?;
        if let Some(existing) = self.tables.get(&key) {
            if existing.format != format || existing.schema != request.schema {
                return Err(MigrationError::TableShapeMismatch {
                    table: request.table,
                    message: "ensure_table is only idempotent for identical reviewed table shapes"
                        .to_string(),
                });
            }
            return Ok(EnsureTableResult {
                applied: false,
                table: existing.table.clone(),
            });
        }

        if let Some(schema) = request.schema.as_ref() {
            schema
                .validate()
                .map_err(|error| MigrationError::SchemaValidation {
                    table: request.table.clone(),
                    message: error.to_string(),
                })?;
        }

        self.tables.insert(
            key,
            CatalogTableState {
                table: request.table.clone(),
                database: request.database,
                format,
                schema: request.schema,
                metadata: BTreeMap::new(),
            },
        );
        Ok(EnsureTableResult {
            applied: true,
            table: request.table,
        })
    }

    fn install_schema_successor(
        &mut self,
        request: InstallSchemaSuccessorRequest,
    ) -> Result<InstallSchemaSuccessorResult, MigrationError> {
        let key = Self::key(request.database.as_deref(), &request.table);
        let Some(existing) = self.tables.get_mut(&key) else {
            return Err(MigrationError::TableNotFound {
                table: request.table,
            });
        };
        if existing.format != TableFormat::Columnar {
            return Err(MigrationError::TableShapeMismatch {
                table: existing.table.clone(),
                message: "schema successors require a columnar table".to_string(),
            });
        }

        let Some(current_schema) = existing.schema.as_ref() else {
            return Err(MigrationError::TableShapeMismatch {
                table: existing.table.clone(),
                message: "schema successors require an existing schema".to_string(),
            });
        };
        if current_schema == &request.successor {
            return Ok(InstallSchemaSuccessorResult {
                applied: false,
                table: existing.table.clone(),
            });
        }

        current_schema
            .validate_successor(&request.successor)
            .map_err(|error| MigrationError::SchemaValidation {
                table: existing.table.clone(),
                message: error.to_string(),
            })?;
        existing.schema = Some(request.successor);
        Ok(InstallSchemaSuccessorResult {
            applied: true,
            table: existing.table.clone(),
        })
    }

    fn update_table_metadata(
        &mut self,
        request: UpdateTableMetadataRequest,
    ) -> Result<UpdateTableMetadataResult, MigrationError> {
        let key = Self::key(request.database.as_deref(), &request.table);
        let Some(existing) = self.tables.get_mut(&key) else {
            return Err(MigrationError::TableNotFound {
                table: request.table,
            });
        };

        let mut applied = false;
        for (key, value) in request.metadata {
            match existing.metadata.get(&key) {
                Some(current) if current == &value => {}
                _ => {
                    existing.metadata.insert(key, value);
                    applied = true;
                }
            }
        }

        Ok(UpdateTableMetadataResult {
            applied,
            table: existing.table.clone(),
        })
    }

    fn check_precondition(
        &self,
        request: CheckPreconditionRequest,
    ) -> Result<CheckPreconditionResult, MigrationError> {
        let key = request
            .table
            .as_ref()
            .map(|table| Self::key(request.database.as_deref(), table));
        let table = key.and_then(|key| self.tables.get(&key));
        Ok(CheckPreconditionResult {
            ok: evaluate_precondition(table, &request.precondition),
        })
    }

    fn load_table(&self, database: Option<&str>, table: &str) -> Option<CatalogTableState> {
        self.tables.get(&Self::key(database, table)).cloned()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingMigrationPublication {
    pub plan: ReviewedMigrationPlan,
    pub recorded_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingMigrationStep {
    pub plan: MigrationPlanRef,
    pub step_id: String,
    pub module_specifier: String,
    pub recorded_at: Timestamp,
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicMigrationStore {
    published: BTreeMap<MigrationPlanRef, ReviewedMigrationPlan>,
    history: BTreeMap<MigrationPlanRef, Vec<MigrationHistoryEntry>>,
    audits: BTreeMap<MigrationPlanRef, Vec<MigrationAuditRecord>>,
    pending_publications: BTreeMap<MigrationPlanRef, PendingMigrationPublication>,
    pending_steps: BTreeMap<MigrationPlanRef, PendingMigrationStep>,
}

impl DeterministicMigrationStore {
    pub fn load(&self, plan: &MigrationPlanRef) -> Option<ReviewedMigrationPlan> {
        self.published.get(plan).cloned()
    }

    pub fn list_published_for_application(
        &self,
        application_id: &str,
    ) -> Vec<ReviewedMigrationPlan> {
        self.published
            .iter()
            .filter(|(plan, _)| plan.application_id == application_id)
            .map(|(_, reviewed)| reviewed.clone())
            .collect()
    }

    pub fn history_for_plan(&self, plan: &MigrationPlanRef) -> Vec<MigrationHistoryEntry> {
        self.history.get(plan).cloned().unwrap_or_default()
    }

    pub fn audits_for_plan(&self, plan: &MigrationPlanRef) -> Vec<MigrationAuditRecord> {
        self.audits.get(plan).cloned().unwrap_or_default()
    }

    pub fn pending_publication(
        &self,
        plan: &MigrationPlanRef,
    ) -> Option<PendingMigrationPublication> {
        self.pending_publications.get(plan).cloned()
    }

    pub fn pending_step(&self, plan: &MigrationPlanRef) -> Option<PendingMigrationStep> {
        self.pending_steps.get(plan).cloned()
    }

    pub fn record_incomplete_publication(&mut self, pending: PendingMigrationPublication) {
        self.pending_publications
            .insert(MigrationPlanRef::from(&pending.plan), pending);
    }

    pub fn record_incomplete_apply(&mut self, pending: PendingMigrationStep) {
        self.pending_steps.insert(pending.plan.clone(), pending);
    }

    fn append_audit(&mut self, plan: &MigrationPlanRef, record: MigrationAuditRecord) {
        self.audits.entry(plan.clone()).or_default().push(record);
    }

    fn append_history(&mut self, plan: &MigrationPlanRef, entry: MigrationHistoryEntry) {
        self.history.entry(plan.clone()).or_default().push(entry);
    }

    fn begin_publication(
        &mut self,
        pending: PendingMigrationPublication,
    ) -> Result<(), MigrationError> {
        let plan = MigrationPlanRef::from(&pending.plan);
        if self.pending_publications.contains_key(&plan) {
            return Err(MigrationError::PendingPublication {
                application_id: plan.application_id,
                plan_id: plan.plan_id,
            });
        }
        self.pending_publications.insert(plan, pending);
        Ok(())
    }

    fn clear_publication(&mut self, plan: &MigrationPlanRef) {
        self.pending_publications.remove(plan);
    }

    fn begin_step(&mut self, pending: PendingMigrationStep) -> Result<(), MigrationError> {
        if let Some(existing) = self
            .pending_steps
            .insert(pending.plan.clone(), pending.clone())
        {
            return Err(MigrationError::PendingApply {
                application_id: existing.plan.application_id,
                plan_id: existing.plan.plan_id,
                step_id: existing.step_id,
            });
        }
        Ok(())
    }

    fn clear_step(&mut self, plan: &MigrationPlanRef) {
        self.pending_steps.remove(plan);
    }
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("reviewed migration plan {application_id}/{plan_id} has not been published")]
    PlanNotPublished {
        application_id: String,
        plan_id: String,
    },
    #[error("reviewed migration plan {application_id}/{plan_id} is already published")]
    DuplicatePlan {
        application_id: String,
        plan_id: String,
    },
    #[error(
        "reviewed migration plan {application_id}/{plan_id} has incomplete publication metadata and must fail closed"
    )]
    PendingPublication {
        application_id: String,
        plan_id: String,
    },
    #[error(
        "reviewed migration plan {application_id}/{plan_id} has incomplete apply metadata for step {step_id} and must fail closed"
    )]
    PendingApply {
        application_id: String,
        plan_id: String,
        step_id: String,
    },
    #[error("migration plan_id must not be empty")]
    EmptyPlanId,
    #[error("migration application_id must not be empty")]
    EmptyApplicationId,
    #[error("migration plan {plan_id} must contain at least one step")]
    EmptyPlan { plan_id: String },
    #[error("migration plan {plan_id} contains duplicate binding {binding_name}")]
    DuplicateBinding {
        plan_id: String,
        binding_name: String,
    },
    #[error("migration plan {plan_id} contains duplicate step {step_id}")]
    DuplicateStep { plan_id: String, step_id: String },
    #[error(
        "migration plan {plan_id} binding {binding_name} must use capability family catalog.migrate.v1, got {capability_family}"
    )]
    BindingFamilyNotCatalog {
        plan_id: String,
        binding_name: String,
        capability_family: String,
    },
    #[error(
        "migration plan {plan_id} binding {binding_name} must use host module specifier {expected_specifier}, got {actual_specifier}"
    )]
    BindingSpecifierMismatch {
        plan_id: String,
        binding_name: String,
        expected_specifier: String,
        actual_specifier: String,
    },
    #[error(
        "migration step {step_id} in plan {plan_id} must not use version-1 unsupported step kind {kind:?}"
    )]
    UnsupportedStepKind {
        plan_id: String,
        step_id: String,
        kind: MigrationStepKind,
    },
    #[error("migration step {step_id} in plan {plan_id} must have a non-empty checksum")]
    EmptyChecksum { plan_id: String, step_id: String },
    #[error("migration step {step_id} in plan {plan_id} requested unknown binding {binding_name}")]
    UnknownRequestedBinding {
        plan_id: String,
        step_id: String,
        binding_name: String,
    },
    #[error(
        "migration step {step_id} in plan {plan_id} attempted to use unrequested binding {binding_name}"
    )]
    BindingNotRequested {
        plan_id: String,
        step_id: String,
        binding_name: String,
    },
    #[error(
        "migration step {step_id} in plan {plan_id} attempted to escape catalog-scoped authority for target {target_identifier} on binding {binding_name}"
    )]
    BindingTargetDenied {
        plan_id: String,
        step_id: String,
        binding_name: String,
        target_identifier: String,
    },
    #[error(
        "migration plan {plan_id} must route {operation:?} through a non-foreground control-plane lane"
    )]
    InvalidExecutionPolicy {
        plan_id: String,
        operation: ExecutionOperation,
    },
    #[error("reviewed migration metadata must include a non-empty reviewer identity")]
    EmptyReviewer,
    #[error("reviewed migration metadata must include a non-empty source revision")]
    EmptySourceRevision,
    #[error(
        "migration step {step_id} in plan {plan_id} has already failed and will not be replayed without a new reviewed plan"
    )]
    StepPreviouslyFailed { plan_id: String, step_id: String },
    #[error("catalog table {table} was not found")]
    TableNotFound { table: String },
    #[error("catalog table {table} does not match the reviewed migration shape: {message}")]
    TableShapeMismatch { table: String, message: String },
    #[error("catalog schema successor validation failed for table {table}: {message}")]
    SchemaValidation { table: String, message: String },
    #[error("reviewed migration module {module_specifier} failed: {message}")]
    Runtime {
        module_specifier: String,
        message: String,
    },
    #[error("migration precondition failed for step {step_id} in plan {plan_id}: {message}")]
    PreconditionFailed {
        plan_id: String,
        step_id: String,
        message: String,
    },
}

pub fn publish_reviewed_plan(
    store: &mut DeterministicMigrationStore,
    reviewed: ReviewedMigrationPlan,
) -> Result<MigrationPublicationReceipt, MigrationError> {
    let plan = begin_reviewed_plan_publication(store, reviewed)?;
    finish_reviewed_plan_publication(store, &plan)
}

pub fn begin_reviewed_plan_publication(
    store: &mut DeterministicMigrationStore,
    reviewed: ReviewedMigrationPlan,
) -> Result<MigrationPlanRef, MigrationError> {
    reviewed.validate_reviewed_surface()?;
    let plan = MigrationPlanRef::from(&reviewed);
    if store.published.contains_key(&plan) {
        return Err(MigrationError::DuplicatePlan {
            application_id: plan.application_id.clone(),
            plan_id: plan.plan_id.clone(),
        });
    }

    store.begin_publication(PendingMigrationPublication {
        plan: reviewed.clone(),
        recorded_at: reviewed.published_at,
    })?;
    store.append_audit(
        &plan,
        audit_record(
            &reviewed.plan,
            None,
            MigrationAuditEventKind::ReviewApproved,
            reviewed.review.approved_at,
            ExecutionOperation::MigrationPublication,
            BTreeMap::from([
                (
                    "reviewed_by".to_string(),
                    JsonValue::String(reviewed.review.reviewed_by.clone()),
                ),
                (
                    "source_revision".to_string(),
                    JsonValue::String(reviewed.review.source_revision.clone()),
                ),
            ]),
        ),
    );
    store.append_audit(
        &plan,
        audit_record(
            &reviewed.plan,
            None,
            MigrationAuditEventKind::PublicationStarted,
            reviewed.published_at,
            ExecutionOperation::MigrationPublication,
            BTreeMap::new(),
        ),
    );

    Ok(plan)
}

pub fn finish_reviewed_plan_publication(
    store: &mut DeterministicMigrationStore,
    plan: &MigrationPlanRef,
) -> Result<MigrationPublicationReceipt, MigrationError> {
    let pending =
        store
            .pending_publication(plan)
            .ok_or_else(|| MigrationError::PlanNotPublished {
                application_id: plan.application_id.clone(),
                plan_id: plan.plan_id.clone(),
            })?;
    if store.published.contains_key(plan) {
        return Err(MigrationError::DuplicatePlan {
            application_id: plan.application_id.clone(),
            plan_id: plan.plan_id.clone(),
        });
    }

    let reviewed = pending.plan;
    store.published.insert(plan.clone(), reviewed.clone());
    store.append_audit(
        plan,
        audit_record(
            &reviewed.plan,
            None,
            MigrationAuditEventKind::PublicationSucceeded,
            reviewed.published_at,
            ExecutionOperation::MigrationPublication,
            BTreeMap::new(),
        ),
    );
    store.clear_publication(plan);

    Ok(MigrationPublicationReceipt {
        plan: plan.clone(),
        published_at: reviewed.published_at,
        execution_policy: reviewed.plan.execution_policy,
    })
}

pub fn apply_published_plan<R, C>(
    store: &mut DeterministicMigrationStore,
    runtime: &mut R,
    catalog: &mut C,
    plan: &MigrationPlanRef,
    applied_at: Timestamp,
) -> Result<MigrationApplicationReceipt, MigrationError>
where
    R: ReviewedMigrationRuntime,
    C: MigrationCatalog + Clone,
{
    if store.pending_publications.contains_key(plan) {
        return Err(MigrationError::PendingPublication {
            application_id: plan.application_id.clone(),
            plan_id: plan.plan_id.clone(),
        });
    }
    if let Some(pending) = store.pending_steps.get(plan) {
        return Err(MigrationError::PendingApply {
            application_id: pending.plan.application_id.clone(),
            plan_id: pending.plan.plan_id.clone(),
            step_id: pending.step_id.clone(),
        });
    }

    let reviewed = store
        .load(plan)
        .ok_or_else(|| MigrationError::PlanNotPublished {
            application_id: plan.application_id.clone(),
            plan_id: plan.plan_id.clone(),
        })?;

    let mut latest_history = BTreeMap::new();
    for entry in store.history_for_plan(plan) {
        latest_history.insert(entry.step_id.clone(), entry);
    }

    let mut applied_steps = Vec::new();
    let mut skipped_steps = Vec::new();
    let mut noop_steps = Vec::new();

    'step_loop: for step in &reviewed.plan.steps {
        if let Some(existing) = latest_history.get(&step.step_id) {
            match existing.state {
                MigrationState::Applied | MigrationState::Skipped => {
                    noop_steps.push(step.step_id.clone());
                    continue;
                }
                MigrationState::Failed => {
                    return Err(MigrationError::StepPreviouslyFailed {
                        plan_id: reviewed.plan.plan_id.clone(),
                        step_id: step.step_id.clone(),
                    });
                }
                MigrationState::Planned => {}
            }
        }

        store.begin_step(PendingMigrationStep {
            plan: plan.clone(),
            step_id: step.step_id.clone(),
            module_specifier: step.module_specifier.clone(),
            recorded_at: applied_at,
        })?;
        store.append_audit(
            plan,
            audit_record(
                &reviewed.plan,
                Some(&step.step_id),
                MigrationAuditEventKind::ApplyStarted,
                applied_at,
                ExecutionOperation::MigrationApply,
                BTreeMap::from([(
                    "module_specifier".to_string(),
                    JsonValue::String(step.module_specifier.clone()),
                )]),
            ),
        );

        let actions = match runtime.execute_step(&reviewed, step) {
            Ok(actions) => actions,
            Err(error) => {
                record_failed_step(store, &reviewed.plan, step, applied_at, error.to_string());
                return Err(error);
            }
        };

        let mut staged_catalog = catalog.clone();
        for action in actions {
            if let Err(error) = validate_action_authority(&reviewed.plan, step, &action) {
                record_failed_step(store, &reviewed.plan, step, applied_at, error.to_string());
                return Err(error);
            }

            match execute_action(&mut staged_catalog, action.clone()) {
                Ok(MigrationModuleActionResult::CheckPrecondition(result)) if !result.ok => {
                    let (binding_name, on_false) = match action {
                        MigrationModuleAction::CheckPrecondition {
                            binding_name,
                            on_false,
                            ..
                        } => (binding_name, on_false),
                        _ => unreachable!(),
                    };
                    match on_false {
                        PreconditionFailurePolicy::SkipStep => {
                            let metadata = BTreeMap::from([
                                ("binding_name".to_string(), JsonValue::String(binding_name)),
                                (
                                    "reason".to_string(),
                                    JsonValue::String(
                                        "precondition returned false; reviewed step skipped"
                                            .to_string(),
                                    ),
                                ),
                            ]);
                            let history_entry = MigrationHistoryEntry {
                                plan_id: reviewed.plan.plan_id.clone(),
                                step_id: step.step_id.clone(),
                                state: MigrationState::Skipped,
                                applied_sequence: None,
                                recorded_at: applied_at,
                                metadata: metadata.clone(),
                            };
                            store.append_history(plan, history_entry);
                            store.append_audit(
                                plan,
                                audit_record(
                                    &reviewed.plan,
                                    Some(&step.step_id),
                                    MigrationAuditEventKind::ApplySkipped,
                                    applied_at,
                                    ExecutionOperation::MigrationApply,
                                    metadata,
                                ),
                            );
                            store.clear_step(plan);
                            latest_history.insert(
                                step.step_id.clone(),
                                MigrationHistoryEntry {
                                    plan_id: reviewed.plan.plan_id.clone(),
                                    step_id: step.step_id.clone(),
                                    state: MigrationState::Skipped,
                                    applied_sequence: None,
                                    recorded_at: applied_at,
                                    metadata: BTreeMap::new(),
                                },
                            );
                            skipped_steps.push(step.step_id.clone());
                            continue 'step_loop;
                        }
                        PreconditionFailurePolicy::FailStep => {
                            let error = MigrationError::PreconditionFailed {
                                plan_id: reviewed.plan.plan_id.clone(),
                                step_id: step.step_id.clone(),
                                message: "reviewed precondition returned false".to_string(),
                            };
                            record_failed_step(
                                store,
                                &reviewed.plan,
                                step,
                                applied_at,
                                error.to_string(),
                            );
                            return Err(error);
                        }
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    record_failed_step(store, &reviewed.plan, step, applied_at, error.to_string());
                    return Err(error);
                }
            }
        }
        *catalog = staged_catalog;

        let history_entry = MigrationHistoryEntry {
            plan_id: reviewed.plan.plan_id.clone(),
            step_id: step.step_id.clone(),
            state: MigrationState::Applied,
            applied_sequence: None,
            recorded_at: applied_at,
            metadata: BTreeMap::new(),
        };
        store.append_history(plan, history_entry.clone());
        store.append_audit(
            plan,
            audit_record(
                &reviewed.plan,
                Some(&step.step_id),
                MigrationAuditEventKind::ApplySucceeded,
                applied_at,
                ExecutionOperation::MigrationApply,
                BTreeMap::new(),
            ),
        );
        store.clear_step(plan);
        latest_history.insert(step.step_id.clone(), history_entry);
        applied_steps.push(step.step_id.clone());
    }

    Ok(MigrationApplicationReceipt {
        plan: plan.clone(),
        applied_at,
        applied_steps,
        skipped_steps,
        noop_steps,
        execution_policy: reviewed.plan.execution_policy,
    })
}

impl MigrationPlan {
    pub fn reference(&self) -> MigrationPlanRef {
        MigrationPlanRef::from(self)
    }

    pub fn binding(&self, binding_name: &str) -> Option<&ManifestBinding> {
        self.requested_manifest
            .bindings
            .iter()
            .find(|binding| binding.binding_name == binding_name)
    }

    pub fn validate_reviewed_surface(&self) -> Result<(), MigrationError> {
        if self.plan_id.trim().is_empty() {
            return Err(MigrationError::EmptyPlanId);
        }
        if self.application_id.trim().is_empty() {
            return Err(MigrationError::EmptyApplicationId);
        }
        if self.steps.is_empty() {
            return Err(MigrationError::EmptyPlan {
                plan_id: self.plan_id.clone(),
            });
        }

        validate_control_plane_policy(&self.plan_id, &self.execution_policy)?;

        let mut seen_bindings = BTreeSet::new();
        for binding in &self.requested_manifest.bindings {
            if !seen_bindings.insert(binding.binding_name.clone()) {
                return Err(MigrationError::DuplicateBinding {
                    plan_id: self.plan_id.clone(),
                    binding_name: binding.binding_name.clone(),
                });
            }
            if binding.capability_family != DatabaseCapabilityFamily::CatalogMigrateV1.as_str() {
                return Err(MigrationError::BindingFamilyNotCatalog {
                    plan_id: self.plan_id.clone(),
                    binding_name: binding.binding_name.clone(),
                    capability_family: binding.capability_family.clone(),
                });
            }
            let expected_specifier = capability_module_specifier(&binding.binding_name);
            if binding.module_specifier != expected_specifier {
                return Err(MigrationError::BindingSpecifierMismatch {
                    plan_id: self.plan_id.clone(),
                    binding_name: binding.binding_name.clone(),
                    expected_specifier,
                    actual_specifier: binding.module_specifier.clone(),
                });
            }
        }

        let mut seen_steps = BTreeSet::new();
        for step in &self.steps {
            if !seen_steps.insert(step.step_id.clone()) {
                return Err(MigrationError::DuplicateStep {
                    plan_id: self.plan_id.clone(),
                    step_id: step.step_id.clone(),
                });
            }
            if step.checksum.trim().is_empty() {
                return Err(MigrationError::EmptyChecksum {
                    plan_id: self.plan_id.clone(),
                    step_id: step.step_id.clone(),
                });
            }
            if matches!(step.kind, MigrationStepKind::DataBackfillReviewOnly) {
                return Err(MigrationError::UnsupportedStepKind {
                    plan_id: self.plan_id.clone(),
                    step_id: step.step_id.clone(),
                    kind: step.kind.clone(),
                });
            }
            let mut seen_requested = BTreeSet::new();
            for binding_name in &step.requested_bindings {
                if !seen_requested.insert(binding_name.clone()) {
                    return Err(MigrationError::BindingNotRequested {
                        plan_id: self.plan_id.clone(),
                        step_id: step.step_id.clone(),
                        binding_name: binding_name.clone(),
                    });
                }
                if self.binding(binding_name).is_none() {
                    return Err(MigrationError::UnknownRequestedBinding {
                        plan_id: self.plan_id.clone(),
                        step_id: step.step_id.clone(),
                        binding_name: binding_name.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}

impl ReviewedMigrationPlan {
    pub fn validate_reviewed_surface(&self) -> Result<(), MigrationError> {
        self.plan.validate_reviewed_surface()?;
        if self.review.reviewed_by.trim().is_empty() {
            return Err(MigrationError::EmptyReviewer);
        }
        if self.review.source_revision.trim().is_empty() {
            return Err(MigrationError::EmptySourceRevision);
        }
        Ok(())
    }
}

fn resolve_table_format(request: &EnsureTableRequest) -> Result<TableFormat, MigrationError> {
    match (request.format, request.schema.as_ref()) {
        (Some(TableFormat::Row), Some(_)) => Err(MigrationError::TableShapeMismatch {
            table: request.table.clone(),
            message: "row tables do not accept a columnar schema".to_string(),
        }),
        (Some(TableFormat::Columnar), None) => Err(MigrationError::TableShapeMismatch {
            table: request.table.clone(),
            message: "columnar tables require a schema".to_string(),
        }),
        (Some(format), _) => Ok(format),
        (None, Some(_)) => Ok(TableFormat::Columnar),
        (None, None) => Ok(TableFormat::Row),
    }
}

fn evaluate_precondition(
    table: Option<&CatalogTableState>,
    precondition: &MigrationPrecondition,
) -> bool {
    match precondition {
        MigrationPrecondition::TableExists => table.is_some(),
        MigrationPrecondition::TableMissing => table.is_none(),
        MigrationPrecondition::SchemaVersion { version } => table
            .and_then(|table| table.schema.as_ref())
            .is_some_and(|schema| schema.version == *version),
        MigrationPrecondition::MetadataEquals { key, value } => table
            .and_then(|table| table.metadata.get(key))
            .is_some_and(|current| current == value),
        MigrationPrecondition::All { checks } => checks
            .iter()
            .all(|check| evaluate_precondition(table, check)),
        MigrationPrecondition::Any { checks } => checks
            .iter()
            .any(|check| evaluate_precondition(table, check)),
        MigrationPrecondition::Not { check } => !evaluate_precondition(table, check),
    }
}

fn validate_control_plane_policy(
    plan_id: &str,
    policy: &ExecutionPolicy,
) -> Result<(), MigrationError> {
    for operation in [
        ExecutionOperation::MigrationPublication,
        ExecutionOperation::MigrationApply,
    ] {
        let assignment = policy.assignment_for(operation);
        let has_control_plane = assignment
            .placement_tags
            .iter()
            .any(|tag| tag == "control-plane");
        if assignment.domain == ExecutionDomain::OwnerForeground || !has_control_plane {
            return Err(MigrationError::InvalidExecutionPolicy {
                plan_id: plan_id.to_string(),
                operation,
            });
        }
    }
    Ok(())
}

fn validate_action_authority(
    plan: &MigrationPlan,
    step: &MigrationStep,
    action: &MigrationModuleAction,
) -> Result<(), MigrationError> {
    let binding_name = action.binding_name();
    if !step
        .requested_bindings
        .iter()
        .any(|name| name == binding_name)
    {
        return Err(MigrationError::BindingNotRequested {
            plan_id: plan.plan_id.clone(),
            step_id: step.step_id.clone(),
            binding_name: binding_name.to_string(),
        });
    }
    let binding =
        plan.binding(binding_name)
            .ok_or_else(|| MigrationError::UnknownRequestedBinding {
                plan_id: plan.plan_id.clone(),
                step_id: step.step_id.clone(),
                binding_name: binding_name.to_string(),
            })?;

    for target in action.resource_targets() {
        let denied = binding
            .resource_policy
            .deny
            .iter()
            .any(|selector| selector.matches_target(&target));
        let allowed = binding.resource_policy.allow.is_empty()
            || binding
                .resource_policy
                .allow
                .iter()
                .any(|selector| selector.matches_target(&target));
        if denied || !allowed {
            return Err(MigrationError::BindingTargetDenied {
                plan_id: plan.plan_id.clone(),
                step_id: step.step_id.clone(),
                binding_name: binding_name.to_string(),
                target_identifier: target.identifier,
            });
        }
    }

    Ok(())
}

fn execute_action<C>(
    catalog: &mut C,
    action: MigrationModuleAction,
) -> Result<MigrationModuleActionResult, MigrationError>
where
    C: MigrationCatalog,
{
    match action {
        MigrationModuleAction::EnsureTable { request, .. } => catalog
            .ensure_table(request)
            .map(MigrationModuleActionResult::EnsureTable),
        MigrationModuleAction::InstallSchemaSuccessor { request, .. } => catalog
            .install_schema_successor(request)
            .map(MigrationModuleActionResult::InstallSchemaSuccessor),
        MigrationModuleAction::UpdateTableMetadata { request, .. } => catalog
            .update_table_metadata(request)
            .map(MigrationModuleActionResult::UpdateTableMetadata),
        MigrationModuleAction::CheckPrecondition { request, .. } => catalog
            .check_precondition(request)
            .map(MigrationModuleActionResult::CheckPrecondition),
    }
}

fn record_failed_step(
    store: &mut DeterministicMigrationStore,
    plan: &MigrationPlan,
    step: &MigrationStep,
    recorded_at: Timestamp,
    message: String,
) {
    let plan_ref = plan.reference();
    let metadata = BTreeMap::from([("message".to_string(), JsonValue::String(message))]);
    store.append_history(
        &plan_ref,
        MigrationHistoryEntry {
            plan_id: plan.plan_id.clone(),
            step_id: step.step_id.clone(),
            state: MigrationState::Failed,
            applied_sequence: None,
            recorded_at,
            metadata: metadata.clone(),
        },
    );
    store.append_audit(
        &plan_ref,
        audit_record(
            plan,
            Some(&step.step_id),
            MigrationAuditEventKind::ApplyFailed,
            recorded_at,
            ExecutionOperation::MigrationApply,
            metadata,
        ),
    );
    store.clear_step(&plan_ref);
}

fn audit_record(
    plan: &MigrationPlan,
    step_id: Option<&str>,
    event: MigrationAuditEventKind,
    recorded_at: Timestamp,
    execution_operation: ExecutionOperation,
    metadata: BTreeMap<String, JsonValue>,
) -> MigrationAuditRecord {
    let audit_id = match step_id {
        Some(step_id) => format!(
            "{}/{}/{:?}/{step_id}/{}",
            plan.application_id,
            plan.plan_id,
            execution_operation,
            recorded_at.get()
        ),
        None => format!(
            "{}/{}/{:?}/{}",
            plan.application_id,
            plan.plan_id,
            execution_operation,
            recorded_at.get()
        ),
    };
    MigrationAuditRecord {
        audit_id,
        application_id: plan.application_id.clone(),
        plan_id: plan.plan_id.clone(),
        step_id: step_id.map(ToOwned::to_owned),
        event,
        recorded_at,
        execution_operation,
        metadata,
    }
}
