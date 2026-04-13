//! Frozen policy and capability contracts shared by sandbox, migrations, procedures, and MCP.
//!
//! The key rule in this crate is intentionally simple:
//!
//! - capability policy constrains authority,
//! - execution-domain policy constrains placement and resource consumption.
//!
//! The two layers compose, but neither replaces the other.

use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PolicyContext {
    pub subject_id: String,
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, JsonValue>,
}

impl From<&PolicySubject> for PolicyContext {
    fn from(subject: &PolicySubject) -> Self {
        Self {
            subject_id: subject.subject_id.clone(),
            tenant_id: subject.tenant_id.clone(),
            group_ids: subject.groups.clone(),
            attributes: subject
                .attributes
                .iter()
                .map(|(key, value)| (key.clone(), JsonValue::String(value.clone())))
                .collect(),
        }
    }
}

impl From<PolicySubject> for PolicyContext {
    fn from(subject: PolicySubject) -> Self {
        Self::from(&subject)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowQueryShape {
    PointRead,
    BoundedPrefixScan,
    WriteMutation,
    MultiTableQuery,
    Aggregate,
}

impl RowQueryShape {
    pub const ALL: [Self; 5] = [
        Self::PointRead,
        Self::BoundedPrefixScan,
        Self::WriteMutation,
        Self::MultiTableQuery,
        Self::Aggregate,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseActionKind {
    PointRead,
    BoundedPrefixScan,
    WriteMutation,
    MultiTableQuery,
    Aggregate,
    SchemaMutation,
    ProcedureInvocation,
}

impl DatabaseActionKind {
    pub fn row_query_shape(self) -> Option<RowQueryShape> {
        match self {
            Self::PointRead => Some(RowQueryShape::PointRead),
            Self::BoundedPrefixScan => Some(RowQueryShape::BoundedPrefixScan),
            Self::WriteMutation => Some(RowQueryShape::WriteMutation),
            Self::MultiTableQuery => Some(RowQueryShape::MultiTableQuery),
            Self::Aggregate => Some(RowQueryShape::Aggregate),
            Self::SchemaMutation | Self::ProcedureInvocation => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseCapabilityFamily {
    DbTableV1,
    DbQueryV1,
    CatalogMigrateV1,
    ProcedureInvokeV1,
}

impl DatabaseCapabilityFamily {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "db.table.v1" => Some(Self::DbTableV1),
            "db.query.v1" => Some(Self::DbQueryV1),
            "catalog.migrate.v1" => Some(Self::CatalogMigrateV1),
            "procedure.invoke.v1" => Some(Self::ProcedureInvokeV1),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::DbTableV1 => "db.table.v1",
            Self::DbQueryV1 => "db.query.v1",
            Self::CatalogMigrateV1 => "catalog.migrate.v1",
            Self::ProcedureInvokeV1 => "procedure.invoke.v1",
        }
    }

    pub fn allows_action(self, action: DatabaseActionKind) -> bool {
        match self {
            Self::DbTableV1 => matches!(
                action,
                DatabaseActionKind::PointRead
                    | DatabaseActionKind::BoundedPrefixScan
                    | DatabaseActionKind::WriteMutation
            ),
            Self::DbQueryV1 => matches!(
                action,
                DatabaseActionKind::PointRead
                    | DatabaseActionKind::BoundedPrefixScan
                    | DatabaseActionKind::WriteMutation
            ),
            Self::CatalogMigrateV1 => action == DatabaseActionKind::SchemaMutation,
            Self::ProcedureInvokeV1 => action == DatabaseActionKind::ProcedureInvocation,
        }
    }

    pub fn generated_methods(self) -> &'static [&'static str] {
        match self {
            Self::DbTableV1 => &["get", "scanPrefix", "put", "delete"],
            Self::DbQueryV1 => &["get", "scanPrefix", "put", "delete"],
            Self::CatalogMigrateV1 => &[
                "ensureTable",
                "installSchemaSuccessor",
                "updateTableMetadata",
                "checkPrecondition",
            ],
            Self::ProcedureInvokeV1 => &["invoke"],
        }
    }

    pub fn generated_typescript_declarations(self, specifier: &str) -> String {
        match self {
            Self::DbTableV1 => format!(
                r#"declare module "{specifier}" {{
  export interface GetRequest {{
    key: string;
  }}
  export interface ScanPrefixRequest {{
    prefix?: string;
    limit?: number;
    resumeToken?: string | null;
  }}
  export interface PutRequest {{
    key: string;
    row: unknown;
    occReadSet?: string[];
  }}
  export interface DeleteRequest {{
    key: string;
    occReadSet?: string[];
  }}
  export function get(request: GetRequest): {{ key: string; row: unknown | null }};
  export function scanPrefix(request: ScanPrefixRequest): {{ rows: Array<{{ key: string; row: unknown }}>; scannedRows: number; returnedRows: number; resumeToken?: unknown }};
  export function put(request: PutRequest): {{ key: string; row: unknown; written: boolean }};
  export function delete(request: DeleteRequest): {{ key: string; deleted: boolean }};
}}"#
            ),
            Self::DbQueryV1 => format!(
                r#"declare module "{specifier}" {{
  export interface GetRequest {{
    table: string;
    database?: string;
    key: string;
  }}
  export interface ScanPrefixRequest {{
    table: string;
    database?: string;
    prefix?: string;
    limit?: number;
    resumeToken?: string | null;
  }}
  export interface PutRequest {{
    table: string;
    database?: string;
    key: string;
    row: unknown;
    occReadSet?: string[];
  }}
  export interface DeleteRequest {{
    table: string;
    database?: string;
    key: string;
    occReadSet?: string[];
  }}
  export function get(request: GetRequest): {{ key: string; row: unknown | null }};
  export function scanPrefix(request: ScanPrefixRequest): {{ rows: Array<{{ key: string; row: unknown }}>; scannedRows: number; returnedRows: number; resumeToken?: unknown }};
  export function put(request: PutRequest): {{ key: string; row: unknown; written: boolean }};
  export function delete(request: DeleteRequest): {{ key: string; deleted: boolean }};
}}"#
            ),
            Self::CatalogMigrateV1 => format!(
                r#"declare module "{specifier}" {{
  export interface EnsureTableRequest {{
    table: string;
    database?: string;
    schema?: unknown;
  }}
  export interface InstallSchemaSuccessorRequest {{
    table: string;
    database?: string;
    successor: unknown;
  }}
  export interface UpdateTableMetadataRequest {{
    table: string;
    database?: string;
    metadata: Record<string, unknown>;
  }}
  export interface CheckPreconditionRequest {{
    table?: string;
    database?: string;
    precondition: unknown;
  }}
  export function ensureTable(request: EnsureTableRequest): {{ applied: boolean; table: string }};
  export function installSchemaSuccessor(request: InstallSchemaSuccessorRequest): {{ applied: boolean; table: string }};
  export function updateTableMetadata(request: UpdateTableMetadataRequest): {{ applied: boolean; table: string }};
  export function checkPrecondition(request: CheckPreconditionRequest): {{ ok: boolean }};
}}"#
            ),
            Self::ProcedureInvokeV1 => format!(
                r#"declare module "{specifier}" {{
  export interface InvokeRequest {{
    procedureId?: string;
    arguments: unknown;
  }}
  export function invoke(request: InvokeRequest): {{ procedureId: string; result: unknown }};
}}"#
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowScopeFamily {
    KeyPrefix,
    TypedRowPredicate,
    VisibilityIndex,
    ProcedureOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VisibilityIndexSubjectKey {
    Subject,
    Tenant,
    Group,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VisibilityIndexSpec {
    pub index_name: String,
    pub index_table: String,
    pub subject_key: VisibilityIndexSubjectKey,
    pub row_id_field: String,
    pub membership_source: Option<String>,
    pub read_mirror_table: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authoritative_sources: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl VisibilityIndexSpec {
    pub fn lookup_keys(&self, context: &PolicyContext) -> Vec<String> {
        match self.subject_key {
            VisibilityIndexSubjectKey::Subject => {
                if context.subject_id.is_empty() {
                    Vec::new()
                } else {
                    vec![context.subject_id.clone()]
                }
            }
            VisibilityIndexSubjectKey::Tenant => context
                .tenant_id
                .iter()
                .filter(|tenant_id| !tenant_id.is_empty())
                .cloned()
                .collect(),
            VisibilityIndexSubjectKey::Group => context
                .group_ids
                .iter()
                .filter(|group_id| !group_id.is_empty())
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum RowScopePolicy {
    KeyPrefix {
        prefix_template: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        context_fields: Vec<String>,
    },
    TypedRowPredicate {
        predicate_id: String,
        row_type: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        referenced_fields: Vec<String>,
    },
    VisibilityIndex {
        index_name: String,
    },
    ProcedureOnly {
        reason: Option<String>,
    },
}

impl RowScopePolicy {
    pub fn family(&self) -> RowScopeFamily {
        match self {
            Self::KeyPrefix { .. } => RowScopeFamily::KeyPrefix,
            Self::TypedRowPredicate { .. } => RowScopeFamily::TypedRowPredicate,
            Self::VisibilityIndex { .. } => RowScopeFamily::VisibilityIndex,
            Self::ProcedureOnly { .. } => RowScopeFamily::ProcedureOnly,
        }
    }

    pub fn supported_query_shapes(&self) -> &'static [RowQueryShape] {
        match self {
            Self::KeyPrefix { .. } => &[
                RowQueryShape::PointRead,
                RowQueryShape::BoundedPrefixScan,
                RowQueryShape::WriteMutation,
            ],
            Self::TypedRowPredicate { .. } => &[
                RowQueryShape::PointRead,
                RowQueryShape::BoundedPrefixScan,
                RowQueryShape::WriteMutation,
            ],
            Self::VisibilityIndex { .. } => {
                &[RowQueryShape::PointRead, RowQueryShape::BoundedPrefixScan]
            }
            Self::ProcedureOnly { .. } => &[],
        }
    }

    pub fn supports_query_shape(&self, shape: RowQueryShape) -> bool {
        self.supported_query_shapes().contains(&shape)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowBudgetAccounting {
    #[serde(default = "default_true")]
    pub charge_scanned_rows: bool,
    #[serde(default = "default_true")]
    pub charge_returned_rows: bool,
}

impl Default for RowBudgetAccounting {
    fn default() -> Self {
        Self {
            charge_scanned_rows: true,
            charge_returned_rows: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowWriteSemantics {
    #[serde(default = "default_true")]
    pub evaluate_preimage: bool,
    #[serde(default = "default_true")]
    pub evaluate_postimage: bool,
    #[serde(default = "default_true")]
    pub reject_scope_escaping_writes: bool,
    #[serde(default = "default_true")]
    pub require_occ_read_set: bool,
    #[serde(default)]
    pub budget_accounting: RowBudgetAccounting,
}

impl Default for RowWriteSemantics {
    fn default() -> Self {
        Self {
            evaluate_preimage: true,
            evaluate_postimage: true,
            reject_scope_escaping_writes: true,
            require_occ_read_set: true,
            budget_accounting: RowBudgetAccounting::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowVisibilityOutcomeKind {
    Visible,
    NotVisible,
    ExplicitlyDenied,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowDenialContract {
    #[serde(default = "default_true")]
    pub conceal_not_visible_as_not_found: bool,
    #[serde(default = "default_true")]
    pub explicit_denial_reveals_reason: bool,
}

impl Default for RowDenialContract {
    fn default() -> Self {
        Self {
            conceal_not_visible_as_not_found: true,
            explicit_denial_reveals_reason: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowVisibilityAuditRecord {
    pub outcome: RowVisibilityOutcomeKind,
    pub family: RowScopeFamily,
    pub query_shape: RowQueryShape,
    pub surface_as_not_found: bool,
    pub index_name: Option<String>,
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowScopeBinding {
    pub binding_id: String,
    pub policy: RowScopePolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_query_shapes: Vec<RowQueryShape>,
    #[serde(default)]
    pub write_semantics: RowWriteSemantics,
    #[serde(default)]
    pub denial_contract: RowDenialContract,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl RowScopeBinding {
    pub fn effective_query_shapes(&self) -> Vec<RowQueryShape> {
        let supported = self.policy.supported_query_shapes();
        if self.allowed_query_shapes.is_empty() {
            return supported.to_vec();
        }

        supported
            .iter()
            .copied()
            .filter(|shape| self.allowed_query_shapes.contains(shape))
            .collect()
    }

    pub fn supports_query_shape(&self, shape: RowQueryShape) -> bool {
        self.policy.supports_query_shape(shape)
            && (self.allowed_query_shapes.is_empty() || self.allowed_query_shapes.contains(&shape))
    }

    pub fn not_visible_audit(
        &self,
        query_shape: RowQueryShape,
        reason: Option<String>,
    ) -> RowVisibilityAuditRecord {
        RowVisibilityAuditRecord {
            outcome: RowVisibilityOutcomeKind::NotVisible,
            family: self.policy.family(),
            query_shape,
            surface_as_not_found: self.denial_contract.conceal_not_visible_as_not_found,
            index_name: self.visibility_index_name(),
            reason,
            metadata: BTreeMap::new(),
        }
    }

    pub fn explicit_denial_audit(
        &self,
        query_shape: RowQueryShape,
        reason: Option<String>,
    ) -> RowVisibilityAuditRecord {
        RowVisibilityAuditRecord {
            outcome: RowVisibilityOutcomeKind::ExplicitlyDenied,
            family: self.policy.family(),
            query_shape,
            surface_as_not_found: false,
            index_name: self.visibility_index_name(),
            reason: self
                .denial_contract
                .explicit_denial_reveals_reason
                .then_some(reason)
                .flatten(),
            metadata: BTreeMap::new(),
        }
    }

    fn visibility_index_name(&self) -> Option<String> {
        match &self.policy {
            RowScopePolicy::VisibilityIndex { index_name } => Some(index_name.clone()),
            RowScopePolicy::KeyPrefix { .. }
            | RowScopePolicy::TypedRowPredicate { .. }
            | RowScopePolicy::ProcedureOnly { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibilityMembershipTransition {
    pub index_name: String,
    pub lookup_key: String,
    pub row_id: String,
    pub from_visible: bool,
    pub to_visible: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilteredScanResumeToken {
    pub version: u8,
    pub binding_name: String,
    pub query_shape: RowQueryShape,
    pub family: RowScopeFamily,
    pub last_primary_key: Option<String>,
    pub last_visibility_key: Option<String>,
    #[serde(default)]
    pub scanned_rows: u64,
    #[serde(default)]
    pub returned_rows: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VisibilityLookupResult {
    pub index_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lookup_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_row_ids: Vec<String>,
    pub resume_token: Option<FilteredScanResumeToken>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectiveRowScopeBinding {
    pub binding_id: String,
    pub context: PolicyContext,
    pub policy: RowScopePolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_query_shapes: Vec<RowQueryShape>,
    pub write_semantics: RowWriteSemantics,
    pub denial_contract: RowDenialContract,
    pub visibility_lookup: Option<VisibilityLookupResult>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Error)]
pub enum TypedRowPredicateEvaluationError {
    #[error("typed row predicate {predicate_id} is not registered")]
    UnknownPredicate { predicate_id: String },
    #[error("typed row predicate {predicate_id} could not be evaluated: {message}")]
    Evaluation {
        predicate_id: String,
        message: String,
    },
}

#[derive(Debug, Error)]
pub enum DatabasePolicyError {
    #[error("binding {binding_name} uses unsupported capability family {capability_family}")]
    UnsupportedCapabilityFamily {
        binding_name: String,
        capability_family: String,
    },
    #[error("database access preflight did not allow result authorization: {outcome:?}")]
    PreflightDenied {
        outcome: PolicyOutcomeKind,
        message: Option<String>,
    },
    #[error("row-scope context is missing required field {field}")]
    MissingContextField { field: String },
    #[error(transparent)]
    Predicate(#[from] TypedRowPredicateEvaluationError),
    #[error(transparent)]
    RowScope(#[from] RowScopeResolutionError),
}

pub trait TypedRowPredicateEvaluator {
    fn evaluate(
        &self,
        predicate_id: &str,
        row_type: Option<&str>,
        row: &JsonValue,
        context: &PolicyContext,
    ) -> Result<bool, TypedRowPredicateEvaluationError>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeterministicTypedRowPredicate {
    MatchContext {
        row_field: String,
        context_field: String,
    },
    MatchLiteral {
        row_field: String,
        value: JsonValue,
    },
    AnyOf {
        predicates: Vec<DeterministicTypedRowPredicate>,
    },
    AllOf {
        predicates: Vec<DeterministicTypedRowPredicate>,
    },
}

impl DeterministicTypedRowPredicate {
    fn evaluate(&self, row: &JsonValue, context: &PolicyContext) -> bool {
        match self {
            Self::MatchContext {
                row_field,
                context_field,
            } => json_lookup(row, row_field)
                .zip(policy_context_value(context, context_field))
                .is_some_and(|(left, right)| left == &right),
            Self::MatchLiteral { row_field, value } => {
                json_lookup(row, row_field).is_some_and(|candidate| candidate == value)
            }
            Self::AnyOf { predicates } => predicates
                .iter()
                .any(|predicate| predicate.evaluate(row, context)),
            Self::AllOf { predicates } => predicates
                .iter()
                .all(|predicate| predicate.evaluate(row, context)),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicTypedRowPredicateEvaluator {
    predicates: BTreeMap<String, DeterministicTypedRowPredicate>,
}

impl DeterministicTypedRowPredicateEvaluator {
    pub fn with_predicate(
        mut self,
        predicate_id: impl Into<String>,
        predicate: DeterministicTypedRowPredicate,
    ) -> Self {
        self.predicates.insert(predicate_id.into(), predicate);
        self
    }
}

impl TypedRowPredicateEvaluator for DeterministicTypedRowPredicateEvaluator {
    fn evaluate(
        &self,
        predicate_id: &str,
        _row_type: Option<&str>,
        row: &JsonValue,
        context: &PolicyContext,
    ) -> Result<bool, TypedRowPredicateEvaluationError> {
        self.predicates
            .get(predicate_id)
            .ok_or_else(|| TypedRowPredicateEvaluationError::UnknownPredicate {
                predicate_id: predicate_id.to_string(),
            })
            .map(|predicate| predicate.evaluate(row, context))
    }
}

impl EffectiveRowScopeBinding {
    pub fn resolved_key_prefix(&self) -> Result<Option<String>, DatabasePolicyError> {
        match &self.policy {
            RowScopePolicy::KeyPrefix {
                prefix_template, ..
            } => Ok(Some(render_prefix_template(
                prefix_template,
                &self.context,
            )?)),
            RowScopePolicy::TypedRowPredicate { .. }
            | RowScopePolicy::VisibilityIndex { .. }
            | RowScopePolicy::ProcedureOnly { .. } => Ok(None),
        }
    }

    pub fn rewrite_point_key(
        &self,
        requested_key: &str,
    ) -> Result<Option<String>, DatabasePolicyError> {
        let Some(prefix) = self.resolved_key_prefix()? else {
            return Ok(None);
        };
        Ok(Some(rewrite_key_with_prefix(&prefix, requested_key)))
    }

    pub fn rewrite_scan_prefix(
        &self,
        requested_prefix: Option<&str>,
    ) -> Result<Option<String>, DatabasePolicyError> {
        let Some(prefix) = self.resolved_key_prefix()? else {
            return Ok(None);
        };
        let requested_prefix = requested_prefix.unwrap_or_default();
        if requested_prefix.is_empty() {
            return Ok(Some(prefix));
        }
        Ok(Some(rewrite_key_with_prefix(&prefix, requested_prefix)))
    }

    pub fn evaluate_read<P>(
        &self,
        query_shape: RowQueryShape,
        row_id: &str,
        row: Option<&JsonValue>,
        predicates: &P,
    ) -> Result<Option<RowVisibilityAuditRecord>, DatabasePolicyError>
    where
        P: TypedRowPredicateEvaluator,
    {
        if !self.supports_query_shape(query_shape) {
            return Ok(Some(self.explicit_denial_audit(
                query_shape,
                Some(format!(
                    "query shape {query_shape:?} is not allowed for this binding"
                )),
            )));
        }

        let visible = match &self.policy {
            RowScopePolicy::KeyPrefix { .. } => {
                let Some(prefix) = self.resolved_key_prefix()? else {
                    return Ok(None);
                };
                row_id.starts_with(&prefix)
            }
            RowScopePolicy::TypedRowPredicate {
                predicate_id,
                row_type,
                ..
            } => match row {
                None => true,
                Some(row) => {
                    predicates.evaluate(predicate_id, row_type.as_deref(), row, &self.context)?
                }
            },
            RowScopePolicy::VisibilityIndex { .. } => {
                self.visibility_lookup.as_ref().is_some_and(|lookup| {
                    lookup
                        .visible_row_ids
                        .iter()
                        .any(|visible_row_id| visible_row_id == row_id)
                })
            }
            RowScopePolicy::ProcedureOnly { reason } => {
                return Ok(Some(
                    self.explicit_denial_audit(query_shape, reason.clone()),
                ));
            }
        };

        if visible {
            Ok(None)
        } else {
            Ok(Some(self.not_visible_audit(
                query_shape,
                Some("row is outside the resolved row scope".to_string()),
            )))
        }
    }

    pub fn evaluate_write<P>(
        &self,
        row_id: &str,
        preimage: Option<&JsonValue>,
        postimage: Option<&JsonValue>,
        occ_read_set: &[String],
        predicates: &P,
    ) -> Result<Option<RowVisibilityAuditRecord>, DatabasePolicyError>
    where
        P: TypedRowPredicateEvaluator,
    {
        if !self.supports_query_shape(RowQueryShape::WriteMutation) {
            return Ok(Some(self.explicit_denial_audit(
                RowQueryShape::WriteMutation,
                Some("write mutation is not allowed for this binding".to_string()),
            )));
        }

        if self.write_semantics.require_occ_read_set
            && preimage.is_some()
            && !occ_read_set.iter().any(|candidate| candidate == row_id)
        {
            return Ok(Some(self.explicit_denial_audit(
                RowQueryShape::WriteMutation,
                Some(format!(
                    "write is missing OCC read-set coverage for {row_id}"
                )),
            )));
        }

        let preimage_visible = if self.write_semantics.evaluate_preimage {
            match self.evaluate_read(RowQueryShape::WriteMutation, row_id, preimage, predicates)? {
                Some(audit) => {
                    return Ok(Some(self.explicit_denial_audit(
                        RowQueryShape::WriteMutation,
                        audit.reason.or(Some(
                            "preimage is outside the resolved row scope".to_string(),
                        )),
                    )));
                }
                None => preimage.is_some(),
            }
        } else {
            preimage.is_some()
        };

        if self.write_semantics.evaluate_postimage
            && let Some(audit) =
                self.evaluate_read(RowQueryShape::WriteMutation, row_id, postimage, predicates)?
        {
            let reason = if self.write_semantics.reject_scope_escaping_writes && preimage_visible {
                Some("write would escape the resolved row scope".to_string())
            } else {
                audit.reason
            };
            return Ok(Some(
                self.explicit_denial_audit(RowQueryShape::WriteMutation, reason),
            ));
        }

        Ok(None)
    }

    fn supports_query_shape(&self, shape: RowQueryShape) -> bool {
        self.policy.supports_query_shape(shape)
            && (self.allowed_query_shapes.is_empty() || self.allowed_query_shapes.contains(&shape))
    }

    fn not_visible_audit(
        &self,
        query_shape: RowQueryShape,
        reason: Option<String>,
    ) -> RowVisibilityAuditRecord {
        RowVisibilityAuditRecord {
            outcome: RowVisibilityOutcomeKind::NotVisible,
            family: self.policy.family(),
            query_shape,
            surface_as_not_found: self.denial_contract.conceal_not_visible_as_not_found,
            index_name: self.visibility_index_name(),
            reason,
            metadata: BTreeMap::new(),
        }
    }

    fn explicit_denial_audit(
        &self,
        query_shape: RowQueryShape,
        reason: Option<String>,
    ) -> RowVisibilityAuditRecord {
        RowVisibilityAuditRecord {
            outcome: RowVisibilityOutcomeKind::ExplicitlyDenied,
            family: self.policy.family(),
            query_shape,
            surface_as_not_found: false,
            index_name: self.visibility_index_name(),
            reason: self
                .denial_contract
                .explicit_denial_reveals_reason
                .then_some(reason)
                .flatten(),
            metadata: BTreeMap::new(),
        }
    }

    fn visibility_index_name(&self) -> Option<String> {
        match &self.policy {
            RowScopePolicy::VisibilityIndex { index_name } => Some(index_name.clone()),
            RowScopePolicy::KeyPrefix { .. }
            | RowScopePolicy::TypedRowPredicate { .. }
            | RowScopePolicy::ProcedureOnly { .. } => None,
        }
    }
}

pub trait VisibilityIndexReader {
    fn lookup(&self, spec: &VisibilityIndexSpec, context: &PolicyContext)
    -> VisibilityLookupResult;
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicVisibilityIndexStore {
    rows_by_index_and_key: BTreeMap<(String, String), Vec<String>>,
}

impl DeterministicVisibilityIndexStore {
    pub fn with_visible_row(
        mut self,
        index_name: impl Into<String>,
        lookup_key: impl Into<String>,
        row_id: impl Into<String>,
    ) -> Self {
        let transition = VisibilityMembershipTransition {
            index_name: index_name.into(),
            lookup_key: lookup_key.into(),
            row_id: row_id.into(),
            from_visible: false,
            to_visible: true,
            metadata: BTreeMap::new(),
        };
        self.apply_transition(&transition);
        self
    }

    pub fn with_transition(mut self, transition: VisibilityMembershipTransition) -> Self {
        self.apply_transition(&transition);
        self
    }

    fn apply_transition(&mut self, transition: &VisibilityMembershipTransition) {
        let key = (transition.index_name.clone(), transition.lookup_key.clone());
        let rows = self.rows_by_index_and_key.entry(key).or_default();
        rows.retain(|row_id| row_id != &transition.row_id);
        if transition.to_visible {
            rows.push(transition.row_id.clone());
            rows.sort();
            rows.dedup();
        }
    }
}

impl VisibilityIndexReader for DeterministicVisibilityIndexStore {
    fn lookup(
        &self,
        spec: &VisibilityIndexSpec,
        context: &PolicyContext,
    ) -> VisibilityLookupResult {
        let lookup_keys = spec.lookup_keys(context);
        let mut visible_row_ids = BTreeSet::new();
        for lookup_key in &lookup_keys {
            if let Some(rows) = self
                .rows_by_index_and_key
                .get(&(spec.index_name.clone(), lookup_key.clone()))
            {
                visible_row_ids.extend(rows.iter().cloned());
            }
        }

        VisibilityLookupResult {
            index_name: spec.index_name.clone(),
            lookup_keys,
            visible_row_ids: visible_row_ids.into_iter().collect(),
            resume_token: None,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Error)]
pub enum RowScopeResolutionError {
    #[error(
        "row scope binding {binding_id} requires visibility index {index_name}, but no matching spec was attached"
    )]
    MissingVisibilityIndex {
        binding_id: String,
        index_name: String,
    },
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_scope_binding: Option<RowScopeBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility_index: Option<VisibilityIndexSpec>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl ResourcePolicy {
    pub fn resolve_row_scope<R>(
        &self,
        context: PolicyContext,
        visibility_indexes: &R,
    ) -> Result<Option<EffectiveRowScopeBinding>, RowScopeResolutionError>
    where
        R: VisibilityIndexReader + ?Sized,
    {
        let Some(binding) = self.row_scope_binding.as_ref() else {
            return Ok(None);
        };

        let visibility_lookup = match &binding.policy {
            RowScopePolicy::VisibilityIndex { index_name } => {
                let spec = self
                    .visibility_index
                    .as_ref()
                    .filter(|spec| spec.index_name == *index_name)
                    .ok_or_else(|| RowScopeResolutionError::MissingVisibilityIndex {
                        binding_id: binding.binding_id.clone(),
                        index_name: index_name.clone(),
                    })?;
                Some(visibility_indexes.lookup(spec, &context))
            }
            RowScopePolicy::KeyPrefix { .. }
            | RowScopePolicy::TypedRowPredicate { .. }
            | RowScopePolicy::ProcedureOnly { .. } => None,
        };

        Ok(Some(EffectiveRowScopeBinding {
            binding_id: binding.binding_id.clone(),
            context,
            policy: binding.policy.clone(),
            allowed_query_shapes: binding.effective_query_shapes(),
            write_semantics: binding.write_semantics.clone(),
            denial_contract: binding.denial_contract.clone(),
            visibility_lookup,
            metadata: binding.metadata.clone(),
        }))
    }
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
    NotVisible,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_visibility: Option<RowVisibilityAuditRecord>,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseTarget {
    pub database: Option<String>,
    pub table: Option<String>,
    pub procedure_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DatabaseAccessRequest {
    pub session_id: String,
    pub operation: ExecutionOperation,
    pub binding_name: String,
    pub capability_family: Option<String>,
    pub action: DatabaseActionKind,
    pub target: DatabaseTarget,
    pub key: Option<String>,
    pub prefix: Option<String>,
    pub row_id: Option<String>,
    pub candidate_row: Option<JsonValue>,
    pub preimage: Option<JsonValue>,
    pub postimage: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub occ_read_set: Vec<String>,
    #[serde(default)]
    pub metrics: CapabilityUseMetrics,
    pub requested_at: Timestamp,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DatabaseAccessDecision {
    pub policy: PolicyDecisionRecord,
    pub family: Option<DatabaseCapabilityFamily>,
    pub action: DatabaseActionKind,
    pub target: DatabaseTarget,
    pub effective_row_scope: Option<EffectiveRowScopeBinding>,
    pub effective_key: Option<String>,
    pub effective_prefix: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DatabaseResultRow {
    pub row_id: String,
    pub row: JsonValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DatabaseAuthorizedRowSet {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<DatabaseResultRow>,
    #[serde(default)]
    pub scanned_rows: u64,
    #[serde(default)]
    pub returned_rows: u64,
    #[serde(default)]
    pub filtered_rows: u64,
    pub resume_token: Option<FilteredScanResumeToken>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub row_audits: Vec<RowVisibilityAuditRecord>,
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
    pub expanded_manifest: Option<CapabilityManifest>,
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
            | PolicyOutcomeKind::NotVisible
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
            expanded_manifest: Some(self.manifest.clone()),
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
                row_visibility: None,
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

    pub fn evaluate_database_access<P, V>(
        &self,
        request: &DatabaseAccessRequest,
        predicates: &P,
        visibility_indexes: &V,
    ) -> Result<DatabaseAccessDecision, DatabasePolicyError>
    where
        P: TypedRowPredicateEvaluator,
        V: VisibilityIndexReader,
    {
        let mut decision = self.evaluate_use(&CapabilityUseRequest {
            session_id: request.session_id.clone(),
            operation: request.operation,
            binding_name: request.binding_name.clone(),
            capability_family: request.capability_family.clone(),
            targets: database_resource_targets(&request.target),
            metrics: request.metrics.clone(),
            requested_at: request.requested_at,
            metadata: request.metadata.clone(),
        });
        let binding = self
            .manifest
            .bindings
            .iter()
            .find(|binding| binding.binding_name == request.binding_name);
        let family = binding
            .and_then(|binding| DatabaseCapabilityFamily::parse(&binding.capability_family))
            .or_else(|| {
                request
                    .capability_family
                    .as_deref()
                    .and_then(DatabaseCapabilityFamily::parse)
            });

        if decision.outcome.outcome != PolicyOutcomeKind::Allowed {
            return Ok(DatabaseAccessDecision {
                policy: decision,
                family,
                action: request.action,
                target: request.target.clone(),
                effective_row_scope: None,
                effective_key: None,
                effective_prefix: None,
            });
        }

        let Some(binding) = binding else {
            return Ok(DatabaseAccessDecision {
                policy: decision,
                family,
                action: request.action,
                target: request.target.clone(),
                effective_row_scope: None,
                effective_key: None,
                effective_prefix: None,
            });
        };

        let family =
            DatabaseCapabilityFamily::parse(&binding.capability_family).ok_or_else(|| {
                DatabasePolicyError::UnsupportedCapabilityFamily {
                    binding_name: binding.binding_name.clone(),
                    capability_family: binding.capability_family.clone(),
                }
            })?;

        if !family.allows_action(request.action) {
            decision.outcome.outcome = PolicyOutcomeKind::Denied;
            decision.outcome.message = Some(format!(
                "action {:?} is not allowed for capability family {}",
                request.action,
                family.as_str()
            ));
            return Ok(DatabaseAccessDecision {
                policy: decision,
                family: Some(family),
                action: request.action,
                target: request.target.clone(),
                effective_row_scope: None,
                effective_key: None,
                effective_prefix: None,
            });
        }

        let effective_row_scope = binding.resource_policy.resolve_row_scope(
            PolicyContext::from(self.subject.clone()),
            visibility_indexes,
        )?;

        let effective_key = match (effective_row_scope.as_ref(), request.key.as_deref()) {
            (Some(row_scope), Some(key)) => row_scope
                .rewrite_point_key(key)?
                .or_else(|| Some(key.to_string())),
            (None, Some(key)) => Some(key.to_string()),
            (_, None) => None,
        };
        let effective_prefix = match (effective_row_scope.as_ref(), request.prefix.as_deref()) {
            (Some(row_scope), prefix) => row_scope
                .rewrite_scan_prefix(prefix)?
                .or_else(|| prefix.map(str::to_string)),
            (None, prefix) => prefix.map(str::to_string),
        };

        if let Some(query_shape) = request.action.row_query_shape()
            && let Some(row_scope) = effective_row_scope.as_ref()
        {
            if !row_scope.supports_query_shape(query_shape) {
                apply_row_scope_audit(
                    &mut decision,
                    row_scope.explicit_denial_audit(
                        query_shape,
                        Some(format!(
                            "query shape {query_shape:?} is not allowed for this binding"
                        )),
                    ),
                );
            } else if request.action == DatabaseActionKind::WriteMutation {
                if let Some(row_id) = request
                    .row_id
                    .as_deref()
                    .or(effective_key.as_deref())
                    .filter(|row_id| !row_id.is_empty())
                    && let Some(audit) = row_scope.evaluate_write(
                        row_id,
                        request.preimage.as_ref(),
                        request.postimage.as_ref(),
                        &request.occ_read_set,
                        predicates,
                    )?
                {
                    apply_row_scope_audit(&mut decision, audit);
                }
            } else if let Some(row_id) = request
                .row_id
                .as_deref()
                .or(effective_key.as_deref())
                .filter(|row_id| !row_id.is_empty())
                && let Some(audit) = row_scope.evaluate_read(
                    query_shape,
                    row_id,
                    request.candidate_row.as_ref(),
                    predicates,
                )?
            {
                apply_row_scope_audit(&mut decision, audit);
            }
        }

        Ok(DatabaseAccessDecision {
            policy: decision,
            family: Some(family),
            action: request.action,
            target: request.target.clone(),
            effective_row_scope,
            effective_key,
            effective_prefix,
        })
    }
}

impl DatabaseAccessDecision {
    pub fn authorize_result_rows<P>(
        &self,
        rows: impl IntoIterator<Item = DatabaseResultRow>,
        predicates: &P,
    ) -> Result<DatabaseAuthorizedRowSet, DatabasePolicyError>
    where
        P: TypedRowPredicateEvaluator,
    {
        if self.policy.outcome.outcome != PolicyOutcomeKind::Allowed {
            return Err(DatabasePolicyError::PreflightDenied {
                outcome: self.policy.outcome.outcome,
                message: self.policy.outcome.message.clone(),
            });
        }

        let Some(query_shape) = self.action.row_query_shape() else {
            let rows = rows.into_iter().collect::<Vec<_>>();
            return Ok(DatabaseAuthorizedRowSet {
                scanned_rows: rows.len() as u64,
                returned_rows: rows.len() as u64,
                rows,
                filtered_rows: 0,
                resume_token: None,
                row_audits: Vec::new(),
            });
        };

        let mut visible_rows = Vec::new();
        let mut row_audits = Vec::new();
        let mut scanned_rows = 0u64;
        let mut filtered_rows = 0u64;
        let mut last_scanned_row_id = None;

        for candidate in rows {
            scanned_rows = scanned_rows.saturating_add(1);
            last_scanned_row_id = Some(candidate.row_id.clone());
            if let Some(row_scope) = self.effective_row_scope.as_ref()
                && let Some(audit) = row_scope.evaluate_read(
                    query_shape,
                    &candidate.row_id,
                    Some(&candidate.row),
                    predicates,
                )?
            {
                filtered_rows = filtered_rows.saturating_add(1);
                row_audits.push(audit);
                continue;
            }
            visible_rows.push(candidate);
        }

        let resume_token = self.effective_row_scope.as_ref().and_then(|row_scope| {
            (query_shape == RowQueryShape::BoundedPrefixScan).then(|| FilteredScanResumeToken {
                version: 1,
                binding_name: self.policy.outcome.binding_name.clone(),
                query_shape,
                family: row_scope.policy.family(),
                last_primary_key: last_scanned_row_id.clone(),
                last_visibility_key: None,
                scanned_rows,
                returned_rows: visible_rows.len() as u64,
                metadata: BTreeMap::new(),
            })
        });

        Ok(DatabaseAuthorizedRowSet {
            rows: visible_rows,
            scanned_rows,
            returned_rows: scanned_rows.saturating_sub(filtered_rows),
            filtered_rows,
            resume_token,
            row_audits,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyResolutionRequest {
    pub subject: SubjectResolutionRequest,
    pub session_mode: SessionMode,
    pub preset_name: Option<String>,
    pub profile_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManifestBindingOverride {
    pub binding_name: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub drop_binding: bool,
    pub resource_policy: Option<ResourcePolicy>,
    pub budget_policy: Option<BudgetPolicy>,
    pub expose_in_just_bash: Option<bool>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionPresetRequest {
    pub subject: SubjectResolutionRequest,
    pub preset_name: String,
    pub profile_name: Option<String>,
    pub session_mode_override: Option<SessionMode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binding_overrides: Vec<ManifestBindingOverride>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedSessionPreset {
    pub preset: CapabilityPresetDescriptor,
    pub profile: Option<CapabilityProfileDescriptor>,
    pub resolved: ResolvedSessionPolicy,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub audit_metadata: BTreeMap<String, JsonValue>,
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
    #[error("binding override references unknown binding {binding_name}")]
    UnknownBindingOverride { binding_name: String },
    #[error("{override_source} cannot widen binding {binding_name} field {field}")]
    UnsafeBindingOverride {
        override_source: String,
        binding_name: String,
        field: String,
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
        if let Some(profile_name) = request.profile_name.as_ref()
            && let Some(policy) = self.by_profile_name.get(profile_name)
        {
            return Ok(policy.clone());
        }
        if let Some(preset_name) = request.preset_name.as_ref()
            && let Some(policy) = self.by_preset_name.get(preset_name)
        {
            return Ok(policy.clone());
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

    pub fn list_presets(&self) -> Vec<CapabilityPresetDescriptor> {
        self.presets.values().cloned().collect()
    }

    pub fn describe_preset(&self, preset_name: &str) -> Option<CapabilityPresetDescriptor> {
        self.presets.get(preset_name).cloned()
    }

    pub fn list_profiles(&self, preset_name: Option<&str>) -> Vec<CapabilityProfileDescriptor> {
        self.profiles
            .values()
            .filter(|profile| preset_name.is_none_or(|name| profile.preset_name == name))
            .cloned()
            .collect()
    }

    pub fn describe_profile(&self, profile_name: &str) -> Option<CapabilityProfileDescriptor> {
        self.profiles.get(profile_name).cloned()
    }

    pub fn prepare_session_from_preset(
        &self,
        request: &SessionPresetRequest,
    ) -> Result<PreparedSessionPreset, PolicyError> {
        let preset = self
            .presets
            .get(&request.preset_name)
            .cloned()
            .ok_or_else(|| PolicyError::UnknownPreset {
                preset_name: request.preset_name.clone(),
            })?;
        let profile = match request.profile_name.as_ref() {
            Some(profile_name) => {
                Some(self.profiles.get(profile_name).cloned().ok_or_else(|| {
                    PolicyError::UnknownProfile {
                        profile_name: profile_name.clone(),
                    }
                })?)
            }
            None => None,
        };
        if let Some(profile) = profile.as_ref()
            && profile.preset_name != request.preset_name
        {
            return Err(PolicyError::ProfilePresetMismatch {
                profile_name: profile.name.clone(),
                preset_name: profile.preset_name.clone(),
            });
        }

        let session_mode = request
            .session_mode_override
            .unwrap_or(preset.default_session_mode);
        let mut resolved = self.resolve(&PolicyResolutionRequest {
            subject: request.subject.clone(),
            session_mode,
            preset_name: Some(request.preset_name.clone()),
            profile_name: request.profile_name.clone(),
        })?;
        apply_manifest_binding_overrides(
            &mut resolved.manifest.bindings,
            &request.binding_overrides,
        )?;
        resolved.rate_limits.retain(|evaluation| {
            resolved
                .manifest
                .bindings
                .iter()
                .any(|binding| binding.binding_name == evaluation.binding_name)
        });
        let audit_metadata = prepared_session_audit_metadata(
            &resolved.manifest,
            resolved.session_mode,
            &preset,
            profile.as_ref(),
            &request.metadata,
        );

        Ok(PreparedSessionPreset {
            preset,
            profile,
            resolved,
            audit_metadata,
        })
    }

    pub fn resolve(
        &self,
        request: &PolicyResolutionRequest,
    ) -> Result<ResolvedSessionPolicy, PolicyError> {
        let subject = self.subject_resolver.resolve_subject(&request.subject)?;
        self.resolve_for_subject(
            subject,
            request.session_mode,
            request.preset_name.clone(),
            request.profile_name.clone(),
        )
    }

    pub fn resolve_for_subject(
        &self,
        subject: PolicySubject,
        session_mode: SessionMode,
        preset_name: Option<String>,
        profile_name: Option<String>,
    ) -> Result<ResolvedSessionPolicy, PolicyError> {
        let grants = self.matching_grants(&subject);

        let manifest = self.resolve_manifest_for_subject(
            &subject,
            &grants,
            preset_name.clone(),
            profile_name.clone(),
        )?;
        let execution_policy =
            self.execution_policy_resolver
                .resolve_execution_policy(&ExecutionPolicyRequest {
                    session_mode,
                    subject_id: subject.subject_id.clone(),
                    preset_name: preset_name.clone(),
                    profile_name: profile_name.clone(),
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
            session_mode,
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

    fn resolve_manifest_for_subject(
        &self,
        subject: &PolicySubject,
        grants: &[&CapabilityGrant],
        preset_name: Option<String>,
        profile_name: Option<String>,
    ) -> Result<CapabilityManifest, PolicyError> {
        let mut bindings = if let Some(preset_name) = &preset_name {
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

        if let Some(profile_name) = &profile_name {
            let profile =
                self.profiles
                    .get(profile_name)
                    .ok_or_else(|| PolicyError::UnknownProfile {
                        profile_name: profile_name.clone(),
                    })?;
            if preset_name.as_deref() != Some(profile.preset_name.as_str()) {
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
                    Some(profile.preset_name.as_str()),
                    Some(profile.name.as_str()),
                )?);
            }
        }

        Ok(CapabilityManifest {
            subject: Some(subject.clone()),
            preset_name,
            profile_name,
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
                self.binding_from_requested_binding(grant, binding, Some(preset_name), None)
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
        let base_resource_policy = grant
            .resource_policy
            .clone()
            .unwrap_or_else(|| template.default_resource_policy.clone());
        let resource_policy = match requested.and_then(|value| value.resource_policy.as_ref()) {
            Some(policy) => intersect_resource_policy(
                &base_resource_policy,
                policy,
                "preset/profile selection",
                &binding_name,
            )?,
            None => base_resource_policy,
        };
        let base_budget_policy = grant
            .budget_policy
            .clone()
            .unwrap_or_else(|| template.default_budget_policy.clone());
        let budget_policy = match requested.and_then(|value| value.budget_policy.as_ref()) {
            Some(policy) => intersect_budget_policy(&base_budget_policy, policy),
            None => base_budget_policy,
        };
        let expose_in_just_bash = template.expose_in_just_bash
            && requested
                .and_then(|value| value.expose_in_just_bash)
                .unwrap_or(template.expose_in_just_bash);

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
        preset_name: Option<&str>,
        profile_name: Option<&str>,
    ) -> Result<ManifestBinding, PolicyError> {
        let mut binding = self.binding_from_grant(grant, Some(requested))?;
        if let Some(preset_name) = preset_name {
            binding.metadata.insert(
                "preset_name".to_string(),
                JsonValue::String(preset_name.to_string()),
            );
        }
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

    #[allow(clippy::too_many_arguments)]
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

fn apply_manifest_binding_overrides(
    bindings: &mut Vec<ManifestBinding>,
    overrides: &[ManifestBindingOverride],
) -> Result<(), PolicyError> {
    for override_spec in overrides {
        let Some(position) = bindings
            .iter()
            .position(|binding| binding.binding_name == override_spec.binding_name)
        else {
            return Err(PolicyError::UnknownBindingOverride {
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
                "session binding override",
                &binding.binding_name,
            )?;
        }
        if let Some(budget_policy) = override_spec.budget_policy.as_ref() {
            binding.budget_policy = intersect_budget_policy(&binding.budget_policy, budget_policy);
        }
        if let Some(expose_in_just_bash) = override_spec.expose_in_just_bash {
            if expose_in_just_bash {
                if binding.shell_command.is_none() {
                    return Err(PolicyError::UnsafeBindingOverride {
                        override_source: "session binding override".to_string(),
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

fn intersect_resource_policy(
    base: &ResourcePolicy,
    requested: &ResourcePolicy,
    source: &str,
    binding_name: &str,
) -> Result<ResourcePolicy, PolicyError> {
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
                    Err(PolicyError::UnsafeBindingOverride {
                        override_source: source.to_string(),
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
            return Err(PolicyError::UnsafeBindingOverride {
                override_source: source.to_string(),
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
            return Err(PolicyError::UnsafeBindingOverride {
                override_source: source.to_string(),
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

fn intersect_string_scopes(base: &[String], requested: &[String]) -> Vec<String> {
    match (base.is_empty(), requested.is_empty()) {
        (true, true) => Vec::new(),
        (true, false) => requested.to_vec(),
        (false, true) => base.to_vec(),
        (false, false) => base
            .iter()
            .filter(|value| requested.contains(*value))
            .cloned()
            .collect(),
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

fn prepared_session_audit_metadata(
    manifest: &CapabilityManifest,
    session_mode: SessionMode,
    preset: &CapabilityPresetDescriptor,
    profile: Option<&CapabilityProfileDescriptor>,
    request_metadata: &BTreeMap<String, JsonValue>,
) -> BTreeMap<String, JsonValue> {
    let mut metadata = request_metadata.clone();
    metadata.insert(
        "preset_name".to_string(),
        JsonValue::String(preset.name.clone()),
    );
    if let Some(profile) = profile {
        metadata.insert(
            "profile_name".to_string(),
            JsonValue::String(profile.name.clone()),
        );
    }
    metadata.insert("session_mode".to_string(), json_value(&session_mode));
    metadata.insert("expanded_manifest".to_string(), json_value(manifest));
    if let Some(policy) = preset.default_execution_policy.as_ref() {
        metadata.insert(
            "preset_execution_policy_hint".to_string(),
            json_value(policy),
        );
    }
    if let Some(policy) = profile.and_then(|profile| profile.execution_policy_override.as_ref()) {
        metadata.insert(
            "profile_execution_policy_hint".to_string(),
            json_value(policy),
        );
    }
    metadata
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

fn default_true() -> bool {
    true
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

fn database_resource_targets(target: &DatabaseTarget) -> Vec<ResourceTarget> {
    let mut resources = Vec::new();
    if let Some(database) = target.database.as_ref() {
        resources.push(ResourceTarget {
            kind: ResourceKind::Database,
            identifier: database.clone(),
        });
    }
    if let Some(table) = target.table.as_ref() {
        resources.push(ResourceTarget {
            kind: ResourceKind::Table,
            identifier: table.clone(),
        });
    }
    if let Some(procedure_id) = target.procedure_id.as_ref() {
        resources.push(ResourceTarget {
            kind: ResourceKind::Procedure,
            identifier: procedure_id.clone(),
        });
    }
    resources
}

fn apply_row_scope_audit(decision: &mut PolicyDecisionRecord, audit: RowVisibilityAuditRecord) {
    decision.outcome.message = audit.reason.clone();
    decision.outcome.outcome = match audit.outcome {
        RowVisibilityOutcomeKind::Visible => PolicyOutcomeKind::Allowed,
        RowVisibilityOutcomeKind::NotVisible => PolicyOutcomeKind::NotVisible,
        RowVisibilityOutcomeKind::ExplicitlyDenied => PolicyOutcomeKind::Denied,
    };
    decision.outcome.row_visibility = Some(audit);
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

fn render_prefix_template(
    template: &str,
    context: &PolicyContext,
) -> Result<String, DatabasePolicyError> {
    let mut rendered = String::new();
    let mut remainder = template;
    while let Some(start) = remainder.find('{') {
        rendered.push_str(&remainder[..start]);
        let after_start = &remainder[start + 1..];
        let Some(end) = after_start.find('}') else {
            rendered.push_str(&remainder[start..]);
            return Ok(rendered);
        };
        let field = &after_start[..end];
        let value = policy_context_value_as_string(context, field).ok_or_else(|| {
            DatabasePolicyError::MissingContextField {
                field: field.to_string(),
            }
        })?;
        rendered.push_str(&value);
        remainder = &after_start[end + 1..];
    }
    rendered.push_str(remainder);
    Ok(rendered)
}

fn rewrite_key_with_prefix(prefix: &str, key: &str) -> String {
    if key.starts_with(prefix) {
        key.to_string()
    } else {
        format!("{prefix}{key}")
    }
}

fn policy_context_value_as_string(context: &PolicyContext, field: &str) -> Option<String> {
    policy_context_value(context, field).map(json_value_to_string)
}

fn policy_context_value(context: &PolicyContext, field: &str) -> Option<JsonValue> {
    match field {
        "subject_id" => Some(JsonValue::String(context.subject_id.clone())),
        "tenant_id" => context.tenant_id.clone().map(JsonValue::String),
        "group_ids" => Some(json_value(&context.group_ids)),
        _ => {
            if let Some(attribute) = field.strip_prefix("attributes.") {
                return context.attributes.get(attribute).cloned();
            }
            None
        }
    }
}

fn json_lookup<'a>(value: &'a JsonValue, field_path: &str) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in field_path.split('.') {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

fn json_value_to_string(value: JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => value,
        JsonValue::Array(_) | JsonValue::Object(_) => value.to_string(),
    }
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
