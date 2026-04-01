//! Frozen migration plan and history contracts for reviewed sandbox-authored migrations.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::{SequenceNumber, Timestamp};
use terracedb_capabilities::{CapabilityManifest, ExecutionPolicy};

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
