//! Frozen reviewed-procedure publication and invocation contracts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::Timestamp;
use terracedb_capabilities::{CapabilityManifest, ExecutionPolicy, PolicySubject, SessionMode};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProcedureVersionRef {
    pub procedure_id: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcedureReview {
    pub reviewed_by: String,
    pub source_revision: String,
    pub note: Option<String>,
    pub approved_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewedProcedurePublication {
    pub publication: ProcedureVersionRef,
    pub entrypoint: String,
    pub published_at: Timestamp,
    pub manifest: CapabilityManifest,
    pub execution_policy: ExecutionPolicy,
    pub review: ProcedureReview,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
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
pub struct ProcedureInvocationReceipt {
    pub invocation_id: String,
    pub publication: ProcedureVersionRef,
    pub accepted_at: Timestamp,
    pub execution_policy: ExecutionPolicy,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Error)]
pub enum ProcedureStoreError {
    #[error(
        "procedure publication {procedure_id}@{version} already exists in the deterministic store"
    )]
    Duplicate { procedure_id: String, version: u64 },
}

pub trait ProcedurePublicationStore {
    fn publish(
        &mut self,
        publication: ReviewedProcedurePublication,
    ) -> Result<(), ProcedureStoreError>;

    fn load(&self, publication: &ProcedureVersionRef) -> Option<ReviewedProcedurePublication>;
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicProcedurePublicationStore {
    publications: BTreeMap<ProcedureVersionRef, ReviewedProcedurePublication>,
}

impl ProcedurePublicationStore for DeterministicProcedurePublicationStore {
    fn publish(
        &mut self,
        publication: ReviewedProcedurePublication,
    ) -> Result<(), ProcedureStoreError> {
        let key = publication.publication.clone();
        if self.publications.contains_key(&key) {
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
