use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{SandboxError, SandboxSession};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeCheckRequest {
    pub roots: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeScriptDiagnostic {
    pub path: String,
    pub message: String,
    pub code: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeCheckReport {
    pub diagnostics: Vec<TypeScriptDiagnostic>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeScriptEmitReport {
    pub emitted_files: Vec<String>,
}

#[async_trait]
pub trait TypeScriptService: Send + Sync {
    fn name(&self) -> &str;
    async fn check(
        &self,
        session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeCheckReport, SandboxError>;
    async fn emit(
        &self,
        session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeScriptEmitReport, SandboxError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicTypeScriptService {
    name: String,
}

impl DeterministicTypeScriptService {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for DeterministicTypeScriptService {
    fn default() -> Self {
        Self::new("deterministic-typescript")
    }
}

#[async_trait]
impl TypeScriptService for DeterministicTypeScriptService {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(
        &self,
        _session: &SandboxSession,
        _request: TypeCheckRequest,
    ) -> Result<TypeCheckReport, SandboxError> {
        Ok(TypeCheckReport::default())
    }

    async fn emit(
        &self,
        _session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeScriptEmitReport, SandboxError> {
        Ok(TypeScriptEmitReport {
            emitted_files: request.roots,
        })
    }
}
