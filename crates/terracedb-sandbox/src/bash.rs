use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{SandboxError, SandboxSession};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BashRequest {
    pub command: String,
    pub cwd: String,
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BashReport {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub cwd: String,
}

#[async_trait]
pub trait BashService: Send + Sync {
    fn name(&self) -> &str;
    async fn run(
        &self,
        session: &SandboxSession,
        request: BashRequest,
    ) -> Result<BashReport, SandboxError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicBashService {
    name: String,
}

impl DeterministicBashService {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for DeterministicBashService {
    fn default() -> Self {
        Self::new("deterministic-bash")
    }
}

#[async_trait]
impl BashService for DeterministicBashService {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(
        &self,
        _session: &SandboxSession,
        request: BashRequest,
    ) -> Result<BashReport, SandboxError> {
        Ok(BashReport {
            exit_code: 0,
            stdout: format!("{}\n", request.command),
            stderr: String::new(),
            cwd: request.cwd,
        })
    }
}
