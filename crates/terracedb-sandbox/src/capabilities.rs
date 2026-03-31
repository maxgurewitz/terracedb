use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use terracedb_vfs::JsonValue;

use crate::{HOST_CAPABILITY_PREFIX, SandboxError, SandboxSession};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxCapability {
    pub name: String,
    pub specifier: String,
    pub description: Option<String>,
    pub typescript_declarations: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

impl SandboxCapability {
    pub fn host_module(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            specifier: format!("{HOST_CAPABILITY_PREFIX}{name}"),
            name,
            description: None,
            typescript_declarations: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> Result<(), SandboxError> {
        if !self.specifier.starts_with(HOST_CAPABILITY_PREFIX) {
            return Err(SandboxError::InvalidCapabilitySpecifier {
                specifier: self.specifier.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxCapabilityMethod {
    pub name: String,
    pub description: Option<String>,
}

impl SandboxCapabilityMethod {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxCapabilityModule {
    pub capability: SandboxCapability,
    pub methods: Vec<SandboxCapabilityMethod>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityCallRequest {
    pub specifier: String,
    pub method: String,
    pub args: Vec<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityCallResult {
    pub specifier: String,
    pub method: String,
    pub value: JsonValue,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    pub capabilities: Vec<SandboxCapability>,
}

impl CapabilityManifest {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn validate(&self) -> Result<(), SandboxError> {
        for capability in &self.capabilities {
            capability.validate()?;
        }
        Ok(())
    }

    pub fn get(&self, specifier: &str) -> Option<&SandboxCapability> {
        self.capabilities
            .iter()
            .find(|capability| capability.specifier == specifier)
    }

    pub fn contains(&self, specifier: &str) -> bool {
        self.get(specifier).is_some()
    }
}

#[async_trait]
pub trait CapabilityRegistry: Send + Sync {
    fn manifest(&self) -> CapabilityManifest;
    fn resolve(&self, specifier: &str) -> Option<SandboxCapability>;

    fn module(&self, _specifier: &str) -> Option<SandboxCapabilityModule> {
        None
    }

    async fn invoke(
        &self,
        _session: &SandboxSession,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        Err(SandboxError::CapabilityUnavailable {
            specifier: request.specifier,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct StaticCapabilityRegistry {
    manifest: CapabilityManifest,
}

impl StaticCapabilityRegistry {
    pub fn new(manifest: CapabilityManifest) -> Result<Self, SandboxError> {
        manifest.validate()?;
        Ok(Self { manifest })
    }
}

#[async_trait]
impl CapabilityRegistry for StaticCapabilityRegistry {
    fn manifest(&self) -> CapabilityManifest {
        self.manifest.clone()
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        self.manifest.get(specifier).cloned()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeterministicCapabilityMethodBehavior {
    EchoArgs,
    Static { value: JsonValue },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeterministicCapabilityModule {
    pub capability: SandboxCapability,
    pub methods: BTreeMap<String, DeterministicCapabilityMethodBehavior>,
    pub metadata: BTreeMap<String, JsonValue>,
}

impl DeterministicCapabilityModule {
    pub fn new(capability: SandboxCapability) -> Result<Self, SandboxError> {
        capability.validate()?;
        Ok(Self {
            capability,
            methods: BTreeMap::new(),
            metadata: BTreeMap::new(),
        })
    }

    pub fn with_echo_method(mut self, method: impl Into<String>) -> Self {
        self.methods.insert(
            method.into(),
            DeterministicCapabilityMethodBehavior::EchoArgs,
        );
        self
    }

    pub fn with_static_method(mut self, method: impl Into<String>, value: JsonValue) -> Self {
        self.methods.insert(
            method.into(),
            DeterministicCapabilityMethodBehavior::Static { value },
        );
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicCapabilityRegistry {
    modules: BTreeMap<String, DeterministicCapabilityModule>,
}

impl DeterministicCapabilityRegistry {
    pub fn new(modules: Vec<DeterministicCapabilityModule>) -> Result<Self, SandboxError> {
        let mut indexed = BTreeMap::new();
        for module in modules {
            module.capability.validate()?;
            indexed.insert(module.capability.specifier.clone(), module);
        }
        Ok(Self { modules: indexed })
    }
}

#[async_trait]
impl CapabilityRegistry for DeterministicCapabilityRegistry {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            capabilities: self
                .modules
                .values()
                .map(|module| module.capability.clone())
                .collect(),
        }
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        self.modules
            .get(specifier)
            .map(|module| module.capability.clone())
    }

    fn module(&self, specifier: &str) -> Option<SandboxCapabilityModule> {
        self.modules
            .get(specifier)
            .map(|module| SandboxCapabilityModule {
                capability: module.capability.clone(),
                methods: module
                    .methods
                    .keys()
                    .cloned()
                    .map(SandboxCapabilityMethod::new)
                    .collect(),
                metadata: module.metadata.clone(),
            })
    }

    async fn invoke(
        &self,
        _session: &SandboxSession,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        let module = self.modules.get(&request.specifier).ok_or_else(|| {
            SandboxError::CapabilityUnavailable {
                specifier: request.specifier.clone(),
            }
        })?;
        let behavior = module.methods.get(&request.method).ok_or_else(|| {
            SandboxError::CapabilityMethodNotFound {
                specifier: request.specifier.clone(),
                method: request.method.clone(),
            }
        })?;
        let value = match behavior {
            DeterministicCapabilityMethodBehavior::EchoArgs => json!({
                "specifier": request.specifier,
                "method": request.method,
                "args": request.args,
            }),
            DeterministicCapabilityMethodBehavior::Static { value } => value.clone(),
        };
        Ok(CapabilityCallResult {
            specifier: request.specifier,
            method: request.method,
            value,
            metadata: module.metadata.clone(),
        })
    }
}
