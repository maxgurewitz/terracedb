use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use terracedb_vfs::JsonValue;

use crate::{HOST_CAPABILITY_PREFIX, SandboxError};

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
}

pub trait CapabilityRegistry: Send + Sync {
    fn manifest(&self) -> CapabilityManifest;
    fn resolve(&self, specifier: &str) -> Option<SandboxCapability>;
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

impl CapabilityRegistry for StaticCapabilityRegistry {
    fn manifest(&self) -> CapabilityManifest {
        self.manifest.clone()
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        self.manifest.get(specifier).cloned()
    }
}
