use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb_vfs::JsonValue;

use crate::{SandboxError, SandboxSession};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageInstallRequest {
    pub packages: Vec<String>,
    pub materialize_compatibility_view: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PackageInstallReport {
    pub installer: String,
    pub packages: Vec<String>,
    pub manifest_path: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait]
pub trait PackageInstaller: Send + Sync {
    fn name(&self) -> &str;
    async fn install(
        &self,
        session: &SandboxSession,
        request: PackageInstallRequest,
    ) -> Result<PackageInstallReport, SandboxError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicPackageInstaller {
    name: String,
}

impl DeterministicPackageInstaller {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for DeterministicPackageInstaller {
    fn default() -> Self {
        Self::new("deterministic-packages")
    }
}

#[async_trait]
impl PackageInstaller for DeterministicPackageInstaller {
    fn name(&self) -> &str {
        &self.name
    }

    async fn install(
        &self,
        _session: &SandboxSession,
        request: PackageInstallRequest,
    ) -> Result<PackageInstallReport, SandboxError> {
        let mut packages = request.packages;
        packages.sort();
        packages.dedup();
        Ok(PackageInstallReport {
            installer: self.name.clone(),
            packages: packages.clone(),
            manifest_path: "/.terrace/npm/install-manifest.json".to_string(),
            metadata: BTreeMap::from([
                ("package_count".to_string(), JsonValue::from(packages.len())),
                (
                    "materialized".to_string(),
                    JsonValue::from(request.materialize_compatibility_view),
                ),
            ]),
        })
    }
}
