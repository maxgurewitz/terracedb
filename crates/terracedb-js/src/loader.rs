use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb_vfs::VolumeSnapshot;

use crate::{JsSubstrateError, JsTraceEvent, JsTracePhase};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsModuleKind {
    Workspace,
    HostCapability,
    Package,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsResolvedModule {
    pub requested_specifier: String,
    pub canonical_specifier: String,
    pub kind: JsModuleKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsLoadedModule {
    pub resolved: JsResolvedModule,
    pub source: String,
    pub trace: Vec<JsTraceEvent>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait(?Send)]
pub trait JsModuleLoader: Send + Sync {
    async fn resolve(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<JsResolvedModule, JsSubstrateError>;
    async fn load(&self, resolved: &JsResolvedModule) -> Result<JsLoadedModule, JsSubstrateError>;
}

#[derive(Clone)]
pub struct VfsJsModuleLoader {
    snapshot: Arc<dyn VolumeSnapshot>,
    workspace_root: Arc<str>,
    package_modules: Arc<BTreeMap<String, String>>,
}

impl VfsJsModuleLoader {
    pub fn new(snapshot: Arc<dyn VolumeSnapshot>) -> Self {
        Self::with_workspace_root(snapshot, "/workspace")
    }

    pub fn with_workspace_root(
        snapshot: Arc<dyn VolumeSnapshot>,
        workspace_root: impl Into<String>,
    ) -> Self {
        Self {
            snapshot,
            workspace_root: Arc::from(normalize_path(&workspace_root.into())),
            package_modules: Arc::new(BTreeMap::new()),
        }
    }

    pub fn with_package_modules(
        snapshot: Arc<dyn VolumeSnapshot>,
        package_modules: BTreeMap<String, String>,
    ) -> Self {
        Self::with_workspace_root_and_package_modules(snapshot, "/workspace", package_modules)
    }

    pub fn with_workspace_root_and_package_modules(
        snapshot: Arc<dyn VolumeSnapshot>,
        workspace_root: impl Into<String>,
        package_modules: BTreeMap<String, String>,
    ) -> Self {
        Self {
            snapshot,
            workspace_root: Arc::from(normalize_path(&workspace_root.into())),
            package_modules: Arc::new(package_modules),
        }
    }
}

#[async_trait(?Send)]
impl JsModuleLoader for VfsJsModuleLoader {
    async fn resolve(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<JsResolvedModule, JsSubstrateError> {
        let (canonical_specifier, kind) = if specifier.starts_with("terrace:/") {
            let normalized = normalize_virtual_path(specifier);
            let path = normalized.trim_start_matches("terrace:");
            if is_within_workspace_root(path, self.workspace_root.as_ref()) {
                (normalized, JsModuleKind::Workspace)
            } else {
                return Err(JsSubstrateError::UnsupportedSpecifier {
                    specifier: specifier.to_string(),
                });
            }
        } else if specifier.starts_with("terrace:host/") {
            (specifier.to_string(), JsModuleKind::HostCapability)
        } else if specifier.starts_with("npm:") {
            (specifier.to_string(), JsModuleKind::Package)
        } else if specifier.starts_with('/') {
            let normalized = normalize_path(specifier);
            if is_within_workspace_root(&normalized, self.workspace_root.as_ref()) {
                (format!("terrace:{normalized}"), JsModuleKind::Workspace)
            } else {
                return Err(JsSubstrateError::UnsupportedSpecifier {
                    specifier: specifier.to_string(),
                });
            }
        } else if !specifier.contains(':') && !specifier.starts_with('.') {
            (format!("npm:{specifier}"), JsModuleKind::Package)
        } else if let Some(referrer) = referrer {
            if specifier.starts_with("./") || specifier.starts_with("../") {
                let resolved = resolve_relative_workspace_specifier(
                    referrer,
                    specifier,
                    self.workspace_root.as_ref(),
                )?;
                (resolved, JsModuleKind::Workspace)
            } else {
                return Err(JsSubstrateError::UnsupportedSpecifier {
                    specifier: specifier.to_string(),
                });
            }
        } else {
            return Err(JsSubstrateError::UnsupportedSpecifier {
                specifier: specifier.to_string(),
            });
        };
        Ok(JsResolvedModule {
            requested_specifier: specifier.to_string(),
            canonical_specifier,
            kind,
        })
    }

    async fn load(&self, resolved: &JsResolvedModule) -> Result<JsLoadedModule, JsSubstrateError> {
        match resolved.kind {
            JsModuleKind::Workspace => {
                let path = resolved
                    .canonical_specifier
                    .trim_start_matches("terrace:")
                    .to_string();
                let bytes = self.snapshot.fs().read_file(&path).await?.ok_or_else(|| {
                    JsSubstrateError::ModuleNotFound {
                        specifier: resolved.canonical_specifier.clone(),
                    }
                })?;
                Ok(JsLoadedModule {
                    resolved: resolved.clone(),
                    source: String::from_utf8_lossy(&bytes).into_owned(),
                    trace: vec![JsTraceEvent {
                        phase: JsTracePhase::ModuleLoad,
                        label: resolved.canonical_specifier.clone(),
                        metadata: BTreeMap::new(),
                    }],
                    metadata: BTreeMap::new(),
                })
            }
            JsModuleKind::HostCapability => Ok(JsLoadedModule {
                resolved: resolved.clone(),
                source: host_capability_preview_source(&resolved.canonical_specifier),
                trace: vec![JsTraceEvent {
                    phase: JsTracePhase::ModuleLoad,
                    label: resolved.canonical_specifier.clone(),
                    metadata: BTreeMap::from([("synthetic".to_string(), JsonValue::Bool(true))]),
                }],
                metadata: BTreeMap::from([
                    ("synthetic".to_string(), JsonValue::Bool(true)),
                    (
                        "host_service".to_string(),
                        JsonValue::String("capability".to_string()),
                    ),
                    (
                        "host_exports".to_string(),
                        JsonValue::Array(vec![JsonValue::String(host_capability_operation_name(
                            &resolved.canonical_specifier,
                        ))]),
                    ),
                ]),
            }),
            JsModuleKind::Package => {
                let source = self
                    .package_modules
                    .get(&resolved.canonical_specifier)
                    .cloned()
                    .ok_or_else(|| JsSubstrateError::ModuleNotFound {
                        specifier: resolved.canonical_specifier.clone(),
                    })?;
                Ok(JsLoadedModule {
                    resolved: resolved.clone(),
                    source,
                    trace: vec![JsTraceEvent {
                        phase: JsTracePhase::ModuleLoad,
                        label: resolved.canonical_specifier.clone(),
                        metadata: BTreeMap::from([("package".to_string(), JsonValue::Bool(true))]),
                    }],
                    metadata: BTreeMap::new(),
                })
            }
        }
    }
}

fn normalize_virtual_path(specifier: &str) -> String {
    let path = specifier.trim_start_matches("terrace:");
    format!("terrace:{}", normalize_path(path))
}

fn resolve_relative_workspace_specifier(
    referrer: &str,
    specifier: &str,
    workspace_root: &str,
) -> Result<String, JsSubstrateError> {
    if !referrer.starts_with("terrace:/") {
        return Err(JsSubstrateError::UnsupportedSpecifier {
            specifier: specifier.to_string(),
        });
    }
    let referrer_path = referrer.trim_start_matches("terrace:");
    if !is_within_workspace_root(referrer_path, workspace_root) {
        return Err(JsSubstrateError::UnsupportedSpecifier {
            specifier: specifier.to_string(),
        });
    }
    let base_dir = referrer_path
        .rsplit_once('/')
        .map(|(prefix, _)| prefix)
        .unwrap_or("/");
    let resolved_path = normalize_path(&format!("{base_dir}/{specifier}"));
    if !is_within_workspace_root(&resolved_path, workspace_root) {
        return Err(JsSubstrateError::UnsupportedSpecifier {
            specifier: specifier.to_string(),
        });
    }
    Ok(format!("terrace:{resolved_path}"))
}

fn normalize_path(path: &str) -> String {
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

fn is_within_workspace_root(path: &str, workspace_root: &str) -> bool {
    path == workspace_root || path.starts_with(&format!("{workspace_root}/"))
}

fn host_capability_preview_source(specifier: &str) -> String {
    let operation = host_capability_operation_name(specifier);
    format!(
        "// synthetic host capability module {}\n// exports: {}\nexport default null;",
        specifier, operation
    )
}

fn host_capability_operation_name(specifier: &str) -> String {
    specifier
        .trim_start_matches("terrace:host/")
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("capability")
        .to_string()
}
