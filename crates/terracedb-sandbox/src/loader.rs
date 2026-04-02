use std::{collections::BTreeMap, path::Path, sync::Arc};

use async_trait::async_trait;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use terracedb_js::{
    JsLoadedModule, JsModuleKind, JsModuleLoader, JsResolvedModule, JsSubstrateError,
};
use serde_json::to_string as json_quote;
use terracedb_vfs::JsonValue;
use tokio::sync::Mutex;

use crate::{
    CapabilityRegistry, GIT_REMOTE_IMPORT_CAPABILITY_SPECIFIER, PackageCompatibilityMode,
    SandboxCapabilityModule, SandboxError, SandboxFilesystemShim, SandboxRuntimeStateHandle,
    SandboxSessionInfo, packages::read_package_install_manifest,
};

pub const TERRACE_WORKSPACE_PREFIX: &str = "terrace:/workspace";
pub const HOST_CAPABILITY_PREFIX: &str = "terrace:host/";
pub const SANDBOX_CAPABILITIES_LIBRARY_SPECIFIER: &str = "@terrace/capabilities";
pub const SANDBOX_FS_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/fs";
pub const SANDBOX_BASH_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/bash";
pub const SANDBOX_GIT_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/git";
pub const SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/typescript";

pub(crate) const FS_HOST_EXPORTS: &[&str] = &[
    "readTextFile",
    "writeTextFile",
    "readJsonFile",
    "writeJsonFile",
    "mkdir",
    "readdir",
    "stat",
    "unlink",
    "rmdir",
    "rename",
    "fsync",
];

pub(crate) const GIT_REMOTE_IMPORT_HOST_EXPORT: &str = "importRepository";
pub(crate) const GIT_REPO_HOST_EXPORTS: &[&str] = &[
    "head",
    "listRefs",
    "status",
    "diff",
    "checkout",
    "updateRef",
    "createPullRequest",
];
pub(crate) const GIT_HOST_EXPORTS: &[&str] = &[
    GIT_REMOTE_IMPORT_HOST_EXPORT,
    "head",
    "listRefs",
    "status",
    "diff",
    "checkout",
    "updateRef",
    "createPullRequest",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SandboxModuleSpecifier {
    Workspace { path: String },
    HostCapability { name: String },
    Npm { package: String },
    NodeBuiltin { builtin: String },
    BuiltinLibrary { specifier: String },
}

impl SandboxModuleSpecifier {
    pub fn parse(specifier: &str) -> Result<Self, SandboxError> {
        if let Some(path) = specifier.strip_prefix(TERRACE_WORKSPACE_PREFIX) {
            let normalized = if path.is_empty() {
                "/workspace".to_string()
            } else {
                normalize_path(&format!("/workspace{path}"))
            };
            return Ok(Self::Workspace { path: normalized });
        }

        if let Some(name) = specifier.strip_prefix(HOST_CAPABILITY_PREFIX) {
            if name.is_empty() {
                return Err(SandboxError::InvalidModuleSpecifier {
                    specifier: specifier.to_string(),
                });
            }
            return Ok(Self::HostCapability {
                name: name.to_string(),
            });
        }

        if let Some(package) = specifier.strip_prefix("npm:") {
            if package.is_empty() {
                return Err(SandboxError::InvalidModuleSpecifier {
                    specifier: specifier.to_string(),
                });
            }
            return Ok(Self::Npm {
                package: package.to_string(),
            });
        }

        if let Some(builtin) = specifier.strip_prefix("node:") {
            if builtin.is_empty() {
                return Err(SandboxError::InvalidModuleSpecifier {
                    specifier: specifier.to_string(),
                });
            }
            return Ok(Self::NodeBuiltin {
                builtin: builtin.to_string(),
            });
        }

        if matches!(
            specifier,
            SANDBOX_CAPABILITIES_LIBRARY_SPECIFIER
                | SANDBOX_FS_LIBRARY_SPECIFIER
                | SANDBOX_BASH_LIBRARY_SPECIFIER
                | SANDBOX_GIT_LIBRARY_SPECIFIER
                | SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER
        ) {
            return Ok(Self::BuiltinLibrary {
                specifier: specifier.to_string(),
            });
        }

        if !specifier.contains(':') && !specifier.starts_with('.') && !specifier.starts_with('/') {
            return Ok(Self::Npm {
                package: specifier.to_string(),
            });
        }

        Err(SandboxError::InvalidModuleSpecifier {
            specifier: specifier.to_string(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxModuleKind {
    Workspace,
    HostCapability,
    Npm,
    NodeBuiltin,
    BuiltinLibrary,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxModuleCacheEntry {
    pub specifier: String,
    pub kind: SandboxModuleKind,
    pub runtime_path: String,
    pub media_type: String,
    pub cache_key: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoadedSandboxModule {
    pub specifier: String,
    pub kind: SandboxModuleKind,
    pub runtime_path: String,
    pub media_type: String,
    pub source: String,
    pub source_map: Option<String>,
    pub cache_entry: SandboxModuleCacheEntry,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxModuleLoadTrace {
    pub module_graph: Vec<String>,
    pub package_imports: Vec<String>,
    pub cache_hits: Vec<String>,
    pub cache_misses: Vec<String>,
    pub cache_entries: Vec<SandboxModuleCacheEntry>,
}

#[derive(Clone)]
pub struct SandboxModuleLoader {
    session_info: SandboxSessionInfo,
    filesystem: Arc<dyn SandboxFilesystemShim>,
    capabilities: Arc<dyn CapabilityRegistry>,
    runtime_state: SandboxRuntimeStateHandle,
    trace: Arc<Mutex<SandboxModuleLoadTrace>>,
}

impl SandboxModuleLoader {
    pub fn new(
        session_info: SandboxSessionInfo,
        filesystem: Arc<dyn SandboxFilesystemShim>,
        capabilities: Arc<dyn CapabilityRegistry>,
        runtime_state: SandboxRuntimeStateHandle,
    ) -> Self {
        Self {
            session_info,
            filesystem,
            capabilities,
            runtime_state,
            trace: Arc::new(Mutex::new(SandboxModuleLoadTrace::default())),
        }
    }

    pub async fn resolve(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<String, SandboxError> {
        self.resolve_specifier(specifier, referrer).await
    }

    pub async fn load(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<LoadedSandboxModule, SandboxError> {
        let resolved = self.resolve_specifier(specifier, referrer).await?;
        let loaded = self.load_resolved(&resolved).await?;
        self.record_loaded_module(&loaded).await;
        Ok(loaded)
    }

    pub async fn trace_snapshot(&self) -> SandboxModuleLoadTrace {
        self.trace.lock().await.clone()
    }

    async fn resolve_specifier(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<String, SandboxError> {
        if specifier.starts_with("./") || specifier.starts_with("../") {
            let referrer = referrer.ok_or_else(|| SandboxError::InvalidModuleSpecifier {
                specifier: specifier.to_string(),
            })?;
            let referrer_path = referrer_to_workspace_path(referrer)?;
            let joined = join_relative_path(&referrer_path, specifier);
            return workspace_path_to_specifier(&joined, &self.session_info.workspace_root);
        }

        if specifier.starts_with('/') {
            return workspace_path_to_specifier(specifier, &self.session_info.workspace_root);
        }

        if let Ok(parsed) = SandboxModuleSpecifier::parse(specifier) {
            return Ok(match parsed {
                SandboxModuleSpecifier::Workspace { path } => format!("terrace:{path}"),
                _ => specifier.to_string(),
            });
        }

        Err(SandboxError::InvalidModuleSpecifier {
            specifier: specifier.to_string(),
        })
    }

    async fn load_resolved(&self, resolved: &str) -> Result<LoadedSandboxModule, SandboxError> {
        match SandboxModuleSpecifier::parse(resolved)? {
            SandboxModuleSpecifier::Workspace { path } => self.load_workspace_module(&path).await,
            SandboxModuleSpecifier::HostCapability { .. } => {
                self.load_capability_module(resolved).await
            }
            SandboxModuleSpecifier::BuiltinLibrary { .. } => self.load_builtin_module(resolved),
            SandboxModuleSpecifier::NodeBuiltin { .. } => self.load_node_builtin_module(resolved),
            SandboxModuleSpecifier::Npm { .. } => self.load_npm_module(resolved).await,
        }
    }

    async fn load_workspace_module(&self, path: &str) -> Result<LoadedSandboxModule, SandboxError> {
        let bytes =
            self.filesystem
                .read_file(path)
                .await?
                .ok_or_else(|| SandboxError::ModuleNotFound {
                    specifier: format!("terrace:{path}"),
                })?;
        let source = String::from_utf8(bytes).map_err(|error| SandboxError::Service {
            service: "module_loader",
            message: format!("workspace module {path} is not valid UTF-8: {error}"),
        })?;
        Ok(self.loaded_module(
            format!("terrace:{path}"),
            SandboxModuleKind::Workspace,
            path.to_string(),
            media_type_for_path(path),
            source,
            Some(source_map_for(&format!("terrace:{path}"), path)),
        ))
    }

    async fn load_capability_module(
        &self,
        specifier: &str,
    ) -> Result<LoadedSandboxModule, SandboxError> {
        if !self
            .session_info
            .provenance
            .capabilities
            .contains(specifier)
        {
            return Err(SandboxError::CapabilityDenied {
                specifier: specifier.to_string(),
            });
        }
        let module = self.capabilities.module(specifier).ok_or_else(|| {
            SandboxError::CapabilityUnavailable {
                specifier: specifier.to_string(),
            }
        })?;
        let runtime_path = format!(
            "/__terrace__/capabilities/{}.mjs",
            sanitize_module_path_component(specifier)
        );
        Ok(self.loaded_module(
            specifier.to_string(),
            SandboxModuleKind::HostCapability,
            runtime_path,
            "text/javascript".to_string(),
            capability_preview_source(&module),
            Some(source_map_for(specifier, specifier)),
        ))
    }

    fn load_builtin_module(&self, specifier: &str) -> Result<LoadedSandboxModule, SandboxError> {
        let source = match specifier {
            SANDBOX_FS_LIBRARY_SPECIFIER => fs_preview_source(),
            SANDBOX_CAPABILITIES_LIBRARY_SPECIFIER => {
                capability_catalog_preview_source(
                    &self.session_info.provenance.capabilities,
                    self.capabilities.as_ref(),
                )
            }
            SANDBOX_BASH_LIBRARY_SPECIFIER => {
                "export const unavailable = true;\nexport const service = 'bash';\n".to_string()
            }
            SANDBOX_GIT_LIBRARY_SPECIFIER => git_preview_source(),
            SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER => {
                "export const unavailable = true;\nexport const service = 'typescript';\n"
                    .to_string()
            }
            _ => {
                return Err(SandboxError::InvalidModuleSpecifier {
                    specifier: specifier.to_string(),
                });
            }
        };
        Ok(self.loaded_module(
            specifier.to_string(),
            SandboxModuleKind::BuiltinLibrary,
            format!(
                "/__terrace__/builtin/{}.mjs",
                sanitize_module_path_component(specifier)
            ),
            "text/javascript".to_string(),
            source,
            Some(source_map_for(specifier, specifier)),
        ))
    }

    fn load_node_builtin_module(
        &self,
        specifier: &str,
    ) -> Result<LoadedSandboxModule, SandboxError> {
        if self.session_info.provenance.package_compat
            != PackageCompatibilityMode::NpmWithNodeBuiltins
        {
            return Err(SandboxError::Service {
                service: "module_loader",
                message: format!(
                    "node builtin imports are disabled for compatibility mode {:?}",
                    self.session_info.provenance.package_compat
                ),
            });
        }

        match specifier {
            "node:fs" | "node:fs/promises" => Ok(self.loaded_module(
                specifier.to_string(),
                SandboxModuleKind::NodeBuiltin,
                format!(
                    "/__terrace__/node/{}.mjs",
                    sanitize_module_path_component(specifier)
                ),
                "text/javascript".to_string(),
                fs_preview_source(),
                Some(source_map_for(specifier, specifier)),
            )),
            _ => Err(SandboxError::Service {
                service: "module_loader",
                message: format!("unsupported node builtin: {specifier}"),
            }),
        }
    }

    async fn load_npm_module(&self, specifier: &str) -> Result<LoadedSandboxModule, SandboxError> {
        if self.session_info.provenance.package_compat == PackageCompatibilityMode::TerraceOnly {
            return Err(SandboxError::Service {
                service: "module_loader",
                message: format!(
                    "package imports are disabled for compatibility mode {:?}",
                    self.session_info.provenance.package_compat
                ),
            });
        }
        let package = match SandboxModuleSpecifier::parse(specifier)? {
            SandboxModuleSpecifier::Npm { package } => package,
            _ => {
                return Err(SandboxError::InvalidModuleSpecifier {
                    specifier: specifier.to_string(),
                });
            }
        };
        let package_name = package_name_for_import(&package)?;
        let manifest = read_package_install_manifest(self.filesystem.as_ref())
            .await?
            .ok_or_else(|| SandboxError::PackageNotInstalled {
                package: package_name.clone(),
            })?;
        let installed =
            manifest
                .package(&package_name)
                .ok_or_else(|| SandboxError::PackageNotInstalled {
                    package: package_name.clone(),
                })?;
        if installed.uses_node_builtins
            && self.session_info.provenance.package_compat
                != PackageCompatibilityMode::NpmWithNodeBuiltins
        {
            return Err(SandboxError::Service {
                service: "module_loader",
                message: format!(
                    "package {package_name} requires node builtin support, but the session compatibility mode is {:?}",
                    self.session_info.provenance.package_compat
                ),
            });
        }
        let entrypoint_path = installed
            .compatibility_entrypoint_path
            .clone()
            .unwrap_or_else(|| installed.entrypoint_path.clone());
        let bytes = self
            .filesystem
            .read_file(&entrypoint_path)
            .await?
            .ok_or_else(|| SandboxError::ModuleNotFound {
                specifier: entrypoint_path.clone(),
            })?;
        let source = String::from_utf8(bytes).map_err(|error| SandboxError::Service {
            service: "module_loader",
            message: format!(
                "npm package module {specifier} at {entrypoint_path} is not valid UTF-8: {error}"
            ),
        })?;
        Ok(self.loaded_module(
            specifier.to_string(),
            SandboxModuleKind::Npm,
            entrypoint_path.clone(),
            media_type_for_path(&entrypoint_path),
            source,
            Some(source_map_for(specifier, &entrypoint_path)),
        ))
    }

    async fn record_loaded_module(&self, loaded: &LoadedSandboxModule) {
        let cached = self
            .runtime_state
            .is_module_cache_hit(&loaded.cache_entry)
            .await;
        self.runtime_state
            .upsert_module_cache(loaded.cache_entry.clone())
            .await;

        let mut trace = self.trace.lock().await;
        if !trace.module_graph.contains(&loaded.specifier) {
            trace.module_graph.push(loaded.specifier.clone());
        }
        if !trace.cache_entries.contains(&loaded.cache_entry) {
            trace.cache_entries.push(loaded.cache_entry.clone());
        }
        if loaded.kind == SandboxModuleKind::Npm
            && let Ok(SandboxModuleSpecifier::Npm { package }) =
                SandboxModuleSpecifier::parse(&loaded.specifier)
            && !trace.package_imports.contains(&package)
        {
            trace.package_imports.push(package);
        }
        let bucket = if cached {
            &mut trace.cache_hits
        } else {
            &mut trace.cache_misses
        };
        if !bucket.contains(&loaded.specifier) {
            bucket.push(loaded.specifier.clone());
        }
    }

    fn loaded_module(
        &self,
        specifier: String,
        kind: SandboxModuleKind,
        runtime_path: String,
        media_type: String,
        source: String,
        source_map: Option<String>,
    ) -> LoadedSandboxModule {
        let cache_key = cache_key_for(&specifier, &media_type, &source);
        LoadedSandboxModule {
            specifier: specifier.clone(),
            kind,
            runtime_path: runtime_path.clone(),
            media_type: media_type.clone(),
            source,
            source_map,
            cache_entry: SandboxModuleCacheEntry {
                specifier,
                kind,
                runtime_path,
                media_type,
                cache_key,
            },
        }
    }

    fn to_js_resolved_module(
        &self,
        requested_specifier: &str,
        canonical_specifier: &str,
    ) -> Result<JsResolvedModule, SandboxError> {
        Ok(JsResolvedModule {
            requested_specifier: requested_specifier.to_string(),
            canonical_specifier: canonical_specifier.to_string(),
            kind: js_module_kind_for_specifier(canonical_specifier)?,
        })
    }

    fn to_js_loaded_module(
        &self,
        loaded: LoadedSandboxModule,
    ) -> Result<JsLoadedModule, SandboxError> {
        let metadata = self.js_metadata_for_loaded(&loaded)?;
        let source = loaded.source.clone();
        let resolved = JsResolvedModule {
            requested_specifier: loaded.specifier.clone(),
            canonical_specifier: loaded.specifier.clone(),
            kind: js_module_kind_for_loaded(&loaded),
        };
        Ok(JsLoadedModule {
            resolved,
            source,
            trace: Vec::new(),
            metadata,
        })
    }

    fn js_metadata_for_loaded(
        &self,
        loaded: &LoadedSandboxModule,
    ) -> Result<BTreeMap<String, JsonValue>, SandboxError> {
        match loaded.kind {
            SandboxModuleKind::HostCapability => {
                let module = self.capabilities.module(&loaded.specifier).ok_or_else(|| {
                    SandboxError::CapabilityUnavailable {
                        specifier: loaded.specifier.clone(),
                    }
                })?;
                Ok(host_service_metadata(
                    &loaded.specifier,
                    module
                        .methods
                        .iter()
                        .map(|method| method.name.clone())
                        .collect(),
                ))
            }
            SandboxModuleKind::NodeBuiltin => Ok(host_service_metadata(
                &loaded.specifier,
                FS_HOST_EXPORTS
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect(),
            )),
            SandboxModuleKind::BuiltinLibrary
                if loaded.specifier == SANDBOX_FS_LIBRARY_SPECIFIER =>
            {
                Ok(host_service_metadata(
                    &loaded.specifier,
                    FS_HOST_EXPORTS
                        .iter()
                        .map(|name| (*name).to_string())
                        .collect(),
                ))
            }
            SandboxModuleKind::BuiltinLibrary
                if loaded.specifier == SANDBOX_GIT_LIBRARY_SPECIFIER =>
            {
                let mut operations = GIT_REPO_HOST_EXPORTS
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect::<Vec<_>>();
                if self
                    .session_info
                    .provenance
                    .capabilities
                    .contains(GIT_REMOTE_IMPORT_CAPABILITY_SPECIFIER)
                {
                    operations.insert(0, GIT_REMOTE_IMPORT_HOST_EXPORT.to_string());
                }
                Ok(host_service_metadata(&loaded.specifier, operations))
            }
            SandboxModuleKind::Npm => Ok(BTreeMap::from([(
                "package".to_string(),
                JsonValue::Bool(true),
            )])),
            SandboxModuleKind::Workspace | SandboxModuleKind::BuiltinLibrary => Ok(BTreeMap::new()),
        }
    }
}

#[async_trait(?Send)]
impl JsModuleLoader for SandboxModuleLoader {
    async fn resolve(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<JsResolvedModule, JsSubstrateError> {
        let resolved = self
            .resolve_specifier(specifier, referrer)
            .await
            .map_err(|error| sandbox_error_to_js_substrate(error, specifier))?;
        self.to_js_resolved_module(specifier, &resolved)
            .map_err(|error| sandbox_error_to_js_substrate(error, specifier))
    }

    async fn load(&self, resolved: &JsResolvedModule) -> Result<JsLoadedModule, JsSubstrateError> {
        let loaded = self
            .load_resolved(&resolved.canonical_specifier)
            .await
            .map_err(|error| {
                sandbox_error_to_js_substrate(error, resolved.canonical_specifier.clone())
            })?;
        self.record_loaded_module(&loaded).await;
        self.to_js_loaded_module(loaded).map_err(|error| {
            sandbox_error_to_js_substrate(error, resolved.canonical_specifier.clone())
        })
    }
}

fn referrer_to_workspace_path(referrer: &str) -> Result<String, SandboxError> {
    if referrer.starts_with(TERRACE_WORKSPACE_PREFIX) {
        return SandboxModuleSpecifier::parse(referrer).and_then(|specifier| match specifier {
            SandboxModuleSpecifier::Workspace { path } => Ok(path),
            _ => Err(SandboxError::InvalidModuleSpecifier {
                specifier: referrer.to_string(),
            }),
        });
    }
    if referrer.starts_with("/workspace") {
        return Ok(normalize_path(referrer));
    }
    Err(SandboxError::InvalidModuleSpecifier {
        specifier: referrer.to_string(),
    })
}

fn package_name_for_import(specifier: &str) -> Result<String, SandboxError> {
    if specifier.is_empty() {
        return Err(SandboxError::InvalidModuleSpecifier {
            specifier: specifier.to_string(),
        });
    }
    if let Some(stripped) = specifier.strip_prefix('@') {
        let mut segments = stripped.split('/');
        let scope = segments
            .next()
            .ok_or_else(|| SandboxError::InvalidModuleSpecifier {
                specifier: specifier.to_string(),
            })?;
        let package = segments
            .next()
            .ok_or_else(|| SandboxError::InvalidModuleSpecifier {
                specifier: specifier.to_string(),
            })?;
        if segments.next().is_some() {
            return Err(SandboxError::UnsupportedPackage {
                package: specifier.to_string(),
                reason: "npm subpath imports are not supported in version 1".to_string(),
            });
        }
        return Ok(format!("@{scope}/{package}"));
    }
    if specifier.contains('/') {
        return Err(SandboxError::UnsupportedPackage {
            package: specifier.to_string(),
            reason: "npm subpath imports are not supported in version 1".to_string(),
        });
    }
    Ok(specifier.to_string())
}

fn workspace_path_to_specifier(path: &str, workspace_root: &str) -> Result<String, SandboxError> {
    let normalized = normalize_path(path);
    if normalized == workspace_root || normalized.starts_with(&format!("{workspace_root}/")) {
        Ok(format!("terrace:{normalized}"))
    } else {
        Err(SandboxError::InvalidModuleSpecifier {
            specifier: path.to_string(),
        })
    }
}

fn normalize_path(path: &str) -> String {
    let mut parts = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    format!("/{}", parts.join("/"))
}

fn join_relative_path(referrer_path: &str, specifier: &str) -> String {
    let base = referrer_path
        .rsplit_once('/')
        .map(|(parent, _)| {
            if parent.is_empty() {
                "/".to_string()
            } else {
                parent.to_string()
            }
        })
        .unwrap_or_else(|| "/".to_string());
    normalize_path(&format!("{base}/{specifier}"))
}

fn media_type_for_path(path: &str) -> String {
    match Path::new(path).extension().and_then(|ext| ext.to_str()) {
        Some("json") => "application/json",
        Some("ts") | Some("tsx") | Some("mts") | Some("cts") => "text/typescript",
        _ => "text/javascript",
    }
    .to_string()
}

fn source_map_for(specifier: &str, source_name: &str) -> String {
    serde_json::json!({
        "version": 3,
        "file": specifier,
        "sources": [source_name],
        "names": [],
        "mappings": "",
    })
    .to_string()
}

fn cache_key_for(specifier: &str, media_type: &str, source: &str) -> String {
    let mut hasher = Hasher::new();
    hasher.update(specifier.as_bytes());
    hasher.update(media_type.as_bytes());
    hasher.update(source.as_bytes());
    format!("{:08x}", hasher.finalize())
}

fn sanitize_module_path_component(specifier: &str) -> String {
    specifier
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' => character,
            _ => '_',
        })
        .collect()
}

fn fs_preview_source() -> String {
    FS_HOST_EXPORTS
        .iter()
        .map(|name| format!("export async function {name}(...args) {{ /* synthetic */ }}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn git_preview_source() -> String {
    GIT_HOST_EXPORTS
        .iter()
        .map(|name| format!("export async function {name}(...args) {{ /* synthetic */ }}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn capability_catalog_preview_source(
    manifest: &crate::CapabilityManifest,
    registry: &dyn CapabilityRegistry,
) -> String {
    let mut used_exports = std::collections::BTreeSet::<String>::new();
    let mut imports = Vec::new();
    let mut bindings = Vec::new();
    let mut object_entries = Vec::new();

    for capability in &manifest.capabilities {
        let base = capability_catalog_export_name(&capability.name);
        let mut export_name = base.clone();
        let mut suffix = 2_u32;
        while !used_exports.insert(export_name.clone()) {
            export_name = format!("{base}{suffix}");
            suffix = suffix.saturating_add(1);
        }
        let specifier = json_quote(&capability.specifier)
            .expect("capability specifier should serialize as JSON");
        if let Some(module) = registry.module(&capability.specifier) {
            let mut method_entries = Vec::new();
            for method in module.methods {
                let method_export =
                    capability_catalog_export_name(&format!("{}_{}", capability.name, method.name));
                let property = json_quote(&method.name)
                    .expect("capability method name should serialize as JSON");
                imports.push(format!(
                    "import {{ {} as {} }} from {};",
                    method.name, method_export, specifier
                ));
                method_entries.push(format!("  {property}: {method_export},"));
            }
            bindings.push(format!("export const {export_name} = {{"));
            bindings.extend(method_entries);
            bindings.push("};".to_string());
        } else {
            imports.push(format!("import * as {export_name} from {specifier};"));
            bindings.push(format!("export {{ {export_name} }};"));
        }
        object_entries.push(format!("  {export_name},"));
    }

    let mut lines = Vec::<String>::new();
    lines.extend(imports);
    lines.extend(bindings);
    lines.push("export const capabilities = {".to_string());
    lines.extend(object_entries);
    lines.push("};".to_string());
    lines.push("export default capabilities;".to_string());
    lines.join("\n")
}

fn capability_catalog_export_name(name: &str) -> String {
    let mut result = String::new();
    let mut uppercase_next = true;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            if result.is_empty() && ch.is_ascii_digit() {
                result.push('C');
            }
            if uppercase_next {
                result.push(ch.to_ascii_uppercase());
            } else {
                result.push(ch);
            }
            uppercase_next = false;
        } else {
            uppercase_next = true;
        }
    }
    if result.is_empty() {
        "Capability".to_string()
    } else {
        result
    }
}

fn capability_preview_source(module: &SandboxCapabilityModule) -> String {
    module
        .methods
        .iter()
        .map(|method| {
            format!(
                "export async function {}(...args) {{ /* synthetic */ }}",
                method.name
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn js_module_kind_for_specifier(specifier: &str) -> Result<JsModuleKind, SandboxError> {
    let parsed = SandboxModuleSpecifier::parse(specifier)?;
    Ok(match parsed {
        SandboxModuleSpecifier::Workspace { .. } => JsModuleKind::Workspace,
        SandboxModuleSpecifier::HostCapability { .. } => JsModuleKind::HostCapability,
        SandboxModuleSpecifier::Npm { .. } => JsModuleKind::Package,
        SandboxModuleSpecifier::NodeBuiltin { .. } => JsModuleKind::HostCapability,
        SandboxModuleSpecifier::BuiltinLibrary { specifier }
            if matches!(
                specifier.as_str(),
                SANDBOX_FS_LIBRARY_SPECIFIER | SANDBOX_GIT_LIBRARY_SPECIFIER
            ) =>
        {
            JsModuleKind::HostCapability
        }
        SandboxModuleSpecifier::BuiltinLibrary { .. } => JsModuleKind::Workspace,
    })
}

fn js_module_kind_for_loaded(loaded: &LoadedSandboxModule) -> JsModuleKind {
    match loaded.kind {
        SandboxModuleKind::Workspace => JsModuleKind::Workspace,
        SandboxModuleKind::HostCapability => JsModuleKind::HostCapability,
        SandboxModuleKind::Npm => JsModuleKind::Package,
        SandboxModuleKind::NodeBuiltin => JsModuleKind::HostCapability,
        SandboxModuleKind::BuiltinLibrary
            if matches!(
                loaded.specifier.as_str(),
                SANDBOX_FS_LIBRARY_SPECIFIER | SANDBOX_GIT_LIBRARY_SPECIFIER
            ) =>
        {
            JsModuleKind::HostCapability
        }
        SandboxModuleKind::BuiltinLibrary => JsModuleKind::Workspace,
    }
}

fn host_service_metadata(service: &str, exports: Vec<String>) -> BTreeMap<String, JsonValue> {
    BTreeMap::from([
        ("synthetic".to_string(), JsonValue::Bool(true)),
        (
            "host_service".to_string(),
            JsonValue::String(service.to_string()),
        ),
        (
            "host_exports".to_string(),
            JsonValue::Array(exports.into_iter().map(JsonValue::String).collect()),
        ),
    ])
}

fn sandbox_error_to_js_substrate(
    error: SandboxError,
    entrypoint: impl Into<String>,
) -> JsSubstrateError {
    match error {
        SandboxError::InvalidModuleSpecifier { specifier } => {
            JsSubstrateError::UnsupportedSpecifier { specifier }
        }
        SandboxError::ModuleNotFound { specifier } => {
            JsSubstrateError::ModuleNotFound { specifier }
        }
        SandboxError::Vfs(error) => JsSubstrateError::Vfs(error),
        other => JsSubstrateError::EvaluationFailed {
            entrypoint: entrypoint.into(),
            message: other.to_string(),
        },
    }
}
