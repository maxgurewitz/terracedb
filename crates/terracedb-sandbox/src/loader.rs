use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, Mutex as StdMutex},
};

use boa_engine::{
    Context, JsError, JsNativeError, JsResult, JsString, JsValue, Module, NativeFunction, Source,
    js_string,
    module::{ModuleLoader as BoaModuleLoader, Referrer, SyntheticModuleInitializer},
    native_function::NativeFunctionPointer,
    object::FunctionObjectBuilder,
};
use crc32fast::Hasher;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use terracedb_vfs::{CreateOptions, JsonValue, MkdirOptions};
use tokio::{sync::Mutex, task_local};

use crate::{
    CapabilityCallRequest, CapabilityCallResult, CapabilityRegistry, PackageCompatibilityMode,
    SandboxCapabilityModule, SandboxError, SandboxFilesystemShim, SandboxRuntimeStateHandle,
    SandboxSession, SandboxSessionInfo, packages::read_package_install_manifest,
};

pub const TERRACE_WORKSPACE_PREFIX: &str = "terrace:/workspace";
pub const HOST_CAPABILITY_PREFIX: &str = "terrace:host/";
pub const SANDBOX_FS_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/fs";
pub const SANDBOX_BASH_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/bash";
pub const SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/typescript";

const CAPTURE_SEPARATOR: &str = "\u{001f}";

task_local! {
    static ACTIVE_GUEST_HOST: Arc<ActiveGuestHost>;
}

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
            SANDBOX_FS_LIBRARY_SPECIFIER
                | SANDBOX_BASH_LIBRARY_SPECIFIER
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

    pub async fn load(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<LoadedSandboxModule, SandboxError> {
        let resolved = self.resolve(specifier, referrer).await?;
        let parsed = SandboxModuleSpecifier::parse(&resolved)?;
        let loaded = match parsed {
            SandboxModuleSpecifier::Workspace { path } => self.load_workspace_module(&path).await?,
            SandboxModuleSpecifier::HostCapability { .. } => {
                self.load_capability_module(&resolved).await?
            }
            SandboxModuleSpecifier::BuiltinLibrary { .. } => self.load_builtin_module(&resolved)?,
            SandboxModuleSpecifier::NodeBuiltin { .. } => {
                self.load_node_builtin_module(&resolved)?
            }
            SandboxModuleSpecifier::Npm { .. } => self.load_npm_module(&resolved).await?,
        };
        self.record_loaded_module(&loaded).await;
        Ok(loaded)
    }

    pub async fn trace_snapshot(&self) -> SandboxModuleLoadTrace {
        self.trace.lock().await.clone()
    }

    pub(crate) fn to_boa_module(
        &self,
        loaded: LoadedSandboxModule,
        context: &mut Context,
    ) -> JsResult<Module> {
        let runtime_path = PathBuf::from(&loaded.runtime_path);
        match loaded.kind {
            SandboxModuleKind::Workspace => {
                let source = Source::from_bytes(loaded.source.as_bytes()).with_path(&runtime_path);
                Module::parse(source, None, context)
            }
            SandboxModuleKind::HostCapability => self.synthetic_capability_module(
                runtime_path,
                &loaded.specifier,
                &loaded.source,
                context,
            ),
            SandboxModuleKind::BuiltinLibrary | SandboxModuleKind::NodeBuiltin => {
                if loaded.specifier == SANDBOX_FS_LIBRARY_SPECIFIER
                    || matches!(loaded.specifier.as_str(), "node:fs" | "node:fs/promises")
                {
                    self.synthetic_fs_module(runtime_path, context)
                } else {
                    let source =
                        Source::from_bytes(loaded.source.as_bytes()).with_path(&runtime_path);
                    Module::parse(source, None, context)
                }
            }
            SandboxModuleKind::Npm => {
                let source = Source::from_bytes(loaded.source.as_bytes()).with_path(&runtime_path);
                Module::parse(source, None, context)
            }
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
            SANDBOX_BASH_LIBRARY_SPECIFIER => {
                "export const unavailable = true;\nexport const service = 'bash';\n".to_string()
            }
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

    fn synthetic_fs_module(
        &self,
        runtime_path: PathBuf,
        context: &mut Context,
    ) -> JsResult<Module> {
        let exports = [
            js_string!("readTextFile"),
            js_string!("writeTextFile"),
            js_string!("readJsonFile"),
            js_string!("writeJsonFile"),
            js_string!("mkdir"),
            js_string!("readdir"),
            js_string!("stat"),
            js_string!("unlink"),
            js_string!("rmdir"),
            js_string!("rename"),
            js_string!("fsync"),
        ];
        Ok(Module::synthetic(
            &exports,
            SyntheticModuleInitializer::from_copy_closure(|module, context| {
                set_exported_native(module, context, "readTextFile", 1, fs_read_text_file)?;
                set_exported_native(module, context, "writeTextFile", 2, fs_write_text_file)?;
                set_exported_native(module, context, "readJsonFile", 1, fs_read_json_file)?;
                set_exported_native(module, context, "writeJsonFile", 2, fs_write_json_file)?;
                set_exported_native(module, context, "mkdir", 1, fs_mkdir)?;
                set_exported_native(module, context, "readdir", 1, fs_readdir)?;
                set_exported_native(module, context, "stat", 1, fs_stat)?;
                set_exported_native(module, context, "unlink", 1, fs_unlink)?;
                set_exported_native(module, context, "rmdir", 1, fs_rmdir)?;
                set_exported_native(module, context, "rename", 2, fs_rename)?;
                set_exported_native(module, context, "fsync", 1, fs_fsync)?;
                Ok(())
            }),
            Some(runtime_path),
            None,
            context,
        ))
    }

    fn synthetic_capability_module(
        &self,
        runtime_path: PathBuf,
        specifier: &str,
        _preview_source: &str,
        context: &mut Context,
    ) -> JsResult<Module> {
        let module = self.capabilities.module(specifier).ok_or_else(|| {
            js_error(SandboxError::CapabilityUnavailable {
                specifier: specifier.to_string(),
            })
        })?;
        let export_names = module
            .methods
            .iter()
            .map(|method| JsString::from(method.name.as_str()))
            .collect::<Vec<_>>();
        let encoded = format!(
            "{specifier}{CAPTURE_SEPARATOR}{}",
            module
                .methods
                .iter()
                .map(|method| method.name.as_str())
                .collect::<Vec<_>>()
                .join(CAPTURE_SEPARATOR)
        );
        Ok(Module::synthetic(
            &export_names,
            SyntheticModuleInitializer::from_copy_closure_with_captures(
                |module, encoded, context| {
                    let encoded = encoded.to_std_string_escaped();
                    let (specifier, methods) =
                        encoded.split_once(CAPTURE_SEPARATOR).ok_or_else(|| {
                            js_error(SandboxError::Service {
                                service: "capabilities",
                                message: "synthetic capability module capture is invalid"
                                    .to_string(),
                            })
                        })?;
                    for method in methods
                        .split(CAPTURE_SEPARATOR)
                        .filter(|candidate| !candidate.is_empty())
                    {
                        let capture =
                            JsString::from(format!("{specifier}{CAPTURE_SEPARATOR}{method}"));
                        let function = FunctionObjectBuilder::new(
                            context.realm(),
                            NativeFunction::from_copy_closure_with_captures(
                                capability_dispatch,
                                capture,
                            ),
                        )
                        .name(JsString::from(method))
                        .length(0)
                        .constructor(false)
                        .build();
                        module.set_export(&JsString::from(method), function.into())?;
                    }
                    Ok(())
                },
                JsString::from(encoded),
            ),
            Some(runtime_path),
            None,
            context,
        ))
    }
}

impl BoaModuleLoader for SandboxModuleLoader {
    async fn load_imported_module(
        self: Rc<Self>,
        referrer: Referrer,
        specifier: JsString,
        context: &RefCell<&mut Context>,
    ) -> JsResult<Module> {
        let referrer = referrer.path().and_then(Path::to_str).map(str::to_string);
        let loaded = self
            .load(&specifier.to_std_string_escaped(), referrer.as_deref())
            .await
            .map_err(js_error)?;
        self.to_boa_module(loaded, &mut context.borrow_mut())
    }
}

#[derive(Clone)]
pub(crate) struct ActiveGuestHost {
    session: SandboxSession,
    capability_calls: Arc<StdMutex<Vec<CapabilityCallResult>>>,
}

impl ActiveGuestHost {
    pub(crate) fn new(session: SandboxSession) -> Self {
        Self {
            session,
            capability_calls: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    pub(crate) fn capability_calls(&self) -> Vec<CapabilityCallResult> {
        self.capability_calls
            .lock()
            .expect("capability call log lock poisoned")
            .clone()
    }

    fn push_capability_call(&self, result: CapabilityCallResult) {
        self.capability_calls
            .lock()
            .expect("capability call log lock poisoned")
            .push(result);
    }
}

pub(crate) async fn with_active_guest_host<F, T>(host: Arc<ActiveGuestHost>, future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    ACTIVE_GUEST_HOST.scope(host, future).await
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
    [
        "export function readTextFile(path) { /* synthetic */ }",
        "export function writeTextFile(path, contents) { /* synthetic */ }",
        "export function readJsonFile(path) { /* synthetic */ }",
        "export function writeJsonFile(path, value) { /* synthetic */ }",
        "export function mkdir(path) { /* synthetic */ }",
        "export function readdir(path) { /* synthetic */ }",
        "export function stat(path) { /* synthetic */ }",
        "export function unlink(path) { /* synthetic */ }",
        "export function rmdir(path) { /* synthetic */ }",
        "export function rename(from, to) { /* synthetic */ }",
        "export function fsync(path) { /* synthetic */ }",
    ]
    .join("\n")
}

fn capability_preview_source(module: &SandboxCapabilityModule) -> String {
    module
        .methods
        .iter()
        .map(|method| {
            format!(
                "export function {}(...args) {{ /* synthetic */ }}",
                method.name
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn set_exported_native(
    module: &boa_engine::module::SyntheticModule,
    context: &mut Context,
    name: &str,
    length: usize,
    function: NativeFunctionPointer,
) -> JsResult<()> {
    let export = FunctionObjectBuilder::new(context.realm(), NativeFunction::from_fn_ptr(function))
        .name(JsString::from(name))
        .length(length)
        .constructor(false)
        .build();
    module.set_export(&JsString::from(name), export.into())
}

fn capability_dispatch(
    _this: &JsValue,
    args: &[JsValue],
    capture: &JsString,
    context: &mut Context,
) -> JsResult<JsValue> {
    let encoded = capture.to_std_string_escaped();
    let (specifier, method) = encoded.split_once(CAPTURE_SEPARATOR).ok_or_else(|| {
        js_error(SandboxError::Service {
            service: "capabilities",
            message: "capability capture is invalid".to_string(),
        })
    })?;
    let arguments = js_args_to_json(args, context)?;
    with_guest_host(|host| {
        let result = block_on(host.session.invoke_capability(CapabilityCallRequest {
            specifier: specifier.to_string(),
            method: method.to_string(),
            args: arguments,
        }))?;
        host.push_capability_call(result.clone());
        Ok(result)
    })
    .and_then(|result| JsValue::from_json(&result.value, context))
}

fn fs_read_text_file(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = required_string_arg(args, 0, context, "path")?;
    with_guest_host(|host| {
        let bytes = block_on(host.session.filesystem().read_file(&path))?.ok_or_else(|| {
            SandboxError::Service {
                service: "filesystem",
                message: format!("path not found: {path}"),
            }
        })?;
        String::from_utf8(bytes).map_err(|error| SandboxError::Service {
            service: "filesystem",
            message: format!("path {path} is not valid UTF-8: {error}"),
        })
    })
    .map(|value| JsValue::from(JsString::from(value)))
}

fn fs_write_text_file(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = required_string_arg(args, 0, context, "path")?;
    let contents = required_string_arg(args, 1, context, "contents")?;
    with_guest_host(|host| {
        block_on(host.session.filesystem().write_file(
            &path,
            contents.into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        ))?;
        Ok(JsValue::undefined())
    })
}

fn fs_read_json_file(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let contents = fs_read_text_file(&JsValue::undefined(), args, context)?;
    let parsed: serde_json::Value =
        serde_json::from_str(&required_value_as_string(&contents, context, "json text")?).map_err(
            |error| {
                js_error(SandboxError::Service {
                    service: "filesystem",
                    message: format!("json parse failed: {error}"),
                })
            },
        )?;
    JsValue::from_json(&parsed, context)
}

fn fs_write_json_file(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> JsResult<JsValue> {
    let path = required_string_arg(args, 0, context, "path")?;
    let value = args.get(1).cloned().unwrap_or_else(JsValue::undefined);
    let json = value.to_json(context)?.unwrap_or(JsonValue::Null);
    let pretty = serde_json::to_vec_pretty(&json).map_err(|error| {
        js_error(SandboxError::Service {
            service: "filesystem",
            message: format!("json encode failed: {error}"),
        })
    })?;
    with_guest_host(|host| {
        block_on(host.session.filesystem().write_file(
            &path,
            pretty,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        ))?;
        Ok(JsValue::undefined())
    })
}

fn fs_mkdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = required_string_arg(args, 0, context, "path")?;
    with_guest_host(|host| {
        block_on(host.session.filesystem().mkdir(
            &path,
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        ))?;
        Ok(JsValue::undefined())
    })
}

fn fs_readdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = required_string_arg(args, 0, context, "path")?;
    let entries = with_guest_host(|host| block_on(host.session.filesystem().readdir(&path)))?;
    let json = entries
        .into_iter()
        .map(|entry| {
            serde_json::json!({
                "name": entry.name,
                "inode": entry.inode.to_string(),
                "kind": format!("{:?}", entry.kind).to_lowercase(),
            })
        })
        .collect::<Vec<_>>();
    JsValue::from_json(&JsonValue::Array(json), context)
}

fn fs_stat(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = required_string_arg(args, 0, context, "path")?;
    let stats = with_guest_host(|host| block_on(host.session.filesystem().stat(&path)))?;
    let value = stats.map(stats_to_json).unwrap_or(JsonValue::Null);
    JsValue::from_json(&value, context)
}

fn fs_unlink(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = required_string_arg(args, 0, context, "path")?;
    with_guest_host(|host| {
        block_on(host.session.filesystem().unlink(&path))?;
        Ok(JsValue::undefined())
    })
}

fn fs_rmdir(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = required_string_arg(args, 0, context, "path")?;
    with_guest_host(|host| {
        block_on(host.session.filesystem().rmdir(&path))?;
        Ok(JsValue::undefined())
    })
}

fn fs_rename(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let from = required_string_arg(args, 0, context, "from")?;
    let to = required_string_arg(args, 1, context, "to")?;
    with_guest_host(|host| {
        block_on(host.session.filesystem().rename(&from, &to))?;
        Ok(JsValue::undefined())
    })
}

fn fs_fsync(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = optional_string_arg(args, 0, context)?;
    with_guest_host(|host| {
        block_on(host.session.filesystem().fsync(path.as_deref()))?;
        Ok(JsValue::undefined())
    })
}

fn optional_string_arg(
    args: &[JsValue],
    index: usize,
    context: &mut Context,
) -> JsResult<Option<String>> {
    match args.get(index) {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            Ok(Some(required_value_as_string(value, context, "string")?))
        }
        _ => Ok(None),
    }
}

fn required_string_arg(
    args: &[JsValue],
    index: usize,
    context: &mut Context,
    name: &str,
) -> JsResult<String> {
    let value = args.get(index).cloned().unwrap_or_else(JsValue::undefined);
    required_value_as_string(&value, context, name)
}

fn required_value_as_string(
    value: &JsValue,
    context: &mut Context,
    name: &str,
) -> JsResult<String> {
    if value.is_null() || value.is_undefined() {
        return Err(js_error(SandboxError::Service {
            service: "runtime",
            message: format!("missing required argument: {name}"),
        }));
    }
    Ok(value.to_string(context)?.to_std_string_escaped())
}

fn js_args_to_json(args: &[JsValue], context: &mut Context) -> JsResult<Vec<JsonValue>> {
    args.iter()
        .map(|value| Ok(value.to_json(context)?.unwrap_or(JsonValue::Null)))
        .collect()
}

fn with_guest_host<T>(f: impl FnOnce(&ActiveGuestHost) -> Result<T, SandboxError>) -> JsResult<T> {
    ACTIVE_GUEST_HOST
        .try_with(|host| f(host))
        .map_err(|_| {
            js_error(SandboxError::Service {
                service: "runtime",
                message: "sandbox guest host is not active".to_string(),
            })
        })?
        .map_err(js_error)
}

fn js_error(error: SandboxError) -> JsError {
    JsNativeError::typ().with_message(error.to_string()).into()
}

fn stats_to_json(stats: terracedb_vfs::Stats) -> JsonValue {
    serde_json::json!({
        "inode": stats.inode.to_string(),
        "kind": format!("{:?}", stats.kind).to_lowercase(),
        "mode": stats.mode,
        "nlink": stats.nlink,
        "uid": stats.uid,
        "gid": stats.gid,
        "size": stats.size,
        "created_at": stats.created_at.get(),
        "modified_at": stats.modified_at.get(),
        "changed_at": stats.changed_at.get(),
        "accessed_at": stats.accessed_at.get(),
        "rdev": stats.rdev,
    })
}
