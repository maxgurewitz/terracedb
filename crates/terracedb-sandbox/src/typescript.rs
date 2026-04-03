use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use terracedb_vfs::CompletedToolRunOutcome;

use crate::{
    CapabilityManifest, PackageCompatibilityMode, PackageInstallRequest,
    SANDBOX_BASH_LIBRARY_SPECIFIER, SANDBOX_CAPABILITIES_LIBRARY_SPECIFIER,
    SANDBOX_FS_LIBRARY_SPECIFIER, SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER,
    SANDBOX_WORKFLOW_LIBRARY_TYPESCRIPT_DECLARATIONS, SandboxError, SandboxSession,
    packages::read_package_install_manifest, session::record_completed_tool_run,
};

pub const TERRACE_TYPESCRIPT_DECLARATIONS_PATH: &str = "/.terrace/typescript/generated.d.ts";
pub const TERRACE_TYPESCRIPT_MIRROR_PATH: &str = "/.terrace/typescript/mirror.json";
pub const TERRACE_TYPESCRIPT_STATE_PATH: &str = "/.terrace/typescript/state.json";
pub const TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR: &str = "/.terrace/typescript/transpile";

const DEFAULT_TYPESCRIPT_VERSION: &str = "5.9.3";
const TYPESCRIPT_RUNNER_SOURCE: &str = include_str!("./typescript_runner.js");

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeCheckRequest {
    pub roots: Vec<String>,
    #[serde(default)]
    pub tsconfig_path: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub module_kind: Option<String>,
    #[serde(default)]
    pub jsx: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeScriptMirrorState {
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default)]
    pub files: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct TypeScriptState {
    compiler_version: String,
    #[serde(default)]
    tsconfig_path: Option<String>,
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
    #[serde(default)]
    pub checked_files: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeScriptEmitReport {
    #[serde(default)]
    pub diagnostics: Vec<TypeScriptDiagnostic>,
    pub emitted_files: Vec<String>,
    #[serde(default)]
    pub root_outputs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeScriptTranspileRequest {
    pub path: String,
    #[serde(default = "default_transpile_target")]
    pub target: String,
    #[serde(default = "default_transpile_module_kind")]
    pub module_kind: String,
    #[serde(default)]
    pub jsx: Option<String>,
    #[serde(default)]
    pub tsconfig_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeScriptTranspileReport {
    pub input_path: String,
    pub cache_key: String,
    pub output_path: String,
    pub source_map_path: String,
    pub cache_hit: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TypeScriptRunMode {
    Check,
    Emit,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TypeScriptRunnerRequest {
    mode: TypeScriptRunMode,
    roots: Vec<String>,
    #[serde(default)]
    tsconfig_path: Option<String>,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    module_kind: Option<String>,
    #[serde(default)]
    jsx: Option<String>,
    #[serde(default)]
    force_source_map: bool,
    cwd: String,
    typescript_lib_root: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct TypeScriptRunnerResponse {
    #[serde(default)]
    diagnostics: Vec<TypeScriptDiagnostic>,
    #[serde(default)]
    checked_files: Vec<String>,
    #[serde(default)]
    emitted_files: Vec<String>,
    #[serde(default)]
    root_outputs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedTypeScriptCompiler {
    version: String,
    lib_root: String,
}

#[async_trait]
pub trait TypeScriptService: Send + Sync {
    fn name(&self) -> &str;
    async fn transpile(
        &self,
        session: &SandboxSession,
        request: TypeScriptTranspileRequest,
    ) -> Result<TypeScriptTranspileReport, SandboxError>;
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

    async fn check_inner(
        &self,
        session: &SandboxSession,
        request: &TypeCheckRequest,
    ) -> Result<TypeCheckReport, SandboxError> {
        let info = session.info().await;
        let compiler =
            prepare_compiler_inputs(session, &request.roots, request.tsconfig_path.as_deref())
                .await?;
        let roots = normalized_roots(&info.workspace_root, &request.roots)?;
        let tsconfig_path = request
            .tsconfig_path
            .as_deref()
            .map(|path| resolve_workspace_path(&info.workspace_root, path))
            .transpose()?;
        let response = run_typescript(
            session,
            TypeScriptRunnerRequest {
                mode: TypeScriptRunMode::Check,
                roots: roots.clone(),
                tsconfig_path: tsconfig_path.clone(),
                target: request.target.clone(),
                module_kind: request.module_kind.clone(),
                jsx: request.jsx.clone(),
                force_source_map: false,
                cwd: info.workspace_root,
                typescript_lib_root: compiler.lib_root,
            },
        )
        .await?;
        persist_typescript_state(
            session,
            &compiler.version,
            &roots,
            &response.checked_files,
            tsconfig_path.as_deref(),
        )
        .await?;
        Ok(TypeCheckReport {
            diagnostics: response.diagnostics,
            checked_files: response.checked_files,
        })
    }

    async fn emit_inner(
        &self,
        session: &SandboxSession,
        request: &TypeCheckRequest,
    ) -> Result<TypeScriptEmitReport, SandboxError> {
        let info = session.info().await;
        let compiler =
            prepare_compiler_inputs(session, &request.roots, request.tsconfig_path.as_deref())
                .await?;
        let roots = normalized_roots(&info.workspace_root, &request.roots)?;
        let tsconfig_path = request
            .tsconfig_path
            .as_deref()
            .map(|path| resolve_workspace_path(&info.workspace_root, path))
            .transpose()?;
        let response = run_typescript(
            session,
            TypeScriptRunnerRequest {
                mode: TypeScriptRunMode::Emit,
                roots: roots.clone(),
                tsconfig_path: tsconfig_path.clone(),
                target: request.target.clone(),
                module_kind: request.module_kind.clone(),
                jsx: request.jsx.clone(),
                force_source_map: false,
                cwd: info.workspace_root,
                typescript_lib_root: compiler.lib_root,
            },
        )
        .await?;
        persist_typescript_state(
            session,
            &compiler.version,
            &roots,
            &response.checked_files,
            tsconfig_path.as_deref(),
        )
        .await?;
        Ok(TypeScriptEmitReport {
            diagnostics: response.diagnostics,
            emitted_files: response.emitted_files,
            root_outputs: response.root_outputs,
        })
    }

    async fn transpile_inner(
        &self,
        session: &SandboxSession,
        request: &TypeScriptTranspileRequest,
    ) -> Result<TypeScriptTranspileReport, SandboxError> {
        let info = session.info().await;
        let input_path = resolve_workspace_path(&info.workspace_root, &request.path)?;
        let compiler = prepare_compiler_inputs(
            session,
            std::slice::from_ref(&input_path),
            request.tsconfig_path.as_deref(),
        )
        .await?;
        let response = run_typescript(
            session,
            TypeScriptRunnerRequest {
                mode: TypeScriptRunMode::Emit,
                roots: vec![input_path.clone()],
                tsconfig_path: request
                    .tsconfig_path
                    .as_deref()
                    .map(|path| resolve_workspace_path(&info.workspace_root, path))
                    .transpose()?,
                target: Some(request.target.clone()),
                module_kind: Some(request.module_kind.clone()),
                jsx: request.jsx.clone(),
                force_source_map: true,
                cwd: info.workspace_root.clone(),
                typescript_lib_root: compiler.lib_root.clone(),
            },
        )
        .await?;
        let cache_key = stable_hash_hex(
            serde_json::to_vec(&serde_json::json!({
                "path": input_path,
                "compiler": compiler.version,
                "target": request.target,
                "module_kind": request.module_kind,
                "jsx": request.jsx,
                "tsconfig_path": request.tsconfig_path,
            }))?
            .as_slice(),
        );
        let metadata_path = format!("{TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR}/{cache_key}.json");
        if let Some(bytes) = session.filesystem().read_file(&metadata_path).await? {
            let mut cached = serde_json::from_slice::<TypeScriptTranspileReport>(&bytes)?;
            cached.cache_hit = true;
            return Ok(cached);
        }
        if !response.diagnostics.is_empty() {
            return Err(SandboxError::Service {
                service: "typescript",
                message: format!(
                    "TypeScript transpile failed: {}",
                    summarize_diagnostics(&response.diagnostics)
                ),
            });
        }
        let output_path = response
            .root_outputs
            .get(&input_path)
            .cloned()
            .unwrap_or_else(|| emitted_output_path(&input_path));
        persist_typescript_state(
            session,
            &compiler.version,
            std::slice::from_ref(&input_path),
            &response.checked_files,
            request.tsconfig_path.as_deref(),
        )
        .await?;
        let report = TypeScriptTranspileReport {
            input_path: input_path.clone(),
            cache_key,
            output_path: output_path.clone(),
            source_map_path: format!("{output_path}.map"),
            cache_hit: false,
        };
        session
            .filesystem()
            .write_file(
                &metadata_path,
                serde_json::to_vec_pretty(&report)?,
                terracedb_vfs::CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await?;
        Ok(report)
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

    async fn transpile(
        &self,
        session: &SandboxSession,
        request: TypeScriptTranspileRequest,
    ) -> Result<TypeScriptTranspileReport, SandboxError> {
        let operation_lock = session.operation_lock();
        let _guard = operation_lock.lock().await;
        if session.execution_policy().await.is_some() {
            return self.transpile_inner(session, &request).await;
        }
        let params = Some(serde_json::to_value(&request)?);
        match self.transpile_inner(session, &request).await {
            Ok(report) => {
                record_completed_tool_run(
                    session.volume().as_ref(),
                    "sandbox.typescript.transpile",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await?;
                Ok(report)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    session.volume().as_ref(),
                    "sandbox.typescript.transpile",
                    params,
                    CompletedToolRunOutcome::Error {
                        message: error.to_string(),
                    },
                )
                .await;
                Err(error)
            }
        }
    }

    async fn check(
        &self,
        session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeCheckReport, SandboxError> {
        let operation_lock = session.operation_lock();
        let _guard = operation_lock.lock().await;
        if session.execution_policy().await.is_some() {
            return self.check_inner(session, &request).await;
        }
        let params = Some(serde_json::to_value(&request)?);
        match self.check_inner(session, &request).await {
            Ok(report) => {
                record_completed_tool_run(
                    session.volume().as_ref(),
                    "sandbox.typescript.check",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await?;
                Ok(report)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    session.volume().as_ref(),
                    "sandbox.typescript.check",
                    params,
                    CompletedToolRunOutcome::Error {
                        message: error.to_string(),
                    },
                )
                .await;
                Err(error)
            }
        }
    }

    async fn emit(
        &self,
        session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeScriptEmitReport, SandboxError> {
        let operation_lock = session.operation_lock();
        let _guard = operation_lock.lock().await;
        if session.execution_policy().await.is_some() {
            return self.emit_inner(session, &request).await;
        }
        let params = Some(serde_json::to_value(&request)?);
        match self.emit_inner(session, &request).await {
            Ok(report) => {
                record_completed_tool_run(
                    session.volume().as_ref(),
                    "sandbox.typescript.emit",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await?;
                Ok(report)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    session.volume().as_ref(),
                    "sandbox.typescript.emit",
                    params,
                    CompletedToolRunOutcome::Error {
                        message: error.to_string(),
                    },
                )
                .await;
                Err(error)
            }
        }
    }
}

fn default_transpile_target() -> String {
    "es2022".to_string()
}

fn default_transpile_module_kind() -> String {
    "esm".to_string()
}

async fn prepare_compiler_inputs(
    session: &SandboxSession,
    roots: &[String],
    tsconfig_path: Option<&str>,
) -> Result<ResolvedTypeScriptCompiler, SandboxError> {
    write_generated_typescript_declarations(session).await?;
    ensure_typescript_compiler(session, roots, tsconfig_path).await
}

async fn write_generated_typescript_declarations(
    session: &SandboxSession,
) -> Result<(), SandboxError> {
    let info = session.info().await;
    session
        .filesystem()
        .write_file(
            TERRACE_TYPESCRIPT_DECLARATIONS_PATH,
            generated_typescript_declarations(
                &info.provenance.capabilities,
                info.provenance.package_compat,
            )
            .into_bytes(),
            terracedb_vfs::CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

async fn persist_typescript_state(
    session: &SandboxSession,
    compiler_version: &str,
    roots: &[String],
    checked_files: &[String],
    tsconfig_path: Option<&str>,
) -> Result<(), SandboxError> {
    let mirror = TypeScriptMirrorState {
        roots: roots.to_vec(),
        files: checked_files
            .iter()
            .cloned()
            .map(|path| (path, String::new()))
            .collect(),
    };
    session
        .filesystem()
        .write_file(
            TERRACE_TYPESCRIPT_MIRROR_PATH,
            serde_json::to_vec_pretty(&mirror)?,
            terracedb_vfs::CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    let state = TypeScriptState {
        compiler_version: compiler_version.to_string(),
        tsconfig_path: tsconfig_path.map(ToString::to_string),
    };
    session
        .filesystem()
        .write_file(
            TERRACE_TYPESCRIPT_STATE_PATH,
            serde_json::to_vec_pretty(&state)?,
            terracedb_vfs::CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

async fn ensure_typescript_compiler(
    session: &SandboxSession,
    roots: &[String],
    tsconfig_path: Option<&str>,
) -> Result<ResolvedTypeScriptCompiler, SandboxError> {
    if let Some(installed) = installed_typescript_compiler(session).await? {
        return Ok(installed);
    }

    let version_request = discover_typescript_version_request(session, roots, tsconfig_path)
        .await?
        .unwrap_or_else(|| DEFAULT_TYPESCRIPT_VERSION.to_string());
    session
        .install_packages(PackageInstallRequest {
            packages: vec![format!("typescript@{version_request}")],
            materialize_compatibility_view: true,
        })
        .await?;
    installed_typescript_compiler(session)
        .await?
        .ok_or_else(|| SandboxError::Service {
            service: "typescript",
            message: "typescript package did not appear after installation".to_string(),
        })
}

async fn installed_typescript_compiler(
    session: &SandboxSession,
) -> Result<Option<ResolvedTypeScriptCompiler>, SandboxError> {
    let Some(manifest) = read_package_install_manifest(session.filesystem().as_ref()).await? else {
        return Ok(None);
    };
    let Some(entry) = manifest.package("typescript") else {
        return Ok(None);
    };
    let package_root = entry
        .compatibility_root
        .clone()
        .unwrap_or_else(|| entry.session_root.clone());
    Ok(Some(ResolvedTypeScriptCompiler {
        version: entry.version.clone(),
        lib_root: format!("{package_root}/lib"),
    }))
}

async fn discover_typescript_version_request(
    session: &SandboxSession,
    roots: &[String],
    tsconfig_path: Option<&str>,
) -> Result<Option<String>, SandboxError> {
    let info = session.info().await;
    let mut candidates = BTreeSet::new();
    if let Some(path) = tsconfig_path {
        collect_package_manifest_candidates(
            &info.workspace_root,
            &resolve_workspace_path(&info.workspace_root, path)?,
            &mut candidates,
        )?;
    }
    for root in roots {
        collect_package_manifest_candidates(
            &info.workspace_root,
            &resolve_workspace_path(&info.workspace_root, root)?,
            &mut candidates,
        )?;
    }
    candidates.insert(format!("{}/package.json", info.workspace_root));
    for candidate in candidates {
        let Some(bytes) = session.filesystem().read_file(&candidate).await? else {
            continue;
        };
        let manifest: serde_json::Value = serde_json::from_slice(&bytes)?;
        for section in ["devDependencies", "dependencies"] {
            if let Some(version) = manifest
                .get(section)
                .and_then(|value| value.as_object())
                .and_then(|deps| deps.get("typescript"))
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                return Ok(Some(version.to_string()));
            }
        }
    }
    Ok(None)
}

fn collect_package_manifest_candidates(
    workspace_root: &str,
    path: &str,
    candidates: &mut BTreeSet<String>,
) -> Result<(), SandboxError> {
    let mut current = if path.ends_with(".json")
        || path.ends_with(".ts")
        || path.ends_with(".tsx")
        || path.ends_with(".mts")
        || path.ends_with(".cts")
        || path.ends_with(".js")
        || path.ends_with(".jsx")
        || path.ends_with(".mjs")
        || path.ends_with(".cjs")
    {
        parent_path(path)
    } else {
        path.to_string()
    };
    let workspace_root = normalize_internal_path(workspace_root)?;
    loop {
        candidates.insert(format!("{current}/package.json"));
        if current == workspace_root || current == "/" {
            break;
        }
        let parent = parent_path(&current);
        if parent == current {
            break;
        }
        current = parent;
    }
    Ok(())
}

fn normalized_roots(workspace_root: &str, roots: &[String]) -> Result<Vec<String>, SandboxError> {
    roots
        .iter()
        .map(|path| resolve_workspace_path(workspace_root, path))
        .collect()
}

async fn run_typescript(
    session: &SandboxSession,
    request: TypeScriptRunnerRequest,
) -> Result<TypeScriptRunnerResponse, SandboxError> {
    let payload = serde_json::to_string(&request)?;
    let source = format!(
        "{}\nexport default runTypeScript({payload});\n",
        TYPESCRIPT_RUNNER_SOURCE
    );
    let workspace_root = session.info().await.workspace_root;
    let session = session.clone();
    let virtual_specifier = format!("terrace:{workspace_root}/.terrace/typescript/runner-eval.mjs");
    let result = tokio::task::spawn_blocking(move || -> Result<_, SandboxError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| SandboxError::Service {
                service: "typescript",
                message: format!("failed to build local runtime for tsc: {error}"),
            })?;
        runtime.block_on(async move {
            session
                .eval_untracked(source, Some(virtual_specifier))
                .await
        })
    })
    .await
    .map_err(|error| SandboxError::Service {
        service: "typescript",
        message: format!("typescript runner task failed: {error}"),
    })??;
    serde_json::from_value(result.result.ok_or_else(|| SandboxError::Service {
        service: "typescript",
        message: "typescript runner did not produce a JSON result".to_string(),
    })?)
    .map_err(Into::into)
}

fn summarize_diagnostics(diagnostics: &[TypeScriptDiagnostic]) -> String {
    diagnostics
        .iter()
        .map(|diagnostic| match diagnostic.code.as_deref() {
            Some(code) => format!("{} {}: {}", diagnostic.path, code, diagnostic.message),
            None => format!("{}: {}", diagnostic.path, diagnostic.message),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn generated_typescript_declarations(
    manifest: &CapabilityManifest,
    package_compat: PackageCompatibilityMode,
) -> String {
    let mut sections = vec![
        SANDBOX_WORKFLOW_LIBRARY_TYPESCRIPT_DECLARATIONS.to_string(),
        sandbox_builtin_typescript_declarations(package_compat),
        manifest.generated_ambient_typescript_declarations(),
    ];
    sections.retain(|section| !section.trim().is_empty());
    sections.join("\n\n")
}

fn sandbox_builtin_typescript_declarations(package_compat: PackageCompatibilityMode) -> String {
    let mut sections = vec![
        format!(
            r#"declare module "{SANDBOX_FS_LIBRARY_SPECIFIER}" {{
  export function readTextFile(path: string): string;
  export function writeTextFile(path: string, contents: string): void;
  export function readJsonFile(path: string): unknown;
  export function writeJsonFile(path: string, value: unknown): void;
  export function mkdir(path: string): void;
  export function readdir(path: string): Array<{{ name: string; kind: "file" | "directory" }}>;
  export function stat(path: string): {{ kind: "file" | "directory" }} | null;
  export function unlink(path: string): void;
  export function rmdir(path: string): void;
  export function rename(from: string, to: string): void;
  export function fsync(path: string): void;
}}"#
        ),
        format!(
            r#"declare module "{SANDBOX_BASH_LIBRARY_SPECIFIER}" {{
  export const unavailable: boolean;
  export const service: string;
}}"#
        ),
        format!(
            r#"declare module "{SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER}" {{
  export const unavailable: boolean;
  export const service: string;
}}"#
        ),
        format!(
            r#"declare module "{SANDBOX_CAPABILITIES_LIBRARY_SPECIFIER}" {{
  const capabilities: Record<string, unknown>;
  export default capabilities;
}}"#
        ),
    ];
    if package_compat == PackageCompatibilityMode::NpmWithNodeBuiltins {
        sections.push(
            r#"declare module "node:fs" {
  export function readTextFile(path: string): string;
}

declare module "node:fs/promises" {
  export function readTextFile(path: string): Promise<string>;
}"#
            .to_string(),
        );
    }
    sections.join("\n\n")
}

fn emitted_output_path(path: &str) -> String {
    if path.ends_with(".tsx") {
        return format!("{}{}", path.trim_end_matches(".tsx"), ".jsx");
    }
    if path.ends_with(".mts") {
        return format!("{}{}", path.trim_end_matches(".mts"), ".mjs");
    }
    if path.ends_with(".cts") {
        return format!("{}{}", path.trim_end_matches(".cts"), ".cjs");
    }
    if path.ends_with(".ts") {
        return format!("{}{}", path.trim_end_matches(".ts"), ".js");
    }
    if path.ends_with(".d.ts") {
        return format!("{}{}", path.trim_end_matches(".d.ts"), ".js");
    }
    path.to_string()
}

fn stable_hash_hex(bytes: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    format!("{:08x}", hasher.finalize())
}

fn resolve_workspace_path(workspace_root: &str, path: &str) -> Result<String, SandboxError> {
    if path.starts_with('/') {
        normalize_internal_path(path)
    } else {
        resolve_path(workspace_root, path)
    }
}

fn resolve_path(base: &str, path: &str) -> Result<String, SandboxError> {
    if path.starts_with('/') {
        return normalize_internal_path(path);
    }
    let combined = if base == "/" {
        format!("/{path}")
    } else {
        format!("{base}/{path}")
    };
    normalize_internal_path(&combined)
}

fn normalize_internal_path(path: &str) -> Result<String, SandboxError> {
    if !path.starts_with('/') {
        return Err(SandboxError::Service {
            service: "typescript",
            message: format!("path must be absolute: {path}"),
        });
    }

    let mut parts = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }

    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn parent_path(path: &str) -> String {
    if path == "/" {
        return "/".to_string();
    }
    match path.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(index) => path[..index].to_string(),
    }
}
