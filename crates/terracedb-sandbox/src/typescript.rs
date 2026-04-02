use std::collections::{BTreeMap, BTreeSet, VecDeque};

use async_trait::async_trait;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use terracedb_vfs::{CompletedToolRunOutcome, CreateOptions, MkdirOptions};

use crate::{
    CapabilityManifest, HOST_CAPABILITY_PREFIX, PackageCompatibilityMode,
    SANDBOX_BASH_LIBRARY_SPECIFIER, SANDBOX_CAPABILITIES_LIBRARY_SPECIFIER,
    SANDBOX_FS_LIBRARY_SPECIFIER, SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER,
    SANDBOX_WORKFLOW_LIBRARY_SPECIFIER, SANDBOX_WORKFLOW_LIBRARY_TYPESCRIPT_DECLARATIONS,
    SandboxError, SandboxSession, packages::read_package_install_manifest,
    session::record_completed_tool_run,
};

pub const TERRACE_TYPESCRIPT_STATE_PATH: &str = "/.terrace/typescript/state.json";
pub const TERRACE_TYPESCRIPT_MIRROR_PATH: &str = "/.terrace/typescript/mirror.json";
pub const TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR: &str = "/.terrace/typescript/transpile";
pub const TERRACE_TYPESCRIPT_DECLARATIONS_PATH: &str = "/.terrace/typescript/generated.d.ts";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeCheckRequest {
    pub roots: Vec<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub module_kind: Option<String>,
    #[serde(default)]
    pub jsx: Option<String>,
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
    pub emitted_files: Vec<String>,
    #[serde(default)]
    pub cache_keys: BTreeMap<String, String>,
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
pub struct TypeScriptMirrorEntry {
    pub path: String,
    pub size_bytes: u64,
    pub content_hash: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeScriptMirrorState {
    pub roots: Vec<String>,
    pub files: Vec<TypeScriptMirrorEntry>,
    pub updated_revision: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedTypeScriptState {
    last_check: Option<TypeCheckReport>,
    last_emit: Option<TypeScriptEmitReport>,
    last_transpile: Option<TypeScriptTranspileReport>,
    updated_revision: u64,
}

#[derive(Clone, Debug)]
struct MirrorBuild {
    mirror: TypeScriptMirrorState,
    contents: BTreeMap<String, String>,
    missing_roots: Vec<String>,
    missing_imports: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
struct TypeScriptImportSurface {
    builtins: BTreeSet<String>,
    capabilities: CapabilityManifest,
    packages: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TranspileCacheEntry {
    cache_key: String,
    input_path: String,
    content_hash: String,
    target: String,
    module_kind: String,
    jsx: Option<String>,
    output_path: String,
    source_map_path: String,
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

    async fn transpile_inner(
        &self,
        session: &SandboxSession,
        request: &TypeScriptTranspileRequest,
    ) -> Result<TypeScriptTranspileReport, SandboxError> {
        let info = session.info().await;
        let input_path = resolve_workspace_path(&info.workspace_root, &request.path)?;
        let source = read_utf8_file(session, &input_path, "typescript").await?;
        let content_hash = stable_hash_hex(source.as_bytes());
        let cache_key = stable_hash_hex(
            serde_json::to_vec(&serde_json::json!({
                "content_hash": content_hash,
                "target": request.target,
                "module_kind": request.module_kind,
                "jsx": request.jsx,
                "path": input_path,
            }))?
            .as_slice(),
        );
        let output_path = format!("{TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR}/{cache_key}.js");
        let source_map_path = format!("{output_path}.map");
        let metadata_path = format!("{TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR}/{cache_key}.json");
        let fs = session.filesystem();
        fs.mkdir(
            TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR,
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await?;
        let cache_hit = fs.read_file(&metadata_path).await?.is_some()
            && fs.read_file(&output_path).await?.is_some()
            && fs.read_file(&source_map_path).await?.is_some();

        if !cache_hit {
            let output = transpile_source(&source);
            let source_map = serde_json::to_vec_pretty(&serde_json::json!({
                "version": 3,
                "file": output_path,
                "sources": [input_path],
                "names": [],
                "mappings": "",
            }))?;
            let metadata = TranspileCacheEntry {
                cache_key: cache_key.clone(),
                input_path: input_path.clone(),
                content_hash,
                target: request.target.clone(),
                module_kind: request.module_kind.clone(),
                jsx: request.jsx.clone(),
                output_path: output_path.clone(),
                source_map_path: source_map_path.clone(),
            };
            fs.write_file(
                &output_path,
                output.into_bytes(),
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await?;
            fs.write_file(
                &source_map_path,
                source_map,
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await?;
            fs.write_file(
                &metadata_path,
                serde_json::to_vec_pretty(&metadata)?,
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await?;
        }

        let report = TypeScriptTranspileReport {
            input_path,
            cache_key,
            output_path,
            source_map_path,
            cache_hit,
        };
        persist_typescript_state(session, |state, revision| {
            state.last_transpile = Some(report.clone());
            state.updated_revision = revision;
        })
        .await?;
        Ok(report)
    }

    async fn check_inner(
        &self,
        session: &SandboxSession,
        request: &TypeCheckRequest,
    ) -> Result<TypeCheckReport, SandboxError> {
        let build = build_mirror(session, &request.roots).await?;
        persist_mirror_state(session, &build.mirror).await?;
        let mut diagnostics = build
            .missing_roots
            .iter()
            .map(|path| TypeScriptDiagnostic {
                path: path.clone(),
                message: format!("file not found: {path}"),
                code: Some("TS6053".to_string()),
            })
            .collect::<Vec<_>>();
        diagnostics.extend(build.missing_imports.iter().map(|(path, specifier)| {
            TypeScriptDiagnostic {
                path: path.clone(),
                message: format!("cannot resolve import `{specifier}`"),
                code: Some("TS2307".to_string()),
            }
        }));
        for (path, source) in &build.contents {
            diagnostics.extend(primitive_assignment_diagnostics(path, source));
        }
        diagnostics.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.message.cmp(&right.message))
        });
        let report = TypeCheckReport {
            diagnostics,
            checked_files: build
                .mirror
                .files
                .iter()
                .map(|entry| entry.path.clone())
                .collect(),
        };
        persist_typescript_state(session, |state, revision| {
            state.last_check = Some(report.clone());
            state.updated_revision = revision;
        })
        .await?;
        Ok(report)
    }

    async fn emit_inner(
        &self,
        session: &SandboxSession,
        request: &TypeCheckRequest,
    ) -> Result<TypeScriptEmitReport, SandboxError> {
        let build = build_mirror(session, &request.roots).await?;
        persist_mirror_state(session, &build.mirror).await?;
        let info = session.info().await;
        let mut emitted_files = Vec::new();
        let mut cache_keys = BTreeMap::new();
        let fs = session.filesystem();

        for root in &request.roots {
            let path = resolve_workspace_path(&info.workspace_root, root)?;
            if !build.contents.contains_key(&path) {
                continue;
            }
            let transpile = self
                .transpile_inner(
                    session,
                    &TypeScriptTranspileRequest {
                        path: path.clone(),
                        target: request
                            .target
                            .clone()
                            .unwrap_or_else(default_transpile_target),
                        module_kind: request
                            .module_kind
                            .clone()
                            .unwrap_or_else(default_transpile_module_kind),
                        jsx: request.jsx.clone(),
                    },
                )
                .await?;
            let emitted_path = emitted_output_path(&path);
            let emitted_map_path = format!("{emitted_path}.map");
            let output = fs.read_file(&transpile.output_path).await?.ok_or_else(|| {
                SandboxError::Service {
                    service: "typescript",
                    message: format!(
                        "transpile cache output {} disappeared before emit",
                        transpile.output_path
                    ),
                }
            })?;
            let source_map = fs
                .read_file(&transpile.source_map_path)
                .await?
                .ok_or_else(|| SandboxError::Service {
                    service: "typescript",
                    message: format!(
                        "transpile source map {} disappeared before emit",
                        transpile.source_map_path
                    ),
                })?;
            fs.write_file(
                &emitted_path,
                output,
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await?;
            fs.write_file(
                &emitted_map_path,
                source_map,
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await?;
            emitted_files.push(emitted_path.clone());
            cache_keys.insert(path, transpile.cache_key);
        }

        let report = TypeScriptEmitReport {
            emitted_files,
            cache_keys,
        };
        persist_typescript_state(session, |state, revision| {
            state.last_emit = Some(report.clone());
            state.updated_revision = revision;
        })
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

async fn build_mirror(
    session: &SandboxSession,
    roots: &[String],
) -> Result<MirrorBuild, SandboxError> {
    let info = session.info().await;
    let fs = session.filesystem();
    let import_surface = build_import_surface(session).await?;
    let generated_declarations =
        generated_typescript_declarations(&info.provenance.capabilities).to_string();
    fs.write_file(
        TERRACE_TYPESCRIPT_DECLARATIONS_PATH,
        generated_declarations.as_bytes().to_vec(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await?;
    let mut pending = VecDeque::new();
    let mut root_set = BTreeSet::new();
    for root in roots {
        let normalized = resolve_workspace_path(&info.workspace_root, root)?;
        root_set.insert(normalized.clone());
        pending.push_back(normalized);
    }
    pending.push_back(TERRACE_TYPESCRIPT_DECLARATIONS_PATH.to_string());

    let mut contents = BTreeMap::new();
    let mut visited = BTreeSet::new();
    let mut missing_roots = Vec::new();
    let mut missing_imports = Vec::new();

    while let Some(path) = pending.pop_front() {
        if !visited.insert(path.clone()) {
            continue;
        }
        let Some(bytes) = fs.read_file(&path).await? else {
            if root_set.contains(&path) {
                missing_roots.push(path);
            }
            continue;
        };
        let source = String::from_utf8(bytes).map_err(|error| SandboxError::Service {
            service: "typescript",
            message: format!("{path} is not valid utf-8: {error}"),
        })?;
        for specifier in extract_module_specifiers(&source) {
            if is_relative_specifier(&specifier) {
                match resolve_import_path(session, &path, &specifier).await? {
                    Some(resolved) => pending.push_back(resolved),
                    None => missing_imports.push((path.clone(), specifier)),
                }
            } else if !import_surface.allows(&specifier) {
                missing_imports.push((path.clone(), specifier));
            }
        }
        contents.insert(path, source);
    }

    let files = contents
        .iter()
        .map(|(path, source)| TypeScriptMirrorEntry {
            path: path.clone(),
            size_bytes: source.len() as u64,
            content_hash: stable_hash_hex(source.as_bytes()),
        })
        .collect::<Vec<_>>();

    Ok(MirrorBuild {
        mirror: TypeScriptMirrorState {
            roots: root_set.into_iter().collect(),
            files,
            updated_revision: info.revision,
        },
        contents,
        missing_roots,
        missing_imports,
    })
}

async fn build_import_surface(
    session: &SandboxSession,
) -> Result<TypeScriptImportSurface, SandboxError> {
    let info = session.info().await;
    let filesystem = session.filesystem();
    let installed_packages = read_package_install_manifest(filesystem.as_ref()).await?;
    let packages = installed_packages
        .map(|manifest| {
            manifest
                .packages
                .into_iter()
                .map(|package| package.package)
                .collect()
        })
        .unwrap_or_default();
    let mut builtins = BTreeSet::from([
        SANDBOX_WORKFLOW_LIBRARY_SPECIFIER.to_string(),
        SANDBOX_CAPABILITIES_LIBRARY_SPECIFIER.to_string(),
        SANDBOX_FS_LIBRARY_SPECIFIER.to_string(),
        SANDBOX_BASH_LIBRARY_SPECIFIER.to_string(),
        SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER.to_string(),
    ]);
    if info.provenance.package_compat == PackageCompatibilityMode::NpmWithNodeBuiltins {
        builtins.insert("node:fs".to_string());
        builtins.insert("node:fs/promises".to_string());
    }
    Ok(TypeScriptImportSurface {
        builtins,
        capabilities: info.provenance.capabilities,
        packages,
    })
}

impl TypeScriptImportSurface {
    fn allows(&self, specifier: &str) -> bool {
        self.builtins.contains(specifier)
            || self.capabilities.contains(specifier)
            || package_name_for_import(specifier)
                .is_some_and(|package| self.packages.contains(&package))
    }
}

fn generated_typescript_declarations(manifest: &CapabilityManifest) -> String {
    [
        SANDBOX_WORKFLOW_LIBRARY_TYPESCRIPT_DECLARATIONS.to_string(),
        manifest.generated_ambient_typescript_declarations(),
    ]
    .into_iter()
    .filter(|section| !section.trim().is_empty())
    .collect::<Vec<_>>()
    .join("\n\n")
}

fn package_name_for_import(specifier: &str) -> Option<String> {
    if specifier.starts_with(HOST_CAPABILITY_PREFIX) || specifier.starts_with("node:") {
        return None;
    }
    if let Some(package) = specifier.strip_prefix("npm:") {
        return (!package.is_empty()).then_some(package.to_string());
    }
    if !specifier.contains(':') && !specifier.starts_with('.') && !specifier.starts_with('/') {
        return Some(specifier.to_string());
    }
    None
}

async fn resolve_import_path(
    session: &SandboxSession,
    base_path: &str,
    specifier: &str,
) -> Result<Option<String>, SandboxError> {
    let fs = session.filesystem();
    for candidate in import_candidates(base_path, specifier)? {
        if fs.read_file(&candidate).await?.is_some() {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn import_candidates(base_path: &str, specifier: &str) -> Result<Vec<String>, SandboxError> {
    let base_dir = parent_path(base_path);
    let absolute = resolve_path(&base_dir, specifier)?;
    let mut candidates = Vec::new();

    if has_extension(specifier) {
        candidates.push(absolute.clone());
    } else {
        for suffix in [
            ".ts", ".tsx", ".mts", ".cts", ".d.ts", ".js", ".jsx", ".mjs", ".cjs",
        ] {
            candidates.push(format!("{absolute}{suffix}"));
        }
    }

    for suffix in [
        "/index.ts",
        "/index.tsx",
        "/index.mts",
        "/index.cts",
        "/index.d.ts",
        "/index.js",
        "/index.jsx",
        "/index.mjs",
        "/index.cjs",
    ] {
        candidates.push(format!("{absolute}{suffix}"));
    }

    candidates.sort();
    candidates.dedup();
    Ok(candidates)
}

fn extract_module_specifiers(source: &str) -> Vec<String> {
    let mut specifiers = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(specifier) = quoted_after(trimmed, "from ") {
            specifiers.push(specifier);
            continue;
        }
        if let Some(specifier) = quoted_after(trimmed, "import(") {
            specifiers.push(specifier);
        }
    }
    specifiers
}

fn quoted_after(line: &str, marker: &str) -> Option<String> {
    let suffix = line.split_once(marker)?.1;
    let mut chars = suffix.chars();
    let quote = chars.next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let mut value = String::new();
    let mut escaped = false;
    for ch in chars {
        if escaped {
            value.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return Some(value);
        }
        value.push(ch);
    }
    None
}

fn primitive_assignment_diagnostics(path: &str, source: &str) -> Vec<TypeScriptDiagnostic> {
    let mut diagnostics = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("const ")
            && !trimmed.starts_with("let ")
            && !trimmed.starts_with("var ")
            && !trimmed.starts_with("export const ")
            && !trimmed.starts_with("export let ")
            && !trimmed.starts_with("export var ")
        {
            continue;
        }
        let Some(colon) = trimmed.find(':') else {
            continue;
        };
        let Some(eq_offset) = trimmed[colon + 1..].find('=') else {
            continue;
        };
        let annotation = trimmed[colon + 1..colon + 1 + eq_offset].trim();
        let value = trimmed[colon + 1 + eq_offset + 1..]
            .trim()
            .trim_end_matches(';')
            .trim();
        let Some(expected) = primitive_kind(annotation) else {
            continue;
        };
        let Some(actual) = literal_kind(value) else {
            continue;
        };
        if expected != actual {
            diagnostics.push(TypeScriptDiagnostic {
                path: path.to_string(),
                message: format!("cannot assign {actual} to {expected}"),
                code: Some("TS2322".to_string()),
            });
        }
    }
    diagnostics
}

fn primitive_kind(annotation: &str) -> Option<&'static str> {
    match annotation.split_whitespace().next()? {
        "string" => Some("string"),
        "number" => Some("number"),
        "boolean" => Some("boolean"),
        _ => None,
    }
}

fn literal_kind(value: &str) -> Option<&'static str> {
    if value.starts_with('"') || value.starts_with('\'') {
        return Some("string");
    }
    if value == "true" || value == "false" {
        return Some("boolean");
    }
    if value.parse::<f64>().is_ok() {
        return Some("number");
    }
    None
}

async fn persist_mirror_state(
    session: &SandboxSession,
    mirror: &TypeScriptMirrorState,
) -> Result<(), SandboxError> {
    write_json_file(session, TERRACE_TYPESCRIPT_MIRROR_PATH, mirror).await
}

async fn persist_typescript_state<F>(
    session: &SandboxSession,
    mutator: F,
) -> Result<(), SandboxError>
where
    F: FnOnce(&mut PersistedTypeScriptState, u64),
{
    let mut state: PersistedTypeScriptState =
        read_json_file(session, TERRACE_TYPESCRIPT_STATE_PATH).await?;
    let revision = session.info().await.revision;
    mutator(&mut state, revision);
    write_json_file(session, TERRACE_TYPESCRIPT_STATE_PATH, &state).await
}

async fn read_json_file<T>(session: &SandboxSession, path: &str) -> Result<T, SandboxError>
where
    T: for<'de> Deserialize<'de> + Default,
{
    match session.filesystem().read_file(path).await? {
        Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
        None => Ok(T::default()),
    }
}

async fn write_json_file<T>(
    session: &SandboxSession,
    path: &str,
    value: &T,
) -> Result<(), SandboxError>
where
    T: Serialize + ?Sized,
{
    session
        .filesystem()
        .write_file(
            path,
            serde_json::to_vec_pretty(value)?,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
}

async fn read_utf8_file(
    session: &SandboxSession,
    path: &str,
    service: &'static str,
) -> Result<String, SandboxError> {
    let bytes =
        session
            .filesystem()
            .read_file(path)
            .await?
            .ok_or_else(|| SandboxError::Service {
                service,
                message: format!("file not found: {path}"),
            })?;
    String::from_utf8(bytes).map_err(|error| SandboxError::Service {
        service,
        message: format!("{path} is not valid utf-8: {error}"),
    })
}

fn transpile_source(source: &str) -> String {
    source
        .lines()
        .filter_map(transpile_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn transpile_line(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    if trimmed.starts_with("type ")
        || trimmed.starts_with("export type ")
        || trimmed.starts_with("interface ")
        || trimmed.starts_with("export interface ")
    {
        return None;
    }

    let mut output = line.replace("import type ", "import ");
    output = strip_variable_annotation(&output);
    output = strip_function_signature_annotations(&output);
    output = strip_keyword_assertions(&output, " as ");
    output = strip_keyword_assertions(&output, " satisfies ");
    Some(output)
}

fn strip_variable_annotation(line: &str) -> String {
    let trimmed = line.trim_start();
    let variable_markers = [
        "const ",
        "let ",
        "var ",
        "export const ",
        "export let ",
        "export var ",
    ];
    if !variable_markers
        .iter()
        .any(|marker| trimmed.starts_with(marker))
    {
        return line.to_string();
    }
    let Some(eq_index) = line.find('=') else {
        return line.to_string();
    };
    let Some(colon_index) = line[..eq_index].rfind(':') else {
        return line.to_string();
    };
    let mut output = String::new();
    output.push_str(&line[..colon_index]);
    output.push(' ');
    output.push_str(line[eq_index..].trim_start());
    output
}

fn strip_function_signature_annotations(line: &str) -> String {
    let Some(function_index) = line.find("function ") else {
        return line.to_string();
    };
    let Some(open_paren) = line[function_index..].find('(') else {
        return line.to_string();
    };
    let open_paren = function_index + open_paren;
    let Some(close_paren) = find_matching_paren(line, open_paren) else {
        return line.to_string();
    };

    let params = &line[open_paren + 1..close_paren];
    let stripped_params = params
        .split(',')
        .map(|part| {
            let trimmed = part.trim();
            match trimmed.split_once(':') {
                Some((name, _)) => name.trim().to_string(),
                None => trimmed.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut output = String::new();
    output.push_str(&line[..open_paren + 1]);
    output.push_str(&stripped_params);
    output.push(')');
    let suffix = &line[close_paren + 1..];
    if let Some(colon_index) = suffix.find(':')
        && let Some(body_index) = suffix[colon_index + 1..].find('{')
    {
        output.push_str(&suffix[..colon_index]);
        output.push(' ');
        output.push_str(suffix[colon_index + 1 + body_index..].trim_start());
        return output;
    }
    output.push_str(suffix);
    output
}

fn find_matching_paren(line: &str, open_paren: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (index, ch) in line.char_indices().skip(open_paren) {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn strip_keyword_assertions(line: &str, marker: &str) -> String {
    let mut output = String::new();
    let mut remainder = line;
    while let Some(index) = remainder.find(marker) {
        output.push_str(&remainder[..index]);
        let after_marker = &remainder[index + marker.len()..];
        let split_index = after_marker
            .find(|ch: char| [',', ';', ')', '}', '\n'].contains(&ch))
            .unwrap_or(after_marker.len());
        remainder = &after_marker[split_index..];
    }
    output.push_str(remainder);
    output
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

fn has_extension(path: &str) -> bool {
    path.rsplit('/')
        .next()
        .is_some_and(|segment| segment.contains('.'))
}

fn is_relative_specifier(specifier: &str) -> bool {
    specifier.starts_with("./") || specifier.starts_with("../")
}
