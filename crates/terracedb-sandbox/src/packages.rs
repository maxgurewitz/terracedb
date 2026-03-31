use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use terracedb_vfs::{CreateOptions, JsonValue, MkdirOptions};
use tokio::sync::Mutex;

use crate::{
    PackageCompatibilityMode, SandboxError, SandboxFilesystemShim, SandboxSession,
    TERRACE_NPM_COMPATIBILITY_ROOT, TERRACE_NPM_INSTALL_MANIFEST_PATH,
    TERRACE_NPM_SESSION_CACHE_DIR,
};

const PACKAGE_MANIFEST_FORMAT_VERSION: u32 = 1;

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
    pub compatibility_root: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionPackageManifest {
    pub format_version: u32,
    pub compatibility_mode: PackageCompatibilityMode,
    pub materialized_compatibility_view: bool,
    pub packages: Vec<InstalledPackageManifest>,
}

impl SessionPackageManifest {
    pub fn package(&self, package: &str) -> Option<&InstalledPackageManifest> {
        self.packages
            .iter()
            .find(|candidate| candidate.package == package)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstalledPackageManifest {
    pub request: String,
    pub package: String,
    pub version: String,
    pub cache_key: String,
    pub integrity: String,
    pub session_root: String,
    pub entrypoint_path: String,
    pub compatibility_root: Option<String>,
    pub compatibility_entrypoint_path: Option<String>,
    pub files: Vec<String>,
    pub dependencies: Vec<String>,
    pub uses_node_builtins: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterministicPackageDefinition {
    pub name: String,
    pub version: String,
    pub entrypoint: String,
    pub files: BTreeMap<String, String>,
    pub dependencies: Vec<String>,
    pub uses_node_builtins: bool,
    pub package_class: DeterministicPackageClass,
}

impl DeterministicPackageDefinition {
    pub fn esm(
        name: impl Into<String>,
        version: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            entrypoint: "index.js".to_string(),
            files: BTreeMap::from([("index.js".to_string(), source.into())]),
            dependencies: Vec::new(),
            uses_node_builtins: false,
            package_class: DeterministicPackageClass::PureJsEsm,
        }
    }

    pub fn with_dependency(mut self, package: impl Into<String>) -> Self {
        self.dependencies.push(package.into());
        self.dependencies.sort();
        self.dependencies.dedup();
        self
    }

    pub fn with_file(
        mut self,
        relative_path: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        self.files.insert(relative_path.into(), source.into());
        self
    }

    pub fn with_entrypoint(mut self, entrypoint: impl Into<String>) -> Self {
        self.entrypoint = entrypoint.into();
        self
    }

    pub fn requires_node_builtins(mut self) -> Self {
        self.uses_node_builtins = true;
        self.package_class = DeterministicPackageClass::NodeBuiltinEsm;
        self
    }

    pub fn unsupported_native_addon(mut self) -> Self {
        self.package_class = DeterministicPackageClass::UnsupportedNativeAddon;
        self
    }

    pub fn unsupported_postinstall(mut self) -> Self {
        self.package_class = DeterministicPackageClass::UnsupportedPostinstall;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeterministicPackageClass {
    PureJsEsm,
    NodeBuiltinEsm,
    UnsupportedNativeAddon,
    UnsupportedPostinstall,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CachedPackage {
    package: String,
    version: String,
    cache_key: String,
    integrity: String,
    files: BTreeMap<String, Vec<u8>>,
    entrypoint: String,
    dependencies: Vec<String>,
    uses_node_builtins: bool,
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
    registry: BTreeMap<String, BTreeMap<String, DeterministicPackageDefinition>>,
    shared_cache: Arc<Mutex<BTreeMap<String, CachedPackage>>>,
}

impl DeterministicPackageInstaller {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            registry: default_registry(),
            shared_cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_definition(mut self, definition: DeterministicPackageDefinition) -> Self {
        self.registry
            .entry(definition.name.clone())
            .or_default()
            .insert(definition.version.clone(), definition);
        self
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
        session: &SandboxSession,
        request: PackageInstallRequest,
    ) -> Result<PackageInstallReport, SandboxError> {
        let session_info = session.info().await;
        if session_info.provenance.package_compat == PackageCompatibilityMode::TerraceOnly {
            return Err(SandboxError::UnsupportedPackage {
                package: request.packages.join(", "),
                reason: "package installation is disabled in terrace_only mode".to_string(),
            });
        }

        let mut existing = read_package_install_manifest(session.filesystem().as_ref())
            .await?
            .unwrap_or(SessionPackageManifest {
                format_version: PACKAGE_MANIFEST_FORMAT_VERSION,
                compatibility_mode: session_info.provenance.package_compat,
                materialized_compatibility_view: false,
                packages: Vec::new(),
            });
        existing.compatibility_mode = session_info.provenance.package_compat;

        let mut requested = request.packages;
        requested.sort();
        requested.dedup();

        let mut by_package = existing
            .packages
            .into_iter()
            .map(|entry| (entry.package.clone(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut cache_hit_packages = Vec::new();
        let mut cache_miss_packages = Vec::new();

        for raw_request in &requested {
            let (package_name, version_request) = parse_package_request(raw_request);
            let definition = self.resolve_definition(&package_name, version_request)?;
            if let Some(reason) = unsupported_reason(&definition.package_class) {
                return Err(SandboxError::UnsupportedPackage {
                    package: raw_request.clone(),
                    reason: reason.to_string(),
                });
            }
            if definition.uses_node_builtins
                && session_info.provenance.package_compat
                    != PackageCompatibilityMode::NpmWithNodeBuiltins
            {
                return Err(SandboxError::UnsupportedPackage {
                    package: raw_request.clone(),
                    reason: format!(
                        "{} requires node builtins, but the session compatibility mode is {:?}",
                        definition.name, session_info.provenance.package_compat
                    ),
                });
            }
            for dependency in &definition.dependencies {
                if !requested.iter().any(|candidate| {
                    let (candidate_name, _) = parse_package_request(candidate);
                    candidate_name == *dependency
                }) && !by_package.contains_key(dependency)
                {
                    return Err(SandboxError::UnsupportedPackage {
                        package: raw_request.clone(),
                        reason: format!(
                            "dependency {} must be installed explicitly in version 1",
                            dependency
                        ),
                    });
                }
            }

            let cached = cached_package_for(&definition);
            let inserted = {
                let mut shared_cache = self.shared_cache.lock().await;
                match shared_cache.entry(cached.cache_key.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(cached.clone());
                        false
                    }
                    std::collections::btree_map::Entry::Occupied(_) => true,
                }
            };
            if inserted {
                cache_hit_packages.push(definition.name.clone());
            } else {
                cache_miss_packages.push(definition.name.clone());
            }

            let session_root = format!("{}/{}", TERRACE_NPM_SESSION_CACHE_DIR, cached.cache_key);
            write_package_tree(session.filesystem().as_ref(), &session_root, &cached).await?;
            by_package.insert(
                definition.name.clone(),
                InstalledPackageManifest {
                    request: raw_request.clone(),
                    package: definition.name.clone(),
                    version: definition.version.clone(),
                    cache_key: cached.cache_key.clone(),
                    integrity: cached.integrity.clone(),
                    session_root: session_root.clone(),
                    entrypoint_path: format!("{session_root}/{}", cached.entrypoint),
                    compatibility_root: None,
                    compatibility_entrypoint_path: None,
                    files: cached.files.keys().cloned().collect(),
                    dependencies: definition.dependencies.clone(),
                    uses_node_builtins: definition.uses_node_builtins,
                },
            );
        }

        let materialized_compatibility_view =
            existing.materialized_compatibility_view || request.materialize_compatibility_view;
        let mut manifest = SessionPackageManifest {
            format_version: PACKAGE_MANIFEST_FORMAT_VERSION,
            compatibility_mode: session_info.provenance.package_compat,
            materialized_compatibility_view,
            packages: by_package.into_values().collect(),
        };
        manifest
            .packages
            .sort_by(|left, right| left.package.cmp(&right.package));

        if manifest.materialized_compatibility_view {
            materialize_compatibility_view(session.filesystem().as_ref(), &mut manifest).await?;
        }

        write_package_install_manifest(session.filesystem().as_ref(), &manifest).await?;

        let packages = manifest
            .packages
            .iter()
            .map(|entry| entry.package.clone())
            .collect::<Vec<_>>();
        Ok(PackageInstallReport {
            installer: self.name.clone(),
            packages,
            manifest_path: TERRACE_NPM_INSTALL_MANIFEST_PATH.to_string(),
            compatibility_root: manifest
                .materialized_compatibility_view
                .then_some(TERRACE_NPM_COMPATIBILITY_ROOT.to_string()),
            metadata: BTreeMap::from([
                (
                    "package_count".to_string(),
                    JsonValue::from(manifest.packages.len()),
                ),
                (
                    "materialized".to_string(),
                    JsonValue::from(manifest.materialized_compatibility_view),
                ),
                (
                    "cache_hits".to_string(),
                    JsonValue::Array(
                        cache_hit_packages
                            .into_iter()
                            .map(JsonValue::String)
                            .collect(),
                    ),
                ),
                (
                    "cache_misses".to_string(),
                    JsonValue::Array(
                        cache_miss_packages
                            .into_iter()
                            .map(JsonValue::String)
                            .collect(),
                    ),
                ),
            ]),
        })
    }
}

impl DeterministicPackageInstaller {
    fn resolve_definition(
        &self,
        package: &str,
        version_request: Option<&str>,
    ) -> Result<DeterministicPackageDefinition, SandboxError> {
        let versions =
            self.registry
                .get(package)
                .ok_or_else(|| SandboxError::UnsupportedPackage {
                    package: package.to_string(),
                    reason: "package is not available in the deterministic registry".to_string(),
                })?;
        match version_request {
            Some(version) => {
                versions
                    .get(version)
                    .cloned()
                    .ok_or_else(|| SandboxError::UnsupportedPackage {
                        package: format!("{package}@{version}"),
                        reason: "version is not available in the deterministic registry"
                            .to_string(),
                    })
            }
            None => versions
                .iter()
                .next_back()
                .map(|(_, definition)| definition.clone())
                .ok_or_else(|| SandboxError::UnsupportedPackage {
                    package: package.to_string(),
                    reason: "package has no deterministic versions".to_string(),
                }),
        }
    }
}

pub async fn read_package_install_manifest(
    filesystem: &dyn SandboxFilesystemShim,
) -> Result<Option<SessionPackageManifest>, SandboxError> {
    match filesystem
        .read_file(TERRACE_NPM_INSTALL_MANIFEST_PATH)
        .await?
    {
        Some(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(Into::into),
        None => Ok(None),
    }
}

pub async fn write_package_install_manifest(
    filesystem: &dyn SandboxFilesystemShim,
    manifest: &SessionPackageManifest,
) -> Result<(), SandboxError> {
    filesystem
        .write_file(
            TERRACE_NPM_INSTALL_MANIFEST_PATH,
            serde_json::to_vec_pretty(manifest)?,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

fn parse_package_request(raw: &str) -> (String, Option<&str>) {
    if raw.starts_with('@') {
        let at_count = raw.chars().filter(|character| *character == '@').count();
        if at_count > 1 {
            match raw.rsplit_once('@') {
                Some((name, version)) => (name.to_string(), Some(version)),
                None => (raw.to_string(), None),
            }
        } else {
            (raw.to_string(), None)
        }
    } else {
        match raw.split_once('@') {
            Some((name, version)) => (name.to_string(), Some(version)),
            None => (raw.to_string(), None),
        }
    }
}

fn unsupported_reason(class: &DeterministicPackageClass) -> Option<&'static str> {
    match class {
        DeterministicPackageClass::PureJsEsm | DeterministicPackageClass::NodeBuiltinEsm => None,
        DeterministicPackageClass::UnsupportedNativeAddon => {
            Some("native addons are not supported in version 1")
        }
        DeterministicPackageClass::UnsupportedPostinstall => {
            Some("packages with postinstall/build steps are not supported in version 1")
        }
    }
}

fn cached_package_for(definition: &DeterministicPackageDefinition) -> CachedPackage {
    let files = definition
        .files
        .iter()
        .map(|(path, source)| (path.clone(), source.as_bytes().to_vec()))
        .collect::<BTreeMap<_, _>>();
    let cache_key = cache_key_for(
        &definition.name,
        &definition.version,
        &definition.files,
        &definition.dependencies,
        definition.uses_node_builtins,
    );
    let integrity = format!("sha256-{cache_key}");
    CachedPackage {
        package: definition.name.clone(),
        version: definition.version.clone(),
        cache_key,
        integrity,
        files,
        entrypoint: definition.entrypoint.clone(),
        dependencies: definition.dependencies.clone(),
        uses_node_builtins: definition.uses_node_builtins,
    }
}

async fn write_package_tree(
    filesystem: &dyn SandboxFilesystemShim,
    root: &str,
    package: &CachedPackage,
) -> Result<(), SandboxError> {
    filesystem
        .mkdir(
            root,
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await?;
    for (relative_path, contents) in &package.files {
        filesystem
            .write_file(
                &format!("{root}/{relative_path}"),
                contents.clone(),
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await?;
    }
    let package_json = serde_json::json!({
        "name": package.package,
        "version": package.version,
        "type": "module",
        "main": package.entrypoint,
        "exports": format!("./{}", package.entrypoint),
    });
    filesystem
        .write_file(
            &format!("{root}/package.json"),
            serde_json::to_vec_pretty(&package_json)?,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

async fn materialize_compatibility_view(
    filesystem: &dyn SandboxFilesystemShim,
    manifest: &mut SessionPackageManifest,
) -> Result<(), SandboxError> {
    for package in &mut manifest.packages {
        let compatibility_root = compatibility_root_for(&package.package);
        filesystem
            .mkdir(
                &compatibility_root,
                MkdirOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await?;
        for relative_path in &package.files {
            let source_path = format!("{}/{}", package.session_root, relative_path);
            let target_path = format!("{}/{}", compatibility_root, relative_path);
            let contents = filesystem.read_file(&source_path).await?.ok_or_else(|| {
                SandboxError::ModuleNotFound {
                    specifier: source_path.clone(),
                }
            })?;
            filesystem
                .write_file(
                    &target_path,
                    contents,
                    CreateOptions {
                        create_parents: true,
                        overwrite: true,
                        ..Default::default()
                    },
                )
                .await?;
        }
        let source_package_json = format!("{}/package.json", package.session_root);
        let target_package_json = format!("{}/package.json", compatibility_root);
        let package_json = filesystem
            .read_file(&source_package_json)
            .await?
            .ok_or_else(|| SandboxError::ModuleNotFound {
                specifier: source_package_json.clone(),
            })?;
        filesystem
            .write_file(
                &target_package_json,
                package_json,
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await?;
        package.compatibility_root = Some(compatibility_root.clone());
        package.compatibility_entrypoint_path = Some(format!(
            "{}/{}",
            compatibility_root,
            relative_entrypoint(&package.entrypoint_path, &package.session_root)
        ));
    }
    Ok(())
}

fn relative_entrypoint(path: &str, root: &str) -> String {
    path.trim_start_matches(root)
        .trim_start_matches('/')
        .to_string()
}

fn compatibility_root_for(package: &str) -> String {
    format!("{}/{}", TERRACE_NPM_COMPATIBILITY_ROOT, package)
}

fn cache_key_for(
    package: &str,
    version: &str,
    files: &BTreeMap<String, String>,
    dependencies: &[String],
    uses_node_builtins: bool,
) -> String {
    let mut hasher = Hasher::new();
    hasher.update(package.as_bytes());
    hasher.update(version.as_bytes());
    for dependency in dependencies {
        hasher.update(dependency.as_bytes());
    }
    hasher.update(if uses_node_builtins { b"node" } else { b"pure" });
    for (path, contents) in files {
        hasher.update(path.as_bytes());
        hasher.update(contents.as_bytes());
    }
    format!("{:08x}", hasher.finalize())
}

fn default_registry() -> BTreeMap<String, BTreeMap<String, DeterministicPackageDefinition>> {
    let definitions = [
        DeterministicPackageDefinition::esm(
            "lodash",
            "4.17.21",
            r#"
            function words(input) {
              return String(input).trim().split(/[^A-Za-z0-9]+/).filter(Boolean);
            }

            export function camelCase(input) {
              const tokens = words(input);
              if (tokens.length === 0) {
                return "";
              }
              return tokens[0].toLowerCase() + tokens.slice(1).map((token) => {
                return token.charAt(0).toUpperCase() + token.slice(1).toLowerCase();
              }).join("");
            }

            export default { camelCase };
            "#,
        ),
        DeterministicPackageDefinition::esm(
            "zod",
            "3.23.8",
            r#"
            class ZString {
              parse(value) {
                if (typeof value !== "string") {
                  throw new TypeError("Expected string");
                }
                return value;
              }
            }

            class ZObject {
              constructor(shape) {
                this.shape = shape;
              }

              parse(value) {
                if (value === null || typeof value !== "object" || Array.isArray(value)) {
                  throw new TypeError("Expected object");
                }
                const result = {};
                for (const key of Object.keys(this.shape)) {
                  result[key] = this.shape[key].parse(value[key]);
                }
                return result;
              }
            }

            export const z = {
              string() {
                return new ZString();
              },
              object(shape) {
                return new ZObject(shape);
              },
            };

            export default z;
            "#,
        ),
        DeterministicPackageDefinition::esm(
            "node-reader",
            "1.0.0",
            r#"
            import { readTextFile } from "node:fs/promises";

            export function read(path) {
              return readTextFile(path);
            }

            export default { read };
            "#,
        )
        .requires_node_builtins(),
        DeterministicPackageDefinition::esm("native-addon", "1.0.0", "export default 1;")
            .unsupported_native_addon(),
        DeterministicPackageDefinition::esm("needs-build", "1.0.0", "export default 1;")
            .unsupported_postinstall(),
    ];

    let mut registry = BTreeMap::<String, BTreeMap<String, DeterministicPackageDefinition>>::new();
    for definition in definitions {
        registry
            .entry(definition.name.clone())
            .or_default()
            .insert(definition.version.clone(), definition);
    }
    registry
}

pub fn installed_package_names(manifest: &SessionPackageManifest) -> BTreeSet<String> {
    manifest
        .packages
        .iter()
        .map(|entry| entry.package.clone())
        .collect()
}
