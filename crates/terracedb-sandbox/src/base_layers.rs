use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use terracedb::Timestamp;
use terracedb_git::{
    GitHostBridge, GitImportLayout, GitImportMode, GitImportRequest, GitImportSink,
    GitImportSource, NeverCancel,
};
use terracedb_vfs::{
    DEFAULT_CHUNK_SIZE, VfsArtifactStoreExt, Volume, VolumeArtifactBuilder, VolumeConfig, VolumeId,
    VolumeStore,
};

use crate::{SandboxError, git::git_substrate_error_to_sandbox};

pub const NODE_V24_14_1_JS_TREE_BASE_LAYER_ARTIFACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/node-v24.14.1-js-tree.tdva"
);
pub const NODE_V24_14_1_NPM_CLI_V11_12_1_BASE_LAYER_ARTIFACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/node-v24.14.1-npm-cli-v11.12.1-runtime.tdva"
);
pub const NODE_V24_14_1_REMOTE_URL: &str = "https://github.com/nodejs/node.git";
pub const NODE_V24_14_1_COMMIT: &str = "d89bb1b482fa09245c4f2cbb3b5b6a70bea6deaf";
pub const NPM_CLI_V11_12_1_REMOTE_URL: &str = "https://github.com/npm/cli.git";
pub const NPM_CLI_V11_12_1_COMMIT: &str = "63b9a7c1a65361eb2e082d5f4aff267df52ba817";

#[derive(Clone, Debug, PartialEq)]
pub struct SandboxSnapshotRecipe {
    // This is the low-level static filesystem composition primitive.
    // The intended next layer above it is a build recipe that can open a sandbox from these
    // layers, run deterministic commands such as dependency installs, and then freeze the
    // resulting volume back into a reusable base artifact.
    //
    // Intended shape:
    //
    // let artifact = SnapshotBuildRecipe::new("app-with-deps")
    //     .layer(node_v24_14_1_js_tree_recipe_layer())
    //     .layer(app_git_layer())
    //     .run_step(
    //         SandboxCommand::new("/node/bin/node")
    //             .arg("/npm/bin/npm-cli.js")
    //             .arg("install")
    //             .cwd("/workspace/app"),
    //     )
    //     .snapshot()
    //     .await?;
    name: Cow<'static, str>,
    layers: Vec<SandboxSnapshotLayer>,
    chunk_size: Option<u32>,
    volume_id: VolumeId,
    created_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SandboxSnapshotLayer {
    name: Cow<'static, str>,
    target_root: String,
    source: SandboxSnapshotLayerSource,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SandboxSnapshotLayerSource {
    Git {
        source: GitImportSource,
        mode: GitImportMode,
        pathspec: Vec<String>,
        metadata: BTreeMap<String, JsonValue>,
    },
    HostTree {
        source_path: String,
        pathspec: Vec<String>,
        skip_git_metadata: bool,
    },
}

#[derive(Clone)]
pub struct SandboxBaseLayer {
    name: Cow<'static, str>,
    source: SandboxBaseLayerSource,
    chunk_size: Option<u32>,
}

#[derive(Clone)]
enum SandboxBaseLayerSource {
    Bytes(Arc<[u8]>),
    ArtifactPath(PathBuf),
}

impl SandboxSnapshotRecipe {
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            layers: Vec::new(),
            chunk_size: None,
            volume_id: VolumeId::new(0),
            created_at: Timestamp::new(0),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn layers(&self) -> &[SandboxSnapshotLayer] {
        &self.layers
    }

    pub fn layer(mut self, layer: SandboxSnapshotLayer) -> Self {
        self.layers.push(layer);
        self
    }

    pub fn with_chunk_size(mut self, chunk_size: u32) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }

    pub fn with_volume_id(mut self, volume_id: VolumeId) -> Self {
        self.volume_id = volume_id;
        self
    }

    pub fn with_created_at(mut self, created_at: Timestamp) -> Self {
        self.created_at = created_at;
        self
    }

    pub async fn build_base_layer(
        &self,
        git_bridge: Arc<dyn GitHostBridge>,
    ) -> Result<SandboxBaseLayer, SandboxError> {
        let mut artifact = Vec::new();
        self.write_artifact(git_bridge, &mut artifact).await?;
        Ok(SandboxBaseLayer::from_bytes(self.name.clone(), artifact)
            .with_chunk_size(self.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE)))
    }

    pub async fn write_artifact<W: Write + Send>(
        &self,
        git_bridge: Arc<dyn GitHostBridge>,
        writer: &mut W,
    ) -> Result<(), SandboxError> {
        let mut builder = VolumeArtifactBuilder::new(
            self.volume_id,
            self.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE),
            self.created_at,
        )?;
        for layer in &self.layers {
            builder = write_snapshot_layer_to_builder(git_bridge.clone(), layer, builder).await?;
        }
        builder.finish_to_writer(writer)?;
        Ok(())
    }

    pub async fn write_artifact_to_path(
        &self,
        git_bridge: Arc<dyn GitHostBridge>,
        artifact_path: impl AsRef<Path>,
    ) -> Result<SandboxBaseLayer, SandboxError> {
        let artifact_path = artifact_path.as_ref();
        let file = File::create(artifact_path).map_err(|error| SandboxError::Io {
            path: artifact_path.display().to_string(),
            message: error.to_string(),
        })?;
        let mut writer = BufWriter::new(file);
        self.write_artifact(git_bridge, &mut writer).await?;
        writer.flush().map_err(|error| SandboxError::Io {
            path: artifact_path.display().to_string(),
            message: error.to_string(),
        })?;
        Ok(
            SandboxBaseLayer::from_artifact_path(self.name.clone(), artifact_path.to_path_buf())
                .with_chunk_size(self.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE)),
        )
    }
}

impl SandboxSnapshotLayer {
    pub fn host_tree(name: impl Into<Cow<'static, str>>, source_path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            target_root: "/".to_string(),
            source: SandboxSnapshotLayerSource::HostTree {
                source_path: source_path.into(),
                pathspec: Vec::new(),
                skip_git_metadata: true,
            },
        }
    }

    pub fn git_host_path(
        name: impl Into<Cow<'static, str>>,
        source_path: impl Into<String>,
    ) -> Self {
        Self::git_source(
            name,
            GitImportSource::HostPath {
                path: source_path.into(),
            },
        )
    }

    pub fn git_remote(
        name: impl Into<Cow<'static, str>>,
        remote_url: impl Into<String>,
        reference: impl Into<Option<String>>,
    ) -> Self {
        Self::git_source(
            name,
            GitImportSource::RemoteRepository {
                remote_url: remote_url.into(),
                reference: reference.into(),
            },
        )
    }

    pub fn git_source(name: impl Into<Cow<'static, str>>, source: GitImportSource) -> Self {
        Self {
            name: name.into(),
            target_root: "/".to_string(),
            source: SandboxSnapshotLayerSource::Git {
                source,
                mode: GitImportMode::Head,
                pathspec: Vec::new(),
                metadata: BTreeMap::new(),
            },
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn target_root(&self) -> &str {
        &self.target_root
    }

    pub fn with_target_root(mut self, target_root: impl Into<String>) -> Self {
        self.target_root = target_root.into();
        self
    }

    pub fn with_mode(mut self, mode: GitImportMode) -> Self {
        match &mut self.source {
            SandboxSnapshotLayerSource::Git { mode: current, .. } => *current = mode,
            SandboxSnapshotLayerSource::HostTree { .. } => {}
        }
        self
    }

    pub fn with_pathspec(mut self, pathspec: Vec<String>) -> Self {
        match &mut self.source {
            SandboxSnapshotLayerSource::Git {
                pathspec: current, ..
            }
            | SandboxSnapshotLayerSource::HostTree {
                pathspec: current, ..
            } => *current = pathspec,
        }
        self
    }

    pub fn with_metadata(mut self, metadata: BTreeMap<String, JsonValue>) -> Self {
        match &mut self.source {
            SandboxSnapshotLayerSource::Git {
                metadata: current, ..
            } => *current = metadata,
            SandboxSnapshotLayerSource::HostTree { .. } => {}
        }
        self
    }

    pub fn with_skip_git_metadata(mut self, skip_git_metadata: bool) -> Self {
        match &mut self.source {
            SandboxSnapshotLayerSource::HostTree {
                skip_git_metadata: current,
                ..
            } => *current = skip_git_metadata,
            SandboxSnapshotLayerSource::Git { .. } => {}
        }
        self
    }
}

impl SandboxBaseLayer {
    pub fn vendored_node_v24_14_1_js_tree() -> Self {
        Self::from_artifact_path(
            "node-v24.14.1-js-tree",
            NODE_V24_14_1_JS_TREE_BASE_LAYER_ARTIFACT_PATH,
        )
    }

    pub fn vendored_node_v24_14_1_npm_cli_v11_12_1() -> Self {
        Self::from_artifact_path(
            "node-v24.14.1-npm-cli-v11.12.1-runtime",
            NODE_V24_14_1_NPM_CLI_V11_12_1_BASE_LAYER_ARTIFACT_PATH,
        )
    }

    pub fn from_static_bytes(name: &'static str, artifact: &'static [u8]) -> Self {
        Self {
            name: Cow::Borrowed(name),
            source: SandboxBaseLayerSource::Bytes(Arc::from(artifact)),
            chunk_size: None,
        }
    }

    pub fn from_bytes(name: impl Into<Cow<'static, str>>, artifact: impl Into<Arc<[u8]>>) -> Self {
        Self {
            name: name.into(),
            source: SandboxBaseLayerSource::Bytes(artifact.into()),
            chunk_size: None,
        }
    }

    pub fn from_artifact_path(
        name: impl Into<Cow<'static, str>>,
        artifact_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            name: name.into(),
            source: SandboxBaseLayerSource::ArtifactPath(artifact_path.into()),
            chunk_size: None,
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: u32) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        match &self.source {
            SandboxBaseLayerSource::Bytes(bytes) => bytes.len(),
            SandboxBaseLayerSource::ArtifactPath(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.source {
            SandboxBaseLayerSource::Bytes(bytes) => bytes.is_empty(),
            SandboxBaseLayerSource::ArtifactPath(_) => false,
        }
    }

    pub fn artifact_bytes(&self) -> Option<&[u8]> {
        match &self.source {
            SandboxBaseLayerSource::Bytes(bytes) => Some(bytes.as_ref()),
            SandboxBaseLayerSource::ArtifactPath(_) => None,
        }
    }

    pub fn artifact_path(&self) -> Option<&Path> {
        match &self.source {
            SandboxBaseLayerSource::Bytes(_) => None,
            SandboxBaseLayerSource::ArtifactPath(path) => Some(path.as_path()),
        }
    }

    pub async fn ensure_volume<S>(
        &self,
        volumes: &S,
        volume_id: VolumeId,
    ) -> Result<Arc<dyn Volume>, SandboxError>
    where
        S: VolumeStore + ?Sized,
    {
        let config = VolumeConfig::new(volume_id)
            .with_chunk_size(self.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE));
        match &self.source {
            SandboxBaseLayerSource::Bytes(bytes) => volumes
                .ensure_imported_volume_artifact(bytes.as_ref(), config)
                .await
                .map_err(Into::into),
            SandboxBaseLayerSource::ArtifactPath(path) => {
                let file = File::open(path).map_err(|error| SandboxError::Io {
                    path: path.display().to_string(),
                    message: error.to_string(),
                })?;
                let mut reader = BufReader::new(file);
                volumes
                    .ensure_imported_volume_artifact_from_reader(&mut reader, config)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}

struct GitImportArtifactSink {
    builder: VolumeArtifactBuilder,
    target_root: String,
}

impl GitImportArtifactSink {
    fn from_builder(
        builder: VolumeArtifactBuilder,
        target_root: &str,
    ) -> Result<Self, SandboxError> {
        Ok(Self {
            builder,
            target_root: normalize_target_root(target_root)?,
        })
    }

    fn into_builder(self) -> VolumeArtifactBuilder {
        self.builder
    }
}

#[async_trait]
impl GitImportSink for GitImportArtifactSink {
    async fn add_directory(
        &mut self,
        path: &str,
        mode: Option<u32>,
    ) -> Result<(), terracedb_git::GitSubstrateError> {
        let path =
            join_target_root(&self.target_root, path).map_err(sandbox_error_to_git_bridge_error)?;
        self.builder
            .add_directory(&path, mode)
            .map_err(vfs_error_to_git_bridge_error)
    }

    async fn add_file(
        &mut self,
        path: &str,
        mode: Option<u32>,
        reader: &mut (dyn Read + Send),
    ) -> Result<(), terracedb_git::GitSubstrateError> {
        let path =
            join_target_root(&self.target_root, path).map_err(sandbox_error_to_git_bridge_error)?;
        self.builder
            .add_file_from_reader(&path, reader, mode)
            .map_err(vfs_error_to_git_bridge_error)
    }

    async fn add_symlink(
        &mut self,
        path: &str,
        target: &str,
        mode: Option<u32>,
    ) -> Result<(), terracedb_git::GitSubstrateError> {
        let path =
            join_target_root(&self.target_root, path).map_err(sandbox_error_to_git_bridge_error)?;
        self.builder
            .add_symlink(&path, target, mode)
            .map_err(vfs_error_to_git_bridge_error)
    }
}

async fn write_snapshot_layer_to_builder(
    git_bridge: Arc<dyn GitHostBridge>,
    layer: &SandboxSnapshotLayer,
    builder: VolumeArtifactBuilder,
) -> Result<VolumeArtifactBuilder, SandboxError> {
    match &layer.source {
        SandboxSnapshotLayerSource::Git {
            source,
            mode,
            pathspec,
            metadata,
        } => {
            let mut sink = GitImportArtifactSink::from_builder(builder, &layer.target_root)?;
            git_bridge
                .import_repository_streaming(
                    GitImportRequest {
                        source: source.clone(),
                        target_root: layer.target_root.clone(),
                        mode: mode.clone(),
                        layout: GitImportLayout::TreeOnly,
                        pathspec: pathspec.clone(),
                        metadata: metadata.clone(),
                    },
                    &mut sink,
                    Arc::new(NeverCancel),
                )
                .await
                .map_err(git_substrate_error_to_sandbox)?;
            Ok(sink.into_builder())
        }
        SandboxSnapshotLayerSource::HostTree {
            source_path,
            pathspec,
            skip_git_metadata,
        } => {
            let mut sink = GitImportArtifactSink::from_builder(builder, &layer.target_root)?;
            stream_host_tree(
                Path::new(source_path),
                pathspec,
                *skip_git_metadata,
                &mut sink,
            )
            .await?;
            Ok(sink.into_builder())
        }
    }
}

pub fn node_v24_14_1_js_tree_recipe() -> SandboxSnapshotRecipe {
    SandboxSnapshotRecipe::new("node-v24.14.1-js-tree").layer(
        SandboxSnapshotLayer::git_remote(
            "node-source",
            NODE_V24_14_1_REMOTE_URL,
            Some(NODE_V24_14_1_COMMIT.to_string()),
        )
        .with_target_root("/node")
        .with_pathspec(vec![
            "lib".to_string(),
            "deps".to_string(),
            "test".to_string(),
        ]),
    )
}

pub fn npm_cli_v11_12_1_runtime_tree_recipe(
    source_path: impl Into<String>,
) -> SandboxSnapshotRecipe {
    SandboxSnapshotRecipe::new("npm-cli-v11.12.1-runtime").layer(
        SandboxSnapshotLayer::host_tree("npm-runtime", source_path)
            .with_target_root("/npm")
            .with_pathspec(vec![
                "bin".to_string(),
                "lib".to_string(),
                "index.js".to_string(),
                "package.json".to_string(),
                "package-lock.json".to_string(),
                "node_modules".to_string(),
            ]),
    )
}

pub fn node_v24_14_1_npm_cli_v11_12_1_recipe(
    npm_runtime_source: impl Into<String>,
) -> SandboxSnapshotRecipe {
    let npm_runtime_source = npm_runtime_source.into();
    SandboxSnapshotRecipe::new("node-v24.14.1-npm-cli-v11.12.1-runtime")
        .layer(
            SandboxSnapshotLayer::git_remote(
                "node-source",
                NODE_V24_14_1_REMOTE_URL,
                Some(NODE_V24_14_1_COMMIT.to_string()),
            )
            .with_target_root("/node")
            .with_pathspec(vec![
                "lib".to_string(),
                "deps".to_string(),
                "test".to_string(),
            ]),
        )
        .layer(
            SandboxSnapshotLayer::host_tree("npm-runtime", npm_runtime_source)
                .with_target_root("/npm")
                .with_pathspec(vec![
                    "bin".to_string(),
                    "lib".to_string(),
                    "index.js".to_string(),
                    "package.json".to_string(),
                    "package-lock.json".to_string(),
                    "node_modules".to_string(),
                ]),
        )
}

async fn stream_host_tree(
    root: &Path,
    pathspec: &[String],
    skip_git_metadata: bool,
    sink: &mut dyn GitImportSink,
) -> Result<(), SandboxError> {
    let root_metadata = fs::symlink_metadata(root).map_err(|error| SandboxError::Io {
        path: root.display().to_string(),
        message: error.to_string(),
    })?;
    if !root_metadata.file_type().is_dir() {
        return Err(SandboxError::Io {
            path: root.display().to_string(),
            message: "host tree source must be a directory".to_string(),
        });
    }

    let mut created_dirs = BTreeSet::new();
    let mut pending = Vec::new();
    if pathspec.is_empty() {
        for (path, relative) in read_sorted_host_tree_children(root, root)?
            .into_iter()
            .rev()
        {
            pending.push((path, relative));
        }
    } else {
        let mut selected = pathspec
            .iter()
            .map(|entry| normalize_bridge_import_path(entry))
            .collect::<Result<Vec<_>, _>>()?;
        selected.sort();
        selected.dedup();
        for relative in selected {
            let path = root.join(&relative);
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    for ancestor in ancestor_relatives(&relative) {
                        if created_dirs.insert(ancestor.clone()) {
                            sink.add_directory(&ancestor, None)
                                .await
                                .map_err(git_substrate_error_to_sandbox)?;
                        }
                    }
                    pending.push((path, relative));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(SandboxError::Io {
                        path: path.display().to_string(),
                        message: error.to_string(),
                    });
                }
            }
        }
    }

    while let Some((path, relative)) = pending.pop() {
        if skip_git_metadata
            && path
                .file_name()
                .is_some_and(|name| name == std::ffi::OsStr::new(".git"))
        {
            continue;
        }

        let metadata = fs::symlink_metadata(&path).map_err(|error| SandboxError::Io {
            path: path.display().to_string(),
            message: error.to_string(),
        })?;
        if metadata.file_type().is_dir() {
            if created_dirs.insert(relative.clone()) {
                sink.add_directory(&relative, None)
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
            }
            for (child_path, child_relative) in read_sorted_host_tree_children(root, &path)?
                .into_iter()
                .rev()
            {
                pending.push((child_path, child_relative));
            }
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&path).map_err(|error| SandboxError::Io {
                path: path.display().to_string(),
                message: error.to_string(),
            })?;
            sink.add_symlink(&relative, &target.to_string_lossy(), None)
                .await
                .map_err(git_substrate_error_to_sandbox)?;
        } else if metadata.file_type().is_file() {
            let mut file = File::open(&path).map_err(|error| SandboxError::Io {
                path: path.display().to_string(),
                message: error.to_string(),
            })?;
            sink.add_file(&relative, Some(host_tree_file_mode(&metadata)), &mut file)
                .await
                .map_err(git_substrate_error_to_sandbox)?;
        }
    }
    Ok(())
}

fn read_sorted_host_tree_children(
    root: &Path,
    current: &Path,
) -> Result<Vec<(PathBuf, String)>, SandboxError> {
    let mut children = fs::read_dir(current)
        .map_err(|error| SandboxError::Io {
            path: current.display().to_string(),
            message: error.to_string(),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| SandboxError::Io {
            path: current.display().to_string(),
            message: error.to_string(),
        })?;
    children.sort_by(|left, right| {
        left.file_name()
            .to_string_lossy()
            .cmp(&right.file_name().to_string_lossy())
    });

    children
        .into_iter()
        .map(|child| {
            let path = child.path();
            let relative = normalize_bridge_import_path(
                path.strip_prefix(root)
                    .map_err(|error| SandboxError::Io {
                        path: path.display().to_string(),
                        message: error.to_string(),
                    })?
                    .to_string_lossy()
                    .as_ref(),
            )?;
            Ok((path, relative))
        })
        .collect()
}

fn ancestor_relatives(relative: &str) -> Vec<String> {
    let mut ancestors = Vec::new();
    let mut current = Path::new(relative).parent();
    while let Some(path) = current {
        if path.as_os_str().is_empty() {
            break;
        }
        ancestors.push(path.to_string_lossy().into_owned());
        current = path.parent();
    }
    ancestors.reverse();
    ancestors
}

fn host_tree_file_mode(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0o644
    }
}

fn normalize_target_root(path: &str) -> Result<String, SandboxError> {
    if path.is_empty() {
        return Ok("/".to_string());
    }
    let normalized = if path == "/" {
        "/".to_string()
    } else if path.starts_with('/') {
        path.trim_end_matches('/').to_string()
    } else {
        format!("/{}", path.trim_end_matches('/'))
    };
    if normalized.is_empty() {
        return Ok("/".to_string());
    }
    if normalized == "/" {
        return Ok(normalized);
    }
    let _ = normalize_bridge_import_path(normalized.trim_start_matches('/'))?;
    Ok(normalized)
}

fn join_target_root(target_root: &str, path: &str) -> Result<String, SandboxError> {
    let relative = normalize_bridge_import_path(path)?;
    Ok(if target_root == "/" {
        format!("/{relative}")
    } else {
        format!("{target_root}/{relative}")
    })
}

fn normalize_bridge_import_path(path: &str) -> Result<String, SandboxError> {
    let mut normalized = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => normalized.push(segment.to_string_lossy().into_owned()),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(SandboxError::Service {
                    service: "git",
                    message: format!("bridge import path escapes sandbox root: {path}"),
                });
            }
        }
    }
    let normalized = normalized.join("/");
    if normalized.is_empty() {
        return Err(SandboxError::Service {
            service: "git",
            message: format!("bridge import path must not be empty: {path}"),
        });
    }
    Ok(normalized)
}

fn sandbox_error_to_git_bridge_error(error: SandboxError) -> terracedb_git::GitSubstrateError {
    terracedb_git::GitSubstrateError::Bridge {
        operation: "import_repository_streaming",
        message: error.to_string(),
    }
}

fn vfs_error_to_git_bridge_error(
    error: terracedb_vfs::VfsError,
) -> terracedb_git::GitSubstrateError {
    terracedb_git::GitSubstrateError::Bridge {
        operation: "import_repository_streaming",
        message: error.to_string(),
    }
}
