use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    io::Read,
    path::{Component, Path, PathBuf},
    process::Command,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use serde::{Deserialize, Serialize};
use terracedb_git::{GitImportSource, GitObjectFormat, GitRepositoryOrigin};
use terracedb_vfs::{
    CreateOptions, FileKind, MkdirOptions, ReadOnlyVfsFileSystem, VfsBatchOperation, VfsError,
    VfsFileSystem, Volume,
};
use tracing::info;

use crate::{ConflictPolicy, GitProvenance, SandboxError, types::sanitize_git_import_source};

pub const HOIST_MANIFEST_PATH: &str = "/.terrace/hoist-manifest.json";
pub const PATCH_BUNDLE_FILE_NAME: &str = ".terrace-sandbox.patch.json";

const HOIST_MANIFEST_FORMAT_VERSION: u32 = 1;
const PATCH_BUNDLE_FORMAT_VERSION: u32 = 1;
const HOIST_FILE_BATCH_SIZE: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoistMode {
    DirectorySnapshot,
    GitHead,
    GitWorkingTree {
        include_untracked: bool,
        include_ignored: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoistRequest {
    pub source_path: String,
    pub target_root: String,
    pub mode: HoistMode,
    pub delete_missing: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoistReport {
    pub source_path: String,
    pub target_root: String,
    pub hoisted_paths: usize,
    pub deleted_paths: usize,
    pub git_provenance: Option<GitProvenance>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EjectMode {
    MaterializeSnapshot,
    ApplyDelta,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EjectRequest {
    pub target_path: String,
    pub mode: EjectMode,
    pub conflict_policy: ConflictPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EjectReport {
    pub target_path: String,
    pub mode: EjectMode,
    pub written_paths: usize,
    pub deleted_paths: usize,
    pub conflicts: ConflictReport,
    pub patch_bundle_path: Option<String>,
    pub provenance_validated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictEntry {
    pub path: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictReport {
    pub conflicts: Vec<ConflictEntry>,
}

impl ConflictReport {
    pub fn is_empty(&self) -> bool {
        self.conflicts.is_empty()
    }
}

impl fmt::Display for ConflictReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.conflicts.is_empty() {
            return write!(f, "no conflicts");
        }
        let mut first = true;
        for conflict in &self.conflicts {
            if !first {
                write!(f, "; ")?;
            }
            first = false;
            write!(f, "{}: {}", conflict.path, conflict.reason)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TreeEntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TreeEntryData {
    File(Vec<u8>),
    HostFile(PathBuf),
    Directory,
    Symlink(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeEntry {
    pub path: String,
    pub data: TreeEntryData,
    pub mode: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManifestEntry {
    pub path: String,
    pub kind: TreeEntryKind,
    pub digest: Option<String>,
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<u32>,
    pub symlink_target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HoistManifest {
    pub format_version: u32,
    pub source: GitImportSource,
    pub target_root: String,
    pub mode: HoistMode,
    pub git_provenance: Option<GitProvenance>,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedHoist {
    pub manifest: HoistManifest,
    pub entries: Vec<TreeEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PatchBundle {
    format_version: u32,
    target_path: String,
    mode: EjectMode,
    conflicts: ConflictReport,
    operations: Vec<PatchOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PatchOperation {
    WriteFile {
        path: String,
        data: Vec<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<u32>,
    },
    CreateDirectory {
        path: String,
    },
    CreateSymlink {
        path: String,
        target: String,
    },
    DeletePath {
        path: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DeltaOperation {
    Create(TreeEntry),
    Update {
        current: TreeEntry,
        base: ManifestEntry,
    },
    Delete {
        path: String,
        base: ManifestEntry,
    },
}

pub(crate) fn prepare_hoist(request: &HoistRequest) -> Result<PreparedHoist, SandboxError> {
    info!(
        target: "terracedb.sandbox.hoist",
        source_path = %request.source_path,
        target_root = %request.target_root,
        mode = ?request.mode,
        "preparing hoist"
    );
    let source_path = canonicalize_for_storage(Path::new(&request.source_path))?;
    let target_root = normalize_vfs_path(&request.target_root)?;
    let (entries, git_provenance) = match &request.mode {
        HoistMode::DirectorySnapshot => {
            let entries = collect_directory_tree(Path::new(&source_path), false)?;
            (entries, None)
        }
        HoistMode::GitHead => {
            let repo_root = git_repo_root(Path::new(&source_path))?;
            let exported_root = export_git_head(&repo_root)?;
            let entries = collect_directory_tree(&exported_root, false)?;
            let git = capture_git_provenance(&repo_root)?;
            let _ = fs::remove_dir_all(&exported_root);
            (entries, Some(git))
        }
        HoistMode::GitWorkingTree {
            include_untracked,
            include_ignored,
        } => {
            let repo_root = git_repo_root(Path::new(&source_path))?;
            let included =
                git_working_tree_paths(&repo_root, *include_untracked, *include_ignored)?;
            let entries = collect_selected_paths(&repo_root, &included)?;
            let git = capture_git_provenance(&repo_root)?;
            (entries, Some(git))
        }
    };
    let manifest = HoistManifest {
        format_version: HOIST_MANIFEST_FORMAT_VERSION,
        source: GitImportSource::HostPath { path: source_path },
        target_root,
        mode: request.mode.clone(),
        git_provenance,
        entries: entries
            .iter()
            .map(ManifestEntry::from_tree_entry)
            .collect::<Result<Vec<_>, _>>()?,
    };
    info!(
        target: "terracedb.sandbox.hoist",
        entries = entries.len(),
        "prepared hoist tree"
    );
    Ok(PreparedHoist { manifest, entries })
}

pub async fn apply_hoist_to_volume(
    volume: &dyn Volume,
    request: &HoistRequest,
) -> Result<HoistReport, SandboxError> {
    let prepared = prepare_hoist(request)?;
    let deleted_paths = replace_vfs_tree(
        volume.fs().as_ref(),
        &prepared.manifest.target_root,
        &prepared.entries,
        request.delete_missing,
    )
    .await?;
    write_hoist_manifest(volume, &prepared.manifest).await?;
    let source_path = match &prepared.manifest.source {
        GitImportSource::HostPath { path } => path.clone(),
        GitImportSource::RemoteRepository { remote_url, .. } => remote_url.clone(),
    };
    Ok(HoistReport {
        source_path,
        target_root: prepared.manifest.target_root,
        hoisted_paths: prepared.entries.len(),
        deleted_paths,
        git_provenance: prepared.manifest.git_provenance,
    })
}

pub(crate) async fn replace_vfs_tree(
    fs: &dyn VfsFileSystem,
    target_root: &str,
    entries: &[TreeEntry],
    delete_missing: bool,
) -> Result<usize, SandboxError> {
    info!(
        target: "terracedb.sandbox.hoist",
        target_root = %target_root,
        entries = entries.len(),
        delete_missing,
        "replacing vfs tree"
    );
    let target_root = normalize_vfs_path(target_root)?;
    let deleted_paths = if delete_missing {
        ensure_vfs_directory(fs, &target_root).await?;
        clear_vfs_directory(fs, &target_root).await?
    } else {
        ensure_vfs_directory(fs, &target_root).await?;
        0
    };
    write_entries_to_vfs(fs, &target_root, entries).await?;
    info!(
        target: "terracedb.sandbox.hoist",
        target_root = %target_root,
        deleted_paths,
        "replaced vfs tree"
    );
    Ok(deleted_paths)
}

pub(crate) async fn populate_vfs_tree_fresh(
    fs: &dyn VfsFileSystem,
    target_root: &str,
    entries: &[TreeEntry],
) -> Result<(), SandboxError> {
    let target_root = normalize_vfs_path(target_root)?;
    info!(
        target: "terracedb.sandbox.hoist",
        target_root = %target_root,
        entries = entries.len(),
        "populating fresh vfs tree"
    );
    let mut ops = Vec::with_capacity(entries.len() + 1);
    ops.push(VfsBatchOperation::Mkdir {
        path: target_root.clone(),
        opts: MkdirOptions {
            recursive: true,
            ..Default::default()
        },
    });
    for entry in entries {
        if matches!(entry.data, TreeEntryData::Directory) {
            ops.push(VfsBatchOperation::Mkdir {
                path: join_vfs_path(&target_root, &entry.path),
                opts: MkdirOptions {
                    recursive: true,
                    ..Default::default()
                },
            });
        }
    }
    info!(
        target: "terracedb.sandbox.hoist",
        operations = ops.len(),
        "applying fresh vfs batch"
    );
    fs.apply_batch(&ops).await?;

    apply_non_directory_entry_batches(fs, &target_root, entries, false).await?;

    Ok(())
}

pub(crate) async fn write_hoist_manifest(
    volume: &dyn Volume,
    manifest: &HoistManifest,
) -> Result<(), SandboxError> {
    let mut durable_manifest = manifest.clone();
    durable_manifest.source = sanitize_git_import_source(&durable_manifest.source);
    durable_manifest.git_provenance = durable_manifest
        .git_provenance
        .as_ref()
        .map(crate::GitProvenance::sanitized_for_durability);
    let payload = serde_json::to_vec_pretty(&durable_manifest)?;
    volume
        .fs()
        .write_file(
            HOIST_MANIFEST_PATH,
            payload,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

pub(crate) async fn load_hoist_manifest(
    volume: &dyn Volume,
) -> Result<Option<HoistManifest>, SandboxError> {
    match volume.fs().read_file(HOIST_MANIFEST_PATH).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

pub(crate) async fn collect_vfs_tree(
    fs: &dyn ReadOnlyVfsFileSystem,
    root: &str,
) -> Result<Vec<TreeEntry>, SandboxError> {
    let root = normalize_vfs_path(root)?;
    if fs.stat(&root).await?.is_none() {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    collect_vfs_tree_recursive(fs, &root, "", &mut entries).await?;
    Ok(entries)
}

pub(crate) async fn materialize_snapshot_to_host(
    fs: &dyn ReadOnlyVfsFileSystem,
    root: &str,
    target_path: &Path,
) -> Result<EjectReport, SandboxError> {
    let entries = collect_vfs_tree(fs, root).await?;
    ensure_host_directory(target_path)?;
    clear_host_directory(target_path, true)?;
    write_entries_to_host(target_path, &entries)?;
    Ok(EjectReport {
        target_path: target_path.to_string_lossy().into_owned(),
        mode: EjectMode::MaterializeSnapshot,
        written_paths: entries.len(),
        deleted_paths: 0,
        conflicts: ConflictReport::default(),
        patch_bundle_path: None,
        provenance_validated: detect_git_repo(target_path).is_some(),
    })
}

pub(crate) async fn apply_delta_to_host(
    fs: &dyn ReadOnlyVfsFileSystem,
    root: &str,
    target_path: &Path,
    manifest: &HoistManifest,
    conflict_policy: ConflictPolicy,
    git_provenance: Option<&GitProvenance>,
) -> Result<EjectReport, SandboxError> {
    let current_entries = collect_vfs_tree(fs, root).await?;
    let current_map = tree_map(&current_entries);
    let base_map = manifest
        .entries
        .iter()
        .cloned()
        .map(|entry| (entry.path.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let target_entries = if target_path.exists() {
        collect_directory_tree(target_path, false)?
    } else {
        Vec::new()
    };
    let target_map = tree_map(&target_entries);
    let operations = build_delta_operations(&base_map, &current_map);
    let mut conflicts = ConflictReport::default();
    let provenance_validated =
        validate_target_provenance(target_path, git_provenance, &mut conflicts)?;
    validate_delta_operations(&operations, &target_map, &base_map, &mut conflicts);

    let patch_bundle_path = if conflicts.is_empty() {
        None
    } else {
        match conflict_policy {
            ConflictPolicy::CreatePatchBundle => Some(write_patch_bundle(
                target_path,
                EjectMode::ApplyDelta,
                &conflicts,
                &operations,
            )?),
            _ => None,
        }
    };

    if !conflicts.is_empty() && matches!(conflict_policy, ConflictPolicy::Fail) {
        return Err(SandboxError::DiskConflict { report: conflicts });
    }

    let applied = match conflict_policy {
        ConflictPolicy::Fail => operations,
        ConflictPolicy::CreatePatchBundle => Vec::new(),
        ConflictPolicy::ApplyNonConflicting => operations
            .into_iter()
            .filter(|operation| !operation_conflicts(operation, &target_map, &base_map))
            .collect(),
    };

    ensure_host_directory(target_path)?;
    let (written_paths, deleted_paths) = apply_delta_operations(target_path, &applied)?;
    Ok(EjectReport {
        target_path: target_path.to_string_lossy().into_owned(),
        mode: EjectMode::ApplyDelta,
        written_paths,
        deleted_paths,
        conflicts,
        patch_bundle_path,
        provenance_validated,
    })
}

fn build_delta_operations(
    base_map: &BTreeMap<String, ManifestEntry>,
    current_map: &BTreeMap<String, TreeEntry>,
) -> Vec<DeltaOperation> {
    let mut all_paths = BTreeSet::new();
    all_paths.extend(base_map.keys().cloned());
    all_paths.extend(current_map.keys().cloned());
    let mut operations = Vec::new();
    for path in all_paths {
        match (base_map.get(&path), current_map.get(&path)) {
            (None, Some(current)) => operations.push(DeltaOperation::Create(current.clone())),
            (Some(base), None) => operations.push(DeltaOperation::Delete {
                path: path.clone(),
                base: base.clone(),
            }),
            (Some(base), Some(current)) if !ManifestEntry::matches_tree_entry(base, current) => {
                operations.push(DeltaOperation::Update {
                    current: current.clone(),
                    base: base.clone(),
                })
            }
            _ => {}
        }
    }
    operations.sort_by(|left, right| {
        delta_operation_sort_key(left).cmp(&delta_operation_sort_key(right))
    });
    operations
}

fn delta_operation_sort_key(operation: &DeltaOperation) -> (u8, usize, String) {
    match operation {
        DeltaOperation::Delete { path, base } => (
            if matches!(base.kind, TreeEntryKind::Directory) {
                2
            } else {
                0
            },
            usize::MAX - path_component_count(path),
            path.clone(),
        ),
        DeltaOperation::Create(entry) | DeltaOperation::Update { current: entry, .. } => (
            if matches!(entry.kind(), TreeEntryKind::Directory) {
                1
            } else {
                3
            },
            path_component_count(&entry.path),
            entry.path.clone(),
        ),
    }
}

fn validate_delta_operations(
    operations: &[DeltaOperation],
    target_map: &BTreeMap<String, TreeEntry>,
    base_map: &BTreeMap<String, ManifestEntry>,
    conflicts: &mut ConflictReport,
) {
    for operation in operations {
        match operation {
            DeltaOperation::Create(entry) => {
                if target_map.contains_key(&entry.path) {
                    conflicts.conflicts.push(ConflictEntry {
                        path: entry.path.clone(),
                        reason: "target path already exists".to_string(),
                    });
                }
            }
            DeltaOperation::Update { current, base } => {
                if !target_matches_base(&current.path, target_map, base_map, base) {
                    conflicts.conflicts.push(ConflictEntry {
                        path: current.path.clone(),
                        reason: "target path diverged from hoisted base".to_string(),
                    });
                }
            }
            DeltaOperation::Delete { path, base } => {
                if !target_matches_base(path, target_map, base_map, base) {
                    conflicts.conflicts.push(ConflictEntry {
                        path: path.clone(),
                        reason: "target path diverged from hoisted base".to_string(),
                    });
                    continue;
                }
                if matches!(base.kind, TreeEntryKind::Directory)
                    && target_has_extra_descendants(path, target_map, base_map)
                {
                    conflicts.conflicts.push(ConflictEntry {
                        path: path.clone(),
                        reason: "target directory contains extra entries".to_string(),
                    });
                }
            }
        }
    }
}

fn operation_conflicts(
    operation: &DeltaOperation,
    target_map: &BTreeMap<String, TreeEntry>,
    base_map: &BTreeMap<String, ManifestEntry>,
) -> bool {
    match operation {
        DeltaOperation::Create(entry) => target_map.contains_key(&entry.path),
        DeltaOperation::Update { current, base } => {
            !target_matches_base(&current.path, target_map, base_map, base)
        }
        DeltaOperation::Delete { path, base } => {
            !target_matches_base(path, target_map, base_map, base)
                || (matches!(base.kind, TreeEntryKind::Directory)
                    && target_has_extra_descendants(path, target_map, base_map))
        }
    }
}

fn target_matches_base(
    path: &str,
    target_map: &BTreeMap<String, TreeEntry>,
    base_map: &BTreeMap<String, ManifestEntry>,
    base: &ManifestEntry,
) -> bool {
    match target_map.get(path) {
        Some(entry) => ManifestEntry::matches_tree_entry(base, entry),
        None => base_map
            .get(path)
            .is_some_and(|expected| matches!(expected.kind, TreeEntryKind::Directory)),
    }
}

fn target_has_extra_descendants(
    path: &str,
    target_map: &BTreeMap<String, TreeEntry>,
    base_map: &BTreeMap<String, ManifestEntry>,
) -> bool {
    let prefix = format!("{path}/");
    target_map
        .keys()
        .any(|candidate| candidate.starts_with(&prefix) && !base_map.contains_key(candidate))
}

fn apply_delta_operations(
    target_root: &Path,
    operations: &[DeltaOperation],
) -> Result<(usize, usize), SandboxError> {
    let mut written_paths = 0;
    let mut deleted_paths = 0;
    for operation in operations {
        match operation {
            DeltaOperation::Create(entry) | DeltaOperation::Update { current: entry, .. } => {
                write_host_entry(target_root, entry)?;
                written_paths += 1;
            }
            DeltaOperation::Delete { path, base } => {
                delete_host_path(target_root, path, &base.kind)?;
                deleted_paths += 1;
            }
        }
    }
    Ok((written_paths, deleted_paths))
}

fn write_patch_bundle(
    target_path: &Path,
    mode: EjectMode,
    conflicts: &ConflictReport,
    operations: &[DeltaOperation],
) -> Result<String, SandboxError> {
    ensure_host_directory(target_path)?;
    let bundle = PatchBundle {
        format_version: PATCH_BUNDLE_FORMAT_VERSION,
        target_path: target_path.to_string_lossy().into_owned(),
        mode,
        conflicts: conflicts.clone(),
        operations: operations
            .iter()
            .map(|operation| -> Result<PatchOperation, SandboxError> {
                Ok(match operation {
                    DeltaOperation::Create(entry)
                    | DeltaOperation::Update { current: entry, .. } => match &entry.data {
                        TreeEntryData::File(bytes) => PatchOperation::WriteFile {
                            path: entry.path.clone(),
                            data: bytes.clone(),
                            mode: entry.mode,
                        },
                        TreeEntryData::HostFile(host_path) => PatchOperation::WriteFile {
                            path: entry.path.clone(),
                            data: std::fs::read(host_path).map_err(|error| SandboxError::Io {
                                path: host_path.to_string_lossy().into_owned(),
                                message: error.to_string(),
                            })?,
                            mode: entry.mode,
                        },
                        TreeEntryData::Directory => PatchOperation::CreateDirectory {
                            path: entry.path.clone(),
                        },
                        TreeEntryData::Symlink(target) => PatchOperation::CreateSymlink {
                            path: entry.path.clone(),
                            target: target.clone(),
                        },
                    },
                    DeltaOperation::Delete { path, .. } => {
                        PatchOperation::DeletePath { path: path.clone() }
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    let bundle_path = target_path.join(PATCH_BUNDLE_FILE_NAME);
    fs::write(&bundle_path, serde_json::to_vec_pretty(&bundle)?).map_err(|error| {
        SandboxError::Io {
            path: bundle_path.to_string_lossy().into_owned(),
            message: error.to_string(),
        }
    })?;
    Ok(bundle_path.to_string_lossy().into_owned())
}

fn validate_target_provenance(
    target_path: &Path,
    git_provenance: Option<&GitProvenance>,
    conflicts: &mut ConflictReport,
) -> Result<bool, SandboxError> {
    let Some(expected) = git_provenance else {
        return Ok(false);
    };
    let Some(repo_root) = detect_git_repo(target_path) else {
        return Ok(false);
    };
    let current_head = git_stdout(&repo_root, &["rev-parse", "HEAD"])?;
    if expected
        .head_commit
        .as_ref()
        .is_some_and(|commit| commit != &current_head)
    {
        conflicts.conflicts.push(ConflictEntry {
            path: target_path.to_string_lossy().into_owned(),
            reason: format!(
                "git HEAD commit mismatch: expected {}, found {}",
                expected.head_commit.clone().unwrap_or_default(),
                current_head
            ),
        });
    }
    if let Some(expected_remote) = &expected.remote_url {
        let current_remote = git_stdout_optional(&repo_root, &["remote", "get-url", "origin"])?;
        if current_remote.as_ref() != Some(expected_remote) {
            conflicts.conflicts.push(ConflictEntry {
                path: target_path.to_string_lossy().into_owned(),
                reason: format!(
                    "git remote mismatch: expected {}, found {}",
                    expected_remote,
                    current_remote.unwrap_or_else(|| "<none>".to_string())
                ),
            });
        }
    }
    let dirty = !git_stdout(&repo_root, &["status", "--porcelain=v1"])
        .unwrap_or_default()
        .is_empty();
    if dirty {
        conflicts.conflicts.push(ConflictEntry {
            path: target_path.to_string_lossy().into_owned(),
            reason: "git working tree is not clean".to_string(),
        });
    }
    Ok(true)
}

fn tree_map(entries: &[TreeEntry]) -> BTreeMap<String, TreeEntry> {
    entries
        .iter()
        .cloned()
        .map(|entry| (entry.path.clone(), entry))
        .collect()
}

async fn collect_vfs_tree_recursive(
    fs: &dyn ReadOnlyVfsFileSystem,
    root: &str,
    relative: &str,
    entries: &mut Vec<TreeEntry>,
) -> Result<(), SandboxError> {
    let directory_path = if relative.is_empty() {
        root.to_string()
    } else {
        join_vfs_path(root, relative)
    };
    let mut directory_entries = fs.readdir_plus(&directory_path).await?;
    directory_entries.sort_by(|left, right| left.entry.name.cmp(&right.entry.name));
    for entry in directory_entries {
        let child_relative = if relative.is_empty() {
            entry.entry.name.clone()
        } else {
            format!("{relative}/{}", entry.entry.name)
        };
        let child_path = join_vfs_path(root, &child_relative);
        match entry.entry.kind {
            FileKind::Directory => {
                entries.push(TreeEntry {
                    path: child_relative.clone(),
                    data: TreeEntryData::Directory,
                    mode: None,
                });
                Box::pin(collect_vfs_tree_recursive(
                    fs,
                    root,
                    &child_relative,
                    entries,
                ))
                .await?;
            }
            FileKind::File => {
                let bytes = fs.read_file(&child_path).await?.ok_or_else(|| {
                    SandboxError::MissingSessionMetadata {
                        path: child_path.clone(),
                    }
                })?;
                entries.push(TreeEntry {
                    path: child_relative,
                    data: TreeEntryData::File(bytes),
                    mode: Some(entry.stats.mode),
                });
            }
            FileKind::Symlink => {
                let target = fs.readlink(&child_path).await?;
                entries.push(TreeEntry {
                    path: child_relative,
                    data: TreeEntryData::Symlink(target),
                    mode: None,
                });
            }
        }
    }
    Ok(())
}

async fn ensure_vfs_directory(fs: &dyn VfsFileSystem, path: &str) -> Result<(), SandboxError> {
    match fs.stat(path).await? {
        Some(stats) if matches!(stats.kind, FileKind::Directory) => Ok(()),
        Some(_) => {
            remove_vfs_path(fs, path).await?;
            fs.mkdir(
                path,
                MkdirOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await?;
            Ok(())
        }
        None => {
            fs.mkdir(
                path,
                MkdirOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await?;
            Ok(())
        }
    }
}

async fn clear_vfs_directory(fs: &dyn VfsFileSystem, root: &str) -> Result<usize, SandboxError> {
    let mut entries = fs.readdir_plus(root).await?;
    entries.sort_by(|left, right| left.entry.name.cmp(&right.entry.name));
    let mut removed = 0usize;
    for entry in entries {
        let child_path = join_vfs_path(root, &entry.entry.name);
        match entry.entry.kind {
            FileKind::Directory => {
                removed += Box::pin(clear_vfs_directory(fs, &child_path)).await?;
                fs.rmdir(&child_path).await?;
                removed += 1;
            }
            FileKind::File | FileKind::Symlink => {
                fs.unlink(&child_path).await?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

async fn remove_vfs_path(fs: &dyn VfsFileSystem, path: &str) -> Result<(), SandboxError> {
    match fs.lstat(path).await? {
        Some(stats) if matches!(stats.kind, FileKind::Directory) => {
            Box::pin(clear_vfs_directory(fs, path)).await?;
            fs.rmdir(path).await?;
        }
        Some(_) => {
            fs.unlink(path).await?;
        }
        None => {}
    }
    Ok(())
}

async fn write_entries_to_vfs(
    fs: &dyn VfsFileSystem,
    target_root: &str,
    entries: &[TreeEntry],
) -> Result<(), SandboxError> {
    for (index, entry) in entries.iter().enumerate() {
        if matches!(entry.data, TreeEntryData::Directory) {
            write_vfs_entry(fs, target_root, entry).await?;
        }
        if (index + 1) % 256 == 0 {
            info!(
                target: "terracedb.sandbox.hoist",
                written = index + 1,
                total = entries.len(),
                phase = "directories",
                "writing hoisted entries to vfs"
            );
        }
    }
    apply_non_directory_entry_batches(fs, target_root, entries, true).await?;
    Ok(())
}

async fn apply_non_directory_entry_batches(
    fs: &dyn VfsFileSystem,
    target_root: &str,
    entries: &[TreeEntry],
    create_parents: bool,
) -> Result<(), SandboxError> {
    let non_directories = entries
        .iter()
        .filter(|entry| !matches!(entry.data, TreeEntryData::Directory))
        .collect::<Vec<_>>();
    for (chunk_index, chunk) in non_directories.chunks(HOIST_FILE_BATCH_SIZE).enumerate() {
        let mut ops = Vec::with_capacity(chunk.len());
        for entry in chunk {
            let destination = join_vfs_path(target_root, &entry.path);
            match &entry.data {
                TreeEntryData::Directory => {}
                TreeEntryData::File(bytes) => ops.push(VfsBatchOperation::WriteFile {
                    path: destination,
                    data: bytes.clone(),
                    opts: CreateOptions {
                        create_parents,
                        overwrite: true,
                        mode: entry.mode.unwrap_or(0o644),
                    },
                }),
                TreeEntryData::HostFile(host_path) => ops.push(VfsBatchOperation::WriteFile {
                    path: destination,
                    data: std::fs::read(host_path).map_err(|error| SandboxError::Io {
                        path: host_path.to_string_lossy().into_owned(),
                        message: error.to_string(),
                    })?,
                    opts: CreateOptions {
                        create_parents,
                        overwrite: true,
                        mode: entry.mode.unwrap_or(0o644),
                    },
                }),
                TreeEntryData::Symlink(target) => ops.push(VfsBatchOperation::Symlink {
                    target: target.clone(),
                    linkpath: destination,
                }),
            }
        }
        info!(
            target: "terracedb.sandbox.hoist",
            written = ((chunk_index + 1) * HOIST_FILE_BATCH_SIZE).min(non_directories.len()),
            total = non_directories.len(),
            phase = "non_directories",
            "writing hoisted entries to vfs"
        );
        fs.apply_batch(&ops).await?;
    }
    Ok(())
}

async fn write_vfs_entry(
    fs: &dyn VfsFileSystem,
    target_root: &str,
    entry: &TreeEntry,
) -> Result<(), SandboxError> {
    let destination = join_vfs_path(target_root, &entry.path);
    if let Some(parent) = parent_vfs_path(&destination) {
        ensure_vfs_directory(fs, &parent).await?;
    }
    match &entry.data {
        TreeEntryData::Directory => {
            ensure_vfs_directory(fs, &destination).await?;
        }
        TreeEntryData::File(bytes) => {
            if matches!(fs.lstat(&destination).await?, Some(stats) if matches!(stats.kind, FileKind::Directory))
            {
                remove_vfs_path(fs, &destination).await?;
            }
            fs.write_file(
                &destination,
                bytes.clone(),
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    mode: entry.mode.unwrap_or(0o644),
                },
            )
            .await?;
        }
        TreeEntryData::HostFile(host_path) => {
            if matches!(fs.lstat(&destination).await?, Some(stats) if matches!(stats.kind, FileKind::Directory))
            {
                remove_vfs_path(fs, &destination).await?;
            }
            fs.write_file(
                &destination,
                std::fs::read(host_path).map_err(|error| SandboxError::Io {
                    path: host_path.to_string_lossy().into_owned(),
                    message: error.to_string(),
                })?,
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    mode: entry.mode.unwrap_or(0o644),
                },
            )
            .await?;
        }
        TreeEntryData::Symlink(target) => {
            if fs.lstat(&destination).await?.is_some() {
                remove_vfs_path(fs, &destination).await?;
            }
            fs.symlink(target, &destination).await?;
        }
    }
    Ok(())
}

fn collect_directory_tree(
    root: &Path,
    skip_git_metadata: bool,
) -> Result<Vec<TreeEntry>, SandboxError> {
    if !root.exists() {
        return Err(SandboxError::Io {
            path: root.to_string_lossy().into_owned(),
            message: "source path does not exist".to_string(),
        });
    }
    let mut entries = Vec::new();
    let mut visited = 0usize;
    collect_directory_tree_recursive(root, root, skip_git_metadata, &mut entries, &mut visited)?;
    Ok(entries)
}

fn collect_selected_paths(
    root: &Path,
    included: &BTreeSet<String>,
) -> Result<Vec<TreeEntry>, SandboxError> {
    let mut entries = Vec::new();
    let mut created_dirs = BTreeSet::new();
    for relative in included {
        let path = root.join(relative);
        if !path.exists() {
            continue;
        }
        for ancestor in ancestor_relatives(relative) {
            if created_dirs.insert(ancestor.clone()) {
                entries.push(TreeEntry {
                    path: ancestor,
                    data: TreeEntryData::Directory,
                    mode: None,
                });
            }
        }
        let metadata = fs::symlink_metadata(&path).map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
        if metadata.file_type().is_file() {
            entries.push(TreeEntry {
                path: relative.clone(),
                data: TreeEntryData::HostFile(path.clone()),
                mode: Some(host_file_mode(&metadata)),
            });
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&path).map_err(|error| SandboxError::Io {
                path: path.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
            entries.push(TreeEntry {
                path: relative.clone(),
                data: TreeEntryData::Symlink(target.to_string_lossy().into_owned()),
                mode: None,
            });
        } else if metadata.file_type().is_dir() && created_dirs.insert(relative.clone()) {
            entries.push(TreeEntry {
                path: relative.clone(),
                data: TreeEntryData::Directory,
                mode: None,
            });
        }
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

fn collect_directory_tree_recursive(
    root: &Path,
    current: &Path,
    skip_git_metadata: bool,
    entries: &mut Vec<TreeEntry>,
    visited: &mut usize,
) -> Result<(), SandboxError> {
    let mut children = fs::read_dir(current)
        .map_err(|error| SandboxError::Io {
            path: current.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| SandboxError::Io {
            path: current.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    children.sort_by(|left, right| {
        left.file_name()
            .to_string_lossy()
            .cmp(&right.file_name().to_string_lossy())
    });
    for child in children {
        *visited = visited.saturating_add(1);
        if *visited % 256 == 0 {
            info!(
                target: "terracedb.sandbox.hoist",
                visited = *visited,
                current = %current.to_string_lossy(),
                "walking hoist directory tree"
            );
        }
        let file_name = child.file_name();
        if skip_git_metadata && file_name == ".git" {
            continue;
        }
        let child_path = child.path();
        let metadata = fs::symlink_metadata(&child_path).map_err(|error| SandboxError::Io {
            path: child_path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
        let relative =
            normalize_relative_path(child_path.strip_prefix(root).map_err(|error| {
                SandboxError::Io {
                    path: child_path.to_string_lossy().into_owned(),
                    message: error.to_string(),
                }
            })?)?;
        if metadata.file_type().is_dir() {
            entries.push(TreeEntry {
                path: relative.clone(),
                data: TreeEntryData::Directory,
                mode: None,
            });
            collect_directory_tree_recursive(
                root,
                &child_path,
                skip_git_metadata,
                entries,
                visited,
            )?;
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&child_path).map_err(|error| SandboxError::Io {
                path: child_path.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
            entries.push(TreeEntry {
                path: relative,
                data: TreeEntryData::Symlink(target.to_string_lossy().into_owned()),
                mode: None,
            });
        } else if metadata.file_type().is_file() {
            entries.push(TreeEntry {
                path: relative,
                data: TreeEntryData::HostFile(child_path.clone()),
                mode: Some(host_file_mode(&metadata)),
            });
        }
    }
    Ok(())
}

fn write_entries_to_host(root: &Path, entries: &[TreeEntry]) -> Result<(), SandboxError> {
    for entry in entries {
        if matches!(entry.data, TreeEntryData::Directory) {
            write_host_entry(root, entry)?;
        }
    }
    for entry in entries {
        if !matches!(entry.data, TreeEntryData::Directory) {
            write_host_entry(root, entry)?;
        }
    }
    Ok(())
}

fn write_host_entry(root: &Path, entry: &TreeEntry) -> Result<(), SandboxError> {
    let destination = root.join(Path::new(&entry.path));
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|error| SandboxError::Io {
            path: parent.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    }
    match &entry.data {
        TreeEntryData::Directory => {
            if destination.exists() && !destination.is_dir() {
                remove_host_path(&destination)?;
            }
            fs::create_dir_all(&destination).map_err(|error| SandboxError::Io {
                path: destination.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
        }
        TreeEntryData::File(bytes) => {
            if destination.is_dir() {
                fs::remove_dir_all(&destination).map_err(|error| SandboxError::Io {
                    path: destination.to_string_lossy().into_owned(),
                    message: error.to_string(),
                })?;
            }
            fs::write(&destination, bytes).map_err(|error| SandboxError::Io {
                path: destination.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
            if let Some(mode) = entry.mode {
                apply_host_file_mode(&destination, mode)?;
            }
        }
        TreeEntryData::HostFile(source_path) => {
            if destination.is_dir() {
                fs::remove_dir_all(&destination).map_err(|error| SandboxError::Io {
                    path: destination.to_string_lossy().into_owned(),
                    message: error.to_string(),
                })?;
            }
            fs::copy(source_path, &destination).map_err(|error| SandboxError::Io {
                path: source_path.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
            if let Some(mode) = entry.mode {
                apply_host_file_mode(&destination, mode)?;
            }
        }
        TreeEntryData::Symlink(target) => {
            if destination.exists() || destination.symlink_metadata().is_ok() {
                remove_host_path(&destination)?;
            }
            create_host_symlink(target, &destination)?;
        }
    }
    Ok(())
}

fn host_file_mode(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        metadata.permissions().mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0o644
    }
}

fn apply_host_file_mode(path: &Path, mode: u32) -> Result<(), SandboxError> {
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(path)
            .map_err(|error| SandboxError::Io {
                path: path.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?
            .permissions();
        permissions.set_mode(mode & 0o777);
        fs::set_permissions(path, permissions).map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
    Ok(())
}

fn delete_host_path(root: &Path, relative: &str, kind: &TreeEntryKind) -> Result<(), SandboxError> {
    let path = root.join(Path::new(relative));
    if !path.exists() && path.symlink_metadata().is_err() {
        return Ok(());
    }
    match kind {
        TreeEntryKind::Directory => {
            fs::remove_dir(&path).map_err(|error| SandboxError::Io {
                path: path.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
        }
        TreeEntryKind::File | TreeEntryKind::Symlink => {
            fs::remove_file(&path).map_err(|error| SandboxError::Io {
                path: path.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
        }
    }
    Ok(())
}

fn ensure_host_directory(path: &Path) -> Result<(), SandboxError> {
    if path.exists() {
        if path.is_dir() {
            return Ok(());
        }
        return Err(SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: "target path is not a directory".to_string(),
        });
    }
    fs::create_dir_all(path).map_err(|error| SandboxError::Io {
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })?;
    Ok(())
}

fn clear_host_directory(path: &Path, preserve_git: bool) -> Result<(), SandboxError> {
    if !path.exists() {
        return Ok(());
    }
    let mut children = fs::read_dir(path)
        .map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    children.sort_by(|left, right| {
        left.file_name()
            .to_string_lossy()
            .cmp(&right.file_name().to_string_lossy())
    });
    for child in children {
        if preserve_git && child.file_name() == ".git" {
            continue;
        }
        remove_host_path(&child.path())?;
    }
    Ok(())
}

fn remove_host_path(path: &Path) -> Result<(), SandboxError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| SandboxError::Io {
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })?;
    if metadata.file_type().is_dir() {
        fs::remove_dir_all(path).map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    } else {
        fs::remove_file(path).map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    }
    Ok(())
}

#[cfg(unix)]
fn create_host_symlink(target: &str, destination: &Path) -> Result<(), SandboxError> {
    std::os::unix::fs::symlink(target, destination).map_err(|error| SandboxError::Io {
        path: destination.to_string_lossy().into_owned(),
        message: error.to_string(),
    })
}

#[cfg(windows)]
fn create_host_symlink(target: &str, destination: &Path) -> Result<(), SandboxError> {
    std::os::windows::fs::symlink_file(target, destination).map_err(|error| SandboxError::Io {
        path: destination.to_string_lossy().into_owned(),
        message: error.to_string(),
    })
}

fn export_git_head(repo_root: &Path) -> Result<PathBuf, SandboxError> {
    let export_root = temp_export_path("git-head");
    let prefix = format!("{}/", export_root.to_string_lossy());
    git_command(repo_root, &["checkout-index", "--all", "--prefix", &prefix])?;
    Ok(export_root)
}

fn capture_git_provenance(repo_root: &Path) -> Result<GitProvenance, SandboxError> {
    let head_commit = git_stdout_optional(repo_root, &["rev-parse", "HEAD"])?;
    let branch = git_stdout_optional(repo_root, &["symbolic-ref", "--quiet", "--short", "HEAD"])?;
    let remote_url = git_stdout_optional(repo_root, &["remote", "get-url", "origin"])?;
    let object_format = detect_git_object_format(repo_root)?;
    let dirty = !git_stdout(
        repo_root,
        &["status", "--porcelain=v1", "--untracked-files=all"],
    )?
    .is_empty();
    Ok(GitProvenance {
        repo_root: canonicalize_for_storage(repo_root)?,
        origin: GitRepositoryOrigin::HostImport,
        head_commit,
        branch,
        remote_url,
        remote_bridge_metadata: BTreeMap::new(),
        object_format: Some(object_format),
        pathspec: vec![".".to_string()],
        dirty,
    })
}

fn detect_git_object_format(repo_root: &Path) -> Result<GitObjectFormat, SandboxError> {
    if let Some(value) = git_stdout_optional(repo_root, &["rev-parse", "--show-object-format"])? {
        return parse_git_object_format(&value);
    }
    if let Some(value) =
        git_stdout_optional(repo_root, &["config", "--get", "extensions.objectformat"])?
    {
        return parse_git_object_format(&value);
    }
    Ok(GitObjectFormat::Sha1)
}

fn parse_git_object_format(value: &str) -> Result<GitObjectFormat, SandboxError> {
    match value.trim() {
        "sha1" => Ok(GitObjectFormat::Sha1),
        "sha256" => Ok(GitObjectFormat::Sha256),
        other => Err(SandboxError::Service {
            service: "git",
            message: format!("unsupported git object format: {other}"),
        }),
    }
}

fn git_working_tree_paths(
    repo_root: &Path,
    include_untracked: bool,
    include_ignored: bool,
) -> Result<BTreeSet<String>, SandboxError> {
    let mut paths = git_ls_files(repo_root, &["ls-files", "-z"])?;
    if include_untracked {
        paths.extend(git_ls_files(
            repo_root,
            &["ls-files", "-z", "--others", "--exclude-standard"],
        )?);
    }
    if include_ignored {
        paths.extend(git_ls_files(
            repo_root,
            &[
                "ls-files",
                "-z",
                "--others",
                "--ignored",
                "--exclude-standard",
            ],
        )?);
    }
    Ok(paths)
}

fn git_repo_root(path: &Path) -> Result<PathBuf, SandboxError> {
    let root = git_stdout(path, &["rev-parse", "--show-toplevel"])?;
    Ok(PathBuf::from(root))
}

fn detect_git_repo(path: &Path) -> Option<PathBuf> {
    git_stdout_optional(path, &["rev-parse", "--show-toplevel"])
        .ok()
        .flatten()
        .map(PathBuf::from)
}

fn git_ls_files(repo_root: &Path, args: &[&str]) -> Result<BTreeSet<String>, SandboxError> {
    let output = git_command(repo_root, args)?;
    let mut paths = BTreeSet::new();
    for entry in output.split('\0').filter(|entry| !entry.is_empty()) {
        paths.insert(normalize_relative_path(Path::new(entry))?);
    }
    Ok(paths)
}

fn git_stdout(repo_root: &Path, args: &[&str]) -> Result<String, SandboxError> {
    Ok(git_command(repo_root, args)?.trim().to_string())
}

fn git_stdout_optional(repo_root: &Path, args: &[&str]) -> Result<Option<String>, SandboxError> {
    match git_command(repo_root, args) {
        Ok(stdout) => Ok(Some(stdout.trim().to_string())),
        Err(SandboxError::CommandFailed { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn git_command(repo_root: &Path, args: &[&str]) -> Result<String, SandboxError> {
    let output = sanitized_git_command(repo_root)
        .args(args)
        .output()
        .map_err(|error| SandboxError::Io {
            path: repo_root.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    if output.status.success() {
        return String::from_utf8(output.stdout).map_err(|error| SandboxError::CommandFailed {
            command: format!("git -C {} {}", repo_root.display(), args.join(" ")),
            status: output.status.code(),
            stderr: error.to_string(),
        });
    }
    Err(SandboxError::CommandFailed {
        command: format!("git -C {} {}", repo_root.display(), args.join(" ")),
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn sanitized_git_command(repo_root: &Path) -> Command {
    let mut command = Command::new("git");
    command.arg("-C").arg(repo_root);
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_PREFIX",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ] {
        command.env_remove(key);
    }
    command
}

fn temp_export_path(label: &str) -> PathBuf {
    let mut branch_label = label.replace('/', "-");
    if branch_label.is_empty() {
        branch_label = "export".to_string();
    }
    let path = std::env::temp_dir().join(format!(
        "terracedb-sandbox-{branch_label}-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

fn canonicalize_for_storage(path: &Path) -> Result<String, SandboxError> {
    fs::canonicalize(path)
        .map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })
        .map(|path| path.to_string_lossy().into_owned())
}

fn normalize_vfs_path(path: &str) -> Result<String, SandboxError> {
    if !path.starts_with('/') {
        return Err(SandboxError::Vfs(VfsError::InvalidPath {
            path: path.to_string(),
        }));
    }
    let mut normalized = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(segment) => normalized.push(segment.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(SandboxError::Vfs(VfsError::InvalidPath {
                    path: path.to_string(),
                }));
            }
            Component::Prefix(_) => {
                return Err(SandboxError::Vfs(VfsError::InvalidPath {
                    path: path.to_string(),
                }));
            }
        }
    }
    Ok(if normalized.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", normalized.join("/"))
    })
}

fn normalize_relative_path(path: &Path) -> Result<String, SandboxError> {
    let mut normalized = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => normalized.push(segment.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(SandboxError::Io {
                    path: path.to_string_lossy().into_owned(),
                    message: "relative path escapes source root".to_string(),
                });
            }
        }
    }
    Ok(normalized.join("/"))
}

fn join_vfs_path(root: &str, relative: &str) -> String {
    if relative.is_empty() {
        return root.to_string();
    }
    if root == "/" {
        format!("/{relative}")
    } else {
        format!("{root}/{relative}")
    }
}

fn parent_vfs_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let parent = Path::new(path).parent()?;
    let parent = parent.to_string_lossy();
    if parent.is_empty() {
        Some("/".to_string())
    } else {
        Some(parent.into_owned())
    }
}

fn path_component_count(path: &str) -> usize {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .count()
}

fn ancestor_relatives(path: &str) -> Vec<String> {
    let parts = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let mut ancestors = Vec::new();
    for index in 1..parts.len() {
        ancestors.push(parts[..index].join("/"));
    }
    ancestors
}

impl ManifestEntry {
    pub(crate) fn from_tree_entry(entry: &TreeEntry) -> Result<Self, SandboxError> {
        match &entry.data {
            TreeEntryData::File(bytes) => Ok(Self {
                path: entry.path.clone(),
                kind: TreeEntryKind::File,
                digest: Some(bytes_digest(bytes)),
                size: Some(bytes.len() as u64),
                mode: entry.mode,
                symlink_target: None,
            }),
            TreeEntryData::HostFile(host_path) => {
                let (digest, size) = digest_host_file(host_path)?;
                Ok(Self {
                    path: entry.path.clone(),
                    kind: TreeEntryKind::File,
                    digest: Some(digest),
                    size: Some(size),
                    mode: entry.mode,
                    symlink_target: None,
                })
            }
            TreeEntryData::Directory => Ok(Self {
                path: entry.path.clone(),
                kind: TreeEntryKind::Directory,
                digest: None,
                size: None,
                mode: None,
                symlink_target: None,
            }),
            TreeEntryData::Symlink(target) => Ok(Self {
                path: entry.path.clone(),
                kind: TreeEntryKind::Symlink,
                digest: None,
                size: None,
                mode: None,
                symlink_target: Some(target.clone()),
            }),
        }
    }

    fn matches_tree_entry(&self, entry: &TreeEntry) -> bool {
        match (&self.kind, &entry.data) {
            (TreeEntryKind::Directory, TreeEntryData::Directory) => true,
            (TreeEntryKind::File, TreeEntryData::File(bytes)) => {
                self.size == Some(bytes.len() as u64)
                    && self.digest.as_deref() == Some(&bytes_digest(bytes))
                    && self
                        .mode
                        .map(|mode| entry.mode == Some(mode))
                        .unwrap_or(true)
            }
            (TreeEntryKind::File, TreeEntryData::HostFile(host_path)) => {
                let Ok((digest, size)) = digest_host_file(host_path) else {
                    return false;
                };
                self.size == Some(size)
                    && self.digest.as_deref() == Some(digest.as_str())
                    && self
                        .mode
                        .map(|mode| entry.mode == Some(mode))
                        .unwrap_or(true)
            }
            (TreeEntryKind::Symlink, TreeEntryData::Symlink(target)) => {
                self.symlink_target.as_deref() == Some(target)
            }
            _ => false,
        }
    }
}

impl TreeEntry {
    fn kind(&self) -> TreeEntryKind {
        match self.data {
            TreeEntryData::File(_) | TreeEntryData::HostFile(_) => TreeEntryKind::File,
            TreeEntryData::Directory => TreeEntryKind::Directory,
            TreeEntryData::Symlink(_) => TreeEntryKind::Symlink,
        }
    }
}

fn bytes_digest(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn digest_host_file(path: &Path) -> Result<(String, u64), SandboxError> {
    let mut file = fs::File::open(path).map_err(|error| SandboxError::Io {
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })?;
    let mut buffer = [0_u8; 64 * 1024];
    let mut hash = 0xcbf29ce484222325u64;
    let mut size = 0_u64;
    loop {
        let read = file.read(&mut buffer).map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
        if read == 0 {
            break;
        }
        size = size.saturating_add(read as u64);
        for byte in &buffer[..read] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    Ok((format!("{hash:016x}"), size))
}
