use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::{
    GitCancellationToken, GitExportReport, GitExportRequest, GitHeadState, GitHostBridge,
    GitImportEntry, GitImportEntryKind, GitImportMode, GitImportReport, GitImportRequest,
    GitIndexEntry, GitIndexSnapshot, GitObjectFormat, GitObjectKind, GitPullRequestReport,
    GitPullRequestRequest, GitPushReport, GitPushRequest, GitRepository, GitSubstrateError,
};

static IMPORT_EXPORT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
enum OptionalGitLookup {
    HeadCommit,
    SymbolicHead,
    OriginRemote,
}

#[derive(Clone, Debug)]
pub struct HostGitBridge {
    name: Arc<str>,
    base_url: Arc<str>,
    pr_counter: Arc<Mutex<u64>>,
}

impl HostGitBridge {
    pub fn new(name: impl Into<Arc<str>>, base_url: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            base_url: base_url.into(),
            pr_counter: Arc::new(Mutex::new(1)),
        }
    }
}

impl Default for HostGitBridge {
    fn default() -> Self {
        Self::new("host-git", "https://example.invalid")
    }
}

#[async_trait]
impl GitHostBridge for HostGitBridge {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn supports_host_filesystem_bridge(&self) -> bool {
        true
    }

    async fn import_repository(
        &self,
        request: GitImportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitImportReport, GitSubstrateError> {
        ensure_not_cancelled(cancellation.as_ref(), "import_repository")?;
        let source_path = canonicalize_for_storage(Path::new(&request.source_path))?;
        let repo_root = git_repo_root(Path::new(&source_path))?;
        let head_commit = git_stdout_optional(
            &repo_root,
            &["rev-parse", "HEAD"],
            OptionalGitLookup::HeadCommit,
        )?;
        let branch = git_stdout_optional(
            &repo_root,
            &["symbolic-ref", "--quiet", "--short", "HEAD"],
            OptionalGitLookup::SymbolicHead,
        )?;
        let remote_url = git_stdout_optional(
            &repo_root,
            &["remote", "get-url", "origin"],
            OptionalGitLookup::OriginRemote,
        )?;
        let object_format = git_object_format(&repo_root)?;
        let worktree_entries = match &request.mode {
            GitImportMode::Head => {
                let export_root = export_git_head(&repo_root)?;
                let entries = collect_directory_tree(&export_root, false)?;
                let _ = fs::remove_dir_all(&export_root);
                entries
            }
            GitImportMode::WorkingTree {
                include_untracked,
                include_ignored,
            } => {
                let included =
                    git_working_tree_paths(&repo_root, *include_untracked, *include_ignored)?;
                collect_selected_paths(&repo_root, &included)?
            }
        };
        let refs = git_references(&repo_root)?;
        let index = git_index_snapshot(&repo_root)?;
        let objects = git_object_closure(&repo_root, &refs, head_commit.as_deref(), &index)?;
        let entries = import_repository_image_entries(
            &branch,
            head_commit.as_deref(),
            refs,
            index,
            objects,
            worktree_entries,
        )?;
        Ok(GitImportReport {
            source_path,
            target_root: request.target_root,
            repository_root: canonicalize_for_storage(&repo_root)?,
            object_format,
            head_commit,
            branch,
            remote_url,
            pathspec: vec![".".to_string()],
            dirty: !git_stdout(
                &repo_root,
                &["status", "--porcelain=v1", "--untracked-files=all"],
            )?
            .is_empty(),
            entries,
            metadata: request.metadata,
        })
    }

    async fn export_repository(
        &self,
        repository: Arc<dyn GitRepository>,
        request: GitExportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitExportReport, GitSubstrateError> {
        ensure_not_cancelled(cancellation.as_ref(), "export_repository")?;
        let head = repository.head().await?;
        let target_ref = export_target_ref(&head, request.branch_name.as_deref());
        let resolved_oid = resolve_repository_target_oid(repository.as_ref(), &target_ref).await?;
        let materialized = materialize_repository_to_host(
            repository.as_ref(),
            &resolved_oid,
            Path::new(&request.target_path),
            false,
            cancellation.as_ref(),
        )
        .await?;
        Ok(GitExportReport {
            target_path: request.target_path,
            branch_name: request.branch_name,
            metadata: BTreeMap::from([
                (
                    "written_paths".to_string(),
                    JsonValue::from(materialized.written_paths as u64),
                ),
                (
                    "deleted_paths".to_string(),
                    JsonValue::from(materialized.deleted_paths as u64),
                ),
                ("head_oid".to_string(), JsonValue::from(resolved_oid)),
            ]),
        })
    }

    async fn push(
        &self,
        repository: Arc<dyn GitRepository>,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError> {
        ensure_not_cancelled(cancellation.as_ref(), "push")?;
        let remote_url = request
            .metadata
            .get("remote_url")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| GitSubstrateError::Bridge {
                operation: "push",
                message: "remote_url metadata is required for host push".to_string(),
            })?;
        let pushed_head = if let Some(oid) = request.head_oid.as_deref() {
            oid.to_string()
        } else {
            repository
                .head()
                .await?
                .oid
                .ok_or_else(|| GitSubstrateError::Bridge {
                    operation: "push",
                    message: "repository HEAD is not available for push".to_string(),
                })?
        };
        let base_branch = request
            .metadata
            .get("base_branch")
            .and_then(JsonValue::as_str)
            .unwrap_or("main");
        let workspace_path = temp_export_path("push");
        clone_remote_checkout(
            remote_url,
            &request.branch_name,
            base_branch,
            &workspace_path,
        )?;
        materialize_repository_to_host(
            repository.as_ref(),
            &pushed_head,
            &workspace_path,
            true,
            cancellation.as_ref(),
        )
        .await?;
        let (title, body) = repository_head_message(repository.as_ref(), &pushed_head).await?;
        commit_host_checkout(&workspace_path, &title, &body)?;
        run_git(
            &workspace_path,
            &["push", "-u", &request.remote, &request.branch_name],
        )
        .map_err(|error| match error {
            GitSubstrateError::Bridge { message, .. } => GitSubstrateError::RemotePushFailed {
                remote: request.remote.clone(),
                branch: request.branch_name.clone(),
                message,
            },
            other => other,
        })?;
        let pushed_oid = git_stdout(&workspace_path, &["rev-parse", "HEAD"])?;
        Ok(GitPushReport {
            remote: request.remote,
            branch_name: request.branch_name,
            pushed_oid: Some(pushed_oid),
            metadata: request.metadata,
        })
    }

    async fn create_pull_request(
        &self,
        request: GitPullRequestRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPullRequestReport, GitSubstrateError> {
        ensure_not_cancelled(cancellation.as_ref(), "create_pull_request")?;
        if self.base_url.is_empty() || self.base_url.contains("example.invalid") {
            return Err(GitSubstrateError::PullRequestProvider {
                provider: self.name().to_string(),
                message: "provider adapter is not configured".to_string(),
            });
        }
        let mut counter = self.pr_counter.lock().await;
        let number = *counter;
        *counter += 1;
        Ok(GitPullRequestReport {
            url: format!("{}/pull/{}", self.base_url, number),
            head_branch: request.head_branch,
            base_branch: request.base_branch,
            metadata: request.metadata,
        })
    }
}

fn ensure_not_cancelled(
    cancellation: &dyn GitCancellationToken,
    operation: &'static str,
) -> Result<(), GitSubstrateError> {
    if cancellation.is_cancelled() {
        return Err(GitSubstrateError::Bridge {
            operation,
            message: "cancelled".to_string(),
        });
    }
    Ok(())
}

fn export_target_ref(head: &GitHeadState, branch_name: Option<&str>) -> String {
    branch_name
        .map(ToString::to_string)
        .or_else(|| head.symbolic_ref.clone())
        .or_else(|| head.oid.clone())
        .unwrap_or_else(|| "HEAD".to_string())
}

fn collect_directory_tree(
    root: &Path,
    skip_git_metadata: bool,
) -> Result<Vec<GitImportEntry>, GitSubstrateError> {
    if !root.exists() {
        return Err(GitSubstrateError::Bridge {
            operation: "import_repository",
            message: format!("source path does not exist: {}", root.display()),
        });
    }
    let mut entries = Vec::new();
    collect_directory_tree_recursive(root, root, skip_git_metadata, &mut entries)?;
    Ok(entries)
}

fn collect_selected_paths(
    root: &Path,
    included: &BTreeSet<String>,
) -> Result<Vec<GitImportEntry>, GitSubstrateError> {
    let mut entries = Vec::new();
    let mut created_dirs = BTreeSet::new();
    for relative in included {
        let path = root.join(relative);
        if !path.exists() {
            continue;
        }
        for ancestor in ancestor_relatives(relative) {
            if created_dirs.insert(ancestor.clone()) {
                entries.push(GitImportEntry {
                    path: ancestor,
                    kind: GitImportEntryKind::Directory,
                    data: None,
                    mode: None,
                    symlink_target: None,
                });
            }
        }
        let metadata = fs::symlink_metadata(&path).map_err(|error| GitSubstrateError::Bridge {
            operation: "import_repository",
            message: format!("{}: {}", path.display(), error),
        })?;
        if metadata.file_type().is_file() {
            entries.push(GitImportEntry {
                path: relative.clone(),
                kind: GitImportEntryKind::File,
                data: Some(fs::read(&path).map_err(|error| GitSubstrateError::Bridge {
                    operation: "import_repository",
                    message: format!("{}: {}", path.display(), error),
                })?),
                mode: Some(host_import_file_mode(&metadata)),
                symlink_target: None,
            });
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&path).map_err(|error| GitSubstrateError::Bridge {
                operation: "import_repository",
                message: format!("{}: {}", path.display(), error),
            })?;
            entries.push(GitImportEntry {
                path: relative.clone(),
                kind: GitImportEntryKind::Symlink,
                data: None,
                mode: None,
                symlink_target: Some(target.to_string_lossy().into_owned()),
            });
        } else if metadata.file_type().is_dir() && created_dirs.insert(relative.clone()) {
            entries.push(GitImportEntry {
                path: relative.clone(),
                kind: GitImportEntryKind::Directory,
                data: None,
                mode: None,
                symlink_target: None,
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
    entries: &mut Vec<GitImportEntry>,
) -> Result<(), GitSubstrateError> {
    let mut children = fs::read_dir(current)
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "import_repository",
            message: format!("{}: {}", current.display(), error),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "import_repository",
            message: format!("{}: {}", current.display(), error),
        })?;
    children.sort_by(|left, right| {
        left.file_name()
            .to_string_lossy()
            .cmp(&right.file_name().to_string_lossy())
    });
    for child in children {
        let file_name = child.file_name();
        if skip_git_metadata && file_name == ".git" {
            continue;
        }
        let child_path = child.path();
        let metadata =
            fs::symlink_metadata(&child_path).map_err(|error| GitSubstrateError::Bridge {
                operation: "import_repository",
                message: format!("{}: {}", child_path.display(), error),
            })?;
        let relative =
            normalize_relative_path(child_path.strip_prefix(root).map_err(|error| {
                GitSubstrateError::Bridge {
                    operation: "import_repository",
                    message: error.to_string(),
                }
            })?)?;
        if metadata.file_type().is_dir() {
            entries.push(GitImportEntry {
                path: relative.clone(),
                kind: GitImportEntryKind::Directory,
                data: None,
                mode: None,
                symlink_target: None,
            });
            collect_directory_tree_recursive(root, &child_path, skip_git_metadata, entries)?;
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&child_path).map_err(|error| GitSubstrateError::Bridge {
                operation: "import_repository",
                message: format!("{}: {}", child_path.display(), error),
            })?;
            entries.push(GitImportEntry {
                path: relative,
                kind: GitImportEntryKind::Symlink,
                data: None,
                mode: None,
                symlink_target: Some(target.to_string_lossy().into_owned()),
            });
        } else if metadata.file_type().is_file() {
            entries.push(GitImportEntry {
                path: relative,
                kind: GitImportEntryKind::File,
                data: Some(
                    fs::read(&child_path).map_err(|error| GitSubstrateError::Bridge {
                        operation: "import_repository",
                        message: format!("{}: {}", child_path.display(), error),
                    })?,
                ),
                mode: Some(host_import_file_mode(&metadata)),
                symlink_target: None,
            });
        }
    }
    Ok(())
}

fn export_git_head(repo_root: &Path) -> Result<PathBuf, GitSubstrateError> {
    let export_root = temp_export_path("git-head");
    fs::create_dir_all(&export_root).map_err(|error| GitSubstrateError::Bridge {
        operation: "import_repository",
        message: format!("{}: {}", export_root.display(), error),
    })?;
    let prefix = format!("{}/", export_root.to_string_lossy());
    run_git(repo_root, &["checkout-index", "--all", "--prefix", &prefix])?;
    Ok(export_root)
}

fn git_working_tree_paths(
    repo_root: &Path,
    include_untracked: bool,
    include_ignored: bool,
) -> Result<BTreeSet<String>, GitSubstrateError> {
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

fn git_repo_root(path: &Path) -> Result<PathBuf, GitSubstrateError> {
    Ok(PathBuf::from(git_stdout(
        path,
        &["rev-parse", "--show-toplevel"],
    )?))
}

fn git_ls_files(repo_root: &Path, args: &[&str]) -> Result<BTreeSet<String>, GitSubstrateError> {
    let output = git_command(repo_root, args)?;
    let mut paths = BTreeSet::new();
    for entry in output.split('\0').filter(|entry| !entry.is_empty()) {
        paths.insert(normalize_relative_path(Path::new(entry))?);
    }
    Ok(paths)
}

fn git_references(repo_root: &Path) -> Result<Vec<(String, String)>, GitSubstrateError> {
    let output = git_command(
        repo_root,
        &["for-each-ref", "--format=%(refname)%00%(objectname)%00"],
    )?;
    let mut refs = Vec::new();
    let mut fields = output.split('\0').filter(|field| !field.is_empty());
    while let Some(name) = fields.next() {
        let Some(target) = fields.next() else {
            break;
        };
        refs.push((name.to_string(), target.to_string()));
    }
    refs.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(refs)
}

fn git_index_snapshot(repo_root: &Path) -> Result<GitIndexSnapshot, GitSubstrateError> {
    let output = sanitized_git_command(repo_root)
        .args(["ls-files", "-s", "-z"])
        .output()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "host_git",
            message: format!("{}: {}", repo_root.display(), error),
        })?;
    if !output.status.success() {
        return Err(GitSubstrateError::Bridge {
            operation: "host_git",
            message: format!(
                "git -C {} ls-files -s -z failed: {}",
                repo_root.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    let mut entries = Vec::new();
    for record in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let text = String::from_utf8_lossy(record);
        let Some((header, path)) = text.split_once('\t') else {
            continue;
        };
        let mut fields = header.split_whitespace();
        let Some(mode) = fields.next() else {
            continue;
        };
        let Some(oid) = fields.next() else {
            continue;
        };
        let Some(stage) = fields.next() else {
            continue;
        };
        if stage != "0" {
            continue;
        }
        entries.push(GitIndexEntry {
            path: path.to_string(),
            oid: Some(oid.to_string()),
            mode: u32::from_str_radix(mode, 8).map_err(|_| GitSubstrateError::Bridge {
                operation: "import_repository",
                message: format!("invalid git index mode: {mode}"),
            })?,
        });
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(GitIndexSnapshot {
        entries,
        metadata: BTreeMap::new(),
    })
}

fn git_object_closure(
    repo_root: &Path,
    refs: &[(String, String)],
    head_commit: Option<&str>,
    index: &GitIndexSnapshot,
) -> Result<BTreeMap<String, (GitObjectKind, Vec<u8>)>, GitSubstrateError> {
    let mut pending = BTreeSet::new();
    if let Some(head_commit) = head_commit {
        pending.insert(head_commit.to_string());
    }
    for (_, target) in refs {
        pending.insert(target.clone());
    }
    for entry in &index.entries {
        if let Some(oid) = entry.oid.as_ref() {
            pending.insert(oid.clone());
        }
    }

    let mut objects = BTreeMap::new();
    while let Some(oid) = pending.pop_first() {
        if objects.contains_key(&oid) {
            continue;
        }
        let kind = git_object_kind(repo_root, &oid)?;
        let payload = git_object_payload(repo_root, &oid)?;
        for child in git_object_children(kind, &payload)? {
            if !objects.contains_key(&child) {
                pending.insert(child);
            }
        }
        objects.insert(oid, (kind, payload));
    }
    Ok(objects)
}

fn git_object_kind(repo_root: &Path, oid: &str) -> Result<GitObjectKind, GitSubstrateError> {
    match git_stdout(repo_root, &["cat-file", "-t", oid])?.as_str() {
        "blob" => Ok(GitObjectKind::Blob),
        "commit" => Ok(GitObjectKind::Commit),
        "tree" => Ok(GitObjectKind::Tree),
        "tag" => Ok(GitObjectKind::Tag),
        _ => Ok(GitObjectKind::Unknown),
    }
}

fn git_object_payload(repo_root: &Path, oid: &str) -> Result<Vec<u8>, GitSubstrateError> {
    let output = sanitized_git_command(repo_root)
        .args(["cat-file", "-p", oid])
        .output()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "host_git",
            message: format!("{}: {}", repo_root.display(), error),
        })?;
    if output.status.success() {
        return Ok(output.stdout);
    }
    Err(GitSubstrateError::Bridge {
        operation: "host_git",
        message: format!(
            "git -C {} cat-file -p {} failed: {}",
            repo_root.display(),
            oid,
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    })
}

fn git_object_children(
    kind: GitObjectKind,
    payload: &[u8],
) -> Result<Vec<String>, GitSubstrateError> {
    match kind {
        GitObjectKind::Commit => {
            let text = String::from_utf8_lossy(payload);
            let mut children = Vec::new();
            for line in text.lines() {
                if let Some(tree) = line.strip_prefix("tree ") {
                    children.push(tree.trim().to_string());
                } else if let Some(parent) = line.strip_prefix("parent ") {
                    children.push(parent.trim().to_string());
                }
            }
            Ok(children)
        }
        GitObjectKind::Tree => Ok(String::from_utf8_lossy(payload)
            .lines()
            .filter_map(|line| {
                let (header, _) = line.split_once('\t')?;
                let mut fields = header.split_whitespace();
                fields.next()?;
                fields.next()?;
                fields.next().map(str::to_string)
            })
            .collect()),
        _ => Ok(Vec::new()),
    }
}

fn import_repository_image_entries(
    branch: &Option<String>,
    head_commit: Option<&str>,
    refs: Vec<(String, String)>,
    index: GitIndexSnapshot,
    objects: BTreeMap<String, (GitObjectKind, Vec<u8>)>,
    worktree_entries: Vec<GitImportEntry>,
) -> Result<Vec<GitImportEntry>, GitSubstrateError> {
    let mut entries = BTreeMap::new();
    insert_import_directory(&mut entries, ".git");
    insert_import_directory(&mut entries, ".git/objects");
    insert_import_directory(&mut entries, ".git/refs");
    let head_payload = branch
        .as_ref()
        .map(|branch| format!("ref: refs/heads/{branch}\n"))
        .or_else(|| head_commit.map(|oid| format!("{oid}\n")))
        .unwrap_or_else(|| "ref: refs/heads/main\n".to_string());
    insert_import_file(&mut entries, ".git/HEAD", head_payload.into_bytes(), 0o644);
    insert_import_file(
        &mut entries,
        ".git/index.json",
        serde_json::to_vec(&index)?,
        0o644,
    );
    for (name, target) in refs {
        insert_import_file(
            &mut entries,
            &format!(".git/{name}"),
            format!("{target}\n").into_bytes(),
            0o644,
        );
    }
    for (oid, (kind, payload)) in objects {
        insert_import_file(
            &mut entries,
            &format!(".git/objects/{oid}"),
            [
                git_object_kind_name(kind).as_bytes(),
                b"\n",
                payload.as_slice(),
            ]
            .concat(),
            0o644,
        );
    }
    for entry in worktree_entries {
        insert_import_entry(&mut entries, entry);
    }
    Ok(entries.into_values().collect())
}

fn insert_import_entry(entries: &mut BTreeMap<String, GitImportEntry>, entry: GitImportEntry) {
    if entry.path.contains('/') {
        for ancestor in ancestor_relatives(&entry.path) {
            insert_import_directory(entries, &ancestor);
        }
    }
    entries.insert(entry.path.clone(), entry);
}

fn insert_import_directory(entries: &mut BTreeMap<String, GitImportEntry>, path: &str) {
    entries
        .entry(path.to_string())
        .or_insert_with(|| GitImportEntry {
            path: path.to_string(),
            kind: GitImportEntryKind::Directory,
            data: None,
            mode: None,
            symlink_target: None,
        });
}

fn insert_import_file(
    entries: &mut BTreeMap<String, GitImportEntry>,
    path: &str,
    data: Vec<u8>,
    mode: u32,
) {
    if path.contains('/') {
        for ancestor in ancestor_relatives(path) {
            insert_import_directory(entries, &ancestor);
        }
    }
    entries.insert(
        path.to_string(),
        GitImportEntry {
            path: path.to_string(),
            kind: GitImportEntryKind::File,
            data: Some(data),
            mode: Some(mode),
            symlink_target: None,
        },
    );
}

fn git_object_kind_name(kind: GitObjectKind) -> &'static str {
    match kind {
        GitObjectKind::Blob => "blob",
        GitObjectKind::Commit => "commit",
        GitObjectKind::Tree => "tree",
        GitObjectKind::Tag => "tag",
        GitObjectKind::Unknown => "unknown",
    }
}

fn git_object_format(repo_root: &Path) -> Result<GitObjectFormat, GitSubstrateError> {
    if let Some(value) = git_stdout_optional(
        repo_root,
        &["rev-parse", "--show-object-format"],
        OptionalGitLookup::HeadCommit,
    )? {
        return parse_git_object_format(&value);
    }
    if let Some(value) = git_stdout_optional(
        repo_root,
        &["config", "--get", "extensions.objectformat"],
        OptionalGitLookup::HeadCommit,
    )? {
        return parse_git_object_format(&value);
    }
    Ok(GitObjectFormat::Sha1)
}

fn parse_git_object_format(value: &str) -> Result<GitObjectFormat, GitSubstrateError> {
    match value.trim() {
        "sha1" => Ok(GitObjectFormat::Sha1),
        "sha256" => Ok(GitObjectFormat::Sha256),
        other => Err(GitSubstrateError::Bridge {
            operation: "import_repository",
            message: format!("unsupported git object format: {other}"),
        }),
    }
}

async fn resolve_repository_target_oid(
    repository: &dyn GitRepository,
    target_ref: &str,
) -> Result<String, GitSubstrateError> {
    if target_ref == "HEAD" {
        return repository
            .head()
            .await?
            .oid
            .ok_or_else(|| GitSubstrateError::ReferenceNotFound {
                reference: "HEAD".to_string(),
            });
    }
    for reference in repository.list_refs().await? {
        if reference.name == target_ref
            || reference.name == format!("refs/heads/{target_ref}")
            || reference.name == format!("refs/tags/{target_ref}")
        {
            return Ok(reference.target);
        }
    }
    if repository.read_object(target_ref).await?.is_some() {
        return Ok(target_ref.to_string());
    }
    Err(GitSubstrateError::ReferenceNotFound {
        reference: target_ref.to_string(),
    })
}

#[derive(Clone, Copy, Debug, Default)]
struct HostMaterializeReport {
    written_paths: usize,
    deleted_paths: usize,
}

async fn materialize_repository_to_host(
    repository: &dyn GitRepository,
    target_oid: &str,
    target_path: &Path,
    preserve_git_metadata: bool,
    cancellation: &dyn GitCancellationToken,
) -> Result<HostMaterializeReport, GitSubstrateError> {
    if cancellation.is_cancelled() {
        return Err(GitSubstrateError::Cancelled {
            repository_id: repository.handle().repository_id,
        });
    }
    let deleted_paths = clear_host_target(target_path, preserve_git_metadata)?;
    fs::create_dir_all(target_path).map_err(|error| GitSubstrateError::ExportConflict {
        path: target_path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })?;
    let mut written_paths = 0;
    if let Some(tree_oid) = repository_commit_tree_oid(repository, target_oid).await? {
        written_paths =
            materialize_tree_oid(repository, &tree_oid, target_path, cancellation).await?;
    }
    Ok(HostMaterializeReport {
        written_paths,
        deleted_paths,
    })
}

async fn repository_commit_tree_oid(
    repository: &dyn GitRepository,
    target_oid: &str,
) -> Result<Option<String>, GitSubstrateError> {
    let Some(object) = repository.read_object(target_oid).await? else {
        return Ok(None);
    };
    match object.kind {
        GitObjectKind::Commit => {
            Ok(String::from_utf8_lossy(&object.data)
                .lines()
                .find_map(|line| {
                    line.strip_prefix("tree ")
                        .map(str::trim)
                        .map(str::to_string)
                }))
        }
        GitObjectKind::Tree => Ok(Some(target_oid.to_string())),
        _ => Err(GitSubstrateError::InvalidObject {
            oid: target_oid.to_string(),
            message: "expected commit or tree object".to_string(),
        }),
    }
}

async fn materialize_tree_oid(
    repository: &dyn GitRepository,
    tree_oid: &str,
    target_path: &Path,
    cancellation: &dyn GitCancellationToken,
) -> Result<usize, GitSubstrateError> {
    if cancellation.is_cancelled() {
        return Err(GitSubstrateError::Cancelled {
            repository_id: repository.handle().repository_id,
        });
    }
    let Some(object) = repository.read_object(tree_oid).await? else {
        return Err(GitSubstrateError::ObjectNotFound {
            oid: tree_oid.to_string(),
        });
    };
    if object.kind != GitObjectKind::Tree {
        return Err(GitSubstrateError::InvalidObject {
            oid: tree_oid.to_string(),
            message: "expected tree object".to_string(),
        });
    }
    let mut written = 0;
    for line in String::from_utf8_lossy(&object.data).lines() {
        if line.trim().is_empty() {
            continue;
        }
        let (header, name) =
            line.split_once('\t')
                .ok_or_else(|| GitSubstrateError::InvalidObject {
                    oid: tree_oid.to_string(),
                    message: format!("tree entry is missing a path: {line}"),
                })?;
        let mut fields = header.split_whitespace();
        let mode = fields
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: tree_oid.to_string(),
                message: format!("tree entry is missing a mode: {line}"),
            })
            .and_then(|mode| {
                u32::from_str_radix(mode, 8).map_err(|_| GitSubstrateError::InvalidObject {
                    oid: tree_oid.to_string(),
                    message: format!("invalid tree mode: {line}"),
                })
            })?;
        let kind = fields
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: tree_oid.to_string(),
                message: format!("tree entry is missing a kind: {line}"),
            })?;
        let oid = fields
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: tree_oid.to_string(),
                message: format!("tree entry is missing an oid: {line}"),
            })?;
        let destination = target_path.join(name);
        if kind == "tree" {
            fs::create_dir_all(&destination).map_err(|error| {
                GitSubstrateError::ExportConflict {
                    path: destination.to_string_lossy().into_owned(),
                    message: error.to_string(),
                }
            })?;
            written += Box::pin(materialize_tree_oid(
                repository,
                oid,
                &destination,
                cancellation,
            ))
            .await?;
            continue;
        }
        let Some(blob) = repository.read_object(oid).await? else {
            return Err(GitSubstrateError::ObjectNotFound {
                oid: oid.to_string(),
            });
        };
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| GitSubstrateError::ExportConflict {
                path: parent.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
        }
        if (mode & 0o170000) == 0o120000 {
            let target =
                String::from_utf8(blob.data).map_err(|_| GitSubstrateError::InvalidObject {
                    oid: oid.to_string(),
                    message: "symlink blob is not valid UTF-8".to_string(),
                })?;
            if destination.exists() {
                remove_host_path(&destination)?;
            }
            #[cfg(unix)]
            std::os::unix::fs::symlink(&target, &destination).map_err(|error| {
                GitSubstrateError::ExportConflict {
                    path: destination.to_string_lossy().into_owned(),
                    message: error.to_string(),
                }
            })?;
            #[cfg(not(unix))]
            {
                let _ = target;
                return Err(GitSubstrateError::Bridge {
                    operation: "export_repository",
                    message: "symlink materialization requires unix support".to_string(),
                });
            }
        } else {
            fs::write(&destination, blob.data).map_err(|error| {
                GitSubstrateError::ExportConflict {
                    path: destination.to_string_lossy().into_owned(),
                    message: error.to_string(),
                }
            })?;
            apply_host_file_mode(&destination, mode)?;
        }
        written += 1;
    }
    Ok(written)
}

fn clear_host_target(path: &Path, preserve_git_metadata: bool) -> Result<usize, GitSubstrateError> {
    if !path.exists() {
        return Ok(0);
    }
    if !path.is_dir() {
        remove_host_path(path)?;
        return Ok(1);
    }
    let mut removed = 0;
    for child in fs::read_dir(path).map_err(|error| GitSubstrateError::ExportConflict {
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })? {
        let child = child.map_err(|error| GitSubstrateError::ExportConflict {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
        if preserve_git_metadata && child.file_name() == ".git" {
            continue;
        }
        remove_host_path(&child.path())?;
        removed += 1;
    }
    Ok(removed)
}

fn remove_host_path(path: &Path) -> Result<(), GitSubstrateError> {
    if path.is_dir()
        && !path
            .symlink_metadata()
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
    {
        fs::remove_dir_all(path).map_err(|error| GitSubstrateError::ExportConflict {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    } else {
        fs::remove_file(path).map_err(|error| GitSubstrateError::ExportConflict {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    }
    Ok(())
}

fn host_import_file_mode(metadata: &fs::Metadata) -> u32 {
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

fn apply_host_file_mode(path: &Path, mode: u32) -> Result<(), GitSubstrateError> {
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(path)
            .map_err(|error| GitSubstrateError::ExportConflict {
                path: path.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?
            .permissions();
        permissions.set_mode(mode & 0o777);
        fs::set_permissions(path, permissions).map_err(|error| {
            GitSubstrateError::ExportConflict {
                path: path.to_string_lossy().into_owned(),
                message: error.to_string(),
            }
        })?;
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
    Ok(())
}

fn clone_remote_checkout(
    remote_url: &str,
    branch_name: &str,
    base_branch: &str,
    workspace_path: &Path,
) -> Result<(), GitSubstrateError> {
    if workspace_path.exists() {
        let _ = fs::remove_dir_all(workspace_path);
    }
    let clone_cwd = workspace_path.parent().unwrap_or_else(|| Path::new("/"));
    let output = sanitized_git_command(clone_cwd)
        .args([
            "clone",
            remote_url,
            workspace_path.to_string_lossy().as_ref(),
        ])
        .output()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "push",
            message: format!("clone {}: {}", workspace_path.display(), error),
        })?;
    if output.status.success() {
        let remote_branch_ref = format!("refs/remotes/origin/{branch_name}");
        let checkout_source = if git_ref_exists(workspace_path, &remote_branch_ref)? {
            remote_branch_ref
        } else {
            format!("refs/remotes/origin/{base_branch}")
        };
        run_git(
            workspace_path,
            &["checkout", "-B", branch_name, checkout_source.as_str()],
        )?;
        return Ok(());
    }
    Err(GitSubstrateError::Bridge {
        operation: "push",
        message: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn git_ref_exists(repo_root: &Path, reference: &str) -> Result<bool, GitSubstrateError> {
    let output = sanitized_git_command(repo_root)
        .args(["show-ref", "--verify", "--quiet", reference])
        .output()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "host_git",
            message: format!("{}: {}", repo_root.display(), error),
        })?;
    Ok(output.status.success())
}

async fn repository_head_message(
    repository: &dyn GitRepository,
    oid: &str,
) -> Result<(String, String), GitSubstrateError> {
    let Some(object) = repository.read_object(oid).await? else {
        return Err(GitSubstrateError::ObjectNotFound {
            oid: oid.to_string(),
        });
    };
    if object.kind != GitObjectKind::Commit {
        return Ok(("TerraceDB export".to_string(), String::new()));
    }
    let text = String::from_utf8_lossy(&object.data);
    let message = text
        .split_once("\n\n")
        .map(|(_, body)| body.trim())
        .unwrap_or("TerraceDB export");
    let mut lines = message.lines();
    let title = lines
        .next()
        .unwrap_or("TerraceDB export")
        .trim()
        .to_string();
    let body = lines.collect::<Vec<_>>().join("\n").trim().to_string();
    Ok((title, body))
}

fn commit_host_checkout(
    workspace_path: &Path,
    title: &str,
    body: &str,
) -> Result<(), GitSubstrateError> {
    run_git(workspace_path, &["add", "-A"])?;
    let status = git_stdout(workspace_path, &["status", "--porcelain=v1"])?;
    if status.trim().is_empty() {
        return Ok(());
    }
    let mut commit = sanitized_git_command(workspace_path);
    commit
        .arg("commit")
        .arg("-m")
        .arg(title)
        .env("GIT_AUTHOR_NAME", "TerraceDB Sandbox")
        .env("GIT_AUTHOR_EMAIL", "sandbox@example.invalid")
        .env("GIT_COMMITTER_NAME", "TerraceDB Sandbox")
        .env("GIT_COMMITTER_EMAIL", "sandbox@example.invalid");
    if let Ok(author_date) = env::var("GIT_AUTHOR_DATE") {
        commit.env("GIT_AUTHOR_DATE", author_date);
    }
    if let Ok(committer_date) = env::var("GIT_COMMITTER_DATE") {
        commit.env("GIT_COMMITTER_DATE", committer_date);
    }
    if !body.trim().is_empty() {
        commit.arg("-m").arg(body.trim());
    }
    let output = commit.output().map_err(|error| GitSubstrateError::Bridge {
        operation: "push",
        message: format!("{}: {}", workspace_path.display(), error),
    })?;
    if output.status.success() {
        return Ok(());
    }
    Err(GitSubstrateError::Bridge {
        operation: "push",
        message: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn run_git(repo_root: &Path, args: &[&str]) -> Result<(), GitSubstrateError> {
    let output = sanitized_git_command(repo_root)
        .args(args)
        .output()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "host_git",
            message: format!("{}: {}", repo_root.display(), error),
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(GitSubstrateError::Bridge {
        operation: "host_git",
        message: format!(
            "git -C {} {} failed: {}",
            repo_root.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    })
}

fn git_stdout(repo_root: &Path, args: &[&str]) -> Result<String, GitSubstrateError> {
    Ok(git_command(repo_root, args)?.trim().to_string())
}

fn git_stdout_optional(
    repo_root: &Path,
    args: &[&str],
    lookup: OptionalGitLookup,
) -> Result<Option<String>, GitSubstrateError> {
    let output = sanitized_git_command(repo_root)
        .args(args)
        .output()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "host_git",
            message: format!("{}: {}", repo_root.display(), error),
        })?;
    if output.status.success() {
        return Ok(Some(
            String::from_utf8_lossy(&output.stdout).trim().to_string(),
        ));
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if optional_lookup_is_missing(lookup, &stderr) {
        return Ok(None);
    }
    Err(GitSubstrateError::Bridge {
        operation: "host_git",
        message: format!(
            "git -C {} {} failed: {}",
            repo_root.display(),
            args.join(" "),
            stderr
        ),
    })
}

fn optional_lookup_is_missing(lookup: OptionalGitLookup, stderr: &str) -> bool {
    match lookup {
        OptionalGitLookup::HeadCommit => {
            stderr.contains("unknown revision")
                || stderr.contains("Needed a single revision")
                || stderr.contains("bad revision")
        }
        OptionalGitLookup::SymbolicHead => {
            stderr.contains("not a symbolic ref") || stderr.is_empty()
        }
        OptionalGitLookup::OriginRemote => stderr.contains("No such remote"),
    }
}

fn git_command(repo_root: &Path, args: &[&str]) -> Result<String, GitSubstrateError> {
    let output = sanitized_git_command(repo_root)
        .args(args)
        .output()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "host_git",
            message: format!("{}: {}", repo_root.display(), error),
        })?;
    if output.status.success() {
        return String::from_utf8(output.stdout).map_err(|error| GitSubstrateError::Bridge {
            operation: "host_git",
            message: error.to_string(),
        });
    }
    Err(GitSubstrateError::Bridge {
        operation: "host_git",
        message: format!(
            "git -C {} {} failed: {}",
            repo_root.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ),
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
    let unique_suffix = IMPORT_EXPORT_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "terracedb-git-{label}-{}-{unique_suffix}",
        std::process::id()
    ))
}

fn canonicalize_for_storage(path: &Path) -> Result<String, GitSubstrateError> {
    path.canonicalize()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "host_git",
            message: format!("{}: {}", path.display(), error),
        })
        .map(|path| path.to_string_lossy().into_owned())
}

fn normalize_relative_path(path: &Path) -> Result<String, GitSubstrateError> {
    let mut normalized = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => normalized.push(segment.to_string_lossy().into_owned()),
            Component::ParentDir => {
                normalized.pop();
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(GitSubstrateError::Bridge {
                    operation: "import_repository",
                    message: format!("path is not relative: {}", path.display()),
                });
            }
        }
    }
    Ok(normalized.join("/"))
}

fn ancestor_relatives(path: &str) -> Vec<String> {
    let parts = path.split('/').collect::<Vec<_>>();
    let mut ancestors = Vec::new();
    for index in 1..parts.len() {
        ancestors.push(parts[..index].join("/"));
    }
    ancestors
}
