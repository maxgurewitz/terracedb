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

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::{
    GitCancellationToken, GitCheckoutRequest, GitExportReport, GitExportRequest,
    GitFinalizeExportReport, GitFinalizeExportRequest, GitHeadState, GitHostBridge, GitImportEntry,
    GitImportEntryKind, GitImportMode, GitImportReport, GitImportRequest, GitPullRequestReport,
    GitPullRequestRequest, GitPushReport, GitPushRequest, GitRepository, GitSubstrateError,
    GitWorkspaceReport, GitWorkspaceRequest,
};

static IMPORT_EXPORT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
enum OptionalGitLookup {
    HeadCommit,
    SymbolicHead,
    OriginRemote,
    GitDir,
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
        let entries = match &request.mode {
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
        Ok(GitImportReport {
            source_path,
            target_root: request.target_root,
            repository_root: canonicalize_for_storage(&repo_root)?,
            head_commit: git_stdout_optional(
                &repo_root,
                &["rev-parse", "HEAD"],
                OptionalGitLookup::HeadCommit,
            )?,
            branch: git_stdout_optional(
                &repo_root,
                &["symbolic-ref", "--quiet", "--short", "HEAD"],
                OptionalGitLookup::SymbolicHead,
            )?,
            remote_url: git_stdout_optional(
                &repo_root,
                &["remote", "get-url", "origin"],
                OptionalGitLookup::OriginRemote,
            )?,
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

    async fn prepare_workspace(
        &self,
        request: GitWorkspaceRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitWorkspaceReport, GitSubstrateError> {
        ensure_not_cancelled(cancellation.as_ref(), "prepare_workspace")?;
        let repo_root = PathBuf::from(&request.repo_root);
        let workspace_path = PathBuf::from(&request.target_path);
        ensure_empty_workspace_path(&workspace_path)?;
        if let Some(parent) = workspace_path.parent() {
            fs::create_dir_all(parent).map_err(|error| GitSubstrateError::Bridge {
                operation: "prepare_workspace",
                message: format!("{}: {}", parent.display(), error),
            })?;
        }
        run_git(
            &repo_root,
            &[
                "worktree",
                "add",
                "--detach",
                &request.target_path,
                &request.base_ref,
            ],
        )?;
        run_git(
            &workspace_path,
            &["checkout", "-B", &request.branch_name, &request.base_ref],
        )?;
        let head_commit = git_stdout(&workspace_path, &["rev-parse", "HEAD"])?;
        let remote_url = git_stdout_optional(
            &repo_root,
            &["remote", "get-url", "origin"],
            OptionalGitLookup::OriginRemote,
        )?;
        let mut metadata = request.metadata;
        metadata.insert(
            "repo_root".to_string(),
            JsonValue::from(request.repo_root.clone()),
        );
        metadata.insert(
            "base_ref".to_string(),
            JsonValue::from(request.base_ref.clone()),
        );
        metadata.insert("head_commit".to_string(), JsonValue::from(head_commit));
        metadata.insert(
            "remote_url".to_string(),
            remote_url.map(JsonValue::from).unwrap_or(JsonValue::Null),
        );
        Ok(GitWorkspaceReport {
            bridge: self.name().to_string(),
            branch_name: request.branch_name,
            workspace_path: request.target_path,
            metadata,
        })
    }

    async fn finalize_export(
        &self,
        request: GitFinalizeExportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitFinalizeExportReport, GitSubstrateError> {
        ensure_not_cancelled(cancellation.as_ref(), "finalize_export")?;
        let workspace_path = PathBuf::from(&request.workspace_path);
        let mut metadata = request.metadata;
        metadata.insert(
            "workspace_path".to_string(),
            JsonValue::from(request.workspace_path.clone()),
        );
        metadata.insert(
            "branch_name".to_string(),
            JsonValue::from(request.head_branch.clone()),
        );
        if !is_git_workspace(&workspace_path) {
            return Ok(GitFinalizeExportReport {
                workspace_path: request.workspace_path,
                head_branch: request.head_branch,
                metadata,
            });
        }
        run_git(&workspace_path, &["add", "-A"])?;
        let status = git_stdout(&workspace_path, &["status", "--porcelain=v1"])?;
        if status.trim().is_empty() {
            metadata.insert("committed".to_string(), JsonValue::from(false));
            metadata.insert("pushed".to_string(), JsonValue::from(false));
            metadata.insert(
                "head_commit".to_string(),
                JsonValue::from(git_stdout(&workspace_path, &["rev-parse", "HEAD"])?),
            );
            return Ok(GitFinalizeExportReport {
                workspace_path: request.workspace_path,
                head_branch: request.head_branch,
                metadata,
            });
        }

        let mut commit = sanitized_git_command(&workspace_path);
        commit
            .arg("commit")
            .arg("-m")
            .arg(&request.title)
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
        if !request.body.trim().is_empty() {
            commit.arg("-m").arg(&request.body);
        }
        let output = commit.output().map_err(|error| GitSubstrateError::Bridge {
            operation: "finalize_export",
            message: format!("{}: {}", workspace_path.display(), error),
        })?;
        if !output.status.success() {
            return Err(GitSubstrateError::Bridge {
                operation: "finalize_export",
                message: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        metadata.insert("committed".to_string(), JsonValue::from(true));
        metadata.insert(
            "head_commit".to_string(),
            JsonValue::from(git_stdout(&workspace_path, &["rev-parse", "HEAD"])?),
        );
        let remote = git_stdout_optional(
            &workspace_path,
            &["remote", "get-url", "origin"],
            OptionalGitLookup::OriginRemote,
        )?;
        if let Some(remote_url) = remote {
            run_git(
                &workspace_path,
                &["push", "-u", "origin", &request.head_branch],
            )
            .map_err(|error| match error {
                GitSubstrateError::Bridge { message, .. } => GitSubstrateError::RemotePushFailed {
                    remote: "origin".to_string(),
                    branch: request.head_branch.clone(),
                    message,
                },
                other => other,
            })?;
            metadata.insert("pushed".to_string(), JsonValue::from(true));
            metadata.insert("push_remote".to_string(), JsonValue::from(remote_url));
        } else {
            metadata.insert("pushed".to_string(), JsonValue::from(false));
        }
        Ok(GitFinalizeExportReport {
            workspace_path: request.workspace_path,
            head_branch: request.head_branch,
            metadata,
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
        let checkout = repository
            .checkout(
                GitCheckoutRequest {
                    target_ref,
                    materialize_path: request.target_path.clone(),
                    pathspec: Vec::new(),
                    update_head: false,
                },
                cancellation,
            )
            .await?;
        Ok(GitExportReport {
            target_path: request.target_path,
            branch_name: request.branch_name,
            metadata: BTreeMap::from([
                (
                    "written_paths".to_string(),
                    JsonValue::from(checkout.written_paths as u64),
                ),
                (
                    "deleted_paths".to_string(),
                    JsonValue::from(checkout.deleted_paths as u64),
                ),
                (
                    "head_oid".to_string(),
                    checkout
                        .head_oid
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
            ]),
        })
    }

    async fn push(
        &self,
        _repository: Arc<dyn GitRepository>,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError> {
        ensure_not_cancelled(cancellation.as_ref(), "push")?;
        let workspace_path = request
            .metadata
            .get("workspace_path")
            .and_then(JsonValue::as_str)
            .map(PathBuf::from)
            .ok_or_else(|| GitSubstrateError::Bridge {
                operation: "push",
                message: "workspace_path metadata is required for host push".to_string(),
            })?;
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
        Ok(GitPushReport {
            remote: request.remote,
            branch_name: request.branch_name,
            pushed_oid: Some(git_stdout(&workspace_path, &["rev-parse", "HEAD"])?),
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

fn ensure_empty_workspace_path(path: &Path) -> Result<(), GitSubstrateError> {
    if !path.exists() {
        return Ok(());
    }
    if !path.is_dir() {
        return Err(GitSubstrateError::ExportConflict {
            path: path.to_string_lossy().into_owned(),
            message: "target workspace path must be a directory".to_string(),
        });
    }
    let mut entries = fs::read_dir(path).map_err(|error| GitSubstrateError::ExportConflict {
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|error| GitSubstrateError::ExportConflict {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?
        .is_some()
    {
        return Err(GitSubstrateError::ExportConflict {
            path: path.to_string_lossy().into_owned(),
            message: "target workspace path must be empty".to_string(),
        });
    }
    fs::remove_dir(path).map_err(|error| GitSubstrateError::ExportConflict {
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })?;
    Ok(())
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
                symlink_target: Some(target.to_string_lossy().into_owned()),
            });
        } else if metadata.file_type().is_dir() && created_dirs.insert(relative.clone()) {
            entries.push(GitImportEntry {
                path: relative.clone(),
                kind: GitImportEntryKind::Directory,
                data: None,
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
                symlink_target: None,
            });
        }
    }
    Ok(())
}

fn export_git_head(repo_root: &Path) -> Result<PathBuf, GitSubstrateError> {
    let export_root = temp_export_path("git-head");
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

fn is_git_workspace(path: &Path) -> bool {
    path.join(".git").exists()
        || matches!(
            git_stdout_optional(path, &["rev-parse", "--git-dir"], OptionalGitLookup::GitDir),
            Ok(Some(_))
        )
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
        OptionalGitLookup::GitDir => stderr.contains("not a git repository"),
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
