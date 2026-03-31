use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb_vfs::JsonValue;

use crate::{SandboxError, SandboxSession};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitWorkspaceRequest {
    pub branch_name: String,
    pub base_branch: Option<String>,
    pub target_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitWorkspaceReport {
    pub manager: String,
    pub branch_name: String,
    pub workspace_path: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait]
pub trait GitWorkspaceManager: Send + Sync {
    fn name(&self) -> &str;
    async fn prepare_workspace(
        &self,
        session: &SandboxSession,
        request: GitWorkspaceRequest,
    ) -> Result<GitWorkspaceReport, SandboxError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicGitWorkspaceManager {
    name: String,
}

impl DeterministicGitWorkspaceManager {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for DeterministicGitWorkspaceManager {
    fn default() -> Self {
        Self::new("deterministic-git")
    }
}

#[async_trait]
impl GitWorkspaceManager for DeterministicGitWorkspaceManager {
    fn name(&self) -> &str {
        &self.name
    }

    async fn prepare_workspace(
        &self,
        session: &SandboxSession,
        request: GitWorkspaceRequest,
    ) -> Result<GitWorkspaceReport, SandboxError> {
        let info = session.info().await;
        let provenance = info.provenance.git.clone();
        let base_ref = provenance
            .as_ref()
            .and_then(|git| git.head_commit.clone())
            .or_else(|| request.base_branch.clone())
            .or_else(|| provenance.as_ref().and_then(|git| git.branch.clone()))
            .unwrap_or_else(|| "HEAD".to_string());
        Ok(GitWorkspaceReport {
            manager: self.name.clone(),
            branch_name: request.branch_name.clone(),
            workspace_path: request.target_path.clone(),
            metadata: BTreeMap::from([
                (
                    "session_volume_id".to_string(),
                    JsonValue::from(info.session_volume_id.to_string()),
                ),
                (
                    "base_branch".to_string(),
                    request
                        .base_branch
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
                ("base_ref".to_string(), JsonValue::from(base_ref)),
                (
                    "repo_root".to_string(),
                    provenance
                        .as_ref()
                        .map(|git| JsonValue::from(git.repo_root.clone()))
                        .unwrap_or(JsonValue::Null),
                ),
                (
                    "head_commit".to_string(),
                    provenance
                        .as_ref()
                        .and_then(|git| git.head_commit.clone())
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
                (
                    "remote_url".to_string(),
                    provenance
                        .and_then(|git| git.remote_url)
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
            ]),
        })
    }
}

#[derive(Clone, Debug)]
pub struct HostGitWorkspaceManager {
    name: String,
}

impl HostGitWorkspaceManager {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for HostGitWorkspaceManager {
    fn default() -> Self {
        Self::new("host-git")
    }
}

#[async_trait]
impl GitWorkspaceManager for HostGitWorkspaceManager {
    fn name(&self) -> &str {
        &self.name
    }

    async fn prepare_workspace(
        &self,
        session: &SandboxSession,
        request: GitWorkspaceRequest,
    ) -> Result<GitWorkspaceReport, SandboxError> {
        let info = session.info().await;
        let provenance = info
            .provenance
            .git
            .ok_or(SandboxError::MissingGitProvenance)?;
        let repo_root = PathBuf::from(provenance.repo_root.clone());
        let base_ref = provenance
            .head_commit
            .clone()
            .or(request.base_branch.clone())
            .or(provenance.branch.clone())
            .unwrap_or_else(|| "HEAD".to_string());
        let workspace_path = PathBuf::from(&request.target_path);
        if workspace_path.exists() {
            if workspace_path.is_dir() {
                let mut entries =
                    fs::read_dir(&workspace_path).map_err(|error| SandboxError::Io {
                        path: workspace_path.to_string_lossy().into_owned(),
                        message: error.to_string(),
                    })?;
                if entries
                    .next()
                    .transpose()
                    .map_err(|error| SandboxError::Io {
                        path: workspace_path.to_string_lossy().into_owned(),
                        message: error.to_string(),
                    })?
                    .is_some()
                {
                    return Err(SandboxError::Io {
                        path: workspace_path.to_string_lossy().into_owned(),
                        message: "target workspace path must be empty".to_string(),
                    });
                }
                fs::remove_dir(&workspace_path).map_err(|error| SandboxError::Io {
                    path: workspace_path.to_string_lossy().into_owned(),
                    message: error.to_string(),
                })?;
            } else {
                return Err(SandboxError::Io {
                    path: workspace_path.to_string_lossy().into_owned(),
                    message: "target workspace path must be a directory".to_string(),
                });
            }
        }
        if let Some(parent) = workspace_path.parent() {
            fs::create_dir_all(parent).map_err(|error| SandboxError::Io {
                path: parent.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
        }
        run_git(
            &repo_root,
            &[
                "worktree",
                "add",
                "--detach",
                &request.target_path,
                &base_ref,
            ],
        )?;
        run_git(
            &workspace_path,
            &["checkout", "-B", &request.branch_name, &base_ref],
        )?;
        let head_commit = git_stdout(&workspace_path, &["rev-parse", "HEAD"])?;
        Ok(GitWorkspaceReport {
            manager: self.name.clone(),
            branch_name: request.branch_name.clone(),
            workspace_path: request.target_path.clone(),
            metadata: BTreeMap::from([
                (
                    "repo_root".to_string(),
                    JsonValue::from(provenance.repo_root.clone()),
                ),
                ("base_ref".to_string(), JsonValue::from(base_ref)),
                ("head_commit".to_string(), JsonValue::from(head_commit)),
                (
                    "remote_url".to_string(),
                    provenance
                        .remote_url
                        .map(JsonValue::from)
                        .unwrap_or(JsonValue::Null),
                ),
            ]),
        })
    }
}

pub(crate) fn default_export_workspace_path(session_id: &str, branch_name: &str) -> PathBuf {
    let branch = sanitize_label(branch_name);
    let path = std::env::temp_dir().join(format!("terracedb-sandbox-{session_id}-{branch}"));
    let _ = fs::remove_dir_all(&path);
    path
}

pub(crate) fn finalize_git_export(
    workspace_path: &Path,
    title: &str,
    body: &str,
    head_branch: &str,
) -> Result<BTreeMap<String, JsonValue>, SandboxError> {
    if !is_git_workspace(workspace_path) {
        return Ok(BTreeMap::new());
    }
    run_git(workspace_path, &["add", "-A"])?;
    let status = git_stdout(workspace_path, &["status", "--porcelain=v1"])?;
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "workspace_path".to_string(),
        JsonValue::from(workspace_path.to_string_lossy().into_owned()),
    );
    metadata.insert(
        "branch_name".to_string(),
        JsonValue::from(head_branch.to_string()),
    );
    if status.trim().is_empty() {
        metadata.insert("committed".to_string(), JsonValue::from(false));
        metadata.insert(
            "head_commit".to_string(),
            JsonValue::from(git_stdout(workspace_path, &["rev-parse", "HEAD"])?),
        );
        return Ok(metadata);
    }
    let mut commit = git_command(workspace_path);
    commit
        .arg("commit")
        .arg("-m")
        .arg(title)
        .env("GIT_AUTHOR_NAME", "TerraceDB Sandbox")
        .env("GIT_AUTHOR_EMAIL", "sandbox@example.invalid")
        .env("GIT_COMMITTER_NAME", "TerraceDB Sandbox")
        .env("GIT_COMMITTER_EMAIL", "sandbox@example.invalid");
    if !body.trim().is_empty() {
        commit.arg("-m").arg(body);
    }
    let output = commit.output().map_err(|error| SandboxError::Io {
        path: workspace_path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })?;
    if !output.status.success() {
        return Err(SandboxError::CommandFailed {
            command: format!("git -C {} commit", workspace_path.display()),
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    let commit_sha = git_stdout(workspace_path, &["rev-parse", "HEAD"])?;
    metadata.insert("committed".to_string(), JsonValue::from(true));
    metadata.insert("head_commit".to_string(), JsonValue::from(commit_sha));
    let remote = git_stdout_optional(workspace_path, &["remote", "get-url", "origin"])?;
    if remote.is_some() {
        run_git(workspace_path, &["push", "-u", "origin", head_branch])?;
        metadata.insert("pushed".to_string(), JsonValue::from(true));
        metadata.insert(
            "push_remote".to_string(),
            JsonValue::from(remote.unwrap_or_default()),
        );
    } else {
        metadata.insert("pushed".to_string(), JsonValue::from(false));
    }
    Ok(metadata)
}

fn is_git_workspace(path: &Path) -> bool {
    path.join(".git").exists()
        || matches!(
            git_stdout_optional(path, &["rev-parse", "--git-dir"]),
            Ok(Some(_))
        )
}

fn run_git(repo_root: &Path, args: &[&str]) -> Result<(), SandboxError> {
    let output = git_command(repo_root)
        .args(args)
        .output()
        .map_err(|error| SandboxError::Io {
            path: repo_root.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(SandboxError::CommandFailed {
        command: format!("git -C {} {}", repo_root.display(), args.join(" ")),
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn git_stdout(repo_root: &Path, args: &[&str]) -> Result<String, SandboxError> {
    let output = git_command(repo_root)
        .args(args)
        .output()
        .map_err(|error| SandboxError::Io {
            path: repo_root.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
    }
    Err(SandboxError::CommandFailed {
        command: format!("git -C {} {}", repo_root.display(), args.join(" ")),
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn git_stdout_optional(repo_root: &Path, args: &[&str]) -> Result<Option<String>, SandboxError> {
    match git_stdout(repo_root, args) {
        Ok(stdout) => Ok(Some(stdout)),
        Err(SandboxError::CommandFailed { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn sanitize_label(value: &str) -> String {
    let mut sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    while sanitized.contains("--") {
        sanitized = sanitized.replace("--", "-");
    }
    sanitized.trim_matches('-').to_string()
}

fn git_command(repo_root: &Path) -> Command {
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
