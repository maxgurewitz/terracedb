//! GitHub-specific remote-provider adapter for `terracedb-git`.
//!
//! The core `terracedb-git` crate intentionally exposes only a generic `GitRemoteProvider`
//! boundary. This crate is one concrete implementation of that contract and acts as a reference
//! shape for provider-specific adapters such as GitHub Enterprise or GitLab-backed variants that
//! want to plug into the same remote import / push / pull-request flow.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb_git::{
    GitCancellationToken, GitImportEntry, GitImportEntryKind, GitImportMode, GitImportReport,
    GitImportRequest, GitImportSource, GitIndexEntry, GitIndexSnapshot, GitObject, GitObjectFormat,
    GitObjectKind, GitPullRequestReport, GitPullRequestRequest, GitPushReport, GitPushRequest,
    GitRemoteProvider, GitRepository, GitRepositoryOrigin, GitSubstrateError,
};
use url::Url;

#[derive(Clone, Debug)]
/// Concrete `GitRemoteProvider` implementation for GitHub-style APIs.
pub struct GitHubRemoteProvider {
    name: Arc<str>,
    supported_hosts: BTreeSet<String>,
    api_base_url_override: Option<Arc<str>>,
}

impl GitHubRemoteProvider {
    pub fn new() -> Self {
        Self {
            name: Arc::from("github"),
            supported_hosts: BTreeSet::from(["github.com".to_string()]),
            api_base_url_override: None,
        }
    }

    pub fn with_supported_host(mut self, host: impl Into<String>) -> Self {
        self.supported_hosts.insert(host.into());
        self
    }

    pub fn with_api_base_url(mut self, api_base_url: impl Into<Arc<str>>) -> Self {
        self.api_base_url_override = Some(api_base_url.into());
        self
    }

    fn supports_remote_inner(
        &self,
        remote_url: &str,
        metadata: &BTreeMap<String, JsonValue>,
    ) -> bool {
        let Ok(parsed) = parse_github_remote(remote_url) else {
            return false;
        };
        metadata_provider_hint_matches(metadata, self.name())
            || self.supported_hosts.contains(&parsed.host)
    }

    fn locate_remote(
        &self,
        remote_url: &str,
        metadata: &BTreeMap<String, JsonValue>,
    ) -> Result<Option<GithubRemoteLocator>, GitSubstrateError> {
        let parsed = parse_github_remote(remote_url)?;
        if !metadata_provider_hint_matches(metadata, self.name())
            && !self.supported_hosts.contains(&parsed.host)
        {
            return Ok(None);
        }
        Ok(Some(GithubRemoteLocator {
            remote_url: remote_url.to_string(),
            api_base_url: self.api_base_url(&parsed),
            owner: parsed.owner,
            repo: parsed.repo,
            auth_token: metadata
                .get("auth_token")
                .and_then(JsonValue::as_str)
                .map(ToString::to_string),
        }))
    }

    fn api_base_url(&self, remote: &ParsedGithubRemote) -> String {
        if let Some(api_base_url) = self.api_base_url_override.as_ref() {
            return api_base_url.to_string();
        }
        if remote.host == "github.com" {
            return "https://api.github.com".to_string();
        }
        let scheme = match remote.transport_scheme.as_deref() {
            Some("http") | Some("https") => remote.transport_scheme.as_deref().unwrap_or("https"),
            _ => "https",
        };
        format!("{scheme}://{}{}", remote.authority, "/api/v3")
    }
}

impl Default for GitHubRemoteProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl GitRemoteProvider for GitHubRemoteProvider {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn supports_remote(&self, remote_url: &str, metadata: &BTreeMap<String, JsonValue>) -> bool {
        self.supports_remote_inner(remote_url, metadata)
    }

    async fn import_repository(
        &self,
        request: GitImportRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitImportReport, GitSubstrateError> {
        let GitImportSource::RemoteRepository {
            remote_url,
            reference,
        } = request.source.clone()
        else {
            return Err(GitSubstrateError::Bridge {
                operation: "import_repository",
                message: "github remote import requires a remote repository source".to_string(),
            });
        };
        if !matches!(request.mode, GitImportMode::Head) {
            return Err(GitSubstrateError::Bridge {
                operation: "import_repository",
                message: "github remote import only supports head snapshots".to_string(),
            });
        }
        let locator = self
            .locate_remote(&remote_url, &request.metadata)?
            .ok_or_else(|| GitSubstrateError::Bridge {
                operation: "import_repository",
                message: format!("github provider does not support {remote_url}"),
            })?;
        import_remote_github_repository(locator, request, reference, cancellation.as_ref()).await
    }

    async fn push(
        &self,
        repository: Arc<dyn GitRepository>,
        remote_url: String,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError> {
        let remote_name = request.remote.clone();
        let branch_name = request.branch_name.clone();
        let locator = self
            .locate_remote(&remote_url, &request.metadata)?
            .ok_or_else(|| GitSubstrateError::RemotePushFailed {
                remote: remote_name.clone(),
                branch: branch_name.clone(),
                message: format!("github provider does not support {remote_url}"),
            })?;
        push_remote_github_branch(
            repository.as_ref(),
            &request,
            &locator,
            cancellation.as_ref(),
        )
        .await
        .map(|pushed_oid| GitPushReport {
            remote: remote_name.clone(),
            branch_name: branch_name.clone(),
            pushed_oid: Some(pushed_oid),
            metadata: request.metadata,
        })
        .map_err(|error| GitSubstrateError::RemotePushFailed {
            remote: remote_name,
            branch: branch_name,
            message: provider_error_message(error),
        })
    }

    async fn create_pull_request(
        &self,
        remote_url: String,
        request: GitPullRequestRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPullRequestReport, GitSubstrateError> {
        let locator = self
            .locate_remote(&remote_url, &request.metadata)?
            .ok_or_else(|| GitSubstrateError::PullRequestProvider {
                provider: self.name().to_string(),
                message: format!("github provider does not support {remote_url}"),
            })?;
        create_github_pull_request(&request, &locator, cancellation.as_ref())
            .await
            .map_err(|error| GitSubstrateError::PullRequestProvider {
                provider: self.name().to_string(),
                message: provider_error_message(error),
            })
    }
}

#[derive(Clone, Debug)]
struct GithubRemoteLocator {
    remote_url: String,
    api_base_url: String,
    owner: String,
    repo: String,
    auth_token: Option<String>,
}

#[derive(Clone, Debug)]
struct ParsedGithubRemote {
    host: String,
    authority: String,
    owner: String,
    repo: String,
    transport_scheme: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct RemoteImportState {
    objects: BTreeMap<String, (GitObjectKind, Vec<u8>)>,
    worktree_entries: Vec<GitImportEntry>,
    index_entries: Vec<GitIndexEntry>,
}

async fn import_remote_github_repository(
    locator: GithubRemoteLocator,
    request: GitImportRequest,
    reference: Option<String>,
    cancellation: &dyn GitCancellationToken,
) -> Result<GitImportReport, GitSubstrateError> {
    ensure_not_cancelled(cancellation, "import_repository")?;
    let client = remote_http_client()?;
    let branch = match reference.as_deref() {
        Some(reference) => normalize_remote_branch_reference(reference),
        None => {
            let repository = github_get_repository(&client, &locator).await?;
            repository.default_branch
        }
    };
    let head = github_get_ref(&client, &locator, &branch)
        .await?
        .ok_or_else(|| GitSubstrateError::ReferenceNotFound {
            reference: format!("refs/heads/{branch}"),
        })?;
    let commit = github_get_commit(&client, &locator, &head).await?;
    let mut state = RemoteImportState::default();
    Box::pin(import_remote_github_tree(
        &client,
        &locator,
        &commit.tree.sha,
        "",
        &mut state,
        cancellation,
    ))
    .await?;
    state.objects.insert(
        head.clone(),
        (
            GitObjectKind::Commit,
            github_commit_payload(&commit.tree.sha, &commit.parents, &commit.message).into_bytes(),
        ),
    );
    let refs = vec![(format!("refs/heads/{branch}"), head.clone())];
    let index = GitIndexSnapshot {
        entries: state.index_entries,
        metadata: BTreeMap::new(),
    };
    let entries = import_repository_image_entries(
        &Some(branch.clone()),
        Some(&head),
        refs,
        index,
        state.objects,
        state.worktree_entries,
    )?;
    Ok(GitImportReport {
        source: request.source,
        target_root: request.target_root,
        repository_root: locator.remote_url.clone(),
        origin: GitRepositoryOrigin::RemoteImport,
        object_format: GitObjectFormat::Sha1,
        head_commit: Some(head),
        branch: Some(branch),
        remote_url: Some(locator.remote_url),
        pathspec: vec![".".to_string()],
        dirty: false,
        entries,
        metadata: request.metadata,
    })
}

async fn import_remote_github_tree(
    client: &Client,
    locator: &GithubRemoteLocator,
    tree_sha: &str,
    prefix: &str,
    state: &mut RemoteImportState,
    cancellation: &dyn GitCancellationToken,
) -> Result<(), GitSubstrateError> {
    ensure_not_cancelled(cancellation, "import_repository")?;
    let tree = github_get_tree(client, locator, tree_sha).await?;
    let mut tree_entries = tree.tree;
    tree_entries.sort_by(|left, right| left.path.cmp(&right.path));
    let mut payload = String::new();
    for entry in tree_entries {
        let entry_sha = entry.sha.clone().ok_or_else(|| GitSubstrateError::Bridge {
            operation: "import_repository",
            message: format!("tree entry {} is missing an object id", entry.path),
        })?;
        let mode = parse_git_mode(&entry.mode)?;
        let full_path = if prefix.is_empty() {
            entry.path.clone()
        } else {
            format!("{prefix}/{}", entry.path)
        };
        match entry.kind.as_str() {
            "blob" => {
                let blob = github_get_blob(client, locator, &entry_sha).await?;
                let bytes = github_blob_bytes(&blob)?;
                state
                    .objects
                    .insert(entry_sha.clone(), (GitObjectKind::Blob, bytes.clone()));
                if mode == 0o120000 {
                    let target = String::from_utf8(bytes.clone()).map_err(|_| {
                        GitSubstrateError::Bridge {
                            operation: "import_repository",
                            message: format!("symlink target for {full_path} is not valid UTF-8"),
                        }
                    })?;
                    state.worktree_entries.push(GitImportEntry {
                        path: full_path.clone(),
                        kind: GitImportEntryKind::Symlink,
                        data: None,
                        mode: Some(mode),
                        symlink_target: Some(target),
                    });
                } else {
                    state.worktree_entries.push(GitImportEntry {
                        path: full_path.clone(),
                        kind: GitImportEntryKind::File,
                        data: Some(bytes),
                        mode: Some(mode),
                        symlink_target: None,
                    });
                }
                state.index_entries.push(GitIndexEntry {
                    path: full_path,
                    oid: Some(entry_sha.clone()),
                    mode,
                });
                payload.push_str(&format!(
                    "{:06o} blob {}\t{}\n",
                    mode, entry_sha, entry.path
                ));
            }
            "tree" => {
                Box::pin(import_remote_github_tree(
                    client,
                    locator,
                    &entry_sha,
                    &full_path,
                    state,
                    cancellation,
                ))
                .await?;
                payload.push_str(&format!(
                    "{:06o} tree {}\t{}\n",
                    mode, entry_sha, entry.path
                ));
            }
            "commit" => {
                return Err(GitSubstrateError::Bridge {
                    operation: "import_repository",
                    message: format!(
                        "git submodules are not supported for remote import: {full_path}"
                    ),
                });
            }
            other => {
                return Err(GitSubstrateError::Bridge {
                    operation: "import_repository",
                    message: format!("unsupported remote tree entry kind: {other}"),
                });
            }
        }
    }
    state.objects.insert(
        tree_sha.to_string(),
        (GitObjectKind::Tree, payload.into_bytes()),
    );
    Ok(())
}

async fn push_remote_github_branch(
    repository: &dyn GitRepository,
    request: &GitPushRequest,
    locator: &GithubRemoteLocator,
    cancellation: &dyn GitCancellationToken,
) -> Result<String, GitSubstrateError> {
    ensure_not_cancelled(cancellation, "push")?;
    let client = remote_http_client()?;
    let local_head_oid = if let Some(oid) = request.head_oid.as_deref() {
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
    let local_commit = read_repository_object(repository, &local_head_oid).await?;
    let local_tree_oid = parse_commit_tree_oid_from_object(&local_commit)?;
    let message = parse_commit_message_from_object(&local_commit);
    let existing_branch = github_get_ref(&client, locator, &request.branch_name).await?;
    let parent_remote_sha = if let Some(existing) = existing_branch.as_ref() {
        Some(existing.clone())
    } else {
        let base_branch = request
            .metadata
            .get("base_branch")
            .and_then(JsonValue::as_str)
            .unwrap_or("main");
        github_get_ref(&client, locator, base_branch).await?
    };
    let mut uploaded = BTreeMap::new();
    let remote_tree_sha = Box::pin(upload_local_tree_to_github(
        &client,
        locator,
        repository,
        &local_tree_oid,
        &mut uploaded,
        cancellation,
    ))
    .await?;
    let remote_commit_sha = github_create_commit(
        &client,
        locator,
        &message,
        &remote_tree_sha,
        parent_remote_sha.as_deref(),
    )
    .await?;
    if existing_branch.is_some() {
        github_update_ref(&client, locator, &request.branch_name, &remote_commit_sha).await?;
    } else {
        github_create_ref(&client, locator, &request.branch_name, &remote_commit_sha).await?;
    }
    Ok(remote_commit_sha)
}

async fn upload_local_tree_to_github(
    client: &Client,
    locator: &GithubRemoteLocator,
    repository: &dyn GitRepository,
    local_tree_oid: &str,
    uploaded: &mut BTreeMap<String, String>,
    cancellation: &dyn GitCancellationToken,
) -> Result<String, GitSubstrateError> {
    if let Some(existing) = uploaded.get(local_tree_oid) {
        return Ok(existing.clone());
    }
    ensure_not_cancelled(cancellation, "push")?;
    let tree = read_repository_object(repository, local_tree_oid).await?;
    let entries = parse_tree_records_from_object(&tree)?;
    let mut payload = GithubCreateTreeRequest { tree: Vec::new() };
    for entry in entries {
        let remote_sha = match entry.kind {
            GitObjectKind::Blob => {
                upload_local_blob_to_github(
                    client,
                    locator,
                    repository,
                    &entry.oid,
                    uploaded,
                    cancellation,
                )
                .await?
            }
            GitObjectKind::Tree => {
                Box::pin(upload_local_tree_to_github(
                    client,
                    locator,
                    repository,
                    &entry.oid,
                    uploaded,
                    cancellation,
                ))
                .await?
            }
            other => {
                return Err(GitSubstrateError::Bridge {
                    operation: "push",
                    message: format!("unsupported tree entry kind for github push: {other:?}"),
                });
            }
        };
        payload.tree.push(GithubCreateTreeEntry {
            path: entry.path,
            mode: format!("{:06o}", entry.mode),
            kind: match entry.kind {
                GitObjectKind::Blob => "blob".to_string(),
                GitObjectKind::Tree => "tree".to_string(),
                GitObjectKind::Commit => "commit".to_string(),
                GitObjectKind::Tag | GitObjectKind::Unknown => "blob".to_string(),
            },
            sha: remote_sha,
        });
    }
    let response = github_create_tree(client, locator, &payload).await?;
    uploaded.insert(local_tree_oid.to_string(), response.sha.clone());
    Ok(response.sha)
}

async fn upload_local_blob_to_github(
    client: &Client,
    locator: &GithubRemoteLocator,
    repository: &dyn GitRepository,
    local_blob_oid: &str,
    uploaded: &mut BTreeMap<String, String>,
    cancellation: &dyn GitCancellationToken,
) -> Result<String, GitSubstrateError> {
    if let Some(existing) = uploaded.get(local_blob_oid) {
        return Ok(existing.clone());
    }
    ensure_not_cancelled(cancellation, "push")?;
    let blob = read_repository_object(repository, local_blob_oid).await?;
    if blob.kind != GitObjectKind::Blob {
        return Err(GitSubstrateError::InvalidObject {
            oid: local_blob_oid.to_string(),
            message: "expected blob object for remote upload".to_string(),
        });
    }
    let response = github_create_blob(
        client,
        locator,
        &GithubCreateBlobRequest {
            content: BASE64_STANDARD.encode(&blob.data),
            encoding: "base64".to_string(),
        },
    )
    .await?;
    uploaded.insert(local_blob_oid.to_string(), response.sha.clone());
    Ok(response.sha)
}

async fn create_github_pull_request(
    request: &GitPullRequestRequest,
    locator: &GithubRemoteLocator,
    cancellation: &dyn GitCancellationToken,
) -> Result<GitPullRequestReport, GitSubstrateError> {
    ensure_not_cancelled(cancellation, "create_pull_request")?;
    let client = remote_http_client()?;
    let response = github_create_pull(
        &client,
        locator,
        &GithubCreatePullRequest {
            title: request.title.clone(),
            head: request.head_branch.clone(),
            base: request.base_branch.clone(),
            body: request.body.clone(),
        },
    )
    .await?;
    Ok(GitPullRequestReport {
        url: response.html_url,
        head_branch: request.head_branch.clone(),
        base_branch: request.base_branch.clone(),
        metadata: request.metadata.clone(),
    })
}

async fn read_repository_object(
    repository: &dyn GitRepository,
    oid: &str,
) -> Result<GitObject, GitSubstrateError> {
    repository
        .read_object(oid)
        .await?
        .ok_or_else(|| GitSubstrateError::ObjectNotFound {
            oid: oid.to_string(),
        })
}

fn parse_commit_tree_oid_from_object(object: &GitObject) -> Result<String, GitSubstrateError> {
    String::from_utf8_lossy(&object.data)
        .lines()
        .find_map(|line| line.strip_prefix("tree ").map(str::to_string))
        .ok_or_else(|| GitSubstrateError::InvalidObject {
            oid: object.oid.clone(),
            message: "commit object is missing a tree header".to_string(),
        })
}

fn parse_commit_message_from_object(object: &GitObject) -> String {
    String::from_utf8_lossy(&object.data)
        .split_once("\n\n")
        .map(|(_, message)| message.trim().to_string())
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| "TerraceDB export".to_string())
}

#[derive(Clone, Debug)]
struct ParsedTreeRecord {
    path: String,
    oid: String,
    mode: u32,
    kind: GitObjectKind,
}

fn parse_tree_records_from_object(
    object: &GitObject,
) -> Result<Vec<ParsedTreeRecord>, GitSubstrateError> {
    let mut entries = Vec::new();
    for line in String::from_utf8_lossy(&object.data).lines() {
        if line.trim().is_empty() {
            continue;
        }
        let (header, path) =
            line.split_once('\t')
                .ok_or_else(|| GitSubstrateError::InvalidObject {
                    oid: object.oid.clone(),
                    message: format!("tree entry is missing a tab separator: {line}"),
                })?;
        let mut parts = header.split_whitespace();
        let mode = parts
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: object.oid.clone(),
                message: format!("tree entry is missing a mode: {line}"),
            })
            .and_then(parse_git_mode)?;
        let kind = match parts
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: object.oid.clone(),
                message: format!("tree entry is missing a kind: {line}"),
            })? {
            "blob" => GitObjectKind::Blob,
            "tree" => GitObjectKind::Tree,
            "commit" => GitObjectKind::Commit,
            "tag" => GitObjectKind::Tag,
            _ => GitObjectKind::Unknown,
        };
        let oid = parts
            .next()
            .ok_or_else(|| GitSubstrateError::InvalidObject {
                oid: object.oid.clone(),
                message: format!("tree entry is missing an oid: {line}"),
            })?
            .to_string();
        entries.push(ParsedTreeRecord {
            path: path.to_string(),
            oid,
            mode,
            kind,
        });
    }
    Ok(entries)
}

fn parse_git_mode(mode: &str) -> Result<u32, GitSubstrateError> {
    u32::from_str_radix(mode, 8).map_err(|_| GitSubstrateError::Bridge {
        operation: "github_remote",
        message: format!("invalid git mode: {mode}"),
    })
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

fn normalize_remote_branch_reference(reference: &str) -> String {
    reference
        .strip_prefix("refs/heads/")
        .unwrap_or(reference)
        .trim_matches('/')
        .to_string()
}

fn github_commit_payload(tree_sha: &str, parents: &[GithubObjectRef], message: &str) -> String {
    let mut payload = format!("tree {tree_sha}\n");
    for parent in parents {
        payload.push_str(&format!("parent {}\n", parent.sha));
    }
    payload.push('\n');
    payload.push_str(message.trim());
    payload.push('\n');
    payload
}

fn remote_http_client() -> Result<Client, GitSubstrateError> {
    Client::builder()
        .user_agent("terracedb-git-github/0.1")
        .build()
        .map_err(|error| GitSubstrateError::Bridge {
            operation: "github_remote",
            message: error.to_string(),
        })
}

fn provider_request(
    client: &Client,
    method: Method,
    url: &str,
    auth_token: Option<&str>,
) -> reqwest::RequestBuilder {
    let builder = client
        .request(method, url)
        .header("accept", "application/json");
    if let Some(token) = auth_token {
        builder.bearer_auth(token)
    } else {
        builder
    }
}

async fn send_provider_json<T: for<'de> Deserialize<'de>>(
    request: reqwest::RequestBuilder,
    operation: &'static str,
) -> Result<T, GitSubstrateError> {
    let response = request
        .send()
        .await
        .map_err(|error| GitSubstrateError::Bridge {
            operation,
            message: error.to_string(),
        })?;
    let status = response.status();
    if status.is_success() {
        return response
            .json::<T>()
            .await
            .map_err(|error| GitSubstrateError::Bridge {
                operation,
                message: error.to_string(),
            });
    }
    let body = response.text().await.unwrap_or_default();
    Err(GitSubstrateError::Bridge {
        operation,
        message: format!("http {}: {}", status.as_u16(), body.trim()),
    })
}

async fn send_provider_optional_json<T: for<'de> Deserialize<'de>>(
    request: reqwest::RequestBuilder,
    operation: &'static str,
) -> Result<Option<T>, GitSubstrateError> {
    let response = request
        .send()
        .await
        .map_err(|error| GitSubstrateError::Bridge {
            operation,
            message: error.to_string(),
        })?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let status = response.status();
    if status.is_success() {
        return response
            .json::<T>()
            .await
            .map(Some)
            .map_err(|error| GitSubstrateError::Bridge {
                operation,
                message: error.to_string(),
            });
    }
    let body = response.text().await.unwrap_or_default();
    Err(GitSubstrateError::Bridge {
        operation,
        message: format!("http {}: {}", status.as_u16(), body.trim()),
    })
}

async fn send_provider_empty(
    request: reqwest::RequestBuilder,
    operation: &'static str,
) -> Result<(), GitSubstrateError> {
    let response = request
        .send()
        .await
        .map_err(|error| GitSubstrateError::Bridge {
            operation,
            message: error.to_string(),
        })?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(GitSubstrateError::Bridge {
        operation,
        message: format!("http {}: {}", status.as_u16(), body.trim()),
    })
}

#[derive(Clone, Debug, Deserialize)]
struct GithubRepositoryResponse {
    default_branch: String,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubRefResponse {
    object: GithubObjectRef,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubObjectRef {
    sha: String,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubCommitResponse {
    message: String,
    tree: GithubObjectRef,
    parents: Vec<GithubObjectRef>,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubTreeResponse {
    #[serde(default)]
    truncated: bool,
    tree: Vec<GithubTreeEntry>,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubTreeEntry {
    path: String,
    mode: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    sha: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubBlobResponse {
    content: String,
    encoding: String,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubShaResponse {
    sha: String,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubPullResponse {
    html_url: String,
}

#[derive(Clone, Debug, Serialize)]
struct GithubCreateBlobRequest {
    content: String,
    encoding: String,
}

#[derive(Clone, Debug, Serialize)]
struct GithubCreateTreeRequest {
    tree: Vec<GithubCreateTreeEntry>,
}

#[derive(Clone, Debug, Serialize)]
struct GithubCreateTreeEntry {
    path: String,
    mode: String,
    #[serde(rename = "type")]
    kind: String,
    sha: String,
}

#[derive(Clone, Debug, Serialize)]
struct GithubCreateCommitRequest {
    message: String,
    tree: String,
    parents: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct GithubCreateRefRequest {
    r#ref: String,
    sha: String,
}

#[derive(Clone, Debug, Serialize)]
struct GithubUpdateRefRequest {
    sha: String,
    force: bool,
}

#[derive(Clone, Debug, Serialize)]
struct GithubCreatePullRequest {
    title: String,
    head: String,
    base: String,
    body: String,
}

async fn github_get_repository(
    client: &Client,
    locator: &GithubRemoteLocator,
) -> Result<GithubRepositoryResponse, GitSubstrateError> {
    send_provider_json(
        provider_request(
            client,
            Method::GET,
            &format!(
                "{}/repos/{}/{}",
                locator.api_base_url, locator.owner, locator.repo
            ),
            locator.auth_token.as_deref(),
        ),
        "import_repository",
    )
    .await
}

async fn github_get_ref(
    client: &Client,
    locator: &GithubRemoteLocator,
    branch: &str,
) -> Result<Option<String>, GitSubstrateError> {
    Ok(send_provider_optional_json::<GithubRefResponse>(
        provider_request(
            client,
            Method::GET,
            &format!(
                "{}/repos/{}/{}/git/ref/heads/{}",
                locator.api_base_url, locator.owner, locator.repo, branch
            ),
            locator.auth_token.as_deref(),
        ),
        "github_remote",
    )
    .await?
    .map(|response| response.object.sha))
}

async fn github_get_commit(
    client: &Client,
    locator: &GithubRemoteLocator,
    sha: &str,
) -> Result<GithubCommitResponse, GitSubstrateError> {
    send_provider_json(
        provider_request(
            client,
            Method::GET,
            &format!(
                "{}/repos/{}/{}/git/commits/{}",
                locator.api_base_url, locator.owner, locator.repo, sha
            ),
            locator.auth_token.as_deref(),
        ),
        "import_repository",
    )
    .await
}

async fn github_get_tree(
    client: &Client,
    locator: &GithubRemoteLocator,
    sha: &str,
) -> Result<GithubTreeResponse, GitSubstrateError> {
    let response: GithubTreeResponse = send_provider_json(
        provider_request(
            client,
            Method::GET,
            &format!(
                "{}/repos/{}/{}/git/trees/{}",
                locator.api_base_url, locator.owner, locator.repo, sha
            ),
            locator.auth_token.as_deref(),
        ),
        "import_repository",
    )
    .await?;
    if response.truncated {
        return Err(GitSubstrateError::Bridge {
            operation: "import_repository",
            message: format!("remote tree {sha} was truncated by the provider"),
        });
    }
    Ok(response)
}

async fn github_get_blob(
    client: &Client,
    locator: &GithubRemoteLocator,
    sha: &str,
) -> Result<GithubBlobResponse, GitSubstrateError> {
    send_provider_json(
        provider_request(
            client,
            Method::GET,
            &format!(
                "{}/repos/{}/{}/git/blobs/{}",
                locator.api_base_url, locator.owner, locator.repo, sha
            ),
            locator.auth_token.as_deref(),
        ),
        "import_repository",
    )
    .await
}

fn github_blob_bytes(blob: &GithubBlobResponse) -> Result<Vec<u8>, GitSubstrateError> {
    match blob.encoding.as_str() {
        "base64" => BASE64_STANDARD
            .decode(blob.content.replace('\n', ""))
            .map_err(|error| GitSubstrateError::Bridge {
                operation: "import_repository",
                message: error.to_string(),
            }),
        "utf-8" | "utf8" => Ok(blob.content.clone().into_bytes()),
        other => Err(GitSubstrateError::Bridge {
            operation: "import_repository",
            message: format!("unsupported github blob encoding: {other}"),
        }),
    }
}

async fn github_create_blob(
    client: &Client,
    locator: &GithubRemoteLocator,
    request: &GithubCreateBlobRequest,
) -> Result<GithubShaResponse, GitSubstrateError> {
    send_provider_json(
        provider_request(
            client,
            Method::POST,
            &format!(
                "{}/repos/{}/{}/git/blobs",
                locator.api_base_url, locator.owner, locator.repo
            ),
            locator.auth_token.as_deref(),
        )
        .json(request),
        "push",
    )
    .await
}

async fn github_create_tree(
    client: &Client,
    locator: &GithubRemoteLocator,
    request: &GithubCreateTreeRequest,
) -> Result<GithubShaResponse, GitSubstrateError> {
    send_provider_json(
        provider_request(
            client,
            Method::POST,
            &format!(
                "{}/repos/{}/{}/git/trees",
                locator.api_base_url, locator.owner, locator.repo
            ),
            locator.auth_token.as_deref(),
        )
        .json(request),
        "push",
    )
    .await
}

async fn github_create_commit(
    client: &Client,
    locator: &GithubRemoteLocator,
    message: &str,
    tree_sha: &str,
    parent_sha: Option<&str>,
) -> Result<String, GitSubstrateError> {
    Ok(send_provider_json::<GithubShaResponse>(
        provider_request(
            client,
            Method::POST,
            &format!(
                "{}/repos/{}/{}/git/commits",
                locator.api_base_url, locator.owner, locator.repo
            ),
            locator.auth_token.as_deref(),
        )
        .json(&GithubCreateCommitRequest {
            message: message.to_string(),
            tree: tree_sha.to_string(),
            parents: parent_sha.into_iter().map(ToString::to_string).collect(),
        }),
        "push",
    )
    .await?
    .sha)
}

async fn github_create_ref(
    client: &Client,
    locator: &GithubRemoteLocator,
    branch: &str,
    sha: &str,
) -> Result<(), GitSubstrateError> {
    send_provider_empty(
        provider_request(
            client,
            Method::POST,
            &format!(
                "{}/repos/{}/{}/git/refs",
                locator.api_base_url, locator.owner, locator.repo
            ),
            locator.auth_token.as_deref(),
        )
        .json(&GithubCreateRefRequest {
            r#ref: format!("refs/heads/{branch}"),
            sha: sha.to_string(),
        }),
        "push",
    )
    .await
}

async fn github_update_ref(
    client: &Client,
    locator: &GithubRemoteLocator,
    branch: &str,
    sha: &str,
) -> Result<(), GitSubstrateError> {
    send_provider_empty(
        provider_request(
            client,
            Method::PATCH,
            &format!(
                "{}/repos/{}/{}/git/refs/heads/{}",
                locator.api_base_url, locator.owner, locator.repo, branch
            ),
            locator.auth_token.as_deref(),
        )
        .json(&GithubUpdateRefRequest {
            sha: sha.to_string(),
            force: false,
        }),
        "push",
    )
    .await
}

async fn github_create_pull(
    client: &Client,
    locator: &GithubRemoteLocator,
    request: &GithubCreatePullRequest,
) -> Result<GithubPullResponse, GitSubstrateError> {
    send_provider_json(
        provider_request(
            client,
            Method::POST,
            &format!(
                "{}/repos/{}/{}/pulls",
                locator.api_base_url, locator.owner, locator.repo
            ),
            locator.auth_token.as_deref(),
        )
        .json(request),
        "create_pull_request",
    )
    .await
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

fn metadata_provider_hint_matches(
    metadata: &BTreeMap<String, JsonValue>,
    provider_name: &str,
) -> bool {
    metadata
        .get("provider")
        .and_then(JsonValue::as_str)
        .map(|provider| provider.eq_ignore_ascii_case(provider_name))
        .unwrap_or(false)
}

fn parse_github_remote(remote_url: &str) -> Result<ParsedGithubRemote, GitSubstrateError> {
    if let Ok(url) = Url::parse(remote_url) {
        let host = url.host_str().ok_or_else(|| GitSubstrateError::Bridge {
            operation: "github_remote",
            message: format!("github remote url is missing a host: {remote_url}"),
        })?;
        let segments = repository_path_segments(&url);
        if segments.len() < 2 {
            return Err(GitSubstrateError::Bridge {
                operation: "github_remote",
                message: format!("github remote url is missing owner/repo: {remote_url}"),
            });
        }
        return Ok(ParsedGithubRemote {
            host: host.to_string(),
            authority: url_authority(&url),
            owner: segments[0].clone(),
            repo: trim_git_suffix(&segments[1]).to_string(),
            transport_scheme: Some(url.scheme().to_string()),
        });
    }

    parse_scp_like_github_remote(remote_url).ok_or_else(|| GitSubstrateError::Bridge {
        operation: "github_remote",
        message: format!("invalid github remote url {remote_url}"),
    })
}

fn parse_scp_like_github_remote(remote_url: &str) -> Option<ParsedGithubRemote> {
    if remote_url.contains("://") {
        return None;
    }
    let (host_part, path) = remote_url.split_once(':')?;
    let host = host_part.rsplit('@').next()?.trim();
    if host.is_empty() {
        return None;
    }
    let segments = path
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() < 2 {
        return None;
    }
    Some(ParsedGithubRemote {
        host: host.to_string(),
        authority: host.to_string(),
        owner: segments[0].to_string(),
        repo: trim_git_suffix(segments[1]).to_string(),
        transport_scheme: None,
    })
}

fn repository_path_segments(url: &Url) -> Vec<String> {
    url.path_segments()
        .map(|segments| {
            segments
                .filter(|segment| !segment.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn trim_git_suffix(repo: &str) -> &str {
    repo.strip_suffix(".git").unwrap_or(repo)
}

fn url_authority(url: &Url) -> String {
    match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().to_string(),
    }
}

fn ancestor_relatives(path: &str) -> Vec<String> {
    let parts = path.split('/').collect::<Vec<_>>();
    let mut ancestors = Vec::new();
    for index in 1..parts.len() {
        ancestors.push(parts[..index].join("/"));
    }
    ancestors
}

fn provider_error_message(error: GitSubstrateError) -> String {
    match error {
        GitSubstrateError::RemotePushFailed { message, .. }
        | GitSubstrateError::PullRequestProvider { message, .. }
        | GitSubstrateError::Bridge { message, .. }
        | GitSubstrateError::InvalidObject { message, .. }
        | GitSubstrateError::ExportConflict { message, .. } => message,
        other => other.to_string(),
    }
}
