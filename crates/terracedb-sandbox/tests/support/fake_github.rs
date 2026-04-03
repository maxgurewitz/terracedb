use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    routing::{get, patch, post},
};
use serde::{Deserialize, Serialize};
use terracedb_git::HostGitBridge;
use terracedb_git_github::GitHubRemoteProvider;
use tokio::{net::TcpListener, sync::Mutex as AsyncMutex};

pub fn configured_remote_github_bridge(host: &str) -> Arc<HostGitBridge> {
    Arc::new(
        HostGitBridge::new("host-git").with_remote_provider(Arc::new(
            GitHubRemoteProvider::new().with_supported_host(host),
        )),
    )
}

#[derive(Clone)]
pub struct FakeGithubServer {
    pub remote_url: String,
    state: Arc<AsyncMutex<FakeGithubState>>,
}

impl FakeGithubServer {
    pub async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake github server");
        let address = listener.local_addr().expect("fake github address");
        let base_url = format!("http://{}", address);
        let remote_url = format!("{}/octo/demo.git", base_url);
        let state = Arc::new(AsyncMutex::new(FakeGithubState::seeded(base_url.clone())));
        let app = Router::new()
            .route("/api/v3/repos/{owner}/{repo}", get(fake_github_repository))
            .route(
                "/api/v3/repos/{owner}/{repo}/git/ref/heads/{*branch}",
                get(fake_github_get_ref),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/git/commits/{sha}",
                get(fake_github_get_commit),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/git/trees/{sha}",
                get(fake_github_get_tree),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/git/blobs/{sha}",
                get(fake_github_get_blob),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/git/blobs",
                post(fake_github_create_blob),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/git/trees",
                post(fake_github_create_tree),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/git/commits",
                post(fake_github_create_commit),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/git/refs",
                post(fake_github_create_ref),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/git/refs/heads/{*branch}",
                patch(fake_github_update_ref),
            )
            .route(
                "/api/v3/repos/{owner}/{repo}/pulls",
                post(fake_github_create_pull),
            )
            .with_state(state.clone());
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve fake github api");
        });
        Self { remote_url, state }
    }

    pub async fn branch_file_text(&self, branch: &str, path: &str) -> Option<String> {
        self.state
            .lock()
            .await
            .branch_file_bytes(branch, path)
            .map(|bytes| String::from_utf8(bytes).expect("fake github blob is utf-8"))
    }
}

#[derive(Clone, Debug)]
struct FakeGithubCommit {
    message: String,
    tree: String,
    parents: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FakeGithubTreeEntry {
    path: String,
    mode: String,
    #[serde(rename = "type")]
    kind: String,
    sha: String,
}

#[derive(Clone, Debug)]
struct FakeGithubState {
    base_url: String,
    next_sha: u64,
    default_branch: String,
    refs: BTreeMap<String, String>,
    blobs: BTreeMap<String, Vec<u8>>,
    trees: BTreeMap<String, Vec<FakeGithubTreeEntry>>,
    commits: BTreeMap<String, FakeGithubCommit>,
}

impl FakeGithubState {
    fn seeded(base_url: String) -> Self {
        let mut state = Self {
            base_url,
            next_sha: 1,
            default_branch: "main".to_string(),
            refs: BTreeMap::new(),
            blobs: BTreeMap::new(),
            trees: BTreeMap::new(),
            commits: BTreeMap::new(),
        };
        let blob_sha = state.alloc_sha();
        state
            .blobs
            .insert(blob_sha.clone(), b"remote base\n".to_vec());
        let tree_sha = state.alloc_sha();
        state.trees.insert(
            tree_sha.clone(),
            vec![FakeGithubTreeEntry {
                path: "README.md".to_string(),
                mode: "100644".to_string(),
                kind: "blob".to_string(),
                sha: blob_sha,
            }],
        );
        let commit_sha = state.alloc_sha();
        state.commits.insert(
            commit_sha.clone(),
            FakeGithubCommit {
                message: "Remote base".to_string(),
                tree: tree_sha,
                parents: Vec::new(),
            },
        );
        state.refs.insert("main".to_string(), commit_sha);
        state
    }

    fn alloc_sha(&mut self) -> String {
        let next = self.next_sha;
        self.next_sha = self.next_sha.saturating_add(1);
        format!("{next:040x}")
    }

    fn branch_file_bytes(&self, branch: &str, path: &str) -> Option<Vec<u8>> {
        let commit_sha = self.refs.get(branch)?;
        let commit = self.commits.get(commit_sha)?;
        self.tree_file_bytes(&commit.tree, path)
    }

    fn tree_file_bytes(&self, tree_sha: &str, path: &str) -> Option<Vec<u8>> {
        let entries = self.trees.get(tree_sha)?;
        if let Some((head, tail)) = path.split_once('/') {
            let subtree = entries
                .iter()
                .find(|entry| entry.kind == "tree" && entry.path == head)?;
            return self.tree_file_bytes(&subtree.sha, tail);
        }
        let blob = entries
            .iter()
            .find(|entry| entry.kind == "blob" && entry.path == path)?;
        self.blobs.get(&blob.sha).cloned()
    }
}

#[derive(Serialize)]
struct FakeGithubRepositoryResponse {
    default_branch: String,
}

#[derive(Serialize)]
struct FakeGithubRefObject {
    sha: String,
}

#[derive(Serialize)]
struct FakeGithubRefResponse {
    object: FakeGithubRefObject,
}

#[derive(Serialize)]
struct FakeGithubCommitResponse {
    message: String,
    tree: FakeGithubRefObject,
    parents: Vec<FakeGithubRefObject>,
}

#[derive(Serialize)]
struct FakeGithubTreeResponse {
    truncated: bool,
    tree: Vec<FakeGithubTreeEntry>,
}

#[derive(Serialize)]
struct FakeGithubBlobResponse {
    content: String,
    encoding: String,
}

#[derive(Serialize)]
struct FakeGithubShaResponse {
    sha: String,
}

#[derive(Serialize)]
struct FakeGithubPullResponse {
    html_url: String,
}

#[derive(Deserialize)]
struct FakeGithubCreateBlobRequest {
    content: String,
    encoding: String,
}

#[derive(Deserialize)]
struct FakeGithubCreateTreeRequest {
    tree: Vec<FakeGithubTreeEntry>,
}

#[derive(Deserialize)]
struct FakeGithubCreateCommitRequest {
    message: String,
    tree: String,
    parents: Vec<String>,
}

#[derive(Deserialize)]
struct FakeGithubCreateRefRequest {
    r#ref: String,
    sha: String,
}

#[derive(Deserialize)]
struct FakeGithubUpdateRefRequest {
    sha: String,
}

#[derive(Deserialize)]
struct FakeGithubCreatePullRequest {
    title: String,
    head: String,
    base: String,
    body: String,
}

async fn fake_github_repository(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo)): AxumPath<(String, String)>,
) -> Json<FakeGithubRepositoryResponse> {
    let state = state.lock().await;
    Json(FakeGithubRepositoryResponse {
        default_branch: state.default_branch.clone(),
    })
}

async fn fake_github_get_ref(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo, branch)): AxumPath<(String, String, String)>,
) -> Result<Json<FakeGithubRefResponse>, StatusCode> {
    let state = state.lock().await;
    let Some(sha) = state.refs.get(branch.trim_start_matches('/')).cloned() else {
        return Err(StatusCode::NOT_FOUND);
    };
    Ok(Json(FakeGithubRefResponse {
        object: FakeGithubRefObject { sha },
    }))
}

async fn fake_github_get_commit(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo, sha)): AxumPath<(String, String, String)>,
) -> Result<Json<FakeGithubCommitResponse>, StatusCode> {
    let state = state.lock().await;
    let Some(commit) = state.commits.get(&sha).cloned() else {
        return Err(StatusCode::NOT_FOUND);
    };
    Ok(Json(FakeGithubCommitResponse {
        message: commit.message,
        tree: FakeGithubRefObject { sha: commit.tree },
        parents: commit
            .parents
            .into_iter()
            .map(|sha| FakeGithubRefObject { sha })
            .collect(),
    }))
}

async fn fake_github_get_tree(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo, sha)): AxumPath<(String, String, String)>,
) -> Result<Json<FakeGithubTreeResponse>, StatusCode> {
    let state = state.lock().await;
    let Some(tree) = state.trees.get(&sha).cloned() else {
        return Err(StatusCode::NOT_FOUND);
    };
    Ok(Json(FakeGithubTreeResponse {
        truncated: false,
        tree,
    }))
}

async fn fake_github_get_blob(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo, sha)): AxumPath<(String, String, String)>,
) -> Result<Json<FakeGithubBlobResponse>, StatusCode> {
    let state = state.lock().await;
    let Some(blob) = state.blobs.get(&sha).cloned() else {
        return Err(StatusCode::NOT_FOUND);
    };
    Ok(Json(FakeGithubBlobResponse {
        content: {
            use base64::{Engine as _, engine::general_purpose::STANDARD};
            STANDARD.encode(blob)
        },
        encoding: "base64".to_string(),
    }))
}

async fn fake_github_create_blob(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo)): AxumPath<(String, String)>,
    Json(request): Json<FakeGithubCreateBlobRequest>,
) -> Result<Json<FakeGithubShaResponse>, StatusCode> {
    let bytes = match request.encoding.as_str() {
        "base64" => {
            use base64::{Engine as _, engine::general_purpose::STANDARD};
            STANDARD
                .decode(request.content.as_bytes())
                .map_err(|_| StatusCode::BAD_REQUEST)?
        }
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let mut state = state.lock().await;
    let sha = state.alloc_sha();
    state.blobs.insert(sha.clone(), bytes);
    Ok(Json(FakeGithubShaResponse { sha }))
}

async fn fake_github_create_tree(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo)): AxumPath<(String, String)>,
    Json(request): Json<FakeGithubCreateTreeRequest>,
) -> Json<FakeGithubShaResponse> {
    let mut state = state.lock().await;
    let sha = state.alloc_sha();
    state.trees.insert(sha.clone(), request.tree);
    Json(FakeGithubShaResponse { sha })
}

async fn fake_github_create_commit(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo)): AxumPath<(String, String)>,
    Json(request): Json<FakeGithubCreateCommitRequest>,
) -> Json<FakeGithubShaResponse> {
    let mut state = state.lock().await;
    let sha = state.alloc_sha();
    state.commits.insert(
        sha.clone(),
        FakeGithubCommit {
            message: request.message,
            tree: request.tree,
            parents: request.parents,
        },
    );
    Json(FakeGithubShaResponse { sha })
}

async fn fake_github_create_ref(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo)): AxumPath<(String, String)>,
    Json(request): Json<FakeGithubCreateRefRequest>,
) -> Json<FakeGithubShaResponse> {
    let mut state = state.lock().await;
    state.refs.insert(
        request.r#ref.trim_start_matches("refs/heads/").to_string(),
        request.sha.clone(),
    );
    Json(FakeGithubShaResponse { sha: request.sha })
}

async fn fake_github_update_ref(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo, branch)): AxumPath<(String, String, String)>,
    Json(request): Json<FakeGithubUpdateRefRequest>,
) -> StatusCode {
    let mut state = state.lock().await;
    state
        .refs
        .insert(branch.trim_start_matches('/').to_string(), request.sha);
    StatusCode::OK
}

async fn fake_github_create_pull(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((owner, repo)): AxumPath<(String, String)>,
    Json(request): Json<FakeGithubCreatePullRequest>,
) -> Json<FakeGithubPullResponse> {
    let state = state.lock().await;
    let _ = (request.title, request.head, request.base, request.body);
    Json(FakeGithubPullResponse {
        html_url: format!("{}/{owner}/{repo}/pull/1", state.base_url),
    })
}
