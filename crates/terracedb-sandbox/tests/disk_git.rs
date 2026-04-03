use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    routing::{get, patch, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_git::HostGitBridge;
use terracedb_git_github::GitHubRemoteProvider;
use terracedb_sandbox::{
    CapabilityManifest, CloseSessionOptions, ConflictPolicy, DefaultSandboxStore,
    DeterministicPackageInstaller, DeterministicPullRequestProviderClient,
    DeterministicReadonlyViewProvider, DeterministicRuntimeBackend, EjectMode, EjectRequest,
    GitRemoteImportRequest, HoistMode, HoistRequest, PullRequestProviderClient, PullRequestReport,
    PullRequestRequest, SANDBOX_GIT_LIBRARY_SPECIFIER, SandboxCapability, SandboxConfig,
    SandboxError, SandboxServices, SandboxStore,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};
use tokio::{net::TcpListener, sync::Mutex as AsyncMutex};

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_test_dir(name: &str) -> PathBuf {
    let id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "terracedb-sandbox-{name}-{}-{id}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn sandbox_store(
    now: u64,
    seed: u64,
    services: SandboxServices,
) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(Arc::new(vfs.clone()), dependencies.clock, services);
    (vfs, sandbox)
}

async fn create_empty_base(store: &InMemoryVfsStore, volume_id: VolumeId) {
    store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
}

fn deterministic_services() -> SandboxServices {
    SandboxServices::deterministic()
}

fn configured_host_git_bridge() -> Arc<HostGitBridge> {
    Arc::new(HostGitBridge::new("host-git"))
}

fn configured_remote_github_bridge(host: &str) -> Arc<HostGitBridge> {
    Arc::new(
        HostGitBridge::new("host-git").with_remote_provider(Arc::new(
            GitHubRemoteProvider::new().with_supported_host(host),
        )),
    )
}

fn configured_remote_github_bridge_with_api_base(
    host: &str,
    api_base_url: impl Into<String>,
) -> Arc<HostGitBridge> {
    Arc::new(
        HostGitBridge::new("host-git").with_remote_provider(Arc::new(
            GitHubRemoteProvider::new()
                .with_supported_host(host)
                .with_api_base_url(api_base_url.into()),
        )),
    )
}

fn host_git_services() -> SandboxServices {
    host_git_services_with_provider(Arc::new(DeterministicPullRequestProviderClient::default()))
}

fn host_git_services_with_provider(
    provider: Arc<dyn PullRequestProviderClient>,
) -> SandboxServices {
    let bridge = configured_host_git_bridge();
    SandboxServices::new(
        Arc::new(DeterministicRuntimeBackend::default()),
        Arc::new(DeterministicPackageInstaller::default()),
        Arc::new(terracedb_sandbox::DeterministicGitRepositoryStore::default()),
        provider,
        Arc::new(DeterministicReadonlyViewProvider::default()),
    )
    .with_git_host_bridge(bridge)
}

fn remote_github_services(host: &str) -> SandboxServices {
    remote_github_services_with_provider(
        host,
        Arc::new(DeterministicPullRequestProviderClient::default()),
    )
}

fn remote_github_services_with_provider(
    host: &str,
    provider: Arc<dyn PullRequestProviderClient>,
) -> SandboxServices {
    let bridge = configured_remote_github_bridge(host);
    SandboxServices::new(
        Arc::new(DeterministicRuntimeBackend::default()),
        Arc::new(DeterministicPackageInstaller::default()),
        Arc::new(terracedb_sandbox::DeterministicGitRepositoryStore::default()),
        provider,
        Arc::new(DeterministicReadonlyViewProvider::default()),
    )
    .with_git_host_bridge(bridge)
}

#[derive(Clone, Debug)]
struct FailingPullRequestProvider;

#[async_trait]
impl PullRequestProviderClient for FailingPullRequestProvider {
    fn name(&self) -> &str {
        "failing-pr-provider"
    }

    async fn create_pull_request(
        &self,
        _session: &terracedb_sandbox::SandboxSession,
        _request: PullRequestRequest,
    ) -> Result<PullRequestReport, SandboxError> {
        Err(SandboxError::Service {
            service: "pull-request",
            message: "legacy provider should not be called".to_string(),
        })
    }
}

#[derive(Clone)]
struct FakeGithubServer {
    remote_url: String,
    state: Arc<AsyncMutex<FakeGithubState>>,
}

impl FakeGithubServer {
    async fn spawn() -> Self {
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

    async fn branch_file_text(&self, branch: &str, path: &str) -> Option<String> {
        self.state
            .lock()
            .await
            .branch_file_bytes(branch, path)
            .map(|bytes| String::from_utf8(bytes).expect("fake github blob is utf-8"))
    }

    async fn branch_entry_mode(&self, branch: &str, path: &str) -> Option<String> {
        self.state.lock().await.branch_entry_mode(branch, path)
    }

    async fn pulls(&self) -> Vec<FakeGithubPull> {
        self.state.lock().await.pulls.clone()
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct FakeGithubPull {
    title: String,
    body: String,
    head: String,
    base: String,
    html_url: String,
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
    pulls: Vec<FakeGithubPull>,
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
            pulls: Vec::new(),
        };
        let blob_sha = state.alloc_sha();
        state
            .blobs
            .insert(blob_sha.clone(), b"remote base\n".to_vec());
        let tree_sha = state.alloc_sha();
        state.trees.insert(
            tree_sha.clone(),
            vec![FakeGithubTreeEntry {
                path: "tracked.txt".to_string(),
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

    fn branch_entry_mode(&self, branch: &str, path: &str) -> Option<String> {
        let commit_sha = self.refs.get(branch)?;
        let commit = self.commits.get(commit_sha)?;
        self.tree_entry_mode(&commit.tree, path)
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

    fn tree_entry_mode(&self, tree_sha: &str, path: &str) -> Option<String> {
        let entries = self.trees.get(tree_sha)?;
        if let Some((head, tail)) = path.split_once('/') {
            let subtree = entries
                .iter()
                .find(|entry| entry.kind == "tree" && entry.path == head)?;
            return self.tree_entry_mode(&subtree.sha, tail);
        }
        entries
            .iter()
            .find(|entry| entry.path == path)
            .map(|entry| entry.mode.clone())
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
    force: bool,
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
    let branch = branch.trim_start_matches('/').to_string();
    let state = state.lock().await;
    let Some(sha) = state.refs.get(&branch).cloned() else {
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
        "utf-8" | "utf8" => request.content.into_bytes(),
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
    let branch = request.r#ref.trim_start_matches("refs/heads/").to_string();
    state.refs.insert(branch, request.sha.clone());
    Json(FakeGithubShaResponse { sha: request.sha })
}

async fn fake_github_update_ref(
    State(state): State<Arc<AsyncMutex<FakeGithubState>>>,
    AxumPath((_owner, _repo, branch)): AxumPath<(String, String, String)>,
    Json(request): Json<FakeGithubUpdateRefRequest>,
) -> StatusCode {
    let mut state = state.lock().await;
    let _ = request.force;
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
    let mut state = state.lock().await;
    let pull_number = state.pulls.len() + 1;
    let html_url = format!("{}/{owner}/{repo}/pull/{pull_number}", state.base_url);
    state.pulls.push(FakeGithubPull {
        title: request.title,
        body: request.body,
        head: request.head,
        base: request.base,
        html_url: html_url.clone(),
    });
    Json(FakeGithubPullResponse { html_url })
}

fn write_host_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent directories");
    }
    fs::write(path, contents).expect("write host file");
}

fn write_executable_host_file(path: &Path, contents: &str) {
    write_host_file(path, contents);
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(path)
            .expect("read executable metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("set executable bit");
    }
}

fn read_host_file(path: &Path) -> String {
    fs::read_to_string(path).expect("read host file")
}

fn sanitized_git_command() -> Command {
    let mut command = Command::new("git");
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

fn git(dir: &Path, args: &[&str]) {
    let output = sanitized_git_command()
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git -C {} {} failed: {}",
        dir.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_git_repo(repo: &Path, remote: Option<&Path>) {
    fs::create_dir_all(repo).expect("create repo dir");
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.name", "Sandbox Tester"]);
    git(repo, &["config", "user.email", "sandbox@example.invalid"]);
    write_host_file(&repo.join("tracked.txt"), "tracked\n");
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "initial"]);
    if let Some(remote) = remote {
        fs::create_dir_all(remote).expect("create remote parent");
        let output = sanitized_git_command()
            .arg("init")
            .arg("--bare")
            .arg(remote)
            .output()
            .expect("init bare remote");
        assert!(
            output.status.success(),
            "git init --bare failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        git(
            repo,
            &["remote", "add", "origin", &remote.to_string_lossy()],
        );
        git(repo, &["push", "-u", "origin", "main"]);
    }
}

fn set_git_remote(repo: &Path, remote_url: &str) {
    let remove = sanitized_git_command()
        .arg("-C")
        .arg(repo)
        .args(["remote", "remove", "origin"])
        .output()
        .expect("remove existing origin");
    assert!(
        remove.status.success()
            || String::from_utf8_lossy(&remove.stderr).contains("No such remote"),
        "remove origin failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );
    git(repo, &["remote", "add", "origin", remote_url]);
}

#[tokio::test]
async fn directory_hoist_round_trip_applies_delta_to_host_directory() {
    let source = unique_test_dir("round-trip-src");
    let target = unique_test_dir("round-trip-target");
    write_host_file(&source.join("tracked.txt"), "hello\n");
    write_host_file(&source.join("dir/keep.txt"), "keep\n");
    fs::create_dir_all(&target).expect("create target dir");
    write_host_file(&target.join("tracked.txt"), "hello\n");
    write_host_file(&target.join("dir/keep.txt"), "keep\n");

    let (vfs, sandbox) = sandbox_store(100, 501, deterministic_services());
    let base_volume_id = VolumeId::new(0x9000);
    let session_volume_id = VolumeId::new(0x9001);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_capabilities(CapabilityManifest {
                    capabilities: vec![SandboxCapability::git_remote_import()],
                }),
        )
        .await
        .expect("open session");
    let hoist = session
        .hoist_from_disk(HoistRequest {
            source_path: source.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::DirectorySnapshot,
            delete_missing: true,
        })
        .await
        .expect("hoist directory");
    assert_eq!(hoist.hoisted_paths, 3);

    let fs = session.filesystem();
    fs.write_file(
        "/workspace/tracked.txt",
        b"updated\n".to_vec(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("update tracked file");
    fs.rename("/workspace/dir/keep.txt", "/workspace/dir/renamed.txt")
        .await
        .expect("rename tracked file");
    fs.write_file(
        "/workspace/new.txt",
        b"new\n".to_vec(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("create new file");

    let eject = session
        .eject_to_disk(EjectRequest {
            target_path: target.to_string_lossy().into_owned(),
            mode: EjectMode::ApplyDelta,
            conflict_policy: ConflictPolicy::Fail,
        })
        .await
        .expect("eject delta");
    assert!(eject.conflicts.is_empty());
    assert_eq!(read_host_file(&target.join("tracked.txt")), "updated\n");
    assert!(!target.join("dir/keep.txt").exists());
    assert_eq!(read_host_file(&target.join("dir/renamed.txt")), "keep\n");
    assert_eq!(read_host_file(&target.join("new.txt")), "new\n");

    session
        .close(CloseSessionOptions::default())
        .await
        .expect("close session");
    cleanup(&source);
    cleanup(&target);
}

#[tokio::test]
async fn eject_conflicts_can_fall_back_to_patch_bundle() {
    let source = unique_test_dir("patch-src");
    let target = unique_test_dir("patch-target");
    write_host_file(&source.join("tracked.txt"), "one\n");
    fs::create_dir_all(&target).expect("create target dir");
    write_host_file(&target.join("tracked.txt"), "external\n");

    let (vfs, sandbox) = sandbox_store(110, 502, deterministic_services());
    let base_volume_id = VolumeId::new(0x9010);
    let session_volume_id = VolumeId::new(0x9011);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_capabilities(CapabilityManifest {
                    capabilities: vec![SandboxCapability::git_remote_import()],
                }),
        )
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: source.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::DirectorySnapshot,
            delete_missing: true,
        })
        .await
        .expect("hoist directory");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"two\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");

    let report = session
        .eject_to_disk(EjectRequest {
            target_path: target.to_string_lossy().into_owned(),
            mode: EjectMode::ApplyDelta,
            conflict_policy: ConflictPolicy::CreatePatchBundle,
        })
        .await
        .expect("eject with patch bundle fallback");
    assert!(!report.conflicts.is_empty());
    let patch_bundle = PathBuf::from(report.patch_bundle_path.expect("patch bundle path"));
    assert!(patch_bundle.exists());
    assert_eq!(read_host_file(&target.join("tracked.txt")), "external\n");

    cleanup(&source);
    cleanup(&target);
}

#[tokio::test]
async fn git_working_tree_hoist_captures_dirty_provenance_and_untracked_files() {
    let repo = unique_test_dir("git-working-tree");
    init_git_repo(&repo, None);
    write_host_file(&repo.join("tracked.txt"), "dirty tracked\n");
    write_host_file(&repo.join("untracked.txt"), "untracked\n");

    let (vfs, sandbox) = sandbox_store(120, 503, host_git_services());
    let base_volume_id = VolumeId::new(0x9020);
    let session_volume_id = VolumeId::new(0x9021);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_capabilities(CapabilityManifest {
                    capabilities: vec![SandboxCapability::git_remote_import()],
                }),
        )
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitWorkingTree {
                include_untracked: true,
                include_ignored: false,
            },
            delete_missing: true,
        })
        .await
        .expect("hoist git working tree");

    assert_eq!(
        session
            .filesystem()
            .read_file("/workspace/tracked.txt")
            .await
            .expect("read tracked file"),
        Some(b"dirty tracked\n".to_vec())
    );
    assert_eq!(
        session
            .filesystem()
            .read_file("/workspace/untracked.txt")
            .await
            .expect("read untracked file"),
        Some(b"untracked\n".to_vec())
    );
    let info = session.info().await;
    let provenance = info.provenance.git.expect("git provenance");
    assert!(provenance.dirty);
    assert_eq!(provenance.branch.as_deref(), Some("main"));
    assert_eq!(
        info.provenance.hoisted_source.expect("hoisted source").mode,
        HoistMode::GitWorkingTree {
            include_untracked: true,
            include_ignored: false,
        }
    );

    cleanup(&repo);
}

#[tokio::test]
async fn git_hoist_without_host_bridge_fails_closed() {
    let repo = unique_test_dir("git-no-bridge");
    init_git_repo(&repo, None);

    let (vfs, sandbox) = sandbox_store(121, 5031, deterministic_services());
    let base_volume_id = VolumeId::new(0x9022);
    let session_volume_id = VolumeId::new(0x9023);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_capabilities(CapabilityManifest {
                    capabilities: vec![SandboxCapability::git_remote_import()],
                }),
        )
        .await
        .expect("open session");
    let error = session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect_err("git hoist should require a host bridge");
    assert!(matches!(
        error,
        SandboxError::Service { service: "git", .. }
    ));

    cleanup(&repo);
}

#[tokio::test]
async fn create_pull_request_pushes_branch_without_exposing_host_workspace() {
    let fake = FakeGithubServer::spawn().await;
    let repo = unique_test_dir("git-pr-repo");
    init_git_repo(&repo, None);
    set_git_remote(&repo, &fake.remote_url);

    let (vfs, sandbox) = sandbox_store(130, 504, remote_github_services("127.0.0.1"));
    let base_volume_id = VolumeId::new(0x9030);
    let session_volume_id = VolumeId::new(0x9031);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_capabilities(CapabilityManifest {
                    capabilities: vec![SandboxCapability::git_remote_import()],
                }),
        )
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"pr branch change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Sandbox PR".to_string(),
            body: "Generated from sandbox".to_string(),
            head_branch: "sandbox/feature".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request");
    assert!(report.url.contains("/pull/1"));
    assert_eq!(
        report.metadata.get("committed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        report.metadata.get("pushed"),
        Some(&serde_json::Value::Bool(true))
    );

    assert_eq!(
        fake.branch_file_text("sandbox/feature", "tracked.txt")
            .await,
        Some("pr branch change\n".to_string())
    );
    assert_eq!(fake.pulls().await.len(), 1);
    assert!(
        report.metadata.get("workspace_path").is_none(),
        "repo-backed PR flow should not expose a host workspace path"
    );

    cleanup(&repo);
}

#[tokio::test]
async fn create_pull_request_supports_github_scp_style_remote_urls() {
    let fake = FakeGithubServer::spawn().await;
    let repo = unique_test_dir("git-pr-scp-remote-repo");
    init_git_repo(&repo, None);
    set_git_remote(&repo, "git@github.example:octo/demo.git");

    let api_base_url = format!(
        "{}/api/v3",
        fake.remote_url.trim_end_matches("/octo/demo.git")
    );
    let services = SandboxServices::new(
        Arc::new(DeterministicRuntimeBackend::default()),
        Arc::new(DeterministicPackageInstaller::default()),
        Arc::new(terracedb_sandbox::DeterministicGitRepositoryStore::default()),
        Arc::new(DeterministicPullRequestProviderClient::default()),
        Arc::new(DeterministicReadonlyViewProvider::default()),
    )
    .with_git_host_bridge(configured_remote_github_bridge_with_api_base(
        "github.example",
        api_base_url,
    ));
    let (vfs, sandbox) = sandbox_store(131, 505, services);
    let base_volume_id = VolumeId::new(0x9032);
    let session_volume_id = VolumeId::new(0x9033);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo with scp remote");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"scp remote change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Sandbox SCP PR".to_string(),
            body: "Generated from an ssh-style remote".to_string(),
            head_branch: "sandbox/scp-remote".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request from scp-style remote");
    assert!(report.url.contains("/pull/1"));
    assert_eq!(report.metadata.get("committed"), Some(&json!(true)));
    assert_eq!(report.metadata.get("pushed"), Some(&json!(true)));
    assert_eq!(
        fake.branch_file_text("sandbox/scp-remote", "tracked.txt")
            .await,
        Some("scp remote change\n".to_string())
    );

    cleanup(&repo);
}

#[tokio::test]
async fn repo_backed_pull_request_fails_closed_when_no_git_changes_exist() {
    let repo = unique_test_dir("git-pr-noop-repo");
    let remote = unique_test_dir("git-pr-noop-remote");
    init_git_repo(&repo, Some(&remote));

    let (vfs, sandbox) = sandbox_store(130, 50401, host_git_services());
    let base_volume_id = VolumeId::new(0x9130);
    let session_volume_id = VolumeId::new(0x9131);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");

    let initial_head = session.git_head().await.expect("read initial git head");
    assert_eq!(
        initial_head.symbolic_ref.as_deref(),
        Some("refs/heads/main")
    );

    let error = session
        .create_pull_request(PullRequestRequest {
            title: "Sandbox PR".to_string(),
            body: "Generated from sandbox".to_string(),
            head_branch: "sandbox/noop".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect_err("unchanged repo-backed PR should fail closed");
    assert!(matches!(
        error,
        SandboxError::NoGitChanges { ref target_ref }
        if target_ref == "refs/heads/sandbox/noop"
    ));

    let after_head = session
        .git_head()
        .await
        .expect("read git head after no-op PR");
    assert_eq!(after_head, initial_head);

    let pushed_head = sanitized_git_command()
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/sandbox/noop"])
        .output()
        .expect("inspect remote no-op branch");
    assert!(
        !pushed_head.status.success(),
        "no-op repo-backed PR should not push a branch"
    );

    cleanup(&repo);
    cleanup(&remote);
}

#[tokio::test]
async fn host_git_import_preserves_executable_bits_through_status_and_push() {
    let fake = FakeGithubServer::spawn().await;
    let repo = unique_test_dir("git-pr-executable-repo");
    init_git_repo(&repo, None);
    set_git_remote(&repo, &fake.remote_url);
    write_executable_host_file(&repo.join("script.sh"), "#!/bin/sh\necho sandbox\n");
    git(&repo, &["add", "script.sh"]);
    git(&repo, &["commit", "-m", "add executable script"]);

    let (vfs, sandbox) = sandbox_store(130, 50402, remote_github_services("127.0.0.1"));
    let base_volume_id = VolumeId::new(0x9132);
    let session_volume_id = VolumeId::new(0x9133);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");

    let imported_script = session
        .filesystem()
        .stat("/workspace/script.sh")
        .await
        .expect("stat imported script")
        .expect("imported script exists");
    assert_eq!(imported_script.mode & 0o777, 0o755);

    let status = session
        .git_status(Default::default())
        .await
        .expect("status imported git repo");
    assert!(
        !status.dirty,
        "imported executable should not appear modified"
    );
    assert!(status.entries.is_empty());

    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"tracked after executable import\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update tracked file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Sandbox executable PR".to_string(),
            body: "Keeps imported executable modes intact".to_string(),
            head_branch: "sandbox/executable".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request");
    assert_eq!(report.metadata.get("pushed"), Some(&json!(true)));
    assert!(
        fake.branch_entry_mode("sandbox/executable", "script.sh")
            .await
            .as_deref()
            == Some("100755"),
        "pushed executable should retain mode 100755"
    );

    cleanup(&repo);
}

#[tokio::test]
async fn repo_backed_pull_request_fails_closed_without_remote() {
    let repo = unique_test_dir("git-pr-no-remote-repo");
    init_git_repo(&repo, None);

    let (vfs, sandbox) = sandbox_store(130, 50403, host_git_services());
    let base_volume_id = VolumeId::new(0x9134);
    let session_volume_id = VolumeId::new(0x9135);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"change without remote\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update tracked file");

    let initial_head = session.git_head().await.expect("read initial git head");
    let error = session
        .create_pull_request(PullRequestRequest {
            title: "Sandbox PR".to_string(),
            body: "Generated from sandbox".to_string(),
            head_branch: "sandbox/no-remote".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect_err("repo-backed PR without remote should fail closed");
    assert!(matches!(error, SandboxError::MissingGitRemote));
    assert_eq!(
        session
            .git_head()
            .await
            .expect("read head after failed no-remote PR"),
        initial_head
    );

    cleanup(&repo);
}

#[tokio::test]
async fn repo_backed_pull_request_uses_git_bridge_not_legacy_provider() {
    let fake = FakeGithubServer::spawn().await;
    let repo = unique_test_dir("git-pr-bridge-provider-repo");
    init_git_repo(&repo, None);
    set_git_remote(&repo, &fake.remote_url);

    let services =
        remote_github_services_with_provider("127.0.0.1", Arc::new(FailingPullRequestProvider));
    let (vfs, sandbox) = sandbox_store(131, 5041, services);
    let base_volume_id = VolumeId::new(0x9032);
    let session_volume_id = VolumeId::new(0x9033);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"bridge provider path\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Bridge Provider PR".to_string(),
            body: "Should bypass legacy provider".to_string(),
            head_branch: "sandbox/bridge-provider".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request through git bridge");
    assert_eq!(report.provider, "host-git");
    assert_eq!(report.metadata.get("pushed"), Some(&json!(true)));
    assert_eq!(
        fake.branch_file_text("sandbox/bridge-provider", "tracked.txt")
            .await,
        Some("bridge provider path\n".to_string())
    );

    cleanup(&repo);
}

#[tokio::test]
async fn repo_backed_js_git_library_can_create_pull_request() {
    let fake = FakeGithubServer::spawn().await;
    let repo = unique_test_dir("git-pr-js-repo");
    init_git_repo(&repo, None);
    set_git_remote(&repo, &fake.remote_url);

    let (vfs, sandbox) = sandbox_store(132, 5042, remote_github_services("127.0.0.1"));
    let base_volume_id = VolumeId::new(0x9034);
    let session_volume_id = VolumeId::new(0x9035);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"js bridge change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update sandbox file");
    session
        .filesystem()
        .write_file(
            "/workspace/main.js",
            format!(
                "import {{ createPullRequest }} from \"{SANDBOX_GIT_LIBRARY_SPECIFIER}\";\n\
const report = await createPullRequest({{\n  title: \"JS Sandbox PR\",\n  body: \"Created through the sandbox git host surface\",\n  headBranch: \"sandbox/js-bridge\",\n  baseBranch: \"main\"\n}});\n\
export default report;\n"
            )
            .into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write js entrypoint");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute js git module");
    let report = result.result.expect("js result");
    assert_eq!(report["provider"], json!("host-git"));
    assert_eq!(report["metadata"]["pushed"], json!(true));
    assert!(
        result
            .module_graph
            .contains(&SANDBOX_GIT_LIBRARY_SPECIFIER.to_string())
    );
    assert_eq!(
        fake.branch_file_text("sandbox/js-bridge", "tracked.txt")
            .await,
        Some("js bridge change\n".to_string())
    );

    cleanup(&repo);
}

#[tokio::test]
async fn remote_repository_import_records_remote_origin_without_host_hoist() {
    let fake = FakeGithubServer::spawn().await;
    let (vfs, sandbox) = sandbox_store(134, 5044, remote_github_services("127.0.0.1"));
    let base_volume_id = VolumeId::new(0x9038);
    let session_volume_id = VolumeId::new(0x9039);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let report = session
        .import_remote_repository(GitRemoteImportRequest {
            remote_url: fake.remote_url.clone(),
            reference: None,
            metadata: BTreeMap::new(),
            target_root: "/workspace".to_string(),
            delete_missing: true,
        })
        .await
        .expect("import remote repository");

    assert_eq!(
        report.git_provenance.origin,
        terracedb_sandbox::GitRepositoryOrigin::RemoteImport
    );
    assert_eq!(report.git_provenance.branch.as_deref(), Some("main"));
    assert_eq!(
        report.git_provenance.remote_url.as_deref(),
        Some(fake.remote_url.as_str())
    );
    assert!(report.imported_paths > 0);

    let info = session.info().await;
    assert_eq!(info.provenance.hoisted_source, None);
    assert_eq!(
        info.provenance.git.expect("session git provenance").origin,
        terracedb_sandbox::GitRepositoryOrigin::RemoteImport
    );
    assert_eq!(
        session
            .git_head()
            .await
            .expect("head after remote import")
            .symbolic_ref,
        Some("refs/heads/main".to_string())
    );
}

#[tokio::test]
async fn repo_backed_js_git_library_can_import_remote_repository_and_create_pull_request() {
    let fake = FakeGithubServer::spawn().await;
    let (vfs, sandbox) = sandbox_store(135, 5045, remote_github_services("127.0.0.1"));
    let base_volume_id = VolumeId::new(0x903a);
    let session_volume_id = VolumeId::new(0x903b);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_capabilities(CapabilityManifest {
                    capabilities: vec![SandboxCapability::git_remote_import()],
                }),
        )
        .await
        .expect("open session");
    session
        .set_remote_bridge_session_metadata(
            fake.remote_url.clone(),
            BTreeMap::from([("auth_token".to_string(), json!("test-token"))]),
        )
        .await;
    session
        .filesystem()
        .write_file(
            "/workspace/main.js",
            format!(
                "import {{ writeTextFile }} from \"@terracedb/sandbox/fs\";\n\
import {{ importRepository, head, status, createPullRequest }} from \"{SANDBOX_GIT_LIBRARY_SPECIFIER}\";\n\
const imported = await importRepository({{\n  remoteUrl: \"{}\",\n  metadata: {{ provider: \"github\", credential_id: \"app-user-1\" }},\n  targetRoot: \"/workspace\",\n  deleteMissing: true\n}});\n\
const headBefore = await head();\n\
await writeTextFile(\"/workspace/tracked.txt\", \"js remote change\\n\");\n\
const statusBefore = await status({{ pathspec: [\"tracked.txt\"] }});\n\
const pr = await createPullRequest({{\n  title: \"JS Remote PR\",\n  body: \"Created from a remote-imported VFS repo\",\n  headBranch: \"sandbox/remote-js\",\n  baseBranch: \"main\"\n}});\n\
export default {{\n  importedOrigin: imported.git_provenance.origin,\n  importedBranch: imported.git_provenance.branch,\n  importedRemote: imported.git_provenance.remote_url,\n  importedProvider: imported.git_provenance.remote_bridge_metadata.provider,\n  importedCredentialId: imported.git_provenance.remote_bridge_metadata.credential_id,\n  importedAuthTokenVisible: Object.prototype.hasOwnProperty.call(imported.git_provenance.remote_bridge_metadata, \"auth_token\"),\n  headBefore: headBefore.symbolic_ref,\n  statusDirty: statusBefore.dirty,\n  provider: pr.provider,\n  url: pr.url\n}};\n",
                fake.remote_url
            )
            .into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write remote js entrypoint");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute remote js git module");
    let report = result.result.expect("js result");
    assert_eq!(report["importedOrigin"], json!("remote_import"));
    assert_eq!(report["importedBranch"], json!("main"));
    assert_eq!(report["importedRemote"], json!(fake.remote_url.clone()));
    assert_eq!(report["importedProvider"], json!("github"));
    assert_eq!(report["importedCredentialId"], json!("app-user-1"));
    assert_eq!(report["importedAuthTokenVisible"], json!(false));
    assert_eq!(report["headBefore"], json!("refs/heads/main"));
    assert_eq!(report["statusDirty"], json!(true));
    assert_eq!(report["provider"], json!("host-git"));
    assert!(
        report["url"]
            .as_str()
            .expect("remote pr url")
            .contains("/pull/1")
    );
    assert!(
        result
            .module_graph
            .contains(&SANDBOX_GIT_LIBRARY_SPECIFIER.to_string())
    );
    assert!(
        result
            .module_graph
            .contains(&"@terracedb/sandbox/fs".to_string())
    );

    let session_info = session.info().await;
    assert_eq!(session_info.provenance.hoisted_source, None);
    let git = session_info.provenance.git.expect("session git provenance");
    assert_eq!(
        git.origin,
        terracedb_sandbox::GitRepositoryOrigin::RemoteImport
    );
    assert_eq!(
        git.remote_bridge_metadata.get("provider"),
        Some(&json!("github"))
    );
    assert_eq!(
        git.remote_bridge_metadata.get("credential_id"),
        Some(&json!("app-user-1"))
    );
    assert!(git.remote_bridge_metadata.get("auth_token").is_none());

    assert_eq!(
        fake.branch_file_text("sandbox/remote-js", "tracked.txt")
            .await,
        Some("js remote change\n".to_string())
    );
    let pulls = fake.pulls().await;
    assert_eq!(pulls.len(), 1);
    assert_eq!(pulls[0].head, "sandbox/remote-js");
    assert_eq!(pulls[0].base, "main");
    assert_eq!(pulls[0].title, "JS Remote PR");
}

#[tokio::test]
async fn repo_backed_js_git_library_rejects_sensitive_remote_metadata() {
    let fake = FakeGithubServer::spawn().await;
    let (vfs, sandbox) = sandbox_store(136, 5046, remote_github_services("127.0.0.1"));
    let base_volume_id = VolumeId::new(0x903c);
    let session_volume_id = VolumeId::new(0x903d);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_capabilities(CapabilityManifest {
                    capabilities: vec![SandboxCapability::git_remote_import()],
                }),
        )
        .await
        .expect("open session");
    session
        .filesystem()
        .write_file(
            "/workspace/main.js",
            format!(
                "import {{ importRepository }} from \"{SANDBOX_GIT_LIBRARY_SPECIFIER}\";\n\
await importRepository({{\n  remoteUrl: \"{}\",\n  metadata: {{ provider: \"github\", auth_token: \"test-token\" }},\n  targetRoot: \"/workspace\",\n  deleteMissing: true\n}});\n\
export default null;\n",
                fake.remote_url
            )
            .into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write rejecting js entrypoint");

    let error = session
        .exec_module("/workspace/main.js")
        .await
        .expect_err("sandbox js should not be allowed to pass remote credentials");
    let message = error.to_string();
    assert!(message.contains("remote bridge credentials must be configured by the host app"));
    assert!(message.contains("auth_token"));
}

#[tokio::test]
async fn repo_backed_js_git_library_remote_import_requires_explicit_capability() {
    let fake = FakeGithubServer::spawn().await;
    let (vfs, sandbox) = sandbox_store(137, 5047, remote_github_services("127.0.0.1"));
    let base_volume_id = VolumeId::new(0x903e);
    let session_volume_id = VolumeId::new(0x903f);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .filesystem()
        .write_file(
            "/workspace/main.js",
            format!(
                "import {{ importRepository }} from \"{SANDBOX_GIT_LIBRARY_SPECIFIER}\";\n\
await importRepository({{\n  remoteUrl: \"{}\",\n  targetRoot: \"/workspace\",\n  deleteMissing: true\n}});\n\
export default null;\n",
                fake.remote_url
            )
            .into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write gating js entrypoint");

    let error = session
        .exec_module("/workspace/main.js")
        .await
        .expect_err("sandbox js should not import remotes without explicit capability");
    let message = error.to_string();
    assert!(message.contains("host module exports are not visible in this runtime policy"));
}

#[tokio::test]
async fn repo_backed_js_git_library_can_inspect_and_switch_vfs_repositories() {
    let repo = unique_test_dir("git-js-surface-repo");
    init_git_repo(&repo, None);

    let (vfs, sandbox) = sandbox_store(133, 5043, host_git_services());
    let base_volume_id = VolumeId::new(0x9036);
    let session_volume_id = VolumeId::new(0x9037);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/main.js",
            format!(
                "import {{ writeTextFile }} from \"@terracedb/sandbox/fs\";\n\
import {{ head, listRefs, status, diff, updateRef, checkout }} from \"{SANDBOX_GIT_LIBRARY_SPECIFIER}\";\n\
const before = await head();\n\
await writeTextFile(\"/workspace/tracked.txt\", \"js surface change\\n\");\n\
const statusBefore = await status({{ pathspec: [\"tracked.txt\"] }});\n\
const diffBefore = await diff({{ pathspec: [\"tracked.txt\"] }});\n\
await updateRef({{\n  name: \"refs/heads/sandbox/js-surface\",\n  target: before.oid,\n  previousTarget: null,\n  symbolic: false\n}});\n\
const refsAfter = await listRefs();\n\
const checkoutReport = await checkout({{\n  targetRef: \"refs/heads/sandbox/js-surface\",\n  materializePath: \"/workspace\",\n  updateHead: true\n}});\n\
const after = await head();\n\
const statusAfter = await status({{ pathspec: [\"tracked.txt\"] }});\n\
export default {{\n  beforeOid: before.oid,\n  beforeRef: before.symbolic_ref,\n  refsAfter: refsAfter.map((reference) => reference.name),\n  statusBeforeDirty: statusBefore.dirty,\n  statusBeforeKinds: statusBefore.entries.map((entry) => entry.kind),\n  diffBeforeKinds: diffBefore.entries.map((entry) => entry.kind),\n  diffBeforePaths: diffBefore.entries.map((entry) => entry.path),\n  checkoutTargetRef: checkoutReport.target_ref,\n  afterOid: after.oid,\n  afterRef: after.symbolic_ref,\n  statusAfterDirty: statusAfter.dirty\n}};\n"
            )
            .into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write js entrypoint");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute js git module");
    let report = result.result.expect("js result");
    assert_eq!(report["beforeRef"], json!("refs/heads/main"));
    assert_eq!(
        report["refsAfter"],
        json!(["refs/heads/main", "refs/heads/sandbox/js-surface"])
    );
    assert_eq!(report["statusBeforeDirty"], json!(true));
    assert_eq!(report["statusBeforeKinds"], json!(["modified"]));
    assert_eq!(report["diffBeforeKinds"], json!(["modified"]));
    assert_eq!(report["diffBeforePaths"], json!(["tracked.txt"]));
    assert_eq!(
        report["checkoutTargetRef"],
        json!("refs/heads/sandbox/js-surface")
    );
    assert_eq!(report["afterRef"], json!("refs/heads/sandbox/js-surface"));
    assert_eq!(report["afterOid"], report["beforeOid"]);
    assert_eq!(report["statusAfterDirty"], json!(false));
    assert!(
        result
            .module_graph
            .contains(&SANDBOX_GIT_LIBRARY_SPECIFIER.to_string())
    );
    assert!(
        result
            .module_graph
            .contains(&"@terracedb/sandbox/fs".to_string())
    );

    cleanup(&repo);
}

#[tokio::test]
async fn replacing_git_repository_store_keeps_host_bridge_in_sync_for_exports() {
    let fake = FakeGithubServer::spawn().await;
    let repo = unique_test_dir("git-pr-manager-sync-repo");
    init_git_repo(&repo, None);
    set_git_remote(&repo, &fake.remote_url);

    let services = remote_github_services("127.0.0.1").with_git_repository_store(Arc::new(
        terracedb_sandbox::DeterministicGitRepositoryStore::default(),
    ));
    let (vfs, sandbox) = sandbox_store(140, 505, services);
    let base_volume_id = VolumeId::new(0x9040);
    let session_volume_id = VolumeId::new(0x9041);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open synced host git session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist synced host git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"synced host bridge change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update synced sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Synced Sandbox PR".to_string(),
            body: "Generated from replaced git manager".to_string(),
            head_branch: "sandbox/manager-sync".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create synced pull request");
    assert_eq!(
        report.metadata.get("committed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        report.metadata.get("pushed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        fake.branch_file_text("sandbox/manager-sync", "tracked.txt")
            .await,
        Some("synced host bridge change\n".to_string())
    );
    assert!(report.metadata.get("workspace_path").is_none());

    cleanup(&repo);
}

#[tokio::test]
async fn replacing_git_host_bridge_keeps_repository_store_in_sync_for_exports() {
    let fake = FakeGithubServer::spawn().await;
    let repo = unique_test_dir("git-pr-bridge-sync-repo");
    init_git_repo(&repo, None);
    set_git_remote(&repo, &fake.remote_url);

    let services = SandboxServices::deterministic()
        .with_git_host_bridge(configured_remote_github_bridge("127.0.0.1"));
    let (vfs, sandbox) = sandbox_store(150, 506, services);
    let base_volume_id = VolumeId::new(0x9050);
    let session_volume_id = VolumeId::new(0x9051);
    create_empty_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open bridge-synced host git session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist bridge-synced host git repo");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            b"bridge-synced host change\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update bridge-synced sandbox file");

    let report = session
        .create_pull_request(PullRequestRequest {
            title: "Bridge Synced Sandbox PR".to_string(),
            body: "Generated from replaced git bridge".to_string(),
            head_branch: "sandbox/bridge-sync".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create bridge-synced pull request");
    assert_eq!(
        report.metadata.get("committed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        report.metadata.get("pushed"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        fake.branch_file_text("sandbox/bridge-sync", "tracked.txt")
            .await,
        Some("bridge-synced host change\n".to_string())
    );
    assert!(report.metadata.get("workspace_path").is_none());

    cleanup(&repo);
}
