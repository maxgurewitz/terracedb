use std::{collections::BTreeMap, sync::Arc};

use serde_json::json;
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_git::{
    DeterministicGitHostBridge, DeterministicGitRepositoryStore, GitDiscoverRequest,
    GitExportRequest, GitForkPolicy, GitHostBridge, GitImportRequest, GitOpenRequest,
    GitPullRequestRequest, GitPushRequest, GitRepositoryImage, GitRepositoryPolicy,
    GitRepositoryProvenance, GitRepositoryStore, NeverCancel as NeverCancelGit,
    VfsGitRepositoryImage,
};
use terracedb_js::{
    DeterministicJsEntropySource, DeterministicJsHostServices, DeterministicJsRuntimeHost,
    DeterministicJsScheduler, DeterministicJsServiceOutcome, FixedJsClock, JsExecutionHooks,
    JsExecutionRequest, JsForkPolicy, JsHostServices, JsModuleLoader, JsRuntimeHost,
    JsRuntimeOpenRequest, JsRuntimePolicy, JsRuntimeProvenance, NeverCancel, NoopJsExecutionHooks,
    VfsJsModuleLoader,
};
use terracedb_vfs::{
    CreateOptions, InMemoryVfsStore, SnapshotOptions, VolumeConfig, VolumeId, VolumeStore,
};

async fn seeded_snapshot() -> Arc<dyn terracedb_vfs::VolumeSnapshot> {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(42))),
        Arc::new(StubRng::seeded(99)),
    );
    let store = InMemoryVfsStore::with_dependencies(dependencies);
    let volume = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x9000))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");

    volume
        .fs()
        .write_file(
            "/workspace/helper.mjs",
            b"export default {\"helper\":true};".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write helper module");
    volume
        .fs()
        .write_file(
            "/workspace/main.mjs",
            br#"
import "terrace:/workspace/helper.mjs";
import "terrace:host/echo";
// terrace-host-call: capability echo {"message":"hello"}
export default {"status":"ok"};
"#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write main module");
    volume
        .fs()
        .write_file(
            "/repo/.git/HEAD",
            b"ref: refs/heads/main\n".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write head");
    volume
        .fs()
        .write_file(
            "/repo/.git/refs/heads/main",
            b"1111\n".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write main ref");
    volume
        .fs()
        .write_file(
            "/repo/.git/objects/1111",
            b"blob\nhello repo\n".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write object");
    volume
        .fs()
        .write_file(
            "/repo/.git/index.json",
            serde_json::to_vec(&terracedb_git::GitIndexSnapshot {
                entries: vec![terracedb_git::types::GitIndexEntry {
                    path: "README.md".to_string(),
                    oid: Some("1111".to_string()),
                    mode: 0o100644,
                }],
                metadata: BTreeMap::new(),
            })
            .expect("encode index"),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write index");

    volume
        .snapshot(SnapshotOptions::default())
        .await
        .expect("snapshot")
}

#[tokio::test]
async fn public_substrate_contracts_are_instantiable() {
    let snapshot = seeded_snapshot().await;
    let loader: Arc<dyn JsModuleLoader> = Arc::new(VfsJsModuleLoader::new(snapshot.clone()));
    let host_services = DeterministicJsHostServices::new();
    host_services
        .register_outcome(
            "capability",
            "echo",
            DeterministicJsServiceOutcome::Response {
                result: json!({"echoed":"hello"}),
                metadata: BTreeMap::from([("kind".to_string(), json!("capability"))]),
            },
        )
        .await;
    let host_services: Arc<dyn JsHostServices> = Arc::new(host_services);
    let hooks: Arc<dyn JsExecutionHooks> = Arc::new(NoopJsExecutionHooks);
    let runtime_host: Arc<dyn JsRuntimeHost> = Arc::new(
        DeterministicJsRuntimeHost::new(loader.clone(), host_services.clone())
            .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
            .with_clock(Arc::new(FixedJsClock::new(1_234)))
            .with_entropy(Arc::new(DeterministicJsEntropySource::new(0xfeed)))
            .with_hooks(hooks),
    );
    let repo_image = Arc::new(VfsGitRepositoryImage::new(snapshot, "/repo"));
    let repo_descriptor = GitRepositoryImage::descriptor(repo_image.as_ref());
    let repo_store: Arc<dyn GitRepositoryStore> =
        Arc::new(DeterministicGitRepositoryStore::default());
    let bridge: Arc<dyn GitHostBridge> = Arc::new(DeterministicGitHostBridge::default());

    assert_eq!(runtime_host.name(), "deterministic-js");
    assert_eq!(repo_store.name(), "deterministic-git");
    assert_eq!(
        runtime_host.fork_policy(),
        JsForkPolicy::simulation_native_baseline()
    );
    assert_eq!(
        repo_store.fork_policy(),
        GitForkPolicy::simulation_native_baseline()
    );

    let discovered = repo_store
        .discover(
            repo_image.clone(),
            GitDiscoverRequest {
                start_path: "/repo/src".to_string(),
                policy: GitRepositoryPolicy::default(),
                metadata: BTreeMap::from([("kind".to_string(), json!("compile-only"))]),
            },
            Arc::new(NeverCancelGit),
        )
        .await
        .expect("discover repo");
    assert_eq!(discovered.repository_root, "/repo");

    let repo = repo_store
        .open(
            repo_image,
            GitOpenRequest {
                repository_id: "repo-1".to_string(),
                repository_image: repo_descriptor.clone(),
                policy: GitRepositoryPolicy::default(),
                provenance: GitRepositoryProvenance {
                    backend: "deterministic-git".to_string(),
                    repo_root: repo_descriptor.root_path.clone(),
                    imported_from_host: false,
                    volume_id: repo_descriptor.volume_id,
                    snapshot_sequence: repo_descriptor.snapshot_sequence,
                    durable_snapshot: repo_descriptor.durable_snapshot,
                    fork_policy: GitForkPolicy::simulation_native_baseline(),
                },
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancelGit),
        )
        .await
        .expect("open repo");
    assert_eq!(
        repo.list_refs()
            .await
            .expect("list refs")
            .into_iter()
            .map(|reference| reference.name)
            .collect::<Vec<_>>(),
        vec!["refs/heads/main".to_string()]
    );

    let imported = bridge
        .import_repository(
            GitImportRequest {
                source_path: "/host/repo".to_string(),
                target_root: "/repo".to_string(),
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancelGit),
        )
        .await
        .expect("import repo");
    assert_eq!(imported.target_root, "/repo");
}

#[tokio::test]
async fn deterministic_smoke_executes_fake_runtime_and_repo_over_vfs() {
    let snapshot = seeded_snapshot().await;
    let loader: Arc<dyn JsModuleLoader> = Arc::new(VfsJsModuleLoader::new(snapshot.clone()));
    let host_services = DeterministicJsHostServices::new();
    host_services
        .register_outcome(
            "capability",
            "echo",
            DeterministicJsServiceOutcome::Response {
                result: json!({"echoed":"hello"}),
                metadata: BTreeMap::from([("kind".to_string(), json!("capability"))]),
            },
        )
        .await;
    let runtime_host = DeterministicJsRuntimeHost::new(loader, Arc::new(host_services))
        .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
        .with_clock(Arc::new(FixedJsClock::new(7_777)))
        .with_entropy(Arc::new(DeterministicJsEntropySource::new(0x1234)));
    let runtime = runtime_host
        .open_runtime(JsRuntimeOpenRequest {
            runtime_id: "runtime-1".to_string(),
            policy: JsRuntimePolicy {
                visible_host_services: vec!["capability::echo".to_string()],
                ..Default::default()
            },
            provenance: JsRuntimeProvenance {
                backend: "deterministic-js".to_string(),
                host_model: "host-owned".to_string(),
                module_root: "/workspace".to_string(),
                volume_id: Some(VolumeId::new(0x9000)),
                snapshot_sequence: Some(1),
                durable_snapshot: false,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            metadata: BTreeMap::new(),
        })
        .await
        .expect("open runtime");
    let report = runtime
        .execute(
            JsExecutionRequest::module("terrace:/workspace/main.mjs"),
            Arc::new(NeverCancel),
        )
        .await
        .expect("execute fake runtime");
    assert_eq!(report.result, Some(json!({"status":"ok"})));
    assert_eq!(report.clock_now_millis, 7_777);
    assert_eq!(
        report.module_graph,
        vec![
            "terrace:/workspace/main.mjs".to_string(),
            "terrace:/workspace/helper.mjs".to_string(),
            "terrace:host/echo".to_string()
        ]
    );
    assert_eq!(report.host_calls.len(), 1);
    assert_eq!(report.host_calls[0].result, Some(json!({"echoed":"hello"})));

    let repo_image = Arc::new(VfsGitRepositoryImage::new(snapshot, "/repo"));
    let repo_descriptor = GitRepositoryImage::descriptor(repo_image.as_ref());
    let repo_store = DeterministicGitRepositoryStore::default();
    let repo = repo_store
        .open(
            repo_image,
            GitOpenRequest {
                repository_id: "repo-2".to_string(),
                repository_image: repo_descriptor.clone(),
                policy: GitRepositoryPolicy {
                    allow_host_bridge: true,
                    ..Default::default()
                },
                provenance: GitRepositoryProvenance {
                    backend: "deterministic-git".to_string(),
                    repo_root: repo_descriptor.root_path.clone(),
                    imported_from_host: false,
                    volume_id: repo_descriptor.volume_id,
                    snapshot_sequence: repo_descriptor.snapshot_sequence,
                    durable_snapshot: repo_descriptor.durable_snapshot,
                    fork_policy: GitForkPolicy::simulation_native_baseline(),
                },
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancelGit),
        )
        .await
        .expect("open deterministic repo");
    let head = repo.head().await.expect("head");
    assert_eq!(head.symbolic_ref.as_deref(), Some("refs/heads/main"));
    assert_eq!(head.oid.as_deref(), Some("1111"));
    let object = terracedb_git::GitObjectDatabase::read_object(repo.as_ref(), "1111")
        .await
        .expect("read object")
        .expect("object");
    assert_eq!(object.kind, terracedb_git::GitObjectKind::Blob);
    assert_eq!(object.data, b"hello repo\n".to_vec());

    let bridge = DeterministicGitHostBridge::default();
    let export = bridge
        .export_repository(
            repo.clone(),
            GitExportRequest {
                target_path: "/tmp/export".to_string(),
                branch_name: Some("sandbox/test".to_string()),
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancelGit),
        )
        .await
        .expect("export repo");
    assert_eq!(export.target_path, "/tmp/export");
    let push = bridge
        .push(
            repo.clone(),
            GitPushRequest {
                remote: "origin".to_string(),
                branch_name: "sandbox/test".to_string(),
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancelGit),
        )
        .await
        .expect("push repo");
    assert_eq!(push.pushed_oid.as_deref(), Some("1111"));
    let pr = bridge
        .create_pull_request(
            GitPullRequestRequest {
                title: "Sandbox".to_string(),
                body: "Body".to_string(),
                head_branch: "sandbox/test".to_string(),
                base_branch: "main".to_string(),
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancelGit),
        )
        .await
        .expect("create pr");
    assert!(pr.url.starts_with("https://example.invalid/pull/"));
}
