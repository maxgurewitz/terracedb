use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::json;
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_git::{
    DeterministicGitHostBridge, DeterministicGitRepositoryStore, GitDiscoverRequest,
    GitExportRequest, GitForkPolicy, GitHostBridge, GitImportMode, GitImportRequest,
    GitOpenRequest, GitPullRequestRequest, GitPushRequest, GitRepositoryImage, GitRepositoryPolicy,
    GitRepositoryProvenance, GitRepositoryStore, NeverCancel as NeverCancelGit,
    VfsGitRepositoryImage,
};
use terracedb_js::{
    BoaJsRuntimeHost, DeterministicJsEntropySource, DeterministicJsHostServices,
    DeterministicJsRuntimeHost, DeterministicJsScheduler, DeterministicJsServiceOutcome,
    FixedJsClock, JsExecutionHooks, JsExecutionRequest, JsForkPolicy, JsHostServiceCallRecord,
    JsHostServiceRequest, JsHostServiceResponse, JsHostServices, JsLoadedModule, JsModuleKind,
    JsModuleLoader, JsResolvedModule, JsRuntimeHost, JsRuntimeOpenRequest, JsRuntimePolicy,
    JsRuntimeProvenance, JsSubstrateError, NeverCancel, NoopJsExecutionHooks, RoutedJsHostServices,
    VfsJsHostServiceAdapter, VfsJsModuleLoader,
};
use terracedb_vfs::{
    CreateOptions, InMemoryVfsStore, SnapshotOptions, VfsStoreExt, VolumeConfig, VolumeId,
    VolumeStore,
};
use tokio::{sync::Mutex, task::yield_now, time::sleep};

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
            "/workspace/boa-helper.mjs",
            b"export default {\"helper\":true,\"message\":\"from helper\"};".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write boa helper module");
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
            "/workspace/boa-main.mjs",
            br#"
import helper from "./boa-helper.mjs";
import { echo } from "terrace:host/echo";

const response = await echo({ "message": helper.message });
export default {
  "status": "ok",
  "helper": helper.helper,
  "echoed": response.echoed,
  "now": Date.now(),
  "console_type": typeof console,
  "process_type": typeof process
};
"#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write boa main module");
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
                mode: GitImportMode::Head,
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancelGit),
        )
        .await
        .expect("import repo");
    assert_eq!(imported.target_root, "/repo");
}

#[tokio::test]
async fn routed_vfs_host_services_read_files_and_report_missing_paths() {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(42))),
        Arc::new(StubRng::seeded(0x1234)),
    );
    let store = InMemoryVfsStore::with_dependencies(dependencies);
    let volume = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x9900))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");
    volume
        .fs()
        .write_file(
            "/workspace/input.txt",
            b"hello routed host service".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed input");

    let services = RoutedJsHostServices::new().with_adapter(Arc::new(
        VfsJsHostServiceAdapter::new(volume.fs(), ["node:fs/promises"]),
    ));
    let response = JsHostServices::call(
        &services,
        JsHostServiceRequest {
            service: "node:fs/promises".to_string(),
            operation: "readTextFile".to_string(),
            arguments: json!(["/workspace/input.txt"]),
            metadata: BTreeMap::new(),
        },
    )
    .await
    .expect("read file through routed services");
    assert_eq!(response.result, Some(json!("hello routed host service")));
    assert_eq!(JsHostServices::calls(&services).await.len(), 1);

    let error = JsHostServices::call(
        &services,
        JsHostServiceRequest {
            service: "node:fs/promises".to_string(),
            operation: "readTextFile".to_string(),
            arguments: json!(["/workspace/missing.txt"]),
            metadata: BTreeMap::new(),
        },
    )
    .await
    .expect_err("missing path should fail closed");
    assert!(matches!(error, JsSubstrateError::EvaluationFailed { .. }));
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
                head_oid: Some("1111".to_string()),
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

#[derive(Clone, Default)]
struct YieldingAsyncBoaHostServices {
    calls: Arc<Mutex<Vec<JsHostServiceCallRecord>>>,
}

#[async_trait(?Send)]
impl JsHostServices for YieldingAsyncBoaHostServices {
    async fn call(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        yield_now().await;
        sleep(Duration::from_millis(1)).await;

        let echoed = request
            .arguments
            .get("message")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let response = JsHostServiceResponse {
            result: Some(json!({ "echoed": echoed })),
            metadata: BTreeMap::new(),
        };
        self.calls.lock().await.push(JsHostServiceCallRecord {
            service: request.service,
            operation: request.operation,
            arguments: request.arguments,
            result: response.result.clone(),
            metadata: response.metadata.clone(),
        });
        Ok(response)
    }

    async fn calls(&self) -> Vec<JsHostServiceCallRecord> {
        self.calls.lock().await.clone()
    }
}

struct YieldingAsyncBoaLoader;

#[async_trait(?Send)]
impl JsModuleLoader for YieldingAsyncBoaLoader {
    async fn resolve(
        &self,
        specifier: &str,
        referrer: Option<&str>,
    ) -> Result<JsResolvedModule, JsSubstrateError> {
        yield_now().await;
        let canonical = match (specifier, referrer) {
            ("terrace:/async-main.mjs", None) => "terrace:/async-main.mjs",
            ("./async-helper.mjs", Some("terrace:/async-main.mjs")) => "terrace:/async-helper.mjs",
            ("terrace:host/echo", Some("terrace:/async-main.mjs")) => "terrace:host/echo",
            _ => {
                return Err(JsSubstrateError::UnsupportedSpecifier {
                    specifier: specifier.to_string(),
                });
            }
        };
        Ok(JsResolvedModule {
            requested_specifier: specifier.to_string(),
            canonical_specifier: canonical.to_string(),
            kind: if canonical.starts_with("terrace:host/") {
                JsModuleKind::HostCapability
            } else {
                JsModuleKind::Workspace
            },
        })
    }

    async fn load(&self, resolved: &JsResolvedModule) -> Result<JsLoadedModule, JsSubstrateError> {
        sleep(Duration::from_millis(1)).await;
        let (source, metadata) = match resolved.canonical_specifier.as_str() {
            "terrace:/async-main.mjs" => (
                r#"
import helper from "./async-helper.mjs";
import { echo } from "terrace:host/echo";

const response = await echo({ message: helper.message });
export default { helper: helper.helper, echoed: response.echoed };
"#
                .to_string(),
                BTreeMap::new(),
            ),
            "terrace:/async-helper.mjs" => (
                r#"export default { helper: true, message: "from async loader" };"#.to_string(),
                BTreeMap::new(),
            ),
            "terrace:host/echo" => (
                String::new(),
                BTreeMap::from([
                    ("synthetic".to_string(), json!(true)),
                    ("host_service".to_string(), json!("terrace:host/echo")),
                    ("host_exports".to_string(), json!(["echo"])),
                ]),
            ),
            _ => {
                return Err(JsSubstrateError::ModuleNotFound {
                    specifier: resolved.canonical_specifier.clone(),
                });
            }
        };
        Ok(JsLoadedModule {
            resolved: resolved.clone(),
            source,
            trace: Vec::new(),
            metadata,
        })
    }
}

#[tokio::test]
async fn boa_runtime_executes_modules_and_capability_imports_over_frozen_interfaces() {
    let snapshot = seeded_snapshot().await;
    let loader = Arc::new(VfsJsModuleLoader::new(snapshot));
    let host_services = DeterministicJsHostServices::new();
    host_services
        .register_outcome(
            "capability",
            "echo",
            DeterministicJsServiceOutcome::Response {
                result: json!({"echoed":"from boa"}),
                metadata: BTreeMap::from([("kind".to_string(), json!("capability"))]),
            },
        )
        .await;
    let runtime = BoaJsRuntimeHost::new(loader, Arc::new(host_services))
        .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
        .with_clock(Arc::new(FixedJsClock::new(9_001)))
        .with_entropy(Arc::new(DeterministicJsEntropySource::new(0xface)))
        .open_runtime(JsRuntimeOpenRequest {
            runtime_id: "boa-runtime-1".to_string(),
            policy: JsRuntimePolicy {
                visible_host_services: vec!["capability::echo".to_string()],
                ..Default::default()
            },
            provenance: JsRuntimeProvenance {
                backend: "boa-js".to_string(),
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
        .expect("open boa runtime");
    let report = runtime
        .execute(
            JsExecutionRequest::module("terrace:/workspace/boa-main.mjs"),
            Arc::new(NeverCancel),
        )
        .await
        .expect("execute boa runtime");

    assert_eq!(
        report.result,
        Some(json!({
            "status": "ok",
            "helper": true,
            "echoed": "from boa",
            "now": 9_001,
            "console_type": "undefined",
            "process_type": "undefined"
        }))
    );
    assert_eq!(
        report.module_graph,
        vec![
            "terrace:/workspace/boa-main.mjs".to_string(),
            "terrace:/workspace/boa-helper.mjs".to_string(),
            "terrace:host/echo".to_string()
        ]
    );
    assert_eq!(report.host_calls.len(), 1);
    assert_eq!(
        report.host_calls[0].arguments,
        json!({"message":"from helper"})
    );
    assert!(
        report
            .scheduled_tasks
            .iter()
            .any(|task| matches!(task.queue, terracedb_js::JsTaskQueue::PromiseJobs))
    );
}

#[tokio::test]
async fn boa_runtime_async_bridge_waits_for_executor_driven_loaders_and_host_services() {
    let host_services = Arc::new(YieldingAsyncBoaHostServices::default());
    let runtime = BoaJsRuntimeHost::new(Arc::new(YieldingAsyncBoaLoader), host_services.clone())
        .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
        .with_clock(Arc::new(FixedJsClock::new(4_200)))
        .with_entropy(Arc::new(DeterministicJsEntropySource::new(0xabba)))
        .open_runtime(JsRuntimeOpenRequest {
            runtime_id: "boa-runtime-async-bridge".to_string(),
            policy: JsRuntimePolicy {
                visible_host_services: vec!["terrace:host/echo::echo".to_string()],
                ..Default::default()
            },
            provenance: JsRuntimeProvenance {
                backend: "boa-js".to_string(),
                host_model: "host-owned".to_string(),
                module_root: "/workspace".to_string(),
                volume_id: Some(VolumeId::new(0x9001)),
                snapshot_sequence: Some(1),
                durable_snapshot: false,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            metadata: BTreeMap::new(),
        })
        .await
        .expect("open async boa runtime");
    let report = runtime
        .execute(
            JsExecutionRequest::module("terrace:/async-main.mjs"),
            Arc::new(NeverCancel),
        )
        .await
        .expect("execute async boa runtime");

    assert_eq!(
        report.result,
        Some(json!({
            "helper": true,
            "echoed": "from async loader"
        }))
    );
    assert_eq!(
        report
            .module_graph
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            "terrace:/async-main.mjs".to_string(),
            "terrace:/async-helper.mjs".to_string(),
            "terrace:host/echo".to_string(),
        ])
    );
    assert_eq!(report.host_calls.len(), 1);
    assert_eq!(
        report.host_calls[0].arguments,
        json!({ "message": "from async loader" })
    );
    assert_eq!(host_services.calls().await.len(), 1);
}

#[tokio::test]
async fn boa_runtime_replaces_ambient_rng_and_denies_package_modules_by_default() {
    let snapshot = seeded_snapshot().await;
    let run_eval = |seed| {
        let snapshot = snapshot.clone();
        async move {
            let loader = Arc::new(VfsJsModuleLoader::with_package_modules(
                snapshot,
                BTreeMap::from([(
                    "npm:test".to_string(),
                    "export default {\"package\":true};".to_string(),
                )]),
            ));
            let runtime =
                BoaJsRuntimeHost::new(loader, Arc::new(DeterministicJsHostServices::new()))
                    .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
                    .with_clock(Arc::new(FixedJsClock::new(2_468)))
                    .with_entropy(Arc::new(DeterministicJsEntropySource::new(seed)))
                    .open_runtime(JsRuntimeOpenRequest {
                        runtime_id: format!("boa-runtime-seed-{seed}"),
                        policy: JsRuntimePolicy::default(),
                        provenance: JsRuntimeProvenance {
                            backend: "boa-js".to_string(),
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
                    .expect("open boa runtime");
            runtime
                .execute(
                    JsExecutionRequest::eval(
                        r#"
export default {
  "now": Date.now(),
  "randoms": [Math.random(), Math.random()],
  "console_type": typeof console,
  "process_type": typeof process
};
"#,
                    ),
                    Arc::new(NeverCancel),
                )
                .await
                .expect("execute boa eval")
        }
    };

    let first = run_eval(0x9abc).await;
    let second = run_eval(0x9abc).await;
    assert_eq!(first.result, second.result);
    assert_eq!(
        first.result,
        Some(json!({
            "now": 2_468,
            "randoms": first.result.as_ref().expect("result")["randoms"].clone(),
            "console_type": "undefined",
            "process_type": "undefined"
        }))
    );

    let loader = Arc::new(VfsJsModuleLoader::with_package_modules(
        seeded_snapshot().await,
        BTreeMap::from([(
            "npm:test".to_string(),
            "export default {\"package\":true};".to_string(),
        )]),
    ));
    let runtime = BoaJsRuntimeHost::new(loader, Arc::new(DeterministicJsHostServices::new()))
        .with_scheduler(Arc::new(DeterministicJsScheduler::default()))
        .with_clock(Arc::new(FixedJsClock::new(2_468)))
        .with_entropy(Arc::new(DeterministicJsEntropySource::new(0x9abc)))
        .open_runtime(JsRuntimeOpenRequest {
            runtime_id: "boa-runtime-package-denied".to_string(),
            policy: JsRuntimePolicy::default(),
            provenance: JsRuntimeProvenance {
                backend: "boa-js".to_string(),
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
        .expect("open boa runtime");
    let denied = runtime
        .execute(
            JsExecutionRequest::eval("import pkg from 'npm:test'; export default pkg;"),
            Arc::new(NeverCancel),
        )
        .await
        .expect_err("package modules should be denied until enabled");
    assert!(matches!(
        denied,
        JsSubstrateError::EvaluationFailed { ref message, .. }
            if message.contains("package module loading is disabled by policy")
    ));
}

#[tokio::test]
async fn vfs_loader_reads_overlay_modules_and_resolves_bare_packages() {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(77))),
        Arc::new(StubRng::seeded(123)),
    );
    let store = InMemoryVfsStore::with_dependencies(dependencies);
    let base = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x9100))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
    base.fs()
        .write_file(
            "/workspace/nested/helper.mjs",
            br#"export default {"version":"base"};"#.to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write base helper");

    let overlay = store
        .create_overlay_from_volume(
            base,
            SnapshotOptions::default(),
            VolumeConfig::new(VolumeId::new(0x9101))
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("create overlay");
    overlay
        .fs()
        .write_file(
            "/workspace/nested/helper.mjs",
            br#"export default {"version":"overlay"};"#.to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("overwrite helper in overlay");

    let snapshot = overlay
        .snapshot(SnapshotOptions::default())
        .await
        .expect("snapshot overlay");
    let loader = VfsJsModuleLoader::with_workspace_root_and_package_modules(
        snapshot,
        "/workspace",
        BTreeMap::from([(
            "npm:left-pad".to_string(),
            r#"export default {"package":true};"#.to_string(),
        )]),
    );

    let absolute = loader
        .resolve("/workspace/nested/helper.mjs", None)
        .await
        .expect("resolve absolute workspace path");
    assert_eq!(
        absolute.canonical_specifier,
        "terrace:/workspace/nested/helper.mjs"
    );
    assert_eq!(absolute.kind, JsModuleKind::Workspace);

    let relative = loader
        .resolve("./helper.mjs", Some("terrace:/workspace/nested/main.mjs"))
        .await
        .expect("resolve relative workspace path");
    assert_eq!(relative.canonical_specifier, absolute.canonical_specifier);
    assert_eq!(relative.kind, JsModuleKind::Workspace);
    assert!(matches!(
        loader
            .resolve("../../escape.mjs", Some("terrace:/workspace/nested/main.mjs"))
            .await,
        Err(JsSubstrateError::UnsupportedSpecifier { ref specifier })
            if specifier == "../../escape.mjs"
    ));

    let loaded = loader.load(&absolute).await.expect("load overlay helper");
    assert!(loaded.source.contains("overlay"));

    let bare_package = loader
        .resolve("left-pad", None)
        .await
        .expect("resolve bare package import");
    assert_eq!(bare_package.canonical_specifier, "npm:left-pad");
    assert_eq!(bare_package.kind, JsModuleKind::Package);
    assert!(
        loader
            .load(&bare_package)
            .await
            .expect("load package module")
            .source
            .contains("\"package\":true")
    );

    let denied = loader
        .resolve("/outside/helper.mjs", None)
        .await
        .expect_err("paths outside the workspace root should be rejected");
    assert!(matches!(
        denied,
        JsSubstrateError::UnsupportedSpecifier { ref specifier }
            if specifier == "/outside/helper.mjs"
    ));
    assert!(matches!(
        loader.resolve("terrace:/outside/helper.mjs", None).await,
        Err(JsSubstrateError::UnsupportedSpecifier { ref specifier })
            if specifier == "terrace:/outside/helper.mjs"
    ));
}
