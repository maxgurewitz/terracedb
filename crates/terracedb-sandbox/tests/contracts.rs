use std::{
    collections::BTreeMap,
    future::poll_fn,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

use async_trait::async_trait;
use serde_json::json;
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_js::{JsModuleKind, JsModuleLoader};
use terracedb_sandbox::{
    BashRequest, BashService, CapabilityCallRequest, CapabilityCallResult, CapabilityManifest,
    CapabilityRegistry, ConflictPolicy, DefaultSandboxStore, DeterministicBashService,
    DeterministicCapabilityModule, DeterministicCapabilityRegistry, DeterministicRuntimeBackend,
    DeterministicTypeScriptService, GitProvenance, HoistMode, HoistedSource,
    PackageCompatibilityMode, PackageInstallRequest, ReadonlyViewCut, ReadonlyViewHandle,
    ReadonlyViewLocation, ReadonlyViewRequest, ReopenSessionOptions, SandboxCapability,
    SandboxCapabilityMethod, SandboxCapabilityModule, SandboxConfig, SandboxError,
    SandboxExecutionKind, SandboxExecutionRequest, SandboxRuntimeBackend,
    SandboxRuntimeStateHandle, SandboxServices, SandboxSessionInfo, SandboxSessionProvenance,
    SandboxStore, StaticCapabilityRegistry, TERRACE_RUNTIME_MODULE_CACHE_PATH, TypeCheckRequest,
    TypeScriptService,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

fn test_store(now: u64, seed: u64) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    test_store_with_services(now, seed, SandboxServices::deterministic())
}

fn test_store_with_services(
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

async fn seed_base_volume(store: &InMemoryVfsStore, volume_id: VolumeId) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
    base.fs()
        .write_file(
            "/workspace/readme.txt",
            b"hello sandbox".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed base workspace");
}

async fn yield_once(flag: &AtomicBool) {
    poll_fn(|context| {
        if flag.swap(true, Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

#[derive(Clone)]
struct YieldingCapabilityRegistry {
    capability: SandboxCapability,
    yielded: Arc<AtomicBool>,
}

impl YieldingCapabilityRegistry {
    fn new() -> Self {
        Self {
            capability: SandboxCapability::host_module("tickets"),
            yielded: Arc::new(AtomicBool::new(false)),
        }
    }

    fn module_surface(&self) -> SandboxCapabilityModule {
        SandboxCapabilityModule {
            capability: self.capability.clone(),
            methods: vec![SandboxCapabilityMethod::new("echo")],
            metadata: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl CapabilityRegistry for YieldingCapabilityRegistry {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            capabilities: vec![self.capability.clone()],
        }
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        (self.capability.specifier.as_str() == specifier).then(|| self.capability.clone())
    }

    fn module(&self, specifier: &str) -> Option<SandboxCapabilityModule> {
        (self.capability.specifier.as_str() == specifier).then(|| self.module_surface())
    }

    async fn invoke(
        &self,
        _session: &terracedb_sandbox::SandboxSession,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        yield_once(self.yielded.as_ref()).await;
        Ok(CapabilityCallResult {
            specifier: request.specifier,
            method: request.method,
            value: json!({
                "ok": true,
                "args": request.args,
            }),
            metadata: BTreeMap::new(),
        })
    }
}

#[tokio::test]
async fn public_sandbox_surface_compiles_and_is_instantiable() {
    let (vfs, sandbox_store) = test_store(10, 41);
    let base_volume_id = VolumeId::new(0x5000);
    let session_volume_id = VolumeId::new(0x5001);
    seed_base_volume(&vfs, base_volume_id).await;

    let capabilities = CapabilityManifest {
        capabilities: vec![SandboxCapability::host_module("tickets")],
    };
    let registry = StaticCapabilityRegistry::new(capabilities.clone()).expect("valid registry");
    assert!(registry.resolve("terrace:host/tickets").is_some());

    let session = sandbox_store
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmPureJs,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: capabilities.clone(),
            execution_policy: None,
            hoisted_source: Some(HoistedSource {
                source_path: "/repo".to_string(),
                mode: HoistMode::GitHead,
            }),
            git_provenance: Some(GitProvenance {
                repo_root: "/repo".to_string(),
                head_commit: Some("abc123".to_string()),
                branch: Some("main".to_string()),
                remote_url: Some("git@example.invalid:terrace/repo.git".to_string()),
                pathspec: vec![".".to_string()],
                dirty: false,
            }),
        })
        .await
        .expect("open sandbox session");

    assert_eq!(
        session
            .filesystem()
            .read_file("/workspace/readme.txt")
            .await
            .expect("read through shim"),
        Some(b"hello sandbox".to_vec())
    );
    assert_eq!(
        terracedb_sandbox::SandboxModuleSpecifier::parse("terrace:/workspace/readme.txt")
            .expect("workspace specifier"),
        terracedb_sandbox::SandboxModuleSpecifier::Workspace {
            path: "/workspace/readme.txt".to_string()
        }
    );
    assert_eq!(
        terracedb_sandbox::SandboxModuleSpecifier::parse("terrace:host/tickets")
            .expect("host specifier"),
        terracedb_sandbox::SandboxModuleSpecifier::HostCapability {
            name: "tickets".to_string()
        }
    );

    let package_report = session
        .install_packages(terracedb_sandbox::PackageInstallRequest {
            packages: vec!["zod".to_string(), "lodash".to_string()],
            materialize_compatibility_view: true,
        })
        .await
        .expect("install packages");
    assert_eq!(package_report.packages, vec!["lodash", "zod"]);

    let git_report = session
        .prepare_git_workspace(terracedb_sandbox::GitWorkspaceRequest {
            branch_name: "sandbox/feature".to_string(),
            base_branch: Some("main".to_string()),
            target_path: "/tmp/export".to_string(),
        })
        .await
        .expect("prepare git workspace");
    assert_eq!(git_report.workspace_path, "/tmp/export");

    let pr_report = session
        .create_pull_request(terracedb_sandbox::PullRequestRequest {
            title: "Sandbox PR".to_string(),
            body: "Body".to_string(),
            head_branch: "sandbox/feature".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request");
    assert!(pr_report.url.contains("example.invalid"));

    let view = session
        .open_readonly_view(ReadonlyViewRequest {
            cut: ReadonlyViewCut::Visible,
            path: "/workspace".to_string(),
            label: Some("workspace".to_string()),
        })
        .await
        .expect("open readonly view");
    assert_eq!(view.location.path, "/workspace");

    let ts = DeterministicTypeScriptService::default();
    assert!(
        ts.check(
            &session,
            TypeCheckRequest {
                roots: vec!["/workspace/readme.txt".to_string()],
                ..Default::default()
            }
        )
        .await
        .expect("check types")
        .diagnostics
        .is_empty()
    );
    let bash = DeterministicBashService::default();
    assert_eq!(
        bash.run(
            &session,
            BashRequest {
                command: "pwd".to_string(),
                cwd: "/workspace".to_string(),
                ..Default::default()
            }
        )
        .await
        .expect("run bash")
        .stdout,
        "/workspace\n"
    );

    let reopened = sandbox_store
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen session");
    assert_eq!(
        reopened.info().await.provenance.capabilities,
        registry.manifest()
    );
}

#[test]
fn metadata_and_view_uri_round_trip() {
    let handle = ReadonlyViewHandle {
        handle_id: "view-1".to_string(),
        provider: "deterministic-view".to_string(),
        label: "workspace".to_string(),
        location: ReadonlyViewLocation {
            session_volume_id: VolumeId::new(0x6001),
            cut: ReadonlyViewCut::Durable,
            path: "/workspace/readme.txt".to_string(),
        },
        uri: "terrace-view://session/00000000000000000000000000006001/durable/workspace/readme.txt"
            .to_string(),
        opened_at: Timestamp::new(44),
        metadata: Default::default(),
    };
    let provenance = SandboxSessionProvenance {
        base_snapshot: terracedb_sandbox::BaseSnapshotIdentity {
            volume_id: VolumeId::new(0x6000),
            sequence: terracedb::SequenceNumber::new(7),
            durable: true,
        },
        hoisted_source: Some(HoistedSource {
            source_path: "/repo".to_string(),
            mode: HoistMode::GitWorkingTree {
                include_untracked: true,
                include_ignored: false,
            },
        }),
        git: Some(GitProvenance {
            repo_root: "/repo".to_string(),
            head_commit: Some("deadbeef".to_string()),
            branch: Some("feature".to_string()),
            remote_url: Some("git@example.invalid:terrace/repo.git".to_string()),
            pathspec: vec!["src".to_string()],
            dirty: true,
        }),
        package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
        capabilities: CapabilityManifest {
            capabilities: vec![SandboxCapability::host_module("tickets")],
        },
        execution_policy: None,
        active_view_handles: vec![handle.clone()],
    };
    let info = SandboxSessionInfo {
        format_version: terracedb_sandbox::SANDBOX_SESSION_FORMAT_VERSION,
        revision: 3,
        session_volume_id: VolumeId::new(0x6001),
        workspace_root: "/workspace".to_string(),
        state: terracedb_sandbox::SandboxSessionState::Open,
        created_at: Timestamp::new(10),
        updated_at: Timestamp::new(20),
        closed_at: None,
        conflict_policy: ConflictPolicy::CreatePatchBundle,
        services: terracedb_sandbox::SandboxServiceBindings {
            runtime_backend: "deterministic-runtime".to_string(),
            package_installer: "deterministic-packages".to_string(),
            git_workspace_manager: "deterministic-git".to_string(),
            pull_request_provider: "deterministic-pr".to_string(),
            readonly_view_provider: "deterministic-view".to_string(),
            typescript_service: "deterministic-typescript".to_string(),
            bash_service: "deterministic-bash".to_string(),
            execution_bindings: Default::default(),
        },
        provenance,
    };

    let encoded = serde_json::to_vec(&info).expect("encode info");
    let decoded: SandboxSessionInfo = serde_json::from_slice(&encoded).expect("decode info");
    assert_eq!(decoded, info);

    let policy =
        serde_json::to_string(&ConflictPolicy::ApplyNonConflicting).expect("encode policy");
    let decoded_policy: ConflictPolicy = serde_json::from_str(&policy).expect("decode policy");
    assert_eq!(decoded_policy, ConflictPolicy::ApplyNonConflicting);

    let parsed = ReadonlyViewLocation::parse(&handle.location.to_uri()).expect("parse uri");
    assert_eq!(parsed, handle.location);
}

#[tokio::test]
async fn module_loader_describes_runtime_services_and_packages() {
    let capabilities = DeterministicCapabilityRegistry::new(vec![
        DeterministicCapabilityModule::new(SandboxCapability::host_module("tickets"))
            .expect("valid capability")
            .with_echo_method("echo"),
    ])
    .expect("registry");
    let (vfs, sandbox_store) = test_store_with_services(
        20,
        42,
        SandboxServices::deterministic().with_capabilities(Arc::new(capabilities.clone())),
    );
    let base_volume_id = VolumeId::new(0x5010);
    let session_volume_id = VolumeId::new(0x5011);
    seed_base_volume(&vfs, base_volume_id).await;

    let session = sandbox_store
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: capabilities.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");
    session
        .install_packages(PackageInstallRequest {
            packages: vec!["lodash".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect("install package");

    let loader = session.module_loader().await;

    let capability_resolved = JsModuleLoader::resolve(&loader, "terrace:host/tickets", None)
        .await
        .expect("resolve capability");
    assert_eq!(capability_resolved.kind, JsModuleKind::HostCapability);
    let capability_loaded = JsModuleLoader::load(&loader, &capability_resolved)
        .await
        .expect("load capability");
    assert_eq!(
        capability_loaded.metadata["host_service"],
        json!("terrace:host/tickets")
    );
    assert_eq!(capability_loaded.metadata["host_exports"], json!(["echo"]));

    let fs_resolved = JsModuleLoader::resolve(&loader, "@terracedb/sandbox/fs", None)
        .await
        .expect("resolve fs library");
    assert_eq!(fs_resolved.kind, JsModuleKind::HostCapability);
    let fs_loaded = JsModuleLoader::load(&loader, &fs_resolved)
        .await
        .expect("load fs library");
    assert_eq!(
        fs_loaded.metadata["host_service"],
        json!("@terracedb/sandbox/fs")
    );
    assert!(
        fs_loaded.metadata["host_exports"]
            .as_array()
            .expect("fs host exports")
            .iter()
            .any(|value| value == "readTextFile")
    );

    let node_resolved = JsModuleLoader::resolve(&loader, "node:fs/promises", None)
        .await
        .expect("resolve node builtin");
    assert_eq!(node_resolved.kind, JsModuleKind::HostCapability);
    let node_loaded = JsModuleLoader::load(&loader, &node_resolved)
        .await
        .expect("load node builtin");
    assert_eq!(
        node_loaded.metadata["host_service"],
        json!("node:fs/promises")
    );

    let package_resolved = JsModuleLoader::resolve(&loader, "lodash", None)
        .await
        .expect("resolve package");
    assert_eq!(package_resolved.kind, JsModuleKind::Package);
    let package_loaded = JsModuleLoader::load(&loader, &package_resolved)
        .await
        .expect("load package");
    assert!(package_loaded.source.contains("camelCase"));
}

#[tokio::test]
async fn runtime_backend_waits_for_capability_calls_that_yield_once() {
    let capabilities = YieldingCapabilityRegistry::new();
    let (vfs, sandbox_store) = test_store_with_services(
        30,
        43,
        SandboxServices::deterministic().with_capabilities(Arc::new(capabilities.clone())),
    );
    let base_volume_id = VolumeId::new(0x501a);
    let session_volume_id = VolumeId::new(0x501b);
    seed_base_volume(&vfs, base_volume_id).await;

    let session = sandbox_store
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: capabilities.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let backend = DeterministicRuntimeBackend::default();
    let handle = backend
        .start_session(&session.info().await)
        .await
        .expect("start runtime session");
    let report = backend
        .execute(
            &session,
            &handle,
            SandboxExecutionRequest {
                kind: SandboxExecutionKind::Eval {
                    source: r#"
                        import { echo } from "terrace:host/tickets";
                        export default await echo({ value: "hello" });
                    "#
                    .to_string(),
                    virtual_specifier: Some(
                        "terrace:/workspace/.terrace/runtime/yielding-capability.mjs".to_string(),
                    ),
                },
                metadata: Default::default(),
            },
            SandboxRuntimeStateHandle::new(),
        )
        .await
        .expect("execute runtime eval");

    assert_eq!(
        report.result,
        Some(json!({
            "ok": true,
            "args": [{ "value": "hello" }]
        }))
    );
    assert_eq!(report.capability_calls.len(), 1);
    assert_eq!(report.capability_calls[0].specifier, "terrace:host/tickets");
    assert_eq!(report.capability_calls[0].method, "echo");
}

#[tokio::test]
async fn runtime_backend_reuses_inline_eval_cache_for_stable_virtual_specifiers() {
    let capabilities = DeterministicCapabilityRegistry::new(vec![
        DeterministicCapabilityModule::new(SandboxCapability::host_module("tickets"))
            .expect("valid capability")
            .with_echo_method("echo"),
    ])
    .expect("registry");
    let (vfs, sandbox_store) = test_store_with_services(
        30,
        43,
        SandboxServices::deterministic().with_capabilities(Arc::new(capabilities.clone())),
    );
    let base_volume_id = VolumeId::new(0x5020);
    let session_volume_id = VolumeId::new(0x5021);
    seed_base_volume(&vfs, base_volume_id).await;

    let session = sandbox_store
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: capabilities.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let backend = DeterministicRuntimeBackend::default();
    let handle = backend
        .start_session(&session.info().await)
        .await
        .expect("start runtime session");
    let state = SandboxRuntimeStateHandle::new();
    let request = SandboxExecutionRequest {
        kind: SandboxExecutionKind::Eval {
            source: r#"
                import { readTextFile } from "@terracedb/sandbox/fs";
                import { echo } from "terrace:host/tickets";
                export default await echo({
                    value: await readTextFile("/workspace/readme.txt")
                });
            "#
            .to_string(),
            virtual_specifier: Some("terrace:/workspace/.terrace/runtime/reused.mjs".to_string()),
        },
        metadata: Default::default(),
    };

    let first = backend
        .execute(&session, &handle, request.clone(), state.clone())
        .await
        .expect("first runtime eval");
    let second = backend
        .execute(&session, &handle, request, state)
        .await
        .expect("second runtime eval");

    assert_eq!(
        first.result,
        Some(json!({
            "specifier": "terrace:host/tickets",
            "method": "echo",
            "args": [{ "value": "hello sandbox" }]
        }))
    );
    assert!(
        first
            .cache_misses
            .contains(&"terrace:/workspace/.terrace/runtime/reused.mjs".to_string())
    );
    assert!(
        second
            .cache_hits
            .contains(&"terrace:/workspace/.terrace/runtime/reused.mjs".to_string())
    );
    assert_eq!(second.capability_calls.len(), 1);

    let runtime_cache = session
        .filesystem()
        .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
        .await
        .expect("read runtime cache")
        .expect("runtime cache manifest should exist");
    let specifiers = serde_json::from_slice::<serde_json::Value>(&runtime_cache)
        .expect("decode runtime cache manifest")
        .as_array()
        .expect("cache entries array")
        .iter()
        .map(|entry| {
            entry["specifier"]
                .as_str()
                .expect("cache specifier")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert!(specifiers.contains(&"terrace:/workspace/.terrace/runtime/reused.mjs".to_string()));
    assert!(specifiers.contains(&"@terracedb/sandbox/fs".to_string()));
    assert!(specifiers.contains(&"terrace:host/tickets".to_string()));
}
