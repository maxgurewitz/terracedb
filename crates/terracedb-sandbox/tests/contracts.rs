use std::sync::Arc;

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    BashRequest, BashService, CapabilityManifest, CapabilityRegistry, ConflictPolicy,
    DefaultSandboxStore, DeterministicBashService, DeterministicTypeScriptService, GitProvenance,
    HoistMode, HoistedSource, PackageCompatibilityMode, ReadonlyViewCut, ReadonlyViewHandle,
    ReadonlyViewLocation, ReadonlyViewRequest, ReopenSessionOptions, SandboxCapability,
    SandboxConfig, SandboxServices, SandboxSessionInfo, SandboxSessionProvenance, SandboxStore,
    StaticCapabilityRegistry, TypeCheckRequest, TypeScriptService,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

fn test_store(now: u64, seed: u64) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(
        Arc::new(vfs.clone()),
        dependencies.clock,
        SandboxServices::deterministic(),
    );
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
                roots: vec!["/workspace/readme.ts".to_string()]
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
        "pwd\n"
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
