use std::sync::Arc;

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, PackageCompatibilityMode, SandboxConfig, SandboxError,
    SandboxServices, SandboxStore,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

fn sandbox_store(now: u64, seed: u64) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
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

async fn seed_base(store: &InMemoryVfsStore, volume_id: VolumeId, path: &str, source: &str) {
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
            path,
            source.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write fixture");
}

#[tokio::test]
async fn npm_cli_bootstrap_currently_needs_the_process_global() {
    let (vfs, sandbox) = sandbox_store(210, 111);
    let base_volume_id = VolumeId::new(0x9210);
    let session_volume_id = VolumeId::new(0x9211);
    seed_base(
        &vfs,
        base_volume_id,
        "/workspace/npm/process-check.mjs",
        "export default process.version;\n",
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmPureJs,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let error = session
        .exec_module("/workspace/npm/process-check.mjs")
        .await
        .expect_err("process should not be available yet");

    match error {
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("process"),
                "npm CLI should eventually be able to read process globals, got: {message}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn npm_cli_bootstrap_currently_needs_node_module_builtins() {
    let (vfs, sandbox) = sandbox_store(211, 112);
    let base_volume_id = VolumeId::new(0x9220);
    let session_volume_id = VolumeId::new(0x9221);
    seed_base(
        &vfs,
        base_volume_id,
        "/workspace/npm/node-module-check.mjs",
        "import { enableCompileCache } from \"node:module\";\nexport default Boolean(enableCompileCache);\n",
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmPureJs,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let error = session
        .exec_module("/workspace/npm/node-module-check.mjs")
        .await
        .expect_err("node:module should not be available yet");

    match error {
        SandboxError::Service { service, message } => {
            assert_eq!(service, "module_loader");
            assert!(
                message.contains("node builtin imports are disabled")
                    || message.contains("unsupported node builtin"),
                "npm CLI should eventually be able to import node:module, got: {message}"
            );
        }
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("module_loader backend error")
                    && message.contains("node builtin imports are disabled"),
                "npm CLI should eventually be able to import node:module, got: {message}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}
