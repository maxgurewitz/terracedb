use std::{collections::BTreeMap, sync::Arc};

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, PackageCompatibilityMode, SandboxConfig, SandboxError,
    SandboxServices, SandboxStore,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

#[path = "support/tracing.rs"]
mod tracing_support;

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

async fn open_node_session(
    now: u64,
    seed: u64,
    entrypoint: &str,
    source: &str,
) -> terracedb_sandbox::SandboxSession {
    tracing_support::init_tracing();
    let (vfs, sandbox) = sandbox_store(now, seed);
    let base_volume_id = VolumeId::new(0xA910u128 + (seed as u128 % 0x100));
    let session_volume_id = VolumeId::new(0xAA10u128 + (seed as u128 % 0x100));
    let base = vfs
        .open_volume(
            VolumeConfig::new(base_volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
    base.fs()
        .write_file(
            entrypoint,
            source.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write node fixture");

    sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmWithNodeBuiltins,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open node sandbox session")
}

async fn exec_node_fixture(
    source: &str,
) -> Result<terracedb_sandbox::SandboxExecutionResult, terracedb_sandbox::SandboxError> {
    let entrypoint = "/workspace/app/index.cjs";
    let session = open_node_session(510, 121, entrypoint, source).await;
    session
        .exec_node_command(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
}

#[tokio::test]
async fn node_builtin_missing_module_member_fails_clearly() {
    let error = exec_node_fixture(
        r#"
        const crypto = require("crypto");
        crypto.createHash("sha256");
        "#,
    )
    .await
    .expect_err("unsupported builtin member should fail clearly");

    match error {
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE_NODE_UNIMPLEMENTED"),
                "{message}"
            );
            assert!(message.contains("crypto.createHash"), "{message}");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn node_partial_builtin_missing_member_fails_clearly() {
    let error = exec_node_fixture(
        r#"
        const path = require("path");
        path.parse("/workspace/app/index.cjs");
        "#,
    )
    .await
    .expect_err("missing path.parse should fail clearly");

    match error {
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE_NODE_UNIMPLEMENTED"),
                "{message}"
            );
            assert!(message.contains("path.parse"), "{message}");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn node_timer_globals_fail_clearly_until_supported() {
    let error = exec_node_fixture("setTimeout(() => {}, 5);\n")
        .await
        .expect_err("timers should fail explicitly until deterministic semantics exist");

    match error {
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE_NODE_UNIMPLEMENTED"),
                "{message}"
            );
            assert!(message.contains("global.setTimeout"), "{message}");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn node_builtin_accesses_are_reported_for_implemented_members() {
    let result = exec_node_fixture(
        r#"
        const path = require("path");
        path.join("alpha", "beta");
        "#,
    )
    .await
    .expect("implemented builtin usage should succeed");

    let report = result.result.expect("node command report");
    let accesses = report["builtinAccesses"]
        .as_array()
        .expect("builtin access log");
    assert!(
        accesses.iter().any(|entry| {
            entry["builtin"].as_str() == Some("path")
                && entry["member"].as_str() == Some("join")
                && entry["status"].as_str() == Some("implemented")
        }),
        "expected a recorded path.join access, got: {accesses:#?}"
    );
}
