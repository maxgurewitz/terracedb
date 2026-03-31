use std::sync::Arc;

use serde_json::json;
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    CapabilityRegistry, ConflictPolicy, DefaultSandboxStore, DeterministicCapabilityModule,
    DeterministicCapabilityRegistry, PackageCompatibilityMode, SandboxCapability, SandboxConfig,
    SandboxError, SandboxServices, SandboxStore, TERRACE_RUNTIME_MODULE_CACHE_PATH,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

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

async fn seed_base(
    store: &InMemoryVfsStore,
    volume_id: VolumeId,
    main_source: &str,
    extra_files: &[(&str, &str)],
) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
    let fs = base.fs();
    fs.write_file(
        "/workspace/main.js",
        main_source.as_bytes().to_vec(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await
    .expect("write main module");
    for (path, contents) in extra_files {
        fs.write_file(
            path,
            contents.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write extra file");
    }
}

fn deterministic_capabilities() -> DeterministicCapabilityRegistry {
    DeterministicCapabilityRegistry::new(vec![
        DeterministicCapabilityModule::new(SandboxCapability::host_module("tickets"))
            .expect("valid capability")
            .with_echo_method("echo"),
    ])
    .expect("build registry")
}

#[tokio::test]
async fn guest_modules_read_write_vfs_and_call_explicit_capabilities() {
    let registry = deterministic_capabilities();
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(100, 51, services);
    let base_volume_id = VolumeId::new(0x9000);
    let session_volume_id = VolumeId::new(0x9001);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { readTextFile, writeTextFile } from "@terracedb/sandbox/fs";
        import { echo } from "terrace:host/tickets";

        const input = readTextFile("/workspace/input.txt");
        writeTextFile("/workspace/output.txt", `${input} + runtime`);

        export default echo({
          text: readTextFile("/workspace/output.txt"),
        });
        "#,
        &[("/workspace/input.txt", "hello sandbox")],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute module");

    assert_eq!(
        session
            .filesystem()
            .read_file("/workspace/output.txt")
            .await
            .expect("read output file"),
        Some(b"hello sandbox + runtime".to_vec())
    );
    assert_eq!(
        result.result,
        Some(json!({
            "specifier": "terrace:host/tickets",
            "method": "echo",
            "args": [{
                "text": "hello sandbox + runtime"
            }]
        }))
    );
    assert!(
        result
            .module_graph
            .contains(&"terrace:/workspace/main.js".to_string())
    );
    assert!(
        result
            .module_graph
            .contains(&"@terracedb/sandbox/fs".to_string())
    );
    assert!(
        result
            .module_graph
            .contains(&"terrace:host/tickets".to_string())
    );
    assert_eq!(result.capability_calls.len(), 1);

    let tool_names = session
        .volume()
        .tools()
        .recent(None)
        .await
        .expect("recent tool runs")
        .into_iter()
        .map(|run| run.name)
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"sandbox.runtime.exec_module".to_string()));
    assert!(tool_names.contains(&"host_api.tickets.echo".to_string()));

    let runtime_cache = session
        .filesystem()
        .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
        .await
        .expect("read runtime cache file")
        .expect("runtime cache should exist after execution");
    let cached: serde_json::Value =
        serde_json::from_slice(&runtime_cache).expect("decode runtime cache manifest");
    assert!(
        cached
            .as_array()
            .expect("cache entries array")
            .iter()
            .any(|entry| entry["specifier"] == "terrace:/workspace/main.js")
    );
}

#[tokio::test]
async fn denied_capabilities_fail_predictably_and_no_runtime_helpers_are_ambient() {
    let registry = deterministic_capabilities();
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(110, 52, services);
    let base_volume_id = VolumeId::new(0x9010);
    let session_volume_id = VolumeId::new(0x9011);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { echo } from "terrace:host/admin";
        export default echo({ denied: true });
        "#,
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let loader = session.module_loader().await;
    let denied = loader
        .load("terrace:host/admin", None)
        .await
        .expect_err("admin capability should be denied");
    assert!(matches!(
        denied,
        SandboxError::CapabilityDenied { ref specifier } if specifier == "terrace:host/admin"
    ));

    let runtime_error = session
        .exec_module("/workspace/main.js")
        .await
        .expect_err("denied capability module should fail");
    assert!(matches!(
        runtime_error,
        SandboxError::Execution { ref message, .. }
            if message.contains("sandbox capability is not allowed in this session")
    ));

    let globals = session
        .eval(
            r#"
            export default {
              fs: typeof readTextFile,
              host: typeof __terrace_capability_call,
            };
            "#,
        )
        .await
        .expect("evaluate globals check");
    assert_eq!(
        globals.result,
        Some(json!({
            "fs": "undefined",
            "host": "undefined"
        }))
    );
}
