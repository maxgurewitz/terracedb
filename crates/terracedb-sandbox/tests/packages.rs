use std::sync::Arc;

use serde_json::json;
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    ConflictPolicy, DefaultSandboxStore, DeterministicPackageInstaller, PackageCompatibilityMode,
    PackageInstallRequest, SandboxConfig, SandboxError, SandboxServices, SandboxStore,
    TERRACE_NPM_COMPATIBILITY_ROOT, read_package_install_manifest,
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

async fn seed_base(store: &InMemoryVfsStore, volume_id: VolumeId, main_source: &str) {
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
            "/workspace/main.js",
            main_source.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write main module");
}

fn session_config(
    base_volume_id: VolumeId,
    session_volume_id: VolumeId,
    package_compat: PackageCompatibilityMode,
) -> SandboxConfig {
    SandboxConfig {
        session_volume_id,
        session_chunk_size: Some(4096),
        base_volume_id,
        durable_base: false,
        workspace_root: "/workspace".to_string(),
        package_compat,
        conflict_policy: ConflictPolicy::Fail,
        capabilities: Default::default(),
        hoisted_source: None,
        git_provenance: None,
    }
}

#[tokio::test]
async fn installed_supported_packages_import_from_guest_code_and_record_package_trace() {
    let (vfs, sandbox) = sandbox_store(120, 61, SandboxServices::deterministic());
    let base_volume_id = VolumeId::new(0x9100);
    let session_volume_id = VolumeId::new(0x9101);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { camelCase } from "lodash";
        import { z } from "zod";

        const schema = z.object({
          petName: z.string(),
        });

        export default schema.parse({
          petName: camelCase("HELLO WORLD"),
        });
        "#,
    )
    .await;

    let session = sandbox
        .open_session(session_config(
            base_volume_id,
            session_volume_id,
            PackageCompatibilityMode::NpmPureJs,
        ))
        .await
        .expect("open session");
    let report = session
        .install_packages(PackageInstallRequest {
            packages: vec!["zod".to_string(), "lodash".to_string()],
            materialize_compatibility_view: true,
        })
        .await
        .expect("install packages");
    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute module");
    let manifest = read_package_install_manifest(session.filesystem().as_ref())
        .await
        .expect("read install manifest")
        .expect("manifest should exist");

    assert_eq!(report.packages, vec!["lodash", "zod"]);
    assert_eq!(
        report.compatibility_root,
        Some(TERRACE_NPM_COMPATIBILITY_ROOT.to_string())
    );
    assert_eq!(
        result.result,
        Some(json!({
            "petName": "helloWorld"
        }))
    );
    assert!(
        result.package_imports.contains(&"lodash".to_string()),
        "package trace should include lodash"
    );
    assert!(
        result.package_imports.contains(&"zod".to_string()),
        "package trace should include zod"
    );
    assert!(manifest.materialized_compatibility_view);
    assert_eq!(manifest.packages.len(), 2);
    assert!(
        session
            .filesystem()
            .read_file("/.terrace/npm/node_modules/lodash/package.json")
            .await
            .expect("read compatibility package.json")
            .is_some()
    );
}

#[tokio::test]
async fn repeated_installs_reuse_shared_cache_state_across_sessions() {
    let installer = Arc::new(DeterministicPackageInstaller::default());
    let mut services = SandboxServices::deterministic();
    services.packages = installer;
    let (vfs, sandbox) = sandbox_store(130, 62, services);
    let base_volume_id = VolumeId::new(0x9110);
    let first_session_volume_id = VolumeId::new(0x9111);
    let second_session_volume_id = VolumeId::new(0x9112);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { camelCase } from "lodash";
        export default camelCase("repeat install");
        "#,
    )
    .await;

    let first = sandbox
        .open_session(session_config(
            base_volume_id,
            first_session_volume_id,
            PackageCompatibilityMode::NpmPureJs,
        ))
        .await
        .expect("open first session");
    let first_report = first
        .install_packages(PackageInstallRequest {
            packages: vec!["lodash".to_string(), "zod".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect("first install");

    let second = sandbox
        .open_session(session_config(
            base_volume_id,
            second_session_volume_id,
            PackageCompatibilityMode::NpmPureJs,
        ))
        .await
        .expect("open second session");
    let second_report = second
        .install_packages(PackageInstallRequest {
            packages: vec!["lodash".to_string(), "zod".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect("second install");

    assert_eq!(first_report.metadata["cache_hits"], json!([]));
    assert_eq!(
        first_report.metadata["cache_misses"],
        json!(["lodash", "zod"])
    );
    assert_eq!(
        second_report.metadata["cache_hits"],
        json!(["lodash", "zod"])
    );
    assert_eq!(second_report.metadata["cache_misses"], json!([]));
}

#[tokio::test]
async fn compatibility_modes_and_supported_surface_fail_predictably() {
    let (vfs, sandbox) = sandbox_store(140, 63, SandboxServices::deterministic());
    let base_volume_id = VolumeId::new(0x9120);
    let pure_session_volume_id = VolumeId::new(0x9121);
    let builtins_session_volume_id = VolumeId::new(0x9122);
    let subpath_session_volume_id = VolumeId::new(0x9123);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { writeTextFile } from "@terracedb/sandbox/fs";
        import { read } from "node-reader";

        writeTextFile("/workspace/data.txt", "node builtins ok");
        export default read("/workspace/data.txt");
        "#,
    )
    .await;

    let pure_session = sandbox
        .open_session(session_config(
            base_volume_id,
            pure_session_volume_id,
            PackageCompatibilityMode::NpmPureJs,
        ))
        .await
        .expect("open pure session");
    let pure_error = pure_session
        .install_packages(PackageInstallRequest {
            packages: vec!["node-reader".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect_err("node-reader should require builtin mode");
    assert!(matches!(
        pure_error,
        SandboxError::UnsupportedPackage { ref package, ref reason }
            if package == "node-reader" && reason.contains("requires node builtins")
    ));

    let builtin_session = sandbox
        .open_session(session_config(
            base_volume_id,
            builtins_session_volume_id,
            PackageCompatibilityMode::NpmWithNodeBuiltins,
        ))
        .await
        .expect("open builtin session");
    builtin_session
        .install_packages(PackageInstallRequest {
            packages: vec!["node-reader".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect("install node-reader");
    let builtin_result = builtin_session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute builtin-compatible module");
    assert_eq!(builtin_result.result, Some(json!("node builtins ok")));
    assert!(
        builtin_result
            .module_graph
            .contains(&"node:fs/promises".to_string())
    );
    assert_eq!(
        builtin_result.package_imports,
        vec!["node-reader".to_string()]
    );

    let unsupported_error = builtin_session
        .install_packages(PackageInstallRequest {
            packages: vec!["native-addon".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect_err("native addons should be rejected");
    assert!(matches!(
        unsupported_error,
        SandboxError::UnsupportedPackage { ref package, ref reason }
            if package == "native-addon" && reason.contains("native addons")
    ));

    let subpath_session = sandbox
        .open_session(session_config(
            base_volume_id,
            subpath_session_volume_id,
            PackageCompatibilityMode::NpmPureJs,
        ))
        .await
        .expect("open subpath session");
    let loader = subpath_session.module_loader().await;
    let subpath_error = loader
        .load("lodash/fp", None)
        .await
        .expect_err("npm subpaths should be rejected in version 1");
    assert!(matches!(
        subpath_error,
        SandboxError::UnsupportedPackage { ref package, ref reason }
            if package == "lodash/fp" && reason.contains("subpath imports")
    ));
}
