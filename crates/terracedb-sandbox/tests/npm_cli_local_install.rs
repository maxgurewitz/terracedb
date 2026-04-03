#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

use terracedb_vfs::CreateOptions;

#[tokio::test]
async fn npm_cli_local_install_materializes_local_dependency() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(420, 93).await else {
        eprintln!("skipping npm cli local install test because npm/cli repo is unavailable");
        return;
    };

    session
        .filesystem()
        .write_file(
            "/workspace/project/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "project",
                "version": "1.0.0"
            }))
            .expect("encode root package.json"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write project package.json");

    session
        .filesystem()
        .write_file(
            "/workspace/project/local-dep/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "local-dep",
                "version": "1.2.3",
                "main": "index.js"
            }))
            .expect("encode local dependency package.json"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write local dependency manifest");
    session
        .filesystem()
        .write_file(
            "/workspace/project/local-dep/index.js",
            b"module.exports = 'local-dep';\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write local dependency index");

    let result = npm_cli::run_npm_command(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        &[
            "npm",
            "install",
            "./local-dep",
            "--loglevel=silly",
            "--ignore-scripts",
            "--no-audit",
            "--no-fund",
        ],
    )
    .await
    .expect("npm install should complete for a local file dependency");

    let report = result.result.clone().expect("node command report");
    assert_eq!(
        report["exitCode"].as_i64(),
        Some(0),
        "expected npm to exit cleanly, got report: {report:#?}"
    );
    assert!(
        result
            .module_graph
            .iter()
            .any(|path| path.ends_with("/lib/commands/install.js")),
        "expected npm to load the install command, got module graph: {:#?}",
        result.module_graph
    );

    let installed_manifest = session
        .filesystem()
        .read_file("/workspace/project/node_modules/local-dep/package.json")
        .await
        .expect("read installed dependency manifest");
    let project_manifest = session
        .filesystem()
        .read_file("/workspace/project/package.json")
        .await
        .expect("read project package.json after install");
    let package_lock_after = session
        .filesystem()
        .read_file("/workspace/project/package-lock.json")
        .await
        .expect("read package-lock after install");
    let node_modules_entries = session
        .filesystem()
        .readdir("/workspace/project/node_modules")
        .await
        .ok()
        .map(|entries| {
            entries
                .into_iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let local_dep_lstat = session
        .filesystem()
        .lstat("/workspace/project/node_modules/local-dep")
        .await
        .ok()
        .flatten()
        .map(|stats| format!("{:?}", stats.kind))
        .unwrap_or_default();
    let local_dep_readlink = session
        .filesystem()
        .readlink("/workspace/project/node_modules/local-dep")
        .await
        .ok();
    let local_dep_children = session
        .filesystem()
        .readdir("/workspace/project/node_modules/local-dep")
        .await
        .ok()
        .map(|entries| {
            entries
                .into_iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert!(
        installed_manifest.is_some(),
        "expected node_modules/local-dep/package.json to exist after install\nstdout:\n{}\n\nstderr:\n{}\n\nproject package.json:\n{}\n\npackage-lock:\n{}\n\nnode_modules entries: {:#?}\nlocal-dep lstat kind: {}\nlocal-dep readlink: {:?}\nlocal-dep children: {:#?}\n\nnode trace: {:#?}\nmetadata: {:#?}\nreport: {report:#?}",
        report["stdout"].as_str().unwrap_or_default(),
        report["stderr"].as_str().unwrap_or_default(),
        project_manifest
            .as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_else(|| "".into()),
        package_lock_after
            .as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_else(|| "".into()),
        node_modules_entries,
        local_dep_lstat,
        local_dep_readlink,
        local_dep_children,
        npm_cli::node_runtime_trace(&result),
        result.metadata
    );
    let installed_manifest =
        installed_manifest.expect("installed dependency manifest should exist");
    let installed_manifest: serde_json::Value =
        serde_json::from_slice(&installed_manifest).expect("decode installed dependency manifest");
    assert_eq!(
        installed_manifest["name"].as_str(),
        Some("local-dep"),
        "expected installed dependency manifest to round-trip, got: {installed_manifest:#?}"
    );

    let saved_manifest = session
        .filesystem()
        .read_file("/workspace/project/package.json")
        .await
        .expect("read updated root package.json")
        .expect("updated root package.json should exist");
    let saved_manifest: serde_json::Value =
        serde_json::from_slice(&saved_manifest).expect("decode updated root package.json");
    assert_eq!(
        saved_manifest["dependencies"]["local-dep"].as_str(),
        Some("file:local-dep"),
        "expected npm to save the local dependency into package.json, got: {saved_manifest:#?}"
    );

    let installed_index = session
        .filesystem()
        .read_file("/workspace/project/node_modules/local-dep/index.js")
        .await
        .expect("read installed dependency index")
        .expect("installed dependency index should exist");
    assert!(
        String::from_utf8_lossy(&installed_index).contains("local-dep"),
        "expected installed dependency source to be present"
    );

    let package_lock = session
        .filesystem()
        .read_file("/workspace/project/package-lock.json")
        .await
        .expect("read package-lock")
        .expect("package-lock should exist");
    let package_lock: serde_json::Value =
        serde_json::from_slice(&package_lock).expect("decode package-lock");
    assert_eq!(
        package_lock["packages"]["node_modules/local-dep"]["link"].as_bool(),
        Some(true),
        "expected package-lock to record the local dependency as a link, got: {package_lock:#?}"
    );
    assert_eq!(
        package_lock["packages"]["node_modules/local-dep"]["resolved"].as_str(),
        Some("local-dep"),
        "expected package-lock to record the link target, got: {package_lock:#?}"
    );
    assert_eq!(
        package_lock["packages"]["local-dep"]["version"].as_str(),
        Some("1.2.3"),
        "expected package-lock to retain the linked package version metadata, got: {package_lock:#?}"
    );
}
