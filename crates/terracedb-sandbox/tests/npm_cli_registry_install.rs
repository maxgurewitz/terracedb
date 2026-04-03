#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

use terracedb_vfs::CreateOptions;

#[tokio::test]
async fn npm_cli_registry_install_materializes_lodash() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(440, 104).await else {
        eprintln!("skipping npm cli registry install test because npm/cli repo is unavailable");
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

    let result = npm_cli::run_npm_command(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        &[
            "npm",
            "install",
            "lodash",
            "--ignore-scripts",
            "--no-audit",
            "--no-fund",
            "--loglevel=warn",
        ],
    )
    .await
    .expect("npm install lodash should execute deterministically");

    let report = result.result.clone().expect("node command report");
    assert_eq!(
        report["exitCode"].as_i64(),
        Some(0),
        "expected npm install lodash to exit cleanly, got report: {report:#?}\nnode trace: {:#?}",
        npm_cli::node_runtime_trace(&result),
    );

    let lodash_manifest = session
        .filesystem()
        .read_file("/workspace/project/node_modules/lodash/package.json")
        .await
        .expect("read installed lodash manifest");
    let package_json = session
        .filesystem()
        .read_file("/workspace/project/package.json")
        .await
        .expect("read project package.json after install");
    let package_lock = session
        .filesystem()
        .read_file("/workspace/project/package-lock.json")
        .await
        .expect("read package lock after install");
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
    let lodash_manifest = lodash_manifest.expect(
        format!(
            "installed lodash manifest should exist\nstdout:\n{}\n\nstderr:\n{}\n\npackage.json:\n{}\n\npackage-lock:\n{}\n\nnode_modules entries: {node_modules_entries:#?}\n\nnode trace: {:#?}\nmetadata: {:#?}",
            report["stdout"].as_str().unwrap_or_default(),
            report["stderr"].as_str().unwrap_or_default(),
            package_json
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_else(|| "".into()),
            package_lock
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_else(|| "".into()),
            npm_cli::node_runtime_trace(&result),
            result.metadata,
        )
        .as_str(),
    );
    let lodash_manifest: serde_json::Value =
        serde_json::from_slice(&lodash_manifest).expect("decode installed lodash manifest");
    assert_eq!(lodash_manifest["name"].as_str(), Some("lodash"));
}

#[tokio::test]
async fn npm_cli_registry_install_direct_exec_reports_real_error_stack() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(441, 105).await else {
        eprintln!(
            "skipping npm cli registry install stack test because npm/cli repo is unavailable"
        );
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

    let result = npm_cli::run_inline_node_script_with_debug(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/npm-install-lodash-direct.cjs",
        r#"
        const Npm = require("/workspace/npm/lib/npm.js");
        const util = require("node:util");
        (async () => {
          const npm = new Npm({ argv: ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--loglevel=warn"] });
          try {
            const loaded = await npm.load();
            console.log(JSON.stringify({ phase: "loaded", ok: true }));
            console.log(JSON.stringify({
              phase: "npm-probes",
              ok: true,
              configGetType: typeof npm.config.get,
              configValidateType: typeof npm.config.validate,
              argv: npm.argv,
            }));
            await npm.exec("install", ["lodash"]);
            console.log(JSON.stringify({ phase: "exec-complete", ok: true }));
            console.log(JSON.stringify({
              ok: true,
              loaded,
              exitCode: process.exitCode ?? null,
            }));
          } catch (error) {
            console.log(JSON.stringify({
              ok: false,
              name: error && error.name ? error.name : null,
              constructorName: error && error.constructor ? error.constructor.name : null,
              message: error && error.message ? error.message : String(error),
              stack: error && error.stack ? error.stack : null,
              errName: error && error.err && error.err.name ? error.err.name : null,
              errConstructorName: error && error.err && error.err.constructor ? error.err.constructor.name : null,
              errMessage: error && error.err && error.err.message ? error.err.message : null,
              errStack: error && error.err && error.err.stack ? error.err.stack : null,
              summary: error && error.summary ? error.summary : null,
              detail: error && error.detail ? error.detail : null,
              verbose: error && error.verbose ? error.verbose : null,
              nestedError: error && error.error ? error.error : null,
              ownKeys: error ? Object.getOwnPropertyNames(error) : [],
              inspect: util.inspect(error, { depth: 12, breakLength: 120, showHidden: true }),
              exitCode: process.exitCode ?? null,
            }));
          }
        })();
        "#,
        &[],
        terracedb_sandbox::NodeDebugExecutionOptions {
            autoinstrument_modules: vec![
                "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/reify.js".to_string(),
                "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/build-ideal-tree.js".to_string(),
                "/workspace/npm/node_modules/@npmcli/arborist/lib/place-dep.js".to_string(),
                "/workspace/npm/node_modules/@npmcli/arborist/lib/shrinkwrap.js".to_string(),
                "/workspace/npm/node_modules/@npmcli/arborist/lib/relpath.js".to_string(),
                "/workspace/npm/node_modules/@npmcli/arborist/lib/from-path.js".to_string(),
                "/workspace/npm/lib/realpath.js".to_string(),
            ],
            capture_exceptions: true,
            trace_intrinsics: true,
        },
    )
    .await
    .expect("direct npm install script should execute deterministically");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().unwrap_or_default();
    let payload = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value.get("ok").is_some() && value.get("phase").is_none())
        .expect("structured payload");
    let npm_log_dir = "/workspace/home/.npm/_logs";
    let log_entries = session
        .filesystem()
        .readdir(npm_log_dir)
        .await
        .unwrap_or_default();
    let latest_log = log_entries
        .iter()
        .filter(|entry| entry.name.ends_with(".log"))
        .map(|entry| entry.name.as_str())
        .max()
        .map(|name| format!("{npm_log_dir}/{name}"));
    let log_contents = if let Some(path) = latest_log.as_deref() {
        session
            .filesystem()
            .read_file(path)
            .await
            .ok()
            .flatten()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default()
    } else {
        String::new()
    };
    let debug_events = npm_cli::node_runtime_events(&result);
    let last_exception = npm_cli::node_runtime_last_exception(&result);
    assert_eq!(
        payload["ok"].as_bool(),
        Some(true),
        "expected direct npm install to succeed, got payload: {payload:#?}\nstdout:\n{}\n\nstderr:\n{}\n\nnpm log:\n{}\n\nlast exception:\n{last_exception:#?}\n\ndebug events:\n{debug_events:#?}\n\nnode trace: {:#?}",
        report["stdout"].as_str().unwrap_or_default(),
        report["stderr"].as_str().unwrap_or_default(),
        log_contents,
        npm_cli::node_runtime_trace(&result),
    );
}
