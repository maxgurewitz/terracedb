#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

#[tokio::test]
async fn npm_cli_pacote_registry_manifest_lodash_smoke_test() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(442, 106).await else {
        eprintln!("skipping pacote registry smoke test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/pacote-registry-manifest.cjs",
        r#"
        const pacote = require("/workspace/npm/node_modules/pacote/lib/index.js");
        (async () => {
          try {
            const manifest = await pacote.manifest("lodash");
            console.log(JSON.stringify({
              ok: true,
              name: manifest.name,
              version: manifest.version,
              resolved: manifest._resolved,
            }));
          } catch (error) {
            console.log(JSON.stringify({
              ok: false,
              name: error && error.name ? error.name : null,
              message: error && error.message ? error.message : String(error),
              stack: error && error.stack ? error.stack : null,
            }));
          }
        })();
        "#,
        &[],
    )
    .await
    .expect("pacote registry manifest script should execute");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().unwrap_or_default();
    let payload = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value.get("ok").is_some())
        .expect("structured payload");
    assert_eq!(
        payload["ok"].as_bool(),
        Some(true),
        "expected pacote registry manifest to succeed, got payload: {payload:#?}\nstdout:\n{stdout}\nnode trace: {:#?}\nreport: {report:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}

#[tokio::test]
async fn npm_cli_registry_fetch_lodash_packument_smoke_test() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(443, 107).await else {
        eprintln!("skipping npm-registry-fetch smoke test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script_with_debug(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/npm-registry-fetch-packument.cjs",
        r#"
        const fetch = require("/workspace/npm/node_modules/npm-registry-fetch/lib/index.js");
        (async () => {
          try {
            const data = await fetch.json("/lodash", {
              registry: "https://registry.npmjs.org/",
            });
            console.log(JSON.stringify({
              ok: true,
              name: data.name,
              versionCount: data.versions ? Object.keys(data.versions).length : 0,
              latest: data["dist-tags"] && data["dist-tags"].latest,
            }));
          } catch (error) {
            console.log(JSON.stringify({
              ok: false,
              name: error && error.name ? error.name : null,
              message: error && error.message ? error.message : String(error),
              stack: error && error.stack ? error.stack : null,
            }));
          }
        })();
        "#,
        &[],
        terracedb_sandbox::NodeDebugExecutionOptions {
            autoinstrument_modules: vec![
                "/workspace/npm/node_modules/npm-registry-fetch/lib/index.js".to_string(),
                "/workspace/npm/node_modules/npm-registry-fetch/lib/check-response.js".to_string(),
                "/workspace/npm/node_modules/make-fetch-happen/lib/index.js".to_string(),
                "/workspace/npm/node_modules/minipass-fetch/lib/index.js".to_string(),
                "/workspace/npm/node_modules/minipass-fetch/lib/request.js".to_string(),
                "/workspace/npm/node_modules/minipass-fetch/lib/response.js".to_string(),
                "/workspace/npm/node_modules/@sigstore/sign/dist/external/fetch.js".to_string(),
            ],
            capture_exceptions: true,
            trace_intrinsics: true,
        },
    )
    .await
    .expect("npm-registry-fetch script should execute");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().unwrap_or_default();
    let payload = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value.get("ok").is_some())
        .expect("structured payload");
    let last_exception = npm_cli::node_runtime_last_exception(&result);
    assert_eq!(
        payload["ok"].as_bool(),
        Some(true),
        "expected npm-registry-fetch packument to succeed, got payload: {payload:#?}\nstdout:\n{stdout}\nlast exception:\n{last_exception:#?}\nnode trace: {:#?}\nreport: {report:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}
