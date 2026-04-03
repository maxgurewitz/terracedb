use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;
#[path = "support/npm_cli.rs"]
mod npm_cli;

fn parse_stdout_json(stdout: &str) -> Value {
    serde_json::from_str(stdout.trim()).expect("stdout json")
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

fn render_fetch_index_source(fetch_require: &str) -> String {
    format!(
        r#"
        const fetch = require({fetch_require});
        const captureReject = async (promiseFactory) => {{
          try {{
            const value = await promiseFactory();
            return {{ ok: true, value }};
          }} catch (error) {{
            return {{
              ok: false,
              name: error.name,
              message: error.message,
              code: error.code ?? null,
              errno: error.errno ?? null,
              type: error.type ?? null,
            }};
          }}
        }};

        (async () => {{
          const results = {{}};
          results.exports = {{
            abortErrorName: new fetch.AbortError("x").name,
            fetchErrorName: new fetch.FetchError("x", "system").name,
            headersTag: String(new fetch.Headers()),
            requestTag: String(new fetch.Request("http://localhost")),
            responseTag: String(new fetch.Response()),
          }};
          results.rejects = {{
            protocolRelative: await captureReject(() => fetch("//example.com/")),
            relative: await captureReject(() => fetch("/some/path")),
            unsupported: await captureReject(() => fetch("ftp://example.com/")),
          }};
          const dataRes = await fetch("data:text/plain,hello");
          results.dataUrl = {{
            status: dataRes.status,
            ok: dataRes.ok,
            text: await dataRes.text(),
            contentType: dataRes.headers.get("Content-Type"),
            contentLength: dataRes.headers.get("Content-Length"),
          }};
          console.log(JSON.stringify(results));
        }})().catch((error) => {{
          console.log(JSON.stringify({{
            ok: false,
            name: error && error.name ? error.name : null,
            message: error && error.message ? error.message : String(error),
            code: error && error.code ? error.code : null,
            type: error && error.type ? error.type : null,
          }}));
        }});
        "#,
        fetch_require = serde_json::to_string(fetch_require).expect("quoted fetch require"),
    )
}

#[tokio::test]
async fn minipass_fetch_index_contract_matches_real_node_for_pure_paths() {
    if !node_compat_support::real_node_available() {
        return;
    }
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping minipass-fetch index contract because npm/cli root is unavailable");
        return;
    };
    let sandbox_source =
        render_fetch_index_source("/workspace/npm/node_modules/minipass-fetch/lib/index.js");
    let real_source =
        render_fetch_index_source(&format!("{npm_root}/node_modules/minipass-fetch/lib/index.js"));

    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(464, 144).await else {
        eprintln!("skipping minipass-fetch index contract because npm/cli session is unavailable");
        return;
    };
    let sandbox = sandbox_stdout_json(
        npm_cli::run_inline_node_script(
            &session,
            npm_cli::SANDBOX_PROJECT_ROOT,
            "/workspace/project/.terrace/minipass-fetch-index-contract.cjs",
            &sandbox_source,
            &[],
        )
        .await
        .expect("sandbox minipass-fetch index contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(&real_source)
            .expect("real node minipass-fetch index contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
