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

fn render_registry_sync_trace_source(
    npm_registry_fetch_require: &str,
    make_fetch_happen_require: &str,
    minipass_require: &str,
    minipass_fetch_require: &str,
) -> String {
    format!(
        r#"
        const trace = [];
        const describe = (value) => {{
          if (value === null) return "null";
          if (value === undefined) return "undefined";
          if (Array.isArray(value)) return "array";
          const kind = typeof value;
          if (kind === "function" && typeof value.name === "string" && value.name.length > 0) {{
            return value.name;
          }}
          if (kind === "object" || kind === "function") {{
            try {{
              const tag = Object.prototype.toString.call(value);
              const match = /^\[object ([^\]]+)\]$/.exec(tag);
              if (match && match[1]) {{
                return match[1];
              }}
            }} catch (_error) {{}}
          }}
          return kind;
        }};
        const wrap = (name, fn) => function (...args) {{
          trace.push({{
            name,
            args: args.map((value) => describe(value)),
          }});
          try {{
            const result = Reflect.apply(fn, this, args);
            trace.push({{
              name: `${{name}}:ok`,
              result: describe(result),
            }});
            return result;
          }} catch (error) {{
            trace.push({{
              name: `${{name}}:throw`,
              error: error && error.message ? error.message : String(error),
            }});
            throw error;
          }}
        }};

        const originalAssign = Object.assign;
        Object.assign = wrap("Object.assign", originalAssign);
        const originalIsBuffer = Buffer.isBuffer;
        Buffer.isBuffer = wrap("Buffer.isBuffer", originalIsBuffer);

        const minipass = require({minipass_require});
        const originalIsStream = minipass.Minipass.isStream;
        minipass.Minipass.isStream = wrap("Minipass.isStream", originalIsStream);

        const mfhPath = require.resolve({make_fetch_happen_require});
        const minipassFetch = require({minipass_fetch_require});
        require.cache[mfhPath] = {{
          id: mfhPath,
          filename: mfhPath,
          loaded: true,
          exports: wrap("makeFetchHappen", () => Promise.resolve(
            new minipassFetch.Response('{{"name":"lodash","dist-tags":{{"latest":"1.0.0"}},"versions":{{"1.0.0":{{}}}}}}', {{
              status: 200,
              url: "https://registry.npmjs.org/lodash",
              headers: {{ "content-type": "application/json" }},
            }}
          ))),
        }};

        (async () => {{
          try {{
            const fetch = require({npm_registry_fetch_require});
            const data = await fetch.json("/lodash", {{
              registry: "https://registry.npmjs.org/",
            }});
            console.log(JSON.stringify({{
              ok: true,
              latest: data["dist-tags"] && data["dist-tags"].latest,
              trace,
            }}));
          }} catch (error) {{
            console.log(JSON.stringify({{
              ok: false,
              name: error && error.name ? error.name : null,
              message: error && error.message ? error.message : String(error),
              trace,
            }}));
          }} finally {{
            Object.assign = originalAssign;
            Buffer.isBuffer = originalIsBuffer;
            minipass.Minipass.isStream = originalIsStream;
          }}
        }})();
        "#,
        npm_registry_fetch_require =
            serde_json::to_string(npm_registry_fetch_require).expect("quoted npm-registry-fetch"),
        make_fetch_happen_require =
            serde_json::to_string(make_fetch_happen_require).expect("quoted make-fetch-happen"),
        minipass_require = serde_json::to_string(minipass_require).expect("quoted minipass"),
        minipass_fetch_require =
            serde_json::to_string(minipass_fetch_require).expect("quoted minipass-fetch"),
    )
}

#[tokio::test]
async fn npm_registry_fetch_sync_contract_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping npm-registry-fetch sync contract because npm/cli root is unavailable");
        return;
    };

    let sandbox_source = render_registry_sync_trace_source(
        "/workspace/npm/node_modules/npm-registry-fetch/lib/index.js",
        "/workspace/npm/node_modules/make-fetch-happen/lib/index.js",
        "/workspace/npm/node_modules/minipass/dist/commonjs/index.js",
        "/workspace/npm/node_modules/minipass-fetch/lib/index.js",
    );
    let real_source = render_registry_sync_trace_source(
        &format!("{npm_root}/node_modules/npm-registry-fetch/lib/index.js"),
        &format!("{npm_root}/node_modules/make-fetch-happen/lib/index.js"),
        &format!("{npm_root}/node_modules/minipass/dist/commonjs/index.js"),
        &format!("{npm_root}/node_modules/minipass-fetch/lib/index.js"),
    );

    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(465, 145).await else {
        eprintln!(
            "skipping npm-registry-fetch sync contract because npm/cli session is unavailable"
        );
        return;
    };
    let sandbox = sandbox_stdout_json(
        npm_cli::run_inline_node_script(
            &session,
            npm_cli::SANDBOX_PROJECT_ROOT,
            "/workspace/project/.terrace/npm-registry-fetch-sync-contract.cjs",
            &sandbox_source,
            &[],
        )
        .await
        .expect("sandbox npm-registry-fetch sync contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(&real_source)
            .expect("real node npm-registry-fetch sync contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
