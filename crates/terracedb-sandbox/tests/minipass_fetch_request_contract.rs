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

fn render_request_source(
    request_require: &str,
    minipass_require: &str,
    http_require: &str,
) -> String {
    format!(
        r#"
        const Request = require({request_require});
        const {{ Minipass }} = require({minipass_require});
        const http = require({http_require});

        const captureError = (fn) => {{
          try {{
            return {{ ok: true, value: fn() }};
          }} catch (error) {{
            return {{ ok: false, name: error.name, message: error.message, code: error.code ?? null }};
          }}
        }};
        const captureErrorAsync = async (fn) => {{
          try {{
            return {{ ok: true, value: await fn() }};
          }} catch (error) {{
            return {{ ok: false, name: error.name, message: error.message, code: error.code ?? null }};
          }}
        }};

        const sanitizeNodeOptions = (options) => {{
          const headers = {{}};
          for (const [key, value] of Object.entries(options.headers || {{}})) {{
            headers[key] = Array.isArray(value) ? [...value] : value;
          }}
          return {{
            auth: options.auth ?? null,
            host: options.host ?? null,
            hostname: options.hostname ?? null,
            path: options.path ?? null,
            port: options.port === "" ? "" : (options.port ?? null),
            protocol: options.protocol ?? null,
            method: options.method ?? null,
            rejectUnauthorized: options.rejectUnauthorized,
            agentType: options.agent === undefined ? "undefined" : typeof options.agent,
            headers,
          }};
        }};

        (async () => {{
          const results = {{}};
          const req = new Request({{ href: "https://github.com/" }});
          results.webidl = {{
            url: req.url,
            tag: String(req),
            enumerableForIn: (() => {{
              const props = [];
              for (const prop in req) props.push(prop);
              return props.sort();
            }})(),
            readonlyErrors: [
              captureError(() => (req.body = "abc")),
              captureError(() => (req.method = "abc")),
              captureError(() => (req.url = "abc")),
            ],
          }};

          results.signalError = captureError(() => new Request("http://foo.com", {{ signal: {{}} }}));

          const parentAbortController = new AbortController();
          const derivedAbortController = new AbortController();
          const agent = new http.Agent();
          const parent = new Request("http://localhost/test", {{
            method: "POST",
            body: "a=1",
            follow: 1,
            signal: parentAbortController.signal,
            agent,
            rejectUnauthorized: false,
          }});
          const derived = new Request(parent, {{
            follow: 2,
            signal: derivedAbortController.signal,
          }});
          const removedSignal = new Request(parent, {{ signal: null }});
          results.derived = {{
            parentUrl: parent.url,
            derivedUrl: derived.url,
            parentMethod: parent.method,
            derivedMethod: derived.method,
            sameBody: derived.body === parent.body,
            parentFollow: parent.follow,
            derivedFollow: derived.follow,
            parentSignalClass: parent.signal && parent.signal.constructor && parent.signal.constructor.name,
            derivedSignalClass: derived.signal && derived.signal.constructor && derived.signal.constructor.name,
            removedSignal: removedSignal.signal,
            parentNodeOptions: sanitizeNodeOptions(Request.getNodeRequestOptions(parent)),
            derivedNodeOptions: sanitizeNodeOptions(Request.getNodeRequestOptions(derived)),
          }};

          results.getHeadErrors = [
            captureError(() => new Request("http://localhost", {{ body: "a" }})),
            captureError(() => new Request("http://localhost", {{ body: "a", method: "HEAD" }})),
            captureError(() => new Request("http://localhost", {{ body: "a", method: "get" }})),
          ];

          const empty = new Request("http://localhost");
          const stringReq = new Request("http://localhost", {{ method: "POST", body: "a=1" }});
          const jsonReq = new Request("http://localhost", {{ method: "POST", body: '{{\"a\":1}}' }});
          const arrayBufferReq = new Request("http://localhost", {{
            method: "POST",
            body: new Uint8Array([97, 61, 49]).buffer,
          }});
          const uint8Req = new Request("http://localhost", {{
            method: "POST",
            body: new Uint8Array([97, 61, 49]),
          }});
          const dataViewReq = new Request("http://localhost", {{
            method: "POST",
            body: new DataView(new Uint8Array([97, 61, 49]).buffer),
          }});
          const blobReq = new Request("http://localhost", {{
            method: "POST",
            body: Buffer.from("a=1"),
          }});
          const cloneSource = new Minipass().end("a=1");
          cloneSource.pause();
          setTimeout(() => cloneSource.resume());
          const cloneReq = new Request("http://localhost", {{
            method: "POST",
            body: cloneSource.pipe(new Minipass()),
            redirect: "manual",
            headers: {{ b: "2" }},
            follow: 3,
            compress: false,
            agent,
            signal: parentAbortController.signal,
          }});
          const cloneCopy = cloneReq.clone();

          results.body = {{
            emptyText: await empty.text(),
            stringText: await stringReq.text(),
            jsonValue: await jsonReq.json(),
            arrayBufferText: Buffer.from(await arrayBufferReq.arrayBuffer()).toString(),
            uint8Text: await uint8Req.text(),
            dataViewText: await dataViewReq.text(),
            bufferText: (await stringReq.clone().buffer()).toString(),
            blob: await (async () => {{
              const blob = await blobReq.blob();
              return {{ size: blob.size, type: blob.type }};
            }})(),
            clone: await captureErrorAsync(async () => {{
              return {{
                values: await Promise.all([cloneCopy.text(), cloneReq.text()]),
                sameBody: cloneReq.body === cloneCopy.body,
                headers: cloneCopy.headers.get("b"),
                follow: cloneCopy.follow,
                compress: cloneCopy.compress,
                agentType: typeof cloneCopy.agent,
              }};
            }}),
          }};

          const tlsBefore = process.env.NODE_TLS_REJECT_UNAUTHORIZED;
          process.env.NODE_TLS_REJECT_UNAUTHORIZED = null;
          results.rejectUnauthorizedDefault = Request.getNodeRequestOptions(new Request("http://a.b")).rejectUnauthorized;
          process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
          results.rejectUnauthorizedZero = Request.getNodeRequestOptions(new Request("http://a.b")).rejectUnauthorized;
          process.env.NODE_TLS_REJECT_UNAUTHORIZED = tlsBefore;

          const requestOptions = Request.getNodeRequestOptions(new Request("http://user:password@a.b:8080/demo?q=1", {{
            method: "PATCH",
            headers: {{ accept: "text/plain; q=1, *.*; q=0.8" }},
            body: "123",
            compress: true,
          }}));
          results.requestOptions = sanitizeNodeOptions(requestOptions);

          console.log(JSON.stringify(results));
        }})().catch((error) => {{
          console.log(JSON.stringify({{
            ok: false,
            name: error && error.name ? error.name : null,
            message: error && error.message ? error.message : String(error),
            code: error && error.code ? error.code : null,
          }}));
        }});
        "#,
        request_require = serde_json::to_string(request_require).expect("quoted request require"),
        minipass_require = serde_json::to_string(minipass_require).expect("quoted minipass require"),
        http_require = serde_json::to_string(http_require).expect("quoted http require"),
    )
}

#[tokio::test]
async fn minipass_fetch_request_contract_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping minipass-fetch request contract because npm/cli root is unavailable");
        return;
    };
    let sandbox_source = render_request_source(
        "/workspace/npm/node_modules/minipass-fetch/lib/request.js",
        "/workspace/npm/node_modules/minipass/dist/commonjs/index.js",
        "http",
    );
    let real_source = render_request_source(
        &format!("{npm_root}/node_modules/minipass-fetch/lib/request.js"),
        &format!("{npm_root}/node_modules/minipass/dist/commonjs/index.js"),
        "http",
    );

    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(461, 141).await else {
        eprintln!("skipping minipass-fetch request contract because npm/cli session is unavailable");
        return;
    };
    let sandbox = sandbox_stdout_json(
        npm_cli::run_inline_node_script(
            &session,
            npm_cli::SANDBOX_PROJECT_ROOT,
            "/workspace/project/.terrace/minipass-fetch-request-contract.cjs",
            &sandbox_source,
            &[],
        )
        .await
        .expect("sandbox minipass-fetch request contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(&real_source)
            .expect("real node minipass-fetch request contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
