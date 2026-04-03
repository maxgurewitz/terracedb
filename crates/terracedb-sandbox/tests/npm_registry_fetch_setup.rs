#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

#[tokio::test]
async fn npm_registry_fetch_sync_setup_smoke_test() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(444, 108).await else {
        eprintln!("skipping npm-registry-fetch setup test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/npm-registry-fetch-setup.cjs",
        r#"
        const getAuth = require("/workspace/npm/node_modules/npm-registry-fetch/lib/auth.js");
        const defaultOpts = require("/workspace/npm/node_modules/npm-registry-fetch/lib/default-opts.js");
        const qs = require("querystring");
        const url = require("url");

        const getHeaders = (uri, auth, opts) => {
          const headers = Object.assign({
            "user-agent": opts.userAgent,
          }, opts.headers || {});

          if (opts.authType) {
            headers["npm-auth-type"] = opts.authType;
          }
          if (opts.scope) {
            headers["npm-scope"] = opts.scope;
          }
          if (opts.npmSession) {
            headers["npm-session"] = opts.npmSession;
          }
          if (opts.npmCommand) {
            headers["npm-command"] = opts.npmCommand;
          }
          if (auth.token) {
            headers.authorization = `Bearer ${auth.token}`;
          } else if (auth.auth) {
            headers.authorization = `Basic ${auth.auth}`;
          }
          if (opts.otp) {
            headers["npm-otp"] = opts.otp;
          }
          return headers;
        };

        const urlIsValid = (u) => {
          try {
            return !!new url.URL(u);
          } catch (_) {
            return false;
          }
        };

        try {
          const steps = [];
          const opts_ = {
            registry: "https://registry.npmjs.org/",
          };
          const opts = {
            ...defaultOpts,
            ...opts_,
          };
          steps.push({ step: "merge", keys: Object.keys(opts).sort() });
          let uri = "/lodash";
          const uriValid = urlIsValid(uri);
          steps.push({ step: "uri-valid", uriValid });
          let registry = opts.registry || defaultOpts.registry;
          if (!uriValid) {
            registry = opts.registry = opts.registry || registry;
            uri = `${registry.trim().replace(/\/?$/g, "")}/${uri.trim().replace(/^\//, "")}`;
            new url.URL(uri);
          }
          steps.push({ step: "uri", uri });
          const method = opts.method || "GET";
          steps.push({ step: "method", method });
          const startTime = Date.now();
          steps.push({ step: "start-time", kind: typeof startTime });
          const auth = getAuth(uri, opts);
          steps.push({ step: "auth", keys: Object.keys(auth).sort() });
          const headers = getHeaders(uri, auth, opts);
          steps.push({ step: "headers", keys: Object.keys(headers).sort() });
          let body = opts.body;
          const bodyIsStream = !!(
            body &&
            typeof body === "object" &&
            typeof body.pipe === "function" &&
            typeof body.on === "function"
          );
          const bodyIsPromise = body && typeof body === "object" && typeof body.then === "function";
          steps.push({ step: "body-shape", bodyIsStream, bodyIsPromise, bodyType: typeof body });
          if (
            body &&
            !bodyIsStream &&
            !bodyIsPromise &&
            typeof body !== "string" &&
            !Buffer.isBuffer(body)
          ) {
            headers["content-type"] = headers["content-type"] || "application/json";
            body = JSON.stringify(body);
          } else if (body && !headers["content-type"]) {
            headers["content-type"] = "application/octet-stream";
          }
          if (opts.gzip) {
            headers["content-encoding"] = "gzip";
            throw new Error("unexpected gzip path in setup smoke test");
          }
          const parsed = new url.URL(uri);
          steps.push({ step: "parsed", href: parsed.href, hostname: parsed.hostname });
          if (opts.query) {
            const q = typeof opts.query === "string" ? qs.parse(opts.query) : opts.query;
            Object.keys(q).forEach((key) => {
              if (q[key] !== undefined) {
                parsed.searchParams.set(key, q[key]);
              }
            });
            uri = url.format(parsed);
          }
          steps.push({ step: "done", uri, headerCount: Object.keys(headers).length });
          console.log(JSON.stringify({ ok: true, steps }));
        } catch (error) {
          console.log(JSON.stringify({
            ok: false,
            name: error && error.name ? error.name : null,
            message: error && error.message ? error.message : String(error),
            stack: error && error.stack ? error.stack : null,
          }));
        }
        "#,
        &[],
    )
    .await
    .expect("npm-registry-fetch setup script should execute");

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
        "expected npm-registry-fetch sync setup to succeed, got payload: {payload:#?}\nstdout:\n{stdout}\nnode trace: {:#?}\nreport: {report:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}
