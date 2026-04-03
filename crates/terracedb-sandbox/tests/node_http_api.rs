use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn parse_stdout_json(stdout: &str) -> Value {
    serde_json::from_str(stdout.trim()).expect("stdout json")
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

#[tokio::test]
async fn node_http_and_https_exports_match_real_node_for_core_api_shape() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const http = require("http");
      const https = require("https");
      console.log(JSON.stringify({
        http: {
          keys: Object.keys(http).sort(),
          methods: http.METHODS.slice(0, 10),
          status200: http.STATUS_CODES[200],
          status404: http.STATUS_CODES[404],
          types: {
            Agent: typeof http.Agent,
            ClientRequest: typeof http.ClientRequest,
            IncomingMessage: typeof http.IncomingMessage,
            request: typeof http.request,
            get: typeof http.get,
            createServer: typeof http.createServer,
            globalAgent: typeof http.globalAgent,
          },
        },
        https: {
          keys: Object.keys(https).sort(),
          types: {
            Agent: typeof https.Agent,
            request: typeof https.request,
            get: typeof https.get,
            createServer: typeof https.createServer,
            globalAgent: typeof https.globalAgent,
          },
        },
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox http api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node http api")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
