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
async fn node_http2_core_exports_and_sigstore_constants_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const http2 = require("http2");
      console.log(JSON.stringify({
        keys: Object.keys(http2).sort(),
        types: {
          connect: typeof http2.connect,
          createServer: typeof http2.createServer,
          createSecureServer: typeof http2.createSecureServer,
          getDefaultSettings: typeof http2.getDefaultSettings,
          getPackedSettings: typeof http2.getPackedSettings,
          getUnpackedSettings: typeof http2.getUnpackedSettings,
          performServerHandshake: typeof http2.performServerHandshake,
          Http2ServerRequest: typeof http2.Http2ServerRequest,
          Http2ServerResponse: typeof http2.Http2ServerResponse,
        },
        constants: {
          HTTP2_HEADER_LOCATION: http2.constants.HTTP2_HEADER_LOCATION,
          HTTP2_HEADER_CONTENT_TYPE: http2.constants.HTTP2_HEADER_CONTENT_TYPE,
          HTTP2_HEADER_USER_AGENT: http2.constants.HTTP2_HEADER_USER_AGENT,
          HTTP_STATUS_INTERNAL_SERVER_ERROR: http2.constants.HTTP_STATUS_INTERNAL_SERVER_ERROR,
          HTTP_STATUS_TOO_MANY_REQUESTS: http2.constants.HTTP_STATUS_TOO_MANY_REQUESTS,
          HTTP_STATUS_REQUEST_TIMEOUT: http2.constants.HTTP_STATUS_REQUEST_TIMEOUT,
        },
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox http2 api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node http2 api")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
