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
async fn node_http_request_validation_and_agent_contract_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Inspired by test-http-hostname-typechecking.js, test-http-agent-getname.js,
    // test-http-request-dont-override-options.js, and test-url-urltooptions.js.
    let source = r#"
      const http = require("http");
      const https = require("https");
      const results = {
        validateHeaderName: null,
        validateHeaderValue: null,
        invalidHosts: [],
      };
      try {
        http.validateHeaderName("bad header");
      } catch (error) {
        results.validateHeaderName = { name: error.name, code: error.code ?? null, message: error.message };
      }
      try {
        http.validateHeaderValue("x-demo", "bad\u0007value");
      } catch (error) {
        results.validateHeaderValue = { name: error.name, code: error.code ?? null, message: error.message };
      }
      for (const value of [0, 1, true, false, Symbol("x"), 1n]) {
        try {
          http.request({ hostname: value });
          results.invalidHosts.push(null);
        } catch (error) {
          results.invalidHosts.push({ name: error.name, code: error.code ?? null, message: error.message });
        }
      }
      const agent = new http.Agent({ keepAlive: true, maxSockets: 3 });
      const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 5 });
      const options = { protocol: "http:", hostname: "example.com", port: 81, localAddress: "127.0.0.1", family: 4 };
      const snapshot = JSON.stringify(options);
      const req = http.request(new URL("http://user:pass@example.com:8080/pkg?q=1"), { agent: false });
      const req2 = https.request("https://example.com/pkg?q=1", { agent: false });
      results.agentName = agent.getName(options);
      results.httpsAgentName = httpsAgent.getName({
        protocol: "https:",
        hostname: "example.com",
        port: 443,
        ca: "ca",
        cert: "cert",
        key: "key",
        pfx: "pfx",
        rejectUnauthorized: false,
        servername: "example.com",
        minVersion: "TLSv1.2",
        maxVersion: "TLSv1.3",
      });
      results.optionsMutated = snapshot !== JSON.stringify(options);
      results.req = {
        method: req.method,
        path: req.path,
        host: req.host,
        protocol: req.protocol,
        agentType: req.agent === false ? "false" : typeof req.agent,
      };
      results.req2 = {
        method: req2.method,
        path: req2.path,
        host: req2.host,
        protocol: req2.protocol,
        agentType: req2.agent === false ? "false" : typeof req2.agent,
      };
      req.destroy();
      req2.destroy();
      console.log(JSON.stringify(results));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox http contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node http contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
