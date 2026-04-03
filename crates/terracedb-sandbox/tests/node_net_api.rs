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
async fn node_net_exports_match_real_node_for_core_api_shape() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const net = require("net");
      console.log(JSON.stringify({
        keys: Object.keys(net).sort(),
        types: {
          BlockList: typeof net.BlockList,
          Server: typeof net.Server,
          Socket: typeof net.Socket,
          SocketAddress: typeof net.SocketAddress,
          Stream: typeof net.Stream,
          connect: typeof net.connect,
          createConnection: typeof net.createConnection,
          createServer: typeof net.createServer,
          getDefaultAutoSelectFamily: typeof net.getDefaultAutoSelectFamily,
          getDefaultAutoSelectFamilyAttemptTimeout: typeof net.getDefaultAutoSelectFamilyAttemptTimeout,
          isIP: typeof net.isIP,
          isIPv4: typeof net.isIPv4,
          isIPv6: typeof net.isIPv6,
          setDefaultAutoSelectFamily: typeof net.setDefaultAutoSelectFamily,
          setDefaultAutoSelectFamilyAttemptTimeout: typeof net.setDefaultAutoSelectFamilyAttemptTimeout,
        },
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox net api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node net api")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
