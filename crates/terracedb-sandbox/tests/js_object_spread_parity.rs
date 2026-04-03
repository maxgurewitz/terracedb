use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn parse_stdout_json(stdout: &str) -> Value {
    let payload = stdout
        .lines()
        .rev()
        .find(|line| line.trim_start().starts_with('{'))
        .unwrap_or(stdout.trim());
    serde_json::from_str(payload.trim()).expect("stdout json")
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

#[tokio::test]
async fn js_object_spread_and_default_cloning_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const result = {
        spreadUndefined: { ...undefined },
        spreadNull: { ...null },
        headersMerge: (() => {
          const defaultOptions = {};
          const options = {};
          return {
            headers: {
              ...defaultOptions.headers,
              ...options.headers,
            },
          };
        })(),
        dnsMerge: (() => {
          const options = {};
          return { ttl: 300000, lookup: "lookup", ...options.dns };
        })(),
        destructureClone: (() => {
          const opts = undefined;
          const { strictSSL, ...options } = { ...opts };
          return { strictSSL: strictSSL ?? null, options };
        })(),
      };
      console.log(JSON.stringify(result));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox object spread parity"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node object spread parity")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
