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
async fn node_dns_lookup_argument_and_ip_fast_path_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on test-dns-lookup.js plus get/set default order behavior.
    let source = r#"
      const dns = require("dns");
      (async () => {
        const result = {
          defaultOrderBefore: dns.getDefaultResultOrder(),
          serversBeforeIsArray: Array.isArray(dns.getServers()),
          serversBeforeNonEmpty: dns.getServers().length > 0,
          invalid: [],
        };
        for (const thunk of [
          () => dns.lookup(1, {}),
          () => dns.lookup(false, "cb"),
          () => dns.lookup(false, { family: 5 }, () => {}),
          () => dns.lookup(false, { hints: "bad" }, () => {}),
          () => dns.lookup(false, { all: "bad" }, () => {}),
          () => dns.lookup(false, { verbatim: "bad" }, () => {}),
        ]) {
          try {
            thunk();
            result.invalid.push(null);
          } catch (error) {
            result.invalid.push({ name: error.name, code: error.code ?? null, message: error.message });
          }
        }
        dns.setDefaultResultOrder("ipv4first");
        result.defaultOrderAfterSet = dns.getDefaultResultOrder();
        dns.setDefaultResultOrder(result.defaultOrderBefore);
        result.lookupLiteral = await dns.promises.lookup("127.0.0.1");
        result.lookupLiteralAll = await dns.promises.lookup("127.0.0.1", { all: true });
        console.log(JSON.stringify(result));
      })();
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox dns lookup contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node dns lookup contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
