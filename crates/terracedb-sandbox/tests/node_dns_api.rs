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
async fn node_dns_exports_and_core_types_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on upstream dns.js export surface and tests like test-dns-lookup.js.
    let source = r#"
      const dns = require("dns");
      console.log(JSON.stringify({
        keys: Object.keys(dns).sort(),
        types: Object.fromEntries([
          "lookup",
          "lookupService",
          "resolve",
          "resolve4",
          "resolve6",
          "resolveAny",
          "resolveCaa",
          "resolveCname",
          "resolveMx",
          "resolveNaptr",
          "resolveNs",
          "resolvePtr",
          "resolveSoa",
          "resolveSrv",
          "resolveTlsa",
          "resolveTxt",
          "reverse",
          "getServers",
          "setServers",
          "setDefaultResultOrder",
          "getDefaultResultOrder",
          "Resolver",
          "promises",
        ].map((key) => [key, typeof dns[key]])),
        promisesKeys: Object.keys(dns.promises).sort(),
        promisesTypes: Object.fromEntries([
          "lookup",
          "lookupService",
          "resolve",
          "resolve4",
          "resolve6",
          "resolveAny",
          "resolveCaa",
          "resolveCname",
          "resolveMx",
          "resolveNaptr",
          "resolveNs",
          "resolvePtr",
          "resolveSoa",
          "resolveSrv",
          "resolveTlsa",
          "resolveTxt",
          "reverse",
          "Resolver",
        ].map((key) => [key, typeof dns.promises[key]])),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox dns api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node dns api")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
