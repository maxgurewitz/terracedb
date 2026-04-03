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
async fn node_tls_validations_and_pure_helpers_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Inspired by test-tls-basic-validations.js and test-tls-check-server-identity.js.
    let source = r#"
      const tls = require("tls");
      const checks = [];
      for (const thunk of [
        () => tls.createSecureContext({ ciphers: 1 }),
        () => tls.createServer({ ciphers: 1 }),
        () => tls.createSecureContext({ key: "dummykey", passphrase: 1 }),
        () => tls.createServer({ key: "dummykey", passphrase: 1 }),
        () => tls.createServer({ ecdhCurve: 1 }),
        () => tls.createServer({ handshakeTimeout: "abcd" }),
        () => tls.createServer({ sessionTimeout: "abcd" }),
        () => tls.createServer({ ticketKeys: "abcd" }),
        () => tls.createSecureContext({ minVersion: "fhqwhgads" }),
        () => tls.createSecureContext({ maxVersion: "fhqwhgads" }),
        () => tls.connect({ checkServerIdentity: 1 }),
      ]) {
        try {
          const value = thunk();
          checks.push({ ok: true, type: typeof value });
        } catch (error) {
          checks.push({ ok: false, name: error.name, code: error.code ?? null, message: error.message });
        }
      }
      const serverIdentity = [
        tls.checkServerIdentity("127.0.0.1", { subjectaltname: "IP Address:127.0.0.1" }),
        tls.checkServerIdentity("example.com", { subject: { CN: "example.com" } }),
        tls.checkServerIdentity("example.com", { subject: { CN: "not-example.com" } }),
      ].map((value) => value ? { name: value.name, code: value.code ?? null, message: value.message } : null);
      console.log(JSON.stringify({ checks, serverIdentity }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox tls contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node tls contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
