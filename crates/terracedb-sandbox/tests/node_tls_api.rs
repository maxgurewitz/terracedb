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
async fn node_tls_exports_and_core_types_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const tls = require("tls");
      console.log(JSON.stringify({
        keys: Object.keys(tls).sort(),
        types: Object.fromEntries([
          "CLIENT_RENEG_LIMIT",
          "CLIENT_RENEG_WINDOW",
          "DEFAULT_CIPHERS",
          "DEFAULT_ECDH_CURVE",
          "DEFAULT_MAX_VERSION",
          "DEFAULT_MIN_VERSION",
          "SecureContext",
          "Server",
          "TLSSocket",
          "checkServerIdentity",
          "connect",
          "convertALPNProtocols",
          "createSecureContext",
          "createServer",
          "getCACertificates",
          "getCiphers",
          "rootCertificates",
          "setDefaultCACertificates",
        ].map((key) => [key, typeof tls[key]])),
        defaults: {
          DEFAULT_CIPHERS: tls.DEFAULT_CIPHERS,
          DEFAULT_ECDH_CURVE: tls.DEFAULT_ECDH_CURVE,
          DEFAULT_MAX_VERSION: tls.DEFAULT_MAX_VERSION,
          DEFAULT_MIN_VERSION: tls.DEFAULT_MIN_VERSION,
        },
        ciphers: {
          nonEmpty: tls.getCiphers().length > 0,
          allStrings: tls.getCiphers().every((value) => typeof value === "string"),
        },
        rootCertificates: {
          isArray: Array.isArray(tls.rootCertificates),
          isFrozen: Object.isFrozen(tls.rootCertificates),
          allStrings: tls.rootCertificates.every((value) => typeof value === "string"),
        },
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox tls api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node tls api")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
