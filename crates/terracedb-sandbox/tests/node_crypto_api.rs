use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_crypto_exports_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const crypto = require("crypto");
      console.log(JSON.stringify({
        keys: Object.keys(crypto).sort(),
        sameWebCrypto: crypto.webcrypto === globalThis.crypto,
        sameSubtle: crypto.subtle === globalThis.crypto.subtle,
        hashes: crypto.getHashes(),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox crypto exports"),
    );
    let real: Value = serde_json::from_str(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node crypto exports")
            .stdout,
    )
    .expect("real node stdout json");

    assert_eq!(sandbox["keys"], real["keys"]);
    assert_eq!(sandbox["sameWebCrypto"], real["sameWebCrypto"]);
    assert_eq!(sandbox["sameSubtle"], real["sameSubtle"]);

    let hashes = sandbox["hashes"].as_array().expect("hash list");
    assert!(
        hashes.iter().any(|entry| entry.as_str() == Some("sha256")),
        "expected sha256 in getHashes, got: {hashes:#?}"
    );
    assert!(
        hashes
            .iter()
            .any(|entry| entry.as_str() == Some("shake256")),
        "expected shake256 in getHashes, got: {hashes:#?}"
    );
}
