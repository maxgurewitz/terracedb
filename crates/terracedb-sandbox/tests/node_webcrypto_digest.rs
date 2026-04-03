use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    serde_json::from_str(stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "stdout json: {error}; stdout={stdout:?}; stderr={:?}",
            report["stderr"]
        )
    })
}

#[tokio::test]
async fn node_webcrypto_digest_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const crypto = require("crypto");
      async function main() {
        const input = Buffer.from("workflow-sdk");
        const digest = Buffer.from(await globalThis.crypto.subtle.digest("SHA-256", input)).toString("hex");
        const sha512 = Buffer.from(await globalThis.crypto.subtle.digest("SHA-512", input)).toString("hex");
        console.log(JSON.stringify({
          digest,
          sha512,
          moduleDigest: crypto.createHash("sha256").update(input).digest("hex"),
          moduleSha512: crypto.createHash("sha512").update(input).digest("hex"),
          sameWebCrypto: crypto.webcrypto === globalThis.crypto,
          sameSubtle: crypto.subtle === globalThis.crypto.subtle,
        }));
      }
      main().catch((error) => {
        console.error(error && error.stack ? error.stack : String(error));
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox webcrypto run"),
    );
    let real: Value = serde_json::from_str(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node webcrypto run")
            .stdout,
    )
    .expect("real node stdout json");

    assert_eq!(sandbox, real);
}
