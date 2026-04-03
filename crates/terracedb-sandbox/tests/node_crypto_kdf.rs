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
async fn node_crypto_pbkdf2_hkdf_and_scrypt_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const crypto = require("crypto");
      async function main() {
        const pbkdf2Async = await new Promise((resolve, reject) =>
          crypto.pbkdf2("password", "salt", 2, 20, "sha256", (error, value) =>
            error ? reject(error) : resolve(value.toString("hex"))
          )
        );
        const hkdfAsync = await new Promise((resolve, reject) =>
          crypto.hkdf("sha256", Buffer.from("ikm"), Buffer.from("salt"), Buffer.from("info"), 32, (error, value) =>
            error ? reject(error) : resolve(Buffer.from(value).toString("hex"))
          )
        );
        const scryptAsync = await new Promise((resolve, reject) =>
          crypto.scrypt("password", "salt", 32, (error, value) =>
            error ? reject(error) : resolve(value.toString("hex"))
          )
        );
        let invalidDigestCode = null;
        try {
          crypto.pbkdf2Sync("pass", "salt", 8, 8, "md55");
        } catch (error) {
          invalidDigestCode = error.code;
        }
        console.log(JSON.stringify({
          pbkdf2Sync: crypto.pbkdf2Sync("password", "salt", 2, 20, "sha256").toString("hex"),
          pbkdf2Async,
          hkdfSync: Buffer.from(crypto.hkdfSync("sha256", Buffer.from("ikm"), Buffer.from("salt"), Buffer.from("info"), 32)).toString("hex"),
          hkdfAsync,
          scryptSync: crypto.scryptSync("password", "salt", 32).toString("hex"),
          scryptAsync,
          invalidDigestCode,
        }));
      }
      main().catch((error) => {
        console.error(error && error.stack ? error.stack : String(error));
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox kdf run"),
    );
    let real: Value = serde_json::from_str(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node kdf run")
            .stdout,
    )
    .expect("real node stdout json");

    assert_eq!(sandbox, real);
}
