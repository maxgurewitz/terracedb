use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_crypto_hash_hmac_and_stream_shapes_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const crypto = require("crypto");
      const streamHash = crypto.createHash("sha512");
      streamHash.end("Test123");
      const writeHash = crypto.createHash("sha512");
      writeHash.write("Te");
      writeHash.write("st");
      writeHash.write("123");
      writeHash.end();

      const wikiKey = "key";
      const wikiText = "The quick brown fox jumps over the lazy dog";
      const secretKey = crypto.createSecretKey(Buffer.from("Node"));

      const finalizedDigestCode = (() => {
        const hash = crypto.createHash("sha256");
        hash.digest("hex");
        try {
          hash.digest("hex");
          return null;
        } catch (error) {
          return error.code;
        }
      })();

      const finalizedUpdateCode = (() => {
        const hash = crypto.createHash("sha256");
        hash.digest("hex");
        try {
          hash.update("boom");
          return null;
        } catch (error) {
          return error.code;
        }
      })();

      console.log(JSON.stringify({
        md5: crypto.createHash("md5").update("Test123").digest("hex"),
        sha256b64: crypto.createHash("sha256").update("Test123").digest("base64"),
        sha512hex: crypto.createHash("sha512").update("Test123").digest("hex"),
        streamHex: Buffer.from(streamHash.read()).toString("hex"),
        writeHex: Buffer.from(writeHash.read()).toString("hex"),
        multiUpdate: crypto.createHash("sha1").update("Test").update("123").digest("hex"),
        shake256: crypto.createHash("shake256", { outputLength: 16 }).update("workflow").digest("hex"),
        shake256Copy: crypto.createHash("shake256", { outputLength: 16 }).update("workflow").copy().digest("hex"),
        hashCtorInstance: crypto.Hash("sha256") instanceof crypto.Hash,
        hmacSha1: crypto.createHmac("sha1", wikiKey).update(wikiText).digest("hex"),
        hmacSha256: crypto.createHmac("sha256", wikiKey).update(wikiText).digest("hex"),
        secretKeyHmac: crypto.createHmac("sha1", secretKey).update("some data").update("to hmac").digest("hex"),
        oneShotHash: crypto.hash("sha256", "workflow-runtime", "hex"),
        finalizedDigestCode,
        finalizedUpdateCode,
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox hash run"),
    );
    let real: Value = serde_json::from_str(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node hash run")
            .stdout,
    )
    .expect("real node stdout json");

    assert_eq!(sandbox, real);
}
