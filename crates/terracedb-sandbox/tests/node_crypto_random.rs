use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_crypto_randomness_is_seeded_and_buffer_operations_work() {
    let source = r#"
      const crypto = require("crypto");
      const filled = Buffer.alloc(8, 0);
      crypto.randomFillSync(filled, 2, 4);
      const typed = new Uint16Array(4);
      globalThis.crypto.getRandomValues(typed);
      const bytes = crypto.randomBytes(16).toString("hex");
      const uuid = crypto.randomUUID();
      let mismatchMessage = null;
      try {
        crypto.timingSafeEqual(Buffer.from([1, 2, 3]), Buffer.from([1, 2]));
      } catch (error) {
        mismatchMessage = error.message;
      }
      console.log(JSON.stringify({
        bytes,
        filled: filled.toString("hex"),
        typed: Buffer.from(typed.buffer).toString("hex"),
        eq: crypto.timingSafeEqual(Buffer.from("foo"), Buffer.from("foo")),
        neq: crypto.timingSafeEqual(Buffer.from("foo"), Buffer.from("bar")),
        mismatchMessage,
        uuid,
      }));
    "#;

    let first = sandbox_stdout_json(
        node_compat_support::exec_node_fixture_with_seed(510, 777, source)
            .await
            .expect("first sandbox random run"),
    );
    let second = sandbox_stdout_json(
        node_compat_support::exec_node_fixture_with_seed(510, 777, source)
            .await
            .expect("second sandbox random run"),
    );
    let different = sandbox_stdout_json(
        node_compat_support::exec_node_fixture_with_seed(510, 778, source)
            .await
            .expect("different seed sandbox random run"),
    );

    assert_eq!(
        first, second,
        "same seed should replay the same crypto output"
    );
    assert_ne!(
        first["bytes"], different["bytes"],
        "different seed should change random byte stream"
    );
    assert_eq!(first["eq"], Value::Bool(true));
    assert_eq!(first["neq"], Value::Bool(false));
    assert!(
        first["mismatchMessage"]
            .as_str()
            .unwrap_or_default()
            .contains("same byte length"),
        "expected timingSafeEqual length error, got: {first:#?}"
    );
    let uuid = first["uuid"].as_str().expect("uuid string");
    assert_eq!(uuid.len(), 36);
    assert_eq!(&uuid[14..15], "4");
    assert!(
        matches!(&uuid[19..20], "8" | "9" | "a" | "b"),
        "expected RFC4122 variant nibble, got {uuid}"
    );
}
