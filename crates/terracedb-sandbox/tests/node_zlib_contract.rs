use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string").trim();
    serde_json::from_str(stdout).unwrap_or_else(|error| {
        panic!(
            "stdout json parse failed: {error}; exit_code={:?}; stdout={:?}; stderr={:?}",
            report["exitCode"], report["stdout"], report["stderr"],
        )
    })
}

#[tokio::test]
async fn node_zlib_flush_exposes_incremental_output() {
    let source = r#"
      "use strict";
      const zlib = require("zlib");
      const deflater = zlib.createDeflate({ level: 0 });
      const chunk = Buffer.from("/9j/4AAQSkZJRgABAQEASA==", "base64");

      deflater.write(chunk, () => {
        deflater.flush(zlib.constants.Z_NO_FLUSH, () => {
          const actualNone = deflater.read();
          deflater.flush(() => {
            const bufs = [];
            let buf;
            while ((buf = deflater.read()) !== null) bufs.push(buf);
            console.log(JSON.stringify({
              noFlushHex: actualNone ? Buffer.from(actualNone).toString("hex") : null,
              fullFlushHex: Buffer.concat(bufs).toString("hex"),
            }));
          });
        });
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox zlib flush contract"),
    );
    assert_eq!(sandbox["noFlushHex"], Value::from("7801"));
    assert!(
        sandbox["fullFlushHex"].as_str().unwrap().len() > 10,
        "expected non-empty flush output, got {sandbox:?}"
    );
}

#[tokio::test]
async fn node_zlib_params_change_subsequent_output() {
    let source = r#"
      "use strict";
      const zlib = require("zlib");
      const chunk1 = Buffer.from("a".repeat(4096), "utf8");
      const chunk2 = Buffer.from("b".repeat(4096), "utf8");
      const deflater = zlib.createDeflate({ level: 9, strategy: zlib.constants.Z_DEFAULT_STRATEGY });
      const bufs = [];

      deflater.write(chunk1, () => {
        while (deflater.read() !== null) {}
        deflater.params(0, zlib.constants.Z_DEFAULT_STRATEGY, () => {
          deflater.on("readable", () => {
            let buf;
            while ((buf = deflater.read()) !== null) bufs.push(buf);
          });
          deflater.end(chunk2);
        });
      });

      deflater.on("finish", () => {
        console.log(JSON.stringify({
          outputHex: Buffer.concat(bufs).toString("hex"),
          size: Buffer.concat(bufs).length,
        }));
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox zlib params contract"),
    );
    assert!(
        sandbox["size"].as_u64().unwrap() > 0,
        "expected params/end flow to emit compressed bytes, got {sandbox:?}"
    );
    assert!(
        sandbox["outputHex"].as_str().unwrap().len() > 10,
        "expected hex output from params flow, got {sandbox:?}"
    );
}

#[tokio::test]
async fn node_zlib_writes_after_readable_end_still_invoke_callbacks_until_flush() {
    let source = r#"
      "use strict";
      const zlib = require("zlib");
      const data = zlib.deflateRawSync("Welcome");
      const inflate = zlib.createInflateRaw();
      let callbackCount = 0;
      inflate.resume();
      inflate.write(data, () => callbackCount++);
      inflate.write(Buffer.from([0x00]), () => callbackCount++);
      inflate.write(Buffer.from([0x00]), () => callbackCount++);
      inflate.flush(() => {
        console.log(JSON.stringify({ callbackCount }));
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox zlib write-after-end contract"),
    );
    assert_eq!(sandbox["callbackCount"], Value::from(3));
}
