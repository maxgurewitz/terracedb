use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_zlib_process_chunk_supports_split_decode_paths() {
    let source = r#"
      "use strict";
      const zlib = require("zlib");
      const input = Buffer.from("chunked zlib data ".repeat(24), "utf8");
      const gzip = zlib.gzipSync(input);
      const brotli = zlib.brotliCompressSync(input);
      const unzip = new zlib.Unzip();
      const gunzip = new zlib.Gunzip();
      const brotliDecoder = new zlib.BrotliDecompress();
      const unzipOut = Buffer.concat([
        unzip._processChunk(gzip.subarray(0, 9), zlib.constants.Z_SYNC_FLUSH),
        unzip._processChunk(gzip.subarray(9), zlib.constants.Z_FINISH),
      ]).toString("utf8");
      const gunzipOut = Buffer.concat([
        gunzip._processChunk(gzip.subarray(0, 7), zlib.constants.Z_SYNC_FLUSH),
        gunzip._processChunk(gzip.subarray(7), zlib.constants.Z_FINISH),
      ]).toString("utf8");
      const brotliOut = Buffer.concat([
        brotliDecoder._processChunk(brotli.subarray(0, 5), zlib.constants.Z_SYNC_FLUSH),
        brotliDecoder._processChunk(brotli.subarray(5), zlib.constants.Z_FINISH),
      ]).toString("utf8");
      console.log(JSON.stringify({
        outputs: { unzipOut, gunzipOut, brotliOut },
        handles: {
          gunzipHandleClose: typeof gunzip._handle.close,
          gunzipProcessChunk: typeof gunzip._processChunk,
        },
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox zlib streaming"),
    );
    let expected = Value::String("chunked zlib data ".repeat(24));
    assert_eq!(sandbox["outputs"]["unzipOut"], expected);
    assert_eq!(sandbox["outputs"]["gunzipOut"], expected);
    assert_eq!(sandbox["outputs"]["brotliOut"], expected);
    assert_eq!(
        sandbox["handles"]["gunzipHandleClose"],
        Value::String("function".into())
    );
    assert_eq!(
        sandbox["handles"]["gunzipProcessChunk"],
        Value::String("function".into())
    );
}
