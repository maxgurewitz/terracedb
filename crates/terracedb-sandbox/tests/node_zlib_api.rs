use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_zlib_exports_and_roundtrips_cover_core_codecs() {
    let source = r#"
      "use strict";
      const zlib = require("zlib");
      const input = Buffer.from("hello terrace zlib ".repeat(32), "utf8");
      const gzip = zlib.gzipSync(input);
      const deflate = zlib.deflateSync(input);
      const raw = zlib.deflateRawSync(input);
      const brotli = zlib.brotliCompressSync(input);
      const zstd = zlib.zstdCompressSync(input);
      const gzipInfo = zlib.gzipSync(input, { info: true });
      console.log(JSON.stringify({
        hasCoreExports: [
          "constants",
          "Gzip",
          "Gunzip",
          "Deflate",
          "Inflate",
          "DeflateRaw",
          "InflateRaw",
          "Unzip",
          "BrotliCompress",
          "BrotliDecompress",
          "ZstdCompress",
          "ZstdDecompress",
          "createGzip",
          "createGunzip",
          "gzipSync",
          "gunzipSync",
          "brotliCompressSync",
          "brotliDecompressSync",
          "zstdCompressSync",
          "zstdDecompressSync",
          "crc32",
        ].every((key) => Object.prototype.hasOwnProperty.call(zlib, key)),
        factories: {
          inflateRaw: zlib.createInflateRaw() instanceof zlib.InflateRaw,
          gzip: zlib.createGzip() instanceof zlib.Gzip,
          unzip: zlib.createUnzip() instanceof zlib.Unzip,
        },
        roundtrip: {
          gzip: zlib.gunzipSync(gzip).toString("utf8"),
          deflate: zlib.inflateSync(deflate).toString("utf8"),
          raw: zlib.inflateRawSync(raw).toString("utf8"),
          brotli: zlib.brotliDecompressSync(brotli).toString("utf8"),
          zstd: zlib.zstdDecompressSync(zstd).toString("utf8"),
        },
        infoShape: {
          hasBuffer: Buffer.isBuffer(gzipInfo.buffer),
          engineName: gzipInfo.engine && gzipInfo.engine.constructor && gzipInfo.engine.constructor.name,
        },
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox zlib behavior"),
    );
    assert_eq!(sandbox["hasCoreExports"], Value::Bool(true));
    assert_eq!(sandbox["factories"]["inflateRaw"], Value::Bool(true));
    assert_eq!(sandbox["factories"]["gzip"], Value::Bool(true));
    assert_eq!(sandbox["factories"]["unzip"], Value::Bool(true));
    let expected = Value::String("hello terrace zlib ".repeat(32));
    assert_eq!(sandbox["roundtrip"]["gzip"], expected);
    assert_eq!(sandbox["roundtrip"]["deflate"], expected);
    assert_eq!(sandbox["roundtrip"]["raw"], expected);
    assert_eq!(sandbox["roundtrip"]["brotli"], expected);
    assert_eq!(sandbox["roundtrip"]["zstd"], expected);
    assert_eq!(sandbox["infoShape"]["hasBuffer"], Value::Bool(true));
    assert_eq!(
        sandbox["infoShape"]["engineName"],
        Value::String("Gzip".into())
    );
}
