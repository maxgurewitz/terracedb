use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_zlib_crc32_matches_known_vectors() {
    let source = r#"
      "use strict";
      const zlib = require("zlib");
      console.log(JSON.stringify({
        empty: zlib.crc32(Buffer.alloc(0)),
        ascii: zlib.crc32("zlib"),
        seeded: zlib.crc32("zlib", 0x73c84278),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox zlib crc32"),
    );
    assert_eq!(sandbox["empty"], Value::from(0_u64));
    assert_eq!(sandbox["ascii"], Value::from(0x73887d3a_u64));
    assert_eq!(sandbox["seeded"], Value::from(0x6432d127_u64));
}
