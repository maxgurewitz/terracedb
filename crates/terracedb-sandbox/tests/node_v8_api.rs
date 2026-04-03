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
async fn node_v8_exports_and_shapes_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const v8 = require("node:v8");
      const serialized = v8.serialize({ a: 1, b: ["x"] });
      const roundtrip = v8.deserialize(serialized);
      const snapshot = v8.getHeapSnapshot();
      console.log(JSON.stringify({
        keys: Object.keys(v8).sort(),
        heapTypes: Object.fromEntries(Object.entries(v8.getHeapStatistics()).map(([k, v]) => [k, typeof v])),
        codeTypes: Object.fromEntries(Object.entries(v8.getHeapCodeStatistics()).map(([k, v]) => [k, typeof v])),
        spaceKeys: Object.keys(v8.getHeapSpaceStatistics()[0] || {}).sort(),
        snapshot: {
          name: snapshot.constructor.name,
          methods: ["pause", "read", "destroy", "on", "pipe"].map((key) => typeof snapshot[key]),
        },
        startup: Object.keys(v8.startupSnapshot).sort(),
        promiseHooks: Object.keys(v8.promiseHooks).sort(),
        serializedIsBuffer: Buffer.isBuffer(serialized),
        roundtrip,
        oneByte: [
          v8.isStringOneByteRepresentation("abc"),
          v8.isStringOneByteRepresentation("snowman-\u2603"),
        ],
      }));
      snapshot.destroy();
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox v8 api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node v8 api")
            .stdout,
    );

    assert_eq!(sandbox["keys"], real["keys"]);
    assert_eq!(sandbox["heapTypes"], real["heapTypes"]);
    assert_eq!(sandbox["codeTypes"], real["codeTypes"]);
    assert_eq!(sandbox["spaceKeys"], real["spaceKeys"]);
    assert_eq!(sandbox["snapshot"], real["snapshot"]);
    assert_eq!(sandbox["startup"], real["startup"]);
    assert_eq!(sandbox["promiseHooks"], real["promiseHooks"]);
    assert_eq!(sandbox["serializedIsBuffer"], real["serializedIsBuffer"]);
    assert_eq!(sandbox["roundtrip"], real["roundtrip"]);
    assert_eq!(sandbox["oneByte"], real["oneByte"]);
}

#[tokio::test]
async fn node_v8_write_heap_snapshot_materializes_file() {
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat_support::open_node_session(
        540,
        151,
        entrypoint,
        r#"
          const v8 = require("node:v8");
          const path = v8.writeHeapSnapshot("/workspace/app/heap.heapsnapshot");
          console.log(JSON.stringify({ path }));
        "#,
    )
    .await;

    let result = session
        .exec_node_command(
            entrypoint.to_string(),
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
        .expect("node v8 writeHeapSnapshot should execute");

    let payload = sandbox_stdout_json(result);
    assert_eq!(
        payload["path"].as_str(),
        Some("/workspace/app/heap.heapsnapshot")
    );

    let snapshot = session
        .filesystem()
        .read_file("/workspace/app/heap.heapsnapshot")
        .await
        .expect("read heap snapshot file")
        .expect("heap snapshot should exist");
    let snapshot_text = String::from_utf8(snapshot).expect("heap snapshot utf8");
    let snapshot_json: Value =
        serde_json::from_str(&snapshot_text).expect("heap snapshot should be json");
    assert_eq!(
        snapshot_json["meta"]["node"].as_str(),
        Some("v24.12.0"),
        "expected heap snapshot to reflect sandbox node version shape, got: {snapshot_json:#?}"
    );
}
