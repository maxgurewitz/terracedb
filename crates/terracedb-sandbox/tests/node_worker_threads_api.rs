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
async fn node_worker_threads_exports_match_real_node_main_thread_shape() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const wt = require("node:worker_threads");
      const channel = new wt.MessageChannel();
      channel.port1.postMessage({ value: 42 });
      const queued = wt.receiveMessageOnPort(channel.port2);
      wt.setEnvironmentData("answer", 42);
      console.log(JSON.stringify({
        keys: Object.keys(wt).sort(),
        types: Object.fromEntries(Object.entries(wt).map(([k, v]) => [k, typeof v])),
        workerProto: Object.getOwnPropertyNames(wt.Worker.prototype).sort(),
        portProto: Object.getOwnPropertyNames(wt.MessagePort.prototype).sort(),
        broadcastProto: Object.getOwnPropertyNames(wt.BroadcastChannel.prototype).sort(),
        isMainThread: wt.isMainThread,
        isInternalThread: wt.isInternalThread,
        threadId: wt.threadId,
        parentPortIsNull: wt.parentPort === null,
        threadNameType: typeof wt.threadName,
        envRoundtrip: wt.getEnvironmentData("answer"),
        receiveMessage: queued ? queued.message : null,
        locksOwn: Object.getOwnPropertyNames(wt.locks || {}).sort(),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox worker_threads api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node worker_threads api")
            .stdout,
    );

    assert_eq!(sandbox["keys"], real["keys"]);
    assert_eq!(sandbox["types"], real["types"]);
    assert_eq!(sandbox["workerProto"], real["workerProto"]);
    assert_eq!(sandbox["portProto"], real["portProto"]);
    assert_eq!(sandbox["broadcastProto"], real["broadcastProto"]);
    assert_eq!(sandbox["isMainThread"], real["isMainThread"]);
    assert_eq!(sandbox["isInternalThread"], real["isInternalThread"]);
    assert_eq!(sandbox["threadId"], real["threadId"]);
    assert_eq!(sandbox["parentPortIsNull"], real["parentPortIsNull"]);
    assert_eq!(sandbox["threadNameType"], real["threadNameType"]);
    assert_eq!(sandbox["envRoundtrip"], real["envRoundtrip"]);
    assert_eq!(sandbox["receiveMessage"], real["receiveMessage"]);
    assert_eq!(sandbox["locksOwn"], real["locksOwn"]);
}

#[tokio::test]
async fn node_worker_threads_message_primitives_work_in_sandbox() {
    let source = r#"
      const wt = require("node:worker_threads");
      const channel = new wt.MessageChannel();
      const received = [];
      channel.port2.on("message", (value) => received.push(value));
      channel.port1.postMessage({ hello: "world" });

      const broadcast = new wt.BroadcastChannel("demo");
      const broadcastPeer = new wt.BroadcastChannel("demo");
      const broadcastMessages = [];
      broadcastPeer.onmessage = (event) => broadcastMessages.push(event.data);
      broadcast.postMessage("ping");
      const value = {};
      wt.markAsUntransferable(value);

      return Promise.resolve().then(() => {
        console.log(JSON.stringify({
          received,
          broadcastMessages,
          markedAfter: wt.isMarkedAsUntransferable(value),
          movedType: typeof wt.moveMessagePortToContext(channel.port1, {}),
        }));
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox worker_threads message primitives"),
    );

    assert_eq!(
        sandbox["received"],
        serde_json::json!([{ "hello": "world" }])
    );
    assert_eq!(sandbox["broadcastMessages"], serde_json::json!(["ping"]));
    assert_eq!(sandbox["markedAfter"], Value::Bool(true));
    assert_eq!(sandbox["movedType"], Value::String("object".into()));
}
