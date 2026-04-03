use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_stream_pipe_multiple_pipes_fans_out_once_per_target() {
    let source = r#"
      "use strict";
      const stream = require("stream");
      const readable = new stream.Readable({ read() {} });
      const outputs = [];
      const pipeEvents = [];
      const unpipeEvents = [];

      const writables = Array.from({ length: 3 }, (_, index) => {
        const target = new stream.Writable({
          write(chunk, encoding, callback) {
            outputs[index].push(Buffer.from(chunk).toString("hex"));
            callback();
          }
        });
        outputs[index] = [];
        target.on("pipe", () => pipeEvents.push(index));
        target.on("unpipe", () => unpipeEvents.push(index));
        readable.pipe(target);
        return target;
      });

      const input = Buffer.from([1, 2, 3, 4, 5]);
      readable.push(input);

      process.nextTick(() => {
        for (const target of writables) readable.unpipe(target);
        readable.push(Buffer.from([9]));
        readable.push(null);
        readable.resume();
        process.nextTick(() => {
          console.log(JSON.stringify({
            outputs,
            pipeEvents,
            unpipeEvents,
          }));
        });
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox stream multiple pipes"),
    );
    let expected_chunk = Value::from("0102030405");
    assert_eq!(sandbox["outputs"][0][0], expected_chunk);
    assert_eq!(sandbox["outputs"][1][0], expected_chunk);
    assert_eq!(sandbox["outputs"][2][0], expected_chunk);
    assert_eq!(sandbox["outputs"][0].as_array().unwrap().len(), 1);
    assert_eq!(sandbox["outputs"][1].as_array().unwrap().len(), 1);
    assert_eq!(sandbox["outputs"][2].as_array().unwrap().len(), 1);
    assert_eq!(sandbox["pipeEvents"].as_array().unwrap().len(), 3);
    assert_eq!(sandbox["unpipeEvents"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn node_stream_unpipe_event_clears_pipe_bookkeeping() {
    let source = r#"
      "use strict";
      const { Writable, Readable } = require("stream");
      class NullWritable extends Writable {
        _write(chunk, encoding, callback) { callback(); }
      }
      class QuickEndReadable extends Readable {
        _read() { this.push(null); }
      }
      const dest = new NullWritable();
      const src = new QuickEndReadable();
      let pipeCount = 0;
      let unpipeCount = 0;
      dest.on("pipe", () => pipeCount++);
      dest.on("unpipe", () => unpipeCount++);
      src.pipe(dest);
      setImmediate(() => {
        console.log(JSON.stringify({
          pipeCount,
          unpipeCount,
          remainingPipes: src._readableState && src._readableState.pipes && src._readableState.pipes.length,
        }));
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox stream unpipe bookkeeping"),
    );
    assert_eq!(sandbox["pipeCount"], Value::from(1));
    assert_eq!(sandbox["unpipeCount"], Value::from(1));
    assert_eq!(sandbox["remainingPipes"], Value::from(0));
}

#[tokio::test]
async fn node_stream_pipe_flow_and_passthrough_deliver_end() {
    let source = r#"
      "use strict";
      const { Readable, PassThrough } = require("stream");
      const rs = new Readable({
        objectMode: true,
        read() {
          this.push({ ok: true });
          this.push(null);
        }
      });
      const pt = rs.pipe(new PassThrough({ objectMode: true }));
      const seen = [];
      let ended = false;
      pt.on("data", (chunk) => seen.push(chunk.ok));
      pt.on("end", () => {
        ended = true;
        console.log(JSON.stringify({ seen, ended }));
      });
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox stream flow"),
    );
    assert_eq!(sandbox["seen"][0], Value::from(true));
    assert_eq!(sandbox["ended"], Value::from(true));
}

#[tokio::test]
async fn node_stream_writable_cork_properties_follow_node_shape() {
    let source = r#"
      "use strict";
      const { Writable } = require("stream");
      const w = new Writable();
      const values = [];
      values.push(w.writableCorked);
      w.uncork();
      values.push(w.writableCorked);
      w.cork();
      values.push(w.writableCorked);
      w.cork();
      values.push(w.writableCorked);
      w.uncork();
      values.push(w.writableCorked);
      w.uncork();
      values.push(w.writableCorked);
      console.log(JSON.stringify(values));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox writable cork properties"),
    );
    assert_eq!(
        sandbox,
        Value::Array(vec![
            Value::from(0),
            Value::from(0),
            Value::from(1),
            Value::from(2),
            Value::from(1),
            Value::from(0),
        ])
    );
}
