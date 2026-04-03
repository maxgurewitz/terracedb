use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_events_once_catches_errors_and_abort_signal() {
    let source = r#"
      "use strict";
      const events = require("events");
      const ee = new events.EventEmitter();
      const ac = new AbortController();
      const signal = ac.signal;

      (async () => {
        const resolved = [];
        process.nextTick(() => ee.emit("value", 42, 24));
        resolved.push(await events.once(ee, "value"));

        let errorMessage = null;
        process.nextTick(() => ee.emit("error", new Error("kaboom")));
        try {
          await events.once(ee, "other");
        } catch (error) {
          errorMessage = error && error.message;
        }

        let abortName = null;
        const aborted = events.once(ee, "never", { signal });
        process.nextTick(() => ac.abort());
        try {
          await aborted;
        } catch (error) {
          abortName = error && error.name;
        }

        const et = new EventTarget();
        process.nextTick(() => et.dispatchEvent(new Event("tick")));
        const targetResult = await events.once(et, "tick");

        console.log(JSON.stringify({
          resolved,
          errorMessage,
          abortName,
          targetResultType: targetResult[0] && targetResult[0].constructor && targetResult[0].constructor.name,
          remainingValueListeners: ee.listenerCount("value"),
          remainingErrorListeners: ee.listenerCount("error"),
        }));
      })();
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox events.once behavior"),
    );
    assert_eq!(sandbox["resolved"][0][0], Value::from(42));
    assert_eq!(sandbox["resolved"][0][1], Value::from(24));
    assert_eq!(sandbox["errorMessage"], Value::from("kaboom"));
    assert_eq!(sandbox["abortName"], Value::from("AbortError"));
    assert_eq!(sandbox["targetResultType"], Value::from("Event"));
    assert_eq!(sandbox["remainingValueListeners"], Value::from(0));
    assert_eq!(sandbox["remainingErrorListeners"], Value::from(0));
}

#[tokio::test]
async fn node_events_static_helpers_cover_event_emitter_and_event_target() {
    let source = r#"
      "use strict";
      const events = require("events");
      const emitter = new events.EventEmitter();
      const target = new EventTarget();
      const ac = new AbortController();
      const fn1 = () => {};
      const fn2 = () => {};

      emitter.on("foo", fn1);
      emitter.on("foo", fn2);
      target.addEventListener("bar", fn1);
      target.addEventListener("bar", fn2);
      target.addEventListener("baz", fn1);
      target.addEventListener("baz", fn1);

      events.setMaxListeners(101, emitter, target);

      console.log(JSON.stringify({
        emitterListeners: events.getEventListeners(emitter, "foo").length,
        targetBarListeners: events.getEventListeners(target, "bar").length,
        targetBazListeners: events.getEventListeners(target, "baz").length,
        emitterMax: events.getMaxListeners(emitter),
        targetMax: events.getMaxListeners(target),
        signalMax: events.getMaxListeners(ac.signal),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox events static helpers"),
    );
    assert_eq!(sandbox["emitterListeners"], Value::from(2));
    assert_eq!(sandbox["targetBarListeners"], Value::from(2));
    assert_eq!(sandbox["targetBazListeners"], Value::from(1));
    assert_eq!(sandbox["emitterMax"], Value::from(101));
    assert_eq!(sandbox["targetMax"], Value::from(101));
    assert_eq!(sandbox["signalMax"], Value::from(0));
}

#[tokio::test]
async fn node_event_emitter_error_without_listener_throws_unhandled_error() {
    let source = r#"
      "use strict";
      const { EventEmitter } = require("events");
      const emitter = new EventEmitter();
      try {
        emitter.emit("error", "boom");
      } catch (error) {
        console.log(JSON.stringify({
          code: error && error.code,
          context: error && error.context,
        }));
      }
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox unhandled error path"),
    );
    assert_eq!(sandbox["code"], Value::from("ERR_UNHANDLED_ERROR"));
    assert_eq!(sandbox["context"], Value::from("boom"));
}
