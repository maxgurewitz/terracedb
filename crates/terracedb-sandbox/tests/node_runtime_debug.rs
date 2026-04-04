#[path = "support/node_compat.rs"]
mod node_compat;
#[path = "support/tracing.rs"]
mod tracing_support;

use terracedb_sandbox::NodeDebugExecutionOptions;
use terracedb_vfs::CreateOptions;

fn node_runtime_events(
    result: &terracedb_sandbox::SandboxExecutionResult,
) -> Vec<serde_json::Value> {
    result
        .metadata
        .get("node_runtime_events")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default()
}

#[tokio::test]
async fn node_runtime_debug_structured_events_capture_js_payloads() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        700,
        201,
        entrypoint,
        r#"
        __terraceDebugTrace("demo", { step: "start", answer: 42 });
        console.log(JSON.stringify({ ok: true }));
        "#,
    )
    .await;

    let result = session
        .exec_node_command_with_debug(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
            NodeDebugExecutionOptions::default(),
        )
        .await
        .expect("debug command should succeed");

    let events = node_runtime_events(&result);
    let demo = events
        .iter()
        .find(|event| {
            event["bucket"].as_str() == Some("js") && event["label"].as_str() == Some("demo")
        })
        .expect("structured js event");
    assert_eq!(demo["data"]["step"].as_str(), Some("start"));
    assert_eq!(demo["data"]["answer"].as_i64(), Some(42));
}

#[tokio::test]
async fn node_runtime_debug_autoinstrumentation_captures_exceptions() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        701,
        202,
        entrypoint,
        r#"
        try {
          require("./fail.cjs");
        } catch (error) {
          console.log(JSON.stringify({ name: error.name, message: error.message }));
        }
        "#,
    )
    .await;
    session
        .filesystem()
        .write_file(
            "/workspace/app/fail.cjs",
            b"throw new Error('boom-from-debug');".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write failing module");

    let result = session
        .exec_node_command_with_debug(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
            NodeDebugExecutionOptions {
                autoinstrument_modules: vec!["/workspace/app/fail.cjs".to_string()],
                capture_exceptions: true,
                trace_intrinsics: false,
            },
        )
        .await
        .expect("autoinstrumented command should succeed");

    let events = node_runtime_events(&result);
    assert!(
        events.iter().any(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("autoinstrument-enter")
                && event["data"]["module"].as_str() == Some("/workspace/app/fail.cjs")
        }),
        "expected autoinstrument-enter event, got {events:#?}",
    );
    assert!(
        events.iter().any(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("autoinstrument-throw")
                && event["data"]["message"].as_str() == Some("boom-from-debug")
        }),
        "expected autoinstrument-throw event, got {events:#?}",
    );

    let last_exception = result
        .metadata
        .get("node_runtime_last_exception")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    assert_eq!(
        last_exception["message"].as_str(),
        Some("boom-from-debug"),
        "expected captured exception snapshot, got {last_exception:#?}",
    );
}

#[tokio::test]
async fn node_runtime_debug_intrinsic_sentinels_capture_nullish_to_object() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        702,
        203,
        entrypoint,
        r#"
        try {
          Object.keys(null);
        } catch (_error) {}
        console.log(JSON.stringify({ ok: true }));
        "#,
    )
    .await;

    let result = session
        .exec_node_command_with_debug(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
            NodeDebugExecutionOptions {
                autoinstrument_modules: Vec::new(),
                capture_exceptions: false,
                trace_intrinsics: true,
            },
        )
        .await
        .expect("intrinsic sentinel command should succeed");

    let events = node_runtime_events(&result);
    let sentinel = events
        .iter()
        .find(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("nullish-to-object")
        })
        .expect("nullish-to-object event");
    assert_eq!(sentinel["data"]["method"].as_str(), Some("Object.keys"));
    assert_eq!(sentinel["data"]["argumentIndex"].as_i64(), Some(0));
}

#[tokio::test]
async fn node_runtime_debug_autoinstrumentation_traces_method_calls() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        703,
        204,
        entrypoint,
        r#"
        const counter = require("./counter.cjs");
        console.log(JSON.stringify({
          first: counter.bump(1),
          second: counter.bump(2),
        }));
        "#,
    )
    .await;
    session
        .filesystem()
        .write_file(
            "/workspace/app/counter.cjs",
            br#"
            module.exports = {
              value: 0,
              bump(step) {
                this.value += step;
                return this.value;
              },
            };
            "#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write counter module");

    let result = session
        .exec_node_command_with_debug(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
            NodeDebugExecutionOptions {
                autoinstrument_modules: vec!["/workspace/app/counter.cjs".to_string()],
                capture_exceptions: true,
                trace_intrinsics: false,
            },
        )
        .await
        .expect("autoinstrumented command should succeed");

    let events = node_runtime_events(&result);
    assert!(
        events.iter().any(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("call-enter")
                && event["data"]["module"].as_str() == Some("/workspace/app/counter.cjs")
                && event["data"]["member"].as_str() == Some("bump")
        }),
        "expected call-enter event, got {events:#?}",
    );
    assert!(
        events.iter().any(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("call-exit")
                && event["data"]["module"].as_str() == Some("/workspace/app/counter.cjs")
                && event["data"]["member"].as_str() == Some("bump")
                && event["data"]["result"].as_str() == Some("number")
        }),
        "expected call-exit event, got {events:#?}",
    );
}

#[tokio::test]
async fn node_runtime_debug_autoinstrumentation_preserves_class_constructors() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        704,
        205,
        entrypoint,
        r#"
        const Base = require("./klass.cjs");
        const instance = new Base("hi");
        console.log(JSON.stringify({ value: instance.value, shout: instance.shout() }));
        "#,
    )
    .await;
    session
        .filesystem()
        .write_file(
            "/workspace/app/klass.cjs",
            br#"
            module.exports = class Demo {
              constructor(value) {
                this.value = value;
              }
              shout() {
                return this.value.toUpperCase();
              }
            };
            "#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write class module");

    let result = session
        .exec_node_command_with_debug(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
            NodeDebugExecutionOptions {
                autoinstrument_modules: vec!["/workspace/app/klass.cjs".to_string()],
                capture_exceptions: true,
                trace_intrinsics: false,
            },
        )
        .await
        .expect("autoinstrumented command should succeed");

    let events = node_runtime_events(&result);
    assert!(
        events.iter().any(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("call-enter")
                && event["data"]["module"].as_str() == Some("/workspace/app/klass.cjs")
                && event["data"]["member"].as_str() == Some("shout")
        }),
        "expected prototype method instrumentation, got {events:#?}",
    );
}

#[tokio::test]
async fn node_runtime_debug_autoinstrumentation_traces_private_method_entry() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        705,
        206,
        entrypoint,
        r#"
        const Demo = require("./private.cjs");
        const instance = new Demo();
        console.log(JSON.stringify({ value: instance.run() }));
        "#,
    )
    .await;
    session
        .filesystem()
        .write_file(
            "/workspace/app/private.cjs",
            br#"
            module.exports = class Demo {
              run() {
                return this.#secret();
              }
              #secret() {
                return 42;
              }
            };
            "#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write private method module");

    let result = session
        .exec_node_command_with_debug(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
            NodeDebugExecutionOptions {
                autoinstrument_modules: vec!["/workspace/app/private.cjs".to_string()],
                capture_exceptions: true,
                trace_intrinsics: false,
            },
        )
        .await
        .expect("private method command should succeed");

    let events = node_runtime_events(&result);
    assert!(
        events.iter().any(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("method-enter-source")
                && event["data"]["module"].as_str() == Some("/workspace/app/private.cjs")
                && event["data"]["method"].as_str() == Some("#secret")
        }),
        "expected source-level private method trace, got {events:#?}",
    );
}

#[tokio::test]
async fn node_runtime_debug_autoinstrumentation_preserves_function_static_methods() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        706,
        207,
        entrypoint,
        r#"
        const helper = require("./callable.cjs");
        console.log(JSON.stringify({
          outer: helper("value"),
          staticResult: helper.json("hello"),
          staticType: typeof helper.json,
        }));
        "#,
    )
    .await;
    session
        .filesystem()
        .write_file(
            "/workspace/app/callable.cjs",
            br#"
            function helper(value) {
              return `outer:${value}`;
            }
            helper.json = function json(value) {
              return { wrapped: value };
            };
            module.exports = helper;
            "#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write callable module");

    let result = session
        .exec_node_command_with_debug(
            entrypoint,
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
            NodeDebugExecutionOptions {
                autoinstrument_modules: vec!["/workspace/app/callable.cjs".to_string()],
                capture_exceptions: true,
                trace_intrinsics: false,
            },
        )
        .await
        .expect("callable module command should succeed");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().unwrap_or_default();
    let payload = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .next()
        .expect("structured payload");
    assert_eq!(payload["outer"].as_str(), Some("outer:value"));
    assert_eq!(payload["staticType"].as_str(), Some("function"));
    assert_eq!(payload["staticResult"]["wrapped"].as_str(), Some("hello"));

    let events = node_runtime_events(&result);
    assert!(
        events.iter().any(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("call-enter")
                && event["data"]["module"].as_str() == Some("/workspace/app/callable.cjs")
                && event["data"]["member"].as_str() == Some("<callable>")
        }),
        "expected callable wrapper entry trace, got {events:#?}",
    );
    assert!(
        events.iter().any(|event| {
            event["bucket"].as_str() == Some("js")
                && event["label"].as_str() == Some("call-enter")
                && event["data"]["module"].as_str() == Some("/workspace/app/callable.cjs")
                && event["data"]["member"].as_str() == Some("json")
        }),
        "expected static method trace, got {events:#?}",
    );
}
