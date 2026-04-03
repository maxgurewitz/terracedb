#[path = "support/node_compat.rs"]
mod node_compat_support;
#[path = "support/tracing.rs"]
mod tracing_support;

fn sandbox_stdout(result: terracedb_sandbox::SandboxExecutionResult) -> String {
    result.result.expect("node command report")["stdout"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

#[tokio::test]
async fn cjs_async_iife_microtask_order_matches_real_node() {
    tracing_support::init_tracing();
    if !node_compat_support::real_node_available() {
        eprintln!("skipping parity test because node is unavailable");
        return;
    }

    let source = r#"
        console.log("A");
        (async () => {
          console.log("B");
          await Promise.resolve();
          console.log("D");
        })();
        console.log("C");
    "#;

    let expected = node_compat_support::exec_real_node_eval(source).expect("run real node");
    let actual = node_compat_support::exec_node_fixture(source)
        .await
        .expect("run sandbox node command");

    assert_eq!(expected.exit_code, 0, "{expected:#?}");
    assert_eq!(sandbox_stdout(actual), expected.stdout);
}

#[tokio::test]
async fn cjs_pending_promise_does_not_keep_process_alive_like_real_node() {
    tracing_support::init_tracing();
    if !node_compat_support::real_node_available() {
        eprintln!("skipping parity test because node is unavailable");
        return;
    }

    let source = r#"
        console.log("A");
        (async () => {
          console.log("B");
          await new Promise(() => {});
          console.log("D");
        })();
        console.log("C");
    "#;

    let expected = node_compat_support::exec_real_node_eval(source).expect("run real node");
    let actual = node_compat_support::exec_node_fixture(source)
        .await
        .expect("run sandbox node command");

    assert_eq!(expected.exit_code, 0, "{expected:#?}");
    assert_eq!(sandbox_stdout(actual), expected.stdout);
}

#[tokio::test]
async fn process_next_tick_order_matches_real_node() {
    tracing_support::init_tracing();
    if !node_compat_support::real_node_available() {
        eprintln!("skipping parity test because node is unavailable");
        return;
    }

    let source = r#"
        const out = [];
        process.nextTick(() => out.push("nextTick"));
        Promise.resolve().then(() => out.push("promise"));
        queueMicrotask(() => out.push("queueMicrotask"));
        queueMicrotask(() => console.log(out.join(",")));
        out.push("sync");
    "#;

    let expected = node_compat_support::exec_real_node_eval(source).expect("run real node");
    let actual = node_compat_support::exec_node_fixture(source)
        .await
        .expect("run sandbox node command");

    assert_eq!(expected.exit_code, 0, "{expected:#?}");
    assert_eq!(sandbox_stdout(actual), expected.stdout);
}

#[tokio::test]
async fn cjs_returned_thenable_is_ignored_like_real_node() {
    tracing_support::init_tracing();
    if !node_compat_support::real_node_available() {
        eprintln!("skipping parity test because node is unavailable");
        return;
    }

    let source = r#"
        console.log("A");
        return Promise.resolve().then(() => console.log("B"));
    "#;

    let expected = node_compat_support::exec_real_node_file(source).expect("run real node");
    let actual = node_compat_support::exec_node_fixture(source)
        .await
        .expect("run sandbox node command");

    assert_eq!(expected.exit_code, 0, "{expected:#?}");
    assert_eq!(sandbox_stdout(actual), expected.stdout);
}
