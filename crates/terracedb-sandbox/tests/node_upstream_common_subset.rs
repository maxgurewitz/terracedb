#[path = "support/node_compat.rs"]
mod node_compat;

fn stdout_stderr_exit(result: &terracedb_sandbox::SandboxExecutionResult) -> (String, String, i64) {
    let report = result
        .result
        .as_ref()
        .and_then(|value| value.as_object())
        .expect("node command report");
    let stdout = report
        .get("stdout")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let stderr = report
        .get("stderr")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let exit_code = report
        .get("exitCode")
        .and_then(|value| value.as_i64())
        .unwrap_or_default();
    (stdout, stderr, exit_code)
}

#[tokio::test]
async fn upstream_test_module_create_require_runs() {
    let result = node_compat::exec_upstream_node_test(
        "/node/test/parallel/test-module-create-require.js",
    )
    .await
    .expect("execute upstream test");
    let (stdout, stderr, exit_code) = stdout_stderr_exit(&result);
    assert_eq!(
        exit_code, 0,
        "stdout:\n{}\n\nstderr:\n{}",
        stdout, stderr
    );
}

#[tokio::test]
async fn upstream_test_require_cache_runs() {
    let result = node_compat::exec_upstream_node_test(
        "/node/test/parallel/test-require-cache.js",
    )
    .await
    .expect("execute upstream test");
    let (stdout, stderr, exit_code) = stdout_stderr_exit(&result);
    assert_eq!(
        exit_code, 0,
        "stdout:\n{}\n\nstderr:\n{}",
        stdout, stderr
    );
}

#[tokio::test]
async fn upstream_test_module_builtin_runs() {
    let result = node_compat::exec_upstream_node_test(
        "/node/test/parallel/test-module-builtin.js",
    )
    .await
    .expect("execute upstream test");
    let (stdout, stderr, exit_code) = stdout_stderr_exit(&result);
    assert_eq!(
        exit_code, 0,
        "stdout:\n{}\n\nstderr:\n{}",
        stdout, stderr
    );
}

#[tokio::test]
async fn upstream_test_require_node_prefix_runs() {
    let result = node_compat::exec_upstream_node_test(
        "/node/test/parallel/test-require-node-prefix.js",
    )
    .await
    .expect("execute upstream test");
    let (stdout, stderr, exit_code) = stdout_stderr_exit(&result);
    assert_eq!(
        exit_code, 0,
        "stdout:\n{}\n\nstderr:\n{}",
        stdout, stderr
    );
}

#[tokio::test]
async fn upstream_test_require_resolve_runs() {
    let result = node_compat::exec_upstream_node_test(
        "/node/test/parallel/test-require-resolve.js",
    )
    .await
    .expect("execute upstream test");
    let (stdout, stderr, exit_code) = stdout_stderr_exit(&result);
    assert_eq!(
        exit_code, 0,
        "stdout:\n{}\n\nstderr:\n{}",
        stdout, stderr
    );
}
