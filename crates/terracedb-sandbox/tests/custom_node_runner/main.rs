#[path = "../support/node_compat.rs"]
mod node_compat;

use std::{thread, time::Duration};

use tokio::{runtime::Builder, time::timeout};

fn stdout_stderr_exit(
    result: &terracedb_sandbox::SandboxExecutionResult,
) -> (String, String, i64) {
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

async fn run_upstream_subcase(path: &'static str, case_timeout: Duration) -> Result<(), String> {
    let result = timeout(case_timeout, node_compat::exec_upstream_node_test(path))
        .await
        .map_err(|_| format!("timed out after {:?}: {path}", case_timeout))?
        .map_err(|error| format!("sandbox execution failed for {path}: {error}"))?;

    let (stdout, stderr, exit_code) = stdout_stderr_exit(&result);
    if exit_code != 0 {
        return Err(format!(
            "non-zero exit for {path}\nexit: {exit_code}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        ));
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_commonjs_custom_runner_single_case() {
    const CASE_TIMEOUT: Duration = Duration::from_secs(30);
    let cases = ["/test/parallel/test-module-create-require.js"];
    let mut threads = Vec::new();

    for path in cases {
        threads.push(thread::spawn(move || {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread tokio runtime")
                .block_on(run_upstream_subcase(path, CASE_TIMEOUT))
        }));
    }

    let mut failures = Vec::new();
    for thread in threads {
        match thread.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(error),
            Err(payload) => failures.push(format!("runner thread panicked: {:?}", payload.type_id())),
        }
    }

    assert!(
        failures.is_empty(),
        "custom upstream runner failures:\n{}",
        failures.join("\n\n")
    );
}
