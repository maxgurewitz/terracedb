#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

fn assert_clear_runtime_failure(error: terracedb_sandbox::SandboxError) {
    match error {
        terracedb_sandbox::SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE")
                    || message.contains("node trace:")
                    || message.contains("recent trace:"),
                "expected a deterministic runtime failure with trace context, got: {message}"
            );
        }
        terracedb_sandbox::SandboxError::Service { service, message } => {
            assert!(
                message.contains("ERR_TERRACE") || message.contains("recent trace:"),
                "expected a deterministic service failure, got {service}: {message}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn npm_cli_stage_require_bin_cli_fails_clearly() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(430, 94).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let error = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/bin-stage.cjs",
        r#"
        require("/workspace/npm/bin/npm-cli.js");
        "#,
        &[],
    )
    .await
    .expect_err("bin/npm-cli.js stage should fail deterministically until supported");

    assert_clear_runtime_failure(error);
}

#[tokio::test]
async fn npm_cli_stage_require_lib_cli_fails_clearly() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(431, 95).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let error = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/lib-cli-stage.cjs",
        r#"
        const cli = require("/workspace/npm/lib/cli.js");
        cli(process);
        "#,
        &[],
    )
    .await
    .expect_err("lib/cli.js stage should fail deterministically until supported");

    assert_clear_runtime_failure(error);
}

#[tokio::test]
async fn npm_cli_stage_require_entry_fails_clearly() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(432, 96).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let error = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/entry-stage.cjs",
        r#"
        const entry = require("/workspace/npm/lib/cli/entry.js");
        entry(process, { node: process.version, npm: "v0.0.0", engines: ">=0", unsupportedMessage: "", off() {} });
        "#,
        &[],
    )
    .await
    .expect_err("lib/cli/entry.js stage should fail deterministically until supported");

    assert_clear_runtime_failure(error);
}
