#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

#[tokio::test]
async fn npm_cli_version_fails_with_a_clear_node_gap() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(400, 91).await else {
        eprintln!("skipping npm cli bootstrap test because npm/cli repo is unavailable");
        return;
    };

    let error = npm_cli::run_npm_command(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        &["npm", "--version"],
    )
    .await
    .expect_err("npm should fail fast with an explicit unsupported-runtime error");

    match error {
        terracedb_sandbox::SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE_NODE_UNIMPLEMENTED"),
                "expected a clear unsupported runtime error, got: {message}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}
