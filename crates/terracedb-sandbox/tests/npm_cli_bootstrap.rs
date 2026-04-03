#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

#[tokio::test]
async fn npm_cli_version_reports_version() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(400, 91).await else {
        eprintln!("skipping npm cli bootstrap test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_npm_command(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        &["npm", "--version"],
    )
    .await
    .expect("npm --version should execute deterministically");

    let report = result.result.expect("node command report");
    assert_eq!(report["exitCode"].as_i64(), Some(0), "{report:#?}");
    assert!(
        report["stdout"]
            .as_str()
            .map(str::trim)
            .map(|value| !value.is_empty())
            .unwrap_or(false),
        "expected npm --version to print a version string, got: {report:#?}"
    );
}
