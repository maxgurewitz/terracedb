#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

#[tokio::test]
async fn npm_cli_init_y_writes_project_manifest() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(410, 92).await else {
        eprintln!("skipping npm cli init test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_npm_command(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        &["npm", "init", "-y"],
    )
    .await
    .expect("npm init -y should complete in the sandbox");

    let report = result.result.clone().expect("node command report");
    assert_eq!(
        report["exitCode"].as_i64(),
        Some(0),
        "expected npm init to exit cleanly, got report: {report:#?}"
    );

    let package_json = session
        .filesystem()
        .read_file("/workspace/project/package.json")
        .await
        .expect("read generated package.json")
        .expect("generated package.json should exist");
    let package_json: serde_json::Value =
        serde_json::from_slice(&package_json).expect("decode generated package.json");

    assert_eq!(package_json["name"].as_str(), Some("project"));
    assert_eq!(package_json["version"].as_str(), Some("1.0.0"));
    assert_eq!(package_json["main"].as_str(), Some("index.js"));
    assert!(
        result
            .module_graph
            .iter()
            .any(|path| path.ends_with("/lib/commands/init.js")),
        "expected npm to load the init command, got module graph: {:#?}",
        result.module_graph
    );
}
