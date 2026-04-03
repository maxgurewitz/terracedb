#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

use terracedb_vfs::CreateOptions;

#[tokio::test]
async fn npm_cli_local_install_fails_with_a_clear_node_gap() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(420, 93).await else {
        eprintln!("skipping npm cli local install test because npm/cli repo is unavailable");
        return;
    };

    session
        .filesystem()
        .write_file(
            "/workspace/project/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "project",
                "version": "1.0.0"
            }))
            .expect("encode root package.json"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write project package.json");

    session
        .filesystem()
        .write_file(
            "/workspace/project/local-dep/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "local-dep",
                "version": "1.2.3",
                "main": "index.js"
            }))
            .expect("encode local dependency package.json"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write local dependency manifest");
    session
        .filesystem()
        .write_file(
            "/workspace/project/local-dep/index.js",
            b"module.exports = 'local-dep';\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write local dependency index");

    let error = npm_cli::run_npm_command(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        &[
            "npm",
            "install",
            "./local-dep",
            "--ignore-scripts",
            "--no-audit",
            "--no-fund",
        ],
    )
    .await
    .expect_err("npm install should fail fast with an explicit unsupported-runtime error");

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
