use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;
#[path = "support/npm_cli.rs"]
mod npm_cli;

fn parse_stdout_json(stdout: &str) -> Value {
    serde_json::from_str(stdout.trim()).expect("stdout json")
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

fn render_options_source(options_require: &str) -> String {
    format!(
        r#"
        const {{ normalizeOptions, cacheOptions }} = require({options_require});
        const snapshot = {{
          familyDefault: normalizeOptions({{}}),
          familyFour: normalizeOptions({{ family: 4 }}),
          familySixString: normalizeOptions({{ family: "6" }}),
          timeoutRewritten: normalizeOptions({{ timeout: 100 }}),
          cacheKeyDefault: cacheOptions(normalizeOptions({{ secureEndpoint: false }})),
          cacheKeyHttps: cacheOptions({{
            ...normalizeOptions({{
              rejectUnauthorized: false,
              keepAlive: true,
              proxy: "http://proxy.example:8080",
              timeouts: {{ idle: 100, response: 25 }},
            }}),
            secureEndpoint: true,
          }}),
        }};
        console.log(JSON.stringify(snapshot));
        "#,
        options_require = serde_json::to_string(options_require).expect("quoted options require")
    )
}

#[tokio::test]
async fn npmcli_agent_options_contract_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping @npmcli/agent options contract because npm/cli root is unavailable");
        return;
    };

    let sandbox_source =
        render_options_source("/workspace/npm/node_modules/@npmcli/agent/lib/options.js");
    let real_source = render_options_source(&format!(
        "{npm_root}/node_modules/@npmcli/agent/lib/options.js"
    ));

    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(470, 150).await else {
        eprintln!("skipping @npmcli/agent options contract because npm/cli session is unavailable");
        return;
    };

    let sandbox = sandbox_stdout_json(
        npm_cli::run_inline_node_script(
            &session,
            npm_cli::SANDBOX_PROJECT_ROOT,
            "/workspace/project/.terrace/npmcli-agent-options-contract.cjs",
            &sandbox_source,
            &[],
        )
        .await
        .expect("sandbox @npmcli/agent options contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(&real_source)
            .expect("real node @npmcli/agent options contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
