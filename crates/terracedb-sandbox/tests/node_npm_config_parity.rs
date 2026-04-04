#[path = "support/node_compat.rs"]
mod node_compat;
#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

use serde_json::Value;

fn parse_json_lines(stdout: &str) -> Vec<Value> {
    stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .map(|line| serde_json::from_str::<Value>(line).expect("json line"))
        .collect()
}

fn npm_config_probe_source(npm_root: &str) -> String {
    let npm_root = serde_json::to_string(npm_root).expect("serialize npm root");
    format!(
        r#"
const npmRoot = {npm_root};
const Npm = require(`${{npmRoot}}/lib/npm.js`);

const describe = (label, npm) => {{
  const config = npm.config;
  const proto = Object.getPrototypeOf(config);
  console.log(JSON.stringify({{
    label,
    hasConfig: !!config,
    configType: typeof config,
    constructorName: config && config.constructor ? config.constructor.name : null,
    hasOwnLogWarnings: !!(config && Object.prototype.hasOwnProperty.call(config, "logWarnings")),
    logWarningsType: config ? typeof config.logWarnings : "missing",
    getType: config ? typeof config.get : "missing",
    protoConstructorName: proto && proto.constructor ? proto.constructor.name : null,
    protoHasLogWarnings: !!(proto && Object.prototype.hasOwnProperty.call(proto, "logWarnings")),
    protoLogWarningsType: proto ? typeof proto.logWarnings : "missing",
    protoGetType: proto ? typeof proto.get : "missing",
    protoKeys: proto ? Object.getOwnPropertyNames(proto).sort() : [],
  }}));
}};

(async () => {{
  const npm = new Npm({{ argv: ["install", "--ignore-scripts", "--no-audit", "--no-fund"] }});
  describe("constructed", npm);
  try {{
    await npm.load();
    describe("loaded", npm);
  }} catch (error) {{
    describe("load-error", npm);
    console.log(JSON.stringify({{
      label: "load-error-detail",
      name: error && error.name ? error.name : null,
      message: error && error.message ? error.message : String(error),
      stack: error && error.stack ? error.stack : null,
    }}));
  }}
}})();
"#
    )
}

#[tokio::test]
async fn sandbox_npm_config_preserves_logwarnings_shape() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(442, 106).await else {
        eprintln!("skipping npm config parity test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/npm-config-parity.cjs",
        npm_config_probe_source(npm_cli::SANDBOX_NPM_ROOT).as_str(),
        &[],
    )
    .await
    .expect("sandbox config probe should execute");

    let report = result.result.clone().expect("node command report");
    let payloads = parse_json_lines(report["stdout"].as_str().unwrap_or_default());
    let constructed = payloads
        .iter()
        .find(|payload| payload["label"].as_str() == Some("constructed"))
        .expect("constructed payload");
    let loaded = payloads
        .iter()
        .find(|payload| payload["label"].as_str() == Some("loaded"))
        .expect("loaded payload");
    let sandbox_payloads = serde_json::json!({
        "constructed": constructed,
        "loaded": loaded,
    });

    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping npm config parity comparison because npm/cli repo is unavailable");
        return;
    };
    if !node_compat::real_node_available() {
        eprintln!("skipping npm config parity comparison because node is unavailable");
        return;
    }
    let output = node_compat::exec_real_node_eval(npm_config_probe_source(&npm_root).as_str())
        .expect("run real node config probe");
    assert_eq!(
        output.exit_code, 0,
        "real node probe should exit cleanly\nstdout:\n{}\n\nstderr:\n{}",
        output.stdout, output.stderr,
    );
    let real_payloads = parse_json_lines(&output.stdout);
    let real_constructed = real_payloads
        .iter()
        .find(|payload| payload["label"].as_str() == Some("constructed"))
        .expect("real constructed payload");
    let real_loaded = real_payloads
        .iter()
        .find(|payload| payload["label"].as_str() == Some("loaded"))
        .expect("real loaded payload");
    let real_payloads = serde_json::json!({
        "constructed": real_constructed,
        "loaded": real_loaded,
    });
    assert_eq!(
        sandbox_payloads,
        real_payloads,
        "sandbox npm.config shape should match real node\nsandbox stdout:\n{}\n\nsandbox stderr:\n{}\n\nreal stdout:\n{}\n\nreal stderr:\n{}\n\ntrace:\n{:#?}",
        report["stdout"].as_str().unwrap_or_default(),
        report["stderr"].as_str().unwrap_or_default(),
        output.stdout,
        output.stderr,
        npm_cli::node_runtime_trace(&result),
    );
}

#[test]
fn real_node_npm_config_exposes_logwarnings_shape() {
    tracing_support::init_tracing();
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping real node npm config parity test because npm/cli repo is unavailable");
        return;
    };
    if !node_compat::real_node_available() {
        eprintln!("skipping real node npm config parity test because node is unavailable");
        return;
    }

    let output = node_compat::exec_real_node_eval(npm_config_probe_source(&npm_root).as_str())
        .expect("run real node config probe");
    assert_eq!(
        output.exit_code, 0,
        "real node probe should exit cleanly\nstdout:\n{}\n\nstderr:\n{}",
        output.stdout, output.stderr,
    );

    let payloads = parse_json_lines(&output.stdout);
    assert!(
        payloads
            .iter()
            .any(|payload| payload["label"].as_str() == Some("constructed")),
        "expected a constructed payload\nstdout:\n{}\n\nstderr:\n{}",
        output.stdout,
        output.stderr,
    );
    assert!(
        payloads
            .iter()
            .any(|payload| payload["label"].as_str() == Some("loaded")),
        "expected a loaded payload\nstdout:\n{}\n\nstderr:\n{}",
        output.stdout,
        output.stderr,
    );
}
