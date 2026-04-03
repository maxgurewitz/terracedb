use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_os_exports_and_env_behavior_match_expected_contract() {
    let source = r#"
      "use strict";
      const os = require("os");
      const originalEnv = { ...process.env };
      process.env.TMPDIR = "/tmpdir";
      process.env.TMP = "/tmp";
      process.env.TEMP = "/temp";
      const tmp0 = os.tmpdir();
      process.env.TMPDIR = "";
      const tmp1 = os.tmpdir();
      process.env.TMP = "";
      const tmp2 = os.tmpdir();
      process.env.TEMP = "";
      const tmp3 = os.tmpdir();
      process.env.TMPDIR = "/tmpdir/";
      const tmp4 = os.tmpdir();
      process.env.TMPDIR = "/tmpdir\\\\";
      const tmp5 = os.tmpdir();
      process.env.HOME = "/workspace/custom-home";
      const home = os.homedir();
      process.env = originalEnv;
      let eolAssign;
      try {
        os.EOL = "x";
        eolAssign = "mutated";
      } catch (error) {
        eolAssign = { name: error.name, code: error.code ?? null };
      }
      console.log(JSON.stringify({
        keys: Object.keys(os).sort(),
        tmp: [tmp0, tmp1, tmp2, tmp3, tmp4, tmp5],
        home,
        eol: os.EOL,
        eolAssign,
        eolDescriptor: Object.getOwnPropertyDescriptor(os, "EOL"),
        devNull: os.devNull,
        machine: os.machine(),
        availableParallelism: os.availableParallelism(),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox os behavior"),
    );
    assert_eq!(
        sandbox["keys"],
        serde_json::json!([
            "EOL",
            "arch",
            "availableParallelism",
            "constants",
            "cpus",
            "devNull",
            "endianness",
            "freemem",
            "getPriority",
            "homedir",
            "hostname",
            "loadavg",
            "machine",
            "networkInterfaces",
            "platform",
            "release",
            "setPriority",
            "tmpdir",
            "totalmem",
            "type",
            "uptime",
            "userInfo",
            "version",
        ])
    );
    assert_eq!(
        sandbox["tmp"],
        serde_json::json!(["/tmpdir", "/tmp", "/temp", "/tmp", "/tmpdir", "/tmpdir\\\\"])
    );
    assert_eq!(
        sandbox["home"],
        Value::String("/workspace/custom-home".into())
    );
    assert_eq!(sandbox["eol"], Value::String("\n".into()));
    assert_eq!(
        sandbox["eolAssign"]["name"],
        Value::String("TypeError".into())
    );
    assert_eq!(sandbox["eolDescriptor"]["writable"], Value::Bool(false));
    assert_eq!(sandbox["eolDescriptor"]["configurable"], Value::Bool(true));
    assert_eq!(sandbox["eolDescriptor"]["enumerable"], Value::Bool(true));
    assert_eq!(sandbox["devNull"], Value::String("/dev/null".into()));
    assert!(sandbox["machine"].as_str().expect("machine string").len() > 0);
    assert!(
        sandbox["availableParallelism"]
            .as_i64()
            .expect("parallelism number")
            > 0
    );
}
