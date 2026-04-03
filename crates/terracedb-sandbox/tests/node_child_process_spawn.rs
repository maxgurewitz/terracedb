#[path = "support/node_compat.rs"]
mod node_compat;

use serde_json::Value;

#[tokio::test]
async fn node_child_process_spawn_honors_env_and_cwd() {
    let result = node_compat::exec_node_fixture(
        r#"
        (async () => {
          const { spawn } = require("node:child_process");
          const env = { ...process.env, HELLO: "WORLD", EMPTY: "", NULL: null };
          Object.setPrototypeOf(env, { FOO: "BAR" });
          const child = spawn("/usr/bin/env", [], { cwd: "/workspace/app", env });
          child.stdout.setEncoding("utf8");
          let stdout = "";
          child.stdout.on("data", (chunk) => stdout += chunk);
          const close = await new Promise((resolve, reject) => {
            child.on("error", reject);
            child.on("close", (code, signal) => resolve({ code, signal, pidType: typeof child.pid }));
          });
          const pwd = spawn("pwd", [], { cwd: "/workspace/app" });
          pwd.stdout.setEncoding("utf8");
          let pwdOut = "";
          pwd.stdout.on("data", (chunk) => pwdOut += chunk);
          const pwdClose = await new Promise((resolve, reject) => {
            pwd.on("error", reject);
            pwd.on("close", (code, signal) => resolve({ code, signal }));
          });
          console.log(JSON.stringify({ stdout, close, pwdOut, pwdClose }));
        })();
        "#,
    )
    .await
    .expect("run sandbox node");

    let report = result.result.expect("node report");
    let payload: Value =
        serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
            .expect("parse payload");
    let stdout = payload["stdout"].as_str().unwrap_or_default();
    assert!(stdout.contains("HELLO=WORLD"));
    assert!(stdout.contains("FOO=BAR"));
    assert!(stdout.contains("NULL=null"));
    assert!(stdout.contains("EMPTY=\n") || stdout.contains("EMPTY=\r\n"));
    assert_eq!(payload["close"]["code"].as_i64(), Some(0));
    assert_eq!(payload["close"]["pidType"].as_str(), Some("number"));
    assert_eq!(payload["pwdOut"].as_str(), Some("/workspace/app\n"));
    assert_eq!(payload["pwdClose"]["code"].as_i64(), Some(0));
}

#[tokio::test]
async fn node_child_process_spawn_missing_cwd_reports_enoent() {
    let result = node_compat::exec_node_fixture(
        r#"
        (async () => {
          const { spawn } = require("node:child_process");
          const child = spawn("pwd", [], { cwd: "does-not-exist" });
          const outcome = await new Promise((resolve) => {
            child.on("error", (error) => resolve({
              pidType: typeof child.pid,
              code: error.code,
              message: error.message,
            }));
          });
          console.log(JSON.stringify(outcome));
        })();
        "#,
    )
    .await
    .expect("run sandbox node");

    let report = result.result.expect("node report");
    let payload: Value =
        serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
            .expect("parse payload");
    assert_eq!(payload["pidType"].as_str(), Some("undefined"));
    assert_eq!(payload["code"].as_str(), Some("ENOENT"));
    assert!(
        payload["message"]
            .as_str()
            .unwrap_or_default()
            .contains("ENOENT")
    );
}
