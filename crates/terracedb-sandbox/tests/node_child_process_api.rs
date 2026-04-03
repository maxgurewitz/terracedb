#[path = "support/node_compat.rs"]
mod node_compat;

use serde_json::Value;

#[tokio::test]
async fn node_child_process_exports_match_real_node() {
    if !node_compat::real_node_available() {
        eprintln!("skipping child_process export parity because real node is unavailable");
        return;
    }

    let real = node_compat::exec_real_node_eval(
        r#"
        const cp = require("node:child_process");
        console.log(JSON.stringify({
          moduleKeys: Object.keys(cp).sort(),
          prototypeKeys: Object.getOwnPropertyNames(cp.ChildProcess.prototype).sort(),
        }));
        "#,
    )
    .expect("run real node");
    assert_eq!(
        real.exit_code, 0,
        "real node should exit cleanly: {real:#?}"
    );

    let result = node_compat::exec_node_fixture(
        r#"
        const cp = require("node:child_process");
        console.log(JSON.stringify({
          moduleKeys: Object.keys(cp).sort(),
          prototypeKeys: Object.getOwnPropertyNames(cp.ChildProcess.prototype).sort(),
        }));
        "#,
    )
    .await
    .expect("run sandbox node");

    let report = result.result.expect("node report");
    let sandbox_keys: Value =
        serde_json::from_str(report["stdout"].as_str().expect("stdout string"))
            .expect("parse sandbox keys");
    let real_keys: Value = serde_json::from_str(real.stdout.trim()).expect("parse real keys");
    assert_eq!(
        sandbox_keys, real_keys,
        "child_process export surface drifted"
    );
}

#[tokio::test]
async fn node_child_process_constructor_spawn_and_kill_basics_work() {
    let result = node_compat::exec_node_fixture(
        r#"
        (async () => {
          const { ChildProcess } = require("node:child_process");
          const child = new ChildProcess();
          child.spawn({
            file: process.execPath,
            args: ["-e", "console.log('child-ok')"],
            cwd: process.cwd(),
            stdio: "pipe",
          });
          child.stdout.setEncoding("utf8");
          let stdout = "";
          child.stdout.on("data", (chunk) => stdout += chunk);
          let invalidSignalCode = null;
          try {
            child.kill("foo");
          } catch (error) {
            invalidSignalCode = error.code;
          }
          const close = await new Promise((resolve, reject) => {
            child.on("error", reject);
            child.on("close", (code, signal) => resolve({ code, signal }));
          });
          console.log(JSON.stringify({
            pidType: typeof child.pid,
            stdout,
            close,
            invalidSignalCode,
            spawnfile: child.spawnfile,
            spawnargsLength: child.spawnargs.length,
          }));
        })();
        "#,
    )
    .await
    .expect("run sandbox node");

    let report = result.result.expect("node report");
    let payload: Value =
        serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
            .expect("parse payload");
    assert_eq!(payload["pidType"].as_str(), Some("number"));
    assert_eq!(payload["stdout"].as_str(), Some("child-ok\n"));
    assert_eq!(payload["close"]["code"].as_i64(), Some(0));
    assert_eq!(
        payload["invalidSignalCode"].as_str(),
        Some("ERR_UNKNOWN_SIGNAL")
    );
    assert_eq!(payload["spawnfile"].as_str(), Some("/usr/bin/node"));
    assert!(payload["spawnargsLength"].as_u64().unwrap_or_default() >= 2);
}
