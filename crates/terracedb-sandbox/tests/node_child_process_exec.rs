#[path = "support/node_compat.rs"]
mod node_compat;

use serde_json::Value;

#[tokio::test]
async fn node_child_process_exec_execfile_and_sync_variants_work() {
    let result = node_compat::exec_node_fixture(
        r#"
        (async () => {
          const fs = require("node:fs");
          const path = "/workspace/app/forked-child.cjs";
          fs.writeFileSync(path, "console.log(process.argv.slice(2).join(','));\n");
          const cp = require("node:child_process");
          const execFileResult = await new Promise((resolve) => {
            cp.execFile(process.execPath, ["-e", "console.log('execfile-ok')"], (error, stdout, stderr) => {
              resolve({ error: error ? { code: error.code, message: error.message } : null, stdout, stderr });
            });
          });
          const execResult = await new Promise((resolve) => {
            cp.exec("echo shell-ok", (error, stdout, stderr) => {
              resolve({ error: error ? { code: error.code, message: error.message } : null, stdout, stderr });
            });
          });
          const missingResult = await new Promise((resolve) => {
            const child = cp.execFile("does-not-exist", (error, stdout, stderr) => {
              resolve({
                pidType: typeof child.pid,
                error: error ? { code: error.code, message: error.message, cmd: error.cmd } : null,
                stdout,
                stderr,
              });
            });
          });
          const spawnSyncResult = cp.spawnSync("echo bar | cat", { shell: true, encoding: "utf8" });
          const execSyncResult = cp.execSync("echo sync-ok", { encoding: "utf8" });
          const execFileSyncResult = cp.execFileSync(process.execPath, ["-e", "console.log('sync-file-ok')"], { encoding: "utf8" });
          const forkChild = cp.fork(path, ["fork", "ok"]);
          forkChild.stdout.setEncoding("utf8");
          let forkStdout = "";
          forkChild.stdout.on("data", (chunk) => forkStdout += chunk);
          const forkClose = await new Promise((resolve, reject) => {
            forkChild.on("error", reject);
            forkChild.on("close", (code, signal) => resolve({ code, signal }));
          });
          console.log(JSON.stringify({
            execFileResult,
            execResult,
            missingResult,
            spawnSyncResult: {
              status: spawnSyncResult.status,
              stdout: spawnSyncResult.stdout,
              stderr: spawnSyncResult.stderr,
              error: spawnSyncResult.error ? { code: spawnSyncResult.error.code, message: spawnSyncResult.error.message } : null,
            },
            execSyncResult,
            execFileSyncResult,
            forkStdout,
            forkClose,
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
    assert_eq!(payload["execFileResult"]["error"], Value::Null);
    assert_eq!(
        payload["execFileResult"]["stdout"].as_str(),
        Some("execfile-ok\n")
    );
    assert_eq!(payload["execResult"]["error"], Value::Null);
    assert_eq!(payload["execResult"]["stdout"].as_str(), Some("shell-ok\n"));
    assert_eq!(
        payload["missingResult"]["pidType"].as_str(),
        Some("undefined")
    );
    assert_eq!(
        payload["missingResult"]["error"]["code"].as_str(),
        Some("ENOENT")
    );
    assert_eq!(payload["spawnSyncResult"]["status"].as_i64(), Some(0));
    assert_eq!(payload["spawnSyncResult"]["stdout"].as_str(), Some("bar\n"));
    assert_eq!(payload["execSyncResult"].as_str(), Some("sync-ok\n"));
    assert_eq!(
        payload["execFileSyncResult"].as_str(),
        Some("sync-file-ok\n")
    );
    assert_eq!(payload["forkStdout"].as_str(), Some("fork,ok\n"));
    assert_eq!(payload["forkClose"]["code"].as_i64(), Some(0));
}
