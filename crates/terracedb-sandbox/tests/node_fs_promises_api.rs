#[path = "support/node_compat.rs"]
mod node_compat_support;
#[path = "support/tracing.rs"]
mod tracing_support;

fn stdout(result: terracedb_sandbox::SandboxExecutionResult) -> String {
    result.result.expect("node command report")["stdout"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_string()
}

#[tokio::test]
async fn node_fs_promises_exports_match_real_node() {
    tracing_support::init_tracing();
    if !node_compat_support::real_node_available() {
        eprintln!("skipping fs.promises export parity test because node is unavailable");
        return;
    }

    let source = r#"console.log(JSON.stringify(Object.keys(require("node:fs").promises).sort()));"#;
    let expected = node_compat_support::exec_real_node_eval(source).expect("run real node");
    let actual = node_compat_support::exec_node_fixture(source)
        .await
        .expect("run sandbox node command");

    assert_eq!(expected.exit_code, 0, "{expected:#?}");
    assert_eq!(stdout(actual), expected.stdout.trim());
}

#[tokio::test]
async fn node_fs_promises_filehandle_write_append_and_close_semantics_work() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const assert = require("node:assert");
        const fs = require("node:fs");
        const path = require("node:path");

        (async () => {
          const base = "/workspace/app/tmp";
          fs.mkdirSync(base, { recursive: true });
          const file = path.join(base, "handle.txt");

          const handle = await fs.promises.open(file, "w+");
          await handle.writeFile(Buffer.from("Hello"));
          await fs.promises.appendFile(handle, " World");
          const beforeClose = fs.readFileSync(file, "utf8");
          await handle.close();

          let afterCloseCode = null;
          try {
            await handle.stat();
          } catch (error) {
            afterCloseCode = error.code;
          }

          const appendHandle = await fs.promises.open(file, "a+");
          await appendHandle.appendFile("!");
          await appendHandle.close();

          const dirents = await fs.promises.readdir(base, { withFileTypes: true });
          assert.ok(dirents.some((entry) => entry.name === "handle.txt" && entry.isFile()));

          console.log(JSON.stringify({
            afterCloseCode,
            afterCloseFd: handle.fd,
            finalContents: fs.readFileSync(file, "utf8"),
          }));
        })().catch((error) => {
          console.error(error && error.stack || String(error));
          process.exitCode = 1;
        });
        "#,
    )
    .await
    .expect("fs.promises filehandle operations should succeed");

    let parsed: serde_json::Value = serde_json::from_str(&stdout(result)).expect("stdout json");
    assert_eq!(parsed["finalContents"], "Hello World!");
    assert_eq!(parsed["afterCloseCode"], "EBADF");
    assert_eq!(parsed["afterCloseFd"], -1);
}
