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
async fn node_fs_exports_match_real_node() {
    tracing_support::init_tracing();
    if !node_compat_support::real_node_available() {
        eprintln!("skipping fs export parity test because node is unavailable");
        return;
    }

    let source = r#"console.log(JSON.stringify(Object.keys(require("node:fs")).sort()));"#;
    let expected = node_compat_support::exec_real_node_eval(source).expect("run real node");
    let actual = node_compat_support::exec_node_fixture(source)
        .await
        .expect("run sandbox node command");

    assert_eq!(expected.exit_code, 0, "{expected:#?}");
    assert_eq!(stdout(actual), expected.stdout.trim());
}

#[tokio::test]
async fn node_fs_access_append_mkdtemp_and_readdir_dirents_work() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const assert = require("node:assert");
        const fs = require("node:fs");
        const path = require("node:path");

        const base = "/workspace/app/tmp";
        fs.mkdirSync(base, { recursive: true });
        const file = path.join(base, "append.txt");
        fs.appendFileSync(file, "AB");
        fs.appendFileSync(file, Buffer.from("CD"));
        fs.accessSync(file, fs.constants.F_OK);
        fs.writeFileSync(path.join(base, "alpha.txt"), "");
        fs.mkdirSync(path.join(base, "nested"), { recursive: true });

        const dirents = fs.readdirSync(base, { withFileTypes: true })
          .map((entry) => ({
            name: entry.name,
            file: entry.isFile(),
            dir: entry.isDirectory(),
            link: entry.isSymbolicLink(),
          }))
          .sort((a, b) => a.name.localeCompare(b.name));

        const stat = fs.statSync(file);
        const temp = fs.mkdtempSync(path.join(base, "foo."));

        assert.strictEqual(fs.readFileSync(file, "utf8"), "ABCD");
        assert.strictEqual(stat.isFile(), true);
        assert.strictEqual(fs.existsSync(temp), true);

        console.log(JSON.stringify({
          basenameLength: path.basename(temp).length,
          dirents,
          fileContents: fs.readFileSync(file, "utf8"),
        }));
        "#,
    )
    .await
    .expect("fs basic operations should succeed");

    let parsed: serde_json::Value = serde_json::from_str(&stdout(result)).expect("stdout json");
    assert_eq!(parsed["fileContents"], "ABCD");
    assert_eq!(parsed["basenameLength"], 10);
    let dirents = parsed["dirents"].as_array().expect("dirents");
    assert!(
        dirents.iter().any(|entry| {
            entry["name"] == "append.txt" && entry["file"] == true && entry["dir"] == false
        }),
        "{dirents:#?}"
    );
    assert!(
        dirents.iter().any(|entry| {
            entry["name"] == "nested" && entry["file"] == false && entry["dir"] == true
        }),
        "{dirents:#?}"
    );
}

#[tokio::test]
async fn node_fs_open_supports_exclusive_create_flags() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const assert = require("node:assert");
        const fs = require("node:fs");
        const path = require("node:path");

        const base = "/workspace/app/tmp-open";
        fs.mkdirSync(base, { recursive: true });

        const wx = path.join(base, "wx.txt");
        const fd = fs.openSync(wx, "wx");
        fs.writeFileSync(fd, "hello");
        fs.closeSync(fd);

        let wxCode = null;
        try {
          fs.openSync(wx, "wx");
        } catch (error) {
          wxCode = error.code;
        }

        const ax = path.join(base, "ax.txt");
        const axFd = fs.openSync(ax, "ax");
        fs.closeSync(axFd);

        let axCode = null;
        try {
          fs.openSync(ax, "ax");
        } catch (error) {
          axCode = error.code;
        }

        console.log(JSON.stringify({
          wxCode,
          axCode,
          wxContents: fs.readFileSync(wx, "utf8"),
        }));
        "#,
    )
    .await
    .expect("fs open exclusive flags should succeed");

    let parsed: serde_json::Value = serde_json::from_str(&stdout(result)).expect("stdout json");
    assert_eq!(parsed["wxContents"], "hello");
    assert_eq!(parsed["wxCode"], "EEXIST");
    assert_eq!(parsed["axCode"], "EEXIST");
}
