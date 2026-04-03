#[path = "support/node_compat.rs"]
mod node_compat_support;

fn stdout(result: terracedb_sandbox::SandboxExecutionResult) -> String {
    result.result.expect("node command report")["stdout"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_string()
}

#[tokio::test]
async fn node_fs_mkdtemp_contract_matches_upstream_subset() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const fs = require("node:fs");
        const path = require("node:path");
        const { pathToFileURL } = require("node:url");

        const base = "/workspace/app/tmp-mkdtemp-contract";
        fs.mkdirSync(base, { recursive: true });

        const stringPath = fs.mkdtempSync(path.join(base, "foo."));
        const bufferPath = fs.mkdtempSync(Buffer.from(path.join(base, "bar.")));
        const uint8Path = fs.mkdtempSync(new TextEncoder().encode(path.join(base, "baz.")));
        const urlPath = fs.mkdtempSync(pathToFileURL(path.join(base, "qux.")));

        let callbackPath = null;
        fs.mkdtemp(path.join(base, "cb."), (error, folder) => {
          if (error) throw error;
          callbackPath = folder;
          console.log(JSON.stringify({
            stringLength: path.basename(stringPath).length,
            bufferExists: fs.existsSync(bufferPath),
            uint8Exists: fs.existsSync(uint8Path),
            urlExists: fs.existsSync(urlPath),
            callbackExists: fs.existsSync(folder),
            callbackBasenameLength: path.basename(folder).length,
          }));
        });
        "#,
    )
    .await
    .expect("fs mkdtemp contract should execute");

    let parsed: serde_json::Value = serde_json::from_str(&stdout(result)).expect("stdout json");
    assert_eq!(parsed["stringLength"], "foo.XXXXXX".len());
    assert_eq!(parsed["bufferExists"], true);
    assert_eq!(parsed["uint8Exists"], true);
    assert_eq!(parsed["urlExists"], true);
    assert_eq!(parsed["callbackExists"], true);
    assert_eq!(parsed["callbackBasenameLength"], "cb.XXXXXX".len());
}
