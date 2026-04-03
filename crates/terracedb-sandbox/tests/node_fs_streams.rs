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
async fn node_fs_write_stream_end_writes_and_closes() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const assert = require("node:assert");
        const fs = require("node:fs");
        const path = require("node:path");

        const file = "/workspace/app/tmp/write-end.txt";
        fs.mkdirSync(path.dirname(file), { recursive: true });

        const stream = fs.createWriteStream(file);
        let sawOpen = false;
        stream.on("open", () => { sawOpen = true; });
        stream.on("finish", () => { console.log("finish:" + sawOpen); });
        stream.on("close", () => {
          console.log("close:" + fs.readFileSync(file, "utf8"));
        });
        stream.end("a\n", "utf8");
        "#,
    )
    .await
    .expect("write stream should finish");

    let out = stdout(result);
    assert!(out.contains("finish:true"), "{out}");
    assert!(out.contains("close:a"), "{out}");
}
