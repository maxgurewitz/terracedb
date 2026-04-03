use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn parse_stdout_json(stdout: &str) -> Value {
    serde_json::from_str(stdout.trim()).expect("stdout json")
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

#[tokio::test]
async fn node_path_parse_format_and_suffix_ops_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const path = require("path");
      console.log(JSON.stringify({
        parse: [
          path.parse("/home/user/dir/file.txt"),
          path.parse("/home/user/dir/.file"),
          path.parse("file"),
        ],
        format: [
          path.format({ root: "/", dir: "/home/user/dir", base: "file.txt", ext: ".txt", name: "file" }),
          path.format({ dir: "/tmp", name: "hello", ext: "txt" }),
          path.format({ root: "/", base: "hello" }),
        ],
        basename: [
          path.basename("/foo/bar/baz/asdf/quux.html"),
          path.basename("/foo/bar/baz/asdf/quux.html", ".html"),
          path.basename("/foo/bar/baz/asdf/"),
        ],
        dirname: [
          path.dirname("/foo/bar/baz/asdf/quux"),
          path.dirname("/foo"),
          path.dirname("foo"),
        ],
        extname: [
          path.extname("index.html"),
          path.extname("index."),
          path.extname(".index"),
          path.extname("index"),
        ],
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox path parse/format"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node path parse/format")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
