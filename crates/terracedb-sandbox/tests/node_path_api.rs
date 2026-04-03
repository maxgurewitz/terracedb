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
async fn node_path_exports_and_core_ops_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const path = require("path");
      console.log(JSON.stringify({
        keys: Object.keys(path).sort(),
        resolve: [
          path.resolve("/foo/bar", "./baz"),
          path.resolve("/foo/bar", "/tmp/file/"),
          path.resolve("/workspace/app", "wwwroot", "static_files/png/", "../gif/image.gif"),
        ],
        join: [
          path.join("/foo", "bar", "baz/asdf", "quux", ".."),
          path.join("foo", "", "bar"),
          path.join("/foo", "../../../bar"),
        ],
        normalize: [
          path.normalize("/foo/bar//baz/asdf/quux/.."),
          path.normalize(""),
          path.normalize("/foo/../../bar"),
        ],
        relative: [
          path.relative("/data/orandea/test/aaa", "/data/orandea/impl/bbb"),
          path.relative("/a/b/c", "/a/b/c"),
          path.relative("/a/b/c", "/a/b"),
        ],
        absolute: [path.isAbsolute("/foo/bar"), path.isAbsolute("qux/")],
        matchesGlob: [
          path.matchesGlob("/foo/bar/baz.txt", "/foo/**/*.txt"),
          path.matchesGlob("/foo/bar/baz.txt", "/foo/**/*.js"),
        ],
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox path api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node path api")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
