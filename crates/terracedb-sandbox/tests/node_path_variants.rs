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
async fn node_path_posix_and_win32_variants_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const path = require("path");
      console.log(JSON.stringify({
        defaultIsPosix: path.sep === "/",
        posix: {
          join: path.posix.join("/foo", "bar", "baz/asdf", "quux", ".."),
          normalize: path.posix.normalize("/foo/bar//baz/asdf/quux/.."),
          relative: path.posix.relative("/data/orandea/test/aaa", "/data/orandea/impl/bbb"),
          parse: path.posix.parse("/home/user/dir/file.txt"),
          toNamespacedPath: path.posix.toNamespacedPath("/foo/bar"),
        },
        win32: {
          join: path.win32.join("C:\\\\foo", "bar", "baz\\\\asdf", "quux", ".."),
          normalize: path.win32.normalize("C:\\\\temp\\\\foo\\\\bar\\\\..\\\\"),
          relative: path.win32.relative("C:\\\\orandea\\\\test\\\\aaa", "C:\\\\orandea\\\\impl\\\\bbb"),
          parse: path.win32.parse("C:\\\\path\\\\dir\\\\file.txt"),
          toNamespacedPath: path.win32.toNamespacedPath("C:\\\\foo\\\\bar"),
          isAbsolute: [path.win32.isAbsolute("C:\\\\foo\\\\.."), path.win32.isAbsolute("bar\\\\baz")],
        },
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox path variants"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node path variants")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
