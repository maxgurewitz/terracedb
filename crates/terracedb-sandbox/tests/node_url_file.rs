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
async fn node_url_file_helpers_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r##"
      const url = require("url");
      const outputs = {
        pathToFileURL: [
          url.pathToFileURL("/tmp/demo.txt").href,
          url.pathToFileURL("/tmp/foo bar").href,
          url.pathToFileURL("/tmp/foo#bar").href,
        ],
        fileURLToPath: [
          url.fileURLToPath("file:///tmp/demo.txt"),
          url.fileURLToPath(new URL("file:///tmp/foo%20bar")),
          url.fileURLToPath(new URL("file:///tmp/foo%23bar"), { windows: false }),
        ],
      };
      for (const [label, fn] of [
        ["badArg", () => url.fileURLToPath(null)],
        ["badScheme", () => url.fileURLToPath("https://example.com/a")],
        ["badHost", () => url.fileURLToPath(new URL("file://host/a"))],
      ]) {
        try {
          fn();
          outputs[label] = null;
        } catch (error) {
          outputs[label] = { name: error.name, code: error.code ?? null };
        }
      }
      console.log(JSON.stringify(outputs));
    "##;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox url file helpers"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node url file helpers")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
