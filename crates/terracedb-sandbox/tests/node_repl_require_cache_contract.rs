use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn parse_stdout_json(stdout: &str) -> Value {
    let payload = stdout
        .lines()
        .rev()
        .find_map(|line| line.find('{').map(|index| &line[index..]))
        .unwrap_or(stdout.trim());
    serde_json::from_str(payload.trim()).unwrap_or_else(|error| {
        panic!("stdout json: {error}; raw stdout was: {stdout:?}");
    })
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

#[tokio::test]
async fn node_repl_does_not_clobber_require_cache_custom_keys() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on test-repl-require-cache.js.
    let source = r#"
      require.cache.something = 1;
      let replError = null;
      try {
        const repl = require("repl");
        repl.start({ useGlobal: false }).close();
      } catch (error) {
        replError = { code: error.code || null, message: error.message };
      }
      console.log(JSON.stringify({
        cacheValue: require.cache.something,
        replError,
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox repl require.cache contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node repl require.cache contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
