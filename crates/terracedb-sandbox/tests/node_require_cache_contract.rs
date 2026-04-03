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
async fn node_require_cache_contract_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on test-require-cache.js and test-require-node-prefix.js.
    let source = r#"
      const fs = require("fs");
      const os = require("os");
      const path = require("path");
      const base = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-require-cache-contract-"));
      const relativePath = path.join(base, "demo.js");
      const absolutePath = path.join(base, "demo.js");

      globalThis.__terraceDemoLoads = 0;
      fs.writeFileSync(
        absolutePath,
        `
          globalThis.__terraceDemoLoads = (globalThis.__terraceDemoLoads || 0) + 1;
          module.exports = { loads: globalThis.__terraceDemoLoads };
        `,
      );
      const cachePath = fs.realpathSync(absolutePath);

      const first = require(relativePath);
      const second = require(relativePath);
      delete require.cache[cachePath];
      const third = require(relativePath);

      const fakeModule = { fake: true };
      require.cache[cachePath] = { exports: fakeModule };
      const fourth = require(relativePath);

      const fakeFs = { sentinel: "fake-fs" };
      require.cache.fs = { exports: fakeFs };
      const builtin = require("fs");
      const nodeBuiltin = require("node:fs");
      delete require.cache.fs;

      console.log(JSON.stringify({
        firstLoads: first.loads,
        secondLoads: second.loads,
        thirdLoads: third.loads,
        fourthIsFake: fourth === fakeModule,
        builtinIsFake: builtin === fakeFs,
        nodeBuiltinIsRealFs: nodeBuiltin === fs,
        cacheHasAbsolute: Object.prototype.hasOwnProperty.call(require.cache, cachePath),
        cacheKeysContainAbsolute: Object.keys(require.cache).includes(cachePath),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox require.cache contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node require.cache contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
