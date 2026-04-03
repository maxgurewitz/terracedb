use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn parse_stdout_json(stdout: &str) -> Value {
    let payload = stdout
        .lines()
        .rev()
        .find(|line| line.trim_start().starts_with('{'))
        .unwrap_or(stdout.trim());
    serde_json::from_str(payload.trim()).expect("stdout json")
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

#[tokio::test]
async fn node_module_helpers_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on:
    // - test-module-create-require.js
    // - test-module-create-require-multibyte.js
    // - test-module-isBuiltin.js
    // - test-module-nodemodulepaths.js
    let source = r#"
      const assert = require("assert");
      const fs = require("fs");
      const os = require("os");
      const path = require("path");
      const { pathToFileURL } = require("url");
      const Module = require("module");

      const base = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-module-"));
      const unicodeDir = path.join(base, "copy", "utf", "新建文件夹");
      fs.mkdirSync(unicodeDir, { recursive: true });
      fs.writeFileSync(path.join(base, "experimental.js"), "module.exports = { ofLife: 42 };");
      fs.writeFileSync(path.join(unicodeDir, "experimental.js"), "module.exports = { ofLife: 99 };");
      fs.writeFileSync(path.join(base, "fake.js"), "");
      fs.writeFileSync(path.join(unicodeDir, "index.js"), "");

      const reqFromUrl = Module.createRequire(pathToFileURL(path.join(base, "fake.js")));
      const reqFromUnicode = Module.createRequire(pathToFileURL(path.join(unicodeDir, "index.js")));

      const invalids = {};
      try {
        Module.createRequire("https://github.com/nodejs/node/pull/27405/");
      } catch (error) {
        invalids.https = { code: error.code, name: error.name, message: error.message };
      }
      try {
        Module.createRequire("../");
      } catch (error) {
        invalids.relative = { code: error.code, name: error.name, message: error.message };
      }
      try {
        Module.createRequire({});
      } catch (error) {
        invalids.object = { code: error.code, name: error.name, message: error.message };
      }

      console.log(JSON.stringify({
        createRequire: {
          urlValue: reqFromUrl("./experimental").ofLife,
          unicodeValue: reqFromUnicode("./experimental").ofLife,
        },
        isBuiltin: {
          http: Module.isBuiltin("http"),
          sys: Module.isBuiltin("sys"),
          nodeFs: Module.isBuiltin("node:fs"),
          nodeTest: Module.isBuiltin("node:test"),
          internalErrors: Module.isBuiltin("internal/errors"),
          testBare: Module.isBuiltin("test"),
          empty: Module.isBuiltin(""),
          undefinedValue: Module.isBuiltin(undefined),
        },
        nodeModulePaths: {
          nested: Module._nodeModulePaths("/usr/test/lib/node_modules/npm/foo"),
          nodeModulesRoot: Module._nodeModulePaths("/node_modules"),
          root: Module._nodeModulePaths("/"),
        },
        invalids,
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox module helper contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node module helper contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
