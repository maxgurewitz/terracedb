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

fn normalize_missing_message(payload: &mut Value) {
    if let Some(message) = payload.get("message").and_then(Value::as_str) {
        let mut normalized = message.to_string();
        if let Some(start) = normalized.find("Cannot find module '") {
            let path_start = start + "Cannot find module '".len();
            if let Some(path_end) = normalized[path_start..].find('\'') {
                let path =
                    normalized[path_start..path_start + path_end].to_string();
                let basename = std::path::Path::new(&path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(path.as_str());
                normalized.replace_range(path_start..path_start + path_end, basename);
            }
        }
        if let Some(index) = normalized.find("\nRequire stack:\n- ") {
            normalized = format!("{}\nRequire stack:\n- <entrypoint>", &normalized[..index]);
        }
        payload["message"] = Value::String(normalized);
    }
}

#[tokio::test]
async fn node_module_multi_extensions_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Broadly based on test-module-multi-extensions.js.
    let source = r#"
      const fs = require("fs");
      const os = require("os");
      const path = require("path");
      const Module = require("module");

      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-extensions-"));
      const file = path.join(tmp, "test-extensions.foo.bar");
      const dotfile = path.join(tmp, ".bar");
      const dotfileWithExtension = path.join(tmp, ".foo.bar");

      fs.writeFileSync(file, "module.exports = require('path').basename(__filename);", "utf8");
      fs.writeFileSync(dotfile, "module.exports = require('path').basename(__filename);", "utf8");
      fs.writeFileSync(dotfileWithExtension, "module.exports = require('path').basename(__filename);", "utf8");
      const fileReal = fs.realpathSync(file);
      const dotfileReal = fs.realpathSync(dotfile);
      const dotfileWithExtensionReal = fs.realpathSync(dotfileWithExtension);

      const calls = [];
      const outputs = {};
      const reset = () => {
        delete require.extensions[".bar"];
        delete require.extensions[".foo.bar"];
        Module._pathCache = { __proto__: null };
        delete require.cache[fileReal];
        delete require.cache[dotfileReal];
        delete require.cache[dotfileWithExtensionReal];
      };

      require.extensions[".bar"] = (mod, filename) => {
        calls.push({ ext: ".bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".bar" };
      };
      require.extensions[".foo.bar"] = (mod, filename) => {
        calls.push({ ext: ".foo.bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".foo.bar" };
      };
      outputs.firstModulePath = require(path.join(tmp, "test-extensions"));
      outputs.firstExplicit = require(file);
      reset();

      require.extensions[".foo.bar"] = (mod, filename) => {
        calls.push({ ext: ".foo.bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".foo.bar" };
      };
      outputs.secondModulePath = require(path.join(tmp, "test-extensions"));
      try {
        require(path.join(tmp, "test-extensions.foo"));
      } catch (error) {
        outputs.secondMissing = { code: error.code, message: error.message };
      }
      outputs.secondExplicit = require(path.join(tmp, "test-extensions.foo.bar"));
      reset();

      try {
        require(path.join(tmp, "test-extensions"));
      } catch (error) {
        outputs.thirdMissing = { code: error.code, message: error.message };
      }
      reset();

      require.extensions[".bar"] = (mod, filename) => {
        calls.push({ ext: ".bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".bar" };
      };
      require.extensions[".foo.bar"] = (mod, filename) => {
        calls.push({ ext: ".foo.bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".foo.bar" };
      };
      outputs.fourthModulePath = require(path.join(tmp, "test-extensions.foo"));
      reset();

      require.extensions[".foo.bar"] = (mod, filename) => {
        calls.push({ ext: ".foo.bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".foo.bar" };
      };
      try {
        require(path.join(tmp, "test-extensions.foo"));
      } catch (error) {
        outputs.fifthMissing = { code: error.code, message: error.message };
      }
      reset();

      require.extensions[".bar"] = (mod, filename) => {
        calls.push({ ext: ".bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".bar" };
      };
      outputs.dotfile = require(dotfile);
      reset();

      require.extensions[".bar"] = (mod, filename) => {
        calls.push({ ext: ".bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".bar" };
      };
      require.extensions[".foo.bar"] = (mod, filename) => {
        calls.push({ ext: ".foo.bar", file: path.basename(filename) });
        mod.exports = { basename: path.basename(filename), ext: ".foo.bar" };
      };
      outputs.dotfileWithExtension = require(dotfileWithExtension);
      reset();

      console.log(JSON.stringify({ calls, outputs }));
    "#;

    let mut sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox module extension contract"),
    );
    let mut real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node module extension contract")
            .stdout,
    );

    for key in ["secondMissing", "thirdMissing", "fifthMissing"] {
        if let Some(payload) = sandbox["outputs"].get_mut(key) {
            normalize_missing_message(payload);
        }
        if let Some(payload) = real["outputs"].get_mut(key) {
            normalize_missing_message(payload);
        }
    }

    assert_eq!(sandbox, real);
}
