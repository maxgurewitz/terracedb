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
async fn node_require_and_import_cache_interop_matches_real_node_for_cjs_entry() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on test-esm-in-require-cache.js.
    let source = r#"
      const fs = require("fs");
      const os = require("os");
      const path = require("path");

      const base = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-esm-cache-"));
      const esmPath = path.join(base, "esm.mjs");

      fs.writeFileSync(esmPath, "export const name = 'esm';\n");
      fs.writeFileSync(path.join(base, "import-esm.mjs"), "export { name } from './esm.mjs';\n");
      fs.writeFileSync(path.join(base, "require-import-esm.cjs"), "exports.name = require('./import-esm.mjs').name;\n");
      fs.writeFileSync(
        path.join(base, "require-esm.cjs"),
        "const path = require('path');\n" +
        "exports.name = require('./esm.mjs').name;\n" +
        "exports.cache = require.cache[path.join(__dirname, 'esm.mjs')];\n",
      );
      fs.writeFileSync(
        path.join(base, "import-require-esm.mjs"),
        "export { name, cache } from './require-esm.cjs';\n",
      );

      const importPath = path.join(base, "import-esm.mjs");
      const requireImportPath = path.join(base, "require-import-esm.cjs");
      const importRequirePath = path.join(base, "import-require-esm.mjs");

      let value = require(importPath);
      const afterIndirectImport = Object.prototype.hasOwnProperty.call(require.cache, esmPath);

      value = require(requireImportPath);
      const afterIndirectRequire = Object.prototype.hasOwnProperty.call(require.cache, esmPath);

      value = require(esmPath);
      const afterDirectRequire = Object.prototype.hasOwnProperty.call(require.cache, esmPath);
      delete require.cache[esmPath];

      value = require(importRequirePath);
      const afterImportRequire = Object.prototype.hasOwnProperty.call(require.cache, esmPath);

      console.log(JSON.stringify({
        afterIndirectImport,
        afterIndirectRequire,
        afterDirectRequire,
        afterImportRequire,
        directName: require(esmPath).name,
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox esm/cache cjs contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node esm/cache cjs contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}

#[tokio::test]
async fn node_require_and_import_cache_interop_matches_real_node_for_esm_entry() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on test-esm-in-require-cache-2.mjs.
    let source = r#"
      import fs from 'node:fs';
      import os from 'node:os';
      import path from 'node:path';
      import { Module } from 'node:module';

      const base = fs.mkdtempSync(path.join(os.tmpdir(), 'terrace-esm-entry-cache-'));
      const esmPath = path.join(base, 'esm.mjs');

      fs.writeFileSync(esmPath, "export const name = 'esm';\n");
      fs.writeFileSync(path.join(base, 'import-esm.mjs'), "export { name } from './esm.mjs';\n");
      fs.writeFileSync(path.join(base, 'require-import-esm.cjs'), "exports.name = require('./import-esm.mjs').name;\n");
      fs.writeFileSync(
        path.join(base, 'require-esm.cjs'),
        "const path = require('path');\n" +
        "exports.name = require('./esm.mjs').name;\n" +
        "exports.cache = require.cache[path.join(__dirname, 'esm.mjs')];\n",
      );
      fs.writeFileSync(
        path.join(base, 'import-require-esm.mjs'),
        "export { name, cache } from './require-esm.cjs';\n",
      );

      let value = await import(path.join(base, 'import-esm.mjs'));
      const afterIndirectImport = Object.prototype.hasOwnProperty.call(Module._cache, esmPath);

      value = await import(esmPath);
      const afterDirectImport = Object.prototype.hasOwnProperty.call(Module._cache, esmPath);

      value = await import(path.join(base, 'require-import-esm.cjs'));
      const afterImportingCjsThatRequiresImport = Object.prototype.hasOwnProperty.call(Module._cache, esmPath);

      value = await import(path.join(base, 'import-require-esm.mjs'));
      const afterImportingModuleThatRequiresEsm = Object.prototype.hasOwnProperty.call(Module._cache, esmPath);

      console.log(JSON.stringify({
        afterIndirectImport,
        afterDirectImport,
        afterImportingCjsThatRequiresImport,
        afterImportingModuleThatRequiresEsm,
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture_at("/workspace/app/index.mjs", source)
            .await
            .expect("sandbox esm/cache esm contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_file_with_extension("esm-cache-entry", "mjs", source)
            .expect("real node esm/cache esm contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
