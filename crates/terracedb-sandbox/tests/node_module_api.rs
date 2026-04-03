#[path = "support/node_compat.rs"]
mod node_compat;
#[path = "support/tracing.rs"]
mod tracing_support;

use std::collections::BTreeMap;

use terracedb_vfs::CreateOptions;

fn overwrite_options() -> CreateOptions {
    CreateOptions {
        create_parents: true,
        overwrite: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn node_module_builtin_exposes_core_cjs_helpers() {
    tracing_support::init_tracing();

    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        542,
        153,
        entrypoint,
        r#"
        const Module = require("module");
        const parent = new Module("/workspace/app/index.cjs", module);
        const customRequire = Module.createRequire("/workspace/app/tools/create-require.cjs");
        console.log(JSON.stringify({
          moduleType: typeof Module,
          sameCtor: Module === Module.Module,
          builtinFs: Module.isBuiltin("fs"),
          builtinNodeFs: Module.isBuiltin("node:fs"),
          helperStatus: Module.enableCompileCache().status,
          resolved: Module._resolveFilename("./dep.js", parent),
          requiredValue: customRequire("./dep.js").value,
        }));
        "#,
    )
    .await;

    let options = overwrite_options();
    session
        .filesystem()
        .write_file(
            "/workspace/app/dep.js",
            br#"module.exports = { value: "from-root-module" };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write root dep");
    session
        .filesystem()
        .write_file(
            "/workspace/app/tools/create-require.cjs",
            br#"module.exports = { helper: true };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write create-require helper");
    session
        .filesystem()
        .write_file(
            "/workspace/app/tools/dep.js",
            br#"module.exports = { value: "from-create-require" };"#.to_vec(),
            options,
        )
        .await
        .expect("write dep");

    let result = session
        .exec_node_command(
            entrypoint.to_string(),
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
        .expect("node command should expose module helpers");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .expect("structured payload");
    let payload: serde_json::Value = serde_json::from_str(payload).expect("decode payload");

    assert_eq!(payload["moduleType"].as_str(), Some("function"));
    assert_eq!(payload["sameCtor"].as_bool(), Some(true));
    assert_eq!(payload["builtinFs"].as_bool(), Some(true));
    assert_eq!(payload["builtinNodeFs"].as_bool(), Some(true));
    assert_eq!(payload["helperStatus"].as_str(), Some("disabled"));
    assert_eq!(payload["resolved"].as_str(), Some("/workspace/app/dep.js"));
    assert_eq!(
        payload["requiredValue"].as_str(),
        Some("from-create-require")
    );
}

#[tokio::test]
async fn node_module_node_module_paths_match_node_reference_case() {
    tracing_support::init_tracing();

    let result = node_compat::exec_node_fixture(
        r#"
        const Module = require("module");
        console.log(JSON.stringify({
          paths: Module._nodeModulePaths("/usr/test/lib/node_modules/npm/foo"),
        }));
        "#,
    )
    .await
    .expect("node command should expose Module._nodeModulePaths");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .expect("structured payload");
    let payload: serde_json::Value = serde_json::from_str(payload).expect("decode payload");

    let expected = vec![
        "/usr/test/lib/node_modules/npm/foo/node_modules",
        "/usr/test/lib/node_modules/npm/node_modules",
        "/usr/test/lib/node_modules",
        "/usr/test/node_modules",
        "/usr/node_modules",
        "/node_modules",
    ];
    let actual = payload["paths"]
        .as_array()
        .expect("paths array")
        .iter()
        .map(|value| value.as_str().expect("path").to_string())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
