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
async fn node_package_imports_and_conditional_exports_follow_node_reference_paths() {
    tracing_support::init_tracing();

    let entrypoint = "/workspace/app/index.mjs";
    let session = node_compat::open_node_session(
        541,
        152,
        entrypoint,
        r##"
        import cjsResults from "./cjs-check.cjs";
        import branchImported from "#branch";
        import externalImported from "#external";
        import wildcardImported from "#external/subpath/test.js";

        console.log(JSON.stringify({
          branchRequire: cjsResults.branchRequire,
          branchImport: branchImported,
          externalRequire: cjsResults.externalRequire,
          externalImport: externalImported.type,
          wildcardImport: wildcardImported,
        }));
        "##,
    )
    .await;

    let options = overwrite_options();
    session
        .filesystem()
        .write_file(
            "/workspace/app/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "app-root",
                "imports": {
                    "#branch": {
                        "require": "./branch-require.cjs",
                        "import": "./branch-import.mjs",
                        "default": "./branch-default.cjs"
                    },
                    "#external": "dep/feature",
                    "#external/subpath/*": "dep/subpath/*"
                }
            }))
            .expect("encode app package.json"),
            options.clone(),
        )
        .await
        .expect("write app package manifest");
    session
        .filesystem()
        .write_file(
            "/workspace/app/branch-require.cjs",
            br#"module.exports = { value: "requirebranch" };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write branch require module");
    session
        .filesystem()
        .write_file(
            "/workspace/app/branch-import.mjs",
            br#"export default "importbranch";"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write branch import module");
    session
        .filesystem()
        .write_file(
            "/workspace/app/branch-default.cjs",
            br#"module.exports = { value: "defaultbranch" };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write branch default module");
    session
        .filesystem()
        .write_file(
            "/workspace/app/cjs-check.cjs",
            br##"
            module.exports = {
              branchRequire: require("#branch").value,
              externalRequire: require("#external").type,
            };
            "##
            .to_vec(),
            options.clone(),
        )
        .await
        .expect("write cjs check module");
    session
        .filesystem()
        .write_file(
            "/workspace/app/node_modules/dep/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "dep",
                "type": "module",
                "exports": {
                    "./feature": {
                        "require": "./feature-require.cjs",
                        "import": "./feature-import.mjs",
                        "default": "./feature-default.cjs"
                    },
                    "./subpath/*": "./subpath/*"
                }
            }))
            .expect("encode dep package.json"),
            options.clone(),
        )
        .await
        .expect("write dep package manifest");
    session
        .filesystem()
        .write_file(
            "/workspace/app/node_modules/dep/feature-require.cjs",
            br#"module.exports = { type: "cjs" };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write dep require target");
    session
        .filesystem()
        .write_file(
            "/workspace/app/node_modules/dep/feature-import.mjs",
            br#"export default { type: "esm" };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write dep import target");
    session
        .filesystem()
        .write_file(
            "/workspace/app/node_modules/dep/feature-default.cjs",
            br#"module.exports = { type: "default" };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write dep default target");
    session
        .filesystem()
        .write_file(
            "/workspace/app/node_modules/dep/subpath/test.js",
            br#"export default "wildcard-subpath";"#.to_vec(),
            options,
        )
        .await
        .expect("write dep subpath target");

    let result = session
        .exec_node_command(
            entrypoint.to_string(),
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
        .expect("node command should resolve package imports and exports");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let debug_report = serde_json::to_string_pretty(&report).expect("encode debug report");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .unwrap_or_else(|| panic!("structured payload missing\nreport={debug_report}"));
    let payload: serde_json::Value = serde_json::from_str(payload).expect("decode payload");

    assert_eq!(payload["branchRequire"].as_str(), Some("requirebranch"));
    assert_eq!(payload["branchImport"].as_str(), Some("importbranch"));
    assert_eq!(payload["externalRequire"].as_str(), Some("cjs"));
    assert_eq!(payload["externalImport"].as_str(), Some("esm"));
    assert_eq!(payload["wildcardImport"].as_str(), Some("wildcard-subpath"));
}
