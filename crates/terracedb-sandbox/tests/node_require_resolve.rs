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
async fn node_require_resolve_matches_node_reference_cases() {
    tracing_support::init_tracing();

    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        540,
        151,
        entrypoint,
        r#"
        const result = {
          builtin: require.resolve("path"),
          builtinNode: require.resolve("node:fs"),
          builtinPaths: require.resolve.paths("path"),
          relativePaths: require.resolve.paths("./pkg/child.js"),
          depPaths: require.resolve.paths("dep"),
          dep: require.resolve("dep"),
          dot: require.resolve("."),
          viaRelativeOptions: require.resolve("./relative-subdir.js", { paths: ["subdir"] }),
          viaCurrentOptions: require.resolve(".", { paths: ["."] }),
        };
        try {
          require.resolve(".", { paths: [null] });
        } catch (error) {
          result.invalidPathsCode = error && error.code;
        }
        try {
          require.resolve("node:unknown");
        } catch (error) {
          result.unknownBuiltinCode = error && error.code;
        }
        console.log(JSON.stringify(result));
        "#,
    )
    .await;

    let options = overwrite_options();
    session
        .filesystem()
        .write_file(
            "/workspace/app/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "app-root",
                "main": "root.js"
            }))
            .expect("encode package.json"),
            options.clone(),
        )
        .await
        .expect("write package manifest");
    session
        .filesystem()
        .write_file(
            "/workspace/app/root.js",
            br#"module.exports = { name: "root-main" };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write root main");
    session
        .filesystem()
        .write_file(
            "/workspace/app/node_modules/dep/index.js",
            br#"module.exports = { ok: true };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write dep");
    session
        .filesystem()
        .write_file(
            "/workspace/app/pkg/child.js",
            br#"module.exports = { here: true };"#.to_vec(),
            options.clone(),
        )
        .await
        .expect("write child");
    session
        .filesystem()
        .write_file(
            "/workspace/app/subdir/relative-subdir.js",
            br#"module.exports = { value: "subdir" };"#.to_vec(),
            options,
        )
        .await
        .expect("write relative subdir file");

    let result = session
        .exec_node_command(
            entrypoint.to_string(),
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
        .expect("node command should expose require.resolve");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .expect("structured payload");
    let payload: serde_json::Value = serde_json::from_str(payload).expect("decode payload");

    assert_eq!(payload["builtin"].as_str(), Some("path"));
    assert_eq!(payload["builtinNode"].as_str(), Some("node:fs"));
    assert!(payload["builtinPaths"].is_null());
    assert_eq!(
        payload["relativePaths"]
            .as_array()
            .and_then(|values| values.first())
            .and_then(|value| value.as_str()),
        Some("/workspace/app"),
    );
    assert_eq!(
        payload["depPaths"]
            .as_array()
            .and_then(|values| values.first())
            .and_then(|value| value.as_str()),
        Some("/workspace/app/node_modules"),
    );
    assert_eq!(
        payload["dep"].as_str(),
        Some("/workspace/app/node_modules/dep/index.js"),
    );
    assert_eq!(payload["dot"].as_str(), Some("/workspace/app/root.js"));
    assert_eq!(
        payload["viaRelativeOptions"].as_str(),
        Some("/workspace/app/subdir/relative-subdir.js"),
    );
    assert_eq!(
        payload["viaCurrentOptions"].as_str(),
        Some("/workspace/app/root.js"),
    );
    assert_eq!(
        payload["invalidPathsCode"].as_str(),
        Some("ERR_INVALID_ARG_TYPE")
    );
    assert_eq!(
        payload["unknownBuiltinCode"].as_str(),
        Some("MODULE_NOT_FOUND")
    );
}
