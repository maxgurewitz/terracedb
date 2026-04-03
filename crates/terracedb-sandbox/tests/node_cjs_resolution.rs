#[path = "support/node_compat.rs"]
mod node_compat;
#[path = "support/tracing.rs"]
mod tracing_support;

use terracedb_vfs::CreateOptions;

#[tokio::test]
async fn node_cjs_require_dot_and_dotdot_resolve_relative_to_referrer_directory() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        521,
        132,
        entrypoint,
        r#"
        const child = require("./pkg/child.js");
        console.log(JSON.stringify(child));
        "#,
    )
    .await;

    let options = CreateOptions {
        create_parents: true,
        overwrite: true,
        ..Default::default()
    };

    session
        .filesystem()
        .write_file(
            "/workspace/app/pkg/index.js",
            br#"
            module.exports = {
              here: "pkg-index",
              parent: require("..").name,
            };
            "#
            .to_vec(),
            options.clone(),
        )
        .await
        .expect("write pkg index");

    session
        .filesystem()
        .write_file(
            "/workspace/app/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "app-root",
                "main": "root.js"
            }))
            .expect("encode app package.json"),
            options.clone(),
        )
        .await
        .expect("write root package.json");

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
            "/workspace/app/pkg/child.js",
            br#"
            module.exports = {
              dot: require(".").here,
              parent: require("..").name,
            };
            "#
            .to_vec(),
            options,
        )
        .await
        .expect("write child");

    let result = session
        .exec_node_command(
            entrypoint.to_string(),
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
        .expect("node command should resolve dot and dotdot");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .expect("structured payload");
    let payload: serde_json::Value = serde_json::from_str(payload).expect("decode payload");
    assert_eq!(payload["dot"].as_str(), Some("pkg-index"));
    assert_eq!(payload["parent"].as_str(), Some("root-main"));
}
