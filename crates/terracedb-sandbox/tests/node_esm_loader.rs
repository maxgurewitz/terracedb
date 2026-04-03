#[path = "support/node_compat.rs"]
mod node_compat;
#[path = "support/tracing.rs"]
mod tracing_support;

use terracedb_vfs::CreateOptions;

#[tokio::test]
async fn node_dynamic_import_supports_module_packages_and_package_imports() {
    tracing_support::init_tracing();
    let entrypoint = "/workspace/app/index.cjs";
    let session = node_compat::open_node_session(
        520,
        131,
        entrypoint,
        r#"
        return (async () => {
          const mod = await import("esm-pkg");
          console.log(JSON.stringify({
            default: mod.default,
            named: mod.named,
            alias: mod.alias,
            platform: mod.platform,
            isatty: mod.isatty,
            release: mod.release,
          }));
        })();
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
            "/workspace/app/node_modules/esm-pkg/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "esm-pkg",
                "type": "module",
                "exports": "./index.js",
                "imports": {
                    "#helper": "./helper.js"
                }
            }))
            .expect("encode package.json"),
            options.clone(),
        )
        .await
        .expect("write esm package manifest");
    session
        .filesystem()
        .write_file(
            "/workspace/app/node_modules/esm-pkg/index.js",
            br#"
            import helper, { named, alias } from '#helper';
            import process from 'node:process';
            import os from 'node:os';
            import tty from 'node:tty';

            export { named, alias };
            export const platform = process.platform;
            export const isatty = tty.isatty(1);
            export const release = os.release();
            export default helper;
            "#
            .to_vec(),
            options.clone(),
        )
        .await
        .expect("write esm package index");
    session
        .filesystem()
        .write_file(
            "/workspace/app/node_modules/esm-pkg/helper.js",
            br#"
            export default 'default-value';
            export const named = 'named-value';
            export const alias = 'alias-value';
            "#
            .to_vec(),
            options,
        )
        .await
        .expect("write esm helper module");

    let result = session
        .exec_node_command(
            entrypoint.to_string(),
            vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
            "/workspace/app".to_string(),
            std::collections::BTreeMap::from([("HOME".to_string(), "/workspace/home".to_string())]),
        )
        .await
        .expect("node command should import ESM package");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .expect("structured ESM payload");
    let payload: serde_json::Value = serde_json::from_str(payload).expect("decode ESM payload");
    assert_eq!(payload["default"].as_str(), Some("default-value"));
    assert_eq!(payload["named"].as_str(), Some("named-value"));
    assert_eq!(payload["alias"].as_str(), Some("alias-value"));
    assert_eq!(payload["platform"].as_str(), Some("linux"));
    assert_eq!(payload["isatty"].as_bool(), Some(false));
    assert!(
        payload["release"]
            .as_str()
            .map(|value| value.contains("terrace"))
            .unwrap_or(false),
        "expected deterministic terrace-flavored os.release, got {payload:#?}",
    );
}
