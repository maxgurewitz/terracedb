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

fn normalize_file_urls(value: &mut Value) {
    match value {
        Value::String(string) if string.starts_with("file://") => {
            if let Ok(url) = url::Url::parse(string) {
                let replacement = url
                    .path_segments()
                    .map(|segments| segments.collect::<Vec<_>>())
                    .and_then(|segments| segments.last().cloned())
                    .map(|leaf| format!("file:///{leaf}"));
                if let Some(normalized) = replacement {
                    *string = normalized;
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize_file_urls(value);
            }
        }
        Value::Object(map) => {
            for value in map.values_mut() {
                normalize_file_urls(value);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn node_import_meta_resolve_matches_real_node_for_core_cases() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on test-esm-import-meta-resolve.mjs.
    let source = r#"
      import fs from 'node:fs';
      import os from 'node:os';
      import path from 'node:path';
      import { pathToFileURL } from 'node:url';

      const base = fs.mkdtempSync(path.join(os.tmpdir(), 'terrace-import-meta-resolve-'));
      const entryUrl = pathToFileURL(path.join(base, 'index.mjs')).href;
      const depUrl = pathToFileURL(path.join(base, 'dep.mjs')).href;

      fs.writeFileSync(path.join(base, 'dep.mjs'), "export default 'dep';\n");

      const missing = {};
      try {
        import.meta.resolve('does-not-exist');
      } catch (error) {
        missing.code = error.code;
        missing.name = error.name;
      }

      console.log(JSON.stringify({
        relative: import.meta.resolve('./dep.mjs'),
        missingRelative: import.meta.resolve('./notfound.mjs'),
        builtin: import.meta.resolve('fs'),
        builtinPrefixed: import.meta.resolve('node:fs'),
        absoluteUrl: import.meta.resolve('http://some-absolute/url'),
        weirdUrl: import.meta.resolve('some://weird/protocol'),
        explicitParent: import.meta.resolve('./dep.mjs', entryUrl),
        missing,
        depUrl,
      }));
    "#;

    let mut sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture_at("/workspace/app/index.mjs", source)
            .await
            .expect("sandbox import.meta.resolve contract"),
    );
    let mut real = parse_stdout_json(
        &node_compat_support::exec_real_node_file_with_extension(
            "import-meta-resolve",
            "mjs",
            source,
        )
        .expect("real node import.meta.resolve contract")
        .stdout,
    );

    normalize_file_urls(&mut sandbox);
    normalize_file_urls(&mut real);
    assert_eq!(sandbox, real);
}
