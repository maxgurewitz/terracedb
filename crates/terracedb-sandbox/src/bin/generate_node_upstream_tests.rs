use clap::Parser;
use std::{
    collections::BTreeSet,
    fs,
    io,
    fmt::Write as _,
    path::{Path, PathBuf},
};

const DENY_TESTS: &[&str] = &[
    "/node/test/parallel/test-module-cache.js",
    "/node/test/parallel/test-module-children.js",
    "/node/test/parallel/test-module-circular-dependency-warning.js",
    "/node/test/parallel/test-module-circular-symlinks.js",
    "/node/test/parallel/test-module-create-require-multibyte.js",
    "/node/test/parallel/test-module-globalpaths-nodepath.js",
    "/node/test/parallel/test-module-isBuiltin.js",
    "/node/test/parallel/test-module-loading-deprecated.js",
    "/node/test/parallel/test-module-loading-error.js",
    "/node/test/parallel/test-module-loading-globalpaths.js",
    "/node/test/parallel/test-module-main-extension-lookup.js",
    "/node/test/parallel/test-module-main-fail.js",
    "/node/test/parallel/test-module-main-preserve-symlinks-fail.js",
    "/node/test/parallel/test-module-multi-extensions.js",
    "/node/test/parallel/test-module-parent-deprecation.js",
    "/node/test/parallel/test-module-parent-setter-deprecation.js",
    "/node/test/parallel/test-module-prototype-mutation.js",
    "/node/test/parallel/test-module-readonly.js",
    "/node/test/parallel/test-module-relative-lookup.js",
    "/node/test/parallel/test-module-run-main-monkey-patch.js",
    "/node/test/parallel/test-module-setsourcemapssupport.js",
    "/node/test/parallel/test-module-stat.js",
    "/node/test/parallel/test-module-strip-types.js",
    "/node/test/parallel/test-module-symlinked-peer-modules.js",
    "/node/test/parallel/test-module-version.js",
    "/node/test/parallel/test-module-wrap.js",
    "/node/test/parallel/test-require-delete-array-iterator.js",
    "/node/test/parallel/test-require-empty-main.js",
    "/node/test/parallel/test-require-enoent-dir.js",
    "/node/test/parallel/test-require-exceptions.js",
    "/node/test/parallel/test-require-extension-over-directory.js",
    "/node/test/parallel/test-require-extensions-main.js",
    "/node/test/parallel/test-require-extensions-same-filename-as-dir-trailing-slash.js",
    "/node/test/parallel/test-require-extensions-same-filename-as-dir.js",
    "/node/test/parallel/test-require-invalid-main-no-exports.js",
    "/node/test/parallel/test-require-invalid-package.js",
    "/node/test/parallel/test-require-json.js",
    "/node/test/parallel/test-require-long-path.js",
    "/node/test/parallel/test-require-mjs.js",
    "/node/test/parallel/test-require-nul.js",
    "/node/test/parallel/test-require-process.js",
    "/node/test/parallel/test-require-resolve-invalid-paths.js",
    "/node/test/parallel/test-require-resolve-opts-paths-relative.js",
    "/node/test/parallel/test-require-symlink.js",
    "/node/test/parallel/test-require-unicode.js",
    "/node/test/sequential/test-module-loading.js",
    "/node/test/sequential/test-require-cache-without-stat.js",
];

#[derive(Debug, Parser)]
#[command(name = "generate_node_upstream_tests")]
struct Cli {
    #[arg(long)]
    repo_root: Option<PathBuf>,
    #[arg(long)]
    wrapper_path: Option<PathBuf>,
    #[arg(long)]
    body_path: Option<PathBuf>,
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();
    let default_repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .to_path_buf();
    let repo_root = cli.repo_root.unwrap_or(default_repo_root);
    let tests_dir = repo_root.join("crates/terracedb-sandbox/tests");
    let generated_dir = tests_dir.join("generated");
    let wrapper_path = cli
        .wrapper_path
        .unwrap_or_else(|| tests_dir.join("generated_node_upstream_common_subset.rs"));
    let body_path = cli
        .body_path
        .unwrap_or_else(|| generated_dir.join("node_upstream_common_subset_body.rs"));

    fs::create_dir_all(&generated_dir)?;

    let tests = collect_tests(&repo_root)?;
    fs::write(&wrapper_path, wrapper_source())?;
    fs::write(&body_path, body_source(&tests))?;

    println!("wrote {}", wrapper_path.display());
    println!("wrote {}", body_path.display());
    Ok(())
}

fn collect_tests(repo_root: &Path) -> io::Result<Vec<String>> {
    let deny = DENY_TESTS.iter().copied().collect::<BTreeSet<_>>();
    let mut tests = Vec::new();
    tests.extend(scan_suite_dir(
        &repo_root.join("third_party/node-src/test/parallel"),
        repo_root,
    )?);
    tests.extend(scan_suite_dir(
        &repo_root.join("third_party/node-src/test/sequential"),
        repo_root,
    )?);
    tests.sort();
    tests.retain(|path| !deny.contains(path.as_str()));
    Ok(tests)
}

fn scan_suite_dir(dir: &Path, repo_root: &Path) -> io::Result<Vec<String>> {
    let mut tests = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let is_relevant = (name.starts_with("test-module-") || name.starts_with("test-require-"))
            && name.ends_with(".js");
        if !is_relevant {
            continue;
        }
        let relative = path
            .strip_prefix(repo_root.join("third_party/node-src"))
            .expect("path under third_party/node-src");
        tests.push(format!("/{}", relative.display()));
    }
    tests.sort();
    Ok(tests)
}

fn wrapper_source() -> String {
    r#"// @generated by cargo run -p terracedb-sandbox --bin generate_node_upstream_tests
#[path = "support/node_compat.rs"]
mod node_compat;

fn stdout_stderr_exit(result: &terracedb_sandbox::SandboxExecutionResult) -> (String, String, i64) {
    let report = result
        .result
        .as_ref()
        .and_then(|value| value.as_object())
        .expect("node command report");
    let stdout = report
        .get("stdout")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let stderr = report
        .get("stderr")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let exit_code = report
        .get("exitCode")
        .and_then(|value| value.as_i64())
        .unwrap_or_default();
    (stdout, stderr, exit_code)
}

include!("generated/node_upstream_common_subset_body.rs");
"#
    .to_string()
}

fn body_source(tests: &[String]) -> String {
    let mut source =
        String::from("// @generated by cargo run -p terracedb-sandbox --bin generate_node_upstream_tests\n");
    for test_path in tests {
        let test_name = Path::new(test_path)
            .file_stem()
            .and_then(|value| value.to_str())
            .expect("test filename");
        let rust_name = test_name.replace(['-', '.'], "_");
        writeln!(
            source,
            r#"
#[tokio::test]
async fn upstream_{rust_name}_runs() {{
    let result = node_compat::exec_upstream_node_test(
        "{test_path}",
    )
    .await
    .expect("execute upstream test");
    let (stdout, stderr, exit_code) = stdout_stderr_exit(&result);
    assert_eq!(
        exit_code, 0,
        "stdout:\n{{}}\n\nstderr:\n{{}}",
        stdout, stderr
    );
}}
"#,
        )
        .expect("write generated test");
    }
    source
}
