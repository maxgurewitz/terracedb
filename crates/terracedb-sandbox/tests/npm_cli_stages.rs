#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

fn assert_clear_runtime_failure(error: terracedb_sandbox::SandboxError) {
    match error {
        terracedb_sandbox::SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE")
                    || message.contains("node trace:")
                    || message.contains("recent trace:"),
                "expected a deterministic runtime failure with trace context, got: {message}"
            );
        }
        terracedb_sandbox::SandboxError::Service { service, message } => {
            assert!(
                message.contains("ERR_TERRACE") || message.contains("recent trace:"),
                "expected a deterministic service failure, got {service}: {message}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn npm_cli_stage_require_bin_cli_executes() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(430, 94).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/bin-stage.cjs",
        r#"
        require("/workspace/npm/bin/npm-cli.js");
        "#,
        &[],
    )
    .await
    .expect("bin/npm-cli.js stage should execute deterministically");

    assert!(
        result
            .module_graph
            .iter()
            .any(|path| path.ends_with("/bin/npm-cli.js")),
        "expected bin/npm-cli.js to load, got module graph: {:#?}",
        result.module_graph
    );
}

#[tokio::test]
async fn npm_cli_stage_require_lib_cli_executes() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(431, 95).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/lib-cli-stage.cjs",
        r#"
        const cli = require("/workspace/npm/lib/cli.js");
        return cli(process);
        "#,
        &[],
    )
    .await
    .expect("lib/cli.js stage should execute deterministically");

    assert!(
        result
            .module_graph
            .iter()
            .any(|path| path.ends_with("/lib/cli.js")),
        "expected lib/cli.js to load, got module graph: {:#?}",
        result.module_graph
    );
}

#[tokio::test]
async fn npm_cli_stage_require_entry_executes() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(432, 96).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/entry-stage.cjs",
        r#"
        const entry = require("/workspace/npm/lib/cli/entry.js");
        return entry(process, { node: process.version, npm: "v0.0.0", engines: ">=0", unsupportedMessage: "", off() {} });
        "#,
        &[],
    )
    .await
    .expect("lib/cli/entry.js stage should execute deterministically");

    assert!(
        result
            .module_graph
            .iter()
            .any(|path| path.ends_with("/lib/cli/entry.js")),
        "expected lib/cli/entry.js to load, got module graph: {:#?}",
        result.module_graph
    );
}

#[tokio::test]
async fn npm_cli_stage_gracefulify_returns_control() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(433, 97).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/gracefulify-stage.cjs",
        r#"
        require("fs").writeFileSync("/workspace/project/01-before.txt", "before");
        const gracefulFs = require("/workspace/npm/node_modules/graceful-fs/graceful-fs.js");
        require("fs").writeFileSync("/workspace/project/02-after-require.txt", typeof gracefulFs.gracefulify);
        const nodeFs = require("node:fs");
        require("fs").writeFileSync("/workspace/project/03-before-gracefulify.txt", typeof nodeFs.writeFileSync);
        const result = gracefulFs.gracefulify(nodeFs);
        require("fs").writeFileSync("/workspace/project/04-after-gracefulify.txt", typeof result.writeFileSync);
        require("fs").writeFileSync("/workspace/project/05-after.txt", typeof result);
        "#,
        &[],
    )
    .await
    .expect("gracefulify stage should execute deterministically");

    let report = result.result.expect("node command report");
    let before = session
        .filesystem()
        .read_file("/workspace/project/01-before.txt")
        .await
        .expect("read before marker");
    let after_require = session
        .filesystem()
        .read_file("/workspace/project/02-after-require.txt")
        .await
        .expect("read after-require marker");
    let before_gracefulify = session
        .filesystem()
        .read_file("/workspace/project/03-before-gracefulify.txt")
        .await
        .expect("read before-gracefulify marker");
    let after_gracefulify = session
        .filesystem()
        .read_file("/workspace/project/04-after-gracefulify.txt")
        .await
        .expect("read after-gracefulify marker");
    let after = session
        .filesystem()
        .read_file("/workspace/project/05-after.txt")
        .await
        .expect("read after marker");
    assert!(
        before.is_some()
            && after_require.is_some()
            && before_gracefulify.is_some()
            && after_gracefulify.is_some()
            && after.is_some(),
        "expected gracefulify stage to keep executing after the patch call, got report: {report:#?}"
    );
}

#[tokio::test]
async fn npm_cli_stage_npm_load_reports_install_command() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(436, 100).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/npm-load-stage.cjs",
        r#"
        const Npm = require("/workspace/npm/lib/npm.js");
        async function main() {
          try {
            __terraceDebugTrace("npm-load-stage", { step: "after-require-npm" });
            const npm = new Npm();
            __terraceDebugTrace("npm-load-stage", { step: "after-new-npm" });
            const loaded = await npm.load();
            __terraceDebugTrace("npm-load-stage", { step: "after-load" });
            console.log(JSON.stringify({
              loaded,
              argv: npm.argv,
              command: npm.command ?? null,
              processArgv: process.argv,
              exitCode: process.exitCode,
            }));
          } catch (error) {
            __terraceDebugTrace("npm-load-error", {
              message: String(error),
              stack: error && error.stack ? error.stack : null,
            });
            console.error(error && error.stack ? error.stack : String(error));
            throw error;
          }
        }
        return main();
        "#,
        &[
            "install",
            "./local-dep",
            "--ignore-scripts",
            "--no-audit",
            "--no-fund",
        ],
    )
    .await
    .expect("npm load stage should execute deterministically");

    let trace = npm_cli::node_runtime_trace(&result);
    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .unwrap_or_else(|| {
            panic!(
                "expected structured load payload in stdout, got report: {report:#?}\nnode trace: {trace:#?}"
            )
        });
    let payload: serde_json::Value =
        serde_json::from_str(payload).expect("decode npm load stage payload");
    assert_eq!(
        payload["loaded"]["exec"].as_bool(),
        Some(true),
        "expected npm.load() to request command execution, got: {payload:#?}"
    );
    assert_eq!(
        payload["loaded"]["command"].as_str(),
        Some("install"),
        "expected npm.load() to identify install as the command, got: {payload:#?}"
    );
}

#[tokio::test]
async fn npm_cli_stage_logfile_constructor_is_constructible() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(437, 101).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/npm-constructor-stage.cjs",
        r#"
        const LogFile = require("/workspace/npm/lib/utils/log-file.js");
        new LogFile();
        console.log("after-new-logfile");
        "#,
        &[],
    )
    .await
    .expect("LogFile constructor stage should execute deterministically");

    let report = result.result.expect("node command report");
    assert!(
        report["stdout"]
            .as_str()
            .unwrap_or_default()
            .contains("after-new-logfile"),
        "expected LogFile construction to succeed, got report: {report:#?}"
    );
}

#[tokio::test]
async fn npm_cli_stage_timers_constructor_is_constructible() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(438, 102).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/npm-timers-constructor-stage.cjs",
        r#"
        const Timers = require("/workspace/npm/lib/utils/timers.js");
        new Timers();
        console.log("after-new-timers");
        "#,
        &[],
    )
    .await
    .expect("Timers constructor stage should execute deterministically");

    let report = result.result.expect("node command report");
    assert!(
        report["stdout"]
            .as_str()
            .unwrap_or_default()
            .contains("after-new-timers"),
        "expected Timers construction to succeed, got report: {report:#?}"
    );
}

#[tokio::test]
async fn npm_cli_stage_npm_load_trace_reaches_success_boundary() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(439, 103).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/npm-error-trace-stage.cjs",
        r#"
        const fs = require("node:fs");
        const sourcePath = "/workspace/npm/lib/npm.js";
        const instrumentedPath = "/workspace/npm/lib/npm.instrumented.cjs";
        let instrumented = fs.readFileSync(sourcePath, "utf8");
        instrumented = instrumented.replace(
          "  async #load () {",
          "  async #load () {\n    __terraceDebugTrace(\"npm-load-phase\", { step: \"start\" })",
        );
        instrumented = instrumented.replace(
          "    await time.start('npm:load:whichnode', async () => {",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"before-whichnode\" })\n    await time.start('npm:load:whichnode', async () => {\n      __terraceDebugTrace(\"npm-load-phase\", { step: \"inside-whichnode\" })",
        );
        instrumented = instrumented.replace(
          "    await time.start('npm:load:configload', () => this.config.load())",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-whichnode\" })\n    __terraceDebugTrace(\"npm-load-phase\", { step: \"before-configload\" })\n    await time.start('npm:load:configload', () => this.config.load())\n    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-configload\" })",
        );
        instrumented = instrumented.replace(
          "    await this.#display.load({",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"before-display-load\", command })\n    await this.#display.load({",
        );
        instrumented = instrumented.replace(
          "    process.env.COLOR = this.color ? '1' : '0'",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-display-load\" })\n    process.env.COLOR = this.color ? '1' : '0'\n    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-set-color\" })",
        );
        instrumented = instrumented.replace(
          "    await time.start('npm:load:mkdirpcache', () =>",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"before-mkdir-cache\" })\n    await time.start('npm:load:mkdirpcache', () =>",
        );
        instrumented = instrumented.replace(
          "    if (this.config.get('logs-max') > 0) {",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-mkdir-cache\" })\n    if (this.config.get('logs-max') > 0) {",
        );
        instrumented = instrumented.replace(
          "    time.start('npm:load:setTitle', () => {",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-mkdir-logs\" })\n    time.start('npm:load:setTitle', () => {\n      __terraceDebugTrace(\"npm-load-phase\", { step: \"inside-set-title\" })",
        );
        instrumented = instrumented.replace(
          "    this.unrefPromises.push(this.#logFile.load({",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-set-title\" })\n    this.unrefPromises.push(this.#logFile.load({",
        );
        instrumented = instrumented.replace(
          "    this.#timers.load({",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-logfile-load\" })\n    this.#timers.load({",
        );
        instrumented = instrumented.replace(
          "    const configScope = this.config.get('scope')",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-timers-load\" })\n    const configScope = this.config.get('scope')",
        );
        instrumented = instrumented.replace(
          "    if (this.config.get('force')) {",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"after-scope\" })\n    if (this.config.get('force')) {",
        );
        instrumented = instrumented.replace(
          "    return { exec: true, command: commandArg, args: this.argv }",
          "    __terraceDebugTrace(\"npm-load-phase\", { step: \"before-return\", command: commandArg, argv: this.argv })\n    return { exec: true, command: commandArg, args: this.argv }",
        );
        instrumented = instrumented.replace(
          "  async #handleError (err) {",
          "  async #handleError (err) {\n    __terraceDebugTrace(\"npm-handle-error\", { step: \"start\", message: err && err.message ? err.message : String(err) })",
        );
        instrumented = instrumented.replace(
          "      const localPkg = await require('@npmcli/package-json')",
          "      __terraceDebugTrace(\"npm-handle-error\", { step: \"before-local-pkg\" })\n      const localPkg = await require('@npmcli/package-json')",
        );
        instrumented = instrumented.replace(
          "      Object.assign(err, this.#getError(err, { pkg: localPkg }))",
          "      __terraceDebugTrace(\"npm-handle-error\", { step: \"before-get-error\" })\n      Object.assign(err, this.#getError(err, { pkg: localPkg }))\n      __terraceDebugTrace(\"npm-handle-error\", { step: \"after-get-error\" })",
        );
        instrumented = instrumented.replace(
          "    this.finish(err)",
          "    __terraceDebugTrace(\"npm-handle-error\", { step: \"before-finish\" })\n    this.finish(err)\n    __terraceDebugTrace(\"npm-handle-error\", { step: \"after-finish\" })",
        );
        instrumented = instrumented.replace(
          "  #getError (rawErr, opts) {",
          "  #getError (rawErr, opts) {\n    __terraceDebugTrace(\"npm-get-error\", { step: \"start\", raw: rawErr && rawErr.message ? rawErr.message : String(rawErr) })",
        );
        instrumented = instrumented.replace(
          "    const { files = [], ...error } = require('./utils/error-message.js').getError(rawErr, {",
          "    __terraceDebugTrace(\"npm-get-error\", { step: \"before-getError-helper\" })\n    const { files = [], ...error } = require('./utils/error-message.js').getError(rawErr, {",
        );
        instrumented = instrumented.replace(
          "    const { writeFileSync } = require('node:fs')",
          "    __terraceDebugTrace(\"npm-get-error\", { step: \"after-getError-helper\", detailType: typeof error.detail, files: files.length })\n    const { writeFileSync } = require('node:fs')\n    __terraceDebugTrace(\"npm-get-error\", { step: \"after-require-fs\", writeFileSync: typeof writeFileSync })",
        );
        instrumented = instrumented.replace(
          "    for (const [file, content] of files) {",
          "    __terraceDebugTrace(\"npm-get-error\", { step: \"before-files-loop\" })\n    for (const [file, content] of files) {",
        );
        instrumented = instrumented.replace(
          "        writeFileSync(filePath, fileContent)",
          "        __terraceDebugTrace(\"npm-get-error\", { step: \"before-write-file\", filePath })\n        writeFileSync(filePath, fileContent)\n        __terraceDebugTrace(\"npm-get-error\", { step: \"after-write-file\", filePath })",
        );
        instrumented = instrumented.replace(
          "    outputError(error)",
          "    __terraceDebugTrace(\"npm-get-error\", { step: \"before-outputError\", detailPush: error.detail && typeof error.detail.push, summaryType: typeof error.summary })\n    outputError(error)\n    __terraceDebugTrace(\"npm-get-error\", { step: \"after-outputError\" })",
        );
        fs.writeFileSync(instrumentedPath, instrumented);
        const Npm = require(instrumentedPath);
        return (async () => {
          const npm = new Npm();
          await npm.load();
        })();
        "#,
        &["install", "./local-dep", "--ignore-scripts", "--no-audit", "--no-fund"],
    )
    .await
    .expect("instrumented npm error path should execute deterministically");

    let trace = npm_cli::node_runtime_trace(&result);
    let report = result.result.expect("node command report");
    println!("instrumented npm error trace:\n{trace:#?}");
    println!("instrumented npm error report:\n{report:#?}");
    assert!(
        trace.iter().any(|entry| entry
            .contains(r#"js:npm-load-phase {"step":"before-return","command":"install""#)),
        "expected instrumented npm load to reach the success return boundary, got report: {report:#?}\nnode trace: {trace:#?}"
    );
    assert!(
        !trace
            .iter()
            .any(|entry| entry.contains(r#"js:npm-handle-error {"step":"before-get-error"}"#)),
        "expected instrumented npm load to stay on the success path, got report: {report:#?}\nnode trace: {trace:#?}"
    );
}

#[tokio::test]
async fn npm_cli_stage_gracefulify_survives_without_object_setprototypeof() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(434, 98).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/gracefulify-no-setprototypeof-stage.cjs",
        r#"
        const originalSetPrototypeOf = Object.setPrototypeOf;
        Object.setPrototypeOf = undefined;
        try {
          require("fs").writeFileSync("/workspace/project/no-setproto-before.txt", "before");
          const gracefulFs = require("/workspace/npm/node_modules/graceful-fs/graceful-fs.js");
          const result = gracefulFs.gracefulify(require("node:fs"));
          require("fs").writeFileSync("/workspace/project/no-setproto-after.txt", typeof result);
        } finally {
          Object.setPrototypeOf = originalSetPrototypeOf;
        }
        "#,
        &[],
    )
    .await;

    let result = match result {
        Ok(value) => value,
        Err(error) => panic!("setPrototypeOf-disabled gracefulify stage still failed: {error}"),
    };

    let report = result.result.expect("node command report");
    let before = session
        .filesystem()
        .read_file("/workspace/project/no-setproto-before.txt")
        .await
        .expect("read before marker");
    let after = session
        .filesystem()
        .read_file("/workspace/project/no-setproto-after.txt")
        .await
        .expect("read after marker");
    assert!(
        before.is_some() && after.is_some(),
        "expected setPrototypeOf-disabled gracefulify stage to keep executing, got report: {report:#?}"
    );
}

#[tokio::test]
async fn npm_cli_stage_instrumented_graceful_fs_locates_failure_boundary() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(435, 99).await else {
        eprintln!("skipping npm cli stage test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/gracefulify-instrumented-stage.cjs",
        r#"
        const fs = require("fs");
        const originalPath = "/workspace/npm/node_modules/graceful-fs/graceful-fs.js";
        const instrumentedPath = "/workspace/npm/node_modules/graceful-fs/graceful-fs.instrumented.js";
        const original = fs.readFileSync(originalPath, "utf8");
        let instrumented = original.replace(
          "module.exports = patch(clone(fs))",
          `
          __terraceDebugTrace("graceful-fs", { step: "before-clone" });
          const __terraceCloned = clone(fs);
          __terraceDebugTrace("graceful-fs", { step: "after-clone" });
          const __terracePatched = patch(__terraceCloned);
          __terraceDebugTrace("graceful-fs", { step: "after-patch" });
          module.exports = __terracePatched
          `,
        );
        instrumented = instrumented.replace(
          "  polyfills(fs)",
          '  polyfills(fs)\n  __terraceDebugTrace("graceful-fs", { step: "after-polyfills" })',
        );
        instrumented = instrumented.replace(
          "  fs.createWriteStream = createWriteStream",
          '  fs.createWriteStream = createWriteStream\n  __terraceDebugTrace("graceful-fs", { step: "after-create-streams" })',
        );
        instrumented = instrumented.replace(
          "  fs.readdir = readdir",
          '  fs.readdir = readdir\n  __terraceDebugTrace("graceful-fs", { step: "after-readdir" })',
        );
        instrumented = instrumented.replace(
          "  var fs$ReadStream = fs.ReadStream",
          '  __terraceDebugTrace("graceful-fs", { step: "before-readstream" })\n  var fs$ReadStream = fs.ReadStream',
        );
        instrumented = instrumented.replace(
          "  Object.defineProperty(fs, 'ReadStream', {",
          '  __terraceDebugTrace("graceful-fs", { step: "before-define-readstream" })\n  Object.defineProperty(fs, \'ReadStream\', {',
        );
        instrumented = instrumented.replace(
          "  Object.defineProperty(fs, 'FileReadStream', {",
          '  __terraceDebugTrace("graceful-fs", { step: "before-define-filereadstream" })\n  Object.defineProperty(fs, \'FileReadStream\', {',
        );
        fs.writeFileSync(instrumentedPath, instrumented);
        require(instrumentedPath);
        "#,
        &[],
    )
    .await;

    let result = match result {
        Ok(value) => value,
        Err(error) => panic!("instrumented graceful-fs stage failed unexpectedly: {error}"),
    };
    let trace = npm_cli::node_runtime_trace(&result);
    let report = result.result.expect("node command report");
    assert!(
        trace
            .iter()
            .any(|entry| entry.contains(r#"js:graceful-fs {"step":"before-clone"}"#)),
        "expected instrumented graceful-fs to reach clone boundary, got report: {report:#?}"
    );
    assert!(
        trace
            .iter()
            .any(|entry| entry.contains(r#"js:graceful-fs {"step":"after-clone"}"#)),
        "expected instrumented graceful-fs to finish clone(fs), got report: {report:#?}"
    );
    assert!(
        trace
            .iter()
            .any(|entry| entry.contains(r#"js:graceful-fs {"step":"after-polyfills"}"#)),
        "expected instrumented graceful-fs to finish polyfills(fs), got report: {report:#?}"
    );
    assert!(
        trace
            .iter()
            .any(|entry| entry.contains(r#"js:graceful-fs {"step":"after-create-streams"}"#)),
        "expected instrumented graceful-fs to get past create stream patching, got report: {report:#?}"
    );
    assert!(
        trace
            .iter()
            .any(|entry| entry.contains(r#"js:graceful-fs {"step":"after-readdir"}"#)),
        "expected instrumented graceful-fs to get past readdir patching, got report: {report:#?}"
    );
    assert!(
        trace
            .iter()
            .any(|entry| entry.contains(r#"js:graceful-fs {"step":"before-readstream"}"#)),
        "expected instrumented graceful-fs to reach ReadStream handling, got report: {report:#?}"
    );
    assert!(
        trace
            .iter()
            .any(|entry| entry.contains(r#"js:graceful-fs {"step":"before-define-readstream"}"#)),
        "expected instrumented graceful-fs to reach ReadStream defineProperty, got report: {report:#?}"
    );
    assert!(
        trace.iter().any(|entry| {
            entry.contains(r#"js:graceful-fs {"step":"before-define-filereadstream"}"#)
        }),
        "expected instrumented graceful-fs to reach FileReadStream defineProperty, got report: {report:#?}"
    );
    assert!(
        trace
            .iter()
            .any(|entry| entry.contains(r#"js:graceful-fs {"step":"after-patch"}"#)),
        "expected instrumented graceful-fs to finish patch(clonedFs), got report: {report:#?}"
    );
}
