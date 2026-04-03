use terracedb_sandbox::SandboxError;

#[path = "support/node_compat.rs"]
mod node_compat_support;
#[path = "support/tracing.rs"]
mod tracing_support;

#[tokio::test]
async fn node_builtin_missing_module_member_fails_clearly() {
    let error = node_compat_support::exec_node_fixture(
        r#"
        const dns = require("dns");
        dns.lookup("example.com");
        "#,
    )
    .await
    .expect_err("unsupported builtin member should fail clearly");

    match error {
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE_NODE_UNIMPLEMENTED"),
                "{message}"
            );
            assert!(message.contains("dns.lookup"), "{message}");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn node_builtin_missing_member_is_emitted_into_runtime_trace() {
    let error = node_compat_support::exec_node_fixture(
        r#"
        const dns = require("dns");
        dns.lookup("example.com");
        "#,
    )
    .await
    .expect_err("unsupported builtin member should fail clearly");

    match error {
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains(r#"missing-builtin {"builtin":"dns","member":"lookup""#),
                "{message}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn node_partial_builtin_missing_member_fails_clearly() {
    let error = node_compat_support::exec_node_fixture(
        r#"
        const path = require("path");
        path.parse("/workspace/app/index.cjs");
        "#,
    )
    .await
    .expect_err("missing path.parse should fail clearly");

    match error {
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE_NODE_UNIMPLEMENTED"),
                "{message}"
            );
            assert!(message.contains("path.parse"), "{message}");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn node_timer_globals_fail_clearly_until_supported() {
    let error = node_compat_support::exec_node_fixture("setTimeout(() => {}, 5);\n")
        .await
        .expect_err("timers should fail explicitly until deterministic semantics exist");

    match error {
        SandboxError::Execution { message, .. } => {
            assert!(
                message.contains("ERR_TERRACE_NODE_UNIMPLEMENTED"),
                "{message}"
            );
            assert!(message.contains("global.setTimeout"), "{message}");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn node_builtin_accesses_are_reported_for_implemented_members() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const path = require("path");
        path.join("alpha", "beta");
        "#,
    )
    .await
    .expect("implemented builtin usage should succeed");

    let report = result.result.expect("node command report");
    let accesses = report["builtinAccesses"]
        .as_array()
        .expect("builtin access log");
    assert!(
        accesses.iter().any(|entry| {
            entry["builtin"].as_str() == Some("path")
                && entry["member"].as_str() == Some("join")
                && entry["status"].as_str() == Some("implemented")
        }),
        "expected a recorded path.join access, got: {accesses:#?}"
    );
}

#[tokio::test]
async fn node_command_drains_async_top_level_work() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        async function main() {
          const path = require("path");
          console.log(`before:${path.basename("/workspace/app/index.cjs")}`);
          await Promise.resolve();
          console.log("after");
        }

        main();
        "#,
    )
    .await
    .expect("async node command should complete");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("before:index.cjs"),
        "expected pre-await output, got: {stdout:?}"
    );
    assert!(
        stdout.contains("after"),
        "expected post-await output, got: {stdout:?}"
    );
}

#[tokio::test]
async fn node_command_drains_async_work_from_required_module() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const fs = require("fs");
        fs.writeFileSync(
          "/workspace/app/dep.cjs",
          `module.exports = async function (process) {
            const path = require("path");
            console.log("before:" + path.basename("/workspace/app/dep.cjs"));
            await Promise.resolve();
            console.log("after");
          };`,
        );
        require("/workspace/app/dep.cjs")(process);
        "#,
    )
    .await
    .expect("required async module should complete");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("before:dep.cjs"),
        "expected pre-await output from required module, got: {stdout:?}"
    );
    assert!(
        stdout.contains("after"),
        "expected post-await output from required module, got: {stdout:?}"
    );
}

#[tokio::test]
async fn node_fs_module_tolerates_commonjs_patch_style_mutation() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const fs = require("fs");
        fs.createReadStream = function () {};
        fs.createWriteStream = function () {};
        const readFile = fs.readFile;
        const writeFile = fs.writeFile;
        console.log(typeof readFile + ":" + typeof writeFile);
        "#,
    )
    .await
    .expect("fs patch-style mutation should keep executing");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("function:function"),
        "expected execution to continue after fs mutation, got: {stdout:?}"
    );
}

#[tokio::test]
async fn node_events_module_can_be_used_as_a_superclass() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const EE = require("node:events");
        class Timers extends EE {
          constructor() {
            super();
            this.count = 0;
            this.on("tick", () => this.count++);
          }
        }
        const timers = new Timers();
        timers.emit("tick");
        console.log(`${typeof EE}:${timers.count}:${timers instanceof EE}`);
        "#,
    )
    .await
    .expect("events module should be constructor-compatible");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("function:1:true"),
        "expected node:events to behave like a superclass export, got: {stdout:?}"
    );
}

#[tokio::test]
async fn node_subclass_with_private_field_initializer_constructs() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const EE = require("node:events");
        class TimersLike extends EE {
          #unfinished = new Map();
          constructor() {
            super();
            console.log(this.#unfinished instanceof Map);
          }
        }
        new TimersLike();
        "#,
    )
    .await
    .expect("subclass with private field initializer should construct");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("true"),
        "expected private field initializer to execute, got: {stdout:?}"
    );
}

#[tokio::test]
async fn node_subclass_with_private_arrow_field_constructs() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const EE = require("node:events");
        class TimersLike extends EE {
          #unfinished = new Map();
          #timeHandler = (level, name) => {
            this.#unfinished.set(name, level);
          };
          constructor() {
            super();
            this.#timeHandler("start", "npm");
            console.log(this.#unfinished.get("npm"));
          }
        }
        new TimersLike();
        "#,
    )
    .await
    .expect("subclass with private arrow field should construct");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("start"),
        "expected private arrow field to execute, got: {stdout:?}"
    );
}

#[tokio::test]
async fn node_graceful_fs_style_patch_keeps_executing() {
    let result =
        node_compat_support::exec_node_fixture(node_compat_support::graceful_fs_repro_source())
            .await
            .expect("graceful-fs repro should keep executing");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("before-require"),
        "expected pre-require marker, got: {stdout:?}"
    );
    assert!(
        stdout.contains("after-require:function"),
        "expected graceful-fs module to load, got: {stdout:?}"
    );
    assert!(
        stdout.contains("before-gracefulify:function"),
        "expected node:fs marker before gracefulify, got: {stdout:?}"
    );
    assert!(
        stdout.contains("after-gracefulify:function"),
        "expected gracefulify to return a patched fs object, got: {stdout:?}"
    );
    assert!(
        stdout.contains("after-write"),
        "expected execution to continue after gracefulify, got report: {report:#?}"
    );
}

#[tokio::test]
async fn node_fs_promises_missing_file_rejects_with_enoent_shape() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const fsp = require("node:fs/promises");
        (async () => {
          try {
            await fsp.readFile("/workspace/app/missing.txt", "utf8");
          } catch (error) {
            console.log(`${error.code}:${error.syscall}:${error.path}`);
          }
        })();
        "#,
    )
    .await
    .expect("fs/promises rejection should stay in guest JS");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("ENOENT:open:/workspace/app/missing.txt"),
        "expected ENOENT-style rejection shape, got: {stdout:?}"
    );
}

#[tokio::test]
async fn node_process_chdir_supports_graceful_fs_polyfill_shape() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const original = process.chdir;
        process.chdir = function (dir) {
          return original.call(process, dir);
        };
        if (Object.setPrototypeOf) {
          Object.setPrototypeOf(process.chdir, original);
        }
        console.log("after-prototype:" + typeof process.chdir);
        "#,
    )
    .await
    .expect("process.chdir should support graceful-fs polyfill shape");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout string");
    assert!(
        stdout.contains("after-prototype:function"),
        "expected process.chdir prototype rewrite to succeed, got: {stdout:?}"
    );
}
