#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

use terracedb_vfs::CreateOptions;

#[tokio::test]
async fn npm_cli_minizlib_gzip_constructor_smoke_test() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(431, 95).await else {
        eprintln!("skipping minizlib smoke test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/minizlib-gzip-smoke.cjs",
        r#"
        const { Gzip } = require("/workspace/npm/node_modules/minizlib/dist/commonjs/index.js");
        const gzip = new Gzip({ portable: true });
        console.log(JSON.stringify({
          ctor: gzip.constructor && gzip.constructor.name,
          hasWrite: typeof gzip.write,
        }));
        "#,
        &[],
    )
    .await
    .expect("minizlib gzip smoke script should execute");

    let report = result.result.clone().expect("node command report");
    assert_eq!(
        report["exitCode"].as_i64(),
        Some(0),
        "expected minizlib gzip construction to succeed, got report: {report:#?}\nnode trace: {:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}

#[tokio::test]
async fn npm_cli_libnpmpack_smoke_test() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(432, 96).await else {
        eprintln!("skipping libnpmpack smoke test because npm/cli repo is unavailable");
        return;
    };

    let options = CreateOptions {
        create_parents: true,
        overwrite: true,
        ..Default::default()
    };
    session
        .filesystem()
        .write_file(
            "/workspace/project/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "project",
                "version": "1.0.0",
                "main": "index.js",
            }))
            .expect("encode project package.json"),
            options.clone(),
        )
        .await
        .expect("write project package.json");
    session
        .filesystem()
        .write_file(
            "/workspace/project/index.js",
            b"module.exports = 'hello pack';\n".to_vec(),
            options,
        )
        .await
        .expect("write project index");

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/libnpmpack-smoke.cjs",
        r#"
        const pack = require("/workspace/npm/node_modules/libnpmpack/lib/index.js");
        const pacote = require("/workspace/npm/node_modules/pacote/lib/index.js");
        const Arborist = require("/workspace/npm/node_modules/@npmcli/arborist/lib/index.js");
        (async () => {
          let stage = "start";
          try {
            stage = "manifest";
            const manifest = await pacote.manifest("file:.", { Arborist });
            stage = "tarball";
            const directTarball = await pacote.tarball(manifest._resolved, {
              Arborist,
              integrity: manifest._integrity,
            });
            stage = "pack";
            const tarball = await pack(".", {
              dryRun: false,
              ignoreScripts: true,
              packDestination: "/workspace/project",
            });
            console.log(JSON.stringify({
              ok: true,
              stage,
              directSize: directTarball.length,
              size: tarball.length,
            }));
          } catch (error) {
            console.log(JSON.stringify({
              ok: false,
              stage,
              message: error && error.message,
              stack: error && error.stack,
            }));
          }
        })();
        "#,
        &[],
    )
    .await
    .expect("libnpmpack smoke script should execute");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().unwrap_or_default();
    let payload = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value.get("ok").is_some())
        .expect("structured payload");
    assert_eq!(
        payload["ok"].as_bool(),
        Some(true),
        "expected libnpmpack to succeed, got payload: {payload:#?}\nnode trace: {:#?}\nreport: {report:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}

#[tokio::test]
async fn npm_cli_pacote_cached_tarball_smoke_test() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(435, 99).await else {
        eprintln!("skipping pacote cached tarball smoke test because npm/cli repo is unavailable");
        return;
    };

    let options = CreateOptions {
        create_parents: true,
        overwrite: true,
        ..Default::default()
    };
    session
        .filesystem()
        .write_file(
            "/workspace/project/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "project",
                "version": "1.0.0",
                "main": "index.js",
            }))
            .expect("encode project package.json"),
            options.clone(),
        )
        .await
        .expect("write project package.json");
    session
        .filesystem()
        .write_file(
            "/workspace/project/index.js",
            b"module.exports = 'hello cached pack';\n".to_vec(),
            options,
        )
        .await
        .expect("write project index");

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/pacote-cached-tarball-smoke.cjs",
        r#"
        const pacote = require("/workspace/npm/node_modules/pacote/lib/index.js");
        const cacache = require("/workspace/npm/node_modules/cacache/lib/index.js");
        const { Minipass } = require("/workspace/npm/node_modules/minipass/dist/commonjs/index.js");
        const Arborist = require("/workspace/npm/node_modules/@npmcli/arborist/lib/index.js");
        let nextStreamId = 1;
        const tag = (stream) => {
          if (!stream || typeof stream !== "object") return "<non-stream>";
          if (!stream.__terraceStreamId) {
            Object.defineProperty(stream, "__terraceStreamId", {
              value: nextStreamId++,
              enumerable: false,
              configurable: true,
            });
          }
          return `${stream.constructor && stream.constructor.name || "Object"}#${stream.__terraceStreamId}`;
        };
        const shouldTrace = (stream) => {
          const name = stream && stream.constructor && stream.constructor.name;
          return name === "Minipass" || name === "Pack" || name === "Pipeline" || name === "CacacheWriteStream";
        };
        const originalPipe = Minipass.prototype.pipe;
        Minipass.prototype.pipe = function (dest, opts) {
          if (shouldTrace(this) || shouldTrace(dest)) {
          console.log(JSON.stringify({
            trace: "minipass.pipe",
            from: tag(this),
            to: tag(dest),
            ended: !!this.emittedEnd,
            bufferLength: this.bufferLength,
            flowing: this.flowing,
          }));
          }
          return originalPipe.call(this, dest, opts);
        };
        const originalWrite = Minipass.prototype.write;
        Minipass.prototype.write = function (chunk, encoding, cb) {
          if (shouldTrace(this)) {
            console.log(JSON.stringify({
              trace: "minipass.write",
              stream: tag(this),
              chunkLength: chunk && chunk.length,
              chunkType: typeof chunk,
              flowing: this.flowing,
              bufferLength: this.bufferLength,
            }));
          }
          return originalWrite.call(this, chunk, encoding, cb);
        };
        const originalEmit = Minipass.prototype.emit;
        Minipass.prototype.emit = function (event, ...args) {
          if (shouldTrace(this) && (event === "data" || event === "resume" || event === "drain" || event === "end")) {
            console.log(JSON.stringify({
              trace: "minipass.emit",
              stream: tag(this),
              event,
              argLength: args[0] && args[0].length,
              flowing: this.flowing,
              bufferLength: this.bufferLength,
            }));
          }
          return originalEmit.call(this, event, ...args);
        };
        const originalPutStream = cacache.put.stream;
        cacache.put.stream = function (...args) {
          const stream = originalPutStream.apply(this, args);
          tag(stream);
          const originalWrite = stream.write;
          const originalFlush = stream.flush;
          stream.write = function (...writeArgs) {
            console.log(JSON.stringify({
              trace: "cacache.put.stream.write",
              stream: tag(this),
              chunkType: typeof writeArgs[0],
              chunkLength: writeArgs[0] && writeArgs[0].length,
            }));
            return originalWrite.apply(this, writeArgs);
          };
          if (typeof originalFlush === "function") {
            stream.flush = function (...flushArgs) {
              console.log(JSON.stringify({ trace: "cacache.put.stream.flush" }));
              return originalFlush.apply(this, flushArgs);
            };
          }
          stream.on("error", (error) => {
            console.log(JSON.stringify({
              trace: "cacache.put.stream.error",
              message: error && error.message,
              code: error && error.code,
            }));
          });
          return stream;
        };

        (async () => {
          try {
            const data = await pacote.tarball.stream(
              "file:.",
              (stream) => stream.concat(),
              {
                Arborist,
                cache: "/workspace/home/.npm/_cacache",
              },
            );
            console.log(JSON.stringify({ ok: true, size: data.length }));
          } catch (error) {
            console.log(JSON.stringify({
              ok: false,
              message: error && error.message,
              code: error && error.code,
              stack: error && error.stack,
            }));
          }
        })();
        "#,
        &[],
    )
    .await
    .expect("pacote cached tarball smoke script should execute");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().unwrap_or_default();
    let payload = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value.get("ok").is_some())
        .expect("structured payload");
    assert_eq!(
        payload["ok"].as_bool(),
        Some(true),
        "expected cached pacote tarball to succeed, got payload: {payload:#?}\nstdout:\n{stdout}\nnode trace: {:#?}\nreport: {report:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}

#[tokio::test]
async fn npm_cli_pack_dependency_require_chain_smoke_test() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(433, 97).await else {
        eprintln!(
            "skipping pack dependency require-chain test because npm/cli repo is unavailable"
        );
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/pack-require-chain.cjs",
        r#"
        try {
          __terraceDebugTrace("pack-require-stage", { stage: "minizlib:start" });
          require("/workspace/npm/node_modules/minizlib/dist/commonjs/index.js");
          __terraceDebugTrace("pack-require-stage", { stage: "minizlib:ok" });

          __terraceDebugTrace("pack-require-stage", { stage: "tar:start" });
          require("/workspace/npm/node_modules/tar/dist/commonjs/index.js");
          __terraceDebugTrace("pack-require-stage", { stage: "tar:ok" });

          __terraceDebugTrace("pack-require-stage", { stage: "pacote:start" });
          require("/workspace/npm/node_modules/pacote/lib/index.js");
          __terraceDebugTrace("pack-require-stage", { stage: "pacote:ok" });

          __terraceDebugTrace("pack-require-stage", { stage: "libnpmpack:start" });
          require("/workspace/npm/node_modules/libnpmpack/lib/index.js");
          __terraceDebugTrace("pack-require-stage", { stage: "libnpmpack:ok" });

          console.log(JSON.stringify({ ok: true }));
        } catch (error) {
          __terraceDebugTrace("pack-require-stage", {
            stage: "error",
            message: error && error.message,
            stack: error && error.stack,
          });
          console.log(JSON.stringify({
            ok: false,
            message: error && error.message,
            stack: error && error.stack,
          }));
        }
        "#,
        &[],
    )
    .await
    .expect("pack dependency require-chain script should execute");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().unwrap_or_default();
    let payload = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value.get("ok").is_some())
        .expect("structured payload");
    assert_eq!(
        payload["ok"].as_bool(),
        Some(true),
        "expected pack dependency require chain to succeed, got payload: {payload:#?}\nnode trace: {:#?}\nreport: {report:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}

#[tokio::test]
async fn npm_cli_tar_create_concat_smoke_test() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(434, 98).await else {
        eprintln!("skipping tar create smoke test because npm/cli repo is unavailable");
        return;
    };

    let options = CreateOptions {
        create_parents: true,
        overwrite: true,
        ..Default::default()
    };
    session
        .filesystem()
        .write_file(
            "/workspace/project/index.js",
            b"module.exports = 'hello tar';\n".to_vec(),
            options,
        )
        .await
        .expect("write project index");

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/tar-create-smoke.cjs",
        r#"
        const tar = require("/workspace/npm/node_modules/tar/dist/commonjs/index.js");
        const minipass = require("/workspace/npm/node_modules/minipass/dist/commonjs/index.js");
        const minizlib = require("/workspace/npm/node_modules/minizlib/dist/commonjs/index.js");
        const writeEntryMod = require("/workspace/npm/node_modules/tar/dist/commonjs/write-entry.js");
        const headerMod = require("/workspace/npm/node_modules/tar/dist/commonjs/header.js");
        const fs = require("fs");
        const zlib = require("zlib");
        const traceCall = (label, target, key) => {
          const original = target && target[key];
          if (typeof original !== "function") {
            console.log(JSON.stringify({
              trace: `${label}.${key}:missing`,
              type: typeof original,
            }));
            return;
          }
          target[key] = function (...args) {
            try {
              const result = original.apply(this, args);
              console.log(JSON.stringify({
                trace: `${label}.${key}`,
                resultType: typeof result,
              }));
              return result;
            } catch (error) {
              console.log(JSON.stringify({
                trace: `${label}.${key}:error`,
                message: error && error.message,
                stack: error && error.stack,
              }));
              throw error;
            }
          };
        };
        const traceEmit = (label, target) => {
          const original = target && target.emit;
          if (typeof original !== "function") {
            return;
          }
          target.emit = function (event, ...args) {
            const payload = {
              trace: `${label}.emit`,
              event: String(event),
            };
            if (event === "error") {
              const error = args[0];
              payload.message = error && error.message;
              payload.stack = error && error.stack;
            }
            console.log(JSON.stringify(payload));
            return original.call(this, event, ...args);
          };
        };
        traceCall("Minipass", minipass.Minipass && minipass.Minipass.prototype, "resume");
        traceCall("Minipass", minipass.Minipass && minipass.Minipass.prototype, "concat");
        traceCall("Minipass", minipass.Minipass && minipass.Minipass.prototype, "collect");
        traceCall("Minipass", minipass.Minipass && minipass.Minipass.prototype, "promise");
        traceCall("Minipass", minipass.Minipass && minipass.Minipass.prototype, "write");
        traceCall("fs", fs, "open");
        traceCall("fs", fs, "read");
        traceCall("fs", fs, "close");
        traceCall("tar.Header", headerMod.Header && headerMod.Header.prototype, "encode");
        traceCall("tar.WriteEntry", writeEntryMod.WriteEntry && writeEntryMod.WriteEntry.prototype, "write");
        traceCall("minizlib.Gzip", minizlib.Gzip && minizlib.Gzip.prototype, "write");
        traceCall("minizlib.Gzip", minizlib.Gzip && minizlib.Gzip.prototype, "end");
        traceCall("minizlib.Gzip", minizlib.Gzip && minizlib.Gzip.prototype, "resume");
        traceCall("zlib.Gzip", zlib.Gzip && zlib.Gzip.prototype, "write");
        traceEmit("tar.Pack", tar.Pack && tar.Pack.prototype);
        traceEmit("tar.WriteEntry", writeEntryMod.WriteEntry && writeEntryMod.WriteEntry.prototype);
        traceEmit("minizlib.Gzip", minizlib.Gzip && minizlib.Gzip.prototype);
        (async () => {
          let stage = "start";
          let streamError = null;
          try {
            stage = "before-create";
            const create = tar.c;
            const stream = create({
              cwd: "/workspace/project",
              prefix: "package/",
              portable: true,
              gzip: { level: 9 },
            }, ["index.js"]);
            stage = "after-create";
            if (stream && typeof stream.on === "function") {
              stream.on("error", (error) => {
                streamError = {
                  message: error && error.message,
                  stack: error && error.stack,
                  ctor: error && error.constructor && error.constructor.name,
                };
              });
            }
            const concat = stream && stream.concat;
            if (typeof concat !== "function") {
              console.log(JSON.stringify({
                ok: false,
                message: "stream.concat is not callable",
                stage,
                streamType: typeof stream,
                concatType: typeof concat,
                hasOn: !!(stream && stream.on),
                hasPipe: !!(stream && stream.pipe),
                ctor: stream && stream.constructor && stream.constructor.name,
              }));
              return;
            }
            stage = "before-concat-call";
            const promise = concat.call(stream);
            stage = "after-concat-call";
            const data = await promise;
            stage = "after-concat-await";
            console.log(JSON.stringify({ ok: true, stage, size: data.length }));
          } catch (error) {
            console.log(JSON.stringify({
              ok: false,
              stage,
              message: error && error.message,
              stack: error && error.stack,
              ctor: error && error.constructor && error.constructor.name,
              streamError,
            }));
          }
        })();
        "#,
        &[],
    )
    .await
    .expect("tar create smoke script should execute");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().unwrap_or_default();
    let payload = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value.get("ok").is_some())
        .expect("structured payload");
    assert_eq!(
        payload["ok"].as_bool(),
        Some(true),
        "expected tar.c(...).concat() to succeed, got payload: {payload:#?}\nnode trace: {:#?}\nreport: {report:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}

#[tokio::test]
async fn npm_cli_pack_creates_local_tarball() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(430, 94).await else {
        eprintln!("skipping npm cli pack test because npm/cli repo is unavailable");
        return;
    };

    let options = CreateOptions {
        create_parents: true,
        overwrite: true,
        ..Default::default()
    };
    session
        .filesystem()
        .write_file(
            "/workspace/project/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "project",
                "version": "1.0.0",
                "main": "index.js",
            }))
            .expect("encode project package.json"),
            options.clone(),
        )
        .await
        .expect("write project package.json");
    session
        .filesystem()
        .write_file(
            "/workspace/project/index.js",
            b"module.exports = 'hello pack';\n".to_vec(),
            options,
        )
        .await
        .expect("write project index");

    let result = npm_cli::run_npm_command(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        &["npm", "pack", "--ignore-scripts", "--json"],
    )
    .await
    .expect("npm pack should complete for a local package");

    let report = result.result.clone().expect("node command report");
    let node_trace = npm_cli::node_runtime_trace(&result);
    let node_trace_tail = node_trace
        .iter()
        .rev()
        .take(80)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    assert_eq!(
        report["exitCode"].as_i64(),
        Some(0),
        "expected npm pack to exit cleanly, got report: {report:#?}\nnode trace tail: {node_trace_tail:#?}"
    );

    let project_entries = session
        .filesystem()
        .readdir("/workspace/project")
        .await
        .ok()
        .map(|entries| {
            entries
                .into_iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let tarball = session
        .filesystem()
        .read_file("/workspace/project/project-1.0.0.tgz")
        .await
        .expect("read generated tarball")
        .unwrap_or_else(|| {
            panic!(
                "generated tarball should exist\nstdout:\n{}\n\nstderr:\n{}\n\nproject entries: {project_entries:#?}\n\nnode trace tail: {node_trace_tail:#?}\n\nreport: {report:#?}",
                report["stdout"].as_str().unwrap_or_default(),
                report["stderr"].as_str().unwrap_or_default(),
            )
        });
    assert!(
        !tarball.is_empty(),
        "expected npm pack to write a non-empty tarball"
    );
}
