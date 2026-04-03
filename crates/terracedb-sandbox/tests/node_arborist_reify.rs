#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

use terracedb_vfs::CreateOptions;

#[tokio::test]
async fn node_arborist_reify_local_file_dependency_emits_stack() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(422, 95).await else {
        eprintln!("skipping arborist reify test because npm/cli repo is unavailable");
        return;
    };

    session
        .filesystem()
        .write_file(
            "/workspace/project/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "project",
                "version": "1.0.0"
            }))
            .expect("encode root package.json"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write project package.json");

    session
        .filesystem()
        .write_file(
            "/workspace/project/local-dep/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "local-dep",
                "version": "1.2.3",
                "main": "index.js"
            }))
            .expect("encode local dependency package.json"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write local dependency manifest");
    session
        .filesystem()
        .write_file(
            "/workspace/project/local-dep/index.js",
            b"module.exports = 'local-dep';\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write local dependency index");

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/arborist-reify.mjs",
        r#"
        const arboristModule = await import("/workspace/npm/node_modules/@npmcli/arborist/lib/index.js");
        const Arborist = arboristModule.default ?? arboristModule;
        const capture = async (label, fn) => {
          try {
            const value = await fn();
            return { ok: true, label, kind: typeof value, path: value?.path ?? null };
          } catch (error) {
            return {
              ok: false,
              label,
              name: error?.name ?? null,
              message: error?.message ?? null,
              stack: typeof error?.stack === "string" ? error.stack : null,
            };
          }
        };

        const arb = new Arborist({ path: "/workspace/project" });
        const loadActualOpts = { ignoreMissing: true };
        const buildIdealOpts = { add: ["./local-dep"] };
        const payload = {
          loadActual: await capture("loadActual", () => arb.loadActual(loadActualOpts)),
          metaFilenameAfterLoadActual: arb.actualTree?.meta?.filename ?? null,
          buildIdealTree: await capture("buildIdealTree", () => arb.buildIdealTree(buildIdealOpts)),
          metaFilenameAfterBuildIdealTree: arb.idealTree?.meta?.filename ?? null,
        };
        const parallel = new Arborist({ path: "/workspace/project" });
        payload.parallelLoadTrees = await capture("parallelLoadTrees", () =>
          Promise.all([
            parallel.loadActual(loadActualOpts),
            parallel.buildIdealTree(buildIdealOpts),
          ])
        );
        payload.reifyDryRun = await capture("reifyDryRun", () => new Arborist({
          path: "/workspace/project",
          dryRun: true,
        }).reify({
          add: ["./local-dep"],
          save: true,
          dryRun: true,
        }));
        payload.reifyPackageLockOnly = await capture("reifyPackageLockOnly", () => new Arborist({
          path: "/workspace/project",
          packageLockOnly: true,
        }).reify({
          add: ["./local-dep"],
          save: true,
          packageLockOnly: true,
        }));
        payload.reifyNoSave = await capture("reifyNoSave", () => new Arborist({
          path: "/workspace/project",
        }).reify({
          add: ["./local-dep"],
          save: false,
        }));
        payload.reify = await capture("reify", () => arb.reify({
          add: ["./local-dep"],
          save: true,
        }));
        console.log(JSON.stringify(payload));
        "#,
        &[],
    )
    .await
    .expect("run arborist reify");

    let report = result.result.clone().expect("node command report");
    let payload: serde_json::Value =
        serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
            .expect("stdout json");
    assert!(
        payload["reify"]["ok"].as_bool() == Some(false),
        "expected current direct arborist repro to fail until runtime gap is fixed, got payload: {payload:#?}"
    );
    assert!(
        !payload["loadActual"]["ok"].is_null(),
        "expected loadActual result to be captured, got payload: {payload:#?}\ntrace: {:#?}",
        npm_cli::node_runtime_trace(&result)
    );
    assert!(
        !payload["buildIdealTree"]["ok"].is_null(),
        "expected buildIdealTree result to be captured, got payload: {payload:#?}\ntrace: {:#?}",
        npm_cli::node_runtime_trace(&result)
    );
    assert!(
        !payload["parallelLoadTrees"]["ok"].is_null(),
        "expected parallel loadTrees result to be captured, got payload: {payload:#?}\ntrace: {:#?}",
        npm_cli::node_runtime_trace(&result)
    );
    eprintln!("arborist phase payload: {payload:#?}");
}

#[tokio::test]
async fn node_arborist_reify_stage_markers_locate_mutation_failure() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(423, 96).await else {
        eprintln!("skipping arborist reify stage test because npm/cli repo is unavailable");
        return;
    };

    session
        .filesystem()
        .write_file(
            "/workspace/project/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "project",
                "version": "1.0.0"
            }))
            .expect("encode root package.json"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write project package.json");
    session
        .filesystem()
        .write_file(
            "/workspace/project/local-dep/package.json",
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "local-dep",
                "version": "1.2.3",
                "main": "index.js"
            }))
            .expect("encode local dependency package.json"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write local dependency manifest");
    session
        .filesystem()
        .write_file(
            "/workspace/project/local-dep/index.js",
            b"module.exports = 'local-dep';\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write local dependency index");

    let reify_path = "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/reify.js";
    let original = session
        .filesystem()
        .read_file(reify_path)
        .await
        .expect("read reify.js")
        .expect("reify.js should exist");
    let mut instrumented = String::from_utf8(original).expect("reify.js utf8");
    instrumented = instrumented.replace(
        "    await this[_loadTrees](options)\n",
        "    await this[_loadTrees](options)\n    process.stderr.write(\"stage:after-loadTrees\\n\")\n",
    );
    instrumented = instrumented.replace(
        "    await this[_diffTrees]()\n",
        "    await this[_diffTrees]()\n    process.stderr.write(\"stage:after-diffTrees\\n\")\n",
    );
    instrumented = instrumented.replace(
        "await this[_reifyPackages]()",
        "process.stderr.write(\"stage:before-reifyPackages\\n\")\n    await this[_reifyPackages]()\n    process.stderr.write(\"stage:after-reifyPackages\\n\")",
    );
    instrumented = instrumented.replace(
        "          await this[action]()\n",
        "          process.stderr.write(`action:before:${String(action)}\\n`)\n          await this[action]()\n          process.stderr.write(`action:after:${String(action)}\\n`)\n",
    );
    instrumented = instrumented.replace(
        "    await this[_saveIdealTree](options)\n",
        "    process.stderr.write(\"stage:before-saveIdealTree\\n\")\n    await this[_saveIdealTree](options)\n    process.stderr.write(\"stage:after-saveIdealTree\\n\")\n",
    );
    instrumented = instrumented.replace(
        "await this.actualTree.meta.save()",
        "process.stderr.write(\"stage:before-meta-save\\n\")\n      await this.actualTree.meta.save()\n      process.stderr.write(\"stage:after-meta-save\\n\")",
    );
    instrumented = instrumented.replace(
        "        const pkgJson = await PackageJson.load(tree.path, { create: true })\n",
        "        process.stderr.write(`save:before-package-json-load:${tree.path}\\n`)\n        const pkgJson = await PackageJson.load(tree.path, { create: true })\n        process.stderr.write(`save:after-package-json-load:${tree.path}\\n`)\n",
    );
    instrumented = instrumented.replace(
        "        await pkgJson.save()\n",
        "        process.stderr.write(`save:before-package-json-save:${tree.path}\\n`)\n        await pkgJson.save()\n        process.stderr.write(`save:after-package-json-save:${tree.path}\\n`)\n",
    );
    instrumented = instrumented.replace(
        "      await this.idealTree.meta.save({\n        format: (this.options.formatPackageLock && format) ? format\n        : this.options.formatPackageLock,\n      })\n",
        "      process.stderr.write(`save:before-lockfile-save:${this.idealTree.meta.filename}\\n`)\n      await this.idealTree.meta.save({\n        format: (this.options.formatPackageLock && format) ? format\n        : this.options.formatPackageLock,\n      })\n      process.stderr.write(`save:after-lockfile-save:${this.idealTree.meta.filename}\\n`)\n",
    );
    assert!(
        instrumented.contains("stage:before-reifyPackages"),
        "expected instrumented source to contain reifyPackages marker"
    );
    session
        .filesystem()
        .write_file(
            reify_path,
            instrumented.into_bytes(),
            CreateOptions {
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write instrumented reify.js");

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/arborist-reify-stages.mjs",
        r#"
        const arboristModule = await import("/workspace/npm/node_modules/@npmcli/arborist/lib/index.js");
        const Arborist = arboristModule.default ?? arboristModule;
        try {
          await new Arborist({ path: "/workspace/project" }).reify({
            add: ["./local-dep"],
            save: true,
          });
          console.log(JSON.stringify({ ok: true }));
        } catch (error) {
          console.log(JSON.stringify({
            ok: false,
            message: error?.message ?? null,
          }));
        }
        "#,
        &[],
    )
    .await
    .expect("run instrumented arborist reify");

    let report = result.result.expect("node command report");
    let stderr = report["stderr"].as_str().unwrap_or_default();
    assert!(
        stderr.contains("stage:after-loadTrees"),
        "expected loadTrees marker, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("stage:after-diffTrees"),
        "expected diffTrees marker, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("stage:before-reifyPackages"),
        "expected reifyPackages entry marker, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("stage:after-reifyPackages"),
        "expected reifyPackages to complete before the current save-phase failure, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("stage:before-saveIdealTree"),
        "expected saveIdealTree marker, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("save:before-lockfile-save:/workspace/project/package-lock.json"),
        "expected lockfile save marker with the resolved package-lock filename, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("save:after-lockfile-save:/workspace/project/package-lock.json"),
        "expected lockfile save to complete, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("stage:after-saveIdealTree"),
        "expected saveIdealTree to complete, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("stage:before-meta-save"),
        "expected hidden lockfile save marker, got stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("stage:after-meta-save"),
        "expected hidden lockfile save to complete, got stderr:\n{stderr}"
    );
    eprintln!("instrumented arborist stderr:\n{stderr}");
}

#[tokio::test]
async fn node_arborist_shrinkwrap_reset_sets_default_filename() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(424, 97).await else {
        eprintln!("skipping shrinkwrap reset test because npm/cli repo is unavailable");
        return;
    };

    session
        .filesystem()
        .mkdir(
            "/workspace/project",
            terracedb_vfs::MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("create project dir");

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/shrinkwrap-reset.mjs",
        r#"
        const shrinkwrapModule = await import("/workspace/npm/node_modules/@npmcli/arborist/lib/shrinkwrap.js");
        const Shrinkwrap = shrinkwrapModule.default ?? shrinkwrapModule;
        const meta = await Shrinkwrap.reset({ path: "/workspace/project" });
        console.log(JSON.stringify({
          path: meta.path,
          filename: meta.filename,
          loadedFromDisk: meta.loadedFromDisk,
          hiddenLockfile: meta.hiddenLockfile,
        }));
        "#,
        &[],
    )
    .await
    .expect("run shrinkwrap reset script");

    let report = result.result.expect("node command report");
    let payload: serde_json::Value =
        serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
            .expect("stdout json");
    assert_eq!(payload["path"].as_str(), Some("/workspace/project"));
    assert_eq!(
        payload["filename"].as_str(),
        Some("/workspace/project/package-lock.json"),
        "expected Shrinkwrap.reset() to default filename to package-lock.json, got payload: {payload:#?}"
    );
    assert_eq!(payload["loadedFromDisk"].as_bool(), Some(false));
    assert_eq!(payload["hiddenLockfile"].as_bool(), Some(false));
}

#[tokio::test]
async fn node_arborist_shrinkwrap_load_reports_missing_file_shape() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(425, 98).await else {
        eprintln!("skipping shrinkwrap load test because npm/cli repo is unavailable");
        return;
    };

    session
        .filesystem()
        .mkdir(
            "/workspace/project",
            terracedb_vfs::MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("create project dir");

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/shrinkwrap-load.mjs",
        r#"
        const fsPromises = await import("node:fs/promises");
        const shrinkwrapModule = await import("/workspace/npm/node_modules/@npmcli/arborist/lib/shrinkwrap.js");
        const Shrinkwrap = shrinkwrapModule.default ?? shrinkwrapModule;

        const missingRead = await fsPromises.readFile("/workspace/project/package-lock.json", "utf8")
          .then(
            () => ({ ok: true }),
            (error) => ({
              ok: false,
              name: error?.name ?? null,
              message: error?.message ?? null,
              code: error?.code ?? null,
              errno: error?.errno ?? null,
              syscall: error?.syscall ?? null,
              path: error?.path ?? null,
              isError: error instanceof Error,
            }),
          );

        const meta = await Shrinkwrap.load({ path: "/workspace/project" });
        console.log(JSON.stringify({
          missingRead,
          meta: {
            path: meta.path,
            filename: meta.filename,
            loadedFromDisk: meta.loadedFromDisk,
            hiddenLockfile: meta.hiddenLockfile,
            type: meta.type,
            loadingError: meta.loadingError
              ? {
                  name: meta.loadingError?.name ?? null,
                  message: meta.loadingError?.message ?? null,
                  code: meta.loadingError?.code ?? null,
                  errno: meta.loadingError?.errno ?? null,
                  syscall: meta.loadingError?.syscall ?? null,
                  path: meta.loadingError?.path ?? null,
                }
              : null,
          },
        }));
        "#,
        &[],
    )
    .await
    .expect("run shrinkwrap load script");

    let report = result.result.expect("node command report");
    let payload: serde_json::Value =
        serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
            .expect("stdout json");

    eprintln!("shrinkwrap load payload: {payload:#?}");
    assert_eq!(payload["missingRead"]["ok"].as_bool(), Some(false));
    assert_eq!(payload["missingRead"]["code"].as_str(), Some("ENOENT"));
    assert_eq!(payload["missingRead"]["errno"].as_i64(), Some(-2));
    assert_eq!(payload["missingRead"]["syscall"].as_str(), Some("open"));
    assert_eq!(
        payload["missingRead"]["path"].as_str(),
        Some("/workspace/project/package-lock.json")
    );
    assert_eq!(
        payload["meta"]["filename"].as_str(),
        Some("/workspace/project/package-lock.json"),
        "expected Shrinkwrap.load() to keep the default package-lock filename, got payload: {payload:#?}"
    );
    assert_eq!(payload["meta"]["loadedFromDisk"].as_bool(), Some(false));
    assert!(
        payload["meta"]["loadingError"].is_null(),
        "expected empty project load to avoid shrinkwrap loadingError, got payload: {payload:#?}"
    );
}
