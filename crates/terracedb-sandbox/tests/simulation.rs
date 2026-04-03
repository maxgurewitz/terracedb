use std::{collections::BTreeMap, time::Duration};

use serde_json::json;
use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{
    CreateOptions, InMemoryVfsStore, MkdirOptions, VolumeConfig, VolumeId, VolumeStore,
};

use terracedb_sandbox::{
    BashRequest, BashService, CapabilityRegistry, DefaultSandboxStore, DeterministicBashService,
    DeterministicCapabilityModule, DeterministicCapabilityRegistry, DeterministicTypeScriptService,
    GitObjectFormat, GitProvenance, LocalReadonlyViewBridge, PackageCompatibilityMode,
    PackageInstallRequest, PullRequestRequest, ReadonlyViewCut, ReadonlyViewProtocolRequest,
    ReadonlyViewProtocolResponse, ReadonlyViewProtocolTransport, ReadonlyViewRequest,
    ReopenSessionOptions, SandboxCapability, SandboxConfig, SandboxExecutionKind,
    SandboxExecutionRequest, SandboxRuntimeBackend, SandboxRuntimeStateHandle, SandboxServices,
    SandboxStore, StaticReadonlyViewRegistry, TERRACE_RUNTIME_MODULE_CACHE_PATH, TypeCheckRequest,
    TypeScriptService, read_package_install_manifest,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SandboxSimulationCapture {
    revision: u64,
    state: String,
    actor_id: String,
    tool_names: Vec<String>,
    active_view_handles: usize,
    package_names: Vec<String>,
    initial_cache_misses: Vec<String>,
    replayed_cache_hits: Vec<String>,
    manifest_packages: Vec<String>,
    materialized: bool,
    branch: String,
    typescript_diagnostics: usize,
    bash_cwd: String,
    pr_url: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadonlyViewSimulationCapture {
    visible_entries: usize,
    durable_entries_before_flush: usize,
    durable_entries_after_flush: usize,
    visible_bytes: Vec<u8>,
}

fn run_sandbox_simulation(seed: u64) -> turmoil::Result<SandboxSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |context| async move {
            let vfs = InMemoryVfsStore::new(context.clock(), context.rng());
            let sandbox = DefaultSandboxStore::new(
                std::sync::Arc::new(vfs.clone()),
                context.clock(),
                SandboxServices::deterministic(),
            );
            let base_volume_id = VolumeId::new(0x7000 + seed as u128);
            let session_volume_id = VolumeId::new(0x7100 + seed as u128);

            let base = vfs
                .open_volume(
                    VolumeConfig::new(base_volume_id)
                        .with_chunk_size(4096)
                        .with_create_if_missing(true),
                )
                .await
                .expect("open base volume");
            base.fs()
                .write_file(
                    "/workspace/index.ts",
                    format!("export const seed = {seed};").into_bytes(),
                    CreateOptions {
                        create_parents: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed base file");
            base.fs()
                .write_file(
                    "/workspace/bad.ts",
                    b"export const broken: number = \"oops\";".to_vec(),
                    CreateOptions {
                        create_parents: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed bad file");

            let session = sandbox
                .open_session(
                    SandboxConfig::new(base_volume_id, session_volume_id)
                        .with_chunk_size(4096)
                        .with_package_compat(PackageCompatibilityMode::NpmPureJs),
                )
                .await
                .expect("open session");
            let typescript = DeterministicTypeScriptService::default();
            let packages = session
                .install_packages(PackageInstallRequest {
                    packages: vec!["zod".to_string(), "lodash".to_string()],
                    materialize_compatibility_view: true,
                })
                .await
                .expect("install packages");
            let handle = session
                .open_readonly_view(ReadonlyViewRequest {
                    cut: ReadonlyViewCut::Visible,
                    path: "/workspace".to_string(),
                    label: Some("workspace".to_string()),
                })
                .await
                .expect("open view");
            let diagnostics = typescript
                .check(
                    &session,
                    TypeCheckRequest {
                        roots: vec!["/workspace/bad.ts".to_string()],
                        ..Default::default()
                    },
                )
                .await
                .expect("check types");
            let bash = DeterministicBashService::default();
            let bash_report = bash
                .run(
                    &session,
                    BashRequest {
                        command: "mkdir -p scratch && cd scratch && pwd".to_string(),
                        cwd: "/workspace".to_string(),
                        ..Default::default()
                    },
                )
                .await
                .expect("run bash");
            seed_repo_backed_workspace(&session, seed).await;
            session
                .update_provenance(|provenance| {
                    provenance.git = Some(GitProvenance {
                        repo_root: "/repo".to_string(),
                        origin: terracedb_sandbox::GitRepositoryOrigin::HostImport,
                        head_commit: Some(format!("{seed:040x}")),
                        branch: Some("main".to_string()),
                        remote_url: Some("https://simulation.invalid/repo.git".to_string()),
                        remote_bridge_metadata: BTreeMap::new(),
                        object_format: Some(GitObjectFormat::Sha1),
                        pathspec: vec![".".to_string()],
                        dirty: false,
                    });
                    provenance.hoisted_source = Some(terracedb_sandbox::HoistedSource {
                        source_path: "/repo".to_string(),
                        mode: terracedb_sandbox::HoistMode::GitHead,
                    });
                })
                .await
                .expect("update provenance");
            let pr = reopened_pr(&session).await;
            session
                .close(terracedb_sandbox::CloseSessionOptions::default())
                .await
                .expect("close session");

            let reopened = sandbox
                .reopen_session(ReopenSessionOptions {
                    session_volume_id,
                    session_chunk_size: Some(4096),
                })
                .await
                .expect("reopen session");
            let replayed = reopened
                .install_packages(PackageInstallRequest {
                    packages: vec!["zod".to_string(), "lodash".to_string()],
                    materialize_compatibility_view: true,
                })
                .await
                .expect("reinstall packages after reopen");
            reopened
                .close_readonly_view(&handle.handle_id)
                .await
                .expect("close reopened view");
            let info = reopened.info().await;
            let manifest = read_package_install_manifest(reopened.filesystem().as_ref())
                .await
                .expect("read package manifest")
                .expect("package manifest should exist");
            let tool_names = reopened
                .volume()
                .tools()
                .recent(None)
                .await
                .expect("recent tool runs")
                .into_iter()
                .map(|run| run.name)
                .collect::<Vec<_>>();
            Ok(SandboxSimulationCapture {
                revision: info.revision,
                state: format!("{:?}", info.state),
                actor_id: reopened.runtime_handle().actor_id,
                tool_names,
                active_view_handles: info.provenance.active_view_handles.len(),
                package_names: packages.packages,
                initial_cache_misses: packages.metadata["cache_misses"]
                    .as_array()
                    .expect("cache misses array")
                    .iter()
                    .map(|value| value.as_str().expect("cache miss string").to_string())
                    .collect(),
                replayed_cache_hits: replayed.metadata["cache_hits"]
                    .as_array()
                    .expect("cache hits array")
                    .iter()
                    .map(|value| value.as_str().expect("cache hit string").to_string())
                    .collect(),
                manifest_packages: manifest
                    .packages
                    .iter()
                    .map(|package| package.package.clone())
                    .collect(),
                materialized: manifest.materialized_compatibility_view,
                branch: info
                    .provenance
                    .git
                    .and_then(|git| git.branch)
                    .expect("branch should be set"),
                typescript_diagnostics: diagnostics.diagnostics.len(),
                bash_cwd: bash_report.cwd,
                pr_url: pr.url,
            })
        })
}

async fn seed_repo_backed_workspace(session: &terracedb_sandbox::SandboxSession, seed: u64) {
    let commit_oid = format!("{seed:040x}");
    let tree_oid = format!("{:040x}", seed ^ 0x1111_1111_1111_1111);
    let blob_oid = format!("{:040x}", seed ^ 0x2222_2222_2222_2222);
    let fs = session.filesystem();
    for path in [
        "/workspace/.git",
        "/workspace/.git/refs",
        "/workspace/.git/refs/heads",
        "/workspace/.git/objects",
    ] {
        fs.mkdir(
            path,
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("create simulated git directory");
    }
    fs.write_file(
        "/workspace/.git/HEAD",
        b"ref: refs/heads/main\n".to_vec(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await
    .expect("write simulated git head");
    fs.write_file(
        "/workspace/.git/refs/heads/main",
        format!("{commit_oid}\n").into_bytes(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("write simulated main ref");
    fs.write_file(
        &format!("/workspace/.git/objects/{commit_oid}"),
        [
            b"commit\n".as_slice(),
            format!("tree {tree_oid}\n").as_bytes(),
        ]
        .concat(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("write simulated commit object");
    fs.write_file(
        &format!("/workspace/.git/objects/{blob_oid}"),
        b"blob\nseeded simulation repo\n".to_vec(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("write simulated blob object");
    fs.write_file(
        &format!("/workspace/.git/objects/{tree_oid}"),
        format!("tree\n100644 blob {blob_oid}\tREADME.md\n").into_bytes(),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("write simulated tree object");
    fs.write_file(
        "/workspace/.git/index.json",
        serde_json::to_vec(&json!({
            "entries": [{
                "path": "README.md",
                "oid": blob_oid,
                "mode": 0o100644
            }],
            "metadata": {}
        }))
        .expect("encode simulated index"),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("write simulated index");
    fs.write_file(
        terracedb_sandbox::disk::HOIST_MANIFEST_PATH,
        serde_json::to_vec_pretty(&json!({
            "format_version": 1,
            "source": {
                "kind": "host_path",
                "path": "/repo"
            },
            "target_root": "/workspace",
            "mode": "git_head",
            "git_provenance": {
                "repo_root": "/repo",
                "origin": "remote_import",
                "head_commit": commit_oid,
                "branch": "main",
                "remote_url": "https://simulation.invalid/repo.git",
                "pathspec": ["."],
                "dirty": false
            },
            "entries": []
        }))
        .expect("encode hoist manifest"),
        CreateOptions {
            create_parents: true,
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .expect("write hoist manifest");
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SandboxRuntimeSimulationCapture {
    actor_id: String,
    result: serde_json::Value,
    tool_names: Vec<String>,
    module_graph: Vec<String>,
    cache_entries: usize,
    eval_first_cache_misses: Vec<String>,
    eval_second_cache_hits: Vec<String>,
    eval_capability_calls: usize,
    runtime_cache_specifiers: Vec<String>,
}

fn run_runtime_simulation(seed: u64) -> turmoil::Result<SandboxRuntimeSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |context| async move {
            let vfs = InMemoryVfsStore::new(context.clock(), context.rng());
            let capabilities = DeterministicCapabilityRegistry::new(vec![
                DeterministicCapabilityModule::new(SandboxCapability::host_module("tickets"))
                    .expect("valid capability")
                    .with_echo_method("echo"),
            ])
            .expect("registry");
            let sandbox = DefaultSandboxStore::new(
                std::sync::Arc::new(vfs.clone()),
                context.clock(),
                SandboxServices::deterministic()
                    .with_capabilities(std::sync::Arc::new(capabilities.clone())),
            );
            let base_volume_id = VolumeId::new(0x7200 + seed as u128);
            let session_volume_id = VolumeId::new(0x7300 + seed as u128);

            let base = vfs
                .open_volume(
                    VolumeConfig::new(base_volume_id)
                        .with_chunk_size(4096)
                        .with_create_if_missing(true),
                )
                .await
                .expect("open base volume");
            base.fs()
                .write_file(
                    "/workspace/main.js",
                    br#"
                    import { readTextFile, writeTextFile } from "@terracedb/sandbox/fs";
                    import { echo } from "terrace:host/tickets";
                    const input = await readTextFile("/workspace/input.txt");
                    await writeTextFile("/workspace/out.txt", `${input}:${input.length}`);
                    export default await echo({
                        value: await readTextFile("/workspace/out.txt")
                    });
                    "#
                    .to_vec(),
                    CreateOptions {
                        create_parents: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed runtime module");
            base.fs()
                .write_file(
                    "/workspace/input.txt",
                    format!("seed-{seed}").into_bytes(),
                    CreateOptions {
                        create_parents: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed input file");

            let session = sandbox
                .open_session(SandboxConfig {
                    session_volume_id,
                    session_chunk_size: Some(4096),
                    base_volume_id,
                    durable_base: false,
                    workspace_root: "/workspace".to_string(),
                    package_compat: terracedb_sandbox::PackageCompatibilityMode::TerraceOnly,
                    conflict_policy: terracedb_sandbox::ConflictPolicy::Fail,
                    capabilities: capabilities.manifest(),
                    execution_policy: None,
                    hoisted_source: None,
                    git_provenance: None,
                })
                .await
                .expect("open session");

            let result = session
                .exec_module("/workspace/main.js")
                .await
                .expect("execute module");
            let backend = terracedb_sandbox::DeterministicRuntimeBackend::default();
            let handle = backend
                .start_session(&session.info().await)
                .await
                .expect("start direct runtime backend");
            let state = SandboxRuntimeStateHandle::new();
            let eval_request = SandboxExecutionRequest {
                kind: SandboxExecutionKind::Eval {
                    source: r#"
                        import { readTextFile } from "@terracedb/sandbox/fs";
                        import { echo } from "terrace:host/tickets";
                        export default await echo({
                            value: await readTextFile("/workspace/input.txt")
                        });
                    "#
                    .to_string(),
                    virtual_specifier: Some(
                        "terrace:/workspace/.terrace/runtime/reused-sim.mjs".to_string(),
                    ),
                },
                metadata: Default::default(),
            };
            let first_eval = backend
                .execute(&session, &handle, eval_request.clone(), state.clone())
                .await
                .expect("execute first eval");
            let second_eval = backend
                .execute(&session, &handle, eval_request, state)
                .await
                .expect("execute second eval");
            let runtime_cache = session
                .filesystem()
                .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
                .await
                .expect("read runtime cache")
                .expect("runtime cache should exist");
            let runtime_cache_specifiers =
                serde_json::from_slice::<serde_json::Value>(&runtime_cache)
                    .expect("decode runtime cache")
                    .as_array()
                    .expect("runtime cache array")
                    .iter()
                    .map(|entry| {
                        entry["specifier"]
                            .as_str()
                            .expect("runtime cache specifier")
                            .to_string()
                    })
                    .collect::<Vec<_>>();
            let tool_names = session
                .volume()
                .tools()
                .recent(None)
                .await
                .expect("recent tool runs")
                .into_iter()
                .map(|run| run.name)
                .collect::<Vec<_>>();
            Ok(SandboxRuntimeSimulationCapture {
                actor_id: session.runtime_handle().actor_id,
                result: result.result.expect("result json"),
                tool_names,
                module_graph: result.module_graph,
                cache_entries: result.cache_entries.len(),
                eval_first_cache_misses: first_eval.cache_misses,
                eval_second_cache_hits: second_eval.cache_hits,
                eval_capability_calls: second_eval.capability_calls.len(),
                runtime_cache_specifiers,
            })
        })
}

async fn reopened_pr(
    session: &terracedb_sandbox::SandboxSession,
) -> terracedb_sandbox::PullRequestReport {
    session
        .create_pull_request(PullRequestRequest {
            title: "Simulated PR".to_string(),
            body: "Body".to_string(),
            head_branch: "sandbox/sim".to_string(),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request")
}

#[test]
fn seeded_stub_sandbox_replays_open_reopen_close_and_metadata_updates() -> turmoil::Result {
    let first = run_sandbox_simulation(0x1234)?;
    let second = run_sandbox_simulation(0x1234)?;
    assert_eq!(first, second);
    assert!(
        first
            .tool_names
            .contains(&"sandbox.session.open".to_string())
    );
    assert!(
        first
            .tool_names
            .contains(&"sandbox.package.install".to_string())
    );
    assert!(
        first
            .tool_names
            .contains(&"sandbox.typescript.check".to_string())
    );
    assert!(first.tool_names.contains(&"sandbox.bash.exec".to_string()));
    assert!(
        first
            .tool_names
            .contains(&"sandbox.session.close".to_string())
    );
    assert_eq!(first.initial_cache_misses, vec!["lodash", "zod"]);
    assert_eq!(first.replayed_cache_hits, vec!["lodash", "zod"]);
    assert_eq!(first.manifest_packages, vec!["lodash", "zod"]);
    assert!(first.materialized);
    assert!(
        first.tool_names.contains(&"sandbox.pr.create".to_string()),
        "pr export should be recorded deterministically"
    );
    assert!(first.pr_url.contains("example.invalid"));
    assert!(first.revision >= 4);
    assert_eq!(first.typescript_diagnostics, 1);
    assert_eq!(first.bash_cwd, "/workspace/scratch");
    Ok(())
}

#[test]
fn seeded_runtime_execution_replays_module_graph_and_capability_calls() -> turmoil::Result {
    let first = run_runtime_simulation(0x1234)?;
    let second = run_runtime_simulation(0x1234)?;
    assert_eq!(first, second);
    assert!(
        first
            .tool_names
            .contains(&"sandbox.runtime.exec_module".to_string())
    );
    assert!(
        first
            .tool_names
            .contains(&"host_api.tickets.echo".to_string())
    );
    assert!(
        first
            .module_graph
            .contains(&"terrace:/workspace/main.js".to_string())
    );
    assert!(
        first
            .module_graph
            .contains(&"terrace:host/tickets".to_string())
    );
    assert_eq!(
        first.result,
        json!({
            "specifier": "terrace:host/tickets",
            "method": "echo",
            "args": [{
                "value": "seed-4660:9"
            }]
        })
    );
    assert!(first.cache_entries >= 2);
    assert!(
        first
            .eval_first_cache_misses
            .contains(&"terrace:/workspace/.terrace/runtime/reused-sim.mjs".to_string())
    );
    assert!(
        first
            .eval_second_cache_hits
            .contains(&"terrace:/workspace/.terrace/runtime/reused-sim.mjs".to_string())
    );
    assert_eq!(first.eval_capability_calls, 1);
    assert!(
        first
            .runtime_cache_specifiers
            .contains(&"terrace:/workspace/.terrace/runtime/reused-sim.mjs".to_string())
    );
    assert!(
        first
            .runtime_cache_specifiers
            .contains(&"@terracedb/sandbox/fs".to_string())
    );
    Ok(())
}

fn run_readonly_view_protocol_simulation(
    seed: u64,
) -> turmoil::Result<ReadonlyViewSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |context| async move {
            let vfs = InMemoryVfsStore::new(context.clock(), context.rng());
            let sandbox = DefaultSandboxStore::new(
                std::sync::Arc::new(vfs.clone()),
                context.clock(),
                SandboxServices::deterministic(),
            );
            let base_volume_id = VolumeId::new(0x7200 + seed as u128);
            let session_volume_id = VolumeId::new(0x7300 + seed as u128);

            let base = vfs
                .open_volume(
                    VolumeConfig::new(base_volume_id)
                        .with_chunk_size(4096)
                        .with_create_if_missing(true),
                )
                .await
                .expect("open base volume");
            base.fs()
                .write_file(
                    "/workspace/base.txt",
                    b"base".to_vec(),
                    CreateOptions {
                        create_parents: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed base");

            let session = sandbox
                .open_session(
                    SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096),
                )
                .await
                .expect("open session");
            session.flush().await.expect("flush base");
            session
                .filesystem()
                .write_file(
                    "/workspace/visible.txt",
                    format!("visible-{seed:x}").into_bytes(),
                    CreateOptions {
                        create_parents: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write visible file");

            let registry = std::sync::Arc::new(StaticReadonlyViewRegistry::new([session.clone()]));
            let service =
                std::sync::Arc::new(terracedb_sandbox::ReadonlyViewService::new(registry));
            let bridge = LocalReadonlyViewBridge::new(service);

            let visible = bridge
                .send(ReadonlyViewProtocolRequest::ReadDir {
                    location: terracedb_sandbox::ReadonlyViewLocation {
                        session_volume_id,
                        cut: ReadonlyViewCut::Visible,
                        path: "/workspace".to_string(),
                    },
                })
                .await
                .expect("read visible dir");
            let ReadonlyViewProtocolResponse::Directory {
                entries: visible_entries,
            } = visible
            else {
                panic!("expected directory response");
            };
            let visible_file = visible_entries
                .iter()
                .find(|entry| entry.name == "visible.txt")
                .expect("visible entry");

            let visible_bytes = bridge
                .send(ReadonlyViewProtocolRequest::ReadFile {
                    location: visible_file.location.clone(),
                })
                .await
                .expect("read visible file");
            let ReadonlyViewProtocolResponse::File { bytes } = visible_bytes else {
                panic!("expected file response");
            };

            let durable_before = bridge
                .send(ReadonlyViewProtocolRequest::ReadDir {
                    location: terracedb_sandbox::ReadonlyViewLocation {
                        session_volume_id,
                        cut: ReadonlyViewCut::Durable,
                        path: "/workspace".to_string(),
                    },
                })
                .await
                .expect("read durable dir before flush");
            let ReadonlyViewProtocolResponse::Directory {
                entries: durable_before_entries,
            } = durable_before
            else {
                panic!("expected directory response");
            };

            session.flush().await.expect("flush visible state");

            let durable_after = bridge
                .send(ReadonlyViewProtocolRequest::ReadDir {
                    location: terracedb_sandbox::ReadonlyViewLocation {
                        session_volume_id,
                        cut: ReadonlyViewCut::Durable,
                        path: "/workspace".to_string(),
                    },
                })
                .await
                .expect("read durable dir after flush");
            let ReadonlyViewProtocolResponse::Directory {
                entries: durable_after_entries,
            } = durable_after
            else {
                panic!("expected directory response");
            };

            Ok(ReadonlyViewSimulationCapture {
                visible_entries: visible_entries.len(),
                durable_entries_before_flush: durable_before_entries.len(),
                durable_entries_after_flush: durable_after_entries.len(),
                visible_bytes: bytes.expect("visible bytes"),
            })
        })
}

#[test]
fn seeded_readonly_view_protocol_replays_visible_and_durable_snapshot_semantics() -> turmoil::Result
{
    let first = run_readonly_view_protocol_simulation(0x5678)?;
    let second = run_readonly_view_protocol_simulation(0x5678)?;
    assert_eq!(first, second);
    assert!(first.visible_entries >= 2);
    assert_eq!(first.durable_entries_before_flush, 1);
    assert_eq!(first.durable_entries_after_flush, 2);
    assert!(
        String::from_utf8(first.visible_bytes)
            .expect("utf8")
            .starts_with("visible-")
    );
    Ok(())
}
