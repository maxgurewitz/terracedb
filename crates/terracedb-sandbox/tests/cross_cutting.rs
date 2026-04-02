use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use terracedb::{
    DbDependencies, LogCursor, Rng, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_sandbox::{
    BashReport, BashRequest, BashService, BashSessionState, CapabilityRegistry, ConflictPolicy,
    DefaultSandboxStore, DeterministicBashService, DeterministicCapabilityModule,
    DeterministicCapabilityRegistry, DeterministicGitWorkspaceManager,
    DeterministicPackageInstaller, DeterministicPullRequestProviderClient,
    DeterministicReadonlyViewProvider, DeterministicRuntimeBackend, DeterministicTypeScriptService,
    GitProvenance, HoistMode, HoistRequest, HostGitWorkspaceManager, PackageCompatibilityMode,
    PackageInstallRequest, PullRequestRequest, ReadonlyViewCut, ReadonlyViewProtocolRequest,
    ReadonlyViewProtocolResponse, ReadonlyViewProtocolTransport, ReadonlyViewRequest,
    ReopenSessionOptions, SandboxCapability, SandboxConfig, SandboxServices, SandboxSession,
    SandboxStore, StaticReadonlyViewRegistry, TERRACE_BASH_SESSION_STATE_PATH,
    TERRACE_RUNTIME_MODULE_CACHE_PATH, TERRACE_TYPESCRIPT_MIRROR_PATH, TypeCheckRequest,
    TypeScriptMirrorState, TypeScriptService, read_package_install_manifest,
};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{
    ActivityKind, ActivityOptions, CreateOptions, InMemoryVfsStore, ReadOnlyVfsFileSystem,
    VolumeConfig, VolumeId, VolumeStore,
};
use uuid::Uuid;

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn sandbox_store(
    now: u64,
    seed: u64,
    services: SandboxServices,
) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(Arc::new(vfs.clone()), dependencies.clock, services);
    (vfs, sandbox)
}

fn deterministic_capabilities() -> DeterministicCapabilityRegistry {
    DeterministicCapabilityRegistry::new(vec![
        DeterministicCapabilityModule::new(SandboxCapability::host_module("tickets"))
            .expect("valid capability")
            .with_echo_method("echo"),
    ])
    .expect("build registry")
}

async fn seed_cross_cutting_base(
    store: &InMemoryVfsStore,
    volume_id: VolumeId,
    seed: u64,
) -> Result<(), terracedb_sandbox::SandboxError> {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await?;
    let fs = base.fs();
    fs.write_file(
        "/workspace/main.js",
        br#"
        import { readTextFile, writeTextFile } from "@terracedb/sandbox/fs";
        import { echo } from "terrace:host/tickets";

        const message = readTextFile("/workspace/message.txt");
        const rendered = `${message}:${message.length}`;
        writeTextFile("/workspace/runtime.txt", rendered);
        export default echo({ value: rendered });
        "#
        .to_vec(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await?;
    fs.write_file(
        "/workspace/message.txt",
        format!("seed-{seed:x}").into_bytes(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await?;
    fs.write_file(
        "/workspace/src/bad.ts",
        b"export const broken: number = \"oops\";\n".to_vec(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrossCuttingOperation {
    FilesystemMutation,
    GuestExecution,
    TypeCheck,
    BashWrite,
    InstallPackages,
    ViewProbe,
    UpdateProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CrossCuttingStepCapture {
    operation: CrossCuttingOperation,
    capture: SandboxCrossCuttingCapture,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CrossCuttingSimulationTrace {
    steps: Vec<CrossCuttingStepCapture>,
    flushed: SandboxCrossCuttingCapture,
    reopened: SandboxCrossCuttingCapture,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SandboxCrossCuttingCapture {
    revision: u64,
    state: String,
    actor_id: String,
    visible_files: BTreeMap<String, Option<String>>,
    durable_files: BTreeMap<String, Option<String>>,
    package_names: Vec<String>,
    package_materialized: bool,
    compatibility_lodash_exists: bool,
    typescript_roots: Vec<String>,
    typescript_file_count: usize,
    bash_cwd: Option<String>,
    bash_history_len: usize,
    branch: Option<String>,
    active_view_handles: Vec<String>,
    capability_values: Vec<String>,
    runtime_cache_specifiers: Vec<String>,
    tool_names: Vec<String>,
    activity_tool_subjects: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct SandboxShadowModel {
    visible_files: BTreeMap<String, Option<String>>,
    durable_files: BTreeMap<String, Option<String>>,
    package_names: BTreeSet<String>,
    package_materialized: bool,
    typescript_roots: Vec<String>,
    typescript_file_count: usize,
    bash_cwd: Option<String>,
    bash_history_len: usize,
    branch: Option<String>,
    active_view_handles: BTreeSet<String>,
    capability_values: Vec<String>,
    required_runtime_cache_specifiers: BTreeSet<String>,
}

impl SandboxShadowModel {
    fn new(seed: u64) -> Self {
        let visible_files = tracked_paths()
            .into_iter()
            .map(|path| {
                let value = match path {
                    "/workspace/message.txt" => Some(format!("seed-{seed:x}")),
                    "/workspace/src/bad.ts" => {
                        Some("export const broken: number = \"oops\";\n".to_string())
                    }
                    _ => None,
                };
                (path.to_string(), value)
            })
            .collect::<BTreeMap<_, _>>();
        Self {
            visible_files: visible_files.clone(),
            durable_files: visible_files,
            ..Default::default()
        }
    }

    fn flush(&mut self) {
        self.durable_files = self.visible_files.clone();
    }
}

fn tracked_paths() -> Vec<&'static str> {
    vec![
        "/workspace/message.txt",
        "/workspace/runtime.txt",
        "/workspace/src/bad.ts",
        "/workspace/scratch/note.txt",
    ]
}

fn shuffle_operations(rng: &dyn Rng) -> Vec<CrossCuttingOperation> {
    let mut operations = vec![
        CrossCuttingOperation::FilesystemMutation,
        CrossCuttingOperation::GuestExecution,
        CrossCuttingOperation::TypeCheck,
        CrossCuttingOperation::BashWrite,
        CrossCuttingOperation::InstallPackages,
        CrossCuttingOperation::ViewProbe,
        CrossCuttingOperation::UpdateProvenance,
    ];
    for index in (1..operations.len()).rev() {
        let swap = (rng.next_u64() as usize) % (index + 1);
        operations.swap(index, swap);
    }
    operations
}

async fn chronological_tool_names(
    session: &SandboxSession,
) -> Result<Vec<String>, terracedb_sandbox::SandboxError> {
    let mut runs = session.volume().tools().recent(None).await?;
    runs.reverse();
    Ok(runs.into_iter().map(|run| run.name).collect())
}

async fn activity_tool_subjects(
    session: &SandboxSession,
) -> Result<Vec<String>, terracedb_sandbox::SandboxError> {
    Ok(session
        .volume()
        .activity_since(LogCursor::beginning(), ActivityOptions::default())
        .await?
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|entry| {
            matches!(
                entry.kind,
                ActivityKind::ToolSucceeded | ActivityKind::ToolFailed
            )
        })
        .filter_map(|entry| entry.subject)
        .collect())
}

async fn read_utf8_visible(
    session: &SandboxSession,
    path: &str,
) -> Result<Option<String>, terracedb_sandbox::SandboxError> {
    session
        .filesystem()
        .read_file(path)
        .await?
        .map(|bytes| {
            String::from_utf8(bytes).map_err(|error| terracedb_sandbox::SandboxError::Service {
                service: "cross-cutting-test",
                message: format!("{path} is not valid utf-8: {error}"),
            })
        })
        .transpose()
}

async fn read_utf8_readonly(
    filesystem: &dyn ReadOnlyVfsFileSystem,
    path: &str,
) -> Result<Option<String>, terracedb_sandbox::SandboxError> {
    filesystem
        .read_file(path)
        .await?
        .map(|bytes| {
            String::from_utf8(bytes).map_err(|error| terracedb_sandbox::SandboxError::Service {
                service: "cross-cutting-test",
                message: format!("{path} is not valid utf-8: {error}"),
            })
        })
        .transpose()
}

async fn capture_state(
    session: &SandboxSession,
) -> Result<SandboxCrossCuttingCapture, terracedb_sandbox::SandboxError> {
    let info = session.info().await;
    let durable_fs = session.readonly_fs(ReadonlyViewCut::Durable).await?;

    let mut visible_files = BTreeMap::new();
    let mut durable_files = BTreeMap::new();
    for path in tracked_paths() {
        visible_files.insert(path.to_string(), read_utf8_visible(session, path).await?);
        durable_files.insert(
            path.to_string(),
            read_utf8_readonly(durable_fs.as_ref(), path).await?,
        );
    }

    let package_manifest = read_package_install_manifest(session.filesystem().as_ref()).await?;
    let (package_names, package_materialized) = match package_manifest {
        Some(manifest) => (
            manifest
                .packages
                .into_iter()
                .map(|package| package.package)
                .collect(),
            manifest.materialized_compatibility_view,
        ),
        None => (Vec::new(), false),
    };

    let compatibility_lodash_exists = session
        .filesystem()
        .read_file("/.terrace/npm/node_modules/lodash/package.json")
        .await?
        .is_some();

    let typescript = session
        .filesystem()
        .read_file(TERRACE_TYPESCRIPT_MIRROR_PATH)
        .await?
        .map(|bytes| serde_json::from_slice::<TypeScriptMirrorState>(&bytes))
        .transpose()?;
    let (typescript_roots, typescript_file_count) = match typescript {
        Some(mirror) => (mirror.roots, mirror.files.len()),
        None => (Vec::new(), 0),
    };

    let bash = session
        .filesystem()
        .read_file(TERRACE_BASH_SESSION_STATE_PATH)
        .await?
        .map(|bytes| serde_json::from_slice::<BashSessionState>(&bytes))
        .transpose()?;
    let (bash_cwd, bash_history_len) = match bash {
        Some(state) => (Some(state.cwd), state.history.len()),
        None => (None, 0),
    };

    let runtime_cache_specifiers = session
        .filesystem()
        .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
        .await?
        .map(
            |bytes| -> Result<Vec<String>, terracedb_sandbox::SandboxError> {
                let value: serde_json::Value = serde_json::from_slice(&bytes)?;
                Ok(value
                    .as_array()
                    .expect("runtime module cache should be an array")
                    .iter()
                    .filter_map(|entry| entry.get("specifier").and_then(|value| value.as_str()))
                    .map(ToString::to_string)
                    .collect())
            },
        )
        .transpose()?
        .unwrap_or_default();

    let mut capability_values = Vec::new();
    let mut runs = session.volume().tools().recent(None).await?;
    runs.reverse();
    for run in &runs {
        if run.name != "host_api.tickets.echo" {
            continue;
        }
        let Some(result) = &run.result else {
            continue;
        };
        if let Some(value) = result
            .get("value")
            .and_then(|value| value.get("args"))
            .and_then(|value| value.as_array())
            .and_then(|args| args.first())
            .and_then(|value| value.get("value"))
            .and_then(|value| value.as_str())
        {
            capability_values.push(value.to_string());
        }
    }

    Ok(SandboxCrossCuttingCapture {
        revision: info.revision,
        state: format!("{:?}", info.state),
        actor_id: session.runtime_handle().actor_id,
        visible_files,
        durable_files,
        package_names,
        package_materialized,
        compatibility_lodash_exists,
        typescript_roots,
        typescript_file_count,
        bash_cwd,
        bash_history_len,
        branch: info.provenance.git.and_then(|git| git.branch),
        active_view_handles: info
            .provenance
            .active_view_handles
            .into_iter()
            .map(|handle| handle.handle_id)
            .collect(),
        capability_values,
        runtime_cache_specifiers,
        tool_names: chronological_tool_names(session).await?,
        activity_tool_subjects: activity_tool_subjects(session).await?,
    })
}

fn assert_capture_matches_shadow(
    seed: u64,
    operation: &str,
    capture: &SandboxCrossCuttingCapture,
    shadow: &SandboxShadowModel,
) {
    assert_eq!(
        capture.visible_files, shadow.visible_files,
        "seed {seed:#x} {operation}: visible file shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.durable_files, shadow.durable_files,
        "seed {seed:#x} {operation}: durable file shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.package_names,
        shadow.package_names.iter().cloned().collect::<Vec<_>>(),
        "seed {seed:#x} {operation}: package manifest shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.package_materialized, shadow.package_materialized,
        "seed {seed:#x} {operation}: package materialization shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.compatibility_lodash_exists, shadow.package_materialized,
        "seed {seed:#x} {operation}: compatibility root shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.typescript_roots, shadow.typescript_roots,
        "seed {seed:#x} {operation}: TypeScript mirror roots shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.typescript_file_count, shadow.typescript_file_count,
        "seed {seed:#x} {operation}: TypeScript mirror file-count shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.bash_cwd, shadow.bash_cwd,
        "seed {seed:#x} {operation}: bash cwd shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.bash_history_len, shadow.bash_history_len,
        "seed {seed:#x} {operation}: bash history shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.branch, shadow.branch,
        "seed {seed:#x} {operation}: provenance branch shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.active_view_handles,
        shadow
            .active_view_handles
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        "seed {seed:#x} {operation}: readonly view handles shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.capability_values, shadow.capability_values,
        "seed {seed:#x} {operation}: capability-visible shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    let runtime_cache_specifiers = capture
        .runtime_cache_specifiers
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert!(
        shadow
            .required_runtime_cache_specifiers
            .is_subset(&runtime_cache_specifiers),
        "seed {seed:#x} {operation}: runtime cache shadow diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
    assert_eq!(
        capture.tool_names, capture.activity_tool_subjects,
        "seed {seed:#x} {operation}: tool/activity ordering diverged\ncapture={capture:#?}\nshadow={shadow:#?}"
    );
}

fn run_cross_cutting_simulation(seed: u64) -> turmoil::Result<CrossCuttingSimulationTrace> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |context| async move {
            let capabilities = deterministic_capabilities();
            let services = SandboxServices::deterministic()
                .with_capabilities(Arc::new(capabilities.clone()));
            let vfs = InMemoryVfsStore::new(context.clock(), context.rng());
            let sandbox =
                DefaultSandboxStore::new(Arc::new(vfs.clone()), context.clock(), services);
            let base_volume_id = VolumeId::new(0x7400 + seed as u128);
            let session_volume_id = VolumeId::new(0x7500 + seed as u128);
            seed_cross_cutting_base(&vfs, base_volume_id, seed)
                .await
                .expect("seed cross-cutting base");

            let session = sandbox
                .open_session(SandboxConfig {
                    session_volume_id,
                    session_chunk_size: Some(4096),
                    base_volume_id,
                    durable_base: false,
                    workspace_root: "/workspace".to_string(),
                    package_compat: PackageCompatibilityMode::NpmPureJs,
                    conflict_policy: ConflictPolicy::Fail,
                    capabilities: capabilities.manifest(),
                    execution_policy: None,
                    hoisted_source: None,
                    git_provenance: None,
                })
                .await
                .expect("open cross-cutting session");

            let registry = Arc::new(StaticReadonlyViewRegistry::new([session.clone()]));
            let bridge = terracedb_sandbox::LocalReadonlyViewBridge::new(Arc::new(
                terracedb_sandbox::ReadonlyViewService::new(registry),
            ));
            let bash = DeterministicBashService::default();
            let typescript = DeterministicTypeScriptService::default();
            let mut shadow = SandboxShadowModel::new(seed);
            let operations = shuffle_operations(context.rng().as_ref());
            let mut steps = Vec::new();

            for operation in operations {
                match operation {
                    CrossCuttingOperation::FilesystemMutation => {
                        let content = format!(
                            "message-{seed:x}-{}",
                            context.rng().next_u64() % 10_000
                        );
                        session
                            .filesystem()
                            .write_file(
                                "/workspace/message.txt",
                                content.clone().into_bytes(),
                                CreateOptions {
                                    create_parents: true,
                                    overwrite: true,
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("mutate workspace message");
                        shadow
                            .visible_files
                            .insert("/workspace/message.txt".to_string(), Some(content));
                    }
                    CrossCuttingOperation::GuestExecution => {
                        let result = session
                            .exec_module("/workspace/main.js")
                            .await
                            .expect("execute guest entrypoint");
                        let message = shadow
                            .visible_files
                            .get("/workspace/message.txt")
                            .and_then(|value| value.clone())
                            .expect("message should be present");
                        let rendered = format!("{message}:{}", message.len());
                        assert_eq!(
                            result.result,
                            Some(serde_json::json!({
                                "specifier": "terrace:host/tickets",
                                "method": "echo",
                                "args": [{ "value": rendered.clone() }],
                            }))
                        );
                        shadow.visible_files.insert(
                            "/workspace/runtime.txt".to_string(),
                            Some(rendered.clone()),
                        );
                        shadow.capability_values.push(rendered);
                        shadow
                            .required_runtime_cache_specifiers
                            .insert("terrace:/workspace/main.js".to_string());
                    }
                    CrossCuttingOperation::TypeCheck => {
                        let report = typescript
                            .check(
                                &session,
                                TypeCheckRequest {
                                    roots: vec!["/workspace/src/bad.ts".to_string()],
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("run deterministic typecheck");
                        assert_eq!(report.diagnostics.len(), 1);
                        shadow.typescript_roots = vec!["/workspace/src/bad.ts".to_string()];
                        shadow.typescript_file_count = 1;
                    }
                    CrossCuttingOperation::BashWrite => {
                        let note = format!("note-{seed:x}-{}", context.rng().next_u64() % 10_000);
                        let report = bash
                            .run(
                                &session,
                                BashRequest {
                                    command: format!(
                                        "mkdir -p scratch && cd scratch && printf {note} > note.txt && pwd"
                                    ),
                                    cwd: "/workspace".to_string(),
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("run deterministic bash workload");
                        assert_eq!(report, BashReport {
                            exit_code: 0,
                            stdout: "/workspace/scratch\n".to_string(),
                            stderr: String::new(),
                            cwd: "/workspace/scratch".to_string(),
                            env: BTreeMap::new(),
                        });
                        shadow.visible_files.insert(
                            "/workspace/scratch/note.txt".to_string(),
                            Some(note),
                        );
                        shadow.bash_cwd = Some("/workspace/scratch".to_string());
                        shadow.bash_history_len += 1;
                    }
                    CrossCuttingOperation::InstallPackages => {
                        let report = session
                            .install_packages(PackageInstallRequest {
                                packages: vec!["zod".to_string(), "lodash".to_string()],
                                materialize_compatibility_view: true,
                            })
                            .await
                            .expect("install packages during cross-cutting workload");
                        assert_eq!(report.packages, vec!["lodash", "zod"]);
                        shadow.package_names.insert("lodash".to_string());
                        shadow.package_names.insert("zod".to_string());
                        shadow.package_materialized = true;
                    }
                    CrossCuttingOperation::ViewProbe => {
                        let visible = bridge
                            .send(ReadonlyViewProtocolRequest::OpenView {
                                session_volume_id,
                                request: ReadonlyViewRequest {
                                    cut: ReadonlyViewCut::Visible,
                                    path: "/workspace/message.txt".to_string(),
                                    label: Some("message visible".to_string()),
                                },
                            })
                            .await
                            .expect("open visible readonly view");
                        let ReadonlyViewProtocolResponse::View {
                            handle: visible_handle,
                        } = visible
                        else {
                            panic!("expected view response");
                        };
                        let durable = bridge
                            .send(ReadonlyViewProtocolRequest::OpenView {
                                session_volume_id,
                                request: ReadonlyViewRequest {
                                    cut: ReadonlyViewCut::Durable,
                                    path: "/workspace/message.txt".to_string(),
                                    label: Some("message durable".to_string()),
                                },
                            })
                            .await
                            .expect("open durable readonly view");
                        let ReadonlyViewProtocolResponse::View {
                            handle: durable_handle,
                        } = durable
                        else {
                            panic!("expected view response");
                        };

                        let visible_file = bridge
                            .send(ReadonlyViewProtocolRequest::ReadFile {
                                location: visible_handle.location.clone(),
                            })
                            .await
                            .expect("read visible readonly file");
                        let ReadonlyViewProtocolResponse::File {
                            bytes: visible_bytes,
                        } = visible_file
                        else {
                            panic!("expected file response");
                        };
                        let durable_file = bridge
                            .send(ReadonlyViewProtocolRequest::ReadFile {
                                location: durable_handle.location.clone(),
                            })
                            .await
                            .expect("read durable readonly file");
                        let ReadonlyViewProtocolResponse::File {
                            bytes: durable_bytes,
                        } = durable_file
                        else {
                            panic!("expected file response");
                        };

                        let expected_visible = shadow
                            .visible_files
                            .get("/workspace/message.txt")
                            .and_then(|value| value.clone())
                            .map(|value| value.into_bytes());
                        let expected_durable = shadow
                            .durable_files
                            .get("/workspace/message.txt")
                            .and_then(|value| value.clone())
                            .map(|value| value.into_bytes());
                        assert_eq!(visible_bytes, expected_visible);
                        assert_eq!(durable_bytes, expected_durable);
                        shadow.active_view_handles.insert(visible_handle.handle_id);
                        shadow.active_view_handles.insert(durable_handle.handle_id);
                    }
                    CrossCuttingOperation::UpdateProvenance => {
                        let branch = format!(
                            "branch-{seed:x}-{}",
                            context.rng().next_u64() % 10_000
                        );
                        session
                            .update_provenance({
                                let branch = branch.clone();
                                move |provenance| {
                                    provenance.git = Some(GitProvenance {
                                        repo_root: "/repo".to_string(),
                                        head_commit: Some(format!("{seed:x}")),
                                        branch: Some(branch),
                                        remote_url: Some(
                                            "git@example.invalid:terrace/sandbox.git".to_string(),
                                        ),
                                        pathspec: vec![".".to_string()],
                                        dirty: false,
                                    });
                                }
                            })
                            .await
                            .expect("update sandbox provenance");
                        shadow.branch = Some(branch);
                    }
                }

                let capture = capture_state(&session)
                    .await
                    .expect("capture cross-cutting step state");
                assert_capture_matches_shadow(seed, &format!("{operation:?}"), &capture, &shadow);
                steps.push(CrossCuttingStepCapture { operation, capture });
            }

            session.flush().await.expect("flush cross-cutting session");
            shadow.flush();
            let flushed = capture_state(&session)
                .await
                .expect("capture flushed cross-cutting state");
            assert_capture_matches_shadow(seed, "flush", &flushed, &shadow);

            session
                .close(terracedb_sandbox::CloseSessionOptions::default())
                .await
                .expect("close cross-cutting session");
            let reopened = sandbox
                .reopen_session(ReopenSessionOptions {
                    session_volume_id,
                    session_chunk_size: Some(4096),
                })
                .await
                .expect("reopen cross-cutting session");
            let reopened_capture = capture_state(&reopened)
                .await
                .expect("capture reopened cross-cutting state");
            assert_capture_matches_shadow(seed, "reopen", &reopened_capture, &shadow);

            Ok(CrossCuttingSimulationTrace {
                steps,
                flushed,
                reopened: reopened_capture,
            })
        })
}

#[test]
fn seeded_cross_cutting_campaign_replays_shadow_state_and_tool_ordering() -> turmoil::Result {
    for seed in [0x4101, 0x4102, 0x4103, 0x4104] {
        let first = run_cross_cutting_simulation(seed)?;
        let second = run_cross_cutting_simulation(seed)?;
        assert_eq!(first, second, "seed {seed:#x} should replay identically");
        assert!(
            first
                .reopened
                .tool_names
                .contains(&"sandbox.package.install".to_string()),
            "seed {seed:#x} should include package install coverage"
        );
        assert!(
            first
                .reopened
                .tool_names
                .contains(&"sandbox.typescript.check".to_string()),
            "seed {seed:#x} should include TypeScript coverage"
        );
        assert!(
            first
                .reopened
                .tool_names
                .contains(&"sandbox.bash.exec".to_string()),
            "seed {seed:#x} should include bash coverage"
        );
        assert!(
            first
                .reopened
                .tool_names
                .contains(&"sandbox.view.open".to_string()),
            "seed {seed:#x} should include readonly view coverage"
        );
        assert!(
            first
                .reopened
                .tool_names
                .contains(&"sandbox.runtime.exec_module".to_string()),
            "seed {seed:#x} should include runtime execution coverage"
        );
        assert!(
            first
                .reopened
                .tool_names
                .contains(&"host_api.tickets.echo".to_string()),
            "seed {seed:#x} should include capability coverage"
        );
    }
    Ok(())
}

fn deterministic_temp_dir(label: &str, seed: u64) -> PathBuf {
    let unique = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    loop {
        let temp_id = Uuid::new_v4();
        let path = std::env::temp_dir().join(format!(
            "terracedb-sandbox-{label}-{seed:x}-{unique}-{temp_id}"
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("create temp dir {path:?}: {error}"),
        }
    }
}

fn cleanup(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn write_host_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create host parent");
    }
    fs::write(path, contents).expect("write host file");
}

fn read_host_file(path: &Path) -> String {
    fs::read_to_string(path).expect("read host file")
}

#[test]
fn deterministic_temp_dir_allocates_unique_paths() {
    let first = deterministic_temp_dir("cross-cutting", 0x5301);
    let second = deterministic_temp_dir("cross-cutting", 0x5301);
    assert_ne!(first, second);
    assert!(first.starts_with(std::env::temp_dir()));
    assert!(second.starts_with(std::env::temp_dir()));
    assert!(first.is_dir());
    assert!(second.is_dir());
}

fn sanitized_git_command() -> Command {
    let mut command = Command::new("git");
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_PREFIX",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ] {
        command.env_remove(key);
    }
    command
}

fn git(dir: &Path, args: &[&str]) {
    let output = sanitized_git_command()
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("run git command");
    assert!(
        output.status.success(),
        "git -C {} {} failed: {}",
        dir.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_out(dir: &Path, args: &[&str]) -> String {
    let output = sanitized_git_command()
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("run git command");
    assert!(
        output.status.success(),
        "git -C {} {} failed: {}",
        dir.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn init_git_repo(repo: &Path, remote: Option<&Path>, seed: u64) {
    cleanup(repo);
    if let Some(remote) = remote {
        cleanup(remote);
    }
    fs::create_dir_all(repo).expect("create repo dir");
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.name", "Sandbox Tester"]);
    git(repo, &["config", "user.email", "sandbox@example.invalid"]);
    write_host_file(&repo.join("tracked.txt"), &format!("tracked-{seed:x}\n"));
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "initial"]);
    if let Some(remote) = remote {
        let output = sanitized_git_command()
            .arg("init")
            .arg("--bare")
            .arg(remote)
            .output()
            .expect("init bare remote");
        assert!(
            output.status.success(),
            "git init --bare failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        git(
            repo,
            &["remote", "add", "origin", &remote.to_string_lossy()],
        );
        git(repo, &["push", "-u", "origin", "main"]);
    }
}

fn host_git_services() -> SandboxServices {
    SandboxServices::new(
        Arc::new(DeterministicRuntimeBackend::default()),
        Arc::new(DeterministicPackageInstaller::default()),
        Arc::new(HostGitWorkspaceManager::default()),
        Arc::new(DeterministicPullRequestProviderClient::default()),
        Arc::new(DeterministicReadonlyViewProvider::default()),
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HostDiskWorkflowCapture {
    view_bytes: Vec<u8>,
    target_tracked: String,
    target_new: String,
    exported_workspace_tracked: String,
    pushed: bool,
    active_view_handles: Vec<String>,
    tool_names: Vec<String>,
    activity_tool_subjects: Vec<String>,
}

async fn create_empty_base(
    store: &InMemoryVfsStore,
    volume_id: VolumeId,
) -> Result<(), terracedb_sandbox::SandboxError> {
    store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await?;
    Ok(())
}

async fn run_host_disk_workflow(seed: u64) -> HostDiskWorkflowCapture {
    let repo = deterministic_temp_dir("host-workflow-repo", seed);
    let remote = deterministic_temp_dir("host-workflow-remote", seed);
    let target = deterministic_temp_dir("host-workflow-target", seed);
    init_git_repo(&repo, Some(&remote), seed);
    cleanup(&target);
    fs::create_dir_all(&target).expect("create target dir");
    write_host_file(&target.join("tracked.txt"), &format!("tracked-{seed:x}\n"));

    let (vfs, sandbox) = sandbox_store(200 + seed, seed, host_git_services());
    let base_volume_id = VolumeId::new(0x9800 + seed as u128);
    let session_volume_id = VolumeId::new(0x9900 + seed as u128);
    create_empty_base(&vfs, base_volume_id)
        .await
        .expect("create host workflow base");

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open host workflow session");
    session
        .hoist_from_disk(HoistRequest {
            source_path: repo.to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::GitHead,
            delete_missing: true,
        })
        .await
        .expect("hoist git repository");
    session
        .filesystem()
        .write_file(
            "/workspace/tracked.txt",
            format!("workflow-{seed:x}\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("update tracked file");
    session
        .filesystem()
        .write_file(
            "/workspace/new.txt",
            format!("new-{seed:x}\n").into_bytes(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write new file");

    let registry = Arc::new(StaticReadonlyViewRegistry::new([session.clone()]));
    let bridge = terracedb_sandbox::LocalReadonlyViewBridge::new(Arc::new(
        terracedb_sandbox::ReadonlyViewService::new(registry),
    ));
    let visible = bridge
        .send(ReadonlyViewProtocolRequest::OpenView {
            session_volume_id,
            request: ReadonlyViewRequest {
                cut: ReadonlyViewCut::Visible,
                path: "/workspace/tracked.txt".to_string(),
                label: Some("tracked".to_string()),
            },
        })
        .await
        .expect("open visible readonly view");
    let ReadonlyViewProtocolResponse::View { handle } = visible else {
        panic!("expected view response");
    };
    let bytes = bridge
        .send(ReadonlyViewProtocolRequest::ReadFile {
            location: handle.location.clone(),
        })
        .await
        .expect("read visible readonly file");
    let ReadonlyViewProtocolResponse::File { bytes } = bytes else {
        panic!("expected file response");
    };

    session
        .eject_to_disk(terracedb_sandbox::EjectRequest {
            target_path: target.to_string_lossy().into_owned(),
            mode: terracedb_sandbox::EjectMode::ApplyDelta,
            conflict_policy: ConflictPolicy::Fail,
        })
        .await
        .expect("eject deterministic delta");
    let pr = session
        .create_pull_request(PullRequestRequest {
            title: format!("Sandbox workflow {seed:x}"),
            body: "Cross-cutting export".to_string(),
            head_branch: format!("sandbox/workflow-{seed:x}"),
            base_branch: "main".to_string(),
        })
        .await
        .expect("create pull request with host git");

    let workspace = PathBuf::from(
        pr.metadata
            .get("workspace_path")
            .and_then(|value| value.as_str())
            .expect("workspace path"),
    );
    let capture = HostDiskWorkflowCapture {
        view_bytes: bytes.expect("visible bytes"),
        target_tracked: read_host_file(&target.join("tracked.txt")),
        target_new: read_host_file(&target.join("new.txt")),
        exported_workspace_tracked: read_host_file(&workspace.join("tracked.txt")),
        pushed: pr
            .metadata
            .get("pushed")
            .and_then(|value| value.as_bool())
            .expect("pushed metadata"),
        active_view_handles: session
            .active_readonly_views()
            .await
            .into_iter()
            .map(|view| view.handle_id)
            .collect(),
        tool_names: chronological_tool_names(&session)
            .await
            .expect("tool names"),
        activity_tool_subjects: activity_tool_subjects(&session)
            .await
            .expect("activity tool subjects"),
    };

    cleanup(&repo);
    cleanup(&remote);
    cleanup(&target);
    cleanup(&workspace);
    capture
}

#[tokio::test]
async fn seeded_host_disk_workflow_replays_hoist_eject_pr_and_view_contracts() {
    for seed in [0x5201_u64, 0x5202_u64] {
        let first = run_host_disk_workflow(seed).await;
        let second = run_host_disk_workflow(seed).await;
        assert_eq!(first, second, "seed {seed:#x} should replay identically");
        assert_eq!(first.target_tracked, format!("workflow-{seed:x}\n"));
        assert_eq!(first.target_new, format!("new-{seed:x}\n"));
        assert_eq!(
            first.exported_workspace_tracked,
            format!("workflow-{seed:x}\n")
        );
        assert!(first.pushed, "seed {seed:#x} should push the PR export");
        assert_eq!(
            first.view_bytes,
            format!("workflow-{seed:x}\n").into_bytes()
        );
        assert_eq!(first.tool_names, first.activity_tool_subjects);
    }
}

#[tokio::test]
async fn deterministic_git_stub_matches_host_workspace_contract_on_shared_metadata() {
    let seed = 0x5301_u64;
    let repo = deterministic_temp_dir("git-conformance-repo", seed);
    let remote = deterministic_temp_dir("git-conformance-remote", seed);
    let workspace = deterministic_temp_dir("git-conformance-workspace", seed);
    init_git_repo(&repo, Some(&remote), seed);
    cleanup(&workspace);

    let head_commit = git_out(&repo, &["rev-parse", "HEAD"]);
    let remote_url = remote.to_string_lossy().into_owned();

    let deterministic_services = SandboxServices::new(
        Arc::new(DeterministicRuntimeBackend::default()),
        Arc::new(DeterministicPackageInstaller::default()),
        Arc::new(DeterministicGitWorkspaceManager::default()),
        Arc::new(DeterministicPullRequestProviderClient::default()),
        Arc::new(DeterministicReadonlyViewProvider::default()),
    );
    let host_services = host_git_services();
    let (det_vfs, det_sandbox) = sandbox_store(300, seed, deterministic_services);
    let (host_vfs, host_sandbox) = sandbox_store(301, seed + 1, host_services);
    let base_volume_id = VolumeId::new(0xa100);
    let det_session_volume_id = VolumeId::new(0xa101);
    let host_session_volume_id = VolumeId::new(0xa102);
    create_empty_base(&det_vfs, base_volume_id)
        .await
        .expect("create deterministic base");
    create_empty_base(&host_vfs, base_volume_id)
        .await
        .expect("create host base");

    let config = |session_volume_id| SandboxConfig {
        session_volume_id,
        session_chunk_size: Some(4096),
        base_volume_id,
        durable_base: false,
        workspace_root: "/workspace".to_string(),
        package_compat: PackageCompatibilityMode::NpmPureJs,
        conflict_policy: ConflictPolicy::Fail,
        capabilities: Default::default(),
        execution_policy: None,
        hoisted_source: None,
        git_provenance: Some(GitProvenance {
            repo_root: repo.to_string_lossy().into_owned(),
            head_commit: Some(head_commit.clone()),
            branch: Some("main".to_string()),
            remote_url: Some(remote_url.clone()),
            pathspec: vec![".".to_string()],
            dirty: false,
        }),
    };

    let deterministic = det_sandbox
        .open_session(config(det_session_volume_id))
        .await
        .expect("open deterministic git session");
    let host = host_sandbox
        .open_session(config(host_session_volume_id))
        .await
        .expect("open host git session");
    let request = terracedb_sandbox::GitWorkspaceRequest {
        branch_name: "sandbox/conformance".to_string(),
        base_branch: Some("main".to_string()),
        target_path: workspace.to_string_lossy().into_owned(),
    };

    let expected = deterministic
        .prepare_git_workspace(request.clone())
        .await
        .expect("prepare deterministic workspace");
    let actual = host
        .prepare_git_workspace(request)
        .await
        .expect("prepare host workspace");

    assert_eq!(expected.branch_name, actual.branch_name);
    assert_eq!(expected.workspace_path, actual.workspace_path);
    for key in ["base_ref", "head_commit", "remote_url", "repo_root"] {
        assert_eq!(
            expected.metadata.get(key),
            actual.metadata.get(key),
            "shared git metadata key `{key}` should match"
        );
    }
    assert_eq!(
        git_out(
            Path::new(&actual.workspace_path),
            &["rev-parse", "--abbrev-ref", "HEAD"]
        ),
        "sandbox/conformance"
    );

    cleanup(&repo);
    cleanup(&remote);
    cleanup(&workspace);
}
