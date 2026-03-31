use std::time::Duration;

use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

use terracedb_sandbox::{
    BashRequest, BashService, DefaultSandboxStore, DeterministicBashService,
    DeterministicTypeScriptService, GitProvenance, PackageInstallRequest, ReadonlyViewCut,
    ReadonlyViewRequest, ReopenSessionOptions, SandboxConfig, SandboxServices, SandboxStore,
    TypeCheckRequest, TypeScriptService,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SandboxSimulationCapture {
    revision: u64,
    state: String,
    actor_id: String,
    tool_names: Vec<String>,
    active_view_handles: usize,
    package_names: Vec<String>,
    branch: String,
    typescript_diagnostics: usize,
    bash_cwd: String,
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
                    SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096),
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
            session
                .update_provenance(|provenance| {
                    provenance.git = Some(GitProvenance {
                        repo_root: "/repo".to_string(),
                        head_commit: Some(format!("{seed:x}")),
                        branch: Some(format!("branch-{seed:x}")),
                        remote_url: None,
                        pathspec: vec![".".to_string()],
                        dirty: false,
                    });
                })
                .await
                .expect("update provenance");
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
            reopened
                .close_readonly_view(&handle.handle_id)
                .await
                .expect("close reopened view");
            let info = reopened.info().await;
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
                branch: info
                    .provenance
                    .git
                    .and_then(|git| git.branch)
                    .expect("branch should be set"),
                typescript_diagnostics: diagnostics.diagnostics.len(),
                bash_cwd: bash_report.cwd,
            })
        })
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
    assert!(first.revision >= 4);
    assert_eq!(first.typescript_diagnostics, 1);
    assert_eq!(first.bash_cwd, "/workspace/scratch");
    Ok(())
}
