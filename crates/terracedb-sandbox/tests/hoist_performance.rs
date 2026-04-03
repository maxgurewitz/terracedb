#[path = "support/tracing.rs"]
mod tracing_support;

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use terracedb::{DbDependencies, StubFileSystem, StubObjectStore};
use terracedb_sandbox::{
    DefaultSandboxStore, HoistMode, HoistRequest, SandboxConfig, SandboxServices, SandboxStore,
};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{
    InMemoryVfsStore, VfsPerformanceSnapshot, VolumeConfig, VolumeId, VolumeStore,
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
struct HoistPerformanceBudget {
    max_overlay_visible_snapshot_builds: u64,
    max_unbatched_paths: u64,
    min_path_inserts: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HoistPerformanceCapture {
    hoisted_paths: usize,
    deleted_paths: usize,
    delta: VfsPerformanceSnapshot,
}

fn seed_host_tree(seed: u64, files_per_dir: usize) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "terracedb-hoist-performance-{seed}-{}",
        Uuid::new_v4()
    ));
    fs::create_dir_all(&root).expect("create host tree root");
    fs::write(
        root.join("package.json"),
        format!("{{\"name\":\"perf-{seed}\",\"version\":\"1.0.0\",\"private\":true}}\n"),
    )
    .expect("write root package.json");

    for dir_index in 0..4usize {
        let dir = root.join(format!("packages/pkg-{dir_index}"));
        fs::create_dir_all(&dir).expect("create package dir");
        fs::write(
            dir.join("package.json"),
            format!(
                "{{\"name\":\"pkg-{dir_index}\",\"version\":\"1.0.0\",\"main\":\"index.js\"}}\n"
            ),
        )
        .expect("write package manifest");
        for file_index in 0..files_per_dir {
            fs::write(
                dir.join(format!("file-{file_index}.js")),
                format!("module.exports = 'seed-{seed}-pkg-{dir_index}-file-{file_index}';\n"),
            )
            .expect("write package file");
        }
    }

    root
}

fn remove_host_tree(root: &Path) {
    if let Err(error) = fs::remove_dir_all(root) {
        eprintln!("failed to clean up host tree {}: {error}", root.display());
    }
}

fn assert_hoist_budget(capture: &HoistPerformanceCapture, budget: HoistPerformanceBudget) {
    assert!(
        capture.delta.overlay_visible_snapshot_builds <= budget.max_overlay_visible_snapshot_builds,
        "hoist rebuilt merged overlay views too often: capture={capture:?}, budget={budget:?}"
    );
    assert!(
        capture.hoisted_paths as u64
            <= capture.delta.batch_operations_applied + budget.max_unbatched_paths,
        "hoist should apply batched operations instead of per-file writes: capture={capture:?}, budget={budget:?}"
    );
    assert!(
        capture.delta.path_inserts >= budget.min_path_inserts,
        "hoist should materialize inserted paths through indexed writes: capture={capture:?}, budget={budget:?}"
    );
}

fn run_hoist_performance_simulation(
    seed: u64,
    files_per_dir: usize,
) -> turmoil::Result<HoistPerformanceCapture> {
    let host_root = seed_host_tree(seed, files_per_dir);
    let source_path = host_root.to_string_lossy().to_string();
    let result = SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |context| {
            let source_path = source_path.clone();
            async move {
                let dependencies = DbDependencies::new(
                    Arc::new(StubFileSystem::default()),
                    Arc::new(StubObjectStore::default()),
                    context.clock(),
                    context.rng(),
                );
                let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
                let sandbox = DefaultSandboxStore::new(
                    Arc::new(vfs.clone()),
                    dependencies.clock,
                    SandboxServices::deterministic(),
                );
                let base_volume_id = VolumeId::new(0xC100 + seed as u128);
                let session_volume_id = VolumeId::new(0xC200 + seed as u128);

                vfs.open_volume(
                    VolumeConfig::new(base_volume_id)
                        .with_chunk_size(4096)
                        .with_create_if_missing(true),
                )
                .await
                .expect("open base volume");

                let session = sandbox
                    .open_session(
                        SandboxConfig::new(base_volume_id, session_volume_id)
                            .with_chunk_size(4096)
                            .with_workspace_root("/workspace"),
                    )
                    .await
                    .expect("open sandbox session");

                let before = vfs
                    .performance_snapshot(session_volume_id)
                    .expect("session performance snapshot before hoist");

                let report = session
                    .hoist_from_disk(HoistRequest {
                        source_path,
                        target_root: "/workspace/seeded-hoist".to_string(),
                        mode: HoistMode::DirectorySnapshot,
                        delete_missing: true,
                    })
                    .await
                    .expect("hoist host tree");

                let after = vfs
                    .performance_snapshot(session_volume_id)
                    .expect("session performance snapshot after hoist");

                Ok(HoistPerformanceCapture {
                    hoisted_paths: report.hoisted_paths,
                    deleted_paths: report.deleted_paths,
                    delta: after.delta_since(&before),
                })
            }
        });
    remove_host_tree(&host_root);
    result
}

#[test]
fn seeded_hoist_simulation_stays_within_budget() -> turmoil::Result<()> {
    tracing_support::init_tracing();

    for seed in [31u64, 57, 91] {
        let capture = run_hoist_performance_simulation(seed, 192)?;
        let budget = HoistPerformanceBudget {
            max_overlay_visible_snapshot_builds: 8,
            max_unbatched_paths: 16,
            min_path_inserts: capture.hoisted_paths as u64,
        };
        assert_hoist_budget(&capture, budget);
    }

    Ok(())
}
