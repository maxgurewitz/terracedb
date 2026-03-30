use std::time::Duration;

use futures::{TryStreamExt, future::join_all};
use terracedb::{Clock, LogCursor};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{
    ActivityOptions, CompletedToolRun, CompletedToolRunOutcome, CreateOptions, InMemoryVfsStore,
    MkdirOptions, SnapshotOptions, VolumeConfig, VolumeId, VolumeStore,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct VfsSimulationCapture {
    visible_before: Vec<String>,
    durable_before: Vec<String>,
    snapshot_names: Vec<String>,
    live_names: Vec<String>,
    durable_after: Vec<String>,
    visible_activity_count: usize,
    durable_activity_count_before: usize,
    durable_activity_count_after: usize,
    tool_ids: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OverlaySimulationCapture {
    overlay_names_before_flush: Vec<String>,
    reopened_names_before_flush: Vec<String>,
    durable_names_before_flush: Vec<String>,
    imported_visible_names: Vec<String>,
    imported_durable_names_before_flush: Vec<String>,
    imported_durable_names_after_flush: Vec<String>,
    overlay_readme_before_flush: Option<Vec<u8>>,
    reopened_readme_before_flush: Option<Vec<u8>>,
    imported_visible_readme: Option<Vec<u8>>,
    imported_durable_readme_before_flush: Option<Vec<u8>>,
    imported_durable_readme_after_flush: Option<Vec<u8>>,
    guide_exists_before_flush: bool,
    imported_visible_guide_exists: bool,
    imported_durable_guide_exists_before_flush: bool,
    imported_durable_guide_exists_after_flush: bool,
}

fn sorted_names(entries: Vec<terracedb_vfs::DirEntry>) -> Vec<String> {
    let mut names = entries
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn run_vfs_simulation(seed: u64) -> turmoil::Result<VfsSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |context| async move {
            let store = InMemoryVfsStore::new(context.clock(), context.rng());
            let volume = store
                .open_volume(
                    VolumeConfig::new(VolumeId::new(0x3900 + seed as u128))
                        .with_chunk_size(4)
                        .with_create_if_missing(true),
                )
                .await
                .expect("open volume");

            volume
                .fs()
                .mkdir(
                    "/jobs",
                    MkdirOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("mkdir");

            let fs = volume.fs();
            let tools = volume.tools();
            let clock = context.clock();
            let tool_futures = (0..6).map(|index| {
                let fs = fs.clone();
                let tools = tools.clone();
                let clock = clock.clone();
                async move {
                    let delay = ((seed + index as u64) % 3) + 1;
                    clock.sleep(Duration::from_millis(delay)).await;
                    let path = format!("/jobs/task-{index}.txt");
                    fs.write_file(
                        &path,
                        format!("payload-{seed:x}-{index}").into_bytes(),
                        CreateOptions::default(),
                    )
                    .await
                    .expect("write file");
                    tools
                        .record_completed(CompletedToolRun {
                            name: format!("tool-{index}"),
                            params: None,
                            outcome: if index % 2 == 0 {
                                CompletedToolRunOutcome::Success { result: None }
                            } else {
                                CompletedToolRunOutcome::Error {
                                    message: format!("error-{index}"),
                                }
                            },
                        })
                        .await
                        .expect("record completed tool run")
                        .get()
                }
            });
            let mut tool_ids = join_all(tool_futures).await;
            tool_ids.sort();

            let visible_before = sorted_names(
                volume
                    .snapshot(SnapshotOptions::default())
                    .await
                    .expect("visible snapshot")
                    .fs()
                    .readdir("/jobs")
                    .await
                    .expect("visible readdir"),
            );
            let durable_before_snapshot = volume
                .snapshot(SnapshotOptions { durable: true })
                .await
                .expect("durable snapshot before flush");
            let durable_before = if durable_before_snapshot
                .fs()
                .stat("/jobs")
                .await
                .expect("durable stat before flush")
                .is_some()
            {
                sorted_names(
                    durable_before_snapshot
                        .fs()
                        .readdir("/jobs")
                        .await
                        .expect("durable readdir before flush"),
                )
            } else {
                Vec::new()
            };

            let snapshot = volume
                .snapshot(SnapshotOptions::default())
                .await
                .expect("create snapshot");
            volume
                .fs()
                .rename("/jobs/task-0.txt", "/jobs/renamed-0.txt")
                .await
                .expect("rename");
            volume
                .fs()
                .pwrite("/jobs/renamed-0.txt", 2, b"ZZ".to_vec())
                .await
                .expect("patch renamed file");

            let snapshot_names = sorted_names(
                snapshot
                    .fs()
                    .readdir("/jobs")
                    .await
                    .expect("snapshot readdir"),
            );
            let live_names =
                sorted_names(volume.fs().readdir("/jobs").await.expect("live readdir"));

            let visible_activity_count = volume
                .activity_since(LogCursor::beginning(), ActivityOptions::default())
                .await
                .expect("visible activity stream")
                .try_collect::<Vec<_>>()
                .await
                .expect("collect visible activities")
                .len();
            let durable_activity_count_before = volume
                .activity_since(
                    LogCursor::beginning(),
                    ActivityOptions {
                        durable: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("durable activity stream before flush")
                .try_collect::<Vec<_>>()
                .await
                .expect("collect durable activities before flush")
                .len();

            volume.flush().await.expect("flush");

            let durable_after = sorted_names(
                volume
                    .snapshot(SnapshotOptions { durable: true })
                    .await
                    .expect("durable snapshot after flush")
                    .fs()
                    .readdir("/jobs")
                    .await
                    .expect("durable readdir after flush"),
            );
            let durable_activity_count_after = volume
                .activity_since(
                    LogCursor::beginning(),
                    ActivityOptions {
                        durable: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("durable activity stream after flush")
                .try_collect::<Vec<_>>()
                .await
                .expect("collect durable activities after flush")
                .len();

            Ok(VfsSimulationCapture {
                visible_before,
                durable_before,
                snapshot_names,
                live_names,
                durable_after,
                visible_activity_count,
                durable_activity_count_before,
                durable_activity_count_after,
                tool_ids,
            })
        })
}

fn run_overlay_simulation(seed: u64) -> turmoil::Result<OverlaySimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |context| async move {
            let store = InMemoryVfsStore::new(context.clock(), context.rng());
            let base_volume_id = VolumeId::new(0x3a00 + seed as u128);
            let overlay_volume_id = VolumeId::new(0x4a00 + seed as u128);

            let base = store
                .open_volume(
                    VolumeConfig::new(base_volume_id)
                        .with_chunk_size(4)
                        .with_create_if_missing(true),
                )
                .await
                .expect("open base volume");

            base.fs()
                .mkdir(
                    "/workspace/docs",
                    MkdirOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("mkdir workspace/docs");
            base.fs()
                .write_file(
                    "/workspace/readme.txt",
                    format!("base-{seed:x}").into_bytes(),
                    CreateOptions::default(),
                )
                .await
                .expect("write base readme");
            base.fs()
                .write_file(
                    "/workspace/docs/guide.txt",
                    format!("guide-{seed:x}").into_bytes(),
                    CreateOptions::default(),
                )
                .await
                .expect("write base guide");
            base.flush().await.expect("flush base volume");

            let base_snapshot = base
                .snapshot(SnapshotOptions { durable: true })
                .await
                .expect("durable base snapshot");
            let overlay = store
                .create_overlay(
                    base_snapshot,
                    VolumeConfig::new(overlay_volume_id)
                        .with_chunk_size(4)
                        .with_create_if_missing(true),
                )
                .await
                .expect("create overlay");

            let clock = context.clock();
            clock.sleep(Duration::from_millis((seed % 3) + 1)).await;

            overlay
                .fs()
                .write_file(
                    "/workspace/readme.txt",
                    format!("overlay-{seed:x}").into_bytes(),
                    CreateOptions::default(),
                )
                .await
                .expect("copy-up and overwrite readme");
            overlay
                .fs()
                .write_file(
                    "/workspace/new.txt",
                    format!("new-{seed:x}").into_bytes(),
                    CreateOptions::default(),
                )
                .await
                .expect("create overlay file");
            overlay
                .fs()
                .unlink("/workspace/docs/guide.txt")
                .await
                .expect("whiteout base file");

            let overlay_names_before_flush = sorted_names(
                overlay
                    .fs()
                    .readdir("/workspace")
                    .await
                    .expect("overlay readdir before flush"),
            );
            let overlay_readme_before_flush = overlay
                .fs()
                .read_file("/workspace/readme.txt")
                .await
                .expect("overlay read readme before flush");
            let guide_exists_before_flush = overlay
                .fs()
                .read_file("/workspace/docs/guide.txt")
                .await
                .expect("overlay read guide before flush")
                .is_some();

            let reopened = store
                .open_volume(VolumeConfig::new(overlay_volume_id).with_chunk_size(4))
                .await
                .expect("reopen overlay");
            let reopened_names_before_flush = sorted_names(
                reopened
                    .fs()
                    .readdir("/workspace")
                    .await
                    .expect("reopened overlay readdir before flush"),
            );
            let reopened_readme_before_flush = reopened
                .fs()
                .read_file("/workspace/readme.txt")
                .await
                .expect("reopened overlay readme before flush");

            let durable_names_before_flush = sorted_names(
                overlay
                    .snapshot(SnapshotOptions { durable: true })
                    .await
                    .expect("durable overlay snapshot before flush")
                    .fs()
                    .readdir("/workspace")
                    .await
                    .expect("durable overlay readdir before flush"),
            );

            let visible_export = store
                .export_volume(terracedb_vfs::CloneVolumeSource::new(overlay_volume_id))
                .await
                .expect("export visible overlay");
            let imported_visible_store = InMemoryVfsStore::new(context.clock(), context.rng());
            let imported_visible = imported_visible_store
                .import_volume(
                    visible_export,
                    VolumeConfig::new(VolumeId::new(0x5a00 + seed as u128))
                        .with_chunk_size(4)
                        .with_create_if_missing(true),
                )
                .await
                .expect("import visible overlay");
            let imported_visible_names = sorted_names(
                imported_visible
                    .fs()
                    .readdir("/workspace")
                    .await
                    .expect("imported visible workspace"),
            );
            let imported_visible_readme = imported_visible
                .fs()
                .read_file("/workspace/readme.txt")
                .await
                .expect("imported visible readme");
            let imported_visible_guide_exists = imported_visible
                .fs()
                .read_file("/workspace/docs/guide.txt")
                .await
                .expect("imported visible guide")
                .is_some();

            let durable_export_before_flush = store
                .export_volume(
                    terracedb_vfs::CloneVolumeSource::new(overlay_volume_id).durable(true),
                )
                .await
                .expect("export durable overlay before flush");
            let imported_durable_before_store =
                InMemoryVfsStore::new(context.clock(), context.rng());
            let imported_durable_before = imported_durable_before_store
                .import_volume(
                    durable_export_before_flush,
                    VolumeConfig::new(VolumeId::new(0x6a00 + seed as u128))
                        .with_chunk_size(4)
                        .with_create_if_missing(true),
                )
                .await
                .expect("import durable overlay before flush");
            let imported_durable_names_before_flush = sorted_names(
                imported_durable_before
                    .fs()
                    .readdir("/workspace")
                    .await
                    .expect("imported durable workspace before flush"),
            );
            let imported_durable_readme_before_flush = imported_durable_before
                .fs()
                .read_file("/workspace/readme.txt")
                .await
                .expect("imported durable readme before flush");
            let imported_durable_guide_exists_before_flush = imported_durable_before
                .fs()
                .read_file("/workspace/docs/guide.txt")
                .await
                .expect("imported durable guide before flush")
                .is_some();

            overlay.flush().await.expect("flush overlay");

            let durable_export_after_flush = store
                .export_volume(
                    terracedb_vfs::CloneVolumeSource::new(overlay_volume_id).durable(true),
                )
                .await
                .expect("export durable overlay after flush");
            let imported_durable_after_store =
                InMemoryVfsStore::new(context.clock(), context.rng());
            let imported_durable_after = imported_durable_after_store
                .import_volume(
                    durable_export_after_flush,
                    VolumeConfig::new(VolumeId::new(0x7a00 + seed as u128))
                        .with_chunk_size(4)
                        .with_create_if_missing(true),
                )
                .await
                .expect("import durable overlay after flush");
            let imported_durable_names_after_flush = sorted_names(
                imported_durable_after
                    .fs()
                    .readdir("/workspace")
                    .await
                    .expect("imported durable workspace after flush"),
            );
            let imported_durable_readme_after_flush = imported_durable_after
                .fs()
                .read_file("/workspace/readme.txt")
                .await
                .expect("imported durable readme after flush");
            let imported_durable_guide_exists_after_flush = imported_durable_after
                .fs()
                .read_file("/workspace/docs/guide.txt")
                .await
                .expect("imported durable guide after flush")
                .is_some();

            Ok(OverlaySimulationCapture {
                overlay_names_before_flush,
                reopened_names_before_flush,
                durable_names_before_flush,
                imported_visible_names,
                imported_durable_names_before_flush,
                imported_durable_names_after_flush,
                overlay_readme_before_flush,
                reopened_readme_before_flush,
                imported_visible_readme,
                imported_durable_readme_before_flush,
                imported_durable_readme_after_flush,
                guide_exists_before_flush,
                imported_visible_guide_exists,
                imported_durable_guide_exists_before_flush,
                imported_durable_guide_exists_after_flush,
            })
        })
}

#[test]
fn vfs_seeded_simulation_replays_same_seed_and_respects_durable_cut() -> turmoil::Result {
    let first = run_vfs_simulation(0x3535)?;
    let second = run_vfs_simulation(0x3535)?;

    assert_eq!(first, second);
    assert_eq!(first.visible_before.len(), 6);
    assert!(first.durable_before.is_empty());
    assert_eq!(first.snapshot_names, first.visible_before);
    assert_eq!(first.durable_after, first.live_names);
    assert!(
        first.live_names.iter().any(|name| name == "renamed-0.txt"),
        "live view should include the renamed entry after the concurrent mutation"
    );
    assert!(
        first.durable_activity_count_before < first.visible_activity_count,
        "durable activity should lag visible activity until flush"
    );
    assert_eq!(
        first.durable_activity_count_after,
        first.visible_activity_count
    );
    assert_eq!(first.tool_ids, vec![1, 2, 3, 4, 5, 6]);

    Ok(())
}

#[test]
fn overlay_seeded_simulation_replays_whiteouts_copy_up_and_durable_exports() -> turmoil::Result {
    let first = run_overlay_simulation(0x4545)?;
    let second = run_overlay_simulation(0x4545)?;

    assert_eq!(first, second);
    assert_eq!(
        first.overlay_names_before_flush,
        vec![
            "docs".to_string(),
            "new.txt".to_string(),
            "readme.txt".to_string()
        ]
    );
    assert_eq!(
        first.reopened_names_before_flush,
        first.overlay_names_before_flush
    );
    assert_eq!(
        first.durable_names_before_flush,
        vec!["docs".to_string(), "readme.txt".to_string()]
    );
    assert_eq!(
        first.imported_visible_names,
        first.overlay_names_before_flush
    );
    assert_eq!(
        first.imported_durable_names_before_flush,
        first.durable_names_before_flush
    );
    assert_eq!(
        first.imported_durable_names_after_flush,
        first.overlay_names_before_flush
    );
    assert_eq!(
        first.overlay_readme_before_flush,
        first.reopened_readme_before_flush
    );
    assert_eq!(
        first.imported_visible_readme,
        first.overlay_readme_before_flush
    );
    assert_ne!(
        first.imported_durable_readme_before_flush,
        first.overlay_readme_before_flush
    );
    assert_eq!(
        first.imported_durable_readme_after_flush,
        first.overlay_readme_before_flush
    );
    assert!(!first.guide_exists_before_flush);
    assert!(!first.imported_visible_guide_exists);
    assert!(first.imported_durable_guide_exists_before_flush);
    assert!(!first.imported_durable_guide_exists_after_flush);

    Ok(())
}
