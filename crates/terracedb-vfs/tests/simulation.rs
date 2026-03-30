use std::time::Duration;

use futures::{TryStreamExt, future::join_all};
use terracedb::{Clock, LogCursor};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{
    ActivityOptions, AgentFsConfig, AgentFsStore, CompletedToolRun, CompletedToolRunOutcome,
    CreateOptions, InMemoryAgentFsStore, MkdirOptions, SnapshotOptions, VolumeId,
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
            let store = InMemoryAgentFsStore::new(context.clock(), context.rng());
            let volume = store
                .open_volume(
                    AgentFsConfig::new(VolumeId::new(0x3900 + seed as u128))
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
