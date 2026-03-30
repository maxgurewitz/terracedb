use std::sync::Arc;

use serde_json::json;

use terracedb::{StubClock, StubRng, Timestamp};
use terracedb_vfs::{
    CompletedToolRun, CompletedToolRunOutcome, CreateOptions, InMemoryVfsStore, MkdirOptions,
    SnapshotOptions, ToolRunStatus, VolumeConfig, VolumeId, VolumeStore,
};

fn test_store(seed: u64, now: u64) -> InMemoryVfsStore {
    InMemoryVfsStore::new(
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    )
}

fn sorted_names(entries: Vec<terracedb_vfs::DirEntry>) -> Vec<String> {
    let mut names = entries
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[tokio::test]
async fn overlays_merge_base_entries_support_whiteouts_and_reopen_correctly() {
    let store = test_store(17, 10);
    let base = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x4100))
                .with_chunk_size(8)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");

    let fs = base.fs();
    fs.mkdir(
        "/workspace/docs",
        MkdirOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .expect("mkdir workspace/docs");
    fs.write_file(
        "/workspace/readme.txt",
        b"base".to_vec(),
        CreateOptions::default(),
    )
    .await
    .expect("write base readme");
    fs.write_file(
        "/workspace/docs/guide.txt",
        b"guide".to_vec(),
        CreateOptions::default(),
    )
    .await
    .expect("write base guide");

    let base_snapshot = base
        .snapshot(SnapshotOptions::default())
        .await
        .expect("base snapshot");
    let overlay_id = VolumeId::new(0x4101);
    let overlay = store
        .create_overlay(
            base_snapshot.clone(),
            VolumeConfig::new(overlay_id)
                .with_chunk_size(8)
                .with_create_if_missing(true),
        )
        .await
        .expect("create overlay");

    assert_eq!(
        sorted_names(
            overlay
                .fs()
                .readdir("/workspace")
                .await
                .expect("overlay readdir base"),
        ),
        vec!["docs".to_string(), "readme.txt".to_string()]
    );

    overlay
        .fs()
        .write_file(
            "/workspace/readme.txt",
            b"overlay".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("copy-up and overwrite base file");
    overlay
        .fs()
        .write_file(
            "/workspace/new.txt",
            b"new".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("create overlay-only file");
    overlay
        .fs()
        .unlink("/workspace/docs/guide.txt")
        .await
        .expect("whiteout base file");

    assert_eq!(
        overlay
            .fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("overlay read updated readme"),
        Some(b"overlay".to_vec())
    );
    assert_eq!(
        base.fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("base read unchanged readme"),
        Some(b"base".to_vec())
    );
    assert_eq!(
        overlay
            .fs()
            .read_file("/workspace/docs/guide.txt")
            .await
            .expect("overlay read whiteouted file"),
        None
    );
    assert_eq!(
        base.fs()
            .read_file("/workspace/docs/guide.txt")
            .await
            .expect("base read guide"),
        Some(b"guide".to_vec())
    );
    assert_eq!(
        sorted_names(
            overlay
                .fs()
                .readdir("/workspace")
                .await
                .expect("overlay merged readdir"),
        ),
        vec![
            "docs".to_string(),
            "new.txt".to_string(),
            "readme.txt".to_string()
        ]
    );
    assert!(
        overlay
            .fs()
            .readdir("/workspace/docs")
            .await
            .expect("overlay docs readdir after whiteout")
            .is_empty()
    );

    overlay
        .fs()
        .write_file(
            "/workspace/docs/guide.txt",
            b"replacement".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("recreate whiteouted path");
    assert_eq!(
        overlay
            .fs()
            .read_file("/workspace/docs/guide.txt")
            .await
            .expect("overlay read recreated file"),
        Some(b"replacement".to_vec())
    );
    assert_eq!(
        sorted_names(
            overlay
                .fs()
                .readdir("/workspace/docs")
                .await
                .expect("overlay docs readdir after recreate"),
        ),
        vec!["guide.txt".to_string()]
    );

    let reopened = store
        .open_volume(VolumeConfig::new(overlay_id).with_chunk_size(8))
        .await
        .expect("reopen overlay through store");
    assert!(reopened.info().overlay_base.is_some());
    assert_eq!(
        reopened
            .fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("read reopened overlay copy-up"),
        Some(b"overlay".to_vec())
    );
    assert_eq!(
        reopened
            .fs()
            .read_file("/workspace/docs/guide.txt")
            .await
            .expect("read reopened overlay recreate"),
        Some(b"replacement".to_vec())
    );
    assert_eq!(
        sorted_names(
            reopened
                .fs()
                .readdir("/workspace")
                .await
                .expect("reopened overlay merged readdir"),
        ),
        vec![
            "docs".to_string(),
            "new.txt".to_string(),
            "readme.txt".to_string()
        ]
    );

    let exported_overlay = store
        .export_volume(terracedb_vfs::CloneVolumeSource::new(overlay_id))
        .await
        .expect("export overlay");
    let imported_store = test_store(18, 20);
    let imported = imported_store
        .import_volume(
            exported_overlay,
            VolumeConfig::new(VolumeId::new(0x4102))
                .with_chunk_size(8)
                .with_create_if_missing(true),
        )
        .await
        .expect("import exported overlay");
    assert_eq!(
        imported
            .fs()
            .read_file("/workspace/readme.txt")
            .await
            .expect("imported readme"),
        Some(b"overlay".to_vec())
    );
    assert_eq!(
        imported
            .fs()
            .read_file("/workspace/docs/guide.txt")
            .await
            .expect("imported guide"),
        Some(b"replacement".to_vec())
    );
    assert_eq!(
        sorted_names(
            imported
                .fs()
                .readdir("/workspace")
                .await
                .expect("imported workspace readdir"),
        ),
        vec![
            "docs".to_string(),
            "new.txt".to_string(),
            "readme.txt".to_string()
        ]
    );
}

#[tokio::test]
async fn export_import_round_trips_visible_state_and_durable_exports_skip_unflushed_rows() {
    let source_store = test_store(23, 100);
    let source = source_store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x4200))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("open source volume");

    source
        .fs()
        .mkdir(
            "/workspace",
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("mkdir workspace");
    source
        .fs()
        .write_file(
            "/workspace/visible.txt",
            b"visible".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write visible file");
    source
        .kv()
        .set_json("mode", json!("draft"))
        .await
        .expect("set visible kv");
    let tool_run_id = source
        .tools()
        .record_completed(CompletedToolRun {
            name: "build".to_string(),
            params: Some(json!({ "target": "docs" })),
            outcome: CompletedToolRunOutcome::Success {
                result: Some(json!({ "ok": true })),
            },
        })
        .await
        .expect("record visible tool run");

    let visible_export = source_store
        .export_volume(terracedb_vfs::CloneVolumeSource::new(
            source.info().volume_id,
        ))
        .await
        .expect("export visible cut");
    let visible_store = test_store(24, 110);
    let visible_import = visible_store
        .import_volume(
            visible_export,
            VolumeConfig::new(VolumeId::new(0x4201))
                .with_chunk_size(8)
                .with_create_if_missing(true),
        )
        .await
        .expect("import visible cut");

    assert_eq!(
        visible_import
            .fs()
            .read_file("/workspace/visible.txt")
            .await
            .expect("imported visible file"),
        Some(b"visible".to_vec())
    );
    assert_eq!(
        visible_import
            .kv()
            .get_json("mode")
            .await
            .expect("imported visible kv"),
        Some(json!("draft"))
    );
    let imported_run = visible_import
        .tools()
        .get(tool_run_id)
        .await
        .expect("imported visible tool run")
        .expect("tool run should exist");
    assert_eq!(imported_run.status, ToolRunStatus::Success);
    assert_eq!(imported_run.result, Some(json!({ "ok": true })));

    source
        .fs()
        .write_file(
            "/workspace/pending.txt",
            b"pending".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write unflushed file");
    source
        .kv()
        .set_json("pending", json!(true))
        .await
        .expect("set unflushed kv");

    let durable_export = source_store
        .export_volume(terracedb_vfs::CloneVolumeSource::new(source.info().volume_id).durable(true))
        .await
        .expect("export durable cut before flush");
    let durable_store = test_store(25, 120);
    let durable_import = durable_store
        .import_volume(
            durable_export,
            VolumeConfig::new(VolumeId::new(0x4202))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut before flush");

    assert_eq!(
        durable_import
            .fs()
            .read_file("/workspace/visible.txt")
            .await
            .expect("durable import visible file"),
        None
    );
    assert_eq!(
        durable_import
            .fs()
            .read_file("/workspace/pending.txt")
            .await
            .expect("durable import pending file"),
        None
    );
    assert_eq!(
        durable_import
            .kv()
            .get_json("mode")
            .await
            .expect("durable import visible kv"),
        None
    );

    source.flush().await.expect("flush source volume");

    let flushed_export = source_store
        .export_volume(terracedb_vfs::CloneVolumeSource::new(source.info().volume_id).durable(true))
        .await
        .expect("export durable cut after flush");
    let flushed_store = test_store(26, 130);
    let flushed_import = flushed_store
        .import_volume(
            flushed_export,
            VolumeConfig::new(VolumeId::new(0x4203))
                .with_chunk_size(8)
                .with_create_if_missing(true),
        )
        .await
        .expect("import durable cut after flush");

    assert_eq!(
        flushed_import
            .fs()
            .read_file("/workspace/visible.txt")
            .await
            .expect("flushed import visible file"),
        Some(b"visible".to_vec())
    );
    assert_eq!(
        flushed_import
            .fs()
            .read_file("/workspace/pending.txt")
            .await
            .expect("flushed import pending file"),
        Some(b"pending".to_vec())
    );
    assert_eq!(
        flushed_import
            .kv()
            .get_json("mode")
            .await
            .expect("flushed import visible kv"),
        Some(json!("draft"))
    );
    assert_eq!(
        flushed_import
            .kv()
            .get_json("pending")
            .await
            .expect("flushed import pending kv"),
        Some(json!(true))
    );
}
