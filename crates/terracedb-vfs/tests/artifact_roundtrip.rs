use std::{io::Cursor, sync::Arc};

use serde_json::json;

use terracedb::{StubClock, StubRng, Timestamp};
use terracedb_vfs::{
    CloneVolumeSource, CompletedToolRun, CompletedToolRunOutcome, CreateOptions, InMemoryVfsStore,
    MkdirOptions, SnapshotOptions, ToolRunStatus, VfsArtifactStoreExt, VolumeConfig, VolumeExport,
    VolumeId, VolumeStore,
};

fn test_store(seed: u64, now: u64) -> InMemoryVfsStore {
    InMemoryVfsStore::new(
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    )
}

#[tokio::test]
async fn artifact_roundtrips_overlay_state_and_streamed_file_payloads() {
    let store = test_store(77, 100);
    let base = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x5100))
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
        .expect("mkdir docs");
    base.fs()
        .write_file(
            "/workspace/base.txt",
            b"base-file".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write base file");
    base.fs()
        .write_file(
            "/workspace/docs/guide.txt",
            b"guide".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("write guide");

    let overlay = store
        .create_overlay(
            base.snapshot(SnapshotOptions::default())
                .await
                .expect("base snapshot"),
            VolumeConfig::new(VolumeId::new(0x5101))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("create overlay");

    overlay
        .fs()
        .write_file(
            "/workspace/app.js",
            b"hello-flatbuffers".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write multi chunk file");
    overlay
        .fs()
        .symlink("app.js", "/workspace/current")
        .await
        .expect("create symlink");
    overlay
        .fs()
        .unlink("/workspace/docs/guide.txt")
        .await
        .expect("whiteout base guide");
    overlay
        .kv()
        .set_json("config", json!({"name": "demo", "flags": ["a", "b"]}))
        .await
        .expect("set kv");
    let tool_id = overlay
        .tools()
        .record_completed(CompletedToolRun {
            name: "build".to_string(),
            params: Some(json!({"optimize": true})),
            outcome: CompletedToolRunOutcome::Success {
                result: Some(json!({"status": "ok"})),
            },
        })
        .await
        .expect("record tool");

    let bytes = store
        .export_volume_artifact(CloneVolumeSource::new(overlay.info().volume_id))
        .await
        .expect("export artifact");

    let imported = store
        .import_volume_artifact(
            &bytes,
            VolumeConfig::new(VolumeId::new(0x5102))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("import artifact");

    assert_eq!(imported.info().chunk_size, 4);
    assert_eq!(
        imported
            .fs()
            .read_file("/workspace/app.js")
            .await
            .expect("read imported file"),
        Some(b"hello-flatbuffers".to_vec())
    );
    assert_eq!(
        imported
            .fs()
            .read_file("/workspace/docs/guide.txt")
            .await
            .expect("read whiteouted file"),
        None
    );
    assert_eq!(
        imported
            .fs()
            .readlink("/workspace/current")
            .await
            .expect("read imported symlink"),
        "app.js"
    );
    assert_eq!(
        imported
            .kv()
            .get_json("config")
            .await
            .expect("get imported kv"),
        Some(json!({"name": "demo", "flags": ["a", "b"]}))
    );

    let tool = imported
        .tools()
        .get(tool_id)
        .await
        .expect("get imported tool")
        .expect("tool exists");
    assert_eq!(tool.status, ToolRunStatus::Success);
    assert_eq!(tool.result, Some(json!({"status": "ok"})));
}

#[tokio::test]
async fn artifact_streaming_reader_writer_roundtrips_without_slice_api() {
    let store = test_store(99, 300);
    let volume = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x5300))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");
    volume
        .fs()
        .write_file(
            "/workspace/data.txt",
            b"streamed-payload".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write streamed file");

    let mut artifact = Vec::new();
    store
        .export_volume_artifact_to_writer(CloneVolumeSource::new(volume.info().volume_id), &mut artifact)
        .await
        .expect("export to writer");

    let mut reader = Cursor::new(artifact);
    let imported = store
        .import_volume_artifact_from_reader(
            &mut reader,
            VolumeConfig::new(VolumeId::new(0x5301))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("import from reader");

    assert_eq!(
        imported
            .fs()
            .read_file("/workspace/data.txt")
            .await
            .expect("read streamed import"),
        Some(b"streamed-payload".to_vec())
    );
}

#[tokio::test]
async fn artifact_compresses_file_payloads_when_smaller() {
    let store = test_store(123, 350);
    let volume = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x5350))
                .with_chunk_size(4 * 1024)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");
    let payload = vec![b'a'; 256 * 1024];
    volume
        .fs()
        .write_file(
            "/workspace/repeated.txt",
            payload.clone(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write repeated payload");

    let artifact = store
        .export_volume_artifact(CloneVolumeSource::new(volume.info().volume_id))
        .await
        .expect("export compressed artifact");

    assert!(
        artifact.len() < payload.len() / 4,
        "expected compressed artifact to be substantially smaller than raw payload: artifact={} raw={}",
        artifact.len(),
        payload.len()
    );

    let imported = store
        .import_volume_artifact(
            &artifact,
            VolumeConfig::new(VolumeId::new(0x5351))
                .with_chunk_size(4 * 1024)
                .with_create_if_missing(true),
        )
        .await
        .expect("import compressed artifact");

    assert_eq!(
        imported
            .fs()
            .read_file("/workspace/repeated.txt")
            .await
            .expect("read compressed import"),
        Some(payload)
    );
}

#[tokio::test]
async fn ensure_imported_volume_artifact_reuses_existing_base_volume() {
    let store = test_store(111, 400);
    let source = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x5400))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("open source");
    source
        .fs()
        .write_file("/workspace/base.txt", b"base".to_vec(), CreateOptions {
            create_parents: true,
            ..Default::default()
        })
        .await
        .expect("write source");

    let artifact = store
        .export_volume_artifact(CloneVolumeSource::new(source.info().volume_id))
        .await
        .expect("export artifact");

    let first = store
        .ensure_imported_volume_artifact(
            &artifact,
            VolumeConfig::new(VolumeId::new(0x5401)).with_chunk_size(4),
        )
        .await
        .expect("initial ensure import");
    first
        .fs()
        .write_file(
            "/workspace/base.txt",
            b"mutated".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("mutate imported base");

    let second = store
        .ensure_imported_volume_artifact(
            &artifact,
            VolumeConfig::new(VolumeId::new(0x5401)).with_chunk_size(4),
        )
        .await
        .expect("reuse existing base");

    assert_eq!(second.info().volume_id, VolumeId::new(0x5401));
    assert_eq!(
        second
            .fs()
            .read_file("/workspace/base.txt")
            .await
            .expect("read reused base"),
        Some(b"mutated".to_vec())
    );
}

#[tokio::test]
async fn artifact_rejects_bad_magic_corrupt_manifest_and_trailing_bytes() {
    let store = test_store(88, 200);
    let volume = store
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x5200))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("open volume");
    volume
        .fs()
        .write_file("/file.txt", b"payload".to_vec(), CreateOptions::default())
        .await
        .expect("write file");

    let export = store
        .export_volume(CloneVolumeSource::new(volume.info().volume_id))
        .await
        .expect("export volume");
    let bytes = export.to_artifact_bytes().expect("artifact bytes");

    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0xff;
    let error = VolumeExport::from_artifact_bytes(&bad_magic)
        .err()
        .expect("bad magic should fail");
    assert!(error.to_string().contains("magic mismatch"));

    let mut corrupt_manifest = bytes.clone();
    let manifest_byte = 16usize;
    corrupt_manifest[manifest_byte] ^= 0xff;
    let error = VolumeExport::from_artifact_bytes(&corrupt_manifest)
        .err()
        .expect("corrupt manifest fails");
    assert!(
        error.to_string().contains("invalid volume artifact manifest")
            || error
                .to_string()
                .contains("manifest identifier mismatch")
    );

    let mut trailing = bytes.clone();
    trailing.push(0);
    let error = VolumeExport::from_artifact_bytes(&trailing)
        .err()
        .expect("trailing bytes fail");
    assert!(error.to_string().contains("trailing bytes"));
}
