use std::{
    fs,
    path::Path,
    process::Command,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use terracedb_vfs::{CreateOptions, VfsArtifactStoreExt, VolumeConfig, VolumeId, VolumeStore};

use terracedb_sandbox::{
    SandboxBaseLayer, SandboxHarness, SandboxServices, SandboxSnapshotLayer, SandboxSnapshotRecipe,
    node_v24_14_1_js_tree_recipe, node_v24_14_1_npm_cli_v11_12_1_recipe,
    npm_cli_v11_12_1_runtime_tree_recipe,
};

fn unique_temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "terracedb-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos()
    ))
}

fn git<I, S>(repo: &Path, args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let status = Command::new("git")
        .args(args)
        .current_dir(repo)
        .status()
        .expect("spawn git");
    assert!(status.success(), "git command failed in {}", repo.display());
}

#[tokio::test]
async fn bundled_base_layer_materializes_once_and_sessions_reuse_it() {
    let harness = SandboxHarness::deterministic(10, 7, SandboxServices::deterministic());
    let volumes = harness.volumes();

    let source = volumes
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x6100))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("open source volume");
    source
        .fs()
        .write_file(
            "/workspace/project/index.js",
            b"module.exports = 1".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write source project");

    let artifact = volumes
        .export_volume_artifact(terracedb_vfs::CloneVolumeSource::new(
            source.info().volume_id,
        ))
        .await
        .expect("export base layer artifact");
    let base_layer = SandboxBaseLayer::from_bytes("test-base", artifact);

    let first = harness
        .open_session_from_base_layer(&base_layer, VolumeId::new(0x6101), VolumeId::new(0x6102))
        .await
        .expect("open first session");
    assert_eq!(
        first
            .volume()
            .fs()
            .read_file("/workspace/project/index.js")
            .await
            .expect("read first session base file"),
        Some(b"module.exports = 1".to_vec())
    );

    let base_volume = volumes
        .open_volume(VolumeConfig::new(VolumeId::new(0x6101)))
        .await
        .expect("reopen base volume");
    base_volume
        .fs()
        .write_file(
            "/workspace/project/index.js",
            b"module.exports = 2".to_vec(),
            CreateOptions::default(),
        )
        .await
        .expect("mutate imported base volume");

    let second = harness
        .open_session_from_base_layer(&base_layer, VolumeId::new(0x6101), VolumeId::new(0x6103))
        .await
        .expect("open second session");
    assert_eq!(
        second
            .volume()
            .fs()
            .read_file("/workspace/project/index.js")
            .await
            .expect("read second session base file"),
        Some(b"module.exports = 2".to_vec())
    );
}

#[tokio::test]
async fn file_backed_base_layer_streams_artifact_from_disk() {
    let harness = SandboxHarness::deterministic(10, 8, SandboxServices::deterministic());
    let volumes = harness.volumes();

    let source = volumes
        .open_volume(
            VolumeConfig::new(VolumeId::new(0x6200))
                .with_chunk_size(4)
                .with_create_if_missing(true),
        )
        .await
        .expect("open source volume");
    source
        .fs()
        .write_file(
            "/workspace/project/package.json",
            br#"{"name":"demo"}"#.to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write source project");

    let artifact = volumes
        .export_volume_artifact(terracedb_vfs::CloneVolumeSource::new(
            source.info().volume_id,
        ))
        .await
        .expect("export base layer artifact");

    let artifact_path = std::env::temp_dir().join(format!(
        "terracedb-base-layer-{}-{}.tdva",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos()
    ));
    fs::write(&artifact_path, artifact).expect("write artifact file");

    let base_layer = SandboxBaseLayer::from_artifact_path("file-base", &artifact_path);
    let session = harness
        .open_session_from_base_layer(&base_layer, VolumeId::new(0x6201), VolumeId::new(0x6202))
        .await
        .expect("open file-backed base layer session");

    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/workspace/project/package.json")
            .await
            .expect("read package json"),
        Some(br#"{"name":"demo"}"#.to_vec())
    );

    fs::remove_file(&artifact_path).expect("remove artifact file");
}

#[tokio::test]
async fn git_backed_base_layer_builds_artifact_via_tree_only_import() {
    let repo_root = unique_temp_dir("git-base-layer");
    fs::create_dir_all(repo_root.join("lib")).expect("mkdir lib");
    fs::create_dir_all(repo_root.join("test")).expect("mkdir test");
    fs::create_dir_all(repo_root.join("docs")).expect("mkdir docs");
    fs::write(repo_root.join("lib/keep.js"), b"exports.keep = true;\n").expect("write keep");
    fs::write(repo_root.join("test/spec.js"), b"exports.spec = true;\n").expect("write spec");
    fs::write(repo_root.join("docs/skip.md"), b"skip\n").expect("write skip");
    fs::write(repo_root.join("README.md"), b"readme\n").expect("write readme");

    git(&repo_root, ["init", "-q"]);

    let harness =
        SandboxHarness::deterministic(10, 9, SandboxServices::deterministic_with_host_git());
    let layer = SandboxSnapshotRecipe::new("git-layer")
        .layer(
            SandboxSnapshotLayer::git_host_path(
                "node-source",
                repo_root.to_string_lossy().into_owned(),
            )
            .with_mode(terracedb_git::GitImportMode::WorkingTree {
                include_untracked: true,
                include_ignored: false,
            })
            .with_target_root("/node")
            .with_pathspec(vec!["lib".to_string(), "test".to_string()]),
        )
        .build_base_layer(Arc::new(terracedb_sandbox::HostGitBridge::default()))
        .await
        .expect("build git base layer");

    let session = harness
        .open_session_from_base_layer(&layer, VolumeId::new(0x6301), VolumeId::new(0x6302))
        .await
        .expect("open git-backed layer session");

    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/node/lib/keep.js")
            .await
            .expect("read imported lib file"),
        Some(b"exports.keep = true;\n".to_vec())
    );
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/node/test/spec.js")
            .await
            .expect("read imported test file"),
        Some(b"exports.spec = true;\n".to_vec())
    );
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/node/docs/skip.md")
            .await
            .expect("read filtered docs file"),
        None
    );
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/node/README.md")
            .await
            .expect("read filtered root file"),
        None
    );

    fs::remove_dir_all(&repo_root).expect("cleanup repo");
}

#[tokio::test]
async fn git_backed_base_layer_can_stream_single_file_artifact_to_disk() {
    let repo_root = unique_temp_dir("git-base-layer-file");
    fs::create_dir_all(repo_root.join("lib")).expect("mkdir lib");
    fs::write(repo_root.join("lib/path.js"), b"exports.path = true;\n").expect("write lib file");
    git(&repo_root, ["init", "-q"]);

    let harness =
        SandboxHarness::deterministic(10, 11, SandboxServices::deterministic_with_host_git());
    let artifact_path = unique_temp_dir("git-base-layer-file-artifact").with_extension("tdva");
    let recipe = SandboxSnapshotRecipe::new("git-layer-file").layer(
        SandboxSnapshotLayer::git_host_path(
            "node-source",
            repo_root.to_string_lossy().into_owned(),
        )
        .with_mode(terracedb_git::GitImportMode::WorkingTree {
            include_untracked: true,
            include_ignored: false,
        })
        .with_target_root("/node")
        .with_pathspec(vec!["lib".to_string()]),
    );

    let layer = recipe
        .write_artifact_to_path(
            Arc::new(terracedb_sandbox::HostGitBridge::default()),
            &artifact_path,
        )
        .await
        .expect("write git base layer artifact");

    assert!(artifact_path.exists(), "artifact file should exist on disk");
    assert!(
        layer.artifact_path().is_some(),
        "layer should reference artifact path"
    );

    let session = harness
        .open_session_from_base_layer(&layer, VolumeId::new(0x6501), VolumeId::new(0x6502))
        .await
        .expect("open session from file-backed git layer");
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/node/lib/path.js")
            .await
            .expect("read imported lib/path.js"),
        Some(b"exports.path = true;\n".to_vec())
    );

    fs::remove_file(&artifact_path).expect("cleanup artifact");
    fs::remove_dir_all(&repo_root).expect("cleanup repo");
}

#[tokio::test]
async fn snapshot_recipe_can_merge_runtime_and_app_layers() {
    let node_repo = unique_temp_dir("snapshot-recipe-node");
    let app_repo = unique_temp_dir("snapshot-recipe-app");
    fs::create_dir_all(node_repo.join("lib")).expect("mkdir node lib");
    fs::create_dir_all(app_repo.join("src")).expect("mkdir app src");
    fs::write(node_repo.join("lib/path.js"), b"exports.sep = '/';\n").expect("write node file");
    fs::write(
        app_repo.join("src/index.js"),
        b"const path = require('/node/lib/path.js'); module.exports = path.sep;\n",
    )
    .expect("write app file");
    git(&node_repo, ["init", "-q"]);
    git(&app_repo, ["init", "-q"]);

    let harness =
        SandboxHarness::deterministic(10, 13, SandboxServices::deterministic_with_host_git());
    let layer = SandboxSnapshotRecipe::new("runtime-and-app")
        .layer(
            SandboxSnapshotLayer::git_host_path(
                "node-source",
                node_repo.to_string_lossy().into_owned(),
            )
            .with_mode(terracedb_git::GitImportMode::WorkingTree {
                include_untracked: true,
                include_ignored: false,
            })
            .with_target_root("/node")
            .with_pathspec(vec!["lib".to_string()]),
        )
        .layer(
            SandboxSnapshotLayer::git_host_path(
                "app-source",
                app_repo.to_string_lossy().into_owned(),
            )
            .with_mode(terracedb_git::GitImportMode::WorkingTree {
                include_untracked: true,
                include_ignored: false,
            })
            .with_target_root("/workspace/app"),
        )
        .build_base_layer(Arc::new(terracedb_sandbox::HostGitBridge::default()))
        .await
        .expect("build merged snapshot recipe");

    let session = harness
        .open_session_from_base_layer(&layer, VolumeId::new(0x6551), VolumeId::new(0x6552))
        .await
        .expect("open merged snapshot recipe session");

    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/node/lib/path.js")
            .await
            .expect("read runtime file"),
        Some(b"exports.sep = '/';\n".to_vec())
    );
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/workspace/app/src/index.js")
            .await
            .expect("read app file"),
        Some(b"const path = require('/node/lib/path.js'); module.exports = path.sep;\n".to_vec())
    );

    fs::remove_dir_all(&node_repo).expect("cleanup node repo");
    fs::remove_dir_all(&app_repo).expect("cleanup app repo");
}

#[tokio::test]
async fn host_tree_layer_builds_artifact_from_materialized_runtime_tree() {
    let runtime_root = unique_temp_dir("host-tree-runtime");
    fs::create_dir_all(runtime_root.join("bin")).expect("mkdir bin");
    fs::create_dir_all(runtime_root.join("lib")).expect("mkdir lib");
    fs::create_dir_all(runtime_root.join("node_modules/lodash")).expect("mkdir node_modules");
    fs::create_dir_all(runtime_root.join(".git")).expect("mkdir git metadata");
    fs::create_dir_all(runtime_root.join("docs")).expect("mkdir docs");
    fs::write(runtime_root.join("bin/npm"), b"#!/usr/bin/env node\n").expect("write bin/npm");
    fs::write(runtime_root.join("lib/cli.js"), b"module.exports = true;\n").expect("write lib");
    fs::write(
        runtime_root.join("node_modules/lodash/index.js"),
        b"module.exports = 'lodash';\n",
    )
    .expect("write lodash");
    fs::write(runtime_root.join("package.json"), b"{\"name\":\"npm\"}\n").expect("write package");
    fs::write(runtime_root.join(".git/config"), b"[core]\n").expect("write git metadata");
    fs::write(runtime_root.join("docs/readme.md"), b"skip\n").expect("write docs");

    let harness =
        SandboxHarness::deterministic(10, 14, SandboxServices::deterministic_with_host_git());
    let layer = SandboxSnapshotRecipe::new("host-tree-runtime")
        .layer(
            SandboxSnapshotLayer::host_tree(
                "npm-runtime",
                runtime_root.to_string_lossy().into_owned(),
            )
            .with_target_root("/npm")
            .with_pathspec(vec![
                "bin".to_string(),
                "lib".to_string(),
                "node_modules".to_string(),
                "package.json".to_string(),
            ]),
        )
        .build_base_layer(Arc::new(terracedb_sandbox::HostGitBridge::default()))
        .await
        .expect("build host-tree layer");

    let session = harness
        .open_session_from_base_layer(&layer, VolumeId::new(0x6701), VolumeId::new(0x6702))
        .await
        .expect("open host-tree session");

    assert!(
        session
            .volume()
            .fs()
            .read_file("/npm/bin/npm")
            .await
            .expect("read bin/npm")
            .is_some()
    );
    assert!(
        session
            .volume()
            .fs()
            .read_file("/npm/node_modules/lodash/index.js")
            .await
            .expect("read lodash")
            .is_some()
    );
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/npm/.git/config")
            .await
            .expect("read excluded git metadata"),
        None
    );
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/npm/docs/readme.md")
            .await
            .expect("read excluded docs file"),
        None
    );

    fs::remove_dir_all(&runtime_root).expect("cleanup runtime root");
}

#[tokio::test]
async fn snapshot_recipe_can_merge_git_and_host_tree_layers() {
    let node_repo = unique_temp_dir("snapshot-recipe-node-git");
    let npm_root = unique_temp_dir("snapshot-recipe-npm-host-tree");
    fs::create_dir_all(node_repo.join("lib")).expect("mkdir node lib");
    fs::create_dir_all(npm_root.join("node_modules/lodash")).expect("mkdir lodash");
    fs::create_dir_all(npm_root.join("lib")).expect("mkdir npm lib");
    fs::write(node_repo.join("lib/path.js"), b"exports.sep = '/';\n").expect("write node file");
    fs::write(npm_root.join("lib/npm.js"), b"module.exports = 'npm';\n").expect("write npm lib");
    fs::write(
        npm_root.join("node_modules/lodash/index.js"),
        b"module.exports = 'lodash';\n",
    )
    .expect("write lodash");
    fs::write(npm_root.join("package.json"), b"{\"name\":\"npm\"}\n").expect("write package");
    git(&node_repo, ["init", "-q"]);

    let harness =
        SandboxHarness::deterministic(10, 15, SandboxServices::deterministic_with_host_git());
    let layer = SandboxSnapshotRecipe::new("node-and-npm")
        .layer(
            SandboxSnapshotLayer::git_host_path(
                "node-source",
                node_repo.to_string_lossy().into_owned(),
            )
            .with_mode(terracedb_git::GitImportMode::WorkingTree {
                include_untracked: true,
                include_ignored: false,
            })
            .with_target_root("/node")
            .with_pathspec(vec!["lib".to_string()]),
        )
        .layer(
            SandboxSnapshotLayer::host_tree("npm-runtime", npm_root.to_string_lossy().into_owned())
                .with_target_root("/npm")
                .with_pathspec(vec![
                    "lib".to_string(),
                    "node_modules".to_string(),
                    "package.json".to_string(),
                ]),
        )
        .build_base_layer(Arc::new(terracedb_sandbox::HostGitBridge::default()))
        .await
        .expect("build mixed snapshot");

    let session = harness
        .open_session_from_base_layer(&layer, VolumeId::new(0x6751), VolumeId::new(0x6752))
        .await
        .expect("open mixed snapshot");

    assert!(
        session
            .volume()
            .fs()
            .read_file("/node/lib/path.js")
            .await
            .expect("read node path.js")
            .is_some()
    );
    assert!(
        session
            .volume()
            .fs()
            .read_file("/npm/node_modules/lodash/index.js")
            .await
            .expect("read npm lodash")
            .is_some()
    );

    fs::remove_dir_all(&node_repo).expect("cleanup node repo");
    fs::remove_dir_all(&npm_root).expect("cleanup npm root");
}

#[test]
fn first_party_snapshot_recipes_are_public_and_pinned() {
    let node = node_v24_14_1_js_tree_recipe();
    assert_eq!(node.name(), "node-v24.14.1-js-tree");
    assert_eq!(node.layers().len(), 1);

    let npm = npm_cli_v11_12_1_runtime_tree_recipe("/tmp/npm-runtime");
    assert_eq!(npm.name(), "npm-cli-v11.12.1-runtime");
    assert_eq!(npm.layers().len(), 1);

    let combined = node_v24_14_1_npm_cli_v11_12_1_recipe("/tmp/npm-runtime");
    assert_eq!(combined.name(), "node-v24.14.1-npm-cli-v11.12.1-runtime");
    assert_eq!(combined.layers().len(), 2);
}

#[tokio::test]
#[ignore = "real node submodule packaging canary"]
async fn node_submodule_can_be_packaged_as_base_layer() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../third_party/node-src")
        .canonicalize()
        .expect("canonicalize node submodule");

    let harness =
        SandboxHarness::deterministic(10, 10, SandboxServices::deterministic_with_host_git());
    let layer = SandboxSnapshotRecipe::new("node-js-tree")
        .layer(
            SandboxSnapshotLayer::git_host_path(
                "node-source",
                repo_root.to_string_lossy().into_owned(),
            )
            .with_mode(terracedb_git::GitImportMode::Head)
            .with_target_root("/node")
            .with_pathspec(vec![
                "lib".to_string(),
                "deps".to_string(),
                "test".to_string(),
            ]),
        )
        .build_base_layer(Arc::new(terracedb_sandbox::HostGitBridge::default()))
        .await
        .expect("build node base layer");

    let session = harness
        .open_session_from_base_layer(&layer, VolumeId::new(0x6401), VolumeId::new(0x6402))
        .await
        .expect("open node base layer session");

    assert!(
        session
            .volume()
            .fs()
            .read_file("/node/lib/path.js")
            .await
            .expect("read node path.js")
            .is_some()
    );
    assert!(
        session
            .volume()
            .fs()
            .read_file("/node/test/parallel/test-module-create-require.js")
            .await
            .expect("read node commonjs test")
            .is_some()
    );
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/node/src/node.cc")
            .await
            .expect("read excluded c++ source"),
        None
    );
}

#[tokio::test]
#[ignore = "large vendored node base artifact canary"]
async fn vendored_node_base_layer_opens_in_sandbox() {
    let harness =
        SandboxHarness::deterministic(10, 12, SandboxServices::deterministic_with_host_git());
    let layer = SandboxBaseLayer::vendored_node_v24_14_1_js_tree();

    let session = harness
        .open_session_from_base_layer(&layer, VolumeId::new(0x6601), VolumeId::new(0x6602))
        .await
        .expect("open vendored node base layer session");

    assert!(
        session
            .volume()
            .fs()
            .read_file("/node/lib/path.js")
            .await
            .expect("read vendored node path.js")
            .is_some()
    );
    assert!(
        session
            .volume()
            .fs()
            .read_file("/node/test/parallel/test-module-create-require.js")
            .await
            .expect("read vendored node commonjs test")
            .is_some()
    );
    assert_eq!(
        session
            .volume()
            .fs()
            .read_file("/node/src/node.cc")
            .await
            .expect("read excluded vendored c++ source"),
        None
    );
}

#[tokio::test]
#[ignore = "large vendored combined node+npm base artifact canary"]
async fn vendored_node_and_npm_base_layer_opens_in_sandbox() {
    let harness =
        SandboxHarness::deterministic(10, 16, SandboxServices::deterministic_with_host_git());
    let layer = SandboxBaseLayer::vendored_node_v24_14_1_npm_cli_v11_12_1();

    let session = harness
        .open_session_from_base_layer(&layer, VolumeId::new(0x6681), VolumeId::new(0x6682))
        .await
        .expect("open vendored combined node+npm base layer session");

    assert!(
        session
            .volume()
            .fs()
            .read_file("/node/lib/path.js")
            .await
            .expect("read vendored node path.js")
            .is_some()
    );
    assert!(
        session
            .volume()
            .fs()
            .read_file("/npm/lib/npm.js")
            .await
            .expect("read vendored npm lib/npm.js")
            .is_some()
    );
    assert!(
        session
            .volume()
            .fs()
            .read_file("/npm/node_modules/minipass/dist/commonjs/index.js")
            .await
            .expect("read vendored npm dependency")
            .is_some()
    );
}
