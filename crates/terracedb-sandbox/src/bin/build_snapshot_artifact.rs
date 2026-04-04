use std::{path::PathBuf, process::ExitCode, sync::Arc};

use terracedb_git::GitImportMode;
use terracedb_sandbox::{HostGitBridge, SandboxSnapshotLayer, SandboxSnapshotRecipe};

fn print_usage(program: &str) {
    eprintln!(
        "usage: {program} <git-host|git-remote|host-tree> <source> <output_path> [--name <name>] [--target-root <path>] [--reference <ref>] [--working-tree] [--include-untracked] [--include-ignored] [--include-git-metadata] [--chunk-size <u32>] [--pathspec <entry>]..."
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = std::env::args().collect::<Vec<_>>();
    let program = args
        .first()
        .cloned()
        .unwrap_or_else(|| "build_snapshot_artifact".to_string());
    if args.len() < 4 {
        print_usage(&program);
        return ExitCode::from(2);
    }

    let source_kind = args[1].as_str();
    let source = args[2].clone();
    let output_path = PathBuf::from(args[3].clone());
    let mut name = output_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("snapshot-artifact")
        .to_string();
    let mut target_root = "/".to_string();
    let mut reference = None;
    let mut working_tree = false;
    let mut include_untracked = false;
    let mut include_ignored = false;
    let mut include_git_metadata = false;
    let mut chunk_size = None;
    let mut pathspec = Vec::new();

    let mut index = 4;
    while index < args.len() {
        match args[index].as_str() {
            "--name" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    print_usage(&program);
                    return ExitCode::from(2);
                };
                name = value.clone();
            }
            "--target-root" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    print_usage(&program);
                    return ExitCode::from(2);
                };
                target_root = value.clone();
            }
            "--reference" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    print_usage(&program);
                    return ExitCode::from(2);
                };
                reference = Some(value.clone());
            }
            "--working-tree" => working_tree = true,
            "--include-untracked" => include_untracked = true,
            "--include-ignored" => include_ignored = true,
            "--include-git-metadata" => include_git_metadata = true,
            "--chunk-size" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    print_usage(&program);
                    return ExitCode::from(2);
                };
                match value.parse::<u32>() {
                    Ok(parsed) => chunk_size = Some(parsed),
                    Err(error) => {
                        eprintln!("invalid --chunk-size value {value}: {error}");
                        return ExitCode::from(2);
                    }
                }
            }
            "--pathspec" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    print_usage(&program);
                    return ExitCode::from(2);
                };
                pathspec.push(value.clone());
            }
            other => {
                eprintln!("unknown argument: {other}");
                print_usage(&program);
                return ExitCode::from(2);
            }
        }
        index += 1;
    }

    let layer = match source_kind {
        "git-host" => {
            SandboxSnapshotLayer::git_host_path("source", source).with_mode(if working_tree {
                GitImportMode::WorkingTree {
                    include_untracked,
                    include_ignored,
                }
            } else {
                GitImportMode::Head
            })
        }
        "git-remote" => SandboxSnapshotLayer::git_remote("source", source, reference),
        "host-tree" => {
            let mut layer = SandboxSnapshotLayer::host_tree("source", source);
            if include_git_metadata {
                layer = layer.with_skip_git_metadata(false);
            }
            layer
        }
        other => {
            eprintln!("unknown source kind: {other}");
            print_usage(&program);
            return ExitCode::from(2);
        }
    }
    .with_target_root(target_root)
    .with_pathspec(pathspec);

    let mut recipe = SandboxSnapshotRecipe::new(name).layer(layer);
    if let Some(chunk_size) = chunk_size {
        recipe = recipe.with_chunk_size(chunk_size);
    }

    match recipe
        .write_artifact_to_path(Arc::new(HostGitBridge::default()), &output_path)
        .await
    {
        Ok(layer) => {
            println!(
                "wrote snapshot artifact {} to {}",
                layer.name(),
                output_path.display()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(1)
        }
    }
}
