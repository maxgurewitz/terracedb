use std::{path::PathBuf, process::ExitCode, sync::Arc};

use terracedb_sandbox::{
    HostGitBridge, node_v24_14_1_npm_cli_v11_12_1_recipe,
};

fn print_usage(program: &str) {
    eprintln!("usage: {program} <npm_runtime_path> <output_path>");
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = std::env::args().collect::<Vec<_>>();
    let program = args
        .first()
        .cloned()
        .unwrap_or_else(|| "build_node_npm_base_layer".to_string());
    if args.len() != 3 {
        print_usage(&program);
        return ExitCode::from(2);
    }

    let npm_runtime_path = args[1].clone();
    let output_path = PathBuf::from(&args[2]);
    let recipe = node_v24_14_1_npm_cli_v11_12_1_recipe(npm_runtime_path);

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
