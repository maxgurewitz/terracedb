use std::env;

use terracedb_example_workflow_duet::run_local_demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_root =
        env::var("WORKFLOW_DUET_DATA_DIR").unwrap_or_else(|_| ".workflow-duet-data".into());
    let report = run_local_demo(&data_root).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
