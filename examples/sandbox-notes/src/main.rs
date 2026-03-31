use terracedb_example_sandbox_notes::run_demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = run_demo().await?;
    println!(
        "reviewed {} open notes for {}",
        report.summary.open_count, report.summary.project
    );
    println!("readonly view: {}", report.view_uri);
    println!("exported snapshot: {}", report.export_path);
    println!(
        "updated note count: {}",
        report
            .notes
            .first()
            .map(|note| note.comments.len())
            .unwrap_or_default()
    );
    Ok(())
}
