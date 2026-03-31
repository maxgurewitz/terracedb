use std::{env, sync::Arc};

use terracedb::SystemClock;
use terracedb_example_domains_api::{
    ANALYTICS_DATABASE_NAME, DOMAINS_SERVER_PORT, DomainsApp, DomainsExampleProfile,
    PRIMARY_DATABASE_NAME, domains_db_builder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env::var("DOMAINS_API_BIND_ADDR")
        .unwrap_or_else(|_| format!("127.0.0.1:{DOMAINS_SERVER_PORT}"));
    let data_root = env::var("DOMAINS_API_DATA_DIR").unwrap_or_else(|_| ".domains-api-data".into());
    let profile = env::var("DOMAINS_API_PROFILE")
        .ok()
        .map(|value| value.parse::<DomainsExampleProfile>())
        .transpose()?
        .unwrap_or_default();
    let deployment = profile.deployment()?;
    let clock = Arc::new(SystemClock);
    let object_store_root = format!("{data_root}/object-store");

    let primary = domains_db_builder(&format!("{data_root}/primary/ssd"), "domains-api/primary")
        .local_object_store(object_store_root.clone())
        .clock(clock.clone())
        .colocated_database(&deployment, PRIMARY_DATABASE_NAME)?
        .open()
        .await?;

    let analytics = domains_db_builder(
        &format!("{data_root}/analytics/ssd"),
        "domains-api/analytics",
    )
    .local_object_store(object_store_root)
    .clock(clock)
    .colocated_database(&deployment, ANALYTICS_DATABASE_NAME)?
    .open()
    .await?;

    let app = DomainsApp::open(primary, analytics, profile).await?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let serve_result = axum::serve(listener, app.router()).await;
    app.shutdown().await?;
    serve_result?;
    Ok(())
}
