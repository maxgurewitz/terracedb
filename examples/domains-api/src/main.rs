use std::env;

use terracedb::DbComponents;
use terracedb_example_domains_api::{
    ANALYTICS_DATABASE_NAME, DOMAINS_SERVER_PORT, DomainsApp, DomainsExampleProfile,
    PRIMARY_DATABASE_NAME, domains_db_settings,
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
    let object_store_root = format!("{data_root}/object-store");
    let components = DbComponents::production_local(object_store_root);

    let primary = deployment
        .open_database(
            PRIMARY_DATABASE_NAME,
            domains_db_settings(&format!("{data_root}/primary/ssd"), "domains-api/primary"),
            components.clone(),
        )
        .await?;

    let analytics = deployment
        .open_database(
            ANALYTICS_DATABASE_NAME,
            domains_db_settings(
                &format!("{data_root}/analytics/ssd"),
                "domains-api/analytics",
            ),
            components,
        )
        .await?;

    let app = DomainsApp::open(deployment, primary, analytics, profile).await?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let serve_result = axum::serve(listener, app.router()).await;
    app.shutdown().await?;
    serve_result?;
    Ok(())
}
