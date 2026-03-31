use std::{env, str::FromStr};

use terracedb_example_telemetry_api::{
    TELEMETRY_SERVER_PORT, TelemetryApp, TelemetryExampleProfile, telemetry_db_builder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env::var("TELEMETRY_API_BIND_ADDR")
        .unwrap_or_else(|_| format!("127.0.0.1:{TELEMETRY_SERVER_PORT}"));
    let data_root =
        env::var("TELEMETRY_API_DATA_DIR").unwrap_or_else(|_| ".telemetry-api-data".into());
    let profile = env::var("TELEMETRY_API_PROFILE")
        .ok()
        .map(|value| TelemetryExampleProfile::from_str(&value))
        .transpose()?
        .unwrap_or_default();
    let ssd_path = format!("{data_root}/ssd");
    let object_store_root = format!("{data_root}/object-store");

    let db = telemetry_db_builder(&ssd_path, "telemetry-api", profile)
        .local_object_store(object_store_root)
        .open()
        .await?;
    let app = TelemetryApp::open(db, profile).await?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let router = app.router();
    let serve_result = axum::serve(listener, router).await;
    app.shutdown().await?;
    serve_result?;
    Ok(())
}
