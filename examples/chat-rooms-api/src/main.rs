use std::{env, sync::Arc};

use terracedb::{DbComponents, SystemClock};
use terracedb_example_chat_rooms_api::{
    CHAT_ROOMS_DATABASE_NAME, CHAT_ROOMS_SERVER_PORT, ChatRoomsApp, ChatRoomsExampleProfile,
    chat_rooms_db_settings,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env::var("CHAT_ROOMS_API_BIND_ADDR")
        .unwrap_or_else(|_| format!("127.0.0.1:{CHAT_ROOMS_SERVER_PORT}"));
    let data_root =
        env::var("CHAT_ROOMS_API_DATA_DIR").unwrap_or_else(|_| ".chat-rooms-api-data".into());
    let profile = env::var("CHAT_ROOMS_API_PROFILE")
        .ok()
        .map(|value| value.parse::<ChatRoomsExampleProfile>())
        .transpose()?
        .unwrap_or_default();

    let deployment = profile.deployment()?;
    let clock = Arc::new(SystemClock);
    let components = DbComponents::production_local(format!("{data_root}/object-store"));
    let db = deployment
        .open_database(
            CHAT_ROOMS_DATABASE_NAME,
            chat_rooms_db_settings(&format!("{data_root}/ssd"), "chat-rooms-api"),
            components,
        )
        .await?;
    let app = ChatRoomsApp::open(deployment, db, profile, clock).await?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let serve_result = axum::serve(listener, app.router()).await;
    app.shutdown().await?;
    serve_result?;
    Ok(())
}
