use std::{env, sync::Arc};

use terracedb::SystemClock;
use terracedb_example_todo_api::{TodoApp, todo_db_builder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env::var("TODO_API_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let data_root = env::var("TODO_API_DATA_DIR").unwrap_or_else(|_| ".todo-api-data".into());
    let ssd_path = format!("{data_root}/ssd");
    let object_store_root = format!("{data_root}/object-store");
    let clock = Arc::new(SystemClock);
    let db = todo_db_builder(&ssd_path, "todo-api")
        .local_object_store(object_store_root)
        .clock(clock.clone())
        .open()
        .await?;
    let app = TodoApp::open(db, clock).await?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let router = app.router();
    let serve_result = axum::serve(listener, router).await;
    app.shutdown().await?;
    serve_result?;
    Ok(())
}
