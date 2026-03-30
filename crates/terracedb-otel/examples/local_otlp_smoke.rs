use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt;
use opentelemetry::trace::TraceContextExt;
use terracedb::{
    Db, LogCursor, ScanOptions, StubFileSystem, StubObjectStore, Value,
    test_support::{row_table_config, test_dependencies, tiered_test_config},
};
use terracedb_otel::{BatchSettings, OtlpConfig, OtlpProtocol, TelemetryMode, TerracedbTelemetry};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

type ExampleResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("otel-smoke-{millis}-{}", std::process::id())
}

#[tokio::main]
async fn main() -> ExampleResult {
    let run_id = run_id();
    let service_name = std::env::var("TERRACEDB_OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| "terracedb-otel-smoke".to_string());
    let otlp_endpoint = std::env::var("TERRACEDB_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:14317".to_string());
    let db_path = format!("/terracedb/debug/otel/{run_id}");

    let mut telemetry = TerracedbTelemetry::builder(service_name.clone())
        .with_environment("local-debug")
        .with_resource_attribute("terracedb.debug.run_id", run_id.clone())
        .with_mode(TelemetryMode::Otlp(OtlpConfig {
            endpoint: otlp_endpoint.clone(),
            protocol: OtlpProtocol::Grpc,
            metric_export_interval: Duration::from_millis(250),
            metric_snapshot_interval: Duration::ZERO,
            trace_batch: BatchSettings {
                scheduled_delay: Duration::from_millis(100),
                ..BatchSettings::default()
            },
            log_batch: BatchSettings {
                scheduled_delay: Duration::from_millis(100),
                ..BatchSettings::default()
            },
            ..OtlpConfig::default()
        }))
        .build()?;

    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let db = Db::open(
        tiered_test_config(&db_path),
        test_dependencies(file_system, object_store),
    )
    .await?;
    telemetry.observe_db(db.clone());

    let events = db.create_table(row_table_config("events")).await?;
    let run_id_for_emit = run_id.clone();

    let (trace_id, sequence, seen_entries) = telemetry
        .with_subscriber(async {
            let root =
                tracing::info_span!("app.terracedb.otel_smoke", terracedb_debug_run_id = %run_id);
            let span_context = root.context().span().span_context().clone();
            async move {
                let sequence = events
                    .write(
                        format!("event:{run_id_for_emit}").into_bytes(),
                        Value::bytes(format!("value:{run_id_for_emit}")),
                    )
                    .await?;
                db.flush().await?;

                let stream = db
                    .scan_since(&events, LogCursor::beginning(), ScanOptions::default())
                    .await?;
                let seen_entries = stream.collect::<Vec<_>>().await.len();

                tracing::warn!(
                    terracedb_debug_run_id = %run_id_for_emit,
                    terracedb_debug_entries = seen_entries,
                    "terracedb otel smoke emitted telemetry"
                );

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    span_context.trace_id().to_string(),
                    sequence.get(),
                    seen_entries,
                ))
            }
            .instrument(root)
            .await
        })
        .await?;

    telemetry.force_flush().await?;
    telemetry.shutdown().await?;

    println!("run_id={run_id}");
    println!("service_name={service_name}");
    println!("trace_id={trace_id}");
    println!("db_path={db_path}");
    println!("otlp_endpoint={otlp_endpoint}");
    println!("mcp_url=http://127.0.0.1:3320/api/mcp");
    println!("committed_sequence={sequence}");
    println!("observed_entries={seen_entries}");
    println!("status=ok");

    Ok(())
}
