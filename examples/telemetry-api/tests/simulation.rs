use std::{error::Error as StdError, sync::Arc, time::Duration};

use axum::http::{Method, StatusCode};
use terracedb::{
    Db, DbConfig, DbDependencies, FieldValue, FileSystem, OpenOptions, SimulatedFileSystem,
    SkipIndexProbe, StorageConfig, StubClock, StubFileSystem, StubObjectStore, StubRng,
    TieredStorageConfig,
};
use terracedb_example_telemetry_api::{
    DeviceStateRecord, IngestReading, IngestReadingsRequest, IngestReadingsResponse,
    TELEMETRY_SERVER_PORT, TelemetryApp, TelemetryColumn, TelemetryExampleProfile,
    TelemetryScanResponse, TelemetryScanRow, TelemetrySummaryResponse, telemetry_db_config,
};
use terracedb_http::{SimulatedHttpClient, axum_router_server_with_shutdown};
use terracedb_simulation::{
    NetworkObjectStore, ObjectStoreFaultSpec, SeededSimulationRunner, SimulationHost,
    TelemetryAppOracle, TelemetryProjectedReading, TelemetryProjectionColumn, TelemetryReading,
    TelemetryScanRequest as OracleScanRequest, TelemetryWindow, TurmoilClock,
};
use tokio::sync::watch;

const SIM_DURATION: Duration = Duration::from_secs(5);
const MIN_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const MAX_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const OBJECT_STORE_HOST: &str = "object-store";
const OBJECT_STORE_PORT: u16 = 9400;
const SERVER_HOST: &str = "server";

type BoxError = Box<dyn StdError>;

fn boxed_error(error: impl StdError + 'static) -> BoxError {
    Box::new(error)
}

fn sample_readings() -> Vec<IngestReading> {
    vec![
        IngestReading {
            device_id: "device-01".to_string(),
            reading_at_ms: 100,
            temperature_c: 20,
            humidity_pct: 40,
            battery_mv: 3600,
            alert_active: false,
        },
        IngestReading {
            device_id: "device-01".to_string(),
            reading_at_ms: 200,
            temperature_c: 22,
            humidity_pct: 42,
            battery_mv: 3590,
            alert_active: true,
        },
        IngestReading {
            device_id: "device-01".to_string(),
            reading_at_ms: 300,
            temperature_c: 24,
            humidity_pct: 44,
            battery_mv: 3580,
            alert_active: true,
        },
        IngestReading {
            device_id: "device-02".to_string(),
            reading_at_ms: 150,
            temperature_c: 18,
            humidity_pct: 55,
            battery_mv: 3610,
            alert_active: false,
        },
    ]
}

fn larger_reading_batch(count: usize) -> Vec<IngestReading> {
    let mut readings = Vec::with_capacity(count);
    for index in 0..count {
        readings.push(IngestReading {
            device_id: "device-01".to_string(),
            reading_at_ms: 1_000 + index as u64 * 10,
            temperature_c: 20 + (index % 5) as i64,
            humidity_pct: 35 + (index % 10) as i64,
            battery_mv: 3600 - index as i64,
            alert_active: index % 3 == 0,
        });
    }
    readings
}

fn oracle_from_readings(readings: &[IngestReading]) -> TelemetryAppOracle {
    let mut oracle = TelemetryAppOracle::default();
    oracle.apply_ingest(readings.iter().map(to_oracle_reading));
    oracle
}

fn to_oracle_reading(reading: &IngestReading) -> TelemetryReading {
    TelemetryReading {
        device_id: reading.device_id.clone(),
        reading_at_ms: reading.reading_at_ms,
        temperature_c: reading.temperature_c,
        humidity_pct: reading.humidity_pct,
        battery_mv: reading.battery_mv,
        alert_active: reading.alert_active,
    }
}

fn to_scan_rows(rows: Vec<TelemetryProjectedReading>) -> Vec<TelemetryScanRow> {
    rows.into_iter()
        .map(|row| TelemetryScanRow {
            device_id: row.device_id,
            reading_at_ms: row.reading_at_ms,
            temperature_c: row.temperature_c,
            humidity_pct: row.humidity_pct,
            battery_mv: row.battery_mv,
            alert_active: row.alert_active,
        })
        .collect()
}

fn telemetry_server_host(
    seed: u64,
    path: &'static str,
    prefix: &'static str,
    profile: TelemetryExampleProfile,
    shutdown: watch::Receiver<bool>,
) -> (SimulationHost, watch::Receiver<bool>) {
    let (ready_tx, ready_rx) = watch::channel(false);
    (
        SimulationHost::new(SERVER_HOST, move || {
            let shutdown = shutdown.clone();
            let ready_tx = ready_tx.clone();
            async move {
                let db = Db::open(
                    telemetry_db_config(path, prefix, profile),
                    DbDependencies::new(
                        Arc::new(SimulatedFileSystem::default()),
                        Arc::new(NetworkObjectStore::new(
                            OBJECT_STORE_HOST,
                            OBJECT_STORE_PORT,
                        )),
                        Arc::new(TurmoilClock),
                        Arc::new(terracedb::DeterministicRng::seeded(seed)),
                    ),
                )
                .await
                .map_err(boxed_error)?;
                let app = TelemetryApp::open(db, profile).await.map_err(boxed_error)?;

                let server = axum_router_server_with_shutdown(
                    SERVER_HOST,
                    TELEMETRY_SERVER_PORT,
                    app.router(),
                    shutdown,
                );
                let mut listening = server.ready();
                let ready_tx = ready_tx.clone();
                tokio::spawn(async move {
                    if !*listening.borrow() {
                        let _ = listening.changed().await;
                    }
                    let _ = ready_tx.send(true);
                });

                let serve_result = server.run().await.map_err(boxed_error);
                let shutdown_result = app.shutdown().await.map_err(boxed_error);
                match (serve_result, shutdown_result) {
                    (Ok(()), Ok(())) => Ok(()),
                    (Err(error), _) => Err(error),
                    (Ok(()), Err(error)) => Err(error),
                }
            }
        }),
        ready_rx,
    )
}

fn happy_path_client_host(
    done: watch::Sender<bool>,
    ready: watch::Receiver<bool>,
) -> SimulationHost {
    SimulationHost::new("client", move || {
        let done = done.clone();
        let ready = ready.clone();
        async move {
            let client = SimulatedHttpClient::new(SERVER_HOST, TELEMETRY_SERVER_PORT);
            let readings = sample_readings();
            let oracle = oracle_from_readings(&readings);
            wait_for_ready(ready).await?;

            let (status, ingested): (StatusCode, IngestReadingsResponse) = client
                .request_json(
                    Method::POST,
                    "/readings",
                    Some(&IngestReadingsRequest {
                        readings: readings.clone(),
                    }),
                )
                .await?;
            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(ingested.received_readings, readings.len());
            assert_eq!(ingested.accepted_readings, readings.len());
            assert_eq!(ingested.updated_devices.len(), 2);

            let expected_state = oracle.latest_state("device-01").expect("device-01 state");
            let (status, state): (StatusCode, DeviceStateRecord) = client
                .request_json::<IngestReadingsRequest, DeviceStateRecord>(
                    Method::GET,
                    "/devices/device-01/state",
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                state,
                DeviceStateRecord {
                    device_id: expected_state.device_id,
                    latest_reading_at_ms: expected_state.latest_reading_at_ms,
                    temperature_c: expected_state.temperature_c,
                    humidity_pct: expected_state.humidity_pct,
                    battery_mv: expected_state.battery_mv,
                    alert_active: expected_state.alert_active,
                }
            );

            let expected_scan = TelemetryScanResponse {
                device_id: "device-01".to_string(),
                start_ms: 100,
                end_ms: 301,
                columns: vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
                only_alerts: true,
                rows: to_scan_rows(oracle.scan(&OracleScanRequest {
                    window: TelemetryWindow {
                        device_id: "device-01".to_string(),
                        start_ms: 100,
                        end_ms: 301,
                    },
                    columns: vec![
                        TelemetryProjectionColumn::TemperatureC,
                        TelemetryProjectionColumn::AlertActive,
                    ],
                    only_alerts: true,
                })),
                execution: None,
            };
            let (status, scan): (StatusCode, TelemetryScanResponse) = client
                .request_json::<IngestReadingsRequest, TelemetryScanResponse>(
                    Method::GET,
                    "/devices/device-01/readings?start_ms=100&end_ms=301&columns=temperature_c,alert_active&only_alerts=true",
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(scan, expected_scan);

            let expected_summary = oracle.summarize(&TelemetryWindow {
                device_id: "device-01".to_string(),
                start_ms: 100,
                end_ms: 301,
            });
            let (status, summary): (StatusCode, TelemetrySummaryResponse) = client
                .request_json::<IngestReadingsRequest, TelemetrySummaryResponse>(
                    Method::GET,
                    "/devices/device-01/summary?start_ms=100&end_ms=301",
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                summary,
                TelemetrySummaryResponse {
                    device_id: expected_summary.device_id,
                    start_ms: expected_summary.start_ms,
                    end_ms: expected_summary.end_ms,
                    reading_count: expected_summary.reading_count,
                    alert_count: expected_summary.alert_count,
                    min_temperature_c: expected_summary.min_temperature_c,
                    max_temperature_c: expected_summary.max_temperature_c,
                    average_temperature_c: expected_summary.average_temperature_c,
                }
            );

            let _ = done.send(true);
            Ok(())
        }
    })
}

async fn wait_for_done(mut done: watch::Receiver<bool>) -> Result<(), std::io::Error> {
    if !*done.borrow() {
        done.changed()
            .await
            .map_err(|_| std::io::Error::other("simulation client did not finish"))?;
    }
    Ok(())
}

async fn wait_for_ready(mut ready: watch::Receiver<bool>) -> Result<(), std::io::Error> {
    if !*ready.borrow() {
        ready
            .changed()
            .await
            .map_err(|_| std::io::Error::other("simulation server did not become ready"))?;
    }
    Ok(())
}

fn stub_dependencies(
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> DbDependencies {
    DbDependencies::new(
        file_system,
        object_store,
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(41)),
    )
}

async fn open_app(
    config: DbConfig,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
    profile: TelemetryExampleProfile,
) -> Result<(Db, TelemetryApp), BoxError> {
    let db = Db::open(config, stub_dependencies(file_system, object_store)).await?;
    let app = TelemetryApp::open(db.clone(), profile).await?;
    Ok((db, app))
}

async fn overwrite_file(
    file_system: &StubFileSystem,
    path: &str,
    bytes: &[u8],
) -> Result<(), BoxError> {
    let handle = file_system
        .open(
            path,
            OpenOptions {
                create: true,
                write: true,
                truncate: true,
                ..OpenOptions::default()
            },
        )
        .await?;
    file_system.write_at(&handle, 0, bytes).await?;
    file_system.sync(&handle).await?;
    Ok(())
}

#[test]
fn telemetry_api_simulation_supports_ingest_state_scan_and_summary() -> turmoil::Result {
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (server, ready_rx) = telemetry_server_host(
        0x5858,
        "/telemetry-api-happy",
        "telemetry-api-happy",
        TelemetryExampleProfile::Base,
        shutdown_rx,
    );

    SeededSimulationRunner::new(0x5858)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .with_host(server)
        .with_host(happy_path_client_host(done_tx, ready_rx))
        .run_with(move |_context| async move {
            wait_for_done(done_rx).await.map_err(boxed_error)?;
            shutdown_tx.send_replace(true);
            Ok(())
        })
}

#[test]
fn telemetry_api_simulation_restarts_cleanly_under_low_budgets_and_reuses_remote_cache()
-> turmoil::Result {
    SeededSimulationRunner::new(0x5859)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .run_with(|context| async move {
            let readings = larger_reading_batch(96);
            let oracle = oracle_from_readings(&readings);
            let window_end = 1_000 + readings.len() as u64 * 10;
            let mut config = telemetry_db_config(
                "/telemetry-api-low-budget",
                "telemetry-api-low-budget",
                TelemetryExampleProfile::Base,
            );
            if let StorageConfig::Tiered(TieredStorageConfig {
                max_local_bytes, ..
            }) = &mut config.storage
            {
                *max_local_bytes = 1_024;
            }
            config.hybrid_read.raw_segment_cache_bytes = 4 * 1024;
            config.hybrid_read.decoded_metadata_cache_entries = 4;
            config.hybrid_read.decoded_column_cache_entries = 8;

            let db = context.open_db(config.clone()).await?;
            let app = TelemetryApp::open(db.clone(), TelemetryExampleProfile::Base).await?;
            app.state()
                .ingest_readings(IngestReadingsRequest {
                    readings: readings.clone(),
                })
                .await
                .map_err(boxed_error)?;

            while db.run_next_offload().await.map_err(boxed_error)? {}

            let before_restart = app
                .state()
                .scan_window(
                    "device-01",
                    1_000,
                    window_end,
                    vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
                    false,
                    false,
                )
                .await
                .map_err(boxed_error)?;
            assert_eq!(
                before_restart.rows,
                to_scan_rows(oracle.scan(&OracleScanRequest {
                    window: TelemetryWindow {
                        device_id: "device-01".to_string(),
                        start_ms: 1_000,
                        end_ms: window_end,
                    },
                    columns: vec![
                        TelemetryProjectionColumn::TemperatureC,
                        TelemetryProjectionColumn::AlertActive,
                    ],
                    only_alerts: false,
                })),
            );

            let point_key = format!("device:device-01:{:020}", 1_000).into_bytes();
            let warmed_value = app
                .tables()
                .sensor_readings_raw()
                .read(point_key.clone())
                .await
                .map_err(boxed_error)?;
            assert!(warmed_value.is_some());
            app.shutdown().await.map_err(boxed_error)?;
            drop(db);

            let reopened = context.open_db(config.clone()).await?;
            let reopened_app =
                TelemetryApp::open(reopened.clone(), TelemetryExampleProfile::Base).await?;
            let state = reopened_app
                .state()
                .get_device_state("device-01")
                .await
                .map_err(boxed_error)?
                .expect("device-01 state after restart");
            let expected = oracle.latest_state("device-01").expect("oracle state");
            assert_eq!(state.latest_reading_at_ms, expected.latest_reading_at_ms);

            let summary = reopened_app
                .state()
                .reading_summary("device-01", 1_000, window_end)
                .await
                .map_err(boxed_error)?;
            let expected_summary = oracle.summarize(&TelemetryWindow {
                device_id: "device-01".to_string(),
                start_ms: 1_000,
                end_ms: window_end,
            });
            assert_eq!(summary.reading_count, expected_summary.reading_count);
            assert_eq!(summary.alert_count, expected_summary.alert_count);
            assert_eq!(
                summary.min_temperature_c,
                expected_summary.min_temperature_c
            );
            assert_eq!(
                summary.max_temperature_c,
                expected_summary.max_temperature_c
            );

            context
                .object_store()
                .inject_failure(ObjectStoreFaultSpec::Timeout {
                    operation: terracedb::ObjectStoreOperation::Get,
                    target_prefix: "telemetry-api-low-budget".to_string(),
                })
                .await
                .map_err(boxed_error)?;
            context
                .object_store()
                .inject_failure(ObjectStoreFaultSpec::Timeout {
                    operation: terracedb::ObjectStoreOperation::GetRange,
                    target_prefix: "telemetry-api-low-budget".to_string(),
                })
                .await
                .map_err(boxed_error)?;
            let reopened_value = reopened_app
                .tables()
                .sensor_readings_raw()
                .read(point_key)
                .await
                .map_err(boxed_error)?;
            assert_eq!(reopened_value, warmed_value);

            reopened_app.shutdown().await.map_err(boxed_error)?;
            Ok(())
        })
}

#[tokio::test]
async fn accelerated_profile_matches_base_results_and_disabled_accelerants_do_not_change_answers()
-> Result<(), BoxError> {
    let readings = sample_readings();
    let oracle = oracle_from_readings(&readings);

    let base_fs = Arc::new(StubFileSystem::default());
    let base_store = Arc::new(StubObjectStore::default());
    let (base_db, base_app) = open_app(
        telemetry_db_config(
            "/telemetry-base-equivalence",
            "telemetry-base-equivalence",
            TelemetryExampleProfile::Base,
        ),
        base_fs,
        base_store,
        TelemetryExampleProfile::Base,
    )
    .await?;
    base_app
        .state()
        .ingest_readings(IngestReadingsRequest {
            readings: readings.clone(),
        })
        .await?;
    let expected_scan = base_app
        .state()
        .scan_window(
            "device-01",
            100,
            301,
            vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
            true,
            false,
        )
        .await?;
    let expected_summary = base_app
        .state()
        .reading_summary("device-01", 100, 301)
        .await?;
    base_app.shutdown().await?;
    drop(base_db);

    let accel_fs = Arc::new(StubFileSystem::default());
    let accel_store = Arc::new(StubObjectStore::default());
    let (accel_db, accel_app) = open_app(
        telemetry_db_config(
            "/telemetry-accelerated-equivalence",
            "telemetry-accelerated-equivalence",
            TelemetryExampleProfile::Accelerated,
        ),
        accel_fs,
        accel_store,
        TelemetryExampleProfile::Accelerated,
    )
    .await?;
    accel_app
        .state()
        .ingest_readings(IngestReadingsRequest {
            readings: readings.clone(),
        })
        .await?;
    accel_db.flush().await?;
    for _ in 0..3 {
        let _ = accel_app
            .state()
            .scan_window(
                "device-01",
                100,
                301,
                vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
                false,
                false,
            )
            .await?;
    }
    let _ = accel_db.run_next_compaction().await?;
    let probe = accel_app
        .tables()
        .sensor_readings_raw()
        .probe_skip_indexes(SkipIndexProbe::FieldEquals {
            field: "alert_active".to_string(),
            value: FieldValue::Bool(true),
        })
        .await?;
    assert!(!probe.is_empty());
    let debug_scan = accel_app
        .state()
        .scan_window(
            "device-01",
            100,
            301,
            vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
            true,
            true,
        )
        .await?;
    assert_eq!(debug_scan.rows, expected_scan.rows);
    let execution = debug_scan
        .execution
        .expect("debug scan should include execution");
    let columnar = execution
        .columnar
        .expect("telemetry history scan should report columnar execution");
    assert_eq!(columnar.rows_returned, debug_scan.rows.len());
    assert!(columnar.sstables_considered >= 1 || columnar.rows_evaluated >= debug_scan.rows.len());
    assert_eq!(
        accel_app
            .state()
            .scan_window(
                "device-01",
                100,
                301,
                vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
                true,
                false,
            )
            .await?,
        expected_scan,
    );
    assert_eq!(
        accel_app
            .state()
            .reading_summary("device-01", 100, 301)
            .await?,
        expected_summary,
    );
    accel_app.shutdown().await?;

    let fallback_fs = Arc::new(StubFileSystem::default());
    let fallback_store = Arc::new(StubObjectStore::default());
    let fallback_path = "/telemetry-disabled-fallback";
    let fallback_prefix = "telemetry-disabled-fallback";
    let (base_written_db, base_written_app) = open_app(
        telemetry_db_config(
            fallback_path,
            fallback_prefix,
            TelemetryExampleProfile::Base,
        ),
        fallback_fs.clone(),
        fallback_store.clone(),
        TelemetryExampleProfile::Base,
    )
    .await?;
    base_written_app
        .state()
        .ingest_readings(IngestReadingsRequest { readings })
        .await?;
    base_written_app.shutdown().await?;
    drop(base_written_db);

    let (_fallback_db, fallback_app) = open_app(
        telemetry_db_config(
            fallback_path,
            fallback_prefix,
            TelemetryExampleProfile::Accelerated,
        ),
        fallback_fs,
        fallback_store,
        TelemetryExampleProfile::Accelerated,
    )
    .await?;
    assert_eq!(
        fallback_app
            .state()
            .scan_window(
                "device-01",
                100,
                301,
                vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
                true,
                false,
            )
            .await?,
        TelemetryScanResponse {
            device_id: "device-01".to_string(),
            start_ms: 100,
            end_ms: 301,
            columns: vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
            only_alerts: true,
            rows: to_scan_rows(oracle.scan(&OracleScanRequest {
                window: TelemetryWindow {
                    device_id: "device-01".to_string(),
                    start_ms: 100,
                    end_ms: 301,
                },
                columns: vec![
                    TelemetryProjectionColumn::TemperatureC,
                    TelemetryProjectionColumn::AlertActive,
                ],
                only_alerts: true,
            })),
            execution: None,
        }
    );
    Ok(())
}

#[tokio::test]
async fn missing_projection_sidecars_fall_back_to_base_results() -> Result<(), BoxError> {
    let readings = sample_readings();
    let oracle = oracle_from_readings(&readings);
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/telemetry-missing-sidecar";
    let prefix = "telemetry-missing-sidecar";

    let mut config = telemetry_db_config(path, prefix, TelemetryExampleProfile::Accelerated);
    config.hybrid_read.compact_to_wide_promotion_enabled = false;
    let (_db, app) = open_app(
        config.clone(),
        file_system.clone(),
        object_store.clone(),
        TelemetryExampleProfile::Accelerated,
    )
    .await?;
    app.state()
        .ingest_readings(IngestReadingsRequest {
            readings: readings.clone(),
        })
        .await?;
    app.shutdown().await?;

    let files = file_system.list(path).await?;
    let projection_sidecar = files
        .iter()
        .find(|path| path.contains(".projection."))
        .cloned()
        .expect("projection sidecar file");
    file_system.delete(&projection_sidecar).await?;

    let (_reopened_db, reopened_app) = open_app(
        config,
        file_system,
        object_store,
        TelemetryExampleProfile::Accelerated,
    )
    .await?;
    let scan = reopened_app
        .state()
        .scan_window(
            "device-01",
            100,
            301,
            vec![TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
            true,
            false,
        )
        .await?;
    assert_eq!(
        scan.rows,
        to_scan_rows(oracle.scan(&OracleScanRequest {
            window: TelemetryWindow {
                device_id: "device-01".to_string(),
                start_ms: 100,
                end_ms: 301,
            },
            columns: vec![
                TelemetryProjectionColumn::TemperatureC,
                TelemetryProjectionColumn::AlertActive,
            ],
            only_alerts: true,
        })),
    );
    Ok(())
}

#[tokio::test]
async fn corrupt_projection_sidecars_fall_back_to_base_results() -> Result<(), BoxError> {
    let readings = sample_readings();
    let oracle = oracle_from_readings(&readings);
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let path = "/telemetry-corrupt-sidecar";
    let prefix = "telemetry-corrupt-sidecar";

    let mut config = telemetry_db_config(path, prefix, TelemetryExampleProfile::Accelerated);
    config.hybrid_read.compact_to_wide_promotion_enabled = false;
    let (_db, app) = open_app(
        config.clone(),
        file_system.clone(),
        object_store.clone(),
        TelemetryExampleProfile::Accelerated,
    )
    .await?;
    app.state()
        .ingest_readings(IngestReadingsRequest { readings })
        .await?;
    app.shutdown().await?;

    let files = file_system.list(path).await?;
    let projection_sidecar = files
        .iter()
        .find(|path| path.contains(".projection."))
        .cloned()
        .expect("projection sidecar file");
    overwrite_file(file_system.as_ref(), &projection_sidecar, b"{broken-json").await?;

    let (_reopened_db, reopened_app) = open_app(
        config,
        file_system,
        object_store,
        TelemetryExampleProfile::Accelerated,
    )
    .await?;
    let summary = reopened_app
        .state()
        .reading_summary("device-01", 100, 301)
        .await?;
    let expected_summary = oracle.summarize(&TelemetryWindow {
        device_id: "device-01".to_string(),
        start_ms: 100,
        end_ms: 301,
    });
    assert_eq!(summary.reading_count, expected_summary.reading_count);
    assert_eq!(summary.alert_count, expected_summary.alert_count);
    assert_eq!(
        summary.min_temperature_c,
        expected_summary.min_temperature_c
    );
    assert_eq!(
        summary.max_temperature_c,
        expected_summary.max_temperature_c
    );
    Ok(())
}
