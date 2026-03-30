use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use opentelemetry::{
    KeyValue,
    logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity},
    metrics::MeterProvider as _,
    trace::{TraceContextExt, TracerProvider as _},
};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{
        BatchConfigBuilder as LogBatchConfigBuilder, BatchLogProcessor, InMemoryLogExporter,
        SdkLogger, SdkLoggerProvider, in_memory_exporter::LogDataWithResource,
    },
    metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider, data::ResourceMetrics},
    propagation::TraceContextPropagator,
    trace::{
        BatchConfigBuilder as TraceBatchConfigBuilder, BatchSpanProcessor, InMemorySpanExporter,
        SdkTracerProvider, SpanData,
    },
};
use parking_lot::Mutex;
use terracedb::{Db, PendingWorkType};
use terracedb_projections::{ProjectionRuntime, ProjectionTelemetrySnapshot};
use terracedb_workflows::{
    WorkflowError, WorkflowHandler, WorkflowRuntime, WorkflowTelemetrySnapshot,
};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};
use tracing::{
    Dispatch, Event, Subscriber,
    field::{Field, Visit},
    instrument::{WithDispatch, WithSubscriber},
};
use tracing_opentelemetry::{OpenTelemetrySpanExt, layer as otel_layer};
use tracing_subscriber::{
    Layer,
    layer::Context as LayerContext,
    prelude::*,
    registry::{LookupSpan, Registry},
};

#[derive(Clone, Debug)]
pub enum TelemetryMode {
    Disabled,
    Test,
    Otlp(OtlpConfig),
}

#[derive(Clone, Debug)]
pub struct OtlpConfig {
    pub endpoint: String,
    pub protocol: OtlpProtocol,
    pub headers: BTreeMap<String, String>,
    pub timeout: Duration,
    pub metric_export_interval: Duration,
    pub metric_snapshot_interval: Duration,
    pub trace_batch: BatchSettings,
    pub log_batch: BatchSettings,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:4317".to_string(),
            protocol: OtlpProtocol::Grpc,
            headers: BTreeMap::new(),
            timeout: Duration::from_secs(5),
            metric_export_interval: Duration::from_secs(30),
            metric_snapshot_interval: Duration::from_secs(5),
            trace_batch: BatchSettings::default(),
            log_batch: BatchSettings::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OtlpProtocol {
    #[default]
    Grpc,
    HttpBinary,
}

#[derive(Clone, Debug)]
pub struct BatchSettings {
    pub max_queue_size: usize,
    pub max_export_batch_size: usize,
    pub scheduled_delay: Duration,
    pub export_timeout: Duration,
}

impl Default for BatchSettings {
    fn default() -> Self {
        Self {
            max_queue_size: 2048,
            max_export_batch_size: 512,
            scheduled_delay: Duration::from_secs(5),
            export_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub struct TerracedbTelemetryBuilder {
    service_name: String,
    service_version: String,
    deployment_environment: Option<String>,
    resource_attributes: Vec<KeyValue>,
    mode: TelemetryMode,
}

impl TerracedbTelemetryBuilder {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            deployment_environment: None,
            resource_attributes: Vec::new(),
            mode: TelemetryMode::Disabled,
        }
    }

    pub fn with_service_version(mut self, service_version: impl Into<String>) -> Self {
        self.service_version = service_version.into();
        self
    }

    pub fn with_environment(mut self, deployment_environment: impl Into<String>) -> Self {
        self.deployment_environment = Some(deployment_environment.into());
        self
    }

    pub fn with_resource_attribute(
        mut self,
        key: impl Into<opentelemetry::Key>,
        value: impl Into<opentelemetry::Value>,
    ) -> Self {
        self.resource_attributes
            .push(KeyValue::new(key.into(), value.into()));
        self
    }

    pub fn with_mode(mut self, mode: TelemetryMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn build(self) -> Result<TerracedbTelemetry, TelemetryError> {
        build_telemetry(self)
    }
}

#[derive(Debug)]
pub struct TerracedbTelemetry {
    dispatch: Dispatch,
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    metrics: Option<Arc<MetricReporter>>,
    background_metrics: Option<BackgroundMetricTask>,
    test_exports: Option<TestTelemetryExports>,
}

impl TerracedbTelemetry {
    pub fn builder(service_name: impl Into<String>) -> TerracedbTelemetryBuilder {
        TerracedbTelemetryBuilder::new(service_name)
    }

    pub fn dispatch(&self) -> Dispatch {
        self.dispatch.clone()
    }

    pub fn in_scope<R>(&self, f: impl FnOnce() -> R) -> R {
        tracing::dispatcher::with_default(&self.dispatch, f)
    }

    pub fn with_subscriber<F>(&self, future: F) -> WithDispatch<F>
    where
        F: Future,
    {
        future.with_subscriber(self.dispatch.clone())
    }

    pub fn observe_db(&self, db: Db) {
        if let Some(metrics) = &self.metrics {
            metrics.observe_db(db);
        }
    }

    pub fn observe_projection_runtime(&self, runtime: ProjectionRuntime) {
        if let Some(metrics) = &self.metrics {
            metrics.observe_projection_runtime(runtime);
        }
    }

    pub fn observe_workflow_runtime<H>(&self, runtime: WorkflowRuntime<H>)
    where
        H: WorkflowHandler + 'static,
    {
        if let Some(metrics) = &self.metrics {
            metrics.observe_workflow_runtime(runtime);
        }
    }

    pub async fn collect_metrics(&self) -> Result<(), TelemetryError> {
        if let Some(metrics) = &self.metrics {
            metrics.collect().await?;
        }
        Ok(())
    }

    pub async fn force_flush(&self) -> Result<(), TelemetryError> {
        self.collect_metrics().await?;
        if let Some(provider) = &self.logger_provider {
            provider.force_flush().map_err(TelemetryError::flush)?;
        }
        if let Some(provider) = &self.tracer_provider {
            provider.force_flush().map_err(TelemetryError::flush)?;
        }
        if let Some(provider) = &self.meter_provider {
            provider.force_flush().map_err(TelemetryError::flush)?;
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), TelemetryError> {
        if let Some(background) = self.background_metrics.take() {
            background.shutdown().await;
        }
        self.force_flush().await?;
        if let Some(provider) = self.logger_provider.take() {
            provider.shutdown().map_err(TelemetryError::shutdown)?;
        }
        if let Some(provider) = self.tracer_provider.take() {
            provider.shutdown().map_err(TelemetryError::shutdown)?;
        }
        if let Some(provider) = self.meter_provider.take() {
            provider.shutdown().map_err(TelemetryError::shutdown)?;
        }
        Ok(())
    }

    pub fn test_exports(&self) -> Option<TestTelemetryExports> {
        self.test_exports.clone()
    }
}

#[derive(Clone)]
pub struct TestTelemetryExports {
    span_exporter: InMemorySpanExporter,
    log_exporter: InMemoryLogExporter,
    metric_exporter: InMemoryMetricExporter,
}

impl TestTelemetryExports {
    pub fn spans(&self) -> Result<Vec<SpanData>, TelemetryError> {
        self.span_exporter
            .get_finished_spans()
            .map_err(TelemetryError::test_export)
    }

    pub fn logs(&self) -> Result<Vec<LogDataWithResource>, TelemetryError> {
        self.log_exporter
            .get_emitted_logs()
            .map_err(TelemetryError::test_export)
    }

    pub fn metrics(&self) -> Result<Vec<ResourceMetrics>, TelemetryError> {
        self.metric_exporter
            .get_finished_metrics()
            .map_err(TelemetryError::test_export)
    }
}

impl std::fmt::Debug for TestTelemetryExports {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestTelemetryExports").finish()
    }
}

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("telemetry setup failed: {0}")]
    Setup(String),
    #[error("telemetry flush failed: {0}")]
    Flush(String),
    #[error("telemetry shutdown failed: {0}")]
    Shutdown(String),
    #[error("telemetry collection failed: {0}")]
    Collection(String),
    #[error("telemetry test exporter failed: {0}")]
    TestExport(String),
}

impl TelemetryError {
    fn setup(error: impl std::fmt::Display) -> Self {
        Self::Setup(error.to_string())
    }

    fn flush(error: impl std::fmt::Display) -> Self {
        Self::Flush(error.to_string())
    }

    fn shutdown(error: impl std::fmt::Display) -> Self {
        Self::Shutdown(error.to_string())
    }

    fn collection(error: impl std::fmt::Display) -> Self {
        Self::Collection(error.to_string())
    }

    fn test_export(error: impl std::fmt::Display) -> Self {
        Self::TestExport(error.to_string())
    }
}

fn build_telemetry(
    builder: TerracedbTelemetryBuilder,
) -> Result<TerracedbTelemetry, TelemetryError> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let resource = build_resource(&builder);
    match builder.mode {
        TelemetryMode::Disabled => {
            let dispatch = Dispatch::new(tracing_subscriber::registry());
            Ok(TerracedbTelemetry {
                dispatch,
                tracer_provider: None,
                logger_provider: None,
                meter_provider: None,
                metrics: None,
                background_metrics: None,
                test_exports: None,
            })
        }
        TelemetryMode::Test => {
            let span_exporter = InMemorySpanExporter::default();
            let tracer_provider = SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_span_processor(BatchSpanProcessor::builder(span_exporter.clone()).build())
                .build();
            let tracer = tracer_provider.tracer("terracedb");

            let log_exporter = InMemoryLogExporter::default();
            let logger_provider = SdkLoggerProvider::builder()
                .with_resource(resource.clone())
                .with_simple_exporter(log_exporter.clone())
                .build();
            let log_layer = TerracedbOtelLogLayer::new(logger_provider.logger("terracedb"));

            let metric_exporter = InMemoryMetricExporter::default();
            let meter_provider = SdkMeterProvider::builder()
                .with_resource(resource)
                .with_reader(
                    PeriodicReader::builder(metric_exporter.clone())
                        .with_interval(Duration::from_secs(24 * 60 * 60))
                        .build(),
                )
                .build();

            let metrics = Arc::new(MetricReporter::new(meter_provider.clone()));
            let dispatch = build_dispatch(Some(tracer), Some(log_layer));
            Ok(TerracedbTelemetry {
                dispatch,
                tracer_provider: Some(tracer_provider),
                logger_provider: Some(logger_provider),
                meter_provider: Some(meter_provider),
                metrics: Some(metrics),
                background_metrics: None,
                test_exports: Some(TestTelemetryExports {
                    span_exporter,
                    log_exporter,
                    metric_exporter,
                }),
            })
        }
        TelemetryMode::Otlp(config) => {
            let span_exporter = build_span_exporter(&config)?;
            let trace_batch = TraceBatchConfigBuilder::default()
                .with_max_queue_size(config.trace_batch.max_queue_size)
                .with_max_export_batch_size(config.trace_batch.max_export_batch_size)
                .with_scheduled_delay(config.trace_batch.scheduled_delay)
                .with_max_export_timeout(config.trace_batch.export_timeout)
                .build();
            let tracer_provider = SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_span_processor(
                    BatchSpanProcessor::builder(span_exporter)
                        .with_batch_config(trace_batch)
                        .build(),
                )
                .build();
            let tracer = tracer_provider.tracer("terracedb");

            let log_exporter = build_log_exporter(&config)?;
            let log_batch = LogBatchConfigBuilder::default()
                .with_max_queue_size(config.log_batch.max_queue_size)
                .with_max_export_batch_size(config.log_batch.max_export_batch_size)
                .with_scheduled_delay(config.log_batch.scheduled_delay)
                .with_max_export_timeout(config.log_batch.export_timeout)
                .build();
            let logger_provider = SdkLoggerProvider::builder()
                .with_resource(resource.clone())
                .with_log_processor(
                    BatchLogProcessor::builder(log_exporter)
                        .with_batch_config(log_batch)
                        .build(),
                )
                .build();
            let log_layer = TerracedbOtelLogLayer::new(logger_provider.logger("terracedb"));

            let metric_exporter = build_metric_exporter(&config)?;
            let meter_provider = SdkMeterProvider::builder()
                .with_resource(resource)
                .with_reader(
                    PeriodicReader::builder(metric_exporter)
                        .with_interval(config.metric_export_interval)
                        .build(),
                )
                .build();

            let metrics = Arc::new(MetricReporter::new(meter_provider.clone()));
            let background_metrics = if config.metric_snapshot_interval.is_zero() {
                None
            } else {
                Some(BackgroundMetricTask::spawn(
                    metrics.clone(),
                    config.metric_snapshot_interval,
                ))
            };
            let dispatch = build_dispatch(Some(tracer), Some(log_layer));

            Ok(TerracedbTelemetry {
                dispatch,
                tracer_provider: Some(tracer_provider),
                logger_provider: Some(logger_provider),
                meter_provider: Some(meter_provider),
                metrics: Some(metrics),
                background_metrics,
                test_exports: None,
            })
        }
    }
}

fn build_resource(builder: &TerracedbTelemetryBuilder) -> Resource {
    let mut attributes = vec![
        KeyValue::new("service.version", builder.service_version.clone()),
        KeyValue::new("process.pid", i64::from(std::process::id())),
        KeyValue::new(
            "process.command",
            std::env::args()
                .next()
                .unwrap_or_else(|| "unknown".to_string()),
        ),
        KeyValue::new("process.runtime.name", "tokio"),
        KeyValue::new("telemetry.sdk.language", "rust"),
    ];
    if let Ok(executable) = std::env::current_exe()
        && let Some(name) = executable.file_name().and_then(|segment| segment.to_str())
    {
        attributes.push(KeyValue::new("process.executable.name", name.to_string()));
        attributes.push(KeyValue::new(
            "process.executable.path",
            executable.to_string_lossy().into_owned(),
        ));
    }
    if let Some(environment) = &builder.deployment_environment {
        attributes.push(KeyValue::new(
            "deployment.environment.name",
            environment.clone(),
        ));
    }
    attributes.extend(builder.resource_attributes.clone());

    Resource::builder_empty()
        .with_service_name(builder.service_name.clone())
        .with_attributes(attributes)
        .build()
}

fn build_dispatch(
    tracer: Option<opentelemetry_sdk::trace::Tracer>,
    log_layer: Option<TerracedbOtelLogLayer>,
) -> Dispatch {
    let trace_layer = tracer.map(|tracer| otel_layer().with_tracer(tracer));
    Dispatch::new(Registry::default().with(trace_layer).with(log_layer))
}

fn build_span_exporter(
    config: &OtlpConfig,
) -> Result<opentelemetry_otlp::SpanExporter, TelemetryError> {
    match config.protocol {
        OtlpProtocol::Grpc => {
            let metadata = build_grpc_metadata(&config.headers)?;
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(config.endpoint.clone())
                .with_timeout(config.timeout)
                .with_metadata(metadata)
                .build()
                .map_err(TelemetryError::setup)
        }
        OtlpProtocol::HttpBinary => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .with_headers(config.headers.clone().into_iter().collect())
            .build()
            .map_err(TelemetryError::setup),
    }
}

fn build_log_exporter(
    config: &OtlpConfig,
) -> Result<opentelemetry_otlp::LogExporter, TelemetryError> {
    match config.protocol {
        OtlpProtocol::Grpc => {
            let metadata = build_grpc_metadata(&config.headers)?;
            opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .with_endpoint(config.endpoint.clone())
                .with_timeout(config.timeout)
                .with_metadata(metadata)
                .build()
                .map_err(TelemetryError::setup)
        }
        OtlpProtocol::HttpBinary => opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .with_headers(config.headers.clone().into_iter().collect())
            .build()
            .map_err(TelemetryError::setup),
    }
}

fn build_metric_exporter(
    config: &OtlpConfig,
) -> Result<opentelemetry_otlp::MetricExporter, TelemetryError> {
    match config.protocol {
        OtlpProtocol::Grpc => {
            let metadata = build_grpc_metadata(&config.headers)?;
            opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(config.endpoint.clone())
                .with_timeout(config.timeout)
                .with_metadata(metadata)
                .build()
                .map_err(TelemetryError::setup)
        }
        OtlpProtocol::HttpBinary => opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .with_headers(config.headers.clone().into_iter().collect())
            .build()
            .map_err(TelemetryError::setup),
    }
}

fn build_grpc_metadata(
    headers: &BTreeMap<String, String>,
) -> Result<tonic::metadata::MetadataMap, TelemetryError> {
    let mut metadata = tonic::metadata::MetadataMap::new();
    for (key, value) in headers {
        let metadata_key =
            tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(key.as_bytes())
                .map_err(TelemetryError::setup)?;
        let metadata_value = value.parse().map_err(TelemetryError::setup)?;
        metadata.insert(metadata_key, metadata_value);
    }
    Ok(metadata)
}

#[derive(Debug)]
struct BackgroundMetricTask {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl BackgroundMetricTask {
    fn spawn(metrics: Arc<MetricReporter>, interval: Duration) -> Self {
        let (shutdown, mut rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = rx.changed() => {
                        if changed.is_err() || *rx.borrow() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(interval) => {
                        let _ = metrics.collect().await;
                    }
                }
            }
        });
        Self { shutdown, task }
    }

    async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

#[derive(Debug)]
struct MetricReporter {
    dbs: Mutex<Vec<Db>>,
    projections: Mutex<Vec<ProjectionRuntime>>,
    workflows: Mutex<Vec<WorkflowObserver>>,
    instruments: MetricInstruments,
}

impl MetricReporter {
    fn new(meter_provider: SdkMeterProvider) -> Self {
        let meter = meter_provider.meter("terracedb");
        Self {
            dbs: Mutex::new(Vec::new()),
            projections: Mutex::new(Vec::new()),
            workflows: Mutex::new(Vec::new()),
            instruments: MetricInstruments::new(meter),
        }
    }

    fn observe_db(&self, db: Db) {
        self.dbs.lock().push(db);
    }

    fn observe_projection_runtime(&self, runtime: ProjectionRuntime) {
        self.projections.lock().push(runtime);
    }

    fn observe_workflow_runtime<H>(&self, runtime: WorkflowRuntime<H>)
    where
        H: WorkflowHandler + 'static,
    {
        let observer = WorkflowObserver {
            collect: Arc::new(move || {
                let runtime = runtime.clone();
                Box::pin(async move { runtime.telemetry_snapshot().await })
            }),
        };
        self.workflows.lock().push(observer);
    }

    async fn collect(&self) -> Result<(), TelemetryError> {
        let started = Instant::now();
        let dbs = self.dbs.lock().clone();
        let projections = self.projections.lock().clone();
        let workflows = self.workflows.lock().clone();

        for db in dbs {
            let attrs = db_metric_attributes(&db);
            let pending_work = db.pending_work().await;
            let scheduler_backlog = pending_work.len() as u64;
            let compaction_debt_bytes = pending_work
                .iter()
                .filter(|work| work.work_type == PendingWorkType::Compaction)
                .map(|work| work.estimated_bytes)
                .sum::<u64>();
            let visible_sequence = db.current_sequence().get();
            let durable_sequence = db.current_durable_sequence().get();
            self.instruments
                .db_visible_sequence
                .record(visible_sequence, &attrs);
            self.instruments
                .db_durable_sequence
                .record(durable_sequence, &attrs);
            self.instruments
                .db_durable_visible_lag
                .record(visible_sequence.saturating_sub(durable_sequence), &attrs);
            self.instruments
                .db_scheduler_backlog
                .record(scheduler_backlog, &attrs);
            self.instruments
                .db_compaction_debt_bytes
                .record(compaction_debt_bytes, &attrs);
        }

        for runtime in projections {
            for snapshot in runtime.telemetry_snapshots() {
                let attrs = projection_metric_attributes(&snapshot);
                self.instruments
                    .projection_frontier_sequence
                    .record(snapshot.frontier_sequence.get(), &attrs);
                self.instruments.projection_lag.record(snapshot.lag, &attrs);
            }
        }

        for workflow in workflows {
            let snapshot = (workflow.collect)()
                .await
                .map_err(TelemetryError::collection)?;
            let attrs = workflow_metric_attributes(&snapshot);
            self.instruments
                .workflow_inbox_depth
                .record(snapshot.inbox_depth as u64, &attrs);
            self.instruments
                .workflow_timer_depth
                .record(snapshot.timer_depth as u64, &attrs);
            self.instruments
                .workflow_outbox_depth
                .record(snapshot.outbox_depth as u64, &attrs);
            self.instruments
                .workflow_in_flight_instances
                .record(snapshot.in_flight_instances as u64, &attrs);
            for source in &snapshot.source_lags {
                let mut source_attrs = attrs.clone();
                source_attrs.push(KeyValue::new(
                    terracedb::telemetry_attrs::SOURCE_TABLE,
                    source.source_table.clone(),
                ));
                self.instruments
                    .workflow_source_lag
                    .record(source.lag, &source_attrs);
            }
        }

        self.instruments.collections.add(1, &[]);
        self.instruments
            .collection_duration_ms
            .record(started.elapsed().as_secs_f64() * 1_000.0, &[]);
        Ok(())
    }
}

type WorkflowSnapshotFuture =
    Pin<Box<dyn Future<Output = Result<WorkflowTelemetrySnapshot, WorkflowError>> + Send>>;
type WorkflowSnapshotCollector = dyn Fn() -> WorkflowSnapshotFuture + Send + Sync;

#[derive(Clone)]
struct WorkflowObserver {
    collect: Arc<WorkflowSnapshotCollector>,
}

impl std::fmt::Debug for WorkflowObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowObserver").finish()
    }
}

#[derive(Debug)]
struct MetricInstruments {
    db_visible_sequence: opentelemetry::metrics::Gauge<u64>,
    db_durable_sequence: opentelemetry::metrics::Gauge<u64>,
    db_durable_visible_lag: opentelemetry::metrics::Gauge<u64>,
    db_scheduler_backlog: opentelemetry::metrics::Gauge<u64>,
    db_compaction_debt_bytes: opentelemetry::metrics::Gauge<u64>,
    projection_frontier_sequence: opentelemetry::metrics::Gauge<u64>,
    projection_lag: opentelemetry::metrics::Gauge<u64>,
    workflow_inbox_depth: opentelemetry::metrics::Gauge<u64>,
    workflow_timer_depth: opentelemetry::metrics::Gauge<u64>,
    workflow_outbox_depth: opentelemetry::metrics::Gauge<u64>,
    workflow_in_flight_instances: opentelemetry::metrics::Gauge<u64>,
    workflow_source_lag: opentelemetry::metrics::Gauge<u64>,
    collections: opentelemetry::metrics::Counter<u64>,
    collection_duration_ms: opentelemetry::metrics::Histogram<f64>,
}

impl MetricInstruments {
    fn new(meter: opentelemetry::metrics::Meter) -> Self {
        Self {
            db_visible_sequence: meter.u64_gauge("terracedb.db.visible_sequence").build(),
            db_durable_sequence: meter.u64_gauge("terracedb.db.durable_sequence").build(),
            db_durable_visible_lag: meter.u64_gauge("terracedb.db.durable_visible_lag").build(),
            db_scheduler_backlog: meter.u64_gauge("terracedb.db.scheduler_backlog").build(),
            db_compaction_debt_bytes: meter
                .u64_gauge("terracedb.db.compaction_debt_bytes")
                .build(),
            projection_frontier_sequence: meter
                .u64_gauge("terracedb.projection.frontier_sequence")
                .build(),
            projection_lag: meter.u64_gauge("terracedb.projection.lag").build(),
            workflow_inbox_depth: meter.u64_gauge("terracedb.workflow.inbox_depth").build(),
            workflow_timer_depth: meter.u64_gauge("terracedb.workflow.timer_depth").build(),
            workflow_outbox_depth: meter.u64_gauge("terracedb.workflow.outbox_depth").build(),
            workflow_in_flight_instances: meter
                .u64_gauge("terracedb.workflow.in_flight_instances")
                .build(),
            workflow_source_lag: meter.u64_gauge("terracedb.workflow.source_lag").build(),
            collections: meter
                .u64_counter("terracedb.telemetry.metric_collections")
                .build(),
            collection_duration_ms: meter
                .f64_histogram("terracedb.telemetry.metric_collection_duration_ms")
                .build(),
        }
    }
}

fn db_metric_attributes(db: &Db) -> Vec<KeyValue> {
    vec![
        KeyValue::new(terracedb::telemetry_attrs::DB_NAME, db.telemetry_db_name()),
        KeyValue::new(
            terracedb::telemetry_attrs::DB_INSTANCE,
            db.telemetry_db_instance(),
        ),
        KeyValue::new(
            terracedb::telemetry_attrs::STORAGE_MODE,
            db.telemetry_storage_mode(),
        ),
    ]
}

fn projection_metric_attributes(snapshot: &ProjectionTelemetrySnapshot) -> Vec<KeyValue> {
    vec![
        KeyValue::new(
            terracedb::telemetry_attrs::PROJECTION_NAME,
            snapshot.projection_name.clone(),
        ),
        KeyValue::new(
            terracedb::telemetry_attrs::PROJECTION_MODE,
            snapshot.projection_mode.clone(),
        ),
        KeyValue::new(
            terracedb::telemetry_attrs::SOURCE_TABLE,
            snapshot.source_table.clone(),
        ),
    ]
}

fn workflow_metric_attributes(snapshot: &WorkflowTelemetrySnapshot) -> Vec<KeyValue> {
    vec![KeyValue::new(
        terracedb::telemetry_attrs::WORKFLOW_NAME,
        snapshot.workflow_name.clone(),
    )]
}

#[derive(Debug, Clone)]
struct TerracedbOtelLogLayer {
    logger: SdkLogger,
}

impl TerracedbOtelLogLayer {
    fn new(logger: SdkLogger) -> Self {
        Self { logger }
    }
}

impl<S> Layer<S> for TerracedbOtelLogLayer
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        let mut record = self.logger.create_log_record();
        let metadata = event.metadata();
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);

        if let Some(body) = visitor.message.take() {
            record.set_body(AnyValue::String(body.into()));
        } else {
            record.set_body(AnyValue::String(metadata.name().to_string().into()));
        }
        record.set_event_name(metadata.name());
        record.set_target(metadata.target().to_string());
        record.set_timestamp(SystemTime::now());
        record.set_observed_timestamp(SystemTime::now());
        record.set_severity_text(level_text(metadata.level()));
        record.set_severity_number(level_severity(metadata.level()));
        record.add_attributes(visitor.attributes);

        let span_context = tracing::Span::current()
            .context()
            .span()
            .span_context()
            .clone();
        if span_context.is_valid() {
            record.set_trace_context(
                span_context.trace_id(),
                span_context.span_id(),
                Some(span_context.trace_flags()),
            );
        }

        self.logger.emit(record);
    }
}

fn level_severity(level: &tracing::Level) -> Severity {
    match *level {
        tracing::Level::ERROR => Severity::Error,
        tracing::Level::WARN => Severity::Warn,
        tracing::Level::INFO => Severity::Info,
        tracing::Level::DEBUG => Severity::Debug,
        tracing::Level::TRACE => Severity::Trace,
    }
}

fn level_text(level: &tracing::Level) -> &'static str {
    match *level {
        tracing::Level::ERROR => "ERROR",
        tracing::Level::WARN => "WARN",
        tracing::Level::INFO => "INFO",
        tracing::Level::DEBUG => "DEBUG",
        tracing::Level::TRACE => "TRACE",
    }
}

#[derive(Default)]
struct EventVisitor {
    message: Option<String>,
    attributes: Vec<(String, AnyValue)>,
}

impl EventVisitor {
    fn push(&mut self, field: &Field, value: AnyValue) {
        if field.name() == "message" {
            self.message = Some(any_value_to_string(&value));
        } else {
            self.attributes.push((field.name().to_string(), value));
        }
    }
}

impl Visit for EventVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, AnyValue::Boolean(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, AnyValue::Int(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(
            field,
            AnyValue::Int(i64::try_from(value).unwrap_or(i64::MAX)),
        );
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, AnyValue::String(truncate_text(value).into()));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.push(
            field,
            AnyValue::String(truncate_text(&value.to_string()).into()),
        );
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push(field, AnyValue::Double(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.push(
            field,
            AnyValue::String(truncate_text(&format!("{value:?}")).into()),
        );
    }
}

fn any_value_to_string(value: &AnyValue) -> String {
    match value {
        AnyValue::Int(value) => value.to_string(),
        AnyValue::Double(value) => value.to_string(),
        AnyValue::String(value) => value.to_string(),
        AnyValue::Boolean(value) => value.to_string(),
        _ => format!("{value:?}"),
    }
}

fn truncate_text(value: &str) -> String {
    const MAX_LEN: usize = 512;
    if value.len() <= MAX_LEN {
        value.to_string()
    } else {
        let mut truncated = value[..MAX_LEN].to_string();
        truncated.push_str("...");
        truncated
    }
}
