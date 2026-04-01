use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use insta::assert_debug_snapshot;
use opentelemetry::{
    logs::AnyValue,
    trace::{SpanId, TraceContextExt},
};
use opentelemetry_sdk::{
    metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics},
    trace::SpanData,
};
use terracedb::{
    Db, DbConfig, LogCursor, ObjectStoreFailure, ObjectStoreOperation, ScanOptions, SequenceNumber,
    StorageSource, StubClock, StubFileSystem, StubObjectStore, Table, Timestamp, Transaction,
    UnifiedStorage, Value,
    test_support::{row_table_config, test_dependencies_with_clock, tiered_test_config},
};
use terracedb_otel::{TelemetryMode, TerracedbTelemetry};
use terracedb_projections::{
    ProjectionHandle, ProjectionHandler, ProjectionHandlerError, ProjectionRuntime,
    ProjectionSequenceRun, ProjectionTransaction, SingleSourceProjection,
};
use terracedb_workflows::{
    WorkflowContext, WorkflowDefinition, WorkflowHandler, WorkflowHandlerError, WorkflowOutput,
    WorkflowRuntime, WorkflowStateMutation, WorkflowTimerCommand, WorkflowTrigger,
};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone)]
struct TestDbEnv {
    config: DbConfig,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
    clock: Arc<StubClock>,
}

impl TestDbEnv {
    fn new(path: &str) -> Self {
        Self {
            config: tiered_test_config(path),
            file_system: Arc::new(StubFileSystem::default()),
            object_store: Arc::new(StubObjectStore::default()),
            clock: Arc::new(StubClock::default()),
        }
    }

    async fn open(&self) -> TestResult<Db> {
        Ok(Db::open(
            self.config.clone(),
            test_dependencies_with_clock(
                self.file_system.clone(),
                self.object_store.clone(),
                self.clock.clone(),
            ),
        )
        .await?)
    }
}

struct MirrorProjection {
    output: Table,
}

#[async_trait]
impl ProjectionHandler for MirrorProjection {
    async fn apply(
        &self,
        run: &ProjectionSequenceRun,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        for entry in run.entries() {
            match &entry.value {
                Some(value) => tx.put(&self.output, entry.key.clone(), value.clone()),
                None => tx.delete(&self.output, entry.key.clone()),
            }
        }
        Ok(())
    }
}

struct EventTimerWorkflow;

#[async_trait]
impl WorkflowHandler for EventTimerWorkflow {
    async fn route_event(
        &self,
        entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        let key = std::str::from_utf8(&entry.key)
            .map_err(|error| WorkflowHandlerError::new(std::io::Error::other(error.to_string())))?;
        Ok(key
            .split_once(':')
            .map(|(instance_id, _)| instance_id.to_string())
            .unwrap_or_else(|| key.to_string()))
    }

    async fn handle(
        &self,
        _instance_id: &str,
        _state: Option<Value>,
        trigger: &WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        match trigger {
            WorkflowTrigger::Event(_) => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("scheduled")),
                outbox_entries: Vec::new(),
                timers: vec![WorkflowTimerCommand::Schedule {
                    timer_id: b"follow-up".to_vec(),
                    fire_at: Timestamp::new(10),
                    payload: b"tick".to_vec(),
                }],
            }),
            WorkflowTrigger::Timer { .. } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("fired")),
                outbox_entries: Vec::new(),
                timers: Vec::new(),
            }),
            WorkflowTrigger::Callback { .. } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("scheduled")),
                outbox_entries: Vec::new(),
                timers: vec![WorkflowTimerCommand::Schedule {
                    timer_id: b"follow-up".to_vec(),
                    fire_at: Timestamp::new(10),
                    payload: b"tick".to_vec(),
                }],
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct NormalizedSpan {
    name: String,
    attrs: Vec<(String, String)>,
    has_parent: bool,
    link_count: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct NormalizedMetricShape {
    metric_names: Vec<String>,
    u64_points: BTreeMap<String, Vec<(BTreeMap<String, String>, u64)>>,
}

fn build_test_telemetry(service_name: &str) -> TerracedbTelemetry {
    TerracedbTelemetry::builder(service_name)
        .with_mode(TelemetryMode::Test)
        .build()
        .expect("build test telemetry")
}

fn span_attrs(span: &SpanData) -> BTreeMap<String, String> {
    span.attributes
        .iter()
        .map(|kv| (kv.key.as_str().to_string(), kv.value.as_str().into_owned()))
        .collect()
}

fn span_attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().into_owned())
}

fn span_with_sequence<'a>(
    spans: &'a [SpanData],
    name: &str,
    sequence: SequenceNumber,
) -> &'a SpanData {
    let expected = sequence.get().to_string();
    spans
        .iter()
        .find(|span| {
            span.name == name
                && span_attr(span, terracedb::telemetry_attrs::SEQUENCE).as_deref()
                    == Some(expected.as_str())
        })
        .unwrap_or_else(|| panic!("missing span {name} for sequence {expected}"))
}

fn span_with_attr<'a>(spans: &'a [SpanData], name: &str, key: &str, value: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|span| span.name == name && span_attr(span, key).as_deref() == Some(value))
        .unwrap_or_else(|| panic!("missing span {name} with {key}={value}"))
}

fn log_value_text(value: &AnyValue) -> String {
    match value {
        AnyValue::Int(value) => value.to_string(),
        AnyValue::Double(value) => value.to_string(),
        AnyValue::String(value) => value.to_string(),
        AnyValue::Boolean(value) => value.to_string(),
        _ => format!("{value:?}"),
    }
}

fn metric_names(snapshot: &ResourceMetrics) -> BTreeSet<String> {
    snapshot
        .scope_metrics()
        .flat_map(|scope| scope.metrics().map(|metric| metric.name().to_string()))
        .collect()
}

fn metric_points_u64(
    snapshot: &ResourceMetrics,
    name: &str,
) -> Vec<(BTreeMap<String, String>, u64)> {
    for scope in snapshot.scope_metrics() {
        for metric in scope.metrics() {
            if metric.name() != name {
                continue;
            }

            return match metric.data() {
                AggregatedMetrics::U64(MetricData::Gauge(gauge)) => gauge
                    .data_points()
                    .map(|point| {
                        (
                            point
                                .attributes()
                                .map(|kv| {
                                    (kv.key.as_str().to_string(), kv.value.as_str().into_owned())
                                })
                                .collect(),
                            point.value(),
                        )
                    })
                    .collect(),
                AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                    .data_points()
                    .map(|point| {
                        (
                            point
                                .attributes()
                                .map(|kv| {
                                    (kv.key.as_str().to_string(), kv.value.as_str().into_owned())
                                })
                                .collect(),
                            point.value(),
                        )
                    })
                    .collect(),
                _ => panic!("metric {name} did not use a u64 gauge or sum"),
            };
        }
    }

    Vec::new()
}

fn normalized_metric_points(
    snapshot: &ResourceMetrics,
    name: &str,
) -> Vec<(BTreeMap<String, String>, u64)> {
    let mut points = metric_points_u64(snapshot, name)
        .into_iter()
        .map(|(mut attrs, value)| {
            attrs.remove(terracedb::telemetry_attrs::DB_INSTANCE);
            (attrs, value)
        })
        .collect::<Vec<_>>();
    points.sort();
    points
}

fn normalize_terracedb_spans(spans: &[SpanData]) -> Vec<NormalizedSpan> {
    let mut normalized = spans
        .iter()
        .filter(|span| span.name.starts_with("terracedb."))
        .map(|span| {
            let mut attrs = span_attrs(span)
                .into_iter()
                .filter(|(key, _)| {
                    key.starts_with("terracedb.") && key != terracedb::telemetry_attrs::DB_INSTANCE
                })
                .collect::<Vec<_>>();
            attrs.sort();
            NormalizedSpan {
                name: span.name.to_string(),
                attrs,
                has_parent: span.parent_span_id != SpanId::INVALID,
                link_count: span.links.len(),
            }
        })
        .collect::<Vec<_>>();
    normalized.sort();
    normalized
}

async fn wait_for_projection(
    handle: &mut ProjectionHandle,
    sequence: SequenceNumber,
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_watermark(sequence))
        .await
        .map_err(|_| {
            std::io::Error::other(format!(
                "timed out waiting for projection watermark {}",
                sequence.get()
            ))
        })??;
    Ok(())
}

async fn wait_for_workflow_state<H>(
    runtime: &WorkflowRuntime<H>,
    instance_id: &str,
    expected: &str,
) -> TestResult
where
    H: WorkflowHandler + 'static,
{
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.wait_for_state(instance_id, Value::bytes(expected)),
    )
    .await;
    if result.is_err() {
        let snapshot = runtime
            .telemetry_snapshot()
            .await
            .map(|snapshot| format!("{snapshot:?}"))
            .unwrap_or_else(|error| format!("telemetry error: {error}"));
        return Err(std::io::Error::other(format!(
            "timed out waiting for workflow state {instance_id}={expected}; snapshot={snapshot}"
        ))
        .into());
    }
    result.expect("timeout should be handled above")?;
    Ok(())
}

async fn capture_db_span_shape(path: &str) -> TestResult<Vec<NormalizedSpan>> {
    let telemetry = build_test_telemetry("shape-check");
    let exports = telemetry
        .test_exports()
        .expect("test telemetry should expose in-memory exporters");
    let env = TestDbEnv::new(path);
    let db = env.open().await?;
    let table = db.create_table(row_table_config("events")).await?;

    telemetry
        .with_subscriber(async {
            let root = tracing::info_span!("app.shape.root");
            async move {
                table
                    .write(b"user:1".to_vec(), Value::bytes("shape-a"))
                    .await?;
                db.flush().await?;
                let stream = db
                    .scan_since(&table, LogCursor::beginning(), ScanOptions::default())
                    .await?;
                let _entries = stream.collect::<Vec<_>>().await;
                let mut tx = Transaction::begin(&db).await;
                tx.write(&table, b"user:2".to_vec(), Value::bytes("shape-b"));
                tx.commit().await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }
            .instrument(root)
            .await
        })
        .await?;

    telemetry.force_flush().await?;
    Ok(normalize_terracedb_spans(&exports.spans()?))
}

async fn capture_metric_shape(path: &str) -> TestResult<NormalizedMetricShape> {
    let telemetry = build_test_telemetry("metric-shape");
    let exports = telemetry
        .test_exports()
        .expect("test telemetry should expose in-memory exporters");
    let env = TestDbEnv::new(path);
    let db = env.open().await?;
    let source = db.create_table(row_table_config("source")).await?;
    let output = db
        .create_table(row_table_config("projection_output"))
        .await?;
    let source_sequence = source
        .write(b"metric:1".to_vec(), Value::bytes("payload"))
        .await?;
    db.flush().await?;

    let (projection_runtime, workflow_runtime) = telemetry
        .with_subscriber(async {
            let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
            let mut projection_handle = projection_runtime
                .start_single_source(
                    SingleSourceProjection::new(
                        "metric-projection",
                        source.clone(),
                        MirrorProjection {
                            output: output.clone(),
                        },
                    )
                    .with_outputs([output.clone()]),
                )
                .await?;
            wait_for_projection(&mut projection_handle, source_sequence).await?;
            projection_handle.shutdown().await?;

            let workflow_runtime = WorkflowRuntime::open(
                db.clone(),
                env.clock.clone(),
                WorkflowDefinition::new("metric-workflow", [source.clone()], EventTimerWorkflow),
            )
            .await?;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                projection_runtime,
                workflow_runtime,
            ))
        })
        .await?;

    telemetry.observe_db(db);
    telemetry.observe_projection_runtime(projection_runtime);
    telemetry.observe_workflow_runtime(workflow_runtime);

    telemetry.force_flush().await?;
    telemetry.force_flush().await?;

    let latest = exports
        .metrics()?
        .into_iter()
        .last()
        .expect("latest metric snapshot");
    let metric_names = metric_names(&latest).into_iter().collect::<Vec<_>>();
    let u64_points = [
        "terracedb.db.visible_sequence",
        "terracedb.db.durable_sequence",
        "terracedb.db.durable_visible_lag",
        "terracedb.projection.frontier_sequence",
        "terracedb.projection.lag",
        "terracedb.workflow.inbox_depth",
        "terracedb.workflow.timer_depth",
        "terracedb.workflow.outbox_depth",
        "terracedb.workflow.in_flight_instances",
        "terracedb.workflow.source_lag",
    ]
    .into_iter()
    .map(|name| (name.to_string(), normalized_metric_points(&latest, name)))
    .collect();

    Ok(NormalizedMetricShape {
        metric_names,
        u64_points,
    })
}

#[tokio::test]
async fn db_spans_follow_ambient_context_and_emit_standalone_roots_without_payloads() -> TestResult
{
    let telemetry = build_test_telemetry("db-correlation");
    let exports = telemetry
        .test_exports()
        .expect("test telemetry should expose in-memory exporters");
    let env = TestDbEnv::new("/terracedb/otel/db-correlation");
    let db = env.open().await?;
    let table = db.create_table(row_table_config("events")).await?;
    let rooted_table = table.clone();

    let (root_trace_id, root_span_id, first_sequence, tx_sequence, seen_entries) = telemetry
        .with_subscriber(async {
            let root = tracing::info_span!("app.db.root");
            let span_context = root.context().span().span_context().clone();
            async move {
                let first_sequence = rooted_table
                    .write(b"user:1".to_vec(), Value::bytes("secret-payload"))
                    .await?;
                db.flush().await?;
                let stream = db
                    .scan_since(
                        &rooted_table,
                        LogCursor::beginning(),
                        ScanOptions::default(),
                    )
                    .await?;
                let seen_entries = stream.collect::<Vec<_>>().await.len();

                let mut tx = Transaction::begin(&db).await;
                tx.write(
                    &rooted_table,
                    b"user:2".to_vec(),
                    Value::bytes("tx-payload"),
                );
                let tx_sequence = tx.commit().await?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    span_context.trace_id(),
                    span_context.span_id(),
                    first_sequence,
                    tx_sequence,
                    seen_entries,
                ))
            }
            .instrument(root)
            .await
        })
        .await?;

    assert_eq!(seen_entries, 1);

    let standalone_sequence = telemetry
        .with_subscriber(async {
            table
                .write(b"standalone".to_vec(), Value::bytes("solo"))
                .await
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
        })
        .await?;

    telemetry.force_flush().await?;
    let spans = exports.spans()?;

    let first_commit = span_with_sequence(&spans, "terracedb.db.commit", first_sequence);
    assert_eq!(first_commit.span_context.trace_id(), root_trace_id);
    assert_eq!(first_commit.parent_span_id, root_span_id);

    let tx_commit = span_with_sequence(&spans, "terracedb.db.commit", tx_sequence);
    assert_eq!(tx_commit.span_context.trace_id(), root_trace_id);
    assert_eq!(tx_commit.parent_span_id, root_span_id);

    let standalone_commit = span_with_sequence(&spans, "terracedb.db.commit", standalone_sequence);
    assert!(standalone_commit.span_context.is_valid());
    assert_eq!(standalone_commit.parent_span_id, SpanId::INVALID);

    let flush = spans
        .iter()
        .find(|span| {
            span.name == "terracedb.db.flush" && span.span_context.trace_id() == root_trace_id
        })
        .expect("expected a flush span inside the application trace");
    assert_eq!(flush.parent_span_id, root_span_id);

    let scan = spans
        .iter()
        .find(|span| {
            span.name == "terracedb.db.change_feed.scan"
                && span.span_context.trace_id() == root_trace_id
        })
        .expect("expected a change-feed scan span inside the application trace");
    assert_eq!(scan.parent_span_id, root_span_id);

    for span in spans.iter().filter(|span| {
        span.name.starts_with("terracedb.") && span.span_context.trace_id() == root_trace_id
    }) {
        assert_ne!(
            span.parent_span_id,
            SpanId::INVALID,
            "terracedb span {} should not become a root span under an active application trace",
            span.name
        );
        for value in span_attrs(span).into_values() {
            assert!(!value.contains("secret-payload"));
            assert!(!value.contains("tx-payload"));
            assert!(!value.contains("user:1"));
            assert!(!value.contains("user:2"));
        }
    }

    Ok(())
}

#[tokio::test]
async fn projection_and_workflow_spans_stay_correlated_after_restart() -> TestResult {
    let telemetry = build_test_telemetry("runtime-correlation");
    let exports = telemetry
        .test_exports()
        .expect("test telemetry should expose in-memory exporters");
    let env = TestDbEnv::new("/terracedb/otel/backlog-replay");
    let db = env.open().await?;
    let _source = db.create_table(row_table_config("source")).await?;
    let _projection_output = db
        .create_table(row_table_config("projection_output"))
        .await?;
    drop(db);

    let reopened = env.open().await?;
    let reopened_source = reopened.table("source");
    let reopened_output = reopened.table("projection_output");

    let mut projection_handle = telemetry
        .with_subscriber(async {
            let runtime = ProjectionRuntime::open(reopened.clone()).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                runtime
                    .start_single_source(
                        SingleSourceProjection::new(
                            "mirror",
                            reopened_source.clone(),
                            MirrorProjection {
                                output: reopened_output.clone(),
                            },
                        )
                        .with_outputs([reopened_output.clone()]),
                    )
                    .await?,
            )
        })
        .await?;

    let workflow_runtime = telemetry
        .with_subscriber(async {
            let workflow_runtime = WorkflowRuntime::open(
                reopened.clone(),
                env.clock.clone(),
                WorkflowDefinition::new(
                    "orders",
                    Vec::<terracedb_workflows::WorkflowSource>::new(),
                    EventTimerWorkflow,
                )
                .with_timer_poll_interval(Duration::from_millis(1)),
            )
            .await?;
            let workflow_handle = workflow_runtime.start().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((workflow_runtime, workflow_handle))
        })
        .await?;

    let (root_trace_id, root_span_id, source_sequence) = telemetry
        .with_subscriber(async {
            let root = tracing::info_span!("app.restart.root");
            let span_context = root.context().span().span_context().clone();
            let source = reopened_source.clone();
            let db = reopened.clone();
            let workflow = workflow_runtime.0.clone();
            async move {
                let source_sequence = source
                    .write(
                        b"acct-1:event-1".to_vec(),
                        Value::bytes("workflow-source-payload"),
                    )
                    .await?;
                workflow
                    .admit_callback("acct-1", "cb-1", b"callback-payload".to_vec())
                    .await?;
                db.flush().await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    span_context.trace_id(),
                    span_context.span_id(),
                    source_sequence,
                ))
            }
            .instrument(root)
            .await
        })
        .await?;
    wait_for_workflow_state(&workflow_runtime.0, "acct-1", "scheduled").await?;
    env.clock.advance(Duration::from_millis(10));
    wait_for_workflow_state(&workflow_runtime.0, "acct-1", "fired").await?;
    workflow_runtime.1.shutdown().await?;
    wait_for_projection(&mut projection_handle, source_sequence).await?;
    projection_handle.shutdown().await?;

    telemetry.force_flush().await?;
    let spans = exports.spans()?;
    let source_commit = span_with_sequence(&spans, "terracedb.db.commit", source_sequence);
    assert_eq!(source_commit.span_context.trace_id(), root_trace_id);
    assert_eq!(source_commit.parent_span_id, root_span_id);
    let causal_parent = source_commit.span_context.span_id();

    let projection_batch = span_with_attr(
        &spans,
        "terracedb.projection.batch",
        terracedb::telemetry_attrs::PROJECTION_NAME,
        "mirror",
    );
    assert_eq!(projection_batch.span_context.trace_id(), root_trace_id);
    assert_eq!(projection_batch.parent_span_id, causal_parent);
    assert_eq!(
        span_attr(projection_batch, terracedb::telemetry_attrs::SOURCE_TABLE).as_deref(),
        Some("source")
    );
    assert_eq!(
        span_attr(projection_batch, terracedb::telemetry_attrs::SEQUENCE).as_deref(),
        Some(source_sequence.get().to_string().as_str())
    );

    let workflow_callback_admit = span_with_attr(
        &spans,
        "terracedb.workflow.callback.admit",
        terracedb::telemetry_attrs::WORKFLOW_NAME,
        "orders",
    );
    assert_eq!(
        workflow_callback_admit.span_context.trace_id(),
        root_trace_id
    );
    assert_eq!(workflow_callback_admit.parent_span_id, root_span_id);

    let workflow_callback_step = span_with_attr(
        &spans,
        "terracedb.workflow.step",
        terracedb::telemetry_attrs::WORKFLOW_TRIGGER_KIND,
        "callback",
    );
    assert_eq!(
        workflow_callback_step.span_context.trace_id(),
        root_trace_id
    );
    assert_eq!(workflow_callback_step.parent_span_id, root_span_id);
    assert_eq!(
        span_attr(
            workflow_callback_step,
            terracedb::telemetry_attrs::WORKFLOW_INSTANCE_ID,
        )
        .as_deref(),
        Some("acct-1")
    );

    let timer_fire_batch = span_with_attr(
        &spans,
        "terracedb.workflow.timer.fire_batch",
        terracedb::telemetry_attrs::WORKFLOW_NAME,
        "orders",
    );
    assert_eq!(timer_fire_batch.span_context.trace_id(), root_trace_id);
    assert_eq!(timer_fire_batch.parent_span_id, root_span_id);

    let workflow_timer_step = span_with_attr(
        &spans,
        "terracedb.workflow.step",
        terracedb::telemetry_attrs::WORKFLOW_TRIGGER_KIND,
        "timer",
    );
    assert_eq!(workflow_timer_step.span_context.trace_id(), root_trace_id);
    assert_eq!(workflow_timer_step.parent_span_id, root_span_id);
    assert_eq!(
        span_attr(workflow_timer_step, terracedb::telemetry_attrs::TIMER_ID).as_deref(),
        Some("follow-up")
    );

    for span in [
        projection_batch,
        workflow_callback_admit,
        workflow_callback_step,
        timer_fire_batch,
        workflow_timer_step,
    ] {
        for value in span_attrs(span).into_values() {
            assert!(!value.contains("workflow-source-payload"));
            assert!(!value.contains("acct-1:event-1"));
            assert!(!value.contains("callback-payload"));
        }
    }

    Ok(())
}

#[tokio::test]
async fn logs_carry_trace_context_for_remote_failures() -> TestResult {
    let telemetry = build_test_telemetry("log-correlation");
    let exports = telemetry
        .test_exports()
        .expect("test telemetry should expose in-memory exporters");
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    object_store.inject_failure(ObjectStoreFailure::timeout(
        ObjectStoreOperation::Get,
        "backup/object-1",
    ));
    let storage = UnifiedStorage::new(file_system, object_store, None);

    let (root_trace_id, root_span_id) = telemetry
        .with_subscriber(async {
            let root = tracing::info_span!("app.remote.root");
            let span_context = root.context().span().span_context().clone();
            async move {
                let error = storage
                    .read_all(&StorageSource::remote_object("backup/object-1"))
                    .await
                    .expect_err("remote read should fail");
                assert!(error.to_string().contains("remote Get failed"));
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    span_context.trace_id(),
                    span_context.span_id(),
                ))
            }
            .instrument(root)
            .await
        })
        .await?;

    telemetry.force_flush().await?;
    let logs = exports.logs()?;
    let remote_log = logs
        .iter()
        .find(|log| {
            log.record.body().is_some_and(|body| {
                log_value_text(body).contains("terracedb remote operation failed")
            })
        })
        .expect("expected a remote failure log");
    let trace_context = remote_log
        .record
        .trace_context()
        .expect("remote log should carry trace context");
    assert_eq!(trace_context.trace_id, root_trace_id);
    assert_eq!(trace_context.span_id, root_span_id);

    let attributes = remote_log
        .record
        .attributes_iter()
        .map(|(key, value)| (key.as_str().to_string(), log_value_text(value)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        attributes
            .get("terracedb_remote_operation")
            .map(String::as_str),
        Some("Get")
    );
    assert_eq!(
        attributes
            .get("terracedb_remote_target")
            .map(String::as_str),
        Some("backup/object-1")
    );
    assert_eq!(
        attributes.get("terracedb_remote_kind").map(String::as_str),
        Some("Timeout")
    );
    assert_eq!(
        attributes
            .get("terracedb_remote_recovery")
            .map(String::as_str),
        Some("Retry")
    );

    Ok(())
}

#[tokio::test]
async fn metric_snapshots_keep_stable_names_and_attribute_shapes() -> TestResult {
    let telemetry = build_test_telemetry("metric-snapshots");
    let exports = telemetry
        .test_exports()
        .expect("test telemetry should expose in-memory exporters");
    let env = TestDbEnv::new("/terracedb/otel/metric-snapshots");
    let db = env.open().await?;
    let source = db.create_table(row_table_config("source")).await?;
    let output = db
        .create_table(row_table_config("projection_output"))
        .await?;
    let source_sequence = source
        .write(b"metric:1".to_vec(), Value::bytes("payload"))
        .await?;
    db.flush().await?;

    let (projection_runtime, workflow_runtime) = telemetry
        .with_subscriber(async {
            let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
            let mut projection_handle = projection_runtime
                .start_single_source(
                    SingleSourceProjection::new(
                        "metric-projection",
                        source.clone(),
                        MirrorProjection {
                            output: output.clone(),
                        },
                    )
                    .with_outputs([output.clone()]),
                )
                .await?;
            wait_for_projection(&mut projection_handle, source_sequence).await?;
            projection_handle.shutdown().await?;

            let workflow_runtime = WorkflowRuntime::open(
                db.clone(),
                env.clock.clone(),
                WorkflowDefinition::new("metric-workflow", [source.clone()], EventTimerWorkflow),
            )
            .await?;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                projection_runtime,
                workflow_runtime,
            ))
        })
        .await?;

    telemetry.observe_db(db.clone());
    telemetry.observe_projection_runtime(projection_runtime);
    telemetry.observe_workflow_runtime(workflow_runtime);

    telemetry.force_flush().await?;
    telemetry.force_flush().await?;

    let metrics = exports.metrics()?;
    assert!(
        metrics.len() >= 2,
        "expected repeated force_flush calls to export multiple metric snapshots"
    );
    let previous = &metrics[metrics.len() - 2];
    let latest = metrics.last().expect("latest metric snapshot");

    let previous_names = metric_names(previous);
    let latest_names = metric_names(latest);
    assert_eq!(previous_names, latest_names);

    let expected_names = BTreeSet::from([
        "terracedb.db.visible_sequence".to_string(),
        "terracedb.db.durable_sequence".to_string(),
        "terracedb.db.durable_visible_lag".to_string(),
        "terracedb.db.scheduler_backlog".to_string(),
        "terracedb.db.compaction_debt_bytes".to_string(),
        "terracedb.projection.frontier_sequence".to_string(),
        "terracedb.projection.lag".to_string(),
        "terracedb.workflow.inbox_depth".to_string(),
        "terracedb.workflow.timer_depth".to_string(),
        "terracedb.workflow.outbox_depth".to_string(),
        "terracedb.workflow.in_flight_instances".to_string(),
        "terracedb.workflow.source_lag".to_string(),
        "terracedb.telemetry.metric_collections".to_string(),
        "terracedb.telemetry.metric_collection_duration_ms".to_string(),
    ]);
    assert!(expected_names.is_subset(&latest_names));

    let db_lag = metric_points_u64(latest, "terracedb.db.durable_visible_lag");
    assert_eq!(db_lag.len(), 1);
    assert_eq!(db_lag[0].1, 0);

    let projection_lag = metric_points_u64(latest, "terracedb.projection.lag");
    assert_eq!(projection_lag.len(), 1);
    assert_eq!(projection_lag[0].1, 0);

    let workflow_source_lag = metric_points_u64(latest, "terracedb.workflow.source_lag");
    assert_eq!(workflow_source_lag.len(), 1);
    assert_eq!(
        workflow_source_lag[0]
            .0
            .get(terracedb::telemetry_attrs::WORKFLOW_NAME)
            .map(String::as_str),
        Some("metric-workflow")
    );
    assert_eq!(
        workflow_source_lag[0]
            .0
            .get(terracedb::telemetry_attrs::SOURCE_TABLE)
            .map(String::as_str),
        Some("source")
    );
    assert_eq!(
        workflow_source_lag,
        metric_points_u64(previous, "terracedb.workflow.source_lag")
    );

    Ok(())
}

#[tokio::test]
async fn metric_payload_shape_snapshot_is_stable() -> TestResult {
    let shape = capture_metric_shape("/terracedb/otel/metric-shape").await?;
    assert_debug_snapshot!("metric_payload_shape", shape);
    Ok(())
}

#[tokio::test]
async fn same_seed_runs_produce_the_same_normalized_terracedb_span_shape() -> TestResult {
    let first = capture_db_span_shape("/terracedb/otel/repeat-a/shape").await?;
    let second = capture_db_span_shape("/terracedb/otel/repeat-b/shape").await?;
    assert_eq!(first, second);
    Ok(())
}
