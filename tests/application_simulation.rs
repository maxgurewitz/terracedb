use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use futures::StreamExt;
use terracedb::{
    CommitOptions, CompactionStrategy, CreateTableError, Db, DbConfig, KvStream, Rng, S3Location,
    ScanOptions, SequenceNumber, Table, TableConfig, TableFormat, TieredDurabilityMode,
    TieredStorageConfig, Value,
};
use terracedb_projections::{
    ProjectionHandle, ProjectionHandler, ProjectionHandlerError, ProjectionRuntime,
    ProjectionSequenceRun, ProjectionTransaction, RecomputeStrategy, SingleSourceProjection,
};
use terracedb_simulation::{
    CutPoint, SeededSimulationRunner, SimulatedExternalService, SimulatedServiceRequest,
    SimulationCheckpoint, SimulationStackBuilder, TerracedbSimulationHarness, TraceEvent,
};
use terracedb_workflows::{
    WorkflowContext, WorkflowDefinition, WorkflowHandle, WorkflowHandler, WorkflowHandlerError,
    WorkflowOutput, WorkflowRuntime, WorkflowStateMutation,
};

const APPLICATION_SIMULATION_DURATION: Duration = Duration::from_secs(2);
const SIMULATION_MIN_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const SIMULATION_MAX_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const DELIVERY_TIMEOUT: Duration = Duration::from_millis(8);
const DELIVERY_TIMEOUT_WHILE_HELD: Duration = Duration::from_millis(5);

fn app_config(path: &str, durability: TieredDurabilityMode) -> DbConfig {
    DbConfig {
        storage: terracedb::StorageConfig::Tiered(TieredStorageConfig {
            ssd: terracedb::SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-app-sim".to_string(),
                prefix: "consumer".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability,
        }),
        scheduler: None,
    }
}

fn row_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Row,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(10),
        history_retention_sequences: None,
        compaction_strategy: CompactionStrategy::Leveled,
        schema: None,
        metadata: BTreeMap::new(),
    }
}

async fn ensure_table(db: &Db, config: TableConfig) -> Result<Table, CreateTableError> {
    match db.create_table(config.clone()).await {
        Ok(table) => Ok(table),
        Err(CreateTableError::AlreadyExists(_)) => Ok(db.table(config.name)),
        Err(error) => Err(error),
    }
}

async fn collect_rows(stream: KvStream) -> Vec<(Vec<u8>, Vec<u8>)> {
    stream
        .map(|(key, value)| match value {
            Value::Bytes(bytes) => (key, bytes),
            Value::Record(_) => panic!("application simulation only expects byte values"),
        })
        .collect()
        .await
}

struct MirrorOrdersProjection {
    output: Table,
}

#[async_trait]
impl ProjectionHandler for MirrorOrdersProjection {
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

struct OrderWorkflow;

#[async_trait]
impl WorkflowHandler for OrderWorkflow {
    async fn route_event(
        &self,
        _entry: &terracedb::ChangeEntry,
    ) -> Result<String, WorkflowHandlerError> {
        Err(WorkflowHandlerError::new(std::io::Error::other(
            "order workflow is started by callbacks in the application simulation",
        )))
    }

    async fn handle(
        &self,
        instance_id: &str,
        state: Option<Value>,
        trigger: &terracedb_workflows::WorkflowTrigger,
        _ctx: &WorkflowContext,
    ) -> Result<WorkflowOutput, WorkflowHandlerError> {
        let already_charged =
            matches!(state.as_ref(), Some(Value::Bytes(bytes)) if bytes == b"charged");

        match trigger {
            terracedb_workflows::WorkflowTrigger::Callback { callback_id, .. }
                if callback_id == "start" =>
            {
                Ok(WorkflowOutput {
                    state: WorkflowStateMutation::Put(Value::bytes("pending")),
                    outbox_entries: vec![terracedb::OutboxEntry {
                        outbox_id: format!("charge:{instance_id}").into_bytes(),
                        idempotency_key: format!("charge:{instance_id}"),
                        payload: instance_id.as_bytes().to_vec(),
                    }],
                    timers: Vec::new(),
                })
            }
            terracedb_workflows::WorkflowTrigger::Callback { .. } if already_charged => {
                Ok(WorkflowOutput::default())
            }
            terracedb_workflows::WorkflowTrigger::Callback { .. } => Ok(WorkflowOutput {
                state: WorkflowStateMutation::Put(Value::bytes("charged")),
                outbox_entries: Vec::new(),
                timers: Vec::new(),
            }),
            terracedb_workflows::WorkflowTrigger::Timer { .. } => Err(WorkflowHandlerError::new(
                std::io::Error::other("order workflow does not use timers"),
            )),
            terracedb_workflows::WorkflowTrigger::Event(_) => {
                Err(WorkflowHandlerError::new(std::io::Error::other(
                    "order workflow does not process source events in the application simulation",
                )))
            }
        }
    }
}

struct AppStack {
    orders: Table,
    order_view: Table,
    delivery_cursors: Table,
    _projection_runtime: ProjectionRuntime,
    projection_handle: ProjectionHandle,
    workflow_runtime: WorkflowRuntime<OrderWorkflow>,
    workflow_handle: WorkflowHandle,
    gateway: SimulatedExternalService,
}

#[derive(Clone, Debug, PartialEq)]
struct AppSimulationCapture {
    order_id: String,
    projected_rows: Vec<(Vec<u8>, Vec<u8>)>,
    workflow_state: Option<Value>,
    service_requests: Vec<SimulatedServiceRequest>,
    projection_watermark: SequenceNumber,
    trace: Vec<TraceEvent>,
    checkpoints: Vec<SimulationCheckpoint>,
}

fn app_stack_builder(gateway: SimulatedExternalService) -> SimulationStackBuilder<AppStack> {
    SimulationStackBuilder::new(
        move |context, db| {
            let gateway = gateway.clone();
            async move {
                let orders = ensure_table(&db, row_table_config("orders")).await?;
                let order_view = ensure_table(&db, row_table_config("order_view")).await?;
                let delivery_cursors =
                    ensure_table(&db, row_table_config("delivery_cursors")).await?;

                let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
                let projection_handle = projection_runtime
                    .start_single_source(
                        SingleSourceProjection::new(
                            "orders-view",
                            orders.clone(),
                            MirrorOrdersProjection {
                                output: order_view.clone(),
                            },
                        )
                        .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState)
                        .with_outputs([order_view.clone()]),
                    )
                    .await?;

                let workflow_runtime = WorkflowRuntime::open(
                    db.clone(),
                    context.clock(),
                    WorkflowDefinition::new(
                        "order-workflow",
                        std::iter::empty::<Table>(),
                        OrderWorkflow,
                    ),
                )
                .await?;
                let workflow_handle = workflow_runtime.start().await?;

                Ok(AppStack {
                    orders,
                    order_view,
                    delivery_cursors,
                    _projection_runtime: projection_runtime,
                    projection_handle,
                    workflow_runtime,
                    workflow_handle,
                    gateway,
                })
            }
        },
        |stack| async move {
            stack.projection_handle.shutdown().await?;
            stack.workflow_handle.shutdown().await?;
            Ok(())
        },
    )
}

async fn checkpoint_app(
    harness: &mut TerracedbSimulationHarness<AppStack>,
    label: impl Into<String>,
    order_id: &str,
) -> Result<(), terracedb_simulation::SimulationHarnessError> {
    let order_id = order_id.to_string();
    harness
        .checkpoint_with(label, move |_db, stack| {
            Box::pin(async move {
                let mut metadata = stack.gateway.checkpoint_metadata();
                metadata.insert(
                    "projection.watermark".to_string(),
                    stack
                        .projection_handle
                        .current_watermark()
                        .get()
                        .to_string(),
                );
                metadata.insert(
                    "workflow.state".to_string(),
                    format!("{:?}", stack.workflow_runtime.load_state(&order_id).await?),
                );
                Ok(metadata)
            })
        })
        .await
}

async fn wait_for_workflow_state(
    harness: &TerracedbSimulationHarness<AppStack>,
    order_id: &str,
    expected: &str,
) -> Result<(), terracedb_simulation::SimulationHarnessError> {
    let state_table = harness
        .stack()
        .workflow_runtime
        .tables()
        .state_table()
        .clone();
    let inbox_table = harness
        .stack()
        .workflow_runtime
        .tables()
        .inbox_table()
        .clone();
    let order_id = order_id.to_string();
    let expected = Value::bytes(expected);
    harness
        .wait_for_change(
            format!("application workflow state {order_id} -> {:?}", expected),
            [&state_table],
            [&inbox_table],
            move |_db, stack| {
                let order_id = order_id.clone();
                let expected = expected.clone();
                Box::pin(async move {
                    Ok(stack.workflow_runtime.load_state(&order_id).await? == Some(expected))
                })
            },
        )
        .await
}

fn decode_outbox_value(value: &Value) -> std::io::Result<(String, Vec<u8>)> {
    const OUTBOX_VALUE_VERSION: u8 = 1;

    let Value::Bytes(bytes) = value else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "outbox value must be bytes",
        ));
    };
    if bytes.len() < 5 || bytes[0] != OUTBOX_VALUE_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "outbox value encoding is invalid",
        ));
    }

    let mut key_len = [0_u8; 4];
    key_len.copy_from_slice(&bytes[1..5]);
    let key_len = u32::from_be_bytes(key_len) as usize;
    let header_len = 5 + key_len;
    if bytes.len() < header_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "outbox value ended before the idempotency key finished",
        ));
    }

    let idempotency_key =
        String::from_utf8(bytes[5..header_len].to_vec()).map_err(std::io::Error::other)?;
    Ok((idempotency_key, bytes[header_len..].to_vec()))
}

async fn deliver_next_order(
    db: &Db,
    stack: &AppStack,
    order_id: &str,
    persist_cursor: bool,
    timeout: Duration,
) -> std::io::Result<usize> {
    let outbox_id = format!("charge:{order_id}").into_bytes();
    if stack
        .delivery_cursors
        .read(outbox_id.clone())
        .await
        .map_err(std::io::Error::other)?
        .is_some()
    {
        return Ok(0);
    }

    let Some(outbox_value) = stack
        .workflow_runtime
        .tables()
        .outbox_table()
        .read(outbox_id.clone())
        .await
        .map_err(std::io::Error::other)?
    else {
        return Ok(0);
    };

    let (idempotency_key, payload) = decode_outbox_value(&outbox_value)?;
    let response = stack.gateway.call(payload, timeout).await?;
    stack
        .workflow_runtime
        .admit_callback(order_id.to_string(), idempotency_key, response)
        .await
        .map_err(std::io::Error::other)?;

    if persist_cursor {
        let mut batch = db.write_batch();
        batch.put(
            &stack.delivery_cursors,
            outbox_id,
            Value::bytes("delivered"),
        );
        db.commit(batch, CommitOptions::default())
            .await
            .map_err(std::io::Error::other)?;
        db.flush().await.map_err(std::io::Error::other)?;
    }

    Ok(1)
}

async fn assert_app_invariants(
    harness: &mut TerracedbSimulationHarness<AppStack>,
    order_id: &str,
    expected_requests: usize,
) -> turmoil::Result<AppSimulationCapture> {
    checkpoint_app(harness, "app-invariants", order_id).await?;

    let view_rows = collect_rows(
        harness
            .stack()
            .order_view
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await?,
    )
    .await;
    harness.require_eq(
        "projected order rows",
        &view_rows,
        &vec![(order_id.as_bytes().to_vec(), b"created".to_vec())],
    )?;

    let workflow_state = harness
        .stack()
        .workflow_runtime
        .load_state(order_id)
        .await?;
    harness.require_eq(
        "workflow state",
        &workflow_state,
        &Some(Value::bytes("charged")),
    )?;

    let requests = harness.stack().gateway.requests();
    harness.require_eq("service request count", &requests.len(), &expected_requests)?;
    harness.require(
        requests
            .iter()
            .all(|request| request.payload == order_id.as_bytes()),
        "service should only receive duplicate deliveries for the same order",
    )?;

    let drained = deliver_next_order(
        harness.db(),
        harness.stack(),
        order_id,
        true,
        DELIVERY_TIMEOUT,
    )
    .await?;
    harness.require_eq("drained outbox", &drained, &0)?;

    Ok(AppSimulationCapture {
        order_id: order_id.to_string(),
        projected_rows: view_rows,
        workflow_state,
        service_requests: requests,
        projection_watermark: harness.stack().projection_handle.current_watermark(),
        trace: harness.trace(),
        checkpoints: harness.checkpoints().to_vec(),
    })
}

fn run_order_app_simulation(seed: u64) -> turmoil::Result<AppSimulationCapture> {
    let gateway = SimulatedExternalService::new("payment-gateway", 9501);
    let gateway_host = gateway.simulation_host();

    SeededSimulationRunner::new(seed)
        .with_simulation_duration(APPLICATION_SIMULATION_DURATION)
        .with_message_latency(
            SIMULATION_MIN_MESSAGE_LATENCY,
            SIMULATION_MAX_MESSAGE_LATENCY,
        )
        .with_host(gateway_host)
        .run_with(move |context| {
            let gateway = gateway.clone();
            async move {
                let mut harness = TerracedbSimulationHarness::open(
                    context,
                    app_config("/application-sim", TieredDurabilityMode::Deferred),
                    app_stack_builder(gateway.clone()),
                )
                .await?;

                let result = async {
                    let order_id = format!("order-{}", harness.context().rng().next_u64() % 1_000);
                    let orders = harness.stack().orders.clone();
                    let mut batch = harness.db().write_batch();
                    batch.put(
                        &orders,
                        order_id.as_bytes().to_vec(),
                        Value::bytes("created"),
                    );
                    let committed = harness.db().commit(batch, CommitOptions::default()).await?;
                    harness.db().flush().await?;
                    harness
                        .stack_mut()
                        .projection_handle
                        .wait_for_watermark(committed)
                        .await?;
                    harness
                        .stack()
                        .workflow_runtime
                        .admit_callback(&order_id, "start", b"start".to_vec())
                        .await?;
                    wait_for_workflow_state(&harness, &order_id, "pending").await?;
                    checkpoint_app(&mut harness, "order-committed", &order_id).await?;

                    harness.stack().gateway.hold_from_driver();
                    let _timeout_error = deliver_next_order(
                        harness.db(),
                        harness.stack(),
                        &order_id,
                        false,
                        DELIVERY_TIMEOUT_WHILE_HELD,
                    )
                    .await
                    .expect_err("scripted gateway timeout should force a retry");
                    harness.stack().gateway.release_from_driver();
                    checkpoint_app(&mut harness, "gateway-timeout", &order_id).await?;

                    let delivered = deliver_next_order(
                        harness.db(),
                        harness.stack(),
                        &order_id,
                        false,
                        DELIVERY_TIMEOUT,
                    )
                    .await?;
                    harness.require_eq("delivered outbox batch size", &delivered, &1)?;
                    wait_for_workflow_state(&harness, &order_id, "charged").await?;
                    checkpoint_app(&mut harness, "delivered-without-cursor", &order_id).await?;

                    harness.restart(CutPoint::AfterStep).await?;
                    harness
                        .stack_mut()
                        .projection_handle
                        .wait_for_watermark(committed)
                        .await?;
                    checkpoint_app(&mut harness, "app-restarted", &order_id).await?;

                    let replayed = deliver_next_order(
                        harness.db(),
                        harness.stack(),
                        &order_id,
                        true,
                        DELIVERY_TIMEOUT,
                    )
                    .await?;
                    harness.require_eq("replayed outbox batch size", &replayed, &1)?;
                    wait_for_workflow_state(&harness, &order_id, "charged").await?;

                    assert_app_invariants(&mut harness, &order_id, 2).await
                }
                .await;

                let harness_shutdown = harness.shutdown().await;
                gateway.release_from_driver();
                let gateway_shutdown = gateway.shutdown().await;

                match (result, harness_shutdown, gateway_shutdown) {
                    (Ok(capture), Ok(()), Ok(())) => Ok(capture),
                    (Err(error), _, _) => Err(error),
                    (Ok(_), Err(error), _) => Err(error.into()),
                    (Ok(_), Ok(()), Err(error)) => Err(error.into()),
                }
            }
        })
}

#[test]
fn consumer_application_simulation_survives_timeout_restart_and_duplicate_delivery()
-> turmoil::Result {
    run_order_app_simulation(0x4201).map(|_| ())
}

#[test]
fn consumer_application_simulation_replays_same_seed() -> turmoil::Result {
    let first = run_order_app_simulation(0x4202)?;
    let second = run_order_app_simulation(0x4202)?;

    assert_eq!(first, second);
    Ok(())
}

#[test]
fn consumer_application_simulation_changes_shape_for_different_seeds() -> turmoil::Result {
    let left = run_order_app_simulation(0x4203)?;
    let right = run_order_app_simulation(0x4204)?;

    assert_ne!(left, right);
    Ok(())
}

#[test]
fn consumer_application_simulation_seed_campaign_is_reproducible() -> turmoil::Result {
    let seeds = [0x4210_u64, 0x4211, 0x4212];

    let first_pass = seeds
        .into_iter()
        .map(|seed| run_order_app_simulation(seed).map(|capture| (seed, capture)))
        .collect::<turmoil::Result<BTreeMap<_, _>>>()?;
    let second_pass = seeds
        .into_iter()
        .map(|seed| run_order_app_simulation(seed).map(|capture| (seed, capture)))
        .collect::<turmoil::Result<BTreeMap<_, _>>>()?;

    assert_eq!(first_pass, second_pass);
    assert!(
        first_pass.values().all(|capture| capture
            .trace
            .iter()
            .any(|event| matches!(event, TraceEvent::Restart))),
        "each seeded app campaign run should include the simulated restart"
    );

    Ok(())
}
