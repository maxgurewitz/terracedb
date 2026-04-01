use std::{error::Error as StdError, sync::Arc, time::Duration};

use axum::http::{Method, StatusCode};
use terracedb::{DeterministicRng, SimulatedFileSystem};
use terracedb_example_todo_api::{
    CreateTodoRequest, MILLIS_PER_DAY, PlannerSchedule, TODO_SERVER_PORT, TodoApp, TodoAppOptions,
    TodoRecord, TodoStatus, UpdateTodoRequest, todo_db_builder,
};
use terracedb_systemtest::{
    http::{SimulatedHttpClient, axum_router_server_with_shutdown},
    simulation::{NetworkObjectStore, SeededSimulationRunner, SimulationHost, TurmoilClock},
};
use tokio::sync::watch;

const SIM_DURATION: Duration = Duration::from_secs(5);
const MIN_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const MAX_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const TEST_DAY_MILLIS: u64 = 10;
const TEST_WEEK_MILLIS: u64 = TEST_DAY_MILLIS * 7;
const HAPPY_PATH_DAY_MILLIS: u64 = 1_000;
const OBJECT_STORE_HOST: &str = "object-store";
const OBJECT_STORE_PORT: u16 = 9400;
const SERVER_HOST: &str = "server";

type BoxError = Box<dyn StdError>;

fn boxed_error(error: impl StdError + Send + Sync + 'static) -> BoxError {
    Box::new(error)
}

fn todo_server_host(
    seed: u64,
    path: &'static str,
    prefix: &'static str,
    planner_schedule: PlannerSchedule,
    shutdown: watch::Receiver<bool>,
) -> (SimulationHost, watch::Receiver<bool>, watch::Receiver<bool>) {
    let (ready_tx, ready_rx) = watch::channel(false);
    let (planner_ready_tx, planner_ready_rx) = watch::channel(false);
    (
        SimulationHost::new(SERVER_HOST, move || {
            let shutdown = shutdown.clone();
            let ready_tx = ready_tx.clone();
            let planner_ready_tx = planner_ready_tx.clone();
            async move {
                let clock = Arc::new(TurmoilClock);
                let db = todo_db_builder(path, prefix)
                    .file_system(Arc::new(SimulatedFileSystem::default()))
                    .object_store(Arc::new(NetworkObjectStore::new(
                        OBJECT_STORE_HOST,
                        OBJECT_STORE_PORT,
                    )))
                    .clock(clock.clone())
                    .rng(Arc::new(DeterministicRng::seeded(seed)))
                    .open()
                    .await
                    .map_err(boxed_error)?;
                let app =
                    TodoApp::open_with_options(db, clock, TodoAppOptions { planner_schedule })
                        .await
                        .map_err(boxed_error)?;
                let mut planner_ready = app.planner_ready();
                let server = axum_router_server_with_shutdown(
                    SERVER_HOST,
                    TODO_SERVER_PORT,
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
                tokio::spawn(async move {
                    if !*planner_ready.borrow() {
                        let _ = planner_ready.changed().await;
                    }
                    let _ = planner_ready_tx.send(true);
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
        planner_ready_rx,
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
            let client = SimulatedHttpClient::new(SERVER_HOST, TODO_SERVER_PORT);
            wait_for_ready(ready).await?;

            let create = CreateTodoRequest {
                todo_id: "todo-1".to_string(),
                title: "Write docs".to_string(),
                notes: "first draft".to_string(),
                scheduled_for_day: Some(2 * MILLIS_PER_DAY),
            };
            let (status, created): (StatusCode, TodoRecord) = client
                .request_json(Method::POST, "/todos", Some(&create))
                .await?;
            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(created.todo_id, "todo-1");
            assert_eq!(created.status, TodoStatus::Open);

            let (status, fetched): (StatusCode, TodoRecord) = client
                .request_json::<CreateTodoRequest, TodoRecord>(Method::GET, "/todos/todo-1", None)
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(fetched, created);

            let update = UpdateTodoRequest {
                title: Some("Write better docs".to_string()),
                notes: Some("second draft".to_string()),
                scheduled_for_day: Some(3 * MILLIS_PER_DAY),
            };
            let (status, updated): (StatusCode, TodoRecord) = client
                .request_json(Method::PATCH, "/todos/todo-1", Some(&update))
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(updated.todo_id, "todo-1");
            assert_eq!(updated.title, "Write better docs");
            assert_eq!(updated.notes, "second draft");
            assert_eq!(updated.scheduled_for_day, Some(3 * MILLIS_PER_DAY));

            let (status, listed): (StatusCode, Vec<TodoRecord>) = client
                .request_json::<CreateTodoRequest, Vec<TodoRecord>>(Method::GET, "/todos", None)
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0], updated);

            let (status, recent): (StatusCode, Vec<TodoRecord>) = client
                .request_json::<CreateTodoRequest, Vec<TodoRecord>>(
                    Method::GET,
                    "/todos/recent",
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(recent.len(), 1);
            assert_eq!(recent[0], updated);

            let _ = done.send(true);
            Ok(())
        }
    })
}

fn weekly_planner_client_host(
    done: watch::Sender<bool>,
    ready: watch::Receiver<bool>,
    planner_ready: watch::Receiver<bool>,
) -> SimulationHost {
    SimulationHost::new("client", move || {
        let done = done.clone();
        let ready = ready.clone();
        let planner_ready = planner_ready.clone();
        async move {
            let client = SimulatedHttpClient::new(SERVER_HOST, TODO_SERVER_PORT);
            wait_for_ready(ready).await?;
            wait_for_ready(planner_ready).await?;

            let (status, todos): (StatusCode, Vec<TodoRecord>) = client
                .request_json::<CreateTodoRequest, Vec<TodoRecord>>(Method::GET, "/todos", None)
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(todos.len(), 7);
            assert!(todos.iter().all(|todo| todo.placeholder));
            assert_eq!(
                todos.first().and_then(|todo| todo.scheduled_for_day),
                Some(TEST_WEEK_MILLIS)
            );
            assert_eq!(
                todos.last().and_then(|todo| todo.scheduled_for_day),
                Some(TEST_WEEK_MILLIS + 6 * TEST_DAY_MILLIS)
            );

            tokio::time::sleep(Duration::from_millis(TEST_DAY_MILLIS)).await;
            let (status, after_extra_day): (StatusCode, Vec<TodoRecord>) = client
                .request_json::<CreateTodoRequest, Vec<TodoRecord>>(Method::GET, "/todos", None)
                .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(after_extra_day.len(), 7);

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

#[test]
fn todo_api_simulation_supports_create_read_update_list_and_recent_projection() -> turmoil::Result {
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (server, ready_rx, _planner_ready_rx) = todo_server_host(
        0x5151,
        "/todo-api-happy",
        "todo-api-happy",
        PlannerSchedule::new(HAPPY_PATH_DAY_MILLIS, 7),
        shutdown_rx,
    );

    SeededSimulationRunner::new(0x5151)
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
fn todo_api_weekly_planner_time_travel_creates_placeholders() -> turmoil::Result {
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (server, ready_rx, planner_ready_rx) = todo_server_host(
        0x6161,
        "/todo-api-weekly",
        "todo-api-weekly",
        PlannerSchedule::new(TEST_DAY_MILLIS, 7),
        shutdown_rx,
    );

    SeededSimulationRunner::new(0x6161)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .with_host(server)
        .with_host(weekly_planner_client_host(
            done_tx,
            ready_rx,
            planner_ready_rx,
        ))
        .run_with(move |_context| async move {
            wait_for_done(done_rx).await.map_err(boxed_error)?;
            shutdown_tx.send_replace(true);
            Ok(())
        })
}
