use std::{error::Error as StdError, sync::Arc, time::Duration};

use axum::http::{Method, StatusCode};
use terracedb::{Db, DbDependencies, DeterministicRng, SimulatedFileSystem};
use terracedb_example_todo_api::{
    CreateTodoRequest, MILLIS_PER_DAY, PlannerSchedule, TODO_SERVER_PORT, TodoApp, TodoAppOptions,
    TodoRecord, TodoStatus, UpdateTodoRequest, todo_db_config,
};
use terracedb_http::{SimulatedHttpClient, axum_router_server_with_shutdown};
use terracedb_simulation::{
    NetworkObjectStore, SeededSimulationRunner, SimulationHost, TurmoilClock,
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
) -> SimulationHost {
    SimulationHost::new(SERVER_HOST, move || {
        let shutdown = shutdown.clone();
        async move {
            let clock = Arc::new(TurmoilClock);
            let db = Db::open(
                todo_db_config(path, prefix),
                DbDependencies::new(
                    Arc::new(SimulatedFileSystem::default()),
                    Arc::new(NetworkObjectStore::new(
                        OBJECT_STORE_HOST,
                        OBJECT_STORE_PORT,
                    )),
                    clock.clone(),
                    Arc::new(DeterministicRng::seeded(seed)),
                ),
            )
            .await
            .map_err(boxed_error)?;
            let app = TodoApp::open_with_options(db, clock, TodoAppOptions { planner_schedule })
                .await
                .map_err(boxed_error)?;

            let server = axum_router_server_with_shutdown(
                SERVER_HOST,
                TODO_SERVER_PORT,
                app.router(),
                shutdown,
            );
            let serve_result = server.run().await.map_err(boxed_error);
            let shutdown_result = app.shutdown().await.map_err(boxed_error);
            match (serve_result, shutdown_result) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(error), _) => Err(error),
                (Ok(()), Err(error)) => Err(error),
            }
        }
    })
}

fn happy_path_client_host(done: watch::Sender<bool>) -> SimulationHost {
    SimulationHost::new("client", move || {
        let done = done.clone();
        async move {
            let client = SimulatedHttpClient::new(SERVER_HOST, TODO_SERVER_PORT);
            tokio::time::sleep(Duration::from_millis(25)).await;

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

            let mut recent = Vec::new();
            for _ in 0..100 {
                let (status, current): (StatusCode, Vec<TodoRecord>) = client
                    .request_json::<CreateTodoRequest, Vec<TodoRecord>>(
                        Method::GET,
                        "/todos/recent",
                        None,
                    )
                    .await?;
                assert_eq!(status, StatusCode::OK);
                recent = current;
                if recent.first().map(|todo| todo.updated_at_ms) == Some(updated.updated_at_ms) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(recent.len(), 1);
            assert_eq!(recent[0], updated);

            let _ = done.send(true);
            Ok(())
        }
    })
}

fn weekly_planner_client_host(done: watch::Sender<bool>) -> SimulationHost {
    SimulationHost::new("client", move || {
        let done = done.clone();
        async move {
            let client = SimulatedHttpClient::new(SERVER_HOST, TODO_SERVER_PORT);
            tokio::time::sleep(Duration::from_millis(TEST_WEEK_MILLIS + 50)).await;

            let mut todos = Vec::new();
            for _ in 0..400 {
                let (status, current): (StatusCode, Vec<TodoRecord>) = client
                    .request_json::<CreateTodoRequest, Vec<TodoRecord>>(Method::GET, "/todos", None)
                    .await?;
                assert_eq!(status, StatusCode::OK);
                todos = current;
                if todos.len() == 7 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
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

#[test]
fn todo_api_simulation_supports_create_read_update_list_and_recent_projection() -> turmoil::Result {
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    SeededSimulationRunner::new(0x5151)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .with_host(todo_server_host(
            0x5151,
            "/todo-api-happy",
            "todo-api-happy",
            PlannerSchedule::new(HAPPY_PATH_DAY_MILLIS, 7),
            shutdown_rx,
        ))
        .with_host(happy_path_client_host(done_tx))
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

    SeededSimulationRunner::new(0x6161)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .with_host(todo_server_host(
            0x6161,
            "/todo-api-weekly",
            "todo-api-weekly",
            PlannerSchedule::new(TEST_DAY_MILLIS, 7),
            shutdown_rx,
        ))
        .with_host(weekly_planner_client_host(done_tx))
        .run_with(move |_context| async move {
            wait_for_done(done_rx).await.map_err(boxed_error)?;
            shutdown_tx.send_replace(true);
            Ok(())
        })
}
