use std::{io, sync::Arc, time::Duration};

#[cfg(feature = "axum")]
use axum::{Json, Router, routing::get};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
#[cfg(feature = "axum")]
use terracedb_http::axum_router_server_with_shutdown;
use terracedb_http::{HttpTraceEvent, SimulatedHttpClient, SimulatedHttpServer};
use terracedb_simulation::{SeededSimulationRunner, SimulationHost};
use tokio::sync::{Mutex, watch};

const SERVER_HOST: &str = "server";
const CLIENT_HOST: &str = "client";
const SERVER_PORT: u16 = 9781;
const SIM_DURATION: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TodoItem {
    id: String,
    title: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Ack {
    value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReproOutcome {
    server_trace: Vec<HttpTraceEvent>,
    client_trace: Vec<HttpTraceEvent>,
}

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> io::Result<Response<Full<Bytes>>> {
    let body = serde_json::to_vec(value).map_err(io::Error::other)?;
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .map_err(io::Error::other)
}

async fn wait_for_done(mut done: watch::Receiver<bool>) -> io::Result<()> {
    if !*done.borrow() {
        done.changed()
            .await
            .map_err(|_| io::Error::other("simulation client did not finish"))?;
    }
    Ok(())
}

async fn wait_for_ready(mut ready: watch::Receiver<bool>) -> io::Result<()> {
    if !*ready.borrow() {
        ready
            .changed()
            .await
            .map_err(|_| io::Error::other("simulation server did not become ready"))?;
    }
    Ok(())
}

#[test]
fn simulated_http_happy_path_supports_create_read_and_list() -> turmoil::Result {
    let store = Arc::new(Mutex::new(Vec::<TodoItem>::new()));
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server = SimulatedHttpServer::with_shutdown(SERVER_HOST, SERVER_PORT, shutdown_rx, {
        let store = store.clone();
        move |request| {
            let store = store.clone();
            async move {
                match (request.method().clone(), request_target(&request)) {
                    (Method::POST, path) if path == "/todos" => {
                        let bytes = request
                            .into_body()
                            .collect()
                            .await
                            .map_err(io::Error::other)?
                            .to_bytes();
                        let created: TodoItem =
                            serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                        store.lock().await.push(created.clone());
                        json_response(StatusCode::CREATED, &created)
                    }
                    (Method::GET, path) if path == "/todos/1" => {
                        let current = store.lock().await;
                        let Some(item) = current.iter().find(|item| item.id == "1") else {
                            return Err(io::Error::new(io::ErrorKind::NotFound, "missing todo"));
                        };
                        json_response(StatusCode::OK, item)
                    }
                    (Method::GET, path) if path == "/todos" => {
                        let current = store.lock().await.clone();
                        json_response(StatusCode::OK, &current)
                    }
                    _ => json_response(
                        StatusCode::NOT_FOUND,
                        &Ack {
                            value: "missing".to_string(),
                        },
                    ),
                }
            }
        }
    });
    let ready = server.ready();
    let client = SimulatedHttpClient::new(SERVER_HOST, SERVER_PORT);

    SeededSimulationRunner::new(0x3205)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(Duration::from_millis(1), Duration::from_millis(1))
        .with_host(server.simulation_host())
        .with_host(SimulationHost::new(CLIENT_HOST, move || {
            let client = client.clone();
            let done_tx = done_tx.clone();
            let ready = ready.clone();
            async move {
                wait_for_ready(ready).await?;

                let create = TodoItem {
                    id: "1".to_string(),
                    title: "write docs".to_string(),
                };
                let (status, created): (StatusCode, TodoItem) = client
                    .request_json(Method::POST, "/todos", Some(&create))
                    .await?;
                assert_eq!(status, StatusCode::CREATED);
                assert_eq!(created, create);

                let (status, fetched): (StatusCode, TodoItem) = client
                    .request_json::<TodoItem, TodoItem>(Method::GET, "/todos/1", None)
                    .await?;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(fetched, create);

                let (status, listed): (StatusCode, Vec<TodoItem>) = client
                    .request_json::<TodoItem, Vec<TodoItem>>(Method::GET, "/todos", None)
                    .await?;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(listed, vec![create]);

                let _ = done_tx.send(true);
                Ok(())
            }
        }))
        .run_with(move |_context| async move {
            wait_for_done(done_rx).await?;
            shutdown_tx.send_replace(true);
            Ok(())
        })
}

#[test]
fn simulated_http_fault_injection_handles_partitions_and_delayed_responses() -> turmoil::Result {
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server = SimulatedHttpServer::with_shutdown(
        SERVER_HOST,
        SERVER_PORT,
        shutdown_rx,
        |_request| async move {
            tokio::time::sleep(Duration::from_millis(8)).await;
            json_response(
                StatusCode::OK,
                &Ack {
                    value: "ready".to_string(),
                },
            )
        },
    );
    let ready = server.ready();
    let client = SimulatedHttpClient::new(SERVER_HOST, SERVER_PORT)
        .with_request_timeout(Duration::from_millis(5))
        .with_connect_retries(50, Duration::from_millis(1));

    SeededSimulationRunner::new(0x3206)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(Duration::from_millis(1), Duration::from_millis(1))
        .with_host(server.simulation_host())
        .with_host(SimulationHost::new(CLIENT_HOST, move || {
            let client = client.clone();
            let done_tx = done_tx.clone();
            let ready = ready.clone();
            async move {
                wait_for_ready(ready).await?;
                tokio::time::sleep(Duration::from_millis(2)).await;
                let first_error = client
                    .request_json::<Ack, Ack>(Method::GET, "/slow", None)
                    .await
                    .expect_err("partitioned request should time out");
                assert_eq!(first_error.kind(), io::ErrorKind::TimedOut);

                tokio::time::sleep(Duration::from_millis(15)).await;
                let retried = client
                    .clone()
                    .with_request_timeout(Duration::from_millis(40));
                let (status, ack): (StatusCode, Ack) = retried
                    .request_json::<Ack, Ack>(Method::GET, "/slow", None)
                    .await?;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(ack.value, "ready");

                let _ = done_tx.send(true);
                Ok(())
            }
        }))
        .run_with(move |_context| async move {
            server.partition_from(CLIENT_HOST);
            tokio::time::sleep(Duration::from_millis(10)).await;
            server.release_from(CLIENT_HOST);
            wait_for_done(done_rx).await?;
            shutdown_tx.send_replace(true);
            Ok(())
        })
}

#[test]
fn simulated_http_fault_injection_handles_dropped_connections_restart_and_retry() -> turmoil::Result
{
    let (done_tx, done_rx) = watch::channel(false);
    let (first_shutdown_tx, first_shutdown_rx) = watch::channel(false);
    let (second_shutdown_tx, second_shutdown_rx) = watch::channel(false);

    let first_server = SimulatedHttpServer::with_shutdown(
        SERVER_HOST,
        SERVER_PORT,
        first_shutdown_rx,
        |_request| async move {
            Err::<Response<Full<Bytes>>, _>(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "drop connection before reply",
            ))
        },
    );
    let second_server = SimulatedHttpServer::with_shutdown(
        SERVER_HOST,
        SERVER_PORT,
        second_shutdown_rx,
        |_request| async move {
            json_response(
                StatusCode::OK,
                &Ack {
                    value: "recovered".to_string(),
                },
            )
        },
    );
    let ready = first_server.ready();

    let host = SimulationHost::new(SERVER_HOST, move || {
        let first_server = first_server.clone();
        let second_server = second_server.clone();
        async move {
            first_server.run().await?;
            second_server.run().await?;
            Ok(())
        }
    });
    let client = SimulatedHttpClient::new(SERVER_HOST, SERVER_PORT)
        .with_request_timeout(Duration::from_millis(20))
        .with_connect_retries(100, Duration::from_millis(1));

    SeededSimulationRunner::new(0x3207)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(Duration::from_millis(1), Duration::from_millis(1))
        .with_host(host)
        .with_host(SimulationHost::new(CLIENT_HOST, move || {
            let client = client.clone();
            let done_tx = done_tx.clone();
            let first_shutdown_tx = first_shutdown_tx.clone();
            let second_shutdown_tx = second_shutdown_tx.clone();
            let ready = ready.clone();
            async move {
                wait_for_ready(ready).await?;
                let request = Request::builder()
                    .method(Method::GET)
                    .uri("/retry")
                    .body(Bytes::new())
                    .map_err(io::Error::other)?;
                assert!(client.send(request).await.is_err());

                first_shutdown_tx.send_replace(true);
                tokio::time::sleep(Duration::from_millis(10)).await;

                let (status, ack): (StatusCode, Ack) = client
                    .request_json::<Ack, Ack>(Method::GET, "/retry", None)
                    .await?;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(ack.value, "recovered");

                second_shutdown_tx.send_replace(true);
                let _ = done_tx.send(true);
                Ok(())
            }
        }))
        .run_with(move |_context| async move {
            wait_for_done(done_rx).await?;
            Ok(())
        })
}

#[cfg(feature = "axum")]
#[test]
fn axum_adapter_serves_routes_through_the_same_http_harness() -> turmoil::Result {
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let router = Router::new().route(
        "/ping",
        get(|| async {
            Json(Ack {
                value: "pong".to_string(),
            })
        }),
    );
    let server = axum_router_server_with_shutdown(SERVER_HOST, SERVER_PORT, router, shutdown_rx);
    let ready = server.ready();
    let client = SimulatedHttpClient::new(SERVER_HOST, SERVER_PORT);

    SeededSimulationRunner::new(0x3208)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(Duration::from_millis(1), Duration::from_millis(1))
        .with_host(server.simulation_host())
        .with_host(SimulationHost::new(CLIENT_HOST, move || {
            let client = client.clone();
            let done_tx = done_tx.clone();
            let ready = ready.clone();
            async move {
                wait_for_ready(ready).await?;
                let (status, ack): (StatusCode, Ack) = client
                    .request_json::<Ack, Ack>(Method::GET, "/ping", None)
                    .await?;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(ack.value, "pong");
                let _ = done_tx.send(true);
                Ok(())
            }
        }))
        .run_with(move |_context| async move {
            wait_for_done(done_rx).await?;
            shutdown_tx.send_replace(true);
            Ok(())
        })?;

    assert!(server.trace().events().iter().any(|event| matches!(
        event,
        HttpTraceEvent::ServerRequestReceived { path, .. } if path == "/ping"
    )));
    Ok(())
}

#[test]
fn same_seed_replays_the_same_http_trace() -> turmoil::Result {
    let first = run_trace_scenario(0x3209)?;
    let second = run_trace_scenario(0x3209)?;
    assert_eq!(first, second);
    Ok(())
}

fn run_trace_scenario(seed: u64) -> turmoil::Result<ReproOutcome> {
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = SimulatedHttpServer::with_shutdown(
        SERVER_HOST,
        SERVER_PORT,
        shutdown_rx,
        |_request| async move {
            json_response(
                StatusCode::OK,
                &Ack {
                    value: "trace".to_string(),
                },
            )
        },
    );
    let ready = server.ready();
    let client = SimulatedHttpClient::new(SERVER_HOST, SERVER_PORT);
    let client_host_client = client.clone();

    SeededSimulationRunner::new(seed)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(Duration::from_millis(1), Duration::from_millis(1))
        .with_host(server.simulation_host())
        .with_host(SimulationHost::new(CLIENT_HOST, move || {
            let client = client_host_client.clone();
            let done_tx = done_tx.clone();
            let ready = ready.clone();
            async move {
                wait_for_ready(ready).await?;
                let (status, ack): (StatusCode, Ack) = client
                    .request_json::<Ack, Ack>(Method::GET, "/trace", None)
                    .await?;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(ack.value, "trace");
                let _ = done_tx.send(true);
                Ok(())
            }
        }))
        .run_with(move |_context| async move {
            wait_for_done(done_rx).await?;
            shutdown_tx.send_replace(true);
            Ok(())
        })?;

    Ok(ReproOutcome {
        server_trace: server.trace().events(),
        client_trace: client.trace().events(),
    })
}

fn request_target(request: &Request<impl Sized>) -> String {
    request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string())
}
