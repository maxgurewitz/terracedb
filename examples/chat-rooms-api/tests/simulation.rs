use std::{
    error::Error as StdError,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::http::{Method, Request, StatusCode};
use serde::{Serialize, de::DeserializeOwned};
use terracedb::{DbComponents, DeterministicRng, SimulatedFileSystem, SimulatedObjectStore};
use terracedb_example_chat_rooms_api::{
    CHAT_ROOMS_DATABASE_NAME, CHAT_ROOMS_SERVER_PORT, ChatRoomsApp, ChatRoomsExampleProfile,
    ChatRoomsObservabilityResponse, CreateRoomRequest, PostMessageRequest, RoomMessagesResponse,
    RoomShardInspection, chat_rooms_db_settings, find_room_id_for_partition, room_sharding_config,
};
use terracedb_http::{SimulatedHttpClient, axum_router_server_with_shutdown};
use terracedb_simulation::{SeededSimulationRunner, SimulationHost, TurmoilClock};
use tokio::sync::watch;

const SIM_DURATION: Duration = Duration::from_secs(5);
const MIN_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const MAX_MESSAGE_LATENCY: Duration = Duration::from_millis(1);
const SERVER_HOST: &str = "server";

type BoxError = Box<dyn StdError>;

fn boxed_error(error: impl StdError + 'static) -> BoxError {
    Box::new(error)
}

async fn request_json_checked<RequestBody, ResponseBody>(
    client: &SimulatedHttpClient,
    method: Method,
    path: &str,
    payload: Option<&RequestBody>,
) -> Result<(StatusCode, ResponseBody), BoxError>
where
    RequestBody: Serialize,
    ResponseBody: DeserializeOwned,
{
    let body = match payload {
        Some(value) => serde_json::to_vec(value).map_err(boxed_error)?,
        None => Vec::new(),
    };
    let mut builder = Request::builder().method(method.clone()).uri(path);
    if payload.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let request = builder.body(body).map_err(boxed_error)?;
    let response = client.send(request).await.map_err(boxed_error)?;
    let status = response.status();
    if !status.is_success() {
        return Err(boxed_error(std::io::Error::other(format!(
            "{method} {path} returned {status}: {}",
            String::from_utf8_lossy(response.body())
        ))));
    }
    let decoded = serde_json::from_slice(response.body()).map_err(boxed_error)?;
    Ok((status, decoded))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SimulationCapture {
    profile: ChatRoomsExampleProfile,
    hot_room_id: String,
    before_shard: RoomShardInspection,
    after_shard: RoomShardInspection,
    after_messages: RoomMessagesResponse,
    observability_before: ChatRoomsObservabilityResponse,
    observability_after: ChatRoomsObservabilityResponse,
}

fn chat_rooms_server_host(
    seed: u64,
    path: &'static str,
    prefix: &'static str,
    profile: ChatRoomsExampleProfile,
    shutdown: watch::Receiver<bool>,
) -> (SimulationHost, watch::Receiver<bool>) {
    let (ready_tx, ready_rx) = watch::channel(false);
    (
        SimulationHost::new(SERVER_HOST, move || {
            let shutdown = shutdown.clone();
            let ready_tx = ready_tx.clone();
            async move {
                let clock = Arc::new(TurmoilClock);
                let deployment = profile.deployment().map_err(boxed_error)?;
                let db = deployment
                    .open_database(
                        CHAT_ROOMS_DATABASE_NAME,
                        chat_rooms_db_settings(path, prefix),
                        DbComponents::new(
                            Arc::new(SimulatedFileSystem::default()),
                            Arc::new(SimulatedObjectStore::default()),
                            clock.clone(),
                            Arc::new(DeterministicRng::seeded(seed)),
                        ),
                    )
                    .await
                    .map_err(boxed_error)?;
                let app = ChatRoomsApp::open(deployment, db, profile, clock)
                    .await
                    .map_err(boxed_error)?;

                let server = axum_router_server_with_shutdown(
                    SERVER_HOST,
                    CHAT_ROOMS_SERVER_PORT,
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

fn workload_client_host(
    ready: watch::Receiver<bool>,
    done: watch::Sender<bool>,
    capture: Arc<Mutex<Option<SimulationCapture>>>,
    profile: ChatRoomsExampleProfile,
) -> SimulationHost {
    SimulationHost::new("client", move || {
        let ready = ready.clone();
        let done = done.clone();
        let capture = capture.clone();
        async move {
            let client = SimulatedHttpClient::new(SERVER_HOST, CHAT_ROOMS_SERVER_PORT);
            wait_for_ready(ready).await?;

            let hot_room_id = find_room_id_for_partition(
                &room_sharding_config(),
                terracedb::VirtualPartitionId::new(1),
                "hot",
            );
            let warm_room_id = find_room_id_for_partition(
                &room_sharding_config(),
                terracedb::VirtualPartitionId::new(2),
                "warm",
            );
            let cold_room_id = find_room_id_for_partition(
                &room_sharding_config(),
                terracedb::VirtualPartitionId::new(3),
                "cold",
            );
            for (room_id, title) in [
                (hot_room_id.as_str(), "Hot room"),
                (warm_room_id.as_str(), "Warm room"),
                (cold_room_id.as_str(), "Cold room"),
            ] {
                let (status, _room): (
                    StatusCode,
                    terracedb_example_chat_rooms_api::RoomStateRecord,
                ) = request_json_checked(
                    &client,
                    Method::POST,
                    "/rooms",
                    Some(&CreateRoomRequest {
                        room_id: room_id.to_string(),
                        title: title.to_string(),
                    }),
                )
                .await?;
                assert_eq!(status, StatusCode::CREATED);
            }

            for ordinal in 0..6 {
                let (status, _message): (
                    StatusCode,
                    terracedb_example_chat_rooms_api::ChatMessageRecord,
                ) = request_json_checked(
                    &client,
                    Method::POST,
                    &format!("/rooms/{hot_room_id}/messages"),
                    Some(&PostMessageRequest {
                        author: "load-generator".to_string(),
                        body: format!("hot-{ordinal}"),
                    }),
                )
                .await?;
                assert_eq!(status, StatusCode::CREATED);
            }
            for room_id in [&warm_room_id, &cold_room_id] {
                let (status, _message): (
                    StatusCode,
                    terracedb_example_chat_rooms_api::ChatMessageRecord,
                ) = request_json_checked(
                    &client,
                    Method::POST,
                    &format!("/rooms/{room_id}/messages"),
                    Some(&PostMessageRequest {
                        author: "load-generator".to_string(),
                        body: format!("{room_id}-one"),
                    }),
                )
                .await?;
                assert_eq!(status, StatusCode::CREATED);
            }

            let (status, before_shard): (StatusCode, RoomShardInspection) =
                request_json_checked::<CreateRoomRequest, RoomShardInspection>(
                    &client,
                    Method::GET,
                    &format!("/rooms/{hot_room_id}/shard"),
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);
            let (status, observability_before): (StatusCode, ChatRoomsObservabilityResponse) =
                request_json_checked::<CreateRoomRequest, ChatRoomsObservabilityResponse>(
                    &client,
                    Method::GET,
                    "/observability",
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);

            let (status, _reshard): (
                StatusCode,
                terracedb_example_chat_rooms_api::ReshardRoomResponse,
            ) = request_json_checked(
                &client,
                Method::POST,
                &format!("/rooms/{hot_room_id}/reshard"),
                Some(&terracedb_example_chat_rooms_api::ReshardRoomRequest {
                    target_physical_shard: 2,
                }),
            )
            .await?;
            assert_eq!(status, StatusCode::OK);

            let (status, _message): (
                StatusCode,
                terracedb_example_chat_rooms_api::ChatMessageRecord,
            ) = request_json_checked(
                &client,
                Method::POST,
                &format!("/rooms/{hot_room_id}/messages"),
                Some(&PostMessageRequest {
                    author: "load-generator".to_string(),
                    body: "after-reshard".to_string(),
                }),
            )
            .await?;
            assert_eq!(status, StatusCode::CREATED);

            let (status, after_shard): (StatusCode, RoomShardInspection) =
                request_json_checked::<CreateRoomRequest, RoomShardInspection>(
                    &client,
                    Method::GET,
                    &format!("/rooms/{hot_room_id}/shard"),
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);
            let (status, after_messages): (StatusCode, RoomMessagesResponse) =
                request_json_checked::<CreateRoomRequest, RoomMessagesResponse>(
                    &client,
                    Method::GET,
                    &format!("/rooms/{hot_room_id}/messages?limit=10"),
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);
            let (status, observability_after): (StatusCode, ChatRoomsObservabilityResponse) =
                request_json_checked::<CreateRoomRequest, ChatRoomsObservabilityResponse>(
                    &client,
                    Method::GET,
                    "/observability",
                    None,
                )
                .await?;
            assert_eq!(status, StatusCode::OK);

            *capture.lock().expect("capture mutex") = Some(SimulationCapture {
                profile,
                hot_room_id,
                before_shard,
                after_shard,
                after_messages,
                observability_before,
                observability_after,
            });
            let _ = done.send(true);
            Ok(())
        }
    })
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

async fn wait_for_done(mut done: watch::Receiver<bool>) -> Result<(), std::io::Error> {
    if !*done.borrow() {
        done.changed()
            .await
            .map_err(|_| std::io::Error::other("simulation client did not finish"))?;
    }
    Ok(())
}

fn run_seeded_workload(
    profile: ChatRoomsExampleProfile,
    seed: u64,
) -> turmoil::Result<SimulationCapture> {
    let capture = Arc::new(Mutex::new(None));
    let (done_tx, done_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (server, ready_rx) = chat_rooms_server_host(
        seed,
        "/chat-rooms-api-sim",
        "chat-rooms-api-sim",
        profile,
        shutdown_rx,
    );

    let capture_for_runner = capture.clone();
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(SIM_DURATION)
        .with_message_latency(MIN_MESSAGE_LATENCY, MAX_MESSAGE_LATENCY)
        .with_host(server)
        .with_host(workload_client_host(ready_rx, done_tx, capture, profile))
        .run_with(move |_context| async move {
            wait_for_done(done_rx).await.map_err(boxed_error)?;
            shutdown_tx.send_replace(true);
            Ok(())
        })?;

    match capture_for_runner.lock().expect("capture mutex").clone() {
        Some(capture) => Ok(capture),
        None => Err(std::io::Error::other("missing simulation capture").into()),
    }
}

#[test]
fn chat_rooms_simulation_is_seed_stable_through_hot_room_skew_and_reshard() -> turmoil::Result {
    let first = run_seeded_workload(ChatRoomsExampleProfile::DatabaseShared, 0x8301)?;
    let second = run_seeded_workload(ChatRoomsExampleProfile::DatabaseShared, 0x8301)?;

    assert_eq!(first, second);
    assert_eq!(first.before_shard.virtual_partition, 1);
    assert_eq!(first.before_shard.physical_shard, 0);
    assert_eq!(first.after_shard.physical_shard, 2);
    assert_eq!(first.after_messages.room.message_count, 7);
    assert_eq!(first.after_messages.messages[0].body, "after-reshard");
    assert_eq!(first.after_messages.messages[6].body, "hot-0");
    assert!(
        first
            .observability_before
            .database_background_backlog
            .queued_work_items
            > 0,
        "the hot room should publish backlog before resharding",
    );
    Ok(())
}

#[test]
fn execution_profile_changes_backlog_placement_not_room_contents() -> turmoil::Result {
    let shared = run_seeded_workload(ChatRoomsExampleProfile::DatabaseShared, 0x8302)?;
    let shard_local = run_seeded_workload(ChatRoomsExampleProfile::ShardLocal, 0x8302)?;

    assert_eq!(shared.after_messages, shard_local.after_messages);
    assert_eq!(
        shared.after_shard.virtual_partition,
        shard_local.after_shard.virtual_partition
    );
    assert_eq!(
        shared.after_shard.physical_shard,
        shard_local.after_shard.physical_shard
    );

    let shared_hot = shared
        .observability_after
        .rooms
        .iter()
        .find(|room| room.room_id == shared.hot_room_id)
        .expect("shared hot room");
    let local_hot = shard_local
        .observability_after
        .rooms
        .iter()
        .find(|room| room.room_id == shard_local.hot_room_id)
        .expect("local hot room");
    assert_eq!(shared_hot.physical_shard, local_hot.physical_shard);
    assert_eq!(shared_hot.shard_map_revision, local_hot.shard_map_revision);
    assert_eq!(
        shared_hot.active_background_domain,
        shared.observability_after.database_background_domain
    );
    assert_ne!(
        local_hot.active_background_domain,
        shard_local.observability_after.database_background_domain,
        "shard-local profile should move backlog onto a shard-local lane",
    );
    assert!(
        shared
            .observability_after
            .database_background_backlog
            .queued_work_items
            >= shard_local
                .observability_after
                .database_background_backlog
                .queued_work_items,
        "database-shared profile should keep at least as much backlog on the shared background lane",
    );
    Ok(())
}
