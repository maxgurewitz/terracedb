use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{Arc, Mutex as StdMutex},
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::StreamExt;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use terracedb::{
    Clock, ColocatedDeployment, CommitError, CreateTableError, Db, DbSettings,
    DomainBackgroundBudget, DomainCpuBudget, ExecutionBacklogPublisher,
    ExecutionDomainBacklogSnapshot, ExecutionDomainBudget, ExecutionDomainPath, ExecutionLane,
    KeyShardRoute, PhysicalShardId, PublishShardMapError, ReshardPlanError,
    ShardExecutionDomainProfile, ShardHashAlgorithm, ShardMapRevision, ShardingConfig,
    ShardingError, StorageError, Table, TableConfig, TieredDurabilityMode, TieredStorageConfig,
    TransactionCommitError, VirtualPartitionId,
};
use terracedb_records::{
    JsonValueCodec, KeyCodec, RecordCodecError, RecordKeyspace, RecordKeyspaceError,
    RecordKeyspaceReshardError, RecordReadError, RecordStream, RecordTable, RecordTransaction,
    RecordTransactionFuture, RecordTransactionRunError, RecordWriteError,
};

use crate::model::{
    CHAT_ROOMS_DATABASE_NAME, ChatMessageRecord, ChatRoomsExampleProfile,
    ChatRoomsObservabilityResponse, CreateRoomRequest, PostMessageRequest,
    ROOM_MESSAGES_TABLE_NAME, ROOM_STATE_TABLE_NAME, RecentMessagesQuery, ReshardRoomRequest,
    ReshardRoomResponse, ReshardTableStateView, RoomBacklogView, RoomMessagesResponse,
    RoomShardInspection, RoomStateRecord, ShardObservabilityView, VirtualPartitionAssignment,
};

pub const CHAT_ROOMS_VIRTUAL_PARTITION_COUNT: u32 = 4;
const INITIAL_SHARD_MAP_REVISION: u64 = 7;
const BACKLOG_FREE_MESSAGES_PER_ROOM: usize = 2;
const DEFAULT_RECENT_MESSAGE_LIMIT: usize = 20;
const MAX_RECENT_MESSAGE_LIMIT: usize = 100;

type RoomStateTable =
    RecordTable<RoomShardKey, RoomStateRecord, RoomShardKeyCodec, JsonValueCodec<RoomStateRecord>>;
type RoomMessagesTable = RecordTable<
    RoomShardKey,
    RoomMessagesRecord,
    RoomShardKeyCodec,
    JsonValueCodec<RoomMessagesRecord>,
>;
type RoomKeyspace = RecordKeyspace<RoomShardKey, RoomShardKeyCodec>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RoomShardKey {
    room_id: String,
}

impl RoomShardKey {
    fn new(room_id: impl Into<String>) -> Self {
        Self {
            room_id: room_id.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct RoomShardKeyCodec;

impl KeyCodec<RoomShardKey> for RoomShardKeyCodec {
    fn encode_key(&self, key: &RoomShardKey) -> Result<Vec<u8>, RecordCodecError> {
        Ok(format!("room:{}", key.room_id).into_bytes())
    }

    fn decode_key(&self, key: &[u8]) -> Result<RoomShardKey, RecordCodecError> {
        let key = std::str::from_utf8(key).map_err(RecordCodecError::decode_key)?;
        let room_id = key.strip_prefix("room:").ok_or_else(|| {
            RecordCodecError::decode_key(io::Error::other(format!(
                "room shard key {key} is missing the room: prefix"
            )))
        })?;
        Ok(RoomShardKey::new(room_id))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
struct RoomMessagesRecord {
    room_id: String,
    messages: Vec<ChatMessageRecord>,
}

#[derive(Default)]
struct RoomWriteLocks {
    by_room: StdMutex<BTreeMap<String, Arc<AsyncMutex<()>>>>,
}

#[derive(Debug, Error)]
pub enum ChatRoomsAppError {
    #[error(transparent)]
    CreateTable(#[from] CreateTableError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Commit(#[from] CommitError),
    #[error(transparent)]
    TransactionCommit(#[from] TransactionCommitError),
    #[error(transparent)]
    RecordRead(#[from] RecordReadError),
    #[error(transparent)]
    RecordWrite(#[from] RecordWriteError),
    #[error(transparent)]
    RecordCodec(#[from] RecordCodecError),
    #[error(transparent)]
    ReshardPlan(#[from] ReshardPlanError),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    InvalidConfig(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ChatRoomsApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Internal(String),
}

impl From<ChatRoomsAppError> for ChatRoomsApiError {
    fn from(error: ChatRoomsAppError) -> Self {
        match error {
            ChatRoomsAppError::NotFound(message) => Self::NotFound(message),
            ChatRoomsAppError::Conflict(message) => Self::Conflict(message),
            ChatRoomsAppError::Usage(message) | ChatRoomsAppError::InvalidConfig(message) => {
                Self::BadRequest(message)
            }
            other => Self::Internal(other.to_string()),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ChatRoomsApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(ErrorBody {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

#[derive(Clone, Debug)]
pub struct ChatRoomsTables {
    room_state: RoomStateTable,
    room_messages: RoomMessagesTable,
    room_keyspace: RoomKeyspace,
}

impl ChatRoomsTables {
    pub fn room_state_raw(&self) -> &Table {
        self.room_state.table()
    }

    pub fn room_messages_raw(&self) -> &Table {
        self.room_messages.table()
    }
}

#[derive(Clone)]
pub struct ChatRoomsAppState {
    db: Db,
    clock: Arc<dyn Clock>,
    profile: ChatRoomsExampleProfile,
    tables: ChatRoomsTables,
    room_write_locks: Arc<RoomWriteLocks>,
    backlog: ExecutionBacklogPublisher,
}

impl std::fmt::Debug for ChatRoomsAppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatRoomsAppState")
            .field("profile", &self.profile)
            .field("tables", &self.tables)
            .finish()
    }
}

pub struct ChatRoomsApp {
    state: ChatRoomsAppState,
}

impl ChatRoomsApp {
    pub async fn open(
        deployment: ColocatedDeployment,
        db: Db,
        profile: ChatRoomsExampleProfile,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ChatRoomsAppError> {
        if db.execution_identity() != CHAT_ROOMS_DATABASE_NAME {
            return Err(ChatRoomsAppError::InvalidConfig(format!(
                "expected execution identity '{CHAT_ROOMS_DATABASE_NAME}', found '{}'",
                db.execution_identity()
            )));
        }
        if deployment
            .execution_profile(CHAT_ROOMS_DATABASE_NAME)
            .is_none()
        {
            return Err(ChatRoomsAppError::InvalidConfig(
                "chat rooms example requires a colocated shard-ready deployment".to_string(),
            ));
        }

        let tables = ensure_chat_rooms_tables(&db).await?;
        let backlog = db.backlog_publisher();
        let state = ChatRoomsAppState {
            db,
            clock,
            profile,
            tables,
            room_write_locks: Arc::new(RoomWriteLocks::default()),
            backlog,
        };

        if profile.uses_shard_local_execution() {
            state.register_shard_execution_domains()?;
        }
        state.publish_backlog().await?;

        Ok(Self { state })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/rooms", post(create_room))
            .route(
                "/rooms/{room_id}/messages",
                post(post_message).get(get_recent_messages),
            )
            .route("/rooms/{room_id}/shard", get(get_room_shard))
            .route("/rooms/{room_id}/reshard", post(reshard_room))
            .route("/observability", get(get_observability))
            .with_state(self.state.clone())
    }

    pub fn state(&self) -> &ChatRoomsAppState {
        &self.state
    }

    pub fn tables(&self) -> &ChatRoomsTables {
        &self.state.tables
    }

    pub async fn shutdown(self) -> Result<(), ChatRoomsAppError> {
        self.state.clear_backlog_publication().await;
        Ok(())
    }
}

impl ChatRoomsAppState {
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn profile(&self) -> ChatRoomsExampleProfile {
        self.profile
    }

    pub fn tables(&self) -> &ChatRoomsTables {
        &self.tables
    }

    pub async fn create_room(
        &self,
        request: CreateRoomRequest,
    ) -> Result<RoomStateRecord, ChatRoomsAppError> {
        let room_id = normalize_room_id(&request.room_id)?;
        let title = normalize_non_empty("title", &request.title)?;
        let write_lock = self.room_write_lock(&room_id);
        let _write_guard = write_lock.lock().await;
        let key = RoomShardKey::new(room_id.clone());
        let _ = self.require_room_route(&key, &room_id)?;

        let tables = self.tables.clone();
        let clock = self.clock.clone();
        let room_id_for_tx = room_id.clone();
        let title_for_tx = title.clone();
        let key_for_tx = key.clone();
        let (state, _sequence) = RecordTransaction::run_conflict_retry_loop(
            &self.db,
            RecordTransaction::replay_safe(move |tx: &mut RecordTransaction| {
                let tables = tables.clone();
                let clock = clock.clone();
                let room_id = room_id_for_tx.clone();
                let title = title_for_tx.clone();
                let key = key_for_tx.clone();
                let future: RecordTransactionFuture<'_, _, _> = Box::pin(async move {
                    if tx.read(&tables.room_state, &key).await?.is_some() {
                        return Err(ChatRoomsAppError::Conflict(format!(
                            "room '{room_id}' already exists"
                        )));
                    }

                    let now_ms = clock.now().get();
                    let state = RoomStateRecord {
                        room_id: room_id.clone(),
                        title,
                        created_at_ms: now_ms,
                        message_count: 0,
                        last_message_at_ms: None,
                    };
                    let log = RoomMessagesRecord {
                        room_id,
                        messages: Vec::new(),
                    };
                    tx.write(&tables.room_state, &key, &state)?;
                    tx.write(&tables.room_messages, &key, &log)?;
                    Ok(state)
                });
                future
            }),
        )
        .await
        .map_err(map_record_transaction_error)?;
        self.publish_backlog().await?;
        Ok(state)
    }

    pub async fn post_message(
        &self,
        room_id: &str,
        request: PostMessageRequest,
    ) -> Result<ChatMessageRecord, ChatRoomsAppError> {
        let room_id = normalize_room_id(room_id)?;
        let author = normalize_non_empty("author", &request.author)?;
        let body = normalize_non_empty("body", &request.body)?;
        let write_lock = self.room_write_lock(&room_id);
        let _write_guard = write_lock.lock().await;
        let key = RoomShardKey::new(room_id.clone());
        let _ = self.require_room_route(&key, &room_id)?;

        let tables = self.tables.clone();
        let clock = self.clock.clone();
        let room_id_for_tx = room_id.clone();
        let author_for_tx = author.clone();
        let body_for_tx = body.clone();
        let key_for_tx = key.clone();
        let (message, _sequence) = RecordTransaction::run_conflict_retry_loop(
            &self.db,
            RecordTransaction::replay_safe(move |tx: &mut RecordTransaction| {
                let tables = tables.clone();
                let clock = clock.clone();
                let room_id = room_id_for_tx.clone();
                let author = author_for_tx.clone();
                let body = body_for_tx.clone();
                let key = key_for_tx.clone();
                let future: RecordTransactionFuture<'_, _, _> = Box::pin(async move {
                    let mut room = tx.read(&tables.room_state, &key).await?.ok_or_else(|| {
                        ChatRoomsAppError::NotFound(format!("room '{room_id}' does not exist"))
                    })?;
                    let mut log = tx.read(&tables.room_messages, &key).await?.ok_or_else(|| {
                        ChatRoomsAppError::InvalidConfig(format!(
                            "room '{room_id}' is missing its message log row"
                        ))
                    })?;

                    let posted_at_ms = clock.now().get();
                    let next_ordinal = room.message_count.saturating_add(1);
                    let message = ChatMessageRecord {
                        room_id: room_id.clone(),
                        message_id: format!("{room_id}:{next_ordinal:08}"),
                        author,
                        body,
                        posted_at_ms,
                    };
                    room.message_count = next_ordinal;
                    room.last_message_at_ms = Some(posted_at_ms);
                    log.messages.push(message.clone());

                    tx.write(&tables.room_state, &key, &room)?;
                    tx.write(&tables.room_messages, &key, &log)?;
                    Ok(message)
                });
                future
            }),
        )
        .await
        .map_err(map_record_transaction_error)?;
        self.publish_backlog().await?;
        Ok(message)
    }

    pub async fn read_recent_messages(
        &self,
        room_id: &str,
        limit: Option<usize>,
    ) -> Result<RoomMessagesResponse, ChatRoomsAppError> {
        let room_id = normalize_room_id(room_id)?;
        let limit = normalize_recent_limit(limit)?;
        let key = RoomShardKey::new(room_id.clone());
        let _ = self.require_room_route(&key, &room_id)?;

        let mut tx = RecordTransaction::begin(&self.db).await;
        let room = tx
            .read(&self.tables.room_state, &key)
            .await?
            .ok_or_else(|| {
                ChatRoomsAppError::NotFound(format!("room '{room_id}' does not exist"))
            })?;
        let log = tx
            .read(&self.tables.room_messages, &key)
            .await?
            .ok_or_else(|| {
                ChatRoomsAppError::InvalidConfig(format!(
                    "room '{room_id}' is missing its message log row"
                ))
            })?;

        let messages = log
            .messages
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        Ok(RoomMessagesResponse { room, messages })
    }

    pub async fn list_rooms(&self) -> Result<Vec<RoomStateRecord>, ChatRoomsAppError> {
        let mut rooms =
            collect_values(self.tables.room_state.scan_all(Default::default()).await?).await;
        rooms.sort_by(|left, right| left.room_id.cmp(&right.room_id));
        Ok(rooms)
    }

    pub fn target_sharding_for_room(
        &self,
        room_id: &str,
        target_physical_shard: u32,
    ) -> Result<ShardingConfig, ChatRoomsAppError> {
        let room_id = normalize_room_id(room_id)?;
        let key = RoomShardKey::new(room_id.clone());
        let (_key, route) = self.require_room_route(&key, &room_id)?;
        self.tables
            .room_keyspace
            .sharding_state()
            .map_err(map_keyspace_error)?
            .config
            .move_partition(
                route.virtual_partition,
                PhysicalShardId::new(target_physical_shard),
            )
            .map_err(|error| map_sharding_error(&room_id, error))
    }

    pub async fn reshard_room(
        &self,
        room_id: &str,
        request: ReshardRoomRequest,
    ) -> Result<ReshardRoomResponse, ChatRoomsAppError> {
        let room_id = normalize_room_id(room_id)?;
        let write_lock = self.room_write_lock(&room_id);
        let _write_guard = write_lock.lock().await;
        let key = RoomShardKey::new(room_id.clone());
        let (_key, route) = self.require_room_route(&key, &room_id)?;
        if self.tables.room_state.read(&key).await?.is_none() {
            return Err(ChatRoomsAppError::NotFound(format!(
                "room '{room_id}' does not exist"
            )));
        }

        let target = PhysicalShardId::new(request.target_physical_shard);
        if route.physical_shard == target {
            return Err(ChatRoomsAppError::Usage(format!(
                "room '{room_id}' is already on physical shard {}",
                target
            )));
        }

        if self.profile.uses_shard_local_execution() && target != PhysicalShardId::UNSHARDED {
            self.register_one_shard_execution_domain(target)?;
        }

        let target_sharding = self.target_sharding_for_room(&room_id, target.get())?;
        let resharded = self
            .tables
            .room_keyspace
            .reshard_to(target_sharding)
            .await
            .map_err(map_keyspace_reshard_error)?;
        self.publish_backlog().await?;

        let room_state_view = reshard_view(
            ROOM_STATE_TABLE_NAME,
            resharded
                .iter()
                .find(|state| state.table_name == ROOM_STATE_TABLE_NAME)
                .cloned(),
        );
        let room_messages_view = reshard_view(
            ROOM_MESSAGES_TABLE_NAME,
            resharded
                .iter()
                .find(|state| state.table_name == ROOM_MESSAGES_TABLE_NAME)
                .cloned(),
        );

        Ok(ReshardRoomResponse {
            room_id,
            virtual_partition: route.virtual_partition.get(),
            from_physical_shard: route.physical_shard.get(),
            to_physical_shard: target.get(),
            room_state: room_state_view,
            room_messages: room_messages_view,
        })
    }

    pub async fn inspect_room_shard(
        &self,
        room_id: &str,
    ) -> Result<RoomShardInspection, ChatRoomsAppError> {
        let room_id = normalize_room_id(room_id)?;
        let key = RoomShardKey::new(room_id.clone());
        let room = self.tables.room_state.read(&key).await?.ok_or_else(|| {
            ChatRoomsAppError::NotFound(format!("room '{room_id}' does not exist"))
        })?;
        let (_key, route) = self.require_room_route(&key, &room_id)?;
        let snapshot = self.db.resource_manager_snapshot();
        let preferred = self
            .db
            .shard_execution_placement(route.physical_shard)
            .map_err(ChatRoomsAppError::from)?;
        let active_background_domain = self.active_background_domain(route.physical_shard)?;

        Ok(RoomShardInspection {
            room_id,
            title: room.title,
            message_count: room.message_count,
            virtual_partition: route.virtual_partition.get(),
            physical_shard: route.physical_shard.get(),
            shard_map_revision: route.shard_map_revision.get(),
            preferred_foreground_domain: preferred.foreground.as_string(),
            preferred_background_domain: preferred.background.as_string(),
            preferred_control_plane_domain: preferred.control_plane.as_string(),
            active_background_domain: active_background_domain.as_string(),
            active_background_backlog: backlog_view(
                snapshot
                    .domains
                    .get(&active_background_domain)
                    .map(|domain| domain.backlog)
                    .unwrap_or_default(),
            ),
            room_state_resharding: self
                .tables
                .room_state
                .resharding_state()
                .map(|state| reshard_view(ROOM_STATE_TABLE_NAME, Some(state))),
            room_messages_resharding: self
                .tables
                .room_messages
                .resharding_state()
                .map(|state| reshard_view(ROOM_MESSAGES_TABLE_NAME, Some(state))),
        })
    }

    pub async fn observability_report(
        &self,
    ) -> Result<ChatRoomsObservabilityResponse, ChatRoomsAppError> {
        let rooms = self.list_rooms().await?;
        let snapshot = self.db.resource_manager_snapshot();
        let shard_assignments = self
            .tables
            .room_state
            .sharding_state()
            .map_err(ChatRoomsAppError::from)?
            .shard_assignments()
            .into_iter()
            .map(|assignment| VirtualPartitionAssignment {
                virtual_partition: assignment.virtual_partition.get(),
                physical_shard: assignment.physical_shard.get(),
            })
            .collect::<Vec<_>>();

        let mut rooms_view = Vec::new();
        let mut rooms_by_shard = BTreeMap::<u32, Vec<String>>::new();
        for room in rooms {
            let room_id = room.room_id.clone();
            let inspection = self.inspect_room_shard(&room_id).await?;
            rooms_by_shard
                .entry(inspection.physical_shard)
                .or_default()
                .push(room_id);
            rooms_view.push(inspection);
        }
        rooms_view.sort_by(|left, right| left.room_id.cmp(&right.room_id));

        let database_background_domain = self
            .db
            .execution_lane_binding(ExecutionLane::UserBackground)
            .domain
            .clone();
        let database_background_backlog = backlog_view(
            snapshot
                .domains
                .get(&database_background_domain)
                .map(|domain| domain.backlog)
                .unwrap_or_default(),
        );

        let shard_ids = self
            .tables
            .room_state
            .sharding_state()
            .map_err(ChatRoomsAppError::from)?
            .physical_shards()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut shards = Vec::new();
        for shard in shard_ids {
            let preferred = self
                .db
                .shard_execution_placement(shard)
                .map_err(ChatRoomsAppError::from)?;
            let active_background_domain = self.active_background_domain(shard)?;
            let room_ids = rooms_by_shard
                .get(&shard.get())
                .cloned()
                .unwrap_or_default();
            shards.push(ShardObservabilityView {
                physical_shard: shard.get(),
                preferred_foreground_domain: preferred.foreground.as_string(),
                preferred_background_domain: preferred.background.as_string(),
                preferred_control_plane_domain: preferred.control_plane.as_string(),
                active_background_domain: active_background_domain.as_string(),
                background_domain_registered: snapshot
                    .domains
                    .contains_key(&active_background_domain),
                active_background_backlog: backlog_view(
                    snapshot
                        .domains
                        .get(&active_background_domain)
                        .map(|domain| domain.backlog)
                        .unwrap_or_default(),
                ),
                room_count: room_ids.len(),
                room_ids,
            });
        }
        shards.sort_by_key(|entry| entry.physical_shard);

        Ok(ChatRoomsObservabilityResponse {
            profile: self.profile,
            database_background_domain: database_background_domain.as_string(),
            database_background_backlog,
            shard_assignments,
            rooms: rooms_view,
            shards,
        })
    }

    fn room_write_lock(&self, room_id: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self
            .room_write_locks
            .by_room
            .lock()
            .expect("room write lock registry poisoned");
        locks
            .entry(room_id.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    async fn clear_backlog_publication(&self) {
        self.backlog.clear();
    }

    async fn publish_backlog(&self) -> Result<(), ChatRoomsAppError> {
        let publication = self.backlog.begin_replace();
        let mut domain_backlog =
            BTreeMap::<String, (ExecutionDomainPath, ExecutionDomainBacklogSnapshot)>::new();
        let mut stream = self
            .tables
            .room_messages
            .scan_all(Default::default())
            .await?;

        while let Some((key, record)) = stream.next().await {
            let backlog = backlog_from_log(&record);
            if backlog.is_empty() {
                continue;
            }
            let route = self
                .tables
                .room_keyspace
                .route_key(&key)
                .map_err(map_keyspace_error)?;
            let path = self.active_background_domain(route.physical_shard)?;
            let entry = domain_backlog
                .entry(path.as_string())
                .or_insert_with(|| (path.clone(), ExecutionDomainBacklogSnapshot::default()));
            entry.1.queued_work_items = entry
                .1
                .queued_work_items
                .saturating_add(backlog.queued_work_items);
            entry.1.queued_bytes = entry.1.queued_bytes.saturating_add(backlog.queued_bytes);
        }

        let _ = self.backlog.replace_all_at(
            publication,
            domain_backlog
                .into_iter()
                .map(|(_key, (path, backlog))| (path, backlog)),
        );
        Ok(())
    }

    fn register_shard_execution_domains(&self) -> Result<(), ChatRoomsAppError> {
        self.db
            .register_shard_execution_domains(
                example_physical_shards(),
                &chat_rooms_shard_execution_profile(),
            )
            .map_err(ChatRoomsAppError::from)
    }

    fn register_one_shard_execution_domain(
        &self,
        shard: PhysicalShardId,
    ) -> Result<(), ChatRoomsAppError> {
        self.db
            .register_shard_execution_domains([shard], &chat_rooms_shard_execution_profile())
            .map_err(ChatRoomsAppError::from)
    }

    fn require_room_route(
        &self,
        key: &RoomShardKey,
        room_id: &str,
    ) -> Result<(RoomShardKey, KeyShardRoute), ChatRoomsAppError> {
        let route = self
            .tables
            .room_keyspace
            .route_key(key)
            .map_err(|error| map_room_keyspace_error(room_id, error))?;
        Ok((key.clone(), route))
    }

    fn active_background_domain(
        &self,
        shard: PhysicalShardId,
    ) -> Result<ExecutionDomainPath, ChatRoomsAppError> {
        if self.profile.uses_shard_local_execution() && shard != PhysicalShardId::UNSHARDED {
            return Ok(self
                .db
                .shard_execution_placement(shard)
                .map_err(ChatRoomsAppError::from)?
                .background);
        }
        Ok(self
            .db
            .execution_lane_binding(ExecutionLane::UserBackground)
            .domain
            .clone())
    }
}

pub fn chat_rooms_db_settings(path: &str, prefix: &str) -> DbSettings {
    DbSettings::tiered_storage(TieredStorageConfig {
        ssd: terracedb::SsdConfig {
            path: path.to_string(),
        },
        s3: terracedb::S3Location {
            bucket: "terracedb-examples".to_string(),
            prefix: prefix.to_string(),
        },
        max_local_bytes: 1024 * 1024,
        durability: TieredDurabilityMode::GroupCommit,
        local_retention: terracedb::TieredLocalRetentionMode::Offload,
    })
}

pub async fn ensure_chat_rooms_tables(db: &Db) -> Result<ChatRoomsTables, ChatRoomsAppError> {
    let room_state = RoomStateTable::get_or_create_by_name(
        db,
        sharded_room_table_config(ROOM_STATE_TABLE_NAME),
        RoomShardKeyCodec,
        JsonValueCodec::new(),
    )
    .await?;
    let room_messages = RoomMessagesTable::get_or_create_by_name(
        db,
        sharded_room_table_config(ROOM_MESSAGES_TABLE_NAME),
        RoomShardKeyCodec,
        JsonValueCodec::new(),
    )
    .await?;
    let room_keyspace = RoomKeyspace::new(
        room_state.table().clone(),
        [room_messages.table().clone()],
        RoomShardKeyCodec,
    )
    .map_err(map_keyspace_error)?;
    Ok(ChatRoomsTables {
        room_keyspace,
        room_state,
        room_messages,
    })
}

pub fn sharded_room_table_config(name: &str) -> TableConfig {
    terracedb::TableConfig::row(name)
        .sharding(room_sharding_config())
        .build()
}

pub fn room_sharding_config() -> ShardingConfig {
    ShardingConfig::hash(
        CHAT_ROOMS_VIRTUAL_PARTITION_COUNT,
        ShardHashAlgorithm::Crc32,
        ShardMapRevision::new(INITIAL_SHARD_MAP_REVISION),
        vec![
            PhysicalShardId::new(0),
            PhysicalShardId::new(0),
            PhysicalShardId::new(1),
            PhysicalShardId::new(1),
        ],
    )
    .expect("chat rooms sharding config should be valid")
}

pub fn find_room_id_for_shard(
    config: &ShardingConfig,
    target: PhysicalShardId,
    prefix: &str,
) -> String {
    let codec = RoomShardKeyCodec;
    for index in 0..10_000_u32 {
        let room_id = format!("{prefix}-{index}");
        let key = codec
            .encode_key(&RoomShardKey::new(room_id.clone()))
            .expect("room key should encode");
        let route = config.route_key(&key).expect("room key should route");
        if route.physical_shard == target {
            return room_id;
        }
    }
    panic!("failed to find room for physical shard {target}");
}

pub fn find_room_id_for_partition(
    config: &ShardingConfig,
    target: VirtualPartitionId,
    prefix: &str,
) -> String {
    let codec = RoomShardKeyCodec;
    for index in 0..10_000_u32 {
        let room_id = format!("{prefix}-{index}");
        let key = codec
            .encode_key(&RoomShardKey::new(room_id.clone()))
            .expect("room key should encode");
        let route = config.route_key(&key).expect("room key should route");
        if route.virtual_partition == target {
            return room_id;
        }
    }
    panic!("failed to find room for virtual partition {target}");
}

fn backlog_from_log(log: &RoomMessagesRecord) -> ExecutionDomainBacklogSnapshot {
    let queued_work_items = log
        .messages
        .len()
        .saturating_sub(BACKLOG_FREE_MESSAGES_PER_ROOM)
        .min(u32::MAX as usize) as u32;
    let queued_bytes = log
        .messages
        .iter()
        .skip(BACKLOG_FREE_MESSAGES_PER_ROOM)
        .map(|message| message.body.len() as u64)
        .sum();
    ExecutionDomainBacklogSnapshot {
        queued_work_items,
        queued_bytes,
    }
}

fn reshard_view(
    table_name: &str,
    state: Option<terracedb::TableReshardingState>,
) -> ReshardTableStateView {
    let Some(state) = state else {
        return ReshardTableStateView {
            table_name: table_name.to_string(),
            source_revision: 0,
            target_revision: 0,
            phase: "none".to_string(),
            paused_partitions: Vec::new(),
            published_revision: None,
            failure: None,
        };
    };
    ReshardTableStateView {
        table_name: state.table_name,
        source_revision: state.source_revision.get(),
        target_revision: state.target_revision.get(),
        phase: state.phase.as_str().to_string(),
        paused_partitions: state
            .paused_partitions
            .into_iter()
            .map(VirtualPartitionId::get)
            .collect(),
        published_revision: state.published_revision.map(ShardMapRevision::get),
        failure: state.failure,
    }
}

fn chat_rooms_shard_execution_profile() -> ShardExecutionDomainProfile {
    ShardExecutionDomainProfile::new(
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(2),
                weight: None,
            },
            ..ExecutionDomainBudget::default()
        },
        ExecutionDomainBudget {
            background: DomainBackgroundBudget {
                task_slots: Some(2),
                max_in_flight_bytes: Some(64 * 1024),
            },
            ..ExecutionDomainBudget::default()
        },
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(1),
                weight: None,
            },
            background: DomainBackgroundBudget {
                task_slots: Some(1),
                max_in_flight_bytes: Some(16 * 1024),
            },
            ..ExecutionDomainBudget::default()
        },
    )
    .with_metadata("terracedb.example", "chat-rooms-api")
}

fn map_record_transaction_error(
    error: RecordTransactionRunError<ChatRoomsAppError>,
) -> ChatRoomsAppError {
    match error {
        RecordTransactionRunError::Callback(error) => error,
        RecordTransactionRunError::Commit(error) => error.into(),
    }
}

fn map_keyspace_error(error: RecordKeyspaceError) -> ChatRoomsAppError {
    match error {
        RecordKeyspaceError::Storage(error) => error.into(),
        RecordKeyspaceError::Codec(error) => error.into(),
        RecordKeyspaceError::IncompatibleMember { .. } => {
            ChatRoomsAppError::InvalidConfig(error.to_string())
        }
        RecordKeyspaceError::MisalignedRouting { .. } => {
            ChatRoomsAppError::Conflict(error.to_string())
        }
    }
}

fn map_room_keyspace_error(room_id: &str, error: RecordKeyspaceError) -> ChatRoomsAppError {
    match error {
        RecordKeyspaceError::MisalignedRouting { .. } => ChatRoomsAppError::Conflict(format!(
            "room '{room_id}' is temporarily unavailable while shard maps converge"
        )),
        other => map_keyspace_error(other),
    }
}

fn map_keyspace_reshard_error(error: RecordKeyspaceReshardError) -> ChatRoomsAppError {
    match error {
        RecordKeyspaceReshardError::Keyspace(error) => map_keyspace_error(error),
        RecordKeyspaceReshardError::Publish(error) => map_publish_shard_error(error),
        RecordKeyspaceReshardError::Plan(error) => error.into(),
        RecordKeyspaceReshardError::PartialFailure { .. } => {
            ChatRoomsAppError::Conflict(error.to_string())
        }
    }
}

fn map_publish_shard_error(error: PublishShardMapError) -> ChatRoomsAppError {
    match error {
        PublishShardMapError::Storage(error) => error.into(),
        other => ChatRoomsAppError::Usage(other.to_string()),
    }
}

fn map_sharding_error(_room_id: &str, error: ShardingError) -> ChatRoomsAppError {
    match error {
        ShardingError::PartitionAlreadyAssigned { physical_shard, .. } => ChatRoomsAppError::Usage(
            format!("room is already on physical shard {physical_shard}"),
        ),
        other => ChatRoomsAppError::InvalidConfig(other.to_string()),
    }
}

fn normalize_room_id(value: &str) -> Result<String, ChatRoomsAppError> {
    let trimmed = normalize_non_empty("room_id", value)?;
    let normalized = trimmed.to_ascii_lowercase();
    if normalized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
    {
        return Ok(normalized);
    }
    Err(ChatRoomsAppError::Usage(
        "room_id may only contain ASCII letters, digits, '-' and '_'".to_string(),
    ))
}

fn normalize_non_empty(field: &str, value: &str) -> Result<String, ChatRoomsAppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ChatRoomsAppError::Usage(format!("{field} cannot be empty")));
    }
    Ok(trimmed.to_string())
}

fn normalize_recent_limit(limit: Option<usize>) -> Result<usize, ChatRoomsAppError> {
    let limit = limit.unwrap_or(DEFAULT_RECENT_MESSAGE_LIMIT);
    if limit == 0 || limit > MAX_RECENT_MESSAGE_LIMIT {
        return Err(ChatRoomsAppError::Usage(format!(
            "limit must be between 1 and {MAX_RECENT_MESSAGE_LIMIT}"
        )));
    }
    Ok(limit)
}

fn backlog_view(snapshot: ExecutionDomainBacklogSnapshot) -> RoomBacklogView {
    RoomBacklogView {
        queued_work_items: u64::from(snapshot.queued_work_items),
        queued_bytes: snapshot.queued_bytes,
    }
}

fn example_physical_shards() -> [PhysicalShardId; 3] {
    [
        PhysicalShardId::new(1),
        PhysicalShardId::new(2),
        PhysicalShardId::new(3),
    ]
}

async fn collect_values<K, V>(mut stream: RecordStream<K, V>) -> Vec<V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    let mut values = Vec::new();
    while let Some((_key, value)) = stream.next().await {
        values.push(value);
    }
    values
}

async fn create_room(
    State(state): State<ChatRoomsAppState>,
    Json(request): Json<CreateRoomRequest>,
) -> Result<(StatusCode, Json<RoomStateRecord>), ChatRoomsApiError> {
    let room = state
        .create_room(request)
        .await
        .map_err(ChatRoomsApiError::from)?;
    Ok((StatusCode::CREATED, Json(room)))
}

async fn post_message(
    State(state): State<ChatRoomsAppState>,
    Path(room_id): Path<String>,
    Json(request): Json<PostMessageRequest>,
) -> Result<(StatusCode, Json<ChatMessageRecord>), ChatRoomsApiError> {
    let message = state
        .post_message(&room_id, request)
        .await
        .map_err(ChatRoomsApiError::from)?;
    Ok((StatusCode::CREATED, Json(message)))
}

async fn get_recent_messages(
    State(state): State<ChatRoomsAppState>,
    Path(room_id): Path<String>,
    Query(query): Query<RecentMessagesQuery>,
) -> Result<Json<RoomMessagesResponse>, ChatRoomsApiError> {
    state
        .read_recent_messages(&room_id, query.limit)
        .await
        .map(Json)
        .map_err(ChatRoomsApiError::from)
}

async fn get_room_shard(
    State(state): State<ChatRoomsAppState>,
    Path(room_id): Path<String>,
) -> Result<Json<RoomShardInspection>, ChatRoomsApiError> {
    state
        .inspect_room_shard(&room_id)
        .await
        .map(Json)
        .map_err(ChatRoomsApiError::from)
}

async fn reshard_room(
    State(state): State<ChatRoomsAppState>,
    Path(room_id): Path<String>,
    Json(request): Json<ReshardRoomRequest>,
) -> Result<Json<ReshardRoomResponse>, ChatRoomsApiError> {
    state
        .reshard_room(&room_id, request)
        .await
        .map(Json)
        .map_err(ChatRoomsApiError::from)
}

async fn get_observability(
    State(state): State<ChatRoomsAppState>,
) -> Result<Json<ChatRoomsObservabilityResponse>, ChatRoomsApiError> {
    state
        .observability_report()
        .await
        .map(Json)
        .map_err(ChatRoomsApiError::from)
}
