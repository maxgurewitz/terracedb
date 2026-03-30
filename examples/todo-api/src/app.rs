use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::StreamExt;
use serde::Serialize;
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};

use terracedb::{
    Clock, CommitError, CompactionStrategy, CreateTableError, Db, DbConfig, OpenError, S3Location,
    ScanOptions, StorageConfig, StorageError, Table, TableConfig, TableFormat,
    TieredDurabilityMode, TieredStorageConfig, Timestamp, Transaction, TransactionCommitError,
    Value,
};
use terracedb_projections::{
    ProjectionContext, ProjectionError, ProjectionHandle, ProjectionHandler,
    ProjectionHandlerError, ProjectionRuntime, ProjectionTransaction, RecomputeStrategy,
    SingleSourceProjection,
};
use terracedb_records::{
    BigEndianU64Codec, JsonValueCodec, RecordCodecError, RecordReadError, RecordStream,
    RecordTable, RecordTransaction, RecordWriteError, Utf8StringCodec,
};
use terracedb_relays::{OutboxRelay, OutboxRelayError, OutboxRelayHandler, RelayEntry};
use terracedb_workflows::{
    DEFAULT_TIMER_POLL_INTERVAL, RecurringSchedule, RecurringTickOutput,
    RecurringWorkflowDefinition, RecurringWorkflowHandle, RecurringWorkflowHandler,
    RecurringWorkflowRuntime, WorkflowContext, WorkflowError, WorkflowHandlerError, WorkflowTables,
    WorkflowTelemetrySnapshot,
};

use crate::model::{
    CreateTodoRequest, PlannerCommand, PlannerSchedule, PlannerState, TodoRecord, TodoStatus,
    UpdateTodoRequest,
};

pub const TODOS_TABLE_NAME: &str = "todos";
pub const RECENT_TODOS_TABLE_NAME: &str = "recent_todos";
pub const PLANNER_WORKFLOW_NAME: &str = "weekly-planner";
pub const PLANNER_INSTANCE_ID: &str = "weekly-planner";
pub const TODO_SERVER_PORT: u16 = 9601;

const FULL_SCAN_START: &[u8] = b"";
const FULL_SCAN_END: &[u8] = &[0xff];
const RECENT_SLOT_COUNT: usize = 10;
const RECENT_PROJECTION_NAME: &str = "recent-todos";
const WEEKLY_PLANNER_TIMER_POLL_INTERVAL: Duration = Duration::from_millis(5);
const PLANNER_DISPATCH_BATCH_LIMIT: usize = 16;
const PLANNER_DISPATCH_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(1);

type TodoRecordTable = RecordTable<String, TodoRecord, Utf8StringCodec, JsonValueCodec<TodoRecord>>;
type RecentTodosTable = RecordTable<u64, TodoRecord, BigEndianU64Codec, JsonValueCodec<TodoRecord>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct TodoAppOptions {
    pub planner_schedule: PlannerSchedule,
}

#[derive(Debug, Error)]
pub enum TodoAppError {
    #[error(transparent)]
    Open(#[from] OpenError),
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
    Projection(#[from] ProjectionError),
    #[error(transparent)]
    Workflow(#[from] WorkflowError),
    #[error(transparent)]
    Relay(#[from] OutboxRelayError<PlannerRelayHandlerError>),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("dispatcher task failed: {0}")]
    DispatcherTask(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum TodoApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Internal(String),
}

impl From<TodoAppError> for TodoApiError {
    fn from(error: TodoAppError) -> Self {
        Self::Internal(error.to_string())
    }
}

#[derive(Debug, Error)]
pub enum PlannerRelayHandlerError {
    #[error(transparent)]
    RecordRead(#[from] RecordReadError),
    #[error(transparent)]
    RecordWrite(#[from] RecordWriteError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for TodoApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(ErrorBody {
            error: self.to_string(),
        });
        (status, body).into_response()
    }
}

#[derive(Clone, Debug)]
pub struct TodoTables {
    todos: TodoRecordTable,
    recent_todos: RecentTodosTable,
}

impl TodoTables {
    pub fn todos(&self) -> &TodoRecordTable {
        &self.todos
    }

    pub fn recent_todos(&self) -> &RecentTodosTable {
        &self.recent_todos
    }

    pub fn todos_raw(&self) -> &Table {
        self.todos.table()
    }

    pub fn recent_todos_raw(&self) -> &Table {
        self.recent_todos.table()
    }
}

#[derive(Clone)]
pub struct TodoAppState {
    db: Db,
    clock: Arc<dyn Clock>,
    tables: TodoTables,
}

impl std::fmt::Debug for TodoAppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TodoAppState")
            .field("tables", &self.tables)
            .finish()
    }
}

pub struct TodoApp {
    state: TodoAppState,
    _projection_runtime: ProjectionRuntime,
    projection_handle: ProjectionHandle,
    _workflow_runtime: RecurringWorkflowRuntime<WeeklyPlannerWorkflow>,
    workflow_handle: RecurringWorkflowHandle,
    dispatcher_shutdown: watch::Sender<bool>,
    dispatcher_task: JoinHandle<Result<(), TodoAppError>>,
}

impl TodoApp {
    pub async fn open(db: Db, clock: Arc<dyn Clock>) -> Result<Self, TodoAppError> {
        Self::open_with_options(db, clock, TodoAppOptions::default()).await
    }

    pub async fn open_with_options(
        db: Db,
        clock: Arc<dyn Clock>,
        options: TodoAppOptions,
    ) -> Result<Self, TodoAppError> {
        let tables = ensure_todo_tables(&db).await?;
        let state = TodoAppState {
            db: db.clone(),
            clock: clock.clone(),
            tables: tables.clone(),
        };

        let projection_runtime = ProjectionRuntime::open(db.clone()).await?;
        let projection_handle = projection_runtime
            .start_single_source(
                SingleSourceProjection::new(
                    RECENT_PROJECTION_NAME,
                    tables.todos_raw().clone(),
                    RecentTodosProjection {
                        todos: tables.todos.clone(),
                        recent_todos: tables.recent_todos.clone(),
                    },
                )
                .with_outputs([tables.recent_todos_raw().clone()])
                .with_recompute_strategy(RecomputeStrategy::RebuildFromCurrentState),
            )
            .await?;

        let planner_schedule = options.planner_schedule;
        let workflow_runtime = RecurringWorkflowRuntime::open(
            db.clone(),
            clock.clone(),
            RecurringWorkflowDefinition::new(
                PLANNER_WORKFLOW_NAME,
                PLANNER_INSTANCE_ID,
                RecurringSchedule::custom(
                    move |now| {
                        Some(Timestamp::new(
                            planner_schedule.next_week_start_ms(now.get()),
                        ))
                    },
                    move |_state, fire_at| {
                        Some(Timestamp::new(
                            fire_at.get().saturating_add(planner_schedule.week_millis()),
                        ))
                    },
                ),
                WeeklyPlannerWorkflow {
                    schedule: planner_schedule,
                },
            )
            .with_timer_poll_interval(
                WEEKLY_PLANNER_TIMER_POLL_INTERVAL.min(DEFAULT_TIMER_POLL_INTERVAL),
            ),
        )
        .await?;
        let workflow_handle = workflow_runtime.start().await?;

        let (dispatcher_shutdown, dispatcher_rx) = watch::channel(false);
        let planner_outbox_table = workflow_runtime.tables().outbox_table().clone();
        let relay = OutboxRelay::new(
            db.clone(),
            clock.clone(),
            planner_outbox_table,
            PlannerRelayHandler {
                state: state.clone(),
            },
        )
        .with_batch_limit(PLANNER_DISPATCH_BATCH_LIMIT)
        .with_idle_poll_interval(PLANNER_DISPATCH_IDLE_POLL_INTERVAL);
        let dispatcher_task =
            tokio::spawn(async move { relay.run(dispatcher_rx).await.map_err(TodoAppError::from) });

        Ok(Self {
            state,
            _projection_runtime: projection_runtime,
            projection_handle,
            _workflow_runtime: workflow_runtime,
            workflow_handle,
            dispatcher_shutdown,
            dispatcher_task,
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/todos", post(create_todo).get(list_todos))
            .route("/todos/recent", get(list_recent_todos))
            .route("/todos/{todo_id}", get(get_todo).patch(update_todo))
            .route("/todos/{todo_id}/complete", post(complete_todo))
            .with_state(self.state.clone())
    }

    pub fn state(&self) -> &TodoAppState {
        &self.state
    }

    pub fn tables(&self) -> &TodoTables {
        &self.state.tables
    }

    pub fn projection_handle_mut(&mut self) -> &mut ProjectionHandle {
        &mut self.projection_handle
    }

    pub fn workflow_tables(&self) -> &WorkflowTables {
        self._workflow_runtime.tables()
    }

    pub async fn workflow_telemetry_snapshot(
        &self,
    ) -> Result<WorkflowTelemetrySnapshot, TodoAppError> {
        Ok(self._workflow_runtime.telemetry_snapshot().await?)
    }

    pub async fn planner_state(&self) -> Result<Option<PlannerState>, TodoAppError> {
        Ok(self
            ._workflow_runtime
            .next_fire_at()
            .await?
            .map(|next_fire_at| PlannerState {
                next_fire_at_ms: next_fire_at.get(),
            }))
    }

    pub async fn shutdown(self) -> Result<(), TodoAppError> {
        self.dispatcher_shutdown.send_replace(true);
        match self.dispatcher_task.await {
            Ok(result) => result?,
            Err(error) => return Err(TodoAppError::DispatcherTask(error.to_string())),
        }
        self.workflow_handle.shutdown().await?;
        self.projection_handle.shutdown().await?;
        Ok(())
    }
}

impl TodoAppState {
    pub fn tables(&self) -> &TodoTables {
        &self.tables
    }

    pub async fn create_todo(
        &self,
        request: CreateTodoRequest,
    ) -> Result<TodoRecord, TodoApiError> {
        let todo_id = normalize_required(&request.todo_id, "todo_id")?;
        let title = normalize_required(&request.title, "title")?;
        let now_ms = self.clock.now().get();
        let todo = TodoRecord {
            todo_id: todo_id.clone(),
            title,
            notes: request.notes,
            status: TodoStatus::Open,
            scheduled_for_day: request.scheduled_for_day,
            placeholder: false,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };

        let mut tx = RecordTransaction::begin(&self.db).await;
        if tx
            .read_str(&self.tables.todos, &todo_id)
            .await
            .map_err(TodoAppError::from)?
            .is_some()
        {
            return Err(TodoApiError::Conflict(format!(
                "TODO {} already exists",
                todo_id
            )));
        }

        tx.write_str(&self.tables.todos, &todo_id, &todo)
            .map_err(TodoAppError::from)?;
        tx.commit().await.map_err(TodoAppError::from)?;
        Ok(todo)
    }

    pub async fn get_todo(&self, todo_id: &str) -> Result<Option<TodoRecord>, TodoAppError> {
        self.tables
            .todos
            .read_str(todo_id)
            .await
            .map_err(Into::into)
    }

    pub async fn update_todo(
        &self,
        todo_id: &str,
        request: UpdateTodoRequest,
    ) -> Result<Option<TodoRecord>, TodoApiError> {
        let mut tx = RecordTransaction::begin(&self.db).await;
        let Some(existing) = tx
            .read_str(&self.tables.todos, todo_id)
            .await
            .map_err(TodoAppError::from)?
        else {
            return Ok(None);
        };

        let mut todo = existing;
        if let Some(title) = request.title {
            todo.title = normalize_required(&title, "title")?;
        }
        if let Some(notes) = request.notes {
            todo.notes = notes;
        }
        if let Some(scheduled_for_day) = request.scheduled_for_day {
            todo.scheduled_for_day = Some(scheduled_for_day);
        }
        todo.updated_at_ms = self.clock.now().get();

        tx.write_str(&self.tables.todos, todo_id, &todo)
            .map_err(TodoAppError::from)?;
        tx.commit().await.map_err(TodoAppError::from)?;
        Ok(Some(todo))
    }

    pub async fn complete_todo(&self, todo_id: &str) -> Result<Option<TodoRecord>, TodoApiError> {
        let mut tx = RecordTransaction::begin(&self.db).await;
        let Some(existing) = tx
            .read_str(&self.tables.todos, todo_id)
            .await
            .map_err(TodoAppError::from)?
        else {
            return Ok(None);
        };

        let mut todo = existing;
        todo.status = TodoStatus::Completed;
        todo.updated_at_ms = self.clock.now().get();
        tx.write_str(&self.tables.todos, todo_id, &todo)
            .map_err(TodoAppError::from)?;
        tx.commit().await.map_err(TodoAppError::from)?;
        Ok(Some(todo))
    }

    pub async fn list_todos(&self) -> Result<Vec<TodoRecord>, TodoAppError> {
        let mut todos =
            collect_todos(self.tables.todos.scan_all(ScanOptions::default()).await?).await;
        sort_list_todos(&mut todos);
        Ok(todos)
    }

    pub async fn list_recent_todos(&self) -> Result<Vec<TodoRecord>, TodoAppError> {
        Ok(collect_todos(
            self.tables
                .recent_todos
                .scan_all(ScanOptions::default())
                .await?,
        )
        .await)
    }

    async fn stage_placeholder_commands(
        &self,
        tx: &mut Transaction,
        commands: &[RelayEntry<PlannerCommand>],
    ) -> Result<(), PlannerRelayHandlerError> {
        for entry in commands {
            let command = &entry.message;
            let exists = self
                .tables
                .todos
                .read_in_str(tx, &command.todo_id)
                .await?
                .is_some();
            if !exists {
                let todo = TodoRecord {
                    todo_id: command.todo_id.clone(),
                    title: command.title.clone(),
                    notes: String::new(),
                    status: TodoStatus::Open,
                    scheduled_for_day: Some(command.scheduled_for_day),
                    placeholder: true,
                    created_at_ms: command.created_at_ms,
                    updated_at_ms: command.created_at_ms,
                };
                self.tables.todos.write_in_str(tx, &todo.todo_id, &todo)?;
            }
        }
        Ok(())
    }
}

pub fn todo_db_config(path: &str, prefix: &str) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: terracedb::SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-example-todo-api".to_string(),
                prefix: prefix.to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability: TieredDurabilityMode::GroupCommit,
        }),
        scheduler: None,
    }
}

pub async fn ensure_todo_tables(db: &Db) -> Result<TodoTables, TodoAppError> {
    Ok(TodoTables {
        todos: todo_table(ensure_table(db, row_table_config(TODOS_TABLE_NAME)).await?),
        recent_todos: recent_todos_table(
            ensure_table(db, row_table_config(RECENT_TODOS_TABLE_NAME)).await?,
        ),
    })
}

async fn ensure_table(db: &Db, config: TableConfig) -> Result<Table, CreateTableError> {
    match db.create_table(config.clone()).await {
        Ok(table) => Ok(table),
        Err(CreateTableError::AlreadyExists(_)) => Ok(db.table(config.name)),
        Err(error) => Err(error),
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
        metadata: Default::default(),
    }
}

fn todo_table(table: Table) -> TodoRecordTable {
    RecordTable::with_codecs(table, Utf8StringCodec, JsonValueCodec::new())
}

fn recent_todos_table(table: Table) -> RecentTodosTable {
    RecordTable::with_codecs(table, BigEndianU64Codec, JsonValueCodec::new())
}

fn recent_slot_key(slot: usize) -> u64 {
    slot as u64
}

fn normalize_required(value: &str, field: &str) -> Result<String, TodoApiError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(TodoApiError::BadRequest(format!("{field} cannot be empty")));
    }
    Ok(trimmed.to_string())
}

async fn collect_todos(
    mut stream: RecordStream<impl Send + 'static, TodoRecord>,
) -> Vec<TodoRecord> {
    let mut todos = Vec::new();
    while let Some((_key, todo)) = stream.next().await {
        todos.push(todo);
    }
    todos
}

fn decode_json_value<T>(value: &Value, context: &str) -> Result<T, TodoAppError>
where
    T: serde::de::DeserializeOwned,
{
    match value {
        Value::Bytes(bytes) => Ok(serde_json::from_slice(bytes)?),
        Value::Record(_) => Err(TodoAppError::Io(std::io::Error::other(format!(
            "{context} must use byte encoding"
        )))),
    }
}

fn sort_recent_todos(todos: &mut [TodoRecord]) {
    todos.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| right.created_at_ms.cmp(&left.created_at_ms))
            .then_with(|| left.todo_id.cmp(&right.todo_id))
    });
}

fn sort_list_todos(todos: &mut [TodoRecord]) {
    todos.sort_by(|left, right| {
        left.scheduled_for_day
            .unwrap_or(u64::MAX)
            .cmp(&right.scheduled_for_day.unwrap_or(u64::MAX))
            .then_with(|| left.created_at_ms.cmp(&right.created_at_ms))
            .then_with(|| left.todo_id.cmp(&right.todo_id))
    });
}

struct RecentTodosProjection {
    todos: TodoRecordTable,
    recent_todos: RecentTodosTable,
}

#[async_trait]
impl ProjectionHandler for RecentTodosProjection {
    async fn apply_with_context(
        &self,
        _run: &terracedb_projections::ProjectionSequenceRun,
        ctx: &ProjectionContext,
        tx: &mut ProjectionTransaction,
    ) -> Result<(), ProjectionHandlerError> {
        let mut todos = collect_todos(
            self.todos
                .decode_stream(
                    ctx.scan(
                        self.todos.table(),
                        FULL_SCAN_START.to_vec(),
                        FULL_SCAN_END.to_vec(),
                        ScanOptions::default(),
                    )
                    .await?,
                )
                .await
                .map_err(ProjectionHandlerError::new)?,
        )
        .await;
        sort_recent_todos(&mut todos);

        for slot in 0..RECENT_SLOT_COUNT {
            let key = recent_slot_key(slot);
            if let Some(todo) = todos.get(slot) {
                tx.put(
                    self.recent_todos.table(),
                    self.recent_todos
                        .encode_key(&key)
                        .map_err(ProjectionHandlerError::new)?,
                    self.recent_todos
                        .encode_value(todo)
                        .map_err(ProjectionHandlerError::new)?,
                );
            } else {
                tx.delete(
                    self.recent_todos.table(),
                    self.recent_todos
                        .encode_key(&key)
                        .map_err(ProjectionHandlerError::new)?,
                );
            }
        }

        Ok(())
    }
}
struct WeeklyPlannerWorkflow {
    schedule: PlannerSchedule,
}

struct PlannerRelayHandler {
    state: TodoAppState,
}

#[async_trait]
impl OutboxRelayHandler for PlannerRelayHandler {
    type Message = PlannerCommand;
    type Error = PlannerRelayHandlerError;

    fn decode(
        &self,
        entry: terracedb::OutboxEntry,
    ) -> Result<RelayEntry<Self::Message>, Self::Error> {
        Ok(RelayEntry {
            outbox_id: entry.outbox_id,
            idempotency_key: entry.idempotency_key,
            message: serde_json::from_slice(&entry.payload)?,
        })
    }

    async fn apply_batch(
        &self,
        tx: &mut Transaction,
        entries: &[RelayEntry<Self::Message>],
    ) -> Result<(), Self::Error> {
        self.state.stage_placeholder_commands(tx, entries).await
    }
}

#[async_trait]
impl RecurringWorkflowHandler for WeeklyPlannerWorkflow {
    async fn tick(
        &self,
        _instance_id: &str,
        _state: &terracedb_workflows::RecurringWorkflowState,
        fire_at: Timestamp,
        _ctx: &WorkflowContext,
    ) -> Result<RecurringTickOutput, WorkflowHandlerError> {
        let week_start_ms = fire_at.get();
        let mut outbox_entries = Vec::with_capacity(self.schedule.days_per_week as usize);
        for day_offset in 0..self.schedule.days_per_week {
            let scheduled_for_day =
                week_start_ms + day_offset.saturating_mul(self.schedule.day_millis);
            let command = PlannerCommand {
                todo_id: self.schedule.placeholder_todo_id(scheduled_for_day),
                title: format!("Plan day {}", self.schedule.day_index(scheduled_for_day)),
                scheduled_for_day,
                created_at_ms: week_start_ms,
            };
            outbox_entries.push(terracedb::OutboxEntry {
                outbox_id: format!("planner:{scheduled_for_day}").into_bytes(),
                idempotency_key: format!("planner:{scheduled_for_day}"),
                payload: serde_json::to_vec(&command).map_err(WorkflowHandlerError::new)?,
            });
        }

        Ok(RecurringTickOutput {
            outbox_entries,
            timers: Vec::new(),
        })
    }
}

async fn create_todo(
    State(state): State<TodoAppState>,
    Json(request): Json<CreateTodoRequest>,
) -> Result<(StatusCode, Json<TodoRecord>), TodoApiError> {
    let todo = state.create_todo(request).await?;
    Ok((StatusCode::CREATED, Json(todo)))
}

async fn list_todos(
    State(state): State<TodoAppState>,
) -> Result<Json<Vec<TodoRecord>>, TodoApiError> {
    Ok(Json(state.list_todos().await.map_err(TodoApiError::from)?))
}

async fn get_todo(
    State(state): State<TodoAppState>,
    Path(todo_id): Path<String>,
) -> Result<Json<TodoRecord>, TodoApiError> {
    let todo = state
        .get_todo(&todo_id)
        .await
        .map_err(TodoApiError::from)?
        .ok_or_else(|| TodoApiError::NotFound(format!("TODO {todo_id} was not found")))?;
    Ok(Json(todo))
}

async fn update_todo(
    State(state): State<TodoAppState>,
    Path(todo_id): Path<String>,
    Json(request): Json<UpdateTodoRequest>,
) -> Result<Json<TodoRecord>, TodoApiError> {
    let todo = state
        .update_todo(&todo_id, request)
        .await?
        .ok_or_else(|| TodoApiError::NotFound(format!("TODO {todo_id} was not found")))?;
    Ok(Json(todo))
}

async fn complete_todo(
    State(state): State<TodoAppState>,
    Path(todo_id): Path<String>,
) -> Result<Json<TodoRecord>, TodoApiError> {
    let todo = state
        .complete_todo(&todo_id)
        .await?
        .ok_or_else(|| TodoApiError::NotFound(format!("TODO {todo_id} was not found")))?;
    Ok(Json(todo))
}

async fn list_recent_todos(
    State(state): State<TodoAppState>,
) -> Result<Json<Vec<TodoRecord>>, TodoApiError> {
    Ok(Json(
        state
            .list_recent_todos()
            .await
            .map_err(TodoApiError::from)?,
    ))
}
