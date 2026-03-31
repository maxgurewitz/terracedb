use std::sync::{Arc, Mutex, MutexGuard};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::StreamExt;
use terracedb::{
    CreateTableError, Db, DbBuilder, FlushError, ReadError, ResourceAdmissionDecision,
    ResourceManager, ScanOptions, StorageError, Table,
};
use terracedb_records::{
    JsonValueCodec, RecordReadError, RecordStream, RecordTable, RecordWriteError, Utf8StringCodec,
};
use thiserror::Error;

use crate::model::{
    ANALYTICS_DATABASE_NAME, ActivePressureView, AdmissionProbeRequest, AdmissionProbeResponse,
    BackgroundMaintenanceRequest, BackgroundMaintenanceResponse, BackgroundPressureView,
    ControlPlaneTableRequest, ControlPlaneTableResponse, CreatePrimaryItemRequest,
    DOMAINS_SERVER_PORT, DomainsExampleProfile, DomainsObservabilityResponse, ExampleDatabase,
    ExampleLane, HELPER_REPORTS_TABLE_NAME, HelperLoadRequest, HelperLoadResponse,
    HelperPressureView, HelperReportRecord, PRIMARY_DATABASE_NAME, PRIMARY_ITEMS_TABLE_NAME,
    PrimaryItemRecord, domains_db_settings, row_table_config,
};

type PrimaryItemsTable =
    RecordTable<String, PrimaryItemRecord, Utf8StringCodec, JsonValueCodec<PrimaryItemRecord>>;
type HelperReportsTable =
    RecordTable<String, HelperReportRecord, Utf8StringCodec, JsonValueCodec<HelperReportRecord>>;

#[derive(Debug, Error)]
pub enum DomainsAppError {
    #[error(transparent)]
    CreateTable(#[from] CreateTableError),
    #[error(transparent)]
    Flush(#[from] FlushError),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    RecordRead(#[from] RecordReadError),
    #[error(transparent)]
    RecordWrite(#[from] RecordWriteError),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    InvalidConfig(String),
    #[error("application state lock poisoned")]
    StatePoisoned,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum DomainsApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Internal(String),
}

impl From<DomainsAppError> for DomainsApiError {
    fn from(error: DomainsAppError) -> Self {
        match error {
            DomainsAppError::Conflict(message) => Self::Conflict(message),
            DomainsAppError::Usage(message) | DomainsAppError::InvalidConfig(message) => {
                Self::BadRequest(message)
            }
            other => Self::Internal(other.to_string()),
        }
    }
}

#[derive(serde::Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for DomainsApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
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

#[derive(Clone, Debug, Default)]
struct ActivePressureState {
    primary_background: Option<BackgroundPressureView>,
    helper: Option<HelperPressureView>,
}

impl ActivePressureState {
    fn view(&self) -> ActivePressureView {
        ActivePressureView {
            primary_background: self.primary_background.clone(),
            helper: self.helper.clone(),
        }
    }
}

#[derive(Clone)]
pub struct DomainsAppState {
    profile: DomainsExampleProfile,
    primary_db: Db,
    analytics_db: Db,
    primary_items: PrimaryItemsTable,
    helper_reports: HelperReportsTable,
    active_pressure: Arc<Mutex<ActivePressureState>>,
}

impl std::fmt::Debug for DomainsAppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DomainsAppState")
            .field("profile", &self.profile)
            .field("primary_db", &self.primary_db.execution_identity())
            .field("analytics_db", &self.analytics_db.execution_identity())
            .finish()
    }
}

pub struct DomainsApp {
    state: DomainsAppState,
}

impl DomainsApp {
    pub async fn open(
        primary_db: Db,
        analytics_db: Db,
        profile: DomainsExampleProfile,
    ) -> Result<Self, DomainsAppError> {
        if primary_db.execution_identity() != PRIMARY_DATABASE_NAME {
            return Err(DomainsAppError::InvalidConfig(format!(
                "expected primary execution identity '{PRIMARY_DATABASE_NAME}', found '{}'",
                primary_db.execution_identity()
            )));
        }
        if analytics_db.execution_identity() != ANALYTICS_DATABASE_NAME {
            return Err(DomainsAppError::InvalidConfig(format!(
                "expected analytics execution identity '{ANALYTICS_DATABASE_NAME}', found '{}'",
                analytics_db.execution_identity()
            )));
        }
        if !Arc::ptr_eq(
            &primary_db.resource_manager(),
            &analytics_db.resource_manager(),
        ) {
            return Err(DomainsAppError::InvalidConfig(
                "example requires colocated databases to share one resource manager".to_string(),
            ));
        }

        let primary_items = PrimaryItemsTable::with_codecs(
            ensure_table(&primary_db, row_table_config(PRIMARY_ITEMS_TABLE_NAME)).await?,
            Utf8StringCodec,
            JsonValueCodec::new(),
        );
        let helper_reports = HelperReportsTable::with_codecs(
            ensure_table(&analytics_db, row_table_config(HELPER_REPORTS_TABLE_NAME)).await?,
            Utf8StringCodec,
            JsonValueCodec::new(),
        );

        Ok(Self {
            state: DomainsAppState {
                profile,
                primary_db,
                analytics_db,
                primary_items,
                helper_reports,
                active_pressure: Arc::new(Mutex::new(ActivePressureState::default())),
            },
        })
    }

    pub fn state(&self) -> &DomainsAppState {
        &self.state
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(
                "/primary/items",
                post(create_primary_item).get(list_primary_items),
            )
            .route("/primary/maintenance", post(apply_primary_maintenance))
            .route(
                "/primary/maintenance/release",
                post(release_primary_maintenance),
            )
            .route("/helper/load", post(run_helper_load))
            .route("/helper/reports", get(list_helper_reports))
            .route("/helper/release", post(release_helper_load))
            .route("/control/ensure-table", post(ensure_control_plane_table))
            .route("/domains/admission", post(probe_admission))
            .route("/domains/report", get(domains_report))
            .with_state(self.state.clone())
    }

    pub async fn shutdown(self) -> Result<(), DomainsAppError> {
        self.state.release_all_pressure()?;
        Ok(())
    }
}

impl DomainsAppState {
    pub fn profile(&self) -> DomainsExampleProfile {
        self.profile
    }

    pub fn primary_db(&self) -> &Db {
        &self.primary_db
    }

    pub fn analytics_db(&self) -> &Db {
        &self.analytics_db
    }

    pub async fn create_primary_item(
        &self,
        request: CreatePrimaryItemRequest,
    ) -> Result<PrimaryItemRecord, DomainsAppError> {
        let item_id = normalize_non_empty("item_id", &request.item_id)?;
        let title = normalize_non_empty("title", &request.title)?;
        if self.primary_items.read_str(&item_id).await?.is_some() {
            return Err(DomainsAppError::Conflict(format!(
                "primary item '{item_id}' already exists"
            )));
        }
        let record = PrimaryItemRecord { item_id, title };
        self.primary_items
            .write_str(&record.item_id, &record)
            .await
            .map_err(DomainsAppError::from)?;
        Ok(record)
    }

    pub async fn list_primary_items(&self) -> Result<Vec<PrimaryItemRecord>, DomainsAppError> {
        let mut items =
            collect_values(self.primary_items.scan_all(ScanOptions::default()).await?).await;
        items.sort_by(|left, right| left.item_id.cmp(&right.item_id));
        Ok(items)
    }

    pub async fn list_helper_reports(&self) -> Result<Vec<HelperReportRecord>, DomainsAppError> {
        let mut reports =
            collect_values(self.helper_reports.scan_all(ScanOptions::default()).await?).await;
        reports.sort_by(|left, right| left.report_id.cmp(&right.report_id));
        Ok(reports)
    }

    pub async fn run_helper_load(
        &self,
        request: HelperLoadRequest,
    ) -> Result<HelperLoadResponse, DomainsAppError> {
        let batch_id = normalize_non_empty("batch_id", &request.batch_id)?;
        let mut last_sequence = None;
        for ordinal in 0..request.report_count {
            let report = HelperReportRecord {
                report_id: format!("{batch_id}:{ordinal:04}"),
                batch_id: batch_id.clone(),
                ordinal,
            };
            last_sequence = Some(
                self.helper_reports
                    .write_str(&report.report_id, &report)
                    .await?
                    .get(),
            );
        }
        let flushed = if request.flush_after_write {
            self.analytics_db.flush().await?;
            true
        } else {
            last_sequence.is_some()
        };

        let helper_pressure = HelperPressureView {
            hold_foreground_cpu_workers: request.hold_foreground_cpu_workers,
            background: BackgroundPressureView {
                hold_background_tasks: request.hold_background_tasks,
                background_in_flight_bytes: request.background_in_flight_bytes,
                queued_work_items: request.queued_work_items,
                queued_bytes: request.queued_bytes,
            },
        };

        let (foreground_admission, background_admission) =
            self.replace_helper_pressure(helper_pressure)?;
        let helper_report_count = self.list_helper_reports().await?.len();
        Ok(HelperLoadResponse {
            written_reports: request.report_count,
            helper_report_count,
            flushed,
            foreground_admission,
            background_admission,
        })
    }

    pub fn release_helper_pressure(&self) -> Result<Option<HelperPressureView>, DomainsAppError> {
        let mut state = self.lock_pressure()?;
        let previous = state.helper.take();
        if previous.is_some() {
            self.apply_helper_pressure_release(previous.as_ref());
        }
        Ok(previous)
    }

    pub async fn apply_primary_maintenance(
        &self,
        request: BackgroundMaintenanceRequest,
    ) -> Result<BackgroundMaintenanceResponse, DomainsAppError> {
        let flushed = if request.flush_now {
            self.primary_db.flush().await?;
            true
        } else {
            false
        };
        let pressure = BackgroundPressureView {
            hold_background_tasks: request.hold_background_tasks,
            background_in_flight_bytes: request.background_in_flight_bytes,
            queued_work_items: request.queued_work_items,
            queued_bytes: request.queued_bytes,
        };
        let background_admission = self.replace_primary_background_pressure(pressure.clone())?;
        Ok(BackgroundMaintenanceResponse {
            flushed,
            background_admission,
            active_pressure: (!pressure.is_empty()).then_some(pressure),
        })
    }

    pub fn release_primary_maintenance(
        &self,
    ) -> Result<Option<BackgroundPressureView>, DomainsAppError> {
        let mut state = self.lock_pressure()?;
        let previous = state.primary_background.take();
        if previous.is_some() {
            self.apply_primary_background_release(previous.as_ref());
        }
        Ok(previous)
    }

    pub async fn ensure_control_plane_table(
        &self,
        request: ControlPlaneTableRequest,
    ) -> Result<ControlPlaneTableResponse, DomainsAppError> {
        let table_name = normalize_non_empty("table_name", &request.table_name)?;
        let created = match self
            .db_for(request.database)
            .create_table(row_table_config(&table_name))
            .await
        {
            Ok(_) => true,
            Err(CreateTableError::AlreadyExists(_)) => false,
            Err(error) => return Err(error.into()),
        };
        Ok(ControlPlaneTableResponse {
            database: request.database,
            table_name,
            created,
        })
    }

    pub fn probe_admission(
        &self,
        request: AdmissionProbeRequest,
    ) -> Result<AdmissionProbeResponse, DomainsAppError> {
        let database = self.db_for(request.database);
        let lane = request.lane.as_execution_lane();
        let domain = database.execution_profile().binding(lane).domain.clone();
        let usage = request.usage.into();
        let manager = self.resource_manager();
        let decision = manager.try_acquire(&domain, usage);
        if decision.admitted {
            manager.release(&domain, usage);
        }
        Ok(AdmissionProbeResponse {
            database: request.database,
            lane: request.lane,
            decision,
        })
    }

    pub fn observability_report(&self) -> Result<DomainsObservabilityResponse, DomainsAppError> {
        Ok(DomainsObservabilityResponse {
            profile: self.profile,
            primary: self.primary_db.execution_placement_report(),
            analytics: self.analytics_db.execution_placement_report(),
            resource_manager: self.primary_db.resource_manager_snapshot(),
            active_pressure: self.lock_pressure()?.view(),
        })
    }

    pub fn release_all_pressure(&self) -> Result<(), DomainsAppError> {
        let mut state = self.lock_pressure()?;
        let primary = state.primary_background.take();
        let helper = state.helper.take();
        drop(state);
        self.apply_primary_background_release(primary.as_ref());
        self.apply_helper_pressure_release(helper.as_ref());
        Ok(())
    }

    fn resource_manager(&self) -> Arc<dyn ResourceManager> {
        self.primary_db.resource_manager()
    }

    fn replace_helper_pressure(
        &self,
        pressure: HelperPressureView,
    ) -> Result<
        (
            Option<ResourceAdmissionDecision>,
            Option<ResourceAdmissionDecision>,
        ),
        DomainsAppError,
    > {
        let mut state = self.lock_pressure()?;
        let previous = state.helper.take();
        drop(state);
        self.apply_helper_pressure_release(previous.as_ref());

        if pressure.is_empty() {
            return Ok((None, None));
        }

        let manager = self.resource_manager();
        let analytics_profile = self.analytics_db.execution_profile();
        let foreground_domain = analytics_profile
            .binding(ExampleLane::Foreground.as_execution_lane())
            .domain
            .clone();
        let background_domain = analytics_profile
            .binding(ExampleLane::Background.as_execution_lane())
            .domain
            .clone();

        let foreground_usage = pressure.foreground_usage();
        let background_usage = pressure.background.usage();
        let background_backlog = pressure.background.backlog();

        let foreground_admission = (pressure.hold_foreground_cpu_workers > 0)
            .then(|| manager.try_acquire(&foreground_domain, foreground_usage));
        let background_admission = (!pressure.background.is_empty()).then(|| {
            manager.set_backlog(&background_domain, background_backlog);
            manager.try_acquire(&background_domain, background_usage)
        });

        let admitted_pressure = HelperPressureView {
            hold_foreground_cpu_workers: foreground_admission
                .as_ref()
                .filter(|decision| decision.admitted)
                .map(|_| pressure.hold_foreground_cpu_workers)
                .unwrap_or_default(),
            background: BackgroundPressureView {
                hold_background_tasks: background_admission
                    .as_ref()
                    .filter(|decision| decision.admitted)
                    .map(|_| pressure.background.hold_background_tasks)
                    .unwrap_or_default(),
                background_in_flight_bytes: background_admission
                    .as_ref()
                    .filter(|decision| decision.admitted)
                    .map(|_| pressure.background.background_in_flight_bytes)
                    .unwrap_or_default(),
                queued_work_items: pressure.background.queued_work_items,
                queued_bytes: pressure.background.queued_bytes,
            },
        };

        if !admitted_pressure.is_empty() {
            self.lock_pressure()?.helper = Some(admitted_pressure);
        }

        Ok((foreground_admission, background_admission))
    }

    fn replace_primary_background_pressure(
        &self,
        pressure: BackgroundPressureView,
    ) -> Result<Option<ResourceAdmissionDecision>, DomainsAppError> {
        let mut state = self.lock_pressure()?;
        let previous = state.primary_background.take();
        drop(state);
        self.apply_primary_background_release(previous.as_ref());

        if pressure.is_empty() {
            return Ok(None);
        }

        let manager = self.resource_manager();
        let background_domain = self
            .primary_db
            .execution_profile()
            .binding(ExampleLane::Background.as_execution_lane())
            .domain
            .clone();
        manager.set_backlog(&background_domain, pressure.backlog());
        let admission = manager.try_acquire(&background_domain, pressure.usage());
        let admitted_pressure = BackgroundPressureView {
            hold_background_tasks: admission
                .admitted
                .then_some(pressure.hold_background_tasks)
                .unwrap_or_default(),
            background_in_flight_bytes: admission
                .admitted
                .then_some(pressure.background_in_flight_bytes)
                .unwrap_or_default(),
            queued_work_items: pressure.queued_work_items,
            queued_bytes: pressure.queued_bytes,
        };
        self.lock_pressure()?.primary_background = Some(admitted_pressure);
        Ok(Some(admission))
    }

    fn apply_primary_background_release(&self, pressure: Option<&BackgroundPressureView>) {
        let Some(pressure) = pressure else {
            return;
        };
        let manager = self.resource_manager();
        let background_domain = self
            .primary_db
            .execution_profile()
            .binding(ExampleLane::Background.as_execution_lane())
            .domain
            .clone();
        manager.release(&background_domain, pressure.usage());
        manager.set_backlog(&background_domain, Default::default());
    }

    fn apply_helper_pressure_release(&self, pressure: Option<&HelperPressureView>) {
        let Some(pressure) = pressure else {
            return;
        };
        let manager = self.resource_manager();
        let analytics_profile = self.analytics_db.execution_profile();
        let foreground_domain = analytics_profile
            .binding(ExampleLane::Foreground.as_execution_lane())
            .domain
            .clone();
        let background_domain = analytics_profile
            .binding(ExampleLane::Background.as_execution_lane())
            .domain
            .clone();
        manager.release(&foreground_domain, pressure.foreground_usage());
        manager.release(&background_domain, pressure.background.usage());
        manager.set_backlog(&background_domain, Default::default());
    }

    fn db_for(&self, database: ExampleDatabase) -> &Db {
        match database {
            ExampleDatabase::Primary => &self.primary_db,
            ExampleDatabase::Analytics => &self.analytics_db,
        }
    }

    fn lock_pressure(&self) -> Result<MutexGuard<'_, ActivePressureState>, DomainsAppError> {
        self.active_pressure
            .lock()
            .map_err(|_| DomainsAppError::StatePoisoned)
    }
}

pub fn domains_db_builder(path: &str, prefix: &str) -> DbBuilder {
    Db::builder().settings(domains_db_settings(path, prefix))
}

async fn ensure_table(db: &Db, config: terracedb::TableConfig) -> Result<Table, CreateTableError> {
    match db.create_table(config.clone()).await {
        Ok(table) => Ok(table),
        Err(CreateTableError::AlreadyExists(_)) => Ok(db.table(config.name)),
        Err(error) => Err(error),
    }
}

async fn collect_values<V>(mut stream: RecordStream<String, V>) -> Vec<V> {
    let mut rows = Vec::new();
    while let Some((_key, value)) = stream.next().await {
        rows.push(value);
    }
    rows
}

fn normalize_non_empty(field: &str, value: &str) -> Result<String, DomainsAppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(DomainsAppError::Usage(format!("{field} cannot be empty")));
    }
    Ok(trimmed.to_string())
}

async fn create_primary_item(
    State(state): State<DomainsAppState>,
    Json(request): Json<CreatePrimaryItemRequest>,
) -> Result<(StatusCode, Json<PrimaryItemRecord>), DomainsApiError> {
    let item = state.create_primary_item(request).await?;
    Ok((StatusCode::CREATED, Json(item)))
}

async fn list_primary_items(
    State(state): State<DomainsAppState>,
) -> Result<Json<Vec<PrimaryItemRecord>>, DomainsApiError> {
    Ok(Json(state.list_primary_items().await?))
}

async fn apply_primary_maintenance(
    State(state): State<DomainsAppState>,
    Json(request): Json<BackgroundMaintenanceRequest>,
) -> Result<Json<BackgroundMaintenanceResponse>, DomainsApiError> {
    Ok(Json(state.apply_primary_maintenance(request).await?))
}

async fn release_primary_maintenance(
    State(state): State<DomainsAppState>,
) -> Result<Json<Option<BackgroundPressureView>>, DomainsApiError> {
    Ok(Json(state.release_primary_maintenance()?))
}

async fn run_helper_load(
    State(state): State<DomainsAppState>,
    Json(request): Json<HelperLoadRequest>,
) -> Result<Json<HelperLoadResponse>, DomainsApiError> {
    Ok(Json(state.run_helper_load(request).await?))
}

async fn list_helper_reports(
    State(state): State<DomainsAppState>,
) -> Result<Json<Vec<HelperReportRecord>>, DomainsApiError> {
    Ok(Json(state.list_helper_reports().await?))
}

async fn release_helper_load(
    State(state): State<DomainsAppState>,
) -> Result<Json<Option<HelperPressureView>>, DomainsApiError> {
    Ok(Json(state.release_helper_pressure()?))
}

async fn ensure_control_plane_table(
    State(state): State<DomainsAppState>,
    Json(request): Json<ControlPlaneTableRequest>,
) -> Result<(StatusCode, Json<ControlPlaneTableResponse>), DomainsApiError> {
    let response = state.ensure_control_plane_table(request).await?;
    let status = if response.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(response)))
}

async fn probe_admission(
    State(state): State<DomainsAppState>,
    Json(request): Json<AdmissionProbeRequest>,
) -> Result<Json<AdmissionProbeResponse>, DomainsApiError> {
    Ok(Json(state.probe_admission(request)?))
}

async fn domains_report(
    State(state): State<DomainsAppState>,
) -> Result<Json<DomainsObservabilityResponse>, DomainsApiError> {
    Ok(Json(state.observability_report()?))
}

#[allow(dead_code)]
pub const DEFAULT_SERVER_PORT: u16 = DOMAINS_SERVER_PORT;
