use std::collections::BTreeMap;

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

use terracedb::{
    CommitError, CompactionStrategy, CreateTableError, Db, DbBuilder, DbConfig, DbSettings,
    FieldDefinition, FieldId, FieldType, FieldValue, HybridCompactToWidePromotionConfig,
    HybridProjectionSidecarConfig, HybridReadConfig, HybridSkipIndexConfig, HybridSkipIndexFamily,
    HybridTableFeatures, OpenError, ReadError, S3Location, ScanOptions, SchemaDefinition,
    StorageError, Table, TableConfig, TableFormat, TieredDurabilityMode, TieredStorageConfig,
    Transaction, TransactionCommitError, Value,
};
use terracedb_records::{
    JsonValueCodec, RecordCodecError, RecordReadError, RecordTable, RecordWriteError,
    Utf8StringCodec,
};

use crate::model::{
    ALERT_ACTIVE_FIELD_NAME, BATTERY_MV_FIELD_NAME, DeviceStateRecord, HUMIDITY_PCT_FIELD_NAME,
    IngestReading, IngestReadingsRequest, IngestReadingsResponse, TEMPERATURE_C_FIELD_NAME,
    TelemetryColumn, TelemetryExampleProfile, TelemetryScanQuery, TelemetryScanResponse,
    TelemetryScanRow, TelemetrySummaryResponse, TelemetryWindowQuery,
};

pub const DEVICE_STATE_TABLE_NAME: &str = "device_state";
pub const SENSOR_READINGS_TABLE_NAME: &str = "sensor_readings";
pub const TELEMETRY_SERVER_PORT: u16 = 9602;

const TEMPERATURE_C_FIELD_ID: FieldId = FieldId::new(1);
const HUMIDITY_PCT_FIELD_ID: FieldId = FieldId::new(2);
const BATTERY_MV_FIELD_ID: FieldId = FieldId::new(3);
const ALERT_ACTIVE_FIELD_ID: FieldId = FieldId::new(4);

type DeviceStateTable =
    RecordTable<String, DeviceStateRecord, Utf8StringCodec, JsonValueCodec<DeviceStateRecord>>;

#[derive(Debug, Error)]
pub enum TelemetryAppError {
    #[error(transparent)]
    Open(#[from] OpenError),
    #[error(transparent)]
    CreateTable(#[from] CreateTableError),
    #[error(transparent)]
    Read(#[from] ReadError),
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
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("{0}")]
    InvalidRecord(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum TelemetryApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Internal(String),
}

impl From<TelemetryAppError> for TelemetryApiError {
    fn from(error: TelemetryAppError) -> Self {
        Self::Internal(error.to_string())
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for TelemetryApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
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
pub struct SensorReadingsTable {
    table: Table,
    schema: SchemaDefinition,
}

impl SensorReadingsTable {
    pub fn table(&self) -> &Table {
        &self.table
    }

    fn encode_key(&self, device_id: &str, reading_at_ms: u64) -> Vec<u8> {
        format!("device:{device_id}:{reading_at_ms:020}").into_bytes()
    }

    fn window_bounds(&self, device_id: &str, start_ms: u64, end_ms: u64) -> (Vec<u8>, Vec<u8>) {
        (
            self.encode_key(device_id, start_ms),
            self.encode_key(device_id, end_ms),
        )
    }

    fn encode_value(&self, reading: &IngestReading) -> Result<Value, TelemetryAppError> {
        Ok(Value::named_record(
            &self.schema,
            [
                (
                    TEMPERATURE_C_FIELD_NAME,
                    FieldValue::Int64(reading.temperature_c),
                ),
                (
                    HUMIDITY_PCT_FIELD_NAME,
                    FieldValue::Int64(reading.humidity_pct),
                ),
                (BATTERY_MV_FIELD_NAME, FieldValue::Int64(reading.battery_mv)),
                (
                    ALERT_ACTIVE_FIELD_NAME,
                    FieldValue::Bool(reading.alert_active),
                ),
            ],
        )?)
    }

    async fn scan_window(
        &self,
        device_id: &str,
        start_ms: u64,
        end_ms: u64,
        columns: &[TelemetryColumn],
    ) -> Result<Vec<TelemetryScanRow>, TelemetryAppError> {
        let (start, end) = self.window_bounds(device_id, start_ms, end_ms);
        let mut stream = self
            .table
            .scan(
                start,
                end,
                ScanOptions {
                    columns: Some(
                        columns
                            .iter()
                            .map(|column| column.as_str().to_string())
                            .collect(),
                    ),
                    ..ScanOptions::default()
                },
            )
            .await?;
        let mut rows = Vec::new();
        while let Some((key, value)) = stream.next().await {
            rows.push(self.decode_projected_row(device_id, &key, &value, columns)?);
        }
        Ok(rows)
    }

    fn decode_projected_row(
        &self,
        device_id: &str,
        key: &[u8],
        value: &Value,
        columns: &[TelemetryColumn],
    ) -> Result<TelemetryScanRow, TelemetryAppError> {
        let Value::Record(record) = value else {
            return Err(TelemetryAppError::InvalidRecord(
                "columnar telemetry rows must decode as records".to_string(),
            ));
        };
        let include = |column| columns.contains(&column);
        Ok(TelemetryScanRow {
            device_id: device_id.to_string(),
            reading_at_ms: decode_timestamp_from_key(key)?,
            temperature_c: include(TelemetryColumn::TemperatureC)
                .then(|| extract_i64(record, TEMPERATURE_C_FIELD_ID, TEMPERATURE_C_FIELD_NAME))
                .transpose()?,
            humidity_pct: include(TelemetryColumn::HumidityPct)
                .then(|| extract_i64(record, HUMIDITY_PCT_FIELD_ID, HUMIDITY_PCT_FIELD_NAME))
                .transpose()?,
            battery_mv: include(TelemetryColumn::BatteryMv)
                .then(|| extract_i64(record, BATTERY_MV_FIELD_ID, BATTERY_MV_FIELD_NAME))
                .transpose()?,
            alert_active: include(TelemetryColumn::AlertActive)
                .then(|| extract_bool(record, ALERT_ACTIVE_FIELD_ID, ALERT_ACTIVE_FIELD_NAME))
                .transpose()?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TelemetryTables {
    device_state: DeviceStateTable,
    sensor_readings: SensorReadingsTable,
}

impl TelemetryTables {
    pub fn device_state(&self) -> &DeviceStateTable {
        &self.device_state
    }

    pub fn sensor_readings(&self) -> &SensorReadingsTable {
        &self.sensor_readings
    }

    pub fn sensor_readings_raw(&self) -> &Table {
        self.sensor_readings.table()
    }
}

#[derive(Clone, Debug)]
pub struct TelemetryAppState {
    db: Db,
    profile: TelemetryExampleProfile,
    tables: TelemetryTables,
}

pub struct TelemetryApp {
    state: TelemetryAppState,
}

impl TelemetryApp {
    pub async fn open(db: Db, profile: TelemetryExampleProfile) -> Result<Self, TelemetryAppError> {
        let tables = ensure_telemetry_tables(&db).await?;
        Ok(Self {
            state: TelemetryAppState {
                db,
                profile,
                tables,
            },
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/readings", post(ingest_readings))
            .route("/devices/{device_id}/state", get(get_device_state))
            .route("/devices/{device_id}/readings", get(scan_readings))
            .route("/devices/{device_id}/summary", get(reading_summary))
            .with_state(self.state.clone())
    }

    pub fn state(&self) -> &TelemetryAppState {
        &self.state
    }

    pub fn tables(&self) -> &TelemetryTables {
        &self.state.tables
    }

    pub async fn shutdown(self) -> Result<(), TelemetryAppError> {
        Ok(())
    }
}

impl TelemetryAppState {
    pub fn profile(&self) -> TelemetryExampleProfile {
        self.profile
    }

    pub fn tables(&self) -> &TelemetryTables {
        &self.tables
    }

    pub async fn ingest_readings(
        &self,
        request: IngestReadingsRequest,
    ) -> Result<IngestReadingsResponse, TelemetryApiError> {
        if request.readings.is_empty() {
            return Err(TelemetryApiError::BadRequest(
                "readings must contain at least one item".to_string(),
            ));
        }

        let mut deduped = BTreeMap::<(String, u64), IngestReading>::new();
        for reading in request.readings {
            let device_id = normalize_device_id(&reading.device_id)?;
            deduped.insert(
                (device_id.clone(), reading.reading_at_ms),
                IngestReading {
                    device_id,
                    ..reading
                },
            );
        }

        let accepted_readings = deduped.len();
        let mut tx = Transaction::begin(&self.db).await;
        let mut updated_devices = BTreeMap::<String, DeviceStateRecord>::new();
        for ((device_id, _reading_at_ms), reading) in deduped {
            tx.write(
                self.tables.sensor_readings.table(),
                self.tables
                    .sensor_readings
                    .encode_key(&device_id, reading.reading_at_ms),
                self.tables.sensor_readings.encode_value(&reading)?,
            );

            let existing = self
                .tables
                .device_state
                .read_in_str(&mut tx, &device_id)
                .await
                .map_err(TelemetryAppError::from)?;
            let should_replace = existing
                .as_ref()
                .is_none_or(|state| state.latest_reading_at_ms <= reading.reading_at_ms);
            if should_replace {
                let state = DeviceStateRecord {
                    device_id: device_id.clone(),
                    latest_reading_at_ms: reading.reading_at_ms,
                    temperature_c: reading.temperature_c,
                    humidity_pct: reading.humidity_pct,
                    battery_mv: reading.battery_mv,
                    alert_active: reading.alert_active,
                };
                self.tables
                    .device_state
                    .write_in_str(&mut tx, &device_id, &state)
                    .map_err(TelemetryAppError::from)?;
                updated_devices.insert(device_id, state);
            }
        }

        tx.commit().await.map_err(TelemetryAppError::from)?;

        Ok(IngestReadingsResponse {
            accepted_readings,
            updated_devices: updated_devices.into_values().collect(),
        })
    }

    pub async fn get_device_state(
        &self,
        device_id: &str,
    ) -> Result<Option<DeviceStateRecord>, TelemetryAppError> {
        self.tables
            .device_state
            .read_str(device_id)
            .await
            .map_err(Into::into)
    }

    pub async fn scan_window(
        &self,
        device_id: &str,
        start_ms: u64,
        end_ms: u64,
        columns: Vec<TelemetryColumn>,
        only_alerts: bool,
    ) -> Result<TelemetryScanResponse, TelemetryApiError> {
        validate_window(start_ms, end_ms)?;
        let device_id = normalize_device_id(device_id)?;
        let response_columns = dedup_columns(columns);
        let mut scan_columns = response_columns.clone();
        if only_alerts && !scan_columns.contains(&TelemetryColumn::AlertActive) {
            scan_columns.push(TelemetryColumn::AlertActive);
        }

        let rows = self
            .tables
            .sensor_readings
            .scan_window(&device_id, start_ms, end_ms, &scan_columns)
            .await?
            .into_iter()
            .filter(|row| !only_alerts || row.alert_active == Some(true))
            .map(|mut row| {
                if !response_columns.contains(&TelemetryColumn::TemperatureC) {
                    row.temperature_c = None;
                }
                if !response_columns.contains(&TelemetryColumn::HumidityPct) {
                    row.humidity_pct = None;
                }
                if !response_columns.contains(&TelemetryColumn::BatteryMv) {
                    row.battery_mv = None;
                }
                if !response_columns.contains(&TelemetryColumn::AlertActive) {
                    row.alert_active = None;
                }
                row
            })
            .collect();

        Ok(TelemetryScanResponse {
            device_id,
            start_ms,
            end_ms,
            columns: response_columns,
            only_alerts,
            rows,
        })
    }

    pub async fn reading_summary(
        &self,
        device_id: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<TelemetrySummaryResponse, TelemetryApiError> {
        validate_window(start_ms, end_ms)?;
        let device_id = normalize_device_id(device_id)?;
        let rows = self
            .tables
            .sensor_readings
            .scan_window(
                &device_id,
                start_ms,
                end_ms,
                &[TelemetryColumn::TemperatureC, TelemetryColumn::AlertActive],
            )
            .await?;
        let reading_count = rows.len();
        let alert_count = rows
            .iter()
            .filter(|row| row.alert_active == Some(true))
            .count();
        let min_temperature_c = rows.iter().filter_map(|row| row.temperature_c).min();
        let max_temperature_c = rows.iter().filter_map(|row| row.temperature_c).max();
        let average_temperature_c = (!rows.is_empty()).then(|| {
            rows.iter()
                .filter_map(|row| row.temperature_c)
                .map(|value| value as f64)
                .sum::<f64>()
                / rows.len() as f64
        });

        Ok(TelemetrySummaryResponse {
            device_id,
            start_ms,
            end_ms,
            reading_count,
            alert_count,
            min_temperature_c,
            max_temperature_c,
            average_temperature_c,
        })
    }
}

pub fn telemetry_db_settings(path: &str, prefix: &str) -> DbSettings {
    DbSettings::tiered_storage(TieredStorageConfig {
        ssd: terracedb::SsdConfig {
            path: path.to_string(),
        },
        s3: S3Location {
            bucket: "terracedb-example-telemetry-api".to_string(),
            prefix: prefix.to_string(),
        },
        max_local_bytes: 32 * 1024,
        durability: TieredDurabilityMode::GroupCommit,
        local_retention: terracedb::TieredLocalRetentionMode::Offload,
    })
}

pub fn telemetry_db_builder(
    path: &str,
    prefix: &str,
    profile: TelemetryExampleProfile,
) -> DbBuilder {
    Db::builder()
        .settings(telemetry_db_settings(path, prefix))
        .hybrid_read_config(profile_hybrid_read_config(profile))
}

pub fn telemetry_db_config(path: &str, prefix: &str, profile: TelemetryExampleProfile) -> DbConfig {
    DbConfig {
        storage: telemetry_db_settings(path, prefix).into_storage(),
        hybrid_read: profile_hybrid_read_config(profile),
        scheduler: None,
    }
}

pub async fn ensure_telemetry_tables(db: &Db) -> Result<TelemetryTables, TelemetryAppError> {
    let sensor_schema = telemetry_sensor_schema();
    Ok(TelemetryTables {
        device_state: RecordTable::with_codecs(
            ensure_table(db, row_table_config(DEVICE_STATE_TABLE_NAME)).await?,
            Utf8StringCodec,
            JsonValueCodec::new(),
        ),
        sensor_readings: SensorReadingsTable {
            table: ensure_table(db, sensor_readings_table_config(SENSOR_READINGS_TABLE_NAME))
                .await?,
            schema: sensor_schema,
        },
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

fn sensor_readings_table_config(name: &str) -> TableConfig {
    with_compact_to_wide_promotion(
        with_hybrid_features(
            TableConfig {
                name: name.to_string(),
                format: TableFormat::Columnar,
                merge_operator: None,
                max_merge_operand_chain_length: None,
                compaction_filter: None,
                bloom_filter_bits_per_key: Some(10),
                history_retention_sequences: None,
                compaction_strategy: CompactionStrategy::Leveled,
                schema: Some(telemetry_sensor_schema()),
                metadata: Default::default(),
            },
            HybridTableFeatures {
                skip_indexes: vec![HybridSkipIndexConfig {
                    name: "alert-active".to_string(),
                    family: HybridSkipIndexFamily::FieldValueBloom,
                    field: Some(ALERT_ACTIVE_FIELD_NAME.to_string()),
                    max_values: 16,
                }],
                projection_sidecars: vec![HybridProjectionSidecarConfig {
                    name: "temperature-alert-summary".to_string(),
                    fields: vec![
                        TEMPERATURE_C_FIELD_NAME.to_string(),
                        ALERT_ACTIVE_FIELD_NAME.to_string(),
                    ],
                }],
            },
        ),
        HybridCompactToWidePromotionConfig {
            max_compact_rows: 8,
            ..HybridCompactToWidePromotionConfig::default()
        },
    )
}

fn with_hybrid_features(mut config: TableConfig, features: HybridTableFeatures) -> TableConfig {
    config.metadata.insert(
        terracedb::HYBRID_TABLE_FEATURES_METADATA_KEY.to_string(),
        serde_json::to_value(features).expect("serialize telemetry hybrid features"),
    );
    config
}

fn with_compact_to_wide_promotion(
    mut config: TableConfig,
    promotion: HybridCompactToWidePromotionConfig,
) -> TableConfig {
    config.metadata.insert(
        terracedb::HYBRID_COMPACT_TO_WIDE_PROMOTION_METADATA_KEY.to_string(),
        serde_json::to_value(promotion).expect("serialize telemetry compact-to-wide config"),
    );
    config
}

fn telemetry_sensor_schema() -> SchemaDefinition {
    SchemaDefinition {
        version: 1,
        fields: vec![
            FieldDefinition {
                id: TEMPERATURE_C_FIELD_ID,
                name: TEMPERATURE_C_FIELD_NAME.to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                default: Some(FieldValue::Int64(0)),
            },
            FieldDefinition {
                id: HUMIDITY_PCT_FIELD_ID,
                name: HUMIDITY_PCT_FIELD_NAME.to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                default: Some(FieldValue::Int64(0)),
            },
            FieldDefinition {
                id: BATTERY_MV_FIELD_ID,
                name: BATTERY_MV_FIELD_NAME.to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                default: Some(FieldValue::Int64(0)),
            },
            FieldDefinition {
                id: ALERT_ACTIVE_FIELD_ID,
                name: ALERT_ACTIVE_FIELD_NAME.to_string(),
                field_type: FieldType::Bool,
                nullable: false,
                default: Some(FieldValue::Bool(false)),
            },
        ],
    }
}

fn profile_hybrid_read_config(profile: TelemetryExampleProfile) -> HybridReadConfig {
    let mut config = HybridReadConfig {
        raw_segment_cache_bytes: 64 * 1024,
        decoded_metadata_cache_entries: 32,
        decoded_column_cache_entries: 64,
        ..HybridReadConfig::default()
    };
    if matches!(profile, TelemetryExampleProfile::Accelerated) {
        config.skip_indexes_enabled = true;
        config.projection_sidecars_enabled = true;
        config.aggressive_background_repair = true;
        config.compact_to_wide_promotion_enabled = true;
    }
    config
}

fn normalize_device_id(value: &str) -> Result<String, TelemetryApiError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(TelemetryApiError::BadRequest(
            "device_id cannot be empty".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

fn dedup_columns(columns: Vec<TelemetryColumn>) -> Vec<TelemetryColumn> {
    let mut deduped = Vec::new();
    for column in columns {
        if !deduped.contains(&column) {
            deduped.push(column);
        }
    }
    deduped
}

fn validate_window(start_ms: u64, end_ms: u64) -> Result<(), TelemetryApiError> {
    if end_ms <= start_ms {
        return Err(TelemetryApiError::BadRequest(
            "end_ms must be greater than start_ms".to_string(),
        ));
    }
    Ok(())
}

fn decode_timestamp_from_key(key: &[u8]) -> Result<u64, TelemetryAppError> {
    let key = std::str::from_utf8(key)?;
    let (_, timestamp) = key.rsplit_once(':').ok_or_else(|| {
        TelemetryAppError::InvalidRecord(format!("telemetry key {key} is missing a timestamp"))
    })?;
    Ok(timestamp.parse()?)
}

fn extract_i64(
    record: &BTreeMap<FieldId, FieldValue>,
    field_id: FieldId,
    field_name: &str,
) -> Result<i64, TelemetryAppError> {
    match record.get(&field_id) {
        Some(FieldValue::Int64(value)) => Ok(*value),
        other => Err(TelemetryAppError::InvalidRecord(format!(
            "field {field_name} was expected to be int64, got {other:?}"
        ))),
    }
}

fn extract_bool(
    record: &BTreeMap<FieldId, FieldValue>,
    field_id: FieldId,
    field_name: &str,
) -> Result<bool, TelemetryAppError> {
    match record.get(&field_id) {
        Some(FieldValue::Bool(value)) => Ok(*value),
        other => Err(TelemetryAppError::InvalidRecord(format!(
            "field {field_name} was expected to be bool, got {other:?}"
        ))),
    }
}

async fn ingest_readings(
    State(state): State<TelemetryAppState>,
    Json(request): Json<IngestReadingsRequest>,
) -> Result<(StatusCode, Json<IngestReadingsResponse>), TelemetryApiError> {
    let response = state.ingest_readings(request).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_device_state(
    State(state): State<TelemetryAppState>,
    Path(device_id): Path<String>,
) -> Result<Json<DeviceStateRecord>, TelemetryApiError> {
    let device_id = normalize_device_id(&device_id)?;
    let state_row = state
        .get_device_state(&device_id)
        .await?
        .ok_or_else(|| TelemetryApiError::NotFound(format!("device {device_id} was not found")))?;
    Ok(Json(state_row))
}

async fn scan_readings(
    State(state): State<TelemetryAppState>,
    Path(device_id): Path<String>,
    Query(query): Query<TelemetryScanQuery>,
) -> Result<Json<TelemetryScanResponse>, TelemetryApiError> {
    let columns = TelemetryColumn::parse_csv(query.columns.as_deref())
        .map_err(TelemetryApiError::BadRequest)?;
    Ok(Json(
        state
            .scan_window(
                &device_id,
                query.start_ms,
                query.end_ms,
                columns,
                query.only_alerts,
            )
            .await?,
    ))
}

async fn reading_summary(
    State(state): State<TelemetryAppState>,
    Path(device_id): Path<String>,
    Query(query): Query<TelemetryWindowQuery>,
) -> Result<Json<TelemetrySummaryResponse>, TelemetryApiError> {
    Ok(Json(
        state
            .reading_summary(&device_id, query.start_ms, query.end_ms)
            .await?,
    ))
}
