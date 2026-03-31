use std::str::FromStr;

use serde::{Deserialize, Serialize};
use terracedb::ScanExecution;

pub const TEMPERATURE_C_FIELD_NAME: &str = "temperature_c";
pub const HUMIDITY_PCT_FIELD_NAME: &str = "humidity_pct";
pub const BATTERY_MV_FIELD_NAME: &str = "battery_mv";
pub const ALERT_ACTIVE_FIELD_NAME: &str = "alert_active";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryExampleProfile {
    Base,
    Accelerated,
}

impl TelemetryExampleProfile {
    pub const fn env_value(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Accelerated => "accelerated",
        }
    }
}

impl Default for TelemetryExampleProfile {
    fn default() -> Self {
        Self::Base
    }
}

impl FromStr for TelemetryExampleProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "base" => Ok(Self::Base),
            "accelerated" | "accelerator" => Ok(Self::Accelerated),
            other => Err(format!(
                "unknown telemetry profile {other}; expected base or accelerated"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryColumn {
    TemperatureC,
    HumidityPct,
    BatteryMv,
    AlertActive,
}

impl TelemetryColumn {
    pub const ALL: [Self; 4] = [
        Self::TemperatureC,
        Self::HumidityPct,
        Self::BatteryMv,
        Self::AlertActive,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TemperatureC => TEMPERATURE_C_FIELD_NAME,
            Self::HumidityPct => HUMIDITY_PCT_FIELD_NAME,
            Self::BatteryMv => BATTERY_MV_FIELD_NAME,
            Self::AlertActive => ALERT_ACTIVE_FIELD_NAME,
        }
    }

    pub fn parse_csv(value: Option<&str>) -> Result<Vec<Self>, String> {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(Self::ALL.to_vec());
        };

        let mut columns = Vec::new();
        for raw in value.split(',') {
            let column = Self::from_str(raw.trim())?;
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
        if columns.is_empty() {
            return Err("columns must contain at least one field".to_string());
        }
        Ok(columns)
    }
}

impl FromStr for TelemetryColumn {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            TEMPERATURE_C_FIELD_NAME => Ok(Self::TemperatureC),
            HUMIDITY_PCT_FIELD_NAME => Ok(Self::HumidityPct),
            BATTERY_MV_FIELD_NAME => Ok(Self::BatteryMv),
            ALERT_ACTIVE_FIELD_NAME => Ok(Self::AlertActive),
            other => Err(format!(
                "unsupported column {other}; expected one of temperature_c, humidity_pct, battery_mv, alert_active"
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestReading {
    pub device_id: String,
    pub reading_at_ms: u64,
    pub temperature_c: i64,
    pub humidity_pct: i64,
    pub battery_mv: i64,
    pub alert_active: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestReadingsRequest {
    pub readings: Vec<IngestReading>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceStateRecord {
    pub device_id: String,
    pub latest_reading_at_ms: u64,
    pub temperature_c: i64,
    pub humidity_pct: i64,
    pub battery_mv: i64,
    pub alert_active: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestReadingsResponse {
    pub received_readings: usize,
    pub accepted_readings: usize,
    pub updated_devices: Vec<DeviceStateRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TelemetryScanRow {
    pub device_id: String,
    pub reading_at_ms: u64,
    pub temperature_c: Option<i64>,
    pub humidity_pct: Option<i64>,
    pub battery_mv: Option<i64>,
    pub alert_active: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TelemetryScanResponse {
    pub device_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub columns: Vec<TelemetryColumn>,
    pub only_alerts: bool,
    pub rows: Vec<TelemetryScanRow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ScanExecution>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TelemetrySummaryResponse {
    pub device_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub reading_count: usize,
    pub alert_count: usize,
    pub min_temperature_c: Option<i64>,
    pub max_temperature_c: Option<i64>,
    pub average_temperature_c: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelemetryScanQuery {
    pub start_ms: u64,
    pub end_ms: u64,
    pub columns: Option<String>,
    #[serde(default)]
    pub only_alerts: bool,
    #[serde(default)]
    pub debug: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelemetryWindowQuery {
    pub start_ms: u64,
    pub end_ms: u64,
}
