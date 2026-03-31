use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use terracedb::{
    ColocatedDatabasePlacement, ColocatedDeployment, ColocatedDeploymentError, CompactionStrategy,
    DbConfig, DbSettings, ExecutionDomainBacklogSnapshot, ExecutionDomainBudget, ExecutionLane,
    ExecutionResourceUsage, S3Location, TableConfig, TableFormat, TieredDurabilityMode,
    TieredLocalRetentionMode, TieredStorageConfig,
};

pub const PRIMARY_DATABASE_NAME: &str = "primary";
pub const ANALYTICS_DATABASE_NAME: &str = "analytics";
pub const PRIMARY_ITEMS_TABLE_NAME: &str = "primary_items";
pub const HELPER_REPORTS_TABLE_NAME: &str = "helper_reports";
pub const DOMAINS_SERVER_PORT: u16 = 9603;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainsExampleProfile {
    #[default]
    Conservative,
    Balanced,
}

impl DomainsExampleProfile {
    pub fn deployment(self) -> Result<ColocatedDeployment, ColocatedDeploymentError> {
        match self {
            Self::Conservative => ColocatedDeployment::primary_with_analytics(
                domains_process_budget(),
                PRIMARY_DATABASE_NAME,
                ANALYTICS_DATABASE_NAME,
            ),
            Self::Balanced => ColocatedDeployment::builder(domains_process_budget())
                .with_database(
                    ColocatedDatabasePlacement::shared(PRIMARY_DATABASE_NAME)
                        .with_metadata("terracedb.execution.role", "primary"),
                )?
                .with_database(ColocatedDatabasePlacement::analytics_helper(
                    ANALYTICS_DATABASE_NAME,
                ))?
                .build()
                .pipe(Ok),
        }
    }
}

impl fmt::Display for DomainsExampleProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conservative => f.write_str("conservative"),
            Self::Balanced => f.write_str("balanced"),
        }
    }
}

impl FromStr for DomainsExampleProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "conservative" => Ok(Self::Conservative),
            "balanced" => Ok(Self::Balanced),
            other => Err(format!(
                "unknown domains example profile '{other}'; expected 'conservative' or 'balanced'"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExampleDatabase {
    Primary,
    Analytics,
}

impl ExampleDatabase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => PRIMARY_DATABASE_NAME,
            Self::Analytics => ANALYTICS_DATABASE_NAME,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExampleLane {
    Foreground,
    Background,
    ControlPlane,
}

impl ExampleLane {
    pub fn as_execution_lane(self) -> ExecutionLane {
        match self {
            Self::Foreground => ExecutionLane::UserForeground,
            Self::Background => ExecutionLane::UserBackground,
            Self::ControlPlane => ExecutionLane::ControlPlane,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryItemRecord {
    pub item_id: String,
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatePrimaryItemRequest {
    pub item_id: String,
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelperReportRecord {
    pub report_id: String,
    pub batch_id: String,
    pub ordinal: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelperLoadRequest {
    pub batch_id: String,
    pub report_count: u32,
    pub hold_foreground_cpu_workers: u32,
    pub hold_background_tasks: u32,
    pub background_in_flight_bytes: u64,
    pub queued_work_items: u32,
    pub queued_bytes: u64,
    pub flush_after_write: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundMaintenanceRequest {
    pub flush_now: bool,
    pub hold_background_tasks: u32,
    pub background_in_flight_bytes: u64,
    pub queued_work_items: u32,
    pub queued_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneTableRequest {
    pub database: ExampleDatabase,
    pub table_name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeUsage {
    pub cpu_workers: u32,
    pub memory_bytes: u64,
    pub local_io_concurrency: u32,
    pub local_io_bytes_per_second: u64,
    pub remote_io_concurrency: u32,
    pub remote_io_bytes_per_second: u64,
    pub background_tasks: u32,
    pub background_in_flight_bytes: u64,
}

impl From<ProbeUsage> for ExecutionResourceUsage {
    fn from(value: ProbeUsage) -> Self {
        Self {
            cpu_workers: value.cpu_workers,
            memory_bytes: value.memory_bytes,
            cache_bytes: 0,
            mutable_bytes: 0,
            local_io_concurrency: value.local_io_concurrency,
            local_io_bytes_per_second: value.local_io_bytes_per_second,
            remote_io_concurrency: value.remote_io_concurrency,
            remote_io_bytes_per_second: value.remote_io_bytes_per_second,
            background_tasks: value.background_tasks,
            background_in_flight_bytes: value.background_in_flight_bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionProbeRequest {
    pub database: ExampleDatabase,
    pub lane: ExampleLane,
    pub usage: ProbeUsage,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundPressureView {
    pub hold_background_tasks: u32,
    pub background_in_flight_bytes: u64,
    pub queued_work_items: u32,
    pub queued_bytes: u64,
}

impl BackgroundPressureView {
    pub fn is_empty(&self) -> bool {
        self.hold_background_tasks == 0
            && self.background_in_flight_bytes == 0
            && self.queued_work_items == 0
            && self.queued_bytes == 0
    }

    pub fn usage(&self) -> ExecutionResourceUsage {
        ExecutionResourceUsage {
            background_tasks: self.hold_background_tasks,
            background_in_flight_bytes: self.background_in_flight_bytes,
            ..ExecutionResourceUsage::default()
        }
    }

    pub fn backlog(&self) -> ExecutionDomainBacklogSnapshot {
        ExecutionDomainBacklogSnapshot {
            queued_work_items: self.queued_work_items,
            queued_bytes: self.queued_bytes,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelperPressureView {
    pub hold_foreground_cpu_workers: u32,
    pub background: BackgroundPressureView,
}

impl HelperPressureView {
    pub fn foreground_usage(&self) -> ExecutionResourceUsage {
        ExecutionResourceUsage {
            cpu_workers: self.hold_foreground_cpu_workers,
            ..ExecutionResourceUsage::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hold_foreground_cpu_workers == 0 && self.background.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivePressureView {
    pub primary_background: Option<BackgroundPressureView>,
    pub helper: Option<HelperPressureView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelperLoadResponse {
    pub written_reports: u32,
    pub helper_report_count: usize,
    pub flushed: bool,
    pub foreground_admission: Option<terracedb::ResourceAdmissionDecision>,
    pub background_admission: Option<terracedb::ResourceAdmissionDecision>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundMaintenanceResponse {
    pub flushed: bool,
    pub background_admission: Option<terracedb::ResourceAdmissionDecision>,
    pub active_pressure: Option<BackgroundPressureView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneTableResponse {
    pub database: ExampleDatabase,
    pub table_name: String,
    pub created: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionProbeResponse {
    pub database: ExampleDatabase,
    pub lane: ExampleLane,
    pub decision: terracedb::ResourceAdmissionDecision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainsObservabilityResponse {
    pub profile: DomainsExampleProfile,
    pub primary: terracedb::DbExecutionPlacementReport,
    pub analytics: terracedb::DbExecutionPlacementReport,
    pub resource_manager: terracedb::ResourceManagerSnapshot,
    pub active_pressure: ActivePressureView,
}

pub fn domains_process_budget() -> ExecutionDomainBudget {
    ExecutionDomainBudget {
        cpu: terracedb::DomainCpuBudget {
            worker_slots: Some(16),
            weight: None,
        },
        memory: terracedb::DomainMemoryBudget {
            total_bytes: Some(8 * 1024),
            cache_bytes: Some(6 * 1024),
            mutable_bytes: Some(6 * 1024),
        },
        io: terracedb::DomainIoBudget {
            local_concurrency: Some(8),
            local_bytes_per_second: Some(16 * 1024),
            remote_concurrency: Some(4),
            remote_bytes_per_second: Some(8 * 1024),
        },
        background: terracedb::DomainBackgroundBudget {
            task_slots: Some(6),
            max_in_flight_bytes: Some(8 * 1024),
        },
    }
}

pub fn domains_db_settings(path: &str, prefix: &str) -> DbSettings {
    DbSettings::tiered_storage(TieredStorageConfig {
        ssd: terracedb::SsdConfig {
            path: path.to_string(),
        },
        s3: S3Location {
            bucket: "terracedb-domains-example".to_string(),
            prefix: prefix.to_string(),
        },
        max_local_bytes: 1024 * 1024,
        durability: TieredDurabilityMode::GroupCommit,
        local_retention: TieredLocalRetentionMode::Offload,
    })
}

pub fn domains_db_config(path: &str, prefix: &str) -> DbConfig {
    DbConfig {
        storage: domains_db_settings(path, prefix).into_storage(),
        hybrid_read: Default::default(),
        scheduler: None,
    }
}

pub fn row_table_config(name: &str) -> TableConfig {
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

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}
