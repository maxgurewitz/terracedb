use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use terracedb::{
    AdmissionDiagnostics, ColocatedDatabasePlacement, ColocatedDeployment,
    ColocatedDeploymentError, DbConfig, DbSettings, ExecutionDomainBacklogSnapshot,
    ExecutionDomainBudget, ExecutionDomainPath, ExecutionLane, ExecutionResourceUsage,
    PressureStats, S3Location, TableConfig, TieredDurabilityMode, TieredLocalRetentionMode,
    TieredStorageConfig,
};

pub const PRIMARY_DATABASE_NAME: &str = "primary";
pub const ANALYTICS_DATABASE_NAME: &str = "analytics";
pub const PRIMARY_ITEMS_TABLE_NAME: &str = "primary_items";
pub const HELPER_REPORTS_TABLE_NAME: &str = "helper_reports";
pub const DOMAINS_SERVER_PORT: u16 = 9603;
pub const DEFAULT_PRIMARY_BURST_TITLE_BYTES: u32 = 1_024;
pub const PRIMARY_PRESSURE_PROBE_WRITE_BYTES: u64 = 1_024;
pub const HELPER_PRESSURE_PROBE_WRITE_BYTES: u64 = 1_024;
const EXAMPLE_STORAGE_MAX_LOCAL_BYTES: u64 = 16 * 1_024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainsExampleProfile {
    #[default]
    Conservative,
    PrimaryProtected,
}

impl DomainsExampleProfile {
    pub fn deployment(self) -> Result<ColocatedDeployment, ColocatedDeploymentError> {
        let (primary, analytics) = match self {
            Self::Conservative => (
                primary_database_placement(3, 2, 6 * 1_024, 4 * 1_024, 2, 2 * 1_024),
                analytics_helper_placement(1, 1, 8 * 1_024, 6 * 1_024, 2, 3 * 1_024),
            ),
            Self::PrimaryProtected => (
                primary_database_placement(4, 1, 4 * 1_024, 2 * 1_024, 1, 1 * 1_024),
                analytics_helper_placement(1, 1, 8 * 1_024, 4 * 1_024, 1, 2 * 1_024),
            ),
        };

        ColocatedDeployment::builder(domains_process_budget())
            .with_database(primary)?
            .with_database(analytics)?
            .build()
            .pipe(Ok)
    }
}

impl fmt::Display for DomainsExampleProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conservative => f.write_str("conservative"),
            Self::PrimaryProtected => f.write_str("primary_protected"),
        }
    }
}

impl FromStr for DomainsExampleProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "conservative" => Ok(Self::Conservative),
            "primary_protected" | "primary-protected" => Ok(Self::PrimaryProtected),
            other => Err(format!(
                "unknown domains example profile '{other}'; expected 'conservative' or 'primary_protected'"
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
pub struct PrimaryBurstRequest {
    pub batch_id: String,
    pub item_count: u32,
    pub title_bytes: u32,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivePressureView {
    pub primary_background: Option<BackgroundPressureView>,
    pub helper: Option<HelperPressureView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelperLoadResponse {
    pub written_reports: u32,
    pub helper_report_count: usize,
    pub flushed: bool,
    pub flush_status: Option<terracedb::FlushStatus>,
    pub foreground_admission: Option<terracedb::ResourceAdmissionDecision>,
    pub background_admission: Option<terracedb::ResourceAdmissionDecision>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundMaintenanceResponse {
    pub flushed: bool,
    pub flush_status: Option<terracedb::FlushStatus>,
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
pub struct WorkloadObservabilityView {
    pub table_name: String,
    pub foreground_domain: ExecutionDomainPath,
    pub background_domain: ExecutionDomainPath,
    pub sample_batch_write_bytes: u64,
    pub current_pressure: PressureStats,
    pub next_write_admission: AdmissionDiagnostics,
    pub throttle_active: bool,
    pub stall_active: bool,
    pub throttled_write_events: u64,
    pub last_non_open_write_admission: Option<terracedb::RecordedAdmissionDiagnostics>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryBurstResponse {
    pub batch_id: String,
    pub written_items: u32,
    pub primary_item_count: usize,
    pub commit_sequence: u64,
    pub primary_writer: WorkloadObservabilityView,
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
    pub deployment: terracedb::ColocatedDeploymentReport,
    pub active_pressure: ActivePressureView,
    pub primary_writer: WorkloadObservabilityView,
    pub helper_writer: WorkloadObservabilityView,
}

pub fn domains_process_budget() -> ExecutionDomainBudget {
    ExecutionDomainBudget {
        cpu: terracedb::DomainCpuBudget {
            worker_slots: Some(16),
            weight: None,
        },
        memory: terracedb::DomainMemoryBudget {
            total_bytes: Some(16 * 1_024),
            cache_bytes: Some(8 * 1_024),
            mutable_bytes: Some(8 * 1_024),
        },
        io: terracedb::DomainIoBudget {
            local_concurrency: Some(8),
            local_bytes_per_second: Some(16 * 1024),
            remote_concurrency: Some(4),
            remote_bytes_per_second: Some(8 * 1024),
        },
        background: terracedb::DomainBackgroundBudget {
            task_slots: Some(6),
            max_in_flight_bytes: Some(4 * 1_024),
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
        max_local_bytes: EXAMPLE_STORAGE_MAX_LOCAL_BYTES,
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
    TableConfig::row(name).build()
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

fn primary_database_placement(
    foreground_weight: u32,
    background_weight: u32,
    foreground_mutable_bytes: u64,
    background_mutable_bytes: u64,
    background_task_slots: u32,
    background_in_flight_bytes: u64,
) -> ColocatedDatabasePlacement {
    let mut placement = ColocatedDatabasePlacement::shared(PRIMARY_DATABASE_NAME);
    placement.foreground.placement = terracedb::ExecutionDomainPlacement::SharedWeighted {
        weight: foreground_weight.max(1),
    };
    placement.background.placement = terracedb::ExecutionDomainPlacement::SharedWeighted {
        weight: background_weight.max(1),
    };
    placement.foreground = placement
        .foreground
        .clone()
        .with_budget(ExecutionDomainBudget {
            memory: terracedb::DomainMemoryBudget {
                mutable_bytes: Some(foreground_mutable_bytes),
                ..Default::default()
            },
            ..Default::default()
        });
    placement.background = placement
        .background
        .clone()
        .with_budget(ExecutionDomainBudget {
            memory: terracedb::DomainMemoryBudget {
                mutable_bytes: Some(background_mutable_bytes),
                ..Default::default()
            },
            background: terracedb::DomainBackgroundBudget {
                task_slots: Some(background_task_slots.max(1)),
                max_in_flight_bytes: Some(background_in_flight_bytes.max(1)),
            },
            ..Default::default()
        });
    placement.with_metadata("terracedb.execution.role", "primary")
}

fn analytics_helper_placement(
    foreground_weight: u32,
    background_weight: u32,
    foreground_mutable_bytes: u64,
    background_mutable_bytes: u64,
    background_task_slots: u32,
    background_in_flight_bytes: u64,
) -> ColocatedDatabasePlacement {
    let mut placement = ColocatedDatabasePlacement::analytics_helper(ANALYTICS_DATABASE_NAME);
    placement.foreground.placement = terracedb::ExecutionDomainPlacement::SharedWeighted {
        weight: foreground_weight.max(1),
    };
    placement.background.placement = terracedb::ExecutionDomainPlacement::SharedWeighted {
        weight: background_weight.max(1),
    };
    placement.foreground = placement
        .foreground
        .clone()
        .with_budget(ExecutionDomainBudget {
            memory: terracedb::DomainMemoryBudget {
                mutable_bytes: Some(foreground_mutable_bytes),
                ..Default::default()
            },
            ..Default::default()
        });
    placement.background = placement
        .background
        .clone()
        .with_budget(ExecutionDomainBudget {
            memory: terracedb::DomainMemoryBudget {
                mutable_bytes: Some(background_mutable_bytes),
                ..Default::default()
            },
            background: terracedb::DomainBackgroundBudget {
                task_slots: Some(background_task_slots.max(1)),
                max_in_flight_bytes: Some(background_in_flight_bytes.max(1)),
            },
            ..Default::default()
        });
    placement
}
