use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::scheduler::PendingWorkType;

/// Hierarchical name for an execution domain.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExecutionDomainPath {
    segments: Vec<String>,
}

impl ExecutionDomainPath {
    pub fn new<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let segments = segments
            .into_iter()
            .map(Into::into)
            .filter(|segment: &String| !segment.is_empty())
            .collect::<Vec<_>>();

        if segments.is_empty() {
            return Self::root("process");
        }

        Self { segments }
    }

    pub fn root(segment: impl Into<String>) -> Self {
        Self {
            segments: vec![segment.into()],
        }
    }

    pub fn child(&self, segment: impl Into<String>) -> Self {
        let mut segments = self.segments.clone();
        let segment = segment.into();
        if !segment.is_empty() {
            segments.push(segment);
        }
        Self { segments }
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn as_string(&self) -> String {
        self.segments.join("/")
    }

    pub fn is_root(&self) -> bool {
        self.segments.len() == 1
    }

    pub fn parent(&self) -> Option<Self> {
        (!self.is_root()).then(|| Self {
            segments: self.segments[..self.segments.len() - 1].to_vec(),
        })
    }

    pub fn lineage(&self) -> Vec<Self> {
        (1..=self.segments.len())
            .map(|len| Self {
                segments: self.segments[..len].to_vec(),
            })
            .collect()
    }

    pub fn is_same_or_descendant_of(&self, ancestor: &Self) -> bool {
        self.segments.starts_with(ancestor.segments())
    }
}

impl Default for ExecutionDomainPath {
    fn default() -> Self {
        Self::root("process")
    }
}

impl fmt::Display for ExecutionDomainPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_string())
    }
}

/// Logical owner that a domain or placement request belongs to.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExecutionDomainOwner {
    ProcessControl,
    Database {
        name: String,
    },
    Shard {
        database: String,
        shard: String,
    },
    Subsystem {
        database: Option<String>,
        name: String,
    },
    Custom {
        kind: String,
        name: String,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainCpuBudget {
    pub worker_slots: Option<u32>,
    pub weight: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainMemoryBudget {
    pub total_bytes: Option<u64>,
    pub cache_bytes: Option<u64>,
    pub mutable_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainIoBudget {
    pub local_concurrency: Option<u32>,
    pub local_bytes_per_second: Option<u64>,
    pub remote_concurrency: Option<u32>,
    pub remote_bytes_per_second: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainBackgroundBudget {
    pub task_slots: Option<u32>,
    pub max_in_flight_bytes: Option<u64>,
}

/// Per-domain resource budget contract.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionDomainBudget {
    pub cpu: DomainCpuBudget,
    pub memory: DomainMemoryBudget,
    pub io: DomainIoBudget,
    pub background: DomainBackgroundBudget,
}

/// Placement mode for a domain inside a process-wide resource manager.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionDomainPlacement {
    SharedWeighted { weight: u32 },
    Dedicated,
}

impl Default for ExecutionDomainPlacement {
    fn default() -> Self {
        Self::SharedWeighted { weight: 1 }
    }
}

/// Frozen execution-domain contract used by later placement backends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionDomainSpec {
    pub path: ExecutionDomainPath,
    pub owner: ExecutionDomainOwner,
    pub budget: ExecutionDomainBudget,
    pub placement: ExecutionDomainPlacement,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionDomainState {
    Registered,
    Active,
    Draining,
    Retired,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionResourceUsage {
    pub cpu_workers: u32,
    pub memory_bytes: u64,
    pub cache_bytes: u64,
    pub mutable_bytes: u64,
    pub local_io_concurrency: u32,
    pub local_io_bytes_per_second: u64,
    pub remote_io_concurrency: u32,
    pub remote_io_bytes_per_second: u64,
    pub background_tasks: u32,
    pub background_in_flight_bytes: u64,
}

impl ExecutionResourceUsage {
    fn metric(&self, kind: ExecutionResourceKind) -> u64 {
        match kind {
            ExecutionResourceKind::CpuWorkers => u64::from(self.cpu_workers),
            ExecutionResourceKind::MemoryBytes => self.memory_bytes,
            ExecutionResourceKind::CacheBytes => self.cache_bytes,
            ExecutionResourceKind::MutableBytes => self.mutable_bytes,
            ExecutionResourceKind::LocalIoConcurrency => u64::from(self.local_io_concurrency),
            ExecutionResourceKind::LocalIoBandwidth => self.local_io_bytes_per_second,
            ExecutionResourceKind::RemoteIoConcurrency => u64::from(self.remote_io_concurrency),
            ExecutionResourceKind::RemoteIoBandwidth => self.remote_io_bytes_per_second,
            ExecutionResourceKind::BackgroundTasks => u64::from(self.background_tasks),
            ExecutionResourceKind::BackgroundInFlightBytes => self.background_in_flight_bytes,
        }
    }

    fn saturating_add_assign(&mut self, other: ExecutionResourceUsage) {
        self.cpu_workers = self.cpu_workers.saturating_add(other.cpu_workers);
        self.memory_bytes = self.memory_bytes.saturating_add(other.memory_bytes);
        self.cache_bytes = self.cache_bytes.saturating_add(other.cache_bytes);
        self.mutable_bytes = self.mutable_bytes.saturating_add(other.mutable_bytes);
        self.local_io_concurrency = self
            .local_io_concurrency
            .saturating_add(other.local_io_concurrency);
        self.local_io_bytes_per_second = self
            .local_io_bytes_per_second
            .saturating_add(other.local_io_bytes_per_second);
        self.remote_io_concurrency = self
            .remote_io_concurrency
            .saturating_add(other.remote_io_concurrency);
        self.remote_io_bytes_per_second = self
            .remote_io_bytes_per_second
            .saturating_add(other.remote_io_bytes_per_second);
        self.background_tasks = self.background_tasks.saturating_add(other.background_tasks);
        self.background_in_flight_bytes = self
            .background_in_flight_bytes
            .saturating_add(other.background_in_flight_bytes);
    }

    fn saturating_sub_assign(&mut self, other: ExecutionResourceUsage) {
        self.cpu_workers = self.cpu_workers.saturating_sub(other.cpu_workers);
        self.memory_bytes = self.memory_bytes.saturating_sub(other.memory_bytes);
        self.cache_bytes = self.cache_bytes.saturating_sub(other.cache_bytes);
        self.mutable_bytes = self.mutable_bytes.saturating_sub(other.mutable_bytes);
        self.local_io_concurrency = self
            .local_io_concurrency
            .saturating_sub(other.local_io_concurrency);
        self.local_io_bytes_per_second = self
            .local_io_bytes_per_second
            .saturating_sub(other.local_io_bytes_per_second);
        self.remote_io_concurrency = self
            .remote_io_concurrency
            .saturating_sub(other.remote_io_concurrency);
        self.remote_io_bytes_per_second = self
            .remote_io_bytes_per_second
            .saturating_sub(other.remote_io_bytes_per_second);
        self.background_tasks = self.background_tasks.saturating_sub(other.background_tasks);
        self.background_in_flight_bytes = self
            .background_in_flight_bytes
            .saturating_sub(other.background_in_flight_bytes);
    }

    fn is_non_zero(&self) -> bool {
        Self::metric_is_non_zero(self.cpu_workers as u64)
            || Self::metric_is_non_zero(self.memory_bytes)
            || Self::metric_is_non_zero(self.cache_bytes)
            || Self::metric_is_non_zero(self.mutable_bytes)
            || Self::metric_is_non_zero(self.local_io_concurrency as u64)
            || Self::metric_is_non_zero(self.local_io_bytes_per_second)
            || Self::metric_is_non_zero(self.remote_io_concurrency as u64)
            || Self::metric_is_non_zero(self.remote_io_bytes_per_second)
            || Self::metric_is_non_zero(self.background_tasks as u64)
            || Self::metric_is_non_zero(self.background_in_flight_bytes)
    }

    fn metric_is_non_zero(value: u64) -> bool {
        value > 0
    }

    fn contains(&self, other: ExecutionResourceUsage) -> bool {
        self.cpu_workers >= other.cpu_workers
            && self.memory_bytes >= other.memory_bytes
            && self.cache_bytes >= other.cache_bytes
            && self.mutable_bytes >= other.mutable_bytes
            && self.local_io_concurrency >= other.local_io_concurrency
            && self.local_io_bytes_per_second >= other.local_io_bytes_per_second
            && self.remote_io_concurrency >= other.remote_io_concurrency
            && self.remote_io_bytes_per_second >= other.remote_io_bytes_per_second
            && self.background_tasks >= other.background_tasks
            && self.background_in_flight_bytes >= other.background_in_flight_bytes
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionDomainBacklogSnapshot {
    pub queued_work_items: u32,
    pub queued_bytes: u64,
}

impl ExecutionDomainBacklogSnapshot {
    fn saturating_add_assign(&mut self, other: ExecutionDomainBacklogSnapshot) {
        self.queued_work_items = self
            .queued_work_items
            .saturating_add(other.queued_work_items);
        self.queued_bytes = self.queued_bytes.saturating_add(other.queued_bytes);
    }

    pub fn is_empty(&self) -> bool {
        !self.is_non_zero()
    }

    fn is_non_zero(&self) -> bool {
        self.queued_work_items > 0 || self.queued_bytes > 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExecutionResourceKind {
    CpuWorkers,
    MemoryBytes,
    CacheBytes,
    MutableBytes,
    LocalIoConcurrency,
    LocalIoBandwidth,
    RemoteIoConcurrency,
    RemoteIoBandwidth,
    BackgroundTasks,
    BackgroundInFlightBytes,
}

impl ExecutionResourceKind {
    const ALL: [Self; 10] = [
        Self::CpuWorkers,
        Self::MemoryBytes,
        Self::CacheBytes,
        Self::MutableBytes,
        Self::LocalIoConcurrency,
        Self::LocalIoBandwidth,
        Self::RemoteIoConcurrency,
        Self::RemoteIoBandwidth,
        Self::BackgroundTasks,
        Self::BackgroundInFlightBytes,
    ];
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionDomainUsageSnapshot {
    pub cpu_workers_in_use: u32,
    pub memory_bytes: u64,
    pub cache_bytes: u64,
    pub mutable_bytes: u64,
    pub local_io_in_flight: u32,
    pub local_io_bytes_per_second: u64,
    pub remote_io_in_flight: u32,
    pub remote_io_bytes_per_second: u64,
    pub background_tasks: u32,
    pub background_in_flight_bytes: u64,
    pub queued_work_items: u32,
    pub queued_bytes: u64,
}

impl ExecutionDomainUsageSnapshot {
    fn from_usage_and_backlog(
        usage: ExecutionResourceUsage,
        backlog: ExecutionDomainBacklogSnapshot,
    ) -> Self {
        Self {
            cpu_workers_in_use: usage.cpu_workers,
            memory_bytes: usage.memory_bytes,
            cache_bytes: usage.cache_bytes,
            mutable_bytes: usage.mutable_bytes,
            local_io_in_flight: usage.local_io_concurrency,
            local_io_bytes_per_second: usage.local_io_bytes_per_second,
            remote_io_in_flight: usage.remote_io_concurrency,
            remote_io_bytes_per_second: usage.remote_io_bytes_per_second,
            background_tasks: usage.background_tasks,
            background_in_flight_bytes: usage.background_in_flight_bytes,
            queued_work_items: backlog.queued_work_items,
            queued_bytes: backlog.queued_bytes,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionDomainContentionSnapshot {
    pub blocked_requests: u64,
    pub contention_events: u64,
    pub last_blocked_by: Vec<ExecutionResourceKind>,
}

impl ExecutionDomainContentionSnapshot {
    fn record_blocked(&mut self, blocked_by: &[ExecutionResourceKind]) {
        if blocked_by.is_empty() {
            return;
        }
        self.blocked_requests = self.blocked_requests.saturating_add(1);
        self.contention_events = self.contention_events.saturating_add(1);
        self.last_blocked_by = blocked_by.to_vec();
    }

    fn saturating_add_assign(&mut self, other: &ExecutionDomainContentionSnapshot) {
        self.blocked_requests = self.blocked_requests.saturating_add(other.blocked_requests);
        self.contention_events = self
            .contention_events
            .saturating_add(other.contention_events);
        let mut reasons = self
            .last_blocked_by
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        reasons.extend(other.last_blocked_by.iter().copied());
        self.last_blocked_by = reasons.into_iter().collect();
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionDomainSnapshot {
    pub spec: ExecutionDomainSpec,
    pub parent: Option<ExecutionDomainPath>,
    pub state: ExecutionDomainState,
    pub effective_budget: ExecutionDomainBudget,
    pub usage: ExecutionDomainUsageSnapshot,
    pub backlog: ExecutionDomainBacklogSnapshot,
    pub contention: ExecutionDomainContentionSnapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionDomainLifecycleEvent {
    Registered,
    Activated,
    Updated,
    Draining,
    Retired,
}

pub trait ExecutionDomainLifecycleHook: Send + Sync {
    fn on_event(&self, snapshot: &ExecutionDomainSnapshot, event: ExecutionDomainLifecycleEvent);
}

/// Durability classes are distinct from execution placement.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DurabilityClass {
    UserData,
    ControlPlane,
    Deferred,
    RemotePrimary,
    Custom(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ExecutionLane {
    UserForeground,
    UserBackground,
    ControlPlane,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ContentionClass {
    UserData,
    ControlPlane,
    EmergencyMaintenance,
    Recovery,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionLaneBinding {
    pub domain: ExecutionDomainPath,
    pub durability_class: DurabilityClass,
}

impl ExecutionLaneBinding {
    pub fn new(domain: ExecutionDomainPath, durability_class: DurabilityClass) -> Self {
        Self {
            domain,
            durability_class,
        }
    }
}

pub fn execution_lane_name(lane: ExecutionLane) -> &'static str {
    match lane {
        ExecutionLane::UserForeground => "user-foreground",
        ExecutionLane::UserBackground => "user-background",
        ExecutionLane::ControlPlane => "control-plane",
    }
}

fn execution_lane_domain_segment(lane: ExecutionLane) -> &'static str {
    match lane {
        ExecutionLane::UserForeground => "foreground",
        ExecutionLane::UserBackground => "background",
        ExecutionLane::ControlPlane => "control",
    }
}

pub fn durability_class_metadata_value(durability_class: &DurabilityClass) -> String {
    match durability_class {
        DurabilityClass::UserData => "user-data".to_string(),
        DurabilityClass::ControlPlane => "control-plane".to_string(),
        DurabilityClass::Deferred => "deferred".to_string(),
        DurabilityClass::RemotePrimary => "remote-primary".to_string(),
        DurabilityClass::Custom(name) => format!("custom:{name}"),
    }
}

pub fn durability_class_from_metadata_value(value: &str) -> DurabilityClass {
    match value {
        "user-data" => DurabilityClass::UserData,
        "control-plane" => DurabilityClass::ControlPlane,
        "deferred" => DurabilityClass::Deferred,
        "remote-primary" => DurabilityClass::RemotePrimary,
        custom => {
            let name = custom.strip_prefix("custom:").unwrap_or(custom).to_string();
            DurabilityClass::Custom(name)
        }
    }
}

pub fn reserved_control_plane_budget() -> ExecutionDomainBudget {
    ExecutionDomainBudget {
        cpu: DomainCpuBudget {
            worker_slots: Some(1),
            weight: Some(1),
        },
        memory: DomainMemoryBudget {
            total_bytes: Some(64 * 1024),
            cache_bytes: None,
            mutable_bytes: Some(64 * 1024),
        },
        io: DomainIoBudget {
            local_concurrency: Some(1),
            local_bytes_per_second: Some(256 * 1024),
            remote_concurrency: Some(1),
            remote_bytes_per_second: Some(256 * 1024),
        },
        background: DomainBackgroundBudget {
            task_slots: Some(1),
            max_in_flight_bytes: Some(256 * 1024),
        },
    }
}

/// Default execution and durability bindings for one database runtime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbExecutionProfile {
    pub foreground: ExecutionLaneBinding,
    pub background: ExecutionLaneBinding,
    pub control_plane: ExecutionLaneBinding,
}

impl Default for DbExecutionProfile {
    fn default() -> Self {
        Self {
            foreground: ExecutionLaneBinding::new(
                ExecutionDomainPath::new(["process", "db", "foreground"]),
                DurabilityClass::UserData,
            ),
            background: ExecutionLaneBinding::new(
                ExecutionDomainPath::new(["process", "db", "background"]),
                DurabilityClass::UserData,
            ),
            control_plane: ExecutionLaneBinding::new(
                ExecutionDomainPath::new(["process", "control"]),
                DurabilityClass::ControlPlane,
            ),
        }
    }
}

impl DbExecutionProfile {
    pub fn with_foreground(mut self, binding: ExecutionLaneBinding) -> Self {
        self.foreground = binding;
        self
    }

    pub fn with_background(mut self, binding: ExecutionLaneBinding) -> Self {
        self.background = binding;
        self
    }

    pub fn with_control_plane(mut self, binding: ExecutionLaneBinding) -> Self {
        self.control_plane = binding;
        self
    }

    pub fn binding(&self, lane: ExecutionLane) -> &ExecutionLaneBinding {
        match lane {
            ExecutionLane::UserForeground => &self.foreground,
            ExecutionLane::UserBackground => &self.background,
            ExecutionLane::ControlPlane => &self.control_plane,
        }
    }

    pub fn work_request(
        &self,
        owner: ExecutionDomainOwner,
        lane: ExecutionLane,
        contention_class: ContentionClass,
    ) -> WorkPlacementRequest {
        let binding = self.binding(lane).clone();
        WorkPlacementRequest {
            owner,
            lane,
            contention_class,
            binding,
        }
    }

    pub fn pending_work_request(
        &self,
        owner: ExecutionDomainOwner,
        work_type: PendingWorkType,
    ) -> WorkPlacementRequest {
        match work_type {
            PendingWorkType::Backup => self.work_request(
                owner,
                ExecutionLane::ControlPlane,
                ContentionClass::ControlPlane,
            ),
            PendingWorkType::Flush
            | PendingWorkType::CurrentStateRetention
            | PendingWorkType::Compaction
            | PendingWorkType::Offload
            | PendingWorkType::Prefetch => self.work_request(
                owner,
                ExecutionLane::UserBackground,
                ContentionClass::UserData,
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkPlacementRequest {
    pub owner: ExecutionDomainOwner,
    pub lane: ExecutionLane,
    pub contention_class: ContentionClass,
    pub binding: ExecutionLaneBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkRuntimeTag {
    pub owner: ExecutionDomainOwner,
    pub lane: ExecutionLane,
    pub contention_class: ContentionClass,
    pub domain: ExecutionDomainPath,
    pub durability_class: DurabilityClass,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainTaggedWork<T> {
    pub work: T,
    pub tag: WorkRuntimeTag,
}

impl<T> DomainTaggedWork<T> {
    pub fn new(work: T, tag: WorkRuntimeTag) -> Self {
        Self { work, tag }
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> DomainTaggedWork<U> {
        DomainTaggedWork {
            work: f(self.work),
            tag: self.tag,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExecutionDomainInvariant {
    DomainMovementPreservesLogicalOutcome,
    OverloadPreservesIsolationBoundaries,
    EmergencyMaintenanceMustMakeProgress,
    ControlPlaneMustProgressUnderUserPressure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionDomainInvariantSet {
    pub required: Vec<ExecutionDomainInvariant>,
}

impl Default for ExecutionDomainInvariantSet {
    fn default() -> Self {
        Self {
            required: vec![
                ExecutionDomainInvariant::DomainMovementPreservesLogicalOutcome,
                ExecutionDomainInvariant::OverloadPreservesIsolationBoundaries,
                ExecutionDomainInvariant::EmergencyMaintenanceMustMakeProgress,
                ExecutionDomainInvariant::ControlPlaneMustProgressUnderUserPressure,
            ],
        }
    }
}

impl ExecutionDomainInvariantSet {
    pub fn contains(&self, invariant: ExecutionDomainInvariant) -> bool {
        self.required.contains(&invariant)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRequest {
    pub owner: ExecutionDomainOwner,
    pub lane: ExecutionLane,
    pub preferred_domain: ExecutionDomainPath,
    pub requested_budget: ExecutionDomainBudget,
    pub placement: ExecutionDomainPlacement,
    pub default_durability_class: DurabilityClass,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementAssignment {
    pub owner: ExecutionDomainOwner,
    pub lane: ExecutionLane,
    pub domain: ExecutionDomainPath,
    pub placement: ExecutionDomainPlacement,
    pub default_durability_class: DurabilityClass,
}

pub trait PlacementPolicy: Send + Sync {
    fn name(&self) -> &'static str;

    fn assign(
        &self,
        request: &PlacementRequest,
        snapshot: &ResourceManagerSnapshot,
    ) -> PlacementAssignment;
}

#[derive(Clone, Default)]
pub struct PreferRequestedDomainPolicy;

impl PlacementPolicy for PreferRequestedDomainPolicy {
    fn name(&self) -> &'static str {
        "prefer-requested-domain"
    }

    fn assign(
        &self,
        request: &PlacementRequest,
        _snapshot: &ResourceManagerSnapshot,
    ) -> PlacementAssignment {
        PlacementAssignment {
            owner: request.owner.clone(),
            lane: request.lane,
            domain: request.preferred_domain.clone(),
            placement: request.placement,
            default_durability_class: request.default_durability_class.clone(),
        }
    }
}

impl fmt::Debug for PreferRequestedDomainPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PreferRequestedDomainPolicy")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlacementTarget {
    pub owner: ExecutionDomainOwner,
    pub lane: ExecutionLane,
}

impl PlacementTarget {
    pub fn new(owner: ExecutionDomainOwner, lane: ExecutionLane) -> Self {
        Self { owner, lane }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionLanePlacementConfig {
    pub binding: ExecutionLaneBinding,
    pub budget: ExecutionDomainBudget,
    pub placement: ExecutionDomainPlacement,
    pub metadata: BTreeMap<String, String>,
}

impl ExecutionLanePlacementConfig {
    pub fn shared(domain: ExecutionDomainPath, durability_class: DurabilityClass) -> Self {
        Self::shared_weighted(domain, durability_class, 1)
    }

    pub fn shared_weighted(
        domain: ExecutionDomainPath,
        durability_class: DurabilityClass,
        weight: u32,
    ) -> Self {
        Self {
            binding: ExecutionLaneBinding::new(domain, durability_class),
            budget: ExecutionDomainBudget::default(),
            placement: ExecutionDomainPlacement::SharedWeighted {
                weight: weight.max(1),
            },
            metadata: BTreeMap::new(),
        }
    }

    pub fn reserved(
        domain: ExecutionDomainPath,
        durability_class: DurabilityClass,
        budget: ExecutionDomainBudget,
    ) -> Self {
        Self {
            binding: ExecutionLaneBinding::new(domain, durability_class),
            budget,
            placement: ExecutionDomainPlacement::Dedicated,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_budget(mut self, budget: ExecutionDomainBudget) -> Self {
        self.budget = budget;
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColocatedDatabasePlacement {
    pub name: String,
    pub foreground: ExecutionLanePlacementConfig,
    pub background: ExecutionLanePlacementConfig,
    pub control_plane: ExecutionLanePlacementConfig,
    pub metadata: BTreeMap<String, String>,
}

/// Canonical shard-ready namespace rules for one colocated database.
///
/// The current shard-ready profile still runs the database through one set of
/// database-wide foreground/background/control domains. The nested
/// `future_shard_namespace` is reserved so later physical shards can slot into
/// the hierarchy without changing how the database-level placement tree is
/// named.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReadyPlacementLayout {
    pub database: String,
    pub database_root: ExecutionDomainPath,
    pub future_shard_namespace: ExecutionDomainPath,
}

impl ShardReadyPlacementLayout {
    pub fn new(database: impl Into<String>) -> Self {
        let database = database.into();
        let database_root = ExecutionDomainPath::new(["process", "shards", database.as_str()]);
        let future_shard_namespace = database_root.child("shards");
        Self {
            database,
            database_root,
            future_shard_namespace,
        }
    }

    pub fn database_lane_path(&self, lane: ExecutionLane) -> ExecutionDomainPath {
        self.database_root
            .child(execution_lane_domain_segment(lane))
    }

    pub fn future_shard_root(&self, shard: impl AsRef<str>) -> ExecutionDomainPath {
        self.future_shard_namespace.child(shard.as_ref())
    }

    pub fn future_shard_lane_path(
        &self,
        shard: impl AsRef<str>,
        lane: ExecutionLane,
    ) -> ExecutionDomainPath {
        self.future_shard_root(shard)
            .child(execution_lane_domain_segment(lane))
    }

    pub fn database_owner(&self) -> ExecutionDomainOwner {
        ExecutionDomainOwner::Database {
            name: self.database.clone(),
        }
    }

    pub fn shard_owner(&self, shard: impl Into<String>) -> ExecutionDomainOwner {
        ExecutionDomainOwner::Shard {
            database: self.database.clone(),
            shard: shard.into(),
        }
    }
}

impl ColocatedDatabasePlacement {
    pub fn new(
        name: impl Into<String>,
        foreground: ExecutionLanePlacementConfig,
        background: ExecutionLanePlacementConfig,
        control_plane: ExecutionLanePlacementConfig,
    ) -> Self {
        Self {
            name: name.into(),
            foreground,
            background,
            control_plane,
            metadata: BTreeMap::new(),
        }
    }

    pub fn shared(name: impl Into<String>) -> Self {
        let name = name.into();
        let root = Self::database_root(&name);
        Self::new(
            name,
            ExecutionLanePlacementConfig::shared(
                root.child("foreground"),
                DurabilityClass::UserData,
            ),
            ExecutionLanePlacementConfig::shared(
                root.child("background"),
                DurabilityClass::UserData,
            ),
            ExecutionLanePlacementConfig::reserved(
                root.child("control"),
                DurabilityClass::ControlPlane,
                reserved_control_plane_budget(),
            ),
        )
    }

    pub fn analytics_helper(name: impl Into<String>) -> Self {
        let name = name.into();
        let mut placement = Self::shared(name);
        placement.foreground.placement = ExecutionDomainPlacement::SharedWeighted { weight: 1 };
        placement.background.placement = ExecutionDomainPlacement::SharedWeighted { weight: 1 };
        placement.metadata.insert(
            "terracedb.execution.role".to_string(),
            "analytics-helper".to_string(),
        );
        placement
    }

    pub fn shard_ready(name: impl Into<String>) -> Self {
        let layout = ShardReadyPlacementLayout::new(name.into());
        let mut placement = Self::new(
            layout.database.clone(),
            ExecutionLanePlacementConfig::shared(
                layout.database_lane_path(ExecutionLane::UserForeground),
                DurabilityClass::UserData,
            ),
            ExecutionLanePlacementConfig::shared(
                layout.database_lane_path(ExecutionLane::UserBackground),
                DurabilityClass::UserData,
            ),
            ExecutionLanePlacementConfig::reserved(
                layout.database_lane_path(ExecutionLane::ControlPlane),
                DurabilityClass::ControlPlane,
                reserved_control_plane_budget(),
            ),
        );
        placement.metadata.insert(
            "terracedb.execution.layout".to_string(),
            "shard-ready".to_string(),
        );
        placement.metadata.insert(
            "terracedb.execution.future_shard_namespace".to_string(),
            layout.future_shard_namespace.as_string(),
        );
        placement.metadata.insert(
            "terracedb.execution.physical_sharding".to_string(),
            "not-enabled".to_string(),
        );
        placement
    }

    pub fn execution_profile(&self) -> DbExecutionProfile {
        DbExecutionProfile {
            foreground: self.foreground.binding.clone(),
            background: self.background.binding.clone(),
            control_plane: self.control_plane.binding.clone(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn database_root(name: &str) -> ExecutionDomainPath {
        ExecutionDomainPath::new(["process", "dbs", name])
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColocatedSubsystemPlacement {
    pub database: Option<String>,
    pub name: String,
    pub lane: ExecutionLane,
    pub placement: ExecutionLanePlacementConfig,
}

impl ColocatedSubsystemPlacement {
    pub fn database_local(
        database: impl Into<String>,
        name: impl Into<String>,
        lane: ExecutionLane,
        placement: ExecutionLanePlacementConfig,
    ) -> Self {
        Self {
            database: Some(database.into()),
            name: name.into(),
            lane,
            placement,
        }
    }

    pub fn process_local(
        name: impl Into<String>,
        lane: ExecutionLane,
        placement: ExecutionLanePlacementConfig,
    ) -> Self {
        Self {
            database: None,
            name: name.into(),
            lane,
            placement,
        }
    }

    pub fn target(&self) -> PlacementTarget {
        PlacementTarget::new(
            ExecutionDomainOwner::Subsystem {
                database: self.database.clone(),
                name: self.name.clone(),
            },
            self.lane,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlacementDecision {
    pub owner: ExecutionDomainOwner,
    pub lane: ExecutionLane,
    pub binding: ExecutionLaneBinding,
    pub snapshot: Option<ExecutionDomainSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbExecutionPlacementReport {
    pub database: String,
    pub placement_policy_name: String,
    pub foreground: ExecutionPlacementDecision,
    pub background: ExecutionPlacementDecision,
    pub control_plane: ExecutionPlacementDecision,
    pub attached_subsystems: BTreeMap<String, ExecutionDomainSnapshot>,
    pub domain_topology: BTreeMap<ExecutionDomainPath, ExecutionDomainSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColocatedDeploymentReport {
    pub placement_policy_name: String,
    pub process_budget: ExecutionDomainBudget,
    pub databases: BTreeMap<String, DbExecutionPlacementReport>,
    pub process_subsystems: BTreeMap<String, ExecutionDomainSnapshot>,
    pub domain_topology: BTreeMap<ExecutionDomainPath, ExecutionDomainSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColocatedDeploymentError {
    DuplicateDatabase {
        name: String,
    },
    DuplicateSubsystem {
        name: String,
        database: Option<String>,
    },
    EmptyDatabaseName,
}

impl fmt::Display for ColocatedDeploymentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateDatabase { name } => {
                write!(f, "database placement '{name}' was declared more than once")
            }
            Self::DuplicateSubsystem { name, database } => match database {
                Some(database) => write!(
                    f,
                    "subsystem placement '{database}/{name}' was declared more than once"
                ),
                None => write!(
                    f,
                    "process subsystem placement '{name}' was declared more than once"
                ),
            },
            Self::EmptyDatabaseName => f.write_str("database placement names cannot be empty"),
        }
    }
}

impl std::error::Error for ColocatedDeploymentError {}

#[derive(Clone)]
pub struct ColocatedDeploymentPlacementPolicy {
    assignments: BTreeMap<PlacementTarget, PlacementAssignment>,
}

impl ColocatedDeploymentPlacementPolicy {
    fn new(assignments: BTreeMap<PlacementTarget, PlacementAssignment>) -> Self {
        Self { assignments }
    }
}

impl PlacementPolicy for ColocatedDeploymentPlacementPolicy {
    fn name(&self) -> &'static str {
        "colocated-deployment"
    }

    fn assign(
        &self,
        request: &PlacementRequest,
        _snapshot: &ResourceManagerSnapshot,
    ) -> PlacementAssignment {
        self.assignments
            .get(&PlacementTarget::new(request.owner.clone(), request.lane))
            .cloned()
            .unwrap_or_else(|| PlacementAssignment {
                owner: request.owner.clone(),
                lane: request.lane,
                domain: request.preferred_domain.clone(),
                placement: request.placement,
                default_durability_class: request.default_durability_class.clone(),
            })
    }
}

impl fmt::Debug for ColocatedDeploymentPlacementPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ColocatedDeploymentPlacementPolicy")
    }
}

#[derive(Clone, Debug)]
pub struct ColocatedDeploymentBuilder {
    process_budget: ExecutionDomainBudget,
    databases: BTreeMap<String, ColocatedDatabasePlacement>,
    subsystems: BTreeMap<PlacementTarget, ColocatedSubsystemPlacement>,
}

impl ColocatedDeploymentBuilder {
    pub fn new(process_budget: ExecutionDomainBudget) -> Self {
        Self {
            process_budget,
            databases: BTreeMap::new(),
            subsystems: BTreeMap::new(),
        }
    }

    pub fn with_database(
        mut self,
        placement: ColocatedDatabasePlacement,
    ) -> Result<Self, ColocatedDeploymentError> {
        if placement.name.is_empty() {
            return Err(ColocatedDeploymentError::EmptyDatabaseName);
        }
        let duplicate_name = placement.name.clone();
        if self
            .databases
            .insert(duplicate_name.clone(), placement)
            .is_some()
        {
            return Err(ColocatedDeploymentError::DuplicateDatabase {
                name: duplicate_name,
            });
        }
        Ok(self)
    }

    pub fn with_subsystem(
        mut self,
        placement: ColocatedSubsystemPlacement,
    ) -> Result<Self, ColocatedDeploymentError> {
        let target = placement.target();
        if self.subsystems.insert(target.clone(), placement).is_some() {
            return Err(ColocatedDeploymentError::DuplicateSubsystem {
                name: match target.owner {
                    ExecutionDomainOwner::Subsystem { ref name, .. } => name.clone(),
                    _ => unreachable!("subsystem targets always use subsystem owners"),
                },
                database: match target.owner {
                    ExecutionDomainOwner::Subsystem { ref database, .. } => database.clone(),
                    _ => unreachable!("subsystem targets always use subsystem owners"),
                },
            });
        }
        Ok(self)
    }

    pub fn build(self) -> ColocatedDeployment {
        ColocatedDeployment::from_builder(self)
    }
}

#[derive(Clone)]
pub struct ColocatedDeployment {
    resource_manager: Arc<dyn ResourceManager>,
    databases: BTreeMap<String, ColocatedDatabasePlacement>,
    subsystems: BTreeMap<PlacementTarget, ColocatedSubsystemPlacement>,
}

impl ColocatedDeployment {
    pub fn builder(process_budget: ExecutionDomainBudget) -> ColocatedDeploymentBuilder {
        ColocatedDeploymentBuilder::new(process_budget)
    }

    pub fn single_database(
        process_budget: ExecutionDomainBudget,
        name: impl Into<String>,
    ) -> Result<Self, ColocatedDeploymentError> {
        Ok(Self::builder(process_budget)
            .with_database(ColocatedDatabasePlacement::shared(name))?
            .build())
    }

    pub fn two_databases(
        process_budget: ExecutionDomainBudget,
        left: impl Into<String>,
        right: impl Into<String>,
    ) -> Result<Self, ColocatedDeploymentError> {
        Ok(Self::builder(process_budget)
            .with_database(ColocatedDatabasePlacement::shared(left))?
            .with_database(ColocatedDatabasePlacement::shared(right))?
            .build())
    }

    pub fn primary_with_analytics(
        process_budget: ExecutionDomainBudget,
        primary: impl Into<String>,
        analytics: impl Into<String>,
    ) -> Result<Self, ColocatedDeploymentError> {
        let primary_name = primary.into();
        let analytics_name = analytics.into();
        let mut primary = ColocatedDatabasePlacement::shared(primary_name.clone());
        primary.foreground.placement = ExecutionDomainPlacement::SharedWeighted { weight: 3 };
        primary.background.placement = ExecutionDomainPlacement::SharedWeighted { weight: 2 };
        primary = primary.with_metadata("terracedb.execution.role", "primary");

        let analytics = ColocatedDatabasePlacement::analytics_helper(analytics_name);

        Ok(Self::builder(process_budget)
            .with_database(primary)?
            .with_database(analytics)?
            .build())
    }

    pub fn shard_ready(
        process_budget: ExecutionDomainBudget,
        name: impl Into<String>,
    ) -> Result<Self, ColocatedDeploymentError> {
        Ok(Self::builder(process_budget)
            .with_database(ColocatedDatabasePlacement::shard_ready(name))?
            .build())
    }

    fn from_builder(builder: ColocatedDeploymentBuilder) -> Self {
        let assignments = build_colocated_assignments(&builder.databases, &builder.subsystems);
        let resource_manager: Arc<dyn ResourceManager> = Arc::new(
            InMemoryResourceManager::new(builder.process_budget).with_placement_policy(Arc::new(
                ColocatedDeploymentPlacementPolicy::new(assignments),
            )),
        );
        register_colocated_domains(&resource_manager, &builder.databases, &builder.subsystems);
        Self {
            resource_manager,
            databases: builder.databases,
            subsystems: builder.subsystems,
        }
    }

    pub fn resource_manager(&self) -> Arc<dyn ResourceManager> {
        self.resource_manager.clone()
    }

    pub fn database_names(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }

    pub fn execution_profile(&self, database: &str) -> Option<DbExecutionProfile> {
        self.databases
            .get(database)
            .map(ColocatedDatabasePlacement::execution_profile)
    }

    pub fn builder_for_database(
        &self,
        database: impl Into<String>,
        settings: crate::DbSettings,
    ) -> Result<crate::DbBuilder, crate::OpenError> {
        crate::Db::builder()
            .settings(settings)
            .colocated_database(self, database)
    }

    pub async fn open_database(
        &self,
        database: impl Into<String>,
        settings: crate::DbSettings,
        components: crate::DbComponents,
    ) -> Result<crate::Db, crate::OpenError> {
        self.builder_for_database(database, settings)?
            .components(components)
            .open()
            .await
    }

    pub fn runtime_report(&self) -> ColocatedDeploymentReport {
        let snapshot = self.resource_manager.snapshot();
        ColocatedDeploymentReport {
            placement_policy_name: snapshot.placement_policy_name.clone(),
            process_budget: snapshot.process_budget,
            databases: self
                .databases
                .iter()
                .map(|(database, placement)| {
                    (
                        database.clone(),
                        build_db_execution_placement_report(
                            &snapshot,
                            database,
                            &placement.execution_profile(),
                        ),
                    )
                })
                .collect(),
            process_subsystems: snapshot
                .domains
                .iter()
                .filter_map(|(path, snapshot)| match &snapshot.spec.owner {
                    ExecutionDomainOwner::Subsystem {
                        database: None,
                        name,
                    } => Some((format!("{name}:{}", path.as_string()), snapshot.clone())),
                    _ => None,
                })
                .collect(),
            domain_topology: snapshot.domains,
        }
    }

    pub fn report(&self) -> ColocatedDeploymentReport {
        self.runtime_report()
    }
}

impl fmt::Debug for ColocatedDeployment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ColocatedDeployment")
            .field("resource_manager", &"<dyn ResourceManager>")
            .field("databases", &self.databases.keys().collect::<Vec<_>>())
            .field("subsystems", &self.subsystems.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn build_colocated_assignments(
    databases: &BTreeMap<String, ColocatedDatabasePlacement>,
    subsystems: &BTreeMap<PlacementTarget, ColocatedSubsystemPlacement>,
) -> BTreeMap<PlacementTarget, PlacementAssignment> {
    let mut assignments = BTreeMap::new();

    for (database, placement) in databases {
        for (lane, lane_placement) in [
            (ExecutionLane::UserForeground, &placement.foreground),
            (ExecutionLane::UserBackground, &placement.background),
        ] {
            let owner = ExecutionDomainOwner::Database {
                name: database.clone(),
            };
            assignments.insert(
                PlacementTarget::new(owner.clone(), lane),
                PlacementAssignment {
                    owner,
                    lane,
                    domain: lane_placement.binding.domain.clone(),
                    placement: lane_placement.placement,
                    default_durability_class: lane_placement.binding.durability_class.clone(),
                },
            );
        }

        let owner = ExecutionDomainOwner::Subsystem {
            database: Some(database.clone()),
            name: "control-plane".to_string(),
        };
        assignments.insert(
            PlacementTarget::new(owner.clone(), ExecutionLane::ControlPlane),
            PlacementAssignment {
                owner,
                lane: ExecutionLane::ControlPlane,
                domain: placement.control_plane.binding.domain.clone(),
                placement: placement.control_plane.placement,
                default_durability_class: placement.control_plane.binding.durability_class.clone(),
            },
        );
    }

    for (target, placement) in subsystems {
        assignments.insert(
            target.clone(),
            PlacementAssignment {
                owner: target.owner.clone(),
                lane: target.lane,
                domain: placement.placement.binding.domain.clone(),
                placement: placement.placement.placement,
                default_durability_class: placement.placement.binding.durability_class.clone(),
            },
        );
    }

    assignments
}

fn register_colocated_domains(
    resource_manager: &Arc<dyn ResourceManager>,
    databases: &BTreeMap<String, ColocatedDatabasePlacement>,
    subsystems: &BTreeMap<PlacementTarget, ColocatedSubsystemPlacement>,
) {
    for (database, placement) in databases {
        for (lane, owner, lane_placement) in [
            (
                ExecutionLane::UserForeground,
                ExecutionDomainOwner::Database {
                    name: database.clone(),
                },
                &placement.foreground,
            ),
            (
                ExecutionLane::UserBackground,
                ExecutionDomainOwner::Database {
                    name: database.clone(),
                },
                &placement.background,
            ),
            (
                ExecutionLane::ControlPlane,
                ExecutionDomainOwner::Subsystem {
                    database: Some(database.clone()),
                    name: "control-plane".to_string(),
                },
                &placement.control_plane,
            ),
        ] {
            let mut metadata = placement.metadata.clone();
            metadata.extend(lane_placement.metadata.clone());
            metadata.insert(
                "terracedb.execution.lane".to_string(),
                execution_lane_name(lane).to_string(),
            );
            metadata.insert("terracedb.execution.database".to_string(), database.clone());
            metadata.insert(
                "terracedb.execution.durability".to_string(),
                durability_class_metadata_value(&lane_placement.binding.durability_class),
            );
            if lane == ExecutionLane::ControlPlane {
                metadata.insert("reserved".to_string(), "true".to_string());
            }
            resource_manager.register_domain(ExecutionDomainSpec {
                path: lane_placement.binding.domain.clone(),
                owner,
                budget: lane_placement.budget,
                placement: lane_placement.placement,
                metadata,
            });
        }
    }

    for placement in subsystems.values() {
        let mut metadata = placement.placement.metadata.clone();
        metadata.insert(
            "terracedb.execution.lane".to_string(),
            execution_lane_name(placement.lane).to_string(),
        );
        metadata.insert(
            "terracedb.execution.durability".to_string(),
            durability_class_metadata_value(&placement.placement.binding.durability_class),
        );
        if let Some(database) = &placement.database {
            metadata.insert("terracedb.execution.database".to_string(), database.clone());
        }
        metadata.insert(
            "terracedb.execution.subsystem".to_string(),
            placement.name.clone(),
        );
        resource_manager.register_domain(ExecutionDomainSpec {
            path: placement.placement.binding.domain.clone(),
            owner: ExecutionDomainOwner::Subsystem {
                database: placement.database.clone(),
                name: placement.name.clone(),
            },
            budget: placement.placement.budget,
            placement: placement.placement.placement,
            metadata,
        });
    }
}

pub fn build_db_execution_placement_report(
    snapshot: &ResourceManagerSnapshot,
    database: &str,
    execution_profile: &DbExecutionProfile,
) -> DbExecutionPlacementReport {
    let foreground = build_execution_placement_decision(
        snapshot,
        ExecutionDomainOwner::Database {
            name: database.to_string(),
        },
        ExecutionLane::UserForeground,
        execution_profile.foreground.clone(),
    );
    let background = build_execution_placement_decision(
        snapshot,
        ExecutionDomainOwner::Database {
            name: database.to_string(),
        },
        ExecutionLane::UserBackground,
        execution_profile.background.clone(),
    );
    let control_plane = build_execution_placement_decision(
        snapshot,
        ExecutionDomainOwner::Subsystem {
            database: Some(database.to_string()),
            name: "control-plane".to_string(),
        },
        ExecutionLane::ControlPlane,
        execution_profile.control_plane.clone(),
    );

    let relevant_roots = [
        foreground.binding.domain.clone(),
        background.binding.domain.clone(),
        control_plane.binding.domain.clone(),
    ];
    let attached_subsystem_entries = snapshot
        .domains
        .iter()
        .filter_map(
            |(path, domain_snapshot)| match &domain_snapshot.spec.owner {
                ExecutionDomainOwner::Subsystem {
                    database: Some(owner_database),
                    name,
                } if owner_database == database && name != "control-plane" => Some((
                    path.clone(),
                    format!("{name}@{}", path.as_string()),
                    domain_snapshot.clone(),
                )),
                _ => None,
            },
        )
        .collect::<Vec<_>>();
    let attached_subsystems = attached_subsystem_entries
        .iter()
        .map(|(_, key, snapshot)| (key.clone(), snapshot.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut included_paths = BTreeSet::new();
    for root in &relevant_roots {
        included_paths.extend(root.lineage());
    }
    for (path, _, _) in &attached_subsystem_entries {
        included_paths.extend(path.lineage());
    }

    let domain_topology = snapshot
        .domains
        .iter()
        .filter(|(path, _)| {
            included_paths.contains(*path)
                || relevant_roots
                    .iter()
                    .any(|root| path.is_same_or_descendant_of(root))
                || attached_subsystem_entries
                    .iter()
                    .any(|(root, _, _)| path.is_same_or_descendant_of(root))
        })
        .map(|(path, snapshot)| (path.clone(), snapshot.clone()))
        .collect();

    DbExecutionPlacementReport {
        database: database.to_string(),
        placement_policy_name: snapshot.placement_policy_name.clone(),
        foreground,
        background,
        control_plane,
        attached_subsystems,
        domain_topology,
    }
}

fn build_execution_placement_decision(
    snapshot: &ResourceManagerSnapshot,
    owner: ExecutionDomainOwner,
    lane: ExecutionLane,
    binding: ExecutionLaneBinding,
) -> ExecutionPlacementDecision {
    ExecutionPlacementDecision {
        owner,
        lane,
        snapshot: snapshot.domains.get(&binding.domain).cloned(),
        binding,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceManagerSnapshot {
    pub process_budget: ExecutionDomainBudget,
    pub placement_policy_name: String,
    pub invariants: ExecutionDomainInvariantSet,
    pub domains: BTreeMap<ExecutionDomainPath, ExecutionDomainSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceAdmissionDecision {
    pub admitted: bool,
    pub borrowed_shared_capacity: bool,
    pub blocked_by: Vec<ExecutionResourceKind>,
    pub effective_budget: ExecutionDomainBudget,
    pub snapshot: ExecutionDomainSnapshot,
}

#[must_use = "execution usage is released when the lease is dropped"]
pub struct ExecutionUsageLease {
    manager: Arc<dyn ResourceManager>,
    path: ExecutionDomainPath,
    usage: ExecutionResourceUsage,
    decision: ResourceAdmissionDecision,
    released: bool,
}

impl ExecutionUsageLease {
    pub fn acquire(
        manager: Arc<dyn ResourceManager>,
        path: ExecutionDomainPath,
        usage: ExecutionResourceUsage,
    ) -> Self {
        let decision = manager.try_acquire(&path, usage);
        Self {
            manager,
            path,
            usage,
            decision,
            released: false,
        }
    }

    pub fn admitted(&self) -> bool {
        self.decision.admitted
    }

    pub fn decision(&self) -> &ResourceAdmissionDecision {
        &self.decision
    }

    pub fn path(&self) -> &ExecutionDomainPath {
        &self.path
    }

    pub fn usage(&self) -> ExecutionResourceUsage {
        self.usage
    }

    pub fn release(mut self) -> Option<ExecutionDomainSnapshot> {
        self.release_inner()
    }

    fn release_inner(&mut self) -> Option<ExecutionDomainSnapshot> {
        if !self.decision.admitted || self.released {
            return None;
        }
        self.released = true;
        Some(self.manager.release(&self.path, self.usage))
    }
}

impl Drop for ExecutionUsageLease {
    fn drop(&mut self) {
        let _ = self.release_inner();
    }
}

impl fmt::Debug for ExecutionUsageLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionUsageLease")
            .field("path", &self.path)
            .field("usage", &self.usage)
            .field("decision", &self.decision)
            .field("released", &self.released)
            .finish()
    }
}

#[must_use = "backlog is restored when the guard is dropped"]
pub struct ExecutionBacklogGuard {
    manager: Arc<dyn ResourceManager>,
    path: ExecutionDomainPath,
    previous: ExecutionDomainBacklogSnapshot,
    current: ExecutionDomainBacklogSnapshot,
    restored: bool,
}

impl ExecutionBacklogGuard {
    pub fn set(
        manager: Arc<dyn ResourceManager>,
        path: ExecutionDomainPath,
        backlog: ExecutionDomainBacklogSnapshot,
    ) -> Self {
        let previous = manager.direct_backlog(&path);
        manager.set_backlog(&path, backlog);
        Self {
            manager,
            path,
            previous,
            current: backlog,
            restored: false,
        }
    }

    pub fn previous(&self) -> ExecutionDomainBacklogSnapshot {
        self.previous
    }

    pub fn current(&self) -> ExecutionDomainBacklogSnapshot {
        self.current
    }

    pub fn restore(mut self) -> ExecutionDomainSnapshot {
        self.restore_inner()
    }

    fn restore_inner(&mut self) -> ExecutionDomainSnapshot {
        if self.restored {
            return self.manager.set_backlog(&self.path, self.previous);
        }
        self.restored = true;
        self.manager.set_backlog(&self.path, self.previous)
    }
}

impl Drop for ExecutionBacklogGuard {
    fn drop(&mut self) {
        if !self.restored {
            self.restored = true;
            self.manager.set_backlog(&self.path, self.previous);
        }
    }
}

impl fmt::Debug for ExecutionBacklogGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionBacklogGuard")
            .field("path", &self.path)
            .field("previous", &self.previous)
            .field("current", &self.current)
            .field("restored", &self.restored)
            .finish()
    }
}

pub trait ResourceManager: Send + Sync {
    fn process_budget(&self) -> ExecutionDomainBudget;
    fn placement_policy(&self) -> Arc<dyn PlacementPolicy>;
    fn register_domain(&self, spec: ExecutionDomainSpec) -> ExecutionDomainSnapshot;
    fn update_domain(&self, spec: ExecutionDomainSpec) -> ExecutionDomainSnapshot;
    fn retire_domain(&self, path: &ExecutionDomainPath);
    fn register_lifecycle_hook(&self, hook: Arc<dyn ExecutionDomainLifecycleHook>);
    fn snapshot(&self) -> ResourceManagerSnapshot;
    fn assign(&self, request: PlacementRequest) -> PlacementAssignment;
    fn placement_tag(&self, request: WorkPlacementRequest) -> WorkRuntimeTag;
    fn try_acquire(
        &self,
        path: &ExecutionDomainPath,
        usage: ExecutionResourceUsage,
    ) -> ResourceAdmissionDecision;
    fn release(
        &self,
        path: &ExecutionDomainPath,
        usage: ExecutionResourceUsage,
    ) -> ExecutionDomainSnapshot;
    fn direct_backlog(&self, path: &ExecutionDomainPath) -> ExecutionDomainBacklogSnapshot;
    fn set_backlog(
        &self,
        path: &ExecutionDomainPath,
        backlog: ExecutionDomainBacklogSnapshot,
    ) -> ExecutionDomainSnapshot;
}

#[derive(Clone)]
struct DomainRuntimeRecord {
    spec: ExecutionDomainSpec,
    state: ExecutionDomainState,
    direct_usage: ExecutionResourceUsage,
    backlog: ExecutionDomainBacklogSnapshot,
    contention: ExecutionDomainContentionSnapshot,
    explicit: bool,
}

#[derive(Clone, Copy)]
struct MetricLimit {
    limit: u64,
    strict_share: u64,
}

/// Deterministic in-memory manager used until a runtime-backed process-wide
/// scheduler lands.
pub struct InMemoryResourceManager {
    process_budget: ExecutionDomainBudget,
    invariants: ExecutionDomainInvariantSet,
    placement_policy: Arc<dyn PlacementPolicy>,
    domains: Mutex<BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>>,
    hooks: Mutex<Vec<Arc<dyn ExecutionDomainLifecycleHook>>>,
}

impl Default for InMemoryResourceManager {
    fn default() -> Self {
        Self::new(ExecutionDomainBudget::default())
    }
}

impl InMemoryResourceManager {
    pub fn new(process_budget: ExecutionDomainBudget) -> Self {
        Self {
            process_budget,
            invariants: ExecutionDomainInvariantSet::default(),
            placement_policy: Arc::new(PreferRequestedDomainPolicy),
            domains: Mutex::new(BTreeMap::new()),
            hooks: Mutex::new(Vec::new()),
        }
    }

    pub fn with_placement_policy(mut self, placement_policy: Arc<dyn PlacementPolicy>) -> Self {
        self.placement_policy = placement_policy;
        self
    }

    pub fn with_invariants(mut self, invariants: ExecutionDomainInvariantSet) -> Self {
        self.invariants = invariants;
        self
    }

    fn synthetic_spec(path: &ExecutionDomainPath) -> ExecutionDomainSpec {
        ExecutionDomainSpec {
            path: path.clone(),
            owner: if path.is_root() {
                ExecutionDomainOwner::ProcessControl
            } else {
                ExecutionDomainOwner::Custom {
                    kind: "hierarchy".to_string(),
                    name: path.as_string(),
                }
            },
            budget: ExecutionDomainBudget::default(),
            placement: ExecutionDomainPlacement::default(),
            metadata: BTreeMap::from([("terracedb.synthetic".to_string(), "true".to_string())]),
        }
    }

    fn ensure_record(
        domains: &mut BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
    ) {
        for ancestor in path.lineage() {
            domains
                .entry(ancestor.clone())
                .or_insert_with(|| DomainRuntimeRecord {
                    spec: Self::synthetic_spec(&ancestor),
                    state: ExecutionDomainState::Active,
                    direct_usage: ExecutionResourceUsage::default(),
                    backlog: ExecutionDomainBacklogSnapshot::default(),
                    contention: ExecutionDomainContentionSnapshot::default(),
                    explicit: false,
                });
        }
    }

    fn merge_budget(
        left: ExecutionDomainBudget,
        right: ExecutionDomainBudget,
    ) -> ExecutionDomainBudget {
        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: merge_optional_u32(left.cpu.worker_slots, right.cpu.worker_slots),
                weight: merge_optional_weight(left.cpu.weight, right.cpu.weight),
            },
            memory: DomainMemoryBudget {
                total_bytes: merge_optional_u64(left.memory.total_bytes, right.memory.total_bytes),
                cache_bytes: merge_optional_u64(left.memory.cache_bytes, right.memory.cache_bytes),
                mutable_bytes: merge_optional_u64(
                    left.memory.mutable_bytes,
                    right.memory.mutable_bytes,
                ),
            },
            io: DomainIoBudget {
                local_concurrency: merge_optional_u32(
                    left.io.local_concurrency,
                    right.io.local_concurrency,
                ),
                local_bytes_per_second: merge_optional_u64(
                    left.io.local_bytes_per_second,
                    right.io.local_bytes_per_second,
                ),
                remote_concurrency: merge_optional_u32(
                    left.io.remote_concurrency,
                    right.io.remote_concurrency,
                ),
                remote_bytes_per_second: merge_optional_u64(
                    left.io.remote_bytes_per_second,
                    right.io.remote_bytes_per_second,
                ),
            },
            background: DomainBackgroundBudget {
                task_slots: merge_optional_u32(
                    left.background.task_slots,
                    right.background.task_slots,
                ),
                max_in_flight_bytes: merge_optional_u64(
                    left.background.max_in_flight_bytes,
                    right.background.max_in_flight_bytes,
                ),
            },
        }
    }

    fn merge_spec(left: ExecutionDomainSpec, right: ExecutionDomainSpec) -> ExecutionDomainSpec {
        let owner = if left.owner == right.owner {
            left.owner.clone()
        } else {
            ExecutionDomainOwner::Custom {
                kind: "shared-domain".to_string(),
                name: left.path.as_string(),
            }
        };
        let placement = match (left.placement, right.placement) {
            (ExecutionDomainPlacement::Dedicated, _) | (_, ExecutionDomainPlacement::Dedicated) => {
                ExecutionDomainPlacement::Dedicated
            }
            (
                ExecutionDomainPlacement::SharedWeighted {
                    weight: left_weight,
                },
                ExecutionDomainPlacement::SharedWeighted {
                    weight: right_weight,
                },
            ) => ExecutionDomainPlacement::SharedWeighted {
                weight: left_weight.max(right_weight),
            },
        };
        let mut metadata = left.metadata.clone();
        metadata.extend(right.metadata);
        ExecutionDomainSpec {
            path: left.path,
            owner,
            budget: Self::merge_budget(left.budget, right.budget),
            placement,
            metadata,
        }
    }

    fn notify(&self, snapshot: &ExecutionDomainSnapshot, event: ExecutionDomainLifecycleEvent) {
        let hooks = self.hooks.lock().clone();
        for hook in hooks {
            hook.on_event(snapshot, event);
        }
    }

    fn aggregate_usage(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
    ) -> ExecutionResourceUsage {
        let mut usage = ExecutionResourceUsage::default();
        for (candidate, record) in domains {
            if candidate.is_same_or_descendant_of(path) {
                usage.saturating_add_assign(record.direct_usage);
            }
        }
        usage
    }

    fn aggregate_metric(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
        kind: ExecutionResourceKind,
    ) -> u64 {
        domains
            .iter()
            .filter(|(candidate, _)| candidate.is_same_or_descendant_of(path))
            .fold(0_u64, |total, (_, record)| {
                total.saturating_add(record.direct_usage.metric(kind))
            })
    }

    fn aggregate_backlog(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
    ) -> ExecutionDomainBacklogSnapshot {
        let mut backlog = ExecutionDomainBacklogSnapshot::default();
        for (candidate, record) in domains {
            if candidate.is_same_or_descendant_of(path) {
                backlog.saturating_add_assign(record.backlog);
            }
        }
        backlog
    }

    fn aggregate_contention(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
    ) -> ExecutionDomainContentionSnapshot {
        let mut contention = ExecutionDomainContentionSnapshot::default();
        for (candidate, record) in domains {
            if candidate.is_same_or_descendant_of(path) {
                contention.saturating_add_assign(&record.contention);
            }
        }
        contention
    }

    fn direct_children(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        parent: &ExecutionDomainPath,
    ) -> Vec<ExecutionDomainPath> {
        domains
            .keys()
            .filter(|path| path.parent().as_ref() == Some(parent))
            .cloned()
            .collect()
    }

    fn is_branch_busy(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        branch: &ExecutionDomainPath,
        active_path: &ExecutionDomainPath,
    ) -> bool {
        active_path.is_same_or_descendant_of(branch)
            || self.aggregate_usage(domains, branch).is_non_zero()
            || self.aggregate_backlog(domains, branch).is_non_zero()
    }

    fn metric_weight_for_path(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
        kind: ExecutionResourceKind,
    ) -> u32 {
        let record = domains
            .get(path)
            .expect("execution domain must exist before computing weight");
        if kind == ExecutionResourceKind::CpuWorkers
            && let Some(weight) = record.spec.budget.cpu.weight
        {
            return weight.max(1);
        }
        match record.spec.placement {
            ExecutionDomainPlacement::SharedWeighted { weight } => weight.max(1),
            ExecutionDomainPlacement::Dedicated => 1,
        }
    }

    fn metric_budget_limit(
        budget: ExecutionDomainBudget,
        kind: ExecutionResourceKind,
    ) -> Option<u64> {
        match kind {
            ExecutionResourceKind::CpuWorkers => budget.cpu.worker_slots.map(u64::from),
            ExecutionResourceKind::MemoryBytes => budget.memory.total_bytes,
            ExecutionResourceKind::CacheBytes => budget.memory.cache_bytes,
            ExecutionResourceKind::MutableBytes => budget.memory.mutable_bytes,
            ExecutionResourceKind::LocalIoConcurrency => budget.io.local_concurrency.map(u64::from),
            ExecutionResourceKind::LocalIoBandwidth => budget.io.local_bytes_per_second,
            ExecutionResourceKind::RemoteIoConcurrency => {
                budget.io.remote_concurrency.map(u64::from)
            }
            ExecutionResourceKind::RemoteIoBandwidth => budget.io.remote_bytes_per_second,
            ExecutionResourceKind::BackgroundTasks => budget.background.task_slots.map(u64::from),
            ExecutionResourceKind::BackgroundInFlightBytes => budget.background.max_in_flight_bytes,
        }
    }

    fn reserved_child_limit(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        child: &ExecutionDomainPath,
        kind: ExecutionResourceKind,
        parent_limit: u64,
    ) -> u64 {
        let record = domains
            .get(child)
            .expect("dedicated child must exist before computing reservations");
        let requested = Self::metric_budget_limit(record.spec.budget, kind).unwrap_or(parent_limit);
        requested.min(parent_limit)
    }

    fn metric_limit_locked(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
        kind: ExecutionResourceKind,
        active_path: &ExecutionDomainPath,
    ) -> MetricLimit {
        let record = domains
            .get(path)
            .expect("execution domain must exist before computing limits");
        let own_requested_limit = Self::metric_budget_limit(record.spec.budget, kind);

        let Some(parent) = path.parent() else {
            let process_limit =
                Self::metric_budget_limit(self.process_budget, kind).unwrap_or(u64::MAX);
            let own_limit = own_requested_limit
                .unwrap_or(process_limit)
                .min(process_limit);
            return MetricLimit {
                limit: own_limit,
                strict_share: own_limit,
            };
        };

        let parent_limit = self
            .metric_limit_locked(domains, &parent, kind, active_path)
            .limit;
        let own_ceiling = own_requested_limit
            .unwrap_or(parent_limit)
            .min(parent_limit);

        match record.spec.placement {
            ExecutionDomainPlacement::Dedicated => MetricLimit {
                limit: own_ceiling,
                strict_share: own_ceiling,
            },
            ExecutionDomainPlacement::SharedWeighted { .. } => {
                let children = self.direct_children(domains, &parent);
                let shared_children = children
                    .iter()
                    .filter(|child| {
                        let child_record = domains
                            .get(*child)
                            .expect("child domain must exist before filtering");
                        child_record.state != ExecutionDomainState::Retired
                            && matches!(
                                child_record.spec.placement,
                                ExecutionDomainPlacement::SharedWeighted { .. }
                            )
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let reserved_total = children
                    .iter()
                    .filter_map(|child| {
                        let child_record = domains
                            .get(child)
                            .expect("child domain must exist before computing reservations");
                        (child_record.state != ExecutionDomainState::Retired
                            && matches!(
                                child_record.spec.placement,
                                ExecutionDomainPlacement::Dedicated
                            ))
                        .then_some(self.reserved_child_limit(
                            domains,
                            child,
                            kind,
                            parent_limit,
                        ))
                    })
                    .fold(0_u64, |total, reserved| total.saturating_add(reserved));
                let shared_pool = parent_limit.saturating_sub(reserved_total);

                let total_registered_weight = shared_children.iter().fold(0_u32, |total, child| {
                    total.saturating_add(self.metric_weight_for_path(domains, child, kind))
                });
                let own_weight = self.metric_weight_for_path(domains, path, kind);
                let strict_share = if shared_children.len() <= 1 || total_registered_weight == 0 {
                    shared_pool
                } else {
                    weighted_share(shared_pool, own_weight, total_registered_weight)
                };

                let busy_children = shared_children
                    .iter()
                    .filter(|child| self.is_branch_busy(domains, child, active_path))
                    .cloned()
                    .collect::<Vec<_>>();
                let busy_weight = busy_children.iter().fold(0_u32, |total, child| {
                    total.saturating_add(self.metric_weight_for_path(domains, child, kind))
                });
                let nonbusy_usage = shared_children
                    .iter()
                    .filter(|child| !busy_children.contains(child))
                    .fold(0_u64, |total, child| {
                        total.saturating_add(self.aggregate_metric(domains, child, kind))
                    });
                let busy_pool = shared_pool.saturating_sub(nonbusy_usage);
                let dynamic_share = if busy_children.len() <= 1 || busy_weight == 0 {
                    busy_pool
                } else {
                    weighted_share(busy_pool, own_weight, busy_weight)
                };

                MetricLimit {
                    limit: own_ceiling.min(dynamic_share),
                    strict_share: own_ceiling.min(strict_share),
                }
            }
        }
    }

    fn effective_budget_locked(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
    ) -> ExecutionDomainBudget {
        let cpu_limit =
            self.metric_limit_locked(domains, path, ExecutionResourceKind::CpuWorkers, path);
        let memory_limit =
            self.metric_limit_locked(domains, path, ExecutionResourceKind::MemoryBytes, path);
        let cache_limit =
            self.metric_limit_locked(domains, path, ExecutionResourceKind::CacheBytes, path);
        let mutable_limit =
            self.metric_limit_locked(domains, path, ExecutionResourceKind::MutableBytes, path);
        let local_io_limit = self.metric_limit_locked(
            domains,
            path,
            ExecutionResourceKind::LocalIoConcurrency,
            path,
        );
        let local_io_bandwidth_limit =
            self.metric_limit_locked(domains, path, ExecutionResourceKind::LocalIoBandwidth, path);
        let remote_io_limit = self.metric_limit_locked(
            domains,
            path,
            ExecutionResourceKind::RemoteIoConcurrency,
            path,
        );
        let remote_io_bandwidth_limit = self.metric_limit_locked(
            domains,
            path,
            ExecutionResourceKind::RemoteIoBandwidth,
            path,
        );
        let background_limit =
            self.metric_limit_locked(domains, path, ExecutionResourceKind::BackgroundTasks, path);
        let background_bytes_limit = self.metric_limit_locked(
            domains,
            path,
            ExecutionResourceKind::BackgroundInFlightBytes,
            path,
        );

        ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: limit_u64_to_u32_option(cpu_limit.limit),
                weight: Some(self.metric_weight_for_path(
                    domains,
                    path,
                    ExecutionResourceKind::CpuWorkers,
                )),
            },
            memory: DomainMemoryBudget {
                total_bytes: limit_u64_to_option(memory_limit.limit),
                cache_bytes: limit_u64_to_option(cache_limit.limit),
                mutable_bytes: limit_u64_to_option(mutable_limit.limit),
            },
            io: DomainIoBudget {
                local_concurrency: limit_u64_to_u32_option(local_io_limit.limit),
                local_bytes_per_second: limit_u64_to_option(local_io_bandwidth_limit.limit),
                remote_concurrency: limit_u64_to_u32_option(remote_io_limit.limit),
                remote_bytes_per_second: limit_u64_to_option(remote_io_bandwidth_limit.limit),
            },
            background: DomainBackgroundBudget {
                task_slots: limit_u64_to_u32_option(background_limit.limit),
                max_in_flight_bytes: limit_u64_to_option(background_bytes_limit.limit),
            },
        }
    }

    fn domain_snapshot_locked(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
        path: &ExecutionDomainPath,
    ) -> ExecutionDomainSnapshot {
        let record = domains
            .get(path)
            .expect("execution domain must exist before snapshotting");
        let backlog = self.aggregate_backlog(domains, path);
        let usage = self.aggregate_usage(domains, path);
        let contention = self.aggregate_contention(domains, path);
        ExecutionDomainSnapshot {
            spec: record.spec.clone(),
            parent: path.parent(),
            state: record.state,
            effective_budget: self.effective_budget_locked(domains, path),
            usage: ExecutionDomainUsageSnapshot::from_usage_and_backlog(usage, backlog),
            backlog,
            contention,
        }
    }

    fn build_snapshot_locked(
        &self,
        domains: &BTreeMap<ExecutionDomainPath, DomainRuntimeRecord>,
    ) -> ResourceManagerSnapshot {
        ResourceManagerSnapshot {
            process_budget: self.process_budget,
            placement_policy_name: self.placement_policy.name().to_string(),
            invariants: self.invariants.clone(),
            domains: domains
                .keys()
                .cloned()
                .map(|path| {
                    let snapshot = self.domain_snapshot_locked(domains, &path);
                    (path, snapshot)
                })
                .collect(),
        }
    }
}

impl ResourceManager for InMemoryResourceManager {
    fn process_budget(&self) -> ExecutionDomainBudget {
        self.process_budget
    }

    fn placement_policy(&self) -> Arc<dyn PlacementPolicy> {
        self.placement_policy.clone()
    }

    fn register_domain(&self, spec: ExecutionDomainSpec) -> ExecutionDomainSnapshot {
        let (snapshot, event) = {
            let mut domains = self.domains.lock();
            Self::ensure_record(&mut domains, &spec.path);
            let event = if domains
                .get(&spec.path)
                .is_some_and(|record| record.explicit)
            {
                ExecutionDomainLifecycleEvent::Updated
            } else {
                ExecutionDomainLifecycleEvent::Registered
            };
            let entry = domains
                .get_mut(&spec.path)
                .expect("domain must exist after ensuring hierarchy");
            entry.spec = if entry.explicit {
                Self::merge_spec(entry.spec.clone(), spec)
            } else {
                spec
            };
            entry.state = ExecutionDomainState::Active;
            entry.explicit = true;
            let path = entry.spec.path.clone();
            let snapshot = self.domain_snapshot_locked(&domains, &path);
            (snapshot, event)
        };
        self.notify(&snapshot, event);
        snapshot
    }

    fn update_domain(&self, spec: ExecutionDomainSpec) -> ExecutionDomainSnapshot {
        let (snapshot, event) = {
            let mut domains = self.domains.lock();
            Self::ensure_record(&mut domains, &spec.path);
            let path = spec.path.clone();
            let event = if domains.get(&path).is_some_and(|record| record.explicit) {
                ExecutionDomainLifecycleEvent::Updated
            } else {
                ExecutionDomainLifecycleEvent::Registered
            };
            let entry = domains
                .get_mut(&path)
                .expect("domain must exist after ensuring hierarchy");
            // `update_domain` is a true reconfiguration seam. It replaces the
            // explicit contract while preserving the live counters attached to
            // the domain path.
            entry.spec = spec;
            entry.state = ExecutionDomainState::Active;
            entry.explicit = true;
            let snapshot = self.domain_snapshot_locked(&domains, &path);
            (snapshot, event)
        };
        self.notify(&snapshot, event);
        snapshot
    }

    fn retire_domain(&self, path: &ExecutionDomainPath) {
        let snapshot = {
            let mut domains = self.domains.lock();
            let Some(entry) = domains.get_mut(path) else {
                return;
            };
            entry.state = ExecutionDomainState::Retired;
            self.domain_snapshot_locked(&domains, path)
        };
        self.notify(&snapshot, ExecutionDomainLifecycleEvent::Retired);
    }

    fn register_lifecycle_hook(&self, hook: Arc<dyn ExecutionDomainLifecycleHook>) {
        self.hooks.lock().push(hook);
    }

    fn snapshot(&self) -> ResourceManagerSnapshot {
        let domains = self.domains.lock();
        self.build_snapshot_locked(&domains)
    }

    fn assign(&self, request: PlacementRequest) -> PlacementAssignment {
        self.placement_policy.assign(&request, &self.snapshot())
    }

    fn placement_tag(&self, request: WorkPlacementRequest) -> WorkRuntimeTag {
        WorkRuntimeTag {
            owner: request.owner,
            lane: request.lane,
            contention_class: request.contention_class,
            domain: request.binding.domain,
            durability_class: request.binding.durability_class,
        }
    }

    fn try_acquire(
        &self,
        path: &ExecutionDomainPath,
        usage: ExecutionResourceUsage,
    ) -> ResourceAdmissionDecision {
        let mut domains = self.domains.lock();
        Self::ensure_record(&mut domains, path);

        let current_usage = self.aggregate_usage(&domains, path);
        let mut after_usage = current_usage;
        after_usage.saturating_add_assign(usage);

        let mut blocked = BTreeSet::new();
        let mut borrowed_shared_capacity = false;

        for kind in ExecutionResourceKind::ALL {
            let path_limit = self.metric_limit_locked(&domains, path, kind, path);
            let after_path_metric = after_usage.metric(kind);
            if after_path_metric > path_limit.limit {
                blocked.insert(kind);
            }

            for ancestor in path.lineage() {
                let current_metric = self.aggregate_metric(&domains, &ancestor, kind);
                let after_metric = current_metric.saturating_add(usage.metric(kind));
                let ancestor_limit = self.metric_limit_locked(&domains, &ancestor, kind, path);
                if after_metric > ancestor_limit.limit {
                    blocked.insert(kind);
                } else if after_metric > ancestor_limit.strict_share {
                    borrowed_shared_capacity = true;
                }
            }
        }

        if blocked.is_empty() {
            let entry = domains
                .get_mut(path)
                .expect("domain must exist before applying usage");
            entry.direct_usage.saturating_add_assign(usage);
            let snapshot = self.domain_snapshot_locked(&domains, path);
            return ResourceAdmissionDecision {
                admitted: true,
                borrowed_shared_capacity,
                blocked_by: Vec::new(),
                effective_budget: snapshot.effective_budget,
                snapshot,
            };
        }

        let blocked_by = blocked.into_iter().collect::<Vec<_>>();
        let entry = domains
            .get_mut(path)
            .expect("domain must exist before recording contention");
        entry.contention.record_blocked(&blocked_by);
        let snapshot = self.domain_snapshot_locked(&domains, path);
        ResourceAdmissionDecision {
            admitted: false,
            borrowed_shared_capacity: false,
            blocked_by,
            effective_budget: snapshot.effective_budget,
            snapshot,
        }
    }

    fn release(
        &self,
        path: &ExecutionDomainPath,
        usage: ExecutionResourceUsage,
    ) -> ExecutionDomainSnapshot {
        let mut domains = self.domains.lock();
        Self::ensure_record(&mut domains, path);
        let entry = domains
            .get_mut(path)
            .expect("domain must exist before releasing usage");
        assert!(
            entry.direct_usage.contains(usage),
            "attempted to release more execution-domain usage than acquired for {}: have {:?}, requested {:?}",
            path,
            entry.direct_usage,
            usage
        );
        entry.direct_usage.saturating_sub_assign(usage);
        self.domain_snapshot_locked(&domains, path)
    }

    fn direct_backlog(&self, path: &ExecutionDomainPath) -> ExecutionDomainBacklogSnapshot {
        self.domains
            .lock()
            .get(path)
            .map(|entry| entry.backlog)
            .unwrap_or_default()
    }

    fn set_backlog(
        &self,
        path: &ExecutionDomainPath,
        backlog: ExecutionDomainBacklogSnapshot,
    ) -> ExecutionDomainSnapshot {
        let mut domains = self.domains.lock();
        Self::ensure_record(&mut domains, path);
        let entry = domains
            .get_mut(path)
            .expect("domain must exist before updating backlog");
        entry.backlog = backlog;
        self.domain_snapshot_locked(&domains, path)
    }
}

impl fmt::Debug for InMemoryResourceManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryResourceManager")
            .field("process_budget", &self.process_budget)
            .field("invariants", &self.invariants)
            .field("placement_policy", &self.placement_policy.name())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainBudgetCharge {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub local_io_bytes: u64,
    pub remote_io_bytes: u64,
    pub background_tasks: u32,
}

impl DomainBudgetCharge {
    fn saturating_add_assign(&mut self, other: DomainBudgetCharge) {
        self.cpu_millis = self.cpu_millis.saturating_add(other.cpu_millis);
        self.memory_bytes = self.memory_bytes.saturating_add(other.memory_bytes);
        self.local_io_bytes = self.local_io_bytes.saturating_add(other.local_io_bytes);
        self.remote_io_bytes = self.remote_io_bytes.saturating_add(other.remote_io_bytes);
        self.background_tasks = self.background_tasks.saturating_add(other.background_tasks);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainBudgetSnapshot {
    pub total: DomainBudgetCharge,
    pub by_contention_class: BTreeMap<ContentionClass, DomainBudgetCharge>,
}

pub trait DomainBudgetOracle: Send + Sync {
    fn record(&self, tag: &WorkRuntimeTag, charge: DomainBudgetCharge);
    fn snapshot(&self) -> BTreeMap<ExecutionDomainPath, DomainBudgetSnapshot>;
}

#[derive(Default)]
pub struct InMemoryDomainBudgetOracle {
    by_domain: Mutex<BTreeMap<ExecutionDomainPath, DomainBudgetSnapshot>>,
}

impl DomainBudgetOracle for InMemoryDomainBudgetOracle {
    fn record(&self, tag: &WorkRuntimeTag, charge: DomainBudgetCharge) {
        let mut by_domain = self.by_domain.lock();
        for path in tag.domain.lineage() {
            let snapshot = by_domain.entry(path).or_default();
            snapshot.total.saturating_add_assign(charge);
            snapshot
                .by_contention_class
                .entry(tag.contention_class)
                .or_default()
                .saturating_add_assign(charge);
        }
    }

    fn snapshot(&self) -> BTreeMap<ExecutionDomainPath, DomainBudgetSnapshot> {
        self.by_domain.lock().clone()
    }
}

/// Simulation-friendly workload descriptor for colocated multi-DB scenarios.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColocatedDbWorkloadSpec {
    pub db_name: String,
    pub execution_profile: DbExecutionProfile,
    pub operation_count: usize,
    pub metadata: BTreeMap<String, String>,
}

pub trait ColocatedDbWorkloadGenerator: Send + Sync {
    fn generate(&self, seed: u64) -> Vec<ColocatedDbWorkloadSpec>;
}

fn merge_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn merge_optional_u32(left: Option<u32>, right: Option<u32>) -> Option<u32> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn merge_optional_weight(left: Option<u32>, right: Option<u32>) -> Option<u32> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value.max(1)),
        (None, None) => None,
    }
}

fn weighted_share(pool: u64, weight: u32, total_weight: u32) -> u64 {
    if total_weight == 0 {
        return pool;
    }
    let share = ((u128::from(pool) * u128::from(weight)) / u128::from(total_weight))
        .min(u128::from(u64::MAX)) as u64;
    if share == 0 && pool > 0 { 1 } else { share }
}

fn limit_u64_to_option(limit: u64) -> Option<u64> {
    (limit != u64::MAX).then_some(limit)
}

fn limit_u64_to_u32_option(limit: u64) -> Option<u32> {
    if limit == u64::MAX {
        None
    } else {
        Some(limit.min(u64::from(u32::MAX)) as u32)
    }
}
