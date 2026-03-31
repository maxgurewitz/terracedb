use std::{collections::BTreeMap, fmt, sync::Arc};

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
pub struct ExecutionDomainUsageSnapshot {
    pub cpu_workers_in_use: u32,
    pub memory_bytes: u64,
    pub local_io_in_flight: u32,
    pub remote_io_in_flight: u32,
    pub background_tasks: u32,
    pub queued_work_items: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionDomainSnapshot {
    pub spec: ExecutionDomainSpec,
    pub state: ExecutionDomainState,
    pub usage: ExecutionDomainUsageSnapshot,
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
    pub preferred_domain: ExecutionDomainPath,
    pub requested_budget: ExecutionDomainBudget,
    pub placement: ExecutionDomainPlacement,
    pub default_durability_class: DurabilityClass,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementAssignment {
    pub owner: ExecutionDomainOwner,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceManagerSnapshot {
    pub process_budget: ExecutionDomainBudget,
    pub placement_policy_name: String,
    pub invariants: ExecutionDomainInvariantSet,
    pub domains: BTreeMap<ExecutionDomainPath, ExecutionDomainSnapshot>,
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
}

/// Deterministic in-memory stub used until a real process-wide scheduler lands.
pub struct InMemoryResourceManager {
    process_budget: ExecutionDomainBudget,
    invariants: ExecutionDomainInvariantSet,
    placement_policy: Arc<dyn PlacementPolicy>,
    domains: Mutex<BTreeMap<ExecutionDomainPath, ExecutionDomainSnapshot>>,
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

    fn snapshot_for_spec(&self, spec: ExecutionDomainSpec) -> ExecutionDomainSnapshot {
        ExecutionDomainSnapshot {
            spec,
            state: ExecutionDomainState::Active,
            usage: ExecutionDomainUsageSnapshot::default(),
        }
    }

    fn notify(&self, snapshot: &ExecutionDomainSnapshot, event: ExecutionDomainLifecycleEvent) {
        let hooks = self.hooks.lock().clone();
        for hook in hooks {
            hook.on_event(snapshot, event);
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
        let snapshot = self.snapshot_for_spec(spec);
        self.domains
            .lock()
            .insert(snapshot.spec.path.clone(), snapshot.clone());
        self.notify(&snapshot, ExecutionDomainLifecycleEvent::Registered);
        snapshot
    }

    fn update_domain(&self, spec: ExecutionDomainSpec) -> ExecutionDomainSnapshot {
        let snapshot = self.snapshot_for_spec(spec);
        self.domains
            .lock()
            .insert(snapshot.spec.path.clone(), snapshot.clone());
        self.notify(&snapshot, ExecutionDomainLifecycleEvent::Updated);
        snapshot
    }

    fn retire_domain(&self, path: &ExecutionDomainPath) {
        let snapshot = {
            let mut domains = self.domains.lock();
            let Some(existing) = domains.get_mut(path) else {
                return;
            };
            existing.state = ExecutionDomainState::Retired;
            existing.clone()
        };
        self.notify(&snapshot, ExecutionDomainLifecycleEvent::Retired);
    }

    fn register_lifecycle_hook(&self, hook: Arc<dyn ExecutionDomainLifecycleHook>) {
        self.hooks.lock().push(hook);
    }

    fn snapshot(&self) -> ResourceManagerSnapshot {
        ResourceManagerSnapshot {
            process_budget: self.process_budget,
            placement_policy_name: self.placement_policy.name().to_string(),
            invariants: self.invariants.clone(),
            domains: self.domains.lock().clone(),
        }
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
        let snapshot = by_domain.entry(tag.domain.clone()).or_default();
        snapshot.total.saturating_add_assign(charge);
        snapshot
            .by_contention_class
            .entry(tag.contention_class)
            .or_default()
            .saturating_add_assign(charge);
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
