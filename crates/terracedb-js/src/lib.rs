//! TerraceDB's JS substrate exposes two backend shapes:
//! - `DeterministicJsRuntimeHost` is the default simulation/oracle backend for deterministic,
//!   async-friendly tests and host seams.
//! - `BoaJsRuntimeHost` is the real `boa_engine` backend for guest-visible JS semantics, using
//!   the same async TerraceDB-owned scheduler, host-service, and module-loader seams.

pub mod adapters;
mod boa;
pub mod engine;
pub mod entropy;
pub mod error;
pub mod host;
pub mod loader;
pub mod runtime;
pub mod scheduler;
pub mod time;
pub mod types;

pub use adapters::{SandboxJsRuntimeBinding, SandboxJsRuntimeRequest};
pub use boa::{BoaJsExecutionHooks, BoaJsRuntimeHost, BoaJsScheduler};
pub use engine::heap::{
    JsCollectorSnapshot, JsEphemeronId, JsGcCycleReport, JsHeapAttachmentSnapshot, JsHeapConfig,
    JsHeapError, JsHeapObjectDescriptor, JsHeapObjectId, JsHeapPhase, JsHeapRootId, JsHeapSnapshot,
    JsRuntimeHeap, JsWeakObjectId,
};
pub use entropy::{DeterministicJsEntropySource, JsEntropySnapshot, JsEntropySource};
pub use error::JsSubstrateError;
pub use host::{
    DeterministicJsHostServices, DeterministicJsServiceOutcome, JsHostServiceAdapter,
    JsHostServices, RoutedJsHostServices, VfsJsHostServiceAdapter,
};
pub use loader::{
    JsLoadedModule, JsModuleKind, JsModuleLoader, JsResolvedModule, VfsJsModuleLoader,
};
pub use runtime::{
    DeterministicJsRuntimeHost, JsCancellationToken, JsExecutionHooks, JsRuntime, JsRuntimeHost,
    NeverCancel, NoopJsExecutionHooks,
};
pub use scheduler::{
    DeterministicJsScheduler, JsScheduledTask, JsScheduler, JsSchedulerSnapshot, JsTaskQueue,
};
pub use time::{DeterministicJsClock, FixedJsClock, JsClock, JsClockSnapshot};
pub use types::{
    JsAmbientDefault, JsCallbackId, JsCompatibilityProfile, JsCompletedHostOperation,
    JsExecutionKind, JsExecutionReport, JsExecutionRequest, JsForkPolicy, JsForkSurface,
    JsForkSurfacePolicy, JsHostOpId, JsHostServiceCallRecord, JsHostServiceRequest,
    JsHostServiceResponse, JsModuleGraphNode, JsModuleId, JsOwnershipMode, JsPendingHostOperation,
    JsPendingTask, JsPendingTimer, JsPromiseId, JsRuntimeAttachmentState, JsRuntimeConfiguration,
    JsRuntimeEnvironment, JsRuntimeErrorReport, JsRuntimeHandle, JsRuntimeOpenRequest,
    JsRuntimePolicy, JsRuntimeProvenance, JsRuntimeSuspendedState, JsRuntimeTurn,
    JsRuntimeTurnCompletion, JsRuntimeTurnKind, JsRuntimeTurnOutcome, JsTaskId, JsTimerId,
    JsTraceEvent, JsTracePhase,
};

pub use serde_json::Value as JsonValue;
