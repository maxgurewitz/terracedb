//! TerraceDB's JS substrate exposes two backend shapes:
//! - `DeterministicJsRuntimeHost` is the default simulation/oracle backend for deterministic,
//!   async-friendly tests and host seams.
//! - `BoaJsRuntimeHost` is the real `boa_engine` backend for guest-visible JS semantics, using
//!   the same async TerraceDB-owned scheduler, host-service, and module-loader seams.

pub mod adapters;
mod boa;
pub mod compat;
pub mod entropy;
pub mod error;
pub mod loader;
pub mod runtime;
pub mod scheduler;
pub mod time;
pub mod types;

pub use adapters::{SandboxJsRuntimeBinding, SandboxJsRuntimeRequest};
pub use boa::{BoaJsExecutionHooks, BoaJsRuntimeHost, BoaJsScheduler};
pub use compat::{DeterministicJsHostServices, DeterministicJsServiceOutcome, JsHostServices};
pub use entropy::{DeterministicJsEntropySource, JsEntropySnapshot, JsEntropySource};
pub use error::JsSubstrateError;
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
    JsAmbientDefault, JsCompatibilityProfile, JsExecutionKind, JsExecutionReport,
    JsExecutionRequest, JsForkPolicy, JsForkSurface, JsForkSurfacePolicy, JsHostServiceCallRecord,
    JsHostServiceRequest, JsHostServiceResponse, JsOwnershipMode, JsRuntimeHandle,
    JsRuntimeOpenRequest, JsRuntimePolicy, JsRuntimeProvenance, JsTraceEvent, JsTracePhase,
};

pub use serde_json::Value as JsonValue;
