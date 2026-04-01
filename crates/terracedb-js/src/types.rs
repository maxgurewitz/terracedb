use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{loader::JsModuleKind, scheduler::JsScheduledTask};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsCompatibilityProfile {
    #[default]
    TerraceOnly,
    SandboxCompat,
    BoaRuntimeCompat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsAmbientDefault {
    CurrentWorkingDirectory,
    HostWallClock,
    ProcessEntropy,
    Stdout,
    Stderr,
    ProcessEnv,
    Tempfiles,
    ThreadSpawn,
    InterruptState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsForkSurface {
    BoaEngine,
    BoaRuntime,
    BoaWinterTc,
    ModuleLoader,
    Scheduler,
    Clock,
    Entropy,
    HostServices,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsOwnershipMode {
    Consumed,
    Wrapped,
    Forked,
    Forbidden,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsForkSurfacePolicy {
    pub surface: JsForkSurface,
    pub mode: JsOwnershipMode,
    pub notes: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsForkPolicy {
    pub surfaces: Vec<JsForkSurfacePolicy>,
    pub forbidden_ambient_defaults: Vec<JsAmbientDefault>,
}

impl Default for JsForkPolicy {
    fn default() -> Self {
        Self::simulation_native_baseline()
    }
}

impl JsForkPolicy {
    pub fn simulation_native_baseline() -> Self {
        Self {
            surfaces: vec![
                JsForkSurfacePolicy {
                    surface: JsForkSurface::BoaEngine,
                    mode: JsOwnershipMode::Wrapped,
                    notes: "real Boa surfaces stay behind terracedb-js host-owned wrappers"
                        .to_string(),
                },
                JsForkSurfacePolicy {
                    surface: JsForkSurface::BoaRuntime,
                    mode: JsOwnershipMode::Forbidden,
                    notes: "ambient runtime helpers stay unavailable until explicitly re-hosted"
                        .to_string(),
                },
                JsForkSurfacePolicy {
                    surface: JsForkSurface::BoaWinterTc,
                    mode: JsOwnershipMode::Forbidden,
                    notes: "web-style compatibility layers must consume explicit host services"
                        .to_string(),
                },
                JsForkSurfacePolicy {
                    surface: JsForkSurface::ModuleLoader,
                    mode: JsOwnershipMode::Forked,
                    notes: "module loading is owned by VFS-native terrace resolution".to_string(),
                },
                JsForkSurfacePolicy {
                    surface: JsForkSurface::Scheduler,
                    mode: JsOwnershipMode::Forked,
                    notes: "promise jobs and timers stay under deterministic host scheduling"
                        .to_string(),
                },
                JsForkSurfacePolicy {
                    surface: JsForkSurface::Clock,
                    mode: JsOwnershipMode::Forked,
                    notes: "guest-visible time must come from injected host clocks".to_string(),
                },
                JsForkSurfacePolicy {
                    surface: JsForkSurface::Entropy,
                    mode: JsOwnershipMode::Forked,
                    notes: "guest-visible randomness must come from injected seeded entropy"
                        .to_string(),
                },
                JsForkSurfacePolicy {
                    surface: JsForkSurface::HostServices,
                    mode: JsOwnershipMode::Wrapped,
                    notes: "console, fetch, process, and capability surfaces stay behind explicit host-service traits"
                        .to_string(),
                },
            ],
            forbidden_ambient_defaults: vec![
                JsAmbientDefault::CurrentWorkingDirectory,
                JsAmbientDefault::HostWallClock,
                JsAmbientDefault::ProcessEntropy,
                JsAmbientDefault::Stdout,
                JsAmbientDefault::Stderr,
                JsAmbientDefault::ProcessEnv,
                JsAmbientDefault::Tempfiles,
                JsAmbientDefault::ThreadSpawn,
                JsAmbientDefault::InterruptState,
            ],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsRuntimePolicy {
    pub execution_domain: Option<String>,
    pub compatibility_profile: JsCompatibilityProfile,
    pub allow_workspace_modules: bool,
    pub allow_host_modules: bool,
    pub allow_package_modules: bool,
    pub visible_host_services: Vec<String>,
    pub forbidden_ambient_defaults: Vec<JsAmbientDefault>,
}

impl Default for JsRuntimePolicy {
    fn default() -> Self {
        Self {
            execution_domain: None,
            compatibility_profile: JsCompatibilityProfile::TerraceOnly,
            allow_workspace_modules: true,
            allow_host_modules: true,
            allow_package_modules: false,
            visible_host_services: Vec::new(),
            forbidden_ambient_defaults: JsForkPolicy::simulation_native_baseline()
                .forbidden_ambient_defaults,
        }
    }
}

impl JsRuntimePolicy {
    pub fn allows_host_service(&self, service: &str, operation: &str) -> bool {
        let key = format!("{service}::{operation}");
        self.visible_host_services
            .iter()
            .any(|candidate| candidate == &key)
    }

    pub fn allows_module_kind(&self, kind: JsModuleKind) -> bool {
        match kind {
            JsModuleKind::Workspace => self.allow_workspace_modules,
            JsModuleKind::HostCapability => self.allow_host_modules,
            JsModuleKind::Package => self.allow_package_modules,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsRuntimeProvenance {
    pub backend: String,
    pub host_model: String,
    pub module_root: String,
    pub volume_id: Option<terracedb_vfs::VolumeId>,
    pub snapshot_sequence: Option<u64>,
    pub durable_snapshot: bool,
    pub fork_policy: JsForkPolicy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsRuntimeOpenRequest {
    pub runtime_id: String,
    pub policy: JsRuntimePolicy,
    pub provenance: JsRuntimeProvenance,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsRuntimeHandle {
    pub runtime_id: String,
    pub backend: String,
    pub policy: JsRuntimePolicy,
    pub provenance: JsRuntimeProvenance,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum JsExecutionKind {
    Module {
        specifier: String,
    },
    Eval {
        source: String,
        virtual_specifier: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsExecutionRequest {
    pub kind: JsExecutionKind,
    pub metadata: BTreeMap<String, JsonValue>,
}

impl JsExecutionRequest {
    pub fn module(specifier: impl Into<String>) -> Self {
        Self {
            kind: JsExecutionKind::Module {
                specifier: specifier.into(),
            },
            metadata: BTreeMap::new(),
        }
    }

    pub fn eval(source: impl Into<String>) -> Self {
        Self {
            kind: JsExecutionKind::Eval {
                source: source.into(),
                virtual_specifier: None,
            },
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsHostServiceRequest {
    pub service: String,
    pub operation: String,
    pub arguments: JsonValue,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsHostServiceResponse {
    pub result: Option<JsonValue>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsHostServiceCallRecord {
    pub service: String,
    pub operation: String,
    pub arguments: JsonValue,
    pub result: Option<JsonValue>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsTracePhase {
    RuntimeOpen,
    ModuleResolve,
    ModuleLoad,
    Scheduler,
    HostCall,
    RuntimeClose,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsTraceEvent {
    pub phase: JsTracePhase,
    pub label: String,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct JsExecutionReport {
    pub runtime_id: String,
    pub backend: String,
    pub entrypoint: String,
    pub result: Option<JsonValue>,
    pub module_graph: Vec<String>,
    pub host_calls: Vec<JsHostServiceCallRecord>,
    pub trace: Vec<JsTraceEvent>,
    pub clock_now_millis: u64,
    pub entropy_sample: Vec<u8>,
    pub scheduled_tasks: Vec<JsScheduledTask>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        JsExecutionKind, JsExecutionRequest, JsForkPolicy, JsRuntimeOpenRequest, JsRuntimePolicy,
        JsRuntimeProvenance, JsTraceEvent, JsTracePhase,
    };

    #[test]
    fn js_request_and_trace_metadata_round_trip() {
        let request = JsRuntimeOpenRequest {
            runtime_id: "runtime-1".to_string(),
            policy: JsRuntimePolicy::default(),
            provenance: JsRuntimeProvenance {
                backend: "deterministic-js".to_string(),
                host_model: "host-owned".to_string(),
                module_root: "/workspace".to_string(),
                volume_id: Some(terracedb_vfs::VolumeId::new(0x7000)),
                snapshot_sequence: Some(12),
                durable_snapshot: false,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            metadata: BTreeMap::from([("kind".to_string(), json!("compile-only"))]),
        };
        let encoded = serde_json::to_vec(&request).expect("encode js request");
        let decoded: JsRuntimeOpenRequest =
            serde_json::from_slice(&encoded).expect("decode js request");
        assert_eq!(decoded, request);

        let trace = JsTraceEvent {
            phase: JsTracePhase::HostCall,
            label: "capability::echo".to_string(),
            metadata: BTreeMap::from([("result".to_string(), json!("ok"))]),
        };
        let encoded = serde_json::to_string(&trace).expect("encode trace");
        let decoded: JsTraceEvent = serde_json::from_str(&encoded).expect("decode trace");
        assert_eq!(decoded, trace);

        let execution = JsExecutionRequest {
            kind: JsExecutionKind::Module {
                specifier: "terrace:/workspace/main.mjs".to_string(),
            },
            metadata: BTreeMap::new(),
        };
        let encoded = serde_json::to_vec(&execution).expect("encode execution request");
        let decoded: JsExecutionRequest =
            serde_json::from_slice(&encoded).expect("decode execution request");
        assert_eq!(decoded, execution);
    }
}
