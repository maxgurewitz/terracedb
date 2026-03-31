use thiserror::Error;

use terracedb_vfs::{VfsError, VolumeId};

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("sandbox session {session_volume_id} requires a base volume to be created")]
    MissingBaseVolume { session_volume_id: VolumeId },
    #[error("sandbox session metadata is missing from {path}")]
    MissingSessionMetadata { path: String },
    #[error("sandbox session metadata at {path} is invalid: {reason}")]
    InvalidSessionMetadata { path: String, reason: String },
    #[error("sandbox session config mismatch for {field}: expected {expected}, found {found}")]
    SessionConfigMismatch {
        field: &'static str,
        expected: String,
        found: String,
    },
    #[error("sandbox capability specifier must start with terrace:host/: {specifier}")]
    InvalidCapabilitySpecifier { specifier: String },
    #[error("unsupported sandbox module specifier: {specifier}")]
    InvalidModuleSpecifier { specifier: String },
    #[error("sandbox module not found: {specifier}")]
    ModuleNotFound { specifier: String },
    #[error("sandbox capability is not allowed in this session: {specifier}")]
    CapabilityDenied { specifier: String },
    #[error("sandbox capability is not available from the host registry: {specifier}")]
    CapabilityUnavailable { specifier: String },
    #[error("sandbox capability method was not found: {specifier}::{method}")]
    CapabilityMethodNotFound { specifier: String, method: String },
    #[error("sandbox package request is unsupported: {package} ({reason})")]
    UnsupportedPackage { package: String, reason: String },
    #[error("sandbox package is not installed: {package}")]
    PackageNotInstalled { package: String },
    #[error("readonly view uri is invalid: {uri}")]
    InvalidReadonlyViewUri { uri: String },
    #[error("readonly view handle {handle_id} not found")]
    ViewHandleNotFound { handle_id: String },
    #[error("sandbox execution failed for {entrypoint}: {message}")]
    Execution { entrypoint: String, message: String },
    #[error("{service} backend error: {message}")]
    Service {
        service: &'static str,
        message: String,
    },
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Vfs(#[from] VfsError),
}
