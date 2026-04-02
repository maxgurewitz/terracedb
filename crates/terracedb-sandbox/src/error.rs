use thiserror::Error;

use terracedb::{ExecutionDomainPath, ExecutionResourceKind};
use terracedb_capabilities::{ExecutionDomain, ExecutionOperation};
use terracedb_vfs::{VfsError, VolumeId};

use crate::ConflictReport;

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
    #[error("sandbox execution policy has no route for {operation:?} in domain {domain:?}")]
    MissingExecutionRoute {
        operation: ExecutionOperation,
        domain: ExecutionDomain,
    },
    #[error(
        "sandbox execution policy routed {operation:?} to {domain:?}, but no {service} backend is registered"
    )]
    MissingExecutionBackend {
        operation: ExecutionOperation,
        domain: ExecutionDomain,
        service: &'static str,
    },
    #[error(
        "sandbox execution-domain admission denied for {operation:?} on {path}: blocked by {blocked_by:?}"
    )]
    ExecutionDomainOverloaded {
        operation: ExecutionOperation,
        path: ExecutionDomainPath,
        blocked_by: Vec<ExecutionResourceKind>,
    },
    #[error("readonly view uri is invalid: {uri}")]
    InvalidReadonlyViewUri { uri: String },
    #[error("readonly view handle {handle_id} not found")]
    ViewHandleNotFound { handle_id: String },
    #[error("sandbox execution failed for {entrypoint}: {message}")]
    Execution { entrypoint: String, message: String },
    #[error("readonly view session {session_volume_id} not found")]
    ReadonlyViewSessionNotFound { session_volume_id: VolumeId },
    #[error("readonly view request is unauthorized")]
    ReadonlyViewUnauthorized,
    #[error("sandbox disk operation found conflicts: {report}")]
    DiskConflict { report: ConflictReport },
    #[error("sandbox session does not have hoist provenance for apply-delta eject")]
    MissingHoistProvenance,
    #[error("sandbox session does not have git provenance for this operation")]
    MissingGitProvenance,
    #[error("sandbox session does not have a git remote for repo-backed push or pull request")]
    MissingGitRemote,
    #[error("sandbox git operation has no repository changes to commit for {target_ref}")]
    NoGitChanges { target_ref: String },
    #[error("host path error at {path}: {message}")]
    Io { path: String, message: String },
    #[error("command failed: {command} (status: {status:?}): {stderr}")]
    CommandFailed {
        command: String,
        status: Option<i32>,
        stderr: String,
    },
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
