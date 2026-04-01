use thiserror::Error;

use terracedb_vfs::VfsError;

#[derive(Debug, Error)]
pub enum JsSubstrateError {
    #[error("unsupported js module specifier: {specifier}")]
    UnsupportedSpecifier { specifier: String },
    #[error("js module not found: {specifier}")]
    ModuleNotFound { specifier: String },
    #[error("js host service is unavailable: {service}::{operation}")]
    HostServiceUnavailable { service: String, operation: String },
    #[error("js host service denied {service}::{operation}: {message}")]
    HostServiceDenied {
        service: String,
        operation: String,
        message: String,
    },
    #[error("js runtime {runtime_id} was cancelled")]
    Cancelled { runtime_id: String },
    #[error("js evaluation failed for {entrypoint}: {message}")]
    EvaluationFailed { entrypoint: String, message: String },
    #[error("invalid js fake-runtime directive in {module}: {message}")]
    InvalidDirective { module: String, message: String },
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Vfs(#[from] VfsError),
}
