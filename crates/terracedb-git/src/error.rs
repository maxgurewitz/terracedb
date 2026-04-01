use thiserror::Error;

use terracedb_vfs::VfsError;

#[derive(Debug, Error)]
pub enum GitSubstrateError {
    #[error("git repository was not found at or above {path}")]
    RepositoryNotFound { path: String },
    #[error(
        "git repository image descriptor mismatch for {field}: expected {expected}, found {found}"
    )]
    RepositoryImageDescriptorMismatch {
        field: &'static str,
        expected: String,
        found: String,
    },
    #[error("git reference was not found: {reference}")]
    ReferenceNotFound { reference: String },
    #[error("git object was not found: {oid}")]
    ObjectNotFound { oid: String },
    #[error("git repository {repository_id} was cancelled")]
    Cancelled { repository_id: String },
    #[error("git host bridge {operation} failed: {message}")]
    Bridge {
        operation: &'static str,
        message: String,
    },
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Vfs(#[from] VfsError),
}
