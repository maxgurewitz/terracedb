use thiserror::Error;

use terracedb_vfs::VfsError;

#[derive(Debug, Error)]
pub enum GitSubstrateError {
    #[error("git repository was not found at or above {path}")]
    RepositoryNotFound { path: String },
    #[error("git repository {repository_id} is read-only")]
    RepositoryReadOnly { repository_id: String },
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
    #[error("git reference {reference} expected {expected:?}, found {found:?}")]
    ReferenceConflict {
        reference: String,
        expected: Option<String>,
        found: Option<String>,
    },
    #[error("git object was not found: {oid}")]
    ObjectNotFound { oid: String },
    #[error("git object {oid} could not be decoded: {message}")]
    InvalidObject { oid: String, message: String },
    #[error("git repository {repository_id} was cancelled")]
    Cancelled { repository_id: String },
    #[error("git repository has no changes to commit for {target_ref}")]
    NoChanges { target_ref: String },
    #[error("git export conflict at {path}: {message}")]
    ExportConflict { path: String, message: String },
    #[error("git remote push failed for {remote}/{branch}: {message}")]
    RemotePushFailed {
        remote: String,
        branch: String,
        message: String,
    },
    #[error("git pull-request provider {provider} failed: {message}")]
    PullRequestProvider { provider: String, message: String },
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
