use crate::ids::SequenceNumber;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageErrorKind {
    Io,
    Corruption,
    Timeout,
    DurabilityBoundary,
    Unsupported,
    NotFound,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{kind:?}: {message}")]
pub struct StorageError {
    kind: StorageErrorKind,
    message: String,
}

impl StorageError {
    pub fn new(kind: StorageErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Io, message)
    }

    pub fn corruption(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Corruption, message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Timeout, message)
    }

    pub fn durability_boundary(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::DurabilityBoundary, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Unsupported, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::NotFound, message)
    }

    pub fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum OpenError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("operation not implemented: {0}")]
    Unimplemented(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CreateTableError {
    #[error("table already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid table configuration: {0}")]
    InvalidConfig(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("operation not implemented: {0}")]
    Unimplemented(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CommitError {
    #[error("conflict detected")]
    Conflict,
    #[error("cannot commit an empty batch")]
    EmptyBatch,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("operation not implemented: {0}")]
    Unimplemented(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum WriteError {
    #[error(transparent)]
    Commit(#[from] CommitError),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReadError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    SnapshotTooOld(#[from] SnapshotTooOld),
    #[error("operation not implemented: {0}")]
    Unimplemented(&'static str),
}

impl ReadError {
    pub fn snapshot_too_old(&self) -> Option<&SnapshotTooOld> {
        match self {
            Self::SnapshotTooOld(error) => Some(error),
            Self::Storage(_) | Self::Unimplemented(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum FlushError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("operation not implemented: {0}")]
    Unimplemented(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error(
    "history older than retained horizon: requested {requested}, oldest available {oldest_available}"
)]
pub struct SnapshotTooOld {
    pub requested: SequenceNumber,
    pub oldest_available: SequenceNumber,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("subscription closed")]
pub struct SubscriptionClosed;
