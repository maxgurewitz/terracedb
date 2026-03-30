use thiserror::Error;

use terracedb::StorageErrorKind;

use crate::{BlobAlias, BlobId};

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum BlobContractError {
    #[error("namespace cannot be empty")]
    EmptyNamespace,
    #[error("alias cannot be empty")]
    EmptyAlias,
    #[error("{field} cannot be empty")]
    EmptyKeyPart { field: &'static str },
    #[error("{field} cannot contain NUL bytes")]
    NulByteInKeyPart { field: &'static str },
    #[error("invalid reserved key: {reason}")]
    InvalidKey { reason: String },
    #[error(transparent)]
    Encoding(#[from] terracedb::IdEncodingError),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum BlobStoreError {
    #[error("blob object not found: {key}")]
    NotFound { key: String },
    #[error("invalid byte range {start}..{end} for object {key} with size {size_bytes}")]
    InvalidRange {
        key: String,
        start: u64,
        end: u64,
        size_bytes: u64,
    },
    #[error("unsupported blob-store operation: {operation}")]
    UnsupportedOperation { operation: &'static str },
    #[error(transparent)]
    Contract(#[from] BlobContractError),
    #[error(transparent)]
    Storage(#[from] terracedb::StorageError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobStoreOperation {
    Put,
    Get,
    Stat,
    Delete,
    List,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobStoreRecoveryHint {
    Retry,
    ConfirmWithStat,
    RefreshListing,
    FailClosed,
}

impl BlobStoreError {
    pub fn kind(&self) -> Option<StorageErrorKind> {
        match self {
            Self::Storage(error) => Some(error.kind()),
            Self::NotFound { .. }
            | Self::InvalidRange { .. }
            | Self::UnsupportedOperation { .. }
            | Self::Contract(_) => None,
        }
    }

    pub fn recovery_hint(&self, operation: BlobStoreOperation) -> BlobStoreRecoveryHint {
        match self {
            Self::NotFound { .. }
            | Self::InvalidRange { .. }
            | Self::UnsupportedOperation { .. }
            | Self::Contract(_) => BlobStoreRecoveryHint::FailClosed,
            Self::Storage(error) => match (operation, error.kind()) {
                (BlobStoreOperation::Put, StorageErrorKind::Timeout)
                | (BlobStoreOperation::Put, StorageErrorKind::Io)
                | (BlobStoreOperation::Put, StorageErrorKind::DurabilityBoundary) => {
                    BlobStoreRecoveryHint::ConfirmWithStat
                }
                (BlobStoreOperation::List, StorageErrorKind::DurabilityBoundary) => {
                    BlobStoreRecoveryHint::RefreshListing
                }
                (_, StorageErrorKind::Timeout) | (_, StorageErrorKind::Io) => {
                    BlobStoreRecoveryHint::Retry
                }
                (_, StorageErrorKind::Corruption)
                | (_, StorageErrorKind::Unsupported)
                | (_, StorageErrorKind::NotFound)
                | (_, StorageErrorKind::DurabilityBoundary) => BlobStoreRecoveryHint::FailClosed,
            },
        }
    }

    pub fn is_retryable(&self, operation: BlobStoreOperation) -> bool {
        matches!(
            self.recovery_hint(operation),
            BlobStoreRecoveryHint::Retry
                | BlobStoreRecoveryHint::ConfirmWithStat
                | BlobStoreRecoveryHint::RefreshListing
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum BlobError {
    #[error("blob {id} not found")]
    NotFound { id: BlobId },
    #[error("blob alias {alias} not found")]
    AliasNotFound { alias: BlobAlias },
    #[error("published metadata references a missing object: {object_key}")]
    MissingObject { object_key: String },
    #[error(transparent)]
    SnapshotTooOld(#[from] terracedb::SnapshotTooOld),
    #[error("unsupported blob-library operation: {operation}")]
    UnsupportedOperation { operation: &'static str },
    #[error(transparent)]
    Contract(#[from] BlobContractError),
    #[error(transparent)]
    Storage(#[from] terracedb::StorageError),
    #[error(transparent)]
    Store(#[from] BlobStoreError),
}
