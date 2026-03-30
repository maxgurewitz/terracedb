use thiserror::Error;

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

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum BlobError {
    #[error("blob {id} not found")]
    NotFound { id: BlobId },
    #[error("blob alias {alias} not found")]
    AliasNotFound { alias: BlobAlias },
    #[error("published metadata references a missing object: {object_key}")]
    MissingObject { object_key: String },
    #[error("unsupported blob-library operation: {operation}")]
    UnsupportedOperation { operation: &'static str },
    #[error(transparent)]
    Contract(#[from] BlobContractError),
    #[error(transparent)]
    Store(#[from] BlobStoreError),
}
