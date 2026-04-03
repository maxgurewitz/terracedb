use thiserror::Error;

use crate::{ToolRunId, VolumeId};

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("volume {volume_id} not found")]
    VolumeNotFound { volume_id: VolumeId },
    #[error("volume {volume_id} already exists")]
    VolumeAlreadyExists { volume_id: VolumeId },
    #[error(
        "volume {volume_id} already exists with chunk size {existing_chunk_size}, requested {requested_chunk_size}"
    )]
    VolumeConfigMismatch {
        volume_id: VolumeId,
        existing_chunk_size: u32,
        requested_chunk_size: u32,
    },
    #[error("volume chunk size must be greater than zero")]
    InvalidChunkSize,
    #[error("path must be absolute and normalized: {path}")]
    InvalidPath { path: String },
    #[error("path already exists: {path}")]
    AlreadyExists { path: String },
    #[error("path not found: {path}")]
    NotFound { path: String },
    #[error("path is not a directory: {path}")]
    NotDirectory { path: String },
    #[error("path is a directory: {path}")]
    IsDirectory { path: String },
    #[error("path is not a regular file: {path}")]
    NotFile { path: String },
    #[error("path is not a symlink: {path}")]
    NotSymlink { path: String },
    #[error("too many symlink expansions while resolving path: {path}")]
    SymlinkLoop { path: String },
    #[error("directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
    #[error("cannot remove or replace the volume root")]
    RootInvariant,
    #[error("tool run {tool_run_id} not found")]
    ToolRunNotFound { tool_run_id: ToolRunId },
    #[error("tool run {tool_run_id} is already completed")]
    ToolRunAlreadyCompleted { tool_run_id: ToolRunId },
    #[error("unsupported operation in the in-memory stub: {operation}")]
    UnsupportedOperation { operation: &'static str },
    #[error("volume artifact error: {reason}")]
    VolumeArtifact { reason: String },
    #[error("expected a snapshot produced by the same store implementation")]
    IncompatibleSnapshotImplementation,
    #[error(transparent)]
    Encoding(#[from] terracedb::IdEncodingError),
    #[error(transparent)]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("invalid reserved key: {reason}")]
    InvalidKey { reason: String },
    #[error(transparent)]
    Storage(#[from] terracedb::StorageError),
}
