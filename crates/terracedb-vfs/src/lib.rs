pub mod activity;
pub mod error;
pub mod filesystem;
pub mod ids;
pub mod kv;
pub mod store;
pub mod tables;
pub mod tool_runs;

pub use activity::{
    ActivityEntry, ActivityKind, ActivityOptions, ActivityReceiver, ActivityStream,
};
pub use error::AgentFsError;
pub use filesystem::{
    AgentFileSystem, CreateOptions, DirEntry, DirEntryPlus, FileKind, MkdirOptions,
    ReadOnlyAgentFileSystem, Stats,
};
pub use ids::{
    ActivityId, ActivityKey, AllocatorKey, AllocatorKind, ChunkKey, DentryKey, InodeId, InodeKey,
    KvKey, OriginKey, SymlinkKey, ToolRunId, ToolRunKey, VolumeId, VolumeKey, WhiteoutKey,
};
pub use kv::{AgentKvStore, ReadOnlyAgentKvStore};
pub use serde_json::Value as JsonValue;
pub use store::{
    AgentFsConfig, AgentFsOverlay, AgentFsSnapshot, AgentFsStore, AgentFsVolume, AgentFsVolumeInfo,
    CloneVolumeSource, DEFAULT_CHUNK_SIZE, InMemoryAgentFsStore, OverlayBaseDescriptor,
    ROOT_INODE_ID, SnapshotOptions, VFS_FORMAT_VERSION,
};
pub use tables::{
    RESERVED_TABLES, ReservedTableDescriptor, VFS_ACTIVITY_TABLE_NAME, VFS_ALLOCATOR_TABLE_NAME,
    VFS_CHUNK_TABLE_NAME, VFS_DENTRY_TABLE_NAME, VFS_INODE_TABLE_NAME, VFS_KV_TABLE_NAME,
    VFS_ORIGIN_TABLE_NAME, VFS_SYMLINK_TABLE_NAME, VFS_TOOL_RUN_TABLE_NAME, VFS_VOLUME_TABLE_NAME,
    VFS_WHITEOUT_TABLE_NAME, reserved_table, reserved_table_configs, reserved_table_descriptors,
};
pub use tool_runs::{
    AgentToolRuns, CompletedToolRun, CompletedToolRunOutcome, ReadOnlyAgentToolRuns, ToolRun,
    ToolRunStatus,
};
