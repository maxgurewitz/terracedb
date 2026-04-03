pub mod adapters;
pub mod bridge;
pub mod error;
pub mod host;
pub mod index;
pub mod object;
pub mod refs;
pub mod store;
pub mod types;
pub mod worktree;

pub use adapters::{SandboxGitBinding, SandboxGitRequest};
pub use bridge::{
    DeterministicGitHostBridge, GitHostBridge, GitImportEntry, GitImportEntryKind, GitImportMode,
    GitImportReport, GitImportRequest, GitImportSource, GitRemoteProvider,
};
pub use error::GitSubstrateError;
pub use host::HostGitBridge;
pub use index::GitIndexStore;
pub use object::{GitObjectDatabase, GitObjectKind};
pub use refs::GitRefDatabase;
pub use store::{
    DeterministicGitRepositoryStore, GitCancellationToken, GitExecutionHooks, GitRepository,
    GitRepositoryImage, GitRepositoryStore, NeverCancel, NoopGitExecutionHooks,
    VfsGitRepositoryImage,
};
pub use types::{
    GitAmbientDefault, GitCheckoutReport, GitCheckoutRequest, GitCommitReport, GitCommitRequest,
    GitDiffEntry, GitDiffKind, GitDiffReport, GitDiffRequest, GitDiscoverReport,
    GitDiscoverRequest, GitExportReport, GitExportRequest, GitForkPolicy, GitForkSurface,
    GitForkSurfacePolicy, GitHeadState, GitIndexEntry, GitIndexSnapshot, GitObject,
    GitObjectFormat, GitOpenRequest, GitOwnershipMode, GitPullRequestReport, GitPullRequestRequest,
    GitPushReport, GitPushRequest, GitRefUpdate, GitRefUpdateReport, GitReference,
    GitRepositoryHandle, GitRepositoryImageDescriptor, GitRepositoryOrigin, GitRepositoryPolicy,
    GitRepositoryProvenance, GitStatusEntry, GitStatusKind, GitStatusOptions, GitStatusReport,
    GitTraceEvent, GitTracePhase,
};

pub use serde_json::Value as JsonValue;
