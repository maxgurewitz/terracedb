pub mod adapters;
pub mod bridge;
pub mod error;
pub mod index;
pub mod object;
pub mod refs;
pub mod store;
pub mod types;
pub mod worktree;

pub use adapters::{SandboxGitBinding, SandboxGitRequest};
pub use bridge::{DeterministicGitHostBridge, GitHostBridge, GitImportReport, GitImportRequest};
pub use error::GitSubstrateError;
pub use index::GitIndexStore;
pub use object::{GitObjectDatabase, GitObjectKind};
pub use refs::GitRefDatabase;
pub use store::{
    DeterministicGitRepositoryStore, GitCancellationToken, GitExecutionHooks, GitRepository,
    GitRepositoryImage, GitRepositoryStore, NeverCancel, NoopGitExecutionHooks,
    VfsGitRepositoryImage,
};
pub use types::{
    GitAmbientDefault, GitCheckoutReport, GitCheckoutRequest, GitDiscoverReport,
    GitDiscoverRequest, GitExportReport, GitExportRequest, GitForkPolicy, GitForkSurface,
    GitForkSurfacePolicy, GitHeadState, GitIndexEntry, GitIndexSnapshot, GitObject, GitOpenRequest,
    GitOwnershipMode, GitPullRequestReport, GitPullRequestRequest, GitPushReport, GitPushRequest,
    GitReference, GitRepositoryHandle, GitRepositoryImageDescriptor, GitRepositoryPolicy,
    GitRepositoryProvenance, GitStatusEntry, GitStatusKind, GitStatusReport, GitTraceEvent,
    GitTracePhase,
};

pub use serde_json::Value as JsonValue;
