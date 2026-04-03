use crate::SandboxError;

pub(crate) fn git_substrate_error_to_sandbox(
    error: terracedb_git::GitSubstrateError,
) -> SandboxError {
    match error {
        terracedb_git::GitSubstrateError::ExportConflict { path, message } => {
            SandboxError::Io { path, message }
        }
        terracedb_git::GitSubstrateError::RemotePushFailed {
            remote,
            branch,
            message,
        } => SandboxError::Service {
            service: "git",
            message: format!("remote push failed for {remote}/{branch}: {message}"),
        },
        terracedb_git::GitSubstrateError::PullRequestProvider { provider, message } => {
            SandboxError::Service {
                service: "git",
                message: format!("pull-request provider {provider} failed: {message}"),
            }
        }
        terracedb_git::GitSubstrateError::Bridge { operation, message } => SandboxError::Service {
            service: "git",
            message: format!("{operation}: {message}"),
        },
        terracedb_git::GitSubstrateError::Cancelled { repository_id } => SandboxError::Service {
            service: "git",
            message: format!("cancelled: {repository_id}"),
        },
        terracedb_git::GitSubstrateError::NoChanges { target_ref } => {
            SandboxError::NoGitChanges { target_ref }
        }
        terracedb_git::GitSubstrateError::RepositoryNotFound { path } => SandboxError::Io {
            path,
            message: "git repository was not found".to_string(),
        },
        terracedb_git::GitSubstrateError::RepositoryReadOnly { repository_id } => {
            SandboxError::Service {
                service: "git",
                message: format!("repository is read-only: {repository_id}"),
            }
        }
        terracedb_git::GitSubstrateError::RepositoryImageDescriptorMismatch {
            field,
            expected,
            found,
        } => SandboxError::Service {
            service: "git",
            message: format!(
                "repository image descriptor mismatch for {field}: expected {expected}, found {found}"
            ),
        },
        terracedb_git::GitSubstrateError::ReferenceNotFound { reference } => {
            SandboxError::Service {
                service: "git",
                message: format!("reference not found: {reference}"),
            }
        }
        terracedb_git::GitSubstrateError::ReferenceConflict {
            reference,
            expected,
            found,
        } => SandboxError::Service {
            service: "git",
            message: format!(
                "reference conflict for {reference}: expected {expected:?}, found {found:?}"
            ),
        },
        terracedb_git::GitSubstrateError::ObjectNotFound { oid } => SandboxError::Service {
            service: "git",
            message: format!("object not found: {oid}"),
        },
        terracedb_git::GitSubstrateError::InvalidObject { oid, message } => SandboxError::Service {
            service: "git",
            message: format!("invalid object {oid}: {message}"),
        },
        terracedb_git::GitSubstrateError::SerdeJson(error) => SandboxError::SerdeJson(error),
        terracedb_git::GitSubstrateError::Vfs(error) => SandboxError::Vfs(error),
    }
}
