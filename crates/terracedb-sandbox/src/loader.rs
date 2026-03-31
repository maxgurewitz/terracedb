use crate::SandboxError;

pub const TERRACE_WORKSPACE_PREFIX: &str = "terrace:/workspace";
pub const HOST_CAPABILITY_PREFIX: &str = "terrace:host/";
pub const SANDBOX_FS_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/fs";
pub const SANDBOX_BASH_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/bash";
pub const SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER: &str = "@terracedb/sandbox/typescript";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SandboxModuleSpecifier {
    Workspace { path: String },
    HostCapability { name: String },
    Npm { package: String },
    NodeBuiltin { builtin: String },
    BuiltinLibrary { specifier: String },
}

impl SandboxModuleSpecifier {
    pub fn parse(specifier: &str) -> Result<Self, SandboxError> {
        if let Some(path) = specifier.strip_prefix(TERRACE_WORKSPACE_PREFIX) {
            let normalized = if path.is_empty() {
                "/workspace".to_string()
            } else {
                format!("/workspace{path}")
            };
            return Ok(Self::Workspace { path: normalized });
        }

        if let Some(name) = specifier.strip_prefix(HOST_CAPABILITY_PREFIX) {
            if name.is_empty() {
                return Err(SandboxError::InvalidModuleSpecifier {
                    specifier: specifier.to_string(),
                });
            }
            return Ok(Self::HostCapability {
                name: name.to_string(),
            });
        }

        if let Some(package) = specifier.strip_prefix("npm:") {
            if package.is_empty() {
                return Err(SandboxError::InvalidModuleSpecifier {
                    specifier: specifier.to_string(),
                });
            }
            return Ok(Self::Npm {
                package: package.to_string(),
            });
        }

        if let Some(builtin) = specifier.strip_prefix("node:") {
            if builtin.is_empty() {
                return Err(SandboxError::InvalidModuleSpecifier {
                    specifier: specifier.to_string(),
                });
            }
            return Ok(Self::NodeBuiltin {
                builtin: builtin.to_string(),
            });
        }

        if matches!(
            specifier,
            SANDBOX_FS_LIBRARY_SPECIFIER
                | SANDBOX_BASH_LIBRARY_SPECIFIER
                | SANDBOX_TYPESCRIPT_LIBRARY_SPECIFIER
        ) {
            return Ok(Self::BuiltinLibrary {
                specifier: specifier.to_string(),
            });
        }

        Err(SandboxError::InvalidModuleSpecifier {
            specifier: specifier.to_string(),
        })
    }
}
