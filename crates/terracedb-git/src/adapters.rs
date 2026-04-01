use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb_vfs::VolumeId;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxGitBinding {
    pub session_volume_id: VolumeId,
    pub repository_root: String,
    pub workspace_root: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxGitRequest {
    pub binding: SandboxGitBinding,
    pub operation: String,
    pub metadata: BTreeMap<String, JsonValue>,
}
