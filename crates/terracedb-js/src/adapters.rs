use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb_vfs::VolumeId;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxJsRuntimeBinding {
    pub session_volume_id: VolumeId,
    pub workspace_root: String,
    pub capability_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxJsRuntimeRequest {
    pub binding: SandboxJsRuntimeBinding,
    pub entrypoint: String,
    pub metadata: BTreeMap<String, JsonValue>,
}
