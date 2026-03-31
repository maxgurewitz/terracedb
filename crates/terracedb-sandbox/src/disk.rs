use serde::{Deserialize, Serialize};

use crate::ConflictPolicy;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoistMode {
    DirectorySnapshot,
    GitHead,
    GitWorkingTree {
        include_untracked: bool,
        include_ignored: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoistRequest {
    pub source_path: String,
    pub target_root: String,
    pub mode: HoistMode,
    pub delete_missing: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EjectMode {
    MaterializeSnapshot,
    ApplyDelta,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EjectRequest {
    pub target_path: String,
    pub mode: EjectMode,
    pub conflict_policy: ConflictPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictEntry {
    pub path: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictReport {
    pub conflicts: Vec<ConflictEntry>,
}
