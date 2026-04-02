use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use terracedb::{
    ColocatedDeployment, ColocatedDeploymentError, DomainBackgroundBudget, DomainCpuBudget,
    ExecutionDomainBudget,
};

pub const CHAT_ROOMS_DATABASE_NAME: &str = "chat-rooms";
pub const CHAT_ROOMS_SERVER_PORT: u16 = 9604;
pub const ROOM_STATE_TABLE_NAME: &str = "room_state";
pub const ROOM_MESSAGES_TABLE_NAME: &str = "room_messages";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatRoomsExampleProfile {
    #[default]
    DatabaseShared,
    ShardLocal,
}

impl ChatRoomsExampleProfile {
    pub fn deployment(self) -> Result<ColocatedDeployment, ColocatedDeploymentError> {
        ColocatedDeployment::shard_ready(process_budget(), CHAT_ROOMS_DATABASE_NAME)
    }

    pub const fn env_value(self) -> &'static str {
        match self {
            Self::DatabaseShared => "database_shared",
            Self::ShardLocal => "shard_local",
        }
    }

    pub const fn uses_shard_local_execution(self) -> bool {
        matches!(self, Self::ShardLocal)
    }
}

impl fmt::Display for ChatRoomsExampleProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.env_value())
    }
}

impl FromStr for ChatRoomsExampleProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "database_shared" => Ok(Self::DatabaseShared),
            "shard_local" => Ok(Self::ShardLocal),
            other => Err(format!(
                "unknown chat rooms profile '{other}'; expected 'database_shared' or 'shard_local'"
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoomStateRecord {
    pub room_id: String,
    pub title: String,
    pub created_at_ms: u64,
    pub message_count: u64,
    pub last_message_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatMessageRecord {
    pub room_id: String,
    pub message_id: String,
    pub author: String,
    pub body: String,
    pub posted_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateRoomRequest {
    pub room_id: String,
    pub title: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostMessageRequest {
    pub author: String,
    pub body: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub struct RecentMessagesQuery {
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoomMessagesResponse {
    pub room: RoomStateRecord,
    pub messages: Vec<ChatMessageRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoomBacklogView {
    pub queued_work_items: u64,
    pub queued_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VirtualPartitionAssignment {
    pub virtual_partition: u32,
    pub physical_shard: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReshardTableStateView {
    pub table_name: String,
    pub source_revision: u64,
    pub target_revision: u64,
    pub phase: String,
    pub paused_partitions: Vec<u32>,
    pub published_revision: Option<u64>,
    pub failure: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoomShardInspection {
    pub room_id: String,
    pub title: String,
    pub message_count: u64,
    pub virtual_partition: u32,
    pub physical_shard: u32,
    pub shard_map_revision: u64,
    pub preferred_foreground_domain: String,
    pub preferred_background_domain: String,
    pub preferred_control_plane_domain: String,
    pub active_background_domain: String,
    pub active_background_backlog: RoomBacklogView,
    pub room_state_resharding: Option<ReshardTableStateView>,
    pub room_messages_resharding: Option<ReshardTableStateView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardObservabilityView {
    pub physical_shard: u32,
    pub preferred_foreground_domain: String,
    pub preferred_background_domain: String,
    pub preferred_control_plane_domain: String,
    pub active_background_domain: String,
    pub background_domain_registered: bool,
    pub active_background_backlog: RoomBacklogView,
    pub room_count: usize,
    pub room_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatRoomsObservabilityResponse {
    pub profile: ChatRoomsExampleProfile,
    pub database_background_domain: String,
    pub database_background_backlog: RoomBacklogView,
    pub shard_assignments: Vec<VirtualPartitionAssignment>,
    pub rooms: Vec<RoomShardInspection>,
    pub shards: Vec<ShardObservabilityView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReshardRoomRequest {
    pub target_physical_shard: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReshardRoomResponse {
    pub room_id: String,
    pub virtual_partition: u32,
    pub from_physical_shard: u32,
    pub to_physical_shard: u32,
    pub room_state: ReshardTableStateView,
    pub room_messages: ReshardTableStateView,
}

fn process_budget() -> ExecutionDomainBudget {
    ExecutionDomainBudget {
        cpu: DomainCpuBudget {
            worker_slots: Some(8),
            weight: None,
        },
        background: DomainBackgroundBudget {
            task_slots: Some(8),
            max_in_flight_bytes: Some(128 * 1024),
        },
        ..ExecutionDomainBudget::default()
    }
}
