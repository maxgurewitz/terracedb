use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderWatchWorkflowMode {
    HistoricalReplay,
    LiveOnlyAttach,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderStatus {
    Open,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderWatchOrder {
    pub order_id: String,
    pub region: String,
    pub status: OrderStatus,
    pub source_partition: u32,
    pub source_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderAttentionView {
    pub order_id: String,
    pub reason: String,
    pub source_partition: u32,
    pub source_offset: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderAttentionTransitionKind {
    Entered,
    Exited,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderAttentionTransition {
    pub order_id: String,
    pub kind: OrderAttentionTransitionKind,
    pub source_partition: u32,
    pub source_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderWatchAlert {
    pub order_id: String,
    pub kind: OrderAttentionTransitionKind,
    pub source_partition: u32,
    pub source_offset: u64,
}
