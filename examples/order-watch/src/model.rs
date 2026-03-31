use serde::{Deserialize, Serialize};
use terracedb_debezium::{
    DebeziumDataError, DebeziumDerivedTransition, DebeziumDerivedTransitionKind,
};

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

impl OrderAttentionTransition {
    pub fn from_debezium_transition(
        transition: &DebeziumDerivedTransition,
    ) -> Result<Self, DebeziumDataError> {
        Ok(Self {
            order_id: transition.primary_key.require_string_or_number("id")?,
            kind: match transition.kind {
                DebeziumDerivedTransitionKind::Entered => OrderAttentionTransitionKind::Entered,
                DebeziumDerivedTransitionKind::Exited => OrderAttentionTransitionKind::Exited,
            },
            source_partition: transition.kafka.partition,
            source_offset: transition.kafka.offset,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderWatchAlert {
    pub order_id: String,
    pub kind: OrderAttentionTransitionKind,
    pub source_partition: u32,
    pub source_offset: u64,
}

impl From<OrderAttentionTransition> for OrderWatchAlert {
    fn from(value: OrderAttentionTransition) -> Self {
        Self {
            order_id: value.order_id,
            kind: value.kind,
            source_partition: value.source_partition,
            source_offset: value.source_offset,
        }
    }
}
