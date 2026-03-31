use std::{collections::BTreeMap, io};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb::Value;
use terracedb_debezium::{
    DebeziumDataError, DebeziumDerivedTransition, DebeziumDerivedTransitionKind, DebeziumMirrorRow,
    DebeziumRowExt,
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

impl OrderWatchOrder {
    pub fn from_mirror_row(row: &DebeziumMirrorRow) -> Result<Self, io::Error> {
        Ok(Self {
            order_id: row
                .primary_key
                .require_string_or_number("id")
                .map_err(io::Error::other)?,
            region: row
                .values
                .require_string("region")
                .map(str::to_string)
                .map_err(io::Error::other)?,
            status: decode_order_status(&row.values)?,
            source_partition: row.kafka.partition,
            source_offset: row.kafka.offset,
        })
    }

    pub fn from_current_value(value: &Value) -> Result<Self, io::Error> {
        let row = DebeziumMirrorRow::from_value(value).map_err(io::Error::other)?;
        Self::from_mirror_row(&row)
    }
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

fn decode_order_status(values: &BTreeMap<String, JsonValue>) -> Result<OrderStatus, io::Error> {
    match values.require_string("status").map_err(io::Error::other)? {
        "open" => Ok(OrderStatus::Open),
        "closed" => Ok(OrderStatus::Closed),
        other => Err(io::Error::other(format!("unknown order status `{other}`"))),
    }
}
