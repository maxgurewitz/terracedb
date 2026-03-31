use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::json;
use terracedb::{Db, SequenceNumber};
use terracedb_debezium::{
    DebeziumMaterializationError, DebeziumMaterializationMode, DebeziumMaterializer,
    DebeziumPartitionedTableLayout, DebeziumRowPredicate, DebeziumSourceTable,
    DebeziumTableFilter, event_log_workflow_source_config,
};
use terracedb_records::{JsonValueCodec, RecordTable, Utf8StringCodec};
use terracedb_workflows::{
    WorkflowSourceAttachMode, WorkflowSourceCapabilities, WorkflowSourceConfig,
    WorkflowSourceRecoveryPolicy,
};

use crate::model::{
    OrderAttentionTransition, OrderAttentionTransitionKind, OrderAttentionView, OrderStatus,
    OrderWatchAlert, OrderWatchOrder, OrderWatchWorkflowMode,
};

pub const WATCHED_REGION: &str = "west";
pub const ATTENTION_ORDERS_TABLE_NAME: &str = "attention_orders";
pub const ATTENTION_TRANSITIONS_TABLE_NAME: &str = "attention_transitions";

pub const FILTERED_OUT_ORDER_ID: &str = "010";
pub const SNAPSHOT_WEST_ORDER_ID: &str = "100";
pub const BACKLOG_ALERT_ORDER_ID: &str = "101";
pub const LIVE_TRANSITION_ORDER_ID: &str = "102";
pub const IGNORED_CUSTOMER_ID: &str = "900";

pub type OrderAttentionOrdersTable =
    RecordTable<String, OrderAttentionView, Utf8StringCodec, JsonValueCodec<OrderAttentionView>>;
pub type OrderAttentionTransitionsTable = RecordTable<
    String,
    OrderAttentionTransition,
    Utf8StringCodec,
    JsonValueCodec<OrderAttentionTransition>,
>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderWatchScenarioStep {
    IgnoredCustomerSnapshot,
    FilteredEastSnapshot,
    RetainedWestSnapshot,
    BacklogWatchedCreate,
    LiveWatchedCreate,
    LiveWatchedExit,
}

impl OrderWatchScenarioStep {
    pub fn description(self) -> &'static str {
        match self {
            Self::IgnoredCustomerSnapshot => {
                "ignore a non-orders Debezium snapshot from the larger commerce database"
            }
            Self::FilteredEastSnapshot => {
                "drop an orders snapshot row that does not match the watched west-region filter"
            }
            Self::RetainedWestSnapshot => {
                "retain a west-region snapshot row in the mirrored current-state and attention view"
            }
            Self::BacklogWatchedCreate => {
                "derive an attention transition before the workflow attaches"
            }
            Self::LiveWatchedCreate => {
                "derive a post-attach attention transition that both workflow modes must see"
            }
            Self::LiveWatchedExit => {
                "remove a retained west-region row after it leaves the watched subset"
            }
        }
    }
}

pub const ORDER_WATCH_SCENARIO: [OrderWatchScenarioStep; 6] = [
    OrderWatchScenarioStep::IgnoredCustomerSnapshot,
    OrderWatchScenarioStep::FilteredEastSnapshot,
    OrderWatchScenarioStep::RetainedWestSnapshot,
    OrderWatchScenarioStep::BacklogWatchedCreate,
    OrderWatchScenarioStep::LiveWatchedCreate,
    OrderWatchScenarioStep::LiveWatchedExit,
];

#[derive(Clone, Debug)]
pub struct OrderWatchBoundary {
    orders_layout: DebeziumPartitionedTableLayout,
    ignored_layout: DebeziumPartitionedTableLayout,
}

impl Default for OrderWatchBoundary {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderWatchBoundary {
    pub fn new() -> Self {
        Self {
            orders_layout: DebeziumPartitionedTableLayout::new(
                "order_watch",
                "commerce.public.orders",
                DebeziumSourceTable::new("commerce", "public", "orders"),
                [0_u32, 1_u32],
            ),
            ignored_layout: DebeziumPartitionedTableLayout::new(
                "order_watch",
                "commerce.public.customers",
                DebeziumSourceTable::new("commerce", "public", "customers"),
                [0_u32],
            ),
        }
    }

    pub fn orders_layout(&self) -> &DebeziumPartitionedTableLayout {
        &self.orders_layout
    }

    pub fn ignored_layout(&self) -> &DebeziumPartitionedTableLayout {
        &self.ignored_layout
    }

    pub fn source_layouts(&self) -> [&DebeziumPartitionedTableLayout; 2] {
        [&self.orders_layout, &self.ignored_layout]
    }

    pub fn watched_region(&self) -> &'static str {
        WATCHED_REGION
    }

    pub fn attention_reason(&self) -> &'static str {
        "watched-west-open"
    }

    pub fn attention_orders_table_name(&self) -> &'static str {
        ATTENTION_ORDERS_TABLE_NAME
    }

    pub fn attention_transitions_table_name(&self) -> &'static str {
        ATTENTION_TRANSITIONS_TABLE_NAME
    }

    pub fn table_filter(&self) -> DebeziumTableFilter {
        DebeziumTableFilter::allow_only([self.orders_layout.source_table().clone()])
    }

    pub fn row_predicate(&self) -> DebeziumRowPredicate {
        DebeziumRowPredicate::ColumnEquals {
            column: "region".to_string(),
            value: json!(WATCHED_REGION),
        }
    }

    pub fn make_materializer(
        &self,
        db: &Db,
        mode: DebeziumMaterializationMode,
    ) -> Result<DebeziumMaterializer, DebeziumMaterializationError> {
        let event_log = terracedb_debezium::DebeziumEventLogTables::from_layouts(
            db,
            self.source_layouts(),
        )?;
        let mirror = terracedb_debezium::DebeziumMirrorTables::from_layouts(
            db,
            self.source_layouts(),
        )?;

        let materializer = match mode {
            DebeziumMaterializationMode::EventLog => DebeziumMaterializer::event_log(event_log),
            DebeziumMaterializationMode::Mirror => DebeziumMaterializer::mirror(mirror),
            DebeziumMaterializationMode::Hybrid => DebeziumMaterializer::hybrid(event_log, mirror),
        };

        Ok(materializer
            .with_table_filter(self.table_filter())
            .with_row_predicate(self.row_predicate()))
    }

    pub fn attention_orders_table(&self, db: &Db) -> OrderAttentionOrdersTable {
        RecordTable::with_codecs(
            db.table(self.attention_orders_table_name().to_string()),
            Utf8StringCodec,
            JsonValueCodec::new(),
        )
    }

    pub fn attention_transitions_table(&self, db: &Db) -> OrderAttentionTransitionsTable {
        RecordTable::with_codecs(
            db.table(self.attention_transitions_table_name().to_string()),
            Utf8StringCodec,
            JsonValueCodec::new(),
        )
    }

    pub fn workflow_source_config(&self, mode: OrderWatchWorkflowMode) -> WorkflowSourceConfig {
        match mode {
            OrderWatchWorkflowMode::HistoricalReplay => event_log_workflow_source_config(),
            OrderWatchWorkflowMode::LiveOnlyAttach => WorkflowSourceConfig::default()
                .with_bootstrap_policy(
                    terracedb_workflows::WorkflowSourceBootstrapPolicy::CurrentDurable,
                )
                .with_recovery_policy(WorkflowSourceRecoveryPolicy::FailClosed)
                .with_capabilities(WorkflowSourceCapabilities::replayable_append_only()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderWatchSourceProgress {
    pub topic: String,
    pub partition: u32,
    pub next_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderWatchOracleSnapshot {
    pub source_offsets: Vec<OrderWatchSourceProgress>,
    pub projection_frontier: BTreeMap<String, SequenceNumber>,
    pub workflow_attach_mode: Option<WorkflowSourceAttachMode>,
    pub orders_current: BTreeMap<String, OrderWatchOrder>,
    pub attention_orders: BTreeMap<String, OrderAttentionView>,
    pub attention_transitions: Vec<OrderAttentionTransition>,
    pub workflow_states: BTreeMap<String, usize>,
    pub outbox_alerts: Vec<OrderWatchAlert>,
}

impl OrderWatchOracleSnapshot {
    pub fn source_offset(&self, topic: &str, partition: u32) -> Option<u64> {
        self.source_offsets
            .iter()
            .find(|progress| progress.topic == topic && progress.partition == partition)
            .map(|progress| progress.next_offset)
    }

    pub fn validate(&self, mode: OrderWatchWorkflowMode) -> Result<(), String> {
        let boundary = OrderWatchBoundary::new();
        let orders_topic = boundary.orders_layout().topic();
        let customers_topic = boundary.ignored_layout().topic();

        expect(
            self.source_offset(orders_topic, 0) == Some(4),
            "orders partition 0 should advance to offset 4",
        )?;
        expect(
            self.source_offset(orders_topic, 1) == Some(1),
            "orders partition 1 should advance to offset 1",
        )?;
        expect(
            self.source_offset(customers_topic, 0) == Some(1),
            "ignored customers topic should still advance its offset",
        )?;

        expect(
            self.workflow_attach_mode
                == Some(match mode {
                    OrderWatchWorkflowMode::HistoricalReplay => {
                        WorkflowSourceAttachMode::Historical
                    }
                    OrderWatchWorkflowMode::LiveOnlyAttach => WorkflowSourceAttachMode::LiveOnly,
                }),
            "workflow attach mode should match the selected profile",
        )?;

        expect(
            !self.orders_current.contains_key(FILTERED_OUT_ORDER_ID),
            "filtered east orders must never materialize into the mirror",
        )?;
        expect(
            !self.orders_current.contains_key(IGNORED_CUSTOMER_ID),
            "ignored non-orders rows must never materialize into the mirror",
        )?;
        expect(
            self.orders_current.contains_key(SNAPSHOT_WEST_ORDER_ID),
            "west snapshot row should seed the mirrored current-state",
        )?;
        expect(
            self.orders_current.contains_key(BACKLOG_ALERT_ORDER_ID),
            "backlog west create should still be visible in the mirror",
        )?;
        expect(
            !self.orders_current.contains_key(LIVE_TRANSITION_ORDER_ID),
            "rows that leave the watched subset should be removed from the mirror",
        )?;

        expect(
            self.attention_orders.contains_key(SNAPSHOT_WEST_ORDER_ID),
            "west snapshot row should appear in the attention read model",
        )?;
        expect(
            self.attention_orders.contains_key(BACKLOG_ALERT_ORDER_ID),
            "backlog west create should appear in the attention read model",
        )?;
        expect(
            !self.attention_orders.contains_key(FILTERED_OUT_ORDER_ID),
            "filtered east rows must never appear in the attention read model",
        )?;
        expect(
            !self.attention_orders.contains_key(LIVE_TRANSITION_ORDER_ID),
            "rows that leave the watched subset should be removed from attention outputs",
        )?;

        for (order_id, attention) in &self.attention_orders {
            let Some(order) = self.orders_current.get(order_id) else {
                return Err(format!(
                    "attention row {order_id} is missing from the mirrored current-state"
                ));
            };
            expect(
                order.region == WATCHED_REGION && order.status == OrderStatus::Open,
                "attention outputs must stay aligned with open west-region mirrored orders",
            )?;
            expect(
                attention.reason == boundary.attention_reason(),
                "attention outputs should use the documented reason string",
            )?;
        }

        expect(
            self.attention_transitions
                == vec![
                    OrderAttentionTransition {
                        order_id: BACKLOG_ALERT_ORDER_ID.to_string(),
                        kind: OrderAttentionTransitionKind::Entered,
                        source_partition: 0,
                        source_offset: 1,
                    },
                    OrderAttentionTransition {
                        order_id: LIVE_TRANSITION_ORDER_ID.to_string(),
                        kind: OrderAttentionTransitionKind::Entered,
                        source_partition: 0,
                        source_offset: 2,
                    },
                    OrderAttentionTransition {
                        order_id: LIVE_TRANSITION_ORDER_ID.to_string(),
                        kind: OrderAttentionTransitionKind::Exited,
                        source_partition: 0,
                        source_offset: 3,
                    },
                ],
            "derived attention transitions should match the frozen order-watch scenario",
        )?;

        let backlog_seen = self
            .outbox_alerts
            .iter()
            .any(|alert| alert.order_id == BACKLOG_ALERT_ORDER_ID);
        expect(
            backlog_seen
                == matches!(mode, OrderWatchWorkflowMode::HistoricalReplay),
            "historical mode should replay backlog alerts and live-only mode should skip them",
        )?;

        expect(
            self.workflow_states.get(LIVE_TRANSITION_ORDER_ID) == Some(&2),
            "live order should produce one enter and one exit alert",
        )?;
        expect(
            self.workflow_states.get(BACKLOG_ALERT_ORDER_ID)
                == Some(&match mode {
                    OrderWatchWorkflowMode::HistoricalReplay => 1,
                    OrderWatchWorkflowMode::LiveOnlyAttach => 0,
                }),
            "backlog order should only be counted by the historical workflow profile",
        )?;

        let historical_frontier = boundary
            .orders_layout()
            .cdc_table_names()
            .into_iter()
            .all(|name| self.projection_frontier.contains_key(&name));
        expect(
            historical_frontier,
            "projection frontier should track every orders CDC partition",
        )?;
        Ok(())
    }
}

fn expect(condition: bool, message: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
}
