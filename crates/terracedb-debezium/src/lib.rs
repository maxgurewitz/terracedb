use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map as JsonMap, Value as JsonValue};
use terracedb::{Db, Key, Table, Transaction, Value};
use terracedb_kafka::{
    KafkaAppendOnlyTables, KafkaBatchHandler, KafkaMaterializationError, KafkaRecord,
    KafkaTopicPartition,
};
use terracedb_workflows::{
    WorkflowSource, WorkflowSourceBootstrapPolicy, WorkflowSourceCapabilities,
    WorkflowSourceConfig, WorkflowSourceRecoveryPolicy,
};
use thiserror::Error;

const PRIMARY_KEY_FORMAT_VERSION: u8 = 1;
const NORMALIZED_EVENT_FORMAT_VERSION: u8 = 1;
const MIRROR_ROW_FORMAT_VERSION: u8 = 1;

pub type DebeziumRow = BTreeMap<String, JsonValue>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DebeziumConnectorKind {
    PostgreSql,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DebeziumOperationKind {
    Create,
    Update,
    Delete,
    Read,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DebeziumEventKind {
    Change { operation: DebeziumOperationKind },
    Tombstone,
}

impl DebeziumEventKind {
    pub fn operation(&self) -> Option<DebeziumOperationKind> {
        match self {
            Self::Change { operation } => Some(*operation),
            Self::Tombstone => None,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DebeziumSnapshotMarker {
    Initial,
    Last,
    Incremental,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DebeziumSourceTable {
    pub database: String,
    pub schema: String,
    pub table: String,
}

impl DebeziumSourceTable {
    pub fn new(
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }
}

impl fmt::Display for DebeziumSourceTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.database, self.schema, self.table)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebeziumKafkaCoordinates {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub timestamp_millis: Option<u64>,
}

impl DebeziumKafkaCoordinates {
    fn from_record(record: &KafkaRecord) -> Self {
        Self {
            topic: record.topic_partition.topic.clone(),
            partition: record.topic_partition.partition.get(),
            offset: record.offset.get(),
            timestamp_millis: record.timestamp_millis,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebeziumPostgresSourceMetadata {
    pub lsn: Option<u64>,
    pub tx_id: Option<u64>,
    pub xmin: Option<u64>,
    pub source_ts_ms: Option<u64>,
    pub payload_ts_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebeziumTransactionMetadata {
    pub id: String,
    pub total_order: Option<u64>,
    pub data_collection_order: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DebeziumPrimaryKey {
    pub fields: DebeziumRow,
}

impl DebeziumPrimaryKey {
    pub fn new(fields: DebeziumRow) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &DebeziumRow {
        &self.fields
    }

    pub fn encode(&self) -> Result<Key, DebeziumCodecError> {
        encode_versioned_json(PRIMARY_KEY_FORMAT_VERSION, "primary key", self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DebeziumCodecError> {
        decode_versioned_json(PRIMARY_KEY_FORMAT_VERSION, "primary key", bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DebeziumEvent {
    pub connector: DebeziumConnectorKind,
    pub source_table: DebeziumSourceTable,
    pub kind: DebeziumEventKind,
    pub snapshot: Option<DebeziumSnapshotMarker>,
    pub kafka: DebeziumKafkaCoordinates,
    pub primary_key: DebeziumPrimaryKey,
    pub before: Option<DebeziumRow>,
    pub after: Option<DebeziumRow>,
    pub source: DebeziumPostgresSourceMetadata,
    pub transaction: Option<DebeziumTransactionMetadata>,
}

impl DebeziumEvent {
    pub fn operation(&self) -> Option<DebeziumOperationKind> {
        self.kind.operation()
    }

    pub fn is_snapshot(&self) -> bool {
        self.snapshot.is_some()
    }

    pub fn is_tombstone(&self) -> bool {
        self.kind.is_tombstone()
    }

    pub fn row(&self, image: DebeziumRowImage) -> Option<&DebeziumRow> {
        match image {
            DebeziumRowImage::Before => self.before.as_ref(),
            DebeziumRowImage::After => self.after.as_ref(),
        }
    }

    pub fn to_value(&self) -> Result<Value, DebeziumCodecError> {
        Ok(Value::bytes(encode_versioned_json(
            NORMALIZED_EVENT_FORMAT_VERSION,
            "normalized event",
            self,
        )?))
    }

    pub fn from_value(value: &Value) -> Result<Self, DebeziumCodecError> {
        let bytes = expect_bytes(value, "normalized event")?;
        decode_versioned_json(NORMALIZED_EVENT_FORMAT_VERSION, "normalized event", bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DebeziumMirrorRow {
    pub source_table: DebeziumSourceTable,
    pub primary_key: DebeziumPrimaryKey,
    pub values: DebeziumRow,
    pub snapshot: Option<DebeziumSnapshotMarker>,
    pub kafka: DebeziumKafkaCoordinates,
    pub transaction: Option<DebeziumTransactionMetadata>,
}

impl DebeziumMirrorRow {
    pub fn to_value(&self) -> Result<Value, DebeziumCodecError> {
        Ok(Value::bytes(encode_versioned_json(
            MIRROR_ROW_FORMAT_VERSION,
            "mirror row",
            self,
        )?))
    }

    pub fn from_value(value: &Value) -> Result<Self, DebeziumCodecError> {
        let bytes = expect_bytes(value, "mirror row")?;
        decode_versioned_json(MIRROR_ROW_FORMAT_VERSION, "mirror row", bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DebeziumRowImage {
    Before,
    After,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebeziumRowMembership {
    pub before: bool,
    pub after: bool,
}

impl DebeziumRowMembership {
    pub fn any(self) -> bool {
        self.before || self.after
    }

    pub fn entered(self) -> bool {
        !self.before && self.after
    }

    pub fn exited(self) -> bool {
        self.before && !self.after
    }

    pub fn stayed_in(self) -> bool {
        self.before && self.after
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DebeziumTableFilter {
    #[default]
    All,
    Only {
        tables: BTreeSet<DebeziumSourceTable>,
    },
}

impl DebeziumTableFilter {
    pub fn allow_only<I>(tables: I) -> Self
    where
        I: IntoIterator<Item = DebeziumSourceTable>,
    {
        Self::Only {
            tables: tables.into_iter().collect(),
        }
    }

    pub fn matches(&self, source_table: &DebeziumSourceTable) -> bool {
        match self {
            Self::All => true,
            Self::Only { tables } => tables.contains(source_table),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DebeziumRowPredicate {
    #[default]
    MatchAll,
    MatchNone,
    And {
        predicates: Vec<DebeziumRowPredicate>,
    },
    Or {
        predicates: Vec<DebeziumRowPredicate>,
    },
    Not {
        predicate: Box<DebeziumRowPredicate>,
    },
    ColumnEquals {
        column: String,
        value: JsonValue,
    },
    PrimaryKeyEquals {
        column: String,
        value: JsonValue,
    },
    OperationIs {
        operation: DebeziumOperationKind,
    },
    SnapshotIs {
        snapshot: bool,
    },
}

impl DebeziumRowPredicate {
    pub fn matches_image(&self, event: &DebeziumEvent, image: DebeziumRowImage) -> bool {
        let row = event.row(image);
        match self {
            Self::MatchAll => row.is_some(),
            Self::MatchNone => false,
            Self::And { predicates } => predicates
                .iter()
                .all(|predicate| predicate.matches_image(event, image)),
            Self::Or { predicates } => predicates
                .iter()
                .any(|predicate| predicate.matches_image(event, image)),
            Self::Not { predicate } => row.is_some() && !predicate.matches_image(event, image),
            Self::ColumnEquals { column, value } => row
                .and_then(|row| row.get(column))
                .map(|field| field == value)
                .unwrap_or(false),
            Self::PrimaryKeyEquals { column, value } => {
                row.is_some()
                    && event
                        .primary_key
                        .fields()
                        .get(column)
                        .map(|field| field == value)
                        .unwrap_or(false)
            }
            Self::OperationIs { operation } => {
                row.is_some() && event.operation() == Some(*operation)
            }
            Self::SnapshotIs { snapshot } => row.is_some() && event.is_snapshot() == *snapshot,
        }
    }

    pub fn membership(&self, event: &DebeziumEvent) -> DebeziumRowMembership {
        DebeziumRowMembership {
            before: self.matches_image(event, DebeziumRowImage::Before),
            after: self.matches_image(event, DebeziumRowImage::After),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DebeziumColumnProjection {
    pub include: Option<BTreeSet<String>>,
    pub redactions: BTreeMap<String, JsonValue>,
}

impl DebeziumColumnProjection {
    pub fn include_only<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.include = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_redaction(mut self, column: impl Into<String>, value: JsonValue) -> Self {
        self.redactions.insert(column.into(), value);
        self
    }

    pub fn project_row(&self, row: &DebeziumRow) -> DebeziumRow {
        let mut projected = match &self.include {
            Some(columns) => row
                .iter()
                .filter(|(column, _)| columns.contains(*column))
                .map(|(column, value)| (column.clone(), value.clone()))
                .collect(),
            None => row.clone(),
        };

        for (column, value) in &self.redactions {
            projected.insert(column.clone(), value.clone());
        }

        projected
    }

    pub fn project_event(&self, event: &DebeziumEvent) -> DebeziumEvent {
        let mut projected = event.clone();
        projected.before = event.before.as_ref().map(|row| self.project_row(row));
        projected.after = event.after.as_ref().map(|row| self.project_row(row));
        projected
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DebeziumMaterializationMode {
    EventLog,
    Mirror,
    Hybrid,
}

impl DebeziumMaterializationMode {
    pub fn writes_event_log(self) -> bool {
        matches!(self, Self::EventLog | Self::Hybrid)
    }

    pub fn writes_mirror(self) -> bool {
        matches!(self, Self::Mirror | Self::Hybrid)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DebeziumWorkflowEventPolicy {
    AllEvents,
    SkipSnapshots,
    SkipSnapshotsAndTombstones,
    DerivedTransitionTables,
}

impl DebeziumWorkflowEventPolicy {
    pub fn accepts(self, event: &DebeziumEvent) -> bool {
        match self {
            Self::AllEvents => true,
            Self::SkipSnapshots => !event.is_snapshot(),
            Self::SkipSnapshotsAndTombstones => !event.is_snapshot() && !event.is_tombstone(),
            Self::DerivedTransitionTables => false,
        }
    }

    pub fn prefers_derived_tables(self) -> bool {
        matches!(self, Self::DerivedTransitionTables)
    }
}

pub fn event_log_workflow_source_config() -> WorkflowSourceConfig {
    WorkflowSourceConfig::default()
        .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::Beginning)
        .with_recovery_policy(WorkflowSourceRecoveryPolicy::ReplayFromHistory)
        .with_capabilities(WorkflowSourceCapabilities::replayable_append_only())
}

pub fn mirror_workflow_source_config() -> WorkflowSourceConfig {
    WorkflowSourceConfig::default()
        .with_bootstrap_policy(WorkflowSourceBootstrapPolicy::CurrentDurable)
        .with_recovery_policy(WorkflowSourceRecoveryPolicy::FailClosed)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebeziumPartitionedTableLayout {
    topic: String,
    source_table: DebeziumSourceTable,
    cdc_partitions: BTreeMap<u32, String>,
    current: String,
}

impl DebeziumPartitionedTableLayout {
    pub fn new<I>(
        prefix: impl Into<String>,
        topic: impl Into<String>,
        source_table: DebeziumSourceTable,
        partitions: I,
    ) -> Self
    where
        I: IntoIterator<Item = u32>,
    {
        let prefix = sanitize_identifier(&prefix.into());
        let table_stem = format!(
            "{}_{}_{}_{}",
            prefix,
            sanitize_identifier(&source_table.database),
            sanitize_identifier(&source_table.schema),
            sanitize_identifier(&source_table.table)
        );
        let cdc_partitions = partitions
            .into_iter()
            .map(|partition| (partition, format!("{table_stem}_cdc_p{partition:04}")))
            .collect();

        Self {
            topic: topic.into(),
            source_table,
            cdc_partitions,
            current: format!("{table_stem}_current"),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn source_table(&self) -> &DebeziumSourceTable {
        &self.source_table
    }

    pub fn cdc_table_name(&self, partition: u32) -> Option<&str> {
        self.cdc_partitions.get(&partition).map(String::as_str)
    }

    pub fn cdc_table_names(&self) -> Vec<String> {
        self.cdc_partitions.values().cloned().collect()
    }

    pub fn current_table_name(&self) -> &str {
        &self.current
    }

    pub fn projection_sources(&self, db: &Db) -> Vec<Table> {
        self.cdc_partitions
            .values()
            .map(|name| db.table(name.clone()))
            .collect()
    }

    pub fn event_log_workflow_sources(&self, db: &Db) -> Vec<WorkflowSource> {
        self.projection_sources(db)
            .into_iter()
            .map(|table| WorkflowSource::new(table).with_config(event_log_workflow_source_config()))
            .collect()
    }

    pub fn mirror_workflow_source(&self, db: &Db) -> WorkflowSource {
        WorkflowSource::new(db.table(self.current.clone()))
            .with_config(mirror_workflow_source_config())
    }
}

#[derive(Clone, Debug)]
pub struct DebeziumEventLogTables {
    by_source: BTreeMap<DebeziumSourceTable, KafkaAppendOnlyTables>,
}

impl DebeziumEventLogTables {
    pub fn one_table_per_partition<I>(bindings: I) -> Result<Self, DebeziumMaterializationError>
    where
        I: IntoIterator<Item = (DebeziumSourceTable, KafkaTopicPartition, Table)>,
    {
        let mut grouped = BTreeMap::<DebeziumSourceTable, Vec<(KafkaTopicPartition, Table)>>::new();
        for (source_table, topic_partition, table) in bindings {
            grouped
                .entry(source_table)
                .or_default()
                .push((topic_partition, table));
        }

        let mut by_source = BTreeMap::new();
        for (source_table, tables) in grouped {
            let append_only =
                KafkaAppendOnlyTables::one_table_per_partition(tables).map_err(|source| {
                    DebeziumMaterializationError::EventLogLayout {
                        source_table: source_table.to_string(),
                        source,
                    }
                })?;
            by_source.insert(source_table, append_only);
        }

        Ok(Self { by_source })
    }

    pub fn from_layouts<'a, I>(db: &Db, layouts: I) -> Result<Self, DebeziumMaterializationError>
    where
        I: IntoIterator<Item = &'a DebeziumPartitionedTableLayout>,
    {
        let mut bindings = Vec::new();
        for layout in layouts {
            for (partition, table_name) in &layout.cdc_partitions {
                bindings.push((
                    layout.source_table.clone(),
                    KafkaTopicPartition::new(layout.topic.clone(), *partition),
                    db.table(table_name.clone()),
                ));
            }
        }
        Self::one_table_per_partition(bindings)
    }

    fn stage_event(
        &self,
        tx: &mut Transaction,
        event: &DebeziumEvent,
        record: &KafkaRecord,
        value: Value,
    ) -> Result<(), DebeziumMaterializationError> {
        let tables = self.by_source.get(&event.source_table).ok_or_else(|| {
            DebeziumMaterializationError::MissingEventLogTable {
                source_table: event.source_table.to_string(),
                topic: record.topic_partition.topic.clone(),
                partition: record.topic_partition.partition.get(),
            }
        })?;
        tables.stage_value(tx, record, value).map_err(|source| {
            DebeziumMaterializationError::EventLogLayout {
                source_table: event.source_table.to_string(),
                source,
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct DebeziumMirrorTables {
    by_source: BTreeMap<DebeziumSourceTable, Table>,
}

impl DebeziumMirrorTables {
    pub fn new<I>(bindings: I) -> Result<Self, DebeziumMaterializationError>
    where
        I: IntoIterator<Item = (DebeziumSourceTable, Table)>,
    {
        let mut by_source = BTreeMap::new();
        for (source_table, table) in bindings {
            if by_source.insert(source_table.clone(), table).is_some() {
                return Err(DebeziumMaterializationError::DuplicateMirrorTable {
                    source_table: source_table.to_string(),
                });
            }
        }
        Ok(Self { by_source })
    }

    pub fn from_layouts<'a, I>(db: &Db, layouts: I) -> Result<Self, DebeziumMaterializationError>
    where
        I: IntoIterator<Item = &'a DebeziumPartitionedTableLayout>,
    {
        Self::new(
            layouts
                .into_iter()
                .map(|layout| {
                    (
                        layout.source_table.clone(),
                        db.table(layout.current.clone()),
                    )
                })
                .collect::<Vec<_>>(),
        )
    }

    fn table_for(
        &self,
        source_table: &DebeziumSourceTable,
    ) -> Result<&Table, DebeziumMaterializationError> {
        self.by_source.get(source_table).ok_or_else(|| {
            DebeziumMaterializationError::MissingMirrorTable {
                source_table: source_table.to_string(),
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DebeziumMirrorMutationPlan {
    Upsert {
        key: DebeziumPrimaryKey,
        row: DebeziumMirrorRow,
    },
    Delete {
        key: DebeziumPrimaryKey,
    },
    Ignore,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DebeziumMaterializationPlan {
    pub mode: DebeziumMaterializationMode,
    pub table_selected: bool,
    pub row_membership: DebeziumRowMembership,
    pub write_event_log: bool,
    pub mirror: DebeziumMirrorMutationPlan,
}

#[derive(Clone, Debug)]
pub struct DebeziumMaterializer {
    mode: DebeziumMaterializationMode,
    event_log: Option<DebeziumEventLogTables>,
    mirror: Option<DebeziumMirrorTables>,
    table_filter: DebeziumTableFilter,
    row_predicate: DebeziumRowPredicate,
    column_projection: DebeziumColumnProjection,
}

impl DebeziumMaterializer {
    pub fn event_log(event_log: DebeziumEventLogTables) -> Self {
        Self {
            mode: DebeziumMaterializationMode::EventLog,
            event_log: Some(event_log),
            mirror: None,
            table_filter: DebeziumTableFilter::default(),
            row_predicate: DebeziumRowPredicate::default(),
            column_projection: DebeziumColumnProjection::default(),
        }
    }

    pub fn mirror(mirror: DebeziumMirrorTables) -> Self {
        Self {
            mode: DebeziumMaterializationMode::Mirror,
            event_log: None,
            mirror: Some(mirror),
            table_filter: DebeziumTableFilter::default(),
            row_predicate: DebeziumRowPredicate::default(),
            column_projection: DebeziumColumnProjection::default(),
        }
    }

    pub fn hybrid(event_log: DebeziumEventLogTables, mirror: DebeziumMirrorTables) -> Self {
        Self {
            mode: DebeziumMaterializationMode::Hybrid,
            event_log: Some(event_log),
            mirror: Some(mirror),
            table_filter: DebeziumTableFilter::default(),
            row_predicate: DebeziumRowPredicate::default(),
            column_projection: DebeziumColumnProjection::default(),
        }
    }

    pub fn mode(&self) -> DebeziumMaterializationMode {
        self.mode
    }

    pub fn with_table_filter(mut self, table_filter: DebeziumTableFilter) -> Self {
        self.table_filter = table_filter;
        self
    }

    pub fn with_row_predicate(mut self, row_predicate: DebeziumRowPredicate) -> Self {
        self.row_predicate = row_predicate;
        self
    }

    pub fn with_column_projection(mut self, column_projection: DebeziumColumnProjection) -> Self {
        self.column_projection = column_projection;
        self
    }

    pub fn plan(&self, event: &DebeziumEvent) -> DebeziumMaterializationPlan {
        let table_selected = self.table_filter.matches(&event.source_table);
        let row_membership = if table_selected {
            self.row_predicate.membership(event)
        } else {
            DebeziumRowMembership::default()
        };
        let write_event_log = table_selected
            && self.mode.writes_event_log()
            && (event.is_tombstone() || row_membership.any());

        let mirror = if !table_selected || !self.mode.writes_mirror() || event.is_tombstone() {
            DebeziumMirrorMutationPlan::Ignore
        } else if row_membership.after {
            let values = event
                .after
                .as_ref()
                .map(|row| self.column_projection.project_row(row))
                .unwrap_or_default();
            DebeziumMirrorMutationPlan::Upsert {
                key: event.primary_key.clone(),
                row: DebeziumMirrorRow {
                    source_table: event.source_table.clone(),
                    primary_key: event.primary_key.clone(),
                    values,
                    snapshot: event.snapshot,
                    kafka: event.kafka.clone(),
                    transaction: event.transaction.clone(),
                },
            }
        } else if matches!(
            event.operation(),
            Some(DebeziumOperationKind::Update | DebeziumOperationKind::Delete)
        ) || row_membership.before
        {
            DebeziumMirrorMutationPlan::Delete {
                key: event.primary_key.clone(),
            }
        } else {
            DebeziumMirrorMutationPlan::Ignore
        };

        DebeziumMaterializationPlan {
            mode: self.mode,
            table_selected,
            row_membership,
            write_event_log,
            mirror,
        }
    }

    pub fn stage_record(
        &self,
        tx: &mut Transaction,
        record: &KafkaRecord,
        event: &DebeziumEvent,
    ) -> Result<(), DebeziumMaterializationError> {
        let plan = self.plan(event);

        if plan.write_event_log {
            let projected = self.column_projection.project_event(event);
            let value = projected.to_value()?;
            let event_log = self
                .event_log
                .as_ref()
                .ok_or(DebeziumMaterializationError::MissingEventLogBindings)?;
            event_log.stage_event(tx, event, record, value)?;
        }

        match plan.mirror {
            DebeziumMirrorMutationPlan::Upsert { key, row } => {
                let mirror = self
                    .mirror
                    .as_ref()
                    .ok_or(DebeziumMaterializationError::MissingMirrorBindings)?;
                tx.write(
                    mirror.table_for(&event.source_table)?,
                    key.encode()?,
                    row.to_value()?,
                );
            }
            DebeziumMirrorMutationPlan::Delete { key } => {
                let mirror = self
                    .mirror
                    .as_ref()
                    .ok_or(DebeziumMaterializationError::MissingMirrorBindings)?;
                tx.delete(mirror.table_for(&event.source_table)?, key.encode()?);
            }
            DebeziumMirrorMutationPlan::Ignore => {}
        }

        Ok(())
    }
}

pub trait DebeziumEnvelopeDecoder: Send + Sync {
    fn decode_record(&self, record: &KafkaRecord) -> Result<DebeziumEvent, DebeziumDecodeError>;
}

#[derive(Clone, Debug, Default)]
pub struct PostgresDebeziumDecoder {
    topic_bindings: BTreeMap<String, DebeziumSourceTable>,
}

impl PostgresDebeziumDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_topic_binding(
        mut self,
        topic: impl Into<String>,
        source_table: DebeziumSourceTable,
    ) -> Self {
        self.topic_bindings.insert(topic.into(), source_table);
        self
    }

    pub fn with_layouts<'a, I>(mut self, layouts: I) -> Self
    where
        I: IntoIterator<Item = &'a DebeziumPartitionedTableLayout>,
    {
        for layout in layouts {
            self.topic_bindings
                .insert(layout.topic.clone(), layout.source_table.clone());
        }
        self
    }

    pub fn topic_binding(&self, topic: &str) -> Option<&DebeziumSourceTable> {
        self.topic_bindings.get(topic)
    }

    fn resolve_row_source_table(
        &self,
        topic: &str,
        source_table: DebeziumSourceTable,
    ) -> Result<DebeziumSourceTable, DebeziumDecodeError> {
        match self.topic_bindings.get(topic) {
            Some(bound) if bound != &source_table => {
                Err(DebeziumDecodeError::TopicBindingMismatch {
                    topic: topic.to_string(),
                    bound: bound.to_string(),
                    decoded: source_table.to_string(),
                })
            }
            Some(bound) => Ok(bound.clone()),
            None => Ok(source_table),
        }
    }

    fn resolve_tombstone_source_table(
        &self,
        topic: &str,
    ) -> Result<DebeziumSourceTable, DebeziumDecodeError> {
        self.topic_bindings.get(topic).cloned().ok_or_else(|| {
            DebeziumDecodeError::UnboundTombstoneTopic {
                topic: topic.to_string(),
            }
        })
    }
}

impl DebeziumEnvelopeDecoder for PostgresDebeziumDecoder {
    fn decode_record(&self, record: &KafkaRecord) -> Result<DebeziumEvent, DebeziumDecodeError> {
        let primary_key = decode_debezium_primary_key(
            record
                .key
                .as_deref()
                .ok_or(DebeziumDecodeError::MissingKey)?,
        )?;

        if record.value.is_none() {
            return Ok(DebeziumEvent {
                connector: DebeziumConnectorKind::PostgreSql,
                source_table: self.resolve_tombstone_source_table(&record.topic_partition.topic)?,
                kind: DebeziumEventKind::Tombstone,
                snapshot: None,
                kafka: DebeziumKafkaCoordinates::from_record(record),
                primary_key,
                before: None,
                after: None,
                source: DebeziumPostgresSourceMetadata::default(),
                transaction: None,
            });
        }

        let root = parse_json(record.value.as_deref().unwrap_or_default(), "record value")?;
        let payload = extract_payload(&root, "record value")?;
        let payload_object = expect_object(payload, "record value payload")?;
        let source_object = expect_field_object(payload_object, "source", "record value payload")?;

        let decoded_table = DebeziumSourceTable::new(
            required_string(source_object, "db", "payload.source")?,
            required_string(source_object, "schema", "payload.source")?,
            required_string(source_object, "table", "payload.source")?,
        );
        let source_table =
            self.resolve_row_source_table(&record.topic_partition.topic, decoded_table)?;

        let operation =
            match required_string(payload_object, "op", "record value payload")?.as_str() {
                "c" => DebeziumOperationKind::Create,
                "u" => DebeziumOperationKind::Update,
                "d" => DebeziumOperationKind::Delete,
                "r" => DebeziumOperationKind::Read,
                other => {
                    return Err(DebeziumDecodeError::UnsupportedOperation {
                        operation: other.to_string(),
                    });
                }
            };

        let before = optional_row(payload_object.get("before"), "payload.before")?;
        let after = optional_row(payload_object.get("after"), "payload.after")?;
        match operation {
            DebeziumOperationKind::Create
            | DebeziumOperationKind::Read
            | DebeziumOperationKind::Update
                if after.is_none() =>
            {
                return Err(DebeziumDecodeError::InvalidEnvelope {
                    message: format!("operation {:?} requires an `after` row image", operation),
                });
            }
            DebeziumOperationKind::Delete if after.is_some() => {
                return Err(DebeziumDecodeError::InvalidEnvelope {
                    message: "delete operations must not include an `after` row image".to_string(),
                });
            }
            _ => {}
        }

        Ok(DebeziumEvent {
            connector: DebeziumConnectorKind::PostgreSql,
            source_table,
            kind: DebeziumEventKind::Change { operation },
            snapshot: optional_snapshot(source_object.get("snapshot"), "payload.source.snapshot")?,
            kafka: DebeziumKafkaCoordinates::from_record(record),
            primary_key,
            before,
            after,
            source: DebeziumPostgresSourceMetadata {
                lsn: optional_u64(source_object.get("lsn"), "payload.source.lsn")?,
                tx_id: optional_u64(source_object.get("txId"), "payload.source.txId")?,
                xmin: optional_u64(source_object.get("xmin"), "payload.source.xmin")?,
                source_ts_ms: optional_u64(source_object.get("ts_ms"), "payload.source.ts_ms")?,
                payload_ts_ms: optional_u64(payload_object.get("ts_ms"), "payload.ts_ms")?,
            },
            transaction: optional_transaction(payload_object.get("transaction"))?,
        })
    }
}

pub struct DebeziumIngressHandler<D> {
    decoder: D,
    materializer: DebeziumMaterializer,
}

impl<D> DebeziumIngressHandler<D> {
    pub fn new(decoder: D, materializer: DebeziumMaterializer) -> Self {
        Self {
            decoder,
            materializer,
        }
    }

    pub fn decoder(&self) -> &D {
        &self.decoder
    }

    pub fn materializer(&self) -> &DebeziumMaterializer {
        &self.materializer
    }
}

#[async_trait]
impl<D> KafkaBatchHandler for DebeziumIngressHandler<D>
where
    D: DebeziumEnvelopeDecoder,
{
    type Error = DebeziumMaterializationError;

    async fn apply_batch(
        &self,
        tx: &mut Transaction,
        batch: &terracedb_kafka::KafkaAdmissionBatch,
    ) -> Result<(), Self::Error> {
        for record in &batch.retained_records {
            let event = self.decoder.decode_record(record)?;
            self.materializer.stage_record(tx, record, &event)?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum DebeziumCodecError {
    #[error("debezium {subject} must be stored as bytes")]
    ValueType { subject: &'static str },
    #[error("debezium {subject} is missing its format version byte")]
    MissingVersion { subject: &'static str },
    #[error("debezium {subject} has unknown format version {version}")]
    UnknownVersion { subject: &'static str, version: u8 },
    #[error("debezium {subject} json encoding failed: {message}")]
    JsonEncode {
        subject: &'static str,
        message: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("debezium {subject} json decoding failed: {message}")]
    JsonDecode {
        subject: &'static str,
        message: String,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
pub enum DebeziumDecodeError {
    #[error("debezium record key is required to extract the primary key")]
    MissingKey,
    #[error("debezium {context} is not valid json: {message}")]
    InvalidJson {
        context: &'static str,
        message: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("debezium {context} must be a json object")]
    ExpectedObject { context: &'static str },
    #[error("debezium {context} is missing required field `{field}`")]
    MissingField {
        context: &'static str,
        field: String,
    },
    #[error("debezium {context}.{field} has invalid type: {message}")]
    InvalidField {
        context: &'static str,
        field: String,
        message: String,
    },
    #[error("debezium primary key must contain at least one field")]
    EmptyPrimaryKey,
    #[error("debezium tombstone on topic {topic} requires an explicit topic binding")]
    UnboundTombstoneTopic { topic: String },
    #[error("debezium topic binding mismatch for {topic}: configured {bound}, decoded {decoded}")]
    TopicBindingMismatch {
        topic: String,
        bound: String,
        decoded: String,
    },
    #[error("debezium operation {operation} is not supported")]
    UnsupportedOperation { operation: String },
    #[error("debezium snapshot marker {marker} is not supported")]
    UnsupportedSnapshotMarker { marker: String },
    #[error("debezium envelope is invalid: {message}")]
    InvalidEnvelope { message: String },
}

#[derive(Debug, Error)]
pub enum DebeziumMaterializationError {
    #[error(transparent)]
    Decode(#[from] DebeziumDecodeError),
    #[error(transparent)]
    Codec(#[from] DebeziumCodecError),
    #[error("debezium event-log table mapping failed for {source_table}")]
    EventLogLayout {
        source_table: String,
        #[source]
        source: KafkaMaterializationError,
    },
    #[error(
        "no Debezium event-log table was configured for {source_table} on {topic}[{partition}]"
    )]
    MissingEventLogTable {
        source_table: String,
        topic: String,
        partition: u32,
    },
    #[error("no Debezium mirror table was configured for {source_table}")]
    MissingMirrorTable { source_table: String },
    #[error("Debezium mirror table mapping contains a duplicate entry for {source_table}")]
    DuplicateMirrorTable { source_table: String },
    #[error("Debezium materializer mode requires event-log table bindings")]
    MissingEventLogBindings,
    #[error("Debezium materializer mode requires mirror table bindings")]
    MissingMirrorBindings,
}

fn sanitize_identifier(input: &str) -> String {
    let mut out = String::new();
    let mut last_was_underscore = false;
    for ch in input.chars() {
        let normalized = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };
        if normalized == '_' {
            if !last_was_underscore {
                out.push('_');
            }
            last_was_underscore = true;
        } else {
            out.push(normalized);
            last_was_underscore = false;
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "unnamed".to_string()
    } else {
        trimmed.to_string()
    }
}

fn encode_versioned_json<T>(
    version: u8,
    subject: &'static str,
    value: &T,
) -> Result<Vec<u8>, DebeziumCodecError>
where
    T: Serialize,
{
    let mut encoded = Vec::with_capacity(1);
    encoded.push(version);
    encoded.extend(
        serde_json::to_vec(value).map_err(|source| DebeziumCodecError::JsonEncode {
            subject,
            message: source.to_string(),
            source,
        })?,
    );
    Ok(encoded)
}

fn decode_versioned_json<T>(
    expected_version: u8,
    subject: &'static str,
    bytes: &[u8],
) -> Result<T, DebeziumCodecError>
where
    T: DeserializeOwned,
{
    let Some(version) = bytes.first().copied() else {
        return Err(DebeziumCodecError::MissingVersion { subject });
    };
    if version != expected_version {
        return Err(DebeziumCodecError::UnknownVersion { subject, version });
    }
    serde_json::from_slice(&bytes[1..]).map_err(|source| DebeziumCodecError::JsonDecode {
        subject,
        message: source.to_string(),
        source,
    })
}

fn expect_bytes<'a>(
    value: &'a Value,
    subject: &'static str,
) -> Result<&'a [u8], DebeziumCodecError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        Value::Record(_) => Err(DebeziumCodecError::ValueType { subject }),
    }
}

fn decode_debezium_primary_key(bytes: &[u8]) -> Result<DebeziumPrimaryKey, DebeziumDecodeError> {
    let root = parse_json(bytes, "record key")?;
    let payload = extract_payload(&root, "record key")?;
    let payload_object = expect_object(payload, "record key payload")?;
    if payload_object.is_empty() {
        return Err(DebeziumDecodeError::EmptyPrimaryKey);
    }
    Ok(DebeziumPrimaryKey::new(json_object_to_row(payload_object)))
}

fn parse_json(bytes: &[u8], context: &'static str) -> Result<JsonValue, DebeziumDecodeError> {
    serde_json::from_slice(bytes).map_err(|source| DebeziumDecodeError::InvalidJson {
        context,
        message: source.to_string(),
        source,
    })
}

fn extract_payload<'a>(
    root: &'a JsonValue,
    context: &'static str,
) -> Result<&'a JsonValue, DebeziumDecodeError> {
    match root {
        JsonValue::Object(object) => Ok(object.get("payload").unwrap_or(root)),
        _ => Err(DebeziumDecodeError::ExpectedObject { context }),
    }
}

fn expect_object<'a>(
    value: &'a JsonValue,
    context: &'static str,
) -> Result<&'a JsonMap<String, JsonValue>, DebeziumDecodeError> {
    value
        .as_object()
        .ok_or(DebeziumDecodeError::ExpectedObject { context })
}

fn expect_field_object<'a>(
    object: &'a JsonMap<String, JsonValue>,
    field: &str,
    context: &'static str,
) -> Result<&'a JsonMap<String, JsonValue>, DebeziumDecodeError> {
    let value = object
        .get(field)
        .ok_or_else(|| DebeziumDecodeError::MissingField {
            context,
            field: field.to_string(),
        })?;
    value
        .as_object()
        .ok_or_else(|| DebeziumDecodeError::InvalidField {
            context,
            field: field.to_string(),
            message: format!("expected object, found {}", json_type_name(value)),
        })
}

fn required_string(
    object: &JsonMap<String, JsonValue>,
    field: &str,
    context: &'static str,
) -> Result<String, DebeziumDecodeError> {
    let value = object
        .get(field)
        .ok_or_else(|| DebeziumDecodeError::MissingField {
            context,
            field: field.to_string(),
        })?;
    value
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| DebeziumDecodeError::InvalidField {
            context,
            field: field.to_string(),
            message: format!("expected string, found {}", json_type_name(value)),
        })
}

fn optional_u64(
    value: Option<&JsonValue>,
    context: &'static str,
) -> Result<Option<u64>, DebeziumDecodeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    match value {
        JsonValue::Number(number) => number
            .as_u64()
            .ok_or_else(|| DebeziumDecodeError::InvalidField {
                context,
                field: context
                    .split('.')
                    .next_back()
                    .unwrap_or_default()
                    .to_string(),
                message: "expected a non-negative integer".to_string(),
            })
            .map(Some),
        JsonValue::String(text) => {
            text.parse::<u64>()
                .map(Some)
                .map_err(|error| DebeziumDecodeError::InvalidField {
                    context,
                    field: context
                        .split('.')
                        .next_back()
                        .unwrap_or_default()
                        .to_string(),
                    message: error.to_string(),
                })
        }
        other => Err(DebeziumDecodeError::InvalidField {
            context,
            field: context
                .split('.')
                .next_back()
                .unwrap_or_default()
                .to_string(),
            message: format!(
                "expected integer-or-string, found {}",
                json_type_name(other)
            ),
        }),
    }
}

fn optional_snapshot(
    value: Option<&JsonValue>,
    context: &'static str,
) -> Result<Option<DebeziumSnapshotMarker>, DebeziumDecodeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    match value {
        JsonValue::Bool(false) => Ok(None),
        JsonValue::Bool(true) => Ok(Some(DebeziumSnapshotMarker::Initial)),
        JsonValue::String(marker) => match marker.as_str() {
            "false" => Ok(None),
            "true" => Ok(Some(DebeziumSnapshotMarker::Initial)),
            "last" => Ok(Some(DebeziumSnapshotMarker::Last)),
            "incremental" => Ok(Some(DebeziumSnapshotMarker::Incremental)),
            other => Err(DebeziumDecodeError::UnsupportedSnapshotMarker {
                marker: other.to_string(),
            }),
        },
        other => Err(DebeziumDecodeError::InvalidField {
            context,
            field: "snapshot".to_string(),
            message: format!("expected bool-or-string, found {}", json_type_name(other)),
        }),
    }
}

fn optional_transaction(
    value: Option<&JsonValue>,
) -> Result<Option<DebeziumTransactionMetadata>, DebeziumDecodeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let object = value
        .as_object()
        .ok_or_else(|| DebeziumDecodeError::InvalidField {
            context: "payload",
            field: "transaction".to_string(),
            message: format!("expected object, found {}", json_type_name(value)),
        })?;
    Ok(Some(DebeziumTransactionMetadata {
        id: required_string(object, "id", "payload.transaction")?,
        total_order: optional_u64(object.get("total_order"), "payload.transaction.total_order")?,
        data_collection_order: optional_u64(
            object.get("data_collection_order"),
            "payload.transaction.data_collection_order",
        )?,
    }))
}

fn optional_row(
    value: Option<&JsonValue>,
    context: &'static str,
) -> Result<Option<DebeziumRow>, DebeziumDecodeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let object = value
        .as_object()
        .ok_or_else(|| DebeziumDecodeError::InvalidField {
            context,
            field: context
                .split('.')
                .next_back()
                .unwrap_or_default()
                .to_string(),
            message: format!("expected object-or-null, found {}", json_type_name(value)),
        })?;
    Ok(Some(json_object_to_row(object)))
}

fn json_object_to_row(object: &JsonMap<String, JsonValue>) -> DebeziumRow {
    object
        .iter()
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect()
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}
