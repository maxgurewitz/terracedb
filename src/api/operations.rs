use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeKind {
    Put,
    Delete,
    Merge,
}

#[derive(Clone, Debug)]
pub struct ChangeEntry {
    pub key: Key,
    pub value: Option<Value>,
    pub cursor: LogCursor,
    pub sequence: SequenceNumber,
    pub kind: ChangeKind,
    pub table: Table,
    pub operation_context: Option<OperationContext>,
}

#[derive(Clone, Debug)]
pub enum BatchOperation {
    Put {
        table: Table,
        key: Key,
        value: Value,
    },
    Merge {
        table: Table,
        key: Key,
        value: Value,
    },
    Delete {
        table: Table,
        key: Key,
    },
}

#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

impl WriteBatch {
    pub fn put(&mut self, table: &Table, key: Key, value: Value) {
        self.operations.push(BatchOperation::Put {
            table: table.clone(),
            key,
            value,
        });
    }

    pub fn merge(&mut self, table: &Table, key: Key, value: Value) {
        self.operations.push(BatchOperation::Merge {
            table: table.clone(),
            key,
            value,
        });
    }

    pub fn delete(&mut self, table: &Table, key: Key) {
        self.operations.push(BatchOperation::Delete {
            table: table.clone(),
            key,
        });
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn operations(&self) -> &[BatchOperation] {
        &self.operations
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSetEntry {
    pub table: Table,
    pub key: Key,
    pub at_sequence: SequenceNumber,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadSet {
    entries: Vec<ReadSetEntry>,
}

impl ReadSet {
    pub fn add(&mut self, table: &Table, key: Key, at_sequence: SequenceNumber) {
        self.entries.push(ReadSetEntry {
            table: table.clone(),
            key,
            at_sequence,
        });
    }

    pub fn entries(&self) -> &[ReadSetEntry] {
        &self.entries
    }
}

#[derive(Clone, Debug, Default)]
pub struct CommitOptions {
    pub read_set: Option<ReadSet>,
    pub operation_context: Option<OperationContext>,
}

impl CommitOptions {
    pub fn with_read_set(mut self, read_set: ReadSet) -> Self {
        self.read_set = Some(read_set);
        self
    }

    pub fn with_operation_context(mut self, operation_context: OperationContext) -> Self {
        self.operation_context = Some(operation_context);
        self
    }

    pub fn with_current_context(mut self) -> Self {
        self.operation_context = Some(OperationContext::current());
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScanPredicate {
    #[default]
    AlwaysTrue,
    All {
        predicates: Vec<ScanPredicate>,
    },
    FieldEquals {
        field: String,
        value: FieldValue,
    },
    Int64AtLeast {
        field: String,
        value: i64,
    },
    BoolEquals {
        field: String,
        value: bool,
    },
}

impl ScanPredicate {
    pub fn all(predicates: impl IntoIterator<Item = Self>) -> Self {
        let predicates = predicates
            .into_iter()
            .filter(|predicate| !matches!(predicate, Self::AlwaysTrue))
            .collect::<Vec<_>>();
        match predicates.len() {
            0 => Self::AlwaysTrue,
            1 => predicates.into_iter().next().expect("single predicate"),
            _ => Self::All { predicates },
        }
    }

    pub fn field_equals(field: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        Self::FieldEquals {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn int64_at_least(field: impl Into<String>, value: i64) -> Self {
        Self::Int64AtLeast {
            field: field.into(),
            value,
        }
    }

    pub fn bool_equals(field: impl Into<String>, value: bool) -> Self {
        Self::BoolEquals {
            field: field.into(),
            value,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanMaterializationSource {
    #[default]
    BasePart,
    ProjectionSidecar,
    ProjectionFallbackToBase,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarScanPartExecution {
    pub local_id: String,
    pub skip_indexes_used: Vec<String>,
    pub skip_index_pruned: bool,
    pub skip_index_fallback_to_base: bool,
    pub zone_map_pruned: bool,
    pub projection_sidecar_reads: usize,
    pub projection_fallback_reads: usize,
    pub base_part_reads: usize,
    pub rows_materialized: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowScanExecution {
    pub visited_key_groups: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarScanExecution {
    pub sstables_considered: usize,
    pub sstables_pruned_by_skip_index: usize,
    pub sstables_pruned_by_zone_map: usize,
    pub rows_evaluated: usize,
    pub rows_returned: usize,
    pub parts: Vec<ColumnarScanPartExecution>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanExecution {
    pub rows_returned: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<RowScanExecution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columnar: Option<ColumnarScanExecution>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanOptions {
    pub reverse: bool,
    pub limit: Option<usize>,
    pub columns: Option<Vec<String>>,
    pub predicate: Option<ScanPredicate>,
}
