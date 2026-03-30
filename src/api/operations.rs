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
}

impl CommitOptions {
    pub fn with_read_set(mut self, read_set: ReadSet) -> Self {
        self.read_set = Some(read_set);
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanOptions {
    pub reverse: bool,
    pub limit: Option<usize>,
    pub columns: Option<Vec<String>>,
}
