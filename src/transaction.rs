use std::collections::HashMap;

use thiserror::Error;

use crate::{
    CommitError, CommitOptions, Db, FlushError, Key, ReadError, ReadSet, SequenceNumber, Snapshot,
    Table, Value, WriteBatch,
};

/// User-space optimistic transaction helper built on top of the engine's
/// snapshot, read-set, and batch-commit primitives.
///
/// Transactions provide snapshot isolation for point reads performed through
/// [`Transaction::read`]. They are not fully serializable, and they do not
/// provide phantom protection for range scans.
#[derive(Clone, Debug)]
pub struct Transaction {
    db: Db,
    snapshot: Snapshot,
    batch: WriteBatch,
    read_set: ReadSet,
    local_writes: HashMap<LocalWriteKey, Option<Value>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionCommitOptions {
    pub flush: bool,
}

impl TransactionCommitOptions {
    pub const fn with_flush(mut self, flush: bool) -> Self {
        self.flush = flush;
        self
    }

    pub const fn no_flush() -> Self {
        Self { flush: false }
    }
}

impl Default for TransactionCommitOptions {
    fn default() -> Self {
        Self { flush: true }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum TransactionCommitError {
    #[error(transparent)]
    Commit(#[from] CommitError),
    #[error("transaction committed at sequence {sequence}, but the post-commit flush failed")]
    Flush {
        sequence: SequenceNumber,
        #[source]
        source: FlushError,
    },
}

impl TransactionCommitError {
    pub fn committed_sequence(&self) -> Option<SequenceNumber> {
        match self {
            Self::Commit(_) => None,
            Self::Flush { sequence, .. } => Some(*sequence),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LocalWriteKey {
    table: Table,
    key: Key,
}

impl LocalWriteKey {
    fn new(table: &Table, key: &[u8]) -> Self {
        Self {
            table: table.clone(),
            key: key.to_vec(),
        }
    }
}

impl Transaction {
    pub async fn begin(db: &Db) -> Self {
        Self {
            db: db.clone(),
            snapshot: db.snapshot().await,
            batch: db.write_batch(),
            read_set: db.read_set(),
            local_writes: HashMap::new(),
        }
    }

    pub fn snapshot_sequence(&self) -> SequenceNumber {
        self.snapshot.sequence()
    }

    pub async fn read(&mut self, table: &Table, key: Key) -> Result<Option<Value>, ReadError> {
        let local_key = LocalWriteKey::new(table, &key);
        if let Some(value) = self.local_writes.get(&local_key) {
            return Ok(value.clone());
        }

        let value = self.snapshot.read(table, key.clone()).await?;
        self.read_set.add(table, key, self.snapshot.sequence());
        Ok(value)
    }

    pub fn write(&mut self, table: &Table, key: Key, value: Value) {
        self.batch.put(table, key.clone(), value.clone());
        self.local_writes
            .insert(LocalWriteKey::new(table, &key), Some(value));
    }

    pub fn delete(&mut self, table: &Table, key: Key) {
        self.batch.delete(table, key.clone());
        self.local_writes
            .insert(LocalWriteKey::new(table, &key), None);
    }

    pub async fn commit(self) -> Result<SequenceNumber, TransactionCommitError> {
        self.commit_with_options(TransactionCommitOptions::default())
            .await
    }

    pub async fn commit_no_flush(self) -> Result<SequenceNumber, TransactionCommitError> {
        self.commit_with_options(TransactionCommitOptions::no_flush())
            .await
    }

    pub async fn commit_with_options(
        self,
        opts: TransactionCommitOptions,
    ) -> Result<SequenceNumber, TransactionCommitError> {
        let Self {
            db,
            snapshot,
            batch,
            read_set,
            local_writes: _,
        } = self;

        let sequence = db
            .commit(batch, CommitOptions::default().with_read_set(read_set))
            .await?;

        if opts.flush
            && let Err(source) = db.flush().await
        {
            snapshot.release();
            return Err(TransactionCommitError::Flush { sequence, source });
        }

        snapshot.release();
        Ok(sequence)
    }

    pub fn abort(self) {}
}
