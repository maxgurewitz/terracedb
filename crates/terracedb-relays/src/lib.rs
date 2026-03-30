use std::{error::Error as StdError, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::StreamExt;
use thiserror::Error;
use tokio::sync::watch;

use terracedb::{
    Clock, Db, Key, OutboxEntry, ReadError, ScanOptions, StorageError, SubscriptionClosed, Table,
    Transaction, TransactionCommitError, decode_outbox_entry,
};

pub const DEFAULT_RELAY_BATCH_LIMIT: usize = 128;
pub const DEFAULT_RELAY_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayEntry<M> {
    pub outbox_id: Key,
    pub idempotency_key: String,
    pub message: M,
}

#[derive(Debug, Error)]
pub enum OutboxRelayError<E>
where
    E: StdError + Send + Sync + 'static,
{
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    TransactionCommit(#[from] TransactionCommitError),
    #[error(transparent)]
    SubscriptionClosed(#[from] SubscriptionClosed),
    #[error("outbox relay handler failed")]
    Handler {
        #[source]
        source: E,
    },
}

#[async_trait]
pub trait OutboxRelayHandler: Send + Sync {
    type Message: Send + Sync;
    type Error: StdError + Send + Sync + 'static;

    fn decode(&self, entry: OutboxEntry) -> Result<RelayEntry<Self::Message>, Self::Error>;

    async fn apply_batch(
        &self,
        tx: &mut Transaction,
        entries: &[RelayEntry<Self::Message>],
    ) -> Result<(), Self::Error>;
}

pub struct OutboxRelay<H> {
    db: Db,
    clock: Arc<dyn Clock>,
    outbox_table: Table,
    handler: H,
    batch_limit: usize,
    idle_poll_interval: Duration,
}

impl<H> OutboxRelay<H>
where
    H: OutboxRelayHandler + 'static,
{
    pub fn new(db: Db, clock: Arc<dyn Clock>, outbox_table: Table, handler: H) -> Self {
        Self {
            db,
            clock,
            outbox_table,
            handler,
            batch_limit: DEFAULT_RELAY_BATCH_LIMIT,
            idle_poll_interval: DEFAULT_RELAY_IDLE_POLL_INTERVAL,
        }
    }

    pub fn with_batch_limit(mut self, batch_limit: usize) -> Self {
        self.batch_limit = batch_limit.max(1);
        self
    }

    pub fn with_idle_poll_interval(mut self, idle_poll_interval: Duration) -> Self {
        self.idle_poll_interval = idle_poll_interval;
        self
    }

    pub async fn run_once(&self) -> Result<usize, OutboxRelayError<H::Error>> {
        let mut rows = self
            .outbox_table
            .scan(
                Vec::new(),
                vec![0xff],
                ScanOptions {
                    limit: Some(self.batch_limit),
                    ..ScanOptions::default()
                },
            )
            .await?;

        let mut entries = Vec::new();
        while let Some((outbox_id, value)) = rows.next().await {
            let entry = decode_outbox_entry(outbox_id, &value)?;
            entries.push(
                self.handler
                    .decode(entry)
                    .map_err(|source| OutboxRelayError::Handler { source })?,
            );
        }

        if entries.is_empty() {
            return Ok(0);
        }

        let mut tx = Transaction::begin(&self.db).await;
        self.handler
            .apply_batch(&mut tx, &entries)
            .await
            .map_err(|source| OutboxRelayError::Handler { source })?;
        for entry in &entries {
            tx.delete(&self.outbox_table, entry.outbox_id.clone());
        }
        tx.commit().await?;
        Ok(entries.len())
    }

    pub async fn run(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), OutboxRelayError<H::Error>> {
        let mut subscription = self.db.subscribe(&self.outbox_table);

        loop {
            let processed = self.run_once().await?;
            if processed == 0 {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    changed = subscription.changed() => {
                        changed?;
                    }
                    _ = self.clock.sleep(self.idle_poll_interval) => {}
                }
            }
        }
    }
}
