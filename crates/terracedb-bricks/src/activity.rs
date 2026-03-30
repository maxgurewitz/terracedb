use std::{collections::BTreeMap, pin::Pin};

use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use terracedb::{LogCursor, SequenceNumber, SubscriptionClosed, Timestamp};

use crate::{BlobActivityId, BlobAlias, BlobId, JsonValue};

pub type BlobActivityStream =
    Pin<Box<dyn Stream<Item = Result<BlobActivityEntry, crate::BlobError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobActivityOptions {
    pub durable: bool,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BlobActivityEntry {
    pub namespace: String,
    pub activity_id: BlobActivityId,
    pub sequence: SequenceNumber,
    pub cursor: LogCursor,
    pub timestamp: Timestamp,
    pub kind: BlobActivityKind,
    pub blob_id: Option<BlobId>,
    pub alias: Option<BlobAlias>,
    pub object_key: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlobActivityKind {
    BlobPublished,
    BlobDeleted,
    AliasUpserted,
    AliasRemoved,
    IndexingQueued,
    IndexingCompleted,
    IndexingFailed,
}

#[derive(Debug)]
pub struct BlobActivityReceiver {
    inner: watch::Receiver<SequenceNumber>,
}

impl BlobActivityReceiver {
    pub(crate) fn new(inner: watch::Receiver<SequenceNumber>) -> Self {
        Self { inner }
    }

    pub fn current(&self) -> SequenceNumber {
        *self.inner.borrow()
    }

    pub async fn changed(&mut self) -> Result<SequenceNumber, SubscriptionClosed> {
        self.inner.changed().await.map_err(|_| SubscriptionClosed)?;
        Ok(*self.inner.borrow_and_update())
    }
}

impl Clone for BlobActivityReceiver {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
