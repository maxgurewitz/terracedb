use std::{collections::BTreeMap, pin::Pin};

use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use terracedb::{LogCursor, SequenceNumber, SubscriptionClosed, Timestamp};

use crate::{ActivityId, AgentFsError, JsonValue, ToolRunId, VolumeId};

pub type ActivityStream =
    Pin<Box<dyn Stream<Item = Result<ActivityEntry, AgentFsError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ActivityOptions {
    pub durable: bool,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivityEntry {
    pub volume_id: VolumeId,
    pub activity_id: ActivityId,
    pub sequence: SequenceNumber,
    pub cursor: LogCursor,
    pub timestamp: Timestamp,
    pub kind: ActivityKind,
    pub subject: Option<String>,
    pub tool_run_id: Option<ToolRunId>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    DirectoryCreated,
    DirectoryRemoved,
    FileWritten,
    FilePatched,
    FileTruncated,
    PathRenamed,
    HardLinkCreated,
    SymlinkCreated,
    PathDeleted,
    KvSet,
    KvDeleted,
    ToolStarted,
    ToolSucceeded,
    ToolFailed,
    VolumeCloned,
    OverlayCreated,
}

#[derive(Debug)]
pub struct ActivityReceiver {
    inner: watch::Receiver<SequenceNumber>,
}

impl ActivityReceiver {
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

impl Clone for ActivityReceiver {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
