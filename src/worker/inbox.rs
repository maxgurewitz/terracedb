use crate::{Error, WorkerIngress, WorkerMsg};

use super::ingress::WorkerIngressInner;
use super::message::WorkerEnvelope;

pub(crate) struct WorkerInbox {
    sender: flume::Sender<WorkerEnvelope>,
    receiver: flume::Receiver<WorkerEnvelope>,
}

impl WorkerInbox {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = flume::unbounded();

        Self { sender, receiver }
    }

    pub(crate) fn ingress(&self) -> WorkerIngress {
        WorkerIngress::new(WorkerIngressInner::new(self.sender.clone()))
    }

    #[allow(dead_code)]
    pub(crate) fn enqueue(&self, msg: WorkerMsg) -> Result<(), Error> {
        self.sender
            .send(WorkerEnvelope::Msg(msg))
            .map_err(|_| Error::WorkerSubmit)
    }

    pub(super) async fn recv(&mut self) -> Result<WorkerEnvelope, Error> {
        self.receiver
            .recv_async()
            .await
            .map_err(|_| Error::WorkerInboxClosed)
    }

    pub(super) fn try_recv(&mut self) -> Result<Option<WorkerEnvelope>, Error> {
        match self.receiver.try_recv() {
            Ok(msg) => Ok(Some(msg)),
            Err(flume::TryRecvError::Empty) => Ok(None),
            Err(flume::TryRecvError::Disconnected) => Err(Error::WorkerInboxClosed),
        }
    }
}
