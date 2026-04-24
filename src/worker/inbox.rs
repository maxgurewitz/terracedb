use crate::{Error, WorkerIngress, WorkerMsg};

use super::ingress::WorkerIngressInner;

pub(crate) struct WorkerInbox {
    sender: flume::Sender<WorkerMsg>,
    receiver: flume::Receiver<WorkerMsg>,
}

impl WorkerInbox {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = flume::unbounded();

        Self { sender, receiver }
    }

    pub(crate) fn ingress(&self) -> WorkerIngress {
        WorkerIngress::new(WorkerIngressInner {
            sender: self.sender.clone(),
        })
    }

    pub(crate) async fn recv(&mut self) -> Result<WorkerMsg, Error> {
        self.receiver
            .recv_async()
            .await
            .map_err(|_| Error::WorkerInboxClosed)
    }
}
