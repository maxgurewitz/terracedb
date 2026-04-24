use crate::Error;

use super::WorkerMsg;

#[derive(Clone)]
pub struct WorkerIngress {
    inner: WorkerIngressInner,
}

impl WorkerIngress {
    pub(crate) fn new(inner: WorkerIngressInner) -> Self {
        Self { inner }
    }

    pub fn submit(&self, msg: WorkerMsg) -> Result<(), Error> {
        self.inner.sender.send(msg).map_err(|_| Error::WorkerSubmit)
    }
}

#[derive(Clone)]
pub(crate) struct WorkerIngressInner {
    pub(crate) sender: flume::Sender<WorkerMsg>,
}
