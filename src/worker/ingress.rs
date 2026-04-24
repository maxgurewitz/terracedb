use crate::Error;

use super::WorkerMsg;
use super::message::{ActorRegistration, WorkerEnvelope};

#[derive(Clone)]
pub struct WorkerIngress {
    inner: WorkerIngressInner,
}

impl WorkerIngress {
    pub(super) fn new(inner: WorkerIngressInner) -> Self {
        Self { inner }
    }

    pub fn submit(&self, msg: WorkerMsg) -> Result<(), Error> {
        self.inner
            .sender
            .send(WorkerEnvelope::Msg(msg))
            .map_err(|_| Error::WorkerSubmit)
    }

    pub(super) fn submit_registration(
        &self,
        registration: Box<dyn ActorRegistration>,
    ) -> Result<(), Error> {
        self.inner
            .sender
            .send(WorkerEnvelope::RegisterActor { registration })
            .map_err(|_| Error::WorkerSubmit)
    }
}

#[derive(Clone)]
pub(super) struct WorkerIngressInner {
    sender: flume::Sender<WorkerEnvelope>,
}

impl WorkerIngressInner {
    pub(super) fn new(sender: flume::Sender<WorkerEnvelope>) -> Self {
        Self { sender }
    }
}
