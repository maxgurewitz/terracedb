use crate::{Actor, ActorRef, Error, WorkerId};

use super::message::TypedActorRegistration;
use super::worker::WorkerShardCtx;
use super::{WorkerIngress, WorkerMsg};

#[derive(Clone)]
pub struct WorkerHandle {
    id: WorkerId,
    ingress: WorkerIngress,
}

impl WorkerHandle {
    pub fn new(id: WorkerId, ingress: WorkerIngress) -> Self {
        Self { id, ingress }
    }

    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn submit(&self, msg: WorkerMsg) -> Result<(), Error> {
        self.ingress.submit(msg)
    }

    pub(crate) fn register_actor<A>(&self, actor: A) -> Result<ActorRef<A>, Error>
    where
        A: Actor<WorkerShardCtx> + Send + 'static,
    {
        let (sender, receiver) = flume::bounded(1);

        self.ingress
            .submit_registration(Box::new(TypedActorRegistration::new(actor, sender)))?;

        receiver.recv().map_err(|_| Error::HostReplyClosed)
    }
}
