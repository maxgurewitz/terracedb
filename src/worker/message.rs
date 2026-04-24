use crate::{ActorId, ActorRef, FsCompletion, NetCompletion, ObjectCompletion, RequestId, TimerId};

use super::worker::{WorkerCore, WorkerShardCtx};
use super::{ErasedActorMsg, ErasedResponse, HostReply};

pub enum WorkerMsg {
    ToActor {
        actor: ActorId,
        msg: ErasedActorMsg,
    },
    HostRequest {
        request_id: RequestId,
        actor: ActorId,
        msg: ErasedActorMsg,
        reply_to: HostReply,
    },
    RequestDone {
        request_id: RequestId,
        response: ErasedResponse,
    },
    NetCompletion(NetCompletion),
    FsCompletion(FsCompletion),
    ObjectStoreCompletion(ObjectCompletion),
    TimerFired(TimerId),
    Shutdown,
}

pub(super) enum WorkerEnvelope {
    RegisterActor {
        registration: Box<dyn ActorRegistration>,
    },
    Msg(WorkerMsg),
}

pub(super) trait ActorRegistration: Send {
    fn register(self: Box<Self>, worker: &mut WorkerCore) -> Result<(), crate::Error>;
}

pub(super) struct TypedActorRegistration<A> {
    actor: A,
    reply: flume::Sender<ActorRef<A>>,
}

impl<A> TypedActorRegistration<A> {
    pub(super) fn new(actor: A, reply: flume::Sender<ActorRef<A>>) -> Self {
        Self { actor, reply }
    }
}

impl<A> ActorRegistration for TypedActorRegistration<A>
where
    A: crate::Actor<WorkerShardCtx> + Send + 'static,
{
    fn register(self: Box<Self>, worker: &mut WorkerCore) -> Result<(), crate::Error> {
        let actor_ref = worker.register_actor(self.actor);

        self.reply
            .send(actor_ref)
            .map_err(|_| crate::Error::HostReplyClosed)
    }
}
