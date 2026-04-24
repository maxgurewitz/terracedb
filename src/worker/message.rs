use crate::{ActorId, FsCompletion, NetCompletion, ObjectCompletion, RequestId, TimerId, WorkerId};

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct WorkerRoute {
    pub worker: WorkerId,
    pub actor: ActorId,
}
