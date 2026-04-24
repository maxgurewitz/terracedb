use std::marker::PhantomData;

use super::{LocalActorId, RemoteActorId, ShardId};
use crate::{ActorId, WorkerId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalActorRef<A> {
    pub id: LocalActorId,
    pub _marker: PhantomData<fn() -> A>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActorRef<A> {
    pub id: ActorId,
    pub _marker: PhantomData<fn() -> A>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Location {
    Local(LocalActorId),
    Remote {
        worker: WorkerId,
        actor: RemoteActorId,
    },
    Shard {
        shard: ShardId,
        actor: RemoteActorId,
    },
}
