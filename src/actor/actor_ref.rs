use std::marker::PhantomData;

use super::{Actor, LocalActorId, RemoteActorId, ShardId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalActorRef<A: Actor> {
    pub id: LocalActorId,
    pub actor: PhantomData<fn() -> A>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActorRef<A: Actor> {
    pub location: Location,
    pub actor: PhantomData<fn() -> A>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Location {
    Local(LocalActorId),
    Remote {
        shard: ShardId,
        actor: RemoteActorId,
    },
}
