use std::collections::HashMap;
use std::marker::PhantomData;

use crate::{Actor, ActorId, ActorRef, Env, Error, LocalActorId, ShardCtx, WorkerId};

use super::{ErasedActor, ErasedActorMsg, ErasedResponse};

pub(crate) struct ActorRegistry<C> {
    worker: WorkerId,
    next_local_id: u64,
    actors: HashMap<LocalActorId, ActorEntry<C>>,
}

pub(crate) struct ActorEntry<C> {
    kind: &'static str,
    pub(crate) actor: Box<dyn ErasedActor<C>>,
}

impl<C: ShardCtx> ActorRegistry<C> {
    pub(crate) fn new(worker: WorkerId) -> Self {
        Self {
            worker,
            next_local_id: 0,
            actors: HashMap::new(),
        }
    }

    pub(crate) fn register<A>(&mut self, actor: A) -> ActorRef<A>
    where
        A: Actor<C> + Send + 'static,
    {
        let local = LocalActorId(self.next_local_id);
        self.next_local_id += 1;

        self.actors.insert(
            local,
            ActorEntry {
                kind: std::any::type_name::<A>(),
                actor: Box::new(ActorAdapter { actor }),
            },
        );

        ActorRef {
            id: ActorId {
                worker: self.worker,
                local,
            },
            _marker: PhantomData,
        }
    }

    pub(crate) fn dispatch_erased(
        &mut self,
        actor: ActorId,
        msg: ErasedActorMsg,
        ctx: &mut C,
        env: &mut dyn Env,
    ) -> Result<ErasedResponse, Error> {
        if actor.worker != self.worker {
            return Err(Error::ActorNotFound { actor });
        }

        let mut entry = self
            .actors
            .remove(&actor.local)
            .ok_or(Error::ActorNotFound { actor })?;
        let result = entry.actor.handle_erased(msg, ctx, env);

        self.actors.insert(actor.local, entry);
        result
    }

    pub(crate) fn remove_entry(&mut self, local: LocalActorId) -> Option<ActorEntry<C>> {
        self.actors.remove(&local)
    }

    pub(crate) fn insert_entry(&mut self, local: LocalActorId, entry: ActorEntry<C>) {
        self.actors.insert(local, entry);
    }
}

struct ActorAdapter<A> {
    actor: A,
}

impl<A, C> ErasedActor<C> for ActorAdapter<A>
where
    A: Actor<C> + Send,
    C: ShardCtx,
{
    fn handle_erased(
        &mut self,
        msg: ErasedActorMsg,
        ctx: &mut C,
        env: &mut dyn Env,
    ) -> Result<ErasedResponse, Error> {
        let msg = *msg
            .downcast::<A::Msg>()
            .map_err(|_| Error::ActorMessageTypeMismatch)?;
        let reply = self.actor.handle(msg, ctx, env)?;

        Ok(Box::new(reply))
    }
}
