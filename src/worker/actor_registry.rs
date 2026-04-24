use std::collections::HashMap;
use std::marker::PhantomData;

use crate::{
    Actor, ActorId, ActorRef, Env, Error, LocalActorId, ShardCtx, TimerCompletion, WorkerId,
};

use super::{ErasedActor, ErasedActorMsg, ErasedResponse};

#[allow(dead_code)]
pub(crate) struct ActorRegistry<C> {
    worker: WorkerId,
    next_local_id: u64,
    actors: HashMap<LocalActorId, ActorEntry<C>>,
}

#[allow(dead_code)]
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub(crate) fn insert_erased(&mut self, actor: ActorId, handler: Box<dyn ErasedActor<C>>) {
        self.next_local_id = self.next_local_id.max(actor.local.0 + 1);

        self.actors.insert(
            actor.local,
            ActorEntry {
                kind: "erased",
                actor: handler,
            },
        );
    }

    pub(crate) fn remove_entry(&mut self, local: LocalActorId) -> Option<ActorEntry<C>> {
        self.actors.remove(&local)
    }

    pub(crate) fn insert_entry(&mut self, local: LocalActorId, entry: ActorEntry<C>) {
        self.actors.insert(local, entry);
    }
}

#[allow(dead_code)]
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

    fn handle_timer_erased(
        &mut self,
        completion: TimerCompletion,
        ctx: &mut C,
        env: &mut dyn Env,
    ) -> Result<ErasedResponse, Error> {
        let msg = A::timer_fired(completion).ok_or(Error::ActorMessageTypeMismatch)?;
        let reply = self.actor.handle(msg, ctx, env)?;

        Ok(Box::new(reply))
    }
}
