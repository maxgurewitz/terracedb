use super::{Actor, ActorRef, LocalActorRef, ReplyTo, ShardId};
use crate::{Env, Error};

pub trait ShardCtx {
    fn shard_id(&self) -> ShardId {
        panic!("ShardCtx::shard_id stub")
    }

    fn call_local<A: Actor>(
        &mut self,
        _actor: LocalActorRef<A>,
        _msg: A::Msg,
        _env: &mut dyn Env,
    ) -> Result<A::Reply, Error> {
        panic!("ShardCtx::call_local stub")
    }

    fn send<A: Actor>(
        &mut self,
        _actor: ActorRef<A>,
        _msg: A::Msg,
        _reply_to: Option<ReplyTo<A::Reply>>,
    ) -> Result<(), Error> {
        panic!("ShardCtx::send stub")
    }

    fn reply<R>(&mut self, _reply_to: ReplyTo<R>, _value: R) -> Result<(), Error> {
        panic!("ShardCtx::reply stub")
    }
}
