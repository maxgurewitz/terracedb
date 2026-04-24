use super::{Actor, LocalActorRef, ReplyTo};
use crate::{Env, ErasedResponse, Error, WorkerHandle, WorkerId, WorkerMsg};

pub trait ShardCtx {
    fn worker_id(&self) -> WorkerId {
        panic!("ShardCtx::worker_id stub")
    }

    fn call_local<A: Actor<Self>>(
        &mut self,
        _actor: LocalActorRef<A>,
        _msg: A::Msg,
        _env: &mut dyn Env,
    ) -> Result<A::Reply, Error>
    where
        Self: Sized,
    {
        panic!("ShardCtx::call_local stub")
    }

    fn send_remote(&mut self, _worker: WorkerHandle, _msg: WorkerMsg) -> Result<(), Error> {
        panic!("ShardCtx::send_remote stub")
    }

    fn reply(&mut self, _reply_to: ReplyTo, _response: ErasedResponse) -> Result<(), Error> {
        panic!("ShardCtx::reply stub")
    }
}
