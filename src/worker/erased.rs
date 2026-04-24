use crate::{Env, Error};

pub type ErasedActorMsg = Box<dyn std::any::Any + Send>;

pub type ErasedResponse = Box<dyn std::any::Any + Send>;

pub(crate) trait ErasedActor<C>: Send {
    fn handle_erased(
        &mut self,
        _msg: ErasedActorMsg,
        _ctx: &mut C,
        _env: &mut dyn Env,
    ) -> Result<ErasedResponse, Error> {
        panic!("ErasedActor::handle_erased stub")
    }
}

pub(crate) trait ErasedShardCtx {
    fn worker_id(&self) -> crate::WorkerId {
        panic!("ErasedShardCtx::worker_id stub")
    }

    fn send_remote(
        &mut self,
        _worker: crate::WorkerHandle,
        _msg: crate::WorkerMsg,
    ) -> Result<(), Error> {
        panic!("ErasedShardCtx::send_remote stub")
    }

    fn reply(&mut self, _reply_to: crate::ReplyTo, _response: ErasedResponse) -> Result<(), Error> {
        panic!("ErasedShardCtx::reply stub")
    }
}
