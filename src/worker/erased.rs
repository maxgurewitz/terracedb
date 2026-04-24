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
