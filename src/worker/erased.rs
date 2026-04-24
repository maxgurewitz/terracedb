use crate::{Env, Error, TimerCompletion};

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

    fn handle_timer_erased(
        &mut self,
        _completion: TimerCompletion,
        _ctx: &mut C,
        _env: &mut dyn Env,
    ) -> Result<ErasedResponse, Error> {
        panic!("ErasedActor::handle_timer_erased stub")
    }
}
