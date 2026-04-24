use super::ShardCtx;
use crate::{Env, Error};

pub trait Actor<C: ShardCtx> {
    type Msg: Send + 'static;
    type Reply: Send + 'static;

    fn handle(
        &mut self,
        _msg: Self::Msg,
        _ctx: &mut C,
        _env: &mut dyn Env,
    ) -> Result<Self::Reply, Error> {
        panic!("Actor::handle stub")
    }
}
