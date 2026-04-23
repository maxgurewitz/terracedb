use super::ShardCtx;
use crate::{Env, Error};

pub trait Actor {
    type Msg;
    type Reply;

    fn handle<C: ShardCtx + ?Sized>(
        &mut self,
        _msg: Self::Msg,
        _ctx: &mut C,
        _env: &mut dyn Env,
    ) -> Result<Self::Reply, Error> {
        panic!("Actor::handle stub")
    }
}
