use std::marker::PhantomData;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplyTo<R> {
    pub reply: PhantomData<fn() -> R>,
}
