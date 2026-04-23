use super::{ActorId, WorkerId};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OpId;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CompletionTarget {
    pub worker: WorkerId,
    pub actor: ActorId,
    pub op: OpId,
}
