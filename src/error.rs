use crate::{ActorId, WorkerId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    ActorNotFound { actor: ActorId },
    ActorMessageTypeMismatch,
    ActorRegistryPoisoned,
    ActorReplyTypeMismatch,
    CompioRuntimeInit { worker: WorkerId, source: String },
    HostReplyClosed,
    InvalidWorkerIndex { worker: usize },
    MissingSimulationSeed,
    MissingSimulationWorkerCount,
    NoCores,
    NoWorkers,
    PinFailed { worker: WorkerId },
    ReentrantActorCall { actor: ActorId },
    ThreadPanicked,
    ThreadSpawn { worker: WorkerId, source: String },
    WorkerInboxClosed,
    WorkerInitChannelClosed,
    WorkerSubmit,
}
