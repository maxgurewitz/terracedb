use crate::{ActorId, JsCompileError, JsRuntimeId, WorkerId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    ActorNotFound {
        actor: ActorId,
    },
    ActorMessageTypeMismatch,
    ActorRegistryPoisoned,
    ActorReplyTypeMismatch,
    CompioRuntimeInit {
        worker: WorkerId,
        source: String,
    },
    HostReplyClosed,
    InvalidWorkerIndex {
        worker: usize,
    },
    JsAssignToConst {
        name: String,
    },
    JsBindingCellNotFound {
        cell: u32,
    },
    JsBindingNotFound {
        name: String,
    },
    JsCannotPopRootScope,
    JsCompile(JsCompileError),
    JsDuplicateBinding {
        name: String,
    },
    JsEnvFrameNotFound {
        frame: u32,
    },
    JsIdentifierNotFound {
        name: String,
    },
    JsInstructionPointerOutOfBounds {
        ip: usize,
    },
    JsInvalidConstant {
        id: u32,
    },
    JsInvalidConsoleCall,
    JsInvalidFunction {
        id: u32,
    },
    JsInvalidOperand,
    JsModuleExportAmbiguous {
        export: String,
    },
    JsModuleExportNotFound {
        specifier: String,
        export: String,
    },
    JsModuleNotDefined {
        specifier: String,
    },
    JsNotCallable,
    JsRuntimeAlreadyExists(JsRuntimeId),
    JsSegmentNotFound {
        segment: u64,
    },
    JsSegmentOwnerMismatch {
        segment: u64,
        expected: JsRuntimeId,
        actual: JsRuntimeId,
    },
    JsSnapshot(String),
    JsHeapLeak {
        objects: usize,
        bytes: usize,
    },
    JsObjectNotFound {
        object: u64,
    },
    JsRefCountUnderflow {
        object: u64,
    },
    JsStackUnderflow,
    JsTypeError {
        message: String,
    },
    JsUninitializedBinding {
        name: String,
    },
    MissingConsole,
    MissingSimulationSeed,
    MissingSimulationWorkerCount,
    NoCores,
    NoWorkers,
    OutputReceiverDropped,
    PinFailed {
        worker: WorkerId,
    },
    ReentrantActorCall {
        actor: ActorId,
    },
    RuntimeNotFound(JsRuntimeId),
    ThreadPanicked,
    ThreadSpawn {
        worker: WorkerId,
        source: String,
    },
    WorkerInboxClosed,
    WorkerInitChannelClosed,
    WorkerSubmit,
}
