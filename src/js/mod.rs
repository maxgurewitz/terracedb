mod attachment;
mod bindings;
mod bytecode;
mod compile;
mod heap;
mod ids;
mod modules;
mod output;
mod pool;
mod runtime;
mod storage;
mod symbol;
mod value;
mod vm;

pub use attachment::{
    AttachmentHostCtx, AttachmentInstallCtx, ConsoleAttachment, CoreHostAttachments,
    CoreHostConfig, HostModuleInstallCtx, JsAttachment, JsAttachmentBundle, JsHostBindings,
};
pub use bindings::{Binding, BindingCellId, BindingKind, EnvFrame, EnvFrameId, EnvStack};
pub use bytecode::{
    BytecodeProgram, CompiledFunction, ConstId, Constant, ConstantPool, FunctionId, Instr,
};
pub use compile::{JsCompileError, JsSpan, compile_module_source, compile_source_to_bytecode};
pub use heap::{
    DEFAULT_GC_THRESHOLD_BYTES, GcHeader, GcMark, GcPolicy, HeapObject, HeapStats, HostFunction,
    HostFunctionKind, JsFunction, JsHeap, JsObject, JsProperty, ModuleNamespace, ObjectId,
    ObjectKind, PropertyKey,
};
pub use ids::{JsRuntimeId, ModuleId, ProgramId, StackId};
pub use modules::{
    CompiledModule, ExportName, ImportEntry, ImportName, IndirectExportEntry, LocalBindingEntry,
    LocalExportEntry, ModuleKey, ModuleRecord, ModuleRegistry, ModuleState, ResolvedExport,
    StarExportEntry,
};
pub use output::{ChannelByteSink, JsOutputChunk, JsOutputReceiver, JsOutputSender, JsStreamKind};
pub use pool::{JsPoolMsg, JsPoolReply, JsRuntimePoolActor, JsRuntimePoolConfig};
pub(crate) use runtime::{
    RuntimeDetached, RuntimeRecord, RuntimeRecordView, RuntimeRecordViewRef, RuntimeSnapshot,
};
pub use storage::{
    FlatSegmentStore, PoolEnvStorage, PoolHeapStorage, PoolModuleStorage, PoolProgramStorage,
    PoolRuntimeStorage, PoolSymbolStorage, PoolVmStorage, RuntimeRecordId, RuntimeSlot,
    RuntimeState, RuntimeStorageSegments, RuntimeStorageUsage, RuntimeTable, SegmentId,
    SegmentKind, SegmentMeta, SegmentTable, StoredFunction, StoredProgram, VmStack,
};
pub(crate) use storage::{ModuleRegistryId, SymbolTableId, VmStateId};
pub use symbol::{Symbol, SymbolTable};
pub use value::JsValue;
pub use vm::{InstructionBudget, RunResult, Vm};
