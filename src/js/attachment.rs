use crate::Error;

use super::{
    BindingKind, EnvStack, HostFunction, HostFunctionKind, JsHeap, JsOutputSender, JsRuntimeId,
    JsValue, ObjectKind, PropertyKey, SymbolTable,
};

pub trait JsAttachment: Send {
    fn install(&self, ctx: &mut AttachmentInstallCtx<'_>) -> Result<(), Error>;
}

pub struct AttachmentInstallCtx<'a> {
    pub runtime_id: JsRuntimeId,
    pub symbols: &'a mut SymbolTable,
    pub heap: &'a mut JsHeap,
    pub global_env: &'a mut EnvStack,
}

#[derive(Clone)]
pub struct ConsoleAttachment {
    pub output_tx: JsOutputSender,
}

impl JsAttachment for ConsoleAttachment {
    fn install(&self, ctx: &mut AttachmentInstallCtx<'_>) -> Result<(), Error> {
        let console_sym = ctx.symbols.intern("console");
        let log_sym = ctx.symbols.intern("log");
        let error_sym = ctx.symbols.intern("error");

        let console_obj = ctx.heap.alloc_object(ObjectKind::Ordinary);
        let log_fn = ctx
            .heap
            .alloc_object(ObjectKind::HostFunction(HostFunction {
                name: log_sym,
                kind: HostFunctionKind::ConsoleLog {
                    runtime_id: ctx.runtime_id,
                    output_tx: self.output_tx.clone(),
                },
            }));
        let error_fn = ctx
            .heap
            .alloc_object(ObjectKind::HostFunction(HostFunction {
                name: error_sym,
                kind: HostFunctionKind::ConsoleError {
                    runtime_id: ctx.runtime_id,
                    output_tx: self.output_tx.clone(),
                },
            }));

        ctx.heap.set_property(
            console_obj,
            PropertyKey::Symbol(log_sym),
            JsValue::Object(log_fn),
        )?;
        ctx.heap.set_property(
            console_obj,
            PropertyKey::Symbol(error_sym),
            JsValue::Object(error_fn),
        )?;
        ctx.global_env.declare_current_value(
            console_sym,
            BindingKind::Const,
            JsValue::Object(console_obj),
            ctx.heap,
            ctx.symbols,
        )
    }
}
