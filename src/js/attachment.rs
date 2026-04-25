use crate::Error;

use super::{
    BindingKind, EnvStack, HostFunction, HostFunctionKind, JsHeap, JsOutputChunk, JsOutputSender,
    JsRuntimeId, JsStreamKind, JsValue, ObjectKind, PropertyKey, SymbolTable,
};

pub trait JsAttachment: Send {
    fn install(&self, ctx: &mut AttachmentInstallCtx<'_>) -> Result<(), Error>;

    fn bind_host(&self, _ctx: &mut AttachmentHostCtx<'_>) -> Result<(), Error> {
        Ok(())
    }
}

pub struct AttachmentInstallCtx<'a> {
    pub runtime_id: JsRuntimeId,
    pub symbols: &'a mut SymbolTable,
    pub heap: &'a mut JsHeap,
    pub global_env: &'a mut EnvStack,
    pub host: &'a mut JsHostBindings,
}

pub struct AttachmentHostCtx<'a> {
    pub runtime_id: JsRuntimeId,
    pub host: &'a mut JsHostBindings,
}

#[derive(Clone, Default)]
pub struct JsHostBindings {
    runtime_id: Option<JsRuntimeId>,
    console_output_tx: Option<JsOutputSender>,
}

impl JsHostBindings {
    pub(crate) fn bind_console(&mut self, runtime_id: JsRuntimeId, output_tx: JsOutputSender) {
        self.runtime_id = Some(runtime_id);
        self.console_output_tx = Some(output_tx);
    }

    pub(crate) fn emit_console(&self, stream: JsStreamKind, args: &[JsValue]) -> Result<(), Error> {
        let runtime_id = self.runtime_id.ok_or(Error::MissingConsole)?;
        let output_tx = self
            .console_output_tx
            .as_ref()
            .ok_or(Error::MissingConsole)?;
        let mut line = args
            .iter()
            .map(JsValue::stringify)
            .collect::<Vec<_>>()
            .join(" ");
        line.push('\n');

        output_tx
            .send(JsOutputChunk {
                runtime_id,
                stream,
                bytes: bytes::Bytes::from(line),
            })
            .map_err(|_| Error::OutputReceiverDropped)
    }
}

#[derive(Clone)]
pub struct ConsoleAttachment {
    pub output_tx: JsOutputSender,
}

impl JsAttachment for ConsoleAttachment {
    fn install(&self, ctx: &mut AttachmentInstallCtx<'_>) -> Result<(), Error> {
        ctx.host
            .bind_console(ctx.runtime_id, self.output_tx.clone());

        let console_sym = ctx.symbols.intern("console");
        let log_sym = ctx.symbols.intern("log");
        let error_sym = ctx.symbols.intern("error");

        let console_obj = ctx.heap.alloc_object(ObjectKind::Ordinary);
        let log_fn = ctx
            .heap
            .alloc_object(ObjectKind::HostFunction(HostFunction {
                name: log_sym,
                kind: HostFunctionKind::ConsoleLog,
            }));
        let error_fn = ctx
            .heap
            .alloc_object(ObjectKind::HostFunction(HostFunction {
                name: error_sym,
                kind: HostFunctionKind::ConsoleError,
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

    fn bind_host(&self, ctx: &mut AttachmentHostCtx<'_>) -> Result<(), Error> {
        ctx.host
            .bind_console(ctx.runtime_id, self.output_tx.clone());
        Ok(())
    }
}
