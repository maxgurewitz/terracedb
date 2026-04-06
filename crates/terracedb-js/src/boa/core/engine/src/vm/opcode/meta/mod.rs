use super::RegisterOperand;
use crate::{
    Context, JsObject, JsValue,
    context::ExecutionOutcome,
    module::ModuleKind,
    native_function::{NativeFunctionResult, NativeResume},
    vm::{ActiveRunnable, CompletionRecord, opcode::Operation},
};
use std::{ops::ControlFlow, unreachable};

fn wrap_import_meta_resume(continuation: NativeResume, dst: usize, import_meta: JsObject) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(
        resume_import_meta_operation,
        (continuation, dst, import_meta),
    )
}

fn resume_import_meta_operation(
    completion: CompletionRecord,
    captures: &(NativeResume, usize, JsObject),
    context: &mut Context,
) -> crate::JsResult<NativeFunctionResult> {
    match captures.0.call(completion, context)? {
        NativeFunctionResult::Complete(record) => match record {
            CompletionRecord::Normal(_) | CompletionRecord::Return(_) => {
                context
                    .vm
                    .set_register(captures.1, JsValue::from(captures.2.clone()));
                context.continue_interruptible_vm()
            }
            CompletionRecord::Throw(error) => context.continue_interruptible_vm_with_throw(error),
            CompletionRecord::Suspend => Err(crate::JsNativeError::error()
                .with_message("import.meta continuation resumed with unexpected suspend completion")
                .into()),
        },
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_import_meta_resume(next, captures.1, captures.2.clone()),
        )),
    }
}

/// `NewTarget` implements the Opcode Operation for `Opcode::NewTarget`
///
/// Operation:
///  - Store the current new target in dst.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NewTarget;

impl NewTarget {
    #[inline(always)]
    pub(super) fn operation(dst: RegisterOperand, context: &mut Context) {
        let new_target = if let Some(new_target) = {
            let frame = context.vm.frame();
            frame
                .environments
                .get_this_environment(frame.realm.environment())
                .as_function()
                .and_then(|env| env.slots().new_target().cloned())
        } {
            new_target.into()
        } else {
            JsValue::undefined()
        };
        context.vm.set_register(dst.into(), new_target);
    }
}

impl Operation for NewTarget {
    const NAME: &'static str = "NewTarget";
    const INSTRUCTION: &'static str = "INST - NewTarget";
    const COST: u8 = 2;
}

/// `ImportMeta` implements the Opcode Operation for `Opcode::ImportMeta`
///
/// Operation:
///  - Store the current `import.meta` in dst.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ImportMeta;

impl ImportMeta {
    #[inline(always)]
    pub(super) fn operation(
        dst: RegisterOperand,
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        // Meta Properties
        //
        // ImportMeta : import . meta
        //
        // https://tc39.es/ecma262/#sec-meta-properties

        // 1. Let module be GetActiveScriptOrModule().

        let Some(ActiveRunnable::Module(module)) = context.get_active_script_or_module() else {
            unreachable!("2. Assert: module is a Source Text Module Record.");
        };

        let ModuleKind::SourceText(src) = module.kind() else {
            unreachable!("2. Assert: module is a Source Text Module Record.");
        };

        // 3. Let importMeta be module.[[ImportMeta]].
        // 4. If importMeta is empty, then
        // 5. Else,
        //     a. Assert: importMeta is an Object.
        let import_meta = if let Some(import_meta) = src.import_meta().borrow().as_ref().cloned() {
            import_meta
        } else {
            let import_meta = JsObject::with_null_proto();
            let outcome = match context
                .module_loader()
                .init_import_meta(&import_meta, &module, context)
            {
                Ok(outcome) => outcome,
                Err(error) => return context.handle_error(error),
            };
            match outcome {
                ExecutionOutcome::Complete(()) => {
                    *src.import_meta().borrow_mut() = Some(import_meta.clone());
                    import_meta
                }
                ExecutionOutcome::Suspend(continuation) => {
                    context.install_interruptible_resume(wrap_import_meta_resume(
                        continuation,
                        usize::from(dst),
                        import_meta,
                    ));
                    return ControlFlow::Break(CompletionRecord::Suspend);
                }
            }
        };

        //     b. Return importMeta.
        //     f. Return importMeta.
        context.vm.set_register(dst.into(), import_meta.into());
        ControlFlow::Continue(())
    }
}

impl Operation for ImportMeta {
    const NAME: &'static str = "ImportMeta";
    const INSTRUCTION: &'static str = "INST - ImportMeta";
    const COST: u8 = 6;
}
