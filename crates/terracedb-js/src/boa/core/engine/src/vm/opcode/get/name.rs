use std::ops::ControlFlow;

use crate::{
    Context, JsResult, JsValue,
    context::ExecutionOutcome,
    error::JsNativeError,
    native_function::{NativeFunctionResult, NativeResume},
    object::{
        InterruptibleCallOutcome,
        internal_methods::InternalMethodPropertyContext,
        shape::slot::SlotAttributes,
    },
    property::PropertyKey,
    vm::{CompletionRecord, opcode::{IndexOperand, Operation, RegisterOperand}},
};

fn wrap_get_name_resume(continuation: NativeResume, dst: usize) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_get_name, (continuation, dst))
}

fn resume_get_name(
    completion: CompletionRecord,
    captures: &(NativeResume, usize),
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match captures.0.call(completion, context)? {
        NativeFunctionResult::Complete(record) => match record {
            CompletionRecord::Normal(value) | CompletionRecord::Return(value) => {
                context.vm.set_register(captures.1, value);
                context.continue_interruptible_vm()
            }
            CompletionRecord::Throw(err) => context.continue_interruptible_vm_with_throw(err),
            CompletionRecord::Suspend => Err(JsNativeError::error()
                .with_message("get name continuation resumed with unexpected suspend completion")
                .into()),
        },
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_get_name_resume(next, captures.1),
        )),
    }
}

/// `GetName` implements the Opcode Operation for `Opcode::GetName`
///
/// Operation:
///  - Find a binding on the environment chain and store its value in dst.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetName;

impl GetName {
    #[inline(always)]
    pub(crate) fn operation(
        (value, index): (RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let mut binding_locator =
            context.vm.frame().code_block.bindings[usize::from(index)].clone();
        context.find_runtime_binding(&mut binding_locator)?;
        let result = context.get_binding(&binding_locator)?.ok_or_else(|| {
            let name = binding_locator.name().to_std_string_escaped();
            JsNativeError::reference().with_message(format!("{name} is not defined"))
        })?;
        context.vm.set_register(value.into(), result);
        Ok(())
    }
}

impl Operation for GetName {
    const NAME: &'static str = "GetName";
    const INSTRUCTION: &'static str = "INST - GetName";
    const COST: u8 = 4;
}

/// `GetNameGlobal` implements the Opcode Operation for `Opcode::GetNameGlobal`
///
/// Operation:
///  - Find a binding in the global object and store its value in dst.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetNameGlobal;

impl GetNameGlobal {
    #[inline(always)]
    pub(crate) fn operation(
        (dst, index, ic_index): (RegisterOperand, IndexOperand, IndexOperand),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let mut binding_locator =
            context.vm.frame().code_block.bindings[usize::from(index)].clone();
        if let Err(error) = context.find_runtime_binding(&mut binding_locator) {
            return context.handle_error(error);
        }

        if binding_locator.is_global() {
            let object = context.global_object();

            let ic = &context.vm.frame().code_block().ic[usize::from(ic_index)];

            let object_borrowed = object.borrow();
            if let Some((shape, slot)) = ic.get(object_borrowed.shape()) {
                let mut result = if slot.attributes.contains(SlotAttributes::PROTOTYPE) {
                    let prototype = shape.prototype().expect("prototype should have value");
                    let prototype = prototype.borrow();
                    prototype.properties().storage[slot.index as usize].clone()
                } else {
                    object_borrowed.properties().storage[slot.index as usize].clone()
                };

                drop(object_borrowed);
                if slot.attributes.has_get() && result.is_object() {
                    match result
                        .as_object()
                        .expect("should contain getter")
                        .call_interruptible(&object.clone().into(), &[], context)
                    {
                        Ok(InterruptibleCallOutcome::Complete(value)) => result = value,
                        Ok(InterruptibleCallOutcome::Suspend(continuation)) => {
                            context.install_interruptible_resume(wrap_get_name_resume(
                                continuation,
                                usize::from(dst),
                            ));
                            return ControlFlow::Break(CompletionRecord::Suspend);
                        }
                        Err(error) => return context.handle_error(error),
                    }
                }
                context.vm.set_register(dst.into(), result);
                return ControlFlow::Continue(());
            }

            drop(object_borrowed);

            let key: PropertyKey = ic.name.clone().into();

            let context = &mut InternalMethodPropertyContext::new(context);
            let result = match object.__try_get_interruptible__(&key, object.clone().into(), context) {
                Ok(ExecutionOutcome::Complete(result)) => result,
                Ok(ExecutionOutcome::Suspend(continuation)) => {
                    context.install_interruptible_resume(wrap_get_name_resume(
                        continuation,
                        usize::from(dst),
                    ));
                    return ControlFlow::Break(CompletionRecord::Suspend);
                }
                Err(error) => return context.handle_error(error),
            };
            let Some(result) = result else {
                let name = binding_locator.name().to_std_string_escaped();
                return context.handle_error(
                    JsNativeError::reference()
                        .with_message(format!("{name} is not defined"))
                        .into(),
                );
            };

            // Cache the property.
            let slot = *context.slot();
            if slot.is_cacheable() {
                let ic = &context.vm.frame().code_block.ic[usize::from(ic_index)];
                let object_borrowed = object.borrow();
                let shape = object_borrowed.shape();
                ic.set(shape, slot);
            }

            context.vm.set_register(dst.into(), result);
            return ControlFlow::Continue(());
        }

        let result = match context.get_binding(&binding_locator) {
            Ok(Some(result)) => result,
            Ok(None) => {
                let name = binding_locator.name().to_std_string_escaped();
                return context.handle_error(
                    JsNativeError::reference()
                        .with_message(format!("{name} is not defined"))
                        .into(),
                );
            }
            Err(error) => return context.handle_error(error),
        };

        context.vm.set_register(dst.into(), result);
        ControlFlow::Continue(())
    }
}

impl Operation for GetNameGlobal {
    const NAME: &'static str = "GetNameGlobal";
    const INSTRUCTION: &'static str = "INST - GetNameGlobal";
    const COST: u8 = 4;
}

/// `GetLocator` implements the Opcode Operation for `Opcode::GetLocator`
///
/// Operation:
///  - Find a binding on the environment and set the `current_binding` of the current frame.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetLocator;

impl GetLocator {
    #[inline(always)]
    pub(crate) fn operation(index: IndexOperand, context: &mut Context) -> JsResult<()> {
        let mut binding_locator =
            context.vm.frame().code_block.bindings[usize::from(index)].clone();
        context.find_runtime_binding(&mut binding_locator)?;

        context.vm.frame_mut().binding_stack.push(binding_locator);

        Ok(())
    }
}

impl Operation for GetLocator {
    const NAME: &'static str = "GetLocator";
    const INSTRUCTION: &'static str = "INST - GetLocator";
    const COST: u8 = 4;
}

/// `GetNameAndLocator` implements the Opcode Operation for `Opcode::GetNameAndLocator`
///
/// Operation:
///  - Find a binding on the environment chain and store its value in dst, setting the
///    `current_binding` of the current frame.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetNameAndLocator;

impl GetNameAndLocator {
    #[inline(always)]
    pub(crate) fn operation(
        (value, index): (RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let mut binding_locator =
            context.vm.frame().code_block.bindings[usize::from(index)].clone();
        context.find_runtime_binding(&mut binding_locator)?;
        let result = context.get_binding(&binding_locator)?.ok_or_else(|| {
            let name = binding_locator.name().to_std_string_escaped();
            JsNativeError::reference().with_message(format!("{name} is not defined"))
        })?;

        context.vm.frame_mut().binding_stack.push(binding_locator);
        context.vm.set_register(value.into(), result);
        Ok(())
    }
}

impl Operation for GetNameAndLocator {
    const NAME: &'static str = "GetNameAndLocator";
    const INSTRUCTION: &'static str = "INST - GetNameAndLocator";
    const COST: u8 = 4;
}

/// `GetNameOrUndefined` implements the Opcode Operation for `Opcode::GetNameOrUndefined`
///
/// Operation:
///  - Find a binding on the environment chain and store its value in dst. If the binding does not exist, store undefined.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetNameOrUndefined;

impl GetNameOrUndefined {
    #[inline(always)]
    pub(crate) fn operation(
        (value, index): (RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let mut binding_locator =
            context.vm.frame().code_block.bindings[usize::from(index)].clone();

        let is_global = binding_locator.is_global();

        context.find_runtime_binding(&mut binding_locator)?;

        let result = if let Some(value) = context.get_binding(&binding_locator)? {
            value
        } else if is_global {
            JsValue::undefined()
        } else {
            let name = binding_locator.name().to_std_string_escaped();
            return Err(JsNativeError::reference()
                .with_message(format!("{name} is not defined"))
                .into());
        };

        context.vm.set_register(value.into(), result);
        Ok(())
    }
}

impl Operation for GetNameOrUndefined {
    const NAME: &'static str = "GetNameOrUndefined";
    const INSTRUCTION: &'static str = "INST - GetNameOrUndefined";
    const COST: u8 = 4;
}
