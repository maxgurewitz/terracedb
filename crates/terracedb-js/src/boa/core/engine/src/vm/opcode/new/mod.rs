use super::IndexOperand;
use crate::{
    Context,
    error::JsNativeError,
    native_function::{NativeFunctionResult, NativeResume},
    object::internal_methods::ResolvedCallValue,
    vm::{CompletionRecord, opcode::Operation},
};
use std::ops::ControlFlow;

fn wrap_construct_resume(continuation: NativeResume) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_construct_operation, continuation)
}

fn resume_construct_operation(
    completion: CompletionRecord,
    continuation: &NativeResume,
    context: &mut Context,
) -> crate::JsResult<NativeFunctionResult> {
    match continuation.call(completion, context)? {
        NativeFunctionResult::Complete(record) => match record {
            CompletionRecord::Normal(value) | CompletionRecord::Return(value) => {
                context.vm.stack.push(value);
                context.continue_interruptible_vm()
            }
            CompletionRecord::Throw(err) => context.continue_interruptible_vm_with_throw(err),
            CompletionRecord::Suspend => Err(crate::error::JsNativeError::error()
                .with_message("construct continuation resumed with unexpected suspend completion")
                .into()),
        },
        NativeFunctionResult::Suspend(next) => {
            Ok(NativeFunctionResult::Suspend(wrap_construct_resume(next)))
        }
    }
}

/// `New` implements the Opcode Operation for `Opcode::New`
///
/// Operation:
///  - Call construct on a function.
#[derive(Debug, Clone, Copy)]
pub(crate) struct New;

impl New {
    #[inline(always)]
    pub(super) fn operation(
        argument_count: IndexOperand,
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let func = context
            .vm
            .stack
            .calling_convention_get_function(argument_count.into());

        let cons = match func.as_object() {
            Some(object) => object.clone(),
            None => return context.handle_error(JsNativeError::typ().with_message("not a constructor").into()),
        };

        context.vm.stack.push(cons.clone()); // Push new.target

        match cons.__construct__(argument_count.into()).resolve(context) {
            Ok(ResolvedCallValue::Ready | ResolvedCallValue::Complete) => ControlFlow::Continue(()),
            Ok(ResolvedCallValue::Suspended(continuation)) => {
                context.install_interruptible_resume(wrap_construct_resume(continuation));
                ControlFlow::Break(CompletionRecord::Suspend)
            }
            Err(error) => context.handle_error(error),
        }
    }
}

impl Operation for New {
    const NAME: &'static str = "New";
    const INSTRUCTION: &'static str = "INST - New";
    const COST: u8 = 3;
}

/// `NewSpread` implements the Opcode Operation for `Opcode::NewSpread`
///
/// Operation:
///  - Call construct on a function where the arguments contain spreads.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NewSpread;

impl NewSpread {
    #[inline(always)]
    pub(super) fn operation((): (), context: &mut Context) -> ControlFlow<CompletionRecord> {
        // Get the arguments that are stored as an array object on the stack.
        let arguments_array = context.vm.stack.pop();
        let arguments_array_object = arguments_array
            .as_object()
            .expect("arguments array in call spread function must be an object");
        let arguments = arguments_array_object
            .borrow()
            .properties()
            .to_dense_indexed_properties()
            .expect("arguments array in call spread function must be dense");

        let func = context.vm.stack.pop();

        let cons = match func.as_object() {
            Some(object) => object.clone(),
            None => return context.handle_error(JsNativeError::typ().with_message("not a constructor").into()),
        };

        let argument_count = arguments.len();
        context.vm.stack.push(func);
        context
            .vm
            .stack
            .calling_convention_push_arguments(&arguments);
        context.vm.stack.push(cons.clone()); // Push new.target

        match cons.__construct__(argument_count).resolve(context) {
            Ok(ResolvedCallValue::Ready | ResolvedCallValue::Complete) => ControlFlow::Continue(()),
            Ok(ResolvedCallValue::Suspended(continuation)) => {
                context.install_interruptible_resume(wrap_construct_resume(continuation));
                ControlFlow::Break(CompletionRecord::Suspend)
            }
            Err(error) => context.handle_error(error),
        }
    }
}

impl Operation for NewSpread {
    const NAME: &'static str = "NewSpread";
    const INSTRUCTION: &'static str = "INST - NewSpread";
    const COST: u8 = 3;
}
