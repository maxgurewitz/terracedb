use super::{IndexOperand, RegisterOperand};
use crate::{
    Context, JsResult,
    context::ExecutionOutcome,
    error::JsNativeError,
    native_function::{NativeFunctionResult, NativeResume},
    vm::{CompletionRecord, opcode::Operation},
};
use std::ops::ControlFlow;

fn wrap_has_property_resume(continuation: NativeResume, dst: usize) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_has_property_operation, (continuation, dst))
}

fn resume_has_property_operation(
    completion: CompletionRecord,
    captures: &(NativeResume, usize),
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match captures.0.call(completion, context)? {
        NativeFunctionResult::Value(value) => {
            context.vm.set_register(captures.1, value);
            context.continue_interruptible_vm()
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_has_property_resume(next, captures.1),
        )),
    }
}

pub(crate) mod logical;
pub(crate) mod macro_defined;

pub(crate) use logical::*;
pub(crate) use macro_defined::*;

/// `StrictEq` implements the Opcode Operation for `Opcode::StrictEq`
///
/// Operation:
///  - Binary `===` operation
#[derive(Debug, Clone, Copy)]
pub(crate) struct StrictEq;

impl StrictEq {
    #[inline(always)]
    pub(super) fn operation(
        (dst, lhs, rhs): (RegisterOperand, RegisterOperand, RegisterOperand),
        context: &mut Context,
    ) {
        let lhs = context.vm.get_register(lhs.into());
        let rhs = context.vm.get_register(rhs.into());
        let value = lhs.strict_equals(rhs);
        context.vm.set_register(dst.into(), value.into());
    }
}

impl Operation for StrictEq {
    const NAME: &'static str = "StrictEq";
    const INSTRUCTION: &'static str = "INST - StrictEq";
    const COST: u8 = 2;
}

/// `StrictNotEq` implements the Opcode Operation for `Opcode::StrictNotEq`
///
/// Operation:
///  - Binary `!==` operation
#[derive(Debug, Clone, Copy)]
pub(crate) struct StrictNotEq;

impl StrictNotEq {
    #[inline(always)]
    pub(super) fn operation(
        (dst, lhs, rhs): (RegisterOperand, RegisterOperand, RegisterOperand),
        context: &mut Context,
    ) {
        let lhs = context.vm.get_register(lhs.into());
        let rhs = context.vm.get_register(rhs.into());
        let value = !lhs.strict_equals(rhs);
        context.vm.set_register(dst.into(), value.into());
    }
}

impl Operation for StrictNotEq {
    const NAME: &'static str = "StrictNotEq";
    const INSTRUCTION: &'static str = "INST - StrictNotEq";
    const COST: u8 = 2;
}

/// `In` implements the Opcode Operation for `Opcode::In`
///
/// Operation:
///  - Binary `in` operation
#[derive(Debug, Clone, Copy)]
pub(crate) struct In;

impl In {
    #[inline(always)]
    pub(super) fn operation(
        (dst, lhs, rhs): (RegisterOperand, RegisterOperand, RegisterOperand),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let rhs = context.vm.get_register(rhs.into()).clone();
        let Some(rhs) = rhs.as_object() else {
            return context.handle_error(
                JsNativeError::typ()
                    .with_message(format!(
                        "right-hand side of 'in' should be an object, got `{}`",
                        rhs.type_of()
                    ))
                    .into(),
            );
        };
        let lhs = context.vm.get_register(lhs.into()).clone();
        let key = match lhs.to_property_key(context) {
            Ok(key) => key,
            Err(error) => return context.handle_error(error),
        };
        match rhs.has_property_interruptible(key, context) {
            Ok(ExecutionOutcome::Complete(value)) => {
                context.vm.set_register(dst.into(), value.into());
                ControlFlow::Continue(())
            }
            Ok(ExecutionOutcome::Suspend(continuation)) => {
                context.install_interruptible_resume(wrap_has_property_resume(
                    continuation,
                    usize::from(dst),
                ));
                ControlFlow::Break(CompletionRecord::Suspend)
            }
            Err(error) => context.handle_error(error),
        }
    }
}

impl Operation for In {
    const NAME: &'static str = "In";
    const INSTRUCTION: &'static str = "INST - In";
    const COST: u8 = 3;
}

/// `InPrivate` implements the Opcode Operation for `Opcode::InPrivate`
///
/// Operation:
///  - Binary `in` operation for private names.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InPrivate;

impl InPrivate {
    #[inline(always)]
    pub(super) fn operation(
        (dst, index, rhs): (RegisterOperand, IndexOperand, RegisterOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let name = context
            .vm
            .frame()
            .code_block()
            .constant_string(index.into());
        let rhs = context.vm.get_register(rhs.into());

        let Some(rhs) = rhs.as_object() else {
            return Err(JsNativeError::typ()
                .with_message(format!(
                    "right-hand side of 'in' should be an object, got `{}`",
                    rhs.type_of()
                ))
                .into());
        };

        let name = context
            .vm
            .frame()
            .environments
            .resolve_private_identifier(name)
            .expect("private name must be in environment");

        let value = rhs.private_element_find(&name, true, true).is_some();

        context.vm.set_register(dst.into(), value.into());
        Ok(())
    }
}

impl Operation for InPrivate {
    const NAME: &'static str = "InPrivate";
    const INSTRUCTION: &'static str = "INST - InPrivate";
    const COST: u8 = 4;
}
