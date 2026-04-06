use boa_string::StaticJsStrings;
use std::ops::ControlFlow;

use crate::{
    Context, JsValue, js_string,
    context::ExecutionOutcome,
    native_function::{NativeFunctionResult, NativeResume},
    object::{
        InterruptibleCallOutcome,
        internal_methods::InternalMethodPropertyContext,
        shape::slot::SlotAttributes,
    },
    property::PropertyKey,
    vm::{CompletionRecord, opcode::{IndexOperand, Operation, RegisterOperand}},
};

fn wrap_get_resume(
    continuation: NativeResume,
    dst: usize,
    key_update: Option<(usize, JsValue)>,
) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(
        resume_get_into_register,
        (continuation, dst, key_update),
    )
}

fn resume_get_into_register(
    completion: CompletionRecord,
    captures: &(NativeResume, usize, Option<(usize, JsValue)>),
    context: &mut Context,
) -> crate::JsResult<NativeFunctionResult> {
    match captures.0.call(completion, context)? {
        NativeFunctionResult::Value(value) => {
            if let Some((key, key_value)) = &captures.2 {
                context.vm.set_register(*key, key_value.clone());
            }
            context.vm.set_register(captures.1, value);
            context.continue_interruptible_vm()
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_get_resume(next, captures.1, captures.2.clone()),
        )),
    }
}

fn get_by_name<const LENGTH: bool>(
    (dst, object, receiver, index): (RegisterOperand, &JsValue, &JsValue, IndexOperand),
    context: &mut Context,
) -> ControlFlow<CompletionRecord> {
    if LENGTH {
        if let Some(object) = object.as_object()
            && object.is_array()
        {
            let value = object.borrow().properties().storage[0].clone();
            context.vm.set_register(dst.into(), value);
            return ControlFlow::Continue(());
        } else if let Some(string) = object.as_string() {
            // NOTE: Since we’re using the prototype returned directly by `base_class()`,
            //       we need to handle string primitives separately due to the
            //       string exotic internal methods.
            context
                .vm
                .set_register(dst.into(), (string.len() as u32).into());
            return ControlFlow::Continue(());
        }
    }

    // OPTIMIZATION:
    //    Instead of calling `to_object()`, which creates a temporary wrapper object for primitive
    //    values (e.g., numbers, strings, booleans) just to query their prototype chain.
    //
    //    To prevent the creation of a temporary JsObject, we directly retrieve the prototype that
    //    `to_object()` would produce, such as `Number.prototype`, `String.prototype`, etc.
    let object = match object.base_class(context) {
        Ok(object) => object,
        Err(error) => return context.handle_error(error),
    };

    let ic = &context.vm.frame().code_block().ic[usize::from(index)];
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
                .call_interruptible(receiver, &[], context)
            {
                Ok(InterruptibleCallOutcome::Complete(value)) => result = value,
                Ok(InterruptibleCallOutcome::Suspend(continuation)) => {
                    context.install_interruptible_resume(wrap_get_resume(
                        continuation,
                        usize::from(dst),
                        None,
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
    let result = match object.__get_interruptible__(&key, receiver.clone(), context) {
        Ok(ExecutionOutcome::Complete(value)) => value,
        Ok(ExecutionOutcome::Suspend(continuation)) => {
            context.install_interruptible_resume(wrap_get_resume(
                continuation,
                usize::from(dst),
                None,
            ));
            return ControlFlow::Break(CompletionRecord::Suspend);
        }
        Err(error) => return context.handle_error(error),
    };

    // Cache the property.
    let slot = *context.slot();
    if slot.is_cacheable() {
        let ic = &context.vm.frame().code_block.ic[usize::from(index)];
        let object_borrowed = object.borrow();
        let shape = object_borrowed.shape();
        ic.set(shape, slot);
    }

    context.vm.set_register(dst.into(), result);
    ControlFlow::Continue(())
}

fn get_by_value<const PUSH_KEY: bool>(
    (dst, key, receiver, object): (
        RegisterOperand,
        RegisterOperand,
        RegisterOperand,
        RegisterOperand,
    ),
    context: &mut Context,
) -> ControlFlow<CompletionRecord> {
    let key_value = context.vm.get_register(key.into()).clone();
    let base = context.vm.get_register(object.into()).clone();
    let object = match base.base_class(context) {
        Ok(object) => object,
        Err(error) => return context.handle_error(error),
    };
    let key_value = match key_value.to_property_key(context) {
        Ok(key_value) => key_value,
        Err(error) => return context.handle_error(error),
    };

    // Fast Path
    //
    // NOTE: Since we’re using the prototype returned directly by `base_class()`,
    //       we need to handle string primitives separately due to the
    //       string exotic internal methods.
    match &key_value {
        PropertyKey::Index(index) => {
            if object.is_array() {
                let object_borrowed = object.borrow();
                if let Some(element) = object_borrowed.properties().get_dense_property(index.get())
                {
                    if PUSH_KEY {
                        context.vm.set_register(key.into(), key_value.into());
                    }

                    context.vm.set_register(dst.into(), element);
                    return ControlFlow::Continue(());
                }
            } else if let Some(string) = base.as_string() {
                let value = string
                    .code_unit_at(index.get() as usize)
                    .map_or_else(JsValue::undefined, |char| {
                        js_string!([char].as_slice()).into()
                    });

                if PUSH_KEY {
                    context.vm.set_register(key.into(), key_value.into());
                }
                context.vm.set_register(dst.into(), value);
                return ControlFlow::Continue(());
            }
        }
        PropertyKey::String(string) if *string == StaticJsStrings::LENGTH => {
            if let Some(string) = base.as_string() {
                let value = string.len().into();

                if PUSH_KEY {
                    context.vm.set_register(key.into(), key_value.into());
                }
                context.vm.set_register(dst.into(), value);
                return ControlFlow::Continue(());
            }
        }
        _ => {}
    }

    let receiver = context.vm.get_register(receiver.into());

    // Slow path:
    let result = match object.__get_interruptible__(
        &key_value,
        receiver.clone(),
        &mut InternalMethodPropertyContext::new(context),
    ) {
        Ok(ExecutionOutcome::Complete(value)) => value,
        Ok(ExecutionOutcome::Suspend(continuation)) => {
            context.install_interruptible_resume(wrap_get_resume(
                continuation,
                usize::from(dst),
                PUSH_KEY.then_some((usize::from(key), key_value.into())),
            ));
            return ControlFlow::Break(CompletionRecord::Suspend);
        }
        Err(error) => return context.handle_error(error),
    };

    if PUSH_KEY {
        context.vm.set_register(key.into(), key_value.into());
    }
    context.vm.set_register(dst.into(), result);
    ControlFlow::Continue(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GetLengthProperty;

impl GetLengthProperty {
    #[inline(always)]
    pub(crate) fn operation(
        (dst, object, index): (RegisterOperand, RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let object = context.vm.get_register(object.into()).clone();
        get_by_name::<true>((dst, &object, &object, index), context)
    }
}

impl Operation for GetLengthProperty {
    const NAME: &'static str = "GetLengthProperty";
    const INSTRUCTION: &'static str = "INST - GetLengthProperty";
    const COST: u8 = 4;
}

/// `GetPropertyByName` implements the Opcode Operation for `Opcode::GetPropertyByName`
///
/// Operation:
///  - Get a property by name from an object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetPropertyByName;

impl GetPropertyByName {
    #[inline(always)]
    pub(crate) fn operation(
        (dst, object, index): (RegisterOperand, RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let object = context.vm.get_register(object.into()).clone();
        get_by_name::<false>((dst, &object, &object, index), context)
    }
}

impl Operation for GetPropertyByName {
    const NAME: &'static str = "GetPropertyByName";
    const INSTRUCTION: &'static str = "INST - GetPropertyByName";
    const COST: u8 = 4;
}

/// `GetPropertyByNameWithThis` implements the Opcode Operation for `Opcode::GetPropertyByNameWithThis`
///
/// Operation:
///  - Get a property by name from an object with this.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetPropertyByNameWithThis;

impl GetPropertyByNameWithThis {
    #[inline(always)]
    pub(crate) fn operation(
        (dst, receiver, value, index): (
            RegisterOperand,
            RegisterOperand,
            RegisterOperand,
            IndexOperand,
        ),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let receiver = context.vm.get_register(receiver.into()).clone();
        let object = context.vm.get_register(value.into()).clone();
        get_by_name::<false>((dst, &object, &receiver, index), context)
    }
}

impl Operation for GetPropertyByNameWithThis {
    const NAME: &'static str = "GetPropertyByNameWithThis";
    const INSTRUCTION: &'static str = "INST - GetPropertyByNameWithThis";
    const COST: u8 = 4;
}

/// `GetPropertyByValue` implements the Opcode Operation for `Opcode::GetPropertyByValue`
///
/// Operation:
///  - Get a property by value from an object and store it in dst.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetPropertyByValue;

impl GetPropertyByValue {
    #[inline(always)]
    pub(crate) fn operation(
        args: (
            RegisterOperand,
            RegisterOperand,
            RegisterOperand,
            RegisterOperand,
        ),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        get_by_value::<false>(args, context)
    }
}

impl Operation for GetPropertyByValue {
    const NAME: &'static str = "GetPropertyByValue";
    const INSTRUCTION: &'static str = "INST - GetPropertyByValue";
    const COST: u8 = 4;
}

/// `GetPropertyByValuePush` implements the Opcode Operation for `Opcode::GetPropertyByValuePush`
///
/// Operation:
///  - Get a property by value from an object and store the key and value in registers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GetPropertyByValuePush;

impl GetPropertyByValuePush {
    #[inline(always)]
    pub(crate) fn operation(
        args: (
            RegisterOperand,
            RegisterOperand,
            RegisterOperand,
            RegisterOperand,
        ),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        get_by_value::<true>(args, context)
    }
}

impl Operation for GetPropertyByValuePush {
    const NAME: &'static str = "GetPropertyByValuePush";
    const INSTRUCTION: &'static str = "INST - GetPropertyByValuePush";
    const COST: u8 = 4;
}
