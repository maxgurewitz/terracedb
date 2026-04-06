use crate::JsExpect;
use crate::JsValue;
use crate::context::ExecutionOutcome;
use crate::native_function::{NativeFunctionResult, NativeResume};
use crate::value::JsVariant;
use crate::vm::opcode::{IndexOperand, RegisterOperand};
use crate::{
    Context, JsNativeError, JsResult,
    builtins::function::set_function_name,
    object::{
        InterruptibleCallOutcome,
        internal_methods::InternalMethodPropertyContext,
        shape::slot::SlotAttributes,
    },
    property::{PropertyDescriptor, PropertyKey},
    vm::{CompletionRecord, opcode::Operation},
};
use boa_macros::js_str;
use std::ops::ControlFlow;

fn wrap_set_setter_resume(continuation: NativeResume) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_set_setter, continuation)
}

fn resume_set_setter(
    completion: CompletionRecord,
    continuation: &NativeResume,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match continuation.call(completion, context)? {
        NativeFunctionResult::Value(_) => context.continue_interruptible_vm(),
        NativeFunctionResult::Suspend(next) => {
            Ok(NativeFunctionResult::Suspend(wrap_set_setter_resume(next)))
        }
    }
}

fn wrap_set_result_resume(
    continuation: NativeResume,
    strict: bool,
    message: Option<String>,
) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(
        resume_set_result,
        (continuation, strict, message),
    )
}

fn resume_set_result(
    completion: CompletionRecord,
    captures: &(NativeResume, bool, Option<String>),
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match captures.0.call(completion, context)? {
        NativeFunctionResult::Value(value) => {
            let succeeded = value.to_boolean();
            if !succeeded && captures.1 {
                return Err(JsNativeError::typ()
                    .with_message(
                        captures
                            .2
                            .clone()
                            .unwrap_or_else(|| "cannot set non-writable property".to_owned()),
                    )
                    .into());
            }
            context.continue_interruptible_vm()
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_set_result_resume(next, captures.1, captures.2.clone()),
        )),
    }
}

fn set_by_name(
    value: RegisterOperand,
    value_object: &JsValue,
    receiver: &JsValue,
    index: IndexOperand,
    context: &mut Context,
) -> ControlFlow<CompletionRecord> {
    let value = context.vm.get_register(value.into()).clone();

    let object = match value_object.to_object(context) {
        Ok(object) => object,
        Err(error) => return context.handle_error(error),
    };

    let ic = &context.vm.frame().code_block().ic[usize::from(index)];

    let object_borrowed = object.borrow();
    if let Some((shape, slot)) = ic.get(object_borrowed.shape()) {
        let slot_index = slot.index as usize;

        if slot.attributes.is_accessor_descriptor() {
            let result = if slot.attributes.contains(SlotAttributes::PROTOTYPE) {
                let prototype = shape.prototype().expect("prototype should have value");
                let prototype = prototype.borrow();

                prototype.properties().storage[slot_index + 1].clone()
            } else {
                object_borrowed.properties().storage[slot_index + 1].clone()
            };

            drop(object_borrowed);
            if slot.attributes.has_set() && result.is_object() {
                match result
                    .as_object()
                    .expect("should contain getter")
                    .call_interruptible(receiver, std::slice::from_ref(&value), context)
                {
                    Ok(InterruptibleCallOutcome::Complete(_)) => {}
                    Ok(InterruptibleCallOutcome::Suspend(continuation)) => {
                        context.install_interruptible_resume(wrap_set_setter_resume(
                            continuation,
                        ));
                        return ControlFlow::Break(CompletionRecord::Suspend);
                    }
                    Err(error) => return context.handle_error(error),
                }
            }
        } else if slot.attributes.contains(SlotAttributes::PROTOTYPE) {
            let prototype = shape.prototype().expect("prototype should have value");
            let mut prototype = prototype.borrow_mut();

            prototype.properties_mut().storage[slot_index] = value.clone();
        } else {
            drop(object_borrowed);
            let mut object_borrowed = object.borrow_mut();
            object_borrowed.properties_mut().storage[slot_index] = value.clone();
        }
        return ControlFlow::Continue(());
    }
    drop(object_borrowed);

    let name: PropertyKey = ic.name.clone().into();

    let context = &mut InternalMethodPropertyContext::new(context);
    let succeeded = match object.__set_interruptible__(name.clone(), value.clone(), receiver.clone(), context) {
        Ok(ExecutionOutcome::Complete(succeeded)) => succeeded,
        Ok(ExecutionOutcome::Suspend(continuation)) => {
            let strict = context.vm.frame().code_block.strict();
            context.install_interruptible_resume(wrap_set_result_resume(
                continuation,
                strict,
                Some(format!("cannot set non-writable property: {name}")),
            ));
            return ControlFlow::Break(CompletionRecord::Suspend);
        }
        Err(error) => return context.handle_error(error),
    };
    if !succeeded && context.vm.frame().code_block.strict() {
        return context.handle_error(
            JsNativeError::typ()
                .with_message(format!("cannot set non-writable property: {name}"))
                .into(),
        );
    }

    // Cache the property.
    let slot = *context.slot();
    if succeeded && slot.is_cacheable() {
        let ic = &context.vm.frame().code_block.ic[usize::from(index)];
        let object_borrowed = object.borrow();
        let shape = object_borrowed.shape();
        ic.set(shape, slot);
    }

    ControlFlow::Continue(())
}

/// `SetPropertyByName` implements the Opcode Operation for `Opcode::SetPropertyByName`
///
/// Operation:
///  - Sets a property by name of an object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetPropertyByName;

impl SetPropertyByName {
    #[inline(always)]
    pub(crate) fn operation(
        (value, object, index): (RegisterOperand, RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let object = context.vm.get_register(object.into()).clone();
        set_by_name(value, &object, &object, index, context)
    }
}

impl Operation for SetPropertyByNameWithThis {
    const NAME: &'static str = "SetPropertyByNameWithThis";
    const INSTRUCTION: &'static str = "INST - SetPropertyByNameWithThis";
    const COST: u8 = 4;
}

/// `SetPropertyByNameWithThis` implements the Opcode Operation for `Opcode::SetPropertyByNameWithThis`
///
/// Operation:
///  - Sets a property by name of an object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetPropertyByNameWithThis;

impl SetPropertyByNameWithThis {
    #[inline(always)]
    pub(crate) fn operation(
        (value, receiver, object, index): (
            RegisterOperand,
            RegisterOperand,
            RegisterOperand,
            IndexOperand,
        ),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let value_object = context.vm.get_register(object.into()).clone();
        let receiver = context.vm.get_register(receiver.into()).clone();
        set_by_name(value, &value_object, &receiver, index, context)
    }
}

impl Operation for SetPropertyByName {
    const NAME: &'static str = "SetPropertyByName";
    const INSTRUCTION: &'static str = "INST - SetPropertyByName";
    const COST: u8 = 4;
}

/// `SetPropertyByValue` implements the Opcode Operation for `Opcode::SetPropertyByValue`
///
/// Operation:
///  - Sets a property by value of an object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetPropertyByValue;

impl SetPropertyByValue {
    #[inline(always)]
    pub(crate) fn operation(
        (value, key, receiver, object): (
            RegisterOperand,
            RegisterOperand,
            RegisterOperand,
            RegisterOperand,
        ),
        context: &mut Context,
    ) -> ControlFlow<CompletionRecord> {
        let value = context.vm.get_register(value.into()).clone();
        let key = context.vm.get_register(key.into()).clone();
        let receiver = context.vm.get_register(receiver.into()).clone();
        let object = context.vm.get_register(object.into()).clone();
        let object = match object.to_object(context) {
            Ok(object) => object,
            Err(error) => return context.handle_error(error),
        };

        let key = match key.to_property_key(context) {
            Ok(key) => key,
            Err(error) => return context.handle_error(error),
        };

        // Fast Path:
        'fast_path: {
            if object.is_array()
                && let PropertyKey::Index(index) = &key
            {
                let mut object_borrowed = object.borrow_mut();

                // Cannot modify if not extensible.
                if !object_borrowed.extensible {
                    break 'fast_path;
                }

                if object_borrowed
                    .properties_mut()
                    .set_dense_property(index.get(), &value)
                {
                    return ControlFlow::Continue(());
                }
            }
        }

        // Slow path:
        let succeeded = match object.__set_interruptible__(
            key.clone(),
            value.clone(),
            receiver.clone(),
            &mut context.into(),
        ) {
            Ok(ExecutionOutcome::Complete(succeeded)) => succeeded,
            Ok(ExecutionOutcome::Suspend(continuation)) => {
                let strict = context.vm.frame().code_block.strict();
                context.install_interruptible_resume(wrap_set_result_resume(
                    continuation,
                    strict,
                    Some(format!("cannot set non-writable property: {key}")),
                ));
                return ControlFlow::Break(CompletionRecord::Suspend);
            }
            Err(error) => return context.handle_error(error),
        };
        if !succeeded && context.vm.frame().code_block.strict() {
            return context.handle_error(
                JsNativeError::typ()
                    .with_message(format!("cannot set non-writable property: {key}"))
                    .into(),
            );
        }

        ControlFlow::Continue(())
    }
}

impl Operation for SetPropertyByValue {
    const NAME: &'static str = "SetPropertyByValue";
    const INSTRUCTION: &'static str = "INST - SetPropertyByValue";
    const COST: u8 = 4;
}

/// `SetPropertyGetterByName` implements the Opcode Operation for `Opcode::SetPropertyGetterByName`
///
/// Operation:
///  - Sets a getter property by name of an object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetPropertyGetterByName;

impl SetPropertyGetterByName {
    #[inline(always)]
    pub(crate) fn operation(
        (object, value, index): (RegisterOperand, RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let object = context.vm.get_register(object.into()).clone();
        let value = context.vm.get_register(value.into()).clone();
        let name = context
            .vm
            .frame()
            .code_block()
            .constant_string(index.into())
            .into();

        let object = object.to_object(context)?;
        let set = object
            .__get_own_property__(&name, &mut InternalMethodPropertyContext::new(context))?
            .as_ref()
            .and_then(PropertyDescriptor::set)
            .cloned();
        object.__define_own_property__(
            &name,
            PropertyDescriptor::builder()
                .maybe_get(Some(value.clone()))
                .maybe_set(set)
                .enumerable(true)
                .configurable(true)
                .build(),
            &mut InternalMethodPropertyContext::new(context),
        )?;
        Ok(())
    }
}

impl Operation for SetPropertyGetterByName {
    const NAME: &'static str = "SetPropertyGetterByName";
    const INSTRUCTION: &'static str = "INST - SetPropertyGetterByName";
    const COST: u8 = 4;
}

/// `SetPropertyGetterByValue` implements the Opcode Operation for `Opcode::SetPropertyGetterByValue`
///
/// Operation:
///  - Sets a getter property by value of an object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetPropertyGetterByValue;

impl SetPropertyGetterByValue {
    #[inline(always)]
    pub(crate) fn operation(
        (value, key, object): (RegisterOperand, RegisterOperand, RegisterOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let value = context.vm.get_register(value.into()).clone();
        let key = context.vm.get_register(key.into()).clone();
        let object = context.vm.get_register(object.into()).clone();
        let object = object.to_object(context)?;
        let name = key.to_property_key(context)?;

        let set = object
            .__get_own_property__(&name, &mut InternalMethodPropertyContext::new(context))?
            .as_ref()
            .and_then(PropertyDescriptor::set)
            .cloned();
        object.__define_own_property__(
            &name,
            PropertyDescriptor::builder()
                .maybe_get(Some(value.clone()))
                .maybe_set(set)
                .enumerable(true)
                .configurable(true)
                .build(),
            &mut InternalMethodPropertyContext::new(context),
        )?;
        Ok(())
    }
}

impl Operation for SetPropertyGetterByValue {
    const NAME: &'static str = "SetPropertyGetterByValue";
    const INSTRUCTION: &'static str = "INST - SetPropertyGetterByValue";
    const COST: u8 = 4;
}

/// `SetPropertySetterByName` implements the Opcode Operation for `Opcode::SetPropertySetterByName`
///
/// Operation:
///  - Sets a setter property by name of an object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetPropertySetterByName;

impl SetPropertySetterByName {
    #[inline(always)]
    pub(crate) fn operation(
        (object, value, index): (RegisterOperand, RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let object = context.vm.get_register(object.into()).clone();
        let value = context.vm.get_register(value.into()).clone();
        let name = context
            .vm
            .frame()
            .code_block()
            .constant_string(index.into())
            .into();

        let object = object.to_object(context)?;

        let get = object
            .__get_own_property__(&name, &mut InternalMethodPropertyContext::new(context))?
            .as_ref()
            .and_then(PropertyDescriptor::get)
            .cloned();
        object.__define_own_property__(
            &name,
            PropertyDescriptor::builder()
                .maybe_set(Some(value.clone()))
                .maybe_get(get)
                .enumerable(true)
                .configurable(true)
                .build(),
            &mut InternalMethodPropertyContext::new(context),
        )?;
        Ok(())
    }
}

impl Operation for SetPropertySetterByName {
    const NAME: &'static str = "SetPropertySetterByName";
    const INSTRUCTION: &'static str = "INST - SetPropertySetterByName";
    const COST: u8 = 4;
}

/// `SetPropertySetterByValue` implements the Opcode Operation for `Opcode::SetPropertySetterByValue`
///
/// Operation:
///  - Sets a setter property by value of an object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetPropertySetterByValue;

impl SetPropertySetterByValue {
    #[inline(always)]
    pub(crate) fn operation(
        (value, key, object): (RegisterOperand, RegisterOperand, RegisterOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let value = context.vm.get_register(value.into()).clone();
        let key = context.vm.get_register(key.into()).clone();
        let object = context.vm.get_register(object.into()).clone();

        let object = object.to_object(context)?;
        let name = key.to_property_key(context)?;

        let get = object
            .__get_own_property__(&name, &mut InternalMethodPropertyContext::new(context))?
            .as_ref()
            .and_then(PropertyDescriptor::get)
            .cloned();
        object.__define_own_property__(
            &name,
            PropertyDescriptor::builder()
                .maybe_set(Some(value.clone()))
                .maybe_get(get)
                .enumerable(true)
                .configurable(true)
                .build(),
            &mut InternalMethodPropertyContext::new(context),
        )?;
        Ok(())
    }
}

impl Operation for SetPropertySetterByValue {
    const NAME: &'static str = "SetPropertySetterByValue";
    const INSTRUCTION: &'static str = "INST - SetPropertySetterByValue";
    const COST: u8 = 4;
}

/// `SetFunctionName` implements the Opcode Operation for `Opcode::SetFunctionName`
///
/// Operation:
///  - Sets the name of a function object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetFunctionName;

impl SetFunctionName {
    #[inline(always)]
    pub(crate) fn operation(
        (function, name, prefix): (RegisterOperand, RegisterOperand, IndexOperand),
        context: &mut Context,
    ) -> JsResult<()> {
        let function = context.vm.get_register(function.into()).clone();
        let name = context.vm.get_register(name.into()).clone();
        let name = match name.variant() {
            JsVariant::String(name) => PropertyKey::from(name.clone()),
            JsVariant::Symbol(name) => PropertyKey::from(name.clone()),
            _ => unreachable!(),
        };

        let prefix = match u32::from(prefix) {
            1 => Some(js_str!("get")),
            2 => Some(js_str!("set")),
            _ => None,
        };

        set_function_name(
            &function
                .as_object()
                .js_expect("function is not an object")?,
            &name,
            prefix,
            context,
        )?;
        Ok(())
    }
}

impl Operation for SetFunctionName {
    const NAME: &'static str = "SetFunctionName";
    const INSTRUCTION: &'static str = "INST - SetFunctionName";
    const COST: u8 = 4;
}
