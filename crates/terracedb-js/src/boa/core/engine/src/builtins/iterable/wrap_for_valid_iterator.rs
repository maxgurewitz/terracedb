//! Boa's implementation of the `%WrapForValidIteratorPrototype%` object.
//!
//! This object wraps an iterator record to make it a valid `Iterator` instance
//! by inheriting from `%Iterator.prototype%`.
//!
//! More information:
//!  - [ECMAScript reference][spec]
//!
//! [spec]: https://tc39.es/ecma262/#sec-wrapforvaliditeratorprototype-object

use crate::{
    Context, JsData, JsResult, JsValue,
    builtins::{BuiltInBuilder, IntrinsicObject, iterable::create_iter_result_object},
    context::intrinsics::Intrinsics,
    error::JsNativeError,
    js_string,
    native_function::{NativeFunction, NativeFunctionResult, NativeResume},
    object::{InterruptibleCallOutcome, JsObject},
    realm::Realm,
};
use boa_gc::{Finalize, Trace};

use super::IteratorRecord;

/// The internal representation of a `WrapForValidIterator` object.
///
/// More information:
///  - [ECMAScript reference][spec]
///
/// [spec]: https://tc39.es/ecma262/#sec-wrapforvaliditeratorprototype-object
#[derive(Debug, Finalize, Trace, JsData)]
pub(crate) struct WrapForValidIterator {
    /// `[[Iterated]]` — the iterator record this wrapper delegates to.
    pub(crate) iterated: IteratorRecord,
}

impl IntrinsicObject for WrapForValidIterator {
    fn init(realm: &Realm) -> crate::JsResult<()> {
        BuiltInBuilder::with_intrinsic::<Self>(realm)
            .prototype(realm.intrinsics().constructors().iterator().prototype())
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::next),
                js_string!("next"),
                0,
            )
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::r#return),
                js_string!("return"),
                0,
            )
            .build()?;

        Ok(())
    }

    fn get(intrinsics: &Intrinsics) -> JsObject {
        intrinsics
            .objects()
            .iterator_prototypes()
            .wrap_for_valid_iterator()
    }
}

impl WrapForValidIterator {
    /// `%WrapForValidIteratorPrototype%.next ( )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-%25wrapforvaliditeratorprototype%25.next
    pub(crate) fn next(
        this: &JsValue,
        _args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be this value.
        // 2. Perform ? RequireInternalSlot(O, [[Iterated]]).
        let object = this.as_object().ok_or_else(|| {
            JsNativeError::typ().with_message("WrapForValidIterator method called on non-object")
        })?;

        let wrapper = object.downcast_mut::<Self>().ok_or_else(|| {
            JsNativeError::typ()
                .with_message("WrapForValidIterator method called on incompatible object")
        })?;

        // 3. Let iteratorRecord be O.[[Iterated]].
        // 4. Return ? Call(iteratorRecord.[[NextMethod]], iteratorRecord.[[Iterator]]).
        let next_method = wrapper.iterated.next_method().clone();
        let iterator = wrapper.iterated.iterator().clone();
        drop(wrapper);

        let next_method = next_method
            .as_object()
            .ok_or_else(|| {
                JsNativeError::typ().with_message("iterator next method must be callable")
            })?;

        match next_method.call_interruptible(&iterator.clone().into(), &[], context)? {
            InterruptibleCallOutcome::Complete(value) => Ok(NativeFunctionResult::complete(value)),
            InterruptibleCallOutcome::Suspend(continuation) => Ok(NativeFunctionResult::Suspend(
                wrap_wrap_for_valid_iterator_next_suspend(continuation, iterator),
            )),
        }
    }

    /// `%WrapForValidIteratorPrototype%.return ( )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-%25wrapforvaliditeratorprototype%25.return
    pub(crate) fn r#return(
        this: &JsValue,
        _args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be this value.
        // 2. Perform ? RequireInternalSlot(O, [[Iterated]]).
        let object = this.as_object().ok_or_else(|| {
            JsNativeError::typ().with_message("WrapForValidIterator method called on non-object")
        })?;

        let wrapper = object.downcast_mut::<Self>().ok_or_else(|| {
            JsNativeError::typ()
                .with_message("WrapForValidIterator method called on incompatible object")
        })?;

        // 3. Let iterator be O.[[Iterated]].[[Iterator]].
        let iterator = wrapper.iterated.iterator().clone();
        drop(wrapper);

        // 4. Assert: iterator is an Object.
        // 5. Let returnMethod be ? GetMethod(iterator, "return").
        let return_method = iterator.get_method(js_string!("return"), context)?;

        match return_method {
            // 6. If returnMethod is undefined, then
            None => {
                // a. Return CreateIterResultObject(undefined, true).
                create_iter_result_object(JsValue::undefined(), true, context)
                    .map(NativeFunctionResult::complete)
            }
            // 7. Return ? Call(returnMethod, iterator).
            Some(return_method) => match return_method.call_interruptible(&iterator.clone().into(), &[], context)? {
                InterruptibleCallOutcome::Complete(value) => Ok(NativeFunctionResult::complete(value)),
                InterruptibleCallOutcome::Suspend(continuation) => Ok(NativeFunctionResult::Suspend(
                    wrap_wrap_for_valid_iterator_return_suspend(continuation, iterator),
                )),
            },
        }
    }
}

fn wrap_wrap_for_valid_iterator_next_suspend(
    continuation: NativeResume,
    iterator: JsObject,
) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(
        resume_wrap_for_valid_iterator_next,
        (continuation, iterator),
    )
}

fn resume_wrap_for_valid_iterator_next(
    completion: crate::vm::CompletionRecord,
    captures: &(NativeResume, JsObject),
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match captures.0.call(completion, context)? {
        NativeFunctionResult::Complete(record) => {
            Ok(NativeFunctionResult::from_completion(record))
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_wrap_for_valid_iterator_next_suspend(next, captures.1.clone()),
        )),
    }
}

fn wrap_wrap_for_valid_iterator_return_suspend(
    continuation: NativeResume,
    iterator: JsObject,
) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(
        resume_wrap_for_valid_iterator_return,
        (continuation, iterator),
    )
}

fn resume_wrap_for_valid_iterator_return(
    completion: crate::vm::CompletionRecord,
    captures: &(NativeResume, JsObject),
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match captures.0.call(completion, context)? {
        NativeFunctionResult::Complete(record) => {
            Ok(NativeFunctionResult::from_completion(record))
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_wrap_for_valid_iterator_return_suspend(next, captures.1.clone()),
        )),
    }
}
