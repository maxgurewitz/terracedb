use crate::{
    Context, JsArgs, JsData, JsError, JsExpect, JsNativeError, JsResult, JsValue,
    builtins::{
        BuiltInBuilder, IntrinsicObject, Promise,
        iterable::{IteratorRecord, IteratorResult, create_iter_result_object},
        promise::{PromiseCapability, if_abrupt_reject_promise},
    },
    context::intrinsics::Intrinsics,
    js_string,
    native_function::{NativeFunction, NativeFunctionResult, NativeResume},
    object::{FunctionObjectBuilder, InterruptibleCallOutcome, JsObject},
    realm::Realm,
};
use boa_gc::{Finalize, Trace};

/// `%AsyncFromSyncIteratorPrototype%` object.
///
/// More information:
///  - [ECMA reference][spec]
///
/// [spec]: https://tc39.es/ecma262/#sec-properties-of-async-from-sync-iterator-instances
#[derive(Clone, Debug, Finalize, Trace, JsData)]
pub(crate) struct AsyncFromSyncIterator {
    // The [[SyncIteratorRecord]] internal slot.
    sync_iterator_record: IteratorRecord,
}

impl IntrinsicObject for AsyncFromSyncIterator {
    fn init(realm: &Realm) -> crate::JsResult<()> {
        BuiltInBuilder::with_intrinsic::<Self>(realm)
            .prototype(
                realm
                    .intrinsics()
                    .objects()
                    .iterator_prototypes()
                    .async_iterator(),
            )
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::next),
                js_string!("next"),
                1,
            )
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::r#return),
                js_string!("return"),
                1,
            )
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::throw),
                js_string!("throw"),
                1,
            )
            .build()?;

        Ok(())
    }

    fn get(intrinsics: &Intrinsics) -> JsObject {
        intrinsics
            .objects()
            .iterator_prototypes()
            .async_from_sync_iterator()
    }
}

impl AsyncFromSyncIterator {
    /// `CreateAsyncFromSyncIterator ( syncIteratorRecord )`
    ///
    /// More information:
    ///  - [ECMA reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-createasyncfromsynciterator
    pub(crate) fn create(
        sync_iterator_record: IteratorRecord,
        context: &mut Context,
    ) -> IteratorRecord {
        // 1. Let asyncIterator be OrdinaryObjectCreate(%AsyncFromSyncIteratorPrototype%, « [[SyncIteratorRecord]] »).
        // 2. Set asyncIterator.[[SyncIteratorRecord]] to syncIteratorRecord.
        let async_iterator = JsObject::from_proto_and_data_with_shared_shape(
            context.root_shape(),
            context
                .intrinsics()
                .objects()
                .iterator_prototypes()
                .async_from_sync_iterator(),
            Self {
                sync_iterator_record,
            },
        )
        .upcast();

        // 3. Let nextMethod be ! Get(asyncIterator, "next").
        let next_method = async_iterator
            .get(js_string!("next"), context)
            .expect("async from sync iterator prototype must have next method");

        // 4. Let iteratorRecord be the Iterator Record { [[Iterator]]: asyncIterator, [[NextMethod]]: nextMethod, [[Done]]: false }.
        // 5. Return iteratorRecord.
        IteratorRecord::new(async_iterator, next_method)
    }

    /// `%AsyncFromSyncIteratorPrototype%.next ( [ value ] )`
    ///
    /// More information:
    ///  - [ECMA reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-%asyncfromsynciteratorprototype%.next
    fn next(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be the this value.
        // 2. Assert: O is an Object that has a [[SyncIteratorRecord]] internal slot.
        // 4. Let syncIteratorRecord be O.[[SyncIteratorRecord]].
        let object = this.as_object();
        let sync_iterator_record = object
            .as_ref()
            .and_then(JsObject::downcast_ref::<Self>)
            .js_expect("async from sync iterator prototype must be object")?
            .sync_iterator_record
            .clone();

        // 3. Let promiseCapability be ! NewPromiseCapability(%Promise%).
        let promise_capability = PromiseCapability::new(
            &context.intrinsics().constructors().promise().constructor(),
            context,
        )
        .js_expect("cannot fail with promise constructor")?;

        // 5. If value is present, then
        //     a. Let result be Completion(IteratorNext(syncIteratorRecord, value)).
        // 6. Else,
        //     a. Let result be Completion(IteratorNext(syncIteratorRecord)).
        let iterator = sync_iterator_record.iterator().clone();
        let next_method = sync_iterator_record
            .next_method()
            .as_object()
            .js_expect("async from sync iterator next method must be callable object")?;
        let result: JsResult<JsValue> = match next_method.call_interruptible(
            &iterator.clone().into(),
            args.first().map_or(&[], std::slice::from_ref),
            context,
        )? {
            InterruptibleCallOutcome::Complete(result) => Ok(result),
            InterruptibleCallOutcome::Suspend(continuation) => {
                return Ok(NativeFunctionResult::Suspend(wrap_async_from_sync_suspend(
                    continuation,
                    AsyncFromSyncSuspendState {
                        sync_iterator_record,
                        promise_capability,
                        close_on_rejection: true,
                    },
                )));
            }
        };

        Self::finish(
            IteratorResult::from_value(result?),
            promise_capability,
            sync_iterator_record,
            true,
            context,
        )
    }

    /// `%AsyncFromSyncIteratorPrototype%.return ( [ value ] )`
    ///
    /// More information:
    ///  - [ECMA reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-%asyncfromsynciteratorprototype%.return
    fn r#return(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be the this value.
        // 2. Assert: O is an Object that has a [[SyncIteratorRecord]] internal slot.
        // 4. Let syncIteratorRecord be O.[[SyncIteratorRecord]].
        let object = this.as_object();
        let sync_iterator_record = object
            .as_ref()
            .and_then(JsObject::downcast_ref::<Self>)
            .js_expect("async from sync iterator prototype must be object")?
            .sync_iterator_record
            .clone();
        // 5. Let syncIterator be syncIteratorRecord.[[Iterator]].
        let sync_iterator = sync_iterator_record.iterator().clone();

        // 3. Let promiseCapability be ! NewPromiseCapability(%Promise%).
        let promise_capability = PromiseCapability::new(
            &context.intrinsics().constructors().promise().constructor(),
            context,
        )
        .js_expect("cannot fail with promise constructor")?;

        // 6. Let return be Completion(GetMethod(syncIterator, "return")).
        let r#return = match sync_iterator.get_method(js_string!("return"), context) {
            Ok(return_method) => return_method,
            Err(err) => {
                return Self::reject_promise(err, promise_capability, context);
            }
        };

        let result = match (r#return, args.first()) {
            // 8. If return is undefined, then
            (None, _) => {
                // a. Let iterResult be CreateIterResultObject(value, true).
                let iter_result =
                    create_iter_result_object(args.get_or_undefined(0).clone(), true, context)?;

                // b. Perform ! Call(promiseCapability.[[Resolve]], undefined, « iterResult »).
                promise_capability
                    .resolve()
                    .call(&JsValue::undefined(), &[iter_result], context)
                    .js_expect("cannot fail according to spec")?;

                // c. Return promiseCapability.[[Promise]].
                return Ok(NativeFunctionResult::complete(
                    promise_capability.promise().clone(),
                ));
            }
            // 9. If value is present, then
            (Some(r#return), Some(value)) => {
                // a. Let result be Completion(Call(return, syncIterator, « value »)).
                match r#return.call_interruptible(
                    &sync_iterator.clone().into(),
                    std::slice::from_ref(value),
                    context,
                )? {
                    InterruptibleCallOutcome::Complete(result) => result,
                    InterruptibleCallOutcome::Suspend(continuation) => {
                        return Ok(NativeFunctionResult::Suspend(wrap_async_from_sync_suspend(
                            continuation,
                            AsyncFromSyncSuspendState {
                                sync_iterator_record,
                                promise_capability,
                                close_on_rejection: false,
                            },
                        )));
                    }
                }
            }
            // 10. Else,
            (Some(r#return), None) => {
                // a. Let result be Completion(Call(return, syncIterator)).
                match r#return.call_interruptible(&sync_iterator.clone().into(), &[], context)? {
                    InterruptibleCallOutcome::Complete(result) => result,
                    InterruptibleCallOutcome::Suspend(continuation) => {
                        return Ok(NativeFunctionResult::Suspend(wrap_async_from_sync_suspend(
                            continuation,
                            AsyncFromSyncSuspendState {
                                sync_iterator_record,
                                promise_capability,
                                close_on_rejection: false,
                            },
                        )));
                    }
                }
            }
        };

            // 12. If Type(result) is not Object, then
        //     a. Perform ! Call(promiseCapability.[[Reject]], undefined, « a newly created TypeError object »).
        Self::finish(
            IteratorResult::from_value(result),
            promise_capability,
            sync_iterator_record,
            false,
            context,
        )
    }

    /// `%AsyncFromSyncIteratorPrototype%.throw ( [ value ] )`
    ///
    /// More information:
    ///  - [ECMA reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-%asyncfromsynciteratorprototype%.throw
    fn throw(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be the this value.
        // 2. Assert: O is an Object that has a [[SyncIteratorRecord]] internal slot.
        // 4. Let syncIteratorRecord be O.[[SyncIteratorRecord]].
        let object = this.as_object();
        let sync_iterator_record = object
            .as_ref()
            .and_then(JsObject::downcast_ref::<Self>)
            .js_expect("async from sync iterator prototype must be object")?
            .sync_iterator_record
            .clone();
        // 5. Let syncIterator be syncIteratorRecord.[[Iterator]].
        let sync_iterator = sync_iterator_record.iterator().clone();

        // 3. Let promiseCapability be ! NewPromiseCapability(%Promise%).
        let promise_capability = PromiseCapability::new(
            &context.intrinsics().constructors().promise().constructor(),
            context,
        )
        .js_expect("cannot fail with promise constructor")?;

        // 6. Let throw be Completion(GetMethod(syncIterator, "throw")).
        let throw = match sync_iterator.get_method(js_string!("throw"), context) {
            Ok(throw_method) => throw_method,
            Err(err) => {
                return Self::reject_promise(err, promise_capability, context);
            }
        };

        let result = match (throw, args.first()) {
            // 8. If throw is undefined, then
            (None, _) => {
                // a. NOTE: If syncIterator does not have a throw method, close it to give it a chance to clean up before we reject the capability.
                // b. Let closeCompletion be NormalCompletion(empty).
                // c. Let result be Completion(IteratorClose(syncIteratorRecord, closeCompletion)).
                let result = sync_iterator_record.close(Ok(JsValue::undefined()), context);
                // d. IfAbruptRejectPromise(result, promiseCapability).
                if let Err(err) = result {
                    return Self::reject_promise(err, promise_capability, context);
                }

                // e. NOTE: The next step throws a TypeError to indicate that there was a protocol violation: syncIterator does not have a throw method.
                // f. NOTE: If closing syncIterator does not throw then the result of that operation is ignored, even if it yields a rejected promise.
                // g. Perform ! Call(promiseCapability.[[Reject]], undefined, « a newly created TypeError object »).
                promise_capability
                    .reject()
                    .call(
                        &JsValue::undefined(),
                        &[JsNativeError::typ()
                            .with_message("sync iterator does not have a throw method")
                            .into_opaque(context)?
                            .into()],
                        context,
                    )
                    .js_expect("cannot fail according to spec")?;

                // h. Return promiseCapability.[[Promise]].
                return Ok(NativeFunctionResult::complete(
                    promise_capability.promise().clone(),
                ));
            }
            // 9. If value is present, then
            (Some(throw), Some(value)) => {
                // a. Let result be Completion(Call(throw, syncIterator, « value »)).
                match throw.call_interruptible(
                    &sync_iterator.clone().into(),
                    std::slice::from_ref(value),
                    context,
                )? {
                    InterruptibleCallOutcome::Complete(result) => result,
                    InterruptibleCallOutcome::Suspend(continuation) => {
                        return Ok(NativeFunctionResult::Suspend(wrap_async_from_sync_suspend(
                            continuation,
                            AsyncFromSyncSuspendState {
                                sync_iterator_record,
                                promise_capability,
                                close_on_rejection: true,
                            },
                        )));
                    }
                }
            }
            // 10. Else,
            (Some(throw), None) => {
                // a. Let result be Completion(Call(throw, syncIterator)).
                match throw.call_interruptible(&sync_iterator.clone().into(), &[], context)? {
                    InterruptibleCallOutcome::Complete(result) => result,
                    InterruptibleCallOutcome::Suspend(continuation) => {
                        return Ok(NativeFunctionResult::Suspend(wrap_async_from_sync_suspend(
                            continuation,
                            AsyncFromSyncSuspendState {
                                sync_iterator_record,
                                promise_capability,
                                close_on_rejection: true,
                            },
                        )));
                    }
                }
            }
        };

            // 12. If Type(result) is not Object, then
        // a. Perform ! Call(promiseCapability.[[Reject]], undefined, « a newly created TypeError object »).
        Self::finish(
            IteratorResult::from_value(result),
            promise_capability,
            sync_iterator_record,
            true,
            context,
        )
    }

    /// `AsyncFromSyncIteratorContinuation ( result, promiseCapability )`
    ///
    /// More information:
    ///  - [ECMA reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-asyncfromsynciteratorcontinuation
    fn continuation(
        result: &IteratorResult,
        promise_capability: &PromiseCapability,
        sync_iterator_record: IteratorRecord,
        close_on_rejection: bool,
        context: &mut Context,
    ) -> JsResult<JsValue> {
        // 1. NOTE: Because promiseCapability is derived from the intrinsic %Promise%,
        // the calls to promiseCapability.[[Reject]] entailed by the
        // use IfAbruptRejectPromise below are guaranteed not to throw.

        // 2. Let done be Completion(IteratorComplete(result)).
        let done = result.complete(context);

        // 3. IfAbruptRejectPromise(done, promiseCapability).
        let done = if_abrupt_reject_promise!(done, promise_capability, context);

        // 4. Let value be Completion(IteratorValue(result)).
        let value = result.value(context);

        // 5. IfAbruptRejectPromise(value, promiseCapability).
        let value = if_abrupt_reject_promise!(value, promise_capability, context);

        // 6. Let valueWrapper be Completion(PromiseResolve(%Promise%, value)).
        let value_wrapper = match Promise::promise_resolve(
            &context.intrinsics().constructors().promise().constructor(),
            value,
            context,
        ) {
            // 7. If valueWrapper is an abrupt completion, done is false, and closeOnRejection is
            //    true, then
            Err(e) if !done && close_on_rejection => {
                // a. Set valueWrapper to Completion(IteratorClose(syncIteratorRecord, valueWrapper)).
                Err(sync_iterator_record.close(Err(e), context).expect_err(
                    "closing an iterator with an error must always return an error back",
                ))
            }
            other => other.map(|o| {
                o.downcast::<Promise>()
                    .expect("%Promise% constructor must return a `Promise` object")
            }),
        };

        // 8. IfAbruptRejectPromise(valueWrapper, promiseCapability).
        let value_wrapper = if_abrupt_reject_promise!(value_wrapper, promise_capability, context);

        // 9. Let unwrap be a new Abstract Closure with parameters (value)
        // that captures done and performs the following steps when called:
        // 10. Let onFulfilled be CreateBuiltinFunction(unwrap, 1, "", « »).
        let on_fulfilled = FunctionObjectBuilder::new(
            context.realm(),
            NativeFunction::from_copy_closure(move |_this, args, context| {
                // a. Return CreateIterResultObject(value, done).
                create_iter_result_object(args.get_or_undefined(0).clone(), done, context)
            }),
        )
        .name(js_string!())
        .length(1)
        .build();

        // 11. NOTE: onFulfilled is used when processing the "value" property of an
        // IteratorResult object in order to wait for its value if it is a promise and
        // re-package the result in a new "unwrapped" IteratorResult object.

        // 12. If done is true, or if closeOnRejection is false, then
        let on_rejected = if done || !close_on_rejection {
            // a. Let onRejected be undefined.
            None
        } else {
            // 13. Else,
            //     a. Let closeIterator be a new Abstract Closure with parameters (error) that
            //        captures syncIteratorRecord and performs the following steps when called:
            //     b. Let onRejected be CreateBuiltinFunction(closeIterator, 1, "", « »).
            //     c. NOTE: onRejected is used to close the Iterator when the "value" property of an
            //        IteratorResult object it yields is a rejected promise.
            Some(
                FunctionObjectBuilder::new(
                    context.realm(),
                    NativeFunction::from_copy_closure_with_captures(
                        |_this, args, iter, context| {
                            // i. Return ? IteratorClose(syncIteratorRecord, ThrowCompletion(error)).
                            iter.close(
                                Err(JsError::from_opaque(args.get_or_undefined(0).clone())),
                                context,
                            )
                        },
                        sync_iterator_record,
                    ),
                )
                .name(js_string!())
                .length(1)
                .build(),
            )
        };

        // 14. Perform PerformPromiseThen(valueWrapper, onFulfilled, undefined, promiseCapability).
        Promise::perform_promise_then(
            &value_wrapper,
            Some(on_fulfilled),
            on_rejected,
            Some(promise_capability.clone()),
            context,
        );

        // 15. Return promiseCapability.[[Promise]].
        Ok(promise_capability.promise().clone().into())
    }
}

#[derive(Clone, Debug, Finalize, Trace)]
struct AsyncFromSyncSuspendState {
    sync_iterator_record: IteratorRecord,
    promise_capability: PromiseCapability,
    close_on_rejection: bool,
}

impl AsyncFromSyncIterator {
    fn finish(
        result: JsResult<IteratorResult>,
        promise_capability: PromiseCapability,
        sync_iterator_record: IteratorRecord,
        close_on_rejection: bool,
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        let result = match result {
            Ok(result) => result,
            Err(err) => return Self::reject_promise(err, promise_capability, context),
        };
        Self::continuation(
            &result,
            &promise_capability,
            sync_iterator_record,
            close_on_rejection,
            context,
        )
        .map(NativeFunctionResult::complete)
    }

    fn reject_promise(
        err: JsError,
        promise_capability: PromiseCapability,
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        let err = err.into_opaque(context)?;
        promise_capability
            .reject()
            .call(&JsValue::undefined(), &[err], context)
            .js_expect("promise rejection must not fail")?;

        Ok(NativeFunctionResult::complete(
            promise_capability.promise().clone(),
        ))
    }
}

fn wrap_async_from_sync_suspend(
    continuation: NativeResume,
    state: AsyncFromSyncSuspendState,
) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(
        resume_async_from_sync_suspend,
        (continuation, state),
    )
}

fn resume_async_from_sync_suspend(
    completion: crate::vm::CompletionRecord,
    captures: &(NativeResume, AsyncFromSyncSuspendState),
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match captures.0.call(completion, context)? {
        NativeFunctionResult::Complete(record) => {
            let result = match record {
                crate::vm::CompletionRecord::Normal(value)
                | crate::vm::CompletionRecord::Return(value) => Ok(value),
                crate::vm::CompletionRecord::Throw(err) => Err(err),
                crate::vm::CompletionRecord::Suspend => Err(JsNativeError::error()
                    .with_message(
                        "async from sync iterator resumed with unexpected suspended completion",
                    )
                    .into()),
            };

            AsyncFromSyncIterator::finish(
                IteratorResult::from_value(result?),
                captures.1.promise_capability.clone(),
                captures.1.sync_iterator_record.clone(),
                captures.1.close_on_rejection,
                context,
            )
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_async_from_sync_suspend(next, captures.1.clone()),
        )),
    }
}
