use boa_gc::{Finalize, Trace};
use crate::{
    Context, JsArgs, JsObject, JsResult, JsSymbol, JsValue,
    builtins::{
        IntrinsicObject,
        array::Array,
        builder::BuiltInBuilder,
        iterable::{
            IteratorRecord, get_iterator_direct, if_abrupt_close_iterator,
            iterator_helper::{self, IteratorHelper},
        },
    },
    context::intrinsics::Intrinsics,
    js_error, js_string,
    object::{CONSTRUCTOR, JsFunction},
    property::{Attribute, PropertyKey},
    realm::Realm,
    native_function::{NativeFunction, NativeFunctionResult, NativeResume},
    object::InterruptibleCallOutcome,
    value::{IntegerOrInfinity, TryFromJs},
    vm::CompletionRecord,
};
use std::cell::Cell;

/// `%IteratorPrototype%` object
///
/// More information:
///  - [ECMA reference][spec]
///
/// [spec]: https://tc39.es/ecma262/#sec-%iteratorprototype%-object
pub(crate) struct Iterator;

impl IntrinsicObject for Iterator {
    fn init(realm: &Realm) -> crate::JsResult<()> {
        let get_constructor = BuiltInBuilder::callable(realm, Self::get_constructor)
            .name(js_string!("get constructor"))
            .build()?;
        let set_constructor = BuiltInBuilder::callable(realm, Self::set_constructor)
            .name(js_string!("set constructor"))
            .build()?;
        let get_to_string_tag = BuiltInBuilder::callable(realm, Self::get_to_string_tag)
            .name(js_string!("get [Symbol.toStringTag]"))
            .build()?;
        let set_to_string_tag = BuiltInBuilder::callable(realm, Self::set_to_string_tag)
            .name(js_string!("set [Symbol.toStringTag]"))
            .build()?;
        BuiltInBuilder::with_intrinsic::<Self>(realm)
            .static_method(|v, _, _| Ok(v.clone()), JsSymbol::iterator(), 0)
            .static_method(Self::map, js_string!("map"), 1)
            .static_method(Self::filter, js_string!("filter"), 1)
            .static_method(Self::take, js_string!("take"), 1)
            .static_method(Self::drop, js_string!("drop"), 1)
            .static_method(Self::flat_map, js_string!("flatMap"), 1)
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::reduce),
                js_string!("reduce"),
                1,
            )
            .static_method(Self::to_array, js_string!("toArray"), 0)
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::for_each),
                js_string!("forEach"),
                1,
            )
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::some),
                js_string!("some"),
                1,
            )
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::every),
                js_string!("every"),
                1,
            )
            .static_method_native(
                NativeFunction::from_suspend_fn_ptr(Self::find),
                js_string!("find"),
                1,
            )
            .static_accessor(
                JsSymbol::to_string_tag(),
                Some(get_to_string_tag),
                Some(set_to_string_tag),
                Attribute::CONFIGURABLE,
            )
            .static_accessor(
                CONSTRUCTOR,
                Some(get_constructor),
                Some(set_constructor),
                Attribute::CONFIGURABLE,
            )
            .build()?;

        Ok(())
    }

    fn get(intrinsics: &Intrinsics) -> JsObject {
        intrinsics.constructors().iterator().prototype()
    }
}

impl Iterator {
    /// `get Iterator.prototype.constructor`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.constructor
    #[allow(clippy::unnecessary_wraps)]
    fn get_constructor(
        _this: &JsValue,
        _args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<JsValue> {
        Ok(context
            .intrinsics()
            .constructors()
            .iterator()
            .constructor()
            .into())
    }

    /// `set Iterator.prototype.constructor`
    ///
    /// `SetterThatIgnoresPrototypeProperties(this, %Iterator.prototype%, "constructor", v)`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.constructor
    fn set_constructor(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<JsValue> {
        Self::setter_that_ignores_prototype_properties(
            this,
            &context.intrinsics().constructors().iterator().prototype(),
            js_string!("constructor"),
            args.get_or_undefined(0),
            context,
        )
    }

    /// `get Iterator.prototype[@@toStringTag]`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype-%40%40tostringtag
    #[allow(clippy::unnecessary_wraps)]
    fn get_to_string_tag(
        _this: &JsValue,
        _args: &[JsValue],
        _context: &mut Context,
    ) -> JsResult<JsValue> {
        Ok(js_string!("Iterator").into())
    }

    /// `set Iterator.prototype[@@toStringTag]`
    ///
    /// `SetterThatIgnoresPrototypeProperties(this, %Iterator.prototype%, @@toStringTag, v)`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype-%40%40tostringtag
    fn set_to_string_tag(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<JsValue> {
        Self::setter_that_ignores_prototype_properties(
            this,
            &context.intrinsics().constructors().iterator().prototype(),
            JsSymbol::to_string_tag(),
            args.get_or_undefined(0),
            context,
        )
    }

    /// `SetterThatIgnoresPrototypeProperties ( this, home, p, v )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-SetterThatIgnoresPrototypeProperties
    fn setter_that_ignores_prototype_properties<K: Into<PropertyKey>>(
        this: &JsValue,
        home: &JsObject,
        p: K,
        v: &JsValue,
        context: &mut Context,
    ) -> JsResult<JsValue> {
        let p = p.into();

        // 1. If this is not an Object, then
        let Some(this_obj) = this.as_object() else {
            // a. Throw a TypeError exception.
            return Err(js_error!(TypeError: "Cannot set property on a non-object"));
        };

        // 2. If this is home, then
        if JsObject::equals(&this_obj, home) {
            // a. NOTE: Throwing here emulates the behavior of a Set handler ...
            // b. Throw a TypeError exception.
            return Err(js_error!(TypeError: "Cannot set property directly on the prototype"));
        }

        // 3. Let desc be ? this.[[GetOwnProperty]](p).
        let desc = this_obj.__get_own_property__(&p, &mut context.into())?;

        // 4. If desc is undefined, then
        if desc.is_none() {
            // a. Perform ? CreateDataPropertyOrThrow(this, p, v).
            this_obj.create_data_property_or_throw(p, v.clone(), context)?;
        } else {
            // 5. Else,
            //    a. Perform ? Set(this, p, v, true).
            this_obj.set(p, v.clone(), true, context)?;
        }

        // 6. Return undefined.
        Ok(JsValue::undefined())
    }

    // ==================== Prototype Methods — Lazy ====================

    /// `Iterator.prototype.map ( mapper )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.map
    fn map(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this
            .as_object()
            .ok_or_else(|| js_error!(TypeError: "Iterator.prototype.map called on non-object"))?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. If IsCallable(mapper) is false, then
        //    a. Let error be ThrowCompletion(a newly created TypeError object).
        //    b. Return ? IteratorClose(iterated, error).
        let mapper = args.get_or_undefined(0);
        let Ok(mapper) = JsFunction::try_from_js(mapper, context) else {
            return iterated.close(
                Err(js_error!(
                    TypeError: "Iterator.prototype.map: mapper is not callable"
                )),
                context,
            );
        };
        // 5. Set iterated to ? GetIteratorDirect(O).
        let iterated = get_iterator_direct(iterated.iterator(), context)?;

        // 6-8 are deferred to `IteratorHelper::create` and `Map::new`.
        let result = IteratorHelper::create(iterator_helper::Map::new(iterated, mapper), context);

        // 9. Return result.
        Ok(result.into())
    }

    /// `Iterator.prototype.filter ( predicate )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.filter
    fn filter(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this.as_object().ok_or_else(
            || js_error!(TypeError: "Iterator.prototype.filter called on non-object"),
        )?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. If IsCallable(predicate) is false, then
        //    a. Let error be ThrowCompletion(a newly created TypeError object).
        //    b. Return ? IteratorClose(iterated, error).
        let predicate = args.get_or_undefined(0);
        let Ok(predicate) = JsFunction::try_from_js(predicate, context) else {
            return iterated.close(
                Err(js_error!(
                    TypeError: "Iterator.prototype.filter: predicate is not callable"
                )),
                context,
            );
        };

        // 5. Set iterated to ? GetIteratorDirect(O).
        let iterated = get_iterator_direct(iterated.iterator(), context)?;

        // 6-8 are deferred to `IteratorHelper::create` and `Filter::new`.
        let result =
            IteratorHelper::create(iterator_helper::Filter::new(iterated, predicate), context);

        // 9. Return result.
        Ok(result.into())
    }

    /// `Iterator.prototype.take ( limit )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.take
    fn take(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this
            .as_object()
            .ok_or_else(|| js_error!(TypeError: "Iterator.prototype.take called on non-object"))?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. Let numLimit be Completion(ToNumber(limit)).
        // 5. IfAbruptCloseIterator(numLimit, iterated).
        let limit = args.get_or_undefined(0);
        let num_limit = if_abrupt_close_iterator!(limit.to_number(context), iterated, context);

        // 6. If numLimit is NaN, throw a RangeError exception.
        if num_limit.is_nan() {
            return iterated.close(
                Err(js_error!(
                    RangeError: "Iterator.prototype.take: limit cannot be NaN"
                )),
                context,
            );
        }

        // 7. Let integerLimit be ! ToIntegerOrInfinity(numLimit).
        let integer_limit = IntegerOrInfinity::from(num_limit);

        // 8. If integerLimit < 0, then
        let integer_limit = match integer_limit {
            IntegerOrInfinity::Integer(n) if n >= 0 => Some(n as u64),
            IntegerOrInfinity::PositiveInfinity => None,
            _ => {
                // a. Let error be ThrowCompletion(a newly created RangeError object).
                // b. Return ? IteratorClose(iterated, error).
                return iterated.close(
                    Err(js_error!(
                        RangeError: "Iterator.prototype.take: limit cannot be negative"
                    )),
                    context,
                );
            }
        };

        // 9. Set iterated to ? GetIteratorDirect(O).
        let iterated = get_iterator_direct(iterated.iterator(), context)?;

        // 10-12 are deferred to `IteratorHelper::create` and `Take::new`.
        let result =
            IteratorHelper::create(iterator_helper::Take::new(iterated, integer_limit), context);

        // 13. Return result.
        Ok(result.into())
    }

    /// `Iterator.prototype.drop ( limit )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.drop
    fn drop(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this
            .as_object()
            .ok_or_else(|| js_error!(TypeError: "Iterator.prototype.drop called on non-object"))?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o.clone(), JsValue::undefined());

        // 4. Let numLimit be Completion(ToNumber(limit)).
        // 5. IfAbruptCloseIterator(numLimit, iterated).
        let limit = args.get_or_undefined(0);
        let num_limit = if_abrupt_close_iterator!(limit.to_number(context), iterated, context);

        // 6. If numLimit is NaN, throw a RangeError exception.
        if num_limit.is_nan() {
            return iterated.close(
                Err(js_error!(
                    RangeError: "Iterator.prototype.drop: limit cannot be NaN"
                )),
                context,
            );
        }

        // 7. Let integerLimit be ! ToIntegerOrInfinity(numLimit).
        let integer_limit = IntegerOrInfinity::from(num_limit);

        // 8. If integerLimit < 0, then
        let integer_limit = match integer_limit {
            IntegerOrInfinity::Integer(n) if n >= 0 => Some(n as u64),
            IntegerOrInfinity::PositiveInfinity => None,
            _ => {
                // a. Let error be ThrowCompletion(a newly created RangeError object).
                // b. Return ? IteratorClose(iterated, error).
                return iterated.close(
                    Err(js_error!(
                        RangeError: "Iterator.prototype.drop: limit cannot be negative"
                    )),
                    context,
                );
            }
        };
        // 9. Set iterated to ? GetIteratorDirect(O).
        let iterated = get_iterator_direct(iterated.iterator(), context)?;

        // 10-12 are deferred to `IteratorHelper::create` and `Drop::new`.
        let result =
            IteratorHelper::create(iterator_helper::Drop::new(iterated, integer_limit), context);

        // 13. Return result.
        Ok(result.into())
    }

    /// `Iterator.prototype.flatMap ( mapper )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.flatmap
    fn flat_map(this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this.as_object().ok_or_else(
            || js_error!(TypeError: "Iterator.prototype.flatMap called on non-object"),
        )?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. If IsCallable(mapper) is false, then
        //    a. Let error be ThrowCompletion(a newly created TypeError object).
        //    b. Return ? IteratorClose(iterated, error).
        let mapper = args.get_or_undefined(0);
        let Ok(mapper) = JsFunction::try_from_js(mapper, context) else {
            return iterated.close(
                Err(js_error!(
                    TypeError: "Iterator.prototype.flatMap: mapper is not callable"
                )),
                context,
            );
        };

        // 5. Set iterated to ? GetIteratorDirect(O).
        let iterated = get_iterator_direct(iterated.iterator(), context)?;

        // 6-8 are deferred to `IteratorHelper::create` and `FlatMap::new`.
        let helper =
            IteratorHelper::create(iterator_helper::FlatMap::new(iterated, mapper), context);

        // 9. Return result.
        Ok(helper.into())
    }

    // ==================== Prototype Methods — Eager (Consuming) ====================

    /// `Iterator.prototype.reduce ( reducer [ , initialValue ] )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.reduce
    fn reduce(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this.as_object().ok_or_else(
            || js_error!(TypeError: "Iterator.prototype.reduce called on non-object"),
        )?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. If IsCallable(reducer) is false, then
        let Some(reducer) = args.get_or_undefined(0).as_callable() else {
            // a. Let error be ThrowCompletion(a newly created TypeError object).
            // b. Return ? IteratorClose(iterated, error).
            return iterated
                .close(
                Err(js_error!(
                    TypeError: "Iterator.prototype.reduce: reducer is not callable"
                )),
                context,
            )
            .map(NativeFunctionResult::complete);
        };

        // 5. Set iterated to ? GetIteratorDirect(O).
        let mut iterated = get_iterator_direct(iterated.iterator(), context)?;

        let (mut accumulator, mut counter) = if let Some(acc) = args.get(1).cloned() {
            // 7. Else,
            //    a. Let accumulator be initialValue.
            //    b. Let counter be 0.
            (acc, 0)
        } else if let Some(first) = iterated.step_value(context)? {
            // 6. If initialValue is not present, then
            //    a. Let accumulator be ? IteratorStepValue(iterated).
            //    c. Let counter be 1.
            (first, 1u64)
        } else {
            // b. If accumulator is done, throw a TypeError exception.
            return Err(js_error!(
                TypeError: "Iterator.prototype.reduce: cannot reduce empty iterator with no initial value"
            ));
        };

        // 8. Repeat,
        //    a. Let value be ? IteratorStepValue(iterated).
        //    b. If value is done, return accumulator.
        while let Some(value) = iterated.step_value(context)? {
            // c. Let result be Completion(Call(reducer, undefined, « accumulator, value, 𝔽(counter) »)).
            let result = reducer.call_interruptible(
                &JsValue::undefined(),
                &[accumulator.clone(), value, JsValue::new(counter)],
                context,
            );
            match result? {
                InterruptibleCallOutcome::Complete(value) => {
                    accumulator = value;
                }
                InterruptibleCallOutcome::Suspend(continuation) => {
                    return Ok(NativeFunctionResult::Suspend(
                        wrap_iterator_reduce_suspend(IteratorReduceSuspendState {
                            state: IteratorReduceState {
                                iterated: Cell::new(Some(iterated)),
                                callback: reducer,
                                accumulator: Cell::new(Some(accumulator)),
                                counter: Cell::new(counter),
                            },
                            continuation,
                        }),
                    ));
                }
            }

            // f. Set counter to counter + 1.
            counter += 1;
        }

        // Step 8.b
        Ok(NativeFunctionResult::complete(accumulator))
    }

    /// `Iterator.prototype.toArray ( )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.toarray
    fn to_array(this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this.as_object().ok_or_else(
            || js_error!(TypeError: "Iterator.prototype.toArray called on non-object"),
        )?;

        // 3. Let iterated be ? GetIteratorDirect(O).
        let iterated = get_iterator_direct(&o, context)?;

        // 4. Let items be a new empty List.
        // 5. Repeat ...
        let items = iterated.into_list(context)?;

        // b. If value is done, return CreateArrayFromList(items).
        Ok(Array::create_array_from_list(items, context)?.into())
    }

    /// `Iterator.prototype.forEach ( fn )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.foreach
    fn for_each(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this.as_object().ok_or_else(
            || js_error!(TypeError: "Iterator.prototype.forEach called on non-object"),
        )?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. If IsCallable(fn) is false, then
        let Some(func) = args.get_or_undefined(0).as_callable() else {
            // a. Let error be ThrowCompletion(a newly created TypeError object).
            // b. Return ? IteratorClose(iterated, error).
            return iterated
                .close(
                    Err(js_error!(
                        TypeError: "Iterator.prototype.forEach: argument is not callable"
                    )),
                    context,
                )
                .map(NativeFunctionResult::complete);
        };
        let func = JsFunction::from_object(func).expect("callable check must produce a function");

        // 5. Set iterated to ? GetIteratorDirect(O).
        let iterated = get_iterator_direct(iterated.iterator(), context)?;

        let state = IteratorForEachState {
            iterated: Cell::new(Some(iterated)),
            func,
            counter: Cell::new(0),
        };

        continue_iterator_for_each(&state, context)
    }

    /// `Iterator.prototype.some ( predicate )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.some
    fn some(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this
            .as_object()
            .ok_or_else(|| js_error!(TypeError: "Iterator.prototype.some called on non-object"))?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. If IsCallable(predicate) is false, then
        let Some(predicate) = args.get_or_undefined(0).as_callable() else {
            // a. Let error be ThrowCompletion(a newly created TypeError object).
            // b. Return ? IteratorClose(iterated, error).
            return iterated
                .close(
                Err(js_error!(
                    TypeError: "Iterator.prototype.some: predicate is not callable"
                )),
                context,
            )
            .map(NativeFunctionResult::complete);
        };

        // 5. Set iterated to ? GetIteratorDirect(O).
        let mut iterated = get_iterator_direct(iterated.iterator(), context)?;

        // 6. Let counter be 0.
        let mut counter = 0u64;
        // 7. Repeat,
        //    a. Let value be ? IteratorStepValue(iterated).
        //    b. If value is done, return false.
        while let Some(value) = iterated.step_value(context)? {
            // c. Let result be Completion(Call(predicate, undefined, « value, 𝔽(counter) »)).
            let result = predicate.call_interruptible(
                &JsValue::undefined(),
                &[value, JsValue::new(counter)],
                context,
            );
            match result? {
                InterruptibleCallOutcome::Complete(result) => {
                    // d. IfAbruptCloseIterator(result, iterated).
                    if result.to_boolean() {
                        return iterated
                            .close(Ok(JsValue::new(true)), context)
                            .map(NativeFunctionResult::complete);
                    }
                }
                InterruptibleCallOutcome::Suspend(continuation) => {
                    return Ok(NativeFunctionResult::Suspend(
                        wrap_iterator_some_suspend(IteratorSomeSuspendState {
                            state: IteratorSomeState {
                                iterated: Cell::new(Some(iterated)),
                                predicate,
                                this_arg: JsValue::undefined(),
                                counter: Cell::new(counter),
                            },
                            continuation,
                        }),
                    ));
                }
            }

            // f. Set counter to counter + 1.
            counter += 1;
        }

        // Step 7.b
        Ok(NativeFunctionResult::complete(false))
    }

    /// `Iterator.prototype.every ( predicate )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.every
    fn every(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this
            .as_object()
            .ok_or_else(|| js_error!(TypeError: "Iterator.prototype.every called on non-object"))?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. If IsCallable(predicate) is false, then
        let Some(predicate) = args.get_or_undefined(0).as_callable() else {
            // a. Let error be ThrowCompletion(a newly created TypeError object).
            // b. Return ? IteratorClose(iterated, error).
            return iterated
                .close(
                Err(js_error!(
                    TypeError: "Iterator.prototype.every: predicate is not callable"
                )),
                context,
            )
            .map(NativeFunctionResult::complete);
        };

        // 5. Set iterated to ? GetIteratorDirect(O).
        let mut iterated = get_iterator_direct(iterated.iterator(), context)?;

        // 6. Let counter be 0.
        let mut counter = 0u64;

        // 7. Repeat,
        //    a. Let value be ? IteratorStepValue(iterated).
        //    b. If value is done, return true.
        while let Some(value) = iterated.step_value(context)? {
            // c. Let result be Completion(Call(predicate, undefined, « value, 𝔽(counter) »)).
            let result = predicate.call_interruptible(
                &JsValue::undefined(),
                &[value, JsValue::new(counter)],
                context,
            );
            match result? {
                InterruptibleCallOutcome::Complete(result) => {
                    // d. IfAbruptCloseIterator(result, iterated).
                    if !result.to_boolean() {
                        return iterated
                            .close(Ok(JsValue::new(false)), context)
                            .map(NativeFunctionResult::complete);
                    }
                }
                InterruptibleCallOutcome::Suspend(continuation) => {
                    return Ok(NativeFunctionResult::Suspend(
                        wrap_iterator_every_suspend(IteratorEverySuspendState {
                            state: IteratorEveryState {
                                iterated: Cell::new(Some(iterated)),
                                predicate,
                                this_arg: JsValue::undefined(),
                                counter: Cell::new(counter),
                            },
                            continuation,
                        }),
                    ));
                }
            }

            // f. Set counter to counter + 1.
            counter += 1;
        }

        // Step 7.b
        Ok(NativeFunctionResult::complete(true))
    }

    /// `Iterator.prototype.find ( predicate )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-iterator.prototype.find
    fn find(
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<NativeFunctionResult> {
        // 1. Let O be the this value.
        // 2. If O is not an Object, throw a TypeError exception.
        let o = this
            .as_object()
            .ok_or_else(|| js_error!(TypeError: "Iterator.prototype.find called on non-object"))?;

        // 3. Let iterated be the Iterator Record { [[Iterator]]: O, [[NextMethod]]: undefined, [[Done]]: false }.
        let iterated = IteratorRecord::new(o, JsValue::undefined());

        // 4. If IsCallable(predicate) is false, then
        let Some(predicate) = args.get_or_undefined(0).as_callable() else {
            // a. Let error be ThrowCompletion(a newly created TypeError object).
            // b. Return ? IteratorClose(iterated, error).
            return iterated
                .close(
                Err(js_error!(
                    TypeError: "Iterator.prototype.find: predicate is not callable"
                )),
                context,
            )
            .map(NativeFunctionResult::complete);
        };
        // 5. Set iterated to ? GetIteratorDirect(O).
        let mut iterated = get_iterator_direct(iterated.iterator(), context)?;

        // 6. Let counter be 0.
        let mut counter = 0u64;

        // 7. Repeat,
        //    a. Let value be ? IteratorStepValue(iterated).
        //    b. If value is done, return undefined.
        while let Some(value) = iterated.step_value(context)? {
            // c. Let result be Completion(Call(predicate, undefined, « value, 𝔽(counter) »)).
            let result = predicate.call_interruptible(
                &JsValue::undefined(),
                &[value.clone(), JsValue::new(counter)],
                context,
            );
            match result? {
                InterruptibleCallOutcome::Complete(result) => {
                    // d. IfAbruptCloseIterator(result, iterated).
                    if result.to_boolean() {
                        return iterated
                            .close(Ok(value), context)
                            .map(NativeFunctionResult::complete);
                    }
                }
                InterruptibleCallOutcome::Suspend(continuation) => {
                    return Ok(NativeFunctionResult::Suspend(
                        wrap_iterator_find_suspend(IteratorFindSuspendState {
                            state: IteratorFindState {
                                iterated: Cell::new(Some(iterated)),
                                predicate,
                                this_arg: JsValue::undefined(),
                                counter: Cell::new(counter),
                            },
                            continuation,
                        }),
                    ));
                }
            }

            // f. Set counter to counter + 1.
            counter += 1;
        }

        // Step 7.b
        Ok(NativeFunctionResult::complete(JsValue::undefined()))
    }
}

#[derive(Trace, Finalize)]
struct IteratorReduceState {
    iterated: Cell<Option<IteratorRecord>>,
    callback: JsObject,
    accumulator: Cell<Option<JsValue>>,
    counter: Cell<u64>,
}

#[derive(Trace, Finalize)]
struct IteratorReduceSuspendState {
    state: IteratorReduceState,
    continuation: NativeResume,
}

fn clone_iterator_reduce_state(state: &IteratorReduceState) -> IteratorReduceState {
    let iterated = state.iterated.take();
    state.iterated.set(iterated.clone());
    let accumulator = state.accumulator.take();
    state.accumulator.set(accumulator.clone());
    IteratorReduceState {
        iterated: Cell::new(iterated),
        callback: state.callback.clone(),
        accumulator: Cell::new(accumulator),
        counter: Cell::new(state.counter.get()),
    }
}

fn wrap_iterator_reduce_suspend(state: IteratorReduceSuspendState) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_iterator_reduce_suspend, state)
}

fn resume_iterator_reduce_suspend(
    completion: CompletionRecord,
    state: &IteratorReduceSuspendState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match state.continuation.call(completion, context)? {
        NativeFunctionResult::Complete(record) => {
            let accumulator = record.consume()?;
            state.state.accumulator.set(Some(accumulator.clone()));
            continue_iterator_reduce(&state.state, context)
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_iterator_reduce_suspend(IteratorReduceSuspendState {
                state: clone_iterator_reduce_state(&state.state),
                continuation: next,
            }),
        )),
    }
}

fn continue_iterator_reduce(
    state: &IteratorReduceState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    let Some(mut iterated) = state.iterated.take() else {
        return Err(js_error!(
            TypeError: "Iterator.prototype.reduce continuation lost iterator state"
        ));
    };
    let mut accumulator = state
        .accumulator
        .take()
        .expect("reduce accumulator must exist");
    let mut counter = state.counter.get();

    while let Some(value) = iterated.step_value(context)? {
        let args = [accumulator.clone(), value, JsValue::new(counter)];
        match state
            .callback
            .call_interruptible(&JsValue::undefined(), &args, context)?
        {
            InterruptibleCallOutcome::Complete(next) => {
                accumulator = next;
            }
            InterruptibleCallOutcome::Suspend(continuation) => {
                state.iterated.set(Some(iterated));
                state.accumulator.set(Some(accumulator.clone()));
                state.counter.set(counter);
                return Ok(NativeFunctionResult::Suspend(wrap_iterator_reduce_suspend(
                    IteratorReduceSuspendState {
                        state: clone_iterator_reduce_state(state),
                        continuation,
                    },
                )));
            }
        }

        counter += 1;
        state.counter.set(counter);
    }

    state.iterated.set(Some(iterated));
    Ok(NativeFunctionResult::complete(accumulator))
}

#[derive(Trace, Finalize)]
struct IteratorSomeState {
    iterated: Cell<Option<IteratorRecord>>,
    predicate: JsObject,
    this_arg: JsValue,
    counter: Cell<u64>,
}

#[derive(Trace, Finalize)]
struct IteratorSomeSuspendState {
    state: IteratorSomeState,
    continuation: NativeResume,
}

fn clone_iterator_some_state(state: &IteratorSomeState) -> IteratorSomeState {
    let iterated = state.iterated.take();
    state.iterated.set(iterated.clone());
    IteratorSomeState {
        iterated: Cell::new(iterated),
        predicate: state.predicate.clone(),
        this_arg: state.this_arg.clone(),
        counter: Cell::new(state.counter.get()),
    }
}

fn wrap_iterator_some_suspend(state: IteratorSomeSuspendState) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_iterator_some_suspend, state)
}

fn resume_iterator_some_suspend(
    completion: CompletionRecord,
    state: &IteratorSomeSuspendState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match state.continuation.call(completion, context)? {
        NativeFunctionResult::Complete(record) => {
            if record.consume()?.to_boolean() {
                let Some(iterated) = state.state.iterated.take() else {
                    return Err(js_error!(
                        TypeError: "Iterator.prototype.some continuation lost iterator state"
                    ));
                };
                iterated
                    .close(Ok(JsValue::new(true)), context)
                    .map(NativeFunctionResult::complete)
            } else {
                continue_iterator_some(&state.state, context)
            }
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_iterator_some_suspend(IteratorSomeSuspendState {
                state: clone_iterator_some_state(&state.state),
                continuation: next,
            }),
        )),
    }
}

fn continue_iterator_some(
    state: &IteratorSomeState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    let Some(mut iterated) = state.iterated.take() else {
        return Err(js_error!(
            TypeError: "Iterator.prototype.some continuation lost iterator state"
        ));
    };
    let mut counter = state.counter.get();

    while let Some(value) = iterated.step_value(context)? {
        match state
            .predicate
            .call_interruptible(&state.this_arg, &[value, JsValue::new(counter)], context)?
        {
            InterruptibleCallOutcome::Complete(result) => {
                if result.to_boolean() {
                    state.iterated.set(Some(iterated));
                    return Ok(NativeFunctionResult::complete(true));
                }
            }
            InterruptibleCallOutcome::Suspend(continuation) => {
                state.iterated.set(Some(iterated));
                state.counter.set(counter);
                return Ok(NativeFunctionResult::Suspend(wrap_iterator_some_suspend(
                    IteratorSomeSuspendState {
                        state: clone_iterator_some_state(state),
                        continuation,
                    },
                )));
            }
        }

        counter += 1;
        state.counter.set(counter);
    }

    state.iterated.set(Some(iterated));
    Ok(NativeFunctionResult::complete(false))
}

#[derive(Trace, Finalize)]
struct IteratorEveryState {
    iterated: Cell<Option<IteratorRecord>>,
    predicate: JsObject,
    this_arg: JsValue,
    counter: Cell<u64>,
}

#[derive(Trace, Finalize)]
struct IteratorEverySuspendState {
    state: IteratorEveryState,
    continuation: NativeResume,
}

fn clone_iterator_every_state(state: &IteratorEveryState) -> IteratorEveryState {
    let iterated = state.iterated.take();
    state.iterated.set(iterated.clone());
    IteratorEveryState {
        iterated: Cell::new(iterated),
        predicate: state.predicate.clone(),
        this_arg: state.this_arg.clone(),
        counter: Cell::new(state.counter.get()),
    }
}

fn wrap_iterator_every_suspend(state: IteratorEverySuspendState) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_iterator_every_suspend, state)
}

fn resume_iterator_every_suspend(
    completion: CompletionRecord,
    state: &IteratorEverySuspendState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match state.continuation.call(completion, context)? {
        NativeFunctionResult::Complete(record) => {
            if !record.consume()?.to_boolean() {
                let Some(iterated) = state.state.iterated.take() else {
                    return Err(js_error!(
                        TypeError: "Iterator.prototype.every continuation lost iterator state"
                    ));
                };
                iterated
                    .close(Ok(JsValue::new(false)), context)
                    .map(NativeFunctionResult::complete)
            } else {
                continue_iterator_every(&state.state, context)
            }
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_iterator_every_suspend(IteratorEverySuspendState {
                state: clone_iterator_every_state(&state.state),
                continuation: next,
            }),
        )),
    }
}

fn continue_iterator_every(
    state: &IteratorEveryState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    let Some(mut iterated) = state.iterated.take() else {
        return Err(js_error!(
            TypeError: "Iterator.prototype.every continuation lost iterator state"
        ));
    };
    let mut counter = state.counter.get();

    while let Some(value) = iterated.step_value(context)? {
        match state
            .predicate
            .call_interruptible(&state.this_arg, &[value, JsValue::new(counter)], context)?
        {
            InterruptibleCallOutcome::Complete(result) => {
                if !result.to_boolean() {
                    state.iterated.set(Some(iterated));
                    return Ok(NativeFunctionResult::complete(false));
                }
            }
            InterruptibleCallOutcome::Suspend(continuation) => {
                state.iterated.set(Some(iterated));
                state.counter.set(counter);
                return Ok(NativeFunctionResult::Suspend(wrap_iterator_every_suspend(
                    IteratorEverySuspendState {
                        state: clone_iterator_every_state(state),
                        continuation,
                    },
                )));
            }
        }

        counter += 1;
        state.counter.set(counter);
    }

    state.iterated.set(Some(iterated));
    Ok(NativeFunctionResult::complete(true))
}

#[derive(Trace, Finalize)]
struct IteratorFindState {
    iterated: Cell<Option<IteratorRecord>>,
    predicate: JsObject,
    this_arg: JsValue,
    counter: Cell<u64>,
}

#[derive(Trace, Finalize)]
struct IteratorFindSuspendState {
    state: IteratorFindState,
    continuation: NativeResume,
}

fn clone_iterator_find_state(state: &IteratorFindState) -> IteratorFindState {
    let iterated = state.iterated.take();
    state.iterated.set(iterated.clone());
    IteratorFindState {
        iterated: Cell::new(iterated),
        predicate: state.predicate.clone(),
        this_arg: state.this_arg.clone(),
        counter: Cell::new(state.counter.get()),
    }
}

fn wrap_iterator_find_suspend(state: IteratorFindSuspendState) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_iterator_find_suspend, state)
}

fn resume_iterator_find_suspend(
    completion: CompletionRecord,
    state: &IteratorFindSuspendState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match state.continuation.call(completion, context)? {
        NativeFunctionResult::Complete(record) => {
            if record.consume()?.to_boolean() {
                let Some(mut iterated) = state.state.iterated.take() else {
                    return Err(js_error!(
                        TypeError: "Iterator.prototype.find continuation lost iterator state"
                    ));
                };
                let value = iterated.value(context)?;
                iterated
                    .close(Ok(value), context)
                    .map(NativeFunctionResult::complete)
            } else {
                continue_iterator_find(&state.state, context)
            }
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_iterator_find_suspend(IteratorFindSuspendState {
                state: clone_iterator_find_state(&state.state),
                continuation: next,
            }),
        )),
    }
}

fn continue_iterator_find(
    state: &IteratorFindState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    let Some(mut iterated) = state.iterated.take() else {
        return Err(js_error!(
            TypeError: "Iterator.prototype.find continuation lost iterator state"
        ));
    };
    let mut counter = state.counter.get();

    while let Some(value) = iterated.step_value(context)? {
        match state
            .predicate
            .call_interruptible(&state.this_arg, &[value.clone(), JsValue::new(counter)], context)?
        {
                InterruptibleCallOutcome::Complete(result) => {
                    if result.to_boolean() {
                        let close_result = iterated.close(Ok(value), context);
                        state.iterated.set(Some(iterated));
                        return close_result.map(NativeFunctionResult::complete);
                    }
                }
            InterruptibleCallOutcome::Suspend(continuation) => {
                state.iterated.set(Some(iterated));
                state.counter.set(counter);
                return Ok(NativeFunctionResult::Suspend(wrap_iterator_find_suspend(
                    IteratorFindSuspendState {
                        state: clone_iterator_find_state(state),
                        continuation,
                    },
                )));
            }
        }

        counter += 1;
        state.counter.set(counter);
    }

    state.iterated.set(Some(iterated));
    Ok(NativeFunctionResult::complete(JsValue::undefined()))
}

#[derive(Trace, Finalize)]
struct IteratorForEachState {
    iterated: Cell<Option<IteratorRecord>>,
    func: JsFunction,
    counter: Cell<u64>,
}

#[derive(Trace, Finalize)]
struct IteratorForEachSuspendState {
    state: IteratorForEachState,
    continuation: NativeResume,
}

fn clone_iterator_for_each_state(state: &IteratorForEachState) -> IteratorForEachState {
    let iterated = state.iterated.take();
    state.iterated.set(iterated.clone());
    IteratorForEachState {
        iterated: Cell::new(iterated),
        func: state.func.clone(),
        counter: Cell::new(state.counter.get()),
    }
}

fn wrap_iterator_for_each_suspend(state: IteratorForEachSuspendState) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_iterator_for_each_suspend, state)
}

fn resume_iterator_for_each_suspend(
    completion: CompletionRecord,
    state: &IteratorForEachSuspendState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    match state.continuation.call(completion, context)? {
        NativeFunctionResult::Complete(record) => {
            record.consume()?;
            continue_iterator_for_each(&state.state, context)
        }
        NativeFunctionResult::Suspend(next) => Ok(NativeFunctionResult::Suspend(
            wrap_iterator_for_each_suspend(IteratorForEachSuspendState {
                state: clone_iterator_for_each_state(&state.state),
                continuation: next,
            }),
        )),
    }
}

fn continue_iterator_for_each(
    state: &IteratorForEachState,
    context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    let Some(mut iterated) = state.iterated.take() else {
        return Err(js_error!(
            TypeError: "Iterator.prototype.forEach continuation lost iterator state"
        ));
    };
    let mut counter = state.counter.get();

    while let Some(value) = iterated.step_value(context)? {
        match state
            .func
            .call_interruptible(&JsValue::undefined(), &[value, JsValue::new(counter)], context)?
        {
            InterruptibleCallOutcome::Complete(_) => {}
            InterruptibleCallOutcome::Suspend(continuation) => {
                state.counter.set(counter + 1);
                state.iterated.set(Some(iterated));
                return Ok(NativeFunctionResult::Suspend(wrap_iterator_for_each_suspend(
                    IteratorForEachSuspendState {
                        state: clone_iterator_for_each_state(state),
                        continuation,
                    },
                )));
            }
        }

        counter += 1;
        state.counter.set(counter);
    }

    state.iterated.set(Some(iterated));
    Ok(NativeFunctionResult::complete(JsValue::undefined()))
}
