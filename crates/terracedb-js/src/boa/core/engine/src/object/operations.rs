use super::internal_methods::InternalMethodPropertyContext;
use crate::context::interrupt_trace;
use crate::js_error;
use crate::value::JsVariant;
use crate::{
    Context, JsExpect, JsResult, JsSymbol, JsValue,
    builtins::{
        Array, Proxy,
        function::{BoundFunction, ClassFieldDefinition, OrdinaryFunction, set_function_name},
    },
    context::ExecutionOutcome,
    context::intrinsics::{StandardConstructor, StandardConstructors},
    error::JsNativeError,
    native_function::{NativeFunctionObject, NativeResume},
    object::{
        CONSTRUCTOR, JsObject, PROTOTYPE, PrivateElement, PrivateName,
        internal_methods::ResolvedCallValue,
    },
    property::{PropertyDescriptor, PropertyDescriptorBuilder, PropertyKey, PropertyNameKind},
    realm::Realm,
    string::StaticJsStrings,
    value::Type,
    vm::CompletionRecord,
};
use boa_gc::{Finalize, Trace};

fn wrap_get_method_resume(continuation: NativeResume) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_get_method, continuation)
}

fn resume_get_method(
    completion: CompletionRecord,
    continuation: &NativeResume,
    context: &mut Context,
) -> JsResult<crate::native_function::NativeFunctionResult> {
    match continuation.call(completion, context)? {
        crate::native_function::NativeFunctionResult::Complete(record) => match record {
            CompletionRecord::Normal(func) | CompletionRecord::Return(func) => match func.variant() {
                JsVariant::Undefined | JsVariant::Null => Ok(
                    crate::native_function::NativeFunctionResult::complete(JsValue::undefined()),
                ),
                JsVariant::Object(obj) if obj.is_callable() => Ok(
                    crate::native_function::NativeFunctionResult::complete(obj.clone()),
                ),
                _ => Err(JsNativeError::typ()
                    .with_message("value returned for property of object is not a function")
                    .into()),
            },
            CompletionRecord::Throw(err) => Ok(
                crate::native_function::NativeFunctionResult::from_completion(
                    CompletionRecord::Throw(err),
                ),
            ),
            CompletionRecord::Suspend => Err(JsNativeError::error()
                .with_message("get method continuation resumed with unexpected suspend completion")
                .into()),
        },
        crate::native_function::NativeFunctionResult::Suspend(next) => Ok(
            crate::native_function::NativeFunctionResult::Suspend(wrap_get_method_resume(next)),
        ),
    }
}

/// Object integrity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityLevel {
    /// Sealed object integrity level.
    ///
    /// Preventing new properties from being added to it and marking all existing
    /// properties as non-configurable. Values of present properties can still be
    /// changed as long as they are writable.
    Sealed,

    /// Frozen object integrity level
    ///
    /// A frozen object can no longer be changed; freezing an object prevents new
    /// properties from being added to it, existing properties from being removed,
    /// prevents changing the enumerability, configurability, or writability of
    /// existing properties, and prevents the values of existing properties from
    /// being changed. In addition, freezing an object also prevents its prototype
    /// from being changed.
    Frozen,
}

#[derive(Clone)]
pub enum InterruptibleCallOutcome<T> {
    Complete(T),
    Suspend(NativeResume),
}

fn wrap_running_call_suspend(continuation: NativeResume) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_running_call_suspend, continuation)
}

fn resume_running_call_suspend(
    completion: CompletionRecord,
    continuation: &NativeResume,
    context: &mut Context,
) -> JsResult<crate::native_function::NativeFunctionResult> {
    match continuation.call(completion, context)? {
        crate::native_function::NativeFunctionResult::Complete(record) => {
            context.vm.pop_frame().js_expect("frame must exist")?;
            Ok(crate::native_function::NativeFunctionResult::from_completion(record))
        }
        crate::native_function::NativeFunctionResult::Suspend(next) => Ok(
            crate::native_function::NativeFunctionResult::Suspend(wrap_running_call_suspend(next)),
        ),
    }
}

#[derive(Clone, Trace, Finalize)]
struct DefineFieldResume {
    continuation: NativeResume,
    receiver: JsObject,
    field_record: ClassFieldDefinition,
}

fn wrap_define_field_suspend(capture: DefineFieldResume) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_define_field_suspend, capture)
}

fn finish_define_field(
    receiver: &JsObject,
    field_record: &ClassFieldDefinition,
    init_value: JsValue,
    context: &mut Context,
) -> JsResult<()> {
    match field_record {
        ClassFieldDefinition::Private(field_name, _) => {
            receiver.private_field_add(field_name, init_value, context)?;
        }
        ClassFieldDefinition::Public(field_name, _, function_name) => {
            if let Some(function_name) = function_name {
                set_function_name(
                    &init_value
                        .as_object()
                        .js_expect("init value must be a function object")?,
                    function_name,
                    None,
                    context,
                )?;
            }

            receiver.create_data_property_or_throw(field_name.clone(), init_value, context)?;
        }
    }

    Ok(())
}

fn resume_define_field_suspend(
    completion: CompletionRecord,
    capture: &DefineFieldResume,
    context: &mut Context,
) -> JsResult<crate::native_function::NativeFunctionResult> {
    match capture.continuation.call(completion, context)? {
        crate::native_function::NativeFunctionResult::Complete(record) => match record {
            CompletionRecord::Normal(init_value) | CompletionRecord::Return(init_value) => {
                finish_define_field(&capture.receiver, &capture.field_record, init_value, context)?;
                Ok(crate::native_function::NativeFunctionResult::complete(
                    JsValue::undefined(),
                ))
            }
            CompletionRecord::Throw(err) => Ok(
                crate::native_function::NativeFunctionResult::from_completion(
                    CompletionRecord::Throw(err),
                ),
            ),
            CompletionRecord::Suspend => Err(JsNativeError::error()
                .with_message("define field continuation resumed with unexpected suspend completion")
                .into()),
        },
        crate::native_function::NativeFunctionResult::Suspend(next) => Ok(
            crate::native_function::NativeFunctionResult::Suspend(wrap_define_field_suspend(
                DefineFieldResume {
                    continuation: next,
                    receiver: capture.receiver.clone(),
                    field_record: capture.field_record.clone(),
                },
            )),
        ),
    }
}

#[derive(Clone, Trace, Finalize)]
struct InitializeInstanceElementsResume {
    continuation: NativeResume,
    receiver: JsObject,
    constructor: JsObject,
    next_field_index: usize,
}

fn wrap_initialize_instance_elements_suspend(capture: InitializeInstanceElementsResume) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(
        resume_initialize_instance_elements_suspend,
        capture,
    )
}

fn continue_initialize_instance_elements_from_index(
    receiver: &JsObject,
    constructor: &JsObject,
    start_index: usize,
    context: &mut Context,
) -> JsResult<ExecutionOutcome<()>> {
    let constructor_function = constructor
        .downcast_ref::<OrdinaryFunction>()
        .js_expect("class constructor must be function object")?;

    for (index, field_record) in constructor_function.get_fields().iter().enumerate().skip(start_index) {
        match receiver.define_field_interruptible(field_record, context)? {
            ExecutionOutcome::Complete(()) => {}
            ExecutionOutcome::Suspend(continuation) => {
                return Ok(ExecutionOutcome::Suspend(
                    wrap_initialize_instance_elements_suspend(InitializeInstanceElementsResume {
                        continuation,
                        receiver: receiver.clone(),
                        constructor: constructor.clone(),
                        next_field_index: index + 1,
                    }),
                ));
            }
        }
    }

    Ok(ExecutionOutcome::Complete(()))
}

fn resume_initialize_instance_elements_suspend(
    completion: CompletionRecord,
    capture: &InitializeInstanceElementsResume,
    context: &mut Context,
) -> JsResult<crate::native_function::NativeFunctionResult> {
    match capture.continuation.call(completion, context)? {
        crate::native_function::NativeFunctionResult::Complete(record) => match record {
            CompletionRecord::Normal(_) | CompletionRecord::Return(_) => {
                match continue_initialize_instance_elements_from_index(
                    &capture.receiver,
                    &capture.constructor,
                    capture.next_field_index,
                    context,
                )? {
                    ExecutionOutcome::Complete(()) => Ok(
                        crate::native_function::NativeFunctionResult::complete(JsValue::undefined()),
                    ),
                    ExecutionOutcome::Suspend(continuation) => Ok(
                        crate::native_function::NativeFunctionResult::Suspend(continuation),
                    ),
                }
            }
            CompletionRecord::Throw(err) => Ok(
                crate::native_function::NativeFunctionResult::from_completion(
                    CompletionRecord::Throw(err),
                ),
            ),
            CompletionRecord::Suspend => Err(JsNativeError::error()
                .with_message(
                    "initialize instance elements continuation resumed with unexpected suspend completion",
                )
                .into()),
        },
        crate::native_function::NativeFunctionResult::Suspend(next) => Ok(
            crate::native_function::NativeFunctionResult::Suspend(
                wrap_initialize_instance_elements_suspend(InitializeInstanceElementsResume {
                    continuation: next,
                    receiver: capture.receiver.clone(),
                    constructor: capture.constructor.clone(),
                    next_field_index: capture.next_field_index,
                }),
            ),
        ),
    }
}

#[derive(Clone, Trace, Finalize)]
struct ValueInvokeResume {
    continuation: NativeResume,
    this_value: JsValue,
    args: Vec<JsValue>,
}

fn wrap_value_invoke_resume(capture: ValueInvokeResume) -> NativeResume {
    NativeResume::from_copy_closure_with_captures(resume_value_invoke, capture)
}

fn resume_value_invoke(
    completion: CompletionRecord,
    capture: &ValueInvokeResume,
    context: &mut Context,
) -> JsResult<crate::native_function::NativeFunctionResult> {
    match capture.continuation.call(completion, context)? {
        crate::native_function::NativeFunctionResult::Complete(record) => match record {
            CompletionRecord::Normal(func) | CompletionRecord::Return(func) => {
                let Some(object) = func.as_object() else {
                    return Err(JsNativeError::typ()
                        .with_message("value returned for property of object is not a function")
                        .into());
                };
                match object.call_interruptible(&capture.this_value, &capture.args, context)? {
                    InterruptibleCallOutcome::Complete(value) => Ok(
                        crate::native_function::NativeFunctionResult::complete(value),
                    ),
                    InterruptibleCallOutcome::Suspend(continuation) => Ok(
                        crate::native_function::NativeFunctionResult::Suspend(continuation),
                    ),
                }
            }
            CompletionRecord::Throw(err) => Ok(
                crate::native_function::NativeFunctionResult::from_completion(
                    CompletionRecord::Throw(err),
                ),
            ),
            CompletionRecord::Suspend => Err(JsNativeError::error()
                .with_message("invoke continuation resumed with unexpected suspend completion")
                .into()),
        },
        crate::native_function::NativeFunctionResult::Suspend(next) => Ok(
            crate::native_function::NativeFunctionResult::Suspend(wrap_value_invoke_resume(
                ValueInvokeResume {
                    continuation: next,
                    this_value: capture.this_value.clone(),
                    args: capture.args.clone(),
                },
            )),
        ),
    }
}

impl<T> InterruptibleCallOutcome<T> {
    pub fn into_execution_outcome(self) -> ExecutionOutcome<T> {
        match self {
            Self::Complete(value) => ExecutionOutcome::Complete(value),
            Self::Suspend(continuation) => ExecutionOutcome::Suspend(continuation),
        }
    }
}

impl IntegrityLevel {
    /// Returns `true` if the integrity level is sealed.
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        matches!(self, Self::Sealed)
    }

    /// Returns `true` if the integrity level is frozen.
    #[must_use]
    pub const fn is_frozen(&self) -> bool {
        matches!(self, Self::Frozen)
    }
}

impl JsObject {
    /// Check if object is extensible.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-isextensible-o
    #[inline]
    pub fn is_extensible(&self, context: &mut Context) -> JsResult<bool> {
        // 1. Return ? O.[[IsExtensible]]().
        self.__is_extensible__(context)
    }

    /// Get property from object or throw.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-get-o-p
    pub fn get<K>(&self, key: K, context: &mut Context) -> JsResult<JsValue>
    where
        K: Into<PropertyKey>,
    {
        let key = key.into();
        interrupt_trace(|| format!("JsObject::get key={key}"));
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Return ? O.[[Get]](P, O).
        self.__get__(
            &key,
            self.clone().into(),
            &mut InternalMethodPropertyContext::new(context),
        )
    }

    /// Get property from object through the interruptible execution path.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-get-o-p
    pub fn get_interruptible<K>(
        &self,
        key: K,
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<JsValue>>
    where
        K: Into<PropertyKey>,
    {
        let key = key.into();
        interrupt_trace(|| format!("JsObject::get_interruptible key={key}"));
        self.__get_interruptible__(
            &key,
            self.clone().into(),
            &mut InternalMethodPropertyContext::new(context),
        )
    }

    /// set property of object or throw if bool flag is passed.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-set-o-p-v-throw
    pub fn set<K, V>(&self, key: K, value: V, throw: bool, context: &mut Context) -> JsResult<bool>
    where
        K: Into<PropertyKey>,
        V: Into<JsValue>,
    {
        let key = key.into();
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Assert: Type(Throw) is Boolean.
        // 4. Let success be ? O.[[Set]](P, V, O).
        let success = self.__set__(
            key.clone(),
            value.into(),
            self.clone().into(),
            &mut InternalMethodPropertyContext::new(context),
        )?;
        // 5. If success is false and Throw is true, throw a TypeError exception.
        if !success && throw {
            return Err(JsNativeError::typ()
                .with_message(format!("cannot set non-writable property: {key}"))
                .into());
        }
        // 6. Return success.
        Ok(success)
    }

    /// Create data property
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-createdataproperty
    pub fn create_data_property<K, V>(
        &self,
        key: K,
        value: V,
        context: &mut Context,
    ) -> JsResult<bool>
    where
        K: Into<PropertyKey>,
        V: Into<JsValue>,
    {
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Let newDesc be the PropertyDescriptor { [[Value]]: V, [[Writable]]: true, [[Enumerable]]: true, [[Configurable]]: true }.
        let new_desc = PropertyDescriptor::builder()
            .value(value)
            .writable(true)
            .enumerable(true)
            .configurable(true);
        // 4. Return ? O.[[DefineOwnProperty]](P, newDesc).
        self.__define_own_property__(
            &key.into(),
            new_desc.into(),
            &mut InternalMethodPropertyContext::new(context),
        )
    }

    /// Create data property
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-createdataproperty
    pub(crate) fn create_data_property_with_slot<K, V>(
        &self,
        key: K,
        value: V,
        context: &mut InternalMethodPropertyContext<'_>,
    ) -> JsResult<bool>
    where
        K: Into<PropertyKey>,
        V: Into<JsValue>,
    {
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Let newDesc be the PropertyDescriptor { [[Value]]: V, [[Writable]]: true, [[Enumerable]]: true, [[Configurable]]: true }.
        let new_desc = PropertyDescriptor::builder()
            .value(value)
            .writable(true)
            .enumerable(true)
            .configurable(true);
        // 4. Return ? O.[[DefineOwnProperty]](P, newDesc).
        self.__define_own_property__(&key.into(), new_desc.into(), context)
    }

    /// `10.2.8 DefineMethodProperty ( homeObject, key, closure, enumerable )`
    ///
    /// Defines a method property on an object with the specified attributes.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-definemethodproperty
    pub(crate) fn define_method_property<K, V>(
        &self,
        key: K,
        value: V,
        context: &mut InternalMethodPropertyContext<'_>,
    ) -> JsResult<()>
    where
        K: Into<PropertyKey>,
        V: Into<JsValue>,
    {
        // 1. Assert: homeObject is an ordinary, extensible object with no non-configurable properties.
        // 2. If key is a Private Name, then
        //    a. Return PrivateElement { [[Key]]: key, [[Kind]]: method, [[Value]]: closure }.
        // 3. Else,
        //    a. Let desc be the PropertyDescriptor { [[Value]]: closure, [[Writable]]: true, [[Enumerable]]: enumerable, [[Configurable]]: true }.
        let new_desc = PropertyDescriptor::builder()
            .value(value)
            .writable(true)
            .enumerable(false)
            .configurable(true);

        //    b. Perform ! DefinePropertyOrThrow(homeObject, key, desc).
        self.__define_own_property__(&key.into(), new_desc.into(), context)?;

        //    c. Return unused.
        Ok(())
    }

    /// Create data property or throw
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-createdatapropertyorthrow
    pub fn create_data_property_or_throw<K, V>(
        &self,
        key: K,
        value: V,
        context: &mut Context,
    ) -> JsResult<bool>
    where
        K: Into<PropertyKey>,
        V: Into<JsValue>,
    {
        let key = key.into();
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Let success be ? CreateDataProperty(O, P, V).
        let success = self.create_data_property(key.clone(), value, context)?;
        // 4. If success is false, throw a TypeError exception.
        if !success {
            return Err(JsNativeError::typ()
                .with_message(format!("cannot redefine property: {key}"))
                .into());
        }
        // 5. Return success.
        Ok(success)
    }

    /// Create non-enumerable data property or throw
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-createnonenumerabledatapropertyinfallibly
    pub(crate) fn create_non_enumerable_data_property_or_throw<K, V>(
        &self,
        key: K,
        value: V,
        context: &mut Context,
    ) -> JsResult<()>
    where
        K: Into<PropertyKey>,
        V: Into<JsValue>,
    {
        // 1. Assert: O is an ordinary, extensible object with no non-configurable properties.

        // 2. Let newDesc be the PropertyDescriptor {
        //    [[Value]]: V,
        //    [[Writable]]: true,
        //    [[Enumerable]]: false,
        //    [[Configurable]]: true
        //  }.
        let new_desc = PropertyDescriptorBuilder::new()
            .value(value)
            .writable(true)
            .enumerable(false)
            .configurable(true)
            .build();

        // 3. Perform ! DefinePropertyOrThrow(O, P, newDesc).
        self.define_property_or_throw(key, new_desc, context)?;

        // 4. Return unused.
        Ok(())
    }

    /// Define property or throw.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-definepropertyorthrow
    pub fn define_property_or_throw<K, P>(
        &self,
        key: K,
        desc: P,
        context: &mut Context,
    ) -> JsResult<bool>
    where
        K: Into<PropertyKey>,
        P: Into<PropertyDescriptor>,
    {
        let key = key.into();
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Let success be ? O.[[DefineOwnProperty]](P, desc).
        let success = self.__define_own_property__(
            &key,
            desc.into(),
            &mut InternalMethodPropertyContext::new(context),
        )?;
        // 4. If success is false, throw a TypeError exception.
        if !success {
            return Err(JsNativeError::typ()
                .with_message(format!("cannot redefine property: {key}"))
                .into());
        }
        // 5. Return success.
        Ok(success)
    }

    /// Defines the property or throws a `TypeError` if the operation fails.
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-deletepropertyorthrow
    pub fn delete_property_or_throw<K>(&self, key: K, context: &mut Context) -> JsResult<bool>
    where
        K: Into<PropertyKey>,
    {
        let key = key.into();
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Let success be ? O.[[Delete]](P).
        let success = self.__delete__(&key, &mut InternalMethodPropertyContext::new(context))?;
        // 4. If success is false, throw a TypeError exception.
        if !success {
            return Err(JsNativeError::typ()
                .with_message(format!("cannot delete non-configurable property: {key}"))
                .into());
        }
        // 5. Return success.
        Ok(success)
    }

    /// Check if object has property.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-hasproperty
    pub fn has_property<K>(&self, key: K, context: &mut Context) -> JsResult<bool>
    where
        K: Into<PropertyKey>,
    {
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Return ? O.[[HasProperty]](P).

        self.__has_property__(
            &key.into(),
            &mut InternalMethodPropertyContext::new(context),
        )
    }

    pub fn has_property_interruptible<K>(
        &self,
        key: K,
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<bool>>
    where
        K: Into<PropertyKey>,
    {
        self.__has_property_interruptible__(
            &key.into(),
            &mut InternalMethodPropertyContext::new(context),
        )
    }

    /// Abstract optimization operation.
    ///
    /// Check if an object has a property and get it if it exists.
    /// This operation combines the abstract operations `HasProperty` and `Get`.
    ///
    /// More information:
    ///  - [ECMAScript reference HasProperty][spec0]
    ///  - [ECMAScript reference Get][spec1]
    ///
    /// [spec0]: https://tc39.es/ecma262/#sec-hasproperty
    /// [spec1]: https://tc39.es/ecma262/#sec-get-o-p
    pub(crate) fn try_get<K>(&self, key: K, context: &mut Context) -> JsResult<Option<JsValue>>
    where
        K: Into<PropertyKey>,
    {
        self.__try_get__(
            &key.into(),
            self.clone().into(),
            &mut InternalMethodPropertyContext::new(context),
        )
    }

    /// Check if object has an own property.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-hasownproperty
    pub fn has_own_property<K>(&self, key: K, context: &mut Context) -> JsResult<bool>
    where
        K: Into<PropertyKey>,
    {
        let key = key.into();
        // 1. Assert: Type(O) is Object.
        // 2. Assert: IsPropertyKey(P) is true.
        // 3. Let desc be ? O.[[GetOwnProperty]](P).
        let desc =
            self.__get_own_property__(&key, &mut InternalMethodPropertyContext::new(context))?;
        // 4. If desc is undefined, return false.
        // 5. Return true.
        Ok(desc.is_some())
    }

    /// Get all the keys of the properties of this object.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-ordinary-object-internal-methods-and-internal-slots-ownpropertykeys
    pub fn own_property_keys(&self, context: &mut Context) -> JsResult<Vec<PropertyKey>> {
        self.__own_property_keys__(context)
    }

    /// `Call ( F, V [ , argumentsList ] )`
    ///
    /// # Panics
    ///
    /// Panics if the object is currently mutably borrowed.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-call
    #[track_caller]
    #[inline]
    pub fn call(
        &self,
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<JsValue> {
        match self.call_interruptible(this, args, context)? {
            InterruptibleCallOutcome::Complete(value) => Ok(value),
            InterruptibleCallOutcome::Suspend(_) => {
                let caller = std::panic::Location::caller();
                Err(JsNativeError::error()
                    .with_message(format!(
                        "suspendable native function requires interruptible execution at {}:{}:{}",
                        caller.file(),
                        caller.line(),
                        caller.column(),
                    ))
                    .into())
            }
        }
    }

    #[track_caller]
    #[inline]
    pub fn call_interruptible(
        &self,
        this: &JsValue,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<InterruptibleCallOutcome<JsValue>> {
        // SKIP: 1. If argumentsList is not present, set argumentsList to a new empty List.
        // SKIP: 2. If IsCallable(F) is false, throw a TypeError exception.
        // NOTE(HalidOdat): For object's that are not callable we implement a special __call__ internal method
        //                  that throws on call.

        context.vm.stack.push(this.clone()); // this
        context.vm.stack.push(self.clone()); // func
        let argument_count = args.len();
        context.vm.stack.calling_convention_push_arguments(args);

        // 3. Return ? F.[[Call]](V, argumentsList).
        let frame_index = context.vm.frames.len();
        match self.__call__(argument_count).resolve(context)? {
            ResolvedCallValue::Complete => {
                return Ok(InterruptibleCallOutcome::Complete(context.vm.stack.pop()));
            }
            ResolvedCallValue::Suspended(continuation) => {
                return Ok(InterruptibleCallOutcome::Suspend(continuation));
            }
            ResolvedCallValue::Ready => {}
        }

        if frame_index + 1 == context.vm.frames.len() {
            context.vm.frame_mut().set_exit_early(true);
        } else {
            context.vm.frames[frame_index + 1].set_exit_early(true);
        }

        context.vm.host_call_depth += 1;
        let result = context.run();
        context.vm.host_call_depth = context.vm.host_call_depth.saturating_sub(1);

        if matches!(result, crate::vm::CompletionRecord::Suspend) {
            let continuation = context
                .vm
                .suspended_resume
                .clone()
                .expect("suspended execution must install a continuation");
            return Ok(InterruptibleCallOutcome::Suspend(
                wrap_running_call_suspend(continuation),
            ));
        }

        context.vm.pop_frame().js_expect("frame must exist")?;

        result.consume().map(InterruptibleCallOutcome::Complete)
    }

    /// `Construct ( F [ , argumentsList [ , newTarget ] ] )`
    ///
    /// Construct an instance of this object with the specified arguments.
    ///
    /// # Panics
    ///
    /// Panics if the object is currently mutably borrowed.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-construct
    #[track_caller]
    #[inline]
    pub fn construct(
        &self,
        args: &[JsValue],
        new_target: Option<&Self>,
        context: &mut Context,
    ) -> JsResult<Self> {
        match self.construct_interruptible(args, new_target, context)? {
            InterruptibleCallOutcome::Complete(value) => Ok(value),
            InterruptibleCallOutcome::Suspend(_) => Err(JsNativeError::error()
                .with_message("suspendable native constructor requires interruptible execution")
                .into()),
        }
    }

    #[track_caller]
    #[inline]
    pub fn construct_interruptible(
        &self,
        args: &[JsValue],
        new_target: Option<&Self>,
        context: &mut Context,
    ) -> JsResult<InterruptibleCallOutcome<Self>> {
        // 1. If newTarget is not present, set newTarget to F.
        let new_target = new_target.unwrap_or(self);

        context.vm.stack.push(JsValue::undefined());
        context.vm.stack.push(self.clone()); // func
        let argument_count = args.len();
        context.vm.stack.calling_convention_push_arguments(args);
        context.vm.stack.push(new_target.clone());

        // 2. If argumentsList is not present, set argumentsList to a new empty List.
        // 3. Return ? F.[[Construct]](argumentsList, newTarget).
        let frame_index = context.vm.frames.len();

        match self.__construct__(argument_count).resolve(context)? {
            ResolvedCallValue::Complete => {
                let result = context.vm.stack.pop();
                return Ok(InterruptibleCallOutcome::Complete(
                    result
                        .as_object()
                        .js_expect("construct value should be an object")?
                        .clone(),
                ));
            }
            ResolvedCallValue::Suspended(continuation) => {
                return Ok(InterruptibleCallOutcome::Suspend(continuation));
            }
            ResolvedCallValue::Ready => {}
        }

        if frame_index + 1 == context.vm.frames.len() {
            context.vm.frame_mut().set_exit_early(true);
        } else {
            context.vm.frames[frame_index + 1].set_exit_early(true);
        }

        context.vm.host_call_depth += 1;
        let result = context.run();
        context.vm.host_call_depth = context.vm.host_call_depth.saturating_sub(1);

        if matches!(result, crate::vm::CompletionRecord::Suspend) {
            let continuation = context
                .vm
                .suspended_resume
                .clone()
                .expect("suspended execution must install a continuation");
            return Ok(InterruptibleCallOutcome::Suspend(
                wrap_running_call_suspend(continuation),
            ));
        }

        context.vm.pop_frame().js_expect("frame must exist")?;

        Ok(InterruptibleCallOutcome::Complete(
            result
                .consume()?
                .as_object()
                .js_expect("should be an object")?
                .clone(),
        ))
    }

    /// Make the object [`sealed`][IntegrityLevel::Sealed] or [`frozen`][IntegrityLevel::Frozen].
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-setintegritylevel
    pub fn set_integrity_level(
        &self,
        level: IntegrityLevel,
        context: &mut Context,
    ) -> JsResult<bool> {
        // 1. Assert: Type(O) is Object.
        // 2. Assert: level is either sealed or frozen.

        // 3. Let status be ? O.[[PreventExtensions]]().
        let status =
            self.__prevent_extensions__(&mut InternalMethodPropertyContext::new(context))?;
        // 4. If status is false, return false.
        if !status {
            return Ok(false);
        }

        // 5. Let keys be ? O.[[OwnPropertyKeys]]().
        let keys = self.__own_property_keys__(&mut InternalMethodPropertyContext::new(context))?;

        match level {
            // 6. If level is sealed, then
            IntegrityLevel::Sealed => {
                // a. For each element k of keys, do
                for k in keys {
                    // i. Perform ? DefinePropertyOrThrow(O, k, PropertyDescriptor { [[Configurable]]: false }).
                    self.define_property_or_throw(
                        k,
                        PropertyDescriptor::builder().configurable(false).build(),
                        context,
                    )?;
                }
            }
            // 7. Else,
            //     a. Assert: level is frozen.
            IntegrityLevel::Frozen => {
                // b. For each element k of keys, do
                for k in keys {
                    // i. Let currentDesc be ? O.[[GetOwnProperty]](k).
                    let current_desc = self.__get_own_property__(
                        &k,
                        &mut InternalMethodPropertyContext::new(context),
                    )?;
                    // ii. If currentDesc is not undefined, then
                    if let Some(current_desc) = current_desc {
                        // 1. If IsAccessorDescriptor(currentDesc) is true, then
                        let desc = if current_desc.is_accessor_descriptor() {
                            // a. Let desc be the PropertyDescriptor { [[Configurable]]: false }.
                            PropertyDescriptor::builder().configurable(false).build()
                        // 2. Else,
                        } else {
                            // a. Let desc be the PropertyDescriptor { [[Configurable]]: false, [[Writable]]: false }.
                            PropertyDescriptor::builder()
                                .configurable(false)
                                .writable(false)
                                .build()
                        };
                        // 3. Perform ? DefinePropertyOrThrow(O, k, desc).
                        self.define_property_or_throw(k, desc, context)?;
                    }
                }
            }
        }

        // 8. Return true.
        Ok(true)
    }

    /// Check if the object is [`sealed`][IntegrityLevel::Sealed] or [`frozen`][IntegrityLevel::Frozen].
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-testintegritylevel
    pub fn test_integrity_level(
        &self,
        level: IntegrityLevel,
        context: &mut Context,
    ) -> JsResult<bool> {
        // 1. Assert: Type(O) is Object.
        // 2. Assert: level is either sealed or frozen.

        // 3. Let extensible be ? IsExtensible(O).
        let extensible = self.is_extensible(context)?;

        // 4. If extensible is true, return false.
        if extensible {
            return Ok(false);
        }

        // 5. NOTE: If the object is extensible, none of its properties are examined.
        // 6. Let keys be ? O.[[OwnPropertyKeys]]().
        let keys = self.__own_property_keys__(&mut InternalMethodPropertyContext::new(context))?;

        // 7. For each element k of keys, do
        for k in keys {
            // a. Let currentDesc be ? O.[[GetOwnProperty]](k).
            let current_desc =
                self.__get_own_property__(&k, &mut InternalMethodPropertyContext::new(context))?;
            // b. If currentDesc is not undefined, then
            if let Some(current_desc) = current_desc {
                // i. If currentDesc.[[Configurable]] is true, return false.
                if current_desc.expect_configurable() {
                    return Ok(false);
                }
                // ii. If level is frozen and IsDataDescriptor(currentDesc) is true, then
                if level.is_frozen() && current_desc.is_data_descriptor() {
                    // 1. If currentDesc.[[Writable]] is true, return false.
                    if current_desc.expect_writable() {
                        return Ok(false);
                    }
                }
            }
        }
        // 8. Return true.
        Ok(true)
    }

    /// Abstract operation [`LengthOfArrayLike ( obj )`][spec].
    ///
    /// Returns the value of the "length" property of an array-like object.
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-lengthofarraylike
    pub(crate) fn length_of_array_like(&self, context: &mut Context) -> JsResult<u64> {
        // 1. Assert: Type(obj) is Object.

        // NOTE: This is an optimization, most of the cases that `LengthOfArrayLike` will be called
        //       is for arrays. The "length" property of an array is stored in the first index.
        if self.is_array() {
            let borrowed_object = self.borrow();
            // NOTE: using `to_u32` instead of `to_length` is an optimization,
            //       since arrays are limited to [0, 2^32 - 1] range.
            return borrowed_object.properties().storage[0]
                .to_u32(context)
                .map(u64::from);
        }

        // 2. Return ℝ(? ToLength(? Get(obj, "length"))).
        self.get(StaticJsStrings::LENGTH, context)?
            .to_length(context)
    }

    /// `7.3.22 SpeciesConstructor ( O, defaultConstructor )`
    ///
    /// The abstract operation `SpeciesConstructor` takes arguments `O` (an Object) and
    /// `defaultConstructor` (a constructor). It is used to retrieve the constructor that should be
    /// used to create new objects that are derived from `O`. `defaultConstructor` is the
    /// constructor to use if a constructor `@@species` property cannot be found starting from `O`.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-speciesconstructor
    pub(crate) fn species_constructor<F>(
        &self,
        default_constructor: F,
        context: &mut Context,
    ) -> JsResult<Self>
    where
        F: FnOnce(&StandardConstructors) -> &StandardConstructor,
    {
        // 1. Assert: Type(O) is Object.

        // 2. Let C be ? Get(O, "constructor").
        let c = self.get(CONSTRUCTOR, context)?;

        // 3. If C is undefined, return defaultConstructor.
        if c.is_undefined() {
            return Ok(default_constructor(context.intrinsics().constructors()).constructor());
        }

        // 4. If Type(C) is not Object, throw a TypeError exception.
        let c = c.as_object().ok_or_else(|| {
            JsNativeError::typ().with_message("property 'constructor' is not an object")
        })?;

        // 5. Let S be ? Get(C, @@species).
        let s = c.get(JsSymbol::species(), context)?;

        // 6. If S is either undefined or null, return defaultConstructor.
        if s.is_null_or_undefined() {
            return Ok(default_constructor(context.intrinsics().constructors()).constructor());
        }

        // 7. If IsConstructor(S) is true, return S.
        if let Some(s) = s.as_constructor() {
            return Ok(s.clone());
        }

        // 8. Throw a TypeError exception.
        Err(JsNativeError::typ()
            .with_message("property 'constructor' is not a constructor")
            .into())
    }

    /// It is used to iterate over names of object's keys.
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-enumerableownpropertynames
    pub(crate) fn enumerable_own_property_names(
        &self,
        kind: PropertyNameKind,
        context: &mut Context,
    ) -> JsResult<Vec<JsValue>> {
        // 1. Assert: Type(O) is Object.
        // 2. Let ownKeys be ? O.[[OwnPropertyKeys]]().
        let own_keys =
            self.__own_property_keys__(&mut InternalMethodPropertyContext::new(context))?;
        // 3. Let properties be a new empty List.
        let mut properties = Vec::with_capacity(own_keys.len());

        // 4. For each element key of ownKeys, do
        for key in own_keys {
            // a. If Type(key) is String, then
            let key_str = match &key {
                PropertyKey::String(s) => Some(s.clone()),
                PropertyKey::Index(i) => Some(i.get().to_string().into()),
                PropertyKey::Symbol(_) => None,
            };

            if let Some(key_str) = key_str {
                // i. Let desc be ? O.[[GetOwnProperty]](key).
                let desc = self
                    .__get_own_property__(&key, &mut InternalMethodPropertyContext::new(context))?;
                // ii. If desc is not undefined and desc.[[Enumerable]] is true, then
                if let Some(desc) = desc
                    && desc.expect_enumerable()
                {
                    match kind {
                        // 1. If kind is key, append key to properties.
                        PropertyNameKind::Key => properties.push(key_str.into()),
                        // 2. Else,
                        // a. Let value be ? Get(O, key).
                        // b. If kind is value, append value to properties.
                        PropertyNameKind::Value => {
                            properties.push(self.get(key.clone(), context)?);
                        }
                        // c. Else,
                        // i. Assert: kind is key+value.
                        // ii. Let entry be ! CreateArrayFromList(« key, value »).
                        // iii. Append entry to properties.
                        PropertyNameKind::KeyAndValue => properties.push(
                            Array::create_array_from_list(
                                [key_str.into(), self.get(key.clone(), context)?],
                                context,
                            )?
                            .into(),
                        ),
                    }
                }
            }
        }

        // 5. Return properties.
        Ok(properties)
    }

    /// Abstract operation `GetMethod ( V, P )`
    ///
    /// Retrieves the value of a specific property, when the value of the property is expected to be a function.
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-getmethod
    pub(crate) fn get_method<K>(&self, key: K, context: &mut Context) -> JsResult<Option<Self>>
    where
        K: Into<PropertyKey>,
    {
        // Note: The spec specifies this function for JsValue.
        // It is implemented for JsObject for convenience.

        // 1. Assert: IsPropertyKey(P) is true.
        // 2. Let func be ? GetV(V, P).

        match self
            .__get__(
                &key.into(),
                self.clone().into(),
                &mut InternalMethodPropertyContext::new(context),
            )?
            .variant()
        {
            // 3. If func is either undefined or null, return undefined.
            JsVariant::Undefined | JsVariant::Null => Ok(None),
            // 5. Return func.
            JsVariant::Object(obj) if obj.is_callable() => Ok(Some(obj.clone())),
            // 4. If IsCallable(func) is false, throw a TypeError exception.
            _ => Err(JsNativeError::typ()
                .with_message("value returned for property of object is not a function")
                .into()),
        }
    }

    /// Abstract operation `IsArray ( argument )`
    ///
    /// Check if a value is an array.
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-isarray
    pub(crate) fn is_array_abstract(&self) -> JsResult<bool> {
        // Note: The spec specifies this function for JsValue.
        // It is implemented for JsObject for convenience.

        // 2. If argument is an Array exotic object, return true.
        if self.is_array() {
            return Ok(true);
        }

        // 3. If argument is a Proxy exotic object, then
        if let Some(proxy) = self.downcast_ref::<Proxy>() {
            // a. If argument.[[ProxyHandler]] is null, throw a TypeError exception.
            // b. Let target be argument.[[ProxyTarget]].
            let (target, _) = proxy.try_data()?;

            // c. Return ? IsArray(target).
            return target.is_array_abstract();
        }

        // 4. Return false.
        Ok(false)
    }

    /// Abstract operation [`GetFunctionRealm`][spec].
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-getfunctionrealm
    pub(crate) fn get_function_realm(&self, context: &mut Context) -> JsResult<Realm> {
        if let Some(fun) = self.downcast_ref::<OrdinaryFunction>() {
            return Ok(fun.realm().clone());
        }

        if let Some(f) = self.downcast_ref::<NativeFunctionObject>() {
            return Ok(f.realm.clone().unwrap_or_else(|| context.realm().clone()));
        }

        if let Some(bound) = self.downcast_ref::<BoundFunction>() {
            let fun = bound.target_function().clone();
            return fun.get_function_realm(context);
        }

        if let Some(proxy) = self.downcast_ref::<Proxy>() {
            let (fun, _) = proxy.try_data()?;
            return fun.get_function_realm(context);
        }

        Ok(context.realm().clone())
    }

    /// `7.3.26 CopyDataProperties ( target, source, excludedItems )`
    ///
    /// More information:
    ///  - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-copydataproperties
    pub fn copy_data_properties<K>(
        &self,
        source: &JsValue,
        excluded_keys: Vec<K>,
        context: &mut Context,
    ) -> JsResult<()>
    where
        K: Into<PropertyKey>,
    {
        let context = &mut InternalMethodPropertyContext::new(context);

        // 1. Assert: Type(target) is Object.
        // 2. Assert: excludedItems is a List of property keys.
        // 3. If source is undefined or null, return target.
        if source.is_null_or_undefined() {
            return Ok(());
        }

        // 4. Let from be ! ToObject(source).
        let from = source
            .to_object(context)
            .expect("function ToObject should never complete abruptly here");

        // 5. Let keys be ? from.[[OwnPropertyKeys]]().
        // 6. For each element nextKey of keys, do
        let excluded_keys: Vec<PropertyKey> = excluded_keys.into_iter().map(Into::into).collect();
        for key in from.__own_property_keys__(context)? {
            // a. Let excluded be false.
            let mut excluded = false;

            // b. For each element e of excludedItems, do
            for e in &excluded_keys {
                // i. If SameValue(e, nextKey) is true, then
                if *e == key {
                    // 1. Set excluded to true.
                    excluded = true;
                    break;
                }
            }
            // c. If excluded is false, then
            if !excluded {
                // i. Let desc be ? from.[[GetOwnProperty]](nextKey).
                let desc = from.__get_own_property__(&key, context)?;

                // ii. If desc is not undefined and desc.[[Enumerable]] is true, then
                if let Some(desc) = desc
                    && let Some(enumerable) = desc.enumerable()
                    && enumerable
                {
                    // 1. Let propValue be ? Get(from, nextKey).
                    let prop_value = from.__get__(&key, from.clone().into(), context)?;

                    // 2. Perform ! CreateDataPropertyOrThrow(target, nextKey, propValue).
                    self.create_data_property_or_throw(key, prop_value, context)
                        .expect("CreateDataPropertyOrThrow should never complete abruptly here");
                }
            }
        }

        // 7. Return target.
        Ok(())
    }

    /// Abstract operation `PrivateElementFind ( O, P )`
    ///
    /// Get the private element from an object.
    ///
    /// More information:
    ///  - [ECMAScript specification][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-privateelementfind
    #[allow(clippy::similar_names)]
    pub(crate) fn private_element_find(
        &self,
        name: &PrivateName,
        is_getter: bool,
        is_setter: bool,
    ) -> Option<PrivateElement> {
        // 1. If O.[[PrivateElements]] contains a PrivateElement whose [[Key]] is P, then
        for (key, value) in &self.borrow().private_elements {
            if key == name {
                // a. Let entry be that PrivateElement.
                // b. Return entry.
                if let PrivateElement::Accessor { getter, setter } = value {
                    if getter.is_some() && is_getter || setter.is_some() && is_setter {
                        return Some(value.clone());
                    }
                } else {
                    return Some(value.clone());
                }
            }
        }

        // 2. Return empty.
        None
    }

    /// Abstract operation `PrivateFieldAdd ( O, P, value )`
    ///
    /// Add private field to an object.
    ///
    /// More information:
    ///  - [ECMAScript specification][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-privatefieldadd
    pub(crate) fn private_field_add(
        &self,
        name: &PrivateName,
        value: JsValue,
        context: &mut Context,
    ) -> JsResult<()> {
        // 1. If the host is a web browser, then
        //    a. Perform ? HostEnsureCanAddPrivateElement(O).
        context
            .host_hooks()
            .ensure_can_add_private_element(self, context)?;

        // 2. If ? IsExtensible(O) is false, throw a TypeError exception.
        // NOTE: From <https://tc39.es/proposal-nonextensible-applies-to-private/#sec-privatefieldadd>
        if !self.is_extensible(context)? {
            return Err(js_error!(
                TypeError: "cannot add private field to non-extensible class instance"
            ));
        }

        // 3. Let entry be PrivateElementFind(O, P).
        let entry = self.private_element_find(name, false, false);

        // 4. If entry is not empty, throw a TypeError exception.
        if entry.is_some() {
            return Err(js_error!(TypeError: "private field already exists on prototype"));
        }

        // 5. Append PrivateElement { [[Key]]: P, [[Kind]]: field, [[Value]]: value } to O.[[PrivateElements]].
        self.borrow_mut()
            .private_elements
            .push((name.clone(), PrivateElement::Field(value)));

        // 5. Return unused.
        Ok(())
    }

    /// Abstract operation `PrivateMethodOrAccessorAdd ( O, method )`
    ///
    /// Add private method or accessor to an object.
    ///
    /// More information:
    ///  - [ECMAScript specification][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-privatemethodoraccessoradd
    pub(crate) fn private_method_or_accessor_add(
        &self,
        name: &PrivateName,
        method: &PrivateElement,
        context: &mut Context,
    ) -> JsResult<()> {
        // 1. Assert: method.[[Kind]] is either method or accessor.
        assert!(matches!(
            method,
            PrivateElement::Method(_) | PrivateElement::Accessor { .. }
        ));

        let (getter, setter) = if let PrivateElement::Accessor { getter, setter } = method {
            (getter.is_some(), setter.is_some())
        } else {
            (false, false)
        };

        // 2. If the host is a web browser, then
        // a. Perform ? HostEnsureCanAddPrivateElement(O).
        context
            .host_hooks()
            .ensure_can_add_private_element(self, context)?;

        // 3. If ? IsExtensible(O) is false, throw a TypeError exception.
        // NOTE: From <https://tc39.es/proposal-nonextensible-applies-to-private/#sec-privatemethodoraccessoradd>
        if !self.is_extensible(context)? {
            return if getter || setter {
                Err(js_error!(
                    TypeError: "cannot add private accessor to non-extensible class instance"
                ))
            } else {
                Err(js_error!(
                    TypeError: "cannot add private method to non-extensible class instance"
                ))
            };
        }

        // 3. Let entry be PrivateElementFind(O, method.[[Key]]).
        let entry = self.private_element_find(name, getter, setter);

        // 4. If entry is not empty, throw a TypeError exception.
        if entry.is_some() {
            return if getter || setter {
                Err(js_error!(
                    TypeError: "private accessor already exists on class instance"
                ))
            } else {
                Err(js_error!(
                    TypeError: "private method already exists on class instance"
                ))
            };
        }

        // 5. Append method to O.[[PrivateElements]].
        self.borrow_mut()
            .append_private_element(name.clone(), method.clone());

        // 6. Return unused.
        Ok(())
    }

    /// Abstract operation `PrivateGet ( O, P )`
    ///
    /// Get the value of a private element.
    ///
    /// More information:
    ///  - [ECMAScript specification][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-privateget
    pub(crate) fn private_get(
        &self,
        name: &PrivateName,
        context: &mut Context,
    ) -> JsResult<JsValue> {
        // 1. Let entry be PrivateElementFind(O, P).
        let entry = self.private_element_find(name, true, true);

        match &entry {
            // 2. If entry is empty, throw a TypeError exception.
            None => Err(JsNativeError::typ()
                .with_message("Private element does not exist on object")
                .into()),

            // 3. If entry.[[Kind]] is field or method, then
            // a. Return entry.[[Value]].
            Some(PrivateElement::Field(value)) => Ok(value.clone()),
            Some(PrivateElement::Method(value)) => Ok(value.clone().into()),

            // 4. Assert: entry.[[Kind]] is accessor.
            Some(PrivateElement::Accessor { getter, .. }) => {
                // 5. If entry.[[Get]] is undefined, throw a TypeError exception.
                // 6. Let getter be entry.[[Get]].
                let getter = getter.as_ref().ok_or_else(|| {
                    JsNativeError::typ()
                        .with_message("private property was defined without a getter")
                })?;

                // 7. Return ? Call(getter, O).
                getter.call(&self.clone().into(), &[], context)
            }
        }
    }

    /// Abstract operation `PrivateSet ( O, P, value )`
    ///
    /// Set the value of a private element.
    ///
    /// More information:
    ///  - [ECMAScript specification][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-privateset
    pub(crate) fn private_set(
        &self,
        name: &PrivateName,
        value: JsValue,
        context: &mut Context,
    ) -> JsResult<()> {
        // 1. Let entry be PrivateElementFind(O, P).
        // Note: This function is inlined here for mutable access.
        let mut object_mut = self.borrow_mut();
        let entry = object_mut
            .private_elements
            .iter_mut()
            .find_map(|(key, value)| if key == name { Some(value) } else { None });

        match entry {
            // 2. If entry is empty, throw a TypeError exception.
            None => {
                return Err(JsNativeError::typ()
                    .with_message("Private element does not exist on object")
                    .into());
            }

            // 3. If entry.[[Kind]] is field, then
            // a. Set entry.[[Value]] to value.
            Some(PrivateElement::Field(field)) => {
                *field = value;
            }

            // 4. Else if entry.[[Kind]] is method, then
            // a. Throw a TypeError exception.
            Some(PrivateElement::Method(_)) => {
                return Err(JsNativeError::typ()
                    .with_message("private method is not writable")
                    .into());
            }

            // 5. Else,
            Some(PrivateElement::Accessor { setter, .. }) => {
                // a. Assert: entry.[[Kind]] is accessor.
                // b. If entry.[[Set]] is undefined, throw a TypeError exception.
                // c. Let setter be entry.[[Set]].
                let setter = setter.clone().ok_or_else(|| {
                    JsNativeError::typ()
                        .with_message("private property was defined without a setter")
                })?;

                // d. Perform ? Call(setter, O, « value »).
                drop(object_mut);
                setter.call(&self.clone().into(), &[value], context)?;
            }
        }

        // 6. Return unused.
        Ok(())
    }

    /// Abstract operation `DefineField ( receiver, fieldRecord )`
    ///
    /// Define a field on an object.
    ///
    /// More information:
    ///  - [ECMAScript specification][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-definefield
    pub(crate) fn define_field(
        &self,
        field_record: &ClassFieldDefinition,
        context: &mut Context,
    ) -> JsResult<()> {
        match self.define_field_interruptible(field_record, context)? {
            ExecutionOutcome::Complete(()) => Ok(()),
            ExecutionOutcome::Suspend(_) => Err(JsNativeError::error()
                .with_message("define field suspended during synchronous execution")
                .into()),
        }
    }

    pub(crate) fn define_field_interruptible(
        &self,
        field_record: &ClassFieldDefinition,
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<()>> {
        let initializer = match field_record {
            ClassFieldDefinition::Public(_, function, _)
            | ClassFieldDefinition::Private(_, function) => function,
        };

        match initializer.call_interruptible(&self.clone().into(), &[], context)? {
            InterruptibleCallOutcome::Complete(init_value) => {
                finish_define_field(self, field_record, init_value, context)?;
                Ok(ExecutionOutcome::Complete(()))
            }
            InterruptibleCallOutcome::Suspend(continuation) => Ok(ExecutionOutcome::Suspend(
                wrap_define_field_suspend(DefineFieldResume {
                    continuation,
                    receiver: self.clone(),
                    field_record: field_record.clone(),
                }),
            )),
        }
    }

    /// Abstract operation `InitializeInstanceElements ( O, constructor )`
    ///
    /// Add private methods and fields from a class constructor to an object.
    ///
    /// More information:
    ///  - [ECMAScript specification][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-initializeinstanceelements
    pub(crate) fn initialize_instance_elements(
        &self,
        constructor: &Self,
        context: &mut Context,
    ) -> JsResult<()> {
        match self.initialize_instance_elements_interruptible(constructor, context)? {
            ExecutionOutcome::Complete(()) => Ok(()),
            ExecutionOutcome::Suspend(_) => Err(JsNativeError::error()
                .with_message("initialize instance elements suspended during synchronous execution")
                .into()),
        }
    }

    pub(crate) fn initialize_instance_elements_interruptible(
        &self,
        constructor: &Self,
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<()>> {
        let constructor_function = constructor
            .downcast_ref::<OrdinaryFunction>()
            .js_expect("class constructor must be function object")?;

        // 1. Let methods be the value of constructor.[[PrivateMethods]].
        // 2. For each PrivateElement method of methods, do
        for (name, method) in constructor_function.get_private_methods() {
            // a. Perform ? PrivateMethodOrAccessorAdd(O, method).
            self.private_method_or_accessor_add(name, method, context)?;
        }

        continue_initialize_instance_elements_from_index(self, constructor, 0, context)
    }

    /// Abstract operation `Invoke ( V, P [ , argumentsList ] )`
    ///
    /// Calls a method property of an ECMAScript object.
    ///
    /// Equivalent to the [`JsValue::invoke`] method, but specialized for objects.
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-invoke
    pub(crate) fn invoke<K>(
        &self,
        key: K,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<JsValue>
    where
        K: Into<PropertyKey>,
    {
        let this_value: JsValue = self.clone().into();

        // 1. If argumentsList is not present, set argumentsList to a new empty List.
        // 2. Let func be ? GetV(V, P).
        let func = self.__get__(
            &key.into(),
            this_value.clone(),
            &mut InternalMethodPropertyContext::new(context),
        )?;

        // 3. Return ? Call(func, V, argumentsList)
        func.call(&this_value, args, context)
    }

    pub(crate) fn get_method_interruptible<K>(
        &self,
        key: K,
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<Option<JsObject>>>
    where
        K: Into<PropertyKey>,
    {
        JsValue::from(self.clone()).get_method_interruptible(key, context)
    }

    pub(crate) fn invoke_interruptible<K>(
        &self,
        key: K,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<JsValue>>
    where
        K: Into<PropertyKey>,
    {
        let this_value: JsValue = self.clone().into();
        let func = match self.__get_interruptible__(
            &key.into(),
            this_value.clone(),
            &mut InternalMethodPropertyContext::new(context),
        )? {
            ExecutionOutcome::Complete(func) => func,
            ExecutionOutcome::Suspend(continuation) => {
                return Ok(ExecutionOutcome::Suspend(continuation));
            }
        };

        let object = func.as_object().ok_or_else(|| {
            JsNativeError::typ().with_message("value returned for property of object is not a function")
        })?;
        match object.call_interruptible(&this_value, args, context)? {
            InterruptibleCallOutcome::Complete(value) => Ok(ExecutionOutcome::Complete(value)),
            InterruptibleCallOutcome::Suspend(continuation) => {
                Ok(ExecutionOutcome::Suspend(continuation))
            }
        }
    }
}

impl JsValue {
    /// Abstract operation `GetV ( V, P )`.
    ///
    /// Retrieves the value of a specific property of an ECMAScript language value. If the value is
    /// not an object, the property lookup is performed using a wrapper object appropriate for the
    /// type of the value.
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-getv
    pub(crate) fn get_v<K>(&self, key: K, context: &mut Context) -> JsResult<Self>
    where
        K: Into<PropertyKey>,
    {
        // 1. Let O be ? ToObject(V).
        let o = self.to_object(context)?;

        // 2. Return ? O.[[Get]](P, V).

        o.__get__(
            &key.into(),
            self.clone(),
            &mut InternalMethodPropertyContext::new(context),
        )
    }

    pub(crate) fn get_v_interruptible<K>(
        &self,
        key: K,
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<Self>>
    where
        K: Into<PropertyKey>,
    {
        let o = self.to_object(context)?;

        o.__get_interruptible__(
            &key.into(),
            self.clone(),
            &mut InternalMethodPropertyContext::new(context),
        )
    }

    /// Abstract operation `GetMethod ( V, P )`
    ///
    /// Retrieves the value of a specific property, when the value of the property is expected to be a function.
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-getmethod
    pub(crate) fn get_method<K>(&self, key: K, context: &mut Context) -> JsResult<Option<JsObject>>
    where
        K: Into<PropertyKey>,
    {
        // Note: The spec specifies this function for JsValue.
        // The main part of the function is implemented for JsObject.
        // 1. Let func be ? GetV(V, P).
        match self.get_v(key, context)?.variant() {
            // 3. If func is either undefined or null, return undefined.
            JsVariant::Undefined | JsVariant::Null => Ok(None),
            // 5. Return func.
            JsVariant::Object(obj) if obj.is_callable() => Ok(Some(obj.clone())),
            // 4. If IsCallable(func) is false, throw a TypeError exception.
            _ => Err(JsNativeError::typ()
                .with_message("value returned for property of object is not a function")
                .into()),
        }
    }

    pub(crate) fn get_method_interruptible<K>(
        &self,
        key: K,
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<Option<JsObject>>>
    where
        K: Into<PropertyKey>,
    {
        match self.get_v_interruptible(key, context)? {
            ExecutionOutcome::Complete(func) => match func.variant() {
                JsVariant::Undefined | JsVariant::Null => Ok(ExecutionOutcome::Complete(None)),
                JsVariant::Object(obj) if obj.is_callable() => {
                    Ok(ExecutionOutcome::Complete(Some(obj.clone())))
                }
                _ => Err(JsNativeError::typ()
                    .with_message("value returned for property of object is not a function")
                    .into()),
            },
            ExecutionOutcome::Suspend(continuation) => {
                Ok(ExecutionOutcome::Suspend(wrap_get_method_resume(continuation)))
            }
        }
    }

    /// It is used to create List value whose elements are provided by the indexed properties of
    /// self.
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-createlistfromarraylike
    pub(crate) fn create_list_from_array_like(
        &self,
        element_types: &[Type],
        context: &mut Context,
    ) -> JsResult<Vec<Self>> {
        // 1. If elementTypes is not present, set elementTypes to « Undefined, Null, Boolean, String, Symbol, Number, BigInt, Object ».
        let types = if element_types.is_empty() {
            &[
                Type::Undefined,
                Type::Null,
                Type::Boolean,
                Type::String,
                Type::Symbol,
                Type::Number,
                Type::BigInt,
                Type::Object,
            ]
        } else {
            element_types
        };

        // 2. If Type(obj) is not Object, throw a TypeError exception.
        let obj = self.as_object().ok_or_else(|| {
            JsNativeError::typ().with_message("cannot create list from a primitive")
        })?;

        // 3. Let len be ? LengthOfArrayLike(obj).
        let len = obj.length_of_array_like(context)?;

        // 4. Let list be a new empty List.
        let mut list = Vec::with_capacity(len as usize);

        // 5. Let index be 0.
        // 6. Repeat, while index < len,
        for index in 0..len {
            // a. Let indexName be ! ToString(𝔽(index)).
            // b. Let next be ? Get(obj, indexName).
            let next = obj.get(index, context)?;
            // c. If Type(next) is not an element of elementTypes, throw a TypeError exception.
            if !types.contains(&next.get_type()) {
                return Err(JsNativeError::typ().with_message("bad type").into());
            }
            // d. Append next as the last element of list.
            list.push(next.clone());
            // e. Set index to index + 1.
        }

        // 7. Return list.
        Ok(list)
    }

    /// Abstract operation [`Call ( F, V [ , argumentsList ] )`][call].
    ///
    /// Calls this value if the value is a callable object.
    ///
    /// # Note
    ///
    /// It is almost always better to try to obtain a callable object first with [`JsValue::as_callable`],
    /// then calling [`JsObject::call`], since that allows reusing the unwrapped function for other
    /// operations. This method is only an utility method for when the spec directly uses `Call`
    /// without using the value as a proper object.
    ///
    /// [call]: https://tc39.es/ecma262/#sec-call
    #[inline]
    #[cfg_attr(feature = "native-backtrace", track_caller)]
    pub(crate) fn call(&self, this: &Self, args: &[Self], context: &mut Context) -> JsResult<Self> {
        let Some(object) = self.as_object() else {
            return Err(JsNativeError::typ()
                .with_message(format!(
                    "value with type `{}` is not callable",
                    self.type_of()
                ))
                .into());
        };

        object.call(this, args, context)
    }

    #[inline]
    pub(crate) fn call_interruptible(
        &self,
        this: &Self,
        args: &[Self],
        context: &mut Context,
    ) -> JsResult<InterruptibleCallOutcome<Self>> {
        let Some(object) = self.as_object() else {
            return Err(JsNativeError::typ()
                .with_message(format!(
                    "value with type `{}` is not callable",
                    self.type_of()
                ))
                .into());
        };

        object.call_interruptible(this, args, context)
    }

    /// Abstract operation `( V, P [ , argumentsList ] )`
    ///
    /// Calls a method property of an ECMAScript language value.
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-invoke
    #[cfg_attr(feature = "native-backtrace", track_caller)]
    pub(crate) fn invoke<K>(&self, key: K, args: &[Self], context: &mut Context) -> JsResult<Self>
    where
        K: Into<PropertyKey>,
    {
        // 1. If argumentsList is not present, set argumentsList to a new empty List.
        // 2. Let func be ? GetV(V, P).
        let func = self.get_v(key, context)?;

        // 3. Return ? Call(func, V, argumentsList)
        func.call(self, args, context)
    }

    pub(crate) fn invoke_interruptible<K>(
        &self,
        key: K,
        args: &[Self],
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<Self>>
    where
        K: Into<PropertyKey>,
    {
        match self.get_v_interruptible(key, context)? {
            ExecutionOutcome::Complete(func) => {
                Ok(self.call_interruptible_from_value(func, args, context)?)
            }
            ExecutionOutcome::Suspend(continuation) => Ok(ExecutionOutcome::Suspend(
                wrap_value_invoke_resume(ValueInvokeResume {
                    continuation,
                    this_value: self.clone(),
                    args: args.to_vec(),
                }),
            )),
        }
    }

    fn call_interruptible_from_value(
        &self,
        func: JsValue,
        args: &[Self],
        context: &mut Context,
    ) -> JsResult<ExecutionOutcome<Self>> {
        let Some(object) = func.as_object() else {
            return Err(JsNativeError::typ()
                .with_message("value returned for property of object is not a function")
                .into());
        };

        match object.call_interruptible(self, args, context)? {
            InterruptibleCallOutcome::Complete(value) => Ok(ExecutionOutcome::Complete(value)),
            InterruptibleCallOutcome::Suspend(continuation) => {
                Ok(ExecutionOutcome::Suspend(continuation))
            }
        }
    }

    /// Abstract operation `OrdinaryHasInstance ( C, O )`
    ///
    /// More information:
    /// - [ECMAScript reference][spec]
    ///
    /// [spec]: https://tc39.es/ecma262/#sec-ordinaryhasinstance
    pub fn ordinary_has_instance(
        function: &Self,
        object: &Self,
        context: &mut Context,
    ) -> JsResult<bool> {
        // 1. If IsCallable(C) is false, return false.
        let Some(function) = function.as_callable() else {
            return Ok(false);
        };

        // 2. If C has a [[BoundTargetFunction]] internal slot, then
        if let Some(bound_function) = function.downcast_ref::<BoundFunction>() {
            // a. Let BC be C.[[BoundTargetFunction]].
            // b. Return ? InstanceofOperator(O, BC).
            return object.instance_of(&bound_function.target_function().clone().into(), context);
        }

        let Some(mut object) = object.as_object() else {
            // 3. If Type(O) is not Object, return false.
            return Ok(false);
        };

        // 4. Let P be ? Get(C, "prototype").
        let prototype = function.get(PROTOTYPE, context)?;

        // 5. If Type(P) is not Object, throw a TypeError exception.
        let prototype = prototype.as_object().ok_or_else(|| {
            JsNativeError::typ()
                .with_message("function has non-object prototype in instanceof check")
        })?;

        // 6. Repeat,
        loop {
            // a. Set O to ? O.[[GetPrototypeOf]]().
            object = match object
                .__get_prototype_of__(&mut InternalMethodPropertyContext::new(context))?
            {
                Some(obj) => obj,
                // b. If O is null, return false.
                None => return Ok(false),
            };

            // c. If SameValue(P, O) is true, return true.
            if JsObject::equals(&object, &prototype) {
                return Ok(true);
            }
        }
    }
}
