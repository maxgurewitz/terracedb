use boa_engine::{
    Context, JsError, JsResult, JsValue, NativeFunction, Source, js_string,
    native_function::NativeFunctionResult,
    object::InterruptibleCallOutcome,
    script::Script,
};
use indoc::indoc;

fn suspend_once(
    _this: &JsValue,
    args: &[JsValue],
    _context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    let value = args.first().cloned().unwrap_or_else(JsValue::undefined);
    Ok(NativeFunctionResult::suspend_with_copy_closure(
        resume_suspended_value,
        value,
    ))
}

fn resume_suspended_value(
    completion: Result<(), JsError>,
    value: &JsValue,
    _context: &mut Context,
) -> JsResult<JsValue> {
    completion?;
    Ok(value.clone())
}

fn install_suspend_once(context: &mut Context) {
    context
        .register_global_builtin_callable(
            js_string!("suspendOnce"),
            1,
            NativeFunction::from_suspend_fn_ptr(suspend_once),
        )
        .expect("failed to install suspendOnce");
}

fn eval_interruptible_to_completion(context: &mut Context, source: &str) -> JsResult<JsValue> {
    let script = Script::parse(Source::from_bytes(source), None, context)?;
    let mut outcome = script.evaluate_interruptible(context)?;
    loop {
        match outcome {
            InterruptibleCallOutcome::Complete(value) => return Ok(value),
            InterruptibleCallOutcome::Suspend(continuation) => {
                context.install_interruptible_resume(continuation);
                outcome = context.resume_interruptible()?;
            }
        }
    }
}

fn eval_interruptible_with_error_on_first_suspend(
    context: &mut Context,
    source: &str,
    error: JsError,
) -> JsResult<JsValue> {
    let script = Script::parse(Source::from_bytes(source), None, context)?;
    let mut outcome = script.evaluate_interruptible(context)?;
    let mut resumed_with_error = false;
    loop {
        match outcome {
            InterruptibleCallOutcome::Complete(value) => return Ok(value),
            InterruptibleCallOutcome::Suspend(continuation) => {
                context.install_interruptible_resume(continuation);
                outcome = if resumed_with_error {
                    context.resume_interruptible()?
                } else {
                    resumed_with_error = true;
                    context.resume_interruptible_with_error(error.clone())?
                };
            }
        }
    }
}

#[test]
fn interruptible_function_apply_preserves_forwarding_suspend() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let value = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            function inner(value) {
                return suspendOnce(value) + 1;
            }
            Reflect.apply(inner, undefined, [41]);
        "#},
    )
    .expect("interruptible Reflect.apply should resume to completion");
    assert_eq!(value, JsValue::from(42));
}

#[test]
fn interruptible_accessor_getter_resumes_with_post_processing() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let value = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            function wrap(value) {
                return suspendOnce(value) + 1;
            }
            const obj = {
                get value() {
                    return wrap(41);
                }
            };
            obj.value;
        "#},
    )
    .expect("interruptible accessor getter should resume to completion");
    assert_eq!(value, JsValue::from(42));
}

#[test]
fn interruptible_accessor_setter_resumes_with_post_processing() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let value = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            let observed = 0;
            function wrap(value) {
                return suspendOnce(value) + 1;
            }
            const obj = {
                set value(next) {
                    observed = wrap(next);
                }
            };
            obj.value = 41;
            observed;
        "#},
    )
    .expect("interruptible accessor setter should resume to completion");
    assert_eq!(value, JsValue::from(42));
}

#[test]
fn interruptible_proxy_get_resumes_with_invariant_checks() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let value = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            const target = {};
            const proxy = new Proxy(target, {
                get(_target, key, _receiver) {
                    return suspendOnce(key === 'value' ? 41 : 0) + 1;
                }
            });
            proxy.value;
        "#},
    )
    .expect("interruptible proxy get should resume to completion");
    assert_eq!(value, JsValue::from(42));
}

#[test]
fn interruptible_proxy_has_resumes_with_invariant_checks() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let value = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            const target = { value: 1 };
            const proxy = new Proxy(target, {
                has(_target, key) {
                    return suspendOnce(key === 'value');
                }
            });
            'value' in proxy;
        "#},
    )
    .expect("interruptible proxy has should resume to completion");
    assert_eq!(value, JsValue::from(true));
}

#[test]
fn interruptible_proxy_has_enforces_invariants_after_resume() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let error = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            const target = {};
            Object.defineProperty(target, 'value', {
                configurable: false,
                value: 1
            });
            const proxy = new Proxy(target, {
                has(_target, key) {
                    return suspendOnce(key !== 'value');
                }
            });
            'value' in proxy;
        "#},
    )
    .expect_err("interruptible proxy has should enforce invariants after resume");
    assert!(
        error.to_string().contains("Proxy trap returned unexpected property"),
        "{error:?}"
    );
}

#[test]
fn interruptible_proxy_set_resumes_with_invariant_checks() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let value = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            let observed = 0;
            const target = {};
            const proxy = new Proxy(target, {
                set(_target, key, value, _receiver) {
                    observed = suspendOnce(value) + 1;
                    return key === 'value';
                }
            });
            proxy.value = 41;
            observed;
        "#},
    )
    .expect("interruptible proxy set should resume to completion");
    assert_eq!(value, JsValue::from(42));
}

#[test]
fn interruptible_proxy_get_enforces_invariants_after_resume() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let error = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            const target = {};
            Object.defineProperty(target, 'value', {
                configurable: false,
                writable: false,
                value: 1
            });
            const proxy = new Proxy(target, {
                get(_target, key, _receiver) {
                    return suspendOnce(key === 'value' ? 2 : 0);
                }
            });
            proxy.value;
        "#},
    )
    .expect_err("interruptible proxy get should enforce invariants after resume");
    assert!(
        error
            .to_string()
            .contains("Proxy trap returned unexpected data descriptor"),
        "{error:?}"
    );
}

#[test]
fn interruptible_proxy_set_enforces_invariants_after_resume() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let error = eval_interruptible_to_completion(
        &mut context,
        indoc! {r#"
            const target = {};
            Object.defineProperty(target, 'value', {
                configurable: false,
                writable: false,
                value: 1
            });
            const proxy = new Proxy(target, {
                set(_target, key, value, _receiver) {
                    return suspendOnce(key === 'value' && value === 2);
                }
            });
            proxy.value = 2;
        "#},
    )
    .expect_err("interruptible proxy set should enforce invariants after resume");
    assert!(
        error
            .to_string()
            .contains("Proxy trap set unexpected data descriptor"),
        "{error:?}"
    );
}

#[test]
fn interruptible_resume_with_error_is_catchable_in_js() {
    let mut context = Context::default();
    install_suspend_once(&mut context);
    let value = eval_interruptible_with_error_on_first_suspend(
        &mut context,
        indoc! {r#"
            try {
                suspendOnce(1);
                'unreachable';
            } catch (error) {
                error.message;
            }
        "#},
        boa_engine::JsNativeError::error()
            .with_message("boom")
            .into(),
    )
    .expect("resume_interruptible_with_error should throw into JS try/catch");
    assert_eq!(value, JsValue::from(js_string!("boom")));
}
