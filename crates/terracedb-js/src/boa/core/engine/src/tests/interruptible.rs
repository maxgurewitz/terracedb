use boa_macros::js_str;
use serde_json::json;

use crate::{
    Context, JsNativeError, JsResult, JsValue, NativeFunction, Script, Source,
    context::ExecutionOutcome,
    native_function::NativeFunctionResult,
    vm::CompletionRecord,
};

fn install_suspend_throw(context: &mut Context) {
    context
        .register_global_builtin_callable(
            js_str!("suspendThrow").into(),
            0,
            NativeFunction::from_suspend_fn_ptr(suspend_throw),
        )
        .expect("register suspendThrow");
}

fn suspend_throw(
    _this: &JsValue,
    _args: &[JsValue],
    _context: &mut Context,
) -> JsResult<NativeFunctionResult> {
    Ok(NativeFunctionResult::suspend_with_copy_closure(
        resume_suspend_throw,
        (),
    ))
}

fn resume_suspend_throw(
    _completion: CompletionRecord,
    _captures: &(),
    _context: &mut Context,
) -> JsResult<CompletionRecord> {
    Ok(CompletionRecord::Throw(
        JsNativeError::error().with_message("boom").into(),
    ))
}

fn evaluate_interruptible(
    source: &str,
    context: &mut Context,
) -> JsResult<JsValue> {
    let script = Script::parse(Source::from_bytes(source), None, context)?;
    let mut outcome = script.evaluate_interruptible(context)?;
    loop {
        match outcome.into_execution_outcome() {
            ExecutionOutcome::Complete(value) => return Ok(value),
            ExecutionOutcome::Suspend(_) => {
                outcome = context.resume_interruptible()?;
            }
        }
    }
}

#[test]
fn suspended_native_throw_surfaces_as_uncaught_error() {
    let context = &mut Context::default();
    install_suspend_throw(context);

    let error = evaluate_interruptible("suspendThrow()", context)
        .expect_err("suspendThrow should throw");
    let native = error.try_native(context).expect("native error");
    assert_eq!(native.kind(), JsNativeError::error().kind());
    assert_eq!(native.message(), "boom");
}

#[test]
fn suspended_native_throw_is_caught_by_try_catch_and_finally() {
    let context = &mut Context::default();
    install_suspend_throw(context);

    let value = evaluate_interruptible(
        r#"
        let caught = '';
        let finallyRan = false;
        try {
          suspendThrow();
        } catch (error) {
          caught = error.message;
        } finally {
          finallyRan = true;
        }
        [caught, finallyRan];
        "#,
        context,
    )
    .expect("suspended throw should be catchable");

    assert_eq!(
        value.to_json(context).expect("json conversion"),
        Some(json!(["boom", true]))
    );
}

#[test]
fn suspended_native_throw_is_caught_through_reflect_apply() {
    let context = &mut Context::default();
    install_suspend_throw(context);

    let value = evaluate_interruptible(
        r#"
        let caught = '';
        try {
          Reflect.apply(suspendThrow, undefined, []);
        } catch (error) {
          caught = error.message;
        }
        caught;
        "#,
        context,
    )
    .expect("Reflect.apply should preserve catchable suspended throws");

    assert_eq!(value, js_str!("boom").into());
}
