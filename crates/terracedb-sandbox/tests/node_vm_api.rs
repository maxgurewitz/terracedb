#[path = "support/node_compat.rs"]
mod node_compat;

#[tokio::test]
async fn node_vm_run_in_this_context_matches_real_node() {
    let source = r#"
const vm = require('node:vm');
globalThis.__terraceVmValue = undefined;
const result = vm.runInThisContext(
  'globalThis.__terraceVmValue = "ok"; Object.prototype.toString.call(process);',
  'terrace-vm-run.js'
);
console.log(JSON.stringify({
  result,
  global: globalThis.__terraceVmValue,
}));
delete globalThis.__terraceVmValue;
"#;

    let sandbox = node_compat::exec_node_fixture(source)
        .await
        .expect("sandbox vm.runInThisContext");
    let real = node_compat::exec_real_node_eval(source).expect("real node vm.runInThisContext");

    let sandbox_stdout = sandbox.result.expect("sandbox report")["stdout"]
        .as_str()
        .expect("sandbox stdout")
        .trim()
        .to_string();
    assert_eq!(sandbox_stdout, real.stdout.trim());
}

#[tokio::test]
async fn node_vm_script_run_in_this_context_matches_real_node() {
    let source = r#"
const vm = require('node:vm');
const script = new vm.Script('(function (left, right) { return left + right; })', {
  filename: 'terrace-vm-script.js',
});
const fn = script.runInThisContext();
console.log(String(fn(20, 22)));
"#;

    let sandbox = node_compat::exec_node_fixture(source)
        .await
        .expect("sandbox vm.Script");
    let real = node_compat::exec_real_node_eval(source).expect("real node vm.Script");

    let sandbox_stdout = sandbox.result.expect("sandbox report")["stdout"]
        .as_str()
        .expect("sandbox stdout")
        .trim()
        .to_string();
    assert_eq!(sandbox_stdout, real.stdout.trim());
}

#[tokio::test]
async fn node_vm_run_in_this_context_preserves_filename_in_stack() {
    let source = r#"
const vm = require('node:vm');
try {
  vm.runInThisContext('throw new Error("boom")', 'terrace-vm-stack.js');
} catch (error) {
  console.log(String(error.stack).split('\n')[0]);
}
"#;

    let sandbox = node_compat::exec_node_fixture(source)
        .await
        .expect("sandbox vm stack");
    let real = node_compat::exec_real_node_eval(source).expect("real node vm stack");

    let sandbox_stdout = sandbox.result.expect("sandbox report")["stdout"]
        .as_str()
        .expect("sandbox stdout")
        .trim()
        .to_string();
    assert_eq!(sandbox_stdout, real.stdout.trim());
}
