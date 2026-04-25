use crate::Error;

use super::{
    BytecodeProgram, EnvStack, GcPolicy, Instr, JsHeap, JsValue, ObjectId, ObjectKind, SymbolTable,
};

pub struct Vm {
    ip: usize,
    stack: Vec<JsValue>,
    env: EnvStack,
    heap: JsHeap,
}

impl Vm {
    pub fn new() -> Self {
        Self::with_gc_policy(GcPolicy::default())
    }

    pub fn with_gc_policy(gc_policy: GcPolicy) -> Self {
        Self {
            ip: 0,
            stack: Vec::new(),
            env: EnvStack::new(),
            heap: JsHeap::with_gc_policy(gc_policy),
        }
    }

    pub(crate) fn install_parts_mut(&mut self) -> (&mut JsHeap, &mut EnvStack) {
        (&mut self.heap, &mut self.env)
    }

    pub(crate) fn run(
        &mut self,
        program: &BytecodeProgram,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        self.release_all_stack_values()?;
        self.ip = 0;
        let base_scope_depth = self.env.depth();

        let result = self.run_inner(program, symbols);

        self.release_all_stack_values()?;

        if result.is_err() {
            self.env
                .truncate_to_depth(base_scope_depth, &mut self.heap)?;
        }

        result
    }

    fn run_inner(
        &mut self,
        program: &BytecodeProgram,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        loop {
            let instr = program
                .instructions
                .get(self.ip)
                .ok_or(Error::JsInstructionPointerOutOfBounds { ip: self.ip })?;
            self.ip += 1;

            match instr {
                Instr::LoadConst(id) => {
                    self.push_value(program.constants.get(*id)?.to_value());
                }
                Instr::PushScope => {
                    self.env.push_scope();
                }
                Instr::PopScope => {
                    self.env.pop_scope(&mut self.heap)?;
                }
                Instr::DeclareLet(symbol) => {
                    self.env
                        .declare_current(*symbol, super::BindingKind::Let, symbols)?;
                }
                Instr::DeclareConst(symbol) => {
                    self.env
                        .declare_current(*symbol, super::BindingKind::Const, symbols)?;
                }
                Instr::LoadBinding(symbol) => {
                    let value = self.env.lookup(*symbol, &mut self.heap, symbols)?;
                    self.push_owned(value);
                }
                Instr::StoreBinding(symbol) => {
                    let value = self.pop_value()?;
                    let result = self
                        .env
                        .store(*symbol, value.clone(), &mut self.heap, symbols);
                    self.heap.free_value(value)?;
                    result?;
                }
                Instr::Add => self.binary_number(|lhs, rhs| lhs + rhs)?,
                Instr::Sub => self.binary_number(|lhs, rhs| lhs - rhs)?,
                Instr::Mul => self.binary_number(|lhs, rhs| lhs * rhs)?,
                Instr::Div => self.binary_number(|lhs, rhs| lhs / rhs)?,
                Instr::Mod => self.binary_number(|lhs, rhs| lhs % rhs)?,
                Instr::LessThan => self.compare_number(|lhs, rhs| lhs < rhs)?,
                Instr::LessThanOrEqual => self.compare_number(|lhs, rhs| lhs <= rhs)?,
                Instr::GreaterThan => self.compare_number(|lhs, rhs| lhs > rhs)?,
                Instr::GreaterThanOrEqual => self.compare_number(|lhs, rhs| lhs >= rhs)?,
                Instr::StrictEqual => {
                    let rhs = self.pop_value()?;
                    let lhs = self.pop_value()?;
                    let result = lhs == rhs;
                    self.heap.free_value(lhs)?;
                    self.heap.free_value(rhs)?;
                    self.push_value(JsValue::Bool(result));
                }
                Instr::StrictNotEqual => {
                    let rhs = self.pop_value()?;
                    let lhs = self.pop_value()?;
                    let result = lhs != rhs;
                    self.heap.free_value(lhs)?;
                    self.heap.free_value(rhs)?;
                    self.push_value(JsValue::Bool(result));
                }
                Instr::LogicalNot => {
                    let value = self.pop_value()?;
                    let result = expect_bool(&value).map(|value| !value);
                    self.heap.free_value(value)?;
                    self.push_value(JsValue::Bool(result?));
                }
                Instr::Jump(target) => {
                    self.ip = *target;
                }
                Instr::JumpIfFalse(target) => {
                    let value = self.pop_value()?;
                    let result = expect_bool(&value);
                    self.heap.free_value(value)?;

                    if !result? {
                        self.ip = *target;
                    }
                }
                Instr::JumpIfTrue(target) => {
                    let value = self.pop_value()?;
                    let result = expect_bool(&value);
                    self.heap.free_value(value)?;

                    if result? {
                        self.ip = *target;
                    }
                }
                Instr::Call(arg_count) => {
                    let mut args = Vec::with_capacity(*arg_count);

                    for _ in 0..*arg_count {
                        args.push(self.pop_value()?);
                    }

                    args.reverse();
                    let callee = self.pop_value()?;
                    let result = self.call_value(&callee, &args, symbols);

                    for arg in args {
                        self.heap.free_value(arg)?;
                    }

                    self.heap.free_value(callee)?;
                    self.push_value(result?);
                }
                Instr::Pop => {
                    self.discard_value()?;
                }
                Instr::NewObject => {
                    let object = self.heap.alloc_object(ObjectKind::Ordinary);
                    self.push_value(JsValue::Object(object));
                }
                Instr::DefineProperty(key) => {
                    let value = self.pop_value()?;
                    let result = expect_object(self.stack.last().ok_or(Error::JsStackUnderflow)?)
                        .and_then(|object| self.heap.set_property(object, *key, value.clone()));
                    self.heap.free_value(value)?;
                    result?;
                }
                Instr::GetProperty(key) => {
                    let object_value = self.pop_value()?;
                    let result = expect_object(&object_value)
                        .and_then(|object| self.heap.get_property(object, *key));
                    self.heap.free_value(object_value)?;
                    self.push_owned(result?);
                }
                Instr::SetProperty(key) => {
                    let value = self.pop_value()?;
                    let object_value = self.pop_value()?;
                    let result = expect_object(&object_value)
                        .and_then(|object| self.heap.set_property(object, *key, value.clone()));
                    self.heap.free_value(object_value)?;
                    match result {
                        Ok(()) => self.push_owned(value),
                        Err(err) => {
                            self.heap.free_value(value)?;
                            return Err(err);
                        }
                    }
                }
                Instr::Halt => {
                    return Ok(JsValue::Undefined);
                }
            }
        }
    }

    pub(crate) fn heap_stats(&self) -> super::HeapStats {
        self.heap.stats()
    }

    pub(crate) fn force_gc(&mut self) -> Result<usize, Error> {
        self.heap.force_gc()
    }

    pub(crate) fn maybe_run_gc(&mut self) -> Result<usize, Error> {
        self.heap.maybe_run_gc()
    }

    pub(crate) fn destroy(&mut self) -> Result<(), Error> {
        self.release_all_stack_values()?;
        self.env.release_all_scopes(&mut self.heap)?;
        self.heap.run_gc()?;
        self.heap.drain_zero_ref()?;

        let stats = self.heap.stats();
        if stats.allocated_objects != 0 {
            return Err(Error::JsHeapLeak {
                objects: stats.allocated_objects,
                bytes: stats.allocated_bytes,
            });
        }

        Ok(())
    }

    fn push_value(&mut self, value: JsValue) {
        self.heap.dup_value(&value);
        self.stack.push(value);
    }

    fn push_owned(&mut self, value: JsValue) {
        self.stack.push(value);
    }

    fn pop_value(&mut self) -> Result<JsValue, Error> {
        self.stack.pop().ok_or(Error::JsStackUnderflow)
    }

    fn discard_value(&mut self) -> Result<(), Error> {
        let value = self.pop_value()?;
        self.heap.free_value(value)
    }

    fn release_all_stack_values(&mut self) -> Result<(), Error> {
        while let Some(value) = self.stack.pop() {
            self.heap.free_value(value)?;
        }

        Ok(())
    }

    fn binary_number(&mut self, op: impl FnOnce(f64, f64) -> f64) -> Result<(), Error> {
        let rhs = self.pop_value()?;
        let lhs = self.pop_value()?;
        let result =
            expect_number(&lhs).and_then(|lhs| expect_number(&rhs).map(|rhs| op(lhs, rhs)));

        self.heap.free_value(lhs)?;
        self.heap.free_value(rhs)?;
        self.push_value(JsValue::Number(result?));

        Ok(())
    }

    fn compare_number(&mut self, op: impl FnOnce(f64, f64) -> bool) -> Result<(), Error> {
        let rhs = self.pop_value()?;
        let lhs = self.pop_value()?;
        let result =
            expect_number(&lhs).and_then(|lhs| expect_number(&rhs).map(|rhs| op(lhs, rhs)));

        self.heap.free_value(lhs)?;
        self.heap.free_value(rhs)?;
        self.push_value(JsValue::Bool(result?));

        Ok(())
    }

    fn call_value(
        &mut self,
        callee: &JsValue,
        args: &[JsValue],
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        let object = expect_object(callee)?;

        match self.heap.object_kind(object)? {
            ObjectKind::HostFunction(function) => function.call(args, &mut self.heap, symbols),
            ObjectKind::Ordinary => Err(Error::JsNotCallable),
        }
    }
}

impl Default for Vm {
    fn default() -> Self {
        Self::new()
    }
}

fn expect_number(value: &JsValue) -> Result<f64, Error> {
    match value {
        JsValue::Number(value) => Ok(*value),
        _ => Err(Error::JsTypeError {
            message: "operator expected numbers".to_owned(),
        }),
    }
}

fn expect_bool(value: &JsValue) -> Result<bool, Error> {
    match value {
        JsValue::Bool(value) => Ok(*value),
        _ => Err(Error::JsTypeError {
            message: "operator expected booleans".to_owned(),
        }),
    }
}

fn expect_object(value: &JsValue) -> Result<ObjectId, Error> {
    match value {
        JsValue::Object(value) => Ok(*value),
        _ => Err(Error::JsTypeError {
            message: "property access expected object".to_owned(),
        }),
    }
}
