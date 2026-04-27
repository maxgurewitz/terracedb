use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::Error;

use super::attachment::JsHostBindings;
use super::{
    BindingCellId, BindingKind, BytecodeProgram, EnvFrameId, EnvStack, GcPolicy, Instr, JsFunction,
    JsHeap, JsValue, ModuleNamespace, ObjectId, ObjectKind, PropertyKey, RuntimeStorageSegments,
    RuntimeStorageUsage, SegmentId, StackId, Symbol, SymbolTable,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InstructionBudget {
    pub instructions: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunResult {
    Suspended,
    Completed(JsValue),
    Failed(Error),
}

#[derive(Deserialize, Serialize)]
pub struct Vm {
    stack_id: StackId,
    stack: Vec<JsValue>,
    env: EnvStack,
    heap: JsHeap,
    execution: Option<ExecutionState>,

    #[serde(skip, default)]
    host: JsHostBindings,
}

#[derive(Deserialize, Serialize)]
struct ExecutionState {
    frames: Vec<ExecFrame>,
}

#[derive(Deserialize, Serialize)]
struct ExecFrame {
    program: BytecodeProgram,
    ip: usize,
    allow_return: bool,
    base_scope_depth: usize,
    restore_frame: Option<EnvFrameId>,
}

enum StepResult {
    Continue,
    Completed(JsValue),
}

impl Vm {
    pub fn new() -> Self {
        Self::with_gc_policy(GcPolicy::default())
    }

    pub fn with_gc_policy(gc_policy: GcPolicy) -> Self {
        Self::with_storage_segments(
            RuntimeStorageSegments {
                heap: SegmentId(0),
                bindings: SegmentId(1),
                env_frames: SegmentId(2),
                stack: SegmentId(3),
            },
            gc_policy,
        )
    }

    pub fn with_storage_segments(segments: RuntimeStorageSegments, gc_policy: GcPolicy) -> Self {
        Self {
            stack_id: StackId {
                segment: segments.stack,
                slot: 0,
            },
            stack: Vec::new(),
            env: EnvStack::with_segments(segments.bindings, segments.env_frames),
            heap: JsHeap::with_segment_and_gc_policy(segments.heap, gc_policy),
            execution: None,
            host: JsHostBindings::default(),
        }
    }

    pub(crate) fn root_env(&self) -> EnvFrameId {
        self.env.root()
    }

    pub(crate) fn stack_id(&self) -> StackId {
        self.stack_id
    }

    pub(crate) fn storage_segments(&self) -> RuntimeStorageSegments {
        RuntimeStorageSegments {
            heap: self.heap.segment(),
            bindings: self.env.binding_segment(),
            env_frames: self.env.frame_segment(),
            stack: self.stack_id.segment,
        }
    }

    pub(crate) fn storage_usage(&self) -> RuntimeStorageUsage {
        RuntimeStorageUsage {
            heap_objects: self.heap.allocated_objects(),
            binding_cells: self.env.live_cell_count(),
            env_frames: self.env.live_frame_count(),
            stacks: 1,
            stack_values: self.stack.len(),
        }
    }

    pub(crate) fn install_parts_mut(
        &mut self,
    ) -> (&mut JsHeap, &mut EnvStack, &mut JsHostBindings) {
        (&mut self.heap, &mut self.env, &mut self.host)
    }

    pub(crate) fn host_mut(&mut self) -> &mut JsHostBindings {
        &mut self.host
    }

    pub(crate) fn create_module_frame(&mut self) -> EnvFrameId {
        self.env.create_module_frame()
    }

    pub(crate) fn declare_in_frame(
        &mut self,
        frame: EnvFrameId,
        name: Symbol,
        kind: BindingKind,
        symbols: &SymbolTable,
    ) -> Result<BindingCellId, Error> {
        self.env.declare_in_frame(frame, name, kind, symbols)
    }

    pub(crate) fn alias_in_frame(
        &mut self,
        frame: EnvFrameId,
        name: Symbol,
        cell: BindingCellId,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        self.env.alias_in_frame(frame, name, cell, symbols)
    }

    pub(crate) fn declare_in_frame_value(
        &mut self,
        frame: EnvFrameId,
        name: Symbol,
        kind: BindingKind,
        value: JsValue,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        self.env
            .declare_in_frame_value(frame, name, kind, value, &mut self.heap, symbols)
    }

    pub(crate) fn cell_for_name_in_frame(
        &self,
        frame: EnvFrameId,
        name: Symbol,
        symbols: &SymbolTable,
    ) -> Result<BindingCellId, Error> {
        self.env.cell_for_name_in_frame(frame, name, symbols)
    }

    pub(crate) fn load_cell(
        &mut self,
        cell: BindingCellId,
        name: Symbol,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        self.env.load_cell(cell, name, &mut self.heap, symbols)
    }

    pub(crate) fn alloc_module_namespace(&mut self, namespace: ModuleNamespace) -> ObjectId {
        self.heap
            .alloc_object(ObjectKind::ModuleNamespace(namespace))
    }

    pub(crate) fn run(
        &mut self,
        program: BytecodeProgram,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        self.start(program)?;

        match self.run_until_complete(symbols) {
            RunResult::Completed(value) => Ok(value),
            RunResult::Failed(err) => Err(err),
            RunResult::Suspended => unreachable!("unbounded run should not suspend"),
        }
    }

    pub(crate) fn run_in_frame(
        &mut self,
        program: BytecodeProgram,
        frame: EnvFrameId,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        let previous = self.env.set_current_frame(frame)?;
        let result = self.run(program, symbols);
        let restore = self.env.restore_current_frame(previous);

        restore?;
        result
    }

    pub(crate) fn start(&mut self, program: BytecodeProgram) -> Result<RunResult, Error> {
        self.release_all_stack_values()?;
        self.execution = Some(ExecutionState {
            frames: vec![ExecFrame {
                program,
                ip: 0,
                allow_return: false,
                base_scope_depth: self.env.depth(),
                restore_frame: None,
            }],
        });

        Ok(RunResult::Suspended)
    }

    pub(crate) fn run_for_budget(
        &mut self,
        budget: InstructionBudget,
        symbols: &SymbolTable,
    ) -> RunResult {
        match self.run_for_budget_inner(budget, symbols) {
            Ok(StepResult::Continue) => RunResult::Suspended,
            Ok(StepResult::Completed(value)) => RunResult::Completed(value),
            Err(err) => {
                let cleanup = self.abort_execution();
                RunResult::Failed(cleanup.err().unwrap_or(err))
            }
        }
    }

    pub(crate) fn run_until_complete(&mut self, symbols: &SymbolTable) -> RunResult {
        loop {
            match self.run_for_budget(
                InstructionBudget {
                    instructions: usize::MAX,
                },
                symbols,
            ) {
                RunResult::Suspended => continue,
                done => return done,
            }
        }
    }

    fn run_for_budget_inner(
        &mut self,
        budget: InstructionBudget,
        symbols: &SymbolTable,
    ) -> Result<StepResult, Error> {
        if self.execution.is_none() {
            return Ok(StepResult::Completed(JsValue::Undefined));
        }

        let mut executed = 0;

        while executed < budget.instructions {
            let instr = {
                let frame = self.current_frame_mut()?;
                let instr = frame
                    .program
                    .instructions
                    .get(frame.ip)
                    .ok_or(Error::JsInstructionPointerOutOfBounds { ip: frame.ip })?
                    .clone();
                frame.ip += 1;
                instr
            };

            executed += 1;

            if let Some(value) = self.execute_instr(instr, symbols)? {
                return Ok(StepResult::Completed(value));
            }

            if self.execution.is_none() {
                return Ok(StepResult::Completed(JsValue::Undefined));
            }
        }

        Ok(StepResult::Continue)
    }

    fn execute_instr(
        &mut self,
        instr: Instr,
        symbols: &SymbolTable,
    ) -> Result<Option<JsValue>, Error> {
        match instr {
            Instr::LoadConst(id) => {
                let value = self.current_frame()?.program.constants.get(id)?.to_value();
                self.push_value(value);
            }
            Instr::PushScope => {
                self.env.push_scope();
            }
            Instr::PopScope => {
                self.env.pop_scope(&mut self.heap)?;
            }
            Instr::DeclareLet(symbol) => {
                self.env
                    .declare_current(symbol, super::BindingKind::Let, symbols)?;
            }
            Instr::DeclareConst(symbol) => {
                self.env
                    .declare_current(symbol, super::BindingKind::Const, symbols)?;
            }
            Instr::LoadBinding(symbol) => {
                let value = self.env.lookup(symbol, &mut self.heap, symbols)?;
                self.push_owned(value);
            }
            Instr::StoreBinding(symbol) => {
                let value = self.pop_value()?;
                let result = self
                    .env
                    .store(symbol, value.clone(), &mut self.heap, symbols);
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
                self.current_frame_mut()?.ip = target;
            }
            Instr::JumpIfFalse(target) => {
                let value = self.pop_value()?;
                let result = expect_bool(&value);
                self.heap.free_value(value)?;

                if !result? {
                    self.current_frame_mut()?.ip = target;
                }
            }
            Instr::JumpIfTrue(target) => {
                let value = self.pop_value()?;
                let result = expect_bool(&value);
                self.heap.free_value(value)?;

                if result? {
                    self.current_frame_mut()?.ip = target;
                }
            }
            Instr::CreateFunction(id) => {
                let function = self.current_frame()?.program.get_function(id)?.clone();
                let captured_env = self.env.capture_current_frame()?;
                let object = self.heap.alloc_object(ObjectKind::JsFunction(JsFunction {
                    name: function.name,
                    params: function.params,
                    body: function.body,
                    captured_env,
                }));

                self.push_value(JsValue::Object(object));
            }
            Instr::Call(arg_count) => {
                self.execute_call(arg_count, symbols)?;
            }
            Instr::Return => {
                let value = self.pop_value()?;

                if self.current_frame()?.allow_return {
                    return self.complete_frame(value);
                }

                self.heap.free_value(value)?;
                return Err(Error::JsTypeError {
                    message: "return outside function".to_owned(),
                });
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
                    .and_then(|object| self.heap.set_property(object, key, value.clone()));
                self.heap.free_value(value)?;
                result?;
            }
            Instr::GetProperty(key) => {
                let object_value = self.pop_value()?;
                let result = expect_object(&object_value)
                    .and_then(|object| self.get_property(object, key, symbols));
                self.heap.free_value(object_value)?;
                self.push_owned(result?);
            }
            Instr::SetProperty(key) => {
                let value = self.pop_value()?;
                let object_value = self.pop_value()?;
                let result = expect_object(&object_value)
                    .and_then(|object| self.set_property(object, key, value.clone()));
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
                return self.complete_frame(JsValue::Undefined);
            }
        }

        Ok(None)
    }

    fn execute_call(&mut self, arg_count: usize, symbols: &SymbolTable) -> Result<(), Error> {
        let mut args = Vec::with_capacity(arg_count);

        for _ in 0..arg_count {
            args.push(self.pop_value()?);
        }

        args.reverse();
        let callee = self.pop_value()?;
        let result = self.call_value(&callee, &args, symbols);

        for arg in args {
            self.heap.free_value(arg)?;
        }

        self.heap.free_value(callee)?;

        if let Some(value) = result? {
            self.push_owned(value);
        }

        Ok(())
    }

    pub(crate) fn heap_stats(&self) -> super::HeapStats {
        self.heap.stats()
    }

    pub(crate) fn force_gc(&mut self) -> Result<usize, Error> {
        self.run_gc_at_safepoint()
    }

    pub(crate) fn maybe_run_gc(&mut self) -> Result<usize, Error> {
        if self.heap.should_run_gc() {
            self.run_gc_at_safepoint()
        } else {
            Ok(0)
        }
    }

    pub(crate) fn destroy(&mut self) -> Result<(), Error> {
        self.abort_execution()?;
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
    ) -> Result<Option<JsValue>, Error> {
        let JsValue::Object(object) = callee else {
            return Err(Error::JsNotCallable);
        };

        match self.heap.object_kind(*object)? {
            ObjectKind::HostFunction(function) => function
                .call(args, &self.host, &mut self.heap, symbols)
                .map(Some),
            ObjectKind::JsFunction(function) => {
                self.enter_js_function(function, args, symbols)?;
                Ok(None)
            }
            ObjectKind::ModuleNamespace(_) => Err(Error::JsNotCallable),
            ObjectKind::Ordinary => Err(Error::JsNotCallable),
        }
    }

    fn get_property(
        &mut self,
        object: ObjectId,
        key: PropertyKey,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        match self.heap.object_kind(object)? {
            ObjectKind::ModuleNamespace(namespace) => match key {
                PropertyKey::Symbol(symbol) => {
                    let Some(cell) = namespace.exports.get(&symbol).copied() else {
                        return Ok(JsValue::Undefined);
                    };

                    self.env.load_cell(cell, symbol, &mut self.heap, symbols)
                }
            },
            _ => self.heap.get_property(object, key),
        }
    }

    fn set_property(
        &mut self,
        object: ObjectId,
        key: PropertyKey,
        value: JsValue,
    ) -> Result<(), Error> {
        match self.heap.object_kind(object)? {
            ObjectKind::ModuleNamespace(_) => Err(Error::JsTypeError {
                message: "module namespace properties are read-only".to_owned(),
            }),
            _ => self.heap.set_property(object, key, value),
        }
    }

    fn enter_js_function(
        &mut self,
        function: JsFunction,
        args: &[JsValue],
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        let saved_frame = self.env.set_current_frame(function.captured_env)?;
        let captured_depth = self.env.depth();

        self.env.push_scope();

        let bind_result = self.bind_function_params(&function, args, symbols);

        if let Err(err) = bind_result {
            let cleanup = self.env.truncate_to_depth(captured_depth, &mut self.heap);
            let restore = self.env.restore_current_frame(saved_frame);
            return Err(cleanup.err().or_else(|| restore.err()).unwrap_or(err));
        }

        self.execution
            .as_mut()
            .expect("call requires active execution")
            .frames
            .push(ExecFrame {
                program: function.body,
                ip: 0,
                allow_return: true,
                base_scope_depth: captured_depth,
                restore_frame: Some(saved_frame),
            });

        Ok(())
    }

    fn bind_function_params(
        &mut self,
        function: &JsFunction,
        args: &[JsValue],
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        let mut bound = HashSet::new();

        for (index, param) in function.params.iter().copied().enumerate() {
            let value = args.get(index).cloned().unwrap_or(JsValue::Undefined);

            if bound.insert(param) {
                self.env.declare_current_value(
                    param,
                    BindingKind::Let,
                    value,
                    &mut self.heap,
                    symbols,
                )?;
            } else {
                self.env.store(param, value, &mut self.heap, symbols)?;
            }
        }

        Ok(())
    }

    fn complete_frame(&mut self, value: JsValue) -> Result<Option<JsValue>, Error> {
        let frame = self
            .execution
            .as_mut()
            .and_then(|execution| execution.frames.pop())
            .ok_or(Error::JsInstructionPointerOutOfBounds { ip: 0 })?;

        let cleanup_result = self
            .env
            .truncate_to_depth(frame.base_scope_depth, &mut self.heap);
        let restore_result = match frame.restore_frame {
            Some(restore_frame) => self.env.restore_current_frame(restore_frame),
            None => Ok(()),
        };

        if let Err(err) = cleanup_result.and(restore_result) {
            self.heap.free_value(value)?;
            return Err(err);
        }

        let is_done = self
            .execution
            .as_ref()
            .is_none_or(|execution| execution.frames.is_empty());

        if is_done {
            self.execution = None;
            self.release_all_stack_values()?;
            Ok(Some(value))
        } else {
            self.push_owned(value);
            Ok(None)
        }
    }

    fn abort_execution(&mut self) -> Result<(), Error> {
        if let Some(mut execution) = self.execution.take() {
            while let Some(frame) = execution.frames.pop() {
                self.env
                    .truncate_to_depth(frame.base_scope_depth, &mut self.heap)?;

                if let Some(restore_frame) = frame.restore_frame {
                    self.env.restore_current_frame(restore_frame)?;
                }
            }
        }

        self.release_all_stack_values()
    }

    fn current_frame(&self) -> Result<&ExecFrame, Error> {
        self.execution
            .as_ref()
            .and_then(|execution| execution.frames.last())
            .ok_or(Error::JsInstructionPointerOutOfBounds { ip: 0 })
    }

    fn current_frame_mut(&mut self) -> Result<&mut ExecFrame, Error> {
        self.execution
            .as_mut()
            .and_then(|execution| execution.frames.last_mut())
            .ok_or(Error::JsInstructionPointerOutOfBounds { ip: 0 })
    }

    fn run_gc_at_safepoint(&mut self) -> Result<usize, Error> {
        let before = self.heap.stats().freed_objects;

        self.release_unreachable_inactive_frames()?;
        self.heap.force_gc()?;

        Ok(self.heap.stats().freed_objects.saturating_sub(before))
    }

    fn release_unreachable_inactive_frames(&mut self) -> Result<usize, Error> {
        let mut marked_frames = HashSet::new();
        let mut marked_objects = HashSet::new();
        let mut frame_work = self.env.active_frame_roots();
        let mut object_work = self
            .stack
            .iter()
            .filter_map(|value| match value {
                JsValue::Object(object) => Some(*object),
                _ => None,
            })
            .collect::<Vec<_>>();

        while !frame_work.is_empty() || !object_work.is_empty() {
            while let Some(frame) = frame_work.pop() {
                if !marked_frames.insert(frame) {
                    continue;
                }

                let edges = self.env.frame_edges(frame)?;
                frame_work.extend(edges.frames);
                object_work.extend(edges.objects);
            }

            while let Some(object) = object_work.pop() {
                if !marked_objects.insert(object) {
                    continue;
                }

                let edges = self.heap.runtime_edges(object)?;
                frame_work.extend(edges.frames);
                object_work.extend(edges.objects);
            }
        }

        self.env
            .release_unmarked_inactive_frames(&marked_frames, &mut self.heap)
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
