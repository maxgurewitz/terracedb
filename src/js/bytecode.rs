use crate::Error;

use super::{JsValue, PropertyKey, Symbol, ValueCols};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
pub enum Constant {
    Number(f64),
    Bool(bool),
    String(String),
    Null,
    Undefined,
}

impl Constant {
    pub(crate) fn to_value(&self) -> JsValue {
        match self {
            Self::Number(value) => JsValue::Number(*value),
            Self::Bool(value) => JsValue::Bool(*value),
            Self::String(value) => JsValue::String(value.clone()),
            Self::Null => JsValue::Null,
            Self::Undefined => JsValue::Undefined,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ConstId(pub u32);

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct FunctionId(pub u32);

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
pub struct ConstantPool {
    values: ValueCols,
}

impl ConstantPool {
    pub fn new() -> Self {
        Self {
            values: ValueCols::new(),
        }
    }

    pub fn push(&mut self, value: Constant) -> ConstId {
        let id = ConstId(self.values.len() as u32);
        self.values.push(constant_to_value(value));
        id
    }

    pub fn get(&self, id: ConstId) -> Result<Constant, Error> {
        if id.0 as usize >= self.values.len() {
            return Err(Error::JsInvalidConstant { id: id.0 });
        }

        value_to_constant(self.values.get(id.0))
    }

    fn append_from(&mut self, other: &Self, start: u32, len: u32) -> u32 {
        let new_start = self.values.len() as u32;
        for index in start..start + len {
            self.values.push(other.values.get(index));
        }
        new_start
    }

    fn copy_range_to_end(&mut self, start: u32, len: u32) -> u32 {
        let values = (start..start + len)
            .map(|index| self.values.get(index))
            .collect::<Vec<_>>();
        let new_start = self.values.len() as u32;
        for value in values {
            self.values.push(value);
        }
        new_start
    }
}

impl Default for ConstantPool {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum Opcode {
    LoadConst,
    PushScope,
    PopScope,
    DeclareLet,
    DeclareConst,
    LoadBinding,
    StoreBinding,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    StrictEqual,
    StrictNotEqual,
    LogicalNot,
    Jump,
    JumpIfFalse,
    JumpIfTrue,
    CreateFunction,
    Call,
    Return,
    Pop,
    NewObject,
    DefineProperty,
    GetProperty,
    SetProperty,
    Halt,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
pub struct BytecodeProgram {
    root_program: u32,

    program_constant_range_start: Vec<u32>,
    program_constant_range_len: Vec<u32>,
    program_instr_range_start: Vec<u32>,
    program_instr_range_len: Vec<u32>,
    program_function_range_start: Vec<u32>,
    program_function_range_len: Vec<u32>,

    constants: ConstantPool,

    instr_opcode: Vec<Opcode>,
    instr_arg0: Vec<u32>,
    instr_arg1: Vec<u32>,

    compiled_function_name: Vec<Option<Symbol>>,
    compiled_function_param_range_start: Vec<u32>,
    compiled_function_param_range_len: Vec<u32>,
    compiled_function_body: Vec<u32>,
    compiled_function_param_symbol: Vec<Symbol>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
pub struct CompiledFunction {
    pub name: Option<Symbol>,
    pub params: Vec<Symbol>,
    pub body: BytecodeProgram,
}

impl BytecodeProgram {
    pub fn new() -> Self {
        Self {
            root_program: 0,
            program_constant_range_start: vec![0],
            program_constant_range_len: vec![0],
            program_instr_range_start: vec![0],
            program_instr_range_len: vec![0],
            program_function_range_start: vec![0],
            program_function_range_len: vec![0],
            constants: ConstantPool::new(),
            instr_opcode: Vec::new(),
            instr_arg0: Vec::new(),
            instr_arg1: Vec::new(),
            compiled_function_name: Vec::new(),
            compiled_function_param_range_start: Vec::new(),
            compiled_function_param_range_len: Vec::new(),
            compiled_function_body: Vec::new(),
            compiled_function_param_symbol: Vec::new(),
        }
    }

    pub fn push_const(&mut self, value: Constant) -> ConstId {
        self.ensure_root_constants_appendable();
        let id = self.constants.push(value);
        let root = self.root_program as usize;
        self.program_constant_range_len[root] += 1;
        id
    }

    pub fn get_const(&self, id: ConstId) -> Result<Constant, Error> {
        let root = self.root_program as usize;
        let offset = self.program_constant_range_start[root] + id.0;
        if id.0 >= self.program_constant_range_len[root] {
            return Err(Error::JsInvalidConstant { id: id.0 });
        }
        self.constants.get(ConstId(offset))
    }

    pub fn push_instr(&mut self, instr: Instr) -> usize {
        self.ensure_root_instructions_appendable();
        let root = self.root_program as usize;
        let ip = self.program_instr_range_len[root] as usize;
        let (opcode, arg0, arg1) = encode_instr(instr);
        self.instr_opcode.push(opcode);
        self.instr_arg0.push(arg0);
        self.instr_arg1.push(arg1);
        self.program_instr_range_len[root] += 1;
        ip
    }

    pub fn instr(&self, ip: usize) -> Result<Instr, Error> {
        let root = self.root_program as usize;
        if ip >= self.program_instr_range_len[root] as usize {
            return Err(Error::JsInstructionPointerOutOfBounds { ip });
        }
        let index = self.program_instr_range_start[root] as usize + ip;
        decode_instr(
            self.instr_opcode[index],
            self.instr_arg0[index],
            self.instr_arg1[index],
        )
    }

    pub fn instructions_len(&self) -> usize {
        self.program_instr_range_len[self.root_program as usize] as usize
    }

    pub fn iter_instructions(&self) -> impl Iterator<Item = Instr> + '_ {
        (0..self.instructions_len()).map(|ip| self.instr(ip).expect("valid instruction range"))
    }

    pub fn last_instr(&self) -> Option<Instr> {
        self.instructions_len()
            .checked_sub(1)
            .and_then(|ip| self.instr(ip).ok())
    }

    pub fn patch_jump(&mut self, at: usize, target: usize) {
        let root = self.root_program as usize;
        let index = self.program_instr_range_start[root] as usize + at;
        match self.instr_opcode[index] {
            Opcode::Jump | Opcode::JumpIfFalse | Opcode::JumpIfTrue => {
                self.instr_arg0[index] = target as u32;
            }
            _ => panic!("cannot patch non-jump instruction"),
        }
    }

    pub fn push_function(&mut self, function: CompiledFunction) -> FunctionId {
        let body_program = self.merge_program(function.body);
        self.ensure_root_functions_appendable();
        let root = self.root_program as usize;
        let id = FunctionId(self.program_function_range_len[root]);
        self.compiled_function_name.push(function.name);
        self.compiled_function_param_range_start
            .push(self.compiled_function_param_symbol.len() as u32);
        self.compiled_function_param_range_len
            .push(function.params.len() as u32);
        self.compiled_function_param_symbol.extend(function.params);
        self.compiled_function_body.push(body_program);
        self.program_function_range_len[root] += 1;
        id
    }

    pub fn get_function(&self, id: FunctionId) -> Result<CompiledFunction, Error> {
        let root = self.root_program as usize;
        if id.0 >= self.program_function_range_len[root] {
            return Err(Error::JsInvalidFunction { id: id.0 });
        }
        let index = self.program_function_range_start[root] + id.0;
        let index = index as usize;
        let param_start = self.compiled_function_param_range_start[index] as usize;
        let param_len = self.compiled_function_param_range_len[index] as usize;

        Ok(CompiledFunction {
            name: self.compiled_function_name[index],
            params: self.compiled_function_param_symbol[param_start..param_start + param_len]
                .to_vec(),
            body: self.program_view(self.compiled_function_body[index]),
        })
    }

    fn merge_program(&mut self, other: BytecodeProgram) -> u32 {
        let program_offset = self.program_constant_range_start.len() as u32;
        let const_offset = self.constants.values.len() as u32;
        let instr_offset = self.instr_opcode.len() as u32;
        let function_offset = self.compiled_function_name.len() as u32;
        let param_offset = self.compiled_function_param_symbol.len() as u32;

        for program in 0..other.program_constant_range_start.len() {
            self.program_constant_range_start
                .push(const_offset + other.program_constant_range_start[program]);
            self.program_constant_range_len
                .push(other.program_constant_range_len[program]);
            self.program_instr_range_start
                .push(instr_offset + other.program_instr_range_start[program]);
            self.program_instr_range_len
                .push(other.program_instr_range_len[program]);
            self.program_function_range_start
                .push(function_offset + other.program_function_range_start[program]);
            self.program_function_range_len
                .push(other.program_function_range_len[program]);
        }

        self.constants
            .append_from(&other.constants, 0, other.constants.values.len() as u32);
        self.instr_opcode.extend(other.instr_opcode);
        self.instr_arg0.extend(other.instr_arg0);
        self.instr_arg1.extend(other.instr_arg1);
        self.compiled_function_name
            .extend(other.compiled_function_name);
        self.compiled_function_param_range_start.extend(
            other
                .compiled_function_param_range_start
                .into_iter()
                .map(|start| start + param_offset),
        );
        self.compiled_function_param_range_len
            .extend(other.compiled_function_param_range_len);
        self.compiled_function_body.extend(
            other
                .compiled_function_body
                .into_iter()
                .map(|body| body + program_offset),
        );
        self.compiled_function_param_symbol
            .extend(other.compiled_function_param_symbol);

        program_offset + other.root_program
    }

    fn program_view(&self, root_program: u32) -> Self {
        let mut cloned = self.clone();
        cloned.root_program = root_program;
        cloned
    }

    fn ensure_root_constants_appendable(&mut self) {
        let root = self.root_program as usize;
        let start = self.program_constant_range_start[root];
        let len = self.program_constant_range_len[root];
        if start + len == self.constants.values.len() as u32 {
            return;
        }

        self.program_constant_range_start[root] = self.constants.copy_range_to_end(start, len);
    }

    fn ensure_root_instructions_appendable(&mut self) {
        let root = self.root_program as usize;
        let start = self.program_instr_range_start[root] as usize;
        let len = self.program_instr_range_len[root] as usize;
        if start + len == self.instr_opcode.len() {
            return;
        }

        let opcodes = self.instr_opcode[start..start + len].to_vec();
        let arg0 = self.instr_arg0[start..start + len].to_vec();
        let arg1 = self.instr_arg1[start..start + len].to_vec();
        self.program_instr_range_start[root] = self.instr_opcode.len() as u32;
        self.instr_opcode.extend(opcodes);
        self.instr_arg0.extend(arg0);
        self.instr_arg1.extend(arg1);
    }

    fn ensure_root_functions_appendable(&mut self) {
        let root = self.root_program as usize;
        let start = self.program_function_range_start[root] as usize;
        let len = self.program_function_range_len[root] as usize;
        if start + len == self.compiled_function_name.len() {
            return;
        }

        let names = self.compiled_function_name[start..start + len].to_vec();
        let param_starts = self.compiled_function_param_range_start[start..start + len].to_vec();
        let param_lens = self.compiled_function_param_range_len[start..start + len].to_vec();
        let bodies = self.compiled_function_body[start..start + len].to_vec();

        self.program_function_range_start[root] = self.compiled_function_name.len() as u32;
        self.compiled_function_name.extend(names);
        self.compiled_function_param_range_start
            .extend(param_starts);
        self.compiled_function_param_range_len.extend(param_lens);
        self.compiled_function_body.extend(bodies);
    }
}

impl Default for BytecodeProgram {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
pub enum Instr {
    LoadConst(ConstId),
    PushScope,
    PopScope,
    DeclareLet(Symbol),
    DeclareConst(Symbol),
    LoadBinding(Symbol),
    StoreBinding(Symbol),
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    StrictEqual,
    StrictNotEqual,
    LogicalNot,
    Jump(usize),
    JumpIfFalse(usize),
    JumpIfTrue(usize),
    CreateFunction(FunctionId),
    Call(usize),
    Return,
    Pop,
    NewObject,
    DefineProperty(PropertyKey),
    GetProperty(PropertyKey),
    SetProperty(PropertyKey),
    Halt,
}

fn constant_to_value(value: Constant) -> JsValue {
    match value {
        Constant::Number(value) => JsValue::Number(value),
        Constant::Bool(value) => JsValue::Bool(value),
        Constant::String(value) => JsValue::String(value),
        Constant::Null => JsValue::Null,
        Constant::Undefined => JsValue::Undefined,
    }
}

fn value_to_constant(value: JsValue) -> Result<Constant, Error> {
    match value {
        JsValue::Number(value) => Ok(Constant::Number(value)),
        JsValue::Bool(value) => Ok(Constant::Bool(value)),
        JsValue::String(value) => Ok(Constant::String(value)),
        JsValue::Null => Ok(Constant::Null),
        JsValue::Undefined => Ok(Constant::Undefined),
        JsValue::Object(_) => Err(Error::JsInvalidConstant { id: u32::MAX }),
    }
}

fn encode_instr(instr: Instr) -> (Opcode, u32, u32) {
    match instr {
        Instr::LoadConst(id) => (Opcode::LoadConst, id.0, 0),
        Instr::PushScope => (Opcode::PushScope, 0, 0),
        Instr::PopScope => (Opcode::PopScope, 0, 0),
        Instr::DeclareLet(symbol) => (Opcode::DeclareLet, symbol.0, 0),
        Instr::DeclareConst(symbol) => (Opcode::DeclareConst, symbol.0, 0),
        Instr::LoadBinding(symbol) => (Opcode::LoadBinding, symbol.0, 0),
        Instr::StoreBinding(symbol) => (Opcode::StoreBinding, symbol.0, 0),
        Instr::Add => (Opcode::Add, 0, 0),
        Instr::Sub => (Opcode::Sub, 0, 0),
        Instr::Mul => (Opcode::Mul, 0, 0),
        Instr::Div => (Opcode::Div, 0, 0),
        Instr::Mod => (Opcode::Mod, 0, 0),
        Instr::LessThan => (Opcode::LessThan, 0, 0),
        Instr::LessThanOrEqual => (Opcode::LessThanOrEqual, 0, 0),
        Instr::GreaterThan => (Opcode::GreaterThan, 0, 0),
        Instr::GreaterThanOrEqual => (Opcode::GreaterThanOrEqual, 0, 0),
        Instr::StrictEqual => (Opcode::StrictEqual, 0, 0),
        Instr::StrictNotEqual => (Opcode::StrictNotEqual, 0, 0),
        Instr::LogicalNot => (Opcode::LogicalNot, 0, 0),
        Instr::Jump(target) => (Opcode::Jump, target as u32, 0),
        Instr::JumpIfFalse(target) => (Opcode::JumpIfFalse, target as u32, 0),
        Instr::JumpIfTrue(target) => (Opcode::JumpIfTrue, target as u32, 0),
        Instr::CreateFunction(id) => (Opcode::CreateFunction, id.0, 0),
        Instr::Call(count) => (Opcode::Call, count as u32, 0),
        Instr::Return => (Opcode::Return, 0, 0),
        Instr::Pop => (Opcode::Pop, 0, 0),
        Instr::NewObject => (Opcode::NewObject, 0, 0),
        Instr::DefineProperty(PropertyKey::Symbol(symbol)) => (Opcode::DefineProperty, symbol.0, 0),
        Instr::GetProperty(PropertyKey::Symbol(symbol)) => (Opcode::GetProperty, symbol.0, 0),
        Instr::SetProperty(PropertyKey::Symbol(symbol)) => (Opcode::SetProperty, symbol.0, 0),
        Instr::Halt => (Opcode::Halt, 0, 0),
    }
}

fn decode_instr(opcode: Opcode, arg0: u32, _arg1: u32) -> Result<Instr, Error> {
    Ok(match opcode {
        Opcode::LoadConst => Instr::LoadConst(ConstId(arg0)),
        Opcode::PushScope => Instr::PushScope,
        Opcode::PopScope => Instr::PopScope,
        Opcode::DeclareLet => Instr::DeclareLet(Symbol(arg0)),
        Opcode::DeclareConst => Instr::DeclareConst(Symbol(arg0)),
        Opcode::LoadBinding => Instr::LoadBinding(Symbol(arg0)),
        Opcode::StoreBinding => Instr::StoreBinding(Symbol(arg0)),
        Opcode::Add => Instr::Add,
        Opcode::Sub => Instr::Sub,
        Opcode::Mul => Instr::Mul,
        Opcode::Div => Instr::Div,
        Opcode::Mod => Instr::Mod,
        Opcode::LessThan => Instr::LessThan,
        Opcode::LessThanOrEqual => Instr::LessThanOrEqual,
        Opcode::GreaterThan => Instr::GreaterThan,
        Opcode::GreaterThanOrEqual => Instr::GreaterThanOrEqual,
        Opcode::StrictEqual => Instr::StrictEqual,
        Opcode::StrictNotEqual => Instr::StrictNotEqual,
        Opcode::LogicalNot => Instr::LogicalNot,
        Opcode::Jump => Instr::Jump(arg0 as usize),
        Opcode::JumpIfFalse => Instr::JumpIfFalse(arg0 as usize),
        Opcode::JumpIfTrue => Instr::JumpIfTrue(arg0 as usize),
        Opcode::CreateFunction => Instr::CreateFunction(FunctionId(arg0)),
        Opcode::Call => Instr::Call(arg0 as usize),
        Opcode::Return => Instr::Return,
        Opcode::Pop => Instr::Pop,
        Opcode::NewObject => Instr::NewObject,
        Opcode::DefineProperty => Instr::DefineProperty(PropertyKey::Symbol(Symbol(arg0))),
        Opcode::GetProperty => Instr::GetProperty(PropertyKey::Symbol(Symbol(arg0))),
        Opcode::SetProperty => Instr::SetProperty(PropertyKey::Symbol(Symbol(arg0))),
        Opcode::Halt => Instr::Halt,
    })
}
