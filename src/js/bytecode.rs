use crate::Error;

use super::{JsValue, PropertyKey, Symbol};

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct ConstId(pub u32);

pub struct ConstantPool {
    values: Vec<Constant>,
}

impl ConstantPool {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn push(&mut self, value: Constant) -> ConstId {
        let id = ConstId(self.values.len() as u32);
        self.values.push(value);
        id
    }

    pub fn get(&self, id: ConstId) -> Result<&Constant, Error> {
        self.values
            .get(id.0 as usize)
            .ok_or(Error::JsInvalidConstant { id: id.0 })
    }
}

impl Default for ConstantPool {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BytecodeProgram {
    pub constants: ConstantPool,
    pub instructions: Vec<Instr>,
}

impl BytecodeProgram {
    pub fn new() -> Self {
        Self {
            constants: ConstantPool::new(),
            instructions: Vec::new(),
        }
    }
}

impl Default for BytecodeProgram {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
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
    Call(usize),
    Pop,
    NewObject,
    DefineProperty(PropertyKey),
    GetProperty(PropertyKey),
    SetProperty(PropertyKey),
    Halt,
}
