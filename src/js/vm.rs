use crate::Error;

use super::{BytecodeProgram, EnvStack, Instr, JsValue, SymbolTable};

pub(crate) trait JsHostIo {
    fn console_log(&mut self, value: JsValue) -> Result<(), Error>;
}

pub struct Vm {
    ip: usize,
    stack: Vec<JsValue>,
    env: EnvStack,
}

impl Vm {
    pub fn new() -> Self {
        Self {
            ip: 0,
            stack: Vec::new(),
            env: EnvStack::new(),
        }
    }

    pub(crate) fn run(
        &mut self,
        program: &BytecodeProgram,
        symbols: &SymbolTable,
        host: &mut dyn JsHostIo,
    ) -> Result<JsValue, Error> {
        self.ip = 0;
        self.stack.clear();
        let base_scope_depth = self.env.depth();

        let result = self.run_inner(program, symbols, host);

        if result.is_err() {
            self.env.truncate_to_depth(base_scope_depth);
        }

        result
    }

    fn run_inner(
        &mut self,
        program: &BytecodeProgram,
        symbols: &SymbolTable,
        host: &mut dyn JsHostIo,
    ) -> Result<JsValue, Error> {
        loop {
            let instr = program
                .instructions
                .get(self.ip)
                .ok_or(Error::JsInstructionPointerOutOfBounds { ip: self.ip })?;
            self.ip += 1;

            match instr {
                Instr::LoadConst(id) => {
                    self.stack.push(program.constants.get(*id)?.to_value());
                }
                Instr::PushScope => {
                    self.env.push_scope();
                }
                Instr::PopScope => {
                    self.env.pop_scope()?;
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
                    self.stack.push(self.env.lookup(*symbol, symbols)?);
                }
                Instr::StoreBinding(symbol) => {
                    let value = self.pop()?;
                    self.env.store(*symbol, value, symbols)?;
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
                    let rhs = self.pop()?;
                    let lhs = self.pop()?;
                    self.stack.push(JsValue::Bool(lhs == rhs));
                }
                Instr::StrictNotEqual => {
                    let rhs = self.pop()?;
                    let lhs = self.pop()?;
                    self.stack.push(JsValue::Bool(lhs != rhs));
                }
                Instr::LogicalNot => {
                    let value = expect_bool(self.pop()?)?;
                    self.stack.push(JsValue::Bool(!value));
                }
                Instr::Jump(target) => {
                    self.ip = *target;
                }
                Instr::JumpIfFalse(target) => {
                    if !expect_bool(self.pop()?)? {
                        self.ip = *target;
                    }
                }
                Instr::JumpIfTrue(target) => {
                    if expect_bool(self.pop()?)? {
                        self.ip = *target;
                    }
                }
                Instr::ConsoleLog => {
                    let value = self.pop()?;
                    host.console_log(value)?;
                }
                Instr::Halt => {
                    return Ok(JsValue::Undefined);
                }
            }
        }
    }

    fn pop(&mut self) -> Result<JsValue, Error> {
        self.stack.pop().ok_or(Error::JsStackUnderflow)
    }

    fn binary_number(&mut self, op: impl FnOnce(f64, f64) -> f64) -> Result<(), Error> {
        let rhs = expect_number(self.pop()?)?;
        let lhs = expect_number(self.pop()?)?;
        self.stack.push(JsValue::Number(op(lhs, rhs)));
        Ok(())
    }

    fn compare_number(&mut self, op: impl FnOnce(f64, f64) -> bool) -> Result<(), Error> {
        let rhs = expect_number(self.pop()?)?;
        let lhs = expect_number(self.pop()?)?;
        self.stack.push(JsValue::Bool(op(lhs, rhs)));
        Ok(())
    }
}

impl Default for Vm {
    fn default() -> Self {
        Self::new()
    }
}

fn expect_number(value: JsValue) -> Result<f64, Error> {
    match value {
        JsValue::Number(value) => Ok(value),
        _ => Err(Error::JsTypeError {
            message: "operator expected numbers".to_owned(),
        }),
    }
}

fn expect_bool(value: JsValue) -> Result<bool, Error> {
    match value {
        JsValue::Bool(value) => Ok(value),
        _ => Err(Error::JsTypeError {
            message: "operator expected booleans".to_owned(),
        }),
    }
}
