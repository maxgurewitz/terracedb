use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use bytes::Bytes;

use crate::Error;

use super::{
    BinaryOp, BindingKind, EnvStack, JsRuntimeAttachment, JsRuntimeId, JsValue, LogicalOp,
    MiniExpr, MiniProgram, MiniStmt, RuntimeConsole, SymbolTable, UnaryOp, parse_and_lower_minijs,
};

pub struct JsRuntimeInstance {
    id: JsRuntimeId,
    symbols: SymbolTable,
    env_stack: EnvStack,
    attachments: HashMap<TypeId, Box<dyn Any + Send>>,
}

impl JsRuntimeInstance {
    pub(crate) fn new(id: JsRuntimeId) -> Self {
        Self {
            id,
            symbols: SymbolTable::new(),
            env_stack: EnvStack::new(),
            attachments: HashMap::new(),
        }
    }

    pub fn id(&self) -> JsRuntimeId {
        self.id
    }

    pub fn install(&mut self, attachment: Box<dyn JsRuntimeAttachment>) -> Result<(), Error> {
        attachment.install(self)
    }

    pub fn insert_attachment<T>(&mut self, attachment: T) -> Option<T>
    where
        T: Send + 'static,
    {
        let previous = self
            .attachments
            .insert(TypeId::of::<T>(), Box::new(attachment));

        previous.and_then(|attachment| attachment.downcast::<T>().ok().map(|value| *value))
    }

    pub fn attachment<T>(&self) -> Option<&T>
    where
        T: Send + 'static,
    {
        self.attachments
            .get(&TypeId::of::<T>())
            .and_then(|attachment| attachment.downcast_ref::<T>())
    }

    pub fn attachment_mut<T>(&mut self) -> Option<&mut T>
    where
        T: Send + 'static,
    {
        self.attachments
            .get_mut(&TypeId::of::<T>())
            .and_then(|attachment| attachment.downcast_mut::<T>())
    }

    pub fn eval(&mut self, source: &str) -> Result<JsValue, Error> {
        let program = parse_and_lower_minijs(source, &mut self.symbols)?;
        self.eval_program(&program)
    }

    fn eval_program(&mut self, program: &MiniProgram) -> Result<JsValue, Error> {
        for stmt in program {
            self.eval_stmt(stmt)?;
        }

        Ok(JsValue::Undefined)
    }

    fn eval_stmt(&mut self, stmt: &MiniStmt) -> Result<(), Error> {
        match stmt {
            MiniStmt::Block(stmts) => self.eval_block(stmts),
            MiniStmt::Let { name, expr } => {
                let value = self.eval_expr(expr)?;
                self.env_stack
                    .declare_current(*name, BindingKind::Let, value, &self.symbols)
            }
            MiniStmt::Const { name, expr } => {
                let value = self.eval_expr(expr)?;
                self.env_stack
                    .declare_current(*name, BindingKind::Const, value, &self.symbols)
            }
            MiniStmt::Assign { name, expr } => {
                let value = self.eval_expr(expr)?;
                self.env_stack.assign(*name, value, &self.symbols)
            }
            MiniStmt::Expr(expr) => {
                let _ = self.eval_expr(expr)?;

                Ok(())
            }
            MiniStmt::ConsoleLog { expr } => {
                let value = self.eval_expr(expr)?;
                let console = self
                    .attachment::<RuntimeConsole>()
                    .ok_or(Error::MissingConsole)?;
                let mut line = value.stringify();
                line.push('\n');

                console.stdout.write(Bytes::from(line))
            }
        }
    }

    fn eval_block(&mut self, stmts: &[MiniStmt]) -> Result<(), Error> {
        self.env_stack.push_scope();

        let eval_result = stmts.iter().try_for_each(|stmt| self.eval_stmt(stmt));
        let pop_result = self.env_stack.pop_scope();

        match (eval_result, pop_result) {
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn eval_expr(&mut self, expr: &MiniExpr) -> Result<JsValue, Error> {
        match expr {
            MiniExpr::Number(value) => Ok(JsValue::Number(*value)),
            MiniExpr::Bool(value) => Ok(JsValue::Bool(*value)),
            MiniExpr::String(value) => Ok(JsValue::String(value.clone())),
            MiniExpr::Null => Ok(JsValue::Null),
            MiniExpr::Undefined => Ok(JsValue::Undefined),
            MiniExpr::Ident(name) => self.env_stack.lookup(*name, &self.symbols),
            MiniExpr::Assign { name, expr } => {
                let value = self.eval_expr(expr)?;
                self.env_stack.assign(*name, value.clone(), &self.symbols)?;

                Ok(value)
            }
            MiniExpr::Unary { op, expr } => match op {
                UnaryOp::Not => Ok(JsValue::Bool(!expect_bool(self.eval_expr(expr)?)?)),
            },
            MiniExpr::Binary { op, left, right } => {
                let left = self.eval_expr(left)?;
                let right = self.eval_expr(right)?;

                eval_binary(op, left, right)
            }
            MiniExpr::Logical { op, left, right } => match op {
                LogicalOp::And => {
                    let left = expect_bool(self.eval_expr(left)?)?;

                    if !left {
                        return Ok(JsValue::Bool(false));
                    }

                    Ok(JsValue::Bool(expect_bool(self.eval_expr(right)?)?))
                }
                LogicalOp::Or => {
                    let left = expect_bool(self.eval_expr(left)?)?;

                    if left {
                        return Ok(JsValue::Bool(true));
                    }

                    Ok(JsValue::Bool(expect_bool(self.eval_expr(right)?)?))
                }
            },
        }
    }
}

fn eval_binary(op: &BinaryOp, left: JsValue, right: JsValue) -> Result<JsValue, Error> {
    match op {
        BinaryOp::Add => Ok(JsValue::Number(
            expect_number(left)? + expect_number(right)?,
        )),
        BinaryOp::Sub => Ok(JsValue::Number(
            expect_number(left)? - expect_number(right)?,
        )),
        BinaryOp::Mul => Ok(JsValue::Number(
            expect_number(left)? * expect_number(right)?,
        )),
        BinaryOp::Div => Ok(JsValue::Number(
            expect_number(left)? / expect_number(right)?,
        )),
        BinaryOp::Mod => Ok(JsValue::Number(
            expect_number(left)? % expect_number(right)?,
        )),
        BinaryOp::LessThan => Ok(JsValue::Bool(expect_number(left)? < expect_number(right)?)),
        BinaryOp::LessThanOrEqual => {
            Ok(JsValue::Bool(expect_number(left)? <= expect_number(right)?))
        }
        BinaryOp::GreaterThan => Ok(JsValue::Bool(expect_number(left)? > expect_number(right)?)),
        BinaryOp::GreaterThanOrEqual => {
            Ok(JsValue::Bool(expect_number(left)? >= expect_number(right)?))
        }
        BinaryOp::StrictEqual => Ok(JsValue::Bool(left == right)),
        BinaryOp::StrictNotEqual => Ok(JsValue::Bool(left != right)),
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
