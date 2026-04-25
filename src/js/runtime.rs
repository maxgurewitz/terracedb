use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use bytes::Bytes;

use crate::Error;

use super::{
    BindingKind, EnvStack, JsRuntimeAttachment, JsRuntimeId, JsValue, MiniExpr, MiniProgram,
    MiniStmt, RuntimeConsole, parse_and_lower_minijs,
};

pub struct JsRuntimeInstance {
    id: JsRuntimeId,
    env_stack: EnvStack,
    attachments: HashMap<TypeId, Box<dyn Any + Send>>,
}

impl JsRuntimeInstance {
    pub(crate) fn new(id: JsRuntimeId) -> Self {
        Self {
            id,
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
        let program = parse_and_lower_minijs(source)?;
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
                    .declare_current(name.clone(), BindingKind::Let, value)
            }
            MiniStmt::Const { name, expr } => {
                let value = self.eval_expr(expr)?;
                self.env_stack
                    .declare_current(name.clone(), BindingKind::Const, value)
            }
            MiniStmt::Assign { name, expr } => {
                let value = self.eval_expr(expr)?;
                self.env_stack.assign(name, value)
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

    fn eval_expr(&self, expr: &MiniExpr) -> Result<JsValue, Error> {
        match expr {
            MiniExpr::Number(value) => Ok(JsValue::Number(*value)),
            MiniExpr::Ident(name) => self.env_stack.lookup(name),
            MiniExpr::Add(lhs, rhs) => {
                let lhs = self.eval_expr(lhs)?;
                let rhs = self.eval_expr(rhs)?;

                match (lhs, rhs) {
                    (JsValue::Number(lhs), JsValue::Number(rhs)) => Ok(JsValue::Number(lhs + rhs)),
                    _ => Err(Error::JsInvalidOperand),
                }
            }
        }
    }
}
