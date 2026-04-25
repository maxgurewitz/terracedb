use std::collections::HashMap;

use crate::Error;

use super::JsValue;

pub struct EnvStack {
    scopes: Vec<LexicalEnv>,
}

pub struct LexicalEnv {
    bindings: HashMap<String, Binding>,
}

pub struct Binding {
    kind: BindingKind,
    value: JsValue,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BindingKind {
    Let,
    Const,
}

impl LexicalEnv {
    fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    fn declare(&mut self, name: String, kind: BindingKind, value: JsValue) -> Result<(), Error> {
        if self.bindings.contains_key(&name) {
            return Err(Error::JsDuplicateBinding { name });
        }

        self.bindings.insert(name, Binding { kind, value });

        Ok(())
    }

    fn get(&self, name: &str) -> Option<JsValue> {
        self.bindings.get(name).map(|binding| binding.value.clone())
    }

    fn get_mut(&mut self, name: &str) -> Option<&mut Binding> {
        self.bindings.get_mut(name)
    }
}

impl EnvStack {
    pub(crate) fn new() -> Self {
        Self {
            scopes: vec![LexicalEnv::new()],
        }
    }

    pub(crate) fn push_scope(&mut self) {
        self.scopes.push(LexicalEnv::new());
    }

    pub(crate) fn pop_scope(&mut self) -> Result<(), Error> {
        if self.scopes.len() == 1 {
            return Err(Error::JsCannotPopRootScope);
        }

        self.scopes.pop();

        Ok(())
    }

    pub(crate) fn declare_current(
        &mut self,
        name: String,
        kind: BindingKind,
        value: JsValue,
    ) -> Result<(), Error> {
        self.scopes
            .last_mut()
            .expect("env stack always has a root scope")
            .declare(name, kind, value)
    }

    pub(crate) fn lookup(&self, name: &str) -> Result<JsValue, Error> {
        self.scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(name))
            .ok_or_else(|| Error::JsBindingNotFound {
                name: name.to_owned(),
            })
    }

    pub(crate) fn assign(&mut self, name: &str, value: JsValue) -> Result<(), Error> {
        for scope in self.scopes.iter_mut().rev() {
            let Some(binding) = scope.get_mut(name) else {
                continue;
            };

            if binding.kind == BindingKind::Const {
                return Err(Error::JsAssignToConst {
                    name: name.to_owned(),
                });
            }

            binding.value = value;

            return Ok(());
        }

        Err(Error::JsBindingNotFound {
            name: name.to_owned(),
        })
    }
}
